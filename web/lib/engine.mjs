// pi-web/lib/engine.mjs — engine telemetry (the thinking box): the 1 Hz probe
// and the derived fallback.
import { storeProviders } from './models.mjs'
import { emitFor } from './notify.mjs'
import { live } from './registry.mjs'
import { liveManager } from './tau.mjs'

// ---------------------------------------------------------------------------
// Engine telemetry — what the thinking box (and the compaction box) renders.
//
// Why the bridge PROBES at all: pi says nothing about the request it is waiting
// on — no phase, no prefill progress, no rate — and during a compaction it emits
// nothing between compaction_start and compaction_end. The engine knows: GET
// /status answers {busy, phase, prompt_tokens, generated, max_tokens} in ~177 B,
// and GET /metrics' `live` field adds prompt_read/prompt_total/tok_s. (Verified
// 2026-07-29: there is NO /metrics.live — 404 — `live` is a field inside a 13.9
// KB /metrics that also carries hardware + history arrays. So /status is the
// cheap call and /metrics rides ONLY while the engine says it is working.)
//
// Discipline, because this file has been burned by every one of these:
//   · one in-flight fetch per engine, results CACHED in the live entry — never
//     on the request path;
//   · ZERO requests while nothing is busy: this is the agent's own model, and a
//     poller that never stops is load on the thing it measures;
//   · a wedged slot must not pile up: a per-call timeout, then park the engine
//     until the next busy transition;
//   · ONE engine slot and `requests[].projection === null` means the engine
//     cannot say WHICH session generated what. With more than one busy session
//     the bridge refuses to attach numbers (`ambiguous`) — a mislabelled tok_s
//     is worse than none. What it CAN say is that a summary is running, because
//     the bridge recorded `compacting`: the engine itself cannot tell a summary
//     from a turn.
// Discovery is pi's own: providers[] from models-store.json carry `routerBase`
// (baseUrl minus /v1). A provider with none (cloud) gets the derived fallback.
// ---------------------------------------------------------------------------

const ENGINE_PROBE_MS = Number(process.env.PI_WEB_ENGINE_PROBE_MS || 1000)
const ENGINE_TIMEOUT_MS = Number(process.env.PI_WEB_ENGINE_TIMEOUT_MS || 500)
const ENGINE_FAIL_LIMIT = Number(process.env.PI_WEB_ENGINE_FAIL_LIMIT || 3)
const ENGINE_FETCHES = new Map() // routerBase -> {fails, parked, inflight, status, live, at}

function engineBases() {
  const out = []
  for (const p of storeProviders()) if (p.routerBase && !out.includes(p.routerBase)) out.push(p.routerBase)
  return out
}

/** The engine a session is talking to ('' = we cannot tell, so we do not guess). */
function engineBaseFor(s) {
  const bases = engineBases()
  if (!bases.length) return ''
  const pid = String(s.model?.provider ?? String(s.modelSpec ?? '').split('/')[0] ?? '')
  const hit = pid ? storeProviders().find((p) => p.id === pid && p.routerBase) : null
  if (hit) return hit.routerBase
  // tau reports the modelSpec it SPAWNED with, which a session created without a
  // model leaves empty. With exactly ONE engine configured there is nothing to
  // get wrong; with several, refuse rather than attach the wrong numbers.
  return bases.length === 1 ? bases[0] : ''
}

/** routerBase -> live session ids generating right now (a turn OR a summary). */
function busyByEngineBase() {
  const g = new Map()
  for (const s of liveManager.sessions.values()) {
    const e = live.get(s.id)
    if (!s.isStreaming && !e?.compacting) continue
    const base = engineBaseFor(s)
    if (!base) continue
    if (!g.has(base)) g.set(base, [])
    g.get(base).push(s.id)
  }
  return g
}

async function engineFetch(base, p) {
  try {
    const res = await fetch(base + p, {
      signal: AbortSignal.timeout(ENGINE_TIMEOUT_MS),
      headers: { accept: 'application/json' },
    })
    if (!res.ok) return null // a plain llama.cpp has no /metrics: nothing to learn, not an error
    return await res.json()
  } catch {
    return null // unreachable / wedged / slow
  }
}

async function engineSample(base) {
  const st =
    ENGINE_FETCHES.get(base) ?? { fails: 0, parked: false, inflight: false, status: null, live: null, at: 0 }
  ENGINE_FETCHES.set(base, st)
  if (st.inflight || st.parked) return st
  st.inflight = true
  const s = await engineFetch(base, '/status')
  const m = s?.busy ? await engineFetch(base, '/metrics') : null
  st.inflight = false
  if (!s) {
    st.fails++
    if (st.fails >= ENGINE_FAIL_LIMIT) {
      st.parked = true
      console.log(`[engine] ${base}: ${st.fails} probes failed — parked until the next busy turn`)
    }
    return st
  }
  st.fails = 0
  st.status = s
  st.live = m?.live ?? null
  st.at = Date.now()
  // per-request stats (reused / hit_rate) live in /metrics' requests[] and only
  // exist AFTER a request ends — keep the newest as the box's "last turn" line
  if (m?.requests?.length) st.lastRequest = m.requests.at(-1)
  return st
}

const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null)

function engineShape(st) {
  const s = st.status ?? {}
  const l = st.live ?? {}
  return {
    source: 'engine',
    at: st.at || Date.now(),
    phase: l.state === 'reading' ? 'thinking' : l.state === 'decoding' ? 'answering' : (s.phase ?? null),
    prompt_read: num(l.prompt_read) ?? num(s.prompt_tokens),
    prompt_total: num(l.prompt_total),
    generated: num(l.generated) ?? num(s.generated),
    max_tokens: num(l.max_tokens) ?? num(s.max_tokens),
    tok_s: num(l.tok_s),
    // per-request reuse exists only AFTER a request ends, so it belongs to the
    // PREVIOUS turn — labelled as such rather than shown as if it were live
    reused: num(st.lastRequest?.reused),
    hit_rate: num(st.lastRequest?.hit_rate),
  }
}

const newDerived = () => ({ t0: Date.now(), first: 0, last: 0, chars: 0 })

function noteDerived(e, chars) {
  const d = e.derived ?? (e.derived = newDerived())
  const now = Date.now()
  if (!d.first) d.first = now
  d.last = now
  d.chars += Math.max(0, chars | 0)
}

/**
 * No engine to ask (cloud, parked, wedged, plain llama.cpp): estimate the same
 * shape from pi's own event stream. Marked `source:'derived'` so the UI can show
 * it as approximate, and it never invents a bar total it cannot know.
 */
function derivedShape(e) {
  const d = e?.derived
  if (!d) return { source: 'derived', at: Date.now(), phase: 'thinking', prompt_read: null, prompt_total: null, generated: 0, tok_s: null, prefill_ms: null }
  const gen = Math.round(d.chars / 4) // chars→tokens is a rough /4
  const decodeMs = d.first ? Math.max(0, d.last - d.first) : 0
  return {
    source: 'derived',
    at: Date.now(),
    phase: d.first ? 'answering' : 'thinking',
    prefill_ms: d.first ? d.first - d.t0 : null,
    prompt_read: null,
    prompt_total: null,
    generated: gen,
    tok_s: decodeMs > 250 ? +((gen / decodeMs) * 1000).toFixed(1) : null,
  }
}

async function engineTick() {
  const groups = busyByEngineBase()
  const publish = []
  for (const [base, ids] of groups) {
    const st = await engineSample(base)
    const shape =
      ids.length > 1
        ? {
            source: st.status ? 'engine' : 'derived',
            ambiguous: true,
            at: st.at || Date.now(),
            phase: null,
            prompt_read: null,
            prompt_total: null,
            generated: null,
            max_tokens: null,
            tok_s: null,
          }
        : st.status
          ? engineShape(st)
          : derivedShape(live.get(ids[0]))
    for (const id of ids) publish.push([id, shape])
  }
  // A session whose provider resolves to NO engine (a cloud provider, or a store
  // with no baseUrl) still deserves the box: phase and rate come from pi's own
  // event stream. It is labelled `source:'derived'` and never invents a prefill
  // total, so the UI can render it as approximate — or drop it.
  for (const s of liveManager.sessions.values()) {
    const e = live.get(s.id)
    if (!s.isStreaming && !e?.compacting) continue
    if (engineBaseFor(s)) continue
    publish.push([s.id, derivedShape(e)])
  }
  const generating = new Set()
  for (const [id, shape] of publish) {
    const e = live.get(id)
    if (!e) continue
    generating.add(id)
    e.engine = shape
    const key = JSON.stringify(shape)
    if (e.engineSent === key) continue // one frame per change, not one per tick
    e.engineSent = key
    emitFor(id, 'session.status', { sessionID: id, status: { type: 'engine', ...shape } })
  }
  // A parked engine wakes on the next busy transition; anything no longer
  // generating drops its block, because the webui re-derives live state from the
  // status map and a stale entry would sit in the box forever.
  for (const [base, st] of ENGINE_FETCHES) if (!groups.has(base)) st.parked = false
  for (const [id, e] of live) {
    if (e.engine && !generating.has(id)) e.engine = null
  }
}

export { ENGINE_PROBE_MS, engineTick, newDerived, noteDerived }
