// /compact check: the webui's `POST /oc/session/{id}/summarize` must reach pi's
// RPC `compact`, and the webui's busy/"Compacting…" strip must un-stick.
//
// Two bugs this guards (both were 501 / a stuck UI):
//   1. /summarize was hard-wired to `not wired in pi-web (mockup)` 501, so
//      /compact could never run.
//   2. pi does NOT emit agent_settled for a manual compaction — compact()
//      aborts a live run FIRST, so the only session.idle the bridge knows about
//      lands BEFORE compaction_start. Without an idle after compaction_end the
//      webui stays busy forever.
// Plus the proxy ceiling path: the POST is answered on a timer and the outcome
// moves to SSE (a Cloudflare 524 must never be what "fails" /compact).
//
// Self-contained: boots pi-web on 8127 per case against the fake pi, asserts,
// kills its OWN child by exact PID (never pattern-kill — SELF-KILL-GATE).
//
//   node .testbin/compact-check.mjs
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const WEB = path.resolve(HERE, '..')
const PORT = Number(process.env.CHECK_PORT || 8127)
const BASE = `http://127.0.0.1:${PORT}`

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}

function req(p, opts = {}) {
  return new Promise((resolve, reject) => {
    const r = http.request(
      new URL(BASE + p),
      { method: opts.method || 'GET', headers: opts.body ? { 'content-type': 'application/json' } : {} },
      (res) => {
        let buf = ''
        res.on('data', (c) => (buf += c))
        res.on('end', () => {
          let json = null
          try {
            json = buf ? JSON.parse(buf) : null
          } catch {
            /* non-json */
          }
          resolve({ status: res.statusCode, json, body: buf })
        })
      },
    )
    r.on('error', reject)
    if (opts.body) r.write(JSON.stringify(opts.body))
    r.end()
  })
}

let events = []
function openSse() {
  return new Promise((resolve, reject) => {
    http.get(`${BASE}/oc/event`, (res) => {
      let buf = ''
      res.on('data', (c) => {
        buf += c
        let i
        while ((i = buf.indexOf('\n\n')) >= 0) {
          const frame = buf.slice(0, i)
          buf = buf.slice(i + 2)
          const d = frame.split('\n').find((l) => l.startsWith('data: '))
          if (!d) continue
          try {
            events.push(JSON.parse(d.slice(6)))
          } catch {
            /* keepalive */
          }
        }
      })
      resolve(res)
    }).on('error', reject)
  })
}

// ---- verbatim from webui/src/lib/sse.ts: the client's busy/compacting state
// machine is the contract, so assert on IT, not on a guess.
function webuiState(evts) {
  let busy = false
  let compacting = false
  let tiles = 0
  for (const e of evts) {
    const p = e?.properties ?? {}
    if (e.type === 'session.next.compaction.started') {
      busy = true
      compacting = true
    } else if (e.type === 'session.next.compaction.ended' || e.type === 'session.compacted') {
      compacting = false
    } else if (e.type === 'session.idle') {
      busy = false
      compacting = false
    } else if (e.type === 'session.error') {
      const em = p.error
      const aborted = em?.name === 'MessageAbortedError'
      if (!aborted) tiles++ // webui persists a session error tile
      busy = false
      compacting = false
    } else if (e.type === 'session.status') {
      const st = p.status ?? p
      if ((st.type ?? st.state) === 'busy') busy = true
    }
  }
  return { busy, compacting, tiles }
}

const seen = (type) => events.filter((e) => e.type === type)
const ofType = (type) => events.filter((e) => e.type === type).map((e) => e.properties ?? {})

// seed a COLD session file (no live child yet) -> proves /summarize resumes one
// instead of 404ing. Needs >=1 user message or sessionMeta() filters it out.
function seedCold(AGENT, SCRATCH) {
  const id = '01a17131-3b1a-73bf-9c4a-3b159ff9d86b'
  const dir = path.join(AGENT, 'sessions', '--seeded--')
  fs.mkdirSync(dir, { recursive: true })
  const now = new Date().toISOString()
  const lines = [
    { type: 'session', id, timestamp: now, cwd: SCRATCH, parentSession: null, version: '1.3.0' },
    {
      type: 'message',
      id: 'e_1',
      parentId: null,
      timestamp: now,
      message: { role: 'user', content: [{ type: 'text', text: 'a seeded question about the thing' }], timestamp: now },
    },
    {
      type: 'message',
      id: 'e_2',
      parentId: 'e_1',
      timestamp: now,
      message: {
        role: 'assistant',
        content: [{ type: 'text', text: 'and an answer' }],
        provider: 'testprov',
        model: 'fake-1',
        timestamp: now,
        usage: { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, totalTokens: 15 },
      },
    },
  ]
  fs.writeFileSync(path.join(dir, `${Date.now()}_${id}.jsonl`), lines.map((l) => JSON.stringify(l)).join('\n') + '\n')
  return id
}

async function boot(env) {
  const SCRATCH = fs.mkdtempSync(path.join(os.tmpdir(), 'compactcheck-'))
  const AGENT = path.join(SCRATCH, 'pi-agent')
  fs.mkdirSync(AGENT, { recursive: true })
  const child = spawn(
    process.execPath,
    [process.env.PI_WEB_SCRIPT || path.join(WEB, 'pi-web.mjs'), '--port', String(PORT), '--dir', AGENT, '--cwd', SCRATCH],
    {
      cwd: WEB,
      env: { ...process.env, PATH: `${HERE}:${process.env.PATH}`, PI_CODING_AGENT_DIR: AGENT, ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  )
  let log = ''
  child.stdout.on('data', (d) => (log += d))
  child.stderr.on('data', (d) => (log += d))
  for (let i = 0; i < 60; i++) {
    try {
      if ((await req('/healthz')).status === 200) break
    } catch {
      /* not up yet */
    }
    await sleep(200)
  }
  events = []
  await openSse()
  const stop = () => {
    try {
      child.kill('SIGKILL') // exact PID only
    } catch {
      /* already gone */
    }
  }
  return { SCRATCH, AGENT, stop, get log() { return log } }
}

const newSession = async (title) => {
  const r = await req('/oc/session', { method: 'POST', body: { cwd: process.env.__SCRATCH, title } })
  return r.json?.id
}

const summarize = (sid, body = { providerID: 'testprov', modelID: 'fake-1' }) =>
  req(`/oc/session/${sid}/summarize`, { method: 'POST', body })

async function caseOk() {
  const b = await boot({ FAKE_PI_COMPACT: 'ok' })
  process.env.__SCRATCH = b.SCRATCH
  try {
    const sid = await newSession('compact-ok')
    check(!!sid, `live session created (${sid})`)
    // seed the cold row up front: creating the live session invalidates pi-web's
    // 1.5s file-list cache, so the seeded file is definitely visible after it
    const coldId = seedCold(b.AGENT, b.SCRATCH)
    // give the session a real turn first: the transcript check below is only
    // meaningful with rows in it, and the settled run also proves the idle
    // ordering against a session that already emitted one
    await req(`/oc/session/${sid}/prompt_async`, {
      method: 'POST',
      body: { parts: [{ type: 'text', text: 'do the thing' }] },
    })
    for (let i = 0; i < 40 && !seen('session.idle').length; i++) await sleep(150)
    const before = await req(`/oc/session/${sid}/message`)
    const n0 = (before.json || []).length
    check(n0 >= 2, `session has a turn to compact (${n0} messages)`)

    const r = await summarize(sid)
    check(r.status === 200, `summarize answers 200 (was 501) — ${r.status} ${String(r.body).slice(0, 80)}`)
    check(r.json?.ok === true, 'response ok:true')
    check(!!r.json?.summary, `summary text forwarded (${String(r.json?.summary).slice(0, 24)}…)`)
    check(
      r.json?.tokensBefore === 42000 && r.json?.estimatedTokensAfter === 900,
      `token deltas forwarded (${r.json?.tokensBefore} -> ${r.json?.estimatedTokensAfter})`,
    )
    check(!!r.json?.firstKeptEntryId, `firstKeptEntryId forwarded (${r.json?.firstKeptEntryId})`)

    const started = ofType('session.next.compaction.started')
    const ended = ofType('session.next.compaction.ended')
    check(started.length === 1, `compaction.started on SSE (${started.length})`)
    check(ended.length === 1, `compaction.ended on SSE (${ended.length})`)
    const endedIdx = events.findIndex((e) => e.type === 'session.next.compaction.ended')
    const lastIdle = events.map((e) => e.type).lastIndexOf('session.idle')
    check(
      lastIdle > endedIdx,
      `session.idle lands AFTER compaction.ended (pi never settles a manual compact) — idle#${lastIdle} ended#${endedIdx}`,
    )
    check(seen('session.error').length === 0, 'no session.error for a successful compaction')
    const st = webuiState(events)
    check(!st.busy && !st.compacting, `webui state un-stuck (busy=${st.busy} compacting=${st.compacting})`)

    const after = await req(`/oc/session/${sid}/message`)
    const rows = after.json || []
    check(
      rows.length === n0 + 1,
      `history is NOT pruned and the summary lands as ONE new row (${n0} -> ${rows.length})`,
    )
    const sum = rows.at(-1)
    check(sum?.id?.startsWith('cmp_'), `summary row has the deterministic compaction id (${sum?.id})`)
    const sumText = (sum?.parts ?? []).map((p) => p.text ?? '').join('')
    check(
      sum?.role === 'assistant' && sumText.includes('Context compacted') && sumText.includes('compact-check fixture'),
      `the row carries pi's summary text + header (${JSON.stringify(sumText.slice(0, 60))})`,
    )
    check(
      (sum?.parts ?? []).length > 0 && (sum?.parts ?? []).every((p) => p.synthetic === true),
      'the row is marked synthetic (no revert/fork affordance on generated text)',
    )
    check(
      seen('session.compacted').length >= 1,
      `session.compacted fires so the webui clears "Compacting" AND refetches (${seen('session.compacted').length})`,
    )

    // The summary row must keep the POSITION it happened at. pi appends its
    // `compaction` entry mid-branch, so once the conversation continues the row
    // is not the newest thing in the transcript. Regression (2026-10-28):
    // transcriptEntries() APPENDED the bridge's live copy — for a live child
    // tau's mirror holds no compaction entries at all, so the dedupe set was
    // always empty — and every refetch re-rendered "Context compacted" as the
    // most recent message for as long as the child stayed live (hours).
    const nBefore = rows.length
    await req(`/oc/session/${sid}/prompt_async`, {
      method: 'POST',
      body: { parts: [{ type: 'text', text: 'carry on after the summary' }] },
    })
    for (let i = 0; i < 40; i++) {
      const now = (await req(`/oc/session/${sid}/message`)).json || []
      if (now.length > nBefore + 1) break
      await sleep(150)
    }
    const later = (await req(`/oc/session/${sid}/message`)).json || []
    const cmpRows = later.filter((r) => String(r.id).startsWith('cmp_'))
    const cmpIdx = later.findIndex((r) => String(r.id).startsWith('cmp_'))
    check(cmpRows.length === 1, `exactly one summary row after a later turn (${cmpRows.length})`)
    check(
      cmpIdx >= 0 && cmpIdx < later.length - 1,
      `the summary row is NOT the newest row after a later turn (cmp at ${cmpIdx} of ${later.length})`,
    )
    check(
      String(later.at(-1)?.id ?? '').startsWith('m_'),
      `the newest row is the post-compaction turn (${later.at(-1)?.id})`,
    )

    // unknown id still 404s, it is not swallowed by the new route
    const ghost = await summarize('00000000-0000-4000-8000-000000000000')
    check(ghost.status === 404, `unknown session -> 404 (${ghost.status})`)

    // cold sidebar row: no GET first, no live child — must resume, not 404
    const cold = await summarize(coldId)
    check(cold.status === 200, `cold session /summarize resumes it (was 404) — ${cold.status}`)
    check(cold.json?.ok === true, 'cold compaction reported ok')
  } finally {
    b.stop()
    await sleep(250)
    fs.rmSync(b.SCRATCH, { recursive: true, force: true })
    if (failed) console.log('--- server log ---\n' + b.log.slice(-1500))
  }
}

async function caseFailFast() {
  const b = await boot({ FAKE_PI_COMPACT: 'fail' })
  process.env.__SCRATCH = b.SCRATCH
  try {
    const sid = await newSession('compact-fail')
    const r = await summarize(sid)
    check(r.status === 502, `failed compaction -> 502 (${r.status})`)
    check(
      String(r.json?.data?.message).includes('Nothing to compact'),
      `pi's reason reaches the toast (${JSON.stringify(r.json?.data?.message)})`,
    )
    const st = webuiState(events)
    check(!st.busy && !st.compacting, `idle still emitted on failure (busy=${st.busy} compacting=${st.compacting})`)
    check(
      seen('session.error').length === 0,
      'no SSE error while an HTTP response is still open (single error surface)',
    )
  } finally {
    b.stop()
    await sleep(250)
    fs.rmSync(b.SCRATCH, { recursive: true, force: true })
    if (failed) console.log('--- server log ---\n' + b.log.slice(-1500))
  }
}

async function caseSlow() {
  // ceiling 300ms, summary takes 1200ms: the proxy never gets to 524 this.
  const b = await boot({ FAKE_PI_COMPACT: 'slow', FAKE_PI_COMPACT_MS: '1200', PI_WEB_SUMMARIZE_WAIT_MS: '300' })
  process.env.__SCRATCH = b.SCRATCH
  try {
    const sid = await newSession('compact-slow')
    const t0 = Date.now()
    const r = await summarize(sid)
    const dt = Date.now() - t0
    check(r.status === 200 && r.json?.started === true && r.json?.pending === true, `answered before the ceiling — ${r.status} ${String(r.body).slice(0, 60)}`)
    check(dt < 1000, `answered in ${dt}ms (< the ${1200}ms summary), so no edge timeout`)
    await sleep(1500) // the RPC + compaction_end land later
    check(ofType('session.next.compaction.ended').length === 1, 'compaction.ended arrives after the answer')
    check(ofType('session.idle').length >= 1, 'session.idle closes the UI without any HTTP response')
    const st = webuiState(events)
    check(!st.busy && !st.compacting, `state un-stuck after a late success (busy=${st.busy} compacting=${st.compacting})`)
  } finally {
    b.stop()
    await sleep(250)
    fs.rmSync(b.SCRATCH, { recursive: true, force: true })
    if (failed) console.log('--- server log ---\n' + b.log.slice(-1500))
  }
}

async function caseSlowFail() {
  const b = await boot({ FAKE_PI_COMPACT: 'slowfail', FAKE_PI_COMPACT_MS: '1200', PI_WEB_SUMMARIZE_WAIT_MS: '300' })
  process.env.__SCRATCH = b.SCRATCH
  try {
    const sid = await newSession('compact-slowfail')
    const r = await summarize(sid)
    check(r.status === 200 && r.json?.pending === true, `answered pending (${r.status})`)
    await sleep(1500)
    const errs = ofType('session.error')
    check(errs.length === 1, `late failure reaches the client over SSE (${errs.length} session.error)`)
    check(
      String(errs[0]?.error?.data?.message).includes('Nothing to compact'),
      `late failure carries pi's reason (${JSON.stringify(errs[0]?.error?.data?.message)})`,
    )
    const st = webuiState(events)
    check(!st.busy && !st.compacting, `state un-stuck after a late failure (busy=${st.busy} compacting=${st.compacting})`)
  } finally {
    b.stop()
    await sleep(250)
    fs.rmSync(b.SCRATCH, { recursive: true, force: true })
    if (failed) console.log('--- server log ---\n' + b.log.slice(-1500))
  }
}

async function caseAbort() {
  const b = await boot({ FAKE_PI_COMPACT: 'abort' })
  process.env.__SCRATCH = b.SCRATCH
  try {
    const sid = await newSession('compact-abort')
    const r = await summarize(sid)
    check(r.status === 502 && String(r.json?.data?.message).includes('cancelled'), `cancelled compaction -> 502 (${r.status})`)
    await sleep(300)
    const st = webuiState(events)
    check(!st.busy && !st.compacting, `cancelled compaction un-sticks the UI (busy=${st.busy} compacting=${st.compacting})`)
    check(st.tiles === 0, `no error tile for a cancel (${st.tiles})`)
  } finally {
    b.stop()
    await sleep(250)
    fs.rmSync(b.SCRATCH, { recursive: true, force: true })
    if (failed) console.log('--- server log ---\n' + b.log.slice(-1500))
  }
}

// The reported failure: `send failed: POST /oc/session/<id>/prompt_async -> 502`
// when queueing a message during a compaction. pi refuses a prompt OUTRIGHT
// while it is summarising (core/agent-session.js:1620 — "Cannot submit a prompt
// while compaction is in progress. Wait for compaction to finish and retry.")
// and the bridge forwarded that refusal as a 502 — a send FAILURE for a session
// that was merely busy summarising. The bridge knows when the summary lands
// (compaction_end), so it holds the send and delivers it.
async function caseSendDuringCompaction() {
  const b = await boot({
    FAKE_PI_COMPACT: 'slow',
    FAKE_PI_COMPACT_MS: '900',
    PI_WEB_SUMMARIZE_WAIT_MS: '300',
    PI_WEB_COMPACT_SEND_WAIT_MS: '10000',
  })
  process.env.__SCRATCH = b.SCRATCH
  try {
    const sid = await newSession('send-during-compaction')
    void summarize(sid)
    await sleep(250) // inside the summary window
    const t0 = Date.now()
    const p = await req(`/oc/session/${sid}/prompt_async`, {
      method: 'POST',
      body: { parts: [{ type: 'text', text: 'queued while summarising' }] },
    })
    const dt = Date.now() - t0
    check(p.status === 204, `a send during compaction is not a send failure (${p.status} ${JSON.stringify(p.json?.error ?? p.json ?? '')})`)
    check(dt >= 550, `the bridge HELD the send for the summary (${dt}ms into a 900ms summary)`)
    await sleep(1400)
    const rows = await req(`/oc/session/${sid}/message`)
    check(
      JSON.stringify(rows.json ?? '').includes('queued while summarising'),
      `the held send ran once the summary landed (${(rows.json ?? []).length} message(s))`,
    )
    check(ofType('session.next.compaction.ended').length === 1, 'the summary still landed')
  } finally {
    b.stop()
    await sleep(250)
    fs.rmSync(b.SCRATCH, { recursive: true, force: true })
    if (failed) console.log('--- server log ---\n' + b.log.slice(-1500))
  }
}

// The bounded path: the summary outlives the hold window, so the client gets a
// RETRYABLE busy. pi never accepted the text, so the outcome is not unknown —
// 504 {unknown:true} (the timeout invariant) would be wrong here, and so is 502.
async function caseSendPastWindow() {
  const b = await boot({
    FAKE_PI_COMPACT: 'slow',
    FAKE_PI_COMPACT_MS: '1500',
    PI_WEB_SUMMARIZE_WAIT_MS: '300',
    PI_WEB_COMPACT_SEND_WAIT_MS: '400',
  })
  process.env.__SCRATCH = b.SCRATCH
  try {
    const sid = await newSession('send-past-window')
    void summarize(sid)
    await sleep(200)
    const p = await req(`/oc/session/${sid}/prompt_async`, {
      method: 'POST',
      body: { parts: [{ type: 'text', text: 'never got a turn' }] },
    })
    check(
      p.status === 409 && (p.json?.name ?? p.json?.error?.name) === 'SessionBusyError',
      `past the hold window it is 409 SessionBusyError, not 502 (${p.status} ${JSON.stringify(p.json)})`,
    )
    await sleep(1600)
    const rows = await req(`/oc/session/${sid}/message`)
    check(
      !JSON.stringify(rows.json ?? '').includes('never got a turn'),
      'and the refused text really did not land (the client may retry it)',
    )
  } finally {
    b.stop()
    await sleep(250)
    fs.rmSync(b.SCRATCH, { recursive: true, force: true })
    if (failed) console.log('--- server log ---\n' + b.log.slice(-1500))
  }
}

// The webui does NOT trust SSE alone for `busy`: its reconcile watchdog
// (webui/src/lib/sse.ts startEvents() → catchUp(), a 10s tick whenever any live
// tab is busy) polls GET /oc/session/status and writes back whatever it reads —
// `if (!busy && t.compacting) compacting = false`. pi's manual compact() emits
// compaction_start/compaction_end and NO agent_start/turn_start (core/
// agent-session.js `compact()`), and tau's `isStreaming` is only ever set by
// those two (pi-tau-web-server src/server/sessions.ts), so a status map built
// from `isStreaming` alone says `idle` for the whole summary. The watchdog then
// cancels the "Compacting session…" strip ~10s into a summary that runs for
// minutes on a big session. So the STATUS ENDPOINT is part of the compaction
// contract, not just the SSE frames.
async function caseStatusDuringCompaction() {
  const b = await boot({ FAKE_PI_COMPACT: 'slow', FAKE_PI_COMPACT_MS: 3000 })
  process.env.__SCRATCH = b.SCRATCH
  try {
    const sid = await newSession('compact-status')
    check(!!sid, `live session created (${sid})`)
    await req(`/oc/session/${sid}/prompt_async`, {
      method: 'POST',
      body: { parts: [{ type: 'text', text: 'fill the session first' }] },
    })
    for (let i = 0; i < 40 && !seen('session.idle').length; i++) await sleep(150)

    const typeOf = async () => {
      const st = await req('/oc/session/status')
      const v = st.json?.[sid]
      return v?.type ?? v?.state ?? `(no entry: ${JSON.stringify(st.json ?? '').slice(0, 60)})`
    }

    check((await typeOf()) === 'idle', `idle before the compaction (${await typeOf()})`)

    const inflight = summarize(sid) // do NOT await: the window is the point
    await sleep(400)
    const mid1 = await typeOf()
    check(mid1 === 'busy', `status says BUSY mid-summary (the watchdog's only input) — got ${mid1}`)
    await sleep(900)
    const mid2 = await typeOf()
    check(mid2 === 'busy', `still busy 1.3s into the summary — got ${mid2}`)
    check(
      seen('session.next.compaction.ended').length === 0,
      'and the summary really is still running (no compaction.ended yet)',
    )

    const r = await inflight
    check(r.status === 200, `summarize answered (${r.status})`)
    let after = await typeOf()
    for (let i = 0; i < 40 && after !== 'idle'; i++) {
      await sleep(150)
      after = await typeOf()
    }
    check(after === 'idle', `status returns to idle once the summary lands — must not pin busy (${after})`)
  } finally {
    b.stop()
    await sleep(250)
    fs.rmSync(b.SCRATCH, { recursive: true, force: true })
    if (failed) console.log('--- server log ---\n' + b.log.slice(-1500))
  }
}

for (const c of [caseOk, caseFailFast, caseSlow, caseSlowFail, caseAbort, caseSendDuringCompaction, caseSendPastWindow, caseStatusDuringCompaction]) {
  console.log(`\n===== ${c.name} =====`)
  try {
    await c()
  } catch (e) {
    console.log('ERROR', e.stack || String(e))
    failed++
  }
}
console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
process.exit(failed ? 1 : 0)
