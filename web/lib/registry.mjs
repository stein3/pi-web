// pi-web/lib/registry.mjs — live pi children: the per-child entry (aliases,
// dup ledger, tool join state), public-id adoption, and the idle reaper.
import fs from 'node:fs'
import path from 'node:path'
import { fileUuid } from './ids.mjs'
import { liveManager } from './tau.mjs'

// ---------------------------------------------------------------------------
// live session bookkeeping
// ---------------------------------------------------------------------------
/**
 * tauId -> { ids:Set, dup:Map, cur:{mid, byIndex}, byCall:Map, toolStart:Map }
 * `ids` = every public id this session answers to (tau id + pi file uuid), so
 * a tab opened from the sidebar (file uuid) and one created live (tau id)
 * both receive the same events.
 * `byCall`/`toolStart` live on the ENTRY, not on `cur`: pi emits the assistant
 * `message_end` BEFORE `tool_execution_*` (agent-loop.ts runs tools after the
 * stream ends), so `cur` is already null when the tool events arrive — keying
 * them off `cur` dropped every live tool completion and left parts ⏳ forever.
 */
const live = new Map()
/** publicId/uuid -> tau session, for cold sessions opened from history */
const byPublicId = new Map()

// ---------------------------------------------------------------------------
// live-child reclamation
// ---------------------------------------------------------------------------
// Children are server-owned and survive browser reloads by design — but nothing
// ever released them. Closing a tab is purely local in the webui (`tabs.close()`;
// no HTTP call), so every session ever opened kept a `pi` child for the whole
// server lifetime. Measured after one working day (2026-10-25): 13 children /
// 1.80 GiB RSS against a 3 GiB cgroup cap (~150 MB each), and ONE background
// child produced 83% of all SSE frames in a 170 s window (4,794 deltas) while
// the foreground session was mid-turn — with the llama.cpp router at
// `max_instances: 1` that child was competing for the same slot the foreground
// turn needed.
//
// Reap idle children: never one that is streaming, has an RPC in flight, or holds
// a pending dialog. Reaped sessions are NOT deleted — the next request takes the
// existing cold path (`sessionFor` -> `liveManager.resume` from the JSONL). Since
// every public id is now pi's file uuid, a reaped session stays addressable by
// construction; `reapedFile` is what keeps a tab that still holds an old tau id
// resolvable after its child is closed.
const IDLE_TTL_MS = Number(process.env.PI_WEB_IDLE_TTL_MS ?? 20 * 60_000)
const MAX_LIVE = Number(process.env.PI_WEB_MAX_LIVE ?? 6)
const REAP_INTERVAL_MS = Number(process.env.PI_WEB_REAP_INTERVAL_MS ?? 60_000)
const reapedFile = new Map() // publicId -> JSONL path once its child is reaped
const INTERNAL_CLOSE = new Map() // tauId -> file: pi-web-initiated close

function childIdleMs(s) {
  const t = Date.parse(s?.lastActiveAt ?? '') || 0
  return t ? Math.max(0, Date.now() - t) : Number.MAX_SAFE_INTEGER
}

function reapable(s) {
  if (s.isStreaming) return false // a turn is running (turn_start -> agent_settled)
  // A summary in flight looks idle to tau (see `compacting` in entryFor) and
  // writes nothing to the JSONL until it lands, so it is the MOST idle-looking
  // session on the box right before MAX_LIVE pressure evicts it.
  if (live.get(s.id)?.compacting) return false
  if (s.pending?.size) return false // an RPC round-trip is in flight
  if (s.pendingDialogs?.size) return false // a permission/question dialog is open
  if (s.abortState && s.abortState !== 'idle') return false // an abort is settling
  return true
}

async function reapIdleChildren() {
  const all = [...liveManager.sessions.values()]
  if (!all.length) return
  // most idle first
  const idle = all.filter(reapable).sort((a, b) => childIdleMs(b) - childIdleMs(a))
  const doomed = new Set()
  for (const s of idle) if (childIdleMs(s) >= IDLE_TTL_MS) doomed.add(s)
  let over = all.length - MAX_LIVE // LRU cap: idle children only, streaming ones stay
  for (const s of idle) {
    if (over <= 0) break
    if (!doomed.has(s)) doomed.add(s)
    over -= 1
  }
  for (const s of doomed) {
    const file = s.sessionFile
    if (!file || !fs.existsSync(file)) continue // no transcript yet — closing it would lose the chat
    const ids = [...publicIdsFor(s.id)]
    for (const id of ids) reapedFile.set(id, file)
    INTERNAL_CLOSE.set(s.id, file)
    console.log(
      `[reap] ${s.id} idle ${Math.round(childIdleMs(s) / 1000)}s -> closed child (${ids.join(',')} resumes from ${path.basename(file)}); ${liveManager.sessions.size} live`,
    )
    try {
      await liveManager.delete(s.id, 'idle_reaper')
    } catch (err) {
      INTERNAL_CLOSE.delete(s.id)
      console.error('[reap] close failed:', err?.message ?? err)
    }
  }
}

function entryFor(tauId) {
  let e = live.get(tauId)
  if (!e) {
    e = {
      ids: new Set([tauId]),
      dup: new Map(),
      cur: null,
      title: null,
      byCall: new Map(),
      toolStart: new Map(),
      file: null, // last sessionFile seen (drives the canonical-uuid alias)
      // A summary is being generated on this child (pi compaction_start ->
      // compaction_end). pi's manual compact() emits NEITHER agent_start nor
      // turn_start (core/agent-session.js `compact()` aborts any live run
      // first), and tau's `isStreaming` is set ONLY by those two (pi-tau-web-
      // server src/server/sessions.ts:462), so `isStreaming` is false for the
      // whole summary. Anything that calls a session busy — GET /session/status
      // (the webui's 10s busy-reconcile watchdog reads it and clears busy AND
      // compacting from it), the idle reaper — must consult this flag too.
      compacting: false,
      // The LAST compaction's outcome, in the shape the UI's box renders from:
      // { state: 'running'|'ok'|'failed'|'cancelled'|'rejected', reason, at,
      //   tokensBefore }. It lives here (not only in SSE frames) because the
      // webui re-derives live state from GET /session/status after every
      // reconnect and on its 10s tick — a failure that exists only in one
      // dropped frame is invisible by the time the tab repaints. pi appends NO
      // JSONL entry for a failed compaction, so this is the only place the
      // reason survives at all.
      compaction: null,
      // Engine telemetry for THIS session (see the engine-probe section): the
      // last shape published, its dedupe key, and the bridge's own event-stream
      // counters used as the fallback when there is no engine to ask.
      engine: null,
      engineSent: '',
      derived: null,
      revert: null, // opencode-style revert point ({messageID, partID?}) until the next turn
      armed: false,
      named: false, // a name is settled for this child (pi's own, or one we sent)
      queue: { steering: [], followUp: [] },
      // compactions this bridge SAW happen inside the live child (compaction_end
      // carries the summary; tau's entries never record it). See transcriptEntries().
      compactions: [],
      level: null, // last thinking level this bridge asked pi for (webui variant)
      modelRef: '', // "provider\0modelId" last applied to THIS child
    }
    live.set(tauId, e)
  }
  return e
}

// pi owns the pending-message queues (steer + follow-up live inside the RPC
// child); we only mirror what its `queue_update` event reports — complete lists,
// never diffs — so a client that missed an event heals on the next one. Nothing
// here can drain them, and nothing here outlives the child.
function queueView(tauId) {
  const q = live.get(tauId)?.queue
  return { steering: [...(q?.steering ?? [])], followUp: [...(q?.followUp ?? [])] }
}

// pi's default hands the follow-up queue over ONE message per stop-boundary, so
// three queued follow-ups land across three turns. A web client expects all of
// them at the next settle, so ask for "all" once per child. NB: this is a
// global pi setting that pi persists into settings.json — it also changes the
// interactive TUI's queue behaviour.
function armSession(s) {
  const e = entryFor(s.id)
  if (e.armed) return
  e.armed = true
  s.send({ type: 'set_follow_up_mode', mode: 'all' }, { timeoutMs: 8000 }).catch(() => {
    e.armed = false // let the next request retry
  })
}
function publicIdsFor(tauId) {
  const e = live.get(tauId)
  if (!e) return [tauId]
  // register the canonical uuid as an alias the moment the child knows its file,
  // so frames reach tabs opened by the uuid AND tabs that still hold a tau id
  const f = liveManager.get(tauId)?.sessionFile
  if (f && f !== e.file) {
    e.file = f
    const uuid = fileUuid(f)
    if (uuid) e.ids.add(uuid)
  }
  return e.ids
}

function sessionByAnyId(id) {
  const s = liveManager.get(id)
  if (s) return s
  const tauId = byPublicId.get(id)
  if (tauId) return liveManager.get(tauId) ?? null
  // Now that the PUBLIC id is pi's file uuid while a child is addressed by its
  // tau id, an id can be missing from both maps and still name a live child —
  // the alias set (`e.ids`) is the authority, and the transcript path is the
  // backstop. Without these, the first click on a uuid sidebar row would fall
  // through to `sessionFor` and resume a SECOND child on the same JSONL.
  for (const [tid, e] of live) {
    if (e.ids.has(id)) {
      const owned = liveManager.get(tid)
      if (owned) return owned
    }
  }
  const f = reapedFile.get(id)
  if (f) {
    const owned = liveManager.findBySessionFile(f)
    if (owned) return owned
  }
  return null
}

/**
 * Make a fresh child's transcript uuid public BEFORE answering the create call.
 * pi computes the path — and the uuid inside it — in `newSession()`
 * (core/session-manager.js), long before the bytes exist; the file itself only
 * appears once the session has a conversation entry. tau probes `get_state` 250 ms
 * after every start to learn it, which is AFTER `POST /oc/session` has answered,
 * so the response used to carry the per-child handle instead: the tab opened as
 * `tau_*`, pi then wrote `<ts>_<uuid>.jsonl`, and the sidebar listed the SAME
 * session under its uuid — two rows, two tabs, one transcript. Waiting here costs
 * one idle RPC round trip; a child that never answers keeps the old fallback and
 * the alias mechanism still adopts the uuid later.
 */
async function learnPublicId(s) {
  const budget = Number(process.env.PI_WEB_ID_WAIT_MS ?? 2000)
  const deadline = Date.now() + budget
  while (!s.sessionFile && Date.now() < deadline) {
    // halfway, ask for ourselves rather than wait for tau's own probe to land
    if (Date.now() > deadline - budget / 2 && !s.__piWebProbed) {
      s.__piWebProbed = true
      try {
        await s.send({ type: 'get_state' }, { timeoutMs: 3000 })
      } catch {
        /* not in RPC mode yet, or busy — the fallback id still works */
      }
    } else {
      await new Promise((r) => setTimeout(r, 40))
    }
  }
  return publicIdsFor(s.id)
}
/** file path -> tau id when a live child owns it (so sidebar rows share tabs) */
function liveTauIdFor(file) {
  for (const s of liveManager.sessions.values()) {
    if (s.sessionFile && path.resolve(s.sessionFile) === path.resolve(file)) return s.id
  }
  return null
}


export { INTERNAL_CLOSE, REAP_INTERVAL_MS, armSession, byPublicId, entryFor, learnPublicId, live, liveTauIdFor, publicIdsFor, queueView, reapIdleChildren, reapedFile, sessionByAnyId }
