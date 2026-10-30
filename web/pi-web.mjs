#!/usr/bin/env node
/**
 * pi-web — an opencode-shaped backend for nopencode's webui, backed by pi.
 *
 *   browser ──> pi-web ──(JSONL RPC over stdio)──> N × `pi --mode rpc`
 *
 * Owns N concurrent pi children (tau's model: one per live tab, server-owned,
 * surviving browser reloads) and re-exposes them under the wire contract the
 * nopencode webui already speaks:
 *
 *   /oc/*            REST + SSE (opencode engine shape)
 *   /api/history/*   read-only view over pi's own JSONL session files
 *   /*               static webui/dist
 *
 * Process management is NOT reimplemented — it is imported from
 * `pi-tau-web-server` (MIT): PiRpcSession + LiveSessionManager handle spawn,
 * JSONL framing, id correlation, timeouts, exit handling and dialog forwarding.
 * tau's own WebSocket protocol and frontend are deliberately unused; a fake
 * WS client is registered on the manager instead, so tau's broadcasts land
 * here and are re-emitted as opencode SSE.
 *
 * Storage is pi's. Nothing here keeps a message store: transcripts are read
 * from PiRpcSession.entries (live) or the JSONL file (cold), through one pure
 * projection `toOcMessages()`. The only live state is
 *   - alias set (which public ids name this session), and
 *   - a dup-count ledger so message ids hash identically on both paths.
 *
 * Message ids are `sha1(role|timestamp|dup)` — derived, never minted, so a
 * refetch and a live event can never disagree (the class of bug opencode's
 * part-id minting + dropBufferedDeltas defends against).
 *
 * The Diff PANE is intentionally not wired (no /api/history/.../changes, no
 * vcs/diff): tau has the better file/diff viewer. The transcript's inline
 * edit-diff block IS wired — pi's tool `details.patch` is mapped to opencode's
 * `state.metadata.filediff.patch` by `toolMetadata()`.
 *
 * Usage:  node pi-web.mjs [--port 8124] [--host 0.0.0.0] [--cwd <workspace>]
 *                         [--dir <agentDir>] [--dist <webui/dist>]
 * Every default is derived from where this file lives (see WEBUI_DIST /
 * PI_AGENT_DIR below) or comes from the matching PI_* env var.
 */

import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { createHash, randomBytes } from 'node:crypto'
import { fileURLToPath } from 'node:url'

// ---------------------------------------------------------------------------
// argv / env — MUST be resolved before importing tau: its config.js snapshots
// PI_CODING_AGENT_DIR / PI_CODING_AGENT_SESSION_DIR at module load.
// ---------------------------------------------------------------------------

const argv = process.argv.slice(2)
const arg = (name, def) => {
  const i = argv.indexOf(`--${name}`)
  return i >= 0 && argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : def
}

const HERE = path.dirname(fileURLToPath(import.meta.url))
const PORT = Number(arg('port', process.env.PI_WEB_PORT || 8124))
const HOST = arg('host', process.env.PI_WEB_HOST || '0.0.0.0')
// The webui is a sibling clone (nopencode), so the default dist is relative to
// this file, not to one machine's checkout: <repo>/web -> up two -> opencode.
// The container always passes --dist /app/webui; the derived default is for
// bare-metal runs.
const WEBUI_DIST = path.resolve(
  arg(
    'dist',
    process.env.WEBUI_DIST || path.join(HERE, '..', '..', 'opencode', 'webui', 'dist'),
  ),
)
const PI_CWD = path.resolve(arg('cwd', process.env.PI_CWD || '/workspace'))
const PI_AGENT_DIR = path.resolve(
  arg('dir', process.env.PI_CODING_AGENT_DIR || path.join(HERE, '..', 'config', 'pi-agent')),
)
process.env.PI_CODING_AGENT_DIR = PI_AGENT_DIR

const { liveManager } = await import('pi-tau-web-server/bin/sessions.js')
const { navigateTree, flattenTree, pathFromRoot } = await import('pi-tau-web-server/bin/tree.js')
const { SESSIONS_DIR } = await import('pi-tau-web-server/bin/config.js')
const { getAvailableModels } = await import(
  'pi-tau-web-server/bin/model-utils.js'
)
// tau's directory completer for the new-session workspace picker
const { completeDirectories } = await import(
  'pi-tau-web-server/bin/path-completion.js'
)

// Soft-delete trash root. pi stores one dir per workspace directly under
// SESSIONS_DIR and listFiles() walks that tree — so a `sessions/trash/` dir
// re-lists every "deleted" session forever (delete looked like a no-op).
// Trash lives OUTSIDE the scanned tree; pi's own resume picker only reads
// sessions/<encoded-cwd>/ flat, so it never sees it either.
const TRASH_DIR = path.join(path.dirname(SESSIONS_DIR), 'sessions-trash')
const TRASHED_DIRS = new Set(
  // 'trash' = the in-tree trash older builds left behind; never re-list it
  [TRASH_DIR, path.join(SESSIONS_DIR, 'trash')].map((p) => path.resolve(p)),
)

// ---------------------------------------------------------------------------
// SSE hub
// ---------------------------------------------------------------------------

/** @type {Set<import('node:http').ServerResponse>} */
const sseClients = new Set()

function emit(type, properties) {
  if (!sseClients.size) return
  const frame = `data: ${JSON.stringify({ type, properties })}\n\n`
  for (const res of sseClients) {
    try {
      res.write(frame)
    } catch {
      sseClients.delete(res)
    }
  }
}

// ---------------------------------------------------------------------------
// tau bridge — tau thinks it is talking to a WebSocket client.
// ---------------------------------------------------------------------------

liveManager.addClient({
  readyState: 1, // WebSocket.OPEN — the only value broadcast() checks
  send: (payload) => {
    try {
      onTauFrame(JSON.parse(payload))
    } catch (err) {
      console.error('[bridge]', err?.stack ?? err, '\npayload:', String(payload).slice(0, 300))
    }
  },
  on() {},
  close() {},
  terminate() {},
})

// ---------------------------------------------------------------------------
// live session bookkeeping
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// unhandled-record telemetry
// ---------------------------------------------------------------------------
// pi and tau both emit more records than this bridge consumes, and a record
// that falls through `onPiEvent`/`onTauFrame` is invisible: it just never
// becomes SSE. That is how whole feature paths hide (pi reports a rename as
// `session_info_changed`, which this bridge never matched) — you cannot debug
// what you cannot see. So: log the FIRST sighting of every record type no
// branch looked at, plus a running count every Nth, and keep the tally
// queryable from /healthz. Bounded by construction — one line per type, never
// one per token.
const UNHANDLED_EVERY = Number(process.env.PI_WEB_UNHANDLED_EVERY ?? 500)
const unhandled = new Map() // 'kind:type' -> count

// A sample that can never be a transcript: keys + truncated scalars only.
function brief(v) {
  if (v === null || typeof v !== 'object') return String(v).slice(0, 80)
  if (Array.isArray(v)) return `[${v.length}]`
  const out = {}
  for (const [k, x] of Object.entries(v)) {
    if (typeof x === 'string') out[k] = x.length > 60 ? `${x.slice(0, 60)}…` : x
    else if (Array.isArray(x)) out[k] = `[${x.length}]`
    else if (x && typeof x === 'object') out[k] = `{${Object.keys(x).join(',')}}`
    else out[k] = x
  }
  return out
}

function noteUnhandled(kind, type, sample) {
  const key = `${kind}:${type ?? 'unknown'}`
  const n = (unhandled.get(key) ?? 0) + 1
  unhandled.set(key, n)
  if (n === 1 || n % UNHANDLED_EVERY === 0) {
    console.error(
      `[unhandled] ${key} n=${n}` + (sample === undefined ? '' : ` ${JSON.stringify(brief(sample))}`),
    )
  }
}

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

/**
 * Name an unnamed session, once, through pi itself.
 *
 * `hint` is the live user message when one is arriving right now; otherwise the
 * first prompt of the transcript is used. Claiming `e.named` before the round
 * trip is what keeps the RPC, the event it causes, and the next user message
 * from each starting a rename.
 */
async function autoName(tauId, hint) {
  const e = live.get(tauId)
  const s = liveManager.get(tauId)
  if (!e || !s || e.named) return
  e.named = true
  const file = s.sessionFile || e.file || null
  // pi named it (or the operator did): never overwrite an existing name.
  if (file) {
    try {
      if (sessionMeta(file)?.named) return
    } catch {
      /* unreadable — fall through and try to name it */
    }
  }
  const name = autoTitle(hint || firstUserText(s.entries) || firstUserText(file ? readJsonl(file) : null))
  if (!name) {
    e.named = false
    return
  }
  try {
    const r = await s.send({ type: 'set_session_name', name }, { timeoutMs: 8000 })
    if (r?.success === false) {
      e.named = false
      return
    }
  } catch {
    e.named = false // child busy/closing: a later message gets another chance
    return
  }
  // pi answers with session_info_changed, which sets the title for us; do it
  // here too so a pi build that stays quiet still names the tab. Signature-gated:
  // a name nobody disagrees about needs no frame.
  e.title = name
  emitSessionUpdated(tauId, s)
}

function apiErr(message, name = 'APIError') {
  return { name, data: { message: String(message) } }
}

// prompt_async file parts arrive in the opencode engine's shape:
// {type:'file', mime, url:'data:<mime>;base64,…'}. pi wants ImageContent blocks:
// {type:'image', data:<base64>, mimeType}. Returns null for anything else.
function fileToImage(p) {
  if (!p || p.type !== 'file') return null
  const m = /^data:([^;,]+);base64,([\s\S]*)$/.exec(p.url ?? '')
  if (!m) return null
  if (!m[1].startsWith('image/')) return null
  return { type: 'image', data: m[2], mimeType: m[1] }
}

/**
 * pi's own session uuid, straight from the file name pi writes:
 * `<iso-ts>_<uuid>.jsonl` (the ts has no underscores, so split on the last one).
 *
 * This is the ONE id that survives everything — child reaped, child exited,
 * server restarted — because it is the name of the transcript on disk. tau's
 * `tau_*` ids are per-child: the same session answers as a DIFFERENT tau id
 * after every resume, which is how one session ends up open in two tabs (the
 * tab holds the old id, the sidebar row was re-labelled with the new one).
 * So the tau id is an internal handle only: every PUBLIC id pi-web publishes —
 * `GET /oc/session`, `GET /oc/session/{id}`, the history rows, and the
 * `sessionID` on every SSE frame — is the uuid whenever a transcript exists.
 * A child with no file yet (created, never prompted) falls back to its tau id;
 * losing those is fine.
 */
function fileUuid(file) {
  if (!file) return null
  const base = path.basename(String(file))
  if (!base.endsWith('.jsonl')) return null
  const i = base.lastIndexOf('_')
  if (i < 0) return null
  const uuid = base.slice(i + 1, -'.jsonl'.length)
  // pi ids are UUID-ish; refuse anything that looks like a collision suffix
  return /^[0-9a-f]{8}-[0-9a-f]{4}-/i.test(uuid) ? uuid : null
}

/** the id this session is known by outside the server */
function canonicalId(s) {
  if (!s) return null
  return fileUuid(s.sessionFile) ?? s.id
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

// ---------------------------------------------------------------------------
// id derivation — identical on the live path and the read path
// ---------------------------------------------------------------------------

const LIVE_ROLES = new Set(['user', 'assistant'])

function sha12(s) {
  return createHash('sha1').update(s).digest('hex').slice(0, 12)
}

/** key -> occurrence index, walked in conversation order on BOTH paths. */
function msgId(role, ts, dupSeen) {
  const key = `${role}|${ts ?? ''}`
  const dup = dupSeen.get(key) ?? 0
  dupSeen.set(key, dup + 1)
  return `m_${sha12(`${key}|${dup}`)}`
}

function messagesOf(entries) {
  return (entries ?? []).filter((e) => e?.type === 'message' && e.message)
}

/**
 * root -> leaf chain of an entry list, by parentId (tau does the same after a
 * navigation: `session.entries = pathFromRoot(byId, newLeafId)`).
 *
 * A pi transcript is an append-only TREE. Without navigation it is linear and
 * this is a no-op; after a revert the abandoned branch is still in the file, and
 * pi's own view is the chain ending at the LAST entry (the `tau:navigate-tree`
 * marker sits at the new leaf). Seeding a resumed child with the raw file instead
 * re-shows messages the user reverted away — the transcript would disagree with
 * what the model actually sees.
 */
function leafChain(entries) {
  const list = entries ?? []
  const byId = new Map()
  for (const e of list) if (e?.id) byId.set(e.id, e)
  let leaf = null
  for (let i = list.length - 1; i >= 0; i--) {
    if (list[i]?.id && list[i].type !== 'session') {
      leaf = list[i]
      break
    }
  }
  if (!leaf) return list
  const path = []
  const seen = new Set()
  let cur = leaf
  while (cur && !seen.has(cur.id)) {
    seen.add(cur.id)
    path.push(cur)
    cur = cur.parentId ? byId.get(cur.parentId) : null
  }
  path.reverse()
  return path
}

/**
 * pi's own session-id shape: uuidv7 (core/session-manager.js `createSessionId`).
 * The transcript FILE name carries it, and the file name is the public id, so it
 * is worth producing the same flavour pi does (time-ordered, sortable).
 */
function newSessionId() {
  const b = randomBytes(16)
  let ms = Date.now()
  for (let i = 5; i >= 0; i--) {
    b[i] = ms & 0xff
    ms = Math.floor(ms / 256)
  }
  b[6] = (b[6] & 0x0f) | 0x70 // version 7
  b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
  const h = b.toString('hex')
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`
}

/** how many forks already point at this transcript (the webui wants "(fork #N)") */
function forkCount(file) {
  let n = 0
  for (const f of listFiles()) {
    if (path.resolve(f) === path.resolve(file)) continue
    const h = readJsonl(f).find((e) => e?.type === 'session')
    if (h?.parentSession && path.resolve(h.parentSession) === path.resolve(file)) n++
  }
  return n
}

/**
 * Fork a transcript the way pi's `SessionManager.forkFrom` does — a new
 * `<ts>_<uuid>.jsonl` written `wx`, a header carrying `parentSession: <source>`,
 * then the copied entries — with three differences the webui needs: only the
 * ACTIVE path is copied (`leafChain`, so a reverted branch does not come back to
 * life), it is cut at the fork point (pi's own `position: "before"`: the picked
 * message is EXCLUDED), and the fork is NAMED, because the webui says the engine
 * titles the fork itself. Naming it with a `session_info` entry also marks it as
 * named, so the auto-tiler will not overwrite it.
 *
 * The point of doing this on the file instead of through pi's `fork`/`clone` RPC:
 * pi's runtime fork REBINDS the live child to the new file — right for the TUI,
 * which switches into the fork, wrong here, where the old tab stays open under its
 * own uuid and would go on writing the NEW transcript.
 */
function forkTranscript(file, keepEntries, name) {
  const srcHeader = readJsonl(file).find((e) => e?.type === 'session') ?? {}
  const stamp = new Date().toISOString()
  const id = newSessionId()
  const dest = path.join(path.dirname(file), `${stamp.replace(/[:.]/g, '-')}_${id}.jsonl`)
  const header = {
    type: 'session',
    version: 3,
    id,
    timestamp: stamp,
    cwd: liveCwd(srcHeader.cwd) || PI_CWD,
    parentSession: path.resolve(file),
  }
  const fd = fs.openSync(dest, 'wx') // never clobber
  try {
    fs.writeFileSync(fd, JSON.stringify(header) + '\n')
    for (const e of keepEntries) fs.writeFileSync(fd, JSON.stringify(e) + '\n')
    if (name) {
      const last = keepEntries.length ? keepEntries[keepEntries.length - 1].id : null
      fs.writeFileSync(fd, JSON.stringify({ type: 'session_info', id: 'forkname', parentId: last, timestamp: stamp, name }) + '\n')
    }
  } finally {
    fs.closeSync(fd)
  }
  return { file: dest, id }
}

/**
 * webui message id -> entry id inside a FILE's chain (the fork reads the file,
 * not a child, so the same derived-id walk has to work without RPC).
 */
function entryIdInChain(chain, messageID) {
  const dupSeen = new Map()
  for (const entry of chain) {
    const m = entry?.type === 'message' ? entry.message : null
    if (!m || !LIVE_ROLES.has(m.role)) continue
    if (msgId(m.role, m.timestamp, dupSeen) === messageID) return entry.id ?? null
  }
  return chain.some((x) => x?.id === messageID) ? messageID : null
}

/**
 * webui message id -> pi entry id, resolved against the CHILD's own tree.
 *
 * Message ids are DERIVED (`msgId(role, timestamp, dup counter)`), so the only
 * faithful inverse is to walk the same rows with the same function in the same
 * order — and the rows must be pi's, root->leaf: tau's mirror entries carry no
 * `id`/`parentId` at all (sessions.js pushes `{type:'message', message}`), so
 * they cannot name a navigation target, and after a revert the mirror's shape is
 * whatever the last navigation left behind.
 */
async function messageEntryId(s, messageID) {
  const r = await s.send({ type: 'get_tree' }, { timeoutMs: 15000 })
  if (r?.success === false) return { error: String(r?.error ?? 'get_tree failed') }
  const d = r?.data ?? {}
  const byId = flattenTree(d.tree ?? [])
  const chain = pathFromRoot(byId, d.leafId ?? null)
  const dupSeen = new Map()
  for (const entry of chain) {
    const m = entry?.type === 'message' ? entry.message : null
    if (!m || !LIVE_ROLES.has(m.role)) continue
    if (msgId(m.role, m.timestamp, dupSeen) === messageID) return { entryId: entry.id ?? null }
  }
  // an API user may hand us pi's own entry id
  return chain.some((x) => x?.id === messageID) ? { entryId: messageID } : { error: null }
}

/**
 * pi's `compaction` entries — the OTHER kind of transcript row.
 *
 * `compact()` (manual `/compact`, or auto at the context ceiling) appends a
 * `compaction` entry holding the generated summary + `firstKeptEntryId` +
 * `tokensBefore`. Projecting only `type: "message"` — which is what opencode's
 * transcript vocabulary has — threw that text away: a compacted session read
 * back with no sign it had ever been compacted. This is the only place the
 * summary exists, and pi never re-emits it as a message.
 *
 * There is no compaction row type in the shared webui, so it lands as a normal
 * assistant message with a bold header line: the markdown renderer gives it a
 * heading, and it stays recognisable in a copied transcript too. `role: user`
 * would label it "you" and hand it a revert/fork button — wrong for generated
 * text — so it takes the role that actually produced it.
 *
 * The id is derived from the two fields BOTH surfaces carry (`firstKeptEntryId`
 * + `tokensBefore`), not from the timestamp: the live `compaction_end` event
 * has no timestamp, and a live row and the file row must be the SAME message id
 * or the post-idle refetch would duplicate/swap the row. `tokensBefore` grows
 * monotonically, so two compactions can never collide.
 */
function compactionId(entry) {
  return `cmp_${sha12(`${entry?.firstKeptEntryId ?? ''}|${entry?.tokensBefore ?? ''}`)}`
}

function compactionRow(entry) {
  const mid = compactionId(entry)
  const created = Date.parse(entry.timestamp ?? '') || 0
  const before = Number(entry.tokensBefore)
  const after = Number(entry.estimatedTokensAfter)
  // The head line (counts + label) belongs to the component, so nothing here
  // formats prose: `summary` rides raw and `state` carries the numbers.
  const summary = String(entry.summary ?? '').trim()
  return {
    id: mid,
    role: 'assistant',
    time: { created, completed: created },
    // pi stamps usage on the entry; the webui's token tally reads `tokens`
    tokens: entry.usage
      ? {
          input: entry.usage.input ?? 0,
          output: entry.usage.output ?? 0,
          reasoning: entry.usage.reasoning ?? 0,
          cache: { read: entry.usage.cacheRead ?? 0, write: entry.usage.cacheWrite ?? 0 },
        }
      : undefined,
    compacted: true, // for checks/clients; normalizeMessages drops unknown keys
    // A TYPED part, because a component needs something to put a box around and
    // the numbers must arrive structured: `normalizeMessages` (webui sse.ts:219)
    // whitelists part keys — type/text/state/synthetic survive, ROW-level keys
    // are dropped — so `type` + `state` are the only marker that reaches a
    // component. The head line is deliberately NOT invented into `text`: pi's
    // summary lands raw and the component owns the wording (M3's
    // `details.cmpbox` in Transcript.svelte builds it from `state`).
    // There is deliberately NO second text part: the box renders this one, so a
    // copy would be drawn twice. A webui build without the branch renders nothing
    // for the type — the bridge and the dist are deployed together (pi-web serves
    // the sibling checkout's `dist`), and both are rebuilt in one step.
    parts: [
      {
        id: `${mid}:0`,
        type: 'compaction',
        text: summary,
        synthetic: true,
        state: {
          status: 'ok',
          at: created || undefined,
          ...(Number.isFinite(before) ? { tokensBefore: before } : {}),
          ...(Number.isFinite(after) ? { tokensAfter: after } : {}),
          ...(entry.firstKeptEntryId ? { firstKeptEntryId: String(entry.firstKeptEntryId) } : {}),
        },
      },
    ],
  }
}

/**
 * The transcript source for a session.
 *
 * A resumed child gets its entries from pi-web's own `readJsonl()` (tau stores
 * what it is handed), so file-side `compaction` entries are already in there.
 * But tau only records `message_start`/`message_end` (`trackMessage`), so a
 * compaction that happens INSIDE a live child never reaches `s.entries` — it
 * would disappear on the very next refetch and only reappear once the child was
 * reaped and the file re-read. Live compactions are therefore kept here and
 * merged in, deduped by id against the file copy.
 */
/**
 * Entry timestamp in ms, across the three entry shapes that reach the read path:
 * pi's JSONL entries (ISO string at the top level), tau's pushed entries (the
 * stamp lives on `message`), and the bridge's synthesized compaction entry (ISO
 * string). NaN means unknown, and unknown never becomes an insertion point.
 */
function entryTime(entry) {
  const raw = entry?.timestamp ?? entry?.message?.timestamp
  if (typeof raw === 'number') return Number.isFinite(raw) ? raw : NaN
  const p = Date.parse(String(raw ?? ''))
  return Number.isFinite(p) ? p : NaN
}

function transcriptEntries(s, e) {
  const base = s?.entries ?? []
  const seen = new Set(base.filter((x) => x?.type === 'compaction').map(compactionId))
  const add = (e?.compactions ?? []).filter((c) => !seen.has(compactionId(c)))
  if (!add.length) return base
  // Splice in at the moment the compaction HAPPENED, never at the end. For a
  // live child `base` holds no compaction entry at all, so the dedupe above is
  // always empty and an append re-rendered "Context compacted" as the newest
  // message on every refetch — for hours, until the child was reaped — even
  // though whole turns had gone past it (compact-check caseOk).
  const out = [...base]
  for (const c of add.sort((a, b) => entryTime(a) - entryTime(b))) {
    const t = entryTime(c)
    let at = out.length
    if (Number.isFinite(t)) {
      at = out.findIndex((x) => {
        const xt = entryTime(x)
        return Number.isFinite(xt) && xt > t
      })
      if (at < 0) at = out.length
    }
    out.splice(at, 0, c)
  }
  return out
}

// ---------------------------------------------------------------------------
// pi message -> opencode message (the one real translation)
// ---------------------------------------------------------------------------

function textOf(content) {
  if (typeof content === 'string') return content
  // `tool_execution_end` hands over the whole ToolResult object
  // ({ content, details }), not its content blocks — unwrap it or the row
  // renders with no output until the toolResult message_end lands.
  if (content && typeof content === 'object' && !Array.isArray(content) && 'content' in content)
    return textOf(content.content)
  if (Array.isArray(content))
    return content
      .filter((b) => b?.type === 'text')
      .map((b) => b.text ?? '')
      .join('')
  return ''
}

/**
 * pi's tool `details` -> opencode's tool-part metadata.
 *
 * The transcript renders an edit row's diff from `state.metadata.filediff.patch`
 * (opencode's shape; `state.metadata.diff` is its fallback). pi's edit tool
 * produces exactly that artefact — `result.details.patch`, a unified diff from
 * `Diff.createTwoFilesPatch`, persisted in the JSONL toolResult message — but
 * the bridge hard-coded `metadata: {}` at every emission site, so the webui had
 * nothing to draw: an edit row showed its path and no diff block. (pi's `write`
 * still worked by accident: the webui synthesises an all-adds pseudo-patch from
 * `state.input.content`, and pi's write input has a `content` arg; pi's edit
 * input is `edits[]`, which that fallback cannot see.)
 *
 * Only the patch is copied, never the whole `details` blob: bash truncation
 * details carry the full displaced content (tens of KB) and would be repeated
 * in every SSE frame. Tools whose details has no string patch keep `{}`.
 */
function toolMetadata(input, details) {
  const patch = typeof details?.patch === 'string' ? details.patch.trim() : ''
  if (!patch) return {}
  const lines = patch.split('\n')
  const header = /^\+\+\+ (.+)$/m.exec(patch)
  return {
    filediff: {
      file: input?.path ?? input?.filePath ?? input?.file_path ?? header?.[1] ?? '',
      patch,
      additions: lines.filter((l) => l.startsWith('+') && !l.startsWith('+++')).length,
      deletions: lines.filter((l) => l.startsWith('-') && !l.startsWith('---')).length,
    },
    // kept in sync: a client that only reads `metadata.diff` still gets a real
    // unified diff, never pi's pretty `details.diff` (line-numbered, not patch)
    diff: patch,
    ...(Number.isFinite(details.firstChangedLine)
      ? { firstChangedLine: details.firstChangedLine }
      : {}),
  }
}

function safeJson(s) {
  if (typeof s !== 'string') return s ?? {}
  try {
    return JSON.parse(s)
  } catch {
    return { _raw: s }
  }
}

/**
 * Heuristic session title — the bridge is the one that names sessions.
 *
 * pi never names a session by itself (no auto-name anywhere in pi), and tau's
 * `maybeTitle` heuristic lives only in tau's memory, so it can never reach the
 * sidebar: a sidebar row is a projection of the JSONL. So the bridge derives a
 * name and hands it to pi's `set_session_name`, which appends the
 * `session_info` entry — after that pi, the TUI, the read path and the webui all
 * read the SAME string, and pi reports it back as `session_info_changed`.
 *
 * Deliberately dumb: a model call per session on a single-slot local router
 * costs more than a name is worth. Rules — collapse whitespace (pi normalizes
 * newlines in names anyway), strip conversational filler, cut at a sentence end
 * inside the first 80 chars, else clamp to 60 at a word boundary with an
 * ellipsis, capitalise the first character.
 */
const FILLER_PREFIX =
  /^(?:ok|okay|so|actually|hey|hi|hello|please|can you|could you|would you|i want(?:ed)? to|i wanna|let'?s|now|also|just)\b[\s,:!-]*/i

function autoTitle(text) {
  if (typeof text !== 'string') return ''
  let t = text.replace(/\s+/g, ' ').trim()
  let prev = ''
  while (t !== prev) {
    prev = t
    t = t.replace(FILLER_PREFIX, '').trim()
  }
  if (!t) return ''
  const se = t.search(/[.!?](\s|$)/)
  if (se > 10 && se < 80) t = t.slice(0, se + 1)
  if (t.length > 60) {
    const cut = t.slice(0, 57)
    const ws = cut.lastIndexOf(' ')
    t = (ws > 20 ? cut.slice(0, ws) : cut).trimEnd() + '…'
  }
  return t.charAt(0).toUpperCase() + t.slice(1)
}

/** the first user prompt in a set of pi session entries ('' when there is none) */
function firstUserText(entries) {
  for (const e of entries ?? []) {
    if (e?.type === 'message' && e.message?.role === 'user') {
      const t = textOf(e.message.content).trim()
      if (t) return t
    }
  }
  return ''
}

function toolTitle(name, input) {
  if (!input || typeof input !== 'object') return ''
  const v =
    input.command ??
    input.filePath ??
    input.file_path ??
    input.path ??
    input.pattern ??
    input.url ??
    input.query ??
    input.description
  return v ? String(v).replace(/\s+/g, ' ').slice(0, 160) : ''
}

// Minimum gap between two detail-line frames for the SAME streaming tool call.
// pi emits one `toolcall_delta` per token of argument JSON; the webui shallow-
// merges part state, so a frame per token is a frame per token of UI work. The
// authoritative frame always follows at toolcall_end, so coalescing here only
// costs a slightly staler typing effect.
const TOOL_TITLE_MS = Math.max(0, Number(process.env.PI_WEB_TOOL_TITLE_MS ?? 250))
const PARTIAL_CAP = 4096 // bytes of argument JSON scanned for the detail line

// The same fields toolTitle prefers, in the same order.
const TITLE_KEYS = ['command', 'filePath', 'file_path', 'path', 'pattern', 'url', 'query', 'description']
const TITLE_RE = (k) => new RegExp(`"${k}"\\s*:\\s*"((?:[^"\\\\]|\\\\.)*)`)

/**
 * Read the detail line out of a PARTIAL tool-call argument document.
 *
 * pi streams a tool call's arguments as `toolcall_delta` fragments, and nothing
 * else on the wire names the call before it is complete: `toolcall_start` is
 * `{id, toolName}` and `toolcall_end` is the first record carrying arguments.
 * So a row that stays blank until the model finishes writing the call — and a
 * row that shows nothing at all while a long `command` streams — is a choice
 * about who reads `toolcall_delta`. The value is decoded from however much JSON
 * has arrived: the field's opening quote is enough, an unterminated escape is
 * dropped rather than shown raw.
 */
function partialTitle(name, text) {
  if (!text) return ''
  for (const k of TITLE_KEYS) {
    const m = TITLE_RE(k).exec(text)
    if (!m) continue
    let v = m[1]
    try {
      v = JSON.parse('"' + v + '"')
    } catch {
      const trimmed = v.replace(/\\+$/, '') // cut mid-escape: show what is unambiguous
      try {
        v = JSON.parse('"' + trimmed + '"')
      } catch {
        /* keep the raw fragment; toolTitle clips it */
      }
    }
    const t = toolTitle(name, { [k]: v })
    if (t) return t
  }
  return ''
}

/**
 * Pure projection: pi's message list -> opencode transcript shape.
 *
 * Positions/blocks are addressed by (derived message id, content index), so
 * pi's own `contentIndex` during streaming and the array position during a
 * read always name the same part.
 *
 * `toolResult` and `system` messages are not rendered — opencode keeps tool
 * output inside the tool part (`state.output`) and has no system rows — but
 * toolResult is consumed here for the join.
 */
function toOcMessages(entries) {
  const list = entries ?? []
  // the join map is built over the WHOLE session first: pi writes the
  // toolResult message after the assistant message that called it
  const msgs = messagesOf(list).map((e) => e.message)
  const results = new Map()
  for (const m of msgs) if (m.role === 'toolResult' && m.toolCallId) results.set(m.toolCallId, m)

  // message ids stay exactly what the message-only walk produced: same order,
  // same dupSeen counter, so live ids keep matching the read path
  const dupSeen = new Map()
  const out = []
  const seenCompaction = new Set()
  for (const entry of list) {
    if (entry?.type === 'compaction') {
      const row = compactionRow(entry)
      if (seenCompaction.has(row.id)) continue // live record + file entry = one row
      seenCompaction.add(row.id)
      out.push(row)
      continue
    }
    const m = entry?.type === 'message' ? entry.message : null
    if (!m || !LIVE_ROLES.has(m.role)) continue
    const mid = msgId(m.role, m.timestamp, dupSeen)
    out.push(toOcMessage(m, mid, results))
  }
  return out
}

function toOcMessage(m, mid, results) {
  const content = Array.isArray(m.content)
    ? m.content
    : [{ type: 'text', text: String(m.content ?? '') }]
  const parts = []
  const created = isoMs(m.timestamp)
  // generation end only (pi's durationMs excludes tool time), extended below
  // by every tool this message called: the transcript splits the message
  // window into agent time vs tool time, so it needs gen + tools (opencode's
  // semantics) or tokens/s under-reads by the tool runtime.
  let completed = m.durationMs != null ? created + m.durationMs : created
  content.forEach((b, i) => {
    if (!b) return
    const id = `${mid}:${i}`
    if (b.type === 'text' && m.role === 'user') {
      parts.push({ id, type: 'text', text: b.text ?? '' })
    } else if (b.type === 'text') {
      parts.push({ id, type: 'text', text: b.text ?? '' })
    } else if (b.type === 'thinking') {
      parts.push({ id, type: 'reasoning', text: b.thinking ?? b.text ?? '' })
    } else if (b.type === 'toolCall') {
      const res = results.get(b.id)
      const input = safeJson(b.arguments)
      const args = typeof input === 'object' && input !== null ? input : {}
      const time = toolWindow(res, created)
      if (time.end > completed) completed = time.end
      parts.push({
        id,
        type: 'tool',
        tool: b.name,
        callID: b.id,
        state: {
          status: res ? (res.isError ? 'error' : 'completed') : 'running',
          title: toolTitle(b.name, args),
          input: args,
          output: res ? textOf(res.content) : '',
          // cold path: pi persisted `details` on the toolResult message, so a
          // refetch/reload must rebuild the same diff the live stream showed
          metadata: toolMetadata(args, res?.details),
          time,
        },
      })
    } else if (b.type === 'file') {
      parts.push({ id, type: 'file', url: b.url ?? '', mime: b.mime ?? '', filename: b.filename ?? '' })
    }
  })

  const usage = m.usage ?? {}
  const tokens = {
    input: usage.input ?? usage.promptTokens ?? 0,
    output: usage.output ?? usage.completionTokens ?? 0,
    reasoning: usage.reasoning ?? 0,
    cache: { read: usage.cacheRead ?? 0, write: usage.cacheWrite ?? 0 },
  }
  const anyTokens = Object.values(tokens).some((v) => (typeof v === 'object' ? Object.values(v).some(Boolean) : v))

  // pi's assistant messages carry their own `durationMs` (479/479 in history)
  // and their `timestamp` is the turn START (ts + duration < next user ts) —
  // the old `completed: Date.now()` stamped both ends with "now", so the
  // transcript's duration was 0 ms and tokens/s never rendered.
  return {
    id: mid,
    role: m.role,
    time: { created, completed },
    tokens: anyTokens ? tokens : undefined,
    modelID: m.model ?? undefined,
    providerID: m.provider ?? undefined,
    error: m.error ?? undefined,
    parts,
  }
}

/**
 * pi's toolResult: `timestamp` is the END and `durationMs` the tool's own
 * runtime (412/412 in history) — the live `tool_execution_end` event carries
 * neither, so the join is the only place a history transcript can get a real
 * per-tool window (`bash 24ms`, not `bash 0ms`).
 */
function toolWindow(res, created) {
  if (!res) return {}
  const end = isoMs(res.timestamp)
  const dur = res.durationMs
  const start = typeof dur === 'number' && Number.isFinite(dur) ? Math.max(created, end - dur) : end
  return { start, end }
}

/**
 * pi stamps messages with epoch **milliseconds numbers** (`1792713118085`),
 * not ISO strings — and `Date.parse(number)` is NaN (it stringifies first),
 * which silently turned every `created` into "right now". Accept numbers
 * (sec-epoch too, for safety) before falling back to Date.parse.
 */
function isoMs(ts) {
  if (typeof ts === 'number' && Number.isFinite(ts)) return ts < 1e12 ? ts * 1000 : ts
  if (ts) {
    const t = Date.parse(ts)
    if (!Number.isNaN(t)) return t
  }
  return Date.now()
}

// ---------------------------------------------------------------------------
// live pi event -> opencode SSE
// ---------------------------------------------------------------------------

function emitFor(tauId, type, properties) {
  // ONE frame per event, addressed by the session's canonical public id (pi's
  // transcript uuid as soon as the child knows its file). `publicIdsFor` keeps the
  // tau handle resolvable INBOUND — a tab that opened with it can still address the
  // session — but echoing every frame to both ids doubled the entire SSE stream for
  // any session whose child knew its file (a 120-delta turn arrived as 240+ frames).
  const s = liveManager.get(tauId)
  emit(type, { ...properties, sessionID: canonicalId(s) ?? tauId })
}

/**
 * `session.updated` is the ONE opencode event pi-web must not mirror verbatim.
 *
 * tau calls `broadcastUpdated()` after **every** pi event and every RPC
 * response (sessions.js: `touch(true)` in handleResponse, and the tail of
 * handleEvent). Mirroring that put a `session.updated` beside every single
 * token delta — measured on the live bridge over 170 s: 6,090 session.updated
 * vs 5,375 message.part.delta, i.e. 1.13 per delta, 53% of ALL SSE frames.
 * The webui coalesces text deltas (~40 ms, sse.ts bufferDelta) but its
 * `session.updated` branch goes straight into `tabs.patch()` +
 * `patchMetrics()` — one uncoalesced full store notify per token, so the
 * transcript re-renders per token. opencode's engine only sends this event on
 * real metadata changes; that asymmetry is why the webui feels laggier on pi.
 *
 * So: republish only when the projection actually changes. `time.updated`
 * (tau's lastActiveAt, bumped per event) is the only field that really moves
 * mid-turn and is deliberately OUT of the signature — the webui uses it for
 * sidebar ordering only, and the forced emit at turn end refreshes it.
 */
const infoSig = new Map() // tauId -> signature of the last session.updated sent

function sessionSig(s) {
  if (!s) return ''
  const i = ocSession(s, undefined, false) // signature never carries time/tokens: skip the file stat
  if (!i) return ''
  return [
    i.title ?? '',
    i.cwd ?? '',
    i.cost ?? 0,
    i.model?.providerID ?? '',
    i.model?.id ?? '',
    i.revert ? 1 : 0,
  ].join('\u0000')
}

function emitSessionUpdated(tauId, s, publicId, force = false) {
  const sig = sessionSig(s)
  const changed = infoSig.get(tauId) !== sig
  if (changed) infoSig.set(tauId, sig)
  if (!changed && !force) return false
  const info = ocSession(s, publicId)
  if (!info) return false
  emitFor(tauId, 'session.updated', { info })
  return true
}

function onTauFrame(frame) {
  // `created` is a tab attaching to a child (new or resumed) — always announce.
  // `updated` fires from tau after EVERY event/RPC response — gate it.
  if (frame.type === 'live_session_created' || frame.type === 'live_session_updated') {
    const s = liveManager.get(frame.session?.id)
    if (s) emitSessionUpdated(s.id, s, undefined, frame.type === 'live_session_created')
    return
  }
  if (frame.type === 'live_session_closed') {
    const ids = publicIdsFor(frame.sessionId)
    live.delete(frame.sessionId)
    infoSig.delete(frame.sessionId)
    // A reaper close is invisible to the client on purpose: `session.deleted`
    // makes the webui CLOSE the tab (sse.ts closes it and bumps the sidebar
    // dirty counter). The child is gone but the session is not — the next
    // request resumes it from its JSONL under the same public id.
    const reapedFrom = INTERNAL_CLOSE.get(frame.sessionId)
    INTERNAL_CLOSE.delete(frame.sessionId)
    for (const id of ids) {
      byPublicId.delete(id)
      if (reapedFrom) reapedFile.set(id, reapedFrom)
      else emit('session.deleted', { sessionID: id })
    }
    return
  }
  if (frame.type !== 'event') {
    noteUnhandled('tau-frame', frame.type, frame.session ? { id: frame.session.id } : undefined)
    return
  }
  if (!frame.event || !frame.sessionId) {
    noteUnhandled('tau-event-malformed', Object.keys(frame).join('+'))
    return
  }
  onPiEvent(frame.sessionId, frame.event)
}

function onPiEvent(tauId, ev) {
  const e = live.get(tauId) ?? entryFor(tauId)
  const session = liveManager.get(tauId)
  const t = ev.type

  // pi enqueued or drained: mirror the complete lists and republish them.
  if (t === 'queue_update') {
    e.queue = {
      steering: (Array.isArray(ev.steering) ? ev.steering : []).map(String),
      followUp: (Array.isArray(ev.followUp) ? ev.followUp : []).map(String),
    }
    emitFor(tauId, 'session.queue', { queue: e.queue })
    return
  }

  if (t === 'session_info_changed') {
    // pi's own rename (TUI /name, set_session_name from any client, an
    // extension). This one is authoritative: pi has already persisted a
    // session_info entry, so the sidebar row will show it on the next read.
    e.title = ev.name || null
    e.named = !!ev.name
    emitSessionUpdated(tauId, session)
    return
  }

  if (t === 'session_name' && ev.name) {
    // tau's in-memory heuristic (maybeTitle): a candidate, not a fact — it is
    // never written to the JSONL, so the file-backed sidebar row cannot show it.
    // Show it now, then make it real through pi so both agree.
    if (!e.named) e.title = ev.name
    void autoName(tauId)
    emitSessionUpdated(tauId, session)
    return
  }

  if (t === 'message_start' && ev.message && LIVE_ROLES.has(ev.message.role)) {
    const mid = msgId(ev.message.role, ev.message.timestamp, e.dup)
    e.cur = { mid, role: ev.message.role, byIndex: new Map(), byTool: new Map() }
    if (ev.message.role === 'user') void autoName(tauId, textOf(ev.message.content))
    // a revert point is a promise about the NEXT prompt: opencode prunes for
    // real when the turn starts, so the marker must not outlive it (webui
    // stores.ts RevertPoint). The leaf already moved, so the transcript is right
    // either way — a stale marker would just keep cutting the view.
    if (e.revert) {
      e.revert = null
      const ses = liveManager.get(tauId)
      if (ses) emitSessionUpdated(tauId, ses)
    }
    emitFor(tauId, 'message.updated', { info: { id: mid, role: ev.message.role, sessionID: tauId, time: { created: isoMs(ev.message.timestamp) } } })
    const c = Array.isArray(ev.message.content) ? ev.message.content : []
    c.forEach((b, i) => emitBlock(tauId, ev.message, b, i, e))
    return
  }

  if (t === 'message_update' && ev.assistantMessageEvent) {
    const cur = e.cur
    if (!cur) return
    const a = ev.assistantMessageEvent
    if (a.type === 'text_start' || a.type === 'thinking_start') {
      const i = a.contentIndex ?? 0
      cur.byIndex.set(i, `${cur.mid}:${i}`)
      emitFor(tauId, 'message.part.updated', {
        part: {
          id: `${cur.mid}:${i}`,
          messageID: cur.mid,
          sessionID: tauId,
          type: a.type === 'text_start' ? 'text' : 'reasoning',
          text: '',
        },
      })
    } else if (a.type === 'text_delta' || a.type === 'thinking_delta') {
      noteDerived(e, (a.delta ?? '').length)
      const i = a.contentIndex ?? 0
      const pid = cur.byIndex.get(i) ?? `${cur.mid}:${i}`
      cur.byIndex.set(i, pid)
      emitFor(tauId, 'message.part.delta', {
        sessionID: tauId,
        messageID: cur.mid,
        partID: pid,
        field: 'text',
        delta: a.delta ?? '',
      })
    } else if (a.type === 'text_end' || a.type === 'thinking_end') {
      const i = a.contentIndex ?? 0
      const pid = cur.byIndex.get(i) ?? `${cur.mid}:${i}`
      emitFor(tauId, 'message.part.updated', {
        part: {
          id: pid,
          messageID: cur.mid,
          sessionID: tauId,
          type: a.type === 'text_end' ? 'text' : 'reasoning',
          text: a.content ?? '',
        },
      })
    } else if (a.type === 'toolcall_start' || a.type === 'toolcall_end') {
      const i = a.contentIndex ?? 0
      const pid = `${cur.mid}:${i}`
      cur.byIndex.set(i, pid)
      const name = a.toolName ?? a.toolCall?.name ?? 'tool'
      // pi puts `id`/`toolName` ONLY on toolcall_start (modes/json-event.js);
      // the end record carries the complete toolCall and NO top-level id. Keying
      // the join on `a.id` therefore meant the arguments never reached `byCall`,
      // and every later frame recomputed the title from `{}`.
      const st = cur.byTool.get(i) ?? { cid: null, name, start: Date.now(), text: '', title: '', at: 0 }
      st.name = name
      if (a.id) st.cid = a.id
      cur.byTool.set(i, st)
      const args = a.type === 'toolcall_end' ? safeJson(a.toolCall?.arguments) : null
      let hit = st.cid ? e.byCall.get(st.cid) : null
      if (args && typeof args === 'object') {
        if (hit) {
          hit.tool = name
          hit.input = args
        } else if (st.cid) {
          e.byCall.set(st.cid, { pid, mid: cur.mid, tool: name, input: args, metadata: {} })
          hit = e.byCall.get(st.cid)
        }
      } else if (!hit && st.cid) {
        e.byCall.set(st.cid, { pid, mid: cur.mid, tool: name, input: {}, metadata: {} })
        hit = e.byCall.get(st.cid)
      }
      const input = hit?.input ?? {}
      const title = toolTitle(name, input) || st.title
      if (title) {
        st.title = title
        st.at = Date.now()
      }
      emitFor(tauId, 'message.part.updated', {
        part: {
          id: pid,
          messageID: cur.mid,
          sessionID: tauId,
          type: 'tool',
          tool: name,
          callID: st.cid,
          state: {
            status: 'running',
            title,
            input,
            output: '',
            metadata: hit?.metadata ?? {},
            time: { start: st.start },
          },
        },
      })
    } else if (a.type === 'toolcall_delta') {
      // The argument JSON streams through HERE, and nothing else on the wire
      // names a tool call before it is complete. Reading the value out of the
      // partial document is what lets the row show its command/path while the
      // model is still writing the call, instead of at the end of it.
      const i = a.contentIndex ?? 0
      const st = cur.byTool.get(i)
      if (!st) return
      st.text += String(a.delta ?? '')
      // A `write`/`edit` argument blob can be hundreds of KB, and the scan runs
      // once per delta — so bound WHAT IS SCANNED (the title field lives in the
      // leading bytes; the COMPLETE object arrives at toolcall_end anyway) rather
      // than skipping deltas, which would lose the typing effect entirely.
      if (st.text.length > PARTIAL_CAP) st.text = st.text.slice(0, PARTIAL_CAP)
      const t = partialTitle(st.name, st.text)
      if (!t || t === st.title) return
      const now = Date.now()
      if (st.title && now - st.at < TOOL_TITLE_MS) return // coalesced: never one frame per token
      st.title = t
      st.at = now
      const hit = st.cid ? e.byCall.get(st.cid) : null
      emitFor(tauId, 'message.part.updated', {
        part: {
          id: `${cur.mid}:${i}`,
          messageID: cur.mid,
          sessionID: tauId,
          type: 'tool',
          tool: st.name,
          callID: st.cid,
          state: {
            status: 'running',
            title: t,
            input: hit?.input ?? {}, // a PARTIAL argument is not an argument
            output: '',
            metadata: hit?.metadata ?? {},
            time: { start: st.start },
          },
        },
      })
    } else if (a.type === 'error') {
      const aborted = /abort/i.test(String(a.reason ?? ''))
      emitFor(tauId, 'session.error', {
        sessionID: tauId,
        error: aborted
          ? { name: 'MessageAbortedError', data: { message: 'aborted' } }
          : { name: 'APIError', data: { message: String(a.error ?? a.reason ?? 'provider error') } },
      })
    } else {
      noteUnhandled('pi-assistant-event', a.type, a)
    }
    return
  }

  if (t === 'tool_execution_start' || t === 'tool_execution_update' || t === 'tool_execution_end') {
    const hit = e.byCall.get(ev.toolCallId)
    if (!hit) return
    if (t === 'tool_execution_start') e.toolStart.set(ev.toolCallId, Date.now())
    const res = t === 'tool_execution_end' ? ev.result : undefined
    const partial = t === 'tool_execution_update' ? ev.partialResult : undefined
    // pi's end record is {toolCallId, toolName, result, isError, durationMs} and
    // carries NO args (only start/update do). Recomputing the detail line from an
    // absent arguments object blanked a line the tab ALREADY had — the webui
    // shallow-merges part state, so a blank frame replaces a populated one. The
    // row went blank exactly when the tool finished and healed only when the
    // post-turn refetch reprojected the transcript from the JSONL: that is
    // "tool titles only appear after the turn ends". Fall back to what the call
    // itself carried.
    const sent = safeJson(ev.args)
    const input =
      sent && typeof sent === 'object' && Object.keys(sent).length ? sent : (hit.input ?? {})
    const metadata = toolMetadata(input, res?.details)
    if (Object.keys(metadata).length) hit.metadata = { ...(hit.metadata ?? {}), ...metadata }
    // stamp the start ONCE at tool_execution_start: re-stamping Date.now() on
    // every event gave every live tool a 0ms window. pi's end event carries no
    // durationMs in the wild — use it only when we never saw the start.
    const now = Date.now()
    const start =
      e.toolStart.get(ev.toolCallId) ??
      (typeof ev.durationMs === 'number' ? now - ev.durationMs : now)
    emitFor(tauId, 'message.part.updated', {
      part: {
        id: hit.pid,
        messageID: hit.mid,
        sessionID: tauId,
        type: 'tool',
        tool: ev.toolName,
        callID: ev.toolCallId,
        state: {
          status:
            t === 'tool_execution_end' ? (ev.isError ? 'error' : 'completed') : 'running',
          title: toolTitle(ev.toolName, input),
          input,
          output: t === 'tool_execution_end' ? textOf(res) : (partial ?? ''),
          // live path: `ev.result` is the ToolResult object, details included
          metadata,
          time: { start, ...(t === 'tool_execution_end' ? { end: now } : {}) },
        },
      },
    })
    return
  }

  if (t === 'message_end' && ev.message?.role === 'toolResult') {
    // pi emits the toolResult message AFTER tool_execution_end, carrying the
    // authoritative END ts + durationMs — re-complete the part from it so a
    // dropped/duplicated live event can't strand the row at ⏳, and the live
    // window matches the history join exactly.
    const res = ev.message
    const hit = res.toolCallId && e.byCall.get(res.toolCallId)
    if (!hit) return
    emitFor(tauId, 'message.part.updated', {
      part: {
        id: hit.pid,
        messageID: hit.mid,
        sessionID: tauId,
        type: 'tool',
        tool: res.toolName ?? hit.tool,
        callID: res.toolCallId,
        state: {
          status: res.isError ? 'error' : 'completed',
          title: toolTitle(res.toolName ?? hit.tool, hit.input),
          input: hit.input && typeof hit.input === 'object' ? hit.input : {},
          output: textOf(res.content),
          // webui upserts parts with a shallow spread, so a later update whose
          // metadata were `{}` would wipe the diff set at tool_execution_end
          metadata: toolMetadata(hit.input, res.details),
          // created=0: the result's own END ts − durationMs is the window
          // (no assistant-created clamp needed — dur can't predate the epoch)
          time: toolWindow(res, 0),
        },
      },
    })
    return
  }

  if (t === 'message_end' && ev.message && LIVE_ROLES.has(ev.message.role)) {
    // authoritative resync: re-project the whole message so any block missed
    // while disconnected lands with the same ids the read path produces
    const cur = e.cur
    const mid = cur?.role === ev.message.role ? cur.mid : msgId(ev.message.role, ev.message.timestamp, new Map())
    const msgs = messagesOf(session?.entries).map((x) => x.message)
    const results = new Map()
    for (const m of msgs) if (m.role === 'toolResult' && m.toolCallId) results.set(m.toolCallId, m)
    const projected = toOcMessage(ev.message, mid, results)
    for (const p of projected.parts) {
      // Tool parts are driven live by tool_execution_*; without a joinable
      // toolResult yet, re-emitting would clobber completed state back to
      // running. Only emit tool parts when the result is known.
      if (p.type === 'tool' && p.state?.status === 'running' && !results.has(p.callID)) continue
      emitFor(tauId, 'message.part.updated', { part: { ...p, messageID: mid, sessionID: tauId } })
    }
    emitFor(tauId, 'message.updated', { info: { ...projected, id: mid, sessionID: tauId } })
    e.cur = null
    return
  }

  if (t === 'agent_settled') {
    e.engine = null // the box goes away with the turn; a stale block would sit there forever
    e.derived = null
    // one forced metadata frame per turn: it carries the fresh `updated` the
    // sidebar orders by, and the webui's session.updated branch schedules the
    // post-turn transcript refetch (it is busy-gated, so mid-turn frames can
    // never trigger one)
    emitSessionUpdated(tauId, session, undefined, true)
    emitFor(tauId, 'session.idle', { sessionID: tauId })
    e.cur = null
    return
  }
  if (t === 'agent_start' || t === 'turn_start') {
    e.derived = newDerived() // prefill = turn_start -> first delta, rate = delta cadence
    e.engine = null
    emitFor(tauId, 'session.status', { sessionID: tauId, status: { type: 'busy' } })
    return
  }
  if (t === 'compaction_start' || t === 'summarization_retry_scheduled') {
    // The ONLY signal that a summary is in flight. Set it here (not in the
    // /summarize handler): compaction starts in the TUI, at the context ceiling
    // and on a retry too, and every one of those must read as busy.
    e.compacting = true
    e.derived = newDerived()
    // The box's live state starts here, before the first engine sample: a
    // summary that never produces telemetry still has to render as running.
    e.compaction = { state: 'running', at: Date.now() }
    emitFor(tauId, 'session.next.compaction.started', { sessionID: tauId })
    return
  }
  if (t === 'compaction_end' || t === 'summarization_retry_finished') {
    // whatever the outcome, the session can take a prompt again
    e.compacting = false
    const done = !ev.aborted && !ev.errorMessage && ev.result?.summary
    // ONE sentence for this failure, recorded before any surface reads it: the
    // open /summarize POST, the SSE tile below and GET /session/status all quote
    // this string, so the toast, the tile and the box cannot disagree. pi's
    // compaction_end message is the canonical one (its RPC error response
    // carries the same failure UNprefixed).
    e.compaction = {
      state: done ? 'ok' : ev.aborted ? 'cancelled' : 'failed',
      reason: ev.errorMessage ?? null,
      at: Date.now(),
      ...(Number.isFinite(Number(ev.result?.tokensBefore)) ? { tokensBefore: Number(ev.result.tokensBefore) } : {}),
    }
    settleCompaction(tauId)
    emitFor(tauId, 'session.next.compaction.ended', { sessionID: tauId })
    // The summary itself is transcript content, and it arrives ONLY here: pi
    // appends a `compaction` entry to its JSONL and never replays it as a
    // message, and tau's entries only ever hold message events. Record it so
    // the read path can project it, then ship the row live so the open tab
    // shows it without a reload. Same id the file entry will produce.
    if (done) {
      const entry = {
        type: 'compaction',
        timestamp: new Date().toISOString(),
        summary: ev.result.summary,
        firstKeptEntryId: ev.result.firstKeptEntryId,
        tokensBefore: ev.result.tokensBefore,
        estimatedTokensAfter: ev.result.estimatedTokensAfter,
        usage: ev.result.usage,
      }
      const known = e.compactions.some((c) => compactionId(c) === compactionId(entry))
      if (!known) e.compactions.push(entry)
      const alreadyIn =
        known || (session?.entries ?? []).some((x) => x?.type === 'compaction' && compactionId(x) === compactionId(entry))
      if (!alreadyIn) {
        // same order the message_end resync uses: parts first, then the info
        // that materializes the row (setMeta seeds `parts` from info too)
        const row = compactionRow(entry)
        for (const p of row.parts) emitFor(tauId, 'message.part.updated', { part: { ...p, messageID: row.id, sessionID: tauId } })
        emitFor(tauId, 'message.updated', { info: { ...row, sessionID: tauId } })
      }
      // committed: the webui clears `compacting` on this event and schedules the
      // transcript refetch that picks the summary row up from the read path
      emitFor(tauId, 'session.compacted', { sessionID: tauId })
    }
    // A manual compaction ends the summary, NOT a run: pi's compact() aborts
    // whatever was live FIRST, so agent_settled (our only session.idle source)
    // already landed before compaction_start. Without an idle here the webui's
    // busy flag — set by /compact itself and by compaction.started — sticks
    // forever (the stop button stays up, the composer stays in steer mode).
    // tau's isStreaming (true from turn_start, false only at agent_settled) is
    // what separates "this summary belongs to a live turn" (auto threshold /
    // overflow compaction — the turn continues) from "the session is at rest".
    if (session?.isStreaming) return
    const failed = ev.aborted || ev.errorMessage
    if (failed && !compactingWait.has(tauId)) {
      // Nobody is holding an HTTP response to report this on: the /compact POST
      // already answered "started" (proxy ceiling), or the compaction came from
      // the TUI / an auto-retry. The client must still learn, or its
      // "Compacting…" strip never clears. A cancelled compaction is a user
      // abort, not an error: opencode's own abort marker is what the webui
      // filters out of its error tiles.
      emitFor(tauId, 'session.error', {
        sessionID: tauId,
        error: ev.aborted
          ? { name: 'MessageAbortedError', data: { message: 'Compaction cancelled' } }
          : apiErr(ev.errorMessage ?? 'Compaction failed'),
      })
    }
    emitFor(tauId, 'session.idle', { sessionID: tauId })
    return
  }

  // Nothing above claimed it: name it, or it stays invisible forever.
  noteUnhandled('pi-event', t, ev)
}

function emitBlock(tauId, msg, b, i, e) {
  const cur = e.cur
  if (!b || !cur) return
  const pid = `${cur.mid}:${i}`
  cur.byIndex.set(i, pid)
  const base = { id: pid, messageID: cur.mid, sessionID: tauId }
  if (b.type === 'text') {
    emitFor(tauId, 'message.part.updated', { part: { ...base, type: 'text', text: b.text ?? '' } })
  } else if (b.type === 'thinking') {
    emitFor(tauId, 'message.part.updated', { part: { ...base, type: 'reasoning', text: b.thinking ?? b.text ?? '' } })
  } else if (b.type === 'toolCall') {
    const args = safeJson(b.arguments)
    // entry-level (see toolcall_start): message_end precedes tool_execution_*
    e.byCall.set(b.id, { pid, mid: cur.mid, tool: b.name, input: args })
    emitFor(tauId, 'message.part.updated', {
      part: { ...base, type: 'tool', tool: b.name, callID: b.id, state: { status: 'running', title: toolTitle(b.name, args), input: args && typeof args === 'object' ? args : {}, output: '', metadata: {}, time: { start: Date.now() } } },
    })
  } else if (b.type === 'file') {
    emitFor(tauId, 'message.part.updated', { part: { ...base, type: 'file', url: b.url ?? '', mime: b.mime ?? '', filename: b.filename ?? '' } })
  }
}

// ---------------------------------------------------------------------------
// opencode session projection
// ---------------------------------------------------------------------------

function ocSession(s, publicId, withFile = true) {
  if (!s) return null
  const e = live.get(s.id)
  const title = e?.title || s.sessionName || null
  const id = publicId ?? canonicalId(s)
  const active = Date.parse(s.lastActiveAt) || 0
  // tau's `lastActiveAt` is NOT "the session changed": it moves when a child attaches
  // (opening a tab resumes one) and whenever that child emits any event, so an idle
  // tab pins its row at browse time forever — measured live: two attached-but-idle
  // children reported 13:05:03 while their JSONLs last changed at 12:24 and 05:17.
  // A session row means "when did this transcript last change", so the file's own
  // clock wins; tau's clock is only the fallback for a child with no known file (never
  // prompted yet), and a busy session's file lags by at most one message.
  let meta = null
  if (withFile) {
    // No `fileForId(id)` fallback here: it walks (and re-primes) the 1500 ms
    // listFilesCached() TTL, and doing that from a projection makes the cold-resume
    // path see a file list that predates a freshly seeded transcript — compact-check
    // then 404s `POST /summarize` on a cold session. `sessionFile` is set for every
    // child tau knows about, and `reapedFile` covers the closed ones.
    const file = s.sessionFile || reapedFile.get(id) || null
    if (file) {
      try {
        meta = sessionMeta(file)
      } catch {
        /* unreadable: fall back to tau's clock */
      }
    }
  }
  const updated = meta?.updated || active || Date.now()
  return {
    // canonical (uuid) unless the caller asked for a specific alias
    id,
    title: title || undefined,
    cwd: s.cwd || undefined,
    time: { created: Date.parse(s.createdAt) || Date.now(), updated },
    // cumulative generated tokens (output+reasoning over every assistant message), same
    // definition as the history projection, so a live tab's overlay cannot stay frozen at
    // an older partial sum. Omitted for an all-zero tally, exactly like the history rows.
    ...(meta?.tokens_generated ? { tokens_generated: meta.tokens_generated } : {}),
    cost: 0,
    agent: undefined,
    model: s.model ? { providerID: s.model.provider || '', id: s.model.id || '' } : undefined,
    revert: live.get(s.id)?.revert ?? null,
  }
}

// ---------------------------------------------------------------------------
// cold (JSONL) history
// ---------------------------------------------------------------------------

function readJsonl(file) {
  const out = []
  let raw
  try {
    raw = fs.readFileSync(file, 'utf8')
  } catch {
    return out
  }
  for (const line of raw.split('\n')) {
    if (!line.trim()) continue
    try {
      out.push(JSON.parse(line))
    } catch {
      /* torn trailing line mid-write */
    }
  }
  return out
}

function deriveTitle(entries) {
  // last name wins, same reason as readSessionMeta: renames append rather than edit
  let named = ''
  for (const e of entries) {
    if (e.type === 'session_info' && e.name) named = e.name
  }
  if (named) return named
  for (const e of entries) {
    if (e.type === 'message' && e.message?.role === 'user') {
      const t = textOf(e.message.content).trim()
      if (t.length > 8) return t.split('\n')[0].slice(0, 60)
    }
  }
  return ''
}

function listFiles() {
  const out = []
  let dirs
  try {
    dirs = fs.readdirSync(SESSIONS_DIR, { withFileTypes: true })
  } catch {
    return out
  }
  for (const d of dirs) {
    if (!d.isDirectory()) continue
    const p = path.join(SESSIONS_DIR, d.name)
    if (TRASHED_DIRS.has(path.resolve(p))) continue // soft-deleted, never listed
    let files
    try {
      files = fs.readdirSync(p)
    } catch {
      continue
    }
    for (const f of files) if (f.endsWith('.jsonl')) out.push(path.join(p, f))
  }
  return out
}

/**
 * Move a session JSONL into the out-of-tree trash. null on success, an error
 * string otherwise. Rename (not unlink) keeps deletes recoverable; pi appends
 * per write, so the owning child must already be closed — see the DELETE route.
 */
/**
 * Append the session_info entry pi would have written for a rename, and drop
 * that file's cached projection so the sidebar re-reads it. For the cases with
 * no live child to ask (or one that would not answer). The path must stay put:
 * pi holds it open via --session.
 */
function appendSessionInfo(file, name) {
  if (!file || !name) return false
  // NEVER create a transcript here: pi writes its own header with `wx` at the
  // first conversation entry, so a file we made first would make pi throw.
  if (!fs.existsSync(file)) return false
  try {
    fs.appendFileSync(
      file,
      JSON.stringify({ type: 'session_info', name, timestamp: new Date().toISOString() }) + '\n',
    )
    metaCache.delete(file)
    return true
  } catch {
    return false // read-only tree
  }
}

function trashSessionFile(file) {
  // A session created and then deleted never had a conversation, so pi never
  // wrote the path it told us about: nothing to move, child already closed.
  if (!fs.existsSync(file)) return null
  try {
    fs.mkdirSync(TRASH_DIR, { recursive: true })
    const ext = path.extname(file)
    const base = path.basename(file, ext)
    let dest = path.join(TRASH_DIR, path.basename(file))
    for (let n = 1; fs.existsSync(dest); n++) dest = path.join(TRASH_DIR, `${base}_${n}${ext}`)
    fs.renameSync(file, dest)
    // drop the workspace dir once empty (pi recreates it on the next session)
    const dir = path.dirname(file)
    if (path.resolve(dir) !== path.resolve(SESSIONS_DIR) && fs.readdirSync(dir).length === 0) {
      fs.rmdirSync(dir)
    }
    return null
  } catch (err) {
    return String(err?.message ?? err)
  }
}

/** file path -> tau id when a live child owns it (so sidebar rows share tabs) */
function liveTauIdFor(file) {
  for (const s of liveManager.sessions.values()) {
    if (s.sessionFile && path.resolve(s.sessionFile) === path.resolve(file)) return s.id
  }
  return null
}

let filesCache = { at: 0, list: null }
function listFilesCached() {
  const now = Date.now()
  if (filesCache.list && now - filesCache.at < 1500) return filesCache.list
  filesCache = { at: now, list: listFiles() }
  return filesCache.list
}

/**
 * Sidebar/history metadata for one session file.
 *
 * Cached per (mtime, size): the webui polls /api/history/sessions every 60 s and
 * `fileForId` uses it too, and each uncached pass re-parsed EVERY session JSONL
 * (61 ms for 27 files / 12 MB on this box, on the single-threaded bridge that is
 * also streaming everyone's SSE — and it grows linearly with history).
 * `readSessionMeta` is the file-derived part; liveness/alias identity is applied
 * per call because it depends on the live children, not the file.
 */
const metaCache = new Map() // file -> { key, meta }

function readSessionMeta(file, st) {
  const entries = readJsonl(file)
  const header = entries.find((e) => e.type === 'session')
  if (!header?.id) return null
  const msgs = messagesOf(entries)
  const users = msgs.filter((m) => m.message.role === 'user').length
  if (users === 0 && entries.length <= 8) return null
  // The CURRENT name, not the first one: a rename APPENDS a session_info entry (pi
  // does the same, and PATCH title appends here), so `find` returned the title the
  // session had when it was created and a rename never reached the sidebar. Measured
  // on a real transcript with three appended names: history said "llama server backup
  // plan" while the session had been renamed to "pi-web ssh".
  let info = null
  for (const e of entries) if (e.type === 'session_info' && e.name) info = e
  const modelChange = [...entries].reverse().find((e) => e.type === 'model_change')
  let tokens
  for (let i = entries.length - 1; i >= 0; i--) {
    const e = entries[i]
    if (e.type === 'usage' && e.usage) {
      tokens = (e.usage.input ?? 0) + (e.usage.output ?? 0) + (e.usage.cacheRead ?? 0)
      break
    }
    if (e.type === 'message' && e.message.role === 'assistant' && e.message.usage) {
      const u = e.message.usage
      tokens = (u.input ?? 0) + (u.output ?? 0) + (u.cacheRead ?? 0)
      break
    }
  }
  // Generated tokens — what the model produced: output + reasoning summed over
  // EVERY assistant message (the `tokens` above is a one-record context
  // estimate, which is what the sidebar used to show). pi stores usage on the
  // assistant message itself — 85/85 assistant messages in a real transcript,
  // and no separate `usage` entries — so summing message.usage cannot double
  // count. Same definition as chatserver's message-level sum for opencode, and
  // the same SELF-ONLY scope: pi-web sessions have no parent link, so the
  // webui's descendant rollup is a no-op here.
  let generated = 0
  for (const m of msgs) {
    const u = m.message?.role === 'assistant' ? m.message.usage : null
    if (!u) continue
    generated += (u.output ?? u.completionTokens ?? 0) + (u.reasoning ?? 0)
  }
  return {
    file,
    cwd: liveCwd(header.cwd) || PI_CWD,
    title: info?.name || deriveTitle(entries) || header.id.slice(0, 12),
    created: Date.parse(header.timestamp) || st.mtimeMs,
    updated: st.mtimeMs,
    message_count: msgs.length,
    cost: 0,
    model: modelChange?.modelId ?? '',
    model_provider: modelChange?.provider ?? '',
    ...(tokens ? { tokens } : {}),
    ...(generated ? { tokens_generated: generated } : {}),
    // an explicit name (any session_info entry) vs the prompt-derived fallback:
    // the bridge must never rename a session pi or the operator already named
    named: !!info,
    uuid: header.id,
  }
}

function sessionMeta(file) {
  let st
  try {
    st = fs.statSync(file)
  } catch {
    return null
  }
  const key = `${st.mtimeMs}|${st.size}`
  let hit = metaCache.get(file)
  if (!hit || hit.key !== key) {
    hit = { key, meta: readSessionMeta(file, st) }
    metaCache.set(file, hit)
  }
  const base = hit.meta
  if (!base) return null
  // Always pi's file uuid — the same string `ocSession()` publishes for the live
  // child, so the sidebar row and the tab can never diverge into two ids (and
  // therefore two tabs) for one session.
  return { ...base, id: base.uuid, live: !!liveTauIdFor(file) }
}

function allSessionMeta() {
  const files = listFilesCached()
  const rows = []
  for (const f of files) {
    try {
      const m = sessionMeta(f)
      if (m) rows.push(m)
    } catch {
      /* unreadable file */
    }
  }
  rows.sort((a, b) => b.updated - a.updated)
  // drop cache entries for files that vanished (deleted/trashed) so the map
  // can't grow across a long-lived server
  if (metaCache.size > files.length) {
    const keep = new Set(files)
    for (const f of metaCache.keys()) if (!keep.has(f)) metaCache.delete(f)
  }
  return rows
}

/**
 * Project chips for the new-session picker: every workspace a session has
 * actually run in (most recent first), then the child dirs of PI_CWD that
 * have never been used — tau's `--projects-dir` chips, minus the config.
 */
function workspaceChips() {
  const seen = new Map() // path -> last activity (0 = never used)
  for (const m of allSessionMeta()) {
    if (m.cwd && !seen.has(m.cwd)) seen.set(m.cwd, m.updated)
  }
  let entries = []
  try {
    entries = fs.readdirSync(PI_CWD, { withFileTypes: true })
  } catch {
    /* root unreadable — the session workspaces above still list */
  }
  for (const e of entries) {
    if (!e.isDirectory() || e.name.startsWith('.')) continue
    const p = path.join(PI_CWD, e.name)
    if (!seen.has(p)) seen.set(p, 0)
  }
  return [...seen]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])) // activity, then A→Z
    .slice(0, 24)
    .map(([p]) => ({ name: path.basename(p) || p, path: p }))
}

// --- star / tag sidecar (same idea as nopencode's webui.db smeta) -----------
const META_FILE = path.join(HERE, '.meta.json')
function loadMeta() {
  try {
    return JSON.parse(fs.readFileSync(META_FILE, 'utf8'))
  } catch {
    return {}
  }
}
function saveMeta(m) {
  try {
    fs.writeFileSync(META_FILE, JSON.stringify(m, null, 2))
  } catch {
    /* read-only tree */
  }
}
const errTiles = new Map() // sid -> [{message,t}]

function toHistMsgs(entries) {
  return toOcMessages(entries).map((m) => ({
    id: m.id,
    role: m.role,
    time: m.time.created,
    modelID: m.modelID,
    providerID: m.providerID,
    error: m.error,
    parts: m.parts.map((p) => ({
      id: p.id,
      type: p.type,
      text: p.text,
      tool: p.tool,
      state_summary: p.state ? String(p.state.status ?? p.state.title ?? '') : undefined,
    })),
  }))
}

function fileForId(id) {
  const s = sessionByAnyId(id)
  // A fresh child knows the path pi WILL write, but no bytes exist yet: handing
  // that path out would have the bridge resume a transcript that is not there,
  // and pi would mint a SECOND uuid inside a file named after the first.
  if (s?.sessionFile && fs.existsSync(s.sessionFile)) return s.sessionFile
  if (reapedFile.has(id)) return reapedFile.get(id) // child reaped; file still there
  const byId = allSessionMeta().find((m) => m.id === id)
  if (byId) return byId.file
  const byTau = allSessionMeta().find((m) => m.file && path.basename(m.file).includes(id))
  return byTau?.file ?? null
}

function entriesForId(id) {
  const s = sessionByAnyId(id)
  if (s) return s.entries
  const file = fileForId(id)
  return file ? readJsonl(file) : null
}

/**
 * Any id this session can answer as: the live child if one knows the id,
 * else resume it from its JSONL (aliasing the public id so later SSE lands
 * on the same tab). null when the id names nothing.
 */
async function sessionFor(id) {
  const existing = sessionByAnyId(id)
  if (existing) return existing
  const file = fileForId(id)
  if (!file) return null
  // A live child may already own this transcript under another alias (its tau
  // id, or the uuid). Never run two children against one JSONL — attach to the
  // one that exists and remember the alias instead.
  const owned = liveManager.findBySessionFile(file)
  if (owned) {
    entryFor(owned.id).ids.add(id)
    byPublicId.set(id, owned.id)
    return owned
  }
  const entries = leafChain(readJsonl(file))
  const hdrCwd = readHeaderCwd(file)
  const runCwd = liveCwd(hdrCwd) || PI_CWD
  if (hdrCwd && hdrCwd !== runCwd) {
    // say it out loud: the workspace was renamed/moved, and the child will not be
    // where the transcript says it was (the sidebar must not claim a dead path)
    console.log(`[resume] ${path.basename(file)}: header cwd ${JSON.stringify(hdrCwd)} is gone -> running in ${runCwd}`)
  }
  const s = await liveManager.resume({
    sessionFile: file,
    cwd: runCwd,
    entries,
    sessionName: deriveTitle(entries) || null,
  })
  const e = entryFor(s.id)
  e.ids.add(id)
  e.file = s.sessionFile ?? file
  byPublicId.set(id, s.id)
  seedDup(entries, e)
  armSession(s)
  // a session opened from the sidebar that pi never named gets named now, so
  // the row, the tab and the TUI stop disagreeing about what it is called
  void autoName(s.id)
  return s
}

function searchSessions(q) {
  const terms = q.toLowerCase().split(/\s+/).filter((t) => t.length >= 2)
  if (!terms.length) return []
  const hits = []
  const meta = allSessionMeta()
  const byFile = new Map(meta.map((m) => [m.file, m]))
  for (const file of listFilesCached()) {
    if (hits.length >= 300) break
    const m = byFile.get(file)
    if (!m) continue
    for (const e of messagesOf(readJsonl(file))) {
      const text = textOf(e.message.content)
      const low = text.toLowerCase()
      if (!text || !terms.every((t) => low.includes(t))) continue
      const idx = low.indexOf(terms[0])
      const a = Math.max(0, idx - 70)
      const b = Math.min(text.length, idx + terms[0].length + 70)
      hits.push({
        session_id: m.id,
        session_title: m.title,
        message_id: 'q',
        part_id: 'q',
        role: e.message.role,
        time: isoMs(e.message.timestamp),
        snippet:
          (a > 0 ? '…' : '') + text.slice(a, b).replace(/\n/g, ' ') + (b < text.length ? '…' : ''),
      })
      if (hits.length >= 300) break
    }
  }
  hits.sort((x, y) => y.time - x.time)
  return hits
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.webmanifest': 'application/manifest+json',
  '.map': 'application/json',
  '.txt': 'text/plain; charset=utf-8',
}

function sendJson(res, obj, code = 200) {
  const body = JSON.stringify(obj)
  res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' })
  res.end(body)
}

function readBody(req) {
  return new Promise((resolve) => {
    let buf = ''
    req.on('data', (c) => (buf += c))
    req.on('end', () => {
      try {
        resolve(JSON.parse(buf || '{}'))
      } catch {
        resolve({})
      }
    })
    req.on('error', () => resolve({}))
  })
}

function serveStatic(req, res, pathname) {
  // The shared dist is built with vite `base: '/webui/'` (nopencode's chatserver
  // serves it under that prefix). pi-web serves the same folder at `/`, so accept
  // both spellings — the built index.html and every dynamic import/chunk inside
  // the JS ask for `/webui/assets/...`, and 404ing those blanks the UI.
  if (pathname === '/webui' || pathname.startsWith('/webui/')) {
    pathname = pathname.slice('/webui'.length) || '/'
  }
  let p = pathname === '/' ? '/index.html' : pathname
  if (!path.extname(p)) p = '/index.html'
  let full
  try {
    full = fs.realpathSync(path.join(WEBUI_DIST, p))
  } catch {
    res.writeHead(404).end('not found')
    return
  }
  if (!full.startsWith(WEBUI_DIST)) {
    res.writeHead(403).end('forbidden')
    return
  }
  if (!fs.existsSync(full)) {
    res.writeHead(404).end('not found')
    return
  }
  const noncePlaceholder = 'OPENCODE_CSP_NONCE'
  let body = fs.readFileSync(full)
  if (body.includes(noncePlaceholder)) {
    body = Buffer.from(body.toString('utf8').replace(/OPENCODE_CSP_NONCE/g, 'pi-web'))
  }
  res.writeHead(200, {
    'content-type': MIME[path.extname(full)] ?? 'application/octet-stream',
    'cache-control': p.startsWith('/assets/') ? 'public, max-age=31536000, immutable' : 'no-cache',
  })
  res.end(body)
}

// --- stubs for opencode-only surfaces --------------------------------------
const EMPTY = {
  '/oc/permission': [],
  '/oc/question': [],
  '/oc/agent': [],
  '/oc/skill': [],
  '/oc/mcp': {},
  '/oc/config': {},
}

// pi's own slash commands (the ones loaded extensions register, e.g.
// an extension's /model-sync) are only visible to pi, so the palette asked the
// engine and got [] forever. Query a live pi child over RPC (`get_commands`)
// instead; pi built-ins stay out because the webui ships web-side twins for
// those, and both exclusions need UI this server never bridges:
// tau's navigate command is internal, and ctx.ui.select/confirm dialogs (used
// by /pick-model) have no client here — the RPC dialog just expires.
const HIDDEN_COMMANDS = new Set(['tau-tree-navigate', 'pick-model'])
const commandCache = { at: 0, ttl: 0, data: [] }
async function engineCommands() {
  if (commandCache.data.length && Date.now() - commandCache.at < commandCache.ttl) return commandCache.data
  const found = new Map()
  await Promise.all(
    [...liveManager.sessions.values()].map(async (s) => {
      try {
        const resp = await s.send({ type: 'get_commands' }, { timeoutMs: 8000 })
        for (const c of resp?.data?.commands ?? []) {
          if (!c?.name || c.source !== 'extension' || HIDDEN_COMMANDS.has(c.name)) continue
          if (!found.has(c.name)) found.set(c.name, { name: c.name, description: c.description ?? '' })
        }
      } catch {
        /* child closing or busy */
      }
    }),
  )
  commandCache.at = Date.now()
  commandCache.data = [...found.values()]
  // empty (no live child yet) re-checks quickly; a real list is good for 5 min
  commandCache.ttl = commandCache.data.length ? 5 * 60_000 : 10_000
  return commandCache.data
}
const NOT_WIRED = new Set([])
// POST sub-routes that must resume a cold/reaped session instead of 404ing
// (they run inside a pi child). `/abort` deliberately does NOT: waking a child
// only to abort it would resurrect a tab nobody asked for.
const RESUME_ON_POST = new Set(['/prompt_async', '/command', '/queue/clear', '/summarize', '/revert', '/fork'])
// opencode surfaces that degrade cleanly to "nothing here"
const EMPTY_VCS = { '/oc/vcs/diff': [], '/oc/vcs/diff/raw': '' }

// ---------------------------------------------------------------------------
// Compaction (webui /compact -> opencode summarize -> pi RPC `compact`)
//
// The webui treats summarize as a BLOCKING POST (it flips busy + shows a
// "Compacting…" strip and only trusts session.idle), and pi answers the RPC
// only once the summary is written — one long prefill over the whole session.
// On a local llama.cpp slot that runs far past a reverse proxy's ~100 s edge
// timeout, where the failure would be a Cloudflare 524 that the webui reports
// as "/compact failed" even when pi compacted the session fine. So the HTTP
// request is answered on a ceiling: settle first -> real result; otherwise
// answer "started" and let SSE carry the outcome (see compactingWait + the
// compaction_end bridge).
//
// tauId -> number of summarize POSTs still waiting on the RPC. While one is
// open the HTTP response is the error channel and the bridge stays silent; a
// compaction_end always lands BEFORE the compact response line on pi's stdout,
// so this map is still populated when the bridge decides who reports.
// ---------------------------------------------------------------------------
const SUMMARIZE_WAIT_MS = Number(process.env.PI_WEB_SUMMARIZE_WAIT_MS || 40_000)
// tau's own send() default is 60 s, far too short for a summarisation call.
const COMPACT_RPC_MS = Number(process.env.PI_WEB_COMPACT_RPC_MS || 600_000)
const compactingWait = new Map()

// pi refuses a prompt OUTRIGHT while a compaction is running
// (core/agent-session.js:1620 — "Cannot submit a prompt while compaction is in
// progress. Wait for compaction to finish and retry."). Forwarding that as a 502
// throws away work: the user pressed send on a session that was only busy
// summarising, and the webui's composer treats a 502 as a send failure. pi-web
// knows when the summary lands (compaction_end), so it holds the send, then
// sends. Bounded: past the window the answer is 409 SessionBusyError — pi never
// accepted the text, so the client can retry without risking a duplicate.
const COMPACT_SEND_WAIT_MS = Number(process.env.PI_WEB_COMPACT_SEND_WAIT_MS || 90_000)
const compactionSettled = new Map() // tauId -> waiters resolved by compaction_end

function settleCompaction(tauId) {
  const set = compactionSettled.get(tauId)
  if (!set?.size) return
  compactionSettled.delete(tauId)
  for (const done of set) done()
}

function waitForCompactionEnd(tauId, ms) {
  return new Promise((res) => {
    const set = compactionSettled.get(tauId) ?? new Set()
    let timer = null
    const done = () => {
      if (timer) clearTimeout(timer)
      set.delete(done)
      res(true)
    }
    set.add(done)
    compactionSettled.set(tauId, set)
    timer = setTimeout(() => {
      set.delete(done)
      if (!set.size) compactionSettled.delete(tauId)
      res(false)
    }, ms)
  })
}

// ---------------------------------------------------------------------------
// Model catalog (feeds GET /oc/config/providers → ComposerModelPicker,
// ModelSelect, Footer/InfoPanel context sizes).
//
// tau's getAvailableModels() shells out to `pi --list-models`, which only knows
// whatever pi last restored into its own store snapshot AND pays for a pi child
// process (30 s timeout) on every refresh. pi-web builds the list natively
// instead, in three tiers:
//   1. `<agentDir>/models-store.json` — the very file pi restores from, parsed
//      directly (mtime-cached, no subprocess). Entries carrying a `type` (the
//      vision bridge) are not chat models and stay out.
//   2. the llama.cpp router's `GET <base>/v1/models`, merged on top so a box
//      nobody re-synced still lists every model. Embedding-only entries (no
//      `text` output modality) are dropped. The same response reports which
//      model is resident, so the loaded one sorts FIRST: webui's
//      preferredDefaultModel takes providers[0]'s first model for a brand-new
//      chat, and an alphabetical list would boot it onto an unloaded GGUF —
//      which the router then loads, evicting the resident one
//      (max_instances: 1).
//   3. tau's CLI listing, only when 1+2 produce nothing (a cloud-only setup,
//      whose providers never appear in a local store).
// Listing never loads anything: selecting an unloaded model makes the router
// load it on that model's next request. A model the router knows but pi does
// not (nothing wrote it into the store) will be rejected by pi's `set_model` —
// pi-web swallows that error and the session keeps its old model.
// ---------------------------------------------------------------------------

const ROUTER_TTL_MS = 60_000
const STATE_RANK = { loaded: 0, sleeping: 1, unloaded: 2 }
// llama.cpp reports an embedding GGUF with output_modalities ["text"] and no
// other tell (no --embeddings in its preset args either), so the name is the
// only signal available. A false negative here costs a model nobody would pick.
const EMBEDDING_RE = /embed|rerank/i
const storeCache = { mtime: 0, providers: [] }
const modelsJsonCache = { mtime: 0, overrides: {} }
let routerCache = { at: 0, base: '', models: [] }

function readJson(p) {
  try {
    return JSON.parse(fs.readFileSync(p, 'utf8'))
  } catch {
    return null
  }
}

/** Providers parsed out of pi's models-store.json; [] when there is no store. */
// pi's thinking levels (core/defaults.js). `xhigh`/`max` exist for a model ONLY
// when it maps them (pi-ai getSupportedThinkingLevels), and a level mapped to
// null is explicitly unsupported — the picker must not offer either.
const THINKING_LEVELS = ['off', 'minimal', 'low', 'medium', 'high']
const THINKING_LEVELS_EXPLICIT = ['xhigh', 'max']

/**
 * The webui's model picker shows per-model "variants" (`extractVariants` reads
 * `models[mid].variants`, keys = variant ids, `{disabled:true}` hides one) and
 * sends the pick back as `model.variant`. For pi that IS the thinking level —
 * without this the picker offers no thinking control for pi models AND a pick
 * made on the opencode side is silently dropped here.
 */
function thinkingVariants(m) {
  if (!m.reasoning) return undefined
  const map = m.thinkingLevelMap ?? {}
  const out = {}
  for (const lvl of [...THINKING_LEVELS, ...THINKING_LEVELS_EXPLICIT]) {
    if (map[lvl] === null) continue
    if (THINKING_LEVELS_EXPLICIT.includes(lvl) && map[lvl] === undefined) continue
    out[lvl] = {}
  }
  return Object.keys(out).length > 1 ? out : undefined
}

/**
 * `modelOverrides` from `<agentDir>/models.json` — the layer pi composes LAST
 * (core/provider-composer.js `applyModelOverride`: reasoning, thinkingLevelMap,
 * compat are all overridable). It matters because `models-store.json` is a CACHE
 * pi's providers rewrite: the llama provider re-probes /props and rewrites its
 * section, hand-edits included (observed 2026-10-30: a hand-patched store went
 * back to `{off, medium}` + `supportsReasoningEffort:false` on the restart after
 * a rebuild). So the catalogue this bridge publishes for the picker has to apply
 * the same override the child will actually run under, or the webui advertises a
 * thinking ladder pi ignores (or hides one it honors).
 */
function modelsJsonOverrides(agentDir) {
  const file = path.join(agentDir, 'models.json')
  let st
  try {
    st = fs.statSync(file)
  } catch {
    return {}
  }
  if (modelsJsonCache.mtime === st.mtimeMs) return modelsJsonCache.overrides
  const raw = readJson(file) ?? {}
  const out = {}
  for (const [pid, entry] of Object.entries(raw?.providers ?? {})) {
    if (entry?.modelOverrides) out[pid] = entry.modelOverrides
  }
  modelsJsonCache.mtime = st.mtimeMs
  modelsJsonCache.overrides = out
  return out
}

function storeProviders() {
  const file = path.join(PI_AGENT_DIR, 'models-store.json')
  let st
  try {
    st = fs.statSync(file)
  } catch {
    return []
  }
  if (storeCache.mtime === st.mtimeMs) return storeCache.providers
  const raw = readJson(file) ?? {}
  const overrides = modelsJsonOverrides(PI_AGENT_DIR)
  const providers = []
  for (const [pid, entry] of Object.entries(raw)) {
    const models = {}
    let routerBase = ''
    for (const m of entry?.models ?? []) {
      if (!m?.id || m.type) continue // typed sections (vision) are not chat models
      if (!routerBase && m.baseUrl) routerBase = String(m.baseUrl).replace(/\/v1\/?$/, '')
      const ov = overrides[pid]?.[m.id] ?? {}
      // pi merges a map, it does not replace it (applyModelOverride).
      const eff = {
        ...m,
        ...(ov.reasoning === undefined ? {} : { reasoning: ov.reasoning }),
        ...(ov.thinkingLevelMap || m.thinkingLevelMap
          ? { thinkingLevelMap: { ...(m.thinkingLevelMap ?? {}), ...(ov.thinkingLevelMap ?? {}) } }
          : {}),
      }
      models[m.id] = {
        id: m.id,
        name: m.name ?? m.id,
        limit: { context: m.contextWindow ?? 0, output: m.maxTokens ?? 0 },
        ...(eff.reasoning === undefined ? {} : { reasoning: !!eff.reasoning }),
        ...(thinkingVariants(eff) ? { variants: thinkingVariants(eff) } : {}),
      }
    }
    if (Object.keys(models).length) providers.push({ id: pid, name: pid, models, routerBase })
  }
  storeCache.mtime = st.mtimeMs
  storeCache.providers = providers
  return providers
}

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

/** A store-less llama.cpp setup still names its router in auth.json. */
function authRouterBase() {
  const raw = readJson(path.join(PI_AGENT_DIR, 'auth.json')) ?? {}
  for (const [pid, cred] of Object.entries(raw)) {
    const url = cred?.env?.LLAMA_BASE_URL
    if (url) return { id: pid, base: String(url).replace(/\/v1\/?$/, '') }
  }
  return null
}

/** llama.cpp router catalogue: {id, context, state}; [] (uncached) on failure. */
async function routerModels(base) {
  if (routerCache.base === base && routerCache.models.length && Date.now() - routerCache.at < ROUTER_TTL_MS)
    return routerCache.models
  const out = []
  try {
    const res = await fetch(`${base}/v1/models`, { signal: AbortSignal.timeout(5000) })
    const data = await res.json()
    for (const m of data?.data ?? []) {
      if (!m?.id || EMBEDDING_RE.test(m.id)) continue
      const mods = m.architecture?.output_modalities
      if (Array.isArray(mods) && !mods.includes('text')) continue // non-text output
      out.push({ id: m.id, context: m.context_length ?? 0, state: m.status?.value ?? 'unknown' })
    }
  } catch (err) {
    console.warn(`[pi-web] router model list failed (${base}: ${String(err?.message ?? err)})`)
    return out
  }
  routerCache = { at: Date.now(), base, models: out }
  return out
}

function sortLoadedFirst(p) {
  const models = Object.values(p.models)
  models.sort(
    (a, b) => (STATE_RANK[a.state] ?? 3) - (STATE_RANK[b.state] ?? 3) || String(a.name).localeCompare(String(b.name)),
  )
  p.models = Object.fromEntries(models.map((m) => [m.id, m]))
}

/** tau's `pi --list-models` mapped into the webui's provider shape. */
async function cliProviders() {
  // pi prints context as "1.0M" / "131.1K" — InfoPanel expects a number.
  const toNum = (s) => {
    const m = String(s ?? '').match(/^([\d.]+)\s*([KM])?$/i)
    if (!m) return 0
    return Math.round(Number(m[1]) * (m[2]?.toUpperCase() === 'M' ? 1e6 : m[2] ? 1e3 : 1))
  }
  const byProv = new Map()
  for (const m of await getAvailableModels().catch(() => [])) {
    const p = m.provider || 'unknown'
    if (!byProv.has(p)) byProv.set(p, { id: p, name: p, models: {} })
    byProv.get(p).models[m.id] = { id: m.id, name: m.id, limit: { context: toNum(m.context) } }
  }
  return [...byProv.values()]
}

async function modelProviders() {
  const providers = storeProviders().map((p) => ({ ...p, models: { ...p.models } }))
  if (!providers.length) {
    const auth = authRouterBase()
    if (auth) providers.push({ id: auth.id, name: auth.id, models: {}, routerBase: auth.base })
  }
  const router = providers.find((p) => p.routerBase)
  if (router) {
    for (const m of await routerModels(router.routerBase)) {
      const existing = router.models[m.id]
      if (existing) {
        existing.state = m.state
        if (!existing.limit.context && m.context) existing.limit.context = m.context
      } else {
        router.models[m.id] = { id: m.id, name: m.id, limit: { context: m.context || 0, output: 0 }, state: m.state }
      }
    }
  }
  if (!Object.values(providers).some((p) => Object.keys(p.models).length)) return cliProviders()
  for (const p of providers) {
    delete p.routerBase
    if (Object.keys(p.models).length) sortLoadedFirst(p)
  }
  return providers.filter((p) => Object.keys(p.models).length)
}

async function handleApi(req, res, url) {
  const { pathname } = url
  const q = url.searchParams

  // ---- static-ish stubs ---------------------------------------------------
  if (pathname in EMPTY) return sendJson(res, EMPTY[pathname])
  if (pathname === '/oc/command') return sendJson(res, await engineCommands())
  if (pathname === '/oc/path') return sendJson(res, { directory: PI_CWD })
  // workspace picker: chip list + tau-style directory type-ahead
  if (pathname === '/oc/workspaces') {
    return sendJson(res, { root: PI_CWD, chips: workspaceChips() })
  }
  if (pathname === '/oc/fs/dirs') {
    return sendJson(res, { items: completeDirectories(q.get('prefix') || '', PI_CWD) })
  }
  if (pathname === '/oc/config/providers') return sendJson(res, { providers: await modelProviders() })
  if (pathname.startsWith('/oc/file/content')) {
    const p = q.get('path') || ''
    try {
      const buf = fs.readFileSync(p)
      const isText = !buf.includes(0)
      return sendJson(
        res,
        isText
          ? { type: 'text', content: buf.toString('utf8') }
          : { type: 'binary', content: buf.toString('base64') },
      )
    } catch {
      return sendJson(res, { type: 'text', content: '' })
    }
  }

  // ---- SSE ---------------------------------------------------------------
  if (pathname === '/oc/event') {
    req.socket.setTimeout(0)
    req.socket.setNoDelay(true)
    req.socket.setKeepAlive(true)
    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    })
    res.write(': ok\n\n')
    sseClients.add(res)
    req.on('close', () => sseClients.delete(res))
    return
  }

  // ---- session status -----------------------------------------------------
  if (pathname === '/oc/session/status') {
    // Authoritative for the webui: its busy-reconcile watchdog overwrites the
    // local busy/compacting flags with what it reads HERE (sse.ts catchUp), so
    // a summary in flight must say busy or the "Compacting session…" strip dies
    // mid-summary. See entryFor().compacting.
    const out = {}
    for (const s of liveManager.sessions.values()) {
      const busy = !!s.isStreaming || live.get(s.id)?.compacting === true
      const ids = publicIdsFor(s.id)
      const rec = live.get(s.id)?.compaction ?? null
      const eng = live.get(s.id)?.engine ?? null
      for (const id of ids) {
        out[id] = {
          type: busy ? 'busy' : 'idle',
          ...(rec ? { compaction: rec } : {}),
          ...(eng ? { engine: eng } : {}),
        }
      }
    }
    return sendJson(res, out)
  }
  if (pathname === '/oc/session/todo') return sendJson(res, [])
  if (pathname in EMPTY_VCS) {
    const v = EMPTY_VCS[pathname]
    if (typeof v === 'string') {
      res.writeHead(200, { 'content-type': 'text/plain' })
      res.end(v)
      return
    }
    return sendJson(res, v)
  }
  if (NOT_WIRED.has(pathname)) return sendJson(res, { error: 'not wired in pi-web (mockup)' }, 501)

  // engine-model / agent switch (webui posts to /oc/api/session/{id}/…)
  const am = pathname.match(/^\/oc\/api\/session\/([^/]+)\/(model|agent)$/)
  if (am) {
    const body = await readBody(req)
    const s = sessionByAnyId(decodeURIComponent(am[1]))
    const ref = modelRefOf(body)
    if (s && am[2] === 'model' && ref.modelId) {
      await s
        .send({ type: 'set_model', provider: ref.provider, modelId: ref.modelId }, { timeoutMs: 10000 })
        .catch(() => {})
      const ent = entryFor(s.id)
      ent.modelRef = `${ref.provider}\u0000${ref.modelId}`
      ent.level = null // the model switch re-picks a level; the pick goes below
      // This POST is the user's own switch, and webui clearVariant() posts the
      // model with NO variant — so an absent variant here means "cleared" and
      // pi's freshly re-picked level is the answer. Do not re-assert the old one.
      if (ref.variant) {
        const r = await s.send({ type: 'set_thinking_level', level: ref.variant }, { timeoutMs: 5000 }).catch(() => null)
        if (r?.success === false)
          console.log(`[thinking] ${s.id}: pi rejected level "${ref.variant}" (${r.error ?? 'unknown'}) — using the session's level`)
        else ent.level = ref.variant
      }
      emitSessionUpdated(s.id, s, undefined, true)
    }
    return sendJson(res, ocSession(s) ?? { ok: true })
  }

  // dialogs / mcp / config toggles: no-op success so the UI never sticks
  if (/^\/oc\/(permission|question)\/[^/]+\/(reply|reject)$/.test(pathname)) {
    await readBody(req)
    return sendJson(res, { ok: true })
  }
  if (/^\/oc\/mcp\/[^/]+\/(connect|disconnect)$/.test(pathname)) return sendJson(res, { ok: true })
  if (pathname === '/oc/config' && req.method === 'PATCH') return sendJson(res, await readBody(req))

  // ---- session list -------------------------------------------------------
  if (pathname === '/oc/session' && req.method === 'GET') {
    return sendJson(
      res,
      [...liveManager.sessions.values()].map((s) => ocSession(s)),
    )
  }
  if (pathname === '/oc/session' && req.method === 'POST') {
    const body = await readBody(req)
    const cwd = expandHome(typeof body?.cwd === 'string' ? body.cwd.trim() : '') || PI_CWD
    let st = null
    try {
      st = fs.statSync(cwd)
    } catch {
      /* missing */
    }
    if (!st?.isDirectory()) return sendJson(res, { error: `not a directory: ${cwd}` }, 400)
    const s = await liveManager.create({ cwd, model: modelSpec(body?.model) })
    const ent = entryFor(s.id)
    const ref = modelRefOf(body)
    if (ref.modelId) ent.modelRef = `${ref.provider}\u0000${ref.modelId}`
    // tau picked the model at spawn, and pi re-picked a thinking level with it,
    // so a variant that came with the create has to go AFTER it.
    if (ref.variant) {
      const r = await s.send({ type: 'set_thinking_level', level: ref.variant }, { timeoutMs: 5000 }).catch(() => null)
      if (r?.success === false) console.log(`[thinking] create: pi rejected level "${ref.variant}" (${r.error ?? 'unknown'})`)
      else ent.level = ref.variant
    }
    armSession(s)
    // answer with pi's id, not the per-child handle: see learnPublicId
    await learnPublicId(s)
    filesCache.list = null
    return sendJson(res, ocSession(s), 201)
  }

  // ---- one session --------------------------------------------------------
  const m = pathname.match(/^\/oc\/session\/([^/]+)(\/.*)?$/)
  if (m) {
    const raw = decodeURIComponent(m[1])
    const rest = m[2] ?? ''

    if (rest === '') {
      if (req.method === 'DELETE') {
        const s = sessionByAnyId(raw)
        // Resolve the JSONL BEFORE closing the child. Live sidebar rows carry a
        // tau id, which appears nowhere in the file name, so s.sessionFile is
        // the only link between them — and that link dies with the child.
        const file = fileForId(raw)
        const ids = s ? [...publicIdsFor(s.id), raw] : [raw]
        // Close first (best effort): pi appends to its JSONL per write, so a
        // child still running could re-create the path we are about to move.
        if (s) {
          try {
            await liveManager.delete(s.id, 'closed_by_user')
          } catch (err) {
            console.error('[delete] close failed:', err?.message ?? err)
          }
        }
        filesCache = { at: 0, list: null }
        // a trashed session must not stay resumable through the reap map
        if (file) metaCache.delete(file)
        for (const id of ids) reapedFile.delete(id)
        if (!s && !file) return sendJson(res, { error: 'unknown session' }, 404)
        if (file) {
          const err = trashSessionFile(file)
          if (err) return sendJson(res, { error: `failed to trash: ${err}` }, 500)
        }
        // star/tag sidecar + error tiles must not outlive the session
        const all = loadMeta()
        for (const id of ids) {
          delete all[id]
          errTiles.delete(id)
        }
        saveMeta(all)
        return sendJson(res, { ok: true, trashed: !!file })
      }
      if (req.method === 'PATCH') {
        const body = await readBody(req)
        const name = typeof body.title === 'string' ? body.title.trim() : ''
        const s = sessionByAnyId(raw)
        if (name && s) {
          const e = entryFor(s.id)
          e.title = name
          e.named = true // the auto-tiler must not undo a rename the operator made
          // Ask pi to rename its own session: it persists the session_info entry
          // AND emits session_info_changed, so pi, the TUI and every other
          // client learn the name. Appending to the file behind the child did
          // neither — the child kept its old name, and could rewrite it away.
          let renamed = false
          try {
            const r = await s.send({ type: 'set_session_name', name }, { timeoutMs: 8000 })
            renamed = r?.success !== false
          } catch {
            /* child not answering */
          }
          if (!renamed) appendSessionInfo(s.sessionFile || fileForId(raw), name)
          emitSessionUpdated(s.id, s, raw, true)
        } else if (name) {
          // No live child: the file is the only thing to write. Doing nothing
          // here meant renaming a closed session answered ok and then silently
          // reverted to the prompt on the next sidebar poll.
          const file = fileForId(raw)
          if (!file) return sendJson(res, { error: 'unknown session' }, 404)
          if (!appendSessionInfo(file, name)) return sendJson(res, apiErr(`failed to write ${file}`), 500)
          filesCache.list = null
        }
        return sendJson(res, { ok: true })
      }
      // any other method (DELETE/PATCH above) handled — GETs fall through so
      // the shared path below resumes cold sessions for every sub-route
    }

    // GET /oc/session/{id}[/sub]: resume a cold session from the JSONL first.
    // openLive fires /message and the info fetch in the same Promise.all, so
    // waiting for the info fetch to do the resume made the boot path 404 its
    // own message/todo reads (sidebar rows only ever GET, never write).
    // /summarize resumes too: compaction has to run inside a pi child, so a
    // session the reaper closed (or a sidebar row never opened) still has to
    // answer /compact instead of 404ing.
    //
    // Same reasoning for the other POSTs that need a child: a tab whose child
    // the reaper closed must be able to send a prompt without the browser first
    // GETing the session. Resume is the existing cold path, and `raw` stays the
    // address events come back on.
    const s =
      req.method === 'GET' || RESUME_ON_POST.has(rest)
        ? await sessionFor(raw)
        : sessionByAnyId(raw)
    if (!s) return sendJson(res, { error: 'unknown session' }, 404)
    if (rest === '' && req.method === 'GET') return sendJson(res, ocSession(s, raw))

    if (rest === '/message') {
      const entries = s ? transcriptEntries(s, live.get(s.id)) : null
      let msgs = entries ? toOcMessages(entries) : []
      const limit = Number(q.get('limit') || 0)
      if (limit > 0 && msgs.length > limit) msgs = msgs.slice(-limit)
      return sendJson(res, msgs)
    }
    // pi's pending queues. Doubles as the capability probe: a backend that
    // answers these two arrays supports the strip, one that 404s keeps the
    // old "queued shows up as a transcript row" behaviour.
    if (rest === '/queue' && req.method === 'GET') return sendJson(res, queueView(s.id))
    if (rest === '/queue/clear' && req.method === 'POST') {
      // pi has no targeted dequeue — clearing is all-or-nothing by design.
      try {
        const r = await s.send({ type: 'clear_queue' }, { timeoutMs: 10000 })
        if (r?.success === false) return sendJson(res, apiErr(r.error ?? 'clear_queue rejected'), 502)
        // queue_update lands on its own; answer with what was dropped.
        return sendJson(res, { ok: true, ...(r?.data ?? {}) })
      } catch (err) {
        return sendJson(res, apiErr(String(err?.message ?? err)), 504)
      }
    }
    if (rest === '/prompt_async' && req.method === 'POST') {
      const body = await readBody(req)
      const parts = Array.isArray(body.parts) ? body.parts : []
      const text = parts
        .filter((p) => p?.type === 'text')
        .map((p) => p.text ?? '')
        .join('\n')
        .trim()
      const images = parts.map(fileToImage).filter(Boolean)
      const refused = parts.filter((p) => p?.type === 'file' && !fileToImage(p))
      // "steer" lands at the next turn boundary, "follow" after the run settles.
      // The client states intent; pi picks the delivery, and only pi knows
      // whether a run is live — a cached busy flag never does.
      const delivery = body.delivery === 'steer' ? 'steer' : 'follow'
      if (refused.length)
        return sendJson(
          res,
          apiErr(`pi-web carries image attachments only; refused: ${refused.map((p) => p.filename ?? 'file').join(', ')}`),
          415,
        )
      if (!text && !images.length) return sendJson(res, apiErr('empty prompt'), 400)
      const streaming = !!s.isStreaming
      const ent = entryFor(s.id) // per-bridge session state (model ref + thinking level live here)
      // The webui re-posts its model pick on EVERY prompt (Composer.svelte), so
      // apply it only when the ref actually differs from what THIS child last
      // got: pi re-picks a thinking level on every set_model, so a per-prompt
      // set_model would undo the variant pick and cost a round trip for nothing.
      const ref = modelRefOf(body)
      const want = ref.modelId ? `${ref.provider}\u0000${ref.modelId}` : ''
      const prevLevel = ent.level
      let switched = false
      // A steer never changes the model: the turn it lands inside is already
      // running on whatever model produced it.
      if (want && want !== ent.modelRef && !(streaming && delivery === 'steer')) {
        await s
          .send({ type: 'set_model', provider: ref.provider, modelId: ref.modelId }, { timeoutMs: 10000 })
          .catch(() => {})
        ent.modelRef = want
        ent.level = null
        switched = true
      }
      // webui variant -> pi thinking level. Only when it changed: this rides on
      // every prompt, and an RPC round trip per prompt is pure latency. A body
      // with NO variant is not a clear (webui clearVariant posts the model with
      // no variant to /api/session/{id}/model), so when the bridge itself just
      // switched the model it has to put the session's level back afterwards.
      const level = ref.variant ?? (switched ? prevLevel : null)
      if (level && level !== ent.level) {
        const r = await s.send({ type: 'set_thinking_level', level }, { timeoutMs: 5000 }).catch(() => null)
        if (r?.success === false) {
          console.log(`[thinking] ${s.id}: pi rejected level "${level}" (${r.error ?? 'unknown'}) — using the session's level`)
        } else {
          ent.level = level
        }
      }
      const cmd = { type: 'prompt', message: text }
      if (images.length) cmd.images = images
      if (streaming) cmd.streamingBehavior = delivery === 'steer' ? 'steer' : 'followUp'
      // An idle prompt acks in milliseconds. A "/name" prompt runs the
      // extension handler synchronously before its preflight ack, so that
      // duration is unbounded by construction and gets a generous ceiling.
      // A timeout is an UNKNOWN outcome (504), never a failure: pi defers
      // prompts issued inside the settle window and acks them later, so
      // treating it as a failure invites a duplicate execution.
      try {
        const ceiling = text.startsWith('/') ? 300000 : streaming ? 15000 : 30000
        let resp = await s.send(cmd, { timeoutMs: ceiling })
        if (resp?.success === false && /compaction is in progress/i.test(String(resp.error ?? ''))) {
          console.log(`[send] ${s.id}: pi is compacting — holding the prompt (${COMPACT_SEND_WAIT_MS}ms) until the summary lands`)
          const settled = await waitForCompactionEnd(s.id, COMPACT_SEND_WAIT_MS)
          if (!settled)
            return sendJson(
              res,
              { name: 'SessionBusyError', error: String(resp.error ?? 'compaction in progress') },
              409,
            )
          // another tab may have started a turn while we waited: then this is a
          // queue request, exactly as the client asked for it
          if (s.isStreaming) cmd.streamingBehavior = delivery === 'steer' ? 'steer' : 'followUp'
          resp = await s.send(cmd, { timeoutMs: ceiling })
        }
        if (resp?.success === false) return sendJson(res, apiErr(resp.error ?? 'prompt rejected'), 502)
      } catch (err) {
        const message = String(err?.message ?? err)
        if (/timed out/i.test(message)) return sendJson(res, { name: 'Timeout', data: { message, unknown: true } }, 504)
        return sendJson(res, { name: 'Error', data: { message } }, 500)
      }
      res.writeHead(204)
      res.end()
      return
    }
    if (rest === '/abort' && req.method === 'POST') {
      // fire-and-forget: pi ACKs async; don't hold the HTTP request on it
      s.abort({}, { timeoutMs: 5000 }).catch(() => {})
      return sendJson(res, { ok: true })
    }
    if (rest === '/todo') return sendJson(res, [])
    if (/^\/message\/[^/]+$/.test(rest) && req.method === 'DELETE') return sendJson(res, { ok: true })
    if (rest === '/share') {
      // pi has no share concept; answer so the sidebar button doesn't stick
      return req.method === 'DELETE' ? sendJson(res, { ok: true }) : sendJson(res, { error: 'not wired in pi-web (mockup)' }, 501)
    }
    if (rest === '/command' && req.method === 'POST') {
      // pi runs an extension command by matching a leading "/name" on a prompt
      // (tau's own session-tree navigation does the same), so that is the wire
      // format for palette picks. Output goes out as RPC `notify` events, which
      // this server has no client for — the effect (a refreshed catalogue)
      // shows up when the picker refetches.
      const body = await readBody(req)
      const args = Array.isArray(body.arguments) ? body.arguments.filter(Boolean).join(' ') : ''
      const cmd = { type: 'prompt', message: `/${body.command}${args ? ` ${args}` : ''}` }
      if (s.isStreaming) cmd.streamingBehavior = 'followUp'
      try {
        // Extension commands execute before the ack and may drive an LLM, so no
        // number here is safe: surface a timeout as unknown (504) rather than
        // as a failure the webui would bounce back into the composer.
        await s.send(cmd, { timeoutMs: 300000 })
      } catch (err) {
        const message = String(err?.message ?? err)
        if (/timed out/i.test(message)) return sendJson(res, { name: 'Timeout', data: { message, unknown: true } }, 504)
        return sendJson(res, { name: 'Error', data: { message } }, 500)
      }
      return sendJson(res, { ok: true })
    }
    if (rest === '/summarize' && req.method === 'POST') {
      // opencode's summarize == pi's RPC `compact`: one summarisation call on
      // the SESSION's own model, then a CompactionEntry is appended (pi keeps
      // the raw transcript, so the webui transcript does NOT shrink — only the
      // model context does, from firstKeptEntryId on).
      //
      // The body's {providerID,modelID} is deliberately ignored: pi has no
      // model override on `compact`, and quietly set_model-ing to the picker's
      // model would change the model of the user's NEXT turn, which /compact
      // never asked for. The webui sends the SESSION's model anyway, and
      // prompts already keep pi's session model in sync. `customInstructions`
      // (pi's `/compact <instructions>` arg) is honoured for API clients.
      const body = await readBody(req)
      const instructions =
        typeof body.customInstructions === 'string' ? body.customInstructions.trim() : ''
      const cmd = { type: 'compact', ...(instructions ? { customInstructions: instructions } : {}) }
      const wait = (compactingWait.get(s.id) ?? 0) + 1
      compactingWait.set(s.id, wait)
      const release = () => {
        const n = (compactingWait.get(s.id) ?? 1) - 1
        if (n > 0) compactingWait.set(s.id, n)
        else compactingWait.delete(s.id)
      }
      const rpc = s.send(cmd, { timeoutMs: COMPACT_RPC_MS })
      // pi always answers eventually; after we replied "started" nothing else
      // holds this promise, so it must not become an unhandled rejection.
      rpc.catch(() => {})
      const raced = await Promise.race([
        rpc.then(
          (r) => ({ how: 'rpc', r }),
          (err) => ({ how: 'rpc', err }),
        ),
        new Promise((done) => setTimeout(() => done({ how: 'ceiling' }), SUMMARIZE_WAIT_MS)),
      ])
      if (raced.how === 'ceiling') {
        // Hand the outcome over to SSE (compactingWait goes -> the bridge
        // reports a failure), and answer as a SUCCESS: the compaction is still
        // running, and a failure toast here invites a second /compact that
        // double-summarises the session.
        release()
        console.log(`[compact] ${s.id}: still summarising after ${SUMMARIZE_WAIT_MS}ms — answering, SSE will finish it`)
        return sendJson(res, { ok: true, started: true, pending: true })
      }
      release()
      // ONE sentence per failure: pi's `compaction_end` message is the canonical
      // string and is recorded on the live entry the moment it lands, so the
      // HTTP body, the SSE tile and GET /session/status quote the same text.
      // tau's own error carries the SAME failure unprefixed — printing both is
      // how a toast and a box end up disagreeing about one event.
      const recorded = () => live.get(s.id)?.compaction?.reason ?? null
      if (raced.err) {
        const message = String(raced.err?.message ?? raced.err)
        // tau gave up on the child, not pi giving up on the summary: the
        // compaction may still land, so say so instead of "failed".
        if (/timed out/i.test(message))
          return sendJson(res, apiErr(`${message} — compaction may still land; check the session before retrying`), 504)
        return sendJson(res, apiErr(recorded() ?? message), 500)
      }
      const r = raced.r
      if (r?.success === false) {
        // pi's own reasons are the useful ones: "Already compacted",
        // "Nothing to compact (session too small)", "No model selected". These
        // are REJECTS — pi never started a summary, so no compaction_start/end
        // fired and nothing else would record why the box went quiet.
        const reason = recorded() ?? String(r.error ?? 'compact rejected')
        const ent = live.get(s.id)
        // Only a REJECT that never started a summary gets this state. pi's
        // compact() also answers success:false when the summary itself threw —
        // compaction_end already recorded 'failed' then, and clobbering it with
        // 'rejected' would lose the fact that a summary was really attempted.
        if (ent && !ent.compaction) ent.compaction = { state: 'rejected', reason, at: Date.now() }
        return sendJson(res, apiErr(reason), 502)
      }
      const d = r?.data ?? {}
      console.log(
        `[compact] ${s.id}: ${d.tokensBefore ?? '?'} -> ${d.estimatedTokensAfter ?? '?'} tokens (kept ${d.firstKeptEntryId ?? '?'})`,
      )
      return sendJson(res, {
        ok: true,
        summary: d.summary ?? '',
        firstKeptEntryId: d.firstKeptEntryId,
        tokensBefore: d.tokensBefore,
        estimatedTokensAfter: d.estimatedTokensAfter,
        ...(d.usage ? { usage: d.usage } : {}),
      })
    }
    if (rest === '/revert' && req.method === 'POST') {
      // Revert = move pi's session leaf, not delete anything. pi's RPC has no
      // leaf-move command, so tau's navigateTree drives its bundled extension's
      // `/tau-tree-navigate <entryId>` and verifies the move with a second
      // get_tree; picking a USER entry moves the leaf to that entry's PARENT and
      // hands back its text (the composer refill the webui does client-side).
      const body = (await readBody(req)) ?? {}
      const mid = String(body.messageID ?? '').trim()
      if (!mid) return sendJson(res, apiErr('messageID is required'), 400)
      if (s.isStreaming || (s.abortState && s.abortState !== 'idle')) {
        // the webui's isSessionBusy() aborts and retries — the shape is the contract
        return sendJson(res, { error: 'session is streaming', name: 'SessionBusyError' }, 409)
      }
      const e = entryFor(s.id)
      const nav0 = await messageEntryId(s, mid)
      if (nav0.error) return sendJson(res, apiErr(`pi could not report the session tree: ${nav0.error}`), 502)
      if (!nav0.entryId) return sendJson(res, apiErr(`no message ${mid} in this session`), 404)
      let nav = null
      try {
        nav = await navigateTree(s, nav0.entryId)
      } catch (err) {
        const msg = String(err?.message ?? err)
        if (/streaming/i.test(msg)) return sendJson(res, { error: 'session is streaming', name: 'SessionBusyError' }, 409)
        return sendJson(res, apiErr(`failed to move the session leaf: ${msg}`), 502)
      }
      // the boundary every client truncates its view at (webui applyRevert)
      e.revert = { messageID: mid, ...(body.partID ? { partID: String(body.partID) } : {}) }
      filesCache.list = null
      emitSessionUpdated(s.id, s, raw, true)
      return sendJson(
        res,
        {
          ...(ocSession(s) ?? { ok: true }),
          // tau hands the reverted text back because an RPC-mode extension cannot
          // fill an editor; the webui already has it locally, but an API user
          // should not have to re-read the transcript for it.
          ...(nav?.editorText ? { editorText: nav.editorText } : {}),
        },
        200,
      )
    }
    if (rest === '/fork' && req.method === 'POST') {
      // The webui contract (api.ts forkSession, Transcript.svelte forkFrom,
      // commands.ts /fork): return the NEW session (id + title); the engine
      // copies everything BEFORE the picked message and titles the fork itself.
      // No child is spawned for the fork — the new tab's first request resumes it
      // through the normal cold path, with the uuid as its public id from byte one.
      const body = (await readBody(req)) ?? {}
      const mid = String(body.messageID ?? '').trim()
      const file = fileForId(raw)
      if (!file || !fs.existsSync(file)) {
        // a created-but-never-prompted session has no transcript to copy
        return sendJson(res, apiErr('this session has no transcript to fork yet'), 400)
      }
      const chain = leafChain(readJsonl(file)).filter((e) => e.type !== 'session')
      let keep = chain
      if (mid) {
        const entryId = entryIdInChain(chain, mid)
        if (!entryId) return sendJson(res, apiErr(`no message ${mid} in this session`), 404)
        const i = chain.findIndex((e) => e.id === entryId)
        if (i < 0) return sendJson(res, apiErr(`message ${mid} is not on this session's active path`), 400)
        keep = chain.slice(0, i) // pi's position:"before" — the picked message is excluded
      }
      const n = forkCount(file) + 1
      const parentTitle = sessionMeta(file)?.title ?? ''
      let fork = null
      try {
        fork = forkTranscript(file, keep, `${parentTitle} (fork #${n})`.trim())
      } catch (err) {
        return sendJson(res, apiErr(`failed to write the fork: ${String(err?.message ?? err)}`), 500)
      }
      filesCache.list = null
      const meta = sessionMeta(fork.file)
      console.log(`[fork] ${raw}: ${keep.length} of ${chain.length} entries -> ${path.basename(fork.file)} (parent ${path.basename(file)})`)
      return sendJson(
        res,
        {
          id: fork.id,
          title: meta?.title ?? `${parentTitle} (fork #${n})`,
          cwd: meta?.cwd ?? s?.cwd ?? PI_CWD,
          time: { created: meta?.created ?? Date.now(), updated: meta?.updated ?? Date.now() },
          ...(meta?.tokens ? { tokens: meta.tokens } : {}),
          ...(meta?.tokens_generated ? { tokens_generated: meta.tokens_generated } : {}),
          cost: 0,
          model: { providerID: meta?.model_provider ?? '', id: meta?.model ?? '' },
          revert: null,
        },
        200,
      )
    }
    return sendJson(res, { error: 'not found' }, 404)
  }

  // ---- history / search (pi's own files) ----------------------------------
  if (pathname === '/api/history/sessions') {
    const meta = loadMeta()
    return sendJson(
      res,
      allSessionMeta().map((r) => {
        const mm = meta[r.id]
        return { ...r, file: undefined, ...(mm?.star ? { star: true } : {}), ...(mm?.tag ? { tag: mm.tag } : {}) }
      }),
    )
  }
  if (pathname === '/api/history/errors') {
    return sendJson(res, [...errTiles.entries()].flatMap(([sid, list]) => list.map((e) => ({ ...e, sid }))))
  }
  const em = pathname.match(/^\/api\/history\/session\/([^/]+)\/errors$/)
  if (em) {
    const sid = decodeURIComponent(em[1])
    const list = errTiles.get(sid) ?? []
    if (req.method === 'POST') {
      const b = await readBody(req)
      if (b.message && !list.some((x) => x.message === b.message)) {
        list.push({ seq: list.length + 1, message: String(b.message), t: b.t ?? Date.now() })
        errTiles.set(sid, list)
      }
      return sendJson(res, { ok: true })
    }
    if (req.method === 'DELETE') {
      errTiles.delete(sid)
      return sendJson(res, { ok: true })
    }
    return sendJson(res, list)
  }
  const mm = pathname.match(/^\/api\/history\/session\/([^/]+)\/meta$/)
  if (mm) {
    const sid = decodeURIComponent(mm[1])
    const all = loadMeta()
    if (req.method === 'DELETE') {
      delete all[sid]
      saveMeta(all)
      return sendJson(res, { ok: true })
    }
    if (req.method === 'PUT' || req.method === 'POST') {
      const b = await readBody(req)
      const cur = all[sid] ?? { star: false, tag: null }
      if (typeof b.star === 'boolean') cur.star = b.star
      if (b.tag !== undefined) cur.tag = b.tag || null
      all[sid] = cur
      saveMeta(all)
      return sendJson(res, { ok: true, ...cur })
    }
    return sendJson(res, all[sid] ?? { star: false, tag: null })
  }
  const sm = pathname.match(/^\/api\/history\/session\/([^/]+)\/changes$/)
  if (sm) return sendJson(res, { files: [] })
  const hm = pathname.match(/^\/api\/history\/session\/([^/]+)$/)
  if (hm) {
    const entries = entriesForId(decodeURIComponent(hm[1]))
    if (!entries) return sendJson(res, { error: 'unknown session' }, 404)
    return sendJson(res, toHistMsgs(entries))
  }
  if (pathname === '/api/search') return sendJson(res, searchSessions(q.get('q') || ''))

  if (pathname === '/healthz') {
    return sendJson(res, {
      ok: true,
      live: liveManager.sessions.size,
      // which records no branch looked at, and how often — `curl :8124/healthz`
      // answers "did the bridge even see that event?" without log spelunking
      unhandled: Object.fromEntries(unhandled),
    })
  }

  return sendJson(res, { error: 'not found' }, 404)
}

function modelSpec(model) {
  const { provider, modelId } = modelRefOf({ model })
  return modelId ? `${provider}/${modelId}` : ''
}

/**
 * One shape for every model the webui posts, because it posts two. Picks that
 * name a session (`createSession`, `setSessionModel`) use `model: {providerID,
 * id, variant?}`; the prompt body (`api.ts prompt()`) uses opencode's shape —
 * `model: {providerID, modelID}` with the thinking **variant as a top-level
 * body field**. Reading only `model.id` / `model.variant` silently dropped BOTH
 * out of every prompt: the model pick never landed, and the composer's thinking
 * selector looked dead — for a tab that had no child at pick time the sibling
 * `/api/session/{id}/model` POST goes nowhere, so the prompt body is the only
 * channel that always exists. The engine reports variant:"default" for unset.
 */
function modelRefOf(body) {
  const m = body?.model ?? {}
  const modelId = typeof m.id === 'string' ? m.id : typeof m.modelID === 'string' ? m.modelID : ''
  const provider = typeof m.providerID === 'string' ? m.providerID : typeof m.provider === 'string' ? m.provider : ''
  const v = typeof m.variant === 'string' ? m.variant : typeof body?.variant === 'string' ? body.variant : ''
  return { provider, modelId, variant: v && v !== 'default' ? v : null }
}

function expandHome(p) {
  if (!p) return ''
  if (p === '~') return process.env.HOME || p
  if (p.startsWith('~/')) return path.join(process.env.HOME || '', p.slice(2))
  return p
}

function readHeaderCwd(file) {
  for (const e of readJsonl(file)) if (e.type === 'session') return e.cwd || null
  return null
}

/**
 * A transcript's header cwd is a snapshot of the workspace as it was when the
 * session STARTED, and workspaces get renamed and moved (a repo dir rename is
 * enough). pi will not run in a directory that is gone: tau throws
 * `Directory not found: <cwd>` (pi-tau-web-server/bin/sessions.js:168) BEFORE it
 * spawns, so every route through `sessionFor` rejects — the session stops being
 * readable, let alone sendable — while the sidebar keeps advertising a workspace
 * that does not exist. So a header cwd is honoured only if it is still a
 * directory; otherwise fall back to its nearest existing ANCESTOR (the repo moved,
 * its parent did not) and finally to PI_CWD. Never the filesystem root: a
 * transcript carried in from another machine must not put a pi child in `/`.
 */
function liveCwd(p) {
  if (!p) return ''
  let cur = path.resolve(expandHome(String(p)))
  for (;;) {
    if (cur === path.parse(cur).root) return ''
    try {
      if (fs.statSync(cur).isDirectory()) return cur
    } catch {}
    const up = path.dirname(cur)
    if (up === cur) return ''
    cur = up
  }
}

function seedDup(entries, e) {
  for (const m of messagesOf(entries).map((x) => x.message)) {
    if (LIVE_ROLES.has(m.role)) msgId(m.role, m.timestamp, e.dup)
  }
}

// ---------------------------------------------------------------------------
// boot
// ---------------------------------------------------------------------------

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://x')
    if (url.pathname.startsWith('/oc/') || url.pathname.startsWith('/api/') || url.pathname === '/healthz') {
      await handleApi(req, res, url)
      return
    }
    if (url.pathname === '/oc') {
      res.writeHead(302, { location: '/oc/session' })
      res.end()
      return
    }
    serveStatic(req, res, url.pathname)
  } catch (err) {
    try {
      sendJson(res, { error: String(err?.message ?? err) }, 500)
    } catch {
      /* headers already sent */
    }
  }
})

// heartbeat: sse.ts treats >30s of silence on an OPEN stream as dead
// Engine telemetry: one global 1 Hz tick. It issues ZERO requests while nothing
// is busy, and never blocks a request handler (results land in the live entry).
if (ENGINE_PROBE_MS > 0) {
  setInterval(() => {
    engineTick().catch((e) => console.log('[engine] probe failed:', e?.message ?? e))
  }, ENGINE_PROBE_MS)
}

const hb = setInterval(() => emit('heartbeat', {}), 10000)
hb.unref?.()

// idle/overflow pi children — see the reclamation block near `live`
const reaper = setInterval(() => {
  reapIdleChildren().catch((err) => console.error('[reap]', err?.message ?? err))
}, REAP_INTERVAL_MS)
reaper.unref?.()

server.listen(PORT, HOST, () => {
  console.log(`pi-web  http://${HOST}:${PORT}`)
  console.log(`  dist    ${WEBUI_DIST}`)
  console.log(`  pi dir  ${PI_AGENT_DIR}`)
  console.log(`  sessions ${SESSIONS_DIR}`)
  console.log(`  cwd     ${PI_CWD}`)
  if (!fs.existsSync(path.join(WEBUI_DIST, 'index.html'))) {
    console.warn(
      `WARNING: ${WEBUI_DIST}/index.html missing — API works but the UI will 404.\n` +
        `  Docker: the /app/webui mount needs a built dist; on the host run:\n` +
        `    cd ../opencode/webui && npm ci && npm run build\n` +
        `  Bare metal: pass --dist /path/to/opencode/webui/dist`,
    )
  }
})

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    clearInterval(hb)
    clearInterval(reaper)
    server.close()
    try {
      await liveManager.shutdown()
    } catch {
      /* best effort */
    }
    process.exit(0)
  })
}
