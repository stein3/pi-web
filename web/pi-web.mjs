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
 * Usage:  node pi-web.mjs [--port 8124] [--cwd /workspace] [--dir ~/.pi/agent]
 *                         [--dist /workspace/opencode/webui/dist]
 */

import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { createHash } from 'node:crypto'
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
const WEBUI_DIST = path.resolve(
  arg('dist', process.env.WEBUI_DIST || '/workspace/opencode/webui/dist'),
)
const PI_CWD = path.resolve(arg('cwd', process.env.PI_CWD || '/workspace'))
const PI_AGENT_DIR = path.resolve(
  arg('dir', process.env.PI_CODING_AGENT_DIR || path.join(HERE, '..', 'config', 'pi-agent')),
)
process.env.PI_CODING_AGENT_DIR = PI_AGENT_DIR

const { liveManager } = await import('pi-tau-web-server/bin/sessions.js')
const { SESSIONS_DIR } = await import('pi-tau-web-server/bin/config.js')
const { getAvailableModels } = await import(
  'pi-tau-web-server/bin/model-utils.js'
)
// tau's directory completer for the new-session workspace picker
const { completeDirectories } = await import(
  'pi-tau-web-server/bin/path-completion.js'
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

function entryFor(tauId) {
  let e = live.get(tauId)
  if (!e) {
    e = { ids: new Set([tauId]), dup: new Map(), cur: null, title: null, byCall: new Map(), toolStart: new Map() }
    live.set(tauId, e)
  }
  return e
}

function publicIdsFor(tauId) {
  return live.get(tauId)?.ids ?? [tauId]
}

function sessionByAnyId(id) {
  const s = liveManager.get(id)
  if (s) return s
  const tauId = byPublicId.get(id)
  if (tauId) return liveManager.get(tauId) ?? null
  return null
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
  const msgs = messagesOf(entries).map((e) => e.message)
  const results = new Map()
  for (const m of msgs) if (m.role === 'toolResult' && m.toolCallId) results.set(m.toolCallId, m)

  const dupSeen = new Map()
  const out = []
  for (const m of msgs) {
    if (!LIVE_ROLES.has(m.role)) continue
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
  for (const id of publicIdsFor(tauId)) emit(type, { ...properties, sessionID: id })
}

function onTauFrame(frame) {
  if (frame.type === 'live_session_created' || frame.type === 'live_session_updated') {
    const s = liveManager.get(frame.session?.id)
    if (s) emitFor(s.id, 'session.updated', { info: ocSession(s) })
    return
  }
  if (frame.type === 'live_session_closed') {
    const ids = publicIdsFor(frame.sessionId)
    live.delete(frame.sessionId)
    for (const id of ids) {
      byPublicId.delete(id)
      emit('session.deleted', { sessionID: id })
    }
    return
  }
  if (frame.type !== 'event' || !frame.event || !frame.sessionId) return
  onPiEvent(frame.sessionId, frame.event)
}

function onPiEvent(tauId, ev) {
  const e = live.get(tauId) ?? entryFor(tauId)
  const session = liveManager.get(tauId)
  const t = ev.type

  if (t === 'session_name' && ev.name) {
    e.title = ev.name
    emitFor(tauId, 'session.updated', { info: ocSession(session) })
    return
  }

  if (t === 'message_start' && ev.message && LIVE_ROLES.has(ev.message.role)) {
    const mid = msgId(ev.message.role, ev.message.timestamp, e.dup)
    e.cur = { mid, role: ev.message.role, byIndex: new Map() }
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
      const args = a.type === 'toolcall_end' ? safeJson(a.toolCall?.arguments) : {}
      const name = a.toolName ?? a.toolCall?.name ?? 'tool'
      // entry-level map: pi emits the assistant message_end BEFORE the
      // tool_execution_* events, so cur is null by the time they arrive
      if (a.id) e.byCall.set(a.id, { pid, mid: cur.mid, tool: name, input: args })
      emitFor(tauId, 'message.part.updated', {
        part: {
          id: pid,
          messageID: cur.mid,
          sessionID: tauId,
          type: 'tool',
          tool: name,
          callID: a.id,
          state: {
            status: 'running',
            title: toolTitle(name, args),
            input: args && typeof args === 'object' ? args : {},
            output: '',
            metadata: {},
            time: { start: Date.now() },
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
    }
    return
  }

  if (t === 'tool_execution_start' || t === 'tool_execution_update' || t === 'tool_execution_end') {
    const hit = e.byCall.get(ev.toolCallId)
    if (!hit) return
    if (t === 'tool_execution_start') e.toolStart.set(ev.toolCallId, Date.now())
    const res = t === 'tool_execution_end' ? ev.result : undefined
    const partial = t === 'tool_execution_update' ? ev.partialResult : undefined
    const input = safeJson(ev.args)
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
          input: input && typeof input === 'object' ? input : {},
          output: t === 'tool_execution_end' ? textOf(res) : (partial ?? ''),
          // live path: `ev.result` is the ToolResult object, details included
          metadata: toolMetadata(input, res?.details),
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
    emitFor(tauId, 'session.idle', { sessionID: tauId })
    e.cur = null
    return
  }
  if (t === 'agent_start' || t === 'turn_start') {
    emitFor(tauId, 'session.status', { sessionID: tauId, status: { type: 'busy' } })
    return
  }
  if (t === 'compaction_start' || t === 'summarization_retry_scheduled') {
    emitFor(tauId, 'session.next.compaction.started', { sessionID: tauId })
    return
  }
  if (t === 'compaction_end' || t === 'summarization_retry_finished') {
    emitFor(tauId, 'session.next.compaction.ended', { sessionID: tauId })
    return
  }
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

function ocSession(s, publicId) {
  if (!s) return null
  const e = live.get(s.id)
  const title = e?.title || s.sessionName || null
  return {
    id: publicId ?? s.id,
    title: title || undefined,
    cwd: s.cwd || undefined,
    time: { created: Date.parse(s.createdAt) || Date.now(), updated: Date.parse(s.lastActiveAt) || Date.now() },
    cost: 0,
    agent: undefined,
    model: s.model ? { providerID: s.model.provider || '', id: s.model.id || '' } : undefined,
    revert: null,
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
  for (const e of entries) {
    if (e.type === 'session_info' && e.name) return e.name
  }
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

function sessionMeta(file) {
  const st = fs.statSync(file)
  const entries = readJsonl(file)
  const header = entries.find((e) => e.type === 'session')
  if (!header?.id) return null
  const msgs = messagesOf(entries)
  const users = msgs.filter((m) => m.message.role === 'user').length
  if (users === 0 && entries.length <= 8) return null
  const tauId = liveTauIdFor(file)
  const info = entries.find((e) => e.type === 'session_info' && e.name)
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
  return {
    id: tauId ?? header.id,
    file,
    cwd: header.cwd || undefined,
    title: info?.name || deriveTitle(entries) || header.id.slice(0, 12),
    created: Date.parse(header.timestamp) || st.mtimeMs,
    updated: st.mtimeMs,
    message_count: msgs.length,
    cost: 0,
    model: modelChange?.modelId ?? '',
    model_provider: modelChange?.provider ?? '',
    ...(tokens ? { tokens } : {}),
    live: !!tauId,
  }
}

function allSessionMeta() {
  const rows = []
  for (const f of listFilesCached()) {
    try {
      const m = sessionMeta(f)
      if (m) rows.push(m)
    } catch {
      /* unreadable file */
    }
  }
  rows.sort((a, b) => b.updated - a.updated)
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
  if (s?.sessionFile) return s.sessionFile
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
  const live = sessionByAnyId(id)
  if (live) return live
  const file = fileForId(id)
  if (!file) return null
  const entries = readJsonl(file)
  const s = await liveManager.resume({
    sessionFile: file,
    cwd: readHeaderCwd(file) ?? PI_CWD,
    entries,
    sessionName: deriveTitle(entries) || null,
  })
  const e = entryFor(s.id)
  e.ids.add(id)
  byPublicId.set(id, s.id)
  seedDup(entries, e)
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
const NOT_WIRED = new Set([
  '/oc/session/fork',
  '/oc/session/revert',
  '/oc/session/summarize',
])
// opencode surfaces that degrade cleanly to "nothing here"
const EMPTY_VCS = { '/oc/vcs/diff': [], '/oc/vcs/diff/raw': '' }

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
let routerCache = { at: 0, base: '', models: [] }

function readJson(p) {
  try {
    return JSON.parse(fs.readFileSync(p, 'utf8'))
  } catch {
    return null
  }
}

/** Providers parsed out of pi's models-store.json; [] when there is no store. */
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
  const providers = []
  for (const [pid, entry] of Object.entries(raw)) {
    const models = {}
    let routerBase = ''
    for (const m of entry?.models ?? []) {
      if (!m?.id || m.type) continue // typed sections (vision) are not chat models
      if (!routerBase && m.baseUrl) routerBase = String(m.baseUrl).replace(/\/v1\/?$/, '')
      models[m.id] = {
        id: m.id,
        name: m.name ?? m.id,
        limit: { context: m.contextWindow ?? 0, output: m.maxTokens ?? 0 },
        ...(m.reasoning === undefined ? {} : { reasoning: !!m.reasoning }),
      }
    }
    if (Object.keys(models).length) providers.push({ id: pid, name: pid, models, routerBase })
  }
  storeCache.mtime = st.mtimeMs
  storeCache.providers = providers
  return providers
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
    const out = {}
    for (const s of liveManager.sessions.values()) {
      const ids = publicIdsFor(s.id)
      for (const id of ids) out[id] = { type: s.isStreaming ? 'busy' : 'idle' }
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
    if (s && am[2] === 'model' && body.model?.id) {
      await s
        .send({ type: 'set_model', provider: body.model.providerID ?? '', modelId: body.model.id }, { timeoutMs: 10000 })
        .catch(() => {})
      emitFor(s.id, 'session.updated', { info: ocSession(s) })
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
    entryFor(s.id)
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
        if (s) {
          await liveManager.delete(s.id, 'closed_by_user')
        } else {
          const meta = allSessionMeta()
          const m = meta.find((sm) => sm.id === raw)
          if (m && m.file) {
            try {
                const file = m.file
                const dir = path.dirname(file)
                const TRASH_DIR = path.join(SESSIONS_DIR, 'trash')
                if (!fs.existsSync(TRASH_DIR)) {
                  fs.mkdirSync(TRASH_DIR, { recursive: true })
                }
                const dest = path.join(TRASH_DIR, path.basename(file))
                let finalDest = dest
                let counter = 1
                while (fs.existsSync(finalDest)) {
                  const ext = path.extname(file)
                  const base = path.basename(file, ext)
                  finalDest = path.join(TRASH_DIR, `${base}_${counter}${ext}`)
                  counter++
                }
                fs.renameSync(file, finalDest)
                if (fs.readdirSync(dir).length === 0) {
                  fs.rmdirSync(dir)
                }
              } catch (err) {
                return sendJson(res, { error: `failed to trash: ${err.message}` }, 500)
              }
          } else {
            return sendJson(res, { error: 'unknown session' }, 404)
          }
        }
        filesCache.list = null
        return sendJson(res, { ok: true })
      }
      if (req.method === 'PATCH') {
        const body = await readBody(req)
        const s = sessionByAnyId(raw)
        if (s && typeof body.title === 'string') {
          const e = entryFor(s.id)
          e.title = body.title
          // tau-style rename: append session_info (file path must stay put —
          // pi already holds the file open via --session)
          if (s.sessionFile) {
            try {
              fs.appendFileSync(
                s.sessionFile,
                JSON.stringify({ type: 'session_info', name: body.title, timestamp: new Date().toISOString() }) + '\n',
              )
              filesCache.list = null
            } catch {
              /* read-only tree — keep in-memory title only */
            }
          }
          emitFor(s.id, 'session.updated', { info: ocSession(s, raw) })
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
    const s = req.method === 'GET' ? await sessionFor(raw) : sessionByAnyId(raw)
    if (!s) return sendJson(res, { error: 'unknown session' }, 404)
    if (rest === '' && req.method === 'GET') return sendJson(res, ocSession(s, raw))

    if (rest === '/message') {
      const entries = s ? s.entries : null
      let msgs = entries ? toOcMessages(entries) : []
      const limit = Number(q.get('limit') || 0)
      if (limit > 0 && msgs.length > limit) msgs = msgs.slice(-limit)
      return sendJson(res, msgs)
    }
    if (rest === '/prompt_async' && req.method === 'POST') {
      const body = await readBody(req)
      const text = (body.parts ?? [])
        .filter((p) => p.type === 'text')
        .map((p) => p.text)
        .join('\n')
      const cmd = { type: 'prompt', message: text }
      if (body.model?.id) {
        await s
          .send(
            { type: 'set_model', provider: body.model.providerID ?? '', modelId: body.model.id },
            { timeoutMs: 10000 },
          )
          .catch(() => {})
      }
      if (s.isStreaming) cmd.streamingBehavior = 'followUp'
      try {
        await s.send(cmd, { timeoutMs: 15000 })
      } catch (err) {
        return sendJson(res, { name: 'Error', data: { message: String(err?.message ?? err) } }, 500)
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
        await s.send(cmd, { timeoutMs: 15000 })
      } catch (err) {
        return sendJson(res, { name: 'Error', data: { message: String(err?.message ?? err) } }, 500)
      }
      return sendJson(res, { ok: true })
    }
    if (rest === '/fork' || rest === '/revert' || rest === '/summarize') {
      return sendJson(res, { error: 'not wired in pi-web (mockup)' }, 501)
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

  if (pathname === '/healthz') return sendJson(res, { ok: true, live: liveManager.sessions.size })

  return sendJson(res, { error: 'not found' }, 404)
}

function modelSpec(model) {
  if (!model?.id) return ''
  return `${model.providerID ?? ''}/${model.id}`
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
const hb = setInterval(() => emit('heartbeat', {}), 10000)
hb.unref?.()

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
    server.close()
    try {
      await liveManager.shutdown()
    } catch {
      /* best effort */
    }
    process.exit(0)
  })
}
