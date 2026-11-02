// pi-web/lib/live-events.mjs — live pi/tau events -> opencode SSE.
import { compactingWait, compactionId, compactionRow, settleCompaction } from './compaction.mjs'
import { isoMs, safeJson, textOf } from './content.mjs'
import { newDerived, noteDerived } from './engine.mjs'
import { LIVE_ROLES, messagesOf, msgId } from './ids.mjs'
import { emitFor, emitSessionUpdated, infoSig } from './notify.mjs'
import { INTERNAL_CLOSE, byPublicId, entryFor, live, publicIdsFor, reapedFile } from './registry.mjs'
import { autoName } from './sessions.mjs'
import { emit } from './sse.mjs'
import { liveManager } from './tau.mjs'
import { noteUnhandled } from './telemetry.mjs'
import { PARTIAL_CAP, TOOL_TITLE_MS, partialTitle, toolTitle } from './titles.mjs'
import { toOcMessage, toolMetadata, toolWindow } from './translate.mjs'
import { apiErr } from './util.mjs'

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
  } else if (b.type === 'image') {
    const mime = b.mimeType ?? b.mime ?? 'image/png'
    const data = b.data ?? b.source?.data ?? ''
    emitFor(tauId, 'message.part.updated', { part: { ...base, type: 'file', url: `data:${mime};base64,${data}`, mime, filename: b.filename ?? '' } })
  }
}

export { onTauFrame }
