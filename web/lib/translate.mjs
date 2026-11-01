// pi-web/lib/translate.mjs — pi message -> opencode message (the one real
// translation).
import { compactionRow } from './compaction.mjs'
import { isoMs, safeJson, textOf } from './content.mjs'
import { LIVE_ROLES, messagesOf, msgId } from './ids.mjs'
import { toolTitle } from './titles.mjs'

// ---------------------------------------------------------------------------
// pi message -> opencode message (the one real translation)
// ---------------------------------------------------------------------------
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

export { toOcMessage, toOcMessages, toolMetadata, toolWindow }
