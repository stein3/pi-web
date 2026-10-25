// API check: pi-web message timing. pi stamps messages with epoch-ms NUMBERS,
// `message.durationMs` is the generation time (timestamp = turn START), and
// tool results are their own messages carrying END timestamp + runtime — the
// projection must pass all of it through, because the webui derives the
// message clock time, duration, tokens/s AND each tool's duration badge from
// `time.created` / `time.completed` / part `state.time` alone.
//
//   BASE_URL=http://127.0.0.1:8127 \
//   PI_CODING_AGENT_DIR=/tmp/piwtest/pi-agent node .testbin/time-check.mjs
// It reads a REAL pi-web transcript (not the fake pi), so it exits 2 rather than
// pretending when there is nothing to read. One assertion — 'no message stamped
// now' — needs a live REAL pi child, because the fake pi answers prompts but
// never writes JSONL; against the fake pi that single check fails by design.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

const BASE = process.env.BASE_URL || 'http://127.0.0.1:8127'
const SESSIONS_DIR =
  process.env.SESSIONS_DIR ||
  path.join(process.env.PI_CODING_AGENT_DIR || path.join(os.tmpdir(), 'piwtest', 'pi-agent'), 'sessions')
if (!fs.existsSync(SESSIONS_DIR)) {
  console.log(
    `SKIP: no sessions dir at ${SESSIONS_DIR}\n` +
      '  start pi-web against a scratch agent dir, run a few turns, then point\n' +
      '  PI_CODING_AGENT_DIR (or SESSIONS_DIR) at it and BASE_URL at the server.',
  )
  process.exit(2)
}

// Preflight: without the API side every assertion below is noise, and a red
// FAIL that is really just 'no server running' teaches nobody anything.
try {
  const ping = await fetch(BASE + '/healthz')
  if (!ping.ok) throw new Error(`HTTP ${ping.status}`)
} catch (e) {
  console.log(
    `SKIP: no pi-web at ${BASE} (${e?.message ?? e}) — run it with PI_CODING_AGENT_DIR\n` +
      '  pointed at the same agent dir you point SESSIONS_DIR at.',
  )
  process.exit(2)
}
console.log(`sessions dir: ${SESSIONS_DIR}`)

let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}
const j = async (u) => {
  const r = await fetch(BASE + u)
  if (!r.ok) throw new Error(`${r.status} ${u}`)
  return r.json()
}
const readJsonl = (f) =>
  fs
    .readFileSync(f, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l)
      } catch {
        return null
      }
    })
    .filter(Boolean)

try {
  // ---- pick a history session with assistant timing AND tool results ----
  const files = []
  for (const d of fs.readdirSync(SESSIONS_DIR)) {
    const dir = path.join(SESSIONS_DIR, d)
    if (!fs.statSync(dir).isDirectory()) continue
    for (const f of fs.readdirSync(dir)) if (f.endsWith('.jsonl')) files.push(path.join(dir, f))
  }
  // most tool results first (widest coverage), newest file breaking ties
  const candidates = files
    .map((f) => ({ f, entries: readJsonl(f) }))
    .map((x) => ({
      ...x,
      tools: x.entries.filter((e) => e.type === 'message' && e.message.role === 'toolResult').length,
      timed: x.entries.some(
        (e) =>
          e.type === 'message' &&
          e.message.role === 'assistant' &&
          typeof e.message.durationMs === 'number' &&
          (e.message.usage?.output ?? 0) > 0,
      ),
    }))
    .filter((x) => x.timed && x.tools > 0)
    .sort((a, b) => b.tools - a.tools || b.f.localeCompare(a.f))
  if (!candidates.length) throw new Error('no history session with assistant + tool timing under ' + SESSIONS_DIR)
  const { f, entries } = candidates[0]
  const headerId = entries.find((e) => e.type === 'session')?.id
  const fileMsgs = entries.filter((e) => e.type === 'message').map((e) => e.message)
  const fileAssist = fileMsgs.filter((m) => m.role === 'assistant')
  const toolByCall = new Map()
  for (const m of fileMsgs) if (m.role === 'toolResult' && m.toolCallId) toolByCall.set(m.toolCallId, m)
  console.log(
    `session ${headerId} (${path.basename(f)}): ${fileAssist.length} assistant, ${toolByCall.size} tool results`,
  )

  /** expected wall window: generation end, extended by the last tool this msg called */
  const expectedEnd = (m) => {
    const created = m.timestamp
    let end = typeof m.durationMs === 'number' ? created + m.durationMs : created
    for (const b of m.content ?? []) {
      if (b?.type !== 'toolCall') continue
      const tr = toolByCall.get(b.id)
      if (tr && typeof tr.timestamp === 'number' && tr.timestamp > end) end = tr.timestamp
    }
    return end
  }

  // ---- fetch the same session through the projection ----
  const rows = await j('/api/history/sessions')
  check(rows.some((r) => r.id === headerId), 'history endpoint lists the session')
  await j(`/oc/session/${encodeURIComponent(headerId)}`) // resumes (fake pi)
  const msgs = await j(`/oc/session/${encodeURIComponent(headerId)}/message`)

  const got = msgs.filter((m) => m.role === 'assistant')
  check(got.length === fileAssist.length, `assistant message count matches file (${got.length}/${fileAssist.length})`)

  const now = Date.now()
  let createdExact = 0
  let endExact = 0
  let toolWin = 0
  let toolBad = 0
  let statShown = 0
  const tpsSeen = []
  const stale = []
  for (let i = 0; i < Math.min(got.length, fileAssist.length); i++) {
    const m = got[i]
    const src = fileAssist[i]
    if (m.time?.created === src.timestamp) createdExact++
    if (m.time?.completed === expectedEnd(src)) endExact++
    if (typeof m.time?.created === 'number') stale.push(m.time.created)

    // per-tool window: toolResult.timestamp is the END, durationMs the runtime.
    // A part that CLAIMED a window (time.end set) but has no matching
    // toolResult is a join miss — fail loudly rather than skipping (that's how
    // a broken read join would hide). A running/aborted part with no result is
    // fine and contributes no window.
    let toolMs = 0
    for (const p of m.parts ?? []) {
      if (p.type !== 'tool' || !p.callID) continue
      const tr = toolByCall.get(p.callID)
      const t = p.state?.time ?? {}
      if (!tr) {
        if (typeof t.end === 'number') toolBad++
        continue
      }
      toolMs += Math.max(0, (t.end ?? 0) - (t.start ?? 0))
      if (t.end === tr.timestamp && (typeof tr.durationMs !== 'number' || t.end - t.start === tr.durationMs)) toolWin++
      else toolBad++
    }

    // Transcript.svelte's own math: toks = output+reasoning, agent time =
    // (completed-created) minus tool spans
    const toks = (m.tokens?.output ?? 0) + (m.tokens?.reasoning ?? 0)
    const agentMs = Math.max(0, Math.max(0, (m.time?.completed ?? 0) - (m.time?.created ?? 0)) - toolMs)
    if (toks > 0 && agentMs > 0) {
      statShown++
      tpsSeen.push(toks / (agentMs / 1000))
    }
  }

  check(createdExact === fileAssist.length, `time.created is pi's epoch-ms stamp (${createdExact}/${fileAssist.length})`)
  check(endExact === fileAssist.length, `time.completed = gen end extended to last tool (${endExact}/${fileAssist.length})`)
  check(
    stale.length > 0 && Math.max(...stale) < now - 60_000,
    `no message stamped "now" (newest created is ${Math.round((now - Math.max(...stale)) / 60000)} min old)`,
  )
  check(toolWin > 0 && toolBad === 0, `tool windows = result timestamp - durationMs (${toolWin} ok, ${toolBad} bad)`)
  check(statShown > 0, `tokens/s block would render for ${statShown} message(s)`)
  check(
    tpsSeen.length > 0 && tpsSeen.every((v) => v >= 0.05 && v <= 2000),
    `tokens/s in a sane range (${tpsSeen.slice(0, 5).map((v) => v.toFixed(1)).join(', ')})`,
  )

  // ---- search hit times must be the message's own time, not Date.now() ----
  const hits = await j('/api/search?q=the')
  check(hits.length > 0, `search returns hits (${hits.length})`)
  check(
    hits.every((h) => h.time < now - 60_000) && new Set(hits.map((h) => h.time)).size > 1,
    `search hit times are message times, distinct (${new Set(hits.map((h) => h.time)).size} distinct)`,
  )
} catch (e) {
  console.log('ERROR', e.message)
  failed++
}

console.log(failed ? `${failed} FAILED` : 'ALL PASS')
process.exit(failed ? 1 : 0)
