/**
 * The reported symptom: "tool call title lines are not populated during the
 * call — they only appear after the turn ends." The title line is the detail
 * string Transcript.svelte draws under a tool row (command for bash, path for
 * read/edit, pattern for grep …). It comes from `state.title` / `state.input`,
 * both of which the bridge RECOMPUTES from whatever the current pi event
 * happens to carry. pi's wire shape makes that fatal:
 *
 *   toolcall_start      {id, toolName}                 <- no args at all
 *   toolcall_delta      {contentIndex, delta}          <- args JSON, streamed
 *   toolcall_end        {contentIndex, toolCall}       <- NO top-level id
 *   tool_execution_end  {toolCallId, toolName, result} <- NO args
 *
 * so the LAST live frame for a part is the one that knows the least: title ''
 * and input {} — and the webui shallow-merges parts, so it REPLACES a populated
 * state with a blank one. The row goes blank while the tool runs and heals only
 * when the post-turn GET /message reprojects the transcript from the JSONL,
 * which does have the arguments. That is exactly "appears after turn end".
 *
 * Layers (same shape as toolmeta-check):
 *   1  the bridge: frames actually received
 *   2  the REAL webui client store (sse.ts + stores.ts in node), sampled live
 *   3  the UI predicate itself — toolDetail(), copied VERBATIM below, is what
 *      every assertion measures. Asserting anything else asserts a UI that does
 *      not exist.
 *
 * Self-contained: scratch dir, fake pi, own child on TOOLTITLE_CHECK_PORT
 * (default 8130 — deliberately not 8126/8127/8129, so it can run alongside the
 * other checks), killed by exact PID. Needs the sibling webui checkout →
 * SKIP (exit 2).
 *
 *   node test/tooltitle-check.mjs
 *   PI_WEB_JS=/path/to/pre-fix/pi-web.mjs node test/tooltitle-check.mjs   # red/green A/B
 */
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { BRIDGE, TESTBIN, WEB } from './harness.mjs'
import { loadWebuiClient, webuiSkip } from './webui-client.mjs'

const PORT = Number(process.env.TOOLTITLE_CHECK_PORT || 8130)
const BASE = `http://127.0.0.1:${PORT}`
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}

// --- from webui/src/components/Transcript.svelte, VERBATIM (firstInput, clip,
// --- toolDetail). The question/branches for skills and questions are stubbed:
// --- the scripted tools are bash / read / edit and never reach them.
const QUESTION_RE = /^question/
const skillsIndex = {}
function firstInput(st, keys) {
  const inp = st?.input ?? {}
  for (const k of keys) {
    const v = inp[k]
    if (typeof v === 'string' && v.trim()) return v.trim()
  }
  return ''
}
function clip(s, n = 120) {
  const one = s.replace(/\s+/g, ' ').trim()
  return one.length > n ? one.slice(0, n - 1) + '…' : one
}
function toolDetail(p) {
  const st = p.state ?? {}
  const tool = String(p.tool ?? '').toLowerCase()
  let detail = ''
  if (/bash|shell|cmd/.test(tool)) {
    detail = firstInput(st, ['command', 'cmd', 'script'])
  } else if (/glob/.test(tool)) {
    const path = firstInput(st, ['path'])
    detail = firstInput(st, ['pattern']) + (path ? ` in ${path}` : '')
  } else if (/grep|find|search/.test(tool)) {
    const inc = firstInput(st, ['include', 'file_pattern'])
    const path = firstInput(st, ['path'])
    detail =
      firstInput(st, ['pattern', 'query', 'regex']) +
      (inc ? ` (${inc})` : '') +
      (path ? ` in ${path}` : '')
  } else if (/read|view|cat|open|list|ls|tree/.test(tool)) {
    detail = firstInput(st, ['filePath', 'file_path', 'path'])
  } else if (/edit|write|patch|save|multiedit/.test(tool)) {
    detail = firstInput(st, ['filePath', 'file_path', 'path'])
  } else if (/fetch|web|http/.test(tool)) {
    detail = firstInput(st, ['url', 'link'])
  } else if (/task|agent|subagent/.test(tool)) {
    detail = firstInput(st, ['description', 'prompt'])
  } else if (QUESTION_RE.test(tool)) {
    const qs = st.input?.questions
    if (Array.isArray(qs) && qs.length) {
      const q = qs[0]
      detail = (q.header ? q.header + ': ' : '') + (q.question ?? '')
    }
  } else if (/skill/.test(tool)) {
    const name = firstInput(st, ['name'])
    detail = skillsIndex[name] || name
  } else if (/todo/.test(tool)) {
    const n = Array.isArray(st.input?.todos) ? st.input.todos.length : 0
    if (n) detail = `${n} todo${n === 1 ? '' : 's'}`
  }
  if (!detail) detail = typeof st.title === 'string' && st.title.trim() ? st.title.trim() : ''
  if (!detail) {
    const inp = st?.input ?? {}
    const v = Object.values(inp).find((x) => typeof x === 'string' && x.trim())
    if (v) detail = v
  }
  return clip(detail)
}

function j(p, opts = {}) {
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

let scratch, agentDir, child, log = ''
function boot() {
  scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'tooltitlecheck-'))
  agentDir = path.join(scratch, 'pi-agent')
  fs.mkdirSync(agentDir, { recursive: true })
  child = spawn(process.execPath, [BRIDGE, '--port', String(PORT), '--dir', agentDir, '--cwd', scratch], {
    cwd: WEB,
    env: {
      ...process.env,
      PATH: `${TESTBIN}:${process.env.PATH}`,
      PI_CODING_AGENT_DIR: agentDir,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  child.stdout.on('data', (d) => (log += d))
  child.stderr.on('data', (d) => (log += d))
}
const stop = () => {
  try {
    child?.kill('SIGKILL') // exact PID only — never pattern-kill
  } catch {
    /* already gone */
  }
}

// what the fake pi scripts, and the detail line the UI must end up showing
const SCRIPT = [
  { tool: 'bash', final: 'echo hi' },
  { tool: 'read', final: '/tmp/x.txt' },
  { tool: 'edit', final: '/tmp/x.txt' },
]

let client, rawFrames = [], teeFrames = [], L1 = [], timeline = []
const isTool = (f) => f.type === 'message.part.updated' && f.properties?.part?.type === 'tool'
const framesFor = (tool) => L1.filter((f) => isTool(f) && f.properties.part.tool === tool)

// Two taps on the SAME event stream, both connected BEFORE the turn:
//   http.get             — an independent SSE reader (the raw wire)
//   the UI's own stream  — wraps the EventSource the webui client opens, so it
//                          sees exactly the bytes a browser tab would see
// The layer-1 assertions read whichever tap actually saw the turn, and both
// counts are printed: a deaf tap can never make an assertion vacuous.
function installTee() {
  const Base = globalThis.EventSource
  globalThis.EventSource = class Tee extends Base {
    constructor(url) {
      super(url)
      let inner = null
      Object.defineProperty(this, 'onmessage', {
        configurable: true,
        get: () => (ev) => {
          try {
            teeFrames.push(JSON.parse(ev.data))
          } catch {
            /* ignore */
          }
          inner?.(ev)
        },
        set: (fn) => {
          inner = fn
        },
      })
    }
  }
  return () => {
    globalThis.EventSource = Base
  }
}

function collectFrames(probeMs = 90000) {
  // 90s, not a "generous" 14s: the FIRST turn in a scratch agent dir pays a pi
  // child spawn, and a timer shorter than spawn+turn destroys the tap mid-turn.
  // That looks exactly like a bridge that emits nothing — a deaf probe, not a
  // deaf bridge. The probe is destroyed explicitly at the end of the case.
  rawFrames = []
  teeFrames = []
  const done = new Promise((resolve) => {
    const req = http.get(`${BASE}/oc/event`, (res) => {
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
            rawFrames.push(JSON.parse(d.slice(6)))
          } catch {
            /* ignore */
          }
        }
      })
      req.on('close', () => resolve(true))
      req.on('error', () => resolve(false))
    })
    const timer = setTimeout(() => req.destroy(), probeMs)
    timer.unref?.() // do not hold the process open; the case destroys it
    globalThis.__frameProbe = req
    globalThis.__frameTimer = timer
  })
  return done
}


async function main() {
  // Layer 2/3 need the sibling webui checkout (the real client store). Layer 1 —
  // the bridge's own frames — does not, and that is the layer where this bug
  // lives. Without the checkout the check runs layer 1 and prints a loud SKIP
  // for the rest; it never passes a layer it did not run.
  const hasUI = !!client
  const sse = client?.sse
  const tabs = client?.tabs
  const ocGet = async (p) => (await j(p)).json
  const created = await j('/oc/session', { method: 'POST', body: { cwd: scratch, title: 'tooltitle-check' } })
  const sid = created.json?.id
  check(!!sid, `session created (${created.status})`)
  const info = hasUI ? await client.oc.session(sid) : await ocGet(`/oc/session/${sid}`)
  let restoreES = null,
    unsub = null
  if (hasUI) {
    tabs.open({ id: sid, title: info?.title || sid.slice(0, 14), messages: [], live: true })
    restoreES = installTee() // BEFORE startEvents: sse.ts reads the global
    sse.startEvents()
  }

  // every store notify, sampled: one row per (part, status, detail) change
  timeline = []
  const sample = () => {
    const t = tabs?.snapshot(sid)
    if (!t) return
    for (const m of t.messages ?? [])
      for (const p of m.parts ?? []) {
        if (p.type !== 'tool') continue
        const row = { tool: p.tool, st: p.state?.status, detail: toolDetail(p), callID: p.callID }
        const prev = timeline.at(-1)
        if (prev && prev.tool === row.tool && prev.st === row.st && prev.detail === row.detail) continue
        timeline.push(row)
      }
  }
  if (hasUI) unsub = tabs.subscribe(sample)
  const probe = collectFrames()
  await sleep(250) // both taps live BEFORE the turn, or frames are simply missed

  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'stream me' }] } })
  restoreES?.()
  if (hasUI) {
    for (let i = 0; i < 120 && tabs.snapshot(sid)?.busy; i++) await sleep(100)
  } else {
    for (let i = 0; i < 120; i++) {
      const st = await j('/oc/session/status')
      if (st.json?.[sid]?.type === 'idle') break
      await sleep(100)
    }
  }
  await sleep(1200) // session.idle -> scheduleRefetch -> GET /message

  // ---- layer 1: the bridge's own frames ----------------------------------
  // Premise: the probe is actually connected. Without this, every layer-1
  // assertion below is vacuously true (a missed stream looks like a clean one).
  const src = rawFrames.length >= teeFrames.length ? rawFrames : teeFrames
  L1 = src
  const typeTally = {}
  for (const f of src) typeTally[f.type] = (typeTally[f.type] || 0) + 1
  check(
    src.length > 20,
    `a tap saw the turn (http.get ${rawFrames.length}, ui-connection tee ${teeFrames.length}; ` +
      `${Object.entries(typeTally).map(([k, v]) => `${k}=${v}`).join(' ')})`,
  )

  for (const { tool, final } of SCRIPT) {
    const fr = framesFor(tool).map((f) => {
      const p = f.properties.part
      return { st: p.state?.status, detail: toolDetail(p), title: p.state?.title, callID: p.callID }
    })
    check(fr.length >= 3, `${tool}: bridge sent ${fr.length} frames for the part`)
    if (!fr.length) continue
    check(
      fr.at(-1).detail !== '',
      `${tool}: the LAST live frame carries a detail line (it was ${JSON.stringify(fr.at(-1).detail)} — ` +
        `title ${JSON.stringify(fr.at(-1).title)}; this is the state the tab keeps)`,
    )
    const running = fr.filter((f) => f.st === 'running')
    check(
      running.some((f) => f.detail !== ''),
      `${tool}: a detail line exists WHILE status is running (${running.length} running frame(s), ` +
        `details: ${JSON.stringify(running.map((f) => f.detail))})`,
    )
    // A frame whose detail is a STRICT PREFIX of the final one can only come
    // from toolcall_delta: every other event either has the whole arguments
    // object or none. This is the "populate it when the call is written, before
    // it executes" half of the request.
    const partial = fr.filter((f) => f.detail && f.detail !== final && final.startsWith(f.detail))
    check(partial.length > 0, `${tool}: arguments stream produced a partial detail line (${JSON.stringify(partial.map((f) => f.detail))})`)
    // the regression itself: a populated state replaced by a blank one
    const first = fr.findIndex((f) => f.detail !== '')
    const blanks = first >= 0 ? fr.slice(first).filter((f) => f.detail === '') : fr
    check(first >= 0 && blanks.length === 0, `${tool}: never goes blank after it is populated (${blanks.length} blank frame(s) after sample #${first})`)
    const noCall = fr.filter((f) => !f.callID)
    check(noCall.length === 0, `${tool}: callID survives every frame (${noCall.length} missing)`)
  }

  // ---- layer 2: the real webui store -------------------------------------
  if (!hasUI) {
    console.log('SKIP layer 2 (webui client store) + layer 3 (tab store): ' + (skipUI || 'no client'))
    console.log('  layer 1 adjudicated the bridge contract; the store merge is untested in this run')
  }
  for (const { tool, final } of hasUI ? SCRIPT : []) {
    const rows = timeline.filter((r) => r.tool === tool)
    check(rows.length >= 2, `${tool}: ${rows.length} store sample(s)`)
    if (!rows.length) continue
    const firstOk = rows.findIndex((r) => r.detail !== '')
    const doneAt = rows.findIndex((r) => r.st === 'completed')
    check(
      firstOk >= 0 && (doneAt < 0 || firstOk < doneAt),
      `${tool}: the store held a detail line BEFORE the part reached completed (first ok #${firstOk}, completed #${doneAt})`,
    )
    const lost = firstOk >= 0 ? rows.slice(firstOk).filter((r) => r.detail === '') : rows
    check(firstOk >= 0 && lost.length === 0, `${tool}: the store never lost it (${lost.length} blank after)`)
  }

  // ---- layer 3: after the turn, and cold ---------------------------------
  const after = tabs?.snapshot(sid)
  const partsAfter = (after?.messages ?? []).flatMap((m) => (m.parts ?? []).filter((p) => p.type === 'tool'))
  for (const { tool, final } of hasUI ? SCRIPT : []) {
    const p = partsAfter.find((x) => x.tool === tool)
    check(p && toolDetail(p) === final, `${tool}: after the turn + refetch the line reads ${JSON.stringify(p ? toolDetail(p) : null)} (want ${JSON.stringify(final)})`)
  }
  const cold = hasUI ? await client.oc.messages(sid) : await ocGet(`/oc/session/${sid}/message`)
  const coldParts = (cold ?? []).flatMap((m) => (m.parts ?? []).filter((p) => p.type === 'tool'))
  check(
    coldParts.length === 3 && coldParts.every((p) => toolDetail(p)),
    `baseline: a cold GET /message has all 3 detail lines (${coldParts.map((p) => JSON.stringify(toolDetail(p))).join(' ')}) — ` +
      `the read path was never the bug`,
  )

  // ---- frame budget: a title that streams must not become a firehose ------
  const toolFrames = L1.filter(isTool).length
  check(
    toolFrames <= 3 * 10,
    `frame budget: ${toolFrames} tool part.updated frames for 3 calls (want <=30) — streamed titles stay throttled`,
  )
  console.log(
    '  frames:',
    JSON.stringify(SCRIPT.map(({ tool }) => `${tool}=${framesFor(tool).length}`)),
    ' store samples:',
    timeline.length,
  )
  console.log('  timeline:', JSON.stringify(timeline))
  unsub?.()
  globalThis.__frameProbe?.destroy()
  clearTimeout(globalThis.__frameTimer)
  await probe
}

// The adversarial argument shape: a `write` whose blob (~76 KB) arrives BEFORE
// the path, streamed in ~400 `toolcall_delta` records. Two failure modes this
// catches: (a) a bridge that scans/emits per token turns one write into hundreds
// of part frames; (b) a scan that gives up past its cap and INVENTS a title out
// of a truncated document. The correct answer is: name it when the complete call
// arrives, and spend no frames before that.
async function caseBigArgs() {
  const created = await j('/oc/session', { method: 'POST', body: { cwd: scratch, title: 'big-args' } })
  const sid = created.json?.id
  check(!!sid, `big-args session created (${created.status})`)
  const probe = collectFrames()
  await sleep(250)
  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: '/fake-bigwrite' }] } })
  for (let i = 0; i < 200; i++) {
    const st = await j('/oc/session/status')
    if (st.json?.[sid]?.type === 'idle') break
    await sleep(100)
  }
  await sleep(500)
  L1 = rawFrames.length >= teeFrames.length ? rawFrames : teeFrames
  const fr = framesFor('write').map((f) => ({ st: f.properties.part.state?.status, detail: toolDetail(f.properties.part) }))
  check(fr.length > 0, `write: the part reached the stream (${fr.length} frame(s), one per state change, no per-delta storm)`)
  check(
    fr.length > 0 && fr.at(-1).detail === '/tmp/big.txt',
    `write: it is named from the COMPLETE call (${JSON.stringify(fr.at(-1)?.detail)}), not from the capped scan`,
  )
  check(fr.length <= 6, `write: ~400 argument deltas cost ${fr.length} part frames (want <=6) — windowed, not per token`)
  const invented = fr.filter((f) => f.detail && f.detail !== '/tmp/big.txt')
  check(invented.length === 0, `write: no title invented from a truncated document (${JSON.stringify(invented.map((f) => f.detail))})`)
  globalThis.__frameProbe?.destroy()
  clearTimeout(globalThis.__frameTimer)
  await probe
}

let skipUI = ''
try {
  client = await loadWebuiClient(BASE)
} catch (e) {
  skipUI = webuiSkip(e) ?? `client load error: ${e?.stack ?? e}`
  client = null
  console.log(skipUI)
  console.log('  → running layer 1 (the bridge frames) only')
}

await boot()
for (let i = 0; i < 60; i++) {
  try {
    if ((await j('/healthz')).status === 200) break
  } catch {
    /* not up yet */
  }
  await sleep(200)
}
for (const c of [main, caseBigArgs]) {
  console.log(`\n===== ${c.name} =====`)
  try {
    await c()
  } catch (e) {
    console.log('ERROR', e?.stack ?? String(e))
    failed++
  }
}
stop()
await sleep(250)
if (scratch) fs.rmSync(scratch, { recursive: true, force: true })
if (failed) console.log('--- server log ---\n' + log.slice(-2500))
console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
process.exit(failed ? 1 : 0)
