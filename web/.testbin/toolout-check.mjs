/**
 * The webui draws an output box under EVERY tool row — the block used to be
 * gated on the tool NAME (/bash|shell|cmd/, /grep|find|search/), which is why a
 * result from any other tool (a search/memory/index tool, say) streamed to the
 * browser and then went invisible. That gate is gone, so the whole contract now
 * lives here: the bridge must carry the result TEXT for any tool, live and cold,
 * including a name the bridge has never heard of.
 *
 * Why assert it at the bridge at all, when nothing is broken there? Because the
 * webui upserts parts with a shallow spread (`parts[i] = {...parts[i], ...part}`),
 * so ONE later frame with `output: ''` blanks a row the user was reading. That
 * exact shape of bug already happened to `title`/`input` at tool_execution_end
 * ("the row goes blank when the tool finishes"), and a UI test against a fixture
 * JSON cannot see it — only a tap on the live wire can.
 *
 * Self-contained: scratch dir, fake pi with FAKE_PI_EXTRA_TOOL=<unknown tool
 * name>, own child on TOOL_OUT_PORT (default 8135 — not 8126/8127/8129/8130, so
 * it can run beside the other checks), killed by exact PID. No sibling checkout
 * needed: this check never skips.
 *
 *   node .testbin/toolout-check.mjs
 *   PI_WEB_JS=/path/to/pre-fix/pi-web.mjs node .testbin/toolout-check.mjs   # prove it bites
 */
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { WEB } from './harness.mjs'

const PORT = Number(process.env.TOOL_OUT_PORT || 8135)
const BASE = `http://127.0.0.1:${PORT}`
const PI_JS = process.env.PI_WEB_JS || path.join(WEB, 'pi-web.mjs')
const EXTRA_TOOL = 'notepad_demo' // unknown to the bridge AND to the webui's old name regexes
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ` — ${note}` : ''))
  if (!ok) failed++
}

// Expected result text per tool — MUST match .testbin/fake-pi.mjs's scripted
// turn (that file is an executable fake, so these are copied, not imported).
const EXPECT = {
  bash: 'hi\n',
  read: 'file contents\n',
  edit: 'Successfully replaced 1 block(s) in /tmp/x.txt.',
  [EXTRA_TOOL]: '#11 [fact/demo] (1.42) first matched line\n#12 [rule/demo] (1.10) second matched line',
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
  scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'tooloutcheck-'))
  agentDir = path.join(scratch, 'pi-agent')
  fs.mkdirSync(agentDir, { recursive: true })
  child = spawn(process.execPath, [PI_JS, '--port', String(PORT), '--dir', agentDir, '--cwd', scratch], {
    cwd: WEB,
    env: {
      ...process.env,
      PATH: `${path.join(WEB, '.testbin')}:${process.env.PATH}`,
      PI_CODING_AGENT_DIR: agentDir,
      FAKE_PI_EXTRA_TOOL: EXTRA_TOOL,
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

const isTool = (f) => f.type === 'message.part.updated' && f.properties?.part?.type === 'tool'

function collectFrames(probeMs = 90000) {
  // Long timer on purpose: the FIRST turn in a scratch dir pays a pi child
  // spawn, and a tap that dies mid-turn reads as a bridge that emits nothing.
  const frames = []
  let req = null
  const stop = () => {
    try {
      req?.destroy()
    } catch {
      /* already closed */
    }
  }
  const done = new Promise((resolve) => {
    req = http.get(`${BASE}/oc/event`, (res) => {
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
            frames.push(JSON.parse(d.slice(6)))
          } catch {
            /* ignore */
          }
        }
      })
      req.on('close', () => resolve(true))
    })
    req.on('error', () => resolve(false))
    setTimeout(() => {
      try {
        req.destroy()
      } catch {
        /* already closed */
      }
    }, probeMs)
  })
  return { frames, done, stop }
}

try {
  boot()
  let up = false
  for (let i = 0; i < 80 && !up; i++) {
    try {
      const r = await j('/oc/session/status')
      up = r.status > 0
    } catch {
      /* not listening yet */
    }
    if (!up) await sleep(250)
  }
  check(up, 'bridge is up')

  const created = await j('/oc/session', { method: 'POST', body: { cwd: scratch, title: 'toolout-check' } })
  const sid = created.json?.id
  check(!!sid, `session created (${created.status})`)

  const probe = collectFrames()
  await sleep(250) // the tap must exist BEFORE the turn
  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'run the tools' }] } })
  for (let i = 0; i < 160; i++) {
    const st = await j('/oc/session/status')
    if (st.json?.[sid]?.type === 'idle') break
    await sleep(100)
  }
  await sleep(1500) // let the post-idle refetch land too, then read the wire
  probe.stop() // close the tap explicitly, or its socket keeps the event loop alive
  await probe.done
  const frames = probe.frames.filter(isTool)
  const tally = {}
  for (const f of probe.frames) tally[f.type] = (tally[f.type] || 0) + 1
  check(
    probe.frames.length > 20 && frames.length >= 4,
    `the tap saw the turn (${probe.frames.length} frames; tool parts ${frames.length}; ${Object.entries(tally)
      .map(([k, v]) => `${k}=${v}`)
      .join(' ')})`,
  )

  const byTool = {}
  for (const f of frames) {
    const p = f.properties.part
    ;(byTool[p.tool] ??= []).push(p)
  }

  for (const [tool, want] of Object.entries(EXPECT)) {
    const parts = byTool[tool] ?? []
    check(parts.length >= 1, `${tool}: bridge emitted ${parts.length} frame(s) for the part`)
    if (!parts.length) continue
    const last = parts.at(-1)
    check(last.state?.status === 'completed', `${tool}: final live frame is completed`, `got ${last.state?.status}`)
    check(last.state?.output === want, `${tool}: final live frame carries the FULL result text`, JSON.stringify(last.state?.output))
    // the #472 shape: a populated field blanked by a later frame. Assert EVERY
    // completed frame, not just the last one — the `toolResult` message_end
    // re-complete fires AFTER tool_execution_end and heals a blanked live frame,
    // so watching only the final frame lets a live-path regression through.
    const blankCompleted = parts.filter((p) => p.state?.status === 'completed' && !p.state?.output)
    check(
      blankCompleted.length === 0,
      `${tool}: no completed frame carries an empty output`,
      blankCompleted.length ? `${blankCompleted.length}/${parts.length} frame(s) had output="" — the tab keeps that state until the refetch heals it` : '',
    )
  }

  // args stream for an unknown name too (the row header and the webui's detail
  // rows come from state.input)
  const xp = (byTool[EXTRA_TOOL] ?? []).at(-1)
  check(xp?.state?.input?.action === 'search' && xp?.state?.input?.query === 'token cost', 'unknown tool: its arguments reached the browser', JSON.stringify(xp?.state?.input))

  // ---- cold path: a reload must reproduce the same text from the JSONL -----
  const hist = await j(`/oc/session/${sid}/message`)
  const cold = []
  for (const m of hist.json ?? []) for (const p of m.parts ?? []) if (p.type === 'tool') cold.push(p)
  const cx = cold.find((p) => p.tool === EXTRA_TOOL)
  check(cx?.state?.output === EXPECT[EXTRA_TOOL], 'cold GET /message reproduces the unknown tool output verbatim', JSON.stringify(cx?.state?.output))
  check(cold.filter((p) => typeof p.state?.output === 'string' && p.state.output).length >= 4, 'cold path carries output for every tool part', `${cold.length} parts`)

  if (failed) console.log('\n--- child log (tail) ---\n' + log.split('\n').slice(-25).join('\n'))
} finally {
  stop()
}

console.log(failed ? `\nFAILED: ${failed}` : '\nALL OK')
process.exitCode = failed ? 1 : 0
