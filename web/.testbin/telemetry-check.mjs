/**
 * Engine telemetry check — the contract the "thinking" box and the compaction
 * box render from (M1 + M4).
 *
 * The bridge probes the engine (Strata/llama.cpp) and folds what it learns into
 * the SAME two surfaces the webui already trusts:
 *   · SSE `session.status` with status.type === 'engine'
 *   · GET /oc/session/status entry key `engine`  (the webui re-derives live
 *     state from THAT poll every reconnect + 10s tick — a value that exists only
 *     in one dropped SSE frame gets cleared by the reconcile, which is exactly
 *     how the Compacting strip used to die mid-summary)
 *
 * What is asserted, in order of how it can lie:
 *   1. RATE. The fake engine counts its own requests: poll while busy, and be
 *      SILENT while idle. A 1 Hz poller that never stops is a permanent load on
 *      the box's own model.
 *   2. ATTRIBUTION. One engine slot means the engine cannot say which session
 *      generated what (requests[].projection is null). Two busy sessions ⇒ the
 *      bridge must refuse to attach numbers (ambiguous), not guess.
 *   3. DEGRADATION. No engine / a 404 engine / a wedged engine: the turn must
 *      still work, the frame must fall back to `source:'derived'`, and the probe
 *      must back off instead of hammering a slot that never answers.
 *   4. COMPACTION PROGRESS (M4). While pi writes a summary, the status entry
 *      must carry compaction.state 'running' AND attributed engine numbers —
 *      pi emits nothing between compaction_start and compaction_end, so the
 *      engine is the ONLY progress source, and it knows it is a summary only
 *      because the bridge recorded `compacting`.
 *
 * Ports: bridge on CHECK_PORT (8126), fake engine on ENGINE_PORT (8127). The
 * engine is IN-PROCESS (imported), so there is no second PID to kill.
 *
 *   node .testbin/telemetry-check.mjs
 */
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { startEngine } from './fake-engine.mjs'
import { ENGINE_PORT } from './harness.mjs'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const WEB = path.resolve(HERE, '..')
const PORT = Number(process.env.CHECK_PORT || 8126)
const BASE = `http://127.0.0.1:${PORT}`
const ENGINE_BASE = `http://127.0.0.1:${ENGINE_PORT}`

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

const ofType = (t) => events.filter((e) => e.type === t)
// emitFor stamps sessionID on the properties; a status frame's payload is
// `properties.status`. Match on the FRAME's session id, not the payload's.
const engineFor = (sid) =>
  ofType('session.status')
    .filter((e) => (e.properties?.sessionID ?? e.properties?.status?.sessionID) === sid)
    .map((e) => e.properties?.status ?? {})
    .filter((s) => s.type === 'engine')

// A scratch agent dir with a models-store.json whose ONLY provider points at
// the fake engine: `storeProviders()` derives routerBase from baseUrl minus /v1,
// which is the discovery path the probe uses — no hard-coded host anywhere.
function writeStore(agent, { baseUrl = `${ENGINE_BASE}/v1` } = {}) {
  fs.writeFileSync(
    path.join(agent, 'models-store.json'),
    JSON.stringify({
      testprov: {
        models: [{ id: 'fake-1', name: 'fake-1', baseUrl, contextWindow: 256000, maxTokens: 8192, reasoning: true }],
      },
    }),
  )
}

let ENG = null
async function boot(env, opts = {}) {
  const SCRATCH = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetrycheck-'))
  const AGENT = path.join(SCRATCH, 'pi-agent')
  fs.mkdirSync(AGENT, { recursive: true })
  if (!opts.noStore) writeStore(AGENT, opts.store || {})
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
      child.kill('SIGKILL') // exact PID only — never pattern-kill
    } catch {
      /* already gone */
    }
  }
  return { SCRATCH, AGENT, stop, get log() { return log } }
}

const newSession = async (title, model = { providerID: 'testprov', id: 'fake-1' }) => {
  const r = await req('/oc/session', { method: 'POST', body: { cwd: process.env.__SCRATCH, title, ...(model ? { model } : {}) } })
  return r.json?.id
}
const prompt = (sid, text = 'stream me') =>
  req(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text }] } })
const statusMap = async () => (await req('/oc/session/status')).json || {}
const summarize = (sid, customInstructions) =>
  req(`/oc/session/${sid}/summarize`, {
    method: 'POST',
    body: { providerID: 'testprov', modelID: 'fake-1', ...(customInstructions ? { customInstructions } : {}) },
  })

async function until(fn, ms = 6000, step = 100) {
  for (let i = 0; i < Math.ceil(ms / step); i++) {
    const v = await fn()
    if (v) return v
    await sleep(step)
  }
  return null
}

function caseEngineAttributed() {
  return {
    title: 'caseEngineAttributed',
    run: async () => {
      ENG = startEngine({ port: ENGINE_PORT, prefillMs: 700, decodeMs: 4000, promptTotal: 42000 })
      const b = await boot({ FAKE_DELTA_MS: '300' })
      process.env.__SCRATCH = b.SCRATCH
      try {
        const sid = await newSession('telemetry-one')
        ENG.run()
        await prompt(sid)
        const hits0 = ENG.hits.status

        // Mid-summary/mid-turn: the webui's authoritative poll must carry the
        // same block SSE does, or the 10s reconcile blanks the box.
        const st = await until(async () => {
          const m = await statusMap()
          return m[sid]?.engine?.source === 'engine' ? m[sid].engine : null
        }, 5000)
        check(!!st, `GET /session/status carries the SAME engine block (${JSON.stringify(st)})`)
        check((await statusMap())[sid]?.type === 'busy', 'and the session reads busy while it generates')

        // Then measure CADENCE across the whole turn, not at the first frame.
        const doneIdle = await until(async () => (await statusMap())[sid]?.type === 'idle', 15000)
        check(!!doneIdle, 'the turn completed')
        const polled = ENG.hits.status - hits0
        // ~4 s turn at 1 Hz ⇒ a handful of polls: enough to prove it polled, few
        // enough to prove it is not hammering the box's own model.
        check(polled >= 2, `the bridge POLLED while busy (${polled} GET /status)`)
        check(polled <= 12, `…at its stated rate, not per-token (${polled} polls)`)
        check(ENG.hits.metrics >= 1, `it also read /metrics for live progress (${ENG.hits.metrics})`)
        // the turn must actually have been observed, or every frame assertion
        // below is vacuously true (graph: a short tap made a deaf probe look
        // like a bridge that emits nothing)
        check(events.length > 20, `PREMISE: the SSE tap saw the turn (${events.length} frames)`)

        const frames = engineFor(sid)
        const eng = frames[0] ?? {}
        check(eng.source === 'engine', `the frame says where it came from (${JSON.stringify(eng.source)})`)
        check(['thinking', 'answering'].includes(eng.phase), `and pi/the engine's phase (${JSON.stringify(eng.phase)})`)
        check(frames.some((f) => f.prompt_total === 42000), `prompt_total for the prefill bar (${eng.prompt_total})`)
        check(
          frames.some((f) => Number(f.prompt_read) > 0),
          `prompt_read advanced (${JSON.stringify(frames.map((f) => f.prompt_read).slice(0, 6))})`,
        )
        check(
          frames.some((f) => Number(f.generated) > 0),
          `generated advanced during the decode (${JSON.stringify(frames.map((f) => f.generated).slice(0, 6))})`,
        )
        check(
          frames.some((f) => typeof f.tok_s === 'number' && f.tok_s > 0),
          `tok_s present while decoding (${JSON.stringify(frames.map((f) => f.tok_s).slice(0, 6))})`,
        )
        check(
          frames.some((f) => Number(f.reused) > 0 && Number(f.hit_rate) > 0),
          `last-turn reuse is available for the box (${JSON.stringify(frames.map((f) => f.reused).slice(0, 4))})`,
        )

        // RATE: idle means SILENCE. The engine is the agent's own model; a
        // poller that keeps poking it forever is load on the thing it measures.
        await until(async () => (await statusMap())[sid]?.type === 'idle', 8000)
        await sleep(400)
        const idleStart = ENG.hits.status
        await sleep(2500)
        const idleHits = ENG.hits.status - idleStart
        check(idleHits === 0, `ZERO polling while nothing is busy (${idleHits} GET /status in 2.5s)`)
        const m = await statusMap()
        check(!m[sid]?.engine, `and the status entry drops the engine block when idle (${JSON.stringify(m[sid])})`)
      } finally {
        b.stop()
        await ENG.stop()
        ENG = null
        await sleep(250)
        fs.rmSync(b.SCRATCH, { recursive: true, force: true })
        if (failed) console.log('--- server log ---\n' + b.log.slice(-2000))
      }
    },
  }
}

function caseAmbiguous() {
  return {
    title: 'caseAmbiguous',
    run: async () => {
      ENG = startEngine({ port: ENGINE_PORT, prefillMs: 700, decodeMs: 5000, promptTotal: 42000 })
      const b = await boot({ FAKE_DELTA_MS: '300' })
      process.env.__SCRATCH = b.SCRATCH
      try {
        const a = await newSession('telemetry-amb-a')
        const c = await newSession('telemetry-amb-b')
        ENG.run()
        await prompt(a, 'first stream')
        await prompt(c, 'second stream')
        const framesA = !!(await until(() => engineFor(a).length > 0, 6000))
        check(!!framesA, 'two live turns still produced engine frames')
        const fa = engineFor(a)
        const last = fa.at(-1) ?? {}
        check(last.ambiguous === true, `two busy sessions on ONE slot ⇒ ambiguous (${JSON.stringify(last)})`)
        check(
          last.prompt_total == null && last.generated == null,
          `and NO numbers are attached — a mislabelled tok_s is worse than none (${JSON.stringify({ pt: last.prompt_total, g: last.generated })})`,
        )
        const m = await statusMap()
        check(m[a]?.engine?.ambiguous === true, `the status poll agrees for session A (${JSON.stringify(m[a]?.engine)})`)
        check(m[c]?.engine?.ambiguous === true, `and for session B (${JSON.stringify(m[c]?.engine)})`)
      } finally {
        b.stop()
        await ENG.stop()
        ENG = null
        await sleep(250)
        fs.rmSync(b.SCRATCH, { recursive: true, force: true })
        if (failed) console.log('--- server log ---\n' + b.log.slice(-2000))
      }
    },
  }
}

function caseNoEngine() {
  return {
    title: 'caseNoEngine',
    run: async () => {
      // Nothing listening: the provider's routerBase points at a dead port, as
      // it does for a cloud provider with no engine to ask.
      ENG = null
      const b = await boot({ FAKE_DELTA_MS: '120' }, { store: { baseUrl: `http://127.0.0.1:${ENGINE_PORT}/v1` } })
      process.env.__SCRATCH = b.SCRATCH
      try {
        const sid = await newSession('telemetry-dead')
        await prompt(sid)
        const done = await until(async () => (await statusMap())[sid]?.type === 'idle', 9000)
        check(!!done, 'a dead engine never wedges the session — the turn completed')
        const frames = engineFor(sid)
        check(frames.length > 0, `it still reports something for the thinking box (${frames.length} frames)`)
        const d = frames.find((f) => f.source === 'derived') ?? frames.at(-1) ?? {}
        check(d.source === 'derived', `from the DERIVED fallback (${JSON.stringify(d.source)})`)
        check(typeof d.prefill_ms === 'number' && d.prefill_ms >= 0, `prefill estimated from turn_start→first delta (${d.prefill_ms})`)
        check(typeof d.tok_s === 'number' && d.tok_s > 0, `decode rate from delta cadence (${d.tok_s})`)
        check(d.prompt_total == null, `and it does NOT invent a bar total (${JSON.stringify(d.prompt_total)})`)
        check(ENG === null, 'PREMISE: no engine was running')
      } finally {
        b.stop()
        await sleep(250)
        fs.rmSync(b.SCRATCH, { recursive: true, force: true })
        if (failed) console.log('--- server log ---\n' + b.log.slice(-2000))
      }
    },
  }
}

function caseWedgedBackoff() {
  return {
    title: 'caseWedgedBackoff',
    run: async () => {
      // A slot that accepts the connection and never answers: without a timeout
      // the probe would pile up fetches; without back-off it hammers a wedged
      // engine once per second forever.
      ENG = startEngine({ port: ENGINE_PORT, mode: 'hang' })
      const b = await boot({ FAKE_DELTA_MS: '500', PI_WEB_ENGINE_TIMEOUT_MS: '200', PI_WEB_ENGINE_FAIL_LIMIT: '2' })
      process.env.__SCRATCH = b.SCRATCH
      try {
        const sid = await newSession('telemetry-wedged')
        ENG.run()
        await prompt(sid)
        await sleep(2200)
        const a = ENG.hits.status
        check(a >= 2, `the probe tried the wedged engine twice then parked it (${a} attempts)`)
        await sleep(2000)
        const more = ENG.hits.status - a
        const stillBusy = (await statusMap())[sid]?.type === 'busy'
        check(stillBusy, `PREMISE: the turn is still running (${JSON.stringify((await statusMap())[sid])})`)
        check(more === 0, `then stayed parked while still busy (${more} more attempts in 2s)`)
        const done = await until(async () => (await statusMap())[sid]?.type === 'idle', 9000)
        check(!!done, 'the turn still finished normally')
      } finally {
        b.stop()
        await ENG.stop()
        ENG = null
        await sleep(250)
        fs.rmSync(b.SCRATCH, { recursive: true, force: true })
        if (failed) console.log('--- server log ---\n' + b.log.slice(-2000))
      }
    },
  }
}

function caseCompactionProgress() {
  return {
    title: 'caseCompactionProgress',
    run: async () => {
      // M4: pi emits NOTHING between compaction_start and compaction_end, so the
      // box's progress can only come from the engine — and the engine cannot
      // tell a summary from a turn (projection is null). The bridge's own
      // `compacting` flag is what makes the attribution meaningful.
      ENG = startEngine({ port: ENGINE_PORT, prefillMs: 900, decodeMs: 5000, promptTotal: 119802 })
      const b = await boot({ FAKE_PI_COMPACT: 'slow', PI_WEB_SUMMARIZE_WAIT_MS: '300' })
      process.env.__SCRATCH = b.SCRATCH
      try {
        const sid = await newSession('telemetry-compact')
        ENG.run()
        await summarize(sid, 'CHECK_COMPACT_MS=5000')
        const mid = await until(async () => {
          const m = await statusMap()
          const e = m[sid]
          return e?.compaction?.state === 'running' && e?.engine ? e : null
        }, 5000)
        check(!!mid, `mid-summary the status entry has BOTH state and engine (${JSON.stringify(mid)})`)
        check(mid?.engine?.source === 'engine', `progress is engine-truth, not derived (${JSON.stringify(mid?.engine?.source)})`)
        check(
          Number(mid?.engine?.prompt_read) > 0 && Number(mid?.engine?.prompt_total) === 119802,
          `the prefill bar has real numbers (${JSON.stringify({ read: mid?.engine?.prompt_read, total: mid?.engine?.prompt_total })})`,
        )
        check(['thinking', 'answering'].includes(mid?.engine?.phase), `and a phase to show (${JSON.stringify(mid?.engine?.phase)})`)
        const engFrames = await until(
          async () => {
            const f = engineFor(sid).filter((x) => Number(x.generated) > 0)
            return f.length ? f : null
          },
          4000,
        )
        check(!!engFrames, `generated tokens streamed into SSE frames during the summary (${(engFrames ?? []).length})`)
        const r = await until(async () => (await statusMap())[sid]?.compaction?.state === 'ok', 9000)
        check(!!r, 'the summary still lands and the state goes ok')
      } finally {
        b.stop()
        await ENG.stop()
        ENG = null
        await sleep(250)
        fs.rmSync(b.SCRATCH, { recursive: true, force: true })
        if (failed) console.log('--- server log ---\n' + b.log.slice(-2000))
      }
    },
  }
}

const CASES0 = [caseEngineAttributed, caseAmbiguous, caseNoEngine, caseWedgedBackoff, caseCompactionProgress]
// CASE=caseWedgedBackoff iterates one case (each boots its own pi-web + engine,
// so a full run is ~2 min; single-case runs are ~20 s).
const CASES = process.env.CASE ? CASES0.filter((f) => f.name === process.env.CASE) : CASES0
if (!CASES.length) {
  console.log(`no case named ${process.env.CASE}; one of ${CASES0.map((f) => f.name).join(', ')}`)
  process.exit(2)
}
for (const make of CASES) {
  const c = make()
  console.log(`\n===== ${c.title} =====`)
  try {
    await c.run()
  } catch (e) {
    console.log('ERROR', e?.stack ?? e)
    failed++
    if (ENG) {
      await ENG.stop()
      ENG = null
    }
  }
  await sleep(300)
}
console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
process.exit(failed ? 1 : 0)
