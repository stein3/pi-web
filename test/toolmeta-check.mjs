/**
 * The reported symptom: "I have to refresh my browser to load tool metadata in
 * the transcript" — i.e. the inline diff block is missing while streaming and
 * appears after a manual reload. Adjudicated where it can be seen:
 *
 *   layer 1  the bridge: the LAST frame for a tool part must carry the patch
 *            (diff-check.mjs covers this; kept here as the premise for layer 2)
 *   layer 2  the REAL webui client store (webui-client.mjs runs sse.ts +
 *            stores.ts + api.ts in node): the patch must be present in the tab
 *            store at every point of the turn — mid-turn, after the post-idle
 *            refetch, and after a cold refetch — and must never be *lost* by a
 *            later frame. A store that holds it but a UI that needs a reload is
 *            a component bug; a store that loses it is a merge/refetch bug.
 *   layer 3  the UI predicate: Transcript.svelte draws the diff block from
 *            patchTextOf(p) (copied verbatim below).
 *
 * Self-contained: seeds fixtures in a scratch dir, spawns pi-web on CHECK_PORT
 * (default 8129 — deliberately NOT 8127, so this can run alongside the other
 * checks) with the fake pi, and kills ONLY its own child by exact PID.
 * Needs the sibling webui checkout → SKIP (exit 2), like client-check.
 *
 *   node test/toolmeta-check.mjs
 *   PI_WEB_JS=/app/pi-web.mjs node test/toolmeta-check.mjs   # A/B a build
 */
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { BRIDGE, TESTBIN, WEB } from './harness.mjs'
import { loadWebuiClient, webuiSkip } from './webui-client.mjs'

const PORT = Number(process.env.CHECK_PORT || 8129)
const BASE = `http://127.0.0.1:${PORT}`
const EDIT_PATH = '/tmp/x.txt' // the fake pi's scripted edit target
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}

// Transcript.svelte patchTextOf() — verbatim; this IS the contract the diff
// block renders from, so asserting anything else would assert a UI that does
// not exist.
function patchTextOf(p) {
  const md = p?.state?.metadata ?? {}
  return (
    (typeof md.filediff?.patch === 'string' && md.filediff.patch) ||
    (typeof md.patch === 'string' && md.patch) ||
    (typeof md.filediff?.diff === 'string' && md.filediff.diff) ||
    (typeof md.diff === 'string' && md.diff) ||
    ''
  )
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
// Mid-turn frame loss WITHOUT a reconnect: the client still believes the stream
// is OPEN (readyState 1; the bridge's 10s heartbeats keep its `lastFrame` the
// only thing that would trip the watchdog), so catchUp() — the ONLY reconcile of
// busy flags — never runs. GAP drops frames at the socket boundary, exactly as
// a half-open proxy pipe does.
let GAP = false
function installGapEventSource() {
  const Base = globalThis.EventSource
  class GapES extends Base {
    set onmessage(fn) {
      this.__fn = fn
    }
    get onmessage() {
      return GAP ? undefined : this.__fn
    }
  }
  globalThis.EventSource = GapES
}
async function boot() {
  scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'toolmetacheck-'))
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
  for (let i = 0; i < 60; i++) {
    try {
      if ((await j('/healthz')).status === 200) break
    } catch {
      /* not up yet */
    }
    await sleep(200)
  }
}
const stop = () => {
  try {
    child?.kill('SIGKILL') // exact PID only — never pattern-kill
  } catch {
    /* already gone */
  }
}

// the fake pi's edit tool part, wherever it lives
const editPart = (msgs) =>
  msgs
    .flatMap((m) => (m.parts ?? []).map((p) => ({ mid: m.id, ...p })))
    .filter((p) => p.type === 'tool' && /edit/.test(String(p.tool ?? '')) && p.state?.input?.path === EDIT_PATH)

async function main() {
  const { sse, tabs, oc } = client
  const created = await j('/oc/session', { method: 'POST', body: { cwd: scratch, title: 'toolmeta-check' } })
  const sid = created.json?.id
  check(!!sid, `session created (${created.status})`)

  // open the tab exactly the way the UI does, then subscribe BEFORE the turn
  const info = await oc.session(sid)
  tabs.open({ id: sid, title: info?.title || sid.slice(0, 14), messages: [], live: true })
  sse.startEvents()

  // every store notify, sampled: (status, hasPatch). The failure mode the user
  // reports is a part whose patch appears then disappears.
  const timeline = []
  const sample = () => {
    const t = tabs.snapshot(sid)
    if (!t) return
    for (const p of editPart(t.messages))
      timeline.push({ st: p.state?.status, patch: !!patchTextOf(p), len: String(patchTextOf(p) || '').length, id: p.id })
  }
  const unsub = tabs.subscribe(sample)

  // layer 1 premise: the bridge's last frame for that part carries the patch.
  // The collector must be running WHILE the turn runs, so start it, prompt,
  // then let it close on its own timer.
  const rawFrames = []
  const probe = new Promise((resolve) => {
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
    // keep it open past the turn; closed explicitly at the end of the check
    setTimeout(() => req.destroy(), 12000).unref?.()
    globalThis.__frameProbe = req
  })

  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'stream me' }] } })
  await sleep(400)
  const mid = tabs.snapshot(sid)
  const midParts = editPart(mid?.messages ?? [])
  check(midParts.length >= 1, `the edit part exists in the tab mid-turn (${midParts.length})`)

  for (let i = 0; i < 100 && tabs.snapshot(sid)?.busy; i++) await sleep(100)
  await sleep(800) // session.idle -> scheduleRefetch -> GET /message
  const after = tabs.snapshot(sid)
  const afterParts = editPart(after?.messages ?? [])
  check(afterParts.length === 1, `exactly ONE edit part after the turn + refetch (${afterParts.length})`)
  check(afterParts.length > 0 && afterParts[0].state?.status === 'completed', 'its state is completed')
  check(
    afterParts.length > 0 && !!patchTextOf(afterParts[0]),
    `the patch is in the TAB STORE after the post-idle refetch — this is what the UI renders ` +
      `(len=${String(patchTextOf(afterParts[0] || {})).length})`,
  )

  // layer 1, from frames actually received
  const framesForPart = rawFrames
    .filter((f) => f.type === 'message.part.updated' && f.properties?.part?.type === 'tool' && /edit/.test(String(f.properties?.part?.tool ?? '')))
    .map((f) => ({ st: f.properties.part.state?.status, patch: !!patchTextOf(f.properties.part) }))
  check(framesForPart.length > 0, `bridge sent ${framesForPart.length} tool frames for the edit part`)
  check(
    framesForPart.length > 0 && framesForPart.at(-1).patch,
    'the LAST bridge frame for that part carries the patch (diff-check premise)',
  )

  // the store never lost it after it first appeared
  const firstPatch = timeline.findIndex((s) => s.patch)
  check(firstPatch >= 0, `the patch entered the store at sample #${firstPatch} of ${timeline.length}`)
  const lost = timeline.slice(firstPatch).filter((s) => !s.patch)
  check(firstPatch >= 0 && lost.length === 0, `it was never dropped afterwards (${lost.length} drop(s) after it appeared)`)
  console.log('  timeline:', JSON.stringify(timeline))

  // a manual refresh, exactly as a reload does it: GET /message then apply
  const cold = await oc.messages(sid)
  const coldParts = editPart(cold ?? [])
  check(
    coldParts.length > 0 && !!patchTextOf(coldParts[0]),
    `a cold GET /message carries it (len=${String(patchTextOf(coldParts[0] || {})).length})`,
  )
  sse.applyMessages(sid, cold, true)
  const refreshed = editPart(tabs.snapshot(sid)?.messages ?? [])
  check(
    refreshed.length === 1 && !!patchTextOf(refreshed[0]),
    `and one part with the patch after applyMessages(replace) — the reload path (${refreshed.length})`,
  )
  unsub()
  globalThis.__frameProbe?.destroy()
  await probe
}

// ===========================================================================
// The reported symptom, made deterministic: frames stop mid-turn while the
// stream still LOOKS healthy. The turn's `session.idle` is lost, so `busy`
// stays true — and `scheduleRefetch` is busy-gated, so nothing ever reconciles.
// The tool part never reaches `completed`, so its metadata never renders, and a
// reload becomes the only way to see it. That is "I have to refresh my browser
// to load tool metadata".
// ===========================================================================
async function caseStalled() {
  const { sse, tabs, oc } = client
  const created = await j('/oc/session', { method: 'POST', body: { cwd: scratch, title: 'stalled' } })
  const sid = created.json?.id
  const info = await oc.session(sid)
  tabs.open({ id: sid, title: info?.title || sid.slice(0, 14), messages: [], live: true })
  sse.startEvents() // a second stream/watchdog in one process: fine for a check

  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'stream me' }] } })
  let running = []
  for (let i = 0; i < 40 && !(running = editPart(tabs.snapshot(sid)?.messages ?? [])).length; i++) await sleep(100)
  check(running.length > 0, `frames were flowing: the tool part reached the tab (${running[0]?.state?.status})`)
  check(tabs.snapshot(sid)?.busy === true, 'the tab went busy (so frames landed)')

  GAP = true // the pipe goes silent; readyState stays OPEN
  for (let i = 0; i < 60; i++) {
    const st = await j('/oc/session/status').catch(() => null)
    if (st?.json?.[sid]?.type === 'idle') break
    await sleep(200)
  }
  await sleep(300) // idle + the completed tool part pass by, dropped
  GAP = false

  // (1) Diagnostic, before anything heals: while `busy` is stuck, applyMessages
  // refuses to rewrite existing messages, so a tab-level refetch cannot cure a
  // part frozen at `running`. That is why the workaround is a PAGE reload
  // (fresh tab, busy:false) rather than closing/re-opening the tab.
  sse.applyMessages(sid, await oc.messages(sid), true)
  const stuck = editPart(tabs.snapshot(sid)?.messages ?? [])[0]
  console.log('  stuck-busy refetch probe:', JSON.stringify({ busy: tabs.snapshot(sid)?.busy, status: stuck?.state?.status, patch: !!patchTextOf(stuck) }))

  // (2) FIX A in isolation: reconcile busy, then refetchNow(). This runs well
  // inside the 10s watchdog window, so a pass here is `refetchNow`'s dirty flag
  // beating applyMessages' identical-id-set skip.
  const st = await oc.status()
  tabs.patch(sid, { busy: (st?.[sid]?.type ?? st?.[sid]?.state) === 'busy' })
  sse.refetchNow(sid)
  await sleep(1200)
  const r = editPart(tabs.snapshot(sid)?.messages ?? [])[0]
  check(
    r?.state?.status === 'completed' && !!patchTextOf(r),
    `FIX A: refetchNow lands the dropped part state (part=${r?.state?.status}, patch=${String(patchTextOf(r)).length})`,
  )

  // (3) FIX B in isolation: a second turn, gapped mid-turn exactly like the
  // first, and then NOTHING is touched by the test. Only the watchdog's
  // busy-reconcile against /oc/session/status can recover this, because the
  // stream never closed and so onopen/catchUp never fired.
  const before = editPart(tabs.snapshot(sid)?.messages ?? []).length
  GAP = false
  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'stream me again' }] } })
  let fresh = false
  for (let i = 0; i < 40 && !fresh; i++) {
    await sleep(100)
    fresh = editPart(tabs.snapshot(sid)?.messages ?? []).length > before && tabs.snapshot(sid)?.busy === true
  }
  check(fresh, 'the second turn reached the tab (busy again, a new tool part)')
  GAP = true
  for (let i = 0; i < 60; i++) {
    const s2 = await j('/oc/session/status').catch(() => null)
    if (s2?.json?.[sid]?.type === 'idle') break
    await sleep(200)
  }
  await sleep(400) // that turn's idle + terminal part frames pass by, dropped
  GAP = false
  let healed = false
  for (let i = 0; i < 100 && !healed; i++) {
    await sleep(200)
    const tt = tabs.snapshot(sid)
    const pp = editPart(tt?.messages ?? []).at(-1)
    healed = tt?.busy !== true && pp?.state?.status === 'completed' && !!patchTextOf(pp)
  }
  const t2 = tabs.snapshot(sid)
  const p2 = editPart(t2?.messages ?? []).at(-1)
  check(t2?.busy !== true, 'FIX B: the tab recovers its idle state with NO reconnect (it missed session.idle)')
  check(
    p2?.state?.status === 'completed' && !!patchTextOf(p2),
    `FIX B: that turn's tool part completes and carries metadata on its own (part=${p2?.state?.status}, patch=${!!patchTextOf(p2)})`,
  )
}

// Load the CLIENT before booting anything: a missing sibling webui checkout is
// a SKIP (exit 2) with a pointer, never a product failure and never a wasted
// server spawn.
let client
try {
  client = await loadWebuiClient(BASE)
  installGapEventSource()
} catch (e) {
  const skip = webuiSkip(e)
  console.log(skip ?? `CLIENT LOAD ERROR ${e?.stack ?? e}`)
  process.exit(skip ? 2 : 1)
}

try {
  await boot()
} catch (e) {
  console.log(`BOOT ERROR ${e?.stack ?? e}`)
  stop()
  await sleep(200)
  if (scratch) fs.rmSync(scratch, { recursive: true, force: true })
  process.exit(1)
}

for (const c of [main, caseStalled]) {
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
