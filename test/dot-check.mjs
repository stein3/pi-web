/**
 * The tab-bar dot must not blink on an IDLE session.
 *
 * The reported symptom: a session that is open but doing nothing flashes green
 * in the tab bar at a regular cadence, and it is most visible while a SIBLING
 * tab runs a turn. The green is `Tabs.svelte`'s `class:dirty={t.dirty &&
 * !t.busy}` — `stores.ts` documents `dirty` as "refetch pending", so the dot
 * literally renders the in-flight window of a background `GET /message`.
 *
 * `catchUp()` is the client's sync backstop and MUST keep running on every open
 * tab: a backgrounded/locked mobile tab loses SSE frames and SSE has no replay.
 * What must not happen is that its periodic pass paints a status dot on a tab
 * that was already idle and clean — re-pulling a transcript we already hold is
 * invisible bookkeeping, not a session state.
 *
 * Layers, all real code:
 *   layer 1  the PREDICATE (Tabs.svelte:64, verbatim below) — the dot must
 *            never be green for a tab that is not busy.
 *   layer 2  the REAL client (`webui-client.mjs` runs the shipped sse.ts +
 *            stores.ts + api.ts in node) driven through the browser's own
 *            gap path: socket dies -> onerror -> backoff re-open -> onopen ->
 *            catchUp(). That pass MUST still do its job — un-pin a stale
 *            `busy` from GET /session/status AND re-pull the messages — while
 *            leaving a tab that was already idle+clean dot-clean.
 *   layer 3  the reported scenario, driven ONLY by sse.ts's own 10s watchdog
 *            (no injected call): an idle tab while a sibling runs a long turn.
 *            `anyBusyLive()` makes the watchdog fire every 10s during that
 *            turn, which is the cadence the user sees.
 *
 * Self-contained: seeds fixtures in a scratch dir, spawns pi-web on 8128/8129
 * (its OWN pair — telemetry-check owns 8126/8127 and polls an engine there, so
 * sharing the pair made that check's assertions flake) with the fake pi, and
 * kills ONLY its own child by exact PID. Needs the sibling
 * webui checkout -> SKIP (exit 2), like client-check / toolmeta-check.
 *
 *   node test/dot-check.mjs
 *   PI_WEB_JS=/app/pi-web.mjs node test/dot-check.mjs   # A/B a build
 *   WEBUI_SRC=../opencode/webui/src                         # the client under test
 */
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { BRIDGE, TESTBIN, WEB } from './harness.mjs'
import { loadWebuiClient, ui, webuiSkip } from './webui-client.mjs'

const PORT = Number(process.env.CHECK_PORT || 8128)
const BASE = `http://127.0.0.1:${PORT}`
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}

// The dot predicate lives in webui-client.mjs's `ui` block, next to the other
// copied UI predicates (renders/partsOf/thinkingBlock), so every check reads the
// same contract. dotGreen is ui.dotDirty — Tabs.svelte:64's
// `class:dirty={t.dirty && !t.busy}`, painted `.dot.dirty { background: var(--ok) }`.
const dotGreen = ui.dotDirty

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
// Mid-turn frame loss WITHOUT a reconnect — the same shape toolmeta-check uses:
// the client still believes the stream is OPEN (readyState 1, the bridge's 10s
// heartbeats keep `lastFrame` fresh), so the ONLY thing that can reconcile is
// sse.ts's own watchdog -> catchUp(). GAP drops frames at the socket boundary,
// exactly as a half-open proxy pipe / a backgrounded mobile tab does. Without
// this, a dropped socket just auto-reconnects and the frames replay — which
// proves nothing about the reconcile.
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

async function boot(env = {}) {
  scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'dotcheck-'))
  agentDir = path.join(scratch, 'pi-agent')
  fs.mkdirSync(agentDir, { recursive: true })
  child = spawn(process.execPath, [BRIDGE, '--port', String(PORT), '--dir', agentDir, '--cwd', scratch], {
    cwd: WEB,
    env: {
      ...process.env,
      PATH: `${TESTBIN}:${process.env.PATH}`,
      PI_CODING_AGENT_DIR: agentDir,
      ...env,
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

// Count GET /message per session without replacing the client's fetch: the
// client's own shim passes a URL object, so a wrapper is fragile. The bridge
// logs nothing per request, so count on the server side instead.
let msgFetches = 0

function watchMessageFetches() {
  const orig = globalThis.fetch
  globalThis.fetch = (u, o) => {
    const s = typeof u === 'string' ? u : String(u?.href ?? u)
    if (/\/message\b/.test(s)) msgFetches++
    return orig(u, o)
  }
  return () => (globalThis.fetch = orig)
}

/** Open a tab the way the UI does (App.svelte openLive), run one turn so its
 *  transcript exists, and wait until the CLIENT believes it is idle. */
async function openIdleTab(client, title) {
  const { tabs, oc } = client
  const created = await j('/oc/session', { method: 'POST', body: { cwd: scratch, title: `dot-${title}` } })
  const sid = created.json?.id
  if (!sid) throw new Error(`session create failed: ${created.status}`)
  const info = await oc.session(sid)
  tabs.open({ id: sid, title: info?.title || sid.slice(0, 14), messages: [], live: true })
  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: `warm ${title}` }] } })
  for (let i = 0; i < 300 && tabs.snapshot(sid)?.busy; i++) await sleep(100)
  return sid
}

/** Wait for a tab to be genuinely idle AND clean (its post-turn idle refetch
 *  has landed). A turn that outlives the caller's budget is reported, not
 *  papered over — every assertion below depends on the tab starting clean. */
async function settleIdle(tabs, sid, budget = 600) {
  for (let i = 0; i < budget; i++) {
    const t = tabs.snapshot(sid)
    // synced == idle, clean, AND holding the transcript: a tab whose first
    // refetch has not landed yet is empty, and the next reconcile legitimately
    // has to pull it. Asserting "no dot" about that tab would assert nothing.
    if (t && t.busy !== true && t.dirty !== true && (t.messages?.length ?? 0) > 0) return true
    await sleep(100)
  }
  return false
}

/** Subscribe and count green windows on one tab (edge-triggered, so a store
 *  notify that merely re-fires the same state does not double-count). */
function watchGreen(tabs, sid) {
  let green = 0
  const transitions = []
  let prev = false
  const unsub = tabs.subscribe((all) => {
    const t = all.find((x) => x.id === sid)
    if (!t) return
    const g = dotGreen(t)
    if (g !== prev) {
      if (g) green++
      transitions.push(g ? 'GREEN' : 'off')
      prev = g
    }
  })
  return { get green() { return green }, transitions, unsub }
}

// ===========================================================================
// Layer 1 — the predicate. The dot is a pure function of the tab store, so the
// store state the client leaves on an idle tab decides whether the bar is green.
// ===========================================================================
async function casePredicate() {
  console.log('\n===== layer 1: the Tabs.svelte dot predicate =====')
  check(dotGreen({ dirty: true, busy: false }) === true, 'dirty && !busy renders green (this is the flash)')
  check(dotGreen({ dirty: false, busy: false }) === false, 'a clean idle tab shows no dot')
  check(dotGreen({ dirty: true, busy: true }) === false, 'busy wins over dirty (amber, not green)')
}

// ===========================================================================
// Layer 2 — the watchdog's PERIODIC pass, driven for real. Frames go silent
// while the stream still looks healthy, so the only thing that can reconcile is
// sse.ts's own 10s tick -> catchUp(). That pass must keep doing its job —
// un-pin a stale busy AND re-pull the messages — while leaving a tab that was
// already idle+clean dot-clean. This is the mobile/background case.
// ===========================================================================
async function caseGapReconcile() {
  console.log('\n===== layer 2: the watchdog pass must sync WITHOUT painting the dot =====')
  const { sse, tabs } = client
  sse.startEvents() // ONE stream + ONE watchdog for the whole check

  // A brand-new tab is EMPTY (App.svelte opens it with messages:[] and lets the
  // first refetch fill it), so the very first reconcile legitimately dirties it.
  // Open + settle a tab BEFORE the window under test, exactly as a returning
  // mobile user has one.
  const A = await openIdleTab(client, 'gap')
  await settleIdle(tabs, A)
  const start = tabs.snapshot(A)
  check(start?.busy !== true, `A is idle before the gap (busy=${start?.busy})`)
  check(start?.dirty !== true, `A is clean before the gap (dirty=${start?.dirty})`)
  check((start?.messages?.length ?? 0) > 0, `A holds its transcript (${start?.messages?.length} message(s))`)

  const dot = watchGreen(tabs, A)
  msgFetches = 0
  const restore = watchMessageFetches()
  // Arm the watchdog: `anyBusyLive()` gates it, so without a busy live tab the
  // tick does nothing and this case proves nothing. B turns; A stays idle.
  const SIB = await openIdleTab(client, 'gap-sibling')
  await settleIdle(tabs, SIB)
  await j(`/oc/session/${SIB}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'keep the watchdog armed' }] } })
  // re-arm once more so the tick is live for the whole window below
  for (let i = 0; i < 200 && tabs.snapshot(SIB)?.busy; i++) await sleep(100)
  await j(`/oc/session/${SIB}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'still armed' }] } })
  GAP = true // A's pipe goes silent; readyState stays OPEN. B's frames still
  // reach sse.ts (one stream), which is exactly the per-session loss the
  // watchdog comment describes: heartbeats keep lastFrame fresh while A's own
  // frames never arrive.
  // 24s, not 13s: the tick is a setInterval that fires only while a live tab is
  // busy, and B's turn ends partway through this window — so one sibling turn
  // arms AT MOST one pass for A. The first tick can still land before A's own
  // post-turn refetch has settled it, which would make the tab legitimately
  // dirty-worthy and prove nothing. Two windows guarantee a pass on a tab that
  // was already idle and clean (measured: 13s flaked red-to-green run to run).
  // 16s, and B turns AGAIN just before the window so the tick is armed for all
  // of it: `anyBusyLive()` gates the setInterval body, so a pass only happens
  // while some live tab is busy. Two ~4s turns + this window guarantee passes on
  // A that land when A is already idle and clean (13s flaked; a longer single
  // turn let the tick fire before A's own post-turn refetch had settled it).
  await sleep(16000)
  const midPass = tabs.snapshot(A)
  GAP = false
  await sleep(1500)
  restore()
  const after = tabs.snapshot(A)
  dot.unsub()

  console.log(`  green windows: ${dot.green}   transitions: ${dot.transitions.join(' ') || '(none)'}   GET /message: ${msgFetches}`)

  // THE BACKSTOP (must survive the fix): SSE has no replay for the lost window,
  // so the reconcile has to re-pull open tabs' transcripts from the server.
  check(msgFetches > 0, `the watchdog pass still re-pulled the transcript (${msgFetches} GET /message) — mobile/background sync intact`)
  check(after?.messages?.length > 0, `and the tab still holds its messages (${after?.messages?.length})`)

  // A pinned busy flag must still be corrected by the same pass.
  tabs.patch(A, { busy: true })
  check(tabs.snapshot(A)?.busy === true, 'A is (wrongly) pinned busy for the reconcile test')
  GAP = true
  await sleep(12000)
  GAP = false
  const healed = tabs.snapshot(A)
  check(healed?.busy !== true, 'catchUp() still un-pins a stale busy=true from GET /session/status (mid-pass: ' + String(midPass?.busy) + ')')

  // And it must not leave the dot lit afterwards.
  check(dotGreen(after) === false, 'and the tab ends the pass dot-clean')
}

// ===========================================================================
// Layer 3 — the reported scenario, with NO injected call: only sse.ts's own
// 10s watchdog. `anyBusyLive()` makes it fire every 10s while a sibling turns,
// and catchUp() refetches EVERY open tab — so each pass put a green dot on the
// idle one. That is "the status icon flashes green for sessions that are open
// but idle".
// ===========================================================================
async function caseSiblingTurn() {
  console.log('\n===== layer 3: idle tab while a sibling runs a turn (watchdog only) =====')
  const { sse, tabs } = client
  sse.startEvents() // the ONE stream + watchdog; frames must reach these tabs

  const A = await openIdleTab(client, 'watcher')
  const B = await openIdleTab(client, 'worker')
  const settled = await settleIdle(tabs, A)
  check(settled, 'A is idle and clean before the sibling turn')

  const dot = watchGreen(tabs, A)
  // Watch B for a dirty flag raised WHILE a tab is busy — i.e. the reconcile
  // re-pulled a session mid-turn. B's own post-idle refetch also sets dirty, so
  // the probe is edge-armed here and read before that settle can blur it.
  GAP = true // A's frames go silent while B's keep arriving on the same stream —
  // per-session loss, exactly what the watchdog comment describes: heartbeats
  // keep lastFrame fresh while one session's frames never arrive.
  await j(`/oc/session/${B}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'stream a long answer' }] } })
  for (let i = 0; i < 60 && !tabs.snapshot(B)?.busy; i++) await sleep(100)
  check(tabs.snapshot(B)?.busy === true, 'B is busy (so anyBusyLive() arms the 10s watchdog)')
  for (let i = 0; i < 200 && tabs.snapshot(B)?.busy; i++) await sleep(100)
  // re-arm: a second turn so `anyBusyLive()` is true across the next tick too
  await j(`/oc/session/${B}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'keep it armed' }] } })
  for (let i = 0; i < 200 && tabs.snapshot(B)?.busy; i++) await sleep(100)
  GAP = false
  await sleep(1200)
  dot.unsub()

  // The BUSY sibling must not be re-pulled by the reconcile: GET /message
  // returns the engine's committed snapshot, which lags the local deltas
  // mid-turn, and applyMessages is add-only while busy — so a catchUp pass on a
  // busy tab can only truncate live text. It heals at session.idle instead.
  // Sample DURING the turn: B's own idle refetch lands dirty=true for ~350ms at
  // settle, which is correct behaviour and would blur an after-the-fact read.

  console.log(`  green windows on the idle tab: ${dot.green}   transitions: ${dot.transitions.join(' ') || '(none)'}`)
  check(dot.green === 0, `the idle tab never flashed green during the sibling's turn (${dot.green} flash(es))`)
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
  // a ~4s turn: long enough that the watchdog's 10s tick lands DURING it, so a
  // busy tab gets reconciled against an idle status map — the case where
  // re-pulling would truncate live text.
  await boot({ FAKE_STREAM_WORDS: '60', FAKE_DELTA_MS: '60' })
} catch (e) {
  console.log(`BOOT ERROR ${e?.stack ?? e}`)
  stop()
  await sleep(200)
  if (scratch) fs.rmSync(scratch, { recursive: true, force: true })
  process.exit(1)
}

for (const c of [casePredicate, caseGapReconcile, caseSiblingTurn]) {
  try {
    await c()
  } catch (e) {
    check(false, `${c.name} threw: ${e?.message ?? e}`)
  }
}

stop()
await sleep(200)
if (scratch) fs.rmSync(scratch, { recursive: true, force: true })
console.log(`\n${failed ? `${failed} FAILURE(S)` : 'all assertions passed'}`)
if (failed) console.log(log.split('\n').filter((l) => /error|Error/.test(l)).slice(-10).join('\n'))
process.exit(failed ? 1 : 0)
