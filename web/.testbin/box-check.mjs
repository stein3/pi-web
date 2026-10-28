/**
 * M3 in the browser: the compaction BOX, driven through the real webui dist.
 *
 * Why a browser check at all — the store-level contract (typed part survives
 * normalizeMessages, the status map carries state + engine) is already pinned by
 * client-check and telemetry-check. What only a browser proves is the part a
 * component owns:
 *   1. the running summary draws an accent box with ENGINE PROGRESS in it
 *      (phase text + a prefill bar whose width actually moved) — the whole
 *      point of M1/M4, and invisible to any store-level assertion;
 *   2. the committed summary draws ONE collapsible row box, and pi's summary
 *      text appears exactly ONCE in the DOM (the bridge may ship an interim text
 *      part for older clients; Transcript.partsOf must drop it);
 *   3. expanding it reveals the body;
 *   4. a FAILED compaction draws pi's reason — pi writes no transcript entry for
 *      that case, so nothing else would ever show it;
 *   5. nothing overflows its container (numeric, not eyeballed: this box has no
 *      vision model to check it with).
 *
 * Needs: the sibling webui dist, the e2e launcher (fonts + shared libs — a
 * hand-rolled chromium.launch() renders blank text here) and chromium. Exits 2
 * with a SKIP message when they are missing — never a silent pass.
 *
 *   node .testbin/box-check.mjs
 */
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { loadE2E, browserSkip, shotsDir, CHECK_PORT, ENGINE_PORT } from './harness.mjs'
import { startEngine } from './fake-engine.mjs'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const WEB = path.resolve(HERE, '..')
const PORT = CHECK_PORT
const BASE = `http://127.0.0.1:${PORT}`
const SUMMARY_MARK = 'BOXCHECK-SUMMARY-MARKER'

let e2e
try {
  e2e = await loadE2E()
} catch (e) {
  console.log(e.message)
  process.exit(e.skip ? 2 : 1)
}
const WEBUI_DIST = process.env.WEBUI_DIST || path.join(e2e.REPO ?? path.resolve(HERE, '..', '..', '..', 'opencode', 'webui'), 'dist')
if (!fs.existsSync(path.join(WEBUI_DIST, 'index.html'))) {
  console.log(`SKIP: webui dist not found at ${WEBUI_DIST}\n  build it: cd ../opencode/webui && npm run build (or set WEBUI_DIST=)`)
  process.exit(2)
}

let browser
try {
  browser = await e2e.launchBrowser()
} catch (e) {
  console.log(browserSkip(e) ?? e?.stack ?? String(e))
  process.exit(browserSkip(e) ? 2 : 1)
}

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
          resolve({ status: res.statusCode, json })
        })
      },
    )
    r.on('error', reject)
    if (opts.body) r.write(JSON.stringify(opts.body))
    r.end()
  })
}

async function boot(env) {
  const SCRATCH = fs.mkdtempSync(path.join(os.tmpdir(), 'boxcheck-'))
  const AGENT = path.join(SCRATCH, 'pi-agent')
  fs.mkdirSync(AGENT, { recursive: true })
  // The provider's baseUrl points at the in-process fake engine: that is how the
  // probe discovers it in production too (models-store.json baseUrl minus /v1),
  // so the box gets engine-truth progress rather than the derived estimate.
  fs.writeFileSync(
    path.join(AGENT, 'models-store.json'),
    JSON.stringify({
      testprov: {
        models: [
          { id: 'fake-1', name: 'fake-1', baseUrl: `http://127.0.0.1:${ENGINE_PORT}/v1`, contextWindow: 256000, maxTokens: 8192, reasoning: true },
        ],
      },
    }),
  )
  const child = spawn(
    process.execPath,
    [
      process.env.PI_WEB_SCRIPT || path.join(WEB, 'pi-web.mjs'),
      '--port', String(PORT),
      '--dir', AGENT,
      '--cwd', SCRATCH,
      '--dist', WEBUI_DIST,
    ],
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
  return {
    SCRATCH,
    stop: () => {
      try {
        child.kill('SIGKILL') // exact PID only, never pattern-kill
      } catch {
        /* already gone */
      }
    },
    get log() {
      return log
    },
  }
}

const shots = shotsDir()
const perr = []

let page = null
async function freshPage() {
  if (page) await page.close().catch(() => {})
  page = await browser.newPage({ viewport: { width: 1280, height: 900, deviceScaleFactor: 2 } })
  page.on('pageerror', (e) => perr.push(String(e)))
  page.on('console', (m) => {
    if (m.type() === 'error') perr.push(m.text())
  })
  return page
}

// The sidebar lists /api/history/sessions — TRANSCRIPTS — so the session must
// persist (FAKE_PI_PERSIST) for a row to exist, exactly like a real one. It is
// created and prompted over the API (the composer needs an open tab, and the tab
// needs the session), then opened from the sidebar like a user would.
async function openSession(title) {
  const r = await req('/oc/session', {
    method: 'POST',
    body: { cwd: process.env.__SCRATCH, title, model: { providerID: 'testprov', id: 'fake-1' } },
  })
  const sid = r.json?.id
  check(!!sid, `PREMISE: session created (${sid})`)
  await req(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'stream me' }] } })
  await sleep(2500) // the turn finishes and the transcript exists
  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 20000 })
  await sleep(1500)
  const mounted = await e2e.poll(() => page.locator('body').innerText().then((t) => t.length > 40), 8000)
  check(mounted, 'PREMISE: the app mounted (text drew, not a blank page)')
  const row = page.locator('aside button.item, .sidebar button.item').first()
  const found = await e2e.poll(() => row.count().then((n) => n > 0), 8000)
  check(found, 'PREMISE: the session is listed in the sidebar')
  await row.click().catch(() => {})
  await sleep(1500)
  const composer = await e2e.poll(() => page.locator('textarea:visible, [contenteditable="true"]:visible').count().then((n) => n > 0), 6000)
  check(composer, 'PREMISE: the tab opened with a composer')
  const turn = await page.evaluate(() => document.body.innerText.includes('Hello streamed world.'))
  check(turn, 'PREMISE: the opened tab shows the streamed turn')
  return sid
}

async function boxState() {
  return page.evaluate(() => {
    const run = document.querySelector('.cmpbox.running')
    const row = [...document.querySelectorAll('details.cmpbox')][0]
    const fail = document.querySelector('.cmpbox.failed, .cmpbox.cancelled, .cmpbox.rejected')
    const bar = run?.querySelector('.cbar i')
    const text = (el) => (el ? (el.textContent ?? '').replace(/\s+/g, ' ').trim() : '')
    return {
      running: !!run,
      runningText: text(run),
      barWidth: bar ? bar.getBoundingClientRect().width : -1,
      row: !!row,
      rowText: text(row?.querySelector('summary') ?? row ?? null),
      open: !!row?.open,
      bodyText: text(row?.querySelector('.cmpbody') ?? null),
      fail: fail ? { kind: fail.className.replace('cmpbox ', '').trim(), text: text(fail) } : null,
      // overflow: the box must fit its container, not scroll sideways
      overflow: [...document.querySelectorAll('.cmpbox')].map((el) => el.scrollWidth - el.clientWidth),
    }
  })
}

async function runCase(name, env, fn) {
  console.log(`\n===== ${name} =====`)
  const b = await boot(env)
  process.env.__SCRATCH = b.SCRATCH
  await freshPage() // a new page per case: the previous case's server is gone, and
  // open-tab persistence (localStorage) must not resurrect its session id
  try {
    await fn()
  } catch (e) {
    console.log('ERROR', e?.stack ?? String(e))
    failed++
    await page.screenshot({ path: `${shots}/box-${name}-error.png` }).catch(() => {})
  } finally {
    b.stop()
    await sleep(300)
    fs.rmSync(b.SCRATCH, { recursive: true, force: true })
    if (failed) console.log('--- server log ---\n' + b.log.slice(-1500))
  }
}

try {
  // ---------- the running box, with engine progress ----------
  await runCase(
    'box-running-progress',
    { FAKE_PI_PERSIST: '1', FAKE_PI_COMPACT: 'slow', FAKE_PI_SUMMARY: `## Goal\n${SUMMARY_MARK} the compacted body\n\n## Next Steps\n1. n/a`, PI_WEB_SUMMARIZE_WAIT_MS: '300' },
    async () => {
      const eng = startEngine({ port: ENGINE_PORT, prefillMs: 1200, decodeMs: 6000, promptTotal: 119802 })
      const sid = await openSession('BOXCHECK-ONE')
    check(!!sid, `PREMISE: a live session exists (${sid})`)
    eng.run()
    await req(`/oc/session/${sid}/summarize`, { method: 'POST', body: { customInstructions: 'CHECK_COMPACT_MS=6000' } })
    const mid = await e2e.poll(async () => (await boxState()).running, 4000)
    check(mid, 'the running compaction BOX is on screen')
    let s = await boxState()
    check(/Compacting session/.test(s.runningText), `it says what is running (${JSON.stringify(s.runningText)})`)
    // The engine sample lands on the probe's own 1 Hz clock, so WAIT for it —
    // asserting on the frame that appeared before the first sample only proves
    // the pre-probe fallback drew.
    const withEngine = await e2e.poll(async () => {
      const x = await boxState()
      return /thinking|answering/.test(x.runningText) && x.barWidth > 0 ? x : null
    }, 6000)
    check(!!withEngine, `it shows the ENGINE phase text and a prefill bar (${JSON.stringify(withEngine?.runningText)} bar=${withEngine?.barWidth?.toFixed(0)}px)`)
    await sleep(1800) // prefill → decode: the numbers must move
    const s2 = await boxState()
    const tokAt = (t) => Number(/([\d,]+) tok/.exec(t)?.[1]?.replace(/,/g, '') ?? -1)
    check(
      tokAt(s2.runningText) > tokAt(withEngine?.runningText ?? ''),
      `generated tokens advanced while the summary ran (${tokAt(withEngine?.runningText ?? '')} → ${tokAt(s2.runningText)})`,
    )
    check(/tok\/s/.test(s2.runningText), `decode rate is shown (${JSON.stringify(s2.runningText)})`)
    await page.screenshot({ path: `${shots}/box-running.png` })
    const done = await e2e.poll(async () => (await boxState()).row, 12000)
    check(done, 'the committed summary became a row box')
    const r = await boxState()
    check(!r.running, 'the running box is gone once the summary lands')
    check(
      /Context compacted/.test(r.rowText) && /42,000 tokens summarised/.test(r.rowText) && /→ 900/.test(r.rowText),
      `the head line carries the counts from state (${JSON.stringify(r.rowText)})`,
    )
    check(!r.open, 'it starts collapsed')
    await page.locator('details.cmpbox summary').first().click()
    await sleep(400)
    const r2 = await boxState()
    check(r2.open && r2.bodyText.includes(SUMMARY_MARK), `expanding reveals the body (${JSON.stringify(r2.bodyText.slice(0, 40))})`)
    // textContent, not innerText: a collapsed <details> hides its body from
    // innerText, so counting there would read 0 and look like a fix.
    const dupe = await page.evaluate((mark) => (document.body.textContent.match(new RegExp(mark, 'g')) ?? []).length, SUMMARY_MARK)
    check(dupe === 1, `pi's summary is in the DOM EXACTLY ONCE (${dupe}) — nothing draws it twice`)
    await page.screenshot({ path: `${shots}/box-row-open.png` })
    const ov = (await boxState()).overflow
    check(ov.every((n) => n === 0), `no box overflows its container (${JSON.stringify(ov)})`)
    await eng.stop()
  })

  // ---------- a failed compaction: the reason has nowhere else to live ----------
  await runCase(
    'box-failed-reason',
    { FAKE_PI_PERSIST: '1', FAKE_PI_COMPACT: 'slowfail', PI_WEB_SUMMARIZE_WAIT_MS: '200', FAKE_PI_COMPACT_MS: '700' },
    async () => {
      const eng = startEngine({ port: ENGINE_PORT, prefillMs: 1200, decodeMs: 6000, promptTotal: 119802 })
      const sid = await openSession('BOXCHECK-TWO')
    check(!!sid, `PREMISE: a live session exists (${sid})`)
    await req(`/oc/session/${sid}/summarize`, { method: 'POST', body: {} })
    // the terminal state is hydrated from the status poll, not the SSE frame
    const shown = await e2e.poll(async () => (await boxState()).fail, 12000)
    check(!!shown, 'the FAILED compaction box is on screen')
    const s = await boxState()
    check(/Compaction failed/.test(s.fail?.text ?? ''), `it says it failed (${JSON.stringify(s.fail?.text)})`)
    check(/Compaction failed: Nothing to compact/.test(s.fail?.text ?? ''), `and quotes pi's OWN reason sentence (${JSON.stringify(s.fail?.text)})`)
      const rows = await page.evaluate(() => document.querySelectorAll('details.cmpbox').length)
      check(rows === 0, `no summary row was invented for a failed compaction (${rows})`)
      await page.screenshot({ path: `${shots}/box-failed.png` })
      await eng.stop()
    },
  )
} finally {
  await browser.close()
}
console.log('\npixel proof: ' + shots)
console.log('page/console errors:', perr.length ? perr.slice(0, 6) : 'none')
console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
process.exit(failed ? 1 : 0)
