/**
 * The Files drawer, rendered: chromium driving the built webui dist.
 *
 * Everything about the file browser is already pinned at the HTTP layer
 * (`fsbrowse-check.mjs` — containment, caps, policy, git shapes) and at the
 * store layer (`changes.test.ts` — the per-call projection). What ONLY a browser
 * proves is the part the components own:
 *
 *   1. the drawer opens with ONE host and a Files | Changes strip — the
 *      coexistence decision, and the reason a phone can still use the app;
 *   2. the tree draws the session workspace: dirs first, `.env` visible,
 *      node_modules absent — tau's policy, as rendered;
 *   3. GIT badgers the tree (● modified, + untracked). This is the answer to
 *      "what did the agent change through python/sed": the transcript carries no
 *      patch for those, git does;
 *   4. selecting a dirty file opens the viewer on its GIT DIFF (a real Monaco
 *      diff editor), and the `file` toggle flips to a single-model read-only
 *      editor — the two modes must not be the same editor (this exact assertion
 *      caught a Svelte reactive-dependency bug: `load()` reads `mode` inside,
 *      which Svelte does NOT track, so the toggle did nothing);
 *   5. the Changes tab still renders the old pane (nothing was replaced);
 *   6. coming back to Files still works off the module cache, and nothing
 *      overflows its container (numeric, not eyeballed).
 *
 * Needs the sibling webui dist, the e2e launcher (fonts + shared libs — a
 * hand-rolled chromium.launch() renders blank text here) and chromium. Exits 2
 * with a SKIP message when they are missing — never a silent pass.
 *
 *   node test/fsbox-check.mjs
 *   PI_WEB_JS=/path/to/older/pi-web.mjs node test/fsbox-check.mjs
 */
import { execFileSync, spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { loadE2E, browserSkip, shotsDir, BRIDGE, CHECK_PORT, WEB } from './harness.mjs'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const PORT = Number(process.env.FSBOX_CHECK_PORT || CHECK_PORT)
const BASE = `http://127.0.0.1:${PORT}`

let e2e
try {
  e2e = await loadE2E()
} catch (e) {
  console.log(e.message)
  process.exit(e.skip ? 2 : 1)
}
const WEBUI_DIST =
  process.env.WEBUI_DIST || path.join(e2e.REPO ?? path.resolve(HERE, '..', '..', 'opencode', 'webui'), 'dist')
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
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
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

// ---- fixtures: the SESSION workspace is a git repo with known dirt --------
const SCRATCH = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'fsbox-')))
// The agent dir lives OUTSIDE the session workspace: inside it, pi's own
// transcripts would show up as untracked dirt in the very tree under test.
const AGENT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'fsbox-agent-')))
const GIT_ENV = {
  ...process.env,
  GIT_CONFIG_GLOBAL: '/dev/null',
  GIT_CONFIG_SYSTEM: '/dev/null',
  GIT_AUTHOR_NAME: 't',
  GIT_AUTHOR_EMAIL: 't@x',
  GIT_COMMITTER_NAME: 't',
  GIT_COMMITTER_EMAIL: 't@x',
}
const git = (...args) => execFileSync('git', ['-C', SCRATCH, ...args], { env: GIT_ENV })

function fixtures() {
  fs.mkdirSync(path.join(AGENT, 'sessions'), { recursive: true })
  fs.writeFileSync(path.join(SCRATCH, 'tracked.txt'), 'clean line\nHEAD-ONLY-MARKER\n')
  fs.mkdirSync(path.join(SCRATCH, 'sub'), { recursive: true })
  fs.writeFileSync(path.join(SCRATCH, 'sub', 'nested.txt'), 'NESTED-FILE-MARKER\n')
  fs.mkdirSync(path.join(SCRATCH, 'node_modules'), { recursive: true })
  fs.writeFileSync(path.join(SCRATCH, 'node_modules', 'junk.js'), 'junk\n')
  fs.writeFileSync(path.join(SCRATCH, '.env'), 'SECRET=x\n')
  git('init', '-q', '.')
  git('add', '-A')
  git('-c', 'commit.gpgsign=false', 'commit', '-qm', 'init')
  // dirty ON PURPOSE, and dirty OUT OF BAND: no tool part in any transcript
  // would ever report these — git is the only thing that can.
  fs.writeFileSync(path.join(SCRATCH, 'tracked.txt'), 'FILE-CONTENT-MARKER\nCHANGED-IN-FIXTURE\n')
  fs.writeFileSync(path.join(SCRATCH, 'untracked.txt'), 'UNTRACKED-MARKER\n')
  fs.writeFileSync(path.join(SCRATCH, 'sub', 'nested.txt'), 'NESTED-FILE-MARKER\nDIRTY-NESTED-ON-PURPOSE\n')
}

function boot() {
  const child = spawn(
    process.execPath,
    [
      BRIDGE,
      '--port', String(PORT),
      '--host', '127.0.0.1',
      '--dir', AGENT,
      '--cwd', SCRATCH,
      '--dist', WEBUI_DIST,
    ],
    {
      cwd: WEB,
      // Own process group: pi-web spawns the pi/fake-pi child, and killing only
      // the bridge left that grandchild holding the port. Two leaked servers on
      // fixed ports made an UNRELATED e2e check die with EADDRINUSE, which reads
      // exactly like a regression.
      detached: true,
      env: {
        ...process.env,
        PATH: `${HERE}:${process.env.PATH}`,
        PI_CODING_AGENT_DIR: AGENT,
        FAKE_PI_PERSIST: '1',
        PI_WEB_REAP_INTERVAL_MS: '3600000',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  )
  let log = ''
  child.stdout.on('data', (d) => (log += d))
  child.stderr.on('data', (d) => (log += d))
  // Kill the whole group by negative PID — exact PID we spawned, never a pattern.
  const reap = () => {
    try {
      process.kill(-child.pid, 'SIGKILL')
    } catch {
      try {
        child.kill('SIGKILL')
      } catch {
        /* already gone */
      }
    }
  }
  // A check interrupted by a signal (or by a shell pipeline closing its stdout)
  // must still leave no server behind.
  process.on('exit', reap)
  for (const sig of ['SIGINT', 'SIGTERM']) {
    process.on(sig, () => {
      reap()
      process.exit(130)
    })
  }
  return {
    stop: reap,
    get log() {
      return log
    },
  }
}

const shots = shotsDir()
const perr = []
let page

// a control that a pre-fix build may not have at all: fail the assertion, do
// not crash the run — the point of running this against an old dist is to see
// WHICH assertions go red.
async function tap(sel) {
  try {
    await page.click(sel, { timeout: 2500 })
    return true
  } catch {
    return false
  }
}

async function drawerState() {
  return page.evaluate(() => {
    const q = (s) => document.querySelector(s)
    const rows = [...document.querySelectorAll('.fb .tree .row, .fb .tree .crumb')].map((el) => ({
      name: el.querySelector('.nm')?.textContent ?? '',
    text: el.textContent.trim(),
      text: el.textContent.trim(),
      badge: el.querySelector('.badge')?.textContent ?? '',
      isDir: el.className.includes('dirrow'),
    }))
    const diff = q('.fb .editor .monaco-diff-editor')
    const plain = q('.fb .editor .monaco-editor')
    return {
      strip: [...document.querySelectorAll('.dtabbar button')].map((b) => b.textContent.trim()),
      dpath: document.querySelector('.dtabbar .dpath')?.textContent ?? '',
      dpathTitle: document.querySelector('.dtabbar .dpath')?.title ?? '',
      onlyDirty: !!document.querySelector('.fb .only input')?.checked,
      crumbs: document.querySelectorAll('.fb .crumb').length,
      open: !!document.querySelector('.dtabbar'),
      wrapW: document.querySelector('.diffwrap')?.clientWidth ?? 0,
      editorW:
        document
          .querySelector('.fb .editor .monaco-editor, .fb .editor .monaco-diff-editor')
          ?.clientWidth ?? 0,
      grip: !!document.querySelector('.rgrip'),
      topbar: [...document.querySelectorAll('.topbar button')].map((b) => b.title),
      activeTab: [...document.querySelectorAll('.dtabbar button.on')].map((b) => b.textContent.trim()),
      rows,
      crumb: q('.fb .count')?.textContent ?? '',
      viewer: !!q('.fb .viewer'),
      viewerHead: q('.fb .viewer .head .p')?.textContent ?? '',
      headBtns: [...document.querySelectorAll('.fb .viewer .head button')].map((b) => b.textContent.trim()),
      diffEditor: !!diff,
      plainOnly: !diff && !!plain,
      diffText: diff ? (diff.textContent ?? '').replace(/\s+/g, ' ') : '',
      editorText: q('.fb .editor .view-lines')?.textContent ?? '',
      changesPane: !!q('.diffpane'),
      // NOTE: InfoPanel renders inside SidePanel's <aside class="panel"> — an
      // "aside.info" selector matches NOTHING and makes every info assertion
      // pass vacuously (it did, until the restore assertions needed a true).
      // Identify the panel by its title instead.
      infoOpen: [...document.querySelectorAll('aside.panel')].some(
        (el) => el.querySelector('.head .ttl')?.textContent === 'Session Info'
      ),
      mainWidth: document.querySelector('main')?.clientWidth ?? 0,
      viewport: window.innerWidth,
      overflow: [...document.querySelectorAll('.fb, .fb .tree, .fb .viewer')].map((el) => el.scrollWidth - el.clientWidth),
    }
  })
}

async function clickByText(sel, text) {
  const n = await page.locator(sel).count()
  for (let i = 0; i < n; i++) {
    const t = (await page.locator(sel).nth(i).textContent()) ?? ''
    if (t.trim() === text || t.includes(text)) {
      await page.locator(sel).nth(i).click()
      return true
    }
  }
  return false
}

async function main() {
  fixtures()
  const srv = boot()
  for (let i = 0; i < 60; i++) {
    try {
      if ((await req('/healthz')).status === 200) break
    } catch {
      /* not up yet */
    }
    await sleep(200)
  }

  // A session in SCRATCH, opened from the sidebar like a user would: the drawer
  // roots itself on the TAB's cwd, so the tab has to be real.
  const created = await req('/oc/session', { method: 'POST', body: { cwd: SCRATCH, title: 'fsbox' } })
  const sid = created.json?.id
  check(!!sid, `PREMISE: session created (${sid})`)
  await req(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'stream me' }] } })
  // the transcript must exist before the page loads (the sidebar fetches its
  // list once) — poll that list, not a guessed turn duration (was sleep(2500))
  await e2e.poll(() => req('/api/history/sessions').then((r) => (r.json ?? []).some((s) => s.id === sid)), 10000)

  page = await browser.newPage({ viewport: { width: 1280, height: 900, deviceScaleFactor: 2 } })
  page.on('pageerror', (e) => perr.push(String(e)))
  page.on('console', (m) => {
    if (m.type() === 'error') perr.push(m.text())
  })
  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 20000 })
  const row = page.locator('aside button.item, .sidebar button.item').first()
  check(await e2e.poll(() => row.count().then((n) => n > 0), 8000), 'PREMISE: the session is listed in the sidebar')
  await row.click().catch(() => {})
  await e2e.poll(() => page.locator('button[title^="Toggle the drawer"], button[title^="Toggle file browser"]').count().then((n) => n > 0), 5000)

  // 1. one drawer host, two tabs, opened on Files
  await page.click('button[title^="Toggle the drawer"], button[title^="Toggle file browser"]')
  check(await e2e.poll(() => page.locator('.dtabbar').count().then((n) => n > 0), 5000), 'the drawer opens with a Files | Changes strip')
  let st = await drawerState()
  check(st.strip.join('|').includes('Files') && st.strip.join('|').includes('Changes'), `the strip names both tabs (${st.strip.join('|')})`)
  check(st.activeTab.join('') === 'Files', 'the folder button opened Files', st.activeTab.join(''))

  // two right drawers at once crush the transcript (this was the screenshot
  // bug: info + drawer left the conversation ~180px at a 1280px viewport)
  st = await drawerState()
  check(!st.infoOpen, 'opening the drawer closed the Info panel (one right drawer at a time)')
  // the crushed case is the point: info + drawer measured ~180px of transcript
  // at this viewport. The drawer's own share (45%) is DiffPane's long-standing
  // width and not what this assertion is for.
  check(st.mainWidth >= st.viewport * 0.28, `the transcript keeps a usable width (${st.mainWidth}px of ${st.viewport}px)`)

  // 2 + 3. the tree, as policy, badged by git
  check(await e2e.poll(() => drawerState().then((s) => s.rows.length > 0), 6000), 'the tree listed the session workspace')
  st = await drawerState()
  const names = st.rows.map((r) => r.name)
  check(names.includes('tracked.txt') && names.includes('untracked.txt'), 'the dirty files are listed', names.join(','))
  check(!names.includes('node_modules'), 'node_modules never shows up', names.join(','))
  check(names.includes('.env'), '.env is listed (the one dotfile that stays)', names.join(','))
  check(st.rows.length > 1 && st.rows[0].isDir, 'directories sort before files', names.join(','))
  const byName = Object.fromEntries(st.rows.map((r) => [r.name, r.badge]))
  check(byName['tracked.txt'] === '●', `a modified file carries the git badge (got ${JSON.stringify(byName['tracked.txt'])})`)
  check(byName['untracked.txt'] === '+', `an untracked file carries + (got ${JSON.stringify(byName['untracked.txt'])})`)
  check(/dirty/.test(st.crumb), 'the head says how dirty the repo is', JSON.stringify(st.crumb.trim()))

  // 4. a dirty file opens on its git diff; the `file` toggle changes the editor
  check(await clickByText('.fb .tree .row', 'tracked.txt'), 'clicking the modified file selected it')
  check(await e2e.poll(() => drawerState().then((s) => s.diffEditor), 6000), 'the viewer opened on a MONACO DIFF EDITOR (git diff vs HEAD)')
  st = await drawerState()
  check(st.diffText.includes('CHANGED-IN-FIXTURE'), 'and the diff carries the out-of-band change no tool part reported', st.diffText.slice(0, 90))

  // diffText above joins BOTH panes, so "it contains the marker" is satisfied by
  // either side — it cannot tell a real diff from the same model on both sides.
  // Ask each pane separately, and ask the renderer whether it painted anything.
  // Per-pane selectors are NOT the way: Monaco flips to inline below its width
  // breakpoint, and then there is no separate original/modified text to read.
  // So make the two images token-distinct at the FIXTURE level: HEAD-ONLY-MARKER
  // exists nowhere on disk, CHANGED-IN-FIXTURE exists nowhere in HEAD. A diff
  // must show both; a file view must show exactly one. Any mount-one-model-
  // twice regression (or a bridge returning the file as its own pre-image) fails
  // one of the two, whichever layout Monaco picked.
  const headToken = (t) => /HEAD-ONLY-MARKER/.test(t)
  check(headToken(st.diffText) && /CHANGED-IN-FIXTURE/.test(st.diffText), 'the diff shows BOTH the HEAD pre-image and the worktree image', st.diffText.slice(0, 100))
  check(st.headBtns.includes('file') && st.headBtns.includes('diff'), 'the viewer offers file|diff', st.headBtns.join('|'))
  // the visual artifact a UI check should leave behind for a human
  await page.screenshot({ path: path.join(shots, 'fsbox-diff.png') }).catch(() => {})
  await clickByText('.fb .viewer .head button', 'file')
  check(await e2e.poll(() => drawerState().then((s) => s.plainOnly), 5000), 'the file toggle swapped to a single-model editor (not a diff editor)')
  st = await drawerState()
  check(st.editorText.includes('FILE-CONTENT-MARKER'), 'and the plain editor shows the file as it is on disk', JSON.stringify(st.editorText.slice(0, 60)))
  check(!headToken(st.editorText), 'the plain editor holds NO HEAD-only text (it reads the disk, not the diff)', JSON.stringify(st.editorText.slice(0, 60)))

  // an untracked file has no HEAD side: the diff route synthesizes all-adds
  await clickByText('.fb .tree .row', 'untracked.txt')
  check(await e2e.poll(() => drawerState().then((s) => s.diffEditor && s.diffText.includes('UNTRACKED-MARKER')), 6000), 'an untracked file opens as an all-adds diff')

  // a clean file opens as plain text, and a nested dir expands on click
  await clickByText('.fb .tree .dirrow', 'sub')
  check(await e2e.poll(() => drawerState().then((s) => s.rows.some((r) => r.name === 'nested.txt')), 5000), 'a directory expands to its children')

  // 4b. one row per directory. The nested tree instance used to render its
  // OWN header for the dir it was showing, so expanding 'web' printed
  // '▾ web 4' and then '▾ web' — the same folder twice.
  st = await drawerState()
  // match on RENDERED text, not the .nm span: the duplicate that used to appear
  // was '▾ sub' with no inner name element, so a name-based filter walked past
  // the exact bug this line is for.
  const subRows = st.rows.filter((r) => /(^|[\s▸▾])sub([\s]|$)/.test(r.text))
  // .crumb counts as a row here on purpose: a selector that ignored it would be
  // blind to the duplicate it is supposed to catch.
  check(subRows.length === 1, 'an expanded dir appears exactly ONCE', JSON.stringify(subRows.map((r) => [r.name, r.isDir])))
  check(st.crumbs === 0, 'the nested listing renders no header of its own', `crumbs=${st.crumbs}`)

  // 4c. the header names the directory pi sees (the session cwd, which for a
  // renamed workspace is the liveCwd() fallback — not the dead header path)
  check(st.dpath.includes(path.basename(SCRATCH)), 'the drawer header shows the session cwd', st.dpath)
  check(st.dpathTitle.includes(SCRATCH), 'and its title carries the whole path', st.dpathTitle.slice(0, 80))

  // Where the path is PAINTED, not what it says. A Range over the element's
  // contents gives the text rect on any build, so this reads the same whether
  // the string lives in a wrapper span or sits loose in a stretched block —
  // which is exactly the difference between "right-aligned at the top edge" and
  // "centered in the space the tabs leave".
  const cent = await page.evaluate(() => {
    const el = document.querySelector('.dtabbar .dpath')
    if (!el) return null
    const box = el.getBoundingClientRect()
    const rng = document.createRange()
    rng.selectNodeContents(el)
    const t = rng.getBoundingClientRect()
    return {
      dx: (t.left + t.right) / 2 - (box.left + box.right) / 2,
      dy: (t.top + t.bottom) / 2 - (box.top + box.bottom) / 2,
      box: [Math.round(box.width), Math.round(box.height)],
      text: [Math.round(t.width), Math.round(t.height)],
    }
  })
  // centered in the space the tabs leave — both axes. A strip with no
  // align-items stretches the block and the text hugs the top edge; right-
  // alignment leaves a third of the row empty on each side of nothing.
  check(Math.abs(cent.dx) <= 1, 'the cwd string sits centered HORIZONTALLY in its space', JSON.stringify(cent))
  check(Math.abs(cent.dy) <= 1, 'and centered VERTICALLY in its space', JSON.stringify(cent))

  // 4d. "only dirty": git-dirt filter. Collapse sub first, so the checkbox
  // re-opening it is evidence of the auto-expand rather than of my last click.
  await clickByText('.fb .tree .dirrow', 'sub')
  check(await e2e.poll(() => drawerState().then((sq) => !sq.rows.some((r) => r.name === 'nested.txt')), 4000), 'clicking an open dir collapses it')
  await tap('.fb .only input')
  check(
    await e2e.poll(
      async () => {
        const sq = await drawerState()
        const names = sq.rows.map((r) => r.name)
        return sq.onlyDirty && !names.includes('.env') && names.includes('tracked.txt') && names.includes('untracked.txt')
      },
      4000
    ),
    'the dirty filter hides clean entries and keeps git-dirty ones',
    JSON.stringify((await drawerState()).rows.map((r) => r.name))
  )
  // (the collapse above is what makes this mean anything: on a build where
  // clicking an open dir does nothing, nested.txt was already showing and this
  // passes vacuously — that build is already red one line up)
  check(
    await e2e.poll(() => drawerState().then((sq) => sq.rows.some((r) => r.name === 'nested.txt')), 5000),
    'and it opens the dirs whose dirt is nested (the dirty count is the roll-up)'
  )
  await tap('.fb .only input')
  check(
    await e2e.poll(() => drawerState().then((sq) => !sq.onlyDirty && sq.rows.some((r) => r.name === '.env')), 4000),
    'unchecking it brings the clean entries back'
  )

  // 5. coexistence: the Changes tab still draws the old pane
  await clickByText('.dtabbar button', 'Changes')
  check(await e2e.poll(() => drawerState().then((s) => s.changesPane), 6000), 'the Changes tab still renders the existing diff pane (nothing replaced)')

  // 6. back to Files: served from the module cache, and nothing overflows
  await clickByText('.dtabbar button', 'Files')
  check(await e2e.poll(() => drawerState().then((s) => s.rows.length > 0), 3000), 'Files comes back with its tree (module cache)')
  st = await drawerState()
  check(st.overflow.every((n) => n <= 1), `nothing overflows its container (${JSON.stringify(st.overflow)})`)

  await page.screenshot({ path: path.join(shots, 'fsbox-files.png'), fullPage: false }).catch(() => {})

  // 7. ONE topbar button, and Ctrl+D is the drawer — on the tab used LAST
  st = await drawerState()
  check(!st.topbar.some((t) => /changes pane|diff pane/i.test(t)), 'no separate changes/diff button survives in the topbar', JSON.stringify(st.topbar))
  check(st.topbar.some((t) => /Toggle the drawer/.test(t) && /Ctrl\+D/.test(t)), 'the folder button is the drawer toggle and says so', JSON.stringify(st.topbar))

  const pressD = () => page.keyboard.press('Control+d')
  await pressD()
  check(await e2e.poll(() => drawerState().then((sq) => !sq.open), 4000), 'Ctrl+D closes an open drawer')
  await pressD()
  st = await drawerState()
  check(st.open && st.activeTab.join('') === 'Files', 'Ctrl+D re-opens it on the tab used last (Files)', st.activeTab.join(''))
  await clickByText('.dtabbar button', 'Changes')
  await pressD()
  check(await e2e.poll(() => drawerState().then((sq) => !sq.open), 4000), 'Ctrl+D closes it again')
  await pressD()
  st = await drawerState()
  check(st.open && st.activeTab.join('') === 'Changes', 'and re-opens on CHANGES now that Changes was last (the hotkey follows the user, not a default)', st.activeTab.join(''))
  await page.click('button[title^="Toggle the drawer"]')
  check(await e2e.poll(() => drawerState().then((sq) => !sq.open), 4000), 'the folder icon alone toggles the drawer closed (one button is enough)')
  await page.click('button[title^="Toggle the drawer"]')
  check(await e2e.poll(() => drawerState().then((sq) => sq.open), 4000), 'and opens it again')

  // 7b. resize: the grip, the clamp, the persistence, and that the editor
  // inside keeps filling the pane (automaticLayout:true is a claim until a
  // check watches monaco's box track the drawer's box through a drag).
  // State-agnostic premise: section 7 may have left the drawer open (a fixed
  // pressD here toggled it SHUT on the first run).
  st = await drawerState()
  if (!st.open) await pressD()
  check(await e2e.poll(() => drawerState().then((sq) => sq.open), 4000), 'PREMISE: drawer open for the resize run')
  // ...and there must be a file in the viewer to track: closing the drawer
  // destroys FileBrowser, selection included, so re-select one. The tree renders
  // after a lazy import + a listing fetch — clickByText silently returns false
  // on an empty tree, so WAIT for the rows before clicking one (a fixed sleep
  // here lost two runs to the race and the failure blamed the editor).
  await clickByText('.dtabbar button', 'Files')
  check(await e2e.poll(() => drawerState().then((sq) => sq.rows.length > 0), 6000), 'PREMISE: the tree rendered after the tab switch')
  check(await clickByText('.fb .tree .row', 'tracked.txt'), 'PREMISE: clicked a file in the tree')
  check(await e2e.poll(() => drawerState().then((sq) => sq.editorW > 0), 6000), 'PREMISE: a file is open in the viewer')
  st = await drawerState()
  check(st.grip, 'the drawer exposes a drag grip on its left edge')

  // The grip follows its edge as the drawer resizes, so RE-READ its position
  // for every drag — tracking it by hand walked the mouse off-screen and made
  // three of these assertions pass on a run where nothing moved at all.
  const drag = async (dx) => {
    const g = await page.evaluate(() => {
      const r = document.querySelector('.rgrip')?.getBoundingClientRect()
      return r ? { x: r.x + r.width / 2, y: r.y + r.height / 2 } : null
    })
    if (!g) return
    await page.mouse.move(g.x, g.y)
    await page.mouse.down()
    await page.mouse.move(g.x + dx, g.y, { steps: 6 })
    await page.mouse.up()
  }

  // widen would immediately hit the ceiling at the 45% default, so narrow
  // first: the clamp's two ends then both get exercised with room to spare.
  const w0 = st.wrapW
  const e0 = st.editorW
  await drag(100) // rightwards = narrower
  st = await drawerState()
  check(Math.abs(st.wrapW - (w0 - 100)) <= 12, `dragging the grip right narrowed the drawer by ~100px (${w0} -> ${st.wrapW})`)
  check(Math.abs(st.editorW - (e0 - 100)) <= 12, `and the editor inside re-fills the new width (${e0} -> ${st.editorW})`)

  await drag(-4000) // way past the ceiling
  st = await drawerState()
  check(st.mainWidth >= st.viewport * 0.28, `the clamp keeps the transcript's 28% (${st.mainWidth}px of ${st.viewport}px)`, `wrap=${st.wrapW}`)

  await drag(8000) // way past the floor
  st = await drawerState()
  check(Math.abs(st.wrapW - 340) <= 2, `the clamp holds the 340px floor (${st.wrapW})`)

  await drag(-200) // settle on a distinctive width for the persistence run
  st = await drawerState()
  const saved = st.wrapW
  check(Math.abs(saved - 540) <= 12, 'PREMISE: a distinctive width to survive the reload', String(saved))

  await page.reload({ waitUntil: 'domcontentloaded', timeout: 20000 })
  await e2e.poll(() => page.locator('button[title^="Toggle the drawer"]').count().then((n) => n > 0), 5000)
  await page.click('button[title^="Toggle the drawer"]')
  check(await e2e.poll(() => drawerState().then((sq) => sq.open), 5000), 'PREMISE: drawer open after the reload')
  st = await drawerState()
  check(Math.abs(st.wrapW - saved) <= 4, `the width survives a reload (${saved} -> ${st.wrapW})`)

  // 8. the drawer borrows Info's half of the right edge — and gives it BACK.
  // The latch lives in stores.ts (stores.test.ts pins the logic); this pins the
  // rendered wiring, which is where a forgotten close path would show up.
  await pressD() // close the drawer left open above
  check(await e2e.poll(() => drawerState().then((sq) => !sq.open), 4000), 'PREMISE: drawer closed again')
  st = await drawerState()
  if (!st.infoOpen) await tap('button[title="Toggle info panel"]')
  check(await e2e.poll(() => drawerState().then((sq) => sq.infoOpen), 4000), 'PREMISE: the Info panel is showing')
  await pressD()
  check(
    await e2e.poll(() => drawerState().then((sq) => sq.open && !sq.infoOpen), 4000),
    'PREMISE: drawer open, Info hidden (one right drawer at a time)'
  )
  await pressD()
  check(
    await e2e.poll(() => drawerState().then((sq) => !sq.open && sq.infoOpen), 5000),
    'closing the drawer gives the space BACK to the Info panel'
  )

  // and an Info the user closed BY HAND is never resurrected by the drawer
  st = await drawerState()
  if (st.infoOpen) await tap('button[title="Toggle info panel"]') // ensure closed, on any build
  check(await e2e.poll(() => drawerState().then((sq) => !sq.infoOpen), 4000), 'PREMISE: Info closed by hand')
  await pressD()
  await pressD()
  check(
    await e2e.poll(() => drawerState().then((sq) => !sq.open && !sq.infoOpen), 5000),
    'an Info closed by hand stays closed (the latch is the drawer\'s debt, not Info\'s state)'
  )

  srv.stop()
}

try {
  await main()
} catch (e) {
  console.log('ERROR', e?.stack ?? String(e))
  failed++
  try {
    await page?.screenshot({ path: path.join(shots, 'fsbox-error.png') })
  } catch {
    /* ignore */
  }
}
await browser.close().catch(() => {})
// the fake engine is deliberately absent here, so the bridge's engine polls fail
// by design; those network errors are not the app's
const realErrors = perr.filter((t) => !/ERR_INCOMPLETE_CHUNKED|ERR_CONNECTION_REFUSED|ERR_EMPTY_RESPONSE|favicon|sw\.js|manifest|Failed to load resource/i.test(t))
if (realErrors.length) console.log('page/console errors:', realErrors.slice(0, 6))
check(realErrors.length === 0, `no page errors (${realErrors.length})`, realErrors.slice(0, 3).join(' | ').slice(0, 200))
for (const d of [SCRATCH, AGENT]) fs.rmSync(d, { recursive: true, force: true })
console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
process.exit(failed ? 1 : 0)
