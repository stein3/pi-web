// Headless check: pi-style workspaces in the nopencode webui, against pi-web
// running the fake pi. Covers workspace group headers, the tab-bar chip, and
// the new-session file picker (chips + path type-ahead → pending tab → real
// session born with the picked cwd).
//
//   BASE_URL=http://127.0.0.1:8126 node .testbin/ws-check.mjs
import path from 'node:path'
import { loadE2E, browserSkip, shotsDir, REPO } from './harness.mjs'

let e2e
try {
  e2e = await loadE2E()
} catch (e) {
  console.log(e.message)
  process.exit(e.skip ? 2 : 1)
}
const { launchBrowser, BASE, sleep, poll } = e2e

const shots = shotsDir()
let browser
try {
  browser = await launchBrowser()
} catch (e) {
  console.log(browserSkip(e) ?? e?.stack ?? String(e))
  process.exit(browserSkip(e) ? 2 : 1)
}

// the workspace this check runs in is the repo itself, so nothing here hard-
// codes an operator's directory layout
const REPO_NAME = path.basename(REPO)
const TYPE_AHEAD = path.join(path.dirname(REPO), REPO_NAME.slice(0, 3))
const page = await browser.newPage({ viewport: { width: 1280, height: 800 } })
const errors = []
page.on('pageerror', (e) => errors.push(String(e)))
page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()) })
page.on('response', (r) => { if (r.status() >= 400) errors.push(`${r.status()} ${r.url()}`) })

let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}

try {
  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 15000 })
  await sleep(2500)
  await page.screenshot({ path: shots + '/ws-sidebar.png' })

  // 1. sessions grouped under workspace headers (tau's project headers)
  const heads = page.locator('aside .wshead')
  const headCount = await heads.count()
  check(headCount >= 2, `workspace headers rendered (${headCount})`)
  const headText = (await heads.allInnerTexts()).join('|').toLowerCase()
  check(headText.includes('finance'), `header names a known workspace (${headText.slice(0, 120)})`)

  // 2. tab bar shows the active session's workspace chip
  const chips = page.locator('.tabbar .ws')
  check((await chips.count()) >= 1, 'tab bar workspace chip present')
  const chipTitle = (await chips.first().getAttribute('title')) || ''
  check(chipTitle.startsWith('/'), `chip tooltip is a path (${chipTitle})`)

  // 3. collapse / expand a group
  const rowsBefore = await page.locator('aside .item').count()
  await heads.first().click()
  await sleep(200)
  const rowsCollapsed = await page.locator('aside .item').count()
  check(rowsCollapsed < rowsBefore, `collapse hides rows (${rowsBefore} → ${rowsCollapsed})`)
  await heads.first().click()
  await sleep(200)
  check((await page.locator('aside .item').count()) === rowsBefore, 'expand restores rows')

  // 4. new-session picker: opens, offers chips, completes paths
  await page.locator('.tabbar .add').click()
  const dialog = page.locator('[role="dialog"][aria-label="New session"]')
  await poll(() => dialog.count().then((n) => n === 1), 3000)
  await sleep(400)
  check((await dialog.locator('.chip').count()) > 0, 'picker shows project chips')

  await page.locator('#ws-cwd').fill(TYPE_AHEAD)
  await poll(
    async () => (await dialog.locator('.acrow').allInnerTexts()).join('|').includes(REPO_NAME),
    3000,
  )
  const acText = (await dialog.locator('.acrow').allInnerTexts()).join('|')
  check(acText.includes(REPO_NAME), `type-ahead lists dirs (${acText.replace(/\n/g, ' ').slice(0, 90)})`)
  await page.screenshot({ path: shots + '/ws-picker.png' })

  // accept the highlighted completion (↓ then ↵), then ↵ starts the session
  await page.keyboard.press('ArrowDown')
  await page.keyboard.press('Enter')
  const accepted = await page.locator('#ws-cwd').inputValue()
  check(accepted === `${REPO}/`, `completion accepted (${accepted})`)
  await page.keyboard.press('Enter')
  await poll(() => dialog.count().then((n) => n === 0), 3000)

  // 5. the pending tab carries the picked workspace
  const activeWs = page.locator('.tab.active .ws')
  check((await activeWs.count()) === 1, 'pending tab has a workspace chip')
  check((await activeWs.first().innerText()) === REPO_NAME, 'chip shows picked dir')

  // 6. first message realizes the session with that cwd
  const input = page.locator('textarea:visible, [contenteditable="true"]:visible').first()
  await input.click()
  await input.fill('Say exactly: WS-OK')
  await page.keyboard.press('Enter')
  const gotReply = await poll(async () => (await page.locator('body').innerText()).includes('WS-OK'), 8000)
  check(gotReply, 'transcript shows the prompt/reply')

  // tool timing: the fake turn runs bash AND read (pi's real event order —
  // assistant message_end BEFORE tool_execution_*); pi-web must complete both
  // parts live so each tool row renders a `…ms` duration badge. A missing
  // read badge here is exactly the live-completion bug (tool events keyed off
  // the cleared cur byCall map).
  const gotBadges = await poll(async () => {
    const cards = page.locator('.toolcard summary')
    const n = await cards.count()
    if (n < 2) return false
    const badges = await page.locator('.toolcard summary .badge').allInnerTexts()
    return badges.filter((b) => /\d+ms/.test(b)).length >= 2
  }, 5000)
  check(gotBadges, 'tool duration badges render for bash AND read (…ms)')
  await sleep(500)

  const live = await page.request.get(BASE + '/oc/session').then((r) => r.json())
  const born = live.find((s) => s.cwd === REPO)
  check(!!born, 'session created with the picked cwd')
  await page.screenshot({ path: shots + '/ws-final.png' })
} catch (e) {
  console.log('ERROR', e.message)
  failed++
  await page.screenshot({ path: shots + '/ws-error.png' }).catch(() => {})
}

console.log('console/page errors:', errors.length ? errors.slice(0, 8) : 'none')
if (errors.length) failed += errors.length
await browser.close()
process.exit(failed ? 1 : 0)
