// Headless check: real webui (dist) against pi-web on :8125 with the fake pi.
import { launchBrowser, BASE, sleep, poll } from '/workspace/opencode/e2e/helpers/setup.mjs'

const shots = '/workspace/pi-local/web/.logs'
const browser = await launchBrowser()
const page = await browser.newPage({ viewport: { width: 1280, height: 800 } })
const errors = []
page.on('pageerror', (e) => errors.push(String(e)))
page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()) })

let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}

try {
  await page.goto(BASE + '/', { waitUntil: 'domcontentloaded', timeout: 15000 })
  await sleep(2000)
  await page.screenshot({ path: shots + '/ui-boot.png' })

  // app chrome present
  const hasComposer = await poll(() => page.locator('textarea, [contenteditable="true"]').count().then((n) => n > 0), 5000)
  check(hasComposer, 'composer rendered')

  const hasSidebar = await poll(() => page.locator('aside, .sidebar, [class*="sidebar"]').count().then((n) => n > 0), 3000)
  check(hasSidebar, 'sidebar rendered')

  // new session via the UI: find the new-session button
  const newBtn = page.locator('button').filter({ hasText: /new session|new chat|\+/i }).first()
  if (await newBtn.count()) {
    await newBtn.click().catch(() => {})
    await sleep(800)
  }

  // type a prompt and send (skip dormant hidden panes — webui keeps them mounted)
  const input = page.locator('textarea:visible, [contenteditable="true"]:visible').first()
  await input.click()
  await input.fill('Say exactly: UI-STREAM-OK')
  await page.keyboard.press('Enter')
  await sleep(3000)
  await page.screenshot({ path: shots + '/ui-sent.png' })

  // streamed reply appears in the transcript
  const gotReply = await poll(async () => {
    const txt = await page.locator('body').innerText()
    return txt.includes('Hello streamed world.') || txt.includes('UI-STREAM-OK')
  }, 8000)
  check(gotReply, 'transcript shows prompt/reply text')

  // idle: composer enabled again (busy cleared)
  const idle = await poll(async () => {
    const body = await page.locator('body').innerText()
    return !/working|streaming|busy/i.test(body)
  }, 5000)
  check(idle, 'session went idle (no busy indicator)')

  await page.screenshot({ path: shots + '/ui-final.png' })
} catch (e) {
  console.log('ERROR', e.message)
  failed++
  await page.screenshot({ path: shots + '/ui-error.png' }).catch(() => {})
}

console.log('console/page errors:', errors.length ? errors.slice(0, 8) : 'none')
await browser.close()
process.exit(failed ? 1 : 0)
