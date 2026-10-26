// Regression check: reverting a message moves pi's session leaf, and every view
// agrees that the reverted branch is gone.
//
// The webui contract (webui/src/lib/api.ts, Transcript.svelte, stores.ts):
//   POST /oc/session/{id}/revert {messageID} -> the updated session, whose
//   `revert: {messageID}` marks the boundary. The engine does NOT delete
//   messages; every client truncates its own view at that boundary, and the
//   next prompt prunes for real. So the bridge has to (a) move the leaf, (b)
//   hand back the marker, (c) stop projecting the abandoned branch, and (d)
//   clear the marker when the next turn starts.
//
// Moving the leaf: pi's RPC protocol has no leaf-move command (get_tree and
// get_entries are read-only), so pi-web drives tau's `navigateTree`
// (pi-tau-web-server/bin/tree.js), which sends the bundled extension's
// `/tau-tree-navigate <entryId>` and then VERIFIES with a second get_tree.
// Selecting a user-message entry moves the leaf to that entry's PARENT and
// hands back its text (the composer refill). The move is persisted by a
// `custom` / `tau:navigate-tree` marker entry appended at the target — pi
// re-derives the leaf from the last entry on load, which is why the marker has
// to exist and why the projection must follow the leaf chain instead of the
// whole file.
//
//   cd web && node .testbin/revert-check.mjs
//   PI_WEB_JS=<other build> node .testbin/revert-check.mjs   # prove it fails pre-fix
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { REPO_CWD, sessionSlug } from './harness.mjs'

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const TESTBIN = path.join(WEB, '.testbin')
const PI_WEB = process.env.PI_WEB_JS || path.join(WEB, 'pi-web.mjs')
const PORT = Number(process.env.REVERT_CHECK_PORT || 8133)
const BASE = `http://127.0.0.1:${PORT}`
const CWD = REPO_CWD
const SDIR = path.join(WEB, '.logs', 'revert-check')
const AGENT = path.join(SDIR, 'pi-agent')
const SESSIONS = path.join(AGENT, 'sessions')

let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
  if (!ok) failed++
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

fs.rmSync(SDIR, { recursive: true, force: true })
fs.mkdirSync(path.join(SESSIONS, sessionSlug(CWD)), { recursive: true })

if (!(await fetch(`${BASE}/healthz`).then(() => null).catch(() => true))) {
  console.log(`FAIL port ${PORT} is already answering — another pi-web/test server is up`)
  process.exit(1)
}

const child = spawn(
  process.execPath,
  [PI_WEB, '--port', String(PORT), '--host', '127.0.0.1', '--dir', AGENT, '--cwd', CWD],
  {
    cwd: WEB,
    env: {
      ...process.env,
      PATH: `${TESTBIN}:${process.env.PATH}`,
      FAKE_PI_PERSIST: '1',
      FAKE_PI_THINKING: '0',
      // last phase: force the reaper to close the child so the COLD projection
      // (file on disk, no live child) is what answers
      PI_WEB_REAP_INTERVAL_MS: '700',
      PI_WEB_IDLE_TTL_MS: '1500',
    },
    stdio: 'ignore',
    detached: true,
  },
)
const stop = () => {
  try {
    process.kill(-child.pid, 'SIGKILL') // the group we spawned, never a pattern
  } catch {
    /* already gone */
  }
}

const req = async (p, opt) => {
  try {
    const r = await fetch(BASE + p, opt)
    return { status: r.status, body: await r.json().catch(() => null) }
  } catch {
    return { status: 0, body: null }
  }
}
const post = (p, body) =>
  req(p, { method: 'POST', body: typeof body === 'string' ? body : JSON.stringify(body ?? {}), headers: { 'content-type': 'application/json' } })

const transcriptFile = () => {
  const out = []
  const walk = (d) => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, e.name)
      if (e.isDirectory()) walk(p)
      else if (e.name.endsWith('.jsonl')) out.push(p)
    }
  }
  try {
    walk(SESSIONS)
  } catch {
    /* none */
  }
  return out[0] ?? null
}
const readEntries = () => {
  const f = transcriptFile()
  if (!f) return []
  return fs
    .readFileSync(f, 'utf8')
    .trim()
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
}

async function waitIdle(sid, tries = 200) {
  for (let i = 0; i < tries; i++) {
    const st = await req('/oc/session/status')
    if (st.body?.[sid]?.type === 'idle') return true
    await sleep(100)
  }
  return false
}
const messages = async (sid) => {
  const r = await req(`/oc/session/${encodeURIComponent(sid)}/message`)
  return Array.isArray(r.body) ? r.body : []
}
const textOf = (m) => (m.parts ?? []).filter((p) => p.type === 'text').map((p) => p.text ?? '').join(' ')

async function main() {
  const created = await post('/oc/session', { cwd: CWD })
  const sid = created.body?.id
  check(!!sid, `session created (${created.status})`)
  await post(`/oc/session/${sid}/prompt_async`, { parts: [{ type: 'text', text: 'the question I regret asking' }] })
  check(await waitIdle(sid), 'the first turn finished')
  const before = await messages(sid)
  const userMsg = before.find((m) => m.role === 'user')
  check(before.length >= 2 && !!userMsg, `the transcript has a turn to undo (${before.length} message(s))`)
  const entriesBefore = readEntries().length
  check(entriesBefore >= 3, `its transcript is on disk (${entriesBefore} entries)`)

  // 1. the revert itself
  const rev = await post(`/oc/session/${sid}/revert`, { messageID: userMsg?.id })
  check(rev.status === 200, `POST /revert answers 200 (${rev.status} ${JSON.stringify(rev.body?.error ?? rev.body ?? '')})`)
  check(rev.body?.revert?.messageID === userMsg?.id, `the response carries the revert point (${JSON.stringify(rev.body?.revert)})`)

  // 2. the projection must follow the leaf: the abandoned branch is not history
  const after = await messages(sid)
  check(after.length < before.length, `the reverted branch is out of the transcript (${before.length} -> ${after.length} message(s))`)
  check(!after.some((m) => textOf(m).includes('the question I regret asking')), `the reverted user message is gone (${JSON.stringify(after.map((m) => textOf(m).slice(0, 24)))})`)
  check(!after.some((m) => m.role === 'assistant'), 'and so is its assistant reply')

  // 3. nothing was DELETED: pi is append-only, the move is a marker entry
  const entries = readEntries()
  const marker = entries.filter((e) => e.type === 'custom' && e.customType === 'tau:navigate-tree')
  check(entries.length > entriesBefore, `the transcript only GREW (${entriesBefore} -> ${entries.length} entries)`)
  check(marker.length === 1, `the leaf move is persisted by a tau:navigate-tree marker (${marker.length})`)
  check(
    (await messages(sid)).every((m) => JSON.stringify(m).includes('tau:navigate-tree') === false),
    'the marker is never projected as a transcript row',
  )

  // 4. the session object carries the revert point, so a reopened tab truncates
  const sess = await req(`/oc/session/${sid}`)
  check(sess.body?.revert?.messageID === userMsg?.id, `GET /oc/session/{id} reports the revert point (${JSON.stringify(sess.body?.revert)})`)

  // 5. bad input is a client error, not a 500
  const bogus = await post(`/oc/session/${sid}/revert`, { messageID: 'no-such-message' })
  check(bogus.status === 404 || bogus.status === 400, `reverting an unknown message id answers 4xx (${bogus.status})`)
  const empty = await post(`/oc/session/${sid}/revert`, {})
  check(empty.status === 400, `revert without a messageID answers 400 (${empty.status})`)

  // 6. busy is 409 SessionBusyError — the webui's isSessionBusy() aborts and
  // retries, so the shape is part of the contract
  await post(`/oc/session/${sid}/prompt_async`, { parts: [{ type: 'text', text: 'a turn to interrupt' }] })
  await sleep(150)
  const busy = await post(`/oc/session/${sid}/revert`, { messageID: userMsg?.id })
  check(busy.status === 409 && (busy.body?.name ?? busy.body?.error?.name) === 'SessionBusyError', `revert during a turn is 409 SessionBusyError (${busy.status} ${JSON.stringify(busy.body)})`)
  check(await waitIdle(sid), 'the interrupted turn finished')

  // 7. the next prompt prunes: the revert point must not outlive the turn
  const again = await post(`/oc/session/${sid}/prompt_async`, { parts: [{ type: 'text', text: 'what I meant to ask instead' }] })
  check(again.status === 200 || again.status === 202 || again.status === 204, `the follow-up prompt was accepted (${again.status})`)
  check(await waitIdle(sid), 'the follow-up turn finished')
  const sess2 = await req(`/oc/session/${sid}`)
  check(!sess2.body?.revert, `the revert point cleared on the next turn (${JSON.stringify(sess2.body?.revert)})`)
  const live = await messages(sid)
  check(live.some((m) => textOf(m).includes('what I meant to ask instead')), 'the new turn is in the transcript')

  // 8. COLD: let the reaper close the child, then read the file. The projection
  // must walk root->leaf, not dump the file, or the reverted branch comes back.
  for (let i = 0; i < 60; i++) {
    const hz = await req('/healthz')
    if (hz.body?.live === 0) break
    await sleep(300)
  }
  const hz = await req('/healthz')
  check(hz.body?.live === 0, `the child was reaped (live=${hz.body?.live}) — the next read is cold`)
  const cold = await messages(sid)
  check(!cold.some((m) => textOf(m).includes('the question I regret asking')), `the COLD transcript still excludes the reverted branch (${JSON.stringify(cold.map((m) => textOf(m).slice(0, 22)))})`)
  check(cold.some((m) => textOf(m).includes('what I meant to ask instead')), 'and still includes the turns after it')
}

for (let i = 0; i < 60; i++) {
  try {
    if ((await req('/healthz')).status === 200) break
  } catch {
    /* not up yet */
  }
  await sleep(200)
}

console.log('\n===== main =====')
try {
  await main()
} catch (e) {
  console.log('ERROR', e?.stack ?? String(e))
  failed++
}
stop()
await sleep(250)
console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
process.exit(failed ? 1 : 0)
