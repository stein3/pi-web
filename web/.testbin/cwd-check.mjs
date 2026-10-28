// Headless regression check: a session whose recorded workspace no longer exists.
//
// Workspaces get renamed and moved (a repo dir rename is enough), and pi's
// transcript header pins the cwd it was created in forever. tau refuses to spawn
// a child in a directory that is not there — pi-tau-web-server/bin/sessions.js:168
// `if (!fs.existsSync(this.cwd) …) throw new Error('Directory not found: …')` —
// and the bridge hands that header cwd straight to `liveManager.resume`
// (pi-web.mjs `sessionFor`: `cwd: readHeaderCwd(file) ?? PI_CWD`). So one rename
// makes the session unreadable AND unsendable: every route through `sessionFor`
// rejects, the sidebar row shows a workspace that does not exist, and a prompt is
// a 5xx. This check pins the fallback instead:
//
//   1. the row and the session object name a directory that EXISTS;
//   2. reading history does not die (200, not a 5xx);
//   3. a prompt still lands (the child really resumed and the turn streamed);
//   4. the fallback is the nearest EXISTING ANCESTOR of the dead path (the repo
//      moved, its parent did not), not blindly the bridge's default cwd;
//   5. a session whose header cwd is real is left exactly as it is;
//   6. a dead path with nothing real above it never resolves to the filesystem
//      root;
//   7. CREATING a session in a dead cwd answers 4xx naming the directory, not a
//      500 — the webui posts whatever the sidebar chip says.
//
// Self-contained: seeds a scratch pi dir, spawns pi-web with its own --cwd, kills
// the exact PID it started. No model, no real transcripts.
//
//   cd web && node .testbin/cwd-check.mjs
//   PI_WEB_JS=/path/to/pre-fix/pi-web.mjs node .testbin/cwd-check.mjs   # prove it fails
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { REPO_CWD, sessionSlug } from './harness.mjs'

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const PI_WEB = process.env.PI_WEB_JS || path.join(WEB, 'pi-web.mjs')
const PORT = Number(process.env.CWD_CHECK_PORT || 8135)
const BASE = `http://127.0.0.1:${PORT}`

const SDIR = path.join(WEB, '.logs', 'cwd-check')
const AGENT = path.join(SDIR, 'pi-agent')
const WORKDIR = path.join(SDIR, 'workdir') // what the bridge is started with
const MOVED = path.join(SDIR, 'moved') // still exists
const DEAD = path.join(MOVED, 'renamed-away') // the repo dir that got renamed
const FOREIGN = '/nope/deep/deeper' // nothing above it exists either
const isDir = (p) => {
  try {
    return fs.statSync(p).isDirectory()
  } catch {
    return false
  }
}

let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
  if (!ok) failed++
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const req = async (p, opt) => {
  const r = await fetch(BASE + p, opt).catch((e) => ({ fetchError: String(e) }))
  if (r.fetchError) return { status: 0, body: null, err: r.fetchError }
  return { status: r.status, body: await r.json().catch(() => null) }
}
const post = (p, body) =>
  req(p, { method: 'POST', body: JSON.stringify(body ?? {}), headers: { 'content-type': 'application/json' } })

/** pi's persisted shape, minimal: header + one user + one assistant message */
function seed(file, id, cwd, text) {
  const t = (n) => new Date(Date.parse('2026-01-01T00:00:00.000Z') + n).toISOString()
  const lines = [
    { type: 'session', version: 3, id, timestamp: t(0), cwd },
    {
      type: 'message', id: 'u1', parentId: null, timestamp: t(10),
      message: { role: 'user', timestamp: t(10), content: [{ type: 'text', text: text }], apiUrl: '', api: 'fake', provider: 'testprov', model: 'fake-1', usage: { input: 5, output: 2, cacheRead: 0, cacheWrite: 0, totalTokens: 7, cost: { total: 0 } } },
    },
    {
      type: 'message', id: 'a1', parentId: 'u1', timestamp: t(11),
      message: { role: 'assistant', timestamp: t(11), model: 'fake-1', provider: 'testprov', stopReason: 'stop', api: 'fake', apiUrl: '', content: [{ type: 'text', text: 'the earlier answer' }], usage: { input: 7, output: 3, cacheRead: 0, cacheWrite: 0, totalTokens: 10, cost: { total: 0 } } },
    },
  ]
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, lines.map((l) => JSON.stringify(l)).join('\n') + '\n')
}

let srv = null
const cleanup = () => {
  if (srv?.pid) {
    try {
      process.kill(-srv.pid, 'SIGKILL')
    } catch {}
    try {
      process.kill(srv.pid, 'SIGKILL')
    } catch {}
  }
}
process.on('exit', cleanup)

async function main() {
  fs.rmSync(SDIR, { recursive: true, force: true })
  fs.mkdirSync(path.join(AGENT, 'sessions'), { recursive: true })
  fs.mkdirSync(WORKDIR, { recursive: true })
  fs.mkdirSync(MOVED, { recursive: true }) // DEAD deliberately NOT created

  // A: the renamed repo dir. B: a control whose cwd is real. C: a path with no
  // real ancestor at all. All three live in slug dirs pi would have used.
  const A = '01a17b00-0000-7000-8000-00000000000a'
  const B = '01a17b00-0000-7000-8000-00000000000b'
  const C = '01a17b00-0000-7000-8000-00000000000c'
  seed(path.join(AGENT, 'sessions', sessionSlug(DEAD), `2026-01-01T00-00-00-000Z_${A}.jsonl`), A, DEAD, 'the message from before the rename')
  seed(path.join(AGENT, 'sessions', sessionSlug(WORKDIR), `2026-01-01T00-00-00-000Z_${B}.jsonl`), B, WORKDIR, 'a session in a live workspace')
  seed(path.join(AGENT, 'sessions', sessionSlug(FOREIGN), `2026-01-01T00-00-00-000Z_${C}.jsonl`), C, FOREIGN, 'a transcript from another machine')

  srv = spawn(process.execPath, [PI_WEB, '--port', String(PORT), '--host', '127.0.0.1', '--dir', AGENT, '--cwd', WORKDIR], {
    cwd: WEB,
    env: {
      ...process.env,
      PATH: `${path.join(WEB, '.testbin')}:${process.env.PATH}`,
      FAKE_PI_PERSIST: '1',
      FAKE_PI_THINKING: '0',
      PI_WEB_REAP_INTERVAL_MS: '3600000',
      PI_WEB_QUIET: '1',
    },
    stdio: 'ignore',
    detached: true,
  })
  for (let i = 0; i < 60; i++) {
    if ((await req('/healthz')).status === 200) break
    await sleep(200)
  }
  check((await req('/healthz')).status === 200, 'bridge is up')

  const rows = (await req('/api/history/sessions')).body ?? []
  const row = rows.find((r) => r.id === A)
  check(!!row, 'the session from the renamed workspace is still listed', rows.length ? `${rows.length} rows` : 'no rows')
  check(!!row && isDir(row.cwd), 'its sidebar row names a directory that EXISTS', row ? JSON.stringify(row.cwd) : 'no row')

  // ---- reading it must not die ------------------------------------------------
  const got = await req(`/oc/session/${A}`)
  check(got.status === 200, `reading its history answers 200, not tau's throw (${got.status})`, JSON.stringify(got.body)?.slice(0, 90))
  check(!!got.body?.cwd && isDir(got.body.cwd), "and the session object's cwd exists", JSON.stringify(got.body?.cwd))

  // ---- the fallback is the nearest EXISTING ANCESTOR, not PI_CWD --------------
  check(got.body?.cwd === MOVED, `the fallback is the nearest existing ancestor, not the bridge default (want ${MOVED})`, JSON.stringify(got.body?.cwd))

  // ---- and sending still works -----------------------------------------------
  const pr = await post(`/oc/session/${A}/prompt_async`, { parts: [{ type: 'text', text: 'send after the rename' }] })
  check(pr.status === 204, 'a prompt lands (204), not the 5xx a dead cwd used to give', JSON.stringify(pr.body)?.slice(0, 120))
  let idle = false
  for (let i = 0; i < 120; i++) {
    const st = await req('/oc/session/status')
    if (st.body?.[A]?.type === 'idle') {
      idle = true
      break
    }
    await sleep(100)
  }
  check(idle, 'the resumed child reached idle')
  const after = await req(`/oc/session/${A}/message`)
  const all = JSON.stringify(after.body ?? '')
  check(all.includes('Hello streamed world'), 'the turn really ran in the resumed session', all.includes('Hello streamed world') ? '' : 'no streamed reply')

  // ---- a real header cwd is never touched ------------------------------------
  const ctrl = await req(`/oc/session/${B}`)
  check(ctrl.status === 200 && ctrl.body?.cwd === WORKDIR, `a session with a live header cwd keeps it exactly (${JSON.stringify(ctrl.body?.cwd)})`)

  // ---- nothing real above it: never the filesystem root ----------------------
  const fr = await req(`/oc/session/${C}`)
  check(fr.status === 200 && isDir(fr.body?.cwd) && fr.body.cwd !== '/' && fs.existsSync(fr.body.cwd), `a foreign path never resolves to / (got ${JSON.stringify(fr.body?.cwd)})`)

  // ---- creating in a dead cwd: a 4xx that names the directory, not a 500 ----
  const bad = await post('/oc/session', { cwd: DEAD })
  check(bad.status >= 400 && bad.status < 500, `creating a session in a dead cwd answers 4xx (pre-fix: ${bad.status})`, JSON.stringify(bad.body)?.slice(0, 120))
  check(/renamed-away|not found|does not exist|no such/i.test(JSON.stringify(bad.body ?? '')), 'and the answer names the directory', JSON.stringify(bad.body)?.slice(0, 120))

  cleanup()
  console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
  process.exit(failed ? 1 : 0)
}

main().catch((e) => {
  console.error('cwd-check crashed:', e)
  cleanup()
  process.exit(1)
})
