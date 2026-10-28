// Regression check: forking a session produces a SECOND transcript that carries
// the history up to the fork point, and never touches the one it came from.
//
// The webui contract (webui/src/lib/api.ts forkSession, Transcript.svelte
// forkFrom, commands.ts /fork): POST /oc/session/{id}/fork {messageID?} returns
// the new session (id + title); the engine copies everything BEFORE that message
// and titles the fork itself ("… (fork #N)"); the client opens a new tab keyed by
// the returned id and prefills its composer.
//
// pi models this as SessionManager.forkFrom (static): a new `<ts>_<newId>.jsonl`
// written `wx`, a header with `parentSession: <source path>` and the target cwd,
// then the copied entries. Its RUNTIME fork (rpc `fork`/`clone`) additionally
// REBINDS the live child to the new file — fine for the TUI, which switches into
// the fork, wrong for a UI that keeps the old tab open: the old tab's public uuid
// would go on writing the new transcript. So pi-web forks the FILE and leaves the
// child alone. This check asserts exactly that (the "old transcript did not move"
// assertions are the whole point).
//
//   cd web && node .testbin/fork-check.mjs
//   PI_WEB_JS=<other build> node .testbin/fork-check.mjs   # prove it fails pre-fix
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { REPO_CWD, sessionSlug } from './harness.mjs'

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const TESTBIN = path.join(WEB, '.testbin')
const PI_WEB = process.env.PI_WEB_JS || path.join(WEB, 'pi-web.mjs')
const PORT = Number(process.env.FORK_CHECK_PORT || 8134)
const BASE = `http://127.0.0.1:${PORT}`
const CWD = REPO_CWD
const SDIR = path.join(WEB, '.logs', 'fork-check')
const AGENT = path.join(SDIR, 'pi-agent')
const SESSIONS = path.join(AGENT, 'sessions')

let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
  if (!ok) failed++
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

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
      PI_WEB_REAP_INTERVAL_MS: '3600000',
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
const patch = (p, body) =>
  req(p, { method: 'PATCH', body: JSON.stringify(body ?? {}), headers: { 'content-type': 'application/json' } })

const filesFor = (id) => {
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
  return out.filter((f) => path.basename(f).endsWith(`_${id}.jsonl`))
}
const entriesOf = (id) => {
  const f = filesFor(id)[0]
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
const textsOf = (id) =>
  entriesOf(id)
    .filter((e) => e.type === 'message')
    .map((e) => (e.message?.content ?? []).filter((b) => b?.type === 'text').map((b) => b.text ?? '').join(' '))

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
  // one session, two turns, so a fork has a real fork point
  const created = await post('/oc/session', { cwd: CWD })
  const sid = created.body?.id
  check(!!sid && UUID.test(sid), `session created with pi's uuid (${JSON.stringify(sid)})`)
  await post(`/oc/session/${sid}/prompt_async`, { parts: [{ type: 'text', text: 'first thing on the record' }] })
  check(await waitIdle(sid), 'turn 1 finished')
  await post(`/oc/session/${sid}/prompt_async`, { parts: [{ type: 'text', text: 'the message I want to fork at' }] })
  check(await waitIdle(sid), 'turn 2 finished')
  const msgs = await messages(sid)
  const users = msgs.filter((m) => m.role === 'user')
  check(users.length >= 2, `the session has two user messages (${users.length})`)
  const forkAt = users[1]?.id

  // 1. a whole-session fork
  const all = await post(`/oc/session/${sid}/fork`, {})
  check(all.status === 200, `POST /fork answers 200 (${all.status} ${JSON.stringify(all.body?.error ?? '')})`)
  const fid = all.body?.id
  check(!!fid && UUID.test(fid) && fid !== sid, `the fork is a NEW session id (${JSON.stringify(fid)})`)
  check(typeof all.body?.title === 'string' && /\(fork #1\)/.test(all.body?.title ?? ''), `the engine titles the fork (${JSON.stringify(all.body?.title)})`)

  // 2. pi's own forkFrom shape: header points at the source, and the history is there
  const fEntries = entriesOf(fid)
  const fHeader = fEntries.find((e) => e.type === 'session')
  const oldFile = filesFor(sid)[0]
  check(fEntries.length > 1, `the fork transcript exists (${fEntries.length} entries)`)
  check(fHeader?.parentSession === oldFile, `its header says parentSession: <the source file> (${JSON.stringify(fHeader?.parentSession)})`)
  check(!!fid && fHeader?.id === fid, `its header id is the published id (${JSON.stringify(fHeader?.id)})`)
  check(fHeader?.version === 3 && fHeader?.cwd === CWD, `pi v3 header for the same cwd (version=${fHeader?.version} cwd=${JSON.stringify(fHeader?.cwd)})`)
  const srcTexts = textsOf(sid)
  check(!!fid && srcTexts.length > 0 && srcTexts.every((t) => textsOf(fid).includes(t)), `all of the parent's messages are in the fork (${textsOf(fid).length} vs ${srcTexts.length})`)

  // 3. the fork point is EXCLUSIVE of the selected message
  const scoped = await post(`/oc/session/${sid}/fork`, { messageID: forkAt })
  check(scoped.status === 200, `POST /fork {messageID} answers 200 (${scoped.status})`)
  const fid2 = scoped.body?.id
  check(!!fid2 && fid2 !== fid && fid2 !== sid, `a scoped fork is another new session (${JSON.stringify(fid2)})`)
  const t2 = textsOf(fid2)
  check(t2.some((t) => t.includes('first thing on the record')), `the scoped fork keeps what came before (${JSON.stringify(t2.map((t) => t.slice(0, 20)))})`)
  check(!!fid2 && t2.length > 0 && !t2.some((t) => t.includes('the message I want to fork at')), `and EXCLUDES the forked message and its reply (${t2.length} message(s) in the scoped fork)`)
  check(/\(fork #2\)/.test(scoped.body?.title ?? ''), `the fork counter keeps counting (${JSON.stringify(scoped.body?.title)})`)

  // 4. the parent did not move: this is the whole reason not to use pi's rpc fork
  const before = { entries: entriesOf(sid).length, file: oldFile }
  await post(`/oc/session/${fid2}/prompt_async`, { parts: [{ type: 'text', text: 'a turn inside the fork' }] })
  check(await waitIdle(fid2), 'the fork ran its own turn')
  const nowEntries = entriesOf(sid)
  check(nowEntries.length === before.entries, `the PARENT transcript is untouched (${before.entries} -> ${nowEntries.length} entries)`)
  check(!textsOf(sid).some((t) => t.includes('a turn inside the fork')), "the fork's turn did not land in the parent")
  check(textsOf(fid2).some((t) => t.includes('a turn inside the fork')), 'and it did land in the fork')

  // 5. two sessions, two ids, one child each, no handle leaking
  const live = (await req('/oc/session')).body ?? []
  const ids = (Array.isArray(live) ? live : []).map((x) => x.id)
  check(ids.includes(sid) && ids.includes(fid2) && !ids.some((x) => /^tau_/.test(x)), `both sessions are addressed by uuid (${JSON.stringify(ids)})`)
  const rows = (await req('/api/history/sessions')).body ?? []
  const rowIds = (Array.isArray(rows) ? rows : []).map((r) => r.id)
  check(rowIds.filter((x) => x === sid || x === fid || x === fid2).length === 3, `three sidebar rows, one per session (${JSON.stringify(rowIds)})`)

  // 6. bad input is a client error
  const bogus = await post('/oc/session/nope/fork', {})
  check(bogus.status === 404, `forking an unknown session answers 404 (${bogus.status})`)
  const badMsg = await post(`/oc/session/${sid}/fork`, { messageID: 'no-such-message' })
  check(badMsg.status === 404 || badMsg.status === 400, `forking at an unknown message answers 4xx (${badMsg.status})`)

  // 7. the fork must be a real session for every other route: rename sticks, and
  // the auto-tiler must not rename over the engine's "(fork #N)" title
  const renamed = await patch(`/oc/session/${fid}`, { title: 'named by hand' })
  check(renamed.status === 200, `the fork accepts a rename (${renamed.status})`)
  await post(`/oc/session/${fid}/prompt_async`, { parts: [{ type: 'text', text: 'a turn that might tempt a rename' }] })
  check(await waitIdle(fid), 'the first fork ran a turn')
  const rowF = (Array.isArray((await req('/api/history/sessions')).body) ? (await req('/api/history/sessions')).body : []).find((r) => r.id === fid)
  check(rowF?.title === 'named by hand', `the rename stuck through a turn (${JSON.stringify(rowF?.title)})`)
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
