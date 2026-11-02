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
//   node test/fork-check.mjs
//   PI_WEB_JS=<other build> node test/fork-check.mjs   # prove it fails pre-fix
import { spawn } from 'node:child_process'
import fs, { realpathSync } from 'node:fs'
import path from 'node:path'


import { REPO_CWD, TESTBIN, WEB, sessionSlug } from './harness.mjs'
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

// The bridge is restartable: one case (fork a session that was reverted while its
// tab was open) needs the idle reaper to have ACTUALLY run, and racing the default
// 20-minute TTL would make the result about timing rather than behaviour. Same
// --dir, so the transcripts survive the restart; only the knobs change.
let child = null
const startBridge = async (extraEnv = {}) => {
  child = spawn(
    process.execPath,
    [PI_WEB, '--port', String(PORT), '--host', '127.0.0.1', '--dir', AGENT, '--cwd', CWD],
    {
      cwd: WEB,
      env: {
        ...process.env,
        PATH: `${TESTBIN}:${process.env.PATH}`,
        FAKE_PI_PERSIST: '1',
        FAKE_PI_THINKING: '0',
        // Case 7b forks a session whose TAB was closed, and the only way to get
        // there over HTTP is the idle reaper. Keep the TTL above one turn (the fake
        // pi streams ~1 char/50ms) so it closes IDLE tabs between phases but never
        // mid-turn: sections 1-6 need their children attached.
        PI_WEB_REAP_INTERVAL_MS: process.env.FORK_CHECK_REAP_MS ?? '300',
        // Above the fake pi's turn ceiling (it streams ~1 char per 50 ms), so
        // "idle" means the CHILD is idle, not that a long turn went quiet. Sections
        // 1-6 need children attached; only 7b wants one closed, and `reapAll()` waits.
        PI_WEB_IDLE_TTL_MS: process.env.FORK_CHECK_IDLE_TTL_MS ?? '2500',
        ...extraEnv,
      },
      stdio: 'ignore',
      detached: true,
    },
  )
  for (let i = 0; i < 60; i++) {
    try {
      if ((await req('/healthz')).status === 200) return true
    } catch {
      /* not up yet */
    }
    await sleep(200)
  }
  return false
}

const stopChild = () => {
  if (!child?.pid) return
  try {
    process.kill(-child.pid, 'SIGKILL') // the group we spawned, never a pattern
  } catch {
    /* already gone */
  }
  child = null
}

const stop = stopChild



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
  // pi names every transcript `<iso-ts>_<uuid>.jsonl`, fork ids included — the uuid
  // is v7 for a bridge-written fork and v4 for one pi made, so match ANY uuid-ish
  // tail. Anchoring on `_${id}.jsonl` silently returned [] for a fork, and every
  // assertion that reads the file (including the pi projection probe) then judged
  // `undefined` and "passed" as zero messages.
  return out.filter((f) => {
    const base = path.basename(f, '.jsonl')
    const tail = base.slice(base.lastIndexOf('_') + 1)
    return tail === id
  })
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
// root -> leaf walk, the same shape pi's buildSessionPath() does — including its
// index, which keys EVERY entry by `entry.id`, missing id included (that is how a
// fork of a file-renamed session ends up cyclic). The bound is what lets a cycle
// be reported instead of hanging the check.
const chainOf = (id) => {
  const list = entriesOf(id).filter((e) => e.type !== 'session')
  if (!list.length) return { cycle: false, out: [], missing: true }
  const byId = new Map()
  for (const e of list) byId.set(e?.id, e)
  const leaf = list[list.length - 1]
  if (!leaf) return { cycle: false, out: [], missing: true }
  const out = []
  const seen = new Set()
  let cur = leaf
  while (cur && !seen.has(cur?.id)) {
    if (out.length > list.length + 1) return { cycle: true, out }
    seen.add(cur.id)
    out.push(cur)
    cur = cur.parentId != null ? byId.get(cur.parentId) ?? null : null
  }
  return { cycle: false, out: out.reverse() }
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

/**
 * Ask REAL pi to project a transcript (core/session-manager.js). The bridge's own
 * leafChain() is cycle-safe, so a bridge-only assertion cannot see the failure
 * that matters: pi's buildSessionPath() has no cycle guard and throws (or hangs)
 * on a duplicated entry id, which is what makes the resumed child unreadable.
 * pi is installed globally (the container image and this box both put it under
 * the node prefix), so resolve it from there rather than by package name — web/
 * only depends on pi-tau-web-server.
 * Returns { messages } or { error }.
 */
const PI_PKG =
  process.env.PI_CODING_AGENT_PKG ||
  path.join(path.dirname(realpathSync(process.execPath)), '..', 'lib', 'node_modules', '@earendil-works', 'pi-coding-agent')
const filesFor0 = (f) => f
const rawEntries = (f) => {
  try {
    return fs.readFileSync(f, 'utf8').split('\n').filter((l) => l.trim()).map((l) => JSON.parse(l))
  } catch {
    return []
  }
}

const piRead = (file) =>
  new Promise((done) => {
    const probe = path.join(SDIR, 'pi-read-probe.mjs')
    fs.writeFileSync(
      probe,
      `const { SessionManager } = await import(${JSON.stringify(path.join(PI_PKG, 'dist', 'core', 'session-manager.js'))});
const sm = SessionManager.open(process.argv[2], undefined, process.cwd());
try {
  console.log(JSON.stringify({ messages: sm.buildSessionProjection().messages.length }));
} catch (e) {
  console.log(JSON.stringify({ error: String(e?.message ?? e) }));
}
`,
    )
    console.log(`PROBE file=${JSON.stringify(file)}`)
    const p = spawn(process.execPath, [probe, file], {
      cwd: WEB,
      env: process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: true,
    })
    let out = ''
    p.stdout.on('data', (d) => (out += d))
    p.stderr.on('data', (d) => (out += d))
    p.on('error', () => done({ error: `cannot run pi's session-manager (${PI_PKG})` }))
    const timer = setTimeout(() => {
      try {
        process.kill(-p.pid, 'SIGKILL')
      } catch {}
      done({ error: 'pi hung reading the transcript' })
    }, 20000)
    timer.unref?.()
    p.on('close', () => {
      clearTimeout(timer)
      const last = out.trim().split('\n').pop() ?? ''
      try {
        done(JSON.parse(last))
      } catch {
        done({ error: last || 'pi printed nothing' })
      }
    })
  })

/**
 * Wait for the idle reaper to close every child — the stand-in for "the operator
 * closed the tab", which has no HTTP call in this webui. Never DELETE the session
 * to get there: that trashes the transcript.
 */
async function reapAll() {
  for (let i = 0; i < 160; i++) {
    const h = await req('/healthz')
    if ((h.body?.live ?? 0) === 0) return true
    await sleep(250)
  }
  return false
}

// The three things a fork file must satisfy BEFORE anything opens it.
const checkForkFile = (id, label) => {
    const counts = {}
    for (const e of entriesOf(id)) if (e.type !== 'session') counts[String(e?.id)] = (counts[String(e?.id)] ?? 0) + 1
    const dup = Object.entries(counts).filter(([, n]) => n > 1)
    check(dup.length === 0, `${label}: every entry id is unique in the file`, JSON.stringify(dup))
    const ch = chainOf(id)
    check(!ch.cycle && !ch.missing, `${label}: its entry chain terminates`, ch.cycle ? `cycle after ${ch.out.length} entries` : ch.missing ? 'no transcript' : `${ch.out.length} entries`)
    return piRead(filesFor(id)[0])
  }

async function main() {
  // A transcript whose rename went through the FILE-side fallback (no live child
  // to ask, so PATCH title appended a session_info entry with NO id of its own).
  // pi indexes that under `undefined` (_buildIndex, core/session-manager.js:732), so
  // the fork has to carry it without letting its OWN title entry collide into the
  // same key. Seeded before the bridge starts so the assertions are deterministic.
  const SEED = '01a17c00-0000-7000-8000-00000000000s'
  fs.mkdirSync(path.join(SESSIONS, sessionSlug(CWD)), { recursive: true })
  fs.writeFileSync(
    path.join(SESSIONS, sessionSlug(CWD), `2026-01-01T00-00-00-000Z_${SEED}.jsonl`),
    [
      { type: 'session', version: 3, id: SEED, timestamp: '2026-01-01T00:00:00.000Z', cwd: CWD },
      { type: 'model_change', id: 'seedmc', parentId: null, timestamp: '2026-01-01T00:00:01.000Z', provider: 'testprov', modelId: 'fake-1' },
      {
        type: 'message', id: 'seedu1', parentId: 'seedmc', timestamp: '2026-01-01T00:00:02.000Z',
        message: { role: 'user', timestamp: '2026-01-01T00:00:02.000Z', content: [{ type: 'text', text: 'a prompt in a renamed session' }], api: 'fake', apiUrl: '', provider: 'testprov', model: 'fake-1', usage: { input: 5, output: 2, cacheRead: 0, cacheWrite: 0, totalTokens: 7, cost: { total: 0 } } },
      },
      // What appendSessionInfo writes TODAY: a name, and NO id. pi indexes that
      // under `undefined` (core/session-manager.js:732-745) and the bridge writes
      // its own title entry with an id — so the fork must carry the copied entry's
      // identity forward, not drop it or collide with it.
      { type: 'session_info', parentId: 'seedu1', timestamp: '2026-01-01T00:00:03.000Z', name: 'renamed while closed' },
    ]
      .map((l) => JSON.stringify(l))
      .join('\n') + '\n',
  )

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

  // 1b. THE FORK MUST RENDER. Judged on the COLD file, immediately: asking
  // /message resumes a child, and that child appends to the fork, so it cannot be
  // used to judge what the fork was WRITTEN as — and a broken chain is invisible
  // from the bridge alone, because pi-web's own leafChain() has a seen-set where
  // pi's buildSessionPath() does not.
  checkForkFile(fid, 'the whole-session fork')

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

  // 7. the fork must RENDER, not just exist: clicking its sidebar row opens a
  // transcript, and the copied entry chain stays a tree pi can walk.
  //
  // A fork copies entry ids VERBATIM — correct, because pi's `firstKeptEntryId`
  // and a revert marker's `targetId` address them — so any id (or MISSING id) the
  // parent holds lands in the fork too. pi-web wrote two entries of its own with
  // ids it never collision-checked against the transcript: the fork's title entry
  // (`id: "forkname"`) and the file-side rename fallback (`appendSessionInfo`
  // writes NO id, and pi indexes that as `undefined`). Copied into a chain that
  // already holds the same key, `parentId` resolves back to an earlier entry with
  // that key — a CYCLE. pi's buildSessionPath() has no cycle guard and pushes
  // forever (`RangeError: Invalid array length`, core/session-manager.js:140);
  // the bridge's own leafChain() HAS a seen-set, so it silently truncates to a
  // couple of entries instead — which is why the tab shows "0 msgs" and the new-
  // session page rather than an error, and the webui's placeholder tab then
  // closes itself. Everything here judges the COLD file: asking /message resumes a
  // child, and that child appends to the fork.

  // 7a. the parent was RENAMED while closed: appendSessionInfo's id-less entry is
  // in the chain, and the fork's own title entry collides with it.
  const seedFork = await post(`/oc/session/${SEED}/fork`, {})
  const seedFid = seedFork.body?.id
  check(seedFork.status === 200 && !!seedFid, `forking a file-renamed session answers 200 (${seedFork.status})`)
  const seedPi = await checkForkFile(seedFid, 'fork of a file-renamed session')
  check(seedPi.messages > 0, `pi itself projects that fork (${JSON.stringify(seedPi)})`, seedPi.error ?? '')

  // 7b. revert, THEN click away and fork from the SIDEBAR — exactly what produced
  // three unreadable "workspace cleanup pi-web (fork #N)" rows on a real box. The
  // tab is closed, so no child is live: the bridge forks the COLD file, whose last
  // entry is the navigation marker. Reaping first is not cosmetic — asking /message
  // keeps a child attached that then appends to whatever it resumes.
  // The webui has no "close tab" HTTP call, so the way a fork becomes COLD is the
  // idle reaper — hence the short TTL this bridge starts with. Never restart the
  // bridge to get there: a fresh process has no child and no reap alias map, and
  // that is a DIFFERENT situation from "the reaper closed it" (see the id rules).
  // 7a-2. revert to a message that is NOT first, then fork COLD — the shape of the
  // reported bug ("workspace cleanup pi-web (fork #1/#2/#3)" rows that open as
  // "0 msgs" and close themselves). The active path here is NOT empty: pi's marker
  // is parented at a real entry, so a correct fork must copy everything and render.
  // Revert to the LAST user message: pi's marker is then parented at that turn's
  // assistant reply, so the file ENDS at an orphan and pi reads the whole transcript
  // back as empty. This is what makes a later fork of it unreadable.
  const allUsers = (await messages(sid)).filter((m) => m.role === 'user')
  const midUser = allUsers[allUsers.length - 1]
  const rev2 = await post(`/oc/session/${sid}/revert`, { messageID: midUser?.id })
  check(rev2.status === 200, `a revert to the SECOND user message answers 200 (${rev2.status})`)
  await reapAll()
  const coldFork = await post(`/oc/session/${sid}/fork`, {})
  const coldFid = coldFork.body?.id
  check(coldFork.status === 200 && !!coldFid, `the COLD sidebar fork of a reverted session answers 200 (${coldFork.status})`, JSON.stringify(coldFork.body?.error ?? ''))
  if (coldFid) {
    const coldPi = await checkForkFile(coldFid, 'the cold fork of a mid-session revert')
    check(coldPi.messages > 0, `pi itself projects it (${JSON.stringify(coldPi)})`, coldPi.error ?? '')
    const coldMsgs = await messages(coldFid)
    check(coldMsgs.length > 0, `and clicking its row renders messages (${coldMsgs.length})`, coldMsgs.length ? '' : 'the resumed child has no transcript to project')
    const coldRow = (await req('/api/history/sessions')).body?.find((r) => r.id === coldFid)
    check(!!coldRow && coldRow.message_count > 0, `its sidebar row is not "0 msgs" (${JSON.stringify(coldRow?.message_count)})`)
  }

  const rUser = (await messages(sid)).find((m) => m.role === 'user')
  const rev = await post(`/oc/session/${sid}/revert`, { messageID: rUser?.id })
  // A revert appends a `custom`/tau:navigate-tree marker and moves the CHILD's
  // leaf; the FILE still ends with that marker. Assert it, or the case below is
  // silently forking something else.
  // The markers accumulate — a revert never removes one — so assert on the TAIL.
  const markers = entriesOf(sid).filter((e) => e.customType === 'tau:navigate-tree')
  check(markers.length >= 1, `the parent holds navigation markers (${markers.length})`, JSON.stringify(markers.map((m) => [m.id, m.parentId])))
  check(rev.status === 200, `a revert on the parent answers 200 (${rev.status})`)
  const reaped = await reapAll(sid)
  check(reaped, 'the tab was closed and its child reaped (the sidebar fork is a COLD fork)', `live=${JSON.stringify((await req('/healthz')).body?.live)}`)
  const revFork = await post(`/oc/session/${sid}/fork`, {})
  const revFid = revFork.body?.id
  // pi's OWN contract decides this one. A revert of the FIRST user message leaves a
  // navigation marker whose parentId is null (resetLeaf -> appendCustomEntry parents
  // at leafId, core/agent-session.js:3364 + session-manager.js:901-911), and pi's
  // buildSessionPath walks from `_buildIndex`'s leaf — the LAST non-header entry —
  // stopping at the first null parentId. So that transcript projects ZERO messages,
  // in pi itself, for pi's own CLI fork too (forkFrom copies every entry verbatim).
  // The bridge must not pretend otherwise and must not write a truncated "fix".
  const srcPi = await piRead(filesFor(sid)[0])
  const blankAtSource = srcPi.messages === 0
  check(
    blankAtSource ? revFork.status === 502 : revFork.status === 200 && !!revFid,
    `forking a session pi itself reads as ${srcPi.messages} message(s) answers ${blankAtSource ? '502 with the reason' : '200'} (${revFork.status})`,
    JSON.stringify(revFork.body?.error ?? ''),
  )
  // A fork that IS written must be complete and must render. (The blank-at-source
  // case above answered 502, so there is nothing to open here.)
  if (!blankAtSource) {
    const parentEntries = entriesOf(sid).filter((e) => e.type !== 'session').length
    const forkEntries = entriesOf(revFid).filter((e) => e.type !== 'session').length
    check(forkEntries >= parentEntries, `the fork copies every entry, never a truncated tail (${forkEntries} of ${parentEntries})`)
    const revPi = await checkForkFile(revFid, 'fork of a reverted session')
    check(revPi.messages > 0, `pi itself projects it (${JSON.stringify(revPi)})`, revPi.error ?? '')
    const revMsgs = await messages(revFid)
    check(revMsgs.length > 0, `and clicking its row renders messages (${revMsgs.length})`, revMsgs.length ? '' : 'the resumed child has no transcript to project')
    const revRow = (await req('/api/history/sessions')).body?.find((r) => r.id === revFid)
    check(!!revRow && revRow.message_count > 0, `its sidebar row is not "0 msgs" (${JSON.stringify(revRow?.message_count)})`)
  }

  // Fork must NOT spawn a pi child: it copies a transcript, it does not talk to a
  // model. A bridge that has `/fork` in RESUME_ON_POST pays a process spawn per fork
  // and then reads back a tree holding only what that fresh child appended.
  await reapAll() // start from no children, so a spawn is unambiguous
  const beforeFork = (await req('/healthz')).body?.live ?? 0
  void (await post(`/oc/session/${sid}/fork`, {}))
  const afterFork = (await req('/healthz')).body?.live ?? 0
  check(afterFork <= beforeFork, `forking spawns no pi child (live ${beforeFork} -> ${afterFork})`)

  // 7c. the plain whole-session fork from section 1 (asserted cold in 1b), now opened
  // The header is the ONLY place pi records the session id: `_buildIndex` skips
  // `type === 'session'` (core/session-manager.js:736-738), so no copied entry can
  // ever collide with it. What matters is that the fork's own title entry has a
  // unique id and a real parent, so pi can still walk the chain.
  const fTitle = entriesOf(fid).filter((e) => e.type === 'session_info' && /\(fork #1\)/.test(e.name ?? ''))
  check(fTitle.length === 1 && !!fTitle[0].id && fTitle[0].id !== fid && fTitle[0].parentId != null, `the fork's title entry is unique, id'd and parented (${JSON.stringify(fTitle.map((e) => [e.id, e.parentId]))})`)
  // ...and that id must not be a FIXED string. pi indexes every entry by `entry.id`
  // (`_buildIndex`, core/session-manager.js:732-741), so a hard-coded title id is one
  // key shared by every fork of every session — and each later fork's chain then
  // resolves through an index that already holds a same-keyed entry, truncating the
  // path pi can walk. Two forks of two different parents must not share an id.
  const otherTitle = entriesOf(fid2).filter((e) => e.type === 'session_info' && /\(fork #/.test(e.name ?? ''))
  const titleIds = [...fTitle, ...otherTitle].map((e) => e.id).filter(Boolean)
  check(new Set(titleIds).size === titleIds.length && titleIds.length >= 2, `each fork titles with a FRESH entry id (${JSON.stringify(titleIds)})`)
  const liveMsgs = await messages(fid)
  check(liveMsgs.length > 0, `opening its row returns messages (${liveMsgs.length})`, liveMsgs.length ? '' : 'the resumed child has no transcript to project')
  check(
    liveMsgs.some((m) => textOf(m).includes('first thing on the record')),
    'and the copied history is what renders',
    JSON.stringify(liveMsgs.map((m) => textOf(m).slice(0, 24))),
  )

  // 8. the fork must be a real session for every other route: rename sticks, and
  // the auto-tiler must not rename over the engine's "(fork #N)" title
  const renamed = await patch(`/oc/session/${fid}`, { title: 'named by hand' })
  check(renamed.status === 200, `the fork accepts a rename (${renamed.status})`)
  await post(`/oc/session/${fid}/prompt_async`, { parts: [{ type: 'text', text: 'a turn that might tempt a rename' }] })
  check(await waitIdle(fid), 'the first fork ran a turn')
  const rowF = (Array.isArray((await req('/api/history/sessions')).body) ? (await req('/api/history/sessions')).body : []).find((r) => r.id === fid)
  check(rowF?.title === 'named by hand', `the rename stuck through a turn (${JSON.stringify(rowF?.title)})`)
}

if (!(await startBridge())) {
  console.log('FAIL the bridge never came up')
  stop()
  process.exit(1)
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
