// Regression check: who names a session, and where that name lands.
//
// Why this exists (investigated 2026-10-26): pi NEVER names a session by itself
// — there is no auto-name anywhere in pi dist; its only hook is the
// `set_session_name` RPC, which appends a `session_info` entry and emits
// `session_info_changed`. tau's `maybeTitle` heuristic (pi-tau-web-server/bin/
// sessions.js) fires a synthetic `session_name` event but keeps the result in
// memory, so it can never reach the sidebar — a sidebar row is a projection of
// the JSONL. So the bridge is the single titler: it derives a name from the first
// user prompt and hands it to pi, which persists it, so pi, the TUI, the read
// path and the webui all read the SAME string. Asserts both directions:
//
//   1. an unnamed session gets named — at cold resume AND on the live first
//      user message of a created session (the bridge must SEND the RPC);
//   2. the name is heuristic-clean: one line, no conversational filler, capped;
//   3. a session pi already named is never renamed by the bridge;
//   4. a rename that happens INSIDE pi (`session_info_changed`) reaches the tab,
//      and the bridge does not fight it afterwards;
//   5. the auto-name is sent exactly once per session (no RPC thrash);
//   6. renaming a session with NO live child persists to the JSONL — a PATCH
//      that only touched memory silently reverted on the next sidebar poll.
//
// Self-contained: seeds fixtures in a scratch pi dir, spawns pi-web against the
// fake pi (which models pi's rename semantics and logs the RPCs it receives),
// asserts over HTTP + SSE + that log, and kills its own child.
//
//   node test/title-check.mjs
//   PI_WEB_JS=<other build> node test/title-check.mjs   # prove it fails pre-fix
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { BRIDGE, REPO_CWD, TESTBIN, WEB, sessionSlug } from './harness.mjs'

const PORT = Number(process.env.TITLE_CHECK_PORT || 8127)
const BASE = `http://127.0.0.1:${PORT}`
const CWD = REPO_CWD
const SDIR = path.join(WEB, '.logs', 'title-check')
const AGENT = path.join(SDIR, 'pi-agent')
const RPC_LOG = path.join(SDIR, 'rpc.log')
const WS_SLUG = sessionSlug(CWD)

let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
  if (!ok) failed++
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const fname = (id) => `2026-01-01T00:00:00-000Z_${id}.jsonl`
function writeSession(file, id, lines) {
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(
    file,
    [
      { type: 'session', version: 3, id, timestamp: '2026-01-01T00:00:00.000Z', cwd: CWD },
      { type: 'model_change', id: 'mc1', parentId: null, timestamp: '2026-01-01T00:00:00.010Z', provider: 'testprov', modelId: 'fake-1' },
      ...lines,
    ]
      .map((e) => JSON.stringify(e) + '\n')
      .join(''),
  )
}
const userMsg = (id, parent, text) => ({
  type: 'message',
  id,
  parentId: parent,
  timestamp: '2026-01-01T00:00:00.100Z',
  message: { role: 'user', content: [{ type: 'text', text }], timestamp: 1 },
})

fs.rmSync(SDIR, { recursive: true, force: true })
fs.mkdirSync(path.join(AGENT, 'sessions', WS_SLUG), { recursive: true })

// messy first prompt: filler prefix, newlines, double spaces, runs past 60 chars
const UN = '01dd0000-0000-7000-8000-00000000000u'
const MESSY = 'okay so   can you fix the transcript\nnot updating in real time with sessions\nand the tool rows too'
writeSession(path.join(AGENT, 'sessions', WS_SLUG, fname(UN)), UN, [userMsg('u1', 'mc1', MESSY)])

// already named by pi — the bridge must never re-name it
const NA = '01dd0000-0000-7000-8000-00000000000n'
writeSession(path.join(AGENT, 'sessions', WS_SLUG, fname(NA)), NA, [
  { type: 'session_info', id: 'si1', parentId: 'mc1', timestamp: '2026-01-01T00:00:00.050Z', name: 'keep this name' },
  userMsg('u1', 'si1', 'never rename this one because it already has a name'),
])

// renamed while no child owns it (cold) — must land in the JSONL
const CL = '01dd0000-0000-7000-8000-00000000000c'
writeSession(path.join(AGENT, 'sessions', WS_SLUG, fname(CL)), CL, [userMsg('u1', 'mc1', 'a cold session waiting to be renamed')])

// A port already held by some older server would make this check assert against
// the wrong bridge (seen today: a server left running because it was killed by
// the wrong pid kept the port, the new one died on EADDRINUSE, and the probe
// happily tested the old build). Fail loudly instead.
if (!(await fetch(`${BASE}/healthz`).then(() => null).catch(() => true))) {
  console.log(`FAIL port ${PORT} is already answering — another pi-web/test server is up`)
  process.exit(1)
}

const child = spawn(
  process.execPath,
  [BRIDGE, '--port', String(PORT), '--host', '127.0.0.1', '--dir', AGENT, '--cwd', CWD],
  {
    cwd: WEB,
    env: {
      ...process.env,
      PATH: `${TESTBIN}:${process.env.PATH}`,
      FAKE_PI_RPC_LOG: RPC_LOG,
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

// SSE: every frame the tabs would see, with the id the frame is addressed to.
// Retries: the check spawns its own bridge, so the first connect usually races
// listen() and gets ECONNREFUSED — a silent zero-frame stream is how a check
// ends up asserting on nothing.
const frames = []
let sseAbort = null
async function openSse() {
  for (let i = 0; i < 40; i++) {
    sseAbort = new AbortController()
    const res = await fetch(`${BASE}/oc/event`, { signal: sseAbort.signal }).catch(() => null)
    if (res?.body) {
      const rd = res.body.getReader()
      const dec = new TextDecoder()
      let buf = ''
      void (async () => {
        for (;;) {
          const { done, value } = await rd.read()
          if (done) return
          buf += dec.decode(value, { stream: true })
          let i
          while ((i = buf.indexOf('\n\n')) >= 0) {
            const chunk = buf.slice(0, i)
            buf = buf.slice(i + 2)
            const line = chunk.split('\n').find((l) => l.startsWith('data: '))
            if (line) {
              try {
                frames.push(JSON.parse(line.slice(6)))
              } catch {
                /* ignore */
              }
            }
          }
        }
      })()
      return true
    }
    await sleep(150)
  }
  return false
}
const titlesFor = (sid) =>
  frames
    .filter((f) => f.type === 'session.updated' && f.properties?.sessionID === sid)
    .map((f) => f.properties?.info?.title)
const namesSent = () => {
  let raw = ''
  try {
    raw = fs.readFileSync(RPC_LOG, 'utf8')
  } catch {
    return []
  }
  return raw
    .split('\n')
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l)
      } catch {
        return null
      }
    })
    .filter((r) => r?.type === 'set_session_name' && r.name)
    .map((r) => r.name)
}
const waitNames = async (n, ms = 6000) => {
  const t0 = Date.now()
  while (Date.now() - t0 < ms) {
    if (namesSent().length >= n) return true
    await sleep(150)
  }
  return false
}
const waitIdle = async (sid, ms = 15000) => {
  const t0 = Date.now()
  while (Date.now() - t0 < ms) {
    if (frames.some((f) => f.type === 'session.idle' && f.properties?.sessionID === sid)) return true
    await sleep(150)
  }
  return false
}

try {
  // wait for the bridge to be listening BEFORE anything else, so the SSE
  // connect and the first assertions can't race it
  let up = false
  for (let i = 0; i < 60 && !up; i++) up = (await req('/healthz')).status === 200 || (await req('/api/history/sessions')).status === 200
  check(await openSse(), 'SSE stream connected (a zero-frame stream proves nothing)')

  let rows = []
  for (let i = 0; i < 60; i++) {
    rows = (await req('/api/history/sessions')).body ?? []
    if ([UN, NA, CL].every((id) => rows.find((r) => r.id === id))) break
    await sleep(250)
  }
  check(rows.length >= 3, 'fixtures listed', `${rows.length} rows`)

  // ---- 6. rename with no live child must persist to the JSONL --------------
  const pr = await req(`/oc/session/${CL}`, {
    method: 'PATCH',
    body: JSON.stringify({ title: 'operator renamed a cold session' }),
    headers: { 'content-type': 'application/json' },
  })
  check(pr.status === 200, 'PATCH title on a cold session answers', `${pr.status}`)
  let coldRow = null
  for (let i = 0; i < 20 && !(coldRow = (await req('/api/history/sessions')).body?.find((r) => r.id === CL)); i++) await sleep(200)
  check(
    coldRow?.title === 'operator renamed a cold session',
    'a rename with NO live child persists (row shows it, not the prompt)',
    `title=${JSON.stringify(coldRow?.title)}`,
  )
  check(
    fs.readFileSync(path.join(AGENT, 'sessions', WS_SLUG, fname(CL)), 'utf8').includes('operator renamed a cold session'),
    'the cold rename is in the JSONL, so pi and the TUI see it too',
  )

  // ---- 1+2. cold resume of an unnamed session: the bridge must NAME it ----
  const g = await req(`/oc/session/${UN}`)
  check(g.status === 200, 'unnamed fixture resumed', `${g.status}`)
  check(await waitNames(1), 'resume of an unnamed session sends set_session_name (pi persists it)', `names=${JSON.stringify(namesSent())}`)
  const auto = namesSent()[0] ?? ''
  check(!/[\r\n]/.test(auto) && auto.length <= 61, 'the generated name is one line and capped', `${JSON.stringify(auto)} (${auto.length})`)
  check(!/^(ok|okay|so|can you|could you|please|hey|actually)\b/i.test(auto), 'conversational filler stripped', JSON.stringify(auto))
  check(auto.charAt(0) === auto.charAt(0).toUpperCase(), 'first letter capitalised', JSON.stringify(auto))
  check(/transcript/i.test(auto), 'the name still describes the session', JSON.stringify(auto))
  // the forced session.updated follows the RPC response, so give it a beat
  let toldTitle = false
  for (let i = 0; i < 20 && !toldTitle; i++) {
    toldTitle = titlesFor(UN).includes(auto)
    if (!toldTitle) await sleep(150)
  }
  check(toldTitle, 'the tab is told the new name over SSE (session.updated)', JSON.stringify(titlesFor(UN)))

  // ---- 3. a session pi already named is never renamed ---------------------
  const g2 = await req(`/oc/session/${NA}`)
  await sleep(900)
  check(g2.status === 200, 'named fixture resumed')
  check(
    !namesSent().some((n) => /never rename/i.test(n)),
    'an existing name is never overwritten by the auto-tiler',
    `names=${JSON.stringify(namesSent())}`,
  )

  // ---- 4. a rename that happens inside pi lands on the tab, and sticks ----
  const before = namesSent().length
  await post(`/oc/session/${UN}/prompt_async`, { parts: [{ type: 'text', text: '/fake-rename Renamed inside pi' }] })
  let renamed = false
  for (let i = 0; i < 40 && !renamed; i++) {
    renamed = titlesFor(UN).includes('Renamed inside pi')
    if (!renamed) await sleep(150)
  }
  check(renamed, "pi's own session_info_changed reaches the tab", JSON.stringify(titlesFor(UN)))

  // ---- 1b. a created session is named from its first live prompt ----------
  const made = await post('/oc/session', { cwd: CWD })
  check(made.status === 201, 'session created', `${made.status}`)
  const mid = made.body?.id
  await post(`/oc/session/${mid}/prompt_async`, { parts: [{ type: 'text', text: 'please   deploy the bridge\nand restart it after' }] })
  await waitIdle(mid)
  check(
    namesSent().some((n) => /deploy the bridge/i.test(n)),
    'a brand-new session is named from its first live user message',
    `names=${JSON.stringify(namesSent())}`,
  )

  // ---- 5. one auto-name per session, not one per event --------------------
  await post(`/oc/session/${UN}/prompt_async`, { parts: [{ type: 'text', text: 'another turn on the renamed session' }] })
  await waitIdle(UN)
  await sleep(600)
  const after = namesSent()
  check(
    after.filter((n) => n === auto).length === 1,
    'the auto-name is sent once; a later turn does not re-name the session',
    `sent=${JSON.stringify(after)} (${after.length - before} new after the pi-side rename)`,
  )
  check(
    !after.slice(before).some((n) => /another turn|renamed session/i.test(n)),
    'nothing re-names a session pi already renamed',
    `new=${JSON.stringify(after.slice(before))}`,
  )
} finally {
  sseAbort?.abort()
  stop()
}

console.log(failed ? `\n${failed} FAILURE(S)` : '\nALL OK')
process.exit(failed ? 1 : 0)
