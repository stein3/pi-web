// Regression check: ONE public id per session, from the first byte to the last.
//
// Why this exists (the reported bug: one session showing up as TWO sidebar rows
// and TWO tabs — `tau_mvpaud47_crpmiww` next to `01a1786c-5cd3-725f-a352-00117ebf7f7b`):
// pi names its transcript `<iso-ts>_<uuid>.jsonl` and that uuid is the only id
// that survives a reaped child, an exited child and a server restart. tau's
// `tau_*` ids are per-CHILD handles: the same session answers as a different tau
// id after every resume. But pi does not create the file until the session has a
// user or assistant message (core/session-manager.js `_hasConversation`: "setup
// entries alone stay in memory so opening and closing pi without chatting leaves
// no file behind"), so a freshly created child has NO file — and `canonicalId()`
// fell back to the tau id. The webui opened its tab with that tau id; the first
// prompt made pi write the file; the sidebar then listed the SAME session under
// its uuid. Two rows, two tabs, one transcript.
//
// The fixture has to be honest for this to be testable at all, so fake-pi runs in
// FAKE_PI_PERSIST mode here: it writes a real pi-shaped transcript into pi's own
// `<agentDir>/sessions/<cwd-slug>/` layout, mints its own uuid when launched
// without `--session`, and keeps the header's id when launched WITH one.
//
//   cd web && node .testbin/id-check.mjs
//   PI_WEB_JS=<other build> node .testbin/id-check.mjs   # prove it fails pre-fix
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { REPO_CWD, sessionSlug } from './harness.mjs'

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const TESTBIN = path.join(WEB, '.testbin')
const PI_WEB = process.env.PI_WEB_JS || path.join(WEB, 'pi-web.mjs')
const PORT = Number(process.env.ID_CHECK_PORT || 8131)
const BASE = `http://127.0.0.1:${PORT}`
const CWD = REPO_CWD
const SDIR = path.join(WEB, '.logs', 'id-check')
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

// a transcript already on disk — the shape a sidebar click resumes
const SEED = '01ee0000-0000-7000-8000-000000000001'
const SEED_FILE = path.join(SESSIONS, sessionSlug(CWD), `2026-01-01T00-00-00-000Z_${SEED}.jsonl`)
fs.writeFileSync(
  SEED_FILE,
  [
    { type: 'session', version: 3, id: SEED, timestamp: '2026-01-01T00:00:00.000Z', cwd: CWD },
    { type: 'session_info', id: 'si1', parentId: null, timestamp: '2026-01-01T00:00:00.050Z', name: 'a session from the sidebar' },
    {
      type: 'message', id: 'u1', parentId: 'si1', timestamp: '2026-01-01T00:00:00.100Z',
      message: { role: 'user', content: [{ type: 'text', text: 'the message that was already there' }], timestamp: 1 },
    },
  ]
    .map((e) => JSON.stringify(e) + '\n')
    .join(''),
)

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
      FAKE_PI_PERSIST: '1', // the fixture must leave a transcript behind
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

// every SSE frame, with the id it is addressed to
let frames = []
let probe = null
let probeTimer = null
function collectFrames(holdMs = 90_000) {
  frames = []
  let abort = null
  let stopWanted = false
  const p = new Promise((res) => {
    ;(async () => {
      for (let i = 0; i < 40 && !stopWanted; i++) {
        // The reader's rejection on abort is NOT covered by the fetch's .catch:
        // an uncaught AbortError here kills the check mid-run (seen 2026-10-26).
        try {
        abort = new AbortController()
        const t = setTimeout(() => abort.abort(), holdMs)
        const r = await fetch(`${BASE}/oc/event`, { signal: abort.signal }).catch(() => null)
        clearTimeout(t)
        if (!r?.body) {
          await sleep(250)
          continue
        }
        const rd = r.body.getReader()
        const dec = new TextDecoder()
        let buf = ''
        for (;;) {
          const { done, value } = await rd.read()
          if (done) break
          buf += dec.decode(value, { stream: true })
          let k
          while ((k = buf.indexOf('\n\n')) >= 0) {
            const chunk = buf.slice(0, k)
            buf = buf.slice(k + 2)
            const data = chunk.split('\n').find((l) => l.startsWith('data:'))
            if (data) {
              try {
                frames.push(JSON.parse(data.slice(5)))
              } catch {
                /* keep going */
              }
            }
          }
        }
        break
        } catch {
          /* aborted or dropped: retry below */
          await sleep(250)
        }
        if (stopWanted) break
      }
      res()
    })()
  })
  probe = () => {
    stopWanted = true // otherwise the retry loop just reconnects forever
    try {
      abort?.abort()
    } catch {
      /* already finished */
    }
  }
  probeTimer = setTimeout(() => abort?.abort(), holdMs + 5000)
  return p
}

function transcriptFiles() {
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
    /* no sessions yet */
  }
  return out
}

/**
 * The bridge caches its file listing for 1500 ms (`listFilesCached`), and the
 * sidebar is POLLED — a check that reads exactly once can land inside a stale
 * window and blame the bridge for a timing artefact. Poll like the UI does.
 */
async function historyRows(wantId = null, tries = 30) {
  let last = { status: 0, body: [] }
  for (let i = 0; i < tries; i++) {
    last = await req('/api/history/sessions')
    const rows = Array.isArray(last.body) ? last.body : []
    if (rows.length && (wantId === null || rows.some((r) => r.id === wantId))) break
    await sleep(200)
  }
  return last
}

async function waitIdle(sid, tries = 200) {
  for (let i = 0; i < tries; i++) {
    const st = await req('/oc/session/status')
    if (st.body?.[sid]?.type === 'idle') return true
    await sleep(100)
  }
  return false
}

async function main() {
  const created = await post('/oc/session', { cwd: CWD, title: 'id contract' })
  const sid = created.body?.id
  check(created.status === 201 && !!sid, `POST /oc/session returned an id (${created.status} ${JSON.stringify(sid)})`)

  // 1. the id handed to the webui is pi's id, not a per-child handle
  check(!!sid && UUID.test(sid), `the created id is pi's uuid shape (${JSON.stringify(sid)}) — not a tau handle`)
  check(!!sid && !/^tau_/.test(sid), `the created id is not a tau_* handle (${JSON.stringify(sid)})`)

  // 2. pi has NOT written anything yet (core/session-manager.js: setup entries
  // alone stay in memory, "opening and closing pi without chatting leaves no
  // file behind") — and the uuid pi computed anyway must already address the
  // child. That is the whole trick: the id is trustworthy before the file is.
  const filesNow = transcriptFiles().filter((f) => path.basename(f).endsWith(`_${sid}.jsonl`))
  check(filesNow.length === 0, `nothing of OURS is on disk before the first prompt (${filesNow.length} file(s) named for it)`)
  const early = await req(`/oc/session/${encodeURIComponent(sid ?? '')}/message`)
  check(early.status === 200, `the published id addresses the file-less child (${early.status})`)

  // 3. an unprompted session is not a sidebar row: pre-minting must not leave
  // empty rows behind (pi's own rule — no conversation, no row)
  const before = await req('/api/history/sessions')
  check(before.status === 200, `GET /api/history/sessions answers (${before.status})`)
  const rowsBefore = Array.isArray(before.body) ? before.body : []
  check(Array.isArray(rowsBefore) && rowsBefore.filter((r) => r.id === sid).length === 0, `an empty new session is not listed yet (${rowsBefore.length} row(s): ${JSON.stringify(rowsBefore.map((r) => r.id))})`)

  // 4. the live list agrees: one entry, addressed as the uuid
  const liveRes = await req('/oc/session')
  const live = Array.isArray(liveRes.body) ? liveRes.body : []
  check(Array.isArray(live) && live.length === 1 && live[0].id === sid, `the live session list says the same id (${JSON.stringify((live ?? []).map((s) => s.id))})`)
  check(Array.isArray(live) && !live.some((s) => /^tau_/.test(s.id ?? '')), `no tau_* id leaks to the client (${JSON.stringify((live ?? []).map((s) => s.id))})`)

  // 5. every frame of the turn is addressed to that one id
  const pending = collectFrames()
  await sleep(250)
  await post(`/oc/session/${sid}/prompt_async`, { parts: [{ type: 'text', text: 'hello from the id contract check' }] })
  const idle = await waitIdle(sid)
  check(idle, 'the turn finished')
  await sleep(500)
  probe?.()
  clearTimeout(probeTimer)
  await pending
  const ids = [...new Set(frames.filter((f) => f.properties?.sessionID).map((f) => f.properties.sessionID))]
  check(frames.length > 5, `the probe saw the turn (${frames.length} frame(s))`)
  check(ids.length === 1 && ids[0] === sid, `every SSE frame used the ONE id (${JSON.stringify(ids)})`)

  // 6. after the prompt, exactly one sidebar row — the duplicate this check
  // exists for: tab under one id, row under another
  const after = await historyRows(sid)
  const rows = Array.isArray(after.body) ? after.body : []
  const mine2 = rows.filter((r) => r.id === sid)
  check(mine2.length === 1, `exactly ONE sidebar row for the session (${after.status}, ${JSON.stringify(rows.map((r) => r.id))})`)
  check(!rows.some((r) => /^tau_/.test(r.id ?? '')), `no sidebar row carries a tau_* id (${JSON.stringify(rows.map((r) => r.id))})`)

  // 6b. and the transcript pi wrote is NAMED AFTER THE ID WE PUBLISHED — the
  // invariant that makes the id survive a reaped child, an exited child and a
  // server restart
  const mine = transcriptFiles().filter((f) => path.basename(f).endsWith(`_${sid}.jsonl`))
  check(mine.length === 1, `the transcript is named *_<published id>.jsonl (${JSON.stringify(transcriptFiles().map((f) => path.basename(f)))})`)
  if (mine.length === 1) {
    const header = JSON.parse(fs.readFileSync(mine[0], 'utf8').split('\n')[0])
    check(header.id === sid, `its header id is the published id (${JSON.stringify(header.id)})`)
    check(
      header.type === 'session' && header.version === 3 && header.cwd === CWD,
      `it is a pi v3 header for this cwd (type=${header.type} version=${header.version} cwd=${JSON.stringify(header.cwd)})`,
    )
  }

  // 7. reading it back must not open a SECOND child on the same transcript
  const msgs = await req(`/oc/session/${sid}/message`)
  check(msgs.status === 200 && Array.isArray(msgs.body) && msgs.body.length >= 2, `the transcript reads back (${msgs.status}, ${(msgs.body ?? []).length} message(s))`)
  const hz = await req('/healthz')
  check(hz.body?.live === 1, `still exactly ONE live child after create + prompt + read (live=${hz.body?.live})`)

  // 8. a session created and never prompted has no transcript to trash: the
  // delete must still close the child and answer 2xx
  const orphan = await post('/oc/session', { cwd: CWD, title: 'never prompted' })
  const oid = orphan.body?.id
  const gone = await req(`/oc/session/${encodeURIComponent(oid ?? '')}`, { method: 'DELETE' })
  check(gone.status === 200 || gone.status === 204, `DELETE of an un-prompted session resolves (${gone.status} for ${JSON.stringify(oid)})`)

  // 9. the OTHER half: a session opened from the sidebar (cold resume). Its live
  // child answers as a NEW tau handle while the row/tab hold the uuid, so frames
  // must be addressed by the uuid alone. Addressing both aliases used to put a
  // SECOND copy of every frame on the wire — for every session whose child knew
  // its file, i.e. every session in a running server.
  const coldRead = await req(`/oc/session/${SEED}/message`)
  check(coldRead.status === 200 && Array.isArray(coldRead.body) && coldRead.body.length >= 1, `the seeded transcript opens cold (${coldRead.status}, ${(coldRead.body ?? []).length} message(s))`)
  const probe2 = collectFrames()
  await sleep(300)
  await post(`/oc/session/${SEED}/prompt_async`, { parts: [{ type: 'text', text: 'a turn on the sidebar session' }] })
  check(await waitIdle(SEED), 'the seeded session finished its turn')
  await sleep(500)
  probe?.()
  clearTimeout(probeTimer)
  await probe2
  const coldIds = [...new Set(frames.filter((f) => f.properties?.sessionID).map((f) => f.properties.sessionID))]
  const deltas = frames.filter((f) => f.type === 'message.part.delta')
  check(coldIds.length === 1 && coldIds[0] === SEED, `a resumed session's frames carry the uuid only (${JSON.stringify(coldIds)})`)
  // the resumed child must STREAM too: a session opened from the sidebar that
  // only lands at turn end is the other half of "updates are not real time"
  const streamed = deltas.map((f) => f.properties?.delta ?? '').join('')
  // the fixture's assistant reply, copied verbatim (fake-pi DEFAULT_WORDS)
  const FIXTURE_TEXT = 'Hello streamed world.'
  check(deltas.length >= 2 && streamed === FIXTURE_TEXT, `a resumed session STREAMS its text (${deltas.length} delta frame(s): ${JSON.stringify(streamed)})`)
  const coldMsgs = await req(`/oc/session/${SEED}/message`)
  const lastText = (Array.isArray(coldMsgs.body) ? coldMsgs.body : [])
    .flatMap((m) => (m.parts ?? []).filter((pt) => pt.type === 'text'))
    .map((pt) => pt.text ?? '')
    .join('')
  check(lastText.includes(streamed.slice(0, 24)), `the streamed text is the text the read path returns (${JSON.stringify(streamed.slice(0, 24))})`)
  const live2 = (await req('/oc/session')).body ?? []
  const ids2 = (Array.isArray(live2) ? live2 : []).map((x) => x.id)
  check(ids2.length === 2 && ids2.includes(SEED) && ids2.includes(sid) && !ids2.some((x) => /^tau_/.test(x)), `two sessions, one child each, both addressed by uuid (${JSON.stringify(ids2)})`)

  const closed = await req(`/oc/session/${sid}`, { method: 'DELETE' })
  check(closed.status === 200 || closed.status === 204, `DELETE resolved it (${closed.status})`)
  const stillThere = transcriptFiles().filter((f) => path.basename(f).endsWith(`_${sid}.jsonl`))
  check(stillThere.length === 0, `its transcript left the tree (${JSON.stringify(stillThere.map((f) => path.basename(f)))})`)
  const trash = path.join(AGENT, 'sessions-trash')
  const inTrash = fs.existsSync(trash) ? fs.readdirSync(trash).filter((f) => f.endsWith(`_${sid}.jsonl`)) : []
  check(inTrash.length === 1, `and it is in the out-of-tree trash (${JSON.stringify(inTrash)})`)
}

let log = ''
try {
  log = fs.readFileSync(path.join(SDIR, 'server.log'), 'utf8')
} catch {
  /* no log */
}

await (async () => {
  for (let i = 0; i < 60; i++) {
    try {
      if ((await req('/healthz')).status === 200) break
    } catch {
      /* not up yet */
    }
    await sleep(200)
  }
})()

console.log('\n===== main =====')
try {
  await main()
} catch (e) {
  console.log('ERROR', e?.stack ?? String(e))
  failed++
}
probe?.()
stop()
await sleep(250)
console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
process.exit(failed ? 1 : 0)
