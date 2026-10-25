// SSE volume + live-child reclamation regression check.
//
// Why this exists (measured on the live bridge, 2026-10-25):
//   * tau calls broadcastUpdated() after EVERY pi event, and pi-web mirrored it
//     verbatim, so every token delta arrived paired with a `session.updated`:
//     6,090 session.updated vs 5,375 message.part.delta in a 170 s window — 53%
//     of all frames. The webui coalesces text deltas (40 ms) but its
//     session.updated branch goes straight to tabs.patch() + patchMetrics(), i.e.
//     one uncoalesced store notify per token → the transcript re-rendered per
//     token. Hence "the webui is laggier on pi than on opencode".
//   * nothing ever closed a live pi child: closing a tab is local to the browser,
//     so 13 children / 1.80 GiB RSS piled up against a 3 GiB cgroup cap, and an
//     unwatched background child streamed 83% of all frames while a foreground
//     turn competed for the single llama.cpp slot.
//
// Assertions, phase 1 (frame census for one turn): deltas flow, the streamed
// text survives translation, and session.updated stays a *metadata* event
// (single digits per turn, not one-per-token).
//
// Assertions, phase 2 (reaper): an idle child is closed, the tab is NOT closed
// (no session.deleted frame), the public id still answers /message by resuming
// from JSONL, the history row keeps the id the browser holds (otherwise one
// session becomes two rows/tabs), and a POST prompt still lands.
//
//   cd /workspace/pi-local/web && node .testbin/stream-check.mjs
//   PI_WEB_JS=/app/pi-web.mjs CENSUS_ONLY=1 node .testbin/stream-check.mjs   # A/B baseline
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const TESTBIN = path.join(WEB, '.testbin')
const PI_WEB = process.env.PI_WEB_JS || path.join(WEB, 'pi-web.mjs')
const PORT = Number(process.env.STREAM_CHECK_PORT || 8127)
const BASE = `http://127.0.0.1:${PORT}`
const CWD = '/workspace/pi-local'
const WS_SLUG = '--workspace-pi-local--'

const WORDS = Number(process.env.STREAM_WORDS || 120)
const DELTA_MS = Number(process.env.STREAM_DELTA_MS || 4)
const EXPECT = Array.from({ length: WORDS }, (_, i) => (i === WORDS - 1 ? 'end.' : `w${i} `)).join('')

let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

function writeSession(file, id, text) {
  const t = (n) => new Date(Date.parse('2026-01-01T00:00:00.000Z') + n).toISOString()
  const lines = [
    { type: 'session', version: 3, id, timestamp: t(0), cwd: CWD },
    { type: 'model_change', id: 'm1', parentId: null, timestamp: t(10), provider: 'testprov', modelId: 'fake-1' },
    {
      type: 'message', id: 'u1', parentId: 'm1', timestamp: t(20),
      message: { role: 'user', content: [{ type: 'text', text }], timestamp: Date.parse(t(20)) },
    },
    {
      type: 'message', id: 'a1', parentId: 'u1', timestamp: t(40),
      message: { role: 'assistant', content: [{ type: 'text', text: 'fixture answer' }], timestamp: Date.parse(t(40)) },
    },
  ]
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, lines.map((l) => JSON.stringify(l)).join('\n') + '\n')
}

const j = async (p, init) => {
  const r = await fetch(BASE + p, {
    ...init,
    headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) },
    body: init?.body !== undefined ? JSON.stringify(init.body) : undefined,
  })
  let body = null
  try {
    body = await r.json()
  } catch {
    /* no body */
  }
  return { status: r.status, json: body }
}

// one SSE connection, frames collected for the whole run
function openSse(collected) {
  return new Promise((resolve, reject) => {
    http
      .get(`${BASE}/oc/event`, (res) => {
        let buf = ''
        res.setEncoding('utf8')
        res.on('data', (d) => {
          buf += d
          let k
          while ((k = buf.indexOf('\n\n')) >= 0) {
            const raw = buf.slice(0, k)
            buf = buf.slice(k + 2)
            const i = raw.indexOf('data: ')
            if (i < 0) continue
            try {
              const ev = JSON.parse(raw.slice(i + 6))
              collected.push({ bytes: raw.length + 2, ...ev })
            } catch {
              /* keepalive */
            }
          }
        })
        resolve(res)
      })
      .on('error', reject)
  })
}

const countBy = (frames, type) => frames.filter((f) => f.type === type).length
const forSession = (frames, sid) => frames.filter((f) => (f.properties?.sessionID ?? f.properties?.info?.id) === sid)

async function waitLive(want, ms = 8000) {
  const t0 = Date.now()
  while (Date.now() - t0 < ms) {
    const h = await j('/healthz')
    if (h.json?.live <= want) return h.json.live
    await sleep(200)
  }
  return (await j('/healthz')).json?.live
}

function spawnWeb(extraEnv) {
  return spawn(
    process.execPath,
    [PI_WEB, '--port', String(PORT), '--host', '127.0.0.1', '--dir', scratch, '--cwd', CWD],
    {
      cwd: WEB,
      env: {
        ...process.env,
        PATH: `${TESTBIN}:${process.env.PATH}`,
        FAKE_STREAM_WORDS: String(WORDS),
        FAKE_DELTA_MS: String(DELTA_MS),
        ...extraEnv,
      },
      stdio: 'ignore',
      detached: true,
    },
  )
}

async function bootServer() {
  for (let i = 0; i < 40; i++) {
    try {
      const r = await fetch(`${BASE}/healthz`)
      if (r.ok) return
    } catch {
      /* not up yet */
    }
    await sleep(150)
  }
  throw new Error('pi-web never answered /healthz')
}

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'piweb-stream-'))
const A = '01aa0000-0000-7000-8000-00000000000a'
const B = '01bb0000-0000-7000-8000-00000000000b'
const C = '01cc0000-0000-7000-8000-00000000000c'
const fname = (id) => `2026-01-01T00-00-00-000Z_${id}.jsonl`
for (const id of [A, B, C]) writeSession(path.join(scratch, 'sessions', WS_SLUG, fname(id)), id, `stream-check fixture ${id}`)

let child = null
const killWeb = () => {
  try {
    process.kill(-child.pid) // detached -> its own process group
  } catch {
    /* already gone */
  }
}

try {
  // =========================================================================
  // phase 1 — one turn, full SSE census
  // =========================================================================
  child = spawnWeb({ PI_WEB_REAP_INTERVAL_MS: String(60 * 60 * 1000) })
  await bootServer()
  let frames = []
  await openSse(frames)

  const created = await j('/oc/session', { method: 'POST', body: { cwd: CWD } })
  check(created.status === 201, `session created (${created.status})`)
  const sid = created.json.id

  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'stream me' }] } })

  // turn = ~WORDS deltas + 300ms of tool time + settle
  const t0 = Date.now()
  while (Date.now() - t0 < 6000 && !frames.some((f) => f.type === 'session.idle' && f.properties?.sessionID === sid)) {
    await sleep(100)
  }
  await sleep(400)

  const mine = forSession(frames, sid)
  const deltas = mine.filter((f) => f.type === 'message.part.delta')
  const sessUpdated = countBy(mine, 'session.updated')
  const partUpdated = countBy(mine, 'message.part.updated')
  const bytes = mine.reduce((s, f) => s + f.bytes, 0)

  console.log(
    `\ncensus for one ${WORDS}-delta turn: frames=${mine.length} bytes=${bytes} ` +
      `deltas=${deltas.length} part.updated=${partUpdated} session.updated=${sessUpdated} ` +
      `message.updated=${countBy(mine, 'message.updated')}\n`,
  )

  check(deltas.length >= WORDS, `token deltas streamed (${deltas.length} >= ${WORDS})`)
  const deltaText = deltas.map((f) => f.properties.delta).join('')
  check(deltaText === EXPECT, 'delta frames rebuild the assistant text exactly')
  const lastSnap = [...mine].reverse().find((f) => f.type === 'message.part.updated' && f.properties?.part?.type === 'text')
  check(lastSnap?.properties?.part?.text === EXPECT, 'final text snapshot agrees with the delta sum (no double-append)')
  // THE regression: session.updated must stay a metadata event, not a per-token echo.
  check(sessUpdated <= 4, `session.updated stays rare (${sessUpdated} for ${deltas.length} deltas)`)
  check(sessUpdated >= 1, 'session.updated still fires (turn end / create)')
  check(countBy(mine, 'session.idle') === 1, 'session.idle fires once')
  check(
    deltas.reduce((s, f) => s + f.bytes, 0) + partUpdated * 0 > 0 &&
      mine.filter((f) => f.type === 'session.updated').reduce((s, f) => s + f.bytes, 0) < bytes * 0.2,
    'session.updated no longer dominates the byte budget',
  )
  const tools = mine
    .filter((f) => f.type === 'message.part.updated' && f.properties?.part?.type === 'tool')
    .map((f) => f.properties.part)
  const done = new Set(tools.filter((p) => p.state?.status === 'completed' || p.state?.status === 'error').map((p) => p.callID))
  check(done.size === 3, `all three tool parts reach a terminal state (${done.size}/3)`)

  if (process.env.CENSUS_ONLY === '1') {
    console.log('\nCENSUS_ONLY — skipping reaper phase')
  } else {
    killWeb()
    await sleep(400)

    // =======================================================================
    // phase 2 — reaper: close idle children, keep the tab usable
    // =======================================================================
    frames = []
    child = spawnWeb({
      PI_WEB_MAX_LIVE: '1',
      PI_WEB_IDLE_TTL_MS: '300',
      PI_WEB_REAP_INTERVAL_MS: '300',
      FAKE_STREAM_WORDS: '0',
      FAKE_DELTA_MS: '0',
    })
    await bootServer()
    await openSse(frames)

    const m0 = await j(`/oc/session/${A}/message`)
    check(m0.status === 200 && Array.isArray(m0.json) && m0.json.length === 2, `fixture A resumed on first read (${m0.status}, ${m0.json?.length} msgs)`)

    // ---- the invariant: ONE public id for one session, everywhere -----------
    const liveList = (await j('/oc/session')).json ?? []
    check(liveList.length === 1 && liveList[0].id === A, `live session list names it by pi's uuid (${liveList[0]?.id})`)
    const rowLive = (await j('/api/history/sessions')).json?.find((r) => r.title?.includes(`fixture ${A}`))
    check(rowLive?.id === A, `history row uses the SAME id as the live list (${rowLive?.id})`)
    check(rowLive?.live === true, 'row reports itself live')
    const status = Object.keys((await j('/oc/session/status')).json ?? {})
    const tauId = status.find((k) => k.startsWith('tau_'))
    check(!!tauId && status.includes(A), `both spellings are aliased for SSE (${tauId} + ${A})`)

    // addressing the same session every other way must NOT spawn a second child
    await j(`/oc/session/${A}`)
    await j(`/oc/session/${tauId}/message`)
    await j(`/oc/session/${A}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'first' }] } })
    const t0b = Date.now()
    while (Date.now() - t0b < 5000 && !frames.some((f) => f.type === 'session.idle' && f.properties?.sessionID === A)) await sleep(100)
    const liveAfter = (await j('/healthz')).json?.live
    check(liveAfter === 1, `uuid + tau id + prompt all reach the SAME child (live=${liveAfter})`)
    const mLive = await j(`/oc/session/${A}/message`)
    check(mLive.status === 200 && mLive.json?.length === 4, `the live turn is in the transcript (${mLive.json?.length} msgs)`)
    check(
      frames.some((f) => f.type === 'session.idle' && f.properties?.sessionID === A),
      'SSE frames reach a tab keyed by the uuid',
    )

    const live1 = liveAfter
    check(live1 === 1, `one live child after resume (${live1})`)

    const reapAt = Date.now()
    const live2 = await waitLive(0)
    check(live2 === 0, `idle child reaped in ${Date.now() - reapAt}ms (live=${live2})`)
    check(
      !frames.some((f) => f.type === 'session.deleted' && [A, tauId].includes(f.properties?.sessionID)),
      'reaping never emits session.deleted (that would close the browser tab)',
    )

    const rowReaped = (await j('/api/history/sessions')).json?.find((r) => r.title?.includes(`fixture ${A}`))
    check(rowReaped?.id === A, `reaped history row still names the uuid (${rowReaped?.id})`)
    check(rowReaped?.live === false, 'row reports itself not-live after the reap')

    const m1 = await j(`/oc/session/${A}/message`)
    // 2, not 4: the fake pi never writes the JSONL (real pi appends per write), so
    // what survives a reap is exactly what the file holds. The point of this
    // assertion is the RESUME (200 on a reaped id), not the message count.
    check(m1.status === 200 && m1.json?.length === 2, `reaped id still answers /message by resuming (${m1.status}, ${m1.json?.length} msgs)`)
    const mTau = await j(`/oc/session/${tauId}/message`)
    check(mTau.status === 200 && mTau.json?.length === 2, `a tab still holding the OLD tau id still resolves (${mTau.status})`)
    check(
      m1.json?.some((mm) => (mm.parts ?? []).some((p) => p.text === 'fixture answer')),
      'resume re-reads the JSONL transcript (no lost text)',
    )

    const rowAfter = (await j('/api/history/sessions')).json?.find((r) => r.title?.includes(`fixture ${A}`))
    check(rowAfter?.id === A, `still ONE row on the SAME uuid after the resume (${rowAfter?.id})`)
    check(rowAfter?.live === true, 'row reports itself live again after the resume')
    check((await j('/healthz')).json?.live === 1, 'resume by uuid did not add a second child')

    const p2 = await j(`/oc/session/${A}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'after reap' }] } })
    check(p2.status === 204, `POST prompt resumes a reaped session (${p2.status})`)
    const mark = frames.length
    const t1 = Date.now()
    while (
      Date.now() - t1 < 5000 &&
      !frames.slice(mark).some((f) => f.type === 'session.idle' && f.properties?.sessionID === A)
    ) {
      await sleep(100)
    }
    check(frames.slice(mark).some((f) => f.type === 'session.idle' && f.properties?.sessionID === A), 'the resumed child settles under the uuid')
    const m2 = await j(`/oc/session/${A}/message`)
    check(m2.status === 200 && m2.json?.length === 4, `turn after the reap lands in the transcript (${m2.json?.length} msgs)`)
    check(
      m2.json?.some((mm) => (mm.parts ?? []).some((p) => p.text === 'after reap')),
      'prompt text reached the resumed child',
    )

    // LRU cap: three tabs, one child allowed — the idle extras must go
    await j(`/oc/session/${B}/message`)
    await j(`/oc/session/${C}/message`)
    const live3 = await waitLive(1, 10000)
    check(live3 <= 1, `MAX_LIVE caps live children (live=${live3})`)
    const mB = await j(`/oc/session/${B}/message`)
    check(mB.status === 200 && mB.json?.length === 2, `a capped-out session still reads back (${mB.status})`)
    const rows = (await j('/api/history/sessions')).json ?? []
    const ids = new Set(rows.map((r) => r.id))
    check(
      [A, B, C].every((id) => ids.has(id)),
      'every session has exactly one row, keyed by its own uuid',
    )
  }
} catch (e) {
  check(false, `harness error: ${e?.stack ?? e}`)
} finally {
  killWeb()
  fs.rmSync(scratch, { recursive: true, force: true })
}
console.log(failed ? `\nFAILED ${failed}` : '\nALL OK')
process.exit(failed ? 1 : 0)
