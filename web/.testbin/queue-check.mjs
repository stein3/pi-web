// Engine-queue check: steer / follow-up / clear against pi-web running the fake
// pi. HTTP + SSE only — no browser — because the assertions are about ordering
// the engine owns, not about how chips look.
//
//   PATH=$PWD/.testbin:$PATH PI_CODING_AGENT_DIR=/tmp/queue-agent node pi-web.mjs --port 8127 &
//   BASE_URL=http://127.0.0.1:8127 node .testbin/queue-check.mjs
import http from 'node:http'

const BASE = process.env.BASE_URL || 'http://127.0.0.1:8127'
const CWD = process.env.CHECK_CWD || '/tmp'
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}

function req(path, opts = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(BASE + path)
    const r = http.request(
      u,
      { method: opts.method || 'GET', headers: opts.body ? { 'content-type': 'application/json' } : {} },
      (res) => {
        let buf = ''
        res.on('data', (c) => (buf += c))
        res.on('end', () => {
          let json = null
          try {
            json = buf ? JSON.parse(buf) : null
          } catch {
            /* non-json body */
          }
          resolve({ status: res.statusCode, json, body: buf })
        })
      },
    )
    r.on('error', reject)
    if (opts.body) r.write(JSON.stringify(opts.body))
    r.end()
  })
}

// ---- SSE tap ----------------------------------------------------------------
const events = []
function openSse() {
  const u = new URL(BASE + '/oc/event')
  return new Promise((resolve, reject) => {
    http.get(u, (res) => {
      let buf = ''
      res.on('data', (c) => {
        buf += c
        let i
        while ((i = buf.indexOf('\n\n')) >= 0) {
          const frame = buf.slice(0, i)
          buf = buf.slice(i + 2)
          const d = frame.split('\n').find((l) => l.startsWith('data: '))
          if (!d) continue
          try {
            const e = JSON.parse(d.slice(6))
            events.push({ t: Date.now(), ...e })
          } catch {
            /* keepalive */
          }
        }
      })
      resolve(res)
    }).on('error', reject)
  })
}
// The bridge's SSE envelope is opencode's: every payload — including the
// sessionID emitFor() attaches — lives under `properties`, never at the top
// level of the frame. And a user message reaches the client as message.updated
// + message.part.updated, never as a pi-style `message_end`, so the transcript
// assertion reads the HTTP transcript (the same projection the webui refetches).
const userMsgs = async (sid) => {
  const r = await req(`/oc/session/${sid}/message`)
  return (r.json || [])
    .filter((m) => m.role === 'user')
    .flatMap((m) => (m.parts || []).filter((p) => p.type === 'text').map((p) => p.text ?? ''))
}
const queuesFor = (sid) =>
  events.filter((e) => e.type === 'session.queue' && (sid === '' || e.properties?.sessionID === sid))
const queueOf = (e) => e.properties?.queue ?? {}

try {
  await openSse()
  const created = await req('/oc/session', { method: 'POST', body: { cwd: CWD, title: 'queue-check' } })
  check(created.status === 201, `session created (${created.status})`)
  const sid = created.json.id

  // ---- idle send: starts, never queues ------------------------------------
  const a = 'first prompt'
  const rA = await req(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: a }] } })
  check(rA.status === 204, `idle send acked (${rA.status})`)
  await sleep(100) // mid-turn: tools are running for 300ms

  // ---- steer during the run -------------------------------------------------
  const b = 'STEER now'
  const rB = await req(`/oc/session/${sid}/prompt_async`, {
    method: 'POST',
    body: { parts: [{ type: 'text', text: b }], delivery: 'steer' },
  })
  check(rB.status === 204, `steer acked (${rB.status})`)
  const qAfterSteer = await req(`/oc/session/${sid}/queue`)
  check(
    qAfterSteer.status === 200 && qAfterSteer.json.steering.includes(b) && !qAfterSteer.json.followUp.length,
    `steer visible in queue (${JSON.stringify(qAfterSteer.json)})`,
  )

  // ---- follow-up during the run --------------------------------------------
  const c = 'FOLLOW later'
  await req(`/oc/session/${sid}/prompt_async`, {
    method: 'POST',
    body: { parts: [{ type: 'text', text: c }], delivery: 'follow' },
  })
  const qAfterFollow = await req(`/oc/session/${sid}/queue`)
  check(
    qAfterFollow.json.steering.includes(b) && qAfterFollow.json.followUp.includes(c),
    'both queues mirror pi',
  )

  // ---- slash command during the run: never queued --------------------------
  const slash = '/model-sync all'
  const rS = await req(`/oc/session/${sid}/command`, { method: 'POST', body: { command: slash.slice(1) } })
  const slashQueued = queuesFor(sid).some((e) => [...(queueOf(e).steering ?? []), ...(queueOf(e).followUp ?? [])].includes(slash))
  check(rS.status === 200 && !slashQueued, `slash command ran without queueing (${rS.status}, queued=${slashQueued})`)

  // ---- abort leaves the queues armed ---------------------------------------
  await req(`/oc/session/${sid}/abort`, { method: 'POST' })
  const qAfterAbort = await req(`/oc/session/${sid}/queue`)
  check(
    qAfterAbort.json.steering.includes(b) && qAfterAbort.json.followUp.includes(c),
    'abort did not discard armed messages',
  )

  // ---- clear drops everything, server-side --------------------------------
  const cleared = await req(`/oc/session/${sid}/queue/clear`, { method: 'POST' })
  check(
    cleared.json?.ok && cleared.json.steering.includes(b) && cleared.json.followUp.includes(c),
    `clear reports what it dropped (${JSON.stringify(cleared.json)})`,
  )
  const qEmpty = await req(`/oc/session/${sid}/queue`)
  check(!qEmpty.json.steering.length && !qEmpty.json.followUp.length, 'queue empty after clear')
  await sleep(300)
  check(
    queuesFor(sid).some((e) => !queueOf(e).steering?.length && !queueOf(e).followUp?.length),
    'clear broadcast an empty queue',
  )

  // ---- attachments ----------------------------------------------------------
  const png = 'POST /oc/session/x/prompt_async'
  const okImg = await req(`/oc/session/${sid}/prompt_async`, {
    method: 'POST',
    body: { parts: [{ type: 'file', mime: 'image/png', url: 'data:image/png;base64,' + Buffer.from([1, 2, 3]).toString('base64'), filename: 'shot.png' }] },
  })
  check(okImg.status === 204, `image part accepted (${okImg.status})`)
  const badFile = await req(`/oc/session/${sid}/prompt_async`, {
    method: 'POST',
    body: { parts: [{ type: 'file', mime: 'text/csv', url: 'data:text/csv;base64,YSxi', filename: 'rows.csv' }] },
  })
  check(badFile.status === 415, `non-image part refused 415 (${badFile.status})`)
  void png

  // ---- unknown session ------------------------------------------------------
  const q404 = await req('/oc/session/does-not-exist/queue')
  check(q404.status === 404, 'queue on unknown session is 404')

  await sleep(1500)
  const seen = await userMsgs(sid)
  check(seen.includes(a), 'idle prompt reached the transcript')
  check(!seen.includes(b) && !seen.includes(c), 'cleared messages never got delivered')
  check(!seen.includes(slash), 'slash command never became a transcript message')
} catch (e) {
  console.log('ERROR', e.stack || String(e))
  failed++
}

console.log('queue events:', queuesFor('').length || events.filter((e) => e.type === 'session.queue').length)
process.exit(failed ? 1 : 0)
