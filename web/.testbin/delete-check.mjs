// Headless regression check: deleting sessions from the webui.
//
// Two bugs this exists to keep dead:
//  1. soft-delete moved the JSONL into `sessions/trash/`, which listFiles()
//     walks like any workspace dir — so "deleted" sessions were re-listed
//     forever and looked undeletable.
//  2. a live sidebar row carries a TAU id (never part of the file name), so the
//     cold-file lookup could never find it; DELETE only closed the child and
//     left the JSONL on disk — the row came straight back on refresh.
//
// Self-contained: seeds a scratch pi dir from generated fixtures, spawns pi-web
// against the fake pi, asserts over the HTTP API, then kills the child.
//
//   cd web && node .testbin/delete-check.mjs
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { REPO_CWD, sessionSlug } from './harness.mjs'

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const TESTBIN = path.join(WEB, '.testbin')
// PI_WEB_JS lets the check run against another copy (e.g. `git show HEAD:...`)
// to confirm the assertions still catch the old bug.
const PI_WEB = process.env.PI_WEB_JS || path.join(WEB, 'pi-web.mjs')
const PORT = Number(process.env.DEL_CHECK_PORT || 8127)
const BASE = `http://127.0.0.1:${PORT}`
const CWD = REPO_CWD // the workspace the fixtures live in; CHECK_CWD overrides
const WS_SLUG = sessionSlug(CWD) // pi's own cwd -> session-dir encoding

let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// minimal pi JSONL: header + model_change + one user + one assistant message
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
      message: { role: 'assistant', content: [{ type: 'text', text: 'ok' }], timestamp: Date.parse(t(40)) },
    },
  ]
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, lines.map((l) => JSON.stringify(l)).join('\n') + '\n')
}

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'piweb-del-'))
const SDIR = path.join(scratch, 'sessions')
const TRASH = path.join(scratch, 'sessions-trash')
const META = path.join(WEB, '.meta.json') // pi-web's sidecar (gitignored, ephemeral)

const A = '01aa0000-0000-7000-8000-00000000000a'
const B = '01bb0000-0000-7000-8000-00000000000b'
const LEGACY = '01cc0000-0000-7000-8000-00000000000c'
const C = '01dd0000-0000-7000-8000-00000000000d'
const D = '01ee0000-0000-7000-8000-00000000000e'
const fname = (id) => `2026-01-01T00-00-00-000Z_${id}.jsonl`
writeSession(path.join(SDIR, WS_SLUG, fname(A)), A, 'delete-check fixture session A')
writeSession(path.join(SDIR, WS_SLUG, fname(B)), B, 'delete-check fixture session B')
writeSession(path.join(SDIR, WS_SLUG, fname(C)), C, 'delete-check fixture session C (starred)')
writeSession(path.join(SDIR, WS_SLUG, fname(D)), D, 'delete-check fixture session D (tau-id delete)')
// the in-tree trash older builds left behind: must never be listed again
writeSession(path.join(SDIR, 'trash', fname(LEGACY)), LEGACY, 'already deleted by an old build')

const child = spawn(
  process.execPath,
  [PI_WEB, '--port', String(PORT), '--host', '127.0.0.1', '--dir', scratch, '--cwd', CWD],
  { cwd: WEB, env: { ...process.env, PATH: `${TESTBIN}:${process.env.PATH}` }, stdio: 'ignore', detached: true },
)

const j = async (p, init) => {
  let r
  try {
    r = await fetch(BASE + p, init)
  } catch {
    return { status: 0, body: null } // connection refused while booting
  }
  let body = null
  try {
    body = await r.json()
  } catch {
    /* non-JSON */
  }
  return { status: r.status, body }
}
const list = async () => (await j('/api/history/sessions')).body ?? []
const live = async () => (await j('/oc/session')).body ?? []
const filesUnder = (dir, id) => {
  const out = []
  const walk = (d) => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, e.name)
      if (e.isDirectory()) walk(p)
      else if (p.includes(id)) out.push(p)
    }
  }
  try {
    walk(dir)
  } catch {
    /* missing dir */
  }
  return out
}

let childExited = false
try {
  // wait for boot
  let up = false
  for (let i = 0; i < 60 && !up; i++) {
    up = (await j('/healthz')).body?.ok === true
    if (!up) await sleep(250)
  }
  check(up, 'pi-web booted against the fake pi')
  if (!up) throw new Error('server never came up')

  // --- listing: real sessions in, old in-tree trash out -------------------
  let rows = await list()
  const ids = rows.map((r) => r.id)
  check(ids.includes(A) && ids.includes(B), `fixture sessions listed (${ids.length} rows)`)
  check(!ids.includes(LEGACY), 'sessions/trash/ contents are NOT listed')

  // --- 1) cold session delete --------------------------------------------
  const d1 = await j(`/oc/session/${A}`, { method: 'DELETE' })
  check(d1.status === 200 && d1.body?.ok && d1.body?.trashed === true, `cold DELETE ok+trashed (${JSON.stringify(d1.body)})`)
  check(filesUnder(SDIR, A).length === 0, 'deleted JSONL gone from the sessions tree')
  check(filesUnder(TRASH, A).length === 1, 'deleted JSONL is in the out-of-tree trash')
  rows = await list()
  check(!rows.some((r) => r.id === A), 'cold session no longer listed after delete')
  check((await j(`/oc/session/${A}`)).status === 404, 'deleted session reopens as 404')

  // --- 2) live session delete: the row carries pi's FILE UUID, the same id the
  //         tab holds — one session, one id, so a row click can't open a second
  //         tab under a tau id (that is how duplicate tabs used to appear)
  check((await j(`/oc/session/${B}`)).status === 200, 'open session B (resumes cold -> live)')
  rows = await list()
  const row = rows.find((r) => r.live)
  check(!!row, 'history row for the live session is marked live')
  check(!!row && row.id === B, `live row id is the file uuid (${row?.id})`)
  const d2 = await j(`/oc/session/${row.id}`, { method: 'DELETE' })
  check(d2.status === 200 && d2.body?.ok && d2.body?.trashed === true, `live DELETE by uuid ok+trashed (${JSON.stringify(d2.body)})`)
  check(filesUnder(SDIR, B).length === 0, 'live session JSONL removed from the tree')
  check(filesUnder(TRASH, B).length === 1, 'live session JSONL is in the trash')
  check((await live()).length === 0, `live child closed (${(await live()).length} left)`)
  check(!(await list()).some((r) => r.id === B || r.live), 'live row gone from the list (no resurrection)')

  // --- 2b) DELETE by the internal tau id must STILL work: tabs opened before the
  //          canonical-uuid change hold that spelling
  check((await j(`/oc/session/${D}`)).status === 200, 'open session D')
  const tauId = Object.keys((await j('/oc/session/status')).body ?? {}).find((k) => k.startsWith('tau_'))
  check(!!tauId, `status still exposes the tau-id alias (${tauId})`)
  const d2b = await j(`/oc/session/${tauId}`, { method: 'DELETE' })
  check(d2b.status === 200 && d2b.body?.ok && d2b.body?.trashed === true, `live DELETE by tau id still works (${JSON.stringify(d2b.body)})`)
  check(filesUnder(SDIR, D).length === 0, 'tau-id delete trashed the file too')

  // --- 3) brand-new live chat, never messaged (no file on disk) -----------
  const created = await j('/oc/session', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ cwd: CWD }) })
  const nid = created.body?.id
  check(!!nid, `new live session created (${nid})`)
  const d3 = await j(`/oc/session/${nid}`, { method: 'DELETE' })
  check(d3.status === 200 && d3.body?.ok && d3.body?.trashed === false, `fileless DELETE ok (${JSON.stringify(d3.body)})`)
  check((await live()).length === 0, 'fileless child closed')

  // --- 4) unknown id + sidecar cleanup -----------------------------------
  check((await j('/oc/session/00000000-0000-7000-8000-000000000000', { method: 'DELETE' })).status === 404, 'unknown session DELETE -> 404')
  await j(`/api/history/session/${C}/meta`, { method: 'PUT', headers: { 'content-type': 'application/json' }, body: '{"star":true}' })
  check((await list()).some((r) => r.id === C && r.star), 'star round-trips into the history row')
  const d4 = await j(`/oc/session/${C}`, { method: 'DELETE' })
  check(d4.status === 200 && d4.body?.trashed === true, 'starred session deletes normally')
  let sidecar = {}
  try {
    sidecar = JSON.parse(fs.readFileSync(META, 'utf8'))
  } catch {
    /* no sidecar yet */
  }
  check(sidecar[C] === undefined, 'star/tag sidecar entry dropped with the session')
} catch (e) {
  check(false, `harness error: ${e?.stack ?? e}`)
} finally {
  try {
    process.kill(-child.pid) // detached -> its own process group
  } catch {
    childExited = true
  }
  fs.rmSync(scratch, { recursive: true, force: true })
}
console.log(failed ? `\nFAILED ${failed}` : '\nALL OK')
process.exit(failed ? 1 : 0)
