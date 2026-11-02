// Headless regression check: the token fields the webui sidebar row reads.
//
// The sidebar's token stat is GENERATED tokens = output + reasoning summed over
// every assistant message (webui/src/lib/gentokens.ts). It replaced the context
// estimate (`tokens`: one newest usage record, input+output+cacheRead), which
// reads as ~95%+ cache reads on real history and grows with turn count rather
// than work done. This check pins both fields on the projection so a future
// edit can't quietly redefine the row again:
//   1. `tokens_generated` sums output+reasoning across ALL assistant messages,
//      counts `reasoning`, and ignores user-message usage;
//   2. it is a different number from `tokens` (the two must not be conflated);
//   3. an all-zero usage record yields NO `tokens_generated` key at all — the
//      webui falls back rather than rendering 0 forever.
//
// Self-contained: seeds a scratch pi dir from generated fixtures, spawns pi-web,
// asserts over the HTTP API, kills its own child (exact PID).
//
//   node test/hist-check.mjs
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { REPO_CWD, TESTBIN, WEB, sessionSlug } from './harness.mjs'

// PI_WEB_JS lets the check run against another copy to prove it fails pre-fix.
const PI_WEB = process.env.PI_WEB_JS || path.join(WEB, 'pi-web.mjs')
const PORT = Number(process.env.HIST_CHECK_PORT || 8126)
const BASE = `http://127.0.0.1:${PORT}`
const CWD = REPO_CWD
const WS_SLUG = sessionSlug(CWD)

let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
  if (!ok) failed++
}

const usage = (u) => ({
  input: u.input ?? 0,
  output: u.output ?? 0,
  cacheRead: u.cacheRead ?? 0,
  cacheWrite: u.cacheWrite ?? 0,
  ...(u.reasoning !== undefined ? { reasoning: u.reasoning } : {}),
  totalTokens: (u.input ?? 0) + (u.output ?? 0),
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
})

// header + model_change + user + N assistant messages with the given usage
function writeSession(file, id, title, msgs) {
  const t = (n) => new Date(Date.parse('2026-01-01T00:00:00.000Z') + n).toISOString()
  const lines = [
    { type: 'session', version: 3, id, timestamp: t(0), cwd: CWD },
    { type: 'model_change', id: 'm1', parentId: null, timestamp: t(10), provider: 'testprov', modelId: 'fake-1' },
    { type: 'session_info', id: 'si1', parentId: 'm1', timestamp: t(11), name: title },
    {
      type: 'message', id: 'u1', parentId: 'm1', timestamp: t(20),
      // user messages carry usage in some pi builds — they must never count as
      // generated output
      message: { role: 'user', content: [{ type: 'text', text: 'go' }], usage: usage({ output: 999 }), timestamp: Date.parse(t(20)) },
    },
  ]
  let pid = 'u1'
  msgs.forEach((u, i) => {
    const mid = `a${i + 1}`
    lines.push({
      type: 'message', id: mid, parentId: pid, timestamp: t(40 + i * 20),
      message: { role: 'assistant', content: [{ type: 'text', text: `turn ${i + 1}` }], usage: usage(u), timestamp: Date.parse(t(40 + i * 20)) },
    })
    pid = mid
  })
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, lines.map((l) => JSON.stringify(l)).join('\n') + '\n')
}

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'piweb-hist-'))
const SDIR = path.join(scratch, 'sessions')
const fname = (id) => `2026-01-01T00-00-00-000Z_${id}.jsonl`

// A: two assistant turns. generated = (120+30) + (80+0) = 230.
//    context estimate (newest record) = 6000+80+0 = 6080 — deliberately ≠ 230.
const A = '01aa0000-0000-7000-8000-00000000000a'
writeSession(path.join(SDIR, WS_SLUG, fname(A)), A, 'hist-check fixture A', [
  { input: 5000, output: 120, reasoning: 30, cacheRead: 9000 },
  { input: 6000, output: 80, cacheRead: 0 },
])
// B: every usage record all-zero (aborted / provider-silent turns) → no key
const B = '01bb0000-0000-7000-8000-00000000000b'
writeSession(path.join(SDIR, WS_SLUG, fname(B)), B, 'hist-check fixture B', [
  { input: 0, output: 0, cacheRead: 0 },
])
// C: renamed twice. A rename APPENDS a session_info entry, so the CURRENT name is
// the LAST one — `entries.find` returned the creation title and the sidebar kept a
// stale name forever (real case: "llama server backup plan" vs renamed "pi-web ssh").
const C = '01cc0000-0000-7000-8000-00000000000c'
const fileC = path.join(SDIR, WS_SLUG, fname(C))
writeSession(fileC, C, 'named at creation', [{ input: 10, output: 40 }])
fs.appendFileSync(
  fileC,
  [
    { type: 'session_info', timestamp: '2026-01-01T00:00:00.500Z', name: 'renamed once' },
    { type: 'session_info', timestamp: '2026-01-01T00:00:00.600Z', name: 'renamed twice' },
  ]
    .map((e) => JSON.stringify(e) + '\n')
    .join(''),
)

const child = spawn(
  process.execPath,
  [PI_WEB, '--port', String(PORT), '--host', '127.0.0.1', '--dir', scratch, '--cwd', CWD],
  { cwd: WEB, env: { ...process.env, PATH: `${TESTBIN}:${process.env.PATH}` }, stdio: 'ignore', detached: true },
)
const stop = () => {
  try {
    process.kill(-child.pid, 'SIGKILL') // kill the whole group we spawned
  } catch {
    /* already gone */
  }
}

const j = async (p) => {
  try {
    const r = await fetch(BASE + p)
    return { status: r.status, body: await r.json().catch(() => null) }
  } catch {
    return { status: 0, body: null }
  }
}

try {
  let rows = []
  for (let i = 0; i < 60 && !rows.length; i++) {
    rows = (await j('/api/history/sessions')).body ?? []
    if (rows.find((r) => r.id === A) && rows.find((r) => r.id === B) && rows.find((r) => r.id === C)) break
    await new Promise((r) => setTimeout(r, 250))
  }
  const ra = rows.find((r) => r.id === A)
  const rb = rows.find((r) => r.id === B)
  const rc = rows.find((r) => r.id === C)
  check(!!ra && !!rb && !!rc, 'all fixture sessions are listed', `${rows.length} rows`)
  check(rc?.title === 'renamed twice', 'title is the CURRENT name (renames append; last session_info wins)', `title=${JSON.stringify(rc?.title)}`)
  check(rc?.title !== 'named at creation', 'the creation title never survives a rename', `title=${JSON.stringify(rc?.title)}`)
  if (ra) {
    check(ra.tokens_generated === 230, 'tokens_generated sums output+reasoning over every assistant msg', `got ${ra.tokens_generated}, want 230`)
    check(ra.tokens === 6080, 'tokens stays the newest-record context estimate', `got ${ra.tokens}, want 6080`)
    check(ra.tokens_generated !== ra.tokens, 'the two fields are not the same number', `${ra.tokens_generated} vs ${ra.tokens}`)
    check(
      ra.tokens_generated < 1000,
      'user-message usage (999 output) is not counted as generated',
      `generated=${ra.tokens_generated} (would be 1229 if the user row counted)`,
    )
  }
  if (rb) {
    check(rb.tokens_generated === undefined, 'all-zero usage emits no tokens_generated key', JSON.stringify(rb.tokens_generated))
  }
} finally {
  stop()
  try {
    fs.rmSync(scratch, { recursive: true, force: true })
  } catch {
    /* best effort */
  }
}

console.log(failed ? `\nhist-check: FAILED (${failed})` : '\nhist-check: ok')
process.exitCode = failed ? 1 : 0
