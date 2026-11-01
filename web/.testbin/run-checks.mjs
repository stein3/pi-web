/**
 * The suite runner behind `npm run check`.
 *
 * One check per LINE in CHECKS below. The old suite was one giant `&&` chain
 * in package.json's "check" script — every added check rewrote that line, so
 * two branches touching the suite were a guaranteed merge conflict. Here a
 * branch adds a line and the lists merge side by side.
 *
 * Semantics are the old chain's, exactly:
 *   - checks run in order; the first failure stops the run and becomes the
 *     exit code;
 *   - `optional: true`: exit 2 is a loud SKIP (never a silent pass) and the
 *     run continues, anything else is a real failure, and a MISSING file is a
 *     failure too — a check that cannot even start proved nothing either way;
 *   - required checks may not skip: any nonzero exit is a failure.
 *
 * (The same table for one-off runs is .testbin/optional.mjs.)
 *
 * Ports: every check gets its OWN CHECK_PORT/ENGINE_PORT pair (see
 * harness.mjs) instead of the chain's shared 8126/8127 — two checks sharing
 * one pair back-to-back can flake on the previous child's teardown. The pair
 * is derived from the check's position, so it is stable across subsets.
 * CHECK_PORT=<n> at the runner shifts the base (for a concurrent suite run).
 * The checks each read their own historical port name (TITLE_CHECK_PORT,
 * DEL_CHECK_PORT, …) with a hardcoded default; the runner sets ALL of those
 * names to the check's pair, so no check ever silently lands on its default
 * and into a neighbour's port.
 *
 * Leaks: each check runs as its own process GROUP and the runner reaps that
 * group when the check ends and on SIGINT/SIGTERM — an interrupted run can
 * leave a pi-web child holding a port (a run cut short mid-compact-check did
 * exactly that), and the next run then dies on a pre-flight guard instead of
 * testing anything. The negative PID is the group THIS spawn created.
 *
 *   node .testbin/run-checks.mjs                  the whole suite
 *   node .testbin/run-checks.mjs hist-check.mjs   just the named checks
 *                                                (substring match, for
 *                                                iterating on one area)
 */
import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const HERE = path.dirname(fileURLToPath(import.meta.url))

// ONE CHECK PER LINE — a new check is a new line, never a rewritten chain.
const CHECKS = [
  'titlefill-check.mjs',
  'diff-check.mjs',
  'delete-check.mjs',
  'compact-check.mjs',
  'hist-check.mjs',
  'title-check.mjs',
  'id-check.mjs',
  'tooltitle-check.mjs',
  'revert-check.mjs',
  'fork-check.mjs',
  'cwd-check.mjs',
  'fsbrowse-check.mjs',
  'stream-check.mjs',
  'toolout-check.mjs',
  'telemetry-check.mjs',
  'thinking-check.mjs',
  { name: 'client-check.mjs', optional: true },
  'filter-check.mjs',
  { name: 'dot-check.mjs', optional: true },
  { name: 'box-check.mjs', optional: true },
  { name: 'fsbox-check.mjs', optional: true },
]

const entries = CHECKS.map((c) => (typeof c === 'string' ? { name: c, optional: false } : c))

const args = process.argv.slice(2)
let selected = entries
if (args.length) {
  selected = entries.filter((e) => args.some((a) => e.name.includes(a)))
  const unmatched = args.filter((a) => !entries.some((e) => e.name.includes(a)))
  if (unmatched.length) {
    console.error(`no check matches: ${unmatched.join(', ')}`)
    console.error(`known: ${entries.map((e) => e.name).join(' ')}`)
    process.exit(1)
  }
}

// every *_PORT name any suite check reads (they each kept their historical
// own name); all get the check's pair. ENGINE_PORT is the fixture-engine slot.
const PORT_ALIASES = [
  'CHECK_PORT',
  'TITLE_CHECK_PORT',
  'DEL_CHECK_PORT',
  'STREAM_CHECK_PORT',
  'TOOLTITLE_CHECK_PORT',
  'TOOL_OUT_PORT',
  'THINKING_CHECK_PORT',
  'REVERT_CHECK_PORT',
  'ID_CHECK_PORT',
  'HIST_CHECK_PORT',
  'FSBROWSE_CHECK_PORT',
  'FSBOX_CHECK_PORT',
  'FORK_CHECK_PORT',
  'CWD_CHECK_PORT',
]

const base = Number(process.env.CHECK_PORT || 8126)

function envFor(i) {
  const port = base + 2 * i
  const env = { ...process.env, ENGINE_PORT: String(port + 1) }
  for (const name of PORT_ALIASES) env[name] = String(port)
  return env
}

// run a check in its own process group; returns { code, error }
function run(target, env) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [target], {
      cwd: process.cwd(),
      stdio: 'inherit',
      env,
      detached: true, // own process group: reapable in one kill(-pid)
    })
    current = child.pid
    const reap = () => {
      try {
        process.kill(-child.pid, 'SIGKILL') // the group this spawn created
      } catch {
        /* already gone */
      }
    }
    child.on('exit', (code, sig) => {
      current = null
      reap() // survivors (a leaked pi-web child) must not outlive the check
      resolve({ code: sig ? 99 : (code ?? 99) })
    })
    child.on('error', (error) => {
      current = null
      resolve({ code: 99, error })
    })
  })
}

let current = null
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    if (current) {
      try {
        process.kill(-current, 'SIGKILL')
      } catch {
        /* already gone */
      }
    }
    process.exit(1)
  })
}

function fmt(ms) {
  return `${(ms / 1000).toFixed(1)}s`
}

let failed = null
const ran = []
for (const e of selected) {
  const i = entries.indexOf(e)
  const target = path.join(HERE, e.name)
  if (!fs.existsSync(target)) {
    console.error(`\n!!! ${e.name}: no such check (${target}) — a missing file, not a SKIP`)
    failed = { name: e.name, code: 1 }
    break
  }
  console.log(`\n===== ${e.name}${e.optional ? ' (optional)' : ''} — port ${base + 2 * i} =====`)
  const t0 = Date.now()
  const res = await run(target, envFor(i))
  const ms = Date.now() - t0
  const code = res.error ? 99 : res.code
  if (e.optional && code === 2) {
    console.log(`\n===== ${e.name}: SKIPPED (dependency absent — NOT a pass) =====`)
    console.log('   set the dependency and re-run; a SKIP means this check proved nothing.')
  } else if (code !== 0) {
    ran.push({ name: e.name, ms, code })
    failed = { name: e.name, code }
    break
  }
  ran.push({ name: e.name, ms, code })
}

console.log('\n===== suite =====')
for (const r of ran) console.log(`  ${fmt(r.ms).padStart(7)}  ${r.name}`)
console.log(`  ${fmt(ran.reduce((a, r) => a + r.ms, 0)).padStart(7)}  total`)
if (failed) {
  console.log(`\n!!! ${failed.name} FAILED (exit ${failed.code}) — suite stopped`)
  process.exit(failed.code)
}
console.log('\nsuite OK' + (ran.some((r) => r.code === 2) ? ' (with SKIPs above)' : ''))
