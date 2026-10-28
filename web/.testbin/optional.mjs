/**
 * Run a check whose DEPENDENCY may be missing, without letting that either
 * fake a pass or fake a failure.
 *
 * `npm run check` is one `&&` chain, so a check that must SKIP when a resource
 * this box does not have (the sibling webui checkout, chromium, a live server)
 * cannot express that: exit 2 aborts the chain and reads as "the product is
 * broken". So the checks exit 2 with a SKIP message (harness.mjs's loadE2E,
 * webuiSkip) and this wrapper translates it for the chain:
 *
 *   exit 0  → pass, chain continues (output kept as-is)
 *   exit 2  → SKIP: a loud banner, chain continues — NEVER a silent pass
 *   else    → the check itself failed: chain stops, its output is not swallowed
 *   99      → the check file is missing / the runner died: a check that cannot
 *             even start is a product failure, never a SKIP
 *
 *   node .testbin/optional.mjs client-check.mjs
 */
import fs from 'node:fs'
import path from 'node:path'
import { spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const name = process.argv[2]
if (!name) {
  console.error('usage: node .testbin/optional.mjs <check.mjs> [args…]')
  process.exit(99)
}
const target = path.join(HERE, name)
if (!fs.existsSync(target)) {
  console.error(`\n!!! ${name}: no such check (${target}) — that is a missing file, not a SKIP`)
  process.exit(99)
}

const res = spawnSync(process.execPath, [target, ...process.argv.slice(3)], {
  cwd: process.cwd(),
  stdio: 'inherit',
  env: process.env,
})
if (res.error) {
  console.error(`\n!!! ${name}: could not run: ${res.error.message}`)
  process.exit(99)
}
const code = res.status ?? 99
if (code === 0) process.exit(0)
if (code === 2) {
  console.log(`\n===== ${name}: SKIPPED (dependency absent — NOT a pass) =====`)
  console.log('   set the dependency and re-run; a SKIP means this check proved nothing.')
  process.exit(0)
}
console.log(`\n!!! ${name} FAILED (exit ${code})`)
process.exit(1)
