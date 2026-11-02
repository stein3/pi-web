// ---------------------------------------------------------------------------
// filter-check — what the picker must NOT offer.
//
// picker.json is the one file a hand-managed "models I do not want" can
// live in (the store is a cache pi rewrites; models.json is pi's compose
// layer). The bridge strips catalogue entries whose id — or "provider/id" —
// matches a `hidden` glob pattern, case-insensitively, before the response is
// written. No probing, no network: patterns are the whole mechanism.
//
// Asserted here:
//   1. patterns strip every source the catalogue merges (store row,
//      models.json def row, cli-merged builtin row);
//   2. glob semantics: bare ids, `*` wildcards, provider-prefixed patterns,
//      and `*Contributor`-style case-insensitive display-word patterns;
//   3. one id hidden never hides its siblings;
//   4. nothing regressed by a restart — the config is re-read from disk.
//
// Self-contained: seeds a scratch agent dir, spawns pi-web with its own `pi`
// shim (PATH beats the test dir's), kills its own child.
//
//   node test/filter-check.mjs
//   PI_WEB_JS=<other build> node test/filter-check.mjs   # prove it fails pre-fix
// ---------------------------------------------------------------------------
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import path from 'node:path'
import { BRIDGE, CHECK_PORT, ENGINE_PORT, REPO_CWD, TESTBIN, WEB } from './harness.mjs'

const PORT = Number(process.env.FILTER_CHECK_PORT || CHECK_PORT)
const BASE = `http://127.0.0.1:${PORT}`
const GATEWAY = `http://127.0.0.1:${ENGINE_PORT}`
const SDIR = path.join(WEB, '.logs', 'filter-check')
const AGENT = path.join(SDIR, 'pi-agent')

let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
  if (!ok) failed++
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const req = (p) =>
  new Promise((resolve, reject) => {
    const r = http.request(new URL(BASE + p), { method: 'GET' }, (res) => {
      let buf = ''
      res.on('data', (c) => (buf += c))
      res.on('end', () => resolve({ status: res.statusCode, body: buf }))
    })
    r.on('error', reject)
    r.end()
  })

// The catalogue's cli layer: this check's own shim answers --list-models with
// a builtin-shaped provider row no config file mentions, so the merge pass is
// exercised too.
const BIN = path.join(SDIR, 'bin')
const SHIM = path.join(BIN, 'pi')

// A stub gateway: the store row's routerBase poll hits /v1/models and must not
// crash the bridge. Nothing else talks to it.
const gateway = http.createServer((rq, rs) => {
  rs.writeHead(404)
  rs.end('{}')
})
await new Promise((done) => gateway.listen(ENGINE_PORT, '127.0.0.1', done))

function seed() {
  // main rmSync()s SDIR first, so everything under it — shim included — is
  // written here and never before.
  fs.mkdirSync(BIN, { recursive: true })
  fs.mkdirSync(AGENT, { recursive: true })
  fs.writeFileSync(
    SHIM,
    `#!/bin/sh
[ "$1" = "--list-models" ] || { echo "unsupported: $1" >&2; exit 1; }
cat <<'EOF'
provider      model             context  max-out  thinking  images
opencode-go   grok-4.7-stub     131.1K   4K       yes       no
opencode-go   keep-me           131.1K   4K       yes       no
opencode-go   visible-go        131.1K   4K       yes       no
opencode-go   test-contributor  1.0M     65.5K    yes       yes
testprov      hidden-cli        128K     4K       yes       no
testprov      visible-cli       128K     4K       yes       no
testprov      cli-contributor   128K     4K       yes       no
EOF
`,
  )
  fs.chmodSync(SHIM, 0o755)
  fs.writeFileSync(
    path.join(AGENT, 'models-store.json'),
    JSON.stringify({
      'fake-router': {
        models: [
          {
            id: 'visible-store',
            name: 'visible-store',
            api: 'openai-completions',
            baseUrl: `${GATEWAY}/v1`,
            contextWindow: 32768,
            maxTokens: 4096,
            reasoning: false,
            input: ['text'],
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          },
          {
            id: 'grok-store-stub',
            name: 'grok-store-stub',
            api: 'openai-completions',
            baseUrl: `${GATEWAY}/v1`,
            contextWindow: 32768,
            maxTokens: 4096,
            reasoning: false,
            input: ['text'],
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          },
        ],
        checkedAt: Date.now(),
      },
    }),
  )
  fs.writeFileSync(
    path.join(AGENT, 'models.json'),
    JSON.stringify({
      providers: {
        suite: {
          api: 'openai-completions',
          models: [
            {
              id: 'visible-def',
              name: 'visible-def',
              baseUrl: `${GATEWAY}/other/v1`,
              reasoning: true,
              contextWindow: 8192,
              maxTokens: 1024,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
            },
            {
              id: 'muse-def-stub',
              name: 'muse-def-stub',
              baseUrl: `${GATEWAY}/other/v1`,
              reasoning: true,
              contextWindow: 8192,
              maxTokens: 1024,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
            },
          ],
        },
      },
    }),
  )
  fs.writeFileSync(path.join(AGENT, 'auth.json'), JSON.stringify({}), { mode: 0o600 })
  fs.writeFileSync(
    path.join(AGENT, 'picker.json'),
    JSON.stringify({
      mode: 'all',
      hidden: ['grok-*', 'muse-*', 'hidden-cli', 'opencode-go/keep-me', 'visible-def', '*Contributor'],
    }),
  )
  fs.writeFileSync(path.join(AGENT, 'settings.json'), '{}\n')
}

let child = null

async function spawnBridge() {
  child = spawn(
    process.execPath,
    [BRIDGE, '--port', String(PORT), '--host', '127.0.0.1', '--dir', AGENT, '--cwd', REPO_CWD],
    {
      cwd: WEB,
      env: { ...process.env, PATH: `${BIN}:${TESTBIN}:${process.env.PATH}` },
      stdio: ['ignore', 'inherit', 'inherit'],
      detached: true,
    },
  )
  for (let i = 0; i < 150; i++) {
    if (await req('/status').catch(() => null)) return
    await sleep(100)
  }
  console.log('FAIL pi-web did not come up on ' + PORT)
  failed += 1
}

async function reapBridge() {
  if (!child?.pid) return
  try {
    process.kill(-child.pid, 'SIGKILL') // the group we spawned, never a pattern
  } catch {}
  child = null
  await sleep(250) // give the OS the port back before a respawn
}

const idsOf = (body) =>
  Object.fromEntries((JSON.parse(body)?.providers ?? []).map((p) => [p.id, Object.keys(p.models).sort()]))

function assertCatalog(first, label) {
  check(
    !(first['fake-router'] ?? []).includes('grok-store-stub') && (first['fake-router'] ?? []).includes('visible-store'),
    `${label}: store row respects patterns`,
    JSON.stringify(first['fake-router']),
  )
  check(
    !(first['suite'] ?? []).includes('muse-def-stub') && !(first['suite'] ?? []).includes('visible-def'),
    `${label}: models.json def row respects patterns both ways`,
    JSON.stringify(first['suite']),
  )
  check(
    !(first['opencode-go'] ?? []).includes('grok-4.7-stub') &&
      !(first['opencode-go'] ?? []).includes('keep-me') &&
      (first['opencode-go'] ?? []).includes('visible-go'),
    `${label}: cli-merged builtin row respects bare and provider-prefixed patterns`,
    JSON.stringify(first['opencode-go']),
  )
  check(
    !(first['opencode-go'] ?? []).includes('test-contributor') && !(first['testprov'] ?? []).includes('cli-contributor'),
    `${label}: *Contributor matches across providers, case-insensitively`,
    JSON.stringify([first['opencode-go'], first['testprov']]),
  )
  check(
    !(first['testprov'] ?? []).includes('hidden-cli') && (first['testprov'] ?? []).includes('visible-cli'),
    `${label}: bare-id pattern hides one, siblings in the same provider survive`,
    JSON.stringify(first['testprov']),
  )
}

// main
fs.rmSync(SDIR, { recursive: true, force: true })
seed()
await spawnBridge()

try {
  const r1 = await req('/oc/config/providers')
  assertCatalog(idsOf(r1.body), 'render')

  // a restart re-reads the config from disk — nothing may reappear
  await reapBridge()
  await spawnBridge()
  const r2 = await req('/oc/config/providers')
  assertCatalog(idsOf(r2.body), 'after restart')
} finally {
  await reapBridge()
  gateway.close()
}

console.log(failed ? `FAILED ${failed}` : 'ALL CHECKS PASSED')
process.exit(failed ? 1 : 0)
