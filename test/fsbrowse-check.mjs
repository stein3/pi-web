// File browser routes: /oc/fs/{list,file,find,status,diff} — read-only, rooted
// at a SESSION cwd, ceiling-bounded at the bridge's --cwd.
//
// What this check exists to hold, in priority order:
//
//   1. CONTAINMENT. These routes hand out file contents and directory listings
//      for whatever cwd the browser names, so the only thing between them and an
//      arbitrary-read endpoint is the ceiling: a cwd/path that resolves above
//      --cwd must 403 and name the ceiling, `..` must not walk out, and a
//      SYMLINK that points out must not be followed (tau's rule — realpath both
//      sides and reject the escape). A symlink that stays inside must still
//      work, or the rule has become a bug.
//   2. No global-root fallback. A request with no cwd, or a cwd that is not a
//      directory, answers 4xx. It never quietly lists PI_CWD: the drawer follows
//      the tab, and a session whose workspace vanished must not get the whole
//      workspace tree handed to it.
//   3. POLICY parity with tau's browser, which the webui renders against: the
//      ignore set, dotfiles-hidden-except-.env, dir-first sort, and the caps
//      (500 entries, 200 find matches, 1 MiB into the viewer, depth 12) — each
//      with `truncated` said out loud rather than a short list that reads as "no
//      more files".
//   4. git is the dirty indicator, and it is READ-ONLY git: status/diff/show
//      only. Not a repo => {repo:false}, not an error. When the repo's top sits
//      ABOVE the ceiling (the ordinary shape of --cwd one level inside a repo)
//      the dirty set must be cut at the ceiling — naming files this server is
//      forbidden to read would leak exactly what rule 1 exists to hide. The
//      worktree here also routinely carries another agent's in-flight edits, so
//      a mutating git command here is a data-loss bug, not a style problem.
//
// Self-contained: seeds a scratch tree (plus two fixture git repos) in a tmp
// dir, spawns pi-web with --cwd = that dir, kills the exact PID it started. The
// "outside the ceiling" fixture is a SIBLING of the scratch dir, which is what
// makes the ceiling a real test rather than a tautology. No model, no pi child,
// no real transcripts.
//
//   node test/fsbrowse-check.mjs
//   PI_WEB_JS=/path/to/pre-fix/pi-web.mjs node test/fsbrowse-check.mjs  # prove it fails
import { execFileSync, spawn } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { BRIDGE, WEB } from './harness.mjs'

const PORT = Number(process.env.FSBROWSE_CHECK_PORT || 8136)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
  if (!ok) failed++
}

async function get(port, p) {
  const r = await fetch(`http://127.0.0.1:${port}${p}`).catch((e) => ({ fetchError: String(e) }))
  if (r.fetchError) return { status: 0, body: null, err: r.fetchError }
  return { status: r.status, body: await r.json().catch(() => null) }
}
const fsq = (port, route, cwd, extra = '') =>
  get(port, `/oc/fs/${route}?cwd=${encodeURIComponent(cwd)}${extra ? `&${extra}` : ''}`)
const names = (b) => (b?.items ?? []).map((i) => i.name)
const has = (b, n) => names(b).includes(n)

// --------------------------------------------------------------------------
// fixtures. CEIL is the bridge's --cwd; OUT is its SIBLING, i.e. one level
// above the ceiling, so every escape assertion is testing something real.
// --------------------------------------------------------------------------
const SCRATCH = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'fsbrowse-check-')))
const CEIL = SCRATCH // == --cwd
const BOX = path.join(CEIL, 'box') // a session root inside the ceiling
const OUT = SCRATCH + '-outside' // ABOVE the ceiling
const AGENT = path.join(CEIL, 'pi-agent')
const REPO = path.join(BOX, 'repo')
const REPO2 = path.join(BOX, 'repo2')

function git(cwd, ...args) {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf8',
    env: {
      ...process.env,
      GIT_CONFIG_GLOBAL: '/dev/null',
      GIT_CONFIG_SYSTEM: '/dev/null',
      GIT_AUTHOR_NAME: 't',
      GIT_AUTHOR_EMAIL: 't@x',
      GIT_COMMITTER_NAME: 't',
      GIT_COMMITTER_EMAIL: 't@x',
      GIT_AUTHOR_DATE: '2026-01-01T00:00:00Z',
      GIT_COMMITTER_DATE: '2026-01-01T00:00:00Z',
    },
  })
}

function fixtures() {
  for (const d of [SCRATCH, OUT]) fs.rmSync(d, { recursive: true, force: true })
  fs.mkdirSync(path.join(AGENT, 'sessions'), { recursive: true })
  fs.mkdirSync(BOX, { recursive: true })
  fs.mkdirSync(OUT, { recursive: true })

  fs.writeFileSync(path.join(BOX, 'keep.txt'), 'l1\nl2\nl3\n')
  fs.writeFileSync(path.join(BOX, '.env'), 'SECRET=keepme\n')
  fs.writeFileSync(path.join(BOX, '.hidden'), 'nope\n')
  fs.writeFileSync(path.join(BOX, 'alpha.txt'), 'this file body holds zzzneedle in its contents\n')
  fs.writeFileSync(path.join(BOX, 'nul.bin'), Buffer.from([0x61, 0x00, 0x62]))
  fs.writeFileSync(path.join(BOX, 'big.txt'), 'x'.repeat(1024 * 1024 + 1))
  // 1x1 png, for the image branch
  fs.writeFileSync(
    path.join(BOX, 'shot.png'),
    Buffer.from(
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==',
      'base64',
    ),
  )

  // ignored names (tau's set) must never surface
  for (const [d, f] of [
    ['node_modules', 'junk.js'],
    ['dist', 'app.js'],
    ['__pycache__', 'x.pyc'],
    ['.venv', 'y.py'],
  ]) {
    fs.mkdirSync(path.join(BOX, d), { recursive: true })
    fs.writeFileSync(path.join(BOX, d, f), 'junk\n')
  }
  fs.mkdirSync(path.join(BOX, 'sub'), { recursive: true })
  fs.writeFileSync(path.join(BOX, 'sub', 'one.txt'), 'one\n')

  // 500-entry cap: 600 files in one dir
  fs.mkdirSync(path.join(BOX, 'many'), { recursive: true })
  for (let i = 0; i < 600; i++) fs.writeFileSync(path.join(BOX, 'many', `f${i}.txt`), '')
  // 200-match cap: 250 matching names
  fs.mkdirSync(path.join(BOX, 'manies'), { recursive: true })
  for (let i = 0; i < 250; i++) fs.writeFileSync(path.join(BOX, 'manies', `match${i}.txt`), '')

  // depth cap (12): a needle at depth 11, another at depth 15
  let deep = BOX
  for (let i = 1; i <= 15; i++) {
    deep = path.join(deep, `deepd${String(i).padStart(2, '0')}`)
    fs.mkdirSync(deep, { recursive: true })
    if (i === 11) fs.writeFileSync(path.join(deep, 'shallowneedle.txt'), '')
    if (i === 15) fs.writeFileSync(path.join(deep, 'deepneedle.txt'), '')
  }

  // symlinks: one escaping the ceiling (file), one escaping (dir), one safe
  fs.writeFileSync(path.join(OUT, 'secret.txt'), 'ABOVE THE CEILING\n')
  fs.writeFileSync(path.join(OUT, 'secretsub.txt'), 'ABOVE THE CEILING\n')
  fs.symlinkSync(path.join(OUT, 'secret.txt'), path.join(BOX, 'link-out'))
  fs.symlinkSync(OUT, path.join(BOX, 'link-dir'))
  fs.symlinkSync('../keep.txt', path.join(BOX, 'sub', 'link-in'))

  // ---- a fixture repo: modified / added / deleted / untracked / renamed ----
  fs.mkdirSync(REPO, { recursive: true })
  fs.writeFileSync(path.join(REPO, 'a.txt'), 'l1\nl2\nl3\n')
  fs.mkdirSync(path.join(REPO, 'sub'), { recursive: true })
  fs.writeFileSync(path.join(REPO, 'sub', 'b.txt'), 'b-line\n')
  fs.writeFileSync(path.join(REPO, 'r_old.txt'), 'moved me\n')
  git(REPO, 'init', '-q', '.')
  git(REPO, 'add', '-A')
  git(REPO, '-c', 'commit.gpgsign=false', 'commit', '-qm', 'init')
  git(REPO, 'branch', '-M', 'main')
  fs.writeFileSync(path.join(REPO, 'a.txt'), 'l1\nCHANGED\nl3\n') // modified, then staged
  git(REPO, 'add', 'a.txt') // `M ` — one badge word with ` M`
  fs.writeFileSync(path.join(REPO, 'c.txt'), 'new staged\n')
  git(REPO, 'add', 'c.txt') // added
  fs.rmSync(path.join(REPO, 'sub', 'b.txt')) // deleted
  fs.writeFileSync(path.join(REPO, 'untracked.txt'), 'untracked\n')
  git(REPO, 'mv', '-f', 'r_old.txt', 'r_new.txt') // renamed

  // ---- a second repo, dirtier than the configured status cap ----
  fs.mkdirSync(REPO2, { recursive: true })
  git(REPO2, 'init', '-q', '.')
  fs.writeFileSync(path.join(REPO2, 'seed.txt'), 'seed\n')
  git(REPO2, 'add', '-A')
  git(REPO2, '-c', 'commit.gpgsign=false', 'commit', '-qm', 'init')
  for (let i = 0; i < 8; i++) fs.writeFileSync(path.join(REPO2, `dirty${i}.txt`), 'x\n')
}

const servers = []
const spawnBridge = (port, cwd) =>
  spawn(process.execPath, [BRIDGE, '--port', String(port), '--host', '127.0.0.1', '--dir', AGENT, '--cwd', cwd], {
    cwd: WEB,
    env: {
      ...process.env,
      PI_CODING_AGENT_DIR: AGENT,
      PI_WEB_QUIET: '1',
      PI_WEB_REAP_INTERVAL_MS: '3600000',
      PI_WEB_FS_STATUS_CAP: '6', // production default is 20k; a fixture repo of 8 must refuse
    },
    stdio: 'ignore',
    detached: true,
  })
async function boot(port, cwd) {
  const srv = spawnBridge(port, cwd)
  servers.push(srv)
  for (let i = 0; i < 60; i++) {
    if ((await get(port, '/healthz')).status === 200) return true
    await sleep(200)
  }
  return false
}
const cleanup = () => {
  for (const srv of servers) {
    if (!srv?.pid) continue
    try {
      process.kill(-srv.pid, 'SIGKILL')
    } catch {}
    try {
      process.kill(srv.pid, 'SIGKILL')
    } catch {}
  }
  if (!process.env.FSBROWSE_KEEP) {
    for (const d of [SCRATCH, OUT]) fs.rmSync(d, { recursive: true, force: true })
  }
}
process.on('exit', cleanup)

// --------------------------------------------------------------------------
// cases (P = the port that answers; ceiling is CEIL for all but the last)
// --------------------------------------------------------------------------
const P = PORT
async function caseRoutes() {
  const r = await fsq(P, 'list', BOX)
  check(r.status === 200, `GET /oc/fs/list answers 200 (${r.status})`)
  check(Array.isArray(r.body?.items), 'the answer carries an items array')
  check(typeof r.body?.path === 'string', 'and the directory it listed', JSON.stringify(r.body?.path))
  const it = r.body?.items?.[0]
  check(
    it && typeof it.name === 'string' && typeof it.path === 'string' && typeof it.isDirectory === 'boolean' &&
      (it.size === null || typeof it.size === 'number') && typeof it.mtime === 'number',
    'each item has name/path/isDirectory/size(null for dirs)/mtime',
    JSON.stringify(it),
  )
  const f = (r.body?.items ?? []).find((i) => !i.isDirectory)
  check(typeof f?.size === 'number' && f.size > 0, 'a file reports its size in bytes', JSON.stringify(f))
  check(r.body?.root === BOX, 'the answer says which root it used', JSON.stringify(r.body?.root))

  // policy: ignore set, dotfiles-except-.env, dir-first then A→Z
  check(has(r.body, 'keep.txt') && has(r.body, 'sub'), 'real entries are listed', names(r.body).slice(0, 6).join(','))
  check(!has(r.body, 'node_modules') && !has(r.body, 'dist') && !has(r.body, '__pycache__') && !has(r.body, '.venv'),
    "tau's ignore set is applied", names(r.body).join(','))
  check(has(r.body, '.env'), '.env stays visible (the reason to open a drawer)')
  check(!has(r.body, '.hidden'), 'other dotfiles stay hidden')
  const dirs = (r.body?.items ?? []).filter((i) => i.isDirectory).map((i) => i.name)
  const files = (r.body?.items ?? []).filter((i) => !i.isDirectory).map((i) => i.name)
  const orderOk = JSON.stringify(r.body?.items?.map((i) => i.isDirectory)) ===
    JSON.stringify([...dirs.map(() => true), ...files.map(() => false)])
  check(orderOk, 'directories sort before files', dirs.slice(0, 8).join(','))
  check(JSON.stringify(dirs) === JSON.stringify([...dirs].sort((a, b) => a.localeCompare(b))), 'and names sort A→Z', dirs.join(','))

  // the escape fixtures are VISIBLE (so the drawer can grey them out) but flagged
  const esc = (r.body?.items ?? []).find((i) => i.name === 'link-out')
  check(!!esc?.escapes, 'a symlink out of the ceiling is listed with escapes:true rather than hidden', JSON.stringify(esc))

  // drill-in
  const sub = await fsq(P, 'list', BOX, `path=${encodeURIComponent('sub')}`)
  check(sub.status === 200 && has(sub.body, 'one.txt'), 'a relative path lists that directory', names(sub.body).join(','))
  const abs = await fsq(P, 'list', BOX, `path=${encodeURIComponent(path.join(BOX, 'sub'))}`)
  check(abs.status === 200 && has(abs.body, 'one.txt'), 'an absolute path inside the root lists it too')
}

async function caseNoFallback() {
  const noCwd = await get(P, '/oc/fs/list')
  check(noCwd.status >= 400 && noCwd.status < 500, `no cwd answers 4xx, never a listing of the default root (${noCwd.status})`)
  check((noCwd.body?.items ?? []).length === 0, 'and it lists NOTHING (no global-root fallback)', JSON.stringify(noCwd.body)?.slice(0, 90))

  const dead = await fsq(P, 'list', path.join(BOX, 'does-not-exist'))
  check(dead.status >= 400 && dead.status < 500, `a cwd that is not a directory answers 4xx (${dead.status})`)
  check(/does-not-exist|not a directory|does not exist|no such/i.test(JSON.stringify(dead.body ?? '')),
    'and the answer names the directory', JSON.stringify(dead.body)?.slice(0, 120))

  const fileDead = await fsq(P, 'file', path.join(BOX, 'does-not-exist'), 'path=keep.txt')
  check(fileDead.status >= 400, `the same rule for /fs/file (${fileDead.status})`)
  const findDead = await fsq(P, 'find', path.join(BOX, 'does-not-exist'), 'q=keep')
  check(findDead.status >= 400 && (findDead.body?.matches ?? []).length === 0, `and for /fs/find (${findDead.status})`)
  const statDead = await fsq(P, 'status', path.join(BOX, 'does-not-exist'))
  check(statDead.status >= 400 && (statDead.body?.dirty ?? []).length === 0, `and for /fs/status (${statDead.status})`)
}

async function caseContainment() {
  const up = await fsq(P, 'list', BOX, `path=../../${path.basename(OUT)}`)
  check(up.status === 403, `a relative path above the ceiling is 403 (pre-fix: ${up.status})`, JSON.stringify(up.body)?.slice(0, 150))
  check(new RegExp(CEIL.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).test(JSON.stringify(up.body ?? '')),
    'and the 403 names the ceiling', JSON.stringify(up.body)?.slice(0, 150))

  const abs = await fsq(P, 'list', BOX, `path=${encodeURIComponent(OUT)}`)
  check(abs.status === 403, `an absolute path above the ceiling is 403 (${abs.status})`)

  const upCwd = await fsq(P, 'list', path.dirname(CEIL))
  check(upCwd.status === 403, `a cwd ABOVE the ceiling is 403 (${upCwd.status})`)
  const etc = await fsq(P, 'file', '/etc', 'path=passwd')
  check(etc.status === 403, '/etc/passwd is 403, not a file body', JSON.stringify(etc.body)?.slice(0, 90))
  check(!JSON.stringify(etc.body ?? '').includes('root:'), 'and no content leaked')

  const trav = await fsq(P, 'file', BOX, `path=sub/../../../${path.basename(OUT)}/secret.txt`)
  check(trav.status === 403, `'sub/../../..'/outside never reads the outside file (${trav.status})`)
  check(!JSON.stringify(trav.body ?? '').includes('ABOVE THE CEILING'), 'no content leaked through traversal')

  // symlink escapes: tau's rule — realpath both sides, reject the escape
  const sl = await fsq(P, 'file', BOX, 'path=link-out')
  check(sl.status === 403, `a symlink pointing outside is 403 (${sl.status})`, JSON.stringify(sl.body)?.slice(0, 150))
  check(!JSON.stringify(sl.body ?? '').includes('ABOVE THE CEILING'), 'and following it leaked nothing')
  const sld = await fsq(P, 'list', BOX, 'path=link-dir')
  check(sld.status === 403, `a symlinked DIRECTORY outside is 403 (${sld.status})`)
  check((sld.body?.items ?? []).length === 0, 'and it listed nothing of the escaped tree')
  const fnd = await fsq(P, 'find', BOX, 'q=secretsub')
  check(fnd.status === 200 && (fnd.body?.matches ?? []).length === 0, `find never walks a symlink out (${fnd.status})`, JSON.stringify(fnd.body?.matches))

  // the same rule must not break an in-ceiling symlink
  const safe = await fsq(P, 'file', BOX, 'path=sub/link-in')
  check(safe.status === 200 && safe.body?.content === 'l1\nl2\nl3\n', 'a symlink INSIDE the ceiling still reads', JSON.stringify(safe.body)?.slice(0, 70))
}

async function caseFile() {
  const t = await fsq(P, 'file', BOX, 'path=keep.txt')
  check(t.status === 200 && t.body?.type === 'text' && t.body?.content === 'l1\nl2\nl3\n', 'a text file returns {type:"text", content}', JSON.stringify(t.body)?.slice(0, 80))

  const nul = await fsq(P, 'file', BOX, 'path=nul.bin')
  check(nul.status === 200 && nul.body?.type === 'binary' && nul.body?.content === undefined,
    'a NUL byte means type:"binary" and no text body', JSON.stringify(nul.body)?.slice(0, 90))

  const big = await fsq(P, 'file', BOX, 'path=big.txt')
  check(big.body?.type === 'too_large' && big.body?.truncated === true && big.body?.size > 1024 * 1024 && !big.body?.content,
    'over 1 MiB is type:"too_large" + truncated + size, never a 1 MiB string', JSON.stringify(big.body)?.slice(0, 120))

  const img = await fsq(P, 'file', BOX, 'path=shot.png')
  check(img.body?.type === 'image' && typeof img.body?.base64 === 'string' && /png/.test(String(img.body?.mime)),
    'an image returns base64 + mime', JSON.stringify(img.body)?.slice(0, 90))

  const miss = await fsq(P, 'file', BOX, 'path=nope.txt')
  check(miss.status === 404, `a missing file is 404 (${miss.status})`)
  const dirAsFile = await fsq(P, 'file', BOX, 'path=sub')
  check(dirAsFile.status >= 400, `a directory via /fs/file is 4xx (${dirAsFile.status})`)
  const noPath = await fsq(P, 'file', BOX)
  check(noPath.status >= 400, `no path on /fs/file is 4xx, not a listing of the root (${noPath.status})`)
}

async function caseFind() {
  const a = await fsq(P, 'find', BOX, 'q=ALPH')
  check((a.body?.matches ?? []).some((m) => m.name === 'alpha.txt'), 'find is case-insensitive on NAMES', JSON.stringify(a.body?.matches)?.slice(0, 90))

  const c = await fsq(P, 'find', BOX, 'q=zzzneedle')
  check(c.status === 200 && (c.body?.matches ?? []).length === 0,
    'and matches NO file whose name lacks the term, however the CONTENTS read', JSON.stringify(c.body?.matches)?.slice(0, 90))

  const d = await fsq(P, 'find', BOX, 'q=deepd0')
  check((d.body?.matches ?? []).some((m) => m.isDirectory), 'directories are findable too')

  const ign = await fsq(P, 'find', BOX, 'q=junk')
  check((ign.body?.matches ?? []).length === 0, 'the ignore set applies to find as well', JSON.stringify(ign.body?.matches)?.slice(0, 90))

  const esc = await fsq(P, 'find', BOX, 'q=keep')
  check(esc.status === 200 && (esc.body?.matches ?? []).length > 0, 'find works at all', `${(esc.body?.matches ?? []).length} matches`)

  const noQ = await fsq(P, 'find', BOX)
  check(noQ.status >= 400 && (noQ.body?.matches ?? []).length === 0, `find with no q is 4xx, not a full-tree walk (${noQ.status})`)
}

async function caseCaps() {
  const many = await fsq(P, 'list', BOX, 'path=many')
  check((many.body?.items ?? []).length === 500, `a 600-file directory lists exactly 500 (${(many.body?.items ?? []).length})`)
  check(many.body?.truncated === true, 'and says truncated:true rather than looking empty')

  const f = await fsq(P, 'find', BOX, 'q=match')
  check((f.body?.matches ?? []).length === 200, `find caps at 200 matches (${(f.body?.matches ?? []).length})`)
  check(f.body?.truncated === true, 'and says so')

  const d = await fsq(P, 'find', BOX, 'q=needle')
  const got = (d.body?.matches ?? []).map((m) => m.name)
  check(got.includes('shallowneedle.txt'), 'depth 11 is findable', got.join(','))
  check(!got.includes('deepneedle.txt'), 'depth 15 is NOT walked', got.join(','))

  const cap2 = await fsq(P, 'status', REPO2)
  check(cap2.status === 200 && cap2.body?.repo === true && typeof cap2.body?.error === 'string' && cap2.body?.truncated === true,
    'a repo dirtier than the status cap refuses to answer rather than dumping it', JSON.stringify(cap2.body)?.slice(0, 170))
}

async function caseGit() {
  const nr = await fsq(P, 'status', BOX)
  check(nr.status === 200 && nr.body?.repo === false && Array.isArray(nr.body?.dirty) && nr.body.dirty.length === 0 && !nr.body?.error,
    'a directory that is not in a repo answers {repo:false} with NO error tile', JSON.stringify(nr.body)?.slice(0, 120))

  const s = await fsq(P, 'status', REPO)
  check(s.status === 200 && s.body?.repo === true, `a repo answers repo:true (${s.status})`)
  check(typeof s.body?.head === 'string' && s.body.head.length >= 7, 'head is a commit sha', JSON.stringify(s.body?.head))
  check(s.body?.branch === 'main', 'branch is the branch', JSON.stringify(s.body?.branch))
  const byFile = {}
  for (const d of s.body?.dirty ?? []) byFile[d.path] = d.status
  check(byFile['a.txt'] === 'modified', 'a staged+modified file reports modified (one badge word for `M ` and ` M`)', JSON.stringify(byFile))
  check(byFile['c.txt'] === 'added', 'a newly staged file reports added')
  check(byFile['sub/b.txt'] === 'deleted', 'a deleted file reports deleted')
  check(byFile['untracked.txt'] === 'untracked', 'an untracked file reports untracked')
  check(byFile['r_new.txt'] === 'renamed', 'a rename reports renamed on the NEW path')
  check((s.body?.dirty ?? []).every((d) => typeof d.abs === 'string' && d.abs.startsWith(REPO)),
    'every dirty entry carries an ABS path, which is the key the tree badges on')
  check((s.body?.dirty ?? []).length <= 6, 'and stays under the configured cap', `${(s.body?.dirty ?? []).length} entries`)
  check(JSON.stringify(s.body ?? '').includes('untracked.txt'),
    'untracked files are IN the dirty set (-uall), which is the whole point of the badge')

  // read-only git: the check asserts the tree survives the request
  check(fs.existsSync(path.join(REPO, 'untracked.txt')), 'an untracked file survives /fs/status')
  const st = git(REPO, 'status', '--porcelain=v1', '--untracked-files=all')
  check(st.includes('untracked.txt') && st.includes('a.txt'), 'the worktree still has the same dirt after the call', st.replace(/\n/g, '|').slice(0, 120))
  check(git(REPO, 'stash', 'list').trim() === '', 'and NO stash entry was created')

  const esc = await fsq(P, 'status', OUT)
  check(esc.status === 403, `status above the ceiling is 403 (${esc.status})`)

  const dd = await fsq(P, 'diff', REPO, 'path=a.txt')
  const patch = dd.body?.patch ?? ''
  check(dd.status === 200 && patch.includes('-l2') && patch.includes('+CHANGED'),
    'git diff for one path returns real unified hunks (against HEAD, so a staged edit still shows)', JSON.stringify(dd.body)?.slice(0, 130))

  const un = await fsq(P, 'diff', REPO, 'path=untracked.txt')
  check(un.body?.patch?.includes('+untracked'), 'an untracked file diffs as all-adds', JSON.stringify(un.body)?.slice(0, 130))

  const del = await fsq(P, 'diff', REPO, 'path=sub/b.txt')
  check(del.body?.patch?.includes('-b-line'), 'a deleted file diffs against HEAD (all-removes)', JSON.stringify(del.body)?.slice(0, 130))

  const ren = await fsq(P, 'diff', REPO, 'path=r_new.txt')
  check(ren.status === 200, 'a renamed file answers 200', JSON.stringify(ren.body)?.slice(0, 110))

  const notRepo = await fsq(P, 'diff', BOX, 'path=keep.txt')
  check(notRepo.status >= 400 && notRepo.status < 500, `a diff with no repo is 4xx (${notRepo.status})`)

  const escD = await fsq(P, 'diff', REPO, `path=../../${path.basename(OUT)}/secret.txt`)
  check(escD.status === 403, 'and the diff route obeys the ceiling too', JSON.stringify(escD.body)?.slice(0, 100))
}

// A repo whose top sits ABOVE the ceiling: `--cwd` one level inside a repo, the
// ordinary shape of a session started in a subdir. Knowing the file is dirty is
// the point; naming files this server may not read is the leak.
async function caseRepoAboveCeiling() {
  const P2 = PORT + 1
  const SUB = path.join(REPO, 'sub')
  check(await boot(P2, SUB), `a second bridge rooted at ${SUB} is up`)

  const s = await fsq(P2, 'status', SUB)
  check(s.status === 200 && s.body?.repo === true, `status still reports the repo (${s.status})`, JSON.stringify(s.body)?.slice(0, 120))
  check(s.body?.toplevel === REPO, 'and names the repo top', JSON.stringify(s.body?.toplevel))
  const paths = (s.body?.dirty ?? []).map((d) => d.path)
  check(paths.includes('sub/b.txt'), 'the file inside the ceiling IS badged', paths.join(','))
  check(!paths.includes('a.txt') && !paths.includes('untracked.txt'),
    'files above the ceiling are NOT named (that is the leak this guards)', paths.join(','))
  check(s.body?.scoped === true && s.body?.omitted >= 4, 'and the answer admits it is partial', JSON.stringify({ scoped: s.body?.scoped, omitted: s.body?.omitted }))
  check(!(s.body?.dirty ?? []).some((d) => !d.abs.startsWith(SUB)), 'no dirty entry resolves above the ceiling')

  const d = await fsq(P2, 'diff', SUB, 'path=b.txt')
  check(d.status === 200 && (d.body?.patch ?? '').includes('-b-line'),
    'diff still works from a subdir of the repo (paths resolve against the top)', JSON.stringify(d.body)?.slice(0, 140))

  const esc = await fsq(P2, 'file', SUB, 'path=../a.txt')
  check(esc.status === 403, `and the ceiling still holds for reads from that root (${esc.status})`)
}

async function main() {
  fixtures()
  check(await boot(P, CEIL), 'bridge is up')

  for (const c of [caseRoutes, caseNoFallback, caseContainment, caseFile, caseFind, caseCaps, caseGit]) {
    console.log(`\n===== ${c.name} =====`)
    try {
      await c()
    } catch (e) {
      console.log('ERROR', e?.stack ?? String(e))
      failed++
    }
  }
  // second bridge, different --cwd, so the ceiling itself moves
  console.log('\n===== caseRepoAboveCeiling =====')
  try {
    await caseRepoAboveCeiling()
  } catch (e) {
    console.log('ERROR', e?.stack ?? String(e))
    failed++
  }
  cleanup()
  console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
  process.exit(failed ? 1 : 0)
}

main().catch((e) => {
  console.error('fsbrowse-check crashed:', e)
  cleanup()
  process.exit(1)
})
