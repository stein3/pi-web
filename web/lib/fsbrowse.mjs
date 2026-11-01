// pi-web/lib/fsbrowse.mjs — GET /oc/fs/*: read-only, ceiling-bounded.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { PI_CWD } from './config.mjs'
import { expandHome } from './util.mjs'

// ---------------------------------------------------------------------------
// File browser — GET /oc/fs/{list,file,find,status,diff}
//
// The webui's Files drawer browses the WORKSPACE of the session whose tab is
// open, and asks git which files are dirty. It is read-only end to end: no
// write route, no delete route, no OS opener, and no mutating git command (this
// worktree routinely carries another agent's in-flight edits — `git add`/`stash`/
// `checkout` from a browser drawer would be a data-loss bug, not a style one).
//
// The one thing standing between these routes and an arbitrary-read endpoint is
// the CEILING: every request names a `cwd` (the drawer follows the tab, so a
// session in another workspace gets a different root) and nothing may resolve
// above the bridge's --cwd. Same policy as tau's file browser, which had this
// thought already: realpath both sides and reject the escape, so a symlink that
// points out of the tree is a 403 and not a free read.
//
// /oc/fs/* is deliberately NOT the existing /oc/file/content: that route takes
// an arbitrary absolute path with NO containment, because it exists to read the
// live before-image of a file the transcript already named. Routing the browser
// through it is how this becomes a hole. Hardening /file/content is separate
// work (it would change DiffPane behaviour).
//
// A 404 here is also the capability probe: a backend that has no /oc/fs/* is "a
// backend with no file browser", exactly like a backend that 404s /queue.
// ---------------------------------------------------------------------------

const FS_LIST_CAP = 500
const FS_FIND_CAP = 200
const FS_MAX_DEPTH = 12
const FS_FILE_BYTES = 1024 * 1024
const FS_DIFF_BYTES = 512 * 1024
// Overridable so a check can prove the refusal without seeding 20k files.
const FS_STATUS_CAP = Number(process.env.PI_WEB_FS_STATUS_CAP || 20000)
// tau's list (IGNORED_NAMES in pi-tau-web-server/bin/server-main.js) plus the
// few that only bite once a repo is under the root. `.env` is the ONE dotfile
// that stays visible — reading it is a common reason to open the drawer.
const FS_IGNORE = new Set([
  'node_modules', '.git', '__pycache__', '.DS_Store', '.Trash', '.next', '.nuxt',
  'dist', 'build', '.cache', '.turbo', 'venv', '.venv', 'env', '.env.local', '.pi',
  'coverage', '.nyc_output', '.parcel-cache', '.pytest_cache', 'target',
])
const FS_IMAGE = {
  png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', gif: 'image/gif',
  webp: 'image/webp', svg: 'image/svg+xml', ico: 'image/x-icon', avif: 'image/avif',
}

class FsError extends Error {
  constructor(status, message) {
    super(message)
    this.status = status
  }
}

let fsCeilingCache = null
/** The workspace nothing may escape. realpath'd once, so a symlinked root still compares. */
function fsCeiling() {
  if (!fsCeilingCache) {
    try {
      fsCeilingCache = fs.realpathSync(PI_CWD)
    } catch {
      fsCeilingCache = PI_CWD // a dead --cwd: every request 4xxes below, which is correct
    }
  }
  return fsCeilingCache
}
const within = (x, root) => x === root || x.startsWith(root + path.sep)
const fsHidden = (name) => FS_IGNORE.has(name) || (name.startsWith('.') && name !== '.env')

/** Validate the `cwd` param: required, real directory, inside the ceiling. */
function fsRoot(q) {
  const raw = String(q.get('cwd') ?? '').trim()
  if (!raw) throw new FsError(400, 'cwd is required — the drawer browses the session workspace, never a server-wide root')
  const ceil = fsCeiling()
  const abs = path.resolve(expandHome(raw))
  if (!within(abs, ceil)) throw new FsError(403, `${abs} is outside the workspace ceiling (${ceil})`)
  let real = abs
  try {
    real = fs.realpathSync(abs)
  } catch {
    /* missing — the stat below reports it */
  }
  if (!within(real, ceil)) throw new FsError(403, `${abs} resolves to ${real}, outside the workspace ceiling (${ceil})`)
  let st = null
  try {
    st = fs.statSync(real)
  } catch {
    /* missing */
  }
  if (!st?.isDirectory()) throw new FsError(400, `not a directory: ${raw}`)
  return real
}

/** Resolve one path under a root and prove it never leaves the ceiling. */
function fsPath(root, p) {
  const raw = String(p ?? '').trim()
  const ceil = fsCeiling()
  const abs = raw ? path.resolve(root, expandHome(raw)) : root
  if (!within(abs, ceil)) throw new FsError(403, `${abs} is outside the workspace ceiling (${ceil})`)
  let real = null
  try {
    real = fs.realpathSync(abs)
  } catch {
    real = null // does not exist yet: callers answer 404, or (for diff) ask git
  }
  // tau's rule: a symlink is only usable if where it POINTS is also allowed.
  if (real && !within(real, ceil)) {
    throw new FsError(403, `${abs} is a symlink to ${real}, outside the workspace ceiling (${ceil})`)
  }
  return { abs, real }
}

/** Read-only git. null on failure ("not a repo", "no such path in HEAD", …). */
function gitOut(cwd, args) {
  return new Promise((resolve) => {
    execFile(
      'git',
      ['-C', cwd, ...args],
      {
        encoding: 'utf8',
        timeout: 15000,
        maxBuffer: 8 * 1024 * 1024,
        // `git status` REFRESHES and rewrites .git/index by default. In a
        // worktree another agent is editing that is a write, so ask for the
        // no-optional-locks path (what `git --no-optional-locks status` does).
        env: { ...process.env, GIT_OPTIONAL_LOCKS: '0' },
      },
      (err, stdout) => resolve(err ? null : String(stdout ?? '')),
    )
  })
}

async function gitTop(cwd) {
  // Deliberately does NOT reject a repo whose top sits above the ceiling: that
  // is the ordinary shape of `--cwd` pointed one level inside a repo, and the
  // drawer still needs to know the file it is looking at is dirty. What must
  // never happen is reading or listing above the ceiling — fsRoot/fsPath/fsDiff
  // hold that line, and fsStatus cuts its dirty set at the ceiling.
  return (await gitOut(cwd, ['rev-parse', '--show-toplevel']))?.trim() || null
}

function fsList(q) {
  const root = fsRoot(q)
  const { abs, real } = fsPath(root, q.get('path'))
  const dir = real ?? abs
  let entries = null
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true })
  } catch (e) {
    const code = e?.code
    throw new FsError(code === 'ENOENT' ? 404 : code === 'EACCES' ? 403 : 400, `cannot list ${abs}: ${e?.code ?? e?.message}`)
  }
  const items = []
  for (const e of entries) {
    if (fsHidden(e.name)) continue
    const full = path.join(dir, e.name)
    let isDir = e.isDirectory()
    let size = null
    let mtime = 0
    let link
    let escapes
    try {
      const st = fs.lstatSync(full) // lstat: never stat THROUGH a symlink
      mtime = st.mtimeMs
      if (e.isSymbolicLink()) {
        link = true
        try {
          const t = fs.realpathSync(full)
          if (within(t, fsCeiling())) {
            isDir = fs.statSync(t).isDirectory()
          } else {
            escapes = true // listed so the drawer can grey it out; every read of it 403s
          }
        } catch {
          escapes = true // broken link
        }
      } else {
        size = isDir ? null : st.size
      }
    } catch {
      /* raced away */
    }
    items.push({
      name: e.name,
      path: full,
      isDirectory: isDir,
      size,
      mtime,
      ...(link ? { symlink: true } : {}),
      ...(escapes ? { escapes: true } : {}),
    })
  }
  // tau's order: directories first, then A→Z (localeCompare, so `.env` sits
  // where a human expects rather than at the top of an ASCII sort)
  items.sort((a, b) => a.isDirectory !== b.isDirectory ? (a.isDirectory ? -1 : 1) : a.name.localeCompare(b.name))
  const truncated = items.length > FS_LIST_CAP
  return { root, path: dir, items: items.slice(0, FS_LIST_CAP), truncated, cap: FS_LIST_CAP }
}

function fsFile(q) {
  const root = fsRoot(q)
  const { abs, real } = fsPath(root, q.get('path'))
  if (!real) throw new FsError(404, `no such file: ${abs}`)
  let st = null
  try {
    st = fs.statSync(real)
  } catch {
    throw new FsError(404, `no such file: ${abs}`)
  }
  if (st.isDirectory()) throw new FsError(400, `not a file: ${abs} (use /oc/fs/list)`)
  if (st.size > FS_FILE_BYTES) return { path: abs, size: st.size, type: 'too_large', truncated: true }
  const buf = fs.readFileSync(real)
  const mime = FS_IMAGE[path.extname(abs).toLowerCase().slice(1)]
  if (mime) return { path: abs, size: st.size, type: 'image', mime, base64: buf.toString('base64') }
  // NUL => binary. No body at all: base64 of a 1 MiB blob on every click is not
  // a preview, and the drawer has nothing to draw with it.
  if (buf.includes(0)) return { path: abs, size: st.size, type: 'binary' }
  return { path: abs, size: st.size, type: 'text', content: buf.toString('utf8') }
}

function fsFind(q) {
  const root = fsRoot(q)
  const needle = String(q.get('q') ?? '').trim().toLowerCase()
  if (!needle) throw new FsError(400, 'q is required')
  const matches = []
  let truncated = false
  const queue = [[root, 0]]
  while (queue.length && !truncated) {
    const [dir, depth] = queue.shift()
    let entries = null
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true })
    } catch {
      continue // raced away or unreadable: skip the subtree, do not fail the search
    }
    for (const e of entries) {
      if (fsHidden(e.name)) continue
      const full = path.join(dir, e.name)
      const depth1 = depth + 1
      if (e.isSymbolicLink()) {
        // NAMES are searchable through a link, but a link is never WALKED: that
        // is the one way a search could leave the ceiling.
        try {
          const t = fs.realpathSync(full)
          if (!within(t, fsCeiling())) continue
        } catch {
          continue
        }
        if (depth1 <= FS_MAX_DEPTH && e.name.toLowerCase().includes(needle)) {
          matches.push({ name: e.name, path: full, isDirectory: false, symlink: true })
        }
        continue
      }
      if (depth1 <= FS_MAX_DEPTH && e.name.toLowerCase().includes(needle)) {
        matches.push({ name: e.name, path: full, isDirectory: e.isDirectory() })
      }
      if (e.isDirectory() && depth1 < FS_MAX_DEPTH) queue.push([full, depth1])
      if (matches.length >= FS_FIND_CAP) {
        truncated = true
        break
      }
    }
  }
  return { root, matches, truncated, cap: FS_FIND_CAP, depth: FS_MAX_DEPTH }
}

// porcelain v1 with -z: NUL-separated `XY path` records, and a rename/copy
// inserts the SOURCE as the next field. Word-ising XY is what lets the tree draw
// one badge per kind instead of re-deriving git's grammar in the browser.
// `M ` (staged) and ` M` (worktree) deliberately collapse to ONE word: the
// drawer badges "this file is dirty", and whether the change happens to be
// staged is somebody else's business in a shared worktree.
function porcelainStatus(xy) {
  const x = xy[0]
  const y = xy[1]
  if (xy === '??') return 'untracked'
  if (x === 'U' || y === 'U') return 'conflicted'
  if (x === 'R' || y === 'R') return 'renamed'
  if (x === 'C' || y === 'C') return 'copied'
  if (x === 'D' || y === 'D') return 'deleted'
  if (x === 'A') return 'added'
  if (x === 'T' || y === 'T') return 'typechange'
  return 'modified'
}

function parsePorcelainZ(out, top) {
  const fields = String(out).split('\0')
  const dirty = []
  for (let i = 0; i < fields.length; i++) {
    const rec = fields[i]
    if (!rec || rec.length < 4) continue
    const xy = rec.slice(0, 2)
    const rel = rec.slice(3)
    if (!rel) continue
    if (xy[0] === 'R' || xy[0] === 'C' || xy[1] === 'R' || xy[1] === 'C') i++ // skip the source field
    const relClean = rel.replace(/^\.\//, '')
    dirty.push({ path: relClean, abs: path.join(top, relClean), status: porcelainStatus(xy), xy })
  }
  return dirty
}

async function fsStatus(q) {
  const root = fsRoot(q)
  const top = await gitTop(root)
  if (!top) return { root, repo: false, dirty: [] } // not a repo: a plain tree, no error tile
  const raw = await gitOut(top, ['status', '--porcelain=v1', '-z', '--untracked-files=all'])
  const head = (await gitOut(top, ['rev-parse', '--short', 'HEAD']))?.trim() || null
  const branch = (await gitOut(top, ['rev-parse', '--abbrev-ref', 'HEAD']))?.trim() || null
  const base = { root, repo: true, toplevel: top, head, branch }
  if (raw === null) return { ...base, dirty: [], error: 'git status failed' }
  const all = parsePorcelainZ(raw, top)
  if (all.length > FS_STATUS_CAP) {
    // Refuse rather than ship 200k rows the drawer cannot draw: a truncated
    // dirty set would silently un-badge files, which is worse than saying no.
    return { ...base, dirty: [], truncated: true, cap: FS_STATUS_CAP, error: `${all.length} changed files exceed the ${FS_STATUS_CAP}-entry cap` }
  }
  // A repo may reach ABOVE the ceiling (the bridge started with --cwd one level
  // inside a repo). Then the dirty set must be cut at the ceiling: those files
  // are unreadable here anyway, and naming them would leak what the containment
  // rule exists to hide. `scoped` says the list is deliberately partial.
  const ceil = fsCeiling()
  const dirty = all.filter((d) => within(d.abs, ceil))
  const scoped = dirty.length !== all.length
  return { ...base, dirty, scoped, ...(scoped ? { omitted: all.length - dirty.length } : {}), truncated: false }
}

async function fsDiff(q) {
  const root = fsRoot(q)
  const { abs, real } = fsPath(root, q.get('path'))
  const top = await gitTop(root)
  if (!top) throw new FsError(400, `not a git repository: ${root}`)
  const rel = path.relative(top, abs)
  if (!rel || rel.startsWith('..') || path.isAbsolute(rel)) {
    throw new FsError(403, `${abs} is outside the git repository at ${top}`)
  }
  // HEAD, not the worktree-vs-index diff: a session's edit may already be
  // staged (by its own agent or another one's), and `git diff` alone would then
  // answer an empty patch for a file the drawer just badged as dirty.
  const body = await gitOut(top, ['diff', '--no-color', '-U3', 'HEAD', '--', rel])
  if (body === null) throw new FsError(400, `git diff failed for ${rel}`)
  let patch = body
  let untracked = false
  if (!patch.trim()) {
    const inHead = (await gitOut(top, ['cat-file', '-e', `HEAD:${rel}`])) !== null
    if (!inHead && real && fs.existsSync(real) && fs.statSync(real).isFile()) {
      // No HEAD side at all: synthesize all-adds, the same shape the bridge
      // builds for pi's `write` tool.
      untracked = true
      const lines = fs.readFileSync(real, 'utf8').replace(/\n$/, '').split('\n')
      patch = `--- /dev/null\n+++ b/${rel}\n@@ -0,0 +1,${lines.length} @@\n` + lines.map((l) => '+' + l).join('\n')
    }
  }
  const truncated = patch.length > FS_DIFF_BYTES
  return { root, toplevel: top, path: abs, rel, patch: truncated ? patch.slice(0, FS_DIFF_BYTES) : patch, truncated, untracked }
}

const FS_ROUTES = { list: fsList, file: fsFile, find: fsFind, status: fsStatus, diff: fsDiff }

export { FS_ROUTES, FsError }
