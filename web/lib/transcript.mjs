// pi-web/lib/transcript.mjs — the JSONL transcript: reading, the root->leaf
// chain, the file listing (with its 1.5 s cache) and per-file lookups.
import fs from 'node:fs'
import path from 'node:path'
import { LIVE_ROLES, msgId } from './ids.mjs'
import { SESSIONS_DIR, TRASHED_DIRS, flattenTree, pathFromRoot } from './tau.mjs'

/**
 * root -> leaf chain of an entry list, by parentId (tau does the same after a
 * navigation: `session.entries = pathFromRoot(byId, newLeafId)`).
 *
 * A pi transcript is an append-only TREE. Without navigation it is linear and
 * this is a no-op; after a revert the abandoned branch is still in the file, and
 * pi's own view is the chain ending at the LAST entry (the `tau:navigate-tree`
 * marker sits at the new leaf). Seeding a resumed child with the raw file instead
 * re-shows messages the user reverted away — the transcript would disagree with
 * what the model actually sees.
 */
function leafChain(entries) {
  const list = entries ?? []
  const byId = new Map()
  for (const e of list) if (e?.id) byId.set(e.id, e)
  let leaf = null
  for (let i = list.length - 1; i >= 0; i--) {
    if (list[i]?.id && list[i].type !== 'session') {
      leaf = list[i]
      break
    }
  }
  if (!leaf) return list
  const path = []
  const seen = new Set()
  let cur = leaf
  while (cur && !seen.has(cur.id)) {
    seen.add(cur.id)
    path.push(cur)
    cur = cur.parentId ? byId.get(cur.parentId) : null
  }
  path.reverse()
  return path
}
/** every entry id a transcript already uses (missing ids excluded — see below) */
function entryIdsIn(file) {
  const out = new Set()
  for (const e of readJsonl(file)) {
    if (e?.type === 'session') {
      if (e.id) out.add(e.id)
      continue
    }
    if (e?.id) out.add(e.id)
  }
  return out
}
/**
 * webui message id -> entry id inside a FILE's chain (the fork reads the file,
 * not a child, so the same derived-id walk has to work without RPC).
 */
function entryIdInChain(chain, messageID) {
  const dupSeen = new Map()
  for (const entry of chain) {
    const m = entry?.type === 'message' ? entry.message : null
    if (!m || !LIVE_ROLES.has(m.role)) continue
    if (msgId(m.role, m.timestamp, dupSeen) === messageID) return entry.id ?? null
  }
  return chain.some((x) => x?.id === messageID) ? messageID : null
}

/**
 * webui message id -> pi entry id, resolved against the CHILD's own tree.
 *
 * Message ids are DERIVED (`msgId(role, timestamp, dup counter)`), so the only
 * faithful inverse is to walk the same rows with the same function in the same
 * order — and the rows must be pi's, root->leaf: tau's mirror entries carry no
 * `id`/`parentId` at all (sessions.js pushes `{type:'message', message}`), so
 * they cannot name a navigation target, and after a revert the mirror's shape is
 * whatever the last navigation left behind.
 */
async function messageEntryId(s, messageID) {
  const r = await s.send({ type: 'get_tree' }, { timeoutMs: 15000 })
  if (r?.success === false) return { error: String(r?.error ?? 'get_tree failed') }
  const d = r?.data ?? {}
  const byId = flattenTree(d.tree ?? [])
  const chain = pathFromRoot(byId, d.leafId ?? null)
  const dupSeen = new Map()
  for (const entry of chain) {
    const m = entry?.type === 'message' ? entry.message : null
    if (!m || !LIVE_ROLES.has(m.role)) continue
    if (msgId(m.role, m.timestamp, dupSeen) === messageID) return { entryId: entry.id ?? null }
  }
  // an API user may hand us pi's own entry id
  return chain.some((x) => x?.id === messageID) ? { entryId: messageID } : { error: null }
}
// ---------------------------------------------------------------------------
// cold (JSONL) history
// ---------------------------------------------------------------------------

function readJsonl(file) {
  const out = []
  let raw
  try {
    raw = fs.readFileSync(file, 'utf8')
  } catch {
    return out
  }
  for (const line of raw.split('\n')) {
    if (!line.trim()) continue
    try {
      out.push(JSON.parse(line))
    } catch {
      /* torn trailing line mid-write */
    }
  }
  return out
}
function listFiles() {
  const out = []
  let dirs
  try {
    dirs = fs.readdirSync(SESSIONS_DIR, { withFileTypes: true })
  } catch {
    return out
  }
  for (const d of dirs) {
    if (!d.isDirectory()) continue
    const p = path.join(SESSIONS_DIR, d.name)
    if (TRASHED_DIRS.has(path.resolve(p))) continue // soft-deleted, never listed
    let files
    try {
      files = fs.readdirSync(p)
    } catch {
      continue
    }
    for (const f of files) if (f.endsWith('.jsonl')) out.push(path.join(p, f))
  }
  return out
}
let filesCache = { at: 0, list: null }
function listFilesCached() {
  const now = Date.now()
  if (filesCache.list && now - filesCache.at < 1500) return filesCache.list
  filesCache = { at: now, list: listFiles() }
  return filesCache.list
}
function readHeaderCwd(file) {
  for (const e of readJsonl(file)) if (e.type === 'session') return e.cwd || null
  return null
}

/** Reset the file-list cache: a fork/rename/delete makes it stale. */
function invalidateFilesCache() {
  filesCache = { at: 0, list: null }
}

export { entryIdInChain, entryIdsIn, filesCache, invalidateFilesCache, leafChain, listFiles, listFilesCached, messageEntryId, readHeaderCwd, readJsonl }
