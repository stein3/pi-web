// pi-web/lib/projection.mjs — opencode session projection + the cold (JSONL)
// history: sidebar metadata, sidecar star/tag, history messages, search.
import fs from 'node:fs'
import path from 'node:path'
import { PI_AGENT_DIR, PI_CWD } from './config.mjs'
import { isoMs, textOf } from './content.mjs'
import { canonicalId, freshEntryId, messagesOf } from './ids.mjs'
import { live, liveTauIdFor, reapedFile } from './registry.mjs'
import { SESSIONS_DIR, TRASH_DIR } from './tau.mjs'
import { entryIdsIn, leafChain, listFilesCached, readJsonl } from './transcript.mjs'
import { toOcMessages } from './translate.mjs'
import { liveCwd } from './util.mjs'

// ---------------------------------------------------------------------------
// opencode session projection
// ---------------------------------------------------------------------------

function ocSession(s, publicId, withFile = true) {
  if (!s) return null
  const e = live.get(s.id)
  const title = e?.title || s.sessionName || null
  const id = publicId ?? canonicalId(s)
  const active = Date.parse(s.lastActiveAt) || 0
  // tau's `lastActiveAt` is NOT "the session changed": it moves when a child attaches
  // (opening a tab resumes one) and whenever that child emits any event, so an idle
  // tab pins its row at browse time forever — measured live: two attached-but-idle
  // children reported 13:05:03 while their JSONLs last changed at 12:24 and 05:17.
  // A session row means "when did this transcript last change", so the file's own
  // clock wins; tau's clock is only the fallback for a child with no known file (never
  // prompted yet), and a busy session's file lags by at most one message.
  let meta = null
  if (withFile) {
    // No `fileForId(id)` fallback here: it walks (and re-primes) the 1500 ms
    // listFilesCached() TTL, and doing that from a projection makes the cold-resume
    // path see a file list that predates a freshly seeded transcript — compact-check
    // then 404s `POST /summarize` on a cold session. `sessionFile` is set for every
    // child tau knows about, and `reapedFile` covers the closed ones.
    const file = s.sessionFile || reapedFile.get(id) || null
    if (file) {
      try {
        meta = sessionMeta(file)
      } catch {
        /* unreadable: fall back to tau's clock */
      }
    }
  }
  const updated = meta?.updated || active || Date.now()
  return {
    // canonical (uuid) unless the caller asked for a specific alias
    id,
    title: title || undefined,
    cwd: s.cwd || undefined,
    time: { created: Date.parse(s.createdAt) || Date.now(), updated },
    // cumulative generated tokens (output+reasoning over every assistant message), same
    // definition as the history projection, so a live tab's overlay cannot stay frozen at
    // an older partial sum. Omitted for an all-zero tally, exactly like the history rows.
    ...(meta?.tokens_generated ? { tokens_generated: meta.tokens_generated } : {}),
    cost: 0,
    agent: undefined,
    model: s.model ? { providerID: s.model.provider || '', id: s.model.id || '' } : undefined,
    revert: live.get(s.id)?.revert ?? null,
  }
}
function deriveTitle(entries) {
  // last name wins, same reason as readSessionMeta: renames append rather than edit
  let named = ''
  for (const e of entries) {
    if (e.type === 'session_info' && e.name) named = e.name
  }
  if (named) return named
  for (const e of entries) {
    if (e.type === 'message' && e.message?.role === 'user') {
      const t = textOf(e.message.content).trim()
      if (t.length > 8) return t.split('\n')[0].slice(0, 60)
    }
  }
  return ''
}
/**
 * Move a session JSONL into the out-of-tree trash. null on success, an error
 * string otherwise. Rename (not unlink) keeps deletes recoverable; pi appends
 * per write, so the owning child must already be closed — see the DELETE route.
 */
/**
 * Append the session_info entry pi would have written for a rename, and drop
 * that file's cached projection so the sidebar re-reads it. For the cases with
 * no live child to ask (or one that would not answer). The path must stay put:
 * pi holds it open via --session.
 */
function appendSessionInfo(file, name) {
  if (!file || !name) return false
  // NEVER create a transcript here: pi writes its own header with `wx` at the
  // first conversation entry, so a file we made first would make pi throw.
  if (!fs.existsSync(file)) return false
  try {
    const ids = entryIdsIn(file)
    // An id-less entry is not "harmless": pi indexes it under `undefined`, and a
    // second one (a later rename, or a fork that copies both) resolves through the
    // same key — see freshEntryId.
    const parentId = leafChain(readJsonl(file)).slice(-1)[0]?.id ?? null
    fs.appendFileSync(
      file,
      JSON.stringify({
        type: 'session_info',
        id: freshEntryId(ids),
        ...(parentId ? { parentId } : {}),
        name,
        timestamp: new Date().toISOString(),
      }) + '\n',
    )
    metaCache.delete(file)
    return true
  } catch {
    return false // read-only tree
  }
}

function trashSessionFile(file) {
  // A session created and then deleted never had a conversation, so pi never
  // wrote the path it told us about: nothing to move, child already closed.
  if (!fs.existsSync(file)) return null
  try {
    fs.mkdirSync(TRASH_DIR, { recursive: true })
    const ext = path.extname(file)
    const base = path.basename(file, ext)
    let dest = path.join(TRASH_DIR, path.basename(file))
    for (let n = 1; fs.existsSync(dest); n++) dest = path.join(TRASH_DIR, `${base}_${n}${ext}`)
    fs.renameSync(file, dest)
    // drop the workspace dir once empty (pi recreates it on the next session)
    const dir = path.dirname(file)
    if (path.resolve(dir) !== path.resolve(SESSIONS_DIR) && fs.readdirSync(dir).length === 0) {
      fs.rmdirSync(dir)
    }
    return null
  } catch (err) {
    return String(err?.message ?? err)
  }
}
/**
 * Sidebar/history metadata for one session file.
 *
 * Cached per (mtime, size): the webui polls /api/history/sessions every 60 s and
 * `fileForId` uses it too, and each uncached pass re-parsed EVERY session JSONL
 * (61 ms for 27 files / 12 MB on this box, on the single-threaded bridge that is
 * also streaming everyone's SSE — and it grows linearly with history).
 * `readSessionMeta` is the file-derived part; liveness/alias identity is applied
 * per call because it depends on the live children, not the file.
 */
const metaCache = new Map() // file -> { key, meta }

function readSessionMeta(file, st) {
  const entries = readJsonl(file)
  const header = entries.find((e) => e.type === 'session')
  if (!header?.id) return null
  const msgs = messagesOf(entries)
  const users = msgs.filter((m) => m.message.role === 'user').length
  if (users === 0 && entries.length <= 8) return null
  // The CURRENT name, not the first one: a rename APPENDS a session_info entry (pi
  // does the same, and PATCH title appends here), so `find` returned the title the
  // session had when it was created and a rename never reached the sidebar. Measured
  // on a real transcript with three appended names: history said "llama server backup
  // plan" while the session had been renamed to "pi-web ssh".
  let info = null
  for (const e of entries) if (e.type === 'session_info' && e.name) info = e
  const modelChange = [...entries].reverse().find((e) => e.type === 'model_change')
  let tokens
  for (let i = entries.length - 1; i >= 0; i--) {
    const e = entries[i]
    if (e.type === 'usage' && e.usage) {
      tokens = (e.usage.input ?? 0) + (e.usage.output ?? 0) + (e.usage.cacheRead ?? 0)
      break
    }
    if (e.type === 'message' && e.message.role === 'assistant' && e.message.usage) {
      const u = e.message.usage
      tokens = (u.input ?? 0) + (u.output ?? 0) + (u.cacheRead ?? 0)
      break
    }
  }
  // Generated tokens — what the model produced: output + reasoning summed over
  // EVERY assistant message (the `tokens` above is a one-record context
  // estimate, which is what the sidebar used to show). pi stores usage on the
  // assistant message itself — 85/85 assistant messages in a real transcript,
  // and no separate `usage` entries — so summing message.usage cannot double
  // count. Same definition as chatserver's message-level sum for opencode, and
  // the same SELF-ONLY scope: pi-web sessions have no parent link, so the
  // webui's descendant rollup is a no-op here.
  let generated = 0
  for (const m of msgs) {
    const u = m.message?.role === 'assistant' ? m.message.usage : null
    if (!u) continue
    generated += (u.output ?? u.completionTokens ?? 0) + (u.reasoning ?? 0)
  }
  return {
    file,
    cwd: liveCwd(header.cwd) || PI_CWD,
    title: info?.name || deriveTitle(entries) || header.id.slice(0, 12),
    created: Date.parse(header.timestamp) || st.mtimeMs,
    updated: st.mtimeMs,
    message_count: msgs.length,
    cost: 0,
    model: modelChange?.modelId ?? '',
    model_provider: modelChange?.provider ?? '',
    ...(tokens ? { tokens } : {}),
    ...(generated ? { tokens_generated: generated } : {}),
    // an explicit name (any session_info entry) vs the prompt-derived fallback:
    // the bridge must never rename a session pi or the operator already named
    named: !!info,
    uuid: header.id,
  }
}

function sessionMeta(file) {
  let st
  try {
    st = fs.statSync(file)
  } catch {
    return null
  }
  const key = `${st.mtimeMs}|${st.size}`
  let hit = metaCache.get(file)
  if (!hit || hit.key !== key) {
    hit = { key, meta: readSessionMeta(file, st) }
    metaCache.set(file, hit)
  }
  const base = hit.meta
  if (!base) return null
  // Always pi's file uuid — the same string `ocSession()` publishes for the live
  // child, so the sidebar row and the tab can never diverge into two ids (and
  // therefore two tabs) for one session.
  return { ...base, id: base.uuid, live: !!liveTauIdFor(file) }
}

function allSessionMeta() {
  const files = listFilesCached()
  const rows = []
  for (const f of files) {
    try {
      const m = sessionMeta(f)
      if (m) rows.push(m)
    } catch {
      /* unreadable file */
    }
  }
  rows.sort((a, b) => b.updated - a.updated)
  // drop cache entries for files that vanished (deleted/trashed) so the map
  // can't grow across a long-lived server
  if (metaCache.size > files.length) {
    const keep = new Set(files)
    for (const f of metaCache.keys()) if (!keep.has(f)) metaCache.delete(f)
  }
  return rows
}

/**
 * Project chips for the new-session picker: every workspace a session has
 * actually run in (most recent first), then the child dirs of PI_CWD that
 * have never been used — tau's `--projects-dir` chips, minus the config.
 */
function workspaceChips() {
  const seen = new Map() // path -> last activity (0 = never used)
  for (const m of allSessionMeta()) {
    if (m.cwd && !seen.has(m.cwd)) seen.set(m.cwd, m.updated)
  }
  let entries = []
  try {
    entries = fs.readdirSync(PI_CWD, { withFileTypes: true })
  } catch {
    /* root unreadable — the session workspaces above still list */
  }
  for (const e of entries) {
    if (!e.isDirectory() || e.name.startsWith('.')) continue
    const p = path.join(PI_CWD, e.name)
    if (!seen.has(p)) seen.set(p, 0)
  }
  return [...seen]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])) // activity, then A→Z
    .slice(0, 24)
    .map(([p]) => ({ name: path.basename(p) || p, path: p }))
}

// --- star / tag sidecar (same idea as nopencode's webui.db smeta) -----------
// Lives in the agent dir, NOT next to pi-web.mjs: in the container the script
// sits at /app (ephemeral layer — every rebuild/recreate wipes it, taking every
// star and tag with it), while the agent dir is the bind mount that survives.
const META_FILE = path.join(PI_AGENT_DIR, '.meta.json')
function loadMeta() {
  try {
    return JSON.parse(fs.readFileSync(META_FILE, 'utf8'))
  } catch {
    return {}
  }
}
function saveMeta(m) {
  try {
    fs.writeFileSync(META_FILE, JSON.stringify(m, null, 2))
  } catch {
    /* read-only tree */
  }
}
const errTiles = new Map() // sid -> [{message,t}]

function toHistMsgs(entries) {
  return toOcMessages(entries).map((m) => ({
    id: m.id,
    role: m.role,
    time: m.time.created,
    modelID: m.modelID,
    providerID: m.providerID,
    error: m.error,
    parts: m.parts.map((p) => ({
      id: p.id,
      type: p.type,
      text: p.text,
      tool: p.tool,
      state_summary: p.state ? String(p.state.status ?? p.state.title ?? '') : undefined,
    })),
  }))
}
function searchSessions(q) {
  const terms = q.toLowerCase().split(/\s+/).filter((t) => t.length >= 2)
  if (!terms.length) return []
  const hits = []
  const meta = allSessionMeta()
  const byFile = new Map(meta.map((m) => [m.file, m]))
  for (const file of listFilesCached()) {
    if (hits.length >= 300) break
    const m = byFile.get(file)
    if (!m) continue
    for (const e of messagesOf(readJsonl(file))) {
      const text = textOf(e.message.content)
      const low = text.toLowerCase()
      if (!text || !terms.every((t) => low.includes(t))) continue
      const idx = low.indexOf(terms[0])
      const a = Math.max(0, idx - 70)
      const b = Math.min(text.length, idx + terms[0].length + 70)
      hits.push({
        session_id: m.id,
        session_title: m.title,
        message_id: 'q',
        part_id: 'q',
        role: e.message.role,
        time: isoMs(e.message.timestamp),
        snippet:
          (a > 0 ? '…' : '') + text.slice(a, b).replace(/\n/g, ' ') + (b < text.length ? '…' : ''),
      })
      if (hits.length >= 300) break
    }
  }
  hits.sort((x, y) => y.time - x.time)
  return hits
}

export { allSessionMeta, appendSessionInfo, deriveTitle, errTiles, loadMeta, metaCache, ocSession, saveMeta, searchSessions, sessionMeta, toHistMsgs, trashSessionFile, workspaceChips }
