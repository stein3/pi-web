// pi-web/lib/fork.mjs — fork is a FILE operation: copy the transcript, never
// pi's fork/clone RPC (that rebinds the live child).
import fs from 'node:fs'
import path from 'node:path'
import { PI_CWD } from './config.mjs'
import { freshEntryId, newSessionId } from './ids.mjs'
import { metaCache } from './projection.mjs'
import { listFiles, readJsonl } from './transcript.mjs'
import { liveCwd } from './util.mjs'

/** how many forks already point at this transcript (the webui wants "(fork #N)") */
function forkCount(file) {
  let n = 0
  for (const f of listFiles()) {
    if (path.resolve(f) === path.resolve(file)) continue
    const h = readJsonl(f).find((e) => e?.type === 'session')
    if (h?.parentSession && path.resolve(h.parentSession) === path.resolve(file)) n++
  }
  return n
}

/**
 * Fork a transcript the way pi's `SessionManager.forkFrom` does — a new
 * `<ts>_<uuid>.jsonl` written `wx`, a header carrying `parentSession: <source>`,
 * then the copied entries — with three differences the webui needs: only the
 * ACTIVE path is copied (`leafChain`, so a reverted branch does not come back to
 * life), it is cut at the fork point (pi's own `position: "before"`: the picked
 * message is EXCLUDED), and the fork is NAMED, because the webui says the engine
 * titles the fork itself. Naming it with a `session_info` entry also marks it as
 * named, so the auto-tiler will not overwrite it.
 *
 * The point of doing this on the file instead of through pi's `fork`/`clone` RPC:
 * pi's runtime fork REBINDS the live child to the new file — right for the TUI,
 * which switches into the fork, wrong here, where the old tab stays open under its
 * own uuid and would go on writing the NEW transcript.
 */
function forkTranscript(file, keepEntries, name) {
  const srcHeader = readJsonl(file).find((e) => e?.type === 'session') ?? {}
  const stamp = new Date().toISOString()
  const id = newSessionId()
  const dest = path.join(path.dirname(file), `${stamp.replace(/[:.]/g, '-')}_${id}.jsonl`)
  const header = {
    type: 'session',
    version: 3,
    id,
    timestamp: stamp,
    cwd: liveCwd(srcHeader.cwd) || PI_CWD,
    parentSession: path.resolve(file),
  }
  // A brand-new transcript must not inherit anything the row cache holds, and the
  // listing must see it: the fork is read back immediately, and a stale row makes
  // fileForId resolve it to some OTHER session's file.
  metaCache.delete(dest)
  const fd = fs.openSync(dest, 'wx') // never clobber
  try {
    fs.writeFileSync(fd, JSON.stringify(header) + '\n')
    for (const e of keepEntries) fs.writeFileSync(fd, JSON.stringify(e) + '\n')
    // NEVER title a fork whose copied entries end at an entry with no parent.
    // pi's buildSessionPath (core/session-manager.js:124-143) walks from
    // `_buildIndex`'s leaf — the LAST non-header entry in the file — and stops at
    // the first entry whose `parentId` is null. So a trailing orphan makes the whole
    // session project ZERO messages, whatever was written before it: "0 msgs", the
    // new-session page, tab closes. A revert of the FIRST user message produces
    // exactly that tail (resetLeaf -> appendCustomEntry parents at leafId = null),
    // and pi's own `forkFrom` copies every entry verbatim, so pi's CLI fork is blank
    // too. Titleing it would only bury the reason.
    const tailOrphan = keepEntries.length
      ? keepEntries[keepEntries.length - 1]?.parentId == null
      : false
    // `lastEntryId` skips an entry that has NO id: pi's `_buildIndex` keys it under
    // `undefined`, so it can never be a walk target. Parenting the title entry at
    // `undefined` (i.e. null) would make the fork's own tail an orphan, and
    // buildSessionPath stops at the first null parentId — the fork would project
    // zero messages even though the source read fine.
    const lastEntryId = [...keepEntries].reverse().find((e) => e?.id)?.id ?? null
    if (name && !tailOrphan && lastEntryId) {
      const last = lastEntryId
      // NEVER a fixed id and never omitted: the copied chain may already hold it,
      // and pi resolves parentId through an index keyed by `entry.id` (missing ids
      // included). See freshEntryId.
      const taken = new Set([id, ...keepEntries.map((e) => e?.id).filter(Boolean)])
      fs.writeFileSync(
        fd,
        JSON.stringify({ type: 'session_info', id: freshEntryId(taken), parentId: last, timestamp: stamp, name }) + '\n',
      )
    }
  } finally {
    fs.closeSync(fd)
  }
  return { file: dest, id }
}

/**
 * The entries to copy into a fork, in file order (header excluded).
 *
 * pi's own `forkFrom` (core/session-manager.js:1374-1415) copies EVERY non-header
 * entry of the source file — it does not walk an active path at all. Do the same.
 *
 * A chain-based fork was the bug. Revert to the FIRST user message and pi's own
 * navigation marker becomes an orphan ROOT: `resetLeaf()` sets leafId to null
 * (core/agent-session.js:3364) and `appendCustomEntry` then parents at
 * `this.leafId` (session-manager.js:901-911). So the "active path" is ONE entry, a
 * chain-based fork copied marker + title row, pi projected ZERO messages, the
 * sidebar row read "0 msgs", and the tab closed itself — exactly the reported
 * "workspace cleanup pi-web (fork #N)" sessions.
 *
 * A live child's `get_entries` (rpc-mode.js:503) is still consulted, but only to
 * ADD entries the file has not written yet — never to decide what the history is.
 * (Never `get_tree`: it nests nodes, and walking a resumed child's leafId through
 * that tree dies at the first parent the child never loaded.)
 */
async function forkEntriesFor(s, file) {
  const all = readJsonl(file).filter((e) => e?.type !== 'session')
  if (!s) return forkResult(all, file)
  let extra = []
  try {
    const r = await s.send({ type: 'get_entries' }, { timeoutMs: 15000 })
    if (r?.success !== false) {
      const known = new Set(all.map((e) => e.id).filter(Boolean))
      extra = (r?.data?.entries ?? [])
        .filter((e) => e?.type !== 'session' && e?.id && !known.has(e.id))
        .map((e) => ({ ...e, parentId: e.parentId ?? (all.length ? all[all.length - 1].id : null) }))
    }
  } catch {
    /* child not answering — the file alone is still a complete transcript */
  }
  return forkResult([...all, ...extra], file)
}

// pi's forkFrom (core/session-manager.js:1374) refuses only an EMPTY transcript.
// It is not enough here, because of how pi resolves a transcript on load:
// `_buildIndex` (session-manager.js:732-745) sets the leaf to the LAST non-header
// entry, and `buildSessionPath` (124-143) then walks parents and STOPS at the first
// entry whose `parentId` is null. So a transcript that ENDS at an orphan projects
// ZERO messages no matter what precedes it — verified against pi itself: user +
// assistant + a root-parented navigation marker reads back as `messages: 0`. That is
// the reported bug: fork a session whose last revert went to the first message and
// the new row says "0 msgs", opens the new-session page, and closes.
// So refuse rather than write a session nothing can render, and tell the user what
// actually happened to their history.
const MIN_FORK_ENTRIES = 2

function forkResult(entries, file) {
  if (!entries.length) {
    return { error: `${path.basename(file)} has no entries to fork` }
  }
  if (entries[entries.length - 1]?.parentId == null) {
    return {
      error:
        `${path.basename(file)} ends at an entry with no parent, so pi would project it as an empty session — ` +
        'fork a message further down, or move the leaf back with /tree first',
    }
  }
  if (entries.length < MIN_FORK_ENTRIES) {
    return { error: `${path.basename(file)} has only one forkable entry — refusing to write an unreadable fork` }
  }
  return { entries }
}

export { forkCount, forkEntriesFor, forkTranscript }
