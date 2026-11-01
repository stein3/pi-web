// pi-web/lib/sessions.glue — cold-resume glue: id -> file -> live-or-resumed
// child, and the once-per-session auto-namer.
import fs from 'node:fs'
import path from 'node:path'
import { PI_CWD } from './config.mjs'
import { seedDup } from './ids.mjs'
import { emitSessionUpdated } from './notify.mjs'
import { allSessionMeta, deriveTitle, sessionMeta } from './projection.mjs'
import { armSession, byPublicId, entryFor, live, reapedFile, sessionByAnyId } from './registry.mjs'
import { liveManager } from './tau.mjs'
import { autoTitle, firstUserText } from './titles.mjs'
import { leafChain, listFiles, readHeaderCwd, readJsonl } from './transcript.mjs'
import { liveCwd } from './util.mjs'

/**
 * Name an unnamed session, once, through pi itself.
 *
 * `hint` is the live user message when one is arriving right now; otherwise the
 * first prompt of the transcript is used. Claiming `e.named` before the round
 * trip is what keeps the RPC, the event it causes, and the next user message
 * from each starting a rename.
 */
async function autoName(tauId, hint) {
  const e = live.get(tauId)
  const s = liveManager.get(tauId)
  if (!e || !s || e.named) return
  e.named = true
  const file = s.sessionFile || e.file || null
  // pi named it (or the operator did): never overwrite an existing name.
  if (file) {
    try {
      if (sessionMeta(file)?.named) return
    } catch {
      /* unreadable — fall through and try to name it */
    }
  }
  const name = autoTitle(hint || firstUserText(s.entries) || firstUserText(file ? readJsonl(file) : null))
  if (!name) {
    e.named = false
    return
  }
  try {
    const r = await s.send({ type: 'set_session_name', name }, { timeoutMs: 8000 })
    if (r?.success === false) {
      e.named = false
      return
    }
  } catch {
    e.named = false // child busy/closing: a later message gets another chance
    return
  }
  // pi answers with session_info_changed, which sets the title for us; do it
  // here too so a pi build that stays quiet still names the tab. Signature-gated:
  // a name nobody disagrees about needs no frame.
  e.title = name
  emitSessionUpdated(tauId, s)
}
function fileForId(id) {
  const s = sessionByAnyId(id)
  // A fresh child knows the path pi WILL write, but no bytes exist yet: handing
  // that path out would have the bridge resume a transcript that is not there,
  // and pi would mint a SECOND uuid inside a file named after the first.
  if (s?.sessionFile && fs.existsSync(s.sessionFile)) return s.sessionFile
  if (reapedFile.has(id)) return reapedFile.get(id) // child reaped; file still there
  // NOT allSessionMeta(): that walks a list cached for 1.5 s, so right after a
  // fork/rename/turn it can hand back a transcript path that no longer describes
  // this session. Fork is the route that gets bitten — it copies whatever path it is
  // given, and copying the wrong file writes a transcript whose parentIds point at
  // entries that are not in it, which pi projects as ZERO messages ("0 msgs").
  const fresh = findFileByUuid(id)
  return fresh ?? allSessionMeta().find((m) => m.id === id)?.file ?? null
}

/**
 * The transcript named by pi's public session uuid, resolved from the directory
 * listing (NOT the cached sidebar rows). The uuid is part of the file name —
 * `<iso-ts>_<uuid>.jsonl` — so a direct scan is exact and cannot be stale.
 */
function findFileByUuid(id) {
  if (!id || !/^[0-9a-f]{8}-[0-9a-f-]{27,}$/i.test(id)) return null
  const suffix = `_${id}.jsonl`
  for (const f of listFiles()) if (f.endsWith(suffix)) return f
  return null
}

/**
 * Transcript entries for the pure-history route.
 *
 * A live child's mirror is NOT a transcript. tau's `trackMessage`
 * (pi-tau-web-server/bin/sessions.js:490-500) pushes `{type:'message', message}`
 * with NO `id` and NO `parentId`, holds only what THIS process streamed, and never
 * drops the branch a revert abandoned. Projecting it made `/api/history/session/
 * {id}` disagree with the sidebar row (which reads the file), so opening a fork
 * from the sidebar showed one thing and its row another.
 *
 * The JSONL is the transcript: pi wrote it, pi re-loads it on the next cold start,
 * and `leafChain` gives the same active path pi would derive. A child with no file
 * yet (created, never prompted) still has only its mirror, so that stays.
 */
function entriesForId(id) {
  const file = fileForId(id)
  if (file) return leafChain(readJsonl(file))
  const s = sessionByAnyId(id)
  return s?.entries ?? null
}

/**
 * Any id this session can answer as: the live child if one knows the id,
 * else resume it from its JSONL (aliasing the public id so later SSE lands
 * on the same tab). null when the id names nothing.
 */
async function sessionFor(id) {
  const existing = sessionByAnyId(id)
  if (existing) return existing
  const file = fileForId(id)
  if (!file) return null
  // A live child may already own this transcript under another alias (its tau
  // id, or the uuid). Never run two children against one JSONL — attach to the
  // one that exists and remember the alias instead.
  const owned = liveManager.findBySessionFile(file)
  if (owned) {
    entryFor(owned.id).ids.add(id)
    byPublicId.set(id, owned.id)
    return owned
  }
  const entries = leafChain(readJsonl(file))
  const hdrCwd = readHeaderCwd(file)
  const runCwd = liveCwd(hdrCwd) || PI_CWD
  if (hdrCwd && hdrCwd !== runCwd) {
    // say it out loud: the workspace was renamed/moved, and the child will not be
    // where the transcript says it was (the sidebar must not claim a dead path)
    console.log(`[resume] ${path.basename(file)}: header cwd ${JSON.stringify(hdrCwd)} is gone -> running in ${runCwd}`)
  }
  const s = await liveManager.resume({
    sessionFile: file,
    cwd: runCwd,
    entries,
    sessionName: deriveTitle(entries) || null,
  })
  const e = entryFor(s.id)
  e.ids.add(id)
  e.file = s.sessionFile ?? file
  byPublicId.set(id, s.id)
  seedDup(entries, e)
  armSession(s)
  // a session opened from the sidebar that pi never named gets named now, so
  // the row, the tab and the TUI stop disagreeing about what it is called
  void autoName(s.id)
  return s
}

export { autoName, entriesForId, fileForId, sessionFor }
