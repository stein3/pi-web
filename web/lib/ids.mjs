// pi-web/lib/ids.mjs — id derivation, identical on the live path and the read
// path (sha12 message ids, file-uuid public ids, fresh entry ids).
import path from 'node:path'
import { createHash, randomBytes, randomUUID } from 'node:crypto'

/**
 * pi's own session uuid, straight from the file name pi writes:
 * `<iso-ts>_<uuid>.jsonl` (the ts has no underscores, so split on the last one).
 *
 * This is the ONE id that survives everything — child reaped, child exited,
 * server restarted — because it is the name of the transcript on disk. tau's
 * `tau_*` ids are per-child: the same session answers as a DIFFERENT tau id
 * after every resume, which is how one session ends up open in two tabs (the
 * tab holds the old id, the sidebar row was re-labelled with the new one).
 * So the tau id is an internal handle only: every PUBLIC id pi-web publishes —
 * `GET /oc/session`, `GET /oc/session/{id}`, the history rows, and the
 * `sessionID` on every SSE frame — is the uuid whenever a transcript exists.
 * A child with no file yet (created, never prompted) falls back to its tau id;
 * losing those is fine.
 */
function fileUuid(file) {
  if (!file) return null
  const base = path.basename(String(file))
  if (!base.endsWith('.jsonl')) return null
  const i = base.lastIndexOf('_')
  if (i < 0) return null
  const uuid = base.slice(i + 1, -'.jsonl'.length)
  // pi ids are UUID-ish; refuse anything that looks like a collision suffix
  return /^[0-9a-f]{8}-[0-9a-f]{4}-/i.test(uuid) ? uuid : null
}

/** the id this session is known by outside the server */
function canonicalId(s) {
  if (!s) return null
  return fileUuid(s.sessionFile) ?? s.id
}
// ---------------------------------------------------------------------------
// id derivation — identical on the live path and the read path
// ---------------------------------------------------------------------------

const LIVE_ROLES = new Set(['user', 'assistant'])

function sha12(s) {
  return createHash('sha1').update(s).digest('hex').slice(0, 12)
}

/** key -> occurrence index, walked in conversation order on BOTH paths. */
function msgId(role, ts, dupSeen) {
  const key = `${role}|${ts ?? ''}`
  const dup = dupSeen.get(key) ?? 0
  dupSeen.set(key, dup + 1)
  return `m_${sha12(`${key}|${dup}`)}`
}

function messagesOf(entries) {
  return (entries ?? []).filter((e) => e?.type === 'message' && e.message)
}

/**
 * pi's own session-id shape: uuidv7 (core/session-manager.js `createSessionId`).
 * The transcript FILE name carries it, and the file name is the public id, so it
 * is worth producing the same flavour pi does (time-ordered, sortable).
 */
function newSessionId() {
  const b = randomBytes(16)
  let ms = Date.now()
  for (let i = 5; i >= 0; i--) {
    b[i] = ms & 0xff
    ms = Math.floor(ms / 256)
  }
  b[6] = (b[6] & 0x0f) | 0x70 // version 7
  b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
  const h = b.toString('hex')
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`
}

/**
 * An entry id that is not already in this transcript.
 *
 * pi generates 8 hex chars and collision-checks them against its own index
 * (`generateId`, core/session-manager.js:21), and pi-web must do the same for
 * every entry it writes behind a child's back — because pi indexes EVERY entry by
 * `entry.id` (`_buildIndex`, session-manager.js:732) and resolves each entry's
 * `parentId` through that map. A duplicate key, or an entry with NO id (which
 * indexes as `undefined`), makes `parentId` resolve back to an earlier entry that
 * shares the key: a CYCLE. pi's `buildSessionPath()` has no cycle guard and pushes
 * forever (`RangeError: Invalid array length`, session-manager.js:140); the
 * bridge's own `leafChain()` HAS a seen-set, so it truncates to a couple of
 * entries instead — which is why a broken transcript reads "0 msgs" in the sidebar
 * and renders the new-session page instead of failing visibly.
 *
 * Two places used to get this wrong: the fork's title entry (`id: "forkname"`, a
 * fixed string that collides with the previous fork's) and `appendSessionInfo`
 * (which wrote no id at all). Copied ids are NEVER rewritten — pi's
 * `firstKeptEntryId` and a revert marker's `targetId` both address them.
 */
function freshEntryId(taken) {
  for (let i = 0; i < 100; i++) {
    const id = randomUUID().slice(0, 8)
    if (!taken.has(id)) return id
  }
  return randomUUID().replace(/-/g, '').slice(0, 12)
}
function seedDup(entries, e) {
  for (const m of messagesOf(entries).map((x) => x.message)) {
    if (LIVE_ROLES.has(m.role)) msgId(m.role, m.timestamp, e.dup)
  }
}

export { LIVE_ROLES, canonicalId, fileUuid, freshEntryId, messagesOf, msgId, newSessionId, seedDup, sha12 }
