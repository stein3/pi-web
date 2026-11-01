// pi-web/lib/notify.mjs — emitFor + the signature-gated session.updated.
import { canonicalId } from './ids.mjs'
import { ocSession } from './projection.mjs'
import { emit } from './sse.mjs'
import { liveManager } from './tau.mjs'

// ---------------------------------------------------------------------------
// live pi event -> opencode SSE
// ---------------------------------------------------------------------------

function emitFor(tauId, type, properties) {
  // ONE frame per event, addressed by the session's canonical public id (pi's
  // transcript uuid as soon as the child knows its file). `publicIdsFor` keeps the
  // tau handle resolvable INBOUND — a tab that opened with it can still address the
  // session — but echoing every frame to both ids doubled the entire SSE stream for
  // any session whose child knew its file (a 120-delta turn arrived as 240+ frames).
  const s = liveManager.get(tauId)
  emit(type, { ...properties, sessionID: canonicalId(s) ?? tauId })
}

/**
 * `session.updated` is the ONE opencode event pi-web must not mirror verbatim.
 *
 * tau calls `broadcastUpdated()` after **every** pi event and every RPC
 * response (sessions.js: `touch(true)` in handleResponse, and the tail of
 * handleEvent). Mirroring that put a `session.updated` beside every single
 * token delta — measured on the live bridge over 170 s: 6,090 session.updated
 * vs 5,375 message.part.delta, i.e. 1.13 per delta, 53% of ALL SSE frames.
 * The webui coalesces text deltas (~40 ms, sse.ts bufferDelta) but its
 * `session.updated` branch goes straight into `tabs.patch()` +
 * `patchMetrics()` — one uncoalesced full store notify per token, so the
 * transcript re-renders per token. opencode's engine only sends this event on
 * real metadata changes; that asymmetry is why the webui feels laggier on pi.
 *
 * So: republish only when the projection actually changes. `time.updated`
 * (tau's lastActiveAt, bumped per event) is the only field that really moves
 * mid-turn and is deliberately OUT of the signature — the webui uses it for
 * sidebar ordering only, and the forced emit at turn end refreshes it.
 */
const infoSig = new Map() // tauId -> signature of the last session.updated sent

function sessionSig(s) {
  if (!s) return ''
  const i = ocSession(s, undefined, false) // signature never carries time/tokens: skip the file stat
  if (!i) return ''
  return [
    i.title ?? '',
    i.cwd ?? '',
    i.cost ?? 0,
    i.model?.providerID ?? '',
    i.model?.id ?? '',
    i.revert ? 1 : 0,
  ].join('\u0000')
}

function emitSessionUpdated(tauId, s, publicId, force = false) {
  const sig = sessionSig(s)
  const changed = infoSig.get(tauId) !== sig
  if (changed) infoSig.set(tauId, sig)
  if (!changed && !force) return false
  const info = ocSession(s, publicId)
  if (!info) return false
  emitFor(tauId, 'session.updated', { info })
  return true
}

export { emitFor, emitSessionUpdated, infoSig }
