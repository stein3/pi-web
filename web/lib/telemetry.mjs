// pi-web/lib/telemetry.mjs — unhandled-record tally (first sighting of every
// record type no branch looked at, plus a running count).

// ---------------------------------------------------------------------------
// unhandled-record telemetry
// ---------------------------------------------------------------------------
// pi and tau both emit more records than this bridge consumes, and a record
// that falls through `onPiEvent`/`onTauFrame` is invisible: it just never
// becomes SSE. That is how whole feature paths hide (pi reports a rename as
// `session_info_changed`, which this bridge never matched) — you cannot debug
// what you cannot see. So: log the FIRST sighting of every record type no
// branch looked at, plus a running count every Nth, and keep the tally
// queryable from /healthz. Bounded by construction — one line per type, never
// one per token.
const UNHANDLED_EVERY = Number(process.env.PI_WEB_UNHANDLED_EVERY ?? 500)
const unhandled = new Map() // 'kind:type' -> count

// A sample that can never be a transcript: keys + truncated scalars only.
function brief(v) {
  if (v === null || typeof v !== 'object') return String(v).slice(0, 80)
  if (Array.isArray(v)) return `[${v.length}]`
  const out = {}
  for (const [k, x] of Object.entries(v)) {
    if (typeof x === 'string') out[k] = x.length > 60 ? `${x.slice(0, 60)}…` : x
    else if (Array.isArray(x)) out[k] = `[${x.length}]`
    else if (x && typeof x === 'object') out[k] = `{${Object.keys(x).join(',')}}`
    else out[k] = x
  }
  return out
}

function noteUnhandled(kind, type, sample) {
  const key = `${kind}:${type ?? 'unknown'}`
  const n = (unhandled.get(key) ?? 0) + 1
  unhandled.set(key, n)
  if (n === 1 || n % UNHANDLED_EVERY === 0) {
    console.error(
      `[unhandled] ${key} n=${n}` + (sample === undefined ? '' : ` ${JSON.stringify(brief(sample))}`),
    )
  }
}

export { noteUnhandled, unhandled }
