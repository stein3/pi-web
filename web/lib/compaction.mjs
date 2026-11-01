// pi-web/lib/compaction.mjs — compaction entries (the OTHER transcript row) and
// the wait/settle machinery behind /summarize and held prompts.
import { sha12 } from './ids.mjs'

/**
 * pi's `compaction` entries — the OTHER kind of transcript row.
 *
 * `compact()` (manual `/compact`, or auto at the context ceiling) appends a
 * `compaction` entry holding the generated summary + `firstKeptEntryId` +
 * `tokensBefore`. Projecting only `type: "message"` — which is what opencode's
 * transcript vocabulary has — threw that text away: a compacted session read
 * back with no sign it had ever been compacted. This is the only place the
 * summary exists, and pi never re-emits it as a message.
 *
 * There is no compaction row type in the shared webui, so it lands as a normal
 * assistant message with a bold header line: the markdown renderer gives it a
 * heading, and it stays recognisable in a copied transcript too. `role: user`
 * would label it "you" and hand it a revert/fork button — wrong for generated
 * text — so it takes the role that actually produced it.
 *
 * The id is derived from the two fields BOTH surfaces carry (`firstKeptEntryId`
 * + `tokensBefore`), not from the timestamp: the live `compaction_end` event
 * has no timestamp, and a live row and the file row must be the SAME message id
 * or the post-idle refetch would duplicate/swap the row. `tokensBefore` grows
 * monotonically, so two compactions can never collide.
 */
function compactionId(entry) {
  return `cmp_${sha12(`${entry?.firstKeptEntryId ?? ''}|${entry?.tokensBefore ?? ''}`)}`
}

function compactionRow(entry) {
  const mid = compactionId(entry)
  const created = Date.parse(entry.timestamp ?? '') || 0
  const before = Number(entry.tokensBefore)
  const after = Number(entry.estimatedTokensAfter)
  // The head line (counts + label) belongs to the component, so nothing here
  // formats prose: `summary` rides raw and `state` carries the numbers.
  const summary = String(entry.summary ?? '').trim()
  return {
    id: mid,
    role: 'assistant',
    time: { created, completed: created },
    // pi stamps usage on the entry; the webui's token tally reads `tokens`
    tokens: entry.usage
      ? {
          input: entry.usage.input ?? 0,
          output: entry.usage.output ?? 0,
          reasoning: entry.usage.reasoning ?? 0,
          cache: { read: entry.usage.cacheRead ?? 0, write: entry.usage.cacheWrite ?? 0 },
        }
      : undefined,
    compacted: true, // for checks/clients; normalizeMessages drops unknown keys
    // A TYPED part, because a component needs something to put a box around and
    // the numbers must arrive structured: `normalizeMessages` (webui sse.ts:219)
    // whitelists part keys — type/text/state/synthetic survive, ROW-level keys
    // are dropped — so `type` + `state` are the only marker that reaches a
    // component. The head line is deliberately NOT invented into `text`: pi's
    // summary lands raw and the component owns the wording (M3's
    // `details.cmpbox` in Transcript.svelte builds it from `state`).
    // There is deliberately NO second text part: the box renders this one, so a
    // copy would be drawn twice. A webui build without the branch renders nothing
    // for the type — the bridge and the dist are deployed together (pi-web serves
    // the sibling checkout's `dist`), and both are rebuilt in one step.
    parts: [
      {
        id: `${mid}:0`,
        type: 'compaction',
        text: summary,
        synthetic: true,
        state: {
          status: 'ok',
          at: created || undefined,
          ...(Number.isFinite(before) ? { tokensBefore: before } : {}),
          ...(Number.isFinite(after) ? { tokensAfter: after } : {}),
          ...(entry.firstKeptEntryId ? { firstKeptEntryId: String(entry.firstKeptEntryId) } : {}),
        },
      },
    ],
  }
}

/**
 * The transcript source for a session.
 *
 * A resumed child gets its entries from pi-web's own `readJsonl()` (tau stores
 * what it is handed), so file-side `compaction` entries are already in there.
 * But tau only records `message_start`/`message_end` (`trackMessage`), so a
 * compaction that happens INSIDE a live child never reaches `s.entries` — it
 * would disappear on the very next refetch and only reappear once the child was
 * reaped and the file re-read. Live compactions are therefore kept here and
 * merged in, deduped by id against the file copy.
 */
/**
 * Entry timestamp in ms, across the three entry shapes that reach the read path:
 * pi's JSONL entries (ISO string at the top level), tau's pushed entries (the
 * stamp lives on `message`), and the bridge's synthesized compaction entry (ISO
 * string). NaN means unknown, and unknown never becomes an insertion point.
 */
function entryTime(entry) {
  const raw = entry?.timestamp ?? entry?.message?.timestamp
  if (typeof raw === 'number') return Number.isFinite(raw) ? raw : NaN
  const p = Date.parse(String(raw ?? ''))
  return Number.isFinite(p) ? p : NaN
}

function transcriptEntries(s, e) {
  const base = s?.entries ?? []
  const seen = new Set(base.filter((x) => x?.type === 'compaction').map(compactionId))
  const add = (e?.compactions ?? []).filter((c) => !seen.has(compactionId(c)))
  if (!add.length) return base
  // Splice in at the moment the compaction HAPPENED, never at the end. For a
  // live child `base` holds no compaction entry at all, so the dedupe above is
  // always empty and an append re-rendered "Context compacted" as the newest
  // message on every refetch — for hours, until the child was reaped — even
  // though whole turns had gone past it (compact-check caseOk).
  const out = [...base]
  for (const c of add.sort((a, b) => entryTime(a) - entryTime(b))) {
    const t = entryTime(c)
    let at = out.length
    if (Number.isFinite(t)) {
      at = out.findIndex((x) => {
        const xt = entryTime(x)
        return Number.isFinite(xt) && xt > t
      })
      if (at < 0) at = out.length
    }
    out.splice(at, 0, c)
  }
  return out
}

// ---------------------------------------------------------------------------
// Compaction (webui /compact -> opencode summarize -> pi RPC `compact`)
//
// The webui treats summarize as a BLOCKING POST (it flips busy + shows a
// "Compacting…" strip and only trusts session.idle), and pi answers the RPC
// only once the summary is written — one long prefill over the whole session.
// On a local llama.cpp slot that runs far past a reverse proxy's ~100 s edge
// timeout, where the failure would be a Cloudflare 524 that the webui reports
// as "/compact failed" even when pi compacted the session fine. So the HTTP
// request is answered on a ceiling: settle first -> real result; otherwise
// answer "started" and let SSE carry the outcome (see compactingWait + the
// compaction_end bridge).
//
// tauId -> number of summarize POSTs still waiting on the RPC. While one is
// open the HTTP response is the error channel and the bridge stays silent; a
// compaction_end always lands BEFORE the compact response line on pi's stdout,
// so this map is still populated when the bridge decides who reports.
// ---------------------------------------------------------------------------
const SUMMARIZE_WAIT_MS = Number(process.env.PI_WEB_SUMMARIZE_WAIT_MS || 40_000)
// tau's own send() default is 60 s, far too short for a summarisation call.
const COMPACT_RPC_MS = Number(process.env.PI_WEB_COMPACT_RPC_MS || 600_000)
const compactingWait = new Map()

// pi refuses a prompt OUTRIGHT while a compaction is running
// (core/agent-session.js:1620 — "Cannot submit a prompt while compaction is in
// progress. Wait for compaction to finish and retry."). Forwarding that as a 502
// throws away work: the user pressed send on a session that was only busy
// summarising, and the webui's composer treats a 502 as a send failure. pi-web
// knows when the summary lands (compaction_end), so it holds the send, then
// sends. Bounded: past the window the answer is 409 SessionBusyError — pi never
// accepted the text, so the client can retry without risking a duplicate.
const COMPACT_SEND_WAIT_MS = Number(process.env.PI_WEB_COMPACT_SEND_WAIT_MS || 90_000)
const compactionSettled = new Map() // tauId -> waiters resolved by compaction_end

function settleCompaction(tauId) {
  const set = compactionSettled.get(tauId)
  if (!set?.size) return
  compactionSettled.delete(tauId)
  for (const done of set) done()
}

function waitForCompactionEnd(tauId, ms) {
  return new Promise((res) => {
    const set = compactionSettled.get(tauId) ?? new Set()
    let timer = null
    const done = () => {
      if (timer) clearTimeout(timer)
      set.delete(done)
      res(true)
    }
    set.add(done)
    compactionSettled.set(tauId, set)
    timer = setTimeout(() => {
      set.delete(done)
      if (!set.size) compactionSettled.delete(tauId)
      res(false)
    }, ms)
  })
}

export { COMPACT_RPC_MS, COMPACT_SEND_WAIT_MS, SUMMARIZE_WAIT_MS, compactingWait, compactionId, compactionRow, settleCompaction, transcriptEntries, waitForCompactionEnd }
