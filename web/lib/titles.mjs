// pi-web/lib/titles.mjs — session/tool titles. The <titler> block is extracted
// verbatim by .testbin/titlefill-check.mjs: keep the two markers, no I/O inside.
import { textOf } from './content.mjs'

/**
 * Heuristic session title — the bridge is the one that names sessions.
 *
 * pi never names a session by itself (no auto-name anywhere in pi), and tau's
 * `maybeTitle` heuristic lives only in tau's memory, so it can never reach the
 * sidebar: a sidebar row is a projection of the JSONL. So the bridge derives a
 * name and hands it to pi's `set_session_name`, which appends the
 * `session_info` entry — after that pi, the TUI, the read path and the webui all
 * read the SAME string, and pi reports it back as `session_info_changed`.
 *
 * Deliberately dumb: a model call per session on a single-slot local router
 * costs more than a name is worth. Rules — collapse whitespace (pi normalizes
 * newlines in names anyway), strip leading filler to a fixed point, cut at a
 * sentence end inside the first 80 chars, else clamp to 60 at a word boundary
 * with an ellipsis, capitalise the first character.
 *
 * The strip list is why a name is not just the prompt truncated: an opener like
 * `my …` or `I think the …` must not survive into the sidebar.
 */
// <titler>
// Pure text rules; .testbin/titlefill-check.mjs extracts this block verbatim and
// asserts a table, so keep the two markers and add no I/O inside.
//
// Leading filler, one rule per source line. Applied at the START only, and
// repeatedly, because prompts stack openers ("ok so my …"). Ordered: a longer
// frame before the shorter one it would shadow (`there is` before `there`,
// `that's` before `that`). Deliberately conservative — a word that can open real
// content is NOT in here (`the`, `a`, `look`, `next`, `well`, `right`, `and`,
// `how`, `what`, bare `i`): eating the start of a real sentence is worse than a
// title that opens with a determiner.
const FILLER_SOURCES = [
  // politeness / request frames
  'do you mind',
  '(?:can|could|would|should) (?:you|i)(?: please)?',
  'please(?: kindly)?',
  'thank(?: ?you)?s?|thx',
  'sorry',
  // first-person mental / intent frames: "I think the…", "I need you to…"
  'i (?:think|thought|believe|reckon|guess|suspect|assume|recall|remember|hope|expect|noticed?)(?: that)?',
  "i(?:'m| am)? was wondering(?: (?:if|whether|that|what|why|how|when|where))?",
  "i(?:'d| would)? (?:like|love)",
  'i (?:need|needed|want|wanted|had to|have to|gotta)(?: you to| to| it)?',
  // the bare first-person subject, then the intent verb as its own rule: the
  // loop joins them ("i'm trying to" = `i'm` then `trying to`)
  "i(?:'m| am)",
  'trying to|looking to|working on|dealing with|stuck on|curious about|figuring out',
  // conversational openers
  'ok(?:ay)?|okey|okie|so|now|also|just|actually|basically|apparently|anyway(?:s)?|hey|hi|hello|btw|fyi|um+|uh+|er+|hmm+',
  // dummy subjects and the possessive / demonstrative openers that carry nothing
  "there(?:'s|'re)? (?:is|are|was|were|has|have|had|should|can|could|will|would|seems?)",
  "there(?:'s|'re)?",
  "it(?:'s| is)(?: (?:seems?|looks? like|appears|clear|obvious))?",
  "here'?s",
  'my|our|your|their|her|his|its',
  "that'?s|this|that|these|those",
]
const FILLER_PREFIX = new RegExp(`^(?:${FILLER_SOURCES.join('|')})\\b[\\s,:.!-]*`, 'i')

function autoTitle(text) {
  if (typeof text !== 'string') return ''
  const orig = text.replace(/\s+/g, ' ').trim()
  let t = orig
  let prev = ''
  for (let i = 0; t !== prev && i < 8; i++) {
    prev = t
    t = t.replace(FILLER_PREFIX, '').trim()
  }
  // Stripped down to politeness residue: the prompt IS the content, so keep it
  // rather than invent a fragment out of the last words of a request frame.
  if ((t.match(/\p{L}/gu) ?? []).length < 3) t = orig
  const se = t.search(/[.!?](\s|$)/)
  if (se > 10 && se < 80) t = t.slice(0, se + 1)
  if (t.length > 60) {
    const cut = t.slice(0, 57)
    const ws = cut.lastIndexOf(' ')
    t = (ws > 20 ? cut.slice(0, ws) : cut).trimEnd() + '…'
  }
  return t.charAt(0).toUpperCase() + t.slice(1)
}

// </titler>

/** the first user prompt in a set of pi session entries ('' when there is none) */
function firstUserText(entries) {
  for (const e of entries ?? []) {
    if (e?.type === 'message' && e.message?.role === 'user') {
      const t = textOf(e.message.content).trim()
      if (t) return t
    }
  }
  return ''
}

function toolTitle(name, input) {
  if (!input || typeof input !== 'object') return ''
  const v =
    input.command ??
    input.filePath ??
    input.file_path ??
    input.path ??
    input.pattern ??
    input.url ??
    input.query ??
    input.description
  return v ? String(v).replace(/\s+/g, ' ').slice(0, 160) : ''
}

// Minimum gap between two detail-line frames for the SAME streaming tool call.
// pi emits one `toolcall_delta` per token of argument JSON; the webui shallow-
// merges part state, so a frame per token is a frame per token of UI work. The
// authoritative frame always follows at toolcall_end, so coalescing here only
// costs a slightly staler typing effect.
const TOOL_TITLE_MS = Math.max(0, Number(process.env.PI_WEB_TOOL_TITLE_MS ?? 250))
const PARTIAL_CAP = 4096 // bytes of argument JSON scanned for the detail line

// The same fields toolTitle prefers, in the same order.
const TITLE_KEYS = ['command', 'filePath', 'file_path', 'path', 'pattern', 'url', 'query', 'description']
const TITLE_RE = (k) => new RegExp(`"${k}"\\s*:\\s*"((?:[^"\\\\]|\\\\.)*)`)

/**
 * Read the detail line out of a PARTIAL tool-call argument document.
 *
 * pi streams a tool call's arguments as `toolcall_delta` fragments, and nothing
 * else on the wire names the call before it is complete: `toolcall_start` is
 * `{id, toolName}` and `toolcall_end` is the first record carrying arguments.
 * So a row that stays blank until the model finishes writing the call — and a
 * row that shows nothing at all while a long `command` streams — is a choice
 * about who reads `toolcall_delta`. The value is decoded from however much JSON
 * has arrived: the field's opening quote is enough, an unterminated escape is
 * dropped rather than shown raw.
 */
function partialTitle(name, text) {
  if (!text) return ''
  for (const k of TITLE_KEYS) {
    const m = TITLE_RE(k).exec(text)
    if (!m) continue
    let v = m[1]
    try {
      v = JSON.parse('"' + v + '"')
    } catch {
      const trimmed = v.replace(/\\+$/, '') // cut mid-escape: show what is unambiguous
      try {
        v = JSON.parse('"' + trimmed + '"')
      } catch {
        /* keep the raw fragment; toolTitle clips it */
      }
    }
    const t = toolTitle(name, { [k]: v })
    if (t) return t
  }
  return ''
}

export { PARTIAL_CAP, TOOL_TITLE_MS, autoTitle, firstUserText, partialTitle, toolTitle }
