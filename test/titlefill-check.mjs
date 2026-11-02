// Unit check for the titler's TEXT RULES — the words that may never open a
// session name.
//
// Why a separate check from title-check.mjs: that one proves the WIRING (the
// bridge sends set_session_name, pi persists it, the tab is told). The bug this
// check holds is one string level down — autoTitle's idea of "conversational
// filler" was a 15-phrase allowlist (2026-10-26), so a prompt that opened with
// "my …" or "I think the …" produced a name that was just the prompt, truncated.
// That is a property of a pure function, so it gets a pure test: extract the
// marked block from the build under test and assert a table. The block lives in
// web/lib/titles.mjs since the bridge was split into modules (a single-file
// A/B build carries it inline) — extraction keeps this check working against
// BOTH layouts.
//
// Two directions, both required:
//   - FILLER: the name must not open with junk;
//   - MANGLE: a first word that IS content must survive (the risk of widening a
//     strip list is eating the beginning of a real sentence).
//
//   node test/titlefill-check.mjs
//   PI_WEB_JS=<other build> node test/titlefill-check.mjs   # prove it fails pre-fix
import fs from 'node:fs'
import path from 'node:path'
import { BRIDGE } from './harness.mjs'

// The titler block: inline in a single-file build (PI_WEB_JS A/B), or in
// lib/titles.mjs beside the entry after the split. Try both.
const block = (p) => {
  try {
    return fs.readFileSync(p, 'utf8').match(/\/\/ <titler>([\s\S]*?)\/\/ <\/titler>/)
  } catch {
    return null
  }
}
const m = block(BRIDGE) ?? block(path.join(path.dirname(BRIDGE), 'lib', 'titles.mjs'))
if (!m) {
  console.log('FAIL no // <titler> … // </titler> block in ' + BRIDGE + ' (nor lib/titles.mjs beside it) — the check would assert nothing')
  process.exit(1)
}
let titler
try {
  titler = new Function(`${m[1]}; return { autoTitle }`)()
} catch (err) {
  console.log(`FAIL titler block does not evaluate: ${err?.message ?? err}`)
  process.exit(1)
}
const { autoTitle } = titler

let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
  if (!ok) failed++
}

// ---- junk must not open a name -------------------------------------------
const FILLER = [
  // possessive / demonstrative openers — the reported complaint
  ["my pi-web namer keeps my prose in the sidebar title", 'Pi-web namer keeps my prose in the sidebar title'],
  ["our reaper kills children too early", 'Reaper kills children too early'],
  ["your tabs duplicate a resumed session", 'Tabs duplicate a resumed session'],
  ["this session should not fork twice", 'Session should not fork twice'],
  ["that fix broke the fork tab", 'Fix broke the fork tab'],
  // first-person mental and intent frames
  ["I think the sidebar is wrong because it duplicates rows", 'The sidebar is wrong because it duplicates rows'],
  ["i believe the engine telemetry is ambiguous", 'The engine telemetry is ambiguous'],
  ["I need you to look at why the engine 500s on sessions", 'Look at why the engine 500s on sessions'],
  // an EMBEDDED question word is part of the frame (what's left is a complete
  // clause); a DIRECT question keeps its wh-word — see KEEP below
  ["i was wondering why the reaper never reaps", 'The reaper never reaps'],
  ["i'm getting 502s from the compaction path", 'Getting 502s from the compaction path'],
  ["i'd like the thinking level to survive a model switch", 'The thinking level to survive a model switch'],
  // stacked openers: the strip must repeat to a fixed point
  ["ok so my understanding is the reaper kills the child", 'Understanding is the reaper kills the child'],
  ["hello there, please fix the fork tab", 'Fix the fork tab'],
  ["please just add a test for the idle reaper", 'Add a test for the idle reaper'],
  // dummy subjects
  ["there is a bug in the revert leaf chain", 'A bug in the revert leaf chain'],
  ["it's the SSE volume that makes the tab stutter", 'The SSE volume that makes the tab stutter'],
  // politeness frames (regression: these already worked)
  ["can you check why the model picker is empty", 'Check why the model picker is empty'],
  ["could you please stream the tool title while args stream", 'Stream the tool title while args stream'],
]
for (const [input, want] of FILLER) {
  const got = autoTitle(input)
  check(got === want, `strips opener: ${JSON.stringify(input)}`, `-> ${JSON.stringify(got)} (want ${JSON.stringify(want)})`)
}

// ---- a first word that is content must survive ----------------------------
const KEEP = [
  'the webui shows no thinking level after the switch',
  'a dead workspace must not make a session unsendable',
  'look at why the engine 500s on sessions',
  'next time the reaper must not kill a live child',
  'well known gotcha: the fork rebinds the child',
  'right sidebar overlaps the tab bar',
  'how does the reaper decide to kill a child',
  'what does the graph say about the compaction row',
  'mycorrhizal networks are not a session title',
  'k8s operator reconciles the transcript',
  'and the tool rows too',
]
for (const input of KEEP) {
  const got = autoTitle(input)
  const want = input.charAt(0).toUpperCase() + input.slice(1) // autoTitle capitalises
  check(got === want, `keeps content opener: ${JSON.stringify(input)}`, `-> ${JSON.stringify(got)}`)
}

// ---- degenerate input must never produce an empty or wrecked name ---------
check(autoTitle('ok so just please my') === 'Ok so just please my', 'a prompt of pure junk keeps its text', JSON.stringify(autoTitle('ok so just please my')))
check(autoTitle('there is') === 'There is', 'a two-word prompt is not stripped to nothing', JSON.stringify(autoTitle('there is')))
check(autoTitle('') === '' && autoTitle(null) === '', 'empty input still returns empty')

console.log(failed ? `\n${failed} FAILURE(S)` : '\nALL OK')
process.exit(failed ? 1 : 0)
