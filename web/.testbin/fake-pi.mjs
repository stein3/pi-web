// Minimal pi RPC stand-in: emits a full assistant turn with text deltas,
// a tool call, tool result, then agent_settled — the exact sequence the
// bridge translates into opencode SSE.
//
// Queue semantics follow pi 1.3.0 (dist/core/agent-session.js +
// pi-agent-core/dist/agent.js), because the queue is the whole point of the
// steer/follow-up feature:
//   * an idle prompt starts a run (disposition "started");
//   * a prompt during a run REQUIRES streamingBehavior, and lands in the
//     steering queue ("steer") or the follow-up queue ("followUp"), acking
//     "queued" and emitting queue_update with the COMPLETE lists;
//   * steering is polled at the turn boundary (after the current turn's tool
//     calls, before the next LLM call); follow-ups at the settle boundary,
//     `one-at-a-time` by default;
//   * a user message is removed from the queue mirror BEFORE its message_start
//     is emitted, so the UI never shows a message twice;
//   * "/name" runs synchronously (no queue) and only acks "handled" when the
//     handler returns;
//   * abort does NOT clear the queues.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import crypto from 'node:crypto'
import readline from 'node:readline'

const out = (o) => {
  process.stdout.write(JSON.stringify(o) + '\n')
  mirrorEntry(o)
}

// --- a transcript on disk, FAKE_PI_PERSIST=1 ---------------------------------
// The id contract cannot be tested without it: real pi mints
// `<agentDir>/sessions/--<cwd-mangled>--/<ts>_<uuid>.jsonl` the moment the
// session has a user or assistant message (setup entries stay in memory), keeps
// the header's id when launched with `--session <file>`, and appends one line
// per entry afterwards. Off by default so the older checks are untouched.
const PERSIST = process.env.FAKE_PI_PERSIST === '1'
let pFile = null
let pFlushed = false
let pHeader = null
let pParent = null
let pSeq = 0
const pPending = []

function sessionDirFor(cwd) {
  const agentDir = process.env.PI_CODING_AGENT_DIR || path.join(os.homedir(), '.pi', 'agent')
  return path.join(agentDir, 'sessions', `--${cwd.replace(/^[/\\]/, '').replace(/[/\\:]/g, '-')}--`)
}

function ensureSessionFile() {
  if (pFile) return pFile
  const i = process.argv.indexOf('--session')
  if (i >= 0 && process.argv[i + 1]) {
    pFile = path.resolve(process.argv[i + 1])
    try {
      pHeader = JSON.parse(fs.readFileSync(pFile, 'utf8').split('\n')[0])
      pFlushed = fs.statSync(pFile).size > 0 // pi: the file is there -> append to it
    } catch {
      pHeader = null
    }
    return pFile
  }
  const stamp = new Date().toISOString()
  pHeader = { type: 'session', version: 3, id: crypto.randomUUID(), timestamp: stamp, cwd: process.cwd() }
  pFile = path.join(sessionDirFor(process.cwd()), `${stamp.replace(/[:.]/g, '-')}_${pHeader.id}.jsonl`)
  return pFile
}

function persist(entry) {
  if (!PERSIST) return
  ensureSessionFile()
  if (pFlushed) {
    fs.appendFileSync(pFile, JSON.stringify(entry) + '\n')
    return
  }
  pPending.push(entry)
  // pi writes the file only once there is a conversation to write
  if (!pPending.some((e) => e.type === 'message' && (e.message.role === 'user' || e.message.role === 'assistant'))) return
  fs.mkdirSync(path.dirname(pFile), { recursive: true })
  const header = pHeader ?? { type: 'session', version: 3, id: crypto.randomUUID(), timestamp: new Date().toISOString(), cwd: process.cwd() }
  const fd = fs.openSync(pFile, 'wx') // pi creates it exclusively, header first
  try {
    fs.writeFileSync(fd, JSON.stringify(header) + '\n')
    for (const e of pPending) fs.writeFileSync(fd, JSON.stringify(e) + '\n')
  } finally {
    fs.closeSync(fd)
  }
  pFlushed = true
  pPending.length = 0
}

// --- the entry tree (get_tree, /tau-tree-navigate) -------------------------
// pi stores an append-only tree: every entry has id/parentId, the current
// position is the "leaf", and moving the leaf is done by tau's bundled pi
// extension, which appends a `custom` marker (customType 'tau:navigate-tree')
// AT the target — the marker becomes the new leaf, because pi re-derives the
// leaf from the last entry when a file is loaded. pi's native RPC has no
// leaf-move command, so this is the whole contract tau drives.
const entries = []
const byId = new Map()
let leafId = null

function treeAdd(entry) {
  entries.push(entry)
  byId.set(entry.id, entry)
  leafId = entry.id
}

function treeOf(id) {
  const kids = new Map()
  for (const e of entries) {
    const p = e.parentId ?? null
    if (!kids.has(p)) kids.set(p, [])
    kids.get(p).push(e)
  }
  const build = (e) => ({ entry: e, children: (kids.get(e.id) ?? []).map(build) })
  return entries.filter((e) => !e.parentId || !byId.has(e.parentId)).map(build)
}

function navTarget(entry) {
  if (entry.type === 'message' && entry.message?.role === 'user') {
    const text = (entry.message.content ?? []).filter((b) => b?.type === 'text').map((b) => b.text).join('\n')
    return { leafTargetId: entry.parentId ?? null, editorText: text }
  }
  return { leafTargetId: entry.id }
}

function mirrorEntry(ev) {
  if (ev?.type === 'message_end' && ev.message) {
    const id = `e${++pSeq}`
    const entry = { type: 'message', id, parentId: leafId, timestamp: new Date().toISOString(), message: ev.message }
    treeAdd(entry)
    persist(entry)
    pParent = id
  } else if (ev?.type === 'session_info_changed' && ev.name) {
    const id = `e${++pSeq}`
    const entry = { type: 'session_info', id, parentId: leafId, timestamp: new Date().toISOString(), name: ev.name }
    treeAdd(entry)
    persist(entry)
    pParent = id
  }
}
const ts = () => Date.now()
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// turn 1 streams + runs two tools; the boundary pause is the window a test uses
// to arm a steer while the agent is mid-flight.
const TOOL_MS = 300
const TURN_GAP_MS = 250

// Token-stream shape knobs for the SSE-volume check (stream-check.mjs): a real
// turn delivers dozens-to-hundreds of text deltas, while the default three
// words keep every other check's assertions (and its runtime) unchanged.
const STREAM_WORDS = Number(process.env.FAKE_STREAM_WORDS || 0)
const DELTA_MS = Number(process.env.FAKE_DELTA_MS || 0)
const DEFAULT_WORDS = ['Hello ', 'streamed ', 'world.']
function streamWords() {
  if (!STREAM_WORDS) return DEFAULT_WORDS
  return Array.from({ length: STREAM_WORDS }, (_, i) => (i === STREAM_WORDS - 1 ? 'end.' : `w${i} `))
}

// Real pi (any reasoning model — including the loaded llama.cpp GGUF) opens the
// assistant message with a `thinking` block and streams it as
// thinking_start/delta/end before the answer. The fake used to skip it, so not
// ONE check exercised the bridge's reasoning mapping — a whole feature could
// break silently. Shapes are pi's own (pi-ai message events:
// thinking_end carries `content`, not `thinking`). FAKE_PI_THINKING=0 disables
// it for checks that count deltas exactly.
const THINKING = process.env.FAKE_PI_THINKING !== '0'
const THINK_WORDS = ['Plan: ', 'read ', 'then edit.']
const THINK_TEXT = THINK_WORDS.join('')
let thinkingLevel = 'medium'
// pi streams a thinking block only while the level is not "off" — so a check
// can prove the webui's variant actually reached the child by its absence.
const thinksNow = () => THINKING && thinkingLevel !== 'off'
// pi also RE-PICKS a thinking level on EVERY model switch: it prefers a
// per-model settings default, then the global one, then the live level
// (core/agent-session.js `_getThinkingLevelForModelSwitch`). A bridge that
// set_model-s on every prompt therefore silently undoes the webui's variant
// pick. Emulated only when a check asks (FAKE_PI_MODEL_RESETS_THINKING=1),
// because no other check sets a level at all.
const MODEL_RESETS_THINKING = process.env.FAKE_PI_MODEL_RESETS_THINKING === '1'

// Captured verbatim from a real pi 1.3.0 edit result (pi's own
// generateUnifiedPatch / generateDiffString over "file contents" -> "v2").
// The edit tool is the only pi tool whose result carries `details`, and
// details.patch is what the webui's transcript diff block reads — so the fake
// must emit the real shape, not a bare string.
const EDIT_PATCH =
  '--- /tmp/x.txt\n+++ /tmp/x.txt\n@@ -1,4 +1,4 @@\n line1\n line2\n-file contents\n+file contents v2\n line4\n'
const EDIT_DIFF = ' 1 line1\n 2 line2\n-3 file contents\n+3 file contents v2\n 4 line4'
const EDIT_ARGS = { path: '/tmp/x.txt', edits: [{ oldText: 'file contents', newText: 'file contents v2' }] }
const EDIT_JSON = JSON.stringify(EDIT_ARGS)

// A tool name the bridge has never heard of, scripted ONLY when a check asks
// for it (FAKE_PI_EXTRA_TOOL=<name>). The webui draws an output box for every
// tool part, so "the result text survives for an unknown name" is a contract,
// not an accident. Off by default: other checks pin this turn's exact part and
// frame counts.
const EXTRA_TOOL = process.env.FAKE_PI_EXTRA_TOOL || ''
const EXTRA_ARGS_JSON = '{"action":"search","query":"token cost"}'
const EXTRA_TEXT = '#11 [fact/demo] (1.42) first matched line\n#12 [rule/demo] (1.10) second matched line'
const EXTRA_RESULT = { content: [{ type: 'text', text: EXTRA_TEXT }] }

let running = false
let aborted = false
let followMode = 'one-at-a-time'
const steerQ = []
const followQ = []

const resp = (cmd, data) =>
  out({ type: 'response', command: cmd.type, success: true, id: cmd.id, ...(data ? { data } : {}) })
const fail = (cmd, error) => out({ type: 'response', command: cmd.type, success: false, id: cmd.id, error })
const queueUpdate = () => out({ type: 'queue_update', steering: [...steerQ], followUp: [...followQ] })

function contentOf(text, images) {
  return [{ type: 'text', text: text ?? '' }, ...(images ?? [])]
}

// pi removes the delivered message from the queue mirror before emitting its
// message_start — the UI must never render it twice.
async function emitUser(text, images) {
  for (const q of [steerQ, followQ]) {
    const i = q.indexOf(text)
    if (i !== -1) {
      q.splice(i, 1)
      queueUpdate()
      break
    }
  }
  const t = ts()
  const message = { role: 'user', content: contentOf(text, images), timestamp: t }
  out({ type: 'message_start', message })
  out({ type: 'message_end', message })
}

async function emitAssistant(text) {
  const t = ts()
  const words = streamWords()
  const full = words.join('')
  // thinking block first -> every later contentIndex shifts by one
  const off = thinksNow() ? 1 : 0
  out({ type: 'message_start', message: { role: 'assistant', content: [], provider: 'testprov', model: 'fake-1', timestamp: t } })
  if (thinksNow()) {
    out({ type: 'message_update', assistantMessageEvent: { type: 'thinking_start', contentIndex: 0 } })
    for (const w of THINK_WORDS) {
      out({ type: 'message_update', assistantMessageEvent: { type: 'thinking_delta', contentIndex: 0, delta: w } })
      if (DELTA_MS) await sleep(DELTA_MS)
    }
    out({ type: 'message_update', assistantMessageEvent: { type: 'thinking_end', contentIndex: 0, content: THINK_TEXT, contentSignature: 'reasoning_content' } })
  }
  out({ type: 'message_update', assistantMessageEvent: { type: 'text_start', contentIndex: off } })
  for (const w of words) {
    out({ type: 'message_update', assistantMessageEvent: { type: 'text_delta', contentIndex: off, delta: w } })
    if (DELTA_MS) await sleep(DELTA_MS)
  }
  out({ type: 'message_update', assistantMessageEvent: { type: 'text_end', contentIndex: off, content: full } })
  // pi's REAL tool-call wire shape (docs/json.md + modes/json-event.js), which
  // the fake used to get wrong in three ways — and every one of them hid a live
  // transcript bug:
  //   * `toolcall_start` is the ONLY record that carries id + toolName;
  //   * the serialized arguments arrive incrementally in `toolcall_delta`;
  //   * `toolcall_end` carries the complete toolCall and NO top-level id;
  //   * `tool_execution_end` carries NO args (only start/update do).
  const calls = [
    { index: off + 1, id: 'call_1', name: 'bash', args: '{"command":"echo hi"}' },
    { index: off + 2, id: 'call_2', name: 'read', args: '{"path":"/tmp/x.txt"}' },
    { index: off + 3, id: 'call_3', name: 'edit', args: EDIT_JSON },
  ]
  if (EXTRA_TOOL) calls.push({ index: off + 4, id: 'call_extra', name: EXTRA_TOOL, args: EXTRA_ARGS_JSON })
  for (const c of calls) {
    out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_start', contentIndex: c.index, id: c.id, toolName: c.name } })
    for (let i = 0; i < c.args.length; i += 9) {
      out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_delta', contentIndex: c.index, delta: c.args.slice(i, i + 9) } })
      if (DELTA_MS) await sleep(DELTA_MS)
    }
    out({
      type: 'message_update',
      assistantMessageEvent: { type: 'toolcall_end', contentIndex: c.index, toolCall: { id: c.id, name: c.name, arguments: c.args } },
    })
  }
  // pi's REAL order (agent-loop.ts): the assistant message_end fires when
  // the LLM stream ends — BEFORE any tool runs. Tools execute next
  // (tool_execution_*), then each toolResult is emitted as its own
  // message_start/message_end pair.
  out({
    type: 'message_end',
    message: {
      role: 'assistant',
      content: [
        ...(thinksNow() ? [{ type: 'thinking', thinking: THINK_TEXT, thinkingSignature: 'reasoning_content' }] : []),
        { type: 'text', text: full },
        { type: 'toolCall', id: 'call_1', name: 'bash', arguments: '{"command":"echo hi"}' },
        { type: 'toolCall', id: 'call_2', name: 'read', arguments: '{"path":"/tmp/x.txt"}' },
        { type: 'toolCall', id: 'call_3', name: 'edit', arguments: EDIT_JSON },
        ...(EXTRA_TOOL ? [{ type: 'toolCall', id: 'call_extra', name: EXTRA_TOOL, arguments: EXTRA_ARGS_JSON }] : []),
      ],
      provider: 'testprov',
      model: 'fake-1',
      timestamp: t,
      durationMs: ts() - t,
      usage: { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, totalTokens: 15 },
    },
  })
}

async function runTools() {
  out({ type: 'tool_execution_start', toolCallId: 'call_1', toolName: 'bash', args: { command: 'echo hi' } })
  out({ type: 'tool_execution_start', toolCallId: 'call_2', toolName: 'read', args: { path: '/tmp/x.txt' } })
  out({ type: 'tool_execution_start', toolCallId: 'call_3', toolName: 'edit', args: EDIT_ARGS })
  if (EXTRA_TOOL) out({ type: 'tool_execution_start', toolCallId: 'call_extra', toolName: EXTRA_TOOL, args: { action: 'search', query: 'token cost' } })
  await sleep(TOOL_MS)
  // pi's tool_execution_end hands over the WHOLE ToolResult object
  // ({ content, details }), not its content blocks.
  // NO args on tool_execution_end: pi's end event is
  // {toolCallId, toolName, result, isError, durationMs} and nothing else.
  out({ type: 'tool_execution_end', toolCallId: 'call_1', toolName: 'bash', result: { content: [{ type: 'text', text: 'hi\n' }] }, isError: false })
  out({ type: 'tool_execution_end', toolCallId: 'call_2', toolName: 'read', result: { content: [{ type: 'text', text: 'file contents\n' }] }, isError: false })
  out({
    type: 'tool_execution_end',
    toolCallId: 'call_3',
    toolName: 'edit',
    result: {
      content: [{ type: 'text', text: 'Successfully replaced 1 block(s) in /tmp/x.txt.' }],
      details: { diff: EDIT_DIFF, patch: EDIT_PATCH, firstChangedLine: 3 },
    },
    isError: false,
  })
  if (EXTRA_TOOL)
    out({ type: 'tool_execution_end', toolCallId: 'call_extra', toolName: EXTRA_TOOL, result: EXTRA_RESULT, isError: false })
  // pi persists tool results as their own messages (END timestamp + runtime)
  // after the tools finish — the projection joins them back onto the tool
  // call for the per-tool window (`bash 5ms`, `read 3ms`).
  const tr1 = { role: 'toolResult', toolCallId: 'call_1', toolName: 'bash', content: [{ type: 'text', text: 'hi\n' }], isError: false, timestamp: ts(), durationMs: 5 }
  const tr2 = { role: 'toolResult', toolCallId: 'call_2', toolName: 'read', content: [{ type: 'text', text: 'file contents\n' }], isError: false, timestamp: ts(), durationMs: 3 }
  const tr3 = { role: 'toolResult', toolCallId: 'call_3', toolName: 'edit', content: [{ type: 'text', text: 'Successfully replaced 1 block(s) in /tmp/x.txt.' }], details: { diff: EDIT_DIFF, patch: EDIT_PATCH, firstChangedLine: 3 }, isError: false, timestamp: ts(), durationMs: 7 }
  out({ type: 'message_start', message: tr1 })
  out({ type: 'message_end', message: tr1 })
  out({ type: 'message_start', message: tr2 })
  out({ type: 'message_end', message: tr2 })
  out({ type: 'message_start', message: tr3 })
  out({ type: 'message_end', message: tr3 })
  if (EXTRA_TOOL) {
    const trX = { role: 'toolResult', toolCallId: 'call_extra', toolName: EXTRA_TOOL, content: [{ type: 'text', text: EXTRA_TEXT }], isError: false, timestamp: ts(), durationMs: 4 }
    out({ type: 'message_start', message: trX })
    out({ type: 'message_end', message: trX })
  }
}

async function bigWriteTurn(chunks) {
  const BIG = 'line of content\n'.repeat(4000) // ~76 KB, arrives BEFORE the path
  const args = JSON.stringify({ content: BIG, path: '/tmp/big.txt' })
  const t = ts()
  out({ type: 'turn_start' })
  await emitUser('/fake-bigwrite')
  out({ type: 'message_start', message: { role: 'assistant', content: [], provider: 'testprov', model: 'fake-1', timestamp: t } })
  out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_start', contentIndex: 0, id: 'call_big', toolName: 'write' } })
  const step = Math.max(64, Math.floor(args.length / chunks))
  for (let i = 0; i < args.length; i += step) {
    out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_delta', contentIndex: 0, delta: args.slice(i, i + step) } })
  }
  out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_end', contentIndex: 0, toolCall: { id: 'call_big', name: 'write', arguments: args } } })
  out({
    type: 'message_end',
    message: {
      role: 'assistant',
      content: [{ type: 'toolCall', id: 'call_big', name: 'write', arguments: args }],
      provider: 'testprov', model: 'fake-1', timestamp: t, durationMs: ts() - t,
      usage: { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, totalTokens: 15 },
    },
  })
  out({ type: 'tool_execution_start', toolCallId: 'call_big', toolName: 'write', args: { content: BIG, path: '/tmp/big.txt' } })
  out({
    type: 'tool_execution_end', toolCallId: 'call_big', toolName: 'write',
    result: { content: [{ type: 'text', text: 'Wrote /tmp/big.txt' }] }, isError: false,
  })
  const tr = { role: 'toolResult', toolCallId: 'call_big', toolName: 'write', content: [{ type: 'text', text: 'Wrote /tmp/big.txt' }], isError: false, timestamp: ts(), durationMs: 12 }
  out({ type: 'message_start', message: tr })
  out({ type: 'message_end', message: tr })
  out({ type: 'turn_end' })
}

async function runTurn(users, withTools) {
  out({ type: 'turn_start' })
  for (const u of users) await emitUser(u[0], u[1])
  await emitAssistant()
  if (withTools) await runTools()
  out({ type: 'turn_end' })
}

async function run(text, images) {
  running = true
  aborted = false
  out({ type: 'agent_start' })
  await runTurn([[text, images]], true)
  // turn boundary: steering lands before the next LLM call, so it is delivered
  // while the run is still live — this is what makes steer useful.
  while (steerQ.length && !aborted) {
    await sleep(TURN_GAP_MS)
    const batch = steerQ.splice(0, steerQ.length)
    queueUpdate()
    await runTurn(batch.map((t) => [t]), false)
  }
  // settle boundary: follow-ups, `one-at-a-time` unless told otherwise.
  while (followQ.length && !aborted) {
    await sleep(TURN_GAP_MS)
    const batch = followMode === 'all' ? followQ.splice(0, followQ.length) : followQ.splice(0, 1)
    queueUpdate()
    await runTurn(batch.map((t) => [t]), false)
  }
  out({ type: 'agent_end' })
  out({ type: 'agent_settled', aborted })
  running = false
}

const rl = readline.createInterface({ input: process.stdin })

// What the bridge actually ASKED pi, line by line. Some behaviors are invisible
// in the SSE stream (a rename is an RPC the bridge must send, not an event it
// receives), so a check asserts on this. FAKE_PI_RPC_LOG=<file>.
const rpcLog = process.env.FAKE_PI_RPC_LOG || null
const logged = { type: 1, name: 1, command: 1, message: 1, level: 1, modelId: 1, mode: 1 }

// pi's own rename path (core/agent-session.js `setSessionName`): appends a
// session_info entry AND emits `session_info_changed`. Notably pi does NOT
// rename a session by itself — nothing in pi derives a name.
let sessionName = null
let compacting = false
const fakeRename = (name) => {
  const trimmed = String(name ?? '').trim()
  if (!trimmed) return false
  sessionName = trimmed
  out({ type: 'session_info_changed', name: sessionName })
  return true
}

rl.on('line', async (line) => {
  let cmd
  try {
    cmd = JSON.parse(line)
  } catch {
    return
  }
  if (rpcLog) {
    const rec = {}
    for (const [k, v] of Object.entries(cmd)) if (logged[k] && typeof v !== 'object') rec[k] = String(v).slice(0, 120)
    try {
      fs.appendFileSync(rpcLog, JSON.stringify(rec) + '\n')
    } catch {
      /* keep running even if the log target is gone */
    }
  }
  if (cmd.type === 'set_session_name') {
    // pi: an empty name is refused; a real one persists and reports itself.
    if (!String(cmd.name ?? '').trim()) return fail(cmd, 'Session name cannot be empty')
    if (!fakeRename(cmd.name)) return fail(cmd, 'Session name cannot be empty')
    return resp(cmd)
  }
  if (cmd.type === 'prompt') {
    const text = cmd.message ?? ''
    if (compacting && !text.startsWith('/')) {
      return fail(cmd, 'Cannot submit a prompt while compaction is in progress. Wait for compaction to finish and retry.')
    }
    const images = cmd.images
    // fixture-only hook: model a rename that happened INSIDE pi (TUI /name, an
    // extension, another client) — the bridge must follow it, never fight it.
    if (text.startsWith('/fake-rename ')) {
      fakeRename(text.slice('/fake-rename '.length))
      await sleep(50)
      resp(cmd, { disposition: 'handled' })
      return
    }
    // fixture-only hook: ONE tool call whose argument blob is large and whose
    // title-bearing field arrives LAST — the adversarial shape for a bridge that
    // reads the detail line out of partial argument JSON. The bridge must still
    // name the call, and must not turn ~400 deltas into ~400 part frames.
    // tau's tree-navigation command (pi-tau-web-server/bin/tree.js): matched
    // extension commands never reach the LLM and never append a user message.
    if (text.startsWith('/tau-tree-navigate')) {
      const target = String(text).split(/\s+/)[1] ?? ''
      const entry = byId.get(target)
      if (!entry) return fail(cmd, `Entry ${target} not found in the session tree`)
      const t = navTarget(entry)
      const marker = {
        type: 'custom',
        customType: 'tau:navigate-tree',
        id: `nav${++pSeq}`,
        parentId: t.leafTargetId,
        timestamp: new Date().toISOString(),
        targetId: entry.id,
      }
      treeAdd(marker)
      persist(marker)
      resp(cmd, { disposition: 'handled' })
      return
    }
    if (text.startsWith('/fake-bigwrite')) {
      await bigWriteTurn(Number(text.slice('/fake-bigwrite'.length).trim()) || 400)
      resp(cmd, { disposition: 'handled' })
      return
    }
    if (text.startsWith('/')) {
      // extension command: runs now even during a run, acks when the handler
      // returns — this is the unbounded-ack case
      await sleep(200)
      resp(cmd, { disposition: 'handled' })
      return
    }
    if (running) {
      if (!cmd.streamingBehavior) return fail(cmd, 'Agent is already processing. Specify streamingBehavior.')
      ;(cmd.streamingBehavior === 'followUp' ? followQ : steerQ).push(text)
      queueUpdate()
      resp(cmd, { disposition: 'queued' })
      return
    }
    resp(cmd, { disposition: 'started' })
    void run(text, images)
  } else if (cmd.type === 'set_model') {
    if (MODEL_RESETS_THINKING) {
      thinkingLevel = 'medium'
      out({ type: 'thinking_level_changed', level: thinkingLevel })
    }
    resp(cmd, { provider: cmd.provider, id: cmd.modelId })
  } else if (cmd.type === 'set_thinking_level') {
    // pi: setThinkingLevel() clamps to the model's levels, appends a
    // thinking_level_change entry and emits this event only when it changes
    // (core/agent-session.js). The webui's model "variant" IS this level.
    thinkingLevel = cmd.level
    out({ type: 'thinking_level_changed', level: thinkingLevel })
    resp(cmd)
  } else if (cmd.type === 'get_thinking_level' || cmd.type === 'cycle_thinking_level') {
    resp(cmd, { level: thinkingLevel })
  } else if (cmd.type === 'set_follow_up_mode') {
    followMode = cmd.mode
    resp(cmd)
  } else if (cmd.type === 'clear_queue') {
    const data = { steering: [...steerQ], followUp: [...followQ] }
    steerQ.length = 0
    followQ.length = 0
    queueUpdate()
    resp(cmd, data)
  } else if (cmd.type === 'abort') {
    // pi aborts the run and leaves the queues armed
    aborted = true
    resp(cmd)
  } else if (cmd.type === 'get_tree') {
    resp(cmd, { leafId, tree: treeOf() })
  } else if (cmd.type === 'get_commands') {
    // tau refuses to navigate unless it can see that its extension command is
    // really loaded — otherwise the command text would fall through to the LLM.
    resp(cmd, { commands: [{ name: 'tau-tree-navigate', source: 'extension' }, { name: 'help', source: 'builtin' }] })
  } else if (cmd.type === 'get_state' || cmd.type === 'get_session_stats') {
    // pi's get_state reports the transcript path the session WILL write: pi
    // assigns sessionFile in newSession() (core/session-manager.js) long before
    // the bytes exist, and tau probes get_state 250ms after every start. pi-web's
    // create route needs that path: the uuid inside it is the one public id the
    // tab, the sidebar row and the file will all share. Only in FAKE_PI_PERSIST
    // mode — the older fixtures have no transcript at all and assert other things.
    resp(
      cmd,
      cmd.type === 'get_state' && PERSIST
        ? {
            model: 'fake-1',
            thinkingLevel,
            isStreaming: running,
            isCompacting: false,
            steeringMode: 'manual',
            followUpMode: followMode,
            sessionFile: ensureSessionFile(),
            sessionId: pHeader?.id ?? null,
            sessionName,
            autoCompactionEnabled: false,
            messageCount: 0,
            pendingMessageCount: steerQ.length + followQ.length,
          }
        : {},
    )
  } else if (cmd.type === 'compact') {
    // pi's manual compaction (core/agent-session.js `compact()`): it aborts any
    // live run FIRST, emits compaction_start, generates the summary, appends
    // the entry, emits compaction_end, and ONLY THEN answers the RPC — and it
    // never emits agent_settled for the compaction itself. That last detail is
    // why pi-web has to synthesise the session.idle.
    //
    // FAKE_PI_COMPACT picks the outcome so a check can force each path:
    //   ok | fail (fast) | slow (longer than the POST ceiling) | slowfail | abort
    // The event carries pi's prefixed `Compaction failed: …`, while the RPC
    // response carries the raw Error message (rpc-mode catches the throw).
    const mode = process.env.FAKE_PI_COMPACT || 'ok'
    const slow = mode === 'slow' || mode === 'slowfail'
    // A check can also pick the window per request: the bridge forwards
    // /summarize's `customInstructions` (pi's real argument), so a marker in it
    // lets ONE case run a summary long enough to outlive a 10s watchdog tick
    // without slowing every other case in the file.
    const req_ms = Number(/CHECK_COMPACT_MS=(\d+)/.exec(String(cmd.customInstructions ?? ''))?.[1])
    const ms = req_ms || Number(process.env.FAKE_PI_COMPACT_MS || (slow ? 1200 : 120))
    // pi's own guard (core/agent-session.js:1620): a prompt landing during a
    // summary is REFUSED, not queued. The backstop timer only matters if this
    // handler throws — the normal paths clear it themselves.
    compacting = true
    setTimeout(() => (compacting = false), ms + 25)
    out({ type: 'compaction_start', reason: 'manual' })
    await sleep(ms)
    compacting = false
    if (mode === 'abort') {
      out({ type: 'compaction_end', reason: 'manual', result: undefined, aborted: true, willRetry: false, errorMessage: undefined })
      fail(cmd, 'Compaction cancelled')
      return
    }
    if (mode === 'fail' || mode === 'slowfail') {
      out({
        type: 'compaction_end',
        reason: 'manual',
        result: undefined,
        aborted: false,
        willRetry: false,
        errorMessage: 'Compaction failed: Nothing to compact (session too small)',
      })
      fail(cmd, 'Nothing to compact (session too small)')
      return
    }
    const result = {
      summary:
        process.env.FAKE_PI_SUMMARY || '## Goal\ncompact-check fixture\n\n## Next Steps\n1. nothing',
      firstKeptEntryId: 'e_2',
      tokensBefore: 42000,
      estimatedTokensAfter: 900,
      usage: { input: 40000, output: 500, cacheRead: 0, cacheWrite: 0, totalTokens: 40500 },
      details: { readFiles: [], modifiedFiles: [] },
    }
    out({ type: 'compaction_end', reason: 'manual', result, aborted: false, willRetry: false })
    resp(cmd, result)
  }
})
