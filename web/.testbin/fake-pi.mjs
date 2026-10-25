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
import readline from 'node:readline'

const out = (o) => process.stdout.write(JSON.stringify(o) + '\n')
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
  out({ type: 'message_start', message: { role: 'assistant', content: [], provider: 'testprov', model: 'fake-1', timestamp: t } })
  out({ type: 'message_update', assistantMessageEvent: { type: 'text_start', contentIndex: 0 } })
  for (const w of words) {
    out({ type: 'message_update', assistantMessageEvent: { type: 'text_delta', contentIndex: 0, delta: w } })
    if (DELTA_MS) await sleep(DELTA_MS)
  }
  out({ type: 'message_update', assistantMessageEvent: { type: 'text_end', contentIndex: 0, content: full } })
  out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_start', contentIndex: 1, id: 'call_1', toolName: 'bash' } })
  out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_end', contentIndex: 1, id: 'call_1', toolCall: { id: 'call_1', name: 'bash', arguments: '{"command":"echo hi"}' } } })
  out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_start', contentIndex: 2, id: 'call_2', toolName: 'read' } })
  out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_end', contentIndex: 2, id: 'call_2', toolCall: { id: 'call_2', name: 'read', arguments: '{"path":"/tmp/x.txt"}' } } })
  out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_start', contentIndex: 3, id: 'call_3', toolName: 'edit' } })
  out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_end', contentIndex: 3, id: 'call_3', toolCall: { id: 'call_3', name: 'edit', arguments: EDIT_JSON } } })
  // pi's REAL order (agent-loop.ts): the assistant message_end fires when
  // the LLM stream ends — BEFORE any tool runs. Tools execute next
  // (tool_execution_*), then each toolResult is emitted as its own
  // message_start/message_end pair.
  out({
    type: 'message_end',
    message: {
      role: 'assistant',
      content: [
        { type: 'text', text: full },
        { type: 'toolCall', id: 'call_1', name: 'bash', arguments: '{"command":"echo hi"}' },
        { type: 'toolCall', id: 'call_2', name: 'read', arguments: '{"path":"/tmp/x.txt"}' },
        { type: 'toolCall', id: 'call_3', name: 'edit', arguments: EDIT_JSON },
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
  await sleep(TOOL_MS)
  // pi's tool_execution_end hands over the WHOLE ToolResult object
  // ({ content, details }), not its content blocks.
  out({ type: 'tool_execution_end', toolCallId: 'call_1', toolName: 'bash', args: { command: 'echo hi' }, result: { content: [{ type: 'text', text: 'hi\n' }] }, isError: false })
  out({ type: 'tool_execution_end', toolCallId: 'call_2', toolName: 'read', args: { path: '/tmp/x.txt' }, result: { content: [{ type: 'text', text: 'file contents\n' }] }, isError: false })
  out({
    type: 'tool_execution_end',
    toolCallId: 'call_3',
    toolName: 'edit',
    args: EDIT_ARGS,
    result: {
      content: [{ type: 'text', text: 'Successfully replaced 1 block(s) in /tmp/x.txt.' }],
      details: { diff: EDIT_DIFF, patch: EDIT_PATCH, firstChangedLine: 3 },
    },
    isError: false,
  })
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
rl.on('line', async (line) => {
  let cmd
  try {
    cmd = JSON.parse(line)
  } catch {
    return
  }
  if (cmd.type === 'prompt') {
    const text = cmd.message ?? ''
    const images = cmd.images
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
    resp(cmd, { provider: cmd.provider, id: cmd.modelId })
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
  } else if (cmd.type === 'get_state' || cmd.type === 'get_session_stats') {
    resp(cmd, {})
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
    const ms = Number(process.env.FAKE_PI_COMPACT_MS || (slow ? 1200 : 120))
    out({ type: 'compaction_start', reason: 'manual' })
    await sleep(ms)
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
      summary: '## Goal\ncompact-check fixture\n\n## Next Steps\n1. nothing',
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
