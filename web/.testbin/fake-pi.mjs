// Minimal pi RPC stand-in: emits a full assistant turn with text deltas,
// a tool call, tool result, then agent_settled — the exact sequence the
// bridge translates into opencode SSE.
import readline from 'node:readline'
const out = (o) => process.stdout.write(JSON.stringify(o) + '\n')
const ev = (o) => out({ type: 'event', ...o })
const ts = () => Date.now()

const rl = readline.createInterface({ input: process.stdin })
rl.on('line', (line) => {
  let cmd
  try { cmd = JSON.parse(line) } catch { return }
  if (cmd.type === 'prompt') {
    out({ type: 'response', command: 'prompt', success: true, id: cmd.id, data: { disposition: 'started' } })
    setTimeout(() => {
      out({ type: 'agent_start' })
      out({ type: 'turn_start' })
      const t0 = ts()
      out({ type: 'message_start', message: { role: 'user', content: [{ type: 'text', text: cmd.message ?? '' }], timestamp: t0 } })
      out({ type: 'message_end', message: { role: 'user', content: [{ type: 'text', text: cmd.message ?? '' }], timestamp: t0 } })
      const t1 = ts()
      out({ type: 'message_start', message: { role: 'assistant', content: [], provider: 'testprov', model: 'fake-1', timestamp: t1 } })
      out({ type: 'message_update', assistantMessageEvent: { type: 'text_start', contentIndex: 0 } })
      for (const w of ['Hello ', 'streamed ', 'world.']) {
        out({ type: 'message_update', assistantMessageEvent: { type: 'text_delta', contentIndex: 0, delta: w } })
      }
      out({ type: 'message_update', assistantMessageEvent: { type: 'text_end', contentIndex: 0, content: 'Hello streamed world.' } })
      out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_start', contentIndex: 1, id: 'call_1', toolName: 'bash' } })
      out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_end', contentIndex: 1, id: 'call_1', toolCall: { id: 'call_1', name: 'bash', arguments: '{"command":"echo hi"}' } } })
      out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_start', contentIndex: 2, id: 'call_2', toolName: 'read' } })
      out({ type: 'message_update', assistantMessageEvent: { type: 'toolcall_end', contentIndex: 2, id: 'call_2', toolCall: { id: 'call_2', name: 'read', arguments: '{"path":"/tmp/x.txt"}' } } })
      // pi's REAL order (agent-loop.ts): the assistant message_end fires when
      // the LLM stream ends — BEFORE any tool runs. Tools execute next
      // (tool_execution_*), then each toolResult is emitted as its own
      // message_start/message_end pair.
      out({ type: 'message_end', message: { role: 'assistant', content: [{ type: 'text', text: 'Hello streamed world.' }, { type: 'toolCall', id: 'call_1', name: 'bash', arguments: '{"command":"echo hi"}' }, { type: 'toolCall', id: 'call_2', name: 'read', arguments: '{"path":"/tmp/x.txt"}' }], provider: 'testprov', model: 'fake-1', timestamp: t1, durationMs: ts() - t1, usage: { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, totalTokens: 15 } } })
      out({ type: 'tool_execution_start', toolCallId: 'call_1', toolName: 'bash', args: { command: 'echo hi' } })
      out({ type: 'tool_execution_start', toolCallId: 'call_2', toolName: 'read', args: { path: '/tmp/x.txt' } })
      out({ type: 'tool_execution_end', toolCallId: 'call_1', toolName: 'bash', args: { command: 'echo hi' }, result: 'hi\n', isError: false })
      out({ type: 'tool_execution_end', toolCallId: 'call_2', toolName: 'read', args: { path: '/tmp/x.txt' }, result: 'file contents\n', isError: false })
      // pi persists tool results as their own messages (END timestamp + runtime)
      // after the tools finish — the projection joins them back onto the tool
      // call for the per-tool window (`bash 5ms`, `read 3ms`).
      const tr1 = { role: 'toolResult', toolCallId: 'call_1', toolName: 'bash', content: [{ type: 'text', text: 'hi\n' }], isError: false, timestamp: ts(), durationMs: 5 }
      const tr2 = { role: 'toolResult', toolCallId: 'call_2', toolName: 'read', content: [{ type: 'text', text: 'file contents\n' }], isError: false, timestamp: ts(), durationMs: 3 }
      out({ type: 'message_start', message: tr1 })
      out({ type: 'message_end', message: tr1 })
      out({ type: 'message_start', message: tr2 })
      out({ type: 'message_end', message: tr2 })
      out({ type: 'turn_end' })
      out({ type: 'agent_end' })
      out({ type: 'agent_settled', aborted: false })
    }, 50)
  } else if (cmd.type === 'set_model') {
    out({ type: 'response', command: 'set_model', success: true, id: cmd.id, data: { provider: cmd.provider, id: cmd.modelId } })
  } else if (cmd.type === 'get_state' || cmd.type === 'get_session_stats') {
    out({ type: 'response', command: cmd.type, success: true, id: cmd.id, data: {} })
  }
})
