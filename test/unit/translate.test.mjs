import { test } from 'node:test'
import { strict as assert } from 'node:assert'
import { toolMetadata, toolWindow, toOcMessages } from '../../web/lib/translate.mjs'

test('toolMetadata copies only the patch (not the whole details blob)', () => {
  const big = { patch: '--- a\n+++ b\n@@ -1 +1 @@\n-old\n+new', truncation: { content: 'X'.repeat(10000) } }
  const m = toolMetadata({ path: 'f.txt' }, big)
  assert.equal(m.filediff.file, 'f.txt')
  assert.equal(m.filediff.additions, 1)
  assert.equal(m.filediff.deletions, 1)
  assert.equal(m.diff, big.patch.trim())
  // no truncation blob leaked
  assert.ok(!JSON.stringify(m).includes('X'.repeat(100)))
})

test('toolMetadata returns {} when there is no string patch', () => {
  assert.deepEqual(toolMetadata({ path: 'f' }, { foo: 1 }), {})
  assert.deepEqual(toolMetadata({ path: 'f' }, null), {})
})

test('toolWindow uses res.timestamp as end and durationMs to back-compute start', () => {
  const w = toolWindow({ timestamp: 2000, durationMs: 300 }, 1000)
  assert.equal(w.end, 2000)
  assert.equal(w.start, 1700)
})

test('toolWindow with no result is empty', () => {
  assert.deepEqual(toolWindow(null, 1000), {})
})

test('toOcMessages projects a user+assistant turn and counts only assistant usage', () => {
  const entries = [
    { type: 'session', id: 's' },
    { type: 'message', message: { role: 'user', content: [{ type: 'text', text: 'hi' }], timestamp: 1000 } },
    { type: 'message', message: { role: 'assistant', content: [{ type: 'text', text: 'yo' }], timestamp: 1001, durationMs: 5, usage: { output: 7, reasoning: 3 } } },
  ]
  const out = toOcMessages(entries)
  assert.equal(out.length, 2)
  assert.equal(out[0].role, 'user')
  assert.equal(out[1].role, 'assistant')
  // tokens_generated-style: output+reasoning = 10; user has no usage key
  assert.equal(out[1].tokens.output, 7)
  assert.equal(out[1].tokens.reasoning, 3)
  assert.equal(out[0].tokens, undefined)
})