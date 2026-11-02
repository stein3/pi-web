import { test } from 'node:test'
import { strict as assert } from 'node:assert'
import { compactionRow, compactionId } from '../../web/lib/compaction.mjs'

test('compactionRow emits exactly ONE compaction part with the raw summary', () => {
  const entry = { timestamp: '2024-01-01T00:00:00.000Z', summary: '  summarized  ', tokensBefore: 1000, estimatedTokensAfter: 200, firstKeptEntryId: 'e1', usage: { input: 1, output: 2 } }
  const row = compactionRow(entry)
  assert.equal(row.parts.length, 1)
  assert.equal(row.parts[0].type, 'compaction')
  assert.equal(row.parts[0].text, 'summarized')
  assert.equal(row.parts[0].state.tokensBefore, 1000)
  assert.equal(row.parts[0].state.tokensAfter, 200)
  assert.equal(row.parts[0].state.firstKeptEntryId, 'e1')
})

test('compactionId is deterministic', () => {
  const e = { firstKeptEntryId: 'e1', tokensBefore: 1000 }
  assert.equal(compactionId(e), compactionId(e))
  assert.match(compactionId(e), /^cmp_[0-9a-f]{12}$/)
})

test('compactionRow omits NaN token counts', () => {
  const row = compactionRow({ timestamp: '2024-01-01T00:00:00.000Z', summary: 's' })
  assert.equal(row.parts[0].state.tokensBefore, undefined)
  assert.equal(row.parts[0].state.tokensAfter, undefined)
})