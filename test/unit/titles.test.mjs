// Unit tests for web/lib/titles.mjs — the autoTitle pure function.
// Sub-second, no spawned pi-web, no fake pi, no ports. The integration
// check (test/titlefill-check.mjs) still guards the wiring against a real
// build's file layout; this guards the RULES against the module directly.
import { test } from 'node:test'
import { strict as assert } from 'node:assert'
import { autoTitle } from '../../web/lib/titles.mjs'

test('empty input stays empty', () => {
  assert.equal(autoTitle(''), '')
  assert.equal(autoTitle('   '), '')
})

test('a plain sentence survives', () => {
  assert.equal(autoTitle('the sidebar is wrong because it duplicates rows'), 'The sidebar is wrong because it duplicates rows')
})

test('filler openers are stripped to a fixed point', () => {
  assert.equal(autoTitle('my pi-web namer keeps my prose in the sidebar title'), 'Pi-web namer keeps my prose in the sidebar title')
  assert.equal(autoTitle('I think the sidebar is wrong because it duplicates rows'), 'The sidebar is wrong because it duplicates rows')
  assert.equal(autoTitle('ok so my tabs duplicate a resumed session'), 'Tabs duplicate a resumed session')
})

test('content words survive the strip list', () => {
  // "the", "a", "how", "what" must never be eaten
  assert.equal(autoTitle('the quick brown fox'), 'The quick brown fox')
  assert.equal(autoTitle('how the sidebar works'), 'How the sidebar works')
})

test('long input clamps at a word boundary with an ellipsis', () => {
  const long = 'this is a very long prompt that should be clamped well before it reaches the end of the sentence because it exceeds the limit'
  const out = autoTitle(long)
  assert.ok(out.length <= 63, `expected <=63 chars, got ${out.length}: ${out}`)
  assert.ok(out.endsWith('…'), `expected ellipsis, got: ${out}`)
})