// Unit tests for web/lib/content.mjs — pure string extraction.
import { test } from 'node:test'
import { strict as assert } from 'node:assert'
import { textOf, safeJson, isoMs } from '../../web/lib/content.mjs'

test('textOf: string passthrough', () => {
  assert.equal(textOf('hello'), 'hello')
})

test('textOf: text blocks joined', () => {
  assert.equal(textOf([{ type: 'text', text: 'a' }, { type: 'image' }, { type: 'text', text: 'b' }]), 'ab')
})

test('textOf: unwraps a tool result object', () => {
  assert.equal(textOf({ content: [{ type: 'text', text: 'out' }], details: { huge: 'x' } }), 'out')
  // details must NOT ride along — only content blocks count
  assert.equal(textOf({ content: 'plain' }), 'plain')
})

test('textOf: array of non-text yields empty string', () => {
  assert.equal(textOf([{ type: 'tool' }]), '')
  assert.equal(textOf(null), '')
  assert.equal(textOf(undefined), '')
})

test('safeJson: parses or wraps raw', () => {
  assert.deepEqual(safeJson('{"a":1}'), { a: 1 })
  assert.deepEqual(safeJson('not json'), { _raw: 'not json' })
  assert.deepEqual(safeJson({ already: true }), { already: true })
})

test('isoMs: ms numbers pass through, sec numbers scale, strings parse', () => {
  assert.equal(isoMs(1700000000000), 1700000000000)
  assert.equal(isoMs(1700000000), 1700000000000)
  assert.equal(isoMs('2024-01-01T00:00:00.000Z'), Date.parse('2024-01-01T00:00:00.000Z'))
})