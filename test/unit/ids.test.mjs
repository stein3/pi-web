import { test } from 'node:test'
import { strict as assert } from 'node:assert'
import { fileUuid, canonicalId, sha12, msgId, messagesOf, newSessionId, freshEntryId } from '../../web/lib/ids.mjs'

test('fileUuid parses pi filenames', () => {
  const f = '/a/2026-10-25T00:00:00.000Z_12345678-1234-1234-1234-123456789abc.jsonl'
  assert.equal(fileUuid(f), '12345678-1234-1234-1234-123456789abc')
  assert.equal(fileUuid('/x/foo.txt'), null)
  assert.equal(fileUuid(null), null)
})

test('canonicalId prefers the file uuid over the tau id', () => {
  assert.equal(canonicalId({ sessionFile: '/a/2026-10-25T00:00:00.000Z_11111111-2222-3333-4444-555555555555.jsonl', id: 'tau_9' }), '11111111-2222-3333-4444-555555555555')
  assert.equal(canonicalId({ id: 'tau_9' }), 'tau_9')
  assert.equal(canonicalId(null), null)
})

test('msgId is stable and collision-aware', () => {
  const seen = new Map()
  const a = msgId('user', 1000, seen)
  const b = msgId('user', 1000, seen) // duplicate timestamp -> different id
  const c = msgId('user', 1000, new Map())
  assert.notEqual(a, b)
  assert.equal(a, c) // same counter state -> same id
  assert.match(a, /^m_[0-9a-f]{12}$/)
})

test('messagesOf filters to message entries', () => {
  const e = [{ type: 'session' }, { type: 'message', message: { role: 'user' } }, { type: 'message' }]
  assert.equal(messagesOf(e).length, 1)
  assert.equal(messagesOf(null).length, 0)
})

test('newSessionId is a uuidv7-ish string', () => {
  const id = newSessionId()
  assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
})

test('freshEntryId avoids collisions', () => {
  const taken = new Set(['aaaa', 'bbbb'])
  const id = freshEntryId(taken)
  assert.equal(id.length, 8)
  assert.ok(!taken.has(id))
  // 8 hex chars
  assert.match(id, /^[0-9a-f]{8}$/)
})