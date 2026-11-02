import { test } from 'node:test'
import { strict as assert } from 'node:assert'
import { liveCwd, expandHome, apiErr, fileToImage } from '../../web/lib/util.mjs'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

test('liveCwd keeps an existing dir', () => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'lc-'))
  assert.equal(liveCwd(d), d)
  fs.rmSync(d, { recursive: true })
})

test('liveCwd falls back to nearest existing ancestor', () => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'lc-'))
  const gone = path.join(d, 'renamed', 'child')
  assert.equal(liveCwd(gone), d)
  fs.rmSync(d, { recursive: true })
})

test('liveCwd never returns the filesystem root', () => {
  assert.equal(liveCwd('/definitely/does/not/exist/anywhere'), '')
  assert.equal(liveCwd(''), '')
})

test('expandHome handles ~', () => {
  const home = process.env.HOME
  assert.equal(expandHome('~/x'), path.join(home, 'x'))
  assert.equal(expandHome('~'), home)
  assert.equal(expandHome('/abs'), '/abs')
})

test('apiErr shape', () => {
  assert.deepEqual(apiErr('boom', 'X'), { name: 'X', data: { message: 'boom' } })
})

test('fileToImage converts a data URL file part', () => {
  const out = fileToImage({ type: 'file', url: 'data:image/png;base64,AAAA', mime: 'image/png' })
  assert.deepEqual(out, { type: 'image', data: 'AAAA', mimeType: 'image/png' })
  assert.equal(fileToImage({ type: 'file', url: 'data:text/plain;base64,AAAA' }), null)
  assert.equal(fileToImage({ type: 'text' }), null)
})