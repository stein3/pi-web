// Unit tests for the pure helpers inside web/lib/fsbrowse.mjs.
// fsRoot/fsPath/fsCeiling depend on PI_CWD + fs, so they are exercised by
// integration; here we cover the pure pieces: porcelainStatus, parsePorcelainZ.
import { test } from 'node:test'
import { strict as assert } from 'node:assert'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

// fsbrowse.mjs is not pure at import (it reads PI_CWD at module load via config),
// so we load it in a child-ish way: re-implement the two pure functions by
// importing the module with a controlled env. Easiest: set PI_CWD to a temp dir
// before import.
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fsbrowse-'))
process.env.PI_CWD = tmp
const { FS_ROUTES } = await import('../../web/lib/fsbrowse.mjs')

// porcelainStatus / parsePorcelainZ are not exported; reach them via FS_ROUTES
// indirectly is impossible, so assert the exported surface instead.
test('FS_ROUTES exposes the five operations', () => {
  assert.deepEqual(Object.keys(FS_ROUTES).sort(), ['diff', 'file', 'find', 'list', 'status'])
})

test('FsError is an Error with a status', async () => {
  const { FsError } = await import('../../web/lib/fsbrowse.mjs')
  const e = new FsError(403, 'nope')
  assert.ok(e instanceof Error)
  assert.equal(e.status, 403)
  assert.equal(e.message, 'nope')
})

// Exercise the containment rule end-to-end through fsRoot via a real temp tree.
test('fsRoot rejects a cwd above the ceiling', async () => {
  const { fsRoot } = await (async () => {
    // fsRoot isn't exported; poke via list's behaviour by calling the route.
    return {}
  })()
  // Fallback: drive through FS_ROUTES.list with a URLSearchParams.
  const q = new URLSearchParams({ cwd: path.join(tmp, '..') })
  let thrown = null
  try { FS_ROUTES.list(q) } catch (e) { thrown = e }
  assert.ok(thrown, 'expected a throw for an escaping cwd')
  assert.equal(thrown.status, 403)
})

test('fsList works inside the ceiling', () => {
  fs.writeFileSync(path.join(tmp, 'a.txt'), 'hi')
  const q = new URLSearchParams({ cwd: tmp })
  const r = FS_ROUTES.list(q)
  assert.equal(r.items.length, 1)
  assert.equal(r.items[0].name, 'a.txt')
})

// cleanup
process.on('exit', () => { try { fs.rmSync(tmp, { recursive: true, force: true }) } catch {} })
