// Transcript inline-diff check: pi's edit-tool `details` must reach the webui
// as opencode's `state.metadata.filediff.patch`, on BOTH paths the transcript
// can be built from (live SSE and the JSONL read path).
//
// Regression this guards: pi-web hard-coded `metadata: {}` at every tool-part
// emission site, so edit rows rendered a path and no diff block. pi's edit tool
// returns { content, details: { diff, patch, firstChangedLine } } and persists
// `details` on the toolResult message, so the data was always there — dropped.
//
// Self-contained: seeds a REAL pi session (one with edit details) into a scratch
// agent dir, spawns pi-web on 8127 against the fake pi, asserts, kills its own
// child by exact PID. Never pattern-kill (see AGENTS.md SELF-KILL-GATE).
//
//   node test/diff-check.mjs
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { REPO, REPO_CWD, WEB, sessionSlug } from './harness.mjs'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const PORT = Number(process.env.CHECK_PORT || 8127)
const BASE = `http://127.0.0.1:${PORT}`
const SCRATCH = fs.mkdtempSync(path.join(os.tmpdir(), 'diffcheck-'))
const AGENT = path.join(SCRATCH, 'pi-agent')

// Preferred cold fixture: a REAL session with edit tool results carrying
// details.patch, which proves the projection against pi's own persisted shape.
// That dir is gitignored private data, so a fresh clone falls back to a
// synthesized session in the identical shape (see synthFixture) — the check must
// never be un-runnable just because the operator's transcripts are absent. The
// slug is DERIVED from the workspace path (harness `sessionSlug`), so it follows
// a checkout move or rename; override with DIFF_FIXTURE=<session dir>.
const FIXTURE_SRC =
  process.env.DIFF_FIXTURE || path.join(REPO, 'config/pi-agent/sessions', sessionSlug(REPO_CWD))

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}

function req(p, opts = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(BASE + p)
    const r = http.request(
      u,
      { method: opts.method || 'GET', headers: opts.body ? { 'content-type': 'application/json' } : {} },
      (res) => {
        let buf = ''
        res.on('data', (c) => (buf += c))
        res.on('end', () => {
          let json = null
          try {
            json = buf ? JSON.parse(buf) : null
          } catch {
            /* non-json */
          }
          resolve({ status: res.statusCode, json, body: buf })
        })
      },
    )
    r.on('error', reject)
    if (opts.body) r.write(JSON.stringify(opts.body))
    r.end()
  })
}

const events = []
// pi-web's SSE envelope is opencode's: { type, properties } — the part lives
// under properties, not at the top level.
const partOf = (e) => e?.properties?.part
function openSse() {
  return new Promise((resolve, reject) => {
    http.get(`${BASE}/oc/event`, (res) => {
      let buf = ''
      res.on('data', (c) => {
        buf += c
        let i
        while ((i = buf.indexOf('\n\n')) >= 0) {
          const frame = buf.slice(0, i)
          buf = buf.slice(i + 2)
          const d = frame.split('\n').find((l) => l.startsWith('data: '))
          if (!d) continue
          try {
            events.push(JSON.parse(d.slice(6)))
          } catch {
            /* keepalive */
          }
        }
      })
      resolve(res)
    }).on('error', reject)
  })
}

// ---- verbatim from webui/src/components/Transcript.svelte (patchTextOf +
// diffLines): the renderer IS the contract, so the check asserts on it, not on
// a private guess of what the bridge "should" send.
function patchTextOf(p) {
  const st = p.state ?? {}
  const md = st.metadata ?? {}
  const patch =
    (typeof md.filediff?.patch === 'string' && md.filediff.patch) ||
    (typeof md.diff === 'string' && md.diff) ||
    ''
  if (patch) return patch
  const c = st.input?.content ?? st.input?.newString
  return typeof c === 'string' && c.trim()
    ? c.replace(/\n$/, '').split('\n').map((l) => '+' + l).join('\n')
    : ''
}
function diffLines(text) {
  const out = []
  let oldN
  let newN
  const lines = text.split('\n')
  for (let idx = 0; idx < lines.length; idx++) {
    const s = lines[idx]
    if (s === '' && (idx === 0 || idx === lines.length - 1)) continue
    const h = /^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/.exec(s)
    if (h) {
      oldN = parseInt(h[1], 10)
      newN = parseInt(h[2], 10)
      out.push({ k: 'meta', s })
      continue
    }
    if (s.startsWith('+++') || s.startsWith('---') || s.startsWith('*** Update File') || s.startsWith('*** Add File') || s.startsWith('*** Delete File')) {
      out.push({ k: 'meta', s })
      continue
    }
    if (s.startsWith('+')) {
      out.push({ k: 'add', s, newLine: newN })
      if (newN !== undefined) newN++
    } else if (s.startsWith('-')) {
      out.push({ k: 'del', s, oldLine: oldN })
      if (oldN !== undefined) oldN++
    } else if (s.startsWith(' ') || s === '') {
      out.push({ k: 'ctx', s, oldLine: oldN, newLine: newN })
      if (oldN !== undefined) oldN++
      if (newN !== undefined) newN++
    }
  }
  return out
}
const renders = (p) => {
  const patch = p ? patchTextOf(p) : ''
  const rows = patch ? diffLines(patch) : []
  return { patch, rows, colored: rows.filter((r) => r.k === 'add' || r.k === 'del').length }
}

/**
 * Fallback cold fixture: a generated session in pi's exact persisted shape.
 * The cold path reads only a `{type:'session',...}` header plus
 * `{type:'message', id, parentId, timestamp, message}` entries (readJsonl +
 * toOcMessages), so that is all this emits: one successful edit whose
 * toolResult carries `details.patch`, one errored edit with NO details (the
 * contract that a failed edit never gets an invented diff), and one bash result
 * to show unrelated tools stay untouched.
 */
function synthFixture(dir) {
  const id = '01a17000-0000-4000-8000-000000000001'
  const now = new Date().toISOString()
  const patch =
    '--- /tmp/synth.txt\n+++ /tmp/synth.txt\n@@ -1,4 +1,4 @@\n keep\n-old line\n+new line\n tail\n'
  const call = (id, name, args) => ({ type: 'toolCall', id, name, arguments: JSON.stringify(args) })
  const entries = [
    { type: 'session', version: 3, id, timestamp: now, cwd: SCRATCH },
    {
      type: 'message', id: 'e1', parentId: null, timestamp: now,
      message: { role: 'user', content: [{ type: 'text', text: 'change the line' }], timestamp: now },
    },
    {
      type: 'message', id: 'e2', parentId: 'e1', timestamp: now,
      message: {
        role: 'assistant',
        content: [
          { type: 'text', text: 'editing' },
          call('call_ok', 'edit', { path: '/tmp/synth.txt', edits: [{ oldText: 'old line', newText: 'new line' }] }),
          call('call_bad', 'edit', { path: '/tmp/synth.txt', edits: [{ oldText: 'nope', newText: 'x' }] }),
          call('call_bash', 'bash', { command: 'echo hi' }),
        ],
        timestamp: now,
      },
    },
    {
      type: 'message', id: 'e3', parentId: 'e2', timestamp: now,
      message: {
        role: 'toolResult', toolCallId: 'call_ok', toolName: 'edit',
        content: [{ type: 'text', text: 'Successfully replaced 1 block(s) in /tmp/synth.txt.' }],
        details: { patch, diff: patch.replace(/^([-+]) /gm, '$1'), firstChangedLine: 2 },
        timestamp: now,
      },
    },
    {
      type: 'message', id: 'e4', parentId: 'e3', timestamp: now,
      message: {
        role: 'toolResult', toolCallId: 'call_bad', toolName: 'edit',
        content: [{ type: 'text', text: 'Could not find oldText in /tmp/synth.txt' }],
        isError: true, timestamp: now,
      },
    },
    {
      type: 'message', id: 'e5', parentId: 'e4', timestamp: now,
      message: {
        role: 'toolResult', toolCallId: 'call_bash', toolName: 'bash',
        content: [{ type: 'text', text: 'hi' }], timestamp: now,
      },
    },
  ]
  const dst = path.join(dir, `synth_${id}.jsonl`)
  fs.writeFileSync(dst, entries.map((e) => JSON.stringify(e)).join('\n') + '\n')
  return { src: '(synthesized)', file: dst, id }
}

// ---- seed a real pi session into the scratch agent dir ---------------------
function seedFixture() {
  const dir = path.join(AGENT, 'sessions', '--tmp--')
  fs.mkdirSync(dir, { recursive: true })
  const src = fs.existsSync(FIXTURE_SRC)
    ? fs
        .readdirSync(FIXTURE_SRC)
        .filter((f) => f.endsWith('.jsonl'))
        .map((f) => path.join(FIXTURE_SRC, f))
        // pick the smallest session file that has an edit result with details.patch
        .sort((a, b) => fs.statSync(a).size - fs.statSync(b).size)
        .find((f) =>
          fs
            .readFileSync(f, 'utf8')
            .split('\n')
            .some((l) => {
              try {
                const m = JSON.parse(l).message
                return m?.role === 'toolResult' && m?.toolName === 'edit' && typeof m?.details?.patch === 'string'
              } catch {
                return false
              }
            }),
        )
    : undefined
  if (!src) return synthFixture(dir)
  const dst = path.join(dir, path.basename(src))
  // rewrite the header cwd to the scratch dir so the resumed child is valid here
  const lines = fs.readFileSync(src, 'utf8').split('\n')
  const h = lines.findIndex((l) => {
    try {
      return JSON.parse(l).type === 'session'
    } catch {
      return false
    }
  })
  const header = JSON.parse(lines[h])
  header.cwd = SCRATCH
  lines[h] = JSON.stringify(header)
  fs.writeFileSync(dst, lines.join('\n'))
  return { src, file: dst, id: path.basename(src).split('_').pop().replace('.jsonl', '') }
}

// ---- boot pi-web ------------------------------------------------------------
fs.mkdirSync(AGENT, { recursive: true })
const fixture = seedFixture()
if (fixture.src === '(synthesized)')
  console.log(`note: cold fixture synthesized — no session data under ${FIXTURE_SRC}`)
const child = spawn(
  process.execPath,
  [process.env.PI_WEB_SCRIPT || path.join(WEB, 'pi-web.mjs'), '--port', String(PORT), '--dir', AGENT, '--cwd', SCRATCH],
  {
    cwd: WEB,
    env: { ...process.env, PATH: `${HERE}:${process.env.PATH}`, PI_CODING_AGENT_DIR: AGENT },
    stdio: ['ignore', 'pipe', 'pipe'],
  },
)
let serverLog = ''
child.stdout.on('data', (d) => (serverLog += d))
child.stderr.on('data', (d) => (serverLog += d))

const stop = () => {
  try {
    child.kill('SIGKILL') // exact PID — never a pattern (self-kill gate)
  } catch {
    /* already gone */
  }
}

try {
  for (let i = 0; i < 60; i++) {
    try {
      const r = await req('/healthz')
      if (r.status === 200) break
    } catch {
      /* not up yet */
    }
    await sleep(250)
  }

  // ===== cold path: real pi JSONL -> toOcMessages -> GET /message =========
  const cold = await req(`/oc/session/${fixture.id}/message`)
  check(cold.status === 200 && Array.isArray(cold.json), `cold session readable (${cold.status})`)
  const coldTools = (cold.json || [])
    .flatMap((m) => m.parts || [])
    .filter((p) => p.type === 'tool' && p.tool === 'edit')
  const coldOk = coldTools.filter((p) => p.state?.metadata?.filediff?.patch)
  check(coldTools.length > 0, `real session has edit parts (${coldTools.length})`)
  check(coldOk.length > 0, `cold path carries filediff.patch (${coldOk.length}/${coldTools.length})`)
  for (const p of coldOk.slice(0, 3)) {
    const r = renders(p)
    check(
      r.colored > 0 && r.rows.some((x) => x.k === 'meta' && x.s.startsWith('@@')),
      `cold edit part renders ${r.colored} +/- rows (${JSON.stringify(r.rows.slice(0, 4).map((x) => x.k))})`,
    )
    check(r.patch === p.state.metadata.filediff.patch, 'cold filediff.patch is the patch itself')
  }
  // never invent a diff for a failed edit: pi throws, so there is no details
  const coldErr = coldTools.filter((p) => p.state?.status === 'error')
  check(
    coldErr.every((p) => !Object.keys(p.state?.metadata ?? {}).length),
    `failed edits keep empty metadata (${coldErr.length} of them)`,
  )

  // ===== live path: fake pi emits pi's real result shape ==================
  await openSse()
  const created = await req('/oc/session', { method: 'POST', body: { cwd: SCRATCH, title: 'diff-check' } })
  check(created.status === 201, `session created (${created.status})`)
  const sid = created.json.id
  await req(`/oc/session/${sid}/prompt_async`, {
    method: 'POST',
    body: { parts: [{ type: 'text', text: 'make the edit' }] },
  })

  const editFrames = () =>
    events
      .filter((e) => e.type === 'message.part.updated' && partOf(e)?.type === 'tool' && partOf(e)?.tool === 'edit')
      .map(partOf)
  for (let i = 0; i < 40 && editFrames().length < 1; i++) await sleep(150)
  await sleep(1200) // let the toolResult message_end re-complete the part
  const frames = editFrames()
  check(frames.length >= 2, `edit part updated live (${frames.length} frames)`)

  const withPatch = frames.filter((f) => f.state?.metadata?.filediff?.patch)
  check(withPatch.length > 0, `live path carries filediff.patch (${withPatch.length}/${frames.length})`)
  // webui upserts parts with a shallow spread, so the LAST frame wins
  const last = frames[frames.length - 1]
  const lr = renders(last)
  check(!!lr.patch, 'LAST edit frame still carries the patch (no clobber)')
  check(
    lr.colored === 2 && lr.rows.some((x) => x.k === 'add') && lr.rows.some((x) => x.k === 'del'),
    `live edit renders +/- rows (${lr.rows.map((x) => x.k).join(',')})`,
  )
  check(
    last.state?.metadata?.filediff?.additions === 1 && last.state?.metadata?.filediff?.deletions === 1,
    `filediff stats counted (a=${last.state?.metadata?.filediff?.additions} d=${last.state?.metadata?.filediff?.deletions})`,
  )
  check(
    last.state?.metadata?.diff === last.state?.metadata?.filediff?.patch,
    'metadata.diff mirrors the unified patch, not pi\'s pretty details.diff',
  )
  check(last.state?.metadata?.filediff?.file === '/tmp/x.txt', `filediff.file (${last.state?.metadata?.filediff?.file})`)
  check(
    (last.state?.output ?? '').includes('Successfully replaced'),
    `tool output present on the final frame (${JSON.stringify(String(last.state?.output ?? '')).slice(0, 60)})`,
  )
  // the FIRST completed frame is the tool_execution_end one: pi hands over the
  // whole ToolResult object there, so textOf() must unwrap .content or the row
  // is blank until the toolResult message_end lands (frame order below)
  const firstCompleted = frames.find((f) => f.state?.status === 'completed')
  check(
    (firstCompleted?.state?.output ?? '').includes('Successfully replaced'),
    `tool_execution_end frame already has output (${JSON.stringify(String(firstCompleted?.state?.output ?? '')).slice(0, 60)})`,
  )

  // the read part proves unrelated tools are untouched
  const readFrames = events
    .filter((e) => e.type === 'message.part.updated' && partOf(e)?.type === 'tool' && partOf(e)?.tool === 'read')
    .map(partOf)
  const readLast = readFrames[readFrames.length - 1]
  check(
    readLast?.state?.status === 'completed' && !Object.keys(readLast?.state?.metadata ?? {}).length,
    'non-diff tools keep empty metadata',
  )

  // ===== live then refetch: same ids, same diff ===========================
  const refetch = await req(`/oc/session/${sid}/message`)
  const rTools = (refetch.json || [])
    .flatMap((m) => m.parts || [])
    .filter((p) => p.type === 'tool' && p.tool === 'edit')
  check(
    rTools.length === 1 && rTools[0].state?.metadata?.filediff?.patch === last.state.metadata.filediff.patch,
    'refetch rebuilds the identical diff',
  )
  check(
    renders(rTools[0] ?? {}).colored === 2,
    'refetched edit part still renders +/- rows',
  )
} catch (e) {
  console.log('ERROR', e.stack || String(e))
  failed++
} finally {
  const types = {}
  for (const e of events) types[e.type] = (types[e.type] || 0) + 1
  console.log('sse frames:', events.length, JSON.stringify(types))
  console.log(
    'parts:',
    events
      .filter((e) => e.type === 'message.part.updated')
      .map((e) => `${partOf(e)?.type}:${partOf(e)?.tool ?? partOf(e)?.text?.slice(0, 8) ?? ''}:${partOf(e)?.state?.status ?? ''}`)
      .join(' | '),
  )
  stop()
  await sleep(300)
  fs.rmSync(SCRATCH, { recursive: true, force: true })
  if (failed) console.log('--- server log ---\n' + serverLog.slice(-2500))
}
process.exit(failed ? 1 : 0)
