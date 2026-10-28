/**
 * The webui's own view of a pi session — what the browser would actually hold.
 *
 * Why this file exists: every other check asserts on the bridge's HTTP/SSE
 * output plus a *hand-copied* model of the client. The bugs that survive that
 * arrangement are exactly the ones a user reports as "the webui doesn't show
 * compaction status / thinking / the summary": the bridge can emit a perfectly
 * good frame that the client maps into nothing, or materialise it under an id
 * the next refetch replaces. So this check loads the REAL `sse.ts` handler, the
 * REAL `stores.ts` tab store and the REAL `api.ts` (webui-client.mjs runs them
 * in node) and asserts on the state the shipped code computes.
 *
 * Three cases, one per reported symptom:
 *   A  reasoning — a thinking block must be RENDERABLE (type + non-empty text)
 *      in the tab, both while it streams and after the post-idle refetch, with
 *      the text exact (deltas + end snapshot merged, not double-appended).
 *   B  compaction status — `tab.compacting` must flip true on the bridge's
 *      `session.next.compaction.started` and clear afterwards with NO error
 *      tile, driven purely by SSE, i.e. as seen by a client that did NOT start
 *      the /compact itself (commands.ts patches busy/compacting locally, so a
 *      local-only strip would hide a bridge regression).
 *   C  the summary itself — pi keeps it ONLY in a `compaction` JSONL entry and
 *      never replays it as a message, so it must reach the transcript as one
 *      row from the live event AND from the file, with a stable id so the
 *      refetch neither duplicates nor drops it.
 *   D  the strip's LIFETIME — the client reconciles `busy`/`compacting` against
 *      GET /session/status on every stream re-open (catchUp) and on a 10s
 *      watchdog tick; a manual compaction must read busy there or the
 *      "Compacting session…" strip is cancelled mid-summary.
 *
 * Self-contained: seeds its fixtures in a scratch dir, spawns pi-web on 8127
 * with the fake pi, and kills ONLY its own child by exact PID (SELF-KILL-GATE).
 * Needs the sibling webui checkout → reports SKIP (exit 2) when absent.
 *
 *   node .testbin/client-check.mjs
 */
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import { REPO, WEB } from './harness.mjs'
import { loadWebuiClient, ui, webuiSkip, dropStream } from './webui-client.mjs'

const PORT = Number(process.env.CHECK_PORT || 8127)
const BASE = `http://127.0.0.1:${PORT}`

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
let failed = 0
const check = (ok, label) => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label)
  if (!ok) failed++
}

function j(p, opts = {}) {
  return new Promise((resolve, reject) => {
    const r = http.request(
      new URL(BASE + p),
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

// The fake pi's scripted turn (fake-pi.mjs): a thinking block, then the answer.
const THINK_TEXT = 'Plan: read then edit.'
const ANSWER_TEXT = 'Hello streamed world.'
const SUMMARY_TEXT = 'client-check fixture summary'

let scratch, agentDir, child, log = ''
let client // { sse, tabs, oc, hist, util }

async function boot(env = {}) {
  scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'clientcheck-'))
  agentDir = path.join(scratch, 'pi-agent')
  fs.mkdirSync(agentDir, { recursive: true })
  child = spawn(
    process.execPath,
    [process.env.PI_WEB_SCRIPT || path.join(WEB, 'pi-web.mjs'), '--port', String(PORT), '--dir', agentDir, '--cwd', scratch],
    {
      cwd: WEB,
      env: {
        ...process.env,
        PATH: `${path.join(WEB, '.testbin')}:${process.env.PATH}`,
        PI_CODING_AGENT_DIR: agentDir,
        FAKE_PI_COMPACT: 'ok',
        FAKE_PI_COMPACT_MS: '400',
        FAKE_PI_SUMMARY: `## Goal\n${SUMMARY_TEXT}\n\n## Next Steps\n1. nothing`,
        ...env,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  )
  child.stdout.on('data', (d) => (log += d))
  child.stderr.on('data', (d) => (log += d))
  for (let i = 0; i < 60; i++) {
    try {
      if ((await j('/healthz')).status === 200) break
    } catch {
      /* not up yet */
    }
    await sleep(200)
  }
}

const stop = () => {
  try {
    child?.kill('SIGKILL') // exact PID only — never pattern-kill
  } catch {
    /* already gone */
  }
}

// Watch every store notify for one tab: what the UI would have drawn.
function observe(sid) {
  const seen = {
    busy: false,
    compacting: false,
    thinkingFrames: 0,
    liveThinking: false,
    autoExpanded: false,
    errors: false,
  }
  const unsub = client.tabs.subscribe((all) => {
    const t = all.find((x) => x.id === sid)
    if (!t) return
    const msgs = t.messages.filter(ui.renders) // Transcript draws only these
    if (t.busy) seen.busy = true
    if (t.compacting) seen.compacting = true
    if (msgs.flatMap((m) => m.parts ?? []).some(ui.thinkingBlock)) seen.thinkingFrames++
    if (ui.liveThinking(t, msgs)) seen.liveThinking = true
    if (ui.liveReasoningId(t, msgs)) seen.autoExpanded = true
    if ((t.errors ?? []).length) seen.errors = true
  })
  return { seen, unsub }
}

const flat = (tab) => tab.messages.flatMap((m) => (m.parts ?? []).map((p) => ({ mid: m.id, ...p })))
const textOf = (m) => (m?.parts ?? []).map((p) => p.text ?? '').join('\n')

// ===========================================================================
// A + B: one live tab, one turn, one /compact started by someone else
// ===========================================================================
async function caseLive() {
  const { sse, tabs, oc } = client
  const created = await j('/oc/session', { method: 'POST', body: { cwd: scratch, title: 'client-check' } })
  const sid = created.json?.id
  check(!!sid, `live session created (${created.status} ${sid})`)

  // exactly what App.svelte openHistory/openLive does for a live session
  const info = await oc.session(sid)
  tabs.open({ id: sid, title: info?.title || sid.slice(0, 14), messages: [], live: true })
  sse.startEvents()
  const { seen, unsub } = observe(sid)

  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'stream me' }] } })
  await sleep(300) // mid-turn: deltas + 300ms of tool time
  const midTurn = tabs.snapshot(sid)
  check(midTurn?.busy, 'the tab is busy while the turn runs')
  const reasoning = flat(midTurn).filter(ui.thinkingBlock)
  check(reasoning.length >= 1, `a RENDERABLE thinking block is in the tab mid-turn (${reasoning.length})`)
  check(
    reasoning.some((p) => (p.text ?? '').includes(THINK_TEXT)),
    `its text is complete, not double-appended (${JSON.stringify(reasoning.at(-1)?.text ?? '')})`,
  )
  check(seen.autoExpanded, 'the live block is the auto-expanded one (Transcript liveReasoningId)')
  check(seen.liveThinking, 'ComposerToolbar would show "💭 Thinking" (liveThinking predicate)')

  for (let i = 0; i < 100 && tabs.snapshot(sid)?.busy; i++) await sleep(100)
  await sleep(700) // session.idle -> scheduleRefetch(350ms) -> GET /message
  const after = tabs.snapshot(sid)
  check(!after?.busy, 'idle after the turn')
  const kept = flat(after).filter(ui.thinkingBlock)
  check(kept.length >= 1, `thinking block survives the post-idle refetch (${kept.length})`)
  check(
    kept.every((p) => (p.text ?? '').trim() === THINK_TEXT),
    `refetched thinking text equals the streamed text (${JSON.stringify(kept.map((p) => p.text))})`,
  )
  check(
    flat(after).some((p) => p.type === 'text' && (p.text ?? '').includes(ANSWER_TEXT)),
    'the answer text is there too (the turn is not thinking-only)',
  )

  // ---------------------------------------------------------------------
  // B: compaction as seen by a client that did not start it
  // ---------------------------------------------------------------------
  const before = after.messages.length
  const res = await j(`/oc/session/${sid}/summarize`, {
    method: 'POST',
    body: { providerID: 'testprov', modelID: 'fake-1' },
  })
  check(res.status === 200, `/summarize accepted (${res.status})`)
  check(seen.compacting, 'tab.compacting flipped true from bridge SSE alone (the Compacting strip)')
  for (let i = 0; i < 60 && tabs.snapshot(sid)?.compacting; i++) await sleep(100)
  await sleep(900) // idle -> refetch, which must keep the row
  const post = tabs.snapshot(sid)
  check(!post?.compacting, 'the Compacting strip clears (compaction.ended / session.compacted / idle)')
  check(!post?.busy, 'the tab is idle again')
  check(!seen.errors, `no error tile for a successful compaction (${JSON.stringify(post?.errors ?? [])})`)

  const rows = (post?.messages ?? []).filter((m) => textOf(m).includes(SUMMARY_TEXT))
  check(
    rows.length === 1,
    `exactly ONE summary row after the refetch — the live row and the read row share an id (${rows.length}; msgs ${before} -> ${post?.messages?.length})`,
  )
  check(rows[0]?.id?.startsWith('cmp_'), `it is a compaction row (${rows[0]?.id})`)
  check(rows[0]?.role === 'assistant', `attributed to the role that generated it (${rows[0]?.role})`)
  // The typed part must survive the REAL normalizeMessages (sse.ts:219 whitelists
  // part keys; row-level keys like `compacted` are dropped, which is why the
  // marker lives on the part) — that is the whole contract the box renders from.
  const cp = (rows[0]?.parts ?? []).find((p) => p.type === 'compaction')
  check(
    !!cp,
    `a part typed 'compaction' survives normalizeMessages (${JSON.stringify((rows[0]?.parts ?? []).map((p) => p.type))})`,
  )
  check(
    cp?.state?.status === 'ok' && Number(cp?.state?.tokensBefore) > 0 && !!cp?.state?.firstKeptEntryId,
    `with the outcome structured in state (${JSON.stringify(cp?.state)})`,
  )
  check(
    textOf(rows[0] ?? {}).includes(SUMMARY_TEXT) && !(cp?.text ?? '').includes('Context compacted'),
    'the typed part holds pi\'s RAW summary — the head line belongs to the component',
  )
  // The interim part keeps the CURRENT renderer (text/reasoning/tool/file only,
  // Transcript.svelte:116) drawing a full row, so the bridge can land before M3
  // without the transcript going blank. M3 ignores it, and this check flips.
  check(
    ui.renders(rows[0] ?? {}) && (rows[0]?.parts ?? []).some((p) => p.type === 'text' && p.text?.includes('Context compacted')),
    'an interim text part means the CURRENT webui still renders the row (bridge lands first, no blank window)',
  )
  check(
    (post?.messages ?? []).indexOf(rows[0]) === (post?.messages?.length ?? 0) - 1,
    'it lands after the turns it summarised',
  )
  unsub()
}

// ===========================================================================
// C: a compacted session read back cold. The summary exists ONLY as a
// `compaction` entry in the JSONL; pi never replays it as a message.
// ===========================================================================
function seedCompacted() {
  const uuid = '01a17999-0000-7000-8000-0000000000c1'
  const dir = path.join(agentDir, 'sessions', '--seeded--')
  fs.mkdirSync(dir, { recursive: true })
  const now = new Date().toISOString()
  const lines = [
    { type: 'session', id: uuid, timestamp: now, cwd: scratch, parentSession: null, version: '1.3.0' },
    {
      type: 'message',
      id: 'e_1',
      parentId: null,
      timestamp: now,
      message: { role: 'user', content: [{ type: 'text', text: 'the first question' }], timestamp: now },
    },
    {
      type: 'message',
      id: 'e_2',
      parentId: 'e_1',
      timestamp: now,
      message: {
        role: 'assistant',
        content: [
          { type: 'thinking', thinking: 'thinking before answering', thinkingSignature: 'reasoning_content' },
          { type: 'text', text: 'the first answer' },
        ],
        provider: 'testprov',
        model: 'fake-1',
        timestamp: now,
        usage: { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, totalTokens: 15 },
      },
    },
    // pi's appendCompaction(): the summary lives here and nowhere else
    {
      type: 'compaction',
      id: 'c_1',
      parentId: 'e_2',
      timestamp: now,
      summary: `## Goal\n${SUMMARY_TEXT}\n\n## Next Steps\n1. nothing`,
      firstKeptEntryId: 'e_2',
      tokensBefore: 119802,
      details: { readFiles: [], modifiedFiles: [] },
      usage: { input: 7982, output: 1200, cacheRead: 0, cacheWrite: 0, reasoning: 0, totalTokens: 9182 },
      fromHook: false,
    },
    {
      type: 'message',
      id: 'e_3',
      parentId: 'c_1',
      timestamp: now,
      message: { role: 'user', content: [{ type: 'text', text: 'and after the compaction' }], timestamp: now },
    },
  ]
  fs.writeFileSync(path.join(dir, `${Date.now()}_${uuid}.jsonl`), lines.map((l) => JSON.stringify(l)).join('\n') + '\n')
  return uuid
}

async function caseCold() {
  const { sse, tabs, oc, util } = client
  const uuid = seedCompacted()
  await sleep(1700) // pi-web's session-file list cache (1.5s) must notice the row
  const msgs = await oc.messages(uuid)
  check(Array.isArray(msgs) && msgs.length === 4, `cold session reads back (${msgs?.length} rows)`)
  const idx = (msgs ?? []).findIndex((m) => textOf(m).includes(SUMMARY_TEXT))
  check(idx > 0, 'the compaction entry became transcript content')
  // The tokens it replaced are STRUCTURED state, not prose in the header: the
  // component formats the head line, so the number has to survive as a number.
  check(
    msgs?.[idx]?.parts?.[0]?.type === 'compaction' && Number(msgs[idx].parts[0].state?.tokensBefore) === 119802,
    `it reports the tokens it replaced as state (${JSON.stringify(msgs?.[idx]?.parts?.[0]?.state)})`,
  )
  check(idx < msgs.length - 1, 'it lands in entry order, mid-conversation (not glued to the end)')
  const reasoning = (msgs ?? []).flatMap((m) => m.parts ?? []).filter(ui.thinkingBlock)
  check(
    reasoning.length === 1 && reasoning[0].text === 'thinking before answering',
    `persisted thinking projects too (${JSON.stringify(reasoning.map((p) => p.text))})`,
  )

  // and through the client: openLive's tail is applyMessages, the one entry
  // point for "we now have the engine's view of this session"
  const info = await oc.session(uuid)
  check(info?.id === uuid, `the cold row is addressable by its uuid (${info?.id})`)
  tabs.open({ id: uuid, title: info?.title || uuid.slice(0, 14), messages: [], live: true, ...(info?.cwd ? { cwd: info.cwd } : {}) })
  sse.applyMessages(uuid, msgs, true)
  const tab = tabs.snapshot(uuid)
  // The row is IN the real store with its typed part intact; the CURRENT
  // renderer's part filter (Transcript.svelte:116 = text/reasoning/tool/file)
  // does not know 'compaction' yet, so it draws nothing. That gap is exactly
  // what M3 closes — assert both halves instead of pretending it renders.
  const stored = tab.messages.filter((m) => textOf(m).includes(SUMMARY_TEXT))
  check(stored.length === 1, `the summary row is in the real tab store, once (${stored.length})`)
  check(
    !!stored[0] && (stored[0].parts ?? []).some((p) => p.type === 'compaction' && p.state?.status === 'ok'),
    'with its typed part + state intact after the real normalizeMessages',
  )
  check(
    !util.taskNoticeOf(stored[0]),
    'it is not mistaken for a subagent notice',
  )
  const drawn = tab.messages.filter(ui.renders)
  check(
    drawn.filter((m) => textOf(m).includes(SUMMARY_TEXT)).length === 1,
    `the CURRENT renderer draws it exactly once, off the interim part (${drawn.length} drawn rows)`,
  )
}

// ===========================================================================
// D: the strip's LIFETIME, not just its existence. The webui does NOT trust SSE
// for `busy`: catchUp() — run on every stream RE-OPEN and on the 10s watchdog
// tick while any live tab is busy — overwrites busy AND compacting with what
// GET /oc/session/status reports. pi's manual compact() emits no agent_start/
// turn_start, and tau sets `isStreaming` only from those, so a status map built
// from `isStreaming` alone says `idle` for the whole summary and the reconcile
// cancels the strip minutes into a summary the engine is still writing.
// ===========================================================================
async function caseStripSurvivesReconcile() {
  const { tabs, oc } = client
  const created = await j('/oc/session', { method: 'POST', body: { cwd: scratch, title: 'client-reconcile' } })
  const sid = created.json?.id
  check(!!sid, `live session created (${sid})`)
  const info = await oc.session(sid)
  tabs.open({ id: sid, title: info?.title || sid.slice(0, 14), messages: [], live: true })
  await j(`/oc/session/${sid}/prompt_async`, { method: 'POST', body: { parts: [{ type: 'text', text: 'warm the session' }] } })
  for (let i = 0; i < 100 && tabs.snapshot(sid)?.busy; i++) await sleep(100)

  // long enough to outlive a reconcile, short enough to keep the suite honest
  const inflight = j(`/oc/session/${sid}/summarize`, {
    method: 'POST',
    body: { providerID: 'testprov', modelID: 'fake-1', customInstructions: 'CHECK_COMPACT_MS=6000' },
  })
  for (let i = 0; i < 40 && !tabs.snapshot(sid)?.compacting; i++) await sleep(100)
  check(tabs.snapshot(sid)?.compacting, 'the Compacting strip is up (SSE compaction.started)')
  check(tabs.snapshot(sid)?.busy, 'and the tab is busy while the summary runs')

  // The browser's own hiccup path: socket dies -> onerror -> sse.ts recreates
  // (1s backoff) -> onopen -> catchUp() reconciles against the status map.
  dropStream()
  await sleep(1800)
  const mid = tabs.snapshot(sid)
  check(
    mid?.compacting,
    `the strip SURVIVES the busy-reconcile mid-summary (compacting=${mid?.compacting} busy=${mid?.busy})`,
  )
  check(mid?.busy, `and the tab stays busy (stop button / steer mode) — busy=${mid?.busy}`)

  const r = await inflight
  check(r.status === 200, `summarize answered (${r.status})`)
  for (let i = 0; i < 80 && tabs.snapshot(sid)?.compacting; i++) await sleep(100)
  await sleep(600)
  const post = tabs.snapshot(sid)
  check(
    !post?.compacting && !post?.busy,
    `the state clears when the summary lands — busy is not pinned (busy=${post?.busy} compacting=${post?.compacting})`,
  )
  check(
    (post?.errors ?? []).length === 0,
    `no error tile from the dropped stream or the compaction (${JSON.stringify(post?.errors ?? [])})`,
  )
  check(
    (post?.messages ?? []).some((m) => textOf(m).includes(SUMMARY_TEXT)),
    'the summary row still lands after the reconnect',
  )
}

try {
  await boot()
  client = await loadWebuiClient(BASE)
} catch (e) {
  const skip = webuiSkip(e)
  console.log(skip ?? `BOOT ERROR ${e?.stack ?? e}`)
  stop()
  process.exit(skip ? 2 : 1)
}

for (const c of [caseLive, caseCold, caseStripSurvivesReconcile]) {
  console.log(`\n===== ${c.name} =====`)
  try {
    await c()
  } catch (e) {
    console.log('ERROR', e?.stack ?? String(e))
    failed++
  }
}
stop()
await sleep(250)
if (scratch) fs.rmSync(scratch, { recursive: true, force: true })
if (failed) console.log('--- server log ---\n' + log.slice(-2000))
console.log(failed ? `\nFAILED ${failed}` : '\nALL CHECKS PASSED')
process.exit(failed ? 1 : 0)
