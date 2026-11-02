// Regression check: the webui's model "variant" IS pi's thinking level, and it
// has to survive the shape the webui actually sends.
//
// Why this exists (investigated 2026-10-29): the composer's variant sub-selector
// appeared to do nothing. Reading only `body.model.variant` (and `body.model.id`)
// is not what the webui posts. `webui/src/lib/api.ts prompt()` builds the body
// opencode-style:
//
//   { parts:[…], model: { providerID, modelID }, variant: 'high' }   // variant TOP-LEVEL
//
// and Composer.svelte re-sends that body on EVERY prompt — including for a tab
// that had NO live child when the pick was made, where the sibling POST
// /oc/api/session/{id}/model lands on a session id the bridge cannot resolve and
// is answered `{ok:true}` into the void. So the prompt body is the only channel
// that always exists, and the bridge read neither the ref nor the variant out of
// it: pi kept the session default and the selector looked dead.
//
// Asserts:
//   1. a pick that DOES reach a live child (`/api/session/{id}/model`, nested
//      variant) is forwarded as `set_thinking_level`;
//   2. the webui's prompt body — `model.modelID` + top-level `variant` — is
//      forwarded too, and `variant:'off'` really suppresses the thinking block
//      (fake pi emits thinking for any level but off, so absence is the proof);
//   3. the ref is applied at most once per child (pi re-picks a thinking level on
//      EVERY set_model, so a per-prompt set_model would undo the pick);
//   4. an unchanged level costs no RPC;
//   5. changing the variant mid-session changes the turn;
//   6. a model change the bridge makes INSIDE a prompt re-asserts the session's
//      level after the set_model;
//   7. an explicit model switch with no variant means "cleared" — the model goes,
//      no level is forced, pi picks its own default.
//
// Self-contained: seeds only a scratch pi dir, spawns pi-web against the fake pi
// (which logs every RPC it receives and models pi's thinking semantics), asserts
// over HTTP + that log, and kills its own child.
//
//   node test/thinking-check.mjs
//   PI_WEB_JS=<other build> node test/thinking-check.mjs   # prove it fails pre-fix
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { CHECK_PORT, REPO_CWD, TESTBIN, WEB } from './harness.mjs'

const PI_WEB = process.env.PI_WEB_JS || path.join(WEB, 'pi-web.mjs')
const PORT = Number(process.env.THINKING_CHECK_PORT || CHECK_PORT)
const BASE = `http://127.0.0.1:${PORT}`
const CWD = REPO_CWD
const SDIR = path.join(WEB, '.logs', 'thinking-check')
const AGENT = path.join(SDIR, 'pi-agent')
const RPC_LOG = path.join(SDIR, 'rpc.log')

let failed = 0
const check = (ok, label, note = '') => {
  console.log((ok ? 'PASS' : 'FAIL') + ' ' + label + (note ? ' — ' + note : ''))
  if (!ok) failed++
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

fs.rmSync(SDIR, { recursive: true, force: true })
fs.mkdirSync(AGENT, { recursive: true })

const child = spawn(
  process.execPath,
  [PI_WEB, '--port', String(PORT), '--host', '127.0.0.1', '--dir', AGENT, '--cwd', CWD],
  {
    cwd: WEB,
    env: {
      ...process.env,
      PATH: `${TESTBIN}:${process.env.PATH}`,
      FAKE_PI_RPC_LOG: RPC_LOG,
      // pi re-picks a thinking level on every model switch
      // (core/agent-session.js `_getThinkingLevelForModelSwitch`). Only this
      // check needs that emulation; other checks never set a level at all.
      FAKE_PI_MODEL_RESETS_THINKING: '1',
      PI_WEB_REAP_INTERVAL_MS: '3600000',
    },
    stdio: 'ignore',
    detached: true,
  },
)
const stop = () => {
  try {
    process.kill(-child.pid, 'SIGKILL') // the group we spawned, never a pattern
  } catch {
    /* already gone */
  }
}

const rpcAll = () => {
  let raw
  try {
    raw = fs.readFileSync(RPC_LOG, 'utf8')
  } catch {
    return []
  }
  return raw
    .split('\n')
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l)
      } catch {
        return null
      }
    })
    .filter(Boolean)
}
// Every child logs into ONE file, and the phases below touch one session at a
// time, so "everything since the last mark" is that phase's RPC traffic.
let cursor = 0
const since = () => {
  const all = rpcAll()
  const slice = all.slice(cursor)
  cursor = all.length
  return slice
}
const lv = (slice) => slice.filter((r) => r.type === 'set_thinking_level').map((r) => r.level)
const md = (slice) => slice.filter((r) => r.type === 'set_model').map((r) => r.modelId)

async function post(url, body) {
  const r = await fetch(BASE + url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  return { status: r.status, text: (await r.text()).slice(0, 200) }
}
async function create() {
  const r = await fetch(BASE + '/oc/session', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: '{}',
  })
  return (await r.json()).id
}
async function partsOf(sid) {
  const msgs = await (await fetch(BASE + `/oc/session/${sid}/message`)).json()
  return (msgs ?? []).flatMap((m) => m.parts ?? [])
}
async function turn(sid, body) {
  const res = await post(`/oc/session/${sid}/prompt_async`, body)
  for (let i = 0; i < 100; i++) {
    await sleep(100)
    const st = await (await fetch(BASE + `/oc/session/status?id=${sid}`)).json().catch(() => ({}))
    const rec = st?.[sid] ?? st?.[Object.keys(st ?? {})[0]]
    if (rec?.type === 'idle') break
  }
  return { res, parts: await partsOf(sid) }
}
const reasoning = (parts) => parts.filter((p) => p.type === 'reasoning' && (p.text ?? '').trim()).length
const texts = (parts) => parts.filter((p) => p.type === 'text' && (p.text ?? '').trim()).length
// the webui's prompt body, verbatim from webui/src/lib/api.ts prompt()
const webuiBody = (text, extra) => ({
  parts: [{ type: 'text', text }],
  model: { providerID: 'testprov', modelID: 'fake-1' },
  ...extra,
})

try {
  for (let i = 0; i < 100; i++) {
    try {
      if ((await fetch(BASE + '/health')).ok) break
    } catch {
      /* starting */
    }
    await sleep(100)
  }

  // ---- 1. a pick that reaches a live child (nested variant) -------------
  const s1 = await create()
  since()
  const picked = await post(`/oc/api/session/${s1}/model`, { model: { providerID: 'testprov', id: 'fake-1', variant: 'high' } })
  const a = since()
  check(picked.status === 200, 'the picker POST /api/session/{id}/model is accepted', `status ${picked.status}`)
  check(lv(a).includes('high'), 'and pi is asked for that thinking level', JSON.stringify(lv(a)))

  // ---- 2. the body the webui really posts ------------------------------
  const s2 = await create()
  since()
  const off = await turn(s2, webuiBody('first', { variant: 'off' }))
  const b = since()
  check(md(b).includes('fake-1'), 'the model ref (modelID, not id) reaches pi as set_model', JSON.stringify(md(b)))
  check(lv(b).includes('off'), 'and the top-level variant reaches pi as set_thinking_level', JSON.stringify(lv(b)))
  check(reasoning(off.parts) === 0, 'variant "off" really suppresses the thinking block', `${reasoning(off.parts)} reasoning parts`)
  check(texts(off.parts) > 0, 'while the answer still streams', `${texts(off.parts)} text parts`)

  // ---- 3-4. no RPC storm: same ref + same level cost nothing -----------
  await turn(s2, webuiBody('second', { variant: 'off' }))
  const c = since()
  check(md(c).length === 0, 'a repeated prompt does NOT re-send set_model (pi re-picks the level on every switch)', JSON.stringify(md(c)))
  check(lv(c).length === 0, 'a repeated prompt does NOT re-send set_thinking_level', JSON.stringify(lv(c)))

  // ---- 5. changing the variant mid-session changes the turn ------------
  const hi = await turn(s2, webuiBody('third', { variant: 'high' }))
  const d = since()
  check(lv(d).includes('high'), 'a later variant pick is forwarded', JSON.stringify(lv(d)))
  check(reasoning(hi.parts) > 0, 'and thinking comes back for that turn', `${reasoning(hi.parts)} reasoning parts`)

  // ---- 6. a model change inside a prompt must not lose the level -------
  const s3 = await create()
  since()
  await turn(s3, webuiBody('lock the level', { variant: 'low' }))
  check(lv(since()).includes('low'), 'the first prompt of a brand-new session carries the level too')
  await turn(s3, webuiBody('new model, no variant', { model: { providerID: 'testprov', modelID: 'fake-2' } }))
  const f = since()
  const types = f.map((r) => r.type)
  const lastModel = types.lastIndexOf('set_model')
  const lastLevel = types.lastIndexOf('set_thinking_level')
  check(
    lastModel > -1 && lastLevel > lastModel && f[lastLevel].level === 'low',
    'a model change inside a prompt re-asserts the session level AFTER set_model',
    `set_model@${lastModel} then ${f[lastLevel]?.type}:${f[lastLevel]?.level}`,
  )
  check(reasoning(await partsOf(s3)) > 0, 'so the thinking block is still there')

  // ---- 7. an explicit model switch with no variant = cleared -----------
  await post(`/oc/api/session/${s3}/model`, { model: { providerID: 'testprov', id: 'fake-1' } })
  const g = since()
  check(
    md(g).slice(-1)[0] === 'fake-1' && lv(g).length === 0,
    'the picker switching model with NO variant sends the model and forces no level (pi picks its default)',
    `models ${JSON.stringify(md(g))} levels ${JSON.stringify(lv(g))}`,
  )

  // ---- 8. the ladder the picker shows is the one pi will run under -------
  // models-store.json is a CACHE pi's providers rewrite (the llama provider
  // re-probes /props and overwrites its section, hand-edits included), so the
  // durable user layer is models.json `modelOverrides`. If the bridge publishes
  // the raw store, the webui advertises whatever pi is about to ignore.
  fs.writeFileSync(
    path.join(AGENT, 'models-store.json'),
    JSON.stringify({
      'llama.cpp': {
        models: [
          {
            id: 'gguf-override',
            name: 'gguf-override',
            api: 'openai-completions',
            reasoning: true,
            contextWindow: 256000,
            maxTokens: 256000,
            thinkingLevelMap: { off: 'off', minimal: null, low: null, medium: 'medium', high: null, xhigh: null },
            compat: { supportsReasoningEffort: false, thinkingFormat: 'qwen-chat-template' },
          },
          {
            id: 'gguf-plain',
            name: 'gguf-plain',
            api: 'openai-completions',
            reasoning: true,
            contextWindow: 256000,
            maxTokens: 256000,
            thinkingLevelMap: { off: 'off', minimal: null, low: null, medium: 'medium', high: null, xhigh: null },
            compat: { supportsReasoningEffort: false, thinkingFormat: 'qwen-chat-template' },
          },
        ],
      },
    }),
  )
  fs.writeFileSync(
    path.join(AGENT, 'models.json'),
    JSON.stringify({
      providers: {
        'llama.cpp': {
          modelOverrides: {
            'gguf-override': {
              reasoning: true,
              thinkingLevelMap: { off: 'none', minimal: null, low: 'low', medium: 'medium', high: null, xhigh: 'xhigh' },
              compat: { supportsReasoningEffort: true, thinkingFormat: 'openai' },
            },
          },
        },
      },
    }),
  )
  const provs = await (await fetch(BASE + '/oc/config/providers')).json()
  const modelsOf = (provs.providers ?? []).find((p) => p.id === 'llama.cpp')?.models ?? {}
  const varsOf = (id) => Object.keys(modelsOf[id]?.variants ?? {})
  check(
    JSON.stringify(varsOf('gguf-override')) === JSON.stringify(['off', 'low', 'medium', 'xhigh']),
    'the picker ladder follows models.json modelOverrides (the layer that survives a store rewrite)',
    JSON.stringify(varsOf('gguf-override')),
  )
  check(
    JSON.stringify(varsOf('gguf-plain')) === JSON.stringify(['off', 'medium']),
    'a model with no override keeps the store ladder — the merge invents no levels',
    JSON.stringify(varsOf('gguf-plain')),
  )

  console.log('\nrpc order:', rpcAll().map((r) => r.type + (r.level ? `:${r.level}` : '')).join(' '))
} catch (err) {
  console.error('ERROR', err)
  failed++
} finally {
  stop()
}
console.log(failed ? `\nFAILURES: ${failed}` : '\nALL PASS')
process.exit(failed ? 1 : 0)
