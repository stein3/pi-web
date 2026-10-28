/**
 * Run the REAL nopencode webui client — its actual `sse.ts` event handler,
 * `stores.ts` tab store and `api.ts` — headless in node, pointed at a running
 * pi-web. No browser, no chromium (this container has none), and no *copy* of
 * the client's state machine: the checks assert on what the shipped code
 * computes, which is the only way a UI bug ("the webui doesn't show X") can be
 * adjudicated here at all.
 *
 * What the shims provide, and why:
 *   - resolve hook      the webui's sources import each other extensionless
 *                       (bundler resolution); node needs the .ts suffix.
 *   - EventSource       sse.ts opens one; we implement it over node:http and
 *                       feed `onmessage` per SSE frame, like a browser.
 *   - fetch             api.ts + sse.ts refetches go through it.
 *   - document/window/… only what the imported modules touch at module scope
 *                       or in the paths these checks exercise.
 *
 * Everything outside this repo is env-overridable and reports as a SKIP
 * (exit 2), not a product failure — same convention as harness.mjs's loadE2E.
 */
import fs from 'node:fs'
import http from 'node:http'
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { REPO } from './harness.mjs'

export const WEBUI_SRC = process.env.WEBUI_SRC || path.join(REPO, '..', 'opencode', 'webui', 'src')

export function webuiSkip(e) {
  const m = String(e?.message ?? e)
  if (/webui sources not found/i.test(m)) return `${m}\n  set WEBUI_SRC=<path to nopencode webui/src>`
  if (/SyntaxError|ERR_MODULE_NOT_FOUND|ERR_UNKNOWN_FILE_EXTENSION|ERR_UNSUPPORTED/i.test(m)) {
    return `SKIP: could not load the webui client under this node: ${m.split('\n')[0]}\n  needs node >= 22.18 (type stripping of .ts)`
  }
  return null
}

function installShims(BASE) {
  const mem = new Map()
  globalThis.localStorage = {
    getItem: (k) => (mem.has(k) ? mem.get(k) : null),
    setItem: (k, v) => mem.set(k, String(v)),
    removeItem: (k) => mem.delete(k),
  }
  // Only the surface the imported modules actually touch. Anything deeper is a
  // bug in the check (a code path that needs a DOM), not something to fake.
  const el = () => ({ style: {}, setAttribute() {}, appendChild() {}, addEventListener() {} })
  globalThis.document = {
    addEventListener() {},
    removeEventListener() {},
    visibilityState: 'visible',
    documentElement: { style: {}, setAttribute() {}, classList: { add() {}, remove() {} } },
    body: el(),
    head: el(),
    createElement: el,
    querySelector: () => null,
    querySelectorAll: () => [],
    getElementById: () => null,
  }
  globalThis.window = {
    addEventListener() {},
    removeEventListener() {},
    localStorage: globalThis.localStorage,
    matchMedia: () => ({ matches: false, addEventListener() {}, addListener() {} }),
    location: { href: `${BASE}/`, origin: BASE },
    navigator: { userAgent: 'node', clipboard: { writeText: async () => {} } },
  }
  for (const k of ['navigator', 'location']) {
    try {
      Object.defineProperty(globalThis, k, { value: globalThis.window[k], configurable: true, writable: true })
    } catch {
      /* node exposes a read-only one; the modules only read it */
    }
  }

  globalThis.fetch = (url, opts) => {
    const u = new URL(url, BASE)
    return new Promise((resolve, reject) => {
      const r = http.request(
        u,
        { method: opts?.method || 'GET', headers: opts?.headers || {} },
        (res) => {
          let b = ''
          res.on('data', (c) => (b += c))
          res.on('end', () =>
            resolve({
              ok: (res.statusCode ?? 0) < 400,
              status: res.statusCode,
              json: async () => JSON.parse(b || 'null'),
              text: async () => b,
            }),
          )
        },
      )
      r.on('error', reject)
      if (opts?.body) r.write(opts.body)
      r.end()
    })
  }

  class NodeEventSource {
    static CONNECTING = 0
    static OPEN = 1
    static CLOSED = 2
    constructor(url) {
      this.readyState = 1
      const u = new URL(url, BASE)
      const req = http.get(u, (res) => {
        // A browser fires `onopen` when the stream is established, and sse.ts
        // leans on it: `if (esEverConnected) void catchUp()` — the ONE path that
        // reconciles busy/compacting against GET /session/status. A shim that
        // never fires it cannot see (or fix) a bug in that reconcile.
        this.readyState = 1
        this.onopen?.()
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
              this.onmessage?.({ data: d.slice(6) })
            } catch (e) {
              throw new Error(`webui sse handler threw on ${d.slice(6, 80)}: ${e.message}`)
            }
          }
        })
        const dropped = () => {
          if (this.readyState === 2) return
          this.readyState = 2
          this.onerror?.()
        }
        res.on('end', dropped)
        res.on('close', dropped)
      })
      req.on('error', () => {
        if (this.readyState !== 2) {
          this.readyState = 2
          this.onerror?.()
        }
      })
      this._req = req
    }
    close() {
      this.readyState = 2
    }
    // Test hook: a mid-turn connection hiccup, exactly as the browser would
    // deliver it (socket dies -> onerror -> sse.ts recreates with backoff ->
    // onopen -> catchUp). Needed to exercise the reconcile on a schedule.
    drop() {
      this._req?.destroy()
      if (this.readyState !== 2) {
        this.readyState = 2
        this.onerror?.()
      }
    }
  }
  NodeEventSource.CONNECTING = 0
  NodeEventSource.OPEN = 1
  NodeEventSource.CLOSED = 2
  globalThis.EventSource = NodeEventSource
  // The most recent stream, so a check can drop it (see dropStream()).
  const orig = NodeEventSource
  globalThis.__lastSse = null
  globalThis.EventSource = function (url) {
    const s = new orig(url)
    globalThis.__lastSse = s
    return s
  }
  globalThis.EventSource.CONNECTING = 0
  globalThis.EventSource.OPEN = 1
  globalThis.EventSource.CLOSED = 2
}

/** Drop the browser's live SSE connection (simulated network hiccup). */
export function dropStream() {
  const s = globalThis.__lastSse
  if (!s) throw new Error('no EventSource open — call sse.startEvents() first')
  s.drop()
  return true
}

/**
 * Import the webui client. Returns { sse, tabs, oc, util }.
 * `base` is the pi-web origin to talk to.
 */
export async function loadWebuiClient(BASE) {
  for (const f of ['lib/sse.ts', 'lib/stores.ts', 'lib/api.ts']) {
    if (!fs.existsSync(path.join(WEBUI_SRC, f))) {
      const err = new Error(
        `webui sources not found: ${path.join(WEBUI_SRC, f)}`,
      )
      err.skip = true
      throw err
    }
  }
  // resolve hook must be installed before the webui modules import each other
  const { registerHooks } = await import('node:module')
  const { fileURLToPath } = await import('node:url')
  const tryFiles = (base) => {
    for (const c of [`${base}.ts`, `${base}.js`, path.join(base, 'index.ts'), path.join(base, 'index.js')])
      if (fs.existsSync(c)) return c
    return null
  }
  registerHooks({
    resolve(spec, ctx, next) {
      if (spec.startsWith('.') && ctx.parentURL?.startsWith('file:')) {
        const abs = path.resolve(path.dirname(fileURLToPath(ctx.parentURL)), spec)
        const hit = tryFiles(abs)
        if (hit) return { url: pathToFileURL(hit).href, shortCircuit: true }
      }
      return next(spec, ctx)
    },
  })
  installShims(BASE)
  const sse = await import(pathToFileURL(path.join(WEBUI_SRC, 'lib/sse.ts')).href)
  const { tabs } = await import(pathToFileURL(path.join(WEBUI_SRC, 'lib/stores.ts')).href)
  const { oc, hist } = await import(pathToFileURL(path.join(WEBUI_SRC, 'lib/api.ts')).href)
  const util = await import(pathToFileURL(path.join(WEBUI_SRC, 'lib/util.ts')).href)
  return { sse, tabs, oc, hist, util }
}

/**
 * Transcript.svelte's own message filter + the two predicates the UI renders
 * from. Copied verbatim (they are the contract, and they live in components we
 * cannot import headless); a check that asserted something else would be
 * asserting a UI that does not exist.
 */
export const ui = {
  /** Transcript.svelte `$: msgs = …filter(...)` */
  renders(m) {
    return (
      !!m.error ||
      (m.parts ?? []).some((p) => ['text', 'tool', 'reasoning', 'file'].includes(p.type))
    )
  },
  /** Transcript.svelte `{:else if p.type === 'reasoning' && (p.text ?? '').trim()}` */
  thinkingBlock(p) {
    return p.type === 'reasoning' && !!(p.text ?? '').trim()
  },
  /** ComposerToolbar.svelte `$: lastHasVisible` / `$: liveThinking` */
  lastHasVisible(msgs) {
    const lastMsg = msgs.at(-1)
    return !!(lastMsg?.parts ?? []).some(
      (p) => (p.type === 'text' && (p.text ?? '').trim()) || p.type === 'tool' || p.type === 'file',
    )
  },
  liveThinking(tab, msgs) {
    const lastMsg = msgs.at(-1)
    return (
      tab.busy &&
      !tab.compacting &&
      (!lastMsg || lastMsg.role !== 'assistant' || !ui.lastHasVisible(msgs))
    )
  },
  /** Transcript.svelte `$: liveReasoningId` (the block that auto-expands) */
  liveReasoningId(tab, msgs) {
    return tab.busy ? (msgs.at(-1)?.parts ?? []).filter((p) => p.type === 'reasoning').at(-1)?.id : undefined
  },
}
