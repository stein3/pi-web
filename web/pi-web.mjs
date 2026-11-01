#!/usr/bin/env node
/**
 * pi-web — an opencode-shaped backend for nopencode's webui, backed by pi.
 *
 *   browser ──> pi-web ──(JSONL RPC over stdio)──> N × `pi --mode rpc`
 *
 * Owns N concurrent pi children (tau's model: one per live tab, server-owned,
 * surviving browser reloads) and re-exposes them under the wire contract the
 * nopencode webui already speaks:
 *
 *   /oc/*            REST + SSE (opencode engine shape)
 *   /api/history/*   read-only view over pi's own JSONL session files
 *   /*               static webui/dist
 *
 * Process management is NOT reimplemented — it is imported from
 * `pi-tau-web-server` (MIT): PiRpcSession + LiveSessionManager handle spawn,
 * JSONL framing, id correlation, timeouts, exit handling and dialog forwarding.
 * tau's own WebSocket protocol and frontend are deliberately unused; a fake
 * WS client is registered on the manager instead, so tau's broadcasts land
 * here and are re-emitted as opencode SSE.
 *
 * Storage is pi's. Nothing here keeps a message store: transcripts are read
 * from PiRpcSession.entries (live) or the JSONL file (cold), through one pure
 * projection `toOcMessages()`. The only live state is
 *   - alias set (which public ids name this session), and
 *   - a dup-count ledger so message ids hash identically on both paths.
 *
 * Message ids are `sha1(role|timestamp|dup)` — derived, never minted, so a
 * refetch and a live event can never disagree (the class of bug opencode's
 * part-id minting + dropBufferedDeltas defends against).
 *
 * The Diff PANE is intentionally not wired (no /api/history/.../changes, no
 * vcs/diff): tau has the better file/diff viewer. The transcript's inline
 * edit-diff block IS wired — pi's tool `details.patch` is mapped to opencode's
 * `state.metadata.filediff.patch` by `toolMetadata()`.
 *
 * Usage:  node pi-web.mjs [--port 8124] [--host 0.0.0.0] [--cwd <workspace>]
 *                         [--dir <agentDir>] [--dist <webui/dist>]
 * Every default is derived from where this file lives (lib/config.mjs resolves
 * them against this file's directory) or comes from the matching PI_* env var.
 */


import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { HOST, PI_AGENT_DIR, PI_CWD, PORT, WEBUI_DIST } from './lib/config.mjs'
import { SESSIONS_DIR, liveManager } from './lib/tau.mjs'
import { ENGINE_PROBE_MS, engineTick } from './lib/engine.mjs'
import { sendJson, serveStatic } from './lib/http.mjs'
import { onTauFrame } from './lib/live-events.mjs'
import { REAP_INTERVAL_MS, reapIdleChildren } from './lib/registry.mjs'
import { handleApi } from './lib/router.mjs'
import { emit } from './lib/sse.mjs'

// ---------------------------------------------------------------------------
// tau bridge — tau thinks it is talking to a WebSocket client.
// ---------------------------------------------------------------------------

liveManager.addClient({
  readyState: 1, // WebSocket.OPEN — the only value broadcast() checks
  send: (payload) => {
    try {
      onTauFrame(JSON.parse(payload))
    } catch (err) {
      console.error('[bridge]', err?.stack ?? err, '\npayload:', String(payload).slice(0, 300))
    }
  },
  on() {},
  close() {},
  terminate() {},
})

// ---------------------------------------------------------------------------
// boot
// ---------------------------------------------------------------------------

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://x')
    if (url.pathname.startsWith('/oc/') || url.pathname.startsWith('/api/') || url.pathname === '/healthz') {
      await handleApi(req, res, url)
      return
    }
    if (url.pathname === '/oc') {
      res.writeHead(302, { location: '/oc/session' })
      res.end()
      return
    }
    serveStatic(req, res, url.pathname)
  } catch (err) {
    try {
      sendJson(res, { error: String(err?.message ?? err) }, 500)
    } catch {
      /* headers already sent */
    }
  }
})

// heartbeat: sse.ts treats >30s of silence on an OPEN stream as dead
// Engine telemetry: one global 1 Hz tick. It issues ZERO requests while nothing
// is busy, and never blocks a request handler (results land in the live entry).
if (ENGINE_PROBE_MS > 0) {
  setInterval(() => {
    engineTick().catch((e) => console.log('[engine] probe failed:', e?.message ?? e))
  }, ENGINE_PROBE_MS)
}

const hb = setInterval(() => emit('heartbeat', {}), 10000)
hb.unref?.()

// idle/overflow pi children — see the reclamation block near `live`
const reaper = setInterval(() => {
  reapIdleChildren().catch((err) => console.error('[reap]', err?.message ?? err))
}, REAP_INTERVAL_MS)
reaper.unref?.()

server.listen(PORT, HOST, () => {
  console.log(`pi-web  http://${HOST}:${PORT}`)
  console.log(`  dist    ${WEBUI_DIST}`)
  console.log(`  pi dir  ${PI_AGENT_DIR}`)
  console.log(`  sessions ${SESSIONS_DIR}`)
  console.log(`  cwd     ${PI_CWD}`)
  if (!fs.existsSync(path.join(WEBUI_DIST, 'index.html'))) {
    console.warn(
      `WARNING: ${WEBUI_DIST}/index.html missing — API works but the UI will 404.\n` +
        `  Docker: the /app/webui mount needs a built dist; on the host run:\n` +
        `    cd ../opencode/webui && npm ci && npm run build\n` +
        `  Bare metal: pass --dist /path/to/opencode/webui/dist`,
    )
  }
})

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    clearInterval(hb)
    clearInterval(reaper)
    server.close()
    try {
      await liveManager.shutdown()
    } catch {
      /* best effort */
    }
    process.exit(0)
  })
}
