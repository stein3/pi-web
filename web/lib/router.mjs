// pi-web/lib/router.mjs — the opencode-shaped API surface (handleApi + fork).
import fs from 'node:fs'
import path from 'node:path'
import { COMPACT_RPC_MS, COMPACT_SEND_WAIT_MS, SUMMARIZE_WAIT_MS, compactingWait, transcriptEntries, waitForCompactionEnd } from './compaction.mjs'
import { PI_CWD } from './config.mjs'
import { forkCount, forkEntriesFor, forkTranscript } from './fork.mjs'
import { FS_ROUTES, FsError } from './fsbrowse.mjs'
import { readBody, sendJson } from './http.mjs'
import { modelProviders } from './models.mjs'
import { emitSessionUpdated } from './notify.mjs'
import { allSessionMeta, appendSessionInfo, errTiles, loadMeta, metaCache, ocSession, saveMeta, searchSessions, sessionMeta, toHistMsgs, trashSessionFile, workspaceChips } from './projection.mjs'
import { armSession, entryFor, learnPublicId, live, publicIdsFor, queueView, reapedFile, sessionByAnyId } from './registry.mjs'
import { entriesForId, fileForId, sessionFor } from './sessions.mjs'
import { sseClients } from './sse.mjs'
import { completeDirectories, liveManager, navigateTree } from './tau.mjs'
import { unhandled } from './telemetry.mjs'
import { entryIdInChain, filesCache, invalidateFilesCache, messageEntryId, readHeaderCwd } from './transcript.mjs'
import { toOcMessages } from './translate.mjs'
import { apiErr, expandHome, fileToImage, liveCwd } from './util.mjs'

// --- stubs for opencode-only surfaces --------------------------------------
const EMPTY = {
  '/oc/permission': [],
  '/oc/question': [],
  '/oc/agent': [],
  '/oc/skill': [],
  '/oc/mcp': {},
  '/oc/config': {},
}

// pi's own slash commands (the ones loaded extensions register, e.g.
// an extension's /model-sync) are only visible to pi, so the palette asked the
// engine and got [] forever. Query a live pi child over RPC (`get_commands`)
// instead; pi built-ins stay out because the webui ships web-side twins for
// those, and both exclusions need UI this server never bridges:
// tau's navigate command is internal, and ctx.ui.select/confirm dialogs (used
// by /pick-model) have no client here — the RPC dialog just expires.
const HIDDEN_COMMANDS = new Set(['tau-tree-navigate', 'pick-model'])
const commandCache = { at: 0, ttl: 0, data: [] }
async function engineCommands() {
  if (commandCache.data.length && Date.now() - commandCache.at < commandCache.ttl) return commandCache.data
  const found = new Map()
  await Promise.all(
    [...liveManager.sessions.values()].map(async (s) => {
      try {
        const resp = await s.send({ type: 'get_commands' }, { timeoutMs: 8000 })
        for (const c of resp?.data?.commands ?? []) {
          if (!c?.name || c.source !== 'extension' || HIDDEN_COMMANDS.has(c.name)) continue
          if (!found.has(c.name)) found.set(c.name, { name: c.name, description: c.description ?? '' })
        }
      } catch {
        /* child closing or busy */
      }
    }),
  )
  commandCache.at = Date.now()
  commandCache.data = [...found.values()]
  // empty (no live child yet) re-checks quickly; a real list is good for 5 min
  commandCache.ttl = commandCache.data.length ? 5 * 60_000 : 10_000
  return commandCache.data
}
const NOT_WIRED = new Set([])
// POST sub-routes that must resume a cold/reaped session instead of 404ing
// (they run inside a pi child). `/abort` deliberately does NOT: waking a child
// only to abort it would resurrect a tab nobody asked for.
const RESUME_ON_POST = new Set(['/prompt_async', '/command', '/queue/clear', '/summarize', '/revert'])
// `/fork` is deliberately NOT in that set: it copies a transcript, and the file IS
// the transcript. Resuming a child first only costs a spawn and hands back a tree
// holding just what that child appended since it resumed (see forkEntriesFor).
// opencode surfaces that degrade cleanly to "nothing here"
const EMPTY_VCS = { '/oc/vcs/diff': [], '/oc/vcs/diff/raw': '' }
async function handleApi(req, res, url) {
  const { pathname } = url
  const q = url.searchParams

  // ---- static-ish stubs ---------------------------------------------------
  if (pathname in EMPTY) return sendJson(res, EMPTY[pathname])
  if (pathname === '/oc/command') return sendJson(res, await engineCommands())
  if (pathname === '/oc/path') return sendJson(res, { directory: PI_CWD })
  // workspace picker: chip list + tau-style directory type-ahead
  if (pathname === '/oc/workspaces') {
    return sendJson(res, { root: PI_CWD, chips: workspaceChips() })
  }
  if (pathname === '/oc/fs/dirs') {
    return sendJson(res, { items: completeDirectories(q.get('prefix') || '', PI_CWD) })
  }
  if (pathname === '/oc/config/providers') return sendJson(res, { providers: await modelProviders() })
  // ---- file browser (read-only; ceiling-bounded; see the block above) -----
  const fsm = /^\/oc\/fs\/(list|file|find|status|diff)$/.exec(pathname)
  if (fsm) {
    try {
      return sendJson(res, await FS_ROUTES[fsm[1]](q))
    } catch (err) {
      if (err instanceof FsError) return sendJson(res, { error: err.message }, err.status)
      return sendJson(res, { error: String(err?.message ?? err) }, 500)
    }
  }
  if (pathname.startsWith('/oc/file/content')) {
    const p = q.get('path') || ''
    try {
      const buf = fs.readFileSync(p)
      const isText = !buf.includes(0)
      return sendJson(
        res,
        isText
          ? { type: 'text', content: buf.toString('utf8') }
          : { type: 'binary', content: buf.toString('base64') },
      )
    } catch {
      return sendJson(res, { type: 'text', content: '' })
    }
  }

  // ---- SSE ---------------------------------------------------------------
  if (pathname === '/oc/event') {
    req.socket.setTimeout(0)
    req.socket.setNoDelay(true)
    req.socket.setKeepAlive(true)
    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    })
    res.write(': ok\n\n')
    sseClients.add(res)
    req.on('close', () => sseClients.delete(res))
    return
  }

  // ---- session status -----------------------------------------------------
  if (pathname === '/oc/session/status') {
    // Authoritative for the webui: its busy-reconcile watchdog overwrites the
    // local busy/compacting flags with what it reads HERE (sse.ts catchUp), so
    // a summary in flight must say busy or the "Compacting session…" strip dies
    // mid-summary. See entryFor().compacting.
    const out = {}
    for (const s of liveManager.sessions.values()) {
      const busy = !!s.isStreaming || live.get(s.id)?.compacting === true
      const ids = publicIdsFor(s.id)
      const rec = live.get(s.id)?.compaction ?? null
      const eng = live.get(s.id)?.engine ?? null
      for (const id of ids) {
        out[id] = {
          type: busy ? 'busy' : 'idle',
          ...(rec ? { compaction: rec } : {}),
          ...(eng ? { engine: eng } : {}),
        }
      }
    }
    return sendJson(res, out)
  }
  if (pathname === '/oc/session/todo') return sendJson(res, [])
  if (pathname in EMPTY_VCS) {
    const v = EMPTY_VCS[pathname]
    if (typeof v === 'string') {
      res.writeHead(200, { 'content-type': 'text/plain' })
      res.end(v)
      return
    }
    return sendJson(res, v)
  }
  if (NOT_WIRED.has(pathname)) return sendJson(res, { error: 'not wired in pi-web (mockup)' }, 501)

  // engine-model / agent switch (webui posts to /oc/api/session/{id}/…)
  const am = pathname.match(/^\/oc\/api\/session\/([^/]+)\/(model|agent)$/)
  if (am) {
    const body = await readBody(req)
    const s = sessionByAnyId(decodeURIComponent(am[1]))
    const ref = modelRefOf(body)
    if (s && am[2] === 'model' && ref.modelId) {
      await s
        .send({ type: 'set_model', provider: ref.provider, modelId: ref.modelId }, { timeoutMs: 10000 })
        .catch(() => {})
      const ent = entryFor(s.id)
      ent.modelRef = `${ref.provider}\u0000${ref.modelId}`
      ent.level = null // the model switch re-picks a level; the pick goes below
      // This POST is the user's own switch, and webui clearVariant() posts the
      // model with NO variant — so an absent variant here means "cleared" and
      // pi's freshly re-picked level is the answer. Do not re-assert the old one.
      if (ref.variant) {
        const r = await s.send({ type: 'set_thinking_level', level: ref.variant }, { timeoutMs: 5000 }).catch(() => null)
        if (r?.success === false)
          console.log(`[thinking] ${s.id}: pi rejected level "${ref.variant}" (${r.error ?? 'unknown'}) — using the session's level`)
        else ent.level = ref.variant
      }
      emitSessionUpdated(s.id, s, undefined, true)
    }
    return sendJson(res, ocSession(s) ?? { ok: true })
  }

  // dialogs / mcp / config toggles: no-op success so the UI never sticks
  if (/^\/oc\/(permission|question)\/[^/]+\/(reply|reject)$/.test(pathname)) {
    await readBody(req)
    return sendJson(res, { ok: true })
  }
  if (/^\/oc\/mcp\/[^/]+\/(connect|disconnect)$/.test(pathname)) return sendJson(res, { ok: true })
  if (pathname === '/oc/config' && req.method === 'PATCH') return sendJson(res, await readBody(req))

  // ---- session list -------------------------------------------------------
  if (pathname === '/oc/session' && req.method === 'GET') {
    return sendJson(
      res,
      [...liveManager.sessions.values()].map((s) => ocSession(s)),
    )
  }
  if (pathname === '/oc/session' && req.method === 'POST') {
    const body = await readBody(req)
    const cwd = expandHome(typeof body?.cwd === 'string' ? body.cwd.trim() : '') || PI_CWD
    let st = null
    try {
      st = fs.statSync(cwd)
    } catch {
      /* missing */
    }
    if (!st?.isDirectory()) return sendJson(res, { error: `not a directory: ${cwd}` }, 400)
    const s = await liveManager.create({ cwd, model: modelSpec(body?.model) })
    const ent = entryFor(s.id)
    const ref = modelRefOf(body)
    if (ref.modelId) ent.modelRef = `${ref.provider}\u0000${ref.modelId}`
    // tau picked the model at spawn, and pi re-picked a thinking level with it,
    // so a variant that came with the create has to go AFTER it.
    if (ref.variant) {
      const r = await s.send({ type: 'set_thinking_level', level: ref.variant }, { timeoutMs: 5000 }).catch(() => null)
      if (r?.success === false) console.log(`[thinking] create: pi rejected level "${ref.variant}" (${r.error ?? 'unknown'})`)
      else ent.level = ref.variant
    }
    armSession(s)
    // answer with pi's id, not the per-child handle: see learnPublicId
    await learnPublicId(s)
    filesCache.list = null
    return sendJson(res, ocSession(s), 201)
  }

  // ---- one session --------------------------------------------------------
  const m = pathname.match(/^\/oc\/session\/([^/]+)(\/.*)?$/)
  if (m) {
    const raw = decodeURIComponent(m[1])
    const rest = m[2] ?? ''

    if (rest === '') {
      if (req.method === 'DELETE') {
        const s = sessionByAnyId(raw)
        // Resolve the JSONL BEFORE closing the child. Live sidebar rows carry a
        // tau id, which appears nowhere in the file name, so s.sessionFile is
        // the only link between them — and that link dies with the child.
        const file = fileForId(raw)
        const ids = s ? [...publicIdsFor(s.id), raw] : [raw]
        // Close first (best effort): pi appends to its JSONL per write, so a
        // child still running could re-create the path we are about to move.
        if (s) {
          try {
            await liveManager.delete(s.id, 'closed_by_user')
          } catch (err) {
            console.error('[delete] close failed:', err?.message ?? err)
          }
        }
        invalidateFilesCache()
        // a trashed session must not stay resumable through the reap map
        if (file) metaCache.delete(file)
        for (const id of ids) reapedFile.delete(id)
        if (!s && !file) return sendJson(res, { error: 'unknown session' }, 404)
        if (file) {
          const err = trashSessionFile(file)
          if (err) return sendJson(res, { error: `failed to trash: ${err}` }, 500)
        }
        // star/tag sidecar + error tiles must not outlive the session
        const all = loadMeta()
        for (const id of ids) {
          delete all[id]
          errTiles.delete(id)
        }
        saveMeta(all)
        return sendJson(res, { ok: true, trashed: !!file })
      }
      if (req.method === 'PATCH') {
        const body = await readBody(req)
        const name = typeof body.title === 'string' ? body.title.trim() : ''
        const s = sessionByAnyId(raw)
        if (name && s) {
          const e = entryFor(s.id)
          e.title = name
          e.named = true // the auto-tiler must not undo a rename the operator made
          // Ask pi to rename its own session: it persists the session_info entry
          // AND emits session_info_changed, so pi, the TUI and every other
          // client learn the name. Appending to the file behind the child did
          // neither — the child kept its old name, and could rewrite it away.
          let renamed = false
          try {
            const r = await s.send({ type: 'set_session_name', name }, { timeoutMs: 8000 })
            renamed = r?.success !== false
          } catch {
            /* child not answering */
          }
          if (!renamed) appendSessionInfo(s.sessionFile || fileForId(raw), name)
          emitSessionUpdated(s.id, s, raw, true)
        } else if (name) {
          // No live child: the file is the only thing to write. Doing nothing
          // here meant renaming a closed session answered ok and then silently
          // reverted to the prompt on the next sidebar poll.
          const file = fileForId(raw)
          if (!file) return sendJson(res, { error: 'unknown session' }, 404)
          if (!appendSessionInfo(file, name)) return sendJson(res, apiErr(`failed to write ${file}`), 500)
          filesCache.list = null
        }
        return sendJson(res, { ok: true })
      }
      // any other method (DELETE/PATCH above) handled — GETs fall through so
      // the shared path below resumes cold sessions for every sub-route
    }

    // GET /oc/session/{id}[/sub]: resume a cold session from the JSONL first.
    // openLive fires /message and the info fetch in the same Promise.all, so
    // waiting for the info fetch to do the resume made the boot path 404 its
    // own message/todo reads (sidebar rows only ever GET, never write).
    // /summarize resumes too: compaction has to run inside a pi child, so a
    // session the reaper closed (or a sidebar row never opened) still has to
    // answer /compact instead of 404ing.
    //
    // Same reasoning for the other POSTs that need a child: a tab whose child
    // the reaper closed must be able to send a prompt without the browser first
    // GETing the session. Resume is the existing cold path, and `raw` stays the
    // address events come back on.
    //
    // `/fork` opts out of that: it copies a transcript rather than talks to a
    // model, so it must not pay a pi spawn, and `s` is only the child already
    // attached to this id, if any (see handleFork / forkEntriesFor).
    if (rest === '/fork' && req.method === 'POST') return handleFork(req, res, raw, sessionByAnyId(raw))
    const s =
      req.method === 'GET' || RESUME_ON_POST.has(rest)
        ? await sessionFor(raw)
        : sessionByAnyId(raw)
    if (!s) return sendJson(res, { error: 'unknown session' }, 404)
    if (rest === '' && req.method === 'GET') return sendJson(res, ocSession(s, raw))

    if (rest === '/message') {
      const entries = s ? transcriptEntries(s, live.get(s.id)) : null
      let msgs = entries ? toOcMessages(entries) : []
      const limit = Number(q.get('limit') || 0)
      if (limit > 0 && msgs.length > limit) msgs = msgs.slice(-limit)
      return sendJson(res, msgs)
    }
    // pi's pending queues. Doubles as the capability probe: a backend that
    // answers these two arrays supports the strip, one that 404s keeps the
    // old "queued shows up as a transcript row" behaviour.
    if (rest === '/queue' && req.method === 'GET') return sendJson(res, queueView(s.id))
    if (rest === '/queue/clear' && req.method === 'POST') {
      // pi has no targeted dequeue — clearing is all-or-nothing by design.
      try {
        const r = await s.send({ type: 'clear_queue' }, { timeoutMs: 10000 })
        if (r?.success === false) return sendJson(res, apiErr(r.error ?? 'clear_queue rejected'), 502)
        // queue_update lands on its own; answer with what was dropped.
        return sendJson(res, { ok: true, ...(r?.data ?? {}) })
      } catch (err) {
        return sendJson(res, apiErr(String(err?.message ?? err)), 504)
      }
    }
    if (rest === '/prompt_async' && req.method === 'POST') {
      const body = await readBody(req)
      const parts = Array.isArray(body.parts) ? body.parts : []
      const text = parts
        .filter((p) => p?.type === 'text')
        .map((p) => p.text ?? '')
        .join('\n')
        .trim()
      const images = parts.map(fileToImage).filter(Boolean)
      const refused = parts.filter((p) => p?.type === 'file' && !fileToImage(p))
      // "steer" lands at the next turn boundary, "follow" after the run settles.
      // The client states intent; pi picks the delivery, and only pi knows
      // whether a run is live — a cached busy flag never does.
      const delivery = body.delivery === 'steer' ? 'steer' : 'follow'
      if (refused.length)
        return sendJson(
          res,
          apiErr(`pi-web carries image attachments only; refused: ${refused.map((p) => p.filename ?? 'file').join(', ')}`),
          415,
        )
      if (!text && !images.length) return sendJson(res, apiErr('empty prompt'), 400)
      const streaming = !!s.isStreaming
      const ent = entryFor(s.id) // per-bridge session state (model ref + thinking level live here)
      // The webui re-posts its model pick on EVERY prompt (Composer.svelte), so
      // apply it only when the ref actually differs from what THIS child last
      // got: pi re-picks a thinking level on every set_model, so a per-prompt
      // set_model would undo the variant pick and cost a round trip for nothing.
      const ref = modelRefOf(body)
      const want = ref.modelId ? `${ref.provider}\u0000${ref.modelId}` : ''
      const prevLevel = ent.level
      let switched = false
      // A steer never changes the model: the turn it lands inside is already
      // running on whatever model produced it.
      if (want && want !== ent.modelRef && !(streaming && delivery === 'steer')) {
        await s
          .send({ type: 'set_model', provider: ref.provider, modelId: ref.modelId }, { timeoutMs: 10000 })
          .catch(() => {})
        ent.modelRef = want
        ent.level = null
        switched = true
      }
      // webui variant -> pi thinking level. Only when it changed: this rides on
      // every prompt, and an RPC round trip per prompt is pure latency. A body
      // with NO variant is not a clear (webui clearVariant posts the model with
      // no variant to /api/session/{id}/model), so when the bridge itself just
      // switched the model it has to put the session's level back afterwards.
      const level = ref.variant ?? (switched ? prevLevel : null)
      if (level && level !== ent.level) {
        const r = await s.send({ type: 'set_thinking_level', level }, { timeoutMs: 5000 }).catch(() => null)
        if (r?.success === false) {
          console.log(`[thinking] ${s.id}: pi rejected level "${level}" (${r.error ?? 'unknown'}) — using the session's level`)
        } else {
          ent.level = level
        }
      }
      const cmd = { type: 'prompt', message: text }
      if (images.length) cmd.images = images
      if (streaming) cmd.streamingBehavior = delivery === 'steer' ? 'steer' : 'followUp'
      // An idle prompt acks in milliseconds. A "/name" prompt runs the
      // extension handler synchronously before its preflight ack, so that
      // duration is unbounded by construction and gets a generous ceiling.
      // A timeout is an UNKNOWN outcome (504), never a failure: pi defers
      // prompts issued inside the settle window and acks them later, so
      // treating it as a failure invites a duplicate execution.
      try {
        const ceiling = text.startsWith('/') ? 300000 : streaming ? 15000 : 30000
        let resp = await s.send(cmd, { timeoutMs: ceiling })
        if (resp?.success === false && /compaction is in progress/i.test(String(resp.error ?? ''))) {
          console.log(`[send] ${s.id}: pi is compacting — holding the prompt (${COMPACT_SEND_WAIT_MS}ms) until the summary lands`)
          const settled = await waitForCompactionEnd(s.id, COMPACT_SEND_WAIT_MS)
          if (!settled)
            return sendJson(
              res,
              { name: 'SessionBusyError', error: String(resp.error ?? 'compaction in progress') },
              409,
            )
          // another tab may have started a turn while we waited: then this is a
          // queue request, exactly as the client asked for it
          if (s.isStreaming) cmd.streamingBehavior = delivery === 'steer' ? 'steer' : 'followUp'
          resp = await s.send(cmd, { timeoutMs: ceiling })
        }
        if (resp?.success === false) return sendJson(res, apiErr(resp.error ?? 'prompt rejected'), 502)
      } catch (err) {
        const message = String(err?.message ?? err)
        if (/timed out/i.test(message)) return sendJson(res, { name: 'Timeout', data: { message, unknown: true } }, 504)
        return sendJson(res, { name: 'Error', data: { message } }, 500)
      }
      res.writeHead(204)
      res.end()
      return
    }
    if (rest === '/abort' && req.method === 'POST') {
      // fire-and-forget: pi ACKs async; don't hold the HTTP request on it
      s.abort({}, { timeoutMs: 5000 }).catch(() => {})
      return sendJson(res, { ok: true })
    }
    if (rest === '/todo') return sendJson(res, [])
    if (/^\/message\/[^/]+$/.test(rest) && req.method === 'DELETE') return sendJson(res, { ok: true })
    if (rest === '/share') {
      // pi has no share concept; answer so the sidebar button doesn't stick
      return req.method === 'DELETE' ? sendJson(res, { ok: true }) : sendJson(res, { error: 'not wired in pi-web (mockup)' }, 501)
    }
    if (rest === '/command' && req.method === 'POST') {
      // pi runs an extension command by matching a leading "/name" on a prompt
      // (tau's own session-tree navigation does the same), so that is the wire
      // format for palette picks. Output goes out as RPC `notify` events, which
      // this server has no client for — the effect (a refreshed catalogue)
      // shows up when the picker refetches.
      const body = await readBody(req)
      const args = Array.isArray(body.arguments) ? body.arguments.filter(Boolean).join(' ') : ''
      const cmd = { type: 'prompt', message: `/${body.command}${args ? ` ${args}` : ''}` }
      if (s.isStreaming) cmd.streamingBehavior = 'followUp'
      try {
        // Extension commands execute before the ack and may drive an LLM, so no
        // number here is safe: surface a timeout as unknown (504) rather than
        // as a failure the webui would bounce back into the composer.
        await s.send(cmd, { timeoutMs: 300000 })
      } catch (err) {
        const message = String(err?.message ?? err)
        if (/timed out/i.test(message)) return sendJson(res, { name: 'Timeout', data: { message, unknown: true } }, 504)
        return sendJson(res, { name: 'Error', data: { message } }, 500)
      }
      return sendJson(res, { ok: true })
    }
    if (rest === '/summarize' && req.method === 'POST') {
      // opencode's summarize == pi's RPC `compact`: one summarisation call on
      // the SESSION's own model, then a CompactionEntry is appended (pi keeps
      // the raw transcript, so the webui transcript does NOT shrink — only the
      // model context does, from firstKeptEntryId on).
      //
      // The body's {providerID,modelID} is deliberately ignored: pi has no
      // model override on `compact`, and quietly set_model-ing to the picker's
      // model would change the model of the user's NEXT turn, which /compact
      // never asked for. The webui sends the SESSION's model anyway, and
      // prompts already keep pi's session model in sync. `customInstructions`
      // (pi's `/compact <instructions>` arg) is honoured for API clients.
      const body = await readBody(req)
      const instructions =
        typeof body.customInstructions === 'string' ? body.customInstructions.trim() : ''
      const cmd = { type: 'compact', ...(instructions ? { customInstructions: instructions } : {}) }
      const wait = (compactingWait.get(s.id) ?? 0) + 1
      compactingWait.set(s.id, wait)
      const release = () => {
        const n = (compactingWait.get(s.id) ?? 1) - 1
        if (n > 0) compactingWait.set(s.id, n)
        else compactingWait.delete(s.id)
      }
      const rpc = s.send(cmd, { timeoutMs: COMPACT_RPC_MS })
      // pi always answers eventually; after we replied "started" nothing else
      // holds this promise, so it must not become an unhandled rejection.
      rpc.catch(() => {})
      const raced = await Promise.race([
        rpc.then(
          (r) => ({ how: 'rpc', r }),
          (err) => ({ how: 'rpc', err }),
        ),
        new Promise((done) => setTimeout(() => done({ how: 'ceiling' }), SUMMARIZE_WAIT_MS)),
      ])
      if (raced.how === 'ceiling') {
        // Hand the outcome over to SSE (compactingWait goes -> the bridge
        // reports a failure), and answer as a SUCCESS: the compaction is still
        // running, and a failure toast here invites a second /compact that
        // double-summarises the session.
        release()
        console.log(`[compact] ${s.id}: still summarising after ${SUMMARIZE_WAIT_MS}ms — answering, SSE will finish it`)
        return sendJson(res, { ok: true, started: true, pending: true })
      }
      release()
      // ONE sentence per failure: pi's `compaction_end` message is the canonical
      // string and is recorded on the live entry the moment it lands, so the
      // HTTP body, the SSE tile and GET /session/status quote the same text.
      // tau's own error carries the SAME failure unprefixed — printing both is
      // how a toast and a box end up disagreeing about one event.
      const recorded = () => live.get(s.id)?.compaction?.reason ?? null
      if (raced.err) {
        const message = String(raced.err?.message ?? raced.err)
        // tau gave up on the child, not pi giving up on the summary: the
        // compaction may still land, so say so instead of "failed".
        if (/timed out/i.test(message))
          return sendJson(res, apiErr(`${message} — compaction may still land; check the session before retrying`), 504)
        return sendJson(res, apiErr(recorded() ?? message), 500)
      }
      const r = raced.r
      if (r?.success === false) {
        // pi's own reasons are the useful ones: "Already compacted",
        // "Nothing to compact (session too small)", "No model selected". These
        // are REJECTS — pi never started a summary, so no compaction_start/end
        // fired and nothing else would record why the box went quiet.
        const reason = recorded() ?? String(r.error ?? 'compact rejected')
        const ent = live.get(s.id)
        // Only a REJECT that never started a summary gets this state. pi's
        // compact() also answers success:false when the summary itself threw —
        // compaction_end already recorded 'failed' then, and clobbering it with
        // 'rejected' would lose the fact that a summary was really attempted.
        if (ent && !ent.compaction) ent.compaction = { state: 'rejected', reason, at: Date.now() }
        return sendJson(res, apiErr(reason), 502)
      }
      const d = r?.data ?? {}
      console.log(
        `[compact] ${s.id}: ${d.tokensBefore ?? '?'} -> ${d.estimatedTokensAfter ?? '?'} tokens (kept ${d.firstKeptEntryId ?? '?'})`,
      )
      return sendJson(res, {
        ok: true,
        summary: d.summary ?? '',
        firstKeptEntryId: d.firstKeptEntryId,
        tokensBefore: d.tokensBefore,
        estimatedTokensAfter: d.estimatedTokensAfter,
        ...(d.usage ? { usage: d.usage } : {}),
      })
    }
    if (rest === '/revert' && req.method === 'POST') {
      // Revert = move pi's session leaf, not delete anything. pi's RPC has no
      // leaf-move command, so tau's navigateTree drives its bundled extension's
      // `/tau-tree-navigate <entryId>` and verifies the move with a second
      // get_tree; picking a USER entry moves the leaf to that entry's PARENT and
      // hands back its text (the composer refill the webui does client-side).
      const body = (await readBody(req)) ?? {}
      const mid = String(body.messageID ?? '').trim()
      if (!mid) return sendJson(res, apiErr('messageID is required'), 400)
      if (s.isStreaming || (s.abortState && s.abortState !== 'idle')) {
        // the webui's isSessionBusy() aborts and retries — the shape is the contract
        return sendJson(res, { error: 'session is streaming', name: 'SessionBusyError' }, 409)
      }
      const e = entryFor(s.id)
      const nav0 = await messageEntryId(s, mid)
      if (nav0.error) return sendJson(res, apiErr(`pi could not report the session tree: ${nav0.error}`), 502)
      if (!nav0.entryId) return sendJson(res, apiErr(`no message ${mid} in this session`), 404)
      let nav = null
      try {
        nav = await navigateTree(s, nav0.entryId)
      } catch (err) {
        const msg = String(err?.message ?? err)
        if (/streaming/i.test(msg)) return sendJson(res, { error: 'session is streaming', name: 'SessionBusyError' }, 409)
        return sendJson(res, apiErr(`failed to move the session leaf: ${msg}`), 502)
      }
      // the boundary every client truncates its view at (webui applyRevert)
      e.revert = { messageID: mid, ...(body.partID ? { partID: String(body.partID) } : {}) }
      filesCache.list = null
      emitSessionUpdated(s.id, s, raw, true)
      return sendJson(
        res,
        {
          ...(ocSession(s) ?? { ok: true }),
          // tau hands the reverted text back because an RPC-mode extension cannot
          // fill an editor; the webui already has it locally, but an API user
          // should not have to re-read the transcript for it.
          ...(nav?.editorText ? { editorText: nav.editorText } : {}),
        },
        200,
      )
    }
    return sendJson(res, { error: 'not found' }, 404)
  }

  // ---- history / search (pi's own files) ----------------------------------
  if (pathname === '/api/history/sessions') {
    const meta = loadMeta()
    return sendJson(
      res,
      allSessionMeta().map((r) => {
        const mm = meta[r.id]
        return { ...r, file: undefined, ...(mm?.star ? { star: true } : {}), ...(mm?.tag ? { tag: mm.tag } : {}) }
      }),
    )
  }
  if (pathname === '/api/history/errors') {
    return sendJson(res, [...errTiles.entries()].flatMap(([sid, list]) => list.map((e) => ({ ...e, sid }))))
  }
  const em = pathname.match(/^\/api\/history\/session\/([^/]+)\/errors$/)
  if (em) {
    const sid = decodeURIComponent(em[1])
    const list = errTiles.get(sid) ?? []
    if (req.method === 'POST') {
      const b = await readBody(req)
      if (b.message && !list.some((x) => x.message === b.message)) {
        list.push({ seq: list.length + 1, message: String(b.message), t: b.t ?? Date.now() })
        errTiles.set(sid, list)
      }
      return sendJson(res, { ok: true })
    }
    if (req.method === 'DELETE') {
      errTiles.delete(sid)
      return sendJson(res, { ok: true })
    }
    return sendJson(res, list)
  }
  const mm = pathname.match(/^\/api\/history\/session\/([^/]+)\/meta$/)
  if (mm) {
    const sid = decodeURIComponent(mm[1])
    const all = loadMeta()
    if (req.method === 'DELETE') {
      delete all[sid]
      saveMeta(all)
      return sendJson(res, { ok: true })
    }
    if (req.method === 'PUT' || req.method === 'POST') {
      const b = await readBody(req)
      const cur = all[sid] ?? { star: false, tag: null }
      if (typeof b.star === 'boolean') cur.star = b.star
      if (b.tag !== undefined) cur.tag = b.tag || null
      all[sid] = cur
      saveMeta(all)
      return sendJson(res, { ok: true, ...cur })
    }
    return sendJson(res, all[sid] ?? { star: false, tag: null })
  }
  const sm = pathname.match(/^\/api\/history\/session\/([^/]+)\/changes$/)
  if (sm) return sendJson(res, { files: [] })
  const hm = pathname.match(/^\/api\/history\/session\/([^/]+)$/)
  if (hm) {
    const entries = entriesForId(decodeURIComponent(hm[1]))
    if (!entries) return sendJson(res, { error: 'unknown session' }, 404)
    return sendJson(res, toHistMsgs(entries))
  }
  if (pathname === '/api/search') return sendJson(res, searchSessions(q.get('q') || ''))

  if (pathname === '/healthz') {
    return sendJson(res, {
      ok: true,
      live: liveManager.sessions.size,
      // which records no branch looked at, and how often — `curl :8124/healthz`
      // answers "did the bridge even see that event?" without log spelunking
      unhandled: Object.fromEntries(unhandled),
    })
  }

  return sendJson(res, { error: 'not found' }, 404)
}
/**
 * POST /oc/session/{id}/fork — fork is a FILE operation.
 *
 * The webui contract (api.ts forkSession, Transcript.svelte forkFrom,
 * commands.ts /fork): return the NEW session (id + title); the bridge copies
 * everything BEFORE the picked message and titles the fork itself. No child is
 * spawned for the fork — the new tab's first request resumes it through the normal
 * cold path, with the uuid as its public id from byte one.
 *
 * `s` is a live child if one happens to exist, and null otherwise: this route must
 * work identically for an open tab and for a sidebar row whose child the reaper
 * closed. Forking anyway when no entry chain can be read would write a transcript
 * that renders as an empty session — the bug this function exists to prevent — so
 * it answers 502 instead.
 */
async function handleFork(req, res, raw, s) {
  const body = (await readBody(req)) ?? {}
  const mid = String(body.messageID ?? '').trim()
  const file = fileForId(raw)
  if (!file || !fs.existsSync(file)) {
    // Nothing on disk names this id. That is either a created-but-never-prompted
    // session (a 400, there is genuinely nothing to copy) or an id the bridge has
    // never seen — and only one of those is the caller's *input* being wrong, which
    // is what 404 means here. Distinguish them by whether WE know the session.
    if (!sessionByAnyId(raw) && !reapedFile.has(raw) && !liveManager.findBySessionFile(file ?? '')) {
      return sendJson(res, { error: 'unknown session' }, 404)
    }
    return sendJson(res, apiErr('this session has no transcript to fork yet'), 400)
  }
  const fc = await forkEntriesFor(s, file)
  if (fc.error) return sendJson(res, apiErr(`could not read the session's entries: ${fc.error}`), 502)
  const chain = fc.entries
  let keep = chain
  if (mid) {
    const entryId = entryIdInChain(chain, mid)
    if (!entryId) return sendJson(res, apiErr(`no message ${mid} in this session`), 404)
    const i = chain.findIndex((e) => e.id === entryId)
    if (i < 0) return sendJson(res, apiErr(`message ${mid} is not on this session's active path`), 400)
    keep = chain.slice(0, i) // pi's position:"before" — the picked message is excluded
  }
  const n = forkCount(file) + 1
  const parentTitle = sessionMeta(file)?.title ?? ''
  let fork = null
  try {
    fork = forkTranscript(file, keep, `${parentTitle} (fork #${n})`.trim())
  } catch (err) {
    return sendJson(res, apiErr(`failed to write the fork: ${String(err?.message ?? err)}`), 500)
  }
  filesCache.list = null
  const meta = sessionMeta(fork.file)
  console.log(`[fork] ${raw}: ${keep.length} of ${chain.length} entries -> ${path.basename(fork.file)} (parent ${path.basename(file)})`)
  return sendJson(
    res,
    {
      id: fork.id,
      title: meta?.title ?? `${parentTitle} (fork #${n})`,
      cwd: meta?.cwd ?? s?.cwd ?? (liveCwd(readHeaderCwd(file)) || PI_CWD),
      time: { created: meta?.created ?? Date.now(), updated: meta?.updated ?? Date.now() },
      ...(meta?.tokens ? { tokens: meta.tokens } : {}),
      ...(meta?.tokens_generated ? { tokens_generated: meta.tokens_generated } : {}),
      cost: 0,
      model: { providerID: meta?.model_provider ?? '', id: meta?.model ?? '' },
      revert: null,
    },
    200,
  )
}

function modelSpec(model) {
  const { provider, modelId } = modelRefOf({ model })
  return modelId ? `${provider}/${modelId}` : ''
}

/**
 * One shape for every model the webui posts, because it posts two. Picks that
 * name a session (`createSession`, `setSessionModel`) use `model: {providerID,
 * id, variant?}`; the prompt body (`api.ts prompt()`) uses opencode's shape —
 * `model: {providerID, modelID}` with the thinking **variant as a top-level
 * body field**. Reading only `model.id` / `model.variant` silently dropped BOTH
 * out of every prompt: the model pick never landed, and the composer's thinking
 * selector looked dead — for a tab that had no child at pick time the sibling
 * `/api/session/{id}/model` POST goes nowhere, so the prompt body is the only
 * channel that always exists. The engine reports variant:"default" for unset.
 */
function modelRefOf(body) {
  const m = body?.model ?? {}
  const modelId = typeof m.id === 'string' ? m.id : typeof m.modelID === 'string' ? m.modelID : ''
  const provider = typeof m.providerID === 'string' ? m.providerID : typeof m.provider === 'string' ? m.provider : ''
  const v = typeof m.variant === 'string' ? m.variant : typeof body?.variant === 'string' ? body.variant : ''
  return { provider, modelId, variant: v && v !== 'default' ? v : null }
}

export { handleApi }
