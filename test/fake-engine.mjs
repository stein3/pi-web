/**
 * A scripted Strata/llama.cpp engine, IN-PROCESS, for the telemetry check.
 *
 * In-process on purpose: the checks' hard rule is that a check only ever kills
 * the PID it spawned, and a module-imported http server has no PID to kill —
 * `stop()` closes it, and a crashed check cannot leave an engine listening.
 *
 * It answers the three endpoints the bridge probes and COUNTS every request, so
 * a check can assert polling DISCIPLINE (rate while busy, silence while idle,
 * back-off when the engine hangs), not just that data arrived.
 *
 *   GET /status  {busy,queued,phase,prompt_tokens,generated,max_tokens}
 *   GET /metrics {live:{state,prompt_read,prompt_total,generated,tok_s},requests:[]}
 *   GET /slots   [{id,is_processing,n_prompt_tokens}]
 *
 * `mode`:
 *   'normal' — answers normally, time-driven prefill then decode
 *   'hang'   — never answers (a wedged slot): exercises the probe timeout +
 *              back-off. Requests still COUNT, which is the point.
 *   '404'    — answers 404 on everything (a plain llama.cpp with no /metrics)
 */
import http from 'node:http'

export function startEngine({ port, prefillMs = 600, decodeMs = 2000, promptTotal = 42000, tokPerMs = 0.02, mode = 'normal' }) {
  const eng = {
    port,
    promptTotal,
    busy: false,
    t0: 0,
    hits: { status: 0, metrics: 0, slots: 0, other: 0 },
    /** make the engine busy from now; it self-clears after prefillMs+decodeMs */
    run(at = Date.now()) {
      eng.busy = true
      eng.t0 = at
    },
    stop() {
      // 'hang' leaves requests open; close() alone would then wait forever.
      server.closeAllConnections?.()
      return new Promise((r) => server.close(r))
    },
  }
  const total = prefillMs + decodeMs
  const elapsed = () => Date.now() - eng.t0
  const sample = () => {
    const e = Math.max(0, Math.min(elapsed(), total))
    const reading = e < prefillMs
    const generated = reading ? 0 : Math.round((e - prefillMs) * tokPerMs)
    return {
      busy: eng.busy && e < total,
      phase: !eng.busy || e >= total ? 'tool call complete' : reading ? 'thinking' : 'answering',
      prompt_tokens: promptTotal,
      prompt_read: reading ? Math.round((promptTotal * e) / prefillMs) : promptTotal,
      prompt_total: promptTotal,
      generated,
      max_tokens: 208398,
      tok_s: reading ? null : +(1000 * tokPerMs).toFixed(1),
    }
  }

  const server = http.createServer((req, res) => {
    const url = String(req.url || '')
    // COUNT first, in every mode: a hung request is still a request, and the
    // back-off assertion is meaningless unless the wedge is measurable.
    if (url === '/status') eng.hits.status++
    else if (url === '/metrics') eng.hits.metrics++
    else if (url === '/slots') eng.hits.slots++
    else eng.hits.other++
    if (mode === 'hang') return // never answer: the probe must time out and back off
    if (mode === '404') {
      res.writeHead(404, { 'content-type': 'application/json' })
      return res.end('{"error":{"message":"not found"}}')
    }
    const s = sample()
    if (url === '/status') {
      res.writeHead(200, { 'content-type': 'application/json' })
      return res.end(
        JSON.stringify({
          busy: s.busy,
          queued: 0,
          phase: s.phase,
          prompt_tokens: s.prompt_tokens,
          generated: s.generated,
          max_tokens: s.max_tokens,
        }),
      )
    }
    if (url === '/metrics') {
      return res.end(
        JSON.stringify({
          engine: { model: 'fake-engine', max_context: 256000 },
          live: s.busy
            ? {
                state: s.phase === 'thinking' ? 'reading' : 'decoding',
                prompt_read: s.prompt_read,
                prompt_total: s.prompt_total,
                generated: s.generated,
                max_tokens: s.max_tokens,
                tok_s: s.tok_s,
              }
            : { state: 'idle', prompt_read: null, prompt_total: null, generated: null, tok_s: null },
          // one finished request: the per-request fields only exist AFTER a
          // request ends, which is why the live box cannot show them
          requests: [{ id: 'r1', reused: 41000, hit_rate: 0.976, decode_tok_s: 20 }],
          totals: { requests: 1 },
        }),
      )
    }
    if (url === '/slots') {
      res.writeHead(200, { 'content-type': 'application/json' })
      return res.end(JSON.stringify([{ id: 0, n_ctx: 256000, is_processing: s.busy, n_prompt_tokens: s.prompt_tokens }]))
    }
    res.writeHead(404, { 'content-type': 'application/json' })
    res.end('{}')
  })
  server.listen(port, '127.0.0.1')
  return eng
}
