// pi-web/lib/sse.mjs — the SSE hub: one fan-out point for every frame.

// ---------------------------------------------------------------------------
// SSE hub
// ---------------------------------------------------------------------------

/** @type {Set<import('node:http').ServerResponse>} */
const sseClients = new Set()

function emit(type, properties) {
  if (!sseClients.size) return
  const frame = `data: ${JSON.stringify({ type, properties })}\n\n`
  for (const res of sseClients) {
    try {
      res.write(frame)
    } catch {
      sseClients.delete(res)
    }
  }
}

export { emit, sseClients }
