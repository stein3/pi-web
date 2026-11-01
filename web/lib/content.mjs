// pi-web/lib/content.mjs — pi content shapes -> strings and timestamps.

function textOf(content) {
  if (typeof content === 'string') return content
  // `tool_execution_end` hands over the whole ToolResult object
  // ({ content, details }), not its content blocks — unwrap it or the row
  // renders with no output until the toolResult message_end lands.
  if (content && typeof content === 'object' && !Array.isArray(content) && 'content' in content)
    return textOf(content.content)
  if (Array.isArray(content))
    return content
      .filter((b) => b?.type === 'text')
      .map((b) => b.text ?? '')
      .join('')
  return ''
}
function safeJson(s) {
  if (typeof s !== 'string') return s ?? {}
  try {
    return JSON.parse(s)
  } catch {
    return { _raw: s }
  }
}
/**
 * pi stamps messages with epoch **milliseconds numbers** (`1792713118085`),
 * not ISO strings — and `Date.parse(number)` is NaN (it stringifies first),
 * which silently turned every `created` into "right now". Accept numbers
 * (sec-epoch too, for safety) before falling back to Date.parse.
 */
function isoMs(ts) {
  if (typeof ts === 'number' && Number.isFinite(ts)) return ts < 1e12 ? ts * 1000 : ts
  if (ts) {
    const t = Date.parse(ts)
    if (!Number.isNaN(t)) return t
  }
  return Date.now()
}

export { isoMs, safeJson, textOf }
