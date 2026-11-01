// pi-web/lib/util.mjs — small shared helpers.
import fs from 'node:fs'
import path from 'node:path'

function apiErr(message, name = 'APIError') {
  return { name, data: { message: String(message) } }
}

// prompt_async file parts arrive in the opencode engine's shape:
// {type:'file', mime, url:'data:<mime>;base64,…'}. pi wants ImageContent blocks:
// {type:'image', data:<base64>, mimeType}. Returns null for anything else.
function fileToImage(p) {
  if (!p || p.type !== 'file') return null
  const m = /^data:([^;,]+);base64,([\s\S]*)$/.exec(p.url ?? '')
  if (!m) return null
  if (!m[1].startsWith('image/')) return null
  return { type: 'image', data: m[2], mimeType: m[1] }
}
function expandHome(p) {
  if (!p) return ''
  if (p === '~') return process.env.HOME || p
  if (p.startsWith('~/')) return path.join(process.env.HOME || '', p.slice(2))
  return p
}
/**
 * A transcript's header cwd is a snapshot of the workspace as it was when the
 * session STARTED, and workspaces get renamed and moved (a repo dir rename is
 * enough). pi will not run in a directory that is gone: tau throws
 * `Directory not found: <cwd>` (pi-tau-web-server/bin/sessions.js:168) BEFORE it
 * spawns, so every route through `sessionFor` rejects — the session stops being
 * readable, let alone sendable — while the sidebar keeps advertising a workspace
 * that does not exist. So a header cwd is honoured only if it is still a
 * directory; otherwise fall back to its nearest existing ANCESTOR (the repo moved,
 * its parent did not) and finally to PI_CWD. Never the filesystem root: a
 * transcript carried in from another machine must not put a pi child in `/`.
 */
function liveCwd(p) {
  if (!p) return ''
  let cur = path.resolve(expandHome(String(p)))
  for (;;) {
    if (cur === path.parse(cur).root) return ''
    try {
      if (fs.statSync(cur).isDirectory()) return cur
    } catch {}
    const up = path.dirname(cur)
    if (up === cur) return ''
    cur = up
  }
}

export { apiErr, expandHome, fileToImage, liveCwd }
