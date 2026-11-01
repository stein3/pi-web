// pi-web/lib/http.mjs — HTTP plumbing: JSON bodies, static webui/dist.
import fs from 'node:fs'
import path from 'node:path'
import { WEBUI_DIST } from './config.mjs'

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.webmanifest': 'application/manifest+json',
  '.map': 'application/json',
  '.txt': 'text/plain; charset=utf-8',
}

function sendJson(res, obj, code = 200) {
  const body = JSON.stringify(obj)
  res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' })
  res.end(body)
}

function readBody(req) {
  return new Promise((resolve) => {
    let buf = ''
    req.on('data', (c) => (buf += c))
    req.on('end', () => {
      try {
        resolve(JSON.parse(buf || '{}'))
      } catch {
        resolve({})
      }
    })
    req.on('error', () => resolve({}))
  })
}

function serveStatic(req, res, pathname) {
  let p = pathname === '/' ? '/index.html' : pathname
  if (!path.extname(p)) p = '/index.html'
  let full
  try {
    full = fs.realpathSync(path.join(WEBUI_DIST, p))
  } catch {
    res.writeHead(404).end('not found')
    return
  }
  if (!full.startsWith(WEBUI_DIST)) {
    res.writeHead(403).end('forbidden')
    return
  }
  if (!fs.existsSync(full)) {
    res.writeHead(404).end('not found')
    return
  }
  const noncePlaceholder = 'OPENCODE_CSP_NONCE'
  let body = fs.readFileSync(full)
  if (body.includes(noncePlaceholder)) {
    body = Buffer.from(body.toString('utf8').replace(/OPENCODE_CSP_NONCE/g, 'pi-web'))
  }
  res.writeHead(200, {
    'content-type': MIME[path.extname(full)] ?? 'application/octet-stream',
    'cache-control': p.startsWith('/assets/') ? 'public, max-age=31536000, immutable' : 'no-cache',
  })
  res.end(body)
}

export { readBody, sendJson, serveStatic }
