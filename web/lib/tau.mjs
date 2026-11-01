// pi-web/lib/tau.mjs — the pi-tau-web-server surface, imported dynamically so
// config.mjs has settled env first. Session trash lives here because it is
// defined relative to tau's SESSIONS_DIR.
import path from 'node:path'
import './config.mjs' // env before tau — see the header

const { liveManager } = await import('pi-tau-web-server/bin/sessions.js')
const { navigateTree, flattenTree, pathFromRoot } = await import('pi-tau-web-server/bin/tree.js')
const { SESSIONS_DIR } = await import('pi-tau-web-server/bin/config.js')
const { getAvailableModels } = await import(
  'pi-tau-web-server/bin/model-utils.js'
)
// tau's directory completer for the new-session workspace picker
const { completeDirectories } = await import(
  'pi-tau-web-server/bin/path-completion.js'
)

// Soft-delete trash root. pi stores one dir per workspace directly under
// SESSIONS_DIR and listFiles() walks that tree — so a `sessions/trash/` dir
// re-lists every "deleted" session forever (delete looked like a no-op).
// Trash lives OUTSIDE the scanned tree; pi's own resume picker only reads
// sessions/<encoded-cwd>/ flat, so it never sees it either.
const TRASH_DIR = path.join(path.dirname(SESSIONS_DIR), 'sessions-trash')
const TRASHED_DIRS = new Set(
  // 'trash' = the in-tree trash older builds left behind; never re-list it
  [TRASH_DIR, path.join(SESSIONS_DIR, 'trash')].map((p) => path.resolve(p)),
)

export { SESSIONS_DIR, TRASHED_DIRS, TRASH_DIR, completeDirectories, flattenTree, getAvailableModels, liveManager, navigateTree, pathFromRoot }
