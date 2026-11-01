// pi-web/lib/config.mjs — argv/env resolution. MUST evaluate before tau is
// imported (tau's config.js snapshots PI_CODING_AGENT_DIR at load); tau.mjs
// imports this module first for exactly that reason. `HERE` is the ENTRY's
// directory (this file's parent), so all defaults match the single-file build.
import path from 'node:path'
import { fileURLToPath } from 'node:url'

// ---------------------------------------------------------------------------
// argv / env — MUST be resolved before importing tau: its config.js snapshots
// PI_CODING_AGENT_DIR / PI_CODING_AGENT_SESSION_DIR at module load.
// ---------------------------------------------------------------------------

const argv = process.argv.slice(2)
const arg = (name, def) => {
  const i = argv.indexOf(`--${name}`)
  return i >= 0 && argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : def
}

// The ENTRY's directory (this file's parent — lib/ sits exactly one level below
// pi-web.mjs), so every default below resolves exactly as it did when the
// bridge was a single file living at the entry.
const HERE = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const PORT = Number(arg('port', process.env.PI_WEB_PORT || 8124))
const HOST = arg('host', process.env.PI_WEB_HOST || '0.0.0.0')
// The webui is a sibling clone (nopencode), so the default dist is relative to
// the entry, not to one machine's checkout: <repo>/web -> up two -> opencode.
// The container always passes --dist /app/webui; the derived default is for
// bare-metal runs.
const WEBUI_DIST = path.resolve(
  arg(
    'dist',
    process.env.WEBUI_DIST || path.join(HERE, '..', '..', 'opencode', 'webui', 'dist'),
  ),
)
const PI_CWD = path.resolve(arg('cwd', process.env.PI_CWD || '/workspace'))
const PI_AGENT_DIR = path.resolve(
  arg('dir', process.env.PI_CODING_AGENT_DIR || path.join(HERE, '..', 'config', 'pi-agent')),
)
process.env.PI_CODING_AGENT_DIR = PI_AGENT_DIR

export { HOST, PI_AGENT_DIR, PI_CWD, PORT, WEBUI_DIST }
