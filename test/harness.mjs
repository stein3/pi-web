// Shared path wiring for the checks. Everything inside this repo is derived
// from this file's own location; anything outside it (the nopencode e2e
// helpers, chromium) is env-overridable and fails with a SKIP message instead
// of a hard crash, so a fresh clone learns what is missing rather than hitting
// ERR_MODULE_NOT_FOUND on someone else's absolute path.
import fs from 'node:fs'
import path from 'node:path'
import { pathToFileURL } from 'node:url'

const TESTBIN = import.meta.dirname ?? path.dirname(new URL(import.meta.url).pathname)

// The checks live at <repo>/test (moved out of web/.testbin), so the bridge dir
// is a SIBLING of this one, not its parent.
export { TESTBIN }
export const REPO = path.resolve(TESTBIN, '..')
export const WEB = path.join(REPO, 'web')

/**
 * The bridge a check boots: an explicit A/B build wins — `PI_WEB_JS`, or the
 * `PI_WEB_SCRIPT` some checks have always read (JS takes precedence if both are
 * set) — else this repo's entry. Set it once and the WHOLE suite points at
 * another tree, which is what makes "prove the check fails pre-fix" a one-flag
 * job instead of 22 per-check overrides.
 */
export const BRIDGE =
  process.env.PI_WEB_JS || process.env.PI_WEB_SCRIPT || path.join(WEB, 'pi-web.mjs')

// screenshots from the browser checks (gitignored: web/.logs)
export const SHOTS = process.env.SHOTS_DIR || path.join(WEB, '.logs')
export function shotsDir() {
  fs.mkdirSync(SHOTS, { recursive: true })
  return SHOTS
}

/** the workspace a check runs in — this repo, unless a caller says otherwise */
export const REPO_CWD = process.env.CHECK_CWD || REPO

/**
 * pi's own cwd -> session-dir encoding, copied from pi's session-manager
 * `getDefaultSessionDirPath`: `--` + the path with the leading separator
 * stripped and every `/` or `:` replaced by `-`, + `--`.
 * Checks that seed fixtures need it; hard-coding one machine's slug does not
 * survive a clone into any other directory.
 */
export function sessionSlug(cwd) {
  return `--${String(cwd).replace(/^[/\\]/, '').replace(/[/\\:]/g, '-')}--`
}

/**
 * Ports the checks bind. Convention: the spawned pi-web on CHECK_PORT and any
 * second fixture server (the fake engine) on ENGINE_PORT — always the 8126/8127
 * pair, never a hand-picked port. Two checks run back-to-back in ONE shell on
 * the same pair can flake on the previous child's teardown, so give each
 * concurrent run its own pair instead of sharing.
 */
export const CHECK_PORT = Number(process.env.CHECK_PORT || 8126)
export const ENGINE_PORT = Number(process.env.ENGINE_PORT || 8127)

/**
 * The nopencode checkout's e2e helpers (launchBrowser / BASE / sleep / poll).
 * Default: the sibling clone, i.e. `cd .. && git clone ... nopencode opencode`.
 * Override with E2E_HELPERS=<path to e2e/helpers/setup.mjs>.
 */
export const E2E_HELPERS =
  process.env.E2E_HELPERS || path.join(REPO, '..', 'opencode', 'e2e', 'helpers', 'setup.mjs')

export async function loadE2E() {
  if (!fs.existsSync(E2E_HELPERS)) {
    const err = new Error(
      `SKIP: e2e helpers not found at ${E2E_HELPERS}\n` +
        '  these two checks drive the real webui, so they need the sibling nopencode\n' +
        '  checkout (or point E2E_HELPERS at its e2e/helpers/setup.mjs) plus chromium.',
    )
    err.skip = true
    throw err
  }
  return import(pathToFileURL(E2E_HELPERS).href)
}

/**
 * Launch-time classification: playwright without a downloaded browser is the
 * same class of "not installed on this machine" as a missing sibling checkout,
 * so it reports as a SKIP (exit 2) rather than a product failure. Returns a
 * message, or null when the error is a real one.
 */
export function browserSkip(e) {
  const m = String(e?.message ?? e)
  if (!/Executable doesn't exist|browserType\.launch|playwright|chromium/i.test(m)) return null
  return (
    `SKIP: chromium unavailable for playwright: ${m.split('\n')[0]}\n` +
    '  install it (npx playwright install chromium) or set CHROMIUM_PATH=…'
  )
}
