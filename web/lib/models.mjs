// pi-web/lib/models.mjs — model catalogue for the picker (store + router + CLI
// tiers) and the picker.json filters.
import fs from 'node:fs'
import path from 'node:path'
import { PI_AGENT_DIR } from './config.mjs'
import { getAvailableModels } from './tau.mjs'

// ---------------------------------------------------------------------------
// Model catalog (feeds GET /oc/config/providers → ComposerModelPicker,
// ModelSelect, Footer/InfoPanel context sizes).
//
// tau's getAvailableModels() shells out to `pi --list-models`, which only knows
// whatever pi last restored into its own store snapshot AND pays for a pi child
// process (30 s timeout) on every refresh. pi-web builds the list natively
// instead, in three tiers:
//   1. `<agentDir>/models-store.json` — the very file pi restores from, parsed
//      directly (mtime-cached, no subprocess). Entries carrying a `type` (the
//      vision bridge) are not chat models and stay out.
//   2. the llama.cpp router's `GET <base>/v1/models`, merged on top so a box
//      nobody re-synced still lists every model. Embedding-only entries (no
//      `text` output modality) are dropped. The same response reports which
//      model is resident, so the loaded one sorts FIRST: webui's
//      preferredDefaultModel takes providers[0]'s first model for a brand-new
//      chat, and an alphabetical list would boot it onto an unloaded GGUF —
//      which the router then loads, evicting the resident one
//      (max_instances: 1).
//   3. tau's CLI listing, only when 1+2 produce nothing (a cloud-only setup,
//      whose providers never appear in a local store).
// Listing never loads anything: selecting an unloaded model makes the router
// load it on that model's next request. A model the router knows but pi does
// not (nothing wrote it into the store) will be rejected by pi's `set_model` —
// pi-web swallows that error and the session keeps its old model.
// ---------------------------------------------------------------------------

const ROUTER_TTL_MS = 60_000
const STATE_RANK = { loaded: 0, sleeping: 1, unloaded: 2 }
// llama.cpp reports an embedding GGUF with output_modalities ["text"] and no
// other tell (no --embeddings in its preset args either), so the name is the
// only signal available. A false negative here costs a model nobody would pick.
const EMBEDDING_RE = /embed|rerank/i
const storeCache = { mtime: 0, providers: [] }
const modelsJsonCache = { mtime: 0, overrides: {}, defs: [] }
let routerCache = { at: 0, base: '', models: [] }

function readJson(p) {
  try {
    return JSON.parse(fs.readFileSync(p, 'utf8'))
  } catch {
    return null
  }
}

/** Providers parsed out of pi's models-store.json; [] when there is no store. */
// pi's thinking levels (core/defaults.js). `xhigh`/`max` exist for a model ONLY
// when it maps them (pi-ai getSupportedThinkingLevels), and a level mapped to
// null is explicitly unsupported — the picker must not offer either.
const THINKING_LEVELS = ['off', 'minimal', 'low', 'medium', 'high']
const THINKING_LEVELS_EXPLICIT = ['xhigh', 'max']

/**
 * The webui's model picker shows per-model "variants" (`extractVariants` reads
 * `models[mid].variants`, keys = variant ids, `{disabled:true}` hides one) and
 * sends the pick back as `model.variant`. For pi that IS the thinking level —
 * without this the picker offers no thinking control for pi models AND a pick
 * made on the opencode side is silently dropped here.
 */
function thinkingVariants(m) {
  if (!m.reasoning) return undefined
  const map = m.thinkingLevelMap ?? {}
  const out = {}
  for (const lvl of [...THINKING_LEVELS, ...THINKING_LEVELS_EXPLICIT]) {
    if (map[lvl] === null) continue
    if (THINKING_LEVELS_EXPLICIT.includes(lvl) && map[lvl] === undefined) continue
    out[lvl] = {}
  }
  return Object.keys(out).length > 1 ? out : undefined
}

/**
 * `modelOverrides` from `<agentDir>/models.json` — the layer pi composes LAST
 * (core/provider-composer.js `applyModelOverride`: reasoning, thinkingLevelMap,
 * compat are all overridable). It matters because `models-store.json` is a CACHE
 * pi's providers rewrite: the llama provider re-probes /props and rewrites its
 * section, hand-edits included (observed 2026-10-30: a hand-patched store went
 * back to `{off, medium}` + `supportsReasoningEffort:false` on the restart after
 * a rebuild). So the catalogue this bridge publishes for the picker has to apply
 * the same override the child will actually run under, or the webui advertises a
 * thinking ladder pi ignores (or hides one it honors).
 */
function readModelsJson(agentDir) {
  const file = path.join(agentDir, 'models.json')
  let st
  try {
    st = fs.statSync(file)
  } catch {
    return { mtimeMs: 0, overrides: {}, defs: [] }
  }
  if (modelsJsonCache.mtime === st.mtimeMs) {
    return { mtimeMs: st.mtimeMs, overrides: modelsJsonCache.overrides, defs: modelsJsonCache.defs }
  }
  const raw = readJson(file) ?? {}
  const out = { overrides: {}, defs: [] }
  for (const [pid, entry] of Object.entries(raw?.providers ?? {})) {
    if (entry?.modelOverrides) out.overrides[pid] = entry.modelOverrides
    // Full provider definitions point at a baseUrl AND list models: that is a
    // provider pi itself composes (strata, opencode-go), so the picker gets a row.
    if (Array.isArray(entry?.models) && entry.models.length) out.defs.push({ id: pid, entry })
  }
  modelsJsonCache.mtime = st.mtimeMs
  modelsJsonCache.overrides = out.overrides
  modelsJsonCache.defs = out.defs
  return { mtimeMs: st.mtimeMs, overrides: out.overrides, defs: out.defs }
}

function modelsJsonOverrides(agentDir) {
  return readModelsJson(agentDir).overrides
}

/** A store row (or models.json definition) shaped how the webui picker reads models. */
function pickerModel(m, ov = {}) {
  // pi merges a map, it does not replace it (applyModelOverride).
  const eff = {
    ...m,
    ...(ov.reasoning === undefined ? {} : { reasoning: ov.reasoning }),
    ...(ov.thinkingLevelMap || m.thinkingLevelMap
      ? { thinkingLevelMap: { ...(m.thinkingLevelMap ?? {}), ...(ov.thinkingLevelMap ?? {}) } }
      : {}),
  }
  const variants = thinkingVariants(eff)
  return {
    id: m.id,
    name: m.name ?? m.id,
    limit: { context: m.contextWindow ?? 0, output: m.maxTokens ?? 0 },
    ...(eff.reasoning === undefined ? {} : { reasoning: !!eff.reasoning }),
    ...(variants ? { variants } : {}),
  }
}

function storeProviders() {
  const file = path.join(PI_AGENT_DIR, 'models-store.json')
  let st
  try {
    st = fs.statSync(file)
  } catch {
    return []
  }
  const mj = readModelsJson(PI_AGENT_DIR)
  const cacheKey = `${st.mtimeMs}:${mj.mtimeMs}`
  if (storeCache.mtime === cacheKey) return storeCache.providers
  const raw = readJson(file) ?? {}
  const providers = []
  for (const [pid, entry] of Object.entries(raw)) {
    const models = {}
    let routerBase = ''
    for (const m of entry?.models ?? []) {
      if (!m?.id || m.type) continue // typed sections (vision) are not chat models
      if (!routerBase && m.baseUrl) routerBase = String(m.baseUrl).replace(/\/v1\/?$/, '')
      models[m.id] = pickerModel(m, mj.overrides[pid]?.[m.id])
    }
    if (Object.keys(models).length) providers.push({ id: pid, name: pid, models, routerBase })
  }
  // Providers a models.json definition names but the store cache never heard of:
  // the store is a cache pi's own providers rewrite (llama.cpp, lemonade), so a
  // hand-defined provider ("strata") or a cloud one ("opencode-go") existed only
  // in the config layer and the picker showed the store's provider instead.
  for (const def of mj.defs) {
    const existing = providers.find((p) => p.id === def.id)
    if (existing) continue
    const models = {}
    let routerBase = ''
    for (const m of def.entry.models) {
      if (!m?.id || m.type) continue
      if (!routerBase && m.baseUrl) routerBase = String(m.baseUrl).replace(/\/v1\/?$/, '')
      models[m.id] = pickerModel(m, mj.overrides[def.id]?.[m.id])
    }
    if (Object.keys(models).length) providers.push({ id: def.id, name: def.id, models, routerBase })
  }
  storeCache.mtime = cacheKey
  storeCache.providers = providers
  return providers
}

// ---------------------------------------------------------------------------
// Picker filters — picker.json decides what the picker SHOWS.
//
// Neither layer a bridge may edit survives a hand-managed "models I do not
// want" list: models-store.json is a CACHE pi's own providers rewrite, and
// models.json is the layer pi composes (arbitrary keys are not pi config). The
// picker extension's config file is the one place already owned by the picker:
//
//   { "mode": "all",
//     "hidden": ["deepseek-v4*", "grok*", "mimo-v2.5*", "*Contributor"] }
//
// `hidden` — glob patterns matched against the bare model id AND
// "provider/model", case-insensitively (ids are lowercase but a shorthand like
// `*Contributor` names the display word, not the id). `*` does not cross a "/"
// inside the pattern; append the prefixed form to cover a whole provider.
// ---------------------------------------------------------------------------

const pickerConfigCache = { mtimeMs: 0, config: {} }

function pickerConfig() {
  const file = path.join(PI_AGENT_DIR, 'picker.json')
  let st
  try {
    st = fs.statSync(file)
  } catch {
    return {}
  }
  if (pickerConfigCache.mtimeMs === st.mtimeMs) return pickerConfigCache.config
  const config = readJson(file) ?? {}
  pickerConfigCache.mtimeMs = st.mtimeMs
  pickerConfigCache.config = config
  return config
}

const globs = new Map()

function globHit(pattern, s) {
  let re = globs.get(pattern)
  if (!re) {
    // case-insensitive: model ids are lowercase but a shorthand like
    // `*Contributor` names the display word, not the id
    re = new RegExp(
      '^' +
        pattern.replace(/[.+^$(){}[\]\\]/g, '\\$&').replace(/\*/g, '[^/]*').replace(/\?/g, '.') +
        '$',
      'i',
    )
    globs.set(pattern, re)
  }
  return re.test(s)
}

function isHiddenModel(pid, id) {
  const hidden = pickerConfig().hidden ?? []
  if (!Array.isArray(hidden) || !hidden.length) return false
  for (const pattern of hidden) {
    if (typeof pattern !== 'string' || !pattern) continue
    if (globHit(pattern, id) || globHit(pattern, `${pid}/${id}`)) return true
  }
  return false
}

function applyPickerFilters(rows) {
  for (const p of rows) {
    for (const id of Object.keys(p.models ?? {})) {
      if (isHiddenModel(p.id, id)) delete p.models[id]
    }
  }
  return rows
}
/** A store-less llama.cpp setup still names its router in auth.json. */
function authRouterBase() {
  const raw = readJson(path.join(PI_AGENT_DIR, 'auth.json')) ?? {}
  for (const [pid, cred] of Object.entries(raw)) {
    const url = cred?.env?.LLAMA_BASE_URL
    if (url) return { id: pid, base: String(url).replace(/\/v1\/?$/, '') }
  }
  return null
}

/** llama.cpp router catalogue: {id, context, state}; [] (uncached) on failure. */
async function routerModels(base) {
  if (routerCache.base === base && routerCache.models.length && Date.now() - routerCache.at < ROUTER_TTL_MS)
    return routerCache.models
  const out = []
  try {
    const res = await fetch(`${base}/v1/models`, { signal: AbortSignal.timeout(5000) })
    const data = await res.json()
    for (const m of data?.data ?? []) {
      if (!m?.id || EMBEDDING_RE.test(m.id)) continue
      const mods = m.architecture?.output_modalities
      if (Array.isArray(mods) && !mods.includes('text')) continue // non-text output
      out.push({ id: m.id, context: m.context_length ?? 0, state: m.status?.value ?? 'unknown' })
    }
  } catch (err) {
    console.warn(`[pi-web] router model list failed (${base}: ${String(err?.message ?? err)})`)
    return out
  }
  routerCache = { at: Date.now(), base, models: out }
  return out
}

function sortLoadedFirst(p) {
  const models = Object.values(p.models)
  models.sort(
    (a, b) => (STATE_RANK[a.state] ?? 3) - (STATE_RANK[b.state] ?? 3) || String(a.name).localeCompare(String(b.name)),
  )
  p.models = Object.fromEntries(models.map((m) => [m.id, m]))
}

/** A line of pi's --list-models output: "1.0M" / "131.1K" -> 1000000 / 131000. */
function listModelsNum(s) {
  const m = String(s ?? '').match(/^([\d.]+)\s*([KM])?$/i)
  if (!m) return 0
  return Math.round(Number(m[1]) * (m[2]?.toUpperCase() === 'M' ? 1e6 : m[2] ? 1e3 : 1))
}

/** tau's `pi --list-models` mapped into the webui's provider shape. */
async function cliProviders() {
  const byProv = new Map()
  for (const m of await getAvailableModels().catch(() => [])) {
    const p = m.provider || 'unknown'
    if (!byProv.has(p)) byProv.set(p, { id: p, name: p, models: {} })
    byProv.get(p).models[m.id] = { id: m.id, name: m.id, limit: { context: listModelsNum(m.context), output: listModelsNum(m.maxOutput) } }
  }
  return [...byProv.values()]
}

async function modelProviders() {
  const providers = storeProviders().map((p) => ({ ...p, models: { ...p.models } }))
  if (!providers.length) {
    const auth = authRouterBase()
    if (auth) providers.push({ id: auth.id, name: auth.id, models: {}, routerBase: auth.base })
  }
  const router = providers.find((p) => p.routerBase)
  if (router) {
    for (const m of await routerModels(router.routerBase)) {
      const existing = router.models[m.id]
      if (existing) {
        existing.state = m.state
        if (!existing.limit.context && m.context) existing.limit.context = m.context
      } else {
        router.models[m.id] = { id: m.id, name: m.id, limit: { context: m.context || 0, output: 0 }, state: m.state }
      }
    }
  }
  // Builtin-only providers. The store is a CACHE pi's own providers rewrite and
  // pi's builtin catalogs (opencode, opencode-go, cloud vendors whose keys are
  // set) never write sections into it — so until now a provider pi could chat
  // with (and the webui could bridge fine) was invisible to the picker whenever
  // no models.json definition described it. The child is the only truthful
  // source for the composed list: tau's pi --list-models reports AVAILABLE
  // models only (auth-gated, so unconfigured vendors never leak in), cached 5
  // min on tau's side. Fill gaps, never clobber: a row from the store or a
  // models.json definition keeps its variants/ladders — the merged entry only
  // adds model ids the config layers do not know.
  for (const m of await getAvailableModels().catch(() => [])) {
    if (!m?.provider || !m?.id) continue
    const p = providers.find((r) => r.id === m.provider)
    if (p) {
      if (!p.models[m.id])
        p.models[m.id] = { id: m.id, name: m.id, limit: { context: listModelsNum(m.context), output: listModelsNum(m.maxOutput) }, ...(m.thinking ? { reasoning: true } : {}) }
      continue
    }
    const row = { id: m.provider, name: m.provider, models: {} }
    row.models[m.id] = { id: m.id, name: m.id, limit: { context: listModelsNum(m.context), output: listModelsNum(m.maxOutput) }, ...(m.thinking ? { reasoning: true } : {}) }
    providers.push(row)
  }
  if (!Object.values(providers).some((p) => Object.keys(p.models).length)) {
    return applyPickerFilters(cliProviders())
  }
  // The filter pass runs LAST over the merged catalogue but ahead of the
  // shaper, over the response rows only — engine telemetry keeps reading
  // unfiltered storeProviders(), whose id table the filter must not disturb.
  applyPickerFilters(providers)
  for (const p of providers) {
    delete p.routerBase
    if (Object.keys(p.models).length) sortLoadedFirst(p)
  }
  return providers.filter((p) => Object.keys(p.models).length)
}

export { modelProviders, storeProviders }
