# AGENTS.md

`web/pi-web.mjs` is a single-file HTTP/SSE bridge: it answers the opencode-shaped
API the nopencode webui speaks, and drives `pi --mode rpc` children through
`pi-tau-web-server`'s `LiveSessionManager`. `README.md` is the human-facing doc;
this file is what matters when you edit the bridge.

## Invariants — each one broke something before

- **Ids.** The public session id is the uuid in pi's `<ts>_<uuid>.jsonl`. tau's
  `tau_*` ids are per-child handles: they change on resume, and a reaped child's
  id must keep resolving (`reapedFile` / `reapAlias`). Get this wrong and one
  session becomes two sidebar rows and two tabs.
- **Delete.** Removing a session must move the **JSONL** to
  `<agentDir>/sessions-trash` — outside every directory `listFiles()` walks — and
  resolve that file *before* closing the child. Closing the child only makes the
  row reappear.
- **Tool metadata.** The transcript's inline diff comes from
  `state.metadata.filediff.patch`, and the webui shallow-merges parts — so every
  later frame for a tool part must re-supply it. Copy the patch **only**:
  bash's `details.truncation.content` holds tens of KB and would ride every SSE
  frame. `pi`'s `edit` gives `details.patch`; `write` needs nothing.
- **SSE volume.** Do not mirror every pi event as `session.updated`: one
  coalesced frame per relevant metadata change, plus one at turn end. Deltas must
  rebuild the assistant text exactly, and a snapshot must equal the delta sum.
- **Queues are complete lists.** pi sends the whole `steering`/`followUp` set;
  mirror it as-is, and map a prompt's `delivery: 'steer' | 'follow'` onto pi's
  queue mode rather than inventing a second mechanism.
- **A timeout is an unknown outcome, not a failure.** Answer
  `504 {unknown:true}` — the request may still have landed — and never retry a
  prompt blindly.
- **Checks never kill the host.** A check spawns its own `pi-web` on 8126/8127
  with a scratch `PI_CODING_AGENT_DIR` and kills the exact PID it spawned
  (`echo $!`). Never pattern-kill: in the deployed container `pi-web` is PID 1
  and the agent's own `pi` child hangs off it, and `pkill -f "<pat>"` also
  matches the tool's own `bash -c` argv — both have killed live sessions.

## Checks

```bash
cd web && npm test    # diff-check 23 · delete-check 27 · compact-check 32 · hist-check 6 · stream-check 32
```

All five are self-contained: they seed fixtures, spawn `pi-web` against
`web/.testbin/fake-pi.mjs`, and kill their own child. A check must never depend
on gitignored data (real transcripts, `config/pi-agent/`) — seed a fixture, or
prefer one when it happens to exist. `PI_WEB_JS=` / `PI_WEB_SCRIPT=` point the
same assertions at another build: prove the check **fails** on the pre-fix build
before believing that it passes. `queue-check`, `ws-check`, `ui-check` and
`time-check` need a live server / chromium / the sibling checkout and exit **2**
with a SKIP message when it is missing — never a silent pass.

- **Token fields.** The history projection emits two *different* numbers: `tokens`
  (newest-record context estimate, kept for InfoPanel/Footer) and
  `tokens_generated` = `output + reasoning` summed over every assistant message —
  the number the webui sidebar row shows. Sum pi's per-message `usage` only
  (pi has no separate `usage` entries: 85/85 in a real transcript), never
  cache/input, and emit **no key** for an all-zero tally. Verify:
  `web/.testbin/hist-check.mjs`.

Paths in tests come from `web/.testbin/harness.mjs` (`REPO`, `SHOTS`,
`E2E_HELPERS`, `CHECK_CWD`, `sessionSlug`); nothing may hard-code a machine's
checkout.

## Deploy

Any edit to `web/pi-web.mjs` needs a rebuild — it is baked into the image:
`sudo docker compose up -d --build pi-web`. pi extensions live in the
bind-mounted agent dir, so they need only a `restart` (new RPC children pick them
up at `session_start`).

## Never commit

`config/pi-agent/**` (auth, session JSONL, model store, extension npm tree),
`.env`, `compose.override.yml`, `web/.logs/`, `web/node_modules/`. Before
pushing anything, grep the tree and the history for deployment leakage —
literal IPs, hostnames, absolute host paths, credentials. Expect no hits; a
public repo's history is public too. Real values belong in the deployment host's
`.env` / `config/`; `.env.example` keeps empty placeholders.
