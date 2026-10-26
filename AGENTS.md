# AGENTS.md

`web/pi-web.mjs` is a single-file HTTP/SSE bridge: it answers the opencode-shaped
API the nopencode webui speaks, and drives `pi --mode rpc` children through
`pi-tau-web-server`'s `LiveSessionManager`. `README.md` is the human-facing doc;
this file is what matters when you edit the bridge.

## Invariants — each one broke something before

- **Ids.** The public session id is the uuid in pi's `<ts>_<uuid>.jsonl`. tau's
  `tau_*` ids are per-child handles: they change on resume, and a reaped child's
  id must keep resolving (`reapedFile` / `reapAlias`). Get this wrong and one
  session becomes two sidebar rows and two tabs. Three things hold that up:
  `POST /oc/session` waits (bounded, `PI_WEB_ID_WAIT_MS`) for pi's `get_state`
  before answering, because pi computes that path and uuid in `newSession()` —
  it *knows* them long before the file exists, and tau's own probe lands 250 ms
  after our response would have;
  `emitFor` addresses **one** id (the canonical one), while the alias set stays
  inbound-only — emitting to every alias put a second copy of every frame on the
  wire for every session whose child knew its file (a resumed turn arrived as
  `Hello Hello streamed streamed world.world.` when the deltas were joined);
  and a `sessionFile` pi has not written yet is NOT a transcript — `fileForId`,
  the idle reaper, `appendSessionInfo` and `trashSessionFile` must all check,
  since resuming a file that isn't there makes pi mint a second uuid inside a
  file named after the first.
- **Delete.** Removing a session must move the **JSONL** to
  `<agentDir>/sessions-trash` — outside every directory `listFiles()` walks — and
  resolve that file *before* closing the child. Closing the child only makes the
  row reappear.
- **Tool metadata.** The transcript's inline diff comes from
  `state.metadata.filediff.patch`, and the webui shallow-merges parts — so every
  later frame for a tool part must re-supply it. Copy the patch **only**:
  bash's `details.truncation.content` holds tens of KB and would ride every SSE
  frame. `pi`'s `edit` gives `details.patch`; `write` needs nothing.
- **A tool part's state is accumulated, never recomputed.** pi puts `id` and
  `toolName` on `toolcall_start` ONLY, streams the arguments through
  `toolcall_delta`, emits `toolcall_end` with no top-level `id`, and gives
  `tool_execution_end` no `args` at all. So `title`/`input`/`metadata` must fall
  back to what the call itself carried (`byCall[id].input`) — deriving them from
  the current event yields `title:""` + `input:{}` on the LAST frame, and because
  the webui shallow-merges, that blank REPLACES a populated line: the row goes
  blank exactly when the tool finishes and heals only at the post-turn refetch.
  Streaming the detail line out of partial argument JSON is what makes the row
  name the command while the model is still writing it (`partialTitle`,
  `PARTIAL_CAP`, `PI_WEB_TOOL_TITLE_MS`).
- **Revert moves the leaf; it never deletes.** `POST /oc/session/{id}/revert`
  drives tau's `navigateTree` (pi's RPC has no leaf-move command — its extension
  appends a `custom`/`tau:navigate-tree` marker which BECOMES the leaf, because pi
  re-derives the leaf from the last entry on load). So every projection must walk
  root→leaf: `sessionFor` seeds `leafChain(readJsonl(file))`, and tau replaces its
  own mirror after a navigation — dump the file and the user sees the branch they
  reverted away. Busy is `409 {name:"SessionBusyError"}` (the webui's
  `isSessionBusy()` aborts and retries), the marker clears on the next user
  message, and the reverted message id → entry id mapping is resolved against
  `get_tree`, not tau's mirror, whose pushed entries have no `id`/`parentId`.
- **Compaction is busy, not broken.** pi refuses a prompt outright while it is
  summarising (core/agent-session.js:1620). Forwarding that as a 502 reads as
  “your message failed” (the reported `send failed: … prompt_async -> 502`), so
  the bridge holds the send until `compaction_end` and delivers it
  (`PI_WEB_COMPACT_SEND_WAIT_MS`); past that window it answers
  `409 {name:"SessionBusyError"}` — pi never took the text, so the outcome is
  *known* and retryable, which is why it is not the `504 {unknown:true}` the
  prompt-timeout rule uses.
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
cd web && npm test    # diff 23 · delete 27 · compact 42 · hist 6 · title 19 · id 29 · tooltitle 39 · revert 24 · stream 32
```

All nine are self-contained: they seed fixtures, spawn `pi-web` against
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
