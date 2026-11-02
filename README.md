# pi-web

One entry — [`web/pi-web.mjs`](web/pi-web.mjs), with its modules in
[`web/lib/`](web/lib) — that turns pi's RPC mode into an
opencode-shaped HTTP + SSE backend, so a browser client can drive a real
`pi --mode rpc` child: sessions, prompts, mid-turn steering, tool rows with
inline diffs, compaction, and history.

pi has no HTTP server; everything programmatic is RPC over stdio. This is the
adapter. `pi-tau-web-server` is used as a **library** (its `LiveSessionManager`
owns the children), not as a server. The frontend it serves is the shared
[nopencode](https://github.com/stein3/nopencode) webui — this repo ships no UI.

## How it works

| | |
|---|---|
| **Wire format** | opencode's dialect: `/oc/session*`, `/oc/event` (SSE), `/oc/config/providers`, `/oc/command`. Frames are `{type, properties}`, and the `sessionID` inside `properties` is what clients key on. |
| **Processes** | one `pi --mode rpc` child per live session. Idle children are reclaimed (`PI_WEB_MAX_LIVE`, `PI_WEB_IDLE_TTL_MS`) — a reaped session is not deleted, the next request resumes it from its JSONL. |
| **Storage** | pi's own JSONL: `<agentDir>/sessions/<encoded-cwd>/<iso-ts>_<uuid>.jsonl`. Nothing else is persisted: history is projected per request (`toOcMessages`), so a cold refetch and the live stream can never disagree — that invariant drives most of the design. |
| **Ids** | every public id is pi's file **uuid**, because it is the name of the transcript and survives a reap, an exit and a restart. tau's `tau_*` ids are per-child handles (they change on every resume); they still resolve, so a tab holding one is never orphaned. |
| **Tool rows** | pi's tool `details.patch` is mapped to `state.metadata.filediff.patch` on the live, tool-result and JSONL paths, which is what the webui's inline diff block reads. Only the patch travels — never the whole `details` blob. |
| **Queues** | pi owns steer/follow-up inside the child; the bridge mirrors `queue_update` as complete lists (`session.queue`), never diffs, so a client that missed a frame heals on the next one. |
| **Deletes** | the JSONL is moved to `<agentDir>/sessions-trash/` — outside the tree `listFiles()` walks — plus its star/tag sidecar. |
| **Errors** | a timeout is an *unknown outcome* (504 `{unknown:true}`), not a failure: pi defers prompts issued inside its settle window, and a 500 invites a duplicate execution. |

## Run it

```bash
# container (host): serves host port 3002 -> 8124, mounts ./config/pi-agent
sudo docker compose up -d --build pi-web
```

The webui dist is bind-mounted from the sibling checkout, so build it once:

```bash
cd ../opencode/webui && npm ci && npm run build
```

`compose.yaml` mounts `${WEBUI_DIST:-../opencode/webui}/dist`, so a differently
named or placed checkout needs one line in `.env`, not an edit to the compose file.

Missing `index.html` produces a loud boot WARNING: the API still works, the UI 404s.

Bare metal (`pi` on PATH, `pi-tau-web-server` resolvable):

```bash
cd web && npm install && ./launch.sh 8124
node pi-web.mjs --port 8124 --dir ~/.pi/agent --cwd "$PWD"   # same thing, explicit
```

Defaults are derived from where `pi-web.mjs` lives: the webui dist is the sibling
clone (`../opencode/webui/dist`), the agent dir is `../config/pi-agent`. Override
with `--dist` / `WEBUI_DIST` and `--dir` / `PI_CODING_AGENT_DIR` if your layout
differs (the container always passes both explicitly).

## Configuration

All of it lives in the pi agent dir (`--dir`, default `~/.pi/agent`), which is
gitignored here because it holds credentials and transcripts.

- `settings.json` needs **both** `defaultProvider` and `defaultModel` — pi's
  `findInitialModel` checks the pair, and omitting one silently falls back to the
  first authed provider.
- `auth.json` base URLs **must include the scheme** (`http://host:port`) or pi
  exits with `ERR_INVALID_URL` before printing anything.
- `picker.json` is optional and picker-local: `hidden` holds glob patterns
  (bare id or `provider/id`, case-insensitive) that are stripped from the
  catalogue before it reaches the webui, and `hiddenCommands` keeps extension
  slash commands whose UI is a TUI dialog out of the palette.
- Provider keys come from the environment (`ANTHROPIC_API_KEY`,
  `OPENAI_API_KEY`, `GEMINI_API_KEY` — pi's names, not opencode's).

Tunables (all optional):

| variable | default | meaning |
|---|---|---|
| `PI_WEB_HOST` / `PI_WEB_PORT` | `0.0.0.0` / `8124` | bind address |
| `PI_WEB_MAX_LIVE` | `6` | LRU cap on live children |
| `PI_WEB_IDLE_TTL_MS` | `1200000` | reap a child after this much idle |
| `PI_WEB_REAP_INTERVAL_MS` | `60000` | reap sweep interval |
| `PI_WEB_SUMMARIZE_WAIT_MS` | `40000` | `/summarize` answers "started" past this, outcome continues over SSE |
| `PI_WEB_COMPACT_RPC_MS` | `600000` | ceiling on pi's `compact` RPC |
| `PI_CODING_AGENT_DIR`, `PI_CWD`, `WEBUI_DIST` | — | same meaning as the flags |

## Checks

`node test/run-checks.mjs` (or `cd web && npm test`) runs the self-contained
checks. Each boots its own pi-web against `test/fake-pi.mjs` — a scripted
stand-in that replays pi 1.3.0's real RPC events, including the result shapes
the bridge projects — in a scratch agent dir. No browser, no model, no network.

| check | guards |
|---|---|
| `diff-check` | edit rows carry `filediff.patch` cold, live, and after refetch; the **last** live frame doesn't clobber it |
| `delete-check` | DELETE removes the JSONL (even for a row keyed by a tau id), the row stays gone, the trash is never re-listed |
| `compact-check` | `/summarize` → pi `compact`: answers before the edge timeout, never sticks the UI busy, one error surface |
| `stream-check` | SSE census (`session.updated` must stay rare) + the reaper/id contract (reaped uuid and legacy tau id both resume; one row, one child) |

Operator-local extras — they drive a server you start yourself, and they exit **2**
(never a silent pass) when their tooling isn't installed:

```bash
# queue-check — ordering the engine owns, HTTP + SSE only
PATH=$PWD/test:$PATH PI_CODING_AGENT_DIR=/tmp/queue-agent node web/pi-web.mjs --port 8127 &
BASE_URL=http://127.0.0.1:8127 node test/queue-check.mjs

# ws-check / ui-check — the real webui in chromium; needs the sibling checkout's
# e2e helpers (E2E_HELPERS=<path/to/e2e/helpers/setup.mjs>, CHROMIUM_PATH=…)
BASE_URL=http://127.0.0.1:8126 node test/ws-check.mjs

# time-check — reads a real transcript, so point it at a scratch agent dir
PI_CODING_AGENT_DIR=/tmp/piwtest/pi-agent BASE_URL=http://127.0.0.1:8127 node test/time-check.mjs
```

Nothing in `test/` hard-codes an operator's directory any more: paths come from
`test/harness.mjs` (`REPO`, `WEB`, `TESTBIN`, `SHOTS`, `E2E_HELPERS`,
`sessionSlug`), and the fixtures' workspace is this repo unless `CHECK_CWD=`
says otherwise.

Every check accepts a build override for A/B against an older tree:
`PI_WEB_JS=` — or the `PI_WEB_SCRIPT=` some checks read first — resolves once,
in `harness.mjs`'s `BRIDGE`, so one flag moves the whole suite at another tree.
Plus `CENSUS_ONLY=1` and `DIFF_FIXTURE=`, because a check that can't fail isn't
a check.

## Deliberately not wired

- The **diff pane**: no `/api/history/session/{id}/changes`, and `/oc/vcs/diff`
  / `diff/raw` answer empty (`[]` / `''`) so the pane renders nothing instead of
  erroring. The inline transcript diff IS wired.
- `/fork` and `/revert` answer 501.
- `sessions/<cwd>/trash/` from older builds is skipped, not re-listed.

## Ignored on purpose

`AGENTS.md` (operator notes: hostnames, provider quirks, LAN details),
`config/pi-agent/` (auth, model store, session JSONL), `.env`,
`compose.override.yml`.
