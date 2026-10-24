#!/usr/bin/env bash
# Launch pi-web. Usage: launch.sh [port] [logfile]
# Requires `pi` on PATH (npm i -g @earendil-works/pi-coding-agent) and
# pi-tau-web-server resolvable (npm i -g, or run from web/ after npm install).
# Env overrides: PI_CODING_AGENT_DIR, PI_CWD, WEBUI_DIST, GEMINI_API_KEY.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PORT="${1:-8124}"
LOG="${2:-$HERE/.logs/pi-web.log}"
mkdir -p "$(dirname "$LOG")"
export PI_CODING_AGENT_DIR="${PI_CODING_AGENT_DIR:-$HERE/../config/pi-agent}"
exec node "$HERE/pi-web.mjs" --port "$PORT" >"$LOG" 2>&1
