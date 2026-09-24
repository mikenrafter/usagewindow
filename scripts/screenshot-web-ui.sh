#!/usr/bin/env bash
set -euo pipefail

# Capture the web UI from the Rust dev server with safe, temporary fixture data.
# Usage: scripts/screenshot-web-ui.sh [output.png]

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output=${1:-"$repo_root/usagewindow-ui.png"}
source_db=${UW_SCREENSHOT_SOURCE_DB:-${HOME:?}/.usagewindow/usagewindow.db}
port=${UW_SCREENSHOT_PORT:-7879}
size=${UW_SCREENSHOT_SIZE:-1440,1200}
fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/usagewindow-screenshot.XXXXXX")
fixture_db="$fixture_dir/usagewindow.db"
server_log="$fixture_dir/server.log"
server_pid=''

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$fixture_dir"
}
trap cleanup EXIT

if [[ ! -f "$source_db" ]]; then
  printf 'source database not found: %s\n' "$source_db" >&2
  exit 1
fi

mkdir -p "$(dirname "$output")"
sqlite3 "$source_db" ".backup '$fixture_db'"

# Keep the existing usage/session data, but make every reported reset happen
# after depletion wherever a burn rate exists. This makes the `+ duration`
# styling observable without touching the live database.
sqlite3 "$fixture_db" \
  "UPDATE usage_samples
   SET resets_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+2 days')
   WHERE resets_at IS NOT NULL;"

(
  cd "$repo_root"
  UW_DB_PATH="$fixture_db" \
  UW_DEV_LISTEN_ADDR="127.0.0.1:$port" \
    nix develop --command cargo run -p uw-web --bin uw-web-dev
) >"$server_log" 2>&1 &
server_pid=$!

for _ in $(seq 1 120); do
  if curl --silent --fail "http://127.0.0.1:$port/" >/dev/null; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$server_log" >&2
    exit 1
  fi
  sleep 0.5
done

if ! curl --silent --fail "http://127.0.0.1:$port/" >/dev/null; then
  printf 'dev server did not become ready on port %s\n' "$port" >&2
  cat "$server_log" >&2
  exit 1
fi

chromium \
  --headless \
  --no-sandbox \
  --disable-gpu \
  --hide-scrollbars \
  --window-size="$size" \
  --virtual-time-budget=5000 \
  --screenshot="$output" \
  "http://127.0.0.1:$port/"

printf 'screenshot written to %s\n' "$output"
