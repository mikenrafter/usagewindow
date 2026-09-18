#!/usr/bin/env bash
set -euo pipefail

if [[ "${UW_RUN_LIVE_CLAUDE_IDENTITY_PROBE:-}" != "1" ]]; then
  echo "Refusing to spend Claude quota. Set UW_RUN_LIVE_CLAUDE_IDENTITY_PROBE=1 to run." >&2
  exit 2
fi

for command in claude jq timeout uuidgen; do
  command -v "$command" >/dev/null || {
    echo "Missing required command: $command" >&2
    exit 2
  }
done
[[ -f "${HOME}/.claude/.credentials.json" ]] || {
  echo "Claude OAuth credentials are unavailable." >&2
  exit 2
}

probe_dir=$(mktemp -d /tmp/usagewindow-claude-identity.XXXXXX)
probe_id=$(uuidgen)
hook_log="${probe_dir}/hooks.jsonl"
hook_command="sh -c 'tee -a ${hook_log} >/dev/null'"
settings=$(jq -cn --arg command "$hook_command" '{hooks:{SessionStart:[{matcher:"",hooks:[{type:"command",command:$command}]}],PreCompact:[{matcher:"",hooks:[{type:"command",command:$command}]}],PostCompact:[{matcher:"",hooks:[{type:"command",command:$command}]}],SessionEnd:[{matcher:"",hooks:[{type:"command",command:$command}]}]}}')
common=(--model sonnet --output-format json --settings "$settings" --setting-sources '' --strict-mcp-config --tools '' --permission-mode dontAsk)

run_new() {
  local output=$1 prompt=$2
  timeout 180 claude -p --session-id "$probe_id" "${common[@]}" "$prompt" >"$output"
}

run_resume() {
  local output=$1 prompt=$2
  timeout 180 claude -p --resume "$probe_id" "${common[@]}" "$prompt" >"$output"
}

run_new "${probe_dir}/initial.json" 'Reply with exactly: identity probe initialized'
filler=$(printf 'disposable-context-line %.0s' {1..2500})
for turn in 1 2 3; do
  run_resume "${probe_dir}/fill-${turn}.json" "Store no files. This is disposable context turn ${turn}. Reply only with ACK-${turn}. ${filler}"
done
run_resume "${probe_dir}/compact.json" '/compact Preserve only that this is a disposable identity verification probe.'
run_resume "${probe_dir}/resume.json" 'Reply with exactly: identity probe resumed after compact'

transcript=$(jq -r 'select(.transcript_path != null) | .transcript_path' "$hook_log" | sort -u)
[[ $(printf '%s\n' "$transcript" | sed '/^$/d' | wc -l) -eq 1 ]]
[[ "${transcript##*/}" == "${probe_id}.jsonl" ]]
jq -se --arg id "$probe_id" 'all(.[]; .session_id == $id)' \
  "${probe_dir}/initial.json" "${probe_dir}/fill-1.json" \
  "${probe_dir}/fill-2.json" "${probe_dir}/fill-3.json" \
  "${probe_dir}/compact.json" "${probe_dir}/resume.json" >/dev/null
jq -se --arg id "$probe_id" '
  all(.session_id == $id)
  and any(.hook_event_name == "PreCompact" and .trigger == "manual")
  and any(.hook_event_name == "SessionStart" and .source == "compact")
  and any(.hook_event_name == "PostCompact" and .trigger == "manual")
' "$hook_log" >/dev/null
boundary=$(jq -sce --arg id "$probe_id" '
  any(.type == "system" and .subtype == "compact_boundary"
      and .sessionId == $id and .compactMetadata.postTokens < .compactMetadata.preTokens)
' "$transcript")
[[ "$boundary" == "true" ]]
boundary_record=$(jq -sc --arg id "$probe_id" '
  map(select(.type == "system" and .subtype == "compact_boundary" and .sessionId == $id))
  | last.compactMetadata
' "$transcript")

jq -n --arg session_id "$probe_id" --arg transcript_path "$transcript" \
  --arg evidence_dir "$probe_dir" --argjson boundary "$boundary_record" \
  '{verified:true,session_id:$session_id,transcript_path:$transcript_path,
    pre_tokens:$boundary.preTokens,post_tokens:$boundary.postTokens,evidence_dir:$evidence_dir}'
