# Codex CLI integration research

Investigation of the locally installed codex-cli 0.154.0 on 2026-09-17. Commands below were run from this repository unless a path is shown. The local CLI is the primary source for command behavior. The [Codex hooks documentation](https://developers.openai.com/codex/hooks) fills in the JSON input/output contract that --help does not print.

## Usage/quota API

Codex exposes provider-reported usage percentages and reset times. The supported local entry point is the Codex app-server protocol, not a documented HTTP endpoint and not a file under ~/.codex.

The existing read-only reference implementation at /home/v0id/.config/DankMaterialShell/plugins/aiOverviewControl/providers/get-codex-usage starts codex app-server and sends:

```
{"method":"initialize","id":0,"params":{"clientInfo":{"name":"ai_overview_control","title":"AiOverviewControl","version":"1.4.12"}}}
{"method":"account/read","id":1,"params":{"refreshToken":false}}
{"method":"account/rateLimits/read","id":2,"params":{}}
```

It reads result.rateLimits, preferring result.rateLimitsByLimitId.codex when present. It maps primary and secondary using usedPercent, windowDurationMins, and resetsAt. It converts Unix-second reset values to ISO-8601 and reports credits separately.

I ran the same protocol directly. Account identity and account id are redacted here:

```
codex-cli 0.154.0
account/read: {"type":"chatgpt","planType":"plus","email":"<redacted>"}
account/rateLimits/read:
{
  "ordinaryUsageAllowed": true,
  "rateLimits": {
    "limitId": "codex",
    "primary": {"usedPercent": 0, "windowDurationMins": 300, "resetsAt": 1789657079},
    "secondary": {"usedPercent": 33, "windowDurationMins": 10080, "resetsAt": 1789854007},
    "credits": {"hasCredits": false, "unlimited": false, "balance": "0"},
    "planType": "plus",
    "rateLimitReachedType": null
  },
  "rateLimitsByLimitId": {"codex": "<same limit object>"}
}
```

The five-hour and seven-day window lengths in this sample are explicit in the response: 300 and 10080 minutes.

The local state files do not provide a separate usage cache. codex doctor reported:

```
state DB             ~/.codex/state_5.sqlite
log DB               ~/.codex/logs_2.sqlite
queue DB             ~/.codex/queue_1.sqlite
thread history DB    ~/.codex/thread_history_1.sqlite
```

Inspection of those SQLite schemas found thread metadata and queued messages, but no rate-limit or reset table. codex features list has no usage or rate-limit feature. The installed config.toml contains project trust entries and [hooks.state] hashes only. It contains no usage, quota, or reset settings.

Adapter decision: implement fetch_usage by running or connecting to codex app-server and requesting account/rateLimits/read. Normalize primary.usedPercent and secondary.usedPercent directly to the two UsageWindowState values. Convert resetsAt from Unix seconds. Treat a missing rateLimits object, an unauthenticated account, or an app-server failure as a fetch error. Do not infer percentages from transcript token counts.

## Hook surface

codex features list reported hooks stable true. The top-level help includes:

```
--dangerously-bypass-hook-trust
    Run enabled hooks without requiring persisted hook trust for this invocation.
    DANGEROUS. Intended only for automation that already vets hook sources
```

This is a trust model, not a hook-disable switch. ~/.codex/config.toml stores SHA-256 trust records under [hooks.state]. The records include the event and hook index.

The actual user-scope file, /home/v0id/.codex/hooks.json, contains one PostToolUse command hook. The project example at /home/v0id/Documents/repos/phoe-nix/.codex/hooks.json contains PostToolUse, SessionStart, Stop, and UserPromptSubmit. Both files use hooks -> event -> matcher -> hooks -> {type:"command", command, timeout}. Their commands invoke tea hooks run --agent codex and entire hooks codex .... Neither file was changed.

Codex's documented event list is broader than the four configured locally. It includes PreToolUse, PermissionRequest, PostToolUse, PreCompact, PostCompact, UserPromptSubmit, SubagentStop, Stop, Interrupt, SessionStart, SubagentStart, and SessionEnd.

| Event | Can add context? | Can block or reject? | Usagewindow implication |
| --- | --- | --- | --- |
| SessionStart | Yes. Plain stdout and hookSpecificOutput.additionalContext become extra developer context. source is startup, resume, clear, or compact. | continue:false ends the turn before another model request. | Best status/context injection point, including immediately after compaction. |
| UserPromptSubmit | Yes. Plain stdout or hookSpecificOutput.additionalContext. | Yes. Return decision:"block" and reason, or exit 2 with the reason on stderr. | Useful for status on the next user turn, but not unsolicited mid-turn delivery. |
| PostToolUse | It can report systemMessage; the documented additional-context contract does not apply. | continue:false can stop normal processing after the tool has run. A blocking decision cannot undo side effects. | Good for logging and validation, not a safe status injection channel. |
| Stop | No additional-context field. It expects JSON, not plain text. | decision:"block" asks Codex to continue with a new continuation prompt. continue:false prevents that continuation. | It can ask Codex to continue, but is not a passive status channel. Guard against stop_hook_active. |
| PreCompact | No. Plain stdout is ignored. | continue:false stops compaction before it happens. | Use for recording or vetoing a compact, not for injecting post-compact status. |
| PostCompact | No. Plain stdout is ignored. | continue:false stops after compaction. | This is the dead end that looks like the right status hook. Use SessionStart with source:compact instead. |
| PreToolUse | No additionalContext support in this release. | Tool policy can block or rewrite input. Unsupported shared fields make the hook fail and Codex continues the tool call. | Not a context-injection path. |
| PermissionRequest | systemMessage is supported, but not additionalContext. | It affects an approval request; it is not a general turn blocker. | Approval policy only. |

The official docs say matching hooks from multiple files all run and matching command hooks for one event run concurrently. A usagewindow hook must not assume it can prevent another hook from starting. Non-managed hooks must be trusted by their exact current definition hash. Ordinary model-visible hook output defaults to roughly 2,500 tokens and spills larger output to a temporary file, so status output should remain short.

There is no Claude-style PostCompact injection gotcha in the same form. Codex has a PostCompact event, but its output cannot add context. The useful replacement is explicit and documented: after root compaction, a SessionStart hook matching source:"compact" runs before the next model request, and its additionalContext is delivered to that continuation. If it returns continue:false, Codex ends the turn without sending the request.

## Compaction mechanism

The requested checks were:

```
codex --help
codex exec --help
codex features list
rg -n -i 'compact|context_window|context.window|auto_compact' ~/.codex/config.toml
```

The CLI help has no compact subcommand or --compact flag. It does list remote_compaction_v2 as a stable enabled feature and context_management as under development, but codex features list does not expose a user-facing compact command. The current config has no auto_compact or context_window setting.

The official hook contract confirms that Codex can fire PreCompact and PostCompact for manual or auto triggers. That establishes automatic compaction exists, but the CLI does not expose a public slash command or headless compact operation in the inspected help. The exact internal trigger and token threshold are opaque to this adapter.

Adapter decision: treat compaction as automatic and observe it through PreCompact/PostCompact plus SessionStart(source=compact). Do not model a Codex equivalent of sending /compact. A SessionStart(source=compact) hook can restore a short status or state summary into the immediate continuation.

## Resume

codex resume --help reported:

```
Usage: codex resume [OPTIONS] [SESSION_ID] [PROMPT]

Resume a previous interactive session (picker by default; use --last to continue the most recent)

[SESSION_ID]
    Session id (UUID) or session name. UUIDs take precedence if it parses.
[PROMPT]
    Optional user prompt to start the session
--last
    Continue the most recent session without showing the picker
--include-non-interactive
    Include non-interactive sessions in the resume picker and --last selection
```

codex exec resume --help reported:

```
Usage: codex exec resume [OPTIONS] [SESSION_ID] [PROMPT]

Resume a previous session by id or pick the most recent with --last

[SESSION_ID]
    Conversation/session id (UUID) or thread name. UUIDs take precedence if it parses.
[PROMPT]
    Prompt to send after resuming the session. If - is used, read from stdin
--last
    Resume the most recent recorded session (newest) without specifying an id
--json
    Print events to stdout as JSONL
```

The exact non-interactive invocation for a specific session is:

```
codex exec resume <session-uuid> "Continue from the saved state."
```

For a prompt on stdin:

```
printf '%s\n' 'Continue from the saved state.' | codex exec resume <session-uuid> -
```

This is headless because it uses exec. It does not require --last and does not open the picker. The optional --json flag gives a JSONL event stream, and --output-last-message <file> captures the final response. --ephemeral makes the resumed run non-persistent and is not appropriate for a tracked resume.

codex agents --help says it browses sessions on the shared local app-server daemon. Running codex agents in this investigation returned ERROR: stdin is not a terminal, so no interactive picker/list was opened. The direct-id exec resume path is independently established by its help output.

Resume is destructive in the usagewindow sense because it starts work in a live session. The adapter must use the repository's claim-before-act queue pattern before invoking it. A failed process launch or nonzero result must close the delivery attempt rather than blindly retrying.

## Seed new session

Codex supports an arbitrary initial prompt directly:

```
codex exec [OPTIONS] [PROMPT]
    Initial instructions for the agent. If not provided as an argument (or if
    - is used), instructions are read from stdin.
```

That starts a new session with a summary or seed message, but it is one initial prompt, not a separate arbitrary message/context channel.

Codex also has a fork primitive. codex exec fork --help reported:

```
Usage: codex exec fork [OPTIONS] <SESSION_ID> [PROMPT]

Fork a previous session by id into a new session

<SESSION_ID>
    Conversation/session id (UUID) or thread name to fork
[PROMPT]
    Optional prompt to send after forking. If - is used, read from stdin
```

The interactive equivalent is codex fork [SESSION_ID] [PROMPT]. Forking preserves the prior conversation as the new session's starting context and then accepts a prompt. It is a strong candidate for a Codex seed_new_session implementation when continuity is wanted. It is not a blank new session preloaded with an arbitrary standalone context. For a summary-only reseed, use codex exec <summary> and persist the returned new session id.

## Session identity

Codex session ids are UUID-shaped identifiers. An inspected rollout file was:

```
~/.codex/sessions/2026/09/17/rollout-2026-09-17T03-57-51-01a0aecd-181f-7491-90b1-d2cc8beaad3f.jsonl
```

Its first record contained:

```
{"type":"session_meta","payload":{"session_id":"01a0aecd-181f-7491-90b1-d2cc8beaad3f","id":"01a0aecd-181f-7491-90b1-d2cc8beaad3f","originator":"codex_cli_rs","cli_version":"0.154.0","source":"exec"}}
```

The state database confirms the same stable id in threads.id, alongside rollout_path, cwd, source, cli_version, history_mode, archived, and timestamps. codex doctor reported 65 active rollout files and 80 archived rollout files, with 145 matching thread rows and zero duplicate ids or stale rows. Rollouts are stored under ~/.codex/sessions/YYYY/MM/DD/ while archived ones are under ~/.codex/archived_sessions/.

The stable value to persist is the UUID from session_meta or threads.id, not the rollout filename or a transient turn id. It can later be passed directly to codex resume <id>, codex exec resume <id>, codex fork <id>, or codex exec fork <id>. Hook payloads also include transcript paths and turn metadata, but the official docs warn that transcript format is not a stable hook interface. Use the UUID and CLI/database inventory for identity; use transcript JSONL only for export and diagnostics.

## Config knobs worth surfacing to hooks/env

- features.hooks = true is the canonical hook enable switch. This machine reports the stable hooks feature enabled. codex_hooks is documented as a deprecated alias.
- Hook trust is stored as hashes in [hooks.state]. A spawned automation may use --dangerously-bypass-hook-trust only when it has vetted the exact hook source. This should be an explicit adapter setting, not a default.
- additionalContextLimit controls the approximate token limit for context returned by command hooks. The documented default is roughly 2,500 tokens; status hooks should stay much shorter.
- SessionStart matchers can select startup, resume, clear, or compact. Usagewindow should use compact for post-compaction state injection and resume if it needs to distinguish a process restart.
- PreCompact and PostCompact matchers can select manual or auto, useful for recording whether Codex initiated compaction or a user action did.
- --ephemeral disables session persistence for codex exec, so the adapter must not use it for a tracked session unless it deliberately wants a non-resumable run.
- --json and --output-last-message are useful for headless adapter process supervision and result capture.
- --thread-source <SOURCE> is available on exec, exec resume, and exec fork for classifying newly created or forked threads.
- --include-non-interactive matters to the interactive resume --last picker. It is not needed when usagewindow passes an explicit id to exec resume.

No Codex config key for a usable context-window size or auto-compaction threshold was found in this installed version. Do not hardcode a context size from codex doctor; record token data only if the runtime emits it in events or the adapter has a separately verified source.

## MCP scoping note

codex mcp --help describes MCP as external-server management with list, get, add, remove, login, and logout. codex doctor reported zero MCP servers configured on this machine. codex plugin --help describes plugin installation and marketplace management; it does not expose a session-specific MCP scope flag.

The local research found no Codex-specific MCP registration that would limit a server to usagewindow-managed sessions. A user-level MCP server should therefore be assumed to load into every Codex session where that config is active. If a future usagewindow MCP server is installed globally, gate its behavior on an adapter-set environment marker or session id and keep its tools read-only by default. The hook path is a better fit for short status injection because it can target SessionStart(source=compact) without adding an MCP tool schema to unrelated sessions.

