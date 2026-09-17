# Claude Code integration research

Condensed from prior investigation of two reference implementations: the
`paseo-smart-session` Paseo plugin (TypeScript) and the `AiOverviewControl`
DankMaterialShell status-bar plugin (bash+QML). This is ground truth for the Claude Code
`HarnessAdapter` implementation (Phase 4) — do not re-derive from scratch.

## Usage/quota API

- `GET https://api.anthropic.com/api/oauth/usage`
- Headers: `Authorization: Bearer <token>`, `Accept: application/json`,
  `Content-Type: application/json`, `User-Agent: claude-code/<version>`,
  `anthropic-beta: oauth-2025-04-20`.
- Token source: `~/.claude/.credentials.json` → OAuth access token
  (`claudeAiOauth.accessToken` per one reference's field path).
- Response shape: `{ five_hour: { utilization: <0-100 float>, resets_at: <ISO8601> },
  seven_day: { utilization, resets_at }, extra_usage: { is_enabled: bool } }`.
- There is no local formula for "percent used" — trust the API's `utilization` field
  verbatim, clamp to `[0,100]` defensively. `resets_at` is authoritative, use directly.
- Cache the response locally (120s TTL is what the reference plugins use). On
  429/5xx/network failure, fall back to the last cached snapshot even past its TTL
  (stale-but-useful beats erroring). On 401/403, treat as a hard auth failure and do
  NOT trust stale cache (credentials are genuinely invalid).
- A second, independent signal exists: `~/.claude/projects/*.jsonl` transcripts, whose
  assistant messages carry `message.usage.{input_tokens, output_tokens,
  cache_read_input_tokens, cache_creation_input_tokens}`. This is useful for local
  token/cost analytics and for the transcript-export step of the reseed flow, but is
  NEVER reconciled against the API's `utilization` percentage by either reference
  plugin — keep them as separate signals, don't average them.
- Subagent (sidechain) transcripts are NOT under `~/.claude/projects` — they live in
  `/tmp/claude-<uid>/.../tasks/<agentId>.output` and are periodically purged. Any local
  token accounting that only reads the projects dir will silently miss subagent spend.

## Hook surface — the load-bearing gotchas

- `hookSpecificOutput.hookEventName` is validated against a narrower union than the
  full hook-event list. `PreCompact`/`PostCompact` are NOT in that union — anything a
  `PostCompact` hook returns is silently rejected wholesale. This is why nothing can
  inject text right at the compaction boundary via that hook.
- `SessionStart:compact` fires ~55ms after `PostCompact` and CAN inject
  `additionalContext` — this is the one confirmed-working "push text into a fresh/
  compacted session's context" channel. Its `initialUserMessage` field does NOT start a
  new turn, though (only the CLI bootstrap consumes `pendingInitialUserMessage`; a hook
  setting it produces `num_turns: 0`, verified empirically) — so it can add context, but
  cannot itself kick off a new turn of work.
- `Stop`'s `additionalContext` really does ride the agent's own live turn (verified with
  an echo-marker test) — this is the channel used to "ask" the agent to consider
  compacting, since it can act on it in the same turn without a separate message being
  sent.
- `stop_hook_active` is enforced with a configurable cap
  (`CLAUDE_CODE_STOP_HOOK_BLOCK_CAP`, default 8) — check it to avoid asking twice into a
  hook-extended turn.
- `Stop` never fires after a `/compact` command runs (a slash command runs no model
  turn) — so nothing in Claude Code auto-continues a task after compaction. Observed on
  one reference author's own machine: of 190 real compactions, 163 just stalled waiting
  on a human.

## Compaction mechanism

- `/compact [instructions]` is a root-only slash command
  (`CLAUDE_ROOT_ONLY_COMMANDS` includes `compact`). There is no other compact API.
  "Compacting an agent" = sending that literal text as a message, timed for when the
  agent is idle (never mid-turn — `/compact` steers a live turn instead of landing as
  its own instruction if sent while one is in flight).
- Compact prompt template (validated end-to-end by one reference, 39,325→6,160 tokens
  with custom instructions honored):
  ```
  /compact
  Preserve: the current goal, the step in progress and its exact next action, decisions
  already made and why, and every approach already tried and rejected.
  Discard: file contents already read, superseded plans, and tool output that has been
  acted on.
  Reason for compacting now: <reason>
  [Authoritative state lives at <statePath>. Re-read that file before acting; where it
  disagrees with this summary, the file wins.]
  ```
- Delivery must be queued with a claim-before-send, atomic state-transition pattern
  (`pending -> sending -> sent|failed|cancelled`) — since `/compact` is destructive and
  non-idempotent, a crash mid-send must fail closed (mark `failed`, never blind-retry).
- `compact_boundary` system message carries `{trigger: manual|auto, preTokens,
  postTokens}` for grading, but `trigger` cannot distinguish an agent-mandated compact
  from a human-typed one — correlate against your own request queue instead.
- Because nothing auto-continues after compaction, send a second message once grading
  confirms the compaction landed (poll token count until it drops below the pre-compact
  value, then send). Keep this to exactly the intentional call sites (the `/compact`
  itself, and this one continuation) — don't grow a third unsolicited-send path without
  a deliberate design decision, since sending unsolicited messages to a live agent is
  the single most invasive thing this adapter does.
- Continuation message default: "Re-read `<statePath>` and continue from its 'Current
  step' section. Where the file disagrees with the summary above, the file is correct...
  Carry on from there without asking what to do next."

## Resume

- `claude --resume <session_id>` (or `claude -r`) respawns against an existing session.
- Session identity: use the harness-assigned STABLE id, not Claude Code's internal
  session id (which can change under the hood during compaction). Sanitize to
  `[A-Za-z0-9_-]` before using as a filesystem/DB key.
- Detecting "stopped because of a usage-limit hit" is NOT solved by either reference
  plugin — one explicitly delegates it to a sibling plugin it doesn't have source access
  to. This adapter must implement real detection itself: candidate signals are (a) the
  process/session is no longer active, (b) the last transcript/hook activity shows a
  usage-limit-shaped error, (c) cross-reference against a `UsageSample` showing the
  relevant window was at/near 100% around the time activity stopped. No existing
  implementation to copy — this needs its own validation against real usage-limit stops.

## Config knobs worth surfacing to hooks/env (from research, useful defaults to expose)

- `CLAUDE_CODE_AUTO_COMPACT_WINDOW` / `autoCompactWindow` — set above this adapter's own
  `compact_pct` threshold so Claude Code's native auto-compact acts as a backstop, never
  racing this system's own trigger at the same number.
- `CLAUDE_CODE_MAX_CONTEXT_TOKENS`, `CLAUDE_CODE_DISABLE_1M_CONTEXT`, the `[1m]` model
  suffix — relevant to correctly reading a session's actual context window size for the
  idle-compact tier lookup (don't hardcode 200k).
- Claude Code has its own native "autocompact is thrashing" detection message when
  context refills to the limit within 3 turns of a previous compact, 3 times running —
  worth mirroring in this adapter's own thrash guard (don't re-ask/re-trigger
  immediately after a compaction that didn't help).

## MCP scoping note

A user-scope stdio MCP server (`claude mcp add-json --scope user <name> <definition>`)
loads into every Claude Code session on the machine, not just ones this tool manages.
Claude Code 2.1.261+ defers MCP tool-schema loading, so an empty/short `tools/list`
response costs negligible tokens when the server has nothing relevant to offer for a
given session (e.g. gate on a marker env var this adapter sets when it spawns/attaches
to a session it's tracking, similar to Paseo's own `PASEO_AGENT_ID`-presence gate).
