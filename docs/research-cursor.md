# Cursor integration research

## Session discovery

`CursorAdapter::discover_sessions` (crates/uw-adapters/src/cursor.rs) reads
`cursor-agent`'s own on-disk transcript store, independent of any hook, the
same way the Claude Code and Codex adapters read their transcript/rollout
files. Layout verified 2026-09-22 on a live `~/.cursor/projects`:

```
~/.cursor/projects/<encoded-cwd>/agent-transcripts/<session-uuid>/<session-uuid>.jsonl
```

overridable via `CURSOR_PROJECTS_DIR` (matching the env var name
`modules/agentsview.nix` already uses for the same directory in phoe-nix).
Each line is a bare `{"role": "...", "message": {"content": [...]}}` object
with **no per-line timestamp, session id, or cwd field** — unlike Claude
Code's and Codex's transcripts, which carry `cwd`/`sessionId` directly. This
is the Cursor CLI/background-agent transcript store; the Cursor IDE's chat
panel keeps a separate sqlite-backed store under `~/.cursor/chats/<workspace
hash>/<session-uuid>/store.db`. The adapter reads that second store only for
child-session lineage, as documented below.

Consequences of the missing structured fields:

- **Session id** comes from the transcript filename (validated as a UUID),
  not file content.
- **cwd** is recovered by decoding the project directory name, which Cursor
  builds by joining the absolute path with `-` (e.g.
  `home-v0id-Documents-repos-usagewindow` ->
  `/home/v0id/Documents/repos/usagewindow`). This is lossy whenever a path
  segment itself contains a literal `-` (a repo named `dozens-game` decodes
  to `dozens/game`), because there is no structured field to disambiguate.
  `repo.json` next to each project directory carries only an opaque
  workspace id, not the real path, and `worker.log` only sometimes logs a
  `workspacePath=` line (42/56 sampled project directories had a
  `worker.log` at all, and not all of those logged that line), so neither is
  reliable enough to use as the primary source. Treat `DiscoveredSession.cwd`
  for Cursor as a best-effort label, not a verified filesystem path.
- **first_seen/last_seen** fall back to the transcript file's own mtime,
  since there is no in-content timestamp to read.
- **title** is the first user message's text, truncated to 120 characters.

## Child-session lineage (verified 2026-09-24)

Cursor stores child-agent identity outside the transcript tree. The chat store at
`~/.cursor/chats/<workspace-hash>/<session-uuid>/store.db` has a `meta` table whose
row with key `0` contains hex-encoded JSON. For the failed compaction incident,
session `e5ded152-88c0-4222-be0d-d629dc344555` decoded to this relationship:

```json
{
  "agentId": "e5ded152-88c0-4222-be0d-d629dc344555",
  "subagentInfo": {
    "parentAgentId": "a293a959-d37d-4e73-8848-f5b7f3d560a0",
    "rootParentAgentId": "a293a959-d37d-4e73-8848-f5b7f3d560a0",
    "typeName": "generalPurpose"
  }
}
```

`CursorAdapter::discover_sessions` reads this database in read-only mode after it
parses each transcript. It records the two ancestor IDs in `SessionLineage`, which the
daemon persists with the discovered session. `CURSOR_CHATS_DIR` overrides the chat root;
the default is `$HOME/.cursor/chats`.

Lineage enrichment is best effort. A missing database, unreadable database, absent row,
invalid hex, or invalid JSON leaves lineage empty and does not hide the transcript.
Malformed ancestor UUIDs are ignored. The decoded `agentId` must equal the transcript
filename ID. A mismatch discards the complete metadata record so an unrelated child
cannot be routed through the wrong owner. Discovery checks lineage again on
transcript-cache hits because Cursor can create the chat metadata after the transcript
first appears.

Only Cursor's format is verified here. The `SessionLineage` model is provider-neutral;
Claude Code and Codex adapters can populate it if their child-session formats are later
verified.

## Per-session context usage (verified 2026-09-25)

Cursor exposes no raw token counts anywhere on disk. Checked and ruled out:

- `~/.cursor/projects/<cwd>/agent-transcripts/<id>/<id>.jsonl` — bare
  `{"role","message"}` lines, no usage field (already documented above).
- `~/.cursor/acp-sessions/<id>/store.db` (`blobs`/`meta` tables, one JSON
  document per row) — a first pass found rows shaped like
  `{"timestamp":...,"type":"token_usage_record","payload":{"usage":{...}}}`,
  which looked like a real structured telemetry record. It was not: those
  rows were verbatim file content from a session where an agent had read
  `crates/uw-adapters/src/codex.rs`'s own test fixtures as tool output, and
  Rust's `format!()` `{{`/`}}` brace-escaping was still present in the text
  (`"usage":{{"input_tokens":10,...}}`). Confirmed by checking a session
  scoped to an unrelated repo (`phoe-nix`): zero `usage`/`token`/`context`
  hits anywhere in that store. **`acp-sessions` carries no usage telemetry**;
  a grep hit for those field names there is conversation content, not schema.
- `~/.cursor/chats/<workspace-hash>/<session-id>/store.db` (same
  blobs/meta shape as acp-sessions, used today only for lineage) — checked a
  51MB store from a real conversation, same null result.
- `~/.cursor/ai-tracking/ai-code-tracking.db` — tracks AI-vs-human line
  attribution for commits, not token/context usage.

The real source is the **desktop Cursor app's** own VSCode-fork global state,
which this adapter already opens for auth (`read_access_token_from_state_db`,
`CURSOR_STATE_DB` env override, default
`$XDG_CONFIG_HOME/Cursor/User/globalStorage/state.vscdb` or
`~/.config/...` when unset): table `cursorDiskKV`, key
`composerData:<session-id>` (JSON blob, `key like 'composerData:%'` to list).
Verified fields on a live conversation:

```json
{
  "contextUsagePercent": 66.2265,
  "name": "Subagent implementation for project remediations",
  "createdAt": 1779405474151,
  "lastUpdatedAt": 1779419767658,
  "usageData": {}
}
```

`contextUsagePercent` is exactly the number the Cursor UI shows for a
conversation's context-window fill. `createdAt`/`lastUpdatedAt` are real
epoch-millisecond timestamps — strictly better than the transcript-mtime
approximation `scan_cursor_transcript` uses today for `first_seen`/
`last_seen`, and `name` is a real title rather than the first-120-chars
scrape. `usageData` was empty on every composer checked; do not rely on it.
The session-id in the key matches the existing `SessionId` this adapter
already assigns from the transcript filename — confirmed directly: composer
id `5caf2dec-a694-4bdc-a180-4775e75bb307` has a matching
`agent-transcripts/5caf2dec-.../` directory. No new correlation is needed to
join this onto an already-discovered session.

Two open items a future change should account for, not assume away:

- **Coverage is unverified.** `composerData` rows exist for every composer id
  I sampled on this machine, but I could not confirm whether `cursor-agent`
  writes one for a session that only ever ran headless (never opened or
  resumed through the desktop IDE). This machine has both installed, so a
  present row here doesn't rule out a purely-CLI machine having none. Treat
  a missing `composerData` row as "no percent signal available," not as an
  error.
- **Per-message `tokenCount` is unreliable.** Individual conversation turns
  are stored separately at `cursorDiskKV` key `bubbleId:<composerId>:<bubbleId>`,
  each with a `tokenCount: {inputTokens, outputTokens}` field. Every bubble
  sampled across a 400-message conversation had `{0, 0}`. `contextUsagePercent`
  on the composer record is the only field that reads as populated in
  practice; don't build anything on `tokenCount`.
- This composer store lives only in the **global** `state.vscdb`; the two
  per-workspace `state.vscdb` files checked on this machine had zero
  `composerData` rows. Only read the global one.

`contextUsagePercent` is a percentage, not a token count — it cannot be
plugged into `TokenUsageRecord.total_tokens`-based accounting (weighted burn
rate, token-tier idle-compact thresholds) without a known context-window
size, which Cursor doesn't expose either. It is being wired in as a parallel
percent-based signal (`TokenUsageRecord.context_pct`,
`SessionSummary.last_known_context_pct`) rather than converted into a fake
token count — `uw-policy::should_idle_compact` gets an independent
percent-threshold path (`IdleCompactConfig.percent_threshold_pct`, default
`65.0`) instead of estimating tokens from a percentage.

## Compaction TODO

Status: usage polling is implemented. Native Cursor compaction delivery remains disabled
until a supported, owner-preserving send path is verified. The provider-neutral fallback
can deliver through T3Code when discovered lineage resolves the Cursor session to a
T3Code-owned ancestor.

- [x] Keep the Cursor adapter's `can_trigger_compaction` and `headless_resume`
  capabilities false until delivery and identity are proven.
- [x] Move shared compaction text and command-prefix normalization into
  `uw-core::compaction`; adapters provide their command syntax.
- [x] Verify Cursor's command names. `/summarize` is the documented command and
  `/compress` is an alias.
- [x] Verify that `preCompact` is observational. It reports `trigger`, context usage,
  token counts, window size, and message counts, but cannot trigger or modify the
  compaction.
- [x] Inspect the installed CLI. `cursor-agent` version `2026.09.08-6caf4ff` exposes
  `--resume [chatId]`, `persist`, `persist attach`, and `persist stop`. Its help does
  not document sending a new prompt to an existing chat by chat ID.
- [ ] Run a disposable Cursor CLI experiment with a real chat: record the chat ID,
  start it through `persist`, attach and detach it, then test whether a second
  `cursor-agent --resume <chat-id>` process can deliver `/compress` without creating
  a fork. Record the exact command, session IDs, and transcript evidence here.
- [ ] Test the Cursor SDK local-agent path. Confirm that `Agent.resume(agentId)` plus
  `agent.send(message)` targets the same local agent and can be safely wrapped by a
  small long-lived helper process. The Rust adapter must communicate with that helper
  through a narrow authenticated local protocol, not depend on undocumented Cursor
  internals.
- [ ] Test Cursor's Agent Client Protocol (ACP) as a possible owner-preserving route.
  Establish whether the protocol can attach to an existing Cursor chat or only create
  a new agent process.
- [ ] Add a provider-specific `CursorCompactionTransport` only after one route passes
  the experiment. It must prove session ownership, require an idle session, and use
  the claim-before-act queue.
- [ ] Add red/green tests for exact `/compress` delivery, duplicate-prefix removal,
  session identity preservation, active-session rejection, and terminal failure with
  no blind retry.
- [ ] Add Cursor `preCompact` hook ingress as observation only. Store context usage
  and compaction trigger data when the session identity can be matched; do not use the
  hook to request or rewrite compaction.
- [ ] Enable `can_trigger_compaction`, `can_observe_compaction`, or token reporting
  only after each capability has a verified transport and matching tests.

Decision gate: if CLI, SDK, and ACP experiments cannot deliver to an existing Cursor
session without forking it, leave Cursor compaction unsupported and expose the reason
through the API rather than adding a best-effort send path.

Cursor's current dashboard usage endpoint is the Connect-RPC method
`POST https://api2.cursor.sh/aiserver.v1.DashboardService/GetCurrentPeriodUsage`.
It accepts `{}` and returns the current billing cycle in `billingCycleStart` and
`billingCycleEnd` (Unix milliseconds, commonly encoded as strings). The two
provider usage bars are `planUsage.autoPercentUsed` and
`planUsage.apiPercentUsed`.

The adapter uses either a bearer access token or an already-issued
`WorkosCursorSessionToken` cookie. It does not attempt login, token exchange, or
browser automation. Explicit configuration uses `UW_CURSOR_ACCESS_TOKEN`,
`CURSOR_AUTH_TOKEN`, or `UW_CURSOR_SESSION_COOKIE`. When those are absent, the
daemon reads Cursor's local IDE `cursorAuth/accessToken` from
`~/.config/Cursor/User/globalStorage/state.vscdb`, then falls back to the Cursor
Agent CLI token in `~/.config/cursor/auth.json`. `CURSOR_STATE_DB` and
`CURSOR_CLI_AUTH_FILE` override those paths.

Cursor's bars are monthly billing-pool percentages, not duration-keyed rolling
windows. They are stored as `WindowKind::Custom("auto")` and
`WindowKind::Custom("api")`, both using `billingCycleEnd` as their reset. Cursor does
not expose a supported external message-delivery or resume channel through this native
integration, so the Cursor adapter cannot deliver a compaction request by itself. A
configured meta-harness fallback can deliver through an owner such as T3Code.
Cursor's CLI does expose `/compress`, with `/summarize` as the canonical command and
`/compress` as an alias. That is useful syntax for a future interactive delivery adapter,
but it is not an API that usagewindow can send to a running session today.

Cursor's current hooks also expose an observational `preCompact` event. It reports whether
the compaction was automatic or manual and can return a `user_message` shown to the user,
but the hook cannot block or modify the compaction. We therefore do not treat it as a
trigger or a way to inject the preserved-state instructions. A future Cursor integration
should investigate a supported CLI/session transport and hook installation before claiming
`can_trigger_compaction` or `can_inject_at_session_start`.

The usage pace estimate uses the last 24 hours of samples. This is deliberately
different from the default short lookback used by rolling-window providers.

Sources verified 2026-09-20:

- Cursor dashboard usage documentation: `GetCurrentPeriodUsage` and the
  `planUsage` fields are documented in the public Cursor integration research
  used for this adapter.
- Cursor's usage-limits documentation confirms the two current monthly pools and
  billing-cycle reset behavior.
- Cursor CLI slash-command documentation confirms `/summarize` and its `/compress` alias:
  https://prod.cursor.com/docs/cli/reference/slash-commands
- Cursor hook documentation confirms `preCompact` is observational and cannot modify
  compaction:
  https://prod.cursor.com/docs/hooks
- Cursor's TypeScript SDK documents resumable local agents through `Agent.resume()` and
  follow-up messages through `agent.send()`:
  https://cursor.com/docs/sdk/typescript
- Cursor's Agent Client Protocol (ACP) documentation is the reference for testing a
  structured client connection:
  https://prod.cursor.com/docs/cli/acp
