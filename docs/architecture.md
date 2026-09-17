# usagewindow — architecture

Full plan: see the phoe-nix repo's plan record for this project (the machine this was
designed on). This file is the in-repo copy subagents and contributors should work from.

## Crate layout

- `uw-core` — public library API. Domain types + the `HarnessAdapter` trait. Zero
  harness-specific code. Other applications depend on this crate to query resume state
  and usage without pulling in adapters or a storage backend.
- `uw-adapters` — `HarnessAdapter` impls: `claude_code`, `codex` (both full parity —
  a v1 requirement, not staged), `generic_hook` (config-driven fallback; manual
  resume-marking works with zero adapter code, only auto-resume/auto-status need one).
- `uw-policy` — pure functions, no I/O: threshold resolution (provider→model→session
  override chain, most-specific-field-wins), burn-rate/exhaustion projection, the
  burn-scaled near-limit trigger, the opportunistic idle-compact trigger, and the reseed
  auto-trigger. Fully unit-testable against synthetic sample sequences.
- `uw-store` — SQLite (`rusqlite`, WAL) persistence. One writer owns the connection;
  readers (CLI status, web UI) read concurrently.
- `uw-daemon` — systemd-managed long-running process: polling loop, compaction-queue
  flush tick, resume scheduler, hook listeners. CLI writes require the daemon running
  (thin client over local socket/HTTP); CLI reads fall back to direct DB access when the
  daemon is down.
- `uw-cli` (`uw` binary) — thin consumer of the same `/api/*` surface the web UI uses.
- `uw-web` — axum server + embedded static UI assets.
- `uw-mcp` — thin MCP server exposing read-only `get_usage`/`get_resume_state` and
  `request_compaction` tools, built from `uw-core`.

## Domain model (Phase 2 target)

```
Provider          = ClaudeCode | Codex | Cursor | Gemini | Other(String)
ModelId(String)   // provider-scoped, free-form
AccountId(String)
SessionId(String) // harness-assigned STABLE id — survives compaction, unlike an
                   // internal/ephemeral session id. Sanitized to [A-Za-z0-9_-].

WindowKind = FiveHour | SevenDay | WeeklyModel(ModelId) | WeeklySurface(String) | Custom(String)
WindowKey  = { provider: Provider, kind: WindowKind }

UsageWindowState = { pct: f32 (0-100, normalized at ingestion, never inferred from magnitude),
                      resets_at: Option<DateTime<Utc>>, severity: Option<Severity>,
                      active: bool, scope: Option<AccountId> }
Severity = Notice | Closing | Compact | Exceeded

UsageSample = { at, fetched_at: Option<DateTime<Utc>>, source: ProviderReported | LocalEstimate,
                 provider, account: Option<AccountId>, windows: HashMap<WindowKey, UsageWindowState>,
                 credits: Option<CreditBalance> }
// effective_at = fetched_at.unwrap_or(at). isStale: 5min tolerance. acceptReading refuses
// anything older-in-effective-time or a same-window-instance pct that regressed.

SessionSummary = { id: SessionId, harness: Provider, model: Option<ModelId>,
                    account: Option<AccountId>, first_seen, last_seen,
                    last_known_token_count: Option<u64>, stopped_reason: Option<StopReason>,
                    resume_marker: Option<ResumeMarker> }
StopReason = UsageLimit { window: WindowKey } | UserQuit | Crashed | Unknown

ResumeMarker = { session_id, reason: AutoDetectedLimit | ManuallyMarked,
                  resume_at: Option<DateTime<Utc>>, created_at, status }
ResumeStatus = Pending | Scheduled | Fired | Cancelled | Failed(String)

CompactionRequest = { id: Uuid, session_id, kind: CompactionKind, prompt: String,
                       reason: String, status: CompactionStatus, created_at }
CompactionKind = AskNearLimit | OpportunisticIdle | AltModelReseed
CompactionStatus = Pending | Sending | Sent | Failed(String) | Cancelled
// claim-before-send: UPDATE ... SET status='sending' WHERE id=? AND status='pending';
// 0 rows affected => already claimed by someone else, fail closed, never retry blindly
// (compaction delivery is destructive/non-idempotent).

ThresholdProfile = { notice_pct, closing_pct, compact_pct, plan_pressure_pct,
                      plan_pressure_min_tokens, burn_multiplier, idle_compact: IdleCompactConfig,
                      reseed_auto: ReseedAutoConfig, keepalive: Option<KeepaliveConfig>,
                      overhead_pct, reask_delta_pct, reask_max_per_epoch }
ThresholdScope = { provider: Provider, model: Option<ModelId>, session: Option<SessionId> }
// Resolution: session > model > provider > global default, per FIELD (partial overrides,
// not whole-profile replacement). Store overrides as (scope_kind, scope_value, field) rows.
```

## HarnessAdapter trait (Phase 4 target)

```
trait HarnessAdapter {
    fn provider(&self) -> Provider;
    async fn fetch_usage(&self, account: Option<&AccountId>) -> Result<UsageSample>;
    async fn detect_stop(&self, session_id: &SessionId) -> Result<Option<StopReason>>;
    async fn emit_status(&self, session_id: &SessionId, status: StatusEvent) -> Result<DeliveryOutcome>;
    async fn ask_compaction(&self, session_id: &SessionId, req: &CompactionRequest) -> Result<DeliveryOutcome>;
    async fn resume_session(&self, session_id: &SessionId) -> Result<()>;
    fn supports_seed_new_session(&self) -> bool { false }
    async fn seed_new_session(&self, seed: &SeedContext) -> Result<SessionId> { Err(Unsupported) }
}
StatusEvent = WillAutoResumeAt(DateTime<Utc>) | CompactedFromTo{before_tokens,after_tokens}
            | AutoResumeCanceled | Custom(String)
DeliveryOutcome = Delivered | QueuedForNextIdle | Unsupported
```

### Claude Code adapter (research already complete — see docs/research-claude-code.md)

- `fetch_usage`: `GET https://api.anthropic.com/api/oauth/usage`, headers
  `Authorization: Bearer <token from ~/.claude/.credentials.json>`,
  `anthropic-beta: oauth-2025-04-20`, `User-Agent: claude-code/<version>`. Trust
  `five_hour.utilization`/`resets_at` and `seven_day.utilization`/`resets_at` verbatim,
  clamp [0,100]. 120s local cache; stale-tolerant on transient failure (429/5xx/network);
  hard-fail on 401/403 (do not trust stale cache).
- `emit_status`/`ask_compaction`: `SessionStart:compact` hook injecting `additionalContext`
  is the ONLY confirmed "inject text into context" channel. `PostCompact` hook output is
  silently rejected (not in Claude Code's allowed `hookEventName` union). `Stop` never
  fires after a `/compact` (no model turn runs). Compaction = sending the literal
  `/compact <instructions>` message when the agent is idle (root-only slash command, no
  other API). Nothing auto-continues a task after compaction — after grading confirms
  token count dropped, send a second continuation message (one of only two allowed
  unsolicited-send call sites in the whole system: the `/compact` itself, and this
  continuation).
- `resume_session`: `claude --resume <session_id>` (or `claude -r`) in the session's
  recorded working directory.
- Session identity: the harness-assigned id, NOT Claude Code's internal `session_id`
  (which can change under compaction).

### Codex adapter (Phase 1 research spike required — no equivalent research done yet)

Full v1 parity is required (not staged behind Claude Code). Before Phase 4 writes real
logic, Phase 1 must establish, for Codex (`codex` CLI, confirmed installed:
`codex-cli 0.154.0`):
- What usage/quota API or local state exposes rate-limit percentage and reset time
  (equivalent of Claude Code's `/api/oauth/usage`).
- Whether Codex has a hook surface analogous to Claude Code's (`codex --help` shows
  `.codex/hooks.json`-style hook config already in use on this machine — investigate
  `post_tool_use`/`session_start`/`user_prompt_submit`/`stop` hooks and what each can
  inject/reject, mirroring the Claude Code gotcha list above).
- Codex's actual compaction mechanism, if any (is there a `/compact`-equivalent, or is
  compaction automatic/opaque?).
- `codex resume <session-id>` / `codex exec resume` — confirm this is the real resume
  primitive (the `codex --help` output lists a `resume` subcommand: "Resume a previous
  interactive session (picker by default; use --last to continue the most recent)" and
  `codex exec resume` too) — document exact invocation shape and whether a specific
  session id (not just "last") can be targeted headlessly.
- Whether Codex supports seeding a new session with prior context (`supports_seed_new_session`).

Write findings to `docs/research-codex.md` in the same structure as
`docs/research-claude-code.md` before Phase 4 begins.

## Trigger policies (Phase 3 target)

- **Burn-rate-scaled near-limit trigger**: `effective_threshold = 100 -
  (burn_pct_per_hour * burn_multiplier)`; fire when `current_pct >=
  max(effective_threshold, closing_pct)`. Burn rate: `(last.pct - first.pct) /
  hours_between(first.t, last.t)`, where `first` = the last point at-or-before
  `now - 30min` (anchoring strictly inside the window risks a zero-length span exactly
  when the answer should be "flat"). Ask-only (never auto-acts) for the compaction
  decision itself; the resulting projected-exhaustion timing IS allowed to drive the
  resume-scheduling lead-time math (that's an auto-resume decision, in scope for req 1).
- **Resume lead-time**: `usable_pct = remaining_pct - cache_write_pct; raw_minutes =
  usable_pct / burn_pct_per_minute; lead = raw_minutes * (1 - overhead_pct/100)`, clamped
  to a configurable `[min_lead_minutes, max_lead_minutes]`. `overhead_pct` (default
  suggestion 30) is a safety margin on the lead time, NOT a usage-percentage trigger —
  keep it a distinct config field from `compact_pct`/`plan_pressure_pct`.
- **Opportunistic idle-compact** (auto-fires, no asking): tiered table
  `Vec<{window_size_floor: u64, token_threshold: u64}>` sorted ascending, pick the
  highest floor `<=` the session's context window size. Defaults: 100k tokens for the
  ≤200k tier, 200k tokens for the 201k+ tier — open-ended, a 1M-window model gets its
  own tier via config with no code change. Fires when `idle_for >= cache_ttl - margin`
  (cache_ttl: flat per-provider default, e.g. 300s for Anthropic) AND
  `last_known_token_count >= tier.token_threshold`.
- **Reseed auto-trigger** (opt-in, default off, distinct from idle-compact): idleness
  gate (same as above) + a minimum dry-session token floor + a cost-comparison gate
  (estimated $ cost of reseeding via the cache-cost model vs. estimated cost of waiting
  for a natural window reset) + a per-session cooldown. Its own explicit enable flag
  even when auto-triggering is globally on, since it starts a genuinely new session
  (destructive to continuity in a way idle-compact is not).
- **Cache keepalive** (opt-in per session, default off): drip-feed turns near
  `cache_ttl` (fires before idle-compact/reseed thresholds would), tagged with a literal
  marker `[[uw-keepalive]]` that reseed's transcript summarization and any cost/analytics
  view explicitly filter out. Hard per-session daily cap (config) as a cost guard.

All numbers above are `ThresholdProfile` fields, overridable per provider/model/session.

## Persistence (Phase 2 target)

SQLite via `rusqlite`, WAL mode. Schema sketch:

```sql
usage_samples(id, provider, account, window_kind, window_scope_value, pct, resets_at,
              severity, active, source, at, fetched_at, credits_json)
  -- one row per (sample, window) — flattened, not JSON-blobbed, so burn-rate queries
  -- are plain range scans: SELECT pct, at FROM usage_samples WHERE provider=? AND
  -- window_kind=? AND at > ? ORDER BY at

sessions(id PRIMARY KEY, harness, model, account, first_seen, last_seen,
         last_known_token_count, stopped_reason, stopped_window_kind, reseeded_from)

resume_markers(session_id PRIMARY KEY REFERENCES sessions, reason, resume_at,
               created_at, status, status_detail)

compaction_requests(id PRIMARY KEY, session_id REFERENCES sessions, kind, prompt,
                     reason, status, created_at, updated_at)

threshold_overrides(id PRIMARY KEY, scope_kind, scope_value, field, value_json, updated_at)

idle_reseed_summaries(id PRIMARY KEY, session_id, source_model, summary_text,
                       token_count_before, token_count_after, created_at)

keepalive_config(session_id PRIMARY KEY, enabled, last_ping_at)
```

Indexes: `(provider, window_kind, at)` on `usage_samples`; `(session_id, status)` on
`compaction_requests` and `resume_markers`. Retention: prune `usage_samples` past a
configurable age (default 30 days), keep `sessions`/latest-per-window derived state.

## CLI / API / Web UI (Phases 6-7 targets)

CLI (`uw`): `status`, `sessions list/show`, `resume <guid> [--at]`, `resume cancel <guid>`,
`compact ask/status <guid>`, `reseed <guid> --model <m>`, `keepalive enable/disable <guid>`,
`thresholds get/set <scope> <field> <value>`, `daemon status/run`. All support `--json`.
CLI is a thin wrapper over `/api/*`, the same surface the web UI consumes.

Web UI routes: `/` (usage tiles + last-updated timestamps), `/sessions`, `/sessions/:id`
(history, resume controls incl. paste-GUID box, compaction log, reseed lineage),
`/providers/:id[/models/:model]` (threshold editors), `/thresholds` (global override
table), `/compactions` (queue view), `/settings`.

## Alt-model reseed flow (Phase 8 target)

1. Trigger: manual (CLI/UI action) or the reseed auto-trigger policy (§ above).
2. Extract context: adapter-specific transcript export (read-only, works even on a
   stopped/limited session — e.g. Claude Code's `~/.claude/projects/**/*.jsonl`).
   Explicitly exclude `[[uw-keepalive]]`-tagged turns.
3. Summarize via a cheap model: `uw-core::Summarizer` trait, one built-in
   OpenAI-compatible chat-completions implementation (works against Ollama-local,
   OpenRouter, Groq, etc. via config: base URL + model + optional key). Prompt template
   mirrors the paseo-smart-session compact template: preserve goal/step-in-progress/
   decisions-and-why/approaches-tried; discard raw file contents/tool output already
   acted on.
4. Persist to `idle_reseed_summaries` with before/after token counts.
5. Seed the new session: if `adapter.supports_seed_new_session()`, launch directly with
   the summary as seed context; else (Claude Code today) start a normal new session and
   inject the summary via its first `SessionStart` hook — same mechanism as compaction
   delivery, fired at session-start instead of mid-session.
6. Link: `sessions.reseeded_from` FK for UI lineage display.

## Cache keepalive (Phase 8 target)

See trigger policy above. Delivery reuses the same idle-only delivery channel as
compaction-ask. UI/CLI surface a running per-session keepalive-turn count + estimated $
cost so it's an informed, visible tradeoff.

## NixOS packaging (Phase 9 target)

Lives in the `phoe-nix` repo, not here — `scout-modules/usagewindow/flake.nix` (modeled on
`scout-modules/paseo/flake.nix`: consumes `packages.${system}.default` from this repo's
flake, exposes `packages.${system}.scout` as CLI-on-PATH, a `flakelets.default` with
`port`/`dataDir`/`user`/`group`/`configFile` options driving a systemd unit running
`uw-daemon`), `modules/usagewindow.nix` (thin `options.v0id.usagewindow.*` wrapper), and a
`hosts/void.nix` entry. This repo just needs to expose `packages.${system}.default` from
its own `flake.nix` once the workspace builds.

## kasetto integration (Phase 9 target)

`kasetto.yaml` in this repo's root: `agent: [claude-code, codex]` + a `destination:`
escape hatch, a `usagewindow-status` skill (teaches the status-string vocabulary and the
keepalive marker), a `uw-mark-resume` command, and the `uw-mcp` server as an MCP entry.
Source content lives under `agent-integration/`. `kasetto.lock` committed once authored.

## Phases (this repo's own build order — 9 total)

1. Repo scaffolding (done) + Codex research spike (docs/research-codex.md).
2. `uw-core` domain types + `uw-store` SQLite schema/migrations, TDD.
3. `uw-policy`: threshold resolution, burn-rate/projection math, burn-scaled trigger,
   opportunistic idle-compact, reseed auto-trigger — pure, fully unit-tested.
4. `uw-adapters`: trait + Claude Code adapter (full) + Codex adapter (full) + generic/stub.
5. `uw-daemon`: polling, compaction queue delivery/claim, hook wiring both harnesses,
   idle auto-compact end-to-end, resume scheduler + respawn both harnesses.
6. `uw-cli` + the shared `/api/*` JSON surface.
7. `uw-web`: UI routes and embedded assets.
8. Alt-model reseed flow (manual + auto-trigger policy) + cache keepalive mode.
9. This repo exposes `packages.${system}.default`; phoe-nix packaging + `uw-mcp` + kasetto config.

Each phase must leave `cargo test` and `cargo clippy --all-targets` green before merge.
