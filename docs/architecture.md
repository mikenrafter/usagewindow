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

// WindowKind is keyed by duration, not a harness-specific label: Claude Code's
// "five_hour"/"seven_day" and Codex's windowDurationMins:300/10080 are the SAME
// underlying concept (a rolling N-minute quota window) and must map to one Rolling
// variant so a provider-side plan change (a new window length) doesn't silently
// mis-key samples under a positional primary/secondary guess. Named variants remain
// for windows that are genuinely not duration-keyed.
WindowKind = Rolling { minutes: u32 } | WeeklyModel(ModelId) | WeeklySurface(String) | Custom(String)
WindowKey  = { provider: Provider, kind: WindowKind }

// `severity` is NOT stored — it's policy output (depends on user-editable thresholds),
// computed at read time by uw-policy from `pct` + the resolved ThresholdProfile. Storing
// it per-sample would go stale the moment thresholds change. The only provider-reported
// fact worth persisting is `exceeded`, a plain bool.
UsageWindowState = { pct: f32 (0-100, normalized at ingestion, never inferred from magnitude),
                      resets_at: Option<DateTime<Utc>>, exceeded: bool,
                      active: bool, scope: Option<AccountId> }
Severity = Notice | Closing | Compact | Exceeded  // uw-policy output type, not persisted

UsageSample = { at, fetched_at: Option<DateTime<Utc>>, source: ProviderReported | LocalEstimate,
                 provider, account: Option<AccountId>, windows: HashMap<WindowKey, UsageWindowState>,
                 credits: Option<CreditBalance> }
// effective_at = fetched_at.unwrap_or(at). isStale: 5min tolerance. acceptReading refuses
// anything older-in-effective-time or a same-window-instance pct that regressed.

// cwd/state_path/context_window_size/launch_mode/pid are load-bearing, not decorative:
// resume needs a working directory and a launch strategy, idle-compact's tier lookup
// needs the model's real context size (never hardcode 200k), and liveness needs SOME
// signal that a session is still running vs. abandoned. `launch_mode` records how THIS
// session was started (Interactive | Headless) so resume can match it — a systemd unit
// has no terminal, so an auto-resume of a session the user was driving interactively
// must default to Headless and let the user `claude attach`/`codex agents` in manually;
// only a session that was already Headless can be silently re-launched Headless.
SessionSummary = { id: SessionId, harness: Provider, model: Option<ModelId>,
                    account: Option<AccountId>, first_seen, last_seen,
                    cwd: String, state_path: Option<String>,
                    context_window_size: Option<u64>, last_known_token_count: Option<u64>,
                    launch_mode: LaunchMode, pid: Option<u32>,
                    stopped_reason: Option<StopReason>, resume_marker: Option<ResumeMarker>,
                    reseeded_from: Option<SessionId> }
LaunchMode = Interactive | Headless
StopReason = UsageLimit { window: WindowKey } | UserQuit | Crashed | Unknown

// Claude Code 2.1.267 was verified with a disposable real session: hook session_id,
// transcript-filename UUID, SessionStart(source=compact), and explicit --resume output
// retained one UUID across a 62,299 -> 721 token manual compaction. The fixture parser
// and hook ingress fail closed on disagreement. Keep superseded_by for a later harness
// version that violates this verified invariant. Codex is also verified clean:
// session_meta.id == threads.id, stable across resume/fork. See both research notes.

// One resume attempt per (session, quota-window-instance), not one-ever-per-session: a
// session can legitimately hit its limit, get resumed, and hit it again next window.
ResumeMarker = { id: Uuid, session_id, reason: AutoDetectedLimit | ManuallyMarked,
                  resume_at: Option<DateTime<Utc>>, created_at, status }
ResumeStatus = Pending | Scheduled | Fired | Cancelled | Failed(String)

// AgentRequested, OpportunisticIdle, and AltModelReseed go through adapter.compact()
// and the claim-before-send queue below — they're destructive. AskNearLimit rows are
// an audit log of adapter.advise() calls (non-destructive, not claimed/queued the same
// way) so the /compactions UI view has one place to see advisory requests too.
CompactionRequest = { id: Uuid, session_id, kind: CompactionKind, prompt: String,
                       reason: String, status: CompactionStatus, created_at }
CompactionKind = AskNearLimit | AgentRequested | OpportunisticIdle | AltModelReseed
CompactionStatus = Pending | Sending | Sent | Failed(String) | Cancelled
// claim-before-send (AgentRequested/OpportunisticIdle/AltModelReseed only):
// UPDATE ... SET status='sending' WHERE id=? AND status='pending';
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

Claude Code and Codex diverge enough (Codex's compaction is automatic/opaque — there is
nothing to ask, per docs/research-codex.md) that a single `ask_compaction` method can't
honestly represent both. Split into a non-destructive `advise` (inject text, may be
ignored, never itself changes session state) and a destructive `compact` (goes through
the claim-before-send queue, only called for adapters that report `can_trigger_compaction`).
A `Capabilities` struct — returned once per adapter, not per call — lets `uw-policy` (Phase
3) and `uw-daemon` (Phase 5) decide up front which policies apply to which harness, instead
of enqueueing `CompactionRequest`s an adapter can only ever fail:

```
trait HarnessAdapter {
    fn provider(&self) -> Provider;
    fn capabilities(&self) -> Capabilities;
    async fn fetch_usage(&self, account: Option<&AccountId>) -> Result<UsageSample>;
    async fn detect_stop(&self, session_id: &SessionId) -> Result<Option<StopReason>>;
    async fn emit_status(&self, session_id: &SessionId, status: StatusEvent) -> Result<DeliveryOutcome>;
    /// Non-destructive: inject advisory text (near-limit pressure, a keepalive ping).
    /// The agent may ignore it entirely. Never queued/claimed — fire-and-forget best effort.
    async fn advise(&self, session_id: &SessionId, text: &str) -> Result<DeliveryOutcome>;
    /// Destructive: only called when `capabilities().can_trigger_compaction`. Goes
    /// through the claim-before-send queue (see CompactionRequest below).
    async fn compact(&self, session_id: &SessionId, req: &CompactionRequest) -> Result<DeliveryOutcome>;
    /// Resume using `session.launch_mode` (Interactive sessions resume Headless by
    /// default — see SessionSummary.launch_mode above — unless explicitly overridden).
    async fn resume_session(&self, session: &SessionSummary) -> Result<()>;
    async fn seed_new_session(&self, mode: SeedMode, seed: &SeedContext) -> Result<SessionId> { Err(Unsupported) }
}

Capabilities = {
    can_trigger_compaction: bool,   // Claude Code: true (/compact). Codex: true (app-server compact).
    can_advise_mid_turn: bool,      // Claude Code: true (Stop's additionalContext rides the live turn).
    can_inject_at_session_start: bool, // Claude Code: true (SessionStart:compact). Codex: true (SessionStart, any source).
    can_observe_compaction: bool,   // both true: PreCompact/PostCompact (Codex) or transcript
                                     // token-count watch (Claude Code) can detect a compaction landed.
    reports_token_counts: bool,     // Claude Code: true (transcript usage fields). Codex: UNVERIFIED,
                                     // see docs/research-codex.md open item — false until confirmed,
                                     // and idle-compact/reseed-auto/keepalive must degrade gracefully
                                     // (skip, don't guess) for any adapter reporting false here.
    headless_resume: bool,          // both true (claude --resume / codex exec resume <id>).
    seed_modes: &[SeedMode],        // see below.
}
SeedMode = InitialPrompt          // fresh session, summary as the literal launch prompt —
                                    // `claude -p "<summary>"` and `codex exec "<summary>"` are the
                                    // SAME primitive; no hook workaround needed for a BRAND NEW
                                    // session (the hook-injection gotchas only apply to injecting
                                    // into an EXISTING/resumed session's context).
         | ForkWithHistory          // Codex only: `codex fork <session-id> <prompt>` — carries the
                                    // ENTIRE prior conversation forward, so this does not shrink
                                    // context and is not the default reseed strategy; expose it as
                                    // a distinct capability for a future "continue under a new id"
                                    // feature, not for the reseed-to-save-tokens flow.
SeedContext = { from_session: Option<SessionId>, summary: String, model: ModelId, cwd: String }

StatusEvent = WillAutoResumeAt(DateTime<Utc>) | CompactedFromTo{before_tokens,after_tokens}
            | AutoResumeCanceled | Custom(String)
DeliveryOutcome = Delivered | QueuedForNextIdle | Unsupported
```

#### Destructive compaction fallback

Adapters may be decorated with a `FallbackCompactionAdapter`. It delegates every
operation except `compact` to the provider-native adapter. Compaction attempts the
native route first; only an error then sends the same queued `/compact` prompt through
the configured meta-harness adapter, such as T3Code or Paseo. If every route fails, the
queue row is marked failed with both errors. This keeps the fallback provider-neutral:
future providers use the same decorator and do not need to know whether the owner is
Paseo, T3Code, or another meta-harness.

Policy implication (Phase 3): the near-limit "ask" trigger only calls `advise` when
`can_advise_mid_turn` (or degrades to `can_inject_at_session_start`, queued for the next
turn) — for an adapter with neither, or with `can_trigger_compaction: false` and no
advise channel, the trigger's only remaining action is checkpointing state to
`state_path` and logging the projected exhaustion time; it must not synthesize a
`CompactionRequest` the adapter has no way to honor. `uw-mcp`'s `request_compaction` tool
(Phase 9) needs the same gate — for a `can_trigger_compaction: false` adapter it returns
an explicit "not supported for this harness" result, not a silently-dropped request.

### Claude Code adapter (research already complete — see docs/research-claude-code.md)

- `fetch_usage`: `GET https://api.anthropic.com/api/oauth/usage`, headers
  `Authorization: Bearer <token from ~/.claude/.credentials.json>`,
  `anthropic-beta: oauth-2025-04-20`, `User-Agent: claude-code/<version>`. Trust
  `five_hour.utilization`/`resets_at` and `seven_day.utilization`/`resets_at` verbatim,
  clamp [0,100]. 120s local cache; stale-tolerant on transient failure (429/5xx/network);
  hard-fail on 401/403 (do not trust stale cache).
- `advise` (mid-turn, near-limit ask): `Stop`'s `additionalContext` is the confirmed
  channel — it rides the agent's own live turn (verified empirically: an echo-marker
  test showed the model acting on it in the same turn), so this is `can_advise_mid_turn`.
  Check `stop_hook_active`/`CLAUDE_CODE_STOP_HOOK_BLOCK_CAP` to avoid double-asking into
  a hook-extended turn.
- `emit_status` for a FRESH or POST-COMPACTION context (no live turn to ride): the
  `SessionStart:compact` hook injecting `additionalContext` is the confirmed channel here
  — a different situation from `advise` above, not a contradiction: `Stop` fires only
  mid-turn on an active session, `SessionStart:compact` fires only at the start of a
  session Claude Code just compacted. `PostCompact` hook output is silently rejected (not
  in Claude Code's allowed `hookEventName` union) — this is `can_inject_at_session_start:
  true` via `SessionStart`, `can_observe_compaction` via watching token count, NOT via
  `PostCompact`'s own output.
- `compact`: sending the literal `/compact <instructions>` message when the agent is idle
  (root-only slash command, no other API) — `can_trigger_compaction: true`. `Stop` never
  fires after a `/compact` runs (no model turn). Nothing auto-continues a task after
  compaction — after grading confirms token count dropped, send a second continuation
  message (one of only two allowed unsolicited-send call sites in the whole adapter: the
  `/compact` itself, and this continuation).
- `resume_session`: `claude --resume <session_id>` (or `claude -r`) in `session.cwd`.
  Launches Headless (`--print`/background) by default per `session.launch_mode`; an
  `Interactive`-launched session resuming Headless is a known, accepted UX tradeoff for
  auto-resume (the user reattaches manually) — see `LaunchMode` above.
- `seed_new_session(InitialPrompt, ...)`: `claude -p "<summary>"` in `seed.cwd` — this is
  a brand-new session, so none of the hook-injection gotchas above apply; an initial
  prompt on the command line is the normal, unproblematic path. No `ForkWithHistory` mode
  (not investigated / no evidence of an equivalent CLI primitive for Claude Code).
- Session identity: the harness-assigned UUID is verified stable across manual
  compaction and explicit headless resume on Claude Code 2.1.267. Fail closed if the
  transcript filename, transcript records, or hook payload disagree. See
  `docs/research-claude-code.md` for the exact evidence and rerunnable probe.

### Codex adapter (research complete — see docs/research-codex.md)

- `fetch_usage`: via `codex app-server`'s `account/rateLimits/read`, not a raw HTTP
  endpoint. Response gives `primary`/`secondary` windows with explicit
  `windowDurationMins` (e.g. 300/10080) — map to `WindowKind::Rolling{minutes}` by that
  duration field, never by positional primary/secondary guessing (a plan change on
  OpenAI's side must not silently mis-key samples). Also carries `rateLimitReachedType`
  and `planType` — feed `rateLimitReachedType` into `StopReason::UsageLimit` detection.
- `Capabilities`: `can_trigger_compaction: true` — the app-server exposes the native
  `thread/compact/start` method. This is not a `/compact` user message and does not
  invoke `codex exec resume`. `can_advise_mid_turn: false`
  — `Stop` has no additional-context field (expects JSON, can only ask Codex to continue
  via `decision:"block"`, which is a different, more invasive primitive than Claude
  Code's passive `additionalContext`). `can_inject_at_session_start: true` — `SessionStart`
  supports `additionalContext`, with a `source` matcher of `startup|resume|clear|compact`;
  use `source:"compact"` to inject state/status right after Codex's own automatic
  compaction, mirroring Claude Code's `SessionStart:compact` role. `can_observe_compaction:
  true` via `PreCompact`/`PostCompact` hooks (their own text output is ignored, but their
  firing is itself the observable signal, with a `manual|auto` matcher). `reports_token_counts:
  false` **pending verification** — no confirmed source for token counts or context-window
  size was found; idle-compact/reseed-auto/keepalive must skip (not guess) for Codex until
  a real source is confirmed (check `codex exec --json` event stream and hook payloads).
  `headless_resume: true`. `seed_modes: [InitialPrompt, ForkWithHistory]`.
- Since `can_advise_mid_turn` is false, the near-limit
  policy's only available action for Codex is: checkpoint state (if/when a state-file
  convention exists for Codex — TBD, no equivalent of Claude Code's paseo-style state
  file researched yet) and log the projected exhaustion time. This is a real, accepted
  capability gap, not a bug to route around.
- `compact`: `thread/compact/start` through the persistent app-server transport;
  `emit_status`/general status strings use the `SessionStart(source:"compact")` channel
  above.
- `resume_session`: `codex exec resume <session-uuid> "Continue from the saved state."`
  (headless, targets a specific id directly — no picker). In `session.cwd`.
- `seed_new_session(InitialPrompt, ...)`: `codex exec "<summary>"` — a blank new session
  with the summary as its literal initial prompt, the same primitive as Claude Code's
  `claude -p "<summary>"`.
- `seed_new_session(ForkWithHistory, ...)`: `codex fork <session-id> "<prompt>"` — carries
  the ENTIRE prior conversation forward. Available as a capability but NOT wired into the
  reseed-to-save-tokens flow (defeats its purpose); reserved for a possible future
  "continue this exact session under a new id" feature.
- Session identity: `session_meta.id` (rollout JSONL) == `threads.id` (state DB) — a
  stable UUID, confirmed unchanged across resume/fork. No `SessionId`-stability caveat
  needed for Codex (unlike Claude Code, see above).

### T3Code meta-harness adapter (research: `docs/research-t3code.md`)

- `resume_session`: send a native `thread.turn.start` command to T3Code's authenticated
  `POST /api/orchestration/dispatch` endpoint. The T3Code thread UUID is the adapter's
  session id. This preserves the T3Code owner process and avoids detached `claude
  --resume` branches.
- `Capabilities`: headless resume and destructive compaction delivery are true. T3Code
  compaction is the meta-harness fallback: it sends the queued `/compact` prompt through
  `thread.turn.start`, rather than claiming a native compaction RPC. Mid-turn advice,
  session-start injection, stop detection, usage polling, and token reporting remain
  false until T3Code exposes stable corresponding endpoints.
- Authentication is explicit configuration: an already-issued bearer token or browser
  session cookie. The adapter does not mint pairing credentials or automate a browser.
- The daemon registers this adapter only when `UW_T3CODE_URL` and exactly one of
  `UW_T3CODE_BEARER_TOKEN` or `UW_T3CODE_COOKIE` are configured.

### Hook ingress — `uw-daemon`'s `uw-hook` shim

Both harnesses deliver context only by executing a configured hook command and reading
its stdout/JSON — there is no long-lived "push" channel from a running session to
`uw-daemon`. This means Phase 4/5 need a small hook-shim binary (harness-invoked,
short-lived, one process per hook firing) that parses the harness's hook payload and
either serves it locally (fail-open, hard-timeout — a daemon that's down must never stall
a user's turn) or forwards to `uw-daemon` over the same local socket the CLI uses. The
phase-5 implementation lives as a `[[bin]]` in `uw-daemon` because it already depends on
`uw-adapters`; it logs JSON and returns a bounded-timeout fail-open response, with
event-specific routing deferred to a later phase. Installation/trust is a separate, real
step for Phase 9 (or earlier, whenever hooks first need to be live for manual testing):
Claude Code needs a `~/.claude/settings.json`
hooks entry per the `paseo-smart-session` reconciler pattern (own only what you installed,
never touch unrelated entries); Codex requires each hook definition to be trust-hashed in
`~/.codex/config.toml`'s `[hooks.state]` before it runs at all (`--dangerously-bypass-hook-trust`
only helps processes usagewindow itself spawns, not a user's own interactive sessions —
not a substitute for real installation).

## Trigger policies (Phase 3 target)

- **Burn-rate-scaled near-limit trigger**: `effective_threshold = 100 -
  (burn_pct_per_hour * burn_multiplier)`; fire when `current_pct >=
  max(effective_threshold, closing_pct)`. Burn rate: `(last.pct - first.pct) /
  hours_between(first.t, last.t)`, where `first` = the last point at-or-before
  `now - 30min` (anchoring strictly inside the window risks a zero-length span exactly
  when the answer should be "flat"). **Both `first` and `last` must be constrained to the
  same window instance** (same `resets_at`, within the existing rollover tolerance) —
  otherwise a window reset inside the 30-minute lookback span produces a nonsensical
  negative burn rate; treat a reset-spanning pair as no-data (return `None`), same as an
  insufficient-history case. Gated by `Capabilities` per the trait section above: only
  calls `adapter.advise()` when possible, degrades to checkpoint-and-log otherwise — never
  auto-acts on the compaction decision itself. The resulting projected-exhaustion timing
  IS allowed to drive the resume-scheduling lead-time math (an auto-resume decision, in
  scope for req 1, independent of whether the adapter can advise/compact at all).
- **Resume lead-time**: `usable_pct = remaining_pct - cache_write_pct; raw_minutes =
  usable_pct / burn_pct_per_minute; lead = raw_minutes * (1 - overhead_pct/100)`, clamped
  to a configurable `[min_lead_minutes, max_lead_minutes]`. `overhead_pct` (default
  suggestion 30) is a safety margin on the lead time, NOT a usage-percentage trigger —
  keep it a distinct config field from `compact_pct`/`plan_pressure_pct`. `cache_write_pct`
  is NOT a free-standing input — it's the output of the cache-write cost model ported from
  `paseo-smart-session` (`shared/cache-cost.ts`): a per-model $/MTok cache-write price
  table (longest-matching-prefix model-id lookup) combined with an empirically-learned
  $/plan-percent ratio (bucket observed plan-% deltas and $ deltas into hourly buckets;
  needs ≥3 usable buckets or falls back to a configured flat default, e.g. 1%). Add
  `cache_write_price_table: Vec<{model_prefix, usd_per_mtok}>` and
  `cache_write_fallback_pct: f32` to `ThresholdProfile` — this is the pricing-input gap
  the reseed cost-comparison gate below also depends on, so define it once here.
  A resume request skips the lead-time wait when the chat cache is still warm, when the
  current burn rate is zero or would take at least 30 minutes to exhaust the remaining
  quota, or when a rolling window is shorter than 30 minutes and the burn rate would not
  exhaust it within that window. The warm-cache approximation is five minutes since the
  session's last agent activity for both Claude Code and Codex until usagewindow can emit
  explicit cache-control markers. Only an uncached chat with projected exhaustion inside
  that horizon waits for the calculated lead time.
- **Opportunistic idle-compact** (auto-fires, no asking): tiered table
  `Vec<{window_size_floor: u64, token_threshold: u64}>` sorted ascending, pick the
  highest floor `<=` the session's context window size. Defaults: 100k tokens for the
  ≤200k tier, 200k tokens for the 201k+ tier — open-ended, a 1M-window model gets its
  own tier via config with no code change. Fires when `idle_for >= cache_ttl - margin`
  AND `last_known_token_count >= tier.token_threshold`. `cache_ttl` is per-provider AND
  per-tier, not one flat number: Anthropic's *default* prompt cache is ~5 minutes, but a
  session using the 1-hour cache beta needs its own `cache_ttl` value or this fires
  hours early on a cache that's still warm — add `cache_ttl_by_provider:
  HashMap<Provider, Duration>` (with a documented way to override per-session when a
  session opted into an extended-TTL cache) rather than a single constant. OpenAI's
  caching is automatic with no documented user-facing TTL — Codex's `cache_ttl` starts
  as an explicit "unresearched, treat as unknown, this policy does not fire for Codex
  until a real value is confirmed" rather than a guessed default.
- **Reseed auto-trigger** (opt-in, default off, distinct from idle-compact): idleness
  gate (same as above) + a minimum dry-session token floor + a cost-comparison gate
  (estimated $ cost of reseeding, using the SAME cache-write cost model as the resume
  lead-time formula above, vs. estimated cost of waiting for a natural window reset) + a
  per-session cooldown. Its own explicit enable flag even when auto-triggering is
  globally on, since it starts a genuinely new session (destructive to continuity in a
  way idle-compact is not). Requires `reports_token_counts: true` on the adapter (see
  `Capabilities`) — skipped entirely for an adapter that can't report token counts, since
  the cost-comparison gate has no inputs otherwise.
- **Cache keepalive** (opt-in per session, default off): drip-feed turns near
  `cache_ttl` (fires before idle-compact/reseed thresholds would, using the same
  per-provider/per-tier `cache_ttl` value above), tagged with a literal marker
  `[[uw-keepalive]]` that reseed's transcript summarization and any cost/analytics view
  explicitly filter out. Hard per-session daily cap (config) as a cost guard. Delivered
  via `adapter.advise()` — never claimed/queued (non-destructive, best-effort, like the
  near-limit ask).

All numbers above are `ThresholdProfile` fields, overridable per provider/model/session.

## Persistence (Phase 2 target)

SQLite via `rusqlite`, WAL mode. Schema sketch:

```sql
-- window_kind stores the Rolling{minutes}/WeeklyModel/WeeklySurface/Custom tag;
-- window_scope_value carries the variant's payload (minutes, model id, label). `severity`
-- is NOT a column (policy output, computed at read time) — `exceeded` is the only
-- provider-reported fact persisted per window-sample.
usage_samples(id, provider, account, window_kind, window_scope_value, pct, resets_at,
              exceeded, active, source, at, fetched_at, credits_json)
  -- one row per (sample, window) — flattened, not JSON-blobbed, so burn-rate queries
  -- are plain range scans: SELECT pct, at FROM usage_samples WHERE provider=? AND
  -- window_kind=? AND window_scope_value=? AND at > ? ORDER BY at
  -- (window_scope_value included in the burn-rate query key: two Rolling windows with
  -- different minute counts, or two WeeklyModel windows for different models, must
  -- never be scanned together.)

-- cwd/state_path/context_window_size/launch_mode/pid: see SessionSummary in the domain
-- model section above for why each is load-bearing (resume needs cwd+launch_mode,
-- idle-compact needs context_window_size, liveness needs pid).
sessions(id PRIMARY KEY, harness, model, account, cwd, state_path,
         context_window_size, last_known_token_count, launch_mode, pid,
         first_seen, last_seen, stopped_reason, stopped_window_kind,
         superseded_by, reseeded_from)

-- id PRIMARY KEY (not session_id): a session can be resumed more than once across
-- separate quota-window instances, so this must support more than one row per session.
resume_markers(id PRIMARY KEY, session_id REFERENCES sessions, reason, resume_at,
               created_at, status, status_detail)
-- UNIQUE INDEX on session_id WHERE status IN ('pending','scheduled') — at most one
-- ACTIVE marker per session at a time, but history of past markers is kept.

compaction_requests(id PRIMARY KEY, session_id REFERENCES sessions, kind, prompt,
                     reason, status, created_at, updated_at)

-- scope_value alone is ambiguous for a model-scoped row (ModelId is provider-scoped, e.g.
-- "opus" means nothing without knowing the provider) — provider is always present,
-- model_value only set when scope_kind = 'model' or 'session'.
threshold_overrides(id PRIMARY KEY, scope_kind, provider, model_value, session_value,
                     field, value_json, updated_at)
-- UNIQUE INDEX on (scope_kind, provider, model_value, session_value, field) — the whole
-- point of per-field overrides is that setting the same field twice at the same scope
-- replaces, not duplicates, the row.

idle_reseed_summaries(id PRIMARY KEY, session_id, source_model, summary_text,
                       token_count_before, token_count_after, created_at)

keepalive_config(session_id PRIMARY KEY, enabled, last_ping_at)
```

Indexes: `(provider, window_kind, window_scope_value, at)` on `usage_samples`;
`(session_id, status)` on `compaction_requests` and `resume_markers`; the partial-unique
index on `resume_markers` and the compound-unique index on `threshold_overrides` noted
above. Retention: prune `usage_samples` past a configurable age (default 30 days), keep
`sessions`/latest-per-window derived state.

`uw-store`'s public API is synchronous (`rusqlite::Connection` is not `Sync`) — a
dedicated writer thread owns the connection; `uw-daemon` wraps calls in
`tokio::task::spawn_blocking` rather than trying to share the connection across async
tasks directly. Readers (CLI direct-DB-read fallback, web UI queries) open their own
short-lived read connections under WAL, no shared state with the writer thread needed.

## CLI / API / Web UI (Phases 6-7 targets)

**Dependency direction**: `/api/*` request/response DTOs (plain serde types, distinct
from the internal domain model where they need to be — e.g. a `SessionListItem` view
type, not the full `SessionSummary`) live in `uw-core` as an `api` module, so `uw-cli`
can build requests against them depending only on `uw-core`. `uw-daemon` embeds `uw-web`'s
axum app directly (one process serves both the CLI-facing API and the browser UI) rather
than running them as separate processes — simplest option that still matches "CLI writes
require the daemon running." `uw-daemon`'s `Cargo.toml` gains a dependency on `uw-web`;
`uw-web`'s route handlers use the `uw-core::api` DTOs and call into `uw-core`'s client API
functions (§ top of this doc) to do real work.

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
5. Seed the new session via `adapter.seed_new_session(SeedMode::InitialPrompt, seed)` —
   both Claude Code (`claude -p "<summary>"`) and Codex (`codex exec "<summary>"`) treat
   this as a brand-new session with the summary as its literal launch prompt; no hook
   workaround needed, since the hook-injection gotchas documented in the adapter sections
   above only apply to injecting into an EXISTING/resumed session's context, not a fresh
   one. `SeedMode::ForkWithHistory` (Codex only) is available as a capability but
   deliberately not used here — forking carries the entire prior conversation forward,
   which defeats the point of a lean reseed.
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

1. Repo scaffolding (done) + Codex research spike (done, docs/research-codex.md) +
   post-review architecture fixes (done — capability-gated adapter trait, corrected
   Claude Code injection-channel documentation, schema fixes, dependency-version notes;
   see the Fable review-1 findings this revision incorporates).
2. `uw-core` domain types + `uw-store` SQLite schema/migrations, TDD. Must include an
   empirical test/check of the `SessionId`-survives-compaction assumption for Claude Code
   before other code relies on it (see the domain-model note above); add a stop-detection
   write-up to docs/research-codex.md (Codex's `rateLimitReachedType`/`SessionEnd`/
   `Interrupt` signals) before `detect_stop` is implemented for Codex in Phase 4.
3. `uw-policy`: threshold resolution, burn-rate/projection math, burn-scaled trigger,
   opportunistic idle-compact, reseed auto-trigger — pure, fully unit-tested, all gated by
   the `Capabilities` struct (a policy must never synthesize a request an adapter can't
   honor).
4. `uw-adapters`: trait + Claude Code adapter (full) + Codex adapter (full) + generic/stub
   + the hook-ingress shim (crate placement decided here, see "Hook ingress" above).
5. `uw-daemon`: polling, compaction queue delivery/claim, hook wiring both harnesses,
   idle auto-compact end-to-end, resume scheduler + respawn both harnesses.
6. `uw-cli` + the shared `/api/*` JSON surface.
7. `uw-web`: UI routes and embedded assets.
8. Alt-model reseed flow (manual + auto-trigger policy) + cache keepalive mode.
9. This repo exposes `packages.${system}.default`; phoe-nix packaging + `uw-mcp` + kasetto config.

Each phase must leave `cargo test` and `cargo clippy --all-targets` green before merge.
