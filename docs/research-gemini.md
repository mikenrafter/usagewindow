# Gemini / Antigravity integration research

Investigation of Antigravity (`agy` CLI / ACP harness running Gemini 4 Neon) and its
T3Code orchestration v2 integration on 2026-11-20. All live database reads used
read-only SQLite connections (`mode=ro`); no live sessions were messaged or restarted.

## Usage / quota CLI

Antigravity exposes quota windows through the non-interactive slash command
`agy --print /usage --print-timeout 15s` (no model call; completes in ~2 s).

Required invocation environment (verified 2026-11-20):

- `GEMINI_HOME=<profile dir>` — T3Code's Antigravity profile directory, e.g.
  `~/.t3/userdata/providers/antigravity/<profile-id>/` (containing
  `antigravity-acp/antigravity-oauth-token`), or `~/.gemini` when used standalone.
- `AGY_ACP_FORCE_FILE_STORAGE=1`
- `AGY_CLI_DISABLE_AUTO_UPDATE=1`
- Working directory set to a temporary directory (`std::env::temp_dir()`) and stdin
  closed (`/dev/null`).
- **Inherited `ANTIGRAVITY_*` variables must be stripped.** Verified empirically on
  2026-11-20: when `agy --print /usage` inherits active ACP session variables
  (`ANTIGRAVITY_APP_DATA_DIR`, `ANTIGRAVITY_LS_ADDRESS`, `ANTIGRAVITY_CSRF_TOKEN`,
  etc.), it ignores the profile's stored token and blocks waiting on interactive
  OAuth (`Authentication required. Please visit the URL to log in:`). Stripping all
  `ANTIGRAVITY_*` environment variables before invoking `agy` makes `/usage` succeed
  reliably even when `uw` or `uw-daemon` is launched inside an Antigravity session.

Verified stdout shape (tab-separated, 4 columns: pool, window label, **percent
remaining**, RFC3339 reset timestamp; captured 2026-11-20):

```tsv
Gemini Models	Weekly Limit Remaining	83%	2026-11-28T03:15:21Z
Gemini Models	Five Hour Limit Remaining	39%	2026-11-21T08:15:21Z
Claude and GPT models	Weekly Limit Remaining	100%	2026-11-28T05:23:27Z
Claude and GPT models	Five Hour Limit Remaining	100%	2026-11-21T10:23:27Z
Gemini 4 Argon	Gemini 4 Argon Weekly Limit	100%	2026-11-28T05:23:27Z
```

Adapter mapping (`Provider::Gemini`):

- Column 3 reports **remaining** percentage, whereas `UsageWindowState.pct` stores
  **used** percentage across all adapters. Convert `pct = (100.0 - remaining).clamp(0.0, 100.0)`
  and `exceeded = remaining <= 0.0`.
- `Gemini Models` / `Five Hour Limit Remaining` → `WindowKind::Rolling { minutes: 300 }`
- `Gemini Models` / `Weekly Limit Remaining` → `WindowKind::Rolling { minutes: 10_080 }`
- `Claude and GPT models` / `Five Hour Limit Remaining` → `WindowKind::Custom("Claude and GPT models 5h")`
- `Claude and GPT models` / `Weekly Limit Remaining` → `WindowKind::WeeklySurface("Claude and GPT models")`
- Other weekly rows (e.g. `Gemini 4 Argon` / `Gemini 4 Argon Weekly Limit`) → `WindowKind::WeeklyModel(ModelId(pool))`
- Any unrecognized future `(pool, window)` row → `WindowKind::Custom("<pool>: <window>")`

Keeping the Gemini pool on `Rolling { minutes: 300 | 10_080 }` and the Claude/GPT pool
on distinct surface/custom keys avoids key collisions in `UsageSample.windows` and
ensures duration-keyed consumers (`usage-budget-probe Gemini --window 5h|weekly`) read
the Gemini pool rather than taking a minimum against the Claude/GPT pool.

## T3Code session discovery and ownership mapping

Verified 2026-11-20 on live `~/.t3/userdata/statev2.sqlite` (read-only):

- T3Code migrated its live state database from `state.sqlite` to `statev2.sqlite`.
  Legacy v1 tables (`provider_session_runtime`, `projection_thread_sessions`) contain
  only `claudeAgent` and `codex` rows; Antigravity threads exist only in the
  orchestration v2 projection tables:
  - `orchestration_v2_projection_threads`: `thread_id`, `project_id`, `title`,
    `default_provider` (`"antigravity"`), `active_provider_thread_id`, `created_at`,
    `updated_at`, `archived_at`, `deleted_at`, and `payload_json` (with
    `$.modelSelection.model` and `$.worktreePath`).
  - `orchestration_v2_projection_provider_threads`: `provider_thread_id`, `thread_id`,
    `provider` (`"antigravity"`), `provider_session_id`, `status`, `updated_at`, and
    `payload_json` carrying:
    - `$.nativeThreadRef.nativeId`: the Antigravity conversation UUID (matches
      `<profile>/antigravity-acp/conversations/<uuid>.db`).
    - `$.contextUsage.usedTokens` and `$.contextUsage.maxTokens` (observed `1048576`
      for `gemini-4-neon`).
  - `orchestration_v2_projection_provider_sessions`: `provider_session_id`, `thread_id`,
    `provider`, `status`, `payload_json` (carries `$.cwd`, `$.model`, `$.lastError`).
  - `projection_projects`: `project_id`, `workspace_root`.
- `orchestration_v2_projection_provider_threads` stores the same
  `nativeThreadRef.nativeId` mapping for `claudeAgent`, `codex`, and `antigravity`.
- `uw` surfaces each non-deleted T3 Antigravity thread as a `Provider::Gemini` session
  keyed by its T3 `thread_id` (`SessionId(thread_id)`).

## Conversation DB per-call token usage

Each Antigravity ACP conversation stores a SQLite database at:

```text
~/.t3/userdata/providers/antigravity/<profile-id>/antigravity-acp/conversations/<conversation-uuid>.db
```

Relevant tables (verified 2026-11-20, read-only):

- `gen_metadata(idx INTEGER PRIMARY KEY, data BLOB, size INTEGER)`: one protobuf blob
  per model call. Field meanings are **inferred** from live captures (not documented
  upstream):
  - `1.4.2`: new (uncached) input tokens
  - `1.4.5`: cached input tokens
  - `1.4.3`: output tokens
  - `1.4.9` and `1.4.10`: observed to sum exactly to `1.4.3` across 5/5 sampled rows
    (likely thinking vs visible output, but which is which is unverified — left as
    `reasoning_output_tokens = 0` rather than guessing)
  - `1.9.10.1`: context token count
  - `1.19`: model id string (e.g. `"gemini-4-neon"`)
  - `1.20` (repeated key/value messages, `1` = key, `2` = value): entry
    `"last_step_index"` gives the step index immediately preceding the model call.
- `steps(idx INTEGER PRIMARY KEY, step_type INTEGER, ..., metadata BLOB)`:
  - Across 8/8 sampled `gen_metadata` rows in a live conversation, `steps[last_step_index + 1]`
    was the model generation step (`step_type = 15`) and `metadata` field `1.1` held its
    Unix-epoch timestamp in seconds (protobuf `google.protobuf.Timestamp`).
- Mapping into `uw_core::adapter::TokenUsageRecord`:
  - `TokenUsageRecord` defines `cached_input_tokens` as a **subset** of `input_tokens`
    (so `uw_policy::weighted_token_rate_per_minute` computes uncached input as
    `input_tokens.saturating_sub(cached_input_tokens)`).
  - Because `1.4.2` (new) and `1.4.5` (cached) are disjoint in Antigravity, the adapter
    sets `input_tokens = new_input + cached_input`, `cached_input_tokens = cached_input`,
    `cache_write_input_tokens = 0`, `output_tokens = output`,
    `reasoning_output_tokens = 0`, `total_tokens = input_tokens + output_tokens`.

## Prompt cache TTL

The probe log at `mstack.research/neon-trial/ttl-probe.log` records cache hits
after 3, 6, 12 and 24 minutes. The 24-minute hit at 2026-11-21T06:00:21Z
reported 8,334 cached tokens. No 48-minute result was present when porting.
Expiry remains unmeasured; these hits do not guarantee future cache retention.

`uw_adapters::antigravity::GEMINI_CACHE_TTL` uses the longest observed hit,
rounded down, minus a one-minute margin: 23 minutes. Discovery populates
`DiscoveredSession.cache_ttl`; observation persists it to `SessionSummary.cache_ttl`.
The deployed branch's existing per-session TTL consumers handle display and
policy timing. Capability gates keep Gemini observe-only even with a known TTL.

## Capabilities (observe-only)

In this integration, `Provider::Gemini` is strictly observe-only:

- `can_trigger_compaction: false`
- `can_advise_mid_turn: false`
- `can_inject_at_session_start: false`
- `can_observe_compaction: false`
- `reports_token_counts: true`
- `headless_resume: false`
- `seed_modes: []`

`AntigravityAdapter` refuses all action methods. `T3CodeAdapter` refuses Gemini
`compact` and `resume_session` before transport access. The daemon leaves Gemini out of compaction fallback wrapping and gates hard-boundary
interrupts on compaction capability. Antigravity action semantics need a dedicated
research pass before enabling delivery.

## Review fixes and remaining quota uncertainty

A read-only scan on 2026-11-21 found 262 `gen_metadata` rows carrying a
`request_id` entry in repeated protobuf field 1.20. Their IDs covered 15
trajectories, with no repeated numeric suffix within a trajectory; the largest
sample contained 70 calls. This is empirical evidence, not an upstream guarantee.
The aggregate-only probe and result are in
`mstack.research/neon-trial/uw-port-fix1-evidence/request-id-probe.{py,json}`.

Call keys combine the native conversation ID with the full request ID. Missing
request IDs fall back to the conversation ID plus `gen_metadata.idx`, in a separate
namespace. The idx fallback cannot identify an edited call whose idx is reused
without a request ID. The generic store upserts by nullable call key; timestamps
and final token counts may change without inserting another identified call.
Legacy rows without call keys retain their previous identity. Existing ambiguous
legacy duplicates cannot be reconstructed from a stable key that was never saved.

Discovery fingerprints include the SQLite database and WAL sizes and mtimes.
Malformed quota rows are logged and skipped while valid sibling rows survive.
A missing `agy` executable is unsupported. Gemini policy skips the known Claude
and GPT pool keys, while observation and display retain those quota rows.

The meaning and policy applicability of the `Gemini 4 Argon` quota row remain
unverified. Its existing mapping is unchanged pending evidence.
