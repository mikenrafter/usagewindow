use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, DiscoveredSession, HarnessAdapter,
    StatusEvent, TokenUsageRecord,
};
use uw_core::model::*;

/// 24-minute cache hit in ttl-probe.log (2026-11-21), minus a one-minute margin.
pub const GEMINI_CACHE_TTL: chrono::Duration = chrono::Duration::minutes(23);

const AGY_USAGE_TIMEOUT_SECS: u64 = 20;

/// Reads raw tab-separated quota output from Antigravity (`agy --print /usage`).
#[async_trait]
pub trait AntigravityUsageReader: Send + Sync {
    async fn read_usage_tsv(&self) -> AdapterResult<String>;
}

/// Invokes the `agy` CLI non-interactively to read Antigravity quota windows.
///
/// Always strips inherited `ANTIGRAVITY_*` environment variables so running
/// inside an active Antigravity ACP session does not trigger interactive OAuth.
pub struct AgyCliUsageReader {
    program: String,
    profile_dir: Option<PathBuf>,
}

impl AgyCliUsageReader {
    pub fn new(program: impl Into<String>, profile_dir: Option<PathBuf>) -> Self {
        Self {
            program: program.into(),
            profile_dir,
        }
    }

    pub fn from_environment() -> Self {
        let program = std::env::var("UW_ANTIGRAVITY_CLI").unwrap_or_else(|_| "agy".into());
        Self::new(program, discover_profile_dir_from_environment())
    }
}

fn antigravity_env_keys_to_remove(keys: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
    keys.into_iter()
        .filter(|key| {
            key.to_str()
                .is_some_and(|name| name.starts_with("ANTIGRAVITY_"))
        })
        .collect()
}

#[async_trait]
impl AntigravityUsageReader for AgyCliUsageReader {
    async fn read_usage_tsv(&self) -> AdapterResult<String> {
        let mut command = tokio::process::Command::new(&self.program);
        command
            .args(["--print", "/usage", "--print-timeout", "15s"])
            .current_dir(std::env::temp_dir())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // CLAIM(agy-env-strip): inheriting parent ANTIGRAVITY_* ACP variables
        // breaks stored-token auth on `agy --print /usage` [docs/research-gemini.md;
        // agy_cli_reader_strips_inherited_antigravity_env_and_passes_profile].
        // ↳ CLAIM(agy-env-strip)
        for key in antigravity_env_keys_to_remove(std::env::vars_os().map(|(k, _)| k)) {
            command.env_remove(key);
        }
        if let Some(profile_dir) = &self.profile_dir {
            command.env("GEMINI_HOME", profile_dir);
        }
        command
            .env("AGY_ACP_FORCE_FILE_STORAGE", "1")
            .env("AGY_CLI_DISABLE_AUTO_UPDATE", "1");

        let child = command
            .spawn()
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    AdapterError::Unsupported
                } else {
                    AdapterError::Transient(format!("spawn {}: {error}", self.program))
                }
            })?;
        let output = tokio::time::timeout(
            StdDuration::from_secs(AGY_USAGE_TIMEOUT_SECS),
            child.wait_with_output(),
        )
        .await
        .map_err(|_| {
            AdapterError::Transient(format!(
                "{} --print /usage timed out after {AGY_USAGE_TIMEOUT_SECS}s",
                self.program
            ))
        })?
        .map_err(|error| AdapterError::Transient(format!("wait for {}: {error}", self.program)))?;

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if looks_like_auth_required(&stdout) || looks_like_auth_required(&stderr) {
            return Err(AdapterError::Auth);
        }
        if !output.status.success() {
            let detail = stderr.trim();
            return Err(AdapterError::Transient(if detail.is_empty() {
                format!("{} --print /usage exited {}", self.program, output.status)
            } else {
                format!(
                    "{} --print /usage exited {}: {detail}",
                    self.program, output.status
                )
            }));
        }
        Ok(stdout)
    }
}

fn looks_like_auth_required(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("authentication required") || lower.contains("authentication failed")
}

/// Discovers the Antigravity profile directory (for `GEMINI_HOME`) given an
/// optional T3Code userdata root and fallback `$HOME`.
///
/// Prefers a T3 profile directory containing `antigravity-acp/antigravity-oauth-token`,
/// then any T3 profile containing `antigravity-acp`, then `<home>/.gemini`.
pub fn discover_profile_dir(
    t3_userdata_dir: Option<&Path>,
    fallback_home: Option<&Path>,
) -> Option<PathBuf> {
    if let Some(t3_dir) = t3_userdata_dir {
        let profiles_root = t3_dir.join("providers/antigravity");
        if let Ok(entries) = std::fs::read_dir(profiles_root) {
            let mut candidates: Vec<PathBuf> = entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
                .collect();
            candidates.sort();
            if let Some(authed) = candidates.iter().find(|dir| {
                dir.join("antigravity-acp/antigravity-oauth-token")
                    .is_file()
            }) {
                return Some(authed.clone());
            }
            if let Some(acp) = candidates
                .iter()
                .find(|dir| dir.join("antigravity-acp").is_dir())
            {
                return Some(acp.clone());
            }
        }
    }
    fallback_home
        .map(|home| home.join(".gemini"))
        .filter(|path| path.is_dir())
}

pub fn discover_profile_dir_from_environment() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("UW_ANTIGRAVITY_PROFILE_DIR")
        && !explicit.is_empty()
    {
        return Some(PathBuf::from(explicit));
    }
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let t3_userdata = home.as_ref().map(|h| h.join(".t3/userdata"));
    discover_profile_dir(t3_userdata.as_deref(), home.as_deref())
}

fn map_window_kind(pool: &str, window_label: &str) -> WindowKind {
    match (pool, window_label) {
        ("Gemini Models", "Five Hour Limit Remaining") => WindowKind::Rolling { minutes: 300 },
        ("Gemini Models", "Weekly Limit Remaining") => WindowKind::Rolling { minutes: 10_080 },
        ("Claude and GPT models", "Five Hour Limit Remaining") => {
            WindowKind::Custom("Claude and GPT models 5h".into())
        }
        ("Claude and GPT models", "Weekly Limit Remaining") => {
            WindowKind::WeeklySurface("Claude and GPT models".into())
        }
        (pool, label) if label.contains("Weekly") => WindowKind::WeeklyModel(ModelId(pool.into())),
        (pool, label) => WindowKind::Custom(format!("{pool}: {label}")),
    }
}

/// Parses tab-separated `agy --print /usage` output into a `Provider::Gemini`
/// `UsageSample`. Column 3 is remaining percent and is inverted to used percent.
pub fn parse_usage_tsv(output: &str, at: DateTime<Utc>) -> AdapterResult<UsageSample> {
    if looks_like_auth_required(output) {
        return Err(AdapterError::Auth);
    }
    let mut windows = HashMap::new();
    for raw_line in output.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let columns: Vec<&str> = line.split('\t').map(str::trim).collect();
        if columns.len() != 4 {
            continue;
        }
        let (pool, window_label, pct_text, reset_text) =
            (columns[0], columns[1], columns[2], columns[3]);
        if pool.is_empty() || window_label.is_empty() {
            continue;
        }
        let remaining = pct_text.strip_suffix('%').unwrap_or(pct_text).trim().parse::<f32>();
        let remaining = match remaining {
            Ok(value) if value.is_finite() => value,
            _ => {
                tracing::warn!(pool, window_label, "skipping invalid Antigravity remaining percent");
                continue;
            }
        };
        let remaining_clamped = remaining.clamp(0.0, 100.0);
        let used_pct = (100.0 - remaining_clamped).clamp(0.0, 100.0);
        let exceeded = remaining_clamped <= 0.0;
        let resets_at = if reset_text.is_empty() || reset_text == "-" || reset_text == "N/A" {
            None
        } else {
            match DateTime::parse_from_rfc3339(reset_text) {
                Ok(value) => Some(value.with_timezone(&Utc)),
                Err(_) => {
                    tracing::warn!(pool, window_label, "skipping invalid Antigravity reset timestamp");
                    continue;
                }
            }
        };
        let kind = map_window_kind(pool, window_label);
        windows.insert(
            WindowKey {
                provider: Provider::Gemini,
                kind,
            },
            UsageWindowState::new(used_pct, exceeded, true, resets_at, None),
        );
    }
    if windows.is_empty() {
        return Err(AdapterError::Other(
            "no usage rows found in Antigravity /usage output".into(),
        ));
    }
    Ok(UsageSample {
        at,
        fetched_at: Some(at),
        source: UsageSource::ProviderReported,
        provider: Provider::Gemini,
        account: None,
        plan: None,
        windows,
        credits: None,
    })
}

/// Per-call token usage decoded from an Antigravity conversation DB's
/// `gen_metadata` protobuf blob (field schema documented in
/// `docs/research-gemini.md`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenCallUsage {
    pub new_input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub context_tokens: Option<u64>,
    pub model: Option<String>,
    pub last_step_index: Option<i64>,
}

enum WireValue<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Fixed,
}

fn read_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*pos)?;
        *pos += 1;
        if shift >= 64 || (shift == 63 && byte > 1) {
            return None;
        }
        result |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Some(result);
        }
        shift += 7;
    }
}

fn parse_proto_fields(bytes: &[u8]) -> Option<Vec<(u32, WireValue<'_>)>> {
    let mut pos = 0usize;
    let mut out = Vec::new();
    while pos < bytes.len() {
        let tag = read_varint(bytes, &mut pos)?;
        let field_number = u32::try_from(tag >> 3).ok()?;
        if field_number == 0 {
            return None;
        }
        let wire_type = tag & 7;
        let value = match wire_type {
            0 => WireValue::Varint(read_varint(bytes, &mut pos)?),
            1 => {
                let end = pos.checked_add(8)?;
                if end > bytes.len() {
                    return None;
                }
                pos = end;
                WireValue::Fixed
            }
            2 => {
                let len = usize::try_from(read_varint(bytes, &mut pos)?).ok()?;
                let end = pos.checked_add(len)?;
                let slice = bytes.get(pos..end)?;
                pos = end;
                WireValue::Bytes(slice)
            }
            5 => {
                let end = pos.checked_add(4)?;
                if end > bytes.len() {
                    return None;
                }
                pos = end;
                WireValue::Fixed
            }
            _ => return None,
        };
        out.push((field_number, value));
    }
    Some(out)
}

fn first_bytes<'a>(fields: &[(u32, WireValue<'a>)], number: u32) -> Option<&'a [u8]> {
    fields.iter().find_map(|(f, v)| match v {
        WireValue::Bytes(slice) if *f == number => Some(*slice),
        _ => None,
    })
}

fn first_varint(fields: &[(u32, WireValue<'_>)], number: u32) -> Option<u64> {
    fields.iter().find_map(|(f, v)| match v {
        WireValue::Varint(value) if *f == number => Some(*value),
        _ => None,
    })
}

pub fn parse_gen_metadata_blob(blob: &[u8]) -> Option<GenCallUsage> {
    let top = parse_proto_fields(blob)?;
    let one = parse_proto_fields(first_bytes(&top, 1)?)?;
    let usage = parse_proto_fields(first_bytes(&one, 4)?)?;
    let new_input_tokens = first_varint(&usage, 2).unwrap_or(0);
    let cached_input_tokens = first_varint(&usage, 5).unwrap_or(0);
    let output_tokens = first_varint(&usage, 3).unwrap_or(0);

    let context_tokens = first_bytes(&one, 9)
        .and_then(parse_proto_fields)
        .and_then(|f9| first_bytes(&f9, 10))
        .and_then(parse_proto_fields)
        .and_then(|f10| first_varint(&f10, 1));

    let model = first_bytes(&one, 19)
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned);

    let mut last_step_index = None;
    for (field, value) in &one {
        if *field == 20
            && let WireValue::Bytes(entry_bytes) = value
            && let Some(entry) = parse_proto_fields(entry_bytes)
            && first_bytes(&entry, 1) == Some(b"last_step_index")
            && let Some(raw_idx) = first_bytes(&entry, 2)
            && let Ok(text) = std::str::from_utf8(raw_idx)
            && let Ok(parsed) = text.parse::<i64>()
        {
            last_step_index = Some(parsed);
        }
    }

    Some(GenCallUsage {
        new_input_tokens,
        cached_input_tokens,
        output_tokens,
        context_tokens,
        model,
        last_step_index,
    })
}

pub fn parse_step_timestamp(blob: &[u8]) -> Option<DateTime<Utc>> {
    let top = parse_proto_fields(blob)?;
    let ts_msg = parse_proto_fields(first_bytes(&top, 1)?)?;
    let seconds = i64::try_from(first_varint(&ts_msg, 1)?).ok()?;
    let nanos = first_varint(&ts_msg, 2)
        .and_then(|n| u32::try_from(n).ok())
        .filter(|n| *n < 1_000_000_000)
        .unwrap_or(0);
    Utc.timestamp_opt(seconds, nanos).single()
}

#[derive(Clone, Debug)]
struct CachedConversation {
    fingerprint: ((u64, Option<std::time::SystemTime>), Option<(u64, Option<std::time::SystemTime>)>),
    records: Vec<TokenUsageRecord>,
    latest_context_tokens: Option<u64>,
    model: Option<String>,
}

fn read_conversation_usage(
    db_path: &Path,
    fallback_at: DateTime<Utc>,
) -> AdapterResult<(Vec<TokenUsageRecord>, Option<u64>, Option<String>)> {
    let connection =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|error| {
                AdapterError::Other(format!(
                    "open Antigravity conversation database {}: {error}",
                    db_path.display()
                ))
            })?;

    let mut step_timestamps: HashMap<i64, DateTime<Utc>> = HashMap::new();
    let has_steps: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'steps')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v != 0)
        .unwrap_or(false);
    if has_steps
        && let Ok(mut stmt) = connection.prepare("SELECT idx, metadata FROM steps")
        && let Ok(rows) = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<Vec<u8>>>(1)?))
        })
    {
        for row in rows.flatten() {
            if let (idx, Some(meta)) = row
                && let Some(ts) = parse_step_timestamp(&meta)
            {
                step_timestamps.insert(idx, ts);
            }
        }
    }

    let has_gen: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'gen_metadata')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v != 0)
        .unwrap_or(false);
    if !has_gen {
        return Ok((Vec::new(), None, None));
    }

    let mut statement = connection
        .prepare("SELECT idx, data FROM gen_metadata ORDER BY idx")
        .map_err(|error| AdapterError::Other(format!("read gen_metadata: {error}")))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(|error| AdapterError::Other(format!("scan gen_metadata: {error}")))?;

    let mut records = Vec::new();
    let mut latest_context_tokens = None;
    let mut latest_model = None;
    for row in rows {
        let (_idx, blob) =
            row.map_err(|error| AdapterError::Other(format!("read gen_metadata row: {error}")))?;
        let Some(call) = parse_gen_metadata_blob(&blob) else {
            continue;
        };
        if let Some(ctx) = call.context_tokens {
            latest_context_tokens = Some(ctx);
        }
        if let Some(model) = &call.model {
            latest_model = Some(model.clone());
        }
        let at = call
            .last_step_index
            .and_then(|step| {
                step_timestamps
                    .get(&(step + 1))
                    .or_else(|| step_timestamps.get(&step))
                    .copied()
            })
            .unwrap_or(fallback_at);
        // CLAIM(antigravity-cached-subset): TokenUsageRecord requires
        // cached_input_tokens to be a subset of input_tokens so
        // uw_policy::weighted_token_rate_per_minute computes uncached input
        // via input_tokens - cached_input_tokens [docs/research-gemini.md;
        // discovers_t3_antigravity_threads_with_thread_id_and_conversation_tokens].
        // ↳ CLAIM(antigravity-cached-subset)
        let input_tokens = call
            .new_input_tokens
            .saturating_add(call.cached_input_tokens);
        let total_tokens = input_tokens.saturating_add(call.output_tokens);
        records.push(TokenUsageRecord {
            at,
            model: call.model.map(ModelId),
            input_tokens,
            cached_input_tokens: call.cached_input_tokens,
            cache_write_input_tokens: 0,
            output_tokens: call.output_tokens,
            reasoning_output_tokens: 0,
            total_tokens,
            context_pct: None,
        });
    }
    Ok((records, latest_context_tokens, latest_model))
}

struct T3AntigravityRow {
    thread_id: String,
    title: Option<String>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    thread_payload: String,
    provider_thread_updated_at: Option<DateTime<Utc>>,
    provider_thread_payload: Option<String>,
    provider_session_payload: Option<String>,
    workspace_root: Option<String>,
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn read_t3_antigravity_threads(state_db: &Path) -> AdapterResult<Vec<T3AntigravityRow>> {
    if !state_db.exists() {
        return Ok(Vec::new());
    }
    let connection =
        rusqlite::Connection::open_with_flags(state_db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|error| AdapterError::Other(format!("open T3Code state database: {error}")))?;

    for required in [
        "orchestration_v2_projection_threads",
        "orchestration_v2_projection_provider_threads",
    ] {
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                [required],
                |row| row.get::<_, i64>(0),
            )
            .map(|v| v != 0)
            .map_err(|error| AdapterError::Other(format!("inspect T3Code schema: {error}")))?;
        if !exists {
            return Ok(Vec::new());
        }
    }

    let has_projects: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'projection_projects')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v != 0)
        .unwrap_or(false);
    let has_sessions: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'orchestration_v2_projection_provider_sessions')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v != 0)
        .unwrap_or(false);

    let projects_join = if has_projects {
        "LEFT JOIN projection_projects p ON p.project_id = t.project_id"
    } else {
        ""
    };
    let projects_col = if has_projects {
        "p.workspace_root"
    } else {
        "NULL"
    };
    let sessions_join = if has_sessions {
        "LEFT JOIN orchestration_v2_projection_provider_sessions ps ON ps.provider_session_id = pt.provider_session_id"
    } else {
        ""
    };
    let sessions_col = if has_sessions {
        "ps.payload_json"
    } else {
        "NULL"
    };

    let sql = format!(
        "SELECT
            t.thread_id,
            t.title,
            t.created_at,
            t.updated_at,
            t.payload_json,
            pt.updated_at,
            pt.payload_json,
            {sessions_col},
            {projects_col}
         FROM orchestration_v2_projection_threads t
         LEFT JOIN orchestration_v2_projection_provider_threads pt
           ON pt.provider_thread_id = t.active_provider_thread_id
         {sessions_join}
         {projects_join}
         WHERE t.deleted_at IS NULL
           AND (t.default_provider = 'antigravity' OR pt.provider = 'antigravity')
         ORDER BY t.updated_at DESC"
    );

    let mut statement = connection.prepare(&sql).map_err(|error| {
        AdapterError::Other(format!("query T3Code Antigravity threads: {error}"))
    })?;
    let rows = statement
        .query_map([], |row| {
            let thread_id: String = row.get(0)?;
            let title: Option<String> = row.get(1)?;
            let created_at: Option<String> = row.get(2)?;
            let updated_at: Option<String> = row.get(3)?;
            let thread_payload: String = row.get(4)?;
            let pt_updated_at: Option<String> = row.get(5)?;
            let pt_payload: Option<String> = row.get(6)?;
            let ps_payload: Option<String> = row.get(7)?;
            let workspace_root: Option<String> = row.get(8)?;
            Ok(T3AntigravityRow {
                thread_id,
                title: title.filter(|value| !value.is_empty()),
                created_at: created_at.as_deref().and_then(parse_rfc3339),
                updated_at: updated_at.as_deref().and_then(parse_rfc3339),
                thread_payload,
                provider_thread_updated_at: pt_updated_at.as_deref().and_then(parse_rfc3339),
                provider_thread_payload: pt_payload,
                provider_session_payload: ps_payload,
                workspace_root: workspace_root.filter(|value| !value.is_empty()),
            })
        })
        .map_err(|error| {
            AdapterError::Other(format!("scan T3Code Antigravity threads: {error}"))
        })?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|error| {
            AdapterError::Other(format!("decode T3Code Antigravity thread row: {error}"))
        })?);
    }
    Ok(out)
}

/// Read-only adapter for Antigravity / Gemini Neon (`Provider::Gemini`).
///
/// Observes quota windows via `agy --print /usage` and discovers T3Code
/// Antigravity threads from `statev2.sqlite`, enriched with per-call cached vs.
/// uncached token usage from Antigravity's conversation SQLite databases.
/// All destructive actions (`compact`, `resume_session`, `interrupt`, `advise`)
/// return `AdapterError::Unsupported`.
pub struct AntigravityAdapter {
    usage_reader: Arc<dyn AntigravityUsageReader>,
    now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    t3_state_db: Option<PathBuf>,
    conversations_dir: Option<PathBuf>,
    discovery_cache: Arc<Mutex<HashMap<PathBuf, CachedConversation>>>,
}

impl AntigravityAdapter {
    pub fn new(usage_reader: Arc<dyn AntigravityUsageReader>) -> Self {
        Self {
            usage_reader,
            now: Arc::new(Utc::now),
            t3_state_db: None,
            conversations_dir: None,
            discovery_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn real() -> Self {
        let profile = discover_profile_dir_from_environment();
        let conversations_dir = profile
            .as_ref()
            .map(|dir| dir.join("antigravity-acp/conversations"));
        Self {
            usage_reader: Arc::new(AgyCliUsageReader::from_environment()),
            now: Arc::new(Utc::now),
            t3_state_db: crate::t3code::default_state_db_path(),
            conversations_dir,
            discovery_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn is_available_in_environment() -> bool {
        if std::env::var_os("UW_ANTIGRAVITY_CLI").is_some()
            || std::env::var_os("UW_ANTIGRAVITY_PROFILE_DIR").is_some()
        {
            return true;
        }
        if discover_profile_dir_from_environment().is_some() {
            return true;
        }
        crate::t3code::default_state_db_path().is_some_and(|path| path.exists())
    }

    pub fn with_clock(mut self, now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    pub fn with_t3_state_db(mut self, path: PathBuf) -> Self {
        self.t3_state_db = Some(path);
        self
    }

    pub fn with_conversations_dir(mut self, dir: PathBuf) -> Self {
        self.conversations_dir = Some(dir);
        self
    }

    pub fn capabilities_static() -> Capabilities {
        Capabilities {
            can_trigger_compaction: false,
            can_advise_mid_turn: false,
            can_inject_at_session_start: false,
            can_observe_compaction: false,
            reports_token_counts: true,
            headless_resume: false,
            seed_modes: vec![],
        }
    }
}

#[async_trait]
impl HarnessAdapter for AntigravityAdapter {
    fn provider(&self) -> Provider {
        Provider::Gemini
    }

    fn capabilities(&self) -> Capabilities {
        Self::capabilities_static()
    }

    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        let output = self.usage_reader.read_usage_tsv().await?;
        parse_usage_tsv(&output, (self.now)())
    }

    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        Ok(None)
    }

    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        let Some(state_db) = self.t3_state_db.clone() else {
            return Ok(Vec::new());
        };
        let conversations_dir = self.conversations_dir.clone();
        let cache = Arc::clone(&self.discovery_cache);
        let now = (self.now)();

        tokio::task::spawn_blocking(move || {
            let rows = read_t3_antigravity_threads(&state_db)?;
            let mut discovered = Vec::with_capacity(rows.len());
            for row in rows {
                let thread_json = serde_json::from_str::<Value>(&row.thread_payload).ok();
                let pt_json = row
                    .provider_thread_payload
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
                let ps_json = row
                    .provider_session_payload
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

                let worktree_path = thread_json
                    .as_ref()
                    .and_then(|v| v.get("worktreePath"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned);
                let ps_cwd = ps_json
                    .as_ref()
                    .and_then(|v| v.get("cwd"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned);
                let cwd = worktree_path
                    .or(row.workspace_root)
                    .or(ps_cwd)
                    .unwrap_or_else(|| ".".into());

                let t3_model = thread_json
                    .as_ref()
                    .and_then(|v| v.pointer("/modelSelection/model"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned);

                let context_window_size = pt_json
                    .as_ref()
                    .and_then(|v| v.pointer("/contextUsage/maxTokens"))
                    .and_then(Value::as_u64)
                    .filter(|tokens| *tokens > 0);
                let t3_used_tokens = pt_json
                    .as_ref()
                    .and_then(|v| v.pointer("/contextUsage/usedTokens"))
                    .and_then(Value::as_u64)
                    .filter(|tokens| *tokens > 0);
                let native_id = pt_json
                    .as_ref()
                    .and_then(|v| v.pointer("/nativeThreadRef/nativeId"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty());

                let mut state_path = None;
                let mut token_usage = Vec::new();
                let mut conv_context_tokens = None;
                let mut conv_model = None;

                if let (Some(conv_dir), Some(native_id)) = (&conversations_dir, native_id) {
                    let db_path = conv_dir.join(format!("{native_id}.db"));
                    if let Ok(metadata) = std::fs::metadata(&db_path) {
                        state_path = Some(db_path.to_string_lossy().into_owned());
                        let modified = metadata.modified().ok();
                        let wal = std::fs::metadata(format!("{}-wal", db_path.display())).ok();
                        let fingerprint = ((metadata.len(), modified), wal.map(|m| (m.len(), m.modified().ok())));
                        let fallback_at = modified
                            .map(DateTime::<Utc>::from)
                            .or(row.provider_thread_updated_at)
                            .or(row.updated_at)
                            .unwrap_or(now);

                        let cached_hit = cache
                            .lock()
                            .map_err(|_| AdapterError::Other("discovery cache poisoned".into()))?
                            .get(&db_path)
                            .filter(|entry| entry.fingerprint == fingerprint)
                            .cloned();

                        if let Some(entry) = cached_hit {
                            token_usage = entry.records;
                            conv_context_tokens = entry.latest_context_tokens;
                            conv_model = entry.model;
                        } else if let Ok((records, ctx_tokens, model)) =
                            read_conversation_usage(&db_path, fallback_at)
                        {
                            cache
                                .lock()
                                .map_err(|_| {
                                    AdapterError::Other("discovery cache poisoned".into())
                                })?
                                .insert(
                                    db_path,
                                    CachedConversation {
                                        fingerprint,
                                        records: records.clone(),
                                        latest_context_tokens: ctx_tokens,
                                        model: model.clone(),
                                    },
                                );
                            token_usage = records;
                            conv_context_tokens = ctx_tokens;
                            conv_model = model;
                        }
                    }
                }

                let model = conv_model.or(t3_model).map(ModelId);
                let last_known_token_count = t3_used_tokens.or(conv_context_tokens);
                let latest_token_at = token_usage.iter().map(|record| record.at).max();
                let last_seen = [
                    row.updated_at,
                    row.provider_thread_updated_at,
                    latest_token_at,
                ]
                .into_iter()
                .flatten()
                .max();

                discovered.push(DiscoveredSession {
                    id: SessionId(row.thread_id),
                    lineage: SessionLineage::default(),
                    cwd,
                    model,
                    context_window_size,
                    last_known_token_count,
                    last_known_context_pct: None,
                    cache_ttl: Some(GEMINI_CACHE_TTL),
                    first_seen: row.created_at,
                    last_seen,
                    state_path,
                    token_usage,
                    title: row.title,
                });
            }
            Ok(discovered)
        })
        .await
        .map_err(|error| AdapterError::Other(error.to_string()))?
    }

    async fn emit_status(&self, _: &SessionId, _: StatusEvent) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }

    async fn advise(&self, _: &SessionId, _: &str) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }

    async fn compact(
        &self,
        _: &SessionSummary,
        _: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }

    async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
        Err(AdapterError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    struct FakeUsageReader(AdapterResult<String>);

    #[async_trait]
    impl AntigravityUsageReader for FakeUsageReader {
        async fn read_usage_tsv(&self) -> AdapterResult<String> {
            match &self.0 {
                Ok(text) => Ok(text.clone()),
                Err(AdapterError::Auth) => Err(AdapterError::Auth),
                Err(AdapterError::Unsupported) => Err(AdapterError::Unsupported),
                Err(AdapterError::Transient(msg)) => Err(AdapterError::Transient(msg.clone())),
                Err(AdapterError::Other(msg)) => Err(AdapterError::Other(msg.clone())),
            }
        }
    }

    /// Literal protobuf bytes hand-encoded from the wire schema verified in
    /// docs/research-gemini.md (independent of our decoder):
    ///
    /// Call 0:
    ///   f1.f4: f2=34448 (0x10 0x90 0x8d 0x02), f3=126 (0x18 0x7e)
    ///   f1.f9.f10: f1=44211 (0x08 0xb3 0xd9 0x02)
    ///   f1.f19: "gemini-4-neon"
    ///   f1.f20: { f1="last_step_index", f2="0" }
    const SAMPLE_GEN_METADATA_CALL_0: &[u8] = &[
        0x0a, 0x37, // f1 len=55 (8 + 8 + 16 + 23)
        // f4 len=6: f2=34448, f3=126
        0x22, 0x06, 0x10, 0x90, 0x8d, 0x02, 0x18, 0x7e, // f9 len=6: f10 len=4: f1=44211
        0x4a, 0x06, 0x52, 0x04, 0x08, 0xb3, 0xd9, 0x02, // f19 len=13: "gemini-4-neon"
        0x9a, 0x01, 0x0d, b'g', b'e', b'm', b'i', b'n', b'i', b'-', b'4', b'-', b'n', b'e', b'o',
        b'n', // f20 len=20: f1="last_step_index" (len 15), f2="0" (len 1)
        0xa2, 0x01, 0x14, 0x0a, 0x0f, b'l', b'a', b's', b't', b'_', b's', b't', b'e', b'p', b'_',
        b'i', b'n', b'd', b'e', b'x', 0x12, 0x01, b'0',
    ];

    /// Call 1:
    ///   f1.f4: f2=2075 (0x10 0x9b 0x10), f3=540 (0x18 0x9c 0x04), f5=34449 (0x28 0x91 0x8d 0x02)
    ///   f1.f9.f10: f1=46547 (0x08 0xd3 0xeb 0x02)
    ///   f1.f19: "gemini-4-neon"
    ///   f1.f20: { f1="last_step_index", f2="2" }
    const SAMPLE_GEN_METADATA_CALL_1: &[u8] = &[
        0x0a, 0x3b, // f1 len=59 (12 + 8 + 16 + 23)
        // f4 len=10: f2=2075, f3=540, f5=34449
        0x22, 0x0a, 0x10, 0x9b, 0x10, 0x18, 0x9c, 0x04, 0x28, 0x91, 0x8d, 0x02,
        // f9 len=6: f10 len=4: f1=46547
        0x4a, 0x06, 0x52, 0x04, 0x08, 0xd3, 0xeb, 0x02, // f19 len=13: "gemini-4-neon"
        0x9a, 0x01, 0x0d, b'g', b'e', b'm', b'i', b'n', b'i', b'-', b'4', b'-', b'n', b'e', b'o',
        b'n', // f20 len=20: f1="last_step_index" (len 15), f2="2" (len 1)
        0xa2, 0x01, 0x14, 0x0a, 0x0f, b'l', b'a', b's', b't', b'_', b's', b't', b'e', b'p', b'_',
        b'i', b'n', b'd', b'e', b'x', 0x12, 0x01, b'2',
    ];

    /// Literal protobuf bytes for steps.metadata field 1 (google.protobuf.Timestamp):
    ///   seconds = 1795238309 (2026-11-21T05:18:29Z) = 0xa5 0xd3 0x84 0xd8 0x06
    const SAMPLE_STEP_TS_1: &[u8] = &[0x0a, 0x06, 0x08, 0xa5, 0xd3, 0x84, 0xd8, 0x06];
    ///   seconds = 1795238313 (2026-11-21T05:18:33Z) = 0xa9 0xd3 0x84 0xd8 0x06
    const SAMPLE_STEP_TS_3: &[u8] = &[0x0a, 0x06, 0x08, 0xa9, 0xd3, 0x84, 0xd8, 0x06];

    #[tokio::test]
    async fn parses_live_shape_antigravity_usage_tsv_into_gemini_windows() {
        // why: `uw status --json` must expose Gemini 5h & weekly windows as
        //      Rolling{300|10080} (for usage-budget-probe) and keep the
        //      Claude/GPT and model-specific pools distinct without overwriting
        //      Gemini's Rolling keys; remaining % must invert to used %.
        // catches: storing remaining % verbatim instead of 100-remaining, or
        //          colliding Claude/GPT pool onto Rolling{300|10080}.
        let tsv = concat!(
            "Gemini Models\tWeekly Limit Remaining\t83%\t2026-11-28T03:15:21Z\n",
            "Gemini Models\tFive Hour Limit Remaining\t39%\t2026-11-21T08:15:21Z\n",
            "Claude and GPT models\tWeekly Limit Remaining\t100%\t2026-11-28T05:23:27Z\n",
            "Claude and GPT models\tFive Hour Limit Remaining\t75%\t2026-11-21T10:23:27Z\n",
            "Gemini 4 Argon\tGemini 4 Argon Weekly Limit\t100%\t2026-11-28T05:23:27Z\n",
        );
        let now = Utc.with_ymd_and_hms(2026, 11, 21, 5, 25, 0).unwrap();
        let adapter = AntigravityAdapter::new(Arc::new(FakeUsageReader(Ok(tsv.into()))))
            .with_clock(Arc::new(move || now));

        let sample = adapter.fetch_usage(None).await.unwrap();
        assert_eq!(sample.provider, Provider::Gemini);
        assert_eq!(sample.source, UsageSource::ProviderReported);
        assert_eq!(sample.at, now);
        assert_eq!(sample.fetched_at, Some(now));
        assert_eq!(sample.windows.len(), 5);

        let gemini_5h = &sample.windows[&WindowKey {
            provider: Provider::Gemini,
            kind: WindowKind::Rolling { minutes: 300 },
        }];
        assert_eq!(gemini_5h.pct, 61.0); // 100 - 39
        assert!(!gemini_5h.exceeded);
        assert_eq!(
            gemini_5h.resets_at,
            Some(Utc.with_ymd_and_hms(2026, 11, 21, 8, 15, 21).unwrap())
        );

        let gemini_weekly = &sample.windows[&WindowKey {
            provider: Provider::Gemini,
            kind: WindowKind::Rolling { minutes: 10_080 },
        }];
        assert_eq!(gemini_weekly.pct, 17.0); // 100 - 83
        assert!(!gemini_weekly.exceeded);
        assert_eq!(
            gemini_weekly.resets_at,
            Some(Utc.with_ymd_and_hms(2026, 11, 28, 3, 15, 21).unwrap())
        );

        let claude_gpt_5h = &sample.windows[&WindowKey {
            provider: Provider::Gemini,
            kind: WindowKind::Custom("Claude and GPT models 5h".into()),
        }];
        assert_eq!(claude_gpt_5h.pct, 25.0); // 100 - 75

        let claude_gpt_weekly = &sample.windows[&WindowKey {
            provider: Provider::Gemini,
            kind: WindowKind::WeeklySurface("Claude and GPT models".into()),
        }];
        assert_eq!(claude_gpt_weekly.pct, 0.0);

        let argon_weekly = &sample.windows[&WindowKey {
            provider: Provider::Gemini,
            kind: WindowKind::WeeklyModel(ModelId("Gemini 4 Argon".into())),
        }];
        assert_eq!(argon_weekly.pct, 0.0);
    }

    #[test]
    fn zero_remaining_marks_window_exceeded_and_rejects_auth_or_empty_output() {
        // why: 0% remaining is an exhausted window (pct=100, exceeded=true);
        //      interactive auth prompts and empty/malformed output must fail closed.
        // catches: missing exceeded flag at 0% remaining, or returning an empty
        //          UsageSample instead of Auth/Other error.
        let now = Utc.with_ymd_and_hms(2026, 11, 21, 5, 25, 0).unwrap();
        let sample = parse_usage_tsv(
            "Gemini Models\tFive Hour Limit Remaining\t0%\t2026-11-21T08:15:21Z\n",
            now,
        )
        .unwrap();
        let window = &sample.windows[&WindowKey {
            provider: Provider::Gemini,
            kind: WindowKind::Rolling { minutes: 300 },
        }];
        assert_eq!(window.pct, 100.0);
        assert!(window.exceeded);

        assert!(matches!(
            parse_usage_tsv(
                "Authentication required. Please visit the URL to log in:\n",
                now
            ),
            Err(AdapterError::Auth)
        ));
        assert!(matches!(
            parse_usage_tsv("", now),
            Err(AdapterError::Other(_))
        ));
        assert!(matches!(
            parse_usage_tsv(
                "Gemini Models\tFive Hour Limit Remaining\t50%\tnot-a-date\n",
                now
            ),
            Err(AdapterError::Other(_))
        ));
    }

    // why: a missing optional CLI is unsupported, never a retryable provider outage.
    #[tokio::test]
    async fn fix1_missing_agy_is_unsupported() {
        let reader = AgyCliUsageReader::new("/missing-uw-fix1/agy", None);
        assert!(matches!(reader.read_usage_tsv().await, Err(AdapterError::Unsupported)));
    }

    // why: one malformed quota row must not discard healthy sibling windows.
    #[test]
    fn fix1_malformed_usage_rows_do_not_drop_healthy_windows() {
        for malformed in ["bad%\t-", "NaN%\t-", "50%\tbad-date"] {
            let output = format!("Gemini Models\tFive Hour Limit Remaining\t70%\t-\nOther\tBad row\t{malformed}\nGemini Models\tWeekly Limit Remaining\t80%\t-\n");
            let sample = parse_usage_tsv(&output, Utc::now()).unwrap();
            assert_eq!(sample.windows.len(), 2);
            assert_eq!(sample.windows[&WindowKey { provider: Provider::Gemini, kind: WindowKind::Rolling { minutes: 300 } }].pct, 30.0);
            assert_eq!(sample.windows[&WindowKey { provider: Provider::Gemini, kind: WindowKind::Rolling { minutes: 10080 } }].pct, 20.0);
        }
    }

    #[test]
    fn decodes_literal_gen_metadata_and_step_timestamp_protobuf_bytes() {
        // why: per-call token accounting comes from gen_metadata protobuf blobs
        //      and step timestamps from steps.metadata; test against independent
        //      hand-encoded bytes so decoder bugs cannot hide in a shared encoder.
        // catches: wrong protobuf field numbers, varint decoding bugs, or panics
        //          on truncated blobs.
        let call0 = parse_gen_metadata_blob(SAMPLE_GEN_METADATA_CALL_0).unwrap();
        assert_eq!(
            call0,
            GenCallUsage {
                new_input_tokens: 34_448,
                cached_input_tokens: 0,
                output_tokens: 126,
                context_tokens: Some(44_211),
                model: Some("gemini-4-neon".into()),
                last_step_index: Some(0),
            }
        );

        let call1 = parse_gen_metadata_blob(SAMPLE_GEN_METADATA_CALL_1).unwrap();
        assert_eq!(
            call1,
            GenCallUsage {
                new_input_tokens: 2_075,
                cached_input_tokens: 34_449,
                output_tokens: 540,
                context_tokens: Some(46_547),
                model: Some("gemini-4-neon".into()),
                last_step_index: Some(2),
            }
        );

        assert_eq!(
            parse_step_timestamp(SAMPLE_STEP_TS_1),
            Some(Utc.with_ymd_and_hms(2026, 11, 21, 5, 18, 29).unwrap())
        );
        assert_eq!(
            parse_step_timestamp(SAMPLE_STEP_TS_3),
            Some(Utc.with_ymd_and_hms(2026, 11, 21, 5, 18, 33).unwrap())
        );
        assert_eq!(parse_gen_metadata_blob(&[0x0a, 0xff, 0x01]), None);
        assert_eq!(parse_step_timestamp(&[0x0a, 0x05, 0x08]), None);
    }

    #[test]
    fn profile_discovery_prefers_t3_profile_with_oauth_token_then_home_gemini() {
        // why: T3 stores Antigravity auth under ~/.t3/userdata/providers/antigravity/<id>/
        //      antigravity-acp/antigravity-oauth-token; fallback is ~/.gemini.
        // catches: picking an unauthenticated directory or failing to fall back to ~/.gemini.
        let root =
            std::env::temp_dir().join(format!("usagewindow-ag-profile-{}", uuid::Uuid::new_v4()));
        let t3 = root.join("t3");
        let empty_profile = t3.join("providers/antigravity/0000-empty");
        let authed_profile = t3.join("providers/antigravity/1111-authed");
        fs::create_dir_all(&empty_profile).unwrap();
        fs::create_dir_all(authed_profile.join("antigravity-acp")).unwrap();
        fs::write(
            authed_profile.join("antigravity-acp/antigravity-oauth-token"),
            b"synthetic",
        )
        .unwrap();

        let home = root.join("home");
        fs::create_dir_all(home.join(".gemini")).unwrap();

        assert_eq!(
            discover_profile_dir(Some(&t3), Some(&home)),
            Some(authed_profile)
        );
        assert_eq!(
            discover_profile_dir(Some(&root.join("missing-t3")), Some(&home)),
            Some(home.join(".gemini"))
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn env_strip_filter_selects_only_antigravity_prefixed_keys() {
        // proves: CLAIM(agy-env-strip)
        let keys = vec![
            OsString::from("PATH"),
            OsString::from("GEMINI_HOME"),
            OsString::from("ANTIGRAVITY_LS_ADDRESS"),
            OsString::from("ANTIGRAVITY_APP_DATA_DIR"),
            OsString::from("HOME"),
        ];
        assert_eq!(
            antigravity_env_keys_to_remove(keys),
            vec![
                OsString::from("ANTIGRAVITY_LS_ADDRESS"),
                OsString::from("ANTIGRAVITY_APP_DATA_DIR"),
            ]
        );
    }

    #[tokio::test]
    async fn agy_cli_reader_strips_inherited_antigravity_env_and_passes_profile() {
        // why: inheriting ANTIGRAVITY_* vars from an active ACP parent causes
        //      `agy --print /usage` to ignore stored tokens and hang on interactive
        //      OAuth (verified 2026-11-20).
        // proves: CLAIM(agy-env-strip)
        // catches: forwarding parent ANTIGRAVITY_* variables or omitting GEMINI_HOME
        //          / AGY_ACP_FORCE_FILE_STORAGE=1.
        let dir =
            std::env::temp_dir().join(format!("usagewindow-agy-cli-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let profile = dir.join("profile");
        fs::create_dir_all(&profile).unwrap();
        let script = dir.join("fake-agy");
        fs::write(
            &script,
            "#!/bin/sh\n\
             if [ -n \"$ANTIGRAVITY_LS_ADDRESS\" ]; then echo 'leaked ANTIGRAVITY_LS_ADDRESS' >&2; exit 2; fi\n\
             if [ \"$AGY_ACP_FORCE_FILE_STORAGE\" != '1' ]; then echo 'missing file storage' >&2; exit 3; fi\n\
             if [ \"$AGY_CLI_DISABLE_AUTO_UPDATE\" != '1' ]; then echo 'missing disable auto update' >&2; exit 4; fi\n\
             if [ -z \"$GEMINI_HOME\" ]; then echo 'missing GEMINI_HOME' >&2; exit 5; fi\n\
             printf 'Gemini Models\\tFive Hour Limit Remaining\\t60%%\\t2026-11-21T08:00:00Z\\n'\n",
        )
        .unwrap();
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();

        let reader = AgyCliUsageReader::new(script.to_string_lossy(), Some(profile));
        let output = reader.read_usage_tsv().await.unwrap();
        assert!(output.contains("Gemini Models\tFive Hour Limit Remaining\t60%"));

        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn discovers_t3_antigravity_threads_with_thread_id_and_conversation_tokens() {
        // why: T3 antigravity threads must appear as Provider::Gemini sessions keyed
        //      by their T3 thread id, enriched with cached vs uncached input tokens
        //      from the conversation DB; deleted and non-antigravity threads must be
        //      skipped.
        // proves: CLAIM(antigravity-cached-subset)
        // catches: keying by conversation UUID instead of T3 thread_id, double-counting
        //          cached input or failing to make cached_input_tokens a subset of
        //          input_tokens, or leaking deleted/non-antigravity threads.
        let dir =
            std::env::temp_dir().join(format!("usagewindow-ag-discover-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let state_db = dir.join("statev2.sqlite");
        let conv_dir = dir.join("conversations");
        fs::create_dir_all(&conv_dir).unwrap();

        let conv_id = "11111111-2222-3333-4444-555555555555";
        let conv_db_path = conv_dir.join(format!("{conv_id}.db"));
        {
            let conn = rusqlite::Connection::open(&conv_db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB, size INTEGER NOT NULL DEFAULT 0);
                 CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER NOT NULL DEFAULT 0, metadata BLOB);",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO steps (idx, step_type, metadata) VALUES (?1, 15, ?2), (?3, 15, ?4)",
                rusqlite::params![1i64, SAMPLE_STEP_TS_1, 3i64, SAMPLE_STEP_TS_3],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO gen_metadata (idx, data, size) VALUES (0, ?1, 0), (1, ?2, 0)",
                rusqlite::params![SAMPLE_GEN_METADATA_CALL_0, SAMPLE_GEN_METADATA_CALL_1],
            )
            .unwrap();
        }

        {
            let conn = rusqlite::Connection::open(&state_db).unwrap();
            conn.execute_batch(
                "CREATE TABLE projection_projects (
                    project_id TEXT PRIMARY KEY,
                    workspace_root TEXT NOT NULL
                );
                CREATE TABLE orchestration_v2_projection_threads (
                    thread_id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    default_provider TEXT NOT NULL,
                    active_provider_thread_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    archived_at TEXT,
                    deleted_at TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE orchestration_v2_projection_provider_threads (
                    provider_thread_id TEXT PRIMARY KEY,
                    thread_id TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    provider_session_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE orchestration_v2_projection_provider_sessions (
                    provider_session_id TEXT PRIMARY KEY,
                    thread_id TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                INSERT INTO projection_projects (project_id, workspace_root)
                VALUES ('proj-1', '/workspace/mstack');

                INSERT INTO orchestration_v2_projection_threads
                    (thread_id, project_id, title, default_provider, active_provider_thread_id,
                     created_at, updated_at, archived_at, deleted_at, payload_json)
                VALUES
                    ('t3-thread-neon-1', 'proj-1', 'Neon Feature Thread', 'antigravity', 'pt-1',
                     '2026-11-21T05:18:26Z', '2026-11-21T05:20:00Z', NULL, NULL,
                     '{\"modelSelection\":{\"model\":\"gemini-4-neon\"},\"worktreePath\":\"/workspace/mstack.wt\"}'),
                    ('t3-thread-deleted', 'proj-1', 'Deleted Neon', 'antigravity', 'pt-del',
                     '2026-11-21T03:00:00Z', '2026-11-21T03:05:00Z', NULL, '2026-11-21T03:10:00Z',
                     '{\"modelSelection\":{\"model\":\"gemini-4-neon\"}}'),
                    ('t3-thread-claude', 'proj-1', 'Claude Thread', 'claudeAgent', 'pt-cl',
                     '2026-11-21T05:00:00Z', '2026-11-21T05:10:00Z', NULL, NULL,
                     '{\"modelSelection\":{\"model\":\"claude-opus-4-6\"}}');

                INSERT INTO orchestration_v2_projection_provider_threads
                    (provider_thread_id, thread_id, provider, provider_session_id, status, updated_at, payload_json)
                VALUES
                    ('pt-1', 't3-thread-neon-1', 'antigravity', 'ps-1', 'active', '2026-11-21T05:21:00Z',
                     '{\"nativeThreadRef\":{\"driver\":\"antigravity\",\"nativeId\":\"11111111-2222-3333-4444-555555555555\"},\"contextUsage\":{\"usedTokens\":46547,\"maxTokens\":1048576}}'),
                    ('pt-del', 't3-thread-deleted', 'antigravity', 'ps-del', 'idle', '2026-11-21T03:05:00Z',
                     '{\"nativeThreadRef\":{\"driver\":\"antigravity\",\"nativeId\":\"99999999-9999-9999-9999-999999999999\"}}'),
                    ('pt-cl', 't3-thread-claude', 'claudeAgent', 'ps-cl', 'idle', '2026-11-21T05:10:00Z',
                     '{\"nativeThreadRef\":{\"driver\":\"claudeAgent\",\"nativeId\":\"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\"}}');

                INSERT INTO orchestration_v2_projection_provider_sessions
                    (provider_session_id, thread_id, provider, status, updated_at, payload_json)
                VALUES
                    ('ps-1', 't3-thread-neon-1', 'antigravity', 'ready', '2026-11-21T05:18:28Z',
                     '{\"cwd\":\"/workspace/fallback\"}');",
            )
            .unwrap();
        }

        let adapter =
            AntigravityAdapter::new(Arc::new(FakeUsageReader(Err(AdapterError::Unsupported))))
                .with_t3_state_db(state_db)
                .with_conversations_dir(conv_dir);

        let sessions = adapter.discover_sessions().await.unwrap();
        assert_eq!(sessions.len(), 1);
        let found = &sessions[0];
        assert_eq!(found.id, SessionId("t3-thread-neon-1".into()));
        assert_eq!(found.title.as_deref(), Some("Neon Feature Thread"));
        assert_eq!(found.cwd, "/workspace/mstack.wt");
        assert_eq!(found.model, Some(ModelId("gemini-4-neon".into())));
        assert_eq!(found.context_window_size, Some(1_048_576));
        assert_eq!(found.last_known_token_count, Some(46_547));
        assert_eq!(
            found.state_path.as_deref(),
            Some(conv_db_path.to_string_lossy().as_ref())
        );
        assert_eq!(
            found.first_seen,
            Some(Utc.with_ymd_and_hms(2026, 11, 21, 5, 18, 26).unwrap())
        );
        assert_eq!(
            found.last_seen,
            Some(Utc.with_ymd_and_hms(2026, 11, 21, 5, 21, 0).unwrap())
        );
        assert_eq!(found.token_usage.len(), 2);

        // Call 0: new=34448, cached=0, out=126 => input=34448, total=34574
        assert_eq!(
            found.token_usage[0],
            TokenUsageRecord {
                at: Utc.with_ymd_and_hms(2026, 11, 21, 5, 18, 29).unwrap(),
                model: Some(ModelId("gemini-4-neon".into())),
                input_tokens: 34_448,
                cached_input_tokens: 0,
                cache_write_input_tokens: 0,
                output_tokens: 126,
                reasoning_output_tokens: 0,
                total_tokens: 34_574,
                context_pct: None,
            }
        );
        // Call 1: new=2075, cached=34449, out=540 => input=36524 (cached is subset), total=37064
        assert_eq!(
            found.token_usage[1],
            TokenUsageRecord {
                at: Utc.with_ymd_and_hms(2026, 11, 21, 5, 18, 33).unwrap(),
                model: Some(ModelId("gemini-4-neon".into())),
                input_tokens: 36_524,
                cached_input_tokens: 34_449,
                cache_write_input_tokens: 0,
                output_tokens: 540,
                reasoning_output_tokens: 0,
                total_tokens: 37_064,
                context_pct: None,
            }
        );

        // why: a 24-minute hit in ttl-probe.log permits a 23-minute safety TTL.
        // catches: leaving Gemini on the five-minute legacy fallback.
        assert_eq!(found.cache_ttl, Some(chrono::Duration::minutes(23)));

        // Second discovery hits the conversation cache cleanly and keeps parity.
        let second = adapter.discover_sessions().await.unwrap();
        assert_eq!(second, sessions);

        // why: SQLite can append calls entirely in WAL while the main file is unchanged.
        let writer = rusqlite::Connection::open(&conv_db_path).unwrap();
        writer.pragma_update(None, "journal_mode", "WAL").unwrap();
        writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        adapter.discover_sessions().await.unwrap();
        let before = fs::metadata(&conv_db_path).unwrap();
        writer.execute("INSERT INTO gen_metadata(idx,data,size) VALUES(2,?1,?2)", rusqlite::params![SAMPLE_GEN_METADATA_CALL_0, SAMPLE_GEN_METADATA_CALL_0.len() as i64]).unwrap();
        let after = fs::metadata(&conv_db_path).unwrap();
        assert_eq!((before.len(), before.modified().unwrap()), (after.len(), after.modified().unwrap()));
        assert_eq!(adapter.discover_sessions().await.unwrap()[0].token_usage.len(), 3);
        drop(writer);
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn discover_sessions_returns_empty_when_state_db_is_missing() {
        // why: hosts without T3Code installed must not error on session discovery.
        // catches: failing the observation tick when statev2.sqlite does not exist.
        let adapter =
            AntigravityAdapter::new(Arc::new(FakeUsageReader(Err(AdapterError::Unsupported))))
                .with_t3_state_db(std::env::temp_dir().join(format!(
                    "usagewindow-missing-{}.sqlite",
                    uuid::Uuid::new_v4()
                )));
        assert_eq!(adapter.discover_sessions().await.unwrap(), Vec::new());
    }

    #[tokio::test]
    async fn antigravity_adapter_is_strictly_observe_only() {
        // why: uw must never trigger compaction, advise, interrupt, or resume
        //      on Gemini sessions in this change.
        // catches: advertising any destructive capability or implementing an
        //          unintended action path.
        let adapter =
            AntigravityAdapter::new(Arc::new(FakeUsageReader(Err(AdapterError::Unsupported))));
        assert_eq!(adapter.provider(), Provider::Gemini);
        let caps = adapter.capabilities();
        assert!(!caps.can_trigger_compaction);
        assert!(!caps.can_advise_mid_turn);
        assert!(!caps.can_inject_at_session_start);
        assert!(!caps.can_observe_compaction);
        assert!(caps.reports_token_counts);
        assert!(!caps.headless_resume);
        assert!(caps.seed_modes.is_empty());

        let now = Utc::now();
        let summary = SessionSummary {
            id: SessionId("t3-thread-neon-1".into()),
            lineage: SessionLineage::default(),
            harness: Provider::Gemini,
            model: Some(ModelId("gemini-4-neon".into())),
            account: None,
            first_seen: now,
            last_seen: now,
            cwd: "/tmp".into(),
            state_path: None,
            context_window_size: Some(1_048_576),
            last_known_token_count: Some(50_000),
            last_known_context_pct: None,
            cache_ttl: None,
            launch_mode: LaunchMode::Interactive,
            pid: None,
            stopped_reason: None,
            superseded_stop_reason: None,
            superseded_stop_reason_at: None,
            superseded_stop_reason_note: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        };
        let req = CompactionRequest {
            id: uuid::Uuid::new_v4(),
            session_id: summary.id.clone(),
            kind: CompactionKind::OpportunisticIdle,
            preempt: false,
            resume_after_compaction: false,
            prompt: "/compact".into(),
            reason: "test".into(),
            status: CompactionStatus::Pending,
            created_at: now,
        };
        assert_eq!(adapter.detect_stop(&summary.id).await.unwrap(), None);
        assert!(matches!(
            adapter.compact(&summary, &req).await,
            Err(AdapterError::Unsupported)
        ));
        assert!(matches!(
            adapter.resume_session(&summary, None).await,
            Err(AdapterError::Unsupported)
        ));
        assert!(matches!(
            adapter.advise(&summary.id, "hi").await,
            Err(AdapterError::Unsupported)
        ));
        assert!(matches!(
            adapter.interrupt(&summary.id).await,
            Err(AdapterError::Unsupported)
        ));
        assert!(matches!(
            adapter
                .emit_status(&summary.id, StatusEvent::AutoResumeCanceled)
                .await,
            Err(AdapterError::Unsupported)
        ));
    }
}
