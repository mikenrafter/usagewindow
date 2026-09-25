use crate::claude_code::TranscriptFileSystem;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, DiscoveredSession, HarnessAdapter,
    StatusEvent,
};
use uw_core::model::*;

/// Cursor's own on-disk transcript store for CLI/background-agent sessions
/// (`cursor-agent`, not the IDE chat panel, which keeps its state in a
/// separate sqlite store under `~/.cursor/chats/`). Layout verified
/// 2026-09-22: `<root>/<encoded-cwd>/agent-transcripts/<session-uuid>/<session-uuid>.jsonl`,
/// one `{"role":...,"message":...}` object per line with no per-line
/// timestamp, session id, or cwd field — unlike Claude Code/Codex transcripts.
pub struct CursorTranscriptFileSystem;

#[async_trait]
impl TranscriptFileSystem for CursorTranscriptFileSystem {
    async fn jsonl_files(&self) -> AdapterResult<Vec<String>> {
        let root = std::env::var("CURSOR_PROJECTS_DIR")
            .unwrap_or_else(|_| std::env::var("HOME").unwrap_or_default() + "/.cursor/projects");
        let mut out = Vec::new();
        let mut dirs = vec![root];
        while let Some(dir) = dirs.pop() {
            let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
                continue;
            };
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| AdapterError::Other(e.to_string()))?
            {
                let path = entry.path();
                if path.is_dir() {
                    dirs.push(path.to_string_lossy().into_owned());
                } else if path.extension().is_some_and(|x| x == "jsonl") {
                    out.push(path.to_string_lossy().into_owned());
                }
            }
        }
        Ok(out)
    }

    async fn read_to_string(&self, path: &str) -> AdapterResult<String> {
        tokio::fs::read_to_string(path)
            .await
            .map_err(|e| AdapterError::Other(e.to_string()))
    }
}

struct CachedCursorTranscript {
    fingerprint: (u64, Option<std::time::SystemTime>),
    session: DiscoveredSession,
}

/// Best-effort decode of Cursor's dash-joined project directory name back
/// into an absolute cwd (`home-v0id-Documents-repos-dozens-game` ->
/// `/home/v0id/Documents/repos/dozens-game`). This is lossy whenever a path
/// segment itself contains a literal `-` (Cursor's transcripts carry no
/// structured cwd field to disambiguate, unlike Claude Code/Codex), so it is
/// only used as a fallback label, never for anything that requires an exact
/// filesystem path.
fn decode_project_dir_name(name: &str) -> String {
    format!("/{}", name.replace('-', "/"))
}

/// Extracts the project directory name from a transcript path shaped like
/// `<root>/<project>/agent-transcripts/<session>/<session>.jsonl`.
fn project_dir_name(path: &str) -> Option<&str> {
    let path = std::path::Path::new(path);
    let mut components: Vec<&str> = path
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect();
    let index = components.iter().rposition(|c| *c == "agent-transcripts")?;
    components.truncate(index);
    components.pop()
}

fn first_user_text(content: &str) -> Option<String> {
    for line in content.lines() {
        let record: Value = serde_json::from_str(line).ok()?;
        if record.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let parts = record.pointer("/message/content")?.as_array()?;
        for part in parts {
            if let Some(text) = part.get("text").and_then(Value::as_str) {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.chars().take(120).collect());
                }
            }
        }
    }
    None
}

/// Cursor transcripts carry no per-line timestamp, so `modified` (the
/// transcript file's own mtime) is the only activity signal available and is
/// used for both `first_seen` and `last_seen` — an approximation, but a
/// closer one than leaving the daemon to default both to "now" on every poll.
fn scan_cursor_transcript(
    path: &str,
    content: &str,
    modified: Option<std::time::SystemTime>,
) -> Option<DiscoveredSession> {
    let id = std::path::Path::new(path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| uuid::Uuid::parse_str(stem).is_ok())?
        .to_owned();
    let cwd = project_dir_name(path)
        .map(decode_project_dir_name)
        .unwrap_or_else(|| ".".into());
    let title = first_user_text(content);
    let seen = modified.map(DateTime::<Utc>::from);
    Some(DiscoveredSession {
        id: SessionId(id),
        lineage: SessionLineage::default(),
        cwd,
        model: None,
        context_window_size: None,
        last_known_token_count: None,
        first_seen: seen,
        last_seen: seen,
        state_path: Some(path.to_owned()),
        token_usage: Vec::new(),
        title,
    })
}

fn decode_hex(input: &str) -> Option<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return None;
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digits = std::str::from_utf8(pair).ok()?;
            u8::from_str_radix(digits, 16).ok()
        })
        .collect()
}

fn cursor_metadata_session_id(value: Option<&Value>) -> Option<SessionId> {
    let id = value?.as_str()?;
    uuid::Uuid::parse_str(id).ok()?;
    Some(SessionId(id.to_owned()))
}

fn read_cursor_chat_lineage(
    chats_root: &std::path::Path,
    session_id: &SessionId,
) -> SessionLineage {
    let Ok(workspaces) = std::fs::read_dir(chats_root) else {
        return SessionLineage::default();
    };
    for workspace in workspaces.flatten() {
        let store_path = workspace.path().join(&session_id.0).join("store.db");
        if !store_path.is_file() {
            continue;
        }
        let Ok(connection) = rusqlite::Connection::open_with_flags(
            store_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ) else {
            continue;
        };
        let Ok(encoded) =
            connection.query_row("SELECT value FROM meta WHERE key = '0'", [], |row| {
                row.get::<_, String>(0)
            })
        else {
            continue;
        };
        let Some(bytes) = decode_hex(&encoded) else {
            continue;
        };
        let Ok(metadata) = serde_json::from_slice::<Value>(&bytes) else {
            continue;
        };
        if metadata.get("agentId").and_then(Value::as_str) != Some(session_id.0.as_str()) {
            return SessionLineage::default();
        }
        let subagent = metadata.get("subagentInfo");
        return SessionLineage {
            parent: cursor_metadata_session_id(
                subagent.and_then(|value| value.get("parentAgentId")),
            ),
            root: cursor_metadata_session_id(
                subagent.and_then(|value| value.get("rootParentAgentId")),
            ),
            related: Vec::new(),
        };
    }
    SessionLineage::default()
}

#[async_trait]
pub trait CursorUsageTransport: Send + Sync {
    async fn current_period_usage(&self) -> AdapterResult<Value>;
}

/// The dashboard usage response has no plan-name field (only the two
/// percentage bars), so plan comes from a separate source: `cursor-agent
/// about`, verified 2026-09-22 against a live run:
/// ```text
/// About Cursor CLI
///
/// CLI Version         2026.09.18-9a7762b
/// ...
/// Subscription Tier   Pro
/// ...
/// ```
#[async_trait]
pub trait CursorPlanReader: Send + Sync {
    async fn read(&self) -> AdapterResult<String>;
}
pub struct CursorCliPlanReader;
#[async_trait]
impl CursorPlanReader for CursorCliPlanReader {
    async fn read(&self) -> AdapterResult<String> {
        let output = tokio::process::Command::new("cursor-agent")
            .arg("about")
            .output()
            .await
            .map_err(|e| AdapterError::Transient(e.to_string()))?;
        if !output.status.success() {
            return Err(AdapterError::Other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

/// `about`'s output is plain `Label<spaces>Value` lines, not structured data.
fn plan_from_about_output(output: &str) -> Option<String> {
    output.lines().find_map(|line| {
        line.strip_prefix("Subscription Tier")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CursorAuth {
    Bearer(String),
    Cookie(String),
}

impl CursorAuth {
    /// Returns every candidate credential this environment has, in the order
    /// they should be tried. An explicit env var override is trusted alone; the
    /// IDE and cursor-agent CLI sources are both always collected because they
    /// expire independently (see `discover`).
    pub fn discover_from_environment() -> Vec<Self> {
        if let Ok(token) = std::env::var("UW_CURSOR_ACCESS_TOKEN")
            && !token.is_empty()
        {
            return vec![Self::Bearer(token)];
        }
        if let Ok(cookie) = std::env::var("UW_CURSOR_SESSION_COOKIE")
            && !cookie.is_empty()
        {
            return vec![Self::Cookie(cookie)];
        }
        if let Ok(token) = std::env::var("CURSOR_AUTH_TOKEN")
            && !token.is_empty()
        {
            return vec![Self::Bearer(token)];
        }

        let Some(home) = std::env::var_os("HOME").map(std::path::PathBuf::from) else {
            return Vec::new();
        };
        let config = std::env::var_os("XDG_CONFIG_HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| home.join(".config"));
        let state_db = std::env::var_os("CURSOR_STATE_DB")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| config.join("Cursor/User/globalStorage/state.vscdb"));
        let cli_auth = std::env::var_os("CURSOR_CLI_AUTH_FILE")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| config.join("cursor/auth.json"));
        Self::discover(Some(&state_db), Some(&cli_auth))
    }

    /// The Cursor IDE and the standalone `cursor-agent` CLI keep entirely
    /// separate access tokens, and either one can be the stale one: a user
    /// who only runs `cursor-agent` has a long-expired IDE token sitting in
    /// `state.vscdb` from whenever they last opened the IDE, while a user who
    /// mostly lives in the IDE may have let a `cursor-agent login` token lapse.
    /// Preferring the IDE source unconditionally (as pure precedence would)
    /// means a dead IDE session permanently shadows a perfectly good CLI one.
    /// So: collect both discovered tokens, and sort the unexpired ones first
    /// (stable sort keeps the IDE-first tie-break when both are equally
    /// valid/invalid). A token this can't decode is treated as unexpired —
    /// trust it and let the API be the final arbiter.
    pub fn discover(
        state_db: Option<&std::path::Path>,
        cli_auth: Option<&std::path::Path>,
    ) -> Vec<Self> {
        let mut tokens: Vec<String> = [
            state_db.and_then(read_access_token_from_state_db),
            cli_auth.and_then(read_access_token_from_cli_auth),
        ]
        .into_iter()
        .flatten()
        .collect();
        tokens.sort_by_key(|token| jwt_is_expired(token));
        tokens.into_iter().map(Self::Bearer).collect()
    }
}

/// Decodes a JWT's unverified payload segment and checks its `exp` claim
/// against the current time. Cursor's access tokens are JWTs; this never
/// validates a signature and must only be used to pick which already-issued,
/// already-trusted local credential to try first.
fn jwt_is_expired(token: &str) -> bool {
    let Some(payload) = token.split('.').nth(1) else {
        return false;
    };
    let mut padded = payload.to_owned();
    while padded.len() % 4 != 0 {
        padded.push('=');
    }
    let Ok(bytes) = base64_url_decode(&padded) else {
        return false;
    };
    let Ok(claims) = serde_json::from_slice::<Value>(&bytes) else {
        return false;
    };
    let Some(exp) = claims.get("exp").and_then(Value::as_i64) else {
        return false;
    };
    exp < Utc::now().timestamp()
}

/// Minimal base64url decoder (RFC 4648 §5) so this file doesn't need a base64
/// crate dependency just to read one JWT claim.
fn base64_url_decode(input: &str) -> Result<Vec<u8>, ()> {
    let mut value: u32 = 0;
    let mut bits = 0u32;
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    for c in input.bytes() {
        let digit = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            b'=' => continue,
            _ => return Err(()),
        } as u32;
        value = (value << 6) | digit;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((value >> bits) as u8);
        }
    }
    Ok(out)
}

fn read_access_token_from_state_db(path: &std::path::Path) -> Option<String> {
    let connection = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .ok()?;
    connection
        .query_row(
            "SELECT value FROM ItemTable WHERE key = 'cursorAuth/accessToken'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .filter(|token| !token.is_empty())
}

fn read_access_token_from_cli_auth(path: &std::path::Path) -> Option<String> {
    let value: Value = serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
    value
        .get("accessToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

pub struct CursorHttpTransport {
    client: reqwest::Client,
    /// Tried in order on every call; a candidate that fails with
    /// `AdapterError::Auth` is skipped in favor of the next one, since the IDE
    /// and cursor-agent CLI tokens expire independently and either can be the
    /// stale one. The last candidate's error (or `Auth` if candidates is
    /// empty) is returned when every candidate fails.
    candidates: Vec<CursorAuth>,
}

impl CursorHttpTransport {
    pub fn new(candidates: Vec<CursorAuth>) -> Self {
        Self {
            client: reqwest::Client::new(),
            candidates,
        }
    }

    async fn call_with(&self, auth: &CursorAuth) -> AdapterResult<Value> {
        let mut request = self
            .client
            .post("https://api2.cursor.sh/aiserver.v1.DashboardService/GetCurrentPeriodUsage")
            .header("Content-Type", "application/json")
            .header("Connect-Protocol-Version", "1")
            .json(&Value::Object(Default::default()));
        request = match auth {
            CursorAuth::Bearer(token) => request.bearer_auth(token),
            CursorAuth::Cookie(cookie) => request.header(reqwest::header::COOKIE, cookie),
        };
        let response = request
            .send()
            .await
            .map_err(|error| AdapterError::Transient(error.to_string()))?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            || response.status() == reqwest::StatusCode::FORBIDDEN
        {
            return Err(AdapterError::Auth);
        }
        if response.status().is_server_error()
            || response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            return Err(AdapterError::Transient(format!(
                "HTTP {}",
                response.status()
            )));
        }
        if !response.status().is_success() {
            return Err(AdapterError::Other(format!("HTTP {}", response.status())));
        }
        response
            .json()
            .await
            .map_err(|error| AdapterError::Other(error.to_string()))
    }
}

#[async_trait]
impl CursorUsageTransport for CursorHttpTransport {
    async fn current_period_usage(&self) -> AdapterResult<Value> {
        for (index, auth) in self.candidates.iter().enumerate() {
            match self.call_with(auth).await {
                Ok(value) => return Ok(value),
                Err(AdapterError::Auth) if index + 1 < self.candidates.len() => continue,
                Err(error) => return Err(error),
            }
        }
        Err(AdapterError::Auth)
    }
}

pub struct CursorAdapter {
    transport: Arc<dyn CursorUsageTransport>,
    now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    account: Option<AccountId>,
    transcript_fs: Arc<dyn TranscriptFileSystem>,
    discovery_cache: Mutex<HashMap<String, CachedCursorTranscript>>,
    plan_reader: Arc<dyn CursorPlanReader>,
    chat_metadata_root: Option<std::path::PathBuf>,
}

impl CursorAdapter {
    pub fn new(transport: Arc<dyn CursorUsageTransport>) -> Self {
        Self {
            transport,
            now: Arc::new(Utc::now),
            account: None,
            transcript_fs: Arc::new(CursorTranscriptFileSystem),
            discovery_cache: Mutex::new(HashMap::new()),
            plan_reader: Arc::new(CursorCliPlanReader),
            chat_metadata_root: std::env::var_os("CURSOR_CHATS_DIR")
                .map(std::path::PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HOME")
                        .map(std::path::PathBuf::from)
                        .map(|home| home.join(".cursor/chats"))
                }),
        }
    }

    pub fn real(candidates: Vec<CursorAuth>) -> Self {
        Self::new(Arc::new(CursorHttpTransport::new(candidates))).with_account(read_cached_email())
    }

    pub fn with_account(mut self, account: Option<AccountId>) -> Self {
        self.account = account;
        self
    }

    pub fn with_clock(mut self, now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    pub fn with_transcript_fs(mut self, fs: Arc<dyn TranscriptFileSystem>) -> Self {
        self.transcript_fs = fs;
        self
    }

    pub fn with_plan_reader(mut self, plan_reader: Arc<dyn CursorPlanReader>) -> Self {
        self.plan_reader = plan_reader;
        self
    }

    pub fn with_chat_metadata_root(mut self, root: std::path::PathBuf) -> Self {
        self.chat_metadata_root = Some(root);
        self
    }

    pub fn capabilities_static() -> Capabilities {
        Capabilities {
            can_trigger_compaction: false,
            can_advise_mid_turn: false,
            can_inject_at_session_start: false,
            can_observe_compaction: false,
            reports_token_counts: false,
            headless_resume: false,
            seed_modes: vec![],
        }
    }

    async fn lineage_for(&self, session_id: &SessionId) -> SessionLineage {
        let Some(root) = self.chat_metadata_root.clone() else {
            return SessionLineage::default();
        };
        let session_id = session_id.clone();
        tokio::task::spawn_blocking(move || read_cursor_chat_lineage(&root, &session_id))
            .await
            .unwrap_or_default()
    }

    fn parse_sample(&self, value: &Value, plan: Option<String>) -> AdapterResult<UsageSample> {
        let plan_usage = value
            .get("planUsage")
            .ok_or_else(|| AdapterError::Other("missing Cursor planUsage".into()))?;
        let reset = value.get("billingCycleEnd").and_then(parse_epoch_millis);
        let auto = percentage(plan_usage, "autoPercentUsed")?;
        let api = percentage(plan_usage, "apiPercentUsed")?;
        let account = account_from_usage(value).or_else(|| self.account.clone());
        let mut windows = HashMap::new();
        for (name, pct) in [("auto", auto), ("api", api)] {
            windows.insert(
                WindowKey {
                    provider: Provider::Cursor,
                    kind: WindowKind::Custom(name.into()),
                },
                UsageWindowState::new(pct, pct >= 100.0, true, reset, None),
            );
        }
        let at = (self.now)();
        Ok(UsageSample {
            at,
            fetched_at: Some(at),
            source: UsageSource::ProviderReported,
            provider: Provider::Cursor,
            account,
            plan,
            windows,
            credits: None,
        })
    }
}

fn account_from_usage(value: &Value) -> Option<AccountId> {
    ["email", "accountEmail", "userEmail"]
        .into_iter()
        .chain(["/user/email", "/account/email"])
        .filter_map(|path| {
            if path.starts_with('/') {
                value.pointer(path).and_then(Value::as_str)
            } else {
                value.get(path).and_then(Value::as_str)
            }
        })
        .find(|value| !value.is_empty())
        .map(|value| AccountId(value.to_owned()))
}

/// Cursor IDE keeps the signed-in email beside its access token in the local
/// VS Code-compatible state database. It is read-only and optional: API-only
/// credentials may not have this file, and the usage response can still carry
/// an email in that case.
fn read_cached_email() -> Option<AccountId> {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from)?;
    let configured = std::env::var_os("CURSOR_STATE_DB").map(std::path::PathBuf::from);
    let xdg = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| home.join(".config"));
    let path = configured.unwrap_or_else(|| xdg.join("Cursor/User/globalStorage/state.vscdb"));
    let connection = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .ok()?;
    connection
        .query_row(
            "SELECT value FROM ItemTable WHERE key = 'cursorAuth/cachedEmail'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .filter(|email| !email.is_empty())
        .map(AccountId)
}

fn percentage(value: &Value, field: &str) -> AdapterResult<f32> {
    let pct = value
        .get(field)
        .and_then(Value::as_f64)
        .ok_or_else(|| AdapterError::Other(format!("missing Cursor {field}")))?
        as f32;
    if pct.is_finite() {
        Ok(pct)
    } else {
        Err(AdapterError::Other(format!("invalid Cursor {field}")))
    }
}

fn parse_epoch_millis(value: &Value) -> Option<DateTime<Utc>> {
    let millis = value
        .as_i64()
        .or_else(|| value.as_str()?.parse::<i64>().ok())?;
    Utc.timestamp_millis_opt(millis).single()
}

#[async_trait]
impl HarnessAdapter for CursorAdapter {
    fn provider(&self) -> Provider {
        Provider::Cursor
    }

    fn capabilities(&self) -> Capabilities {
        Self::capabilities_static()
    }

    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        let value = self.transport.current_period_usage().await?;
        // Best-effort: `cursor-agent` missing or `about`'s output shape
        // changing must not fail the usage fetch, just leave `plan` unset.
        let plan = self
            .plan_reader
            .read()
            .await
            .ok()
            .and_then(|output| plan_from_about_output(&output));
        self.parse_sample(&value, plan)
    }

    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        Err(AdapterError::Unsupported)
    }

    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        let mut discovered = Vec::new();
        for path in self.transcript_fs.jsonl_files().await? {
            let fingerprint = tokio::fs::metadata(&path)
                .await
                .ok()
                .map(|metadata| (metadata.len(), metadata.modified().ok()));
            let cached_session = if let Some(fingerprint) = fingerprint {
                let cached = self
                    .discovery_cache
                    .lock()
                    .map_err(|_| AdapterError::Other("discovery cache poisoned".into()))?;
                if let Some(entry) = cached.get(&path)
                    && entry.fingerprint == fingerprint
                {
                    Some(entry.session.clone())
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(mut session) = cached_session {
                session.lineage = self.lineage_for(&session.id).await;
                discovered.push(session);
                continue;
            }
            let content = self.transcript_fs.read_to_string(&path).await?;
            let modified = fingerprint.and_then(|(_, modified)| modified);
            if let Some(mut session) = scan_cursor_transcript(&path, &content, modified) {
                if let Some(fingerprint) = fingerprint {
                    self.discovery_cache
                        .lock()
                        .map_err(|_| AdapterError::Other("discovery cache poisoned".into()))?
                        .insert(
                            path,
                            CachedCursorTranscript {
                                fingerprint,
                                session: session.clone(),
                            },
                        );
                }
                session.lineage = self.lineage_for(&session.id).await;
                discovered.push(session);
            }
        }
        Ok(discovered)
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
    use serde_json::json;
    use uw_core::adapter::HarnessAdapter;

    struct FakeTransport(Value);

    #[async_trait]
    impl CursorUsageTransport for FakeTransport {
        async fn current_period_usage(&self) -> AdapterResult<Value> {
            Ok(self.0.clone())
        }
    }

    /// Always fails, so tests exercise `fetch_usage`'s plan-lookup fallback
    /// deterministically instead of spawning a real `cursor-agent` process
    /// (which may not be installed, and would report whatever plan is
    /// actually signed in on the test machine).
    struct FailingPlanReader;
    #[async_trait]
    impl CursorPlanReader for FailingPlanReader {
        async fn read(&self) -> AdapterResult<String> {
            Err(AdapterError::Other("no cursor-agent CLI in tests".into()))
        }
    }

    struct FakePlanReader(&'static str);
    #[async_trait]
    impl CursorPlanReader for FakePlanReader {
        async fn read(&self) -> AdapterResult<String> {
            Ok(self.0.into())
        }
    }

    #[tokio::test]
    async fn parses_cursor_two_bar_monthly_usage() {
        let adapter = CursorAdapter::new(Arc::new(FakeTransport(json!({
            "billingCycleEnd": "1771077734000",
            "email": "cursor@example.com",
            "planUsage": {
                "autoPercentUsed": 12.5,
                "apiPercentUsed": 87.25
            }
        }))))
        .with_plan_reader(Arc::new(FakePlanReader(
            "About Cursor CLI\n\nSubscription Tier   Pro\n",
        )));
        let sample = adapter.fetch_usage(None).await.unwrap();
        let reset = Utc.timestamp_millis_opt(1771077734000).single();
        assert_eq!(sample.provider, Provider::Cursor);
        assert_eq!(sample.account, Some(AccountId("cursor@example.com".into())));
        assert_eq!(sample.plan, Some("Pro".into()));
        assert_eq!(sample.windows.len(), 2);
        assert_eq!(
            sample.windows[&WindowKey {
                provider: Provider::Cursor,
                kind: WindowKind::Custom("auto".into())
            }]
                .pct,
            12.5
        );
        assert_eq!(
            sample.windows[&WindowKey {
                provider: Provider::Cursor,
                kind: WindowKind::Custom("api".into())
            }]
                .pct,
            87.25
        );
        assert_eq!(
            sample
                .windows
                .values()
                .map(|window| window.resets_at)
                .collect::<Vec<_>>(),
            vec![reset, reset]
        );
    }

    #[tokio::test]
    async fn clamps_percentages_and_marks_each_exhausted_bar() {
        let adapter = CursorAdapter::new(Arc::new(FakeTransport(json!({
            "planUsage": { "autoPercentUsed": 120, "apiPercentUsed": -5 }
        }))))
        .with_plan_reader(Arc::new(FailingPlanReader));
        let sample = adapter.fetch_usage(None).await.unwrap();
        let auto = &sample.windows[&WindowKey {
            provider: Provider::Cursor,
            kind: WindowKind::Custom("auto".into()),
        }];
        let api = &sample.windows[&WindowKey {
            provider: Provider::Cursor,
            kind: WindowKind::Custom("api".into()),
        }];
        assert_eq!(auto.pct, 100.0);
        assert!(auto.exceeded);
        assert_eq!(api.pct, 0.0);
        assert!(!api.exceeded);
    }

    #[test]
    fn extracts_plan_from_about_output() {
        let output = "About Cursor CLI\n\nCLI Version         2026.09.18-9a7762b\nLatest              2026.09.18-9a7762b (up to date)\nModel               Composer 2.5\nSubscription Tier   Pro\nOS                  linux (x64)\nTerminal            unknown\nShell               bash\nUser Email          customer@example.com\n";
        assert_eq!(plan_from_about_output(output), Some("Pro".into()));
    }

    #[test]
    fn about_output_missing_the_tier_line_yields_no_plan() {
        assert_eq!(plan_from_about_output("About Cursor CLI\n"), None);
    }

    #[test]
    fn cursor_has_no_destructive_harness_capabilities() {
        assert!(!CursorAdapter::capabilities_static().can_trigger_compaction);
        assert!(!CursorAdapter::capabilities_static().reports_token_counts);
    }

    #[tokio::test]
    async fn unsupported_stop_detection_is_explicit() {
        let adapter = CursorAdapter::new(Arc::new(FakeTransport(json!({}))));
        assert!(matches!(
            adapter
                .detect_stop(&SessionId("cursor-session".into()))
                .await,
            Err(AdapterError::Unsupported)
        ));
    }

    #[test]
    fn discovers_access_token_from_cursor_ide_state_database() {
        let path =
            std::env::temp_dir().join(format!("usagewindow-cursor-{}.vscdb", uuid::Uuid::new_v4()));
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                "CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO ItemTable (key, value) VALUES ('cursorAuth/accessToken', 'ide-token')",
                [],
            )
            .unwrap();

        assert_eq!(
            CursorAuth::discover(Some(&path), None),
            vec![CursorAuth::Bearer("ide-token".into())]
        );
        drop(connection);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn falls_back_to_cursor_agent_auth_json() {
        let path =
            std::env::temp_dir().join(format!("usagewindow-cursor-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&path, r#"{"accessToken":"cli-token"}"#).unwrap();

        assert_eq!(
            CursorAuth::discover(None, Some(&path)),
            vec![CursorAuth::Bearer("cli-token".into())]
        );
        let _ = std::fs::remove_file(path);
    }

    fn fake_jwt(exp: i64) -> String {
        fn b64url(bytes: &[u8]) -> String {
            const CHARS: &[u8] =
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
            let mut out = String::new();
            for chunk in bytes.chunks(3) {
                let b0 = chunk[0] as u32;
                let b1 = *chunk.get(1).unwrap_or(&0) as u32;
                let b2 = *chunk.get(2).unwrap_or(&0) as u32;
                let n = (b0 << 16) | (b1 << 8) | b2;
                out.push(CHARS[((n >> 18) & 63) as usize] as char);
                out.push(CHARS[((n >> 12) & 63) as usize] as char);
                if chunk.len() > 1 {
                    out.push(CHARS[((n >> 6) & 63) as usize] as char);
                }
                if chunk.len() > 2 {
                    out.push(CHARS[(n & 63) as usize] as char);
                }
            }
            out
        }
        let payload = b64url(json!({"exp": exp}).to_string().as_bytes());
        format!("header.{payload}.sig")
    }

    #[test]
    fn an_expired_ide_token_does_not_shadow_a_valid_cli_token() {
        let far_future = Utc::now().timestamp() + 3600;
        let long_expired = Utc::now().timestamp() - 3600;

        let state_db =
            std::env::temp_dir().join(format!("usagewindow-cursor-{}.vscdb", uuid::Uuid::new_v4()));
        let connection = rusqlite::Connection::open(&state_db).unwrap();
        connection
            .execute(
                "CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO ItemTable (key, value) VALUES ('cursorAuth/accessToken', ?1)",
                [fake_jwt(long_expired)],
            )
            .unwrap();
        drop(connection);

        let cli_auth =
            std::env::temp_dir().join(format!("usagewindow-cursor-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(
            &cli_auth,
            json!({"accessToken": fake_jwt(far_future)}).to_string(),
        )
        .unwrap();

        assert_eq!(
            CursorAuth::discover(Some(&state_db), Some(&cli_auth)),
            vec![
                CursorAuth::Bearer(fake_jwt(far_future)),
                CursorAuth::Bearer(fake_jwt(long_expired)),
            ]
        );

        let _ = std::fs::remove_file(state_db);
        let _ = std::fs::remove_file(cli_auth);
    }

    struct FixtureTranscriptFs(Vec<(String, String)>);

    #[async_trait]
    impl TranscriptFileSystem for FixtureTranscriptFs {
        async fn jsonl_files(&self) -> AdapterResult<Vec<String>> {
            Ok(self.0.iter().map(|(path, _)| path.clone()).collect())
        }

        async fn read_to_string(&self, path: &str) -> AdapterResult<String> {
            self.0
                .iter()
                .find(|(candidate, _)| candidate == path)
                .map(|(_, content)| content.clone())
                .ok_or_else(|| AdapterError::Other("fixture path not found".into()))
        }
    }

    fn cursor_transcript_line(role: &str, text: &str) -> String {
        json!({"role": role, "message": {"content": [{"type": "text", "text": text}]}}).to_string()
    }

    struct CursorChatFixture {
        root: std::path::PathBuf,
    }

    impl CursorChatFixture {
        fn new() -> Self {
            let root = std::env::temp_dir()
                .join(format!("usagewindow-cursor-chats-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            Self { root }
        }

        fn insert_meta(&self, directory_session_id: &str, metadata: Value) {
            let session_dir = self
                .root
                .join("3489ad9e9dbc710184e395124ffa1db2")
                .join(directory_session_id);
            std::fs::create_dir_all(&session_dir).unwrap();
            let connection = rusqlite::Connection::open(session_dir.join("store.db")).unwrap();
            connection
                .execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)", [])
                .unwrap();
            let encoded = metadata
                .to_string()
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            connection
                .execute("INSERT INTO meta (key, value) VALUES ('0', ?1)", [encoded])
                .unwrap();
        }

        fn insert_corrupt_meta(&self, directory_session_id: &str) {
            let session_dir = self
                .root
                .join("3489ad9e9dbc710184e395124ffa1db2")
                .join(directory_session_id);
            std::fs::create_dir_all(&session_dir).unwrap();
            let connection = rusqlite::Connection::open(session_dir.join("store.db")).unwrap();
            connection
                .execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)", [])
                .unwrap();
            connection
                .execute("INSERT INTO meta (key, value) VALUES ('0', 'not-hex')", [])
                .unwrap();
        }

        fn insert_unreadable_store(&self, directory_session_id: &str) {
            let store_path = self
                .root
                .join("3489ad9e9dbc710184e395124ffa1db2")
                .join(directory_session_id)
                .join("store.db");
            std::fs::create_dir_all(store_path).unwrap();
        }
    }

    impl Drop for CursorChatFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn cursor_adapter_with_chat_root(id: &str, chat_root: std::path::PathBuf) -> CursorAdapter {
        let path = format!(
            "/home/v0id/.cursor/projects/home-v0id-Documents-repos-usagewindow/agent-transcripts/{id}/{id}.jsonl"
        );
        CursorAdapter::new(Arc::new(FakeTransport(json!({}))))
            .with_transcript_fs(Arc::new(FixtureTranscriptFs(vec![(
                path,
                cursor_transcript_line("user", "hello"),
            )])))
            .with_chat_metadata_root(chat_root)
    }

    #[test]
    fn decodes_project_dir_name_into_a_cwd() {
        assert_eq!(
            decode_project_dir_name("home-v0id-Documents-repos-usagewindow"),
            "/home/v0id/Documents/repos/usagewindow"
        );
    }

    #[test]
    fn decode_is_lossy_when_a_path_segment_contains_a_literal_dash() {
        // Cursor's transcripts carry no structured cwd field, so a real
        // repo name containing a dash (`dozens-game`) cannot be
        // distinguished from a path separator — documented in
        // `decode_project_dir_name`'s doc comment.
        assert_eq!(
            decode_project_dir_name("home-v0id-Documents-repos-dozens-game"),
            "/home/v0id/Documents/repos/dozens/game"
        );
    }

    #[test]
    fn extracts_project_dir_name_from_a_transcript_path() {
        let path = "/home/v0id/.cursor/projects/home-v0id-Documents-repos-usagewindow/agent-transcripts/b283a138-1a72-4d11-8d02-2ac1a333619a/b283a138-1a72-4d11-8d02-2ac1a333619a.jsonl";
        assert_eq!(
            project_dir_name(path),
            Some("home-v0id-Documents-repos-usagewindow")
        );
    }

    #[test]
    fn scans_a_cursor_transcript_into_a_discovered_session() {
        let id = "b283a138-1a72-4d11-8d02-2ac1a333619a";
        let path = format!(
            "/home/v0id/.cursor/projects/home-v0id-Documents-repos-usagewindow/agent-transcripts/{id}/{id}.jsonl"
        );
        let content = format!(
            "{}\n{}",
            cursor_transcript_line("user", "add a retry loop"),
            cursor_transcript_line("assistant", "done")
        );
        let session = scan_cursor_transcript(&path, &content, None).unwrap();
        assert_eq!(session.id, SessionId(id.into()));
        assert_eq!(session.cwd, "/home/v0id/Documents/repos/usagewindow");
        assert_eq!(session.title, Some("add a retry loop".into()));
        assert_eq!(session.state_path, Some(path));
    }

    #[test]
    fn rejects_a_transcript_whose_filename_is_not_a_session_uuid() {
        let path = "/home/v0id/.cursor/projects/foo/agent-transcripts/worker.log";
        assert!(scan_cursor_transcript(path, "", None).is_none());
    }

    #[tokio::test]
    async fn discovers_cursor_sessions_from_transcript_fixtures() {
        let id = "b283a138-1a72-4d11-8d02-2ac1a333619a";
        let path = format!(
            "/home/v0id/.cursor/projects/home-v0id-Documents-repos-usagewindow/agent-transcripts/{id}/{id}.jsonl"
        );
        let content = cursor_transcript_line("user", "hello");
        let adapter = CursorAdapter::new(Arc::new(FakeTransport(json!({}))))
            .with_transcript_fs(Arc::new(FixtureTranscriptFs(vec![(path.clone(), content)])));

        let discovered = adapter.discover_sessions().await.unwrap();

        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].id, SessionId(id.into()));
        assert_eq!(discovered[0].cwd, "/home/v0id/Documents/repos/usagewindow");
    }

    #[tokio::test]
    async fn discovers_cursor_child_lineage_from_hex_encoded_chat_metadata() {
        let fixture = CursorChatFixture::new();
        let child = "e5ded152-88c0-4222-be0d-d629dc344555";
        let root = "a293a959-d37d-4e73-8848-f5b7f3d560a0";
        fixture.insert_meta(
            child,
            json!({
                "agentId": child,
                "latestRootBlobId": "6271900c",
                "name": "New Agent",
                "mode": "default",
                "isRunEverything": false,
                "createdAt": 1790257726036_i64,
                "subagentInfo": {
                    "parentAgentId": root,
                    "rootParentAgentId": root,
                    "toolCallId": "tool_f9fe97e1-7daf-4e7f-8662-ac0d7ccfcfc4",
                    "typeName": "generalPurpose"
                }
            }),
        );
        let adapter = cursor_adapter_with_chat_root(child, fixture.root.clone());

        let discovered = adapter.discover_sessions().await.unwrap();

        assert_eq!(discovered.len(), 1);
        assert_eq!(
            discovered[0].lineage,
            SessionLineage {
                parent: Some(SessionId(root.into())),
                root: Some(SessionId(root.into())),
                related: Vec::new(),
            }
        );
    }

    #[tokio::test]
    async fn cursor_chat_metadata_agent_id_mismatch_fails_closed() {
        let fixture = CursorChatFixture::new();
        let child = "e5ded152-88c0-4222-be0d-d629dc344555";
        let root = "a293a959-d37d-4e73-8848-f5b7f3d560a0";
        fixture.insert_meta(
            child,
            json!({
                "agentId": "ffffffff-ffff-4fff-8fff-ffffffffffff",
                "subagentInfo": {
                    "parentAgentId": root,
                    "rootParentAgentId": root
                }
            }),
        );
        let adapter = cursor_adapter_with_chat_root(child, fixture.root.clone());

        let discovered = adapter.discover_sessions().await.unwrap();

        assert_eq!(discovered.len(), 1);
        assert!(discovered[0].lineage.is_empty());
    }

    #[tokio::test]
    async fn missing_or_corrupt_cursor_chat_metadata_keeps_transcript_discovery_working() {
        let fixture = CursorChatFixture::new();
        let corrupt = "e5ded152-88c0-4222-be0d-d629dc344555";
        let missing = "b283a138-1a72-4d11-8d02-2ac1a333619a";
        let unreadable = "73f1c40d-9152-4a27-ac20-34efb98b99bd";
        fixture.insert_corrupt_meta(corrupt);
        fixture.insert_unreadable_store(unreadable);

        for id in [corrupt, missing, unreadable] {
            let adapter = cursor_adapter_with_chat_root(id, fixture.root.clone());
            let discovered = adapter.discover_sessions().await.unwrap();

            assert_eq!(discovered.len(), 1);
            assert_eq!(discovered[0].id, SessionId(id.into()));
            assert!(discovered[0].lineage.is_empty());
        }
    }
}
