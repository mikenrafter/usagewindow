use crate::process::{ProcessSpawner, ProcessSpec};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, DiscoveredSession, HarnessAdapter,
    SeedContext, SeedMode, StatusEvent, TokenUsageRecord,
};
use uw_core::model::*;

#[async_trait]
pub trait TranscriptFileSystem: Send + Sync {
    async fn jsonl_files(&self) -> AdapterResult<Vec<String>>;
    async fn read_to_string(&self, path: &str) -> AdapterResult<String>;
    fn incremental(&self) -> bool {
        false
    }
    /// Scan one transcript for session discovery. The real file system streams
    /// it line by line; transcripts reach tens of MB and active ones change
    /// every tick, so reading them whole each time inflated the daemon's heap.
    async fn scan(&self, path: &str) -> AdapterResult<Option<DiscoveredSession>> {
        Ok(scan_transcript(path, &self.read_to_string(path).await?))
    }
    async fn acknowledge_scan(
        &self,
        _path: &str,
        _id: &SessionId,
        _records: &[TokenUsageRecord],
    ) -> AdapterResult<()> {
        Ok(())
    }
    async fn activity(
        &self,
        path: &str,
        id: &SessionId,
    ) -> AdapterResult<uw_core::activity::SessionActivity> {
        Ok(transcript_activity(&self.read_to_string(path).await?, id))
    }
    /// Whether one transcript records a quota error; streamed like `scan`.
    async fn has_quota_error(&self, path: &str) -> AdapterResult<bool> {
        Ok(transcript_has_quota_error(
            &self.read_to_string(path).await?,
        ))
    }
}

#[derive(Default)]
pub struct ClaudeTranscriptFileSystem {
    scans: Arc<Mutex<crate::jsonl::JsonlCache<TranscriptScan>>>,
}

/// Usage percentage at or above which a transcript quota error is corroborated
/// by the provider's own reported usage, rather than some unrelated error.
const NEAR_LIMIT_STOP_PCT: f32 = 95.0;

/// Phrases that mark a Claude Code transcript record as a real quota ceiling
/// (verified 2026-09-21 against session `9675ac22-…` and local
/// `isApiErrorMessage` samples). Auth / model-selection errors also set
/// `isApiErrorMessage` and must not match.
fn quota_limit_text(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("spend limit")
        || lower.contains("usage limit")
        || lower.contains("session limit")
        || lower.contains("rate limit")
        || lower.contains("cc_cli_limit_message")
}

fn assistant_text(message: &Value) -> Option<String> {
    let content = message.get("content")?;
    if let Some(text) = content.as_str() {
        return Some(text.to_owned());
    }
    let parts = content.as_array()?;
    let mut out = String::new();
    for part in parts {
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(text);
        }
    }
    (!out.is_empty()).then_some(out)
}

/// Verified Claude Code quota markers (see docs/research-claude-code.md):
/// - `assistant` + `isApiErrorMessage` with quota-shaped text
/// - `system` / `local_command` whose content carries `<local-command-stderr>`
///   spend/usage/session limit text (e.g. failed `/compact`)
/// - legacy `type:"error"` / `error.type:"rate_limit_error"` if a future
///   Claude release emits it
fn transcript_has_quota_error(content: &str) -> bool {
    content.lines().any(line_has_quota_error)
}

fn line_has_quota_error(line: &str) -> bool {
    {
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            return false;
        };
        record_has_quota_error(&record)
    }
}

fn record_has_quota_error(record: &Value) -> bool {
    match record.get("type").and_then(Value::as_str) {
        Some("error") => {
            record
                .get("error")
                .and_then(|error| error.get("type"))
                .and_then(Value::as_str)
                == Some("rate_limit_error")
                || record
                    .get("error")
                    .and_then(|error| error.get("message"))
                    .and_then(Value::as_str)
                    .is_some_and(quota_limit_text)
        }
        Some("assistant")
            if record.get("isApiErrorMessage").and_then(Value::as_bool) == Some(true) =>
        {
            record
                .get("message")
                .and_then(assistant_text)
                .is_some_and(|text| quota_limit_text(&text))
        }
        Some("system")
            if record.get("subtype").and_then(Value::as_str) == Some("local_command") =>
        {
            record
                .get("content")
                .and_then(Value::as_str)
                .is_some_and(|content| {
                    content.contains("local-command-stderr") && quota_limit_text(content)
                })
        }
        _ => false,
    }
}

pub fn filter_keepalive_transcript(content: &str) -> String {
    content
        .lines()
        .filter(|line| !line.contains("[[uw-keepalive]]"))
        .collect::<Vec<_>>()
        .join("\n")
}
#[async_trait]
impl TranscriptFileSystem for ClaudeTranscriptFileSystem {
    fn incremental(&self) -> bool {
        true
    }
    async fn jsonl_files(&self) -> AdapterResult<Vec<String>> {
        let root = std::env::var("HOME").map_err(|e| AdapterError::Other(e.to_string()))?
            + "/.claude/projects";
        let mut out = Vec::new();
        let mut dirs = vec![root];
        while let Some(dir) = dirs.pop() {
            let mut entries = tokio::fs::read_dir(&dir)
                .await
                .map_err(|e| AdapterError::Other(e.to_string()))?;
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
        let scans = self.scans.clone();
        let present: std::collections::HashSet<std::path::PathBuf> =
            out.iter().map(std::path::PathBuf::from).collect();
        tokio::task::spawn_blocking(move || {
            scans
                .lock()
                .map_err(|e| AdapterError::Other(e.to_string()))?
                .retain(|path| present.contains(path));
            Ok::<_, AdapterError>(())
        })
        .await
        .map_err(|e| AdapterError::Other(e.to_string()))??;
        Ok(out)
    }
    async fn read_to_string(&self, path: &str) -> AdapterResult<String> {
        tokio::fs::read_to_string(path)
            .await
            .map_err(|e| AdapterError::Other(e.to_string()))
    }
    async fn scan(&self, path: &str) -> AdapterResult<Option<DiscoveredSession>> {
        let path = path.to_owned();
        let scans = self.scans.clone();
        tokio::task::spawn_blocking(move || {
            let mut scans = scans
                .lock()
                .map_err(|e| AdapterError::Other(e.to_string()))?;
            let scan = scans
                .scan(std::path::Path::new(&path), TranscriptScan::observe)
                .map_err(|e| AdapterError::Other(e.to_string()))?;
            Ok(scan.session(&path))
        })
        .await
        .map_err(|e| AdapterError::Other(e.to_string()))?
    }
    async fn acknowledge_scan(
        &self,
        path: &str,
        id: &SessionId,
        records: &[TokenUsageRecord],
    ) -> AdapterResult<()> {
        let cache = self.scans.clone();
        let path = path.to_owned();
        let id = id.clone();
        let records = records.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut cache = cache
                .lock()
                .map_err(|e| AdapterError::Other(e.to_string()))?;
            if let Some(scan) = cache.get_mut(std::path::Path::new(&path))
                && scan.record_id.as_ref().is_none_or(|known| known == &id.0)
            {
                crate::jsonl::acknowledge_usage(&mut scan.token_usage, &records);
            }
            Ok(())
        })
        .await
        .map_err(|e| AdapterError::Other(e.to_string()))?
    }

    async fn has_quota_error(&self, path: &str) -> AdapterResult<bool> {
        let path = path.to_owned();
        let scans = self.scans.clone();
        tokio::task::spawn_blocking(move || {
            let mut scans = scans
                .lock()
                .map_err(|e| AdapterError::Other(e.to_string()))?;
            Ok(scans
                .scan(std::path::Path::new(&path), TranscriptScan::observe)
                .map_err(|e| AdapterError::Other(e.to_string()))?
                .quota_error)
        })
        .await
        .map_err(|e| AdapterError::Other(e.to_string()))?
    }
    async fn activity(
        &self,
        path: &str,
        id: &SessionId,
    ) -> AdapterResult<uw_core::activity::SessionActivity> {
        let path = path.to_owned();
        let id = id.clone();
        let scans = self.scans.clone();
        tokio::task::spawn_blocking(move || {
            let mut scans = scans
                .lock()
                .map_err(|e| AdapterError::Other(e.to_string()))?;
            let scan = scans
                .scan(std::path::Path::new(&path), TranscriptScan::observe)
                .map_err(|e| AdapterError::Other(e.to_string()))?;
            if scan.invalid || scan.record_id.as_deref() != Some(id.0.as_str()) {
                return Ok(uw_core::activity::SessionActivity::Unknown);
            }
            let activity = scan.activity.activity();
            Ok(if scans.is_complete(std::path::Path::new(&path)) {
                activity
            } else {
                uw_core::activity::SessionActivity::Unknown
            })
        })
        .await
        .map_err(|e| AdapterError::Other(e.to_string()))?
    }
}

#[async_trait]
pub trait CredentialsReader: Send + Sync {
    async fn read(&self) -> AdapterResult<String>;
}
pub struct FileCredentialsReader;
#[async_trait]
impl CredentialsReader for FileCredentialsReader {
    async fn read(&self) -> AdapterResult<String> {
        tokio::fs::read_to_string(
            std::env::var("HOME").map_err(|e| AdapterError::Other(e.to_string()))?
                + "/.claude/.credentials.json",
        )
        .await
        .map_err(|e| AdapterError::Other(e.to_string()))
    }
}

/// Account identity and plan, as reported by the `claude` CLI itself.
/// `.claude/.credentials.json`'s OAuth blob carries only tokens and
/// `subscriptionType`/`rateLimitTier` on real installs — no email, despite
/// the pointers `account_from_credentials` still checks as a fallback — so
/// `claude auth status` is the only source that reliably has the
/// human-readable account name. Verified 2026-09-22 against a live `claude
/// auth status`: `{"loggedIn":true,...,"email":"...","subscriptionType":"pro",...}`.
#[async_trait]
pub trait AuthStatusReader: Send + Sync {
    async fn read(&self) -> AdapterResult<Value>;
}
pub struct ClaudeCliAuthStatusReader;
#[async_trait]
impl AuthStatusReader for ClaudeCliAuthStatusReader {
    async fn read(&self) -> AdapterResult<Value> {
        let output = tokio::process::Command::new("claude")
            .args(["auth", "status"])
            .output()
            .await
            .map_err(|e| AdapterError::Transient(e.to_string()))?;
        if !output.status.success() {
            return Err(AdapterError::Other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        serde_json::from_slice(&output.stdout).map_err(|e| AdapterError::Other(e.to_string()))
    }
}

/// `claude auth status`'s `subscriptionType` is lowercase/machine-shaped
/// (`"pro"`, `"max"`, `"free"`); the card should show the same casing Claude's
/// own UI uses.
fn display_plan_name(subscription_type: &str) -> String {
    match subscription_type {
        "free" => "Free".into(),
        "pro" => "Pro".into(),
        "max" => "Max".into(),
        "team" => "Team".into(),
        "enterprise" => "Enterprise".into(),
        other => other.into(),
    }
}

fn account_and_plan_from_auth_status(value: &Value) -> (Option<AccountId>, Option<String>) {
    let account = value
        .get("email")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(|value| AccountId(value.to_owned()));
    let plan = value
        .get("subscriptionType")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(display_plan_name);
    (account, plan)
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}
#[async_trait]
pub trait HttpTransport: Send + Sync {
    async fn get(&self, request: HttpRequest) -> AdapterResult<HttpResponse>;
}
pub struct ReqwestHttpTransport {
    client: reqwest::Client,
}
impl Default for ReqwestHttpTransport {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}
#[async_trait]
impl HttpTransport for ReqwestHttpTransport {
    async fn get(&self, request: HttpRequest) -> AdapterResult<HttpResponse> {
        let mut req = self.client.get(request.url);
        for (k, v) in request.headers {
            req = req.header(k, v);
        }
        let response = req
            .send()
            .await
            .map_err(|e| AdapterError::Transient(e.to_string()))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| AdapterError::Transient(e.to_string()))?;
        Ok(HttpResponse { status, body })
    }
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CacheEntry {
    pub fetched_at: DateTime<Utc>,
    pub sample: UsageSample,
}
#[async_trait]
pub trait CacheStore: Send + Sync {
    async fn load(&self) -> AdapterResult<Option<CacheEntry>>;
    async fn save(&self, entry: CacheEntry) -> AdapterResult<()>;
}

pub struct FileCacheStore {
    path: std::path::PathBuf,
}

impl FileCacheStore {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl CacheStore for FileCacheStore {
    async fn load(&self) -> AdapterResult<Option<CacheEntry>> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| AdapterError::Other(error.to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(AdapterError::Other(error.to_string())),
        }
    }

    async fn save(&self, entry: CacheEntry) -> AdapterResult<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| AdapterError::Other(error.to_string()))?;
        }
        let bytes =
            serde_json::to_vec(&entry).map_err(|error| AdapterError::Other(error.to_string()))?;
        let temporary = self.path.with_extension("tmp");
        tokio::fs::write(&temporary, bytes)
            .await
            .map_err(|error| AdapterError::Other(error.to_string()))?;
        tokio::fs::rename(temporary, &self.path)
            .await
            .map_err(|error| AdapterError::Other(error.to_string()))
    }
}
#[async_trait]
pub trait HookChannel: Send + Sync {
    async fn advise(&self, session_id: &SessionId, text: &str) -> AdapterResult<DeliveryOutcome>;
    async fn emit_status(
        &self,
        session_id: &SessionId,
        text: &str,
    ) -> AdapterResult<DeliveryOutcome> {
        self.advise(session_id, text).await
    }
}
#[async_trait]
pub trait SessionMessenger: Send + Sync {
    async fn send(&self, session_id: &SessionId, text: &str) -> AdapterResult<DeliveryOutcome>;
}

pub struct ClaudeCodeAdapter {
    credentials: Arc<dyn CredentialsReader>,
    http: Arc<dyn HttpTransport>,
    cache: Arc<dyn CacheStore>,
    version: String,
    now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    hook: Option<Arc<dyn HookChannel>>,
    messenger: Option<Arc<dyn SessionMessenger>>,
    spawner: Arc<dyn ProcessSpawner>,
    transcript_fs: Arc<dyn TranscriptFileSystem>,
    discovery_cache: Mutex<HashMap<String, CachedTranscript>>,
    /// Transcript path to the session id its records carry, from discovery.
    /// Kept apart from the cache because a file without readable metadata is
    /// scanned but not cached.
    known_ids: Mutex<HashMap<String, SessionId>>,
    auth_status: Arc<dyn AuthStatusReader>,
}

struct CachedTranscript {
    fingerprint: (u64, Option<std::time::SystemTime>),
    session: DiscoveredSession,
}
impl ClaudeCodeAdapter {
    pub fn real(cache_path: impl Into<std::path::PathBuf>, version: impl Into<String>) -> Self {
        Self::with_dependencies(
            Arc::new(FileCredentialsReader),
            Arc::new(ReqwestHttpTransport::default()),
            Arc::new(FileCacheStore::new(cache_path)),
            version.into(),
            Arc::new(Utc::now),
        )
    }
    pub fn with_dependencies(
        credentials: Arc<dyn CredentialsReader>,
        http: Arc<dyn HttpTransport>,
        cache: Arc<dyn CacheStore>,
        version: String,
        now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    ) -> Self {
        Self {
            credentials,
            http,
            cache,
            version,
            now,
            hook: None,
            messenger: None,
            spawner: Arc::new(crate::process::TokioProcessSpawner),
            transcript_fs: Arc::new(ClaudeTranscriptFileSystem::default()),
            discovery_cache: Mutex::new(HashMap::new()),
            known_ids: Mutex::new(HashMap::new()),
            auth_status: Arc::new(ClaudeCliAuthStatusReader),
        }
    }
    pub fn capabilities_static() -> Capabilities {
        Capabilities {
            observe_only: false,
            can_trigger_compaction: true,
            can_advise_mid_turn: true,
            can_inject_at_session_start: true,
            can_observe_compaction: true,
            reports_token_counts: true,
            headless_resume: true,
            seed_modes: vec![SeedMode::InitialPrompt],
        }
    }
    pub fn with_delivery(
        mut self,
        hook: Arc<dyn HookChannel>,
        messenger: Arc<dyn SessionMessenger>,
        spawner: Arc<dyn ProcessSpawner>,
    ) -> Self {
        self.hook = Some(hook);
        self.messenger = Some(messenger);
        self.spawner = spawner;
        self
    }
    pub fn with_hook_channel(mut self, hook: Arc<dyn HookChannel>) -> Self {
        self.hook = Some(hook);
        self
    }
    pub fn with_transcript_fs(mut self, fs: Arc<dyn TranscriptFileSystem>) -> Self {
        self.transcript_fs = fs;
        self
    }
    pub fn with_auth_status(mut self, auth_status: Arc<dyn AuthStatusReader>) -> Self {
        self.auth_status = auth_status;
        self
    }
    async fn fetch_live(&self) -> AdapterResult<UsageSample> {
        let value: Value = serde_json::from_str(&self.credentials.read().await?)
            .map_err(|e| AdapterError::Other(e.to_string()))?;
        // `claude auth status` is the only reliable source of the account
        // email and plan (see `AuthStatusReader`'s doc comment); the OAuth
        // blob's own pointers are kept as a fallback for older CLI versions
        // that predate the `auth status` subcommand, or when it's briefly
        // unreachable (e.g. spawn failure) — never fatal to the usage fetch.
        let (account, plan) = match self.auth_status.read().await {
            Ok(status) => account_and_plan_from_auth_status(&status),
            Err(_) => (
                account_from_credentials(&value),
                value
                    .pointer("/claudeAiOauth/subscriptionType")
                    .and_then(Value::as_str)
                    .map(display_plan_name),
            ),
        };
        let token = value
            .pointer("/claudeAiOauth/accessToken")
            .and_then(Value::as_str)
            .ok_or(AdapterError::Auth)?;
        let response = self
            .http
            .get(HttpRequest {
                url: "https://api.anthropic.com/api/oauth/usage".into(),
                headers: vec![
                    ("Authorization".into(), format!("Bearer {token}")),
                    ("Accept".into(), "application/json".into()),
                    ("Content-Type".into(), "application/json".into()),
                    ("User-Agent".into(), format!("claude-code/{}", self.version)),
                    ("anthropic-beta".into(), "oauth-2025-04-20".into()),
                ],
            })
            .await?;
        if response.status == 401 || response.status == 403 {
            return Err(AdapterError::Auth);
        }
        if response.status == 429 || response.status >= 500 {
            return Err(AdapterError::Transient(format!("HTTP {}", response.status)));
        }
        if !(200..300).contains(&response.status) {
            return Err(AdapterError::Other(format!("HTTP {}", response.status)));
        }
        let parsed: UsageResponse =
            serde_json::from_str(&response.body).map_err(|e| AdapterError::Other(e.to_string()))?;
        let at = (self.now)();
        let mut windows = HashMap::new();
        windows.insert(
            WindowKey {
                provider: Provider::ClaudeCode,
                kind: WindowKind::Rolling { minutes: 300 },
            },
            parsed.window("five_hour", 300),
        );
        windows.insert(
            WindowKey {
                provider: Provider::ClaudeCode,
                kind: WindowKind::Rolling { minutes: 10080 },
            },
            parsed.window("seven_day", 10080),
        );
        Ok(UsageSample {
            at,
            fetched_at: Some(at),
            source: UsageSource::ProviderReported,
            provider: Provider::ClaudeCode,
            account,
            plan,
            windows,
            credits: None,
        })
    }
}
impl ClaudeCodeAdapter {
    /// Transcripts that belong to `session_id`: named after it (the main
    /// file, or a subagent directory under it), or found by discovery to carry
    /// its id in their records. This used to read every transcript on the
    /// machine (2.6 GB on one host) per warm session per tick.
    fn session_paths(
        &self,
        session_id: &SessionId,
        paths: Vec<String>,
    ) -> AdapterResult<Vec<String>> {
        let known = self
            .known_ids
            .lock()
            .map_err(|_| AdapterError::Other("session id map poisoned".into()))?;
        Ok(paths
            .into_iter()
            .filter(|path| path.contains(&session_id.0) || known.get(path) == Some(session_id))
            .collect())
    }

    async fn has_quota_error_evidence(&self, session_id: &SessionId) -> AdapterResult<bool> {
        let paths = self.session_paths(session_id, self.transcript_fs.jsonl_files().await?)?;
        for path in paths {
            if self.transcript_fs.has_quota_error(&path).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
fn account_from_credentials(value: &Value) -> Option<AccountId> {
    [
        "/claudeAiOauth/email",
        "/claudeAiOauth/accountEmail",
        "/claudeAiOauth/accountUuid",
        "/claudeAiOauth/organizationUuid",
    ]
    .into_iter()
    .filter_map(|path| value.pointer(path).and_then(Value::as_str))
    .find(|value| !value.is_empty())
    .map(|value| AccountId(value.to_owned()))
}

#[derive(Deserialize)]
struct UsageResponse {
    five_hour: ApiWindow,
    seven_day: ApiWindow,
}
#[derive(Deserialize)]
struct ApiWindow {
    utilization: f32,
    resets_at: Option<DateTime<Utc>>,
}
impl UsageResponse {
    fn window(&self, name: &str, _minutes: u32) -> UsageWindowState {
        let w = if name == "five_hour" {
            &self.five_hour
        } else {
            &self.seven_day
        };
        UsageWindowState::new(w.utilization, false, true, w.resets_at, None)
    }
}
pub fn compact_instructions(prompt: &str) -> String {
    uw_core::compaction::message("/compact", prompt)
}

#[async_trait]
impl HarnessAdapter for ClaudeCodeAdapter {
    fn provider(&self) -> Provider {
        Provider::ClaudeCode
    }
    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Self::capabilities_static();
        capabilities.can_advise_mid_turn = self.hook.is_some();
        capabilities.can_inject_at_session_start = self.hook.is_some();
        capabilities
    }
    async fn fetch_usage(&self, account: Option<&AccountId>) -> AdapterResult<UsageSample> {
        self.fetch_usage_with_max_age(account, std::time::Duration::from_secs(120))
            .await
    }
    async fn fetch_usage_with_max_age(
        &self,
        _: Option<&AccountId>,
        max_age: std::time::Duration,
    ) -> AdapterResult<UsageSample> {
        let max_age = chrono::Duration::from_std(max_age.min(std::time::Duration::from_secs(120)))
            .expect("cache age is capped at 120 seconds");
        if let Some(entry) = self.cache.load().await?
            && (self.now)() - entry.fetched_at < max_age
        {
            let mut sample = entry.sample;
            if sample.account.is_none()
                && let Ok(credentials) = self.credentials.read().await
                && let Ok(value) = serde_json::from_str::<Value>(&credentials)
            {
                sample.account = account_from_credentials(&value);
                sample.plan = sample.plan.or_else(|| {
                    value
                        .pointer("/claudeAiOauth/subscriptionType")
                        .and_then(Value::as_str)
                        .map(display_plan_name)
                });
            }
            return Ok(sample);
        }
        match self.fetch_live().await {
            Ok(sample) => {
                self.cache
                    .save(CacheEntry {
                        fetched_at: (self.now)(),
                        sample: sample.clone(),
                    })
                    .await?;
                Ok(sample)
            }
            Err(AdapterError::Auth) => Err(AdapterError::Auth),
            Err(error @ AdapterError::Transient(_)) => {
                if let Some(entry) = self.cache.load().await? {
                    Ok(entry.sample)
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }
    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        Ok(None)
    }
    async fn detect_stop_with_usage(
        &self,
        session_id: &SessionId,
        sample: Option<&UsageSample>,
    ) -> AdapterResult<Option<StopReason>> {
        let Some(sample) = sample else {
            return Ok(None);
        };
        if !self.has_quota_error_evidence(session_id).await? {
            return Ok(None);
        }
        Ok(sample
            .windows
            .iter()
            .find(|(_, window)| window.pct >= NEAR_LIMIT_STOP_PCT)
            .map(|(window, _)| StopReason::UsageLimit {
                window: window.clone(),
            }))
    }
    async fn emit_status(
        &self,
        session: &SessionId,
        status: StatusEvent,
    ) -> AdapterResult<DeliveryOutcome> {
        self.hook
            .as_ref()
            .ok_or(AdapterError::Unsupported)?
            .emit_status(session, &format!("{status:?}"))
            .await
    }
    async fn advise(&self, session: &SessionId, text: &str) -> AdapterResult<DeliveryOutcome> {
        self.hook
            .as_ref()
            .ok_or(AdapterError::Unsupported)?
            .advise(session, text)
            .await
    }
    async fn compact(
        &self,
        session: &SessionSummary,
        req: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome> {
        validate_session_owner(session)?;
        let text = compact_instructions(&req.prompt);
        if let Some(messenger) = &self.messenger {
            return messenger.send(&session.id, &text).await;
        }
        Err(AdapterError::Unsupported)
    }
    async fn resume_session(
        &self,
        session: &SessionSummary,
        message: Option<&str>,
    ) -> AdapterResult<()> {
        validate_session_owner(session)?;
        let mut args = vec![
            "--print".to_string(),
            "--resume".into(),
            session.id.0.clone(),
        ];
        if let Some(message) = message {
            args.push(message.to_string());
        }
        self.spawner
            .run(ProcessSpec {
                program: "claude".into(),
                args,
                cwd: session.cwd.clone(),
            })
            .await
            .map(|_| ())
    }
    async fn seed_new_session(
        &self,
        mode: SeedMode,
        seed: &SeedContext,
    ) -> AdapterResult<SessionId> {
        if mode != SeedMode::InitialPrompt {
            return Err(AdapterError::Unsupported);
        }
        let out = self
            .spawner
            .run(ProcessSpec {
                program: "claude".into(),
                args: vec![
                    "-p".into(),
                    "--output-format".into(),
                    "json".into(),
                    seed.summary.clone(),
                ],
                cwd: seed.cwd.clone(),
            })
            .await?;
        out.session_id.map(SessionId).ok_or_else(|| {
            AdapterError::Other(
                "Claude Code did not report the new session id in JSON output".into(),
            )
        })
    }

    async fn session_activity(
        &self,
        session: &SessionSummary,
    ) -> AdapterResult<uw_core::activity::SessionActivity> {
        use uw_core::activity::SessionActivity;
        let path = match &session.state_path {
            Some(path) => Some(path.clone()),
            None => self
                .transcript_fs
                .jsonl_files()
                .await
                .ok()
                .and_then(|paths| {
                    paths.into_iter().find(|path| {
                        std::path::Path::new(path)
                            .file_stem()
                            .and_then(|name| name.to_str())
                            == Some(session.id.0.as_str())
                    })
                }),
        };
        let Some(path) = path else {
            return Ok(SessionActivity::Unknown);
        };
        if std::path::Path::new(&path)
            .file_stem()
            .and_then(|name| name.to_str())
            != Some(session.id.0.as_str())
        {
            return Ok(SessionActivity::Unknown);
        }
        Ok(self
            .transcript_fs
            .activity(&path, &session.id)
            .await
            .unwrap_or(uw_core::activity::SessionActivity::Unknown))
    }

    async fn export_transcript(&self, session: &SessionSummary) -> AdapterResult<String> {
        let mut matching = Vec::new();
        for path in self.session_paths(&session.id, self.transcript_fs.jsonl_files().await?)? {
            matching.push(self.transcript_fs.read_to_string(&path).await?);
        }
        Ok(filter_keepalive_transcript(&matching.join("\n")))
    }

    async fn acknowledge_discovery(
        &self,
        path: &str,
        id: &SessionId,
        records: &[TokenUsageRecord],
    ) -> AdapterResult<()> {
        self.transcript_fs
            .acknowledge_scan(path, id, records)
            .await?;
        if let Some(cached) = self
            .discovery_cache
            .lock()
            .map_err(|e| AdapterError::Other(e.to_string()))?
            .get_mut(path)
            && cached.session.id == *id
        {
            crate::jsonl::acknowledge_usage(&mut cached.session.token_usage, records);
        }
        Ok(())
    }

    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        let mut discovered = Vec::new();
        let paths = self.transcript_fs.jsonl_files().await?;
        let present: std::collections::HashSet<&str> = paths.iter().map(String::as_str).collect();
        // Forget transcripts that no longer exist, so the cache tracks the
        // directory instead of every file it ever saw.
        self.discovery_cache
            .lock()
            .map_err(|_| AdapterError::Other("discovery cache poisoned".into()))?
            .retain(|path, _| present.contains(path.as_str()));
        self.known_ids
            .lock()
            .map_err(|_| AdapterError::Other("session id map poisoned".into()))?
            .retain(|path, _| present.contains(path.as_str()));
        for path in paths {
            let fingerprint = tokio::fs::metadata(&path)
                .await
                .ok()
                .map(|metadata| (metadata.len(), metadata.modified().ok()));
            if let Some(fingerprint) = fingerprint.filter(|_| !self.transcript_fs.incremental()) {
                let cached = self
                    .discovery_cache
                    .lock()
                    .map_err(|_| AdapterError::Other("discovery cache poisoned".into()))?;
                if cached
                    .get(&path)
                    .is_some_and(|entry| entry.fingerprint == fingerprint)
                {
                    discovered.push(cached.get(&path).unwrap().session.clone());
                    continue;
                }
            }
            if let Some(session) = self.transcript_fs.scan(&path).await? {
                self.known_ids
                    .lock()
                    .map_err(|_| AdapterError::Other("session id map poisoned".into()))?
                    .insert(path.clone(), session.id.clone());
                if let Some(fingerprint) = fingerprint.filter(|_| !self.transcript_fs.incremental())
                {
                    self.discovery_cache
                        .lock()
                        .map_err(|_| AdapterError::Other("discovery cache poisoned".into()))?
                        .insert(
                            path,
                            CachedTranscript {
                                fingerprint,
                                session: session.clone(),
                            },
                        );
                }
                discovered.push(session);
            }
        }
        Ok(discovered)
    }
}

fn scan_transcript(path: &str, content: &str) -> Option<DiscoveredSession> {
    scan_transcript_lines(path, content.lines())
}

fn scan_transcript_lines<S: AsRef<str>>(
    path: &str,
    lines: impl Iterator<Item = S>,
) -> Option<DiscoveredSession> {
    let mut scan = TranscriptScan::default();
    for line in lines {
        scan.observe(line.as_ref());
    }
    scan.session(path)
}

#[derive(Default)]
struct TranscriptScan {
    invalid: bool,
    activity: ActivityScan,
    quota_error: bool,
    record_id: Option<String>,
    cwd: Option<String>,
    model: Option<ModelId>,
    context_window_size: Option<u64>,
    extended_context_model: bool,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    last_known_token_count: Option<u64>,
    token_usage: Vec<TokenUsageRecord>,
    title: Option<String>,
    cache_ttl: Option<chrono::Duration>,
}

impl TranscriptScan {
    fn observe(&mut self, line: &str) {
        if line.trim().is_empty() {
            return;
        }
        if self.invalid {
            return;
        }
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            self.activity.invalid = true;
            return;
        };
        if let Some(candidate) = record
            .get("sessionId")
            .or_else(|| record.get("session_id"))
            .and_then(Value::as_str)
            .filter(|value| uuid::Uuid::parse_str(value).is_ok())
        {
            if self
                .record_id
                .as_deref()
                .is_some_and(|known| known != candidate)
            {
                self.invalid = true;
                return;
            }
            self.record_id.get_or_insert_with(|| candidate.to_owned());
        }
        self.activity.observe_record(
            &record,
            &SessionId(self.record_id.clone().unwrap_or_default()),
        );
        if record.get("type").and_then(Value::as_str) == Some("assistant")
            && record.get("isSidechain").and_then(Value::as_bool) != Some(true)
            && let Some(split) = record.pointer("/message/usage/cache_creation")
        {
            if split
                .get("ephemeral_1h_input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > 0
            {
                self.cache_ttl = Some(chrono::Duration::hours(1));
            } else if split
                .get("ephemeral_5m_input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > 0
            {
                self.cache_ttl = Some(chrono::Duration::minutes(5));
            }
        }
        self.quota_error |= record_has_quota_error(&record);
        if self.cwd.is_none() {
            self.cwd = record.get("cwd").and_then(Value::as_str).map(str::to_owned);
        }
        if record.get("type").and_then(Value::as_str) == Some("ai-title")
            && let Some(value) = record.get("aiTitle").and_then(Value::as_str)
            && !value.is_empty()
        {
            self.title = Some(value.to_owned());
        }
        let timestamp = record
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc));
        if let Some(at) = timestamp {
            self.first_seen.get_or_insert(at);
            self.last_seen = Some(at);
        }
        if let Some(value) = record.pointer("/message/model").and_then(Value::as_str) {
            self.extended_context_model |= value.to_ascii_lowercase().contains("[1m]");
            self.model = Some(ModelId(value.to_owned()));
        }
        if let Some(value) = record
            .pointer("/message/model_context_window")
            .and_then(Value::as_u64)
        {
            self.context_window_size = Some(value);
        }
        if let Some(value) = record
            .pointer("/message/usage/input_tokens")
            .and_then(Value::as_u64)
        {
            let cache_read = record
                .pointer("/message/usage/cache_read_input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let cache_write = record
                .pointer("/message/usage/cache_creation_input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let output = record
                .pointer("/message/usage/output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let total_input = value.saturating_add(cache_read).saturating_add(cache_write);
            let total = total_input.saturating_add(output);
            let Some(at) = timestamp else { return };
            // Claude/T3Code can silently promote a session selected as 200k to
            // the 1M context tier. In that path the transcript may omit both
            // the `[1m]` model suffix and `model_context_window`; a token
            // observation above 200k is nevertheless definitive evidence that
            // the effective window was promoted.
            if total > 200_000 {
                self.context_window_size =
                    Some(self.context_window_size.unwrap_or(200_000).max(1_000_000));
            }
            self.last_known_token_count = Some(total);
            self.token_usage.push(TokenUsageRecord {
                call_key: None,
                at,
                model: self.model.clone(),
                input_tokens: total_input,
                cached_input_tokens: cache_read,
                cache_write_input_tokens: cache_write,
                output_tokens: output,
                reasoning_output_tokens: 0,
                total_tokens: total,
                context_pct: None,
            });
        }
    }
    fn session(&self, path: &str) -> Option<DiscoveredSession> {
        if self.invalid {
            return None;
        }
        let filename_id = std::path::Path::new(path)
            .file_stem()
            .and_then(|s| s.to_str())
            .filter(|s| uuid::Uuid::parse_str(s).is_ok())
            .map(str::to_owned);
        if filename_id
            .as_deref()
            .zip(self.record_id.as_deref())
            .is_some_and(|(filename, record)| filename != record)
        {
            return None;
        }
        let id = filename_id.or(self.record_id.clone())?;
        let context_window_size = self.context_window_size.or({
            Some(if self.extended_context_model {
                1_000_000
            } else {
                200_000
            })
        });
        Some(DiscoveredSession {
            id: SessionId(id),
            lineage: SessionLineage::default(),
            cwd: self.cwd.clone().unwrap_or_else(|| ".".into()),
            model: self.model.clone(),
            context_window_size,
            last_known_token_count: self.last_known_token_count,
            last_known_context_pct: None,
            cache_ttl: self.cache_ttl,
            first_seen: self.first_seen,
            last_seen: self.last_seen,
            state_path: Some(path.to_owned()),
            token_usage: self.token_usage.clone(),
            title: self.title.clone(),
        })
    }
}

fn validate_session_owner(session: &SessionSummary) -> AdapterResult<()> {
    if uuid::Uuid::parse_str(&session.id.0).is_err() {
        return Err(AdapterError::Other(
            "Claude Code session id is not a valid UUID".into(),
        ));
    }
    if let Some(replacement) = &session.superseded_by {
        return Err(AdapterError::Other(format!(
            "Claude Code session {} was superseded by {}",
            session.id.0, replacement.0
        )));
    }
    Ok(())
}
/// Reconstruct outstanding work from the raw root-session transcript.
pub fn transcript_activity(
    content: &str,
    session_id: &SessionId,
) -> uw_core::activity::SessionActivity {
    let mut scan = ActivityScan::default();
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        scan.observe(line, session_id);
    }
    scan.activity()
}

#[derive(Default)]
struct ActivityScan {
    tracker: uw_core::activity::ActivityTracker,
    calls: HashMap<String, String>,
    finished: std::collections::HashSet<String>,
    identified: bool,
    notification_turn: bool,
    invalid: bool,
}
impl ActivityScan {
    fn observe(&mut self, line: &str, session_id: &SessionId) {
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            self.invalid = true;
            return;
        };
        self.observe_record(&record, session_id);
    }
    fn observe_record(&mut self, record: &Value, session_id: &SessionId) {
        use uw_core::activity::ActivityEvent;
        if self.invalid {
            return;
        }
        if record.get("isSidechain").and_then(Value::as_bool) == Some(true) {
            return;
        }
        if let Some(id) = record.get("sessionId").and_then(Value::as_str) {
            if id != session_id.0 {
                self.invalid = true;
                return;
            }
            self.identified = true;
        }
        let kind = record.get("type").and_then(Value::as_str);
        if kind == Some("system")
            && record.get("subtype").and_then(Value::as_str) == Some("turn_duration")
        {
            self.notification_turn = false;
        }
        if !matches!(kind, Some("assistant" | "user")) {
            return;
        }
        let Some(content) = record.pointer("/message/content") else {
            self.invalid = true;
            return;
        };
        if kind == Some("user")
            && let Some(text) = content.as_str()
        {
            let text = text.trim();
            if text.starts_with("<task-notification>") && text.ends_with("</task-notification>") {
                self.notification_turn = true;
                let terminal = ["completed", "failed", "stopped"]
                    .iter()
                    .any(|status| text.contains(&format!("<status>{status}</status>")));
                if terminal {
                    for part in text.split("<task-id>").skip(1) {
                        if let Some((id, _)) = part.split_once("</task-id>") {
                            self.tracker
                                .observe(ActivityEvent::BackgroundFinished(id.trim().to_owned()));
                        }
                    }
                }
            }
            return;
        }
        let Some(parts) = content.as_array() else {
            if content.is_string() {
                return;
            }
            self.invalid = true;
            return;
        };
        for part in parts {
            match (kind, part.get("type").and_then(Value::as_str)) {
                (Some("assistant"), Some("tool_use")) => {
                    let (Some(id), Some(name)) = (
                        part.get("id").and_then(Value::as_str),
                        part.get("name").and_then(Value::as_str),
                    ) else {
                        self.invalid = true;
                        return;
                    };
                    self.calls
                        .entry(id.to_owned())
                        .or_insert_with(|| name.to_owned());
                    if !self.finished.contains(id) {
                        self.tracker
                            .observe(ActivityEvent::ToolStarted(id.to_owned()));
                    }
                }
                (Some("user"), Some("tool_result")) => {
                    let Some(id) = part.get("tool_use_id").and_then(Value::as_str) else {
                        self.invalid = true;
                        return;
                    };
                    if !self.calls.contains_key(id) {
                        self.invalid = true;
                        return;
                    }
                    if !self.finished.insert(id.to_owned()) {
                        continue;
                    }
                    self.tracker
                        .observe(ActivityEvent::ToolFinished(id.to_owned()));
                    if part.get("is_error").and_then(Value::as_bool) == Some(true) {
                        continue;
                    }
                    if let Some(text) = part.get("content").and_then(Value::as_str) {
                        let task = match self.calls.get(id).map(String::as_str) {
                            Some("Bash") => text
                                .strip_prefix("Command running in background with ID: ")
                                .and_then(|rest| {
                                    rest.split_once(". Output is being written to:")
                                        .map(|(id, _)| id)
                                }),
                            Some("Monitor") => text
                                .strip_prefix("Monitor started (task ")
                                .and_then(|rest| {
                                    rest.split_once(", expires in ").map(|(id, _)| id)
                                }),
                            _ => None,
                        };
                        if let Some(task) = task
                            .filter(|task| !task.is_empty() && !task.contains(char::is_whitespace))
                        {
                            self.tracker
                                .observe(ActivityEvent::BackgroundStarted(task.to_owned()));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    fn activity(&self) -> uw_core::activity::SessionActivity {
        use uw_core::activity::SessionActivity;
        if self.invalid || !self.identified {
            SessionActivity::Unknown
        } else if self.notification_turn {
            SessionActivity::Busy
        } else {
            self.tracker.activity()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::sync::{Arc, Mutex};
    use uw_core::adapter::{Capabilities, HarnessAdapter};

    struct TranscriptFixture {
        path: String,
        content: String,
    }

    #[async_trait::async_trait]
    impl TranscriptFileSystem for TranscriptFixture {
        async fn jsonl_files(&self) -> AdapterResult<Vec<String>> {
            Ok(vec![self.path.clone()])
        }

        async fn read_to_string(&self, _: &str) -> AdapterResult<String> {
            Ok(self.content.clone())
        }
    }

    #[tokio::test]
    async fn discover_sessions_reads_claude_usage_and_cache_fields() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let adapter = adapter(200, Arc::new(Cache { entry: Mutex::new(None) }), Arc::new(Mutex::new(0)))
            .with_transcript_fs(Arc::new(TranscriptFixture {
                path: format!("/tmp/{id}.jsonl"),
                content: format!(
                    r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}","cwd":"/work"}}
{{"timestamp":"2026-09-17T23:32:34Z","type":"assistant","message":{{"model":"claude-sonnet-5","usage":{{"input_tokens":1200,"cache_read_input_tokens":800,"cache_creation_input_tokens":100,"output_tokens":300}}}}}}
{{"timestamp":"2026-09-17T23:32:35Z","type":"assistant","message":{{"model":"claude-sonnet-5","usage":{{"input_tokens":100,"cache_read_input_tokens":50,"cache_creation_input_tokens":0,"output_tokens":25}}}}}}"#
                ),
            }));
        let found = adapter.discover_sessions().await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, SessionId(id.into()));
        assert_eq!(found[0].cwd, "/work");
        assert_eq!(found[0].model, Some(ModelId("claude-sonnet-5".into())));
        assert_eq!(found[0].context_window_size, Some(200_000));
        assert_eq!(found[0].last_known_token_count, Some(175));
        assert_eq!(found[0].token_usage.len(), 2);
        assert_eq!(found[0].token_usage[0].input_tokens, 2100);
        assert_eq!(found[0].token_usage[0].cached_input_tokens, 800);
        assert_eq!(found[0].token_usage[0].cache_write_input_tokens, 100);
        assert_eq!(found[0].token_usage[0].total_tokens, 2400);
    }

    #[tokio::test]
    async fn discover_sessions_reads_the_latest_ai_title_record() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b42";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: format!("/tmp/{id}.jsonl"),
            content: format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}","cwd":"/work"}}
{{"type":"ai-title","aiTitle":"Fix the retry loop","sessionId":"{id}"}}
{{"type":"ai-title","aiTitle":"Fix the retry loop, take two","sessionId":"{id}"}}"#
            ),
        }));
        let found = adapter.discover_sessions().await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].title.as_deref(),
            Some("Fix the retry loop, take two")
        );
    }

    #[tokio::test]
    async fn discover_sessions_has_no_title_when_no_ai_title_record_is_present() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b43";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: format!("/tmp/{id}.jsonl"),
            content: format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}","cwd":"/work"}}"#
            ),
        }));
        let found = adapter.discover_sessions().await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].title, None);
    }

    #[test]
    fn transcript_context_metadata_overrides_the_model_suffix() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b41";
        let session = scan_transcript(
            &format!("/tmp/{id}.jsonl"),
            &format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}"}}
{{"timestamp":"2026-09-17T23:32:34Z","type":"assistant","message":{{"model":"claude-sonnet-5[1m]","model_context_window":200000,"usage":{{"input_tokens":10}}}}}}"#
            ),
        )
        .unwrap();
        assert_eq!(session.context_window_size, Some(200_000));
    }

    #[test]
    fn transcript_model_suffix_selects_extended_context_when_metadata_is_absent() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b42";
        let session = scan_transcript(
            &format!("/tmp/{id}.jsonl"),
            &format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}"}}
{{"timestamp":"2026-09-17T23:32:34Z","type":"assistant","message":{{"model":"claude-sonnet-5[1m]","usage":{{"input_tokens":10}}}}}}"#
            ),
        )
        .unwrap();
        assert_eq!(session.context_window_size, Some(1_000_000));
    }

    #[test]
    fn observed_usage_detects_silent_promotion_to_extended_context() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b44";
        let session = scan_transcript(
            &format!("/tmp/{id}.jsonl"),
            &format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}"}}
{{"timestamp":"2026-09-17T23:32:34Z","type":"assistant","message":{{"model":"claude-sonnet-5","usage":{{"input_tokens":2,"cache_read_input_tokens":210307,"cache_creation_input_tokens":852,"output_tokens":264}}}}}}"#
            ),
        )
        .unwrap();
        assert_eq!(session.context_window_size, Some(1_000_000));
        assert_eq!(session.last_known_token_count, Some(211_425));
    }

    #[tokio::test]
    async fn discovery_fails_closed_when_transcript_identity_disagrees() {
        let filename_id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let record_id = "7fd75ec4-661a-4d52-8308-b65c60f44b85";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: format!("/tmp/{filename_id}.jsonl"),
            content: format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{record_id}","cwd":"/work"}}"#
            ),
        }));

        assert!(adapter.discover_sessions().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn discovery_fails_closed_when_records_contain_multiple_session_ids() {
        let first = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let second = "7fd75ec4-661a-4d52-8308-b65c60f44b85";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: "/tmp/not-an-id.jsonl".into(),
            content: format!("{{\"sessionId\":\"{first}\"}}\n{{\"sessionId\":\"{second}\"}}"),
        }));

        assert!(adapter.discover_sessions().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn file_cache_survives_adapter_reconstruction() {
        let path = std::env::temp_dir().join(format!("uw-cache-{}.json", uuid::Uuid::new_v4()));
        let cache = FileCacheStore::new(path.clone());
        let at = Utc::now();
        let entry = CacheEntry {
            fetched_at: at,
            sample: UsageSample {
                at,
                fetched_at: Some(at),
                source: UsageSource::ProviderReported,
                provider: Provider::ClaudeCode,
                account: None,
                plan: None,
                windows: HashMap::new(),
                credits: None,
            },
        };
        cache.save(entry.clone()).await.unwrap();
        let loaded = FileCacheStore::new(path.clone())
            .load()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.fetched_at, entry.fetched_at);
        assert_eq!(loaded.sample, entry.sample);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn transcript_export_filter_removes_keepalive_turns() {
        let result = filter_keepalive_transcript(
            "goal\n[[uw-keepalive]] no action needed, acknowledge briefly\nnext step",
        );
        assert_eq!(result, "goal\nnext step");
    }

    #[test]
    fn capabilities_match_claude_contract() {
        assert_eq!(
            ClaudeCodeAdapter::capabilities_static(),
            Capabilities {
                observe_only: false,
                can_trigger_compaction: true,
                can_advise_mid_turn: true,
                can_inject_at_session_start: true,
                can_observe_compaction: true,
                reports_token_counts: true,
                headless_resume: true,
                seed_modes: vec![SeedMode::InitialPrompt],
            }
        );
    }

    #[test]
    fn queued_compaction_capability_does_not_depend_on_local_delivery_wiring() {
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        );

        assert!(adapter.capabilities().can_trigger_compaction);
    }

    struct Credentials;
    #[async_trait::async_trait]
    impl CredentialsReader for Credentials {
        async fn read(&self) -> AdapterResult<String> {
            Ok(r#"{"claudeAiOauth":{"accessToken":"secret","email":"claude@example.com"}}"#.into())
        }
    }

    struct Http {
        status: u16,
        body: String,
        calls: Arc<Mutex<usize>>,
        headers: Arc<Mutex<Vec<(String, String)>>>,
    }
    #[async_trait::async_trait]
    impl HttpTransport for Http {
        async fn get(&self, request: HttpRequest) -> AdapterResult<HttpResponse> {
            *self.calls.lock().unwrap() += 1;
            self.headers.lock().unwrap().extend(request.headers);
            Ok(HttpResponse {
                status: self.status,
                body: self.body.clone(),
            })
        }
    }
    struct Cache {
        entry: Mutex<Option<CacheEntry>>,
    }
    #[async_trait::async_trait]
    impl CacheStore for Cache {
        async fn load(&self) -> AdapterResult<Option<CacheEntry>> {
            Ok(self.entry.lock().unwrap().clone())
        }
        async fn save(&self, entry: CacheEntry) -> AdapterResult<()> {
            *self.entry.lock().unwrap() = Some(entry);
            Ok(())
        }
    }
    fn body() -> String {
        r#"{"five_hour":{"utilization":120,"resets_at":"2026-09-17T12:00:00Z"},"seven_day":{"utilization":-4,"resets_at":null}}"#.into()
    }
    /// Always fails, so tests exercise the credentials.json fallback in
    /// `fetch_live` deterministically instead of spawning a real `claude`
    /// CLI process (which may not be installed, and would report whatever
    /// account is actually logged in on the test machine).
    struct FailingAuthStatus;
    #[async_trait::async_trait]
    impl AuthStatusReader for FailingAuthStatus {
        async fn read(&self) -> AdapterResult<Value> {
            Err(AdapterError::Other("no claude CLI in tests".into()))
        }
    }
    fn adapter(status: u16, cache: Arc<Cache>, calls: Arc<Mutex<usize>>) -> ClaudeCodeAdapter {
        ClaudeCodeAdapter::with_dependencies(
            Arc::new(Credentials),
            Arc::new(Http {
                status,
                body: body(),
                calls,
                headers: Arc::new(Mutex::new(vec![])),
            }),
            cache,
            "1.2.3".into(),
            Arc::new(|| Utc.timestamp_opt(1_800_000_000, 0).unwrap()),
        )
        .with_auth_status(Arc::new(FailingAuthStatus))
    }
    #[tokio::test]
    async fn fetch_usage_parses_and_clamps() {
        let calls = Arc::new(Mutex::new(0));
        let result = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            calls,
        )
        .fetch_usage(None)
        .await
        .unwrap();
        assert_eq!(result.windows.len(), 2);
        assert_eq!(result.account, Some(AccountId("claude@example.com".into())));
        assert_eq!(
            result.windows[&WindowKey {
                provider: Provider::ClaudeCode,
                kind: WindowKind::Rolling { minutes: 300 }
            }]
                .pct,
            100.0
        );
        assert_eq!(
            result.windows[&WindowKey {
                provider: Provider::ClaudeCode,
                kind: WindowKind::Rolling { minutes: 10080 }
            }]
                .pct,
            0.0
        );
    }
    #[tokio::test]
    async fn auth_failure_does_not_use_stale_cache() {
        let cache = Arc::new(Cache {
            entry: Mutex::new(Some(CacheEntry {
                fetched_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                sample: UsageSample {
                    at: Utc::now(),
                    fetched_at: None,
                    source: UsageSource::ProviderReported,
                    provider: Provider::ClaudeCode,
                    account: None,
                    plan: None,
                    windows: Default::default(),
                    credits: None,
                },
            })),
        });
        assert!(matches!(
            adapter(401, cache, Arc::new(Mutex::new(0)))
                .fetch_usage(None)
                .await,
            Err(AdapterError::Auth)
        ));
    }
    #[tokio::test]
    async fn transient_failure_uses_stale_cache() {
        let sample = UsageSample {
            at: Utc::now(),
            fetched_at: None,
            source: UsageSource::ProviderReported,
            provider: Provider::ClaudeCode,
            account: None,
            plan: None,
            windows: Default::default(),
            credits: None,
        };
        let cache = Arc::new(Cache {
            entry: Mutex::new(Some(CacheEntry {
                fetched_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                sample: sample.clone(),
            })),
        });
        let got = adapter(503, cache, Arc::new(Mutex::new(0)))
            .fetch_usage(None)
            .await
            .unwrap();
        assert_eq!(got, sample);
    }
    #[tokio::test]
    async fn fresh_cache_skips_http() {
        let sample = UsageSample {
            at: Utc::now(),
            fetched_at: None,
            source: UsageSource::ProviderReported,
            provider: Provider::ClaudeCode,
            account: Some(AccountId("claude@example.com".into())),
            plan: None,
            windows: Default::default(),
            credits: None,
        };
        let calls = Arc::new(Mutex::new(0));
        let cache = Arc::new(Cache {
            entry: Mutex::new(Some(CacheEntry {
                fetched_at: Utc.timestamp_opt(1_799_999_950, 0).unwrap(),
                sample: sample.clone(),
            })),
        });
        assert_eq!(
            adapter(200, cache, calls.clone())
                .fetch_usage(None)
                .await
                .unwrap(),
            sample
        );
        assert_eq!(*calls.lock().unwrap(), 0);
    }
    fn cache_at_age(seconds: i64) -> Arc<Cache> {
        let at = Utc.timestamp_opt(1_800_000_000 - seconds, 0).unwrap();
        Arc::new(Cache {
            entry: Mutex::new(Some(CacheEntry {
                fetched_at: at,
                sample: UsageSample {
                    at,
                    fetched_at: Some(at),
                    source: UsageSource::ProviderReported,
                    provider: Provider::ClaudeCode,
                    account: Some(AccountId("claude@example.com".into())),
                    plan: None,
                    windows: Default::default(),
                    credits: None,
                },
            })),
        })
    }

    #[tokio::test]
    async fn sensitive_fetch_refreshes_a_cache_younger_than_the_normal_ttl() {
        for max_age in [15, 5, 1] {
            let calls = Arc::new(Mutex::new(0));
            let got = adapter(200, cache_at_age(50), calls.clone())
                .fetch_usage_with_max_age(None, std::time::Duration::from_secs(max_age))
                .await
                .unwrap();
            assert_eq!(*calls.lock().unwrap(), 1, "max_age={max_age}");
            assert_eq!(got.windows.len(), 2);
            assert_eq!(
                got.fetched_at,
                Some(Utc.timestamp_opt(1_800_000_000, 0).unwrap())
            );
        }
    }

    #[tokio::test]
    async fn requested_cache_age_reuses_younger_samples_and_preserves_normal_caching() {
        for max_age in [15, 5, 1] {
            let cache = cache_at_age(0);
            let expected = cache.entry.lock().unwrap().as_ref().unwrap().sample.clone();
            let calls = Arc::new(Mutex::new(0));
            assert_eq!(
                adapter(200, cache, calls.clone())
                    .fetch_usage_with_max_age(None, std::time::Duration::from_secs(max_age))
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(*calls.lock().unwrap(), 0);
        }
        let cache = cache_at_age(50);
        let expected = cache.entry.lock().unwrap().as_ref().unwrap().sample.clone();
        let calls = Arc::new(Mutex::new(0));
        assert_eq!(
            adapter(200, cache, calls.clone())
                .fetch_usage(None)
                .await
                .unwrap(),
            expected
        );
        assert_eq!(*calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn requested_cache_age_retains_stale_fallback_on_transient_error() {
        for status in [429, 503] {
            let cache = cache_at_age(50);
            let expected = cache.entry.lock().unwrap().as_ref().unwrap().sample.clone();
            let calls = Arc::new(Mutex::new(0));
            assert_eq!(
                adapter(status, cache, calls.clone())
                    .fetch_usage_with_max_age(None, std::time::Duration::from_secs(1))
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(*calls.lock().unwrap(), 1);
        }
    }

    #[tokio::test]
    async fn requested_cache_age_does_not_hide_auth_failure() {
        for status in [401, 403] {
            let calls = Arc::new(Mutex::new(0));
            assert!(matches!(
                adapter(status, cache_at_age(50), calls.clone())
                    .fetch_usage_with_max_age(None, std::time::Duration::from_secs(1))
                    .await,
                Err(AdapterError::Auth)
            ));
            assert_eq!(*calls.lock().unwrap(), 1);
        }
    }

    #[test]
    fn compact_message_uses_claude_command_shape() {
        assert_eq!(
            compact_instructions("requested prompt"),
            "/compact requested prompt"
        );
        assert_eq!(compact_instructions("/compact"), "/compact");
        assert_eq!(compact_instructions(""), "/compact");
    }
    #[tokio::test]
    async fn compact_sends_only_the_compact_command_and_optional_message() {
        let sent = Arc::new(Mutex::new(None));
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_delivery(
            Arc::new(NoHook),
            Arc::new(RecordingMessenger(sent.clone())),
            Arc::new(Recorder(Arc::new(Mutex::new(None)))),
        );
        let session = session(LaunchMode::Headless);
        adapter
            .compact(
                &session,
                &CompactionRequest {
                    id: uuid::Uuid::new_v4(),
                    session_id: session.id.clone(),
                    kind: CompactionKind::OpportunisticIdle,
                    prompt: "actual prompt".into(),
                    reason: "ignored routing metadata".into(),
                    resume_after_compaction: false,
                    preempt: true,
                    status: CompactionStatus::Pending,
                    created_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        let message = sent.lock().unwrap().clone().unwrap();
        assert_eq!(message, "/compact actual prompt");
        assert!(!message.contains("ignored routing metadata"));
    }
    struct Recorder(Arc<Mutex<Option<ProcessSpec>>>);
    #[async_trait::async_trait]
    impl ProcessSpawner for Recorder {
        async fn run(&self, spec: ProcessSpec) -> AdapterResult<crate::process::ProcessOutput> {
            *self.0.lock().unwrap() = Some(spec);
            Ok(crate::process::ProcessOutput {
                session_id: Some("01a0aecd-181f-7491-90b1-d2cc8beaad3f".into()),
                ..Default::default()
            })
        }
    }
    fn session(mode: LaunchMode) -> SessionSummary {
        SessionSummary {
            id: SessionId("3507fe61-2d6b-4aae-a0b6-4fe4eec12b40".into()),
            lineage: SessionLineage::default(),
            harness: Provider::ClaudeCode,
            model: None,
            account: None,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            cwd: "/work".into(),
            state_path: None,
            context_window_size: None,
            last_known_token_count: None,
            last_known_context_pct: None,
            cache_ttl: None,
            launch_mode: mode,
            pid: None,
            stopped_reason: None,
            superseded_stop_reason: None,
            superseded_stop_reason_at: None,
            superseded_stop_reason_note: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        }
    }
    #[tokio::test]
    async fn resume_and_seed_build_expected_commands_without_spawning() {
        let record = Arc::new(Mutex::new(None));
        let spawner = Arc::new(Recorder(record.clone()));
        let a = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_delivery(Arc::new(NoHook), Arc::new(NoMessenger), spawner);
        a.resume_session(&session(LaunchMode::Headless), None)
            .await
            .unwrap();
        assert_eq!(
            *record.lock().unwrap(),
            Some(ProcessSpec {
                program: "claude".into(),
                args: vec![
                    "--print".into(),
                    "--resume".into(),
                    "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40".into()
                ],
                cwd: "/work".into()
            })
        );
        let seed = SeedContext {
            from_session: None,
            summary: "sum".into(),
            model: ModelId("m".into()),
            cwd: "/seed".into(),
        };
        a.seed_new_session(SeedMode::InitialPrompt, &seed)
            .await
            .unwrap();
        assert_eq!(
            *record.lock().unwrap(),
            Some(ProcessSpec {
                program: "claude".into(),
                args: vec![
                    "-p".into(),
                    "--output-format".into(),
                    "json".into(),
                    "sum".into()
                ],
                cwd: "/seed".into()
            })
        );
        assert!(matches!(
            a.seed_new_session(SeedMode::ForkWithHistory, &seed).await,
            Err(AdapterError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn resume_rejects_non_uuid_and_superseded_session_ownership() {
        let record = Arc::new(Mutex::new(None));
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_delivery(
            Arc::new(NoHook),
            Arc::new(NoMessenger),
            Arc::new(Recorder(record.clone())),
        );

        let mut malformed = session(LaunchMode::Headless);
        malformed.id = SessionId("not-a-uuid".into());
        assert!(matches!(
            adapter.resume_session(&malformed, None).await,
            Err(AdapterError::Other(message)) if message.contains("valid UUID")
        ));
        let mut superseded = session(LaunchMode::Headless);
        superseded.id = SessionId("3507fe61-2d6b-4aae-a0b6-4fe4eec12b40".into());
        superseded.superseded_by = Some(SessionId("7fd75ec4-661a-4d52-8308-b65c60f44b85".into()));
        assert!(matches!(
            adapter.resume_session(&superseded, None).await,
            Err(AdapterError::Other(message)) if message.contains("superseded")
        ));
        assert!(record.lock().unwrap().is_none());
    }

    fn near_limit_sample(at: DateTime<Utc>) -> UsageSample {
        UsageSample {
            at,
            fetched_at: Some(at),
            source: UsageSource::ProviderReported,
            provider: Provider::ClaudeCode,
            account: None,
            plan: None,
            windows: HashMap::from([(
                WindowKey {
                    provider: Provider::ClaudeCode,
                    kind: WindowKind::Rolling { minutes: 300 },
                },
                UsageWindowState::new(99.8, false, true, None, None),
            )]),
            credits: None,
        }
    }

    #[tokio::test]
    async fn stop_detection_requires_quota_error_evidence_and_matching_near_limit_usage() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: format!("/tmp/{id}.jsonl"),
            content: format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}","type":"error","error":{{"type":"rate_limit_error","message":"usage limit reached"}}}}"#
            ),
        }));
        let sample = near_limit_sample(Utc::now());

        assert_eq!(
            adapter
                .detect_stop_with_usage(&SessionId(id.into()), Some(&sample))
                .await
                .unwrap(),
            Some(StopReason::UsageLimit {
                window: WindowKey {
                    provider: Provider::ClaudeCode,
                    kind: WindowKind::Rolling { minutes: 300 },
                }
            })
        );
    }

    #[tokio::test]
    async fn stop_detection_accepts_verified_local_command_spend_limit_stderr() {
        // Captured from real session 9675ac22-f2d9-490c-80c5-76f6517bfcf4
        // (Claude Code 2.1.267) after /compact failed at the monthly spend ceiling.
        let id = "9675ac22-f2d9-490c-80c5-76f6517bfcf4";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: format!("/tmp/{id}.jsonl"),
            content: format!(
                r#"{{"type":"system","subtype":"local_command","sessionId":"{id}","content":"<local-command-stderr>Error during compaction: You've hit your monthly spend limit · raise it at claude.ai/settings/usage?from=cc_cli_limit_message · your session limit resets 10:40pm (America/Denver)</local-command-stderr>"}}"#
            ),
        }));
        assert_eq!(
            adapter
                .detect_stop_with_usage(&SessionId(id.into()), Some(&near_limit_sample(Utc::now())))
                .await
                .unwrap(),
            Some(StopReason::UsageLimit {
                window: WindowKey {
                    provider: Provider::ClaudeCode,
                    kind: WindowKind::Rolling { minutes: 300 },
                }
            })
        );
    }

    #[tokio::test]
    async fn stop_detection_accepts_verified_is_api_error_message_spend_limit() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: format!("/tmp/{id}.jsonl"),
            content: format!(
                r#"{{"type":"assistant","sessionId":"{id}","isApiErrorMessage":true,"message":{{"role":"assistant","content":[{{"type":"text","text":"You've hit your monthly spend limit. Switch to another model, or manage usage credits at claude.ai/settings/usage?from=cc_cli_limit_message, to continue."}}]}}}}"#
            ),
        }));
        assert!(
            adapter
                .detect_stop_with_usage(&SessionId(id.into()), Some(&near_limit_sample(Utc::now())))
                .await
                .unwrap()
                .is_some()
        );
    }

    /// Several transcripts; records which paths were read.
    struct TranscriptDir {
        files: Mutex<Vec<(String, String)>>,
        reads: Mutex<Vec<String>>,
    }

    impl TranscriptDir {
        fn new(files: &[(&str, String)]) -> Self {
            Self {
                files: Mutex::new(
                    files
                        .iter()
                        .map(|(p, c)| (p.to_string(), c.clone()))
                        .collect(),
                ),
                reads: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl TranscriptFileSystem for TranscriptDir {
        async fn jsonl_files(&self) -> AdapterResult<Vec<String>> {
            Ok(self
                .files
                .lock()
                .unwrap()
                .iter()
                .map(|(p, _)| p.clone())
                .collect())
        }
        async fn read_to_string(&self, path: &str) -> AdapterResult<String> {
            self.reads.lock().unwrap().push(path.to_owned());
            self.files
                .lock()
                .unwrap()
                .iter()
                .find(|(p, _)| p == path)
                .map(|(_, c)| c.clone())
                .ok_or_else(|| AdapterError::Other("missing".into()))
        }
    }

    fn spend_limit_line(id: &str) -> String {
        format!(
            r#"{{"type":"assistant","sessionId":"{id}","isApiErrorMessage":true,"message":{{"role":"assistant","content":[{{"type":"text","text":"You've hit your monthly spend limit."}}]}}}}"#
        )
    }

    fn plain_adapter() -> ClaudeCodeAdapter {
        adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
    }

    #[tokio::test]
    async fn quota_check_reads_only_the_sessions_own_transcripts() {
        // why: the leak. Every tick, each warm session's check read every
        // transcript on the machine; it must read only its own.
        // catches: filtering after reading (path || content contains id).
        let (a, b) = (
            "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40",
            "9b1f0c1e-0000-4000-8000-000000000001",
        );
        let dir = Arc::new(TranscriptDir::new(&[
            (&format!("/p/{a}.jsonl"), spend_limit_line(a)),
            (
                &format!("/p/{b}.jsonl"),
                format!(r#"{{"type":"user","sessionId":"{b}"}}"#),
            ),
            (
                &format!("/p/{b}/subagents/agent-1.jsonl"),
                format!(r#"{{"type":"user","sessionId":"{b}"}}"#),
            ),
        ]));
        let adapter = plain_adapter().with_transcript_fs(dir.clone());
        assert!(
            !adapter
                .has_quota_error_evidence(&SessionId(b.into()))
                .await
                .unwrap()
        );
        let reads = dir.reads.lock().unwrap().clone();
        assert_eq!(reads.len(), 2, "{reads:?}");
        assert!(reads.iter().all(|path| path.contains(b)), "{reads:?}");
    }

    #[tokio::test]
    async fn quota_check_finds_a_renamed_transcript_through_discovery() {
        // why: transcripts whose file name is not the session id were found by
        // content before; discovery's cache now carries that mapping.
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let dir = Arc::new(TranscriptDir::new(&[(
            "/p/renamed.jsonl",
            format!(r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}","cwd":"/work"}}"#)
                + "\n"
                + &spend_limit_line(id),
        )]));
        let adapter = plain_adapter().with_transcript_fs(dir);
        assert!(
            !adapter
                .has_quota_error_evidence(&SessionId(id.into()))
                .await
                .unwrap()
        );
        adapter.discover_sessions().await.unwrap();
        assert!(
            adapter
                .has_quota_error_evidence(&SessionId(id.into()))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn discovery_forgets_deleted_transcripts() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let path = format!("/p/{id}.jsonl");
        let dir = Arc::new(TranscriptDir::new(&[(
            &path,
            format!(r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}","cwd":"/work"}}"#),
        )]));
        let adapter = plain_adapter().with_transcript_fs(dir.clone());
        assert_eq!(adapter.discover_sessions().await.unwrap().len(), 1);
        assert!(adapter.known_ids.lock().unwrap().contains_key(&path));
        dir.files.lock().unwrap().clear();
        assert!(adapter.discover_sessions().await.unwrap().is_empty());
        assert!(adapter.known_ids.lock().unwrap().is_empty());
        assert!(adapter.discovery_cache.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn streamed_reads_match_whole_file_reads() {
        // why: the real file system now streams; it must agree with the
        // whole-string functions the fixtures and older code use.
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let dir = std::env::temp_dir().join(format!("uw-stream-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.jsonl"));
        let content = format!(
            "{}\n{}\r\n{}\n",
            format_args!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}","cwd":"/work"}}"#
            ),
            r#"{"type":"user","message":"x"}"#,
            spend_limit_line(id)
        );
        std::fs::write(&path, &content).unwrap();
        let path = path.to_string_lossy().into_owned();
        let fs = ClaudeTranscriptFileSystem::default();
        assert_eq!(
            fs.scan(&path).await.unwrap(),
            scan_transcript(&path, &content)
        );
        assert!(fs.scan(&path).await.unwrap().is_some());
        assert!(fs.has_quota_error(&path).await.unwrap());
        std::fs::write(&path, content.replace("spend limit", "nothing")).unwrap();
        assert!(!fs.has_quota_error(&path).await.unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // why: old transcripts and non-assistant records cannot invent a cache-write TTL.
    #[test]
    fn discovery_leaves_unknown_cache_ttl_for_legacy_or_non_assistant_records() {
        let path = "/tmp/3507fe61-2d6b-4aae-a0b6-4fe4eec12b40.jsonl";
        for kind in ["user", "system", "assistant"] {
            let split = if kind == "assistant" {
                serde_json::json!({})
            } else {
                serde_json::json!({"ephemeral_1h_input_tokens": 1})
            };
            let record =
                serde_json::json!({"type": kind, "message": {"usage": {"cache_creation": split}}});
            assert_eq!(
                scan_transcript(path, &record.to_string())
                    .unwrap()
                    .cache_ttl,
                None
            );
        }
    }

    // why: TTL is root-session cache-write metadata, retained across cache hits and polls.
    // catches: ignoring 1h writes, accepting sidechains, or clearing TTL on a zero write.
    #[tokio::test]
    async fn discovery_tracks_cache_ttl_in_the_incremental_scan() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let path = dir.path().join(format!("{id}.jsonl"));
        let fs = ClaudeTranscriptFileSystem::default();
        let record = |hour, short, sidechain| {
            serde_json::json!({
            "sessionId": id, "type": "assistant", "isSidechain": sidechain,
            "timestamp": "2026-09-17T23:32:34Z", "message": {"content": [], "usage": {
                "cache_creation": {"ephemeral_1h_input_tokens": hour,
                                   "ephemeral_5m_input_tokens": short}}}})
        };
        std::fs::write(&path, format!("{}\n", record(0, 10, false))).unwrap();
        for (hour, short, sidechain, seconds) in [
            (0, 0, false, 300),
            (10, 0, true, 300),
            (10, 10, false, 3600),
            (0, 0, false, 3600),
            (0, 10, false, 300),
        ] {
            writeln!(
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap(),
                "{}",
                record(hour, short, sidechain)
            )
            .unwrap();
            let found = fs.scan(path.to_str().unwrap()).await.unwrap().unwrap();
            let encoded = serde_json::to_value(found).unwrap();
            let detected: Option<chrono::Duration> = encoded
                .get("cache_ttl")
                .and_then(|value| serde_json::from_value(value.clone()).ok());
            assert_eq!(
                detected,
                Some(chrono::Duration::seconds(seconds)),
                "write=({hour},{short}), sidechain={sidechain}"
            );
        }
    }

    // why: the production filesystem must emit each append once, without retaining or replaying old usage.
    #[tokio::test]
    async fn discovery_reads_only_new_claude_usage() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let path = dir.path().join(format!("{id}.jsonl"));
        let line = |second, tokens| {
            format!(
                r#"{{"sessionId":"{id}","timestamp":"2026-09-17T23:32:{second}Z","message":{{"model":"sonnet","usage":{{"input_tokens":{tokens},"output_tokens":2}}}}}}"#
            )
        };
        std::fs::write(&path, line("34", 10) + "\n").unwrap();
        let fs = ClaudeTranscriptFileSystem::default();
        let path = path.to_str().unwrap();
        assert_eq!(
            fs.scan(path).await.unwrap().unwrap().token_usage[0].total_tokens,
            12
        );
        assert_eq!(
            fs.scan(path).await.unwrap().unwrap().token_usage.len(),
            1,
            "uncommitted records must be replayed"
        );
        let pending = fs.scan(path).await.unwrap().unwrap().token_usage;
        fs.acknowledge_scan(path, &SessionId(id.into()), &pending)
            .await
            .unwrap();
        assert!(fs.scan(path).await.unwrap().unwrap().token_usage.is_empty());
        writeln!(
            std::fs::OpenOptions::new().append(true).open(path).unwrap(),
            "{}",
            line("35", 20)
        )
        .unwrap();
        let appended = fs.scan(path).await.unwrap().unwrap();
        assert_eq!(appended.token_usage.len(), 1);
        assert_eq!(appended.token_usage[0].total_tokens, 22);
    }

    #[tokio::test]
    async fn stop_detection_ignores_is_api_error_message_auth_failures() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: format!("/tmp/{id}.jsonl"),
            content: format!(
                r#"{{"type":"assistant","sessionId":"{id}","isApiErrorMessage":true,"message":{{"role":"assistant","content":[{{"type":"text","text":"Login expired · Please run /login"}}]}}}}"#
            ),
        }));
        assert_eq!(
            adapter
                .detect_stop_with_usage(&SessionId(id.into()), Some(&near_limit_sample(Utc::now())))
                .await
                .unwrap(),
            None
        );
    }

    #[test]
    fn transcript_quota_detector_matches_verified_shapes_only() {
        assert!(transcript_has_quota_error(
            r#"{"type":"system","subtype":"local_command","content":"<local-command-stderr>Error during compaction: You've hit your monthly spend limit · cc_cli_limit_message</local-command-stderr>"}"#
        ));
        assert!(transcript_has_quota_error(
            r#"{"type":"assistant","isApiErrorMessage":true,"message":{"content":[{"type":"text","text":"You've hit your monthly spend limit"}]}}"#
        ));
        assert!(!transcript_has_quota_error(
            r#"{"type":"assistant","isApiErrorMessage":true,"message":{"content":[{"type":"text","text":"Failed to authenticate: OAuth session expired"}]}}"#
        ));
        assert!(!transcript_has_quota_error(
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"You've hit your monthly spend limit"}]}}"#
        ));
    }

    #[tokio::test]
    async fn near_limit_usage_without_quota_error_evidence_does_not_invent_a_stop() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: format!("/tmp/{id}.jsonl"),
            content: format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}","type":"assistant","message":{{"content":"done"}}}}"#
            ),
        }));
        let sample = near_limit_sample(Utc::now());

        assert_eq!(
            adapter
                .detect_stop_with_usage(&SessionId(id.into()), Some(&sample))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn quota_error_without_near_limit_usage_does_not_invent_a_stop() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let adapter = adapter(
            200,
            Arc::new(Cache {
                entry: Mutex::new(None),
            }),
            Arc::new(Mutex::new(0)),
        )
        .with_transcript_fs(Arc::new(TranscriptFixture {
            path: format!("/tmp/{id}.jsonl"),
            content: format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","sessionId":"{id}","type":"error","error":{{"type":"rate_limit_error","message":"usage limit reached"}}}}"#
            ),
        }));
        let mut sample = near_limit_sample(Utc::now());
        sample
            .windows
            .values_mut()
            .for_each(|window| window.pct = 42.0);

        assert_eq!(
            adapter
                .detect_stop_with_usage(&SessionId(id.into()), Some(&sample))
                .await
                .unwrap(),
            None
        );
    }

    struct NoHook;
    #[async_trait::async_trait]
    impl HookChannel for NoHook {
        async fn advise(&self, _: &SessionId, _: &str) -> AdapterResult<DeliveryOutcome> {
            Ok(DeliveryOutcome::Delivered)
        }
    }
    struct NoMessenger;
    #[async_trait::async_trait]
    impl SessionMessenger for NoMessenger {
        async fn send(&self, _: &SessionId, _: &str) -> AdapterResult<DeliveryOutcome> {
            Ok(DeliveryOutcome::Delivered)
        }
    }
    struct RecordingMessenger(Arc<Mutex<Option<String>>>);
    #[async_trait::async_trait]
    impl SessionMessenger for RecordingMessenger {
        async fn send(&self, _: &SessionId, text: &str) -> AdapterResult<DeliveryOutcome> {
            *self.0.lock().unwrap() = Some(text.into());
            Ok(DeliveryOutcome::Delivered)
        }
    }
}
