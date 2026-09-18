use crate::process::{ProcessSpawner, ProcessSpec};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, DiscoveredSession,
    HarnessAdapter, SeedContext, SeedMode, StatusEvent, TokenUsageRecord,
};
use uw_core::model::*;

#[async_trait]
pub trait TranscriptFileSystem: Send + Sync {
    async fn jsonl_files(&self) -> AdapterResult<Vec<String>>;
    async fn read_to_string(&self, path: &str) -> AdapterResult<String>;
}

pub struct ClaudeTranscriptFileSystem;
pub fn filter_keepalive_transcript(content: &str) -> String {
    content
        .lines()
        .filter(|line| !line.contains("[[uw-keepalive]]"))
        .collect::<Vec<_>>()
        .join("\n")
}
#[async_trait]
impl TranscriptFileSystem for ClaudeTranscriptFileSystem {
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
        Ok(out)
    }
    async fn read_to_string(&self, path: &str) -> AdapterResult<String> {
        tokio::fs::read_to_string(path)
            .await
            .map_err(|e| AdapterError::Other(e.to_string()))
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
            transcript_fs: Arc::new(ClaudeTranscriptFileSystem),
        }
    }
    pub fn capabilities_static() -> Capabilities {
        Capabilities {
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
    async fn fetch_live(&self) -> AdapterResult<UsageSample> {
        let value: Value = serde_json::from_str(&self.credentials.read().await?)
            .map_err(|e| AdapterError::Other(e.to_string()))?;
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
            account: None,
            windows,
            credits: None,
        })
    }
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
    let prompt = prompt.trim();
    if prompt == "/compact" || prompt.starts_with("/compact ") {
        prompt.to_owned()
    } else if prompt.is_empty() {
        "/compact".into()
    } else {
        format!("/compact {prompt}")
    }
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
    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        if let Some(entry) = self.cache.load().await?
            && (self.now)() - entry.fetched_at < chrono::Duration::seconds(120)
        {
            return Ok(entry.sample);
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
        let mut args = vec!["--print".to_string(), "--resume".into(), session.id.0.clone()];
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

    async fn export_transcript(&self, session: &SessionSummary) -> AdapterResult<String> {
        let mut matching = Vec::new();
        for path in self.transcript_fs.jsonl_files().await? {
            let content = self.transcript_fs.read_to_string(&path).await?;
            if path.contains(&session.id.0) || content.contains(&session.id.0) {
                matching.push(content);
            }
        }
        Ok(filter_keepalive_transcript(&matching.join("\n")))
    }

    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        let mut discovered = Vec::new();
        for path in self.transcript_fs.jsonl_files().await? {
            let content = self.transcript_fs.read_to_string(&path).await?;
            if let Some(session) = scan_transcript(&path, &content) {
                discovered.push(session);
            }
        }
        Ok(discovered)
    }
}

fn scan_transcript(path: &str, content: &str) -> Option<DiscoveredSession> {
    let filename_id = std::path::Path::new(path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| uuid::Uuid::parse_str(stem).is_ok())
        .map(str::to_owned);
    let mut record_id: Option<String> = None;
    let mut cwd = None;
    let mut model = None;
    let mut context_window_size = None;
    let mut first_seen = None;
    let mut last_seen = None;
    let mut last_known_token_count = None;
    let mut token_usage = Vec::new();

    for line in content.lines() {
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(candidate) = record
            .get("sessionId")
            .or_else(|| record.get("session_id"))
            .and_then(Value::as_str)
            .filter(|value| uuid::Uuid::parse_str(value).is_ok())
        {
            if record_id.as_deref().is_some_and(|known| known != candidate) {
                return None;
            }
            record_id.get_or_insert_with(|| candidate.to_owned());
        }
        if cwd.is_none() {
            cwd = record.get("cwd").and_then(Value::as_str).map(str::to_owned);
        }
        let timestamp = record
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc));
        if let Some(at) = timestamp {
            first_seen.get_or_insert(at);
            last_seen = Some(at);
        }
        if let Some(value) = record.pointer("/message/model").and_then(Value::as_str) {
            model = Some(ModelId(value.to_owned()));
        }
        if let Some(value) = record
            .pointer("/message/usage/input_tokens")
            .and_then(Value::as_u64)
        {
            context_window_size = record
                .pointer("/message/model_context_window")
                .and_then(Value::as_u64)
                .or(context_window_size);
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
            let total_input = value
                .saturating_add(cache_read)
                .saturating_add(cache_write);
            let total = total_input.saturating_add(output);
            let Some(at) = timestamp else { continue };
            last_known_token_count = Some(total);
            token_usage.push(TokenUsageRecord {
                at,
                model: model.clone(),
                input_tokens: total_input,
                cached_input_tokens: cache_read,
                cache_write_input_tokens: cache_write,
                output_tokens: output,
                reasoning_output_tokens: 0,
                total_tokens: total,
            });
        }
    }

    if filename_id
        .as_deref()
        .zip(record_id.as_deref())
        .is_some_and(|(filename, record)| filename != record)
    {
        return None;
    }
    let id = filename_id.or(record_id)?;
    Some(DiscoveredSession {
        id: SessionId(id),
        cwd: cwd.unwrap_or_else(|| ".".into()),
        model,
        context_window_size,
        last_known_token_count,
        first_seen,
        last_seen,
        state_path: Some(path.to_owned()),
        token_usage,
    })
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
        assert_eq!(found[0].last_known_token_count, Some(175));
        assert_eq!(found[0].token_usage.len(), 2);
        assert_eq!(found[0].token_usage[0].input_tokens, 2100);
        assert_eq!(found[0].token_usage[0].cached_input_tokens, 800);
        assert_eq!(found[0].token_usage[0].cache_write_input_tokens, 100);
        assert_eq!(found[0].token_usage[0].total_tokens, 2400);
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
            content: format!(
                "{{\"sessionId\":\"{first}\"}}\n{{\"sessionId\":\"{second}\"}}"
            ),
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
            Ok(r#"{"claudeAiOauth":{"accessToken":"secret"}}"#.into())
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
            account: None,
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
            harness: Provider::ClaudeCode,
            model: None,
            account: None,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            cwd: "/work".into(),
            state_path: None,
            context_window_size: None,
            last_known_token_count: None,
            launch_mode: mode,
            pid: None,
            stopped_reason: None,
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
        superseded.superseded_by = Some(SessionId(
            "7fd75ec4-661a-4d52-8308-b65c60f44b85".into(),
        ));
        assert!(matches!(
            adapter.resume_session(&superseded, None).await,
            Err(AdapterError::Other(message)) if message.contains("superseded")
        ));
        assert!(record.lock().unwrap().is_none());
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
