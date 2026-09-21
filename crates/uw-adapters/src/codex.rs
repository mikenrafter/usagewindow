use crate::process::{ProcessSpawner, ProcessSpec};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, DiscoveredSession, HarnessAdapter,
    SeedContext, SeedMode, StatusEvent, TokenUsageRecord, TurnPreview, TurnRole,
};
use uw_core::model::*;

#[cfg(test)]
static ROLLOUT_SCAN_COUNTS: LazyLock<Mutex<HashMap<PathBuf, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
fn rollout_scan_count(path: &std::path::Path) -> usize {
    ROLLOUT_SCAN_COUNTS
        .lock()
        .unwrap()
        .get(path)
        .copied()
        .unwrap_or_default()
}

#[async_trait]
pub trait AppServerTransport: Send + Sync {
    async fn call(&self, method: &str, params: Value) -> AdapterResult<Value>;
}
/// Persistent app-server transport: spawns `codex app-server` at most once and
/// reuses that single child process/stdio session for every subsequent
/// JSON-RPC call, instead of spawning a new process per call.
pub struct CodexAppServerTransport {
    program: String,
    args: Vec<String>,
    session: tokio::sync::Mutex<Option<AppServerSession>>,
}

struct AppServerSession {
    _child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: tokio::io::BufReader<tokio::process::ChildStdout>,
    next_id: u64,
}

impl CodexAppServerTransport {
    pub fn new() -> Self {
        Self::spawn_with("codex", vec!["app-server".into()])
    }

    fn spawn_with(program: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            program: program.into(),
            args,
            session: tokio::sync::Mutex::new(None),
        }
    }

    async fn open_session(&self) -> AdapterResult<AppServerSession> {
        use std::process::Stdio;
        let mut child = tokio::process::Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| AdapterError::Transient(e.to_string()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| AdapterError::Other("app-server stdin unavailable".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AdapterError::Other("app-server stdout unavailable".into()))?;
        Ok(AppServerSession {
            _child: child,
            stdin,
            stdout: tokio::io::BufReader::new(stdout),
            next_id: 1,
        })
    }
}

impl Default for CodexAppServerTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexAppServerTransport {
    /// Writes one JSON-RPC request and reads lines from the session's stdout
    /// until the response carrying the matching `id` shows up, discarding any
    /// unsolicited notification lines the app-server may interleave.
    async fn roundtrip(
        session: &mut AppServerSession,
        method: &str,
        params: Value,
    ) -> AdapterResult<Value> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let id = session.next_id;
        session.next_id += 1;
        let request = json!({"method": method, "id": id, "params": params});
        session
            .stdin
            .write_all(format!("{request}\n").as_bytes())
            .await
            .map_err(|e| AdapterError::Transient(e.to_string()))?;
        session
            .stdin
            .flush()
            .await
            .map_err(|e| AdapterError::Transient(e.to_string()))?;
        loop {
            let mut line = String::new();
            let bytes_read = session
                .stdout
                .read_line(&mut line)
                .await
                .map_err(|e| AdapterError::Transient(e.to_string()))?;
            if bytes_read == 0 {
                return Err(AdapterError::Transient("app-server closed stdout".into()));
            }
            let value: Value = serde_json::from_str(line.trim())
                .map_err(|e| AdapterError::Other(e.to_string()))?;
            if value.get("id").and_then(Value::as_u64) == Some(id) {
                return Ok(value);
            }
        }
    }
}

#[async_trait]
impl AppServerTransport for CodexAppServerTransport {
    async fn call(&self, method: &str, params: Value) -> AdapterResult<Value> {
        let mut guard = self.session.lock().await;
        if guard.is_none() {
            *guard = Some(self.open_session().await?);
        }
        let result = Self::roundtrip(guard.as_mut().unwrap(), method, params).await;
        if result.is_err() {
            // Drop the broken session so the next call respawns a fresh one
            // instead of reusing a process that may be dead or desynced.
            *guard = None;
        }
        result
    }
}
pub struct CodexAdapter {
    transport: Arc<dyn AppServerTransport>,
    spawner: Arc<dyn ProcessSpawner>,
    now: Arc<dyn Fn() -> chrono::DateTime<Utc> + Send + Sync>,
    hook: Option<Arc<dyn crate::claude_code::HookChannel>>,
    sessions_root: Option<PathBuf>,
    discovery_cache: Mutex<HashMap<PathBuf, CachedRollout>>,
}

struct CachedRollout {
    fingerprint: (u64, Option<std::time::SystemTime>),
    session: DiscoveredSession,
}
impl CodexAdapter {
    pub fn new(transport: Arc<dyn AppServerTransport>) -> Self {
        Self {
            transport,
            spawner: Arc::new(crate::process::TokioProcessSpawner),
            now: Arc::new(Utc::now),
            hook: None,
            sessions_root: None,
            discovery_cache: Mutex::new(HashMap::new()),
        }
    }
    pub fn real() -> Self {
        Self::new(Arc::new(CodexAppServerTransport::new()))
    }
    pub fn with_spawner(mut self, spawner: Arc<dyn ProcessSpawner>) -> Self {
        self.spawner = spawner;
        self
    }
    pub fn with_hook_channel(mut self, hook: Arc<dyn crate::claude_code::HookChannel>) -> Self {
        self.hook = Some(hook);
        self
    }
    /// Overrides where `discover_sessions` looks for rollout files. Defaults to
    /// `$CODEX_HOME/sessions` (or `~/.codex/sessions`); exists so tests don't touch a
    /// real `$HOME`.
    pub fn with_sessions_root(mut self, root: PathBuf) -> Self {
        self.sessions_root = Some(root);
        self
    }
    fn resolved_sessions_root(&self) -> PathBuf {
        self.sessions_root.clone().unwrap_or_else(|| {
            let home = std::env::var("CODEX_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".codex")
                });
            home.join("sessions")
        })
    }
    pub fn capabilities_static() -> Capabilities {
        Capabilities {
            can_trigger_compaction: true,
            can_advise_mid_turn: true,
            can_inject_at_session_start: true,
            can_observe_compaction: true,
            reports_token_counts: true,
            headless_resume: true,
            seed_modes: vec![SeedMode::InitialPrompt, SeedMode::ForkWithHistory],
        }
    }
    async fn limits(&self) -> AdapterResult<(Value, Value)> {
        self.transport.call("initialize",json!({"clientInfo":{"name":"usagewindow","title":"usagewindow","version":"0.1.0"}})).await?;
        let account = self
            .transport
            .call("account/read", json!({"refreshToken":false}))
            .await?;
        if account.get("result").is_none() {
            return Err(AdapterError::Other("missing account".into()));
        }
        let response = self
            .transport
            .call("account/rateLimits/read", json!({}))
            .await?;
        let result = response
            .pointer("/result")
            .cloned()
            .ok_or_else(|| AdapterError::Other("missing rateLimits".into()))?;
        Ok((account, result))
    }
    fn rate_limits(result: &Value) -> AdapterResult<&Value> {
        result
            .pointer("/rateLimitsByLimitId/codex")
            .or_else(|| result.get("rateLimits"))
            .ok_or_else(|| AdapterError::Other("missing rateLimits".into()))
    }
}
#[async_trait]
impl HarnessAdapter for CodexAdapter {
    fn provider(&self) -> Provider {
        Provider::Codex
    }
    fn capabilities(&self) -> Capabilities {
        Self::capabilities_static()
    }
    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        let (account_response, result) = self.limits().await?;
        let account = account_from_response(&account_response);
        let limits = Self::rate_limits(&result)?;
        let limited = limits
            .get("rateLimitReachedType")
            .is_some_and(|v| !v.is_null())
            || result.get("ordinaryUsageAllowed").and_then(Value::as_bool) == Some(false);
        let mut windows = HashMap::new();
        for name in ["primary", "secondary"] {
            if let Some(w) = limits.get(name) {
                let mins = w
                    .get("windowDurationMins")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| AdapterError::Other("missing window duration".into()))?
                    as u32;
                let pct = w
                    .get("usedPercent")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| AdapterError::Other("missing usage percent".into()))?
                    as f32;
                let reset = w
                    .get("resetsAt")
                    .and_then(Value::as_i64)
                    .and_then(|s| Utc.timestamp_opt(s, 0).single());
                windows.insert(
                    WindowKey {
                        provider: Provider::Codex,
                        kind: WindowKind::Rolling { minutes: mins },
                    },
                    UsageWindowState::new(pct, limited, true, reset, None),
                );
            }
        }
        if windows.is_empty() {
            return Err(AdapterError::Other("missing rate-limit windows".into()));
        }
        let at = (self.now)();
        Ok(UsageSample {
            at,
            fetched_at: Some(at),
            source: UsageSource::ProviderReported,
            provider: Provider::Codex,
            account,
            windows,
            credits: None,
        })
    }
    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        let (_, result) = self.limits().await?;
        let limits = Self::rate_limits(&result)?;
        if limits
            .get("rateLimitReachedType")
            .is_some_and(|v| !v.is_null())
            || result.get("ordinaryUsageAllowed").and_then(Value::as_bool) == Some(false)
        {
            let mins = limits
                .pointer("/primary/windowDurationMins")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    AdapterError::Other(
                        "rate limit reached but window duration is missing from account/rateLimits/read".into(),
                    )
                })? as u32;
            return Ok(Some(StopReason::UsageLimit {
                window: WindowKey {
                    provider: Provider::Codex,
                    kind: WindowKind::Rolling { minutes: mins },
                },
            }));
        }
        Ok(None)
    }
    async fn detect_stop_with_usage(
        &self,
        session_id: &SessionId,
        sample: Option<&UsageSample>,
    ) -> AdapterResult<Option<StopReason>> {
        let Some(sample) = sample else {
            return self.detect_stop(session_id).await;
        };
        Ok(sample
            .windows
            .iter()
            .find(|(_, window)| window.exceeded)
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
        self.transport
            .call(
                "initialize",
                json!({"clientInfo":{"name":"usagewindow","title":"usagewindow","version":"0.1.0"}}),
            )
            .await?;
        let response = self
            .transport
            .call(
                "turn/start",
                json!({
                    "threadId": session.0,
                    "input": [{"type": "text", "text": text}],
                }),
            )
            .await?;
        if let Some(error) = response.get("error") {
            return Err(AdapterError::Other(error.to_string()));
        }
        Ok(DeliveryOutcome::Delivered)
    }
    async fn compact(
        &self,
        session: &SessionSummary,
        _: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome> {
        self.transport
            .call(
                "initialize",
                json!({"clientInfo":{"name":"usagewindow","title":"usagewindow","version":"0.1.0"}}),
            )
            .await?;
        let response = self
            .transport
            .call("thread/compact/start", json!({"threadId": session.id.0}))
            .await?;
        if let Some(error) = response.get("error") {
            return Err(AdapterError::Other(error.to_string()));
        }
        Ok(DeliveryOutcome::Delivered)
    }
    async fn resume_session(
        &self,
        session: &SessionSummary,
        message: Option<&str>,
    ) -> AdapterResult<()> {
        let message = message.unwrap_or("Continue from the saved state.");
        let resume = self.spawner.run(ProcessSpec {
            program: "codex".into(),
            args: vec![
                "exec".into(),
                "resume".into(),
                session.id.0.clone(),
                message.into(),
            ],
            cwd: session.cwd.clone(),
        });
        match resume.await {
            Ok(_) => Ok(()),
            Err(error) if error.to_string().contains("active writer") => self
                .spawner
                .run(ProcessSpec {
                    program: "codex".into(),
                    args: vec![
                        "queue".into(),
                        "--thread".into(),
                        session.id.0.clone(),
                        "--message".into(),
                        message.into(),
                    ],
                    cwd: session.cwd.clone(),
                })
                .await
                .map(|_| ()),
            Err(error) => Err(error),
        }
    }
    async fn seed_new_session(
        &self,
        mode: SeedMode,
        seed: &SeedContext,
    ) -> AdapterResult<SessionId> {
        let args = match mode {
            SeedMode::InitialPrompt => {
                vec!["exec".into(), "--json".into(), seed.summary.clone()]
            }
            SeedMode::ForkWithHistory => {
                let id = seed
                    .from_session
                    .as_ref()
                    .ok_or_else(|| AdapterError::Other("fork requires a source session".into()))?;
                vec![
                    "exec".into(),
                    "fork".into(),
                    "--json".into(),
                    id.0.clone(),
                    seed.summary.clone(),
                ]
            }
        };
        let out = self
            .spawner
            .run(ProcessSpec {
                program: "codex".into(),
                args,
                cwd: seed.cwd.clone(),
            })
            .await?;
        out.session_id.map(SessionId).ok_or_else(|| {
            AdapterError::Other("Codex did not report the new session id in JSON output".into())
        })
    }

    async fn export_transcript(
        &self,
        _: &SessionSummary,
    ) -> uw_core::adapter::AdapterResult<String> {
        // Codex rollout JSONL is not a stable transcript-export interface; this is a
        // documented research gap, so failing closed is safer than exporting wrong data.
        Err(uw_core::adapter::AdapterError::Unsupported)
    }

    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        let root = self.resolved_sessions_root();
        let mut cache = self
            .discovery_cache
            .lock()
            .map_err(|_| AdapterError::Other("discovery cache poisoned".into()))?;
        let mut found = Vec::new();
        for path in rollout_files(&root) {
            let metadata = match std::fs::metadata(&path) {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            let fingerprint = (metadata.len(), metadata.modified().ok());
            if let Some(cached) = cache.get(&path)
                && cached.fingerprint == fingerprint
            {
                found.push(cached.session.clone());
                continue;
            }
            let Some(session) = scan_rollout(&path) else {
                cache.remove(&path);
                continue;
            };
            let returned = cache
                .get(&path)
                .map(|old| {
                    let old_count = old.session.token_usage.len();
                    let mut delta = session.clone();
                    delta.token_usage =
                        session.token_usage[old_count.min(session.token_usage.len())..].to_vec();
                    delta
                })
                .unwrap_or_else(|| session.clone());
            cache.insert(
                path,
                CachedRollout {
                    fingerprint,
                    session,
                },
            );
            found.push(returned);
        }
        Ok(found)
    }

    async fn session_preview(&self, session: &SessionSummary) -> AdapterResult<Vec<TurnPreview>> {
        let known_path = session.state_path.clone();
        let id = session.id.0.clone();
        let root = self.resolved_sessions_root();
        tokio::task::spawn_blocking(move || {
            // A session tracked via hooks (not discovery) never had `state_path` set —
            // find its rollout file by id so the preview still works for it.
            let path = known_path.map(PathBuf::from).or_else(|| {
                rollout_files(&root).into_iter().find(|p| {
                    p.file_stem()
                        .and_then(|s| s.to_str())
                        .is_some_and(|name| name.ends_with(&id))
                })
            })?;
            rollout_turn_preview(&path)
        })
        .await
        .map_err(|e| AdapterError::Other(e.to_string()))?
        .ok_or(AdapterError::Unsupported)
    }
}

fn account_from_response(value: &Value) -> Option<AccountId> {
    ["email", "id", "accountId"]
        .into_iter()
        .filter_map(|field| value.pointer(&format!("/result/{field}"))?.as_str())
        .find(|value| !value.is_empty())
        .map(|value| AccountId(value.to_owned()))
}

/// Every `*.jsonl` file under `root`, recursively (rollout files live under
/// `sessions/YYYY/MM/DD/`). Best-effort: an unreadable directory is skipped, not fatal.
fn rollout_files(root: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
                out.push(path);
            }
        }
    }
    out
}

/// Streams an entire rollout file once, pulling together everything usagewindow can
/// honestly know about the session from Codex's own on-disk record:
/// - `session_meta` (first line): id, cwd, session start time.
/// - `turn_context.payload.model`: the model actually in use (last one seen wins, in
///   case the session ever switched models).
/// - `event_msg{type:"task_started"}.payload.model_context_window`: the context window
///   size Codex itself is using.
/// - `token_usage_record.payload.usage.total_tokens`: the most recent single response's
///   total tokens, used as a proxy for current context occupancy (the `thread_token_usage`
///   field on the same record is a lifetime sum across every turn, not "how full is the
///   context right now" — using it here would badly overstate usage).
/// - The timestamp on the last record read, as `last_seen`.
///
/// A file that isn't valid JSONL or doesn't start with `session_meta` is skipped, not an
/// error — most files under `sessions/` are unrelated or historical. The daemon uses this
/// scan both to discover new sessions and to refresh token history for known sessions.
fn scan_rollout(path: &std::path::Path) -> Option<DiscoveredSession> {
    #[cfg(test)]
    {
        *ROLLOUT_SCAN_COUNTS
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .or_default() += 1;
    }

    let file = std::fs::File::open(path).ok()?;
    let mut lines = std::io::BufReader::new(file).lines();

    let first: Value = serde_json::from_str(&lines.next()?.ok()?).ok()?;
    if first.get("type").and_then(Value::as_str) != Some("session_meta") {
        return None;
    }
    let payload = first.get("payload")?;
    let id = payload.get("session_id").and_then(Value::as_str)?;
    uuid::Uuid::parse_str(id).ok()?;
    let cwd = payload
        .get("cwd")
        .and_then(Value::as_str)
        .unwrap_or(".")
        .to_owned();
    let first_seen = first
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        .map(|ts| ts.with_timezone(&Utc));

    let mut model = None;
    let mut context_window_size = None;
    let mut last_known_token_count = None;
    let mut last_seen = first_seen;
    let mut token_usage = Vec::new();
    let mut usage_model = model.clone();
    for line in lines.map_while(Result::ok) {
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if let Some(ts) = record
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        {
            last_seen = Some(ts.with_timezone(&Utc));
        }
        match record.get("type").and_then(Value::as_str) {
            Some("turn_context") => {
                if let Some(m) = record.pointer("/payload/model").and_then(Value::as_str) {
                    model = Some(ModelId(m.to_owned()));
                    usage_model = model.clone();
                }
            }
            Some("event_msg")
                if record.pointer("/payload/type").and_then(Value::as_str)
                    == Some("task_started") =>
            {
                if let Some(w) = record
                    .pointer("/payload/model_context_window")
                    .and_then(Value::as_u64)
                {
                    context_window_size = Some(w);
                }
            }
            Some("token_usage_record") => {
                let Some(timestamp) = record
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
                    .map(|ts| ts.with_timezone(&Utc))
                else {
                    continue;
                };
                let Some(usage) = record.pointer("/payload/usage") else {
                    continue;
                };
                let Some(total_tokens) = usage.get("total_tokens").and_then(Value::as_u64) else {
                    continue;
                };
                let token_usage_record = TokenUsageRecord {
                    at: timestamp,
                    model: usage_model.clone(),
                    input_tokens: usage
                        .get("input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                    cached_input_tokens: usage
                        .get("cached_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                    cache_write_input_tokens: usage
                        .get("cache_write_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                    output_tokens: usage
                        .get("output_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                    reasoning_output_tokens: usage
                        .get("reasoning_output_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                    total_tokens,
                };
                last_known_token_count = Some(total_tokens);
                token_usage.push(token_usage_record);
            }
            _ => {}
        }
    }
    Some(DiscoveredSession {
        id: SessionId(id.to_owned()),
        cwd,
        model,
        context_window_size,
        last_known_token_count,
        first_seen,
        last_seen,
        state_path: Some(path.to_string_lossy().into_owned()),
        token_usage,
    })
}

/// The role- and text-bearing turns from an `item_completed` event: `UserMessage` and
/// `AgentMessage` items. `Reasoning`/`CommandExecution` items are internal, not part of
/// the conversation a user would want reminded of.
fn turn_from_item_completed(record: &Value) -> Option<TurnPreview> {
    let item = record.pointer("/payload/item")?;
    let role = match item.get("type").and_then(Value::as_str)? {
        "UserMessage" => TurnRole::User,
        "AgentMessage" => TurnRole::Assistant,
        _ => return None,
    };
    let text = item
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    if text.is_empty() {
        return None;
    }
    const MAX_CHARS: usize = 2000;
    let text = if text.len() > MAX_CHARS {
        format!("{}…", &text[..MAX_CHARS])
    } else {
        text
    };
    Some(TurnPreview { role, text })
}

/// The first two and last two user/assistant turns in a rollout file, read in one
/// forward streaming pass (no need to hold the whole file in memory — only a
/// 2-entry front buffer and a 2-entry rolling tail buffer).
fn rollout_turn_preview(path: &std::path::Path) -> Option<Vec<TurnPreview>> {
    let file = std::fs::File::open(path).ok()?;
    let mut first_two = Vec::with_capacity(2);
    let mut last_two: std::collections::VecDeque<TurnPreview> =
        std::collections::VecDeque::with_capacity(2);
    for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if record.get("type").and_then(Value::as_str) != Some("event_msg")
            || record.pointer("/payload/type").and_then(Value::as_str) != Some("item_completed")
        {
            continue;
        }
        let Some(turn) = turn_from_item_completed(&record) else {
            continue;
        };
        if first_two.len() < 2 {
            first_two.push(turn.clone());
        }
        if last_two.len() == 2 {
            last_two.pop_front();
        }
        last_two.push_back(turn);
    }
    if first_two.is_empty() {
        return None;
    }
    // Avoid repeating the same turns in both halves for a short session.
    let mut preview = first_two.clone();
    for turn in last_two {
        if !first_two.contains(&turn) {
            preview.push(turn);
        }
    }
    Some(preview)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;
    use uw_core::adapter::{Capabilities, HarnessAdapter};

    fn write_incremental_rollout(root: &std::path::Path, id: &str) -> std::path::PathBuf {
        let day_dir = root.join("2026/09/17");
        std::fs::create_dir_all(&day_dir).unwrap();
        let path = day_dir.join(format!("rollout-2026-09-17T17-32-31-{id}.jsonl"));
        std::fs::write(
            &path,
            format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","type":"session_meta","payload":{{"session_id":"{id}","cwd":"/tmp/project"}}}}
{{"timestamp":"2026-09-17T23:32:33Z","type":"turn_context","payload":{{"model":"gpt-5.6-luna"}}}}
{{"timestamp":"2026-09-17T23:32:34Z","type":"token_usage_record","payload":{{"usage":{{"input_tokens":10,"total_tokens":11}}}}}}
"#
            ),
        )
        .unwrap();
        path
    }

    /// A fake `codex app-server`: appends to a marker file once per process
    /// start (not per request), then answers each JSON-RPC request line with
    /// a canned response for the three methods `limits()` sends, echoing the
    /// request id it was asked for.
    const FAKE_APP_SERVER_SCRIPT: &str = r#"#!/bin/sh
echo start >> "$1"
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"account/rateLimits/read"'*)
      printf '{"id":%s,"result":{"ordinaryUsageAllowed":true,"rateLimits":{"primary":{"usedPercent":1,"windowDurationMins":10080,"resetsAt":1800000000},"secondary":{"usedPercent":2,"windowDurationMins":300,"resetsAt":1800000100}}}}\n' "$id"
      ;;
    *'"method":"account/read"'*)
      printf '{"id":%s,"result":{"id":"acct"}}\n' "$id"
      ;;
    *)
      printf '{"id":%s,"result":{}}\n' "$id"
      ;;
  esac
done
"#;

    struct FakeAppServerScript {
        script_path: std::path::PathBuf,
        marker_path: std::path::PathBuf,
    }

    impl FakeAppServerScript {
        fn write() -> Self {
            let dir = std::env::temp_dir();
            let unique = uuid::Uuid::new_v4();
            let script_path = dir.join(format!("uw-codex-fake-app-server-{unique}.sh"));
            let marker_path = dir.join(format!("uw-codex-fake-app-server-{unique}.marker"));
            std::fs::write(&script_path, FAKE_APP_SERVER_SCRIPT).unwrap();
            let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
            std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
            std::fs::set_permissions(&script_path, perms).unwrap();
            Self {
                script_path,
                marker_path,
            }
        }

        fn transport(&self) -> CodexAppServerTransport {
            CodexAppServerTransport::spawn_with(
                self.script_path.to_string_lossy().into_owned(),
                vec![self.marker_path.to_string_lossy().into_owned()],
            )
        }

        fn process_start_count(&self) -> usize {
            std::fs::read_to_string(&self.marker_path)
                .map(|s| s.lines().count())
                .unwrap_or(0)
        }
    }

    impl Drop for FakeAppServerScript {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.script_path);
            let _ = std::fs::remove_file(&self.marker_path);
        }
    }

    #[tokio::test]
    async fn app_server_transport_reuses_one_process_and_reaps_child_on_drop() {
        let fake = FakeAppServerScript::write();
        let transport = Arc::new(fake.transport());
        let adapter = CodexAdapter::new(transport.clone() as Arc<dyn AppServerTransport>);

        // fetch_usage alone drives three JSON-RPC calls (initialize,
        // account/read, account/rateLimits/read) through `limits()`.
        let sample = adapter.fetch_usage(None).await.unwrap();
        assert_eq!(sample.windows.len(), 2);

        // A second, independent high-level call must still reuse the same
        // app-server session rather than spawning another process.
        adapter.detect_stop(&SessionId("s".into())).await.unwrap();

        assert_eq!(
            fake.process_start_count(),
            1,
            "app-server process must be spawned once and reused across all JSON-RPC calls"
        );

        let pid = transport
            .session
            .lock()
            .await
            .as_ref()
            .and_then(|s| s._child.id())
            .expect("an active app-server session should exist after calls");

        drop(adapter);
        drop(transport);

        let proc_path = format!("/proc/{pid}");
        let mut reaped = false;
        for _ in 0..100 {
            if !std::path::Path::new(&proc_path).exists() {
                reaped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            reaped,
            "app-server child process should be terminated and reaped once the transport is dropped"
        );
    }

    #[test]
    fn capabilities_match_codex_contract() {
        assert_eq!(
            CodexAdapter::capabilities_static(),
            Capabilities {
                can_trigger_compaction: true,
                can_advise_mid_turn: true,
                can_inject_at_session_start: true,
                can_observe_compaction: true,
                reports_token_counts: true,
                headless_resume: true,
                seed_modes: vec![SeedMode::InitialPrompt, SeedMode::ForkWithHistory]
            }
        );
    }
    struct Rpc;
    #[async_trait::async_trait]
    impl AppServerTransport for Rpc {
        async fn call(
            &self,
            method: &str,
            _params: serde_json::Value,
        ) -> AdapterResult<serde_json::Value> {
            Ok(match method {
                "account/read" => json!({"result":{"id":"acct","email":"codex@example.com"}}),
                "account/rateLimits/read" => {
                    json!({"result":{"ordinaryUsageAllowed":true,"rateLimits":{"primary":{"usedPercent":1,"windowDurationMins":10080,"resetsAt":1800000000},"secondary":{"usedPercent":2,"windowDurationMins":300,"resetsAt":1800000100}}}})
                }
                _ => json!({"result":{}}),
            })
        }
    }
    #[tokio::test]
    async fn rate_limits_key_by_duration_not_position() {
        let sample = CodexAdapter::new(Arc::new(Rpc))
            .fetch_usage(None)
            .await
            .unwrap();
        assert_eq!(sample.account, Some(AccountId("codex@example.com".into())));
        assert_eq!(
            sample.windows[&WindowKey {
                provider: Provider::Codex,
                kind: WindowKind::Rolling { minutes: 300 }
            }]
                .pct,
            2.0
        );
        assert_eq!(
            sample.windows[&WindowKey {
                provider: Provider::Codex,
                kind: WindowKind::Rolling { minutes: 10080 }
            }]
                .pct,
            1.0
        );
    }
    #[tokio::test]
    async fn stop_detection_maps_limit_signal() {
        let adapter = CodexAdapter::new(Arc::new(RpcWithLimit));
        assert!(matches!(
            adapter.detect_stop(&SessionId("s".into())).await.unwrap(),
            Some(StopReason::UsageLimit { .. })
        ));
    }

    #[tokio::test]
    async fn stop_detection_does_not_invent_a_window_duration() {
        struct LimitWithoutWindow;
        #[async_trait::async_trait]
        impl AppServerTransport for LimitWithoutWindow {
            async fn call(
                &self,
                method: &str,
                _: serde_json::Value,
            ) -> AdapterResult<serde_json::Value> {
                if method == "account/rateLimits/read" {
                    Ok(json!({
                        "result": {
                            "ordinaryUsageAllowed": false,
                            "rateLimits": {"rateLimitReachedType": "primary"}
                        }
                    }))
                } else {
                    Ok(json!({"result": {}}))
                }
            }
        }

        let adapter = CodexAdapter::new(Arc::new(LimitWithoutWindow));
        assert!(adapter.detect_stop(&SessionId("s".into())).await.is_err());
    }

    struct RpcWithLimit;
    #[async_trait::async_trait]
    impl AppServerTransport for RpcWithLimit {
        async fn call(
            &self,
            method: &str,
            _params: serde_json::Value,
        ) -> AdapterResult<serde_json::Value> {
            if method == "account/rateLimits/read" {
                Ok(
                    json!({"result":{"ordinaryUsageAllowed":false,"rateLimits":{"rateLimitReachedType":"primary","primary":{"windowDurationMins":300,"usedPercent":100}}}}),
                )
            } else {
                Ok(json!({"result":{}}))
            }
        }
    }
    #[tokio::test]
    async fn codex_compact_uses_native_app_server_method() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let a = CodexAdapter::new(Arc::new(RecordingRpc(calls.clone())));
        let id = SessionId("s".into());
        assert_eq!(
            a.compact(
                &SessionSummary {
                    id: id.clone(),
                    harness: Provider::Codex,
                    model: None,
                    account: None,
                    first_seen: Utc::now(),
                    last_seen: Utc::now(),
                    cwd: "/tmp".into(),
                    state_path: None,
                    context_window_size: None,
                    last_known_token_count: None,
                    launch_mode: LaunchMode::Headless,
                    pid: None,
                    stopped_reason: None,
                    resume_marker: None,
                    superseded_by: None,
                    reseeded_from: None,
                },
                &test_request(),
            )
            .await
            .unwrap(),
            DeliveryOutcome::Delivered
        );
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            [
                (
                    "initialize".into(),
                    json!({"clientInfo":{"name":"usagewindow","title":"usagewindow","version":"0.1.0"}})
                ),
                ("thread/compact/start".into(), json!({"threadId":"s"}))
            ]
        );
    }
    #[tokio::test]
    async fn codex_advise_sends_a_turn_with_the_given_text() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let a = CodexAdapter::new(Arc::new(RecordingRpc(calls.clone())));
        let id = SessionId("s".into());
        assert_eq!(
            a.advise(
                &id,
                "[[uw-keepalive]] no action needed, acknowledge briefly"
            )
            .await
            .unwrap(),
            DeliveryOutcome::Delivered
        );
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            [
                (
                    "initialize".into(),
                    json!({"clientInfo":{"name":"usagewindow","title":"usagewindow","version":"0.1.0"}})
                ),
                (
                    "turn/start".into(),
                    json!({
                        "threadId":"s",
                        "input": [{"type": "text", "text": "[[uw-keepalive]] no action needed, acknowledge briefly"}],
                    })
                )
            ]
        );
    }
    struct RecordingRpc(Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>);
    #[async_trait::async_trait]
    impl AppServerTransport for RecordingRpc {
        async fn call(
            &self,
            method: &str,
            params: serde_json::Value,
        ) -> AdapterResult<serde_json::Value> {
            self.0.lock().unwrap().push((method.into(), params));
            Ok(json!({"result":{}}))
        }
    }
    fn test_request() -> CompactionRequest {
        CompactionRequest {
            id: uuid::Uuid::nil(),
            session_id: SessionId("s".into()),
            kind: CompactionKind::AskNearLimit,
            prompt: String::new(),
            reason: String::new(),
            status: CompactionStatus::Pending,
            created_at: chrono::Utc::now(),
        }
    }
    struct Recorder(Arc<std::sync::Mutex<Option<ProcessSpec>>>);
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

    struct BusyThenQueue {
        specs: Arc<std::sync::Mutex<Vec<ProcessSpec>>>,
    }

    #[async_trait::async_trait]
    impl ProcessSpawner for BusyThenQueue {
        async fn run(&self, spec: ProcessSpec) -> AdapterResult<crate::process::ProcessOutput> {
            let mut specs = self.specs.lock().unwrap();
            specs.push(spec);
            if specs.len() == 1 {
                Err(AdapterError::Other(
                    "thread-store conflict: active writer".into(),
                ))
            } else {
                Ok(crate::process::ProcessOutput::default())
            }
        }
    }

    #[tokio::test]
    async fn resume_queues_into_an_existing_codex_writer() {
        let specs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let adapter = CodexAdapter::new(Arc::new(Rpc)).with_spawner(Arc::new(BusyThenQueue {
            specs: specs.clone(),
        }));
        adapter
            .resume_session(&session_summary("thread"), Some("continue"))
            .await
            .unwrap();
        let specs = specs.lock().unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].program, "codex");
        assert_eq!(&specs[0].args[..3], &["exec", "resume", "thread"]);
        assert_eq!(
            specs[1].args,
            vec!["queue", "--thread", "thread", "--message", "continue"]
        );
    }

    #[tokio::test]
    async fn resume_and_both_seed_modes_build_expected_commands() {
        let record = Arc::new(std::sync::Mutex::new(None));
        let a = CodexAdapter::new(Arc::new(Rpc)).with_spawner(Arc::new(Recorder(record.clone())));
        let session = SessionSummary {
            id: SessionId("uuid".into()),
            harness: Provider::Codex,
            model: None,
            account: None,
            first_seen: chrono::Utc::now(),
            last_seen: chrono::Utc::now(),
            cwd: "/work".into(),
            state_path: None,
            context_window_size: None,
            last_known_token_count: None,
            launch_mode: LaunchMode::Headless,
            pid: None,
            stopped_reason: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        };
        a.resume_session(&session, None).await.unwrap();
        assert_eq!(
            *record.lock().unwrap(),
            Some(ProcessSpec {
                program: "codex".into(),
                args: vec![
                    "exec".into(),
                    "resume".into(),
                    "uuid".into(),
                    "Continue from the saved state.".into()
                ],
                cwd: "/work".into()
            })
        );
        a.resume_session(&session, Some("custom message"))
            .await
            .unwrap();
        assert_eq!(
            *record.lock().unwrap(),
            Some(ProcessSpec {
                program: "codex".into(),
                args: vec![
                    "exec".into(),
                    "resume".into(),
                    "uuid".into(),
                    "custom message".into()
                ],
                cwd: "/work".into()
            })
        );
        let seed = SeedContext {
            from_session: Some(SessionId("old".into())),
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
                program: "codex".into(),
                args: vec!["exec".into(), "--json".into(), "sum".into()],
                cwd: "/seed".into()
            })
        );
        a.seed_new_session(SeedMode::ForkWithHistory, &seed)
            .await
            .unwrap();
        assert_eq!(
            *record.lock().unwrap(),
            Some(ProcessSpec {
                program: "codex".into(),
                args: vec![
                    "exec".into(),
                    "fork".into(),
                    "--json".into(),
                    "old".into(),
                    "sum".into()
                ],
                cwd: "/seed".into()
            })
        );
    }

    #[tokio::test]
    async fn discover_sessions_reads_rollout_headers_and_skips_junk() {
        let script = FakeAppServerScript::write();
        let root = std::env::temp_dir().join(format!("uw-codex-sessions-{}", uuid::Uuid::new_v4()));
        let day_dir = root.join("2026/09/17");
        std::fs::create_dir_all(&day_dir).unwrap();

        let id = "01a0b1b6-f024-7b73-a5fc-a6ea05f0a57f";
        let rollout_path = day_dir.join(format!("rollout-2026-09-17T17-32-31-{id}.jsonl"));
        std::fs::write(
            &rollout_path,
            format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","type":"session_meta","payload":{{"session_id":"{id}","cwd":"/home/v0id/Documents/repos/usagewindow"}}}}
{{"timestamp":"2026-09-17T23:32:32.5Z","type":"event_msg","payload":{{"type":"task_started","model_context_window":258400}}}}
{{"timestamp":"2026-09-17T23:32:33Z","type":"turn_context","payload":{{"model":"gpt-5.6-luna"}}}}
{{"timestamp":"2026-09-17T23:32:34Z","type":"token_usage_record","payload":{{"usage":{{"input_tokens":12000,"cached_input_tokens":9000,"cache_write_input_tokens":500,"output_tokens":1862,"reasoning_output_tokens":321,"total_tokens":14362}},"thread_token_usage":{{"total_tokens":4304079}}}}}}
{{"timestamp":"2026-09-17T23:32:34.5Z","type":"token_usage_record","payload":{{"usage":{{"input_tokens":999}}}}}}
{{"type":"token_usage_record","payload":{{"usage":{{"total_tokens":999}}}}}}
{{"timestamp":"2026-09-17T23:32:35Z","type":"event_msg","payload":{{"type":"item_completed","item":{{"type":"UserMessage","content":[{{"type":"text","text":"first question"}}]}}}}}}
{{"timestamp":"2026-09-17T23:32:36Z","type":"event_msg","payload":{{"type":"item_completed","item":{{"type":"AgentMessage","content":[{{"type":"Text","text":"first answer"}}]}}}}}}
{{"timestamp":"2026-09-18T00:23:18Z","type":"event_msg","payload":{{"type":"item_completed","item":{{"type":"UserMessage","content":[{{"type":"text","text":"last question"}}]}}}}}}
{{"timestamp":"2026-09-18T00:23:19Z","type":"event_msg","payload":{{"type":"item_completed","item":{{"type":"AgentMessage","content":[{{"type":"Text","text":"last answer"}}]}}}}}}
"#
            ),
        )
        .unwrap();
        // Not a session_meta first line — must be skipped, not mistaken for a session.
        std::fs::write(
            day_dir.join("rollout-not-a-session.jsonl"),
            r#"{"type":"other","payload":{}}"#,
        )
        .unwrap();
        // Malformed JSON — must be skipped without failing the whole scan.
        std::fs::write(day_dir.join("rollout-broken.jsonl"), "not json\n").unwrap();

        let a = CodexAdapter::new(Arc::new(script.transport())).with_sessions_root(root.clone());
        let found = a.discover_sessions().await.unwrap();

        assert_eq!(found.len(), 1);
        let found = &found[0];
        assert_eq!(found.id, SessionId(id.into()));
        assert_eq!(found.cwd, "/home/v0id/Documents/repos/usagewindow");
        assert_eq!(found.model, Some(ModelId("gpt-5.6-luna".into())));
        assert_eq!(found.context_window_size, Some(258400));
        // The last response's total, not thread_token_usage's lifetime sum.
        assert_eq!(found.last_known_token_count, Some(14362));
        assert_eq!(
            found.token_usage,
            vec![TokenUsageRecord {
                at: DateTime::parse_from_rfc3339("2026-09-17T23:32:34Z")
                    .unwrap()
                    .with_timezone(&Utc),
                model: Some(ModelId("gpt-5.6-luna".into())),
                input_tokens: 12000,
                cached_input_tokens: 9000,
                cache_write_input_tokens: 500,
                output_tokens: 1862,
                reasoning_output_tokens: 321,
                total_tokens: 14362,
            }]
        );
        assert_eq!(
            found.first_seen.unwrap().to_rfc3339(),
            "2026-09-17T23:32:32+00:00"
        );
        assert_eq!(
            found.last_seen.unwrap().to_rfc3339(),
            "2026-09-18T00:23:19+00:00"
        );
        assert_eq!(found.state_path.as_deref(), rollout_path.to_str());

        let preview = a
            .session_preview(&SessionSummary {
                state_path: found.state_path.clone(),
                ..session_summary(id)
            })
            .await
            .unwrap();
        assert_eq!(
            preview.iter().map(|t| t.text.as_str()).collect::<Vec<_>>(),
            vec![
                "first question",
                "first answer",
                "last question",
                "last answer"
            ]
        );

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn discover_sessions_does_not_reparse_an_unchanged_rollout() {
        let script = FakeAppServerScript::write();
        let root = std::env::temp_dir().join(format!("uw-codex-sessions-{}", uuid::Uuid::new_v4()));
        let id = "01a0b1b6-f024-7b73-a5fc-a6ea05f0a580";
        let rollout_path = write_incremental_rollout(&root, id);
        let adapter =
            CodexAdapter::new(Arc::new(script.transport())).with_sessions_root(root.clone());

        adapter.discover_sessions().await.unwrap();
        let scans_after_first_discovery = rollout_scan_count(&rollout_path);
        adapter.discover_sessions().await.unwrap();
        let scans_after_second_discovery = rollout_scan_count(&rollout_path);
        std::fs::remove_dir_all(&root).ok();

        assert_eq!(scans_after_first_discovery, 1);
        assert_eq!(
            scans_after_second_discovery, scans_after_first_discovery,
            "an unchanged rollout must be served from discovery state without reparsing"
        );
    }

    #[tokio::test]
    async fn discover_sessions_parses_only_content_appended_since_the_previous_scan() {
        use std::io::Write as _;

        let script = FakeAppServerScript::write();
        let root = std::env::temp_dir().join(format!("uw-codex-sessions-{}", uuid::Uuid::new_v4()));
        let id = "01a0b1b6-f024-7b73-a5fc-a6ea05f0a581";
        let rollout_path = write_incremental_rollout(&root, id);
        let adapter =
            CodexAdapter::new(Arc::new(script.transport())).with_sessions_root(root.clone());

        let initial = adapter.discover_sessions().await.unwrap();
        assert_eq!(initial[0].token_usage.len(), 1);
        writeln!(
            std::fs::OpenOptions::new()
                .append(true)
                .open(&rollout_path)
                .unwrap(),
            r#"{{"timestamp":"2026-09-17T23:33:00Z","type":"token_usage_record","payload":{{"usage":{{"input_tokens":20,"total_tokens":22}}}}}}"#
        )
        .unwrap();

        let appended = adapter.discover_sessions().await.unwrap();
        std::fs::remove_dir_all(&root).ok();

        assert_eq!(appended.len(), 1);
        assert_eq!(
            appended[0].token_usage,
            vec![TokenUsageRecord {
                at: DateTime::parse_from_rfc3339("2026-09-17T23:33:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                model: Some(ModelId("gpt-5.6-luna".into())),
                input_tokens: 20,
                cached_input_tokens: 0,
                cache_write_input_tokens: 0,
                output_tokens: 0,
                reasoning_output_tokens: 0,
                total_tokens: 22,
            }],
            "a growing rollout must emit only records beyond the saved byte offset"
        );
        assert_eq!(appended[0].last_known_token_count, Some(22));
    }

    #[tokio::test]
    async fn session_preview_finds_the_rollout_file_by_id_when_state_path_is_unset() {
        // A session tracked via hooks (not discovery) never got `state_path` filled
        // in — the preview must still work by locating the file from the id alone.
        let script = FakeAppServerScript::write();
        let root = std::env::temp_dir().join(format!("uw-codex-sessions-{}", uuid::Uuid::new_v4()));
        let day_dir = root.join("2026/09/17");
        std::fs::create_dir_all(&day_dir).unwrap();
        let id = "01a0b1b6-f024-7b73-a5fc-a6ea05f0a57f";
        std::fs::write(
            day_dir.join(format!("rollout-2026-09-17T17-32-31-{id}.jsonl")),
            format!(
                r#"{{"timestamp":"2026-09-17T23:32:32Z","type":"session_meta","payload":{{"session_id":"{id}","cwd":"/tmp"}}}}
{{"timestamp":"2026-09-17T23:32:33Z","type":"event_msg","payload":{{"type":"item_completed","item":{{"type":"UserMessage","content":[{{"type":"text","text":"hi"}}]}}}}}}
"#
            ),
        )
        .unwrap();

        let a = CodexAdapter::new(Arc::new(script.transport())).with_sessions_root(root.clone());
        let preview = a.session_preview(&session_summary(id)).await.unwrap();
        std::fs::remove_dir_all(&root).ok();

        assert_eq!(preview.len(), 1);
        assert_eq!(preview[0].text, "hi");
    }

    fn session_summary(id: &str) -> SessionSummary {
        SessionSummary {
            id: SessionId(id.into()),
            harness: Provider::Codex,
            model: None,
            account: None,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            cwd: ".".into(),
            state_path: None,
            context_window_size: None,
            last_known_token_count: None,
            launch_mode: LaunchMode::Interactive,
            pid: None,
            stopped_reason: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        }
    }
}
