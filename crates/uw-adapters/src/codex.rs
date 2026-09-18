use crate::process::{ProcessSpawner, ProcessSpec};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, HarnessAdapter, SeedContext,
    SeedMode, StatusEvent,
};
use uw_core::model::*;

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
}
impl CodexAdapter {
    pub fn new(transport: Arc<dyn AppServerTransport>) -> Self {
        Self {
            transport,
            spawner: Arc::new(crate::process::TokioProcessSpawner),
            now: Arc::new(Utc::now),
            hook: None,
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
    pub fn capabilities_static() -> Capabilities {
        Capabilities {
            can_trigger_compaction: false,
            can_advise_mid_turn: false,
            can_inject_at_session_start: true,
            can_observe_compaction: true,
            reports_token_counts: false,
            headless_resume: true,
            seed_modes: vec![SeedMode::InitialPrompt, SeedMode::ForkWithHistory],
        }
    }
    async fn limits(&self) -> AdapterResult<Value> {
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
        response
            .pointer("/result")
            .cloned()
            .ok_or_else(|| AdapterError::Other("missing rateLimits".into()))
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
        let result = self.limits().await?;
        let limits = Self::rate_limits(&result)?;
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
                    UsageWindowState::new(pct, false, true, reset, None),
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
            account: None,
            windows,
            credits: None,
        })
    }
    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        let result = self.limits().await?;
        let limits = Self::rate_limits(&result)?;
        if limits
            .get("rateLimitReachedType")
            .is_some_and(|v| !v.is_null())
            || result.get("ordinaryUsageAllowed").and_then(Value::as_bool) == Some(false)
        {
            let mins = limits
                .pointer("/primary/windowDurationMins")
                .and_then(Value::as_u64)
                .unwrap_or(300) as u32;
            return Ok(Some(StopReason::UsageLimit {
                window: WindowKey {
                    provider: Provider::Codex,
                    kind: WindowKind::Rolling { minutes: mins },
                },
            }));
        }
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
    async fn resume_session(
        &self,
        session: &SessionSummary,
        message: Option<&str>,
    ) -> AdapterResult<()> {
        self.spawner
            .run(ProcessSpec {
                program: "codex".into(),
                args: vec![
                    "exec".into(),
                    "resume".into(),
                    session.id.0.clone(),
                    message.unwrap_or("Continue from the saved state.").into(),
                ],
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;
    use uw_core::adapter::{Capabilities, HarnessAdapter};

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
                can_trigger_compaction: false,
                can_advise_mid_turn: false,
                can_inject_at_session_start: true,
                can_observe_compaction: true,
                reports_token_counts: false,
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
                "account/read" => json!({"result":{"id":"acct"}}),
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
    async fn compact_and_advise_are_unsupported() {
        let a = CodexAdapter::new(Arc::new(Rpc));
        let id = SessionId("s".into());
        assert!(matches!(
            a.advise(&id, "x").await,
            Err(AdapterError::Unsupported)
        ));
        assert!(matches!(
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
            .await,
            Err(AdapterError::Unsupported)
        ));
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
}
