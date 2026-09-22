use async_trait::async_trait;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
#[cfg(test)]
use std::path::Path;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, HarnessAdapter, SeedContext,
    SeedMode, StatusEvent,
};
use uw_core::model::*;

const COMPACTION_STEP_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// The credential already issued by T3Code. Pairing/session creation is kept
/// outside usagewindow so this adapter cannot silently create another owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum T3CodeAuth {
    Bearer(String),
    Cookie(String),
}

#[async_trait]
pub trait T3CodeTransport: Send + Sync {
    async fn post_dispatch(&self, payload: Value) -> AdapterResult<Value>;
    async fn post_interrupt(&self, payload: Value) -> AdapterResult<Value>;
    /// Mutates T3Code's producer-owned external status overlay. `None` clears it.
    async fn post_thread_status(
        &self,
        thread_id: &str,
        status: Option<Value>,
    ) -> AdapterResult<Value>;
    /// `GET /api/orchestration/threads/:thread_id`. Returns the raw response
    /// body; callers read `thread.session.status`/`lastError` out of it.
    async fn get_thread(&self, thread_id: &str) -> AdapterResult<Value>;
}

/// HTTP transport for a running T3Code environment server.
pub struct T3CodeHttpTransport {
    client: reqwest::Client,
    base_url: String,
    auth: T3CodeAuth,
}

impl T3CodeHttpTransport {
    pub fn new(base_url: impl Into<String>, auth: T3CodeAuth) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            auth,
        }
    }
}

impl T3CodeHttpTransport {
    fn authed(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            T3CodeAuth::Bearer(token) => builder.bearer_auth(token),
            T3CodeAuth::Cookie(cookie) => builder.header(reqwest::header::COOKIE, cookie),
        }
    }

    async fn send(&self, request: reqwest::RequestBuilder, what: &str) -> AdapterResult<Value> {
        let response = request
            .send()
            .await
            .map_err(|error| AdapterError::Transient(error.to_string()))?;
        let status = response.status();
        let body = response
            .json::<Value>()
            .await
            .map_err(|error| AdapterError::Transient(error.to_string()))?;
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(AdapterError::Auth);
        }
        if !status.is_success() {
            return Err(AdapterError::Other(format!(
                "T3Code {what} failed ({status}): {body}"
            )));
        }
        Ok(body)
    }
}

#[async_trait]
impl T3CodeTransport for T3CodeHttpTransport {
    async fn post_dispatch(&self, payload: Value) -> AdapterResult<Value> {
        let request = self.authed(
            self.client
                .post(format!("{}/api/orchestration/dispatch", self.base_url))
                .json(&payload),
        );
        self.send(request, "dispatch").await
    }

    async fn post_interrupt(&self, payload: Value) -> AdapterResult<Value> {
        let request = self.authed(
            self.client
                .post(format!("{}/api/orchestration/dispatch", self.base_url))
                .json(&payload),
        );
        self.send(request, "interrupt").await
    }

    async fn post_thread_status(
        &self,
        thread_id: &str,
        status: Option<Value>,
    ) -> AdapterResult<Value> {
        let request = self.authed(
            self.client
                .post(format!("{}/api/orchestration/threads/{thread_id}/status", self.base_url))
                .json(&json!({ "status": status })),
        );
        self.send(request, "thread status").await
    }

    async fn get_thread(&self, thread_id: &str) -> AdapterResult<Value> {
        let request = self.authed(self.client.get(format!(
            "{}/api/orchestration/threads/{thread_id}",
            self.base_url
        )));
        self.send(request, "thread read").await
    }
}

/// T3Code's owner-preserving meta-harness adapter.
///
/// A T3Code thread id is the session id for this adapter. Resuming means
/// dispatching a native `thread.turn.start`; it never starts `claude` itself.
pub struct T3CodeAdapter {
    transport: Arc<dyn T3CodeTransport>,
    state_db: Option<PathBuf>,
}

impl T3CodeAdapter {
    pub fn new(transport: Arc<dyn T3CodeTransport>) -> Self {
        Self {
            transport,
            state_db: None,
        }
    }

    pub fn real(base_url: impl Into<String>, auth: T3CodeAuth) -> Self {
        let state_db = std::env::var_os("UW_T3CODE_STATE_DB")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".t3/userdata/state.sqlite")));
        Self {
            transport: Arc::new(T3CodeHttpTransport::new(base_url, auth)),
            state_db,
        }
    }

    #[cfg(test)]
    fn with_state_db(transport: Arc<dyn T3CodeTransport>, path: &Path) -> Self {
        Self {
            transport,
            state_db: Some(path.to_owned()),
        }
    }

    fn provider_cursor(provider: &Provider) -> Option<(&'static str, &'static str)> {
        match provider {
            Provider::ClaudeCode => Some(("claudeAgent", "resume")),
            Provider::Cursor => Some(("cursor", "sessionId")),
            Provider::Codex => Some(("codex", "threadId")),
            Provider::Other(_) | Provider::Gemini => None,
        }
    }

    fn native_owner(&self, session: &SessionSummary) -> AdapterResult<Option<SessionId>> {
        let Some(path) = &self.state_db else {
            return Ok(None);
        };
        let Some((provider_name, cursor_key)) = Self::provider_cursor(&session.harness) else {
            return Ok(None);
        };
        let connection = rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(|error| AdapterError::Other(format!("open T3Code state database: {error}")))?;
        let mut statement = connection
            .prepare(
                "SELECT thread_id, resume_cursor_json
                 FROM provider_session_runtime
                 WHERE provider_name = ?1",
            )
            .map_err(|error| AdapterError::Other(format!("read T3Code ownership table: {error}")))?;
        let rows = statement
            .query_map([provider_name], |row| {
                let thread_id: String = row.get(0)?;
                let cursor: String = row.get(1)?;
                Ok((thread_id, cursor))
            })
            .map_err(|error| AdapterError::Other(format!("scan T3Code ownership table: {error}")))?;
        for row in rows {
            let (thread_id, cursor) = row
                .map_err(|error| AdapterError::Other(format!("read T3Code ownership row: {error}")))?;
            let cursor: Value = serde_json::from_str(&cursor)
                .map_err(|error| AdapterError::Other(format!("parse T3Code resume cursor: {error}")))?;
            if cursor.get(cursor_key).and_then(Value::as_str) == Some(session.id.0.as_str()) {
                return Ok(Some(SessionId(thread_id)));
            }
        }
        Ok(None)
    }

    async fn owned_thread(&self, session: &SessionSummary) -> AdapterResult<SessionId> {
        let thread_id = match &session.harness {
            Provider::Other(name) if name == "t3code" => session.id.clone(),
            _ => self.native_owner(session)?.ok_or(AdapterError::Unsupported)?,
        };
        match self.transport.get_thread(&thread_id.0).await {
            Ok(_) => Ok(thread_id),
            Err(AdapterError::Other(_)) => Err(AdapterError::Unsupported),
            Err(error) => Err(error),
        }
    }

    pub fn capabilities_static() -> Capabilities {
        Capabilities {
            can_trigger_compaction: true,
            can_advise_mid_turn: false,
            can_inject_at_session_start: false,
            can_observe_compaction: false,
            reports_token_counts: false,
            headless_resume: true,
            seed_modes: vec![],
        }
    }

    fn compaction_instructions(prompt: &str, reason: &str) -> String {
        let mut instructions = prompt.trim();
        while let Some(rest) = instructions.strip_prefix("/compact") {
            if rest.is_empty() || rest.chars().next().is_some_and(char::is_whitespace) {
                instructions = rest.trim_start();
            } else {
                break;
            }
        }
        let instructions = instructions.trim();
        if instructions.is_empty() {
            uw_core::compaction::instruction_body(reason)
        } else {
            instructions.to_owned()
        }
    }

    fn turn_start_payload(thread_id: &str, text: &str) -> Value {
        json!({
            "type": "thread.turn.start",
            "commandId": uuid::Uuid::new_v4(),
            "threadId": thread_id,
            "message": {
                "messageId": uuid::Uuid::new_v4(),
                "role": "user",
                "text": text,
                "attachments": []
            },
            "runtimeMode": "full-access",
            "interactionMode": "default",
            "createdAt": chrono::Utc::now(),
        })
    }

    fn interrupt_payload(thread_id: &str) -> Value {
        json!({
            "type": "thread.turn.interrupt",
            "commandId": uuid::Uuid::new_v4(),
            "threadId": thread_id,
            "createdAt": chrono::Utc::now(),
        })
    }
}

#[async_trait]
impl HarnessAdapter for T3CodeAdapter {
    fn provider(&self) -> Provider {
        Provider::Other("t3code".into())
    }

    fn capabilities(&self) -> Capabilities {
        Self::capabilities_static()
    }

    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        Err(AdapterError::Unsupported)
    }

    async fn set_external_status(
        &self,
        session: &SessionSummary,
        status: Option<Value>,
    ) -> AdapterResult<()> {
        let thread_id = self.owned_thread(session).await?;
        self.transport
            .post_thread_status(&thread_id.0, status)
            .await
            .map(|_| ())
    }

    /// Verified against a live T3Code thread that actually hit a provider
    /// quota limit (`status: "error"`, `lastError` a plain string containing
    /// "usage limit" from a Codex-backed thread; see docs/research-t3code.md).
    /// `lastError`'s shape is provider-free text, not a structured code, so
    /// this matches the same quota phrasing already trusted elsewhere in this
    /// codebase (Codex's own "usage limit" message, Claude's
    /// "rate_limit_error"/"rate limit") rather than inventing new evidence.
    /// A thread with any other status, or an error that doesn't look
    /// quota-shaped, reports no stop — this never guesses.
    async fn detect_stop(&self, session_id: &SessionId) -> AdapterResult<Option<StopReason>> {
        let thread = self.transport.get_thread(&session_id.0).await?;
        let session = thread.pointer("/thread/session");
        let status = session
            .and_then(|s| s.get("status"))
            .and_then(Value::as_str);
        if status != Some("error") {
            return Ok(None);
        }
        let Some(error) = session
            .and_then(|s| s.get("lastError"))
            .and_then(Value::as_str)
        else {
            return Ok(None);
        };
        let lower = error.to_ascii_lowercase();
        if !(lower.contains("usage limit") || lower.contains("rate limit")) {
            return Ok(None);
        }
        Ok(Some(StopReason::UsageLimit {
            window: WindowKey {
                provider: Provider::Other("t3code".into()),
                kind: WindowKind::Custom("quota".into()),
            },
        }))
    }

    async fn emit_status(&self, _: &SessionId, _: StatusEvent) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }

    async fn advise(&self, _: &SessionId, _: &str) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }

    async fn interrupt(&self, session_id: &SessionId) -> AdapterResult<DeliveryOutcome> {
        self.transport
            .post_interrupt(Self::interrupt_payload(&session_id.0))
            .await
            .map(|_| DeliveryOutcome::Delivered)
    }

    async fn compact(
        &self,
        session: &SessionSummary,
        request: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome> {
        let thread_id = self.owned_thread(session).await?;
        let instructions = Self::compaction_instructions(&request.prompt, &request.reason);
        self.transport
            .post_dispatch(Self::turn_start_payload(&thread_id.0, &instructions))
            .await?;
        tokio::time::sleep(COMPACTION_STEP_DELAY).await;
        self.transport
            .post_interrupt(Self::interrupt_payload(&thread_id.0))
            .await?;
        tokio::time::sleep(COMPACTION_STEP_DELAY).await;
        self.transport
            .post_interrupt(Self::interrupt_payload(&thread_id.0))
            .await?;
        tokio::time::sleep(COMPACTION_STEP_DELAY).await;
        self.transport
            .post_dispatch(Self::turn_start_payload(&thread_id.0, "/compact"))
            .await
            .map(|_| DeliveryOutcome::Delivered)
    }

    async fn resume_session(
        &self,
        session: &SessionSummary,
        message: Option<&str>,
    ) -> AdapterResult<()> {
        let thread_id = self.owned_thread(session).await?;
        let text = message.unwrap_or("Continue from the saved state.");
        let payload = json!({
            "type": "thread.turn.start",
            "commandId": uuid::Uuid::new_v4(),
            "threadId": thread_id.0,
            "message": {
                "messageId": uuid::Uuid::new_v4(),
                "role": "user",
                "text": text,
                "attachments": []
            },
            "runtimeMode": "full-access",
            "interactionMode": "default",
            "createdAt": chrono::Utc::now(),
        });
        self.transport.post_dispatch(payload).await.map(|_| ())
    }

    async fn seed_new_session(&self, _: SeedMode, _: &SeedContext) -> AdapterResult<SessionId> {
        Err(AdapterError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::Value;
    use std::fs;
    use std::sync::Mutex;
    use uw_core::model::{LaunchMode, Provider, SessionId, SessionSummary};

    struct FakeTransport {
        calls: Mutex<Vec<(String, Value)>>,
        call_times: Mutex<Vec<std::time::Instant>>,
        thread: Mutex<Option<Value>>,
    }

    impl FakeTransport {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                call_times: Mutex::new(Vec::new()),
                thread: Mutex::new(None),
            }
        }

        fn with_thread(thread: Value) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                call_times: Mutex::new(Vec::new()),
                thread: Mutex::new(Some(thread)),
            }
        }
    }

    #[async_trait]
    impl T3CodeTransport for FakeTransport {
        async fn post_dispatch(&self, payload: Value) -> AdapterResult<Value> {
            self.call_times.lock().unwrap().push(std::time::Instant::now());
            self.calls
                .lock()
                .unwrap()
                .push(("dispatch".into(), payload));
            Ok(serde_json::json!({"sequence": 108562}))
        }

        async fn post_interrupt(&self, payload: Value) -> AdapterResult<Value> {
            self.call_times.lock().unwrap().push(std::time::Instant::now());
            self.calls
                .lock()
                .unwrap()
                .push(("interrupt".into(), payload));
            Ok(serde_json::json!({"sequence": 108563}))
        }

        async fn post_thread_status(
            &self,
            thread_id: &str,
            status: Option<Value>,
        ) -> AdapterResult<Value> {
            self.call_times.lock().unwrap().push(std::time::Instant::now());
            self.calls.lock().unwrap().push((
                "thread_status".into(),
                serde_json::json!({"threadId": thread_id, "status": status}),
            ));
            Ok(serde_json::json!({"sequence": 108564}))
        }

        async fn get_thread(&self, thread_id: &str) -> AdapterResult<Value> {
            self.call_times.lock().unwrap().push(std::time::Instant::now());
            self.calls
                .lock()
                .unwrap()
                .push(("get_thread".into(), Value::String(thread_id.into())));
            self.thread
                .lock()
                .unwrap()
                .clone()
                .ok_or(AdapterError::Other("no thread fixture set".into()))
        }
    }

    fn session() -> SessionSummary {
        let now = chrono::Utc::now();
        SessionSummary {
            id: SessionId("3cfef86e-7ab8-4bb1-8410-f54e9c135ea4".into()),
            harness: Provider::Other("t3code".into()),
            model: None,
            account: None,
            first_seen: now,
            last_seen: now,
            cwd: "/tmp/project".into(),
            state_path: None,
            context_window_size: None,
            last_known_token_count: None,
            launch_mode: LaunchMode::Interactive,
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

    fn native_claude_session() -> SessionSummary {
        SessionSummary {
            id: SessionId("de50f3dc-713e-44ff-baaa-ea46fd0e4e1a".into()),
            harness: Provider::ClaudeCode,
            ..session()
        }
    }

    fn native_codex_session() -> SessionSummary {
        SessionSummary {
            id: SessionId("01a0c8b8-27d5-79c1-b394-dd237acc56ec".into()),
            harness: Provider::Codex,
            ..session()
        }
    }

    fn native_cursor_session() -> SessionSummary {
        SessionSummary {
            id: SessionId("cursor-native-session".into()),
            harness: Provider::Cursor,
            ..session()
        }
    }

    fn compaction_request(session: &SessionSummary) -> CompactionRequest {
        CompactionRequest {
            id: uuid::Uuid::new_v4(),
            session_id: session.id.clone(),
            kind: CompactionKind::AgentRequested,
            prompt: "/compact\nPreserve the active goal.".into(),
            reason: "test".into(),
            resume_after_compaction: false,
            status: CompactionStatus::Sending,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn empty_compaction_prompt_uses_the_shared_instruction_blurb() {
        assert_eq!(
            T3CodeAdapter::compaction_instructions("", "manual request"),
            uw_core::compaction::instruction_body("manual request")
        );
    }

    fn owner_db(rows: &[(&str, &str, &str)]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "usagewindow-t3-owner-{}.db",
            uuid::Uuid::new_v4()
        ));
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE provider_session_runtime (
                    thread_id TEXT NOT NULL,
                    provider_name TEXT NOT NULL,
                    resume_cursor_json TEXT NOT NULL
                );",
            )
            .unwrap();
        for (thread_id, provider_name, cursor) in rows {
            connection
                .execute(
                    "INSERT INTO provider_session_runtime
                        (thread_id, provider_name, resume_cursor_json)
                     VALUES (?1, ?2, ?3)",
                    rusqlite::params![thread_id, provider_name, cursor],
                )
                .unwrap();
        }
        drop(connection);
        path
    }

    #[tokio::test]
    async fn resume_resolves_native_claude_id_to_its_t3_thread_owner() {
        let path = std::env::temp_dir().join(format!("usagewindow-t3-owner-{}.db", uuid::Uuid::new_v4()));
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE provider_session_runtime (
                    thread_id TEXT NOT NULL,
                    provider_name TEXT NOT NULL,
                    resume_cursor_json TEXT NOT NULL
                );
                INSERT INTO provider_session_runtime
                    (thread_id, provider_name, resume_cursor_json)
                VALUES
                    ('d02b9d75-f564-4cbd-8dcd-62bb445b28c6', 'claudeAgent',
                     '{\"resume\":\"de50f3dc-713e-44ff-baaa-ea46fd0e4e1a\"}');",
            )
            .unwrap();
        drop(connection);

        let owned_thread = serde_json::json!({
            "thread": {
                "id": "d02b9d75-f564-4cbd-8dcd-62bb445b28c6",
                "session": {"status": "stopped", "providerName": "claudeAgent"}
            }
        });
        let transport = std::sync::Arc::new(FakeTransport::with_thread(owned_thread));
        let adapter = T3CodeAdapter::with_state_db(transport.clone(), &path);

        adapter
            .resume_session(&native_claude_session(), Some("continue"))
            .await
            .unwrap();

        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls[0], ("get_thread".into(), Value::String("d02b9d75-f564-4cbd-8dcd-62bb445b28c6".into())));
        assert_eq!(calls[1].0, "dispatch");
        assert_eq!(calls[1].1["threadId"], "d02b9d75-f564-4cbd-8dcd-62bb445b28c6");
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn resume_checks_ownership_before_sending_a_native_thread_turn() {
        let owned_thread = serde_json::json!({
            "thread": {
                "id": session().id.0,
                "session": {
                    "threadId": session().id.0,
                    "status": "stopped",
                    "providerName": "claudeAgent"
                }
            }
        });
        let transport = std::sync::Arc::new(FakeTransport::with_thread(owned_thread));
        let adapter = T3CodeAdapter::new(transport.clone());

        adapter
            .resume_session(&session(), Some("test message"))
            .await
            .unwrap();

        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "get_thread");
        assert_eq!(calls[0].1, session().id.0);
        assert_eq!(calls[1].0, "dispatch");
        assert_eq!(calls[1].1["type"], "thread.turn.start");
        assert_eq!(calls[1].1["threadId"], session().id.0);
        assert_eq!(calls[1].1["message"]["role"], "user");
        assert_eq!(calls[1].1["message"]["text"], "test message");
        assert_eq!(calls[1].1["message"]["attachments"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn resume_declines_an_unknown_thread_without_dispatching() {
        let transport = std::sync::Arc::new(FakeTransport::new());
        let adapter = T3CodeAdapter::new(transport.clone());

        assert!(matches!(
            adapter.resume_session(&session(), None).await,
            Err(AdapterError::Unsupported)
        ));

        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "get_thread");
    }

    #[tokio::test]
    async fn sets_and_clears_external_status_through_the_owned_t3_thread() {
        let owned_thread = serde_json::json!({
            "thread": {
                "id": session().id.0,
                "session": {"status": "stopped", "providerName": "claudeAgent"}
            }
        });
        let transport = std::sync::Arc::new(FakeTransport::with_thread(owned_thread));
        let adapter = T3CodeAdapter::new(transport.clone());
        let status = serde_json::json!({
            "source": "usagewindow",
            "key": "paused",
            "text": "Paused",
            "icon": "pause",
            "color": "slate",
            "notifyUser": false,
            "expiresAt": null,
            "clearsOn": "work",
            "updatedAt": "2026-09-22T12:00:00.000Z"
        });

        adapter
            .set_external_status(&session(), Some(status.clone()))
            .await
            .unwrap();
        adapter.set_external_status(&session(), None).await.unwrap();

        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls[1].0, "thread_status");
        assert_eq!(calls[1].1["threadId"], session().id.0);
        assert_eq!(calls[1].1["status"], status);
        assert_eq!(calls[3].1["status"], Value::Null);
    }

    #[tokio::test]
    async fn sends_instructions_interrupts_and_uses_t3code_native_compaction() {
        let transport = std::sync::Arc::new(FakeTransport::with_thread(serde_json::json!({
            "thread": {
                "id": session().id.0,
                "session": {"status": "stopped", "providerName": "claudeAgent"}
            }
        })));
        let adapter = T3CodeAdapter::new(transport.clone());
        let request = compaction_request(&session());

        assert_eq!(
            adapter.compact(&session(), &request).await.unwrap(),
            DeliveryOutcome::Delivered
        );
        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls.len(), 5);
        assert_eq!(calls[0].0, "get_thread");
        assert_eq!(calls[1].1["type"], "thread.turn.start");
        assert_eq!(calls[1].1["threadId"], session().id.0);
        assert_eq!(calls[1].1["message"]["text"], "Preserve the active goal.");
        assert_eq!(calls[2].0, "interrupt");
        assert_eq!(calls[2].1["type"], "thread.turn.interrupt");
        assert_eq!(calls[2].1["threadId"], session().id.0);
        assert!(calls[2].1.get("turnId").is_none());
        assert_eq!(calls[3].0, "interrupt");
        assert_eq!(calls[3].1["type"], "thread.turn.interrupt");
        assert_eq!(calls[3].1["threadId"], session().id.0);
        assert_eq!(calls[4].0, "dispatch");
        assert_eq!(calls[4].1["type"], "thread.turn.start");
        assert_eq!(calls[4].1["threadId"], session().id.0);
        assert_eq!(calls[4].1["message"]["text"], "/compact");
        let call_times = transport.call_times.lock().unwrap();
        assert!(call_times[2].duration_since(call_times[1]) >= std::time::Duration::from_secs(2));
        assert!(call_times[3].duration_since(call_times[2]) >= std::time::Duration::from_secs(2));
        assert!(call_times[4].duration_since(call_times[3]) >= std::time::Duration::from_secs(2));
    }

    #[tokio::test]
    async fn compaction_resolves_every_native_provider_to_its_owned_t3_thread() {
        let cases = [
            (
                native_claude_session(),
                "claudeAgent",
                r#"{"resume":"de50f3dc-713e-44ff-baaa-ea46fd0e4e1a"}"#,
                "t3-claude-thread",
            ),
            (
                native_codex_session(),
                "codex",
                r#"{"threadId":"01a0c8b8-27d5-79c1-b394-dd237acc56ec"}"#,
                "t3-codex-thread",
            ),
            (
                native_cursor_session(),
                "cursor",
                r#"{"sessionId":"cursor-native-session"}"#,
                "t3-cursor-thread",
            ),
        ];

        for (native_session, provider_name, cursor, t3_thread_id) in cases {
            let path = owner_db(&[(t3_thread_id, provider_name, cursor)]);
            let transport = std::sync::Arc::new(FakeTransport::with_thread(serde_json::json!({
                "thread": {
                    "id": t3_thread_id,
                    "session": {"status": "stopped", "providerName": provider_name}
                }
            })));
            let adapter = T3CodeAdapter::with_state_db(transport.clone(), &path);

            adapter
                .compact(&native_session, &compaction_request(&native_session))
                .await
                .unwrap();

            let calls = transport.calls.lock().unwrap();
            assert_eq!(calls[0].0, "get_thread");
            assert_eq!(calls[0].1, t3_thread_id);
            assert_eq!(calls[1].1["threadId"], t3_thread_id);
            assert_eq!(calls[2].1["threadId"], t3_thread_id);
            assert_eq!(calls[3].1["threadId"], t3_thread_id);
            assert_eq!(calls[4].1["threadId"], t3_thread_id);
            fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn advertises_only_native_resume() {
        assert_eq!(
            T3CodeAdapter::capabilities_static(),
            Capabilities {
                can_trigger_compaction: true,
                can_advise_mid_turn: false,
                can_inject_at_session_start: false,
                can_observe_compaction: false,
                reports_token_counts: false,
                headless_resume: true,
                seed_modes: vec![],
            }
        );
    }

    /// Shape verified live against a T3Code thread that actually hit a
    /// provider quota limit (see docs/research-t3code.md): a Codex-backed
    /// thread with `status: "error"` and a plain-string `lastError`.
    fn error_thread(last_error: &str) -> Value {
        serde_json::json!({
            "thread": {
                "id": session().id.0,
                "session": {
                    "threadId": session().id.0,
                    "status": "error",
                    "providerName": "codex",
                    "providerInstanceId": "codex",
                    "runtimeMode": "full-access",
                    "activeTurnId": null,
                    "lastError": last_error,
                    "updatedAt": "2026-09-21T06:25:25.218Z",
                }
            }
        })
    }

    #[tokio::test]
    async fn stop_detection_reports_a_verified_quota_error_thread() {
        let adapter = T3CodeAdapter::new(Arc::new(FakeTransport::with_thread(error_thread(
            "You've hit your usage limit. Upgrade to Pro (https://chatgpt.com/explore/pro), \
             visit https://chatgpt.com/codex/settings/usage to purchase more credits or try \
             again at 3:48 AM.",
        ))));
        assert_eq!(
            adapter.detect_stop(&session().id).await.unwrap(),
            Some(StopReason::UsageLimit {
                window: WindowKey {
                    provider: Provider::Other("t3code".into()),
                    kind: WindowKind::Custom("quota".into()),
                },
            })
        );
    }

    #[tokio::test]
    async fn stop_detection_does_not_invent_a_stop_for_a_non_quota_error() {
        let adapter = T3CodeAdapter::new(Arc::new(FakeTransport::with_thread(error_thread(
            "internal server error",
        ))));
        assert_eq!(adapter.detect_stop(&session().id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn stop_detection_does_not_invent_a_stop_for_a_normally_stopped_thread() {
        let normal = serde_json::json!({
            "thread": {
                "id": session().id.0,
                "session": {
                    "threadId": session().id.0,
                    "status": "stopped",
                    "providerName": "claudeAgent",
                    "providerInstanceId": "claudeAgent",
                    "runtimeMode": "full-access",
                    "activeTurnId": null,
                    "lastError": null,
                    "updatedAt": "2026-09-18T06:31:17.415Z",
                }
            }
        });
        let adapter = T3CodeAdapter::new(Arc::new(FakeTransport::with_thread(normal)));
        assert_eq!(adapter.detect_stop(&session().id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn stop_detection_propagates_a_transport_error_instead_of_guessing() {
        let adapter = T3CodeAdapter::new(Arc::new(FakeTransport::new()));
        assert!(adapter.detect_stop(&session().id).await.is_err());
    }
}
