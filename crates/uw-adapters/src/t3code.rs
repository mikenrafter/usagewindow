use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Arc;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, HarnessAdapter, SeedContext,
    SeedMode, StatusEvent,
};
use uw_core::model::*;

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
}

impl T3CodeAdapter {
    pub fn new(transport: Arc<dyn T3CodeTransport>) -> Self {
        Self { transport }
    }

    pub fn real(base_url: impl Into<String>, auth: T3CodeAuth) -> Self {
        Self::new(Arc::new(T3CodeHttpTransport::new(base_url, auth)))
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

    async fn compact(
        &self,
        session: &SessionSummary,
        request: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome> {
        let payload = json!({
            "type": "thread.turn.start",
            "commandId": uuid::Uuid::new_v4(),
            "threadId": session.id.0,
            "message": {
                "messageId": uuid::Uuid::new_v4(),
                "role": "user",
                "text": uw_core::compaction::message("/compact", &request.prompt),
                "attachments": []
            },
            "runtimeMode": "full-access",
            "interactionMode": "default",
            "createdAt": chrono::Utc::now(),
        });
        self.transport
            .post_dispatch(payload)
            .await
            .map(|_| DeliveryOutcome::Delivered)
    }

    async fn resume_session(
        &self,
        session: &SessionSummary,
        message: Option<&str>,
    ) -> AdapterResult<()> {
        let text = message.unwrap_or("Continue from the saved state.");
        let payload = json!({
            "type": "thread.turn.start",
            "commandId": uuid::Uuid::new_v4(),
            "threadId": session.id.0,
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
    use std::sync::Mutex;
    use uw_core::model::{LaunchMode, Provider, SessionId, SessionSummary};

    struct FakeTransport {
        calls: Mutex<Vec<(String, Value)>>,
        thread: Mutex<Option<Value>>,
    }

    impl FakeTransport {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                thread: Mutex::new(None),
            }
        }

        fn with_thread(thread: Value) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                thread: Mutex::new(Some(thread)),
            }
        }
    }

    #[async_trait]
    impl T3CodeTransport for FakeTransport {
        async fn post_dispatch(&self, payload: Value) -> AdapterResult<Value> {
            self.calls
                .lock()
                .unwrap()
                .push(("dispatch".into(), payload));
            Ok(serde_json::json!({"sequence": 108562}))
        }

        async fn get_thread(&self, thread_id: &str) -> AdapterResult<Value> {
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
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        }
    }

    #[tokio::test]
    async fn sends_a_native_thread_turn() {
        let transport = std::sync::Arc::new(FakeTransport::new());
        let adapter = T3CodeAdapter::new(transport.clone());

        adapter
            .resume_session(&session(), Some("test message"))
            .await
            .unwrap();

        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "dispatch");
        assert_eq!(calls[0].1["type"], "thread.turn.start");
        assert_eq!(calls[0].1["threadId"], session().id.0);
        assert_eq!(calls[0].1["message"]["role"], "user");
        assert_eq!(calls[0].1["message"]["text"], "test message");
        assert_eq!(calls[0].1["message"]["attachments"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn sends_compact_as_a_meta_harness_message() {
        let transport = std::sync::Arc::new(FakeTransport::new());
        let adapter = T3CodeAdapter::new(transport.clone());
        let request = CompactionRequest {
            id: uuid::Uuid::new_v4(),
            session_id: session().id.clone(),
            kind: CompactionKind::AgentRequested,
            prompt: "/compact\nPreserve the active goal.".into(),
            reason: "test".into(),
            status: CompactionStatus::Sending,
            created_at: chrono::Utc::now(),
        };

        assert_eq!(
            adapter.compact(&session(), &request).await.unwrap(),
            DeliveryOutcome::Delivered
        );
        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls[0].1["type"], "thread.turn.start");
        assert_eq!(calls[0].1["threadId"], session().id.0);
        assert_eq!(
            calls[0].1["message"]["text"],
            "/compact Preserve the active goal."
        );
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
