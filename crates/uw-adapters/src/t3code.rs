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

#[async_trait]
impl T3CodeTransport for T3CodeHttpTransport {
    async fn post_dispatch(&self, payload: Value) -> AdapterResult<Value> {
        let mut request = self
            .client
            .post(format!("{}/api/orchestration/dispatch", self.base_url))
            .json(&payload);
        request = match &self.auth {
            T3CodeAuth::Bearer(token) => request.bearer_auth(token),
            T3CodeAuth::Cookie(cookie) => request.header(reqwest::header::COOKIE, cookie),
        };
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
                "T3Code dispatch failed ({status}): {body}"
            )));
        }
        Ok(body)
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

    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        Err(AdapterError::Unsupported)
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
                "text": crate::claude_code::compact_instructions(&request.prompt),
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
        let transport = std::sync::Arc::new(FakeTransport {
            calls: Mutex::new(Vec::new()),
        });
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
        let transport = std::sync::Arc::new(FakeTransport {
            calls: Mutex::new(Vec::new()),
        });
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
        assert_eq!(calls[0].1["message"]["text"], request.prompt);
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
}
