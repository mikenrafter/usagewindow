use async_trait::async_trait;
use std::sync::Arc;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, DiscoveredSession, HarnessAdapter,
    SeedContext, SeedMode, StatusEvent, TurnPreview,
};
use uw_core::model::*;

/// Decorates a provider-native adapter with a meta-harness delivery route.
///
/// Only destructive compaction is allowed to cross this boundary. All other
/// operations remain owned by the provider-native adapter, so a missing native
/// control surface cannot accidentally change usage, resume, or observation
/// semantics.
pub struct FallbackCompactionAdapter {
    native: Arc<dyn HarnessAdapter>,
    meta: Arc<dyn HarnessAdapter>,
}

#[async_trait]
pub trait MetaHarnessMessenger: Send + Sync {
    async fn send(&self, session_id: &SessionId, text: &str) -> AdapterResult<DeliveryOutcome>;
}

/// A provider-neutral `/compact` delivery adapter backed by a meta-harness
/// messenger such as Paseo. It is intentionally useful only as a fallback;
/// it does not claim native usage, resume, or observation capabilities.
pub struct MessageCompactionAdapter {
    messenger: Arc<dyn MetaHarnessMessenger>,
}

impl MessageCompactionAdapter {
    pub fn new(messenger: Arc<dyn MetaHarnessMessenger>) -> Self {
        Self { messenger }
    }
}

#[async_trait]
impl HarnessAdapter for MessageCompactionAdapter {
    fn provider(&self) -> Provider {
        Provider::Other("meta-harness".into())
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            can_trigger_compaction: true,
            can_advise_mid_turn: false,
            can_inject_at_session_start: false,
            can_observe_compaction: false,
            reports_token_counts: false,
            headless_resume: false,
            seed_modes: vec![],
        }
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
        let text = crate::claude_code::compact_instructions(&request.prompt);
        self.messenger.send(&session.id, &text).await
    }
    async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
        Err(AdapterError::Unsupported)
    }
}

impl FallbackCompactionAdapter {
    pub fn new(native: Arc<dyn HarnessAdapter>, meta: Arc<dyn HarnessAdapter>) -> Self {
        Self { native, meta }
    }
}

#[async_trait]
impl HarnessAdapter for FallbackCompactionAdapter {
    fn provider(&self) -> Provider {
        self.native.provider()
    }
    fn capabilities(&self) -> Capabilities {
        self.native.capabilities()
    }
    async fn fetch_usage(&self, account: Option<&AccountId>) -> AdapterResult<UsageSample> {
        self.native.fetch_usage(account).await
    }
    async fn detect_stop(&self, session_id: &SessionId) -> AdapterResult<Option<StopReason>> {
        self.native.detect_stop(session_id).await
    }
    async fn detect_stop_with_usage(
        &self,
        session_id: &SessionId,
        sample: Option<&UsageSample>,
    ) -> AdapterResult<Option<StopReason>> {
        self.native.detect_stop_with_usage(session_id, sample).await
    }
    async fn emit_status(
        &self,
        session_id: &SessionId,
        status: StatusEvent,
    ) -> AdapterResult<DeliveryOutcome> {
        self.native.emit_status(session_id, status).await
    }
    async fn advise(&self, session_id: &SessionId, text: &str) -> AdapterResult<DeliveryOutcome> {
        self.native.advise(session_id, text).await
    }
    async fn compact(
        &self,
        session: &SessionSummary,
        request: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome> {
        match self.native.compact(session, request).await {
            Ok(outcome) => Ok(outcome),
            Err(native_error) => self.meta.compact(session, request).await.map_err(|meta_error| {
                AdapterError::Other(format!("native compaction failed: {native_error}; meta-harness fallback failed: {meta_error}"))
            }),
        }
    }
    async fn resume_session(
        &self,
        session: &SessionSummary,
        message: Option<&str>,
    ) -> AdapterResult<()> {
        self.native.resume_session(session, message).await
    }
    async fn export_transcript(&self, session: &SessionSummary) -> AdapterResult<String> {
        self.native.export_transcript(session).await
    }
    async fn seed_new_session(
        &self,
        mode: SeedMode,
        seed: &SeedContext,
    ) -> AdapterResult<SessionId> {
        self.native.seed_new_session(mode, seed).await
    }
    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        self.native.discover_sessions().await
    }
    async fn session_preview(&self, session: &SessionSummary) -> AdapterResult<Vec<TurnPreview>> {
        self.native.session_preview(session).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use uw_core::adapter::{AdapterResult, Capabilities};

    #[derive(Clone)]
    struct Fake {
        compact_result: Arc<Mutex<Option<AdapterResult<DeliveryOutcome>>>>,
        compact_calls: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl HarnessAdapter for Fake {
        fn provider(&self) -> Provider {
            Provider::Codex
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                can_trigger_compaction: true,
                can_advise_mid_turn: false,
                can_inject_at_session_start: false,
                can_observe_compaction: false,
                reports_token_counts: false,
                headless_resume: false,
                seed_modes: vec![],
            }
        }
        async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
            Err(AdapterError::Unsupported)
        }
        async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
            Err(AdapterError::Unsupported)
        }
        async fn emit_status(
            &self,
            _: &SessionId,
            _: StatusEvent,
        ) -> AdapterResult<DeliveryOutcome> {
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
            *self.compact_calls.lock().unwrap() += 1;
            self.compact_result
                .lock()
                .unwrap()
                .take()
                .unwrap_or(Ok(DeliveryOutcome::Delivered))
        }
        async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
            Err(AdapterError::Unsupported)
        }
    }

    fn request() -> CompactionRequest {
        let now = chrono::Utc::now();
        CompactionRequest {
            id: uuid::Uuid::new_v4(),
            session_id: SessionId("thread".into()),
            kind: CompactionKind::AgentRequested,
            prompt: "/compact".into(),
            reason: "test".into(),
            status: CompactionStatus::Sending,
            created_at: now,
        }
    }

    fn session() -> SessionSummary {
        let now = chrono::Utc::now();
        SessionSummary {
            id: SessionId("thread".into()),
            harness: Provider::Codex,
            model: None,
            account: None,
            first_seen: now,
            last_seen: now,
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
        }
    }

    #[tokio::test]
    async fn tries_meta_harness_only_after_native_compaction_fails() {
        let native = Fake {
            compact_result: Arc::new(Mutex::new(Some(Err(AdapterError::Transient(
                "offline".into(),
            ))))),
            compact_calls: Arc::new(Mutex::new(0)),
        };
        let meta = Fake {
            compact_result: Arc::new(Mutex::new(Some(Ok(DeliveryOutcome::Delivered)))),
            compact_calls: Arc::new(Mutex::new(0)),
        };
        let adapter =
            FallbackCompactionAdapter::new(Arc::new(native.clone()), Arc::new(meta.clone()));

        assert_eq!(
            adapter.compact(&session(), &request()).await.unwrap(),
            DeliveryOutcome::Delivered
        );
        assert_eq!(*native.compact_calls.lock().unwrap(), 1);
        assert_eq!(*meta.compact_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn does_not_use_meta_harness_after_native_success() {
        let native = Fake {
            compact_result: Arc::new(Mutex::new(Some(Ok(DeliveryOutcome::Delivered)))),
            compact_calls: Arc::new(Mutex::new(0)),
        };
        let meta = Fake {
            compact_result: Arc::new(Mutex::new(Some(Ok(DeliveryOutcome::Delivered)))),
            compact_calls: Arc::new(Mutex::new(0)),
        };
        let adapter =
            FallbackCompactionAdapter::new(Arc::new(native.clone()), Arc::new(meta.clone()));

        adapter.compact(&session(), &request()).await.unwrap();
        assert_eq!(*meta.compact_calls.lock().unwrap(), 0);
    }
}
