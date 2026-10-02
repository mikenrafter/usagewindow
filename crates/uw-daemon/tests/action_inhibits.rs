use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, HarnessAdapter, SeedMode,
    StatusEvent,
};
use uw_core::model::*;
use uw_daemon::{SqliteDaemonStore, run_observation_tick, run_resume_tick, run_compaction_tick, SessionLivenessChecker};
use uw_store::Store;

struct FakeAdapter {
    sample: UsageSample,
    resumed_with: Mutex<Option<String>>,
    compactions: Mutex<u32>,
}

#[async_trait]
impl HarnessAdapter for FakeAdapter {
    async fn session_activity(&self, _: &SessionSummary) -> AdapterResult<uw_core::activity::SessionActivity> {
        Ok(uw_core::activity::SessionActivity::Idle)
    }

    fn provider(&self) -> Provider {
        Provider::Codex
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            can_trigger_compaction: true,
            can_advise_mid_turn: false,
            can_inject_at_session_start: true,
            can_observe_compaction: true,
            reports_token_counts: false,
            headless_resume: true,
            seed_modes: vec![SeedMode::InitialPrompt],
        }
    }

    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        Ok(self.sample.clone())
    }

    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        Ok(Some(StopReason::UsageLimit {
            window: self.sample.windows.keys().next().unwrap().clone(),
        }))
    }

    async fn emit_status(&self, _: &SessionId, _: StatusEvent) -> AdapterResult<DeliveryOutcome> {
        Ok(DeliveryOutcome::QueuedForNextIdle)
    }

    async fn advise(&self, _: &SessionId, _: &str) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }

    async fn compact(
        &self,
        _: &SessionSummary,
        _: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome> {
        *self.compactions.lock().unwrap() += 1;
        Ok(DeliveryOutcome::Delivered)
    }

    async fn resume_session(&self, _: &SessionSummary, message: Option<&str>) -> AdapterResult<()> {
        *self.resumed_with.lock().unwrap() = message.map(str::to_string);
        Ok(())
    }
}

fn session(id: SessionId) -> SessionSummary {
    SessionSummary {
        id,
        lineage: SessionLineage::default(),
        harness: Provider::Codex,
        model: Some(ModelId("gpt".into())),
        account: None,
        first_seen: Utc::now(),
        last_seen: Utc::now(),
        cwd: "/tmp".into(),
        state_path: None,
        context_window_size: None,
        last_known_token_count: None,
        last_known_context_pct: None,
        launch_mode: LaunchMode::Headless,
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


struct Idle;
#[async_trait]
impl SessionLivenessChecker for Idle {
    async fn is_idle(&self, _: &SessionSummary) -> bool { true }
}

#[tokio::test]
async fn overrides_block_all_queued_compactions_and_resumes_but_keep_observation() {
    for scope in [None, Some(Provider::Codex), Some(Provider::ClaudeCode)] {
        for kind in [CompactionKind::AgentRequested, CompactionKind::OpportunisticIdle, CompactionKind::AltModelReseed] {
            for reason in [ResumeReason::ManuallyMarked, ResumeReason::AutoDetectedLimit] {
                let store = Store::open_memory().unwrap();
                let now = Utc::now();
                let session = session(SessionId("inhibit-session".into()));
                store.insert_session(&session).unwrap();
                let request = CompactionRequest {
                    id: uuid::Uuid::new_v4(), session_id: session.id.clone(), kind: kind.clone(),
                    prompt: "compact".into(), reason: "test".into(), resume_after_compaction: false,
                    preempt: true, status: CompactionStatus::Pending, created_at: now,
                };
                store.insert_compaction_request(&request).unwrap();
                let marker = ResumeMarker {
                    id: uuid::Uuid::new_v4(), session_id: session.id.clone(), reason,
                    resume_at: Some(now), requested_at: None, created_at: now,
                    status: ResumeStatus::Scheduled, message: Some("continue".into()), preempt: true,
                };
                store.insert_resume_marker(&marker).unwrap();
                store.set_action_inhibit(scope.as_ref(), true).unwrap();
                let blocked = scope != Some(Provider::ClaudeCode);
                let adapter = Arc::new(FakeAdapter {
                    sample: UsageSample { at: now, fetched_at: Some(now), source: UsageSource::ProviderReported,
                        provider: Provider::Codex, account: None, plan: None,
                        windows: HashMap::from([(WindowKey { provider: Provider::Codex, kind: WindowKind::Rolling { minutes: 300 } },
                            UsageWindowState::new(10.0, false, true, None, None))]), credits: None },
                    resumed_with: Mutex::new(None), compactions: Mutex::new(0),
                });
                let adapters = HashMap::from([(Provider::Codex, adapter.clone() as Arc<dyn HarnessAdapter>)]);
                let store = SqliteDaemonStore::new(store);
                run_observation_tick(&store, &adapters).await.unwrap();
                run_compaction_tick(&store, &adapters, &Idle).await.unwrap();
                run_resume_tick(&store, &adapters, now).await.unwrap();
                assert_eq!(*adapter.compactions.lock().unwrap(), u32::from(!blocked), "{scope:?} {kind:?}");
                assert_eq!(adapter.resumed_with.lock().unwrap().is_some(), !blocked);
                let shared = store.shared_store();
                {
                    let db = shared.lock().unwrap();
                    assert_eq!(db.all_usage_samples().unwrap().len(), 1);
                    if blocked {
                        assert_eq!(db.compaction_requests_for_session(&session.id).unwrap()[0].status, CompactionStatus::Pending);
                        assert_eq!(db.resume_markers_for_session(&session.id).unwrap()[0].status, ResumeStatus::Scheduled);
                    }
                    db.set_action_inhibit(scope.as_ref(), false).unwrap();
                }
                run_compaction_tick(&store, &adapters, &Idle).await.unwrap();
                run_resume_tick(&store, &adapters, now).await.unwrap();
                run_compaction_tick(&store, &adapters, &Idle).await.unwrap();
                run_resume_tick(&store, &adapters, now).await.unwrap();
                assert_eq!(*adapter.compactions.lock().unwrap(), 1);
                assert_eq!(adapter.resumed_with.lock().unwrap().as_deref(), Some("continue"));
            }
        }
    }
}
