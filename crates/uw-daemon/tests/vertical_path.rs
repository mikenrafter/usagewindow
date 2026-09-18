use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, HarnessAdapter, SeedMode,
    StatusEvent,
};
use uw_core::model::*;
use uw_daemon::{DaemonStore, SqliteDaemonStore, run_observation_tick, run_resume_tick};
use uw_store::Store;

struct FakeAdapter {
    sample: UsageSample,
}

#[async_trait]
impl HarnessAdapter for FakeAdapter {
    fn provider(&self) -> Provider {
        Provider::Codex
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            can_trigger_compaction: false,
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
        Err(AdapterError::Unsupported)
    }

    async fn resume_session(&self, _: &SessionSummary) -> AdapterResult<()> {
        Ok(())
    }
}

fn session(id: SessionId) -> SessionSummary {
    SessionSummary {
        id,
        harness: Provider::Codex,
        model: Some(ModelId("gpt".into())),
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
    }
}

#[tokio::test]
async fn observation_and_claimed_resume_cross_the_sqlite_boundary() {
    let path = std::env::temp_dir().join(format!("uw-vertical-{}.db", uuid::Uuid::new_v4()));
    let path_string = path.to_string_lossy().into_owned();
    let initial = Store::open(&path_string).unwrap();
    let id = SessionId("integration-session".into());
    initial.insert_session(&session(id.clone())).unwrap();
    initial.record_hook_event(&id, "SessionEnd").unwrap();
    drop(initial);

    let key = WindowKey {
        provider: Provider::Codex,
        kind: WindowKind::Rolling { minutes: 300 },
    };
    let now = Utc::now();
    let adapter = Arc::new(FakeAdapter {
        sample: UsageSample {
            at: now,
            fetched_at: Some(now),
            source: UsageSource::ProviderReported,
            provider: Provider::Codex,
            account: None,
            windows: HashMap::from([(
                key.clone(),
                UsageWindowState::new(100.0, true, true, None, None),
            )]),
            credits: None,
        },
    });
    let adapters = HashMap::from([(Provider::Codex, adapter as Arc<dyn HarnessAdapter>)]);
    let store = Arc::new(SqliteDaemonStore::new(Store::open(&path_string).unwrap()));

    let report = run_observation_tick(store.as_ref(), &adapters)
        .await
        .unwrap();
    assert_eq!(report.samples_recorded, 1);
    assert_eq!(report.stops_recorded, 1);

    let marker = ResumeMarker {
        id: uuid::Uuid::new_v4(),
        session_id: id.clone(),
        reason: ResumeReason::AutoDetectedLimit,
        resume_at: Some(now),
        created_at: now,
        status: ResumeStatus::Scheduled,
    };
    store.insert_resume_marker(marker.clone()).await.unwrap();
    run_resume_tick(store.as_ref(), &adapters, now)
        .await
        .unwrap();

    let reader = Store::open(&path_string).unwrap();
    assert_eq!(reader.all_usage_samples().unwrap().len(), 1);
    assert_eq!(
        reader.read_session(&id).unwrap().stopped_reason,
        Some(StopReason::UsageLimit { window: key })
    );
    assert_eq!(
        reader.resume_markers_for_session(&id).unwrap()[0].status,
        ResumeStatus::Fired
    );
    drop(reader);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}
