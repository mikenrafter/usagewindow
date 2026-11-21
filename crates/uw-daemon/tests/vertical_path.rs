use async_trait::async_trait;
use chrono::{Duration, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, HarnessAdapter, SeedMode,
    StatusEvent,
};
use uw_core::model::*;
use uw_daemon::{DaemonStore, SqliteDaemonStore, run_observation_tick, run_resume_tick};
use uw_store::Store;

struct FakeAdapter {
    sample: UsageSample,
    resumed_with: Mutex<Option<String>>,
}

#[async_trait]
impl HarnessAdapter for FakeAdapter {
    fn provider(&self) -> Provider {
        Provider::Codex
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            observe_only: false,
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
        cache_ttl: None,
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
            plan: None,
            windows: HashMap::from([(
                key.clone(),
                UsageWindowState::new(100.0, true, true, None, None),
            )]),
            credits: None,
        },
        resumed_with: Mutex::new(None),
    });
    let adapter_ref = adapter.clone();
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
        requested_at: None,
        created_at: now,
        status: ResumeStatus::Scheduled,
        message: Some("what's the state of the project? You have been resumed".into()),
        preempt: true,
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
    assert_eq!(
        adapter_ref.resumed_with.lock().unwrap().as_deref(),
        Some("what's the state of the project? You have been resumed")
    );
    drop(reader);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn antigravity_observation_persists_gemini_windows_and_t3_sessions_observe_only() {
    // why: end-to-end vertical path for Antigravity/Gemini: quota TSV ->
    //      Provider::Gemini windows, T3 statev2 + conversation DB -> Gemini
    //      session keyed by T3 thread id with cached/uncached token usage,
    //      and observe-only refusal on due resume even with T3Code registered.
    // catches: dropped sibling windows, wrong session id or harness, or
    //          T3Code firing resume on a Gemini session.
    struct FakeUsage;
    #[async_trait]
    impl uw_adapters::antigravity::AntigravityUsageReader for FakeUsage {
        async fn read_usage_tsv(&self) -> AdapterResult<String> {
            Ok(concat!(
                "Gemini Models\tWeekly Limit Remaining\t83%\t2026-11-28T03:15:21Z\n",
                "Gemini Models\tFive Hour Limit Remaining\t39%\t2026-11-21T08:15:21Z\n",
                "Claude and GPT models\tWeekly Limit Remaining\t100%\t2026-11-28T05:23:27Z\n",
                "Claude and GPT models\tFive Hour Limit Remaining\t100%\t2026-11-21T10:23:27Z\n",
                "Gemini 4 Argon\tGemini 4 Argon Weekly Limit\t100%\t2026-11-28T05:23:27Z\n",
            )
            .into())
        }
    }

    struct RecordingT3Transport {
        dispatches: Mutex<u32>,
    }
    #[async_trait]
    impl uw_adapters::t3code::T3CodeTransport for RecordingT3Transport {
        async fn post_dispatch(&self, _: serde_json::Value) -> AdapterResult<serde_json::Value> {
            *self.dispatches.lock().unwrap() += 1;
            Ok(serde_json::json!({"sequence": 1}))
        }
        async fn post_interrupt(&self, _: serde_json::Value) -> AdapterResult<serde_json::Value> {
            *self.dispatches.lock().unwrap() += 1;
            Ok(serde_json::json!({"sequence": 2}))
        }
        async fn get_thread(&self, id: &str) -> AdapterResult<serde_json::Value> {
            Ok(serde_json::json!({
                "thread": {
                    "id": id,
                    "session": {"status": "idle", "providerName": "antigravity"}
                }
            }))
        }
    }

    let root = std::env::temp_dir().join(format!("uw-ag-vertical-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let uw_db = root.join("usagewindow.db");
    let t3_db = root.join("statev2.sqlite");
    let conv_dir = root.join("conversations");
    std::fs::create_dir_all(&conv_dir).unwrap();

    // Synthetic statev2.sqlite matching live T3 v2 schema
    {
        let t3_store = Store::open(&t3_db.to_string_lossy()).unwrap();
        t3_store.connection().execute_batch(
            "CREATE TABLE projection_projects (project_id TEXT PRIMARY KEY, workspace_root TEXT NOT NULL);
             CREATE TABLE orchestration_v2_projection_threads (
                 thread_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, title TEXT NOT NULL,
                 default_provider TEXT NOT NULL, active_provider_thread_id TEXT,
                 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, archived_at TEXT,
                 deleted_at TEXT, payload_json TEXT NOT NULL
             );
             CREATE TABLE orchestration_v2_projection_provider_threads (
                 provider_thread_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL,
                 provider TEXT NOT NULL, provider_session_id TEXT NOT NULL,
                 status TEXT NOT NULL, updated_at TEXT NOT NULL, payload_json TEXT NOT NULL
             );
             CREATE TABLE orchestration_v2_projection_provider_sessions (
                 provider_session_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL,
                 provider TEXT NOT NULL, status TEXT NOT NULL,
                 updated_at TEXT NOT NULL, payload_json TEXT NOT NULL
             );
             INSERT INTO projection_projects VALUES ('proj-1', '/workspace/mstack');
             INSERT INTO orchestration_v2_projection_threads VALUES (
                 'f360e6a4-2efd-4016-b759-01a2bca774fd', 'proj-1', 'Neon Integration Run',
                 'antigravity', 'pt-neon', '2026-11-21T05:18:00Z', '2026-11-21T05:22:00Z',
                 NULL, NULL, '{\"modelSelection\":{\"model\":\"gemini-4-neon\"}}'
             );
             INSERT INTO orchestration_v2_projection_provider_threads VALUES (
                 'pt-neon', 'f360e6a4-2efd-4016-b759-01a2bca774fd', 'antigravity', 'ps-neon',
                 'active', '2026-11-21T05:22:30Z',
                 '{\"nativeThreadRef\":{\"driver\":\"antigravity\",\"nativeId\":\"22222222-3333-4444-5555-666666666666\"},\"contextUsage\":{\"usedTokens\":46547,\"maxTokens\":1048576}}'
             );
             INSERT INTO orchestration_v2_projection_provider_sessions VALUES (
                 'ps-neon', 'f360e6a4-2efd-4016-b759-01a2bca774fd', 'antigravity', 'ready',
                 '2026-11-21T05:18:00Z', '{\"cwd\":\"/workspace/mstack\"}'
             );",
        )
        .unwrap();
    }

    let ag_adapter: Arc<dyn HarnessAdapter> = Arc::new(
        uw_adapters::antigravity::AntigravityAdapter::new(Arc::new(FakeUsage))
            .with_t3_state_db(t3_db)
            .with_conversations_dir(conv_dir),
    );
    let t3_transport = Arc::new(RecordingT3Transport {
        dispatches: Mutex::new(0),
    });
    let t3_adapter: Arc<dyn HarnessAdapter> = Arc::new(uw_adapters::t3code::T3CodeAdapter::new(
        t3_transport.clone(),
    ));
    let adapters = HashMap::from([
        (Provider::Gemini, ag_adapter),
        (Provider::Other("t3code".into()), t3_adapter),
    ]);

    let db_str = uw_db.to_string_lossy().into_owned();
    let store = Arc::new(SqliteDaemonStore::new(Store::open(&db_str).unwrap()));
    let report = run_observation_tick(store.as_ref(), &adapters)
        .await
        .unwrap();
    assert_eq!(report.samples_recorded, 1);
    assert_eq!(report.sessions_discovered, 1);
    // t3code.fetch_usage is Unsupported (1 adapter error), Antigravity has 0 errors.
    assert_eq!(report.adapter_errors, 1);

    let thread_id = SessionId("f360e6a4-2efd-4016-b759-01a2bca774fd".into());
    let now = Utc::now();
    let marker = ResumeMarker {
        id: uuid::Uuid::new_v4(),
        session_id: thread_id.clone(),
        reason: ResumeReason::ManuallyMarked,
        resume_at: Some(now),
        requested_at: None,
        created_at: now,
        status: ResumeStatus::Scheduled,
        message: Some("should not fire".into()),
        preempt: false,
    };
    store.insert_resume_marker(marker).await.unwrap();
    run_resume_tick(store.as_ref(), &adapters, now)
        .await
        .unwrap();
    assert_eq!(
        *t3_transport.dispatches.lock().unwrap(),
        0,
        "T3Code must never dispatch a resume turn for a Provider::Gemini session"
    );

    let reader = Store::open(&db_str).unwrap();
    let session = reader.read_session(&thread_id).unwrap();
    assert_eq!(session.harness, Provider::Gemini);
    assert_eq!(session.model, Some(ModelId("gemini-4-neon".into())));
    assert_eq!(session.context_window_size, Some(1_048_576));
    assert_eq!(session.last_known_token_count, Some(46_547));
    // why: discovery must persist the measured TTL for shared cache-warm consumers.
    assert_eq!(session.cache_ttl, Some(Duration::minutes(23)));
    assert!(is_cache_warm_for(
        session.last_seen,
        session.last_seen + Duration::minutes(23),
        session.prompt_cache_ttl()
    ));
    assert!(!is_cache_warm_for(
        session.last_seen,
        session.last_seen + Duration::minutes(23) + Duration::seconds(1),
        session.prompt_cache_ttl()
    ));
    assert_eq!(
        reader.resolve_session_title(&thread_id).unwrap().as_deref(),
        Some("Neon Integration Run")
    );
    assert!(matches!(
        reader.resume_markers_for_session(&thread_id).unwrap()[0].status,
        ResumeStatus::Failed(_)
    ));

    let samples = reader.usage_samples_for(&Provider::Gemini, None).unwrap();
    assert_eq!(samples.len(), 5);

    drop(reader);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
