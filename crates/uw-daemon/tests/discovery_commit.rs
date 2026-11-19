use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use uw_core::{
    adapter::{AdapterError, AdapterResult, DiscoveredSession, HarnessAdapter},
    model::*,
};
use uw_daemon::{ObservationStore, SqliteDaemonStore, run_observation_tick};

struct NoAccount;
#[async_trait]
impl uw_adapters::codex::AppServerTransport for NoAccount {
    async fn call(&self, _: &str, _: serde_json::Value) -> AdapterResult<serde_json::Value> {
        Err(AdapterError::Unsupported)
    }
}

struct InterruptedStore {
    inner: SqliteDaemonStore,
    first: AtomicBool,
    commit_first: bool,
    entered: tokio::sync::Notify,
}
#[async_trait]
impl ObservationStore for InterruptedStore {
    async fn tracked_sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        ObservationStore::tracked_sessions(&self.inner).await
    }
    async fn record_usage(&self, sample: UsageSample) -> anyhow::Result<()> {
        self.inner.record_usage(sample).await
    }
    async fn record_stop(&self, id: &SessionId, reason: StopReason) -> anyhow::Result<()> {
        self.inner.record_stop(id, reason).await
    }
    async fn last_hook_event(&self, id: &SessionId) -> anyhow::Result<Option<String>> {
        self.inner.last_hook_event(id).await
    }
    async fn record_fetch_result(
        &self,
        provider: Provider,
        account: Option<AccountId>,
        result: Result<(), String>,
        at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.inner
            .record_fetch_result(provider, account, result, at)
            .await
    }
    async fn upsert_discovered_session(
        &self,
        provider: Provider,
        found: DiscoveredSession,
        account: Option<AccountId>,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let interrupt = self.first.swap(false, Ordering::SeqCst);
        if !interrupt || self.commit_first {
            self.inner
                .upsert_discovered_session(provider, found, account, now)
                .await?;
        }
        if interrupt {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

// why: cancellation can precede the SQLite commit or follow it before acknowledgement.
// Both paths must retry the record and persist exactly one copy through the production adapter wrapper.
#[tokio::test]
async fn cancelled_discovery_replays_until_committed_and_acknowledged() {
    for commit_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let id = "01a0b1b6-f024-7b73-a5fc-a6ea05f0a581";
        let path = dir.path().join(format!("rollout-{id}.jsonl"));
        std::fs::write(&path, format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"session_id\":\"{id}\",\"cwd\":\"/work\"}}}}\n{{\"type\":\"token_usage_record\",\"timestamp\":\"2026-09-17T23:32:34Z\",\"payload\":{{\"usage\":{{\"input_tokens\":10,\"total_tokens\":12}}}}}}\n"
        )).unwrap();
        let native: Arc<dyn HarnessAdapter> = Arc::new(
            uw_adapters::codex::CodexAdapter::new(Arc::new(NoAccount))
                .with_sessions_root(dir.path().to_owned()),
        );
        let wrapped: Arc<dyn HarnessAdapter> = Arc::new(
            uw_adapters::fallback::FallbackCompactionAdapter::new(native.clone(), native.clone()),
        );
        let adapters = HashMap::from([(Provider::Codex, wrapped)]);
        let store = InterruptedStore {
            inner: SqliteDaemonStore::new(uw_store::Store::open_memory().unwrap()),
            first: AtomicBool::new(true),
            commit_first,
            entered: tokio::sync::Notify::new(),
        };
        let mut tick = Box::pin(run_observation_tick(&store, &adapters));
        tokio::select! {
            _ = store.entered.notified() => {}
            result = tick.as_mut() => panic!("observation must reach the interrupted write: {result:?}"),
        }
        drop(tick);
        let retry = native.discover_sessions().await.unwrap();
        assert_eq!(retry[0].token_usage.len(), 1, "commit_first={commit_first}");
        run_observation_tick(&store, &adapters).await.unwrap();
        let records = store
            .inner
            .shared_store()
            .lock()
            .unwrap()
            .token_usage_for_session(&SessionId(id.into()))
            .unwrap();
        assert_eq!(records.len(), 1, "commit_first={commit_first}");
        assert_eq!(records[0].total_tokens, 12);
        assert!(
            native.discover_sessions().await.unwrap()[0]
                .token_usage
                .is_empty()
        );
    }
}

struct DiscoveryOnlyStore(SqliteDaemonStore);
#[async_trait]
impl ObservationStore for DiscoveryOnlyStore {
    async fn tracked_sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        ObservationStore::tracked_sessions(&self.0).await
    }
    async fn record_usage(&self, _: UsageSample) -> anyhow::Result<()> {
        Ok(())
    }
    async fn record_stop(&self, _: &SessionId, _: StopReason) -> anyhow::Result<()> {
        Ok(())
    }
    async fn last_hook_event(&self, _: &SessionId) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    async fn record_fetch_result(
        &self,
        _: Provider,
        _: Option<AccountId>,
        _: Result<(), String>,
        _: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        // Keep the WAL probe specific to discovery, excluding new poll timestamps.
        Ok(())
    }
    async fn upsert_discovered_session(
        &self,
        provider: Provider,
        found: DiscoveredSession,
        account: Option<AccountId>,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.0
            .upsert_discovered_session(provider, found, account, now)
            .await
    }
}

struct MultipleSources {
    found: std::sync::Mutex<Vec<DiscoveredSession>>,
    acknowledgements:
        std::sync::Mutex<Vec<(String, SessionId, Vec<uw_core::adapter::TokenUsageRecord>)>>,
}
#[async_trait]
impl HarnessAdapter for MultipleSources {
    fn provider(&self) -> Provider {
        Provider::ClaudeCode
    }
    fn capabilities(&self) -> uw_core::adapter::Capabilities {
        uw_core::adapter::Capabilities {
            can_trigger_compaction: false,
            can_advise_mid_turn: false,
            can_inject_at_session_start: false,
            can_observe_compaction: false,
            reports_token_counts: true,
            headless_resume: false,
            seed_modes: vec![],
        }
    }
    async fn emit_status(
        &self,
        _: &SessionId,
        _: uw_core::adapter::StatusEvent,
    ) -> AdapterResult<uw_core::adapter::DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }
    async fn advise(
        &self,
        _: &SessionId,
        _: &str,
    ) -> AdapterResult<uw_core::adapter::DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }
    async fn compact(
        &self,
        _: &SessionSummary,
        _: &CompactionRequest,
    ) -> AdapterResult<uw_core::adapter::DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }
    async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
        Err(AdapterError::Unsupported)
    }
    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        Err(AdapterError::Unsupported)
    }
    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        Ok(None)
    }
    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        Ok(self.found.lock().unwrap().clone())
    }
    async fn acknowledge_discovery(
        &self,
        path: &str,
        id: &SessionId,
        records: &[uw_core::adapter::TokenUsageRecord],
    ) -> AdapterResult<()> {
        self.acknowledgements
            .lock()
            .unwrap()
            .push((path.into(), id.clone(), records.to_vec()));
        Ok(())
    }
}

fn discovered(path: &str, at: Option<DateTime<Utc>>, tokens: u64) -> DiscoveredSession {
    DiscoveredSession {
        id: SessionId("parent".into()),
        lineage: Default::default(),
        cwd: path.into(),
        model: Some(ModelId(path.into())),
        context_window_size: Some(200_000),
        last_known_token_count: Some(tokens),
        last_known_context_pct: None,
        cache_ttl: None,
        first_seen: at,
        last_seen: at,
        state_path: Some(path.into()),
        title: Some(path.into()),
        token_usage: at
            .filter(|_| tokens > 0)
            .map(|at| uw_core::adapter::TokenUsageRecord {
                at,
                model: None,
                input_tokens: tokens - 2,
                cached_input_tokens: 0,
                cache_write_input_tokens: 0,
                output_tokens: 2,
                reasoning_output_tokens: 0,
                total_tokens: tokens,
                context_pct: None,
            })
            .into_iter()
            .collect(),
    }
}

fn truncate_wal(store: &DiscoveryOnlyStore) {
    store
        .0
        .shared_store()
        .lock()
        .unwrap()
        .connection()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
}

// why: CLAIM(discovery-summary): competing sidechain summaries must generate no
// WAL frames when unchanged, while every source's unique usage survives once.
// catches: writing each source separately, picking by directory order, or dropping losing sources' tokens.
#[tokio::test]
async fn discovery_coalesces_sources_without_replaying_summary_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("usage.db");
    let store = DiscoveryOnlyStore(SqliteDaemonStore::new(
        uw_store::Store::open(path.to_str().unwrap()).unwrap(),
    ));
    let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut older = discovered("/older", Some(at), 12);
    older.cache_ttl = Some(chrono::Duration::hours(1));
    let newer = discovered("/newer", Some(at + chrono::Duration::seconds(1)), 24);
    let duplicate = DiscoveredSession {
        last_seen: newer.last_seen,
        token_usage: older.token_usage.clone(),
        ..discovered("/duplicate", Some(at), 12)
    };
    let adapter = Arc::new(MultipleSources {
        found: std::sync::Mutex::new(vec![newer.clone(), older.clone(), duplicate.clone()]),
        acknowledgements: Default::default(),
    });
    let adapters = HashMap::from([(
        Provider::ClaudeCode,
        adapter.clone() as Arc<dyn HarnessAdapter>,
    )]);
    let first = run_observation_tick(&store, &adapters).await.unwrap();
    assert_eq!(
        first.sessions_discovered, 1,
        "discovery counts session IDs, not source files"
    );
    let shared = store.0.shared_store();
    {
        let db = shared.lock().unwrap();
        let summary = db.read_session(&newer.id).unwrap();
        assert_eq!(summary.state_path, newer.state_path);
        assert_eq!(summary.model, newer.model);
        assert_eq!(summary.last_seen, newer.last_seen.unwrap());
        assert_eq!(summary.first_seen, at);
        assert_eq!(summary.cache_ttl, Some(chrono::Duration::hours(1)));
        assert_eq!(db.resolve_session_title(&newer.id).unwrap(), newer.title);
        let records = db.token_usage_for_session(&newer.id).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records.iter().map(|r| r.total_tokens).sum::<u64>(), 36); // 12 + 24; duplicate 12 is ignored.
    }
    let wal = path.with_extension("db-wal");
    assert!(
        wal.metadata().unwrap().len() > 0,
        "positive control: inserts must move the WAL probe"
    );
    truncate_wal(&store);
    for _ in 0..2 {
        adapter.found.lock().unwrap().reverse();
        assert_eq!(
            run_observation_tick(&store, &adapters)
                .await
                .unwrap()
                .sessions_discovered,
            0
        );
        assert_eq!(
            wal.metadata().unwrap().len(),
            0,
            "unchanged discovery must not write WAL frames"
        );
    }
    let acks = adapter.acknowledgements.lock().unwrap();
    assert_eq!(acks.len(), 9);
    for source in [newer, older, duplicate] {
        assert_eq!(
            acks.iter()
                .filter(
                    |(path, id, records)| Some(path) == source.state_path.as_ref()
                        && id == &source.id
                        && records == &source.token_usage
                )
                .count(),
            3
        );
    }
}

// why: absent transcript timestamps describe unknown activity, not new activity at
// every poll. Retain the first fallback timestamp until dated evidence arrives.
// catches: found.last_seen.unwrap_or(now) on every discovery pass.
#[tokio::test]
async fn undated_discovery_does_not_refresh_activity_or_write_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("usage.db");
    let store = DiscoveryOnlyStore(SqliteDaemonStore::new(
        uw_store::Store::open(path.to_str().unwrap()).unwrap(),
    ));
    let found = discovered("/undated", None, 0);
    let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    store
        .upsert_discovered_session(Provider::ClaudeCode, found.clone(), None, at)
        .await
        .unwrap();
    truncate_wal(&store);
    store
        .upsert_discovered_session(
            Provider::ClaudeCode,
            found.clone(),
            None,
            at + chrono::Duration::hours(1),
        )
        .await
        .unwrap();
    let shared = store.0.shared_store();
    {
        let db = shared.lock().unwrap();
        assert_eq!(db.read_session(&found.id).unwrap().last_seen, at);
        assert!(db.session_last_seen_missing(&found.id).unwrap());
        assert_eq!(path.with_extension("db-wal").metadata().unwrap().len(), 0);
    }
    let dated = DiscoveredSession {
        last_seen: Some(at + chrono::Duration::hours(2)),
        ..found
    };
    store
        .upsert_discovered_session(
            Provider::ClaudeCode,
            dated.clone(),
            None,
            at + chrono::Duration::hours(3),
        )
        .await
        .unwrap();
    let db = shared.lock().unwrap();
    assert_eq!(
        db.read_session(&dated.id).unwrap().last_seen,
        dated.last_seen.unwrap()
    );
    assert!(!db.session_last_seen_missing(&dated.id).unwrap());
    assert!(path.with_extension("db-wal").metadata().unwrap().len() > 0);
}

// why: cancellation after a combined write must replay all sources through the
// uniqueness constraint before acknowledging any source, without losing usage.
#[tokio::test]
async fn combined_discovery_retries_all_sources_after_cancellation() {
    let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    for commit_first in [false, true] {
        let adapter = Arc::new(MultipleSources {
            found: std::sync::Mutex::new(vec![
                discovered("/first", Some(at), 12),
                discovered("/second", Some(at + chrono::Duration::seconds(1)), 24),
            ]),
            acknowledgements: Default::default(),
        });
        let adapters = HashMap::from([(
            Provider::ClaudeCode,
            adapter.clone() as Arc<dyn HarnessAdapter>,
        )]);
        let store = InterruptedStore {
            inner: SqliteDaemonStore::new(uw_store::Store::open_memory().unwrap()),
            first: AtomicBool::new(true),
            commit_first,
            entered: tokio::sync::Notify::new(),
        };
        let mut tick = Box::pin(run_observation_tick(&store, &adapters));
        tokio::select! {
            _ = store.entered.notified() => {}
            result = tick.as_mut() => panic!("must reach persistence: {result:?}"),
        }
        drop(tick);
        assert!(adapter.acknowledgements.lock().unwrap().is_empty());
        run_observation_tick(&store, &adapters).await.unwrap();
        let records = store
            .inner
            .shared_store()
            .lock()
            .unwrap()
            .token_usage_for_session(&SessionId("parent".into()))
            .unwrap();
        assert_eq!(records.len(), 2, "commit_first={commit_first}");
        assert_eq!(
            records
                .iter()
                .map(|record| record.total_tokens)
                .sum::<u64>(),
            36
        );
        assert_eq!(adapter.acknowledgements.lock().unwrap().len(), 2);
    }
}
