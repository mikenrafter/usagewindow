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
