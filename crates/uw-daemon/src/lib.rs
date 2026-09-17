//! Polling and delivery orchestration. Decisions are kept separate from I/O so
//! policy behavior can be tested with synthetic state and fake adapters/stores.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt};
use uw_core::adapter::{Capabilities, DeliveryOutcome, HarnessAdapter, StatusEvent};
use uw_core::model::*;
use uw_policy::{AdviseChannel, CacheCostObservation, WindowBlock};
use uw_store::Store;

pub const IDLE_COMPACT_PROMPT: &str = "/compact\nPreserve: the current goal, the step in progress and its exact next action, decisions already made and why, and every approach already tried and rejected.\nDiscard: file contents already read, superseded plans, and tool output that has been acted on.\nReason for compacting now: idle cache expiry";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionPlan {
    SkipUnsupported(String),
    WaitForIdle,
    ClaimAndSend,
}

pub fn plan_compaction_tick(
    _request: &CompactionRequest,
    capabilities: &Capabilities,
    idle: bool,
) -> CompactionPlan {
    if !capabilities.can_trigger_compaction {
        return CompactionPlan::SkipUnsupported(
            "adapter cannot honor destructive compaction requests".into(),
        );
    }
    if !idle {
        CompactionPlan::WaitForIdle
    } else {
        CompactionPlan::ClaimAndSend
    }
}

/// Placeholder liveness signal. Real harness-specific idle detection remains a
/// documented research gap; this trait prevents the daemon from inventing it.
#[async_trait]
pub trait SessionLivenessChecker: Send + Sync {
    async fn is_idle(&self, session: &SessionSummary) -> bool;
}

#[async_trait]
pub trait DaemonStore: Send + Sync {
    async fn pending_compactions(&self) -> anyhow::Result<Vec<CompactionRequest>>;
    async fn compaction_owner(&self, id: &SessionId) -> anyhow::Result<SessionSummary>;
    async fn claim_compaction(&self, id: uuid::Uuid) -> anyhow::Result<bool>;
    async fn update_compaction(
        &self,
        id: uuid::Uuid,
        status: CompactionStatus,
    ) -> anyhow::Result<()>;
    async fn enqueue_compaction(&self, request: CompactionRequest) -> anyhow::Result<()>;
    async fn usage_samples(&self, session: &SessionSummary) -> anyhow::Result<Vec<UsageSample>>;
    async fn has_active_resume_marker(&self, session_id: &SessionId) -> anyhow::Result<bool>;
    async fn insert_resume_marker(&self, marker: ResumeMarker) -> anyhow::Result<()>;
    async fn due_resume_markers(&self, now: DateTime<Utc>) -> anyhow::Result<Vec<ResumeMarker>>;
    async fn resume_owner(&self, session_id: &SessionId) -> anyhow::Result<SessionSummary>;
    async fn update_resume(&self, id: uuid::Uuid, status: ResumeStatus) -> anyhow::Result<()>;
}

/// Async facade for uw-store's `rusqlite::Connection` owner. Every operation
/// runs on a blocking worker, keeping SQLite off the Tokio executor.
pub struct SqliteDaemonStore {
    inner: Arc<Mutex<Store>>,
}
impl SqliteDaemonStore {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(Mutex::new(store)),
        }
    }
    async fn blocking<T, F>(&self, operation: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Store) -> anyhow::Result<T> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let guard = inner
                .lock()
                .map_err(|_| anyhow::anyhow!("store lock poisoned"))?;
            operation(&guard)
        })
        .await?
    }
}
#[async_trait]
impl DaemonStore for SqliteDaemonStore {
    async fn pending_compactions(&self) -> anyhow::Result<Vec<CompactionRequest>> {
        self.blocking(|s| Ok(s.pending_compaction_requests()?))
            .await
    }
    async fn compaction_owner(&self, id: &SessionId) -> anyhow::Result<SessionSummary> {
        let id = id.clone();
        self.blocking(move |s| Ok(s.read_session(&id)?)).await
    }
    async fn claim_compaction(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        self.blocking(move |s| Ok(s.claim_compaction(id)?)).await
    }
    async fn update_compaction(
        &self,
        id: uuid::Uuid,
        status: CompactionStatus,
    ) -> anyhow::Result<()> {
        self.blocking(move |s| Ok(s.update_compaction_status(id, status)?))
            .await
    }
    async fn enqueue_compaction(&self, request: CompactionRequest) -> anyhow::Result<()> {
        self.blocking(move |s| Ok(s.insert_compaction_request(&request)?))
            .await
    }
    async fn usage_samples(&self, session: &SessionSummary) -> anyhow::Result<Vec<UsageSample>> {
        let provider = session.harness.clone();
        let account = session.account.clone();
        self.blocking(move |s| {
            Ok(s.all_usage_samples()?
                .into_iter()
                .filter(|sample| sample.provider == provider && sample.account == account)
                .collect())
        })
        .await
    }
    async fn has_active_resume_marker(&self, id: &SessionId) -> anyhow::Result<bool> {
        let id = id.clone();
        self.blocking(move |s| Ok(s.has_active_resume_marker(&id)?))
            .await
    }
    async fn insert_resume_marker(&self, marker: ResumeMarker) -> anyhow::Result<()> {
        self.blocking(move |s| Ok(s.insert_resume_marker(&marker)?))
            .await
    }
    async fn due_resume_markers(&self, now: DateTime<Utc>) -> anyhow::Result<Vec<ResumeMarker>> {
        self.blocking(move |s| Ok(s.due_resume_markers(now)?)).await
    }
    async fn resume_owner(&self, id: &SessionId) -> anyhow::Result<SessionSummary> {
        let id = id.clone();
        self.blocking(move |s| Ok(s.read_session(&id)?)).await
    }
    async fn update_resume(&self, id: uuid::Uuid, status: ResumeStatus) -> anyhow::Result<()> {
        self.blocking(move |s| Ok(s.update_resume_status(id, status)?))
            .await
    }
}

pub async fn run_compaction_tick(
    store: &dyn DaemonStore,
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
    liveness: &dyn SessionLivenessChecker,
) -> anyhow::Result<()> {
    for request in store.pending_compactions().await? {
        let session = store.compaction_owner(&request.session_id).await?;
        let Some(adapter) = adapters.get(&session.harness) else {
            store
                .update_compaction(request.id, CompactionStatus::Failed("no adapter".into()))
                .await?;
            continue;
        };
        match plan_compaction_tick(
            &request,
            &adapter.capabilities(),
            liveness.is_idle(&session).await,
        ) {
            CompactionPlan::SkipUnsupported(reason) => {
                store
                    .update_compaction(request.id, CompactionStatus::Failed(reason))
                    .await?;
            }
            CompactionPlan::WaitForIdle => {}
            CompactionPlan::ClaimAndSend => {
                if !store.claim_compaction(request.id).await? {
                    continue;
                }
                let result = adapter.compact(&session.id, &request).await;
                let status = match result {
                    Ok(DeliveryOutcome::Delivered | DeliveryOutcome::QueuedForNextIdle) => {
                        CompactionStatus::Sent
                    }
                    Ok(DeliveryOutcome::Unsupported) => {
                        CompactionStatus::Failed("adapter reported unsupported".into())
                    }
                    Err(error) => CompactionStatus::Failed(error.to_string()),
                };
                store.update_compaction(request.id, status).await?;
            }
        }
    }
    Ok(())
}

pub async fn run_near_limit_tick(
    store: &dyn DaemonStore,
    adapter: &dyn HarnessAdapter,
    session: &SessionSummary,
    key: &WindowKey,
    profile: &ThresholdProfile,
    last_asked_pct: Option<f32>,
    asks_this_epoch: u32,
) -> anyhow::Result<NearLimitOutcome> {
    let samples = store.usage_samples(session).await?;
    let blocks = uw_policy::segment_blocks(&samples, key, session.account.as_ref());
    let Some(block) = blocks.last() else {
        return Ok(NearLimitOutcome::NoData);
    };
    let Some((_, current_pct)) = block.points.last() else {
        return Ok(NearLimitOutcome::NoData);
    };
    let Some(burn) = uw_policy::burn_rate_pct_per_hour(block, Duration::minutes(30)) else {
        return Ok(NearLimitOutcome::NoData);
    };
    if !uw_policy::should_trigger_near_limit(*current_pct, burn, profile)
        || !uw_policy::should_reask(last_asked_pct, asks_this_epoch, *current_pct, profile)
    {
        return Ok(NearLimitOutcome::NotTriggered);
    }
    let text = format!(
        "Usage is at {current_pct:.1}% and projected to exhaust at {:?}.",
        uw_policy::projected_exhaustion(block, Utc::now())
    );
    match uw_policy::advise_channel_for(&adapter.capabilities()) {
        AdviseChannel::MidTurn => {
            adapter.advise(&session.id, &text).await?;
            Ok(NearLimitOutcome::Advised)
        }
        AdviseChannel::QueuedAtSessionStart => {
            adapter
                .emit_status(&session.id, StatusEvent::Custom(text))
                .await?;
            Ok(NearLimitOutcome::Advised)
        }
        AdviseChannel::None => {
            // Codex currently has neither channel for passive advice. Preserve state and log.
            tracing::info!(session = %session.id.0, burn, "near-limit projected exhaustion; no advise channel");
            Ok(NearLimitOutcome::Checkpointed)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NearLimitOutcome {
    NoData,
    NotTriggered,
    Advised,
    Checkpointed,
}

pub struct IdleEpisodeTracker {
    enqueued: HashSet<SessionId>,
}
impl IdleEpisodeTracker {
    pub fn new() -> Self {
        Self {
            enqueued: HashSet::new(),
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn tick(
        &mut self,
        store: &dyn DaemonStore,
        session: &SessionSummary,
        now: DateTime<Utc>,
        idle: bool,
        cache_ttl: Duration,
        profile: &ThresholdProfile,
        adapter: &dyn HarnessAdapter,
    ) -> anyhow::Result<bool> {
        if !idle {
            self.enqueued.remove(&session.id);
            return Ok(false);
        }
        let Some(tokens) = session.last_known_token_count else {
            return Ok(false);
        };
        let Some(window) = session.context_window_size else {
            return Ok(false);
        };
        let should = uw_policy::should_idle_compact(
            now - session.last_seen,
            cache_ttl,
            tokens,
            window,
            &profile.idle_compact,
            &adapter.capabilities(),
        );
        if should && self.enqueued.insert(session.id.clone()) {
            store
                .enqueue_compaction(CompactionRequest {
                    id: uuid::Uuid::new_v4(),
                    session_id: session.id.clone(),
                    kind: CompactionKind::OpportunisticIdle,
                    prompt: IDLE_COMPACT_PROMPT.into(),
                    reason: "idle cache expiry".into(),
                    status: CompactionStatus::Pending,
                    created_at: Utc::now(),
                })
                .await?;
            return Ok(true);
        }
        Ok(false)
    }
}

pub async fn schedule_resume_if_needed(
    store: &dyn DaemonStore,
    session: &SessionSummary,
    block: &WindowBlock,
    now: DateTime<Utc>,
    profile: &ThresholdProfile,
    model: &str,
    history: &[CacheCostObservation],
) -> anyhow::Result<bool> {
    if store.has_active_resume_marker(&session.id).await? {
        return Ok(false);
    }
    let Some(marker) = plan_resume_marker(session, block, now, profile, model, history) else {
        return Ok(false);
    };
    store.insert_resume_marker(marker).await?;
    Ok(true)
}
impl Default for IdleEpisodeTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub fn plan_resume_marker(
    session: &SessionSummary,
    block: &WindowBlock,
    now: DateTime<Utc>,
    profile: &ThresholdProfile,
    model: &str,
    history: &[CacheCostObservation],
) -> Option<ResumeMarker> {
    if !matches!(session.stopped_reason, Some(StopReason::UsageLimit { .. })) {
        return None;
    }
    let (_, pct) = *block.points.last()?;
    let burn = uw_policy::burn_rate_pct_per_hour(block, Duration::minutes(30))? / 60.0;
    let cache = uw_policy::estimate_cache_write_pct(
        session.last_known_token_count.unwrap_or_default(),
        model,
        profile,
        history,
    );
    let lead = uw_policy::resume_lead_minutes(100.0 - pct, cache, burn, profile)?;
    Some(ResumeMarker {
        id: uuid::Uuid::new_v4(),
        session_id: session.id.clone(),
        reason: ResumeReason::AutoDetectedLimit,
        resume_at: Some(now + Duration::minutes(lead as i64)),
        created_at: now,
        status: ResumeStatus::Scheduled,
    })
}

pub async fn run_resume_tick(
    store: &dyn DaemonStore,
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    for marker in store.due_resume_markers(now).await? {
        let session = store.resume_owner(&marker.session_id).await?;
        let Some(adapter) = adapters.get(&session.harness) else {
            store
                .update_resume(marker.id, ResumeStatus::Failed("no adapter".into()))
                .await?;
            continue;
        };
        let result = adapter.resume_session(&session).await;
        store
            .update_resume(
                marker.id,
                match result {
                    Ok(()) => ResumeStatus::Fired,
                    Err(e) => ResumeStatus::Failed(e.to_string()),
                },
            )
            .await?;
    }
    Ok(())
}

/// Runs the destructive queue and resume scheduler on one shared cadence.
pub async fn run_polling_loop(
    store: &dyn DaemonStore,
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
    liveness: &dyn SessionLivenessChecker,
    interval: std::time::Duration,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        run_compaction_tick(store, adapters, liveness).await?;
        run_resume_tick(store, adapters, Utc::now()).await?;
    }
}

/// Entry point shared by the daemon binary and the Phase 6 CLI command. Service
/// configuration (SQLite path and adapter registry) is intentionally still a later
/// wiring concern; this preserves the same cadence/ownership point for Phase 7.
pub async fn run_daemon_loop() -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        ticker.tick().await;
        tracing::debug!("uw-daemon poll tick");
    }
}

/// Hook ingress is intentionally fail-open. The backend is a future routing seam;
/// TODO Phase-6: match harness-specific event names to daemon actions.
pub async fn handle_hook<R, F, Fut>(input: R, backend: F) -> serde_json::Value
where
    R: AsyncRead + Unpin,
    F: FnOnce(serde_json::Value) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<serde_json::Value>>,
{
    let mut bytes = Vec::new();
    let read = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        input.take(64 * 1024).read_to_end(&mut bytes),
    )
    .await;
    let Ok(Ok(_)) = read else {
        return serde_json::json!({});
    };
    let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return serde_json::json!({});
    };
    tracing::debug!(payload = %payload, "hook ingress");
    match tokio::time::timeout(std::time::Duration::from_millis(100), backend(payload)).await {
        Ok(Ok(response)) => response,
        _ => serde_json::json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use uw_core::adapter::{AdapterError, AdapterResult};

    fn request() -> CompactionRequest {
        CompactionRequest {
            id: uuid::Uuid::new_v4(),
            session_id: SessionId("s".into()),
            kind: CompactionKind::OpportunisticIdle,
            prompt: "p".into(),
            reason: "r".into(),
            status: CompactionStatus::Pending,
            created_at: Utc::now(),
        }
    }
    fn caps(compact: bool, advise: bool, start: bool) -> Capabilities {
        Capabilities {
            can_trigger_compaction: compact,
            can_advise_mid_turn: advise,
            can_inject_at_session_start: start,
            can_observe_compaction: true,
            reports_token_counts: true,
            headless_resume: true,
            seed_modes: vec![],
        }
    }
    fn session() -> SessionSummary {
        SessionSummary {
            id: SessionId("s".into()),
            harness: Provider::ClaudeCode,
            model: None,
            account: None,
            first_seen: Utc::now() - Duration::hours(1),
            last_seen: Utc::now() - Duration::minutes(1),
            cwd: "/tmp".into(),
            state_path: None,
            context_window_size: Some(200_000),
            last_known_token_count: Some(150_000),
            launch_mode: LaunchMode::Headless,
            pid: None,
            stopped_reason: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        }
    }

    struct FakeStore {
        request: StdMutex<Option<CompactionRequest>>,
        owner: SessionSummary,
        claim: bool,
        status: StdMutex<Vec<CompactionStatus>>,
        samples: Vec<UsageSample>,
        enqueues: Arc<StdMutex<u32>>,
    }
    #[async_trait]
    impl DaemonStore for FakeStore {
        async fn pending_compactions(&self) -> anyhow::Result<Vec<CompactionRequest>> {
            Ok(self.request.lock().unwrap().clone().into_iter().collect())
        }
        async fn compaction_owner(&self, _: &SessionId) -> anyhow::Result<SessionSummary> {
            Ok(self.owner.clone())
        }
        async fn claim_compaction(&self, _: uuid::Uuid) -> anyhow::Result<bool> {
            Ok(self.claim)
        }
        async fn update_compaction(
            &self,
            _: uuid::Uuid,
            status: CompactionStatus,
        ) -> anyhow::Result<()> {
            self.status.lock().unwrap().push(status);
            Ok(())
        }
        async fn enqueue_compaction(&self, _: CompactionRequest) -> anyhow::Result<()> {
            *self.enqueues.lock().unwrap() += 1;
            Ok(())
        }
        async fn usage_samples(&self, _: &SessionSummary) -> anyhow::Result<Vec<UsageSample>> {
            Ok(self.samples.clone())
        }
        async fn has_active_resume_marker(&self, _: &SessionId) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn insert_resume_marker(&self, _: ResumeMarker) -> anyhow::Result<()> {
            Ok(())
        }
        async fn due_resume_markers(&self, _: DateTime<Utc>) -> anyhow::Result<Vec<ResumeMarker>> {
            Ok(vec![])
        }
        async fn resume_owner(&self, _: &SessionId) -> anyhow::Result<SessionSummary> {
            Ok(self.owner.clone())
        }
        async fn update_resume(&self, _: uuid::Uuid, _: ResumeStatus) -> anyhow::Result<()> {
            Ok(())
        }
    }
    struct FakeAdapter {
        capabilities: Capabilities,
        compacted: Arc<StdMutex<u32>>,
        advised: Arc<StdMutex<u32>>,
    }
    #[async_trait]
    impl HarnessAdapter for FakeAdapter {
        fn provider(&self) -> Provider {
            Provider::ClaudeCode
        }
        fn capabilities(&self) -> Capabilities {
            self.capabilities.clone()
        }
        async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
            Err(AdapterError::Unsupported)
        }
        async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
            Ok(None)
        }
        async fn emit_status(
            &self,
            _: &SessionId,
            _: StatusEvent,
        ) -> AdapterResult<DeliveryOutcome> {
            Ok(DeliveryOutcome::Delivered)
        }
        async fn advise(&self, _: &SessionId, _: &str) -> AdapterResult<DeliveryOutcome> {
            *self.advised.lock().unwrap() += 1;
            Ok(DeliveryOutcome::Delivered)
        }
        async fn compact(
            &self,
            _: &SessionId,
            _: &CompactionRequest,
        ) -> AdapterResult<DeliveryOutcome> {
            *self.compacted.lock().unwrap() += 1;
            Ok(DeliveryOutcome::Delivered)
        }
        async fn resume_session(&self, _: &SessionSummary) -> AdapterResult<()> {
            Ok(())
        }
    }
    struct AlwaysIdle;
    #[async_trait]
    impl SessionLivenessChecker for AlwaysIdle {
        async fn is_idle(&self, _: &SessionSummary) -> bool {
            true
        }
    }

    #[test]
    fn unsupported_compaction_is_failed_without_send_plan() {
        assert_eq!(
            plan_compaction_tick(&request(), &caps(false, false, false), true),
            CompactionPlan::SkipUnsupported(
                "adapter cannot honor destructive compaction requests".into()
            )
        );
    }
    #[test]
    fn race_plan_requires_claim_before_send() {
        assert_eq!(
            plan_compaction_tick(&request(), &caps(true, false, false), true),
            CompactionPlan::ClaimAndSend
        );
    }
    #[tokio::test]
    async fn hook_backend_failure_is_fail_open() {
        let result = handle_hook(tokio::io::empty(), |_| async {
            Err(anyhow::anyhow!("down"))
        })
        .await;
        assert_eq!(result, serde_json::json!({}));
    }

    #[tokio::test]
    async fn unsupported_adapter_is_marked_failed_without_compact_call() {
        let compacted = Arc::new(StdMutex::new(0));
        let adapter = Arc::new(FakeAdapter {
            capabilities: caps(false, false, false),
            compacted: compacted.clone(),
            advised: Arc::new(StdMutex::new(0)),
        });
        let store = FakeStore {
            request: StdMutex::new(Some(request())),
            owner: session(),
            claim: true,
            status: StdMutex::new(vec![]),
            samples: vec![],
            enqueues: Arc::new(StdMutex::new(0)),
        };
        run_compaction_tick(
            &store,
            &HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]),
            &AlwaysIdle,
        )
        .await
        .unwrap();
        assert_eq!(*compacted.lock().unwrap(), 0);
        assert!(
            matches!(&store.status.lock().unwrap()[0], CompactionStatus::Failed(reason) if reason.contains("cannot honor"))
        );
    }

    #[tokio::test]
    async fn claim_race_skips_second_delivery_and_success_is_sent() {
        let compacted = Arc::new(StdMutex::new(0));
        let adapter = Arc::new(FakeAdapter {
            capabilities: caps(true, false, false),
            compacted: compacted.clone(),
            advised: Arc::new(StdMutex::new(0)),
        });
        let raced = FakeStore {
            request: StdMutex::new(Some(request())),
            owner: session(),
            claim: false,
            status: StdMutex::new(vec![]),
            samples: vec![],
            enqueues: Arc::new(StdMutex::new(0)),
        };
        run_compaction_tick(
            &raced,
            &HashMap::from([(
                Provider::ClaudeCode,
                adapter.clone() as Arc<dyn HarnessAdapter>,
            )]),
            &AlwaysIdle,
        )
        .await
        .unwrap();
        assert_eq!(*compacted.lock().unwrap(), 0);
        let sent = FakeStore {
            request: StdMutex::new(Some(request())),
            owner: session(),
            claim: true,
            status: StdMutex::new(vec![]),
            samples: vec![],
            enqueues: Arc::new(StdMutex::new(0)),
        };
        run_compaction_tick(
            &sent,
            &HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]),
            &AlwaysIdle,
        )
        .await
        .unwrap();
        assert_eq!(*compacted.lock().unwrap(), 1);
        assert!(matches!(
            sent.status.lock().unwrap()[0],
            CompactionStatus::Sent
        ));
    }

    #[tokio::test]
    async fn codex_shaped_near_limit_tick_never_calls_advise() {
        let now = Utc::now();
        let key = WindowKey {
            provider: Provider::Codex,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        let make = |at, pct| UsageSample {
            at,
            fetched_at: None,
            source: UsageSource::ProviderReported,
            provider: Provider::Codex,
            account: None,
            windows: HashMap::from([(
                key.clone(),
                UsageWindowState::new(pct, false, true, None, None),
            )]),
            credits: None,
        };
        let store = FakeStore {
            request: StdMutex::new(None),
            owner: session(),
            claim: true,
            status: StdMutex::new(vec![]),
            samples: vec![make(now - Duration::minutes(31), 0.0), make(now, 99.0)],
            enqueues: Arc::new(StdMutex::new(0)),
        };
        let adapter = FakeAdapter {
            capabilities: caps(false, false, false),
            compacted: Arc::new(StdMutex::new(0)),
            advised: Arc::new(StdMutex::new(0)),
        };
        let result = run_near_limit_tick(
            &store,
            &adapter,
            &session(),
            &key,
            &ThresholdProfile::default(),
            None,
            0,
        )
        .await
        .unwrap();
        assert_eq!(result, NearLimitOutcome::Checkpointed);
        assert_eq!(*adapter.advised.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn idle_compact_enqueues_once_per_episode_and_resets_when_active() {
        let now = Utc::now();
        let enqueues = Arc::new(StdMutex::new(0));
        let store = FakeStore {
            request: StdMutex::new(None),
            owner: session(),
            claim: true,
            status: StdMutex::new(vec![]),
            samples: vec![],
            enqueues: enqueues.clone(),
        };
        let adapter = FakeAdapter {
            capabilities: caps(true, false, false),
            compacted: Arc::new(StdMutex::new(0)),
            advised: Arc::new(StdMutex::new(0)),
        };
        let mut tracker = IdleEpisodeTracker::new();
        let profile = ThresholdProfile::default();
        assert!(
            tracker
                .tick(
                    &store,
                    &session(),
                    now,
                    true,
                    Duration::minutes(1),
                    &profile,
                    &adapter
                )
                .await
                .unwrap()
        );
        assert!(
            !tracker
                .tick(
                    &store,
                    &session(),
                    now,
                    true,
                    Duration::minutes(1),
                    &profile,
                    &adapter
                )
                .await
                .unwrap()
        );
        assert!(
            !tracker
                .tick(
                    &store,
                    &session(),
                    now,
                    false,
                    Duration::minutes(1),
                    &profile,
                    &adapter
                )
                .await
                .unwrap()
        );
        assert!(
            tracker
                .tick(
                    &store,
                    &session(),
                    now,
                    true,
                    Duration::minutes(1),
                    &profile,
                    &adapter
                )
                .await
                .unwrap()
        );
        assert_eq!(*enqueues.lock().unwrap(), 2);
    }

    struct ResumeFake {
        active: bool,
        inserts: StdMutex<u32>,
        owner: SessionSummary,
    }
    #[async_trait]
    impl DaemonStore for ResumeFake {
        async fn pending_compactions(&self) -> anyhow::Result<Vec<CompactionRequest>> {
            Ok(vec![])
        }
        async fn compaction_owner(&self, _: &SessionId) -> anyhow::Result<SessionSummary> {
            Ok(self.owner.clone())
        }
        async fn claim_compaction(&self, _: uuid::Uuid) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn update_compaction(
            &self,
            _: uuid::Uuid,
            _: CompactionStatus,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn enqueue_compaction(&self, _: CompactionRequest) -> anyhow::Result<()> {
            Ok(())
        }
        async fn usage_samples(&self, _: &SessionSummary) -> anyhow::Result<Vec<UsageSample>> {
            Ok(vec![])
        }
        async fn has_active_resume_marker(&self, _: &SessionId) -> anyhow::Result<bool> {
            Ok(self.active)
        }
        async fn insert_resume_marker(&self, _: ResumeMarker) -> anyhow::Result<()> {
            *self.inserts.lock().unwrap() += 1;
            Ok(())
        }
        async fn due_resume_markers(&self, _: DateTime<Utc>) -> anyhow::Result<Vec<ResumeMarker>> {
            Ok(vec![])
        }
        async fn resume_owner(&self, _: &SessionId) -> anyhow::Result<SessionSummary> {
            Ok(self.owner.clone())
        }
        async fn update_resume(&self, _: uuid::Uuid, _: ResumeStatus) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn resume_scheduler_does_not_attempt_duplicate_active_marker() {
        let mut stopped = session();
        let key = WindowKey {
            provider: Provider::ClaudeCode,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        stopped.stopped_reason = Some(StopReason::UsageLimit {
            window: key.clone(),
        });
        let block = WindowBlock {
            key,
            account: None,
            started_at: Utc::now() - Duration::minutes(31),
            resets_at: None,
            points: vec![
                (Utc::now() - Duration::minutes(31), 0.0),
                (Utc::now(), 50.0),
            ],
            point_resets_at: vec![None, None],
        };
        let store = ResumeFake {
            active: true,
            inserts: StdMutex::new(0),
            owner: stopped.clone(),
        };
        assert!(
            !schedule_resume_if_needed(
                &store,
                &stopped,
                &block,
                Utc::now(),
                &ThresholdProfile::default(),
                "claude",
                &[]
            )
            .await
            .unwrap()
        );
        assert_eq!(*store.inserts.lock().unwrap(), 0);
    }
}
