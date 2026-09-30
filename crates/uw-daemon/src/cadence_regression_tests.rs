use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use uw_core::adapter::AdapterResult;

struct SequenceAdapter {
    sample: UsageSample,
    next_pct: Option<f32>,
    fetches: AtomicUsize,
    discoveries: AtomicUsize,
    compactions: AtomicUsize,
    slow_discovery: bool,
    block_initial_fetch: bool,
    panic_second_fetch: bool,
    auth_failure: bool,
    initial_fetch_release: Option<Arc<tokio::sync::Notify>>,
    compact: bool,
}

impl SequenceAdapter {
    fn new(provider: Provider) -> Self {
        let now = Utc::now();
        Self {
            sample: UsageSample {
                at: now,
                fetched_at: Some(now),
                source: UsageSource::ProviderReported,
                provider: provider.clone(),
                account: None,
                plan: None,
                windows: HashMap::from([(
                    WindowKey {
                        provider,
                        kind: WindowKind::Rolling { minutes: 300 },
                    },
                    UsageWindowState::new(95.0, false, true, None, None),
                )]),
                credits: None,
            },
            next_pct: None,
            fetches: AtomicUsize::new(0),
            discoveries: AtomicUsize::new(0),
            compactions: AtomicUsize::new(0),
            slow_discovery: false,
            block_initial_fetch: false,
            panic_second_fetch: false,
            auth_failure: false,
            initial_fetch_release: None,
            compact: false,
        }
    }
}

#[async_trait]
impl HarnessAdapter for SequenceAdapter {
    fn provider(&self) -> Provider {
        self.sample.provider.clone()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            can_trigger_compaction: self.compact,
            can_advise_mid_turn: false,
            can_inject_at_session_start: false,
            can_observe_compaction: false,
            reports_token_counts: true,
            headless_resume: false,
            seed_modes: vec![],
        }
    }
    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        let index = self.fetches.fetch_add(1, Ordering::SeqCst);
        if index == 0
            && let Some(release) = &self.initial_fetch_release
        {
            release.notified().await;
        }
        assert!(
            !(index == 1 && self.panic_second_fetch),
            "synthetic provider fetch panic"
        );
        if index == 0 && self.block_initial_fetch {
            tokio::sync::Notify::new().notified().await;
        }
        if self.auth_failure {
            return Err(AdapterError::Auth);
        }
        let mut sample = self.sample.clone();
        if index > 0
            && let Some(pct) = self.next_pct
        {
            sample.at = Utc::now();
            sample.fetched_at = Some(sample.at);
            for window in sample.windows.values_mut() {
                window.pct = pct;
            }
        }
        Ok(sample)
    }
    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        self.discoveries.fetch_add(1, Ordering::SeqCst);
        if self.slow_discovery {
            tokio::time::sleep(StdDuration::from_secs(5)).await;
        }
        Ok(vec![])
    }
    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        Ok(None)
    }
    async fn emit_status(&self, _: &SessionId, _: StatusEvent) -> AdapterResult<DeliveryOutcome> {
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
        self.compactions.fetch_add(1, Ordering::SeqCst);
        Ok(DeliveryOutcome::Delivered)
    }
    async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
        Err(AdapterError::Unsupported)
    }
}

struct Idle;
#[async_trait]
impl SessionLivenessChecker for Idle {
    async fn is_idle(&self, _: &SessionSummary) -> bool {
        true
    }
}

fn setup(adapter: &SequenceAdapter, age: i64) -> (PathBuf, Arc<SqliteDaemonStore>) {
    let path =
        std::env::temp_dir().join(format!("uw-cadence-regression-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&path.to_string_lossy()).unwrap();
    let mut session = crate::tests::session();
    session.harness = adapter.provider();
    session.last_seen = Utc::now() - Duration::seconds(age);
    store.insert_session(&session).unwrap();
    store.insert_usage_sample(&adapter.sample).unwrap();
    (path, Arc::new(SqliteDaemonStore::new(store)))
}

fn launch(
    store: Arc<SqliteDaemonStore>,
    adapter: Arc<SequenceAdapter>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let adapters = HashMap::from([(adapter.provider(), adapter as Arc<dyn HarnessAdapter>)]);
        run_production_ticks(store, &adapters, &Idle, StdDuration::from_secs(60)).await
    })
}

async fn cleanup(task: tokio::task::JoinHandle<anyhow::Result<()>>, path: PathBuf) {
    task.abort();
    let _ = task.await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

async fn await_count(
    counter: &AtomicUsize,
    count: usize,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(StdDuration::from_millis(2300), async {
        while counter.load(Ordering::SeqCst) < count {
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
}

#[tokio::test]
async fn production_deescalates_after_fresh_usage_falls_below_sensitive_band() {
    let mut adapter = SequenceAdapter::new(Provider::Other("deescalation-probe".into()));
    adapter.next_pct = Some(20.0);
    let (path, store) = setup(&adapter, 0);
    let adapter = Arc::new(adapter);
    let task = launch(store, adapter.clone());
    let accelerated = await_count(&adapter.fetches, 2).await;
    tokio::time::sleep(StdDuration::from_millis(1500)).await;
    cleanup(task, path).await;
    assert!(
        accelerated.is_ok(),
        "the initial 95% sample should accelerate polling"
    );
    assert_eq!(
        adapter.fetches.load(Ordering::SeqCst),
        2,
        "fresh 20% usage should return to the 60s baseline"
    );
    assert_eq!(adapter.discoveries.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn slow_session_discovery_does_not_block_sensitive_usage_sampling() {
    let mut adapter = SequenceAdapter::new(Provider::Other("slow-discovery-probe".into()));
    adapter.slow_discovery = true;
    let (path, store) = setup(&adapter, 0);
    let adapter = Arc::new(adapter);
    let task = launch(store, adapter.clone());
    let result = await_count(&adapter.fetches, 2).await;
    cleanup(task, path).await;
    assert!(
        result.is_ok(),
        "a 5s disk discovery must not block the 1s usage cadence"
    );
    assert_eq!(adapter.discoveries.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn slow_session_discovery_does_not_block_idle_compaction_delivery() {
    let mut adapter = SequenceAdapter::new(Provider::ClaudeCode);
    adapter.slow_discovery = true;
    adapter.compact = true;
    let (path, store) = setup(&adapter, 269);
    let adapter = Arc::new(adapter);
    let task = launch(store, adapter.clone());
    let result = await_count(&adapter.compactions, 1).await;
    cleanup(task, path).await;
    assert!(
        result.is_ok(),
        "disk discovery must not hold a 4m29s compaction past cache expiry"
    );
    assert_eq!(adapter.compactions.load(Ordering::SeqCst), 1);
}

fn provider_fixture() -> (UsageSample, SessionSummary) {
    let adapter = SequenceAdapter::new(Provider::ClaudeCode);
    let mut session = crate::tests::session();
    session.last_seen = Utc::now();
    (adapter.sample, session)
}

fn interval_for(sample: &UsageSample, session: &SessionSummary) -> StdDuration {
    cadence::provider_interval(
        StdDuration::from_secs(60),
        &Provider::ClaudeCode,
        std::slice::from_ref(sample),
        std::slice::from_ref(session),
        Utc::now(),
    )
}

#[test]
fn provider_cadence_matches_known_accounts_and_allows_unreconciled_session_accounts() {
    let (mut sample, mut session) = provider_fixture();
    sample.account = Some(AccountId("quota-account".into()));
    session.account = Some(AccountId("different-account".into()));
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
    session.account = sample.account.clone();
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(1));
    session.account = None;
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(1));
}

#[test]
fn provider_cadence_ignores_inactive_expired_and_unrelated_windows() {
    let (mut sample, session) = provider_fixture();
    for window in sample.windows.values_mut() {
        window.active = false;
    }
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
    for window in sample.windows.values_mut() {
        window.active = true;
        window.resets_at = Some(Utc::now() - Duration::seconds(1));
    }
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
    let window = sample.windows.values().next().unwrap().clone();
    sample.windows = HashMap::from([(
        WindowKey {
            provider: Provider::Codex,
            kind: WindowKind::Rolling { minutes: 300 },
        },
        UsageWindowState {
            resets_at: None,
            ..window
        },
    )]);
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
}

#[test]
fn provider_cadence_uses_window_account_scope_when_present() {
    let (mut sample, mut session) = provider_fixture();
    sample.account = Some(AccountId("parent-account".into()));
    session.account = Some(AccountId("parent-account".into()));
    for window in sample.windows.values_mut() {
        window.scope = Some(AccountId("other-account".into()));
    }
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
    session.account = Some(AccountId("other-account".into()));
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(1));
}

#[test]
fn provider_cadence_requires_current_live_unsuperseded_sessions() {
    let (sample, mut session) = provider_fixture();
    session.stopped_reason = Some(StopReason::UserQuit);
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
    session.stopped_reason = None;
    session.superseded_by = Some(SessionId("successor".into()));
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
    session.superseded_by = None;
    session.last_seen = Utc::now() - Duration::minutes(6);
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
    session.last_seen = Utc::now();
    session.harness = Provider::Codex;
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
    session.harness = Provider::ClaudeCode;
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(1));
}

#[test]
fn provider_cadence_tracks_the_tightest_current_window_and_recovers_after_reset() {
    let (mut sample, session) = provider_fixture();
    for window in sample.windows.values_mut() {
        window.pct = 85.0;
    }
    sample.windows.insert(
        WindowKey {
            provider: Provider::ClaudeCode,
            kind: WindowKind::Custom("weekly".into()),
        },
        UsageWindowState::new(90.0, false, true, None, None),
    );
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(5));
    sample.windows.insert(
        WindowKey {
            provider: Provider::ClaudeCode,
            kind: WindowKind::Custom("third-window".into()),
        },
        UsageWindowState::new(95.0, false, true, None, None),
    );
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(1));
    for window in sample.windows.values_mut() {
        window.pct = 20.0;
        window.exceeded = false;
    }
    assert_eq!(interval_for(&sample, &session), StdDuration::from_secs(60));
}

#[tokio::test]
async fn production_startup_preserves_sensitive_windows_from_separate_recent_samples() {
    let mut adapter = SequenceAdapter::new(Provider::Other("multi-window-boot".into()));
    let mut sensitive = adapter.sample.clone();
    sensitive.at -= Duration::seconds(1);
    sensitive.fetched_at = Some(sensitive.at);
    adapter.sample.windows = HashMap::from([(
        WindowKey {
            provider: adapter.provider(),
            kind: WindowKind::Custom("weekly".into()),
        },
        UsageWindowState::new(20.0, false, true, None, None),
    )]);
    adapter.block_initial_fetch = true;
    let (path, store) = setup(&adapter, 0);
    store
        .shared_store()
        .lock()
        .unwrap()
        .insert_usage_sample(&sensitive)
        .unwrap();
    let adapter = Arc::new(adapter);
    let task = launch(store, adapter.clone());
    let result = await_count(&adapter.fetches, 2).await;
    cleanup(task, path).await;
    assert!(
        result.is_ok(),
        "boot history must merge the sensitive rolling window and bound the first hanging fetch to 1s"
    );
}

#[tokio::test]
async fn production_replaces_prior_account_quota_cache_when_provider_account_changes() {
    let mut adapter = SequenceAdapter::new(Provider::Other("account-change-probe".into()));
    let mut old_account_sample = adapter.sample.clone();
    old_account_sample.account = Some(AccountId("old-account".into()));
    old_account_sample.at -= Duration::seconds(1);
    old_account_sample.fetched_at = Some(old_account_sample.at);
    adapter.sample.account = Some(AccountId("new-account".into()));
    for window in adapter.sample.windows.values_mut() {
        window.pct = 20.0;
    }
    let (path, store) = setup(&adapter, 0);
    store
        .shared_store()
        .lock()
        .unwrap()
        .insert_usage_sample(&old_account_sample)
        .unwrap();
    let adapter = Arc::new(adapter);
    let task = launch(store, adapter.clone());
    let started = await_count(&adapter.fetches, 1).await;
    tokio::time::sleep(StdDuration::from_millis(1500)).await;
    cleanup(task, path).await;
    assert!(
        started.is_ok(),
        "startup observation should poll the provider"
    );
    assert_eq!(
        adapter.fetches.load(Ordering::SeqCst),
        1,
        "the old account's sensitive cache must stop driving new account polls"
    );
}

struct GatedDiscoveryAdapter {
    inner: SequenceAdapter,
    discovery_entered: tokio::sync::Notify,
    discovery_release: tokio::sync::Notify,
    stop_samples: Mutex<Vec<Option<UsageSample>>>,
}

#[async_trait]
impl HarnessAdapter for GatedDiscoveryAdapter {
    fn provider(&self) -> Provider {
        self.inner.provider()
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn fetch_usage(&self, account: Option<&AccountId>) -> AdapterResult<UsageSample> {
        self.inner.fetch_usage(account).await
    }
    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        self.discovery_entered.notify_one();
        self.discovery_release.notified().await;
        Ok(vec![])
    }
    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        Ok(None)
    }
    async fn detect_stop_with_usage(
        &self,
        _: &SessionId,
        sample: Option<&UsageSample>,
    ) -> AdapterResult<Option<StopReason>> {
        self.stop_samples.lock().unwrap().push(sample.cloned());
        Ok(None)
    }
    async fn emit_status(&self, _: &SessionId, _: StatusEvent) -> AdapterResult<DeliveryOutcome> {
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
        Err(AdapterError::Unsupported)
    }
    async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
        Err(AdapterError::Unsupported)
    }
}

#[tokio::test]
async fn stop_checks_after_slow_discovery_use_the_newest_concurrent_usage_sample() {
    let mut inner = SequenceAdapter::new(Provider::ClaudeCode);
    inner.next_pct = Some(20.0);
    let (path, store) = setup(&inner, 0);
    let adapter = Arc::new(GatedDiscoveryAdapter {
        inner,
        discovery_entered: tokio::sync::Notify::new(),
        discovery_release: tokio::sync::Notify::new(),
        stop_samples: Mutex::new(vec![]),
    });
    let runtime = Arc::new(ObservationRuntime::default());
    let observation = {
        let store = store.clone();
        let adapter = adapter.clone();
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let adapters =
                HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]);
            run_observation_tick_cached(store.as_ref(), &adapters, &runtime, &HashMap::new(), None)
                .await
        })
    };
    tokio::time::timeout(
        StdDuration::from_secs(1),
        adapter.discovery_entered.notified(),
    )
    .await
    .expect("the full observation should reach discovery");
    let (_, fresh) = poll_provider(
        store.as_ref(),
        adapter.as_ref(),
        &runtime,
        Some(StdDuration::from_secs(1)),
        None,
    )
    .await
    .unwrap();
    assert!(
        fresh
            .unwrap()
            .windows
            .values()
            .all(|window| window.pct == 20.0)
    );
    adapter.discovery_release.notify_one();
    tokio::time::timeout(StdDuration::from_secs(1), observation)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let observed = adapter.stop_samples.lock().unwrap().clone();
    drop(store);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    assert_eq!(
        observed.len(),
        1,
        "the tracked warm session should receive one stop check"
    );
    assert!(
        observed[0]
            .as_ref()
            .unwrap()
            .windows
            .values()
            .all(|window| window.pct == 20.0),
        "a reset observed during discovery must replace the old 95% stop-check snapshot"
    );
}

#[tokio::test]
async fn successful_full_usage_snapshot_evicts_windows_omitted_by_the_provider() {
    let mut adapter = SequenceAdapter::new(Provider::ClaudeCode);
    let old = adapter.sample.clone();
    adapter.sample.windows = HashMap::from([(
        WindowKey {
            provider: Provider::ClaudeCode,
            kind: WindowKind::Custom("weekly".into()),
        },
        UsageWindowState::new(20.0, false, true, None, None),
    )]);
    let (path, store) = setup(&adapter, 0);
    let runtime = ObservationRuntime::default();
    runtime
        .samples
        .lock()
        .unwrap()
        .insert((Provider::ClaudeCode, None), old);
    poll_provider(
        store.as_ref(),
        &adapter,
        &runtime,
        Some(StdDuration::from_secs(1)),
        None,
    )
    .await
    .unwrap();
    let cached = runtime.samples();
    let mut session = crate::tests::session();
    session.last_seen = Utc::now();
    assert_eq!(cached.len(), 1);
    assert_eq!(
        cached[0].windows, adapter.sample.windows,
        "a verified provider snapshot replaces windows absent from that response"
    );
    assert_eq!(
        cadence::provider_interval(
            StdDuration::from_secs(60),
            &Provider::ClaudeCode,
            &cached,
            &[session],
            Utc::now()
        ),
        StdDuration::from_secs(60)
    );
    drop(store);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn production_recovers_sensitive_polling_after_a_background_fetch_panics() {
    let mut adapter = SequenceAdapter::new(Provider::Other("fetch-panic-probe".into()));
    adapter.panic_second_fetch = true;
    let (path, store) = setup(&adapter, 0);
    let adapter = Arc::new(adapter);
    let task = launch(store, adapter.clone());
    let result = tokio::time::timeout(StdDuration::from_millis(3500), async {
        while adapter.fetches.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await;
    cleanup(task, path).await;
    assert!(
        result.is_ok(),
        "a panicking background fetch must release provider scheduling so the next sensitive poll can run"
    );
}

struct IdleAfter {
    started: Instant,
}
#[async_trait]
impl SessionLivenessChecker for IdleAfter {
    async fn is_idle(&self, _: &SessionSummary) -> bool {
        self.started.elapsed() >= StdDuration::from_millis(500)
    }
}

#[tokio::test]
async fn production_wakes_at_four_thirty_when_a_previously_busy_session_becomes_idle() {
    let mut adapter = SequenceAdapter::new(Provider::ClaudeCode);
    adapter.compact = true;
    for window in adapter.sample.windows.values_mut() {
        window.pct = 20.0;
    }
    let (path, store) = setup(&adapter, 269);
    let adapter = Arc::new(adapter);
    let task = {
        let adapter = adapter.clone();
        tokio::spawn(async move {
            let adapters =
                HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]);
            let liveness = IdleAfter {
                started: Instant::now(),
            };
            run_production_ticks(store, &adapters, &liveness, StdDuration::from_secs(60)).await
        })
    };
    let result = await_count(&adapter.compactions, 1).await;
    cleanup(task, path).await;
    assert!(
        result.is_ok(),
        "the 4m30s checkpoint must revisit initial busy liveness and compact without waiting for the 60s base cadence"
    );
    assert_eq!(adapter.compactions.load(Ordering::SeqCst), 1);
    assert_eq!(
        adapter.fetches.load(Ordering::SeqCst),
        1,
        "the idle checkpoint must not trigger a provider quota fetch"
    );
}

#[tokio::test]
async fn full_observation_auth_failure_does_not_reuse_cached_quota_for_stop_checks() {
    let mut inner = SequenceAdapter::new(Provider::ClaudeCode);
    let old = inner.sample.clone();
    let (path, store) = setup(&inner, 0);
    inner.auth_failure = true;
    let adapter = Arc::new(GatedDiscoveryAdapter {
        inner,
        discovery_entered: tokio::sync::Notify::new(),
        discovery_release: tokio::sync::Notify::new(),
        stop_samples: Mutex::new(vec![]),
    });
    adapter.discovery_release.notify_one();
    let runtime = ObservationRuntime::default();
    runtime
        .samples
        .lock()
        .unwrap()
        .insert((Provider::ClaudeCode, None), old);
    let adapters = HashMap::from([(
        Provider::ClaudeCode,
        adapter.clone() as Arc<dyn HarnessAdapter>,
    )]);
    let report =
        run_observation_tick_cached(store.as_ref(), &adapters, &runtime, &HashMap::new(), None)
            .await
            .unwrap();
    let observed = adapter.stop_samples.lock().unwrap().clone();
    drop(store);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    assert_eq!(report.adapter_errors, 1);
    assert_eq!(
        observed,
        vec![None],
        "a hard auth failure must not let prior cached quota corroborate a stop"
    );
}

struct RejectUsageStore;
#[async_trait]
impl ObservationStore for RejectUsageStore {
    async fn tracked_sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        Ok(vec![])
    }
    async fn record_usage(&self, _: UsageSample) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("synthetic write failure"))
    }
    async fn record_stop(&self, _: &SessionId, _: StopReason) -> anyhow::Result<()> {
        Ok(())
    }
    async fn last_hook_event(&self, _: &SessionId) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    async fn upsert_discovered_session(
        &self,
        _: Provider,
        _: DiscoveredSession,
        _: Option<AccountId>,
        _: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn record_fetch_result(
        &self,
        _: Provider,
        _: Option<AccountId>,
        _: Result<(), String>,
        _: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn failed_usage_persistence_keeps_the_prior_successful_runtime_snapshot() {
    let mut adapter = SequenceAdapter::new(Provider::ClaudeCode);
    let mut old = adapter.sample.clone();
    old.account = Some(AccountId("old-account".into()));
    adapter.sample.account = Some(AccountId("new-account".into()));
    for window in adapter.sample.windows.values_mut() {
        window.pct = 20.0;
    }
    let runtime = ObservationRuntime::default();
    runtime
        .samples
        .lock()
        .unwrap()
        .insert((Provider::ClaudeCode, old.account.clone()), old.clone());
    assert!(
        poll_provider(
            &RejectUsageStore,
            &adapter,
            &runtime,
            Some(StdDuration::from_secs(1)),
            None
        )
        .await
        .is_err()
    );
    assert_eq!(
        runtime.samples(),
        vec![old],
        "a failed SQLite write must preserve the last successful account and quota snapshot"
    );
}

struct ReleaseQuotaAfterInitialPolicy {
    release: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl SessionLivenessChecker for ReleaseQuotaAfterInitialPolicy {
    async fn is_idle(&self, _: &SessionSummary) -> bool {
        self.release.notify_one();
        true
    }
}

#[tokio::test]
async fn fresh_sensitive_usage_triggers_policy_while_session_discovery_is_pending() {
    let mut adapter = SequenceAdapter::new(Provider::ClaudeCode);
    adapter.compact = true;
    adapter.slow_discovery = true;
    for window in adapter.sample.windows.values_mut() {
        window.pct = 20.0;
    }
    let (path, store) = setup(&adapter, 0);
    for window in adapter.sample.windows.values_mut() {
        window.pct = 95.0;
    }
    adapter.sample.at += Duration::milliseconds(1);
    adapter.sample.fetched_at = Some(adapter.sample.at);
    let release = Arc::new(tokio::sync::Notify::new());
    adapter.initial_fetch_release = Some(release.clone());
    let adapter = Arc::new(adapter);
    let task = {
        let adapter = adapter.clone();
        tokio::spawn(async move {
            let adapters =
                HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]);
            let liveness = ReleaseQuotaAfterInitialPolicy { release };
            run_production_ticks(store, &adapters, &liveness, StdDuration::from_secs(60)).await
        })
    };
    let result = await_count(&adapter.compactions, 1).await;
    cleanup(task, path).await;
    assert!(
        result.is_ok(),
        "a fresh 95% sample must trigger compaction while discovery is pending, even when later fast polls return an identical sample"
    );
    assert_eq!(adapter.discoveries.load(Ordering::SeqCst), 1);
    assert_eq!(adapter.compactions.load(Ordering::SeqCst), 1);
}
