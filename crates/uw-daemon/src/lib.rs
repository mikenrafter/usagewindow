//! Polling and delivery orchestration. Decisions are kept separate from I/O so
//! policy behavior can be tested with synthetic state and fake adapters/stores.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::path::{Path, PathBuf};
use std::time::{Duration as StdDuration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use uw_core::adapter::{
    AdapterError, Capabilities, DeliveryOutcome, DiscoveredSession, HarnessAdapter, SeedContext,
    SeedMode, StatusEvent, TokenUsageRecord,
};
use uw_core::model::*;
use uw_core::summarizer::{SummarizeTemplate, Summarizer};
use uw_policy::{AdviseChannel, CacheCostObservation, WindowBlock};
use uw_store::Store;

struct StoreHookChannel {
    db_path: String,
}

#[cfg(test)]
mod paseo_messenger_tests {
    use super::*;

    #[test]
    fn resolves_paseo_agent_from_claude_session_id() {
        let root = std::env::temp_dir().join(format!(
            "usagewindow-paseo-test-{}",
            uuid::Uuid::new_v4()
        ));
        let agents = root.join("agents/project");
        std::fs::create_dir_all(&agents).unwrap();
        std::fs::write(
            agents.join("agent.json"),
            r#"{"id":"paseo-agent","runtimeInfo":{"sessionId":"claude-session"}}"#,
        )
        .unwrap();

        assert_eq!(
            resolve_paseo_agent_id(&root, &SessionId("claude-session".into())).unwrap(),
            Some("paseo-agent".into())
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn does_not_resolve_an_unrelated_claude_session() {
        let root = std::env::temp_dir().join(format!(
            "usagewindow-paseo-test-{}",
            uuid::Uuid::new_v4()
        ));
        let agents = root.join("agents/project");
        std::fs::create_dir_all(&agents).unwrap();
        std::fs::write(
            agents.join("agent.json"),
            r#"{"id":"paseo-agent","persistence":{"sessionId":"other-session"}}"#,
        )
        .unwrap();

        assert_eq!(
            resolve_paseo_agent_id(&root, &SessionId("claude-session".into())).unwrap(),
            None
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn resolve_paseo_agent_id(home: &Path, session_id: &SessionId) -> anyhow::Result<Option<String>> {
    fn visit(dir: &Path, session_id: &str) -> anyhow::Result<Option<String>> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(id) = visit(&path, session_id)? {
                    return Ok(Some(id));
                }
                continue;
            }
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let value: serde_json::Value = match serde_json::from_str(&std::fs::read_to_string(&path)?) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let matches = value
                .pointer("/runtimeInfo/sessionId")
                .or_else(|| value.pointer("/persistence/sessionId"))
                .and_then(serde_json::Value::as_str)
                == Some(session_id);
            if matches
                && let Some(id) = value.get("id").and_then(serde_json::Value::as_str)
            {
                return Ok(Some(id.to_owned()));
            }
        }
        Ok(None)
    }

    let agents = home.join("agents");
    if !agents.is_dir() {
        return Ok(None);
    }
    visit(&agents, &session_id.0)
}

#[derive(Clone)]
struct PaseoSessionMessenger {
    home: PathBuf,
    program: String,
}

impl PaseoSessionMessenger {
    fn from_environment() -> Self {
        let home = std::env::var_os("PASEO_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".paseo")))
            .unwrap_or_else(|| PathBuf::from(".paseo"));
        Self {
            home,
            program: std::env::var("UW_PASEO_CLI").unwrap_or_else(|_| "paseo".into()),
        }
    }
}

#[async_trait]
impl uw_adapters::claude_code::SessionMessenger for PaseoSessionMessenger {
    async fn send(
        &self,
        session_id: &SessionId,
        text: &str,
    ) -> uw_core::adapter::AdapterResult<DeliveryOutcome> {
        let home = self.home.clone();
        let session_id = session_id.clone();
        let agent_id = tokio::task::spawn_blocking(move || resolve_paseo_agent_id(&home, &session_id))
            .await
            .map_err(|error| AdapterError::Other(error.to_string()))?
            .map_err(|error| AdapterError::Other(error.to_string()))?
            .ok_or(AdapterError::Unsupported)?;
        let output = tokio::process::Command::new(&self.program)
            .args(["send", "--no-wait", &agent_id, text])
            .output()
            .await
            .map_err(|error| AdapterError::Transient(error.to_string()))?;
        if !output.status.success() {
            return Err(AdapterError::Other(format!(
                "Paseo send failed for agent {agent_id}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(DeliveryOutcome::Delivered)
    }
}

#[async_trait]
impl uw_adapters::fallback::MetaHarnessMessenger for PaseoSessionMessenger {
    async fn send(
        &self,
        session_id: &SessionId,
        text: &str,
    ) -> uw_core::adapter::AdapterResult<DeliveryOutcome> {
        <Self as uw_adapters::claude_code::SessionMessenger>::send(self, session_id, text).await
    }
}

#[async_trait]
impl uw_adapters::claude_code::HookChannel for StoreHookChannel {
    async fn advise(
        &self,
        session_id: &SessionId,
        text: &str,
    ) -> uw_core::adapter::AdapterResult<DeliveryOutcome> {
        let path = self.db_path.clone();
        let session_id = session_id.clone();
        let text = text.to_owned();
        tokio::task::spawn_blocking(move || {
            Store::open(&path)
                .and_then(|store| store.enqueue_hook_message(&session_id, "Stop", &text))
        })
        .await
        .map_err(|error| uw_core::adapter::AdapterError::Other(error.to_string()))?
        .map_err(|error| uw_core::adapter::AdapterError::Other(error.to_string()))?;
        Ok(DeliveryOutcome::QueuedForNextIdle)
    }

    async fn emit_status(
        &self,
        session_id: &SessionId,
        text: &str,
    ) -> uw_core::adapter::AdapterResult<DeliveryOutcome> {
        let path = self.db_path.clone();
        let session_id = session_id.clone();
        let text = text.to_owned();
        tokio::task::spawn_blocking(move || {
            Store::open(&path)
                .and_then(|store| store.enqueue_hook_message(&session_id, "SessionStart", &text))
        })
        .await
        .map_err(|error| uw_core::adapter::AdapterError::Other(error.to_string()))?
        .map_err(|error| uw_core::adapter::AdapterError::Other(error.to_string()))?;
        Ok(DeliveryOutcome::QueuedForNextIdle)
    }
}

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
    session: &SessionSummary,
) -> CompactionPlan {
    if let Some(replacement) = &session.superseded_by {
        return CompactionPlan::SkipUnsupported(format!(
            "session identity was superseded by {}",
            replacement.0
        ));
    }
    if !capabilities.can_trigger_compaction {
        return CompactionPlan::SkipUnsupported(
            "adapter cannot honor destructive compaction requests".into(),
        );
    }
    match _request.kind {
        CompactionKind::AgentRequested | CompactionKind::AskNearLimit => {
            CompactionPlan::ClaimAndSend
        }
        CompactionKind::OpportunisticIdle | CompactionKind::AltModelReseed => {
            if idle {
                CompactionPlan::ClaimAndSend
            } else {
                CompactionPlan::WaitForIdle
            }
        }
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
    async fn token_usage(&self, session_id: &SessionId) -> anyhow::Result<Vec<TokenUsageRecord>>;
    async fn active_resume_marker(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<Option<ResumeMarker>>;
    async fn insert_resume_marker(&self, marker: ResumeMarker) -> anyhow::Result<()>;
    async fn set_resume_at(
        &self,
        id: uuid::Uuid,
        resume_at: DateTime<Utc>,
    ) -> anyhow::Result<()>;
    async fn due_resume_markers(&self, now: DateTime<Utc>) -> anyhow::Result<Vec<ResumeMarker>>;
    async fn claim_resume_marker(&self, _id: uuid::Uuid) -> anyhow::Result<bool> {
        Ok(false)
    }
    async fn resume_owner(&self, session_id: &SessionId) -> anyhow::Result<SessionSummary>;
    async fn update_resume(&self, id: uuid::Uuid, status: ResumeStatus) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ReseedStore: Send + Sync {
    async fn insert_reseed_summary(&self, summary: ReseedSummary) -> anyhow::Result<()>;
    async fn insert_reseeded_session(&self, session: SessionSummary) -> anyhow::Result<()>;
    async fn last_reseed_at(&self, id: &SessionId) -> anyhow::Result<Option<DateTime<Utc>>>;
    async fn link_reseeded_session(
        &self,
        new_id: &SessionId,
        source_id: &SessionId,
    ) -> anyhow::Result<()>;
}

#[async_trait]
pub trait KeepaliveStore: Send + Sync {
    async fn keepalive_sessions(&self) -> anyhow::Result<Vec<SessionSummary>>;
    async fn keepalive_state(&self, id: &SessionId) -> anyhow::Result<KeepaliveState>;
    async fn record_keepalive_ping(&self, id: &SessionId, at: DateTime<Utc>) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ObservationStore: Send + Sync {
    async fn tracked_sessions(&self) -> anyhow::Result<Vec<SessionSummary>>;
    async fn record_usage(&self, sample: UsageSample) -> anyhow::Result<()>;
    async fn record_stop(&self, id: &SessionId, reason: StopReason) -> anyhow::Result<()>;
    async fn last_hook_event(&self, id: &SessionId) -> anyhow::Result<Option<String>>;
    /// Registers a session the adapter found on disk but that usagewindow has never
    /// seen via a hook. A no-op if the session is already tracked.
    async fn upsert_discovered_session(
        &self,
        provider: Provider,
        found: DiscoveredSession,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()>;
    /// Records the outcome of a provider usage poll without touching `usage_samples` —
    /// a failure must never clobber the last-known-good sample.
    async fn record_fetch_result(
        &self,
        provider: Provider,
        account: Option<AccountId>,
        result: Result<(), String>,
        at: DateTime<Utc>,
    ) -> anyhow::Result<()>;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObservationReport {
    pub samples_recorded: u32,
    pub stops_recorded: u32,
    pub adapter_errors: u32,
    pub sessions_discovered: u32,
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
    pub async fn prune_usage_before(&self, cutoff: DateTime<Utc>) -> anyhow::Result<usize> {
        self.blocking(move |store| Ok(store.prune_usage_before(cutoff)?))
            .await
    }
    pub async fn resolved_profile(
        &self,
        session: &SessionSummary,
        mut profile: ThresholdProfile,
    ) -> anyhow::Result<ThresholdProfile> {
        let provider = session.harness.clone();
        let model = session.model.clone();
        let session_id = session.id.clone();
        self.blocking(move |store| {
            for scope in [
                None,
                Some(ThresholdScope {
                    provider: provider.clone(),
                    model: None,
                    session: None,
                }),
                model.clone().map(|model| ThresholdScope {
                    provider: provider.clone(),
                    model: Some(model),
                    session: None,
                }),
                Some(ThresholdScope {
                    provider,
                    model,
                    session: Some(session_id),
                }),
            ] {
                apply_threshold_values(&mut profile, store.threshold_values(scope.as_ref())?)?;
            }
            Ok(profile)
        })
        .await
    }
}

fn apply_threshold_values(
    profile: &mut ThresholdProfile,
    values: std::collections::BTreeMap<String, String>,
) -> anyhow::Result<()> {
    for (field, raw) in values {
        let raw = raw.trim_matches('"');
        match field.as_str() {
            "notice_pct" => profile.notice_pct = raw.parse()?,
            "closing_pct" => profile.closing_pct = raw.parse()?,
            "compact_pct" => profile.compact_pct = raw.parse()?,
            "plan_pressure_pct" => profile.plan_pressure_pct = raw.parse()?,
            "plan_pressure_min_tokens" => profile.plan_pressure_min_tokens = raw.parse()?,
            "burn_multiplier" => profile.burn_multiplier = raw.parse()?,
            "overhead_pct" => profile.overhead_pct = raw.parse()?,
            "min_lead_minutes" => profile.min_lead_minutes = raw.parse()?,
            "max_lead_minutes" => profile.max_lead_minutes = raw.parse()?,
            "reask_delta_pct" => profile.reask_delta_pct = raw.parse()?,
            "reask_max_per_epoch" => profile.reask_max_per_epoch = raw.parse()?,
            "cache_write_fallback_pct" => profile.cache_write_fallback_pct = raw.parse()?,
            _ => tracing::warn!(%field, "unsupported threshold override ignored"),
        }
    }
    Ok(())
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
        self.blocking(move |s| Ok(s.usage_samples_for(&provider, account.as_ref())?))
            .await
    }
    async fn token_usage(&self, id: &SessionId) -> anyhow::Result<Vec<TokenUsageRecord>> {
        let id = id.clone();
        self.blocking(move |store| Ok(store.token_usage_for_session(&id)?))
            .await
    }
    async fn active_resume_marker(
        &self,
        id: &SessionId,
    ) -> anyhow::Result<Option<ResumeMarker>> {
        let id = id.clone();
        self.blocking(move |s| Ok(s.active_resume_marker(&id)?))
            .await
    }
    async fn insert_resume_marker(&self, marker: ResumeMarker) -> anyhow::Result<()> {
        self.blocking(move |s| Ok(s.insert_resume_marker(&marker)?))
            .await
    }
    async fn set_resume_at(&self, id: uuid::Uuid, resume_at: DateTime<Utc>) -> anyhow::Result<()> {
        self.blocking(move |s| Ok(s.set_resume_at(id, resume_at)?))
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
    async fn claim_resume_marker(&self, id: uuid::Uuid) -> anyhow::Result<bool> {
        self.blocking(move |s| Ok(s.claim_resume_marker(id)?)).await
    }
}

#[async_trait]
impl ReseedStore for SqliteDaemonStore {
    async fn insert_reseed_summary(&self, summary: ReseedSummary) -> anyhow::Result<()> {
        self.blocking(move |s| Ok(s.insert_reseed_summary(&summary)?))
            .await
    }
    async fn link_reseeded_session(
        &self,
        new_id: &SessionId,
        source_id: &SessionId,
    ) -> anyhow::Result<()> {
        let new_id = new_id.clone();
        let source_id = source_id.clone();
        self.blocking(move |s| Ok(s.link_reseeded_session(&new_id, &source_id)?))
            .await
    }
    async fn insert_reseeded_session(&self, session: SessionSummary) -> anyhow::Result<()> {
        self.blocking(move |store| Ok(store.insert_session(&session)?))
            .await
    }
    async fn last_reseed_at(&self, id: &SessionId) -> anyhow::Result<Option<DateTime<Utc>>> {
        let id = id.clone();
        self.blocking(move |store| Ok(store.last_reseed_at(&id)?))
            .await
    }
}
#[async_trait]
impl KeepaliveStore for SqliteDaemonStore {
    async fn keepalive_sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        self.blocking(|s| Ok(s.list_sessions()?)).await
    }
    async fn keepalive_state(&self, id: &SessionId) -> anyhow::Result<KeepaliveState> {
        let id = id.clone();
        self.blocking(move |s| Ok(s.keepalive_config(&id)?)).await
    }
    async fn record_keepalive_ping(&self, id: &SessionId, at: DateTime<Utc>) -> anyhow::Result<()> {
        let id = id.clone();
        self.blocking(move |s| Ok(s.record_keepalive_ping(&id, at)?))
            .await
    }
}

#[async_trait]
impl ObservationStore for SqliteDaemonStore {
    async fn tracked_sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        self.blocking(|store| Ok(store.list_sessions()?)).await
    }

    async fn record_usage(&self, sample: UsageSample) -> anyhow::Result<()> {
        self.blocking(move |store| {
            store.insert_usage_sample(&sample)?;
            Ok(())
        })
        .await
    }

    async fn record_stop(&self, id: &SessionId, reason: StopReason) -> anyhow::Result<()> {
        let id = id.clone();
        self.blocking(move |store| Ok(store.update_session_stop(&id, Some(&reason))?))
            .await
    }
    async fn last_hook_event(&self, id: &SessionId) -> anyhow::Result<Option<String>> {
        let id = id.clone();
        self.blocking(move |store| Ok(store.latest_hook_event(&id)?))
            .await
    }
    async fn upsert_discovered_session(
        &self,
        provider: Provider,
        found: DiscoveredSession,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.blocking(move |store| {
            let session = SessionSummary {
                id: found.id.clone(),
                harness: provider,
                model: found.model,
                account: None,
                first_seen: found.first_seen.unwrap_or(now),
                last_seen: found.last_seen.unwrap_or(now),
                cwd: found.cwd,
                state_path: found.state_path,
                context_window_size: found.context_window_size,
                last_known_token_count: found.last_known_token_count,
                launch_mode: LaunchMode::Interactive,
                pid: None,
                stopped_reason: None,
                resume_marker: None,
                superseded_by: None,
                reseeded_from: None,
            };
            store.upsert_session(&session)?;
            store.insert_token_usage_records(&session.id, &found.token_usage)?;
            Ok(())
        })
        .await
    }
    async fn record_fetch_result(
        &self,
        provider: Provider,
        account: Option<AccountId>,
        result: Result<(), String>,
        at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.blocking(move |store| {
            match &result {
                Ok(()) => store.record_fetch_success(&provider, account.as_ref(), at)?,
                Err(error) => store.record_fetch_failure(&provider, account.as_ref(), at, error)?,
            }
            Ok(())
        })
        .await
    }
}

/// Polls each registered provider once, then checks every tracked session for a
/// structured stop signal. Adapter failures are isolated so one provider cannot
/// prevent the others from being observed on the same cadence.
pub async fn run_observation_tick(
    store: &dyn ObservationStore,
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
) -> anyhow::Result<ObservationReport> {
    let known = store.tracked_sessions().await?;
    let mut report = ObservationReport::default();
    let mut fetched_samples = HashMap::new();
    for adapter in adapters.values() {
        match adapter.fetch_usage(None).await {
            Ok(sample) => {
                fetched_samples.insert(adapter.provider(), sample.clone());
                store
                    .record_fetch_result(adapter.provider(), None, Ok(()), Utc::now())
                    .await?;
                store.record_usage(sample).await?;
                report.samples_recorded += 1;
            }
            Err(error) => {
                store
                    .record_fetch_result(
                        adapter.provider(),
                        None,
                        Err(error.to_string()),
                        Utc::now(),
                    )
                    .await?;
                report.adapter_errors += 1;
                tracing::warn!(provider = ?adapter.provider(), %error, "usage poll failed");
            }
        }
        match adapter.discover_sessions().await {
            Ok(found) => {
                let now = Utc::now();
                for session in found {
                    if !known.iter().any(|existing| existing.id == session.id) {
                        report.sessions_discovered += 1;
                    }
                    store
                        .upsert_discovered_session(adapter.provider(), session, now)
                        .await?;
                }
            }
            Err(AdapterError::Unsupported) => {}
            Err(error) => {
                report.adapter_errors += 1;
                tracing::warn!(provider = ?adapter.provider(), %error, "session discovery failed");
            }
        }
    }
    // Re-read so newly discovered sessions get a stop check in this same tick.
    let sessions = store.tracked_sessions().await?;
    for session in sessions {
        let Some(adapter) = adapters.get(&session.harness) else {
            continue;
        };
        let sampled_stop = fetched_samples
            .get(&session.harness)
            .and_then(|sample| {
                sample
                    .windows
                    .iter()
                    .find(|(_, window)| window.exceeded)
                    .map(|(window, _)| StopReason::UsageLimit { window: window.clone() })
            });
        let detected = match (sampled_stop, fetched_samples.contains_key(&session.harness)) {
            (Some(stop), _) => Ok(Some(stop)),
            (None, true) => Ok(None),
            (None, false) => adapter.detect_stop(&session.id).await,
        };
        match detected {
            Ok(Some(reason)) if session.stopped_reason.as_ref() != Some(&reason) => {
                // Codex has no passive mid-turn channel, so a hook event that isn't
                // SessionEnd/Interrupt is live contradicting evidence the session is
                // still going (suppress the stop). A session with NO hook history at
                // all — e.g. one that was only ever found via `discover_sessions`,
                // never wired to hooks — has no such evidence to contradict `detect_stop`,
                // so it must not be treated the same as "known to still be live".
                if session.harness == Provider::Codex
                    && matches!(reason, StopReason::UsageLimit { .. })
                    && matches!(
                        store.last_hook_event(&session.id).await?.as_deref(),
                        Some(event) if !matches!(event, "SessionEnd" | "Interrupt")
                    )
                {
                    continue;
                }
                store.record_stop(&session.id, reason).await?;
                report.stops_recorded += 1;
            }
            Ok(_) => {}
            Err(error) => {
                report.adapter_errors += 1;
                tracing::warn!(session = %session.id.0, %error, "stop detection failed");
            }
        }
    }
    Ok(report)
}

pub async fn run_reseed(
    session: &SessionSummary,
    cheap_model: ModelId,
    adapter: &dyn HarnessAdapter,
    summarizer: &dyn Summarizer,
    store: &dyn ReseedStore,
) -> anyhow::Result<SessionId> {
    let transcript = adapter.export_transcript(session).await?;
    let before = transcript.split_whitespace().count() as u64;
    let summary = summarizer
        .summarize(&transcript, &SummarizeTemplate::default())
        .await?;
    let after = summary.split_whitespace().count() as u64;
    store
        .insert_reseed_summary(ReseedSummary {
            id: uuid::Uuid::new_v4(),
            session_id: session.id.clone(),
            source_model: cheap_model.clone(),
            summary_text: summary.clone(),
            token_count_before: before,
            token_count_after: after,
            created_at: Utc::now(),
        })
        .await?;
    let new_id = adapter
        .seed_new_session(
            SeedMode::InitialPrompt,
            &SeedContext {
                from_session: Some(session.id.clone()),
                summary,
                model: cheap_model.clone(),
                cwd: session.cwd.clone(),
            },
        )
        .await?;
    store
        .insert_reseeded_session(SessionSummary {
            id: new_id.clone(),
            harness: adapter.provider(),
            model: Some(cheap_model),
            account: session.account.clone(),
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            cwd: session.cwd.clone(),
            state_path: None,
            context_window_size: None,
            last_known_token_count: None,
            launch_mode: LaunchMode::Headless,
            pid: None,
            stopped_reason: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: Some(session.id.clone()),
        })
        .await?;
    store.link_reseeded_session(&new_id, &session.id).await?;
    Ok(new_id)
}

/// Policy-gated reseed entry point. The caller supplies the already-resolved profile
/// and cost estimates; all orchestration remains in `run_reseed`.
#[allow(clippy::too_many_arguments)]
pub async fn run_reseed_auto_tick(
    session: &SessionSummary,
    cheap_model: ModelId,
    adapter: &dyn HarnessAdapter,
    summarizer: &dyn Summarizer,
    store: &dyn ReseedStore,
    now: DateTime<Utc>,
    cache_ttl: Duration,
    estimated_reseed_cost_usd: f64,
    estimated_wait_for_reset_cost_usd: f64,
    time_since_last_reseed: Duration,
    profile: &ThresholdProfile,
) -> anyhow::Result<Option<SessionId>> {
    let should = uw_policy::should_auto_reseed(
        now - session.last_seen,
        cache_ttl,
        session.last_known_token_count.unwrap_or_default(),
        estimated_reseed_cost_usd,
        estimated_wait_for_reset_cost_usd,
        time_since_last_reseed,
        &profile.reseed_auto,
        &adapter.capabilities(),
    );
    if should {
        Ok(Some(
            run_reseed(session, cheap_model, adapter, summarizer, store).await?,
        ))
    } else {
        Ok(None)
    }
}

pub struct AutoReseedRuntime {
    pub cheap_model: ModelId,
    pub summarizer: Arc<dyn Summarizer>,
    pub estimated_reseed_cost_usd: f64,
    pub estimated_wait_for_reset_cost_usd: f64,
}

pub async fn run_auto_reseed_ticks<S>(
    store: &S,
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
    sessions: &[SessionSummary],
    profiles: &HashMap<SessionId, ThresholdProfile>,
    runtime: &AutoReseedRuntime,
    now: DateTime<Utc>,
) -> anyhow::Result<u32>
where
    S: ReseedStore + DaemonStore,
{
    let mut reseeded = 0;
    for session in sessions {
        let Some(profile) = profiles.get(&session.id) else {
            continue;
        };
        let Some(cache_ttl) = profile.cache_ttl_by_provider.get(&session.harness) else {
            continue;
        };
        let Some(adapter) = adapters.get(&session.harness) else {
            continue;
        };
        let last = store.last_reseed_at(&session.id).await?;
        let since = last.map_or(Duration::MAX, |at| now - at);
        let should_reseed = uw_policy::should_auto_reseed(
            now - session.last_seen,
            *cache_ttl,
            session.last_known_token_count.unwrap_or_default(),
            runtime.estimated_reseed_cost_usd,
            runtime.estimated_wait_for_reset_cost_usd,
            since,
            &profile.reseed_auto,
            &adapter.capabilities(),
        );
        if !should_reseed {
            continue;
        }
        let request = CompactionRequest {
            id: uuid::Uuid::new_v4(),
            session_id: session.id.clone(),
            kind: CompactionKind::AltModelReseed,
            prompt: String::new(),
            reason: "automatic idle reseed".into(),
            status: CompactionStatus::Pending,
            created_at: now,
        };
        store.enqueue_compaction(request.clone()).await?;
        if !store.claim_compaction(request.id).await? {
            continue;
        }
        match run_reseed(
            session,
            runtime.cheap_model.clone(),
            adapter.as_ref(),
            runtime.summarizer.as_ref(),
            store,
        )
        .await
        {
            Ok(_) => {
                store
                    .update_compaction(request.id, CompactionStatus::Sent)
                    .await?;
                reseeded += 1;
            }
            Err(error) => {
                store
                    .update_compaction(request.id, CompactionStatus::Failed(error.to_string()))
                    .await?;
            }
        }
    }
    Ok(reseeded)
}

pub const KEEPALIVE_MARKER: &str = "[[uw-keepalive]] no action needed, acknowledge briefly";

pub fn should_fire_keepalive(
    now: DateTime<Utc>,
    session: &SessionSummary,
    state: &KeepaliveState,
    profile: &ThresholdProfile,
) -> bool {
    let Some(config) = profile.keepalive.as_ref() else {
        return false;
    };
    if !config.enabled {
        return false;
    }
    let Some(ttl) = profile.cache_ttl_by_provider.get(&session.harness) else {
        return false;
    };
    let keepalive_margin = profile.idle_compact.margin + Duration::minutes(1);
    let last_activity = state
        .last_ping_at
        .map_or(session.last_seen, |ping| ping.max(session.last_seen));
    if !state.enabled || last_activity >= now || now - last_activity < *ttl - keepalive_margin {
        return false;
    }
    let today = now.date_naive().to_string();
    state.ping_day.as_deref() != Some(today.as_str()) || state.ping_count < config.daily_cap
}

pub async fn run_keepalive_tick(
    store: &dyn KeepaliveStore,
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
    now: DateTime<Utc>,
    profiles: &HashMap<SessionId, ThresholdProfile>,
) -> anyhow::Result<u32> {
    let mut sent = 0;
    for session in store.keepalive_sessions().await? {
        let Some(profile) = profiles.get(&session.id) else {
            continue;
        };
        let Some(adapter) = adapters.get(&session.harness) else {
            continue;
        };
        if !adapter.capabilities().reports_token_counts {
            continue;
        }
        let state = match store.keepalive_state(&session.id).await {
            Ok(x) => x,
            Err(_) => continue,
        };
        if !should_fire_keepalive(now, &session, &state, profile) {
            continue;
        }
        if matches!(
            adapter.advise(&session.id, KEEPALIVE_MARKER).await,
            Ok(DeliveryOutcome::Delivered | DeliveryOutcome::QueuedForNextIdle)
        ) {
            store.record_keepalive_ping(&session.id, now).await?;
            sent += 1;
        }
    }
    Ok(sent)
}

pub async fn run_compaction_tick(
    store: &dyn DaemonStore,
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
    liveness: &dyn SessionLivenessChecker,
) -> anyhow::Result<()> {
    for request in store.pending_compactions().await? {
        if !matches!(
            request.kind,
            CompactionKind::AskNearLimit
                | CompactionKind::AgentRequested
                | CompactionKind::OpportunisticIdle
                | CompactionKind::AltModelReseed
        ) {
            continue;
        }
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
            &session,
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
                let result = adapter.compact(&session, &request).await;
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
    samples: &[UsageSample],
    adapter: &dyn HarnessAdapter,
    session: &SessionSummary,
    key: &WindowKey,
    profile: &ThresholdProfile,
    last_asked_pct: Option<f32>,
    asks_this_epoch: u32,
) -> anyhow::Result<NearLimitOutcome> {
    let blocks = uw_policy::segment_blocks(samples, key, session.account.as_ref());
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

#[derive(Default)]
pub struct PolicyRuntimeState {
    idle: IdleEpisodeTracker,
    asked: HashMap<AskEpoch, AskState>,
}

type AskEpoch = (SessionId, WindowKey, Option<DateTime<Utc>>);
type AskState = (f32, u32);

/// Runs the non-destructive policy decisions and queues any destructive work.
/// Destructive delivery remains in `run_compaction_tick` and `run_resume_tick`,
/// which both claim their rows before acting.
#[allow(clippy::too_many_arguments)]
pub async fn run_policy_tick(
    store: &(dyn DaemonStore + Send + Sync),
    keepalive_store: &(dyn KeepaliveStore + Send + Sync),
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
    liveness: &dyn SessionLivenessChecker,
    sessions: &[SessionSummary],
    profiles: &HashMap<SessionId, ThresholdProfile>,
    state: &mut PolicyRuntimeState,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut token_rates = HashMap::new();
    for session in sessions {
        let records = store.token_usage(&session.id).await?;
        let activity_weight = if now - session.last_seen <= Duration::minutes(5) {
            1.0
        } else {
            0.3
        };
        if let Some(rate) = uw_policy::weighted_token_rate_per_minute(
            &records,
            now,
            Duration::minutes(30),
            Duration::minutes(5),
            1.2,
            activity_weight,
        ) {
            token_rates.insert(session.id.clone(), rate);
        }
    }
    for session in sessions {
        let Some(adapter) = adapters.get(&session.harness) else {
            continue;
        };
        let Some(profile) = profiles.get(&session.id) else {
            continue;
        };
        let samples = store.usage_samples(session).await?;
        let mut keys = HashSet::new();
        for sample in &samples {
            keys.extend(sample.windows.keys().cloned());
        }
        for key in keys {
            let blocks = uw_policy::segment_blocks(&samples, &key, session.account.as_ref());
            let Some(block) = blocks.last() else {
                continue;
            };
            let Some((_, current_pct)) = block.points.last() else {
                continue;
            };
            let ask_key = (session.id.clone(), key.clone(), block.resets_at);
            let previous = state.asked.get(&ask_key).copied();
            let outcome = run_near_limit_tick(
                &samples,
                adapter.as_ref(),
                session,
                &key,
                profile,
                previous.map(|entry| entry.0),
                previous.map_or(0, |entry| entry.1),
            )
            .await?;
            if matches!(
                outcome,
                NearLimitOutcome::Advised | NearLimitOutcome::Checkpointed
            ) {
                state.asked.insert(
                    ask_key,
                    (*current_pct, previous.map_or(1, |entry| entry.1 + 1)),
                );
            }
            let stopped_on_this_window = matches!(
                session.stopped_reason.as_ref(),
                Some(StopReason::UsageLimit { window }) if window == &key
            );
            let manual_resume_waiting_for_schedule = store
                .active_resume_marker(&session.id)
                .await?
                .is_some_and(|marker| {
                    marker.reason == ResumeReason::ManuallyMarked
                        && marker.resume_at.is_none()
                });
            if stopped_on_this_window || manual_resume_waiting_for_schedule {
                reconcile_resume_marker_with_token_rates(
                    store,
                    session,
                    block,
                    now,
                    profile,
                    session.model.as_ref().map_or("", |model| model.0.as_str()),
                    &[],
                    sessions,
                    &token_rates,
                )
                .await?;
            }
        }
        if let Some(cache_ttl) = profile.cache_ttl_by_provider.get(&session.harness) {
            state
                .idle
                .tick(
                    store,
                    session,
                    now,
                    liveness.is_idle(session).await,
                    *cache_ttl,
                    profile,
                    adapter.as_ref(),
                )
                .await?;
        }
    }
    run_keepalive_tick(keepalive_store, adapters, now, profiles).await?;
    Ok(())
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

/// Reconciles the session's single active resume marker (the DB enforces at
/// most one `pending`/`scheduled` marker per session) against the current
/// window/burn-rate state:
/// - No active marker and the session just hit its usage limit → auto-plan one.
/// - An active marker still awaiting resolution (a manually queued resume with
///   no `resume_at` yet) → resolve it to `max(requested_at, policy floor)`,
///   so a manual resume can never fire before the window/burn-rate check says
///   it's safe, only later if `--at` asked for more margin.
/// - An already-resolved marker → leave it alone.
pub async fn reconcile_resume_marker(
    store: &dyn DaemonStore,
    session: &SessionSummary,
    block: &WindowBlock,
    now: DateTime<Utc>,
    profile: &ThresholdProfile,
    model: &str,
    history: &[CacheCostObservation],
) -> anyhow::Result<bool> {
    reconcile_resume_marker_with_token_rates(
        store,
        session,
        block,
        now,
        profile,
        model,
        history,
        &[],
        &HashMap::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_resume_marker_with_token_rates(
    store: &dyn DaemonStore,
    session: &SessionSummary,
    block: &WindowBlock,
    now: DateTime<Utc>,
    profile: &ThresholdProfile,
    model: &str,
    history: &[CacheCostObservation],
    sessions: &[SessionSummary],
    token_rates: &HashMap<SessionId, f64>,
) -> anyhow::Result<bool> {
    let token_burn_rate = scaled_token_burn_rate(block, session, sessions, token_rates);
    match store.active_resume_marker(&session.id).await? {
        Some(marker) if marker.resume_at.is_none() => {
            let Some(floor) = resume_floor_with_burn(
                session,
                block,
                now,
                profile,
                model,
                history,
                token_burn_rate,
            ) else {
                return Ok(false);
            };
            let resume_at = marker.requested_at.map_or(floor, |requested| requested.max(floor));
            store.set_resume_at(marker.id, resume_at).await?;
            Ok(true)
        }
        Some(_) => Ok(false),
        None => {
            let Some(marker) = plan_resume_marker_with_burn(
                session,
                block,
                now,
                profile,
                model,
                history,
                token_burn_rate,
            )
            else {
                return Ok(false);
            };
            store.insert_resume_marker(marker).await?;
            Ok(true)
        }
    }
}

fn scaled_token_burn_rate(
    block: &WindowBlock,
    session: &SessionSummary,
    sessions: &[SessionSummary],
    token_rates: &HashMap<SessionId, f64>,
) -> Option<f64> {
    let global = uw_policy::burn_rate_pct_per_hour_available(block, Duration::minutes(30))?
        as f64
        / 60.0;
    let target = *token_rates.get(&session.id)?;
    let total: f64 = sessions
        .iter()
        .filter(|other| {
            other.harness == session.harness && other.account == session.account
        })
        .filter_map(|other| token_rates.get(&other.id))
        .sum();
    (total > 0.0).then_some(global * target / total)
}
impl Default for IdleEpisodeTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// The earliest the policy engine considers it safe to resume: the point at
/// which remaining window budget, burn rate, and cache-write cost line up, per
/// `uw_policy::resume_lead_minutes`. This is the invariant floor — nothing may
/// resume before it, whether auto-detected or manually requested via `--at`.
#[cfg(test)]
fn resume_floor(
    session: &SessionSummary,
    block: &WindowBlock,
    now: DateTime<Utc>,
    profile: &ThresholdProfile,
    model: &str,
    history: &[CacheCostObservation],
) -> Option<DateTime<Utc>> {
    resume_floor_with_burn(session, block, now, profile, model, history, None)
}

fn resume_floor_with_burn(
    session: &SessionSummary,
    block: &WindowBlock,
    now: DateTime<Utc>,
    profile: &ThresholdProfile,
    model: &str,
    history: &[CacheCostObservation],
    token_burn_rate: Option<f64>,
) -> Option<DateTime<Utc>> {
    let (_, pct) = *block.points.last()?;
    let cache_ttl = profile
        .cache_ttl_by_provider
        .get(&session.harness)
        .copied()
        .unwrap_or_default();
    if cache_ttl > Duration::zero() && now - session.last_seen <= cache_ttl {
        return Some(now);
    }
    let lookback_minutes = match block.key.kind {
        WindowKind::Rolling { minutes } => u64::from(minutes).clamp(1, 30),
        _ => 30,
    };
    let burn = token_burn_rate.unwrap_or_else(|| {
        uw_policy::burn_rate_pct_per_hour_available(
            block,
            Duration::minutes(lookback_minutes as i64),
        )
        .unwrap_or_default() as f64
            / 60.0
    });
    let cache = uw_policy::estimate_cache_write_pct(
        session.last_known_token_count.unwrap_or_default(),
        model,
        profile,
        history,
    );
    let remaining = 100.0_f64 - f64::from(pct);
    if burn <= 0.0 {
        return (remaining > f64::from(cache)).then_some(now);
    }
    let horizon_minutes = match block.key.kind {
        WindowKind::Rolling { minutes } => f64::from(minutes.min(30)),
        _ => 30.0,
    };
    if remaining / burn >= horizon_minutes {
        return Some(now);
    }
    let lead = uw_policy::resume_lead_minutes(100.0 - pct, cache, burn as f32, profile)?;
    Some(now + Duration::minutes(lead as i64))
}

pub fn plan_resume_marker(
    session: &SessionSummary,
    block: &WindowBlock,
    now: DateTime<Utc>,
    profile: &ThresholdProfile,
    model: &str,
    history: &[CacheCostObservation],
) -> Option<ResumeMarker> {
    plan_resume_marker_with_burn(session, block, now, profile, model, history, None)
}

fn plan_resume_marker_with_burn(
    session: &SessionSummary,
    block: &WindowBlock,
    now: DateTime<Utc>,
    profile: &ThresholdProfile,
    model: &str,
    history: &[CacheCostObservation],
    token_burn_rate: Option<f64>,
) -> Option<ResumeMarker> {
    if !matches!(session.stopped_reason, Some(StopReason::UsageLimit { .. })) {
        return None;
    }
    let resume_at = resume_floor_with_burn(
        session,
        block,
        now,
        profile,
        model,
        history,
        token_burn_rate,
    )?;
    Some(ResumeMarker {
        id: uuid::Uuid::new_v4(),
        session_id: session.id.clone(),
        reason: ResumeReason::AutoDetectedLimit,
        resume_at: Some(resume_at),
        requested_at: None,
        created_at: now,
        status: ResumeStatus::Scheduled,
        message: None,
    })
}

pub async fn run_resume_tick(
    store: &dyn DaemonStore,
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    for marker in store.due_resume_markers(now).await? {
        if !store.claim_resume_marker(marker.id).await? {
            continue;
        }
        let session = store.resume_owner(&marker.session_id).await?;
        let Some(adapter) = adapters.get(&session.harness) else {
            store
                .update_resume(marker.id, ResumeStatus::Failed("no adapter".into()))
                .await?;
            continue;
        };
        let result = adapter.resume_session(&session, marker.message.as_deref()).await;
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
    run_scheduling_ticks(store, adapters, liveness, interval).await
}

/// Runs the daemon's destructive scheduling work on its configured cadence.
/// This is kept separate from the HTTP server so the production entry point and
/// integration tests use the same scheduling code.
pub async fn run_scheduling_ticks(
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

pub async fn run_production_ticks(
    store: Arc<SqliteDaemonStore>,
    adapters: &HashMap<Provider, Arc<dyn HarnessAdapter>>,
    liveness: &dyn SessionLivenessChecker,
    interval: std::time::Duration,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(interval);
    let mut policy_state = PolicyRuntimeState::default();
    let auto_reseed = auto_reseed_runtime_from_env();
    let observation_interval = env_duration_or("UW_OBSERVATION_INTERVAL_SECS", interval);
    let policy_interval = env_duration_secs("UW_POLICY_INTERVAL_SECS", 30);
    let maintenance_interval = env_duration_secs("UW_MAINTENANCE_INTERVAL_SECS", 3600);
    let started = Instant::now();
    let mut last_observation = started - observation_interval;
    let mut last_policy = started - policy_interval;
    let mut last_maintenance = started - maintenance_interval;
    loop {
        ticker.tick().await;
        // Explicit compaction requests are destructive and user-directed. Dispatch
        // them before provider observation, which may block on a harness transport.
        run_compaction_tick(store.as_ref(), adapters, liveness).await?;
        let now = Instant::now();
        if now.duration_since(last_observation) >= observation_interval {
            run_observation_tick(store.as_ref(), adapters).await?;
            last_observation = now;
        }
        if now.duration_since(last_maintenance) >= maintenance_interval {
            let retention_days = std::env::var("UW_RETENTION_DAYS")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|days| *days > 0)
                .unwrap_or(30);
            store
                .prune_usage_before(Utc::now() - Duration::days(retention_days))
                .await?;
            last_maintenance = now;
        }
        if now.duration_since(last_policy) < policy_interval {
            run_resume_tick(store.as_ref(), adapters, Utc::now()).await?;
            continue;
        }
        last_policy = now;
        let sessions = store.tracked_sessions().await?;
        let mut profiles = HashMap::new();
        for session in &sessions {
            let mut profile = ThresholdProfile {
                keepalive: Some(KeepaliveConfig {
                    enabled: true,
                    daily_cap: std::env::var("UW_KEEPALIVE_DAILY_CAP")
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(24),
                }),
                ..Default::default()
            };
            if matches!(session.harness, Provider::ClaudeCode | Provider::Codex) {
                profile
                    .cache_ttl_by_provider
                    .insert(session.harness.clone(), Duration::minutes(5));
            }
            if auto_reseed.is_some() {
                profile.reseed_auto = ReseedAutoConfig {
                    enabled: true,
                    min_tokens: std::env::var("UW_RESEED_MIN_TOKENS")
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(100_000),
                    cooldown: Duration::minutes(
                        std::env::var("UW_RESEED_COOLDOWN_MINUTES")
                            .ok()
                            .and_then(|value| value.parse().ok())
                            .unwrap_or(60),
                    ),
                    margin: Duration::minutes(1),
                };
            }
            profiles.insert(
                session.id.clone(),
                store.resolved_profile(session, profile).await?,
            );
        }
        run_policy_tick(
            store.as_ref(),
            store.as_ref(),
            adapters,
            liveness,
            &sessions,
            &profiles,
            &mut policy_state,
            Utc::now(),
        )
        .await?;
        if let Some(runtime) = &auto_reseed {
            run_auto_reseed_ticks(
                store.as_ref(),
                adapters,
                &sessions,
                &profiles,
                runtime,
                Utc::now(),
            )
            .await?;
        }
        run_resume_tick(store.as_ref(), adapters, Utc::now()).await?;
    }
}

fn env_duration_secs(name: &str, default_secs: u64) -> StdDuration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(StdDuration::from_secs)
        .unwrap_or_else(|| StdDuration::from_secs(default_secs))
}

fn env_duration_or(name: &str, default: StdDuration) -> StdDuration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(StdDuration::from_secs)
        .unwrap_or(default)
}

fn auto_reseed_runtime_from_env() -> Option<AutoReseedRuntime> {
    if std::env::var("UW_RESEED_AUTO").ok().as_deref() != Some("true") {
        return None;
    }
    let base_url = std::env::var("UW_SUMMARIZER_BASE_URL").ok()?;
    let summarizer_model = std::env::var("UW_SUMMARIZER_MODEL").ok()?;
    let target_model = std::env::var("UW_RESEED_MODEL").ok()?;
    let reseed_cost = std::env::var("UW_RESEED_ESTIMATED_COST_USD")
        .ok()?
        .parse()
        .ok()?;
    let wait_cost = std::env::var("UW_WAIT_FOR_RESET_ESTIMATED_COST_USD")
        .ok()?
        .parse()
        .ok()?;
    Some(AutoReseedRuntime {
        cheap_model: ModelId(target_model),
        summarizer: Arc::new(uw_adapters::summarizer::OpenAiCompatibleSummarizer::new(
            base_url,
            summarizer_model,
            std::env::var("UW_SUMMARIZER_API_KEY").ok(),
            Arc::new(uw_adapters::summarizer::ReqwestChatTransport::default()),
        )),
        estimated_reseed_cost_usd: reseed_cost,
        estimated_wait_for_reset_cost_usd: wait_cost,
    })
}

/// Entry point shared by the daemon binary and the Phase 6 CLI command. Service
/// configuration (SQLite path and adapter registry) is intentionally still a later
/// wiring concern; this preserves the same cadence/ownership point for Phase 7.
pub async fn run_daemon_loop() -> anyhow::Result<()> {
    let path = std::env::var("UW_DB_PATH").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/.usagewindow/usagewindow.db")
    });
    let store = Store::open(&path)?;
    let daemon_store = Arc::new(SqliteDaemonStore::new(store));
    let cache_path = std::env::var("UW_CLAUDE_CACHE_PATH")
        .unwrap_or_else(|_| format!("{path}.claude-usage-cache.json"));
    let hook_channel = Arc::new(StoreHookChannel {
        db_path: path.clone(),
    });
    let paseo_messenger = Arc::new(PaseoSessionMessenger::from_environment());
    let claude = uw_adapters::claude_code::ClaudeCodeAdapter::real(
        cache_path,
        std::env::var("UW_CLAUDE_VERSION").unwrap_or_else(|_| "unknown".into()),
    )
    .with_delivery(
        hook_channel.clone(),
        paseo_messenger.clone(),
        Arc::new(uw_adapters::process::TokioProcessSpawner),
    );
    let paseo_fallback: Arc<dyn HarnessAdapter> = Arc::new(
        uw_adapters::fallback::MessageCompactionAdapter::new(paseo_messenger),
    );
    let mut codex: Arc<dyn HarnessAdapter> = Arc::new(
        uw_adapters::codex::CodexAdapter::real().with_hook_channel(hook_channel),
    );
    let mut adapters: HashMap<Provider, Arc<dyn HarnessAdapter>> = HashMap::from([
        (Provider::Codex, codex.clone()),
        (
            Provider::ClaudeCode,
            Arc::new(claude) as Arc<dyn HarnessAdapter>,
        ),
    ]);
    if let Ok(command) = std::env::var("UW_GENERIC_USAGE_COMMAND") {
        let command: Vec<String> = command.split_whitespace().map(str::to_owned).collect();
        if !command.is_empty() {
            adapters.insert(
                Provider::Other("generic".into()),
                Arc::new(uw_adapters::generic_hook::GenericHookAdapter::new(Some(
                    command,
                ))),
            );
        }
    }
    if let Ok(base_url) = std::env::var("UW_T3CODE_URL") {
        let auth = match (
            std::env::var("UW_T3CODE_BEARER_TOKEN").ok(),
            std::env::var("UW_T3CODE_COOKIE").ok(),
        ) {
            (Some(token), None) if !token.is_empty() => {
                Some(uw_adapters::t3code::T3CodeAuth::Bearer(token))
            }
            (None, Some(cookie)) if !cookie.is_empty() => {
                Some(uw_adapters::t3code::T3CodeAuth::Cookie(cookie))
            }
            (Some(_), Some(_)) => {
                tracing::warn!(
                    "both UW_T3CODE_BEARER_TOKEN and UW_T3CODE_COOKIE are set; T3Code adapter disabled"
                );
                None
            }
            _ => {
                tracing::warn!(
                    "UW_T3CODE_URL is set but neither T3Code auth variable is configured; adapter disabled"
                );
                None
            }
        };
        if let Some(auth) = auth {
            let t3code: Arc<dyn HarnessAdapter> = Arc::new(
                uw_adapters::t3code::T3CodeAdapter::real(base_url, auth),
            );
            codex = Arc::new(uw_adapters::fallback::FallbackCompactionAdapter::new(
                codex,
                t3code.clone(),
            ));
            adapters.insert(
                Provider::Other("t3code".into()),
                t3code,
            );
        }
    }
    codex = Arc::new(uw_adapters::fallback::FallbackCompactionAdapter::new(
        codex,
        paseo_fallback,
    ));
    adapters.insert(Provider::Codex, codex);
    let liveness = SystemSessionLivenessChecker {
        db_path: path.clone(),
    };
    let app = uw_web::app_with_adapters(Store::open(&path)?, adapters.clone());
    let address = std::env::var("UW_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:7878".into());
    let listener = tokio::net::TcpListener::bind(address).await?;
    let server = async move { axum::serve(listener, app).await };
    tokio::select! {
        result = server => result.map_err(Into::into),
        result = run_production_ticks(daemon_store, &adapters, &liveness, std::time::Duration::from_secs(60)) => result,
    }
}

struct SystemSessionLivenessChecker {
    db_path: String,
}
#[async_trait]
impl SessionLivenessChecker for SystemSessionLivenessChecker {
    async fn is_idle(&self, session: &SessionSummary) -> bool {
        let path = self.db_path.clone();
        let id = session.id.clone();
        tokio::task::spawn_blocking(move || {
            Store::open(&path)
                .and_then(|store| store.latest_hook_event(&id))
                .ok()
                .flatten()
                .as_deref()
                == Some("Stop")
        })
        .await
        .unwrap_or(false)
    }
}

/// Hook ingress is intentionally fail-open. The HTTP backend validates and routes
/// harness-specific event names, records session observations, and returns only the
/// response fields supported by that harness and event.
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
    match tokio::time::timeout(std::time::Duration::from_millis(250), backend(payload)).await {
        Ok(Ok(response)) => response,
        _ => serde_json::json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    };
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
        active_resume: Option<ResumeMarker>,
        resolved: ResolvedTimes,
    }
    type ResolvedTimes = Arc<StdMutex<Vec<(uuid::Uuid, DateTime<Utc>)>>>;
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
        async fn token_usage(&self, _: &SessionId) -> anyhow::Result<Vec<TokenUsageRecord>> {
            Ok(vec![])
        }
        async fn active_resume_marker(
            &self,
            _: &SessionId,
        ) -> anyhow::Result<Option<ResumeMarker>> {
            Ok(self.active_resume.clone())
        }
        async fn insert_resume_marker(&self, _: ResumeMarker) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_resume_at(&self, id: uuid::Uuid, at: DateTime<Utc>) -> anyhow::Result<()> {
            self.resolved.lock().unwrap().push((id, at));
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
    static RESUME_CALLS: AtomicUsize = AtomicUsize::new(0);
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
            _: &SessionSummary,
            _: &CompactionRequest,
        ) -> AdapterResult<DeliveryOutcome> {
            *self.compacted.lock().unwrap() += 1;
            Ok(DeliveryOutcome::Delivered)
        }
        async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
            RESUME_CALLS.fetch_add(1, Ordering::SeqCst);
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

    #[async_trait]
    impl KeepaliveStore for FakeStore {
        async fn keepalive_sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
            Ok(vec![])
        }

        async fn keepalive_state(&self, _: &SessionId) -> anyhow::Result<KeepaliveState> {
            Ok(KeepaliveState {
                session_id: SessionId("s".into()),
                enabled: false,
                last_ping_at: None,
                ping_day: None,
                ping_count: 0,
            })
        }

        async fn record_keepalive_ping(
            &self,
            _: &SessionId,
            _: DateTime<Utc>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn manual_resume_marker_is_reconciled_without_stopped_reason() {
        let now = Utc::now();
        let key = WindowKey {
            provider: Provider::ClaudeCode,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        let marker = ResumeMarker {
            id: uuid::Uuid::new_v4(),
            session_id: SessionId("s".into()),
            reason: ResumeReason::ManuallyMarked,
            resume_at: None,
            requested_at: None,
            created_at: now,
            status: ResumeStatus::Pending,
            message: Some("continue".into()),
        };
        let store = FakeStore {
            request: StdMutex::new(None),
            owner: session(),
            claim: true,
            status: StdMutex::new(vec![]),
            samples: vec![
                UsageSample {
                    at: now - Duration::minutes(31),
                    fetched_at: None,
                    source: UsageSource::ProviderReported,
                    provider: Provider::ClaudeCode,
                    account: None,
                    windows: HashMap::from([(
                        key.clone(),
                        UsageWindowState::new(0.0, false, true, None, None),
                    )]),
                    credits: None,
                },
                UsageSample {
                    at: now,
                    fetched_at: None,
                    source: UsageSource::ProviderReported,
                    provider: Provider::ClaudeCode,
                    account: None,
                    windows: HashMap::from([(
                        key,
                        UsageWindowState::new(51.0, false, true, None, None),
                    )]),
                    credits: None,
                },
            ],
            enqueues: Arc::new(StdMutex::new(0)),
            active_resume: Some(marker.clone()),
            resolved: Arc::new(StdMutex::new(vec![])),
        };
        let adapter = Arc::new(FakeAdapter {
            capabilities: caps(false, false, false),
            compacted: Arc::new(StdMutex::new(0)),
            advised: Arc::new(StdMutex::new(0)),
        });
        let mut profiles = HashMap::new();
        profiles.insert(marker.session_id.clone(), ThresholdProfile::default());
        let mut policy_state = PolicyRuntimeState::default();

        run_policy_tick(
            &store,
            &store,
            &HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]),
            &AlwaysIdle,
            &[session()],
            &profiles,
            &mut policy_state,
            now,
        )
        .await
        .unwrap();

        assert_eq!(store.resolved.lock().unwrap().len(), 1);
        assert_eq!(store.resolved.lock().unwrap()[0].0, marker.id);
    }

    #[test]
    fn resume_floor_only_waits_for_uncached_fast_exhaustion() {
        let now = Utc::now();
        let (mut session, mut block) = near_limit_session_and_block();
        let mut profile = ThresholdProfile::default();
        profile
            .cache_ttl_by_provider
            .insert(Provider::ClaudeCode, Duration::minutes(5));

        // A warm cache warrants an immediate resume even with a fast burn rate.
        session.last_seen = now - Duration::minutes(1);
        block.points = vec![
            (now - Duration::minutes(10), 0.0),
            (now, 50.0),
        ];
        block.point_resets_at = vec![None, None];
        assert_eq!(
            resume_floor(&session, &block, now, &profile, "claude", &[]),
            Some(now)
        );

        // An uncached chat still resumes immediately when exhaustion is not
        // expected within the 30-minute safety horizon.
        session.last_seen = now - Duration::minutes(10);
        block.points = vec![
            (now - Duration::minutes(31), 0.0),
            (now, 1.0),
        ];
        block.point_resets_at = vec![None, None];
        assert_eq!(
            resume_floor(&session, &block, now, &profile, "claude", &[]),
            Some(now)
        );

        // A short rolling window uses its own duration as the horizon.
        block.key.kind = WindowKind::Rolling { minutes: 5 };
        block.points = vec![(now - Duration::minutes(6), 0.0), (now, 10.0)];
        block.point_resets_at = vec![None, None];
        assert_eq!(
            resume_floor(&session, &block, now, &profile, "claude", &[]),
            Some(now)
        );

        // Only an uncached chat expected to exhaust inside the horizon waits.
        block.key.kind = WindowKind::Rolling { minutes: 300 };
        block.points = vec![
            (now - Duration::minutes(31), 0.0),
            (now - Duration::minutes(10), 40.0),
            (now, 90.0),
        ];
        block.point_resets_at = vec![None, None, None];
        assert!(resume_floor(&session, &block, now, &profile, "claude", &[])
            .is_some_and(|at| at > now));
    }

    #[test]
    fn token_burn_share_uses_per_chat_work_rates() {
        let (first, mut block) = near_limit_session_and_block();
        let now = Utc::now();
        block.points = vec![(now - Duration::minutes(20), 0.0), (now, 50.0)];
        block.point_resets_at = vec![None, None];
        let mut second = session();
        second.id = SessionId("other".into());
        let rates = HashMap::from([(first.id.clone(), 2.0), (second.id.clone(), 0.3)]);
        let scaled = scaled_token_burn_rate(
            &block,
            &first,
            &[first.clone(), second],
            &rates,
        )
        .expect("the block has a usable quota burn rate");
        let global = uw_policy::burn_rate_pct_per_hour_available(
            &block,
            Duration::minutes(30),
        )
        .unwrap() as f64
            / 60.0;
        assert!((scaled - global * 2.0 / 2.3).abs() < 0.0001);
    }

    #[test]
    fn unsupported_compaction_is_failed_without_send_plan() {
        assert_eq!(
            plan_compaction_tick(
                &request(),
                &caps(false, false, false),
                true,
                &session(),
            ),
            CompactionPlan::SkipUnsupported(
                "adapter cannot honor destructive compaction requests".into()
            )
        );
    }
    #[test]
    fn race_plan_requires_claim_before_send() {
        assert_eq!(
            plan_compaction_tick(&request(), &caps(true, false, false), true, &session()),
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

    struct ObservationFake {
        sessions: Vec<SessionSummary>,
        samples: StdMutex<Vec<UsageSample>>,
        stops: StdMutex<Vec<(SessionId, StopReason)>>,
        last_hook_event: Option<String>,
    }

    #[async_trait]
    impl ObservationStore for ObservationFake {
        async fn tracked_sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
            Ok(self.sessions.clone())
        }
        async fn record_usage(&self, sample: UsageSample) -> anyhow::Result<()> {
            self.samples.lock().unwrap().push(sample);
            Ok(())
        }
        async fn record_stop(&self, id: &SessionId, reason: StopReason) -> anyhow::Result<()> {
            self.stops.lock().unwrap().push((id.clone(), reason));
            Ok(())
        }
        async fn last_hook_event(&self, _: &SessionId) -> anyhow::Result<Option<String>> {
            Ok(self.last_hook_event.clone())
        }
        async fn upsert_discovered_session(
            &self,
            _: Provider,
            _: DiscoveredSession,
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

    struct ObservationAdapter {
        sample: UsageSample,
        stop: Option<StopReason>,
    }

    #[async_trait]
    impl HarnessAdapter for ObservationAdapter {
        fn provider(&self) -> Provider {
            self.sample.provider.clone()
        }
        fn capabilities(&self) -> Capabilities {
            caps(false, false, false)
        }
        async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
            Ok(self.sample.clone())
        }
        async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
            Ok(self.stop.clone())
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
            Err(AdapterError::Unsupported)
        }
        async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
            Err(AdapterError::Unsupported)
        }
    }

    struct ProviderRequestCountingAdapter {
        sample: UsageSample,
        stop: Option<StopReason>,
        provider_requests: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HarnessAdapter for ProviderRequestCountingAdapter {
        fn provider(&self) -> Provider {
            self.sample.provider.clone()
        }
        fn capabilities(&self) -> Capabilities {
            caps(false, false, false)
        }
        async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
            self.provider_requests.fetch_add(1, Ordering::SeqCst);
            Ok(self.sample.clone())
        }
        async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
            self.provider_requests.fetch_add(1, Ordering::SeqCst);
            Ok(self.stop.clone())
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
            Err(AdapterError::Unsupported)
        }
        async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
            Err(AdapterError::Unsupported)
        }
    }

    #[tokio::test]
    async fn observation_tick_polls_usage_and_records_detected_stops() {
        let now = Utc::now();
        let key = WindowKey {
            provider: Provider::Codex,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        let sample = UsageSample {
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
        };
        let store = ObservationFake {
            sessions: vec![SessionSummary {
                harness: Provider::Codex,
                ..session()
            }],
            samples: StdMutex::new(vec![]),
            stops: StdMutex::new(vec![]),
            last_hook_event: Some("SessionEnd".into()),
        };
        let adapter = Arc::new(ObservationAdapter {
            sample,
            stop: Some(StopReason::UsageLimit {
                window: key.clone(),
            }),
        });

        let report = run_observation_tick(
            &store,
            &HashMap::from([(Provider::Codex, adapter as Arc<dyn HarnessAdapter>)]),
        )
        .await
        .unwrap();

        assert_eq!(report.samples_recorded, 1);
        assert_eq!(report.stops_recorded, 1);
        assert_eq!(store.samples.lock().unwrap().len(), 1);
        assert_eq!(
            store.stops.lock().unwrap().as_slice(),
            &[(
                SessionId("s".into()),
                StopReason::UsageLimit { window: key }
            )]
        );
    }

    #[tokio::test]
    async fn observation_tick_reuses_one_provider_fetch_for_every_session_stop_check() {
        let key = WindowKey {
            provider: Provider::Codex,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        let now = Utc::now();
        let mut first = session();
        first.id = SessionId("first".into());
        first.harness = Provider::Codex;
        let mut second = first.clone();
        second.id = SessionId("second".into());
        let store = ObservationFake {
            sessions: vec![first, second],
            samples: StdMutex::new(vec![]),
            stops: StdMutex::new(vec![]),
            last_hook_event: None,
        };
        let provider_requests = Arc::new(AtomicUsize::new(0));
        let adapter = Arc::new(ProviderRequestCountingAdapter {
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
            stop: Some(StopReason::UsageLimit { window: key }),
            provider_requests: provider_requests.clone(),
        });

        let report = run_observation_tick(
            &store,
            &HashMap::from([(Provider::Codex, adapter as Arc<dyn HarnessAdapter>)]),
        )
        .await
        .unwrap();

        assert_eq!(report.samples_recorded, 1);
        assert_eq!(report.stops_recorded, 2);
        assert_eq!(
            provider_requests.load(Ordering::SeqCst),
            1,
            "the provider sample fetched at the start of the tick must drive all stop checks"
        );
    }

    #[tokio::test]
    async fn codex_usage_limit_stop_is_recorded_for_a_session_with_no_hook_history() {
        // A session that only ever surfaced via `discover_sessions` (never wired to
        // hooks) has no hook evidence either way. It must not be treated as "known
        // still live" just because it has never reported in.
        let key = WindowKey {
            provider: Provider::Codex,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        let now = Utc::now();
        let store = ObservationFake {
            sessions: vec![SessionSummary {
                harness: Provider::Codex,
                ..session()
            }],
            samples: StdMutex::new(vec![]),
            stops: StdMutex::new(vec![]),
            last_hook_event: None,
        };
        let adapter = Arc::new(ObservationAdapter {
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
            stop: Some(StopReason::UsageLimit {
                window: key.clone(),
            }),
        });

        let report = run_observation_tick(
            &store,
            &HashMap::from([(Provider::Codex, adapter as Arc<dyn HarnessAdapter>)]),
        )
        .await
        .unwrap();

        assert_eq!(report.stops_recorded, 1);
        assert_eq!(
            store.stops.lock().unwrap().as_slice(),
            &[(SessionId("s".into()), StopReason::UsageLimit { window: key })]
        );
    }

    #[tokio::test]
    async fn codex_usage_limit_stop_is_suppressed_by_live_non_terminal_hook_evidence() {
        // A hook event that isn't SessionEnd/Interrupt is real, recent evidence the
        // session is still going — this is the case the gate exists to protect.
        let key = WindowKey {
            provider: Provider::Codex,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        let now = Utc::now();
        let store = ObservationFake {
            sessions: vec![SessionSummary {
                harness: Provider::Codex,
                ..session()
            }],
            samples: StdMutex::new(vec![]),
            stops: StdMutex::new(vec![]),
            last_hook_event: Some("PostToolUse".into()),
        };
        let adapter = Arc::new(ObservationAdapter {
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
            stop: Some(StopReason::UsageLimit { window: key }),
        });

        let report = run_observation_tick(
            &store,
            &HashMap::from([(Provider::Codex, adapter as Arc<dyn HarnessAdapter>)]),
        )
        .await
        .unwrap();

        assert_eq!(report.stops_recorded, 0);
        assert!(store.stops.lock().unwrap().is_empty());
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
            active_resume: None,
            resolved: Arc::new(StdMutex::new(vec![])),
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
    async fn ask_near_limit_is_sent_immediately_even_when_session_is_active() {
        let compacted = Arc::new(StdMutex::new(0));
        let adapter = Arc::new(FakeAdapter {
            capabilities: caps(true, true, false),
            compacted: compacted.clone(),
            advised: Arc::new(StdMutex::new(0)),
        });
        let mut ask = request();
        ask.kind = CompactionKind::AskNearLimit;
        let store = FakeStore {
            request: StdMutex::new(Some(ask)),
            owner: session(),
            claim: true,
            status: StdMutex::new(vec![]),
            samples: vec![],
            enqueues: Arc::new(StdMutex::new(0)),
            active_resume: None,
            resolved: Arc::new(StdMutex::new(vec![])),
        };
        struct NeverIdle;
        #[async_trait]
        impl SessionLivenessChecker for NeverIdle {
            async fn is_idle(&self, _: &SessionSummary) -> bool {
                false
            }
        }
        run_compaction_tick(
            &store,
            &HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]),
            &NeverIdle,
        )
        .await
        .unwrap();
        assert_eq!(*compacted.lock().unwrap(), 1);
        assert!(matches!(
            store.status.lock().unwrap()[0],
            CompactionStatus::Sent
        ));
    }

    #[tokio::test]
    async fn agent_requested_compaction_is_claimed_and_sent() {
        let compacted = Arc::new(StdMutex::new(0));
        let adapter = Arc::new(FakeAdapter {
            capabilities: caps(true, true, false),
            compacted: compacted.clone(),
            advised: Arc::new(StdMutex::new(0)),
        });
        let mut requested = request();
        requested.kind = CompactionKind::AgentRequested;
        let store = FakeStore {
            request: StdMutex::new(Some(requested)),
            owner: session(),
            claim: true,
            status: StdMutex::new(vec![]),
            samples: vec![],
            enqueues: Arc::new(StdMutex::new(0)),
            active_resume: None,
            resolved: Arc::new(StdMutex::new(vec![])),
        };
        run_compaction_tick(
            &store,
            &HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]),
            &AlwaysIdle,
        )
        .await
        .unwrap();
        assert_eq!(*compacted.lock().unwrap(), 1);
        assert!(matches!(
            store.status.lock().unwrap()[0],
            CompactionStatus::Sent
        ));
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
            active_resume: None,
            resolved: Arc::new(StdMutex::new(vec![])),
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
            active_resume: None,
            resolved: Arc::new(StdMutex::new(vec![])),
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
    async fn scheduling_entry_point_runs_ticks_on_its_cadence() {
        let compacted = Arc::new(StdMutex::new(0));
        let adapter = Arc::new(FakeAdapter {
            capabilities: caps(true, false, false),
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
            active_resume: None,
            resolved: Arc::new(StdMutex::new(vec![])),
        };
        let adapters = HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            run_scheduling_ticks(
                &store,
                &adapters,
                &AlwaysIdle,
                std::time::Duration::from_millis(1),
            ),
        )
        .await;
        assert!(result.is_err(), "the scheduler should keep running");
        assert!(*compacted.lock().unwrap() > 0);
    }

    #[tokio::test]
    async fn production_hot_ticks_do_not_repeat_usage_pruning() {
        let path = std::env::temp_dir().join(format!(
            "uw-maintenance-cadence-{}.db",
            uuid::Uuid::new_v4()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let daemon_store = Arc::new(SqliteDaemonStore::new(Store::open(&path_string).unwrap()));
        let provider = Provider::Other("cadence-probe".into());
        let window = WindowKey {
            provider: provider.clone(),
            kind: WindowKind::Rolling { minutes: 60 },
        };
        let now = Utc::now();
        let provider_requests = Arc::new(AtomicUsize::new(0));
        let adapter = Arc::new(ProviderRequestCountingAdapter {
            sample: UsageSample {
                at: now,
                fetched_at: Some(now),
                source: UsageSource::ProviderReported,
                provider: provider.clone(),
                account: None,
                windows: HashMap::from([(
                    window.clone(),
                    UsageWindowState::new(5.0, false, true, None, None),
                )]),
                credits: None,
            },
            stop: None,
            provider_requests: provider_requests.clone(),
        });
        let adapters = HashMap::from([(provider.clone(), adapter as Arc<dyn HarnessAdapter>)]);
        let daemon = tokio::spawn(async move {
            run_production_ticks(
                daemon_store,
                &adapters,
                &AlwaysIdle,
                std::time::Duration::from_millis(10),
            )
            .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while provider_requests.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the hot observation loop did not start");

        let old_at = now - Duration::days(31);
        let old = UsageSample {
            at: old_at,
            fetched_at: Some(old_at),
            source: UsageSource::ProviderReported,
            provider,
            account: None,
            windows: HashMap::from([(
                window,
                UsageWindowState::new(1.0, false, true, None, None),
            )]),
            credits: None,
        };
        Store::open(&path_string)
            .unwrap()
            .insert_usage_sample(&old)
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while provider_requests.load(Ordering::SeqCst) < 5 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the hot observation loop did not advance");
        daemon.abort();
        let _ = daemon.await;

        let old_sample_survived = Store::open(&path_string)
            .unwrap()
            .all_usage_samples()
            .unwrap()
            .into_iter()
            .any(|sample| sample.at == old_at);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }

        assert!(
            old_sample_survived,
            "pruning must not run again during successive hot observation ticks"
        );
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
        let samples = vec![make(now - Duration::minutes(31), 0.0), make(now, 99.0)];
        let adapter = FakeAdapter {
            capabilities: caps(false, false, false),
            compacted: Arc::new(StdMutex::new(0)),
            advised: Arc::new(StdMutex::new(0)),
        };
        let result = run_near_limit_tick(
            &samples,
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
            active_resume: None,
            resolved: Arc::new(StdMutex::new(vec![])),
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
        active: Option<ResumeMarker>,
        inserts: StdMutex<u32>,
        resolved: StdMutex<Vec<(uuid::Uuid, DateTime<Utc>)>>,
        owner: SessionSummary,
        due: Vec<ResumeMarker>,
        claim: Arc<StdMutex<bool>>,
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
        async fn token_usage(&self, _: &SessionId) -> anyhow::Result<Vec<TokenUsageRecord>> {
            Ok(vec![])
        }
        async fn active_resume_marker(
            &self,
            _: &SessionId,
        ) -> anyhow::Result<Option<ResumeMarker>> {
            Ok(self.active.clone())
        }
        async fn insert_resume_marker(&self, _: ResumeMarker) -> anyhow::Result<()> {
            *self.inserts.lock().unwrap() += 1;
            Ok(())
        }
        async fn set_resume_at(&self, id: uuid::Uuid, resume_at: DateTime<Utc>) -> anyhow::Result<()> {
            self.resolved.lock().unwrap().push((id, resume_at));
            Ok(())
        }
        async fn due_resume_markers(&self, _: DateTime<Utc>) -> anyhow::Result<Vec<ResumeMarker>> {
            Ok(self.due.clone())
        }
        async fn claim_resume_marker(&self, _: uuid::Uuid) -> anyhow::Result<bool> {
            Ok(std::mem::replace(&mut *self.claim.lock().unwrap(), false))
        }
        async fn resume_owner(&self, _: &SessionId) -> anyhow::Result<SessionSummary> {
            Ok(self.owner.clone())
        }
        async fn update_resume(&self, _: uuid::Uuid, _: ResumeStatus) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn near_limit_session_and_block() -> (SessionSummary, WindowBlock) {
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
        (stopped, block)
    }

    #[tokio::test]
    async fn resume_scheduler_does_not_attempt_duplicate_active_marker() {
        let (stopped, block) = near_limit_session_and_block();
        let already_scheduled = ResumeMarker {
            id: uuid::Uuid::new_v4(),
            session_id: stopped.id.clone(),
            reason: ResumeReason::AutoDetectedLimit,
            resume_at: Some(Utc::now() + Duration::minutes(10)),
            requested_at: None,
            created_at: Utc::now(),
            status: ResumeStatus::Scheduled,
            message: None,
        };
        let store = ResumeFake {
            active: Some(already_scheduled),
            inserts: StdMutex::new(0),
            resolved: StdMutex::new(vec![]),
            owner: stopped.clone(),
            due: vec![],
            claim: Arc::new(StdMutex::new(false)),
        };
        assert!(
            !reconcile_resume_marker(
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
        assert!(store.resolved.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn manual_resume_never_resolves_earlier_than_the_policy_floor() {
        let (stopped, block) = near_limit_session_and_block();
        let now = Utc::now();
        let floor = resume_floor(
            &stopped,
            &block,
            now,
            &ThresholdProfile::default(),
            "claude",
            &[],
        )
        .expect("policy floor should be computable from the block above");

        // An --at in the past (or before the floor) must not win: the marker
        // resolves to the floor, never earlier.
        let too_early = ResumeMarker {
            id: uuid::Uuid::new_v4(),
            session_id: stopped.id.clone(),
            reason: ResumeReason::ManuallyMarked,
            resume_at: None,
            requested_at: Some(floor - Duration::hours(1)),
            created_at: now,
            status: ResumeStatus::Pending,
            message: Some("resume now please".into()),
        };
        let store = ResumeFake {
            active: Some(too_early.clone()),
            inserts: StdMutex::new(0),
            resolved: StdMutex::new(vec![]),
            owner: stopped.clone(),
            due: vec![],
            claim: Arc::new(StdMutex::new(false)),
        };
        assert!(
            reconcile_resume_marker(
                &store,
                &stopped,
                &block,
                now,
                &ThresholdProfile::default(),
                "claude",
                &[]
            )
            .await
            .unwrap()
        );
        assert_eq!(store.resolved.lock().unwrap()[0], (too_early.id, floor));

        // An --at further out than the floor is honored as extra margin.
        let later = floor + Duration::hours(1);
        let ask_for_more_margin = ResumeMarker {
            requested_at: Some(later),
            ..too_early
        };
        let store = ResumeFake {
            active: Some(ask_for_more_margin.clone()),
            inserts: StdMutex::new(0),
            resolved: StdMutex::new(vec![]),
            owner: stopped.clone(),
            due: vec![],
            claim: Arc::new(StdMutex::new(false)),
        };
        assert!(
            reconcile_resume_marker(
                &store,
                &stopped,
                &block,
                now,
                &ThresholdProfile::default(),
                "claude",
                &[]
            )
            .await
            .unwrap()
        );
        assert_eq!(
            store.resolved.lock().unwrap()[0],
            (ask_for_more_margin.id, later)
        );
    }

    #[tokio::test]
    async fn concurrent_resume_ticks_only_spawn_after_winning_claim() {
        RESUME_CALLS.store(0, Ordering::SeqCst);
        let marker = ResumeMarker {
            id: uuid::Uuid::new_v4(),
            session_id: SessionId("s".into()),
            reason: ResumeReason::AutoDetectedLimit,
            resume_at: Some(Utc::now() - Duration::seconds(1)),
            requested_at: None,
            created_at: Utc::now() - Duration::minutes(1),
            status: ResumeStatus::Scheduled,
            message: None,
        };
        let store = Arc::new(ResumeFake {
            active: None,
            inserts: StdMutex::new(0),
            resolved: StdMutex::new(vec![]),
            owner: session(),
            due: vec![marker],
            claim: Arc::new(StdMutex::new(true)),
        });
        let adapter = Arc::new(FakeAdapter {
            capabilities: caps(true, false, false),
            compacted: Arc::new(StdMutex::new(0)),
            advised: Arc::new(StdMutex::new(0)),
        });
        let adapters = Arc::new(HashMap::from([(
            Provider::ClaudeCode,
            adapter as Arc<dyn HarnessAdapter>,
        )]));
        let (first, second) = tokio::join!(
            run_resume_tick(store.as_ref(), adapters.as_ref(), Utc::now()),
            run_resume_tick(store.as_ref(), adapters.as_ref(), Utc::now()),
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(RESUME_CALLS.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn superseded_session_is_not_eligible_for_destructive_compaction() {
        let mut owner = session();
        owner.superseded_by = Some(SessionId("replacement".into()));
        assert_eq!(
            plan_compaction_tick(&request(), &caps(true, true, true), true, &owner),
            CompactionPlan::SkipUnsupported(
                "session identity was superseded by replacement".into()
            )
        );
    }

    #[test]
    fn disabled_reseed_auto_never_passes_policy_gate() {
        let profile = ThresholdProfile::default();
        assert!(!uw_policy::should_auto_reseed(
            Duration::hours(2),
            Duration::minutes(5),
            200_000,
            1.0,
            10.0,
            Duration::hours(2),
            &profile.reseed_auto,
            &caps(true, true, true)
        ));
    }

    #[test]
    fn stored_threshold_values_override_the_runtime_profile() {
        let mut profile = ThresholdProfile::default();
        apply_threshold_values(
            &mut profile,
            std::collections::BTreeMap::from([
                ("closing_pct".into(), "77.5".into()),
                ("reask_max_per_epoch".into(), "3".into()),
            ]),
        )
        .unwrap();
        assert_eq!(profile.closing_pct, 77.5);
        assert_eq!(profile.reask_max_per_epoch, 3);
    }

    #[test]
    fn keepalive_requires_enabled_and_daily_cap() {
        let now = Utc::now();
        let mut profile = ThresholdProfile {
            keepalive: Some(KeepaliveConfig {
                enabled: true,
                daily_cap: 1,
            }),
            ..Default::default()
        };
        profile
            .cache_ttl_by_provider
            .insert(Provider::ClaudeCode, Duration::minutes(5));
        let session = SessionSummary {
            last_seen: now - Duration::minutes(10),
            ..session()
        };
        let disabled = KeepaliveState {
            session_id: session.id.clone(),
            enabled: false,
            last_ping_at: None,
            ping_day: None,
            ping_count: 0,
        };
        assert!(!should_fire_keepalive(now, &session, &disabled, &profile));
        let capped = KeepaliveState {
            session_id: session.id.clone(),
            enabled: true,
            last_ping_at: Some(now),
            ping_day: Some(now.date_naive().to_string()),
            ping_count: 1,
        };
        assert!(!should_fire_keepalive(now, &session, &capped, &profile));
    }

    #[test]
    fn keepalive_waits_a_full_cadence_after_the_last_ping() {
        let now = Utc::now();
        let mut profile = ThresholdProfile {
            keepalive: Some(KeepaliveConfig {
                enabled: true,
                daily_cap: 10,
            }),
            ..Default::default()
        };
        profile
            .cache_ttl_by_provider
            .insert(Provider::ClaudeCode, Duration::minutes(5));
        let session = SessionSummary {
            last_seen: now - Duration::hours(1),
            ..session()
        };
        let state = KeepaliveState {
            session_id: session.id.clone(),
            enabled: true,
            last_ping_at: Some(now - Duration::minutes(1)),
            ping_day: Some(now.date_naive().to_string()),
            ping_count: 1,
        };
        assert!(!should_fire_keepalive(now, &session, &state, &profile));
    }

    #[tokio::test]
    async fn keepalive_uses_advise_and_exact_marker() {
        struct KStore {
            session: SessionSummary,
            state: KeepaliveState,
            recorded: StdMutex<u32>,
        }
        #[async_trait]
        impl KeepaliveStore for KStore {
            async fn keepalive_sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
                Ok(vec![self.session.clone()])
            }
            async fn keepalive_state(&self, _: &SessionId) -> anyhow::Result<KeepaliveState> {
                Ok(self.state.clone())
            }
            async fn record_keepalive_ping(
                &self,
                _: &SessionId,
                _: DateTime<Utc>,
            ) -> anyhow::Result<()> {
                *self.recorded.lock().unwrap() += 1;
                Ok(())
            }
        }
        let now = Utc::now();
        let session = SessionSummary {
            last_seen: now - Duration::minutes(10),
            ..session()
        };
        let mut profile = ThresholdProfile {
            keepalive: Some(KeepaliveConfig {
                enabled: true,
                daily_cap: 2,
            }),
            ..Default::default()
        };
        profile
            .cache_ttl_by_provider
            .insert(Provider::ClaudeCode, Duration::minutes(5));
        let advised = Arc::new(StdMutex::new(0));
        let adapter = Arc::new(FakeAdapter {
            capabilities: caps(false, true, false),
            compacted: Arc::new(StdMutex::new(0)),
            advised: advised.clone(),
        });
        let store = KStore {
            session: session.clone(),
            state: KeepaliveState {
                session_id: session.id.clone(),
                enabled: true,
                last_ping_at: None,
                ping_day: None,
                ping_count: 0,
            },
            recorded: StdMutex::new(0),
        };
        assert_eq!(
            run_keepalive_tick(
                &store,
                &HashMap::from([(Provider::ClaudeCode, adapter as Arc<dyn HarnessAdapter>)]),
                now,
                &HashMap::from([(session.id.clone(), profile)])
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(*advised.lock().unwrap(), 1);
        assert_eq!(
            KEEPALIVE_MARKER,
            "[[uw-keepalive]] no action needed, acknowledge briefly"
        );
    }
}
