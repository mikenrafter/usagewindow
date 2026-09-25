use crate::model::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("unsupported")]
    Unsupported,
    #[error("authentication failed")]
    Auth,
    #[error("transient adapter failure: {0}")]
    Transient(String),
    #[error("adapter error: {0}")]
    Other(String),
}
pub type AdapterResult<T> = Result<T, AdapterError>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Whether the harness exposes a destructive compaction command (false for Codex,
    /// whose compaction is automatic and opaque).
    pub can_trigger_compaction: bool,
    /// Whether advisory text can ride an active turn (true for Claude Code's Stop hook;
    /// Codex has no equivalent passive mid-turn channel).
    pub can_advise_mid_turn: bool,
    /// Whether status text can be injected at a session boundary (both supported
    /// harnesses provide a SessionStart channel).
    pub can_inject_at_session_start: bool,
    /// Whether a compaction can be observed after it lands (hooks/token watching).
    pub can_observe_compaction: bool,
    /// Whether token counts are trustworthy; policies must skip token-dependent work
    /// rather than guess when this is false (currently false for Codex).
    pub reports_token_counts: bool,
    /// Whether the harness can relaunch a session without an interactive terminal.
    pub headless_resume: bool,
    /// Fresh-session and history-fork primitives available from this adapter.
    pub seed_modes: Vec<SeedMode>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeedMode {
    InitialPrompt,
    ForkWithHistory,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedContext {
    pub from_session: Option<SessionId>,
    pub summary: String,
    pub model: ModelId,
    pub cwd: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusEvent {
    WillAutoResumeAt(DateTime<Utc>),
    CompactedFromTo {
        before_tokens: u64,
        after_tokens: u64,
    },
    AutoResumeCanceled,
    Custom(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryOutcome {
    Delivered,
    QueuedForNextIdle,
    Unsupported,
}
/// A session the harness itself knows about (e.g. an on-disk session record) that
/// usagewindow hasn't necessarily seen via a hook yet. Fields beyond `id`/`cwd` are
/// best-effort: an adapter fills in whatever its on-disk record actually carries and
/// leaves the rest `None` rather than guessing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredSession {
    pub id: SessionId,
    #[serde(default)]
    pub lineage: SessionLineage,
    pub cwd: String,
    pub model: Option<ModelId>,
    pub context_window_size: Option<u64>,
    pub last_known_token_count: Option<u64>,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    /// Path to the on-disk record backing this session, if any — lets later reads
    /// (e.g. a transcript preview) go straight to it without re-scanning.
    pub state_path: Option<String>,
    /// Per-turn token usage recovered from the session's on-disk rollout.
    pub token_usage: Vec<TokenUsageRecord>,
    /// A human-readable title recovered from the on-disk record, if the
    /// harness (or a tool observing it) writes one. `None` when the harness
    /// has no title concept — the UI falls back to the session id.
    pub title: Option<String>,
}
/// Token accounting emitted by a harness for one response. Cached input is a
/// subset of input tokens; cache-write tokens are reported separately when the
/// harness exposes them. `reasoning_output_tokens` is a subtype of output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsageRecord {
    pub at: DateTime<Utc>,
    pub model: Option<ModelId>,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
}
/// One user- or assistant-authored turn, for a short "remind me what this session was
/// about" preview — not a full transcript export.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnPreview {
    pub role: TurnRole,
    pub text: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TurnRole {
    User,
    Assistant,
}

#[async_trait]
pub trait HarnessAdapter: Send + Sync {
    fn provider(&self) -> Provider;
    fn capabilities(&self) -> Capabilities;
    async fn fetch_usage(&self, account: Option<&AccountId>) -> AdapterResult<UsageSample>;
    async fn detect_stop(&self, session_id: &SessionId) -> AdapterResult<Option<StopReason>>;
    /// Check for a stop using a provider sample already fetched in this tick.
    /// Adapters that need a separate signal may fall back to `detect_stop`.
    async fn detect_stop_with_usage(
        &self,
        session_id: &SessionId,
        _sample: Option<&UsageSample>,
    ) -> AdapterResult<Option<StopReason>> {
        self.detect_stop(session_id).await
    }
    async fn emit_status(
        &self,
        session_id: &SessionId,
        status: StatusEvent,
    ) -> AdapterResult<DeliveryOutcome>;
    async fn advise(&self, session_id: &SessionId, text: &str) -> AdapterResult<DeliveryOutcome>;
    /// Interrupt a currently running turn without otherwise changing session state.
    /// Adapters that cannot target a live turn return `Unsupported`.
    async fn interrupt(&self, _session_id: &SessionId) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }
    async fn compact(
        &self,
        session: &SessionSummary,
        req: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome>;
    async fn resume_session(
        &self,
        session: &SessionSummary,
        message: Option<&str>,
    ) -> AdapterResult<()>;
    /// Set or clear a producer-owned UI status overlay. Harnesses that do not
    /// expose such a metadata surface keep the default unsupported behavior.
    async fn set_external_status(
        &self,
        _session: &SessionSummary,
        _status: Option<Value>,
    ) -> AdapterResult<()> {
        Err(AdapterError::Unsupported)
    }
    /// Export a read-only transcript. Implementations may support this even when the
    /// harness session is stopped; adapters must omit usagewindow keepalive turns.
    async fn export_transcript(&self, _session: &SessionSummary) -> AdapterResult<String> {
        Err(AdapterError::Unsupported)
    }
    async fn seed_new_session(
        &self,
        mode: SeedMode,
        seed: &SeedContext,
    ) -> AdapterResult<SessionId> {
        let _ = (mode, seed);
        Err(AdapterError::Unsupported)
    }
    /// Finds sessions the harness already knows about independent of hooks (e.g. by
    /// reading its own on-disk session records), so a session that was never wired up
    /// to report `SessionStart` can still be tracked and later resumed. Adapters
    /// without a discoverable session store return `Unsupported`; the daemon then
    /// relies on hooks alone for that harness.
    async fn discover_sessions(&self) -> AdapterResult<Vec<DiscoveredSession>> {
        Err(AdapterError::Unsupported)
    }
    /// The first two and last two user/assistant turns, for a quick "what was this
    /// session about" refresh — deliberately not a full transcript (see
    /// `export_transcript`'s stability caveat). Adapters without a readable record
    /// return `Unsupported`.
    async fn session_preview(&self, _session: &SessionSummary) -> AdapterResult<Vec<TurnPreview>> {
        Err(AdapterError::Unsupported)
    }
}
