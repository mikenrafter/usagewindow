use crate::model::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("unsupported")]
    Unsupported,
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

#[async_trait]
pub trait HarnessAdapter {
    fn provider(&self) -> Provider;
    fn capabilities(&self) -> Capabilities;
    async fn fetch_usage(&self, account: Option<&AccountId>) -> AdapterResult<UsageSample>;
    async fn detect_stop(&self, session_id: &SessionId) -> AdapterResult<Option<StopReason>>;
    async fn emit_status(
        &self,
        session_id: &SessionId,
        status: StatusEvent,
    ) -> AdapterResult<DeliveryOutcome>;
    async fn advise(&self, session_id: &SessionId, text: &str) -> AdapterResult<DeliveryOutcome>;
    async fn compact(
        &self,
        session_id: &SessionId,
        req: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome>;
    async fn resume_session(&self, session: &SessionSummary) -> AdapterResult<()>;
    async fn seed_new_session(
        &self,
        mode: SeedMode,
        seed: &SeedContext,
    ) -> AdapterResult<SessionId> {
        let _ = (mode, seed);
        Err(AdapterError::Unsupported)
    }
}
