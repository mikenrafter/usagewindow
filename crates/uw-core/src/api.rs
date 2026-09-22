use crate::model::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatusResponse {
    pub usage: Vec<ProviderUsageSummary>,
    pub last_updated: DateTime<Utc>,
    pub provider_status: Vec<ProviderFetchStatus>,
    pub keepalive_active_count: u32,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderUsageSummary {
    pub provider: Provider,
    pub account: Option<AccountId>,
    pub plan: Option<String>,
    pub windows: Vec<UsageWindowSummary>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageWindowSummary {
    pub window: WindowKind,
    pub pct: f32,
    pub resets_at: Option<DateTime<Utc>>,
    pub exceeded: bool,
    pub burn_rate_pct_per_hour: Option<f32>,
    pub active_sessions: u32,
    pub keptalive_sessions: u32,
    pub scheduled_sessions: u32,
    pub depletes_at: Option<DateTime<Utc>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionListItem {
    pub id: SessionId,
    /// Resolved display title: this session's own title if it has one, else
    /// the nearest titled ancestor's via `reseeded_from`. `None` when nothing
    /// in the chain has a title — the UI falls back to `id`.
    pub title: Option<String>,
    pub harness: Provider,
    pub model: Option<ModelId>,
    pub account: Option<AccountId>,
    pub last_seen: DateTime<Utc>,
    pub stopped_reason: Option<StopReason>,
    pub resume_status: Option<ResumeStatus>,
    pub compaction_status: Option<CompactionStatus>,
    pub keepalive: bool,
    pub active: bool,
    pub cached: bool,
    pub context_window_size: Option<u64>,
    pub last_known_token_count: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionsPage {
    pub items: Vec<SessionListItem>,
    pub total: u32,
    pub offset: u32,
    pub limit: u32,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionDetail {
    pub summary: SessionSummary,
    /// Resolved display title (see `SessionListItem::title`).
    pub title: Option<String>,
    pub history: Vec<SparklinePoint>,
    pub resume_controls: ResumeControls,
    pub compaction_log: Vec<CompactionRequest>,
    pub reseed_lineage: Vec<SessionId>,
    pub errors: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    pub id: SessionId,
    pub harness: Provider,
    pub cwd: String,
    pub model: Option<ModelId>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateSessionRequest {
    pub cwd: String,
    pub model: Option<ModelId>,
    pub account: Option<AccountId>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SupersedeStopRequest {
    pub note: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetFetchNonBlockingRequest {
    pub provider: Provider,
    pub account: Option<AccountId>,
    pub non_blocking: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SparklinePoint {
    pub at: DateTime<Utc>,
    pub pct: f32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeControls {
    pub can_resume: bool,
    pub marker: Option<ResumeMarker>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeRequest {
    pub session_id: SessionId,
    pub at: Option<DateTime<Utc>>,
    pub message: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeResponse {
    pub marker: Option<ResumeMarker>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelResumeRequest {
    pub session_id: SessionId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactAskRequest {
    pub session_id: SessionId,
    pub reason: Option<String>,
    #[serde(default)]
    pub resume_after_compaction: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelCompactRequest {
    pub session_id: SessionId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactStatusResponse {
    pub requests: Vec<CompactionRequest>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecentAction {
    pub session_id: SessionId,
    pub session_title: Option<String>,
    pub kind: String,
    pub status: String,
    pub at: DateTime<Utc>,
    pub detail: Option<String>,
    pub error: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThresholdGetRequest {
    pub scope: Option<ThresholdScope>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThresholdSetRequest {
    pub scope: ThresholdScope,
    pub field: String,
    pub value: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThresholdResponse {
    pub scope: Option<ThresholdScope>,
    pub values: std::collections::BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;

    fn round_trip<T>(value: T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_string(&value).unwrap();
        assert_eq!(value, serde_json::from_str(&encoded).unwrap());
    }

    #[test]
    fn status_round_trips() {
        round_trip(StatusResponse {
            usage: vec![ProviderUsageSummary {
                provider: Provider::Codex,
                account: Some(AccountId("a".into())),
                plan: Some("Plus".into()),
                windows: vec![UsageWindowSummary {
                    window: WindowKind::Rolling { minutes: 300 },
                    pct: 42.0,
                    resets_at: None,
                    exceeded: false,
                    burn_rate_pct_per_hour: Some(1.5),
                    active_sessions: 2,
                    keptalive_sessions: 1,
                    scheduled_sessions: 0,
                    depletes_at: None,
                }],
            }],
            last_updated: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            provider_status: vec![ProviderFetchStatus {
                provider: Provider::Codex,
                account: None,
                last_attempt_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                last_success_at: None,
                last_error: Some("timeout".into()),
                consecutive_failures: 1,
                non_blocking: false,
            }],
            keepalive_active_count: 3,
        });
    }

    #[test]
    fn session_views_round_trip() {
        let item = SessionListItem {
            id: SessionId("s".into()),
            title: Some("A titled session".into()),
            harness: Provider::Codex,
            model: None,
            account: None,
            context_window_size: None,
            last_known_token_count: None,
            last_seen: Utc::now(),
            stopped_reason: None,
            resume_status: None,
            compaction_status: Some(CompactionStatus::Failed("adapter error".into())),
            keepalive: false,
            active: true,
            cached: true,
        };
        round_trip(item.clone());
        round_trip(SessionsPage {
            items: vec![item],
            total: 1,
            offset: 0,
            limit: 15,
        });
        round_trip(SessionDetail {
            summary: SessionSummary {
                id: SessionId("s".into()),
                harness: Provider::Codex,
                model: None,
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
                superseded_stop_reason: None,
                superseded_stop_reason_at: None,
                superseded_stop_reason_note: None,
                resume_marker: None,
                superseded_by: None,
                reseeded_from: None,
            },
            title: Some("A titled session".into()),
            history: vec![SparklinePoint {
                at: Utc::now(),
                pct: 50.0,
            }],
            resume_controls: ResumeControls {
                can_resume: true,
                marker: None,
            },
            compaction_log: vec![],
            reseed_lineage: vec![],
            errors: vec![],
        });
    }

    #[test]
    fn command_dtos_round_trip() {
        round_trip(ResumeRequest {
            session_id: SessionId("s".into()),
            at: None,
            message: None,
        });
        round_trip(ResumeResponse { marker: None });
        round_trip(CancelResumeRequest {
            session_id: SessionId("s".into()),
        });
        round_trip(CompactAskRequest {
            session_id: SessionId("s".into()),
            reason: Some("r".into()),
            resume_after_compaction: true,
        });
        round_trip(CancelCompactRequest {
            session_id: SessionId("s".into()),
        });
        round_trip(CompactStatusResponse { requests: vec![] });
        round_trip(SupersedeStopRequest {
            note: Some("t3code errors are non-blocking".into()),
        });
        round_trip(SetFetchNonBlockingRequest {
            provider: Provider::Other("t3code".into()),
            account: None,
            non_blocking: true,
        });
        round_trip(ThresholdGetRequest { scope: None });
        round_trip(ThresholdSetRequest {
            scope: ThresholdScope {
                provider: Provider::Codex,
                model: None,
                session: None,
            },
            field: "closing_pct".into(),
            value: "85".into(),
        });
    }

    #[test]
    fn compaction_event_dto_round_trips() {
        round_trip(CompactionEvent {
            id: uuid::Uuid::new_v4(),
            session_id: SessionId("s".into()),
            source: CompactionSource::External,
            trigger: Some("auto".into()),
            context_pct_before: Some(90.0),
            usage_window_pct_before: Some(60.0),
            tokens_before: Some(150_000),
            tokens_after: None,
            started_at: Utc::now(),
            completed_at: None,
        });
    }

    #[test]
    fn api_shapes_are_plain_serde_values() {
        let _: HashMap<String, String> = serde_json::from_str(
            &serde_json::to_string(&ThresholdGetRequest { scope: None }).unwrap(),
        )
        .unwrap_or_default();
    }
}
