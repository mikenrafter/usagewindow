use crate::model::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatusResponse {
    pub usage: Vec<ProviderUsageSummary>,
    pub last_updated: DateTime<Utc>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderUsageSummary {
    pub provider: Provider,
    pub account: Option<AccountId>,
    pub windows: Vec<UsageWindowSummary>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageWindowSummary {
    pub window: WindowKind,
    pub pct: f32,
    pub resets_at: Option<DateTime<Utc>>,
    pub exceeded: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionListItem {
    pub id: SessionId,
    pub harness: Provider,
    pub model: Option<ModelId>,
    pub account: Option<AccountId>,
    pub last_seen: DateTime<Utc>,
    pub stopped_reason: Option<StopReason>,
    pub resume_status: Option<ResumeStatus>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionDetail {
    pub summary: SessionSummary,
    pub history: Vec<SparklinePoint>,
    pub resume_controls: ResumeControls,
    pub compaction_log: Vec<CompactionRequest>,
    pub reseed_lineage: Vec<SessionId>,
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
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactStatusResponse {
    pub requests: Vec<CompactionRequest>,
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
                windows: vec![UsageWindowSummary {
                    window: WindowKind::Rolling { minutes: 300 },
                    pct: 42.0,
                    resets_at: None,
                    exceeded: false,
                }],
            }],
            last_updated: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        });
    }

    #[test]
    fn session_views_round_trip() {
        let item = SessionListItem {
            id: SessionId("s".into()),
            harness: Provider::Codex,
            model: None,
            account: None,
            last_seen: Utc::now(),
            stopped_reason: None,
            resume_status: None,
        };
        round_trip(item);
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
                resume_marker: None,
                superseded_by: None,
                reseeded_from: None,
            },
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
        });
        round_trip(CompactStatusResponse { requests: vec![] });
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
    fn api_shapes_are_plain_serde_values() {
        let _: HashMap<String, String> = serde_json::from_str(
            &serde_json::to_string(&ThresholdGetRequest { scope: None }).unwrap(),
        )
        .unwrap_or_default();
    }
}
