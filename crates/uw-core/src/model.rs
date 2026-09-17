use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Provider {
    ClaudeCode,
    Codex,
    Cursor,
    Gemini,
    Other(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelId(pub String);
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountId(pub String);
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub String);
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WindowKind {
    Rolling { minutes: u32 },
    WeeklyModel(ModelId),
    WeeklySurface(String),
    Custom(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WindowKey {
    pub provider: Provider,
    pub kind: WindowKind,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageWindowState {
    pub pct: f32,
    pub resets_at: Option<DateTime<Utc>>,
    pub exceeded: bool,
    pub active: bool,
    pub scope: Option<AccountId>,
}
impl UsageWindowState {
    pub fn new(
        pct: f32,
        exceeded: bool,
        active: bool,
        resets_at: Option<DateTime<Utc>>,
        scope: Option<AccountId>,
    ) -> Self {
        Self {
            pct: pct.clamp(0.0, 100.0),
            resets_at,
            exceeded,
            active,
            scope,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    Notice,
    Closing,
    Compact,
    Exceeded,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UsageSource {
    ProviderReported,
    LocalEstimate,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageSample {
    pub at: DateTime<Utc>,
    pub fetched_at: Option<DateTime<Utc>>,
    pub source: UsageSource,
    pub provider: Provider,
    pub account: Option<AccountId>,
    #[serde(with = "window_map")]
    pub windows: HashMap<WindowKey, UsageWindowState>,
    pub credits: Option<CreditBalance>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreditBalance {
    pub remaining: f64,
    pub currency: String,
}
mod window_map {
    use super::*;
    use serde::{de::Error as DeError, ser::Error as SerError};
    pub fn serialize<S: serde::Serializer>(
        map: &HashMap<WindowKey, UsageWindowState>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let mut out = HashMap::new();
        for (k, v) in map {
            out.insert(serde_json::to_string(k).map_err(S::Error::custom)?, v);
        }
        out.serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<HashMap<WindowKey, UsageWindowState>, D::Error> {
        let raw: HashMap<String, UsageWindowState> = HashMap::deserialize(d)?;
        raw.into_iter()
            .map(|(k, v)| Ok((serde_json::from_str(&k).map_err(D::Error::custom)?, v)))
            .collect()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: SessionId,
    pub harness: Provider,
    pub model: Option<ModelId>,
    pub account: Option<AccountId>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub cwd: String,
    pub state_path: Option<String>,
    pub context_window_size: Option<u64>,
    pub last_known_token_count: Option<u64>,
    pub launch_mode: LaunchMode,
    pub pid: Option<u32>,
    pub stopped_reason: Option<StopReason>,
    pub resume_marker: Option<ResumeMarker>,
    pub superseded_by: Option<SessionId>,
    pub reseeded_from: Option<SessionId>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LaunchMode {
    Interactive,
    Headless,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopReason {
    UsageLimit { window: WindowKey },
    UserQuit,
    Crashed,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeMarker {
    pub id: Uuid,
    pub session_id: SessionId,
    pub reason: ResumeReason,
    pub resume_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub status: ResumeStatus,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResumeReason {
    AutoDetectedLimit,
    ManuallyMarked,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResumeStatus {
    Pending,
    Scheduled,
    Fired,
    Cancelled,
    Failed(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionRequest {
    pub id: Uuid,
    pub session_id: SessionId,
    pub kind: CompactionKind,
    pub prompt: String,
    pub reason: String,
    pub status: CompactionStatus,
    pub created_at: DateTime<Utc>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionKind {
    AskNearLimit,
    OpportunisticIdle,
    AltModelReseed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionStatus {
    Pending,
    Sending,
    Sent,
    Failed(String),
    Cancelled,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct IdleCompactConfig {
    pub tiers: Vec<TokenTier>,
    pub margin: Duration,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenTier {
    pub window_size_floor: u64,
    pub token_threshold: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct ReseedAutoConfig {
    pub enabled: bool,
    pub min_tokens: u64,
    pub cooldown: Duration,
    pub margin: Duration,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeepaliveConfig {
    pub enabled: bool,
    pub daily_cap: u32,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CacheWritePrice {
    pub model_prefix: String,
    pub usd_per_mtok: f64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ThresholdProfile {
    pub notice_pct: f32,
    pub closing_pct: f32,
    pub compact_pct: f32,
    pub plan_pressure_pct: f32,
    pub plan_pressure_min_tokens: u64,
    pub burn_multiplier: f32,
    pub idle_compact: IdleCompactConfig,
    pub reseed_auto: ReseedAutoConfig,
    pub keepalive: Option<KeepaliveConfig>,
    pub overhead_pct: f32,
    pub min_lead_minutes: f32,
    pub max_lead_minutes: f32,
    pub reask_delta_pct: f32,
    pub reask_max_per_epoch: u32,
    pub cache_write_price_table: Vec<CacheWritePrice>,
    pub cache_write_fallback_pct: f32,
    pub cache_ttl_by_provider: HashMap<Provider, Duration>,
}
impl Default for ThresholdProfile {
    fn default() -> Self {
        Self {
            notice_pct: 70.0,
            closing_pct: 85.0,
            compact_pct: 90.0,
            plan_pressure_pct: 95.0,
            plan_pressure_min_tokens: 0,
            burn_multiplier: 1.0,
            idle_compact: IdleCompactConfig {
                tiers: vec![
                    TokenTier {
                        window_size_floor: 0,
                        token_threshold: 100_000,
                    },
                    TokenTier {
                        window_size_floor: 201_000,
                        token_threshold: 200_000,
                    },
                ],
                margin: Duration::minutes(1),
            },
            reseed_auto: Default::default(),
            keepalive: None,
            overhead_pct: 30.0,
            min_lead_minutes: 5.0,
            max_lead_minutes: 60.0,
            reask_delta_pct: 5.0,
            reask_max_per_epoch: 1,
            cache_write_price_table: vec![],
            cache_write_fallback_pct: 1.0,
            cache_ttl_by_provider: HashMap::new(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThresholdScope {
    pub provider: Provider,
    pub model: Option<ModelId>,
    pub session: Option<SessionId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;

    #[test]
    fn window_keys_are_duration_sensitive() {
        let a = WindowKey {
            provider: Provider::Codex,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        let b = WindowKey {
            provider: Provider::Codex,
            kind: WindowKind::Rolling { minutes: 301 },
        };
        let mut map = HashMap::new();
        map.insert(a, 1);
        assert_eq!(map.get(&b), None);
    }

    #[test]
    fn usage_sample_round_trips_through_serde() {
        let key = WindowKey {
            provider: Provider::ClaudeCode,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        let sample = UsageSample {
            at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            fetched_at: None,
            source: UsageSource::ProviderReported,
            provider: Provider::ClaudeCode,
            account: Some(AccountId("a".into())),
            windows: HashMap::from([(
                key,
                UsageWindowState {
                    pct: 42.0,
                    resets_at: None,
                    exceeded: false,
                    active: true,
                    scope: None,
                },
            )]),
            credits: None,
        };
        let encoded = serde_json::to_string(&sample).unwrap();
        assert_eq!(sample, serde_json::from_str(&encoded).unwrap());
    }

    #[test]
    fn session_and_threshold_profiles_round_trip_through_serde() {
        let session = SessionSummary {
            id: SessionId("s".into()),
            harness: Provider::Codex,
            model: None,
            account: None,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            cwd: "/tmp".into(),
            state_path: None,
            context_window_size: Some(200_000),
            last_known_token_count: Some(100),
            launch_mode: LaunchMode::Headless,
            pid: None,
            stopped_reason: None,
            resume_marker: None,
            superseded_by: Some(SessionId("s2".into())),
            reseeded_from: None,
        };
        assert_eq!(
            session,
            serde_json::from_str(&serde_json::to_string(&session).unwrap()).unwrap()
        );
        let profile = ThresholdProfile::default();
        assert_eq!(
            profile,
            serde_json::from_str(&serde_json::to_string(&profile).unwrap()).unwrap()
        );
    }

    #[test]
    fn percentage_constructor_clamps() {
        assert_eq!(
            UsageWindowState::new(150.0, false, true, None, None).pct,
            100.0
        );
        assert_eq!(
            UsageWindowState::new(-2.0, false, true, None, None).pct,
            0.0
        );
    }
}
