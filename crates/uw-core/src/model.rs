use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Approximation used when a harness does not expose explicit prompt-cache
/// state. Keep this in the domain model so consumers do not invent their own
/// cache-warm checks.
pub const CACHE_WARM_APPROXIMATION_MINUTES: i64 = 5;

pub fn is_cache_warm(last_seen: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    is_cache_warm_for(last_seen, now, Duration::minutes(CACHE_WARM_APPROXIMATION_MINUTES))
}

pub fn is_cache_warm_for(last_seen: DateTime<Utc>, now: DateTime<Utc>, ttl: Duration) -> bool {
    ttl > Duration::zero() && now - last_seen <= ttl
}

/// Matches the web UI's active-session filter: a live session with token
/// accounting in the last thirty minutes.
pub const SESSION_ACTIVE_TOKEN_LOOKBACK_MINUTES: i64 = 30;

pub fn session_has_recent_token_activity(
    records: &[crate::adapter::TokenUsageRecord],
    now: DateTime<Utc>,
) -> bool {
    records.iter().any(|record| {
        record.at >= now - Duration::minutes(SESSION_ACTIVE_TOKEN_LOOKBACK_MINUTES)
            && record.total_tokens > 0
    })
}

/// Keepalive only applies while the provider prompt cache is still warm and
/// the session is actively consuming quota — not for cold archives discovered
/// from historical transcripts.
pub fn is_keepalive_eligible(
    session: &SessionSummary,
    token_records: &[crate::adapter::TokenUsageRecord],
    now: DateTime<Utc>,
) -> bool {
    session.stopped_reason.is_none()
        && is_cache_warm(session.last_seen, now)
        && session_has_recent_token_activity(token_records, now)
}
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Provider {
    ClaudeCode,
    Codex,
    Cursor,
    Gemini,
    Other(String),
}
impl FromStr for Provider {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "claude-code" | "ClaudeCode" => Self::ClaudeCode,
            "codex" | "Codex" => Self::Codex,
            "cursor" | "Cursor" => Self::Cursor,
            "gemini" | "Gemini" => Self::Gemini,
            other if !other.is_empty() => Self::Other(other.into()),
            _ => return Err("provider cannot be empty".into()),
        })
    }
}
impl FromStr for ModelId {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.into()))
    }
}
impl FromStr for AccountId {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.into()))
    }
}
impl FromStr for SessionId {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.into()))
    }
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
    /// Human-readable subscription tier as the provider names it (e.g. "Pro",
    /// "Plus"), read from the same account/auth lookup as `account`. `None`
    /// when the harness exposes no plan concept or the lookup failed.
    pub plan: Option<String>,
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
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLineage {
    #[serde(default)]
    pub parent: Option<SessionId>,
    #[serde(default)]
    pub root: Option<SessionId>,
    #[serde(default)]
    pub related: Vec<SessionId>,
}

impl SessionLineage {
    pub fn is_empty(&self) -> bool {
        self.parent.is_none() && self.root.is_none() && self.related.is_empty()
    }

    /// Native session ids that may identify the same logical execution, ordered
    /// from the most specific id to the least specific relationship.
    pub fn ownership_candidates<'a>(&'a self, requested: &'a SessionId) -> Vec<&'a SessionId> {
        let mut candidates = vec![requested];
        for candidate in self
            .parent
            .iter()
            .chain(self.root.iter())
            .chain(self.related.iter())
        {
            if !candidates.contains(&candidate) {
                candidates.push(candidate);
            }
        }
        candidates
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: SessionId,
    #[serde(default)]
    pub lineage: SessionLineage,
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
    /// The stop reason this session had before it was manually superseded
    /// (e.g. a stale/incorrect crash detection). Superseding clears
    /// `stopped_reason` back to `None` — so the session is eligible to be
    /// treated as active again — while keeping the overridden reason here
    /// for audit.
    pub superseded_stop_reason: Option<StopReason>,
    pub superseded_stop_reason_at: Option<DateTime<Utc>>,
    pub superseded_stop_reason_note: Option<String>,
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
    /// The actual scheduled fire time. `None` until the policy engine has
    /// resolved a manually queued resume against the session's window/burn
    /// rate; never fires before that resolution happens.
    pub resume_at: Option<DateTime<Utc>>,
    /// The user-supplied `--at` floor for a manual resume, if any. Combined
    /// with the policy-computed window boundary as `max(requested_at, floor)`
    /// when `resume_at` is resolved — it can only push the fire time later,
    /// never earlier.
    pub requested_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub status: ResumeStatus,
    pub message: Option<String>,
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
    #[serde(default)]
    pub resume_after_compaction: bool,
    pub status: CompactionStatus,
    pub created_at: DateTime<Utc>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionKind {
    AskNearLimit,
    AgentRequested,
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IdleCompactConfig {
    pub tiers: Vec<TokenTier>,
    pub margin: Duration,
    #[serde(default = "default_unknown_context_token_threshold")]
    pub unknown_context_token_threshold: u64,
}

fn default_unknown_context_token_threshold() -> u64 {
    150_000
}

impl Default for IdleCompactConfig {
    fn default() -> Self {
        Self {
            tiers: Vec::new(),
            margin: Duration::zero(),
            unknown_context_token_threshold: default_unknown_context_token_threshold(),
        }
    }
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeepaliveState {
    pub session_id: SessionId,
    pub enabled: bool,
    pub last_ping_at: Option<DateTime<Utc>>,
    pub ping_day: Option<String>,
    pub ping_count: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReseedSummary {
    pub id: Uuid,
    pub session_id: SessionId,
    pub source_model: ModelId,
    pub summary_text: String,
    pub token_count_before: u64,
    pub token_count_after: u64,
    pub created_at: DateTime<Utc>,
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
                        window_size_floor: 200_000,
                        token_threshold: 150_000,
                    },
                    TokenTier {
                        window_size_floor: 300_000,
                        token_threshold: 200_000,
                    },
                ],
                // 2m leaves ~policy+compaction cadence headroom so delivery
                // lands inside a 5m warm window (~4–4.5m), not after expiry.
                margin: Duration::minutes(2),
                unknown_context_token_threshold: 150_000,
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderFetchStatus {
    pub provider: Provider,
    pub account: Option<AccountId>,
    pub last_attempt_at: DateTime<Utc>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    /// Set when these failures are known not to indicate a real problem
    /// (e.g. an adapter that doesn't support usage fetching at all).
    /// Failures still accumulate normally; this only tells consumers to
    /// stop surfacing them as an alert.
    pub non_blocking: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionSource {
    Inline,
    External,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionEvent {
    pub id: Uuid,
    pub session_id: SessionId,
    pub source: CompactionSource,
    pub trigger: Option<String>,
    pub context_pct_before: Option<f32>,
    pub usage_window_pct_before: Option<f32>,
    pub tokens_before: Option<u64>,
    pub tokens_after: Option<u64>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
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
            plan: None,
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
            lineage: SessionLineage::default(),
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
            superseded_stop_reason: None,
            superseded_stop_reason_at: None,
            superseded_stop_reason_note: None,
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
    fn session_lineage_defaults_when_older_json_omits_it() {
        let session = SessionSummary {
            id: SessionId("legacy-session".into()),
            lineage: SessionLineage::default(),
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
        };
        let mut encoded = serde_json::to_value(session).unwrap();
        encoded.as_object_mut().unwrap().remove("lineage");

        let decoded: SessionSummary = serde_json::from_value(encoded).unwrap();

        assert_eq!(decoded.lineage, SessionLineage::default());
    }

    #[test]
    fn session_lineage_orders_and_deduplicates_ownership_candidates() {
        let requested = SessionId("requested".into());
        let lineage = SessionLineage {
            parent: Some(SessionId("parent".into())),
            root: Some(SessionId("root".into())),
            related: vec![
                SessionId("parent".into()),
                SessionId("related".into()),
                SessionId("requested".into()),
            ],
        };

        let candidates = lineage
            .ownership_candidates(&requested)
            .into_iter()
            .map(|id| id.0.as_str())
            .collect::<Vec<_>>();

        assert_eq!(candidates, ["requested", "parent", "root", "related"]);
    }

    #[test]
    fn provider_fetch_status_and_compaction_event_round_trip_through_serde() {
        let status = ProviderFetchStatus {
            provider: Provider::Codex,
            account: None,
            last_attempt_at: Utc::now(),
            last_success_at: Some(Utc::now()),
            last_error: Some("timeout".into()),
            consecutive_failures: 2,
            non_blocking: false,
        };
        assert_eq!(
            status,
            serde_json::from_str(&serde_json::to_string(&status).unwrap()).unwrap()
        );
        let event = CompactionEvent {
            id: Uuid::new_v4(),
            session_id: SessionId("s".into()),
            source: CompactionSource::Inline,
            trigger: Some("manual".into()),
            context_pct_before: Some(80.0),
            usage_window_pct_before: Some(50.0),
            tokens_before: Some(100_000),
            tokens_after: Some(10_000),
            started_at: Utc::now(),
            completed_at: None,
        };
        assert_eq!(
            event,
            serde_json::from_str(&serde_json::to_string(&event).unwrap()).unwrap()
        );
    }

    #[test]
    fn keepalive_eligibility_requires_warm_cache_and_recent_tokens() {
        use crate::adapter::TokenUsageRecord;
        let now = Utc::now();
        let mut session = SessionSummary {
            id: SessionId("s".into()),
            lineage: SessionLineage::default(),
            harness: Provider::ClaudeCode,
            model: None,
            account: None,
            first_seen: now,
            last_seen: now - Duration::minutes(1),
            cwd: ".".into(),
            state_path: None,
            context_window_size: None,
            last_known_token_count: None,
            launch_mode: LaunchMode::Interactive,
            pid: None,
            stopped_reason: None,
            superseded_stop_reason: None,
            superseded_stop_reason_at: None,
            superseded_stop_reason_note: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        };
        let recent = vec![TokenUsageRecord {
            at: now - Duration::minutes(5),
            model: None,
            input_tokens: 10,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 5,
            reasoning_output_tokens: 0,
            total_tokens: 15,
        }];
        assert!(is_keepalive_eligible(&session, &recent, now));
        session.last_seen = now - Duration::minutes(10);
        assert!(!is_keepalive_eligible(&session, &recent, now));
        session.last_seen = now - Duration::minutes(1);
        assert!(!is_keepalive_eligible(&session, &[], now));
    }

    #[test]
    fn session_has_recent_token_activity_counts_a_nonzero_context_pct_with_zero_tokens() {
        use crate::adapter::TokenUsageRecord;
        let now = Utc::now();
        let records = vec![TokenUsageRecord {
            at: now - Duration::minutes(5),
            model: None,
            input_tokens: 0,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 0,
            reasoning_output_tokens: 0,
            total_tokens: 0,
            context_pct: Some(12.0),
        }];
        assert!(session_has_recent_token_activity(&records, now));
    }

    #[test]
    fn session_has_recent_token_activity_ignores_a_zero_context_pct() {
        use crate::adapter::TokenUsageRecord;
        let now = Utc::now();
        let records = vec![TokenUsageRecord {
            at: now - Duration::minutes(5),
            model: None,
            input_tokens: 0,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 0,
            reasoning_output_tokens: 0,
            total_tokens: 0,
            context_pct: Some(0.0),
        }];
        assert!(!session_has_recent_token_activity(&records, now));
    }

    #[test]
    fn session_has_recent_token_activity_still_respects_the_lookback_window_for_context_pct() {
        use crate::adapter::TokenUsageRecord;
        let now = Utc::now();
        let records = vec![TokenUsageRecord {
            at: now - Duration::minutes(SESSION_ACTIVE_TOKEN_LOOKBACK_MINUTES + 1),
            model: None,
            input_tokens: 0,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 0,
            reasoning_output_tokens: 0,
            total_tokens: 0,
            context_pct: Some(12.0),
        }];
        assert!(!session_has_recent_token_activity(&records, now));
    }

    #[test]
    fn idle_compact_config_percent_threshold_defaults_to_65() {
        assert_eq!(IdleCompactConfig::default().percent_threshold_pct, 65.0);
    }

    #[test]
    fn idle_compact_config_percent_threshold_defaults_when_older_json_omits_it() {
        let config = IdleCompactConfig::default();
        let mut encoded = serde_json::to_value(&config).unwrap();
        encoded
            .as_object_mut()
            .unwrap()
            .remove("percent_threshold_pct");

        let decoded: IdleCompactConfig = serde_json::from_value(encoded).unwrap();

        assert_eq!(decoded.percent_threshold_pct, 65.0);
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
