//! Pure threshold and trigger logic for usage-window policies.

use chrono::{DateTime, Duration, Utc};
use std::collections::BTreeMap;
use uw_core::adapter::Capabilities;
use uw_core::adapter::TokenUsageRecord;
use uw_core::model::{
    AccountId, IdleCompactConfig, Provider, ReseedAutoConfig, Severity, ThresholdProfile,
    UsageSample, WindowKind, WindowKey,
};

const ROLLOVER_TOLERANCE: Duration = Duration::minutes(2);
const CACHE_WRITE_MIN_PCT: f32 = 0.1;
const CACHE_WRITE_MAX_PCT: f32 = 20.0;

#[derive(Clone, Debug, PartialEq)]
pub struct CacheCostObservation {
    pub at: DateTime<Utc>,
    pub plan_pct_delta: f32,
    pub usd_delta: f64,
}

/// Returns the price for the longest model prefix, if the table has a match.
pub fn cache_write_price_for_model(model: &str, profile: &ThresholdProfile) -> Option<f64> {
    profile
        .cache_write_price_table
        .iter()
        .filter(|price| model.starts_with(&price.model_prefix))
        .max_by_key(|price| price.model_prefix.len())
        .map(|price| price.usd_per_mtok)
}

/// Learns dollars per plan-percent from usable hourly buckets. A bucket is usable
/// only when its aggregate deltas are positive; three buckets are required to avoid
/// making a pricing decision from a single noisy observation.
pub fn learned_usd_per_plan_percent(history: &[CacheCostObservation]) -> Option<f64> {
    let mut buckets: BTreeMap<i64, (f32, f64)> = BTreeMap::new();
    for observation in history {
        if observation.plan_pct_delta > 0.0 && observation.usd_delta > 0.0 {
            let hour = observation.at.timestamp().div_euclid(3600);
            let entry = buckets.entry(hour).or_default();
            entry.0 += observation.plan_pct_delta;
            entry.1 += observation.usd_delta;
        }
    }
    let usable: Vec<(f32, f64)> = buckets
        .into_values()
        .filter(|(pct, usd)| *pct > 0.0 && *usd > 0.0)
        .collect();
    if usable.len() < 3 {
        return None;
    }
    let pct: f32 = usable.iter().map(|(pct, _)| *pct).sum();
    let usd: f64 = usable.iter().map(|(_, usd)| *usd).sum();
    (pct > 0.0).then_some(usd / f64::from(pct))
}

/// Estimates the quota percentage consumed by writing `tokens` into the cache.
/// The learned historical ratio is preferred; the configured flat percentage is
/// the fallback when history is insufficient or pricing is unavailable.
pub fn estimate_cache_write_pct(
    tokens: u64,
    model: &str,
    profile: &ThresholdProfile,
    history: &[CacheCostObservation],
) -> f32 {
    let fallback = profile.cache_write_fallback_pct;
    let Some(price) = cache_write_price_for_model(model, profile) else {
        return fallback.clamp(CACHE_WRITE_MIN_PCT, CACHE_WRITE_MAX_PCT);
    };
    let Some(usd_per_pct) = learned_usd_per_plan_percent(history) else {
        return fallback.clamp(CACHE_WRITE_MIN_PCT, CACHE_WRITE_MAX_PCT);
    };
    let estimate = (tokens as f64 / 1_000_000.0) * price / usd_per_pct;
    (estimate as f32).clamp(CACHE_WRITE_MIN_PCT, CACHE_WRITE_MAX_PCT)
}

#[derive(Clone, Debug, PartialEq)]
pub struct WindowBlock {
    pub key: WindowKey,
    pub account: Option<AccountId>,
    pub started_at: DateTime<Utc>,
    pub resets_at: Option<DateTime<Utc>>,
    pub points: Vec<(DateTime<Utc>, f32)>,
    /// The reset value observed with each point. Kept separately so callers can
    /// construct derived blocks while burn-rate math can still reject a reset
    /// spanning pair.
    pub point_resets_at: Vec<Option<DateTime<Utc>>>,
}

/// History needed for a useful burn-rate estimate for each provider.
pub fn burn_rate_lookback(provider: &Provider) -> Duration {
    match provider {
        Provider::Cursor => Duration::hours(24),
        _ => Duration::minutes(30),
    }
}

/// Lookback for status/UI burn-rate display for a provider and window.
/// Cursor uses 24h for every window; weekly and other long windows use 24h for
/// other providers, while short session windows use 30m.
pub fn burn_rate_display_lookback_for_provider(
    provider: &Provider,
    kind: &WindowKind,
) -> Duration {
    if matches!(provider, Provider::Cursor) {
        return Duration::hours(24);
    }
    match kind {
        WindowKind::WeeklyModel(_) | WindowKind::WeeklySurface(_) => Duration::hours(24),
        WindowKind::Rolling { minutes } if *minutes >= 24 * 60 => Duration::hours(24),
        WindowKind::Rolling { .. } | WindowKind::Custom(_) => Duration::minutes(30),
    }
}

/// Lookback for non-provider-specific callers using the default window policy.
pub fn burn_rate_display_lookback(kind: &WindowKind) -> Duration {
    burn_rate_display_lookback_for_provider(&Provider::Codex, kind)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdviseChannel {
    MidTurn,
    QueuedAtSessionStart,
    None,
}

/// Segment one account's samples. `None` selects samples with no account.
pub fn segment_blocks(
    samples: &[UsageSample],
    key: &WindowKey,
    account: Option<&AccountId>,
) -> Vec<WindowBlock> {
    let mut matching: Vec<&UsageSample> = samples
        .iter()
        .filter(|sample| sample.account.as_ref() == account)
        .collect();
    matching.sort_by_key(|sample| sample.at);
    let mut blocks = Vec::new();
    for sample in matching {
        let Some(state) = sample.windows.get(key) else {
            continue;
        };
        let reset = state.resets_at;
        let new_block = blocks
            .last()
            .is_none_or(|block: &WindowBlock| !same_window_instance(block.resets_at, reset));
        if new_block {
            blocks.push(WindowBlock {
                key: key.clone(),
                account: sample.account.clone(),
                started_at: sample.at,
                resets_at: reset,
                points: Vec::new(),
                point_resets_at: Vec::new(),
            });
        }
        let block = blocks
            .last_mut()
            .expect("block was just created or already exists");
        block.points.push((sample.at, state.pct));
        block.point_resets_at.push(reset);
    }
    blocks
}

fn same_window_instance(a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => (a - b).abs() <= ROLLOVER_TOLERANCE,
        _ => false,
    }
}

pub fn burn_rate_pct_per_hour(block: &WindowBlock, lookback: Duration) -> Option<f32> {
    let &(last_at, last_pct) = block.points.last()?;
    let cutoff = last_at - lookback;
    let (first_index, &(first_at, first_pct)) = block
        .points
        .iter()
        .enumerate()
        .rev()
        .find(|(_, (at, _))| *at <= cutoff)?;
    burn_rate_between(block, first_index, first_at, first_pct, last_at, last_pct)
}

/// Computes burn from the available portion of a window when a full lookback
/// anchor does not exist yet. It still refuses reset-spanning pairs and
/// zero-length intervals; callers can use this during daemon startup when a
/// fresh window has only a few minutes of history.
pub fn burn_rate_pct_per_hour_available(block: &WindowBlock, lookback: Duration) -> Option<f32> {
    let anchor = available_anchor(block, lookback)?;
    pct_delta_between(block, anchor).map(|(delta, hours)| delta / hours)
}

/// Percent of quota actually consumed within the available lookback history —
/// the raw delta between the oldest and newest sample in range, with no
/// extrapolation to the nominal lookback length. A block that is younger than
/// `lookback` reports only the consumption it has actually observed, instead
/// of projecting a short, possibly bursty interval up to a full-window
/// figure (which is how a few minutes of Cursor usage right after a reset
/// could previously get reported as hundreds of percent "per 24h").
pub fn burn_pct_available(block: &WindowBlock, lookback: Duration) -> Option<f32> {
    let anchor = available_anchor(block, lookback)?;
    pct_delta_between(block, anchor).map(|(delta, _)| delta)
}

struct Anchor {
    first_index: usize,
    first_at: DateTime<Utc>,
    first_pct: f32,
    last_at: DateTime<Utc>,
    last_pct: f32,
}

fn available_anchor(block: &WindowBlock, lookback: Duration) -> Option<Anchor> {
    let &(last_at, last_pct) = block.points.last()?;
    let cutoff = last_at - lookback;
    let (first_index, &(first_at, first_pct)) = block
        .points
        .iter()
        .enumerate()
        .find(|(_, (at, _))| *at >= cutoff && *at < last_at)?;
    Some(Anchor {
        first_index,
        first_at,
        first_pct,
        last_at,
        last_pct,
    })
}

fn burn_rate_between(
    block: &WindowBlock,
    first_index: usize,
    first_at: DateTime<Utc>,
    first_pct: f32,
    last_at: DateTime<Utc>,
    last_pct: f32,
) -> Option<f32> {
    pct_delta_between(
        block,
        Anchor {
            first_index,
            first_at,
            first_pct,
            last_at,
            last_pct,
        },
    )
    .map(|(delta, hours)| delta / hours)
}

fn pct_delta_between(block: &WindowBlock, anchor: Anchor) -> Option<(f32, f32)> {
    let Anchor { first_index, first_at, first_pct, last_at, last_pct } = anchor;
    if first_at >= last_at
        || !same_window_instance(
            block
                .point_resets_at
                .get(first_index)
                .copied()
                .flatten()
                .or(block.resets_at),
            block
                .point_resets_at
                .last()
                .copied()
                .flatten()
                .or(block.resets_at),
        )
    {
        return None;
    }
    let hours = (last_at - first_at).num_seconds() as f32 / 3600.0;
    (hours > 0.0).then_some((last_pct - first_pct, hours))
}

/// Estimates per-session token work from rollout records. Each record describes
/// one response, so its work is assigned to the interval since the previous
/// response. The newest five minutes receive a configurable recency coefficient;
/// `activity_weight` lets callers reduce the contribution of inactive chats when
/// combining sessions.
pub fn weighted_token_rate_per_minute(
    records: &[TokenUsageRecord],
    now: DateTime<Utc>,
    lookback: Duration,
    recent_window: Duration,
    recent_coefficient: f64,
    activity_weight: f64,
) -> Option<f64> {
    if records.len() < 2
        || recent_coefficient <= 0.0
        || activity_weight <= 0.0
        || lookback <= Duration::zero()
    {
        return None;
    }
    let cutoff = now - lookback;
    let recent_cutoff = now - recent_window;
    let mut ordered = records.to_vec();
    ordered.sort_by_key(|record| record.at);
    let mut weighted_work = 0.0;
    let mut weighted_minutes = 0.0;
    for pair in ordered.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if current.at <= cutoff || previous.at >= now {
            continue;
        }
        let start = previous.at.max(cutoff);
        let end = current.at.min(now);
        if start >= end {
            continue;
        }
        let minutes = (end - start).num_milliseconds() as f64 / 60_000.0;
        let uncached_input = current
            .input_tokens
            .saturating_sub(current.cached_input_tokens);
        // Cache writes are part of input_tokens, so they remain in uncached_input.
        // reasoning_output_tokens is a subtype of output_tokens and is not added
        // separately.
        let work = uncached_input.saturating_add(current.output_tokens) as f64;
        let work_per_minute = work / minutes;
        let recent_minutes = if end > recent_cutoff {
            (end - start.max(recent_cutoff)).num_milliseconds() as f64 / 60_000.0
        } else {
            0.0
        };
        let old_minutes = minutes - recent_minutes;
        let weighted_interval_minutes = old_minutes + recent_minutes * recent_coefficient;
        weighted_work += work_per_minute * weighted_interval_minutes * activity_weight;
        weighted_minutes += weighted_interval_minutes;
    }
    (weighted_minutes > 0.0).then_some(weighted_work / weighted_minutes)
}

pub fn projected_exhaustion(block: &WindowBlock, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    projected_exhaustion_with_lookback(block, now, Duration::minutes(30))
}

pub fn projected_exhaustion_with_lookback(
    block: &WindowBlock,
    now: DateTime<Utc>,
    lookback: Duration,
) -> Option<DateTime<Utc>> {
    let rate = burn_rate_pct_per_hour(block, lookback)?;
    if rate <= 0.0 {
        return None;
    }
    let (_, current_pct) = *block.points.last()?;
    let seconds = ((100.0 - current_pct) / rate * 3600.0).ceil() as i64;
    let _ = now;
    let projected = block.points.last().map(|(last_at, _)| *last_at)? + Duration::seconds(seconds);
    block
        .resets_at
        .filter(|reset| projected < *reset)
        .map_or_else(
            || {
                if block.resets_at.is_none() {
                    Some(projected)
                } else {
                    None
                }
            },
            |_| Some(projected),
        )
}

pub fn should_trigger_near_limit(
    current_pct: f32,
    burn_pct_per_hour: f32,
    profile: &ThresholdProfile,
) -> bool {
    let effective = 100.0 - burn_pct_per_hour * profile.burn_multiplier;
    current_pct >= effective.max(profile.closing_pct)
}

pub fn resume_lead_minutes(
    remaining_pct: f32,
    cache_write_pct: f32,
    burn_pct_per_minute: f32,
    profile: &ThresholdProfile,
) -> Option<f32> {
    let usable = remaining_pct - cache_write_pct;
    if burn_pct_per_minute <= 0.0
        || usable <= 0.0
        || profile.min_lead_minutes > profile.max_lead_minutes
    {
        return None;
    }
    let raw = usable / burn_pct_per_minute;
    Some(
        (raw * (1.0 - profile.overhead_pct / 100.0))
            .clamp(profile.min_lead_minutes, profile.max_lead_minutes),
    )
}

pub fn should_idle_compact(
    idle_for: Duration,
    cache_ttl: Duration,
    last_known_token_count: u64,
    context_window_size: Option<u64>,
    config: &IdleCompactConfig,
    caps: &Capabilities,
) -> bool {
    caps.can_trigger_compaction
        && caps.reports_token_counts
        && idle_for >= cache_ttl - config.margin
        && match context_window_size {
            Some(size) => config
                .tiers
                .iter()
                .filter(|tier| tier.window_size_floor <= size)
                .max_by_key(|tier| tier.window_size_floor)
                .is_some_and(|tier| last_known_token_count >= tier.token_threshold),
            None => last_known_token_count >= config.unknown_context_token_threshold,
        }
}

#[allow(clippy::too_many_arguments)]
pub fn should_auto_reseed(
    idle_for: Duration,
    cache_ttl: Duration,
    last_known_token_count: u64,
    estimated_reseed_cost_usd: f64,
    estimated_wait_for_reset_cost_usd: f64,
    time_since_last_reseed: Duration,
    config: &ReseedAutoConfig,
    caps: &Capabilities,
) -> bool {
    caps.reports_token_counts
        && caps
            .seed_modes
            .contains(&uw_core::adapter::SeedMode::InitialPrompt)
        && config.enabled
        && idle_for >= cache_ttl - config.margin
        && last_known_token_count >= config.min_tokens
        && estimated_reseed_cost_usd < estimated_wait_for_reset_cost_usd
        && time_since_last_reseed >= config.cooldown
}

pub fn advise_channel_for(caps: &Capabilities) -> AdviseChannel {
    if caps.can_advise_mid_turn {
        AdviseChannel::MidTurn
    } else if caps.can_inject_at_session_start {
        AdviseChannel::QueuedAtSessionStart
    } else {
        AdviseChannel::None
    }
}

pub fn severity_for(pct: f32, exceeded: bool, profile: &ThresholdProfile) -> Option<Severity> {
    if exceeded || pct >= profile.plan_pressure_pct {
        Some(Severity::Exceeded)
    } else if pct >= profile.compact_pct {
        Some(Severity::Compact)
    } else if pct >= profile.closing_pct {
        Some(Severity::Closing)
    } else if pct >= profile.notice_pct {
        Some(Severity::Notice)
    } else {
        None
    }
}

pub fn should_reask(
    last_asked_pct: Option<f32>,
    asks_this_epoch: u32,
    current_pct: f32,
    profile: &ThresholdProfile,
) -> bool {
    asks_this_epoch < profile.reask_max_per_epoch
        && last_asked_pct.is_none_or(|last| current_pct - last >= profile.reask_delta_pct)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::collections::HashMap;
    use uw_core::model::{
        ModelId, Provider, TokenTier, UsageSource, UsageWindowState, WindowKind,
    };

    #[test]
    fn cursor_burn_rate_uses_the_last_24_hours() {
        assert_eq!(burn_rate_lookback(&Provider::Cursor), Duration::hours(24));
        assert_eq!(burn_rate_lookback(&Provider::Codex), Duration::minutes(30));
    }

    #[test]
    fn display_lookback_is_24h_for_weekly_and_long_rolling_windows() {
        assert_eq!(
            burn_rate_display_lookback_for_provider(&Provider::Codex, &WindowKind::WeeklyModel(ModelId("claude".into()))),
            Duration::hours(24)
        );
        assert_eq!(
            burn_rate_display_lookback_for_provider(&Provider::Codex, &WindowKind::Rolling { minutes: 10_080 }),
            Duration::hours(24)
        );
        assert_eq!(
            burn_rate_display_lookback_for_provider(&Provider::Codex, &WindowKind::Rolling { minutes: 300 }),
            Duration::minutes(30)
        );
        assert_eq!(
            burn_rate_display_lookback_for_provider(&Provider::Cursor, &WindowKind::Rolling { minutes: 300 }),
            Duration::hours(24)
        );
    }

    #[test]
    fn cache_cost_falls_back_with_fewer_than_three_buckets() {
        let profile = ThresholdProfile {
            cache_write_fallback_pct: 1.25,
            cache_write_price_table: vec![uw_core::model::CacheWritePrice {
                model_prefix: "claude".into(),
                usd_per_mtok: 3.0,
            }],
            ..Default::default()
        };
        let history = vec![CacheCostObservation {
            at: at(0),
            plan_pct_delta: 1.0,
            usd_delta: 0.5,
        }];
        assert_eq!(
            estimate_cache_write_pct(1_000_000, "claude-sonnet", &profile, &history),
            1.25
        );
    }

    #[test]
    fn cache_cost_learns_ratio_across_three_hour_buckets() {
        let profile = ThresholdProfile {
            cache_write_price_table: vec![uw_core::model::CacheWritePrice {
                model_prefix: "claude".into(),
                usd_per_mtok: 3.0,
            }],
            ..Default::default()
        };
        let history = (0..3)
            .map(|hour| CacheCostObservation {
                at: at(hour * 60),
                plan_pct_delta: 2.0,
                usd_delta: 1.0,
            })
            .collect::<Vec<_>>();
        // $3 / ($1 / 2 plan-percent) = 6 plan-percent.
        assert_eq!(
            estimate_cache_write_pct(1_000_000, "claude-sonnet", &profile, &history),
            6.0
        );
    }

    #[test]
    fn cache_cost_uses_the_longest_matching_model_prefix() {
        let profile = ThresholdProfile {
            cache_write_price_table: vec![
                uw_core::model::CacheWritePrice {
                    model_prefix: "claude".into(),
                    usd_per_mtok: 1.0,
                },
                uw_core::model::CacheWritePrice {
                    model_prefix: "claude-sonnet-4-6".into(),
                    usd_per_mtok: 4.0,
                },
            ],
            ..Default::default()
        };
        assert_eq!(
            cache_write_price_for_model("claude-sonnet-4-6", &profile),
            Some(4.0)
        );
    }

    fn key() -> WindowKey {
        WindowKey {
            provider: Provider::ClaudeCode,
            kind: WindowKind::Rolling { minutes: 300 },
        }
    }
    fn sample(at: DateTime<Utc>, pct: f32, reset: Option<DateTime<Utc>>) -> UsageSample {
        UsageSample {
            at,
            fetched_at: None,
            source: UsageSource::ProviderReported,
            provider: Provider::ClaudeCode,
            account: Some(AccountId("a".into())),
            plan: None,
            windows: HashMap::from([(key(), UsageWindowState::new(pct, false, true, reset, None))]),
            credits: None,
        }
    }
    fn at(minutes: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + minutes * 60, 0).unwrap()
    }
    fn token_record(
        minutes: i64,
        input: u64,
        cached: u64,
        cache_write: u64,
        output: u64,
    ) -> TokenUsageRecord {
        TokenUsageRecord {
            at: at(minutes),
            model: Some(uw_core::model::ModelId("gpt-5.6-luna".into())),
            input_tokens: input,
            cached_input_tokens: cached,
            cache_write_input_tokens: cache_write,
            output_tokens: output,
            reasoning_output_tokens: 0,
            total_tokens: input.saturating_add(output),
        }
    }

    #[test]
    fn weighted_token_rate_uses_uncached_input_and_output() {
        let records = vec![
            token_record(0, 0, 0, 0, 0),
            token_record(10, 100, 50, 20, 0),
            token_record(20, 200, 100, 40, 0),
        ];
        // Each interval contributes 50 and 100 uncached tokens over ten minutes.
        assert_eq!(
            weighted_token_rate_per_minute(
                &records,
                at(20),
                Duration::minutes(30),
                Duration::minutes(5),
                1.2,
                1.0,
            ),
            Some(160.0 / 21.0)
        );
    }

    #[test]
    fn weighted_token_rate_emphasizes_recent_work_and_inactive_weight() {
        let records = vec![
            token_record(0, 0, 0, 0, 0),
            token_record(10, 100, 0, 0, 0),
            token_record(20, 100, 0, 0, 0),
            token_record(30, 300, 0, 0, 100),
        ];
        // Old work is 10 tokens/min. Recent work is 40 tokens/min, weighted 1.2.
        // The inactive chat contributes at 0.3 of that weighted average.
        let rate = weighted_token_rate_per_minute(
            &records,
            at(30),
            Duration::minutes(30),
            Duration::minutes(5),
            1.2,
            0.3,
        )
        .unwrap();
        assert!((rate - (640.0 / 31.0 * 0.3)).abs() < 0.0001, "rate={rate}");
    }

    #[test]
    fn weighted_token_rate_uses_available_history_without_thirty_minute_anchor() {
        let records = vec![
            token_record(20, 100, 0, 0, 0),
            token_record(25, 200, 0, 0, 0),
            token_record(30, 300, 0, 0, 0),
        ];
        assert!(
            weighted_token_rate_per_minute(
                &records,
                at(30),
                Duration::minutes(30),
                Duration::minutes(5),
                1.2,
                1.0,
            )
            .is_some()
        );
    }
    fn block(points: Vec<(DateTime<Utc>, f32)>, reset: Option<DateTime<Utc>>) -> WindowBlock {
        WindowBlock {
            key: key(),
            account: Some(AccountId("a".into())),
            started_at: points[0].0,
            resets_at: reset,
            point_resets_at: vec![reset; points.len()],
            points,
        }
    }
    fn profile() -> ThresholdProfile {
        ThresholdProfile::default()
    }

    #[test]
    fn segments_rollovers_but_absorbs_earlier_jitter() {
        let reset = Some(at(60));
        let samples = vec![
            sample(at(0), 10., reset),
            sample(at(10), 20., Some(at(59))),
            sample(at(20), 30., Some(at(61))),
            sample(at(30), 5., Some(at(200))),
        ];
        let blocks = segment_blocks(&samples, &key(), Some(&AccountId("a".into())));
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].points.len(), 3);
    }

    #[test]
    fn burn_rate_needs_an_anchor_and_same_window_instance() {
        let b = block(vec![(at(20), 10.), (at(40), 20.)], Some(at(100)));
        assert_eq!(burn_rate_pct_per_hour(&b, Duration::minutes(30)), None);
        let reset = WindowBlock {
            key: key(),
            account: None,
            started_at: at(0),
            resets_at: Some(at(100)),
            points: vec![(at(0), 90.), (at(20), 10.)],
            point_resets_at: vec![Some(at(100)), Some(at(200))],
        };
        assert_eq!(burn_rate_pct_per_hour(&reset, Duration::minutes(15)), None);
    }

    #[test]
    fn available_burn_rate_uses_fresh_window_history_before_thirty_minutes() {
        let b = block(
            vec![(at(20), 0.), (at(25), 1.), (at(30), 3.)],
            Some(at(100)),
        );
        assert_eq!(
            burn_rate_pct_per_hour_available(&b, Duration::minutes(30)),
            Some(18.0)
        );
    }

    #[test]
    fn available_burn_pct_reports_observed_delta_not_a_window_projection() {
        // Only 10 minutes of history exist, well inside a 24h lookback. The
        // rate-based figure would extrapolate this burst across the full 24h;
        // burn_pct_available must report just the 3 points actually consumed.
        let b = block(
            vec![(at(20), 0.), (at(25), 1.), (at(30), 3.)],
            Some(at(100)),
        );
        assert_eq!(burn_pct_available(&b, Duration::hours(24)), Some(3.0));
    }

    #[test]
    fn available_burn_pct_needs_an_anchor_and_same_window_instance() {
        let b = block(vec![(at(20), 10.), (at(40), 20.)], Some(at(100)));
        assert_eq!(burn_pct_available(&b, Duration::minutes(10)), None);
        let reset = WindowBlock {
            key: key(),
            account: None,
            started_at: at(0),
            resets_at: Some(at(100)),
            points: vec![(at(0), 90.), (at(20), 10.)],
            point_resets_at: vec![Some(at(100)), Some(at(200))],
        };
        assert_eq!(burn_pct_available(&reset, Duration::minutes(30)), None);
    }

    #[test]
    fn projects_only_positive_exhaustion_before_reset() {
        let b = block(vec![(at(0), 50.), (at(30), 75.)], Some(at(120)));
        assert_eq!(projected_exhaustion(&b, at(30)), Some(at(60)));
        assert_eq!(projected_exhaustion(&b, at(90)), Some(at(60)));
        assert_eq!(
            projected_exhaustion(
                &block(vec![(at(0), 50.), (at(30), 50.)], Some(at(120))),
                at(30)
            ),
            None
        );
        assert_eq!(
            projected_exhaustion(
                &block(vec![(at(0), 75.), (at(30), 50.)], Some(at(120))),
                at(30)
            ),
            None
        );
        assert_eq!(
            projected_exhaustion(
                &block(vec![(at(0), 50.), (at(30), 75.)], Some(at(40))),
                at(30)
            ),
            None
        );
    }

    #[test]
    fn fast_burn_triggers_earlier_and_closing_is_a_floor() {
        let p = profile();
        assert!(should_trigger_near_limit(90., 20., &p));
        assert!(!should_trigger_near_limit(90., 1., &p));
        assert!(should_trigger_near_limit(85., 15., &p));
    }

    #[test]
    fn lead_time_clamps_and_rejects_unusable_inputs() {
        let mut p = profile();
        p.min_lead_minutes = 10.;
        p.max_lead_minutes = 30.;
        assert_eq!(resume_lead_minutes(90., 10., 1., &p), Some(30.));
        assert_eq!(resume_lead_minutes(20., 0., 1., &p), Some(14.));
        assert_eq!(resume_lead_minutes(0., 0., 1., &p), None);
        assert_eq!(resume_lead_minutes(20., 21., 1., &p), None);
        assert_eq!(resume_lead_minutes(20., 0., 0., &p), None);
        p.min_lead_minutes = 31.;
        assert_eq!(resume_lead_minutes(90., 0., 1., &p), None);
    }

    #[test]
    fn idle_compact_uses_highest_fitting_tier() {
        let c = IdleCompactConfig {
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
            unknown_context_token_threshold: 150_000,
        };
        let caps = caps();
        assert!(should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            200_000,
            Some(210_000),
            &c,
            &caps
        ));
        assert!(!should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            150_000,
            Some(210_000),
            &c,
            &caps
        ));
        assert!(!should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            99_999,
            Some(200_000),
            &c,
            &caps
        ));
    }

    #[test]
    fn idle_compact_uses_interpolated_threshold_when_context_size_is_unknown() {
        let config = IdleCompactConfig {
            unknown_context_token_threshold: 150_000,
            ..profile().idle_compact
        };
        let caps = caps();
        assert!(!should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            149_999,
            None,
            &config,
            &caps
        ));
        assert!(should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            150_000,
            None,
            &config,
            &caps
        ));
    }

    #[test]
    fn idle_compact_percent_path_triggers_without_token_reporting() {
        let mut config = profile().idle_compact;
        config.percent_threshold_pct = 65.0;
        let mut caps = caps();
        caps.reports_token_counts = false;
        assert!(should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            0,
            None,
            Some(70.0),
            &config,
            &caps
        ));
    }

    #[test]
    fn idle_compact_percent_path_does_not_trigger_below_threshold() {
        let mut config = profile().idle_compact;
        config.percent_threshold_pct = 65.0;
        let mut caps = caps();
        caps.reports_token_counts = false;
        assert!(!should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            0,
            None,
            Some(50.0),
            &config,
            &caps
        ));
    }

    #[test]
    fn idle_compact_percent_path_still_requires_compaction_capability() {
        let mut config = profile().idle_compact;
        config.percent_threshold_pct = 65.0;
        let mut caps = caps();
        caps.reports_token_counts = false;
        caps.can_trigger_compaction = false;
        assert!(!should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            0,
            None,
            Some(90.0),
            &config,
            &caps
        ));
    }

    #[test]
    fn idle_compact_percent_path_still_requires_idle_duration_gate() {
        let mut config = profile().idle_compact;
        config.percent_threshold_pct = 65.0;
        let mut caps = caps();
        caps.reports_token_counts = false;
        caps.can_trigger_compaction = true;
        assert!(!should_idle_compact(
            Duration::minutes(1),
            Duration::minutes(5),
            0,
            None,
            Some(90.0),
            &config,
            &caps
        ));
    }

    #[test]
    fn idle_compact_percent_path_uses_the_default_threshold() {
        // `IdleCompactConfig::default()`'s margin is zero, so idle_for must
        // fully cover cache_ttl to clear the shared idle-duration gate.
        let config = IdleCompactConfig::default();
        let mut caps = caps();
        caps.reports_token_counts = false;
        assert!(should_idle_compact(
            Duration::minutes(5),
            Duration::minutes(5),
            0,
            None,
            Some(70.0),
            &config,
            &caps
        ));
        assert!(!should_idle_compact(
            Duration::minutes(5),
            Duration::minutes(5),
            0,
            None,
            Some(60.0),
            &config,
            &caps
        ));
    }

    #[test]
    fn reseed_requires_every_gate() {
        let c = ReseedAutoConfig {
            enabled: true,
            min_tokens: 100,
            cooldown: Duration::hours(1),
            margin: Duration::minutes(1),
        };
        let caps = caps();
        assert!(should_auto_reseed(
            Duration::minutes(5),
            Duration::minutes(5),
            100,
            1.,
            2.,
            Duration::hours(1),
            &c,
            &caps
        ));
        for args in [
            (Duration::minutes(1), 100, 1., 2., Duration::hours(1)),
            (Duration::minutes(5), 99, 1., 2., Duration::hours(1)),
            (Duration::minutes(5), 100, 2., 1., Duration::hours(1)),
            (Duration::minutes(5), 100, 1., 2., Duration::minutes(30)),
        ] {
            assert!(!should_auto_reseed(
                args.0,
                Duration::minutes(5),
                args.1,
                args.2,
                args.3,
                args.4,
                &c,
                &caps
            ));
        }
        let disabled = ReseedAutoConfig {
            enabled: false,
            ..c.clone()
        };
        assert!(!should_auto_reseed(
            Duration::minutes(5),
            Duration::minutes(5),
            100,
            1.,
            2.,
            Duration::hours(1),
            &disabled,
            &caps
        ));
        let mut codex = caps;
        codex.can_trigger_compaction = false;
        codex.reports_token_counts = false;
        assert!(!should_idle_compact(
            Duration::minutes(5),
            Duration::minutes(5),
            200_000,
            Some(210_000),
            &IdleCompactConfig {
                tiers: vec![TokenTier {
                    window_size_floor: 0,
                    token_threshold: 100_000
                }],
                margin: Duration::minutes(1),
                unknown_context_token_threshold: 150_000,
            },
            &codex
        ));
        assert!(!should_auto_reseed(
            Duration::minutes(5),
            Duration::minutes(5),
            100,
            1.,
            2.,
            Duration::hours(1),
            &c,
            &codex
        ));
    }

    #[test]
    fn reseed_uses_seed_capability_not_compaction_capability() {
        let mut capabilities = caps();
        capabilities.can_trigger_compaction = false;
        capabilities.seed_modes = vec![uw_core::adapter::SeedMode::InitialPrompt];
        let config = ReseedAutoConfig {
            enabled: true,
            min_tokens: 100,
            cooldown: Duration::minutes(30),
            margin: Duration::minutes(1),
        };
        assert!(should_auto_reseed(
            Duration::minutes(5),
            Duration::minutes(5),
            100,
            1.0,
            2.0,
            Duration::minutes(30),
            &config,
            &capabilities,
        ));
    }

    #[test]
    fn advise_channel_follows_capabilities() {
        let mut c = caps();
        assert_eq!(advise_channel_for(&c), AdviseChannel::MidTurn);
        c.can_advise_mid_turn = false;
        assert_eq!(advise_channel_for(&c), AdviseChannel::QueuedAtSessionStart);
        c.can_inject_at_session_start = false;
        assert_eq!(advise_channel_for(&c), AdviseChannel::None);
    }

    #[test]
    fn severity_thresholds_are_inclusive() {
        let p = profile();
        assert_eq!(severity_for(69., false, &p), None);
        assert_eq!(severity_for(70., false, &p), Some(Severity::Notice));
        assert_eq!(severity_for(85., false, &p), Some(Severity::Closing));
        assert_eq!(severity_for(90., false, &p), Some(Severity::Compact));
        assert_eq!(severity_for(1., true, &p), Some(Severity::Exceeded));
    }

    #[test]
    fn segment_blocks_filter_accounts_and_sort_points() {
        let a = AccountId("a".into());
        let b = AccountId("b".into());
        let mut samples = vec![sample(at(20), 20., None), sample(at(0), 0., None)];
        samples[0].account = Some(b.clone());
        samples[1].account = Some(a.clone());
        samples.push({
            let mut s = sample(at(10), 10., None);
            s.account = Some(a.clone());
            s
        });
        assert_eq!(
            segment_blocks(&samples, &key(), Some(&a))[0].points,
            vec![(at(0), 0.), (at(10), 10.)]
        );
        assert_eq!(
            segment_blocks(&samples, &key(), Some(&b))[0].points,
            vec![(at(20), 20.)]
        );
    }

    #[test]
    fn reask_is_damped_and_capped() {
        let p = profile();
        assert!(should_reask(None, 0, 50., &p));
        assert!(!should_reask(Some(50.), 0, 54., &p));
        assert!(should_reask(Some(50.), 0, 55., &p));
        assert!(!should_reask(Some(50.), p.reask_max_per_epoch, 100., &p));
    }

    #[test]
    fn default_idle_tiers_match_architecture() {
        assert_eq!(
            profile().idle_compact.tiers,
            vec![
                TokenTier {
                    window_size_floor: 0,
                    token_threshold: 100_000
                },
                TokenTier {
                    window_size_floor: 200_000,
                    token_threshold: 150_000
                },
                TokenTier {
                    window_size_floor: 300_000,
                    token_threshold: 200_000
                }
            ]
        );
    }

    #[test]
    fn default_idle_compact_margin_leaves_warm_window_headroom() {
        // Policy (~30s) + compaction (~60s) cadence can add ~90s after the
        // threshold. Margin of 2m on a 5m TTL fires at ~3m so delivery lands
        // around 4–4.5m — still inside the warm cache, not after it expires.
        let c = &profile().idle_compact;
        assert_eq!(c.margin, Duration::minutes(2));
        let caps = caps();
        assert!(should_idle_compact(
            Duration::minutes(3),
            Duration::minutes(5),
            150_000,
            Some(200_000),
            c,
            &caps
        ));
        assert!(!should_idle_compact(
            Duration::minutes(2) + Duration::seconds(59),
            Duration::minutes(5),
            150_000,
            Some(200_000),
            c,
            &caps
        ));
    }

    fn caps() -> Capabilities {
        Capabilities {
            can_trigger_compaction: true,
            can_advise_mid_turn: true,
            can_inject_at_session_start: true,
            can_observe_compaction: true,
            reports_token_counts: true,
            headless_resume: true,
            seed_modes: vec![uw_core::adapter::SeedMode::InitialPrompt],
        }
    }
}
