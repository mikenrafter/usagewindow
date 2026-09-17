//! Pure threshold and trigger logic for usage-window policies.

use chrono::{DateTime, Duration, Utc};
use std::collections::BTreeMap;
use uw_core::adapter::Capabilities;
use uw_core::model::{
    AccountId, IdleCompactConfig, ReseedAutoConfig, Severity, ThresholdProfile, UsageSample,
    WindowKey,
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
    (hours > 0.0).then_some((last_pct - first_pct) / hours)
}

pub fn projected_exhaustion(block: &WindowBlock, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let rate = burn_rate_pct_per_hour(block, Duration::minutes(30))?;
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
    context_window_size: u64,
    config: &IdleCompactConfig,
    caps: &Capabilities,
) -> bool {
    caps.can_trigger_compaction
        && caps.reports_token_counts
        && idle_for >= cache_ttl - config.margin
        && config
            .tiers
            .iter()
            .filter(|tier| tier.window_size_floor <= context_window_size)
            .max_by_key(|tier| tier.window_size_floor)
            .is_some_and(|tier| last_known_token_count >= tier.token_threshold)
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
    caps.can_trigger_compaction
        && caps.reports_token_counts
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
    use uw_core::model::{Provider, TokenTier, UsageSource, UsageWindowState, WindowKind};

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
            windows: HashMap::from([(key(), UsageWindowState::new(pct, false, true, reset, None))]),
            credits: None,
        }
    }
    fn at(minutes: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + minutes * 60, 0).unwrap()
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
        };
        let caps = caps();
        assert!(should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            200_000,
            210_000,
            &c,
            &caps
        ));
        assert!(!should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            150_000,
            210_000,
            &c,
            &caps
        ));
        assert!(!should_idle_compact(
            Duration::minutes(4),
            Duration::minutes(5),
            99_999,
            200_000,
            &c,
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
            210_000,
            &IdleCompactConfig {
                tiers: vec![TokenTier {
                    window_size_floor: 0,
                    token_threshold: 100_000
                }],
                margin: Duration::minutes(1)
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
                    window_size_floor: 201_000,
                    token_threshold: 200_000
                }
            ]
        );
    }

    fn caps() -> Capabilities {
        Capabilities {
            can_trigger_compaction: true,
            can_advise_mid_turn: true,
            can_inject_at_session_start: true,
            can_observe_compaction: true,
            reports_token_counts: true,
            headless_resume: true,
            seed_modes: vec![],
        }
    }
}
