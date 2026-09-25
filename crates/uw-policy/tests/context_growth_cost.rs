//! Cache-write cost learned from how much context a session grew over its
//! lifetime versus the plan percentage that growth consumed. Lives in `tests/`
//! rather than the crate's `mod tests` only because the in-crate test module
//! does not currently build.

use chrono::{DateTime, TimeZone, Utc};
use uw_core::model::{CacheWritePrice, ThresholdProfile};
use uw_policy::{
    CacheCostObservation, ContextSizeSample, PlanUsageSample, estimate_cache_write_pct,
    estimate_cache_write_pct_from_context_growth, learned_cache_write_pct_per_1k_context,
    positive_context_growth_tokens, positive_plan_pct_growth,
};

fn at(minutes: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + minutes * 60, 0).unwrap()
}

fn context(series: &[(i64, u64)]) -> Vec<ContextSizeSample> {
    series
        .iter()
        .map(|(minutes, context_tokens)| ContextSizeSample {
            at: at(*minutes),
            context_tokens: *context_tokens,
        })
        .collect()
}

fn plan(series: &[(i64, f32)]) -> Vec<PlanUsageSample> {
    series
        .iter()
        .map(|(minutes, pct)| PlanUsageSample {
            at: at(*minutes),
            pct: *pct,
        })
        .collect()
}

#[test]
fn context_growth_counts_only_positive_deltas() {
    // 5k -> 180k -> 9k -> 100k: the drop to 9k is a compaction, not negative
    // work, so the total is (180 - 5) + (100 - 9) = 266k.
    let samples = context(&[(0, 5_000), (10, 180_000), (20, 9_000), (30, 100_000)]);

    assert_eq!(positive_context_growth_tokens(&samples), 266_000);
}

#[test]
fn context_growth_is_zero_for_a_series_that_only_shrinks() {
    let samples = context(&[(0, 180_000), (10, 90_000), (20, 5_000)]);

    assert_eq!(positive_context_growth_tokens(&samples), 0);
}

#[test]
fn context_growth_orders_samples_by_time_before_differencing() {
    let samples = context(&[(30, 100_000), (0, 5_000), (20, 9_000), (10, 180_000)]);

    assert_eq!(positive_context_growth_tokens(&samples), 266_000);
}

#[test]
fn context_growth_of_a_single_sample_is_zero() {
    assert_eq!(positive_context_growth_tokens(&context(&[(0, 50_000)])), 0);
    assert_eq!(positive_context_growth_tokens(&[]), 0);
}

#[test]
fn plan_pct_growth_counts_only_positive_deltas() {
    // A window reset drops the percentage back to 1.0; that is not a refund.
    let samples = plan(&[(0, 4.0), (10, 12.0), (20, 1.0), (30, 6.3)]);

    let growth = positive_plan_pct_growth(&samples);

    assert!((growth - 13.3).abs() < 0.001, "growth={growth}");
}

#[test]
fn cache_write_cost_is_plan_pct_per_1k_of_context_growth() {
    // 266k of context growth cost 13.3 plan percent, so 0.05 percent per 1k.
    let context = context(&[(0, 5_000), (10, 180_000), (20, 9_000), (30, 100_000)]);
    let plan = plan(&[(0, 4.0), (10, 12.0), (20, 1.0), (30, 6.3)]);

    let rate = learned_cache_write_pct_per_1k_context(&context, &plan)
        .expect("both series grew, so a rate is learnable");

    assert!((rate - 0.05).abs() < 0.0005, "rate={rate}");
}

#[test]
fn cache_write_cost_is_unknown_without_positive_context_growth() {
    let shrinking = context(&[(0, 180_000), (30, 9_000)]);
    let plan = plan(&[(0, 4.0), (30, 12.0)]);

    assert_eq!(
        learned_cache_write_pct_per_1k_context(&shrinking, &plan),
        None
    );
}

#[test]
fn cache_write_cost_is_unknown_without_positive_plan_growth() {
    let context = context(&[(0, 5_000), (30, 100_000)]);
    let flat = plan(&[(0, 12.0), (30, 12.0)]);

    assert_eq!(
        learned_cache_write_pct_per_1k_context(&context, &flat),
        None
    );
}

#[test]
fn estimated_cache_write_pct_scales_with_the_context_being_rewritten() {
    // 0.05 percent per 1k, restoring a 100k context, costs 5 percent.
    let estimate =
        estimate_cache_write_pct_from_context_growth(100_000, 0.05, &ThresholdProfile::default());

    assert!((estimate - 5.0).abs() < 0.001, "estimate={estimate}");
}

#[test]
fn estimated_cache_write_pct_from_context_growth_stays_within_the_policy_bounds() {
    let profile = ThresholdProfile::default();

    assert_eq!(
        estimate_cache_write_pct_from_context_growth(10_000_000, 0.05, &profile),
        20.0
    );
    assert_eq!(
        estimate_cache_write_pct_from_context_growth(100, 0.000_01, &profile),
        0.1
    );
}

#[test]
fn existing_priced_cache_write_estimate_is_unchanged() {
    // The context-growth path is a sibling, not a replacement: the priced and
    // fallback behavior of `estimate_cache_write_pct` must still hold.
    let profile = ThresholdProfile {
        cache_write_fallback_pct: 1.25,
        cache_write_price_table: vec![CacheWritePrice {
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

    assert_eq!(
        estimate_cache_write_pct(1_000_000, "claude-sonnet", &profile, &history),
        6.0
    );
    assert_eq!(
        estimate_cache_write_pct(1_000_000, "claude-sonnet", &profile, &[]),
        1.25
    );
}
