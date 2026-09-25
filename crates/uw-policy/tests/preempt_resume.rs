//! Selection math for spending a window's last few minutes of budget on
//! sessions that would otherwise sit waiting for the reset. Lives in `tests/`
//! rather than the crate's `mod tests` only because the in-crate test module
//! does not currently build.

use chrono::{DateTime, Duration, TimeZone, Utc};
use uw_core::model::SessionId;
use uw_policy::{
    PREEMPT_MAX_LEAD_MINUTES, PREEMPT_MAX_SESSIONS, PreemptCandidate, select_preempt_resumes,
};

/// Plan percent charged per 1k context tokens written back on resume.
const PCT_PER_1K: f32 = 0.05;

fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 0).unwrap()
}

fn candidate(id: &str, burn_pct_per_minute: f32, context_tokens: u64) -> PreemptCandidate {
    PreemptCandidate {
        session_id: SessionId(id.into()),
        // Parked well past every reset under test, which is what makes it
        // preemptable at all.
        resume_at: now() + Duration::days(1),
        burn_pct_per_minute,
        context_tokens,
    }
}

fn ids(selections: &[uw_policy::PreemptSelection]) -> Vec<String> {
    selections
        .iter()
        .map(|selection| selection.session_id.0.clone())
        .collect()
}

/// A reset three minutes out, inside the preempt lead window. Costs at that
/// lead: fast 1.0 + 3.0 = 4.0, middle 0.5 + 1.5 = 2.0, slow 0.5 + 0.6 = 1.1.
fn three_minutes_out() -> (DateTime<Utc>, Vec<PreemptCandidate>) {
    (
        now() + Duration::minutes(3),
        vec![
            candidate("fast", 1.0, 20_000),
            candidate("middle", 0.5, 10_000),
            candidate("slow", 0.2, 10_000),
            candidate("crawl", 0.1, 10_000),
        ],
    )
}

#[test]
fn preempt_selects_at_most_two_sessions() {
    let (resets_at, candidates) = three_minutes_out();

    let selected = select_preempt_resumes(&candidates, 50.0, resets_at, now(), PCT_PER_1K);

    assert_eq!(selected.len(), PREEMPT_MAX_SESSIONS);
    assert_eq!(ids(&selected), vec!["fast", "middle"]);
}

#[test]
fn preempt_prefers_the_highest_tempo_session() {
    let (resets_at, candidates) = three_minutes_out();

    // Only 4.5 percent left: the fast session fits, nothing else does.
    let selected = select_preempt_resumes(&candidates, 4.5, resets_at, now(), PCT_PER_1K);

    assert_eq!(ids(&selected), vec!["fast"]);
}

#[test]
fn preempt_skips_a_session_the_remaining_budget_cannot_cover() {
    let (resets_at, candidates) = three_minutes_out();

    // 5.5 percent buys the fast session (4.0) and then the slow one (1.1);
    // the middle session (2.0) no longer fits and is passed over.
    let selected = select_preempt_resumes(&candidates, 5.5, resets_at, now(), PCT_PER_1K);

    assert_eq!(ids(&selected), vec!["fast", "slow"]);
}

#[test]
fn preempt_charges_the_cache_write_cost_of_the_resumed_context() {
    let resets_at = now() + Duration::minutes(3);
    // A 1M-token context costs 50 percent to write back, far past the budget,
    // even though three minutes of its burn would only cost 3 percent.
    let candidates = vec![candidate("huge-context", 1.0, 1_000_000)];

    assert!(select_preempt_resumes(&candidates, 20.0, resets_at, now(), PCT_PER_1K).is_empty());
}

#[test]
fn preempt_reports_the_lead_and_the_cost_it_charged() {
    let resets_at = now() + Duration::minutes(3);
    let candidates = vec![candidate("fast", 1.0, 20_000)];

    let selected = select_preempt_resumes(&candidates, 50.0, resets_at, now(), PCT_PER_1K);

    assert_eq!(selected[0].resume_at, now());
    assert!(
        (selected[0].lead_minutes - 3.0).abs() < 0.001,
        "lead={}",
        selected[0].lead_minutes
    );
    assert!(
        (selected[0].estimated_cost_pct - 4.0).abs() < 0.001,
        "cost={}",
        selected[0].estimated_cost_pct
    );
}

#[test]
fn preempt_lead_time_is_clamped_to_a_few_minutes() {
    let candidates = vec![candidate("fast", 1.0, 20_000)];
    let inside = now() + Duration::seconds((PREEMPT_MAX_LEAD_MINUTES * 60.0) as i64 - 30);
    let outside = now() + Duration::seconds((PREEMPT_MAX_LEAD_MINUTES * 60.0) as i64 + 30);

    let selected = select_preempt_resumes(&candidates, 50.0, inside, now(), PCT_PER_1K);
    assert_eq!(selected.len(), 1);
    assert!(
        selected[0].lead_minutes <= PREEMPT_MAX_LEAD_MINUTES,
        "lead={}",
        selected[0].lead_minutes
    );

    assert!(select_preempt_resumes(&candidates, 50.0, outside, now(), PCT_PER_1K).is_empty());
}

#[test]
fn preempt_returns_nothing_when_the_reset_is_far_away() {
    let candidates = vec![candidate("fast", 1.0, 20_000)];
    let resets_at = now() + Duration::hours(1);

    assert!(select_preempt_resumes(&candidates, 90.0, resets_at, now(), PCT_PER_1K).is_empty());
}

#[test]
fn preempt_returns_nothing_once_the_reset_has_passed() {
    let candidates = vec![candidate("fast", 1.0, 20_000)];
    let resets_at = now() - Duration::minutes(1);

    assert!(select_preempt_resumes(&candidates, 90.0, resets_at, now(), PCT_PER_1K).is_empty());
}

#[test]
fn preempt_ignores_advisory_keepalive_and_plan_pressure_thresholds() {
    // The window sits at 96 percent — past the 85 advisory, the 90 keepalive
    // and the 95 plan-pressure gates. None of them apply to a preempt: the
    // remaining 4 percent expires at the reset regardless.
    let (resets_at, candidates) = three_minutes_out();

    let selected = select_preempt_resumes(&candidates, 4.0, resets_at, now(), PCT_PER_1K);

    assert_eq!(ids(&selected), vec!["fast"]);
}

#[test]
fn preempt_accepts_a_session_scheduled_for_the_reset_itself() {
    let resets_at = now() + Duration::minutes(3);
    let waiting_for_reset = PreemptCandidate {
        resume_at: resets_at,
        ..candidate("fast", 1.0, 20_000)
    };

    let selected = select_preempt_resumes(&[waiting_for_reset], 50.0, resets_at, now(), PCT_PER_1K);

    assert_eq!(ids(&selected), vec!["fast"]);
}

#[test]
fn preempt_skips_sessions_already_scheduled_before_the_reset() {
    let resets_at = now() + Duration::minutes(3);
    let already_earlier = PreemptCandidate {
        resume_at: resets_at - Duration::minutes(1),
        ..candidate("fast", 1.0, 20_000)
    };

    assert!(
        select_preempt_resumes(&[already_earlier], 50.0, resets_at, now(), PCT_PER_1K).is_empty()
    );
}

#[test]
fn preempt_skips_sessions_with_no_measured_tempo() {
    let resets_at = now() + Duration::minutes(3);
    let candidates = vec![candidate("idle", 0.0, 10_000)];

    assert!(select_preempt_resumes(&candidates, 50.0, resets_at, now(), PCT_PER_1K).is_empty());
}
