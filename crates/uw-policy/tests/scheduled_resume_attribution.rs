//! Attribution of scheduled sessions to the one window bar whose reset wakes
//! them. Lives in `tests/` rather than the crate's `mod tests` only because the
//! in-crate test module does not currently build.

use chrono::{DateTime, TimeZone, Utc};
use uw_core::model::{ModelId, SessionId, WindowKind};
use uw_policy::{
    RESUME_BLOCKING_REMAINING_PCT, ScheduledResume, WindowResetSlot, attribute_wake_to_window,
    effective_wake_time, pending_resume_wake_time, scheduled_sessions_per_window,
};

fn at(minutes: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + minutes * 60, 0).unwrap()
}

fn five_hour() -> WindowKind {
    WindowKind::Rolling { minutes: 300 }
}

fn weekly() -> WindowKind {
    WindowKind::WeeklyModel(ModelId("claude-sonnet".into()))
}

fn slot(kind: WindowKind, pct: f32, resets_in_minutes: i64) -> WindowResetSlot {
    WindowResetSlot {
        kind,
        pct,
        resets_at: Some(at(resets_in_minutes)),
    }
}

fn resume(id: &str, resume_at: Option<DateTime<Utc>>) -> ScheduledResume {
    ScheduledResume {
        session_id: SessionId(id.into()),
        resume_at,
    }
}

#[test]
fn scheduled_session_attributed_to_soonest_reset_on_or_after_wake() {
    let windows = vec![slot(five_hour(), 40.0, 60), slot(weekly(), 40.0, 600)];

    assert_eq!(
        attribute_wake_to_window(at(30), &windows),
        Some(five_hour())
    );
    assert_eq!(attribute_wake_to_window(at(90), &windows), Some(weekly()));
}

#[test]
fn scheduled_session_waking_exactly_at_a_reset_belongs_to_that_window() {
    let windows = vec![slot(five_hour(), 40.0, 60), slot(weekly(), 40.0, 600)];

    assert_eq!(
        attribute_wake_to_window(at(60), &windows),
        Some(five_hour())
    );
}

#[test]
fn scheduled_session_waking_after_every_reset_is_attributed_to_no_window() {
    let windows = vec![slot(five_hour(), 40.0, 60), slot(weekly(), 40.0, 600)];

    assert_eq!(attribute_wake_to_window(at(601), &windows), None);
}

#[test]
fn window_without_a_reset_time_never_claims_a_scheduled_session() {
    let windows = vec![
        WindowResetSlot {
            kind: five_hour(),
            pct: 40.0,
            resets_at: None,
        },
        slot(weekly(), 40.0, 600),
    ];

    assert_eq!(attribute_wake_to_window(at(30), &windows), Some(weekly()));
}

#[test]
fn pending_resume_wakes_at_the_soonest_blocked_window_reset() {
    // Only the weekly window is out of budget, so the resume waits for it even
    // though the five-hour window resets sooner.
    let windows = vec![
        slot(five_hour(), 40.0, 60),
        slot(weekly(), 100.0 - RESUME_BLOCKING_REMAINING_PCT, 600),
    ];

    assert_eq!(pending_resume_wake_time(&windows, at(0)), Some(at(600)));
}

#[test]
fn pending_resume_waits_for_the_later_reset_when_every_window_is_blocked() {
    let windows = vec![slot(five_hour(), 92.0, 60), slot(weekly(), 88.0, 600)];

    assert_eq!(pending_resume_wake_time(&windows, at(0)), Some(at(600)));
}

#[test]
fn pending_resume_with_no_blocked_window_wakes_immediately() {
    let windows = vec![slot(five_hour(), 40.0, 60), slot(weekly(), 10.0, 600)];

    assert_eq!(pending_resume_wake_time(&windows, at(0)), Some(at(0)));
}

#[test]
fn effective_wake_time_prefers_a_resolved_resume_at() {
    let windows = vec![slot(five_hour(), 92.0, 60), slot(weekly(), 88.0, 600)];

    assert_eq!(
        effective_wake_time(Some(at(75)), &windows, at(0)),
        Some(at(75))
    );
    assert_eq!(effective_wake_time(None, &windows, at(0)), Some(at(600)));
}

#[test]
fn each_scheduled_session_is_counted_against_exactly_one_window() {
    let windows = vec![slot(five_hour(), 40.0, 60), slot(weekly(), 40.0, 600)];
    let resumes = vec![
        resume("a", Some(at(30))),
        resume("b", Some(at(55))),
        resume("c", Some(at(120))),
    ];

    assert_eq!(
        scheduled_sessions_per_window(&resumes, &windows, at(0)),
        vec![2, 1]
    );
}

#[test]
fn pending_sessions_are_counted_against_the_window_that_will_release_them() {
    // Nothing is resolved yet; both windows are blocked, so both resumes wait
    // for the later reset and land on the weekly bar only.
    let windows = vec![slot(five_hour(), 92.0, 60), slot(weekly(), 88.0, 600)];
    let resumes = vec![resume("a", None), resume("b", None)];

    assert_eq!(
        scheduled_sessions_per_window(&resumes, &windows, at(0)),
        vec![0, 2]
    );
}

#[test]
fn scheduled_sessions_beyond_every_reset_are_counted_nowhere() {
    let windows = vec![slot(five_hour(), 40.0, 60), slot(weekly(), 40.0, 600)];
    let resumes = vec![resume("a", Some(at(9_000)))];

    assert_eq!(
        scheduled_sessions_per_window(&resumes, &windows, at(0)),
        vec![0, 0]
    );
}
