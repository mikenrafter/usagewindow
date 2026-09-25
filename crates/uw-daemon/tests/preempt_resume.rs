//! Wiring the preempt selection into resume scheduling: shortly before a
//! window resets, a session parked until the reset gets pulled forward onto
//! the budget that is about to expire.

use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use uw_core::adapter::TokenUsageRecord;
use uw_core::model::*;
use uw_daemon::{DaemonStore, SqliteDaemonStore, run_preempt_resume_tick};
use uw_store::Store;

fn session(id: &str) -> SessionSummary {
    SessionSummary {
        id: SessionId(id.into()),
        lineage: SessionLineage::default(),
        harness: Provider::ClaudeCode,
        model: Some(ModelId("claude-sonnet".into())),
        account: None,
        first_seen: Utc::now(),
        last_seen: Utc::now(),
        cwd: "/tmp".into(),
        state_path: None,
        context_window_size: Some(200_000),
        // The context that has to be written back to cache on resume.
        last_known_token_count: Some(20_000),
        last_known_context_pct: None,
        launch_mode: LaunchMode::Headless,
        pid: None,
        stopped_reason: Some(StopReason::UsageLimit { window: key() }),
        superseded_stop_reason: None,
        superseded_stop_reason_at: None,
        superseded_stop_reason_note: None,
        resume_marker: None,
        superseded_by: None,
        reseeded_from: None,
    }
}

fn key() -> WindowKey {
    WindowKey {
        provider: Provider::ClaudeCode,
        kind: WindowKind::Rolling { minutes: 300 },
    }
}

/// Seeds a window climbing to `pct` so the session has a measurable tempo,
/// plus per-turn token records so its share of that burn is attributable.
fn seed(store: &Store, session: &SessionSummary, now: DateTime<Utc>, resets_at: DateTime<Utc>) {
    store.insert_session(session).unwrap();
    for (minutes_ago, pct) in [(30, 80.0), (15, 88.0), (0, 96.0)] {
        store
            .insert_usage_sample(&UsageSample {
                at: now - Duration::minutes(minutes_ago),
                fetched_at: None,
                source: UsageSource::ProviderReported,
                provider: Provider::ClaudeCode,
                account: None,
                plan: None,
                windows: HashMap::from([(
                    key(),
                    UsageWindowState::new(pct, false, true, Some(resets_at), None),
                )]),
                credits: None,
            })
            .unwrap();
    }
    let records = [30, 20, 10, 0]
        .into_iter()
        .map(|minutes_ago| TokenUsageRecord {
            at: now - Duration::minutes(minutes_ago),
            model: session.model.clone(),
            input_tokens: 10_000,
            cached_input_tokens: 2_000,
            cache_write_input_tokens: 1_000,
            output_tokens: 2_000,
            reasoning_output_tokens: 0,
            total_tokens: 12_000,
            context_pct: None,
        })
        .collect::<Vec<_>>();
    store
        .insert_token_usage_records(&session.id, &records)
        .unwrap();
}

fn marker(session: &SessionSummary, resume_at: DateTime<Utc>) -> ResumeMarker {
    ResumeMarker {
        id: uuid::Uuid::new_v4(),
        session_id: session.id.clone(),
        reason: ResumeReason::AutoDetectedLimit,
        resume_at: Some(resume_at),
        requested_at: None,
        created_at: Utc::now(),
        status: ResumeStatus::Scheduled,
        message: None,
        preempt: true,
    }
}

#[tokio::test]
async fn preempt_tick_skips_markers_with_preempt_false() {
    let now = Utc::now();
    let resets_at = now + Duration::minutes(3);
    let waiting = session("no-preempt-session");
    let store = Store::open_memory().unwrap();
    seed(&store, &waiting, now, resets_at);
    let mut parked = marker(&waiting, resets_at);
    parked.preempt = false;
    store.insert_resume_marker(&parked).unwrap();
    let store = SqliteDaemonStore::new(store);

    let moved = run_preempt_resume_tick(
        &store,
        std::slice::from_ref(&waiting),
        now,
        &ThresholdProfile::default(),
    )
    .await
    .unwrap();

    assert_eq!(moved, 0);
    assert_eq!(
        store
            .active_resume_marker(&waiting.id)
            .await
            .unwrap()
            .unwrap()
            .resume_at,
        Some(resets_at)
    );
}

#[tokio::test]
async fn preempt_tick_still_moves_preempt_true_markers() {
    let now = Utc::now();
    let resets_at = now + Duration::minutes(3);
    let waiting = session("yes-preempt-session");
    let store = Store::open_memory().unwrap();
    seed(&store, &waiting, now, resets_at);
    let mut parked = marker(&waiting, resets_at);
    parked.preempt = true;
    store.insert_resume_marker(&parked).unwrap();
    let store = SqliteDaemonStore::new(store);

    let moved = run_preempt_resume_tick(
        &store,
        std::slice::from_ref(&waiting),
        now,
        &ThresholdProfile::default(),
    )
    .await
    .unwrap();

    assert_eq!(moved, 1);
}

#[tokio::test]
async fn preempt_tick_pulls_a_parked_resume_forward_before_the_reset() {
    let now = Utc::now();
    let resets_at = now + Duration::minutes(3);
    let waiting = session("parked-session");
    let store = Store::open_memory().unwrap();
    seed(&store, &waiting, now, resets_at);
    // Parked until the window resets, which is exactly what preempt targets.
    store
        .insert_resume_marker(&marker(&waiting, resets_at))
        .unwrap();
    let store = SqliteDaemonStore::new(store);

    let moved = run_preempt_resume_tick(
        &store,
        std::slice::from_ref(&waiting),
        now,
        &ThresholdProfile::default(),
    )
    .await
    .unwrap();

    assert_eq!(moved, 1);
    let pulled = store
        .active_resume_marker(&waiting.id)
        .await
        .unwrap()
        .expect("the marker is still active, just earlier")
        .resume_at
        .expect("a preempted marker is scheduled, not pending");
    assert!(
        pulled < resets_at && pulled >= now,
        "pulled={pulled} now={now} resets_at={resets_at}"
    );
}

#[tokio::test]
async fn preempt_tick_leaves_a_resume_alone_while_the_reset_is_far_off() {
    let now = Utc::now();
    let resets_at = now + Duration::hours(2);
    let waiting = session("far-off-session");
    let store = Store::open_memory().unwrap();
    seed(&store, &waiting, now, resets_at);
    store
        .insert_resume_marker(&marker(&waiting, resets_at))
        .unwrap();
    let store = SqliteDaemonStore::new(store);

    let moved = run_preempt_resume_tick(
        &store,
        std::slice::from_ref(&waiting),
        now,
        &ThresholdProfile::default(),
    )
    .await
    .unwrap();

    assert_eq!(moved, 0);
    assert_eq!(
        store
            .active_resume_marker(&waiting.id)
            .await
            .unwrap()
            .unwrap()
            .resume_at,
        Some(resets_at)
    );
}

#[tokio::test]
async fn preempt_tick_never_pulls_a_resume_that_is_already_earlier_than_the_reset() {
    let now = Utc::now();
    let resets_at = now + Duration::minutes(3);
    let waiting = session("already-soon-session");
    let already_soon = now + Duration::minutes(1);
    let store = Store::open_memory().unwrap();
    seed(&store, &waiting, now, resets_at);
    store
        .insert_resume_marker(&marker(&waiting, already_soon))
        .unwrap();
    let store = SqliteDaemonStore::new(store);

    let moved = run_preempt_resume_tick(
        &store,
        std::slice::from_ref(&waiting),
        now,
        &ThresholdProfile::default(),
    )
    .await
    .unwrap();

    assert_eq!(moved, 0);
    assert_eq!(
        store
            .active_resume_marker(&waiting.id)
            .await
            .unwrap()
            .unwrap()
            .resume_at,
        Some(already_soon)
    );
}

#[tokio::test]
async fn preempt_tick_moves_at_most_two_sessions_per_reset() {
    let now = Utc::now();
    let resets_at = now + Duration::minutes(3);
    let store = Store::open_memory().unwrap();
    let waiting = ["one", "two", "three", "four"]
        .map(session)
        .into_iter()
        .collect::<Vec<_>>();
    for candidate in &waiting {
        seed(&store, candidate, now, resets_at);
        store
            .insert_resume_marker(&marker(candidate, resets_at))
            .unwrap();
    }
    let store = SqliteDaemonStore::new(store);

    let moved = run_preempt_resume_tick(&store, &waiting, now, &ThresholdProfile::default())
        .await
        .unwrap();

    assert!(moved <= 2, "moved={moved}");
    let mut pulled = 0;
    for candidate in &waiting {
        let resume_at = store
            .active_resume_marker(&candidate.id)
            .await
            .unwrap()
            .unwrap()
            .resume_at
            .unwrap();
        if resume_at < resets_at {
            pulled += 1;
        }
    }
    assert_eq!(pulled, moved);
}
