//! The "pull earlier" write the preempt path needs. `set_resume_at` only ever
//! resolves a pending marker and `defer_resume_marker` only pushes markers out,
//! so neither can move a scheduled resume forward. Lives in `tests/` rather
//! than the crate's `mod tests` only because the in-crate test module does not
//! currently build.

use chrono::{DateTime, Duration, Utc};
use uw_core::model::{
    LaunchMode, ModelId, Provider, ResumeMarker, ResumeReason, ResumeStatus, SessionId,
    SessionLineage, SessionSummary,
};
use uw_store::Store;

fn session(id: &SessionId) -> SessionSummary {
    SessionSummary {
        id: id.clone(),
        lineage: SessionLineage::default(),
        harness: Provider::ClaudeCode,
        model: Some(ModelId("claude-sonnet".into())),
        account: None,
        first_seen: Utc::now(),
        last_seen: Utc::now(),
        cwd: "/tmp".into(),
        state_path: None,
        context_window_size: None,
        last_known_token_count: None,
        last_known_context_pct: None,
        launch_mode: LaunchMode::Headless,
        pid: None,
        stopped_reason: None,
        superseded_stop_reason: None,
        superseded_stop_reason_at: None,
        superseded_stop_reason_note: None,
        resume_marker: None,
        superseded_by: None,
        reseeded_from: None,
    }
}

/// A store holding one session with one marker in the given state.
fn store_with_marker(
    status: ResumeStatus,
    resume_at: Option<DateTime<Utc>>,
) -> (Store, SessionId, uuid::Uuid) {
    let store = Store::open_memory().unwrap();
    let id = SessionId("preempt-session".into());
    store.insert_session(&session(&id)).unwrap();
    let marker = ResumeMarker {
        id: uuid::Uuid::new_v4(),
        session_id: id.clone(),
        reason: ResumeReason::AutoDetectedLimit,
        resume_at,
        requested_at: None,
        created_at: Utc::now(),
        status,
        message: None,
        preempt: true,
    };
    store.insert_resume_marker(&marker).unwrap();
    (store, id, marker.id)
}

fn only_marker(store: &Store, id: &SessionId) -> ResumeMarker {
    let markers = store.resume_markers_for_session(id).unwrap();
    assert_eq!(markers.len(), 1);
    markers[0].clone()
}

#[test]
fn pull_resume_earlier_moves_a_scheduled_marker_forward() {
    let reset = Utc::now() + Duration::hours(1);
    let (store, session_id, marker_id) = store_with_marker(ResumeStatus::Scheduled, Some(reset));
    let earlier = reset - Duration::minutes(3);

    assert!(store.pull_resume_earlier(marker_id, earlier).unwrap());

    let marker = only_marker(&store, &session_id);
    assert_eq!(marker.resume_at, Some(earlier));
    assert_eq!(marker.status, ResumeStatus::Scheduled);
}

#[test]
fn pull_resume_earlier_refuses_to_move_a_marker_later() {
    let reset = Utc::now() + Duration::hours(1);
    let (store, session_id, marker_id) = store_with_marker(ResumeStatus::Scheduled, Some(reset));

    assert!(
        !store
            .pull_resume_earlier(marker_id, reset + Duration::minutes(10))
            .unwrap()
    );

    assert_eq!(only_marker(&store, &session_id).resume_at, Some(reset));
}

#[test]
fn pull_resume_earlier_schedules_a_pending_marker_that_has_no_time_yet() {
    let (store, session_id, marker_id) = store_with_marker(ResumeStatus::Pending, None);
    let soon = Utc::now() + Duration::minutes(3);

    assert!(store.pull_resume_earlier(marker_id, soon).unwrap());

    let marker = only_marker(&store, &session_id);
    assert_eq!(marker.resume_at, Some(soon));
    assert_eq!(marker.status, ResumeStatus::Scheduled);
}

#[test]
fn pull_resume_earlier_never_revives_a_fired_marker() {
    let fired_at = Utc::now() - Duration::hours(1);
    let (store, session_id, marker_id) = store_with_marker(ResumeStatus::Fired, Some(fired_at));

    assert!(
        !store
            .pull_resume_earlier(marker_id, fired_at - Duration::minutes(10))
            .unwrap()
    );

    let marker = only_marker(&store, &session_id);
    assert_eq!(marker.status, ResumeStatus::Fired);
    assert_eq!(marker.resume_at, Some(fired_at));
}

#[test]
fn pull_resume_earlier_reports_an_unknown_marker_as_unmoved() {
    let (store, _, _) = store_with_marker(ResumeStatus::Scheduled, Some(Utc::now()));

    assert!(
        !store
            .pull_resume_earlier(uuid::Uuid::new_v4(), Utc::now())
            .unwrap()
    );
}

#[test]
fn resume_marker_preempt_round_trips_true_and_false() {
    for preempt in [true, false] {
        let store = Store::open_memory().unwrap();
        let id = SessionId(format!("preempt-flag-{preempt}"));
        store.insert_session(&session(&id)).unwrap();
        let marker = ResumeMarker {
            id: uuid::Uuid::new_v4(),
            session_id: id.clone(),
            reason: ResumeReason::ManuallyMarked,
            resume_at: None,
            requested_at: None,
            created_at: Utc::now(),
            status: ResumeStatus::Pending,
            message: None,
            preempt,
        };
        store.insert_resume_marker(&marker).unwrap();
        assert_eq!(only_marker(&store, &id).preempt, preempt);
    }
}

#[test]
fn opening_a_pre_preempt_database_defaults_preempt_to_true() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys=ON;
            CREATE TABLE sessions(
                id TEXT PRIMARY KEY, harness TEXT NOT NULL, model TEXT, account TEXT,
                cwd TEXT NOT NULL, state_path TEXT, context_window_size INTEGER,
                last_known_token_count INTEGER, launch_mode TEXT NOT NULL, pid INTEGER,
                first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, stopped_reason TEXT,
                stopped_window_kind TEXT, superseded_by TEXT, reseeded_from TEXT
            );
            CREATE TABLE resume_markers(
                id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id),
                reason TEXT NOT NULL, resume_at TEXT, requested_at TEXT, created_at TEXT NOT NULL,
                status TEXT NOT NULL, status_detail TEXT, message TEXT
            );
            INSERT INTO sessions(id,harness,cwd,launch_mode,first_seen,last_seen)
            VALUES('legacy','"Codex"','/tmp','"Headless"','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z');
            INSERT INTO resume_markers(id,session_id,reason,resume_at,requested_at,created_at,status,status_detail,message)
            VALUES('11111111-1111-1111-1111-111111111111','legacy','"ManuallyMarked"',NULL,NULL,'2020-01-01T00:00:00Z','pending',NULL,NULL);
            "#,
        )
        .unwrap();
    }
    let store = Store::open(path.to_str().unwrap()).unwrap();
    let marker = only_marker(&store, &SessionId("legacy".into()));
    assert!(marker.preempt);
}
