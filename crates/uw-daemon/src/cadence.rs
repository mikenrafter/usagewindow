use chrono::{DateTime, Utc};
use std::{collections::HashMap, time::Duration};
use uw_core::model::{SessionId, SessionSummary, ThresholdProfile};

pub(super) fn sensitive_interval(base: Duration, pct: f32, active: bool) -> Duration {
    if !active || !pct.is_finite() || !(0.0..=100.0).contains(&pct) {
        return base;
    }
    let cap = if pct >= 95.0 {
        1
    } else if pct >= 90.0 {
        5
    } else if pct >= 85.0 {
        15
    } else {
        return base;
    };
    base.min(Duration::from_secs(cap))
}

pub(super) fn idle_deadline(
    now: DateTime<Utc>,
    sessions: &[SessionSummary],
    profiles: &HashMap<SessionId, ThresholdProfile>,
) -> Option<Duration> {
    sessions
        .iter()
        .filter(|s| s.stopped_reason.is_none() && s.superseded_by.is_none())
        .filter_map(|session| {
            let profile = profiles.get(&session.id)?;
            let ttl = *profile.cache_ttl_by_provider.get(&session.harness)?;
            let age = now - session.last_seen;
            if age >= ttl {
                return None;
            }
            [
                ttl - profile.idle_compact.margin,
                ttl - chrono::Duration::seconds(30),
            ]
            .into_iter()
            .filter_map(|target| {
                (target > age)
                    .then(|| (target - age).to_std().ok())
                    .flatten()
            })
            .min()
        })
        .min()
}

/// Selects cadence using only live sessions and their matching account windows.
pub(super) fn provider_interval(
    base: Duration,
    provider: &uw_core::model::Provider,
    samples: &[uw_core::model::UsageSample],
    sessions: &[SessionSummary],
    now: DateTime<Utc>,
) -> Duration {
    samples
        .iter()
        .filter(|sample| &sample.provider == provider)
        .flat_map(|sample| {
            sample
                .windows
                .iter()
                .filter(move |(key, window)| {
                    &key.provider == provider
                        && window.active
                        && window.resets_at.is_none_or(|reset| reset > now)
                        && sessions.iter().any(|session| {
                            &session.harness == provider
                                && session.stopped_reason.is_none()
                                && session.superseded_by.is_none()
                                && super::is_cache_warm(session.last_seen, now)
                                && (session.account.is_none()
                                    || session.account.as_ref()
                                        == window.scope.as_ref().or(sample.account.as_ref()))
                        })
                })
                .map(|(_, window)| sensitive_interval(base, window.pct, true))
        })
        .fold(base, Duration::min)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, TimeZone};
    use uw_core::model::{Provider, StopReason};

    fn now() -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000, 0).unwrap()
    }

    fn fixture(age_seconds: i64) -> (SessionSummary, HashMap<SessionId, ThresholdProfile>) {
        let mut session = crate::tests::session();
        session.last_seen = now() - ChronoDuration::seconds(age_seconds);
        let mut profile = ThresholdProfile::default();
        profile
            .cache_ttl_by_provider
            .insert(Provider::ClaudeCode, ChronoDuration::minutes(5));
        let profiles = HashMap::from([(session.id.clone(), profile)]);
        (session, profiles)
    }

    #[test]
    fn sensitive_cadence_enters_each_band_at_the_inclusive_boundary() {
        let base = Duration::from_secs(60);
        for (pct, seconds) in [
            (0.0, 60),
            (84.999, 60),
            (85.0, 15),
            (89.999, 15),
            (90.0, 5),
            (94.999, 5),
            (95.0, 1),
            (99.99, 1),
            (100.0, 1),
        ] {
            assert_eq!(
                sensitive_interval(base, pct, true),
                Duration::from_secs(seconds),
                "pct={pct}"
            );
        }
    }

    #[test]
    fn sensitive_cadence_never_slows_an_already_faster_poll() {
        let base = Duration::from_millis(500);
        for pct in [0.0, 85.0, 90.0, 95.0, 100.0] {
            assert_eq!(sensitive_interval(base, pct, true), base);
        }
    }

    #[test]
    fn sensitive_cadence_requires_an_active_session_and_valid_percentage() {
        let base = Duration::from_secs(60);
        for pct in [85.0, 90.0, 95.0, 100.0] {
            assert_eq!(sensitive_interval(base, pct, false), base);
        }
        for pct in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -1.0, 100.1] {
            assert_eq!(sensitive_interval(base, pct, true), base, "pct={pct}");
        }
    }

    #[test]
    fn idle_deadline_hits_the_configured_trigger_without_minute_rounding() {
        let (session, profiles) = fixture(179);
        assert_eq!(
            idle_deadline(now(), &[session], &profiles),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn idle_deadline_includes_the_four_minute_thirty_second_warm_checkpoint() {
        let (session, profiles) = fixture(269);
        assert_eq!(
            idle_deadline(now(), &[session], &profiles),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn idle_deadline_hits_four_thirty_when_configured_with_a_thirty_second_margin() {
        let (session, mut profiles) = fixture(269);
        profiles.get_mut(&session.id).unwrap().idle_compact.margin = ChronoDuration::seconds(30);
        assert_eq!(
            idle_deadline(now(), &[session], &profiles),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn idle_deadline_uses_the_earliest_staggered_session() {
        let (first, mut profiles) = fixture(100);
        let mut second = first.clone();
        second.id = SessionId("second".into());
        second.last_seen = now() - ChronoDuration::seconds(173);
        profiles.insert(second.id.clone(), profiles[&first.id].clone());
        assert_eq!(
            idle_deadline(now(), &[first, second], &profiles),
            Some(Duration::from_secs(7))
        );
    }

    #[test]
    fn idle_deadline_uses_each_sessions_ttl_and_margin() {
        let (session, mut profiles) = fixture(1000);
        let profile = profiles.get_mut(&session.id).unwrap();
        profile
            .cache_ttl_by_provider
            .insert(Provider::ClaudeCode, ChronoDuration::minutes(20));
        profile.idle_compact.margin = ChronoDuration::seconds(90);
        assert_eq!(
            idle_deadline(now(), std::slice::from_ref(&session), &profiles),
            Some(Duration::from_secs(110))
        );
        let mut later = session;
        later.last_seen = now() - ChronoDuration::seconds(1150);
        assert_eq!(
            idle_deadline(now(), &[later], &profiles),
            Some(Duration::from_secs(20))
        );
    }

    #[test]
    fn idle_deadline_skips_reached_targets_without_returning_zero_forever() {
        let (session, profiles) = fixture(180);
        assert_eq!(
            idle_deadline(now(), &[session], &profiles),
            Some(Duration::from_secs(90))
        );
        let (session, profiles) = fixture(270);
        assert_eq!(
            idle_deadline(now(), std::slice::from_ref(&session), &profiles),
            None
        );
        assert_eq!(idle_deadline(now(), &[session], &profiles), None);
    }

    #[test]
    fn idle_deadline_ignores_superseded_sessions() {
        let (mut session, profiles) = fixture(179);
        session.superseded_by = Some(SessionId("replacement".into()));
        assert_eq!(idle_deadline(now(), &[session], &profiles), None);
    }

    #[test]
    fn idle_deadline_ignores_stopped_cold_and_unknown_ttl_sessions() {
        let (mut session, profiles) = fixture(179);
        session.stopped_reason = Some(StopReason::UserQuit);
        assert_eq!(idle_deadline(now(), &[session], &profiles), None);
        let (session, profiles) = fixture(301);
        assert_eq!(idle_deadline(now(), &[session], &profiles), None);
        let (session, mut profiles) = fixture(179);
        profiles
            .get_mut(&session.id)
            .unwrap()
            .cache_ttl_by_provider
            .clear();
        assert_eq!(idle_deadline(now(), &[session], &profiles), None);
        assert_eq!(idle_deadline(now(), &[], &HashMap::new()), None);
    }
}
