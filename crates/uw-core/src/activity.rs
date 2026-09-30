//! Provider-neutral outstanding work accounting.
use std::collections::HashSet;

/// Whether a session has outstanding tool calls or background work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionActivity {
    /// A complete activity record contains no outstanding work.
    Idle,
    /// At least one tool call or background job still needs a completion.
    Busy,
    /// The adapter cannot establish activity from a complete record.
    Unknown,
}
/// Adapter-normalized events keyed by tool-call or background-job identity.
#[derive(Clone, Debug)]
pub enum ActivityEvent {
    ToolStarted(String),
    ToolFinished(String),
    BackgroundStarted(String),
    BackgroundFinished(String),
}
#[derive(Default)]
pub struct ActivityTracker {
    tools: HashSet<String>,
    background: HashSet<String>,
}
impl ActivityTracker {
    pub fn observe(&mut self, event: ActivityEvent) {
        match event {
            ActivityEvent::ToolStarted(id) => {
                self.tools.insert(id);
            }
            ActivityEvent::ToolFinished(id) => {
                self.tools.remove(&id);
            }
            ActivityEvent::BackgroundStarted(id) => {
                self.background.insert(id);
            }
            ActivityEvent::BackgroundFinished(id) => {
                self.background.remove(&id);
            }
        }
    }
    pub fn activity(&self) -> SessionActivity {
        if self.tools.is_empty() && self.background.is_empty() {
            SessionActivity::Idle
        } else {
            SessionActivity::Busy
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn concurrent_foreground_tools_require_all_matching_completions() {
        let mut tracker = ActivityTracker::default();
        tracker.observe(ActivityEvent::ToolStarted("a".into()));
        tracker.observe(ActivityEvent::ToolStarted("b".into()));
        tracker.observe(ActivityEvent::ToolFinished("a".into()));
        assert_eq!(tracker.activity(), SessionActivity::Busy);
        tracker.observe(ActivityEvent::ToolFinished("unrelated".into()));
        assert_eq!(tracker.activity(), SessionActivity::Busy);
        tracker.observe(ActivityEvent::ToolFinished("b".into()));
        assert_eq!(tracker.activity(), SessionActivity::Idle);
    }
    #[test]
    fn background_work_survives_poll_completion_and_repeated_idle_checks() {
        let mut tracker = ActivityTracker::default();
        tracker.observe(ActivityEvent::ToolStarted("launch".into()));
        tracker.observe(ActivityEvent::BackgroundStarted("job".into()));
        tracker.observe(ActivityEvent::ToolFinished("launch".into()));
        tracker.observe(ActivityEvent::ToolStarted("poll".into()));
        tracker.observe(ActivityEvent::ToolFinished("poll".into()));
        for _ in 0..1000 {
            assert_eq!(tracker.activity(), SessionActivity::Busy);
        }
        tracker.observe(ActivityEvent::BackgroundFinished("other".into()));
        assert_eq!(tracker.activity(), SessionActivity::Busy);
        tracker.observe(ActivityEvent::BackgroundFinished("job".into()));
        assert_eq!(tracker.activity(), SessionActivity::Idle);
    }
}
