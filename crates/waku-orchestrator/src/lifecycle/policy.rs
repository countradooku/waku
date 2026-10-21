//! The auto-settle rule (#64), as a pure function of a thread's facts, its
//! pull request and the project's settings. Semantics follow T3's
//! `ThreadSettlementService` (audit §1.13): a thread settles once it has been
//! inactive for `after_days`, or as soon as its pull request merged (with
//! `on_merge`) or closed after the user last engaged with it; with
//! `keep_pull_requests`, a thread that has a pull request only settles from
//! that pull request.

use std::collections::HashMap;

use waku_protocol::orchestration::{ProjectId, ThreadId, Timestamp};

pub const DAY_MS: Timestamp = 24 * 60 * 60 * 1_000;
/// T3's default inactivity window.
pub const DEFAULT_AUTO_SETTLE_AFTER_DAYS: u32 = 3;

/// The effective rule for one project.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoSettlePolicy {
    /// Settle after this many days without activity; `None` disables the
    /// inactivity rule.
    pub after_days: Option<u32>,
    /// Settle when the thread's pull request merges.
    pub on_merge: bool,
    /// A thread with a pull request settles only from that pull request.
    pub keep_pull_requests: bool,
}

impl Default for AutoSettlePolicy {
    fn default() -> Self {
        Self {
            after_days: Some(DEFAULT_AUTO_SETTLE_AFTER_DAYS),
            on_merge: true,
            keep_pull_requests: false,
        }
    }
}

impl AutoSettlePolicy {
    pub const DISABLED: Self = Self {
        after_days: None,
        on_merge: false,
        keep_pull_requests: false,
    };

    /// Whether any rule can settle a thread.
    pub fn enabled(&self) -> bool {
        self.after_days.is_some() || self.on_merge
    }
}

/// A project's overrides; `None` inherits the default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AutoSettleOverride {
    /// `Some(None)` disables the inactivity rule for the project.
    pub after_days: Option<Option<u32>>,
    pub on_merge: Option<bool>,
    pub keep_pull_requests: Option<bool>,
}

/// The daemon-wide rule plus per-project overrides.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AutoSettleSettings {
    pub defaults: AutoSettlePolicy,
    pub projects: HashMap<ProjectId, AutoSettleOverride>,
}

impl AutoSettleSettings {
    pub fn disabled() -> Self {
        Self {
            defaults: AutoSettlePolicy::DISABLED,
            projects: HashMap::new(),
        }
    }

    pub fn for_project(&self, project_id: ProjectId) -> AutoSettlePolicy {
        let defaults = self.defaults;
        match self.projects.get(&project_id) {
            None => defaults,
            Some(overrides) => AutoSettlePolicy {
                after_days: overrides.after_days.unwrap_or(defaults.after_days),
                on_merge: overrides.on_merge.unwrap_or(defaults.on_merge),
                keep_pull_requests: overrides
                    .keep_pull_requests
                    .unwrap_or(defaults.keep_pull_requests),
            },
        }
    }

    /// Whether any project can auto-settle at all.
    pub fn enabled(&self) -> bool {
        self.defaults.enabled()
            || self
                .projects
                .keys()
                .any(|id| self.for_project(*id).enabled())
    }
}

/// A thread's pull request as last known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PullRequestState {
    Open,
    Merged { at: Timestamp },
    Closed { at: Timestamp },
}

/// Everything the rule reads about a thread, from its projection rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettlementFacts {
    pub thread_id: ThreadId,
    pub project_id: ProjectId,
    pub created_at: Timestamp,
    /// The snapshot an `AutoSettle` command carries; any later change makes
    /// it a no-op.
    pub updated_at: Timestamp,
    pub archived: bool,
    pub deleted: bool,
    pub settled: bool,
    /// The user settled or unsettled it by hand.
    pub settled_override: bool,
    pub pinned: bool,
    pub auto_settle_disabled: bool,
    /// A run is preparing through finalizing, or runs are queued.
    pub busy: bool,
    pub pending_requests: bool,
    /// The provider still runs background work it will wake the thread for.
    pub background_work: bool,
    /// The latest run failed.
    pub failed: bool,
    pub snoozed_until: Option<Timestamp>,
    pub snoozed_at: Option<Timestamp>,
    pub last_snooze_wake_at: Option<Timestamp>,
    /// The latest user message (the latest run's request).
    pub latest_user_message_at: Option<Timestamp>,
    pub latest_run_completed_at: Option<Timestamp>,
    /// The latest of `last_reply_at` and every run's request, start and
    /// completion; `None` for a thread nobody wrote in.
    pub activity_at: Option<Timestamp>,
}

impl SettlementFacts {
    /// Snoozed and not woken early by a finished or failed run.
    pub fn snoozed(&self, now: Timestamp) -> bool {
        if self.snoozed_until.is_none_or(|until| until <= now) {
            return false;
        }
        let completed_after_snooze = matches!(
            (self.snoozed_at, self.latest_run_completed_at),
            (Some(snoozed), Some(completed)) if completed > snoozed
        );
        let woke_on_error = self.failed && (self.snoozed_at.is_none() || completed_after_snooze);
        !(woke_on_error || completed_after_snooze)
    }
}

/// Whether the thread may auto-settle at all right now (T3's
/// `isAutoSettlementCandidate`).
pub fn is_candidate(facts: &SettlementFacts, now: Timestamp) -> bool {
    !(facts.archived
        || facts.deleted
        || facts.settled
        || facts.settled_override
        || facts.pinned
        || facts.auto_settle_disabled
        || facts.pending_requests
        || facts.busy
        || facts.background_work
        || facts.snoozed(now))
}

/// What to do about one thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing will settle it until its facts change.
    Never,
    /// Evaluate again at this time (inactivity elapses, or a snooze ends).
    At(Timestamp),
    SettleNow,
}

/// Decides one thread at `now`.
pub fn evaluate(
    facts: &SettlementFacts,
    pull_request: Option<PullRequestState>,
    policy: &AutoSettlePolicy,
    now: Timestamp,
) -> Verdict {
    if !is_candidate(facts, now) {
        // A snoozed thread becomes a candidate when its snooze ends.
        return match facts.snoozed_until {
            Some(until) if facts.snoozed(now) && is_candidate_unsnoozed(facts) => {
                Verdict::At(until)
            }
            _ => Verdict::Never,
        };
    }
    if let Some(pull_request) = pull_request {
        let terminal_at = match pull_request {
            PullRequestState::Merged { at } if policy.on_merge => Some(at),
            PullRequestState::Closed { at } => Some(at),
            _ => None,
        };
        // Only a pull request that ended after the user last engaged settles
        // the thread: one merged earlier records work the thread did.
        let anchor = facts
            .latest_user_message_at
            .map_or(facts.created_at, |at| at.max(facts.created_at));
        if terminal_at.is_some_and(|at| at >= anchor) {
            return Verdict::SettleNow;
        }
        if policy.keep_pull_requests {
            return Verdict::Never;
        }
    }
    let (Some(days), Some(activity_at)) = (policy.after_days, facts.activity_at) else {
        return Verdict::Never;
    };
    let woke_at = facts.snoozed_until.filter(|until| *until <= now);
    let inactive_since = [Some(activity_at), facts.last_snooze_wake_at, woke_at]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(activity_at);
    // T3 settles once `inactive_since < now - days`.
    let due = inactive_since + Timestamp::from(days) * DAY_MS + 1;
    if now >= due {
        Verdict::SettleNow
    } else {
        Verdict::At(due)
    }
}

/// Whether the thread would be a candidate if it were not snoozed.
fn is_candidate_unsnoozed(facts: &SettlementFacts) -> bool {
    let unsnoozed = SettlementFacts {
        snoozed_until: None,
        ..facts.clone()
    };
    is_candidate(&unsnoozed, Timestamp::MIN)
}
