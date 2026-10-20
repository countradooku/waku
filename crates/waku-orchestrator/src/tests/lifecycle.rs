//! Server-owned thread lifecycle: the auto-settle rule, its scheduling and
//! snooze wake (#64).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;
use waku_protocol::orchestration::*;

use super::common::*;
use crate::clock::Clock as _;
use crate::command::InternalCommand;
use crate::lifecycle::policy::DAY_MS;
use crate::lifecycle::{
    AutoSettleOverride, AutoSettlePolicy, AutoSettleSettings, Fire, LifecycleService,
    NoPullRequests, PullRequestSource, PullRequestState, Scheduler, SettlementFacts, Verdict,
    evaluate,
};

const NOW: Timestamp = 1_800_000_000_000;
const RECENT: Timestamp = NOW - DAY_MS;

fn idle_facts() -> SettlementFacts {
    SettlementFacts {
        thread_id: ThreadId(Uuid::from_u128(1)),
        project_id: ProjectId(Uuid::from_u128(2)),
        created_at: NOW - 30 * DAY_MS,
        updated_at: NOW - 10 * DAY_MS,
        archived: false,
        deleted: false,
        settled: false,
        settled_override: false,
        pinned: false,
        auto_settle_disabled: false,
        busy: false,
        pending_requests: false,
        background_work: false,
        failed: false,
        snoozed_until: None,
        snoozed_at: None,
        last_snooze_wake_at: None,
        latest_user_message_at: Some(NOW - 10 * DAY_MS),
        latest_run_completed_at: Some(NOW - 10 * DAY_MS),
        activity_at: Some(NOW - 10 * DAY_MS),
    }
}

const THREE_DAYS: AutoSettlePolicy = AutoSettlePolicy {
    after_days: Some(3),
    on_merge: true,
    keep_pull_requests: false,
};

#[test]
fn the_auto_settle_rule_as_a_table() {
    type Edit = fn(&mut SettlementFacts);
    let merged = |at| Some(PullRequestState::Merged { at });
    let closed = |at| Some(PullRequestState::Closed { at });
    let cases: Vec<(
        &str,
        Edit,
        Option<PullRequestState>,
        AutoSettlePolicy,
        Verdict,
    )> = vec![
        (
            "inactive past the window",
            |_| {},
            None,
            THREE_DAYS,
            Verdict::SettleNow,
        ),
        (
            "active inside the window",
            |facts| facts.activity_at = Some(NOW - DAY_MS),
            None,
            THREE_DAYS,
            Verdict::At(NOW - DAY_MS + 3 * DAY_MS + 1),
        ),
        (
            "exactly at the window is not yet past it",
            |facts| facts.activity_at = Some(NOW - 3 * DAY_MS),
            None,
            THREE_DAYS,
            Verdict::At(NOW + 1),
        ),
        (
            "nobody ever wrote",
            |facts| facts.activity_at = None,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "archived",
            |facts| facts.archived = true,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "deleted",
            |facts| facts.deleted = true,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "already settled",
            |facts| facts.settled = true,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "settled or unsettled by hand",
            |facts| facts.settled_override = true,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "pinned",
            |facts| facts.pinned = true,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "auto-settle off for the thread",
            |facts| facts.auto_settle_disabled = true,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "a run is busy",
            |facts| facts.busy = true,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "waiting on the user",
            |facts| facts.pending_requests = true,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "background work pending",
            |facts| facts.background_work = true,
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "snoozed: look again when the snooze ends",
            |facts| {
                facts.snoozed_until = Some(NOW + DAY_MS);
                facts.snoozed_at = Some(NOW - DAY_MS);
            },
            None,
            THREE_DAYS,
            Verdict::At(NOW + DAY_MS),
        ),
        (
            "snoozed and pinned: nothing",
            |facts| {
                facts.snoozed_until = Some(NOW + DAY_MS);
                facts.pinned = true;
            },
            None,
            THREE_DAYS,
            Verdict::Never,
        ),
        (
            "a run finished after the snooze wakes it early",
            |facts| {
                facts.snoozed_until = Some(NOW + DAY_MS);
                facts.snoozed_at = Some(NOW - 20 * DAY_MS);
            },
            None,
            THREE_DAYS,
            Verdict::SettleNow,
        ),
        (
            "an ended snooze counts as activity",
            |facts| {
                facts.snoozed_until = Some(NOW - DAY_MS);
                facts.snoozed_at = Some(NOW - 5 * DAY_MS);
            },
            None,
            THREE_DAYS,
            Verdict::At(NOW - DAY_MS + 3 * DAY_MS + 1),
        ),
        (
            "a snooze wake counts as activity",
            |facts| facts.last_snooze_wake_at = Some(NOW - 2 * DAY_MS),
            None,
            THREE_DAYS,
            Verdict::At(NOW - 2 * DAY_MS + 3 * DAY_MS + 1),
        ),
        (
            "merged after the last message settles at once",
            |facts| facts.activity_at = Some(RECENT),
            merged(RECENT + 1),
            THREE_DAYS,
            Verdict::SettleNow,
        ),
        (
            "merged before the last message does not",
            |facts| {
                facts.activity_at = Some(RECENT);
                facts.latest_user_message_at = Some(RECENT);
            },
            merged(RECENT - 1),
            THREE_DAYS,
            Verdict::At(RECENT + 3 * DAY_MS + 1),
        ),
        (
            "merge rule off: inactivity still applies",
            |facts| facts.activity_at = Some(RECENT),
            merged(RECENT + 1),
            AutoSettlePolicy {
                on_merge: false,
                ..THREE_DAYS
            },
            Verdict::At(RECENT + 3 * DAY_MS + 1),
        ),
        (
            "a closed pull request settles even without the merge rule",
            |facts| facts.activity_at = Some(RECENT),
            closed(RECENT + 1),
            AutoSettlePolicy {
                on_merge: false,
                ..THREE_DAYS
            },
            Verdict::SettleNow,
        ),
        (
            "keep threads with a pull request: an open one holds it",
            |_| {},
            Some(PullRequestState::Open),
            AutoSettlePolicy {
                keep_pull_requests: true,
                ..THREE_DAYS
            },
            Verdict::Never,
        ),
        (
            "without keep, an open pull request does not hold it",
            |_| {},
            Some(PullRequestState::Open),
            THREE_DAYS,
            Verdict::SettleNow,
        ),
        (
            "inactivity rule off",
            |_| {},
            None,
            AutoSettlePolicy {
                after_days: None,
                ..THREE_DAYS
            },
            Verdict::Never,
        ),
        (
            "inactivity rule off, merged pull request",
            |_| {},
            merged(NOW),
            AutoSettlePolicy {
                after_days: None,
                ..THREE_DAYS
            },
            Verdict::SettleNow,
        ),
    ];
    for (name, edit, pull_request, policy, expected) in cases {
        let mut facts = idle_facts();
        edit(&mut facts);
        assert_eq!(
            evaluate(&facts, pull_request, &policy, NOW),
            expected,
            "{name}"
        );
    }
}

#[test]
fn project_overrides_inherit_the_defaults() {
    let quiet = ProjectId(Uuid::from_u128(10));
    let keep = ProjectId(Uuid::from_u128(11));
    let other = ProjectId(Uuid::from_u128(12));
    let settings = AutoSettleSettings {
        defaults: THREE_DAYS,
        projects: HashMap::from([
            (
                quiet,
                AutoSettleOverride {
                    after_days: Some(None),
                    on_merge: Some(false),
                    ..AutoSettleOverride::default()
                },
            ),
            (
                keep,
                AutoSettleOverride {
                    after_days: Some(Some(7)),
                    keep_pull_requests: Some(true),
                    ..AutoSettleOverride::default()
                },
            ),
        ]),
    };
    assert_eq!(settings.for_project(other), THREE_DAYS);
    assert_eq!(settings.for_project(quiet), AutoSettlePolicy::DISABLED);
    assert!(!settings.for_project(quiet).enabled());
    assert_eq!(
        settings.for_project(keep),
        AutoSettlePolicy {
            after_days: Some(7),
            on_merge: true,
            keep_pull_requests: true,
        }
    );
    assert!(settings.enabled());
    assert!(!AutoSettleSettings::disabled().enabled());
}

/// A thread with one finished turn at the current clock.
fn finished_thread(env: &Env, project: ProjectId) -> ThreadId {
    let thread_id = env.thread(project);
    turn(env, thread_id, "do the thing", "done");
    thread_id
}

fn poll(env: &Env, scheduler: &mut Scheduler) -> Vec<Fire> {
    let now = env.clock.now();
    env.orchestrator
        .reader()
        .with(|connection| scheduler.poll(connection, &NoPullRequests, now))
        .unwrap()
}

fn reload_all(env: &Env, scheduler: &mut Scheduler) {
    let now = env.clock.now();
    env.orchestrator
        .reader()
        .with(|connection| scheduler.reload_all(connection, &NoPullRequests, now))
        .unwrap();
}

#[test]
fn the_scheduler_keeps_one_deadline_per_thread_and_fires_when_due() {
    let env = Env::new();
    let project = env.project();
    let idle = finished_thread(&env, project);
    let pinned = finished_thread(&env, project);
    env.ok(Command::ThreadPin {
        thread_id: pinned,
        order_key: None,
    });
    let empty = env.thread(project);
    let snoozed = finished_thread(&env, project);
    let wake_at = env.clock.now() + DAY_MS;
    env.ok(Command::ThreadSnooze {
        thread_id: snoozed,
        until: wake_at,
    });

    let mut scheduler = Scheduler::new(AutoSettleSettings {
        defaults: THREE_DAYS,
        projects: HashMap::new(),
    });
    reload_all(&env, &mut scheduler);
    // The idle thread (inactivity) and the snoozed one (wake, then a
    // check); the pinned and the empty thread have nothing scheduled.
    assert_eq!(scheduler.tracked(), 2);
    let _ = empty;
    assert_eq!(scheduler.next_due(), Some(wake_at));
    assert!(poll(&env, &mut scheduler).is_empty());

    env.clock.advance(DAY_MS);
    let fires = poll(&env, &mut scheduler);
    assert_eq!(
        fires,
        vec![Fire::Wake {
            thread_id: snoozed,
            at: wake_at
        }]
    );

    env.clock.advance(3 * DAY_MS);
    let fires = poll(&env, &mut scheduler);
    assert!(fires.iter().any(|fire| matches!(
        fire,
        Fire::Settle { thread_id, .. } if *thread_id == idle
    )));
    assert!(!fires.iter().any(|fire| matches!(
        fire,
        Fire::Settle { thread_id, .. } if *thread_id == pinned
    )));
}

/// Waits until `check` holds or five seconds pass.
fn eventually(mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if check() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    check()
}

#[test]
fn the_service_settles_idle_threads_and_wakes_snoozed_ones() {
    let env = Env::new();
    let project = env.project();
    let idle = finished_thread(&env, project);
    let busy = env.thread(project);
    env.send(busy, "still running", DispatchMode::Auto);
    let snoozed = finished_thread(&env, project);
    let wake_at = env.clock.now() + DAY_MS;
    env.ok(Command::ThreadSnooze {
        thread_id: snoozed,
        until: wake_at,
    });
    let service = LifecycleService::start(
        env.orchestrator.clone(),
        AutoSettleSettings {
            defaults: THREE_DAYS,
            projects: HashMap::new(),
        },
        Arc::new(NoPullRequests),
    );

    // The snooze ends: the thread wakes.
    env.clock.advance(DAY_MS);
    service.wake();
    assert!(eventually(|| {
        env.projection(snoozed).thread.snoozed_until.is_none()
    }));
    let woken = env.projection(snoozed).thread.clone();
    assert_eq!(woken.last_snooze_wake_at, Some(wake_at));

    // Three idle days later the idle thread settles; the busy one does not.
    env.clock.advance(3 * DAY_MS - 10);
    service.wake();
    assert!(eventually(|| env
        .projection(idle)
        .thread
        .settled_at
        .is_some()));
    assert!(env.projection(busy).thread.settled_at.is_none());
    // The woken thread's inactivity counts from its wake.
    assert!(env.projection(snoozed).thread.settled_at.is_none());
    env.clock.advance(DAY_MS);
    service.wake();
    assert!(eventually(|| {
        env.projection(snoozed).thread.settled_at.is_some()
    }));
    // Settling closes the thread's terminals.
    assert!(
        env.outbox()
            .iter()
            .any(|(_, effect_type, _)| effect_type == "terminal.cleanup")
    );

    // A new message brings a settled thread back; it may settle again later.
    env.send(idle, "one more thing", DispatchMode::Auto);
    let thread = env.projection(idle).thread.clone();
    assert!(thread.settled_at.is_none());
    assert!(thread.settled_override.is_none());
    service.shutdown();
    env.assert_verified();
}

#[test]
fn settings_changes_and_pull_requests_reschedule() {
    struct Merged(Timestamp);
    impl PullRequestSource for Merged {
        fn pull_request(&self, _thread: &AppThread) -> Option<PullRequestState> {
            Some(PullRequestState::Merged { at: self.0 })
        }
    }
    let env = Env::new();
    let project = env.project();
    let thread_id = finished_thread(&env, project);
    let service = LifecycleService::start(
        env.orchestrator.clone(),
        AutoSettleSettings::disabled(),
        Arc::new(Merged(env.clock.now() + 1)),
    );
    std::thread::sleep(Duration::from_millis(50));
    assert!(env.projection(thread_id).thread.settled_at.is_none());
    // Turning the merge rule on settles the merged thread at once.
    service.set_settings(AutoSettleSettings {
        defaults: AutoSettlePolicy {
            after_days: None,
            on_merge: true,
            keep_pull_requests: false,
        },
        projects: HashMap::new(),
    });
    assert!(eventually(|| {
        env.projection(thread_id).thread.settled_at.is_some()
    }));
    drop(service);
}

#[test]
fn auto_settle_is_a_no_op_after_the_thread_changed() {
    let env = Env::new();
    let project = env.project();
    let thread_id = finished_thread(&env, project);
    let snapshot = env.projection(thread_id).thread.updated_at;
    env.ok(Command::ThreadRename {
        thread_id,
        title: "Renamed".into(),
    });
    let auto_settle = |id: &str, snapshot_at| {
        env.tick();
        env.orchestrator
            .dispatch_internal(
                cid(id),
                InternalCommand::AutoSettle {
                    thread_id,
                    snapshot_at,
                },
            )
            .unwrap()
    };
    auto_settle("stale", snapshot);
    assert!(env.projection(thread_id).thread.settled_at.is_none());
    // Pinned threads never auto-settle.
    env.ok(Command::ThreadPin {
        thread_id,
        order_key: None,
    });
    let snapshot = env.projection(thread_id).thread.updated_at;
    auto_settle("pinned", snapshot);
    assert!(env.projection(thread_id).thread.settled_at.is_none());
    env.ok(Command::ThreadUnpin { thread_id });
    let snapshot = env.projection(thread_id).thread.updated_at;
    auto_settle("fresh", snapshot);
    let thread = env.projection(thread_id).thread.clone();
    assert!(thread.settled_at.is_some());
    // Auto-settling leaves the override to the user.
    assert!(thread.settled_override.is_none());
}
