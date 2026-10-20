//! #40 acceptance: killing the sink between any two steps never leaves
//! projections ahead of the log or effects without their events.
//!
//! Each fault point aborts the transaction where a crash would stop it; the
//! test then "restarts" (reopens the database) and verifies the projection
//! against a rebuild of the log, plus that every outbox row has an accepted
//! receipt.

use waku_protocol::orchestration::*;

use super::common::*;
use crate::orchestrator::DispatchError;
use crate::store::faults;
use crate::store::sink::FaultPoint;

fn snapshot(env: &Env) -> (i64, i64, i64, i64, i64) {
    (
        env.count("events"),
        env.count("effect_outbox"),
        env.count("command_receipts"),
        env.count("p_runs"),
        env.count("p_turn_items"),
    )
}

#[test]
fn a_crash_after_any_commit_step_leaves_no_partial_state() {
    for point in FaultPoint::ALL {
        let mut env = Env::new();
        let project = env.project();
        let thread = env.thread(project);
        // One committed run, so the crashing command has real state to
        // disturb: it would start a queued run and enqueue effects.
        let first = env.send(thread, "first", DispatchMode::Auto);
        env.assert_verified();
        let before = snapshot(&env);
        let projection_before = env.projection(thread);

        let message = fresh();
        faults::arm(point);
        let error = env
            .dispatch(
                "crashing",
                Env::send_command(thread, message, "second", DispatchMode::QueueAfterActive),
            )
            .unwrap_err();
        assert!(
            matches!(error, DispatchError::Storage(_)),
            "{point:?}: {error:?}"
        );
        assert_eq!(snapshot(&env), before, "{point:?} left partial rows");
        env.assert_verified();
        // The cache was dropped, so the live view still matches the log.
        assert_eq!(*env.projection(thread), *projection_before, "{point:?}");

        env.reopen();
        env.assert_verified();
        assert_eq!(snapshot(&env), before, "{point:?} after restart");
        // The command never happened, so its id is free and a retry works.
        let retry = env
            .dispatch(
                "crashing",
                Env::send_command(thread, message, "second", DispatchMode::QueueAfterActive),
            )
            .unwrap();
        assert!(!retry.duplicate, "{point:?}");
        assert_eq!(env.run(thread, run_for(message)).status, RunStatus::Queued);
        assert_eq!(env.run(thread, first).status, RunStatus::Starting);
        env.assert_verified();
    }
}

#[test]
fn a_crash_while_starting_a_queued_run_keeps_effects_with_their_events() {
    for point in FaultPoint::ALL {
        let mut env = Env::new();
        let project = env.project();
        let thread = env.thread(project);
        let first = env.send(thread, "first", DispatchMode::Auto);
        let second = env.send(thread, "second", DispatchMode::QueueAfterActive);
        let before = snapshot(&env);
        let outbox_before = env.outbox();

        // Interrupting the starting run cancels its start effect, ends it and
        // starts the queued one: events, a cancellation and a new effect.
        faults::arm(point);
        let error = env
            .dispatch(
                "interrupt",
                Command::RunInterrupt {
                    thread_id: thread,
                    run_id: first,
                    reason: None,
                    hold_queue: false,
                },
            )
            .unwrap_err();
        assert!(matches!(error, DispatchError::Storage(_)), "{point:?}");
        assert_eq!(snapshot(&env), before, "{point:?}");
        assert_eq!(
            env.outbox(),
            outbox_before,
            "{point:?}: cancellation leaked"
        );

        env.reopen();
        env.assert_verified();
        env.ok(Command::RunInterrupt {
            thread_id: thread,
            run_id: first,
            reason: None,
            hold_queue: false,
        });
        assert_eq!(env.run(thread, first).status, RunStatus::Interrupted);
        assert_eq!(env.run(thread, second).status, RunStatus::Starting);
        env.assert_verified();
    }
}

#[test]
fn a_crash_during_ingestion_drops_the_whole_decision() {
    for point in FaultPoint::ALL {
        let env = Env::new();
        let project = env.project();
        let thread = env.thread(project);
        let run = env.send(thread, "go", DispatchMode::Auto);
        go_running(&env, thread, run);
        let before = snapshot(&env);
        let projection_before = env.projection(thread);
        faults::arm(point);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assistant_text(&env, thread, run, "partial")
        }));
        // `ingest` unwraps, so the injected storage error panics the helper.
        assert!(result.is_err(), "{point:?} did not abort");
        assert_eq!(snapshot(&env), before, "{point:?}");
        assert_eq!(*env.projection(thread), *projection_before, "{point:?}");
        env.assert_verified();
    }
}
