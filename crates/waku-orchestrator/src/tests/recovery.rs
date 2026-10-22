//! #59 over a real `state.sqlite`: a restart mid-turn reconciles the
//! effects, recovers the thread, and the continuation runs once.

use uuid::Uuid;
use waku_protocol::orchestration::*;

use super::common::*;
use crate::effects::worker::EffectWorkerPool;
use crate::recovery::{
    RESUMED_NOTICE, candidate_threads, continuation_command, recover_after_restart,
};
use crate::store::outbox::EffectStatus;

fn continue_rows(env: &Env) -> Vec<EffectStatus> {
    env.outbox()
        .into_iter()
        .filter(|(_, kind, _)| kind == "provider_runtime.continue")
        .map(|(_, _, status)| status)
        .collect()
}

/// What the session manager does with a `provider_runtime.continue` row.
fn run_continuation(env: &Env, thread_id: ThreadId, source: RunId) {
    let projection = env.projection(thread_id);
    let (command_id, command) = continuation_command(thread_id, &projection.runs[&source]);
    env.tick();
    env.orchestrator
        .dispatch_internal(command_id, command)
        .unwrap();
}

#[test]
fn a_restart_mid_turn_is_continued_once() {
    let mut env = Env::new();
    let project = env.project();
    let thread_id = env.thread(project);
    let quiet = env.thread(project);
    turn(&env, quiet, "hello", "hi");
    let run = env.send(thread_id, "Write the docs", DispatchMode::Auto);
    go_running(&env, thread_id, run).unwrap();
    assistant_text(&env, thread_id, run, "Writing");

    // kill -9: the next process reconciles and recovers.
    env.reopen();
    let reconciled = EffectWorkerPool::reconcile_after_process_loss(&env.orchestrator).unwrap();
    assert!(reconciled.requeued.is_empty());
    // The quiet thread's session was alive too: it is a candidate whose
    // recovery only stops that session.
    let mut expected = vec![thread_id, quiet];
    expected.sort_by_key(ToString::to_string);
    let candidates = env.orchestrator.reader().with(candidate_threads).unwrap();
    assert_eq!(candidates, expected);
    let boot = Uuid::from_u128(1);
    let report = recover_after_restart(&env.orchestrator, boot).unwrap();
    assert_eq!(report.threads, expected);
    let quiet_projection = env.projection(quiet);
    assert!(
        quiet_projection
            .provider_sessions
            .values()
            .all(|session| session.status == SessionStatus::Stopped)
    );
    assert!(
        quiet_projection
            .runs
            .values()
            .all(|run| run.status == RunStatus::Completed)
    );
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    assert_eq!(report.continuations, 1);
    assert_eq!(continue_rows(&env), [EffectStatus::Pending]);
    let interrupted = env.run(thread_id, run);
    assert_eq!(interrupted.status, RunStatus::Interrupted);

    // Running recovery again in the same boot is a receipt hit.
    let again = recover_after_restart(&env.orchestrator, boot).unwrap();
    assert!(again.threads.is_empty(), "nothing is left to recover");

    run_continuation(&env, thread_id, run);
    let projection = env.projection(thread_id);
    let continued = projection
        .runs
        .values()
        .find(|candidate| candidate.continuation.is_some())
        .unwrap()
        .clone();
    assert_eq!(continued.status, RunStatus::Starting);
    // A repeated effect is a receipt hit; the run exists once.
    run_continuation(&env, thread_id, run);
    assert_eq!(env.projection(thread_id).runs.len(), 2);

    // The continuation completes; another restart finds nothing to do.
    go_running(&env, thread_id, continued.id).unwrap();
    final_assistant(&env, thread_id, continued.id, "Done");
    provider_done(&env, thread_id, continued.id);
    env.reopen();
    EffectWorkerPool::reconcile_after_process_loss(&env.orchestrator).unwrap();
    let report = recover_after_restart(&env.orchestrator, Uuid::from_u128(2)).unwrap();
    assert_eq!(report.continuations, 0);
    let projection = env.projection(thread_id);
    assert_eq!(projection.runs.len(), 2, "continued once");

    // The resumed row is stored in the continuation's block.
    let resumed = env.orchestrator.reader().with(|connection| {
        connection
            .query_row(
                "SELECT COUNT(*) FROM p_turn_items WHERE run_id = ?1
                   AND json_extract(payload_json, '$.kind.lifecycle') = 'restart_resumed'
                   AND json_extract(payload_json, '$.kind.message') = ?2",
                rusqlite::params![continued.id.to_string(), RESUMED_NOTICE],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    });
    assert_eq!(resumed, 1);
    env.assert_verified();
}

#[test]
fn turning_the_setting_off_leaves_the_run_interrupted() {
    let mut env = Env::new();
    let project = env.project();
    let thread_id = env.thread(project);
    let run = env.send(thread_id, "Write the docs", DispatchMode::Auto);
    go_running(&env, thread_id, run).unwrap();
    env.reopen();
    env.orchestrator.set_continue_after_restart(false);
    EffectWorkerPool::reconcile_after_process_loss(&env.orchestrator).unwrap();
    let report = recover_after_restart(&env.orchestrator, Uuid::from_u128(1)).unwrap();
    assert_eq!(report.continuations, 0);
    assert!(continue_rows(&env).is_empty());
    assert_eq!(env.run(thread_id, run).status, RunStatus::Interrupted);
    env.assert_verified();
}
