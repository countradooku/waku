//! #41 acceptance: per-thread ordering, the title lane does not block turns,
//! restart requeues replay-safe and cancels process-bound effects; plus
//! retries, cancellation, leases, pruning and the default executor.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use waku_protocol::orchestration::*;

use super::common::*;
use crate::clock::Clock;
use crate::decide::Decision;
use crate::effects::executor::DefaultExecutor;
use crate::effects::worker::{EffectWorkerPool, WorkerConfig};
use crate::effects::{Effect, EffectType, TitleKind};
use crate::fakes::{FakePorts, Script, ScriptedExecutor};
use crate::store::outbox::{self, EffectStatus};
use crate::store::sink::WriteGuard;

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    condition()
}

fn enqueue(env: &Env, thread: ThreadId, effects: Vec<Effect>) {
    env.ingest(
        thread,
        Decision {
            effects,
            ..Decision::default()
        },
        WriteGuard::None,
    );
}

fn cleanup(n: usize) -> Effect {
    Effect::WorktreeCleanup {
        path: PathBuf::from(format!("/tmp/wt-{n}")),
        branch: None,
    }
}

fn status_of(env: &Env, effect_type: &str) -> Vec<EffectStatus> {
    env.outbox()
        .into_iter()
        .filter(|(_, kind, _)| kind == effect_type)
        .map(|(_, _, status)| status)
        .collect()
}

fn config(workers: usize) -> WorkerConfig {
    WorkerConfig {
        workers,
        backoff_base: Duration::from_millis(10),
        backoff_cap: Duration::from_millis(100),
        liveness_poll: Duration::from_millis(200),
        ..WorkerConfig::default()
    }
}

#[test]
fn effects_of_one_thread_run_in_fifo_order_and_threads_run_in_parallel() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let first = env.thread(project);
    let second = env.thread(project);
    let executor = Arc::new(ScriptedExecutor::default());
    executor.script(
        EffectType::WorktreeCleanup,
        Script {
            delay: Duration::from_millis(15),
            ..Script::default()
        },
    );
    enqueue(&env, first, (0..6).map(cleanup).collect());
    enqueue(&env, second, (0..6).map(cleanup).collect());
    let pool = EffectWorkerPool::start(env.orchestrator.clone(), executor.clone(), config(4));
    assert!(executor.wait_for(12, Duration::from_secs(10)));
    pool.shutdown(Duration::from_secs(1));

    let executions = executor.executions();
    for thread in [first, second] {
        let mine = executions
            .iter()
            .filter(|execution| execution.thread_id == Some(thread))
            .collect::<Vec<_>>();
        assert_eq!(mine.len(), 6);
        assert!(
            mine.windows(2)
                .all(|pair| pair[0].effect_id < pair[1].effect_id),
            "FIFO by effect id"
        );
        assert!(
            mine.windows(2)
                .all(|pair| pair[0].finished <= pair[1].started),
            "never two effects of one thread lane at once"
        );
    }
    let overlap = executions.iter().any(|a| {
        executions
            .iter()
            .any(|b| a.thread_id != b.thread_id && a.started < b.finished && b.started < a.finished)
    });
    assert!(overlap, "different threads run concurrently");
    assert!(
        status_of(&env, "worktree.cleanup")
            .iter()
            .all(|status| *status == EffectStatus::Succeeded)
    );
}

#[test]
fn the_title_lane_does_not_block_turns() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let thread = env.thread(project);
    let executor = Arc::new(ScriptedExecutor::default());
    executor.script(
        EffectType::ThreadTitleGenerate,
        Script {
            gate: true,
            ..Script::default()
        },
    );
    let title = |n: &str| Effect::ThreadTitleGenerate {
        request_id: cid(n),
        kind: TitleKind::Regenerate,
    };
    // Titles first, then thread-lane work, all for one thread.
    enqueue(
        &env,
        thread,
        vec![title("t1"), title("t2"), cleanup(1), cleanup(2)],
    );
    let pool = EffectWorkerPool::start(env.orchestrator.clone(), executor.clone(), config(4));
    assert!(executor.wait_started(
        EffectType::ThreadTitleGenerate,
        Duration::from_secs(5),
        None
    ));
    // Both thread-lane effects finish while the title is stuck.
    assert!(wait_until(Duration::from_secs(5), || {
        status_of(&env, "worktree.cleanup") == vec![EffectStatus::Succeeded; 2]
    }));
    // The second title waits behind the first: the title lane is FIFO too.
    assert_eq!(
        status_of(&env, "thread_title.generate"),
        vec![EffectStatus::Running, EffectStatus::Pending]
    );
    executor.release();
    assert!(wait_until(Duration::from_secs(5), || {
        status_of(&env, "thread_title.generate") == vec![EffectStatus::Succeeded; 2]
    }));
    pool.shutdown(Duration::from_secs(1));
}

#[test]
fn restart_cancels_process_bound_and_requeues_replay_safe_effects() {
    let mut env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    let run = env.send(thread, "go", DispatchMode::Auto); // provider_turn.start + title
    enqueue(
        &env,
        thread,
        vec![
            Effect::CheckpointCapture {
                run_id: Some(run),
                scope_id: fresh(),
                turn_start: false,
            },
            Effect::RuntimeRequestRespond {
                provider_session_id: fresh(),
                request_id: fresh(),
                response: RuntimeResponse::Dismiss,
            },
            Effect::TerminalCleanup,
        ],
    );
    // Simulate the crashed process holding leases on the first claimable
    // rows of each lane: the turn start and the title.
    {
        let writer = env.orchestrator.store().writer();
        let now = env.clock.now();
        assert!(
            outbox::claim(&writer, "dead:0", now, 30_000)
                .unwrap()
                .is_some()
        );
        assert!(
            outbox::claim(&writer, "dead:1", now, 30_000)
                .unwrap()
                .is_some()
        );
    }
    let running_before = env
        .outbox()
        .into_iter()
        .filter(|(_, _, status)| *status == EffectStatus::Running)
        .map(|(_, kind, _)| kind)
        .collect::<Vec<_>>();
    // The thread lane starts with the run's turn-start checkpoint.
    assert_eq!(
        running_before,
        ["checkpoint.capture", "thread_title.generate"]
    );

    env.reopen();
    let report = EffectWorkerPool::reconcile_after_process_loss(&env.orchestrator).unwrap();
    let outbox = env.outbox();
    let by_type = |kind: &str| {
        outbox
            .iter()
            .find(|(_, effect_type, _)| effect_type == kind)
            .cloned()
            .unwrap()
    };
    // Process-bound, running or pending: cancelled.
    assert_eq!(by_type("provider_turn.start").2, EffectStatus::Cancelled);
    assert_eq!(
        by_type("runtime_request.respond").2,
        EffectStatus::Cancelled
    );
    // Replay-safe and running: back to pending.
    assert_eq!(by_type("thread_title.generate").2, EffectStatus::Pending);
    assert_eq!(by_type("checkpoint.capture").2, EffectStatus::Pending);
    // Replay-safe and pending: untouched.
    assert_eq!(
        outbox
            .iter()
            .filter(|(_, kind, status)| kind == "checkpoint.capture"
                && *status == EffectStatus::Pending)
            .count(),
        2
    );
    assert_eq!(by_type("terminal.cleanup").2, EffectStatus::Pending);
    assert_eq!(
        report.cancelled,
        vec![
            by_type("provider_turn.start").0,
            by_type("runtime_request.respond").0
        ]
    );
    assert_eq!(
        report.requeued,
        vec![
            by_type("checkpoint.capture").0,
            by_type("thread_title.generate").0
        ]
    );
    let reason: String = env
        .orchestrator
        .store()
        .writer()
        .query_row(
            "SELECT cancel_reason FROM effect_outbox WHERE effect_type = 'provider_turn.start'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reason, "process_loss");
}

#[test]
fn retryable_failures_back_off_and_then_succeed() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let thread = env.thread(project);
    let executor = Arc::new(ScriptedExecutor::default());
    executor.script(
        EffectType::WorktreeCleanup,
        Script {
            retryable_failures: 2,
            ..Script::default()
        },
    );
    enqueue(&env, thread, vec![cleanup(1), cleanup(2)]);
    let pool = EffectWorkerPool::start(env.orchestrator.clone(), executor.clone(), config(2));
    // The manual clock only moves when the test moves it; backoff is in it.
    assert!(wait_until(Duration::from_secs(10), || {
        env.clock.advance(50);
        pool.notify(1);
        executor.executions().len() >= 6
    }));
    pool.shutdown(Duration::from_secs(1));
    let executions = executor.executions();
    let first = executions
        .iter()
        .filter(|execution| execution.effect_id == executions[0].effect_id)
        .map(|execution| execution.attempt)
        .collect::<Vec<_>>();
    assert_eq!(first, [1, 2, 3]);
    // The second row waited for the first, backoff included (FIFO).
    let first_done = executions
        .iter()
        .filter(|e| e.effect_id == executions[0].effect_id)
        .map(|e| e.finished)
        .max()
        .unwrap();
    assert!(
        executions
            .iter()
            .filter(|e| e.effect_id != executions[0].effect_id)
            .all(|e| e.started >= first_done)
    );
    assert_eq!(
        status_of(&env, "worktree.cleanup"),
        vec![EffectStatus::Succeeded; 2]
    );
    assert_eq!(
        WorkerConfig::default().backoff(1),
        Duration::from_millis(100)
    );
    assert_eq!(
        WorkerConfig::default().backoff(3),
        Duration::from_millis(400)
    );
    assert_eq!(WorkerConfig::default().backoff(40), Duration::from_secs(30));
}

#[test]
fn permanent_errors_and_exhausted_retries_fail_and_free_the_lane() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let thread = env.thread(project);
    let executor = Arc::new(ScriptedExecutor::default());
    executor.script(
        EffectType::WorktreeCleanup,
        Script {
            permanent: true,
            ..Script::default()
        },
    );
    executor.script(
        EffectType::AttachmentCleanup,
        Script {
            retryable_failures: 100,
            ..Script::default()
        },
    );
    enqueue(
        &env,
        thread,
        vec![
            cleanup(1),
            Effect::AttachmentCleanup {
                attachment_ids: vec!["a".into()],
            },
            Effect::TerminalCleanup,
        ],
    );
    let pool = EffectWorkerPool::start(
        env.orchestrator.clone(),
        executor.clone(),
        WorkerConfig {
            max_attempts: 3,
            ..config(2)
        },
    );
    assert!(wait_until(Duration::from_secs(10), || {
        env.clock.advance(200);
        pool.notify(1);
        status_of(&env, "terminal.cleanup") == vec![EffectStatus::Succeeded]
    }));
    pool.shutdown(Duration::from_secs(1));
    assert_eq!(
        status_of(&env, "worktree.cleanup"),
        vec![EffectStatus::Failed]
    );
    assert_eq!(
        status_of(&env, "attachment.cleanup"),
        vec![EffectStatus::Failed]
    );
    let attempts = executor
        .executions()
        .iter()
        .filter(|execution| execution.effect_type == EffectType::AttachmentCleanup)
        .count();
    assert_eq!(attempts, 3);
}

#[test]
fn cancel_unsettled_cancels_a_running_effect() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let thread = env.thread(project);
    let executor = Arc::new(ScriptedExecutor::default());
    executor.script(
        EffectType::ProviderTurnStart,
        Script {
            gate: true,
            ..Script::default()
        },
    );
    let pool = EffectWorkerPool::start(env.orchestrator.clone(), executor.clone(), config(2));
    let run = env.send(thread, "go", DispatchMode::Auto);
    assert!(executor.wait_started(
        EffectType::ProviderTurnStart,
        Duration::from_secs(5),
        Some(thread)
    ));
    // No provider turn yet, so the interrupt cancels the start directly.
    env.ok(Command::RunInterrupt {
        thread_id: thread,
        run_id: run,
        reason: None,
        hold_queue: false,
    });
    assert!(wait_until(Duration::from_secs(5), || {
        executor
            .executions()
            .iter()
            .any(|execution| execution.effect_type == EffectType::ProviderTurnStart)
    }));
    pool.shutdown(Duration::from_secs(1));
    assert_eq!(
        status_of(&env, "provider_turn.start"),
        vec![EffectStatus::Cancelled]
    );
    assert_eq!(env.run(thread, run).status, RunStatus::Interrupted);
}

#[test]
fn expired_leases_return_to_pending() {
    let env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    enqueue(&env, thread, vec![cleanup(1)]);
    let writer = env.orchestrator.store().writer();
    let now = env.clock.now();
    let claimed = outbox::claim(&writer, "w", now, 1_000).unwrap().unwrap();
    assert_eq!(outbox::expire_leases(&writer, now + 500).unwrap(), 0);
    assert_eq!(outbox::expire_leases(&writer, now + 1_001).unwrap(), 1);
    // The old holder can no longer settle it.
    assert!(
        !outbox::settle(
            &writer,
            claimed.effect_id,
            "w",
            &outbox::Settle::Succeeded,
            now
        )
        .unwrap()
    );
    let again = outbox::claim(&writer, "w2", now + 1_002, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!(again.effect_id, claimed.effect_id);
    assert_eq!(again.attempt_count, 2);
}

#[test]
fn pruning_removes_old_settled_rows_and_keeps_failed_ones() {
    let env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    enqueue(&env, thread, (0..4).map(cleanup).collect());
    let writer = env.orchestrator.store().writer();
    writer
        .execute_batch(
            "UPDATE effect_outbox SET status = 'succeeded', settled_at = 10 WHERE effect_id = 1;
             UPDATE effect_outbox SET status = 'cancelled', settled_at = 10 WHERE effect_id = 2;
             UPDATE effect_outbox SET status = 'failed',    settled_at = 10 WHERE effect_id = 3;
             UPDATE effect_outbox SET status = 'succeeded', settled_at = 500 WHERE effect_id = 4;",
        )
        .unwrap();
    assert_eq!(outbox::prune(&writer, 100, 500).unwrap(), 2);
    let left = outbox::list(&writer).unwrap();
    assert_eq!(
        left.iter()
            .map(|(id, _, status)| (*id, *status))
            .collect::<Vec<_>>(),
        [(3, EffectStatus::Failed), (4, EffectStatus::Succeeded)]
    );
}

#[test]
fn the_default_executor_drives_a_deferred_run_to_completion() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let thread = env.thread(project);
    let ports = FakePorts::default();
    let provider = Arc::new(ScriptedExecutor::default());
    let executor = Arc::new(DefaultExecutor {
        provider: provider.clone(),
        ports: ports.ports(),
    });
    let pool = EffectWorkerPool::start(env.orchestrator.clone(), executor, config(4));

    let run = env.send(
        thread,
        "Build the parser\nwith tests",
        DispatchMode::DeferStart {
            workspace: WorkspaceStrategy::NewWorktree {
                base_ref: "main".into(),
                branch: Some("waku/parser".into()),
                start_from_origin: false,
            },
        },
    );
    // workspace.prepare → prepared_run.release → starting → provider_turn.start
    assert!(provider.wait_started(
        EffectType::ProviderTurnStart,
        Duration::from_secs(5),
        Some(thread)
    ));
    let projection = env.projection(thread);
    assert_eq!(projection.runs[&run].status, RunStatus::Starting);
    assert_eq!(
        projection.thread.worktree_path,
        Some(PathBuf::from("/tmp/wt"))
    );
    assert_eq!(projection.thread.branch.as_deref(), Some("waku/parser"));
    assert!(projection.thread.worktree_owned);
    // The title lane generated a title from the first message.
    assert!(wait_until(Duration::from_secs(5), || {
        env.projection(thread).thread.title_source == TitleSource::Generated
    }));
    assert_eq!(
        env.projection(thread).thread.title,
        "Title: Build the parser"
    );

    // The provider runs and finishes; the barrier captures a checkpoint.
    let queued = env.send(thread, "next", DispatchMode::QueueAfterActive);
    go_running(&env, thread, run).unwrap();
    assistant_text(&env, thread, run, "Done.").unwrap();
    provider_done(&env, thread, run).unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        env.run(thread, run).status == RunStatus::Completed
    }));
    let projection = env.projection(thread);
    let completed = &projection.runs[&run];
    let checkpoint = &projection.checkpoints[&completed.checkpoint_id.unwrap()];
    assert_eq!(checkpoint.status, CheckpointStatus::Ready);
    assert_eq!(checkpoint.app_run_ordinal, Some(1));
    assert!(ports.checkpoints.calls.lock()[0].starts_with("capture refs/waku/t/"));
    // The queued run started once the thread was free.
    assert_eq!(projection.runs[&queued].status, RunStatus::Starting);
    assert_eq!(projection.runs[&queued].ordinal, Some(2));
    pool.shutdown(Duration::from_secs(1));
    env.assert_verified();
}

#[test]
fn shutdown_cancels_effects_still_running_after_the_grace_period() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let thread = env.thread(project);
    let executor = Arc::new(ScriptedExecutor::default());
    executor.script(
        EffectType::WorktreeCleanup,
        Script {
            gate: true,
            ..Script::default()
        },
    );
    enqueue(&env, thread, vec![cleanup(1)]);
    let pool = EffectWorkerPool::start(env.orchestrator.clone(), executor.clone(), config(1));
    assert!(executor.wait_started(EffectType::WorktreeCleanup, Duration::from_secs(5), None));
    pool.shutdown(Duration::from_millis(50));
    assert!(wait_until(Duration::from_secs(5), || executor
        .executions()
        .len()
        == 1));
    // Left running for the next start's reconcile, which requeues it.
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(
        status_of(&env, "worktree.cleanup"),
        vec![EffectStatus::Running]
    );
    let report = EffectWorkerPool::reconcile_after_process_loss(&env.orchestrator).unwrap();
    assert_eq!(report.requeued.len(), 1);
    assert_eq!(
        status_of(&env, "worktree.cleanup"),
        vec![EffectStatus::Pending]
    );
}
