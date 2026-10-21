//! Turn-start checkpoints, turn and thread diff ranges, rewinding the first
//! message, background-work stop and refresh, and session commands (#180).

use waku_protocol::orchestration::*;

use super::common::*;
use crate::checkpoints::{self, RangeError};
use crate::command::{CapturedCheckpoint, InternalCommand};
use crate::effects::Effect;
use crate::provider::{AdapterEvent, IngestEnvelope};
use crate::store::outbox::EffectStatus;

fn root_scope(env: &Env, thread: ThreadId, run: RunId) -> CheckpointScopeId {
    let projection = env.projection(thread);
    let node = projection.runs[&run].root_node_id.unwrap();
    projection.nodes[&node].checkpoint_scope_id.unwrap()
}

fn captured(env: &Env, id: &str, thread: ThreadId, run: RunId, turn_start: bool, ordinal: u32) {
    let scope_id = root_scope(env, thread, run);
    env.tick();
    env.orchestrator
        .dispatch_internal(
            cid(id),
            InternalCommand::CheckpointCaptured {
                thread_id: thread,
                run_id: Some(run),
                scope_id,
                turn_start,
                result: Ok(CapturedCheckpoint {
                    git_ref: format!("refs/waku/t/{thread}/{scope_id}/{ordinal}"),
                    files: Vec::new(),
                }),
            },
        )
        .unwrap();
}

/// Runs one turn the way the workers would: turn-start capture, provider
/// turn, finalization capture.
fn full_turn(env: &Env, thread: ThreadId, text: &str) -> RunId {
    let run = env.send(thread, text, DispatchMode::Auto);
    captured(env, &format!("start:{run}"), thread, run, true, 0);
    go_running(env, thread, run).unwrap();
    final_assistant(env, thread, run, "done");
    provider_done(env, thread, run).unwrap();
    captured(env, &format!("end:{run}"), thread, run, false, 1);
    assert_eq!(env.run(thread, run).status, RunStatus::Completed);
    run
}

fn effect_types(env: &Env) -> Vec<(String, EffectStatus)> {
    env.outbox()
        .into_iter()
        .map(|(_, kind, status)| (kind, status))
        .collect()
}

#[test]
fn a_run_captures_its_turn_start_before_the_provider_turn() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let run = env.send(thread, "go", DispatchMode::Auto);
    let kinds = effect_types(&env)
        .into_iter()
        .map(|(kind, _)| kind)
        .filter(|kind| kind != "thread_title.generate")
        .collect::<Vec<_>>();
    assert_eq!(kinds, ["checkpoint.capture", "provider_turn.start"]);

    captured(&env, "start", thread, run, true, 0);
    let projection = env.projection(thread);
    let start = projection
        .checkpoints
        .values()
        .find(|checkpoint| checkpoint.run_id == Some(run))
        .cloned()
        .unwrap();
    assert_eq!(start.ordinal_within_scope, 0);
    assert_eq!(start.app_run_ordinal, Some(0), "the state before run 1");
    assert_eq!(start.status, CheckpointStatus::Ready);
    assert_eq!(env.run(thread, run).status, RunStatus::Starting);
    assert!(
        !projection
            .hot_items
            .values()
            .any(|item| matches!(item.kind, TurnItemKind::Checkpoint { .. })),
        "a turn start shows no changed-files row"
    );
    // A replay (another command id, same result) records nothing new.
    captured(&env, "start-again", thread, run, true, 0);
    assert_eq!(env.projection(thread).checkpoints.len(), 1);
    env.assert_verified();
}

#[test]
fn a_turn_diffs_against_its_turn_start_and_the_thread_against_the_first() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let first = full_turn(&env, thread, "one");
    let second = full_turn(&env, thread, "two");
    let projection = env.projection(thread);

    let first_start = checkpoints::turn_start(&projection, &projection.runs[&first])
        .unwrap()
        .clone();
    let first_end = projection.runs[&first].checkpoint_id.unwrap();
    assert_eq!(
        projection.checkpoints[&first_end].parent_checkpoint_id,
        Some(first_start.id)
    );
    assert_eq!(projection.checkpoints[&first_end].ordinal_within_scope, 1);
    assert_eq!(projection.checkpoints[&first_end].app_run_ordinal, Some(1));

    let range = checkpoints::turn_range(&projection, first).unwrap();
    assert_eq!(range.from.id, first_start.id);
    assert_eq!(range.to.id, first_end);
    assert_eq!(range.cwd, std::path::Path::new("/repo"));

    let second_start = checkpoints::turn_start(&projection, &projection.runs[&second]).unwrap();
    assert_eq!(second_start.app_run_ordinal, Some(1));
    let range = checkpoints::turn_range(&projection, second).unwrap();
    assert_eq!(range.from.id, second_start.id);

    let whole = checkpoints::thread_range(&projection).unwrap();
    assert_eq!(whole.from.id, first_start.id);
    assert_eq!(whole.to.id, projection.runs[&second].checkpoint_id.unwrap());

    assert_eq!(
        checkpoints::turn_range(&projection, fresh()).unwrap_err(),
        RangeError::RunNotFound
    );
    // A running turn has nothing to diff yet.
    let running = env.send(thread, "three", DispatchMode::Auto);
    let projection = env.projection(thread);
    assert_eq!(
        checkpoints::turn_range(&projection, running).unwrap_err(),
        RangeError::NotCaptured
    );
}

#[test]
fn rewinding_the_first_message_rolls_every_run_back() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let first = full_turn(&env, thread, "one");
    let second = full_turn(&env, thread, "two");
    let projection = env.projection(thread);
    let target = checkpoints::rewind_point(&projection, &projection.runs[&first])
        .unwrap()
        .clone();
    assert_eq!(target.app_run_ordinal, Some(0));
    assert_eq!(
        checkpoints::rewind_point(&projection, &projection.runs[&second])
            .unwrap()
            .app_run_ordinal,
        Some(1)
    );

    env.dispatch(
        "rewind",
        Command::CheckpointRollback {
            thread_id: thread,
            scope_id: target.scope_id,
            checkpoint_id: target.id,
            restore_files: true,
        },
    )
    .unwrap();
    assert!(effect_types(&env).iter().any(
        |(kind, status)| kind == "provider_thread.rollback" && *status == EffectStatus::Pending
    ));
    env.tick();
    env.orchestrator
        .dispatch_internal(
            cid("rewind-applied"),
            InternalCommand::CheckpointRollbackApplied {
                thread_id: thread,
                request_id: cid("rewind"),
                snapshot: None,
            },
        )
        .unwrap();
    assert_eq!(env.run(thread, first).status, RunStatus::RolledBack);
    assert_eq!(env.run(thread, second).status, RunStatus::RolledBack);
    assert!(env.projection(thread).thread.rollback.is_none());
    assert_eq!(
        checkpoints::thread_range(&env.projection(thread)).unwrap_err(),
        RangeError::NoTurns
    );

    // The next message is the first live run again: its turn start is its
    // own rewind point.
    let next = full_turn(&env, thread, "again");
    let projection = env.projection(thread);
    let point = checkpoints::rewind_point(&projection, &projection.runs[&next]).unwrap();
    assert_eq!(point.run_id, Some(next));
    env.assert_verified();
}

/// A provider thread of `thread` that reports `task` as running.
fn hold_task(env: &Env, thread: ThreadId, run: RunId, task: PendingBackgroundTask) {
    let projection = env.projection(thread);
    let attempt = projection.attempts[&projection.runs[&run].active_attempt_id.unwrap()].clone();
    let session = projection.provider_threads[&attempt.provider_thread_id]
        .provider_session_id
        .unwrap();
    env.adapter(IngestEnvelope::for_thread(
        thread,
        session,
        1,
        AdapterEvent::BackgroundWorkUpdated {
            work: crate::provider::BackgroundWorkItem {
                provider_thread_id: attempt.provider_thread_id,
                task,
                live: true,
            },
        },
    ));
}

fn task(kind: PendingTaskKind, id: &str) -> PendingBackgroundTask {
    PendingBackgroundTask {
        kind,
        task_id: id.into(),
        description: Some("npm run dev".into()),
        parent_task_id: None,
        child_thread_id: None,
        item_id: Some(fresh()),
    }
}

fn pending_tasks(env: &Env, thread: ThreadId) -> Vec<String> {
    let mut tasks = env
        .projection(thread)
        .provider_threads
        .values()
        .flat_map(|provider_thread| provider_thread.pending_background_tasks.clone())
        .map(|task| task.task_id)
        .collect::<Vec<_>>();
    tasks.sort();
    tasks
}

#[test]
fn background_work_stops_through_its_session_or_settles_without_one() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let run = env.send(thread, "go", DispatchMode::Auto);
    go_running(&env, thread, run).unwrap();
    hold_task(&env, thread, run, task(PendingTaskKind::Command, "proc-1"));
    hold_task(
        &env,
        thread,
        run,
        task(PendingTaskKind::Subagent, "agent-1"),
    );
    assert_eq!(pending_tasks(&env, thread), ["agent-1", "proc-1"]);

    // A subagent is stopped with `subagent.stop`; an unknown task is not
    // found.
    assert!(
        env.try_ok(Command::BackgroundWorkStop {
            thread_id: thread,
            task_id: "agent-1".into(),
            provider_thread_id: None,
        })
        .is_err()
    );
    assert!(
        env.try_ok(Command::BackgroundWorkStop {
            thread_id: thread,
            task_id: "nope".into(),
            provider_thread_id: None,
        })
        .is_err()
    );

    // With the session open, the session manager stops it.
    env.ok(Command::BackgroundWorkStop {
        thread_id: thread,
        task_id: "proc-1".into(),
        provider_thread_id: None,
    });
    let stop = env
        .outbox()
        .into_iter()
        .find(|(_, kind, _)| kind == "background_work.stop")
        .unwrap();
    assert_eq!(stop.2, EffectStatus::Pending);
    let payload: String = env
        .orchestrator
        .store()
        .writer()
        .query_row(
            "SELECT payload_json FROM effect_outbox WHERE effect_type = 'background_work.stop'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let effect: Effect = serde_json::from_str(&payload).unwrap();
    assert!(
        matches!(effect, Effect::BackgroundWorkStop { ref task_id, .. } if task_id == "proc-1")
    );
    // Its result settles the task.
    let provider_thread_id = match effect {
        Effect::BackgroundWorkStop {
            provider_thread_id, ..
        } => provider_thread_id,
        _ => unreachable!(),
    };
    env.tick();
    env.orchestrator
        .dispatch_internal(
            cid("settled"),
            InternalCommand::BackgroundWorkSettle {
                thread_id: thread,
                provider_thread_id,
                task_id: "proc-1".into(),
                outcome: crate::command::BackgroundOutcome::Cancelled,
            },
        )
        .unwrap();
    assert_eq!(pending_tasks(&env, thread), ["agent-1"]);

    // Once the session is gone nothing runs the work: stop and refresh
    // drop it at once.
    hold_task(&env, thread, run, task(PendingTaskKind::Monitor, "watch-1"));
    let session = env
        .projection(thread)
        .provider_sessions
        .values()
        .next()
        .unwrap()
        .clone();
    env.adapter(IngestEnvelope::for_thread(
        thread,
        session.id,
        1,
        AdapterEvent::SessionUpdated {
            session: ProviderSession {
                status: SessionStatus::Stopped,
                detached_at: Some(env.tick()),
                ..session
            },
        },
    ));
    env.ok(Command::BackgroundWorkStop {
        thread_id: thread,
        task_id: "watch-1".into(),
        provider_thread_id: None,
    });
    assert_eq!(pending_tasks(&env, thread), ["agent-1"]);
    env.ok(Command::BackgroundWorkRefresh { thread_id: thread });
    assert!(pending_tasks(&env, thread).is_empty());
    assert!(
        !effect_types(&env)
            .iter()
            .any(|(kind, _)| kind == "background_work.refresh"),
        "no session to ask"
    );
    env.assert_verified();
}

#[test]
fn refreshing_asks_the_open_session() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let run = env.send(thread, "go", DispatchMode::Auto);
    go_running(&env, thread, run).unwrap();
    env.ok(Command::BackgroundWorkRefresh { thread_id: thread });
    assert!(effect_types(&env).iter().any(
        |(kind, status)| kind == "background_work.refresh" && *status == EffectStatus::Pending
    ));
}

#[test]
fn session_commands_are_stored_on_the_session() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let run = env.send(thread, "go", DispatchMode::Auto);
    go_running(&env, thread, run).unwrap();
    let session = *env
        .projection(thread)
        .provider_sessions
        .keys()
        .next()
        .unwrap();
    let commands = vec![
        SessionCommand {
            name: "review".into(),
            description: Some("Review the changes".into()),
        },
        SessionCommand {
            name: "compact".into(),
            description: None,
        },
    ];
    env.adapter(IngestEnvelope::for_thread(
        thread,
        session,
        1,
        AdapterEvent::AvailableCommandsUpdated {
            commands: commands.clone(),
        },
    ));
    assert_eq!(
        env.projection(thread).provider_sessions[&session].available_commands,
        commands
    );
    env.assert_verified();
}

#[test]
fn a_scratch_project_needs_the_daemon_host() {
    let env = Env::new();
    let error = env
        .try_ok(Command::ProjectCreateScratch {
            project_id: fresh(),
            name: None,
            prompt: None,
        })
        .unwrap_err();
    assert!(error.to_string().contains("daemon host"), "{error}");
}
