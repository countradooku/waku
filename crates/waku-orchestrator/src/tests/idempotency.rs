//! #39 acceptance: re-dispatching a command id returns the same result and
//! emits nothing new (RFC 0001 rule 2, R16).

use std::sync::Arc;

use waku_protocol::orchestration::*;

use super::common::*;
use crate::command::CommandError;
use crate::orchestrator::DispatchError;

#[test]
fn duplicate_command_returns_the_stored_receipt_and_emits_nothing() {
    let env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    let message = fresh();
    let command = Env::send_command(thread, message, "hello", DispatchMode::Auto);

    let first = env.dispatch("send-1", command.clone()).unwrap();
    assert!(!first.duplicate);
    let events = env.count("events");
    let effects = env.count("effect_outbox");
    let receipts = env.count("command_receipts");
    let feed = env.orchestrator.subscribe();

    for _ in 0..3 {
        let again = env.dispatch("send-1", command.clone()).unwrap();
        assert!(again.duplicate);
        assert_eq!(again.sequence, first.sequence);
    }
    assert_eq!(env.count("events"), events);
    assert_eq!(env.count("effect_outbox"), effects);
    assert_eq!(env.count("command_receipts"), receipts);
    assert!(feed.try_recv().is_none(), "a duplicate published events");
    // The run exists once.
    assert_eq!(env.projection(thread).runs.len(), 1);
}

#[test]
fn duplicate_of_a_rejected_command_returns_the_stored_error() {
    let env = Env::new();
    let project = env.project();
    let thread = fresh::<ThreadId>();
    let rename = Command::ThreadRename {
        thread_id: thread,
        title: "Named".into(),
    };
    let first = env.dispatch("rename-1", rename.clone()).unwrap_err();
    assert!(matches!(
        first,
        DispatchError::Rejected(CommandError::NotFound { .. })
    ));
    // The thread appears afterwards, but the id keeps its original outcome
    // instead of being re-decided against newer state.
    env.ok(Command::ThreadCreate {
        thread_id: thread,
        project_id: project,
        title: None,
        model_selection: ModelSelection::new(INSTANCE),
        runtime_mode: RuntimeMode::FullAccess,
        interaction_mode: InteractionMode::Default,
        workspace: WorkspaceStrategy::Root { branch: None },
        imported_native_thread: None,
    });
    let events = env.count("events");
    let again = env.dispatch("rename-1", rename).unwrap_err();
    assert_eq!(again, first);
    assert_eq!(env.count("events"), events);
    assert_eq!(
        env.projection(thread).thread.title,
        crate::decide::PLACEHOLDER_TITLE
    );
}

#[test]
fn receipts_survive_a_restart() {
    let mut env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    let command = Env::send_command(thread, fresh(), "hi", DispatchMode::Auto);
    let first = env.dispatch("persisted", command.clone()).unwrap();
    env.reopen();
    let again = env.dispatch("persisted", command).unwrap();
    assert_eq!(again.sequence, first.sequence);
    assert!(again.duplicate);
}

#[test]
fn concurrent_duplicates_commit_once() {
    let env = Arc::new(Env::new());
    let project = env.project();
    let thread = env.thread(project);
    let command = Env::send_command(thread, fresh(), "race", DispatchMode::Auto);
    let handles = (0..8)
        .map(|_| {
            let env = env.clone();
            let command = command.clone();
            std::thread::spawn(move || env.dispatch("raced", command).unwrap())
        })
        .collect::<Vec<_>>();
    let receipts = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        receipts.iter().filter(|receipt| !receipt.duplicate).count(),
        1
    );
    assert!(
        receipts
            .windows(2)
            .all(|pair| pair[0].sequence == pair[1].sequence)
    );
    assert_eq!(env.projection(thread).runs.len(), 1);
    env.assert_verified();
}

#[test]
fn effect_results_with_derived_ids_are_no_ops_when_replayed() {
    let env = Env::new();
    let project = env.project();
    let thread = env.thread(project);
    let regenerate = env.ok(Command::ThreadRename {
        thread_id: thread,
        title: "Mine".into(),
    });
    assert!(!regenerate.duplicate);
    let command_id = cid("effect:41:1");
    let generated = crate::command::InternalCommand::TitleGenerated {
        thread_id: thread,
        request_id: cid("whatever"),
        title: Some("Generated".into()),
    };
    let first = env
        .orchestrator
        .dispatch_internal(command_id.clone(), generated.clone())
        .unwrap();
    let again = env
        .orchestrator
        .dispatch_internal(command_id, generated)
        .unwrap();
    assert!(again.duplicate);
    assert_eq!(again.sequence, first.sequence);
    // A user title is never overwritten by generation.
    assert_eq!(env.projection(thread).thread.title, "Mine");
}
