//! `decide_ingest` through `Orchestrator::ingest` (RFC 0002 "Normalized
//! events", #44): guards, positions, child turns and terminals.

use waku_protocol::orchestration::*;

use super::common::*;
use crate::orchestrator::IngestOutcome;
use crate::provider::{
    AdapterEvent, IngestEnvelope, ProviderTurnRef, RunRef, ThreadDisposition, TurnOutcome,
    TurnTerminal,
};

fn reference(env: &Env, thread: ThreadId, run: RunId) -> (RunRef, ProviderSessionId, RunAttempt) {
    let projection = env.projection(thread);
    let run = projection.runs[&run].clone();
    let attempt = projection.attempts[&run.active_attempt_id.unwrap()].clone();
    let session = projection.provider_threads[&attempt.provider_thread_id]
        .provider_session_id
        .unwrap_or_else(|| session_for(attempt.provider_thread_id));
    (
        RunRef {
            thread_id: thread,
            run_id: run.id,
            attempt_id: attempt.id,
            root_node_id: attempt.root_node_id,
        },
        session,
        attempt,
    )
}

fn terminal(
    run: RunRef,
    provider_turn: Option<ProviderTurnRef>,
    outcome: TurnOutcome,
) -> AdapterEvent {
    AdapterEvent::TurnTerminal {
        terminal: TurnTerminal {
            run,
            provider_turn,
            outcome,
            thread_disposition: ThreadDisposition::Reusable,
            child_turns_continue: false,
            background_pending: false,
            summary: None,
        },
    }
}

#[test]
fn a_repeated_snapshot_is_a_noop_and_items_keep_their_position() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let run = env.send(thread, "go", DispatchMode::Auto);
    go_running(&env, thread, run).unwrap();
    assert!(matches!(
        assistant_text(&env, thread, run, "Hel"),
        Some(IngestOutcome::Committed(_))
    ));
    let position = |env: &Env| {
        env.projection(thread)
            .hot_items
            .values()
            .find(|item| matches!(item.kind, TurnItemKind::AssistantMessage { .. }))
            .map(|item| item.position)
    };
    let first = position(&env);
    assistant_text(&env, thread, run, "Hello");
    assert_eq!(position(&env), first, "a known item keeps its position");
    // The same snapshot again changes nothing (only the clock moved).
    let (reference, session, _) = reference(&env, thread, run);
    let item = env
        .projection(thread)
        .hot_items
        .values()
        .find(|item| matches!(item.kind, TurnItemKind::AssistantMessage { .. }))
        .cloned()
        .unwrap();
    let outcome = env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &reference,
        AdapterEvent::TurnItemUpdated { item },
    ));
    assert_eq!(outcome, IngestOutcome::Noop);
    env.assert_verified();
}

#[test]
fn a_late_frame_of_a_superseded_attempt_is_dropped() {
    let mut capabilities = Capabilities::default();
    capabilities.turns.interrupt = true;
    capabilities.turns.steering_by_interrupt_restart = true;
    let env = Env::with_capabilities(capabilities);
    let thread = env.thread(env.project());
    let run = env.send(thread, "go", DispatchMode::Auto);
    go_running(&env, thread, run).unwrap();
    let (old, session, _) = reference(&env, thread, run);
    // Steering without native support restarts the turn as a new attempt.
    env.send(
        thread,
        "change course",
        DispatchMode::SteerActive { target_run_id: run },
    );
    let projection = env.projection(thread);
    let attempt = &projection.attempts[&projection.runs[&run].active_attempt_id.unwrap()];
    assert_eq!(attempt.reason, AttemptReason::SteeringRestart);
    // The interrupted attempt's terminal arrives: the run goes on.
    env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &old,
        terminal(old, None, TurnOutcome::Interrupted),
    ));
    assert_eq!(env.run(thread, run).status, RunStatus::Running);
    // So does a late text snapshot of the old attempt: nothing is written.
    let before = env.orchestrator.last_sequence();
    let outcome = env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &old,
        AdapterEvent::GoalUpdated { goal: None },
    ));
    assert_eq!(outcome, IngestOutcome::Noop);
    assert_eq!(env.orchestrator.last_sequence(), before);
    env.assert_verified();
}

#[test]
fn a_child_turn_never_completes_the_parent_run() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let run = env.send(thread, "go", DispatchMode::Auto);
    go_running(&env, thread, run).unwrap();
    let (parent, session, attempt) = reference(&env, thread, run);
    let child_node: NodeId = fresh();
    let child_thread: ProviderThreadId = fresh();
    let child_turn: ProviderTurnId = fresh();
    let now = env.tick();
    let projection = env.projection(thread);
    let parent_thread = projection.provider_threads[&attempt.provider_thread_id].clone();
    env.adapter(IngestEnvelope::for_thread(
        thread,
        session,
        1,
        AdapterEvent::ProviderThreadUpdated {
            thread: ProviderThread {
                id: child_thread,
                owner_node_id: Some(child_node),
                native_thread_ref: Some(ProviderRef::strong(parent_thread.driver, "child")),
                created_at: now,
                updated_at: now,
                ..parent_thread
            },
            retained_native_turn_ids: None,
        },
    ));
    env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &parent,
        AdapterEvent::ProviderTurnUpdated {
            turn: ProviderTurn {
                id: child_turn,
                provider_thread_id: child_thread,
                node_id: child_node,
                run_attempt_id: None,
                native_turn_ref: None,
                ordinal: 1,
                status: ProviderTurnStatus::Running,
                started_at: Some(now),
                completed_at: None,
                token_usage: None,
                turn_token_usage: None,
            },
        },
    ));
    let child = ProviderTurnRef {
        provider_turn_id: child_turn,
        native: crate::provider::NativeRef::strong("child-turn"),
        ordinal: 1,
    };
    env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &parent,
        terminal(parent, Some(child), TurnOutcome::Completed),
    ));
    let projection = env.projection(thread);
    assert_eq!(projection.runs[&run].status, RunStatus::Running);
    assert_eq!(
        projection.provider_turns[&child_turn].status,
        ProviderTurnStatus::Completed
    );
    // The parent's own terminal enters the finalization barrier.
    provider_done(&env, thread, run).unwrap();
    assert_eq!(env.run(thread, run).status, RunStatus::Finalizing);
    env.assert_verified();
}

#[test]
fn a_usage_limit_fails_the_run_with_its_reset_time() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let run = env.send(thread, "go", DispatchMode::Auto);
    let queued = env.send(thread, "next", DispatchMode::QueueAfterActive);
    go_running(&env, thread, run).unwrap();
    let (reference, session, _) = reference(&env, thread, run);
    let mut failure = ProviderFailure::new(FailureClass::UsageLimit, "limit reached");
    failure.reset_at = Some(START + 3_600_000);
    env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &reference,
        terminal(reference, None, TurnOutcome::Failed { failure }),
    ));
    let projection = env.projection(thread);
    let failed = &projection.runs[&run];
    assert_eq!(failed.status, RunStatus::Failed);
    assert_eq!(
        failed.failure.as_ref().unwrap().reset_at,
        Some(START + 3_600_000)
    );
    assert_eq!(
        projection.thread.limit_recovery.as_ref().unwrap().reset_at,
        Some(START + 3_600_000)
    );
    // The same provider would fail the next message too: the queue holds.
    assert_eq!(projection.runs[&queued].status, RunStatus::Queued);
    assert!(projection.runs[&queued].queue_held);
    env.assert_verified();
}

#[test]
fn an_interrupt_before_the_turn_started_still_ends_the_run() {
    let env = Env::new();
    let thread = env.thread(env.project());
    let run = env.send(thread, "go", DispatchMode::Auto);
    // No provider turn yet: the run is starting.
    let (reference, session, _) = reference(&env, thread, run);
    env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &reference,
        terminal(reference, None, TurnOutcome::Cancelled),
    ));
    assert_eq!(env.run(thread, run).status, RunStatus::Interrupted);
    env.assert_verified();
}
