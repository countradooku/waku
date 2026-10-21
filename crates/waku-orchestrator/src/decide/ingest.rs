//! `decide_ingest`: one normalized adapter event to events, effects and a
//! write guard (RFC 0001 pipeline, RFC 0002 "Normalized events").
//!
//! Drivers report entity snapshots keyed by app ids. This module owns what
//! drivers must not decide:
//!
//! - **positions** of turn items (R15): a new item takes the next position in
//!   its run's block, a known item keeps its position;
//! - **run state**: the run follows its active attempt's provider turn
//!   (`starting` → `running`), waits while a request is pending, and on the
//!   turn's terminal enters the finalization barrier or ends;
//! - **child turns**: a subagent's turn never completes the parent run;
//! - **guards**: run-scoped events commit only while their attempt is the
//!   run's active one (`WriteGuard::RunCurrent`), so a late frame from an
//!   interrupted or superseded attempt is dropped whole.
//!
//! Re-delivered snapshots that equal the projection produce no events, so
//! the same frame ingested twice (a driver's own turn-started event and the
//! session manager's synthesized one) is a no-op the second time.

use waku_protocol::orchestration::*;

use super::run::{finish_run, interrupted_row, provider_turn_failed};
use super::{DecideContext, Decision, Draft};
use crate::command::CommandError;
use crate::effects::Effect;
use crate::projection::ThreadProjection;
use crate::provider::{
    AdapterEvent, IngestEnvelope, ThreadDisposition, TurnOutcome, TurnTerminal, derived,
};
use crate::store::sink::WriteGuard;

/// A decision for one adapter event and the guard its commit needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestDecision {
    pub decision: Decision,
    pub guard: WriteGuard,
}

/// Decides one adapter event. Pure: no I/O is reachable.
pub fn decide_ingest(
    cx: &mut DecideContext<'_>,
    input: &IngestEnvelope,
) -> Result<IngestDecision, CommandError> {
    let projection = match cx.thread {
        Some(projection) if projection.id() == input.thread_id => projection,
        _ => return Err(CommandError::not_found("thread", input.thread_id)),
    };
    let project_path = cx.project.map(|project| project.path.clone());
    let mut draft = Draft::new(projection, cx.now, cx.ids, project_path);
    let guard = ingest(&mut draft, input)?;
    let mut decision = draft.finish();
    decision
        .events
        .retain(|event| !unchanged(projection, event));
    Ok(IngestDecision { decision, guard })
}

/// The run and attempt an envelope addresses, when that attempt is still the
/// run's active one and the run has not ended.
fn current(draft: &Draft<'_>, input: &IngestEnvelope) -> Option<(Run, RunAttempt)> {
    let run = draft.run(input.run_id?)?.clone();
    let attempt_id = input.attempt_id?;
    if run.status.is_terminal() || run.active_attempt_id != Some(attempt_id) {
        return None;
    }
    let attempt = draft.attempt(attempt_id)?.clone();
    Some((run, attempt))
}

fn run_guard(run: &Run, attempt: &RunAttempt) -> WriteGuard {
    WriteGuard::RunCurrent {
        run_id: run.id,
        attempt_id: attempt.id,
    }
}

fn owner_guard(provider_thread: &ProviderThread) -> WriteGuard {
    match provider_thread.app_thread_id {
        Some(thread_id) => WriteGuard::ProviderThreadOwner {
            provider_thread_id: provider_thread.id,
            thread_id,
            owner_node_id: provider_thread.owner_node_id,
        },
        None => WriteGuard::None,
    }
}

/// An event scoped to a run that is no longer current decides nothing.
fn stale() -> Result<WriteGuard, CommandError> {
    Ok(WriteGuard::None)
}

fn ingest(draft: &mut Draft<'_>, input: &IngestEnvelope) -> Result<WriteGuard, CommandError> {
    match &input.event {
        AdapterEvent::SessionUpdated { session } => {
            if session.id != input.provider_session_id {
                return Err(CommandError::invalid(
                    "session.id",
                    "differs from the envelope's provider session",
                ));
            }
            draft.put_provider_session(session.clone());
            Ok(WriteGuard::None)
        }
        AdapterEvent::ProviderThreadUpdated { thread, .. } => provider_thread(draft, input, thread),
        AdapterEvent::ProviderTurnUpdated { turn } => provider_turn(draft, input, turn),
        AdapterEvent::NodeUpdated { node } => {
            if node.run_id.is_none() {
                draft.put_node(ExecutionNode {
                    thread_id: input.thread_id,
                    ..node.clone()
                });
                return Ok(WriteGuard::None);
            }
            let Some((run, attempt)) = current(draft, input) else {
                return stale();
            };
            if node.run_id != Some(run.id) {
                return stale();
            }
            draft.put_node(ExecutionNode {
                thread_id: input.thread_id,
                ..node.clone()
            });
            Ok(run_guard(&run, &attempt))
        }
        AdapterEvent::SubagentUpdated { subagent } => subagent_updated(draft, input, subagent),
        AdapterEvent::MessageUpdated { message } => {
            let Some((run, attempt)) = current(draft, input) else {
                return stale();
            };
            draft.put_message(Message {
                thread_id: input.thread_id,
                run_id: Some(run.id),
                node_id: message.node_id.or(run.root_node_id),
                ..message.clone()
            });
            Ok(run_guard(&run, &attempt))
        }
        AdapterEvent::TurnItemUpdated { item } => {
            let Some((run, attempt)) = current(draft, input) else {
                return stale();
            };
            put_run_item(draft, &run, &attempt, item.clone());
            Ok(run_guard(&run, &attempt))
        }
        AdapterEvent::RuntimeRequestUpdated { request } => runtime_request(draft, input, request),
        AdapterEvent::PlanUpdated { plan } => {
            let Some((run, attempt)) = current(draft, input) else {
                return stale();
            };
            let node_id = plan.node_id;
            let plan = Plan {
                thread_id: input.thread_id,
                run_id: Some(run.id),
                ..plan.clone()
            };
            let kind = match &plan.kind {
                PlanKind::TodoList { steps, explanation } => TurnItemKind::TodoList {
                    plan_id: plan.id,
                    steps: steps.clone(),
                    explanation: explanation.clone(),
                },
                PlanKind::ProposedPlan { markdown } => TurnItemKind::ProposedPlan {
                    plan_id: plan.id,
                    markdown: markdown.clone(),
                    streaming: false,
                },
                PlanKind::Unknown => TurnItemKind::Unknown,
            };
            let status = match plan.status {
                PlanStatus::Completed | PlanStatus::Superseded => ItemStatus::Completed,
                _ => ItemStatus::Running,
            };
            draft.put_plan(plan.clone());
            if !kind.is_unknown() {
                let mut item = template_item(draft, &run, &attempt, derived::plan_item(plan.id));
                item.node_id = Some(node_id);
                item.status = status;
                item.kind = kind;
                put_run_item(draft, &run, &attempt, item);
            }
            Ok(run_guard(&run, &attempt))
        }
        AdapterEvent::BackgroundWorkUpdated { work } => {
            let Some(mut provider_thread) = draft.provider_thread(work.provider_thread_id).cloned()
            else {
                return Ok(WriteGuard::None);
            };
            let guard = owner_guard(&provider_thread);
            let tasks = &mut provider_thread.pending_background_tasks;
            let position = tasks
                .iter()
                .position(|task| task.task_id == work.task.task_id);
            match (position, work.live) {
                (Some(index), true) => tasks[index] = work.task.clone(),
                (None, true) => tasks.push(work.task.clone()),
                (Some(index), false) => {
                    tasks.remove(index);
                }
                (None, false) => return Ok(WriteGuard::None),
            }
            provider_thread.updated_at = draft.now;
            draft.put_provider_thread(provider_thread);
            Ok(guard)
        }
        AdapterEvent::GoalUpdated { goal } => {
            let Some(mut provider_thread) = draft
                .thread
                .active_provider_thread_id
                .and_then(|id| draft.provider_thread(id))
                .cloned()
            else {
                return Ok(WriteGuard::None);
            };
            if &provider_thread.goal == goal {
                return Ok(WriteGuard::None);
            }
            let guard = owner_guard(&provider_thread);
            provider_thread.goal = goal.clone();
            provider_thread.updated_at = draft.now;
            draft.put_provider_thread(provider_thread);
            Ok(guard)
        }
        AdapterEvent::CommandOutputDelta { .. } | AdapterEvent::AccountUsageUpdated { .. } => {
            // Routed by the session manager; never thread state.
            Ok(WriteGuard::None)
        }
        AdapterEvent::TurnTerminal { terminal } => turn_terminal(draft, input, terminal),
    }
}

fn provider_thread(
    draft: &mut Draft<'_>,
    input: &IngestEnvelope,
    thread: &ProviderThread,
) -> Result<WriteGuard, CommandError> {
    if thread
        .app_thread_id
        .is_some_and(|owner| owner != input.thread_id)
    {
        return Err(CommandError::invalid(
            "thread.app_thread_id",
            "belongs to another thread",
        ));
    }
    let incoming = ProviderThread {
        app_thread_id: Some(input.thread_id),
        ..thread.clone()
    };
    let existing = draft.provider_thread(incoming.id).cloned();
    let guard = match &existing {
        Some(existing) => owner_guard(existing),
        None => match incoming
            .native_thread_ref
            .as_ref()
            .and_then(|reference| reference.native_id.clone())
        {
            Some(native_id) => WriteGuard::UnlessNativeThreadOwned {
                driver: incoming.driver,
                native_id,
                thread_id: input.thread_id,
            },
            None => WriteGuard::None,
        },
    };
    let active = draft.thread.active_provider_thread_id;
    // A rollback that replaced the provider thread: the replacement says it
    // was forked from the active one, and the app thread follows it. The old
    // row stays for lineage (RFC 0002 "Degradation policies").
    let replaces_active = existing.is_none()
        && incoming.owner_node_id.is_none()
        && active.is_some()
        && incoming
            .forked_from
            .as_ref()
            .is_some_and(|fork| Some(fork.provider_thread_id) == active);
    // A thread without a provider thread adopts the first root one it is
    // given (a native fork into a new app thread).
    let adopts = existing.is_none() && incoming.owner_node_id.is_none() && active.is_none();
    if replaces_active || adopts {
        draft
            .touch_thread(ThreadChange::ActiveProviderThreadChanged)
            .active_provider_thread_id = Some(incoming.id);
    }
    // A broken or closed active thread is not reused: the next run opens a
    // fresh provider thread.
    if matches!(
        incoming.status,
        ProviderThreadStatus::Closed | ProviderThreadStatus::Error
    ) && active == Some(incoming.id)
    {
        draft
            .touch_thread(ThreadChange::ActiveProviderThreadChanged)
            .active_provider_thread_id = None;
    }
    draft.put_provider_thread(incoming);
    Ok(guard)
}

fn provider_turn(
    draft: &mut Draft<'_>,
    input: &IngestEnvelope,
    turn: &ProviderTurn,
) -> Result<WriteGuard, CommandError> {
    let Some(provider_thread) = draft.provider_thread(turn.provider_thread_id).cloned() else {
        return Err(CommandError::not_found(
            "provider thread",
            turn.provider_thread_id,
        ));
    };
    let current = current(draft, input).filter(|(_, attempt)| {
        turn.run_attempt_id == Some(attempt.id)
            && provider_thread.owner_node_id.is_none()
            && attempt.provider_thread_id == provider_thread.id
    });
    let Some((run, attempt)) = current else {
        // A provider-initiated turn, a child turn or a superseded attempt:
        // record the turn, touch no run.
        draft.put_provider_turn(turn.clone());
        return Ok(owner_guard(&provider_thread));
    };
    let now = draft.now;
    draft.put_provider_turn(turn.clone());
    if turn.status == ProviderTurnStatus::Running {
        if attempt.status == AttemptStatus::Pending || attempt.provider_turn_id != Some(turn.id) {
            draft.put_attempt(RunAttempt {
                provider_turn_id: Some(turn.id),
                status: AttemptStatus::Running,
                started_at: attempt.started_at.or(turn.started_at).or(Some(now)),
                native_thread_id: attempt.native_thread_id.clone().or_else(|| {
                    provider_thread
                        .native_thread_ref
                        .as_ref()
                        .and_then(|reference| reference.native_id.clone())
                }),
                ..attempt.clone()
            });
        }
        if let Some(node) = run.root_node_id.and_then(|id| draft.node(id)).cloned()
            && (node.status == NodeStatus::Pending || node.provider_turn_id != Some(turn.id))
        {
            draft.put_node(ExecutionNode {
                status: NodeStatus::Running,
                provider_turn_id: Some(turn.id),
                started_at: node.started_at.or(Some(now)),
                ..node
            });
        }
        if run.status == RunStatus::Starting {
            draft.put_run(Run {
                status: RunStatus::Running,
                started_at: run.started_at.or(Some(now)),
                ..run.clone()
            });
        }
        if provider_thread.status != ProviderThreadStatus::Active {
            draft.put_provider_thread(ProviderThread {
                status: ProviderThreadStatus::Active,
                updated_at: now,
                ..provider_thread
            });
        }
    }
    Ok(run_guard(&run, &attempt))
}

fn subagent_updated(
    draft: &mut Draft<'_>,
    input: &IngestEnvelope,
    subagent: &Subagent,
) -> Result<WriteGuard, CommandError> {
    let subagent = Subagent {
        thread_id: input.thread_id,
        ..subagent.clone()
    };
    draft.put_subagent(subagent.clone());
    // The row in the transcript follows the subagent while its run is open.
    // A subagent that outlives its run (child_turns_continue) only updates
    // the entity: the run's rows are no longer hot.
    let Some((run, attempt)) =
        current(draft, input).filter(|(run, _)| subagent.run_id == Some(run.id))
    else {
        return Ok(WriteGuard::None);
    };
    let mut item = template_item(draft, &run, &attempt, derived::subagent_item(subagent.id));
    item.node_id = Some(subagent.id);
    item.status = match subagent.status {
        SubagentStatus::Completed => ItemStatus::Completed,
        SubagentStatus::Failed => ItemStatus::Failed,
        SubagentStatus::Cancelled => ItemStatus::Cancelled,
        SubagentStatus::Interrupted => ItemStatus::Interrupted,
        SubagentStatus::Waiting => ItemStatus::Waiting,
        SubagentStatus::Pending | SubagentStatus::Idle => ItemStatus::Pending,
        _ => ItemStatus::Running,
    };
    item.title = subagent.title.clone();
    item.kind = TurnItemKind::Subagent {
        subagent_id: subagent.id,
        origin: subagent.origin,
        driver: subagent.driver,
        child_thread_id: subagent.child_thread_id,
        prompt: subagent.prompt.clone(),
        progress: subagent.progress.clone(),
        result: subagent.result.clone(),
    };
    put_run_item(draft, &run, &attempt, item);
    Ok(run_guard(&run, &attempt))
}

fn runtime_request(
    draft: &mut Draft<'_>,
    input: &IngestEnvelope,
    request: &RuntimeRequest,
) -> Result<WriteGuard, CommandError> {
    let Some((run, attempt)) = current(draft, input) else {
        // A request outside an open run (or after it ended) cannot be shown
        // or answered.
        return stale();
    };
    let now = draft.now;
    let existing = draft.request(request.id).cloned();
    let node_id = existing
        .as_ref()
        .map(|existing| existing.node_id)
        .unwrap_or_else(|| derived::request_node(request.id));
    let request = RuntimeRequest {
        thread_id: input.thread_id,
        node_id,
        provider_turn_id: request.provider_turn_id.or(attempt.provider_turn_id),
        // An answer the user gave stays recorded until the driver says more.
        response: request
            .response
            .clone()
            .or_else(|| existing.and_then(|existing| existing.response)),
        ..request.clone()
    };
    let pending = request.status == RequestStatus::Pending;
    let user_input = request.kind == RuntimeRequestKind::UserInput;

    let root = run.root_node_id;
    let node = draft.node(node_id).cloned();
    draft.put_node(ExecutionNode {
        id: node_id,
        thread_id: input.thread_id,
        run_id: Some(run.id),
        parent_node_id: root,
        root_node_id: root.unwrap_or(node_id),
        kind: if user_input {
            NodeKind::UserInputRequest
        } else {
            NodeKind::ApprovalRequest
        },
        status: match request.status {
            RequestStatus::Pending => NodeStatus::Waiting,
            RequestStatus::Resolved => NodeStatus::Completed,
            _ => NodeStatus::Cancelled,
        },
        counts_for_run: false,
        provider_thread_id: Some(attempt.provider_thread_id),
        provider_turn_id: request.provider_turn_id,
        native_item_ref: None,
        runtime_request_id: Some(request.id),
        checkpoint_scope_id: None,
        started_at: node.as_ref().and_then(|node| node.started_at).or(Some(now)),
        completed_at: (!pending).then_some(now),
    });

    let item_id = derived::request_item(request.id);
    let mut item = draft
        .item(item_id)
        .cloned()
        .unwrap_or_else(|| template_item(draft, &run, &attempt, item_id));
    item.node_id = Some(node_id);
    item.status = match request.status {
        RequestStatus::Pending => ItemStatus::Waiting,
        RequestStatus::Resolved => ItemStatus::Completed,
        _ => ItemStatus::Cancelled,
    };
    item.kind = match item.kind {
        // Keep a prompt the driver put on its own row.
        TurnItemKind::ApprovalRequest {
            prompt, app_name, ..
        } if !user_input => TurnItemKind::ApprovalRequest {
            request_id: request.id,
            request_kind: request.kind,
            prompt,
            app_name,
            options: request.options.clone(),
        },
        _ if user_input => TurnItemKind::UserInputRequest {
            request_id: request.id,
            questions: request.questions.clone(),
            answers: match &request.response {
                Some(RuntimeResponse::UserInput { answers }) => Some(answers.clone()),
                _ => None,
            },
            response_mode: None,
        },
        _ => TurnItemKind::ApprovalRequest {
            request_id: request.id,
            request_kind: request.kind,
            prompt: None,
            app_name: None,
            options: request.options.clone(),
        },
    };
    put_run_item(draft, &run, &attempt, item);
    draft.put_request(request.clone());

    // The run waits while any of its requests is pending.
    let other_pending = draft
        .all_requests()
        .into_iter()
        .filter(|other| other.id != request.id && other.status == RequestStatus::Pending)
        .any(|other| {
            draft
                .node(other.node_id)
                .is_some_and(|node| node.run_id == Some(run.id))
        });
    let run_now = draft.run(run.id).cloned().unwrap_or(run.clone());
    if pending && run_now.status == RunStatus::Running {
        draft.put_run(Run {
            status: RunStatus::Waiting,
            ..run_now
        });
    } else if !pending && !other_pending && run_now.status == RunStatus::Waiting {
        draft.put_run(Run {
            status: RunStatus::Running,
            ..run_now
        });
    }
    Ok(run_guard(&run, &attempt))
}

fn turn_terminal(
    draft: &mut Draft<'_>,
    input: &IngestEnvelope,
    terminal: &TurnTerminal,
) -> Result<WriteGuard, CommandError> {
    let now = draft.now;
    let turn_status = match &terminal.outcome {
        TurnOutcome::Completed => ProviderTurnStatus::Completed,
        TurnOutcome::Interrupted => ProviderTurnStatus::Interrupted,
        TurnOutcome::Cancelled => ProviderTurnStatus::Cancelled,
        TurnOutcome::Failed { .. } => ProviderTurnStatus::Failed,
    };
    let reported_turn = terminal
        .provider_turn
        .as_ref()
        .and_then(|reference| draft.provider_turn(reference.provider_turn_id))
        .cloned();
    let settle_turn = |draft: &mut Draft<'_>, turn: Option<ProviderTurn>| {
        if let Some(turn) = turn
            && turn.status != turn_status
        {
            draft.put_provider_turn(ProviderTurn {
                status: turn_status,
                completed_at: turn.completed_at.or(Some(now)),
                ..turn
            });
        }
    };

    // A child turn (a subagent's provider thread) never completes the parent.
    if let Some(turn) = &reported_turn
        && let Some(provider_thread) = draft.provider_thread(turn.provider_thread_id).cloned()
        && provider_thread.owner_node_id.is_some()
    {
        settle_turn(draft, Some(turn.clone()));
        return Ok(owner_guard(&provider_thread));
    }

    let Some((run, attempt)) = current(draft, input).filter(|(run, attempt)| {
        terminal.run.run_id == run.id && terminal.run.attempt_id == attempt.id
    }) else {
        // A superseded attempt (steering restart) or a late terminal: record
        // the provider turn only.
        let guard = reported_turn
            .as_ref()
            .and_then(|turn| draft.provider_thread(turn.provider_thread_id))
            .map(owner_guard)
            .unwrap_or(WriteGuard::None);
        settle_turn(draft, reported_turn);
        return Ok(guard);
    };
    let guard = run_guard(&run, &attempt);
    // A terminal that names another provider turn than the attempt's own
    // (a child turn whose row has not arrived) never ends the run.
    if let (Some(reported), Some(own)) = (&terminal.provider_turn, attempt.provider_turn_id)
        && reported.provider_turn_id != own
    {
        settle_turn(draft, reported_turn);
        return Ok(guard);
    }
    if !matches!(
        run.status,
        RunStatus::Starting | RunStatus::Running | RunStatus::Waiting
    ) {
        // Already finalizing: exactly one terminal is honoured.
        return Ok(guard);
    }
    let turn = attempt
        .provider_turn_id
        .and_then(|id| draft.provider_turn(id))
        .cloned()
        .or(reported_turn);
    settle_turn(draft, turn);
    flush_streams(
        draft,
        run.id,
        !matches!(terminal.outcome, TurnOutcome::Completed),
    );

    if terminal.thread_disposition == ThreadDisposition::Broken
        && let Some(provider_thread) = draft.provider_thread(attempt.provider_thread_id).cloned()
    {
        if draft.thread.active_provider_thread_id == Some(provider_thread.id) {
            draft
                .touch_thread(ThreadChange::ActiveProviderThreadChanged)
                .active_provider_thread_id = None;
        }
        draft.put_provider_thread(ProviderThread {
            status: ProviderThreadStatus::Error,
            updated_at: now,
            ..provider_thread
        });
    }

    match &terminal.outcome {
        TurnOutcome::Completed => {
            close_child_nodes(
                draft,
                &run,
                NodeStatus::Completed,
                terminal.child_turns_continue,
            );
            draft.put_attempt(RunAttempt {
                status: AttemptStatus::Completed,
                completed_at: Some(now),
                ..attempt.clone()
            });
            let scope = run
                .root_node_id
                .and_then(|id| draft.node(id))
                .and_then(|node| node.checkpoint_scope_id);
            match scope {
                // The finalization barrier: the checkpoint capture completes
                // the run (`CheckpointCaptured`).
                Some(scope_id) => {
                    draft.put_run(Run {
                        status: RunStatus::Finalizing,
                        provider_completed_at: Some(now),
                        ..run.clone()
                    });
                    draft.effect(Effect::CheckpointCapture {
                        run_id: Some(run.id),
                        scope_id,
                    });
                }
                None => {
                    draft.put_run(Run {
                        provider_completed_at: Some(now),
                        ..run.clone()
                    });
                    finish_run(draft, run.id, RunStatus::Completed, None);
                }
            }
        }
        TurnOutcome::Interrupted | TurnOutcome::Cancelled => {
            close_child_nodes(
                draft,
                &run,
                NodeStatus::Interrupted,
                terminal.child_turns_continue,
            );
            let message = terminal
                .summary
                .clone()
                .unwrap_or_else(|| "Interrupted".to_owned());
            interrupted_row(draft, &run, message);
            draft.put_run(Run {
                provider_completed_at: Some(now),
                ..run.clone()
            });
            finish_run(draft, run.id, RunStatus::Interrupted, None);
        }
        TurnOutcome::Failed { failure } => {
            close_child_nodes(
                draft,
                &run,
                NodeStatus::Failed,
                terminal.child_turns_continue,
            );
            // An error row, limit recovery with the reset time for
            // `UsageLimit`, a held queue, and the run fails.
            provider_turn_failed(draft, run.id, attempt.id, failure);
        }
    }
    Ok(guard)
}

/// Streaming rows of a run stop streaming when its turn ends (the first step
/// of the finalization barrier).
pub(super) fn flush_streams(draft: &mut Draft<'_>, run_id: RunId, interrupted: bool) {
    let now = draft.now;
    let status = if interrupted {
        ItemStatus::Interrupted
    } else {
        ItemStatus::Completed
    };
    for mut item in draft.items_of_run(run_id) {
        let streaming = match &mut item.kind {
            TurnItemKind::AssistantMessage { streaming, .. }
            | TurnItemKind::Reasoning { streaming, .. }
            | TurnItemKind::ProposedPlan { streaming, .. } => std::mem::take(streaming),
            _ => false,
        };
        let open = !item.status.is_terminal()
            && matches!(
                item.kind,
                TurnItemKind::AssistantMessage { .. } | TurnItemKind::Reasoning { .. }
            );
        if streaming || open {
            if !item.status.is_terminal() {
                item.status = status;
                item.completed_at = Some(now);
            }
            item.updated_at = now;
            draft.put_item(item);
        }
    }
    for message in draft.messages_of_run(run_id) {
        if message.streaming {
            draft.put_message(Message {
                streaming: false,
                updated_at: now,
                ..message
            });
        }
    }
}

/// Open tool and message nodes under the run's root end with it. Subagent
/// nodes keep running when the provider says their turns continue.
pub(super) fn close_child_nodes(
    draft: &mut Draft<'_>,
    run: &Run,
    status: NodeStatus,
    children_continue: bool,
) {
    let now = draft.now;
    for node in draft.nodes_of_run(run.id) {
        let open = matches!(
            node.status,
            NodeStatus::Idle | NodeStatus::Pending | NodeStatus::Running | NodeStatus::Waiting
        );
        if !open || Some(node.id) == run.root_node_id {
            continue;
        }
        if node.kind == NodeKind::Subagent && children_continue {
            continue;
        }
        draft.put_node(ExecutionNode {
            status,
            completed_at: Some(now),
            ..node
        });
    }
}

/// A turn item of `run` with the common fields filled in; the caller sets
/// the status and kind. Its position is assigned by `put_run_item`.
fn template_item(draft: &Draft<'_>, run: &Run, attempt: &RunAttempt, id: TurnItemId) -> TurnItem {
    TurnItem {
        id,
        thread_id: draft.thread_id(),
        run_id: Some(run.id),
        node_id: run.root_node_id,
        provider_thread_id: Some(attempt.provider_thread_id),
        provider_turn_id: attempt.provider_turn_id,
        native_item_ref: None,
        parent_item_id: None,
        position: 0,
        status: ItemStatus::Running,
        title: None,
        tool: None,
        stream_rev: 0,
        started_at: Some(draft.now),
        completed_at: None,
        updated_at: draft.now,
        kind: TurnItemKind::Unknown,
    }
}

/// Upserts a run's item: a known item keeps its position and start time, a
/// new one takes the next position in the run's block (R15).
fn put_run_item(draft: &mut Draft<'_>, run: &Run, attempt: &RunAttempt, item: TurnItem) {
    let now = draft.now;
    let existing = draft.item(item.id).cloned();
    let (position, started_at) = match &existing {
        Some(existing) => (existing.position, existing.started_at),
        None => {
            let block = run.ordinal.unwrap_or_else(|| draft.latest_ordinal());
            (draft.alloc_position(block), item.started_at.or(Some(now)))
        }
    };
    let completed_at = if item.status.is_terminal() {
        item.completed_at
            .or(existing.as_ref().and_then(|existing| existing.completed_at))
            .or(Some(now))
    } else {
        None
    };
    draft.put_item(TurnItem {
        thread_id: draft.thread_id(),
        run_id: Some(run.id),
        node_id: item.node_id.or(run.root_node_id),
        provider_thread_id: item.provider_thread_id.or(Some(attempt.provider_thread_id)),
        provider_turn_id: item.provider_turn_id.or(attempt.provider_turn_id),
        position,
        started_at,
        completed_at,
        ..item
    });
}

/// Whether an event re-states what the projection already holds.
fn unchanged(projection: &ThreadProjection, event: &DomainEvent) -> bool {
    match event {
        DomainEvent::ThreadUpdated(update) => update.thread == projection.thread,
        DomainEvent::RunUpdated(run) => projection.runs.get(&run.id) == Some(run),
        DomainEvent::RunAttemptUpdated(attempt) => {
            projection.attempts.get(&attempt.id) == Some(attempt)
        }
        DomainEvent::NodeUpdated(node) => projection.nodes.get(&node.id) == Some(node),
        DomainEvent::SubagentUpdated(subagent) => {
            projection.subagents.get(&subagent.id) == Some(subagent)
        }
        DomainEvent::ProviderSessionUpdated(session) => {
            projection.provider_sessions.get(&session.id) == Some(session)
        }
        DomainEvent::ProviderThreadUpdated(provider_thread) => {
            projection.provider_threads.get(&provider_thread.id) == Some(provider_thread)
        }
        DomainEvent::ProviderTurnUpdated(turn) => {
            projection.provider_turns.get(&turn.id) == Some(turn)
        }
        DomainEvent::RuntimeRequestUpdated(request) => {
            projection.runtime_requests.get(&request.id) == Some(request)
        }
        DomainEvent::MessageUpdated(message) => {
            projection.hot_messages.get(&message.id) == Some(message)
        }
        DomainEvent::PlanUpdated(plan) => projection.plans.get(&plan.id) == Some(plan),
        DomainEvent::TurnItemUpdated(item) => {
            projection.hot_items.get(&item.position) == Some(item)
        }
        _ => false,
    }
}
