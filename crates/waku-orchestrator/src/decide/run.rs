//! Messages, runs and the queue: the run state machine.
//!
//! ```text
//! dispatch ─┬─ idle ───────────────► starting ──(driver)──► running ⇄ waiting
//!           ├─ busy ─► queued ──────┘                          │
//!           ├─ defer ─► preparing ──(workspace ready)──────────┘
//!           └─ steer ─► (same run; steer or restart effect)     ▼
//!                                         finalizing ──(checkpoint)──► completed
//! any active run ──interrupt / failure──► interrupted / failed ──► next queued run
//! ```
//!
//! Ids of everything a run creates are derived from the run id (UUIDv5), so a
//! retried decision allocates the same ids. Only new provider threads draw a
//! fresh id.

use waku_protocol::orchestration::*;

use super::policy::{SteeringPolicy, steering_policy};
use super::{CapabilitySource, Draft, truncate_chars};
use crate::command::CommandError;
use crate::effects::{Effect, EffectType, TitleKind};

/// The longest title taken from a prompt.
const PROMPT_TITLE_CHARS: usize = 80;

pub(super) fn decide_client(
    draft: &mut Draft<'_>,
    capabilities: &dyn CapabilitySource,
    envelope: &CommandEnvelope,
) -> Result<(), CommandError> {
    match &envelope.command {
        Command::MessageDispatch {
            message_id,
            text,
            display_text,
            attachments,
            context,
            model_selection,
            mode,
            title_seed,
            source_plan,
            ..
        } => {
            dispatch_message(
                draft,
                capabilities,
                envelope,
                DispatchInput {
                    message_id: *message_id,
                    text,
                    display_text: display_text.as_deref(),
                    attachments,
                    context,
                    model_selection: model_selection.as_ref(),
                    mode,
                    title_seed: title_seed.as_deref(),
                    source_plan: source_plan.as_ref(),
                },
            )?;
            super::thread::reengage(draft);
            Ok(())
        }
        Command::RunInterrupt {
            run_id,
            reason,
            hold_queue,
            ..
        } => interrupt(draft, *run_id, reason.as_deref(), *hold_queue),
        Command::PreparedRunRetry { run_id, .. } => retry_preparation(draft, *run_id),
        Command::QueueResume { .. } => {
            let held = draft
                .queued_runs()
                .into_iter()
                .filter(|run| run.queue_held)
                .cloned()
                .collect::<Vec<_>>();
            for mut run in held {
                run.queue_held = false;
                draft.put_run(run);
            }
            advance_queue(draft);
            Ok(())
        }
        Command::QueuedRunCancel { run_id, .. } => cancel_queued(draft, *run_id),
        _ => unreachable!("routed by decide_client"),
    }
}

struct DispatchInput<'c> {
    message_id: MessageId,
    text: &'c str,
    display_text: Option<&'c str>,
    attachments: &'c [Attachment],
    context: &'c [ContextRecord],
    model_selection: Option<&'c ModelSelection>,
    mode: &'c DispatchMode,
    title_seed: Option<&'c str>,
    source_plan: Option<&'c PlanRef>,
}

enum Delivery {
    Start,
    Queue,
    Defer(WorkspaceStrategy),
    Steer(RunId),
    Restart(RunId),
}

/// A live provider turn a steer or interrupt can address.
struct LiveTurn {
    session: ProviderSessionId,
    provider_thread: ProviderThreadId,
    turn: ProviderTurnId,
    attempt: RunAttemptId,
}

fn live_turn(draft: &Draft<'_>, run: &Run) -> Option<LiveTurn> {
    let attempt = draft.attempt(run.active_attempt_id?)?;
    let turn = attempt.provider_turn_id?;
    let provider_thread = draft.provider_thread(attempt.provider_thread_id)?;
    let session = provider_thread.provider_session_id?;
    Some(LiveTurn {
        session,
        provider_thread: provider_thread.id,
        turn,
        attempt: attempt.id,
    })
}

/// Capabilities of the live session for `instance`, else the static ones.
fn capabilities_for(
    draft: &Draft<'_>,
    capabilities: &dyn CapabilitySource,
    instance: &ProviderInstanceId,
    session: Option<ProviderSessionId>,
) -> Capabilities {
    session
        .and_then(|session| draft.provider_session(session))
        .filter(|session| &session.provider_instance_id == instance)
        .map(|session| session.capabilities.clone())
        .or_else(|| capabilities.for_instance(instance))
        .unwrap_or_default()
}

fn dispatch_message(
    draft: &mut Draft<'_>,
    capabilities: &dyn CapabilitySource,
    envelope: &CommandEnvelope,
    input: DispatchInput<'_>,
) -> Result<(), CommandError> {
    if draft.thread.deleted_at.is_some() {
        return Err(CommandError::invalid_state("thread is deleted"));
    }
    if draft.thread.archived_at.is_some() {
        return Err(CommandError::invalid_state("thread is archived"));
    }
    if input.text.trim().is_empty() && input.attachments.is_empty() {
        return Err(CommandError::invalid("text", "message is empty"));
    }
    if draft.message_id_used(input.message_id) {
        return Err(CommandError::conflict(format!(
            "message {} already exists",
            input.message_id
        )));
    }
    let selection = input
        .model_selection
        .cloned()
        .unwrap_or_else(|| draft.thread.model_selection.clone());
    let active = draft.active_run().cloned();
    let queue_waiting = draft.queue_head().is_some();

    let steerable = |draft: &Draft<'_>, run: &Run| -> Option<Delivery> {
        let live = live_turn(draft, run)?;
        let caps = capabilities_for(
            draft,
            capabilities,
            &run.model_selection.instance,
            Some(live.session),
        );
        match steering_policy(&caps) {
            SteeringPolicy::Native => Some(Delivery::Steer(run.id)),
            SteeringPolicy::InterruptRestart => Some(Delivery::Restart(run.id)),
            SteeringPolicy::Queue => None,
        }
    };
    let target_active = |target: RunId| -> Result<Run, CommandError> {
        match &active {
            Some(run) if run.id == target => Ok(run.clone()),
            _ => Err(CommandError::invalid_state(format!(
                "run {target} is not the active run"
            ))),
        }
    };

    let delivery = match input.mode {
        DispatchMode::Auto => match &active {
            None if !queue_waiting => Delivery::Start,
            None => Delivery::Queue,
            Some(run) if run.status == RunStatus::Running => steerable(draft, run)
                .filter(|delivery| matches!(delivery, Delivery::Steer(_)))
                .unwrap_or(Delivery::Queue),
            Some(_) => Delivery::Queue,
        },
        DispatchMode::StartImmediately => {
            if let Some(run) = &active {
                return Err(CommandError::invalid_state(format!(
                    "run {} is active; queue or steer instead",
                    run.id
                )));
            }
            Delivery::Start
        }
        DispatchMode::QueueAfterActive => {
            if active.is_none() && !queue_waiting {
                Delivery::Start
            } else {
                Delivery::Queue
            }
        }
        DispatchMode::SteerActive { target_run_id } => {
            let run = target_active(*target_run_id)?;
            // A turn the provider has not accepted yet cannot be steered;
            // the message waits for it instead.
            steerable(draft, &run).unwrap_or(Delivery::Queue)
        }
        DispatchMode::RestartActive { target_run_id } => {
            let run = target_active(*target_run_id)?;
            if live_turn(draft, &run).is_none() {
                return Err(CommandError::invalid_state(
                    "the active run has no provider turn to restart yet",
                ));
            }
            Delivery::Restart(run.id)
        }
        DispatchMode::DeferStart { workspace } => {
            if let Some(run) = &active {
                return Err(CommandError::invalid_state(format!(
                    "run {} is active; a deferred start needs an idle thread",
                    run.id
                )));
            }
            if matches!(workspace, WorkspaceStrategy::Unknown) {
                return Err(CommandError::invalid("mode.workspace", "unknown strategy"));
            }
            Delivery::Defer(workspace.clone())
        }
    };

    let first_message = !draft.has_runs();
    let run_id = RunId(draft.derived("run", input.message_id));
    let message_run = match delivery {
        Delivery::Steer(target) | Delivery::Restart(target) => target,
        _ => run_id,
    };
    draft.put_message(Message {
        id: input.message_id,
        thread_id: draft.thread_id(),
        run_id: Some(message_run),
        node_id: None,
        role: MessageRole::User,
        created_by: envelope.issued_by,
        creation_source: envelope.source,
        text: input.text.to_owned(),
        display_text: input.display_text.map(str::to_owned),
        attachments: input.attachments.to_vec(),
        context: input.context.to_vec(),
        notification: None,
        sender_thread_id: None,
        streaming: false,
        created_at: draft.now,
        updated_at: draft.now,
    });

    let new_run = |draft: &Draft<'_>, status: RunStatus| Run {
        id: run_id,
        thread_id: draft.thread_id(),
        ordinal: None,
        model_selection: selection.clone(),
        provider_thread_id: None,
        user_message_id: input.message_id,
        root_node_id: None,
        active_attempt_id: None,
        status,
        queue_position: None,
        queue_held: false,
        requested_at: draft.now,
        started_at: None,
        provider_completed_at: None,
        completed_at: None,
        failure: None,
        checkpoint_id: None,
        context_handoff_id: None,
        continuation: None,
        workspace_preparation: None,
        restart_cancelled_background_work: Vec::new(),
        source_plan: input.source_plan.cloned(),
        delegated_task_id: None,
        delegated_completion: None,
    };

    match delivery {
        Delivery::Start => {
            let run = new_run(draft, RunStatus::Queued);
            draft.put_run(run);
            start_run(draft, run_id, InputIntent::TurnStart);
        }
        Delivery::Queue => {
            let position = draft
                .max_queue_position()
                .map_or(1, |position| position + 1);
            let mut run = new_run(draft, RunStatus::Queued);
            run.queue_position = Some(position);
            draft.put_run(run);
        }
        Delivery::Defer(workspace) => {
            let mut run = new_run(draft, RunStatus::Preparing);
            run.workspace_preparation = Some(workspace.clone());
            draft.put_run(run);
            draft.effect(Effect::WorkspacePrepare {
                run_id,
                strategy: workspace,
            });
        }
        Delivery::Steer(target) => {
            let run = draft.run(target).cloned().expect("active run");
            let live = live_turn(draft, &run).expect("steerable run has a live turn");
            steer_item(draft, &run, input.message_id, InputIntent::Steer);
            draft.effect(Effect::ProviderTurnSteer {
                provider_session_id: live.session,
                provider_thread_id: live.provider_thread,
                provider_turn_id: live.turn,
                message_id: input.message_id,
            });
        }
        Delivery::Restart(target) => {
            let run = draft.run(target).cloned().expect("active run");
            let live = live_turn(draft, &run).expect("restartable run has a live turn");
            steer_item(draft, &run, input.message_id, InputIntent::Steer);
            let previous = draft.attempt(live.attempt).cloned().expect("live attempt");
            let next_ordinal = previous.attempt_ordinal + 1;
            let next_id =
                RunAttemptId(draft.derived("run_attempt", format!("{}:{next_ordinal}", run.id)));
            draft.put_attempt(RunAttempt {
                status: AttemptStatus::Superseded,
                completed_at: Some(draft.now),
                ..previous.clone()
            });
            draft.put_attempt(RunAttempt {
                id: next_id,
                attempt_ordinal: next_ordinal,
                provider_turn_id: None,
                reason: AttemptReason::SteeringRestart,
                status: AttemptStatus::Pending,
                started_at: None,
                completed_at: None,
                ..previous
            });
            draft.put_run(Run {
                active_attempt_id: Some(next_id),
                ..run.clone()
            });
            draft.effect(Effect::ProviderTurnRestart {
                run_id: run.id,
                interrupted_attempt_id: live.attempt,
                next_attempt_id: next_id,
                session_transition: None,
            });
        }
    }

    let now = draft.now;
    let title = (first_message && draft.thread.title_source == TitleSource::Placeholder)
        .then(|| prompt_title(input.title_seed, input.display_text, input.text))
        .flatten();
    let thread = draft.touch_thread(ThreadChange::Activity);
    thread.updated_at = now;
    thread.last_reply_at = Some(now);
    if let Some(title) = title {
        thread.title = title;
        thread.title_source = TitleSource::Prompt;
        draft.effect(Effect::ThreadTitleGenerate {
            request_id: envelope.command_id.clone(),
            kind: TitleKind::Initial {
                message_id: input.message_id,
            },
        });
    }
    Ok(())
}

fn prompt_title(seed: Option<&str>, display: Option<&str>, text: &str) -> Option<String> {
    let source = seed.or(display).unwrap_or(text);
    let line = source
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())?;
    Some(truncate_chars(line, PROMPT_TITLE_CHARS))
}

/// The user-message row for a steer, in the target run's block.
fn steer_item(draft: &mut Draft<'_>, run: &Run, message_id: MessageId, intent: InputIntent) {
    let Some(message) = draft.message(message_id).cloned() else {
        return;
    };
    let block = run.ordinal.unwrap_or_else(|| draft.latest_ordinal());
    let id = TurnItemId(draft.derived("user_item", message_id));
    let item = draft.new_item(
        id,
        Some(run.id),
        block,
        ItemStatus::Completed,
        user_message_kind(&message, intent),
    );
    draft.put_message(Message {
        node_id: run.root_node_id,
        ..message
    });
    draft.put_item(item);
}

fn user_message_kind(message: &Message, intent: InputIntent) -> TurnItemKind {
    TurnItemKind::UserMessage {
        message_id: message.id,
        input_intent: intent,
        text: message.text.clone(),
        display_text: message.display_text.clone(),
        attachments: message.attachments.clone(),
        created_by: message.created_by,
        sender_thread_id: message.sender_thread_id,
    }
}

/// Moves a queued or prepared run to `starting`: takes the next ordinal,
/// binds a provider thread, creates the attempt, root node, root checkpoint
/// scope and the user-message row, and enqueues `provider_turn.start`.
pub(super) fn start_run(draft: &mut Draft<'_>, run_id: RunId, intent: InputIntent) {
    // A run takes an ordinal once (R14); its rows' positions depend on it.
    let Some(run) = draft
        .run(run_id)
        .cloned()
        .filter(|run| run.ordinal.is_none())
    else {
        return;
    };
    let now = draft.now;
    let ordinal = draft.latest_ordinal() + 1;
    let instance = run.model_selection.instance.clone();

    // Reuse the active provider thread of the same instance, else open one.
    let reusable = draft
        .thread
        .active_provider_thread_id
        .and_then(|id| draft.provider_thread(id))
        .filter(|provider_thread| provider_thread.provider_instance_id == instance)
        .cloned();
    let provider_thread = match reusable {
        Some(existing) => ProviderThread {
            first_run_ordinal: existing.first_run_ordinal.or(Some(ordinal)),
            last_run_ordinal: Some(ordinal),
            updated_at: now,
            ..existing
        },
        None => {
            let id = ProviderThreadId(draft.new_id());
            draft
                .touch_thread(ThreadChange::ActiveProviderThreadChanged)
                .active_provider_thread_id = Some(id);
            ProviderThread {
                id,
                driver: DriverKind::from_instance(&instance),
                provider_instance_id: instance.clone(),
                provider_session_id: None,
                app_thread_id: Some(draft.thread_id()),
                owner_node_id: None,
                native_thread_ref: None,
                native_conversation_head_ref: None,
                status: ProviderThreadStatus::NotLoaded,
                first_run_ordinal: Some(ordinal),
                last_run_ordinal: Some(ordinal),
                handoff_ids: Vec::new(),
                forked_from: None,
                pending_background_tasks: Vec::new(),
                context_usage: None,
                goal: None,
                native_metadata: None,
                created_at: now,
                updated_at: now,
            }
        }
    };

    let root_node_id = NodeId(draft.derived("root_node", run.id));
    let attempt_id = RunAttemptId(draft.derived("run_attempt", format!("{}:1", run.id)));
    let scope_id = CheckpointScopeId(draft.derived("root_scope", run.id));
    let cwd = draft
        .thread
        .worktree_path
        .clone()
        .or_else(|| draft.project_path.clone())
        .unwrap_or_default();

    draft.put_scope(CheckpointScope {
        id: scope_id,
        thread_id: draft.thread_id(),
        run_id: Some(run.id),
        node_id: root_node_id,
        parent_scope_id: None,
        provider_thread_id: Some(provider_thread.id),
        kind: ScopeKind::RootRun,
        ordinal_within_parent: ordinal,
        advances_app_run_count: true,
        cwd,
        created_at: now,
    });
    draft.put_node(ExecutionNode {
        id: root_node_id,
        thread_id: draft.thread_id(),
        run_id: Some(run.id),
        parent_node_id: None,
        root_node_id,
        kind: NodeKind::RootTurn,
        status: NodeStatus::Pending,
        counts_for_run: true,
        provider_thread_id: Some(provider_thread.id),
        provider_turn_id: None,
        native_item_ref: None,
        runtime_request_id: None,
        checkpoint_scope_id: Some(scope_id),
        started_at: None,
        completed_at: None,
    });
    draft.put_attempt(RunAttempt {
        id: attempt_id,
        run_id: run.id,
        attempt_ordinal: 1,
        root_node_id,
        provider_instance_id: instance,
        provider_thread_id: provider_thread.id,
        native_thread_id: provider_thread
            .native_thread_ref
            .as_ref()
            .and_then(|reference| reference.native_id.clone()),
        provider_turn_id: None,
        reason: AttemptReason::Initial,
        status: AttemptStatus::Pending,
        started_at: None,
        completed_at: None,
    });
    let provider_thread_id = provider_thread.id;
    draft.put_provider_thread(provider_thread);
    draft.put_run(Run {
        ordinal: Some(ordinal),
        status: RunStatus::Starting,
        queue_position: None,
        queue_held: false,
        root_node_id: Some(root_node_id),
        active_attempt_id: Some(attempt_id),
        provider_thread_id: Some(provider_thread_id),
        ..run.clone()
    });
    if let Some(message) = draft.message(run.user_message_id).cloned() {
        let item_id = TurnItemId(draft.derived("user_item", message.id));
        let item = draft.new_item(
            item_id,
            Some(run.id),
            ordinal,
            ItemStatus::Completed,
            user_message_kind(&message, intent),
        );
        draft.put_message(Message {
            node_id: Some(root_node_id),
            updated_at: now,
            ..message
        });
        draft.put_item(item);
    }
    draft.thread.updated_at = now;
    draft.touch_thread(ThreadChange::Activity);
    // The turn-start checkpoint, ahead of the provider turn in the thread's
    // lane: what the run's diff starts from, and what rewinding its message
    // restores (for the first run, the state before anything ran).
    draft.effect(Effect::CheckpointCapture {
        run_id: Some(run.id),
        scope_id,
        turn_start: true,
    });
    draft.effect(Effect::ProviderTurnStart {
        run_id: run.id,
        attempt_id,
    });
}

/// Starts the head of the queue if the thread is free and the queue is not
/// held.
pub(super) fn advance_queue(draft: &mut Draft<'_>) {
    if draft.thread.deleted_at.is_some()
        || draft.thread.archived_at.is_some()
        || draft.active_run().is_some()
    {
        return;
    }
    // Holds and resumes always apply to the whole queue and new runs join at
    // its end, so a held queue always has a held head.
    if let Some(head) = draft
        .queue_head()
        .filter(|run| !run.queue_held)
        .map(|run| run.id)
    {
        start_run(draft, head, InputIntent::QueuedTurn);
    }
}

/// Holds every queued run so nothing starts without the user.
pub(super) fn hold_queue(draft: &mut Draft<'_>) {
    let queued = draft
        .queued_runs()
        .into_iter()
        .filter(|run| !run.queue_held)
        .cloned()
        .collect::<Vec<_>>();
    for mut run in queued {
        run.queue_held = true;
        draft.put_run(run);
    }
}

fn node_status_for(status: RunStatus) -> NodeStatus {
    match status {
        RunStatus::Completed => NodeStatus::Completed,
        RunStatus::Failed => NodeStatus::Failed,
        RunStatus::Cancelled => NodeStatus::Cancelled,
        RunStatus::RolledBack => NodeStatus::RolledBack,
        _ => NodeStatus::Interrupted,
    }
}

fn attempt_status_for(status: RunStatus) -> AttemptStatus {
    match status {
        RunStatus::Completed => AttemptStatus::Completed,
        RunStatus::Failed => AttemptStatus::Failed,
        RunStatus::Cancelled => AttemptStatus::Cancelled,
        _ => AttemptStatus::Interrupted,
    }
}

/// Ends a run: the run, its active attempt and root node become terminal,
/// its pending runtime requests are cancelled, and the queue advances.
pub(super) fn finish_run(
    draft: &mut Draft<'_>,
    run_id: RunId,
    status: RunStatus,
    failure: Option<ProviderFailure>,
) {
    let Some(run) = draft.run(run_id).cloned() else {
        return;
    };
    let now = draft.now;
    if let Some(attempt) = run
        .active_attempt_id
        .and_then(|id| draft.attempt(id))
        .cloned()
        && !attempt.status.is_terminal()
    {
        draft.put_attempt(RunAttempt {
            status: attempt_status_for(status),
            completed_at: Some(now),
            ..attempt
        });
    }
    if let Some(node) = run.root_node_id.and_then(|id| draft.node(id)).cloned() {
        draft.put_node(ExecutionNode {
            status: node_status_for(status),
            completed_at: Some(now),
            ..node
        });
    }
    let pending = draft
        .all_requests()
        .into_iter()
        .filter(|request| request.status == RequestStatus::Pending)
        .filter(|request| {
            draft
                .node(request.node_id)
                .is_some_and(|node| node.run_id == Some(run_id))
        })
        .cloned()
        .collect::<Vec<_>>();
    for request in pending {
        draft.put_request(RuntimeRequest {
            status: RequestStatus::Cancelled,
            resolved_at: Some(now),
            ..request
        });
    }
    draft.put_run(Run {
        status,
        queue_position: None,
        completed_at: Some(now),
        failure: failure.or(run.failure.clone()),
        ..run
    });
    draft.touch_thread(ThreadChange::Activity).updated_at = now;
    advance_queue(draft);
}

fn interrupt(
    draft: &mut Draft<'_>,
    run_id: RunId,
    reason: Option<&str>,
    hold: bool,
) -> Result<(), CommandError> {
    let run = draft
        .run(run_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("run", run_id))?;
    if hold {
        hold_queue(draft);
    }
    interrupt_run(draft, &run, reason)
}

/// Interrupts an active run: through the provider when a turn is live,
/// otherwise directly.
pub(super) fn interrupt_run(
    draft: &mut Draft<'_>,
    run: &Run,
    reason: Option<&str>,
) -> Result<(), CommandError> {
    let message = reason.unwrap_or("Interrupted by the user").to_owned();
    match run.status {
        RunStatus::Queued => Err(CommandError::invalid_state(
            "run is queued; cancel it with queued_run.cancel",
        )),
        RunStatus::Finalizing => Err(CommandError::invalid_state("run is finalizing")),
        status if status.is_terminal() => Err(CommandError::invalid_state("run is not active")),
        RunStatus::Preparing => {
            draft.cancel(&[EffectType::WorkspacePrepare], "interrupted");
            interrupted_row(draft, run, message);
            finish_run(draft, run.id, RunStatus::Interrupted, None);
            Ok(())
        }
        _ => match live_turn(draft, run) {
            Some(live) => {
                let id = TurnItemId(draft.new_id());
                let block = run.ordinal.unwrap_or_else(|| draft.latest_ordinal());
                let item = draft.new_item(
                    id,
                    Some(run.id),
                    block,
                    ItemStatus::Completed,
                    TurnItemKind::RunInterruptRequest { message },
                );
                draft.put_item(item);
                draft.effect(Effect::ProviderTurnInterrupt {
                    provider_session_id: live.session,
                    provider_thread_id: live.provider_thread,
                    provider_turn_id: live.turn,
                    subagent: None,
                });
                Ok(())
            }
            None => {
                // The provider never accepted the turn: drop the pending start.
                draft.cancel(
                    &[
                        EffectType::ProviderTurnStart,
                        EffectType::ProviderTurnRestart,
                    ],
                    "interrupted",
                );
                interrupted_row(draft, run, message);
                finish_run(draft, run.id, RunStatus::Interrupted, None);
                Ok(())
            }
        },
    }
}

pub(super) fn interrupted_row(draft: &mut Draft<'_>, run: &Run, message: String) {
    // A fresh id: a retried preparation can be interrupted again, and an
    // item's position is assigned once (R15).
    let id = TurnItemId(draft.new_id());
    let block = run.ordinal.unwrap_or_else(|| draft.latest_ordinal());
    let item = draft.new_item(
        id,
        Some(run.id),
        block,
        ItemStatus::Completed,
        TurnItemKind::RunInterruptResult { message },
    );
    draft.put_item(item);
}

fn cancel_queued(draft: &mut Draft<'_>, run_id: RunId) -> Result<(), CommandError> {
    let run = draft
        .run(run_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("run", run_id))?;
    if run.status != RunStatus::Queued {
        return Err(CommandError::invalid_state("run is not queued"));
    }
    let blobs = draft
        .message(run.user_message_id)
        .map(|message| {
            message
                .attachments
                .iter()
                .filter_map(|attachment| attachment.blob_reference.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !blobs.is_empty() {
        draft.effect(Effect::AttachmentCleanup {
            attachment_ids: blobs,
        });
    }
    let now = draft.now;
    draft.put_run(Run {
        status: RunStatus::Cancelled,
        queue_position: None,
        completed_at: Some(now),
        ..run
    });
    draft.touch_thread(ThreadChange::Activity).updated_at = now;
    advance_queue(draft);
    Ok(())
}

fn retry_preparation(draft: &mut Draft<'_>, run_id: RunId) -> Result<(), CommandError> {
    let run = draft
        .run(run_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("run", run_id))?;
    let Some(strategy) = run.workspace_preparation.clone() else {
        return Err(CommandError::invalid_state(
            "run has no workspace preparation to retry",
        ));
    };
    // Only a run that failed before it started: a run that already took an
    // ordinal failed in the provider, and its rows keep their positions.
    if run.status != RunStatus::Failed || run.ordinal.is_some() {
        return Err(CommandError::invalid_state(
            "run has no failed workspace preparation to retry",
        ));
    }
    if draft.thread.archived_at.is_some() || draft.thread.deleted_at.is_some() {
        return Err(CommandError::invalid_state("thread is archived or deleted"));
    }
    if let Some(active) = draft.active_run() {
        return Err(CommandError::invalid_state(format!(
            "run {} is active on this thread",
            active.id
        )));
    }
    // Messages held when this preparation failed follow the run again.
    let held = draft
        .queued_runs()
        .into_iter()
        .filter(|queued| queued.queue_held)
        .cloned()
        .collect::<Vec<_>>();
    for mut queued in held {
        queued.queue_held = false;
        draft.put_run(queued);
    }
    let now = draft.now;
    draft.put_run(Run {
        status: RunStatus::Preparing,
        completed_at: None,
        failure: None,
        ..run
    });
    draft.touch_thread(ThreadChange::Activity).updated_at = now;
    draft.effect(Effect::WorkspacePrepare { run_id, strategy });
    Ok(())
}

/// `prepared_run.release`: the workspace is ready, start the run.
pub(super) fn release_prepared(
    draft: &mut Draft<'_>,
    run_id: RunId,
    workspace: &crate::command::PreparedWorkspace,
) -> Result<(), CommandError> {
    let run = draft
        .run(run_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("run", run_id))?;
    if run.status != RunStatus::Preparing {
        return Err(CommandError::invalid_state(
            "run is not awaiting workspace preparation",
        ));
    }
    let thread = draft.touch_thread(ThreadChange::WorkspaceChanged);
    thread.branch = workspace.branch.clone().or(thread.branch.take());
    if workspace.worktree_path.is_some() {
        thread.worktree_path = workspace.worktree_path.clone();
        thread.worktree_owned = workspace.owned;
    }
    start_run(draft, run_id, InputIntent::TurnStart);
    Ok(())
}

/// `prepared_run.fail`: the run fails and the queue waits for the user.
pub(super) fn fail_prepared(
    draft: &mut Draft<'_>,
    run_id: RunId,
    failure: &ProviderFailure,
) -> Result<(), CommandError> {
    let run = draft
        .run(run_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("run", run_id))?;
    if run.status != RunStatus::Preparing {
        return Err(CommandError::invalid_state(
            "run is not awaiting workspace preparation",
        ));
    }
    error_row(draft, &run, failure.clone());
    hold_queue(draft);
    finish_run(draft, run_id, RunStatus::Failed, Some(failure.clone()));
    Ok(())
}

/// An `error` row for a run.
pub(super) fn error_row(draft: &mut Draft<'_>, run: &Run, failure: ProviderFailure) {
    // A fresh id: a retried preparation can fail again (R15: positions are
    // assigned once, so a row is never moved).
    let id = TurnItemId(draft.new_id());
    let block = run.ordinal.unwrap_or_else(|| draft.latest_ordinal());
    let item = draft.new_item(
        id,
        Some(run.id),
        block,
        ItemStatus::Failed,
        TurnItemKind::Error {
            failure,
            retry: None,
        },
    );
    draft.put_item(item);
}

/// `provider_turn.failed`: fails the run if the attempt is still current.
pub(super) fn provider_turn_failed(
    draft: &mut Draft<'_>,
    run_id: RunId,
    attempt_id: RunAttemptId,
    failure: &ProviderFailure,
) {
    let Some(run) = draft.run(run_id).cloned() else {
        return;
    };
    if run.status.is_terminal() || run.active_attempt_id != Some(attempt_id) {
        return;
    }
    error_row(draft, &run, failure.clone());
    if failure.class == FailureClass::UsageLimit {
        draft
            .touch_thread(ThreadChange::LimitRecoveryChanged)
            .limit_recovery = Some(LimitRecovery {
            run_id,
            reset_at: failure.reset_at,
            auto_resume: false,
            snooze: false,
        });
    }
    // The same provider would fail the next message too; validation errors
    // are specific to this run.
    if failure.class != FailureClass::ValidationError {
        hold_queue(draft);
    }
    finish_run(draft, run_id, RunStatus::Failed, Some(failure.clone()));
}
