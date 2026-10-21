//! Restart recovery (#59, RFC 0001 "Recovery"): `RecoverThread` and
//! `ContinueRun`.
//!
//! ```text
//! daemon start ─► reconcile effects ─► RecoverThread per candidate thread
//!   ├─ sessions of the dead process stop, their live requests expire,
//!   │  their background work and running subagents are cancelled
//!   ├─ the in-flight run ends `interrupted` (process_loss) with a
//!   │  `restart_interrupted` notice
//!   │    └─ continuation allowed ─► provider_runtime.continue
//!   │          └─ ContinueRun ─► a new run "Continue where you left off."
//!   │                            with a `restart_resumed` notice
//!   ├─ a run the provider never accepted retries its attempt
//!   │  (`provider_recovery`), under the same rules
//!   └─ an idle thread with a queue starts its head (unless held)
//! ```
//!
//! Continuation is refused when the setting is off, when the thread is
//! archived, deleted, settled or snoozed, when the run waited on the user,
//! when newer user work is queued (the queue is released instead), for a
//! `/compact` turn, and once a chain has `max_restart_continuations`
//! consecutive restart continuations.

use waku_protocol::orchestration::*;

use super::ingest::{close_child_nodes, flush_streams};
use super::run::{advance_queue, finish_run, start_run};
use super::{Draft, OrchestratorSettings};
use crate::command::CommandError;
use crate::effects::Effect;

/// `ProviderFailure.code` and `ProviderSession.detach_reason` after a restart.
pub const PROCESS_LOSS: &str = "process_loss";
/// The message a restart continuation sends (T3's wording).
pub const CONTINUE_TEXT: &str = "Continue where you left off.";
/// The interrupted run's notice.
pub const INTERRUPTED_NOTICE: &str = "Waku restarted while this turn was running.";
/// The continuation run's notice.
pub const RESUMED_NOTICE: &str = "Resumed after Waku restarted.";

/// Why a run interrupted by a restart is not continued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// "Continue threads after restarts" is off.
    Disabled,
    Deleted,
    Archived,
    Settled,
    Snoozed,
    /// The run was waiting on the user (an approval or a question).
    WaitingOnUser,
    /// Newer user work is queued; the queue runs instead.
    NewerWork,
    /// A `/compact` turn is not repeated.
    Compact,
    /// The chain already has `max_restart_continuations` continuations.
    ChainLimit,
}

impl Refusal {
    /// The reason as the notice states it.
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Disabled => "continuing threads after restarts is turned off",
            Self::Deleted => "the thread is deleted",
            Self::Archived => "the thread is archived",
            Self::Settled => "the thread is settled",
            Self::Snoozed => "the thread is snoozed",
            Self::WaitingOnUser => "it was waiting for your answer",
            Self::NewerWork => "newer messages were queued",
            Self::Compact => "it was a compaction",
            Self::ChainLimit => "it was already continued after several restarts",
        }
    }
}

/// Refusals that depend on the thread, not on the run.
fn thread_refusal(draft: &Draft<'_>, settings: &OrchestratorSettings) -> Option<Refusal> {
    let thread = &draft.thread;
    if !settings.continue_after_restart {
        Some(Refusal::Disabled)
    } else if thread.deleted_at.is_some() {
        Some(Refusal::Deleted)
    } else if thread.archived_at.is_some() {
        Some(Refusal::Archived)
    } else if thread.settled_at.is_some() {
        Some(Refusal::Settled)
    } else if thread.snoozed_until.is_some_and(|until| until > draft.now) {
        Some(Refusal::Snoozed)
    } else {
        None
    }
}

/// The number of consecutive restart continuations ending at `run`.
pub(crate) fn restart_chain(draft: &Draft<'_>, run: &Run) -> u8 {
    let mut length = 0u8;
    let mut current = run.continuation.clone();
    while let Some(continuation) = current.filter(|c| c.kind == ContinuationKind::Restart) {
        if length == u8::MAX {
            break;
        }
        length += 1;
        current = draft
            .run(continuation.of_run_id)
            .and_then(|run| run.continuation.clone());
    }
    length
}

fn is_compact(draft: &Draft<'_>, run: &Run) -> bool {
    draft.message(run.user_message_id).is_some_and(|message| {
        let text = message.text.trim_start();
        text == "/compact" || text.starts_with("/compact ")
    })
}

/// `RecoverThread`: the startup half of recovery for one thread.
pub(super) fn recover_thread(
    draft: &mut Draft<'_>,
    settings: &OrchestratorSettings,
) -> Result<(), CommandError> {
    let cancelled = stop_dead_sessions(draft);
    let Some(run) = draft.active_run().cloned() else {
        // An idle thread whose queue was waiting: release it.
        advance_queue(draft);
        return Ok(());
    };
    match run.status {
        // `checkpoint.capture` and `workspace.prepare` are replay-safe and
        // were requeued by the effect reconciliation.
        RunStatus::Finalizing | RunStatus::Preparing => Ok(()),
        RunStatus::Waiting if pending_request_survives(draft, &run) => Ok(()),
        RunStatus::Starting | RunStatus::Running | RunStatus::Waiting => {
            let refusal = thread_refusal(draft, settings)
                .or_else(|| (run.status == RunStatus::Waiting).then_some(Refusal::WaitingOnUser))
                .or_else(|| draft.queue_head().is_some().then_some(Refusal::NewerWork))
                .or_else(|| is_compact(draft, &run).then_some(Refusal::Compact));
            if never_reached_provider(draft, &run) {
                retry_attempt(draft, settings, &run, refusal, cancelled);
                return Ok(());
            }
            let refusal = refusal.or_else(|| {
                (restart_chain(draft, &run) >= settings.max_restart_continuations)
                    .then_some(Refusal::ChainLimit)
            });
            interrupt_for_restart(draft, &run, refusal, cancelled);
            if refusal.is_none() {
                draft.effect(Effect::ProviderRuntimeContinue {
                    source_run_id: run.id,
                    chain_length: restart_chain(draft, &run) + 1,
                });
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Every session of the previous process stops (`process_loss`). Requests
/// it could answer expire unless its capabilities say they survive a
/// restart; its provider threads lose the session and their background
/// work; open provider turns and subagents are interrupted. Returns the
/// background work that was cancelled.
fn stop_dead_sessions(draft: &mut Draft<'_>) -> Vec<CancelledBackgroundWork> {
    let now = draft.now;
    let mut stopped = Vec::new();
    let sessions = draft
        .base
        .provider_sessions
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for id in sessions {
        let Some(session) = draft.provider_session(id).cloned() else {
            continue;
        };
        if matches!(
            session.status,
            SessionStatus::Stopped | SessionStatus::Error
        ) {
            continue;
        }
        let survives = session
            .capabilities
            .sessions
            .pending_requests_survive_restart;
        stopped.push((id, survives));
        draft.put_provider_session(ProviderSession {
            status: SessionStatus::Stopped,
            detached_at: Some(now),
            detach_reason: Some(PROCESS_LOSS.to_owned()),
            updated_at: now,
            ..session
        });
    }
    let survives = |session: ProviderSessionId| {
        stopped
            .iter()
            .any(|(id, survives)| *id == session && *survives)
    };
    let expired = draft
        .all_requests()
        .into_iter()
        .filter(|request| request.status == RequestStatus::Pending)
        .filter(|request| match request.response_capability {
            ResponseCapability::Live {
                provider_session_id,
            } => !survives(provider_session_id),
            _ => false,
        })
        .cloned()
        .collect::<Vec<_>>();
    for request in expired {
        draft.put_request(RuntimeRequest {
            status: RequestStatus::Expired,
            resolved_at: Some(now),
            ..request
        });
    }

    let mut cancelled = Vec::new();
    let provider_threads = draft
        .base
        .provider_threads
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for id in provider_threads {
        let Some(provider_thread) = draft.provider_thread(id).cloned() else {
            continue;
        };
        let bound = provider_thread
            .provider_session_id
            .is_some_and(|session| stopped.iter().any(|(id, _)| *id == session));
        if !bound && provider_thread.pending_background_tasks.is_empty() {
            continue;
        }
        cancelled.extend(provider_thread.pending_background_tasks.iter().map(|task| {
            CancelledBackgroundWork {
                kind: match task.kind {
                    PendingTaskKind::Subagent => BackgroundWorkKind::Subagent,
                    PendingTaskKind::Command => BackgroundWorkKind::Shell,
                    PendingTaskKind::Monitor => BackgroundWorkKind::Monitor,
                    _ => BackgroundWorkKind::Task,
                },
                label: task
                    .description
                    .clone()
                    .unwrap_or_else(|| task.task_id.clone()),
                id: task.task_id.clone(),
            }
        }));
        draft.put_provider_thread(ProviderThread {
            provider_session_id: if bound {
                None
            } else {
                provider_thread.provider_session_id
            },
            status: match provider_thread.status {
                ProviderThreadStatus::Active => ProviderThreadStatus::Idle,
                status => status,
            },
            pending_background_tasks: Vec::new(),
            updated_at: now,
            ..provider_thread
        });
    }

    let turns = draft
        .base
        .provider_turns
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for id in turns {
        if let Some(turn) = draft.provider_turn(id).cloned()
            && matches!(
                turn.status,
                ProviderTurnStatus::Pending | ProviderTurnStatus::Running
            )
        {
            draft.put_provider_turn(ProviderTurn {
                status: ProviderTurnStatus::Interrupted,
                completed_at: Some(now),
                ..turn
            });
        }
    }

    let subagents = draft.base.subagents.keys().copied().collect::<Vec<_>>();
    for id in subagents {
        if let Some(subagent) = draft.subagent(id).cloned()
            && matches!(
                subagent.status,
                SubagentStatus::Idle
                    | SubagentStatus::Pending
                    | SubagentStatus::Running
                    | SubagentStatus::Waiting
            )
        {
            if !cancelled.iter().any(|work| {
                subagent
                    .native_task_ref
                    .as_ref()
                    .and_then(|reference| reference.native_id.as_deref())
                    == Some(work.id.as_str())
            }) {
                cancelled.push(CancelledBackgroundWork {
                    kind: BackgroundWorkKind::Subagent,
                    label: subagent
                        .title
                        .clone()
                        .unwrap_or_else(|| subagent.prompt.clone()),
                    id: subagent.id.to_string(),
                });
            }
            draft.put_subagent(Subagent {
                status: SubagentStatus::Interrupted,
                completed_at: Some(now),
                updated_at: now,
                ..subagent
            });
        }
    }
    cancelled
}

/// A waiting run whose pending request survived the restart keeps waiting
/// for the driver to re-attach.
fn pending_request_survives(draft: &Draft<'_>, run: &Run) -> bool {
    draft.all_requests().into_iter().any(|request| {
        request.status == RequestStatus::Pending
            && draft
                .node(request.node_id)
                .is_some_and(|node| node.run_id == Some(run.id))
    })
}

/// The provider never accepted the run's current attempt: its
/// `provider_turn.start` was cancelled with the process.
fn never_reached_provider(draft: &Draft<'_>, run: &Run) -> bool {
    run.status == RunStatus::Starting
        && run
            .active_attempt_id
            .and_then(|id| draft.attempt(id))
            .is_some_and(|attempt| attempt.provider_turn_id.is_none())
}

/// Ends `run` as `interrupted` by the restart, with the restart notice.
fn interrupt_for_restart(
    draft: &mut Draft<'_>,
    run: &Run,
    refusal: Option<Refusal>,
    cancelled: Vec<CancelledBackgroundWork>,
) {
    let now = draft.now;
    flush_streams(draft, run.id, true);
    for mut item in draft.items_of_run(run.id) {
        if !item.status.is_terminal() {
            item.status = ItemStatus::Interrupted;
            item.completed_at = Some(now);
            item.updated_at = now;
            draft.put_item(item);
        }
    }
    close_child_nodes(draft, run, NodeStatus::Interrupted, false);
    let message = match refusal {
        None => INTERRUPTED_NOTICE.to_owned(),
        Some(refusal) => format!(
            "{INTERRUPTED_NOTICE} It was not continued because {}.",
            refusal.reason()
        ),
    };
    // Derived from the run: a run is interrupted by a restart once.
    let id = TurnItemId(draft.derived("restart_interrupted", run.id));
    let block = run.ordinal.unwrap_or_else(|| draft.latest_ordinal());
    let item = draft.new_item(
        id,
        Some(run.id),
        block,
        ItemStatus::Completed,
        TurnItemKind::SystemNotice {
            message,
            lifecycle: Some(LifecycleNotice::RestartInterrupted),
        },
    );
    draft.put_item(item);
    let mut current = draft.run(run.id).cloned().unwrap_or_else(|| run.clone());
    current.restart_cancelled_background_work = cancelled;
    draft.put_run(current);
    let failure = ProviderFailure::new(FailureClass::TransportError, INTERRUPTED_NOTICE)
        .with_code(PROCESS_LOSS);
    finish_run(draft, run.id, RunStatus::Interrupted, Some(failure));
}

/// A run whose attempt never reached the provider sends its own message
/// again with a `provider_recovery` attempt, unless refused or retried
/// `max_restart_continuations` times already.
fn retry_attempt(
    draft: &mut Draft<'_>,
    settings: &OrchestratorSettings,
    run: &Run,
    refusal: Option<Refusal>,
    cancelled: Vec<CancelledBackgroundWork>,
) {
    let Some(attempt) = run
        .active_attempt_id
        .and_then(|id| draft.attempt(id))
        .cloned()
    else {
        interrupt_for_restart(draft, run, refusal, cancelled);
        return;
    };
    let recoveries = draft
        .base
        .attempts
        .values()
        .filter(|candidate| {
            candidate.run_id == run.id && candidate.reason == AttemptReason::ProviderRecovery
        })
        .count();
    let refusal = refusal.or_else(|| {
        (recoveries >= usize::from(settings.max_restart_continuations))
            .then_some(Refusal::ChainLimit)
    });
    if refusal.is_some() {
        interrupt_for_restart(draft, run, refusal, cancelled);
        return;
    }
    let now = draft.now;
    let next_ordinal = attempt.attempt_ordinal + 1;
    let next_id = RunAttemptId(draft.derived("run_attempt", format!("{}:{next_ordinal}", run.id)));
    draft.put_attempt(RunAttempt {
        status: AttemptStatus::Interrupted,
        completed_at: Some(now),
        ..attempt.clone()
    });
    draft.put_attempt(RunAttempt {
        id: next_id,
        attempt_ordinal: next_ordinal,
        provider_turn_id: None,
        reason: AttemptReason::ProviderRecovery,
        status: AttemptStatus::Pending,
        started_at: None,
        completed_at: None,
        ..attempt
    });
    draft.put_run(Run {
        active_attempt_id: Some(next_id),
        restart_cancelled_background_work: cancelled,
        ..run.clone()
    });
    draft.effect(Effect::ProviderTurnStart {
        run_id: run.id,
        attempt_id: next_id,
    });
}

/// `ContinueRun`: a new run that continues `of_run_id` with a
/// system-authored message and a `restart_resumed` notice.
pub(super) fn continue_run(
    draft: &mut Draft<'_>,
    settings: &OrchestratorSettings,
    of_run_id: RunId,
    kind: ContinuationKind,
    message_id: MessageId,
    text: &str,
) -> Result<(), CommandError> {
    let source = draft
        .run(of_run_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("run", of_run_id))?;
    if !source.status.is_terminal() {
        return Err(CommandError::invalid_state("the run is still active"));
    }
    if draft.all_runs().iter().any(|run| {
        run.continuation
            .as_ref()
            .is_some_and(|continuation| continuation.of_run_id == of_run_id)
    }) {
        return Err(CommandError::conflict(format!(
            "run {of_run_id} was already continued"
        )));
    }
    if kind == ContinuationKind::Restart
        && let Some(refusal) = thread_refusal(draft, settings)
    {
        return Err(CommandError::invalid_state(refusal.reason()));
    }
    if draft.thread.deleted_at.is_some() || draft.thread.archived_at.is_some() {
        return Err(CommandError::invalid_state("thread is archived or deleted"));
    }
    if draft.active_run().is_some() || draft.queue_head().is_some() {
        return Err(CommandError::invalid_state(Refusal::NewerWork.reason()));
    }
    if text.trim().is_empty() {
        return Err(CommandError::invalid("text", "message is empty"));
    }
    if draft.message_id_used(message_id) {
        return Err(CommandError::conflict(format!(
            "message {message_id} already exists"
        )));
    }
    let now = draft.now;
    let run_id = RunId(draft.derived("run", message_id));
    draft.put_message(Message {
        id: message_id,
        thread_id: draft.thread_id(),
        run_id: Some(run_id),
        node_id: None,
        role: MessageRole::User,
        created_by: Actor::System,
        creation_source: CreationSource::Server,
        text: text.to_owned(),
        display_text: None,
        attachments: Vec::new(),
        context: Vec::new(),
        notification: None,
        sender_thread_id: None,
        streaming: false,
        created_at: now,
        updated_at: now,
    });
    draft.put_run(Run {
        id: run_id,
        thread_id: draft.thread_id(),
        ordinal: None,
        model_selection: source.model_selection.clone(),
        provider_thread_id: None,
        user_message_id: message_id,
        root_node_id: None,
        active_attempt_id: None,
        status: RunStatus::Queued,
        queue_position: None,
        queue_held: false,
        requested_at: now,
        started_at: None,
        provider_completed_at: None,
        completed_at: None,
        failure: None,
        checkpoint_id: None,
        context_handoff_id: None,
        continuation: Some(RunContinuation { kind, of_run_id }),
        workspace_preparation: None,
        restart_cancelled_background_work: source.restart_cancelled_background_work.clone(),
        source_plan: None,
        delegated_task_id: None,
        delegated_completion: None,
    });
    // The notice opens the run's block, before its message (R15).
    let ordinal = draft.latest_ordinal() + 1;
    let notice_id = TurnItemId(draft.derived("restart_resumed", run_id));
    let mut notice = draft.new_item(
        notice_id,
        Some(run_id),
        ordinal,
        ItemStatus::Completed,
        TurnItemKind::SystemNotice {
            message: RESUMED_NOTICE.to_owned(),
            lifecycle: Some(LifecycleNotice::RestartResumed),
        },
    );
    start_run(draft, run_id, InputIntent::TurnStart);
    let started = draft.run(run_id).cloned();
    if let Some(started) = started {
        notice.node_id = started.root_node_id;
        notice.provider_thread_id = started.provider_thread_id;
    }
    draft.put_item(notice);
    draft.touch_thread(ThreadChange::Activity).updated_at = now;
    Ok(())
}
