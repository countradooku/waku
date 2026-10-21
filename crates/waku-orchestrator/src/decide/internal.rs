//! Internal commands: effect results (R21) and lifecycle signals.
//!
//! Effect results can arrive late (a run interrupted while its checkpoint was
//! captured). A result that no longer applies is accepted with no events, so
//! a replayed effect stays a no-op.

use waku_protocol::orchestration::*;

use super::run::{
    fail_prepared, finish_run, hold_queue, interrupt_run, provider_turn_failed, release_prepared,
};
use super::{Draft, truncate_chars};
use crate::command::{CommandError, InternalCommand};
use crate::effects::Effect;

const MAX_GENERATED_TITLE_CHARS: usize = 120;

pub(super) fn decide(
    draft: &mut Draft<'_>,
    _command_id: &CommandId,
    command: &InternalCommand,
) -> Result<(), CommandError> {
    let now = draft.now;
    match command {
        InternalCommand::PreparedRunProgress { run_id, .. } => {
            // Progress is shown live by the effect; nothing to persist.
            if draft.run(*run_id).is_none() {
                return Err(CommandError::not_found("run", run_id));
            }
        }
        InternalCommand::PreparedRunRelease {
            run_id, workspace, ..
        } => release_prepared(draft, *run_id, workspace)?,
        InternalCommand::PreparedRunFail {
            run_id, failure, ..
        } => fail_prepared(draft, *run_id, failure)?,
        InternalCommand::ProviderTurnFailed {
            run_id,
            attempt_id,
            failure,
            ..
        } => provider_turn_failed(draft, *run_id, *attempt_id, failure),
        InternalCommand::CheckpointCaptured {
            run_id,
            scope_id,
            result,
            ..
        } => checkpoint_captured(draft, *run_id, *scope_id, result)?,
        InternalCommand::CheckpointRollbackApplied {
            request_id,
            snapshot,
            ..
        } => {
            let Some(rollback) = draft
                .thread
                .rollback
                .clone()
                .filter(|rollback| &rollback.request_id == request_id)
            else {
                return Ok(());
            };
            let through = draft
                .checkpoint(rollback.checkpoint_id)
                .and_then(|checkpoint| checkpoint.app_run_ordinal)
                .unwrap_or(0);
            let rolled_back = draft
                .all_runs()
                .into_iter()
                .filter(|run| run.ordinal.is_some_and(|ordinal| ordinal > through))
                .filter(|run| run.status != RunStatus::RolledBack)
                .cloned()
                .collect::<Vec<_>>();
            for run in rolled_back {
                draft.put_run(Run {
                    status: RunStatus::RolledBack,
                    completed_at: run.completed_at.or(Some(now)),
                    ..run
                });
            }
            if let Some(snapshot) = snapshot
                && snapshot.thread.app_thread_id == Some(draft.thread_id())
            {
                draft.put_provider_thread(snapshot.thread.clone());
            }
            let thread = draft.touch_thread(ThreadChange::RollbackCompleted);
            thread.rollback = None;
            thread.updated_at = now;
        }
        InternalCommand::CheckpointRollbackFailed {
            request_id,
            message,
            ..
        } => {
            if let Some(mut rollback) = draft
                .thread
                .rollback
                .clone()
                .filter(|rollback| &rollback.request_id == request_id)
            {
                rollback.failure = Some(message.clone());
                draft.touch_thread(ThreadChange::RollbackFailed).rollback = Some(rollback);
            }
        }
        InternalCommand::TitleGenerated {
            request_id, title, ..
        } => {
            let regeneration_done = draft
                .thread
                .title_regeneration
                .as_ref()
                .is_some_and(|regeneration| &regeneration.request_id == request_id);
            let title = title
                .as_deref()
                .map(str::trim)
                .filter(|title| !title.is_empty())
                .map(|title| truncate_chars(title, MAX_GENERATED_TITLE_CHARS));
            // A user-chosen title is never overwritten.
            let apply = title.filter(|_| draft.thread.title_source != TitleSource::User);
            if apply.is_some() || regeneration_done {
                let thread = draft.touch_thread(ThreadChange::TitleGenerated);
                if let Some(title) = apply {
                    thread.title = title;
                    thread.title_source = TitleSource::Generated;
                }
                if regeneration_done {
                    thread.title_regeneration = None;
                }
            }
        }
        InternalCommand::SessionLost {
            provider_session_id,
            reason,
            ..
        } => {
            let Some(session) = draft.provider_session(*provider_session_id).cloned() else {
                return Ok(());
            };
            draft.put_provider_session(ProviderSession {
                status: SessionStatus::Stopped,
                last_error: Some(reason.clone()),
                detached_at: Some(now),
                detach_reason: Some(reason.clone()),
                updated_at: now,
                ..session
            });
            let live = draft
                .all_requests()
                .into_iter()
                .filter(|request| request.status == RequestStatus::Pending)
                .filter(|request| {
                    matches!(
                        request.response_capability,
                        ResponseCapability::Live { provider_session_id: id }
                            if id == *provider_session_id
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            for request in live {
                draft.put_request(RuntimeRequest {
                    status: RequestStatus::Expired,
                    resolved_at: Some(now),
                    ..request
                });
            }
        }
        InternalCommand::ThreadStop { reason, .. } => {
            hold_queue(draft);
            if let Some(active) = draft.active_run().cloned()
                && active.status != RunStatus::Finalizing
            {
                interrupt_run(draft, &active, reason.as_deref())?;
            }
        }
        InternalCommand::BackgroundWorkSettle {
            provider_thread_id,
            task_id,
            ..
        } => {
            if let Some(provider_thread) = draft.provider_thread(*provider_thread_id).cloned()
                && provider_thread
                    .pending_background_tasks
                    .iter()
                    .any(|task| &task.task_id == task_id)
            {
                let mut provider_thread = provider_thread;
                provider_thread
                    .pending_background_tasks
                    .retain(|task| &task.task_id != task_id);
                provider_thread.updated_at = now;
                draft.put_provider_thread(provider_thread);
            }
        }
        InternalCommand::SnoozeWake { at, .. } => {
            if draft.thread.snoozed_until.is_some_and(|until| until <= *at) {
                let thread = draft.touch_thread(ThreadChange::SnoozeWoke);
                thread.snoozed_until = None;
                thread.snoozed_at = None;
                thread.last_snooze_wake_at = Some(*at);
            }
        }
        InternalCommand::AutoSettle { snapshot_at, .. } => {
            // The auto-settle service decided on a snapshot of the thread;
            // anything that changed since (`updated_at`), or any state the
            // policy refuses (`lifecycle::policy::is_candidate`), makes this
            // a no-op.
            let thread = &draft.thread;
            let pending_request = draft
                .all_requests()
                .iter()
                .any(|request| request.status == RequestStatus::Pending);
            let background_work = draft
                .base
                .provider_threads
                .values()
                .any(|provider_thread| !provider_thread.pending_background_tasks.is_empty());
            let eligible = thread.auto_settle_disabled_at.is_none()
                && thread.settled_at.is_none()
                && thread.settled_override.is_none()
                && thread.archived_at.is_none()
                && thread.pinned_at.is_none()
                && thread.deleted_at.is_none()
                && thread.updated_at <= *snapshot_at
                && draft.active_run().is_none()
                && draft.queue_head().is_none()
                && !pending_request
                && !background_work;
            if eligible {
                draft.touch_thread(ThreadChange::AutoSettled).settled_at = Some(now);
                draft.effect(Effect::TerminalCleanup);
            }
        }
        InternalCommand::RecoverThread { .. } | InternalCommand::ContinueRun { .. } => {
            unreachable!("routed by decide_internal")
        }
    }
    Ok(())
}

fn checkpoint_captured(
    draft: &mut Draft<'_>,
    run_id: Option<RunId>,
    scope_id: CheckpointScopeId,
    result: &Result<crate::command::CapturedCheckpoint, String>,
) -> Result<(), CommandError> {
    let scope = draft
        .scope(scope_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("checkpoint scope", scope_id))?;
    let run = run_id.and_then(|run_id| draft.run(run_id)).cloned();
    let existing = draft.checkpoints_in_scope(scope_id);
    let ordinal = existing.len() as u32;
    let parent = existing.last().map(|checkpoint| checkpoint.id);
    let id = CheckpointId(draft.derived("checkpoint", format!("{scope_id}:{ordinal}")));
    let (git_ref, status, files) = match result {
        Ok(captured) => (
            captured.git_ref.clone(),
            CheckpointStatus::Ready,
            captured.files.clone(),
        ),
        Err(_) => (
            checkpoint_ref(draft.thread_id(), scope_id, ordinal),
            CheckpointStatus::Error,
            Vec::new(),
        ),
    };
    let now = draft.now;
    draft.put_checkpoint(Checkpoint {
        id,
        thread_id: draft.thread_id(),
        scope_id,
        run_id,
        node_id: scope.node_id,
        parent_checkpoint_id: parent,
        ordinal_within_scope: ordinal,
        app_run_ordinal: scope
            .advances_app_run_count
            .then(|| run.as_ref().and_then(|run| run.ordinal))
            .flatten(),
        git_ref,
        status,
        files: files.clone(),
        captured_at: now,
    });
    if let Some(run) = &run {
        if status == CheckpointStatus::Ready {
            let item_id = TurnItemId(draft.derived("checkpoint_item", id));
            let block = run.ordinal.unwrap_or_else(|| draft.latest_ordinal());
            let item = draft.new_item(
                item_id,
                Some(run.id),
                block,
                ItemStatus::Completed,
                TurnItemKind::Checkpoint {
                    checkpoint_id: id,
                    scope_id,
                    files,
                },
            );
            draft.put_item(item);
        }
        // The capture is the last step of the finalization barrier.
        if run.status == RunStatus::Finalizing && scope.advances_app_run_count {
            draft.put_run(Run {
                checkpoint_id: Some(id),
                ..run.clone()
            });
            finish_run(draft, run.id, RunStatus::Completed, None);
        }
    }
    Ok(())
}

/// `refs/waku/t/<thread>/<scope>/<ordinal>`.
pub fn checkpoint_ref(thread_id: ThreadId, scope_id: CheckpointScopeId, ordinal: u32) -> String {
    format!("refs/waku/t/{thread_id}/{scope_id}/{ordinal}")
}
