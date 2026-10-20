//! Runtime requests, subagent stops, rollback and session detach.

use waku_protocol::orchestration::*;

use super::Draft;
use crate::command::CommandError;
use crate::effects::{Effect, SubagentTarget};

pub(super) fn decide_client(
    draft: &mut Draft<'_>,
    envelope: &CommandEnvelope,
) -> Result<(), CommandError> {
    if draft.thread.deleted_at.is_some() {
        return Err(CommandError::invalid_state("thread is deleted"));
    }
    match &envelope.command {
        Command::RuntimeRequestRespond {
            request_id,
            response,
            ..
        } => respond(draft, *request_id, response),
        Command::UserInputDismiss { request_id, .. } => dismiss(draft, *request_id),
        Command::SubagentStop {
            run_id,
            subagent_id,
            ..
        } => stop_subagent(draft, *run_id, *subagent_id),
        Command::CheckpointRollback {
            scope_id,
            checkpoint_id,
            restore_files,
            ..
        } => rollback(
            draft,
            &envelope.command_id,
            *scope_id,
            *checkpoint_id,
            *restore_files,
        ),
        Command::ProviderSessionDetach {
            provider_session_id,
            reason,
            ..
        } => {
            if draft.provider_session(*provider_session_id).is_none() {
                return Err(CommandError::not_found(
                    "provider session",
                    provider_session_id,
                ));
            }
            draft.effect(Effect::ProviderSessionDetach {
                provider_session_id: *provider_session_id,
                detail: reason.clone(),
            });
            Ok(())
        }
        _ => unreachable!("routed by decide_client"),
    }
}

fn pending_request(
    draft: &Draft<'_>,
    request_id: RuntimeRequestId,
) -> Result<RuntimeRequest, CommandError> {
    let request = draft
        .request(request_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("runtime request", request_id))?;
    if request.status != RequestStatus::Pending {
        return Err(CommandError::invalid_state("request is no longer pending"));
    }
    Ok(request)
}

fn respond(
    draft: &mut Draft<'_>,
    request_id: RuntimeRequestId,
    response: &RuntimeResponse,
) -> Result<(), CommandError> {
    let request = pending_request(draft, request_id)?;
    if request.response.is_some() {
        return Err(CommandError::conflict("request was already answered"));
    }
    match response {
        RuntimeResponse::Unknown => {
            return Err(CommandError::invalid("response", "unknown response type"));
        }
        RuntimeResponse::Approval { option_id } => {
            if !request.options.is_empty()
                && !request.options.iter().any(|option| &option.id == option_id)
            {
                return Err(CommandError::invalid(
                    "response.option_id",
                    format!("{option_id} is not an option of this request"),
                ));
            }
        }
        RuntimeResponse::UserInput { .. } | RuntimeResponse::Dismiss => {}
    }
    match &request.response_capability {
        ResponseCapability::Live {
            provider_session_id,
        } => {
            // The request resolves when the driver confirms it.
            draft.effect(Effect::RuntimeRequestRespond {
                provider_session_id: *provider_session_id,
                request_id,
                response: response.clone(),
            });
            draft.put_request(RuntimeRequest {
                response: Some(response.clone()),
                ..request
            });
            Ok(())
        }
        ResponseCapability::Message => Err(CommandError::unsupported(
            "answering a request with a message (Phase 3)",
        )),
        ResponseCapability::NotResumable { reason } => Err(CommandError::invalid_state(format!(
            "request cannot be answered: {reason}"
        ))),
        ResponseCapability::Unknown => Err(CommandError::invalid_state(
            "request has an unknown response capability",
        )),
    }
}

fn dismiss(draft: &mut Draft<'_>, request_id: RuntimeRequestId) -> Result<(), CommandError> {
    let request = pending_request(draft, request_id)?;
    if request.kind != RuntimeRequestKind::UserInput {
        return Err(CommandError::invalid_state(
            "request is not a user-input request",
        ));
    }
    if let ResponseCapability::Live {
        provider_session_id,
    } = request.response_capability
        && request.response.is_none()
    {
        draft.effect(Effect::RuntimeRequestRespond {
            provider_session_id,
            request_id,
            response: RuntimeResponse::Dismiss,
        });
        draft.put_request(RuntimeRequest {
            response: Some(RuntimeResponse::Dismiss),
            ..request
        });
    } else {
        let now = draft.now;
        draft.put_request(RuntimeRequest {
            status: RequestStatus::Cancelled,
            response: Some(RuntimeResponse::Dismiss),
            resolved_at: Some(now),
            ..request
        });
    }
    Ok(())
}

fn stop_subagent(
    draft: &mut Draft<'_>,
    run_id: RunId,
    subagent_id: NodeId,
) -> Result<(), CommandError> {
    let subagent = draft
        .subagent(subagent_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("subagent", subagent_id))?;
    if subagent.run_id != Some(run_id) {
        return Err(CommandError::invalid(
            "run_id",
            "subagent belongs to another run",
        ));
    }
    if subagent.status.is_terminal() {
        return Err(CommandError::invalid_state("subagent is not running"));
    }
    let run = draft
        .run(run_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("run", run_id))?;
    let attempt = run
        .active_attempt_id
        .and_then(|id| draft.attempt(id))
        .cloned()
        .ok_or_else(|| CommandError::invalid_state("run has no active attempt"))?;
    let provider_thread = draft
        .provider_thread(attempt.provider_thread_id)
        .cloned()
        .ok_or_else(|| CommandError::invalid_state("run has no provider thread"))?;
    let (Some(session), Some(turn), Some(native_task_id)) = (
        provider_thread.provider_session_id,
        attempt.provider_turn_id,
        subagent
            .native_task_ref
            .as_ref()
            .and_then(|reference| reference.native_id.clone()),
    ) else {
        return Err(CommandError::invalid_state(
            "subagent has no live provider task to stop",
        ));
    };
    draft.effect(Effect::ProviderTurnInterrupt {
        provider_session_id: session,
        provider_thread_id: provider_thread.id,
        provider_turn_id: turn,
        subagent: Some(SubagentTarget {
            node_id: subagent_id,
            native_task_id,
        }),
    });
    Ok(())
}

fn rollback(
    draft: &mut Draft<'_>,
    request_id: &CommandId,
    scope_id: CheckpointScopeId,
    checkpoint_id: CheckpointId,
    restore_files: bool,
) -> Result<(), CommandError> {
    if let Some(active) = draft.active_run() {
        return Err(CommandError::invalid_state(format!(
            "run {} is active",
            active.id
        )));
    }
    if draft.thread.rollback.is_some() {
        return Err(CommandError::conflict("a rollback is already in progress"));
    }
    let checkpoint = draft
        .checkpoint(checkpoint_id)
        .cloned()
        .ok_or_else(|| CommandError::not_found("checkpoint", checkpoint_id))?;
    if checkpoint.scope_id != scope_id {
        return Err(CommandError::invalid(
            "scope_id",
            "checkpoint is in another scope",
        ));
    }
    if checkpoint.status != CheckpointStatus::Ready {
        return Err(CommandError::invalid_state("checkpoint is not ready"));
    }
    let provider_thread_id = draft
        .thread
        .active_provider_thread_id
        .ok_or_else(|| CommandError::invalid_state("thread has no provider thread"))?;
    let now = draft.now;
    draft.touch_thread(ThreadChange::RollbackRequested).rollback = Some(RollbackState {
        request_id: request_id.clone(),
        scope_id,
        checkpoint_id,
        requested_at: now,
        failure: None,
    });
    draft.effect(Effect::ProviderThreadRollback {
        provider_thread_id,
        scope_id,
        checkpoint_id,
        restore_files,
        request_id: request_id.clone(),
    });
    Ok(())
}
