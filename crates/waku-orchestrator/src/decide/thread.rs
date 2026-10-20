//! Thread lifecycle and sidebar state (#64 basics).
//!
//! A command that would not change anything (archiving an archived thread)
//! is accepted with no events, so retries and double clicks are harmless.

use waku_protocol::orchestration::*;

use super::project::time_order_key;
use super::run::{advance_queue, interrupt_run};
use super::{DecideContext, Decision, Draft, truncate_chars};
use crate::command::CommandError;
use crate::effects::{Effect, EffectType, TitleKind};

/// The title of a thread nobody named yet.
pub const PLACEHOLDER_TITLE: &str = "New thread";
const MAX_TITLE_CHARS: usize = 256;

fn user_title(title: &str) -> Result<String, CommandError> {
    let title = title.trim();
    if title.is_empty() {
        return Err(CommandError::invalid("title", "must not be empty"));
    }
    Ok(truncate_chars(title, MAX_TITLE_CHARS))
}

pub(super) fn create(
    cx: &mut DecideContext<'_>,
    envelope: &CommandEnvelope,
) -> Result<Decision, CommandError> {
    let Command::ThreadCreate {
        thread_id,
        project_id,
        title,
        model_selection,
        runtime_mode,
        interaction_mode,
        workspace,
        imported_native_thread,
    } = &envelope.command
    else {
        unreachable!("routed by decide_client");
    };
    if cx.thread.is_some() {
        return Err(CommandError::conflict(format!(
            "thread {thread_id} already exists"
        )));
    }
    let project = cx
        .project
        .filter(|project| project.id == *project_id && project.removed_at.is_none())
        .ok_or_else(|| CommandError::not_found("project", project_id))?;
    let now = cx.now;
    let (title, title_source) = match title.as_deref().map(str::trim) {
        Some(title) if !title.is_empty() => (user_title(title)?, TitleSource::User),
        _ => (PLACEHOLDER_TITLE.to_owned(), TitleSource::Placeholder),
    };
    let (branch, worktree_path) = match workspace {
        WorkspaceStrategy::Root { branch } => (branch.clone(), None),
        WorkspaceStrategy::ExistingWorktree { path, branch } => {
            (branch.clone(), Some(path.clone()))
        }
        // The first run prepares the worktree (`DispatchMode::DeferStart`).
        WorkspaceStrategy::NewWorktree { branch, .. } => (branch.clone(), None),
        WorkspaceStrategy::Unknown => {
            return Err(CommandError::invalid("workspace", "unknown strategy"));
        }
    };
    let mut thread = AppThread {
        id: *thread_id,
        project_id: project.id,
        created_by: envelope.issued_by,
        creation_source: envelope.source,
        title,
        title_source,
        model_selection: model_selection.clone(),
        runtime_mode: *runtime_mode,
        interaction_mode: *interaction_mode,
        branch,
        worktree_path,
        worktree_owned: false,
        active_provider_thread_id: None,
        history_origin: HistoryOrigin::Native,
        lineage: ThreadLineage::root(*thread_id),
        forked_from: None,
        created_at: now,
        updated_at: now,
        last_reply_at: None,
        archived_at: None,
        deleted_at: None,
        settled_override: None,
        settled_at: None,
        unsettled_at: None,
        auto_settle_disabled_at: None,
        snoozed_until: None,
        snoozed_at: None,
        last_snooze_wake_at: None,
        pinned_at: None,
        pin_order_key: None,
        active_order_key: None,
        last_visited_at: Some(now),
        limit_recovery: None,
        title_regeneration: None,
        rollback: None,
    };
    let mut events = Vec::new();
    let provider_thread = imported_native_thread.as_ref().map(|reference| {
        let id = ProviderThreadId(
            cx.ids
                .derived("imported_provider_thread", &thread_id.to_string()),
        );
        thread.active_provider_thread_id = Some(id);
        ProviderThread {
            id,
            driver: reference.driver,
            provider_instance_id: model_selection.instance.clone(),
            provider_session_id: None,
            app_thread_id: Some(*thread_id),
            owner_node_id: None,
            native_thread_ref: Some(reference.clone()),
            native_conversation_head_ref: None,
            status: ProviderThreadStatus::NotLoaded,
            first_run_ordinal: None,
            last_run_ordinal: None,
            handoff_ids: Vec::new(),
            forked_from: None,
            pending_background_tasks: Vec::new(),
            context_usage: None,
            goal: None,
            native_metadata: None,
            created_at: now,
            updated_at: now,
        }
    });
    events.push(DomainEvent::ThreadUpdated(ThreadUpdate {
        change: ThreadChange::Created,
        thread,
    }));
    events.extend(provider_thread.map(DomainEvent::ProviderThreadUpdated));
    Ok(Decision {
        events,
        ..Decision::default()
    })
}

/// Commands that edit the thread entity itself.
pub(super) fn decide_lifecycle(
    draft: &mut Draft<'_>,
    envelope: &CommandEnvelope,
) -> Result<(), CommandError> {
    let now = draft.now;
    let current = draft.thread.clone();
    if current.deleted_at.is_some() {
        return match envelope.command {
            Command::ThreadDelete { .. } => Ok(()),
            _ => Err(CommandError::invalid_state("thread is deleted")),
        };
    }
    match &envelope.command {
        Command::ThreadArchive { .. } => {
            if current.archived_at.is_none() {
                let thread = draft.touch_thread(ThreadChange::Archived);
                thread.archived_at = Some(now);
                thread.updated_at = now;
                draft.effect(Effect::TerminalCleanup);
            }
        }
        Command::ThreadUnarchive { .. } => {
            if current.archived_at.is_some() {
                let thread = draft.touch_thread(ThreadChange::Unarchived);
                thread.archived_at = None;
                thread.updated_at = now;
                advance_queue(draft);
            }
        }
        Command::ThreadDelete { .. } => delete(draft)?,
        Command::ThreadSettle { .. } => {
            if current.settled_at.is_none() {
                let thread = draft.touch_thread(ThreadChange::Settled);
                thread.settled_at = Some(now);
                thread.settled_override = Some(SettledOverride::Settled);
                thread.unsettled_at = None;
                draft.effect(Effect::TerminalCleanup);
            }
        }
        Command::ThreadUnsettle { .. } => {
            if current.settled_at.is_some()
                || current.settled_override != Some(SettledOverride::Active)
            {
                let thread = draft.touch_thread(ThreadChange::Unsettled);
                thread.settled_at = None;
                thread.settled_override = Some(SettledOverride::Active);
                thread.unsettled_at = Some(now);
            }
        }
        Command::ThreadSetAutoSettle { enabled, .. } => {
            if *enabled == current.auto_settle_disabled_at.is_some() {
                draft
                    .touch_thread(ThreadChange::AutoSettleSet)
                    .auto_settle_disabled_at = (!enabled).then_some(now);
            }
        }
        Command::ThreadSnooze { until, .. } => {
            if *until <= now {
                return Err(CommandError::invalid("until", "must be in the future"));
            }
            let thread = draft.touch_thread(ThreadChange::Snoozed);
            thread.snoozed_until = Some(*until);
            thread.snoozed_at = Some(now);
        }
        Command::ThreadUnsnooze { .. } => {
            if current.snoozed_until.is_some() {
                let thread = draft.touch_thread(ThreadChange::Unsnoozed);
                thread.snoozed_until = None;
                thread.snoozed_at = None;
            }
        }
        Command::ThreadPin { order_key, .. } => {
            if current.pinned_at.is_none() || order_key.is_some() {
                let thread = draft.touch_thread(ThreadChange::Pinned);
                thread.pinned_at = Some(current.pinned_at.unwrap_or(now));
                thread.pin_order_key =
                    Some(order_key.clone().unwrap_or_else(|| time_order_key(now)));
            }
        }
        Command::ThreadUnpin { .. } => {
            if current.pinned_at.is_some() {
                let thread = draft.touch_thread(ThreadChange::Unpinned);
                thread.pinned_at = None;
                thread.pin_order_key = None;
            }
        }
        Command::ThreadPinReorder { order_key, .. } => {
            if current.pinned_at.is_none() {
                return Err(CommandError::invalid_state("thread is not pinned"));
            }
            draft.touch_thread(ThreadChange::PinReordered).pin_order_key = Some(order_key.clone());
        }
        Command::ThreadActiveReorder { order_key, .. } => {
            draft
                .touch_thread(ThreadChange::ActiveReordered)
                .active_order_key = Some(order_key.clone());
        }
        Command::ThreadVisit { visited_at, .. } => {
            // Monotonic, and never bumps `updated_at`.
            if current
                .last_visited_at
                .is_none_or(|last| last < *visited_at)
            {
                draft.touch_thread(ThreadChange::Visited).last_visited_at = Some(*visited_at);
            }
        }
        Command::ThreadMarkUnread { .. } => {
            if current.last_visited_at.is_some() {
                draft
                    .touch_thread(ThreadChange::MarkedUnread)
                    .last_visited_at = None;
            }
        }
        Command::ThreadRename { title, .. } => {
            let title = user_title(title)?;
            if title != current.title || current.title_source != TitleSource::User {
                let thread = draft.touch_thread(ThreadChange::Renamed);
                thread.title = title;
                thread.title_source = TitleSource::User;
                thread.updated_at = now;
            }
        }
        Command::ThreadRegenerateTitle { .. } => {
            if !draft.has_runs() {
                return Err(CommandError::invalid_state(
                    "thread has no message to title",
                ));
            }
            draft
                .touch_thread(ThreadChange::TitleRegenerationStarted)
                .title_regeneration = Some(TitleRegeneration {
                request_id: envelope.command_id.clone(),
                started_at: now,
            });
            draft.effect(Effect::ThreadTitleGenerate {
                request_id: envelope.command_id.clone(),
                kind: TitleKind::Regenerate,
            });
        }
        Command::ThreadSetWorkspace {
            branch,
            worktree_path,
            expected_branch,
            expected_worktree_path,
            expected_empty,
            ..
        } => {
            if expected_branch.is_some() && *expected_branch != current.branch {
                return Err(CommandError::conflict("branch changed"));
            }
            if expected_worktree_path.is_some() && *expected_worktree_path != current.worktree_path
            {
                return Err(CommandError::conflict("worktree changed"));
            }
            if *expected_empty && draft.has_runs() {
                return Err(CommandError::conflict("thread is not empty"));
            }
            if draft.active_run().is_some() {
                return Err(CommandError::invalid_state("a run is active"));
            }
            let thread = draft.touch_thread(ThreadChange::WorkspaceChanged);
            if *worktree_path != current.worktree_path {
                thread.worktree_owned = false;
            }
            thread.branch = branch.clone();
            thread.worktree_path = worktree_path.clone();
            thread.updated_at = now;
        }
        Command::ThreadSetRuntimeMode { runtime_mode, .. } => {
            if *runtime_mode == RuntimeMode::Unknown {
                return Err(CommandError::invalid("runtime_mode", "unknown mode"));
            }
            if *runtime_mode != current.runtime_mode {
                draft
                    .touch_thread(ThreadChange::RuntimeModeChanged)
                    .runtime_mode = *runtime_mode;
            }
        }
        Command::ThreadSetInteractionMode {
            interaction_mode, ..
        } => {
            if *interaction_mode == InteractionMode::Unknown {
                return Err(CommandError::invalid("interaction_mode", "unknown mode"));
            }
            if *interaction_mode != current.interaction_mode {
                draft
                    .touch_thread(ThreadChange::InteractionModeChanged)
                    .interaction_mode = *interaction_mode;
            }
        }
        Command::ThreadSetModelSelection {
            model_selection, ..
        } => {
            if model_selection.instance != current.model_selection.instance && draft.has_runs() {
                return Err(CommandError::unsupported(
                    "provider.switch (Phase 3): changing the instance of a thread with runs",
                ));
            }
            if *model_selection != current.model_selection {
                draft
                    .touch_thread(ThreadChange::ModelSelectionChanged)
                    .model_selection = model_selection.clone();
            }
        }
        Command::ThreadSetLimitRecovery {
            run_id,
            auto_resume,
            snooze,
            ..
        } => {
            let Some(mut recovery) = current
                .limit_recovery
                .clone()
                .filter(|recovery| recovery.run_id == *run_id)
            else {
                return Err(CommandError::not_found("limit recovery for run", run_id));
            };
            if let Some(auto_resume) = auto_resume {
                recovery.auto_resume = *auto_resume;
            }
            if let Some(snooze) = snooze {
                recovery.snooze = *snooze;
            }
            if current.limit_recovery.as_ref() != Some(&recovery) {
                draft
                    .touch_thread(ThreadChange::LimitRecoveryChanged)
                    .limit_recovery = Some(recovery);
            }
        }
        _ => unreachable!("routed by decide_client"),
    }
    Ok(())
}

/// Soft delete: cancels queued runs and pending provider work, interrupts the
/// active run, and cleans up terminals and an owned worktree.
fn delete(draft: &mut Draft<'_>) -> Result<(), CommandError> {
    let now = draft.now;
    let thread = draft.touch_thread(ThreadChange::Deleted);
    thread.deleted_at = Some(now);
    thread.updated_at = now;
    let queued = draft.queued_runs().into_iter().cloned().collect::<Vec<_>>();
    for run in queued {
        draft.put_run(Run {
            status: RunStatus::Cancelled,
            queue_position: None,
            completed_at: Some(now),
            ..run
        });
    }
    if let Some(active) = draft.active_run().cloned()
        && active.status != RunStatus::Finalizing
    {
        interrupt_run(draft, &active, Some("Thread deleted"))?;
    }
    draft.cancel(&EffectType::PROVIDER_CONTROL, "thread deleted");
    draft.effect(Effect::TerminalCleanup);
    if draft.thread.worktree_owned
        && let Some(path) = draft.thread.worktree_path.clone()
    {
        draft.effect(Effect::WorktreeCleanup {
            path,
            branch: draft.thread.branch.clone(),
        });
    }
    Ok(())
}
