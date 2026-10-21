//! The orchestration commands the desktop dispatches (#55), built from what
//! it renders ([`ThreadView`]) so every caller makes the same choices: which
//! dispatch mode a composer action means, which run an interrupt targets,
//! which checkpoint a rewind rolls back to.
//!
//! Ids of new entities are chosen here (RFC 0003 §5.3), so a caller can
//! open the thread it is creating before the daemon answers.

use std::path::PathBuf;

use uuid::Uuid;
use waku_protocol::model::{self, MessageAttachment, ProviderKind, SessionWorkspace};
use waku_protocol::orchestration::*;

use crate::thread_view::{ThreadView, instance_of, workspace_strategy};

/// What the user asked the composer to do with a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendIntent {
    /// Enter: start a turn, or wait behind the running one.
    Submit,
    /// Explicitly queue behind the running turn.
    Queue,
    /// Fold into the running turn now.
    Steer,
}

/// How `message.dispatch` should deliver a message on `view`.
///
/// Everything but a steer the live session can take is `queueAfterActive`:
/// the daemon starts it at once on an idle thread with an empty queue and
/// otherwise queues it behind the active run, so a submit decided from a
/// view a few milliseconds old (two quick sends) never collides with the
/// run the first one started. A steer targets the active run when the live
/// session can take one (natively or by restarting the turn).
pub fn dispatch_mode(view: &ThreadView, intent: SendIntent) -> DispatchMode {
    match (intent, view.active_run_id) {
        (SendIntent::Steer, Some(active)) if view.can_steer => DispatchMode::SteerActive {
            target_run_id: active,
        },
        _ => DispatchMode::QueueAfterActive,
    }
}

/// A message to send: the provider-facing text and what the transcript
/// shows instead of it.
#[derive(Clone, Debug, Default)]
pub struct Outgoing {
    pub text: String,
    pub display_text: Option<String>,
    pub attachments: Vec<MessageAttachment>,
}

/// `message.dispatch` of `outgoing` into `thread_id`.
pub fn send_message(
    thread_id: Uuid,
    outgoing: &Outgoing,
    mode: DispatchMode,
    model_selection: Option<ModelSelection>,
) -> Command {
    Command::MessageDispatch {
        thread_id: ThreadId(thread_id),
        message_id: MessageId(Uuid::new_v4()),
        text: outgoing.text.clone(),
        display_text: outgoing.display_text.clone(),
        attachments: outgoing.attachments.iter().map(Attachment::from).collect(),
        context: Vec::new(),
        model_selection,
        mode,
        title_seed: None,
        source_plan: None,
    }
}

/// The provider, model and per-model options of a thread.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Selection {
    pub provider: ProviderKind,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub context_window: Option<String>,
    pub agent_preset: Option<String>,
}

impl Selection {
    pub fn of(view: &ThreadView) -> Self {
        Self {
            provider: view.provider,
            model: view.model.clone(),
            reasoning_effort: view.reasoning_effort.clone(),
            service_tier: view.service_tier.clone(),
            context_window: view.context_window.clone(),
            agent_preset: view.agent_preset.clone(),
        }
    }

    pub fn model_selection(&self) -> ModelSelection {
        ModelSelection {
            instance: instance_of(self.provider),
            model: self.model.clone(),
            reasoning_effort: self.reasoning_effort.clone(),
            service_tier: self.service_tier.clone(),
            context_window: self.context_window.clone(),
            agent_preset: self.agent_preset.clone(),
        }
    }
}

/// Everything `thread.create` needs for a task the user is about to send.
#[derive(Clone, Debug)]
pub struct NewThread {
    pub thread_id: Uuid,
    pub project_id: Uuid,
    pub selection: Selection,
    pub runtime_mode: model::RuntimeMode,
    pub workspace: SessionWorkspace,
}

/// `thread.create` then the first `message.dispatch`, to be dispatched in
/// that order ([`crate::store::ProjectionStore::dispatch_all`]). The title
/// stays a placeholder until the daemon names the thread from the prompt.
pub fn start_thread(new: &NewThread, outgoing: &Outgoing) -> [Command; 2] {
    let workspace = workspace_strategy(&new.workspace);
    // A new worktree is prepared by the run itself (`defer_start`), so the
    // thread starts in the project root.
    let (thread_workspace, mode) = match workspace {
        WorkspaceStrategy::NewWorktree { .. } => (
            WorkspaceStrategy::Root { branch: None },
            DispatchMode::DeferStart { workspace },
        ),
        workspace => (workspace, DispatchMode::StartImmediately),
    };
    [
        Command::ThreadCreate {
            thread_id: ThreadId(new.thread_id),
            project_id: ProjectId(new.project_id),
            title: None,
            model_selection: new.selection.model_selection(),
            runtime_mode: new.runtime_mode.into(),
            interaction_mode: InteractionMode::Default,
            workspace: thread_workspace,
            imported_native_thread: None,
        },
        send_message(new.thread_id, outgoing, mode, None),
    ]
}

/// `run.interrupt` of the active run, if there is one.
pub fn interrupt(view: &ThreadView) -> Option<Command> {
    Some(Command::RunInterrupt {
        thread_id: ThreadId(view.id),
        run_id: view.active_run_id?,
        reason: None,
        hold_queue: false,
    })
}

/// `runtime_request.respond` with the provider's own option id.
pub fn respond_approval(thread_id: Uuid, request_id: &str, option_id: &str) -> Option<Command> {
    Some(Command::RuntimeRequestRespond {
        thread_id: ThreadId(thread_id),
        request_id: RuntimeRequestId(request_id.parse().ok()?),
        response: RuntimeResponse::Approval {
            option_id: option_id.to_owned(),
        },
    })
}

/// `runtime_request.respond` with structured answers.
pub fn respond_user_input(
    thread_id: Uuid,
    request_id: &str,
    answers: &[model::UserInputAnswer],
) -> Option<Command> {
    Some(Command::RuntimeRequestRespond {
        thread_id: ThreadId(thread_id),
        request_id: RuntimeRequestId(request_id.parse().ok()?),
        response: RuntimeResponse::UserInput {
            answers: answers.iter().map(UserInputAnswer::from).collect(),
        },
    })
}

/// `user_input.dismiss`: the user declined to answer.
pub fn dismiss_user_input(thread_id: Uuid, request_id: &str) -> Option<Command> {
    Some(Command::UserInputDismiss {
        thread_id: ThreadId(thread_id),
        request_id: RuntimeRequestId(request_id.parse().ok()?),
    })
}

/// Why a turn cannot be rewound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RewindError {
    /// A run is active; stop it first.
    Busy,
    /// The turn is unknown or nothing precedes it to roll back to.
    NoCheckpoint,
}

/// `checkpoint.rollback` so that the turn `turn_id` and everything after it
/// disappear, restoring the files of the checkpoint the previous turn ended
/// with. The edited message is dispatched once the rollback completed.
pub fn rewind(view: &ThreadView, turn_id: Uuid) -> Result<Command, RewindError> {
    if view.active_run_id.is_some() {
        return Err(RewindError::Busy);
    }
    let ordinal = view
        .turn_ordinal(turn_id)
        .ok_or(RewindError::NoCheckpoint)?;
    let point = view
        .rewind_point(ordinal)
        .ok_or(RewindError::NoCheckpoint)?;
    Ok(Command::CheckpointRollback {
        thread_id: ThreadId(view.id),
        scope_id: point.scope_id,
        checkpoint_id: point.checkpoint_id,
        restore_files: true,
    })
}

/// `thread.fork` of `view` through the turn `turn_id` (inclusive) into a
/// new thread.
pub fn fork(view: &ThreadView, turn_id: Uuid, target: Uuid, title: Option<String>) -> Command {
    Command::ThreadFork {
        source_thread_id: ThreadId(view.id),
        target_thread_id: ThreadId(target),
        source_point: ForkSourcePoint::Run {
            run_id: RunId(turn_id),
        },
        title,
    }
}

pub fn rename(thread_id: Uuid, title: &str) -> Option<Command> {
    let title = title.trim();
    (!title.is_empty()).then(|| Command::ThreadRename {
        thread_id: ThreadId(thread_id),
        title: title.to_owned(),
    })
}

pub fn archive(thread_id: Uuid) -> Command {
    Command::ThreadArchive {
        thread_id: ThreadId(thread_id),
    }
}

pub fn unarchive(thread_id: Uuid) -> Command {
    Command::ThreadUnarchive {
        thread_id: ThreadId(thread_id),
    }
}

pub fn delete(thread_id: Uuid) -> Command {
    Command::ThreadDelete {
        thread_id: ThreadId(thread_id),
    }
}

/// `thread.visit`: the user looked at the thread now.
pub fn visit(thread_id: Uuid, at_millis: Timestamp) -> Command {
    Command::ThreadVisit {
        thread_id: ThreadId(thread_id),
        visited_at: at_millis,
    }
}

pub fn set_runtime_mode(thread_id: Uuid, mode: model::RuntimeMode) -> Command {
    Command::ThreadSetRuntimeMode {
        thread_id: ThreadId(thread_id),
        runtime_mode: mode.into(),
    }
}

pub fn set_selection(thread_id: Uuid, selection: &Selection) -> Command {
    Command::ThreadSetModelSelection {
        thread_id: ThreadId(thread_id),
        model_selection: selection.model_selection(),
    }
}

/// `queued_run.cancel` of a queued message (its id is its run's).
pub fn cancel_queued(thread_id: Uuid, queued_id: Uuid) -> Command {
    Command::QueuedRunCancel {
        thread_id: ThreadId(thread_id),
        run_id: RunId(queued_id),
    }
}

pub fn create_project(project_id: Uuid, path: PathBuf, name: String) -> Command {
    Command::ProjectCreate {
        project_id: ProjectId(project_id),
        name,
        path,
        order_key: None,
    }
}

pub fn remove_project(project_id: Uuid) -> Command {
    Command::ProjectRemove {
        project_id: ProjectId(project_id),
    }
}

pub fn rename_project(project_id: Uuid, name: &str) -> Option<Command> {
    let name = name.trim();
    (!name.is_empty()).then(|| Command::ProjectRename {
        project_id: ProjectId(project_id),
        name: name.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::thread_view::RunCheckpoint;
    use waku_protocol::model::{AgentTurn, QueuedMessage, TurnStatus};
    use waku_protocol::v8::orchestration::{ThreadActivity, ThreadRow};

    fn view() -> ThreadView {
        let thread: AppThread = serde_json::from_value(serde_json::json!({
            "id": Uuid::from_u128(1),
            "projectId": Uuid::from_u128(2),
            "createdBy": "user",
            "creationSource": "desktop",
            "title": "t",
            "titleSource": "user",
            "modelSelection": {"instance": "codex"},
            "runtimeMode": "full_access",
            "interactionMode": "default",
            "historyOrigin": "native",
            "lineage": {"rootThreadId": Uuid::from_u128(1)},
            "createdAt": 0,
            "updatedAt": 0,
        }))
        .unwrap();
        ThreadView::from_row(&ThreadRow {
            thread,
            activity: ThreadActivity::Idle,
            pending_request_count: 0,
            queued_run_count: 0,
        })
    }

    fn busy(can_steer: bool) -> ThreadView {
        let mut view = view();
        view.active_run_id = Some(RunId(Uuid::from_u128(9)));
        view.can_steer = can_steer;
        view
    }

    #[test]
    fn everything_but_a_possible_steer_queues_after_the_active_run() {
        // The daemon starts a queue-after-active message at once on an idle
        // thread, so a stale idle view cannot collide with a run that just
        // started.
        for intent in [SendIntent::Submit, SendIntent::Queue, SendIntent::Steer] {
            assert_eq!(
                dispatch_mode(&view(), intent),
                DispatchMode::QueueAfterActive
            );
        }
        let mut queued = view();
        queued.queued_messages.push(QueuedMessage::new("first"));
        assert_eq!(
            dispatch_mode(&queued, SendIntent::Submit),
            DispatchMode::QueueAfterActive
        );
    }

    #[test]
    fn a_busy_thread_queues_submissions_and_steers_when_it_can() {
        assert_eq!(
            dispatch_mode(&busy(true), SendIntent::Submit),
            DispatchMode::QueueAfterActive
        );
        assert_eq!(
            dispatch_mode(&busy(true), SendIntent::Queue),
            DispatchMode::QueueAfterActive
        );
        assert_eq!(
            dispatch_mode(&busy(true), SendIntent::Steer),
            DispatchMode::SteerActive {
                target_run_id: RunId(Uuid::from_u128(9))
            }
        );
        assert_eq!(
            dispatch_mode(&busy(false), SendIntent::Steer),
            DispatchMode::QueueAfterActive,
            "a session that cannot steer queues instead"
        );
    }

    #[test]
    fn a_new_worktree_is_prepared_by_the_first_run() {
        let new = NewThread {
            thread_id: Uuid::from_u128(1),
            project_id: Uuid::from_u128(2),
            selection: Selection {
                provider: ProviderKind::Claude,
                model: Some("opus".into()),
                ..Selection::default()
            },
            runtime_mode: model::RuntimeMode::Ask,
            workspace: SessionWorkspace::NewWorktree {
                base_branch: Some("main".into()),
            },
        };
        let outgoing = Outgoing {
            text: "go".into(),
            ..Outgoing::default()
        };
        let [create, message] = start_thread(&new, &outgoing);
        let Command::ThreadCreate {
            model_selection,
            runtime_mode,
            workspace,
            ..
        } = create
        else {
            panic!("thread.create first");
        };
        assert_eq!(model_selection.instance.as_str(), "claude");
        assert_eq!(model_selection.model.as_deref(), Some("opus"));
        assert_eq!(runtime_mode, RuntimeMode::Ask);
        assert_eq!(workspace, WorkspaceStrategy::Root { branch: None });
        let Command::MessageDispatch { mode, text, .. } = message else {
            panic!("then message.dispatch");
        };
        assert_eq!(text, "go");
        assert!(matches!(
            mode,
            DispatchMode::DeferStart {
                workspace: WorkspaceStrategy::NewWorktree { base_ref, .. }
            } if base_ref == "main"
        ));
    }

    #[test]
    fn rewinds_roll_back_to_the_previous_turns_checkpoint() {
        let mut view = view();
        let turn = |n: u128, ordinal: usize| AgentTurn {
            id: Uuid::from_u128(n),
            turn_count: ordinal,
            status: TurnStatus::Completed,
            provider_turn_started: true,
            provider_resume_at: None,
            started_at: 0,
            completed_at: Some(0),
            checkpoint: None,
        };
        view.turns = vec![turn(11, 1), turn(12, 2)];
        view.run_checkpoints = vec![RunCheckpoint {
            run_id: RunId(Uuid::from_u128(11)),
            ordinal: 1,
            scope_id: CheckpointScopeId(Uuid::from_u128(21)),
            checkpoint_id: CheckpointId(Uuid::from_u128(31)),
            ready: true,
        }];
        assert_eq!(
            rewind(&view, Uuid::from_u128(11)),
            Err(RewindError::NoCheckpoint)
        );
        let Ok(Command::CheckpointRollback {
            checkpoint_id,
            restore_files,
            ..
        }) = rewind(&view, Uuid::from_u128(12))
        else {
            panic!("a rollback");
        };
        assert_eq!(checkpoint_id, CheckpointId(Uuid::from_u128(31)));
        assert!(restore_files);
        view.active_run_id = Some(RunId(Uuid::from_u128(12)));
        assert_eq!(rewind(&view, Uuid::from_u128(12)), Err(RewindError::Busy));
    }

    #[test]
    fn requests_answer_with_typed_ids() {
        let id = Uuid::from_u128(5).to_string();
        let Some(Command::RuntimeRequestRespond { response, .. }) =
            respond_approval(Uuid::nil(), &id, "allow_once")
        else {
            panic!("a response");
        };
        assert_eq!(
            response,
            RuntimeResponse::Approval {
                option_id: "allow_once".into()
            }
        );
        assert!(respond_approval(Uuid::nil(), "not-a-uuid", "x").is_none());
        assert!(rename(Uuid::nil(), "   ").is_none());
    }
}
