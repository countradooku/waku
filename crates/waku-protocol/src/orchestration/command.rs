//! Client-dispatchable commands (RFC 0001 "Commands").
//!
//! There is no `Unknown` command: an unknown command fails to decode and is
//! answered with `invalid_request`. Commands are never persisted, only their
//! receipts.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::entities::PlanRef;
use super::ids::*;
use super::values::*;
use crate::ts::TS;

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CommandEnvelope {
    pub command_id: CommandId,
    pub issued_by: Actor,
    pub source: CreationSource,
    pub command: Command,
}

/// How `message.dispatch` delivers a message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
// A client → daemon union: an unknown tag fails to decode and the daemon
// answers `badRequest`, so the schema marks it closed instead of open.
#[cfg_attr(feature = "schema", schemars(extend("x-waku-closed" = true)))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum DispatchMode {
    /// Policy picks inside the thread lock: start, steer or queue.
    Auto,
    StartImmediately,
    QueueAfterActive,
    SteerActive {
        target_run_id: RunId,
    },
    RestartActive {
        target_run_id: RunId,
    },
    /// Prepare a worktree first, then start.
    DeferStart {
        workspace: WorkspaceStrategy,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
// A client → daemon union: an unknown tag fails to decode and the daemon
// answers `badRequest`, so the schema marks it closed instead of open.
#[cfg_attr(feature = "schema", schemars(extend("x-waku-closed" = true)))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum ForkSourcePoint {
    LatestStable,
    Run { run_id: RunId },
    Checkpoint { checkpoint_id: CheckpointId },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
// A client → daemon union: an unknown tag fails to decode and the daemon
// answers `badRequest`, so the schema marks it closed instead of open.
#[cfg_attr(feature = "schema", schemars(extend("x-waku-closed" = true)))]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum Command {
    // Projects (R8)
    #[serde(rename = "project.create")]
    ProjectCreate {
        project_id: ProjectId,
        name: String,
        path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        order_key: Option<OrderKey>,
    },
    #[serde(rename = "project.rename")]
    ProjectRename { project_id: ProjectId, name: String },
    #[serde(rename = "project.reorder")]
    ProjectReorder {
        project_id: ProjectId,
        order_key: OrderKey,
    },
    #[serde(rename = "project.remove")]
    ProjectRemove { project_id: ProjectId },
    /// A project in a fresh folder of its own under the daemon's projectless
    /// root, for a task that has no project (one folder per task). The
    /// daemon creates the folder and records it as `project.create`; a retry
    /// with the same command id finds the project it made.
    #[serde(rename = "project.create_scratch")]
    ProjectCreateScratch {
        project_id: ProjectId,
        /// The sidebar name; "No project" when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// Names the folder (its slug), like the first prompt would.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prompt: Option<String>,
    },

    // Thread lifecycle and sidebar state (#64)
    #[serde(rename = "thread.create")]
    ThreadCreate {
        thread_id: ThreadId,
        project_id: ProjectId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        model_selection: ModelSelection,
        #[serde(default)]
        runtime_mode: RuntimeMode,
        #[serde(default)]
        interaction_mode: InteractionMode,
        workspace: WorkspaceStrategy,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        imported_native_thread: Option<ProviderRef>,
    },
    #[serde(rename = "thread.archive")]
    ThreadArchive { thread_id: ThreadId },
    #[serde(rename = "thread.unarchive")]
    ThreadUnarchive { thread_id: ThreadId },
    #[serde(rename = "thread.delete")]
    ThreadDelete { thread_id: ThreadId },
    #[serde(rename = "thread.settle")]
    ThreadSettle { thread_id: ThreadId },
    #[serde(rename = "thread.unsettle")]
    ThreadUnsettle { thread_id: ThreadId },
    #[serde(rename = "thread.auto_settle.set")]
    ThreadSetAutoSettle { thread_id: ThreadId, enabled: bool },
    #[serde(rename = "thread.snooze")]
    ThreadSnooze {
        thread_id: ThreadId,
        until: Timestamp,
    },
    #[serde(rename = "thread.unsnooze")]
    ThreadUnsnooze { thread_id: ThreadId },
    #[serde(rename = "thread.pin")]
    ThreadPin {
        thread_id: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        order_key: Option<OrderKey>,
    },
    #[serde(rename = "thread.unpin")]
    ThreadUnpin { thread_id: ThreadId },
    #[serde(rename = "thread.pin.reorder")]
    ThreadPinReorder {
        thread_id: ThreadId,
        order_key: OrderKey,
    },
    #[serde(rename = "thread.active.reorder")]
    ThreadActiveReorder {
        thread_id: ThreadId,
        order_key: OrderKey,
    },
    #[serde(rename = "thread.visit")]
    ThreadVisit {
        thread_id: ThreadId,
        visited_at: Timestamp,
    },
    #[serde(rename = "thread.mark_unread")]
    ThreadMarkUnread { thread_id: ThreadId },
    #[serde(rename = "thread.rename")]
    ThreadRename { thread_id: ThreadId, title: String },
    #[serde(rename = "thread.title.regenerate")]
    ThreadRegenerateTitle { thread_id: ThreadId },
    #[serde(rename = "thread.workspace.set")]
    ThreadSetWorkspace {
        thread_id: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        worktree_path: Option<PathBuf>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_branch: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_worktree_path: Option<PathBuf>,
        #[serde(default, skip_serializing_if = "is_false")]
        expected_empty: bool,
    },
    #[serde(rename = "thread.runtime_mode.set")]
    ThreadSetRuntimeMode {
        thread_id: ThreadId,
        runtime_mode: RuntimeMode,
    },
    #[serde(rename = "thread.interaction_mode.set")]
    ThreadSetInteractionMode {
        thread_id: ThreadId,
        interaction_mode: InteractionMode,
    },
    #[serde(rename = "thread.model_selection.set")]
    ThreadSetModelSelection {
        thread_id: ThreadId,
        model_selection: ModelSelection,
    },
    #[serde(rename = "thread.limit_recovery.set")]
    ThreadSetLimitRecovery {
        thread_id: ThreadId,
        run_id: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auto_resume: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        snooze: Option<bool>,
    },

    // Messages, runs and the queue
    #[serde(rename = "message.dispatch")]
    MessageDispatch {
        thread_id: ThreadId,
        message_id: MessageId,
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_text: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<Attachment>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        context: Vec<ContextRecord>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model_selection: Option<ModelSelection>,
        mode: DispatchMode,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title_seed: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_plan: Option<PlanRef>,
    },
    #[serde(rename = "run.interrupt")]
    RunInterrupt {
        thread_id: ThreadId,
        run_id: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(default, skip_serializing_if = "is_false")]
        hold_queue: bool,
    },
    #[serde(rename = "prepared_run.retry")]
    PreparedRunRetry { thread_id: ThreadId, run_id: RunId },
    #[serde(rename = "queue.resume")]
    QueueResume { thread_id: ThreadId },
    #[serde(rename = "queued_run.cancel")]
    QueuedRunCancel { thread_id: ThreadId, run_id: RunId },
    /// (P3)
    #[serde(rename = "queued_run.reorder")]
    QueuedRunReorder {
        thread_id: ThreadId,
        run_id: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before_run_id: Option<RunId>,
    },
    /// (P3)
    #[serde(rename = "queued_run.edit")]
    QueuedRunEdit {
        thread_id: ThreadId,
        run_id: RunId,
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attachments: Option<Vec<Attachment>>,
    },
    /// (P3)
    #[serde(rename = "queued_run.promote_to_steer")]
    QueuedRunPromoteToSteer {
        thread_id: ThreadId,
        queued_run_id: RunId,
        target_run_id: RunId,
    },
    #[serde(rename = "runtime_request.respond")]
    RuntimeRequestRespond {
        thread_id: ThreadId,
        request_id: RuntimeRequestId,
        response: RuntimeResponse,
    },
    #[serde(rename = "user_input.dismiss")]
    UserInputDismiss {
        thread_id: ThreadId,
        request_id: RuntimeRequestId,
    },
    #[serde(rename = "subagent.stop")]
    SubagentStop {
        thread_id: ThreadId,
        run_id: RunId,
        subagent_id: NodeId,
    },
    /// Stop a background process or monitor a provider thread reports in
    /// `pendingBackgroundTasks` (subagents use `subagent.stop`).
    #[serde(rename = "background_work.stop")]
    BackgroundWorkStop {
        thread_id: ThreadId,
        task_id: String,
        /// The provider thread that holds the task; any of the thread's
        /// when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_thread_id: Option<ProviderThreadId>,
    },
    /// Ask the live session which background work still runs and drop what
    /// it no longer reports (without a live session, nothing runs).
    #[serde(rename = "background_work.refresh")]
    BackgroundWorkRefresh { thread_id: ThreadId },

    // Checkpoints, provider sessions and lineage
    #[serde(rename = "checkpoint.rollback")]
    CheckpointRollback {
        thread_id: ThreadId,
        scope_id: CheckpointScopeId,
        checkpoint_id: CheckpointId,
        #[serde(default)]
        restore_files: bool,
    },
    #[serde(rename = "provider_session.detach")]
    ProviderSessionDetach {
        thread_id: ThreadId,
        provider_session_id: ProviderSessionId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// (P3)
    #[serde(rename = "thread.fork")]
    ThreadFork {
        source_thread_id: ThreadId,
        target_thread_id: ThreadId,
        source_point: ForkSourcePoint,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    /// (P3)
    #[serde(rename = "thread.merge_back")]
    ThreadMergeBack {
        source_thread_id: ThreadId,
        target_thread_id: ThreadId,
        source_point: ForkSourcePoint,
    },
    /// (P3)
    #[serde(rename = "provider.switch")]
    ProviderSwitch {
        thread_id: ThreadId,
        model_selection: ModelSelection,
    },
}

/// What a command's lock is keyed by (R8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CommandTarget {
    Project(ProjectId),
    Thread(ThreadId),
}

impl Command {
    /// The serde tag, stored in `command_receipts.command_type`.
    pub const fn command_type(&self) -> &'static str {
        match self {
            Self::ProjectCreate { .. } => "project.create",
            Self::ProjectRename { .. } => "project.rename",
            Self::ProjectReorder { .. } => "project.reorder",
            Self::ProjectRemove { .. } => "project.remove",
            Self::ProjectCreateScratch { .. } => "project.create_scratch",
            Self::ThreadCreate { .. } => "thread.create",
            Self::ThreadArchive { .. } => "thread.archive",
            Self::ThreadUnarchive { .. } => "thread.unarchive",
            Self::ThreadDelete { .. } => "thread.delete",
            Self::ThreadSettle { .. } => "thread.settle",
            Self::ThreadUnsettle { .. } => "thread.unsettle",
            Self::ThreadSetAutoSettle { .. } => "thread.auto_settle.set",
            Self::ThreadSnooze { .. } => "thread.snooze",
            Self::ThreadUnsnooze { .. } => "thread.unsnooze",
            Self::ThreadPin { .. } => "thread.pin",
            Self::ThreadUnpin { .. } => "thread.unpin",
            Self::ThreadPinReorder { .. } => "thread.pin.reorder",
            Self::ThreadActiveReorder { .. } => "thread.active.reorder",
            Self::ThreadVisit { .. } => "thread.visit",
            Self::ThreadMarkUnread { .. } => "thread.mark_unread",
            Self::ThreadRename { .. } => "thread.rename",
            Self::ThreadRegenerateTitle { .. } => "thread.title.regenerate",
            Self::ThreadSetWorkspace { .. } => "thread.workspace.set",
            Self::ThreadSetRuntimeMode { .. } => "thread.runtime_mode.set",
            Self::ThreadSetInteractionMode { .. } => "thread.interaction_mode.set",
            Self::ThreadSetModelSelection { .. } => "thread.model_selection.set",
            Self::ThreadSetLimitRecovery { .. } => "thread.limit_recovery.set",
            Self::MessageDispatch { .. } => "message.dispatch",
            Self::RunInterrupt { .. } => "run.interrupt",
            Self::PreparedRunRetry { .. } => "prepared_run.retry",
            Self::QueueResume { .. } => "queue.resume",
            Self::QueuedRunCancel { .. } => "queued_run.cancel",
            Self::QueuedRunReorder { .. } => "queued_run.reorder",
            Self::QueuedRunEdit { .. } => "queued_run.edit",
            Self::QueuedRunPromoteToSteer { .. } => "queued_run.promote_to_steer",
            Self::RuntimeRequestRespond { .. } => "runtime_request.respond",
            Self::UserInputDismiss { .. } => "user_input.dismiss",
            Self::SubagentStop { .. } => "subagent.stop",
            Self::BackgroundWorkStop { .. } => "background_work.stop",
            Self::BackgroundWorkRefresh { .. } => "background_work.refresh",
            Self::CheckpointRollback { .. } => "checkpoint.rollback",
            Self::ProviderSessionDetach { .. } => "provider_session.detach",
            Self::ThreadFork { .. } => "thread.fork",
            Self::ThreadMergeBack { .. } => "thread.merge_back",
            Self::ProviderSwitch { .. } => "provider.switch",
        }
    }

    /// The key whose lock the command holds.
    pub fn target(&self) -> CommandTarget {
        match self {
            Self::ProjectCreate { project_id, .. }
            | Self::ProjectRename { project_id, .. }
            | Self::ProjectReorder { project_id, .. }
            | Self::ProjectRemove { project_id }
            | Self::ProjectCreateScratch { project_id, .. } => CommandTarget::Project(*project_id),
            Self::ThreadFork {
                source_thread_id, ..
            }
            | Self::ThreadMergeBack {
                source_thread_id, ..
            } => CommandTarget::Thread(*source_thread_id),
            Self::ThreadCreate { thread_id, .. }
            | Self::ThreadArchive { thread_id }
            | Self::ThreadUnarchive { thread_id }
            | Self::ThreadDelete { thread_id }
            | Self::ThreadSettle { thread_id }
            | Self::ThreadUnsettle { thread_id }
            | Self::ThreadSetAutoSettle { thread_id, .. }
            | Self::ThreadSnooze { thread_id, .. }
            | Self::ThreadUnsnooze { thread_id }
            | Self::ThreadPin { thread_id, .. }
            | Self::ThreadUnpin { thread_id }
            | Self::ThreadPinReorder { thread_id, .. }
            | Self::ThreadActiveReorder { thread_id, .. }
            | Self::ThreadVisit { thread_id, .. }
            | Self::ThreadMarkUnread { thread_id }
            | Self::ThreadRename { thread_id, .. }
            | Self::ThreadRegenerateTitle { thread_id }
            | Self::ThreadSetWorkspace { thread_id, .. }
            | Self::ThreadSetRuntimeMode { thread_id, .. }
            | Self::ThreadSetInteractionMode { thread_id, .. }
            | Self::ThreadSetModelSelection { thread_id, .. }
            | Self::ThreadSetLimitRecovery { thread_id, .. }
            | Self::MessageDispatch { thread_id, .. }
            | Self::RunInterrupt { thread_id, .. }
            | Self::PreparedRunRetry { thread_id, .. }
            | Self::QueueResume { thread_id }
            | Self::QueuedRunCancel { thread_id, .. }
            | Self::QueuedRunReorder { thread_id, .. }
            | Self::QueuedRunEdit { thread_id, .. }
            | Self::QueuedRunPromoteToSteer { thread_id, .. }
            | Self::RuntimeRequestRespond { thread_id, .. }
            | Self::UserInputDismiss { thread_id, .. }
            | Self::SubagentStop { thread_id, .. }
            | Self::BackgroundWorkStop { thread_id, .. }
            | Self::BackgroundWorkRefresh { thread_id }
            | Self::CheckpointRollback { thread_id, .. }
            | Self::ProviderSessionDetach { thread_id, .. }
            | Self::ProviderSwitch { thread_id, .. } => CommandTarget::Thread(*thread_id),
        }
    }

    /// A thread the command creates, whose lock it also holds.
    pub fn created_thread(&self) -> Option<ThreadId> {
        match self {
            Self::ThreadFork {
                target_thread_id, ..
            } => Some(*target_thread_id),
            _ => None,
        }
    }
}
