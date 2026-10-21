//! The execution graph (RFC 0001 "Entities"). Events carry these as full
//! snapshots and applying one is an upsert by id (rule 4).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::capabilities::Capabilities;
use super::ids::*;
use super::values::*;
use crate::ts::TS;

fn is_false(value: &bool) -> bool {
    !*value
}

// ---------------------------------------------------------------------------
// Project

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub path: PathBuf,
    pub order_key: OrderKey,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_at: Option<Timestamp>,
}

// ---------------------------------------------------------------------------
// AppThread

open_enum! {
    pub enum TitleSource {
        Placeholder = "placeholder",
        Prompt = "prompt",
        Generated = "generated",
        /// Set by the user; never overwritten by generation.
        User = "user",
    }
}

open_enum! {
    pub enum HistoryOrigin {
        Native = "native",
        WakuImport = "waku_import",
    }
}

open_enum! {
    pub enum LineageRelationship {
        Fork = "fork",
        Subagent = "subagent",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadLineage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<ThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship: Option<LineageRelationship>,
    pub root_thread_id: ThreadId,
}

impl ThreadLineage {
    pub fn root(thread_id: ThreadId) -> Self {
        Self {
            parent_thread_id: None,
            relationship: None,
            root_thread_id: thread_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum ForkOrigin {
    Run {
        thread_id: ThreadId,
        run_id: RunId,
    },
    Node {
        node_id: NodeId,
    },
    ProviderThread {
        provider_thread_id: ProviderThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_turn_id: Option<ProviderTurnId>,
    },
    #[serde(other)]
    Unknown,
}

open_enum! {
    pub enum SettledOverride {
        Settled = "settled",
        Active = "active",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct LimitRecovery {
    pub run_id: RunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at: Option<Timestamp>,
    #[serde(default)]
    pub auto_resume: bool,
    #[serde(default)]
    pub snooze: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TitleRegeneration {
    pub request_id: CommandId,
    pub started_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RollbackState {
    pub request_id: CommandId,
    pub scope_id: CheckpointScopeId,
    pub checkpoint_id: CheckpointId,
    pub requested_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// A user-visible conversation. It survives provider exits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct AppThread {
    pub id: ThreadId,
    pub project_id: ProjectId,
    pub created_by: Actor,
    pub creation_source: CreationSource,
    /// Always displayable.
    pub title: String,
    pub title_source: TitleSource,
    /// The default for the next run.
    pub model_selection: ModelSelection,
    pub runtime_mode: RuntimeMode,
    pub interaction_mode: InteractionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<PathBuf>,
    /// Created by Waku, so deleting the thread removes it.
    #[serde(default, skip_serializing_if = "is_false")]
    pub worktree_owned: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_provider_thread_id: Option<ProviderThreadId>,
    pub history_origin: HistoryOrigin,
    pub lineage: ThreadLineage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from: Option<ForkOrigin>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    /// Sidebar recency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reply_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<Timestamp>,
    /// Soft delete; cleanup runs as effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_override: Option<SettledOverride>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsettled_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_settle_disabled_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snoozed_until: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snoozed_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_snooze_wake_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_order_key: Option<OrderKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_order_key: Option<OrderKey>,
    /// Monotonic; a visit never bumps `updated_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_visited_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_recovery: Option<LimitRecovery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title_regeneration: Option<TitleRegeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackState>,
}

// ---------------------------------------------------------------------------
// Run and RunAttempt

open_enum! {
    pub enum RunStatus {
        Preparing = "preparing",
        Queued = "queued",
        Starting = "starting",
        Running = "running",
        /// Waiting on the user (an approval or a question).
        Waiting = "waiting",
        /// The provider is done and the finalization barrier is running (R13).
        Finalizing = "finalizing",
        Completed = "completed",
        Interrupted = "interrupted",
        Failed = "failed",
        Cancelled = "cancelled",
        RolledBack = "rolled_back",
    }
}

impl RunStatus {
    /// Terminal runs never change status again, except a failed preparation
    /// that the user retries.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Interrupted | Self::Failed | Self::Cancelled | Self::RolledBack
        )
    }

    /// The run occupies the thread: nothing else may start.
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Preparing | Self::Starting | Self::Running | Self::Waiting | Self::Finalizing
        )
    }
}

open_enum! {
    pub enum BackgroundWorkKind {
        Subagent = "subagent",
        Shell = "shell",
        Monitor = "monitor",
        Task = "task",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CancelledBackgroundWork {
    pub kind: BackgroundWorkKind,
    pub label: String,
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PlanRef {
    pub thread_id: ThreadId,
    pub plan_id: PlanId,
}

open_enum! {
    pub enum CohortDisposition {
        Open = "open",
        Stopped = "stopped",
        Disposed = "disposed",
    }
}

open_enum! {
    pub enum DeliveryState {
        Pending = "pending",
        Claimed = "claimed",
        Acknowledged = "acknowledged",
        Delivered = "delivered",
        Disposed = "disposed",
    }
}

/// (P3) The delegated-task completion cohort of a parent run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DelegatedCompletionCohort {
    pub disposition: CohortDisposition,
    pub next_generation: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryState>,
}

/// A counted, user-visible turn. Only its root node completes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Run {
    pub id: RunId,
    pub thread_id: ThreadId,
    /// `None` while preparing or queued (R14).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<u32>,
    pub model_selection: ModelSelection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_thread_id: Option<ProviderThreadId>,
    pub user_message_id: MessageId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_attempt_id: Option<RunAttemptId>,
    pub status: RunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<u32>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub queue_held: bool,
    pub requested_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    /// Provider terminal; the duration excludes finalization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_completed_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<ProviderFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<CheckpointId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_handoff_id: Option<ContextHandoffId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<RunContinuation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_preparation: Option<WorkspaceStrategy>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub restart_cancelled_background_work: Vec<CancelledBackgroundWork>,
    /// (P3) Implement a plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_plan: Option<PlanRef>,
    /// (P3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegated_task_id: Option<NodeId>,
    /// (P3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegated_completion: Option<DelegatedCompletionCohort>,
}

open_enum! {
    pub enum AttemptReason {
        Initial = "initial",
        SteeringRestart = "steering_restart",
        Retry = "retry",
        ProviderRecovery = "provider_recovery",
    }
}

open_enum! {
    pub enum AttemptStatus {
        Pending = "pending",
        Running = "running",
        Completed = "completed",
        Interrupted = "interrupted",
        Failed = "failed",
        Cancelled = "cancelled",
        Superseded = "superseded",
    }
}

impl AttemptStatus {
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Running | Self::Unknown)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RunAttempt {
    pub id: RunAttemptId,
    pub run_id: RunId,
    pub attempt_ordinal: u32,
    pub root_node_id: NodeId,
    pub provider_instance_id: ProviderInstanceId,
    pub provider_thread_id: ProviderThreadId,
    /// The input destination, kept when provider-thread rows are reused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn_id: Option<ProviderTurnId>,
    pub reason: AttemptReason,
    pub status: AttemptStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamp>,
}

// ---------------------------------------------------------------------------
// Execution nodes and subagents

open_enum! {
    pub enum NodeKind {
        RootTurn = "root_turn",
        AssistantMessage = "assistant_message",
        Reasoning = "reasoning",
        Plan = "plan",
        TodoList = "todo_list",
        ToolCall = "tool_call",
        ApprovalRequest = "approval_request",
        UserInputRequest = "user_input_request",
        Subagent = "subagent",
        Hook = "hook",
        System = "system",
    }
}

open_enum! {
    pub enum NodeStatus {
        Idle = "idle",
        Pending = "pending",
        Running = "running",
        Waiting = "waiting",
        Completed = "completed",
        Interrupted = "interrupted",
        Failed = "failed",
        Cancelled = "cancelled",
        RolledBack = "rolled_back",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ExecutionNode {
    pub id: NodeId,
    pub thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_node_id: Option<NodeId>,
    pub root_node_id: NodeId,
    pub kind: NodeKind,
    pub status: NodeStatus,
    /// True only for the root.
    #[serde(default, skip_serializing_if = "is_false")]
    pub counts_for_run: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_thread_id: Option<ProviderThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn_id: Option<ProviderTurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_item_ref: Option<ProviderRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_request_id: Option<RuntimeRequestId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_scope_id: Option<CheckpointScopeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamp>,
}

open_enum! {
    pub enum SubagentOrigin {
        ProviderNative = "provider_native",
        AppOwned = "app_owned",
    }
}

open_enum! {
    pub enum SubagentStatus {
        Idle = "idle",
        Pending = "pending",
        Running = "running",
        Waiting = "waiting",
        Completed = "completed",
        Failed = "failed",
        Cancelled = "cancelled",
        Interrupted = "interrupted",
    }
}

impl SubagentStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

open_enum! {
    pub enum CompletionWake {
        Always = "always",
        SettledOnly = "settled_only",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CompletionDelivery {
    pub state: DeliveryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_by_run_id: Option<RunId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Subagent {
    pub id: NodeId,
    pub thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    pub parent_node_id: NodeId,
    pub origin: SubagentOrigin,
    pub created_by: Actor,
    pub driver: DriverKind,
    pub provider_instance_id: ProviderInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_thread_id: Option<ProviderThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_thread_id: Option<ThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_task_ref: Option<ProviderRef>,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub status: SubagentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// (P3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_wake: Option<CompletionWake>,
    /// (P3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_delivery: Option<CompletionDelivery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamp>,
    pub updated_at: Timestamp,
}

// ---------------------------------------------------------------------------
// Provider sessions, threads and turns

open_enum! {
    pub enum SessionStatus {
        Starting = "starting",
        Ready = "ready",
        Running = "running",
        Waiting = "waiting",
        Stopped = "stopped",
        Error = "error",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderSession {
    pub id: ProviderSessionId,
    pub driver: DriverKind,
    pub provider_instance_id: ProviderInstanceId,
    pub status: SessionStatus,
    pub cwd: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub capabilities: Capabilities,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Replaces T3's `provider-session.detached` event (R11).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detached_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detach_reason: Option<String>,
    /// Slash commands the live session reported (ACP's
    /// `available_commands_update`, Claude's `system/init`), replaced whole
    /// on every report. Clients merge them with `composer.slashCommands`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub available_commands: Vec<SessionCommand>,
}

/// A slash command a provider session reported.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SessionCommand {
    /// Without the leading `/`.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

open_enum! {
    pub enum ProviderThreadStatus {
        NotLoaded = "not_loaded",
        Idle = "idle",
        Active = "active",
        Archived = "archived",
        Closed = "closed",
        Error = "error",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderThreadFork {
    pub provider_thread_id: ProviderThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn_id: Option<ProviderTurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<CheckpointId>,
}

open_enum! {
    pub enum PendingTaskKind {
        Subagent = "subagent",
        Command = "command",
        Monitor = "monitor",
        BackgroundTask = "background_task",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PendingBackgroundTask {
    pub kind: PendingTaskKind,
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_thread_id: Option<ThreadId>,
    /// The command turn item whose output the process streams: subscribe to
    /// `commandOutput{threadId, itemId}` for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<TurnItemId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct NativeMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<Timestamp>,
    /// The driver's opaque resume cursor (today's `ProviderResumeCursor`,
    /// serialized), kept for native resume (R23).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_cursor: Option<JsonText>,
}

/// A provider-native conversation handle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderThread {
    pub id: ProviderThreadId,
    pub driver: DriverKind,
    pub provider_instance_id: ProviderInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<ProviderSessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_thread_id: Option<ThreadId>,
    /// Set for a subagent's provider thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_ref: Option<ProviderRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_conversation_head_ref: Option<ProviderRef>,
    pub status: ProviderThreadStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_run_ordinal: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_ordinal: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handoff_ids: Vec<ContextHandoffId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from: Option<ProviderThreadFork>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_background_tasks: Vec<PendingBackgroundTask>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_usage: Option<ContextUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<ThreadGoal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_metadata: Option<NativeMetadata>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

open_enum! {
    pub enum ProviderTurnStatus {
        Pending = "pending",
        Running = "running",
        Completed = "completed",
        Interrupted = "interrupted",
        Failed = "failed",
        Cancelled = "cancelled",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderTurn {
    pub id: ProviderTurnId,
    pub provider_thread_id: ProviderThreadId,
    pub node_id: NodeId,
    /// A Codex `/goal` keeps one attempt across several turns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_attempt_id: Option<RunAttemptId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_turn_ref: Option<ProviderRef>,
    pub ordinal: u32,
    pub status: ProviderTurnStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamp>,
    /// Thread-cumulative, as reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenUsage>,
    /// This turn only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_token_usage: Option<TokenUsage>,
}

// ---------------------------------------------------------------------------
// Runtime requests

open_enum! {
    pub enum RuntimeRequestKind {
        CommandApproval = "command_approval",
        FileReadApproval = "file_read_approval",
        FileChangeApproval = "file_change_approval",
        ApplyPatchApproval = "apply_patch_approval",
        Permission = "permission",
        UserInput = "user_input",
        DynamicToolCall = "dynamic_tool_call",
        AuthRefresh = "auth_refresh",
    }
}

open_enum! {
    pub enum RequestStatus {
        Pending = "pending",
        Resolved = "resolved",
        Expired = "expired",
        Cancelled = "cancelled",
    }
}

/// How a request can be answered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum ResponseCapability {
    /// The answer goes to a live provider session.
    Live {
        provider_session_id: ProviderSessionId,
    },
    /// Answered by a new user message (Codex async questions).
    Message,
    NotResumable {
        reason: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRequest {
    pub id: RuntimeRequestId,
    pub thread_id: ThreadId,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn_id: Option<ProviderTurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_request_ref: Option<ProviderRef>,
    pub kind: RuntimeRequestKind,
    pub status: RequestStatus,
    pub response_capability: ResponseCapability,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<PermissionOption>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub questions: Vec<UserInputQuestion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<RuntimeResponse>,
    pub created_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<Timestamp>,
}

// ---------------------------------------------------------------------------
// Messages and plans

open_enum! {
    pub enum MessageRole {
        User = "user",
        Assistant = "assistant",
        System = "system",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: MessageId,
    pub thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    pub role: MessageRole,
    pub created_by: Actor,
    pub creation_source: CreationSource,
    /// Provider-facing text.
    pub text: String,
    /// What the user typed, before attachment mentions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_text: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<ContextRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<Notification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_thread_id: Option<ThreadId>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub streaming: bool,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

open_enum! {
    pub enum PlanStatus {
        Draft = "draft",
        Active = "active",
        Completed = "completed",
        Superseded = "superseded",
    }
}

/// The variant part of a [`Plan`], nested under `kind` (R22).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum PlanKind {
    ProposedPlan {
        markdown: String,
    },
    TodoList {
        steps: Vec<PlanStep>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        explanation: Option<String>,
    },
    #[serde(other)]
    Unknown,
}

impl PlanKind {
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::ProposedPlan { .. } => "proposed_plan",
            Self::TodoList { .. } => "todo_list",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub id: PlanId,
    pub thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    pub node_id: NodeId,
    pub status: PlanStatus,
    pub kind: PlanKind,
}

// ---------------------------------------------------------------------------
// Checkpoints

open_enum! {
    pub enum ScopeKind {
        RootRun = "root_run",
        Subagent = "subagent",
        Tool = "tool",
        ProviderThread = "provider_thread",
        Manual = "manual",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CheckpointScope {
    pub id: CheckpointScopeId,
    pub thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_scope_id: Option<CheckpointScopeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_thread_id: Option<ProviderThreadId>,
    pub kind: ScopeKind,
    pub ordinal_within_parent: u32,
    /// True only for `RootRun`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub advances_app_run_count: bool,
    pub cwd: PathBuf,
    pub created_at: Timestamp,
}

open_enum! {
    pub enum CheckpointStatus {
        Ready = "ready",
        Missing = "missing",
        Error = "error",
        Stale = "stale",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub id: CheckpointId,
    pub thread_id: ThreadId,
    pub scope_id: CheckpointScopeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_checkpoint_id: Option<CheckpointId>,
    pub ordinal_within_scope: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_run_ordinal: Option<u32>,
    /// New refs: `refs/waku/t/<thread>/<scope>/<ordinal>`; imported refs keep
    /// their names.
    pub git_ref: String,
    pub status: CheckpointStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<CheckpointFile>,
    pub captured_at: Timestamp,
}

// ---------------------------------------------------------------------------
// Context transfer and handoff (rows written from Phase 3; schema fixed now)

open_enum! {
    pub enum TransferKind {
        Fork = "fork",
        ProviderHandoff = "provider_handoff",
        MergeBack = "merge_back",
        SubagentSpawn = "subagent_spawn",
        SubagentResult = "subagent_result",
    }
}

open_enum! {
    pub enum TransferStatus {
        Pending = "pending",
        ResolvedNative = "resolved_native",
        ResolvedPortable = "resolved_portable",
        Failed = "failed",
        Consumed = "consumed",
        Superseded = "superseded",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ContextPoint {
    pub thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<CheckpointId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_item_id: Option<TurnItemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_thread_ref: Option<ProviderRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn_ref: Option<ProviderRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum TransferResolution {
    NativeFork {
        provider_thread_ref: ProviderRef,
    },
    PortableContext {
        handoff_id: ContextHandoffId,
    },
    DeltaContext {
        handoff_id: ContextHandoffId,
    },
    ForkDeltaContext {
        handoff_id: ContextHandoffId,
    },
    CheckpointContext {
        handoff_id: ContextHandoffId,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ContextTransfer {
    pub id: ContextTransferId,
    pub kind: TransferKind,
    pub source_thread_id: ThreadId,
    pub target_thread_id: ThreadId,
    pub source_point: ContextPoint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_point: Option<ContextPoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_provider_instance_id: Option<ProviderInstanceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_provider_instance_id: Option<ProviderInstanceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_run_id: Option<RunId>,
    pub status: TransferStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<TransferResolution>,
    pub created_by: Actor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumed_at: Option<Timestamp>,
}

open_enum! {
    pub enum HandoffStrategy {
        DeltaSinceTargetLastSeen = "delta_since_target_last_seen",
        ForkDeltaSummary = "fork_delta_summary",
        FullThreadSummary = "full_thread_summary",
        CheckpointSummary = "checkpoint_summary",
        ManualContext = "manual_context",
    }
}

open_enum! {
    pub enum HandoffStatus {
        Pending = "pending",
        Ready = "ready",
        Failed = "failed",
        Superseded = "superseded",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HandoffHistory {
    pub messages: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coverage: Option<RunOrdinalRange>,
    #[serde(default)]
    pub omitted_items: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omitted_item_ids: Vec<TurnItemId>,
}

open_enum! {
    pub enum HandoffDeliveryStatus {
        Pending = "pending",
        Injected = "injected",
        Inline = "inline",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HandoffDelivery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_thread_id: Option<String>,
    pub status: HandoffDeliveryStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub item_ids: Vec<TurnItemId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ContextHandoff {
    pub id: ContextHandoffId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<ContextTransferId>,
    pub thread_id: ThreadId,
    pub target_run_id: RunId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from_provider_thread_ids: Vec<ProviderThreadId>,
    pub to_provider_thread_id: ProviderThreadId,
    /// Inclusive.
    pub covered_run_ordinals: RunOrdinalRange,
    pub strategy: HandoffStrategy,
    pub status: HandoffStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_message_id: Option<MessageId>,
    pub summary_text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<HandoffHistory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<HandoffDelivery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_provider_instance_id: Option<ProviderInstanceId>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}
