//! Domain events: one `<entity>.updated` per entity kind, each carrying the
//! full entity (RFC 0001 R11). Applying an event is an upsert by id.

use serde::{Deserialize, Serialize};

use super::entities::*;
use super::ids::*;
use super::turn_item::TurnItem;
use super::values::DriverKind;
use crate::ts::TS;

open_enum! {
    /// Why a thread snapshot was emitted. Keeps the audit trail that T3's
    /// separate thread events carried.
    pub enum ThreadChange {
        Created = "created",
        Imported = "imported",
        Archived = "archived",
        Unarchived = "unarchived",
        Deleted = "deleted",
        Settled = "settled",
        AutoSettled = "auto_settled",
        Unsettled = "unsettled",
        AutoSettleSet = "auto_settle_set",
        Snoozed = "snoozed",
        Unsnoozed = "unsnoozed",
        SnoozeWoke = "snooze_woke",
        Pinned = "pinned",
        Unpinned = "unpinned",
        PinReordered = "pin_reordered",
        ActiveReordered = "active_reordered",
        Visited = "visited",
        MarkedUnread = "marked_unread",
        Renamed = "renamed",
        TitleGenerated = "title_generated",
        TitleRegenerationStarted = "title_regeneration_started",
        WorkspaceChanged = "workspace_changed",
        RuntimeModeChanged = "runtime_mode_changed",
        InteractionModeChanged = "interaction_mode_changed",
        ModelSelectionChanged = "model_selection_changed",
        ProviderSwitched = "provider_switched",
        ActiveProviderThreadChanged = "active_provider_thread_changed",
        RollbackRequested = "rollback_requested",
        RollbackFailed = "rollback_failed",
        RollbackCompleted = "rollback_completed",
        LimitRecoveryChanged = "limit_recovery_changed",
        Activity = "activity",
    }
}

/// The payload of `thread.updated`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadUpdate {
    pub change: ThreadChange,
    pub thread: AppThread,
}

/// A committed domain fact. Internally tagged by `type` (RFC 0003 §1.1) so an
/// unknown event type skips its payload and decodes to `Unknown`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "type")]
pub enum DomainEvent {
    #[serde(rename = "project.updated")]
    ProjectUpdated(Project),
    #[serde(rename = "thread.updated")]
    ThreadUpdated(ThreadUpdate),
    #[serde(rename = "run.updated")]
    RunUpdated(Run),
    #[serde(rename = "run_attempt.updated")]
    RunAttemptUpdated(RunAttempt),
    #[serde(rename = "node.updated")]
    NodeUpdated(ExecutionNode),
    #[serde(rename = "subagent.updated")]
    SubagentUpdated(Subagent),
    #[serde(rename = "provider_session.updated")]
    ProviderSessionUpdated(ProviderSession),
    #[serde(rename = "provider_thread.updated")]
    ProviderThreadUpdated(ProviderThread),
    #[serde(rename = "provider_turn.updated")]
    ProviderTurnUpdated(ProviderTurn),
    #[serde(rename = "runtime_request.updated")]
    RuntimeRequestUpdated(RuntimeRequest),
    #[serde(rename = "message.updated")]
    MessageUpdated(Message),
    #[serde(rename = "plan.updated")]
    PlanUpdated(Plan),
    #[serde(rename = "turn_item.updated")]
    TurnItemUpdated(TurnItem),
    #[serde(rename = "checkpoint_scope.updated")]
    CheckpointScopeUpdated(CheckpointScope),
    #[serde(rename = "checkpoint.updated")]
    CheckpointUpdated(Checkpoint),
    #[serde(rename = "context_transfer.updated")]
    ContextTransferUpdated(ContextTransfer),
    #[serde(rename = "context_handoff.updated")]
    ContextHandoffUpdated(ContextHandoff),
    /// An event type written by a newer version.
    #[serde(rename = "unknown", other)]
    Unknown,
}

impl DomainEvent {
    /// Every known event type, in declaration order.
    pub const EVENT_TYPES: [&'static str; 17] = [
        "project.updated",
        "thread.updated",
        "run.updated",
        "run_attempt.updated",
        "node.updated",
        "subagent.updated",
        "provider_session.updated",
        "provider_thread.updated",
        "provider_turn.updated",
        "runtime_request.updated",
        "message.updated",
        "plan.updated",
        "turn_item.updated",
        "checkpoint_scope.updated",
        "checkpoint.updated",
        "context_transfer.updated",
        "context_handoff.updated",
    ];

    /// The serde tag; stored in `events.event_type`.
    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::ProjectUpdated(_) => "project.updated",
            Self::ThreadUpdated(_) => "thread.updated",
            Self::RunUpdated(_) => "run.updated",
            Self::RunAttemptUpdated(_) => "run_attempt.updated",
            Self::NodeUpdated(_) => "node.updated",
            Self::SubagentUpdated(_) => "subagent.updated",
            Self::ProviderSessionUpdated(_) => "provider_session.updated",
            Self::ProviderThreadUpdated(_) => "provider_thread.updated",
            Self::ProviderTurnUpdated(_) => "provider_turn.updated",
            Self::RuntimeRequestUpdated(_) => "runtime_request.updated",
            Self::MessageUpdated(_) => "message.updated",
            Self::PlanUpdated(_) => "plan.updated",
            Self::TurnItemUpdated(_) => "turn_item.updated",
            Self::CheckpointScopeUpdated(_) => "checkpoint_scope.updated",
            Self::CheckpointUpdated(_) => "checkpoint.updated",
            Self::ContextTransferUpdated(_) => "context_transfer.updated",
            Self::ContextHandoffUpdated(_) => "context_handoff.updated",
            Self::Unknown => "unknown",
        }
    }

    /// The payload's id; stored in `events.entity_id`.
    pub fn entity_id(&self) -> String {
        match self {
            Self::ProjectUpdated(entity) => entity.id.to_string(),
            Self::ThreadUpdated(update) => update.thread.id.to_string(),
            Self::RunUpdated(entity) => entity.id.to_string(),
            Self::RunAttemptUpdated(entity) => entity.id.to_string(),
            Self::NodeUpdated(entity) => entity.id.to_string(),
            Self::SubagentUpdated(entity) => entity.id.to_string(),
            Self::ProviderSessionUpdated(entity) => entity.id.to_string(),
            Self::ProviderThreadUpdated(entity) => entity.id.to_string(),
            Self::ProviderTurnUpdated(entity) => entity.id.to_string(),
            Self::RuntimeRequestUpdated(entity) => entity.id.to_string(),
            Self::MessageUpdated(entity) => entity.id.to_string(),
            Self::PlanUpdated(entity) => entity.id.to_string(),
            Self::TurnItemUpdated(entity) => entity.id.to_string(),
            Self::CheckpointScopeUpdated(entity) => entity.id.to_string(),
            Self::CheckpointUpdated(entity) => entity.id.to_string(),
            Self::ContextTransferUpdated(entity) => entity.id.to_string(),
            Self::ContextHandoffUpdated(entity) => entity.id.to_string(),
            Self::Unknown => String::new(),
        }
    }

    /// The run the payload belongs to, for the `events.run_id` column.
    pub fn run_id(&self) -> Option<RunId> {
        match self {
            Self::RunUpdated(entity) => Some(entity.id),
            Self::RunAttemptUpdated(entity) => Some(entity.run_id),
            Self::NodeUpdated(entity) => entity.run_id,
            Self::SubagentUpdated(entity) => entity.run_id,
            Self::MessageUpdated(entity) => entity.run_id,
            Self::PlanUpdated(entity) => entity.run_id,
            Self::TurnItemUpdated(entity) => entity.run_id,
            Self::CheckpointScopeUpdated(entity) => entity.run_id,
            Self::CheckpointUpdated(entity) => entity.run_id,
            Self::ContextTransferUpdated(entity) => entity.target_run_id,
            Self::ContextHandoffUpdated(entity) => Some(entity.target_run_id),
            _ => None,
        }
    }

    pub fn node_id(&self) -> Option<NodeId> {
        match self {
            Self::RunUpdated(entity) => entity.root_node_id,
            Self::RunAttemptUpdated(entity) => Some(entity.root_node_id),
            Self::NodeUpdated(entity) => Some(entity.id),
            Self::SubagentUpdated(entity) => Some(entity.id),
            Self::ProviderTurnUpdated(entity) => Some(entity.node_id),
            Self::RuntimeRequestUpdated(entity) => Some(entity.node_id),
            Self::MessageUpdated(entity) => entity.node_id,
            Self::PlanUpdated(entity) => Some(entity.node_id),
            Self::TurnItemUpdated(entity) => entity.node_id,
            Self::CheckpointScopeUpdated(entity) => Some(entity.node_id),
            Self::CheckpointUpdated(entity) => Some(entity.node_id),
            _ => None,
        }
    }

    pub fn driver(&self) -> Option<DriverKind> {
        match self {
            Self::SubagentUpdated(entity) => Some(entity.driver),
            Self::ProviderSessionUpdated(entity) => Some(entity.driver),
            Self::ProviderThreadUpdated(entity) => Some(entity.driver),
            Self::RunAttemptUpdated(entity) => {
                Some(DriverKind::from_instance(&entity.provider_instance_id))
            }
            _ => None,
        }
    }

    /// The app thread named by the payload itself, when it names one.
    pub fn payload_thread_id(&self) -> Option<ThreadId> {
        match self {
            Self::ThreadUpdated(update) => Some(update.thread.id),
            Self::RunUpdated(entity) => Some(entity.thread_id),
            Self::NodeUpdated(entity) => Some(entity.thread_id),
            Self::SubagentUpdated(entity) => Some(entity.thread_id),
            Self::ProviderThreadUpdated(entity) => entity.app_thread_id,
            Self::RuntimeRequestUpdated(entity) => Some(entity.thread_id),
            Self::MessageUpdated(entity) => Some(entity.thread_id),
            Self::PlanUpdated(entity) => Some(entity.thread_id),
            Self::TurnItemUpdated(entity) => Some(entity.thread_id),
            Self::CheckpointScopeUpdated(entity) => Some(entity.thread_id),
            Self::CheckpointUpdated(entity) => Some(entity.thread_id),
            Self::ContextHandoffUpdated(entity) => Some(entity.thread_id),
            _ => None,
        }
    }

    /// The payload alone, as stored in `events.payload_json`.
    pub fn payload_json(&self) -> serde_json::Result<String> {
        match self {
            Self::ProjectUpdated(entity) => serde_json::to_string(entity),
            Self::ThreadUpdated(update) => serde_json::to_string(update),
            Self::RunUpdated(entity) => serde_json::to_string(entity),
            Self::RunAttemptUpdated(entity) => serde_json::to_string(entity),
            Self::NodeUpdated(entity) => serde_json::to_string(entity),
            Self::SubagentUpdated(entity) => serde_json::to_string(entity),
            Self::ProviderSessionUpdated(entity) => serde_json::to_string(entity),
            Self::ProviderThreadUpdated(entity) => serde_json::to_string(entity),
            Self::ProviderTurnUpdated(entity) => serde_json::to_string(entity),
            Self::RuntimeRequestUpdated(entity) => serde_json::to_string(entity),
            Self::MessageUpdated(entity) => serde_json::to_string(entity),
            Self::PlanUpdated(entity) => serde_json::to_string(entity),
            Self::TurnItemUpdated(entity) => serde_json::to_string(entity),
            Self::CheckpointScopeUpdated(entity) => serde_json::to_string(entity),
            Self::CheckpointUpdated(entity) => serde_json::to_string(entity),
            Self::ContextTransferUpdated(entity) => serde_json::to_string(entity),
            Self::ContextHandoffUpdated(entity) => serde_json::to_string(entity),
            Self::Unknown => Ok("{}".to_owned()),
        }
    }

    /// Rebuilds an event from its stored columns. An unknown `event_type`
    /// decodes to `Unknown` without reading the payload.
    pub fn from_parts(event_type: &str, payload_json: &str) -> serde_json::Result<Self> {
        Ok(match event_type {
            "project.updated" => Self::ProjectUpdated(serde_json::from_str(payload_json)?),
            "thread.updated" => Self::ThreadUpdated(serde_json::from_str(payload_json)?),
            "run.updated" => Self::RunUpdated(serde_json::from_str(payload_json)?),
            "run_attempt.updated" => Self::RunAttemptUpdated(serde_json::from_str(payload_json)?),
            "node.updated" => Self::NodeUpdated(serde_json::from_str(payload_json)?),
            "subagent.updated" => Self::SubagentUpdated(serde_json::from_str(payload_json)?),
            "provider_session.updated" => {
                Self::ProviderSessionUpdated(serde_json::from_str(payload_json)?)
            }
            "provider_thread.updated" => {
                Self::ProviderThreadUpdated(serde_json::from_str(payload_json)?)
            }
            "provider_turn.updated" => {
                Self::ProviderTurnUpdated(serde_json::from_str(payload_json)?)
            }
            "runtime_request.updated" => {
                Self::RuntimeRequestUpdated(serde_json::from_str(payload_json)?)
            }
            "message.updated" => Self::MessageUpdated(serde_json::from_str(payload_json)?),
            "plan.updated" => Self::PlanUpdated(serde_json::from_str(payload_json)?),
            "turn_item.updated" => Self::TurnItemUpdated(serde_json::from_str(payload_json)?),
            "checkpoint_scope.updated" => {
                Self::CheckpointScopeUpdated(serde_json::from_str(payload_json)?)
            }
            "checkpoint.updated" => Self::CheckpointUpdated(serde_json::from_str(payload_json)?),
            "context_transfer.updated" => {
                Self::ContextTransferUpdated(serde_json::from_str(payload_json)?)
            }
            "context_handoff.updated" => {
                Self::ContextHandoffUpdated(serde_json::from_str(payload_json)?)
            }
            _ => Self::Unknown,
        })
    }
}

/// An event as committed: what streams send as `event{sequence, …}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct StoredEvent {
    pub sequence: Sequence,
    pub event_id: EventId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_id: Option<CommandId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<ThreadId>,
    pub occurred_at: Timestamp,
    pub event: DomainEvent,
}
