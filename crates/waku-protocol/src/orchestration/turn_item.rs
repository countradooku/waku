//! Turn items: the ordered display projection of a thread (#43).
//!
//! T3's 24 variants plus `Unknown`. Background tasks and delegated tasks are
//! `Notification` items (by `NotificationSource`) and `Subagent` items, as in
//! T3, which is why #43 counts 26 kinds. A client that decodes `Unknown` skips
//! the row but still advances its cursor.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::entities::{ForkOrigin, HandoffStrategy, RuntimeRequestKind, SubagentOrigin};
use super::ids::*;
use super::values::*;
use crate::ts::TS;

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

open_enum! {
    pub enum ItemStatus {
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

impl ItemStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

open_enum! {
    /// Where a tool acted.
    pub enum ToolSurface {
        Browser = "browser",
        Computer = "computer",
    }
}

/// An icon for a tool's activity row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum ToolIcon {
    Website {
        page_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        favicon_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        favicon_url_dark: Option<String>,
    },
    NativeApp {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        app_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
    },
    ThemedLogo {
        logo_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        logo_url_dark: Option<String>,
    },
    #[serde(other)]
    Unknown,
}

open_enum! {
    pub enum ToolSourceKind {
        Browser = "browser",
        Computer = "computer",
        Integration = "integration",
    }
}

/// The integration a tool belongs to (an MCP server, the browser, …).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ToolSource {
    pub key: String,
    pub name: String,
    pub kind: ToolSourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<ToolIcon>,
}

/// Presentation hints for tool rows.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ToolPresentation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<ToolSurface>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<ToolIcon>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<ToolSource>,
    /// Open string, e.g. `"read"` for a tool that only looks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_execution_kind: Option<String>,
}

open_enum! {
    /// Why a user message entered the transcript.
    pub enum InputIntent {
        TurnStart = "turn_start",
        QueuedTurn = "queued_turn",
        Steer = "steer",
        PromotedQueuedToSteer = "promoted_queued_to_steer",
    }
}

open_enum! {
    pub enum ResponseMode {
        Message = "message",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FileChangeDetail {
    /// Open string: `add`, `update`, `delete`, `move`, …
    pub operation: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FileSearchResult {
    pub file_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct WebSearchResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderRetry {
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_delay_ms: Option<u64>,
}

open_enum! {
    pub enum SecretStatus {
        Pending = "pending",
        Saved = "saved",
        Declined = "declined",
        Cancelled = "cancelled",
    }
}

/// One display row of a thread.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TurnItem {
    pub id: TurnItemId,
    pub thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_thread_id: Option<ProviderThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn_id: Option<ProviderTurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_item_ref: Option<ProviderRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_item_id: Option<TurnItemId>,
    /// `(run_ordinal << 24) | n`, assigned once by `decide` (R15). Doubles as
    /// the history-paging cursor.
    pub position: u64,
    pub status: ItemStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<ToolPresentation>,
    /// The number of live `delta`s folded into this snapshot (RFC 0003 §5.4).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub stream_rev: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamp>,
    pub updated_at: Timestamp,
    /// The variant, nested rather than flattened (R22).
    pub kind: TurnItemKind,
}

impl TurnItem {
    /// Bits of `position` that count items inside one run block.
    pub const POSITION_BLOCK_SHIFT: u32 = 24;
    pub const POSITION_INDEX_MASK: u64 = (1 << Self::POSITION_BLOCK_SHIFT) - 1;

    pub const fn position_of(block: u32, index: u32) -> u64 {
        ((block as u64) << Self::POSITION_BLOCK_SHIFT) | (index as u64 & Self::POSITION_INDEX_MASK)
    }

    /// The run block (the run ordinal, or 0 before any run).
    pub const fn block(&self) -> u32 {
        (self.position >> Self::POSITION_BLOCK_SHIFT) as u32
    }

    pub const fn index_in_block(&self) -> u32 {
        (self.position & Self::POSITION_INDEX_MASK) as u32
    }
}

/// The 24 display kinds plus `Unknown`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum TurnItemKind {
    Notification(Notification),
    UserMessage {
        message_id: MessageId,
        input_intent: InputIntent,
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_text: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<Attachment>,
        created_by: Actor,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sender_thread_id: Option<ThreadId>,
    },
    AssistantMessage {
        message_id: MessageId,
        text: String,
        #[serde(default, skip_serializing_if = "is_false")]
        streaming: bool,
    },
    Reasoning {
        text: String,
        #[serde(default, skip_serializing_if = "is_false")]
        streaming: bool,
    },
    ProposedPlan {
        plan_id: PlanId,
        markdown: String,
        #[serde(default, skip_serializing_if = "is_false")]
        streaming: bool,
    },
    TodoList {
        plan_id: PlanId,
        steps: Vec<PlanStep>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        explanation: Option<String>,
    },
    UserInputRequest {
        request_id: RuntimeRequestId,
        questions: Vec<UserInputQuestion>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        answers: Option<Vec<UserInputAnswer>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response_mode: Option<ResponseMode>,
    },
    FileChange {
        file_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        additions: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deletions: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diff: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        old_text: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        new_text: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changes: Vec<FileChangeDetail>,
        /// Wire projection only (#57); never set in storage.
        #[serde(default, skip_serializing_if = "is_false")]
        body_omitted: bool,
    },
    CommandExecution {
        input: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        /// Set in storage when the 256 KiB cap applied (R12).
        #[serde(default, skip_serializing_if = "is_false")]
        output_truncated: bool,
        /// Wire projection only (#57).
        #[serde(default, skip_serializing_if = "is_false")]
        output_omitted: bool,
        #[serde(default, skip_serializing_if = "is_false")]
        output_indicates_failure: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
    },
    FileSearch {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pattern: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        results: Vec<FileSearchResult>,
    },
    WebSearch {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        patterns: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        results: Vec<WebSearchResult>,
    },
    ApprovalRequest {
        request_id: RuntimeRequestId,
        request_kind: RuntimeRequestKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prompt: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        app_name: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        options: Vec<PermissionOption>,
    },
    Checkpoint {
        checkpoint_id: CheckpointId,
        scope_id: CheckpointScopeId,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        files: Vec<CheckpointFile>,
    },
    RunInterruptRequest {
        message: String,
    },
    RunInterruptResult {
        message: String,
    },
    SystemNotice {
        message: String,
    },
    Error {
        failure: ProviderFailure,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry: Option<ProviderRetry>,
    },
    Compaction {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        driver: Option<DriverKind>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before_tokens: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_tokens: Option<u64>,
    },
    Handoff {
        context_handoff_id: ContextHandoffId,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        from_provider_instance_ids: Vec<ProviderInstanceId>,
        to_provider_instance_id: ProviderInstanceId,
        strategy: HandoffStrategy,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
    Fork {
        source: ForkOrigin,
        target_thread_id: ThreadId,
    },
    ThreadCreated {
        target_thread_id: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_run_id: Option<RunId>,
        target_provider_instance_id: ProviderInstanceId,
        target_model: String,
    },
    SecretRequest {
        label: String,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        placeholder: Option<String>,
        secret_status: SecretStatus,
    },
    Subagent {
        subagent_id: NodeId,
        origin: SubagentOrigin,
        driver: DriverKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        child_thread_id: Option<ThreadId>,
        prompt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        progress: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<String>,
    },
    DynamicTool {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        /// Opaque tool input (RFC 0003 §1.1: no `serde_json::Value`).
        input: JsonText,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<JsonText>,
        /// Wire projection only (#57).
        #[serde(default, skip_serializing_if = "is_false")]
        output_omitted: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        viewed_image_path: Option<PathBuf>,
    },
    /// A kind written by a newer version. Clients skip the row and still
    /// advance their cursor.
    #[serde(other)]
    Unknown,
}

impl TurnItemKind {
    /// Every known tag, in declaration order.
    pub const TYPE_NAMES: [&'static str; 24] = [
        "notification",
        "user_message",
        "assistant_message",
        "reasoning",
        "proposed_plan",
        "todo_list",
        "user_input_request",
        "file_change",
        "command_execution",
        "file_search",
        "web_search",
        "approval_request",
        "checkpoint",
        "run_interrupt_request",
        "run_interrupt_result",
        "system_notice",
        "error",
        "compaction",
        "handoff",
        "fork",
        "thread_created",
        "secret_request",
        "subagent",
        "dynamic_tool",
    ];

    /// The serde tag, also stored in `p_turn_items.item_type`.
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::Notification(_) => "notification",
            Self::UserMessage { .. } => "user_message",
            Self::AssistantMessage { .. } => "assistant_message",
            Self::Reasoning { .. } => "reasoning",
            Self::ProposedPlan { .. } => "proposed_plan",
            Self::TodoList { .. } => "todo_list",
            Self::UserInputRequest { .. } => "user_input_request",
            Self::FileChange { .. } => "file_change",
            Self::CommandExecution { .. } => "command_execution",
            Self::FileSearch { .. } => "file_search",
            Self::WebSearch { .. } => "web_search",
            Self::ApprovalRequest { .. } => "approval_request",
            Self::Checkpoint { .. } => "checkpoint",
            Self::RunInterruptRequest { .. } => "run_interrupt_request",
            Self::RunInterruptResult { .. } => "run_interrupt_result",
            Self::SystemNotice { .. } => "system_notice",
            Self::Error { .. } => "error",
            Self::Compaction { .. } => "compaction",
            Self::Handoff { .. } => "handoff",
            Self::Fork { .. } => "fork",
            Self::ThreadCreated { .. } => "thread_created",
            Self::SecretRequest { .. } => "secret_request",
            Self::Subagent { .. } => "subagent",
            Self::DynamicTool { .. } => "dynamic_tool",
            Self::Unknown => "unknown",
        }
    }

    pub const fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }
}
