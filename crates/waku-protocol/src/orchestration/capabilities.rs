//! Provider capability groups (RFC 0002 "Capabilities", decision D-4).
//!
//! Field names match T3's `OrchestrationV2ProviderCapabilities` one to one in
//! snake_case, so the T3 matrix maps mechanically. `Default` is the weakest
//! claim, and `#[serde(default)]` on every group means a row persisted before
//! a field existed decodes to "not supported".

use serde::{Deserialize, Serialize};

use super::values::{RefStrength, RuntimeMode};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct Capabilities {
    pub sessions: SessionCaps,
    pub threads: ThreadCaps,
    pub turns: TurnCaps,
    pub streaming: StreamingCaps,
    pub tools: ToolCaps,
    pub approvals: ApprovalCaps,
    pub planning: PlanningCaps,
    pub subagents: SubagentCaps,
    pub context: ContextCaps,
    pub checkpointing: CheckpointCaps,
    pub identity: IdentityCaps,
    pub runtime_policy: RuntimePolicyCaps,
    pub extras: ExtraCaps,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct SessionCaps {
    pub multiple_threads_per_session: bool,
    pub model_switch_in_session: bool,
    pub provider_switching_via_handoff: bool,
    pub runtime_mode_switch_in_session: bool,
    pub pending_requests_survive_restart: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct ThreadCaps {
    pub create_empty_thread: bool,
    pub read_snapshot: bool,
    pub rollback: bool,
    pub fork: bool,
    pub fork_from_turn: bool,
    pub fork_from_subagent_thread: bool,
    /// The fork is made by the session that will run the forked thread, so
    /// the new thread is loaded where its turns run (Codex: a thread stays
    /// owned by the app-server that loaded it).
    pub fork_in_target_session: bool,
    pub native_thread_id: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct TurnCaps {
    pub native_turn_id: bool,
    pub emits_turn_started: bool,
    pub emits_turn_completed: bool,
    pub interrupt: bool,
    pub active_steering: bool,
    /// Native steering cancels pending tools before consuming the message.
    pub active_steering_interrupts_tools: bool,
    pub steering_by_interrupt_restart: bool,
    pub queued_messages: bool,
    pub terminal_status_quality: Quality,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct StreamingCaps {
    pub assistant_text: bool,
    pub reasoning: bool,
    pub tool_output: bool,
    pub plan_text: bool,
    pub emits_message_completed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct ToolCaps {
    pub tool_item_ids: bool,
    pub emits_tool_started: bool,
    pub emits_tool_completed: bool,
    pub emits_tool_output: bool,
    pub mcp_tools: bool,
    pub dynamic_tool_callbacks: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct ApprovalCaps {
    pub command: bool,
    pub file_read: bool,
    pub file_change: bool,
    pub apply_patch: bool,
    pub native_request_ids: bool,
    /// The reply must reach the same live process. A restart loses the request.
    pub callbacks_are_live_only: bool,
    pub from_subagents: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct PlanningCaps {
    pub emits_plan_updated: bool,
    pub emits_todo_list: bool,
    pub emits_proposed_plan: bool,
    pub structured_questions: bool,
    pub plan_deltas_have_item_ids: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct SubagentCaps {
    pub supported: bool,
    pub exposes_thread_ids: bool,
    pub emits_lifecycle: bool,
    pub can_wait: bool,
    pub can_close: bool,
    pub can_fork_thread: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct ContextCaps {
    pub accepts_system_context: bool,
    pub accepts_developer_context: bool,
    pub accepts_synthetic_user_context: bool,
    pub can_generate_summaries: bool,
    pub can_consume_handoff_summaries: bool,
    pub delta_handoff: bool,
    pub full_thread_handoff: bool,
    /// `None` = the orchestrator default (`DEFAULT_HANDOFF_CHARS = 100_000`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_recommended_handoff_chars: Option<u32>,
    /// Waku name for T3's optional `injectHistory`.
    pub native_history_injection: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct CheckpointCaps {
    pub app_can_checkpoint_filesystem: bool,
    pub nested_scopes: bool,
    pub provider_can_rollback_conversation: bool,
    pub provider_rollback_returns_snapshot: bool,
    pub provider_can_read_conversation_snapshot: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct IdentityCaps {
    pub thread_ids: RefStrength,
    pub turn_ids: RefStrength,
    pub item_ids: RefStrength,
    pub request_ids: RefStrength,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct RuntimePolicyCaps {
    pub enforcement: Enforcement,
    /// Modes the provider enforces itself. A stored mode outside this set runs
    /// as `RuntimeMode::Ask`, as in T3's `RuntimePolicy.ts`.
    pub supported_modes: Vec<RuntimeMode>,
    pub plan_mode_toggle: bool,
}

open_enum! {
    #[derive(Default)]
    pub enum Enforcement {
        fallback #[default] ClientBoundary = "client_boundary";
        Native = "native",
    }
}

open_enum! {
    #[derive(Default)]
    pub enum Quality {
        fallback #[default] None = "none";
        Strong = "strong",
        Weak = "weak",
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct ExtraCaps {
    pub native_image_input: bool,
    pub image_mime_types: Vec<String>,
    pub goals: bool,
    pub compaction: bool,
    pub background_work: bool,
    pub account_usage_stream: bool,
    pub agent_presets: bool,
    pub computer_use: bool,
    pub model_discovery: bool,
}
