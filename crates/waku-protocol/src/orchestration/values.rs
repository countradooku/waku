//! Shared value types of the orchestration graph (RFC 0001 "Ids and shared values").

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::ids::*;
use crate::ts::TS;

fn is_false(value: &bool) -> bool {
    !*value
}

open_enum! {
    /// The driver family behind a provider instance.
    pub enum DriverKind {
        Amp = "amp",
        Claude = "claude",
        Codex = "codex",
        Cursor = "cursor",
        DeepSeek = "deep_seek",
        Fx = "fx",
        OpenCode = "open_code",
        Grok = "grok",
        Kimi = "kimi",
        OhMyPi = "oh_my_pi",
        Pi = "pi",
    }
}

impl DriverKind {
    /// Resolves a provider tag: the snake_case name above or the legacy
    /// camelCase `ProviderKind` tag (`"deepSeek"`, `"openCode"`, `"ohMyPi"`).
    pub fn from_tag(tag: &str) -> Self {
        match tag {
            "deepSeek" => Self::DeepSeek,
            "openCode" | "openCode2" => Self::OpenCode,
            "ohMyPi" => Self::OhMyPi,
            other => Self::ALL
                .iter()
                .copied()
                .find(|kind| kind.as_str() == other)
                .unwrap_or(Self::Unknown),
        }
    }

    pub fn from_instance(instance: &ProviderInstanceId) -> Self {
        Self::from_tag(instance.driver_tag())
    }
}

open_enum! {
    pub enum Actor {
        User = "user",
        Agent = "agent",
        System = "system",
    }
}

open_enum! {
    pub enum CreationSource {
        Desktop = "desktop",
        Web = "web",
        Mobile = "mobile",
        Mcp = "mcp",
        Provider = "provider",
        Server = "server",
        Import = "import",
    }
}

open_enum! {
    /// How far a provider id can be trusted (RFC 0002 `RefStrength`).
    #[derive(Default)]
    pub enum RefStrength {
        fallback #[default] None = "none";
        Strong = "strong",
        Weak = "weak",
    }
}

/// A provider-native id with its strength. App ids are primary; this is a
/// reference only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderRef {
    pub driver: DriverKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_id: Option<String>,
    #[serde(default)]
    pub strength: RefStrength,
    /// Fallback correlation only; never full assistant text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    /// Scoped ordinal for weak providers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<u32>,
}

impl ProviderRef {
    pub fn strong(driver: DriverKind, native_id: impl Into<String>) -> Self {
        Self {
            driver,
            native_id: Some(native_id.into()),
            strength: RefStrength::Strong,
            fingerprint: None,
            ordinal: None,
        }
    }
}

/// The provider, model and per-model options of a thread or run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ModelSelection {
    pub instance: ProviderInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_preset: Option<String>,
}

impl ModelSelection {
    pub fn new(instance: impl Into<String>) -> Self {
        Self {
            instance: ProviderInstanceId::new(instance),
            model: None,
            reasoning_effort: None,
            service_tier: None,
            context_window: None,
            agent_preset: None,
        }
    }
}

open_enum! {
    /// The runtime permission mode. Mirrors the legacy `model::RuntimeMode`,
    /// which rejects unknown values and so cannot be stored here.
    #[derive(Default)]
    pub enum RuntimeMode {
        /// Supervised: every tool asks (T3's `approval-required`).
        Ask = "ask",
        AutoAcceptEdits = "auto_accept_edits",
        Auto = "auto",
        #[default]
        FullAccess = "full_access",
    }
}

open_enum! {
    #[derive(Default)]
    pub enum InteractionMode {
        #[default]
        Default = "default",
        Plan = "plan",
    }
}

/// Where a thread's next run works.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum WorkspaceStrategy {
    Root {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },
    ExistingWorktree {
        path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },
    NewWorktree {
        base_ref: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        #[serde(default)]
        start_from_origin: bool,
    },
    #[serde(other)]
    Unknown,
}

open_enum! {
    pub enum FailureClass {
        UsageLimit = "usage_limit",
        ProviderError = "provider_error",
        TransportError = "transport_error",
        PermissionError = "permission_error",
        ValidationError = "validation_error",
    }
}

/// A normalized provider or preparation failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProviderFailure {
    pub class: FailureClass,
    /// At most 4096 bytes, redacted by the producer.
    pub message: String,
    /// At most 128 bytes; `"process_loss"` after a daemon crash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at: Option<Timestamp>,
}

impl ProviderFailure {
    pub const MAX_MESSAGE_BYTES: usize = 4096;

    pub fn new(class: FailureClass, message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.len() > Self::MAX_MESSAGE_BYTES {
            let mut end = Self::MAX_MESSAGE_BYTES;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
        }
        Self {
            class,
            message,
            code: None,
            retryable: None,
            reset_at: None,
        }
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }
}

/// Token accounting as the provider reports it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TokenUsage {
    pub used: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_output: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache_ttl_ms: Option<u64>,
    pub updated_at: Timestamp,
}

/// A file attached to a message. Mirrors `model::MessageAttachment` with
/// camelCase field names.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Attachment {
    /// Absolute path on the daemon host, handed to the provider.
    pub path: PathBuf,
    /// Provider-facing path text, relative to the workspace when possible.
    pub mention: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_dir: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_image: bool,
    /// Durable `waku-blob:` or `waku-attachment:` reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_reference: Option<String>,
}

/// A composer context record (a file, a selection, a terminal excerpt…).
/// The record body is opaque to the orchestrator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ContextRecord {
    pub context_id: String,
    /// Open string: `file`, `selection`, `image`, `terminal`, `mention`, …
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonText>,
}

/// One option of an approval request. `id` is the provider's own option id,
/// kept through normalization (RFC 0002).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PermissionOption {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub allow: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UserInputOption {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UserInputQuestion {
    pub id: String,
    pub header: String,
    pub question: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<UserInputOption>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub multi_select: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UserInputAnswer {
    pub question_id: String,
    pub answers: Vec<String>,
}

open_enum! {
    pub enum ThreadGoalStatus {
        Active = "active",
        Paused = "paused",
        Blocked = "blocked",
        UsageLimited = "usage_limited",
        BudgetLimited = "budget_limited",
        Complete = "complete",
    }
}

/// A provider-persisted objective (Codex and Claude `/goal`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ThreadGoal {
    pub objective: String,
    pub status: ThreadGoalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<i64>,
    #[serde(default)]
    pub tokens_used: i64,
    #[serde(default)]
    pub time_used_seconds: i64,
}

/// How full the provider's context window is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ContextUsage {
    pub tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<u64>,
}

/// Where a background-work notification came from. As in T3, an unknown
/// source decodes as `BackgroundTask` (RFC 0001 "Turn items").
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schema",
    schemars(extend("x-waku-fallback" = "background_task"))
)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum NotificationSource {
    DelegatedTask {
        #[serde(default)]
        task_ids: Vec<NodeId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        child_thread_id: Option<ThreadId>,
    },
    Subagent {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        child_thread_id: Option<ThreadId>,
    },
    Command,
    Monitor,
    System,
    #[serde(other)]
    BackgroundTask,
}

open_enum! {
    pub enum NotificationOutcome {
        Completed = "completed",
        Failed = "failed",
        Cancelled = "cancelled",
        Updated = "updated",
    }
}

/// A background-work notification carried by a message or a turn item.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Notification {
    pub source: NotificationSource,
    pub outcome: NotificationOutcome,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

open_enum! {
    pub enum PlanStepStatus {
        Pending = "pending",
        Running = "running",
        Completed = "completed",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PlanStep {
    pub id: String,
    pub text: String,
    pub status: PlanStepStatus,
}

/// One file of a checkpoint diff summary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CheckpointFile {
    pub path: String,
    /// Open string: `added`, `modified`, `deleted`, `renamed`, …
    pub kind: String,
    pub additions: u32,
    pub deletions: u32,
}

/// How a run continues an earlier one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RunContinuation {
    pub kind: ContinuationKind,
    pub of_run_id: RunId,
}

open_enum! {
    pub enum ContinuationKind {
        Restart = "restart",
        UsageLimit = "usage_limit",
        Manual = "manual",
    }
}

/// An inclusive range of app run ordinals.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RunOrdinalRange {
    pub first: u32,
    pub last: u32,
}

/// A user's answer to a runtime request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum RuntimeResponse {
    /// `option_id` is the provider's own option id, exactly as emitted.
    Approval {
        option_id: String,
    },
    UserInput {
        answers: Vec<UserInputAnswer>,
    },
    Dismiss,
    #[serde(other)]
    Unknown,
}
