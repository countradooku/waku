//! What provider drivers hand the orchestrator (RFC 0002 "Normalized events").
//!
//! Drivers (`waku-core::driver::v2`, RFC 0002) emit [`AdapterEvent`]s: entity
//! snapshots keyed by app ids, plus exactly one [`TurnTerminal`] per accepted
//! turn. The `ProviderSessionManager` wraps each one in an [`IngestEnvelope`]
//! and `decide_ingest` turns it into domain events under a write guard.
//!
//! These types live here rather than next to the trait because `decide_ingest`
//! reads them and this crate has no async runtime (R9); `waku-core` re-exports
//! them from `driver::v2`.

use serde::{Deserialize, Serialize};
use waku_protocol::orchestration::*;
use waku_protocol::usage::PlanUsage;

/// Bounded adapter-event channel capacity: a slow sink slows the read of the
/// provider's pipe instead of growing memory (RFC 0002 rule 6).
pub const ADAPTER_EVENT_CAPACITY: usize = 1024;

/// The run a provider turn serves. Drivers copy it from `TurnStart` into
/// every snapshot they emit for that turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRef {
    pub thread_id: ThreadId,
    pub run_id: RunId,
    pub attempt_id: RunAttemptId,
    pub root_node_id: NodeId,
}

/// A provider-native id and how far it can be trusted.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeRef {
    pub id: String,
    #[serde(default)]
    pub strength: RefStrength,
}

impl NativeRef {
    pub fn strong(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            strength: RefStrength::Strong,
        }
    }

    pub fn weak(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            strength: RefStrength::Weak,
        }
    }

    /// The equivalent persisted reference.
    pub fn to_provider_ref(&self, driver: DriverKind) -> ProviderRef {
        ProviderRef {
            driver,
            native_id: (!self.id.is_empty()).then(|| self.id.clone()),
            strength: self.strength,
            fingerprint: None,
            ordinal: None,
        }
    }
}

/// A provider turn: its app id, native id and 1-based ordinal within its
/// provider thread.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderTurnRef {
    pub provider_turn_id: ProviderTurnId,
    pub native: NativeRef,
    pub ordinal: u32,
}

/// Work that outlives a turn (processes and monitors). A subagent is a
/// [`AdapterEvent::SubagentUpdated`] instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundWorkItem {
    pub provider_thread_id: ProviderThreadId,
    pub task: PendingBackgroundTask,
    /// `false` once the work finished, failed or was stopped.
    pub live: bool,
}

/// How a turn ended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TurnOutcome {
    Completed,
    /// The user interrupted and the provider honoured it.
    Interrupted,
    /// The turn never ran or was superseded (an interrupt before the provider
    /// registered it, an interrupt-restart attempt).
    Cancelled,
    Failed {
        failure: ProviderFailure,
    },
}

/// Whether the provider thread can take another turn.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadDisposition {
    #[default]
    Reusable,
    Broken,
}

/// The end of one accepted turn. Exactly one per `start_turn` that resolved
/// `Ok`, on every path (RFC 0002 "Exactly one terminal").
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnTerminal {
    pub run: RunRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_turn: Option<ProviderTurnRef>,
    pub outcome: TurnOutcome,
    #[serde(default)]
    pub thread_disposition: ThreadDisposition,
    /// Subagent turns started by this turn keep running.
    #[serde(default)]
    pub child_turns_continue: bool,
    /// Detached work will wake the session later (today's `TurnParked`).
    #[serde(default)]
    pub background_pending: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// A normalized provider event: an entity snapshot keyed by app ids, or the
/// turn's terminal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdapterEvent {
    /// Status, commands, model, agent preset, last error.
    SessionUpdated {
        session: ProviderSession,
    },
    ProviderThreadUpdated {
        thread: ProviderThread,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retained_native_turn_ids: Option<Vec<String>>,
    },
    /// Status, origin and token or context usage.
    ProviderTurnUpdated {
        turn: ProviderTurn,
    },
    NodeUpdated {
        node: ExecutionNode,
    },
    SubagentUpdated {
        subagent: Subagent,
    },
    MessageUpdated {
        message: Message,
    },
    /// `position` is assigned by `decide_ingest` (R15); drivers leave it 0.
    TurnItemUpdated {
        item: TurnItem,
    },
    /// Never persisted. The session manager routes it to the output hub
    /// (RFC 0003) and it never reaches `ingest`.
    CommandOutputDelta {
        item: TurnItemId,
        chunk: Vec<u8>,
        replace: bool,
    },
    /// Approvals, structured questions, dynamic tool calls, auth refreshes.
    RuntimeRequestUpdated {
        request: RuntimeRequest,
    },
    /// Todo lists and proposed plans.
    PlanUpdated {
        plan: Plan,
    },
    /// Processes and monitors that outlive a turn (Waku addition).
    BackgroundWorkUpdated {
        work: BackgroundWorkItem,
    },
    /// The provider-persisted goal of the active provider thread (Waku addition).
    GoalUpdated {
        goal: Option<ThreadGoal>,
    },
    /// Instance-level rate-limit meters (Waku addition). Not thread state: the
    /// session manager fans it out and never ingests it.
    AccountUsageUpdated {
        usage: PlanUsage,
    },
    TurnTerminal {
        terminal: TurnTerminal,
    },
}

impl AdapterEvent {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::SessionUpdated { .. } => "session_updated",
            Self::ProviderThreadUpdated { .. } => "provider_thread_updated",
            Self::ProviderTurnUpdated { .. } => "provider_turn_updated",
            Self::NodeUpdated { .. } => "node_updated",
            Self::SubagentUpdated { .. } => "subagent_updated",
            Self::MessageUpdated { .. } => "message_updated",
            Self::TurnItemUpdated { .. } => "turn_item_updated",
            Self::CommandOutputDelta { .. } => "command_output_delta",
            Self::RuntimeRequestUpdated { .. } => "runtime_request_updated",
            Self::PlanUpdated { .. } => "plan_updated",
            Self::BackgroundWorkUpdated { .. } => "background_work_updated",
            Self::GoalUpdated { .. } => "goal_updated",
            Self::AccountUsageUpdated { .. } => "account_usage_updated",
            Self::TurnTerminal { .. } => "turn_terminal",
        }
    }

    /// Whether the event is thread state that `ingest` persists.
    pub const fn is_persisted(&self) -> bool {
        !matches!(
            self,
            Self::CommandOutputDelta { .. } | Self::AccountUsageUpdated { .. }
        )
    }
}

/// One adapter event on its way into `decide_ingest`.
#[derive(Clone, Debug, PartialEq)]
pub struct IngestEnvelope {
    pub thread_id: ThreadId,
    pub provider_session_id: ProviderSessionId,
    /// Bumped on every reopen; the session manager drops stale generations
    /// before they get here.
    pub session_generation: u64,
    pub run_id: Option<RunId>,
    pub attempt_id: Option<RunAttemptId>,
    pub event: AdapterEvent,
}

impl IngestEnvelope {
    /// An envelope for an event scoped to `run`.
    pub fn for_run(
        provider_session_id: ProviderSessionId,
        session_generation: u64,
        run: &RunRef,
        event: AdapterEvent,
    ) -> Self {
        Self {
            thread_id: run.thread_id,
            provider_session_id,
            session_generation,
            run_id: Some(run.run_id),
            attempt_id: Some(run.attempt_id),
            event,
        }
    }

    /// An envelope for a session- or thread-level event.
    pub fn for_thread(
        thread_id: ThreadId,
        provider_session_id: ProviderSessionId,
        session_generation: u64,
        event: AdapterEvent,
    ) -> Self {
        Self {
            thread_id,
            provider_session_id,
            session_generation,
            run_id: None,
            attempt_id: None,
            event,
        }
    }
}

/// Derived ids `decide_ingest` uses for rows it creates on a driver's behalf,
/// so a driver that wants to carry its own display data (an approval prompt)
/// can emit a `TurnItemUpdated` with the same id.
pub mod derived {
    use waku_protocol::orchestration::*;

    use crate::ids::derived_id;

    pub fn request_node(request: RuntimeRequestId) -> NodeId {
        NodeId(derived_id("request_node", &request.to_string()))
    }

    pub fn request_item(request: RuntimeRequestId) -> TurnItemId {
        TurnItemId(derived_id("request_item", &request.to_string()))
    }

    pub fn plan_item(plan: PlanId) -> TurnItemId {
        TurnItemId(derived_id("plan_item", &plan.to_string()))
    }

    pub fn subagent_item(subagent: NodeId) -> TurnItemId {
        TurnItemId(derived_id("subagent_item", &subagent.to_string()))
    }
}
