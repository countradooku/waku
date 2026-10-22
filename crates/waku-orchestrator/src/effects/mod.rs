//! Side effects decided with the events and run after commit (RFC 0001
//! "Effects").

pub mod executor;
pub mod worker;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use waku_protocol::orchestration::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum SessionTransition {
    Replace {
        replacement_provider_session_id: ProviderSessionId,
    },
    Detach,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum TitleKind {
    Initial { message_id: MessageId },
    Regenerate,
}

/// A subagent addressed by an interrupt: its node and native task id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentTarget {
    pub node_id: NodeId,
    pub native_task_id: String,
}

/// One outbox row's payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum Effect {
    #[serde(rename = "workspace.prepare")]
    WorkspacePrepare {
        run_id: RunId,
        strategy: WorkspaceStrategy,
    },
    #[serde(rename = "provider_turn.start")]
    ProviderTurnStart {
        run_id: RunId,
        attempt_id: RunAttemptId,
    },
    #[serde(rename = "provider_turn.steer")]
    ProviderTurnSteer {
        provider_session_id: ProviderSessionId,
        provider_thread_id: ProviderThreadId,
        provider_turn_id: ProviderTurnId,
        message_id: MessageId,
    },
    #[serde(rename = "provider_turn.interrupt")]
    ProviderTurnInterrupt {
        provider_session_id: ProviderSessionId,
        provider_thread_id: ProviderThreadId,
        provider_turn_id: ProviderTurnId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent: Option<SubagentTarget>,
    },
    #[serde(rename = "provider_turn.restart")]
    ProviderTurnRestart {
        run_id: RunId,
        interrupted_attempt_id: RunAttemptId,
        next_attempt_id: RunAttemptId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_transition: Option<SessionTransition>,
    },
    #[serde(rename = "runtime_request.respond")]
    RuntimeRequestRespond {
        provider_session_id: ProviderSessionId,
        request_id: RuntimeRequestId,
        response: RuntimeResponse,
    },
    #[serde(rename = "provider_thread.rollback")]
    ProviderThreadRollback {
        provider_thread_id: ProviderThreadId,
        scope_id: CheckpointScopeId,
        checkpoint_id: CheckpointId,
        restore_files: bool,
        request_id: CommandId,
    },
    #[serde(rename = "checkpoint.capture")]
    CheckpointCapture {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<RunId>,
        scope_id: CheckpointScopeId,
        /// The turn-start checkpoint, captured before the run's provider
        /// turn starts (ordinal 0 of its scope); otherwise the checkpoint
        /// that ends the run.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        turn_start: bool,
    },
    #[serde(rename = "background_work.stop")]
    BackgroundWorkStop {
        provider_session_id: ProviderSessionId,
        provider_thread_id: ProviderThreadId,
        task_id: String,
    },
    #[serde(rename = "background_work.refresh")]
    BackgroundWorkRefresh {
        provider_session_id: ProviderSessionId,
    },
    #[serde(rename = "provider_session.detach")]
    ProviderSessionDetach {
        provider_session_id: ProviderSessionId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
    #[serde(rename = "provider_runtime.continue")]
    ProviderRuntimeContinue {
        source_run_id: RunId,
        chain_length: u8,
    },
    #[serde(rename = "thread_title.generate")]
    ThreadTitleGenerate {
        request_id: CommandId,
        kind: TitleKind,
    },
    #[serde(rename = "terminal.cleanup")]
    TerminalCleanup,
    #[serde(rename = "attachment.cleanup")]
    AttachmentCleanup { attachment_ids: Vec<String> },
    #[serde(rename = "worktree.cleanup")]
    WorktreeCleanup {
        path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },
    #[serde(rename = "delegated_tasks.stop")]
    DelegatedTasksStop {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

/// The fieldless mirror of [`Effect`]; its tag is `effect_outbox.effect_type`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EffectType {
    WorkspacePrepare,
    ProviderTurnStart,
    ProviderTurnSteer,
    ProviderTurnInterrupt,
    ProviderTurnRestart,
    RuntimeRequestRespond,
    ProviderThreadRollback,
    CheckpointCapture,
    ProviderSessionDetach,
    ProviderRuntimeContinue,
    ThreadTitleGenerate,
    TerminalCleanup,
    AttachmentCleanup,
    WorktreeCleanup,
    DelegatedTasksStop,
    BackgroundWorkStop,
    BackgroundWorkRefresh,
}

impl EffectType {
    pub const ALL: [Self; 17] = [
        Self::WorkspacePrepare,
        Self::ProviderTurnStart,
        Self::ProviderTurnSteer,
        Self::ProviderTurnInterrupt,
        Self::ProviderTurnRestart,
        Self::RuntimeRequestRespond,
        Self::ProviderThreadRollback,
        Self::CheckpointCapture,
        Self::ProviderSessionDetach,
        Self::ProviderRuntimeContinue,
        Self::ThreadTitleGenerate,
        Self::TerminalCleanup,
        Self::AttachmentCleanup,
        Self::WorktreeCleanup,
        Self::DelegatedTasksStop,
        Self::BackgroundWorkStop,
        Self::BackgroundWorkRefresh,
    ];

    /// The provider-control effects a stop or delete cancels.
    pub const PROVIDER_CONTROL: [Self; 6] = [
        Self::ProviderTurnStart,
        Self::ProviderTurnSteer,
        Self::ProviderTurnRestart,
        Self::RuntimeRequestRespond,
        Self::WorkspacePrepare,
        Self::ProviderRuntimeContinue,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkspacePrepare => "workspace.prepare",
            Self::ProviderTurnStart => "provider_turn.start",
            Self::ProviderTurnSteer => "provider_turn.steer",
            Self::ProviderTurnInterrupt => "provider_turn.interrupt",
            Self::ProviderTurnRestart => "provider_turn.restart",
            Self::RuntimeRequestRespond => "runtime_request.respond",
            Self::ProviderThreadRollback => "provider_thread.rollback",
            Self::CheckpointCapture => "checkpoint.capture",
            Self::ProviderSessionDetach => "provider_session.detach",
            Self::ProviderRuntimeContinue => "provider_runtime.continue",
            Self::ThreadTitleGenerate => "thread_title.generate",
            Self::TerminalCleanup => "terminal.cleanup",
            Self::AttachmentCleanup => "attachment.cleanup",
            Self::WorktreeCleanup => "worktree.cleanup",
            Self::DelegatedTasksStop => "delegated_tasks.stop",
            Self::BackgroundWorkStop => "background_work.stop",
            Self::BackgroundWorkRefresh => "background_work.refresh",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == text)
    }

    /// Process-bound effects talk to a live provider process; after a daemon
    /// crash they are cancelled, everything else is requeued (RFC 0001
    /// "Effects" table, T3's `PROCESS_BOUND_EFFECT_TYPES`).
    pub const fn process_bound(self) -> bool {
        matches!(
            self,
            Self::ProviderTurnStart
                | Self::ProviderTurnSteer
                | Self::ProviderTurnInterrupt
                | Self::ProviderTurnRestart
                | Self::RuntimeRequestRespond
                | Self::BackgroundWorkStop
                | Self::BackgroundWorkRefresh
        )
    }

    pub const fn lane(self) -> Lane {
        match self {
            Self::ThreadTitleGenerate => Lane::Title,
            _ => Lane::Thread,
        }
    }
}

/// An outbox lane. Effects are FIFO per (thread, lane); the title lane only
/// blocks other title effects, so title generation never delays a turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Lane {
    Thread,
    Title,
}

impl Lane {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Thread => "thread",
            Self::Title => "title",
        }
    }
}

impl Effect {
    pub const fn effect_type(&self) -> EffectType {
        match self {
            Self::WorkspacePrepare { .. } => EffectType::WorkspacePrepare,
            Self::ProviderTurnStart { .. } => EffectType::ProviderTurnStart,
            Self::ProviderTurnSteer { .. } => EffectType::ProviderTurnSteer,
            Self::ProviderTurnInterrupt { .. } => EffectType::ProviderTurnInterrupt,
            Self::ProviderTurnRestart { .. } => EffectType::ProviderTurnRestart,
            Self::RuntimeRequestRespond { .. } => EffectType::RuntimeRequestRespond,
            Self::ProviderThreadRollback { .. } => EffectType::ProviderThreadRollback,
            Self::CheckpointCapture { .. } => EffectType::CheckpointCapture,
            Self::ProviderSessionDetach { .. } => EffectType::ProviderSessionDetach,
            Self::ProviderRuntimeContinue { .. } => EffectType::ProviderRuntimeContinue,
            Self::ThreadTitleGenerate { .. } => EffectType::ThreadTitleGenerate,
            Self::TerminalCleanup => EffectType::TerminalCleanup,
            Self::AttachmentCleanup { .. } => EffectType::AttachmentCleanup,
            Self::WorktreeCleanup { .. } => EffectType::WorktreeCleanup,
            Self::DelegatedTasksStop { .. } => EffectType::DelegatedTasksStop,
            Self::BackgroundWorkStop { .. } => EffectType::BackgroundWorkStop,
            Self::BackgroundWorkRefresh { .. } => EffectType::BackgroundWorkRefresh,
        }
    }

    pub const fn lane(&self) -> Lane {
        self.effect_type().lane()
    }

    pub const fn process_bound(&self) -> bool {
        self.effect_type().process_bound()
    }
}
