//! Daemon-only commands, lock keys and command errors (RFC 0001 R17).

use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;
use waku_protocol::orchestration::*;

/// The phase a workspace preparation reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparePhase {
    Worktree,
    Setup,
}

/// What `WorkspacePort::prepare` produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedWorkspace {
    pub cwd: PathBuf,
    pub branch: Option<String>,
    pub worktree_path: Option<PathBuf>,
    /// Created by Waku, so deleting the thread removes it.
    pub owned: bool,
}

/// What `CheckpointPort::capture` produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedCheckpoint {
    pub git_ref: String,
    pub files: Vec<CheckpointFile>,
}

/// A provider conversation snapshot after a rollback (RFC 0002).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadSnapshot {
    pub thread: ProviderThread,
    pub turns: Vec<ProviderTurn>,
    pub messages: Vec<Message>,
    pub pending_requests: Vec<RuntimeRequest>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackgroundOutcome {
    Completed,
    Failed,
    Cancelled,
}

/// The shell of a thread read from a legacy `app.db` (#63).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyThreadShell {
    pub thread: AppThread,
    pub provider_thread: Option<ProviderThread>,
}

/// The transcript of a legacy thread (#63). Decided in Phase 2's import step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyTranscript {
    pub runs: Vec<Run>,
    pub messages: Vec<Message>,
}

/// Commands built only inside the daemon: effect results, lifecycle signals,
/// recovery and import.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InternalCommand {
    // Effect results (R21)
    PreparedRunProgress {
        thread_id: ThreadId,
        run_id: RunId,
        phase: PreparePhase,
    },
    PreparedRunRelease {
        thread_id: ThreadId,
        run_id: RunId,
        workspace: PreparedWorkspace,
    },
    PreparedRunFail {
        thread_id: ThreadId,
        run_id: RunId,
        failure: ProviderFailure,
    },
    ProviderTurnFailed {
        thread_id: ThreadId,
        run_id: RunId,
        attempt_id: RunAttemptId,
        failure: ProviderFailure,
    },
    CheckpointCaptured {
        thread_id: ThreadId,
        run_id: Option<RunId>,
        scope_id: CheckpointScopeId,
        result: Result<CapturedCheckpoint, String>,
    },
    CheckpointRollbackApplied {
        thread_id: ThreadId,
        request_id: CommandId,
        snapshot: Option<ThreadSnapshot>,
    },
    CheckpointRollbackFailed {
        thread_id: ThreadId,
        request_id: CommandId,
        message: String,
    },
    TitleGenerated {
        thread_id: ThreadId,
        request_id: CommandId,
        title: Option<String>,
    },
    // Process and session lifecycle
    SessionLost {
        thread_id: ThreadId,
        provider_session_id: ProviderSessionId,
        reason: String,
    },
    /// Interrupt, hold the queue and stop children.
    ThreadStop {
        thread_id: ThreadId,
        reason: Option<String>,
    },
    BackgroundWorkSettle {
        thread_id: ThreadId,
        provider_thread_id: ProviderThreadId,
        task_id: String,
        outcome: BackgroundOutcome,
    },
    /// From the scheduler tick.
    SnoozeWake {
        thread_id: ThreadId,
        at: Timestamp,
    },
    /// From the auto-settle service (#64).
    AutoSettle {
        thread_id: ThreadId,
        snapshot_at: Timestamp,
    },
    // Recovery (#59)
    RecoverThread {
        thread_id: ThreadId,
        boot_id: Uuid,
    },
    ContinueRun {
        thread_id: ThreadId,
        of_run_id: RunId,
        kind: ContinuationKind,
        message_id: MessageId,
        text: String,
    },
    // Import (#63)
    ImportLegacyThread {
        shell: LegacyThreadShell,
    },
    ImportLegacyTranscript {
        thread_id: ThreadId,
        transcript: LegacyTranscript,
    },
}

impl InternalCommand {
    /// Stored in `command_receipts.command_type`.
    pub const fn command_type(&self) -> &'static str {
        match self {
            Self::PreparedRunProgress { .. } => "prepared_run.progress",
            Self::PreparedRunRelease { .. } => "prepared_run.release",
            Self::PreparedRunFail { .. } => "prepared_run.fail",
            Self::ProviderTurnFailed { .. } => "provider_turn.failed",
            Self::CheckpointCaptured { .. } => "checkpoint.captured",
            Self::CheckpointRollbackApplied { .. } => "checkpoint.rollback.applied",
            Self::CheckpointRollbackFailed { .. } => "checkpoint.rollback.fail",
            Self::TitleGenerated { .. } => "thread.title.generated",
            Self::SessionLost { .. } => "provider_session.lost",
            Self::ThreadStop { .. } => "thread.stop",
            Self::BackgroundWorkSettle { .. } => "thread.background_work.settle",
            Self::SnoozeWake { .. } => "thread.snooze.wake",
            Self::AutoSettle { .. } => "thread.auto_settle",
            Self::RecoverThread { .. } => "thread.recover",
            Self::ContinueRun { .. } => "run.continue",
            Self::ImportLegacyThread { .. } => "import.thread",
            Self::ImportLegacyTranscript { .. } => "import.transcript",
        }
    }

    pub fn thread_id(&self) -> ThreadId {
        match self {
            Self::PreparedRunProgress { thread_id, .. }
            | Self::PreparedRunRelease { thread_id, .. }
            | Self::PreparedRunFail { thread_id, .. }
            | Self::ProviderTurnFailed { thread_id, .. }
            | Self::CheckpointCaptured { thread_id, .. }
            | Self::CheckpointRollbackApplied { thread_id, .. }
            | Self::CheckpointRollbackFailed { thread_id, .. }
            | Self::TitleGenerated { thread_id, .. }
            | Self::SessionLost { thread_id, .. }
            | Self::ThreadStop { thread_id, .. }
            | Self::BackgroundWorkSettle { thread_id, .. }
            | Self::SnoozeWake { thread_id, .. }
            | Self::AutoSettle { thread_id, .. }
            | Self::RecoverThread { thread_id, .. }
            | Self::ContinueRun { thread_id, .. }
            | Self::ImportLegacyTranscript { thread_id, .. } => *thread_id,
            Self::ImportLegacyThread { shell } => shell.thread.id,
        }
    }
}

/// A command from a client or from inside the daemon.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnyCommand {
    Client(CommandEnvelope),
    Internal {
        command_id: CommandId,
        command: InternalCommand,
    },
}

/// What a command's lock is keyed by (R8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LockKey {
    Project(ProjectId),
    Thread(ThreadId),
}

impl LockKey {
    pub fn thread_id(self) -> Option<ThreadId> {
        match self {
            Self::Thread(id) => Some(id),
            Self::Project(_) => None,
        }
    }

    pub fn project_id(self) -> Option<ProjectId> {
        match self {
            Self::Project(id) => Some(id),
            Self::Thread(_) => None,
        }
    }
}

impl AnyCommand {
    pub fn command_id(&self) -> &CommandId {
        match self {
            Self::Client(envelope) => &envelope.command_id,
            Self::Internal { command_id, .. } => command_id,
        }
    }

    /// Stored in `command_receipts.command_type`.
    pub fn command_type(&self) -> &'static str {
        match self {
            Self::Client(envelope) => envelope.command.command_type(),
            Self::Internal { command, .. } => command.command_type(),
        }
    }

    pub fn issued_by(&self) -> Actor {
        match self {
            Self::Client(envelope) => envelope.issued_by,
            Self::Internal { .. } => Actor::System,
        }
    }

    pub fn source(&self) -> CreationSource {
        match self {
            Self::Client(envelope) => envelope.source,
            Self::Internal { .. } => CreationSource::Server,
        }
    }

    /// The key the command's events are recorded under.
    pub fn primary_key(&self) -> LockKey {
        match self {
            Self::Client(envelope) => match envelope.command.target() {
                CommandTarget::Project(id) => LockKey::Project(id),
                CommandTarget::Thread(id) => LockKey::Thread(id),
            },
            Self::Internal { command, .. } => LockKey::Thread(command.thread_id()),
        }
    }

    /// Sorted lock keys: the primary key plus any thread the command creates,
    /// so two commands never take locks in opposite orders.
    pub fn lock_keys(&self) -> SmallVec<[LockKey; 2]> {
        let mut keys = SmallVec::new();
        keys.push(self.primary_key());
        if let Self::Client(envelope) = self
            && let Some(created) = envelope.command.created_thread()
        {
            keys.push(LockKey::Thread(created));
        }
        keys.sort();
        keys.dedup();
        keys
    }
}

/// Why a command was rejected. Stored as `command_receipts.error_json`, so a
/// retried command id returns the same error (R16).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandError {
    NotFound {
        entity: String,
        id: String,
    },
    /// E.g. "run is not active", "thread is archived".
    InvalidState {
        reason: String,
    },
    /// An `expected_*` guard failed.
    Conflict {
        reason: String,
    },
    /// (P3) commands and missing capabilities.
    Unsupported {
        capability: String,
    },
    Invalid {
        field: String,
        reason: String,
    },
}

impl CommandError {
    pub fn not_found(entity: &str, id: impl ToString) -> Self {
        Self::NotFound {
            entity: entity.to_owned(),
            id: id.to_string(),
        }
    }

    pub fn invalid_state(reason: impl Into<String>) -> Self {
        Self::InvalidState {
            reason: reason.into(),
        }
    }

    pub fn conflict(reason: impl Into<String>) -> Self {
        Self::Conflict {
            reason: reason.into(),
        }
    }

    pub fn unsupported(capability: impl Into<String>) -> Self {
        Self::Unsupported {
            capability: capability.into(),
        }
    }

    pub fn invalid(field: &str, reason: impl Into<String>) -> Self {
        Self::Invalid {
            field: field.to_owned(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { entity, id } => write!(formatter, "{entity} {id} not found"),
            Self::InvalidState { reason } => write!(formatter, "invalid state: {reason}"),
            Self::Conflict { reason } => write!(formatter, "conflict: {reason}"),
            Self::Unsupported { capability } => write!(formatter, "unsupported: {capability}"),
            Self::Invalid { field, reason } => write!(formatter, "invalid {field}: {reason}"),
        }
    }
}

impl std::error::Error for CommandError {}
