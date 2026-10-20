//! Running effects: the executor trait, ports and the default executor.
//!
//! Effects never touch the database directly. They report back as internal
//! commands with derived ids `effect:<effect_id>:<n>` (R21), so a replay-safe
//! effect that re-runs after a crash re-dispatches the same ids and the
//! receipts turn those into no-ops.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use waku_protocol::orchestration::*;

use super::Effect;
use crate::command::{CapturedCheckpoint, InternalCommand, PreparePhase, PreparedWorkspace};
use crate::decide::checkpoint_ref;
use crate::orchestrator::{DispatchError, DispatchReceipt, Orchestrator};
use crate::projection::sql;

/// A cancellation signal raced against an effect. Executors poll it (or wait
/// on it) at their own cancellation points.
#[derive(Debug, Default)]
pub struct CancelToken {
    cancelled: AtomicBool,
    lock: Mutex<()>,
    signal: Condvar,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let _guard = self.lock.lock();
        self.signal.notify_all();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Waits up to `timeout`; returns whether the token was cancelled.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let mut guard = self.lock.lock();
        if self.is_cancelled() {
            return true;
        }
        self.signal.wait_for(&mut guard, timeout);
        self.is_cancelled()
    }
}

/// A claimed outbox row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedEffect {
    pub id: EffectId,
    pub command_id: Option<CommandId>,
    pub thread_id: Option<ThreadId>,
    /// 1 on the first try.
    pub attempt: u32,
    /// Whether a `Retryable` error will be retried.
    pub will_retry: bool,
    pub effect: Effect,
}

pub struct EffectContext<'a> {
    pub orchestrator: &'a Orchestrator,
    pub cancel: &'a CancelToken,
    pub now: Timestamp,
    pub effect_id: EffectId,
}

impl EffectContext<'_> {
    /// Dispatches an effect result with command id `effect:<effect_id>:<n>`
    /// (R21).
    pub fn dispatch(
        &self,
        n: u8,
        command: InternalCommand,
    ) -> Result<DispatchReceipt, DispatchError> {
        let command_id = CommandId::new(format!("effect:{}:{n}", self.effect_id))
            .expect("effect command ids are valid");
        self.orchestrator.dispatch_internal(command_id, command)
    }

    /// Like `dispatch`, but a rejection means the result no longer applies
    /// (the run moved on), which settles the effect; storage errors retry.
    pub fn report(&self, n: u8, command: InternalCommand) -> Result<(), EffectError> {
        match self.dispatch(n, command) {
            Ok(_) | Err(DispatchError::Rejected(_)) => Ok(()),
            Err(DispatchError::ShuttingDown) => Err(EffectError::Cancelled),
            Err(DispatchError::Storage(error)) => Err(EffectError::Retryable(error)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectError {
    /// Retried with backoff until `max_attempts`.
    Retryable(String),
    /// Settles as `failed` at once.
    Permanent(String),
    Cancelled,
}

pub trait EffectExecutor: Send + Sync {
    fn execute(&self, effect: &ClaimedEffect, cx: &EffectContext<'_>) -> Result<(), EffectError>;
}

/// A port failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortError {
    pub message: String,
    pub retryable: bool,
}

impl PortError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }
}

fn port_error(error: PortError) -> EffectError {
    if error.retryable {
        EffectError::Retryable(error.message)
    } else {
        EffectError::Permanent(error.message)
    }
}

/// Worktrees. Every method is idempotent: preparing an existing worktree
/// returns it.
pub trait WorkspacePort: Send + Sync {
    fn prepare(
        &self,
        project_root: &Path,
        strategy: &WorkspaceStrategy,
        progress: &dyn Fn(PreparePhase),
    ) -> Result<PreparedWorkspace, PortError>;
    fn remove_worktree(&self, path: &Path, branch: Option<&str>) -> Result<(), PortError>;
}

/// Hidden git refs. Capturing an existing ref returns it.
pub trait CheckpointPort: Send + Sync {
    fn capture(
        &self,
        cwd: &Path,
        git_ref: &str,
        base_ref: Option<&str>,
    ) -> Result<CapturedCheckpoint, PortError>;
    fn restore(&self, cwd: &Path, git_ref: &str) -> Result<(), PortError>;
}

pub trait TitlePort: Send + Sync {
    fn generate(
        &self,
        selection: &ModelSelection,
        prompt: &str,
    ) -> Result<Option<String>, PortError>;
}

pub trait CleanupPort: Send + Sync {
    fn terminals(&self, thread_id: ThreadId) -> Result<(), PortError>;
    fn attachments(&self, ids: &[String]) -> Result<(), PortError>;
}

/// What `waku-core` implements for the default executor.
#[derive(Clone)]
pub struct Ports {
    pub workspace: Arc<dyn WorkspacePort>,
    pub checkpoints: Arc<dyn CheckpointPort>,
    pub titles: Arc<dyn TitlePort>,
    pub cleanup: Arc<dyn CleanupPort>,
}

/// Runs workspace, checkpoint, title and cleanup effects through [`Ports`]
/// and routes provider-control effects (`provider_*`,
/// `runtime_request.respond`, `delegated_tasks.stop`) to `provider`, the
/// `ProviderSessionManager` of #44.
pub struct DefaultExecutor {
    pub provider: Arc<dyn EffectExecutor>,
    pub ports: Ports,
}

impl DefaultExecutor {
    fn thread(
        &self,
        effect: &ClaimedEffect,
        cx: &EffectContext<'_>,
    ) -> Result<Arc<crate::projection::ThreadProjection>, EffectError> {
        let thread_id = effect
            .thread_id
            .ok_or_else(|| EffectError::Permanent("effect has no thread".to_owned()))?;
        cx.orchestrator
            .thread(thread_id)
            .map_err(|error| match error {
                DispatchError::Storage(error) => EffectError::Retryable(error),
                other => EffectError::Permanent(other.to_string()),
            })
    }
}

impl EffectExecutor for DefaultExecutor {
    fn execute(&self, claimed: &ClaimedEffect, cx: &EffectContext<'_>) -> Result<(), EffectError> {
        match &claimed.effect {
            Effect::ProviderTurnStart { .. }
            | Effect::ProviderTurnSteer { .. }
            | Effect::ProviderTurnInterrupt { .. }
            | Effect::ProviderTurnRestart { .. }
            | Effect::RuntimeRequestRespond { .. }
            | Effect::ProviderThreadRollback { .. }
            | Effect::ProviderSessionDetach { .. }
            | Effect::ProviderRuntimeContinue { .. }
            | Effect::DelegatedTasksStop { .. } => self.provider.execute(claimed, cx),
            Effect::WorkspacePrepare { run_id, strategy } => {
                let thread = self.thread(claimed, cx)?;
                let root = cx
                    .orchestrator
                    .project(thread.thread.project_id)
                    .map(|project| project.path)
                    .unwrap_or_default();
                let thread_id = thread.id();
                let progress = |phase: PreparePhase| {
                    let n = match phase {
                        PreparePhase::Worktree => 1,
                        PreparePhase::Setup => 2,
                    };
                    let _ = cx.report(
                        n,
                        InternalCommand::PreparedRunProgress {
                            thread_id,
                            run_id: *run_id,
                            phase,
                        },
                    );
                };
                match self.ports.workspace.prepare(&root, strategy, &progress) {
                    Ok(workspace) => cx.report(
                        3,
                        InternalCommand::PreparedRunRelease {
                            thread_id,
                            run_id: *run_id,
                            workspace,
                        },
                    ),
                    Err(error) if error.retryable && claimed.will_retry => {
                        Err(EffectError::Retryable(error.message))
                    }
                    Err(error) => cx.report(
                        4,
                        InternalCommand::PreparedRunFail {
                            thread_id,
                            run_id: *run_id,
                            failure: ProviderFailure::new(
                                FailureClass::ProviderError,
                                error.message,
                            )
                            .with_code("workspace_preparation_failed"),
                        },
                    ),
                }
            }
            Effect::CheckpointCapture { run_id, scope_id } => {
                let thread = self.thread(claimed, cx)?;
                let Some(scope) = thread.checkpoint_scopes.get(scope_id) else {
                    return Err(EffectError::Permanent(format!(
                        "scope {scope_id} not found"
                    )));
                };
                let mut existing = thread
                    .checkpoints
                    .values()
                    .filter(|checkpoint| checkpoint.scope_id == *scope_id)
                    .collect::<Vec<_>>();
                existing.sort_by_key(|checkpoint| checkpoint.ordinal_within_scope);
                let git_ref = checkpoint_ref(thread.id(), *scope_id, existing.len() as u32);
                let base = existing
                    .last()
                    .map(|checkpoint| checkpoint.git_ref.as_str());
                let result = match self.ports.checkpoints.capture(&scope.cwd, &git_ref, base) {
                    Ok(captured) => Ok(captured),
                    Err(error) if error.retryable && claimed.will_retry => {
                        return Err(EffectError::Retryable(error.message));
                    }
                    Err(error) => Err(error.message),
                };
                cx.report(
                    1,
                    InternalCommand::CheckpointCaptured {
                        thread_id: thread.id(),
                        run_id: *run_id,
                        scope_id: *scope_id,
                        result,
                    },
                )
            }
            Effect::ThreadTitleGenerate { request_id, kind } => {
                let thread = self.thread(claimed, cx)?;
                let message = cx
                    .orchestrator
                    .reader()
                    .with(|connection| match kind {
                        super::TitleKind::Initial { message_id } => {
                            sql::load_message(connection, *message_id)
                        }
                        super::TitleKind::Regenerate => {
                            sql::first_user_message(connection, thread.id())
                        }
                    })
                    .map_err(|error| EffectError::Retryable(error.to_string()))?;
                let title = match message {
                    Some(message) => self
                        .ports
                        .titles
                        .generate(&thread.thread.model_selection, &message.text)
                        .map_err(port_error)?,
                    None => None,
                };
                cx.report(
                    1,
                    InternalCommand::TitleGenerated {
                        thread_id: thread.id(),
                        request_id: request_id.clone(),
                        title,
                    },
                )
            }
            Effect::TerminalCleanup => {
                let thread_id = claimed
                    .thread_id
                    .ok_or_else(|| EffectError::Permanent("effect has no thread".to_owned()))?;
                self.ports.cleanup.terminals(thread_id).map_err(port_error)
            }
            Effect::AttachmentCleanup { attachment_ids } => self
                .ports
                .cleanup
                .attachments(attachment_ids)
                .map_err(port_error),
            Effect::WorktreeCleanup { path, branch } => self
                .ports
                .workspace
                .remove_worktree(path, branch.as_deref())
                .map_err(port_error),
        }
    }
}
