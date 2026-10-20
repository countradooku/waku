//! `decide`: commands to events and effects, without I/O (RFC 0001 rule 1).
//!
//! Everything this module reads arrives through [`DecideContext`]: the
//! projection, the clock value, the id source and the capability lookup. It
//! imports no storage, filesystem or network code; `tests::purity` checks the
//! `use` lines of every file here.

mod draft;
mod internal;
mod project;
mod request;
mod run;
mod thread;

use std::time::Duration;

use waku_protocol::orchestration::*;

use crate::command::{AnyCommand, CommandError, InternalCommand};
use crate::effects::{Effect, EffectType};
use crate::ids::IdSource;
use crate::projection::ThreadProjection;

pub(crate) use draft::Draft;
pub use internal::checkpoint_ref;
pub use thread::PLACEHOLDER_TITLE;

/// What a command decided: events to append, effects to enqueue and
/// unsettled effects to cancel, all committed in one transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Decision {
    pub events: Vec<DomainEvent>,
    pub effects: Vec<Effect>,
    pub cancel_unsettled: Vec<CancelUnsettled>,
}

impl Decision {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.effects.is_empty() && self.cancel_unsettled.is_empty()
    }
}

/// Cancels pending and running outbox rows of one thread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelUnsettled {
    pub thread_id: ThreadId,
    pub effect_types: Vec<EffectType>,
    pub reason: String,
}

/// Static provider capabilities, used before a session exists.
pub trait CapabilitySource: Send + Sync {
    fn for_instance(&self, instance: &ProviderInstanceId) -> Option<Capabilities>;
}

/// Capabilities that answer "nothing supported" for every instance.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCapabilities;

impl CapabilitySource for NoCapabilities {
    fn for_instance(&self, _instance: &ProviderInstanceId) -> Option<Capabilities> {
        None
    }
}

/// Tunables `decide` and the orchestrator read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrchestratorSettings {
    pub continue_after_restart: bool,
    pub max_restart_continuations: u8,
    /// R12.
    pub text_persist_interval: Duration,
    /// R12: 256 KiB.
    pub stored_output_cap: usize,
}

impl Default for OrchestratorSettings {
    fn default() -> Self {
        Self {
            continue_after_restart: true,
            max_restart_continuations: 3,
            text_persist_interval: Duration::from_millis(250),
            stored_output_cap: 256 * 1024,
        }
    }
}

/// Everything `decide` may read.
pub struct DecideContext<'a> {
    pub now: Timestamp,
    pub ids: &'a mut dyn IdSource,
    pub capabilities: &'a dyn CapabilitySource,
    pub settings: &'a OrchestratorSettings,
    /// The command's thread; `None` only for creates.
    pub thread: Option<&'a ThreadProjection>,
    /// A fork source or delegated parent, read-only.
    pub related: Option<&'a ThreadProjection>,
    /// The command's project, or the thread's project.
    pub project: Option<&'a Project>,
}

/// Decides a command against its context. Pure: no I/O is reachable.
pub fn decide(cx: &mut DecideContext<'_>, command: &AnyCommand) -> Result<Decision, CommandError> {
    match command {
        AnyCommand::Client(envelope) => decide_client(cx, envelope),
        AnyCommand::Internal {
            command_id,
            command,
        } => decide_internal(cx, command_id, command),
    }
}

fn decide_client(
    cx: &mut DecideContext<'_>,
    envelope: &CommandEnvelope,
) -> Result<Decision, CommandError> {
    use Command::*;
    let command = &envelope.command;
    match command {
        ProjectCreate { .. }
        | ProjectRename { .. }
        | ProjectReorder { .. }
        | ProjectRemove { .. } => project::decide(cx, command),
        ThreadCreate { .. } => thread::create(cx, envelope),
        QueuedRunReorder { .. } | QueuedRunEdit { .. } | QueuedRunPromoteToSteer { .. } => Err(
            CommandError::unsupported(format!("{} (Phase 3)", command.command_type())),
        ),
        ThreadFork { .. } | ThreadMergeBack { .. } | ProviderSwitch { .. } => Err(
            CommandError::unsupported(format!("{} (Phase 3)", command.command_type())),
        ),
        _ => {
            let projection = existing_thread(cx, command)?;
            let project_path = cx.project.map(|project| project.path.clone());
            let mut draft = Draft::new(projection, cx.now, cx.ids, project_path);
            match command {
                MessageDispatch { .. }
                | RunInterrupt { .. }
                | PreparedRunRetry { .. }
                | QueueResume { .. }
                | QueuedRunCancel { .. } => {
                    run::decide_client(&mut draft, cx.capabilities, envelope)?
                }
                RuntimeRequestRespond { .. }
                | UserInputDismiss { .. }
                | SubagentStop { .. }
                | CheckpointRollback { .. }
                | ProviderSessionDetach { .. } => request::decide_client(&mut draft, envelope)?,
                _ => thread::decide_lifecycle(&mut draft, envelope)?,
            }
            Ok(draft.finish())
        }
    }
}

fn existing_thread<'a>(
    cx: &DecideContext<'a>,
    command: &Command,
) -> Result<&'a ThreadProjection, CommandError> {
    let target = match command.target() {
        CommandTarget::Thread(id) => id,
        CommandTarget::Project(id) => return Err(CommandError::not_found("thread", id)),
    };
    match cx.thread {
        Some(projection) if projection.id() == target => Ok(projection),
        _ => Err(CommandError::not_found("thread", target)),
    }
}

fn decide_internal(
    cx: &mut DecideContext<'_>,
    command_id: &CommandId,
    command: &InternalCommand,
) -> Result<Decision, CommandError> {
    match command {
        InternalCommand::ImportLegacyThread { shell } => internal::import_thread(cx, shell),
        InternalCommand::RecoverThread { .. } | InternalCommand::ContinueRun { .. } => Err(
            CommandError::unsupported(format!("{} (#59)", command.command_type())),
        ),
        InternalCommand::ImportLegacyTranscript { .. } => Err(CommandError::unsupported(
            "import.transcript (#63)".to_owned(),
        )),
        _ => {
            let thread_id = command.thread_id();
            let projection = match cx.thread {
                Some(projection) if projection.id() == thread_id => projection,
                _ => return Err(CommandError::not_found("thread", thread_id)),
            };
            let project_path = cx.project.map(|project| project.path.clone());
            let mut draft = Draft::new(projection, cx.now, cx.ids, project_path);
            internal::decide(&mut draft, command_id, command)?;
            Ok(draft.finish())
        }
    }
}

/// Truncates to at most `max` characters on a char boundary.
pub(crate) fn truncate_chars(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((end, _)) => text[..end].to_owned(),
        None => text.to_owned(),
    }
}

#[cfg(test)]
mod tests;
