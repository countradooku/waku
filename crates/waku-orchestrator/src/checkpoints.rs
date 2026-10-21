//! Which checkpoints a turn or thread diff compares (`threads.turnDiff`,
//! `threads.fullDiff`, RFC 0003 §5.2), and which one rewinding a run's
//! message restores.
//!
//! A run's root scope holds two checkpoints: ordinal 0, captured before its
//! provider turn starts (the turn-start checkpoint), and the one the run
//! ended with, whose parent is the turn-start one. A turn's diff is the
//! second against the first, so edits made between turns belong to neither;
//! a run whose turn start is missing falls back to the previous live run's
//! ending checkpoint.

use std::path::Path;

use waku_protocol::orchestration::*;

use crate::projection::ThreadProjection;

/// Two checkpoints to diff, in the working tree they were captured in.
#[derive(Debug)]
pub struct CheckpointRange<'a> {
    pub cwd: &'a Path,
    pub from: &'a Checkpoint,
    pub to: &'a Checkpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeError {
    RunNotFound,
    /// The run has not ended with a ready checkpoint (still running, not in
    /// a repository, or the capture failed).
    NotCaptured,
    /// Neither the run's turn-start checkpoint nor an earlier run's is
    /// ready.
    StartUnavailable,
    /// No live run has ended with a ready checkpoint.
    NoTurns,
}

impl std::fmt::Display for RangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::RunNotFound => "no such run",
            Self::NotCaptured => "the run has no checkpoint to diff",
            Self::StartUnavailable => "the turn's starting checkpoint is unavailable",
            Self::NoTurns => "no completed turn has a checkpoint",
        })
    }
}

impl std::error::Error for RangeError {}

fn ready<'a>(projection: &'a ThreadProjection, id: Option<CheckpointId>) -> Option<&'a Checkpoint> {
    projection
        .checkpoints
        .get(&id?)
        .filter(|checkpoint| checkpoint.status == CheckpointStatus::Ready)
}

/// Runs that took an ordinal and were not rolled back, by ordinal.
fn live_runs(projection: &ThreadProjection) -> Vec<&Run> {
    let mut runs = projection
        .runs
        .values()
        .filter(|run| run.ordinal.is_some() && run.status != RunStatus::RolledBack)
        .collect::<Vec<_>>();
    runs.sort_by_key(|run| run.ordinal);
    runs
}

/// The ready checkpoint `run` started from: ordinal 0 of its root scope.
pub fn turn_start<'a>(projection: &'a ThreadProjection, run: &Run) -> Option<&'a Checkpoint> {
    projection.checkpoints.values().find(|checkpoint| {
        checkpoint.run_id == Some(run.id)
            && Some(checkpoint.id) != run.checkpoint_id
            && checkpoint.ordinal_within_scope == 0
            && checkpoint.status == CheckpointStatus::Ready
            && projection
                .checkpoint_scopes
                .get(&checkpoint.scope_id)
                .is_some_and(|scope| scope.kind == ScopeKind::RootRun)
    })
}

/// The ready checkpoint the live run before `run` ended with.
fn previous_end<'a>(projection: &'a ThreadProjection, run: &Run) -> Option<&'a Checkpoint> {
    let ordinal = run.ordinal?;
    live_runs(projection)
        .into_iter()
        .rev()
        .filter(|earlier| earlier.ordinal.is_some_and(|earlier| earlier < ordinal))
        .find_map(|earlier| ready(projection, earlier.checkpoint_id))
}

/// Where `run`'s changes start: its turn-start checkpoint, else the
/// previous live run's ending one.
fn start_of<'a>(projection: &'a ThreadProjection, run: &Run) -> Option<&'a Checkpoint> {
    let end = ready(projection, run.checkpoint_id);
    end.and_then(|end| ready(projection, end.parent_checkpoint_id))
        .or_else(|| turn_start(projection, run))
        .or_else(|| previous_end(projection, run))
}

/// What run `run_id` changed.
pub fn turn_range(
    projection: &ThreadProjection,
    run_id: RunId,
) -> Result<CheckpointRange<'_>, RangeError> {
    let run = projection
        .runs
        .get(&run_id)
        .ok_or(RangeError::RunNotFound)?;
    let to = ready(projection, run.checkpoint_id).ok_or(RangeError::NotCaptured)?;
    let scope = projection
        .checkpoint_scopes
        .get(&to.scope_id)
        .ok_or(RangeError::NotCaptured)?;
    let from = start_of(projection, run).ok_or(RangeError::StartUnavailable)?;
    Ok(CheckpointRange {
        cwd: &scope.cwd,
        from,
        to,
    })
}

/// What the thread's live runs changed: from before the first one to the
/// last one that ended with a checkpoint.
pub fn thread_range(projection: &ThreadProjection) -> Result<CheckpointRange<'_>, RangeError> {
    let runs = live_runs(projection);
    let to = runs
        .iter()
        .rev()
        .find_map(|run| ready(projection, run.checkpoint_id))
        .ok_or(RangeError::NoTurns)?;
    let scope = projection
        .checkpoint_scopes
        .get(&to.scope_id)
        .ok_or(RangeError::NoTurns)?;
    let from = runs
        .iter()
        .find_map(|run| start_of(projection, run))
        .ok_or(RangeError::StartUnavailable)?;
    Ok(CheckpointRange {
        cwd: &scope.cwd,
        from,
        to,
    })
}

/// The checkpoint that rewinding `run`'s message restores (the state just
/// before the run started), and that `checkpoint.rollback` takes: its
/// turn-start checkpoint, else the previous live run's ending one.
pub fn rewind_point<'a>(projection: &'a ThreadProjection, run: &Run) -> Option<&'a Checkpoint> {
    turn_start(projection, run)
        .filter(|checkpoint| checkpoint.app_run_ordinal.is_some())
        .or_else(|| previous_end(projection, run))
}
