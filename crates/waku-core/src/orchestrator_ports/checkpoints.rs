//! `CheckpointPort`: hidden refs under `refs/waku/`.

use std::path::Path;

use waku_orchestrator::command::CapturedCheckpoint;
use waku_orchestrator::{CheckpointPort, PortError};
use waku_protocol::orchestration::CheckpointFile;

use crate::checkpoint;

/// Captures the working tree (tracked and untracked files, through an
/// isolated index, never touching the user's) into a dangling commit under
/// the ref the orchestrator names, and restores files from such a ref.
///
/// Capturing a ref that exists returns it with its diff, so a requeued
/// capture never moves a checkpoint. Only refs under `refs/waku/` are
/// written.
#[derive(Clone, Copy, Debug, Default)]
pub struct GitCheckpointPort;

fn kind(status: char) -> &'static str {
    match status {
        'A' => "added",
        'D' => "deleted",
        'R' => "renamed",
        'C' => "copied",
        'T' => "type_changed",
        _ => "modified",
    }
}

fn saturate(count: u64) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

impl CheckpointPort for GitCheckpointPort {
    fn capture(
        &self,
        cwd: &Path,
        git_ref: &str,
        base_ref: Option<&str>,
    ) -> Result<CapturedCheckpoint, PortError> {
        if !git_ref.starts_with("refs/waku/") {
            return Err(PortError::permanent(format!(
                "checkpoint ref `{git_ref}` is outside refs/waku/"
            )));
        }
        if !cwd.is_dir() || !checkpoint::is_repository(cwd) {
            return Err(PortError::permanent(format!(
                "checkpoints require a Git repository ({})",
                cwd.display()
            )));
        }
        if !checkpoint::has_ref(cwd, git_ref) {
            checkpoint::capture_ref(cwd, git_ref)
                .map_err(|error| PortError::retryable(format!("{error:#}")))?;
        }
        // A base that no longer resolves (pruned) diffs against HEAD.
        let base = base_ref.filter(|base| checkpoint::has_ref(cwd, base));
        let files = checkpoint::diff_refs(cwd, base, git_ref)
            .map_err(|error| PortError::retryable(format!("{error:#}")))?
            .into_iter()
            .map(|file| CheckpointFile {
                path: file.path,
                kind: kind(file.status).to_owned(),
                additions: saturate(file.additions),
                deletions: saturate(file.deletions),
            })
            .collect();
        Ok(CapturedCheckpoint {
            git_ref: git_ref.to_owned(),
            files,
        })
    }

    fn restore(&self, cwd: &Path, git_ref: &str) -> Result<(), PortError> {
        if !checkpoint::has_ref(cwd, git_ref) {
            return Err(PortError::permanent(format!(
                "checkpoint `{git_ref}` is unavailable"
            )));
        }
        checkpoint::restore_ref(cwd, git_ref)
            .map_err(|error| PortError::retryable(format!("{error:#}")))
    }
}
