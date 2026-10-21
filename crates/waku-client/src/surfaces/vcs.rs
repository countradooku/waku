//! Version control and review: `vcs.*` and `review.diff`.

use std::path::Path;

use anyhow::{Context as _, bail};
use uuid::Uuid;

use super::DaemonV8;
use super::files::path_root;
use crate::v8::{
    BranchSnapshot, CommitResult, CommitSnapshot, GitStatus, ReviewDiff, ReviewDiffParams,
    ReviewDiffSource, RootOpParams, RootParams, TextPayload, VcsCommitParams,
    VcsGenerateCommitMessageParams, VcsStageParams, VcsSwitchRefParams, method,
};
use waku_protocol::model::ProviderKind;
use waku_protocol::orchestration::{ModelSelection, RunId, ThreadId};
use waku_protocol::v8::orchestration::{FullDiffParams, TurnDiffParams};

/// The branch a snapshot shows: the current one, else the detached head.
pub fn display_branch(snapshot: &BranchSnapshot) -> Option<&str> {
    snapshot
        .current
        .as_deref()
        .or(snapshot.detached_head.as_deref())
}

pub fn status(daemon: &DaemonV8, root: &Path) -> anyhow::Result<GitStatus> {
    daemon.call::<method::VcsStatus>(RootParams {
        root: path_root(root),
    })
}

/// The branches of the repository at `root`; `None` outside a repository.
pub fn refs(daemon: &DaemonV8, root: &Path) -> anyhow::Result<Option<BranchSnapshot>> {
    daemon.call::<method::VcsRefs>(RootParams {
        root: path_root(root),
    })
}

/// Check out `name`, creating it first with `create`.
pub fn switch_ref(
    daemon: &DaemonV8,
    root: &Path,
    name: String,
    create: bool,
) -> anyhow::Result<BranchSnapshot> {
    daemon.call::<method::VcsSwitchRef>(VcsSwitchRefParams {
        root: path_root(root),
        name,
        create,
        op_id: Uuid::new_v4(),
    })
}

pub fn commit_preview(daemon: &DaemonV8, root: &Path) -> anyhow::Result<CommitSnapshot> {
    daemon.call::<method::VcsCommitPreview>(RootParams {
        root: path_root(root),
    })
}

/// Stage (or unstage) `paths`; returns the status after it.
pub fn stage(
    daemon: &DaemonV8,
    root: &Path,
    paths: Vec<String>,
    stage: bool,
) -> anyhow::Result<GitStatus> {
    daemon.call::<method::VcsStage>(VcsStageParams {
        root: path_root(root),
        paths,
        stage,
        op_id: Uuid::new_v4(),
    })
}

/// The provider and model a commit message is written with.
pub fn commit_message_selection(
    provider: ProviderKind,
    model: Option<String>,
    reasoning_effort: Option<String>,
) -> ModelSelection {
    ModelSelection {
        model,
        reasoning_effort,
        ..ModelSelection::new(provider.id())
    }
}

pub fn generate_commit_message(
    daemon: &DaemonV8,
    root: &Path,
    include_unstaged: bool,
    selection: ModelSelection,
) -> anyhow::Result<String> {
    Ok(daemon
        .call::<method::VcsGenerateCommitMessage>(VcsGenerateCommitMessageParams {
            root: path_root(root),
            include_unstaged,
            selection,
        })?
        .message)
}

pub fn commit(
    daemon: &DaemonV8,
    root: &Path,
    message: String,
    include_unstaged: bool,
    push: bool,
) -> anyhow::Result<CommitResult> {
    daemon.call::<method::VcsCommit>(VcsCommitParams {
        root: path_root(root),
        message,
        include_unstaged,
        push,
        op_id: Uuid::new_v4(),
    })
}

pub fn push(daemon: &DaemonV8, root: &Path) -> anyhow::Result<()> {
    daemon.call::<method::VcsPush>(RootOpParams {
        root: path_root(root),
        op_id: Uuid::new_v4(),
    })?;
    Ok(())
}

/// Pull; a pull that stopped on conflicts is an error naming them.
pub fn pull(daemon: &DaemonV8, root: &Path) -> anyhow::Result<()> {
    let result = daemon.call::<method::VcsPull>(RootOpParams {
        root: path_root(root),
        op_id: Uuid::new_v4(),
    })?;
    pull_outcome(&result.conflicts)
}

/// A pull's conflicts as the desktop reports them.
pub fn pull_outcome(conflicts: &[String]) -> anyhow::Result<()> {
    if conflicts.is_empty() {
        return Ok(());
    }
    bail!(
        "the pull stopped on merge conflicts in {}",
        conflicts.join(", ")
    )
}

/// A review diff's raw parts, ready for the desktop's parser.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectedDiff {
    pub numstat: String,
    pub patch: String,
    pub complete_context: bool,
}

/// `review.diff` of `root`, with a patch too large to come inline
/// downloaded from the blob store.
pub fn review_diff(
    daemon: &DaemonV8,
    root: &Path,
    source: ReviewDiffSource,
    ignore_whitespace: bool,
) -> anyhow::Result<CollectedDiff> {
    let diff = daemon.call::<method::ReviewDiffGet>(ReviewDiffParams {
        root: path_root(root),
        source,
        ignore_whitespace,
    })?;
    collected(daemon, diff)
}

/// `threads.turnDiff`: what run `run_id` of `thread_id` changed, from its
/// turn-start checkpoint to the checkpoint it ended with.
pub fn turn_diff(
    daemon: &DaemonV8,
    thread_id: Uuid,
    run_id: Uuid,
    ignore_whitespace: bool,
) -> anyhow::Result<CollectedDiff> {
    let diff = daemon.call::<method::ThreadsTurnDiff>(TurnDiffParams {
        thread_id: ThreadId(thread_id),
        run_id: RunId(run_id),
        ignore_whitespace,
    })?;
    collected(daemon, diff)
}

/// `threads.fullDiff`: what the thread's live runs changed together.
pub fn full_diff(
    daemon: &DaemonV8,
    thread_id: Uuid,
    ignore_whitespace: bool,
) -> anyhow::Result<CollectedDiff> {
    let diff = daemon.call::<method::ThreadsFullDiff>(FullDiffParams {
        thread_id: ThreadId(thread_id),
        ignore_whitespace,
    })?;
    collected(daemon, diff)
}

/// A diff's parts, with a patch too large to come inline downloaded from
/// the blob store.
fn collected(daemon: &DaemonV8, diff: ReviewDiff) -> anyhow::Result<CollectedDiff> {
    let patch = match diff.patch {
        TextPayload::Inline { value } => value,
        TextPayload::Blob { reference, .. } => {
            let (_, bytes) = daemon
                .client()?
                .download_blob(&reference, None)
                .map_err(super::call_error)?;
            String::from_utf8(bytes).context("the diff is not valid UTF-8")?
        }
        TextPayload::Unknown => bail!("the daemon sent the diff in an unknown form"),
    };
    Ok(CollectedDiff {
        numstat: diff.numstat,
        patch,
        complete_context: diff.complete_context,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_detached_head_stands_in_for_the_branch() {
        let mut snapshot = BranchSnapshot {
            repository: "/srv/app".into(),
            current: None,
            detached_head: Some("abc1234".into()),
            default_branch: Some("main".into()),
            branches: Vec::new(),
            additions: 0,
            deletions: 0,
        };
        assert_eq!(display_branch(&snapshot), Some("abc1234"));
        snapshot.current = Some("main".into());
        assert_eq!(display_branch(&snapshot), Some("main"));
    }

    #[test]
    fn a_commit_message_selection_names_the_provider_instance() {
        let selection = commit_message_selection(ProviderKind::Codex, Some("gpt-5".into()), None);
        assert_eq!(selection.instance.as_str(), "codex");
        assert_eq!(selection.model.as_deref(), Some("gpt-5"));
        assert_eq!(selection.reasoning_effort, None);
    }

    #[test]
    fn a_pull_with_conflicts_is_an_error() {
        assert!(pull_outcome(&[]).is_ok());
        let error = pull_outcome(&["a.rs".into(), "b.rs".into()]).unwrap_err();
        assert!(error.to_string().contains("a.rs, b.rs"), "{error}");
    }
}
