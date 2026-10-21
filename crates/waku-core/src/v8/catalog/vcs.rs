//! `vcs.*`, `review.diff` and the `vcsStatus` stream.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use uuid::Uuid;
use waku_protocol::v8::{
    Ack, BranchEntry, BranchSnapshot, CommitMessage, CommitResult, CommitSnapshot, CreatedWorktree,
    EndReason, Error, FileDiffSummary, GitStatus, GitStatusFile, GitWorktree,
    MAX_INLINE_PATCH_BYTES, PullResult, ReviewDiff, ReviewDiffParams, ReviewDiffSource,
    RootOpParams, RootParams, TextPayload, VCS_STATUS_MIN_INTERVAL_MS, VCS_STATUS_REFRESH_MS,
    VcsCommitParams, VcsCreateWorktreeParams, VcsGenerateCommitMessageParams,
    VcsRemoveWorktreeParams, VcsStageParams, VcsStatusItem, VcsStatusSpec, VcsSwitchRefParams,
};

use super::{Domain, backend_error, bad_request, display_path, now_ms, workspace};
use crate::v8::V8Context;
use crate::v8::connection::StreamSink;
use crate::v8::methods::{project_not_found, root_path};
use crate::v8::orchestration::{live_threads, projects};
use crate::workspace::{WorkspaceOperation, WorkspaceResult};

/// Tells `vcsStatus` streams that this daemon changed a repository, so they
/// re-read at once instead of at their next refresh.
pub(crate) struct VcsBus {
    sender: watch::Sender<u64>,
}

impl Default for VcsBus {
    fn default() -> Self {
        Self {
            sender: watch::channel(0).0,
        }
    }
}

impl VcsBus {
    pub(crate) fn changed(&self) {
        self.sender.send_modify(|generation| *generation += 1);
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.sender.subscribe()
    }
}

fn unexpected() -> Error {
    super::internal("unexpected workspace result")
}

pub(crate) fn wire_status(status: waku_protocol::git::GitStatus) -> GitStatus {
    GitStatus {
        repository: display_path(&status.repository),
        branch: status.branch,
        head: status.head,
        upstream: status.upstream,
        ahead: status.ahead,
        behind: status.behind,
        files: status
            .files
            .into_iter()
            .map(|file| GitStatusFile {
                path: file.path,
                previous_path: file.previous_path,
                index_status: file.index_status,
                worktree_status: file.worktree_status,
            })
            .collect(),
    }
}

fn wire_branches(snapshot: waku_protocol::git::BranchSnapshot) -> BranchSnapshot {
    BranchSnapshot {
        repository: display_path(&snapshot.repository),
        current: snapshot.current,
        detached_head: snapshot.detached_head,
        default_branch: snapshot.default_branch,
        branches: snapshot
            .branches
            .into_iter()
            .map(|branch| BranchEntry {
                name: branch.name,
                checked_out_elsewhere: branch.checked_out_elsewhere,
            })
            .collect(),
        additions: snapshot.additions,
        deletions: snapshot.deletions,
    }
}

fn status_at(context: &V8Context, cwd: PathBuf) -> Result<GitStatus, Error> {
    match workspace(context, WorkspaceOperation::GitStatus { cwd }, Domain::Vcs)? {
        WorkspaceResult::GitStatus { status } => Ok(wire_status(status)),
        _ => Err(unexpected()),
    }
}

pub(super) fn status(context: &V8Context, params: RootParams) -> Result<GitStatus, Error> {
    status_at(context, root_path(context, &params.root)?)
}

pub(super) fn refs(
    context: &V8Context,
    params: RootParams,
) -> Result<Option<BranchSnapshot>, Error> {
    let cwd = root_path(context, &params.root)?;
    match workspace(
        context,
        WorkspaceOperation::InspectBranches { cwd },
        Domain::Vcs,
    )? {
        WorkspaceResult::Branches { snapshot } => Ok(snapshot.map(wire_branches)),
        _ => Err(unexpected()),
    }
}

pub(super) fn switch_ref(
    context: &V8Context,
    params: VcsSwitchRefParams,
) -> Result<BranchSnapshot, Error> {
    let cwd = root_path(context, &params.root)?;
    if params.name.trim().is_empty() {
        return Err(bad_request("name", "empty"));
    }
    let result = workspace(
        context,
        WorkspaceOperation::CheckoutBranch {
            cwd,
            branch: params.name,
            create: params.create,
        },
        Domain::Vcs,
    );
    context.catalog.vcs.changed();
    match result? {
        WorkspaceResult::BranchChanged { snapshot } => Ok(wire_branches(snapshot)),
        _ => Err(unexpected()),
    }
}

pub(super) fn worktrees(
    context: &V8Context,
    params: RootParams,
) -> Result<Vec<GitWorktree>, Error> {
    let cwd = root_path(context, &params.root)?;
    match workspace(
        context,
        WorkspaceOperation::ListWorktrees { cwd },
        Domain::Vcs,
    )? {
        WorkspaceResult::Worktrees { worktrees } => Ok(worktrees
            .into_iter()
            .map(|worktree| GitWorktree {
                path: display_path(&worktree.path),
                head: worktree.head,
                branch: worktree.branch,
                bare: worktree.bare,
                locked: worktree.locked,
                prunable: worktree.prunable,
            })
            .collect()),
        _ => Err(unexpected()),
    }
}

pub(super) fn create_worktree(
    context: &V8Context,
    params: VcsCreateWorktreeParams,
) -> Result<CreatedWorktree, Error> {
    let project = projects(context)?
        .into_iter()
        .find(|project| project.id.0 == params.project_id)
        .ok_or_else(|| project_not_found(params.project_id))?;
    // The worktree is named for an orchestration thread (D6), when given.
    if let Some(thread_id) = params.thread_id
        && live_threads(context, [thread_id])?.is_empty()
    {
        return Err(super::not_found("thread", thread_id.to_string()));
    }
    let result = workspace(
        context,
        WorkspaceOperation::CreateWorktree {
            project_path: project.path,
            project_id: project.id.0,
            session_id: params.thread_id.unwrap_or_else(Uuid::new_v4),
            prompt: params.branch_hint,
            base_branch: params.base_ref,
        },
        Domain::Vcs,
    );
    context.catalog.vcs.changed();
    match result? {
        WorkspaceResult::WorktreeCreated { worktree } => Ok(CreatedWorktree {
            path: display_path(&worktree.path),
            branch: worktree.branch,
        }),
        _ => Err(unexpected()),
    }
}

fn git(cwd: &Path, args: &[&str]) -> Result<String, Error> {
    crate::git_workflow::git(cwd, args)
        .map(|output| String::from_utf8_lossy(&output).trim().to_owned())
        .map_err(|error| backend_error(error, Domain::Vcs))
}

pub(super) fn remove_worktree(
    context: &V8Context,
    params: VcsRemoveWorktreeParams,
) -> Result<Ack, Error> {
    let path = super::absolute_path("path", &params.path)?;
    if !path.is_dir() {
        return Err(super::not_found("worktree", &params.path));
    }
    // Run from the repository that owns the worktree, not from inside it.
    let common = git(
        &path,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?;
    let common = PathBuf::from(common);
    let owner = if common.file_name().is_some_and(|name| name == ".git") {
        common.parent().map(Path::to_path_buf).unwrap_or(common)
    } else {
        common
    };
    let target = super::path_string(&path)?;
    let mut args = vec!["worktree", "remove"];
    if params.force {
        args.push("--force");
    }
    args.push(&target);
    let result = git(&owner, &args);
    context.catalog.vcs.changed();
    result.map(|_| Ack {})
}

pub(super) fn commit_preview(
    context: &V8Context,
    params: RootParams,
) -> Result<CommitSnapshot, Error> {
    let cwd = root_path(context, &params.root)?;
    match workspace(
        context,
        WorkspaceOperation::InspectCommit { cwd },
        Domain::Vcs,
    )? {
        WorkspaceResult::CommitSnapshot { snapshot } => Ok(CommitSnapshot {
            branch: snapshot.branch,
            additions: snapshot.additions,
            deletions: snapshot.deletions,
            staged_additions: snapshot.staged_additions,
            staged_deletions: snapshot.staged_deletions,
            has_staged: snapshot.has_staged,
            has_unstaged: snapshot.has_unstaged,
            can_push: snapshot.can_push,
        }),
        _ => Err(unexpected()),
    }
}

pub(super) fn stage(context: &V8Context, params: VcsStageParams) -> Result<GitStatus, Error> {
    let cwd = root_path(context, &params.root)?;
    let result = workspace(
        context,
        WorkspaceOperation::StageFiles {
            cwd: cwd.clone(),
            paths: params.paths,
            stage: params.stage,
        },
        Domain::Vcs,
    );
    context.catalog.vcs.changed();
    result?;
    status_at(context, cwd)
}

pub(super) fn generate_commit_message(
    context: &V8Context,
    params: VcsGenerateCommitMessageParams,
) -> Result<CommitMessage, Error> {
    let cwd = root_path(context, &params.root)?;
    let provider = super::provider_kind(params.selection.instance.as_str())?;
    let binary = super::providers::provider_binary(context, provider)?;
    match workspace(
        context,
        WorkspaceOperation::GenerateCommitMessage {
            cwd,
            include_unstaged: params.include_unstaged,
            invocation: waku_protocol::git::AgentInvocation {
                provider,
                binary,
                model: params.selection.model,
                reasoning_effort: params.selection.reasoning_effort,
            },
        },
        Domain::Provider,
    )? {
        WorkspaceResult::CommitMessage { message } => Ok(CommitMessage { message }),
        _ => Err(unexpected()),
    }
}

pub(super) fn commit(context: &V8Context, params: VcsCommitParams) -> Result<CommitResult, Error> {
    let cwd = root_path(context, &params.root)?;
    if params.message.trim().is_empty() {
        return Err(bad_request("message", "empty"));
    }
    let committed = workspace(
        context,
        WorkspaceOperation::Commit {
            cwd: cwd.clone(),
            message: params.message,
            include_unstaged: params.include_unstaged,
            push: false,
        },
        Domain::Vcs,
    );
    context.catalog.vcs.changed();
    committed?;
    let sha = git(&cwd, &["rev-parse", "HEAD"])?;
    if params.push {
        let pushed = workspace(context, WorkspaceOperation::Push { cwd }, Domain::Vcs);
        context.catalog.vcs.changed();
        if let Err(mut error) = pushed {
            error.message = format!("committed {sha}, but the push failed: {}", error.message);
            return Err(error);
        }
    }
    Ok(CommitResult {
        sha,
        pushed: params.push,
    })
}

pub(super) fn push(context: &V8Context, params: RootOpParams) -> Result<Ack, Error> {
    let cwd = root_path(context, &params.root)?;
    let result = workspace(context, WorkspaceOperation::Push { cwd }, Domain::Vcs);
    context.catalog.vcs.changed();
    result.map(|_| Ack {})
}

/// Paths git left unmerged.
fn conflicts(status: &GitStatus) -> Vec<String> {
    status
        .files
        .iter()
        .filter(|file| {
            matches!(
                (file.index_status.as_str(), file.worktree_status.as_str()),
                ("U", _) | (_, "U") | ("A", "A") | ("D", "D")
            )
        })
        .map(|file| file.path.clone())
        .collect()
}

pub(super) fn pull(context: &V8Context, params: RootOpParams) -> Result<PullResult, Error> {
    let cwd = root_path(context, &params.root)?;
    let before = git(&cwd, &["rev-parse", "HEAD"]).ok();
    let pulled = workspace(
        context,
        WorkspaceOperation::Pull { cwd: cwd.clone() },
        Domain::Vcs,
    );
    context.catalog.vcs.changed();
    let after = git(&cwd, &["rev-parse", "HEAD"]).ok();
    let updated = before != after;
    match pulled {
        Ok(_) => Ok(PullResult {
            updated,
            conflicts: Vec::new(),
        }),
        // A pull that stops on conflicts leaves them for the user to resolve.
        Err(error) => {
            let conflicts = status_at(context, cwd)
                .map(|status| conflicts(&status))
                .unwrap_or_default();
            if conflicts.is_empty() {
                return Err(error);
            }
            Ok(PullResult { updated, conflicts })
        }
    }
}

fn legacy_source(source: ReviewDiffSource) -> Result<crate::workspace::ReviewDiffSource, Error> {
    use crate::workspace::ReviewDiffSource as Legacy;
    Ok(match source {
        ReviewDiffSource::Uncommitted => Legacy::Uncommitted,
        ReviewDiffSource::Unstaged => Legacy::Unstaged,
        ReviewDiffSource::Staged => Legacy::Staged,
        ReviewDiffSource::Committed => Legacy::Committed,
        ReviewDiffSource::Branch => Legacy::Branch,
        ReviewDiffSource::Unknown => {
            return Err(bad_request("source", "unknown review diff source"));
        }
    })
}

/// `git diff --numstat` lines: `added\tdeleted\tpath`, `-` for binary.
pub(crate) fn parse_numstat(numstat: &str) -> Vec<FileDiffSummary> {
    numstat
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, '\t');
            let additions = fields.next()?;
            let deletions = fields.next()?;
            let path = fields.next()?.to_owned();
            let binary = additions == "-" && deletions == "-";
            Some(FileDiffSummary {
                path,
                additions: additions.parse().ok(),
                deletions: deletions.parse().ok(),
                binary,
            })
        })
        .collect()
}

pub(super) fn review_diff(
    context: &V8Context,
    params: ReviewDiffParams,
) -> Result<ReviewDiff, Error> {
    let cwd = root_path(context, &params.root)?;
    let source = legacy_source(params.source)?;
    let WorkspaceResult::ReviewDiff { data } = workspace(
        context,
        WorkspaceOperation::CollectReviewDiff {
            cwd,
            source,
            ignore_whitespace: params.ignore_whitespace,
        },
        Domain::Vcs,
    )?
    else {
        return Err(unexpected());
    };
    let size = data.patch.len() as u64;
    let patch = if size > MAX_INLINE_PATCH_BYTES {
        let (reference, _) = super::store_blob(context, "text/plain", data.patch.into_bytes())?;
        TextPayload::Blob { reference, size }
    } else {
        TextPayload::Inline { value: data.patch }
    };
    Ok(ReviewDiff {
        source: params.source,
        files: parse_numstat(&data.numstat),
        numstat: data.numstat,
        patch,
        complete_context: data.complete_context,
    })
}

/// `vcsStatus`: the status whenever it differs from the last one sent, read
/// at most once a second, at once after this daemon changes a repository and
/// otherwise every [`VCS_STATUS_REFRESH_MS`]. Nothing runs once the
/// subscription ends.
pub(super) async fn status_stream(sink: StreamSink, context: Arc<V8Context>, spec: VcsStatusSpec) {
    let mut changes = context.catalog.vcs.subscribe();
    let task_context = context.clone();
    let root = match tokio::task::spawn_blocking(move || root_path(&task_context, &spec.root)).await
    {
        Ok(Ok(root)) => root,
        Ok(Err(error)) => {
            sink.end(EndReason::Error { error });
            return;
        }
        Err(_) => return,
    };
    let min_interval = Duration::from_millis(VCS_STATUS_MIN_INTERVAL_MS);
    let refresh = Duration::from_millis(VCS_STATUS_REFRESH_MS);
    let mut last: Option<GitStatus> = None;
    let mut last_read: Option<Instant> = None;
    loop {
        if let Some(read) = last_read {
            let elapsed = read.elapsed();
            if elapsed < min_interval {
                tokio::time::sleep(min_interval - elapsed).await;
            }
        }
        last_read = Some(Instant::now());
        changes.borrow_and_update();
        let task_context = context.clone();
        let cwd = root.clone();
        let status = match tokio::task::spawn_blocking(move || status_at(&task_context, cwd)).await
        {
            Ok(status) => status,
            Err(_) => return,
        };
        match status {
            Ok(status) if last.as_ref() != Some(&status) => {
                let item = VcsStatusItem::Status {
                    status: status.clone(),
                    at: now_ms(),
                };
                if sink.push(&item, None).is_err() {
                    return;
                }
                last = Some(status);
            }
            Ok(_) => {}
            Err(error) => {
                sink.end(EndReason::Error { error });
                return;
            }
        }
        tokio::select! {
            changed = changes.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = tokio::time::sleep(refresh) => {}
        }
    }
}
