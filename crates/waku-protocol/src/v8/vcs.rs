//! `vcs.*`, `review.diff` and the `vcsStatus` stream (RFC 0003 §5.2
//! "Version control and review").

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{RootRef, TextPayload};
use crate::orchestration::ModelSelection;

/// A review patch above this many bytes is sent as a blob reference.
pub const MAX_INLINE_PATCH_BYTES: u64 = 1024 * 1024;
/// `vcsStatus` computes at most once per this many milliseconds.
pub const VCS_STATUS_MIN_INTERVAL_MS: u64 = 1_000;
/// Without a change made through the daemon, `vcsStatus` re-reads the
/// repository this often while subscribed.
pub const VCS_STATUS_REFRESH_MS: u64 = 2_000;

/// The params of a read that only names a root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RootParams {
    pub root: RootRef,
}

/// The params of a mutation that only names a root (`vcs.push`, `vcs.pull`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RootOpParams {
    pub root: RootRef,
    pub op_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct GitStatus {
    pub repository: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    pub ahead: u64,
    pub behind: u64,
    pub files: Vec<GitStatusFile>,
}

/// One changed path, with git's porcelain status letters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct GitStatusFile {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_path: Option<String>,
    pub index_status: String,
    pub worktree_status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct BranchSnapshot {
    pub repository: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detached_head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,
    pub branches: Vec<BranchEntry>,
    pub additions: u64,
    pub deletions: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct BranchEntry {
    pub name: String,
    pub checked_out_elsewhere: bool,
}

/// `vcs.switchRef` params: check out `name`, creating it first with
/// `create`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct VcsSwitchRefParams {
    pub root: RootRef,
    pub name: String,
    #[serde(default)]
    pub create: bool,
    pub op_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct GitWorktree {
    pub path: String,
    pub head: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    pub bare: bool,
    pub locked: bool,
    pub prunable: bool,
}

/// `vcs.createWorktree` params: a new `waku/` branch and worktree for a
/// project, in the managed worktree folder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct VcsCreateWorktreeParams {
    pub project_id: Uuid,
    /// Branch or commit to start from; the repository's default branch when
    /// absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    /// Text the branch and folder names are derived from (a prompt or a
    /// title).
    pub branch_hint: String,
    /// The thread the worktree is for, recorded in its name; a fresh id when
    /// absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<Uuid>,
    pub op_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CreatedWorktree {
    pub path: String,
    pub branch: String,
}

/// `vcs.removeWorktree` params. Without `force`, git refuses a worktree
/// with modified or untracked files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct VcsRemoveWorktreeParams {
    /// Absolute path of a linked worktree.
    pub path: String,
    #[serde(default)]
    pub force: bool,
    pub op_id: Uuid,
}

/// What a commit dialog shows before committing.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CommitSnapshot {
    pub branch: String,
    pub additions: u64,
    pub deletions: u64,
    pub staged_additions: u64,
    pub staged_deletions: u64,
    pub has_staged: bool,
    pub has_unstaged: bool,
    pub can_push: bool,
}

/// `vcs.stage` params: stage (or, with `stage: false`, unstage) `paths`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct VcsStageParams {
    pub root: RootRef,
    pub paths: Vec<String>,
    pub stage: bool,
    pub op_id: Uuid,
}

/// `vcs.generateCommitMessage` params. The provider instance's binary comes
/// from settings, never from the request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct VcsGenerateCommitMessageParams {
    pub root: RootRef,
    #[serde(default)]
    pub include_unstaged: bool,
    pub selection: ModelSelection,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CommitMessage {
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct VcsCommitParams {
    pub root: RootRef,
    pub message: String,
    #[serde(default)]
    pub include_unstaged: bool,
    #[serde(default)]
    pub push: bool,
    pub op_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CommitResult {
    /// The new commit.
    pub sha: String,
    pub pushed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PullResult {
    /// `HEAD` moved.
    pub updated: bool,
    /// Paths left unmerged.
    pub conflicts: Vec<String>,
}

/// Which changes `review.diff` compares. The last turn of a thread is
/// `threads.turnDiff`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ReviewDiffSource {
    /// Working tree and index against `HEAD`, untracked files included.
    Uncommitted,
    /// Working tree against the index.
    Unstaged,
    /// Index against `HEAD`.
    Staged,
    /// Commits on the branch since it left its base.
    Committed,
    /// Everything on the branch since its base, committed or not.
    Branch,
    #[serde(other)]
    Unknown,
}

/// `review.diff` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReviewDiffParams {
    pub root: RootRef,
    pub source: ReviewDiffSource,
    #[serde(default)]
    pub ignore_whitespace: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReviewDiff {
    pub source: ReviewDiffSource,
    /// `git diff --numstat` output.
    pub numstat: String,
    /// `numstat`, parsed.
    pub files: Vec<FileDiffSummary>,
    /// The unified patch; a blob reference above [`MAX_INLINE_PATCH_BYTES`].
    pub patch: TextPayload,
    /// The patch carries full file context (small diffs only).
    pub complete_context: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FileDiffSummary {
    pub path: String,
    /// Absent for binary files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletions: Option<u64>,
    pub binary: bool,
}

/// `vcsStatus` spec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct VcsStatusSpec {
    pub root: RootRef,
}

/// `vcsStatus` items: the status whenever it changes, at most once per
/// [`VCS_STATUS_MIN_INTERVAL_MS`]. Each replaces the previous one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum VcsStatusItem {
    Status {
        status: GitStatus,
        /// When it was read, in milliseconds since the Unix epoch.
        at: i64,
    },
    #[serde(other)]
    Unknown,
}
