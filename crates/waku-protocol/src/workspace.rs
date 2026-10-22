use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::composer::{FileEntry, SlashCommand};
use crate::git::{
    AgentInvocation, BranchSnapshot, CommitSnapshot, CreatedWorktree, GitStatus, GitWorktree,
};
use crate::model::ProviderKind;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ReviewDiffSource {
    Uncommitted,
    Unstaged,
    Staged,
    Committed,
    Branch,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewDiffData {
    pub source: ReviewDiffSource,
    pub numstat: String,
    pub patch: String,
    pub complete_context: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkingTreeEntry {
    pub relative_path: String,
    pub absolute_path: PathBuf,
    pub name: String,
    pub is_dir: bool,
    pub expanded: bool,
    pub depth: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WorkspaceOperation {
    ListTree {
        root: PathBuf,
        expanded_paths: Vec<PathBuf>,
    },
    BrowseDirectory {
        path: Option<PathBuf>,
    },
    WriteTextFile {
        root: PathBuf,
        relative_path: PathBuf,
        content: String,
    },
    ListProjectFiles {
        root: PathBuf,
        cap: usize,
    },
    DiscoverSlashCommands {
        provider: ProviderKind,
        project_root: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binary_override: Option<String>,
    },
    InspectBranches {
        cwd: PathBuf,
    },
    CheckoutBranch {
        cwd: PathBuf,
        branch: String,
        create: bool,
    },
    CreateWorktree {
        project_path: PathBuf,
        project_id: Uuid,
        session_id: Uuid,
        prompt: String,
        base_branch: Option<String>,
    },
    InspectCommit {
        cwd: PathBuf,
    },
    GitStatus {
        cwd: PathBuf,
    },
    StageFiles {
        cwd: PathBuf,
        paths: Vec<String>,
        stage: bool,
    },
    Pull {
        cwd: PathBuf,
    },
    ListWorktrees {
        cwd: PathBuf,
    },
    GenerateCommitMessage {
        cwd: PathBuf,
        include_unstaged: bool,
        invocation: AgentInvocation,
    },
    Commit {
        cwd: PathBuf,
        message: String,
        include_unstaged: bool,
        push: bool,
    },
    Push {
        cwd: PathBuf,
    },
    CollectReviewDiff {
        cwd: PathBuf,
        source: ReviewDiffSource,
        #[serde(default)]
        ignore_whitespace: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WorkspaceResult {
    Ack,
    WorkingTree {
        entries: Vec<WorkingTreeEntry>,
    },
    Directory {
        path: PathBuf,
        parent: Option<PathBuf>,
        home: PathBuf,
        filesystem_root: PathBuf,
        entries: Vec<WorkingTreeEntry>,
    },
    ProjectFiles {
        entries: Vec<FileEntry>,
    },
    SlashCommands {
        commands: Vec<SlashCommand>,
    },
    Branches {
        snapshot: Option<BranchSnapshot>,
    },
    BranchChanged {
        snapshot: BranchSnapshot,
    },
    WorktreeCreated {
        worktree: CreatedWorktree,
    },
    CommitSnapshot {
        snapshot: CommitSnapshot,
    },
    GitStatus {
        status: GitStatus,
    },
    Worktrees {
        worktrees: Vec<GitWorktree>,
    },
    CommitMessage {
        message: String,
    },
    ReviewDiff {
        data: ReviewDiffData,
    },
}
