//! The daemon itself, settings changes and storage (RFC 0003 §5.2 "Daemon
//! and session", "Settings"; the worktree storage of #62).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{AppGrant, EnvironmentInfo};

/// `daemon.info` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DaemonInfo {
    pub version: String,
    pub environment: EnvironmentInfo,
    pub features: Vec<String>,
    pub uptime_ms: u64,
    /// Where a thread without a project runs by default.
    pub default_cwd: String,
    /// The folder scratch projects are created in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projectless_root: Option<String>,
    /// The daemon user's home directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home: Option<String>,
}

/// `daemon.shutdown` params. Only a desktop-owned daemon accepts it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DaemonShutdownParams {
    #[serde(default)]
    pub reason: String,
}

// -- settings.update --------------------------------------------------------

/// `settings.update` params. Fields present in `patch` replace the current
/// value; with `baseRevision`, the update is refused with `conflict`
/// (`revisionConflict`) unless the settings still have that revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SettingsUpdateParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_revision: Option<u64>,
    pub patch: SettingsPatch,
    pub op_id: Uuid,
}

/// The settings fields to change. Touching `disabledProviders` or
/// `providerBinaryOverrides` needs `providers:manage` as well.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default, rename_all = "camelCase")]
pub struct SettingsPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computer_use_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computer_use_allowed_apps: Option<Vec<AppGrant>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_providers: Option<Vec<String>>,
    /// The whole map: binary path per provider instance id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_binary_overrides: Option<BTreeMap<String, String>>,
    /// The managed worktree folder and its cleanup rules. Moving the folder
    /// keeps the old location in `previousWorktreesDirectories`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageSettings>,
    /// Continue threads whose turn a daemon restart interrupted (#59).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continue_threads_after_restart: Option<bool>,
}

impl SettingsPatch {
    /// Whether the patch changes provider binaries or which providers run
    /// (RFC 0003 §6.3: `providers:manage`).
    pub fn touches_providers(&self) -> bool {
        self.disabled_providers.is_some() || self.provider_binary_overrides.is_some()
    }
}

// -- storage ----------------------------------------------------------------

/// Upper bound for every "after N days" rule.
pub const MAX_RETENTION_DAYS: u32 = 3650;

/// The managed worktree folder and its cleanup rules (#62).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default, rename_all = "camelCase")]
pub struct StorageSettings {
    /// Where new worktrees are created; the daemon's default folder when
    /// absent. Absolute; a leading `~` is expanded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktrees_directory: Option<String>,
    /// Earlier locations, still managed by cleanup.
    pub previous_worktrees_directories: Vec<String>,
    pub worktree_cleanup: WorktreeCleanupSettings,
}

/// Automatic worktree cleanup rules; all off by default.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(default, rename_all = "camelCase")]
pub struct WorktreeCleanupSettings {
    /// Remove a worktree whose threads had no activity for this many days.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inactive_after_days: Option<u32>,
    /// Remove a worktree this many days after its threads finished.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_after_days: Option<u32>,
    /// Remove a worktree whose branch was merged into its base.
    pub merged: bool,
    /// Remove a worktree with no commits beyond its base.
    pub unchanged: bool,
    /// Remove the worktree of a deleted thread.
    pub deleted_tasks: bool,
    pub keep_when: WorktreeKeepWhen,
    /// Allow removing a worktree whose branch holds commits that exist
    /// nowhere else (the branch is kept).
    pub remove_unpushed_commits: bool,
    /// Hours between automatic runs, 1–168.
    pub interval_hours: u32,
}

impl Default for WorktreeCleanupSettings {
    fn default() -> Self {
        Self {
            inactive_after_days: None,
            finished_after_days: None,
            merged: false,
            unchanged: false,
            deleted_tasks: false,
            keep_when: WorktreeKeepWhen::UncommittedChanges,
            remove_unpushed_commits: false,
            interval_hours: 24,
        }
    }
}

open_string_enum! {
    /// How much local work keeps a worktree on disk.
    #[derive(Default)]
    pub enum WorktreeKeepWhen {
        /// Modified, untracked or ignored files (dependency folders aside).
        AnyLocalFiles = "anyLocalFiles",
        /// Modified or untracked files.
        #[default]
        UncommittedChanges = "uncommittedChanges",
        /// Modified tracked files only.
        TrackedChanges = "trackedChanges",
    }
}

/// `storage.runWorktreeCleanup` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct StorageRunCleanupParams {
    pub op_id: Uuid,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct WorktreeCleanupStatus {
    /// Milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_report: Option<WorktreeCleanupReport>,
    /// When the next automatic run is due, if any rule is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<i64>,
    pub running: bool,
}

open_string_enum! {
    pub enum CleanupTrigger {
        Automatic = "automatic",
        Manual = "manual",
    }
}

open_string_enum! {
    pub enum CleanupOutcome {
        Removed = "removed",
        Kept = "kept",
        Failed = "failed",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct WorktreeCleanupReport {
    pub trigger: CleanupTrigger,
    /// Milliseconds since the Unix epoch.
    pub started_at: i64,
    pub finished_at: i64,
    pub entries: Vec<WorktreeCleanupEntry>,
    pub removed: u32,
    pub kept: u32,
    pub failed: u32,
    /// Rows beyond the report's bound, counted but not listed.
    pub omitted: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct WorktreeCleanupEntry {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_title: Option<String>,
    pub outcome: CleanupOutcome,
    pub reason: CleanupReason,
    /// The `waku/` branch was deleted with the worktree.
    pub branch_deleted: bool,
}

/// Why a worktree was removed, kept, or could not be removed. Clients
/// localize these; `base` names the branch compared with.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CleanupReason {
    TaskDeleted,
    Inactive {
        days: u64,
    },
    Finished {
        days: u64,
    },
    Merged {
        base: String,
    },
    Unchanged {
        base: String,
    },
    NoRuleApplies,
    TaskRunning,
    LiveRuntime,
    OpenTerminal,
    OutsideManagedFolder,
    NotWakuBranch,
    NotLinkedWorktree,
    BranchChanged,
    UncommittedChanges {
        files: u32,
    },
    IgnoredFiles {
        example: String,
    },
    UnpushedCommits {
        commits: u32,
    },
    InactiveFor {
        days: u64,
        of: u32,
    },
    FinishedFor {
        days: u64,
        of: u32,
    },
    NotFinished,
    NotMerged {
        base: String,
    },
    HasCommits {
        commits: u32,
        base: String,
    },
    BaseUnavailable,
    ChangedDuringCleanup,
    Error {
        message: String,
    },
    #[serde(other)]
    Unknown,
}
