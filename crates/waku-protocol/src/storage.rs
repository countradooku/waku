//! Worktree storage settings and cleanup reports (#62).
//!
//! The daemon owns every task worktree beneath one managed folder. These
//! types describe where that folder is, which rules make a worktree eligible
//! for removal, how much local work protects one, and what a cleanup run did.
//! They carry data only; the cleanup service lives in `waku-core`.

use std::path::{Path, PathBuf};

use crate::ts::TS;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Upper bound for every "after N days" rule, matching T3 Code's ten years.
pub const MAX_RETENTION_DAYS: u32 = 3650;
pub const DEFAULT_CLEANUP_INTERVAL_HOURS: u32 = 24;
pub const MIN_CLEANUP_INTERVAL_HOURS: u32 = 1;
pub const MAX_CLEANUP_INTERVAL_HOURS: u32 = 24 * 7;
/// Older managed locations kept for cleanup after the folder moves.
pub const MAX_PREVIOUS_WORKTREE_DIRECTORIES: usize = 8;
/// A report keeps this many rows; the rest are only counted.
pub const MAX_REPORT_ENTRIES: usize = 200;
/// Branch prefix of every worktree branch Waku creates.
pub const WORKTREE_BRANCH_PREFIX: &str = "waku/";

/// How much local work keeps a worktree on disk. Unpushed commits are
/// governed separately by [`WorktreeCleanupSettings::remove_unpushed_commits`].
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeKeepWhen {
    /// Keep a worktree with modified, untracked or ignored files. Dependency
    /// folders such as `node_modules` and `target` do not count.
    AnyLocalFiles,
    /// Keep a worktree with modified or untracked files.
    #[default]
    UncommittedChanges,
    /// Keep a worktree only for modified tracked files; untracked files are
    /// deleted with it.
    TrackedChanges,
}

impl WorktreeKeepWhen {
    pub const ALL: [Self; 3] = [
        Self::AnyLocalFiles,
        Self::UncommittedChanges,
        Self::TrackedChanges,
    ];
}

/// Automatic worktree cleanup rules. Every rule is off by default, so an
/// existing installation never loses a worktree until the user opts in.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(default)]
pub struct WorktreeCleanupSettings {
    /// Remove a worktree whose tasks have had no activity for this many days.
    pub inactive_after_days: Option<u32>,
    /// Remove a worktree this many days after its tasks finished (their last
    /// turn completed and nothing is running). Zero means once finished.
    pub finished_after_days: Option<u32>,
    /// Remove a worktree whose branch was merged into the base branch.
    pub merged: bool,
    /// Remove a worktree with no commits beyond the base branch.
    pub unchanged: bool,
    /// Remove the worktree of a deleted task.
    pub deleted_tasks: bool,
    pub keep_when: WorktreeKeepWhen,
    /// Allow removing a worktree whose branch holds commits that exist
    /// nowhere else. The branch itself is always kept in that case.
    pub remove_unpushed_commits: bool,
    /// Hours between automatic runs.
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
            keep_when: WorktreeKeepWhen::default(),
            remove_unpushed_commits: false,
            interval_hours: DEFAULT_CLEANUP_INTERVAL_HOURS,
        }
    }
}

impl WorktreeCleanupSettings {
    /// Whether any rule can make a worktree eligible.
    pub fn any_rule_enabled(&self) -> bool {
        self.inactive_after_days.is_some()
            || self.finished_after_days.is_some()
            || self.merged
            || self.unchanged
            || self.deleted_tasks
    }

    /// Clamp values a hand-edited file or an older client may carry.
    pub fn normalized(mut self) -> Self {
        self.inactive_after_days = self
            .inactive_after_days
            .map(|days| days.clamp(1, MAX_RETENTION_DAYS));
        self.finished_after_days = self
            .finished_after_days
            .map(|days| days.min(MAX_RETENTION_DAYS));
        self.interval_hours = self
            .interval_hours
            .clamp(MIN_CLEANUP_INTERVAL_HOURS, MAX_CLEANUP_INTERVAL_HOURS);
        self
    }
}

/// Daemon storage settings: the managed worktree folder and its cleanup.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(default)]
pub struct StorageSettings {
    /// Where new worktrees are created; `None` is `~/.waku-fork/worktrees`.
    pub worktrees_directory: Option<PathBuf>,
    /// Earlier locations. Worktrees created there stay managed, so cleanup
    /// still reaches them after the folder moves.
    pub previous_worktrees_directories: Vec<PathBuf>,
    pub worktree_cleanup: WorktreeCleanupSettings,
}

impl StorageSettings {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    /// Clamp rule values and drop locations that could never be valid.
    pub fn normalized(mut self, home: Option<&Path>) -> Self {
        self.worktree_cleanup = self.worktree_cleanup.normalized();
        self.worktrees_directory = self
            .worktrees_directory
            .take()
            .and_then(|path| validate_worktrees_directory(&path.to_string_lossy(), home).ok());
        let current = self.worktrees_directory.clone();
        let mut previous = Vec::new();
        for path in std::mem::take(&mut self.previous_worktrees_directories) {
            let Ok(path) = validate_worktrees_directory(&path.to_string_lossy(), home) else {
                continue;
            };
            if Some(&path) != current.as_ref() && !previous.contains(&path) {
                previous.push(path);
            }
        }
        previous.truncate(MAX_PREVIOUS_WORKTREE_DIRECTORIES);
        self.previous_worktrees_directories = previous;
        self
    }
}

/// Why a typed worktree location was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorktreeDirectoryError {
    Empty,
    NotAbsolute,
    FilesystemRoot,
}

/// Resolve a worktree location typed by the user: trims it, expands a
/// leading `~`, and requires an absolute path below the filesystem root.
/// Performs no I/O, so a client can validate before sending.
pub fn validate_worktrees_directory(
    input: &str,
    home: Option<&Path>,
) -> Result<PathBuf, WorktreeDirectoryError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(WorktreeDirectoryError::Empty);
    }
    let path = if input == "~" {
        home.map(Path::to_path_buf)
            .ok_or(WorktreeDirectoryError::NotAbsolute)?
    } else if let Some(rest) = input
        .strip_prefix("~/")
        .or_else(|| input.strip_prefix("~\\"))
    {
        home.ok_or(WorktreeDirectoryError::NotAbsolute)?.join(rest)
    } else {
        PathBuf::from(input)
    };
    if !path.is_absolute() {
        return Err(WorktreeDirectoryError::NotAbsolute);
    }
    let normalized: PathBuf = path.components().collect();
    if normalized.parent().is_none() {
        return Err(WorktreeDirectoryError::FilesystemRoot);
    }
    Ok(normalized)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeCleanupTrigger {
    Automatic,
    Manual,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeCleanupOutcome {
    Removed,
    Kept,
    Failed,
}

/// Why a worktree was removed, kept, or could not be removed. Clients
/// localize these; `base` names the branch the worktree was compared with.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "snake_case"
)]
pub enum WorktreeCleanupReason {
    TaskDeleted,
    Inactive { days: u64 },
    Finished { days: u64 },
    Merged { base: String },
    Unchanged { base: String },
    NoRuleApplies,
    TaskRunning,
    LiveRuntime,
    OpenTerminal,
    OutsideManagedFolder,
    NotWakuBranch,
    NotLinkedWorktree,
    BranchChanged,
    UncommittedChanges { files: u32 },
    IgnoredFiles { example: String },
    UnpushedCommits { commits: u32 },
    InactiveFor { days: u64, of: u32 },
    FinishedFor { days: u64, of: u32 },
    NotFinished,
    NotMerged { base: String },
    HasCommits { commits: u32, base: String },
    BaseUnavailable,
    ChangedDuringCleanup,
    Error { message: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
pub struct WorktreeCleanupEntry {
    pub path: PathBuf,
    pub branch: Option<String>,
    pub task_id: Option<Uuid>,
    pub task_title: Option<String>,
    pub outcome: WorktreeCleanupOutcome,
    pub reason: WorktreeCleanupReason,
    /// The `waku/` branch was deleted together with the worktree.
    pub branch_deleted: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
pub struct WorktreeCleanupReport {
    pub trigger: WorktreeCleanupTrigger,
    /// Unix seconds.
    pub started_at: u64,
    pub finished_at: u64,
    pub entries: Vec<WorktreeCleanupEntry>,
    pub removed: u32,
    pub kept: u32,
    pub failed: u32,
    /// Rows beyond [`MAX_REPORT_ENTRIES`], counted but not listed.
    pub omitted: u32,
}

impl WorktreeCleanupReport {
    pub fn new(trigger: WorktreeCleanupTrigger, started_at: u64) -> Self {
        Self {
            trigger,
            started_at,
            finished_at: started_at,
            entries: Vec::new(),
            removed: 0,
            kept: 0,
            failed: 0,
            omitted: 0,
        }
    }

    pub fn push(&mut self, entry: WorktreeCleanupEntry) {
        match entry.outcome {
            WorktreeCleanupOutcome::Removed => self.removed += 1,
            WorktreeCleanupOutcome::Kept => self.kept += 1,
            WorktreeCleanupOutcome::Failed => self.failed += 1,
        }
        if self.entries.len() < MAX_REPORT_ENTRIES {
            self.entries.push(entry);
        } else {
            self.omitted += 1;
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, TS)]
pub struct WorktreeCleanupStatus {
    /// Unix seconds of the last finished run, automatic or manual.
    pub last_run_at: Option<u64>,
    pub last_report: Option<WorktreeCleanupReport>,
    /// When the next automatic run is due, if any rule is enabled.
    pub next_run_at: Option<u64>,
    pub running: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::DaemonSettings;

    #[test]
    fn settings_without_storage_get_safe_defaults() {
        let settings: DaemonSettings =
            serde_json::from_str(r#"{"computer_use_enabled":true}"#).unwrap();
        let storage = &settings.storage;
        assert_eq!(storage.worktrees_directory, None);
        assert!(storage.previous_worktrees_directories.is_empty());
        let cleanup = &storage.worktree_cleanup;
        assert!(!cleanup.any_rule_enabled());
        assert_eq!(cleanup.keep_when, WorktreeKeepWhen::UncommittedChanges);
        assert!(!cleanup.remove_unpushed_commits);
        assert_eq!(cleanup.interval_hours, DEFAULT_CLEANUP_INTERVAL_HOURS);
        // An untouched document stays as small as before.
        let value = serde_json::to_value(&settings).unwrap();
        assert!(value.get("storage").is_none());
    }

    #[test]
    fn partial_storage_keeps_defaults_for_missing_fields() {
        let settings: DaemonSettings = serde_json::from_str(
            r#"{"storage":{"worktree_cleanup":{"merged":true,"keep_when":"any_local_files"}}}"#,
        )
        .unwrap();
        let cleanup = &settings.storage.worktree_cleanup;
        assert!(cleanup.merged);
        assert!(!cleanup.deleted_tasks);
        assert_eq!(cleanup.keep_when, WorktreeKeepWhen::AnyLocalFiles);
        assert_eq!(cleanup.interval_hours, DEFAULT_CLEANUP_INTERVAL_HOURS);
        let round_trip: DaemonSettings =
            serde_json::from_value(serde_json::to_value(&settings).unwrap()).unwrap();
        assert_eq!(round_trip, settings);
    }

    #[test]
    fn normalization_clamps_rules_and_drops_invalid_locations() {
        let home = Path::new("/home/user");
        let storage = StorageSettings {
            worktrees_directory: Some(PathBuf::from("relative/path")),
            previous_worktrees_directories: vec![
                PathBuf::from("/old"),
                PathBuf::from("/old"),
                PathBuf::from("/"),
                PathBuf::from("~/elsewhere"),
            ],
            worktree_cleanup: WorktreeCleanupSettings {
                inactive_after_days: Some(0),
                finished_after_days: Some(99_999),
                interval_hours: 0,
                ..WorktreeCleanupSettings::default()
            },
        }
        .normalized(Some(home));
        assert_eq!(storage.worktrees_directory, None);
        assert_eq!(
            storage.previous_worktrees_directories,
            vec![PathBuf::from("/old"), home.join("elsewhere")]
        );
        assert_eq!(storage.worktree_cleanup.inactive_after_days, Some(1));
        assert_eq!(
            storage.worktree_cleanup.finished_after_days,
            Some(MAX_RETENTION_DAYS)
        );
        assert_eq!(
            storage.worktree_cleanup.interval_hours,
            MIN_CLEANUP_INTERVAL_HOURS
        );
    }

    #[test]
    fn worktree_locations_expand_home_and_refuse_roots() {
        let home = Path::new("/home/user");
        assert_eq!(
            validate_worktrees_directory(" ~/trees ", Some(home)),
            Ok(home.join("trees"))
        );
        assert_eq!(
            validate_worktrees_directory("~", Some(home)),
            Ok(home.to_path_buf())
        );
        assert_eq!(
            validate_worktrees_directory("", Some(home)),
            Err(WorktreeDirectoryError::Empty)
        );
        assert_eq!(
            validate_worktrees_directory("trees", Some(home)),
            Err(WorktreeDirectoryError::NotAbsolute)
        );
        #[cfg(unix)]
        {
            assert_eq!(
                validate_worktrees_directory("/", Some(home)),
                Err(WorktreeDirectoryError::FilesystemRoot)
            );
            assert_eq!(
                validate_worktrees_directory("/srv/./trees/", None),
                Ok(PathBuf::from("/srv/trees"))
            );
        }
    }

    #[test]
    fn reports_bound_their_rows_but_count_everything() {
        let mut report = WorktreeCleanupReport::new(WorktreeCleanupTrigger::Manual, 1);
        for index in 0..MAX_REPORT_ENTRIES + 5 {
            report.push(WorktreeCleanupEntry {
                path: PathBuf::from(format!("/w/{index}")),
                branch: None,
                task_id: None,
                task_title: None,
                outcome: if index % 2 == 0 {
                    WorktreeCleanupOutcome::Kept
                } else {
                    WorktreeCleanupOutcome::Removed
                },
                reason: WorktreeCleanupReason::NoRuleApplies,
                branch_deleted: false,
            });
        }
        assert_eq!(report.entries.len(), MAX_REPORT_ENTRIES);
        assert_eq!(report.omitted, 5);
        assert_eq!(
            report.kept + report.removed,
            (MAX_REPORT_ENTRIES + 5) as u32
        );
    }
}
