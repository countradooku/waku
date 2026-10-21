//! Real-Git integration tests for worktree cleanup.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use uuid::Uuid;
use waku_protocol::storage::{
    StorageSettings, WorktreeCleanupOutcome, WorktreeCleanupReason, WorktreeCleanupReport,
    WorktreeCleanupSettings, WorktreeCleanupTrigger, WorktreeKeepWhen,
};

use super::{CleanupHost, DeletedWorktree, TaskWorktree, WorktreeCleanup, now_seconds};

const DAY: u64 = 24 * 60 * 60;

fn git(cwd: &Path, args: &[&str]) -> String {
    let output = crate::git_workflow::git(cwd, args).unwrap();
    String::from_utf8(output).unwrap()
}

fn commit(cwd: &Path, message: &str) {
    git(cwd, &["add", "-A", "--", "."]);
    git(
        cwd,
        &[
            "-c",
            "user.name=Waku Tests",
            "-c",
            "user.email=waku@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            message,
        ],
    );
}

struct Fixture {
    root: PathBuf,
    repository: PathBuf,
    worktrees: PathBuf,
    cleanup: WorktreeCleanup,
    project_id: Uuid,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("waku-cleanup-{}", Uuid::new_v4()));
        let repository = root.join("repository");
        fs::create_dir_all(&repository).unwrap();
        let repository = dunce::canonicalize(&repository).unwrap();
        git(&repository, &["init", "-b", "main"]);
        git(&repository, &["config", "core.autocrlf", "false"]);
        fs::write(repository.join("README.md"), "main\n").unwrap();
        fs::write(repository.join(".gitignore"), ".env\nnode_modules/\n").unwrap();
        commit(&repository, "initial");
        let worktrees = root.join("worktrees");
        let cleanup = WorktreeCleanup::open(root.join("state/worktree-cleanup.json"));
        Self {
            root,
            repository,
            worktrees,
            cleanup,
            project_id: Uuid::new_v4(),
        }
    }

    fn create(&self, prompt: &str) -> (PathBuf, String) {
        let created = crate::worktree::create_in(
            &self.repository,
            &self.worktrees,
            self.project_id,
            Uuid::new_v4(),
            prompt,
            None,
        )
        .unwrap();
        (created.path, created.branch)
    }

    fn storage(&self, edit: impl FnOnce(&mut WorktreeCleanupSettings)) -> StorageSettings {
        let mut rules = WorktreeCleanupSettings::default();
        edit(&mut rules);
        StorageSettings {
            worktrees_directory: Some(self.worktrees.clone()),
            previous_worktrees_directories: Vec::new(),
            worktree_cleanup: rules,
        }
    }

    fn run(&self, host: &Host, storage: &StorageSettings) -> WorktreeCleanupReport {
        self.cleanup
            .run(host, storage, WorktreeCleanupTrigger::Manual)
    }

    fn delete(&self, path: &Path, branch: &str) -> Uuid {
        let task_id = Uuid::new_v4();
        self.cleanup.record_deleted(DeletedWorktree {
            task_id,
            title: "Deleted task".into(),
            workspace_path: path.to_path_buf(),
            branch: branch.to_owned(),
            deleted_at: now_seconds(),
        });
        task_id
    }

    fn branch_exists(&self, branch: &str) -> bool {
        crate::git_workflow::git(
            &self.repository,
            &[
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("refs/heads/{branch}"),
            ],
        )
        .is_ok()
    }

    fn linked_worktrees(&self) -> usize {
        crate::git_workflow::list_worktrees(&self.repository)
            .unwrap()
            .len()
            - 1
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[derive(Default)]
struct Host {
    tasks: Vec<TaskWorktree>,
    blocked: Mutex<HashSet<Uuid>>,
}

impl Host {
    fn with_task(path: &Path, branch: &str, idle_days: u64) -> (Self, Uuid) {
        let task_id = Uuid::new_v4();
        let host = Self {
            tasks: vec![TaskWorktree {
                task_id,
                title: "Live task".into(),
                workspace_path: path.to_path_buf(),
                branch: branch.to_owned(),
                activity_at: now_seconds() - idle_days * DAY,
                finished: true,
            }],
            blocked: Mutex::default(),
        };
        (host, task_id)
    }
}

impl CleanupHost for Host {
    fn tasks(&self) -> anyhow::Result<Vec<TaskWorktree>> {
        Ok(self.tasks.clone())
    }

    fn blocked(&self, task_ids: &[Uuid]) -> Option<WorktreeCleanupReason> {
        let blocked = self.blocked.lock();
        task_ids
            .iter()
            .any(|id| blocked.contains(id))
            .then_some(WorktreeCleanupReason::LiveRuntime)
    }
}

fn only(report: &WorktreeCleanupReport) -> &waku_protocol::storage::WorktreeCleanupEntry {
    assert_eq!(report.entries.len(), 1, "{report:#?}");
    &report.entries[0]
}

#[test]
fn deleted_task_worktree_is_removed_with_its_branch_and_metadata() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Fix the parser");
    // A stale registration of another worktree whose folder vanished.
    let (stale, _) = fixture.create("Gone already");
    fs::remove_dir_all(&stale).unwrap();
    fixture.delete(&path, &branch);

    let report = fixture.run(
        &Host::default(),
        &fixture.storage(|rules| rules.deleted_tasks = true),
    );

    let entry = only(&report);
    assert_eq!(entry.outcome, WorktreeCleanupOutcome::Removed);
    assert_eq!(entry.reason, WorktreeCleanupReason::TaskDeleted);
    assert!(entry.branch_deleted);
    assert!(!path.exists());
    assert!(!fixture.branch_exists(&branch));
    // `git worktree prune` dropped the vanished folder's registration too.
    assert_eq!(fixture.linked_worktrees(), 0);
    assert!(fixture.cleanup.deleted_records().is_empty());
    let status = fixture.cleanup.status(&StorageSettings::default());
    assert_eq!(status.last_report, Some(report.clone()));
    assert!(status.last_run_at.is_some());
}

#[test]
fn deleted_rule_off_reports_without_touching_git() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Keep me");
    fixture.delete(&path, &branch);

    let report = fixture.run(&Host::default(), &fixture.storage(|_| {}));

    assert_eq!(only(&report).reason, WorktreeCleanupReason::NoRuleApplies);
    assert!(path.exists());
    assert_eq!(fixture.cleanup.deleted_records().len(), 1);
}

#[test]
fn dirty_worktrees_are_refused() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Dirty work");
    fs::write(path.join("README.md"), "edited\n").unwrap();
    fs::write(path.join("notes.txt"), "draft\n").unwrap();
    fixture.delete(&path, &branch);

    let storage = fixture.storage(|rules| rules.deleted_tasks = true);
    let report = fixture.run(&Host::default(), &storage);

    let entry = only(&report);
    assert_eq!(entry.outcome, WorktreeCleanupOutcome::Kept);
    assert_eq!(
        entry.reason,
        WorktreeCleanupReason::UncommittedChanges { files: 2 }
    );
    assert!(path.join("notes.txt").exists());
    assert!(fixture.branch_exists(&branch));
    assert_eq!(fixture.linked_worktrees(), 1);
}

#[test]
fn ignored_files_protect_only_at_the_any_local_files_level() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Secrets");
    fs::write(path.join(".env"), "TOKEN=1\n").unwrap();
    fs::create_dir_all(path.join("node_modules/pkg")).unwrap();
    fs::write(path.join("node_modules/pkg/index.js"), "").unwrap();
    fixture.delete(&path, &branch);

    let strict = fixture.storage(|rules| {
        rules.deleted_tasks = true;
        rules.keep_when = WorktreeKeepWhen::AnyLocalFiles;
    });
    let report = fixture.run(&Host::default(), &strict);
    assert_eq!(
        only(&report).reason,
        WorktreeCleanupReason::IgnoredFiles {
            example: ".env".into()
        }
    );
    assert!(path.exists());

    let report = fixture.run(
        &Host::default(),
        &fixture.storage(|rules| rules.deleted_tasks = true),
    );
    assert_eq!(only(&report).outcome, WorktreeCleanupOutcome::Removed);
    assert!(!path.exists());
}

#[test]
fn tracked_changes_level_cleans_untracked_files_before_removing() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Scratch files");
    fs::write(path.join("scratch.txt"), "temporary\n").unwrap();
    fixture.delete(&path, &branch);

    let report = fixture.run(
        &Host::default(),
        &fixture.storage(|rules| {
            rules.deleted_tasks = true;
            rules.keep_when = WorktreeKeepWhen::TrackedChanges;
        }),
    );

    assert_eq!(only(&report).outcome, WorktreeCleanupOutcome::Removed);
    assert!(!path.exists());
    assert_eq!(fixture.linked_worktrees(), 0);
}

#[test]
fn unpushed_commits_are_refused_unless_allowed_and_then_keep_the_branch() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Agent commits");
    fs::write(path.join("feature.txt"), "work\n").unwrap();
    commit(&path, "agent work");
    fixture.delete(&path, &branch);

    let report = fixture.run(
        &Host::default(),
        &fixture.storage(|rules| rules.deleted_tasks = true),
    );
    assert_eq!(
        only(&report).reason,
        WorktreeCleanupReason::UnpushedCommits { commits: 1 }
    );
    assert!(path.exists());

    let report = fixture.run(
        &Host::default(),
        &fixture.storage(|rules| {
            rules.deleted_tasks = true;
            rules.remove_unpushed_commits = true;
        }),
    );
    let entry = only(&report);
    assert_eq!(entry.outcome, WorktreeCleanupOutcome::Removed);
    assert!(!entry.branch_deleted);
    assert!(!path.exists());
    // The commit survives on its branch.
    assert!(fixture.branch_exists(&branch));
}

#[test]
fn commits_merged_elsewhere_are_not_unpushed() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Merged work");
    fs::write(path.join("feature.txt"), "work\n").unwrap();
    commit(&path, "agent work");
    git(&fixture.repository, &["merge", "--ff-only", &branch]);
    fixture.delete(&path, &branch);

    let report = fixture.run(
        &Host::default(),
        &fixture.storage(|rules| rules.deleted_tasks = true),
    );

    let entry = only(&report);
    assert_eq!(entry.outcome, WorktreeCleanupOutcome::Removed);
    assert!(entry.branch_deleted);
}

#[test]
fn inactive_live_task_is_removed_and_restored_on_its_next_turn() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Old task");
    fs::write(path.join("feature.txt"), "work\n").unwrap();
    commit(&path, "agent work");
    let (host, _) = Host::with_task(&path, &branch, 30);
    let storage = fixture.storage(|rules| {
        rules.inactive_after_days = Some(14);
        // The commit exists only on this branch; a live task keeps its
        // branch, so removing the checkout loses nothing.
        rules.remove_unpushed_commits = true;
    });

    let report = fixture.run(&host, &storage);
    let entry = only(&report);
    assert_eq!(entry.reason, WorktreeCleanupReason::Inactive { days: 30 });
    assert!(!entry.branch_deleted);
    assert!(!path.exists());
    assert!(fixture.branch_exists(&branch));

    assert!(fixture.cleanup.restore_if_removed(&path).unwrap());
    assert_eq!(
        fs::read_to_string(path.join("feature.txt")).unwrap(),
        "work\n"
    );
    assert!(!fixture.cleanup.restore_if_removed(&path).unwrap());
}

#[test]
fn recent_and_blocked_tasks_are_kept() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Recent task");
    let (host, task_id) = Host::with_task(&path, &branch, 2);
    let storage = fixture.storage(|rules| rules.inactive_after_days = Some(14));

    let report = fixture.run(&host, &storage);
    assert_eq!(
        only(&report).reason,
        WorktreeCleanupReason::InactiveFor { days: 2, of: 14 }
    );

    let (mut host, _) = Host::with_task(&path, &branch, 30);
    host.tasks[0].task_id = task_id;
    host.blocked.lock().insert(task_id);
    let report = fixture.run(&host, &storage);
    assert_eq!(only(&report).reason, WorktreeCleanupReason::LiveRuntime);
    assert!(path.exists());
}

#[test]
fn merged_and_unchanged_rules_compare_with_the_base_branch() {
    let fixture = Fixture::new();
    let (untouched, untouched_branch) = fixture.create("Untouched");
    let (merged, merged_branch) = fixture.create("Merged");
    fs::write(merged.join("feature.txt"), "work\n").unwrap();
    commit(&merged, "agent work");
    git(&fixture.repository, &["merge", "--ff-only", &merged_branch]);
    let (open, open_branch) = fixture.create("Still open");
    fs::write(open.join("other.txt"), "work\n").unwrap();
    commit(&open, "unmerged work");

    let host = Host {
        tasks: [
            (&untouched, &untouched_branch),
            (&merged, &merged_branch),
            (&open, &open_branch),
        ]
        .into_iter()
        .map(|(path, branch)| TaskWorktree {
            task_id: Uuid::new_v4(),
            title: branch.clone(),
            workspace_path: path.clone(),
            branch: branch.clone(),
            activity_at: now_seconds(),
            finished: true,
        })
        .collect(),
        blocked: Mutex::default(),
    };

    let report = fixture.run(&host, &fixture.storage(|rules| rules.merged = true));
    let reason_for = |path: &Path| {
        let canonical = dunce::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        report
            .entries
            .iter()
            .find(|entry| {
                entry.path == canonical || entry.path == path || canonical.starts_with(&entry.path)
            })
            .unwrap()
            .reason
            .clone()
    };
    let base = || "main".to_owned();
    assert_eq!(
        reason_for(&untouched),
        WorktreeCleanupReason::NotMerged { base: base() }
    );
    // Its only commit exists nowhere else, which protects it first.
    assert_eq!(
        reason_for(&open),
        WorktreeCleanupReason::UnpushedCommits { commits: 1 }
    );
    assert!(!merged.exists());
    assert_eq!(report.removed, 1);

    let report = fixture.run(&host, &fixture.storage(|rules| rules.unchanged = true));
    assert_eq!(report.removed, 1);
    assert!(!untouched.exists());
    assert!(open.exists());
}

#[test]
fn worktrees_outside_the_managed_folder_are_never_removed() {
    let fixture = Fixture::new();
    let outside = fixture.root.join("elsewhere");
    git(
        &fixture.repository,
        &[
            "worktree",
            "add",
            "-b",
            "waku/outside",
            outside.to_str().unwrap(),
        ],
    );
    fixture.delete(&outside, "waku/outside");

    let report = fixture.run(
        &Host::default(),
        &fixture.storage(|rules| rules.deleted_tasks = true),
    );

    assert_eq!(
        only(&report).reason,
        WorktreeCleanupReason::OutsideManagedFolder
    );
    assert!(outside.exists());
}

#[test]
fn a_switched_branch_is_kept() {
    let fixture = Fixture::new();
    let (path, branch) = fixture.create("Switched");
    git(&path, &["checkout", "-b", "user-branch"]);
    fixture.delete(&path, &branch);

    let report = fixture.run(
        &Host::default(),
        &fixture.storage(|rules| rules.deleted_tasks = true),
    );

    assert_eq!(only(&report).reason, WorktreeCleanupReason::BranchChanged);
    assert!(path.exists());
}

#[test]
fn automatic_runs_wait_for_their_interval() {
    let fixture = Fixture::new();
    let off = fixture.storage(|_| {});
    assert_eq!(fixture.cleanup.automatic_delay(&off, now_seconds()), None);

    let on = fixture.storage(|rules| {
        rules.deleted_tasks = true;
        rules.interval_hours = 6;
    });
    // Never run: due now, after the startup delay.
    assert_eq!(
        fixture.cleanup.automatic_delay(&on, now_seconds()),
        Some(super::STARTUP_DELAY)
    );
    fixture
        .cleanup
        .run(&Host::default(), &on, WorktreeCleanupTrigger::Automatic);
    let delay = fixture.cleanup.automatic_delay(&on, now_seconds()).unwrap();
    assert!(delay > std::time::Duration::from_secs(5 * 60 * 60));
    // A manual run does not move the automatic schedule.
    let before = fixture.cleanup.status(&on).next_run_at;
    fixture.run(&Host::default(), &on);
    assert_eq!(fixture.cleanup.status(&on).next_run_at, before);

    // Cleanup state survives a daemon restart.
    let reopened = WorktreeCleanup::open(fixture.root.join("state/worktree-cleanup.json"));
    assert_eq!(reopened.status(&on).next_run_at, before);

    // Settings changes wake the scheduler.
    let seen = fixture.cleanup.wake_generation();
    fixture.cleanup.wake_scheduler();
    assert!(fixture.cleanup.wait(seen, None));
}
