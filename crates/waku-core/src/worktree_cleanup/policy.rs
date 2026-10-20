//! Pure cleanup decision for one worktree. Everything it needs is gathered
//! first (task state by the daemon, repository state by [`super::git`]), so
//! the rules can be tested as a table without Git or a daemon.

use waku_protocol::storage::{WorktreeCleanupReason, WorktreeCleanupSettings, WorktreeKeepWhen};

/// Task-side facts for one worktree, merged over every task that uses it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskFacts {
    /// The worktree belongs to a deleted task and no remaining task.
    pub deleted: bool,
    /// Something that must not lose its directory: a live provider runtime,
    /// an open terminal, or a running turn.
    pub blocked: Option<WorktreeCleanupReason>,
    /// Whole days since the newest activity of any task using the worktree.
    pub idle_days: u64,
    /// Every task's last turn completed and nothing is running.
    pub finished: bool,
}

/// Repository-side facts for one worktree, from [`super::git::inspect`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GitFacts {
    /// The checked-out branch is still the task's `waku/` branch.
    pub branch_matches: bool,
    /// Modified, staged, deleted or conflicted tracked files.
    pub tracked_changes: u32,
    pub untracked_files: u32,
    /// An ignored path other than a dependency cache, when inspected.
    pub ignored_example: Option<String>,
    /// Commits reachable only from this branch and not patch-equivalent to
    /// the base: work that exists nowhere else.
    pub unpushed_commits: u32,
    /// The branch the worktree is compared with, when one is known.
    pub base: Option<String>,
    /// Commits in `base..HEAD`.
    pub ahead_of_base: Option<u32>,
    /// Commits in `base..HEAD` with no patch-equivalent commit in the base.
    pub unmerged_commits: Option<u32>,
    /// The branch moved after Waku created it (its reflog has more than the
    /// creation entry), so an empty `base..HEAD` means merged, not untouched.
    pub branch_advanced: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Decision {
    Remove(WorktreeCleanupReason),
    Keep(WorktreeCleanupReason),
}

/// Whether removing this worktree would discard local work the rules
/// protect. Checked before eligibility, and again just before removal.
pub fn protected(rules: &WorktreeCleanupSettings, git: &GitFacts) -> Option<WorktreeCleanupReason> {
    if !git.branch_matches {
        return Some(WorktreeCleanupReason::BranchChanged);
    }
    let uncommitted = match rules.keep_when {
        WorktreeKeepWhen::TrackedChanges => git.tracked_changes,
        WorktreeKeepWhen::UncommittedChanges | WorktreeKeepWhen::AnyLocalFiles => {
            git.tracked_changes + git.untracked_files
        }
    };
    if uncommitted > 0 {
        return Some(WorktreeCleanupReason::UncommittedChanges { files: uncommitted });
    }
    if rules.keep_when == WorktreeKeepWhen::AnyLocalFiles
        && let Some(example) = &git.ignored_example
    {
        return Some(WorktreeCleanupReason::IgnoredFiles {
            example: example.clone(),
        });
    }
    if git.unpushed_commits > 0 && !rules.remove_unpushed_commits {
        return Some(WorktreeCleanupReason::UnpushedCommits {
            commits: git.unpushed_commits,
        });
    }
    None
}

/// Decide one worktree. Safety comes first: a blocked or protected worktree
/// is kept whichever rule would otherwise remove it.
pub fn evaluate(rules: &WorktreeCleanupSettings, task: &TaskFacts, git: &GitFacts) -> Decision {
    if task.deleted && !rules.deleted_tasks {
        return Decision::Keep(WorktreeCleanupReason::NoRuleApplies);
    }
    if !task.deleted && !rules.any_rule_enabled() {
        return Decision::Keep(WorktreeCleanupReason::NoRuleApplies);
    }
    if let Some(reason) = &task.blocked {
        return Decision::Keep(reason.clone());
    }
    if let Some(reason) = protected(rules, git) {
        return Decision::Keep(reason);
    }
    if task.deleted {
        return Decision::Remove(WorktreeCleanupReason::TaskDeleted);
    }
    if let Some(after) = rules.inactive_after_days
        && task.idle_days >= u64::from(after)
    {
        return Decision::Remove(WorktreeCleanupReason::Inactive {
            days: task.idle_days,
        });
    }
    if let Some(after) = rules.finished_after_days
        && task.finished
        && task.idle_days >= u64::from(after)
    {
        return Decision::Remove(WorktreeCleanupReason::Finished {
            days: task.idle_days,
        });
    }
    let base = git.base.clone().unwrap_or_default();
    if rules.merged
        && git.unmerged_commits == Some(0)
        && (git.ahead_of_base.is_some_and(|ahead| ahead > 0) || git.branch_advanced)
    {
        return Decision::Remove(WorktreeCleanupReason::Merged { base });
    }
    if rules.unchanged && git.ahead_of_base == Some(0) {
        return Decision::Remove(WorktreeCleanupReason::Unchanged { base });
    }
    Decision::Keep(not_eligible(rules, task, git, base))
}

/// The most useful explanation for a worktree no rule selected, preferring
/// the rule the user is most likely waiting on.
fn not_eligible(
    rules: &WorktreeCleanupSettings,
    task: &TaskFacts,
    git: &GitFacts,
    base: String,
) -> WorktreeCleanupReason {
    if let Some(of) = rules.inactive_after_days {
        return WorktreeCleanupReason::InactiveFor {
            days: task.idle_days,
            of,
        };
    }
    if let Some(of) = rules.finished_after_days {
        return if task.finished {
            WorktreeCleanupReason::FinishedFor {
                days: task.idle_days,
                of,
            }
        } else {
            WorktreeCleanupReason::NotFinished
        };
    }
    if (rules.merged || rules.unchanged) && git.base.is_none() {
        return WorktreeCleanupReason::BaseUnavailable;
    }
    if rules.merged {
        return WorktreeCleanupReason::NotMerged { base };
    }
    if rules.unchanged {
        return WorktreeCleanupReason::HasCommits {
            commits: git.ahead_of_base.unwrap_or_default(),
            base,
        };
    }
    WorktreeCleanupReason::NoRuleApplies
}

#[cfg(test)]
mod tests {
    use super::*;
    use WorktreeCleanupReason as Reason;

    fn clean() -> GitFacts {
        GitFacts {
            branch_matches: true,
            base: Some("origin/main".into()),
            ahead_of_base: Some(0),
            unmerged_commits: Some(0),
            ..GitFacts::default()
        }
    }

    fn idle(days: u64) -> TaskFacts {
        TaskFacts {
            idle_days: days,
            finished: true,
            ..TaskFacts::default()
        }
    }

    fn rules(edit: impl FnOnce(&mut WorktreeCleanupSettings)) -> WorktreeCleanupSettings {
        let mut rules = WorktreeCleanupSettings::default();
        edit(&mut rules);
        rules
    }

    fn with_commits(ahead: u32, unmerged: u32, unpushed: u32) -> GitFacts {
        GitFacts {
            ahead_of_base: Some(ahead),
            unmerged_commits: Some(unmerged),
            unpushed_commits: unpushed,
            ..clean()
        }
    }

    #[test]
    fn policy_table() {
        let base = || "origin/main".to_owned();
        let cases: Vec<(&str, WorktreeCleanupSettings, TaskFacts, GitFacts, Decision)> = vec![
            (
                "defaults never remove anything",
                WorktreeCleanupSettings::default(),
                idle(400),
                clean(),
                Decision::Keep(Reason::NoRuleApplies),
            ),
            (
                "deleted task without the deleted rule is kept",
                rules(|r| r.unchanged = true),
                TaskFacts {
                    deleted: true,
                    ..TaskFacts::default()
                },
                clean(),
                Decision::Keep(Reason::NoRuleApplies),
            ),
            (
                "deleted task is removed",
                rules(|r| r.deleted_tasks = true),
                TaskFacts {
                    deleted: true,
                    ..TaskFacts::default()
                },
                with_commits(2, 2, 0),
                Decision::Remove(Reason::TaskDeleted),
            ),
            (
                "deleted task with unpushed commits is kept",
                rules(|r| r.deleted_tasks = true),
                TaskFacts {
                    deleted: true,
                    ..TaskFacts::default()
                },
                with_commits(2, 2, 2),
                Decision::Keep(Reason::UnpushedCommits { commits: 2 }),
            ),
            (
                "unpushed commits are removable when explicitly allowed",
                rules(|r| {
                    r.deleted_tasks = true;
                    r.remove_unpushed_commits = true;
                }),
                TaskFacts {
                    deleted: true,
                    ..TaskFacts::default()
                },
                with_commits(2, 2, 2),
                Decision::Remove(Reason::TaskDeleted),
            ),
            (
                "inactive past the threshold",
                rules(|r| r.inactive_after_days = Some(14)),
                idle(20),
                with_commits(3, 3, 0),
                Decision::Remove(Reason::Inactive { days: 20 }),
            ),
            (
                "inactive below the threshold",
                rules(|r| r.inactive_after_days = Some(14)),
                idle(3),
                with_commits(3, 3, 0),
                Decision::Keep(Reason::InactiveFor { days: 3, of: 14 }),
            ),
            (
                "running task is never removed",
                rules(|r| r.inactive_after_days = Some(1)),
                TaskFacts {
                    blocked: Some(Reason::TaskRunning),
                    ..idle(30)
                },
                clean(),
                Decision::Keep(Reason::TaskRunning),
            ),
            (
                "open terminal protects the worktree",
                rules(|r| r.deleted_tasks = true),
                TaskFacts {
                    deleted: true,
                    blocked: Some(Reason::OpenTerminal),
                    ..TaskFacts::default()
                },
                clean(),
                Decision::Keep(Reason::OpenTerminal),
            ),
            (
                "uncommitted changes protect by default",
                rules(|r| r.inactive_after_days = Some(1)),
                idle(30),
                GitFacts {
                    tracked_changes: 1,
                    untracked_files: 2,
                    ..clean()
                },
                Decision::Keep(Reason::UncommittedChanges { files: 3 }),
            ),
            (
                "tracked-changes level ignores untracked files",
                rules(|r| {
                    r.inactive_after_days = Some(1);
                    r.keep_when = WorktreeKeepWhen::TrackedChanges;
                }),
                idle(30),
                GitFacts {
                    untracked_files: 2,
                    ..clean()
                },
                Decision::Remove(Reason::Inactive { days: 30 }),
            ),
            (
                "tracked-changes level still keeps tracked edits",
                rules(|r| {
                    r.inactive_after_days = Some(1);
                    r.keep_when = WorktreeKeepWhen::TrackedChanges;
                }),
                idle(30),
                GitFacts {
                    tracked_changes: 1,
                    untracked_files: 2,
                    ..clean()
                },
                Decision::Keep(Reason::UncommittedChanges { files: 1 }),
            ),
            (
                "any-local-files level keeps ignored files",
                rules(|r| {
                    r.inactive_after_days = Some(1);
                    r.keep_when = WorktreeKeepWhen::AnyLocalFiles;
                }),
                idle(30),
                GitFacts {
                    ignored_example: Some(".env".into()),
                    ..clean()
                },
                Decision::Keep(Reason::IgnoredFiles {
                    example: ".env".into(),
                }),
            ),
            (
                "ignored files do not protect at the default level",
                rules(|r| r.inactive_after_days = Some(1)),
                idle(30),
                GitFacts {
                    ignored_example: Some(".env".into()),
                    ..clean()
                },
                Decision::Remove(Reason::Inactive { days: 30 }),
            ),
            (
                "a switched branch is kept",
                rules(|r| r.unchanged = true),
                idle(0),
                GitFacts {
                    branch_matches: false,
                    ..clean()
                },
                Decision::Keep(Reason::BranchChanged),
            ),
            (
                "finished for long enough",
                rules(|r| r.finished_after_days = Some(2)),
                idle(2),
                with_commits(1, 1, 0),
                Decision::Remove(Reason::Finished { days: 2 }),
            ),
            (
                "finished immediately with zero days",
                rules(|r| r.finished_after_days = Some(0)),
                idle(0),
                with_commits(1, 1, 0),
                Decision::Remove(Reason::Finished { days: 0 }),
            ),
            (
                "a failed or unsettled task is not finished",
                rules(|r| r.finished_after_days = Some(0)),
                TaskFacts {
                    finished: false,
                    ..idle(10)
                },
                with_commits(1, 1, 0),
                Decision::Keep(Reason::NotFinished),
            ),
            (
                "merged by rebase: commits ahead but all patch-equivalent",
                rules(|r| r.merged = true),
                idle(0),
                with_commits(3, 0, 0),
                Decision::Remove(Reason::Merged { base: base() }),
            ),
            (
                "merged by fast-forward: nothing ahead, branch advanced",
                rules(|r| r.merged = true),
                idle(0),
                GitFacts {
                    branch_advanced: true,
                    ..clean()
                },
                Decision::Remove(Reason::Merged { base: base() }),
            ),
            (
                "an untouched branch is not reported as merged",
                rules(|r| r.merged = true),
                idle(0),
                clean(),
                Decision::Keep(Reason::NotMerged { base: base() }),
            ),
            (
                "unmerged commits keep a merged-rule worktree",
                rules(|r| r.merged = true),
                idle(0),
                with_commits(3, 1, 0),
                Decision::Keep(Reason::NotMerged { base: base() }),
            ),
            (
                "unchanged: nothing beyond the base",
                rules(|r| r.unchanged = true),
                idle(0),
                clean(),
                Decision::Remove(Reason::Unchanged { base: base() }),
            ),
            (
                "unchanged rule keeps a branch with commits",
                rules(|r| r.unchanged = true),
                idle(0),
                with_commits(2, 2, 0),
                Decision::Keep(Reason::HasCommits {
                    commits: 2,
                    base: base(),
                }),
            ),
            (
                "unknown base keeps merged and unchanged worktrees",
                rules(|r| {
                    r.merged = true;
                    r.unchanged = true;
                }),
                idle(0),
                GitFacts {
                    branch_matches: true,
                    ..GitFacts::default()
                },
                Decision::Keep(Reason::BaseUnavailable),
            ),
        ];
        for (name, rules, task, git, expected) in cases {
            assert_eq!(evaluate(&rules, &task, &git), expected, "{name}");
        }
    }
}
