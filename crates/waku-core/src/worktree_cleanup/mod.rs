//! Daemon-side worktree cleanup (#62).
//!
//! Task worktrees live beneath one managed folder (`<root>/<project>/<slug>`)
//! and were never removed. This service applies the user's rules from
//! [`StorageSettings`]: it removes eligible worktrees with `git worktree
//! remove`, prunes Git's metadata, deletes the `waku/` branch of a deleted
//! task when no commit would be lost, and reports every worktree it kept with
//! the reason. It runs on daemon start and then at most every
//! `interval_hours`, from one thread that sleeps between runs, plus on demand
//! ("Delete now").
//!
//! Safety rules, in order: a worktree with a live runtime, open terminal or
//! running turn is never touched; a worktree outside the managed folder, on a
//! switched branch, or not linked to its repository is kept; local changes
//! and unpushed commits protect it according to the keep level; everything is
//! re-checked under the repository lock just before removal; and Git is never
//! asked to `--force`.

pub mod git;
pub mod policy;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use waku_protocol::storage::{
    StorageSettings, WorktreeCleanupEntry, WorktreeCleanupOutcome, WorktreeCleanupReason,
    WorktreeCleanupReport, WorktreeCleanupStatus, WorktreeCleanupTrigger,
};

use policy::{Decision, TaskFacts};

const DAY_SECONDS: u64 = 24 * 60 * 60;
const HOUR_SECONDS: u64 = 60 * 60;
/// The first automatic run waits this long after the daemon starts, so it
/// never competes with app launch.
pub const STARTUP_DELAY: Duration = Duration::from_secs(60);
const MAX_DELETED_RECORDS: usize = 1024;
const MAX_REMOVED_RECORDS: usize = 512;

/// One task's worktree binding, as the daemon's task store knows it.
#[derive(Clone, Debug)]
pub struct TaskWorktree {
    pub task_id: Uuid,
    pub title: String,
    /// The task's working directory: the worktree root or, for a project
    /// inside a larger repository, a subdirectory of it.
    pub workspace_path: PathBuf,
    pub branch: String,
    /// Unix seconds of the task's newest activity.
    pub activity_at: u64,
    /// The last turn completed and nothing is running.
    pub finished: bool,
}

/// What the daemon exposes to a cleanup run.
pub trait CleanupHost {
    /// Every live task that runs in a worktree.
    fn tasks(&self) -> anyhow::Result<Vec<TaskWorktree>>;
    /// Why these tasks' directory must stay, checked again right before a
    /// removal: a running turn, live provider runtime or open terminal.
    fn blocked(&self, task_ids: &[Uuid]) -> Option<WorktreeCleanupReason>;
}

/// A deleted task whose worktree became eligible for the "deleted tasks"
/// rule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeletedWorktree {
    pub task_id: Uuid,
    pub title: String,
    pub workspace_path: PathBuf,
    pub branch: String,
    pub deleted_at: u64,
}

/// A live task's worktree that cleanup removed while keeping its branch, so
/// the next turn can check it out again.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RemovedWorktree {
    toplevel: PathBuf,
    branch: String,
    repository: PathBuf,
    removed_at: u64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct CleanupState {
    last_run_at: Option<u64>,
    last_automatic_run_at: Option<u64>,
    last_report: Option<WorktreeCleanupReport>,
    deleted: Vec<DeletedWorktree>,
    removed: Vec<RemovedWorktree>,
}

pub struct WorktreeCleanup {
    path: PathBuf,
    state: Mutex<CleanupState>,
    run_lock: Mutex<()>,
    running: AtomicBool,
    /// Bumped to wake the scheduler when settings change.
    wake_generation: Mutex<u64>,
    wake: Condvar,
}

pub fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

/// `~/.waku-fork/worktrees`, where worktrees go unless the user moved them.
pub fn default_worktrees_directory() -> anyhow::Result<PathBuf> {
    Ok(dirs::home_dir()
        .context("could not locate the home directory for worktrees")?
        .join(crate::identity::HOME_DIRECTORY_NAME)
        .join("worktrees"))
}

/// The folder new worktrees are created in.
pub fn worktrees_directory(storage: &StorageSettings) -> anyhow::Result<PathBuf> {
    match &storage.worktrees_directory {
        Some(path) => Ok(path.clone()),
        None => default_worktrees_directory(),
    }
}

/// Every folder whose worktrees cleanup may remove: the current location,
/// earlier ones, and the default. Only existing folders, canonicalized, and
/// never a filesystem root.
fn managed_roots(storage: &StorageSettings) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    let candidates = storage
        .worktrees_directory
        .iter()
        .chain(&storage.previous_worktrees_directories)
        .cloned()
        .chain(default_worktrees_directory().ok());
    for root in candidates {
        if let Ok(root) = dunce::canonicalize(&root)
            && root.parent().is_some()
            && !roots.contains(&root)
        {
            roots.push(root);
        }
    }
    roots
}

/// The worktree directory (`<root>/<project>/<slug>`) holding `path`, when
/// `path` exists inside a managed root. Paths are compared canonically, so a
/// symlink cannot lead cleanup outside the managed folder.
fn managed_toplevel(roots: &[PathBuf], path: &Path) -> Option<PathBuf> {
    let path = dunce::canonicalize(path).ok()?;
    roots.iter().find_map(|root| {
        let mut rest = path.strip_prefix(root).ok()?.components();
        let (Some(Component::Normal(project)), Some(Component::Normal(slug))) =
            (rest.next(), rest.next())
        else {
            return None;
        };
        Some(root.join(project).join(slug))
    })
}

/// Canonicalize the deepest existing ancestor of a path that may not exist.
fn canonicalize_lossy(path: &Path) -> PathBuf {
    let mut existing = path;
    let mut missing = Vec::new();
    loop {
        if let Ok(canonical) = dunce::canonicalize(existing) {
            return missing
                .iter()
                .rev()
                .fold(canonical, |path: PathBuf, part| path.join(part));
        }
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                missing.push(name.to_owned());
                existing = parent;
            }
            _ => return path.to_path_buf(),
        }
    }
}

struct Candidate {
    toplevel: PathBuf,
    branch: String,
    task_ids: Vec<Uuid>,
    title: String,
    deleted: bool,
    activity_at: u64,
    finished: bool,
}

impl WorktreeCleanup {
    /// Open the cleanup state stored next to the task database.
    pub fn open(path: PathBuf) -> Self {
        let state = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|error| {
                eprintln!(
                    "Waku daemon ignored unreadable worktree cleanup state {}: {error}",
                    path.display()
                );
                CleanupState::default()
            }),
            Err(_) => CleanupState::default(),
        };
        Self {
            path,
            state: Mutex::new(state),
            run_lock: Mutex::new(()),
            running: AtomicBool::new(false),
            wake_generation: Mutex::new(0),
            wake: Condvar::new(),
        }
    }

    fn save(&self, state: &CleanupState) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_vec_pretty(state).map_err(io::Error::other)?;
        let temporary = self.path.with_extension("json.tmp");
        fs::write(&temporary, data)?;
        fs::rename(temporary, &self.path)
    }

    fn update(&self, edit: impl FnOnce(&mut CleanupState)) {
        let mut state = self.state.lock();
        edit(&mut state);
        if let Err(error) = self.save(&state) {
            eprintln!("Waku daemon could not save worktree cleanup state: {error}");
        }
    }

    /// Remember a deleted task's worktree for the "deleted tasks" rule.
    pub fn record_deleted(&self, deleted: DeletedWorktree) {
        self.update(|state| {
            state
                .deleted
                .retain(|existing| existing.task_id != deleted.task_id);
            state.deleted.push(deleted);
            let excess = state.deleted.len().saturating_sub(MAX_DELETED_RECORDS);
            state.deleted.drain(..excess);
        });
    }

    #[cfg(test)]
    fn deleted_records(&self) -> Vec<DeletedWorktree> {
        self.state.lock().deleted.clone()
    }

    pub fn status(&self, storage: &StorageSettings) -> WorktreeCleanupStatus {
        let state = self.state.lock();
        WorktreeCleanupStatus {
            last_run_at: state.last_run_at,
            last_report: state.last_report.clone(),
            next_run_at: Self::next_due(&state, storage),
            running: self.running.load(Ordering::Acquire),
        }
    }

    fn next_due(state: &CleanupState, storage: &StorageSettings) -> Option<u64> {
        let rules = &storage.worktree_cleanup;
        if !rules.any_rule_enabled() {
            return None;
        }
        let interval = u64::from(rules.clone().normalized().interval_hours) * HOUR_SECONDS;
        Some(
            state
                .last_automatic_run_at
                .map_or(0, |last| last.saturating_add(interval)),
        )
    }

    /// How long the scheduler should sleep before the next automatic run;
    /// `None` while no rule is enabled.
    pub fn automatic_delay(&self, storage: &StorageSettings, now: u64) -> Option<Duration> {
        let due = Self::next_due(&self.state.lock(), storage)?;
        Some(Duration::from_secs(due.saturating_sub(now)).max(STARTUP_DELAY))
    }

    /// Wake the scheduler to re-read the settings.
    pub fn wake_scheduler(&self) {
        *self.wake_generation.lock() += 1;
        self.wake.notify_all();
    }

    pub fn wake_generation(&self) -> u64 {
        *self.wake_generation.lock()
    }

    /// Sleep until `timeout` passes (forever when `None`) or the scheduler
    /// is woken after `seen`. Returns `true` when woken.
    pub fn wait(&self, seen: u64, timeout: Option<Duration>) -> bool {
        let mut generation = self.wake_generation.lock();
        if *generation != seen {
            return true;
        }
        match timeout {
            Some(timeout) => {
                let woken = !self.wake.wait_for(&mut generation, timeout).timed_out();
                woken || *generation != seen
            }
            None => {
                self.wake.wait(&mut generation);
                true
            }
        }
    }

    /// Check out a worktree that cleanup removed from a live task again, when
    /// that task's directory is requested. Returns whether it restored one.
    pub fn restore_if_removed(&self, cwd: &Path) -> anyhow::Result<bool> {
        if cwd.exists() {
            return Ok(false);
        }
        let wanted = canonicalize_lossy(cwd);
        let Some(record) = self
            .state
            .lock()
            .removed
            .iter()
            .find(|record| wanted.starts_with(&record.toplevel))
            .cloned()
        else {
            return Ok(false);
        };
        let lock = crate::git_workflow::repository_lock(&record.repository)?;
        let _guard = lock.lock();
        if !record.toplevel.exists() {
            git::restore(&record.repository, &record.toplevel, &record.branch)?;
        }
        self.update(|state| state.removed.retain(|existing| existing != &record));
        Ok(true)
    }

    /// Apply the rules once and store the report. Waits for a run already in
    /// progress, so "Delete now" never overlaps the scheduler.
    pub fn run(
        &self,
        host: &dyn CleanupHost,
        storage: &StorageSettings,
        trigger: WorktreeCleanupTrigger,
    ) -> WorktreeCleanupReport {
        let _run = self.run_lock.lock();
        self.running.store(true, Ordering::Release);
        let started_at = now_seconds();
        let mut report = WorktreeCleanupReport::new(trigger, started_at);
        self.run_inner(host, storage, started_at, &mut report);
        report.finished_at = now_seconds().max(started_at);
        self.update(|state| {
            state.last_run_at = Some(report.finished_at);
            if trigger == WorktreeCleanupTrigger::Automatic {
                state.last_automatic_run_at = Some(report.finished_at);
            }
            state.last_report = Some(report.clone());
        });
        self.running.store(false, Ordering::Release);
        report
    }

    fn run_inner(
        &self,
        host: &dyn CleanupHost,
        storage: &StorageSettings,
        now: u64,
        report: &mut WorktreeCleanupReport,
    ) {
        let rules = storage.worktree_cleanup.clone().normalized();
        let roots = managed_roots(storage);
        let tasks = match host.tasks() {
            Ok(tasks) => tasks,
            Err(error) => {
                report.push(WorktreeCleanupEntry {
                    path: PathBuf::new(),
                    branch: None,
                    task_id: None,
                    task_title: None,
                    outcome: WorktreeCleanupOutcome::Failed,
                    reason: WorktreeCleanupReason::Error {
                        message: format!("{error:#}"),
                    },
                    branch_deleted: false,
                });
                return;
            }
        };

        // Group live tasks by worktree: a forked task shares its source's
        // directory, and every task using it must agree before it goes.
        let mut groups: BTreeMap<PathBuf, Candidate> = BTreeMap::new();
        let mut outside = BTreeSet::new();
        for task in tasks {
            let Some(toplevel) = managed_toplevel(&roots, &task.workspace_path) else {
                if task.workspace_path.exists() {
                    outside.insert((
                        task.workspace_path.clone(),
                        task.branch,
                        task.task_id,
                        task.title,
                    ));
                }
                continue;
            };
            let candidate = groups.entry(toplevel.clone()).or_insert_with(|| Candidate {
                toplevel,
                branch: task.branch.clone(),
                task_ids: Vec::new(),
                title: task.title.clone(),
                deleted: false,
                activity_at: 0,
                finished: true,
            });
            if candidate.branch != task.branch {
                // Tasks disagree about the branch; `inspect` keeps it.
                candidate.branch.clear();
            }
            candidate.task_ids.push(task.task_id);
            candidate.activity_at = candidate.activity_at.max(task.activity_at);
            candidate.finished &= task.finished;
        }
        for (path, branch, task_id, title) in outside {
            report.push(WorktreeCleanupEntry {
                path,
                branch: Some(branch),
                task_id: Some(task_id),
                task_title: Some(title),
                outcome: WorktreeCleanupOutcome::Kept,
                reason: WorktreeCleanupReason::OutsideManagedFolder,
                branch_deleted: false,
            });
        }

        // Deleted tasks whose worktree no live task still uses. A record
        // whose directory is already gone has nothing left to do.
        let deleted = self.state.lock().deleted.clone();
        let mut finished_records = Vec::new();
        let mut deleted_candidates = Vec::new();
        for record in deleted {
            match managed_toplevel(&roots, &record.workspace_path) {
                Some(toplevel) if !groups.contains_key(&toplevel) => {
                    deleted_candidates.push((
                        record.task_id,
                        Candidate {
                            toplevel,
                            branch: record.branch,
                            task_ids: vec![record.task_id],
                            title: record.title,
                            deleted: true,
                            activity_at: record.deleted_at,
                            finished: true,
                        },
                    ));
                }
                Some(_) => finished_records.push(record.task_id),
                None if !record.workspace_path.exists() => finished_records.push(record.task_id),
                None => {
                    report.push(WorktreeCleanupEntry {
                        path: record.workspace_path,
                        branch: Some(record.branch),
                        task_id: Some(record.task_id),
                        task_title: Some(record.title),
                        outcome: WorktreeCleanupOutcome::Kept,
                        reason: WorktreeCleanupReason::OutsideManagedFolder,
                        branch_deleted: false,
                    });
                }
            }
        }

        let mut bases = BTreeMap::new();
        let mut prune = BTreeSet::new();
        let mut restorable = Vec::new();
        let candidates = groups
            .into_values()
            .map(|candidate| (None, candidate))
            .chain(
                deleted_candidates
                    .into_iter()
                    .map(|(task_id, candidate)| (Some(task_id), candidate)),
            );
        for (deleted_task, candidate) in candidates {
            let outcome = self.process(host, &rules, &candidate, now, &mut bases);
            let entry = WorktreeCleanupEntry {
                path: candidate.toplevel.clone(),
                branch: (!candidate.branch.is_empty()).then(|| candidate.branch.clone()),
                task_id: candidate.task_ids.first().copied(),
                task_title: Some(candidate.title.clone()),
                outcome: outcome.outcome,
                reason: outcome.reason,
                branch_deleted: outcome.branch_deleted,
            };
            if let Some(repository) = outcome.repository
                && entry.outcome == WorktreeCleanupOutcome::Removed
            {
                prune.insert(repository.clone());
                if let Some(task_id) = deleted_task {
                    finished_records.push(task_id);
                } else {
                    restorable.push(RemovedWorktree {
                        toplevel: candidate.toplevel.clone(),
                        branch: candidate.branch.clone(),
                        repository,
                        removed_at: now,
                    });
                }
            }
            report.push(entry);
        }
        for repository in prune {
            if let Err(error) = git::prune(&repository) {
                eprintln!(
                    "Waku daemon could not prune worktrees of {}: {error:#}",
                    repository.display()
                );
            }
        }
        self.update(|state| {
            state
                .deleted
                .retain(|record| !finished_records.contains(&record.task_id));
            state.removed.extend(restorable);
            let excess = state.removed.len().saturating_sub(MAX_REMOVED_RECORDS);
            state.removed.drain(..excess);
        });
    }

    fn process(
        &self,
        host: &dyn CleanupHost,
        rules: &waku_protocol::storage::WorktreeCleanupSettings,
        candidate: &Candidate,
        now: u64,
        bases: &mut BTreeMap<PathBuf, Option<git::BaseRef>>,
    ) -> ProcessOutcome {
        let keep = |reason| ProcessOutcome::kept(reason);
        // Cheap checks first: most worktrees are decided without Git.
        if candidate.deleted && !rules.deleted_tasks
            || !candidate.deleted && !rules.any_rule_enabled()
        {
            return keep(WorktreeCleanupReason::NoRuleApplies);
        }
        if let Some(reason) = host.blocked(&candidate.task_ids) {
            return keep(reason);
        }
        if !git::is_waku_branch(&candidate.branch) {
            return keep(WorktreeCleanupReason::NotWakuBranch);
        }
        if !candidate.toplevel.join(".git").is_file() {
            return keep(WorktreeCleanupReason::NotLinkedWorktree);
        }
        let Ok(repository) = git::main_worktree(&candidate.toplevel) else {
            return keep(WorktreeCleanupReason::NotLinkedWorktree);
        };
        let base = if !candidate.deleted && (rules.merged || rules.unchanged) {
            bases
                .entry(repository.clone())
                .or_insert_with(|| git::resolve_base(&repository, true))
                .clone()
        } else {
            None
        };
        let inspect = || {
            git::inspect(
                &candidate.toplevel,
                &candidate.branch,
                base.as_ref(),
                rules.keep_when,
            )
        };
        let facts = match inspect() {
            Ok(facts) => facts,
            Err(error) => return ProcessOutcome::failed(error),
        };
        let task = TaskFacts {
            deleted: candidate.deleted,
            blocked: None,
            idle_days: now.saturating_sub(candidate.activity_at) / DAY_SECONDS,
            finished: candidate.finished,
        };
        let reason = match policy::evaluate(rules, &task, &facts) {
            Decision::Keep(reason) => return keep(reason),
            Decision::Remove(reason) => reason,
        };
        let Ok(head) = git::head(&candidate.toplevel) else {
            return keep(WorktreeCleanupReason::ChangedDuringCleanup);
        };

        // Re-check everything under the repository lock that Waku's own Git
        // actions (commit, checkout, worktree creation) also take.
        let lock = match crate::git_workflow::repository_lock(&repository) {
            Ok(lock) => lock,
            Err(error) => return ProcessOutcome::failed(error),
        };
        let _guard = lock.lock();
        if let Some(reason) = host.blocked(&candidate.task_ids) {
            return keep(reason);
        }
        match (inspect(), git::head(&candidate.toplevel)) {
            (Ok(latest), Ok(latest_head)) if latest_head == head => {
                if let Some(reason) = policy::protected(rules, &latest) {
                    return keep(reason);
                }
            }
            _ => return keep(WorktreeCleanupReason::ChangedDuringCleanup),
        }
        if let Err(error) = git::remove(&repository, &candidate.toplevel, rules.keep_when) {
            return ProcessOutcome::failed(error);
        }
        // A deleted task's branch goes too, but only when none of its
        // commits would be lost. A live task keeps its branch so its next
        // turn can check the worktree out again.
        let branch_deleted = candidate.deleted
            && facts.unpushed_commits == 0
            && git::delete_branch(&repository, &candidate.branch, &head).is_ok();
        ProcessOutcome {
            outcome: WorktreeCleanupOutcome::Removed,
            reason,
            branch_deleted,
            repository: Some(repository),
        }
    }
}

struct ProcessOutcome {
    outcome: WorktreeCleanupOutcome,
    reason: WorktreeCleanupReason,
    branch_deleted: bool,
    repository: Option<PathBuf>,
}

impl ProcessOutcome {
    fn kept(reason: WorktreeCleanupReason) -> Self {
        Self {
            outcome: WorktreeCleanupOutcome::Kept,
            reason,
            branch_deleted: false,
            repository: None,
        }
    }

    fn failed(error: anyhow::Error) -> Self {
        Self {
            outcome: WorktreeCleanupOutcome::Failed,
            reason: WorktreeCleanupReason::Error {
                message: format!("{error:#}"),
            },
            branch_deleted: false,
            repository: None,
        }
    }
}
