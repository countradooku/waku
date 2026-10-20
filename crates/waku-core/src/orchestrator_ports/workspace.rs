//! `WorkspacePort`: the workspace a run starts in.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use waku_orchestrator::command::{PreparePhase, PreparedWorkspace};
use waku_orchestrator::{PortError, PrepareRequest, WorkspacePort};
use waku_protocol::orchestration::WorkspaceStrategy;
use waku_protocol::storage::{WORKTREE_BRANCH_PREFIX, WorktreeCleanupSettings};

use crate::worktree_cleanup::{git, policy};

type RootSource = dyn Fn() -> anyhow::Result<PathBuf> + Send + Sync;
type RulesSource = dyn Fn() -> WorktreeCleanupSettings + Send + Sync;

/// Prepares root, existing and new worktrees, and removes owned ones.
///
/// New worktrees go to `<root>/<project id>/<slug>-<thread>` on
/// `waku/<slug>-<thread>` (or the branch the strategy names). The name
/// depends only on the thread and its title, so a retried preparation finds
/// the worktree it already made. Removal applies the worktree-cleanup safety
/// rules (#62): a worktree with local changes or unpushed commits, on a
/// switched branch, or outside its repository is kept, and the effect fails
/// with the reason instead.
#[derive(Clone)]
pub struct GitWorkspacePort {
    worktree_root: Arc<RootSource>,
    rules: Arc<RulesSource>,
}

impl GitWorkspacePort {
    /// `worktree_root` answers the managed folder (Settings → Storage);
    /// `rules` the user's cleanup rules, read at removal time.
    pub fn new(
        worktree_root: impl Fn() -> anyhow::Result<PathBuf> + Send + Sync + 'static,
        rules: impl Fn() -> WorktreeCleanupSettings + Send + Sync + 'static,
    ) -> Self {
        Self {
            worktree_root: Arc::new(worktree_root),
            rules: Arc::new(rules),
        }
    }

    /// A fixed folder and the default rules (keep uncommitted changes and
    /// unpushed commits).
    pub fn with_root(root: PathBuf) -> Self {
        Self::new(move || Ok(root.clone()), WorktreeCleanupSettings::default)
    }
}

fn permanent(error: impl std::fmt::Display) -> PortError {
    PortError::permanent(error.to_string())
}

/// The branch checked out at `cwd`, if any.
fn current_branch(cwd: &Path) -> Option<String> {
    let output = crate::command_env::plain_command("git")
        .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
        .current_dir(cwd)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|branch| !branch.is_empty())
}

impl WorkspacePort for GitWorkspacePort {
    fn prepare(
        &self,
        request: &PrepareRequest<'_>,
        progress: &dyn Fn(PreparePhase),
    ) -> Result<PreparedWorkspace, PortError> {
        match request.strategy {
            WorkspaceStrategy::Root { branch } => {
                if !request.project_root.is_dir() {
                    return Err(permanent(format!(
                        "project folder {} is missing",
                        request.project_root.display()
                    )));
                }
                Ok(PreparedWorkspace {
                    cwd: request.project_root.to_owned(),
                    branch: branch
                        .clone()
                        .or_else(|| current_branch(request.project_root)),
                    worktree_path: None,
                    owned: false,
                })
            }
            WorkspaceStrategy::ExistingWorktree { path, branch } => {
                if !path.is_dir() {
                    return Err(permanent(format!("worktree {} is missing", path.display())));
                }
                Ok(PreparedWorkspace {
                    cwd: path.clone(),
                    branch: branch.clone().or_else(|| current_branch(path)),
                    worktree_path: Some(path.clone()),
                    owned: false,
                })
            }
            WorkspaceStrategy::NewWorktree {
                base_ref,
                branch,
                start_from_origin,
            } => {
                progress(PreparePhase::Worktree);
                let name =
                    crate::worktree::thread_worktree_name(request.name_hint, request.thread_id.0);
                let branch = branch
                    .clone()
                    .unwrap_or_else(|| format!("{WORKTREE_BRANCH_PREFIX}{name}"));
                let root = (self.worktree_root)().map_err(permanent)?;
                let created = crate::worktree::ensure_named_in(
                    request.project_root,
                    &root,
                    request.project_id.0,
                    &name,
                    &branch,
                    Some(base_ref.as_str()),
                    *start_from_origin,
                )
                .map_err(|error| permanent(format!("{error:#}")))?;
                Ok(PreparedWorkspace {
                    cwd: created.path.clone(),
                    branch: Some(created.branch),
                    worktree_path: Some(created.path),
                    owned: true,
                })
            }
            WorkspaceStrategy::Unknown => Err(permanent("unknown workspace strategy")),
        }
    }

    fn remove_worktree(&self, path: &Path, branch: Option<&str>) -> Result<(), PortError> {
        if !path.exists() {
            return Ok(());
        }
        let toplevel = {
            let output = crate::command_env::plain_command("git")
                .args(["rev-parse", "--show-toplevel"])
                .current_dir(path)
                .output()
                .map_err(permanent)?;
            if !output.status.success() {
                return Err(permanent(format!(
                    "{} is not a Git worktree",
                    path.display()
                )));
            }
            PathBuf::from(String::from_utf8_lossy(&output.stdout).trim())
        };
        let repository = git::main_worktree(&toplevel).map_err(permanent)?;
        let branch = match branch {
            Some(branch) => branch.to_owned(),
            None => current_branch(&toplevel)
                .ok_or_else(|| permanent("the worktree has no branch checked out"))?,
        };
        let rules = (self.rules)();
        let facts = git::inspect(&toplevel, &branch, None, rules.keep_when).map_err(permanent)?;
        if let Some(reason) = policy::protected(&rules, &facts) {
            return Err(permanent(format!("kept the worktree: {reason:?}")));
        }
        let head = git::head(&toplevel).map_err(permanent)?;
        git::remove(&repository, &toplevel, rules.keep_when).map_err(permanent)?;
        let _ = git::prune(&repository);
        // The branch goes too when it is Waku's and no commit would be lost.
        if git::is_waku_branch(&branch) && facts.unpushed_commits == 0 {
            let _ = git::delete_branch(&repository, &branch, &head);
        }
        Ok(())
    }
}
