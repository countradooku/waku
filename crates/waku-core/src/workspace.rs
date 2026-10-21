//! Daemon-owned workspace filesystem and Git API.
//!
//! Paths in this module always name resources on the daemon host. A client
//! may display them, but must never reinterpret them against its own machine.

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Output;

use anyhow::{Context as _, anyhow, bail};

const MAX_HYDRATED_PATCH_BYTES: usize = 32 * 1024 * 1024;
const EMPTY_TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

pub use waku_protocol::workspace::{
    ReviewDiffData, ReviewDiffSource, WorkingTreeEntry, WorkspaceOperation, WorkspaceResult,
};

pub fn execute(operation: WorkspaceOperation) -> anyhow::Result<WorkspaceResult> {
    execute_in(operation, None)
}

/// Like [`execute`], creating new worktrees beneath `worktree_root` (the
/// configured managed folder) instead of the default one.
pub fn execute_in(
    operation: WorkspaceOperation,
    worktree_root: Option<&Path>,
) -> anyhow::Result<WorkspaceResult> {
    let repository_lock = match &operation {
        WorkspaceOperation::StageFiles { cwd, .. }
        | WorkspaceOperation::Pull { cwd }
        | WorkspaceOperation::Commit { cwd, .. }
        | WorkspaceOperation::Push { cwd }
        | WorkspaceOperation::CheckoutBranch { cwd, .. } => {
            Some(crate::git_workflow::repository_lock(cwd)?)
        }
        WorkspaceOperation::CreateWorktree { project_path, .. } => {
            Some(crate::git_workflow::repository_lock(project_path)?)
        }
        _ => None,
    };
    let _repository_guard = repository_lock.as_ref().map(|lock| lock.lock());
    Ok(match operation {
        WorkspaceOperation::ListTree {
            root,
            expanded_paths,
        } => WorkspaceResult::WorkingTree {
            entries: list_tree(&root, &expanded_paths.into_iter().collect()),
        },
        WorkspaceOperation::BrowseDirectory { path } => {
            let home = dirs::home_dir().ok_or_else(|| anyhow!("home directory is unavailable"))?;
            let path = fs::canonicalize(path.as_deref().unwrap_or(&home)).with_context(|| {
                format!(
                    "could not open directory {}",
                    path.as_deref().unwrap_or(&home).display()
                )
            })?;
            if !fs::metadata(&path)?.is_dir() {
                bail!("not a directory: {}", path.display());
            }
            let filesystem_root = path
                .ancestors()
                .last()
                .map(Path::to_owned)
                .unwrap_or_else(|| path.clone());
            WorkspaceResult::Directory {
                parent: path.parent().map(Path::to_owned),
                entries: list_directory(&path)?,
                path,
                home,
                filesystem_root,
            }
        }
        WorkspaceOperation::WriteTextFile {
            root,
            relative_path,
            content,
        } => {
            fs::write(resolve_workspace_path(&root, &relative_path)?, content)?;
            WorkspaceResult::Ack
        }
        WorkspaceOperation::ListProjectFiles { root, cap } => WorkspaceResult::ProjectFiles {
            entries: crate::composer_complete::list_project_files(&root, cap),
        },
        WorkspaceOperation::DiscoverSlashCommands {
            provider,
            project_root,
            binary_override,
        } => WorkspaceResult::SlashCommands {
            commands: crate::composer_complete::discover_slash_commands(
                provider,
                &project_root,
                binary_override.as_deref(),
            ),
        },
        WorkspaceOperation::InspectBranches { cwd } => WorkspaceResult::Branches {
            snapshot: crate::git_branch::inspect(&cwd)?,
        },
        WorkspaceOperation::CheckoutBranch {
            cwd,
            branch,
            create,
        } => WorkspaceResult::BranchChanged {
            snapshot: if create {
                crate::git_branch::create_and_checkout(&cwd, &branch)?
            } else {
                crate::git_branch::checkout(&cwd, &branch)?
            },
        },
        WorkspaceOperation::CreateWorktree {
            project_path,
            project_id,
            session_id,
            prompt,
            base_branch,
        } => WorkspaceResult::WorktreeCreated {
            worktree: match worktree_root {
                Some(root) => crate::worktree::create_in(
                    &project_path,
                    root,
                    project_id,
                    session_id,
                    &prompt,
                    base_branch.as_deref(),
                )?,
                None => crate::worktree::create(
                    &project_path,
                    project_id,
                    session_id,
                    &prompt,
                    base_branch.as_deref(),
                )?,
            },
        },
        WorkspaceOperation::InspectCommit { cwd } => WorkspaceResult::CommitSnapshot {
            snapshot: crate::git_commit::inspect(&cwd)?,
        },
        WorkspaceOperation::GitStatus { cwd } => WorkspaceResult::GitStatus {
            status: crate::git_workflow::status(&cwd)?,
        },
        WorkspaceOperation::StageFiles { cwd, paths, stage } => {
            crate::git_workflow::stage_files(&cwd, &paths, stage)?;
            WorkspaceResult::Ack
        }
        WorkspaceOperation::Pull { cwd } => {
            crate::git_workflow::pull(&cwd)?;
            WorkspaceResult::Ack
        }
        WorkspaceOperation::ListWorktrees { cwd } => WorkspaceResult::Worktrees {
            worktrees: crate::git_workflow::list_worktrees(&cwd)?,
        },
        WorkspaceOperation::GenerateCommitMessage {
            cwd,
            include_unstaged,
            invocation,
        } => WorkspaceResult::CommitMessage {
            message: crate::git_commit::generate_message(&cwd, include_unstaged, &invocation)?,
        },
        WorkspaceOperation::Commit {
            cwd,
            message,
            include_unstaged,
            push,
        } => {
            crate::git_commit::commit(&cwd, &message, include_unstaged)?;
            if push {
                crate::git_commit::push(&cwd)?;
            }
            WorkspaceResult::Ack
        }
        WorkspaceOperation::Push { cwd } => {
            crate::git_commit::push(&cwd)?;
            WorkspaceResult::Ack
        }
        WorkspaceOperation::CollectReviewDiff {
            cwd,
            source,
            ignore_whitespace,
        } => WorkspaceResult::ReviewDiff {
            data: collect_review_diff(&cwd, source, ignore_whitespace)?,
        },
    })
}

fn resolve_workspace_path(root: &Path, relative: &Path) -> anyhow::Result<PathBuf> {
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("workspace path must be a non-empty relative path");
    }
    Ok(root.join(relative))
}

fn list_tree(root: &Path, expanded_paths: &HashSet<PathBuf>) -> Vec<WorkingTreeEntry> {
    fn visit(
        directory: &Path,
        relative_directory: &Path,
        depth: usize,
        expanded_paths: &HashSet<PathBuf>,
        output: &mut Vec<WorkingTreeEntry>,
    ) {
        let Ok(entries) = fs::read_dir(directory) else {
            return;
        };
        let mut children = entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name == ".git" {
                    return None;
                }
                let is_dir = entry.file_type().ok()?.is_dir();
                Some((entry.path(), name, is_dir))
            })
            .collect::<Vec<_>>();
        children.sort_by_key(|(_, name, is_dir)| (!*is_dir, name.to_lowercase()));
        for (absolute_path, name, is_dir) in children {
            let relative_path = relative_directory.join(&name);
            let expanded = is_dir && expanded_paths.contains(&absolute_path);
            output.push(WorkingTreeEntry {
                relative_path: relative_path.to_string_lossy().into_owned(),
                absolute_path: absolute_path.clone(),
                name,
                is_dir,
                expanded,
                depth,
            });
            if expanded {
                visit(
                    &absolute_path,
                    &relative_path,
                    depth + 1,
                    expanded_paths,
                    output,
                );
            }
        }
    }
    let mut output = Vec::new();
    visit(root, Path::new(""), 0, expanded_paths, &mut output);
    output
}

fn list_directory(directory: &Path) -> anyhow::Result<Vec<WorkingTreeEntry>> {
    let mut entries = fs::read_dir(directory)
        .with_context(|| format!("could not read directory {}", directory.display()))?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            let is_dir = fs::metadata(entry.path()).ok()?.is_dir();
            Some(WorkingTreeEntry {
                relative_path: name.clone(),
                absolute_path: entry.path(),
                name,
                is_dir,
                expanded: false,
                depth: 0,
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| (!entry.is_dir, entry.name.to_lowercase()));
    Ok(entries)
}

#[derive(Clone, Debug)]
struct DiffRange {
    from: String,
    to: String,
}

fn collect_review_diff(
    cwd: &Path,
    source: ReviewDiffSource,
    ignore_whitespace: bool,
) -> anyhow::Result<ReviewDiffData> {
    ensure_repository(cwd)?;
    let range = resolve_diff_range(cwd, source)?;
    let numstat = diff_output(cwd, &range, &["--numstat"], ignore_whitespace)?;
    let hydrated = diff_output(cwd, &range, &["--unified=2147483647"], ignore_whitespace)?;
    let (patch, complete_context) = if hydrated.len() <= MAX_HYDRATED_PATCH_BYTES {
        (hydrated, true)
    } else {
        (
            diff_output(cwd, &range, &["--unified=3"], ignore_whitespace)?,
            false,
        )
    };
    Ok(ReviewDiffData {
        source,
        numstat,
        patch,
        complete_context,
    })
}

/// A checkpoint-to-checkpoint diff (`threads.turnDiff`, `threads.fullDiff`):
/// `numstat`, and the patch with full context when it is small enough (as
/// `review.diff` collects it). Both refs must resolve in `cwd`.
pub struct RefDiff {
    pub numstat: String,
    pub patch: String,
    pub complete_context: bool,
}

pub fn collect_ref_diff(
    cwd: &Path,
    from: &str,
    to: &str,
    ignore_whitespace: bool,
) -> anyhow::Result<RefDiff> {
    ensure_repository(cwd)?;
    let resolved = |git_ref: &str| {
        resolve(cwd, git_ref).ok_or_else(|| anyhow!("checkpoint `{git_ref}` is unavailable"))
    };
    let range = DiffRange {
        from: resolved(from)?,
        to: resolved(to)?,
    };
    let numstat = diff_output(cwd, &range, &["--numstat"], ignore_whitespace)?;
    let hydrated = diff_output(cwd, &range, &["--unified=2147483647"], ignore_whitespace)?;
    let (patch, complete_context) = if hydrated.len() <= MAX_HYDRATED_PATCH_BYTES {
        (hydrated, true)
    } else {
        (
            diff_output(cwd, &range, &["--unified=3"], ignore_whitespace)?,
            false,
        )
    };
    Ok(RefDiff {
        numstat,
        patch,
        complete_context,
    })
}

fn resolve_diff_range(cwd: &Path, source: ReviewDiffSource) -> anyhow::Result<DiffRange> {
    let head = resolve(cwd, "HEAD").unwrap_or_else(|| EMPTY_TREE.to_owned());
    Ok(match source {
        ReviewDiffSource::Uncommitted => DiffRange {
            from: head,
            to: crate::checkpoint::capture_worktree_commit(cwd)?,
        },
        ReviewDiffSource::Unstaged => DiffRange {
            from: index_tree(cwd)?,
            to: crate::checkpoint::capture_worktree_commit(cwd)?,
        },
        ReviewDiffSource::Staged => DiffRange {
            from: head,
            to: index_tree(cwd)?,
        },
        ReviewDiffSource::Committed => DiffRange {
            from: branch_base(cwd)?,
            to: head,
        },
        ReviewDiffSource::Branch => DiffRange {
            from: branch_base(cwd)?,
            to: crate::checkpoint::capture_worktree_commit(cwd)?,
        },
    })
}

fn branch_base(cwd: &Path) -> anyhow::Result<String> {
    let Some(snapshot) = crate::git_branch::inspect(cwd)? else {
        bail!("the workspace is not a Git repository");
    };
    let Some(head) = resolve(cwd, "HEAD") else {
        return Ok(EMPTY_TREE.to_owned());
    };
    let current = snapshot.current.as_deref();
    let default_branch = snapshot
        .default_branch
        .filter(|branch| current != Some(branch.as_str()))
        .or_else(|| {
            ["main", "master"]
                .into_iter()
                .find(|candidate| {
                    current != Some(*candidate)
                        && snapshot
                            .branches
                            .iter()
                            .any(|branch| branch.name == *candidate)
                })
                .map(str::to_owned)
        });
    let Some(default_branch) = default_branch else {
        return Ok(head);
    };
    let output = git(cwd, ["merge-base", "HEAD", default_branch.as_str()])?;
    let base = output.trim();
    Ok(if base.is_empty() {
        head
    } else {
        base.to_owned()
    })
}

fn index_tree(cwd: &Path) -> anyhow::Result<String> {
    let output = crate::command_env::plain_command("git")
        .args(["write-tree"])
        .current_dir(cwd)
        .output()
        .context("failed to snapshot the Git index")?;
    if output.status.success() {
        let tree = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !tree.is_empty() {
            return Ok(tree);
        }
    }
    if resolve(cwd, "HEAD").is_none() {
        Ok(EMPTY_TREE.to_owned())
    } else {
        bail!("{}", command_error(&output))
    }
}

fn diff_output(
    cwd: &Path,
    range: &DiffRange,
    modes: &[&str],
    ignore_whitespace: bool,
) -> anyhow::Result<String> {
    let output = crate::command_env::plain_command("git")
        .args([
            "-c",
            "core.quotePath=false",
            "diff",
            "--no-ext-diff",
            "--no-color",
        ])
        .args(modes)
        .args(if ignore_whitespace {
            &["--ignore-all-space"][..]
        } else {
            &[]
        })
        .arg("--no-renames")
        .arg(&range.from)
        .arg(&range.to)
        .args(["--", "."])
        .current_dir(cwd)
        .output()
        .context("failed to generate Git diff")?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        bail!("{}", command_error(&output))
    }
}

fn ensure_repository(cwd: &Path) -> anyhow::Result<()> {
    let output = crate::command_env::plain_command("git")
        .args(["rev-parse", "--is-inside-work-tree"])
        .current_dir(cwd)
        .output()
        .context("failed to inspect Git workspace")?;
    if output.status.success() {
        Ok(())
    } else {
        bail!("the workspace is not a Git repository")
    }
}

fn resolve(cwd: &Path, revision: &str) -> Option<String> {
    let output = crate::command_env::plain_command("git")
        .args(["rev-parse", "--verify", &format!("{revision}^{{commit}}")])
        .current_dir(cwd)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn git<I, S>(cwd: &Path, args: I) -> anyhow::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = crate::command_env::plain_command("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .context("failed to execute git")?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        bail!("{}", command_error(&output))
    }
}

fn command_error(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    if stderr.is_empty() {
        format!("git exited with {}", output.status)
    } else {
        stderr
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use uuid::Uuid;

    fn git_ok(cwd: &Path, args: &[&str]) {
        let output = crate::command_env::plain_command("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .unwrap();
        assert!(output.status.success(), "{}", command_error(&output));
    }

    fn repository() -> PathBuf {
        let root = std::env::temp_dir().join(format!("waku-workspace-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        git_ok(&root, &["init", "-b", "main"]);
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/lib.rs"), "fn baseline() {}\n").unwrap();
        git_ok(&root, &["add", "."]);
        git_ok(
            &root,
            &[
                "-c",
                "user.name=Waku Tests",
                "-c",
                "user.email=waku@example.com",
                "commit",
                "-m",
                "baseline",
            ],
        );
        root
    }

    fn collect(root: &Path, source: ReviewDiffSource) -> ReviewDiffData {
        let WorkspaceResult::ReviewDiff { data } = execute(WorkspaceOperation::CollectReviewDiff {
            cwd: root.to_path_buf(),
            source,
            ignore_whitespace: false,
        })
        .unwrap() else {
            panic!("unexpected workspace response")
        };
        data
    }

    fn numstat_summary(numstat: &str) -> (usize, u64, u64) {
        let mut files = 0;
        let mut additions = 0;
        let mut deletions = 0;
        for line in numstat.lines().filter(|line| !line.is_empty()) {
            let mut fields = line.splitn(3, '\t');
            additions += fields
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            deletions += fields
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            assert!(fields.next().is_some(), "numstat row is missing its path");
            files += 1;
        }
        (files, additions, deletions)
    }

    #[test]
    fn directory_browser_lists_an_arbitrary_daemon_directory() {
        let directory =
            std::env::temp_dir().join(format!("waku-directory-browser-{}", Uuid::new_v4()));
        fs::create_dir_all(directory.join("folder")).unwrap();
        fs::create_dir_all(directory.join(".git")).unwrap();
        fs::write(directory.join("notes.txt"), "notes").unwrap();

        let WorkspaceResult::Directory {
            path,
            parent,
            entries,
            ..
        } = execute(WorkspaceOperation::BrowseDirectory {
            path: Some(directory.clone()),
        })
        .unwrap()
        else {
            panic!("unexpected workspace response")
        };

        assert_eq!(path, fs::canonicalize(&directory).unwrap());
        assert_eq!(parent, path.parent().map(Path::to_owned));
        assert_eq!(
            entries
                .iter()
                .map(|entry| (&*entry.name, entry.is_dir))
                .collect::<Vec<_>>(),
            [(".git", true), ("folder", true), ("notes.txt", false)]
        );

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn source_modes_compare_consistent_git_snapshots() {
        let root = repository();
        git_ok(&root, &["switch", "-c", "feature"]);
        fs::write(root.join("src/lib.rs"), "fn committed() {}\n").unwrap();
        git_ok(&root, &["add", "src/lib.rs"]);
        git_ok(
            &root,
            &[
                "-c",
                "user.name=Waku Tests",
                "-c",
                "user.email=waku@example.com",
                "commit",
                "-m",
                "feature",
            ],
        );
        fs::write(
            root.join("src/lib.rs"),
            "fn committed() {}\nfn staged() {}\n",
        )
        .unwrap();
        git_ok(&root, &["add", "src/lib.rs"]);
        fs::write(
            root.join("src/lib.rs"),
            "fn committed() {}\nfn staged() {}\nfn unstaged() {}\n",
        )
        .unwrap();
        fs::write(root.join("new file.txt"), "untracked\n").unwrap();

        let committed = collect(&root, ReviewDiffSource::Committed);
        let staged = collect(&root, ReviewDiffSource::Staged);
        let unstaged = collect(&root, ReviewDiffSource::Unstaged);
        let uncommitted = collect(&root, ReviewDiffSource::Uncommitted);
        let branch = collect(&root, ReviewDiffSource::Branch);

        assert_eq!(numstat_summary(&committed.numstat), (1, 1, 1));
        assert_eq!(numstat_summary(&staged.numstat), (1, 1, 0));
        assert_eq!(
            numstat_summary(&unstaged.numstat),
            (2, 2, 0),
            "unstaged includes untracked files"
        );
        assert_eq!(numstat_summary(&uncommitted.numstat), (2, 3, 0));
        assert_eq!(numstat_summary(&branch.numstat), (2, 4, 1));
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn ignore_whitespace_filters_patch_and_counts_without_hiding_code_changes() {
        let root = repository();
        let filtered = |source| {
            let WorkspaceResult::ReviewDiff { data } =
                execute(WorkspaceOperation::CollectReviewDiff {
                    cwd: root.clone(),
                    source,
                    ignore_whitespace: true,
                })
                .unwrap()
            else {
                panic!("unexpected workspace response")
            };
            data
        };
        fs::write(root.join("src/lib.rs"), "fn  baseline() { }\n").unwrap();
        assert!(
            !collect(&root, ReviewDiffSource::Uncommitted)
                .patch
                .is_empty()
        );
        for source in [ReviewDiffSource::Unstaged, ReviewDiffSource::Uncommitted] {
            let data = filtered(source);
            assert!(data.patch.is_empty());
            assert!(data.numstat.is_empty());
        }
        git_ok(&root, &["add", "src/lib.rs"]);
        assert!(filtered(ReviewDiffSource::Staged).patch.is_empty());
        fs::write(
            root.join("src/lib.rs"),
            "fn  baseline() { }\nfn added() {}\n",
        )
        .unwrap();
        let data = filtered(ReviewDiffSource::Uncommitted);
        assert!(data.patch.contains("+fn added() {}"));
        assert_eq!(numstat_summary(&data.numstat), (1, 1, 0));
        assert_eq!(
            fs::read_to_string(root.join("src/lib.rs")).unwrap(),
            "fn  baseline() { }\nfn added() {}\n"
        );
        fs::remove_dir_all(root).unwrap();
    }
}
