//! Daemon-owned isolated Git worktrees for tasks.
//!
//! A draft records only the user's choice. The first submission creates the
//! worktree beneath the managed folder (`~/.waku-fork/worktrees` unless the
//! user moved it in Settings → Storage), then every workspace consumer uses
//! the returned project-relative path for the lifetime of the task.
//! [`crate::worktree_cleanup`] removes them again according to the user's
//! rules.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;

use anyhow::{Context as _, bail};
use uuid::Uuid;

const DEFAULT_SLUG: &str = "new-worktree";
const MAX_SLUG_BYTES: usize = 48;
const MAX_CANDIDATES: usize = 100;

pub use waku_protocol::git::CreatedWorktree;

pub fn create(
    project_path: &Path,
    project_id: Uuid,
    session_id: Uuid,
    prompt: &str,
    base_branch: Option<&str>,
) -> anyhow::Result<CreatedWorktree> {
    let root = crate::worktree_cleanup::default_worktrees_directory()?;
    create_in(
        project_path,
        &root,
        project_id,
        session_id,
        prompt,
        base_branch,
    )
}

/// Create a task worktree beneath `worktree_root`, at
/// `<worktree_root>/<project id>/<slug>` on a new `waku/<slug>` branch.
pub fn create_in(
    project_path: &Path,
    worktree_root: &Path,
    project_id: Uuid,
    session_id: Uuid,
    prompt: &str,
    requested_base: Option<&str>,
) -> anyhow::Result<CreatedWorktree> {
    let project_path = fs::canonicalize(project_path)
        .with_context(|| format!("could not open project {}", project_path.display()))?;
    let repository = git_stdout(&project_path, &["rev-parse", "--show-toplevel"])
        .context("new worktrees require a Git repository")?;
    let repository = fs::canonicalize(PathBuf::from(repository.trim()))
        .context("could not resolve the Git repository root")?;
    let project_relative = project_path
        .strip_prefix(&repository)
        .context("the project is outside its Git repository root")?
        .to_owned();
    let base_ref = requested_base
        .map(str::trim)
        .filter(|branch| !branch.is_empty())
        .map(str::to_owned)
        .map(Ok)
        .unwrap_or_else(|| default_base_ref(&repository))?;
    // Pin a validated commit before creating anything. Never pass a client
    // supplied revision as an option to `git worktree add`.
    let base_commit = git_stdout(
        &repository,
        &[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{base_ref}^{{commit}}"),
        ],
    )
    .with_context(|| format!("base branch `{base_ref}` is unavailable"))?;
    if !project_relative.as_os_str().is_empty() {
        let tree_path = project_relative
            .components()
            .map(|component| {
                component
                    .as_os_str()
                    .to_str()
                    .context("project path must be UTF-8")
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join("/");
        let kind = git_stdout(
            &repository,
            &["cat-file", "-t", &format!("{base_commit}:{tree_path}")],
        )
        .context("the selected base does not contain the project directory")?;
        if kind != "tree" {
            bail!("the selected base does not contain a project directory");
        }
    }
    let project_worktrees = worktree_root.join(project_id.to_string());
    fs::create_dir_all(&project_worktrees).with_context(|| {
        format!(
            "could not create the worktree directory {}",
            project_worktrees.display()
        )
    })?;

    let slug = worktree_slug(prompt);
    for index in 0..MAX_CANDIDATES {
        let name = candidate_name(&slug, index);
        let branch = format!("waku/{name}");
        let path = project_worktrees.join(&name);
        if path.exists() || local_branch_exists(&repository, &branch)? {
            continue;
        }

        let output = crate::command_env::plain_command("git")
            .args(["worktree", "add", "-b"])
            .arg(&branch)
            .arg(&path)
            .arg(&base_commit)
            .current_dir(&repository)
            .output()
            .context("failed to execute git worktree add")?;
        if !output.status.success() {
            bail!("{}", command_error(&output));
        }

        let project_path = path.join(&project_relative);
        if !project_path.is_dir() {
            bail!(
                "Git created the worktree, but its project directory is missing: {}",
                project_path.display()
            );
        }
        return Ok(CreatedWorktree {
            path: project_path,
            branch,
        });
    }

    // A UUID fallback makes the final attempt independent of human-readable
    // name collisions while keeping the common path and branch pleasant.
    let fallback = format!("{slug}-{}", &session_id.simple().to_string()[..8]);
    let branch = format!("waku/{fallback}");
    let path = project_worktrees.join(&fallback);
    if path.exists() || local_branch_exists(&repository, &branch)? {
        bail!("could not allocate a unique Git worktree name");
    }
    let output = crate::command_env::plain_command("git")
        .args(["worktree", "add", "-b"])
        .arg(&branch)
        .arg(&path)
        .arg(&base_commit)
        .current_dir(&repository)
        .output()
        .context("failed to execute git worktree add")?;
    if !output.status.success() {
        bail!("{}", command_error(&output));
    }
    Ok(CreatedWorktree {
        path: path.join(project_relative),
        branch,
    })
}

/// The worktree name the orchestrator gives a thread's worktree: a slug of
/// `hint` plus the first eight hex digits of the thread id, so a retried
/// preparation computes the same name.
pub fn thread_worktree_name(hint: &str, thread_id: Uuid) -> String {
    format!(
        "{}-{}",
        worktree_slug(hint),
        &thread_id.simple().to_string()[..8]
    )
}

/// Creates `<worktree_root>/<project id>/<name>` on `branch`, or returns it
/// when it already exists (idempotent, for the orchestrator's
/// `workspace.prepare` effect, RFC 0001).
///
/// An existing directory must be a worktree of the project's repository with
/// `branch` checked out, else this fails rather than reuse foreign files. A
/// `branch` that exists without its directory (a removed worktree) is checked
/// out again. `start_from_origin` bases a new branch on
/// `origin/<base>` after a best-effort fetch.
pub fn ensure_named_in(
    project_path: &Path,
    worktree_root: &Path,
    project_id: Uuid,
    name: &str,
    branch: &str,
    requested_base: Option<&str>,
    start_from_origin: bool,
) -> anyhow::Result<CreatedWorktree> {
    if name.is_empty() || name.contains(['/', '\\']) || name.starts_with('.') {
        bail!("invalid worktree name `{name}`");
    }
    let valid_branch = crate::command_env::plain_command("git")
        .args(["check-ref-format", "--branch", branch])
        .output()
        .is_ok_and(|output| output.status.success());
    if branch.starts_with('-') || !valid_branch {
        bail!("invalid branch name `{branch}`");
    }
    let project_path = fs::canonicalize(project_path)
        .with_context(|| format!("could not open project {}", project_path.display()))?;
    let repository = git_stdout(&project_path, &["rev-parse", "--show-toplevel"])
        .context("new worktrees require a Git repository")?;
    let repository = fs::canonicalize(PathBuf::from(repository.trim()))
        .context("could not resolve the Git repository root")?;
    let project_relative = project_path
        .strip_prefix(&repository)
        .context("the project is outside its Git repository root")?
        .to_owned();
    let path = worktree_root.join(project_id.to_string()).join(name);

    if path.exists() {
        let toplevel = git_stdout(&path, &["rev-parse", "--show-toplevel"])
            .with_context(|| format!("{} is not a Git worktree", path.display()))?;
        let common = git_stdout(
            &path,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )?;
        let expected_common = git_stdout(
            &repository,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )?;
        let same = |left: &str, right: &Path| {
            fs::canonicalize(left)
                .is_ok_and(|left| fs::canonicalize(right).is_ok_and(|right| left == right))
        };
        if !same(&toplevel, &path) || !same(&common, Path::new(&expected_common)) {
            bail!(
                "{} exists but is not a worktree of {}",
                path.display(),
                repository.display()
            );
        }
        let current = git_optional_stdout(&path, &["symbolic-ref", "--quiet", "--short", "HEAD"])?;
        if current.as_deref() != Some(branch) {
            bail!(
                "{} has `{}` checked out, not `{branch}`",
                path.display(),
                current.unwrap_or_else(|| "a detached HEAD".to_owned())
            );
        }
        return Ok(CreatedWorktree {
            path: path.join(&project_relative),
            branch: branch.to_owned(),
        });
    }

    let parent = path.parent().context("worktree path has no parent")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "could not create the worktree directory {}",
            parent.display()
        )
    })?;
    let mut command = crate::command_env::plain_command("git");
    command.args(["worktree", "add"]);
    if local_branch_exists(&repository, branch)? {
        // A worktree removed earlier: check its branch out again.
        command.arg(&path).arg(branch);
    } else {
        let base_ref = requested_base
            .map(str::trim)
            .filter(|base| !base.is_empty())
            .map(str::to_owned)
            .map(Ok)
            .unwrap_or_else(|| default_base_ref(&repository))?;
        let base_ref = if start_from_origin {
            origin_base(&repository, &base_ref).unwrap_or(base_ref)
        } else {
            base_ref
        };
        let base_commit = git_stdout(
            &repository,
            &[
                "rev-parse",
                "--verify",
                "--end-of-options",
                &format!("{base_ref}^{{commit}}"),
            ],
        )
        .with_context(|| format!("base branch `{base_ref}` is unavailable"))?;
        command.arg("-b").arg(branch).arg(&path).arg(&base_commit);
    }
    let output = command
        .current_dir(&repository)
        .output()
        .context("failed to execute git worktree add")?;
    if !output.status.success() {
        bail!("{}", command_error(&output));
    }
    let project_path = path.join(&project_relative);
    if !project_path.is_dir() {
        bail!(
            "Git created the worktree, but its project directory is missing: {}",
            project_path.display()
        );
    }
    Ok(CreatedWorktree {
        path: project_path,
        branch: branch.to_owned(),
    })
}

/// `origin/<base>` after a best-effort fetch of it, if it resolves.
fn origin_base(repository: &Path, base: &str) -> Option<String> {
    let branch = base.strip_prefix("origin/").unwrap_or(base);
    if branch.starts_with('-') {
        return None;
    }
    let mut fetch = crate::command_env::plain_command("git");
    fetch
        .args(["fetch", "--quiet", "--no-tags", "origin", branch])
        .current_dir(repository)
        .env("GIT_TERMINAL_PROMPT", "0");
    let _ = crate::git_commit::run_capture(&mut fetch, std::time::Duration::from_secs(30));
    let remote = format!("origin/{branch}");
    git_stdout(
        repository,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            &format!("{remote}^{{commit}}"),
        ],
    )
    .ok()
    .map(|_| remote)
}

/// Prefer the local branch named by `origin/HEAD`, so a worktree starts from
/// the default branch without fetching or mutating the user's ordinary
/// checkout. Repositories without that metadata fall back to their current
/// branch, then detached `HEAD`.
fn default_base_ref(repository: &Path) -> anyhow::Result<String> {
    if let Some(remote_default) = git_optional_stdout(
        repository,
        &[
            "symbolic-ref",
            "--quiet",
            "--short",
            "refs/remotes/origin/HEAD",
        ],
    )? {
        if let Some(local_default) = remote_default.trim().strip_prefix("origin/")
            && local_branch_exists(repository, local_default)?
        {
            return Ok(local_default.to_owned());
        }
        return Ok(remote_default.trim().to_owned());
    }
    if let Some(branch) = git_optional_stdout(repository, &["branch", "--show-current"])?
        && !branch.trim().is_empty()
    {
        return Ok(branch.trim().to_owned());
    }
    git_stdout(repository, &["rev-parse", "--verify", "HEAD"])?;
    Ok("HEAD".to_owned())
}

fn local_branch_exists(repository: &Path, branch: &str) -> anyhow::Result<bool> {
    let output = crate::command_env::plain_command("git")
        .args(["show-ref", "--verify", "--quiet"])
        .arg(format!("refs/heads/{branch}"))
        .current_dir(repository)
        .output()
        .context("failed to inspect Git branches")?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => bail!("{}", command_error(&output)),
    }
}

fn git_stdout(cwd: &Path, args: &[&str]) -> anyhow::Result<String> {
    let output = crate::command_env::plain_command("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .context("failed to execute git")?;
    if !output.status.success() {
        bail!("{}", command_error(&output));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_optional_stdout(cwd: &Path, args: &[&str]) -> anyhow::Result<Option<String>> {
    let output = crate::command_env::plain_command("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .context("failed to execute git")?;
    if output.status.success() {
        return Ok(Some(
            String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        ));
    }
    if output.status.code() == Some(1) {
        return Ok(None);
    }
    bail!("{}", command_error(&output))
}

fn command_error(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    if stderr.is_empty() {
        format!("git exited with {}", output.status)
    } else {
        stderr
    }
}

fn candidate_name(slug: &str, index: usize) -> String {
    if index == 0 {
        slug.to_owned()
    } else {
        format!("{slug}-{}", index + 1)
    }
}

fn worktree_slug(prompt: &str) -> String {
    let mut slug = prompt
        .to_ascii_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .take(6)
        .collect::<Vec<_>>()
        .join("-");
    slug.truncate(MAX_SLUG_BYTES);
    if slug.is_empty() {
        DEFAULT_SLUG.to_owned()
    } else {
        slug
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_git(cwd: &Path, args: &[&str]) {
        let output = crate::command_env::plain_command("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .unwrap();
        assert!(output.status.success(), "{}", command_error(&output));
    }

    #[test]
    fn creates_isolated_worktrees_from_the_default_branch() {
        let root = std::env::temp_dir().join(format!("waku-worktree-test-{}", Uuid::new_v4()));
        let repository = root.join("repository");
        let project = repository.join("packages/app");
        fs::create_dir_all(&project).unwrap();
        run_git(&repository, &["init", "-b", "main"]);
        run_git(&repository, &["config", "core.autocrlf", "false"]);
        fs::write(project.join("README.md"), "main\n").unwrap();
        run_git(&repository, &["add", "."]);
        run_git(
            &repository,
            &[
                "-c",
                "user.name=Waku Tests",
                "-c",
                "user.email=waku@example.com",
                "commit",
                "-m",
                "initial",
            ],
        );
        run_git(
            &repository,
            &["update-ref", "refs/remotes/origin/main", "refs/heads/main"],
        );
        run_git(
            &repository,
            &[
                "symbolic-ref",
                "refs/remotes/origin/HEAD",
                "refs/remotes/origin/main",
            ],
        );
        run_git(&repository, &["checkout", "-b", "feature"]);
        fs::write(project.join("README.md"), "feature\n").unwrap();
        run_git(&repository, &["add", "."]);
        run_git(
            &repository,
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

        let worktree_root = root.join("worktrees");
        let project_id = Uuid::new_v4();
        let first = create_in(
            &project,
            &worktree_root,
            project_id,
            Uuid::new_v4(),
            "Build a project selector",
            None,
        )
        .unwrap();
        assert_eq!(first.branch, "waku/build-a-project-selector");
        assert_eq!(
            fs::read_to_string(first.path.join("README.md")).unwrap(),
            "main\n"
        );
        fs::write(first.path.join("README.md"), "worktree\n").unwrap();
        assert_eq!(
            fs::read_to_string(project.join("README.md")).unwrap(),
            "feature\n"
        );

        let second = create_in(
            &project,
            &worktree_root,
            project_id,
            Uuid::new_v4(),
            "Build a project selector",
            None,
        )
        .unwrap();
        assert_eq!(second.branch, "waku/build-a-project-selector-2");

        let from_feature = create_in(
            &project,
            &worktree_root,
            project_id,
            Uuid::new_v4(),
            "Use selected base",
            Some("feature"),
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(from_feature.path.join("README.md")).unwrap(),
            "feature\n"
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn slug_is_short_and_branch_safe() {
        assert_eq!(worktree_slug("你好 👋"), DEFAULT_SLUG);
        assert_eq!(
            worktree_slug("Fix Project/Worktree Picker!"),
            "fix-project-worktree-picker"
        );
        assert!(worktree_slug(&"a".repeat(100)).len() <= MAX_SLUG_BYTES);
    }

    #[test]
    fn missing_project_directory_and_invalid_base_leave_no_worktree() {
        let root = std::env::temp_dir().join(format!("waku-worktree-preflight-{}", Uuid::new_v4()));
        let repository = root.join("repository");
        fs::create_dir_all(&repository).unwrap();
        run_git(&repository, &["init", "-b", "main"]);
        fs::write(repository.join("README"), "root\n").unwrap();
        run_git(&repository, &["add", "."]);
        run_git(
            &repository,
            &[
                "-c",
                "user.name=Waku Tests",
                "-c",
                "user.email=waku@example.com",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "initial",
            ],
        );
        let project = repository.join("subproject");
        fs::create_dir(&project).unwrap();
        for base in ["main", "--help", "does-not-exist"] {
            assert!(
                create_in(
                    &project,
                    &root.join("worktrees"),
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    "test",
                    Some(base)
                )
                .is_err()
            );
            assert_eq!(
                crate::git_workflow::list_worktrees(&repository)
                    .unwrap()
                    .len(),
                1
            );
            assert!(!root.join("worktrees").exists());
        }
        fs::remove_dir_all(root).unwrap();
    }
}
