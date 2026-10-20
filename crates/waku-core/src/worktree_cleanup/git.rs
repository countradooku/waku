//! Git inspection and removal for worktree cleanup. Every command runs with a
//! timeout and without prompts; nothing here passes `--force`, so Git itself
//! is the last guard against removing a worktree that gained edits after it
//! was inspected.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, bail};
use waku_protocol::storage::{WORKTREE_BRANCH_PREFIX, WorktreeKeepWhen};

use super::policy::GitFacts;

const TIMEOUT: Duration = Duration::from_secs(120);
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
/// Ignored folders that are regenerated on demand and never count as local
/// work for the "any local files" level.
const DEPENDENCY_CACHES: [&str; 2] = ["node_modules/", "target/"];

struct Output {
    success: bool,
    stdout: String,
    stderr: String,
}

fn run_with_timeout(cwd: &Path, args: &[&str], timeout: Duration) -> anyhow::Result<Output> {
    let mut command = crate::command_env::plain_command("git");
    command
        .args(args)
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_EDITOR", "true")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("LC_ALL", "C");
    let output = crate::git_commit::run_capture(&mut command, timeout)
        .with_context(|| format!("could not run git {}", args.first().unwrap_or(&"")))?;
    Ok(Output {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
    })
}

fn run(cwd: &Path, args: &[&str]) -> anyhow::Result<Output> {
    run_with_timeout(cwd, args, TIMEOUT)
}

fn stdout(cwd: &Path, args: &[&str]) -> anyhow::Result<String> {
    let output = run(cwd, args)?;
    if !output.success {
        bail!("{}", first_line(&output.stderr, "git command failed"));
    }
    Ok(output.stdout)
}

fn optional(cwd: &Path, args: &[&str]) -> Option<String> {
    run(cwd, args)
        .ok()
        .filter(|output| output.success)
        .map(|output| output.stdout.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn first_line<'a>(text: &'a str, fallback: &'a str) -> &'a str {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or(fallback)
}

/// Whether `branch` is one Waku creates: `waku/` followed by a slug. The
/// charset check also makes the name safe as a ref-exclusion pattern.
pub fn is_waku_branch(branch: &str) -> bool {
    branch
        .strip_prefix(WORKTREE_BRANCH_PREFIX)
        .is_some_and(|slug| {
            !slug.is_empty()
                && slug
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

/// The repository's main worktree, which owns the linked worktree at
/// `toplevel`. Fails unless Git lists `toplevel` as one of its worktrees.
pub fn main_worktree(toplevel: &Path) -> anyhow::Result<PathBuf> {
    let worktrees = crate::git_workflow::list_worktrees(toplevel)?;
    let main = worktrees.first().context("Git listed no worktrees")?;
    if main.bare {
        bail!("bare repositories are not managed");
    }
    let canonical = |path: &Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.into());
    let wanted = canonical(toplevel);
    if !worktrees
        .iter()
        .skip(1)
        .any(|entry| canonical(&entry.path) == wanted)
    {
        bail!("not a linked worktree of its repository");
    }
    Ok(main.path.clone())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseRef {
    /// Display name, such as `origin/main`.
    pub name: String,
    pub reference: String,
}

/// The branch worktrees are compared with: the remote default branch when
/// `origin/HEAD` names one (fetched first when `fetch` is set, so merges on
/// the host are visible), otherwise the main checkout's current branch.
pub fn resolve_base(repository: &Path, fetch: bool) -> Option<BaseRef> {
    if let Some(remote_head) = optional(
        repository,
        &["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"],
    ) && let Some(name) = remote_head.strip_prefix("refs/remotes/")
        && let Some(branch) = name.strip_prefix("origin/")
    {
        if fetch {
            // Best effort: a failed or slow fetch leaves the last known
            // remote state, which can only make fewer worktrees eligible.
            let _ = run_with_timeout(
                repository,
                &["fetch", "--quiet", "--no-tags", "origin", branch],
                FETCH_TIMEOUT,
            );
        }
        let reference = format!("refs/remotes/{name}");
        if optional(
            repository,
            &[
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("{reference}^{{commit}}"),
            ],
        )
        .is_some()
        {
            return Some(BaseRef {
                name: name.to_owned(),
                reference,
            });
        }
    }
    let branch = optional(repository, &["symbolic-ref", "--quiet", "--short", "HEAD"])?;
    if is_waku_branch(&branch) {
        return None;
    }
    let reference = format!("refs/heads/{branch}");
    optional(
        repository,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{reference}^{{commit}}"),
        ],
    )?;
    Some(BaseRef {
        name: branch,
        reference,
    })
}

/// The commit checked out in `toplevel`.
pub fn head(toplevel: &Path) -> anyhow::Result<String> {
    Ok(
        stdout(toplevel, &["rev-parse", "--verify", "HEAD^{commit}"])?
            .trim()
            .to_owned(),
    )
}

/// Count tracked changes and untracked files, like `git status`.
pub fn local_changes(toplevel: &Path) -> anyhow::Result<(u32, u32)> {
    let raw = stdout(
        toplevel,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--ignore-submodules=none",
            "--untracked-files=all",
        ],
    )?;
    let mut tracked = 0;
    let mut untracked = 0;
    let mut records = raw.split('\0').filter(|record| !record.is_empty());
    while let Some(record) = records.next() {
        let status = record.as_bytes();
        if status.starts_with(b"??") {
            untracked += 1;
        } else {
            tracked += 1;
            // A rename or copy carries its source path as the next record.
            if status
                .iter()
                .take(2)
                .any(|code| matches!(code, b'R' | b'C'))
            {
                records.next();
            }
        }
    }
    Ok((tracked, untracked))
}

/// The first ignored path that is not a dependency cache.
fn ignored_example(toplevel: &Path) -> anyhow::Result<Option<String>> {
    let raw = stdout(
        toplevel,
        &[
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--directory",
            "-z",
        ],
    )?;
    Ok(raw
        .split('\0')
        .filter(|entry| !entry.is_empty())
        .find(|entry| {
            !DEPENDENCY_CACHES.iter().any(|cache| {
                entry
                    .strip_suffix(cache)
                    .is_some_and(|parent| parent.is_empty() || parent.ends_with('/'))
            })
        })
        .map(str::to_owned))
}

fn count_lines(text: &str) -> u32 {
    text.lines().filter(|line| !line.trim().is_empty()).count() as u32
}

fn rev_count(toplevel: &Path, args: &[&str]) -> anyhow::Result<u32> {
    stdout(toplevel, args)?
        .trim()
        .parse()
        .context("Git returned an invalid commit count")
}

/// Gather everything [`super::policy`] needs about one worktree. Comparisons
/// with the base only run when `base` is given; ignored files only for the
/// "any local files" level.
pub fn inspect(
    toplevel: &Path,
    branch: &str,
    base: Option<&BaseRef>,
    keep_when: WorktreeKeepWhen,
) -> anyhow::Result<GitFacts> {
    let current = optional(toplevel, &["symbolic-ref", "--quiet", "HEAD"]);
    let branch_ref = format!("refs/heads/{branch}");
    let mut facts = GitFacts {
        branch_matches: current.as_deref() == Some(branch_ref.as_str()),
        ..GitFacts::default()
    };
    if !facts.branch_matches {
        return Ok(facts);
    }
    (facts.tracked_changes, facts.untracked_files) = local_changes(toplevel)?;
    if keep_when == WorktreeKeepWhen::AnyLocalFiles {
        facts.ignored_example = ignored_example(toplevel)?;
    }

    // Commits that no other branch or remote reaches would be lost with this
    // branch. The slug charset makes `branch` a literal exclusion pattern.
    let exclude = format!("--exclude={branch}");
    let only_here = |starts: &[&str]| -> anyhow::Result<HashSet<String>> {
        let mut args = vec!["rev-list"];
        args.extend_from_slice(starts);
        args.extend(["--not", exclude.as_str(), "--branches", "--remotes"]);
        Ok(stdout(toplevel, &args)?
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect())
    };

    let Some(base) = base else {
        facts.unpushed_commits = only_here(&["HEAD"])?.len() as u32;
        return Ok(facts);
    };
    facts.base = Some(base.name.clone());
    let range = format!("{}..HEAD", base.reference);
    facts.ahead_of_base = Some(rev_count(toplevel, &["rev-list", "--count", &range])?);
    // `git cherry` marks commits whose patch is already upstream with `-`,
    // which recognizes rebase and cherry-pick merges.
    let unmerged: Vec<String> = stdout(toplevel, &["cherry", &base.reference, "HEAD"])?
        .lines()
        .filter_map(|line| line.strip_prefix("+ "))
        .map(|oid| oid.trim().to_owned())
        .collect();
    facts.unmerged_commits = Some(unmerged.len() as u32);
    if !unmerged.is_empty() {
        let starts: Vec<&str> = unmerged.iter().map(String::as_str).collect();
        let reachable_only_here = only_here(&starts)?;
        facts.unpushed_commits = unmerged
            .iter()
            .filter(|oid| reachable_only_here.contains(*oid))
            .count() as u32;
    }
    facts.branch_advanced = optional(
        toplevel,
        &["reflog", "show", "--format=%H", &branch_ref, "--"],
    )
    .is_some_and(|log| count_lines(&log) > 1);
    Ok(facts)
}

/// Remove one linked worktree. With the tracked-changes level, untracked
/// files are cleaned first; Git still refuses if a tracked edit appears.
pub fn remove(
    repository: &Path,
    toplevel: &Path,
    keep_when: WorktreeKeepWhen,
) -> anyhow::Result<()> {
    if keep_when == WorktreeKeepWhen::TrackedChanges {
        let output = run(toplevel, &["clean", "-ffdx", "--quiet"])?;
        if !output.success {
            bail!("{}", first_line(&output.stderr, "git clean failed"));
        }
    }
    let path = toplevel.to_str().context("worktree path must be UTF-8")?;
    let output = run(
        repository,
        &[
            "-c",
            "status.showUntrackedFiles=normal",
            "worktree",
            "remove",
            path,
        ],
    )?;
    if !output.success {
        bail!(
            "{}",
            first_line(&output.stderr, "git worktree remove failed")
        );
    }
    Ok(())
}

/// Delete a `waku/` branch only while it still points at `expected` and no
/// worktree has it checked out.
pub fn delete_branch(repository: &Path, branch: &str, expected: &str) -> anyhow::Result<()> {
    if !is_waku_branch(branch) {
        bail!("only Waku's own worktree branches are deleted");
    }
    if crate::git_workflow::list_worktrees(repository)?
        .iter()
        .any(|worktree| worktree.branch.as_deref() == Some(branch))
    {
        bail!("the branch is checked out in another worktree");
    }
    let reference = format!("refs/heads/{branch}");
    stdout(repository, &["update-ref", "-d", &reference, expected])?;
    Ok(())
}

/// Drop administrative files of worktrees whose directories are gone.
pub fn prune(repository: &Path) -> anyhow::Result<()> {
    stdout(repository, &["worktree", "prune"])?;
    Ok(())
}

/// Check a removed worktree out again from its kept branch.
pub fn restore(repository: &Path, toplevel: &Path, branch: &str) -> anyhow::Result<()> {
    if !is_waku_branch(branch) {
        bail!("only Waku's own worktree branches are restored");
    }
    if let Some(parent) = toplevel.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // A stale registration of the same path would make `add` refuse.
    let _ = prune(repository);
    let path = toplevel.to_str().context("worktree path must be UTF-8")?;
    stdout(repository, &["worktree", "add", path, branch])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_slug_branches_under_the_waku_prefix_qualify() {
        assert!(is_waku_branch("waku/fix-the-picker-2"));
        assert!(!is_waku_branch("waku/"));
        assert!(!is_waku_branch("main"));
        assert!(!is_waku_branch("waku/feature/nested"));
        assert!(!is_waku_branch("waku/glob*"));
    }
}
