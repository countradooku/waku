//! Daemon-owned Git checkpoint capture and restoration.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;

use anyhow::{Context as _, anyhow, bail};
use uuid::Uuid;

const EMPTY_TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

pub fn capture_ref(cwd: &Path, git_ref: &str) -> anyhow::Result<()> {
    if !is_git_repository(cwd) {
        bail!("checkpoints require a Git repository");
    }

    let commit = capture_worktree_commit(cwd)?;
    git_output(cwd, ["update-ref", git_ref, &commit])?;
    Ok(())
}

/// Capture the current worktree and untracked files as a dangling commit.
///
/// This shares the checkpoint path's isolated temporary index, so it never
/// stages or unstages the user's files. Review uses the returned treeish to
/// compare a stable worktree snapshot while edits continue on disk.
pub fn capture_worktree_commit(cwd: &Path) -> anyhow::Result<String> {
    if !is_git_repository(cwd) {
        bail!("worktree snapshots require a Git repository");
    }

    let head = resolve_ref(cwd, "HEAD");
    capture_worktree_commit_from(cwd, head.as_deref(), "Waku worktree snapshot", &[])
}

fn capture_worktree_commit_from(
    cwd: &Path,
    head: Option<&str>,
    message: &str,
    parents: &[String],
) -> anyhow::Result<String> {
    if !is_git_repository(cwd) {
        bail!("worktree snapshots require a Git repository");
    }

    let common_dir = git_output(cwd, ["rev-parse", "--git-common-dir"])?
        .trim()
        .to_owned();
    if common_dir.is_empty() {
        bail!("git did not return its common directory");
    }
    let common_dir = PathBuf::from(common_dir);
    let common_dir = if common_dir.is_absolute() {
        common_dir
    } else {
        cwd.join(common_dir)
    };
    let temporary_index = common_dir.join(format!("waku-checkpoint-index-{}", Uuid::new_v4()));

    let result = (|| {
        if let Some(head) = head {
            git_with_index(cwd, &temporary_index, ["read-tree", head])?;
        }
        git_with_index(cwd, &temporary_index, ["add", "-A", "--", "."])?;
        let tree = git_with_index(cwd, &temporary_index, ["write-tree"])?
            .trim()
            .to_owned();
        if tree.is_empty() {
            bail!("git write-tree returned no object id");
        }
        let mut arguments = vec![
            "commit-tree".to_owned(),
            tree,
            "-m".to_owned(),
            message.to_owned(),
        ];
        for parent in parents {
            arguments.push("-p".to_owned());
            arguments.push(parent.clone());
        }
        let commit = git_with_identity_and_index(cwd, &temporary_index, &arguments)?
            .trim()
            .to_owned();
        if commit.is_empty() {
            bail!("git commit-tree returned no object id");
        }
        Ok(commit)
    })();

    let _ = fs::remove_file(&temporary_index);
    let _ = fs::remove_file(temporary_index.with_extension("lock"));
    result
}

pub fn restore_ref(cwd: &Path, git_ref: &str) -> anyhow::Result<()> {
    let commit = resolve_ref(cwd, git_ref)
        .ok_or_else(|| anyhow!("checkpoint `{git_ref}` is unavailable"))?;
    git_output(
        cwd,
        [
            "restore",
            "--source",
            &commit,
            "--worktree",
            "--staged",
            "--",
            ".",
        ],
    )?;
    git_output(cwd, ["clean", "-fd", "--", "."])?;
    if has_head(cwd) {
        git_output(cwd, ["reset", "--quiet", "--", "."])?;
    }
    Ok(())
}

/// Whether `cwd` is inside a Git work tree.
pub fn is_repository(cwd: &Path) -> bool {
    is_git_repository(cwd)
}

/// One file that differs between two snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefDiffFile {
    pub path: String,
    /// `git diff --name-status` letter: `A`, `M`, `D`, `T`, …
    pub status: char,
    pub additions: u64,
    pub deletions: u64,
}

/// The files that differ from `from` to `to`; with no `from`, from `HEAD`
/// (or the empty tree in a repository without commits). Renames are
/// reported as a deletion and an addition.
pub fn diff_refs(cwd: &Path, from: Option<&str>, to: &str) -> anyhow::Result<Vec<RefDiffFile>> {
    let from = match from {
        Some(from) => from.to_owned(),
        None if has_head(cwd) => "HEAD".to_owned(),
        None => EMPTY_TREE.to_owned(),
    };
    let statuses = git_output(
        cwd,
        [
            "diff",
            "--name-status",
            "--no-renames",
            "-z",
            &from,
            to,
            "--",
            ".",
        ],
    )?;
    let mut status_by_path = HashMap::new();
    let mut fields = statuses.split('\0').filter(|field| !field.is_empty());
    while let (Some(status), Some(path)) = (fields.next(), fields.next()) {
        status_by_path.insert(path.to_owned(), status.chars().next().unwrap_or('M'));
    }
    let numstat = git_output(
        cwd,
        [
            "diff",
            "--numstat",
            "--no-renames",
            "-z",
            &from,
            to,
            "--",
            ".",
        ],
    )?;
    let mut files = Vec::new();
    for record in numstat.split('\0').filter(|record| !record.is_empty()) {
        let mut columns = record.splitn(3, '\t');
        let additions = columns.next().unwrap_or("0").parse().unwrap_or(0);
        let deletions = columns.next().unwrap_or("0").parse().unwrap_or(0);
        let Some(path) = columns.next() else {
            continue;
        };
        files.push(RefDiffFile {
            status: status_by_path.get(path).copied().unwrap_or('M'),
            path: path.to_owned(),
            additions,
            deletions,
        });
    }
    Ok(files)
}

pub fn has_ref(cwd: &Path, git_ref: &str) -> bool {
    resolve_ref(cwd, git_ref).is_some()
}

fn is_git_repository(cwd: &Path) -> bool {
    crate::command_env::plain_command("git")
        .args(["rev-parse", "--is-inside-work-tree"])
        .current_dir(cwd)
        .output()
        .is_ok_and(|output| output.status.success())
}

fn has_head(cwd: &Path) -> bool {
    crate::command_env::plain_command("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .current_dir(cwd)
        .output()
        .is_ok_and(|output| output.status.success())
}

fn resolve_ref(cwd: &Path, git_ref: &str) -> Option<String> {
    let output = crate::command_env::plain_command("git")
        .args(["rev-parse", "--verify", &format!("{git_ref}^{{commit}}")])
        .current_dir(cwd)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn git_output<I, S>(cwd: &Path, args: I) -> anyhow::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
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

fn git_with_index<I, S>(cwd: &Path, index: &Path, args: I) -> anyhow::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    git_with_environment(cwd, index, args, false)
}

fn git_with_identity_and_index<I, S>(cwd: &Path, index: &Path, args: I) -> anyhow::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    git_with_environment(cwd, index, args, true)
}

fn git_with_environment<I, S>(
    cwd: &Path,
    index: &Path,
    args: I,
    identity: bool,
) -> anyhow::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = crate::command_env::plain_command("git");
    command
        .args(args)
        .current_dir(cwd)
        .env("GIT_INDEX_FILE", index);
    if identity {
        command
            .env("GIT_AUTHOR_NAME", "Waku")
            .env("GIT_AUTHOR_EMAIL", "waku@localhost")
            .env("GIT_COMMITTER_NAME", "Waku")
            .env("GIT_COMMITTER_EMAIL", "waku@localhost");
    }
    let output = command.output().context("failed to execute git")?;
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
    use super::*;

    fn git_ok(cwd: &Path, args: &[&str]) {
        let status = crate::command_env::plain_command("git")
            .args(args)
            .current_dir(cwd)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    fn git_text(cwd: &Path, args: &[&str]) -> String {
        let output = crate::command_env::plain_command("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {args:?} failed");
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    #[test]
    fn captures_diffs_and_restores_tracked_and_untracked_files() {
        let directory = std::env::temp_dir().join(format!("waku-checkpoints-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        git_ok(&directory, &["init", "--quiet"]);
        git_ok(&directory, &["config", "core.autocrlf", "false"]);
        fs::write(directory.join("tracked.txt"), "baseline\n").unwrap();
        git_ok(&directory, &["add", "tracked.txt"]);
        git_ok(
            &directory,
            &[
                "-c",
                "user.name=Waku Test",
                "-c",
                "user.email=waku@example.com",
                "commit",
                "--quiet",
                "-m",
                "baseline",
            ],
        );

        let baseline = "refs/waku/test-baseline";
        let turn = "refs/waku/test-turn";
        capture_ref(&directory, baseline).unwrap();

        fs::write(directory.join("tracked.txt"), "changed\n").unwrap();
        fs::write(directory.join("new.txt"), "new\n").unwrap();
        fs::write(directory.join("already-staged.txt"), "staged\n").unwrap();
        git_ok(&directory, &["add", "already-staged.txt"]);
        capture_ref(&directory, turn).unwrap();
        let mut files = diff_refs(&directory, Some(baseline), turn).unwrap();
        files.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(
            files
                .iter()
                .map(|file| (file.path.as_str(), file.status))
                .collect::<Vec<_>>(),
            [
                ("already-staged.txt", 'A'),
                ("new.txt", 'A'),
                ("tracked.txt", 'M')
            ]
        );
        // Capturing never touches the user's index.
        assert_eq!(
            git_text(&directory, &["diff", "--cached", "--name-only"]),
            "already-staged.txt"
        );

        fs::write(directory.join("tracked.txt"), "later\n").unwrap();
        fs::remove_file(directory.join("new.txt")).unwrap();
        fs::write(directory.join("discard.txt"), "discard\n").unwrap();
        restore_ref(&directory, turn).unwrap();

        assert_eq!(
            fs::read_to_string(directory.join("tracked.txt")).unwrap(),
            "changed\n"
        );
        assert_eq!(
            fs::read_to_string(directory.join("new.txt")).unwrap(),
            "new\n"
        );
        assert!(!directory.join("discard.txt").exists());

        fs::remove_dir_all(directory).ok();
    }
}
