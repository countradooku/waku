//! Git operations run on the daemon's workspace workers, never a render path.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use anyhow::{Context, bail};
use parking_lot::Mutex;
use waku_protocol::git::{GitStatus, GitStatusFile, GitWorktree};

const MAX_OUTPUT: usize = 1024 * 1024;
const MAX_PATHS: usize = 512;
const TIMEOUT: Duration = Duration::from_secs(120);

static REPOSITORY_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// Git's index lock protects individual commands. This also protects multi-step
// operations against another Waku client using a linked worktree.
pub(crate) fn repository_lock(cwd: &Path) -> anyhow::Result<Arc<Mutex<()>>> {
    let common = git(
        cwd,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?;
    let common = std::fs::canonicalize(line(&common)?)?;
    let mut locks = REPOSITORY_LOCKS.lock();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(&common).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(common, Arc::downgrade(&lock));
    Ok(lock)
}

pub fn status(cwd: &Path) -> anyhow::Result<GitStatus> {
    let repository = repository(cwd)?;
    let raw = git(
        &repository,
        &[
            "status",
            "--porcelain=v2",
            "--branch",
            "-z",
            "--untracked-files=all",
        ],
    )?;
    parse_status(repository, &raw)
}

pub fn stage_files(cwd: &Path, paths: &[String], stage: bool) -> anyhow::Result<()> {
    if paths.is_empty() || paths.len() > MAX_PATHS {
        bail!("select between 1 and {MAX_PATHS} files");
    }
    let snapshot = status(cwd)?;
    let selected: HashSet<&str> = paths.iter().map(String::as_str).collect();
    let mut known = HashSet::new();
    let mut expanded = Vec::new();
    for file in &snapshot.files {
        if !selected.contains(file.path.as_str()) {
            continue;
        }
        known.insert(file.path.as_str());
        expanded.push(file.path.as_str());
        // Unstaging an indexed rename must restore both paths. When staging
        // it again, its old name has already left the index and would be an
        // unmatched pathspec. Only a working-tree rename needs that old path.
        if let Some(previous) = &file.previous_path
            && (!stage || file.worktree_status == "R")
        {
            expanded.push(previous);
        }
    }
    if selected.iter().any(|path| !known.contains(path)) {
        bail!("the selected files changed; refresh Git status and select them again");
    }
    for path in &expanded {
        validate_path(&snapshot.repository, path)?;
    }
    let mut args = if stage {
        vec!["add", "-A", "--"]
    } else if snapshot.head.is_some() {
        vec!["restore", "--staged", "--"]
    } else {
        // `restore --staged` needs HEAD. Remove only index entries in an
        // unborn repository; never remove the user's working files.
        vec!["rm", "--cached", "--ignore-unmatch", "--"]
    };
    args.extend(expanded);
    git(&snapshot.repository, &args)?;
    Ok(())
}

pub fn pull(cwd: &Path) -> anyhow::Result<()> {
    let snapshot = status(cwd)?;
    if snapshot.branch.is_none() || snapshot.head.is_none() {
        bail!("pull requires a checked-out branch with a commit");
    }
    if snapshot.upstream.is_none() {
        bail!("this branch has no upstream; configure one before pulling");
    }
    if !snapshot.files.is_empty() {
        bail!("commit or stash local changes before pulling");
    }
    // Override local pull.rebase and autostash settings: this action must not
    // rewrite commits, hide local edits, or leave a merge awaiting input.
    git(
        &snapshot.repository,
        &["pull", "--ff-only", "--no-rebase", "--no-autostash"],
    )?;
    Ok(())
}

pub fn list_worktrees(cwd: &Path) -> anyhow::Result<Vec<GitWorktree>> {
    parse_worktrees(&git(cwd, &["worktree", "list", "--porcelain", "-z"])?)
}

fn repository(cwd: &Path) -> anyhow::Result<PathBuf> {
    let root = git(cwd, &["rev-parse", "--show-toplevel"])?;
    std::fs::canonicalize(line(&root)?).context("could not resolve Git repository")
}

// rev-parse emits one LF after the path. Whitespace before it belongs to the
// path, including a trailing space or newline on Unix.
fn line(bytes: &[u8]) -> anyhow::Result<&str> {
    text(
        bytes
            .strip_suffix(b"\n")
            .context("incomplete Git path response")?,
    )
}

fn validate_path(repository: &Path, value: &str) -> anyhow::Result<()> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains('\0')
        || path.is_absolute()
        || path.components().any(|component| match component {
            Component::Normal(name) => name
                .to_str()
                .is_some_and(|name| name.eq_ignore_ascii_case(".git")),
            _ => true,
        })
    {
        bail!("invalid repository-relative file path");
    }
    // Git may stage a symlink itself, but no parent may lead outside the
    // repository. Walk existing ancestors so deleted paths remain selectable.
    let mut parent = repository.join(path);
    parent.pop();
    while !parent.exists() {
        if !parent.pop() {
            bail!("could not resolve selected file parent");
        }
    }
    if !std::fs::canonicalize(parent)?.starts_with(repository) {
        bail!("selected file is outside its repository");
    }
    Ok(())
}

fn text(bytes: &[u8]) -> anyhow::Result<&str> {
    std::str::from_utf8(bytes)
        .context("Git returned a non-UTF-8 path; use Git directly for this file")
}

fn parse_status(repository: PathBuf, raw: &[u8]) -> anyhow::Result<GitStatus> {
    if !raw.is_empty() && !raw.ends_with(b"\0") {
        bail!("incomplete Git status response");
    }
    let mut snapshot = GitStatus {
        repository,
        branch: None,
        head: None,
        upstream: None,
        ahead: 0,
        behind: 0,
        files: Vec::new(),
    };
    let mut records = text(raw)?.split('\0').filter(|record| !record.is_empty());
    while let Some(record) = records.next() {
        if let Some(value) = record.strip_prefix("# branch.oid ") {
            snapshot.head = (value != "(initial)").then(|| value.to_owned());
        } else if let Some(value) = record.strip_prefix("# branch.head ") {
            snapshot.branch = (value != "(detached)").then(|| value.to_owned());
        } else if let Some(value) = record.strip_prefix("# branch.upstream ") {
            snapshot.upstream = Some(value.to_owned());
        } else if let Some(value) = record.strip_prefix("# branch.ab ") {
            let (ahead, behind) = value
                .split_once(' ')
                .context("invalid Git divergence record")?;
            snapshot.ahead = ahead
                .strip_prefix('+')
                .context("invalid Git ahead count")?
                .parse()?;
            snapshot.behind = behind
                .strip_prefix('-')
                .context("invalid Git behind count")?
                .parse()?;
        } else if let Some(path) = record.strip_prefix("? ") {
            snapshot.files.push(GitStatusFile {
                path: path.to_owned(),
                previous_path: None,
                index_status: "?".into(),
                worktree_status: "?".into(),
            });
        } else if record.starts_with("1 ") || record.starts_with("2 ") || record.starts_with("u ") {
            let field_count = match record.as_bytes()[0] {
                b'1' => 9,
                b'2' => 10,
                _ => 11,
            };
            let fields: Vec<_> = record.splitn(field_count, ' ').collect();
            if fields.len() != field_count || fields[1].len() != 2 || !fields[1].is_ascii() {
                bail!("invalid Git status record");
            }
            let previous_path = if record.starts_with("2 ") {
                Some(
                    records
                        .next()
                        .context("missing Git rename source")?
                        .to_owned(),
                )
            } else {
                None
            };
            snapshot.files.push(GitStatusFile {
                path: fields[field_count - 1].to_owned(),
                previous_path,
                index_status: fields[1][..1].to_owned(),
                worktree_status: fields[1][1..].to_owned(),
            });
        } else if !record.starts_with("# ") {
            bail!("unsupported Git status record");
        }
    }
    Ok(snapshot)
}

fn parse_worktrees(raw: &[u8]) -> anyhow::Result<Vec<GitWorktree>> {
    let mut worktrees = Vec::new();
    let mut current: Option<GitWorktree> = None;
    for record in text(raw)?.split('\0') {
        if let Some(path) = record.strip_prefix("worktree ") {
            if let Some(previous) = current.take() {
                worktrees.push(previous);
            }
            current = Some(GitWorktree {
                path: path.into(),
                head: String::new(),
                branch: None,
                bare: false,
                locked: false,
                prunable: false,
            });
        } else if let Some(entry) = &mut current {
            if let Some(head) = record.strip_prefix("HEAD ") {
                entry.head = head.into();
            } else if let Some(branch) = record.strip_prefix("branch refs/heads/") {
                entry.branch = Some(branch.into());
            } else if record == "bare" {
                entry.bare = true;
            } else if record == "locked" || record.starts_with("locked ") {
                entry.locked = true;
            } else if record == "prunable" || record.starts_with("prunable ") {
                entry.prunable = true;
            }
        }
    }
    if let Some(entry) = current {
        worktrees.push(entry);
    }
    Ok(worktrees)
}

pub(crate) fn git(cwd: &Path, args: &[&str]) -> anyhow::Result<Vec<u8>> {
    let mut command = crate::command_env::plain_command("git");
    command
        .arg("--literal-pathspecs")
        .args(args)
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_EDITOR", "true")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("LC_ALL", "C");
    let output = crate::git_commit::run_capture(&mut command, TIMEOUT)?;
    if !output.status.success() {
        bail!(
            "Git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
                .chars()
                .take(4000)
                .collect::<String>()
                .trim()
        );
    }
    if output.stdout.len() > MAX_OUTPUT {
        bail!("Git output exceeds 1 MiB; narrow the changes using Git before retrying");
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    struct Repository(PathBuf);
    impl Repository {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("waku-git-workflow-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            // Git reports canonical paths; on macOS the temp dir is under the
            // `/var` -> `/private/var` symlink, so compare against the real path.
            // Not on Windows, where `canonicalize` yields `\\?\` verbatim paths
            // that git does not accept.
            #[cfg(not(windows))]
            let path = path.canonicalize().unwrap();
            git(&path, &["init", "-b", "main"]).unwrap();
            git(&path, &["config", "user.name", "Waku Tests"]).unwrap();
            git(&path, &["config", "user.email", "waku@example.com"]).unwrap();
            git(&path, &["config", "core.autocrlf", "false"]).unwrap();
            Self(path)
        }

        fn commit(&self) {
            git(&self.0, &["add", "-A", "--", "."]).unwrap();
            git(
                &self.0,
                &["-c", "commit.gpgsign=false", "commit", "-m", "test"],
            )
            .unwrap();
        }
    }
    impl Drop for Repository {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    #[cfg(unix)]
    fn stages_literal_paths_and_unstages_without_losing_work() {
        let repo = Repository::new();
        for file in [
            "a.txt",
            "[a].txt",
            ":(glob)*",
            "two words.txt",
            "line\nbreak.txt",
            "-option",
        ] {
            fs::write(repo.0.join(file), "original\n").unwrap();
        }
        let names = vec![
            "[a].txt".into(),
            ":(glob)*".into(),
            "line\nbreak.txt".into(),
            "-option".into(),
        ];
        stage_files(&repo.0, &names, true).unwrap();
        let snapshot = status(&repo.0).unwrap();
        assert!(snapshot.head.is_none());
        assert_eq!(
            snapshot
                .files
                .iter()
                .filter(|file| file.index_status == "A")
                .count(),
            4
        );
        assert_eq!(
            snapshot
                .files
                .iter()
                .find(|file| file.path == "a.txt")
                .unwrap()
                .index_status,
            "?"
        );
        stage_files(&repo.0, &names, false).unwrap();
        assert!(
            status(&repo.0)
                .unwrap()
                .files
                .iter()
                .all(|file| file.index_status == "?")
        );
        repo.commit();
        fs::write(repo.0.join("[a].txt"), "edited\n").unwrap();
        stage_files(&repo.0, &["[a].txt".into()], true).unwrap();
        stage_files(&repo.0, &["[a].txt".into()], false).unwrap();
        assert_eq!(
            fs::read_to_string(repo.0.join("[a].txt")).unwrap(),
            "edited\n"
        );
        assert_eq!(status(&repo.0).unwrap().files[0].worktree_status, "M");
    }

    #[test]
    fn unstage_rename_restores_both_index_paths() {
        let repo = Repository::new();
        fs::write(repo.0.join("old name"), "content\n").unwrap();
        repo.commit();
        git(&repo.0, &["mv", "old name", "new name"]).unwrap();
        let snapshot = status(&repo.0).unwrap();
        assert_eq!(snapshot.files[0].previous_path.as_deref(), Some("old name"));
        stage_files(&repo.0, &["new name".into()], false).unwrap();
        assert!(
            git(&repo.0, &["diff", "--cached", "--name-only"])
                .unwrap()
                .is_empty()
        );
        assert!(repo.0.join("new name").exists());
        assert!(!repo.0.join("old name").exists());
    }

    #[test]
    fn can_stage_new_edits_to_an_already_staged_rename() {
        let repo = Repository::new();
        fs::write(repo.0.join("old"), "one\ntwo\nthree\nfour\n").unwrap();
        repo.commit();
        git(&repo.0, &["mv", "old", "new"]).unwrap();
        fs::write(repo.0.join("new"), "one\ntwo\nthree\nfour\nfive\n").unwrap();
        let before = status(&repo.0).unwrap();
        assert_eq!(before.files[0].index_status, "R");
        assert_eq!(before.files[0].worktree_status, "M");
        stage_files(&repo.0, &["new".into()], true).unwrap();
        assert!(git(&repo.0, &["diff", "--name-only"]).unwrap().is_empty());
        assert_eq!(
            git(&repo.0, &["show", ":new"]).unwrap(),
            b"one\ntwo\nthree\nfour\nfive\n"
        );
    }

    #[test]
    fn rejects_unknown_or_unsafe_selections_without_changing_index() {
        let repo = Repository::new();
        fs::write(repo.0.join("file"), "content\n").unwrap();
        for paths in [
            vec![],
            vec![".".into()],
            vec!["../outside".into()],
            vec![".git/config".into()],
            vec!["file".into(), "missing".into()],
        ] {
            assert!(stage_files(&repo.0, &paths, true).is_err());
            assert!(git(&repo.0, &["ls-files"]).unwrap().is_empty());
        }
    }

    #[test]
    fn status_and_selection_are_repository_relative_from_nested_projects() {
        let repo = Repository::new();
        fs::create_dir(repo.0.join("nested")).unwrap();
        fs::write(repo.0.join("nested/file"), "content\n").unwrap();
        let nested = repo.0.join("nested");
        let snapshot = status(&nested).unwrap();
        assert_eq!(snapshot.repository, repo.0);
        assert_eq!(snapshot.files[0].path, "nested/file");
        stage_files(&nested, &["nested/file".into()], true).unwrap();
        assert_eq!(status(&nested).unwrap().files[0].index_status, "A");
    }

    #[test]
    fn lists_linked_worktrees_and_shares_their_mutation_lock() {
        let repo = Repository::new();
        fs::write(repo.0.join("file"), "content\n").unwrap();
        repo.commit();
        let linked = repo.0.join("linked");
        git(
            &repo.0,
            &["worktree", "add", "-b", "feature", linked.to_str().unwrap()],
        )
        .unwrap();
        git(
            &repo.0,
            &[
                "worktree",
                "lock",
                "--reason",
                "in use",
                linked.to_str().unwrap(),
            ],
        )
        .unwrap();
        let entries = list_worktrees(&repo.0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].branch.as_deref(), Some("feature"));
        assert!(entries[1].locked);
        assert!(Arc::ptr_eq(
            &repository_lock(&repo.0).unwrap(),
            &repository_lock(&linked).unwrap()
        ));
    }

    #[test]
    fn pull_fast_forwards_but_refuses_dirty_or_divergent_checkouts() {
        let remote = Repository::new();
        fs::write(remote.0.join("file"), "first\n").unwrap();
        remote.commit();
        let local = Repository::new();
        git(
            &local.0,
            &["remote", "add", "origin", remote.0.to_str().unwrap()],
        )
        .unwrap();
        git(&local.0, &["fetch", "origin"]).unwrap();
        git(&local.0, &["reset", "--hard", "origin/main"]).unwrap();
        git(
            &local.0,
            &["branch", "--set-upstream-to=origin/main", "main"],
        )
        .unwrap();
        fs::write(remote.0.join("file"), "second\n").unwrap();
        remote.commit();
        fs::write(local.0.join("untracked"), "keep\n").unwrap();
        assert!(pull(&local.0).unwrap_err().to_string().contains("stash"));
        fs::remove_file(local.0.join("untracked")).unwrap();
        git(&local.0, &["config", "pull.rebase", "true"]).unwrap();
        pull(&local.0).unwrap();
        assert_eq!(
            fs::read_to_string(local.0.join("file")).unwrap(),
            "second\n"
        );
        fs::write(remote.0.join("remote"), "remote\n").unwrap();
        remote.commit();
        fs::write(local.0.join("local"), "local\n").unwrap();
        local.commit();
        let head = status(&local.0).unwrap().head;
        assert!(pull(&local.0).is_err());
        let snapshot = status(&local.0).unwrap();
        assert_eq!(snapshot.head, head);
        assert_eq!((snapshot.ahead, snapshot.behind), (1, 1));
        assert!(!local.0.join(".git/MERGE_HEAD").exists());
    }

    #[test]
    #[cfg(unix)]
    fn repository_paths_preserve_trailing_whitespace() {
        let mut repo = Repository::new();
        let renamed = repo.0.with_file_name(format!(
            "{} \n",
            repo.0.file_name().unwrap().to_str().unwrap()
        ));
        fs::rename(&repo.0, &renamed).unwrap();
        repo.0 = renamed;
        fs::write(repo.0.join("file"), "content\n").unwrap();
        assert_eq!(status(&repo.0).unwrap().repository, repo.0);
        let _lock = repository_lock(&repo.0).unwrap();
        stage_files(&repo.0, &["file".into()], true).unwrap();
        assert_eq!(status(&repo.0).unwrap().files[0].index_status, "A");
    }

    #[test]
    fn parser_preserves_paths_and_rejects_incomplete_records() {
        let raw = b"# branch.oid (initial)\0# branch.head main\0? \"tab\t and\nnewline\"\0";
        assert_eq!(
            parse_status(PathBuf::new(), raw).unwrap().files[0].path,
            "\"tab\t and\nnewline\""
        );
        assert!(parse_status(PathBuf::new(), b"1 M\0").is_err());
        assert!(parse_status(PathBuf::new(), b"? truncated").is_err());
        assert!(parse_status(PathBuf::new(), b"? bad\xff\0").is_err());
    }

    #[test]
    fn detached_and_unborn_branches_cannot_pull() {
        let repo = Repository::new();
        assert!(pull(&repo.0).is_err());
        fs::write(repo.0.join("file"), "content\n").unwrap();
        repo.commit();
        assert!(pull(&repo.0).unwrap_err().to_string().contains("upstream"));
        git(&repo.0, &["checkout", "--detach"]).unwrap();
        assert!(status(&repo.0).unwrap().branch.is_none());
        assert!(pull(&repo.0).is_err());
    }

    #[test]
    #[cfg(unix)]
    fn symlink_parent_cannot_escape_the_repository() {
        let repo = Repository::new();
        let outside = Repository::new();
        std::os::unix::fs::symlink(&outside.0, repo.0.join("link")).unwrap();
        assert!(validate_path(&repo.0, "link/file").is_err());
        // Staging the link itself is safe: Git records the link, not its target.
        stage_files(&repo.0, &["link".into()], true).unwrap();
        assert!(git(&outside.0, &["ls-files"]).unwrap().is_empty());
    }

    #[test]
    #[ignore = "manual Git status performance measurement"]
    fn benchmark_large_status() {
        let repo = Repository::new();
        for index in 0..10_000 {
            fs::write(repo.0.join(format!("file-{index}")), "content\n").unwrap();
        }
        let started = std::time::Instant::now();
        let mut samples = Vec::new();
        for _ in 0..10 {
            let sample = std::time::Instant::now();
            assert_eq!(status(&repo.0).unwrap().files.len(), 10_000);
            samples.push(sample.elapsed().as_millis());
        }
        samples.sort_unstable();
        eprintln!(
            "10,000 files: median={}ms max={}ms total={:?}",
            samples[5],
            samples[9],
            started.elapsed()
        );
    }
}
