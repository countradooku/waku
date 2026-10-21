//! Files and the filesystem: `files.tree`, `files.read`, `files.write`,
//! `files.searchEntries` and `fs.browse`.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail};
use uuid::Uuid;

use super::DaemonV8;
use crate::v8::{
    Directory, FilesReadParams, FilesSearchEntriesParams, FilesTreeParams, FilesWriteParams,
    FsBrowseParams, MAX_SEARCH_ENTRIES, RootRef, TreeEntry, method,
};

/// A daemon-host directory as a root. The desktop still knows its
/// workspaces by path; a thread or project root replaces this once the
/// session layer hands out orchestration ids (#55).
pub fn path_root(path: &Path) -> RootRef {
    RootRef::Path {
        path: path.to_string_lossy().into_owned(),
    }
}

/// `path` relative to `root` with `/` separators, if it lies inside it.
pub fn relative_slash_path(root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    let parts = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| parts.join("/"))
}

/// One row of a working tree, with its absolute daemon-host path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeRow {
    pub relative_path: String,
    pub absolute_path: PathBuf,
    pub name: String,
    pub is_dir: bool,
    pub expanded: bool,
    pub depth: usize,
}

/// The `files.tree` params for `root` with the `expanded` directories
/// (absolute paths; those outside `root` are dropped).
pub fn tree_params<'a>(
    root: &Path,
    expanded: impl IntoIterator<Item = &'a PathBuf>,
) -> FilesTreeParams {
    let mut expanded = expanded
        .into_iter()
        .filter_map(|path| relative_slash_path(root, path))
        .collect::<Vec<_>>();
    expanded.sort();
    FilesTreeParams {
        root: path_root(root),
        expanded,
    }
}

/// `files.tree` rows under `root`.
pub fn tree_rows(root: &Path, entries: Vec<TreeEntry>) -> Vec<TreeRow> {
    entries
        .into_iter()
        .map(|entry| TreeRow {
            absolute_path: entry
                .path
                .split('/')
                .filter(|part| !part.is_empty())
                .fold(root.to_path_buf(), |path, part| path.join(part)),
            relative_path: entry.path,
            name: entry.name,
            is_dir: entry.is_dir,
            expanded: entry.expanded,
            depth: entry.depth as usize,
        })
        .collect()
}

/// The working tree under `root` with the `expanded` directories open.
pub fn tree<'a>(
    daemon: &DaemonV8,
    root: &Path,
    expanded: impl IntoIterator<Item = &'a PathBuf>,
) -> anyhow::Result<Vec<TreeRow>> {
    let entries = daemon.call::<method::FilesTree>(tree_params(root, expanded))?;
    Ok(tree_rows(root, entries))
}

/// The text of `relative_path` under `root`, downloading it when it is too
/// large to come inline. A binary file is an error.
pub fn read_text(daemon: &DaemonV8, root: &Path, relative_path: &str) -> anyhow::Result<String> {
    let content = daemon.call::<method::FilesRead>(FilesReadParams {
        root: path_root(root),
        path: relative_path.replace('\\', "/"),
        max_bytes: None,
    })?;
    if content.binary {
        bail!("{relative_path} is not a text file");
    }
    if let Some(text) = content.text {
        return Ok(text);
    }
    let reference = content
        .blob
        .context("the daemon sent neither the text nor a blob")?;
    let (_, bytes) = daemon
        .client()?
        .download_blob(&reference, None)
        .map_err(super::call_error)?;
    String::from_utf8(bytes).context("the file is not valid UTF-8")
}

/// Overwrite `relative_path` under `root`; returns the new revision.
pub fn write_text(
    daemon: &DaemonV8,
    root: &Path,
    relative_path: &str,
    content: String,
) -> anyhow::Result<String> {
    Ok(daemon
        .call::<method::FilesWrite>(FilesWriteParams {
            root: path_root(root),
            path: relative_path.replace('\\', "/"),
            content,
            base_revision: None,
            op_id: Uuid::new_v4(),
        })?
        .revision)
}

/// A directory on the daemon host, or its home for `None`.
pub fn browse(daemon: &DaemonV8, path: Option<&Path>) -> anyhow::Result<Directory> {
    daemon.call::<method::FsBrowse>(FsBrowseParams {
        path: path.map(|path| path.to_string_lossy().into_owned()),
    })
}

/// The `@`-mention index of `root`: up to `cap` entries, shallow first,
/// directories ending in `/`. The composer ranks them itself.
pub fn mention_index(
    daemon: &DaemonV8,
    root: &Path,
    cap: usize,
) -> anyhow::Result<Vec<waku_protocol::composer::FileEntry>> {
    let entries = daemon.call::<method::FilesSearchEntries>(FilesSearchEntriesParams {
        root: path_root(root),
        query: String::new(),
        limit: Some(
            u32::try_from(cap)
                .unwrap_or(u32::MAX)
                .min(MAX_SEARCH_ENTRIES),
        ),
    })?;
    Ok(entries
        .into_iter()
        .map(|entry| waku_protocol::composer::FileEntry {
            path: entry.path,
            is_dir: entry.is_dir,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expanded_directories_are_sent_relative_to_the_root() {
        let root = Path::new("/srv/app");
        let expanded = [
            PathBuf::from("/srv/app/src/ui"),
            PathBuf::from("/srv/app/docs"),
            PathBuf::from("/srv/other"),
            PathBuf::from("/srv/app"),
        ];
        let params = tree_params(root, &expanded);
        assert_eq!(
            params.root,
            RootRef::Path {
                path: "/srv/app".into()
            }
        );
        assert_eq!(params.expanded, ["docs", "src/ui"]);
    }

    #[test]
    fn tree_rows_regain_their_absolute_paths() {
        let rows = tree_rows(
            Path::new("/srv/app"),
            vec![
                TreeEntry {
                    path: "src".into(),
                    name: "src".into(),
                    is_dir: true,
                    expanded: true,
                    depth: 0,
                },
                TreeEntry {
                    path: "src/main.rs".into(),
                    name: "main.rs".into(),
                    is_dir: false,
                    expanded: false,
                    depth: 1,
                },
            ],
        );
        assert_eq!(rows[1].absolute_path, PathBuf::from("/srv/app/src/main.rs"));
        assert_eq!(rows[1].relative_path, "src/main.rs");
        assert_eq!(rows[1].depth, 1);
        assert!(rows[0].is_dir && rows[0].expanded);
    }
}
