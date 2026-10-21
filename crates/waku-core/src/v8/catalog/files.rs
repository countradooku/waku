//! `files.tree`, `files.searchEntries`, `files.searchContents`,
//! `files.write` and `fs.browse`.

use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use waku_protocol::v8::{
    CONTENT_PREVIEW_CHARS, ContentHit, DEFAULT_CONTENT_HITS, DEFAULT_SEARCH_ENTRIES, Directory,
    DirectoryEntry, Error, ErrorCode, ErrorDetails, FileEntry, FileRevision,
    FilesSearchContentsParams, FilesSearchEntriesParams, FilesTreeParams, FilesWriteParams,
    FsBrowseParams, MAX_CONTENT_HITS, MAX_SEARCH_ENTRIES, TreeEntry,
};

use super::{Domain, bad_request, display_path, path_string, workspace};
use crate::v8::V8Context;
use crate::v8::methods::{resolve, root_path};
use crate::workspace::{WorkspaceOperation, WorkspaceResult};

/// Files read per `files.searchContents`; a larger tree is searched in
/// part.
const MAX_SEARCHED_FILES: usize = 50_000;
/// Files above this are not searched.
const MAX_SEARCHED_FILE_BYTES: u64 = 2 * 1024 * 1024;
/// Bytes inspected to tell text from binary.
const BINARY_SNIFF_BYTES: usize = 8 * 1024;

/// Serializes `files.write`, so a `baseRevision` check and the write after
/// it cannot interleave with another write.
static WRITES: Mutex<()> = Mutex::new(());

/// A file's content revision, as `files.read` reports it.
pub(crate) fn file_revision(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

/// `/`-separated, whatever the host's separator.
fn slash_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

pub(super) fn tree(context: &V8Context, params: FilesTreeParams) -> Result<Vec<TreeEntry>, Error> {
    let root = root_path(context, &params.root)?;
    let expanded = params
        .expanded
        .iter()
        .map(|relative| resolve(&root, relative))
        .collect::<Result<Vec<_>, _>>()?;
    let WorkspaceResult::WorkingTree { entries } = workspace(
        context,
        WorkspaceOperation::ListTree {
            root,
            expanded_paths: expanded,
        },
        Domain::General,
    )?
    else {
        return Err(super::internal("unexpected tree result"));
    };
    Ok(entries
        .into_iter()
        .map(|entry| TreeEntry {
            path: slash_path(Path::new(&entry.relative_path)),
            name: entry.name,
            is_dir: entry.is_dir,
            expanded: entry.expanded,
            depth: u32::try_from(entry.depth).unwrap_or(u32::MAX),
        })
        .collect())
}

pub(super) fn search_entries(
    context: &V8Context,
    params: FilesSearchEntriesParams,
) -> Result<Vec<FileEntry>, Error> {
    let root = root_path(context, &params.root)?;
    let limit = params
        .limit
        .unwrap_or(DEFAULT_SEARCH_ENTRIES)
        .min(MAX_SEARCH_ENTRIES) as usize;
    let query = params.query.trim();
    // An empty query lists the head of the index; a query ranks all of it.
    let index_cap = if query.is_empty() {
        limit
    } else {
        MAX_SEARCH_ENTRIES as usize * 5
    };
    let WorkspaceResult::ProjectFiles { entries } = workspace(
        context,
        WorkspaceOperation::ListProjectFiles {
            root,
            cap: index_cap,
        },
        Domain::General,
    )?
    else {
        return Err(super::internal("unexpected file index result"));
    };
    let entries = if query.is_empty() {
        entries
    } else {
        let mut matcher = crate::composer_complete::matcher();
        let mut ranked = crate::composer_complete::filter_files(&entries, query, &mut matcher)
            .into_iter()
            .map(|scored| scored.item)
            .collect::<Vec<_>>();
        // `filter_files` caps at the popup's size; a larger limit gets the
        // rest of the matches in index order.
        if ranked.len() < limit {
            let pattern = query.to_lowercase();
            for entry in &entries {
                if ranked.len() >= limit {
                    break;
                }
                if entry.path.to_lowercase().contains(&pattern) && !ranked.contains(entry) {
                    ranked.push(entry.clone());
                }
            }
        }
        ranked
    };
    Ok(entries
        .into_iter()
        .take(limit)
        .map(|entry| FileEntry {
            path: entry.path,
            is_dir: entry.is_dir,
        })
        .collect())
}

pub(super) fn search_contents(
    context: &V8Context,
    params: FilesSearchContentsParams,
) -> Result<Vec<ContentHit>, Error> {
    let root = root_path(context, &params.root)?;
    if params.query.is_empty() {
        return Err(bad_request("query", "empty"));
    }
    let limit = params
        .limit
        .unwrap_or(DEFAULT_CONTENT_HITS)
        .min(MAX_CONTENT_HITS) as usize;
    let pattern = if params.regex {
        params.query.clone()
    } else {
        regex::escape(&params.query)
    };
    let matcher = regex::RegexBuilder::new(&pattern)
        .case_insensitive(!params.case_sensitive)
        .size_limit(1 << 20)
        .build()
        .map_err(|error| bad_request("query", error.to_string()))?;
    let files = crate::composer_complete::list_project_files(&root, MAX_SEARCHED_FILES);
    let mut hits = Vec::new();
    for file in files.iter().filter(|entry| !entry.is_dir) {
        if hits.len() >= limit {
            break;
        }
        search_file(&root, &file.path, &matcher, limit, &mut hits);
    }
    Ok(hits)
}

fn search_file(
    root: &Path,
    relative: &str,
    matcher: &regex::Regex,
    limit: usize,
    hits: &mut Vec<ContentHit>,
) {
    let path = root.join(relative);
    let Ok(metadata) = std::fs::metadata(&path) else {
        return;
    };
    if !metadata.is_file() || metadata.len() > MAX_SEARCHED_FILE_BYTES {
        return;
    }
    let Ok(bytes) = std::fs::read(&path) else {
        return;
    };
    if bytes[..bytes.len().min(BINARY_SNIFF_BYTES)].contains(&0) {
        return;
    }
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return;
    };
    for (index, line) in text.lines().enumerate() {
        let Some(found) = matcher.find(line) else {
            continue;
        };
        hits.push(ContentHit {
            path: relative.to_owned(),
            line: u32::try_from(index + 1).unwrap_or(u32::MAX),
            column: u32::try_from(line[..found.start()].chars().count() + 1).unwrap_or(u32::MAX),
            preview: line.chars().take(CONTENT_PREVIEW_CHARS as usize).collect(),
        });
        if hits.len() >= limit {
            return;
        }
    }
}

pub(super) fn write(context: &V8Context, params: FilesWriteParams) -> Result<FileRevision, Error> {
    let root = root_path(context, &params.root)?;
    let path = resolve(&root, &params.path)?;
    let _guard = WRITES.lock();
    if let Some(base) = &params.base_revision {
        let current = match std::fs::read(&path) {
            Ok(bytes) => file_revision(&bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(Error::new(
                    ErrorCode::Io,
                    format!("{}: {error}", params.path),
                ));
            }
        };
        if &current != base {
            return Err(Error::new(
                ErrorCode::Conflict,
                format!("{} changed since revision {base}", params.path),
            )
            .with_details(ErrorDetails::RevisionConflict { current }));
        }
    }
    let revision = file_revision(params.content.as_bytes());
    workspace(
        context,
        WorkspaceOperation::WriteTextFile {
            root,
            relative_path: PathBuf::from(&params.path),
            content: params.content,
        },
        Domain::General,
    )?;
    Ok(FileRevision { revision })
}

pub(super) fn browse(context: &V8Context, params: FsBrowseParams) -> Result<Directory, Error> {
    let path = params
        .path
        .as_deref()
        .map(|path| super::absolute_path("path", path))
        .transpose()?;
    let WorkspaceResult::Directory {
        path,
        parent,
        home,
        filesystem_root,
        entries,
    } = workspace(
        context,
        WorkspaceOperation::BrowseDirectory { path },
        Domain::General,
    )?
    else {
        return Err(super::internal("unexpected directory result"));
    };
    Ok(Directory {
        path: path_string(&path)?,
        parent: parent.as_deref().map(display_path),
        home: display_path(&home),
        filesystem_root: display_path(&filesystem_root),
        entries: entries
            .into_iter()
            .map(|entry| DirectoryEntry {
                name: entry.name,
                path: display_path(&entry.absolute_path),
                is_dir: entry.is_dir,
            })
            .collect(),
    })
}
