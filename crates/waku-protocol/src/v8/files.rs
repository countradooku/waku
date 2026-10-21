//! `files.*` and `fs.browse` (RFC 0003 §5.2 "Files and filesystem").
//!
//! Every path a client sends is relative to a [`RootRef`] with `/`
//! separators and may not leave it; `fs.browse` alone takes absolute
//! daemon-host paths. Project roots are a convenience, not a security
//! boundary: `files:read` covers any path the daemon user can read.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::RootRef;

/// `files.searchEntries` returns at most this many entries.
pub const MAX_SEARCH_ENTRIES: u32 = 10_000;
/// `files.searchEntries` without a `limit`.
pub const DEFAULT_SEARCH_ENTRIES: u32 = 200;
/// `files.searchContents` returns at most this many hits.
pub const MAX_CONTENT_HITS: u32 = 1_000;
/// `files.searchContents` without a `limit`.
pub const DEFAULT_CONTENT_HITS: u32 = 200;
/// A content hit's `preview` is cut to this many characters.
pub const CONTENT_PREVIEW_CHARS: u32 = 200;

/// `files.tree` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FilesTreeParams {
    pub root: RootRef,
    /// Directories (relative to `root`) whose children are listed too.
    #[serde(default)]
    pub expanded: Vec<String>,
}

/// One row of the file tree, depth first.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TreeEntry {
    /// Relative to the root, `/`-separated.
    pub path: String,
    pub name: String,
    pub is_dir: bool,
    pub expanded: bool,
    pub depth: u32,
}

/// `files.searchEntries` params: the project's files and directories for
/// `@` mentions, fuzzy-ranked by `query` (all of them, shallow first, when it
/// is empty).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FilesSearchEntriesParams {
    pub root: RootRef,
    #[serde(default)]
    pub query: String,
    /// At most [`MAX_SEARCH_ENTRIES`]; [`DEFAULT_SEARCH_ENTRIES`] when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// A file or directory (with a trailing `/`) relative to the root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub path: String,
    pub is_dir: bool,
}

/// `files.searchContents` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FilesSearchContentsParams {
    pub root: RootRef,
    pub query: String,
    /// `query` is a regular expression (Rust `regex` syntax) instead of a
    /// literal.
    #[serde(default)]
    pub regex: bool,
    #[serde(default)]
    pub case_sensitive: bool,
    /// At most [`MAX_CONTENT_HITS`]; [`DEFAULT_CONTENT_HITS`] when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// One matching line.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ContentHit {
    pub path: String,
    /// 1-based.
    pub line: u32,
    /// 1-based, in characters (Unicode scalar values) of the line.
    pub column: u32,
    /// The line, cut to [`CONTENT_PREVIEW_CHARS`].
    pub preview: String,
}

/// `files.write` params. With `baseRevision`, the write is refused with
/// `conflict` (`revisionConflict`) unless the file still has that revision;
/// `""` means "the file must not exist yet".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FilesWriteParams {
    pub root: RootRef,
    pub path: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_revision: Option<String>,
    pub op_id: Uuid,
}

/// The revision a file has after `files.write`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FileRevision {
    pub revision: String,
}

/// `fs.browse` params: an absolute daemon-host directory, or the home
/// directory when absent.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FsBrowseParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// A directory on the daemon host, for project pickers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Directory {
    /// Canonical absolute path.
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub home: String,
    pub filesystem_root: String,
    pub entries: Vec<DirectoryEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DirectoryEntry {
    pub name: String,
    /// Absolute.
    pub path: String,
    pub is_dir: bool,
}
