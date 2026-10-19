//! Shared application identity used by the daemon and desktop client.
//!
//! This fork keeps the product name "Waku" but uses its own internal identity
//! (`waku-fork`) for bundle ids, data directories and the per-user home
//! directory, so it never reads or writes upstream Waku's files and both can
//! be installed side by side. Renaming the product later means changing this
//! module (plus packaging metadata), not hunting for paths.

use std::path::PathBuf;

#[cfg(debug_assertions)]
pub const APP_NAME: &str = "Waku Debug";
#[cfg(not(debug_assertions))]
pub const APP_NAME: &str = "Waku";

#[cfg(debug_assertions)]
pub const APP_ID: &str = "io.github.countradooku.waku.dev";
#[cfg(not(debug_assertions))]
pub const APP_ID: &str = "io.github.countradooku.waku";

/// Folder name under the platform data directory
/// (`~/Library/Application Support`, `%LOCALAPPDATA%`, `~/.local/share`).
#[cfg(debug_assertions)]
pub const DATA_DIRECTORY_NAME: &str = "Waku Fork Debug";
#[cfg(not(debug_assertions))]
pub const DATA_DIRECTORY_NAME: &str = "Waku Fork";

/// Folder name directly under the user's home directory that holds settings,
/// worktrees and projectless workspaces (upstream uses `.waku`).
pub const HOME_DIRECTORY_NAME: &str = ".waku-fork";

/// `~/.waku-fork`, or a temp-dir fallback when no home directory exists.
pub fn home_directory() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(HOME_DIRECTORY_NAME)
}
