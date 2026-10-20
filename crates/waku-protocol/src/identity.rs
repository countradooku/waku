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

/// Prefix of per-user runtime names (sockets, pipes) that live outside the
/// data directory, so Debug and Release daemons never share one.
#[cfg(debug_assertions)]
pub const RUNTIME_NAME_PREFIX: &str = "waku-fork-dev";
#[cfg(not(debug_assertions))]
pub const RUNTIME_NAME_PREFIX: &str = "waku-fork";

/// Longest Unix socket path used as is. `sun_path` holds 104 bytes on macOS
/// (108 on Linux) including the terminator; longer paths fall back to a
/// short per-user directory under the temp dir.
#[cfg(unix)]
const MAX_SOCKET_PATH_BYTES: usize = 100;

/// The managed daemon's local endpoint (#19): a Unix socket path, or a named
/// pipe name on Windows. `WAKU_DAEMON_SOCKET` overrides it.
///
/// - macOS: `~/Library/Application Support/<data dir>/run/daemon.sock`
/// - Linux: `$XDG_RUNTIME_DIR/<prefix>/daemon.sock`, else
///   `~/.waku-fork/run/daemon.sock`
/// - Windows: `\\.\pipe\<prefix>-<user>-daemon`
///
/// The socket's directory is private to the user (0700); the daemon checks
/// that when it binds.
pub fn daemon_socket_path() -> PathBuf {
    if let Some(path) = std::env::var_os(crate::DAEMON_SOCKET_ENV).filter(|path| !path.is_empty()) {
        return PathBuf::from(path);
    }
    default_daemon_socket_path()
}

#[cfg(target_os = "macos")]
fn default_daemon_socket_path() -> PathBuf {
    let path = dirs::data_local_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(DATA_DIRECTORY_NAME)
        .join("run")
        .join("daemon.sock");
    short_enough(path)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn default_daemon_socket_path() -> PathBuf {
    let path = match dirs::runtime_dir() {
        Some(runtime) => runtime.join(RUNTIME_NAME_PREFIX).join("daemon.sock"),
        None => home_directory().join("run").join("daemon.sock"),
    };
    short_enough(path)
}

#[cfg(unix)]
fn short_enough(path: PathBuf) -> PathBuf {
    use std::os::unix::ffi::OsStrExt as _;

    if path.as_os_str().as_bytes().len() <= MAX_SOCKET_PATH_BYTES {
        return path;
    }
    // SAFETY: `geteuid` has no preconditions and cannot fail.
    let uid = unsafe { libc::geteuid() };
    std::env::temp_dir()
        .join(format!("{RUNTIME_NAME_PREFIX}-{uid}"))
        .join("daemon.sock")
}

#[cfg(windows)]
fn default_daemon_socket_path() -> PathBuf {
    // Pipe names are global to the machine, so the user is part of the name.
    // Only characters that are safe in a pipe name survive.
    let user = std::env::var("USERNAME")
        .unwrap_or_default()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
        .collect::<String>();
    PathBuf::from(format!(r"\\.\pipe\{RUNTIME_NAME_PREFIX}-{user}-daemon"))
}

#[cfg(not(any(unix, windows)))]
fn default_daemon_socket_path() -> PathBuf {
    home_directory().join("run").join("daemon.sock")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn socket_paths_fit_in_sun_path() {
        use std::os::unix::ffi::OsStrExt as _;

        let long = std::env::temp_dir()
            .join("x".repeat(MAX_SOCKET_PATH_BYTES))
            .join("daemon.sock");
        let path = short_enough(long);
        assert!(path.as_os_str().as_bytes().len() <= MAX_SOCKET_PATH_BYTES);
        assert!(path.ends_with("daemon.sock"));
        let short = PathBuf::from("/tmp/waku/daemon.sock");
        assert_eq!(short_enough(short.clone()), short);
    }
}
