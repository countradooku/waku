//! The daemon process contract outside protocol v8's frames: the
//! environment a client hands a daemon it launches, the readiness line the
//! daemon prints on stdout, and the PTY host's retention bounds.

use serde::{Deserialize, Serialize};

pub const DAEMON_TOKEN_ENV: &str = "WAKU_DAEMON_TOKEN";
pub const DAEMON_ADDRESS_ENV: &str = "WAKU_DAEMON_ADDRESS";
/// Overrides the managed daemon's local socket path (Unix) or pipe name
/// (Windows), for tests and side-by-side daemons. See
/// [`crate::identity::daemon_socket_path`].
pub const DAEMON_SOCKET_ENV: &str = "WAKU_DAEMON_SOCKET";
pub const APP_EXECUTABLE_ENV: &str = "WAKU_APP_EXECUTABLE";
/// Lines of output a daemon terminal retains for replay (RFC 0003 §5.4).
pub const TERMINAL_RETAINED_LINES: usize = 5_000;
/// Bytes of output a daemon terminal retains for replay (RFC 0003 §5.4).
pub const TERMINAL_RETAINED_BYTES: usize = 8 * 1024 * 1024;
/// The most retained output one terminal attach replays, and the default
/// when the client names no smaller bound. It matches the v8 terminal
/// stream's initial credit (RFC 0003 §12).
pub const TERMINAL_REPLAY_BYTES: usize = 1024 * 1024;

/// The one JSON line a daemon prints on stdout once it accepts connections.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonReady {
    /// The TCP listener's address, or empty when the daemon listens only on
    /// its local socket (the managed daemon with exposure off).
    #[serde(default)]
    pub address: String,
    /// The per-user Unix socket path or Windows pipe name, when the daemon
    /// listens on one. Clients on the same machine prefer it over TCP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket: Option<String>,
    /// [`crate::v8::PROTOCOL_MAJOR`] of the daemon.
    pub protocol_version: u32,
    pub pid: u32,
}
