#![recursion_limit = "256"]

//! Waku's daemon-side core.
//!
//! Provider, database, filesystem, and Git implementations live here, behind
//! the transport-neutral contract in `waku-protocol`. Client applications
//! intentionally depend on `waku-client` instead of this crate.

rust_i18n::i18n!("../../locales", fallback = "en");

macro_rules! tr {
    ($key:expr) => {
        crate::i18n::translate($key)
    };
    ($key:expr, $($args:tt)*) => {
        rust_i18n::t!($key, $($args)*).into_owned()
    };
}

/// Enter a `tracing` span at `$level` (`TRACE`, `DEBUG`, ...) for the rest of
/// the enclosing block. With the `profiling` feature off this expands to
/// nothing, so instrumented hot paths cost nothing in shipping builds.
macro_rules! profile_span {
    ($level:ident, $name:literal) => {
        #[cfg(feature = "profiling")]
        let _profile_span = tracing::span!(tracing::Level::$level, $name).entered();
    };
}

pub mod acp_session;
pub mod amp_session;
pub mod attachments;
pub mod blob_store;
pub mod checkpoint;
mod claude_metadata;
pub mod claude_session;
pub mod codex_session;
pub mod command_env;
pub mod composer_complete;
pub mod computer_use;
pub mod cursor_session;
pub mod daemon;
pub mod deepseek_pool;
pub mod deepseek_session;
pub mod driver;
mod frontmatter;
pub mod git_branch;
pub mod git_commit;
pub mod git_workflow;
pub mod grok_session;
mod http_wire;
pub mod i18n;
pub mod identity;
pub mod kimi_session;
pub mod model;
pub mod model_catalog;
pub mod opencode_api;
pub mod opencode_service;
pub mod opencode_session;
pub mod persistence;
pub mod pi_session;
pub mod projectless;
pub mod settings;
pub mod skills;
mod slash_command_catalog;
pub mod terminal;
pub mod theme;
pub mod usage;
pub mod usage_history;
pub mod workspace;
pub mod worktree;
pub mod worktree_cleanup;

mod fs_ext;
mod local_socket;
mod protocol;
mod server;
mod v8;

pub use local_socket::LocalListener;
pub use protocol::{
    APP_EXECUTABLE_ENV, ClientMessage, Command, DAEMON_ADDRESS_ENV, DAEMON_SOCKET_ENV,
    DAEMON_TOKEN_ENV, DaemonReady, PROTOCOL_VERSION, ReplayCursor, Request, ResponseOutcome,
    ResponsePayload, RpcError, SequencedEvent, ServerMessage, WireComputerToolRequest,
    WireDriverEvent, WireDriverStartOptions, WireSessionOptions,
};
pub use server::{
    Backend, EventSink, Listeners, ServerOptions, ShutdownSignal, serve, serve_listeners,
};
pub use settings::{DaemonSettings, DaemonSettingsStore};
pub use workspace::{WorkspaceOperation, WorkspaceResult};
