#![recursion_limit = "256"]

//! Waku's shared, versioned wire contract.
//!
//! This crate contains serializable data only. It performs no provider,
//! database, workspace, Git, attachment, or transport I/O, so native and web
//! clients can depend on it without pulling in the daemon implementation.

rust_i18n::i18n!("../../locales", fallback = "en");

// `i18n!` reads these files in a proc macro. Explicit includes make them
// visible to Cargo's dependency tracker, so locale-only edits rebuild this
// shared translation registry under the development watcher.
const _LOCALE_SOURCES: [&str; 3] = [
    include_str!("../../../locales/app.yml"),
    include_str!("../../../locales/zh-CN.yml"),
    include_str!("../../../locales/ja.yml"),
];

macro_rules! tr {
    ($key:expr) => {
        crate::i18n::translate($key)
    };
    ($key:expr, $($args:tt)*) => {
        rust_i18n::t!($key, $($args)*).into_owned()
    };
}

pub mod attachments;
pub mod blob;
pub mod checkpoint;
pub mod composer;
pub mod computer_use;
pub mod daemon;
pub mod git;
pub mod i18n;
pub mod identity;
pub mod model;
pub mod model_catalog;
pub mod orchestration;
pub mod persistence;
pub mod projectless;
pub mod settings;
pub mod shared;
pub mod skills;
pub mod storage;
pub mod theme;
pub mod usage;
pub mod usage_history;
/// Protocol v8 (RFC 0003), the daemon's only protocol (decision D6).
pub mod v8;
pub mod workspace;

pub use daemon::{
    APP_EXECUTABLE_ENV, DAEMON_ADDRESS_ENV, DAEMON_SOCKET_ENV, DAEMON_TOKEN_ENV, DaemonReady,
    TERMINAL_REPLAY_BYTES, TERMINAL_RETAINED_BYTES, TERMINAL_RETAINED_LINES,
};
pub use settings::DaemonSettings;
pub use workspace::{WorkspaceOperation, WorkspaceResult};
