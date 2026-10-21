//! Rust transport and lifecycle support for clients of `waku-daemon`.
//!
//! This crate intentionally depends only on [`waku_protocol`], so GUI and CLI
//! clients cannot accidentally reach daemon-owned filesystem, Git, database,
//! or provider implementations.

mod client;
pub mod command_env;
/// The orchestration commands the desktop dispatches.
pub mod commands;
pub mod composer_complete;
pub mod computer_use;
pub mod persistence;
mod process;
/// Client-side reducers for the v8 shell, thread and command-output streams.
pub mod projection;
/// The client-side projection store the desktop renders from (#55).
pub mod store;
pub mod surfaces;
pub mod terminal;
/// Render views of the v8 projections.
pub mod thread_view;
/// Protocol v8 (RFC 0003); shadows the protocol crate's `v8` module and
/// re-exports its types.
pub mod v8;
mod workspace_client;

pub use client::{DaemonClient, local_endpoint_address};
pub use process::{
    DEFAULT_EXPOSED_DAEMON_PORT, DaemonExposureSettings, DaemonProcess, DaemonSupervisor,
    parse_allowed_origins,
};
pub use v8::V8Client;
pub use waku_protocol::*;
pub use workspace_client::WorkspaceClient;

pub mod git_branch {
    pub use crate::v8::{BranchEntry, BranchSnapshot};

    /// The branch a snapshot shows: the current one, else the detached head.
    pub trait DisplayBranch {
        fn display_branch(&self) -> Option<&str>;
    }

    impl DisplayBranch for BranchSnapshot {
        fn display_branch(&self) -> Option<&str> {
            crate::surfaces::vcs::display_branch(self)
        }
    }
}

pub mod git_commit {
    pub use crate::v8::CommitSnapshot as Snapshot;
}

pub mod worktree {
    pub use waku_protocol::git::CreatedWorktree;
}
