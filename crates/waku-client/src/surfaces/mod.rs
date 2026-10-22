//! The desktop's non-session daemon surfaces over protocol v8 (#53, #55).
//!
//! Each module wraps one area of the RFC 0003 §5.2 catalog in blocking
//! helpers over the shared [`DaemonV8`] connection: they build the request,
//! make the call, and turn the reply into what the desktop renders. Callers
//! run them on the background executor or a worker thread, never from a
//! frame. The terminal link lives in [`crate::terminal`].
//!
//! Workspaces are still addressed by daemon-host path (`RootRef::Path`) and
//! drafts by the ids the session layer hands out; the session layer's move
//! onto orchestration ids is the seam where these become thread and project
//! roots.

pub mod blobs;
pub mod composer;
mod connection;
pub mod drafts;
pub mod files;
pub mod providers;
pub mod settings;
pub mod vcs;

pub use connection::{DaemonV8, Endpoint, call_error, remote_code};
