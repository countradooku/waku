//! What the daemon's `shell`, `thread` and `commandOutput` streams read
//! (#54, #56, #57; RFC 0003 §4, §5.4).
//!
//! - [`thread`]: the bounded thread snapshot, history pages, one full turn
//!   item, and the replay-or-snapshot decision on resume;
//! - [`shell`]: navigation rows, their snapshot, compact resume deltas and
//!   the per-subscription live filter, plus the archive pages;
//! - [`wire`]: the wire projection that keeps large bodies off streams;
//! - [`coalesce`]: the 50 ms coalescing of running tool updates;
//! - [`output_hub`]: live command output, which never becomes events;
//! - [`deltas`]: live text deltas of streaming items;
//! - [`cursor`]: opaque history cursors.
//!
//! Everything here is blocking and runtime-free like the rest of the crate
//! (R9): reads take a connection of the [`ReadPool`](crate::store::ReadPool),
//! the hubs take wakers. The daemon's transport (`waku-core::v8`) drives
//! them from its tasks and keeps the live budgets.

pub mod coalesce;
pub mod cursor;
pub mod deltas;
pub mod output_hub;
pub mod seed;
pub mod shell;
pub mod thread;
pub mod wire;

use std::sync::Arc;

pub use coalesce::ToolCoalescer;
pub use deltas::{DeltaBus, DeltaSubscription, LiveDelta};
pub use output_hub::{CommandOutputHub, OutputHubConfig, OutputSubscription, StoredOutput};

/// The live, non-event channels a provider session feeds and the daemon's
/// streams read: one per daemon.
#[derive(Clone, Default)]
pub struct LiveStreams {
    /// `AdapterEvent::CommandOutputDelta` goes here, never to `ingest`.
    pub output: Arc<CommandOutputHub>,
    /// The streaming coalescer's text deltas.
    pub deltas: Arc<DeltaBus>,
}
