//! Stream cursors (RFC 0003 §4).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Where a subscriber stands in a stream. A client passes its last *applied*
/// cursor as `subscribe.after`; the daemon decides between replay and a
/// snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum StreamCursor {
    /// The global event sequence of event store `store` (shell, thread).
    Sequence { store: Uuid, seq: u64 },
    /// A revision of a small document (settings, providers, access, drafts).
    /// Revisions are compared for equality only.
    Revision { revision: u64 },
    /// A byte offset into `generation` (terminal output, blob download).
    Offset { generation: Uuid, offset: u64 },
    #[serde(other)]
    Unknown,
}
