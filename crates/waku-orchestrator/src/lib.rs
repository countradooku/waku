//! The event-sourced orchestrator core of the Waku daemon (RFC 0001).
//!
//! ```text
//! CommandEnvelope ─► receipt lookup ─► per-key lock ─► decide (pure)
//!   ─► EventSink::commit (one SQLite transaction: events, projections,
//!      receipt, cancellations, outbox) ─► apply to the cached projection
//!   ─► publish to CommittedFeed ─► wake EffectWorkerPool
//! ```
//!
//! The crate exposes blocking APIs and has no async runtime (R9). It depends
//! only on `waku-protocol` for the shared wire and disk types; `waku-core`
//! implements the effect ports and `waku-daemon` composes everything.

pub mod clock;
pub mod command;
pub mod decide;
pub mod effects;
pub mod fakes;
pub mod feed;
pub mod ids;
pub mod lifecycle;
pub mod orchestrator;
pub mod projection;
pub mod search;
pub mod store;

pub use clock::{Clock, ManualClock, SystemClock};
pub use command::{AnyCommand, CommandError, InternalCommand, LockKey};
pub use decide::{
    CancelUnsettled, CapabilitySource, DecideContext, Decision, NoCapabilities,
    OrchestratorSettings, decide,
};
pub use effects::executor::{
    CancelToken, ClaimedEffect, DefaultExecutor, EffectContext, EffectError, EffectExecutor, Ports,
};
pub use effects::worker::{EffectWorkerPool, WorkerConfig};
pub use effects::{Effect, EffectType, Lane};
pub use feed::CommittedFeed;
pub use ids::{IdSource, SequentialIds, UuidV7Ids};
pub use orchestrator::{
    Dependencies, DispatchError, DispatchReceipt, IngestOutcome, Orchestrator, OrchestratorConfig,
};
pub use projection::{ApplyOutcome, ThreadProjection};
pub use store::outbox::ReconcileReport;
pub use store::sink::{CommitContext, CommitResult, EventSink, WriteGuard};
pub use store::{SinkError, StoreError};

#[cfg(test)]
mod tests;
