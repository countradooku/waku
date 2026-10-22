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

pub mod checkpoints;
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
pub mod provider;
pub mod recovery;
pub mod search;
pub mod store;
pub mod streams;

pub use clock::{Clock, ManualClock, SystemClock};
pub use command::{AnyCommand, CommandError, InternalCommand, LockKey};
pub use decide::policy::{
    ForkPointKind, ForkStrategy, RollbackStrategy, RuntimeModeDecision, SessionTransitionAction,
    SteeringPolicy, TransitionPlan,
};
pub use decide::{
    CancelUnsettled, CapabilitySource, DecideContext, Decision, IngestDecision, NoCapabilities,
    OrchestratorSettings, decide, decide_ingest,
};
pub use effects::executor::{
    CancelToken, CheckpointPort, ClaimedEffect, CleanupPort, DefaultExecutor, EffectContext,
    EffectError, EffectExecutor, PortError, Ports, PrepareRequest, TitlePort, WorkspacePort,
};
pub use effects::worker::{EffectWorkerPool, WorkerConfig};
pub use effects::{Effect, EffectType, Lane};
pub use feed::CommittedFeed;
pub use ids::{IdSource, SequentialIds, UuidV7Ids};
pub use orchestrator::{
    Dependencies, DispatchError, DispatchReceipt, IngestOutcome, Orchestrator, OrchestratorConfig,
};
pub use projection::{ApplyOutcome, ThreadProjection};
pub use provider::{
    ADAPTER_EVENT_CAPACITY, AdapterEvent, BackgroundWorkItem, IngestEnvelope, NativeRef,
    ProviderTurnRef, RunRef, ThreadDisposition, TurnOutcome, TurnTerminal,
};
pub use store::outbox::ReconcileReport;
pub use store::sink::{CommitContext, CommitResult, EventSink, WriteGuard};
pub use store::{SinkError, StoreError};

#[cfg(test)]
mod tests;
