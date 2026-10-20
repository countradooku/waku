//! The event-sourced orchestrator core of the Waku daemon (RFC 0001).
//!
//! This first slice holds the daemon-only commands, the pure `decide`
//! functions and the in-memory thread projection they read. The crate exposes
//! blocking APIs and has no async runtime (R9). It depends only on
//! `waku-protocol` for the shared wire and disk types.

pub mod clock;
pub mod command;
pub mod decide;
pub mod effects;
pub mod fakes;
pub mod ids;
pub mod projection;

pub use clock::{Clock, ManualClock, SystemClock};
pub use command::{AnyCommand, CommandError, InternalCommand, LockKey};
pub use decide::{
    CancelUnsettled, CapabilitySource, DecideContext, Decision, NoCapabilities,
    OrchestratorSettings, decide,
};
pub use effects::{Effect, EffectType, Lane};
pub use ids::{IdSource, SequentialIds, UuidV7Ids};
pub use projection::{ApplyOutcome, ThreadProjection};
