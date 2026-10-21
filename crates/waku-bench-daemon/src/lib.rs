//! Deterministic benchmark fixtures for Waku.
//!
//! * [`server::BenchServer`] is `waku-daemon` as its `main` composes it
//!   (orchestrator, provider session manager, protocol v8), with the Codex
//!   and Claude CLIs replaced by the fakes and the data directory seeded
//!   with [`fixture`] threads (issue #7).
//! * [`autoplay`] prompts a timeline into a desktop once it subscribes to
//!   the thread, so benchmark runs need no UI input (issue #9).
//! * [`native`] translates the timelines into provider-native streams for
//!   the `fake-codex` and `fake-claude` binaries (issue #8).
//!
//! See `README.md` in this crate for usage and for what is faked.

pub mod autoplay;
pub mod content;
pub mod fixture;
pub mod generate;
pub mod native;
pub mod png;
pub mod replay;
pub mod rng;
pub mod server;
pub mod timeline;

pub use replay::{Rate, ReplayStats};
pub use timeline::{TimedEvent, Timeline};
