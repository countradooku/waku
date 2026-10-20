//! Deterministic benchmark fixtures for Waku.
//!
//! * [`server::BenchServer`] is a `waku-daemon` stand-in: the real server and
//!   persistence backend with the provider runtime replaced by timeline
//!   replay (issue #7).
//! * [`autoplay`] replays a timeline into a desktop once it attaches, so
//!   benchmark runs need no UI input (issue #9).
//! * [`native`] translates the same timelines into provider-native streams for
//!   the `fake-codex` and `fake-claude` binaries (issue #8).
//!
//! See `README.md` in this crate for usage and for what is faked.

pub mod autoplay;
pub mod backend;
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
