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
pub mod binary_frame;
pub mod blob;
pub mod checkpoint;
pub mod composer;
pub mod computer_use;
mod driver_wire;
pub mod git;
pub mod i18n;
pub mod identity;
pub mod model;
pub mod model_catalog;
pub mod persistence;
pub mod projectless;
pub mod provider_session;
pub mod settings;
pub mod shared;
pub mod skills;
pub mod theme;
pub mod usage;
pub mod usage_history;
/// Protocol v8 (RFC 0003), served beside the legacy protocol on its own
/// endpoints until the clients move over (#53).
pub mod v8;
pub mod wire_event;
pub mod workspace;

mod protocol;

/// The `TS` derive on wire types. With the `ts` feature (only
/// `export_types`, through `bun run protocol:generate` / `protocol:check`)
/// it is ts-rs; otherwise a stub that accepts the same `#[ts(...)]`
/// attributes and generates nothing, so the app and daemon never build
/// ts-rs (#30).
mod ts {
    #[cfg(feature = "ts")]
    pub use ts_rs::TS;
    #[cfg(not(feature = "ts"))]
    pub use waku_ts_stub::TS;
}

pub use driver_wire::{decode_enum, encode_enum, event_from_wire, event_to_wire};
pub use protocol::{
    APP_EXECUTABLE_ENV, ClientMessage, Command, DAEMON_ADDRESS_ENV, DAEMON_SOCKET_ENV,
    DAEMON_TOKEN_ENV, DaemonReady, KEEPALIVE_INTERVAL, KEEPALIVE_TIMEOUT, MAX_WIRE_MESSAGE_BYTES,
    PROTOCOL_VERSION, ReplayCursor, Request, ResponseOutcome, ResponsePayload, RpcError,
    SequencedEvent, ServerMessage, WireComputerToolRequest, WireDriverStartOptions,
    WireSessionOptions,
};
/// The legacy (v7) protocol version, under its RFC 0003 name. v8 is
/// [`v8::PROTOCOL_MAJOR`]; the two never share an endpoint.
pub const LEGACY_PROTOCOL_VERSION: u32 = PROTOCOL_VERSION;
pub use settings::DaemonSettings;
pub use wire_event::WireDriverEvent;
pub use workspace::{WorkspaceOperation, WorkspaceResult};
