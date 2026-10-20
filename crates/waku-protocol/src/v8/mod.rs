//! Protocol v8 (RFC 0003): Waku's own typed protocol.
//!
//! Native clients send MessagePack (`rmp-serde`, structs as maps with named
//! fields) and web clients JSON; both share one data model, so one set of
//! serde attributes, one schema (`schema/v8.json`) and one generated
//! TypeScript type set cover every client. Bulk bytes travel as raw DATA
//! frames, never inside messages.
//!
//! Layers, bottom up:
//! - [`frame`]: the 8-byte local preface and the 12-byte frame header
//!   (length, kind, flags, channel), with the size limits;
//! - [`codec`]: MessagePack and JSON encodings of a message body;
//! - [`message`]: the frame catalogue — `hello`/`welcome`, calls and
//!   replies, stream subscriptions and items, keepalive, `goodbye`;
//! - [`api`]: the methods and streams, declared with [`methods!`] and
//!   [`streams!`].
//!
//! v8 never shares an endpoint with the legacy (v7) protocol: it is served on
//! the WebSocket path [`WEBSOCKET_PATH`] with subprotocol
//! [`WEBSOCKET_SUBPROTOCOL`], and on the local socket behind [`PREFACE`].
//!
//! - [`orchestration`]: the envelopes of `orchestration.dispatch`, the
//!   `threads.*` reads and the `shell`, `thread` and `commandOutput` streams
//!   (#54, #56, #57), over the entities of [`crate::orchestration`].

#[macro_use]
mod macros;
mod buffered;

pub mod api;
pub mod codec;
pub mod cursor;
pub mod error;
pub mod frame;
pub mod handshake;
pub mod message;
pub mod orchestration;
#[cfg(feature = "schema")]
pub mod schema;
pub mod scope;

pub use api::*;
pub use codec::{CodecError, Encoding};
pub use cursor::StreamCursor;
pub use error::{Error, ErrorCode, ErrorDetails};
pub use frame::{Frame, FrameDecoder, FrameError, FrameHeader, FrameLimits};
pub use handshake::{
    ClientInfo, ClientKind, Compression, Credential, EnvironmentInfo, Hello, IssuedCredential,
    Limits, ProtocolVersion, RejectCode, Rejected, SessionInfo, Welcome,
};
pub use message::{
    ClientMessage, DecodeContext, End, EndReason, GoodbyeCode, Notice, Reply, ServerMessage,
    ServerMessageSeed, Subscribe,
};
pub use scope::Scope;

/// The protocol's major version. A daemon serves exactly one.
pub const PROTOCOL_MAJOR: u16 = 8;
/// Additive revisions within [`PROTOCOL_MAJOR`]. Behaviour is gated by
/// feature strings, not by comparing this.
pub const PROTOCOL_MINOR: u16 = 0;

/// The first 8 bytes in each direction of a local stream transport:
/// `"WAKU"`, a reserved byte, the major version, two reserved bytes.
pub const PREFACE: [u8; 8] = [
    0x57,
    0x41,
    0x4B,
    0x55,
    0x00,
    PROTOCOL_MAJOR as u8,
    0x00,
    0x00,
];
/// Length of the binary frame header.
pub const FRAME_HEADER_BYTES: usize = 12;

/// WebSocket path of the v8 endpoint.
pub const WEBSOCKET_PATH: &str = "/v8";
/// WebSocket subprotocol a v8 client must offer; the upgrade is refused with
/// HTTP 426 without it.
pub const WEBSOCKET_SUBPROTOCOL: &str = "waku.v8";

/// Cap on a MSG body before `welcome` (the `hello`).
pub const HANDSHAKE_MESSAGE_BYTES: u32 = 64 * 1024;
/// Cap on a MSG body after `welcome` (`maxMessageBytes`).
pub const MAX_MESSAGE_BYTES: u32 = 4 * 1024 * 1024;
/// Cap on a DATA body (`maxDataChunkBytes`).
pub const MAX_DATA_CHUNK_BYTES: u32 = 256 * 1024;
/// Largest blob a transfer may carry (`maxBlobBytes`).
pub const MAX_BLOB_BYTES: u64 = 64 * 1024 * 1024;
/// Each side pings after sending nothing for this long.
pub const PING_INTERVAL_MS: u32 = 5_000;
/// A connection that receives nothing at all for this long is dead.
pub const PING_TIMEOUT_MS: u32 = 15_000;
/// `hello` must arrive within this of the connection opening.
pub const HELLO_DEADLINE_MS: u32 = 10_000;
/// A socket write that makes no progress for this long closes the connection
/// (`writeStalled`).
pub const WRITE_STALL_MS: u32 = 10_000;
/// Calls in flight per connection; more get `busy`.
pub const MAX_IN_FLIGHT_CALLS: u32 = 64;
/// Open subscriptions per connection.
pub const MAX_SUBSCRIPTIONS: u32 = 256;
/// Live budget of one MSG item stream: queued items before `end{overflow}`.
pub const LIVE_BUDGET_ITEMS: u32 = 1_000;
/// Live budget of one MSG item stream: queued encoded bytes.
pub const LIVE_BUDGET_BYTES: u32 = 8 * 1024 * 1024;
/// Cap on everything queued for one connection's writer.
pub const CONNECTION_QUEUE_BYTES: u32 = 32 * 1024 * 1024;
/// Initial DATA credit of a subscription that does not set `window`.
pub const DEFAULT_DATA_WINDOW: u32 = 1024 * 1024;
/// Largest `u64` a JavaScript number holds exactly (2^53 − 1). Every integer
/// on the wire stays at or below it.
pub const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// Every constant the generated clients need, by wire name.
pub const CONSTANTS: &[(&str, u64)] = &[
    ("PROTOCOL_MAJOR", PROTOCOL_MAJOR as u64),
    ("PROTOCOL_MINOR", PROTOCOL_MINOR as u64),
    ("FRAME_HEADER_BYTES", FRAME_HEADER_BYTES as u64),
    ("HANDSHAKE_MESSAGE_BYTES", HANDSHAKE_MESSAGE_BYTES as u64),
    ("MAX_MESSAGE_BYTES", MAX_MESSAGE_BYTES as u64),
    ("MAX_DATA_CHUNK_BYTES", MAX_DATA_CHUNK_BYTES as u64),
    ("MAX_BLOB_BYTES", MAX_BLOB_BYTES),
    ("PING_INTERVAL_MS", PING_INTERVAL_MS as u64),
    ("PING_TIMEOUT_MS", PING_TIMEOUT_MS as u64),
    ("HELLO_DEADLINE_MS", HELLO_DEADLINE_MS as u64),
    ("WRITE_STALL_MS", WRITE_STALL_MS as u64),
    ("MAX_IN_FLIGHT_CALLS", MAX_IN_FLIGHT_CALLS as u64),
    ("MAX_SUBSCRIPTIONS", MAX_SUBSCRIPTIONS as u64),
    ("LIVE_BUDGET_ITEMS", LIVE_BUDGET_ITEMS as u64),
    ("LIVE_BUDGET_BYTES", LIVE_BUDGET_BYTES as u64),
    ("CONNECTION_QUEUE_BYTES", CONNECTION_QUEUE_BYTES as u64),
    ("DEFAULT_DATA_WINDOW", DEFAULT_DATA_WINDOW as u64),
    ("MAX_SAFE_INTEGER", MAX_SAFE_INTEGER),
    ("FRAME_KIND_MSG", frame::KIND_MSG as u64),
    ("FRAME_KIND_DATA", frame::KIND_DATA as u64),
    ("FRAME_FLAG_FIN", frame::FLAG_FIN as u64),
    ("FRAME_FLAG_ZSTD", frame::FLAG_ZSTD as u64),
    (
        "MAX_HISTORY_CURSOR_BYTES",
        orchestration::MAX_HISTORY_CURSOR_BYTES as u64,
    ),
    (
        "SNAPSHOT_USER_TURNS",
        orchestration::SNAPSHOT_USER_TURNS as u64,
    ),
    ("SNAPSHOT_ITEMS", orchestration::SNAPSHOT_ITEMS as u64),
    ("SNAPSHOT_BYTES", orchestration::SNAPSHOT_BYTES as u64),
    (
        "THREAD_REPLAY_EVENTS",
        orchestration::THREAD_REPLAY_EVENTS as u64,
    ),
    (
        "THREAD_REPLAY_BYTES",
        orchestration::THREAD_REPLAY_BYTES as u64,
    ),
    (
        "SHELL_REPLAY_EVENTS",
        orchestration::SHELL_REPLAY_EVENTS as u64,
    ),
    (
        "SHELL_SNAPSHOT_BYTES",
        orchestration::SHELL_SNAPSHOT_BYTES as u64,
    ),
    ("TOOL_COALESCE_MS", orchestration::TOOL_COALESCE_MS),
    (
        "TOOL_COALESCE_PENDING",
        orchestration::TOOL_COALESCE_PENDING as u64,
    ),
    ("OUTPUT_FRAME_MS", orchestration::OUTPUT_FRAME_MS),
    (
        "OUTPUT_OVERFLOW_FRAME_MS",
        orchestration::OUTPUT_OVERFLOW_FRAME_MS,
    ),
    (
        "OUTPUT_APPEND_BYTES",
        orchestration::OUTPUT_APPEND_BYTES as u64,
    ),
    ("OUTPUT_LIVE_TAILS", orchestration::OUTPUT_LIVE_TAILS as u64),
    (
        "OUTPUT_FINISHED_TAILS",
        orchestration::OUTPUT_FINISHED_TAILS as u64,
    ),
    ("OUTPUT_HEAD_BYTES", orchestration::OUTPUT_HEAD_BYTES as u64),
    ("OUTPUT_TAIL_BYTES", orchestration::OUTPUT_TAIL_BYTES as u64),
    (
        "WIRE_SUMMARY_CHARS",
        orchestration::WIRE_SUMMARY_CHARS as u64,
    ),
    ("WIRE_VALUE_BYTES", orchestration::WIRE_VALUE_BYTES as u64),
    ("MAX_ARCHIVED_PAGE", orchestration::MAX_ARCHIVED_PAGE as u64),
];
