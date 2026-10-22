//! The handshake (RFC 0003 §2.1): `hello` → `welcome` or `rejected`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    MAX_BLOB_BYTES, MAX_DATA_CHUNK_BYTES, MAX_IN_FLIGHT_CALLS, MAX_MESSAGE_BYTES,
    MAX_SUBSCRIPTIONS, PING_INTERVAL_MS, PING_TIMEOUT_MS, PROTOCOL_MAJOR, PROTOCOL_MINOR, Scope,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    /// The version this build speaks.
    pub const CURRENT: Self = Self {
        major: PROTOCOL_MAJOR,
        minor: PROTOCOL_MINOR,
    };
}

/// The client's first message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Hello {
    pub protocol: ProtocolVersion,
    pub client: ClientInfo,
    pub credential: Credential,
    /// Compression the client accepts for the daemon → client direction.
    #[serde(default)]
    pub compression: Vec<Compression>,
    /// Client feature flags.
    #[serde(default)]
    pub features: Vec<String>,
    /// The client's receive cap; the daemon uses the smaller of its own and
    /// this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_bytes: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ClientInfo {
    pub kind: ClientKind,
    pub app_version: String,
    pub os: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_label: Option<String>,
    /// Stable per installation, so a reconnect is recognized.
    pub instance_id: Uuid,
}

open_string_enum! {
    pub enum ClientKind {
        Desktop = "desktop",
        Web = "web",
        Ios = "ios",
        Android = "android",
        Cli = "cli",
        Bench = "bench",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Credential {
    /// The daemon's local token (`run/local.token`).
    Local { token: String },
    /// A session token issued by pairing.
    Session { token: String },
    /// A one-time pairing secret; `welcome.issuedCredential` returns the
    /// session token.
    Pairing { secret: String, label: String },
    #[serde(other)]
    Unknown,
}

open_string_enum! {
    pub enum Compression {
        Zstd = "zstd",
    }
}

/// The daemon's answer to an accepted `hello`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Welcome {
    pub protocol: ProtocolVersion,
    pub daemon_version: String,
    pub environment: EnvironmentInfo,
    pub session: SessionInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issued_credential: Option<IssuedCredential>,
    pub limits: Limits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    /// Daemon feature flags.
    #[serde(default)]
    pub features: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct EnvironmentInfo {
    pub id: Uuid,
    pub name: String,
    pub host_os: String,
    pub host_arch: String,
    /// The event store's id (RFC 0001); cursors from another store are
    /// answered with a snapshot.
    pub store_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub id: Uuid,
    pub scopes: Vec<Scope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct IssuedCredential {
    pub token: String,
    pub expires_at: i64,
}

/// The limits in force for the rest of the connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Limits {
    pub max_message_bytes: u32,
    pub max_data_chunk_bytes: u32,
    pub max_blob_bytes: u64,
    pub ping_interval_ms: u32,
    pub ping_timeout_ms: u32,
    pub max_in_flight_calls: u32,
    pub max_subscriptions: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_message_bytes: MAX_MESSAGE_BYTES,
            max_data_chunk_bytes: MAX_DATA_CHUNK_BYTES,
            max_blob_bytes: MAX_BLOB_BYTES,
            ping_interval_ms: PING_INTERVAL_MS,
            ping_timeout_ms: PING_TIMEOUT_MS,
            max_in_flight_calls: MAX_IN_FLIGHT_CALLS,
            max_subscriptions: MAX_SUBSCRIPTIONS,
        }
    }
}

/// The daemon's answer to a refused `hello`. Everything the client pipelined
/// after `hello` is discarded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Rejected {
    pub code: RejectCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

open_string_enum! {
    pub enum RejectCode {
        VersionUnsupported = "versionUnsupported",
        Unauthenticated = "unauthenticated",
        Expired = "expired",
        Revoked = "revoked",
        RateLimited = "rateLimited",
        OriginNotAllowed = "originNotAllowed",
        Overloaded = "overloaded",
    }
}

impl Hello {
    /// The version gate: a daemon serves exactly one major.
    pub fn check_version(&self) -> Result<(), Rejected> {
        if self.protocol.major == PROTOCOL_MAJOR {
            return Ok(());
        }
        Err(Rejected {
            code: RejectCode::VersionUnsupported,
            message: format!(
                "protocol {}.{} is unsupported; this daemon speaks {PROTOCOL_MAJOR}",
                self.protocol.major, self.protocol.minor
            ),
            retry_after_ms: None,
        })
    }
}
