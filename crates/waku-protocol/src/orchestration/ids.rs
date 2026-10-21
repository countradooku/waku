//! App ids, command ids and the scalar aliases of RFC 0001.

use std::fmt;

use serde::{Deserialize, Serialize};

/// `events.sequence`; 0 means "before the first event".
pub type Sequence = u64;
/// Unix epoch milliseconds, UTC (R7).
pub type Timestamp = i64;
/// `effect_outbox.effect_id`.
pub type EffectId = i64;

uuid_ids!(
    ProjectId,
    ThreadId,
    RunId,
    RunAttemptId,
    /// Also a `Subagent`'s id, as in T3.
    NodeId,
    ProviderSessionId,
    ProviderThreadId,
    ProviderTurnId,
    RuntimeRequestId,
    MessageId,
    PlanId,
    TurnItemId,
    CheckpointScopeId,
    CheckpointId,
    ContextTransferId,
    ContextHandoffId,
    EventId,
);

/// The idempotency key of a dispatched command: 1–256 bytes of
/// `[A-Za-z0-9:._-]`, checked on decode. Ids derived from stable inputs
/// (`limit-resume:<thread>:<run>:<reset>`, `effect:<id>:<n>`) stay readable.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CommandId(String);

/// Why a string is not a valid [`CommandId`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandIdError {
    Empty,
    TooLong(usize),
    InvalidCharacter(char),
}

impl fmt::Display for CommandIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("command id is empty"),
            Self::TooLong(length) => {
                write!(formatter, "command id is {length} bytes; the limit is 256")
            }
            Self::InvalidCharacter(character) => {
                write!(formatter, "command id contains {character:?}")
            }
        }
    }
}

impl std::error::Error for CommandIdError {}

impl CommandId {
    pub const MAX_BYTES: usize = 256;

    pub fn new(value: impl Into<String>) -> Result<Self, CommandIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CommandIdError::Empty);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(CommandIdError::TooLong(value.len()));
        }
        if let Some(character) = value
            .chars()
            .find(|character| !(character.is_ascii_alphanumeric() || ":._-".contains(*character)))
        {
            return Err(CommandIdError::InvalidCharacter(character));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for CommandId {
    type Error = CommandIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CommandId> for String {
    fn from(id: CommandId) -> Self {
        id.0
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for CommandId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CommandId".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "1–256 bytes of `[A-Za-z0-9:._-]`; retries reuse it.",
            "type": "string",
            "minLength": 1,
            "maxLength": Self::MAX_BYTES,
            "pattern": "^[A-Za-z0-9:._-]+$",
        })
    }
}

impl fmt::Display for CommandId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A configured provider instance. Phase 2: the driver tag (`"codex"`);
/// Phase 3: `"codex:<name>"`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct ProviderInstanceId(pub String);

impl ProviderInstanceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The driver tag before an optional `:<name>` suffix.
    pub fn driver_tag(&self) -> &str {
        self.0.split(':').next().unwrap_or_default()
    }
}

impl fmt::Display for ProviderInstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A fractional index (base-62), compared bytewise.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct OrderKey(pub String);

impl OrderKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque JSON text, never parsed by the protocol layer (RFC 0003 §5.1).
/// Replaces `serde_json::Value` wherever provider-native JSON is carried.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct JsonText(pub String);

impl JsonText {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
