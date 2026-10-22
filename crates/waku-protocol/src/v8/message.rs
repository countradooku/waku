//! The frame catalogue (RFC 0003 §2.2): every message a client and the
//! daemon exchange.
//!
//! Both envelopes are open unions tagged by `type`. Two of their parts are
//! decoded by context rather than by tag: a `call`'s `params` by its
//! `method` key, and a `reply`'s `ok` / an `items` message's items by the
//! call or subscription they answer. The serde impls are therefore written
//! by hand, with a fast path that decodes straight into the typed value when
//! the keys arrive in the order every encoder here writes them (`type` first,
//! `method` before `params`, `id` before `ok`, `channel` before `items`).
//! Any other order still decodes, through a buffered replay.

use std::borrow::Cow;
use std::collections::BTreeMap;

use serde::de::value::MapAccessDeserializer;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::buffered::{Buffered, BufferedError};
use super::{
    Error, Hello, Method, MethodResult, Rejected, StreamCursor, StreamItem, StreamSpec, Welcome,
};

/// Client → daemon.
#[derive(Clone, Debug, PartialEq)]
pub enum ClientMessage {
    /// The first message of a connection.
    Hello(Hello),
    /// `{"type":"call","id":7,"method":"settings.get","params":{}}`.
    Call {
        id: u32,
        method: Method,
    },
    /// Best effort: the reply becomes `cancelled` if the cancel wins.
    Cancel {
        id: u32,
    },
    Subscribe(Subscribe),
    Unsubscribe {
        channel: u32,
    },
    /// Return DATA credit on `channel` (§2.4).
    Credit {
        channel: u32,
        bytes: u32,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    Goodbye {
        code: GoodbyeCode,
        message: String,
    },
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Subscribe {
    /// Chosen by the client, unique among its open subscriptions; `0` is
    /// reserved.
    pub channel: u32,
    pub stream: StreamSpec,
    /// The client's last applied cursor, to resume from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<StreamCursor>,
    /// Initial DATA credit in bytes, for streams that carry DATA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<u32>,
}

/// Daemon → client.
#[derive(Clone, Debug, PartialEq)]
pub enum ServerMessage {
    Welcome(Welcome),
    Rejected(Rejected),
    Reply(Reply),
    /// 1..n items of one subscription, in stream order.
    Items {
        channel: u32,
        items: Vec<StreamItem>,
    },
    End(End),
    Notice {
        notice: Notice,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    Goodbye {
        code: GoodbyeCode,
        message: String,
        reconnect: bool,
    },
    Unknown,
}

/// The answer to a call: exactly one of `ok` and `error` is present.
/// `ok` is `None` also when the reply's call id was not pending (its value is
/// skipped then).
#[derive(Clone, Debug, PartialEq)]
pub struct Reply {
    pub id: u32,
    pub ok: Option<MethodResult>,
    pub error: Option<Error>,
}

impl Reply {
    pub fn ok(id: u32, result: MethodResult) -> Self {
        Self {
            id,
            ok: Some(result),
            error: None,
        }
    }

    pub fn error(id: u32, error: Error) -> Self {
        Self {
            id,
            ok: None,
            error: Some(error),
        }
    }
}

/// The last message of a subscription. Its channel may be reused afterwards.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct End {
    pub channel: u32,
    pub reason: EndReason,
    /// The last cursor delivered, to resubscribe from (after `overflow`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_after: Option<StreamCursor>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum EndReason {
    Unsubscribed,
    Completed,
    /// The subscription's live budget overflowed; resubscribe from
    /// `resumeAfter`. Other streams on the connection are unaffected.
    Overflow,
    NotFound,
    Forbidden,
    Error {
        error: Error,
    },
    /// A newer subscription of the same client replaced this one.
    Superseded,
    ShuttingDown,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Notice {
    UpdateAvailable {
        version: String,
    },
    CredentialExpiring {
        at: i64,
    },
    #[serde(other)]
    Unknown,
}

open_string_enum! {
    /// Why a connection is closing.
    pub enum GoodbyeCode {
        ShuttingDown = "shuttingDown",
        Revoked = "revoked",
        Expired = "expired",
        ProtocolError = "protocolError",
        FrameTooLarge = "frameTooLarge",
        IdleTimeout = "idleTimeout",
        WriteStalled = "writeStalled",
        Overloaded = "overloaded",
    }
}

// ---------------------------------------------------------------------------
// Encoding: borrowed mirrors of the envelopes, derived, so the map length and
// the key order (`type` first) come from serde itself.

#[derive(Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum ClientRef<'a> {
    Hello(&'a Hello),
    Call {
        id: u32,
        method: &'a str,
        params: Params<'a>,
    },
    Cancel {
        id: u32,
    },
    Subscribe(&'a Subscribe),
    Unsubscribe {
        channel: u32,
    },
    Credit {
        channel: u32,
        bytes: u32,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    Goodbye {
        code: &'a GoodbyeCode,
        message: &'a str,
    },
    Unknown,
}

struct Params<'a>(&'a Method);

impl Serialize for Params<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize_params(serializer)
    }
}

impl Serialize for ClientMessage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Hello(hello) => ClientRef::Hello(hello),
            Self::Call { id, method } => ClientRef::Call {
                id: *id,
                method: method.name(),
                params: Params(method),
            },
            Self::Cancel { id } => ClientRef::Cancel { id: *id },
            Self::Subscribe(subscribe) => ClientRef::Subscribe(subscribe),
            Self::Unsubscribe { channel } => ClientRef::Unsubscribe { channel: *channel },
            Self::Credit { channel, bytes } => ClientRef::Credit {
                channel: *channel,
                bytes: *bytes,
            },
            Self::Ping { nonce } => ClientRef::Ping { nonce: *nonce },
            Self::Pong { nonce } => ClientRef::Pong { nonce: *nonce },
            Self::Goodbye { code, message } => ClientRef::Goodbye { code, message },
            Self::Unknown => ClientRef::Unknown,
        }
        .serialize(serializer)
    }
}

#[derive(Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum ServerRef<'a> {
    Welcome(&'a Welcome),
    Rejected(&'a Rejected),
    Reply {
        id: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        ok: Option<&'a MethodResult>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<&'a Error>,
    },
    Items {
        channel: u32,
        items: &'a [StreamItem],
    },
    End(&'a End),
    Notice {
        notice: &'a Notice,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    Goodbye {
        code: &'a GoodbyeCode,
        message: &'a str,
        reconnect: bool,
    },
    Unknown,
}

impl Serialize for ServerMessage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Welcome(welcome) => ServerRef::Welcome(welcome),
            Self::Rejected(rejected) => ServerRef::Rejected(rejected),
            Self::Reply(reply) => ServerRef::Reply {
                id: reply.id,
                ok: reply.ok.as_ref(),
                error: reply.error.as_ref(),
            },
            Self::Items { channel, items } => ServerRef::Items {
                channel: *channel,
                items,
            },
            Self::End(end) => ServerRef::End(end),
            Self::Notice { notice } => ServerRef::Notice { notice },
            Self::Ping { nonce } => ServerRef::Ping { nonce: *nonce },
            Self::Pong { nonce } => ServerRef::Pong { nonce: *nonce },
            Self::Goodbye {
                code,
                message,
                reconnect,
            } => ServerRef::Goodbye {
                code,
                message,
                reconnect: *reconnect,
            },
            Self::Unknown => ServerRef::Unknown,
        }
        .serialize(serializer)
    }
}

// ---------------------------------------------------------------------------
// Decoding.

/// A map key or tag, borrowed from the input when possible.
struct Key<'de>(Cow<'de, str>);

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;
        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = Key<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a string key")
            }
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Borrowed(value)))
            }
            fn visit_str<E>(self, value: &str) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(value.to_owned())))
            }
            fn visit_string<E>(self, value: String) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(value)))
            }
            fn visit_bytes<E>(self, value: &[u8]) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(String::from_utf8_lossy(value).into_owned())))
            }
            // Non-string keys and tags never match a name; they are kept as
            // text so they fall through to "unknown".
            fn visit_u64<E>(self, value: u64) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(value.to_string())))
            }
            fn visit_i64<E>(self, value: i64) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(value.to_string())))
            }
            fn visit_bool<E>(self, value: bool) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(value.to_string())))
            }
        }
        deserializer.deserialize_any(KeyVisitor)
    }
}

/// The rest of a map whose first key was not `type`, buffered.
fn buffer_rest<'de, A: MapAccess<'de>>(
    first: Key<'de>,
    mut map: A,
) -> Result<Vec<(Buffered, Buffered)>, A::Error> {
    let mut entries = vec![(
        Buffered::Str(first.0.into_owned()),
        map.next_value::<Buffered>()?,
    )];
    while let Some(entry) = map.next_entry::<Buffered, Buffered>()? {
        entries.push(entry);
    }
    Ok(entries)
}

/// Replay a buffered map through `visitor` with its `type` entry first.
fn replay<'de, V: Visitor<'de>>(
    mut entries: Vec<(Buffered, Buffered)>,
    visitor: V,
) -> Result<V::Value, BufferedError> {
    let index = entries
        .iter()
        .position(|(key, _)| key.as_str() == Some("type"))
        .ok_or_else(|| de::Error::missing_field("type"))?;
    let tag = entries.remove(index);
    entries.insert(0, tag);
    Buffered::Map(entries).deserialize_map(visitor)
}

fn skip_rest<'de, A: MapAccess<'de>>(mut map: A) -> Result<(), A::Error> {
    while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
    Ok(())
}

fn rest<'de, T: Deserialize<'de>, A: MapAccess<'de>>(map: A) -> Result<T, A::Error> {
    T::deserialize(MapAccessDeserializer::new(map))
}

#[derive(Deserialize)]
struct IdBody {
    id: u32,
}

#[derive(Deserialize)]
struct ChannelBody {
    channel: u32,
}

#[derive(Deserialize)]
struct CreditBody {
    channel: u32,
    bytes: u32,
}

#[derive(Deserialize)]
struct NonceBody {
    nonce: u64,
}

#[derive(Deserialize)]
struct GoodbyeBody {
    code: GoodbyeCode,
    #[serde(default)]
    message: String,
    #[serde(default)]
    reconnect: bool,
}

#[derive(Deserialize)]
struct NoticeBody {
    notice: Notice,
}

impl<'de> Deserialize<'de> for ClientMessage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(ClientVisitor)
    }
}

struct ClientVisitor;

impl<'de> Visitor<'de> for ClientVisitor {
    type Value = ClientMessage;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a v8 client message")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ClientMessage, A::Error> {
        let Some(first) = map.next_key::<Key>()? else {
            return Err(de::Error::missing_field("type"));
        };
        if first.0 != "type" {
            let entries = buffer_rest(first, map)?;
            return replay(entries, ClientVisitor).map_err(de::Error::custom);
        }
        let tag = map.next_value::<Key>()?;
        Ok(match &*tag.0 {
            "hello" => ClientMessage::Hello(rest(map)?),
            "call" => call_body(map)?,
            "cancel" => ClientMessage::Cancel {
                id: rest::<IdBody, _>(map)?.id,
            },
            "subscribe" => ClientMessage::Subscribe(rest(map)?),
            "unsubscribe" => ClientMessage::Unsubscribe {
                channel: rest::<ChannelBody, _>(map)?.channel,
            },
            "credit" => {
                let body: CreditBody = rest(map)?;
                ClientMessage::Credit {
                    channel: body.channel,
                    bytes: body.bytes,
                }
            }
            "ping" => ClientMessage::Ping {
                nonce: rest::<NonceBody, _>(map)?.nonce,
            },
            "pong" => ClientMessage::Pong {
                nonce: rest::<NonceBody, _>(map)?.nonce,
            },
            "goodbye" => {
                let body: GoodbyeBody = rest(map)?;
                ClientMessage::Goodbye {
                    code: body.code,
                    message: body.message,
                }
            }
            _ => {
                skip_rest(map)?;
                ClientMessage::Unknown
            }
        })
    }
}

struct ParamsSeed<'a>(&'a str);

impl<'de> DeserializeSeed<'de> for ParamsSeed<'_> {
    type Value = Method;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Method, D::Error> {
        Method::deserialize_params(self.0, deserializer)
    }
}

fn call_body<'de, A: MapAccess<'de>>(mut map: A) -> Result<ClientMessage, A::Error> {
    let mut id = None;
    let mut name: Option<String> = None;
    let mut method = None;
    let mut early_params: Option<Buffered> = None;
    while let Some(key) = map.next_key::<Key>()? {
        match &*key.0 {
            "id" => id = Some(map.next_value::<u32>()?),
            "method" => name = Some(map.next_value::<String>()?),
            "params" => match &name {
                Some(name) => method = Some(map.next_value_seed(ParamsSeed(name))?),
                None => early_params = Some(map.next_value()?),
            },
            _ => {
                map.next_value::<IgnoredAny>()?;
            }
        }
    }
    let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
    let name = name.ok_or_else(|| de::Error::missing_field("method"))?;
    let method = match method {
        Some(method) => method,
        // `params` before `method`, or no `params` at all (an empty object).
        None => Method::deserialize_params(
            &name,
            early_params.unwrap_or_else(|| Buffered::Map(Vec::new())),
        )
        .map_err(de::Error::custom)?,
    };
    Ok(ClientMessage::Call { id, method })
}

/// What a client knows about its own connection, needed to decode replies
/// and stream items.
pub trait DecodeContext {
    /// The method of the pending call `id`.
    fn call_method(&self, id: u32) -> Option<&'static str>;
    /// The stream subscribed on `channel`.
    fn channel_stream(&self, channel: u32) -> Option<&'static str>;
}

/// A [`DecodeContext`] held in two maps.
#[derive(Clone, Debug, Default)]
pub struct Routes {
    pub calls: BTreeMap<u32, &'static str>,
    pub channels: BTreeMap<u32, &'static str>,
}

impl DecodeContext for Routes {
    fn call_method(&self, id: u32) -> Option<&'static str> {
        self.calls.get(&id).copied()
    }

    fn channel_stream(&self, channel: u32) -> Option<&'static str> {
        self.channels.get(&channel).copied()
    }
}

/// Decodes a [`ServerMessage`] with the client's [`DecodeContext`].
pub struct ServerMessageSeed<'a, C: ?Sized>(pub &'a C);

impl<'de, C: DecodeContext + ?Sized> DeserializeSeed<'de> for ServerMessageSeed<'_, C> {
    type Value = ServerMessage;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<ServerMessage, D::Error> {
        deserializer.deserialize_map(ServerVisitor(self.0))
    }
}

struct ServerVisitor<'a, C: ?Sized>(&'a C);

impl<'de, C: DecodeContext + ?Sized> Visitor<'de> for ServerVisitor<'_, C> {
    type Value = ServerMessage;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a v8 daemon message")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ServerMessage, A::Error> {
        let context = self.0;
        let Some(first) = map.next_key::<Key>()? else {
            return Err(de::Error::missing_field("type"));
        };
        if first.0 != "type" {
            let entries = buffer_rest(first, map)?;
            return replay(entries, ServerVisitor(context)).map_err(de::Error::custom);
        }
        let tag = map.next_value::<Key>()?;
        Ok(match &*tag.0 {
            "welcome" => ServerMessage::Welcome(rest(map)?),
            "rejected" => ServerMessage::Rejected(rest(map)?),
            "reply" => reply_body(map, context)?,
            "items" => items_body(map, context)?,
            "end" => ServerMessage::End(rest(map)?),
            "notice" => ServerMessage::Notice {
                notice: rest::<NoticeBody, _>(map)?.notice,
            },
            "ping" => ServerMessage::Ping {
                nonce: rest::<NonceBody, _>(map)?.nonce,
            },
            "pong" => ServerMessage::Pong {
                nonce: rest::<NonceBody, _>(map)?.nonce,
            },
            "goodbye" => {
                let body: GoodbyeBody = rest(map)?;
                ServerMessage::Goodbye {
                    code: body.code,
                    message: body.message,
                    reconnect: body.reconnect,
                }
            }
            _ => {
                skip_rest(map)?;
                ServerMessage::Unknown
            }
        })
    }
}

/// A reply's `ok`, decoded as the result of `method`.
struct ResultSeed(Option<&'static str>);

impl<'de> DeserializeSeed<'de> for ResultSeed {
    type Value = Option<MethodResult>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        match self.0 {
            Some(method) => MethodResult::deserialize_for(method, deserializer),
            None => {
                IgnoredAny::deserialize(deserializer)?;
                Ok(None)
            }
        }
    }
}

fn reply_body<'de, A: MapAccess<'de>, C: DecodeContext + ?Sized>(
    mut map: A,
    context: &C,
) -> Result<ServerMessage, A::Error> {
    let mut id = None;
    let mut ok = None;
    let mut early_ok: Option<Buffered> = None;
    let mut error = None;
    while let Some(key) = map.next_key::<Key>()? {
        match &*key.0 {
            "id" => id = Some(map.next_value::<u32>()?),
            "ok" => match id {
                Some(id) => ok = map.next_value_seed(ResultSeed(context.call_method(id)))?,
                None => early_ok = Some(map.next_value()?),
            },
            "error" => error = map.next_value::<Option<Error>>()?,
            _ => {
                map.next_value::<IgnoredAny>()?;
            }
        }
    }
    let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
    if let Some(value) = early_ok {
        ok = ResultSeed(context.call_method(id))
            .deserialize(value)
            .map_err(de::Error::custom)?;
    }
    Ok(ServerMessage::Reply(Reply { id, ok, error }))
}

/// An `items` array, decoded as items of `stream`.
struct ItemsSeed(Option<&'static str>);

impl<'de> DeserializeSeed<'de> for ItemsSeed {
    type Value = Vec<StreamItem>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        match self.0 {
            Some(stream) => deserializer.deserialize_seq(ItemsVisitor(stream)),
            None => {
                IgnoredAny::deserialize(deserializer)?;
                Ok(Vec::new())
            }
        }
    }
}

struct ItemsVisitor(&'static str);

impl<'de> Visitor<'de> for ItemsVisitor {
    type Value = Vec<StreamItem>;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an array of stream items")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<StreamItem>, A::Error> {
        let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(1024));
        while let Some(item) = seq.next_element_seed(ItemSeed(self.0))? {
            items.extend(item);
        }
        Ok(items)
    }
}

struct ItemSeed(&'static str);

impl<'de> DeserializeSeed<'de> for ItemSeed {
    type Value = Option<StreamItem>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        StreamItem::deserialize_for(self.0, deserializer)
    }
}

fn items_body<'de, A: MapAccess<'de>, C: DecodeContext + ?Sized>(
    mut map: A,
    context: &C,
) -> Result<ServerMessage, A::Error> {
    let mut channel = None;
    let mut items = Vec::new();
    let mut early_items: Option<Buffered> = None;
    while let Some(key) = map.next_key::<Key>()? {
        match &*key.0 {
            "channel" => channel = Some(map.next_value::<u32>()?),
            "items" => match channel {
                Some(channel) => {
                    items = map.next_value_seed(ItemsSeed(context.channel_stream(channel)))?;
                }
                None => early_items = Some(map.next_value()?),
            },
            _ => {
                map.next_value::<IgnoredAny>()?;
            }
        }
    }
    let channel = channel.ok_or_else(|| de::Error::missing_field("channel"))?;
    if let Some(value) = early_items {
        items = ItemsSeed(context.channel_stream(channel))
            .deserialize(value)
            .map_err(de::Error::custom)?;
    }
    Ok(ServerMessage::Items { channel, items })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v8::codec::{Encoding, from_slice, from_slice_seed, to_vec};
    use crate::v8::{FilesReadParams, RootRef, SettingsItem};
    use serde_json::json;

    /// Encodes a JSON document in either encoding (test input only).
    fn bytes(encoding: Encoding, value: &serde_json::Value) -> Vec<u8> {
        to_vec(encoding, value).unwrap()
    }

    fn routes() -> Routes {
        Routes {
            calls: [(7, "settings.get"), (8, "files.read")].into(),
            channels: [(3, "settings")].into(),
        }
    }

    const ENCODINGS: [Encoding; 2] = [Encoding::MessagePack, Encoding::Json];

    #[test]
    fn calls_decode_in_any_key_order() {
        let expected = ClientMessage::Call {
            id: 4,
            method: Method::FilesRead(FilesReadParams {
                root: RootRef::Path {
                    path: "/tmp".into(),
                },
                path: "a.txt".into(),
                max_bytes: None,
            }),
        };
        let params = json!({"root": {"type": "path", "path": "/tmp"}, "path": "a.txt"});
        for encoding in ENCODINGS {
            for document in [
                json!({"type": "call", "id": 4, "method": "files.read", "params": params}),
                json!({"params": params, "method": "files.read", "id": 4, "type": "call"}),
                json!({"type": "call", "params": params, "id": 4, "method": "files.read"}),
            ] {
                let decoded: ClientMessage = from_slice(encoding, &bytes(encoding, &document))
                    .unwrap_or_else(|error| panic!("{document}: {error}"));
                assert_eq!(decoded, expected, "{encoding:?} {document}");
            }
            let encoded = to_vec(encoding, &expected).unwrap();
            assert_eq!(
                from_slice::<ClientMessage>(encoding, &encoded).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn unknown_methods_messages_and_fields_are_skipped() {
        for encoding in ENCODINGS {
            let call = json!({"type": "call", "id": 1, "method": "future.thing", "params": {"deep": [1, {"x": 2}]}});
            assert_eq!(
                from_slice::<ClientMessage>(encoding, &bytes(encoding, &call)).unwrap(),
                ClientMessage::Call {
                    id: 1,
                    method: Method::Unknown {
                        name: "future.thing".into()
                    }
                }
            );
            let unknown = json!({"type": "teleport", "where": {"x": 1}});
            assert_eq!(
                from_slice::<ClientMessage>(encoding, &bytes(encoding, &unknown)).unwrap(),
                ClientMessage::Unknown
            );
            let ping = json!({"type": "ping", "nonce": 5, "extra": [1, 2, 3]});
            assert_eq!(
                from_slice::<ClientMessage>(encoding, &bytes(encoding, &ping)).unwrap(),
                ClientMessage::Ping { nonce: 5 }
            );
            let unknown_server = json!({"type": "hologram", "a": 1});
            assert_eq!(
                from_slice_seed(
                    encoding,
                    &bytes(encoding, &unknown_server),
                    ServerMessageSeed(&routes())
                )
                .unwrap(),
                ServerMessage::Unknown
            );
        }
    }

    #[test]
    fn replies_and_items_decode_by_their_call_and_channel() {
        let routes = routes();
        for encoding in ENCODINGS {
            let reply = json!({"error": null, "ok": {"revision": 9, "settings": {}}, "id": 7, "type": "reply"});
            let ServerMessage::Reply(reply) = from_slice_seed(
                encoding,
                &bytes(encoding, &reply),
                ServerMessageSeed(&routes),
            )
            .unwrap() else {
                panic!("expected a reply");
            };
            assert_eq!(reply.id, 7);
            let Some(MethodResult::SettingsGet(snapshot)) = reply.ok else {
                panic!("expected a settings.get result");
            };
            assert_eq!(snapshot.revision, 9);

            // A reply to a call that is no longer pending keeps its id only.
            let stray = json!({"type": "reply", "id": 99, "ok": {"anything": true}});
            assert_eq!(
                from_slice_seed(
                    encoding,
                    &bytes(encoding, &stray),
                    ServerMessageSeed(&routes)
                )
                .unwrap(),
                ServerMessage::Reply(Reply {
                    id: 99,
                    ok: None,
                    error: None
                })
            );

            let items = json!({"type": "items", "items": [
                {"type": "synchronized", "cursor": {"type": "revision", "revision": 9}},
                {"type": "somethingNew", "payload": [1, 2]},
                {"type": "synchronized", "cursor": {"type": "wormhole"}}
            ], "channel": 3});
            assert_eq!(
                from_slice_seed(
                    encoding,
                    &bytes(encoding, &items),
                    ServerMessageSeed(&routes)
                )
                .unwrap(),
                ServerMessage::Items {
                    channel: 3,
                    items: vec![
                        StreamItem::Settings(SettingsItem::Synchronized {
                            cursor: StreamCursor::Revision { revision: 9 }
                        }),
                        StreamItem::Settings(SettingsItem::Unknown),
                        StreamItem::Settings(SettingsItem::Synchronized {
                            cursor: StreamCursor::Unknown
                        }),
                    ]
                }
            );
            let stray = json!({"type": "items", "channel": 77, "items": [{"type": "x"}]});
            assert_eq!(
                from_slice_seed(
                    encoding,
                    &bytes(encoding, &stray),
                    ServerMessageSeed(&routes)
                )
                .unwrap(),
                ServerMessage::Items {
                    channel: 77,
                    items: Vec::new()
                }
            );
        }
    }

    #[test]
    fn open_string_enums_keep_unknown_values() {
        for encoding in ENCODINGS {
            let goodbye =
                json!({"type": "goodbye", "code": "solarFlare", "message": "", "reconnect": true});
            assert_eq!(
                from_slice_seed(
                    encoding,
                    &bytes(encoding, &goodbye),
                    ServerMessageSeed(&routes())
                )
                .unwrap(),
                ServerMessage::Goodbye {
                    code: GoodbyeCode::Unknown("solarFlare".into()),
                    message: String::new(),
                    reconnect: true
                }
            );
            let end = json!({"type": "end", "channel": 3, "reason": {"type": "error", "error": {"code": "quantum", "message": "m", "details": {"type": "newDetail", "x": 1}}}});
            let ServerMessage::End(end) = from_slice_seed(
                encoding,
                &bytes(encoding, &end),
                ServerMessageSeed(&routes()),
            )
            .unwrap() else {
                panic!("expected end");
            };
            let EndReason::Error { error } = end.reason else {
                panic!("expected an error end");
            };
            assert_eq!(error.code, crate::v8::ErrorCode::Unknown("quantum".into()));
            assert_eq!(error.details, Some(crate::v8::ErrorDetails::Unknown));
        }
    }

    #[test]
    fn messages_without_a_type_are_errors() {
        for encoding in ENCODINGS {
            let document = json!({"id": 1});
            assert!(from_slice::<ClientMessage>(encoding, &bytes(encoding, &document)).is_err());
            assert!(from_slice::<ClientMessage>(encoding, &bytes(encoding, &json!([1]))).is_err());
        }
    }

    #[test]
    fn items_envelope_matches_the_encoded_message() {
        let items = vec![
            StreamItem::Settings(SettingsItem::Synchronized {
                cursor: StreamCursor::Revision { revision: 1 },
            }),
            StreamItem::Settings(SettingsItem::Unknown),
        ];
        for encoding in ENCODINGS {
            let message = ServerMessage::Items {
                channel: 300,
                items: items.clone(),
            };
            let encoded_items = items
                .iter()
                .map(|item| to_vec(encoding, item).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                crate::v8::codec::encode_items_envelope(encoding, 300, &encoded_items),
                to_vec(encoding, &message).unwrap(),
                "{encoding:?}"
            );
        }
    }
}
