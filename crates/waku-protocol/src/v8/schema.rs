//! The JSON Schema export of v8 (RFC 0003 §9), behind the `schema` feature.
//!
//! [`document`] is what `waku-codegen` writes to `schema/v8.json`: the
//! protocol manifest (constants, methods, streams, scopes, error codes) and a
//! JSON Schema 2020-12 subset of every wire type under `$defs`. The Rust
//! types are the source of truth; generated clients are built from this file
//! only.

use std::borrow::Cow;

use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde_json::{Map, Value, json};

use super::{
    CONSTANTS, End, Error, ErrorCode, GoodbyeCode, Hello, Notice, PREFACE, PROTOCOL_MAJOR,
    PROTOCOL_MINOR, Rejected, Scope, Subscribe, WEBSOCKET_PATH, WEBSOCKET_SUBPROTOCOL, Welcome,
};

/// A value whose type depends on context: a call's `params` (by `method`),
/// a reply's `ok` (by the call) and stream items (by the subscription). The
/// manifest gives the type per method and stream.
pub struct Dependent;

impl JsonSchema for Dependent {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        "Dependent".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({ "x-waku-dependent": true })
    }
}

// The wire shape of `ClientMessage`. The real type has hand-written serde
// impls (see `message.rs`); this mirror only describes it.
/// Client → daemon.
#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(rename = "ClientMessage")]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum ClientMessageShape {
    Hello(Hello),
    Call {
        id: u32,
        method: String,
        params: Dependent,
    },
    Cancel {
        id: u32,
    },
    Subscribe(Subscribe),
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
        code: GoodbyeCode,
        message: String,
    },
    #[serde(other)]
    Unknown,
}

// The wire shape of `ServerMessage`; see `ClientMessageShape`.
/// Daemon → client.
#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(rename = "ServerMessage")]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum ServerMessageShape {
    Welcome(Welcome),
    Rejected(Rejected),
    Reply {
        id: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ok: Option<Dependent>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<Error>,
    },
    Items {
        channel: u32,
        items: Vec<Dependent>,
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
    #[serde(other)]
    Unknown,
}

/// The whole `schema/v8.json` document.
pub fn document() -> Value {
    let mut generator = SchemaSettings::draft2020_12().into_generator();
    let client = generator.subschema_for::<ClientMessageShape>();
    let server = generator.subschema_for::<ServerMessageShape>();
    let methods = super::method_schemas(&mut generator)
        .into_iter()
        .map(|(info, params, result)| {
            json!({
                "name": info.name,
                "scope": info.scope.as_str(),
                "opId": info.op_id,
                "params": params,
                "result": result,
            })
        })
        .collect::<Vec<_>>();
    let streams = super::stream_schemas(&mut generator)
        .into_iter()
        .map(|(info, spec, item)| {
            json!({
                "name": info.name,
                "scope": info.scope.as_str(),
                "cursor": info.cursor.as_str(),
                "data": info.data,
                "spec": spec,
                "item": item,
            })
        })
        .collect::<Vec<_>>();
    let constants = CONSTANTS
        .iter()
        .map(|(name, value)| ((*name).to_owned(), json!(value)))
        .collect::<Map<_, _>>();
    let definitions = generator
        .definitions()
        .iter()
        .map(|(name, schema)| (name.clone(), schema.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "waku-protocol v8",
        "protocol": {
            "major": PROTOCOL_MAJOR,
            "minor": PROTOCOL_MINOR,
            "preface": PREFACE,
            "websocketPath": WEBSOCKET_PATH,
            "websocketSubprotocol": WEBSOCKET_SUBPROTOCOL,
        },
        "constants": constants,
        "scopes": Scope::KNOWN,
        "errorCodes": ErrorCode::KNOWN,
        "messages": {
            "client": client,
            "server": server,
        },
        "methods": methods,
        "streams": streams,
        // TODO(#54): the orchestration `Command` manifest (`commands!`).
        "commands": [],
        "$defs": definitions,
    })
}
