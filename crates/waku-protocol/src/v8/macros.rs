//! Declaration macros for the v8 wire contract (RFC 0003 §9).
//!
//! - [`open_string_enum!`] declares a string enum that keeps unknown values
//!   (`Unknown(String)`), for error codes, scopes and other vocabularies that
//!   grow without a major bump.
//! - [`methods!`] declares the call methods: the `Method` and
//!   `MethodResult` enums, a marker type per method implementing
//!   [`MethodDef`](super::api::MethodDef), the static scope table and the
//!   manifest the schema exporter writes.
//! - [`streams!`] does the same for subscriptions: `StreamSpec`,
//!   `StreamItem` and [`StreamDef`](super::api::StreamDef).
//! - [`commands!`] declares an open union of commands with a scope per
//!   command. `orchestration.dispatch` carries RFC 0001's own `Command`
//!   instead (one definition for daemon and clients, R17), so the macro is
//!   kept for later command sets.
//!
//! Each macro also emits, behind the `schema` feature, the function that
//! registers its types with the JSON Schema generator, so the manifest in
//! `schema/v8.json` cannot drift from the declarations.

/// A string enum whose unknown values decode to `Unknown(raw)` instead of
/// failing (RFC 0003 §1.1). Serialized as the bare string.
macro_rules! open_string_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident = $value:literal, )*
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis enum $name {
            $( $(#[$variant_meta])* $variant, )*
            /// A value this build does not know, kept verbatim.
            Unknown(String),
        }

        impl $name {
            /// Every value this build knows, in declaration order.
            pub const KNOWN: &'static [&'static str] = &[$($value),*];

            pub fn as_str(&self) -> &str {
                match self {
                    $( Self::$variant => $value, )*
                    Self::Unknown(raw) => raw,
                }
            }

            pub fn parse(raw: &str) -> Self {
                match raw {
                    $( $value => Self::$variant, )*
                    other => Self::Unknown(other.to_owned()),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct Visitor;
                impl serde::de::Visitor<'_> for Visitor {
                    type Value = $name;
                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str(concat!("a ", stringify!($name), " string"))
                    }
                    fn visit_str<E: serde::de::Error>(self, raw: &str) -> Result<$name, E> {
                        Ok($name::parse(raw))
                    }
                    fn visit_bytes<E: serde::de::Error>(self, raw: &[u8]) -> Result<$name, E> {
                        std::str::from_utf8(raw)
                            .map($name::parse)
                            .map_err(|_| E::invalid_value(serde::de::Unexpected::Bytes(raw), &self))
                    }
                }
                deserializer.deserialize_str(Visitor)
            }
        }

        #[cfg(feature = "schema")]
        impl schemars::JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                stringify!($name).into()
            }

            fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({
                    "type": "string",
                    "enum": Self::KNOWN,
                    "x-waku-open": true,
                })
            }
        }
    };
}

/// Declares the call methods. Each line is
/// `"domain.verb" => Variant(Params) -> Result, scope = Scope[, op_id];`.
macro_rules! methods {
    (
        $(
            $(#[$meta:meta])*
            $name:literal => $variant:ident($params:ty) -> $result:ty,
            scope = $scope:ident $(, $flag:ident)? ;
        )*
    ) => {
        /// A call's method with its parameters. On the wire it is two keys
        /// of the `call` message: `method` (the name) and `params`.
        #[derive(Clone, Debug, PartialEq)]
        pub enum Method {
            $( $(#[$meta])* $variant($params), )*
            /// A method this build does not know; its params were skipped.
            /// The daemon answers it with `unknownMethod`.
            Unknown { name: String },
        }

        /// A reply's `ok` value: the method's result, as is.
        #[derive(Clone, Debug, PartialEq)]
        pub enum MethodResult {
            $( $variant($result), )*
        }

        /// One marker type per method, for typed calls:
        /// `client.call::<method::SettingsGet>(params)`.
        pub mod method {
            #[allow(unused_imports)]
            use super::*;
            $(
                $(#[$meta])*
                #[derive(Clone, Copy, Debug)]
                pub struct $variant;

                impl $crate::v8::api::MethodDef for $variant {
                    const NAME: &'static str = $name;
                    const SCOPE: $crate::v8::Scope = $crate::v8::Scope::$scope;
                    const OP_ID: bool = methods!(@op_id $($flag)?);
                    type Params = $params;
                    type Result = $result;

                    fn wrap(params: $params) -> super::Method {
                        super::Method::$variant(params)
                    }

                    fn unwrap(result: super::MethodResult) -> Option<$result> {
                        #[allow(unreachable_patterns)]
                        match result {
                            super::MethodResult::$variant(result) => Some(result),
                            _ => None,
                        }
                    }
                }
            )*
        }

        impl Method {
            /// The wire name, or the raw name of an unknown method.
            pub fn name(&self) -> &str {
                match self {
                    $( Self::$variant(_) => $name, )*
                    Self::Unknown { name } => name,
                }
            }

            /// The wire name of a known method.
            pub fn static_name(&self) -> Option<&'static str> {
                match self {
                    $( Self::$variant(_) => Some($name), )*
                    Self::Unknown { .. } => None,
                }
            }

            /// The idempotency key of a mutating method (RFC 0003 §2.3).
            pub fn op_id(&self) -> Option<uuid::Uuid> {
                match self {
                    $( Self::$variant(params) => methods!(@op_id_of params $($flag)?), )*
                    Self::Unknown { .. } => None,
                }
            }

            /// The scope the method requires, before payload-dependent
            /// additions.
            pub fn scope(&self) -> Option<$crate::v8::Scope> {
                match self {
                    $( Self::$variant(_) => Some($crate::v8::Scope::$scope), )*
                    Self::Unknown { .. } => None,
                }
            }

            /// Write the `params` value.
            pub fn serialize_params<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::Serialize as _;
                match self {
                    $( Self::$variant(params) => params.serialize(serializer), )*
                    Self::Unknown { .. } => {
                        use serde::ser::SerializeMap as _;
                        serializer.serialize_map(Some(0))?.end()
                    }
                }
            }

            /// Read `params` for the method called `name`. Unknown names
            /// skip the value and yield [`Method::Unknown`].
            pub fn deserialize_params<'de, D: serde::Deserializer<'de>>(
                name: &str,
                deserializer: D,
            ) -> Result<Self, D::Error> {
                use serde::Deserialize as _;
                match name {
                    $( $name => <$params>::deserialize(deserializer).map(Self::$variant), )*
                    _ => {
                        serde::de::IgnoredAny::deserialize(deserializer)?;
                        Ok(Self::Unknown { name: name.to_owned() })
                    }
                }
            }
        }

        impl MethodResult {
            pub fn method_name(&self) -> &'static str {
                match self {
                    $( Self::$variant(_) => $name, )*
                }
            }

            /// Read a reply's `ok` for a call of `method`. `None` for a method
            /// this build does not know, whose value is skipped.
            pub fn deserialize_for<'de, D: serde::Deserializer<'de>>(
                method: &str,
                deserializer: D,
            ) -> Result<Option<Self>, D::Error> {
                use serde::Deserialize as _;
                match method {
                    $( $name => <$result>::deserialize(deserializer).map(|result| Some(Self::$variant(result))), )*
                    _ => {
                        serde::de::IgnoredAny::deserialize(deserializer)?;
                        Ok(None)
                    }
                }
            }
        }

        impl serde::Serialize for MethodResult {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match self {
                    $( Self::$variant(result) => result.serialize(serializer), )*
                }
            }
        }

        /// The method manifest, in declaration order.
        pub fn methods() -> Vec<$crate::v8::api::MethodInfo> {
            vec![$(
                $crate::v8::api::MethodInfo {
                    name: $name,
                    scope: $crate::v8::Scope::$scope,
                    op_id: methods!(@op_id $($flag)?),
                },
            )*]
        }

        /// Registers every method's params and result type; returns the
        /// manifest entries with their schemas.
        #[cfg(feature = "schema")]
        pub fn method_schemas(
            generator: &mut schemars::SchemaGenerator,
        ) -> Vec<($crate::v8::api::MethodInfo, schemars::Schema, schemars::Schema)> {
            methods()
                .into_iter()
                .zip([$(
                    (generator.subschema_for::<$params>(), generator.subschema_for::<$result>()),
                )*])
                .map(|(info, (params, result))| (info, params, result))
                .collect()
        }
    };
    (@op_id op_id) => { true };
    (@op_id) => { false };
    (@op_id_of $params:ident op_id) => { Some($crate::v8::api::OpIdParams::op_id($params)) };
    (@op_id_of $params:ident) => {{
        let _ = $params;
        None
    }};
}

/// Declares the streams. Each line is
/// `"name" => Variant(Spec) -> Item, cursor = Kind, scope = Scope[, data];`.
/// `data` marks streams that also carry DATA frames under credit.
macro_rules! streams {
    (
        $(
            $(#[$meta:meta])*
            $name:literal => $variant:ident($spec:ty) -> $item:ty,
            cursor = $cursor:ident, scope = $scope:ident $(, $flag:ident)? ;
        )*
    ) => {
        /// What a `subscribe` asks for: an open union tagged by stream name.
        #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
        #[serde(tag = "type")]
        pub enum StreamSpec {
            $( $(#[$meta])* #[serde(rename = $name)] $variant($spec), )*
            /// A stream this build does not know; answered with
            /// `end{error: unsupported}`.
            #[serde(other, rename = "unknown")]
            Unknown,
        }

        /// One item of a subscription, decoded by the stream the channel was
        /// subscribed to. On the wire it is the stream's own item value.
        #[derive(Clone, Debug, PartialEq)]
        pub enum StreamItem {
            $( $variant($item), )*
        }

        /// One marker type per stream, for typed subscriptions.
        pub mod stream {
            #[allow(unused_imports)]
            use super::*;
            $(
                $(#[$meta])*
                #[derive(Clone, Copy, Debug)]
                pub struct $variant;

                impl $crate::v8::api::StreamDef for $variant {
                    const NAME: &'static str = $name;
                    const SCOPE: $crate::v8::Scope = $crate::v8::Scope::$scope;
                    const CURSOR: $crate::v8::api::CursorKind = $crate::v8::api::CursorKind::$cursor;
                    const DATA: bool = streams!(@data $($flag)?);
                    type Spec = $spec;
                    type Item = $item;

                    fn wrap(spec: $spec) -> super::StreamSpec {
                        super::StreamSpec::$variant(spec)
                    }

                    fn unwrap(item: super::StreamItem) -> Option<$item> {
                        #[allow(unreachable_patterns)]
                        match item {
                            super::StreamItem::$variant(item) => Some(item),
                            _ => None,
                        }
                    }
                }
            )*
        }

        impl StreamSpec {
            pub fn static_name(&self) -> Option<&'static str> {
                match self {
                    $( Self::$variant(_) => Some($name), )*
                    Self::Unknown => None,
                }
            }

            pub fn scope(&self) -> Option<$crate::v8::Scope> {
                match self {
                    $( Self::$variant(_) => Some($crate::v8::Scope::$scope), )*
                    Self::Unknown => None,
                }
            }
        }

        impl StreamItem {
            pub fn stream_name(&self) -> &'static str {
                match self {
                    $( Self::$variant(_) => $name, )*
                }
            }

            /// Read one item of a `stream` subscription. `None` for a stream
            /// this build does not know, whose value is skipped.
            pub fn deserialize_for<'de, D: serde::Deserializer<'de>>(
                stream: &str,
                deserializer: D,
            ) -> Result<Option<Self>, D::Error> {
                use serde::Deserialize as _;
                match stream {
                    $( $name => <$item>::deserialize(deserializer).map(|item| Some(Self::$variant(item))), )*
                    _ => {
                        serde::de::IgnoredAny::deserialize(deserializer)?;
                        Ok(None)
                    }
                }
            }
        }

        impl serde::Serialize for StreamItem {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match self {
                    $( Self::$variant(item) => item.serialize(serializer), )*
                }
            }
        }

        /// The stream manifest, in declaration order.
        pub fn streams() -> Vec<$crate::v8::api::StreamInfo> {
            vec![$(
                $crate::v8::api::StreamInfo {
                    name: $name,
                    scope: $crate::v8::Scope::$scope,
                    cursor: $crate::v8::api::CursorKind::$cursor,
                    data: streams!(@data $($flag)?),
                },
            )*]
        }

        #[cfg(feature = "schema")]
        pub fn stream_schemas(
            generator: &mut schemars::SchemaGenerator,
        ) -> Vec<($crate::v8::api::StreamInfo, schemars::Schema, schemars::Schema)> {
            streams()
                .into_iter()
                .zip([$(
                    (generator.subschema_for::<$spec>(), generator.subschema_for::<$item>()),
                )*])
                .map(|(info, (spec, item))| (info, spec, item))
                .collect()
        }
    };
    (@data data) => { true };
    (@data) => { false };
}

/// Declares an open union of orchestration commands, tagged by `type`, with
/// the static scope each needs (RFC 0003 §5.3). Each line is
/// `"name" => Variant(Body), scope = Scope;`.
///
/// `orchestration.dispatch` uses `crate::orchestration::Command` directly, so
/// the macro is exercised by its tests only for now.
#[allow(unused_macros)]
macro_rules! commands {
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $enum_name:ident;
        $(
            $(#[$meta:meta])*
            $name:literal => $variant:ident($body:ty), scope = $scope:ident ;
        )*
    ) => {
        $(#[$enum_meta])*
        #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
        #[serde(tag = "type")]
        $vis enum $enum_name {
            $( $(#[$meta])* #[serde(rename = $name)] $variant($body), )*
            /// A command this build does not know (or a daemon-internal one a
            /// client may not send): rejected with `badRequest`.
            #[serde(other, rename = "unknown")]
            Unknown,
        }

        impl $enum_name {
            pub fn static_name(&self) -> Option<&'static str> {
                match self {
                    $( Self::$variant(_) => Some($name), )*
                    Self::Unknown => None,
                }
            }

            /// The static scope; payload-dependent additions are a function
            /// per command (RFC 0003 §6.3).
            pub fn scope(&self) -> Option<$crate::v8::Scope> {
                match self {
                    $( Self::$variant(_) => Some($crate::v8::Scope::$scope), )*
                    Self::Unknown => None,
                }
            }

            /// The command manifest, in declaration order.
            pub fn manifest() -> Vec<$crate::v8::api::CommandInfo> {
                vec![$(
                    $crate::v8::api::CommandInfo {
                        name: $name,
                        scope: $crate::v8::Scope::$scope,
                    },
                )*]
            }
        }
    };
}
