//! Declarative helpers for the orchestration types.

/// A fieldless enum that crosses the wire or reaches disk.
///
/// Every variant names its serialized string explicitly, which doubles as the
/// value stored in indexed SQLite columns (`as_str`). An unknown string
/// decodes to `Unknown`, or, when the enum starts with
/// `fallback Variant = "text"`, to that variant (used where "the weakest claim"
/// is the safe reading, as for capability strengths). Unknown values therefore
/// never fail a frame or an event.
macro_rules! open_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident = $text:literal ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord,
            ::serde::Serialize, ::serde::Deserialize,
        )]
        pub enum $name {
            $( $(#[$vmeta])* #[serde(rename = $text)] $variant, )+
            /// A value written by a newer version.
            #[serde(rename = "unknown", other)]
            Unknown,
        }

        impl $name {
            /// Every known variant, in declaration order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// The serialized name, also stored in indexed columns.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                    Self::Unknown => "unknown",
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        /// A string enum whose unknown values decode instead of failing
        /// (`x-waku-open`), as RFC 0003 §1.1 requires.
        #[cfg(feature = "schema")]
        impl ::schemars::JsonSchema for $name {
            fn schema_name() -> ::std::borrow::Cow<'static, str> {
                stringify!($name).into()
            }

            fn json_schema(_: &mut ::schemars::SchemaGenerator) -> ::schemars::Schema {
                let known: ::std::vec::Vec<&'static str> = Self::ALL
                    .iter()
                    .map(|value| value.as_str())
                    .filter(|value| *value != "unknown")
                    .collect();
                ::schemars::json_schema!({
                    "type": "string",
                    "enum": known,
                    "x-waku-open": true,
                })
            }
        }
    };
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            fallback $(#[$fmeta:meta])* $fallback:ident = $ftext:literal;
            $( $(#[$vmeta:meta])* $variant:ident = $text:literal ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord,
            ::serde::Serialize, ::serde::Deserialize,
        )]
        pub enum $name {
            $( $(#[$vmeta])* #[serde(rename = $text)] $variant, )+
            /// Also what an unknown value decodes to.
            $(#[$fmeta])*
            #[serde(rename = $ftext, other)]
            $fallback,
        }

        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant,)+ Self::$fallback];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                    Self::$fallback => $ftext,
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        /// A string enum whose unknown values decode instead of failing
        /// (`x-waku-open`), as RFC 0003 §1.1 requires.
        #[cfg(feature = "schema")]
        impl ::schemars::JsonSchema for $name {
            fn schema_name() -> ::std::borrow::Cow<'static, str> {
                stringify!($name).into()
            }

            fn json_schema(_: &mut ::schemars::SchemaGenerator) -> ::schemars::Schema {
                let known: ::std::vec::Vec<&'static str> = Self::ALL
                    .iter()
                    .map(|value| value.as_str())
                    .filter(|value| *value != "unknown")
                    .collect();
                ::schemars::json_schema!({
                    "type": "string",
                    "enum": known,
                    "x-waku-open": true,
                })
            }
        }
    };
}

/// `#[serde(transparent)]` UUID newtypes for app ids (RFC 0001 R6).
macro_rules! uuid_ids {
    ($( $(#[$meta:meta])* $name:ident ),+ $(,)?) => {
        $(
            $(#[$meta])*
            #[derive(
                Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord,
                ::serde::Serialize, ::serde::Deserialize,
            )]
            #[cfg_attr(feature = "schema", derive(::schemars::JsonSchema))]
            #[serde(transparent)]
            pub struct $name(pub ::uuid::Uuid);

            impl $name {
                pub const fn from_uuid(uuid: ::uuid::Uuid) -> Self {
                    Self(uuid)
                }

                pub const fn as_uuid(&self) -> ::uuid::Uuid {
                    self.0
                }
            }

            impl ::std::fmt::Display for $name {
                fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    ::std::fmt::Display::fmt(&self.0.hyphenated(), formatter)
                }
            }

            impl ::std::str::FromStr for $name {
                type Err = ::uuid::Error;

                fn from_str(text: &str) -> Result<Self, Self::Err> {
                    ::uuid::Uuid::parse_str(text).map(Self)
                }
            }

            impl From<::uuid::Uuid> for $name {
                fn from(uuid: ::uuid::Uuid) -> Self {
                    Self(uuid)
                }
            }
        )+
    };
}
