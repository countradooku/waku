//! An owned copy of one decoded value, for the rare message whose keys arrive
//! out of the order our encoders write them.
//!
//! The envelope decoders read `type` first and, for a call, `method` before
//! `params`, so they can decode each value straight into its type. Every
//! encoder in this repository writes keys in that order. A peer that does
//! not (a hand-written debugging client, a JSON library that sorts keys)
//! still decodes: the out-of-order part is buffered here and replayed through
//! the same typed decoders. This is internal decoding state, never a wire
//! type, so it does not fall under the no-`Value` rule.

use serde::de::value::{MapDeserializer, SeqDeserializer, StringDeserializer};
use serde::de::{self, Deserialize, Deserializer, IntoDeserializer, Visitor};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Buffered {
    Nil,
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
    Seq(Vec<Buffered>),
    Map(Vec<(Buffered, Buffered)>),
}

impl Buffered {
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(text) => Some(text),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for Buffered {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(BufferedVisitor)
    }
}

struct BufferedVisitor;

impl<'de> Visitor<'de> for BufferedVisitor {
    type Value = Buffered;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("any value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Buffered, E> {
        Ok(Buffered::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Buffered, E> {
        Ok(if value >= 0 {
            Buffered::U64(value as u64)
        } else {
            Buffered::I64(value)
        })
    }

    fn visit_u64<E>(self, value: u64) -> Result<Buffered, E> {
        Ok(Buffered::U64(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Buffered, E> {
        Ok(Buffered::F64(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Buffered, E> {
        Ok(Buffered::Str(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Buffered, E> {
        Ok(Buffered::Str(value))
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Buffered, E> {
        Ok(Buffered::Bytes(value.to_vec()))
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Buffered, E> {
        Ok(Buffered::Bytes(value))
    }

    fn visit_none<E>(self) -> Result<Buffered, E> {
        Ok(Buffered::Nil)
    }

    fn visit_unit<E>(self) -> Result<Buffered, E> {
        Ok(Buffered::Nil)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Buffered, D::Error> {
        Buffered::deserialize(deserializer)
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Buffered, D::Error> {
        Buffered::deserialize(deserializer)
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Buffered, A::Error> {
        let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(1024));
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Buffered::Seq(items))
    }

    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Buffered, A::Error> {
        let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(0).min(1024));
        while let Some(entry) = map.next_entry()? {
            entries.push(entry);
        }
        Ok(Buffered::Map(entries))
    }
}

/// Decoding error from a [`Buffered`] value; converted into the outer
/// decoder's error type by the caller.
#[derive(Debug)]
pub(crate) struct BufferedError(String);

impl std::fmt::Display for BufferedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BufferedError {}

impl de::Error for BufferedError {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

impl<'de> IntoDeserializer<'de, BufferedError> for Buffered {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self {
        self
    }
}

impl<'de> Deserializer<'de> for Buffered {
    type Error = BufferedError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, BufferedError> {
        match self {
            Self::Nil => visitor.visit_unit(),
            Self::Bool(value) => visitor.visit_bool(value),
            Self::U64(value) => visitor.visit_u64(value),
            Self::I64(value) => visitor.visit_i64(value),
            Self::F64(value) => visitor.visit_f64(value),
            Self::Str(value) => visitor.visit_string(value),
            Self::Bytes(value) => visitor.visit_byte_buf(value),
            Self::Seq(items) => {
                let mut seq = SeqDeserializer::new(items.into_iter());
                let value = visitor.visit_seq(&mut seq)?;
                seq.end()?;
                Ok(value)
            }
            Self::Map(entries) => {
                let mut map = MapDeserializer::new(entries.into_iter());
                let value = visitor.visit_map(&mut map)?;
                map.end()?;
                Ok(value)
            }
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, BufferedError> {
        match self {
            Self::Nil => visitor.visit_none(),
            other => visitor.visit_some(other),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, BufferedError> {
        match self {
            Self::Str(variant) => {
                let variant: StringDeserializer<BufferedError> = variant.into_deserializer();
                visitor.visit_enum(variant)
            }
            _ => Err(de::Error::custom(
                "only unit enum variants are encoded as plain values",
            )),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, BufferedError> {
        visitor.visit_newtype_struct(self)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct seq tuple tuple_struct map struct
        identifier ignored_any
    }
}
