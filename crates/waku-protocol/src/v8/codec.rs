//! Message encodings (RFC 0003 §1.3).
//!
//! - **MessagePack** for native clients: `rmp-serde` with structs written as
//!   maps with named fields, so unknown fields and variants can be skipped.
//!   It runs in human-readable mode, so ids are UUID strings exactly as in
//!   JSON and both encodings share one logical shape.
//! - **JSON** for web clients.
//!
//! Integers are written in their smallest form and map keys in declaration
//! order, so an encoding is deterministic: the golden tests compare bytes.

use serde::Serialize;
use serde::de::{Deserialize, DeserializeSeed};

/// A connection's message encoding, fixed by its first message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Encoding {
    MessagePack,
    Json,
}

#[derive(Debug)]
pub enum CodecError {
    Encode(String),
    Decode(String),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(message) => write!(f, "could not encode a v8 message: {message}"),
            Self::Decode(message) => write!(f, "could not decode a v8 message: {message}"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Encode `value` as one message body.
pub fn to_vec<T: Serialize + ?Sized>(encoding: Encoding, value: &T) -> Result<Vec<u8>, CodecError> {
    match encoding {
        Encoding::MessagePack => {
            let mut buffer = Vec::with_capacity(128);
            let mut serializer = rmp_serde::Serializer::new(&mut buffer)
                .with_struct_map()
                .with_human_readable();
            value
                .serialize(&mut serializer)
                .map_err(|error| CodecError::Encode(error.to_string()))?;
            Ok(buffer)
        }
        Encoding::Json => {
            serde_json::to_vec(value).map_err(|error| CodecError::Encode(error.to_string()))
        }
    }
}

/// Decode one message body.
pub fn from_slice<'de, T: Deserialize<'de>>(
    encoding: Encoding,
    bytes: &'de [u8],
) -> Result<T, CodecError> {
    from_slice_seed(encoding, bytes, std::marker::PhantomData::<T>)
}

/// Decode one message body with a seed, e.g. a
/// [`ServerMessageSeed`](super::ServerMessageSeed) that knows which method
/// each pending call id belongs to.
pub fn from_slice_seed<'de, S: DeserializeSeed<'de>>(
    encoding: Encoding,
    bytes: &'de [u8],
    seed: S,
) -> Result<S::Value, CodecError> {
    let decode = |error: &dyn std::fmt::Display| CodecError::Decode(error.to_string());
    match encoding {
        Encoding::MessagePack => {
            let mut deserializer =
                rmp_serde::Deserializer::from_read_ref(bytes).with_human_readable();
            // A frame body holds exactly one message, so bytes after it
            // cannot be another message; they are ignored.
            seed.deserialize(&mut deserializer)
                .map_err(|error| decode(&error))
        }
        Encoding::Json => {
            let mut deserializer = serde_json::Deserializer::from_slice(bytes);
            let value = seed
                .deserialize(&mut deserializer)
                .map_err(|error| decode(&error))?;
            deserializer.end().map_err(|error| decode(&error))?;
            Ok(value)
        }
    }
}

/// The `items` message around stream items that were encoded once and are
/// shared by every subscriber (RFC 0003 §2.4, "encode once"). Byte-identical
/// to encoding `ServerMessage::Items` with the decoded items.
pub fn encode_items_envelope<B: AsRef<[u8]>>(
    encoding: Encoding,
    channel: u32,
    items: &[B],
) -> Vec<u8> {
    let payload: usize = items.iter().map(|item| item.as_ref().len()).sum();
    match encoding {
        Encoding::MessagePack => {
            let mut buffer = Vec::with_capacity(payload + 32);
            // Writing into a Vec cannot fail.
            let _ = rmp::encode::write_map_len(&mut buffer, 3);
            let _ = rmp::encode::write_str(&mut buffer, "type");
            let _ = rmp::encode::write_str(&mut buffer, "items");
            let _ = rmp::encode::write_str(&mut buffer, "channel");
            let _ = rmp::encode::write_uint(&mut buffer, u64::from(channel));
            let _ = rmp::encode::write_str(&mut buffer, "items");
            let _ = rmp::encode::write_array_len(&mut buffer, items.len() as u32);
            for item in items {
                buffer.extend_from_slice(item.as_ref());
            }
            buffer
        }
        Encoding::Json => {
            let mut buffer = Vec::with_capacity(payload + items.len() + 48);
            buffer.extend_from_slice(b"{\"type\":\"items\",\"channel\":");
            buffer.extend_from_slice(channel.to_string().as_bytes());
            buffer.extend_from_slice(b",\"items\":[");
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    buffer.push(b',');
                }
                buffer.extend_from_slice(item.as_ref());
            }
            buffer.extend_from_slice(b"]}");
            buffer
        }
    }
}
