//! Bulk bytes as binary WebSocket messages (#22).
//!
//! Blob data and uploads used to travel as base64 inside JSON: a third larger,
//! and encoded, copied and decoded on both ends. When both sides agree in
//! their hellos (`binaryFrames`), a message that carries bulk bytes is sent
//! as one binary WebSocket message instead:
//!
//! ```text
//! u32 (big-endian) header length | header: the JSON message | raw bytes
//! ```
//!
//! The header is the ordinary message with its one bulk field left empty;
//! the receiver puts the raw bytes back. Only these fields are carried this
//! way: `ResponsePayload::BlobData.bytes` (daemon → client) and
//! `Command::StoreBlob.bytes` / `Command::WriteTerminal.data` (client →
//! daemon). Everything else stays a JSON text message, and a peer that did
//! not ask for binary frames never receives one. This is a v7-compatible
//! step toward RFC 0003's binary framing, not that framing itself.

use crate::{ClientMessage, Command, ResponseOutcome, ResponsePayload, ServerMessage};

/// Join a JSON header and raw bytes into one binary message.
pub fn encode(header: &[u8], body: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(4 + header.len() + body.len());
    message.extend_from_slice(&(header.len() as u32).to_be_bytes());
    message.extend_from_slice(header);
    message.extend_from_slice(body);
    message
}

/// Split a binary message into its JSON header and raw bytes.
pub fn split(message: &[u8]) -> Option<(&[u8], &[u8])> {
    let length = u32::from_be_bytes(message.get(..4)?.try_into().ok()?) as usize;
    let rest = &message[4..];
    (length <= rest.len()).then(|| rest.split_at(length))
}

impl ServerMessage {
    /// Move the bulk bytes out of this message, leaving the field empty.
    pub fn take_bulk(&mut self) -> Option<Vec<u8>> {
        match self {
            Self::Response {
                outcome:
                    ResponseOutcome::Ok {
                        payload: ResponsePayload::BlobData { bytes },
                    },
                ..
            } => Some(std::mem::take(bytes)),
            _ => None,
        }
    }

    /// Put bulk bytes back into the field [`Self::take_bulk`] emptied.
    /// Returns `false` when this message has no bulk field.
    pub fn put_bulk(&mut self, body: Vec<u8>) -> bool {
        match self {
            Self::Response {
                outcome:
                    ResponseOutcome::Ok {
                        payload: ResponsePayload::BlobData { bytes },
                    },
                ..
            } => {
                *bytes = body;
                true
            }
            _ => false,
        }
    }
}

impl ClientMessage {
    /// Move the bulk bytes out of this message, leaving the field empty.
    pub fn take_bulk(&mut self) -> Option<Vec<u8>> {
        match self {
            Self::Request(request) => match &mut request.command {
                Command::StoreBlob { bytes, .. } => Some(std::mem::take(bytes)),
                Command::WriteTerminal { data } => Some(std::mem::take(data)),
                _ => None,
            },
            _ => None,
        }
    }

    /// Put bulk bytes back into the field [`Self::take_bulk`] emptied.
    /// Returns `false` when this message has no bulk field.
    pub fn put_bulk(&mut self, body: Vec<u8>) -> bool {
        match self {
            Self::Request(request) => match &mut request.command {
                Command::StoreBlob { bytes, .. } => {
                    *bytes = body;
                    true
                }
                Command::WriteTerminal { data } => {
                    *data = body;
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Request;
    use uuid::Uuid;

    #[test]
    fn blob_responses_round_trip_without_base64() {
        let bytes = (0..=255).collect::<Vec<u8>>();
        let mut message = ServerMessage::Response {
            request_id: Uuid::from_u128(1),
            outcome: ResponseOutcome::Ok {
                payload: ResponsePayload::BlobData {
                    bytes: bytes.clone(),
                },
            },
        };
        let body = message.take_bulk().unwrap();
        let header = serde_json::to_vec(&message).unwrap();
        // The header carries no base64 of the body.
        assert!(header.len() < 160, "{}", String::from_utf8_lossy(&header));
        let frame = encode(&header, &body);
        assert_eq!(frame.len(), 4 + header.len() + bytes.len());

        let (header, body) = split(&frame).unwrap();
        let mut decoded: ServerMessage = serde_json::from_slice(header).unwrap();
        assert!(decoded.put_bulk(body.to_vec()));
        let ServerMessage::Response {
            outcome:
                ResponseOutcome::Ok {
                    payload: ResponsePayload::BlobData { bytes: decoded },
                },
            ..
        } = decoded
        else {
            panic!("expected blob data");
        };
        assert_eq!(decoded, bytes);
    }

    #[test]
    fn uploads_round_trip_and_other_messages_have_no_bulk() {
        let mut upload = ClientMessage::Request(Request {
            request_id: Uuid::from_u128(2),
            session_id: Uuid::nil(),
            runtime_id: Uuid::nil(),
            command: Command::StoreBlob {
                mime_type: "image/png".into(),
                bytes: vec![1, 2, 3],
            },
        });
        assert_eq!(upload.take_bulk(), Some(vec![1, 2, 3]));
        assert!(upload.put_bulk(vec![4, 5]));
        assert_eq!(upload.take_bulk(), Some(vec![4, 5]));

        let mut other = ClientMessage::Shutdown;
        assert_eq!(other.take_bulk(), None);
        assert!(!other.put_bulk(Vec::new()));
        assert!(split(&[0, 0, 0, 9, b'{']).is_none());
        assert!(split(&[0, 0]).is_none());
    }
}
