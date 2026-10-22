//! Binary framing (RFC 0003 §1.4).
//!
//! Every binary frame starts with a 12-byte header:
//!
//! ```text
//! 0  u32 BE  body_len   length of the body, excluding the header
//! 4  u8      kind       0x01 MSG (an encoded message), 0x02 DATA (raw bytes)
//! 5  u8      flags      bit 0 FIN (last DATA chunk), bit 1 ZSTD (compressed MSG)
//! 6  u16     reserved   0
//! 8  u32 BE  channel    0 for MSG; the subscription or transfer for DATA
//! ```
//!
//! Local stream transports (Unix socket, named pipe) carry a sequence of
//! frames after the 8-byte [`PREFACE`](super::PREFACE); [`FrameDecoder`]
//! splits them out of a read buffer however the reads were cut. A binary
//! WebSocket message is exactly one frame ([`parse_message`]).
//!
//! This module is pure: it does no I/O, so the daemon and every client share
//! one implementation of the limits.

use super::message::GoodbyeCode;
use super::{
    FRAME_HEADER_BYTES, HANDSHAKE_MESSAGE_BYTES, MAX_DATA_CHUNK_BYTES, MAX_MESSAGE_BYTES, PREFACE,
};

/// An encoded message.
pub const KIND_MSG: u8 = 0x01;
/// Raw bytes on a channel.
pub const KIND_DATA: u8 = 0x02;
/// DATA: the last chunk of a transfer.
pub const FLAG_FIN: u8 = 0x01;
/// MSG: the body is zstd-compressed. Only valid once `welcome` selected
/// compression, which this daemon never does yet.
pub const FLAG_ZSTD: u8 = 0x02;
const KNOWN_FLAGS: u8 = FLAG_FIN | FLAG_ZSTD;

/// One frame header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub body_len: u32,
    pub kind: u8,
    pub flags: u8,
    pub channel: u32,
}

impl FrameHeader {
    pub fn msg(body_len: usize) -> Self {
        Self {
            body_len: body_len as u32,
            kind: KIND_MSG,
            flags: 0,
            channel: 0,
        }
    }

    pub fn data(channel: u32, body_len: usize, fin: bool) -> Self {
        Self {
            body_len: body_len as u32,
            kind: KIND_DATA,
            flags: if fin { FLAG_FIN } else { 0 },
            channel,
        }
    }

    pub fn encode(&self) -> [u8; FRAME_HEADER_BYTES] {
        let mut header = [0; FRAME_HEADER_BYTES];
        header[0..4].copy_from_slice(&self.body_len.to_be_bytes());
        header[4] = self.kind;
        header[5] = self.flags;
        header[8..12].copy_from_slice(&self.channel.to_be_bytes());
        header
    }

    /// Parse a header. Fails only on a non-zero reserved field; the kind,
    /// flags and length are checked by [`Self::validate`].
    pub fn decode(bytes: &[u8; FRAME_HEADER_BYTES]) -> Result<Self, FrameError> {
        if bytes[6] != 0 || bytes[7] != 0 {
            return Err(FrameError::ReservedNotZero);
        }
        Ok(Self {
            body_len: u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            kind: bytes[4],
            flags: bytes[5],
            channel: u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
        })
    }

    /// Check this header against the receiver's limits before reading its
    /// body, so an oversized frame is refused without buffering it.
    /// `Ok(false)` means a frame kind this side does not know: skip the body.
    pub fn validate(&self, limits: &FrameLimits) -> Result<bool, FrameError> {
        if self.flags & !KNOWN_FLAGS != 0 {
            return Err(FrameError::UnknownFlags(self.flags));
        }
        match self.kind {
            KIND_MSG => {
                if self.flags & FLAG_FIN != 0 {
                    return Err(FrameError::FinOnMessage);
                }
                if self.flags & FLAG_ZSTD != 0 && !limits.compression {
                    return Err(FrameError::CompressionNotNegotiated);
                }
                if self.channel != 0 {
                    return Err(FrameError::MessageOnChannel(self.channel));
                }
                if self.body_len > limits.max_message_bytes {
                    return Err(FrameError::TooLarge {
                        kind: self.kind,
                        len: self.body_len,
                        limit: limits.max_message_bytes,
                    });
                }
                Ok(true)
            }
            KIND_DATA => {
                if self.flags & FLAG_ZSTD != 0 {
                    return Err(FrameError::CompressedData);
                }
                if self.channel == 0 {
                    return Err(FrameError::DataOnChannelZero);
                }
                if self.body_len > limits.max_data_bytes {
                    return Err(FrameError::TooLarge {
                        kind: self.kind,
                        len: self.body_len,
                        limit: limits.max_data_bytes,
                    });
                }
                Ok(true)
            }
            // Unknown kinds are skipped by length, but still bounded: a peer
            // cannot make the receiver read gigabytes it will throw away.
            _ if self.body_len > limits.max_message_bytes => Err(FrameError::TooLarge {
                kind: self.kind,
                len: self.body_len,
                limit: limits.max_message_bytes,
            }),
            _ => Ok(false),
        }
    }
}

/// What a receiver accepts. [`FrameLimits::handshake`] applies until
/// `welcome`, [`FrameLimits::session`] after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameLimits {
    pub max_message_bytes: u32,
    pub max_data_bytes: u32,
    /// Whether `welcome` selected compression.
    pub compression: bool,
}

impl FrameLimits {
    pub const fn handshake() -> Self {
        Self {
            max_message_bytes: HANDSHAKE_MESSAGE_BYTES,
            max_data_bytes: MAX_DATA_CHUNK_BYTES,
            compression: false,
        }
    }

    pub const fn session(max_message_bytes: u32) -> Self {
        Self {
            max_message_bytes,
            max_data_bytes: MAX_DATA_CHUNK_BYTES,
            compression: false,
        }
    }
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self::session(MAX_MESSAGE_BYTES)
    }
}

/// A decoded frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// An encoded message body.
    Msg(Vec<u8>),
    /// Raw bytes on `channel`; `fin` marks the last chunk of a transfer.
    Data {
        channel: u32,
        fin: bool,
        body: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    TooLarge {
        kind: u8,
        len: u32,
        limit: u32,
    },
    UnknownFlags(u8),
    ReservedNotZero,
    FinOnMessage,
    MessageOnChannel(u32),
    DataOnChannelZero,
    CompressedData,
    CompressionNotNegotiated,
    /// A binary WebSocket message whose header length disagrees with the
    /// message length.
    LengthMismatch {
        header: u32,
        actual: usize,
    },
    /// A binary WebSocket message shorter than a header.
    Truncated,
}

impl FrameError {
    /// The `goodbye` code a receiver sends before closing.
    pub fn goodbye_code(&self) -> GoodbyeCode {
        match self {
            Self::TooLarge { .. } => GoodbyeCode::FrameTooLarge,
            _ => GoodbyeCode::ProtocolError,
        }
    }
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge { kind, len, limit } => {
                write!(
                    f,
                    "frame of kind {kind} has {len} bytes, over the {limit}-byte limit"
                )
            }
            Self::UnknownFlags(flags) => write!(f, "frame has unknown flags {flags:#04x}"),
            Self::ReservedNotZero => f.write_str("frame header has non-zero reserved bytes"),
            Self::FinOnMessage => f.write_str("MSG frame has the FIN flag"),
            Self::MessageOnChannel(channel) => write!(f, "MSG frame on channel {channel}"),
            Self::DataOnChannelZero => f.write_str("DATA frame on channel 0"),
            Self::CompressedData => f.write_str("DATA frame has the ZSTD flag"),
            Self::CompressionNotNegotiated => {
                f.write_str("compressed MSG frame but compression was not negotiated")
            }
            Self::LengthMismatch { header, actual } => write!(
                f,
                "frame header says {header} body bytes but the message has {actual}"
            ),
            Self::Truncated => f.write_str("binary message is shorter than a frame header"),
        }
    }
}

impl std::error::Error for FrameError {}

/// Header plus body of a MSG frame.
pub fn encode_msg(body: &[u8]) -> Vec<u8> {
    encode(FrameHeader::msg(body.len()), body)
}

/// Header plus body of a DATA frame.
pub fn encode_data(channel: u32, fin: bool, body: &[u8]) -> Vec<u8> {
    encode(FrameHeader::data(channel, body.len(), fin), body)
}

fn encode(header: FrameHeader, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + body.len());
    frame.extend_from_slice(&header.encode());
    frame.extend_from_slice(body);
    frame
}

/// Parse one binary WebSocket message, which is exactly one frame.
/// `Ok(None)` is a frame of an unknown kind, to be ignored.
pub fn parse_message(message: &[u8], limits: &FrameLimits) -> Result<Option<Frame>, FrameError> {
    let header: &[u8; FRAME_HEADER_BYTES] = message
        .get(..FRAME_HEADER_BYTES)
        .and_then(|header| header.try_into().ok())
        .ok_or(FrameError::Truncated)?;
    let header = FrameHeader::decode(header)?;
    let body = &message[FRAME_HEADER_BYTES..];
    if header.body_len as usize != body.len() {
        return Err(FrameError::LengthMismatch {
            header: header.body_len,
            actual: body.len(),
        });
    }
    if !header.validate(limits)? {
        return Ok(None);
    }
    Ok(Some(frame_from(header, body.to_vec())))
}

fn frame_from(header: FrameHeader, body: Vec<u8>) -> Frame {
    if header.kind == KIND_MSG {
        Frame::Msg(body)
    } else {
        Frame::Data {
            channel: header.channel,
            fin: header.flags & FLAG_FIN != 0,
            body,
        }
    }
}

/// Splits frames out of a local stream's read buffer.
///
/// Feed it whatever the socket returned, in order; it handles frames cut
/// across reads and several frames in one read, refuses an oversized frame
/// from its header alone, and skips frames of unknown kinds by length.
#[derive(Debug)]
pub struct FrameDecoder {
    limits: FrameLimits,
    buffer: Vec<u8>,
    /// Body bytes of an unknown-kind frame still to be discarded.
    skipping: usize,
}

impl FrameDecoder {
    pub fn new(limits: FrameLimits) -> Self {
        Self {
            limits,
            buffer: Vec::new(),
            skipping: 0,
        }
    }

    pub fn limits(&self) -> FrameLimits {
        self.limits
    }

    /// Switch limits, e.g. from the handshake cap to `maxMessageBytes` once
    /// `welcome` is out.
    pub fn set_limits(&mut self, limits: FrameLimits) {
        self.limits = limits;
    }

    /// Append bytes read from the socket.
    pub fn extend(&mut self, bytes: &[u8]) {
        if self.skipping > 0 {
            let skipped = self.skipping.min(bytes.len());
            self.skipping -= skipped;
            self.buffer.extend_from_slice(&bytes[skipped..]);
        } else {
            self.buffer.extend_from_slice(bytes);
        }
    }

    /// The next whole frame, if one is buffered.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        loop {
            if self.skipping > 0 {
                let skipped = self.skipping.min(self.buffer.len());
                self.buffer.drain(..skipped);
                self.skipping -= skipped;
                if self.skipping > 0 {
                    return Ok(None);
                }
            }
            let Some(header) = self.buffer.get(..FRAME_HEADER_BYTES) else {
                return Ok(None);
            };
            let header = FrameHeader::decode(header.try_into().expect("12-byte slice"))?;
            let known = header.validate(&self.limits)?;
            let total = FRAME_HEADER_BYTES + header.body_len as usize;
            if !known {
                self.buffer.drain(..FRAME_HEADER_BYTES);
                self.skipping = header.body_len as usize;
                continue;
            }
            if self.buffer.len() < total {
                // Reserve once for the whole body instead of growing per read.
                self.buffer.reserve(total - self.buffer.len());
                return Ok(None);
            }
            let body = self.buffer[FRAME_HEADER_BYTES..total].to_vec();
            self.buffer.drain(..total);
            return Ok(Some(frame_from(header, body)));
        }
    }
}

/// How a received local preface compares with ours.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefaceCheck {
    Match,
    /// A Waku preface for another major version: answer with ours and close.
    OtherMajor(u8),
    /// Not a Waku preface at all.
    NotWaku,
}

pub fn check_preface(preface: &[u8; 8]) -> PrefaceCheck {
    if preface[..4] != PREFACE[..4] {
        return PrefaceCheck::NotWaku;
    }
    if preface[5] != PREFACE[5] {
        return PrefaceCheck::OtherMajor(preface[5]);
    }
    PrefaceCheck::Match
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips_and_matches_the_rfc_layout() {
        let header = FrameHeader::data(0x0102_0304, 5, true);
        let bytes = header.encode();
        assert_eq!(
            bytes,
            [0, 0, 0, 5, KIND_DATA, FLAG_FIN, 0, 0, 1, 2, 3, 4],
            "length, kind, flags, reserved, channel"
        );
        assert_eq!(FrameHeader::decode(&bytes).unwrap(), header);
        assert_eq!(PREFACE, [0x57, 0x41, 0x4B, 0x55, 0, 8, 0, 0]);
    }

    #[test]
    fn decoder_handles_partial_and_coalesced_reads() {
        let mut stream = encode_msg(b"hello");
        stream.extend(encode_data(3, false, b"abc"));
        stream.extend(encode_data(3, true, b""));
        let mut decoder = FrameDecoder::new(FrameLimits::default());
        let mut frames = Vec::new();
        // One byte at a time.
        for byte in &stream {
            decoder.extend(std::slice::from_ref(byte));
            while let Some(frame) = decoder.next_frame().unwrap() {
                frames.push(frame);
            }
        }
        let expected = vec![
            Frame::Msg(b"hello".to_vec()),
            Frame::Data {
                channel: 3,
                fin: false,
                body: b"abc".to_vec(),
            },
            Frame::Data {
                channel: 3,
                fin: true,
                body: Vec::new(),
            },
        ];
        assert_eq!(frames, expected);
        // All at once.
        let mut decoder = FrameDecoder::new(FrameLimits::default());
        decoder.extend(&stream);
        let mut frames = Vec::new();
        while let Some(frame) = decoder.next_frame().unwrap() {
            frames.push(frame);
        }
        assert_eq!(frames, expected);
    }

    #[test]
    fn unknown_kinds_are_skipped_by_length() {
        let mut stream = encode(
            FrameHeader {
                body_len: 4,
                kind: 0x7F,
                flags: 0,
                channel: 9,
            },
            b"skip",
        );
        stream.extend(encode_msg(b"next"));
        for split in 0..stream.len() {
            let mut decoder = FrameDecoder::new(FrameLimits::default());
            decoder.extend(&stream[..split]);
            let mut frames = Vec::new();
            while let Some(frame) = decoder.next_frame().unwrap() {
                frames.push(frame);
            }
            decoder.extend(&stream[split..]);
            while let Some(frame) = decoder.next_frame().unwrap() {
                frames.push(frame);
            }
            assert_eq!(
                frames,
                vec![Frame::Msg(b"next".to_vec())],
                "split at {split}"
            );
        }
        let unknown = encode(
            FrameHeader {
                body_len: 0,
                kind: 0x09,
                flags: 0,
                channel: 0,
            },
            b"",
        );
        assert_eq!(parse_message(&unknown, &FrameLimits::default()), Ok(None));
    }

    #[test]
    fn limits_are_enforced_from_the_header_alone() {
        let mut decoder = FrameDecoder::new(FrameLimits::handshake());
        // Only the header of a frame one byte over the handshake cap.
        decoder.extend(&FrameHeader::msg(HANDSHAKE_MESSAGE_BYTES as usize + 1).encode());
        let error = decoder.next_frame().unwrap_err();
        assert!(matches!(error, FrameError::TooLarge { .. }));
        assert_eq!(error.goodbye_code(), GoodbyeCode::FrameTooLarge);

        let mut decoder = FrameDecoder::new(FrameLimits::handshake());
        decoder.extend(&FrameHeader::msg(HANDSHAKE_MESSAGE_BYTES as usize).encode());
        assert_eq!(
            decoder.next_frame(),
            Ok(None),
            "exactly at the cap waits for the body"
        );

        let session = FrameLimits::default();
        let data = FrameHeader::data(1, MAX_DATA_CHUNK_BYTES as usize + 1, false);
        assert!(matches!(
            data.validate(&session),
            Err(FrameError::TooLarge { .. })
        ));
        let message = FrameHeader::msg(MAX_MESSAGE_BYTES as usize + 1);
        assert!(matches!(
            message.validate(&session),
            Err(FrameError::TooLarge { .. })
        ));
        assert_eq!(
            FrameHeader::msg(MAX_MESSAGE_BYTES as usize).validate(&session),
            Ok(true)
        );
    }

    #[test]
    fn malformed_headers_are_protocol_errors() {
        let limits = FrameLimits::default();
        let mut header = FrameHeader::msg(0);
        header.flags = 0x80;
        assert_eq!(
            header.validate(&limits),
            Err(FrameError::UnknownFlags(0x80))
        );
        header.flags = FLAG_ZSTD;
        assert_eq!(
            header.validate(&limits),
            Err(FrameError::CompressionNotNegotiated)
        );
        header.flags = FLAG_FIN;
        assert_eq!(header.validate(&limits), Err(FrameError::FinOnMessage));
        assert_eq!(
            FrameHeader::data(0, 1, false).validate(&limits),
            Err(FrameError::DataOnChannelZero)
        );
        let mut on_channel = FrameHeader::msg(1);
        on_channel.channel = 4;
        assert_eq!(
            on_channel.validate(&limits),
            Err(FrameError::MessageOnChannel(4))
        );
        let mut bytes = FrameHeader::msg(0).encode();
        bytes[7] = 1;
        assert_eq!(
            FrameHeader::decode(&bytes),
            Err(FrameError::ReservedNotZero)
        );
        assert_eq!(
            FrameError::UnknownFlags(0x80).goodbye_code(),
            GoodbyeCode::ProtocolError
        );
    }

    #[test]
    fn websocket_messages_must_match_their_header_length() {
        let limits = FrameLimits::default();
        let mut message = encode_msg(b"abc");
        assert_eq!(
            parse_message(&message, &limits),
            Ok(Some(Frame::Msg(b"abc".to_vec())))
        );
        message.push(0);
        assert_eq!(
            parse_message(&message, &limits),
            Err(FrameError::LengthMismatch {
                header: 3,
                actual: 4
            })
        );
        assert_eq!(parse_message(&[0; 5], &limits), Err(FrameError::Truncated));
    }

    #[test]
    fn prefaces_are_checked() {
        assert_eq!(check_preface(&PREFACE), PrefaceCheck::Match);
        let mut other = PREFACE;
        other[5] = 9;
        assert_eq!(check_preface(&other), PrefaceCheck::OtherMajor(9));
        assert_eq!(check_preface(b"GET / HT"), PrefaceCheck::NotWaku);
    }
}
