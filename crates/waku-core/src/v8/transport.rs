//! The two v8 transports under one reader/writer pair (RFC 0003 §1.4):
//!
//! - a local stream (Unix socket, named pipe): the 8-byte preface, then
//!   12-byte-header frames in both directions, always MessagePack;
//! - a WebSocket on `/v8`: one frame per binary message, or a JSON message
//!   per text message.

use std::io;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufWriter};
use tokio::io::{ReadHalf, WriteHalf};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{self, Message};
use waku_protocol::v8::frame::{self, Frame, FrameDecoder, FrameError, FrameHeader, FrameLimits};
use waku_protocol::v8::{Encoding, FRAME_HEADER_BYTES};

/// Something read from the peer.
pub(crate) enum Incoming {
    /// An encoded message and its encoding.
    Message(Vec<u8>, Encoding),
    Data {
        channel: u32,
        fin: bool,
        body: Vec<u8>,
    },
}

/// Something to write to the peer.
pub(crate) enum Outgoing {
    /// An encoded message body.
    Message(Vec<u8>),
    Data {
        channel: u32,
        fin: bool,
        body: Vec<u8>,
    },
}

pub(crate) enum ReadError {
    Frame(FrameError),
    Io(io::Error),
    WebSocket(tungstenite::Error),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Frame(error) => error.fmt(f),
            Self::Io(error) => error.fmt(f),
            Self::WebSocket(error) => error.fmt(f),
        }
    }
}

const READ_CHUNK: usize = 64 * 1024;

pub(crate) enum FrameReader<S> {
    Local {
        read: ReadHalf<S>,
        decoder: FrameDecoder,
        chunk: Box<[u8]>,
    },
    WebSocket {
        stream: SplitStream<WebSocketStream<S>>,
        limits: FrameLimits,
    },
}

pub(crate) enum FrameWriter<S> {
    Local(BufWriter<WriteHalf<S>>),
    WebSocket {
        sink: SplitSink<WebSocketStream<S>, Message>,
        /// Messages go as JSON text; DATA is binary either way.
        json: bool,
    },
}

/// Split a local stream that has exchanged prefaces.
pub(crate) fn local<S>(stream: S) -> (FrameReader<S>, FrameWriter<S>)
where
    S: AsyncRead + AsyncWrite,
{
    let (read, write) = tokio::io::split(stream);
    (
        FrameReader::Local {
            read,
            decoder: FrameDecoder::new(FrameLimits::handshake()),
            chunk: vec![0; READ_CHUNK].into_boxed_slice(),
        },
        FrameWriter::Local(BufWriter::new(write)),
    )
}

pub(crate) fn websocket<S>(socket: WebSocketStream<S>) -> (FrameReader<S>, FrameWriter<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (sink, stream) = socket.split();
    (
        FrameReader::WebSocket {
            stream,
            limits: FrameLimits::handshake(),
        },
        FrameWriter::WebSocket { sink, json: false },
    )
}

impl<S: AsyncRead + AsyncWrite + Unpin> FrameReader<S> {
    pub(crate) fn set_limits(&mut self, new: FrameLimits) {
        match self {
            Self::Local { decoder, .. } => decoder.set_limits(new),
            Self::WebSocket { limits, .. } => *limits = new,
        }
    }

    /// The next message or DATA frame; `None` at a clean end of stream.
    /// Frames of unknown kinds are skipped.
    pub(crate) async fn next(&mut self) -> Result<Option<Incoming>, ReadError> {
        match self {
            Self::Local {
                read,
                decoder,
                chunk,
            } => loop {
                if let Some(frame) = decoder.next_frame().map_err(ReadError::Frame)? {
                    return Ok(Some(incoming(frame)));
                }
                let read = read.read(chunk).await.map_err(ReadError::Io)?;
                if read == 0 {
                    return Ok(None);
                }
                decoder.extend(&chunk[..read]);
            },
            Self::WebSocket { stream, limits } => loop {
                let message = match stream.next().await {
                    None => return Ok(None),
                    Some(Err(
                        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed,
                    )) => return Ok(None),
                    Some(Err(error)) => return Err(ReadError::WebSocket(error)),
                    Some(Ok(message)) => message,
                };
                match message {
                    Message::Text(text) => {
                        if text.len() > limits.max_message_bytes as usize {
                            return Err(ReadError::Frame(FrameError::TooLarge {
                                kind: frame::KIND_MSG,
                                len: text.len() as u32,
                                limit: limits.max_message_bytes,
                            }));
                        }
                        return Ok(Some(Incoming::Message(
                            text.as_bytes().to_vec(),
                            Encoding::Json,
                        )));
                    }
                    Message::Binary(bytes) => {
                        match frame::parse_message(&bytes, limits).map_err(ReadError::Frame)? {
                            Some(frame) => return Ok(Some(incoming(frame))),
                            None => continue,
                        }
                    }
                    Message::Close(_) => return Ok(None),
                    // WebSocket pings are answered by tungstenite; they still
                    // count as traffic for the caller's idle timer.
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
                }
            },
        }
    }
}

fn incoming(frame: Frame) -> Incoming {
    match frame {
        Frame::Msg(body) => Incoming::Message(body, Encoding::MessagePack),
        Frame::Data { channel, fin, body } => Incoming::Data { channel, fin, body },
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> FrameWriter<S> {
    /// Fix the message encoding once `hello` has chosen it.
    pub(crate) fn set_encoding(&mut self, encoding: Encoding) {
        if let Self::WebSocket { json, .. } = self {
            *json = encoding == Encoding::Json;
        }
    }

    /// Queue one frame; [`Self::flush`] sends what is queued.
    pub(crate) async fn feed(&mut self, outgoing: Outgoing) -> io::Result<()> {
        match self {
            Self::Local(write) => {
                let (header, body) = match &outgoing {
                    Outgoing::Message(body) => (FrameHeader::msg(body.len()), body),
                    Outgoing::Data { channel, fin, body } => {
                        (FrameHeader::data(*channel, body.len(), *fin), body)
                    }
                };
                let header: [u8; FRAME_HEADER_BYTES] = header.encode();
                write.write_all(&header).await?;
                write.write_all(body).await
            }
            Self::WebSocket { sink, json } => {
                let message = match outgoing {
                    Outgoing::Message(body) if *json => {
                        let text = String::from_utf8(body).map_err(|error| {
                            io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                        })?;
                        Message::Text(text.into())
                    }
                    Outgoing::Message(body) => Message::Binary(frame::encode_msg(&body).into()),
                    Outgoing::Data { channel, fin, body } => {
                        Message::Binary(frame::encode_data(channel, fin, &body).into())
                    }
                };
                sink.feed(message).await.map_err(websocket_error)
            }
        }
    }

    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Local(write) => write.flush().await,
            Self::WebSocket { sink, .. } => sink.flush().await.map_err(websocket_error),
        }
    }

    pub(crate) async fn close(&mut self) {
        match self {
            Self::Local(write) => {
                let _ = write.shutdown().await;
            }
            Self::WebSocket { sink, .. } => {
                let _ = sink.close().await;
            }
        }
    }
}

fn websocket_error(error: tungstenite::Error) -> io::Error {
    match error {
        tungstenite::Error::Io(error) => error,
        other => io::Error::other(other.to_string()),
    }
}
