//! The real [`HttpTransport`]: HTTP/1.1 over tokio with a keep-alive
//! connection pool, server-sent-event and WebSocket downlinks, timeouts and
//! cancellation by drop (RFC 0002 "Transports", #49, #52).
//!
//! It speaks to loopback services Waku drives instead of stdio CLIs: the
//! user's OpenCode service and a pooled DeepSeek Harness `dsh web`. The wire
//! rules are the ones `http_wire` learned on those servers, reused here:
//!
//! * a response ends at its own body boundary (`Content-Length` or the
//!   terminating chunk), never at EOF, because a keep-alive server never
//!   closes;
//! * an event stream is read through one buffered reader that owns the
//!   socket, so no bytes of the first event are lost after the head;
//! * a named SSE event (OpenCode's `effect/httpapi/stream/failure`) means the
//!   stream itself failed and ends the downlink with an error.
//!
//! Credentials come from the base URL's user info (`http://user:pass@host`)
//! and become one precomputed `Authorization: Basic` header that is never
//! logged. One [`HttpClient`] exists per base URL ([`HttpClient::shared`]);
//! every session talking to the same service leases connections from its
//! pool, and a request returns its connection only after reading a complete
//! response the server did not ask to close.
//!
//! Cancellation is by drop: dropping a request future or a downlink closes
//! its socket. Nothing here blocks a runtime thread.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine as _;
use futures_util::StreamExt as _;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use url::Url;

use super::transport::{Downlink, HttpMethod, HttpResponse, HttpTransport};
use crate::http_wire::{SseDecoder, SseFrame, http_response_is_complete, split_http_response};

/// Limits and budgets of one client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpOptions {
    pub connect_timeout: Duration,
    /// The whole exchange: lease, write, and the complete response. Generous
    /// because OpenCode copies a long session on fork for up to two minutes.
    pub request_timeout: Duration,
    /// A silent SSE downlink ends with `TimedOut` after this long. OpenCode
    /// heartbeats every 15 s, so 45 s tolerates two lost beats. WebSocket
    /// downlinks rely on the server's pings instead.
    pub stream_idle_timeout: Option<Duration>,
    /// Idle keep-alive connections kept per client.
    pub max_idle_connections: usize,
    /// An idle connection older than this is closed instead of reused.
    pub idle_connection_ttl: Duration,
    /// A larger response fails instead of growing memory without bound.
    pub max_response_bytes: usize,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(150),
            stream_idle_timeout: Some(Duration::from_secs(45)),
            max_idle_connections: 4,
            idle_connection_ttl: Duration::from_secs(30),
            max_response_bytes: 64 * 1024 * 1024,
        }
    }
}

/// An HTTP/1.1 client for one base URL.
pub struct HttpClient {
    host: String,
    port: u16,
    /// `host:port`, for the `Host` header and WebSocket URLs.
    authority: String,
    /// The base URL's path without its trailing slash; request paths are
    /// appended to it.
    base_path: String,
    /// `Basic …`, precomputed once. Never logged.
    authorization: Option<String>,
    options: HttpOptions,
    idle: Mutex<Vec<(TcpStream, Instant)>>,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpClient")
            .field("authority", &self.authority)
            .field("base_path", &self.base_path)
            .field("authorized", &self.authorization.is_some())
            .finish()
    }
}

type Registry = Mutex<HashMap<String, Weak<HttpClient>>>;

fn registry() -> &'static Registry {
    static CLIENTS: OnceLock<Registry> = OnceLock::new();
    CLIENTS.get_or_init(Default::default)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn protocol(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn timed_out(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, format!("{what} timed out"))
}

impl HttpClient {
    /// A client for `base` (`http://[user:password@]host:port[/prefix]`).
    pub fn new(base: Url, options: HttpOptions) -> io::Result<Self> {
        if base.scheme() != "http" {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("only plain http is supported, not {}", base.scheme()),
            ));
        }
        let host = base
            .host_str()
            .ok_or_else(|| invalid("the base URL has no host"))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        let port = base
            .port_or_known_default()
            .ok_or_else(|| invalid("the base URL has no port"))?;
        let authority = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        let authorization = (!base.username().is_empty() || base.password().is_some()).then(|| {
            let user = percent_decode(base.username());
            let password = percent_decode(base.password().unwrap_or_default());
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"))
            )
        });
        Ok(Self {
            host,
            port,
            authority,
            base_path: base.path().trim_end_matches('/').to_owned(),
            authorization,
            options,
            idle: Mutex::new(Vec::new()),
        })
    }

    /// The process-wide client for `base`, created on first use. Sessions
    /// against one service share its keep-alive pool; the client goes away
    /// with its last session.
    pub fn shared(base: Url, options: HttpOptions) -> io::Result<Arc<Self>> {
        let key = base.as_str().to_owned();
        let mut clients = registry().lock();
        clients.retain(|_, client| client.strong_count() > 0);
        if let Some(client) = clients.get(&key).and_then(Weak::upgrade)
            && client.options == options
        {
            return Ok(client);
        }
        let client = Arc::new(Self::new(base, options)?);
        clients.insert(key, Arc::downgrade(&client));
        Ok(client)
    }

    fn target(&self, path: &str) -> String {
        if path.starts_with('/') {
            format!("{}{path}", self.base_path)
        } else {
            format!("{}/{path}", self.base_path)
        }
    }

    fn authorization_header(&self) -> String {
        self.authorization
            .as_deref()
            .map(|value| format!("Authorization: {value}\r\n"))
            .unwrap_or_default()
    }

    async fn connect(&self) -> io::Result<TcpStream> {
        let stream = tokio::time::timeout(
            self.options.connect_timeout,
            TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .map_err(|_| timed_out("connecting"))??;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    /// An idle pooled connection that is still open, else a new one. The
    /// flag says whether it was reused (a stale reuse may be retried).
    async fn lease(&self) -> io::Result<(TcpStream, bool)> {
        loop {
            let candidate = self.idle.lock().pop();
            let Some((stream, parked)) = candidate else {
                return Ok((self.connect().await?, false));
            };
            if parked.elapsed() > self.options.idle_connection_ttl {
                continue;
            }
            // An idle connection the server closed reads EOF at once; one
            // that has unread bytes is out of sync. Either is dropped.
            let mut probe = [0_u8; 1];
            match stream.try_read(&mut probe) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok((stream, true));
                }
                _ => continue,
            }
        }
    }

    fn release(&self, stream: TcpStream) {
        let mut idle = self.idle.lock();
        if idle.len() < self.options.max_idle_connections {
            idle.push((stream, Instant::now()));
        }
    }

    fn request_head(&self, method: HttpMethod, target: &str, body: Option<&[u8]>) -> String {
        let content = match (method, body) {
            (_, Some(body)) => format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                body.len()
            ),
            // A bodiless POST still says so: some servers wait for a body
            // without it.
            (HttpMethod::Get, None) => String::new(),
            (_, None) => "Content-Length: 0\r\n".to_owned(),
        };
        format!(
            "{} {target} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\n{}{content}Connection: keep-alive\r\n\r\n",
            method.as_str(),
            self.authority,
            self.authorization_header(),
        )
    }

    async fn exchange(
        &self,
        stream: &mut TcpStream,
        head: &str,
        body: Option<&[u8]>,
    ) -> Result<crate::http_wire::ParsedResponse, ExchangeError> {
        let mut message = Vec::with_capacity(head.len() + body.map_or(0, <[u8]>::len));
        message.extend_from_slice(head.as_bytes());
        if let Some(body) = body {
            message.extend_from_slice(body);
        }
        stream
            .write_all(&message)
            .await
            .map_err(ExchangeError::Stale)?;
        stream.flush().await.map_err(ExchangeError::Stale)?;
        let mut response = Vec::new();
        let mut buffer = vec![0_u8; 16 * 1024];
        loop {
            if http_response_is_complete(&response)
                .map_err(|error| ExchangeError::Failed(protocol(error.to_string())))?
            {
                break;
            }
            let read = match stream.read(&mut buffer).await {
                Ok(read) => read,
                Err(error) if response.is_empty() => return Err(ExchangeError::Stale(error)),
                Err(error) => return Err(ExchangeError::Failed(error)),
            };
            if read == 0 {
                if response.is_empty() {
                    return Err(ExchangeError::Stale(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the server closed the connection before answering",
                    )));
                }
                // A body delimited by the connection close.
                let mut parsed = split_http_response(&response)
                    .map_err(|error| ExchangeError::Failed(protocol(error.to_string())))?;
                parsed.close = true;
                return Ok(parsed);
            }
            response.extend_from_slice(&buffer[..read]);
            if response.len() > self.options.max_response_bytes {
                return Err(ExchangeError::Failed(protocol(
                    "the response exceeds the transport's size limit",
                )));
            }
        }
        split_http_response(&response)
            .map_err(|error| ExchangeError::Failed(protocol(error.to_string())))
    }

    async fn request_inner(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<Value>,
    ) -> io::Result<HttpResponse> {
        let body = body
            .map(|body| serde_json::to_vec(&body))
            .transpose()
            .map_err(|error| invalid(error.to_string()))?;
        let head = self.request_head(method, &self.target(path), body.as_deref());
        // A pooled connection the server closed while idle can fail before
        // any byte of the answer; that one request is retried once on a
        // fresh connection. Nothing was processed, so this cannot repeat it.
        for attempt in 0..2 {
            let (mut stream, reused) = self.lease().await?;
            match self.exchange(&mut stream, &head, body.as_deref()).await {
                Ok(parsed) => {
                    if !parsed.close {
                        self.release(stream);
                    }
                    return Ok(HttpResponse {
                        status: parsed.status,
                        body: decode_body(&parsed.body),
                    });
                }
                Err(ExchangeError::Stale(_)) if reused && attempt == 0 => {}
                Err(ExchangeError::Stale(error) | ExchangeError::Failed(error)) => {
                    return Err(error);
                }
            }
        }
        Err(io::Error::other("the request could not be delivered"))
    }

    async fn open_event_stream(&self, path: &str) -> io::Result<Downlink> {
        let mut stream = self.connect().await?;
        let head = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\n{}Connection: keep-alive\r\n\r\n",
            self.target(path),
            self.authority,
            self.authorization_header(),
        );
        stream.write_all(head.as_bytes()).await?;
        stream.flush().await?;
        // Read exactly through the head; the rest of the buffer is body.
        let mut raw = Vec::new();
        let mut buffer = vec![0_u8; 8 * 1024];
        let header_end = loop {
            if let Some(end) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
                break end;
            }
            if raw.len() > 64 * 1024 {
                return Err(protocol("the event stream's response head is too large"));
            }
            let read = stream.read(&mut buffer).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the server closed the event stream during setup",
                ));
            }
            raw.extend_from_slice(&buffer[..read]);
        };
        let headers = String::from_utf8_lossy(&raw[..header_end]).into_owned();
        let status = headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|status| status.parse::<u16>().ok())
            .ok_or_else(|| protocol("the event stream answered an invalid status"))?;
        // An authenticated server answers 401 with a well-formed head and
        // then says nothing; without this check that is an idle stream.
        if !(200..300).contains(&status) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("the server refused the event stream with HTTP {status}"),
            ));
        }
        let chunked = headers.lines().skip(1).any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.trim().eq_ignore_ascii_case("transfer-encoding")
                    && value.to_ascii_lowercase().contains("chunked")
            })
        });
        let body = raw.split_off(header_end + 4);
        let reader = SseReader {
            stream,
            idle_timeout: self.options.stream_idle_timeout,
            dechunk: chunked.then(Dechunk::default),
            pending: body,
            text: Vec::new(),
            decoder: SseDecoder::default(),
            frames: VecDeque::new(),
            done: false,
        };
        Ok(Box::pin(futures_util::stream::unfold(
            reader,
            |mut reader| async move {
                let item = reader.next_value().await?;
                Some((item, reader))
            },
        )))
    }

    async fn open_websocket(&self, path: &str) -> io::Result<Downlink> {
        let stream = self.connect().await?;
        let url = format!("ws://{}{}", self.authority, self.target(path));
        let mut request = url
            .into_client_request()
            .map_err(|error| invalid(error.to_string()))?;
        if let Some(authorization) = &self.authorization {
            request.headers_mut().insert(
                "Authorization",
                HeaderValue::from_str(authorization).map_err(|error| invalid(error.to_string()))?,
            );
        }
        let (socket, _) = tokio::time::timeout(
            self.options.request_timeout,
            tokio_tungstenite::client_async(request, stream),
        )
        .await
        .map_err(|_| timed_out("the WebSocket handshake"))?
        .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Box::pin(futures_util::stream::unfold(
            Some(socket),
            |socket| async move {
                let mut socket = socket?;
                loop {
                    let message = socket.next().await?;
                    let value = match message {
                        Ok(Message::Text(text)) => serde_json::from_str::<Value>(text.as_str()),
                        Ok(Message::Binary(bytes)) => serde_json::from_slice::<Value>(&bytes),
                        Ok(Message::Close(_)) => return None,
                        // Pings are answered by the protocol layer.
                        Ok(_) => continue,
                        Err(error) => {
                            return Some((Err(io::Error::other(error.to_string())), None));
                        }
                    };
                    // A message that is not JSON is not an event; skip it.
                    if let Ok(value) = value {
                        return Some((Ok(value), Some(socket)));
                    }
                }
            },
        )))
    }
}

enum ExchangeError {
    /// Failed before any byte of the answer: safe to retry on a fresh
    /// connection when this one was reused.
    Stale(io::Error),
    Failed(io::Error),
}

/// An empty body is `null`; a body that is not JSON (a plain-text error
/// page) is kept as a string so the driver can still report it.
fn decode_body(body: &[u8]) -> Value {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Value::Null;
    }
    serde_json::from_slice(body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()))
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let Some(byte) = std::str::from_utf8(&bytes[index + 1..index + 3])
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
        {
            out.push(byte);
            index += 3;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Incremental `Transfer-Encoding: chunked` decoding.
#[derive(Default)]
struct Dechunk {
    /// Bytes left in the current chunk, then its CRLF.
    remaining: usize,
    in_chunk: bool,
    finished: bool,
}

impl Dechunk {
    /// Moves decoded body bytes from `raw` into `out`; leaves an incomplete
    /// size line in `raw`.
    fn feed(&mut self, raw: &mut Vec<u8>, out: &mut Vec<u8>) -> io::Result<()> {
        let mut consumed = 0;
        loop {
            if self.finished {
                consumed = raw.len();
                break;
            }
            let rest = &raw[consumed..];
            if self.in_chunk {
                if self.remaining > 0 {
                    let take = self.remaining.min(rest.len());
                    out.extend_from_slice(&rest[..take]);
                    self.remaining -= take;
                    consumed += take;
                    if self.remaining > 0 {
                        break;
                    }
                }
                let rest = &raw[consumed..];
                if rest.len() < 2 {
                    break;
                }
                if &rest[..2] != b"\r\n" {
                    return Err(protocol("the event stream sent an invalid chunk"));
                }
                consumed += 2;
                self.in_chunk = false;
                continue;
            }
            let Some(line_end) = rest.windows(2).position(|window| window == b"\r\n") else {
                break;
            };
            let size_text = String::from_utf8_lossy(&rest[..line_end]);
            let size =
                usize::from_str_radix(size_text.split(';').next().unwrap_or_default().trim(), 16)
                    .map_err(|_| protocol("the event stream sent an invalid chunk size"))?;
            consumed += line_end + 2;
            if size == 0 {
                self.finished = true;
                continue;
            }
            self.remaining = size;
            self.in_chunk = true;
        }
        raw.drain(..consumed);
        Ok(())
    }
}

/// Reads server-sent events off one connection.
struct SseReader {
    stream: TcpStream,
    idle_timeout: Option<Duration>,
    dechunk: Option<Dechunk>,
    /// Socket bytes not yet decoded.
    pending: Vec<u8>,
    /// Body bytes not yet split into lines.
    text: Vec<u8>,
    decoder: SseDecoder,
    frames: VecDeque<SseFrame>,
    done: bool,
}

impl SseReader {
    fn decode(&mut self) -> io::Result<()> {
        match &mut self.dechunk {
            Some(dechunk) => dechunk.feed(&mut self.pending, &mut self.text)?,
            None => self.text.append(&mut self.pending),
        }
        while let Some(end) = self.text.iter().position(|byte| *byte == b'\n') {
            let line = self.text.drain(..=end).collect::<Vec<_>>();
            if let Some(frame) = self.decoder.push_line(&String::from_utf8_lossy(&line)) {
                self.frames.push_back(frame);
            }
        }
        Ok(())
    }

    async fn next_value(&mut self) -> Option<io::Result<Value>> {
        loop {
            if self.done {
                return None;
            }
            while let Some(frame) = self.frames.pop_front() {
                match frame {
                    SseFrame::Data(data) => {
                        // Every event is one JSON envelope; anything else is
                        // not an event and is skipped.
                        if let Ok(value) = serde_json::from_str::<Value>(&data) {
                            return Some(Ok(value));
                        }
                    }
                    SseFrame::Named { event, .. } => {
                        self.done = true;
                        return Some(Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            format!("the server reported a stream failure ({event})"),
                        )));
                    }
                    SseFrame::Comment => {}
                }
            }
            if let Err(error) = self.decode() {
                self.done = true;
                return Some(Err(error));
            }
            if !self.frames.is_empty() {
                continue;
            }
            let mut buffer = [0_u8; 16 * 1024];
            let read = match self.idle_timeout {
                Some(idle) => match tokio::time::timeout(idle, self.stream.read(&mut buffer)).await
                {
                    Ok(read) => read,
                    Err(_) => {
                        self.done = true;
                        return Some(Err(timed_out("the event stream")));
                    }
                },
                None => self.stream.read(&mut buffer).await,
            };
            match read {
                Ok(0) => {
                    self.done = true;
                    return None;
                }
                Ok(read) => self.pending.extend_from_slice(&buffer[..read]),
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

#[async_trait]
impl HttpTransport for HttpClient {
    async fn request(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<Value>,
    ) -> io::Result<HttpResponse> {
        tokio::time::timeout(
            self.options.request_timeout,
            self.request_inner(method, path, body),
        )
        .await
        .map_err(|_| timed_out(&format!("{} {path}", method.as_str())))?
    }

    async fn stream(&self, path: &str) -> io::Result<Downlink> {
        tokio::time::timeout(self.options.request_timeout, self.open_event_stream(path))
            .await
            .map_err(|_| timed_out("opening the event stream"))?
    }

    async fn websocket(&self, path: &str) -> io::Result<Downlink> {
        self.open_websocket(path).await
    }
}

#[cfg(test)]
mod tests;
