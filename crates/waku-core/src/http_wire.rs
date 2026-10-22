//! Minimal blocking HTTP/1.1 and server-sent-event wire for the providers Waku
//! drives over a local HTTP server rather than stdio: OpenCode's background
//! service and DeepSeek Harness's RPC routes.
//!
//! A local server sets the same two traps for every client, so the wire lives
//! here once:
//!
//! * A keep-alive server never EOFs, so response completion is detected at the
//!   protocol's own body boundary (`Content-Length` or the terminating chunk).
//!   Waiting for EOF turns a perfectly valid response into a macOS `EAGAIN`
//!   once the socket timeout elapses.
//! * An event stream's response head must be consumed one byte at a time. A
//!   `BufReader` reads ahead into the first event and loses those bytes when it
//!   is dropped.
//!
//! Everything here blocks, so every caller must already be off the UI thread.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use base64::Engine as _;
use serde_json::Value;

/// Where a request goes, and how it authenticates.
///
/// A private loopback server can serve unauthenticated; OpenCode's background
/// service demands HTTP Basic with the password from its registration file.
/// Precompute the header once so it is never rebuilt per request — and never
/// log it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Endpoint {
    pub host: String,
    pub port: u16,
    pub auth: Option<String>,
}

impl Endpoint {
    /// An unauthenticated loopback server.
    pub(crate) fn local(port: u16) -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port,
            auth: None,
        }
    }

    /// Parses a registration URL and precomputes its Basic credential.
    pub(crate) fn basic(url: &str, user: &str, password: &str) -> anyhow::Result<Self> {
        let parsed = url::Url::parse(url).with_context(|| format!("invalid server URL {url}"))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow!("server URL {url} has no host"))?
            .to_owned();
        let port = parsed
            .port_or_known_default()
            .ok_or_else(|| anyhow!("server URL {url} has no port"))?;
        let credential =
            base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        Ok(Self {
            host,
            port,
            auth: Some(format!("Basic {credential}")),
        })
    }

    pub(crate) fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Sends one request and returns the decoded JSON body.
///
/// A 204/205/304 — or any empty success body — answers `Value::Null`, which is
/// what every acknowledgement-only route returns.
pub(crate) fn request_json(
    endpoint: &Endpoint,
    method: &str,
    path: &str,
    body: Option<&Value>,
    timeout: Duration,
) -> anyhow::Result<Value> {
    let body = body.map(serde_json::to_vec).transpose()?;
    let response = http_request(endpoint, method, path, body.as_deref(), timeout)?;
    if response.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&response)
        .with_context(|| format!("OpenCode returned invalid JSON for {method} {path}"))
}

/// Percent-encodes one path segment or query component, keeping only RFC 3986
/// unreserved characters. Session ids, MCP server names and directories all
/// pass through here, and a deepObject key's brackets must arrive as
/// `%5B`/`%5D`.
pub(crate) fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

pub(crate) fn http_request(
    endpoint: &Endpoint,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    timeout: Duration,
) -> anyhow::Result<Vec<u8>> {
    let address = endpoint.address();
    let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .with_context(|| format!("could not connect to OpenCode on {address}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let body = body.unwrap_or_default();
    let authorization = endpoint
        .auth
        .as_deref()
        .map(|auth| format!("Authorization: {auth}\r\n"))
        .unwrap_or_default();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nAccept: application/json\r\n{authorization}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;

    let mut response = Vec::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        // HTTP/1.1 connections are allowed to stay alive after a complete
        // response, even when the client asks to close. Waiting for EOF made a
        // valid OpenCode response end as macOS EAGAIN once the socket timeout
        // elapsed. Stop at the protocol's own body boundary instead.
        if http_response_is_complete(&response)? {
            break;
        }
        let read = stream
            .read(&mut buffer)
            .with_context(|| format!("failed reading OpenCode response for {method} {path}"))?;
        if read == 0 {
            break;
        }
        response.extend_from_slice(&buffer[..read]);
    }
    parse_http_response(&response)
}

pub(crate) fn http_response_is_complete(response: &[u8]) -> anyhow::Result<bool> {
    let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Ok(false);
    };
    let headers = std::str::from_utf8(&response[..header_end])?;
    let body = &response[header_end + 4..];
    if header_value(headers, "transfer-encoding").is_some_and(|value| {
        value
            .split(',')
            .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
    }) {
        return chunked_body_is_complete(body);
    }
    if let Some(length) = header_value(headers, "content-length") {
        let length = length
            .trim()
            .parse::<usize>()
            .context("OpenCode returned an invalid HTTP content length")?;
        return Ok(body.len() >= length);
    }

    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok());
    Ok(status.is_some_and(|status| matches!(status, 204 | 205 | 304)))
}

fn header_value<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().skip(1).find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(name).then_some(value.trim())
    })
}

fn chunked_body_is_complete(mut input: &[u8]) -> anyhow::Result<bool> {
    loop {
        let Some(line_end) = input.windows(2).position(|window| window == b"\r\n") else {
            return Ok(false);
        };
        let size_text = std::str::from_utf8(&input[..line_end])?
            .split(';')
            .next()
            .unwrap_or_default();
        let size = usize::from_str_radix(size_text.trim(), 16)
            .context("OpenCode returned an invalid HTTP chunk size")?;
        input = &input[line_end + 2..];
        if size == 0 {
            return Ok(true);
        }
        if input.len() < size + 2 {
            return Ok(false);
        }
        if &input[size..size + 2] != b"\r\n" {
            bail!("OpenCode returned an invalid chunked response");
        }
        input = &input[size + 2..];
    }
}

fn parse_http_response(response: &[u8]) -> anyhow::Result<Vec<u8>> {
    let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        bail!("OpenCode returned an invalid HTTP response");
    };
    let headers = std::str::from_utf8(&response[..header_end])?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| anyhow!("OpenCode returned an invalid HTTP status"))?;
    let body = &response[header_end + 4..];
    let body = if headers.lines().any(|line| {
        line.eq_ignore_ascii_case("transfer-encoding: chunked")
            || line
                .to_ascii_lowercase()
                .starts_with("transfer-encoding: chunked")
    }) {
        decode_chunked(body)?
    } else {
        body.to_vec()
    };
    if !(200..300).contains(&status) {
        let detail = String::from_utf8_lossy(&body);
        bail!("OpenCode session request failed with HTTP {status}: {detail}");
    }
    Ok(body)
}

/// One complete HTTP/1.1 response, its body already de-chunked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedResponse {
    pub status: u16,
    pub body: Vec<u8>,
    /// The server asked to close the connection after this response.
    pub close: bool,
}

/// Splits a complete response into status, body and connection reuse,
/// without judging the status: the async transport (`driver::v2::http`)
/// hands a non-2xx answer to the driver, which reads the server's own error
/// body.
pub(crate) fn split_http_response(response: &[u8]) -> anyhow::Result<ParsedResponse> {
    let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        bail!("the server returned an invalid HTTP response");
    };
    let headers = std::str::from_utf8(&response[..header_end])?;
    let status_line = headers.lines().next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| anyhow!("the server returned an invalid HTTP status"))?;
    let body = &response[header_end + 4..];
    let chunked = header_value(headers, "transfer-encoding").is_some_and(|value| {
        value
            .split(',')
            .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
    });
    let body = if chunked {
        decode_chunked(body)?
    } else {
        match header_value(headers, "content-length")
            .and_then(|length| length.trim().parse::<usize>().ok())
        {
            Some(length) if body.len() >= length => body[..length].to_vec(),
            _ => body.to_vec(),
        }
    };
    let close = status_line.starts_with("HTTP/1.0")
        || header_value(headers, "connection").is_some_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("close"))
        });
    Ok(ParsedResponse {
        status,
        body,
        close,
    })
}

fn decode_chunked(mut input: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut output = Vec::new();
    loop {
        let Some(line_end) = input.windows(2).position(|window| window == b"\r\n") else {
            bail!("OpenCode returned an invalid chunked response");
        };
        let size_text = std::str::from_utf8(&input[..line_end])?
            .split(';')
            .next()
            .unwrap_or_default();
        let size = usize::from_str_radix(size_text.trim(), 16)
            .context("OpenCode returned an invalid HTTP chunk size")?;
        input = &input[line_end + 2..];
        if size == 0 {
            break;
        }
        if input.len() < size + 2 || &input[size..size + 2] != b"\r\n" {
            bail!("OpenCode returned a truncated HTTP chunk");
        }
        output.extend_from_slice(&input[..size]);
        input = &input[size + 2..];
    }
    Ok(output)
}

/// One parsed server-sent-event frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SseFrame {
    /// A frame with only `data:` lines, which is every ordinary event.
    Data(String),
    /// A frame carrying an `event:` field. OpenCode uses this solely to
    /// report that the stream itself failed
    /// (`event: effect/httpapi/stream/failure`), so a reader that ignores the
    /// field sits blind on a dead stream until its read timeout fires.
    Named { event: String, data: String },
    /// A `:`-prefixed comment. OpenCode's service heartbeats with `: heartbeat`.
    Comment,
}

/// Incremental server-sent-event framing: feed it one line at a time (a
/// trailing `\r\n` or `\n` is ignored) and it answers a frame whenever a
/// blank line dispatches one. The async transport (`driver::v2::http`) reads
/// event streams through it.
#[derive(Debug, Default)]
pub(crate) struct SseDecoder {
    event: String,
    data: String,
    saw_comment: bool,
}

impl SseDecoder {
    pub(crate) fn push_line(&mut self, line: &str) -> Option<SseFrame> {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            // A blank line dispatches the frame accumulated so far.
            if !self.data.is_empty() || !self.event.is_empty() {
                let payload = std::mem::take(&mut self.data);
                let name = std::mem::take(&mut self.event);
                self.saw_comment = false;
                return Some(if name.is_empty() {
                    SseFrame::Data(payload)
                } else {
                    SseFrame::Named {
                        event: name,
                        data: payload,
                    }
                });
            }
            if std::mem::take(&mut self.saw_comment) {
                return Some(SseFrame::Comment);
            }
            return None;
        }
        if trimmed.starts_with(':') {
            self.saw_comment = true;
            return None;
        }
        let (field, value) = match trimmed.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (trimmed, ""),
        };
        match field {
            // Multiple `data:` lines in one frame are joined with newlines.
            "data" => {
                if !self.data.is_empty() {
                    self.data.push('\n');
                }
                self.data.push_str(value);
            }
            "event" => {
                self.event.clear();
                self.event.push_str(value);
            }
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(input: &str) -> Vec<SseFrame> {
        let mut decoder = SseDecoder::default();
        input
            .split_inclusive('\n')
            .filter_map(|line| decoder.push_line(line))
            .collect()
    }

    #[test]
    fn heartbeat_comments_are_reported_separately_from_data() {
        assert_eq!(
            frames(": heartbeat\n\ndata: {\"a\":1}\n\n"),
            vec![SseFrame::Comment, SseFrame::Data("{\"a\":1}".to_owned())]
        );
    }

    #[test]
    fn multiline_data_fields_are_joined() {
        assert_eq!(
            frames("data: {\"a\":\ndata: 1}\n\n"),
            vec![SseFrame::Data("{\"a\":\n1}".to_owned())]
        );
    }

    /// `GET /api/event` reports a server-side stream failure as a NAMED event
    /// whose data is an Effect cause. Dropping the name loses the only signal
    /// that the stream is dead.
    #[test]
    fn named_events_preserve_their_event_field() {
        assert_eq!(
            frames("event: effect/httpapi/stream/failure\ndata: {\"_tag\":\"Fail\"}\n\n"),
            vec![SseFrame::Named {
                event: "effect/httpapi/stream/failure".to_owned(),
                data: "{\"_tag\":\"Fail\"}".to_owned(),
            }]
        );
    }

    #[test]
    fn parses_content_length_and_chunked_http_responses() {
        assert_eq!(
            parse_http_response(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").unwrap(),
            b"{}"
        );
        assert_eq!(
            parse_http_response(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n{\"id\r\n4\r\n\":1}\r\n0\r\n\r\n"
            )
            .unwrap(),
            b"{\"id\":1}"
        );
    }

    #[test]
    fn detects_complete_http_bodies_without_waiting_for_connection_close() {
        assert!(
            !http_response_is_complete(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{").unwrap()
        );
        assert!(
            http_response_is_complete(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").unwrap()
        );
        assert!(
            !http_response_is_complete(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n{}"
            )
            .unwrap()
        );
        assert!(
            http_response_is_complete(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n"
            )
            .unwrap()
        );
        assert!(
            http_response_is_complete(b"HTTP/1.1 204 No Content\r\nConnection: keep-alive\r\n\r\n")
                .unwrap()
        );
    }

    #[test]
    fn basic_endpoint_precomputes_its_credential() {
        let endpoint = Endpoint::basic("http://127.0.0.1:49374", "opencode", "secret").unwrap();
        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 49374);
        assert_eq!(endpoint.address(), "127.0.0.1:49374");
        // base64("opencode:secret")
        assert_eq!(endpoint.auth.as_deref(), Some("Basic b3BlbmNvZGU6c2VjcmV0"));
    }

    #[test]
    fn local_endpoint_is_unauthenticated_loopback() {
        let endpoint = Endpoint::local(1234);
        assert_eq!(endpoint.address(), "127.0.0.1:1234");
        assert!(endpoint.auth.is_none());
    }
}
