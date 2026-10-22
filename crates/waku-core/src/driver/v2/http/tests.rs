//! The real HTTP transport against small loopback servers.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::SinkExt as _;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use super::*;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Reads one request (head plus `Content-Length` body) off `stream`.
async fn read_request(stream: &mut BufReader<TcpStream>) -> Option<(String, String)> {
    let mut head = String::new();
    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        if line == "\r\n" {
            break;
        }
        head.push_str(&line);
    }
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.ok()?;
    Some((head, String::from_utf8(body).unwrap()))
}

fn client(port: u16, options: HttpOptions) -> HttpClient {
    HttpClient::new(
        Url::parse(&format!("http://127.0.0.1:{port}")).unwrap(),
        options,
    )
    .unwrap()
}

#[test]
fn keep_alive_connections_are_leased_and_reused() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut socket = BufReader::new(socket);
                    while let Some((head, body)) = read_request(&mut socket).await {
                        let reply = if head.starts_with("POST /api/echo") {
                            body
                        } else {
                            "{\"ok\":true}".to_owned()
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{reply}",
                            reply.len()
                        );
                        socket.get_mut().write_all(response.as_bytes()).await.unwrap();
                    }
                });
            }
        });
        let client = client(port, HttpOptions::default());
        let first = client.request(HttpMethod::Get, "/api/info", None).await.unwrap();
        assert_eq!(first, HttpResponse { status: 200, body: json!({"ok": true}) });
        let echoed = client
            .request(HttpMethod::Post, "/api/echo", Some(json!({"text": "hi"})))
            .await
            .unwrap();
        assert_eq!(echoed.body, json!({"text": "hi"}));
        assert_eq!(accepted.load(Ordering::SeqCst), 1, "the second request reused the connection");
    });
}

#[test]
fn chunked_bodies_errors_and_empty_bodies_decode() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            while let Some((head, _)) = read_request(&mut socket).await {
                let response = if head.starts_with("GET /chunked") {
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n{\"id\r\n4\r\n\":1}\r\n0\r\n\r\n".to_owned()
                } else if head.starts_with("GET /missing") {
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 42\r\n\r\n{\"_tag\":\"NotFound\",\"message\":\"no session\"}".to_owned()
                } else {
                    "HTTP/1.1 204 No Content\r\n\r\n".to_owned()
                };
                socket.get_mut().write_all(response.as_bytes()).await.unwrap();
            }
        });
        let client = client(port, HttpOptions::default());
        let chunked = client.request(HttpMethod::Get, "/chunked", None).await.unwrap();
        assert_eq!(chunked.body, json!({"id": 1}));
        let missing = client.request(HttpMethod::Get, "/missing", None).await.unwrap();
        assert_eq!(missing.status, 404);
        assert_eq!(missing.body["_tag"], "NotFound");
        let empty = client.request(HttpMethod::Post, "/ack", None).await.unwrap();
        assert_eq!(empty, HttpResponse { status: 204, body: Value::Null });
    });
}

#[test]
fn credentials_in_the_base_url_become_basic_auth_and_bodiless_posts_say_so() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let (head, _) = read_request(&mut socket).await.unwrap();
            socket
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                .await
                .unwrap();
            head
        });
        let client = HttpClient::new(
            Url::parse(&format!(
                "http://opencode:s%40cret@127.0.0.1:{port}/prefix/"
            ))
            .unwrap(),
            HttpOptions::default(),
        )
        .unwrap();
        client
            .request(HttpMethod::Post, "/api/session/ses_1/interrupt", None)
            .await
            .unwrap();
        let head = server.await.unwrap();
        assert!(
            head.starts_with("POST /prefix/api/session/ses_1/interrupt HTTP/1.1\r\n"),
            "{head}"
        );
        // base64("opencode:s@cret")
        assert!(
            head.contains("Authorization: Basic b3BlbmNvZGU6c0BjcmV0\r\n"),
            "{head}"
        );
        assert!(head.contains("Content-Length: 0\r\n"), "{head}");
        assert!(!format!("{client:?}").contains("cret"));
    });
}

#[test]
fn a_pooled_connection_the_server_closed_is_retried_once() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                // One answer per connection, then a silent close (no
                // `Connection: close`), the way an idle timeout looks.
                let mut socket = BufReader::new(socket);
                if read_request(&mut socket).await.is_some() {
                    socket
                        .get_mut()
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                        .await
                        .unwrap();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        let client = client(port, HttpOptions::default());
        for _ in 0..3 {
            let response = client
                .request(HttpMethod::Post, "/x", Some(json!({})))
                .await
                .unwrap();
            assert_eq!(response.status, 200);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

#[test]
fn requests_time_out() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let client = client(
            port,
            HttpOptions {
                request_timeout: Duration::from_millis(100),
                ..HttpOptions::default()
            },
        );
        let error = client
            .request(HttpMethod::Get, "/slow", None)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    });
}

#[test]
fn event_streams_dechunk_skip_heartbeats_and_end_on_a_named_failure() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let (head, _) = read_request(&mut socket).await.unwrap();
            assert!(head.contains("Accept: text/event-stream"));
            let socket = socket.get_mut();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await
                .unwrap();
            // An event split across two chunks, a heartbeat, a second event.
            for piece in [
                "data: {\"type\":\"server.con",
                "nected\"}\n\n",
                ": heartbeat\n\n",
                "data: {\"type\":\"session.idle\"}\n\n",
                "event: effect/httpapi/stream/failure\ndata: {}\n\n",
            ] {
                let chunk = format!("{:x}\r\n{piece}\r\n", piece.len());
                socket.write_all(chunk.as_bytes()).await.unwrap();
                socket.flush().await.unwrap();
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let client = client(port, HttpOptions::default());
        let mut events = client.stream("/api/event").await.unwrap();
        assert_eq!(
            events.next().await.unwrap().unwrap(),
            json!({"type": "server.connected"})
        );
        assert_eq!(
            events.next().await.unwrap().unwrap(),
            json!({"type": "session.idle"})
        );
        let failure = events.next().await.unwrap().unwrap_err();
        assert_eq!(failure.kind(), io::ErrorKind::ConnectionAborted);
        assert!(events.next().await.is_none());
    });
}

#[test]
fn a_silent_event_stream_times_out_and_a_refused_one_fails() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            // First: a healthy head, then silence. Second: 401.
            let (socket, _) = listener.accept().await.unwrap();
            let mut first = BufReader::new(socket);
            read_request(&mut first).await.unwrap();
            first
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
                .await
                .unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            let mut second = BufReader::new(socket);
            read_request(&mut second).await.unwrap();
            second
                .get_mut()
                .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
            drop(first);
        });
        let client = client(
            port,
            HttpOptions {
                stream_idle_timeout: Some(Duration::from_millis(100)),
                ..HttpOptions::default()
            },
        );
        let mut events = client.stream("/api/event").await.unwrap();
        let error = events.next().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(events.next().await.is_none());
        let refused = client.stream("/api/event").await.err().unwrap();
        assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied);
    });
}

#[test]
fn dropping_a_downlink_closes_its_connection() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (closed, mut closed_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            read_request(&mut socket).await.unwrap();
            socket
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
                .await
                .unwrap();
            let mut rest = Vec::new();
            let _ = socket.read_to_end(&mut rest).await;
            let _ = closed.send(());
        });
        let client = client(port, HttpOptions::default());
        let events = client.stream("/api/event").await.unwrap();
        assert!(closed_rx.try_recv().is_err());
        drop(events);
        tokio::time::timeout(Duration::from_secs(2), closed_rx)
            .await
            .expect("the server saw the close")
            .unwrap();
    });
}

#[test]
fn websocket_downlinks_yield_json_messages_until_close() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            for text in [
                "{\"type\":\"server-request\",\"payload\":{\"sessionId\":\"s1\"}}",
                "not json",
                "{\"type\":\"server-request\",\"payload\":{\"type\":\"session/jobs\"}}",
            ] {
                socket.send(Message::text(text)).await.unwrap();
            }
            socket.close(None).await.unwrap();
        });
        let client = client(port, HttpOptions::default());
        let mut events = client.websocket("/api/events.mux").await.unwrap();
        assert_eq!(
            events.next().await.unwrap().unwrap()["payload"]["sessionId"],
            "s1"
        );
        assert_eq!(
            events.next().await.unwrap().unwrap()["payload"]["type"],
            "session/jobs"
        );
        assert!(events.next().await.is_none());
    });
}

#[test]
fn clients_are_shared_per_base_url() {
    let base = Url::parse("http://127.0.0.1:1/").unwrap();
    let first = HttpClient::shared(base.clone(), HttpOptions::default()).unwrap();
    let second = HttpClient::shared(base, HttpOptions::default()).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    let other = HttpClient::shared(
        Url::parse("http://127.0.0.1:2/").unwrap(),
        HttpOptions::default(),
    )
    .unwrap();
    assert!(!Arc::ptr_eq(&first, &other));
    assert!(
        HttpClient::new(
            Url::parse("https://example.invalid").unwrap(),
            HttpOptions::default()
        )
        .is_err()
    );
}

#[test]
fn chunk_decoding_handles_split_size_lines() {
    let mut dechunk = Dechunk::default();
    let mut raw = b"5\r".to_vec();
    let mut out = Vec::new();
    dechunk.feed(&mut raw, &mut out).unwrap();
    assert!(out.is_empty());
    raw.extend_from_slice(b"\nhel");
    dechunk.feed(&mut raw, &mut out).unwrap();
    raw.extend_from_slice(b"lo\r\n0\r\n\r\n");
    dechunk.feed(&mut raw, &mut out).unwrap();
    assert_eq!(out, b"hello");
    assert!(dechunk.finished);
}
