//! `RecordingTransportFactory`: wraps the real transports and tees every
//! frame into a transcript (RFC 0002 "Recorder").
//!
//! Outbound frames are recorded when the driver writes them and inbound
//! frames when the driver reads them, so the recorded order respects
//! causality: a frame the driver needed before writing is always before
//! what it wrote. The process start and its exit are recorded too.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures_util::StreamExt as _;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use url::Url;

use super::transport::{process_start_frame, spawn_channel};
use super::{ExitStatusKind, ReplayEntry};
use crate::driver::v2::InstanceConfig;
use crate::driver::v2::transport::{
    Downlink, HttpMethod, HttpResponse, HttpTransport, ProcessSpec, StdioTransport,
    TransportFactory,
};

type Entries = Arc<Mutex<Vec<ReplayEntry>>>;

pub struct RecordingTransportFactory {
    inner: Arc<dyn TransportFactory>,
    entries: Entries,
    spawns: Mutex<HashMap<String, u32>>,
}

impl RecordingTransportFactory {
    pub fn new(inner: Arc<dyn TransportFactory>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            entries: Arc::new(Mutex::new(Vec::new())),
            spawns: Mutex::new(HashMap::new()),
        })
    }

    /// Everything recorded so far, in order.
    pub fn entries(&self) -> Vec<ReplayEntry> {
        self.entries.lock().clone()
    }
}

fn channel_field(channel: &str) -> Option<String> {
    (channel != "main").then(|| channel.to_owned())
}

fn label_of(frame: &Value) -> Option<String> {
    frame
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| frame.get("type").and_then(Value::as_str).map(str::to_owned))
}

/// Splits a byte stream into JSON lines and records each one.
struct LineTee {
    buffer: Vec<u8>,
    channel: String,
    outbound: bool,
    entries: Entries,
}

impl LineTee {
    fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
        while let Some(end) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line = self.buffer.drain(..=end).collect::<Vec<_>>();
            let text = String::from_utf8_lossy(&line);
            let text = text.trim();
            if text.is_empty() {
                continue;
            }
            let frame =
                serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()));
            let channel = channel_field(&self.channel);
            let label = label_of(&frame);
            let entry = if self.outbound {
                ReplayEntry::ExpectOutbound {
                    label,
                    channel,
                    frame,
                }
            } else {
                ReplayEntry::EmitInbound {
                    label,
                    channel,
                    frame,
                    after_ms: None,
                }
            };
            self.entries.lock().push(entry);
        }
    }
}

struct TeeWriter {
    inner: Box<dyn AsyncWrite + Send + Unpin>,
    tee: LineTee,
}

impl AsyncWrite for TeeWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.tee.push(&buf[..written]);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

struct TeeReader {
    inner: Box<dyn AsyncRead + Send + Unpin>,
    tee: LineTee,
}

impl AsyncRead for TeeReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let filled = buf.filled()[before..].to_vec();
            this.tee.push(&filled);
        }
        result
    }
}

#[async_trait]
impl TransportFactory for RecordingTransportFactory {
    async fn spawn_stdio(&self, channel: &str, spec: ProcessSpec) -> io::Result<StdioTransport> {
        let channel = &spawn_channel(&mut self.spawns.lock(), channel);
        self.entries.lock().push(ReplayEntry::ExpectOutbound {
            label: Some("process_start".to_owned()),
            channel: channel_field(channel),
            frame: process_start_frame(&spec),
        });
        let transport = self.inner.spawn_stdio(channel, spec).await?;
        let entries = self.entries.clone();
        let exit_channel = channel_field(channel);
        let exit = transport.exit;
        Ok(StdioTransport {
            stdin: Box::new(TeeWriter {
                inner: transport.stdin,
                tee: LineTee {
                    buffer: Vec::new(),
                    channel: channel.to_owned(),
                    outbound: true,
                    entries: self.entries.clone(),
                },
            }),
            stdout: Box::new(TeeReader {
                inner: transport.stdout,
                tee: LineTee {
                    buffer: Vec::new(),
                    channel: channel.to_owned(),
                    outbound: false,
                    entries: self.entries.clone(),
                },
            }),
            stderr: transport.stderr,
            exit: Box::pin(async move {
                let status = exit.await;
                let (kind, code, signal) = match &status {
                    Ok(exit) if exit.success => (ExitStatusKind::Success, exit.code, exit.signal),
                    Ok(exit) => (ExitStatusKind::Error, exit.code, exit.signal),
                    Err(_) => (ExitStatusKind::Error, None, None),
                };
                entries.lock().push(ReplayEntry::RuntimeExit {
                    channel: exit_channel,
                    status: kind,
                    exit_code: code.filter(|code| *code != 0),
                    signal,
                    error: None,
                });
                status
            }),
            pid: transport.pid,
        })
    }

    async fn http(&self, channel: &str, base: Url) -> io::Result<Arc<dyn HttpTransport>> {
        let channel = spawn_channel(&mut self.spawns.lock(), channel);
        let inner = self.inner.http(&channel, base).await?;
        Ok(Arc::new(TeeHttp {
            inner,
            channel,
            entries: self.entries.clone(),
        }))
    }

    fn provider_home(&self, cfg: &InstanceConfig) -> PathBuf {
        self.inner.provider_home(cfg)
    }
}

/// Records an HTTP channel: each request as an `http.request` followed by its
/// `http.response`, each downlink as a `stream.open` / `websocket.open`, the
/// `stream.event`s read from it, and a `stream.closed` when it ends. A request
/// that got no HTTP answer at all records nothing past its request.
struct TeeHttp {
    inner: Arc<dyn HttpTransport>,
    channel: String,
    entries: Entries,
}

impl TeeHttp {
    fn outbound(&self, label: String, frame: Value) {
        self.entries.lock().push(ReplayEntry::ExpectOutbound {
            label: Some(label),
            channel: channel_field(&self.channel),
            frame,
        });
    }

    fn tee(&self, path: &str, downlink: Downlink) -> Downlink {
        let entries = self.entries.clone();
        let channel = channel_field(&self.channel);
        let path = path.to_owned();
        Box::pin(futures_util::stream::unfold(
            Some(downlink),
            move |downlink| {
                let entries = entries.clone();
                let channel = channel.clone();
                let path = path.clone();
                async move {
                    let mut downlink = downlink?;
                    let item = downlink.next().await;
                    let frame = match &item {
                        Some(Ok(data)) => {
                            json!({ "type": "stream.event", "path": path, "data": data })
                        }
                        _ => json!({ "type": "stream.closed", "path": path }),
                    };
                    let label = match &item {
                        Some(Ok(data)) => label_of(data),
                        _ => Some("stream.closed".to_owned()),
                    };
                    entries.lock().push(ReplayEntry::EmitInbound {
                        label,
                        channel,
                        frame,
                        after_ms: None,
                    });
                    match item {
                        Some(Ok(data)) => Some((Ok(data), Some(downlink))),
                        Some(Err(error)) => Some((Err(error), None)),
                        None => None,
                    }
                }
            },
        ))
    }
}

#[async_trait]
impl HttpTransport for TeeHttp {
    async fn request(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<Value>,
    ) -> io::Result<HttpResponse> {
        let mut frame = json!({ "type": "http.request", "method": method.as_str(), "path": path });
        if let Some(body) = &body {
            frame["body"] = body.clone();
        }
        self.outbound(format!("{} {path}", method.as_str()), frame);
        let response = self.inner.request(method, path, body).await?;
        self.entries.lock().push(ReplayEntry::EmitInbound {
            label: Some("http.response".to_owned()),
            channel: channel_field(&self.channel),
            frame: json!({ "type": "http.response", "status": response.status, "body": response.body }),
            after_ms: None,
        });
        Ok(response)
    }

    async fn stream(&self, path: &str) -> io::Result<Downlink> {
        self.outbound(
            "stream.open".to_owned(),
            json!({ "type": "stream.open", "path": path }),
        );
        let downlink = self.inner.stream(path).await?;
        Ok(self.tee(path, downlink))
    }

    async fn websocket(&self, path: &str) -> io::Result<Downlink> {
        self.outbound(
            "websocket.open".to_owned(),
            json!({ "type": "websocket.open", "path": path }),
        );
        let downlink = self.inner.websocket(path).await?;
        Ok(self.tee(path, downlink))
    }
}

/// Merges a scenario's `instance` switches into an instance's `extra`.
pub fn instance_for(mut instance: InstanceConfig, input: &super::ScenarioInput) -> InstanceConfig {
    if let Some(Value::Object(switches)) = &input.instance {
        let mut extra = match instance.extra.take() {
            Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        extra.extend(switches.clone());
        instance.extra = Value::Object(extra);
    }
    instance
}

/// What to record.
pub struct RecordRequest {
    pub driver: Arc<dyn crate::driver::v2::DriverV2>,
    pub instance: InstanceConfig,
    /// The real transports (or the fake agent).
    pub live: Arc<dyn TransportFactory>,
    pub header: super::TranscriptHeader,
    pub input: super::ScenarioInput,
    pub out: PathBuf,
    pub renumber_ids: bool,
}

/// Records a scenario (RFC 0002 "Recorder"): runs it live through the real
/// driver over `RecordingTransportFactory`, sanitizes and writes the
/// transcript, fails closed on any leak, then replays the file and keeps it
/// only if replay passes.
pub fn record_scenario(
    request: RecordRequest,
) -> Result<super::ReplayTranscript, super::ReplayError> {
    use super::harness::{ReplayHarness, Transports};
    use super::{ReplayError, ReplayProfile, ReplayTranscript};

    let instance = instance_for(request.instance, &request.input);
    let recorder = RecordingTransportFactory::new(request.live);
    let (workspace, home) = {
        let mut harness = ReplayHarness::new(
            request.driver.clone(),
            instance.clone(),
            Transports::Live(recorder.clone()),
        )?;
        harness.run(&request.input)?;
        (harness.workspace.clone(), harness.home.clone())
    };
    let mut entries = recorder.entries();
    let mut sanitizer = super::Sanitizer::from_env(&workspace, Some(&home));
    sanitizer.renumber_ids = request.renumber_ids;
    sanitizer.sanitize(&mut entries);
    let transcript = ReplayTranscript::new(request.header, entries);
    super::write_transcript(&request.out, &transcript)
        .map_err(|error| ReplayError::Io(error.to_string()))?;
    sanitizer
        .verify_file(&request.out)
        .map_err(|error| ReplayError::Harness(error.to_string()))?;
    let written = super::read_transcript(&request.out)?;
    let protocol = written.header.protocol.clone();
    let replayed = ReplayHarness::replay(
        request.driver,
        instance,
        written.clone(),
        request.out.parent().map(std::path::Path::to_path_buf),
        ReplayProfile::new(protocol),
    )
    .and_then(|mut harness| harness.run(&request.input).map(|_| ()));
    if let Err(error) = replayed {
        let _ = std::fs::remove_file(&request.out);
        return Err(error);
    }
    Ok(written)
}
