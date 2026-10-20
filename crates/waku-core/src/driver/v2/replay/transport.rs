//! `ReplayTransportFactory`: provider processes and HTTP services played
//! back from a transcript (RFC 0002 "Matching").
//!
//! Every frame a driver writes must equal the entry at the replay cursor.
//! Before an outbound frame is checked, every `emit_inbound` at the cursor is
//! delivered (T3's `drainInbound`). `afterMs` advances the harness clock,
//! never wall time. A `runtime_exit` with status `error` ends the channel's
//! output at once (a crash); `success` and `cancelled` wait until the driver
//! closes the channel's stdin (a graceful shutdown), then end it.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use url::Url;

use super::matcher::{Placeholders, frame_diff, matches, prepare};
use super::{ExitStatusKind, ReplayEntry, ReplayError, ReplayProfile, ReplayTranscript};
use crate::driver::v2::InstanceConfig;
use crate::driver::v2::transport::{
    HttpMethod, HttpResponse, HttpTransport, ProcessExit, ProcessSpec, StdioTransport,
    TransportFactory,
};

/// Called with `afterMs` before a delayed inbound frame is delivered.
pub type AdvanceClock = Arc<dyn Fn(u64) + Send + Sync>;

/// The `process_start` frame a spawn records: basename, args, and only the
/// environment the driver sets.
pub fn process_start_frame(spec: &ProcessSpec) -> Value {
    let program = spec
        .program
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| spec.program.display().to_string());
    let env = spec
        .env
        .iter()
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect::<serde_json::Map<_, _>>();
    json!({ "type": "process_start", "program": program, "args": spec.args, "env": env })
}

struct Channel {
    /// Lines to the driver's stdout; dropping it ends the stream.
    stdout: Option<mpsc::UnboundedSender<String>>,
    exit: Option<oneshot::Sender<ProcessExit>>,
    stdin_closed: bool,
    /// HTTP responses waiting for the next request on this channel.
    responses: Option<mpsc::UnboundedSender<Value>>,
}

/// The transcript name of a spawn: the driver's channel name for the first
/// process spawned under it, `<name>#<n>` for the n-th (a reopened session,
/// a second thread's session). Recorder and replay apply the same rule.
pub fn spawn_channel(spawns: &mut HashMap<String, u32>, channel: &str) -> String {
    let count = spawns.entry(channel.to_owned()).or_default();
    *count += 1;
    if *count == 1 {
        channel.to_owned()
    } else {
        format!("{channel}#{count}")
    }
}

struct State {
    transcript: ReplayTranscript,
    cursor: usize,
    spawns: HashMap<String, u32>,
    channels: HashMap<String, Channel>,
    error: Option<ReplayError>,
    /// Bumped whenever the cursor moves; the harness watches it for idleness.
    progress: u64,
}

pub struct ReplayTransportFactory {
    state: Mutex<State>,
    placeholders: Placeholders,
    ignored: Vec<String>,
    home: PathBuf,
    advance: Mutex<Option<AdvanceClock>>,
}

fn exit_of(status: ExitStatusKind, exit_code: Option<i32>, signal: Option<i32>) -> ProcessExit {
    ProcessExit {
        success: status == ExitStatusKind::Success,
        code: exit_code.or(match status {
            ExitStatusKind::Success => Some(0),
            _ => None,
        }),
        signal,
    }
}

impl ReplayTransportFactory {
    pub fn new(
        transcript: ReplayTranscript,
        workspace: PathBuf,
        home: PathBuf,
        profile: ReplayProfile,
    ) -> Arc<Self> {
        let mut ignored = profile.ignored_pointers;
        ignored.extend(transcript.header.ignored_pointers());
        Arc::new(Self {
            placeholders: Placeholders::new(&workspace, &home),
            ignored,
            home,
            state: Mutex::new(State {
                transcript,
                cursor: 0,
                spawns: HashMap::new(),
                channels: HashMap::new(),
                error: None,
                progress: 0,
            }),
            advance: Mutex::new(None),
        })
    }

    /// Lets `afterMs` move the harness clock.
    pub fn set_clock_advance(&self, advance: AdvanceClock) {
        *self.advance.lock() = Some(advance);
    }

    pub fn scenario(&self) -> String {
        self.state.lock().transcript.header.scenario.clone()
    }

    /// The first failure, if any.
    pub fn error(&self) -> Option<ReplayError> {
        self.state.lock().error.clone()
    }

    pub fn progress(&self) -> u64 {
        self.state.lock().progress
    }

    /// Fails with the first recorded error, or with the entries never
    /// reached.
    pub fn assert_exhausted(&self) -> Result<(), ReplayError> {
        let state = self.state.lock();
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        if state.cursor < state.transcript.entries.len() {
            let remaining = state.transcript.entries[state.cursor..]
                .iter()
                .zip(&state.transcript.lines[state.cursor..])
                .map(|(entry, line)| format!("line {line}: {}", entry.describe()))
                .collect();
            return Err(ReplayError::NotExhausted {
                scenario: state.transcript.header.scenario.clone(),
                remaining,
            });
        }
        Ok(())
    }

    /// Delivers inbound frames and exits at the cursor until an outbound
    /// frame is expected (or a graceful exit waits for stdin to close).
    fn advance(&self, state: &mut State) {
        loop {
            let Some(entry) = state.transcript.entries.get(state.cursor).cloned() else {
                return;
            };
            match entry {
                ReplayEntry::ExpectOutbound { .. } => return,
                ReplayEntry::EmitInbound {
                    channel,
                    frame,
                    after_ms,
                    ..
                } => {
                    let channel = channel.unwrap_or_else(|| "main".to_owned());
                    if !state.channels.contains_key(&channel) {
                        // Not spawned yet: wait for the spawn.
                        return;
                    }
                    if let Some(after) = after_ms.filter(|after| *after > 0)
                        && let Some(advance) = self.advance.lock().clone()
                    {
                        advance(after);
                    }
                    let frame = self.placeholders.materialize(&frame);
                    let channel = state.channels.get_mut(&channel).expect("checked above");
                    if let Some(responses) = &channel.responses
                        && frame.get("type").and_then(Value::as_str) == Some("http.response")
                    {
                        let _ = responses.send(frame);
                    } else if let Some(stdout) = &channel.stdout {
                        let _ = stdout.send(frame.to_string());
                    }
                }
                ReplayEntry::RuntimeExit {
                    channel,
                    status,
                    exit_code,
                    signal,
                    ..
                } => {
                    let channel = channel.unwrap_or_else(|| "main".to_owned());
                    let Some(state_channel) = state.channels.get_mut(&channel) else {
                        return;
                    };
                    if status != ExitStatusKind::Error && !state_channel.stdin_closed {
                        return;
                    }
                    state_channel.stdout.take();
                    state_channel.responses.take();
                    if let Some(exit) = state_channel.exit.take() {
                        let _ = exit.send(exit_of(status, exit_code, signal));
                    }
                }
            }
            state.cursor += 1;
            state.progress += 1;
        }
    }

    fn fail(&self, state: &mut State, error: ReplayError) {
        if state.error.is_none() {
            state.error = Some(error);
        }
        // Stop the session: every channel ends, so the driver sees the
        // provider exit and the scenario settles quickly.
        for channel in state.channels.values_mut() {
            channel.stdout.take();
            channel.responses.take();
            if let Some(exit) = channel.exit.take() {
                let _ = exit.send(ProcessExit {
                    success: false,
                    code: Some(1),
                    signal: None,
                });
            }
        }
        state.progress += 1;
    }

    /// Checks one outbound frame against the cursor.
    fn outbound(&self, channel: &str, frame: Value) {
        let mut state = self.state.lock();
        if state.error.is_some() {
            return;
        }
        self.advance(&mut state);
        let scenario = state.transcript.header.scenario.clone();
        let cursor = state.cursor;
        match state.transcript.entries.get(cursor).cloned() {
            Some(ReplayEntry::ExpectOutbound {
                channel: expected_channel,
                frame: expected,
                label,
            }) if expected_channel.as_deref().unwrap_or("main") == channel => {
                let (expected, actual) =
                    prepare(&expected, &frame, &self.placeholders, &self.ignored);
                if matches(&expected, &actual) {
                    state.cursor += 1;
                    state.progress += 1;
                    self.advance(&mut state);
                } else {
                    let line = state.transcript.lines[cursor];
                    let diff = frame_diff(&expected, &actual);
                    self.fail(
                        &mut state,
                        ReplayError::Mismatch {
                            scenario,
                            line,
                            label,
                            expected,
                            actual,
                            diff,
                        },
                    );
                }
            }
            Some(ReplayEntry::ExpectOutbound {
                channel: expected_channel,
                frame: expected,
                label,
            }) => {
                let line = state.transcript.lines[cursor];
                let expected = json!({
                    "channel": expected_channel.unwrap_or_else(|| "main".to_owned()),
                    "frame": expected,
                });
                let actual = json!({ "channel": channel, "frame": frame });
                let diff = frame_diff(&expected, &actual);
                self.fail(
                    &mut state,
                    ReplayError::Mismatch {
                        scenario,
                        line,
                        label,
                        expected,
                        actual,
                        diff,
                    },
                );
            }
            _ => self.fail(
                &mut state,
                ReplayError::UnexpectedOutbound {
                    scenario,
                    channel: channel.to_owned(),
                    frame,
                },
            ),
        }
    }

    fn stdin_closed(&self, channel: &str) {
        let mut state = self.state.lock();
        if let Some(channel) = state.channels.get_mut(channel) {
            channel.stdin_closed = true;
        }
        self.advance(&mut state);
    }
}

#[async_trait]
impl TransportFactory for Arc<ReplayTransportFactory> {
    async fn spawn_stdio(&self, channel: &str, spec: ProcessSpec) -> io::Result<StdioTransport> {
        let (driver_stdin, agent_stdin) = tokio::io::duplex(64 * 1024);
        let (mut agent_stdout, driver_stdout) = tokio::io::duplex(64 * 1024);
        let (stdout_lines, mut stdout_queue) = mpsc::unbounded_channel::<String>();
        let (exit_sender, exit_receiver) = oneshot::channel();
        let channel = {
            let mut state = self.state.lock();
            let channel = spawn_channel(&mut state.spawns, channel);
            state.channels.insert(
                channel.clone(),
                Channel {
                    stdout: Some(stdout_lines),
                    exit: Some(exit_sender),
                    stdin_closed: false,
                    responses: None,
                },
            );
            channel
        };
        // The spawn itself is the channel's first outbound frame.
        self.outbound(&channel, process_start_frame(&spec));
        tokio::spawn(async move {
            while let Some(line) = stdout_queue.recv().await {
                if agent_stdout.write_all(line.as_bytes()).await.is_err()
                    || agent_stdout.write_all(b"\n").await.is_err()
                {
                    return;
                }
                let _ = agent_stdout.flush().await;
            }
            let _ = agent_stdout.shutdown().await;
        });
        let factory = self.clone();
        let name = channel;
        tokio::spawn(async move {
            let mut lines = BufReader::new(agent_stdin).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let frame = serde_json::from_str(&line).unwrap_or(Value::String(line));
                factory.outbound(&name, frame);
            }
            factory.stdin_closed(&name);
        });
        let failed = ProcessExit {
            success: false,
            code: None,
            signal: None,
        };
        Ok(StdioTransport {
            stdin: Box::new(driver_stdin),
            stdout: Box::new(driver_stdout),
            stderr: Box::new(tokio::io::empty()),
            exit: Box::pin(async move { Ok(exit_receiver.await.unwrap_or(failed)) }),
            pid: None,
        })
    }

    async fn http(&self, channel: &str, _base: Url) -> io::Result<Arc<dyn HttpTransport>> {
        let (responses, receiver) = mpsc::unbounded_channel();
        let (stream_lines, stream_receiver) = mpsc::unbounded_channel();
        {
            let mut state = self.state.lock();
            state.channels.insert(
                channel.to_owned(),
                Channel {
                    stdout: Some(stream_lines),
                    exit: None,
                    stdin_closed: false,
                    responses: Some(responses),
                },
            );
            self.advance(&mut state);
        }
        Ok(Arc::new(ReplayHttp {
            factory: self.clone(),
            channel: channel.to_owned(),
            responses: tokio::sync::Mutex::new(receiver),
            stream: Mutex::new(Some(stream_receiver)),
        }))
    }

    fn provider_home(&self, _cfg: &InstanceConfig) -> PathBuf {
        self.home.clone()
    }
}

/// HTTP over a transcript: a request is an outbound `http.request` frame and
/// its response the next `http.response`; `stream.event` frames feed the one
/// streaming downlink of the channel.
struct ReplayHttp {
    factory: Arc<ReplayTransportFactory>,
    channel: String,
    responses: tokio::sync::Mutex<mpsc::UnboundedReceiver<Value>>,
    stream: Mutex<Option<mpsc::UnboundedReceiver<String>>>,
}

#[async_trait]
impl HttpTransport for ReplayHttp {
    async fn request(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<Value>,
    ) -> io::Result<HttpResponse> {
        let mut frame = json!({ "type": "http.request", "method": method.as_str(), "path": path });
        if let Some(body) = body {
            frame["body"] = body;
        }
        let mut responses = self.responses.lock().await;
        self.factory.outbound(&self.channel, frame);
        let response = responses
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "replay channel ended"))?;
        Ok(HttpResponse {
            status: response
                .get("status")
                .and_then(Value::as_u64)
                .unwrap_or(200) as u16,
            body: response.get("body").cloned().unwrap_or(Value::Null),
        })
    }

    async fn stream(&self, _path: &str) -> io::Result<BoxStream<'static, io::Result<Value>>> {
        let receiver = self
            .stream
            .lock()
            .take()
            .ok_or_else(|| io::Error::other("the replay channel has one stream"))?;
        let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
            loop {
                let line = receiver.recv().await?;
                let frame: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
                match frame.get("type").and_then(Value::as_str) {
                    Some("stream.event") => {
                        let data = frame.get("data").cloned().unwrap_or(Value::Null);
                        return Some((Ok(data), receiver));
                    }
                    Some("stream.closed") => return None,
                    _ => continue,
                }
            }
        });
        Ok(Box::pin(stream))
    }
}

/// Copies a fixture's `<provider>.home/` seed, if any, into `home`.
pub fn seed_home(fixture_dir: &Path, provider: &str, home: &Path) -> io::Result<()> {
    let seed = fixture_dir.join(format!("{provider}.home"));
    if !seed.is_dir() {
        return Ok(());
    }
    copy_dir(&seed, home)
}

fn copy_dir(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}
