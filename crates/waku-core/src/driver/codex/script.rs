//! A scripted Codex app-server for deriving replay fixtures without the
//! real CLI (and without spending anyone's quota).
//!
//! The script is a transcript in the replay record shapes, usually adapted
//! from a recording (T3's `codex_transcript.ndjson` fixtures) or a committed
//! `codex.ndjson`. Its inbound frames are played back verbatim; its outbound
//! frames are matched leniently, by method only, so the frames Waku's driver
//! really writes are what the recorder captures. JSON-RPC ids of the
//! script's requests are mapped onto the driver's own, so a response always
//! answers the request that asked for it. `<workspace>` in inbound frames
//! becomes the session's working directory.
//!
//! `waku-replay-record --provider codex --script PATH` records through it:
//! the result is sanitized, written and replayed like any recording.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;
use url::Url;

use crate::driver::v2::InstanceConfig;
use crate::driver::v2::replay::transport::spawn_channel;
use crate::driver::v2::replay::{ExitStatusKind, ReplayEntry, ReplayTranscript};
use crate::driver::v2::transport::{
    HttpTransport, ProcessExit, ProcessSpec, StdioTransport, TransportFactory,
};

type ExitSender = Arc<Mutex<Option<oneshot::Sender<ProcessExit>>>>;

/// Plays a script's inbound frames to a live driver.
pub struct ScriptedAppServer {
    entries: Vec<ReplayEntry>,
    spawns: Mutex<HashMap<String, u32>>,
    home: PathBuf,
    /// Why the script stopped matching, if it did.
    error: Arc<Mutex<Option<String>>>,
    /// Running app-servers, for `kill_all` (a daemon restart, #59).
    running: Mutex<Vec<(tokio::task::AbortHandle, ExitSender)>>,
}

impl ScriptedAppServer {
    pub fn new(script: ReplayTranscript, home: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            entries: script.entries,
            spawns: Mutex::new(HashMap::new()),
            home,
            error: Arc::new(Mutex::new(None)),
            running: Mutex::new(Vec::new()),
        })
    }

    pub fn error(&self) -> Option<String> {
        self.error.lock().clone()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    Request,
    Notification,
    Response,
}

fn shape(frame: &Value) -> Shape {
    match (
        frame.get("method").is_some(),
        frame.get("id").is_some_and(|id| !id.is_null()),
    ) {
        (true, true) => Shape::Request,
        (true, false) => Shape::Notification,
        _ => Shape::Response,
    }
}

fn materialize(value: &Value, workspace: &str) -> Value {
    match value {
        Value::String(text) => Value::String(text.replace("<workspace>", workspace)),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| materialize(item, workspace))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, item)| (key.clone(), materialize(item, workspace)))
                .collect(),
        ),
        other => other.clone(),
    }
}

struct Channel {
    entries: Vec<ReplayEntry>,
    cursor: usize,
    /// Script request id → the driver's id for the same request.
    ids: HashMap<String, Value>,
    workspace: String,
}

enum Next {
    Line(String),
    Exit(ProcessExit, bool),
    Wait,
}

impl Channel {
    /// Inbound frames and exits at the cursor, up to the next outbound one.
    fn drain(&mut self) -> Vec<Next> {
        let mut out = Vec::new();
        while let Some(entry) = self.entries.get(self.cursor) {
            match entry {
                ReplayEntry::ExpectOutbound { .. } => break,
                // A process sees nothing of a daemon restart but its kill.
                ReplayEntry::DaemonRestart => {}
                ReplayEntry::EmitInbound { frame, .. } => {
                    let mut frame = materialize(frame, &self.workspace);
                    if shape(&frame) == Shape::Response
                        && let Some(id) = frame.get("id").map(Value::to_string)
                        && let Some(actual) = self.ids.get(&id)
                    {
                        frame["id"] = actual.clone();
                    }
                    out.push(Next::Line(frame.to_string()));
                }
                ReplayEntry::RuntimeExit {
                    status,
                    exit_code,
                    signal,
                    ..
                } => {
                    let exit = ProcessExit {
                        success: *status == ExitStatusKind::Success,
                        code: exit_code.or((*status == ExitStatusKind::Success).then_some(0)),
                        signal: *signal,
                    };
                    out.push(Next::Exit(exit, *status == ExitStatusKind::Error));
                    self.cursor += 1;
                    return out;
                }
            }
            self.cursor += 1;
        }
        if out.is_empty() {
            out.push(Next::Wait);
        }
        out
    }

    /// Matches one frame the driver wrote.
    fn outbound(&mut self, frame: &Value) -> Result<(), String> {
        let Some(ReplayEntry::ExpectOutbound {
            frame: expected, ..
        }) = self.entries.get(self.cursor)
        else {
            return Err(format!("the script has no outbound frame left for {frame}"));
        };
        let (got, want) = (shape(frame), shape(expected));
        let same_method = frame.get("method") == expected.get("method");
        if got != want || !same_method {
            return Err(format!(
                "the driver wrote {frame}, the script expects {expected}"
            ));
        }
        if got == Shape::Request
            && let (Some(script), Some(actual)) = (expected.get("id"), frame.get("id"))
        {
            self.ids.insert(script.to_string(), actual.clone());
        }
        self.cursor += 1;
        Ok(())
    }
}

#[async_trait]
impl TransportFactory for ScriptedAppServer {
    async fn spawn_stdio(&self, channel: &str, spec: ProcessSpec) -> io::Result<StdioTransport> {
        let name = spawn_channel(&mut self.spawns.lock(), channel);
        let entries = self
            .entries
            .iter()
            .filter(|entry| entry.channel() == name)
            .filter(|entry| {
                !matches!(entry, ReplayEntry::ExpectOutbound { frame, .. }
                    if frame.get("type").and_then(Value::as_str) == Some("process_start"))
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut state = Channel {
            entries,
            cursor: 0,
            ids: HashMap::new(),
            workspace: spec.cwd.display().to_string(),
        };
        let (driver_stdin, server_stdin) = tokio::io::duplex(1 << 20);
        let (mut server_stdout, driver_stdout) = tokio::io::duplex(1 << 20);
        let (_, driver_stderr) = tokio::io::duplex(64);
        let (exit_sender, exit_receiver) = oneshot::channel();
        let exit_sender: ExitSender = Arc::new(Mutex::new(Some(exit_sender)));
        let killable = exit_sender.clone();
        let error = self.error.clone();
        let task = tokio::spawn(async move {
            let exit_sender = exit_sender;
            let mut lines = BufReader::new(server_stdin).lines();
            let mut pending_exit = None;
            let play =
                |state: &mut Channel, out: &mut Vec<String>| -> Option<(ProcessExit, bool)> {
                    for next in state.drain() {
                        match next {
                            Next::Line(line) => out.push(line),
                            Next::Exit(exit, crash) => return Some((exit, crash)),
                            Next::Wait => {}
                        }
                    }
                    None
                };
            let mut out = Vec::new();
            let mut exit = play(&mut state, &mut out);
            loop {
                for line in out.drain(..) {
                    if server_stdout
                        .write_all(format!("{line}\n").as_bytes())
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                let _ = server_stdout.flush().await;
                if let Some((status, crash)) = exit.take() {
                    if crash {
                        drop(server_stdout);
                        if let Some(sender) = exit_sender.lock().take() {
                            let _ = sender.send(status);
                        }
                        return;
                    }
                    pending_exit = Some(status);
                }
                let Ok(Some(line)) = lines.next_line().await else {
                    break;
                };
                let Ok(frame) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if let Err(message) = state.outbound(&frame) {
                    eprintln!("scripted codex app-server: {message}");
                    error.lock().get_or_insert(message);
                    break;
                }
                exit = play(&mut state, &mut out);
            }
            // stdin closed (or the script stopped matching): the process ends.
            drop(server_stdout);
            if let Some(sender) = exit_sender.lock().take() {
                let _ = sender.send(pending_exit.unwrap_or(ProcessExit::SUCCESS));
            }
        });
        let mut running = self.running.lock();
        running.retain(|(task, _)| !task.is_finished());
        running.push((task.abort_handle(), killable));
        Ok(StdioTransport {
            stdin: Box::new(driver_stdin),
            stdout: Box::new(driver_stdout),
            stderr: Box::new(driver_stderr),
            exit: Box::pin(async move {
                exit_receiver
                    .await
                    .map_err(|_| io::Error::other("the scripted app-server vanished"))
            }),
            pid: None,
        })
    }

    async fn http(&self, _channel: &str, _base: Url) -> io::Result<Arc<dyn HttpTransport>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the Codex app-server speaks stdio",
        ))
    }

    fn provider_home(&self, cfg: &InstanceConfig) -> PathBuf {
        cfg.provider_home
            .clone()
            .unwrap_or_else(|| self.home.clone())
    }

    /// SIGKILL for every running app-server (a daemon restart).
    fn kill_all(&self) {
        for (task, exit) in self.running.lock().drain(..) {
            task.abort();
            if let Some(exit) = exit.lock().take() {
                let _ = exit.send(ProcessExit {
                    success: false,
                    code: None,
                    signal: Some(9),
                });
            }
        }
    }
}
