//! The "CLI" behind the fake driver: an in-process agent speaking
//! `fake.jsonl`, scripted by markers in the prompt. The recorder runs the
//! fake driver against it exactly as it would run a real driver against a
//! real CLI, so the fake's fixtures are recorded, not hand-written.
//!
//! Prompt markers:
//! - `[approval]`: a tool asks for approval; allowed it runs, denied it skips.
//! - `[question]`: a structured question; the answer is echoed.
//! - `[slow]`: the turn streams one delta and waits for a steer or an interrupt.
//! - `[usage-limit]`: the turn fails with a usage limit and a reset time.
//! - `[subagent]`: a subagent runs and its child turn completes mid-turn.
//! - `[plan]`: a todo list is published.
//! - anything else: the reply echoes the prompt.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::sync::oneshot;
use url::Url;

use super::transport::{HttpTransport, ProcessExit, ProcessSpec, StdioTransport, TransportFactory};
use super::{CONTEXT_ENVELOPE, InstanceConfig};

/// The usage-limit reset the agent reports: 2026-01-01T05:00:00Z.
pub const USAGE_LIMIT_RESET_MS: i64 = 1_767_243_600_000;

#[derive(Default)]
struct World {
    next: u64,
    /// Native thread id → its turns' ids, in order.
    threads: HashMap<String, Vec<String>>,
}

impl World {
    fn id(&mut self, prefix: &str) -> String {
        self.next += 1;
        format!("{prefix}-{}", self.next)
    }
}

type ExitSender = Arc<Mutex<Option<oneshot::Sender<ProcessExit>>>>;

/// Spawns fake agents. Threads live in one world shared by every process the
/// factory starts, as a real CLI's threads live on disk.
#[derive(Clone)]
pub struct FakeAgentFactory {
    world: Arc<Mutex<World>>,
    home: PathBuf,
    /// A rollback replaces the native thread instead of truncating it.
    pub rollback_replaces: bool,
    /// Running agents, for `kill_all` (a daemon restart).
    running: Arc<Mutex<Vec<(tokio::task::AbortHandle, ExitSender)>>>,
}

impl FakeAgentFactory {
    pub fn new(home: PathBuf) -> Self {
        Self {
            world: Arc::default(),
            home,
            rollback_replaces: false,
            running: Arc::default(),
        }
    }
}

#[async_trait]
impl TransportFactory for FakeAgentFactory {
    async fn spawn_stdio(&self, _channel: &str, _spec: ProcessSpec) -> io::Result<StdioTransport> {
        let (driver_stdin, agent_stdin) = tokio::io::duplex(64 * 1024);
        let (agent_stdout, driver_stdout) = tokio::io::duplex(64 * 1024);
        let (exit_sender, exit_receiver) = oneshot::channel();
        let agent = Agent {
            world: self.world.clone(),
            out: agent_stdout,
            rollback_replaces: self.rollback_replaces,
            active: None,
            next_request: 0,
        };
        let exit_sender: ExitSender = Arc::new(Mutex::new(Some(exit_sender)));
        let finished = exit_sender.clone();
        let task = tokio::spawn(async move {
            agent.run(agent_stdin).await;
            if let Some(exit) = finished.lock().take() {
                let _ = exit.send(ProcessExit::SUCCESS);
            }
        });
        let mut running = self.running.lock();
        running.retain(|(task, _)| !task.is_finished());
        running.push((task.abort_handle(), exit_sender));
        Ok(StdioTransport {
            stdin: Box::new(driver_stdin),
            stdout: Box::new(driver_stdout),
            stderr: Box::new(tokio::io::empty()),
            exit: Box::pin(async move { Ok(exit_receiver.await.unwrap_or(ProcessExit::SUCCESS)) }),
            pid: None,
        })
    }

    async fn http(&self, _channel: &str, _base: Url) -> io::Result<Arc<dyn HttpTransport>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the fake agent speaks stdio",
        ))
    }

    fn provider_home(&self, _cfg: &InstanceConfig) -> PathBuf {
        self.home.clone()
    }

    /// SIGKILL for every running agent: its output ends mid-turn and it
    /// exits with the signal. The world (its threads) survives, as a real
    /// CLI's threads survive on disk.
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

enum Awaiting {
    /// `[slow]`: a steer or an interrupt ends the turn.
    Input,
    Approval {
        request: String,
        item: String,
    },
    Question {
        request: String,
    },
}

struct ActiveTurn {
    thread: String,
    turn: String,
    awaiting: Awaiting,
}

struct Agent {
    world: Arc<Mutex<World>>,
    out: DuplexStream,
    rollback_replaces: bool,
    active: Option<ActiveTurn>,
    next_request: u64,
}

fn params(frame: &Value) -> &Value {
    frame.get("params").unwrap_or(&Value::Null)
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

impl Agent {
    async fn send(&mut self, frame: Value) {
        let mut line = frame.to_string();
        line.push('\n');
        let _ = self.out.write_all(line.as_bytes()).await;
        let _ = self.out.flush().await;
    }

    async fn notify(&mut self, method: &str, params: Value) {
        self.send(json!({ "method": method, "params": params }))
            .await;
    }

    fn id(&self, prefix: &str) -> String {
        self.world.lock().id(prefix)
    }

    async fn run(mut self, stdin: DuplexStream) {
        let mut lines = BufReader::new(stdin).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(frame) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            match (frame.get("method").and_then(Value::as_str), frame.get("id")) {
                (Some(method), Some(id)) => {
                    let method = method.to_owned();
                    let id = id.clone();
                    self.request(&method, id, &frame).await;
                }
                (None, Some(Value::String(id))) => {
                    let id = id.clone();
                    self.response(&id, frame.get("result").unwrap_or(&Value::Null))
                        .await;
                }
                _ => {}
            }
        }
        let _ = self.out.shutdown().await;
    }

    async fn reply(&mut self, id: Value, result: Value) {
        self.send(json!({ "id": id, "result": result })).await;
    }

    async fn error(&mut self, id: Value, code: &str, message: &str) {
        self.send(json!({ "id": id, "error": { "code": code, "message": message } }))
            .await;
    }

    async fn request(&mut self, method: &str, id: Value, frame: &Value) {
        let params = params(frame).clone();
        match method {
            "initialize" => {
                self.reply(id, json!({ "agent": "fake-agent", "version": "1.0.0" }))
                    .await
            }
            "thread/start" => {
                let thread = self.id("t");
                self.world.lock().threads.insert(thread.clone(), Vec::new());
                self.reply(id, json!({ "threadId": thread })).await;
            }
            "thread/resume" => {
                let thread = text(&params, "threadId").to_owned();
                self.world.lock().threads.entry(thread.clone()).or_default();
                self.reply(id, json!({ "threadId": thread })).await;
            }
            "session/set_model" | "thread/compact" | "subagent/stop" => {
                self.reply(id, json!({})).await;
            }
            "turn/start" => self.start_turn(id, &params).await,
            "turn/steer" => {
                let matches = self
                    .active
                    .as_ref()
                    .is_some_and(|active| active.turn == text(&params, "turnId"));
                if !matches {
                    self.error(id, "turn_settled", "the turn already ended")
                        .await;
                    return;
                }
                self.reply(id, json!({})).await;
                let active = self.active.take().expect("checked");
                let steer = text(&params, "text").to_owned();
                self.finish(&active.thread, &active.turn, &format!(" Steered: {steer}"))
                    .await;
            }
            "turn/interrupt" => {
                self.reply(id, json!({})).await;
                let interrupted = self
                    .active
                    .as_ref()
                    .is_some_and(|active| active.turn == text(&params, "turnId"));
                if interrupted {
                    let active = self.active.take().expect("checked");
                    self.notify(
                        "turn/completed",
                        json!({ "turnId": active.turn, "status": "interrupted" }),
                    )
                    .await;
                }
            }
            "thread/rollback" => {
                let thread = text(&params, "threadId").to_owned();
                let remove = params.get("numTurns").and_then(Value::as_u64).unwrap_or(0) as usize;
                let result = {
                    let mut world = self.world.lock();
                    let mut turns = world.threads.get(&thread).cloned().unwrap_or_default();
                    turns.truncate(turns.len().saturating_sub(remove));
                    if self.rollback_replaces {
                        let replacement = world.id("t");
                        world.threads.insert(replacement.clone(), turns);
                        replacement
                    } else {
                        world.threads.insert(thread.clone(), turns);
                        thread
                    }
                };
                self.reply(id, json!({ "threadId": result })).await;
            }
            "thread/fork" => {
                let source = text(&params, "threadId").to_owned();
                let after = params
                    .get("afterTurnId")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let forked = {
                    let mut world = self.world.lock();
                    let mut turns = world.threads.get(&source).cloned().unwrap_or_default();
                    if let Some(after) = after
                        && let Some(index) = turns.iter().position(|turn| *turn == after)
                    {
                        turns.truncate(index + 1);
                    }
                    let forked = world.id("t");
                    world.threads.insert(forked.clone(), turns);
                    forked
                };
                self.reply(id, json!({ "threadId": forked })).await;
            }
            _ => self.error(id, "unknown_method", method).await,
        }
    }

    async fn start_turn(&mut self, id: Value, params: &Value) {
        if self.active.is_some() {
            self.error(id, "busy", "a turn is running").await;
            return;
        }
        let thread = text(params, "threadId").to_owned();
        let prompt = text(params, "text").to_owned();
        let turn = self.id("u");
        self.world
            .lock()
            .threads
            .entry(thread.clone())
            .or_default()
            .push(turn.clone());
        self.reply(id, json!({ "turnId": turn })).await;
        self.notify(
            "turn/started",
            json!({ "threadId": thread, "turnId": turn }),
        )
        .await;
        if prompt.contains("[usage-limit]") {
            self.notify(
                "turn/completed",
                json!({
                    "turnId": turn,
                    "status": "failed",
                    "error": {
                        "code": "usage_limit",
                        "message": "You've hit your usage limit.",
                        "resetsAt": USAGE_LIMIT_RESET_MS,
                    }
                }),
            )
            .await;
            return;
        }
        if prompt.contains("[plan]") {
            self.notify(
                "plan/updated",
                json!({
                    "turnId": turn, "planId": "plan-1",
                    "steps": [
                        { "id": "1", "text": "Read the code", "status": "completed" },
                        { "id": "2", "text": "Write the fix", "status": "running" },
                    ]
                }),
            )
            .await;
        }
        if prompt.contains("[approval]") {
            let item = self.id("i");
            self.notify(
                "tool/started",
                json!({ "turnId": turn, "itemId": item, "command": "ls" }),
            )
            .await;
            let request = self.request_id();
            self.send(json!({
                "id": request, "method": "approval/request",
                "params": {
                    "turnId": turn, "itemId": item, "command": "ls",
                    "options": [
                        { "id": "allow", "label": "Allow", "allow": true },
                        { "id": "deny", "label": "Deny", "allow": false },
                    ]
                }
            }))
            .await;
            self.active = Some(ActiveTurn {
                thread,
                turn,
                awaiting: Awaiting::Approval { request, item },
            });
            return;
        }
        if prompt.contains("[question]") {
            let request = self.request_id();
            self.send(json!({
                "id": request, "method": "question/request",
                "params": {
                    "turnId": turn,
                    "questions": [{
                        "id": "q1", "header": "Color", "question": "Which color?",
                        "options": [{ "label": "Red" }, { "label": "Blue" }]
                    }]
                }
            }))
            .await;
            self.active = Some(ActiveTurn {
                thread,
                turn,
                awaiting: Awaiting::Question { request },
            });
            return;
        }
        if prompt.contains("[subagent]") {
            let child = self.id("t");
            self.notify(
                "subagent/updated",
                json!({ "turnId": turn, "taskId": "task-1", "threadId": child,
                        "status": "running", "prompt": "Look around" }),
            )
            .await;
            let child_turn = self.id("u");
            self.notify(
                "subagent/turn_completed",
                json!({ "turnId": turn, "taskId": "task-1", "threadId": child, "childTurnId": child_turn }),
            )
            .await;
            self.notify(
                "subagent/updated",
                json!({ "turnId": turn, "taskId": "task-1", "threadId": child,
                        "status": "completed", "prompt": "Look around", "result": "Found it." }),
            )
            .await;
        }
        if prompt.contains("[slow]") {
            let item = self.id("i");
            self.notify(
                "item/delta",
                json!({ "turnId": turn, "itemId": item, "kind": "text", "delta": "Working" }),
            )
            .await;
            self.active = Some(ActiveTurn {
                thread,
                turn,
                awaiting: Awaiting::Input,
            });
            return;
        }
        let reply = if prompt.contains(&format!("<{CONTEXT_ENVELOPE}>")) {
            "I have the earlier context.".to_owned()
        } else {
            format!("Echo: {}", prompt.lines().next().unwrap_or_default())
        };
        self.reply_text(&turn, &reply).await;
        self.notify(
            "turn/completed",
            json!({ "turnId": turn, "status": "completed" }),
        )
        .await;
    }

    fn request_id(&mut self) -> String {
        self.next_request += 1;
        format!("r{}", self.next_request)
    }

    /// A streamed text reply: two deltas, then the completed item.
    async fn reply_text(&mut self, turn: &str, reply: &str) {
        let item = self.id("i");
        let split = reply
            .char_indices()
            .nth(reply.chars().count() / 2)
            .map_or(reply.len(), |(index, _)| index);
        for delta in [&reply[..split], &reply[split..]] {
            self.notify(
                "item/delta",
                json!({ "turnId": turn, "itemId": item, "kind": "text", "delta": delta }),
            )
            .await;
        }
        self.notify(
            "item/completed",
            json!({ "turnId": turn, "itemId": item, "kind": "text", "text": reply }),
        )
        .await;
    }

    async fn finish(&mut self, _thread: &str, turn: &str, reply: &str) {
        self.reply_text(turn, reply.trim()).await;
        self.notify(
            "turn/completed",
            json!({ "turnId": turn, "status": "completed" }),
        )
        .await;
    }

    async fn response(&mut self, id: &str, result: &Value) {
        let Some(active) = self.active.take() else {
            return;
        };
        match &active.awaiting {
            Awaiting::Approval { request, item } if request == id => {
                let item = item.clone();
                if text(result, "optionId") == "allow" {
                    self.notify(
                        "tool/output",
                        json!({ "turnId": active.turn, "itemId": item, "chunk": "README.md\n" }),
                    )
                    .await;
                    self.notify(
                        "tool/completed",
                        json!({ "turnId": active.turn, "itemId": item, "command": "ls",
                                "exitCode": 0, "output": "README.md\n" }),
                    )
                    .await;
                    self.finish(&active.thread, &active.turn, "Listed the files.")
                        .await;
                } else {
                    self.notify(
                        "tool/completed",
                        json!({ "turnId": active.turn, "itemId": item, "command": "ls", "exitCode": 1 }),
                    )
                    .await;
                    self.finish(&active.thread, &active.turn, "Skipped the command.")
                        .await;
                }
            }
            Awaiting::Question { request } if request == id => {
                let answer = result
                    .get("answers")
                    .and_then(|answers| answers.get("q1"))
                    .and_then(Value::as_array)
                    .and_then(|answers| answers.first())
                    .and_then(Value::as_str)
                    .unwrap_or("nothing")
                    .to_owned();
                self.finish(
                    &active.thread,
                    &active.turn,
                    &format!("You chose {answer}."),
                )
                .await;
            }
            _ => self.active = Some(active),
        }
    }
}
