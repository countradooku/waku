//! `fake-codex`: enough of `codex` for Waku and T3 Code to run turns.
//!
//! Subcommands:
//! - `app-server [--stdio] [-c k=v]…` speaks newline-delimited JSON-RPC: it
//!   initializes, opens and resumes threads, starts and interrupts turns, and
//!   streams timelines as agent-message, reasoning, command, file-change and
//!   MCP tool items;
//! - `exec … --output-last-message FILE -` answers T3's structured text
//!   generation;
//! - `mcp list --json` prints `[]`;
//! - `login status` reports an API-key login;
//! - `--version` prints `codex-cli <version>`.

use std::collections::HashSet;
use std::io::Read as _;
use std::sync::Arc;

use crossbeam_channel::Receiver;
use parking_lot::Mutex;
use serde_json::{Value, json};
use uuid::Uuid;
use waku_protocol::model::ProviderKind;

use super::{
    ActivityView, FakeConfig, Output, TurnRunner, now_ms, now_secs, split_data_url, stdin_messages,
    value_for_schema,
};
use crate::timeline::TimedEvent;

/// Reported everywhere a version is read. T3 gates models on it and parses
/// `/x.y.z` out of `userAgent`.
pub const VERSION: &str = "0.160.0";

pub fn main(args: Vec<String>) -> i32 {
    match run(args) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("fake-codex: {error:#}");
            1
        }
    }
}

fn run(args: Vec<String>) -> anyhow::Result<i32> {
    let argv0 = args.first().cloned().unwrap_or_else(|| "codex".into());
    let rest = &args[1.min(args.len())..];
    if rest.iter().any(|arg| arg == "--version" || arg == "-V") && subcommand(rest).is_none() {
        println!("codex-cli {VERSION}");
        return Ok(0);
    }
    match subcommand(rest).as_deref() {
        Some("app-server") => {
            if rest.iter().any(|arg| arg == "--help" || arg == "-h") {
                println!("Run the app server (fake-codex for Waku benchmarks)");
                return Ok(0);
            }
            let config = FakeConfig::resolve(&argv0)?;
            let output = Output::stdout(config.log.as_deref());
            serve(config, stdin_messages(), output);
            Ok(0)
        }
        Some("exec") => exec(rest),
        Some("mcp") => {
            println!("[]");
            Ok(0)
        }
        Some("login") => {
            println!("Logged in using an API key - fake-codex");
            Ok(0)
        }
        _ => {
            eprintln!(
                "fake-codex {VERSION}: supported commands are app-server, exec, mcp list, login status, --version"
            );
            Ok(2)
        }
    }
}

/// The first positional argument, skipping `-c key=value` style options.
fn subcommand(args: &[String]) -> Option<String> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-c" | "--config" | "-m" | "--model" | "-p" | "--profile" | "-s" | "--sandbox"
            | "-C" | "--cd" => {
                iter.next();
            }
            flag if flag.starts_with('-') => {}
            positional => return Some(positional.to_owned()),
        }
    }
    None
}

/// `codex exec`: read the prompt from stdin and write a structured answer to
/// `--output-last-message`, shaped by `--output-schema` when given.
fn exec(args: &[String]) -> anyhow::Result<i32> {
    let value_after = |flag: &str| {
        args.iter()
            .position(|arg| arg == flag)
            .and_then(|index| args.get(index + 1))
    };
    let mut prompt = String::new();
    if args.last().is_some_and(|arg| arg == "-") {
        std::io::stdin().read_to_string(&mut prompt)?;
    }
    let schema = value_after("--output-schema")
        .and_then(|path| std::fs::read_to_string(path).ok())
        .and_then(|text| serde_json::from_str::<Value>(&text).ok());
    let answer = match schema {
        Some(schema) => value_for_schema(&schema, "Bench task").to_string(),
        None => "Bench task".to_owned(),
    };
    if let Some(path) = value_after("--output-last-message") {
        std::fs::write(path, &answer)?;
    }
    println!("{answer}");
    Ok(0)
}

struct Session {
    thread_id: Option<String>,
    cwd: String,
    model: String,
    initialized: bool,
}

/// Serve `codex app-server` JSON-RPC until stdin closes.
pub fn serve(config: FakeConfig, input: Receiver<Value>, output: Output) {
    let session = Arc::new(Mutex::new(Session {
        thread_id: None,
        cwd: std::env::current_dir()
            .map(|cwd| cwd.display().to_string())
            .unwrap_or_default(),
        model: default_model(),
        initialized: false,
    }));
    let mut turns = TurnRunner::default();
    while let Ok(message) = input.recv() {
        output.log("in", &message);
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            // A response to a server request; the fake never sends any.
            continue;
        };
        let id = message.get("id").cloned();
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        let mut deferred = None;
        let reply = handle(
            method,
            &params,
            &session,
            &config,
            &output,
            &mut deferred,
            &mut turns,
        );
        if let Some(id) = id {
            let response = match reply {
                Ok(result) => json!({"id": id, "result": result}),
                Err((code, error)) => json!({"id": id, "error": {"code": code, "message": error}}),
            };
            output.log("out", &response);
            if output.send(&response).is_err() {
                break;
            }
        }
        // A turn starts only once its `turn/start` response is out, so its
        // `turn/started` can never overtake the response.
        if let Some(start) = deferred {
            start(&mut turns);
        }
    }
    turns.cancel();
    turns.join();
}

type Reply = Result<Value, (i64, String)>;
type Deferred = Option<Box<dyn FnOnce(&mut TurnRunner) + Send>>;

fn handle(
    method: &str,
    params: &Value,
    session: &Arc<Mutex<Session>>,
    config: &FakeConfig,
    output: &Output,
    deferred: &mut Deferred,
    turns: &mut TurnRunner,
) -> Reply {
    match method {
        "initialize" => {
            let mut session = session.lock();
            if session.initialized {
                return Err((-32600, "Already initialized".into()));
            }
            session.initialized = true;
            Ok(json!({
                "userAgent": format!("codex_cli_rs/{VERSION} (fake-codex; waku-bench)"),
                "codexHome": std::env::var("CODEX_HOME").unwrap_or_else(|_| {
                    std::env::temp_dir().join("fake-codex-home").display().to_string()
                }),
                "platformFamily": if cfg!(windows) { "windows" } else { "unix" },
                "platformOs": std::env::consts::OS,
            }))
        }
        "account/read" => Ok(json!({
            "account": {"type": "apiKey"},
            "requiresOpenaiAuth": true
        })),
        "account/rateLimits/read" => Ok(json!({"rateLimits": {}})),
        "skills/list" => {
            let data = params
                .get("cwds")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|cwd| json!({"cwd": cwd, "errors": [], "skills": []}))
                .collect::<Vec<_>>();
            Ok(json!({"data": data}))
        }
        "skills/extraRoots/set" => Ok(json!({})),
        "model/list" => Ok(json!({"data": models(), "nextCursor": null})),
        "thread/start" | "thread/resume" => {
            let mut session = session.lock();
            if let Some(cwd) = params.get("cwd").and_then(Value::as_str) {
                session.cwd = cwd.to_owned();
            }
            if let Some(model) = params.get("model").and_then(Value::as_str) {
                session.model = model.to_owned();
            }
            let thread_id = params
                .get("threadId")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            session.thread_id = Some(thread_id.clone());
            let ephemeral = params.get("ephemeral").and_then(Value::as_bool) == Some(true);
            Ok(thread_response(&session, &thread_id, params, ephemeral))
        }
        "thread/name/set" => {
            if let (Some(thread_id), Some(name)) = (
                params.get("threadId").and_then(Value::as_str),
                params.get("name").and_then(Value::as_str),
            ) {
                let _ = output.send(&json!({
                    "method": "thread/name/updated",
                    "params": {"threadId": thread_id, "threadName": name}
                }));
            }
            Ok(json!({}))
        }
        "turn/start" => {
            let thread_id = params
                .get("threadId")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| session.lock().thread_id.clone())
                .unwrap_or_default();
            let turn_id = Uuid::new_v4().to_string();
            let prompt = params
                .get("input")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            let started_at = now_secs();
            let cwd = session.lock().cwd.clone();
            super::scripted::apply_writes(&prompt, std::path::Path::new(&cwd));
            let background = super::scripted::background_command(&prompt);
            if params.get("outputSchema").is_none_or(Value::is_null) {
                super::scripted::codex_turn_started(&thread_id, &turn_id);
            }
            if let Some(command) = background {
                let (output, thread_id, turn_id) =
                    (output.clone(), thread_id.clone(), turn_id.clone());
                *deferred = Some(Box::new(move |_| {
                    super::scripted::codex_background_turn(&output, &thread_id, &turn_id, &command);
                }));
            } else if let Some(schema) = params
                .get("outputSchema")
                .filter(|schema| !schema.is_null())
            {
                // Structured side requests (Waku's title generation) get one
                // agent message that satisfies the schema, not a timeline.
                let text = value_for_schema(schema, "Bench task").to_string();
                let (output, turn_id, config) = (output.clone(), turn_id.clone(), config.clone());
                *deferred = Some(Box::new(move |turns| {
                    structured_turn(output, thread_id, turn_id, text, turns, &config);
                }));
            } else {
                let timeline = config.timeline_for(&prompt);
                output.log("note", &json!({"replay": timeline.name}));
                let mut mapper = CodexTurn::new(output.clone(), thread_id, turn_id.clone());
                let finisher = output.clone();
                let finish_ids = (mapper.thread_id.clone(), mapper.turn_id.clone());
                let config = config.clone();
                *deferred = Some(Box::new(move |turns| {
                    turns.start(
                        timeline,
                        &config,
                        move |event| mapper.event(event),
                        move |canceled| {
                            if canceled {
                                let _ = finisher.send(&turn_completed(
                                    &finish_ids.0,
                                    &finish_ids.1,
                                    "interrupted",
                                    None,
                                ));
                            }
                        },
                    );
                }));
            }
            Ok(json!({
                "turn": {"id": turn_id, "items": [], "status": "inProgress", "startedAt": started_at}
            }))
        }
        "thread/turns/list" => Ok(super::scripted::codex_turns_list(params)),
        "thread/revert" => Ok(super::scripted::codex_revert(params)),
        "thread/backgroundTerminals/list" => Ok(super::scripted::codex_list(params)),
        "thread/backgroundTerminals/terminate" => {
            Ok(super::scripted::codex_terminate(output, params))
        }
        "turn/interrupt" => {
            turns.cancel();
            Ok(json!({}))
        }
        "turn/steer" => Err((
            -32600,
            "fake-codex replays fixed timelines and does not support steering".into(),
        )),
        _ => Err((-32601, format!("Method not found: {method}"))),
    }
}

/// A turn whose only content is one agent message, used for structured
/// side requests.
fn structured_turn(
    output: Output,
    thread_id: String,
    turn_id: String,
    text: String,
    turns: &mut TurnRunner,
    config: &FakeConfig,
) {
    let timeline = crate::Timeline {
        name: "structured".into(),
        description: String::new(),
        events: vec![
            TimedEvent::new(0, "turnStarted", Value::Null),
            TimedEvent::new(1_000, "textDelta", Value::String(text)),
            TimedEvent::new(
                2_000,
                "turnFinished",
                json!({"success": true, "summary": null}),
            ),
        ],
    };
    let mut mapper = CodexTurn::new(output, thread_id, turn_id);
    let config = FakeConfig {
        loops: 1,
        rate: crate::Rate::Max,
        ..config.clone()
    };
    turns.start(
        Arc::new(timeline),
        &config,
        move |event| mapper.event(event),
        |_| {},
    );
}

fn default_model() -> String {
    waku_protocol::model_catalog::fallback_models(ProviderKind::Codex)
        .into_iter()
        .find(|model| model.is_default)
        .map(|model| model.id)
        .unwrap_or_else(|| "gpt-5.5".into())
}

fn models() -> Vec<Value> {
    waku_protocol::model_catalog::fallback_models(ProviderKind::Codex)
        .into_iter()
        .map(|model| {
            let efforts = if model.reasoning_efforts.is_empty() {
                vec![json!({"reasoningEffort": "medium", "description": "Balanced"})]
            } else {
                model
                    .reasoning_efforts
                    .iter()
                    .map(
                        |effort| json!({"reasoningEffort": effort.id, "description": effort.label}),
                    )
                    .collect()
            };
            json!({
                "id": model.id,
                "model": model.id,
                "displayName": model.name,
                "description": format!("{} (fake-codex)", model.name),
                "hidden": false,
                "isDefault": model.is_default,
                "defaultReasoningEffort": model
                    .default_reasoning_effort
                    .clone()
                    .unwrap_or_else(|| "medium".into()),
                "supportedReasoningEfforts": efforts,
            })
        })
        .collect()
}

fn thread_response(session: &Session, thread_id: &str, params: &Value, ephemeral: bool) -> Value {
    let now = now_secs();
    let approval_policy = params
        .get("approvalPolicy")
        .and_then(Value::as_str)
        .unwrap_or("never");
    json!({
        "approvalPolicy": approval_policy,
        "approvalsReviewer": "user",
        "cwd": session.cwd,
        "model": session.model,
        "modelProvider": "openai",
        "reasoningEffort": null,
        "sandbox": {"type": "dangerFullAccess"},
        "thread": {
            "id": thread_id,
            "cliVersion": VERSION,
            "createdAt": now,
            "updatedAt": now,
            "cwd": session.cwd,
            "ephemeral": ephemeral,
            "modelProvider": "openai",
            "preview": "",
            "projectId": null,
            "sessionId": thread_id,
            "source": "appServer",
            "status": {"type": "idle"},
            "turns": [],
            "name": null
        }
    })
}

fn turn_completed(thread_id: &str, turn_id: &str, status: &str, error: Option<&str>) -> Value {
    json!({
        "method": "turn/completed",
        "params": {
            "threadId": thread_id,
            "turn": {
                "id": turn_id,
                "items": [],
                "status": status,
                "completedAt": now_secs(),
                "error": error.map(|message| json!({"message": message})),
            }
        }
    })
}

/// Translates one turn's timeline events into app-server notifications.
pub struct CodexTurn {
    output: Output,
    thread_id: String,
    turn_id: String,
    agent: Option<(String, String)>,
    reasoning: Option<(String, String)>,
    started: HashSet<String>,
    next_item: u64,
}

impl CodexTurn {
    pub fn new(output: Output, thread_id: String, turn_id: String) -> Self {
        Self {
            output,
            thread_id,
            turn_id,
            agent: None,
            reasoning: None,
            started: HashSet::new(),
            next_item: 0,
        }
    }

    fn notify(&self, method: &str, mut params: Value) -> anyhow::Result<()> {
        params["threadId"] = json!(self.thread_id);
        params["turnId"] = json!(self.turn_id);
        self.output
            .send(&json!({"method": method, "params": params}))
    }

    fn item_started(&self, item: Value) -> anyhow::Result<()> {
        self.notify(
            "item/started",
            json!({"startedAtMs": now_ms(), "item": item}),
        )
    }

    fn item_completed(&self, item: Value) -> anyhow::Result<()> {
        self.notify(
            "item/completed",
            json!({"completedAtMs": now_ms(), "item": item}),
        )
    }

    fn item_id(&mut self, prefix: &str) -> String {
        self.next_item += 1;
        format!("{prefix}_{}_{}", &self.turn_id[..8], self.next_item)
    }

    fn close_agent(&mut self) -> anyhow::Result<()> {
        if let Some((id, text)) = self.agent.take() {
            self.item_completed(json!({"type": "agentMessage", "id": id, "text": text}))?;
        }
        Ok(())
    }

    fn close_reasoning(&mut self) -> anyhow::Result<()> {
        if let Some((id, text)) = self.reasoning.take() {
            self.item_completed(json!({
                "type": "reasoning", "id": id, "summary": [text], "content": []
            }))?;
        }
        Ok(())
    }

    pub fn event(&mut self, event: &TimedEvent) -> anyhow::Result<()> {
        match event.kind.as_str() {
            "turnStarted" => self.output.send(&json!({
                "method": "turn/started",
                "params": {
                    "threadId": self.thread_id,
                    "turn": {
                        "id": self.turn_id, "items": [], "status": "inProgress",
                        "startedAt": now_secs()
                    }
                }
            })),
            "textDelta" => {
                let delta = event.payload.as_str().unwrap_or_default();
                self.close_reasoning()?;
                if self.agent.is_none() {
                    let id = self.item_id("msg");
                    self.item_started(json!({"type": "agentMessage", "id": id, "text": ""}))?;
                    self.agent = Some((id, String::new()));
                }
                let (id, text) = self.agent.as_mut().expect("opened above");
                text.push_str(delta);
                let id = id.clone();
                self.notify(
                    "item/agentMessage/delta",
                    json!({"itemId": id, "delta": delta}),
                )
            }
            "reasoningDelta" => {
                let delta = event.payload.as_str().unwrap_or_default();
                self.close_agent()?;
                if self.reasoning.is_none() {
                    let id = self.item_id("rs");
                    self.item_started(
                        json!({"type": "reasoning", "id": id, "summary": [], "content": []}),
                    )?;
                    self.reasoning = Some((id, String::new()));
                }
                let (id, text) = self.reasoning.as_mut().expect("opened above");
                text.push_str(delta);
                let id = id.clone();
                self.notify(
                    "item/reasoning/summaryTextDelta",
                    json!({"itemId": id, "delta": delta, "summaryIndex": 0}),
                )
            }
            "richActivity" | "activity" => {
                let Some(activity) = ActivityView::parse(&event.kind, &event.payload) else {
                    return Ok(());
                };
                self.close_agent()?;
                self.close_reasoning()?;
                let first = self.started.insert(activity.id.clone());
                if first {
                    self.item_started(codex_item(&activity, false))?;
                }
                if activity.complete {
                    self.item_completed(codex_item(&activity, true))?;
                }
                Ok(())
            }
            "usageUpdated" => {
                let tokens = event.payload.get("contextTokens").and_then(Value::as_u64);
                let window = event.payload.get("contextWindow").and_then(Value::as_u64);
                let breakdown = |total: u64| {
                    json!({
                        "inputTokens": total.saturating_sub(total / 10),
                        "cachedInputTokens": 0,
                        "outputTokens": total / 10,
                        "reasoningOutputTokens": 0,
                        "totalTokens": total,
                    })
                };
                let total = tokens.unwrap_or_default();
                self.notify(
                    "thread/tokenUsage/updated",
                    json!({"tokenUsage": {
                        "total": breakdown(total),
                        "last": breakdown(total),
                        "modelContextWindow": window,
                    }}),
                )
            }
            "error" => self.notify(
                "error",
                json!({
                    "willRetry": false,
                    "error": {"message": event.payload.as_str().unwrap_or("error")}
                }),
            ),
            "turnFinished" => {
                self.close_agent()?;
                self.close_reasoning()?;
                // T3 drops a completion for a turn it has not registered
                // yet; a short gap after the last item avoids racing it.
                std::thread::sleep(std::time::Duration::from_millis(20));
                let success = event.payload.get("success").and_then(Value::as_bool) != Some(false);
                let summary = event.payload.get("summary").and_then(Value::as_str);
                self.output.send(&turn_completed(
                    &self.thread_id,
                    &self.turn_id,
                    if success { "completed" } else { "failed" },
                    (!success).then(|| summary.unwrap_or("turn failed")),
                ))
            }
            // Daemon-level events with no app-server equivalent.
            _ => Ok(()),
        }
    }
}

/// The app-server item for one timeline activity.
fn codex_item(activity: &ActivityView<'_>, completed: bool) -> Value {
    let status = match (completed, activity.failed) {
        (false, _) => "inProgress",
        (true, false) => "completed",
        (true, true) => "failed",
    };
    let cwd = crate::generate::FIXTURE_CWD;
    let output = |item: &mut Value| {
        if completed {
            item["aggregatedOutput"] = json!(activity.output.unwrap_or_default());
            item["exitCode"] = json!(if activity.failed { 1 } else { 0 });
        }
    };
    match activity.kind {
        "command" => {
            let mut item = json!({
                "type": "commandExecution", "id": activity.id, "command": activity.command(),
                "commandActions": [], "cwd": cwd, "status": status,
            });
            output(&mut item);
            item
        }
        "fileRead" | "fileSearch" | "fileList" => {
            // Codex reports reads and searches as shell commands annotated
            // with a parsed action; clients present them by the action.
            let path = activity
                .argument("file_path")
                .or_else(|| activity.argument("path"))
                .unwrap_or(".");
            let (command, action) = match activity.kind {
                "fileRead" => (
                    format!("sed -n '1,400p' {path}"),
                    json!({"type": "read", "command": format!("sed -n '1,400p' {path}"),
                           "name": path.rsplit('/').next().unwrap_or(path), "path": path}),
                ),
                "fileSearch" => {
                    let query = activity.argument("pattern").unwrap_or(activity.title);
                    let command = format!("rg -n '{query}' {path}");
                    (
                        command.clone(),
                        json!({"type": "search", "command": command, "query": query, "path": path}),
                    )
                }
                _ => (
                    format!("ls {path}"),
                    json!({"type": "listFiles", "command": format!("ls {path}"), "path": path}),
                ),
            };
            let mut item = json!({
                "type": "commandExecution", "id": activity.id, "command": command,
                "commandActions": [action], "cwd": cwd, "status": status,
            });
            output(&mut item);
            item
        }
        "fileChange" => {
            let changes = activity
                .file_changes
                .iter()
                .map(|(path, diff)| json!({"path": path, "diff": diff, "kind": {"type": "update"}}))
                .collect::<Vec<_>>();
            json!({"type": "fileChange", "id": activity.id, "changes": changes, "status": status})
        }
        _ => {
            let mut content = Vec::new();
            if let Some(text) = activity.output {
                content.push(json!({"type": "text", "text": text}));
            }
            for url in &activity.image_urls {
                if let Some((mime, data)) = split_data_url(url) {
                    content.push(json!({"type": "image", "data": data, "mimeType": mime}));
                }
            }
            let arguments = activity.arguments.clone().unwrap_or_else(|| json!({}));
            let mut item = json!({
                "type": "mcpToolCall", "id": activity.id, "server": "bench",
                "tool": activity.tool_name.unwrap_or(activity.title),
                "arguments": arguments, "status": status,
            });
            if completed {
                item["result"] = json!({"content": content, "structuredContent": null});
            }
            item
        }
    }
}
