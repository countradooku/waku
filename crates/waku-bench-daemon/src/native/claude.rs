//! `fake-claude`: enough of Claude Code for Waku and the Claude Agent SDK.
//!
//! Modes:
//! - `--input-format stream-json --output-format stream-json …`: the realtime
//!   transport both apps drive. It answers SDK control requests
//!   (`initialize`, `interrupt`, `mcp_set_servers`, `set_model`, …), echoes
//!   user messages under `--replay-user-messages`, and streams each prompt's
//!   timeline as `system/init`, partial `stream_event`s (with
//!   `--include-partial-messages`), per-block `assistant` snapshots,
//!   `tool_use` / `tool_result` pairs and a closing `result`;
//! - `-p` without stream input answers one-shot requests, such as T3's
//!   `--json-schema` title generation;
//! - `--version` prints `<version> (Claude Code)`.

use std::collections::HashSet;
use std::io::Read as _;
use std::time::Instant;

use crossbeam_channel::Receiver;
use serde_json::{Value, json};
use uuid::Uuid;
use waku_protocol::model::ProviderKind;

use super::{
    ActivityView, FakeConfig, Output, TurnRunner, diff_sides, split_data_url, stdin_messages,
    structured_patch, value_for_schema,
};
use crate::timeline::TimedEvent;

/// Above T3's supported floor (2.1.280) and every manifest `minVersion`.
pub const VERSION: &str = "2.1.300";

pub fn main(args: Vec<String>) -> i32 {
    match run(args) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("fake-claude: {error:#}");
            1
        }
    }
}

/// Command-line options the fake reads; everything else is accepted and
/// ignored, including the duplicate flags the Agent SDK passes.
#[derive(Clone, Debug, Default)]
pub struct Flags {
    pub session_id: Option<String>,
    pub model: Option<String>,
    pub permission_mode: Option<String>,
    pub json_schema: Option<String>,
    pub output_format: Option<String>,
    pub input_format: Option<String>,
    pub partial_messages: bool,
    pub replay_user_messages: bool,
    pub print: bool,
    pub version: bool,
}

impl Flags {
    pub fn parse(args: &[String]) -> Self {
        let mut flags = Self::default();
        let mut iter = args.iter().peekable();
        while let Some(arg) = iter.next() {
            let (name, inline) = match arg.split_once('=') {
                Some((name, value)) if name.starts_with("--") => (name, Some(value.to_owned())),
                _ => (arg.as_str(), None),
            };
            let mut value = || inline.clone().or_else(|| iter.next().cloned());
            match name {
                "--version" | "-v" => flags.version = true,
                "-p" | "--print" => flags.print = true,
                "--include-partial-messages" => flags.partial_messages = true,
                "--replay-user-messages" => flags.replay_user_messages = true,
                "--session-id" | "--resume" => flags.session_id = value(),
                "--model" => flags.model = value(),
                "--permission-mode" => flags.permission_mode = value(),
                "--json-schema" => flags.json_schema = value(),
                "--output-format" => flags.output_format = value(),
                "--input-format" => flags.input_format = value(),
                _ => {}
            }
        }
        flags
    }
}

fn run(args: Vec<String>) -> anyhow::Result<i32> {
    let argv0 = args.first().cloned().unwrap_or_else(|| "claude".into());
    let flags = Flags::parse(&args[1.min(args.len())..]);
    if flags.version {
        println!("{VERSION} (Claude Code)");
        return Ok(0);
    }
    if flags.input_format.as_deref() == Some("stream-json") {
        let config = FakeConfig::resolve(&argv0)?;
        let output = Output::stdout(config.log.as_deref());
        serve(config, flags, stdin_messages(), output);
        return Ok(0);
    }
    if flags.print {
        return one_shot(&flags);
    }
    eprintln!("fake-claude {VERSION}: use -p, --input-format stream-json, or --version");
    Ok(2)
}

/// `claude -p`: read the prompt from stdin and answer once.
fn one_shot(flags: &Flags) -> anyhow::Result<i32> {
    let mut prompt = String::new();
    std::io::stdin().read_to_string(&mut prompt)?;
    let session_id = flags
        .session_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let structured = flags
        .json_schema
        .as_deref()
        .and_then(|schema| serde_json::from_str::<Value>(schema).ok())
        .map(|schema| value_for_schema(&schema, "Bench task"));
    let text = structured
        .as_ref()
        .map(Value::to_string)
        .unwrap_or_else(|| "Bench task".into());
    let mut result = json!({
        "type": "result", "subtype": "success", "is_error": false,
        "duration_ms": 1, "duration_api_ms": 0, "num_turns": 1,
        "result": text, "session_id": session_id, "total_cost_usd": 0.0,
        "usage": usage(0, 0), "uuid": Uuid::new_v4().to_string(),
    });
    if let Some(structured) = structured {
        result["structured_output"] = structured;
    }
    match flags.output_format.as_deref() {
        Some("json") => println!("{result}"),
        Some("stream-json") => println!("{result}"),
        _ => println!("{text}"),
    }
    Ok(0)
}

fn usage(input: u64, output: u64) -> Value {
    json!({
        "input_tokens": input,
        "output_tokens": output,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
    })
}

fn default_model() -> String {
    waku_protocol::model_catalog::fallback_models(ProviderKind::Claude)
        .into_iter()
        .find(|model| model.is_default)
        .map(|model| model.id)
        .unwrap_or_else(|| "claude-opus-5".into())
}

/// The `initialize` control response: an API-key account (no
/// `subscriptionType`, which would send T3 to Anthropic's usage endpoint)
/// and the stock model catalog.
fn initialize_response() -> Value {
    let models = waku_protocol::model_catalog::fallback_models(ProviderKind::Claude)
        .into_iter()
        .map(|model| {
            json!({"value": model.id, "displayName": model.name,
                   "description": format!("{} (fake-claude)", model.name)})
        })
        .collect::<Vec<_>>();
    json!({
        "commands": [],
        "agents": [],
        "models": models,
        "output_style": "default",
        "available_output_styles": ["default"],
        "account": {
            "email": "bench@example.invalid",
            "apiKeySource": "ANTHROPIC_API_KEY",
            "apiProvider": "firstParty",
            "tokenSource": "apiKey"
        }
    })
}

/// Serve the stream-json transport until stdin closes.
pub fn serve(config: FakeConfig, flags: Flags, input: Receiver<Value>, output: Output) {
    let session_id = flags
        .session_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let model = flags.model.clone().unwrap_or_else(default_model);
    let permission_mode = flags
        .permission_mode
        .clone()
        .unwrap_or_else(|| "default".into());
    let mut turns = TurnRunner::default();
    let mut num_turns = 0_u64;
    while let Ok(message) = input.recv() {
        output.log("in", &message);
        match message.get("type").and_then(Value::as_str) {
            Some("control_request") => {
                let request_id = message.get("request_id").cloned().unwrap_or(Value::Null);
                let subtype = message
                    .pointer("/request/subtype")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let response = match subtype {
                    "initialize" => initialize_response(),
                    "interrupt" => {
                        turns.cancel();
                        json!({})
                    }
                    "mcp_set_servers" => json!({"added": [], "removed": [], "errors": {}}),
                    _ => json!({}),
                };
                let reply = json!({
                    "type": "control_response",
                    "response": {"subtype": "success", "request_id": request_id, "response": response}
                });
                output.log("out", &reply);
                if output.send(&reply).is_err() {
                    break;
                }
            }
            Some("user") => {
                let prompt = prompt_text(&message);
                if flags.replay_user_messages {
                    let mut echo = message.clone();
                    echo["isReplay"] = json!(true);
                    echo["session_id"] = json!(session_id);
                    echo["parent_tool_use_id"] = Value::Null;
                    echo["uuid"] = json!(Uuid::new_v4().to_string());
                    let _ = output.send(&echo);
                }
                num_turns += 1;
                let timeline = config.timeline_for(&prompt);
                output.log("note", &json!({"replay": timeline.name}));
                let mut mapper = ClaudeTurn::new(
                    output.clone(),
                    session_id.clone(),
                    model.clone(),
                    flags.partial_messages,
                    num_turns,
                );
                let init = json!({
                    "type": "system", "subtype": "init", "session_id": session_id,
                    "cwd": std::env::current_dir().map(|cwd| cwd.display().to_string()).unwrap_or_default(),
                    "tools": ["Bash", "Read", "Edit", "Grep", "Glob"], "mcp_servers": [],
                    "model": model, "permissionMode": permission_mode,
                    "slash_commands": [], "apiKeySource": "ANTHROPIC_API_KEY",
                    "claude_code_version": VERSION, "output_style": "default",
                    "agents": [], "skills": [], "plugins": [],
                    "uuid": Uuid::new_v4().to_string(),
                });
                let finisher = output.clone();
                let finish_session = session_id.clone();
                turns.start(
                    timeline,
                    &config,
                    move |event| {
                        if event.kind == "turnStarted" {
                            mapper.output.send(&init)?;
                        }
                        mapper.event(event)
                    },
                    move |canceled| {
                        if canceled {
                            let _ = finisher.send(&json!({
                                "type": "result", "subtype": "error_during_execution",
                                "is_error": true, "errors": ["Request interrupted by user"],
                                "duration_ms": 0, "duration_api_ms": 0, "num_turns": num_turns,
                                "session_id": finish_session, "total_cost_usd": 0.0,
                                "usage": usage(0, 0), "uuid": Uuid::new_v4().to_string(),
                            }));
                        }
                    },
                );
            }
            // Responses to permission prompts, keep-alives and anything
            // else need no answer.
            _ => {}
        }
    }
    // End of input: let the turn in flight finish, as the CLI does.
    turns.join();
}

fn prompt_text(message: &Value) -> String {
    match message.pointer("/message/content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum BlockKind {
    Text,
    Thinking,
}

struct Block {
    kind: BlockKind,
    text: String,
}

/// Translates one turn's timeline events into stream-json frames.
pub struct ClaudeTurn {
    output: Output,
    session_id: String,
    model: String,
    partial: bool,
    num_turns: u64,
    message_id: Option<String>,
    block: Option<Block>,
    index: usize,
    messages: u64,
    tools: HashSet<String>,
    last_text: String,
    context_tokens: u64,
    context_window: Option<u64>,
    output_tokens: u64,
    started: Instant,
}

impl ClaudeTurn {
    pub fn new(
        output: Output,
        session_id: String,
        model: String,
        partial: bool,
        num_turns: u64,
    ) -> Self {
        Self {
            output,
            session_id,
            model,
            partial,
            num_turns,
            message_id: None,
            block: None,
            index: 0,
            messages: 0,
            tools: HashSet::new(),
            last_text: String::new(),
            context_tokens: 0,
            context_window: None,
            output_tokens: 0,
            started: Instant::now(),
        }
    }

    fn frame(&self, mut value: Value) -> anyhow::Result<()> {
        value["parent_tool_use_id"] = Value::Null;
        value["session_id"] = json!(self.session_id);
        value["uuid"] = json!(Uuid::new_v4().to_string());
        self.output.send(&value)
    }

    fn stream(&self, event: Value) -> anyhow::Result<()> {
        if self.partial {
            self.frame(json!({"type": "stream_event", "event": event}))
        } else {
            Ok(())
        }
    }

    fn ensure_message(&mut self) -> anyhow::Result<String> {
        if let Some(id) = &self.message_id {
            return Ok(id.clone());
        }
        self.messages += 1;
        let id = format!("msg_bench_{}_{}", self.num_turns, self.messages);
        self.message_id = Some(id.clone());
        self.index = 0;
        self.stream(json!({
            "type": "message_start",
            "message": {
                "id": id, "type": "message", "role": "assistant", "model": self.model,
                "content": [], "stop_reason": null, "stop_sequence": null,
                "usage": usage(self.context_tokens, 0)
            }
        }))?;
        Ok(id)
    }

    /// One `assistant` frame carrying a single content block, as Claude Code
    /// emits them.
    fn snapshot(&self, id: &str, block: Value) -> anyhow::Result<()> {
        self.frame(json!({
            "type": "assistant",
            "message": {
                "id": id, "type": "message", "role": "assistant", "model": self.model,
                "content": [block], "stop_reason": null, "stop_sequence": null,
                "usage": usage(self.context_tokens, self.output_tokens)
            }
        }))
    }

    fn close_block(&mut self) -> anyhow::Result<()> {
        let Some(block) = self.block.take() else {
            return Ok(());
        };
        let id = self.message_id.clone().unwrap_or_default();
        self.stream(json!({"type": "content_block_stop", "index": self.index}))?;
        self.index += 1;
        match block.kind {
            BlockKind::Text => {
                self.snapshot(&id, json!({"type": "text", "text": block.text}))?;
                self.last_text = block.text;
            }
            BlockKind::Thinking => self.snapshot(
                &id,
                json!({"type": "thinking", "thinking": block.text, "signature": "fake"}),
            )?,
        }
        Ok(())
    }

    fn close_message(&mut self, stop_reason: &str) -> anyhow::Result<()> {
        self.close_block()?;
        if self.message_id.take().is_some() {
            self.stream(json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": null},
                "usage": {"output_tokens": self.output_tokens}
            }))?;
            self.stream(json!({"type": "message_stop"}))?;
        }
        Ok(())
    }

    fn delta(&mut self, kind: BlockKind, text: &str) -> anyhow::Result<()> {
        if self.block.as_ref().is_some_and(|block| block.kind != kind) {
            self.close_block()?;
        }
        self.ensure_message()?;
        if self.block.is_none() {
            let content_block = match kind {
                BlockKind::Text => json!({"type": "text", "text": ""}),
                BlockKind::Thinking => json!({"type": "thinking", "thinking": "", "signature": ""}),
            };
            self.stream(json!({
                "type": "content_block_start", "index": self.index, "content_block": content_block
            }))?;
            self.block = Some(Block {
                kind,
                text: String::new(),
            });
        }
        self.output_tokens += 1;
        self.block
            .as_mut()
            .expect("opened above")
            .text
            .push_str(text);
        let delta = match kind {
            BlockKind::Text => json!({"type": "text_delta", "text": text}),
            BlockKind::Thinking => json!({"type": "thinking_delta", "thinking": text}),
        };
        self.stream(json!({"type": "content_block_delta", "index": self.index, "delta": delta}))
    }

    fn tool(&mut self, activity: &ActivityView<'_>) -> anyhow::Result<()> {
        let tool_use_id = format!(
            "toolu_{}",
            activity
                .id
                .chars()
                .filter(char::is_ascii_alphanumeric)
                .collect::<String>()
        );
        if self.tools.insert(tool_use_id.clone()) {
            self.close_block()?;
            let id = self.ensure_message()?;
            let (name, input) = claude_tool(activity);
            self.stream(json!({
                "type": "content_block_start", "index": self.index,
                "content_block": {"type": "tool_use", "id": tool_use_id, "name": name, "input": {}}
            }))?;
            self.stream(json!({
                "type": "content_block_delta", "index": self.index,
                "delta": {"type": "input_json_delta", "partial_json": input.to_string()}
            }))?;
            self.stream(json!({"type": "content_block_stop", "index": self.index}))?;
            self.index += 1;
            self.snapshot(
                &id,
                json!({"type": "tool_use", "id": tool_use_id, "name": name, "input": input}),
            )?;
            self.close_message("tool_use")?;
        }
        if activity.complete {
            let content = if activity.image_urls.is_empty() {
                json!(tool_result_text(activity))
            } else {
                Value::Array(
                    activity
                        .image_urls
                        .iter()
                        .filter_map(|url| split_data_url(url))
                        .map(|(mime, data)| {
                            json!({"type": "image", "source": {
                                "type": "base64", "media_type": mime, "data": data
                            }})
                        })
                        .collect(),
                )
            };
            let mut frame = json!({
                "type": "user",
                "message": {"role": "user", "content": [{
                    "type": "tool_result", "tool_use_id": tool_use_id,
                    "content": content, "is_error": activity.failed
                }]}
            });
            // Edits carry the hunks they applied, as Claude Code's do; that
            // is where clients read the positioned diff from.
            if activity.kind == "fileChange"
                && let Some((path, diff)) = activity.file_changes.first()
            {
                let (old, new) = diff_sides(diff);
                frame["tool_use_result"] = json!({
                    "filePath": format!("{}/{path}", crate::generate::FIXTURE_CWD),
                    "oldString": old,
                    "newString": new,
                    "originalFile": null,
                    "structuredPatch": structured_patch(diff),
                    "userModified": false,
                    "replaceAll": false,
                });
            }
            self.frame(frame)?;
        }
        Ok(())
    }

    pub fn event(&mut self, event: &TimedEvent) -> anyhow::Result<()> {
        match event.kind.as_str() {
            "textDelta" => self.delta(BlockKind::Text, event.payload.as_str().unwrap_or_default()),
            "reasoningDelta" => self.delta(
                BlockKind::Thinking,
                event.payload.as_str().unwrap_or_default(),
            ),
            "richActivity" | "activity" => match ActivityView::parse(&event.kind, &event.payload) {
                Some(activity) => self.tool(&activity),
                None => Ok(()),
            },
            "usageUpdated" => {
                if let Some(tokens) = event.payload.get("contextTokens").and_then(Value::as_u64) {
                    self.context_tokens = tokens;
                }
                if let Some(window) = event.payload.get("contextWindow").and_then(Value::as_u64) {
                    self.context_window = Some(window);
                }
                Ok(())
            }
            "turnFinished" => {
                self.close_message("end_turn")?;
                let success = event.payload.get("success").and_then(Value::as_bool) != Some(false);
                let mut model_usage = json!({
                    "inputTokens": self.context_tokens, "outputTokens": self.output_tokens,
                    "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0,
                    "webSearchRequests": 0, "costUSD": 0.0,
                });
                if let Some(window) = self.context_window {
                    model_usage["contextWindow"] = json!(window);
                }
                let mut result = json!({
                    "type": "result",
                    "subtype": if success { "success" } else { "error_during_execution" },
                    "is_error": !success,
                    "duration_ms": self.started.elapsed().as_millis() as u64,
                    "duration_api_ms": self.started.elapsed().as_millis() as u64,
                    "num_turns": self.num_turns,
                    "total_cost_usd": 0.0,
                    "usage": usage(self.context_tokens, self.output_tokens),
                    "modelUsage": {self.model.clone(): model_usage},
                    "permission_denials": [],
                    "stop_reason": "end_turn",
                });
                if success {
                    result["result"] = json!(self.last_text);
                } else {
                    let summary = event.payload.get("summary").and_then(Value::as_str);
                    result["errors"] = json!([summary.unwrap_or("turn failed")]);
                }
                let mut result = result;
                result["session_id"] = json!(self.session_id);
                result["uuid"] = json!(Uuid::new_v4().to_string());
                self.output.send(&result)
            }
            _ => Ok(()),
        }
    }
}

/// The Claude Code tool call that produces this activity.
fn claude_tool(activity: &ActivityView<'_>) -> (String, Value) {
    let path = activity
        .argument("file_path")
        .or_else(|| activity.argument("path"))
        .unwrap_or(".")
        .to_owned();
    match activity.kind {
        "command" => ("Bash".into(), json!({"command": activity.command()})),
        "fileChange" => {
            let (file, diff) = activity
                .file_changes
                .first()
                .copied()
                .unwrap_or((&path, ""));
            let (old, new) = diff_sides(diff);
            (
                "Edit".into(),
                json!({"file_path": format!("{}/{file}", crate::generate::FIXTURE_CWD),
                       "old_string": old, "new_string": new}),
            )
        }
        "fileRead" => ("Read".into(), json!({"file_path": path})),
        "fileSearch" => (
            "Grep".into(),
            json!({"pattern": activity.argument("pattern").unwrap_or(activity.title), "path": path}),
        ),
        "fileList" => ("Glob".into(), json!({"pattern": "*", "path": path})),
        _ if !activity.image_urls.is_empty() => ("Read".into(), json!({"file_path": path})),
        _ => (
            activity.tool_name.unwrap_or(activity.title).to_owned(),
            activity.arguments.clone().unwrap_or_else(|| json!({})),
        ),
    }
}

fn tool_result_text(activity: &ActivityView<'_>) -> String {
    match activity.kind {
        "fileChange" => {
            let file = activity
                .file_changes
                .first()
                .map_or("the file", |(path, _)| path);
            format!("The file {file} has been updated successfully.")
        }
        _ => activity.output.unwrap_or_default().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_accept_sdk_argv_with_duplicates_and_inline_values() {
        let args = [
            "--output-format",
            "stream-json",
            "--verbose",
            "--input-format",
            "stream-json",
            "--thinking-display",
            "summarized",
            "--setting-sources=user,project,local",
            "--session-id",
            "abc",
            "--include-partial-messages",
            "--thinking-display",
            "summarized",
            "--model=claude-x",
        ]
        .map(String::from);
        let flags = Flags::parse(&args);
        assert_eq!(flags.input_format.as_deref(), Some("stream-json"));
        assert_eq!(flags.session_id.as_deref(), Some("abc"));
        assert_eq!(flags.model.as_deref(), Some("claude-x"));
        assert!(flags.partial_messages);
        assert!(!flags.replay_user_messages);
    }
}
