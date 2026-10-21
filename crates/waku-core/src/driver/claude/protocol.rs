//! Claude Code's stream-json control protocol: launch arguments, the frames
//! Waku writes, and the pure classification of what the CLI reports
//! (RFC 0002 "Claude transport", D-3).
//!
//! Flags and payloads were read off the real CLI and the Agent SDK's own
//! invocation, and the frame schemas are the ones CLI 2.1.304 embeds for its
//! SDK (`rate_limit_event`, `result`, `task_*`, `control_request`). Nothing
//! here performs I/O.

use serde_json::{Value, json};
use waku_protocol::model::ProviderResumeCursor;
use waku_protocol::orchestration::{FailureClass, ProviderFailure, RuntimeMode, Timestamp};

use crate::driver::v2::Selection;

/// The provider id fixtures and capability snapshots use.
pub const CLAUDE_PROVIDER: &str = "claude";
/// The replay protocol name (RFC 0002 "File format").
pub const CLAUDE_PROTOCOL: &str = "claude.stream-json";

/// Option ids for Claude's approvals. `can_use_tool` has no native options,
/// so these constants are the ids (RFC 0002 "Claude").
pub const ALLOW: &str = "allow";
pub const DENY: &str = "deny";

/// The permission posture Claude is launched with.
pub fn permission_mode(mode: RuntimeMode) -> &'static str {
    match mode {
        RuntimeMode::Ask => "default",
        RuntimeMode::AutoAcceptEdits => "acceptEdits",
        RuntimeMode::Auto => "auto",
        _ => "bypassPermissions",
    }
}

/// The model id to hand the CLI for a context-window choice. Claude reaches
/// its 1M window through a `[1m]` suffix on the model id, so the window
/// travels with `--model` and with `set_model`.
pub fn wire_model(model: Option<&str>, context_window: Option<&str>) -> Option<String> {
    let model = model?;
    if context_window == Some("1m") && !model.ends_with("[1m]") {
        Some(format!("{model}[1m]"))
    } else {
        Some(model.to_owned())
    }
}

/// How a process names its conversation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Launch {
    /// A conversation that has never run: `--session-id <uuid>`.
    New(String),
    /// A conversation with history: `--resume <uuid>`.
    Resume(String),
}

impl Launch {
    pub fn session_id(&self) -> &str {
        match self {
            Self::New(id) | Self::Resume(id) => id,
        }
    }
}

/// `claude -p` in streaming-input mode. `--permission-prompt-tool stdio` is
/// absent from `--help` and is what lets Supervised ask the host instead of
/// deciding alone.
pub fn launch_args(
    selection: &Selection,
    launch: &Launch,
    system_prompt: Option<&str>,
) -> Vec<String> {
    let mut args = [
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
        // Newer models omit readable thinking unless a summary is requested.
        "--thinking-display",
        "summarized",
        // Echoes each user message back: the turn starts at the echo.
        "--replay-user-messages",
        "--permission-prompt-tool",
        "stdio",
        "--permission-mode",
    ]
    .map(str::to_owned)
    .to_vec();
    args.push(permission_mode(selection.runtime_mode).to_owned());
    if selection.runtime_mode == RuntimeMode::FullAccess {
        args.push("--dangerously-skip-permissions".to_owned());
    }
    if let Some(model) = wire_model(
        selection.model.as_deref(),
        selection.context_window.as_deref(),
    ) {
        args.extend(["--model".to_owned(), model]);
    }
    if let Some(effort) = &selection.reasoning_effort {
        args.extend(["--effort".to_owned(), effort.clone()]);
    }
    if let Some(prompt) = system_prompt.filter(|prompt| !prompt.is_empty()) {
        args.extend(["--append-system-prompt".to_owned(), prompt.to_owned()]);
    }
    match launch {
        Launch::New(id) => args.extend(["--session-id".to_owned(), id.clone()]),
        Launch::Resume(id) => args.extend(["--resume".to_owned(), id.clone()]),
    }
    args
}

/// The stream-json user message prompts and steers are both delivered as.
pub fn user_message(text: &str) -> Value {
    json!({
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
        "parent_tool_use_id": null
    })
}

pub fn control_request(request_id: &str, request: Value) -> Value {
    json!({"type": "control_request", "request_id": request_id, "request": request})
}

pub fn control_success(request_id: &str, response: Value) -> Value {
    json!({
        "type": "control_response",
        "response": {"subtype": "success", "request_id": request_id, "response": response}
    })
}

pub fn allow() -> Value {
    json!({"behavior": "allow"})
}

pub fn deny(message: &str) -> Value {
    json!({"behavior": "deny", "message": message})
}

/// The `updatedInput` an answered `AskUserQuestion` carries: the original
/// questions plus answers keyed by the full question text (Claude resolves
/// answers by text). Multi-select answers stay arrays.
pub fn question_answers(input: &Value, answers: &[(String, Vec<String>)]) -> Value {
    let mut values = serde_json::Map::new();
    for (question_id, chosen) in answers {
        let multi_select = input
            .get("questions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|question| {
                question.get("question").and_then(Value::as_str) == Some(question_id.as_str())
            })
            .and_then(|question| question.get("multiSelect"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let value = if multi_select {
            json!(chosen)
        } else {
            Value::String(chosen.first().cloned().unwrap_or_default())
        };
        values.insert(question_id.clone(), value);
    }
    json!({
        "behavior": "allow",
        "updatedInput": {
            "questions": input.get("questions").cloned().unwrap_or(Value::Array(Vec::new())),
            "answers": values
        }
    })
}

/// The opaque cursor a Claude provider thread persists.
pub fn cursor(session_id: &str, resume_at: Option<&str>) -> ProviderResumeCursor {
    ProviderResumeCursor::Claude {
        session_id: session_id.to_owned(),
        resume_at: resume_at.map(str::to_owned),
    }
}

/// What a `rate_limit_event` said about the account.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RateLimit {
    /// The window is exhausted (`status: "rejected"` without overage).
    pub blocked: bool,
    /// Epoch milliseconds (`resetsAt` is epoch seconds on the wire).
    pub reset_at: Option<Timestamp>,
    pub limit_type: Option<String>,
}

/// Reads a `rate_limit_event`. A rejected status only counts as blocking
/// when overage is not carrying the session.
pub fn rate_limit(frame: &Value) -> Option<RateLimit> {
    let info = frame.get("rate_limit_info")?;
    let status = info.get("status").and_then(Value::as_str)?;
    let overage = matches!(
        info.get("overageStatus").and_then(Value::as_str),
        Some("allowed" | "allowed_warning")
    ) || info.get("isUsingOverage").and_then(Value::as_bool) == Some(true)
        || info.get("overageInUse").and_then(Value::as_bool) == Some(true);
    Some(RateLimit {
        blocked: status == "rejected" && !overage,
        reset_at: info
            .get("resetsAt")
            .and_then(Value::as_i64)
            .map(|seconds| seconds * 1000),
        limit_type: info
            .get("rateLimitType")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

/// How a `result` frame ended the turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultOutcome {
    Completed,
    Interrupted,
    Failed(ProviderFailure),
}

/// Evidence collected during the turn that a failure is a usage limit.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LimitEvidence {
    pub rate_limit: Option<RateLimit>,
    /// The last root assistant message carried `error: "rate_limit"`.
    pub assistant_rate_limited: bool,
}

/// Classifies a `result`. The reset time comes from the structured
/// `rate_limit_event`, never from the result text.
pub fn classify_result(
    frame: &Value,
    interrupted: bool,
    evidence: &LimitEvidence,
) -> ResultOutcome {
    if interrupted {
        return ResultOutcome::Interrupted;
    }
    let subtype = frame
        .get("subtype")
        .and_then(Value::as_str)
        .unwrap_or("success");
    let is_error = frame.get("is_error").and_then(Value::as_bool) == Some(true);
    let terminal_reason = frame.get("terminal_reason").and_then(Value::as_str);
    if matches!(terminal_reason, Some("aborted_tools" | "aborted_streaming")) {
        return ResultOutcome::Interrupted;
    }
    if subtype == "success" && !is_error {
        return ResultOutcome::Completed;
    }
    let errors = frame
        .get("errors")
        .and_then(Value::as_array)
        .map(|errors| {
            errors
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    if subtype != "success" && errors.to_ascii_lowercase().contains("interrupt") {
        return ResultOutcome::Interrupted;
    }
    let api_status = frame.get("api_error_status").and_then(Value::as_i64);
    let message = frame
        .get("result")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .filter(|text| !text.trim().is_empty())
        .or_else(|| (!errors.is_empty()).then_some(errors.clone()))
        .unwrap_or_else(|| "Claude Code reported an error.".to_owned());
    let message = message.chars().take(2000).collect::<String>();
    let blocked = evidence
        .rate_limit
        .as_ref()
        .is_some_and(|limit| limit.blocked);
    let usage_limited = blocked || evidence.assistant_rate_limited || api_status == Some(429);
    let (class, code) = if usage_limited {
        (FailureClass::UsageLimit, "usage_limit".to_owned())
    } else if frame.get("error").and_then(Value::as_str) == Some("authentication_failed") {
        (
            FailureClass::PermissionError,
            "authentication_failed".to_owned(),
        )
    } else {
        (FailureClass::Unknown, subtype.to_owned())
    };
    let mut failure = ProviderFailure::new(class, message).with_code(code);
    if usage_limited {
        failure.reset_at = evidence
            .rate_limit
            .as_ref()
            .and_then(|limit| limit.reset_at);
        failure.retryable = Some(true);
    }
    ResultOutcome::Failed(failure)
}

/// The context window of the model that served the turn, from the result's
/// per-model usage map, falling back to the largest reported window.
pub fn context_window(frame: &Value, last_model: Option<&str>) -> Option<u64> {
    let models = frame.get("modelUsage")?.as_object()?;
    if let Some(model) = last_model {
        for (key, entry) in models {
            if key == model || entry.get("canonicalModel").and_then(Value::as_str) == Some(model) {
                return entry.get("contextWindow").and_then(Value::as_u64);
            }
        }
    }
    models
        .values()
        .filter_map(|entry| entry.get("contextWindow").and_then(Value::as_u64))
        .max()
}

/// The context occupancy of one API call from a Claude `usage` object: the
/// last iteration when present (summed outer fields double-count caches).
pub fn context_tokens(usage: &Value) -> Option<u64> {
    let call = usage
        .get("iterations")
        .and_then(Value::as_array)
        .and_then(|iterations| iterations.last())
        .unwrap_or(usage);
    let field = |name: &str| call.get(name).and_then(Value::as_u64).unwrap_or(0);
    let total = field("input_tokens")
        + field("cache_read_input_tokens")
        + field("cache_creation_input_tokens")
        + field("output_tokens");
    (total > 0).then_some(total)
}

/// The text of a tool result's `content` (a string or text blocks).
pub fn tool_result_text(content: Option<&Value>) -> Option<String> {
    let text = match content? {
        Value::String(text) => text.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|block| match block {
                Value::String(text) => Some(text.clone()),
                block if block.get("type").and_then(Value::as_str) == Some("text") => {
                    block.get("text").and_then(Value::as_str).map(str::to_owned)
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Null => return None,
        other => other.to_string(),
    };
    (!text.is_empty()).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_modes_map_to_claude_permission_modes() {
        assert_eq!(permission_mode(RuntimeMode::Ask), "default");
        assert_eq!(permission_mode(RuntimeMode::AutoAcceptEdits), "acceptEdits");
        assert_eq!(permission_mode(RuntimeMode::Auto), "auto");
        assert_eq!(
            permission_mode(RuntimeMode::FullAccess),
            "bypassPermissions"
        );
    }

    #[test]
    fn launch_requests_readable_reasoning_and_the_context_window() {
        let selection = Selection {
            model: Some("claude-opus-5".into()),
            context_window: Some("1m".into()),
            reasoning_effort: Some("high".into()),
            runtime_mode: RuntimeMode::FullAccess,
            ..Selection::default()
        };
        let args = launch_args(&selection, &Launch::Resume("s".into()), None);
        for pair in [
            ["--thinking-display", "summarized"],
            ["--permission-prompt-tool", "stdio"],
            ["--permission-mode", "bypassPermissions"],
            ["--model", "claude-opus-5[1m]"],
            ["--effort", "high"],
            ["--resume", "s"],
        ] {
            assert!(args.windows(2).any(|window| window == pair), "{pair:?}");
        }
        assert!(args.contains(&"--dangerously-skip-permissions".to_owned()));
    }

    #[test]
    fn rate_limit_resets_are_epoch_seconds() {
        let limit = rate_limit(&json!({"type":"rate_limit_event","rate_limit_info":{
            "status":"rejected","resetsAt":1767240000,"rateLimitType":"five_hour"}}))
        .unwrap();
        assert!(limit.blocked);
        assert_eq!(limit.reset_at, Some(1_767_240_000_000));
        let overage = rate_limit(&json!({"rate_limit_info":{
            "status":"rejected","resetsAt":1,"overageStatus":"allowed"}}))
        .unwrap();
        assert!(!overage.blocked);
    }

    #[test]
    fn results_classify_interrupts_limits_and_failures() {
        let evidence = LimitEvidence::default();
        assert_eq!(
            classify_result(
                &json!({"subtype":"success","is_error":false}),
                false,
                &evidence
            ),
            ResultOutcome::Completed
        );
        assert_eq!(
            classify_result(
                &json!({"subtype":"success","is_error":false}),
                true,
                &evidence
            ),
            ResultOutcome::Interrupted
        );
        assert_eq!(
            classify_result(
                &json!({"subtype":"error_during_execution","is_error":true,"errors":["Request interrupted by user"]}),
                false,
                &evidence
            ),
            ResultOutcome::Interrupted
        );
        let limited = LimitEvidence {
            rate_limit: Some(RateLimit {
                blocked: true,
                reset_at: Some(5_000),
                limit_type: None,
            }),
            assistant_rate_limited: false,
        };
        let ResultOutcome::Failed(failure) = classify_result(
            &json!({"subtype":"success","is_error":true,"result":"You've hit your limit"}),
            false,
            &limited,
        ) else {
            panic!("a limited result fails");
        };
        assert_eq!(failure.class, FailureClass::UsageLimit);
        assert_eq!(failure.reset_at, Some(5_000));
        let ResultOutcome::Failed(failure) = classify_result(
            &json!({"subtype":"error_max_turns","is_error":true,"errors":["max turns"]}),
            false,
            &evidence,
        ) else {
            panic!("an error result fails");
        };
        assert_eq!(failure.class, FailureClass::Unknown);
        assert_eq!(failure.code.as_deref(), Some("error_max_turns"));
    }

    #[test]
    fn usage_counts_the_last_iteration_only() {
        let usage = json!({"input_tokens":900,"cache_read_input_tokens":70,
            "iterations":[{"input_tokens":2,"cache_read_input_tokens":100,
                "cache_creation_input_tokens":10,"output_tokens":8}]});
        assert_eq!(context_tokens(&usage), Some(120));
        let result = json!({"modelUsage":{
            "claude-haiku-4-5":{"contextWindow":200000},
            "claude-fable-5":{"contextWindow":1000000,"canonicalModel":"claude-fable-5"}}});
        assert_eq!(
            context_window(&result, Some("claude-fable-5")),
            Some(1_000_000)
        );
    }

    #[test]
    fn multi_select_answers_stay_arrays() {
        let input = json!({"questions":[
            {"question":"Pick one","multiSelect":false},
            {"question":"Pick many","multiSelect":true}]});
        let reply = question_answers(
            &input,
            &[
                ("Pick one".into(), vec!["A".into()]),
                ("Pick many".into(), vec!["B".into(), "C".into()]),
            ],
        );
        assert_eq!(reply["updatedInput"]["answers"]["Pick one"], "A");
        assert_eq!(
            reply["updatedInput"]["answers"]["Pick many"],
            json!(["B", "C"])
        );
    }
}
