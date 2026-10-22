//! Amp's stream-json frames and launch arguments (RFC 0002 "Amp", #51).
//!
//! `--stream-json-input` makes Amp read newline-delimited user messages from
//! stdin and keep the process alive until both the assistant is done *and*
//! stdin closes, so one process serves a thread's turns. The output shares
//! Claude Code's stream-json shapes (`system`, `assistant`, `user`,
//! `result`), but a turn ends with `stop_reason: "end_turn"` on the assistant
//! message rather than with a `result`; both facts came from probing the real
//! CLI for the v1 driver.

use serde_json::{Value, json};
use waku_protocol::model::ProviderResumeCursor;
use waku_protocol::orchestration::{FailureClass, ProviderFailure};

use crate::driver::v2::Selection;

/// `ProviderKind::id()` of Amp.
pub const AMP_PROVIDER: &str = "amp";
/// The replay protocol name.
pub const AMP_PROTOCOL: &str = "amp.stream-json";

/// The session process for `thread_id`. The prompt never rides here; it
/// goes in on stdin. Amp's "models" are its agent modes, so the mode is the
/// agent preset, else the model.
pub fn launch_args(selection: &Selection, thread_id: &str) -> Vec<String> {
    let mut args = vec![
        "threads".to_owned(),
        "continue".to_owned(),
        thread_id.to_owned(),
        "--execute".to_owned(),
        // Implies --stream-json, which --stream-json-input requires.
        "--stream-json-thinking".to_owned(),
        "--stream-json-input".to_owned(),
        // Amp exposes no permission request on the stream: Full access only.
        "--dangerously-allow-all".to_owned(),
    ];
    if let Some(mode) = selection
        .agent_preset
        .as_deref()
        .or(selection.model.as_deref())
    {
        args.extend(["--mode".to_owned(), mode.to_owned()]);
    }
    if let Some(effort) = selection.reasoning_effort.as_deref() {
        args.extend(["--effort".to_owned(), effort.to_owned()]);
    }
    if selection.service_tier.as_deref() == Some("fast") {
        args.push("--fast".to_owned());
    }
    args
}

/// A user message line. `steer` marks it for the running turn's next
/// interruption point (the manual's Streaming JSON section); without it Amp
/// holds the message until `end_turn` and runs it as a turn of its own.
pub fn user_message(text: &str, steer: bool) -> Value {
    let mut frame = json!({
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]}
    });
    if steer {
        frame["steer"] = Value::Bool(true);
    }
    frame
}

pub fn cursor(thread_id: &str) -> ProviderResumeCursor {
    ProviderResumeCursor::Amp {
        thread_id: thread_id.to_owned(),
        fork_context: None,
    }
}

/// The thread id `amp threads new` prints: a `T-…` token on its own line
/// (a JSON string when a transcript carries it).
pub fn thread_id_from_line(line: &str) -> Option<String> {
    let line = line.trim();
    let text = serde_json::from_str::<String>(line).unwrap_or_else(|_| line.to_owned());
    let text = text.trim();
    (text.starts_with("T-") && !text.chars().any(char::is_whitespace)).then(|| text.to_owned())
}

/// The failure an `is_error` result reports. A usage limit is read from
/// structured fields only: HTTP 429 (`api_error_status`, Claude's shape) or
/// a limit `error_type`; its reset from `resetsAt` (epoch seconds).
pub fn result_failure(frame: &Value) -> ProviderFailure {
    let message = frame
        .get("error")
        .and_then(Value::as_str)
        .or_else(|| frame.get("result").and_then(Value::as_str))
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .unwrap_or("Amp reported an error.")
        .chars()
        .take(2000)
        .collect::<String>();
    let error_type = frame
        .get("error_type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let usage_limited = frame.get("api_error_status").and_then(Value::as_i64) == Some(429)
        || ["rate_limit", "usage_limit", "insufficient_credit", "quota"]
            .iter()
            .any(|kind| error_type.contains(kind));
    if usage_limited {
        let mut failure =
            ProviderFailure::new(FailureClass::UsageLimit, message).with_code("usage_limit");
        failure.retryable = Some(true);
        failure.reset_at = frame
            .get("resetsAt")
            .or_else(|| frame.get("resets_at"))
            .and_then(Value::as_i64)
            .map(|seconds| seconds * 1000);
        failure
    } else {
        let code = frame
            .get("subtype")
            .and_then(Value::as_str)
            .unwrap_or("error")
            .to_owned();
        ProviderFailure::new(FailureClass::Unknown, message).with_code(code)
    }
}
