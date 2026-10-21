//! The Codex app-server wire: one JSON-RPC connection per session, request
//! and notification builders, and the classification of native errors.
//!
//! JSON-RPC ids are a per-channel counter starting at 1 (RFC 0002
//! "Matching"), so a replayed session writes the same ids it recorded.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;
use waku_protocol::orchestration::{FailureClass, ProviderFailure, RuntimeMode};

use crate::driver::v2::{
    ContextBlock, ContextKind, DriverError, DriverResult, FrameDirection, ProtocolReason,
    RawFrameLog, Selection, SteerRejectReason, wrap_synthetic_context,
};

/// A JSON-RPC error object the app-server answered with.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct RpcError {
    pub code: Option<i64>,
    pub message: String,
}

impl RpcError {
    fn from_value(error: &Value) -> Self {
        Self {
            code: error.get("code").and_then(Value::as_i64),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("The Codex app-server returned an error.")
                .to_owned(),
        }
    }

    pub fn is_method_not_found(&self) -> bool {
        let lowered = self.message.to_ascii_lowercase();
        self.code == Some(-32601)
            || lowered.contains("method not found")
            || lowered.contains("unsupported")
    }
}

type Reply = oneshot::Sender<Result<Value, RpcError>>;

/// One app-server process: the single writer, the request-id counter and
/// the requests in flight.
pub(super) struct Connection {
    channel: String,
    writer: tokio::sync::Mutex<Option<Box<dyn AsyncWrite + Send + Unpin>>>,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, (String, Reply)>>,
    raw_log: Arc<dyn RawFrameLog>,
}

impl Connection {
    pub fn new(
        channel: impl Into<String>,
        writer: Box<dyn AsyncWrite + Send + Unpin>,
        raw_log: Arc<dyn RawFrameLog>,
    ) -> Self {
        Self {
            channel: channel.into(),
            writer: tokio::sync::Mutex::new(Some(writer)),
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            raw_log,
        }
    }

    pub fn raw_log(&self, direction: FrameDirection, frame: &str) {
        self.raw_log.record(&self.channel, direction, frame);
    }

    pub async fn write(&self, frame: &Value) -> DriverResult<()> {
        let mut line = serde_json::to_vec(frame).map_err(|error| DriverError::Protocol {
            reason: ProtocolReason::Decode,
            detail: error.to_string(),
        })?;
        line.push(b'\n');
        let mut writer = self.writer.lock().await;
        let Some(writer) = writer.as_mut() else {
            return Err(DriverError::SessionLost {
                detail: "the Codex app-server's stdin is closed".to_owned(),
            });
        };
        writer.write_all(&line).await?;
        writer.flush().await?;
        Ok(())
    }

    /// Sends a request and waits for its response.
    pub async fn request(&self, method: &str, params: Value) -> DriverResult<Value> {
        match self.request_raw(method, params).await? {
            Ok(result) => Ok(result),
            Err(error) => Err(rpc_failure(method, &error)),
        }
    }

    /// Sends a request; the outer error is transport loss, the inner one the
    /// app-server's JSON-RPC error.
    pub async fn request_raw(
        &self,
        method: &str,
        params: Value,
    ) -> DriverResult<Result<Value, RpcError>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().insert(id, (method.to_owned(), sender));
        if let Err(error) = self
            .write(&json!({ "id": id, "method": method, "params": params }))
            .await
        {
            self.pending.lock().remove(&id);
            return Err(error);
        }
        receiver.await.map_err(|_| DriverError::SessionLost {
            detail: format!("{method}: the Codex app-server exited"),
        })
    }

    pub async fn notify(&self, method: &str, params: Value) -> DriverResult<()> {
        self.write(&json!({ "method": method, "params": params }))
            .await
    }

    /// Answers a server request.
    pub async fn respond(&self, id: &Value, result: Value) -> DriverResult<()> {
        self.write(&json!({ "id": id, "result": result })).await
    }

    pub async fn respond_error(&self, id: &Value, code: i64, message: &str) -> DriverResult<()> {
        self.write(&json!({ "id": id, "error": { "code": code, "message": message } }))
            .await
    }

    /// The method of the request a response answers, without completing it.
    pub fn pending_method(&self, frame: &Value) -> Option<String> {
        let id = frame.get("id").and_then(Value::as_u64)?;
        self.pending
            .lock()
            .get(&id)
            .map(|(method, _)| method.clone())
    }

    /// Routes a response to its request. Returns the request's method.
    pub fn complete(&self, frame: &Value) -> Option<String> {
        let id = frame.get("id").and_then(Value::as_u64)?;
        let (method, reply) = self.pending.lock().remove(&id)?;
        let result = match frame.get("error").filter(|error| !error.is_null()) {
            Some(error) => Err(RpcError::from_value(error)),
            None => Ok(frame.get("result").cloned().unwrap_or(Value::Null)),
        };
        let _ = reply.send(result);
        Some(method)
    }

    /// The process is gone: every request in flight fails.
    pub fn fail_all(&self) {
        self.pending.lock().clear();
    }

    /// Closes stdin, which is how the app-server is asked to exit.
    pub async fn close(&self) {
        if let Some(mut writer) = self.writer.lock().await.take() {
            let _ = writer.shutdown().await;
        }
    }
}

/// A JSON-RPC error as a driver error. Only fixed reasons reach the user;
/// the native message goes to `detail`.
pub(super) fn rpc_failure(method: &str, error: &RpcError) -> DriverError {
    match method {
        "turn/steer" => DriverError::SteerRejected {
            reason: SteerRejectReason::TurnSettled,
        },
        "turn/start" if error.message.to_ascii_lowercase().contains("active turn") => {
            DriverError::Protocol {
                reason: ProtocolReason::PreviousTurnActive,
                detail: format!("{method}: {}", error.message),
            }
        }
        _ if error.is_method_not_found() => DriverError::Unsupported {
            operation: static_method(method),
        },
        _ => DriverError::Protocol {
            reason: ProtocolReason::UnexpectedFrame,
            detail: format!("{method}: {}", error.message),
        },
    }
}

fn static_method(method: &str) -> &'static str {
    match method {
        "thread/goal/get" | "thread/goal/set" | "thread/goal/clear" => "goal",
        "thread/backgroundTerminals/list" | "thread/backgroundTerminals/terminate" => {
            "stop_background_work"
        }
        "thread/revert" | "thread/turns/list" => "rollback",
        "thread/fork" => "fork",
        "thread/read" => "read_snapshot",
        "turn/steer" => "steer_turn",
        _ => "codex request",
    }
}

// -- permissions --------------------------------------------------------------------

/// `(approvalPolicy, sandbox, approvalsReviewer)` for a runtime mode.
pub(super) fn permissions(mode: RuntimeMode) -> (&'static str, &'static str, &'static str) {
    match mode {
        RuntimeMode::Ask => ("untrusted", "read-only", "user"),
        RuntimeMode::AutoAcceptEdits => ("on-request", "workspace-write", "user"),
        RuntimeMode::Auto => ("on-request", "workspace-write", "auto_review"),
        _ => ("never", "danger-full-access", "user"),
    }
}

pub(super) fn sandbox_policy(sandbox: &str) -> Value {
    match sandbox {
        "read-only" => json!({"type": "readOnly"}),
        "danger-full-access" => json!({"type": "dangerFullAccess"}),
        _ => json!({"type": "workspaceWrite"}),
    }
}

// -- request params -----------------------------------------------------------------

pub(super) fn initialize_params() -> Value {
    json!({
        "clientInfo": {
            "name": "waku",
            "title": "Waku",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "capabilities": { "experimentalApi": true },
    })
}

/// `thread/start` and `thread/resume` share the runtime fields.
pub(super) fn thread_params(cwd: &str, selection: &Selection) -> Value {
    let (approval_policy, sandbox, reviewer) = permissions(selection.runtime_mode);
    let mut params = json!({
        "cwd": cwd,
        "approvalPolicy": approval_policy,
        "sandbox": sandbox,
        "approvalsReviewer": reviewer,
    });
    if let Some(model) = &selection.model {
        params["model"] = json!(model);
    }
    if let Some(tier) = &selection.service_tier {
        params["serviceTier"] = json!(tier);
    }
    params
}

pub(super) fn thread_start_params(cwd: &str, selection: &Selection) -> Value {
    let mut params = thread_params(cwd, selection);
    params["serviceName"] = json!("waku");
    params
}

pub(super) fn thread_resume_params(thread_id: &str, cwd: &str, selection: &Selection) -> Value {
    let mut params = thread_params(cwd, selection);
    params["threadId"] = json!(thread_id);
    params
}

/// `turn/start`: every turn carries its model, effort, tier and permission
/// profile. Developer context travels in `collaborationMode` (as T3's
/// `CodexAdapterV2` sends it) when a model is known, synthetic-user context
/// in the shared envelope.
pub(super) fn turn_start_params(
    thread_id: &str,
    text: &str,
    selection: &Selection,
    context: &[ContextBlock],
) -> Value {
    let (approval_policy, sandbox, reviewer) = permissions(selection.runtime_mode);
    let developer = context
        .iter()
        .filter(|block| block.kind != ContextKind::SyntheticUser)
        .cloned()
        .collect::<Vec<_>>();
    let mut synthetic = context
        .iter()
        .filter(|block| block.kind == ContextKind::SyntheticUser)
        .cloned()
        .collect::<Vec<_>>();
    let developer_channel = selection.model.is_some() && !developer.is_empty();
    if !developer_channel {
        // No model to name in `collaborationMode`: the blocks go in the
        // envelope with the rest.
        synthetic.splice(0..0, developer.iter().cloned());
    }
    let mut params = json!({
        "threadId": thread_id,
        "input": [{"type": "text", "text": wrap_synthetic_context(&synthetic, text)}],
        "approvalPolicy": approval_policy,
        "approvalsReviewer": reviewer,
        "sandboxPolicy": sandbox_policy(sandbox),
        // Some models default reasoning summaries to `none`; Waku shows a
        // reasoning disclosure, so ask for readable text.
        "summary": "auto",
    });
    if let Some(model) = &selection.model {
        params["model"] = json!(model);
    }
    if let Some(effort) = &selection.reasoning_effort {
        params["effort"] = json!(effort);
    }
    if let Some(tier) = &selection.service_tier {
        params["serviceTier"] = json!(tier);
    }
    if developer_channel {
        let instructions = developer
            .iter()
            .map(|block| block.text.as_str())
            .collect::<Vec<_>>()
            .join("\n\n");
        params["collaborationMode"] = json!({
            "mode": "default",
            "settings": {
                "model": selection.model,
                "reasoning_effort": selection.reasoning_effort,
                "developer_instructions": instructions,
            },
        });
    }
    params
}

pub(super) fn text_input(text: &str) -> Value {
    json!([{"type": "text", "text": text}])
}

// -- approvals ----------------------------------------------------------------------

/// One native approval decision: its option id, its label and the value the
/// reply carries (`"accept"`, or an object such as
/// `{"acceptWithExecpolicyAmendment": {...}}`).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Decision {
    pub id: String,
    pub label: String,
    pub allow: bool,
    pub value: Value,
}

/// The decisions a `*/requestApproval` request offers: its own
/// `availableDecisions` when present, else the app-server's defaults.
pub(super) fn approval_decisions(params: &Value) -> Vec<Decision> {
    let listed = params
        .get("availableDecisions")
        .and_then(Value::as_array)
        .map(|decisions| {
            decisions
                .iter()
                .filter_map(|decision| match decision {
                    Value::String(id) => Some((id.clone(), decision.clone())),
                    Value::Object(map) => {
                        map.keys().next().map(|id| (id.clone(), decision.clone()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .filter(|decisions| !decisions.is_empty());
    let decisions = listed.unwrap_or_else(|| {
        ["accept", "acceptForSession", "decline"]
            .into_iter()
            .map(|id| (id.to_owned(), Value::String(id.to_owned())))
            .collect()
    });
    decisions
        .into_iter()
        .map(|(id, value)| Decision {
            label: decision_label(&id),
            allow: id.starts_with("accept"),
            id,
            value,
        })
        .collect()
}

/// `item/permissions/requestApproval` answers with a grant, not a decision.
pub(super) fn permission_decisions() -> Vec<Decision> {
    ["accept", "acceptForSession", "decline"]
        .into_iter()
        .map(|id| Decision {
            id: id.to_owned(),
            label: decision_label(id),
            allow: id != "decline",
            value: Value::String(id.to_owned()),
        })
        .collect()
}

fn decision_label(id: &str) -> String {
    match id {
        "accept" => tr!("permission.allow_once"),
        "acceptForSession" => tr!("permission.allow_for_session"),
        "decline" => tr!("common.deny"),
        "cancel" => tr!("common.cancel"),
        other if other.starts_with("accept") || other.starts_with("apply") => {
            tr!("permission.always_allow")
        }
        other => split_camel_case(other),
    }
}

/// The reply to an approval request for the chosen decision.
pub(super) fn approval_reply(method: &str, params: &Value, decision: &Decision) -> Value {
    if method == "item/permissions/requestApproval" {
        return match decision.id.as_str() {
            "decline" | "cancel" => json!({ "permissions": {} }),
            id => json!({
                "permissions": params.get("permissions").cloned().unwrap_or_else(|| json!({})),
                "scope": if id == "acceptForSession" { "session" } else { "turn" },
            }),
        };
    }
    json!({ "decision": decision.value })
}

pub(super) fn split_camel_case(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 4);
    for (index, character) in value.chars().enumerate() {
        if character == '_' || character == '-' {
            if !output.ends_with(' ') {
                output.push(' ');
            }
            continue;
        }
        if index > 0 && character.is_ascii_uppercase() {
            output.push(' ');
        }
        output.push(character);
    }
    let mut characters = output.chars();
    characters
        .next()
        .map(|first| first.to_uppercase().collect::<String>() + characters.as_str())
        .unwrap_or_else(|| "Activity".into())
}

// -- failures -----------------------------------------------------------------------

/// The `codexErrorInfo` code: a string, or the single key of an object
/// (`{"httpConnectionFailed": {...}}`).
pub(super) fn error_code(info: Option<&Value>) -> Option<String> {
    match info? {
        Value::String(code) => Some(code.clone()),
        Value::Object(map) => map.keys().next().cloned(),
        _ => None,
    }
}

/// Classifies a turn error from its structured `codexErrorInfo`, never from
/// its text. `reset_at` comes from the rate-limit snapshot.
pub(super) fn classify(error: &Value, reset_at: Option<i64>) -> ProviderFailure {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty())
        .unwrap_or("Codex could not complete the turn.");
    let code = error_code(error.get("codexErrorInfo"));
    let class = match code.as_deref() {
        Some("usageLimitExceeded" | "rateLimitExceeded" | "sessionBudgetExceeded") => {
            FailureClass::UsageLimit
        }
        Some("unauthorized") => FailureClass::PermissionError,
        Some("badRequest") => FailureClass::ValidationError,
        Some(
            "httpConnectionFailed"
            | "responseStreamConnectionFailed"
            | "responseStreamDisconnected"
            | "responseTooManyFailedAttempts",
        ) => FailureClass::TransportError,
        Some(
            "contextWindowExceeded"
            | "flexUnavailable"
            | "serverOverloaded"
            | "cyberPolicy"
            | "misalignmentPolicyViolation"
            | "tooManyDenials"
            | "internalServerError"
            | "threadRollbackFailed"
            | "sandboxError"
            | "activeTurnNotSteerable",
        ) => FailureClass::ProviderError,
        _ => FailureClass::Unknown,
    };
    let mut failure = ProviderFailure::new(class, message);
    if let Some(code) = code {
        failure = failure.with_code(code);
    }
    match class {
        FailureClass::UsageLimit => {
            failure.retryable = Some(true);
            failure.reset_at = reset_at;
        }
        FailureClass::TransportError | FailureClass::ProviderError => {
            failure.retryable = Some(class == FailureClass::TransportError);
        }
        _ => {}
    }
    failure
}

// -- rate limits --------------------------------------------------------------------

/// Codex sends `account/rateLimits/updated` as a partial view: a field an
/// update omits keeps its earlier value. Snapshots of another allowance
/// (`limitId` other than `codex`) never replace the main one.
pub(super) fn merge_rate_limits(previous: Option<&Value>, update: &Value) -> Option<Value> {
    if update
        .get("limitId")
        .and_then(Value::as_str)
        .is_some_and(|id| id != "codex")
    {
        return previous.cloned();
    }
    let Some(Value::Object(previous)) = previous else {
        return Some(update.clone());
    };
    let mut merged = previous.clone();
    if let Value::Object(update) = update {
        for (key, value) in update {
            merged.insert(key.clone(), value.clone());
        }
    }
    Some(Value::Object(merged))
}

/// When every exhausted window has reopened, in epoch milliseconds. `None`
/// when no window is exhausted or one has no reset time.
pub(super) fn usage_limit_reset_at(snapshot: Option<&Value>) -> Option<i64> {
    let snapshot = snapshot?;
    let mut latest = None;
    for key in ["primary", "secondary"] {
        let Some(window) = snapshot.get(key).filter(|window| !window.is_null()) else {
            continue;
        };
        let used = window.get("usedPercent").and_then(Value::as_f64)?;
        if used < 100.0 {
            continue;
        }
        let reset = window.get("resetsAt").and_then(Value::as_i64)? * 1000;
        latest = Some(latest.map_or(reset, |current: i64| current.max(reset)));
    }
    latest
}

/// The account panel's plan rows for a rate-limit snapshot.
pub(super) fn plan_usage(snapshot: &Value) -> Option<crate::usage::PlanUsage> {
    let mut windows = Vec::new();
    for key in ["primary", "secondary"] {
        let Some(window) = snapshot.get(key).filter(|window| !window.is_null()) else {
            continue;
        };
        let Some(percent) = window.get("usedPercent").and_then(Value::as_f64) else {
            continue;
        };
        windows.push(crate::usage::PlanWindow {
            label: crate::usage::window_label_from_minutes(
                window.get("windowDurationMins").and_then(Value::as_i64),
            ),
            percent: percent.clamp(0.0, 100.0),
            resets_at: window.get("resetsAt").and_then(Value::as_i64),
        });
    }
    if windows.is_empty() {
        return None;
    }
    Some(crate::usage::PlanUsage {
        plan_label: crate::usage::openai_plan_label(
            snapshot.get("planType").and_then(Value::as_str),
        ),
        windows,
    })
}

// -- goals --------------------------------------------------------------------------

/// `thread/goal/set` params. Omitted fields keep their provider-side value.
pub(super) fn goal_set_params(
    thread_id: &str,
    goal: &waku_protocol::orchestration::ThreadGoal,
) -> Value {
    let mut params = json!({ "threadId": thread_id });
    if !goal.objective.is_empty() {
        params["objective"] = json!(goal.objective);
    }
    if let Ok(status) = serde_json::to_value(goal.status) {
        params["status"] = codex_goal_status(&status);
    }
    if let Some(budget) = goal.token_budget {
        params["tokenBudget"] = json!(budget);
    }
    params
}

/// Codex spells goal statuses in camelCase (`usageLimited`); the
/// orchestration types use snake_case.
fn codex_goal_status(status: &Value) -> Value {
    match status.as_str() {
        Some(status) => {
            let mut out = String::new();
            let mut upper = false;
            for character in status.chars() {
                if character == '_' {
                    upper = true;
                } else if upper {
                    out.extend(character.to_uppercase());
                    upper = false;
                } else {
                    out.push(character);
                }
            }
            Value::String(out)
        }
        None => status.clone(),
    }
}

/// Parses the app-server's `goal` object.
pub(super) fn parse_goal(goal: &Value) -> Option<waku_protocol::orchestration::ThreadGoal> {
    let objective = goal.get("objective").and_then(Value::as_str)?.to_owned();
    let status = goal
        .get("status")
        .and_then(Value::as_str)
        .map(|status| {
            let mut snake = String::new();
            for character in status.chars() {
                if character.is_ascii_uppercase() {
                    snake.push('_');
                    snake.push(character.to_ascii_lowercase());
                } else {
                    snake.push(character);
                }
            }
            serde_json::from_value(Value::String(snake))
                .unwrap_or(waku_protocol::orchestration::ThreadGoalStatus::Active)
        })
        .unwrap_or(waku_protocol::orchestration::ThreadGoalStatus::Active);
    Some(waku_protocol::orchestration::ThreadGoal {
        objective,
        status,
        token_budget: goal.get("tokenBudget").and_then(Value::as_i64),
        tokens_used: goal.get("tokensUsed").and_then(Value::as_i64).unwrap_or(0),
        time_used_seconds: goal
            .get("timeUsedSeconds")
            .and_then(Value::as_i64)
            .unwrap_or(0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_modes_match_codex_permission_profiles() {
        assert_eq!(
            permissions(RuntimeMode::Ask),
            ("untrusted", "read-only", "user")
        );
        assert_eq!(
            permissions(RuntimeMode::AutoAcceptEdits),
            ("on-request", "workspace-write", "user")
        );
        assert_eq!(
            permissions(RuntimeMode::Auto),
            ("on-request", "workspace-write", "auto_review")
        );
        assert_eq!(
            permissions(RuntimeMode::FullAccess),
            ("never", "danger-full-access", "user")
        );
    }

    #[test]
    fn turns_request_readable_reasoning_summaries_and_carry_the_selection() {
        let selection = Selection {
            model: Some("gpt-6-luna".into()),
            reasoning_effort: Some("low".into()),
            ..Selection::default()
        };
        let params = turn_start_params("thread-1", "Inspect the failure", &selection, &[]);
        assert_eq!(params["summary"], "auto");
        assert_eq!(params["threadId"], "thread-1");
        assert_eq!(params["input"][0]["text"], "Inspect the failure");
        assert_eq!(params["model"], "gpt-6-luna");
        assert_eq!(params["effort"], "low");
        assert!(params.get("collaborationMode").is_none());
    }

    #[test]
    fn developer_context_uses_the_collaboration_mode_and_synthetic_context_the_envelope() {
        let selection = Selection {
            model: Some("gpt-6-luna".into()),
            ..Selection::default()
        };
        let context = [
            ContextBlock {
                key: "rules".into(),
                kind: ContextKind::Developer,
                text: "Prefer tabs.".into(),
            },
            ContextBlock {
                key: "fork".into(),
                kind: ContextKind::SyntheticUser,
                text: "Earlier: hello".into(),
            },
        ];
        let params = turn_start_params("t", "go", &selection, &context);
        assert_eq!(
            params["collaborationMode"]["settings"]["developer_instructions"],
            "Prefer tabs."
        );
        let text = params["input"][0]["text"].as_str().unwrap();
        assert!(text.starts_with("<WAKU_CONTEXT_V1>"));
        assert!(text.contains("Earlier: hello") && !text.contains("Prefer tabs."));
        // Without a model the developer block joins the envelope.
        let params = turn_start_params("t", "go", &Selection::default(), &context);
        assert!(params.get("collaborationMode").is_none());
        assert!(
            params["input"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Prefer tabs.")
        );
    }

    #[test]
    fn approval_decisions_pass_native_ids_through() {
        let decisions = approval_decisions(&json!({
            "availableDecisions": ["accept", {"acceptWithExecpolicyAmendment": {"execpolicy_amendment": ["ls"]}}, "cancel"]
        }));
        assert_eq!(
            decisions
                .iter()
                .map(|decision| decision.id.as_str())
                .collect::<Vec<_>>(),
            ["accept", "acceptWithExecpolicyAmendment", "cancel"]
        );
        assert!(decisions[1].allow && !decisions[2].allow);
        assert_eq!(
            approval_reply(
                "item/commandExecution/requestApproval",
                &json!({}),
                &decisions[1]
            ),
            json!({"decision": {"acceptWithExecpolicyAmendment": {"execpolicy_amendment": ["ls"]}}})
        );
        let defaults = approval_decisions(&json!({}));
        assert_eq!(
            defaults
                .iter()
                .map(|decision| decision.id.as_str())
                .collect::<Vec<_>>(),
            ["accept", "acceptForSession", "decline"]
        );
        let grant = approval_reply(
            "item/permissions/requestApproval",
            &json!({"permissions": {"network": true}}),
            &permission_decisions()[1],
        );
        assert_eq!(
            grant,
            json!({"permissions": {"network": true}, "scope": "session"})
        );
    }

    #[test]
    fn failures_are_classified_from_the_structured_code() {
        let failure = classify(
            &json!({"message": "You've hit your usage limit.", "codexErrorInfo": "usageLimitExceeded"}),
            Some(2_000_100_000_000),
        );
        assert_eq!(failure.class, FailureClass::UsageLimit);
        assert_eq!(failure.reset_at, Some(2_000_100_000_000));
        assert_eq!(failure.code.as_deref(), Some("usageLimitExceeded"));
        let failure = classify(
            &json!({"message": "x", "codexErrorInfo": {"responseStreamDisconnected": {"httpStatusCode": 502}}}),
            None,
        );
        assert_eq!(failure.class, FailureClass::TransportError);
        // A usage limit spelled out in the text alone is not classified.
        let failure = classify(&json!({"message": "usage limit reached"}), None);
        assert_eq!(failure.class, FailureClass::Unknown);
    }

    #[test]
    fn rate_limit_snapshots_merge_and_give_the_reset_time() {
        let first = json!({"limitId": "codex", "planType": "plus", "primary": {"usedPercent": 40.0, "resetsAt": 100, "windowDurationMins": 300}, "secondary": {"usedPercent": 10.0, "resetsAt": 900, "windowDurationMins": 10080}});
        let merged = merge_rate_limits(None, &first);
        let merged = merge_rate_limits(
            merged.as_ref(),
            &json!({"primary": {"usedPercent": 100.0, "resetsAt": 2_000_100_000, "windowDurationMins": 300}}),
        );
        assert_eq!(merged.as_ref().unwrap()["planType"], "plus");
        assert_eq!(
            usage_limit_reset_at(merged.as_ref()),
            Some(2_000_100_000_000)
        );
        // Another allowance never replaces the main one.
        let kept = merge_rate_limits(
            merged.as_ref(),
            &json!({"limitId": "spark", "primary": {"usedPercent": 1.0}}),
        );
        assert_eq!(kept, merged);
        let usage = plan_usage(merged.as_ref().unwrap()).unwrap();
        assert_eq!(usage.windows.len(), 2);
        assert_eq!(usage.windows[0].percent, 100.0);
    }

    #[test]
    fn goals_cross_the_wire_in_codex_casing() {
        let goal = parse_goal(&json!({
            "threadId": "t", "objective": "Ship it", "status": "usageLimited",
            "tokenBudget": 50000, "tokensUsed": 12500, "timeUsedSeconds": 90
        }))
        .unwrap();
        assert_eq!(
            goal.status,
            waku_protocol::orchestration::ThreadGoalStatus::UsageLimited
        );
        assert_eq!(goal.tokens_used, 12500);
        let params = goal_set_params("t", &goal);
        assert_eq!(params["status"], "usageLimited");
        assert_eq!(params["objective"], "Ship it");
    }
}
