//! DeepSeek Harness `dsh web` client API as the driver uses it, over the
//! session's [`HttpTransport`] (RFC 0002 "DeepSeek Harness").
//!
//! Unary operations are typed envelopes: `POST /api/<method>` with
//! `{type: "client-request", rpcId, method, payload}`, answered by
//! `{type: "server-response", rpcId, result: {ok, value | error}}`. Requests
//! the Host makes of the client (approvals, questions) arrive on the
//! `/api/events.mux` WebSocket and are answered with `POST /api/respond`
//! (`{type: "client-response", rpcId, result}`). Routes, payloads and
//! envelopes are the ones the v1 driver used against Harness 0.1.x.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};
use waku_protocol::orchestration::{FailureClass, ProviderFailure};

use crate::driver::v2::{DriverError, HttpMethod, HttpTransport, ProtocolReason};

pub(super) const MUX_ROUTE: &str = "/api/events.mux";
pub(super) const HOST_ROUTE: &str = "/api/events.host";
const HISTORY_PAGE: u64 = 200;
const MAX_HISTORY_PAGES: usize = 1_000;

/// Unique per process: the Host answers each request on its own HTTP
/// exchange, so the id only has to be distinct, not predictable.
fn next_rpc_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!("waku-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

/// A failed call.
#[derive(Debug)]
pub(super) enum RpcError {
    /// The Host answered `ok: false`.
    Refused {
        code: String,
        message: String,
    },
    Transport(std::io::Error),
    Decode(String),
}

impl RpcError {
    pub(super) fn into_driver(self, what: &str) -> DriverError {
        match self {
            Self::Refused { code, message } => {
                let class = match code.as_str() {
                    "invalid" | "invalid_argument" | "bad_request" => FailureClass::ValidationError,
                    "permission_denied" | "forbidden" => FailureClass::PermissionError,
                    _ => FailureClass::Unknown,
                };
                DriverError::Provider(
                    ProviderFailure::new(
                        class,
                        if message.is_empty() {
                            format!("DeepSeek Harness could not {what}.")
                        } else {
                            message
                        },
                    )
                    .with_code(code),
                )
            }
            Self::Transport(error) => DriverError::Io(error),
            Self::Decode(detail) => DriverError::Protocol {
                reason: ProtocolReason::Decode,
                detail: format!("{what}: {detail}"),
            },
        }
    }
}

pub(super) type RpcResult<T> = Result<T, RpcError>;

/// The Host's client API for one session.
#[derive(Clone)]
pub(super) struct Rpc {
    http: Arc<dyn HttpTransport>,
}

impl Rpc {
    pub(super) fn new(http: Arc<dyn HttpTransport>) -> Self {
        Self { http }
    }

    pub(super) fn transport(&self) -> &Arc<dyn HttpTransport> {
        &self.http
    }

    pub(super) async fn call(&self, method: &str, payload: Value) -> RpcResult<Value> {
        let body = json!({
            "type": "client-request",
            "rpcId": next_rpc_id(),
            "method": method,
            "payload": payload,
        });
        let response = self
            .http
            .request(HttpMethod::Post, &format!("/api/{method}"), Some(body))
            .await
            .map_err(RpcError::Transport)?;
        if response.body.get("type").and_then(Value::as_str) != Some("server-response") {
            return Err(RpcError::Decode(format!(
                "{method}: HTTP {} without a server response",
                response.status
            )));
        }
        let result = response
            .body
            .get("result")
            .ok_or_else(|| RpcError::Decode(format!("{method}: no result")))?;
        if result.get("ok").and_then(Value::as_bool) == Some(true) {
            return Ok(result.get("value").cloned().unwrap_or(Value::Null));
        }
        let error = result.get("error").unwrap_or(&Value::Null);
        Err(RpcError::Refused {
            code: error
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        })
    }

    /// Answers a Host request (`approval/requested`, `question/requested`).
    pub(super) async fn respond(&self, rpc_id: &str, result: Value) -> RpcResult<()> {
        let body = json!({ "type": "client-response", "rpcId": rpc_id, "result": result });
        let response = self
            .http
            .request(HttpMethod::Post, "/api/respond", Some(body))
            .await
            .map_err(RpcError::Transport)?;
        if response.body.get("accepted").and_then(Value::as_bool) == Some(true) {
            Ok(())
        } else {
            Err(RpcError::Refused {
                code: "rejected".to_owned(),
                message: response
                    .body
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("DeepSeek Harness rejected the response")
                    .to_owned(),
            })
        }
    }

    pub(super) async fn create_session(
        &self,
        cwd: &str,
        session_id: &str,
        agent_preset: Option<&str>,
    ) -> RpcResult<Value> {
        let mut payload = json!({ "cwd": cwd, "sessionId": session_id });
        if let Some(preset) = agent_preset {
            payload["agentPreset"] = json!(preset);
        }
        self.call("session.create", payload).await
    }

    pub(super) async fn prompt(&self, session: &str, text: &str, mode: &str) -> RpcResult<Value> {
        self.call(
            "session.prompt",
            json!({
                "sessionId": session,
                "mode": mode,
                "content": [{"type": "text", "text": text}],
            }),
        )
        .await
    }

    pub(super) async fn cancel(&self, session: &str) -> RpcResult<()> {
        self.call("session.cancel", json!({ "sessionId": session }))
            .await
            .map(drop)
    }

    pub(super) async fn fork(&self, session: &str, at_seq: u64) -> RpcResult<String> {
        let forked = self
            .call(
                "session.fork",
                json!({ "sessionId": session, "atSeq": at_seq }),
            )
            .await?;
        forked
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| RpcError::Decode("session.fork: no session id".to_owned()))
    }

    pub(super) async fn command_names(&self, session: &str) -> RpcResult<Vec<String>> {
        let commands = self
            .call("commands/list", json!({ "args": { "agentId": session } }))
            .await?;
        Ok(commands
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|command| {
                command
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect())
    }

    /// Runs a Harness command line. `(success, text)`.
    pub(super) async fn execute(
        &self,
        session: &str,
        line: &str,
    ) -> RpcResult<(bool, Option<String>)> {
        let execution = match self
            .call(
                "commands/execute",
                json!({ "args": { "agentId": session, "line": line, "images": [] } }),
            )
            .await
        {
            // Harness 0.1.0's strict descriptor rejects the image list
            // 0.1.1 made required; retry only that compatibility failure.
            Err(RpcError::Refused { message, .. })
                if message.contains("args fields do not match the descriptor")
                    && message.contains("images") =>
            {
                self.call(
                    "commands/execute",
                    json!({ "args": { "agentId": session, "line": line } }),
                )
                .await?
            }
            other => other?,
        };
        if execution.is_null() {
            return Err(RpcError::Refused {
                code: "unknown_command".to_owned(),
                message: format!("unknown or malformed command: {line}"),
            });
        }
        let result = execution.get("result").unwrap_or(&Value::Null);
        let text = result
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(str::to_owned);
        match result.get("kind").and_then(Value::as_str) {
            Some("success") => Ok((true, text)),
            Some("error") => Ok((false, text)),
            other => Err(RpcError::Decode(format!(
                "commands/execute: unknown result {other:?}"
            ))),
        }
    }

    pub(super) async fn select_model(
        &self,
        session: &str,
        model: &str,
        reasoning_effort: Option<&str>,
    ) -> RpcResult<()> {
        let (provider, model) = match model.split_once('/') {
            Some((provider, model)) => (provider.to_owned(), model.to_owned()),
            None => {
                let catalog = self
                    .call("session.models", json!({ "sessionId": session }))
                    .await?;
                let provider = catalog
                    .pointer("/current/provider")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        RpcError::Decode("session.models: no current provider".to_owned())
                    })?
                    .to_owned();
                (provider, model.to_owned())
            }
        };
        let mut selection = json!({ "sessionId": session, "provider": provider, "model": model });
        if let Some(effort) = reasoning_effort {
            selection["reasoningEffort"] = json!(effort);
        }
        self.call("session.selectModel", selection).await.map(drop)
    }

    /// The whole stored history, oldest first, plus the projections of the
    /// newest page.
    pub(super) async fn history(&self, session: &str) -> RpcResult<History> {
        let mut entries = Vec::new();
        let mut projections = None;
        let mut before: Option<u64> = None;
        for _ in 0..MAX_HISTORY_PAGES {
            let mut payload = json!({ "sessionId": session, "maxMessages": HISTORY_PAGE });
            if let Some(before) = before {
                payload["beforeSeq"] = json!(before);
            }
            let page = self.call("session.history", payload).await?;
            if projections.is_none() {
                projections = page.pointer("/projections/values").cloned();
            }
            let events = page
                .get("events")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let oldest = events.iter().filter_map(event_seq).min();
            entries.extend(events);
            if !page
                .get("hasMore")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                break;
            }
            match oldest.filter(|oldest| *oldest > 0) {
                Some(oldest) => before = Some(oldest),
                None => break,
            }
        }
        entries.sort_by_key(|entry| event_seq(entry).unwrap_or(u64::MAX));
        entries.dedup_by_key(|entry| event_seq(entry).unwrap_or(u64::MAX));
        Ok(History {
            entries,
            projections,
        })
    }
}

/// `session.history`, oldest first.
pub(super) struct History {
    /// `{event, view?}` entries.
    pub entries: Vec<Value>,
    pub projections: Option<Value>,
}

impl History {
    /// `(turn/start seq, turn/end seq)` of every completed turn, and the
    /// newest seq.
    pub(super) fn turns(&self) -> (Vec<(u64, u64)>, i64) {
        let mut turns = Vec::new();
        let mut start = None;
        let mut last = -1_i64;
        for entry in &self.entries {
            let event = entry.get("event").unwrap_or(entry);
            let Some(seq) = event.get("seq").and_then(Value::as_u64) else {
                continue;
            };
            last = last.max(i64::try_from(seq).unwrap_or(i64::MAX));
            match event.get("type").and_then(Value::as_str) {
                Some("turn/start") => start = Some(seq),
                Some("turn/end") => turns.push((start.take().unwrap_or(seq), seq)),
                _ => {}
            }
        }
        (turns, last)
    }

    pub(super) fn permission(&self) -> Option<&str> {
        self.projections
            .as_ref()?
            .pointer("/permissions/currentValue")
            .and_then(Value::as_str)
    }

    /// Older sessions can keep Harness's command-backed read-only state.
    pub(super) fn legacy_read_only(&self) -> bool {
        let flag = |pointer: &str| {
            self.projections
                .as_ref()
                .and_then(|values| values.pointer(pointer))
                .and_then(Value::as_bool)
                .unwrap_or(false)
        };
        flag("/plan/active") || flag("/plan/pending")
    }
}

fn event_seq(entry: &Value) -> Option<u64> {
    entry
        .pointer("/event/seq")
        .or_else(|| entry.get("seq"))
        .and_then(Value::as_u64)
}
