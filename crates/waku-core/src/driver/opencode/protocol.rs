//! OpenCode 2's HTTP routes as the driver uses them, over the session's
//! [`HttpTransport`] (RFC 0002 "OpenCode").
//!
//! Every route here was exercised by the v1 driver and `opencode_api`
//! against OpenCode 2.0.x: paths, bodies and envelopes are unchanged. Two
//! envelopes exist: most routes answer `{ data }`, while `POST …/interrupt`
//! answers a bare `{ interrupted }`. A non-2xx answer carries the service's
//! own `{ _tag, message }` body.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};
use waku_protocol::orchestration::{FailureClass, ProviderFailure};

use crate::driver::v2::{DriverError, HttpMethod, HttpResponse, HttpTransport, ProtocolReason};
use crate::http_wire::encode_path_segment;
use crate::opencode_api::{FormAnswer, ModelRef, PermissionReply};

/// The event route every session subscribes to.
pub(super) const EVENT_ROUTE: &str = "/api/event";
/// Messages per transcript page.
pub(super) const MESSAGE_PAGE: usize = 200;
/// A transcript longer than this many pages is refused rather than walked.
const MAX_MESSAGE_PAGES: usize = 1_000;

/// How a prompt joins a session that is already running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Delivery {
    Steer,
}

impl Delivery {
    fn as_str(self) -> &'static str {
        match self {
            Self::Steer => "steer",
        }
    }
}

/// One failed call, split by what the driver can do about it.
#[derive(Debug)]
pub(super) enum ApiError {
    /// The service answered with an error status.
    Status {
        status: u16,
        tag: Option<String>,
        message: String,
    },
    /// No HTTP answer at all.
    Transport(std::io::Error),
    /// An answer that does not decode.
    Decode(String),
}

impl ApiError {
    pub(super) fn is_not_found(&self) -> bool {
        matches!(self, Self::Status { status: 404, .. })
    }

    /// The driver error a caller returns. `what` names the operation for the
    /// raw log; only fixed reasons and the service's own message reach users.
    pub(super) fn into_driver(self, what: &str) -> DriverError {
        match self {
            Self::Status {
                status,
                tag,
                message,
            } => {
                let class = match status {
                    400 | 422 => FailureClass::ValidationError,
                    401 | 403 => FailureClass::PermissionError,
                    429 => FailureClass::UsageLimit,
                    _ => FailureClass::Unknown,
                };
                let mut failure = ProviderFailure::new(
                    class,
                    if message.is_empty() {
                        format!("OpenCode could not {what} (HTTP {status}).")
                    } else {
                        message
                    },
                );
                failure = failure.with_code(tag.unwrap_or_else(|| format!("http_{status}")));
                DriverError::Provider(failure)
            }
            Self::Transport(error) => DriverError::Io(error),
            Self::Decode(detail) => DriverError::Protocol {
                reason: ProtocolReason::Decode,
                detail: format!("{what}: {detail}"),
            },
        }
    }
}

pub(super) type ApiResult<T> = Result<T, ApiError>;

/// `location[directory]=…`, the deepObject every location-aware route takes.
pub(super) fn location_query(directory: &str) -> String {
    format!(
        "?{}={}",
        encode_path_segment("location[directory]"),
        encode_path_segment(directory)
    )
}

fn session_path(session: &str, rest: &str) -> String {
    format!("/api/session/{}{rest}", encode_path_segment(session))
}

/// The routes one session uses.
#[derive(Clone)]
pub(super) struct Api {
    http: Arc<dyn HttpTransport>,
}

impl Api {
    pub(super) fn new(http: Arc<dyn HttpTransport>) -> Self {
        Self { http }
    }

    pub(super) fn transport(&self) -> &Arc<dyn HttpTransport> {
        &self.http
    }

    async fn call(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<Value>,
    ) -> ApiResult<HttpResponse> {
        let response = self
            .http
            .request(method, path, body)
            .await
            .map_err(ApiError::Transport)?;
        if (200..300).contains(&response.status) {
            return Ok(response);
        }
        let tag = response
            .body
            .get("_tag")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let message = response
            .body
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| response.body.as_str().map(str::to_owned))
            .unwrap_or_default();
        Err(ApiError::Status {
            status: response.status,
            tag,
            message,
        })
    }

    /// The `{ data }` member of a 2xx answer.
    async fn data(&self, method: HttpMethod, path: &str, body: Option<Value>) -> ApiResult<Value> {
        let response = self.call(method, path, body).await?;
        match response.body {
            Value::Object(mut fields) => fields
                .remove("data")
                .ok_or_else(|| ApiError::Decode(format!("{path}: no data"))),
            other => Err(ApiError::Decode(format!("{path}: unreadable {other}"))),
        }
    }

    /// `"{providerID}/{id}"` → `limit.context`, from the model catalogue.
    pub(super) async fn context_windows(&self) -> ApiResult<HashMap<String, u64>> {
        let models = self.data(HttpMethod::Get, "/api/model", None).await?;
        Ok(models
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|model| {
                let provider = model.get("providerID").and_then(Value::as_str)?;
                let id = model.get("id").and_then(Value::as_str)?;
                let context = model.pointer("/limit/context").and_then(Value::as_u64)?;
                Some((format!("{provider}/{id}"), context))
            })
            .collect())
    }

    /// Primary agents a session can run (not hidden, not subagent-only).
    pub(super) async fn selectable_agents(&self, directory: &str) -> ApiResult<Vec<String>> {
        let agents = self
            .data(
                HttpMethod::Get,
                &format!("/api/agent{}", location_query(directory)),
                None,
            )
            .await?;
        Ok(agents
            .as_array()
            .into_iter()
            .flatten()
            .filter(|agent| {
                !agent
                    .get("hidden")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                    && agent.get("mode").and_then(Value::as_str) != Some("subagent")
            })
            .filter_map(|agent| agent.get("id").and_then(Value::as_str).map(str::to_owned))
            .collect())
    }

    pub(super) async fn command_names(&self, directory: &str) -> ApiResult<Vec<String>> {
        let commands = self
            .data(
                HttpMethod::Get,
                &format!("/api/command{}", location_query(directory)),
                None,
            )
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

    /// Creates a session and answers it. The service mints the id: the
    /// session's actor reads the event stream only after this answer, so an
    /// event the service publishes before answering waits in the stream
    /// rather than racing the create.
    pub(super) async fn create_session(
        &self,
        directory: &str,
        agent: Option<&str>,
        model: Option<&ModelRef>,
    ) -> ApiResult<Value> {
        let mut body = json!({ "location": { "directory": directory } });
        if let Some(agent) = agent {
            body["agent"] = json!(agent);
        }
        if let Some(model) = model {
            body["model"] = json!(model);
        }
        self.data(HttpMethod::Post, "/api/session", Some(body))
            .await
    }

    pub(super) async fn get_session(&self, session: &str) -> ApiResult<Value> {
        self.data(HttpMethod::Get, &session_path(session, ""), None)
            .await
    }

    pub(super) async fn pending_permissions(&self, session: &str) -> ApiResult<Vec<Value>> {
        let list = self
            .data(HttpMethod::Get, &session_path(session, "/permission"), None)
            .await?;
        Ok(list.as_array().cloned().unwrap_or_default())
    }

    pub(super) async fn pending_forms(&self, session: &str) -> ApiResult<Vec<Value>> {
        let list = self
            .data(HttpMethod::Get, &session_path(session, "/form"), None)
            .await?;
        Ok(list.as_array().cloned().unwrap_or_default())
    }

    pub(super) async fn switch_agent(&self, session: &str, agent: &str) -> ApiResult<()> {
        self.call(
            HttpMethod::Post,
            &session_path(session, "/agent"),
            Some(json!({ "agent": agent })),
        )
        .await
        .map(drop)
    }

    pub(super) async fn switch_model(&self, session: &str, model: &ModelRef) -> ApiResult<()> {
        self.call(
            HttpMethod::Post,
            &session_path(session, "/model"),
            Some(json!({ "model": model })),
        )
        .await
        .map(drop)
    }

    /// Submits a prompt and answers its inbox id, which is also the id of the
    /// user message it becomes (the turn's native id and fork boundary).
    pub(super) async fn prompt(
        &self,
        session: &str,
        text: &str,
        delivery: Option<Delivery>,
    ) -> ApiResult<(String, Option<String>)> {
        let mut body = json!({ "text": text });
        if let Some(delivery) = delivery {
            body["delivery"] = json!(delivery.as_str());
        }
        let entry = self
            .data(
                HttpMethod::Post,
                &session_path(session, "/prompt"),
                Some(body),
            )
            .await?;
        let id = entry
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::Decode("prompt: no inbox id".to_owned()))?
            .to_owned();
        let delivered = entry
            .get("delivery")
            .and_then(Value::as_str)
            .map(str::to_owned);
        Ok((id, delivered))
    }

    /// Runs a registered command; answers 204, and its execution arrives on
    /// the event stream.
    pub(super) async fn command(
        &self,
        session: &str,
        name: &str,
        arguments: &str,
    ) -> ApiResult<()> {
        self.call(
            HttpMethod::Post,
            &session_path(session, "/command"),
            Some(json!({ "name": name, "text": arguments })),
        )
        .await
        .map(drop)
    }

    /// Moves an undelivered inbox entry to steer the running turn.
    pub(super) async fn steer_inbox(&self, session: &str, inbox: &str) -> ApiResult<()> {
        self.call(
            HttpMethod::Patch,
            &format!(
                "{}/inbox/{}",
                session_path(session, ""),
                encode_path_segment(inbox)
            ),
            Some(json!({ "delivery": "steer" })),
        )
        .await
        .map(drop)
    }

    /// Whether there was a running execution to interrupt. Bare envelope.
    pub(super) async fn interrupt(&self, session: &str) -> ApiResult<bool> {
        let response = self
            .call(HttpMethod::Post, &session_path(session, "/interrupt"), None)
            .await?;
        Ok(response
            .body
            .get("interrupted")
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }

    pub(super) async fn reply_permission(
        &self,
        session: &str,
        request: &str,
        reply: PermissionReply,
    ) -> ApiResult<()> {
        // `always` would write the GLOBAL `/api/permission/saved`: it does
        // not exist as a reply.
        self.call(
            HttpMethod::Post,
            &format!(
                "{}/permission/{}/reply",
                session_path(session, ""),
                encode_path_segment(request)
            ),
            Some(json!({ "decision": reply })),
        )
        .await
        .map(drop)
    }

    pub(super) async fn reply_form(
        &self,
        session: &str,
        form: &str,
        answer: &FormAnswer,
    ) -> ApiResult<()> {
        self.call(
            HttpMethod::Post,
            &format!(
                "{}/form/{}/reply",
                session_path(session, ""),
                encode_path_segment(form)
            ),
            Some(json!({ "answer": answer })),
        )
        .await
        .map(drop)
    }

    /// Copies the session into a new one; `before` names the first message
    /// left out (none: everything). Answers the new session.
    pub(super) async fn fork(&self, session: &str, before: Option<&str>) -> ApiResult<Value> {
        let body = match before {
            Some(message) => json!({ "before": message }),
            None => json!({}),
        };
        self.data(
            HttpMethod::Post,
            &session_path(session, "/fork"),
            Some(body),
        )
        .await
    }

    /// Every stored message of the session, oldest first.
    pub(super) async fn messages(&self, session: &str) -> ApiResult<Vec<Value>> {
        let mut messages = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_MESSAGE_PAGES {
            // The order is baked into the cursor; sending both is a 400.
            let query = match &cursor {
                Some(cursor) => format!(
                    "?limit={MESSAGE_PAGE}&cursor={}",
                    encode_path_segment(cursor)
                ),
                None => format!("?limit={MESSAGE_PAGE}&order=asc"),
            };
            let response = self
                .call(
                    HttpMethod::Get,
                    &session_path(session, &format!("/message{query}")),
                    None,
                )
                .await?;
            let page = response
                .body
                .get("data")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            // The empty page is the only terminator: `cursor.next` is minted
            // on every page including the last.
            if page.is_empty() {
                return Ok(messages);
            }
            let next = response
                .body
                .pointer("/cursor/next")
                .and_then(Value::as_str)
                .map(str::to_owned);
            messages.extend(page);
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok(messages),
            }
        }
        Err(ApiError::Decode(format!(
            "session {session} has too many messages"
        )))
    }
}
