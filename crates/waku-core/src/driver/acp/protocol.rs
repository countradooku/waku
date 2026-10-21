//! JSON-RPC 2.0 framing for the Agent Client Protocol over a stdio transport.
//!
//! The driver speaks ACP's JSON-RPC itself instead of through the
//! `agent-client-protocol` SDK connection (RFC 0002 amendment, #48): the SDK
//! allocates UUIDv4 request ids, which replay cannot match literally, and its
//! connection is `!Send`. Requests here take ids from a per-channel counter
//! starting at 1 (RFC 0002 "Matching", rule 5); the SDK's schema types are
//! still used to read typed payloads such as session config options.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;

use crate::driver::v2::{DriverError, DriverResult, FrameDirection, ProtocolReason, RawFrameLog};

/// A JSON-RPC error object.
#[derive(Clone, Debug, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcError {
    pub fn from_value(value: &Value) -> Self {
        Self {
            code: value.get("code").and_then(Value::as_i64).unwrap_or(-32603),
            message: value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Internal error")
                .to_owned(),
            data: value.get("data").cloned().filter(|data| !data.is_null()),
        }
    }

    /// The connection went away before the response arrived.
    pub fn closed() -> Self {
        Self {
            code: CONNECTION_CLOSED,
            message: "the agent exited".to_owned(),
            data: None,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.code == CONNECTION_CLOSED
    }

    /// The most specific human-readable text the error carries.
    pub fn detail(&self) -> String {
        self.data
            .as_ref()
            .and_then(|data| {
                data.get("message")
                    .and_then(Value::as_str)
                    .or_else(|| data.as_str())
            })
            .map(str::to_owned)
            .unwrap_or_else(|| self.message.clone())
    }
}

/// Not a JSON-RPC code: marks a request whose connection closed.
const CONNECTION_CLOSED: i64 = i64::MIN;
pub const METHOD_NOT_FOUND: i64 = -32601;

/// One decoded inbound line.
#[derive(Debug, PartialEq)]
pub enum Inbound {
    Response {
        id: u64,
        result: Result<Value, RpcError>,
    },
    /// An agent → client request; `id` is echoed back verbatim.
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
        params: Value,
    },
}

/// Decodes one JSON-RPC message. `None` for anything else (a response to an
/// id this client never used, a non-object line).
pub fn decode(frame: &Value) -> Option<Inbound> {
    let method = frame.get("method").and_then(Value::as_str);
    let id = frame.get("id").filter(|id| !id.is_null());
    let params = frame.get("params").cloned().unwrap_or(Value::Null);
    match (method, id) {
        (Some(method), Some(id)) => Some(Inbound::Request {
            id: id.clone(),
            method: method.to_owned(),
            params,
        }),
        (Some(method), None) => Some(Inbound::Notification {
            method: method.to_owned(),
            params,
        }),
        (None, Some(id)) => {
            let id = id
                .as_u64()
                .or_else(|| id.as_str().and_then(|id| id.parse().ok()))?;
            let result = match frame.get("error") {
                Some(error) => Err(RpcError::from_value(error)),
                None => Ok(frame.get("result").cloned().unwrap_or(Value::Null)),
            };
            Some(Inbound::Response { id, result })
        }
        (None, None) => None,
    }
}

type Writer = Box<dyn AsyncWrite + Send + Unpin>;
type Pending = oneshot::Sender<Result<Value, RpcError>>;

/// The client side of one ACP connection: one writer, one id counter and the
/// map of requests in flight. Responses are routed by the read loop.
pub struct Connection {
    channel: String,
    writer: tokio::sync::Mutex<Option<Writer>>,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, Pending>>,
    raw_log: Arc<dyn RawFrameLog>,
}

impl Connection {
    pub fn new(channel: &str, writer: Writer, raw_log: Arc<dyn RawFrameLog>) -> Self {
        Self {
            channel: channel.to_owned(),
            writer: tokio::sync::Mutex::new(Some(writer)),
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            raw_log,
        }
    }

    async fn write(&self, frame: &Value) -> DriverResult<()> {
        let mut line = serde_json::to_vec(frame).map_err(|error| DriverError::Protocol {
            reason: ProtocolReason::Decode,
            detail: error.to_string(),
        })?;
        line.push(b'\n');
        let mut writer = self.writer.lock().await;
        let Some(writer) = writer.as_mut() else {
            return Err(DriverError::SessionLost {
                detail: "the agent's stdin is closed".to_owned(),
            });
        };
        self.raw_log
            .record(&self.channel, FrameDirection::Outbound, &frame.to_string());
        let written = async {
            writer.write_all(&line).await?;
            writer.flush().await
        }
        .await;
        written.map_err(|error| DriverError::SessionLost {
            detail: error.to_string(),
        })
    }

    /// Sends a request and returns its id and the receiver of its response,
    /// without waiting (a prompt's response is its turn's end).
    pub async fn send_request(
        &self,
        method: &str,
        params: Value,
    ) -> DriverResult<(u64, oneshot::Receiver<Result<Value, RpcError>>)> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().insert(id, sender);
        let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        if let Err(error) = self.write(&frame).await {
            self.pending.lock().remove(&id);
            return Err(error);
        }
        Ok((id, receiver))
    }

    /// Sends a request and waits for its response.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let (_, receiver) = self
            .send_request(method, params)
            .await
            .map_err(|_| RpcError::closed())?;
        receiver.await.unwrap_or_else(|_| Err(RpcError::closed()))
    }

    pub async fn notify(&self, method: &str, params: Value) -> DriverResult<()> {
        self.write(&json!({ "jsonrpc": "2.0", "method": method, "params": params }))
            .await
    }

    pub async fn respond(&self, id: &Value, result: Value) -> DriverResult<()> {
        self.write(&json!({ "jsonrpc": "2.0", "id": id, "result": result }))
            .await
    }

    pub async fn respond_error(&self, id: &Value, code: i64, message: &str) -> DriverResult<()> {
        self.write(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": code, "message": message }
        }))
        .await
    }

    /// Routes a response to its waiter. `false` for an unknown id.
    pub fn resolve(&self, id: u64, result: Result<Value, RpcError>) -> bool {
        match self.pending.lock().remove(&id) {
            Some(sender) => {
                let _ = sender.send(result);
                true
            }
            None => false,
        }
    }

    /// Fails every request in flight (the agent exited).
    pub fn fail_pending(&self) {
        for (_, sender) in self.pending.lock().drain() {
            let _ = sender.send(Err(RpcError::closed()));
        }
    }

    /// Closes stdin: the agent's shutdown signal.
    pub async fn close(&self) {
        if let Some(mut writer) = self.writer.lock().await.take() {
            let _ = writer.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_the_four_message_shapes() {
        assert_eq!(
            decode(&json!({"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}})),
            Some(Inbound::Response {
                id: 3,
                result: Ok(json!({"stopReason":"end_turn"}))
            })
        );
        assert!(matches!(
            decode(&json!({"jsonrpc":"2.0","id":3,"error":{"code":-32003,"message":"limit","data":{"message":"weekly limit"}}})),
            Some(Inbound::Response { id: 3, result: Err(error) }) if error.code == -32003 && error.detail() == "weekly limit"
        ));
        assert!(matches!(
            decode(&json!({"jsonrpc":"2.0","id":"perm-1","method":"session/request_permission","params":{}})),
            Some(Inbound::Request { id, method, .. }) if id == json!("perm-1") && method == "session/request_permission"
        ));
        assert!(matches!(
            decode(&json!({"jsonrpc":"2.0","method":"session/update","params":{"x":1}})),
            Some(Inbound::Notification { method, params }) if method == "session/update" && params["x"] == 1
        ));
        assert_eq!(decode(&json!({"jsonrpc":"2.0"})), None);
    }
}
