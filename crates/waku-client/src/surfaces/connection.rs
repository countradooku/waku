//! The desktop's shared protocol v8 connection.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context as _;
use parking_lot::Mutex;

use crate::v8::{CallError, MethodDef, V8Client};

/// Where the daemon listens and the token it takes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// `unix:<path>`, `pipe:<name>`, `host:port`, `ws://…` or `wss://…`.
    pub address: String,
    pub token: String,
}

impl Endpoint {
    /// Whether the daemon runs on this machine behind its local socket, so
    /// its paths are this machine's paths.
    pub fn is_local(&self) -> bool {
        is_local_address(&self.address)
    }
}

pub(crate) fn is_local_address(address: &str) -> bool {
    address.starts_with("unix:")
        || address.starts_with("pipe:")
        || address.starts_with(r"\\.\pipe\")
}

type Source = dyn Fn() -> anyhow::Result<Endpoint> + Send + Sync;

struct Inner {
    source: Box<Source>,
    client: Mutex<Option<V8Client>>,
    /// Serializes reconnects so a burst of callers opens one connection.
    connecting: Mutex<()>,
    local: AtomicBool,
}

/// One protocol v8 connection for every non-session desktop surface, opened
/// at startup and reopened on demand after the daemon restarts.
///
/// [`Self::client`] and [`Self::call`] block (a reconnect, a round trip):
/// call them from the background executor or a worker thread, never from a
/// frame.
#[derive(Clone)]
pub struct DaemonV8 {
    inner: Arc<Inner>,
}

impl DaemonV8 {
    /// A handle that asks `source` where the daemon is whenever it needs a
    /// new connection (a supervised daemon moves when it restarts).
    pub fn new(source: impl Fn() -> anyhow::Result<Endpoint> + Send + Sync + 'static) -> Self {
        let local = source().is_ok_and(|endpoint| endpoint.is_local());
        Self {
            inner: Arc::new(Inner {
                source: Box::new(source),
                client: Mutex::new(None),
                connecting: Mutex::new(()),
                local: AtomicBool::new(local),
            }),
        }
    }

    /// A handle on a fixed endpoint.
    pub fn fixed(endpoint: Endpoint) -> Self {
        Self::new(move || Ok(endpoint.clone()))
    }

    /// Whether the daemon is reached through its local socket, where its
    /// paths are this machine's paths. Cheap; safe on the UI thread.
    pub fn is_local(&self) -> bool {
        self.inner.local.load(Ordering::Acquire)
    }

    fn current(&self) -> Option<V8Client> {
        self.inner
            .client
            .lock()
            .as_ref()
            .filter(|client| !client.is_disconnected())
            .cloned()
    }

    /// The live connection, connecting first when there is none. Blocks.
    pub fn client(&self) -> anyhow::Result<V8Client> {
        if let Some(client) = self.current() {
            return Ok(client);
        }
        let _connecting = self.inner.connecting.lock();
        if let Some(client) = self.current() {
            return Ok(client);
        }
        let endpoint = (self.inner.source)()?;
        self.inner
            .local
            .store(endpoint.is_local(), Ordering::Release);
        let client = V8Client::connect(&endpoint.address, endpoint.token)
            .context("could not connect to the Waku daemon")?;
        *self.inner.client.lock() = Some(client.clone());
        Ok(client)
    }

    /// Call `M` on the live connection. Blocks. A remote error keeps its
    /// [`crate::v8::Error`] (downcast it to branch on the code), but reads as
    /// its message alone.
    pub fn call<M: MethodDef>(&self, params: M::Params) -> anyhow::Result<M::Result> {
        let client = self.client()?;
        client.call::<M>(params).map_err(call_error)
    }

    /// Close the connection, if one is open.
    pub fn close(&self) {
        if let Some(client) = self.inner.client.lock().take() {
            client.close();
        }
    }
}

/// A failed call as the desktop shows it: the daemon's message without its
/// code prefix, with the typed error still reachable by downcasting.
pub fn call_error(error: CallError) -> anyhow::Error {
    match error {
        CallError::Remote(error) => {
            let message = error.message.clone();
            anyhow::Error::new(error).context(message)
        }
        other => anyhow::Error::new(other),
    }
}

/// The daemon's error code behind `error`, when it came from the daemon.
pub fn remote_code(error: &anyhow::Error) -> Option<crate::v8::ErrorCode> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<crate::v8::Error>())
        .map(|error| error.code.clone())
}
