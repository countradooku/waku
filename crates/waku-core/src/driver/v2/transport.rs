//! The only way a driver reaches a provider (RFC 0002 "Transports"): stdio
//! child processes, HTTP plus streaming downlinks, and the provider's home on
//! disk. Replay replaces exactly this layer.

use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use tokio::io::{AsyncRead, AsyncWrite};
use url::Url;

use super::InstanceConfig;

/// A stdio child process to start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    /// Only the variables the driver sets on top of the inherited environment.
    pub env: Vec<(String, String)>,
    pub cwd: PathBuf,
}

/// How a process ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessExit {
    pub success: bool,
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

impl ProcessExit {
    pub const SUCCESS: Self = Self {
        success: true,
        code: Some(0),
        signal: None,
    };

    pub fn from_status(status: std::process::ExitStatus) -> Self {
        #[cfg(unix)]
        let signal = std::os::unix::process::ExitStatusExt::signal(&status);
        #[cfg(not(unix))]
        let signal = None;
        Self {
            success: status.success(),
            code: status.code(),
            signal,
        }
    }
}

/// A started stdio process. Closing `stdin` is how a driver shuts it down.
pub struct StdioTransport {
    pub stdin: Box<dyn AsyncWrite + Send + Unpin>,
    pub stdout: Box<dyn AsyncRead + Send + Unpin>,
    pub stderr: Box<dyn AsyncRead + Send + Unpin>,
    pub exit: BoxFuture<'static, io::Result<ProcessExit>>,
    pub pid: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: serde_json::Value,
}

/// A downlink: one JSON value per server-sent event or WebSocket message.
/// The stream ends when the connection closes; an `Err` item ends it too
/// (a timeout, a server-reported stream failure). Dropping it closes the
/// connection.
pub type Downlink = BoxStream<'static, io::Result<serde_json::Value>>;

/// An HTTP client plus streaming downlinks (OpenCode service, DeepSeek
/// `dsh web`).
///
/// `path` is relative to the base URL the transport was opened with and may
/// carry a query. A non-2xx answer is an `Ok` response with its status: the
/// driver reads the server's own error body. `Err` means no HTTP answer at
/// all (connect failure, timeout, a malformed response).
#[async_trait]
pub trait HttpTransport: Send + Sync {
    async fn request(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> io::Result<HttpResponse>;
    /// A server-sent-event downlink (`GET` with `Accept: text/event-stream`).
    async fn stream(&self, path: &str) -> io::Result<Downlink>;
    /// A WebSocket downlink whose text messages are JSON values (DeepSeek
    /// Harness `/api/events.*`).
    async fn websocket(&self, _path: &str) -> io::Result<Downlink> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "this transport has no WebSocket downlink",
        ))
    }
}

#[async_trait]
pub trait TransportFactory: Send + Sync {
    /// A stdio child process. `channel` names it in replay ("main", "fork-1").
    async fn spawn_stdio(&self, channel: &str, spec: ProcessSpec) -> io::Result<StdioTransport>;
    /// HTTP client plus streaming downlinks.
    async fn http(&self, channel: &str, base: Url) -> io::Result<Arc<dyn HttpTransport>>;
    /// Root for provider state on disk. Replay points it at a temp copy of
    /// the fixture's home.
    fn provider_home(&self, cfg: &InstanceConfig) -> PathBuf;
    /// Sends SIGINT to a process this factory spawned under `channel` (its
    /// `StdioTransport::pid`): Amp's interrupt, the one exception to
    /// "shut down by closing stdin" (RFC 0002 "Threading" rule 2). Replay
    /// records it as an outbound `{"type": "signal", "signal": "SIGINT"}`.
    async fn interrupt(&self, _channel: &str, pid: Option<u32>) -> io::Result<()> {
        send_interrupt(pid)
    }
}

/// SIGINT to `pid`.
pub fn send_interrupt(pid: Option<u32>) -> io::Result<()> {
    let Some(pid) = pid.and_then(|pid| i32::try_from(pid).ok()) else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "the process has no pid to signal",
        ));
    };
    #[cfg(unix)]
    {
        // SAFETY: `kill` takes a plain pid and signal number.
        if unsafe { libc::kill(pid, libc::SIGINT) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "interrupting a process needs a Unix signal",
        ))
    }
}

/// Real processes. Children are never killed on drop: `close()` owns
/// shutdown by closing stdin.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessTransportFactory;

#[async_trait]
impl TransportFactory for ProcessTransportFactory {
    async fn spawn_stdio(&self, _channel: &str, spec: ProcessSpec) -> io::Result<StdioTransport> {
        // The provider runs with the login-shell environment and search path
        // the v1 drivers use (`command_env`).
        let mut command = tokio::process::Command::from(crate::command_env::command(&spec.program));
        command
            .args(&spec.args)
            .envs(spec.env.iter().map(|(key, value)| (key, value)))
            .current_dir(&spec.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let missing = |name: &str| io::Error::other(format!("child {name} unavailable"));
        let stdin = child.stdin.take().ok_or_else(|| missing("stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| missing("stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| missing("stderr"))?;
        let pid = child.id();
        Ok(StdioTransport {
            stdin: Box::new(stdin),
            stdout: Box::new(stdout),
            stderr: Box::new(stderr),
            exit: Box::pin(async move { child.wait().await.map(ProcessExit::from_status) }),
            pid,
        })
    }

    async fn http(&self, _channel: &str, base: Url) -> io::Result<Arc<dyn HttpTransport>> {
        // One client per origin and credential, shared by every session that
        // talks to it, so keep-alive connections are leased across sessions.
        Ok(super::http::HttpClient::shared(
            base,
            super::http::HttpOptions::default(),
        )?)
    }

    fn provider_home(&self, cfg: &InstanceConfig) -> PathBuf {
        cfg.provider_home
            .clone()
            .or_else(dirs::home_dir)
            .unwrap_or_default()
    }
}
