use std::collections::HashSet;
use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::SystemTime;
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use crossbeam_channel::{Receiver, Sender, unbounded};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::DaemonClient;
use waku_protocol::{APP_EXECUTABLE_ENV, DAEMON_TOKEN_ENV, DaemonReady, PROTOCOL_VERSION};
const START_TIMEOUT: Duration = Duration::from_secs(15);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
/// How often a development supervisor (`watch_for_rebuilds`, which only
/// debug builds of the app pass) checks the daemon binary for a rebuild. A
/// release supervisor never polls: it sleeps until the child exits or the
/// connection closes (#178).
const REBUILD_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// First delay before retrying a failed restart or reconnect; it doubles up
/// to [`MAX_RETRY_INTERVAL`]. These timers only run while the daemon is
/// unreachable, never while it is healthy.
const MIN_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(2);
pub const DEFAULT_EXPOSED_DAEMON_PORT: u16 = 34_123;

/// Desktop-owned launch configuration for the daemon it supervises.
///
/// Provider settings belong to the daemon and live in `settings.json`; this
/// is an app preference because it controls how the desktop launches its own
/// child process. The bearer token is intentionally stable across daemon-only
/// rebuilds and desktop relaunches so a configured web client keeps working.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct DaemonExposureSettings {
    pub enabled: bool,
    pub port: u16,
    pub allowed_origins: Vec<String>,
    pub token: String,
}

impl Default for DaemonExposureSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            port: DEFAULT_EXPOSED_DAEMON_PORT,
            allowed_origins: vec!["http://localhost:3001".into()],
            token: Self::new_token(),
        }
    }
}

impl DaemonExposureSettings {
    pub fn new_token() -> String {
        Uuid::new_v4().simple().to_string()
    }

    pub fn ensure_token(&mut self) -> bool {
        if !self.token.trim().is_empty() {
            return false;
        }
        self.token = Self::new_token();
        true
    }

    pub fn allowed_origins_text(&self) -> String {
        self.allowed_origins.join(", ")
    }

    pub fn with_allowed_origins_text(mut self, text: &str) -> anyhow::Result<Self> {
        self.allowed_origins = parse_allowed_origins(text)?;
        Ok(self)
    }

    pub fn validate(mut self) -> anyhow::Result<Self> {
        if self.port == 0 {
            bail!("daemon port must be between 1 and 65535");
        }
        if self.token.trim().is_empty() {
            bail!("daemon authentication token is empty");
        }
        self.allowed_origins = parse_allowed_origins(&self.allowed_origins_text())?;
        Ok(self)
    }

    /// The TCP listener the managed daemon opens, only when exposure is on.
    /// The desktop itself always talks to it over the local socket.
    fn bind_address(&self) -> Option<String> {
        self.enabled.then(|| format!("0.0.0.0:{}", self.port))
    }
}

/// Parse the comma-separated exact browser origins edited by the desktop.
/// Browser Origin headers contain only an HTTP(S) origin, never a path.
pub fn parse_allowed_origins(text: &str) -> anyhow::Result<Vec<String>> {
    let mut origins = Vec::new();
    let mut seen = HashSet::new();
    for candidate in text
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let url = url::Url::parse(candidate)
            .with_context(|| format!("invalid browser origin {candidate:?}"))?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            bail!(
                "browser origin {candidate:?} must be an exact http:// or https:// origin without a path"
            );
        }
        let origin = url.origin().ascii_serialization();
        if origin == "null" {
            bail!("browser origin {candidate:?} is not a network origin");
        }
        if seen.insert(origin.clone()) {
            origins.push(origin);
        }
    }
    Ok(origins)
}

pub struct DaemonProcess {
    client: DaemonClient,
    child: Child,
    /// Where the desktop reaches this daemon, and its token: what a second,
    /// protocol v8 connection needs.
    address: String,
    token: String,
}

impl DaemonProcess {
    pub fn spawn(executable: &Path) -> anyhow::Result<Self> {
        Self::spawn_configured(executable, DaemonExposureSettings::default())
    }

    fn spawn_configured(
        executable: &Path,
        settings: DaemonExposureSettings,
    ) -> anyhow::Result<Self> {
        let settings = settings.validate()?;
        let token = settings.token.clone();
        let app_executable = std::env::current_exe().context("could not locate Waku executable")?;
        let mut command = ProcessCommand::new(executable);
        // The desktop is a GUI-subsystem binary on Windows, so a console
        // child would get a console window of its own. `stderr` still reaches
        // the app's inherited handle.
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;

            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }
        // The desktop reaches its daemon over the per-user local socket (#19);
        // TCP is opened only for exposure to other devices and browsers.
        command
            .arg("--socket")
            .arg(waku_protocol::identity::daemon_socket_path())
            .arg("--parent-pid")
            .arg(std::process::id().to_string());
        if let Some(bind) = settings.bind_address() {
            command.arg("--bind").arg(bind).arg("--allow-non-loopback");
        }
        for origin in &settings.allowed_origins {
            command.arg("--allow-origin").arg(origin);
        }
        let mut child = command
            .env(DAEMON_TOKEN_ENV, &token)
            .env(APP_EXECUTABLE_ENV, app_executable)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("could not launch {}", executable.display()))?;
        let stdout = child
            .stdout
            .take()
            .context("Waku daemon did not expose its readiness stream")?;
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("waku-daemon-ready".into())
            .spawn(move || {
                let mut line = String::new();
                let result = BufReader::new(stdout)
                    .read_line(&mut line)
                    .map_err(anyhow::Error::from)
                    .and_then(|bytes| {
                        if bytes == 0 {
                            bail!("Waku daemon exited before becoming ready")
                        }
                        serde_json::from_str::<DaemonReady>(&line).map_err(anyhow::Error::from)
                    });
                let _ = ready_tx.send(result);
            })
            .context("could not start Waku daemon readiness reader")?;
        let ready = match ready_rx.recv_timeout(START_TIMEOUT) {
            Ok(Ok(ready)) => ready,
            Ok(Err(error)) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                bail!("timed out waiting for Waku daemon: {error}");
            }
        };
        if ready.protocol_version != PROTOCOL_VERSION {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "daemon protocol {} does not match desktop protocol {}",
                ready.protocol_version,
                PROTOCOL_VERSION
            );
        }
        let client_address = match ready.socket.as_deref() {
            Some(socket) => Ok(crate::local_endpoint_address(socket)),
            None => desktop_client_address(&ready.address),
        };
        let client_address = match client_address {
            Ok(address) => address,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        };
        let client = match DaemonClient::connect(&client_address, token.clone()) {
            Ok(client) => client,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        };
        Ok(Self {
            client,
            child,
            address: client_address,
            token,
        })
    }

    pub fn client(&self) -> DaemonClient {
        self.client.clone()
    }

    fn has_exited(&mut self) -> bool {
        !matches!(self.child.try_wait(), Ok(None))
    }

    /// Send `wake` when the child exits, from a thread that blocks in
    /// `waitid(WNOWAIT)`: the kernel wakes it on exit and the zombie is left
    /// for [`Self::has_exited`] or [`Self::stop`] to reap, so this never races
    /// the owner of the `Child`. Any failure (the child was already reaped)
    /// also wakes the supervisor, which then checks for itself.
    ///
    /// Elsewhere there is no exit thread; the daemon's exit closes its
    /// connection, and the supervisor's disconnect wake covers it.
    #[cfg(unix)]
    fn notify_on_exit(&self, wake: Sender<()>) {
        let pid = self.child.id() as libc::id_t;
        let spawned = std::thread::Builder::new()
            .name("waku-daemon-exit".into())
            .spawn(move || {
                loop {
                    let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
                    let result = unsafe {
                        libc::waitid(libc::P_PID, pid, &mut status, libc::WEXITED | libc::WNOWAIT)
                    };
                    if result == 0
                        || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR)
                    {
                        break;
                    }
                }
                let _ = wake.send(());
            });
        if spawned.is_err() {
            eprintln!("could not watch the Waku daemon for exit; relying on its connection");
        }
    }

    #[cfg(not(unix))]
    fn notify_on_exit(&self, _wake: Sender<()>) {}

    fn stop(&mut self) {
        self.client.shutdown();
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        while Instant::now() < deadline {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(25)),
                Err(_) => break,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

fn desktop_client_address(address: &str) -> anyhow::Result<String> {
    let address = address
        .parse::<std::net::SocketAddr>()
        .with_context(|| format!("Waku daemon returned an invalid address {address:?}"))?;
    let ip = if address.ip().is_unspecified() {
        if address.is_ipv4() {
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        } else {
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
        }
    } else {
        address.ip()
    };
    Ok(std::net::SocketAddr::new(ip, address.port()).to_string())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExecutableStamp {
    modified: Option<SystemTime>,
    len: u64,
}

impl ExecutableStamp {
    fn read(path: &Path) -> anyhow::Result<Self> {
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("could not inspect {}", path.display()))?;
        Ok(Self {
            modified: metadata.modified().ok(),
            len: metadata.len(),
        })
    }
}

struct SupervisorInner {
    executable: Option<PathBuf>,
    target: Mutex<DaemonTarget>,
    exposure: Mutex<Option<DaemonExposureSettings>>,
    restart: Mutex<()>,
    client_updates: Mutex<Vec<Sender<DaemonClient>>>,
    running: AtomicBool,
    /// Wakes the monitor thread: the child exited, the connection closed,
    /// a restart needs retrying, or the supervisor is gone.
    wake: Sender<()>,
}

impl Drop for SupervisorInner {
    fn drop(&mut self) {
        // The monitor holds only a weak reference; this wake lets it notice.
        let _ = self.wake.send(());
    }
}

impl SupervisorInner {
    /// Wake the monitor when `client`'s connection closes.
    fn watch_client(&self, client: &DaemonClient) {
        let wake = self.wake.clone();
        client.on_disconnect(move || {
            let _ = wake.send(());
        });
    }

    /// Wake the monitor when `process` exits or its connection closes.
    fn watch_process(&self, process: &DaemonProcess) {
        process.notify_on_exit(self.wake.clone());
        self.watch_client(&process.client);
    }
}

enum DaemonTarget {
    Local(DaemonProcess),
    Restarting(DaemonClient),
    Remote {
        client: DaemonClient,
        address: String,
        token: String,
    },
}

impl DaemonTarget {
    fn endpoint(&self) -> Option<(String, String)> {
        match self {
            Self::Local(process) => Some((process.address.clone(), process.token.clone())),
            Self::Restarting(_) => None,
            Self::Remote { address, token, .. } => Some((address.clone(), token.clone())),
        }
    }

    fn client(&self) -> DaemonClient {
        match self {
            Self::Local(process) => process.client(),
            Self::Restarting(client) => client.clone(),
            Self::Remote { client, .. } => client.clone(),
        }
    }
}

/// Owns the current daemon and, in development, swaps it after a successful
/// rebuild without requiring the desktop process to relaunch.
#[derive(Clone)]
pub struct DaemonSupervisor {
    inner: Arc<SupervisorInner>,
}

impl DaemonSupervisor {
    pub fn spawn(executable: &Path, watch_for_rebuilds: bool) -> anyhow::Result<Self> {
        Self::spawn_configured(
            executable,
            watch_for_rebuilds,
            DaemonExposureSettings::default(),
        )
    }

    pub fn spawn_configured(
        executable: &Path,
        watch_for_rebuilds: bool,
        exposure: DaemonExposureSettings,
    ) -> anyhow::Result<Self> {
        let exposure = exposure.validate()?;
        let process = DaemonProcess::spawn_configured(executable, exposure.clone())?;
        let initial_stamp = ExecutableStamp::read(executable)?;
        let (supervisor, wake) = Self::from_target(
            DaemonTarget::Local(process),
            Some(executable.to_owned()),
            Some(exposure),
        )?;
        if let DaemonTarget::Local(process) = &*supervisor.inner.target.lock() {
            supervisor.inner.watch_process(process);
        }
        let weak_inner = Arc::downgrade(&supervisor.inner);
        std::thread::Builder::new()
            .name("waku-daemon-supervisor".into())
            .spawn(move || {
                monitor_daemon(weak_inner, wake, Some(initial_stamp), watch_for_rebuilds)
            })
            .context("could not start Waku daemon supervisor")?;
        Ok(supervisor)
    }

    /// Connect to a daemon managed on another host (or by an external local
    /// service manager). Dropping the desktop never shuts this daemon down.
    pub fn connect(address: &str, token: String) -> anyhow::Result<Self> {
        let client = DaemonClient::connect(address, token.clone())?;
        let (supervisor, wake) = Self::from_target(
            DaemonTarget::Remote {
                client: client.clone(),
                address: address.to_owned(),
                token,
            },
            None,
            None,
        )?;
        supervisor.inner.watch_client(&client);
        let weak_inner = Arc::downgrade(&supervisor.inner);
        std::thread::Builder::new()
            .name("waku-remote-daemon-supervisor".into())
            .spawn(move || monitor_daemon(weak_inner, wake, None, false))
            .context("could not start remote Waku daemon supervisor")?;
        Ok(supervisor)
    }

    fn from_target(
        target: DaemonTarget,
        executable: Option<PathBuf>,
        exposure: Option<DaemonExposureSettings>,
    ) -> anyhow::Result<(Self, Receiver<()>)> {
        let (wake, wake_rx) = unbounded();
        let inner = Arc::new(SupervisorInner {
            executable,
            target: Mutex::new(target),
            exposure: Mutex::new(exposure),
            restart: Mutex::new(()),
            client_updates: Mutex::new(Vec::new()),
            running: AtomicBool::new(true),
            wake,
        });
        Ok((Self { inner }, wake_rx))
    }

    pub fn client(&self) -> DaemonClient {
        self.inner.target.lock().client()
    }

    /// Subscribe to the active daemon connection. The current client is sent
    /// immediately, followed by each replacement after a managed restart.
    pub fn subscribe_clients(&self) -> Receiver<DaemonClient> {
        let (updates, receiver) = unbounded();
        // Holding the target lock through registration makes the initial send
        // atomic with respect to replacement: a subscriber sees either the old
        // client followed by the new one, or the new client directly.
        let target = self.inner.target.lock();
        self.inner.client_updates.lock().push(updates.clone());
        let _ = updates.send(target.client());
        receiver
    }

    pub fn is_remote(&self) -> bool {
        self.inner.executable.is_none()
    }

    /// Where the current daemon listens and its token, for a protocol v8
    /// connection beside this one; `None` while a restart is in progress.
    pub fn endpoint(&self) -> Option<(String, String)> {
        self.inner.target.lock().endpoint()
    }

    /// Restart only the desktop-managed daemon with a new listener policy.
    /// The caller should run this off the UI thread.
    pub fn reconfigure(&self, exposure: DaemonExposureSettings) -> anyhow::Result<()> {
        let exposure = exposure.validate()?;
        let executable = self
            .inner
            .executable
            .as_ref()
            .context("the connected daemon is managed outside Waku Desktop")?
            .clone();
        let _restart = self.inner.restart.lock();
        let previous = self
            .inner
            .exposure
            .lock()
            .clone()
            .context("managed daemon launch settings are unavailable")?;
        match replace_local_daemon(&self.inner, &executable, &exposure) {
            Ok(()) => {
                *self.inner.exposure.lock() = Some(exposure);
                Ok(())
            }
            Err(error) => match replace_local_daemon(&self.inner, &executable, &previous) {
                Ok(()) => Err(error),
                Err(restore_error) => {
                    // The target is left `Restarting`; the monitor retries
                    // with the previous settings from here.
                    let _ = self.inner.wake.send(());
                    Err(error.context(format!(
                        "the previous daemon configuration also failed to restart: {restore_error:#}"
                    )))
                }
            },
        }
    }
}

impl Drop for DaemonSupervisor {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.running.store(false, Ordering::Release);
        }
    }
}

/// Sleeps until something can have changed the daemon's state, then checks
/// it. Wakes come from the child's exit, the connection closing, a failed
/// `reconfigure`, and the supervisor being dropped; the only timers are the
/// retry backoff while the daemon is unreachable and, with
/// `watch_for_rebuilds` (debug builds of the app only), the rebuild poll. A
/// healthy release supervisor therefore never wakes (#178).
fn monitor_daemon(
    weak_inner: std::sync::Weak<SupervisorInner>,
    wake: Receiver<()>,
    mut active_stamp: Option<ExecutableStamp>,
    watch_for_rebuilds: bool,
) {
    let mut retry: Option<Duration> = None;
    loop {
        let rebuild_poll = watch_for_rebuilds.then_some(REBUILD_POLL_INTERVAL);
        let timeout = match (retry, rebuild_poll) {
            (Some(retry), Some(poll)) => Some(retry.min(poll)),
            (timeout, None) | (None, timeout) => timeout,
        };
        let closed = match timeout {
            Some(timeout) => matches!(
                wake.recv_timeout(timeout),
                Err(crossbeam_channel::RecvTimeoutError::Disconnected)
            ),
            None => wake.recv().is_err(),
        };
        if closed {
            return;
        }
        // One check covers every wake queued so far.
        while wake.try_recv().is_ok() {}
        let Some(inner) = weak_inner.upgrade() else {
            return;
        };
        if !inner.running.load(Ordering::Acquire) {
            return;
        }
        retry = match check_daemon(&inner, &mut active_stamp, watch_for_rebuilds) {
            Check::Settled => None,
            Check::Retry => Some(retry.map_or(MIN_RETRY_INTERVAL, |previous| {
                (previous * 2).min(MAX_RETRY_INTERVAL)
            })),
        };
    }
}

enum Check {
    /// Nothing to do until the next wake.
    Settled,
    /// The daemon is unreachable and the attempt to fix it failed; try again
    /// after a backoff.
    Retry,
}

/// What [`check_daemon`] reads of a local target.
struct LocalState {
    /// The process exited (or a restart is pending), or the binary changed.
    needs_restart: bool,
    connection_closed: bool,
    observed_stamp: Option<ExecutableStamp>,
}

fn local_state(
    inner: &SupervisorInner,
    executable: &Path,
    active_stamp: Option<ExecutableStamp>,
    watch_for_rebuilds: bool,
) -> Option<LocalState> {
    let (process_exited, connection_closed) = match &mut *inner.target.lock() {
        DaemonTarget::Local(process) => (process.has_exited(), process.client.is_disconnected()),
        DaemonTarget::Restarting(_) => (true, true),
        DaemonTarget::Remote { .. } => return None,
    };
    let observed_stamp = watch_for_rebuilds
        .then(|| ExecutableStamp::read(executable).ok())
        .flatten();
    let executable_changed = observed_stamp.is_some_and(|observed| Some(observed) != active_stamp);
    Some(LocalState {
        needs_restart: process_exited || executable_changed,
        connection_closed,
        observed_stamp,
    })
}

fn check_daemon(
    inner: &SupervisorInner,
    active_stamp: &mut Option<ExecutableStamp>,
    watch_for_rebuilds: bool,
) -> Check {
    let remote_reconnect = {
        let target = inner.target.lock();
        match &*target {
            DaemonTarget::Remote {
                client,
                address,
                token,
            } if client.is_disconnected() => Some((
                client.clone(),
                address.clone(),
                token.clone(),
                client.last_sequences(),
            )),
            _ => None,
        }
    };
    if let Some((disconnected, address, token, resume_from)) = remote_reconnect {
        let _restart = inner.restart.lock();
        let still_current = matches!(
            &*inner.target.lock(),
            DaemonTarget::Remote { client, .. }
                if client.same_connection(&disconnected) && client.is_disconnected()
        );
        if !still_current {
            return Check::Settled;
        }
        let Ok(replacement) =
            DaemonClient::connect_with_resume(&address, token.clone(), resume_from)
        else {
            return Check::Retry;
        };
        inner.watch_client(&replacement);
        *inner.target.lock() = DaemonTarget::Remote {
            client: replacement.clone(),
            address,
            token,
        };
        inner
            .client_updates
            .lock()
            .retain(|subscriber| subscriber.send(replacement.clone()).is_ok());
        return Check::Settled;
    }
    let Some(executable) = inner.executable.as_deref() else {
        return Check::Settled;
    };
    let Some(state) = local_state(inner, executable, *active_stamp, watch_for_rebuilds) else {
        return Check::Settled;
    };
    if !state.needs_restart {
        // Without an exit thread a closed connection is the only sign of
        // exit, and it can arrive just before the process is gone.
        return if cfg!(not(unix)) && state.connection_closed {
            Check::Retry
        } else {
            Check::Settled
        };
    }
    let _restart = inner.restart.lock();
    // `reconfigure` may have replaced the daemon while this waited for the
    // lock (its old process's exit is what woke us), so look again.
    let Some(state) = local_state(inner, executable, *active_stamp, watch_for_rebuilds)
        .filter(|state| state.needs_restart)
    else {
        return Check::Settled;
    };
    let Some(exposure) = inner.exposure.lock().clone() else {
        return Check::Settled;
    };
    if let Err(error) = replace_local_daemon(inner, executable, &exposure) {
        eprintln!("could not restart rebuilt Waku daemon: {error:#}");
        return Check::Retry;
    }
    if let Some(observed_stamp) = state.observed_stamp {
        *active_stamp = Some(observed_stamp);
    }
    Check::Settled
}

fn replace_local_daemon(
    inner: &SupervisorInner,
    executable: &Path,
    exposure: &DaemonExposureSettings,
) -> anyhow::Result<()> {
    let previous = {
        let mut target = inner.target.lock();
        match &*target {
            DaemonTarget::Remote { .. } => {
                bail!("the connected daemon is managed outside Waku Desktop")
            }
            DaemonTarget::Restarting(_) => None,
            DaemonTarget::Local(process) => {
                let disconnected = process.client();
                let previous =
                    std::mem::replace(&mut *target, DaemonTarget::Restarting(disconnected));
                match previous {
                    DaemonTarget::Local(process) => Some(process),
                    _ => unreachable!("local daemon target changed while locked"),
                }
            }
        }
    };
    // Dropping can wait briefly for graceful shutdown, but the target lock is
    // already released so UI actions never block behind process teardown.
    drop(previous);
    let replacement = DaemonProcess::spawn_configured(executable, exposure.clone())?;
    inner.watch_process(&replacement);
    let client = replacement.client();
    *inner.target.lock() = DaemonTarget::Local(replacement);
    inner
        .client_updates
        .lock()
        .retain(|subscriber| subscriber.send(client.clone()).is_ok());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_origins_are_exact_and_deduplicated() {
        assert_eq!(
            parse_allowed_origins(
                "https://app.waku.test, http://localhost:3001, https://app.waku.test"
            )
            .unwrap(),
            ["https://app.waku.test", "http://localhost:3001"]
        );
        assert!(parse_allowed_origins("https://app.waku.test/path").is_err());
        assert!(parse_allowed_origins("ws://app.waku.test").is_err());
    }

    #[test]
    fn desktop_uses_loopback_to_reach_an_unspecified_listener() {
        assert_eq!(
            desktop_client_address("0.0.0.0:34123").unwrap(),
            "127.0.0.1:34123"
        );
        assert_eq!(desktop_client_address("[::]:34123").unwrap(), "[::1]:34123");
    }
}
