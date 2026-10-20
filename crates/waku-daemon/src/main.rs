use std::io::Write as _;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use waku_protocol::{DAEMON_TOKEN_ENV, DaemonReady, PROTOCOL_VERSION};

fn main() -> anyhow::Result<()> {
    #[cfg(feature = "profiling")]
    init_profiling();
    let arguments = Arguments::parse(std::env::args().skip(1))?;
    let token =
        std::env::var(DAEMON_TOKEN_ENV).context("Waku daemon authentication token is missing")?;
    // The bearer capability belongs only to this server process. Remove it
    // before any provider or workspace subprocess can inherit the daemon's
    // environment.
    unsafe { std::env::remove_var(DAEMON_TOKEN_ENV) };
    // Bind before any other thread starts: the local socket briefly narrows
    // the process umask.
    let local = arguments
        .socket
        .as_deref()
        .map(waku_core::LocalListener::bind)
        .transpose()?;
    let tcp = arguments
        .tcp_bind()
        .map(|bind| {
            let listener = TcpListener::bind(bind)
                .with_context(|| format!("could not bind Waku daemon to {bind}"))?;
            ensure_bind_allowed(listener.local_addr()?, arguments.allow_non_loopback)?;
            anyhow::Ok(listener)
        })
        .transpose()?;
    let ready = DaemonReady {
        address: match &tcp {
            Some(listener) => listener.local_addr()?.to_string(),
            None => String::new(),
        },
        socket: local
            .as_ref()
            .map(|local| local.path().to_string_lossy().into_owned()),
        protocol_version: PROTOCOL_VERSION,
        pid: std::process::id(),
    };
    println!("{}", serde_json::to_string(&ready)?);
    std::io::stdout().flush()?;

    let shutdown = waku_core::ShutdownSignal::new();
    if let Some(parent_pid) = arguments.parent_pid {
        let monitor_shutdown = shutdown.clone();
        std::thread::Builder::new()
            .name("waku-daemon-parent".into())
            .spawn(move || {
                wait_for_process_exit(parent_pid, &monitor_shutdown);
                monitor_shutdown.trigger();
            })?;
    }

    let task_path = waku_core::persistence::StateStore::default_path();
    let settings = waku_core::DaemonSettingsStore::open_with_legacy(
        waku_core::DaemonSettings::default_path(),
        [task_path.with_file_name("settings.json")],
    )
    .context("could not load daemon settings")?;
    let task_store = waku_core::persistence::StateStore::daemon(task_path);
    waku_core::serve_listeners(
        waku_core::Listeners { tcp, local },
        token,
        Arc::new(waku_core::daemon::WakuBackend::new(settings, task_store)?),
        shutdown,
        waku_core::ServerOptions {
            allowed_origins: arguments.allowed_origins.into_iter().collect(),
            allow_shutdown: arguments.parent_pid.is_some(),
        },
    )
}

/// Record waku-core's spans in Tracy, and on stderr when `WAKU_TRACE` names
/// a filter. The desktop app's `src/profiling.rs` is the same setup; the
/// daemon shows up as a second client in Tracy's discovery list.
#[cfg(feature = "profiling")]
fn init_profiling() {
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::filter::Targets;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;

    let stderr = std::env::var("WAKU_TRACE")
        .ok()
        .filter(|filter| !filter.trim().is_empty())
        .map(|filter| {
            let everything = Targets::new().with_default(tracing::Level::TRACE);
            let targets = match filter.trim() {
                "1" | "true" | "all" => everything,
                filter => filter.parse().unwrap_or_else(|error| {
                    eprintln!("ignoring unparsable WAKU_TRACE={filter:?}: {error}");
                    everything
                }),
            };
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_span_events(FmtSpan::CLOSE)
                .with_filter(targets)
        });
    let _ = tracing_subscriber::registry()
        .with(tracing_tracy::TracyLayer::default())
        .with(stderr)
        .try_init();
}

fn ensure_bind_allowed(address: SocketAddr, allow_non_loopback: bool) -> anyhow::Result<()> {
    if address.ip().is_loopback() || allow_non_loopback {
        return Ok(());
    }
    bail!(
        "refusing non-loopback daemon bind {address}; pass --allow-non-loopback only after configuring authentication and exact browser origins"
    )
}

struct Arguments {
    /// `--bind`: the TCP listener. Without it the daemon listens on TCP
    /// (127.0.0.1, any port) only when no `--socket` is given either.
    bind: Option<String>,
    /// `--socket`: the per-user local socket (or Windows pipe) to listen on.
    socket: Option<std::path::PathBuf>,
    parent_pid: Option<u32>,
    allowed_origins: Vec<String>,
    allow_non_loopback: bool,
}

impl Arguments {
    fn parse(arguments: impl IntoIterator<Item = String>) -> anyhow::Result<Self> {
        let mut bind = None;
        let mut socket = None;
        let mut parent_pid = None;
        let mut allowed_origins = Vec::new();
        let mut allow_non_loopback = false;
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--bind" => {
                    bind = Some(
                        arguments
                            .next()
                            .ok_or_else(|| anyhow!("--bind requires an address"))?,
                    );
                }
                "--socket" => {
                    socket = Some(
                        arguments
                            .next()
                            .filter(|path| !path.is_empty())
                            .ok_or_else(|| anyhow!("--socket requires a path"))?
                            .into(),
                    );
                }
                "--parent-pid" => {
                    parent_pid = Some(
                        arguments
                            .next()
                            .ok_or_else(|| anyhow!("--parent-pid requires a process id"))?
                            .parse()
                            .context("--parent-pid is not a valid process id")?,
                    );
                }
                "--allow-origin" => {
                    let origin = arguments
                        .next()
                        .filter(|origin| !origin.trim().is_empty())
                        .ok_or_else(|| anyhow!("--allow-origin requires an origin"))?;
                    allowed_origins.push(origin);
                }
                "--allow-non-loopback" => {
                    allow_non_loopback = true;
                }
                "--help" | "-h" => {
                    println!(
                        "usage: {} [--socket PATH] [--bind ADDRESS] [--allow-non-loopback] [--parent-pid PID] [--allow-origin ORIGIN]...",
                        env!("CARGO_BIN_NAME")
                    );
                    std::process::exit(0);
                }
                unknown => bail!("unknown argument {unknown:?}"),
            }
        }
        Ok(Self {
            bind,
            socket,
            parent_pid,
            allowed_origins,
            allow_non_loopback,
        })
    }
}

impl Arguments {
    /// The TCP address to listen on, if any. A daemon started with only
    /// `--socket` opens no TCP port at all.
    fn tcp_bind(&self) -> Option<&str> {
        match (&self.bind, &self.socket) {
            (Some(bind), _) => Some(bind),
            (None, Some(_)) => None,
            (None, None) => Some("127.0.0.1:0"),
        }
    }
}

/// How often the portable fallback checks whether the parent is still alive.
const PARENT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Block until `pid` exits or the daemon is already stopping. Where the
/// kernel can report a process exit (kqueue on macOS, a pidfd on Linux) this
/// sleeps without waking; elsewhere it polls.
fn wait_for_process_exit(pid: u32, shutdown: &waku_core::ShutdownSignal) {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    if wait_for_exit_event(pid).is_ok() {
        return;
    }
    while !shutdown.wait_timeout(PARENT_POLL_INTERVAL) {
        if !process_is_alive(pid) {
            return;
        }
    }
}

/// kqueue's `EVFILT_PROC`/`NOTE_EXIT` fires once when the process exits. A
/// parent that is already gone fails registration with `ESRCH`.
#[cfg(target_os = "macos")]
fn wait_for_exit_event(pid: u32) -> std::io::Result<()> {
    unsafe {
        let queue = libc::kqueue();
        if queue < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut change: libc::kevent = std::mem::zeroed();
        change.ident = pid as libc::uintptr_t;
        change.filter = libc::EVFILT_PROC;
        change.flags = libc::EV_ADD | libc::EV_ONESHOT;
        change.fflags = libc::NOTE_EXIT;
        let registered = libc::kevent(queue, &change, 1, std::ptr::null_mut(), 0, std::ptr::null());
        if registered < 0 {
            let error = std::io::Error::last_os_error();
            libc::close(queue);
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(())
            } else {
                Err(error)
            };
        }
        let mut event: libc::kevent = std::mem::zeroed();
        let result = loop {
            if libc::kevent(queue, std::ptr::null(), 0, &mut event, 1, std::ptr::null()) > 0 {
                break Ok(());
            }
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINTR) {
                break Err(error);
            }
        };
        libc::close(queue);
        result
    }
}

/// A pidfd becomes readable when the process exits (Linux 5.3+).
#[cfg(target_os = "linux")]
fn wait_for_exit_event(pid: u32) -> std::io::Result<()> {
    unsafe {
        let fd = libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) as libc::c_int;
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(())
            } else {
                Err(error)
            };
        }
        let mut poll = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let result = loop {
            if libc::poll(&mut poll, 1, -1) >= 0 {
                break Ok(());
            }
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINTR) {
                break Err(error);
            }
        };
        libc::close(fd);
        result
    }
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as i32, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Windows reuses process ids, so the handle is opened for the narrowest
/// right that answers the question and closed immediately. A pid that no
/// longer exists fails to open; one that has exited but is still held open by
/// another handle reports an exit code instead of `STILL_ACTIVE`.
#[cfg(windows)]
fn process_is_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }
        let mut exit_code = 0_u32;
        let read = GetExitCodeProcess(handle, &mut exit_code);
        CloseHandle(handle);
        // A failed read leaves the parent's state unknown; outliving the app
        // is the safer error than shutting a live daemon down.
        read == 0 || exit_code == STILL_ACTIVE as u32
    }
}

#[cfg(not(any(unix, windows)))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_loopback_listener_requires_an_explicit_flag() {
        assert!(ensure_bind_allowed("127.0.0.1:3000".parse().unwrap(), false).is_ok());
        assert!(ensure_bind_allowed("[::1]:3000".parse().unwrap(), false).is_ok());
        assert!(ensure_bind_allowed("0.0.0.0:3000".parse().unwrap(), false).is_err());
        assert!(ensure_bind_allowed("[::]:3000".parse().unwrap(), false).is_err());
        assert!(ensure_bind_allowed("0.0.0.0:3000".parse().unwrap(), true).is_ok());
    }

    #[test]
    fn parses_repeated_browser_origin_allowlist_entries() {
        let arguments = Arguments::parse([
            "--allow-origin".into(),
            "https://app.waku.test".into(),
            "--allow-origin".into(),
            "http://localhost:3000".into(),
        ])
        .unwrap();

        assert_eq!(
            arguments.allowed_origins,
            ["https://app.waku.test", "http://localhost:3000"]
        );
        assert!(!arguments.allow_non_loopback);
    }

    #[test]
    fn a_local_socket_replaces_the_default_tcp_listener() {
        let socket_only = Arguments::parse(["--socket".into(), "/tmp/waku.sock".into()]).unwrap();
        assert_eq!(socket_only.tcp_bind(), None);
        let both = Arguments::parse([
            "--socket".into(),
            "/tmp/waku.sock".into(),
            "--bind".into(),
            "0.0.0.0:34123".into(),
        ])
        .unwrap();
        assert_eq!(both.tcp_bind(), Some("0.0.0.0:34123"));
        let neither = Arguments::parse(Vec::<String>::new()).unwrap();
        assert_eq!(neither.tcp_bind(), Some("127.0.0.1:0"));
        assert!(Arguments::parse(["--socket".into()]).is_err());
    }

    #[test]
    fn parses_explicit_non_loopback_opt_in() {
        let arguments = Arguments::parse(["--allow-non-loopback".into()]).unwrap();
        assert!(arguments.allow_non_loopback);
    }
}
