//! Daemon-owned pseudoterminals for remote clients.
//!
//! The daemon owns the shell, cwd, and PTY. Clients only render the byte
//! stream and send input/resize controls, so a browser can operate against a
//! daemon on another machine without interpreting any daemon-side paths.

#[cfg(not(unix))]
use std::path::Path;

#[cfg(not(unix))]
use anyhow::bail;

#[cfg(not(unix))]
use crate::EventSink;

#[cfg(unix)]
mod platform {
    use std::io::{Read as _, Write as _};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};

    use crate::{EventSink, WireDriverEvent};
    use alacritty_terminal::event::{OnResize as _, WindowSize};
    use alacritty_terminal::tty::{self, EventedPty as _, EventedReadWrite as _, Shell};
    use anyhow::{Context as _, bail};
    use base64::Engine as _;
    use parking_lot::Mutex;
    use polling::{Event, Events, PollMode, Poller};
    use waku_protocol::wire_event::TerminalOutputPayload;

    const CELL_WIDTH: u16 = 8;
    const CELL_HEIGHT: u16 = 16;
    const MIN_COLUMNS: u16 = 2;
    const MIN_ROWS: u16 = 1;
    const SHELL_SHUTDOWN_GRACE: Duration = Duration::from_millis(250);
    /// One `read` from the PTY master.
    const READ_CHUNK: usize = 32 * 1024;
    /// Output already waiting in the PTY is coalesced into one event, up to
    /// this many bytes.
    const MAX_OUTPUT_BATCH: usize = 64 * 1024;
    /// Retry interval while the PTY reports `EIO` before the child has
    /// attached its slave. That window is brief and only at start-up; an
    /// open terminal otherwise never wakes without output.
    const SLAVE_ATTACH_RETRY: Duration = Duration::from_millis(4);

    pub struct DaemonTerminal {
        pty: Arc<Mutex<tty::Pty>>,
        stopped: Arc<AtomicBool>,
        /// The output thread blocks on this; dropping the terminal wakes it.
        poller: Arc<Poller>,
        reader: Option<JoinHandle<()>>,
    }

    impl DaemonTerminal {
        pub fn open(
            cwd: &std::path::Path,
            cols: u16,
            rows: u16,
            events: EventSink,
        ) -> anyhow::Result<Self> {
            let shell = crate::command_env::default_terminal_shell();
            let shell_args = crate::command_env::default_terminal_shell_args(&shell);
            Self::open_with_shell(
                cwd,
                cols,
                rows,
                events,
                Shell::new(shell.to_string_lossy().into_owned(), shell_args),
            )
        }

        pub(crate) fn open_with_shell(
            cwd: &std::path::Path,
            cols: u16,
            rows: u16,
            events: EventSink,
            shell: Shell,
        ) -> anyhow::Result<Self> {
            if !cwd.is_dir() {
                bail!(
                    "terminal working directory does not exist: {}",
                    cwd.display()
                );
            }

            let mut options = tty::Options {
                shell: Some(shell),
                working_directory: Some(cwd.to_owned()),
                drain_on_exit: false,
                ..Default::default()
            };
            for (name, value) in crate::command_env::shell_environment() {
                options.env.insert(
                    name.to_string_lossy().into_owned(),
                    value.to_string_lossy().into_owned(),
                );
            }
            options.env.insert("TERM".into(), "xterm-256color".into());
            options.env.insert("COLORTERM".into(), "truecolor".into());

            let size = window_size(cols, rows);
            let pty = tty::new(&options, size, 0)
                .with_context(|| format!("spawn terminal in {}", cwd.display()))?;
            let output = pty.file().try_clone().context("clone terminal output")?;
            let poller = Arc::new(Poller::new().context("create terminal output poller")?);
            // SAFETY: the output thread owns `output` and deletes it from the
            // poller before the descriptor is closed.
            unsafe { poller.add_with_mode(&output, Event::readable(0), PollMode::Level) }
                .context("watch terminal output")?;
            let pty = Arc::new(Mutex::new(pty));
            let stopped = Arc::new(AtomicBool::new(false));
            let reader = {
                let pty = pty.clone();
                let stopped = stopped.clone();
                let poller = poller.clone();
                std::thread::Builder::new()
                    .name("waku-daemon-terminal-output".into())
                    .spawn(move || {
                        read_output(&output, &pty, &stopped, &poller, &events);
                        let _ = poller.delete(&output);
                    })
                    .context("start terminal output thread")?
            };

            Ok(Self {
                pty,
                stopped,
                poller,
                reader: Some(reader),
            })
        }

        pub fn write(&self, data: Vec<u8>) -> anyhow::Result<()> {
            if data.is_empty() {
                return Ok(());
            }
            let mut pty = self.pty.lock();
            pty.writer()
                .write_all(&data)
                .context("write terminal input")?;
            pty.writer().flush().context("flush terminal input")
        }

        pub fn resize(&self, cols: u16, rows: u16) {
            self.pty.lock().on_resize(window_size(cols, rows));
        }
    }

    impl Drop for DaemonTerminal {
        fn drop(&mut self) {
            self.stopped.store(true, Ordering::Release);
            let _ = self.poller.notify();
            // Alacritty's PTY destructor sends SIGHUP and then waits for the
            // child without a timeout. A shell can ignore or defer SIGHUP,
            // so bound its grace period before either join or PTY drop.
            // Hold the lock so the reader cannot reap the child between our
            // exit check and signal delivery.
            terminate_shell(&self.pty.lock());
            if let Some(reader) = self.reader.take() {
                let _ = reader.join();
            }
        }
    }

    /// How one drain of the PTY ended.
    enum Drained {
        /// Nothing more to read right now.
        Idle,
        /// The batch is full; more output may be waiting.
        Full,
        /// The slave side is closed (end of file).
        Closed,
        /// `EIO`: the slave is closed, or not yet attached.
        Io,
        Failed(std::io::Error),
    }

    /// Streams PTY output as ephemeral `TerminalOutput` events (#178). The
    /// thread blocks on the PTY's readiness instead of sleeping between
    /// non-blocking reads, so an idle terminal costs no wakeups. Each wake
    /// reads everything already buffered, up to [`MAX_OUTPUT_BATCH`], into
    /// one event.
    fn read_output(
        mut output: &std::fs::File,
        pty: &Mutex<tty::Pty>,
        stopped: &AtomicBool,
        poller: &Poller,
        events: &EventSink,
    ) {
        let mut buffer = vec![0_u8; READ_CHUNK];
        let mut batch = Vec::with_capacity(READ_CHUNK);
        let mut ready = Events::new();
        loop {
            ready.clear();
            match poller.wait(&mut ready, None) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    let _ =
                        events.send_ephemeral(WireDriverEvent::TerminalError(error.to_string()));
                    return;
                }
            }
            if stopped.load(Ordering::Acquire) {
                return;
            }
            loop {
                let drained = loop {
                    match output.read(&mut buffer) {
                        Ok(0) => break Drained::Closed,
                        Ok(read) => {
                            batch.extend_from_slice(&buffer[..read]);
                            if batch.len() >= MAX_OUTPUT_BATCH {
                                break Drained::Full;
                            }
                        }
                        Err(error) => match error.kind() {
                            std::io::ErrorKind::Interrupted => {}
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                                break Drained::Idle;
                            }
                            _ if error.raw_os_error() == Some(libc::EIO) => break Drained::Io,
                            _ => break Drained::Failed(error),
                        },
                    }
                };
                if !batch.is_empty() {
                    let data = base64::engine::general_purpose::STANDARD.encode(&batch);
                    batch.clear();
                    let _ = events.send_ephemeral(WireDriverEvent::TerminalOutput(
                        TerminalOutputPayload { data },
                    ));
                }
                match drained {
                    Drained::Full => continue,
                    Drained::Idle => break,
                    Drained::Closed => {
                        let _ = events.send_ephemeral(WireDriverEvent::TerminalExited);
                        return;
                    }
                    Drained::Io => {
                        // A PTY master may report EIO briefly before the
                        // freshly spawned child has attached its slave.
                        // Only treat it as EOF after Alacritty's SIGCHLD
                        // channel confirms the child actually exited.
                        if pty.lock().next_child_event().is_some() {
                            let _ = events.send_ephemeral(WireDriverEvent::TerminalExited);
                            return;
                        }
                        // The PTY stays ready while it reports EIO, so the
                        // next wait returns at once; pace it instead.
                        std::thread::sleep(SLAVE_ATTACH_RETRY);
                        break;
                    }
                    Drained::Failed(error) => {
                        let _ = events
                            .send_ephemeral(WireDriverEvent::TerminalError(error.to_string()));
                        return;
                    }
                }
            }
        }
    }

    fn terminate_shell(pty: &tty::Pty) {
        let child_pid = pty.child().id() as libc::pid_t;
        if child_has_exited(child_pid) {
            return;
        }
        unsafe {
            libc::kill(child_pid, libc::SIGHUP);
        }
        let deadline = Instant::now() + SHELL_SHUTDOWN_GRACE;
        while !child_has_exited(child_pid) {
            if Instant::now() >= deadline {
                unsafe {
                    libc::kill(child_pid, libc::SIGKILL);
                }
                break;
            }
            std::thread::sleep(Duration::from_millis(4));
        }
    }

    fn child_has_exited(child_pid: libc::pid_t) -> bool {
        // WNOWAIT leaves reaping to Alacritty's Child, keeping the PID owned
        // until its destructor has finished sending signals and waiting.
        let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                child_pid as libc::id_t,
                &mut status,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            unsafe { status.si_pid() != 0 }
        } else {
            // The output reader may already have reaped a naturally exited
            // shell via next_child_event before shutdown began.
            std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD)
        }
    }

    fn window_size(cols: u16, rows: u16) -> WindowSize {
        WindowSize {
            num_lines: rows.max(MIN_ROWS),
            num_cols: cols.max(MIN_COLUMNS),
            cell_width: CELL_WIDTH,
            cell_height: CELL_HEIGHT,
        }
    }
}

#[cfg(unix)]
pub use platform::DaemonTerminal;

#[cfg(not(unix))]
pub struct DaemonTerminal;

#[cfg(not(unix))]
impl DaemonTerminal {
    pub fn open(_cwd: &Path, _cols: u16, _rows: u16, _events: EventSink) -> anyhow::Result<Self> {
        bail!("daemon terminals are not supported on this platform")
    }

    pub fn write(&self, _data: Vec<u8>) -> anyhow::Result<()> {
        bail!("daemon terminals are not supported on this platform")
    }

    pub fn resize(&self, _cols: u16, _rows: u16) {}
}
