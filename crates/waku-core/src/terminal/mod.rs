//! Daemon-owned pseudoterminals (#60).
//!
//! The daemon owns the shell, cwd, and PTY on every platform. Clients only
//! render the byte stream and send input and resize controls, so the desktop,
//! a browser or a phone can drive a terminal on another machine without
//! interpreting any daemon-side path.
//!
//! The PTY is `alacritty_terminal`'s: a Unix PTY, or ConPTY on Windows. It is
//! already the desktop's PTY layer and a dependency of this crate, it covers
//! Windows, and its readiness API (`EventedReadWrite` over `polling`) lets the
//! output thread block until there is output or the child exits instead of
//! polling (#178), on both platforms. `portable-pty` would add a dependency
//! tree for the same ConPTY coverage and a blocking reader thread per
//! terminal.
//!
//! Output is broadcast live as ephemeral `terminalOutput` events, each with
//! its raw byte offset, and a filtered copy is retained ([`history`]) so a
//! client attaching later — a second client, or the same one after a
//! reconnect — replays a bounded tail first. The output thread appends to the
//! history and broadcasts under one lock, which is what makes the offset in an
//! attach snapshot exact: every live chunk below it is in the replay, and
//! every chunk at or above it is not.

mod history;

use std::io::{ErrorKind, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use alacritty_terminal::event::{OnResize as _, WindowSize};
use alacritty_terminal::tty::{self, ChildEvent, EventedPty as _, EventedReadWrite as _, Shell};
use anyhow::{Context as _, bail};
use base64::Engine as _;
use parking_lot::Mutex;
use polling::{Event, Events, PollMode, Poller};
use waku_protocol::wire_event::{TerminalOutputPayload, TerminalResetPayload};
use waku_protocol::{TERMINAL_REPLAY_BYTES, TerminalSnapshot};

use self::history::{QueryFilter, TerminalHistory};
use crate::{EventSink, WireDriverEvent};

const CELL_WIDTH: u16 = 8;
const CELL_HEIGHT: u16 = 16;
const MIN_COLUMNS: u16 = 2;
const MIN_ROWS: u16 = 1;
/// One `read` from the PTY.
const READ_CHUNK: usize = 32 * 1024;
/// Output already waiting in the PTY is coalesced into one event, up to
/// this many bytes.
const MAX_OUTPUT_BATCH: usize = 64 * 1024;
/// Pause between attempts while the PTY's input buffer is full, with the
/// PTY unlocked so the output thread keeps draining.
const WRITE_RETRY: Duration = Duration::from_millis(2);
/// A write that cannot make progress for this long fails.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a closed PTY waits for its child's exit status.
const EXIT_STATUS_WAIT: Duration = Duration::from_secs(1);
/// Retry interval while a Unix PTY reports `EIO` before the child has
/// attached its slave. That window is brief and only at start-up; an open
/// terminal otherwise never wakes without output.
#[cfg(unix)]
const SLAVE_ATTACH_RETRY: Duration = Duration::from_millis(4);
#[cfg(unix)]
const SHELL_SHUTDOWN_GRACE: Duration = Duration::from_millis(250);

/// The most terminals one daemon keeps open. Each retains up to
/// [`waku_protocol::TERMINAL_RETAINED_BYTES`] of output.
pub const MAX_TERMINALS: usize = 64;

/// One daemon terminal: its retained output and the shell process, which a
/// restart replaces.
pub struct DaemonTerminal {
    shared: Arc<Shared>,
    process: Mutex<Option<TerminalProcess>>,
    cwd: PathBuf,
    shell: Shell,
}

/// State the output thread and requests share.
struct Shared {
    state: Mutex<TerminalState>,
    events: EventSink,
}

struct TerminalState {
    history: TerminalHistory,
    filter: QueryFilter,
    /// Raw bytes the PTY has produced since the terminal opened.
    offset: u64,
    generation: u32,
    cols: u16,
    rows: u16,
    exited: bool,
    exit_code: Option<i32>,
}

impl DaemonTerminal {
    pub fn open(cwd: &Path, cols: u16, rows: u16, events: EventSink) -> anyhow::Result<Self> {
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
        cwd: &Path,
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
        let (cols, rows) = clamp_size(cols, rows);
        let shared = Arc::new(Shared {
            state: Mutex::new(TerminalState {
                history: TerminalHistory::default(),
                filter: QueryFilter::default(),
                offset: 0,
                generation: 0,
                cols,
                rows,
                exited: false,
                exit_code: None,
            }),
            events,
        });
        let process = TerminalProcess::spawn(cwd, &shell, cols, rows, shared.clone(), 0)?;
        Ok(Self {
            shared,
            process: Mutex::new(Some(process)),
            cwd: cwd.to_owned(),
            shell,
        })
    }

    pub fn write(&self, data: Vec<u8>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let pty = self
            .process
            .lock()
            .as_ref()
            .map(|process| process.pty.clone())
            .context("the terminal's shell is not running")?;
        write_all(&pty, &data)
    }

    pub fn resize(&self, cols: u16, rows: u16) {
        let (cols, rows) = clamp_size(cols, rows);
        {
            let mut state = self.shared.state.lock();
            state.cols = cols;
            state.rows = rows;
        }
        if let Some(process) = self.process.lock().as_ref() {
            process.pty.lock().on_resize(window_size(cols, rows));
        }
    }

    /// The terminal's state and a replay of at most `max_replay_bytes` of its
    /// retained output (capped at [`TERMINAL_REPLAY_BYTES`]).
    pub fn snapshot(&self, max_replay_bytes: Option<u32>) -> TerminalSnapshot {
        let max_replay_bytes = max_replay_bytes
            .map_or(TERMINAL_REPLAY_BYTES, |bytes| bytes as usize)
            .min(TERMINAL_REPLAY_BYTES);
        let state = self.shared.state.lock();
        let pending = state.filter.pending();
        let (mut replay, mut truncated) = state
            .history
            .tail(max_replay_bytes.saturating_sub(pending.len()));
        if replay.len() + pending.len() <= max_replay_bytes {
            // A sequence still being decided continues in live output; the
            // replay carries its start so the client parses it whole.
            replay.extend_from_slice(pending);
        } else {
            truncated = true;
        }
        TerminalSnapshot {
            generation: state.generation,
            cols: state.cols,
            rows: state.rows,
            offset: state.offset,
            replay,
            truncated,
            exited: state.exited,
            exit_code: state.exit_code,
        }
    }

    /// Drop the retained output and tell attached clients to clear.
    pub fn clear(&self) {
        let mut state = self.shared.state.lock();
        state.history.clear();
        let reset = TerminalResetPayload {
            generation: state.generation,
            offset: state.offset,
        };
        let _ = self
            .shared
            .events
            .send_ephemeral(WireDriverEvent::TerminalReset(reset));
    }

    /// Replace the shell with a fresh one in the same directory and size.
    pub fn restart(&self) -> anyhow::Result<TerminalSnapshot> {
        let mut process = self.process.lock();
        // Stops the old output thread before the history resets, so no old
        // output lands after the reset event.
        drop(process.take());
        let snapshot = {
            let mut state = self.shared.state.lock();
            state.generation = state.generation.wrapping_add(1);
            state.history.clear();
            state.filter.reset();
            state.exited = false;
            state.exit_code = None;
            let reset = TerminalResetPayload {
                generation: state.generation,
                offset: state.offset,
            };
            let _ = self
                .shared
                .events
                .send_ephemeral(WireDriverEvent::TerminalReset(reset));
            // Taken before the new shell starts: everything it writes is at
            // or above this offset and reaches the caller live.
            TerminalSnapshot {
                generation: state.generation,
                cols: state.cols,
                rows: state.rows,
                offset: state.offset,
                replay: Vec::new(),
                truncated: false,
                exited: false,
                exit_code: None,
            }
        };
        *process = Some(TerminalProcess::spawn(
            &self.cwd,
            &self.shell,
            snapshot.cols,
            snapshot.rows,
            self.shared.clone(),
            snapshot.generation,
        )?);
        Ok(snapshot)
    }
}

/// A running shell and the thread streaming its output.
struct TerminalProcess {
    pty: Arc<Mutex<tty::Pty>>,
    stopped: Arc<AtomicBool>,
    /// The output thread blocks on this; dropping the process wakes it.
    poller: Arc<Poller>,
    reader: Option<JoinHandle<()>>,
}

impl TerminalProcess {
    fn spawn(
        cwd: &Path,
        shell: &Shell,
        cols: u16,
        rows: u16,
        shared: Arc<Shared>,
        generation: u32,
    ) -> anyhow::Result<Self> {
        let mut options = tty::Options {
            shell: Some(shell.clone()),
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

        let mut pty = tty::new(&options, window_size(cols, rows), 0)
            .with_context(|| format!("spawn terminal in {}", cwd.display()))?;
        let poller = Arc::new(Poller::new().context("create terminal output poller")?);
        // SAFETY: the PTY outlives its registration: the output thread
        // deregisters it before exiting, and `Drop` joins that thread before
        // the PTY is dropped.
        unsafe { pty.register(&poller, Event::readable(0), PollMode::Level) }
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
                    read_output(&pty, &stopped, &poller, &shared, generation);
                    let _ = pty.lock().deregister(&poller);
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
}

impl Drop for TerminalProcess {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        let _ = self.poller.notify();
        // Alacritty's Unix PTY destructor sends SIGHUP and then waits for
        // the child without a timeout. A shell can ignore or defer SIGHUP,
        // so bound its grace period before either join or PTY drop. Hold the
        // lock so the reader cannot reap the child between our exit check
        // and signal delivery.
        #[cfg(unix)]
        terminate_shell(&self.pty.lock());
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        // On Windows dropping the PTY closes the pseudoconsole, which ends
        // the processes attached to it.
    }
}

/// Write all of `data`, retrying while the PTY's input buffer is full. The
/// PTY is unlocked between attempts so its output keeps draining: a shell
/// echoing input it cannot write out would otherwise never read more.
fn write_all(pty: &Mutex<tty::Pty>, mut data: &[u8]) -> anyhow::Result<()> {
    let mut stalled_since = None;
    while !data.is_empty() {
        let written = {
            let mut pty = pty.lock();
            match pty.writer().write(data) {
                Ok(written) => {
                    let _ = pty.writer().flush();
                    Ok(written)
                }
                Err(error) => Err(error),
            }
        };
        match written {
            Ok(0) => bail!("the terminal closed its input"),
            Ok(written) => {
                data = &data[written..];
                stalled_since = None;
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                let since = *stalled_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= WRITE_TIMEOUT {
                    bail!("the terminal stopped reading input");
                }
                std::thread::sleep(WRITE_RETRY);
            }
            Err(error) => return Err(error).context("write terminal input"),
        }
    }
    Ok(())
}

/// How one drain of the PTY ended.
enum Drained {
    /// Nothing more to read right now.
    Idle,
    /// The batch is full; more output may be waiting.
    Full,
    /// The slave side is closed (end of file).
    Closed,
    /// Unix `EIO`: the slave is closed, or not yet attached.
    #[cfg(unix)]
    Io,
    Failed(std::io::Error),
}

/// Read everything the PTY has buffered, up to [`MAX_OUTPUT_BATCH`].
fn drain(pty: &Mutex<tty::Pty>, buffer: &mut [u8], batch: &mut Vec<u8>) -> Drained {
    let mut pty = pty.lock();
    loop {
        match pty.reader().read(buffer) {
            Ok(0) => return Drained::Closed,
            Ok(read) => {
                batch.extend_from_slice(&buffer[..read]);
                if batch.len() >= MAX_OUTPUT_BATCH {
                    return Drained::Full;
                }
            }
            Err(error) => match error.kind() {
                ErrorKind::Interrupted => {}
                ErrorKind::WouldBlock | ErrorKind::TimedOut => return Drained::Idle,
                #[cfg(unix)]
                _ if error.raw_os_error() == Some(libc::EIO) => return Drained::Io,
                _ => return Drained::Failed(error),
            },
        }
    }
}

/// Streams PTY output (#178): the thread blocks on the PTY's readiness, so an
/// idle terminal costs no wakeups. Each wake reads everything already
/// buffered into one event, retains a filtered copy, and then checks whether
/// the shell exited.
fn read_output(
    pty: &Mutex<tty::Pty>,
    stopped: &AtomicBool,
    poller: &Poller,
    shared: &Shared,
    generation: u32,
) {
    let mut buffer = vec![0_u8; READ_CHUNK];
    let mut batch = Vec::with_capacity(READ_CHUNK);
    let mut ready = Events::new();
    loop {
        ready.clear();
        match poller.wait(&mut ready, None) {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => {
                shared.report_error(error.to_string());
                return;
            }
        }
        if stopped.load(Ordering::Acquire) {
            return;
        }
        loop {
            let drained = drain(pty, &mut buffer, &mut batch);
            shared.publish(&mut batch, generation);
            match drained {
                Drained::Full => continue,
                Drained::Idle => break,
                Drained::Closed => {
                    let status = child_exit(pty);
                    shared.exited(generation, status);
                    return;
                }
                #[cfg(unix)]
                Drained::Io => {
                    // A PTY master may report EIO briefly before the freshly
                    // spawned child has attached its slave. Only treat it as
                    // EOF once the child has actually exited.
                    let child_event = pty.lock().next_child_event();
                    if let Some(ChildEvent::Exited(status)) = child_event {
                        shared.exited(generation, status);
                        return;
                    }
                    // The PTY stays ready while it reports EIO, so the next
                    // wait returns at once; pace it instead.
                    std::thread::sleep(SLAVE_ATTACH_RETRY);
                    break;
                }
                Drained::Failed(error) => {
                    shared.report_error(error.to_string());
                    return;
                }
            }
        }
        let child_event = pty.lock().next_child_event();
        if let Some(ChildEvent::Exited(status)) = child_event {
            // Output the shell wrote just before exiting is still buffered.
            loop {
                let drained = drain(pty, &mut buffer, &mut batch);
                shared.publish(&mut batch, generation);
                if !matches!(drained, Drained::Full) {
                    break;
                }
            }
            shared.exited(generation, status);
            return;
        }
    }
}

/// The exit status of a child whose end of the PTY just closed. The exit
/// notification can trail the end of file by a moment, so wait for it
/// briefly; a child that keeps running without its terminal reports none.
fn child_exit(pty: &Mutex<tty::Pty>) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + EXIT_STATUS_WAIT;
    loop {
        let child_event = pty.lock().next_child_event();
        if let Some(ChildEvent::Exited(status)) = child_event {
            return status;
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(4));
    }
}

impl Shared {
    /// Retain and broadcast one batch of output. Both happen under the state
    /// lock, so an attach snapshot's offset splits live output exactly.
    fn publish(&self, batch: &mut Vec<u8>, generation: u32) {
        if batch.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        if state.generation != generation {
            batch.clear();
            return;
        }
        let offset = state.offset;
        state.offset += batch.len() as u64;
        let mut retained = Vec::with_capacity(batch.len());
        state.filter.feed(batch, &mut retained);
        state.history.push(&retained);
        let data = base64::engine::general_purpose::STANDARD.encode(&*batch);
        batch.clear();
        let _ =
            self.events
                .send_ephemeral(WireDriverEvent::TerminalOutput(TerminalOutputPayload {
                    data,
                    offset: Some(offset),
                }));
    }

    fn exited(&self, generation: u32, status: Option<std::process::ExitStatus>) {
        let mut state = self.state.lock();
        if state.generation != generation {
            return;
        }
        state.exited = true;
        state.exit_code = status.and_then(|status| status.code());
        let _ = self.events.send_ephemeral(WireDriverEvent::TerminalExited);
    }

    fn report_error(&self, message: String) {
        let _ = self
            .events
            .send_ephemeral(WireDriverEvent::TerminalError(message));
    }
}

#[cfg(unix)]
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

#[cfg(unix)]
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

fn clamp_size(cols: u16, rows: u16) -> (u16, u16) {
    (cols.max(MIN_COLUMNS), rows.max(MIN_ROWS))
}

fn window_size(cols: u16, rows: u16) -> WindowSize {
    WindowSize {
        num_lines: rows,
        num_cols: cols,
        cell_width: CELL_WIDTH,
        cell_height: CELL_HEIGHT,
    }
}

#[cfg(all(test, unix))]
mod tests;
