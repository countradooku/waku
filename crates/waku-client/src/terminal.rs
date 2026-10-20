//! The client side of a daemon terminal (#60).
//!
//! [`TerminalLink`] opens a terminal on the daemon and keeps an emulator fed
//! with its output, attaching again with a scrollback replay after every
//! reconnect. [`TerminalStream`] is the ordering rule underneath, separate so
//! it can be tested without a daemon: live output the replay already covers
//! is skipped by its byte offset, and resets that predate the replay are
//! ignored.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use anyhow::Context as _;
use base64::Engine as _;
use crossbeam_channel::{Receiver, Sender, bounded, select};
use parking_lot::Mutex;
use uuid::Uuid;
use waku_protocol::{Command, ResponsePayload, TerminalSnapshot, WireDriverEvent};

use crate::DaemonClient;

/// What an emulator attached to a daemon terminal must do next.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TerminalUpdate {
    /// Clear the screen, scrollback and parser state.
    Reset,
    /// Feed these bytes to the parser.
    Output(Vec<u8>),
    /// The shell exited.
    Exited,
    /// The daemon could not run or reach the terminal.
    Error(String),
}

/// Orders replayed and live output for one attached terminal.
#[derive(Debug, Default)]
pub struct TerminalStream {
    /// Live output below this raw offset is already on screen.
    resume_at: u64,
    generation: u32,
}

impl TerminalStream {
    /// A terminal this client just opened: nothing is on screen yet, so all
    /// of its output applies.
    pub fn opened(&mut self) {
        *self = Self::default();
    }

    /// Begin from an attach snapshot. Returns the updates that rebuild the
    /// screen, starting with a reset unless the emulator is fresh.
    pub fn attach(&mut self, snapshot: TerminalSnapshot, fresh: bool) -> Vec<TerminalUpdate> {
        self.resume_at = snapshot.offset;
        self.generation = snapshot.generation;
        let mut updates = Vec::new();
        if !fresh {
            updates.push(TerminalUpdate::Reset);
        }
        if !snapshot.replay.is_empty() {
            updates.push(TerminalUpdate::Output(snapshot.replay));
        }
        if snapshot.exited {
            updates.push(TerminalUpdate::Exited);
        }
        updates
    }

    /// The update one live terminal event calls for, if any.
    pub fn apply(&mut self, event: WireDriverEvent) -> Option<TerminalUpdate> {
        match event {
            WireDriverEvent::TerminalOutput(chunk) => {
                let bytes = match base64::engine::general_purpose::STANDARD.decode(&chunk.data) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        return Some(TerminalUpdate::Error(format!(
                            "the daemon sent invalid terminal output: {error}"
                        )));
                    }
                };
                let Some(offset) = chunk.offset else {
                    // An older daemon: no replay to overlap with.
                    return Some(TerminalUpdate::Output(bytes));
                };
                let end = offset + bytes.len() as u64;
                if end <= self.resume_at {
                    return None;
                }
                let skip = self.resume_at.saturating_sub(offset) as usize;
                self.resume_at = end;
                Some(TerminalUpdate::Output(bytes[skip..].to_vec()))
            }
            WireDriverEvent::TerminalReset(reset) => {
                // A reset the attach snapshot already reflects would wipe
                // the replay; only a newer one applies.
                if reset.generation < self.generation || reset.offset < self.resume_at {
                    return None;
                }
                self.generation = reset.generation;
                self.resume_at = reset.offset;
                Some(TerminalUpdate::Reset)
            }
            WireDriverEvent::TerminalExited => Some(TerminalUpdate::Exited),
            WireDriverEvent::TerminalError(message) => Some(TerminalUpdate::Error(message)),
            _ => None,
        }
    }
}

/// Receives a linked terminal's updates on the link's thread.
pub trait TerminalSink: Send + 'static {
    fn update(&mut self, update: TerminalUpdate);

    /// When the sink next needs [`Self::tick`] (a synchronized-output
    /// timeout), if ever.
    fn deadline(&self) -> Option<Instant> {
        None
    }

    fn tick(&mut self) {}
}

struct LinkShared {
    terminal_id: Uuid,
    client: Mutex<DaemonClient>,
    size: Mutex<(u16, u16)>,
    closed: AtomicBool,
}

impl LinkShared {
    fn notify(&self, command: Command) {
        let client = self.client.lock().clone();
        let _ = client.notify(self.terminal_id, self.terminal_id, command);
    }

    fn close(&self) {
        self.notify(Command::CloseTerminal);
    }
}

/// A daemon terminal this client opened, kept attached across reconnects.
/// Dropping it closes the terminal.
pub struct TerminalLink {
    shared: Arc<LinkShared>,
    shutdown: Sender<()>,
}

impl TerminalLink {
    /// Open a terminal in `cwd` (a daemon-host path) and stream it into
    /// `sink` from a new thread. `clients` yields the current daemon
    /// connection first and each replacement after it, as
    /// [`crate::DaemonSupervisor::subscribe_clients`] does.
    pub fn open(
        clients: Receiver<DaemonClient>,
        cwd: PathBuf,
        cols: u16,
        rows: u16,
        sink: impl TerminalSink,
    ) -> anyhow::Result<Self> {
        let client = clients
            .recv()
            .context("no Waku daemon connection is available")?;
        let shared = Arc::new(LinkShared {
            terminal_id: Uuid::new_v4(),
            client: Mutex::new(client.clone()),
            size: Mutex::new((cols, rows)),
            closed: AtomicBool::new(false),
        });
        let (shutdown, shutdown_rx) = bounded(1);
        let thread_shared = shared.clone();
        std::thread::Builder::new()
            .name(format!("waku-terminal-{}", shared.terminal_id))
            .spawn(move || run_link(&thread_shared, client, &clients, &shutdown_rx, cwd, sink))
            .context("could not start the terminal link thread")?;
        Ok(Self { shared, shutdown })
    }

    /// The daemon's id for this terminal (its session and runtime id).
    pub fn terminal_id(&self) -> Uuid {
        self.shared.terminal_id
    }

    /// Send input. Fire-and-forget: input typed while the daemon is
    /// unreachable is dropped.
    pub fn write(&self, data: Vec<u8>) {
        self.writer().write(data);
    }

    /// A handle that only sends input, for an emulator's own replies (cursor
    /// reports, colour queries) from inside its parser.
    pub fn writer(&self) -> TerminalWriter {
        TerminalWriter(self.shared.clone())
    }

    pub fn resize(&self, cols: u16, rows: u16) {
        *self.shared.size.lock() = (cols, rows);
        self.shared.notify(Command::ResizeTerminal { cols, rows });
    }

    /// Drop the daemon's retained output; every attached client clears.
    pub fn clear(&self) {
        self.shared.notify(Command::ClearTerminal);
    }

    /// Replace the shell with a fresh one in the same directory.
    pub fn restart(&self) {
        self.shared.notify(Command::RestartTerminal);
    }
}

/// Sends input to a [`TerminalLink`]'s terminal. It does not keep the
/// terminal open.
#[derive(Clone)]
pub struct TerminalWriter(Arc<LinkShared>);

impl TerminalWriter {
    pub fn write(&self, data: Vec<u8>) {
        if !data.is_empty() && !self.0.closed.load(Ordering::Acquire) {
            self.0.notify(Command::WriteTerminal { data });
        }
    }
}

impl Drop for TerminalLink {
    fn drop(&mut self) {
        // Mark first: an open still in flight on the link thread sees the
        // flag after its response and closes the terminal itself.
        self.shared.closed.store(true, Ordering::Release);
        self.shared.close();
        let _ = self.shutdown.try_send(());
    }
}

/// How a connection's attach went.
enum Start {
    Opened,
    Attached(TerminalSnapshot),
}

fn run_link(
    shared: &LinkShared,
    mut client: DaemonClient,
    clients: &Receiver<DaemonClient>,
    shutdown: &Receiver<()>,
    cwd: PathBuf,
    mut sink: impl TerminalSink,
) {
    let terminal_id = shared.terminal_id;
    let mut stream = TerminalStream::default();
    let mut first = true;
    loop {
        // Subscribe before opening or attaching: output produced while the
        // request is in flight is queued here, and the stream sorts out
        // what the replay already holds.
        let events = client.subscribe(terminal_id, terminal_id);
        let (cols, rows) = *shared.size.lock();
        let open = || {
            client
                .request(
                    terminal_id,
                    terminal_id,
                    Command::OpenTerminal {
                        cwd: cwd.clone(),
                        cols,
                        rows,
                    },
                )
                .map(|_| Start::Opened)
        };
        let started = if first {
            open()
        } else {
            match client.request(
                terminal_id,
                terminal_id,
                Command::AttachTerminal {
                    cols: Some(cols),
                    rows: Some(rows),
                    max_replay_bytes: None,
                },
            ) {
                Ok(ResponsePayload::TerminalAttached { terminal }) => Ok(Start::Attached(terminal)),
                Ok(_) => Err(anyhow::anyhow!(
                    "Waku daemon returned an invalid terminal attachment"
                )),
                // The daemon no longer has it (it restarted): open a fresh
                // shell in the same place.
                Err(_) if !client.is_disconnected() => open(),
                Err(error) => Err(error),
            }
        };
        if shared.closed.load(Ordering::Acquire) {
            if matches!(started, Ok(Start::Opened)) {
                shared.close();
            }
            client.unsubscribe(terminal_id, terminal_id);
            return;
        }
        match started {
            Ok(Start::Opened) => {
                stream.opened();
                if !first {
                    sink.update(TerminalUpdate::Reset);
                }
            }
            Ok(Start::Attached(snapshot)) => {
                for update in stream.attach(snapshot, false) {
                    sink.update(update);
                }
            }
            Err(error) if !client.is_disconnected() => {
                sink.update(TerminalUpdate::Error(error.to_string()));
            }
            // The connection dropped mid-request; the next one attaches.
            Err(_) => {}
        }
        first = false;

        loop {
            let timer = match sink.deadline() {
                Some(deadline) => crossbeam_channel::at(deadline),
                None => crossbeam_channel::never(),
            };
            select! {
                recv(shutdown) -> _ => {
                    client.unsubscribe(terminal_id, terminal_id);
                    return;
                }
                recv(events) -> event => match event {
                    Ok(event) => {
                        if let Some(update) = stream.apply(event.event) {
                            sink.update(update);
                        }
                    }
                    // The connection closed.
                    Err(_) => break,
                },
                recv(timer) -> _ => sink.tick(),
            }
        }
        client.unsubscribe(terminal_id, terminal_id);

        // A closed connection says nothing about the terminal: wait for the
        // supervisor's next connection and attach again.
        loop {
            select! {
                recv(shutdown) -> _ => return,
                recv(clients) -> replacement => match replacement {
                    Ok(replacement) if !replacement.same_connection(&client) => {
                        client = replacement;
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => return,
                },
            }
        }
        *shared.client.lock() = client.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use waku_protocol::wire_event::{TerminalOutputPayload, TerminalResetPayload};

    fn output(offset: Option<u64>, bytes: &[u8]) -> WireDriverEvent {
        WireDriverEvent::TerminalOutput(TerminalOutputPayload {
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
            offset,
        })
    }

    fn snapshot(offset: u64, replay: &[u8], generation: u32, exited: bool) -> TerminalSnapshot {
        TerminalSnapshot {
            generation,
            cols: 80,
            rows: 24,
            offset,
            replay: replay.to_vec(),
            truncated: false,
            exited,
            exit_code: None,
        }
    }

    #[test]
    fn live_output_the_replay_covers_is_skipped() {
        let mut stream = TerminalStream::default();
        // Chunks [0, 5) and [5, 11) were queued before the attach response;
        // the replay ends at 11.
        let updates = stream.attach(snapshot(11, b"hello world", 0, false), true);
        assert_eq!(
            updates,
            vec![TerminalUpdate::Output(b"hello world".to_vec())]
        );
        assert_eq!(stream.apply(output(Some(0), b"hello")), None);
        assert_eq!(stream.apply(output(Some(5), b" world")), None);
        assert_eq!(
            stream.apply(output(Some(11), b"!")),
            Some(TerminalUpdate::Output(b"!".to_vec()))
        );
        // A chunk straddling the resume point keeps only its new part.
        let mut stream = TerminalStream::default();
        stream.attach(snapshot(3, b"abc", 0, false), true);
        assert_eq!(
            stream.apply(output(Some(1), b"bcde")),
            Some(TerminalUpdate::Output(b"de".to_vec()))
        );
    }

    #[test]
    fn a_reattach_resets_the_emulator_before_replaying() {
        let mut stream = TerminalStream::default();
        assert_eq!(
            stream.attach(snapshot(4, b"$ ls", 0, true), false),
            vec![
                TerminalUpdate::Reset,
                TerminalUpdate::Output(b"$ ls".to_vec()),
                TerminalUpdate::Exited,
            ]
        );
    }

    #[test]
    fn resets_older_than_the_replay_are_ignored() {
        let mut stream = TerminalStream::default();
        stream.attach(snapshot(20, b"after clear", 1, false), true);
        let stale = WireDriverEvent::TerminalReset(TerminalResetPayload {
            generation: 1,
            offset: 9,
        });
        assert_eq!(stream.apply(stale), None);
        let older_generation = WireDriverEvent::TerminalReset(TerminalResetPayload {
            generation: 0,
            offset: 30,
        });
        assert_eq!(stream.apply(older_generation), None);
        let newer = WireDriverEvent::TerminalReset(TerminalResetPayload {
            generation: 2,
            offset: 25,
        });
        assert_eq!(stream.apply(newer), Some(TerminalUpdate::Reset));
        assert_eq!(
            stream.apply(output(Some(25), b"$ ")),
            Some(TerminalUpdate::Output(b"$ ".to_vec()))
        );
    }

    #[test]
    fn a_freshly_opened_terminal_applies_everything() {
        let mut stream = TerminalStream::default();
        stream.attach(snapshot(100, b"", 0, false), true);
        stream.opened();
        assert_eq!(
            stream.apply(output(Some(0), b"prompt")),
            Some(TerminalUpdate::Output(b"prompt".to_vec()))
        );
        assert_eq!(
            stream.apply(output(None, b"legacy")),
            Some(TerminalUpdate::Output(b"legacy".to_vec()))
        );
        assert_eq!(
            stream.apply(WireDriverEvent::TerminalExited),
            Some(TerminalUpdate::Exited)
        );
        assert_eq!(stream.apply(WireDriverEvent::TurnStarted), None);
    }
}
