//! The client side of a daemon terminal over protocol v8 (#60, #55).
//!
//! [`TerminalLink`] opens a terminal with `terminal.open` and keeps an
//! emulator fed from an interactive `terminal` stream: input goes out as DATA
//! on the subscription's channel, output comes back as DATA under credit,
//! and after every reconnect the link resubscribes from the offset already
//! on screen, so the daemon either continues the stream or replays its
//! retained tail. [`TerminalStream`] is the ordering rule underneath,
//! separate so it can be tested without a daemon.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use crossbeam_channel::{Receiver, Sender, select, unbounded};
use parking_lot::Mutex;
use uuid::Uuid;

use crate::surfaces::DaemonV8;
use crate::surfaces::files::path_root;
use crate::v8::{
    EndReason, ErrorCode, StreamCursor, StreamEvent, StreamItem, SubscribeOptions, Subscription,
    TerminalItem, TerminalOpenParams, TerminalParams, TerminalResizeParams, TerminalSpec, V8Client,
    method, stream,
};

/// A link that cannot reach the daemon tries again after this, doubling up
/// to [`MAX_RETRY`].
const MIN_RETRY: Duration = Duration::from_millis(200);
const MAX_RETRY: Duration = Duration::from_secs(2);
/// Input typed while the link is between connections waits up to this
/// much for the next attach; beyond it, keystrokes are dropped.
const MAX_PENDING_INPUT: usize = 64 * 1024;

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

/// Orders one emulator's view of a `terminal` stream across attaches,
/// resets and resumes.
#[derive(Debug, Default)]
pub struct TerminalStream {
    /// The shell process whose output is on screen.
    generation: Option<Uuid>,
    /// The raw output offset the screen has reached.
    offset: u64,
    /// Replayed bytes still to come; they rebuild what `offset` already
    /// counts.
    replay_left: u64,
    /// An exit the replay must finish drawing before it is reported.
    exit_after_replay: bool,
}

impl TerminalStream {
    /// Where to resume a subscription without a replay, once anything is
    /// on screen.
    pub fn cursor(&self) -> Option<StreamCursor> {
        self.generation.map(|generation| StreamCursor::Offset {
            generation,
            offset: self.offset,
        })
    }

    /// The updates a stream item calls for.
    pub fn item(&mut self, item: TerminalItem) -> Vec<TerminalUpdate> {
        let mut updates = Vec::new();
        match item {
            TerminalItem::Attached {
                generation,
                offset,
                replay,
                exited,
                ..
            } => {
                // A resume at exactly the screen's position continues it;
                // anything else replaces the screen with the replay.
                let continues =
                    replay == 0 && self.generation == Some(generation) && self.offset == offset;
                if self.generation.is_some() && !continues {
                    updates.push(TerminalUpdate::Reset);
                }
                self.generation = Some(generation);
                self.offset = offset;
                self.replay_left = replay;
                self.exit_after_replay = exited && replay > 0;
                if exited && replay == 0 && !continues {
                    updates.push(TerminalUpdate::Exited);
                }
            }
            TerminalItem::Reset {
                generation,
                offset,
                replay,
            } => {
                updates.push(TerminalUpdate::Reset);
                self.generation = Some(generation);
                self.offset = offset;
                self.replay_left = replay;
                self.exit_after_replay = false;
            }
            TerminalItem::Exited { .. } => {
                if self.replay_left > 0 {
                    self.exit_after_replay = true;
                } else {
                    updates.push(TerminalUpdate::Exited);
                }
            }
            TerminalItem::Error { message } => updates.push(TerminalUpdate::Error(message)),
            TerminalItem::Resized { .. } | TerminalItem::Unknown => {}
        }
        updates
    }

    /// The updates DATA bytes call for.
    pub fn data(&mut self, bytes: Vec<u8>) -> Vec<TerminalUpdate> {
        if bytes.is_empty() {
            return Vec::new();
        }
        let length = bytes.len() as u64;
        let replayed = length.min(self.replay_left);
        self.replay_left -= replayed;
        self.offset += length - replayed;
        let mut updates = vec![TerminalUpdate::Output(bytes)];
        if self.replay_left == 0 && std::mem::take(&mut self.exit_after_replay) {
            updates.push(TerminalUpdate::Exited);
        }
        updates
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

enum Control {
    Resize,
    Clear,
    Restart,
    Close,
}

struct LinkShared {
    /// The interactive subscription input goes out on, while attached, and
    /// the input typed while it was not.
    input: Mutex<(Option<Arc<Subscription>>, Vec<u8>)>,
    size: Mutex<(u16, u16)>,
    closed: AtomicBool,
}

/// A daemon terminal this client opened, kept attached across reconnects.
/// Dropping it closes the terminal. Every method returns at once; the
/// daemon round trips happen on the link's thread.
pub struct TerminalLink {
    shared: Arc<LinkShared>,
    control: Sender<Control>,
}

impl TerminalLink {
    /// Open a terminal in `cwd` (a daemon-host path) and stream it into
    /// `sink` from a new thread.
    pub fn open(
        daemon: DaemonV8,
        cwd: PathBuf,
        cols: u16,
        rows: u16,
        sink: impl TerminalSink,
    ) -> anyhow::Result<Self> {
        let shared = Arc::new(LinkShared {
            input: Mutex::new((None, Vec::new())),
            size: Mutex::new((cols, rows)),
            closed: AtomicBool::new(false),
        });
        let (control, control_rx) = unbounded();
        let thread_shared = shared.clone();
        std::thread::Builder::new()
            .name("waku-terminal-link".into())
            .spawn(move || run_link(&thread_shared, &daemon, &control_rx, cwd, sink))
            .context("could not start the terminal link thread")?;
        Ok(Self { shared, control })
    }

    /// Send input. Input typed before the terminal is attached (or while it
    /// reconnects) is sent once it is, up to a bound.
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
        let _ = self.control.send(Control::Resize);
    }

    /// Drop the daemon's retained output; every attached client clears.
    pub fn clear(&self) {
        let _ = self.control.send(Control::Clear);
    }

    /// Replace the shell with a fresh one in the same directory.
    pub fn restart(&self) {
        let _ = self.control.send(Control::Restart);
    }
}

/// Sends input to a [`TerminalLink`]'s terminal. It does not keep the
/// terminal open.
#[derive(Clone)]
pub struct TerminalWriter(Arc<LinkShared>);

impl TerminalWriter {
    pub fn write(&self, data: Vec<u8>) {
        if data.is_empty() || self.0.closed.load(Ordering::Acquire) {
            return;
        }
        let mut input = self.0.input.lock();
        let (subscription, pending) = &mut *input;
        match subscription {
            Some(subscription) => subscription.send_data(data),
            None if pending.len() + data.len() <= MAX_PENDING_INPUT => pending.extend(data),
            None => {}
        }
    }
}

impl Drop for TerminalLink {
    fn drop(&mut self) {
        // Mark first: an open still in flight on the link thread sees the
        // flag after its reply and closes the terminal itself.
        self.shared.closed.store(true, Ordering::Release);
        let _ = self.control.send(Control::Close);
    }
}

/// How one attached connection ended.
enum Detached {
    /// The link was closed.
    Closed,
    /// The connection dropped; reconnect and resume.
    Disconnected,
    /// The daemon no longer has the terminal; open a fresh one.
    Gone,
    /// The daemon ended the stream for good.
    Failed,
}

fn close_terminal(client: &V8Client, terminal_id: Uuid) {
    let _ = client.call::<method::TerminalClose>(TerminalParams { terminal_id });
}

/// Wait out a retry backoff; `false` when the link closed meanwhile.
fn backoff(control: &Receiver<Control>, delay: Duration) -> bool {
    let deadline = Instant::now() + delay;
    loop {
        match control.recv_deadline(deadline) {
            Ok(Control::Close) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                return false;
            }
            Ok(_) => {}
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => return true,
        }
    }
}

fn run_link(
    shared: &LinkShared,
    daemon: &DaemonV8,
    control: &Receiver<Control>,
    cwd: PathBuf,
    mut sink: impl TerminalSink,
) {
    let mut stream = TerminalStream::default();
    let mut terminal_id = None;
    let mut retry = MIN_RETRY;
    let mut reported = false;
    loop {
        if shared.closed.load(Ordering::Acquire) {
            return;
        }
        let client = match daemon.client() {
            Ok(client) => client,
            Err(error) => {
                if !reported {
                    sink.update(TerminalUpdate::Error(format!("{error:#}")));
                    reported = true;
                }
                if !backoff(control, retry) {
                    return;
                }
                retry = (retry * 2).min(MAX_RETRY);
                continue;
            }
        };
        let id = match terminal_id {
            Some(id) => id,
            None => {
                let (cols, rows) = *shared.size.lock();
                match client.call::<method::TerminalOpen>(TerminalOpenParams {
                    root: path_root(&cwd),
                    cols,
                    rows,
                    title: None,
                    op_id: Uuid::new_v4(),
                }) {
                    Ok(opened) => {
                        if shared.closed.load(Ordering::Acquire) {
                            close_terminal(&client, opened.terminal_id);
                            return;
                        }
                        terminal_id = Some(opened.terminal_id);
                        opened.terminal_id
                    }
                    Err(error) => {
                        if !client.is_disconnected() {
                            sink.update(TerminalUpdate::Error(error.to_string()));
                            reported = true;
                        }
                        if !backoff(control, retry) {
                            return;
                        }
                        retry = (retry * 2).min(MAX_RETRY);
                        continue;
                    }
                }
            }
        };
        retry = MIN_RETRY;
        reported = false;
        match attach(shared, &client, id, control, &mut stream, &mut sink) {
            Detached::Closed => {
                close_terminal(&client, id);
                return;
            }
            Detached::Disconnected => {
                if !backoff(control, MIN_RETRY) {
                    return;
                }
            }
            Detached::Gone => terminal_id = None,
            Detached::Failed => {
                // Wait for the view to go away; nothing will come back.
                while let Ok(message) = control.recv() {
                    if matches!(message, Control::Close) {
                        return;
                    }
                }
                return;
            }
        }
    }
}

/// Stream one terminal over one connection until something ends it.
fn attach(
    shared: &LinkShared,
    client: &V8Client,
    terminal_id: Uuid,
    control: &Receiver<Control>,
    stream: &mut TerminalStream,
    sink: &mut impl TerminalSink,
) -> Detached {
    let subscription = Arc::new(client.subscribe::<stream::Terminal>(
        TerminalSpec {
            terminal_id,
            interactive: true,
        },
        SubscribeOptions {
            after: stream.cursor(),
            window: None,
        },
    ));
    {
        let mut input = shared.input.lock();
        let pending = std::mem::take(&mut input.1);
        if !pending.is_empty() {
            subscription.send_data(pending);
        }
        input.0 = Some(subscription.clone());
    }
    let detached = loop {
        let timer = match sink.deadline() {
            Some(deadline) => crossbeam_channel::at(deadline),
            None => crossbeam_channel::never(),
        };
        select! {
            recv(control) -> message => match message {
                Ok(Control::Resize) => {
                    let (cols, rows) = *shared.size.lock();
                    let _ = client.call::<method::TerminalResize>(TerminalResizeParams {
                        terminal_id,
                        cols,
                        rows,
                    });
                }
                Ok(Control::Clear) => {
                    let _ = client.call::<method::TerminalClear>(TerminalParams { terminal_id });
                }
                Ok(Control::Restart) => {
                    let _ = client.call::<method::TerminalRestart>(TerminalParams { terminal_id });
                }
                Ok(Control::Close) | Err(_) => break Detached::Closed,
            },
            recv(subscription.events()) -> event => match event {
                Ok(StreamEvent::Item(StreamItem::Terminal(item))) => {
                    let resize = match &item {
                        TerminalItem::Attached { cols, rows, .. } => {
                            *shared.size.lock() != (*cols, *rows)
                        }
                        _ => false,
                    };
                    for update in stream.item(item) {
                        sink.update(update);
                    }
                    // The terminal kept another size while this view was
                    // away (or another client resized it).
                    if resize {
                        let (cols, rows) = *shared.size.lock();
                        let _ = client.call::<method::TerminalResize>(TerminalResizeParams {
                            terminal_id,
                            cols,
                            rows,
                        });
                    }
                }
                Ok(StreamEvent::Data { bytes, .. }) => {
                    let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
                    for update in stream.data(bytes) {
                        sink.update(update);
                    }
                    subscription.consumed(length);
                }
                Ok(StreamEvent::End(end)) => break match end.reason {
                    EndReason::NotFound => Detached::Gone,
                    EndReason::Error { error } if error.code == ErrorCode::NotFound => {
                        Detached::Gone
                    }
                    EndReason::Error { error } => {
                        sink.update(TerminalUpdate::Error(error.message));
                        Detached::Failed
                    }
                    EndReason::Forbidden => {
                        sink.update(TerminalUpdate::Error(
                            "the daemon does not allow this client to use terminals".into(),
                        ));
                        Detached::Failed
                    }
                    EndReason::Completed => Detached::Failed,
                    _ => Detached::Disconnected,
                },
                Ok(_) => {}
                // The connection closed.
                Err(_) => break Detached::Disconnected,
            },
            recv(timer) -> _ => sink.tick(),
        }
    };
    shared.input.lock().0 = None;
    if !matches!(detached, Detached::Disconnected) {
        subscription.unsubscribe();
    }
    detached
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attached(generation: u128, offset: u64, replay: u64, exited: bool) -> TerminalItem {
        TerminalItem::Attached {
            generation: Uuid::from_u128(generation),
            cols: 80,
            rows: 24,
            offset,
            replay,
            truncated: false,
            exited,
            exit_code: None,
            title: None,
        }
    }

    fn output(bytes: &[u8]) -> TerminalUpdate {
        TerminalUpdate::Output(bytes.to_vec())
    }

    #[test]
    fn a_first_attach_draws_the_replay_on_a_fresh_screen() {
        let mut stream = TerminalStream::default();
        assert_eq!(stream.cursor(), None);
        assert_eq!(stream.item(attached(1, 5, 5, false)), vec![]);
        assert_eq!(stream.data(b"hello".to_vec()), vec![output(b"hello")]);
        assert_eq!(stream.data(b"!".to_vec()), vec![output(b"!")]);
        assert_eq!(
            stream.cursor(),
            Some(StreamCursor::Offset {
                generation: Uuid::from_u128(1),
                offset: 6,
            })
        );
    }

    #[test]
    fn a_resume_at_the_screens_offset_continues_without_a_reset() {
        let mut stream = TerminalStream::default();
        stream.item(attached(1, 0, 0, false));
        stream.data(b"$ ls".to_vec());
        assert_eq!(stream.item(attached(1, 4, 0, false)), vec![]);
        assert_eq!(stream.data(b"\n".to_vec()), vec![output(b"\n")]);
    }

    #[test]
    fn a_replayed_reattach_resets_the_screen_first() {
        let mut stream = TerminalStream::default();
        stream.item(attached(1, 0, 0, false));
        stream.data(b"$ ls".to_vec());
        // The daemon could not continue (the tail was trimmed): it replays.
        assert_eq!(
            stream.item(attached(1, 20, 6, false)),
            vec![TerminalUpdate::Reset]
        );
        assert_eq!(stream.data(b"tail..".to_vec()), vec![output(b"tail..")]);
        assert_eq!(
            stream.cursor(),
            Some(StreamCursor::Offset {
                generation: Uuid::from_u128(1),
                offset: 20,
            })
        );
        // A new shell (the daemon restarted it) replaces the screen too.
        assert_eq!(
            stream.item(attached(2, 0, 0, false)),
            vec![TerminalUpdate::Reset]
        );
    }

    #[test]
    fn an_exit_is_reported_after_its_replay() {
        let mut stream = TerminalStream::default();
        assert_eq!(stream.item(attached(1, 3, 3, true)), vec![]);
        assert_eq!(
            stream.data(b"bye".to_vec()),
            vec![output(b"bye"), TerminalUpdate::Exited]
        );
        let mut stream = TerminalStream::default();
        assert_eq!(
            stream.item(attached(1, 0, 0, true)),
            vec![TerminalUpdate::Exited]
        );
        assert_eq!(
            stream.item(TerminalItem::Exited { code: Some(0) }),
            vec![TerminalUpdate::Exited]
        );
    }

    #[test]
    fn a_reset_clears_and_replays() {
        let mut stream = TerminalStream::default();
        stream.item(attached(1, 0, 0, false));
        stream.data(b"old output".to_vec());
        assert_eq!(
            stream.item(TerminalItem::Reset {
                generation: Uuid::from_u128(3),
                offset: 2,
                replay: 2,
            }),
            vec![TerminalUpdate::Reset]
        );
        assert_eq!(stream.data(b"$ ".to_vec()), vec![output(b"$ ")]);
        assert_eq!(
            stream.item(TerminalItem::Error {
                message: "pty".into()
            }),
            vec![TerminalUpdate::Error("pty".into())]
        );
        assert_eq!(
            stream.item(TerminalItem::Resized { cols: 1, rows: 1 }),
            vec![]
        );
    }
}
