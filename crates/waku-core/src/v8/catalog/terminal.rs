//! Daemon terminals over v8: `terminal.*`, and the `terminal` and
//! `terminalList` streams (the PTY host of #60).
//!
//! The PTY, its retained history and its offsets are the backend's
//! [`DaemonTerminal`](crate::terminal::DaemonTerminal), opened through the
//! same `OpenTerminal` command as the legacy protocol. A v8 terminal routes
//! that terminal's events to a broadcast of its own instead of the legacy
//! hub, and each `terminal` subscription turns them into DATA under the
//! client's credit:
//!
//! - output below the attach snapshot's offset is part of the replay and is
//!   skipped;
//! - output arriving while the client has no credit waits, up to
//!   [`PENDING_BYTES`]; past that the subscriber is behind, and when credit
//!   returns it gets `reset` with a fresh tail instead;
//! - clear and restart arrive as `reset` from the terminal itself.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine as _;
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;
use waku_protocol::v8::{
    Ack, EndReason, Error, StreamCursor, TerminalGeneration, TerminalInfo, TerminalItem,
    TerminalListItem, TerminalListSpec, TerminalOpenParams, TerminalOpened, TerminalParams,
    TerminalResizeParams, TerminalSpec,
};
use waku_protocol::{TERMINAL_REPLAY_BYTES, TerminalSnapshot};

use super::{Domain, backend_error, display_path, not_found, now_ms, unexpected};
use crate::protocol::{Command, ResponsePayload, WireDriverEvent};
use crate::server::EventSink;
use crate::v8::V8Context;
use crate::v8::connection::{DataChunk, Ended, StreamSink};
use crate::v8::methods::root_path;

/// Output a subscriber without credit holds before it is reset instead.
const PENDING_BYTES: usize = TERMINAL_REPLAY_BYTES;
/// Events a terminal queues for a slow subscription before it lags.
const EVENT_QUEUE: usize = 1024;

/// Input of an interactive `terminal` subscription.
pub(crate) type TerminalInput = DataChunk;

#[derive(Clone, Debug)]
enum TerminalEvent {
    Output { offset: u64, data: Arc<[u8]> },
    Reset { generation: u32, offset: u64 },
    Exited,
    Error(String),
    Resized { cols: u16, rows: u16 },
    Closed,
}

#[derive(Clone, Debug)]
enum ListEvent {
    Upserted(TerminalInfo),
    Removed(Uuid),
}

/// The terminals opened over v8.
pub(crate) struct TerminalHub {
    terminals: Mutex<HashMap<Uuid, Arc<V8Terminal>>>,
    list: broadcast::Sender<ListEvent>,
}

impl Default for TerminalHub {
    fn default() -> Self {
        Self {
            terminals: Mutex::new(HashMap::new()),
            list: broadcast::channel(256).0,
        }
    }
}

impl TerminalHub {
    fn get(&self, id: Uuid) -> Result<Arc<V8Terminal>, Error> {
        self.terminals
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| not_found("terminal", id.to_string()))
    }

    fn upserted(&self, terminal: &V8Terminal) {
        let _ = self.list.send(ListEvent::Upserted(terminal.info()));
    }

    fn infos(&self, cwd: Option<&str>) -> Vec<TerminalInfo> {
        let mut infos = self
            .terminals
            .lock()
            .values()
            .map(|terminal| terminal.info())
            .filter(|info| cwd.is_none_or(|cwd| info.cwd == cwd))
            .collect::<Vec<_>>();
        infos.sort_by_key(|info| (info.created_at, info.terminal_id));
        infos
    }
}

struct V8Terminal {
    id: Uuid,
    cwd: PathBuf,
    title: Option<String>,
    created_at: i64,
    events: broadcast::Sender<TerminalEvent>,
    state: Mutex<TerminalState>,
}

struct TerminalState {
    cols: u16,
    rows: u16,
    /// The backend's generation counter and the id clients see for it.
    generation: (u32, Uuid),
    exited: bool,
}

impl V8Terminal {
    fn generation_id(&self, generation: u32) -> Uuid {
        let mut state = self.state.lock();
        if generation == state.generation.0 {
            return state.generation.1;
        }
        if generation > state.generation.0 {
            state.generation = (generation, Uuid::new_v4());
            return state.generation.1;
        }
        // An older shell's: matches no cursor.
        Uuid::new_v4()
    }

    fn info(&self) -> TerminalInfo {
        let state = self.state.lock();
        TerminalInfo {
            terminal_id: self.id,
            cwd: display_path(&self.cwd),
            title: self.title.clone(),
            cols: state.cols,
            rows: state.rows,
            generation: state.generation.1,
            exited: state.exited,
            created_at: self.created_at,
        }
    }

    /// An event of the backend terminal.
    fn on_event(&self, event: WireDriverEvent) {
        let event = match event {
            WireDriverEvent::TerminalOutput(output) => {
                let Ok(data) = base64::engine::general_purpose::STANDARD.decode(output.data) else {
                    return;
                };
                TerminalEvent::Output {
                    offset: output.offset.unwrap_or_default(),
                    data: data.into(),
                }
            }
            WireDriverEvent::TerminalReset(reset) => {
                {
                    let mut state = self.state.lock();
                    if reset.generation != state.generation.0 {
                        state.exited = false;
                    }
                }
                self.generation_id(reset.generation);
                TerminalEvent::Reset {
                    generation: reset.generation,
                    offset: reset.offset,
                }
            }
            WireDriverEvent::TerminalExited => {
                self.state.lock().exited = true;
                TerminalEvent::Exited
            }
            WireDriverEvent::TerminalError(message) => TerminalEvent::Error(message),
            _ => return,
        };
        let _ = self.events.send(event);
    }
}

/// A sink for backend calls whose events do not matter (they reach the
/// terminal through the sink it was opened with).
fn no_events() -> EventSink {
    EventSink::listener(|_| {})
}

fn terminal_call(
    context: &V8Context,
    id: Uuid,
    command: Command,
) -> Result<ResponsePayload, Error> {
    context
        .backend
        .call_as(id, id, command, no_events())
        .map_err(|error| backend_error(error, Domain::General))
}

fn snapshot(context: &V8Context, id: Uuid, max_replay: usize) -> Result<TerminalSnapshot, Error> {
    match terminal_call(
        context,
        id,
        Command::AttachTerminal {
            cols: None,
            rows: None,
            max_replay_bytes: Some(u32::try_from(max_replay).unwrap_or(u32::MAX)),
        },
    )? {
        ResponsePayload::TerminalAttached { terminal } => Ok(terminal),
        other => Err(unexpected(&other)),
    }
}

pub(super) fn open(
    context: &V8Context,
    params: TerminalOpenParams,
) -> Result<TerminalOpened, Error> {
    let cwd = root_path(context, &params.root)?;
    let id = Uuid::new_v4();
    let terminal = Arc::new(V8Terminal {
        id,
        cwd: cwd.clone(),
        title: params.title,
        created_at: now_ms(),
        events: broadcast::channel(EVENT_QUEUE).0,
        state: Mutex::new(TerminalState {
            cols: params.cols,
            rows: params.rows,
            generation: (0, Uuid::new_v4()),
            exited: false,
        }),
    });
    let listener = terminal.clone();
    context
        .backend
        .call_as(
            id,
            id,
            Command::OpenTerminal {
                cwd,
                cols: params.cols,
                rows: params.rows,
            },
            EventSink::listener(move |event| listener.on_event(event)),
        )
        .map_err(|error| backend_error(error, Domain::General))?;
    let hub = &context.catalog.terminals;
    hub.terminals.lock().insert(id, terminal.clone());
    hub.upserted(&terminal);
    Ok(TerminalOpened {
        terminal_id: id,
        generation: terminal.generation_id(0),
    })
}

pub(super) fn resize(context: &V8Context, params: TerminalResizeParams) -> Result<Ack, Error> {
    let hub = &context.catalog.terminals;
    let terminal = hub.get(params.terminal_id)?;
    terminal_call(
        context,
        terminal.id,
        Command::ResizeTerminal {
            cols: params.cols,
            rows: params.rows,
        },
    )?;
    // The PTY clamps tiny sizes; report what it has.
    let size = snapshot(context, terminal.id, 0)?;
    {
        let mut state = terminal.state.lock();
        state.cols = size.cols;
        state.rows = size.rows;
    }
    let _ = terminal.events.send(TerminalEvent::Resized {
        cols: size.cols,
        rows: size.rows,
    });
    hub.upserted(&terminal);
    Ok(Ack {})
}

pub(super) fn clear(context: &V8Context, params: TerminalParams) -> Result<Ack, Error> {
    let terminal = context.catalog.terminals.get(params.terminal_id)?;
    terminal_call(context, terminal.id, Command::ClearTerminal)?;
    Ok(Ack {})
}

pub(super) fn restart(
    context: &V8Context,
    params: TerminalParams,
) -> Result<TerminalGeneration, Error> {
    let hub = &context.catalog.terminals;
    let terminal = hub.get(params.terminal_id)?;
    let generation = match terminal_call(context, terminal.id, Command::RestartTerminal)? {
        ResponsePayload::TerminalAttached { terminal: snapshot } => snapshot.generation,
        other => return Err(unexpected(&other)),
    };
    terminal.state.lock().exited = false;
    let generation = terminal.generation_id(generation);
    hub.upserted(&terminal);
    Ok(TerminalGeneration { generation })
}

pub(super) fn close(context: &V8Context, params: TerminalParams) -> Result<Ack, Error> {
    let hub = &context.catalog.terminals;
    let terminal = hub.get(params.terminal_id)?;
    let closed = terminal_call(context, terminal.id, Command::CloseTerminal);
    hub.terminals.lock().remove(&terminal.id);
    let _ = terminal.events.send(TerminalEvent::Closed);
    let _ = hub.list.send(ListEvent::Removed(terminal.id));
    closed.map(|_| Ack {})
}

/// Send `bytes` as DATA, waiting for credit as needed.
async fn send_bytes(sink: &StreamSink, mut bytes: &[u8]) -> Result<(), Ended> {
    while !bytes.is_empty() {
        let credit = usize::try_from(sink.credit().await?).unwrap_or(usize::MAX);
        let length = bytes.len().min(sink.max_data_chunk()).min(credit);
        sink.data(false, bytes[..length].to_vec())?;
        bytes = &bytes[length..];
    }
    Ok(())
}

async fn blocking_snapshot(
    context: &Arc<V8Context>,
    id: Uuid,
    max_replay: usize,
) -> Option<Result<TerminalSnapshot, Error>> {
    let context = context.clone();
    tokio::task::spawn_blocking(move || snapshot(&context, id, max_replay))
        .await
        .ok()
}

/// Writes an interactive subscription's input in order, off the stream's
/// task, until the input closes or the stream ends.
fn spawn_writer(
    context: Arc<V8Context>,
    id: Uuid,
    mut input: mpsc::Receiver<TerminalInput>,
) -> tokio::task::AbortHandle {
    tokio::spawn(async move {
        while let Some(chunk) = input.recv().await {
            if chunk.body.is_empty() {
                continue;
            }
            let context = context.clone();
            let written = tokio::task::spawn_blocking(move || {
                terminal_call(&context, id, Command::WriteTerminal { data: chunk.body })
            })
            .await;
            if !matches!(written, Ok(Ok(_))) {
                return;
            }
        }
    })
    .abort_handle()
}

struct AbortOnDrop(Option<tokio::task::AbortHandle>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// `terminal`: `attached` (with the retained tail as DATA unless `after` is
/// current), then live output under credit, `resized`, `reset`, `exited`
/// and `error`; `end{completed}` when the terminal closes.
pub(super) async fn terminal_stream(
    sink: StreamSink,
    context: Arc<V8Context>,
    spec: TerminalSpec,
    after: Option<StreamCursor>,
    input: Option<mpsc::Receiver<TerminalInput>>,
) {
    let terminal = match context.catalog.terminals.get(spec.terminal_id) {
        Ok(terminal) => terminal,
        Err(_) => {
            sink.end(EndReason::NotFound);
            return;
        }
    };
    let _writer = AbortOnDrop(input.map(|input| spawn_writer(context.clone(), terminal.id, input)));
    // Listen before the snapshot, so no output falls between them.
    let mut events = terminal.events.subscribe();
    let window = usize::try_from(sink.available_credit()).unwrap_or(usize::MAX);
    let Some(snap) =
        blocking_snapshot(&context, terminal.id, window.min(TERMINAL_REPLAY_BYTES)).await
    else {
        return;
    };
    let snap = match snap {
        Ok(snap) => snap,
        Err(error) => {
            sink.end(EndReason::Error { error });
            return;
        }
    };
    let generation = terminal.generation_id(snap.generation);
    let current = StreamCursor::Offset {
        generation,
        offset: snap.offset,
    };
    let resumed = after.as_ref() == Some(&current);
    let replay = if resumed { Vec::new() } else { snap.replay };
    let attached = TerminalItem::Attached {
        generation,
        cols: snap.cols,
        rows: snap.rows,
        offset: snap.offset,
        replay: replay.len() as u64,
        truncated: !resumed && snap.truncated,
        exited: snap.exited,
        exit_code: snap.exit_code,
        title: terminal.title.clone(),
    };
    if sink.push(&attached, Some(current)).is_err() || send_bytes(&sink, &replay).await.is_err() {
        return;
    }
    let mut position = snap.offset;
    let mut pending: VecDeque<Vec<u8>> = VecDeque::new();
    let mut pending_bytes = 0;
    let mut behind = false;
    loop {
        let waiting = behind || !pending.is_empty();
        tokio::select! {
            event = events.recv() => {
                let item = match event {
                    Ok(TerminalEvent::Output { offset, data }) => {
                        let end = offset + data.len() as u64;
                        if end <= position {
                            continue;
                        }
                        if offset > position {
                            // Output this subscriber never saw.
                            behind = true;
                        }
                        let skip = usize::try_from(position.saturating_sub(offset)).unwrap_or(0);
                        position = end;
                        if behind {
                            continue;
                        }
                        pending_bytes += data.len() - skip;
                        pending.push_back(data[skip..].to_vec());
                        if pending_bytes > PENDING_BYTES {
                            pending.clear();
                            pending_bytes = 0;
                            behind = true;
                        }
                        continue;
                    }
                    Ok(TerminalEvent::Reset { generation, offset }) => {
                        pending.clear();
                        pending_bytes = 0;
                        behind = false;
                        position = offset;
                        TerminalItem::Reset {
                            generation: terminal.generation_id(generation),
                            offset,
                            replay: 0,
                        }
                    }
                    Ok(TerminalEvent::Exited) => {
                        // Output before the exit goes out first.
                        let code = match blocking_snapshot(&context, terminal.id, 0).await {
                            Some(Ok(snap)) => snap.exit_code,
                            _ => None,
                        };
                        if flush(&sink, &mut pending, &mut pending_bytes).await.is_err() {
                            return;
                        }
                        TerminalItem::Exited { code }
                    }
                    Ok(TerminalEvent::Error(message)) => TerminalItem::Error { message },
                    Ok(TerminalEvent::Resized { cols, rows }) => TerminalItem::Resized { cols, rows },
                    Ok(TerminalEvent::Closed) | Err(broadcast::error::RecvError::Closed) => {
                        if flush(&sink, &mut pending, &mut pending_bytes).await.is_ok() {
                            sink.end(EndReason::Completed);
                        }
                        return;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        pending.clear();
                        pending_bytes = 0;
                        behind = true;
                        continue;
                    }
                };
                if sink.push_after_data(&item).is_err() {
                    return;
                }
            }
            credit = sink.credit(), if waiting => {
                let Ok(credit) = credit else {
                    return;
                };
                let credit = usize::try_from(credit).unwrap_or(usize::MAX);
                if behind {
                    let snap = match blocking_snapshot(&context, terminal.id, credit.min(TERMINAL_REPLAY_BYTES)).await {
                        Some(Ok(snap)) => snap,
                        Some(Err(error)) => {
                            sink.end(EndReason::Error { error });
                            return;
                        }
                        None => return,
                    };
                    behind = false;
                    position = snap.offset;
                    let reset = TerminalItem::Reset {
                        generation: terminal.generation_id(snap.generation),
                        offset: snap.offset,
                        replay: snap.replay.len() as u64,
                    };
                    if sink.push_after_data(&reset).is_err() || send_bytes(&sink, &snap.replay).await.is_err() {
                        return;
                    }
                    continue;
                }
                if send_pending(&sink, &mut pending, &mut pending_bytes, credit).is_err() {
                    return;
                }
            }
        }
    }
}

/// Send queued output up to `credit` bytes.
fn send_pending(
    sink: &StreamSink,
    pending: &mut VecDeque<Vec<u8>>,
    pending_bytes: &mut usize,
    mut credit: usize,
) -> Result<(), Ended> {
    while credit > 0
        && let Some(front) = pending.front_mut()
    {
        let length = front.len().min(sink.max_data_chunk()).min(credit);
        let body = if length == front.len() {
            pending.pop_front().expect("front exists")
        } else {
            front.drain(..length).collect()
        };
        credit -= length;
        *pending_bytes -= length;
        sink.data(false, body)?;
    }
    Ok(())
}

/// Send everything queued, waiting for credit.
async fn flush(
    sink: &StreamSink,
    pending: &mut VecDeque<Vec<u8>>,
    pending_bytes: &mut usize,
) -> Result<(), Ended> {
    while !pending.is_empty() {
        let credit = usize::try_from(sink.credit().await?).unwrap_or(usize::MAX);
        send_pending(sink, pending, pending_bytes, credit)?;
    }
    Ok(())
}

/// `terminalList`: a snapshot, then upserts and removals.
pub(super) async fn terminal_list_stream(
    sink: StreamSink,
    context: Arc<V8Context>,
    spec: TerminalListSpec,
) {
    let hub = context.catalog.terminals.clone();
    let mut events = hub.list.subscribe();
    let cwd = match spec.root {
        Some(root) => {
            let task_context = context.clone();
            match tokio::task::spawn_blocking(move || root_path(&task_context, &root)).await {
                Ok(Ok(path)) => Some(display_path(&path)),
                Ok(Err(error)) => {
                    sink.end(EndReason::Error { error });
                    return;
                }
                Err(_) => return,
            }
        }
        None => None,
    };
    loop {
        let snapshot = TerminalListItem::Snapshot {
            terminals: hub.infos(cwd.as_deref()),
        };
        if sink.push(&snapshot, None).is_err() {
            return;
        }
        loop {
            let item = match events.recv().await {
                Ok(ListEvent::Upserted(info)) => {
                    if cwd.as_deref().is_some_and(|cwd| info.cwd != cwd) {
                        continue;
                    }
                    TerminalListItem::Upserted { terminal: info }
                }
                Ok(ListEvent::Removed(terminal_id)) => TerminalListItem::Removed { terminal_id },
                Err(broadcast::error::RecvError::Lagged(_)) => break,
                Err(broadcast::error::RecvError::Closed) => return,
            };
            if sink.push_after_data(&item).is_err() {
                return;
            }
        }
    }
}
