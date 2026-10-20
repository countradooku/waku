//! `CommandOutputHub` (#56, RFC 0003 §5.4): live command output on its own
//! stream, never as events.
//!
//! Routing output deltas through `turn_item.updated` floods the event log
//! and blocks ingestion for every thread (T3 regression, audit §4.3). The
//! provider session manager instead feeds each `AdapterEvent::
//! CommandOutputDelta` to [`CommandOutputHub::append`], which keeps one
//! bounded tail per command, and calls [`CommandOutputHub::finish`] when the
//! command completes; the capped text it returns is what the item stores in
//! `output` (RFC 0001 R12: head 64 KiB + tail 192 KiB).
//!
//! The hub is bounded everywhere: [`OUTPUT_LIVE_TAILS`] running commands and
//! [`OUTPUT_FINISHED_TAILS`] finished ones, each retaining at most its head
//! and tail. Subscribers (one per expanded command row) get frames: a
//! `replace` with the retained text first, then `append`s of at most
//! [`OUTPUT_APPEND_BYTES`]; output beyond that between two frames becomes a
//! `replace` again, and the subscriber is then paced at
//! [`OUTPUT_OVERFLOW_FRAME_MS`] instead of [`OUTPUT_FRAME_MS`].
//!
//! Like the rest of the crate the hub is runtime-free: a subscriber passes a
//! waker, and the daemon's stream task pulls frames when woken.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use waku_protocol::orchestration::{ThreadId, TurnItemId};
use waku_protocol::v8::orchestration::{
    CommandOutputItem, OUTPUT_APPEND_BYTES, OUTPUT_FINISHED_TAILS, OUTPUT_FRAME_MS,
    OUTPUT_HEAD_BYTES, OUTPUT_LIVE_TAILS, OUTPUT_OVERFLOW_FRAME_MS, OUTPUT_TAIL_BYTES,
};

/// Bounds of a hub; [`Default`] is the protocol's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputHubConfig {
    pub live_tails: usize,
    pub finished_tails: usize,
    pub head_bytes: usize,
    pub tail_bytes: usize,
    pub append_bytes: usize,
}

impl Default for OutputHubConfig {
    fn default() -> Self {
        Self {
            live_tails: OUTPUT_LIVE_TAILS,
            finished_tails: OUTPUT_FINISHED_TAILS,
            head_bytes: OUTPUT_HEAD_BYTES,
            tail_bytes: OUTPUT_TAIL_BYTES,
            append_bytes: OUTPUT_APPEND_BYTES,
        }
    }
}

/// What a finished command keeps in its turn item's `output`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredOutput {
    pub text: String,
    /// Output between the head and the tail was dropped.
    pub truncated: bool,
}

/// Marks where retained output skips bytes.
pub const ELISION: &str = "\n…\n";

fn floor_boundary(text: &str, index: usize) -> usize {
    let mut index = index.min(text.len());
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_boundary(text: &str, index: usize) -> usize {
    let mut index = index.min(text.len());
    while !text.is_char_boundary(index) {
        index += 1;
    }
    index
}

/// The first `head_bytes` and the last `tail_bytes` of a command's output.
/// Trimming never splits a UTF-8 code point.
#[derive(Default)]
struct Retained {
    head: String,
    tail: String,
    /// Bytes were dropped between head and tail.
    elided: bool,
}

impl Retained {
    fn push(&mut self, mut chunk: &str, config: &OutputHubConfig) {
        if self.tail.is_empty() && self.head.len() < config.head_bytes {
            let cut = floor_boundary(chunk, config.head_bytes - self.head.len());
            self.head.push_str(&chunk[..cut]);
            chunk = &chunk[cut..];
        }
        if chunk.is_empty() {
            return;
        }
        if chunk.len() > config.tail_bytes {
            // Only the end of a huge chunk can survive.
            chunk = &chunk[ceil_boundary(chunk, chunk.len() - config.tail_bytes)..];
            self.elided = true;
        }
        self.tail.push_str(chunk);
        // Amortized: trim once the tail is a quarter over its cap.
        if self.tail.len() > config.tail_bytes + config.tail_bytes / 4 {
            let start = ceil_boundary(&self.tail, self.tail.len() - config.tail_bytes);
            self.tail.drain(..start);
            self.elided = true;
        }
    }

    fn view(&self, config: &OutputHubConfig) -> StoredOutput {
        let start = if self.tail.len() > config.tail_bytes {
            ceil_boundary(&self.tail, self.tail.len() - config.tail_bytes)
        } else {
            0
        };
        let truncated = self.elided || start > 0;
        let tail = &self.tail[start..];
        let mut text = String::with_capacity(self.head.len() + ELISION.len() + tail.len());
        text.push_str(&self.head);
        if truncated {
            text.push_str(ELISION);
        }
        text.push_str(tail);
        StoredOutput { text, truncated }
    }

    fn bytes(&self) -> usize {
        self.head.capacity() + self.tail.capacity()
    }
}

struct Tail {
    thread_id: ThreadId,
    retained: Retained,
    running: bool,
    exit_code: Option<i32>,
    subscribers: Vec<Weak<Subscriber>>,
    touched: u64,
}

#[derive(Default)]
struct SubState {
    /// Send the whole retained text next.
    replace: bool,
    /// Output since the last frame, while it fits an `append`.
    pending: String,
    /// More than an `append` arrived between frames: pace slower.
    overflowed: bool,
    /// The command finished: `(exit code, truncated)`.
    finished: Option<(Option<i32>, bool)>,
    finished_sent: bool,
}

struct Subscriber {
    state: Mutex<SubState>,
    waker: Box<dyn Fn() + Send + Sync>,
}

impl Subscriber {
    fn wake(&self) {
        (self.waker)();
    }
}

#[derive(Default)]
struct Inner {
    tails: HashMap<TurnItemId, Tail>,
    /// Finished commands, oldest first.
    finished: VecDeque<TurnItemId>,
    /// Subscribers to commands that have produced nothing yet.
    waiting: HashMap<TurnItemId, (ThreadId, Vec<Weak<Subscriber>>)>,
    tick: u64,
}

impl Inner {
    fn live_count(&self) -> usize {
        self.tails.len() - self.finished.len()
    }
}

/// See the module documentation.
pub struct CommandOutputHub {
    config: OutputHubConfig,
    inner: Mutex<Inner>,
}

impl Default for CommandOutputHub {
    fn default() -> Self {
        Self::new(OutputHubConfig::default())
    }
}

impl CommandOutputHub {
    pub fn new(config: OutputHubConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// The tail of `item_id`, created (running) if needed.
    fn tail_mut<'a>(
        &self,
        inner: &'a mut Inner,
        thread_id: ThreadId,
        item_id: TurnItemId,
    ) -> &'a mut Tail {
        inner.tick += 1;
        let tick = inner.tick;
        if !inner.tails.contains_key(&item_id) {
            if inner.live_count() >= self.config.live_tails {
                self.evict_live(inner);
            }
            let subscribers = match inner.waiting.remove(&item_id) {
                Some((_, waiting)) => waiting,
                None => Vec::new(),
            };
            for subscriber in subscribers.iter().filter_map(Weak::upgrade) {
                subscriber.state.lock().replace = true;
                subscriber.wake();
            }
            inner.tails.insert(
                item_id,
                Tail {
                    thread_id,
                    retained: Retained::default(),
                    running: true,
                    exit_code: None,
                    subscribers,
                    touched: tick,
                },
            );
        }
        let tail = inner.tails.get_mut(&item_id).expect("inserted above");
        tail.touched = tick;
        tail
    }

    /// Drops the least recently active running tail; its subscribers see it
    /// finish truncated.
    fn evict_live(&self, inner: &mut Inner) {
        let Some(oldest) = inner
            .tails
            .iter()
            .filter(|(_, tail)| tail.running)
            .min_by_key(|(_, tail)| tail.touched)
            .map(|(id, _)| *id)
        else {
            return;
        };
        if let Some(tail) = inner.tails.remove(&oldest) {
            for subscriber in tail.subscribers.iter().filter_map(Weak::upgrade) {
                subscriber.state.lock().finished = Some((None, true));
                subscriber.wake();
            }
        }
    }

    /// A command started producing output (optional: [`Self::append`]
    /// starts it too).
    pub fn start(&self, thread_id: ThreadId, item_id: TurnItemId) {
        let mut inner = self.inner.lock();
        self.tail_mut(&mut inner, thread_id, item_id);
    }

    /// New output of a running command. Never blocks on subscribers and
    /// never touches the event log.
    pub fn append(&self, thread_id: ThreadId, item_id: TurnItemId, chunk: &str) {
        if chunk.is_empty() {
            return;
        }
        let append_bytes = self.config.append_bytes;
        let mut inner = self.inner.lock();
        let tail = self.tail_mut(&mut inner, thread_id, item_id);
        if !tail.running {
            return;
        }
        tail.retained.push(chunk, &self.config);
        tail.subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            let mut state = subscriber.state.lock();
            if !state.replace {
                if state.pending.len() + chunk.len() > append_bytes {
                    state.pending = String::new();
                    state.replace = true;
                    state.overflowed = true;
                } else {
                    state.pending.push_str(chunk);
                }
            }
            drop(state);
            subscriber.wake();
            true
        });
    }

    /// The command finished. Returns what its turn item should store
    /// (`None` for a command the hub never saw).
    pub fn finish(&self, item_id: TurnItemId, exit_code: Option<i32>) -> Option<StoredOutput> {
        let mut inner = self.inner.lock();
        let tail = inner.tails.get_mut(&item_id)?;
        let stored = tail.retained.view(&self.config);
        if tail.running {
            tail.running = false;
            tail.exit_code = exit_code;
            for subscriber in tail.subscribers.iter().filter_map(Weak::upgrade) {
                subscriber.state.lock().finished = Some((exit_code, stored.truncated));
                subscriber.wake();
            }
            inner.finished.push_back(item_id);
            while inner.finished.len() > self.config.finished_tails {
                if let Some(oldest) = inner.finished.pop_front() {
                    inner.tails.remove(&oldest);
                }
            }
        }
        Some(stored)
    }

    /// The retained output of `item_id`: `(output, running)`.
    pub fn tail(&self, item_id: TurnItemId) -> Option<(StoredOutput, bool)> {
        let inner = self.inner.lock();
        let tail = inner.tails.get(&item_id)?;
        Some((tail.retained.view(&self.config), tail.running))
    }

    /// Subscribes to `item_id`'s output; `waker` runs (on the producing
    /// thread, so it must be cheap) whenever a frame may be ready. Returns
    /// `None` when the command is known to belong to another thread.
    pub fn subscribe(
        self: &Arc<Self>,
        thread_id: ThreadId,
        item_id: TurnItemId,
        waker: impl Fn() + Send + Sync + 'static,
    ) -> Option<OutputSubscription> {
        let subscriber = Arc::new(Subscriber {
            state: Mutex::new(SubState::default()),
            waker: Box::new(waker),
        });
        let mut inner = self.inner.lock();
        let known = match inner.tails.get_mut(&item_id) {
            Some(tail) if tail.thread_id != thread_id => return None,
            Some(tail) => {
                let mut state = subscriber.state.lock();
                state.replace = true;
                if !tail.running {
                    state.finished =
                        Some((tail.exit_code, tail.retained.view(&self.config).truncated));
                }
                drop(state);
                tail.subscribers.push(Arc::downgrade(&subscriber));
                true
            }
            None => {
                let entry = inner
                    .waiting
                    .entry(item_id)
                    .or_insert_with(|| (thread_id, Vec::new()));
                if entry.0 != thread_id {
                    return None;
                }
                entry.1.retain(|waiting| waiting.strong_count() > 0);
                entry.1.push(Arc::downgrade(&subscriber));
                false
            }
        };
        Some(OutputSubscription {
            hub: self.clone(),
            item_id,
            known,
            subscriber,
        })
    }

    /// Running commands with a tail.
    pub fn live_tails(&self) -> usize {
        self.inner.lock().live_count()
    }

    /// Finished commands with a tail.
    pub fn finished_tails(&self) -> usize {
        self.inner.lock().finished.len()
    }

    /// Bytes held by every retained tail (for the bound tests).
    pub fn retained_bytes(&self) -> usize {
        let inner = self.inner.lock();
        inner.tails.values().map(|tail| tail.retained.bytes()).sum()
    }
}

/// One subscriber's handle; dropping it unsubscribes.
pub struct OutputSubscription {
    hub: Arc<CommandOutputHub>,
    item_id: TurnItemId,
    known: bool,
    subscriber: Arc<Subscriber>,
}

impl OutputSubscription {
    /// The hub had output for the command when this subscribed.
    pub fn was_known(&self) -> bool {
        self.known
    }

    /// The frames ready now, and how long to wait before taking the next
    /// ones. After a `finished` frame the subscription is over.
    pub fn take_frames(&self) -> (Vec<CommandOutputItem>, Duration) {
        let hub = &self.hub;
        let inner = hub.inner.lock();
        let mut state = self.subscriber.state.lock();
        let mut frames = Vec::new();
        if state.replace {
            state.replace = false;
            state.pending.clear();
            match inner.tails.get(&self.item_id) {
                Some(tail) => {
                    let view = tail.retained.view(&hub.config);
                    frames.push(CommandOutputItem::Replace {
                        text: view.text,
                        truncated: view.truncated,
                        running: tail.running,
                    });
                }
                None => frames.push(CommandOutputItem::Replace {
                    text: String::new(),
                    truncated: true,
                    running: false,
                }),
            }
        } else if !state.pending.is_empty() {
            frames.push(CommandOutputItem::Append {
                text: std::mem::take(&mut state.pending),
            });
        }
        if let Some((exit_code, truncated)) = state.finished
            && !state.finished_sent
        {
            state.finished_sent = true;
            frames.push(CommandOutputItem::Finished {
                exit_code,
                truncated,
            });
        }
        let interval = if std::mem::take(&mut state.overflowed) {
            Duration::from_millis(OUTPUT_OVERFLOW_FRAME_MS)
        } else {
            Duration::from_millis(OUTPUT_FRAME_MS)
        };
        (frames, interval)
    }

    /// Whether `finished` has been taken.
    pub fn is_finished(&self) -> bool {
        self.subscriber.state.lock().finished_sent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn ids(n: u128) -> (ThreadId, TurnItemId) {
        (
            ThreadId(uuid::Uuid::from_u128(1)),
            TurnItemId(uuid::Uuid::from_u128(n)),
        )
    }

    #[test]
    fn subscribers_get_replace_then_appends_then_finished() {
        let hub = Arc::new(CommandOutputHub::default());
        let (thread, item) = ids(10);
        hub.append(thread, item, "hello ");
        let wakes = Arc::new(AtomicUsize::new(0));
        let counter = wakes.clone();
        let subscription = hub
            .subscribe(thread, item, move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        assert!(subscription.was_known());
        let (frames, interval) = subscription.take_frames();
        assert_eq!(
            frames,
            [CommandOutputItem::Replace {
                text: "hello ".into(),
                truncated: false,
                running: true
            }]
        );
        assert_eq!(interval, Duration::from_millis(OUTPUT_FRAME_MS));
        hub.append(thread, item, "wor");
        hub.append(thread, item, "ld");
        assert_eq!(wakes.load(Ordering::Relaxed), 2);
        assert_eq!(
            subscription.take_frames().0,
            [CommandOutputItem::Append {
                text: "world".into()
            }]
        );
        let stored = hub.finish(item, Some(0)).unwrap();
        assert_eq!(stored.text, "hello world");
        assert_eq!(
            subscription.take_frames().0,
            [CommandOutputItem::Finished {
                exit_code: Some(0),
                truncated: false
            }]
        );
        assert!(subscription.is_finished());
        assert!(subscription.take_frames().0.is_empty());
        // Another thread's subscriber is refused.
        assert!(
            hub.subscribe(ThreadId(uuid::Uuid::nil()), item, || {})
                .is_none()
        );
    }

    #[test]
    fn a_waiting_subscriber_starts_with_the_first_output() {
        let hub = Arc::new(CommandOutputHub::default());
        let (thread, item) = ids(11);
        let subscription = hub.subscribe(thread, item, || {}).unwrap();
        assert!(!subscription.was_known());
        assert!(subscription.take_frames().0.is_empty());
        hub.append(thread, item, "first");
        assert_eq!(
            subscription.take_frames().0,
            [CommandOutputItem::Replace {
                text: "first".into(),
                truncated: false,
                running: true
            }]
        );
    }

    #[test]
    fn a_flood_overflows_into_a_replace_and_stays_bounded() {
        let config = OutputHubConfig {
            head_bytes: 1024,
            tail_bytes: 4096,
            append_bytes: 512,
            ..OutputHubConfig::default()
        };
        let hub = Arc::new(CommandOutputHub::new(config));
        let (thread, item) = ids(12);
        hub.start(thread, item);
        let subscription = hub.subscribe(thread, item, || {}).unwrap();
        subscription.take_frames();
        for _ in 0..100_000 {
            hub.append(thread, item, "y\n");
        }
        let (frames, interval) = subscription.take_frames();
        let [
            CommandOutputItem::Replace {
                text,
                truncated,
                running,
            },
        ] = frames.as_slice()
        else {
            panic!("{frames:?}");
        };
        assert!(*truncated && *running);
        assert!(text.len() <= 1024 + ELISION.len() + 4096, "{}", text.len());
        assert!(text.starts_with("y\ny\n") && text.ends_with("y\n"));
        assert_eq!(interval, Duration::from_millis(OUTPUT_OVERFLOW_FRAME_MS));
        assert!(
            hub.retained_bytes() <= 2 * (1024 + 4096 + 4096 / 4),
            "{}",
            hub.retained_bytes()
        );
        let stored = hub.finish(item, None).unwrap();
        assert!(stored.truncated && stored.text.contains(ELISION));
    }

    #[test]
    fn utf8_is_never_split() {
        let config = OutputHubConfig {
            head_bytes: 5,
            tail_bytes: 7,
            ..OutputHubConfig::default()
        };
        let hub = Arc::new(CommandOutputHub::new(config));
        let (thread, item) = ids(13);
        for _ in 0..50 {
            hub.append(thread, item, "héllo wörld ");
        }
        let (stored, _) = hub.tail(item).unwrap();
        assert!(stored.truncated);
        // `String` guarantees validity; the head stopped before the `é`.
        assert!(stored.text.starts_with("héll"));
    }

    #[test]
    fn live_and_finished_tails_are_capped() {
        let config = OutputHubConfig {
            live_tails: 3,
            finished_tails: 2,
            ..OutputHubConfig::default()
        };
        let hub = Arc::new(CommandOutputHub::new(config));
        let thread = ThreadId(uuid::Uuid::from_u128(1));
        let item = |n: u128| TurnItemId(uuid::Uuid::from_u128(100 + n));
        let first = hub.subscribe(thread, item(0), || {}).unwrap();
        for n in 0..5 {
            hub.append(thread, item(n), "x");
        }
        assert_eq!(hub.live_tails(), 3);
        // The evicted command's subscriber sees it end.
        let frames = first.take_frames().0;
        assert!(frames.contains(&CommandOutputItem::Finished {
            exit_code: None,
            truncated: true
        }));
        for n in 2..5 {
            hub.finish(item(n), Some(0));
        }
        assert_eq!((hub.live_tails(), hub.finished_tails()), (0, 2));
        assert!(hub.tail(item(2)).is_none());
        assert!(hub.tail(item(4)).is_some());
    }
}
