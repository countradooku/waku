//! Live text deltas (RFC 0003 §5.4, RFC 0001 R12).
//!
//! The provider session manager's streaming coalescer publishes the text it
//! folds into a streaming item, every 8–16 ms, as [`LiveDelta`]s; the item's
//! persisted snapshots (at most every 250 ms) carry `streamRev`, the number
//! of deltas folded in. Deltas are live-only: never sequenced, persisted or
//! replayed, and lossy under pressure — a subscriber that cannot keep up
//! drops them, its client sees a `rev` gap and waits for the next snapshot.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use waku_protocol::orchestration::{ThreadId, TurnItemId};
use waku_protocol::v8::orchestration::{DeltaChannel, ThreadItem};

/// Text appended to a streaming item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveDelta {
    pub thread_id: ThreadId,
    pub item_id: TurnItemId,
    pub channel: DeltaChannel,
    /// The item's `streamRev` once this delta is applied.
    pub rev: u32,
    pub text: String,
}

impl LiveDelta {
    /// The `thread` stream item.
    pub fn item(&self) -> ThreadItem {
        ThreadItem::Delta {
            item_id: self.item_id,
            channel: self.channel.clone(),
            rev: self.rev,
            text: self.text.clone(),
        }
    }
}

/// Receives the deltas of one thread. Returns `false` to unsubscribe. Runs
/// on the publishing thread, so it must not block.
pub type DeltaSink = Box<dyn Fn(&Arc<LiveDelta>) -> bool + Send + Sync>;

type Subscribers = HashMap<ThreadId, Vec<(u64, Arc<DeltaSink>)>>;

/// Routes deltas to the `thread` streams subscribed to their thread.
#[derive(Default)]
pub struct DeltaBus {
    subscribers: RwLock<Subscribers>,
    next: AtomicU64,
}

impl DeltaBus {
    /// Whether anyone watches `thread_id`; a producer may skip building
    /// deltas nobody reads.
    pub fn has_subscribers(&self, thread_id: ThreadId) -> bool {
        self.subscribers.read().contains_key(&thread_id)
    }

    pub fn publish(&self, delta: LiveDelta) {
        let delta = Arc::new(delta);
        let sinks = match self.subscribers.read().get(&delta.thread_id) {
            Some(sinks) => sinks.clone(),
            None => return,
        };
        let mut dropped = Vec::new();
        for (id, sink) in sinks {
            if !sink(&delta) {
                dropped.push(id);
            }
        }
        if !dropped.is_empty() {
            self.remove(delta.thread_id, &dropped);
        }
    }

    fn remove(&self, thread_id: ThreadId, ids: &[u64]) {
        let mut subscribers = self.subscribers.write();
        if let Some(sinks) = subscribers.get_mut(&thread_id) {
            sinks.retain(|(id, _)| !ids.contains(id));
            if sinks.is_empty() {
                subscribers.remove(&thread_id);
            }
        }
    }

    /// Delivers `thread_id`'s deltas to `sink` until the returned guard is
    /// dropped.
    pub fn subscribe(self: &Arc<Self>, thread_id: ThreadId, sink: DeltaSink) -> DeltaSubscription {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.subscribers
            .write()
            .entry(thread_id)
            .or_default()
            .push((id, Arc::new(sink)));
        DeltaSubscription {
            bus: Arc::downgrade(self),
            thread_id,
            id,
        }
    }
}

/// Unsubscribes when dropped.
pub struct DeltaSubscription {
    bus: Weak<DeltaBus>,
    thread_id: ThreadId,
    id: u64,
}

impl Drop for DeltaSubscription {
    fn drop(&mut self) {
        if let Some(bus) = self.bus.upgrade() {
            bus.remove(self.thread_id, &[self.id]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    #[test]
    fn deltas_reach_their_thread_until_unsubscribed() {
        let bus = Arc::new(DeltaBus::default());
        let thread = ThreadId(uuid::Uuid::from_u128(1));
        let other = ThreadId(uuid::Uuid::from_u128(2));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink_seen = seen.clone();
        let subscription = bus.subscribe(
            thread,
            Box::new(move |delta| {
                sink_seen.lock().push(delta.rev);
                true
            }),
        );
        let delta = |thread_id, rev| LiveDelta {
            thread_id,
            item_id: TurnItemId(uuid::Uuid::nil()),
            channel: DeltaChannel::Text,
            rev,
            text: "x".into(),
        };
        assert!(bus.has_subscribers(thread) && !bus.has_subscribers(other));
        bus.publish(delta(thread, 1));
        bus.publish(delta(other, 2));
        bus.publish(delta(thread, 3));
        assert_eq!(*seen.lock(), [1, 3]);
        drop(subscription);
        assert!(!bus.has_subscribers(thread));
        bus.publish(delta(thread, 4));
        assert_eq!(*seen.lock(), [1, 3]);
        // A sink that refuses is dropped.
        let _refusing = bus.subscribe(thread, Box::new(|_| false));
        bus.publish(delta(thread, 5));
        assert!(!bus.has_subscribers(thread));
    }
}
