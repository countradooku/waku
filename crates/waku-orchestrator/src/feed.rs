//! `CommittedFeed`: every committed event, after COMMIT, in sequence order.
//!
//! Subscribers share one `Arc<StoredEvent>` per event (no per-subscriber
//! clone). Each subscription is bounded; one that falls behind is ended with
//! `overflowed()` set rather than growing memory or slowing the writer
//! (RFC 0003 P9), and its owner resubscribes from its cursor.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded};
use parking_lot::Mutex;
use waku_protocol::orchestration::StoredEvent;

/// Events a subscriber may lag behind before it is dropped.
pub const DEFAULT_FEED_CAPACITY: usize = 16_384;

struct Subscriber {
    sender: Sender<Arc<StoredEvent>>,
    overflowed: Arc<AtomicBool>,
}

#[derive(Default)]
pub struct FeedPublisher {
    subscribers: Mutex<Vec<Subscriber>>,
}

impl FeedPublisher {
    pub fn subscribe(&self, capacity: usize) -> CommittedFeed {
        let (sender, receiver) = bounded(capacity.max(1));
        let overflowed = Arc::new(AtomicBool::new(false));
        self.subscribers.lock().push(Subscriber {
            sender,
            overflowed: overflowed.clone(),
        });
        CommittedFeed {
            receiver,
            overflowed,
        }
    }

    /// Called by the sink after COMMIT, while it still holds the writer.
    pub(crate) fn publish(&self, events: &[Arc<StoredEvent>]) {
        if events.is_empty() {
            return;
        }
        let mut subscribers = self.subscribers.lock();
        subscribers.retain(|subscriber| {
            for event in events {
                match subscriber.sender.try_send(event.clone()) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        subscriber.overflowed.store(true, Ordering::Release);
                        return false;
                    }
                    Err(TrySendError::Disconnected(_)) => return false,
                }
            }
            true
        });
    }

    /// Ends every subscription (shutdown).
    pub(crate) fn close(&self) {
        self.subscribers.lock().clear();
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.lock().len()
    }
}

/// The receiving end of a subscription.
pub struct CommittedFeed {
    receiver: Receiver<Arc<StoredEvent>>,
    overflowed: Arc<AtomicBool>,
}

/// Why a feed ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedEnded {
    /// The subscriber fell behind; resubscribe from the last sequence seen.
    Overflowed,
    /// The orchestrator shut down.
    Closed,
}

impl CommittedFeed {
    fn ended(&self) -> FeedEnded {
        if self.overflowed.load(Ordering::Acquire) {
            FeedEnded::Overflowed
        } else {
            FeedEnded::Closed
        }
    }

    /// Blocks for the next committed event.
    pub fn recv(&self) -> Result<Arc<StoredEvent>, FeedEnded> {
        self.receiver.recv().map_err(|_| self.ended())
    }

    /// The next event if one is already committed.
    pub fn try_recv(&self) -> Option<Arc<StoredEvent>> {
        self.receiver.try_recv().ok()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Option<Arc<StoredEvent>>, FeedEnded> {
        match self.receiver.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err(self.ended()),
        }
    }

    /// Everything already committed and not yet received.
    pub fn drain(&self) -> Vec<Arc<StoredEvent>> {
        self.receiver.try_iter().collect()
    }

    /// The underlying channel, for `crossbeam_channel::select!` with other
    /// sources. A disconnect means the feed ended; see [`Self::overflowed`].
    pub fn receiver(&self) -> &Receiver<Arc<StoredEvent>> {
        &self.receiver
    }

    pub fn overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Acquire)
    }
}
