//! Snapshot coalescing for streaming text (RFC 0002 rule 7, RFC 0001 R12).
//!
//! Streaming assistant text and reasoning arrive as many small deltas, but
//! the stream carries entity snapshots. A [`SnapshotCoalescer`] keeps the
//! latest snapshot per entity and lets one through at most once per
//! interval. Any other event flushes the pending snapshots first, so order
//! across entities is preserved and nothing is lost before a terminal.
//!
//! Two coalescers use it:
//! - [`DeltaCoalescer`] in drivers: at most one snapshot per item every 16 ms;
//! - the session manager's persist throttle: at most one *persisted*
//!   snapshot per item every 250 ms, and only at a block boundary (a finished
//!   line), while every status change persists at once (R12).
//!
//! Time comes from the session's `Clock`, so replay with a manual clock is
//! deterministic: nothing is released by the interval until the clock moves.

use std::collections::HashMap;
use std::sync::Arc;

use waku_protocol::orchestration::{ItemStatus, Timestamp, TurnItemKind};

use super::{AdapterEvent, Clock};

/// The coalescing key of a streaming snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Key {
    Message(uuid::Uuid),
    Item(uuid::Uuid),
}

/// The text a streaming snapshot carries, if it is one.
fn streaming_text(event: &AdapterEvent) -> Option<(Key, &str)> {
    match event {
        AdapterEvent::MessageUpdated { message } if message.streaming => {
            Some((Key::Message(message.id.0), message.text.as_str()))
        }
        AdapterEvent::TurnItemUpdated { item } if !item.status.is_terminal() => match &item.kind {
            TurnItemKind::AssistantMessage {
                text,
                streaming: true,
                ..
            }
            | TurnItemKind::Reasoning {
                text,
                streaming: true,
            } => Some((Key::Item(item.id.0), text.as_str())),
            _ => None,
        },
        _ => None,
    }
}

fn status_of(event: &AdapterEvent) -> Option<ItemStatus> {
    match event {
        AdapterEvent::TurnItemUpdated { item } => Some(item.status),
        _ => None,
    }
}

struct Released {
    at: Timestamp,
    text_len: usize,
    status: Option<ItemStatus>,
}

/// Keeps the latest streaming snapshot per entity.
pub struct SnapshotCoalescer {
    clock: Arc<dyn Clock>,
    interval_ms: i64,
    /// Release only snapshots whose text gained a finished line.
    block_boundaries: bool,
    pending: Vec<(Key, AdapterEvent)>,
    released: HashMap<Key, Released>,
}

impl SnapshotCoalescer {
    pub fn new(clock: Arc<dyn Clock>, interval_ms: i64, block_boundaries: bool) -> Self {
        Self {
            clock,
            interval_ms,
            block_boundaries,
            pending: Vec::new(),
            released: HashMap::new(),
        }
    }

    /// Offers one event; returns what to emit now, in order.
    pub fn offer(&mut self, event: AdapterEvent) -> Vec<AdapterEvent> {
        let Some((key, text)) = streaming_text(&event) else {
            // Not a streaming snapshot: everything pending goes first.
            let mut out = self.flush();
            if let Some(key) = released_key(&event) {
                self.released.remove(&key);
            }
            out.push(event);
            return out;
        };
        let now = self.clock.now();
        let text_len = text.len();
        let release = match self.released.get(&key) {
            None => true,
            Some(last) => {
                let status_changed = last.status != status_of(&event);
                let elapsed = now - last.at >= self.interval_ms;
                let boundary = !self.block_boundaries
                    || text
                        .get(last.text_len.min(text_len)..)
                        .is_some_and(|added| added.contains('\n'));
                status_changed || (elapsed && boundary)
            }
        };
        if release {
            self.pending.retain(|(pending, _)| *pending != key);
            self.mark(key, &event, now, text_len);
            vec![event]
        } else {
            match self.pending.iter_mut().find(|(pending, _)| *pending == key) {
                Some(slot) => slot.1 = event,
                None => self.pending.push((key, event)),
            }
            Vec::new()
        }
    }

    /// Snapshots whose interval has passed (a timer tick).
    pub fn due(&mut self) -> Vec<AdapterEvent> {
        let now = self.clock.now();
        let mut out = Vec::new();
        let pending = std::mem::take(&mut self.pending);
        for (key, event) in pending {
            let ready = self.released.get(&key).is_none_or(|last| {
                let text_len = streaming_text(&event).map_or(0, |(_, text)| text.len());
                let boundary = !self.block_boundaries
                    || streaming_text(&event).is_some_and(|(_, text)| {
                        text.get(last.text_len.min(text_len)..)
                            .is_some_and(|added| added.contains('\n'))
                    });
                now - last.at >= self.interval_ms && boundary
            });
            if ready {
                let text_len = streaming_text(&event).map_or(0, |(_, text)| text.len());
                self.mark(key, &event, now, text_len);
                out.push(event);
            } else {
                self.pending.push((key, event));
            }
        }
        out
    }

    /// Everything pending, in arrival order.
    pub fn flush(&mut self) -> Vec<AdapterEvent> {
        let now = self.clock.now();
        let pending = std::mem::take(&mut self.pending);
        let mut out = Vec::with_capacity(pending.len());
        for (key, event) in pending {
            let text_len = streaming_text(&event).map_or(0, |(_, text)| text.len());
            self.mark(key, &event, now, text_len);
            out.push(event);
        }
        out
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    fn mark(&mut self, key: Key, event: &AdapterEvent, now: Timestamp, text_len: usize) {
        self.released.insert(
            key,
            Released {
                at: now,
                text_len,
                status: status_of(event),
            },
        );
    }
}

/// A final (non-streaming) snapshot ends an entity's coalescing state.
fn released_key(event: &AdapterEvent) -> Option<Key> {
    match event {
        AdapterEvent::MessageUpdated { message } => Some(Key::Message(message.id.0)),
        AdapterEvent::TurnItemUpdated { item } => Some(Key::Item(item.id.0)),
        _ => None,
    }
}

/// The drivers' coalescer: at most one snapshot per item every 16 ms.
pub struct DeltaCoalescer(SnapshotCoalescer);

impl DeltaCoalescer {
    pub const INTERVAL_MS: i64 = 16;

    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self(SnapshotCoalescer::new(clock, Self::INTERVAL_MS, false))
    }

    pub fn offer(&mut self, event: AdapterEvent) -> Vec<AdapterEvent> {
        self.0.offer(event)
    }

    pub fn due(&mut self) -> Vec<AdapterEvent> {
        self.0.due()
    }

    pub fn flush(&mut self) -> Vec<AdapterEvent> {
        self.0.flush()
    }

    /// Whether a snapshot waits for its interval (drivers arm a 16 ms tick).
    pub fn has_pending(&self) -> bool {
        self.0.has_pending()
    }
}

#[cfg(test)]
mod tests {
    use waku_orchestrator::ManualClock;
    use waku_protocol::orchestration::*;

    use super::*;

    fn item(text: &str, status: ItemStatus) -> AdapterEvent {
        AdapterEvent::TurnItemUpdated {
            item: TurnItem {
                id: TurnItemId(uuid::Uuid::from_u128(1)),
                thread_id: ThreadId(uuid::Uuid::from_u128(2)),
                run_id: None,
                node_id: None,
                provider_thread_id: None,
                provider_turn_id: None,
                native_item_ref: None,
                parent_item_id: None,
                position: 0,
                status,
                title: None,
                tool: None,
                stream_rev: 0,
                started_at: None,
                completed_at: None,
                updated_at: 0,
                kind: TurnItemKind::AssistantMessage {
                    message_id: MessageId(uuid::Uuid::from_u128(3)),
                    text: text.to_owned(),
                    streaming: !status.is_terminal(),
                },
            },
        }
    }

    fn text_of(events: &[AdapterEvent]) -> Vec<String> {
        events
            .iter()
            .map(|event| match event {
                AdapterEvent::TurnItemUpdated { item } => match &item.kind {
                    TurnItemKind::AssistantMessage { text, .. } => text.clone(),
                    _ => String::new(),
                },
                other => other.kind().to_owned(),
            })
            .collect()
    }

    #[test]
    fn snapshots_are_released_at_most_once_per_interval() {
        let clock = Arc::new(ManualClock::new(0));
        let mut coalescer = DeltaCoalescer::new(clock.clone());
        assert_eq!(
            text_of(&coalescer.offer(item("a", ItemStatus::Running))),
            ["a"]
        );
        assert!(coalescer.offer(item("ab", ItemStatus::Running)).is_empty());
        assert!(coalescer.offer(item("abc", ItemStatus::Running)).is_empty());
        assert!(coalescer.due().is_empty());
        clock.advance(16);
        assert_eq!(text_of(&coalescer.due()), ["abc"]);
        // A final snapshot passes at once and flushes nothing stale.
        assert!(
            coalescer
                .offer(item("abcd", ItemStatus::Running))
                .is_empty()
        );
        assert_eq!(
            text_of(&coalescer.offer(item("abcde", ItemStatus::Completed))),
            ["abcd", "abcde"]
        );
    }

    #[test]
    fn other_events_flush_pending_snapshots_first() {
        let clock = Arc::new(ManualClock::new(0));
        let mut coalescer = DeltaCoalescer::new(clock);
        coalescer.offer(item("a", ItemStatus::Running));
        coalescer.offer(item("ab", ItemStatus::Running));
        let goal = AdapterEvent::GoalUpdated { goal: None };
        assert_eq!(text_of(&coalescer.offer(goal)), ["ab", "goal_updated"]);
    }

    #[test]
    fn the_persist_throttle_waits_for_a_finished_line() {
        let clock = Arc::new(ManualClock::new(0));
        let mut throttle = SnapshotCoalescer::new(clock.clone(), 250, true);
        assert_eq!(
            text_of(&throttle.offer(item("# T", ItemStatus::Running))),
            ["# T"]
        );
        clock.advance(300);
        // No finished line yet: held even though the interval passed.
        assert!(
            throttle
                .offer(item("# Title", ItemStatus::Running))
                .is_empty()
        );
        assert!(throttle.due().is_empty());
        assert_eq!(
            text_of(&throttle.offer(item("# Title\nBody", ItemStatus::Running))),
            ["# Title\nBody"]
        );
        // A status change persists at once.
        assert_eq!(
            text_of(&throttle.offer(item("# Title\nBody", ItemStatus::Waiting))),
            ["# Title\nBody"]
        );
    }
}
