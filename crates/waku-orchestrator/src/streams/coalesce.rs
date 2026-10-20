//! Coalescing of running tool updates on the `thread` stream (RFC 0003
//! §5.4; T3 `ThreadLiveEventCoalescer`).
//!
//! A running tool can update its turn item many times a second. Updates of
//! running tool items are held for up to [`TOOL_COALESCE_MS`]; a newer update
//! of the same item replaces the held one. Any other event first flushes
//! everything held, so the stream stays in sequence order and a client's
//! cursor never passes an update it has not received: a superseded update is
//! fully contained in the one that replaced it (events are full entity
//! snapshots, RFC 0001 R11).
//!
//! The coalescer is a pure state machine; the caller supplies the time and
//! arms a timer for [`ToolCoalescer::deadline`].

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use waku_protocol::orchestration::{DomainEvent, ItemStatus, Sequence, TurnItemId, TurnItemKind};
use waku_protocol::v8::orchestration::{TOOL_COALESCE_MS, TOOL_COALESCE_PENDING};

/// The item a coalescible event updates: a `turn_item.updated` of a running
/// tool row.
pub fn coalesce_key(event: &DomainEvent) -> Option<TurnItemId> {
    let DomainEvent::TurnItemUpdated(item) = event else {
        return None;
    };
    let running = matches!(item.status, ItemStatus::Running | ItemStatus::Pending);
    let tool = matches!(
        item.kind,
        TurnItemKind::CommandExecution { .. }
            | TurnItemKind::DynamicTool { .. }
            | TurnItemKind::FileChange { .. }
            | TurnItemKind::FileSearch { .. }
            | TurnItemKind::WebSearch { .. }
            | TurnItemKind::Subagent { .. }
    );
    (running && tool).then_some(item.id)
}

/// The item any `turn_item.updated` concerns.
fn item_of(event: &DomainEvent) -> Option<TurnItemId> {
    match event {
        DomainEvent::TurnItemUpdated(item) => Some(item.id),
        _ => None,
    }
}

pub struct ToolCoalescer<T> {
    window: Duration,
    max_pending: usize,
    pending: BTreeMap<Sequence, (Option<TurnItemId>, T)>,
    by_item: HashMap<TurnItemId, Sequence>,
    deadline: Option<Instant>,
}

impl<T> Default for ToolCoalescer<T> {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(TOOL_COALESCE_MS),
            TOOL_COALESCE_PENDING,
        )
    }
}

impl<T> ToolCoalescer<T> {
    pub fn new(window: Duration, max_pending: usize) -> Self {
        Self {
            window,
            max_pending: max_pending.max(1),
            pending: BTreeMap::new(),
            by_item: HashMap::new(),
            deadline: None,
        }
    }

    /// When held updates must go out.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Takes one event; returns what to send now, in sequence order.
    pub fn push(&mut self, now: Instant, seq: Sequence, event: &DomainEvent, value: T) -> Vec<T> {
        if let Some(item) = coalesce_key(event) {
            if let Some(previous) = self.by_item.insert(item, seq) {
                self.pending.remove(&previous);
            }
            self.pending.insert(seq, (Some(item), value));
            self.deadline.get_or_insert(now + self.window);
            if self.pending.len() > self.max_pending {
                return self.flush();
            }
            return Vec::new();
        }
        // A final update of a held item replaces it outright.
        if let Some(previous) = item_of(event).and_then(|item| self.by_item.remove(&item)) {
            self.pending.remove(&previous);
        }
        let mut out = self.flush();
        out.push(value);
        out
    }

    /// Everything held, in sequence order.
    pub fn flush(&mut self) -> Vec<T> {
        self.deadline = None;
        self.by_item.clear();
        std::mem::take(&mut self.pending)
            .into_values()
            .map(|(_, value)| value)
            .collect()
    }

    /// Everything held if the window has passed.
    pub fn due(&mut self, now: Instant) -> Vec<T> {
        match self.deadline {
            Some(deadline) if now >= deadline => self.flush(),
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use waku_protocol::orchestration::{ThreadId, TurnItem};

    fn update(id: u128, status: ItemStatus) -> DomainEvent {
        DomainEvent::TurnItemUpdated(TurnItem {
            id: TurnItemId(uuid::Uuid::from_u128(id)),
            thread_id: ThreadId(uuid::Uuid::nil()),
            run_id: None,
            node_id: None,
            provider_thread_id: None,
            provider_turn_id: None,
            native_item_ref: None,
            parent_item_id: None,
            position: id as u64,
            status,
            title: None,
            tool: None,
            stream_rev: 0,
            started_at: None,
            completed_at: None,
            updated_at: 0,
            kind: TurnItemKind::CommandExecution {
                input: "make".into(),
                output: None,
                output_truncated: false,
                output_omitted: false,
                output_indicates_failure: false,
                exit_code: None,
            },
        })
    }

    #[test]
    fn running_updates_collapse_and_other_events_flush_in_order() {
        let start = Instant::now();
        let mut coalescer = ToolCoalescer::new(Duration::from_millis(50), 512);
        let running = update(1, ItemStatus::Running);
        let other = update(2, ItemStatus::Running);
        assert!(coalescer.push(start, 10, &running, 10).is_empty());
        assert!(coalescer.push(start, 11, &other, 11).is_empty());
        assert!(coalescer.push(start, 12, &running, 12).is_empty());
        assert_eq!(
            coalescer.deadline(),
            Some(start + Duration::from_millis(50))
        );
        assert!(coalescer.due(start + Duration::from_millis(49)).is_empty());
        assert_eq!(coalescer.due(start + Duration::from_millis(50)), [11, 12]);
        assert!(coalescer.is_empty() && coalescer.deadline().is_none());

        // A non-coalescible event flushes first; a final update of a held item
        // replaces it.
        coalescer.push(start, 20, &running, 20);
        coalescer.push(start, 21, &other, 21);
        let done = update(1, ItemStatus::Completed);
        assert_eq!(coalescer.push(start, 22, &done, 22), [21, 22]);
        let run = DomainEvent::Unknown;
        assert_eq!(coalescer.push(start, 23, &run, 23), [23]);
    }

    #[test]
    fn the_pending_cap_flushes() {
        let start = Instant::now();
        let mut coalescer = ToolCoalescer::new(Duration::from_secs(1), 2);
        assert!(
            coalescer
                .push(start, 1, &update(1, ItemStatus::Running), 1)
                .is_empty()
        );
        assert!(
            coalescer
                .push(start, 2, &update(2, ItemStatus::Running), 2)
                .is_empty()
        );
        assert_eq!(
            coalescer.push(start, 3, &update(3, ItemStatus::Running), 3),
            [1, 2, 3]
        );
    }
}
