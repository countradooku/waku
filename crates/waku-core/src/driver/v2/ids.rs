//! Native ids map to app ids deterministically (RFC 0002 "IdAllocator"):
//! UUIDv5 over (scope app id, `"<kind>:<native id>"`). No storage is needed,
//! replaying the same frame finds the same binding, and bindings survive
//! restarts because scope ids are persisted.

use std::sync::atomic::{AtomicU64, Ordering};

use uuid::Uuid;

/// What an id names; part of the binding key so a turn and an item with the
/// same native id never collide.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EntityKind {
    Session,
    ProviderThread,
    ProviderTurn,
    Item,
    Message,
    Request,
    Subagent,
    Plan,
    Node,
}

impl EntityKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::ProviderThread => "provider_thread",
            Self::ProviderTurn => "provider_turn",
            Self::Item => "item",
            Self::Message => "message",
            Self::Request => "request",
            Self::Subagent => "subagent",
            Self::Plan => "plan",
            Self::Node => "node",
        }
    }
}

/// Scopes: the provider session id scopes thread bindings; the provider
/// thread id scopes turn, item, request, subagent and plan bindings. With
/// `RefStrength::None`, drivers bind the turn ordinal, or
/// `turn_ordinal << 20 | item_index` for items.
pub trait IdAllocator: Send + Sync {
    fn bind(&self, scope: Uuid, kind: EntityKind, native: &str) -> Uuid {
        bind_v5(scope, kind, native)
    }
    fn bind_ordinal(&self, scope: Uuid, kind: EntityKind, ordinal: u64) -> Uuid {
        bind_v5(scope, kind, &format!("#{ordinal}"))
    }
    /// UUIDv7 in production, a counter in tests.
    fn fresh(&self, kind: EntityKind) -> Uuid;
}

/// The production binding rule.
pub fn bind_v5(scope: Uuid, kind: EntityKind, native: &str) -> Uuid {
    Uuid::new_v5(&scope, format!("{}:{native}", kind.as_str()).as_bytes())
}

/// UUIDv7 fresh ids.
#[derive(Clone, Copy, Debug, Default)]
pub struct UuidIdAllocator;

impl IdAllocator for UuidIdAllocator {
    fn fresh(&self, _kind: EntityKind) -> Uuid {
        Uuid::now_v7()
    }
}

/// Fresh ids `00000000-0000-7000-8000-%012d`, for replay.
#[derive(Debug, Default)]
pub struct SeqIdAllocator {
    next: AtomicU64,
}

impl SeqIdAllocator {
    pub fn new() -> Self {
        Self::default()
    }
}

impl IdAllocator for SeqIdAllocator {
    fn fresh(&self, _kind: EntityKind) -> Uuid {
        let n = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        Uuid::from_u128((0x7000_8000u128 << 48) | n as u128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bindings_are_stable_and_kind_scoped() {
        let ids = SeqIdAllocator::new();
        let scope = Uuid::from_u128(42);
        assert_eq!(
            ids.bind(scope, EntityKind::ProviderTurn, "t-1"),
            ids.bind(scope, EntityKind::ProviderTurn, "t-1")
        );
        assert_ne!(
            ids.bind(scope, EntityKind::ProviderTurn, "t-1"),
            ids.bind(scope, EntityKind::Item, "t-1")
        );
        assert_ne!(
            ids.bind(scope, EntityKind::Item, "t-1"),
            ids.bind(Uuid::from_u128(43), EntityKind::Item, "t-1")
        );
        assert_eq!(
            ids.fresh(EntityKind::Session).to_string(),
            "00000000-0000-7000-8000-000000000001"
        );
        assert_eq!(
            ids.fresh(EntityKind::Session).to_string(),
            "00000000-0000-7000-8000-000000000002"
        );
    }
}
