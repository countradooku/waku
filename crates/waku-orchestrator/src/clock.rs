//! Time sources. `decide` never reads the clock; the orchestrator reads it
//! once per command and passes the value in.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use waku_protocol::orchestration::Timestamp;

pub trait Clock: Send + Sync {
    /// Unix epoch milliseconds, UTC (R7).
    fn now(&self) -> Timestamp;
}

/// The wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as Timestamp)
            .unwrap_or_default()
    }
}

/// A clock that only moves when told to, for deterministic tests.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicI64);

impl ManualClock {
    pub fn new(start: Timestamp) -> Self {
        Self(AtomicI64::new(start))
    }

    pub fn set(&self, now: Timestamp) {
        self.0.store(now, Ordering::SeqCst);
    }

    pub fn advance(&self, milliseconds: i64) -> Timestamp {
        self.0.fetch_add(milliseconds, Ordering::SeqCst) + milliseconds
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        self.0.load(Ordering::SeqCst)
    }
}
