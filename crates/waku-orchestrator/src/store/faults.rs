//! Test-only fault points in `EventSink::commit` (crash injection).

use std::cell::Cell;

use super::StoreError;
use super::sink::FaultPoint;

thread_local! {
    static ARMED: Cell<Option<FaultPoint>> = const { Cell::new(None) };
}

/// Arms a one-shot fault for the next commit on this OS thread: the
/// transaction aborts right after `point`, as a crash there would.
pub fn arm(point: FaultPoint) {
    ARMED.with(|armed| armed.set(Some(point)));
}

pub fn check(point: FaultPoint) -> Result<(), StoreError> {
    ARMED.with(|armed| {
        if armed.get() == Some(point) {
            armed.set(None);
            Err(StoreError::Injected(point))
        } else {
            Ok(())
        }
    })
}
