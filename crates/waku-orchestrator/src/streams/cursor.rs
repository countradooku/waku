//! Opaque history cursors (RFC 0003 §4 `HistoryCursor`).
//!
//! A cursor is a short versioned string. Clients treat it as opaque; the
//! daemon checks that it names the thread it is used with, so a cursor of
//! one thread cannot page another.

use waku_protocol::orchestration::{ThreadId, Timestamp};
use waku_protocol::v8::orchestration::MAX_HISTORY_CURSOR_BYTES;

/// Why a cursor was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BadCursor(pub &'static str);

impl std::fmt::Display for BadCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid history cursor: {}", self.0)
    }
}

impl std::error::Error for BadCursor {}

/// Items of `thread_id` before `position`.
pub fn encode_history(thread_id: ThreadId, before: u64) -> String {
    format!("h1.{}.{before:x}", thread_id.0.simple())
}

pub fn decode_history(cursor: &str, thread_id: ThreadId) -> Result<u64, BadCursor> {
    if cursor.len() > MAX_HISTORY_CURSOR_BYTES {
        return Err(BadCursor("too long"));
    }
    let mut parts = cursor.split('.');
    if parts.next() != Some("h1") {
        return Err(BadCursor("unknown version"));
    }
    let thread = parts.next().ok_or(BadCursor("no thread"))?;
    if thread != thread_id.0.simple().to_string() {
        return Err(BadCursor("belongs to another thread"));
    }
    let before = parts.next().ok_or(BadCursor("no position"))?;
    if parts.next().is_some() {
        return Err(BadCursor("trailing data"));
    }
    u64::from_str_radix(before, 16).map_err(|_| BadCursor("bad position"))
}

/// Archived threads after `(archived_at, thread_id)` in the archive's order.
pub fn encode_archived(archived_at: Timestamp, thread_id: ThreadId) -> String {
    format!("a1.{archived_at:x}.{}", thread_id.0.simple())
}

pub fn decode_archived(cursor: &str) -> Result<(Timestamp, ThreadId), BadCursor> {
    if cursor.len() > MAX_HISTORY_CURSOR_BYTES {
        return Err(BadCursor("too long"));
    }
    let mut parts = cursor.split('.');
    if parts.next() != Some("a1") {
        return Err(BadCursor("unknown version"));
    }
    let archived_at = parts
        .next()
        .and_then(|text| i64::from_str_radix(text, 16).ok())
        .ok_or(BadCursor("bad time"))?;
    let thread_id = parts
        .next()
        .and_then(|text| uuid::Uuid::parse_str(text).ok())
        .map(ThreadId)
        .ok_or(BadCursor("bad thread"))?;
    if parts.next().is_some() {
        return Err(BadCursor("trailing data"));
    }
    Ok((archived_at, thread_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_round_trip_and_stay_bound_to_their_thread() {
        let thread = ThreadId(uuid::Uuid::from_u128(42));
        let other = ThreadId(uuid::Uuid::from_u128(43));
        let cursor = encode_history(thread, 0x0300_0002);
        assert_eq!(decode_history(&cursor, thread), Ok(0x0300_0002));
        assert!(decode_history(&cursor, other).is_err());
        assert!(decode_history("h2.x.1", thread).is_err());
        assert!(decode_history(&format!("{cursor}.1"), thread).is_err());
        let archived = encode_archived(1_700_000_000_000, thread);
        assert_eq!(decode_archived(&archived), Ok((1_700_000_000_000, thread)));
        assert!(decode_archived("a1.zz.0").is_err());
    }
}
