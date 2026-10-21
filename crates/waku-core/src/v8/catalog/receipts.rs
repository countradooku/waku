//! `opId` receipts (RFC 0003 §2.3): a mutation re-sent with an `opId` the
//! daemon already ran returns the stored result instead of running again.
//!
//! The bounds are the legacy response cache's: 2,048 receipts, 64 MiB of
//! encoded results, 10 minutes. A retry that arrives while the first
//! attempt still runs waits for it. Only successes are stored, so a failed
//! mutation can be retried for real.
//!
//! TODO(Phase 7): receipts are per credential session once sessions exist;
//! until then every connection holds the one local credential.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Notify;
use uuid::Uuid;
use waku_protocol::v8::{Encoding, Error, MethodResult, codec};

const MAX_RECEIPTS: usize = 2_048;
const MAX_RECEIPT_BYTES: usize = 64 * 1024 * 1024;
const RECEIPT_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Default)]
pub(crate) struct Receipts {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    entries: HashMap<Uuid, Entry>,
    /// Stored receipts, oldest first.
    order: VecDeque<(Uuid, Instant)>,
    bytes: usize,
}

enum Entry {
    Running(Arc<Notify>),
    Done {
        result: MethodResult,
        bytes: usize,
        at: Instant,
    },
}

/// The right to run an `opId` once.
pub(crate) struct Ticket {
    op_id: Uuid,
    notify: Arc<Notify>,
    state: Arc<Mutex<State>>,
    finished: bool,
}

impl Drop for Ticket {
    /// A call cancelled before it finished leaves no receipt; a waiting
    /// retry then runs it.
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = self.state.lock();
        if matches!(state.entries.get(&self.op_id), Some(Entry::Running(_))) {
            state.entries.remove(&self.op_id);
        }
        drop(state);
        self.notify.notify_waiters();
    }
}

pub(crate) enum Begin {
    /// Run the method, then [`Receipts::finish`].
    Run(Ticket),
    /// It already ran; this is its result.
    Done(MethodResult),
}

impl Receipts {
    pub(crate) async fn begin(&self, op_id: Uuid) -> Begin {
        loop {
            let waiting = {
                let mut state = self.state.lock();
                expire(&mut state, Instant::now());
                match state.entries.get(&op_id) {
                    Some(Entry::Done { result, .. }) => return Begin::Done(result.clone()),
                    Some(Entry::Running(notify)) => {
                        // Registered under the lock, so `finish` cannot
                        // notify before this waiter listens.
                        let notified = notify.clone().notified_owned();
                        let mut notified = Box::pin(notified);
                        notified.as_mut().enable();
                        notified
                    }
                    None => {
                        let notify = Arc::new(Notify::new());
                        state.entries.insert(op_id, Entry::Running(notify.clone()));
                        return Begin::Run(Ticket {
                            op_id,
                            notify,
                            state: self.state.clone(),
                            finished: false,
                        });
                    }
                }
            };
            waiting.await;
        }
    }

    pub(crate) fn finish(&self, mut ticket: Ticket, outcome: &Result<MethodResult, Error>) {
        ticket.finished = true;
        let mut state = self.state.lock();
        match outcome {
            Ok(result) => {
                let bytes =
                    codec::to_vec(Encoding::MessagePack, result).map_or(0, |encoded| encoded.len());
                let at = Instant::now();
                state.entries.insert(
                    ticket.op_id,
                    Entry::Done {
                        result: result.clone(),
                        bytes,
                        at,
                    },
                );
                state.order.push_back((ticket.op_id, at));
                state.bytes += bytes;
                while state.order.len() > MAX_RECEIPTS || state.bytes > MAX_RECEIPT_BYTES {
                    let Some((oldest, _)) = state.order.pop_front() else {
                        break;
                    };
                    remove(&mut state, oldest);
                }
            }
            Err(_) => {
                state.entries.remove(&ticket.op_id);
            }
        }
        drop(state);
        ticket.notify.notify_waiters();
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state.lock().order.len()
    }
}

fn remove(state: &mut State, op_id: Uuid) {
    if let Some(Entry::Done { bytes, .. }) = state.entries.get(&op_id) {
        state.bytes -= bytes;
        state.entries.remove(&op_id);
    }
}

fn expire(state: &mut State, now: Instant) {
    while let Some(&(oldest, at)) = state.order.front() {
        if now.duration_since(at) < RECEIPT_TTL {
            break;
        }
        state.order.pop_front();
        // A newer receipt for the same id keeps its own place in `order`.
        if matches!(state.entries.get(&oldest), Some(Entry::Done { at: stored, .. }) if *stored == at)
        {
            remove(state, oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use waku_protocol::v8::{Ack, ErrorCode};

    use super::*;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn a_retried_op_id_returns_the_stored_result_and_failures_rerun() {
        runtime().block_on(async {
            let receipts = Receipts::default();
            let op = Uuid::from_u128(1);
            let Begin::Run(ticket) = receipts.begin(op).await else {
                panic!("the first attempt runs");
            };
            // A concurrent retry waits for the first attempt.
            let waiting = receipts.begin(op);
            let result = MethodResult::TerminalClose(Ack {});
            receipts.finish(ticket, &Ok(result.clone()));
            assert!(matches!(waiting.await, Begin::Done(done) if done == result));
            assert_eq!(receipts.len(), 1);

            let failed = Uuid::from_u128(2);
            let Begin::Run(ticket) = receipts.begin(failed).await else {
                panic!("runs");
            };
            receipts.finish(ticket, &Err(Error::new(ErrorCode::Io, "disk full")));
            assert!(matches!(receipts.begin(failed).await, Begin::Run(_)));
        });
    }

    #[test]
    fn receipts_are_bounded() {
        runtime().block_on(async {
            let receipts = Receipts::default();
            for index in 0..(MAX_RECEIPTS as u128 + 10) {
                let Begin::Run(ticket) = receipts.begin(Uuid::from_u128(index)).await else {
                    panic!("fresh ids run");
                };
                receipts.finish(ticket, &Ok(MethodResult::TerminalClose(Ack {})));
            }
            assert_eq!(receipts.len(), MAX_RECEIPTS);
            assert!(matches!(
                receipts.begin(Uuid::from_u128(0)).await,
                Begin::Run(_)
            ));
        });
    }
}
