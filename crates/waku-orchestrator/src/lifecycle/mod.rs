//! Server-owned thread lifecycle services (#64): auto-settle and snooze
//! wake.
//!
//! [`LifecycleService`] owns one thread. It subscribes to the committed
//! feed, keeps a deadline per thread in a [`Scheduler`], and sleeps until the
//! earliest deadline or the next event that touches a thread's lifecycle
//! facts (a thread, run, request or provider-thread update). There is no
//! periodic scan: a quiet daemon with nothing due sleeps until the next
//! commit. The wait is capped at [`MAX_WAIT`] only so that a wall-clock jump
//! (the machine slept) is noticed; a capped wake-up reads nothing.
//!
//! It dispatches `AutoSettle { snapshot_at }` (command id
//! `auto-settle:<thread>:<snapshot_at>`) and `SnoozeWake { at }` (command id
//! `snooze-wake:<thread>:<at>`), so a repeated fire is a receipt hit.

pub mod policy;
pub mod scheduler;

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, select, unbounded};
use waku_protocol::orchestration::*;

use crate::command::InternalCommand;
use crate::feed::CommittedFeed;
use crate::orchestrator::Orchestrator;

pub use policy::{
    AutoSettleOverride, AutoSettlePolicy, AutoSettleSettings, PullRequestState, SettlementFacts,
    Verdict, evaluate, is_candidate,
};
pub use scheduler::{Fire, NoPullRequests, PullRequestSource, Scheduler};

/// The longest the service sleeps without a deadline or an event.
pub const MAX_WAIT: Duration = Duration::from_secs(10 * 60);

enum Control {
    Settings(AutoSettleSettings),
    /// A pull request of this thread changed state (a merge event).
    PullRequest(ThreadId),
    /// Re-read the clock (after a system wake, or a test's manual clock).
    Wake,
    Stop,
}

/// The running auto-settle and snooze-wake service.
pub struct LifecycleService {
    control: Sender<Control>,
    handle: Option<JoinHandle<()>>,
}

impl LifecycleService {
    pub fn start(
        orchestrator: Arc<Orchestrator>,
        settings: AutoSettleSettings,
        pull_requests: Arc<dyn PullRequestSource>,
    ) -> Self {
        let (control, receiver) = unbounded();
        // Subscribe before the first scan so no commit falls between them.
        let feed = orchestrator.subscribe();
        let handle = std::thread::Builder::new()
            .name("waku-lifecycle".to_owned())
            .spawn(move || {
                Worker {
                    orchestrator,
                    pull_requests,
                    scheduler: Scheduler::new(settings),
                    control: receiver,
                }
                .run(feed)
            })
            .expect("spawn the lifecycle thread");
        Self {
            control,
            handle: Some(handle),
        }
    }

    pub fn set_settings(&self, settings: AutoSettleSettings) {
        let _ = self.control.send(Control::Settings(settings));
    }

    /// A pull request of `thread_id` merged or closed: look at it now.
    pub fn pull_request_changed(&self, thread_id: ThreadId) {
        let _ = self.control.send(Control::PullRequest(thread_id));
    }

    /// Re-reads the clock and fires anything now due.
    pub fn wake(&self) {
        let _ = self.control.send(Control::Wake);
    }

    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        let _ = self.control.send(Control::Stop);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for LifecycleService {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Worker {
    orchestrator: Arc<Orchestrator>,
    pull_requests: Arc<dyn PullRequestSource>,
    scheduler: Scheduler,
    control: Receiver<Control>,
}

/// Whether an event can change a thread's lifecycle facts.
fn touches_lifecycle(event: &DomainEvent) -> bool {
    matches!(
        event,
        DomainEvent::ThreadUpdated(_)
            | DomainEvent::RunUpdated(_)
            | DomainEvent::RuntimeRequestUpdated(_)
            | DomainEvent::ProviderThreadUpdated(_)
    )
}

impl Worker {
    fn now(&self) -> Timestamp {
        self.orchestrator.clock().now()
    }

    fn reload_all(&mut self) {
        let now = self.now();
        let (scheduler, pull_requests) = (&mut self.scheduler, &*self.pull_requests);
        let _ = self
            .orchestrator
            .reader()
            .with(|connection| scheduler.reload_all(connection, pull_requests, now));
    }

    fn reload(&mut self, threads: &[ThreadId]) {
        let now = self.now();
        let (scheduler, pull_requests) = (&mut self.scheduler, &*self.pull_requests);
        self.orchestrator.reader().with(|connection| {
            for thread_id in threads {
                let _ = scheduler.reload(connection, pull_requests, *thread_id, now);
            }
        });
    }

    fn fire_due(&mut self) {
        let now = self.now();
        let (scheduler, pull_requests) = (&mut self.scheduler, &*self.pull_requests);
        let fires = self
            .orchestrator
            .reader()
            .with(|connection| scheduler.poll(connection, pull_requests, now))
            .unwrap_or_default();
        for fire in fires {
            let (command_id, command) = match fire {
                Fire::Settle {
                    thread_id,
                    snapshot_at,
                } => (
                    format!("auto-settle:{thread_id}:{snapshot_at}"),
                    InternalCommand::AutoSettle {
                        thread_id,
                        snapshot_at,
                    },
                ),
                Fire::Wake { thread_id, at } => (
                    format!("snooze-wake:{thread_id}:{at}"),
                    InternalCommand::SnoozeWake { thread_id, at },
                ),
            };
            let Ok(command_id) = CommandId::new(command_id) else {
                continue;
            };
            // A rejection means the thread moved on; its commit reloads it.
            let _ = self.orchestrator.dispatch_internal(command_id, command);
        }
    }

    fn timeout(&self) -> Duration {
        match self.scheduler.next_due() {
            None => MAX_WAIT,
            Some(due) => {
                let wait = due.saturating_sub(self.now()).max(0) as u64;
                Duration::from_millis(wait).min(MAX_WAIT)
            }
        }
    }

    fn run(mut self, mut feed: CommittedFeed) {
        enum Next {
            Events(Vec<ThreadId>),
            Resubscribe,
            Control(Control),
            Timeout,
            Exit,
        }
        self.reload_all();
        loop {
            self.fire_due();
            let timeout = self.timeout();
            let next = select! {
                recv(feed.receiver()) -> event => match event {
                    Ok(event) => {
                        let mut threads = Vec::new();
                        for event in std::iter::once(event).chain(feed.drain()) {
                            if touches_lifecycle(&event.event)
                                && let Some(thread_id) = event.thread_id
                                && !threads.contains(&thread_id)
                            {
                                threads.push(thread_id);
                            }
                        }
                        Next::Events(threads)
                    }
                    Err(_) if feed.overflowed() => Next::Resubscribe,
                    // The orchestrator shut down.
                    Err(_) => Next::Exit,
                },
                recv(self.control) -> message => match message {
                    Ok(control) => Next::Control(control),
                    Err(_) => Next::Exit,
                },
                default(timeout) => Next::Timeout,
            };
            match next {
                Next::Events(threads) => self.reload(&threads),
                Next::Resubscribe => {
                    feed = self.orchestrator.subscribe();
                    self.reload_all();
                }
                Next::Control(Control::Settings(settings)) => {
                    self.scheduler.set_settings(settings);
                    self.reload_all();
                }
                Next::Control(Control::PullRequest(thread_id)) => self.reload(&[thread_id]),
                Next::Control(Control::Wake) | Next::Timeout => {}
                Next::Control(Control::Stop) | Next::Exit => return,
            }
        }
    }
}
