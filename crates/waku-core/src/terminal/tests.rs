//! Daemon terminals through the daemon's backend (#60): replay on attach,
//! several viewers of one terminal, resize, clear, restart, exit and close.
//! The v8 terminal streams are exercised in `v8::catalog`'s tests and the
//! desktop's v8 terminal link in `waku-bench-daemon`'s `desktop_surfaces`
//! test. The shells are `/bin/sh` scripts, so these run on Unix; the PTY
//! code itself is shared with Windows.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use parking_lot::Mutex;
use uuid::Uuid;

use super::{DaemonTerminal, TerminalEvent, TerminalEvents, TerminalSnapshot};
use crate::Backend;
use crate::daemon::WakuBackend;
use crate::settings::DaemonSettingsStore;

const TIMEOUT: Duration = Duration::from_secs(5);

/// A backend whose terminals run `script` in `/bin/sh`.
struct TestDaemon {
    root: PathBuf,
    backend: WakuBackend,
}

impl TestDaemon {
    fn start(script: &str) -> Self {
        let root = std::env::temp_dir().join(format!("waku-terminal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let backend = WakuBackend::new(
            DaemonSettingsStore::open(root.join("settings.json")).unwrap(),
            root.join("data"),
        )
        .unwrap()
        .with_terminal_shell(shell(script));
        Self { root, backend }
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        self.backend.shutdown();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Do not load the developer's or CI runner's login files, prompt plugins,
/// or terminal capability queries in a terminal test.
fn shell(script: &str) -> alacritty_terminal::tty::Shell {
    alacritty_terminal::tty::Shell::new("/bin/sh".into(), vec!["-c".into(), script.into()])
}

/// A terminal's events, fanned out to every viewer that joined so far, the
/// way the v8 terminal broadcasts them to its subscribers.
#[derive(Clone, Default)]
struct Viewers(Arc<Mutex<Vec<Sender<TerminalEvent>>>>);

impl Viewers {
    fn sink(&self) -> TerminalEvents {
        let viewers = self.clone();
        Arc::new(move |event| {
            viewers
                .0
                .lock()
                .retain(|viewer| viewer.send(event.clone()).is_ok());
        })
    }

    fn join(&self) -> Receiver<TerminalEvent> {
        let (sender, receiver) = unbounded();
        self.0.lock().push(sender);
        receiver
    }
}

fn attach(daemon: &TestDaemon, id: Uuid) -> TerminalSnapshot {
    daemon
        .backend
        .attach_terminal(id, None, None, None)
        .unwrap()
}

/// Live output from `events` until `marker` appears, with each chunk's
/// offset.
fn output_until(events: &Receiver<TerminalEvent>, marker: &[u8]) -> (Vec<u8>, Vec<u64>) {
    let deadline = Instant::now() + TIMEOUT;
    let mut output = Vec::new();
    let mut offsets = Vec::new();
    while !contains(&output, marker) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(event) = events.recv_timeout(remaining) else {
            panic!(
                "terminal output never contained {:?}: {:?}",
                String::from_utf8_lossy(marker),
                String::from_utf8_lossy(&output)
            );
        };
        if let TerminalEvent::Output { offset, data } = event {
            offsets.push(offset);
            output.extend(data);
        }
    }
    (output, offsets)
}

fn next_matching(
    events: &Receiver<TerminalEvent>,
    mut matches: impl FnMut(&TerminalEvent) -> bool,
) -> TerminalEvent {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = events
            .recv_timeout(remaining)
            .expect("the expected terminal event never arrived");
        if matches(&event) {
            return event;
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

const ECHO_SHELL: &str = "printf 'ready\\n'\n\
    printf 'query:\\033[6n\\033]11;?\\007:end\\n'\n\
    while IFS= read -r line; do\n\
      if [ \"$line\" = size ]; then stty size; else printf 'received:%s\\n' \"$line\"; fi\n\
    done";

#[test]
fn a_second_viewer_attaches_with_a_stripped_replay_and_shares_the_terminal() {
    let daemon = TestDaemon::start(ECHO_SHELL);
    let viewers = Viewers::default();
    let first = viewers.join();
    let terminal_id = Uuid::new_v4();
    daemon
        .backend
        .open_terminal(terminal_id, &daemon.root, 80, 24, viewers.sink())
        .unwrap();
    // Live output is unfiltered: the attached emulator answers the query.
    let (live, _) = output_until(&first, b":end");
    assert!(contains(&live, b"\x1b[6n"));

    // Resize, then let the shell report the PTY's size.
    daemon
        .backend
        .resize_terminal(terminal_id, 100, 30)
        .unwrap();
    daemon
        .backend
        .write_terminal(terminal_id, b"size\r".to_vec())
        .unwrap();
    output_until(&first, b"30 100");

    // A second viewer attaches and replays the retained tail, with the
    // queries gone.
    let second = viewers.join();
    let snapshot = attach(&daemon, terminal_id);
    let replay = String::from_utf8_lossy(&snapshot.replay).into_owned();
    assert!(replay.contains("ready"), "replay: {replay:?}");
    assert!(replay.contains("query::end"), "replay: {replay:?}");
    assert!(replay.contains("30 100"), "replay: {replay:?}");
    assert!(!snapshot.replay.contains(&0x1b), "replay: {replay:?}");
    assert_eq!((snapshot.cols, snapshot.rows), (100, 30));
    assert_eq!(snapshot.generation, 0);
    assert!(!snapshot.exited);
    assert!(snapshot.offset > 0);

    // Input reaches the one shell, and both viewers see it.
    daemon
        .backend
        .write_terminal(terminal_id, b"from-second\r".to_vec())
        .unwrap();
    let (_, offsets) = output_until(&second, b"received:from-second");
    assert!(
        offsets.iter().all(|offset| *offset >= snapshot.offset),
        "live chunks below the replay offset {}: {offsets:?}",
        snapshot.offset
    );
    output_until(&first, b"received:from-second");

    daemon.backend.close_terminal(terminal_id).unwrap();
    assert!(
        daemon
            .backend
            .attach_terminal(terminal_id, None, None, None)
            .is_err(),
        "a closed terminal still accepted an attach"
    );
}

#[test]
fn clear_and_restart_reset_every_viewer() {
    let daemon = TestDaemon::start(
        "printf 'started\\n'\nwhile IFS= read -r line; do printf 'received:%s\\n' \"$line\"; done",
    );
    let viewers = Viewers::default();
    let events = viewers.join();
    let terminal_id = Uuid::new_v4();
    daemon
        .backend
        .open_terminal(terminal_id, &daemon.root, 80, 24, viewers.sink())
        .unwrap();
    output_until(&events, b"started");

    daemon.backend.clear_terminal(terminal_id).unwrap();
    let TerminalEvent::Reset { generation, offset } = next_matching(&events, |event| {
        matches!(event, TerminalEvent::Reset { .. })
    }) else {
        unreachable!()
    };
    assert_eq!(generation, 0);
    let cleared = attach(&daemon, terminal_id);
    assert!(cleared.replay.is_empty());
    assert!(!cleared.truncated);
    assert_eq!(cleared.offset, offset);

    let restarted = daemon.backend.restart_terminal(terminal_id).unwrap();
    assert_eq!(restarted.generation, 1);
    let TerminalEvent::Reset { generation, .. } = next_matching(&events, |event| {
        matches!(event, TerminalEvent::Reset { .. })
    }) else {
        unreachable!()
    };
    assert_eq!(generation, 1);
    // The fresh shell's output starts at the reset offset.
    let (_, offsets) = output_until(&events, b"started");
    assert_eq!(offsets.first().copied(), Some(restarted.offset));
    daemon
        .backend
        .write_terminal(terminal_id, b"after-restart\r".to_vec())
        .unwrap();
    output_until(&events, b"received:after-restart");
    daemon.backend.close_terminal(terminal_id).unwrap();
}

#[test]
fn an_exited_terminal_keeps_its_scrollback_for_a_late_attach() {
    let daemon = TestDaemon::start("printf 'goodbye\\n'; exit 3");
    let viewers = Viewers::default();
    let events = viewers.join();
    let terminal_id = Uuid::new_v4();
    daemon
        .backend
        .open_terminal(terminal_id, &daemon.root, 80, 24, viewers.sink())
        .unwrap();
    // Output arrives before the exit is reported.
    output_until(&events, b"goodbye");
    next_matching(&events, |event| matches!(event, TerminalEvent::Exited));

    let snapshot = attach(&daemon, terminal_id);
    assert!(snapshot.exited);
    assert_eq!(snapshot.exit_code, Some(3));
    assert!(contains(&snapshot.replay, b"goodbye"));

    // A restart brings the exited terminal back.
    let terminal = daemon.backend.restart_terminal(terminal_id).unwrap();
    assert!(!terminal.exited);
    daemon.backend.close_terminal(terminal_id).unwrap();
}

#[test]
fn dropping_an_idle_terminal_does_not_wait_for_output() {
    let root = std::env::temp_dir().join(format!("waku-terminal-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let terminal = DaemonTerminal::open_with_shell(
        &root,
        80,
        24,
        Arc::new(|_| {}),
        shell("while IFS= read -r line; do :; done"),
    )
    .unwrap();
    let (dropped, finished) = bounded(1);
    std::thread::spawn(move || {
        drop(terminal);
        let _ = dropped.send(());
    });

    assert!(
        finished.recv_timeout(Duration::from_secs(3)).is_ok(),
        "dropping an idle daemon terminal blocked on its output reader"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn input_round_trips_and_close_returns_promptly() {
    round_trip_then_close(false);
}

#[test]
fn close_does_not_wait_for_a_shell_ignoring_hangup() {
    round_trip_then_close(true);
}

fn round_trip_then_close(ignore_hangup: bool) {
    let daemon = TestDaemon::start(&format!(
        "{}\nprintf 'ready:%s\\n' \"$$\"\nwhile IFS= read -r line; do printf 'received:%s\\n' \"$line\"; done",
        if ignore_hangup { "trap '' HUP" } else { ":" },
    ));
    let viewers = Viewers::default();
    let events = viewers.join();
    let terminal_id = Uuid::new_v4();
    daemon
        .backend
        .open_terminal(terminal_id, &daemon.root, 80, 24, viewers.sink())
        .unwrap();
    // Wait until the shell has installed its signal handler. Receiving local
    // echo alone does not prove that shell startup has completed.
    let (ready, _) = output_until(&events, b"\n");
    let child_pid: libc::pid_t = String::from_utf8_lossy(&ready)
        .trim()
        .strip_prefix("ready:")
        .unwrap()
        .parse()
        .unwrap();
    daemon
        .backend
        .write_terminal(terminal_id, b"waku-terminal-round-trip\r".to_vec())
        .unwrap();
    // The response prefix is absent from the input, so a PTY echo cannot
    // satisfy this before the child has actually read it.
    output_until(&events, b"received:waku-terminal-round-trip");

    let daemon = Arc::new(daemon);
    let (closed, finished) = bounded(1);
    let closing = daemon.clone();
    let close = std::thread::spawn(move || {
        let _ = closed.send(closing.backend.close_terminal(terminal_id).is_ok());
    });
    let result = finished.recv_timeout(Duration::from_secs(3));
    if result.is_err() {
        // Clean up the fixture even when closing regresses, and fail here.
        unsafe {
            libc::kill(child_pid, libc::SIGKILL);
        }
    }
    close.join().unwrap();
    assert_eq!(
        result,
        Ok(true),
        "closing the daemon terminal did not finish"
    );
}
