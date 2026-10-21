//! Daemon terminals over a real WebSocket daemon (#60): replay on attach,
//! several attached clients, resize, clear, restart and exit. The desktop's
//! v8 terminal link is exercised in `waku-bench-daemon`'s
//! `desktop_surfaces` test. The shells are `/bin/sh` scripts, so these run on Unix;
//! the PTY code itself is shared with Windows.

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use base64::Engine as _;
use crossbeam_channel::Receiver;
use uuid::Uuid;
use waku_client::DaemonClient;
use waku_protocol::{Command, ResponsePayload, SequencedEvent, TerminalSnapshot, WireDriverEvent};

use crate::daemon::WakuBackend;
use crate::persistence::StateStore;
use crate::settings::DaemonSettingsStore;
use crate::{ServerOptions, ShutdownSignal, serve};

const TIMEOUT: Duration = Duration::from_secs(5);

struct TestDaemon {
    root: PathBuf,
    address: String,
    shutdown: ShutdownSignal,
    server: Option<JoinHandle<()>>,
}

impl TestDaemon {
    fn start(script: &str) -> Self {
        let root = std::env::temp_dir().join(format!("waku-terminal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        // Do not load the developer's or CI runner's login files, prompt
        // plugins, or terminal capability queries in a transport test.
        let shell =
            alacritty_terminal::tty::Shell::new("/bin/sh".into(), vec!["-c".into(), script.into()]);
        let backend = WakuBackend::new(
            DaemonSettingsStore::open(root.join("settings.json")).unwrap(),
            StateStore::daemon(root.join("app.db")),
        )
        .unwrap()
        .with_terminal_shell(shell);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let shutdown = ShutdownSignal::new();
        let server_shutdown = shutdown.clone();
        let server = std::thread::spawn(move || {
            serve(
                listener,
                "secret".into(),
                Arc::new(backend),
                server_shutdown,
                ServerOptions::default(),
            )
            .unwrap()
        });
        Self {
            root,
            address,
            shutdown,
            server: Some(server),
        }
    }

    fn connect(&self) -> DaemonClient {
        DaemonClient::connect(&self.address, "secret".into()).unwrap()
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        self.shutdown.trigger();
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn request(client: &DaemonClient, terminal_id: Uuid, command: Command) -> ResponsePayload {
    client.request(terminal_id, terminal_id, command).unwrap()
}

fn attach(client: &DaemonClient, terminal_id: Uuid, size: Option<(u16, u16)>) -> TerminalSnapshot {
    match request(
        client,
        terminal_id,
        Command::AttachTerminal {
            cols: size.map(|(cols, _)| cols),
            rows: size.map(|(_, rows)| rows),
            max_replay_bytes: None,
        },
    ) {
        ResponsePayload::TerminalAttached { terminal } => terminal,
        other => panic!("unexpected attach response: {other:?}"),
    }
}

/// Live output from `events` until `marker` appears, with each chunk's
/// offset.
fn output_until(events: &Receiver<SequencedEvent>, marker: &[u8]) -> (Vec<u8>, Vec<u64>) {
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
        if let WireDriverEvent::TerminalOutput(chunk) = event.event {
            offsets.extend(chunk.offset);
            output.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(&chunk.data)
                    .unwrap(),
            );
        }
    }
    (output, offsets)
}

fn next_matching(
    events: &Receiver<SequencedEvent>,
    mut matches: impl FnMut(&WireDriverEvent) -> bool,
) -> WireDriverEvent {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = events
            .recv_timeout(remaining)
            .expect("the expected terminal event never arrived");
        if matches(&event.event) {
            return event.event;
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
fn a_second_client_attaches_with_a_stripped_replay_and_shares_the_terminal() {
    let daemon = TestDaemon::start(ECHO_SHELL);
    let first = daemon.connect();
    let terminal_id = Uuid::new_v4();
    let first_events = first.subscribe(terminal_id, terminal_id);
    request(
        &first,
        terminal_id,
        Command::OpenTerminal {
            cwd: daemon.root.clone(),
            cols: 80,
            rows: 24,
        },
    );
    // Live output is unfiltered: the attached emulator answers the query.
    let (live, _) = output_until(&first_events, b":end");
    assert!(contains(&live, b"\x1b[6n"));

    // Resize, then let the shell report the PTY's size.
    request(
        &first,
        terminal_id,
        Command::ResizeTerminal {
            cols: 100,
            rows: 30,
        },
    );
    request(
        &first,
        terminal_id,
        Command::WriteTerminal {
            data: b"size\r".to_vec(),
        },
    );
    output_until(&first_events, b"30 100");

    // A second client attaches and replays the retained tail, with the
    // queries gone.
    let second = daemon.connect();
    let second_events = second.subscribe(terminal_id, terminal_id);
    let snapshot = attach(&second, terminal_id, None);
    let replay = String::from_utf8_lossy(&snapshot.replay).into_owned();
    assert!(replay.contains("ready"), "replay: {replay:?}");
    assert!(replay.contains("query::end"), "replay: {replay:?}");
    assert!(replay.contains("30 100"), "replay: {replay:?}");
    assert!(!snapshot.replay.contains(&0x1b), "replay: {replay:?}");
    assert_eq!((snapshot.cols, snapshot.rows), (100, 30));
    assert_eq!(snapshot.generation, 0);
    assert!(!snapshot.exited);
    assert!(snapshot.offset > 0);

    // Input from either client reaches the one shell, and both see it.
    request(
        &second,
        terminal_id,
        Command::WriteTerminal {
            data: b"from-second\r".to_vec(),
        },
    );
    let (_, offsets) = output_until(&second_events, b"received:from-second");
    assert!(
        offsets.iter().all(|offset| *offset >= snapshot.offset),
        "live chunks below the replay offset {}: {offsets:?}",
        snapshot.offset
    );
    output_until(&first_events, b"received:from-second");
    request(
        &first,
        terminal_id,
        Command::WriteTerminal {
            data: b"from-first\r".to_vec(),
        },
    );
    output_until(&second_events, b"received:from-first");

    request(&first, terminal_id, Command::CloseTerminal);
    assert!(
        second
            .request(
                terminal_id,
                terminal_id,
                Command::AttachTerminal {
                    cols: None,
                    rows: None,
                    max_replay_bytes: None,
                },
            )
            .is_err(),
        "a closed terminal still accepted an attach"
    );
}

#[test]
fn clear_and_restart_reset_every_attached_client() {
    let daemon = TestDaemon::start(
        "printf 'started\\n'\nwhile IFS= read -r line; do printf 'received:%s\\n' \"$line\"; done",
    );
    let client = daemon.connect();
    let terminal_id = Uuid::new_v4();
    let events = client.subscribe(terminal_id, terminal_id);
    request(
        &client,
        terminal_id,
        Command::OpenTerminal {
            cwd: daemon.root.clone(),
            cols: 80,
            rows: 24,
        },
    );
    output_until(&events, b"started");

    request(&client, terminal_id, Command::ClearTerminal);
    let WireDriverEvent::TerminalReset(reset) = next_matching(&events, |event| {
        matches!(event, WireDriverEvent::TerminalReset(_))
    }) else {
        unreachable!()
    };
    assert_eq!(reset.generation, 0);
    let cleared = attach(&client, terminal_id, None);
    assert!(cleared.replay.is_empty());
    assert!(!cleared.truncated);
    assert_eq!(cleared.offset, reset.offset);

    let ResponsePayload::TerminalAttached {
        terminal: restarted,
    } = request(&client, terminal_id, Command::RestartTerminal)
    else {
        panic!("restart did not return the terminal");
    };
    assert_eq!(restarted.generation, 1);
    let WireDriverEvent::TerminalReset(reset) = next_matching(&events, |event| {
        matches!(event, WireDriverEvent::TerminalReset(_))
    }) else {
        unreachable!()
    };
    assert_eq!(reset.generation, 1);
    // The fresh shell's output starts at the reset offset.
    let (_, offsets) = output_until(&events, b"started");
    assert_eq!(offsets.first().copied(), Some(restarted.offset));
    request(
        &client,
        terminal_id,
        Command::WriteTerminal {
            data: b"after-restart\r".to_vec(),
        },
    );
    output_until(&events, b"received:after-restart");
    request(&client, terminal_id, Command::CloseTerminal);
}

#[test]
fn an_exited_terminal_keeps_its_scrollback_for_a_late_attach() {
    let daemon = TestDaemon::start("printf 'goodbye\\n'; exit 3");
    let client = daemon.connect();
    let terminal_id = Uuid::new_v4();
    let events = client.subscribe(terminal_id, terminal_id);
    request(
        &client,
        terminal_id,
        Command::OpenTerminal {
            cwd: daemon.root.clone(),
            cols: 80,
            rows: 24,
        },
    );
    next_matching(&events, |event| {
        matches!(event, WireDriverEvent::TerminalExited)
    });

    let late = daemon.connect();
    let snapshot = attach(&late, terminal_id, None);
    assert!(snapshot.exited);
    assert_eq!(snapshot.exit_code, Some(3));
    assert!(contains(&snapshot.replay, b"goodbye"));

    // A restart brings the exited terminal back.
    let ResponsePayload::TerminalAttached { terminal } =
        request(&late, terminal_id, Command::RestartTerminal)
    else {
        panic!("restart did not return the terminal");
    };
    assert!(!terminal.exited);
    request(&late, terminal_id, Command::CloseTerminal);
}
