//! The bench daemon over protocol v8: the fixture it serves (project,
//! timeline threads, history threads), the providers it offers, and prompts
//! replayed by the fake CLIs through the orchestrator, interrupts included,
//! up to `--exit-after`.
#![cfg(unix)]

mod common;

use std::time::{Duration, Instant};

use common::{BENCH_TOKEN, Bench, dispatch, sample_timeline, snapshot, wait_settled};
use uuid::Uuid;
use waku_bench_daemon::fixture;
use waku_client::v8::{NoParams, StreamEvent, SubscribeOptions, V8Client, method, stream};
use waku_protocol::orchestration::{
    Command, DispatchMode, DomainEvent, MessageId, RunStatus, ThreadId, TurnItemKind,
};
use waku_protocol::v8::StreamItem;
use waku_protocol::v8::orchestration::{
    ShellItem, ShellLocation, ShellSpec, ThreadItem, ThreadSearchParams, ThreadSpec,
};

fn send(client: &V8Client, thread_id: Uuid, text: &str) {
    dispatch(
        client,
        Command::MessageDispatch {
            thread_id: ThreadId(thread_id),
            message_id: MessageId(Uuid::new_v4()),
            text: text.into(),
            display_text: None,
            attachments: Vec::new(),
            context: Vec::new(),
            model_selection: None,
            mode: DispatchMode::QueueAfterActive,
            title_seed: None,
            source_plan: None,
        },
    );
}

#[test]
fn the_fixture_is_served_over_v8() {
    let timeline = sample_timeline();
    let (mut options, root) = Bench::options(&timeline, "1");
    options.history_turns = 30;
    options.history_sessions = 2;
    let bench = Bench::start(options, root);
    let server = bench.server();

    assert!(
        V8Client::connect(&server.client_address(), "wrong").is_err(),
        "a wrong token is refused"
    );
    let client = bench.connect();

    let projects = client.call::<method::ProjectsList>(NoParams {}).unwrap();
    assert_eq!(
        projects
            .iter()
            .map(|project| project.id.0)
            .collect::<Vec<_>>(),
        [fixture::PROJECT_ID]
    );
    assert_eq!(projects[0].path, server.project_dir);

    // The sidebar: the custom timeline first, every built-in, both history
    // threads.
    let shell = client.subscribe::<stream::Shell>(
        ShellSpec {
            location: ShellLocation::Active,
            project_id: None,
        },
        SubscribeOptions::default(),
    );
    let mut titles = Vec::new();
    loop {
        match shell
            .events()
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
        {
            StreamEvent::Item(StreamItem::Shell(ShellItem::Snapshot {
                threads, complete, ..
            })) => {
                titles.extend(threads.into_iter().map(|row| row.thread.title));
                if complete {
                    break;
                }
            }
            StreamEvent::Item(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    titles.sort();
    let mut expected = waku_bench_daemon::timeline::BUILTIN_NAMES
        .iter()
        .map(|name| format!("Bench · {name}"))
        .chain([
            format!("Bench · {}", timeline.name),
            "Bench · history (30 turns)".to_owned(),
            "Bench · history 2 (30 turns)".to_owned(),
        ])
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(titles, expected);
    assert_eq!(server.timeline_threads.len(), 7);
    assert_eq!(server.timeline_threads[0].0, timeline.name);
    assert_eq!(server.default_thread, fixture::thread_id(&timeline.name));

    // A timeline thread opens with one settled turn.
    let opened = snapshot(&client, server.default_thread);
    assert_eq!(opened.projection.runs.len(), 1);
    assert_eq!(opened.projection.runs[0].status, RunStatus::Completed);
    assert!(!opened.has_more_history);

    // A history thread is long: the snapshot is the newest window, older
    // turns page in.
    let history = snapshot(&client, server.history_threads[0]);
    assert!(history.has_more_history);
    let users = history
        .projection
        .turn_items
        .iter()
        .filter(|item| matches!(item.kind, TurnItemKind::UserMessage { .. }))
        .count();
    assert!(
        (1..=10).contains(&users),
        "{users} user turns in the window"
    );
    assert!(
        history
            .projection
            .runs
            .iter()
            .all(|run| run.status == RunStatus::Completed)
    );
    assert!(
        history.projection.turn_items.iter().any(|item| matches!(
            item.kind,
            TurnItemKind::CommandExecution { .. } | TurnItemKind::FileSearch { .. }
        )),
        "history turns carry tool calls"
    );

    // Search finds the timeline threads by their intro.
    let hits = client
        .call::<method::ThreadsSearch>(ThreadSearchParams {
            query: "timeline".into(),
            ..ThreadSearchParams::default()
        })
        .unwrap();
    assert_eq!(hits.len(), 7, "{hits:#?}");

    // Only the faked providers are offered.
    let providers = client.call::<method::ProvidersList>(NoParams {}).unwrap();
    let mut enabled = providers
        .iter()
        .filter(|status| status.enabled)
        .map(|status| status.instance_id.as_str())
        .collect::<Vec<_>>();
    enabled.sort_unstable();
    assert_eq!(enabled, ["claude", "codex"]);
}

#[test]
fn prompts_replay_interrupts_cancel_and_exit_after_stops() {
    let timeline = sample_timeline();
    // The sample spans 12 ms; at this rate a turn takes 2.4 s.
    let (mut options, root) = Bench::options(&timeline, "0.005");
    options.exit_after = Some(2);
    options.provider = waku_protocol::model::ProviderKind::Claude;
    let mut bench = Bench::start(options, root);
    let thread_id = bench.server().default_thread;
    let client = bench.connect();
    assert_eq!(
        snapshot(&client, thread_id).projection.runs[0]
            .model_selection
            .instance
            .as_str(),
        "claude"
    );

    // Interrupt the first prompt once its answer streams.
    send(&client, thread_id, "go");
    let deadline = Instant::now() + Duration::from_secs(30);
    let run_id = loop {
        let projection = snapshot(&client, thread_id).projection;
        let streaming = projection.runs.iter().find(|run| {
            !run.status.is_terminal()
                && projection.turn_items.iter().any(|item| {
                    item.run_id == Some(run.id)
                        && matches!(item.kind, TurnItemKind::AssistantMessage { .. })
                })
        });
        if let Some(run) = streaming {
            break run.id;
        }
        assert!(Instant::now() < deadline, "the prompt never streamed");
        std::thread::sleep(Duration::from_millis(10));
    };
    dispatch(
        &client,
        Command::RunInterrupt {
            thread_id: ThreadId(thread_id),
            run_id,
            reason: None,
            hold_queue: false,
        },
    );
    let settled = wait_settled(&client, thread_id, 2);
    let interrupted = settled
        .projection
        .runs
        .iter()
        .find(|run| run.id == run_id)
        .unwrap();
    assert_eq!(interrupted.status, RunStatus::Interrupted);

    // The second prompt finishes, which is the second replay: the daemon
    // stops by itself, so the run is followed on the live stream.
    let live = client.subscribe::<stream::Thread>(
        ThreadSpec {
            thread_id: ThreadId(thread_id),
            compact_turn_items: false,
        },
        SubscribeOptions::default(),
    );
    send(&client, thread_id, &timeline.name);
    let mut answer = std::collections::BTreeMap::new();
    let mut completed = None;
    while completed.is_none() {
        match live.events().recv_timeout(Duration::from_secs(30)) {
            Ok(StreamEvent::Item(StreamItem::Thread(ThreadItem::Event { event, .. }))) => {
                match event {
                    DomainEvent::TurnItemUpdated(item) if item.run_id != Some(run_id) => {
                        if let TurnItemKind::AssistantMessage { text, .. } = item.kind {
                            answer.insert(item.position, text);
                        }
                    }
                    DomainEvent::RunUpdated(run)
                        if run.id != run_id && run.status.is_terminal() =>
                    {
                        completed = Some(run.status);
                    }
                    _ => {}
                }
            }
            Ok(StreamEvent::Item(_) | StreamEvent::Resumed { .. }) => {}
            other => panic!("the second run never completed: {other:?}"),
        }
    }
    assert_eq!(completed, Some(RunStatus::Completed));
    assert_eq!(
        answer.into_values().collect::<String>(),
        timeline.assistant_text()
    );
    drop(live);
    drop(client);

    let server = bench.server.take().unwrap();
    let address = server.client_address();
    let reports = server.wait().unwrap();
    assert!(
        V8Client::connect(&address, BENCH_TOKEN).is_err(),
        "the daemon stopped after the second replay"
    );
    assert_eq!(reports.len(), 2, "{reports:#?}");
    let first = reports[0].replay.as_ref().unwrap();
    assert!(first.canceled);
    assert!(first.events < timeline.events.len() as u64);
    assert_eq!(reports[0].status, RunStatus::Interrupted);
    let second = reports[1].replay.as_ref().unwrap();
    assert!(!second.canceled);
    assert_eq!(second.events, timeline.events.len() as u64);
}
