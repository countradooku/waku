//! #54, #56, #57: bounded snapshots, history paging, the replay decision,
//! the wire projection under a tool storm, the shell, and the output hub
//! under a flood.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use waku_protocol::orchestration::*;
use waku_protocol::v8::StreamCursor;
use waku_protocol::v8::codec::{Encoding, to_vec};
use waku_protocol::v8::orchestration::{
    SNAPSHOT_BYTES, ShellItem, ShellLocation, ShellSpec, ThreadItem, ThreadSnapshot,
};

use super::common::*;
use crate::decide::Decision;
use crate::store::sink::WriteGuard;
use crate::streams::seed::{SeedOptions, seed_history, seed_thread};
use crate::streams::shell::{
    ShellChange, ShellFilter, ShellResume, archived_page, plan_shell_resume, shell_change,
    shell_rows, snapshot_items,
};
use crate::streams::thread::{
    ReplayBudget, SnapshotOptions, ThreadResume, WindowBudget, history_page, plan_thread_resume,
    thread_snapshot, turn_item,
};
use crate::streams::{CommandOutputHub, OutputHubConfig};

fn snapshot(env: &Env, thread_id: ThreadId) -> ThreadSnapshot {
    snapshot_with(env, thread_id, SnapshotOptions::default())
}

fn snapshot_with(env: &Env, thread_id: ThreadId, options: SnapshotOptions) -> ThreadSnapshot {
    let store = env.orchestrator.store_id();
    env.orchestrator
        .reader()
        .with(|connection| thread_snapshot(connection, store, thread_id, options))
        .unwrap()
        .expect("the thread exists")
}

/// Bytes of the `snapshot` stream item in both encodings.
fn wire_sizes(snapshot: &ThreadSnapshot) -> (usize, usize) {
    let item = ThreadItem::Snapshot(snapshot.clone());
    (
        to_vec(Encoding::Json, &item).unwrap().len(),
        to_vec(Encoding::MessagePack, &item).unwrap().len(),
    )
}

fn seeded(env: &Env, name: &str, options: SeedOptions) -> ThreadId {
    let (_, thread_id) = seed_thread(&env.orchestrator, name).unwrap();
    seed_history(&env.orchestrator, thread_id, options).unwrap();
    thread_id
}

#[test]
fn opening_a_5000_message_thread_transfers_at_most_about_a_mebibyte() {
    let env = Env::new();
    // 2,500 turns: 5,000 messages and 5,000 turn items.
    let thread_id = seeded(
        &env,
        "long",
        SeedOptions {
            turns: 2_500,
            batch: 250,
            ..SeedOptions::default()
        },
    );
    assert_eq!(env.count("p_messages"), 5_000);
    let started = Instant::now();
    let snapshot = snapshot(&env, thread_id);
    let elapsed = started.elapsed();
    let (json, msgpack) = wire_sizes(&snapshot);
    eprintln!(
        "5,000-message thread: snapshot {json} B JSON, {msgpack} B MessagePack, {} items, {} runs, read in {elapsed:?}",
        snapshot.projection.turn_items.len(),
        snapshot.projection.runs.len()
    );
    assert!(json <= SNAPSHOT_BYTES + 64 * 1024, "{json}");
    assert!(msgpack < json);
    // Ten user turns: the window starts at a user message.
    let users = snapshot
        .projection
        .turn_items
        .iter()
        .filter(|item| matches!(item.kind, TurnItemKind::UserMessage { .. }))
        .count();
    assert_eq!(users, 10);
    assert_eq!(snapshot.projection.turn_items.len(), 20);
    assert_eq!(snapshot.projection.runs.len(), 10);
    assert!(snapshot.has_more_history && snapshot.history.is_some());
    assert!(!snapshot.budget_exceeded);
    // No duplicate message table: finished runs' messages stay out.
    assert!(snapshot.projection.messages.is_empty());
    assert_eq!(
        snapshot.cursor,
        StreamCursor::Sequence {
            store: env.orchestrator.store_id(),
            seq: env.orchestrator.last_sequence(),
        }
    );

    // Huge messages hit the byte budget instead; one row always fits.
    let huge = seeded(
        &env,
        "huge",
        SeedOptions {
            turns: 40,
            assistant_text_bytes: 300 * 1024,
            ..SeedOptions::default()
        },
    );
    let snapshot = self::snapshot(&env, huge);
    let (json, _) = wire_sizes(&snapshot);
    assert!(snapshot.budget_exceeded && snapshot.has_more_history);
    assert!(json <= SNAPSHOT_BYTES + 64 * 1024, "{json}");
    let oversized = seeded(
        &env,
        "oversized",
        SeedOptions {
            turns: 3,
            assistant_text_bytes: 2 * SNAPSHOT_BYTES,
            ..SeedOptions::default()
        },
    );
    let snapshot = self::snapshot(&env, oversized);
    assert_eq!(snapshot.projection.turn_items.len(), 1);
    assert!(snapshot.budget_exceeded && snapshot.has_more_history);
}

#[test]
fn the_window_counts_items_and_user_turns() {
    let env = Env::new();
    let thread_id = seeded(
        &env,
        "tools",
        SeedOptions {
            turns: 30,
            tool_calls_per_turn: 8,
            tool_output_bytes: 64,
            ..SeedOptions::default()
        },
    );
    // 10 items per turn: the 75-item cap binds before ten user turns.
    let snapshot = snapshot(&env, thread_id);
    assert_eq!(snapshot.projection.turn_items.len(), 75);
    let positions: Vec<u64> = snapshot
        .projection
        .turn_items
        .iter()
        .map(|item| item.position)
        .collect();
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));

    let small = snapshot_with(
        &env,
        thread_id,
        SnapshotOptions {
            compact: false,
            budget: WindowBudget {
                user_turns: 2,
                ..WindowBudget::default()
            },
        },
    );
    assert_eq!(small.projection.turn_items.len(), 20);
    assert!(matches!(
        small.projection.turn_items[0].kind,
        TurnItemKind::UserMessage { .. }
    ));
}

#[test]
fn history_pages_return_every_item_once_in_order() {
    let env = Env::new();
    let thread_id = seeded(
        &env,
        "paged",
        SeedOptions {
            turns: 120,
            tool_calls_per_turn: 3,
            tool_output_bytes: 4_000,
            ..SeedOptions::default()
        },
    );
    let budget = WindowBudget::default();
    let snapshot = snapshot(&env, thread_id);
    let mut seen: Vec<u64> = snapshot
        .projection
        .turn_items
        .iter()
        .map(|item| item.position)
        .collect();
    let mut cursor = snapshot.history.clone();
    let mut pages = 0;
    while let Some(next) = cursor {
        let page = env
            .orchestrator
            .reader()
            .with(|connection| history_page(connection, thread_id, &next, budget))
            .unwrap();
        pages += 1;
        assert!(!page.items.is_empty() && page.items.len() <= budget.items);
        // Each page carries the runs its items belong to.
        let runs: BTreeSet<RunId> = page.runs.iter().map(|run| run.id).collect();
        assert!(
            page.items
                .iter()
                .all(|item| runs.contains(&item.run_id.unwrap()))
        );
        let mut positions: Vec<u64> = page.items.iter().map(|item| item.position).collect();
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(positions.last() < seen.first());
        positions.extend(seen);
        seen = positions;
        cursor = page.next;
    }
    assert_eq!(seen.len() as i64, env.count("p_turn_items"));
    assert_eq!(seen.len(), seen.iter().collect::<BTreeSet<_>>().len());
    assert!(pages >= 7, "{pages}");
    // A cursor of another thread is refused.
    let other = seeded(&env, "other", SeedOptions::default());
    let refused = env.orchestrator.reader().with(|connection| {
        history_page(
            connection,
            other,
            snapshot.history.as_ref().unwrap(),
            budget,
        )
    });
    assert!(refused.is_err());
}

#[test]
fn turn_item_serves_the_full_body() {
    let env = Env::new();
    let thread_id = seeded(
        &env,
        "full",
        SeedOptions {
            turns: 1,
            tool_calls_per_turn: 1,
            tool_output_bytes: 100_000,
            ..SeedOptions::default()
        },
    );
    let snapshot = snapshot(&env, thread_id);
    let tool = snapshot
        .projection
        .turn_items
        .iter()
        .find(|item| matches!(item.kind, TurnItemKind::CommandExecution { .. }))
        .unwrap();
    let TurnItemKind::CommandExecution {
        output,
        output_omitted,
        ..
    } = &tool.kind
    else {
        unreachable!()
    };
    assert!(output.is_none() && *output_omitted);
    let full = env
        .orchestrator
        .reader()
        .with(|connection| turn_item(connection, thread_id, tool.id))
        .unwrap()
        .unwrap();
    let TurnItemKind::CommandExecution { output, .. } = &full.kind else {
        unreachable!()
    };
    assert_eq!(output.as_ref().unwrap().len(), 100_000);
    let other = seeded(&env, "elsewhere", SeedOptions::default());
    assert!(
        env.orchestrator
            .reader()
            .with(|connection| turn_item(connection, other, tool.id))
            .unwrap()
            .is_none()
    );
}

#[test]
fn a_tool_storm_streams_the_same_bytes_whatever_the_output_size() {
    let env = Env::new();
    let storm = |name: &str, output: usize| {
        seeded(
            &env,
            name,
            SeedOptions {
                turns: 50,
                tool_calls_per_turn: 20,
                tool_output_bytes: output,
                ..SeedOptions::default()
            },
        )
    };
    let small = storm("storm-small", 10);
    let large = storm("storm-large", 256 * 1024);
    let small_snapshot = snapshot(&env, small);
    let large_snapshot = snapshot(&env, large);
    let (small_json, small_msgpack) = wire_sizes(&small_snapshot);
    let (large_json, large_msgpack) = wire_sizes(&large_snapshot);
    eprintln!(
        "tool storm snapshot: {small_json} B with 10 B outputs, {large_json} B with 256 KiB outputs"
    );
    // Same structure, same ids' lengths: identical sizes.
    assert_eq!(small_json, large_json);
    assert_eq!(small_msgpack, large_msgpack);
    assert!(!large_snapshot.budget_exceeded);

    // Replay of a storm: the same.
    let replay = |thread_id: ThreadId, from: StreamCursor| {
        env.orchestrator.reader().with(|connection| {
            plan_thread_resume(
                connection,
                env.orchestrator.store_id(),
                thread_id,
                &from,
                ReplayBudget {
                    events: 10_000,
                    bytes: usize::MAX,
                },
            )
        })
    };
    let before = StreamCursor::Sequence {
        store: env.orchestrator.store_id(),
        seq: env.orchestrator.last_sequence(),
    };
    let options = |output| SeedOptions {
        turns: 5,
        first_ordinal: 51,
        tool_calls_per_turn: 20,
        tool_output_bytes: output,
        ..SeedOptions::default()
    };
    seed_history(&env.orchestrator, small, options(10)).unwrap();
    seed_history(&env.orchestrator, large, options(256 * 1024)).unwrap();
    let size = |resume: ThreadResume| match resume {
        ThreadResume::Replay { events, .. } => events
            .iter()
            .map(|(seq, event)| {
                to_vec(
                    Encoding::MessagePack,
                    &ThreadItem::Event {
                        seq: *seq,
                        event: event.clone(),
                    },
                )
                .unwrap()
                .len()
            })
            .sum::<usize>(),
        ThreadResume::Snapshot => panic!("expected a replay"),
    };
    let small_replay = size(replay(small, before.clone()).unwrap());
    let large_replay = size(replay(large, before).unwrap());
    // Sequence numbers differ in width at most by a byte per event.
    assert!(
        small_replay.abs_diff(large_replay) <= 5 * 23,
        "{small_replay} vs {large_replay}"
    );
}

fn commit_items(env: &Env, thread_id: ThreadId, count: usize, bytes: usize, first_ordinal: u32) {
    seed_history(
        &env.orchestrator,
        thread_id,
        SeedOptions {
            turns: count,
            first_ordinal,
            assistant_text_bytes: bytes,
            batch: 1,
            ..SeedOptions::default()
        },
    )
    .unwrap();
}

#[test]
fn resume_replays_small_gaps_and_snapshots_large_ones() {
    let env = Env::new();
    let thread_id = seeded(&env, "resume", SeedOptions::default());
    let store = env.orchestrator.store_id();
    let at = |seq| StreamCursor::Sequence { store, seq };
    let plan = |after: &StreamCursor| {
        env.orchestrator.reader().with(|connection| {
            plan_thread_resume(connection, store, thread_id, after, ReplayBudget::default())
        })
    };
    let cursor = at(env.orchestrator.last_sequence());
    assert_eq!(
        plan(&cursor).unwrap(),
        ThreadResume::Replay {
            events: Vec::new(),
            head: env.orchestrator.last_sequence(),
        }
    );
    // 5 events per seeded turn: 25 turns = 125 events, then 3 more.
    commit_items(&env, thread_id, 25, 100, 11);
    let ThreadResume::Replay { events, head } = plan(&cursor).unwrap() else {
        panic!("125 events replay");
    };
    assert_eq!(events.len(), 125);
    assert_eq!(head, env.orchestrator.last_sequence());
    assert!(events.windows(2).all(|pair| pair[0].0 < pair[1].0));
    // Other threads' events do not count.
    seeded(
        &env,
        "noise",
        SeedOptions {
            turns: 40,
            ..SeedOptions::default()
        },
    );
    assert!(matches!(
        plan(&cursor).unwrap(),
        ThreadResume::Replay { .. }
    ));
    // Exactly 128 events still replay; 129 do not.
    env.ingest(
        thread_id,
        Decision {
            events: (0..3)
                .map(|_| {
                    let projection = env.projection(thread_id);
                    DomainEvent::ThreadUpdated(ThreadUpdate {
                        change: ThreadChange::Renamed,
                        thread: projection.thread.clone(),
                    })
                })
                .collect(),
            ..Decision::default()
        },
        WriteGuard::None,
    );
    let ThreadResume::Replay { events, .. } = plan(&cursor).unwrap() else {
        panic!("128 events replay");
    };
    assert_eq!(events.len(), 128);
    commit_items(&env, thread_id, 1, 100, 36);
    assert_eq!(plan(&cursor).unwrap(), ThreadResume::Snapshot);

    // Over 1 MiB of payload: snapshot, even for two events.
    let cursor = at(env.orchestrator.last_sequence());
    commit_items(&env, thread_id, 1, 600 * 1024, 37);
    assert_eq!(plan(&cursor).unwrap(), ThreadResume::Snapshot);

    // Another store, a cursor from the future, or another cursor kind.
    let foreign = StreamCursor::Sequence {
        store: uuid::Uuid::from_u128(9),
        seq: 1,
    };
    assert_eq!(plan(&foreign).unwrap(), ThreadResume::Snapshot);
    assert_eq!(
        plan(&at(u64::from(u32::MAX))).unwrap(),
        ThreadResume::Snapshot
    );
    assert_eq!(
        plan(&StreamCursor::Revision { revision: 1 }).unwrap(),
        ThreadResume::Snapshot
    );
}

#[test]
fn pending_requests_are_always_in_the_snapshot() {
    let env = Env::new();
    let project = env.project();
    let thread_id = env.thread(project);
    let run_id = env.send(thread_id, "go", DispatchMode::Auto);
    go_running(&env, thread_id, run_id).unwrap();
    let (_, request_id) = request_approval(&env, thread_id, run_id).unwrap();
    // A hundred newer rows push the request out of the window.
    let projection = env.projection(thread_id);
    let run = projection.runs[&run_id].clone();
    let block = run.ordinal.unwrap();
    let next = projection.next_position[&block];
    let events = (0..100)
        .map(|n| {
            DomainEvent::TurnItemUpdated(TurnItem {
                id: fresh(),
                thread_id,
                run_id: Some(run_id),
                node_id: None,
                provider_thread_id: None,
                provider_turn_id: None,
                native_item_ref: None,
                parent_item_id: None,
                position: TurnItem::position_of(block, next + n),
                status: ItemStatus::Completed,
                title: None,
                tool: None,
                stream_rev: 0,
                started_at: None,
                completed_at: None,
                updated_at: START,
                kind: TurnItemKind::SystemNotice {
                    message: format!("row {n}"),
                },
            })
        })
        .collect();
    env.ingest(
        thread_id,
        Decision {
            events,
            ..Decision::default()
        },
        WriteGuard::None,
    );
    let snapshot = snapshot(&env, thread_id);
    let projection = &snapshot.projection;
    assert!(
        projection
            .runtime_requests
            .iter()
            .any(|request| request.id == request_id)
    );
    assert!(projection.turn_items.iter().any(|item| matches!(
        &item.kind,
        TurnItemKind::ApprovalRequest { request_id: id, .. } if *id == request_id
    )));
    // 75 window rows plus the required one.
    assert_eq!(projection.turn_items.len(), 76);
    // The running run comes with its control rows and messages.
    assert!(projection.runs.iter().any(|run| run.id == run_id));
    assert!(!projection.nodes.is_empty() && !projection.run_attempts.is_empty());
    assert!(!projection.provider_threads.is_empty());
    assert!(
        projection
            .messages
            .iter()
            .any(|message| message.run_id == Some(run_id))
    );
    // With compactTurnItems, a message an item already shows is left out.
    let compact = snapshot_with(
        &env,
        thread_id,
        SnapshotOptions {
            compact: true,
            budget: WindowBudget {
                items: 200,
                ..WindowBudget::default()
            },
        },
    );
    assert!(compact.projection.messages.len() < projection.messages.len());
}

#[test]
fn the_shell_holds_navigation_rows_and_resumes_with_deltas() {
    let env = Env::new();
    let project = env.project();
    let first = env.thread(project);
    let second = env.thread(project);
    let store = env.orchestrator.store_id();
    let rows = env
        .orchestrator
        .reader()
        .with(|connection| shell_rows(connection, store, &ShellSpec::default()))
        .unwrap();
    assert_eq!(rows.threads.len(), 2);
    assert_eq!(rows.projects.len(), 1);
    let cursor = rows.cursor.clone();
    let items = snapshot_items(rows);
    assert!(matches!(
        items.as_slice(),
        [ShellItem::Snapshot { complete: true, .. }]
    ));
    let mut filter = ShellFilter::new(ShellSpec::default());
    filter.seed(&items[0]);

    // Archiving moves the row out of the active location.
    env.ok(Command::ThreadArchive { thread_id: first });
    let archived_event = StoredEvent {
        sequence: env.orchestrator.last_sequence(),
        event_id: fresh(),
        command_id: None,
        project_id: Some(project),
        thread_id: Some(first),
        occurred_at: START,
        event: DomainEvent::ThreadUpdated(ThreadUpdate {
            change: ThreadChange::Archived,
            thread: env.projection(first).thread.clone(),
        }),
    };
    let change = env
        .orchestrator
        .reader()
        .with(|connection| shell_change(connection, &archived_event))
        .unwrap()
        .unwrap();
    let ShellChange::Thread { row, .. } = &change else {
        panic!("{change:?}")
    };
    assert_eq!(row.location(), Some(ShellLocation::Archived));
    let Some(ShellItem::ThreadUpdated { location, .. }) = filter.admit(&change) else {
        panic!("the active shell hears the row leave");
    };
    assert_eq!(location, ShellLocation::Archived);
    assert_eq!(filter.admit(&change), None, "already gone");
    let mut archive = ShellFilter::new(ShellSpec {
        location: ShellLocation::Archived,
        project_id: None,
    });
    assert!(matches!(
        archive.admit(&change),
        Some(ShellItem::ThreadUpdated { .. })
    ));
    assert_eq!(archive.admit(&change), None, "unchanged rows are skipped");

    // Resume: one change per touched row, at its newest state.
    env.ok(Command::ThreadRename {
        thread_id: second,
        title: "renamed".into(),
    });
    env.ok(Command::ThreadRename {
        thread_id: second,
        title: "renamed again".into(),
    });
    let ShellResume::Changes { changes, head } = env
        .orchestrator
        .reader()
        .with(|connection| plan_shell_resume(connection, store, &cursor, 4096))
        .unwrap()
    else {
        panic!("a small gap resumes with deltas");
    };
    assert_eq!(head, env.orchestrator.last_sequence());
    assert_eq!(changes.len(), 2);
    let ShellChange::Thread { row, .. } = &changes[1] else {
        panic!()
    };
    assert_eq!(row.thread.title, "renamed again");
    let mut resumed = ShellFilter::resumed(ShellSpec::default());
    assert!(matches!(
        resumed.admit(&changes[0]),
        Some(ShellItem::ThreadUpdated {
            location: ShellLocation::Archived,
            ..
        })
    ));
    assert!(matches!(
        env.orchestrator
            .reader()
            .with(|connection| plan_shell_resume(connection, store, &cursor, 1))
            .unwrap(),
        ShellResume::Snapshot
    ));

    // The archive pages.
    let archived = env
        .orchestrator
        .reader()
        .with(|connection| archived_page(connection, None, Some(1), None))
        .unwrap();
    assert_eq!(archived.rows.len(), 1);
    assert_eq!(archived.rows[0].thread.id, first);
    assert!(archived.next.is_none());
}

#[test]
fn split_shell_snapshots_mark_only_the_last_complete() {
    let env = Env::new();
    let project = env.project();
    for _ in 0..30 {
        env.thread(project);
    }
    let rows = env
        .orchestrator
        .reader()
        .with(|connection| {
            shell_rows(
                connection,
                env.orchestrator.store_id(),
                &ShellSpec::default(),
            )
        })
        .unwrap();
    let items = crate::streams::shell::split_snapshot(rows, 4 * 1024);
    assert!(items.len() > 2, "{}", items.len());
    let completes: Vec<bool> = items
        .iter()
        .map(|item| match item {
            ShellItem::Snapshot { complete, .. } => *complete,
            _ => panic!(),
        })
        .collect();
    assert_eq!(completes.iter().filter(|complete| **complete).count(), 1);
    assert!(*completes.last().unwrap());
    let threads: usize = items
        .iter()
        .map(|item| match item {
            ShellItem::Snapshot { threads, .. } => threads.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(threads, 30);
}

#[test]
fn a_command_output_flood_never_reaches_the_event_log() {
    let env = Env::new();
    let thread_id = seeded(&env, "flood", SeedOptions::default());
    let events_before = env.count("events");
    let hub = Arc::new(CommandOutputHub::new(OutputHubConfig::default()));
    let item: TurnItemId = fresh();
    let subscription = hub.subscribe(thread_id, item, || {}).unwrap();
    let chunk = "y\n".repeat(32 * 1024);
    let started = Instant::now();
    // 256 MiB of `yes`.
    for _ in 0..4_096 {
        hub.append(thread_id, item, &chunk);
    }
    let elapsed = started.elapsed();
    eprintln!("256 MiB of output through the hub in {elapsed:?}");
    // Ingestion is not blocked meanwhile: a commit still goes through.
    seed_history(
        &env.orchestrator,
        thread_id,
        SeedOptions {
            turns: 1,
            first_ordinal: 11,
            ..SeedOptions::default()
        },
    )
    .unwrap();
    assert_eq!(env.count("events"), events_before + 5);
    assert!(
        hub.retained_bytes() < 1024 * 1024,
        "{}",
        hub.retained_bytes()
    );
    let (frames, _) = subscription.take_frames();
    assert!(matches!(
        frames.as_slice(),
        [
            waku_protocol::v8::orchestration::CommandOutputItem::Replace {
                truncated: true,
                ..
            }
        ]
    ));
    let stored = hub.finish(item, Some(0)).unwrap();
    assert!(stored.truncated);
    assert!(stored.text.len() <= 256 * 1024 + 8);
}
