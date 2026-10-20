//! Wire cost of the daemon → client event stream: 10k sequenced driver events
//! through `event_to_wire` + JSON (daemon side) and back through
//! `serde_json::from_str::<ServerMessage>` + `event_from_wire` (client side),
//! exactly the two halves `server.rs` and `waku-client` run per event.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use uuid::Uuid;
use waku_protocol::model::{ActivityKind, DriverEvent};
use waku_protocol::{SequencedEvent, ServerMessage, event_from_wire, event_to_wire};

const EVENTS: usize = 10_000;

/// A streaming turn's mix: mostly text and reasoning deltas, with tool
/// activity, usage updates and turn boundaries interleaved.
fn driver_events() -> Vec<DriverEvent> {
    let words = [
        " the",
        " parser",
        " streams",
        " each",
        " delta",
        " into",
        " a",
        " settled",
        " prefix",
        "\n\n",
        " `code`",
        " **bold**",
    ];
    (0..EVENTS)
        .map(|index| match index % 50 {
            0 => DriverEvent::TurnStarted,
            1..=9 => DriverEvent::ReasoningDelta(words[index % words.len()].to_owned()),
            10 | 30 => DriverEvent::Activity {
                id: Some(format!("call_{index}")),
                kind: ActivityKind::Command,
                title: "cargo test --workspace".to_owned(),
                detail: Some(format!(
                    "running 214 tests\ntest md::parser::tests::case_{index} ... ok\n"
                )),
                complete: index % 20 == 10,
            },
            20 => DriverEvent::UsageUpdated {
                context_tokens: Some(48_000 + index as u64),
                context_window: Some(200_000),
            },
            49 => DriverEvent::TurnFinished {
                success: true,
                summary: Some("Done.".to_owned()),
            },
            _ => DriverEvent::TextDelta(words[index % words.len()].to_owned()),
        })
        .collect()
}

fn encode(
    events: Vec<DriverEvent>,
    session_id: Uuid,
    runtime_id: Uuid,
    epoch: Uuid,
) -> Vec<String> {
    events
        .into_iter()
        .enumerate()
        .map(|(sequence, event)| {
            let message = ServerMessage::Event(SequencedEvent {
                session_id,
                runtime_id,
                epoch,
                sequence: sequence as u64 + 1,
                event: event_to_wire(event).expect("encodable event"),
            });
            serde_json::to_string(&message).expect("serializable message")
        })
        .collect()
}

fn decode(frames: &[String]) -> usize {
    frames
        .iter()
        .map(|frame| match serde_json::from_str::<ServerMessage>(frame) {
            Ok(ServerMessage::Event(event)) => event_from_wire(event.event).map_or(0, |_| 1),
            _ => 0,
        })
        .sum()
}

fn wire(c: &mut Criterion) {
    let events = driver_events();
    let (session_id, runtime_id, epoch) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let frames = encode(events.clone(), session_id, runtime_id, epoch);
    assert_eq!(decode(&frames), EVENTS, "every frame must round-trip");
    let bytes = frames.iter().map(String::len).sum::<usize>() as u64;

    let mut group = c.benchmark_group("wire");
    group.throughput(Throughput::Bytes(bytes));
    group.bench_function("encode_10k_events", |b| {
        // The daemon owns each event it encodes; cloning the input is setup.
        b.iter_batched(
            || events.clone(),
            |events| encode(black_box(events), session_id, runtime_id, epoch),
            BatchSize::LargeInput,
        );
    });
    group.bench_function("decode_10k_events", |b| {
        b.iter(|| decode(black_box(&frames)));
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.03);
    targets = wire
}
criterion_main!(benches);
