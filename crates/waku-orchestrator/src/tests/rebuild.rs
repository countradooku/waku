//! #42 acceptance (property test): random command sequences, then rebuild
//! the projection tables from the log; the rebuilt tables, the live tables
//! and the in-memory `ThreadProjection`s must all be identical.
//!
//! Cases come from a seeded xorshift generator, so a failure names its seed.
//! `WAKU_PROPTEST_CASES` raises the case count.

use std::collections::BTreeMap;
use std::sync::Arc;

use waku_protocol::orchestration::*;

use super::common::*;
use crate::clock::Clock;
use crate::command::{CapturedCheckpoint, InternalCommand, PreparedWorkspace};
use crate::orchestrator::DispatchError;
use crate::projection::{ThreadProjection, sql};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> Option<T> {
        (!items.is_empty()).then(|| items[self.below(items.len())])
    }
}

/// Accepts rejections (the generator does not avoid invalid commands) but
/// never a storage failure.
fn allow(result: Result<impl Sized, DispatchError>) {
    if let Err(error @ (DispatchError::Storage(_) | DispatchError::ShuttingDown)) = result {
        panic!("storage failure: {error}");
    }
}

fn internal(env: &Env, n: &mut u64, command: InternalCommand) {
    *n += 1;
    env.tick();
    allow(
        env.orchestrator
            .dispatch_internal(cid(&format!("i{n}")), command),
    );
}

/// Runs in request order, so picks do not depend on id values.
fn runs_with(projection: &ThreadProjection, status: RunStatus) -> Vec<RunId> {
    let mut runs = projection
        .runs
        .values()
        .filter(|run| run.status == status)
        .map(|run| (run.requested_at, run.id))
        .collect::<Vec<_>>();
    runs.sort();
    runs.into_iter().map(|(_, id)| id).collect()
}

fn step(
    env: &mut Env,
    rng: &mut Rng,
    projects: &mut Vec<ProjectId>,
    threads: &mut Vec<ThreadId>,
    n: &mut u64,
) {
    if threads.is_empty() || (threads.len() < 3 && rng.chance(3)) {
        if projects.is_empty() || rng.chance(20) {
            projects.push(env.project());
        }
        let project = rng.pick(projects).unwrap();
        threads.push(env.thread(project));
        return;
    }
    let thread = rng.pick(threads).unwrap();
    let projection = env.projection(thread);
    let active = projection.active_run().map(|run| run.id);
    let mode = |rng: &mut Rng| match rng.below(4) {
        0 => DispatchMode::Auto,
        1 => DispatchMode::QueueAfterActive,
        2 => DispatchMode::StartImmediately,
        _ => DispatchMode::DeferStart {
            workspace: WorkspaceStrategy::NewWorktree {
                base_ref: "main".into(),
                branch: None,
                start_from_origin: false,
            },
        },
    };
    match rng.below(27) {
        0..=3 => {
            let mode = mode(rng);
            allow(env.try_ok(Env::send_command(thread, fresh(), "message\nbody", mode)));
        }
        4 => {
            if let Some(target) = active {
                allow(env.try_ok(Env::send_command(
                    thread,
                    fresh(),
                    "steer",
                    DispatchMode::SteerActive {
                        target_run_id: target,
                    },
                )));
            }
        }
        5 => {
            if let Some(run) = active {
                let hold_queue = rng.chance(30);
                allow(env.try_ok(Command::RunInterrupt {
                    thread_id: thread,
                    run_id: run,
                    reason: None,
                    hold_queue,
                }));
            }
        }
        6 => {
            if let Some(run) = rng.pick(&runs_with(&projection, RunStatus::Queued)) {
                allow(env.try_ok(Command::QueuedRunCancel {
                    thread_id: thread,
                    run_id: run,
                }));
            }
        }
        7 => allow(env.try_ok(Command::QueueResume { thread_id: thread })),
        8 | 9 => {
            if let Some(run) = active {
                go_running(env, thread, run);
            }
        }
        10 | 11 => {
            if let Some(run) = active {
                let text = "x".repeat(rng.below(40) + 1);
                assistant_text(env, thread, run, &text);
            }
        }
        12 => {
            if let Some(run) = active {
                if let Some((_, request)) = request_approval(env, thread, run)
                    && rng.chance(70)
                {
                    allow(env.try_ok(Command::RuntimeRequestRespond {
                        thread_id: thread,
                        request_id: request,
                        response: RuntimeResponse::Approval {
                            option_id: "allow_once".into(),
                        },
                    }));
                }
                if rng.chance(50) {
                    resolve_requests(env, thread, run);
                }
            }
        }
        13 | 23 | 24 => {
            if let Some(run) = active
                && provider_done(env, thread, run).is_some()
            {
                let projection = env.projection(thread);
                let scope = projection.runs[&run]
                    .root_node_id
                    .and_then(|node| projection.nodes[&node].checkpoint_scope_id)
                    .unwrap();
                let result = if rng.chance(85) {
                    Ok(CapturedCheckpoint {
                        git_ref: format!("refs/waku/t/{thread}/{scope}/0"),
                        files: Vec::new(),
                    })
                } else {
                    Err("git failed".to_owned())
                };
                internal(
                    env,
                    n,
                    InternalCommand::CheckpointCaptured {
                        thread_id: thread,
                        run_id: Some(run),
                        scope_id: scope,
                        result,
                    },
                );
            }
        }
        14 => {
            if let Some(run) = rng.pick(&runs_with(&projection, RunStatus::Preparing)) {
                let command = if rng.chance(60) {
                    InternalCommand::PreparedRunRelease {
                        thread_id: thread,
                        run_id: run,
                        workspace: PreparedWorkspace {
                            cwd: "/tmp/wt".into(),
                            branch: Some("b".into()),
                            worktree_path: Some("/tmp/wt".into()),
                            owned: true,
                        },
                    }
                } else {
                    InternalCommand::PreparedRunFail {
                        thread_id: thread,
                        run_id: run,
                        failure: ProviderFailure::new(FailureClass::ProviderError, "no git"),
                    }
                };
                internal(env, n, command);
            } else if let Some(run) = rng.pick(&runs_with(&projection, RunStatus::Failed)) {
                allow(env.try_ok(Command::PreparedRunRetry {
                    thread_id: thread,
                    run_id: run,
                }));
            }
        }
        15 => {
            if let Some(run) = active.and_then(|run| projection.runs.get(&run))
                && let Some(attempt) = run.active_attempt_id
            {
                let class = rng
                    .pick(&[
                        FailureClass::UsageLimit,
                        FailureClass::ValidationError,
                        FailureClass::TransportError,
                    ])
                    .unwrap();
                internal(
                    env,
                    n,
                    InternalCommand::ProviderTurnFailed {
                        thread_id: thread,
                        run_id: run.id,
                        attempt_id: attempt,
                        failure: ProviderFailure::new(class, "boom"),
                    },
                );
            }
        }
        16 | 17 => {
            let command = match rng.below(12) {
                0 => Command::ThreadArchive { thread_id: thread },
                1 => Command::ThreadUnarchive { thread_id: thread },
                2 => Command::ThreadSettle { thread_id: thread },
                3 => Command::ThreadUnsettle { thread_id: thread },
                4 => Command::ThreadPin {
                    thread_id: thread,
                    order_key: None,
                },
                5 => Command::ThreadUnpin { thread_id: thread },
                6 => Command::ThreadVisit {
                    thread_id: thread,
                    visited_at: env.clock.now(),
                },
                7 => Command::ThreadRename {
                    thread_id: thread,
                    title: format!("Title {}", rng.below(100)),
                },
                8 => Command::ThreadSnooze {
                    thread_id: thread,
                    until: env.clock.now() + 1_000,
                },
                9 => Command::ThreadMarkUnread { thread_id: thread },
                10 => Command::ThreadSetRuntimeMode {
                    thread_id: thread,
                    runtime_mode: RuntimeMode::Auto,
                },
                _ => Command::ThreadRegenerateTitle { thread_id: thread },
            };
            allow(env.try_ok(command));
        }
        18 => {
            let request = projection
                .thread
                .title_regeneration
                .as_ref()
                .map(|regeneration| regeneration.request_id.clone())
                .unwrap_or_else(|| cid("initial"));
            internal(
                env,
                n,
                InternalCommand::TitleGenerated {
                    thread_id: thread,
                    request_id: request,
                    title: Some("Generated".into()),
                },
            );
        }
        19 | 25 => {
            // Sorted, so a seed replays the same picks (the map is a HashMap).
            let mut ready = projection
                .checkpoints
                .values()
                .filter(|checkpoint| checkpoint.status == CheckpointStatus::Ready)
                .map(|checkpoint| {
                    (
                        checkpoint.app_run_ordinal,
                        checkpoint.scope_id,
                        checkpoint.id,
                    )
                })
                .collect::<Vec<_>>();
            ready.sort();
            let target = if rng.chance(60) {
                ready.first().copied()
            } else {
                rng.pick(&ready)
            };
            if let Some((_, scope, checkpoint)) = target {
                let request = format!("rb{n}");
                *n += 1;
                env.tick();
                let accepted = env.dispatch(
                    &request,
                    Command::CheckpointRollback {
                        thread_id: thread,
                        scope_id: scope,
                        checkpoint_id: checkpoint,
                        restore_files: false,
                    },
                );
                if accepted.is_ok() {
                    let command = if rng.chance(70) {
                        InternalCommand::CheckpointRollbackApplied {
                            thread_id: thread,
                            request_id: cid(&request),
                            snapshot: None,
                        }
                    } else {
                        InternalCommand::CheckpointRollbackFailed {
                            thread_id: thread,
                            request_id: cid(&request),
                            message: "nope".into(),
                        }
                    };
                    internal(env, n, command);
                }
            }
        }
        20 => internal(
            env,
            n,
            InternalCommand::AutoSettle {
                thread_id: thread,
                snapshot_at: env.clock.now(),
            },
        ),
        21 => internal(
            env,
            n,
            InternalCommand::ThreadStop {
                thread_id: thread,
                reason: None,
            },
        ),
        22 => {
            if rng.chance(15) {
                allow(env.try_ok(Command::ThreadDelete { thread_id: thread }));
            } else if let Some(project) = rng.pick(projects) {
                allow(env.try_ok(Command::ProjectRename {
                    project_id: project,
                    name: format!("P{}", rng.below(9)),
                }));
            }
        }
        _ => {
            if rng.chance(25) {
                // A restart: the cache starts cold and must load equal state.
                env.reopen();
            }
        }
    }
}

fn live_projections(env: &Env, threads: &[ThreadId]) -> BTreeMap<ThreadId, Arc<ThreadProjection>> {
    threads
        .iter()
        .map(|thread| (*thread, env.projection(*thread)))
        .collect()
}

fn loaded(env: &Env, thread: ThreadId) -> ThreadProjection {
    sql::load_thread(&env.orchestrator.store().writer(), thread)
        .unwrap()
        .unwrap()
}

/// The fields of two projections that differ, for a readable failure.
fn diff(live: &ThreadProjection, stored: &ThreadProjection) -> String {
    let mut out = String::new();
    macro_rules! field {
        ($name:ident) => {
            if live.$name != stored.$name {
                out.push_str(&format!(
                    "\n{}:\n  live:   {:?}\n  stored: {:?}",
                    stringify!($name),
                    live.$name,
                    stored.$name
                ));
            }
        };
    }
    field!(thread);
    field!(last_sequence);
    field!(runs);
    field!(attempts);
    field!(nodes);
    field!(subagents);
    field!(provider_sessions);
    field!(provider_threads);
    field!(provider_turns);
    field!(runtime_requests);
    field!(plans);
    field!(checkpoint_scopes);
    field!(checkpoints);
    field!(context_transfers);
    field!(context_handoffs);
    field!(hot_messages);
    field!(hot_items);
    field!(next_position);
    out
}

fn check(env: &Env, threads: &[ThreadId], seed: u64, step: usize) {
    let report = env.orchestrator.verify().unwrap();
    assert!(report.is_clean(), "seed {seed} step {step}: {report:#?}");
    for thread in threads {
        let live = env.projection(*thread);
        let stored = loaded(env, *thread);
        assert!(
            *live == stored,
            "seed {seed} step {step}: cached projection of {thread} differs from storage:{}",
            diff(&live, &stored)
        );
    }
}

fn run_case(seed: u64, steps: usize) -> BTreeMap<&'static str, usize> {
    let mut env = Env::with_capabilities(steering_capabilities());
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut projects = Vec::new();
    let mut threads = Vec::new();
    let mut n = 0;
    for index in 0..steps {
        step(&mut env, &mut rng, &mut projects, &mut threads, &mut n);
        if index % 40 == 39 {
            check(&env, &threads, seed, index);
        }
    }
    check(&env, &threads, seed, steps);

    // Rebuild every projection table from the log and compare the in-memory
    // projections loaded from the rebuilt tables with the live ones.
    let live = live_projections(&env, &threads);
    let tables_before = env.count("p_turn_items") + env.count("p_runs") + env.count("p_threads");
    env.orchestrator.rebuild_projections().unwrap();
    assert_eq!(
        env.count("p_turn_items") + env.count("p_runs") + env.count("p_threads"),
        tables_before
    );
    env.assert_verified();
    for (thread, projection) in &live {
        assert_eq!(
            **projection,
            loaded(&env, *thread),
            "seed {seed}: rebuilt projection differs"
        );
    }
    // And a cold start on the rebuilt database serves the same state.
    env.reopen();
    for (thread, projection) in &live {
        assert_eq!(
            **projection,
            *env.projection(*thread),
            "seed {seed}: reopened projection differs"
        );
    }
    let mut coverage = BTreeMap::new();
    for projection in live.values() {
        for run in projection.runs.values() {
            *coverage.entry(run.status.as_str()).or_default() += 1;
        }
        *coverage.entry("checkpoint").or_default() += projection.checkpoints.len();
        *coverage.entry("runtime_request").or_default() += projection.runtime_requests.len();
    }
    coverage
}

#[test]
fn rebuilding_projections_from_the_log_reproduces_live_state() {
    let cases = std::env::var("WAKU_PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16u64);
    let mut coverage = BTreeMap::<&str, usize>::new();
    for seed in 1..=cases {
        for (key, count) in run_case(seed, 160) {
            *coverage.entry(key).or_default() += count;
        }
    }
    // The generator must actually reach the interesting states.
    for state in [
        "queued",
        "starting",
        "running",
        "waiting",
        "completed",
        "interrupted",
        "failed",
        "cancelled",
        "rolled_back",
        "checkpoint",
        "runtime_request",
    ] {
        assert!(
            coverage.get(state).copied().unwrap_or(0) > 0,
            "never reached {state}: {coverage:?}"
        );
    }
}
