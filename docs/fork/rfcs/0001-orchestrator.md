# RFC 0001: `waku-orchestrator`, an event-sourced daemon core

- **Status:** Draft. Tracked in [#11](https://github.com/countradooku/waku/issues/11).
- **Implements:** Phase 2 ([#165](https://github.com/countradooku/waku/issues/165)).
- **Reference:** T3 Code orchestration v2 ([audit](../audit/t3-server.md) §1, `t3code/docs/orchestration-v2/`).

## Problem

Today the desktop builds the transcript and the daemon does not.

- The daemon forwards provider events and persists whatever the desktop sends back in `SaveTaskState`.
- With no client attached, a running turn is only kept in a 2,048-event in-memory journal.
- Saves are full-session snapshots sent from the UI thread.

This rules out background agents, scheduled tasks, a consistent view across several clients, and reliable crash recovery.

## Goals

- The daemon is the only source of truth for threads, runs and transcripts.
- Every user or agent intent is durable before any side effect runs.
- Side effects survive restarts with explicit "replay safe" vs "process bound" semantics.
- Clients receive bounded snapshots plus small replays, never whole transcripts.
- Feature behaviour follows provider capabilities, not provider names.

## Non-goals

- Wire compatibility with T3 Code. Decision D1: we speak our own protocol (RFC 0003).
- Storing raw provider frames in SQLite. They go to rotating diagnostic logs.

## Pipeline

```
client request / MCP tool / scheduler / effect result
  → Command (app ids only)
  → per-thread lock
  → decide(&Projection, Command) -> Decision { events, effects, cancel_unsettled }   // no I/O
  → EventSink::commit(Decision)   // ONE SQLite transaction:
        append events (global sequence)
        apply events to projection tables (upsert by entity id)
        write command receipt (command_id → last sequence)
        enqueue outbox effects
  → after COMMIT: publish to streams, wake effect workers
  → EffectWorker (lease, per-thread FIFO, title lane) runs effects
  → results come back as Commands or as normalized AdapterEvents (RFC 0002)
```

### Rules

1. `decide` reads only the projection and the command. It is a plain function with no I/O and is unit-testable.
2. Dispatch returns the last committed sequence.
   - A duplicate `command_id` returns the stored receipt and emits nothing.
   - Command ids derived from stable inputs (MCP `clientRequestId`, `limit-resume:<thread>:<run>:<reset>`) make retries idempotent.
3. A command holds only its own thread's lock. Cross-thread work (stopping child threads) goes through post-commit effects.
4. Events carry full entity snapshots: `RunUpdated(Run)`, `TurnItemUpdated(TurnItem)` and so on. Applying an event is an upsert by id, which keeps replay and client reducers trivial.
5. Guarded writes for provider ingestion: write if the run is current, if the provider thread is still the owner, if unchanged.

## Entity model

```
Project
  AppThread                       user-visible conversation; survives provider exits
    Run (ordinal)                 counted user-visible turn; only its root node completes it
      RunAttempt                  initial | steering_restart | retry | provider_recovery
        ExecutionNode tree        root_turn, assistant_message, reasoning, plan, todo_list,
                                  tool_call, approval_request, user_input_request, subagent, hook, system
    Subagent                      provider_native | app_owned; child_thread_id; completion delivery
    ProviderSession               live/resumable runtime + Capabilities (RFC 0002)
    ProviderThread                native conversation handle (ProviderRef{driver, native_id, strength})
      ProviderTurn                native turn + token usage
    RuntimeRequest                approval | user_input | dynamic_tool_call | auth_refresh
    Message                       user/assistant/system; attachments; context records
    Plan                          proposed_plan (markdown) | todo_list (steps)
    TurnItem (ordered)            the display projection (26 variants, forward compatible)
    CheckpointScope / Checkpoint  hidden git refs; nested scopes do not advance the run count
    ContextTransfer / Handoff     fork | provider_handoff | merge_back | subagent_spawn | subagent_result
```

### Invariants (from T3 v2)

- App ids are primary. Provider ids are references with a strength: strong, weak or none.
- A child execution completing never closes the parent run.
- A run is terminal only after the provider's terminal event and the finalization barrier: flush streams, finalize plans, close child nodes, capture the checkpoint.
- Rollback is expressed in app run count and reconciled with the provider conversation.

## Storage

- **File:** the daemon data dir (new name per D5): `state.sqlite`, opened with WAL, `synchronous=NORMAL`, `busy_timeout=5000`, `foreign_keys=ON` and incremental auto-vacuum.
- **Table pattern:** indexed scalar columns plus `payload_json`.

| Table | Contents |
|---|---|
| `events` | `sequence` PK autoinc, `event_id`, `command_id`, `thread_id`, `run_id`, `node_id`, `driver`, `event_type`, `occurred_at`, `payload_json` |
| `command_receipts` | `command_id` PK, `command_type`, `result_sequence`, `created_at` |
| `effect_outbox` | `id`, `thread_id`, `effect_type`, `lane`, `status`, `lease_until`, `attempts`, `payload_json`, `created_at`, `settled_at` |
| `p_threads`, `p_runs`, `p_run_attempts`, `p_nodes`, `p_subagents`, `p_provider_sessions`, `p_provider_threads`, `p_provider_turns`, `p_runtime_requests`, `p_messages`, `p_plans`, `p_turn_items`, `p_turn_item_positions`, `p_checkpoint_scopes`, `p_checkpoints`, `p_context_transfers`, `p_context_handoffs` | Projections |
| `projects`, `scheduled_tasks`, `auth_sessions`, `pairing_links` | Other domains |

- **Migrations:** keep the existing approach of drizzle generating SQL embedded by `build.rs`, but use a ledger separate from upstream's so fork migrations never collide.
- **Search:** FTS5 over message text and plans ([#65](https://github.com/countradooku/waku/issues/65)).

## Effects

| Effect | Process-bound? |
|---|---|
| `provider_turn.start` / `steer` / `interrupt` / `restart` | yes, cancelled after restart |
| `runtime_request.respond` | yes |
| `provider_thread.rollback` | no, requeued |
| `checkpoint.capture` | no |
| `provider_session.detach` | no |
| `provider_runtime.continue` | no |
| `thread_title.generate` | no (own lane) |
| `terminal.cleanup`, `attachment.cleanup` | no |
| `delegated_tasks.stop` | no |

Workers:
- Use a lease-based claim. An effect is claimable only if no earlier pending or running effect exists for the same thread.
- Retry with backoff.
- Cancel by thread and effect type, racing in-process signals against execution. Re-read the row after claiming it.
- Prune settled rows after retention and keep failed rows.

## Recovery

On start, `reconcile_after_process_loss` does the following:
1. Cancel process-bound effects and requeue replay-safe ones.
2. Select continuation candidates from the projection: queued runs, pending requests, live sessions, background work and undelivered completions.
3. Record the continuation intent, then dispatch a continuation message with stable ids.

Limits: at most 3 consecutive continuations per chain. Archived, settled and snoozed threads are excluded, as are runs waiting on the user.

## Streams (detailed in RFC 0003)

| Stream | Behaviour |
|---|---|
| Shell | Active navigation rows only; deltas after a cursor. |
| Thread | A bounded snapshot (10 user turns / 75 items / 1 MiB). Replays at most 128 events / 1 MiB after a cursor, otherwise sends a fresh snapshot. History pages use an opaque cursor. |
| Command output | A separate hub. Output never becomes events. |

## Migration from current Waku data

- Import projects and sessions as threads, messages as messages plus turn items, worktree bindings, and existing checkpoint refs.
- Import transcripts lazily, on first open.
- Leave the old DB untouched.

## Testing

- Replay-backed integration tests (RFC 0002) replace only the provider transport.
- A deterministic clock and id allocator.
- A property test: rebuilding the projections from the event log is identical to the live projections.
- A crash-injection test: no projection is ahead of the log and no effect exists without its events.

## Open questions

1. Postcard vs JSON for `payload_json`. JSON is debuggable; postcard is smaller. Proposal: JSON, compressed with zstd above 4 KiB.
2. Daemon-wide or per-project database files. Proposal: one file, simpler cross-thread commands.
3. How long to keep raw events once projections are verified. Proposal: keep everything and compact turn-item churn events older than N days.
