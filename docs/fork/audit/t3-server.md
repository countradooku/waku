# T3 Code — Server & Orchestration V2 Audit (for a Rust/GPUI parity port)

Source: T3 Code repo (read-only clone). All paths are relative to the repo root.
Scope of this report: `apps/server`, `packages/contracts`, the provider packages it loads, and the
orchestration-v2 / internals docs. Client UI is out of scope except where the server owns the behavior.

Size orientation (non-test TypeScript): `apps/server/src` is ~210k lines across ~740 files.
`apps/server/src/orchestration-v2/` alone is ~83k lines (`Orchestrator.ts` 12k, `ProjectionStore.ts`
6.9k, `ClaudeAdapterV2.ts` 9.9k, `CodexAdapterV2.ts` 7.4k, `ProviderSessionManager.ts` 2.5k). The
contracts file `packages/contracts/src/orchestrationV2.ts` is 3.6k lines and `rpc.ts` 2k lines.
This is a large surface. Do not plan on a line-for-line port.

---

## 1. Orchestration V2 architecture

### 1.1 Pipeline: command → decide → commit → effect → ingest

```
client RPC / MCP tool / HTTP / internal worker
   │  OrchestrationV2Command (or InternalCommand)            packages/contracts/src/orchestrationV2.ts:2555-3059
   ▼
ThreadManagementService (shared app boundary for WS + MCP)  apps/server/src/orchestration-v2/ThreadManagementService.ts
   │  per-thread KeyedLock (ThreadCommandExecutor)           orchestration-v2/ThreadCommandExecutor.ts
   ▼
OrchestratorV2.dispatch → dispatchOnce(command)              orchestration-v2/Orchestrator.ts:~10990
   │  pure-ish "decide": reads projection, emits
   │  {events[], effects[], cancelUnsettledEffects?}
   │  no provider / filesystem I/O inside the decision
   ▼
EventSinkV2.writeWithEffects (ONE SQLite transaction)        orchestration-v2/EventSink.ts
   ├─ append events to orchestration_events (global sequence)
   ├─ apply events to persisted projections (orchestration_v2_projection_*)
   ├─ write turn-item positions
   ├─ write command receipt (command_id → result_sequence)   orchestration-v2/CommandReceiptStore.ts
   └─ enqueue outbox effects (orchestration_v2_effect_outbox) orchestration-v2/EffectOutbox.ts
   │  after COMMIT: publish to PubSub, notify effect workers
   ▼
EffectWorker (4 workers, lease-based claim, retry/backoff)   orchestration-v2/EffectWorker.ts
   │  executes: provider-turn.start/steer/interrupt/restart, runtime-request.respond,
   │  provider-thread.rollback, checkpoint.capture, provider-session.detach,
   │  provider-runtime.continue, thread-title.generate, terminal/preview/attachment cleanup,
   │  delegated-tasks.stop
   ▼
ProviderSessionManager → ProviderAdapterV2 session runtime  orchestration-v2/ProviderSessionManager.ts,
   │                                                         packages/provider-core/src/server/ProviderAdapter.ts
   │  adapter emits normalized ProviderAdapterV2Event stream (entity upserts)
   ▼
RunExecutionService (routes events to run/attempt/node)      orchestration-v2/RunExecutionService.ts
ProviderEventIngestor (normalize → domain events → sink)     orchestration-v2/ProviderEventIngestor.ts
   ▼
EventSink.write... (guarded variants: writeIfRunCurrent, writeIfProviderThreadOwner,
                    writeIfProviderThreadUnchanged, unlessNativeThreadOwned)
   ▼
subscribers: ShellStream / ThreadStream / reactors (PR sync, settlement, delegated-task, analytics)
```

Key properties (from `docs/internals/overview.md` "Durable intent and side effects"):

- **Command ack = intent committed**, not "provider finished". `orchestration.dispatchCommand` returns
  `{ sequence }` = last committed stored-event sequence (`OrchestrationV2DispatchCommandResult`).
- **Idempotency via receipts**: duplicate `commandId` returns the same receipt-backed result without
  re-running side effects. Command IDs are frequently *derived* from stable inputs (e.g. MCP
  `clientRequestId`, `limit-resume:<thread>:<run>:<resetMs>`), so retries naturally dedupe.
- **No external I/O in decide or inside the transaction.** Effects run after commit; their results come
  back as new commands or ingested provider events.
- **Projection can never get ahead of the log**: events + projections + receipt + outbox are one
  transaction (`EventSink.ts`). Subscribers see events only after commit.
- **Events are entity snapshots**: almost every domain event carries the *full* entity payload
  (`run.updated` carries an `OrchestrationV2Run`, `turn-item.updated` a full `TurnItem`, etc.), so
  projection application is an upsert keyed by entity id (`applyToProjection` in `ProjectionStore.ts`).
  This makes replay, catch-up and client reducers simple. In Rust: `enum DomainEvent { RunUpdated(Run), ... }`
  plus `fn apply(&mut Projection, &DomainEvent)`.
- **Per-thread serialization**: a `KeyedLock<ThreadId>` serializes commands for one thread; a command
  holds only its own thread's lock, so cross-thread work (stopping child threads) is done via
  post-commit effects (`delegated-tasks.stop` → internal `thread.stop` per child, depth-first).
- **Effect ordering**: outbox claim SQL enforces per-thread FIFO: an effect is claimable only if no
  earlier pending/running effect exists for the same thread. `thread-title.generate` runs in its own
  lane so title generation never blocks turns (`EffectOutbox.ts:318-356`). Default 4 workers, 30 s
  liveness poll, post-commit in-process notify as the fast path (`EffectWorker.ts:820-891`).
- **Process-loss recovery**: effects are split into `REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS`
  (continue, detach, rollback, checkpoint.capture, cleanup, title, delegated-tasks.stop) and
  `PROCESS_BOUND_EFFECT_TYPES` (turn start/interrupt/steer/restart, runtime-request.respond). After a
  restart `reconcileAfterProcessLoss` requeues the former and cancels the latter
  (`EffectOutbox.ts:118-140`). Settled rows are retained 7 days then pruned in batches of 500
  (`EffectOutbox.layerPruneWorker`). Failed rows are kept (storage cleanup will not delete a deleted
  thread's worktree while any of its effects failed).
- **Cancellation**: `cancelUnsettled({threadId, effectTypes, reason})` + in-process `Deferred` signals
  raced against effect execution, with a re-read of the durable row after claim to close the race.

### 1.2 Data model (core graph)

Contracts: `packages/contracts/src/orchestrationV2.ts`. Design doc:
`docs/orchestration-v2/core-graph-and-data-model.md`.

```
Project
  AppThread (ThreadId)                         user-visible conversation; survives provider exits
    Run (RunId, ordinal)                       counted user-visible turn; only root node completes it
      RunAttempt (RunAttemptId)                initial | steering_restart | retry | provider_recovery
        ExecutionNode tree (NodeId)            root_turn, assistant_message, reasoning, plan, todo_list,
                                               tool_call, approval_request, user_input_request,
                                               subagent, hook, system
    Subagent (NodeId)                          origin provider_native | app_owned; childThreadId
    ProviderSession (ProviderSessionId)        live/resumable runtime metadata + capabilities
    ProviderThread (ProviderThreadId)          provider-native conversation handle + coverage
      ProviderTurn (ProviderTurnId)            normalized native turn + token usage
    RuntimeRequest (RuntimeRequestId)          approval / user_input / dynamic_tool_call / auth_refresh
    ConversationMessage (MessageId)            user/assistant/system; attachments; context
    PlanArtifact (PlanId)                      proposed_plan (markdown) | todo_list (steps)
    TurnItem (TurnItemId)                      ordered display projection (26 variants)
    CheckpointScope / Checkpoint               nested filesystem snapshots (hidden git refs)
    ContextTransfer / ContextHandoff           fork, provider_handoff, merge_back, subagent_spawn/result
```

Notable fields beyond the design doc (all in `orchestrationV2.ts`):

- **AppThread** (`:356-424`): `createdBy` (user|agent|system) + `creationSource`
  (web|mobile|mcp|provider|server); `providerInstanceId` + `modelSelection`; `runtimeMode`
  (approval-required | auto-accept-edits | auto | full-access); `interactionMode` (default|plan);
  `branch`, `worktreePath`; `linkedPullRequest` (legacy) + `pullRequests[]` + `branchPullRequest`;
  `activeProviderThreadId`; `historyOrigin` (native|v1_import); `lineage {parentThreadId,
  relationshipToParent: fork|subagent, rootThreadId}`; `forkedFrom`; sidebar state:
  `archivedAt, settledOverride, settledAt, unsettledAt, snoozedUntil, snoozedAt, lastSnoozeWakeAt,
  pinnedAt, pinOrderKey, activeOrderKey, autoSettleDisabledAt, lastVisitedAt`; `limitRecovery
  {runId, resetAt, autoResume, snooze}`; `titleRegeneration`; `rollbackRequestId/rollbackFailure`;
  `deletedAt`.
- **Run** (`:520-553`): status `preparing|queued|starting|running|waiting|completed|interrupted|
  failed|cancelled|rolled_back`; `queuePosition`, `queueHeld`; `restartContinuationOfRunId`;
  `delegatedTaskId`; `workStartedAt`; `restartCancelledBackgroundWork[]`; `sourcePlanRef` (implement a
  proposed plan); `delegatedCompletion` cohort (open|stopped|disposed, generation, delivery);
  `workspacePreparation` (root | existing_worktree | worktree{baseRef, startFromOrigin}).
  Ordinals can skip (a queued run takes the next ordinal when it starts).
- **ProviderThread** (`:859-894`): `nativeThreadRef`/`nativeConversationHeadRef` as
  `ProviderRef{driver, nativeId, strength: strong|weak|none, fingerprint?, ordinal?}`;
  `firstRunOrdinal/lastRunOrdinal` (coverage), `handoffIds[]`, `forkedFrom`,
  `pendingBackgroundTasks[]` (subagent|command|monitor|background_task), `contextUsage`,
  `nativeMetadata`, `goal` (Codex/Claude `/goal`: objective, status active|paused|blocked|
  usage_limited|budget_limited|complete, tokens/time budget).
- **RuntimeRequest** (`:1006-1026`): `responseCapability` is `live{providerSessionId}` |
  `message` (Codex async questions answered by a new user message) | `not_resumable{reason}`.
- **ProviderTurn**: `tokenUsage` (used/max/input/cached/output/reasoning, promptCacheTtlMs) +
  `turnTokenUsage`. A Codex `/goal` keeps one run open across several native turns.
- **Subagent** (`:681-716`): `origin`, `nativeTaskRef`, `prompt`, `title`, `model`,
  `completionWake: always|settled_only`, `completionDelivery` state machine
  (pending→claimed→acknowledged/delivered→disposed), `progress`, `workflow` (Claude workflow phases
  and agents with tokens/toolCalls/duration), `result`.
- **ConversationMessage**: `notification {source: delegated_task|subagent|command|monitor|
  background_task|system, outcome, summary, detail}`, `scheduledTaskId`, `senderThreadId`,
  `workflowScript`, `context` (composer context records), `attachments`, `delegatedCompletion`.
- **ProviderFailure** (`:1233-1258`): class `usage_limit|provider_error|transport_error|
  permission_error|validation_error|unknown`, message ≤4096 (redacted), code, retryable, resetAt.

**Turn item variants** (`OrchestrationV2TurnItem`, `:1332-1534`): `notification`, `user_message`
(with `inputIntent: turn_start|queued_turn|steer|promoted_queued_to_steer`), `assistant_message`,
`reasoning`, `proposed_plan`, `todo_list`, `user_input_request`, `file_change` (diffStr/oldStr/newStr,
per-file `changes[]`), `command_execution` (input/output/exitCode, `outputOmitted`,
`outputIndicatesFailure`), `file_search`, `web_search`, `approval_request` (requestKind, options,
appName), `checkpoint`, `run_interrupt_request`, `run_interrupt_result`, `system_notice`, `error`
(failure + retry), `compaction` (before/after token counts), `handoff`, `fork`, `thread_created`,
`secret_request`, `subagent`, `dynamic_tool` (toolName, input/output, `viewedImagePath`). Every item
carries tool presentation hints (`toolSurface`, `toolIcon`, `toolSource`, `toolNonExecutionKind`),
`parentItemId`, `ordinal`, status, timestamps, provider refs.

**Forward compatibility**: arrays of turn items and the thread stream use
`ForwardCompatibleUnion(Array)` — unknown `type`/`kind` values decode to an `unknown-event` that the
client skips while still advancing its cursor (`orchestrationV2.ts:3245-3316`). Notification sources
use a `kindUnionWithFallback` that downgrades unknown kinds to `background_task`. Persisted events
must stay decodable forever (overview.md). Mirror this in Rust with `#[serde(other)]`/untagged
fallbacks.

### 1.3 Entity IDs and correlation

Doc: `docs/orchestration-v2/entity-ids-and-correlation.md`. Code:
`packages/provider-core/src/server/IdAllocator.ts`.

- App IDs are primary keys; provider IDs are `ProviderRef`s with a strength
  (`strong|weak|none`). Correlation is always scoped (provider+session+thread, thread+turn ordinal,
  turn+item ordinal, etc.). Weak providers get deterministic scoped ordinals; fingerprints only as a
  fallback (never full assistant text).
- `IdAllocator` exposes per-entity allocators (`message({threadId, ordinal})`, `providerSession`,
  `runtimeRequest`, `checkpointScope`, `checkpoint`, `contextHandoff`, `contextTransfer`, `event`,
  `rawEvent`, ...) built on Effect `Random`, so tests can make them deterministic.
- `ProviderCapabilities.identity` declares `nativeThreadIds/TurnIds/ItemIds/RequestIds` strength.
- Re-processing the same provider frame must find the existing binding (replay determinism).
- Commands target app IDs; adapters resolve to native refs at the edge (`RunId → root NodeId →
  ProviderTurnId → nativeTurnRef`).

### 1.4 Provider capability system

Contract: `OrchestrationV2ProviderCapabilities` (`orchestrationV2.ts:193-330`); doc
`provider-capability-system.md`. Each adapter returns capabilities at session open
(`ProviderAdapterV2.getCapabilities`), persisted on `ProviderSession.capabilities`.

Groups: `sessions` (multi-thread per session, model switch in session, handoff switching, runtime
mode switch, pending requests survive restart) · `threads` (create empty, read snapshot, rollback,
fork, fork-from-turn, fork-from-subagent, native thread id) · `turns` (native turn id, started/
completed events, interrupt, active steering, `activeSteeringInterruptsTools`, steer via
interrupt-restart, queued messages, `terminalStatusQuality`) · `streaming` · `tools` (item ids,
started/completed/output, MCP tools, dynamic tool callbacks) · `approvals` (command/file-read/
file-change/apply-patch, native request ids, live-only callbacks, from subagents) · `planning`
(plan updated, todo list, proposed plan, structured questions) · `subagents` · `context` (system/
developer/synthetic-user context, summaries, delta/full handoff, `maxRecommendedHandoffChars`) ·
`checkpointing` (app fs checkpoint, nested scopes, provider rollback, rollback snapshot) ·
`identity` · `runtimePolicy.enforcement: native|client-boundary` (defaults to `client-boundary` when
decoding old events so replay never over-claims).

Policy code: `CommandPolicy.ts` (dispatch decisions `start_run|steer_active|restart_active|
queue_after_active|switch_provider`; `SteeringExecutionPolicyV2 = active_steering|
interrupt_restart`; `ForkExecutionPolicyV2 = native_fork|portable_context`; capability gate names),
`ProviderSessionTransitionPolicy.ts` (`reuse | switch_model_in_session | restart_and_resume |
create_with_handoff | reject`), `RuntimePolicy.ts` (maps thread modes → adapter runtime policy;
providers that don't offer a runtime mode run it as `approval-required`).

**Adapter contract** (`ProviderAdapter.ts:548-700`): `getCapabilities`, `planSelectionTransition`,
`openSession(scoped)` → session runtime with `events` stream / `subscribeEvents`, `ensureThread`,
`resumeThread`, `injectHistory?`, `startTurn`, `compactThread?`, `steerTurn`, `stopTask?`,
`interruptTurn`, `stopSubagent?`, `unloadThread?`, `respondToRuntimeRequest`, `readThreadSnapshot`,
`rollbackThread`, `forkThread`, `uploadFeedback?`, `mcpApps?{listTools, callTool, readResource}`,
`hasPendingBackgroundWork[ForThread]?`, `backgroundTasksEndWithProcess?`, `getModelContextWindow?`,
`canReuseContextUsage?`.

**Adapter event stream** (`ProviderAdapterV2Event`, `ProviderAdapter.ts:79-175`) is already
normalized to V2 entities: `app_thread.created`, `provider_session.updated`,
`provider_thread.updated` (+`retainedNativeTurnIds`), `provider_turn.updated`, `node.updated`,
`subagent.updated`, `message.updated`, `turn_item.updated`, `command_output.delta` (never
persisted), `runtime_request.updated`, `plan.updated`, `turn.terminal` (completed|interrupted|
cancelled, or failed with `failure`, `retry`, `threadDisposition: reusable|broken`,
`childTurnsContinue`). This is the single most important seam for a Rust port: each provider driver
only has to produce this stream.

### 1.5 Thread lineage, context transfer and handoffs

Docs: `thread-lineage-and-context-transfer.md`, `provider-switching-and-context.md`,
`docs/internals/context-handoffs.md`, `docs/user/portable-handoffs.md`. Code:
`ContextHandoffService.ts`, `ContextHandoffDelivery.ts`, `ThreadForkService.ts`,
`ProviderSwitchService.ts`, `Orchestrator.ts`.

- `ContextTransfer{type: fork|provider_handoff|merge_back|subagent_spawn|subagent_result,
  sourcePoint, basePoint, source/targetProviderInstanceId, targetRunId, status: pending|
  resolved_native|resolved_portable|failed|consumed|superseded, resolution}`. Resolutions:
  `native_fork | portable_context | delta_context | fork_delta_context | checkpoint_context`.
- `ContextHandoff{strategy: delta_since_target_last_seen|fork_delta_summary|full_thread_summary|
  checkpoint_summary|manual_context, coveredRunOrdinals, history{messages, coverage, omittedItems,
  omittedItemIds}, delivery{nativeThreadId, status: pending|injected|inline, itemIds}}`.
- **Fork** (`thread.fork` with source `latest_stable | run | checkpoint`): creates target AppThread
  with lineage + pending transfer; no provider session/thread is created. First dispatch on the fork
  resolves: same provider + native fork support → `native_fork` (Codex `thread/fork` with inclusive
  `lastTurnId`; fallback fork-latest + rollback, refused on paginated-history threads); else lazily
  materialize a portable handoff. Active source runs are rejected; `latest_stable` = latest completed
  checkpointed run. Forkable statuses: completed, waiting (provider done, checkpoint pending), failed,
  interrupted, cancelled (`ThreadForkService.ts`).
- **Merge-back** (`thread.merge_back`): delta from fork point to fork's latest stable point,
  injected with the next source-thread message.
- **Provider switch** (`provider.switch` / model selection change on send): same app thread, new or
  resumed provider thread; default strategy is resume the previous provider thread and inject a
  delta of runs it missed; fall back to fresh thread + full summary.
- **Handoff content** (current implementation): a *budgeted selection of real messages*, not an LLM
  summary. `ContextHandoffServiceV2` includes user/assistant messages, command executions, file
  changes, checkpoints, prior handoffs; text compacted to 240 chars per item in the summary text;
  selected messages keep full text. Budget: `T3CODE_CONTEXT_HANDOFF_TOKEN_CAP` default 16,000
  (clamped 1,024–64,000), 1 token per UTF-8 byte accounting, assumes 128k window if unknown, reserve
  ≥16k or ¼ window, images reserve 8,192 tokens, other attachments 4,096; imported history ceiling
  64,000 bytes. Codex receives history natively via `injectHistory` when supported; others get
  attributed context text. Omitted items are referenced so the agent can fetch them via
  `t3_thread_read`.
- **Delivery durability** (`ContextHandoffDelivery.ts`): persist before/after injection; an
  ambiguous pending delivery forces a fresh native thread.
- **Legacy v1 continuation** uses a different algorithm: user+assistant only, newest suffix within
  32,000 chars.

### 1.6 Subagents and delegated tasks (child threads)

- **Native subagents** (Codex collab agents, Claude Task tool/workflows, Pi `subagent` extension
  results): adapter emits `subagent.updated` with `origin: provider_native`; child provider turns are
  separate `ProviderTurn`s linked by `parentNodeId`; child `turn/completed` never completes the parent
  run; child checkpoints are nested scopes (`advancesAppRunCount=false`). Provider-native subagent
  child threads can't take messages (`isProviderNativeSubagentThread`).
- **App-owned delegated tasks**: MCP `delegate_task` → command `delegated_task.request` → child
  AppThread (`lineage.relationshipToParent = subagent`, `creationSource: mcp`) + child run with only
  the task prompt (no parent history) + `subagent_spawn` transfer; on child terminal the parent's
  subagent/node/turn item is finalized and a `subagent_result` transfer recorded (idempotent;
  finalization re-runs after restart via event replay). Commands:
  `delegated_task.wake-policy`, `.completion-delivery.acknowledge`, `.completion-delivery.dispose`,
  `thread.created.record`, `subagent.stop`.
- **Completion wakes**: when tasks finish, the parent gets a `message.dispatch` with
  `delegatedCompletion{parentRunId, generation, taskIds}`; delivery cohorts are generation-numbered;
  `completionWake: settled_only` (default) waits until the parent has no live run; automatic
  completion runs jump the queue (`QueuedRunOrder.ts`).
- **Child questions** (`DelegatedTaskQuestionReactor.ts`): when a child opens a pending `user_input`
  request, the parent receives a notification message telling it to answer via
  `t3_pending_request_*` or ask the user; cancelled if answered first. Approvals are not routed.
- **Stop cascade**: Stop = `run.interrupt{holdQueue}` + end PR watches + drop pending wakes +
  `delegated-tasks.stop` effect → internal `thread.stop` on each child, depth-first. `task_cancel`
  sends `thread.stop` to the child.
- **Privilege ceiling** (`DispatchModeLimit.ts`, `mcp/threadAccess.ts`): child/target runtime mode
  rank `approval-required(0) < auto-accept-edits(1) < auto(2) < full-access(3)`, interaction
  `plan(0) < default(1)`; re-checked inside the target thread's command lock.
- **Claude workflows**: `OrchestrationV2SubagentWorkflow` (phases, agents, run handles,
  transcriptDir, scriptPath); RPCs `orchestration.getWorkflowScript`, `orchestration.stopWorkflow`;
  MCP `t3_workflow_stop`; transcripts read from disk with a 512 KiB cap
  (`workflowAgentTranscript.ts`, `workflowFileRead.ts`, `workflowScriptQuery.ts`,
  `Adapters/claudeWorkflowProgress.ts`).

### 1.7 Queueing, steering, restart, compaction

- `message.dispatch.dispatchMode`: `start_immediately | queue_after_active | steer_active{targetRunId}
  | restart_active{targetRunId} | defer_start{workspaceStrategy}`; optional `deliveryIntent:
  auto|steer|restart` resolved *server-side inside the thread lock* from authoritative state
  (`CommandPolicy.resolveMessageDispatchIntent`). `auto` = start if idle, steer if the active
  provider turn is steerable, else queue.
- Steering: `provider-turn.steer` effect if `supportsActiveSteering`; otherwise
  `provider-turn.restart` (interrupt + new `RunAttempt{reason: steering_restart}` under the same run;
  optional session transition replace/detach).
- Queue commands: `queued-run.reorder` (beforeRunId), `queued-run.cancel`, `queued-run.edit` (text,
  context, optional attachment replacement), `queued-message.promote-to-steer`, `queue.resume`
  (release a held queue). Runs carry `queuePosition`/`queueHeld`. `prepared-run.*` commands
  (`release`, `progress{phase: worktree|setup}`, `fail`, `retry`) cover runs waiting on workspace
  preparation (worktree creation + setup script) before they become startable.
- Compaction: `/compact` and `/logout` are "native maintenance commands" — sent as a turn
  (`compactThread` when the adapter implements it); steering into them is converted to queueing;
  `/compact` on an empty conversation is refused. Compaction is a `compaction` turn item with
  before/after token counts.
- Notifications as wakes: provider background notifications buffer in a "mailbox" and are delivered
  at least once, reusing message ids (`NotificationMailbox.ts`, `notification.delivery.accept`).

### 1.8 Approvals, user input, secrets

- Provider callbacks → `RuntimeRequest` + `approval_request`/`user_input_request` node + turn item;
  run status `waiting`. `runtime-request.respond{decision | answers, attachmentsByQuestionId}` →
  `runtime-request.respond` effect → `RuntimeRequestServiceV2.respond` (errors:
  request-missing/not-ready/not-resumable/provider-session-not-active).
- Message-mode questions (Codex async) commit the resolution **and** a user message in one
  transaction; repeats return the receipt. `thread.user-input.dismiss` dismisses.
- Question answers can carry file attachments (≤100 files) (`docs/user/question-attachments.md`).
- Native permission/question option IDs must survive normalization (labels are not valid replies).
- After restart, pending live requests become `not_resumable` unless `pendingRequestsSurviveRestart`.
- **Secret requests**: MCP `request_secret` → internal `secret_request.record` command → private card
  (`secret_request` turn item, status pending|saved|declined|cancelled); the value is stored in the
  server secret store and never returned to the agent; RPC `secrets.answerRequest`.

### 1.9 Plan mode

- `interactionMode: plan|default` per thread (`thread.interaction-mode.set`), provider-native plan
  modes. Plan items: `proposed_plan` (markdown, streaming) and `todo_list` (steps with
  pending/running/completed + durations), stored in `orchestration_v2_projection_plans` and as turn
  items. Implementing a plan sends a message with `sourcePlanRef{threadId, planId}` (can target a new
  thread). Shell exposes `hasActionableProposedPlan`.

### 1.10 Interrupts, stop, recovery

- `run.interrupt{runId, reason, holdQueue}` → `provider-turn.interrupt` effect; the run is **not**
  terminal until the provider's terminal event (`run_interrupt_request`/`run_interrupt_result` turn
  items record it). `ProviderTurnControlService.interruptAndAwaitTerminal` exists for restart.
- `thread.stop` (internal): interrupt + hold queue + end PR watches + stop delegated children.
- **ProviderSessionManager**: owns live runtimes; idle release after 30 min
  (`DEFAULT_IDLE_TIMEOUT_MS`), but pinned while background work runs, up to 4 h
  (`DEFAULT_MAX_IDLE_PIN_MS`); per-thread idle unload for multi-thread sessions; issues MCP
  credentials before opening and revokes on close; fan-out event subscriptions per provider thread.
- **ProviderRuntimeRecoveryService** + **RestartContinuation** + **RestartBackgroundNote** +
  **ProviderContinuationService**: on startup, select candidates from projection state (queued runs,
  pending requests, live sessions, background work, unfinished delegated deliveries), retire
  process-bound effects, record continuation intent in the outbox (`provider-runtime.continue`),
  wait for server activation, then re-dispatch a continuation message with stable IDs. Refuses
  archived/settled/snoozed threads, runs waiting on the user, newer user work, `/compact` turns; caps
  at 3 consecutive continuations per `restartContinuationOfRunId` chain. Graceful shutdown captures
  intent before closing providers. Preference: restart continuation on by default
  (`docs/internals/server-updates.md`).
- **Usage-limit recovery** (`UsageLimitRecoveryWorker.ts`): threads failed with
  `usage_limit` + `resetAt` get `limitRecovery` armed (auto-resume and/or snooze per settings), and at
  reset a `message.dispatch` "Continue where you left off." with derived command id.
- **Finalization barrier**: provider terminal → flush streams → finalize plans → close child nodes →
  `checkpoint.capture` effect → `RunFinalizationService` → run terminal. Provider duration is
  recorded independently so late checkpoint/diff work doesn't extend it.

### 1.11 Checkpoints and rollback

- Hidden git refs (`apps/server/src/checkpointing/CheckpointStore.ts`), captured by
  `CheckpointCaptureService.ts` per `CheckpointScope` (root_run | subagent | tool | provider_thread |
  manual). Diffs via `orchestration.getTurnDiff` / `getFullThreadDiff` and `CheckpointDiffQuery`.
- `checkpoint.rollback{scopeId, checkpointId, restoreFiles?}` → `checkpoint.rollback-requested` event
  → `provider-thread.rollback` effect (`CheckpointRollbackService.ts`): rejects before touching files
  when the provider can't roll back its conversation; refuses restore when the workspace is shared
  (`CheckpointRestoreSafety.ts`); marks later runs `rolled_back`, supersedes handoffs; failure →
  internal `checkpoint.rollback.fail` → `thread.rollbackFailure`. Codex `thread/rollback` only works
  on legacy-history threads.

### 1.12 Snapshot queries, streaming, replay and offline catch-up

- **Global sequence**: every committed event (project + agent) gets a monotonically increasing
  `sequence` in `orchestration_events`. Snapshots carry `snapshotSequence`; streams send only events
  with `sequence > N`.
- **Shell stream** `orchestration.subscribeShell{afterSequence?, requestCompletionMarker?}`: items
  `synchronized | snapshot | project.updated | project.removed | thread.updated{location:
  active|archive} | thread.removed`. Initial shell includes only active navigation rows; archive via
  `getArchivedShellSnapshot`/`subscribeArchivedShell`. Resume sends deltas + compact repository
  enrichment, unchanged shells skipped but re-sent after 5 s (`ShellStream.ts`).
- **Thread stream** `orchestration.subscribeThread{threadId, afterSequence?, requestCompletionMarker?,
  acceptBoundedSnapshot?, acceptCompactTurnItems?}`: items `synchronized | snapshot{projection,
  historyCursor, hasMoreHistory, latestLocalTurnOrdinal, payloadBudgetExceeded,
  turnItemsOmitLocalVisible} | event{sequence, event}`. Resume plan (`ThreadStream.ts`): replay if
  ≤128 events and ≤1 MiB encoded (and ≤1 MiB raw persisted payload pre-check), else fresh snapshot.
- **Bounded snapshots & history paging** (`threadHistoryPaging.ts`): recent window = 10 user turns /
  75 items / 1 MiB; older pages via HTTP `/api/orchestration/threads/:id/history` with an opaque
  cursor (≤4096 chars); window always admits ≥1 row; required control records (pending requests,
  etc.) are always included; per-fork-ancestor bounds.
- **Live stream budget** (`LiveStreamBudget.ts`): subscribe-before-high-water, buffer ≤1,000 items /
  8 MiB, fail the subscription (client resubscribes) instead of unbounded buffering.
- **Coalescing** (`ThreadLiveEventCoalescer.ts`): running tool `turn-item.updated` events coalesced
  in a 50 ms window, ≤512 pending.
- **Wire projection** (`WireProjection.ts`): command output, dynamic tool output, inline file-change
  bodies omitted on the wire (`outputOmitted`, `outputIndicatesFailure`); file-change diffs truncated
  even in storage; handoff summary stripped; fetch full body via `orchestration.getTurnItem`.
- **Command output hub** (`CommandOutputHub.ts`): live command output is never an event;
  `orchestration.subscribeCommandOutput{threadId,itemId}` streams `replace|append` frames
  (100 ms frame interval, 500 ms after overflow, 16 KiB append cap, 64 live commands, 16 finished
  tails).
- Other read RPCs: `getThreadProjection`, `getTurnItem`, `searchThreads`, `searchThread` /
  `searchThreadStream` (in-thread find with navigation ≤17 entries), `launchThread`.
- HTTP snapshot endpoints: `/api/orchestration/shell`, `/threads/:id`, `/threads/:id/bounded`,
  `/threads/:id/history`, `/threads/:id/transcript` (`orchestration-v2/http.ts`,
  `packages/contracts/src/environmentHttp.ts`).
- **Protocol gate**: `ORCHESTRATION_PROTOCOL_VERSION = 2`; `/ws?orchestrationProtocol=2` else HTTP
  426 `orchestration_protocol_incompatible` (`apps/server/src/ws.ts`,
  `docs/internals/legacy-orchestration-migration.md`).

### 1.13 Thread lifecycle & sidebar state (server-owned)

Commands: `thread.create` (optionally `importedNativeThread`), `thread.archive/unarchive/delete`,
`thread.settle / auto-settle{snapshotAt} / unsettle / auto-settle.set`, `thread.snooze / unsnooze`,
`thread.pin / unpin / pin.reorder / active.reorder` (fractional `orderKey`s), `thread.visit`
(monotonic `lastVisitedAt`, no `updatedAt` bump), `thread.mark-unread`, `thread.metadata.update`
(title, regenerateTitle, branch/worktree with `expected*` guards, `expectedEmpty`, `limitRecovery`,
legacy `linkedPullRequest`), `thread.title.regeneration.complete`, `thread.runtime-mode.set`,
`thread.interaction-mode.set`, `thread.model-selection.set`, `provider-session.detach`,
PR commands (`thread.pull-request.link/unlink/watch/sync`, `.pull-request-link.sync`, internal
`.pull-request-watch.sync`). `ThreadSettlementService.ts` (server-side auto-settle on merge/inactivity
— default 3 days — with "keep threads with PR", per-project overrides; closes idle terminals; runs
`runOnSettle` project scripts), `ThreadDeletion.ts`, `ResourceCleanupService.ts`,
`ThreadTitleRegenerationService.ts`.

### 1.14 Storage (SQLite)

- File: `<T3 home>/userdata/statev2.sqlite` (`apps/server/src/config.ts:124`). First V2 launch
  snapshots `state.sqlite` (v1) via SQLite backup into `statev2.sqlite`; only the copy is migrated
  (`persistence/initializeV2Database.ts`). PRAGMAs: WAL, `synchronous=NORMAL`, `busy_timeout=5000`,
  `foreign_keys=ON`, incremental auto-vacuum, `checkpoint_fullfsync`, journal size limit
  (`persistence/Sqlite.ts`).
- Migrations: `persistence/Migrations/001…064` (Effect SQL migrator; ids only compared — fork-local
  migrations must use a separate ledger). V2 schema is migration `055_OrchestrationV2.ts` +
  sub-migrations in `Migrations/OrchestrationV2/`.
- V2 tables: `orchestration_events` (shared app event log; v2 rows have
  `application_event_version=2`, columns sequence PK autoinc, event_id, command_id, thread_id,
  run_id, node_id, driver, provider_instance_id, raw_event_id, event_type, occurred_at,
  payload_json), `orchestration_command_receipts` (+`command_type`), `orchestration_v2_effect_outbox`,
  `orchestration_v2_projection_{threads, runs, run_attempts, nodes, subagents, provider_sessions,
  provider_session_bindings, provider_threads, provider_turns, runtime_requests, messages, plans,
  turn_items, checkpoint_scopes, checkpoints, context_handoffs, context_transfers, metadata}`,
  `orchestration_v2_turn_item_positions`, `orchestration_v2_thread_launch_workflows` (multi-step
  launch saga: setup/thread/message committed flags), `orchestration_v2_legacy_imports`,
  `scheduled_tasks`. Pattern: indexed scalar columns + full `payload_json`.
- Retained v1 tables (import source): `projection_threads`, `projection_thread_messages`,
  `projection_thread_pull_requests`, `projection_projects`, `projection_thread_sessions`,
  `provider_session_runtime`. Dropped by 063: activities, turns, proposed plans, pending approvals,
  projection_state, checkpoint_diff_blobs.
- Other tables: `auth_pairing_links`, `auth_sessions`, `pull_request_files_viewed`,
  `pull_request_review_guides`, `scheduled_task_webhook_deliveries`,
  `scheduled_task_webhook_relay_deliveries`, `mcp_app_model_context`.
- Raw provider frames are **not** stored in SQLite: rotating log files with a 64 KiB per-payload
  encoded budget and structural summaries (`docs/internals/providers.md` "Provider diagnostics").
- Projection maintenance: verify/rebuild in bounded pages (`ProjectionMaintenance.ts`);
  `t3 doctor` CLI.

### 1.15 What changed vs V1

| Area | V1 | V2 |
|---|---|---|
| Unit of work | provider turn = app turn | `Run` (app) ≠ `ProviderTurn` (native) ≠ `RunAttempt`; execution node tree |
| Events | V1 command/event union, activities table | V2-owned entity-snapshot events; V1 unions removed (only importer reads V1 rows) |
| Side effects | reactors observing live event subscriptions | durable outbox committed with events + lease-based effect worker |
| Subagents | flattened, child completions could close parent | nested nodes, child provider threads, app-owned delegated child threads |
| Provider switch | implicit | explicit `ContextTransfer` + auditable `ContextHandoff` |
| Forks | n/a / eager | lazy: lineage + pending transfer, resolved on first run (native or portable) |
| Capabilities | provider-name branches | `ProviderCapabilities` + policies |
| Projections | `projection_thread_activities`, `projection_turns`, pending approvals | normalized graph tables + ordered `turn_items` + `visibleTurnItems` (fork-inherited items) |
| Wire | full snapshots | bounded snapshots, history paging, wire-omitted bodies, separate command-output stream |
| DB | `state.sqlite` | `statev2.sqlite` copy; lazy transcript import (shell first, transcript on read/continue) |
| Protocol | — | `ORCHESTRATION_PROTOCOL_VERSION=2` handshake gate |

Legacy import (`orchestration-v2/legacy/LegacyV1ThreadImporter.ts`): preserves thread ids/title/
model/modes/branch/worktree/timestamps/settle/snooze/pin/linked PR; transcript import (user +
assistant messages, attachments; streaming → interrupted) happens lazily; provider sessions,
checkpoints, activities, approvals, plans are **not** imported. First continuation of an imported
thread starts a fresh provider session with the 32k-char legacy handoff.

### 1.16 Testing approach worth copying

Replay-backed integration tests: only the provider transport is replaced (`ProviderReplayTranscript`
with `expect_outbound` / `emit_inbound` / `runtime_exit` entries, `orchestrationV2.ts:3598-3642`);
real orchestrator, adapter, sink, projections, checkpoints. Deterministic clock/random. Recorders:
`record:codex-replay`, `record:claude-replay`, `record:cursor-replay`. Testkits live next to adapters
(`*.testkit.ts`). Perf guard: `vp run test:perf:v2-wire`.

### 1.17 Orchestrator MCP server (app-owned MCP endpoint)

Docs: `docs/orchestration-v2/orchestrator-mcp-server.md`, `docs/internals/environment-auth.md`
("MCP clients are a separate audience"). Code: `apps/server/src/mcp/`.

**Transport and auth**
- One authenticated streamable-HTTP MCP endpoint: `http://127.0.0.1:<port>/mcp`, server key
  `t3-code` (tool names appear to agents as `mcp__t3-code__<tool>`). `McpHttpServer.ts` registers
  every toolkit; Effect's `McpServer` is only allowed inside this file (lint rule
  `t3code/no-raw-mcp-registration`).
- **Provider-session credentials** (`McpSessionRegistry.ts`): before `ProviderSessionManager` opens
  a session it mints a bearer token scoped to {environment, parent thread, provider instance,
  provider session} with capabilities drawn from `preview | orchestration | worktree | device | pull-requests`
  (`McpInvocationContext.ts`; `preview` only when browser tools are available). Max lifetime + idle expiry; revoked when the session is released; never
  persisted in orchestration state.
- **External MCP clients** (agents T3 did not launch): OAuth 2.1 authorization-code + PKCE server at
  `/oauth/mcp/{register,authorize,approval,decision,token}` + `/.well-known/oauth-protected-resource/mcp`
  (`apps/server/src/auth/McpOAuth.ts`). Stateless client registration; loopback or any HTTPS redirect;
  grant is read-only (`orchestration:read`) or a signed runtime-mode ceiling; subject `mcp-client`
  only accepted on `/mcp`.
- **Per-tool access declarations** (`McpToolAccess.ts`): every tool must declare who may call it
  (read vs operate, live caller, full-access caller); `threadAccess.ts` enforces "target modes may not
  exceed caller modes" and project scoping.
- **Injection per provider**: Codex via `-c mcp_servers.t3-code.url=… -c
  mcp_servers.t3-code.bearer_token_env_var="T3_MCP_BEARER_TOKEN"`; Claude SDK `mcpServers` http
  entry + `allowedTools: ["mcp__t3-code__*"]`; Cursor SDK `mcpServers`; ACP (Grok, registry agents)
  via `session/new|load|fork mcpServers` — with `AcpMcpStdioBridge.ts` relaying stdio↔HTTP because
  ACP agents must support stdio MCP but HTTP support is uneven; OpenCode 2.x registers a per-thread
  `t3-code-<thread>` MCP entry with session permission rules denying other threads' entries; Pi gets a
  T3-written extension file `pi-t3-mcp-extension.ts` + `T3_MCP_URL`/`T3_MCP_BEARER_TOKEN` env, with
  `mcp-protocol-version: 2025-06-18` header on follow-ups and hidden `mcp__t3_code__` aliases.
- Failures are typed `OrchestratorMcpFailure` codes: `capability_denied, parent_not_active,
  provider_unavailable, model_unavailable, runtime_mode_escalation_denied,
  interaction_mode_escalation_denied, task_not_found, task_not_cancellable, thread_not_found,
  run_not_found, thread_not_sendable, thread_not_interruptible, invalid_request,
  orchestration_error`.
- Idempotency: `clientRequestId` derives stable command/thread/message ids within the provider
  session.

**Complete tool list (83 tools = 82 literal names + `html_render`, by toolkit file)**

Orchestrator toolkit — `apps/server/src/mcp/toolkits/orchestrator/tools.ts`
(service: `mcp/OrchestratorMcpService.ts`, schemas `packages/contracts/src/orchestratorMcp.ts`)
| Tool | Purpose |
|---|---|
| `orchestrator_capabilities` | Provider instances, models (incl. custom), inherited modes, which can run child tasks, runtimeModes offered, feature flags |
| `delegate_task` | Create a T3-owned child thread+run with only the task prompt; `mode: async|wait`, `timeoutMs`, `role`, target provider/model, modes (may only narrow), `clientRequestId` |
| `task_status` | Durable delegated-task state (`workState`, `hasPendingChildRuns`, `latestTerminal*`, `hasPendingQuestion`) |
| `task_cancel` | Stop the child (internal `thread.stop`), its delegated descendants, and dispose parent delivery |
| `t3_workflow_stop` | Stop one running Claude workflow (coordinator subagentId) |
| `schedule_task` | Create a persistent scheduled task (interval / fixed_time / cron-like / webhook triggers) |
| `list_scheduled_tasks` | List scheduled tasks |
| `update_scheduled_task` | Patch a task (enable/disable, rebind to current thread, schedule) |
| `delete_scheduled_task` | Delete a task |
| `request_secret` | Ask the user for a secret via a private card; value never returned to the agent |
| `create_threads` | Create 1–20 ordinary top-level threads (shared checkout), optional prompts |
| `t3_thread_list` | List thread shells in a project (filters: status, title, settled, snoozed, include subagents), paginated |
| `t3_thread_read` | Read durable state + timeline (`messages` or `activity` view), bounded text, `afterPosition`/`nextPosition` |
| `t3_thread_update` | `rename`, `regenerate_title`, `link_pull_request`, `unlink_pull_request` |
| `t3_thread_send` | Send to any thread: `auto|queue|steer|restart` |
| `t3_thread_wait` | Wait for a run to reach a terminal state (timeout doesn't cancel) |
| `t3_thread_interrupt` | Interrupt newest/selected active run |

Thread toolkit — `toolkits/thread/tools.ts`
| `t3_thread_organize` | pin / snooze / settle / archive / mark unread (settling self deferred to turn end) |
| `t3_queue_list`, `t3_queue_read` (≤16,000 chars), `t3_queue_edit`, `t3_queue_cancel`, `t3_queue_reorder`, `t3_queue_promote_to_steer` | Queue management |
| `t3_pending_request_list`, `t3_pending_request_read`, `t3_pending_request_respond` | Answer pending user-input questions (not approvals) |
| `t3_thread_configuration`, `t3_thread_configure` | Read / set provider+model selection |
| `t3_thread_fork`, `t3_thread_merge_back`, `t3_thread_transfers` | Fork, merge back, read transfer status |
| `t3_thread_search` | Bounded title+content search within a project |
| `run_scheduled_task_now` | Run a scheduled task manually (full-access caller) |

Project toolkit — `toolkits/project/tools.ts`
| `t3_project_list`, `t3_project_read`, `t3_project_create` (incl. "new project from title": git init + README in managed projects folder), `t3_project_update`, `t3_project_delete` (force for nonempty), `t3_project_clone` | Project registry |
| `t3_thread_launch` | Launch one top-level thread bound to `root | existing_worktree | worktree{baseRef, startFromOrigin}`; `scratch: true` for project-less scratch folder |

Worktree toolkit — `toolkits/worktree/tools.ts` (`mcp/WorktreeMcpService.ts`)
| `t3_worktree_handoff` | Move the calling thread into a new git worktree |
| `t3_worktree_status` | Report thread's worktree binding |
| `t3_worktree_list` | List branch refs and checkout paths |

Pull-request toolkit — `toolkits/pullRequests/tools.ts`
| `link_pull_request`, `unlink_pull_request`, `list_thread_pull_requests` (with stacks), `watch_pull_request` (2-min polling; wakes agent on failed checks / required checks pass / comments / reviews / merge), `unwatch_pull_request`, `write_pull_request_guides` (≤20 PRs, runs in temp checkout), `read_pull_request_guide` |

Attachment toolkit — `toolkits/attachment/tools.ts`
| `t3_attachment_prepare_upload` (signed upload URL on the MCP origin), `t3_attachment_discard`, `t3_thread_send_attachments` |

Environment toolkit — `toolkits/environment/tools.ts`
| `t3_environment_read` (server identity + selected prefs; instructions ≤4,000 chars), `t3_environment_preferences_update` (full-access caller) |

Preview (collaborative browser) toolkit — `toolkits/preview/tools.ts` (`mcp/PreviewAutomationBroker.ts`)
| `preview_status`, `preview_open`, `preview_dialog`, `preview_navigate` (URL or `environment-port`), `preview_resize` (fill/freeform/device preset), `preview_set_appearance`, `preview_snapshot` (accessibility/DOM snapshot with Playwright locators), `preview_click`, `preview_type`, `preview_hover`, `preview_select`, `preview_drag`, `preview_upload`, `preview_press`, `preview_scroll`, `preview_evaluate`, `preview_wait_for`, `preview_recording_start`, `preview_recording_stop` (≤50 MiB evidence file) |

Preview controls — `toolkits/previewControls/tools.ts`
| `t3_preview_list`, `t3_preview_close` |

HTML — `toolkits/html/tools.ts`
| `html_preview` (render HTML headlessly → PNG + contentHeight + console logs), `html_render` (show a finished HTML page inline in the thread) |

Device — `toolkits/device/tools.ts`
| `device_list`, `device_open` (boots simulator/emulator, returns pinned `agent-device` CLI invocation), `device_screenshot`, `device_close` |

Also MCP-adjacent: **MCP Apps** (interactive provider MCP app UIs): RPCs `mcpApps.callTool`,
`mcpApps.toolInfo`, `mcpApps.readResource`, `mcpApps.updateModelContext`; table
`mcp_app_model_context`; adapter `mcpApps` hook (`apps/server/src/mcpApps/`).

Rust note: the `rmcp` crate can host the streamable-HTTP server. The hard parts are the
credential scoping, privilege ceiling, idempotent `clientRequestId` → command id derivation, and the
wait/poll semantics on durable state, not the protocol.

## 2. Server-side feature catalogue

Complexity is the effort to reimplement in Rust: S ≈ days, M ≈ 1–2 weeks, L ≈ 3–6 weeks,
XL ≈ multiple months. Paths are under `apps/server/src/` unless stated. Sources: the service layers
wired in `server.ts`, `orchestration-v2/runtimeLayer.ts`, the contracts and the docs. Where a
directory was only skimmed and not read in depth, the entry says so.

### Providers & adapters
| Feature | Description | Key paths | Cx |
|---|---|---|---|
| V2 adapter contract | Normalized `ProviderAdapterV2` + event stream (see §1.4) | `packages/provider-core/src/server/ProviderAdapter.ts` | L |
| Codex app-server adapter | JSON-RPC app-server: threads/turns/items, approvals, async questions (message-mode), `/goal` multi-turn runs, native fork with `lastTurnId`, rollback (legacy history only), history injection, collab subagents, MCP injection via `-c` | `orchestration-v2/Adapters/CodexAdapterV2.ts` (7.4k lines), `packages/effect-codex-app-server`, `provider/CodexProvider.ts`, `codexModelOptions.ts` | XL |
| Claude Agent SDK adapter | SDK `query()` stream, tool permissions, Task subagents, workflows (phases/agents), background tasks/monitors, `/goal` from Stop-hook output, MCP http injection | `orchestration-v2/Adapters/ClaudeAdapterV2.ts` (9.9k), `ClaudeTaskOutputTail.ts`, `claudeWorkflowProgress.ts`, `provider/ClaudeProvider.ts`, `claudeModelOptions.ts` | XL |
| Cursor Agent SDK adapter | open/send + onDelta, MCP servers | `packages/provider-cursor`, `CursorAccountReader`, `CursorKeychain`, `CursorUsageAccounts` | L |
| ACP shared adapter + Grok flavor | JSON-RPC over NDJSON stdio, `session/new|load|fork`, xAI structured questions, steering by cancel+restart | `packages/effect-acp`, `packages/provider-acp`, `packages/provider-grok` | L |
| ACP Registry agents | Search official ACP registry, prepare binaries (checksummed versioned cache), npx/uvx global installs, sessions list/import/delete, URL auth | `packages/provider-acp-registry`, `AcpRegistrySupport`, `server.*AcpRegistry*` RPCs | L |
| OpenCode 1.x / 2.x | Version probe; 1.x one T3-managed server per thread; 2.x one server per instance with per-thread MCP entries & permission rules; HTTP+SSE races | `packages/provider-opencode` (`server/adapter.ts`, `v2/adapter.ts`, `OpenCode2Server.ts`, `OpenCodeServerOwner.ts`), `OpenCodeRuntime`, `OpenCodeServerLedger` | L |
| Pi (RPC mode) | User's `pi` in `--mode rpc` + T3 MCP bridge extension; session files back resume/rollback/fork (fork via CLI in destination dir) | `packages/provider-pi/src/server/adapter.ts` | M |
| Muse | Additional driver | `packages/provider-muse`, `Adapters/MuseAdapterV2.testkit.ts` | M |
| Antigravity | Per-instance account profiles, file credential store, scrubbed Google env, leased immutable installs, checkpoint-only (no conversation rollback) | `orchestration-v2/Adapters/AntigravityAdapterV2.ts`, `provider/AntigravityInstallation.ts`, `AntigravityAuth.ts`, `antigravityAuthSupport.ts`, `AntigravityUsage` | L |
| Provider instances | Multiple configured instances per driver, routed by instance id; enable/disable per project | `provider/ProviderRegistry.ts`, `ProviderInstanceRegistryHydration`, contracts `providerInstance.ts` | M |
| Provider status / health probes | Non-side-effecting probes, cached status snapshot, slash command + skill lists per provider | `provider/providerStatusCache.ts`, `packages/provider-core/src/server/snapshotProbe.ts`, `server.refreshProviders` | M |
| Provider install/update/maintenance | Installer-ownership proof (brew/npm/volta/native), self-updaters, version check, lock key | `provider/providerMaintenance.ts`, `providerMaintenanceRunner.ts`, `ProviderLatestVersions`, `provider.install.*`, `server.updateProvider` | L |
| Provider sign-in | Auth start/respond/complete/cancel/logout streams; ChatGPT handoff from local primary to remote env; Codex auth callback; Antigravity OAuth return URL | `provider/CodexChatGptHandoff.ts`, `AntigravityAuth.ts`, `ProviderAuthService`, `provider.auth.*` RPCs | L |
| Model catalog & manifest | Models, custom models, reasoning effort / fast mode / context window options, model classification | `ModelManifest`, `docs/internals/model-manifest.md`, contracts `model.ts`, `modelSelection.ts` | M |
| Runtime & interaction modes | approval-required / auto-accept-edits / auto / full-access; plan vs default; per-provider support lists | contracts `providerRuntime.ts`, `orchestration-v2/RuntimePolicy.ts`, `docs/user/permission-modes.md` | M |
| Usage limits & reset credits | Ingest provider rate-limit windows, reset times, consume reset credits | `ProviderUsageLimitsIngestion`, `UsageLimitSources`, `ResetCreditCoordinator`, `provider.consumeResetCredit` | M |
| Text generation | Thread titles, commit messages/PR text, review guides via helper provider calls (tool-denied) | `textGeneration/`, `textGeneration/AntigravityTextGeneration.ts` | M |
| Raw provider diagnostics | Rotating NDJSON logs, 64 KiB payload budget, delta filtering, redaction | provider transports, `docs/internals/providers.md` | M |
| Provider feedback upload | Forward feedback to provider | `provider.uploadFeedback`, adapter `uploadFeedback` | S |
| Session idle reaper / multi-thread sessions | 30 min idle, 4 h background pin, per-thread unload | `orchestration-v2/ProviderSessionManager.ts` | L |

### Orchestration/session model
| Feature | Description | Key paths | Cx |
|---|---|---|---|
| Command/event/outbox core | Orchestrator decide + EventSink txn + receipts + outbox + effect worker | `orchestration-v2/Orchestrator.ts`, `EventSink.ts`, `EffectOutbox.ts`, `EffectWorker.ts`, `CommandReceiptStore.ts` | XL |
| Execution graph projection | Runs, attempts, nodes, provider threads/turns, requests, turn items, positions | `ProjectionStore.ts` (6.9k), `TurnItemPositionStore.ts`, `RunExecutionService.ts`, `ProviderEventIngestor.ts` | XL |
| Turn start pipeline | Session ensure/resume, handoff resolution, history injection, attachments claim, compaction turns | `ProviderTurnStartService.ts`, `ContextHandoffDelivery.ts`, `AttachmentClaims.ts` | L |
| Queue / steer / restart | Dispatch modes, queue reorder/edit/cancel/promote, held queues | `CommandPolicy.ts`, `QueuedRunOrder.ts`, `ProviderTurnControlService.ts` | L |
| Interrupt / Stop cascade | `run.interrupt{holdQueue}`, `thread.stop`, delegated stop | `Orchestrator.ts`, `ThreadManagementService.ts` | M |
| Approvals & questions | RuntimeRequests live/message/not_resumable, dismiss, question attachments | `RuntimeRequestService.ts`, `DelegatedTaskQuestionReactor.ts` | M |
| Plan mode & proposed plans | Plan artifacts, todo lists, implement-plan (`sourcePlanRef`) | contracts, adapters | M |
| Forks / merge-back / provider switch | ContextTransfer + ContextHandoff, lazy native/portable resolution, budgeted handoff | `ThreadForkService.ts`, `ProviderSwitchService.ts`, `ContextHandoffService.ts`, `ProviderSessionTransitionPolicy.ts` | L |
| Delegated tasks / subagents | App-owned child threads, completion delivery cohorts, wake policy, native subagents | `Orchestrator.ts`, `mcp/OrchestratorMcpService.ts`, `NotificationMailbox.ts` | XL |
| Claude workflows | Workflow script/agents/phases, stop, transcript reads | `workflowAgentTranscript.ts`, `workflowFileRead.ts`, `workflowScriptQuery.ts` | M |
| Restart recovery & continuation | Reconcile outbox after process loss, continuation prompts, background-work notes | `ProviderRuntimeRecoveryService.ts`, `RestartContinuation.ts`, `RestartBackgroundNote.ts`, `ProviderContinuationService.ts` | L |
| Usage-limit auto-resume/snooze | Arm recovery, resume at reset | `UsageLimitRecoveryWorker.ts` | S |
| Thread launch saga | Worktree + setup script + thread + first message, resumable (`orchestration_v2_thread_launch_workflows`), `defer_start`/`prepared-run.*` | `ThreadLaunchService.ts`, `project/ProjectSetupScriptRunner.ts`, `WorktreeSetupTracker.ts` | L |
| Thread lifecycle/sidebar state | archive, delete, settle/auto-settle, snooze, pin/order keys, visit/unread, title regen | `ThreadLifecycleService.ts`, `ThreadSettlementService.ts`, `ThreadDeletion.ts`, `ThreadTitleRegenerationService.ts` | M |
| Live streams | Shell/thread/archived streams, bounded snapshots, history paging, coalescing, budgets | `ShellStream.ts`, `ThreadStream.ts`, `threadHistoryPaging.ts`, `LiveStreamBudget.ts`, `ThreadLiveEventCoalescer.ts`, `WireProjection.ts`, `ws.ts` | L |
| Command output hub | Separate bounded live output stream | `CommandOutputHub.ts` | M |
| Thread search / find | Cross-thread LIKE search; in-thread find with navigation | `ThreadSearch.ts`, `ThreadFind.ts`, `threadSearch.ts` (contracts) | M |
| User-facing errors | Failure classification/redaction | `UserFacingErrors.ts` | S |
| Legacy v1 import | Shell + lazy transcript import, 32k legacy handoff | `orchestration-v2/legacy/LegacyV1ThreadImporter.ts` | M (skip for Waku) |
| Outside agent session import | Scan/import Claude/Codex/ACP native sessions as threads (`thread.create.importedNativeThread`) | `project/AgentSessionScanner.ts`, `AgentSessionImporter.ts`, `claudeHistoryWorker.ts`, `agentSessions.*` | M |

### Checkpoints/rewind/git/worktrees/source control
| Feature | Description | Key paths | Cx |
|---|---|---|---|
| Hidden-ref checkpoints | Capture workspace into hidden git refs per scope, file summaries | `checkpointing/CheckpointStore.ts`, `orchestration-v2/CheckpointService.ts`, `CheckpointCaptureService.ts` | M |
| Turn / thread diffs | `getTurnDiff`, `getFullThreadDiff`, diff preview/file contents | `CheckpointDiffQuery`, `review/`, contracts `checkpointDiff.ts`, `review.ts` | M |
| Rewind / rollback | Restore files + provider rollback, shared-workspace safety | `CheckpointRollbackService.ts`, `CheckpointRestoreSafety.ts` | M |
| VCS driver & status | Git driver, status broadcaster, auto-pull policy, fetch backoff, refs, worktree create/remove, init, switch/create ref | `vcs/`, `git/GitManager`, `GitVcsDriver`, `VcsStatusBroadcaster`, `vcs.*` RPCs | L |
| Stacked git actions | commit/push/PR creation flows (`git.runStackedAction`), resolve PR → thread | `git/GitWorkflowService`, `git.*` RPCs | L |
| Worktrees & setup scripts | Managed worktree dir, setup progress stream/cancel, `t3.json` scripts incl. `runOnSettle` | `worktreesDirectory.ts`, `project/WorktreeSetupTracker.ts`, `T3ProjectFileLoader`, contracts `t3ProjectFile.ts`, `worktreeSetup.ts` | M |
| Pull requests | List/detail/checks/activity/comments/reviews/reactions/labels/reviewers/files-viewed, stacks, routing identity, linked threads | `pullRequest/` (4.9k lines), `PullRequestService`, `PullRequestReadCache`, `pullRequests.*` RPCs | XL |
| PR linking / sync / watch | Multi-link per thread, branch PR discovery, 2-min watch wakes, merge-driven settlement | `orchestration-v2/ThreadPullRequestService.ts`, `PullRequestSyncReactor.ts`, `PullRequestWatchReactor.ts`, `pullRequestWatch.ts` | L |
| PR review guides | Agent-written review tours in temp checkouts, stored with staleness | `PullRequestReviewGuideService`, `persistence/PullRequestReviewGuides.ts` | M |
| Source-control hosts | GitHub, GitLab, Bitbucket, Azure DevOps, Forgejo, GitCafe drivers; discovery, lookup, clone, publish; rate limiting | `packages/source-control-*`, `sourceControl/`, `SourceControlRateLimit` | L (GitHub only: M) |
| Project clone tracker | Clone with progress stream, cancel/retry | `project/ProjectCloneTracker.ts`, `gitCloneProgress.ts` | S |

### Composer inputs (attachments, mentions, skills, slash commands, voice, prompt links)
| Feature | Description | Key paths | Cx |
|---|---|---|---|
| Attachment store & uploads | Signed upload URLs, per-thread claims, cleanup effect, limits (100 files, 10 MiB img / 80 MiB total, 50 MiB files), text-file paste | `attachmentStore.ts`, `attachmentPaths.ts`, `orchestration-v2/AttachmentClaims.ts`, `ServerHttp.layerAttachmentUploadRoute`, `attachments.*` RPCs | M |
| Composer context records | `t3-context://v1/<kind>/<id>` references, attachment bindings, image inventory | contracts `composerContext.ts`, `docs/internals/composer-context-references.md` | M |
| Thread mention links | `t3-thread://v1/<threadId>` resolved per environment | contracts, `docs/orchestration-v2/orchestrator-mcp-server.md` | S |
| Slash commands & skills | Provider-discovered slash commands and skills in provider snapshot (`ServerProviderSlashCommand`, `ServerProviderSkill`), skill-aware thread search | contracts `server.ts:96-136`, `provider/*Provider.ts` | M |
| Native maintenance commands | `/compact`, `/logout`, `/goal` | `CommandPolicy.ts`, adapters | S |
| Prompt links | `app.t3.codes/new?prompt&repo&provider&model&pr` — client-side; server supplies PR resolution | `docs/user/prompt-links.md`, `git.resolvePullRequest` | S |
| Voice input | Documented in `docs/internals/voice-input.md`; no server voice RPC appears in `WS_METHODS` (it looks client-side) | — | S–M |

### Transcript content types
26 turn-item variants (§1.2), notifications, secret requests, handoff/fork/compaction lifecycle
items, subagent cards, MCP-app tool rows, HTML renders (`htmlRender/`, MCP `html_render`),
assistant citations (`docs/internals/assistant-citations.md`, contracts `assistantCitations.ts`),
tool presentation hints (`ToolActivitySurface/Icon/Source`). Cx: L for parity across all variants.

### Terminal/PTY
| Feature | Description | Key paths | Cx |
|---|---|---|---|
| PTY sessions | open/attach/observe/write/resize/clear/restart/close; shared sessions; metadata stream | `terminal/Manager.ts`, `NodePtyAdapter`, `terminal.*` RPCs | M (`portable-pty`) |
| History retention | 5,000 lines / 8 MiB cap, coalesced persistence, bounded tail restore, query stripping | `terminal/Manager.ts`, `docs/internals/terminal-runtime.md` | M |
| Terminal cleanup on settle/delete | `terminal.cleanup` effect, idle-prompt terminals closed on settle | `EffectWorker.ts`, `ThreadSettlementService.ts` | S |

### Files/diff/review
| Feature | Description | Key paths | Cx |
|---|---|---|---|
| Workspace file system | list/search entries, content search, read/write with path rules; filesystem browse/metadata | `workspace/WorkspaceFileSystem.ts`, `WorkspaceEntries`, `WorkspacePaths`, `projects.*`, `filesystem.*` | M |
| Signed asset URLs | Per-file inode-bound bearer URLs, range-friendly video serving | `assets/AssetAccess.ts`, `http.ts` | M |
| Diff review | Diff previews and file contents for turns, threads, PRs | `review/ReviewService`, `pullRequests.diffFileContents` | M |
| Open in editor | `shell.openInEditor`, external launcher | contracts `editor.ts`, `ExternalLauncher` | S |
| Browser preview | Server headless browser tabs, dev-server discovery, profiles, recordings, collaborative control | `preview/` (6.5k), `PreviewManager`, `PreviewBrowser`, `PortScanner`, `preview.*` | XL |
| HTML renders | Headless HTML render/preview | `htmlRender/` | M |

### Projects/threads management
Project registry (add/mutate/delete/scratch/create-new with managed folders), favicons/icons,
repository identity (upstream-aware), per-project settings overrides and defaults,
`t3.json` project file, enrichment — `project/ProjectService.ts`, `ProjectMutation.ts`,
`ManagedProjectFolders.ts`, `ProjectFaviconResolver.ts`, `RepositoryIdentityResolver.ts`,
`ProjectEnrichmentService.ts`, `orchestration-v2/ProjectCommands.ts`, `ProjectStore.ts`. Cx: M.
Threads: everything in §1.13 (Cx M–L). Scratch threads under `~/.t3/scratch` (S).

### Remote & multi-device (auth, pairing, devices, relay, tunnel, tailscale, ssh)
| Feature | Description | Key paths | Cx |
|---|---|---|---|
| Environment identity & descriptor | Stable env id, `/.well-known/t3/environment` with capability flags | `environment/ServerEnvironment.ts`, contracts `environmentHttp.ts` | S |
| Scoped sessions & pairing | Pairing links, bootstrap credentials, scopes/permissions, cookies/bearer/DPoP, ws tickets, client list/revoke | `auth/` (5.5k), `persistence/AuthSessions.ts`, `AuthPairingLinks.ts`, `auth/RpcAuthorization.ts` | L |
| MCP OAuth server | PKCE, stateless registration, read-only or ceiling grants | `auth/McpOAuth.ts` | M |
| T3 Connect / relay | Clerk-linked cloud identity, link proofs, mint credentials, managed Cloudflare tunnel, cloudflared pinning/upgrade, held webhooks waker, agent awareness for mobile | `cloud/CloudLink.ts`, `CloudManagedEndpointRuntime`, `relay/`, `packages/shared/src/relayClient.ts`, `HeldHooksWaker`, `AgentAwarenessRelay` | XL |
| Tailscale & direct endpoints | Advertise LAN/tailnet routes | `packages/tailscale`, `DirectEndpoints` | S–M |
| SSH launch/tunnel | Launch remote server + forward (desktop-owned) | `packages/ssh/src/tunnel.ts` | M |
| Devices (simulators/emulators) | expo-device-hub + agent-device installs, hub proxy, `device.action`, SSH device hosts | `device/` (4.2k), `DeviceHubProxy.ts`, `DeviceActions.ts`, `docs/internals/devices.md` | XL |
| Remote open targets / browser channel | Open URLs on desktop clients | `RemoteOpenTargets`, `DesktopBrowserChannel` | S |

### Notifications/background service/scheduled tasks
| Feature | Description | Key paths | Cx |
|---|---|---|---|
| Scheduled tasks | Interval / fixed-time / webhook triggers, run now, enable, rotate webhook token, delivery history (local + relay), bind to thread or launch new thread | `scheduledTasks/` (2.1k), `scheduling/`, `WebhookRoute`, tables `scheduled_tasks`, `scheduled_task_webhook_*` | L |
| Wake notifications | PR watch wakes, delegated completion wakes, background task notifications | `PullRequestWatchReactor.ts`, `NotificationMailbox.ts` | M |
| Background policy | Client activity + host power → background scheduling policy | `background/`, `BackgroundPolicy`, `HostPowerMonitor`, `server.report*` | M |
| Background service install | systemd/launchd service with stable launcher | `serviceLauncher.ts`, `cli/`, `docs/user/background-service.md` | M |
| Mobile/push notifications | Via relay device registration / live activities | contracts `relay.ts`, `docs/operations/android-notifications.md` | M |

### MCP & agent tools
App MCP server with 83 tools (§1.17), per-session credentials, OAuth for external agents, ACP stdio
bridge, Pi extension bridge, MCP Apps — `mcp/` (10.7k lines), `mcpApps/`. Cx: L.

### Usage/telemetry/activity/resource monitoring
| Feature | Description | Key paths | Cx |
|---|---|---|---|
| Usage summary & pricing | Token/cost accounting per provider/model, rate refresh | `usage/` (2.8k), `UsageService`, `server.getUsageSummary` | M |
| Context usage | Per provider thread context window usage, compaction thresholds | `ProviderThread.contextUsage`, adapters | S |
| Resource telemetry | Native Rust `sysinfo` monitor child, process tree history, signal process | `resourceTelemetry/` (3.6k), `native/resource-monitor/src/main.rs` | M (already Rust) |
| Diagnostics | Trace diagnostics, process diagnostics, heap snapshots, event-loop monitor | `diagnostics/`, `TraceDiagnostics`, `ProcessDiagnostics`, `HeapSnapshot`, `EventLoopMonitor` | M |
| Observability | OpenTelemetry, OTLP proxy route & endpoint check, rotating trace files | `observability/`, `ServerHttp.layerOtlpTracesProxyRoute` | M |
| Product analytics | Opt-out analytics, provider turn analytics, delegated-task analytics | `telemetry/`, `AnalyticsService`, `docs/internals/product-analytics.md` | S |

### Install/update/packaging
Launcher-owned self-update with trial/commit/rollback and SQLite snapshot (`serviceLauncher.ts`,
`ServerSelfUpdate`, `server.updateServer*`, `docs/internals/server-updates.md`, Cx L); desktop update
two-phase handoff (`desktopUpdate/`, `server.commitDesktopUpdate`, Cx M); CLI (`cli/`, `bin.ts`,
`binCli.ts`: serve, auth session issue, doctor, connect login/link, service commands — Cx M);
cloudflared install (`cloud.installRelayClient`, S); provider installers (see Providers).

### Persistence/migration
SQLite + migrator (64 migrations), `statev2.sqlite` copy-on-first-launch, projection maintenance,
database storage report and incremental vacuum, storage cleanup of worktrees/attachments, `t3
doctor` — `persistence/`, `storageCleanup.ts`, `DatabaseStorage`, `DatabaseMaintenance`. Cx: L
(schema design for Waku can start fresh and skip the v1 import).

### Misc
Server settings (`serverSettings.ts`, `settingsSecrets.ts`, `ServerSecretStore`, Cx M);
keybindings (`keybindings.ts`, S); environment theme (S); server lifecycle events (S); startup access
URLs (S); CORS and HTTP compression (S); outbound HTTP client (S); OS quirks (`os-jank.ts`, S);
`rpcInitialItems.ts` initial config payload (S); secret requests (S–M).

## 3. RPC / WebSocket method list and event types

Source of truth: `packages/contracts/src/rpc.ts` (`WS_METHODS`, `WsRpcGroup` at `:1855`, wrapped by
`RpcScopeAuthorization` middleware) and `packages/contracts/src/orchestrationV2.ts`
(`ORCHESTRATION_V2_WS_METHODS` at `:3061`). Transport: Effect RPC over WebSocket `/ws`
(`apps/server/src/ws.ts`), authenticated at upgrade (cookie or short-lived ws ticket), with every RPC
declaring a required permission (`apps/server/src/auth/RpcAuthorization.ts`). Permission vocabulary:
`access:read, access:write, diagnostics:read, filesystem:read, filesystem:write,
orchestration:read, orchestration:operate, preview:operate, providers:manage, relay:read,
relay:write, review:write, settings:write, source-control:write, terminal:read, terminal:operate`.
Methods named `subscribe*`/`*.subscribe*` are server streams.

### 3.1 Orchestration (V2)
`orchestration.dispatchCommand` (payload = `OrchestrationV2Command`, returns `{sequence}`) ·
`orchestration.getThreadProjection` · `orchestration.getTurnItem` · `orchestration.getTurnDiff` ·
`orchestration.getFullThreadDiff` · `orchestration.searchThreads` · `orchestration.searchThread` ·
`orchestration.searchThreadStream` · `orchestration.getArchivedShellSnapshot` ·
`orchestration.getWorkflowScript` · `orchestration.stopWorkflow` · `orchestration.launchThread` ·
`orchestration.subscribeShell` · `orchestration.subscribeArchivedShell` ·
`orchestration.subscribeThread` · `orchestration.subscribeCommandOutput`

### 3.2 Projects, files, filesystem, editor, agent sessions
`projects.listEntries` · `projects.readFile` · `projects.searchContents` · `projects.searchEntries` ·
`projects.writeFile` · `projects.mutate` · `projects.ensureScratch` · `projects.createNew` ·
(`projects.list`, `projects.add`, `projects.remove` are declared names with no RPC binding — legacy)
· `shell.openInEditor` · `filesystem.browse` · `filesystem.getMetadata` · `agentSessions.scan` ·
`agentSessions.import` · `projectClone.start` · `projectClone.cancel` · `projectClone.retry` ·
`subscribeProjectClones`

### 3.3 Assets & attachments
`assets.createUrl` · `assets.persistChatAttachments` · `attachments.createUploadUrl` ·
`attachments.delete`

### 3.4 MCP apps
`mcpApps.callTool` · `mcpApps.toolInfo` · `mcpApps.readResource` · `mcpApps.updateModelContext`

### 3.5 Providers (auth, install, feedback, usage credits)
`provider.uploadFeedback` · `provider.auth.start` · `provider.auth.respond` ·
`provider.auth.complete` · `provider.auth.cancel` · `provider.auth.logout` ·
`provider.auth.subscribe` · `provider.chatgpt.reconnect-profile` · `provider.chatgpt.import-profile`
· `provider.chatgpt.handoff.subscribe` · `provider.codex.auth-callback.subscribe` ·
`provider.install.start` · `provider.install.cancel` · `provider.install.subscribe` ·
`provider.install.remove` · `provider.consumeResetCredit`

### 3.6 Server / settings / diagnostics / ACP registry / usage / background
`server.probe` · `server.getConfig` · `server.refreshProviders` · `server.updateProvider` ·
`server.updateServer` · `server.updateServerWithProgress` · `server.commitDesktopUpdate` ·
`server.upsertKeybinding` · `server.removeKeybinding` · `server.runStorageCleanup` ·
`server.getStorageCleanupReport` · `server.getDatabaseStorage` · `server.getSettings` ·
`server.updateSettings` · `server.discoverSourceControl` · `server.searchAcpRegistry` ·
`server.prepareAcpRegistryAgent` · `server.uninstallAcpRegistryManagedBinary` ·
`server.acceptAcpRegistryUrlAuth` · `server.listAcpRegistrySessions` ·
`server.importAcpRegistrySession` · `server.deleteAcpRegistrySession` ·
`server.listAcpRegistryProviders` · `server.setAcpRegistryProvider` ·
`server.disableAcpRegistryProvider` · `server.logoutAcpRegistry` · `server.getTraceDiagnostics` ·
`server.getProcessDiagnostics` · `server.getHostResources` · `server.getProcessResourceHistory` ·
`server.getResourceTelemetryHistory` · `server.retryResourceTelemetry` ·
`server.checkOtlpEndpoint` · `server.signalProcess` · `server.reportClientActivity` ·
`server.reportHostPowerState` · `server.getBackgroundPolicy` · `server.getUsageSummary` ·
`server.refreshUsageRates` · `subscribeServerConfig` · `subscribeServerLifecycle` ·
`subscribeAuthAccess` · `subscribeBackgroundPolicy` · `subscribeResourceTelemetry`

### 3.7 VCS / git / review
`vcs.pull` · `vcs.refreshStatus` · `vcs.listRefs` · `vcs.createWorktree` · `vcs.removeWorktree` ·
`vcs.createRef` · `vcs.switchRef` · `vcs.init` · `git.runStackedAction` ·
`git.resolvePullRequest` · `git.preparePullRequestThread` · `review.getDiffPreview` ·
`review.getDiffFileContents` · `subscribeVcsStatus` · `subscribeWorktreeSetup` ·
`worktreeSetup.cancel`

### 3.8 Pull requests
`pullRequests.list` · `pullRequests.listStats` · `pullRequests.summary` · `pullRequests.routing` ·
`pullRequests.routingIdentity` · `pullRequests.stack` · `pullRequests.linkedThreads` ·
`pullRequests.detail` · `pullRequests.preview` · `pullRequests.checks` · `pullRequests.activity` ·
`pullRequests.reviewGuide` · `pullRequests.generateReviewGuide` · `pullRequests.threadComments` ·
`pullRequests.diffFileContents` · `pullRequests.filesViewed` · `pullRequests.setFilesViewed` ·
`pullRequests.runAction` · `pullRequests.update` · `pullRequests.comment` ·
`pullRequests.updateComment` · `pullRequests.submitReview` · `pullRequests.replyToThread` ·
`pullRequests.setThreadResolution` · `pullRequests.setReaction` · `pullRequests.invalidate` ·
`pullRequests.reportState` · `pullRequests.subscribeRefreshes` · `pullRequests.reviewerCandidates`
· `pullRequests.requestReviewers` · `pullRequests.labelCandidates` · `pullRequests.setLabels`

### 3.9 Source control hosting
`sourceControl.lookupRepository` · `sourceControl.cloneRepository` ·
`sourceControl.publishRepository`

### 3.10 Terminal
`terminal.open` · `terminal.attach` · `terminal.observe` · `terminal.write` · `terminal.resize` ·
`terminal.clear` · `terminal.restart` · `terminal.close` · `subscribeTerminalEvents` ·
`subscribeTerminalMetadata`

### 3.11 Preview (browser) and local servers
`preview.open` · `preview.navigate` · `preview.resize` · `preview.adjust` · `preview.refresh` ·
`preview.close` · `preview.list` · `preview.clearProfile` · `preview.reportProfiles` ·
`preview.reportStatus` · `subscribePreviewEvents` · `subscribeDiscoveredLocalServers`

### 3.12 Devices
`device.configure` · `device.list` · `device.testHost` · `device.open` · `device.close` ·
`device.shutdown` · `device.detail` · `device.action` · `subscribeDeviceState`

### 3.13 Scheduled tasks, secrets, cloud relay client
`scheduledTasks.list` · `scheduledTasks.subscribe` · `scheduledTasks.upsert` ·
`scheduledTasks.setEnabled` · `scheduledTasks.delete` · `scheduledTasks.runNow` ·
`scheduledTasks.rotateWebhookToken` · `scheduledTasks.listWebhookDeliveries` ·
`scheduledTasks.getWebhookDelivery` · `secrets.answerRequest` · `cloud.getRelayClientStatus` ·
`cloud.installRelayClient`

Total: 16 orchestration methods + 179 other bound methods (182 `WS_METHODS` keys, 3 unbound) = 195 RPCs.

### 3.14 HTTP API (non-WS) relevant to orchestration
From `packages/contracts/src/environmentHttp.ts`: `GET /.well-known/t3/environment` (descriptor &
capabilities), `GET /api/orchestration/shell`, `GET /api/orchestration/threads/:threadId`,
`…/:threadId/transcript`, `…/:threadId/bounded`, `…/:threadId/history`, `GET /api/projects`,
`POST /api/projects/mutate`, `POST /api/pull-requests/diff`, auth endpoints (`/api/auth/*`,
`/oauth/token`, `/oauth/mcp/*`, `/.well-known/oauth-*`), T3 Connect endpoints (`/api/connect/*`,
`/api/t3-connect/*`), scheduled-task webhook route (GET/POST/PUT/PATCH), plus asset, attachment
upload, OTLP trace proxy, static/dev routes and `/mcp` (see the remote/auth part of §2).

### 3.15 Command types (`OrchestrationV2Command`, client-dispatchable)
Thread lifecycle: `thread.create`, `thread.archive`, `thread.unarchive`, `thread.delete`,
`thread.settle`, `thread.auto-settle`, `thread.unsettle`, `thread.snooze`, `thread.unsnooze`,
`thread.auto-settle.set`, `thread.pin`, `thread.unpin`, `thread.pin.reorder`,
`thread.active.reorder`, `thread.visit`, `thread.mark-unread`, `thread.metadata.update`,
`thread.title.regeneration.complete`, `thread.runtime-mode.set`, `thread.interaction-mode.set`,
`thread.model-selection.set`, `provider.switch`, `provider-session.detach`.
Pull requests: `thread.pull-request.link`, `thread.pull-request.unlink`,
`thread.pull-request-link.sync`, `thread.pull-request.watch`, `thread.pull-request.sync`.
Messages/runs: `message.dispatch` (modes `start_immediately | queue_after_active | steer_active |
restart_active | defer_start`), `notification.delivery.accept`, `prepared-run.release`,
`prepared-run.progress`, `prepared-run.fail`, `prepared-run.retry`, `run.interrupt`,
`subagent.stop`, `queued-message.promote-to-steer`, `queue.resume`, `queued-run.reorder`,
`queued-run.cancel`, `queued-run.edit`, `runtime-request.respond`, `thread.user-input.dismiss`.
Checkpoints/lineage: `checkpoint.rollback`, `thread.fork`, `thread.merge_back`.
Delegation: `delegated_task.request`, `delegated_task.wake-policy`,
`delegated_task.completion-delivery.acknowledge`, `delegated_task.completion-delivery.dispose`,
`thread.created.record`.
**Internal-only** (`OrchestrationV2InternalCommand`, never accepted from clients):
`thread.pull-request-watch.sync`, `checkpoint.rollback.fail`, `thread.background-work.settle`,
`thread.stop`, `secret_request.record`.

### 3.16 Domain event types (`OrchestrationV2DomainEvent`, `orchestrationV2.ts:1594-1737`)
Common envelope: `{id, threadId, runId?, nodeId?, driver?, providerInstanceId?, rawEventId?,
occurredAt, type, payload}`; stored as `{sequence, commandId|null, event}`.
- Thread (payload = full `AppThread`): `thread.created`, `thread.archived`, `thread.unarchived`,
  `thread.deleted`, `thread.settled`, `thread.unsettled`, `thread.snoozed`, `thread.unsnoozed`,
  `thread.pinned`, `thread.unpinned`, `thread.pin-reordered`, `thread.active-reordered`,
  `thread.auto-settle-set`, `thread.visited`, `thread.marked-unread`, `thread.metadata-updated`,
  `thread.pull-request-synced`, `thread.runtime-mode-updated`, `thread.interaction-mode-updated`,
  `thread.model-selection-updated`, `thread.provider-switched`
- `thread.pull-request-link-updated` (ThreadPullRequestLink)
- `run.created`, `run.updated`, `run.background-work-cancelled`
- `run-attempt.created`, `run-attempt.updated`
- `node.updated`, `subagent.updated`
- `provider-session.attached`, `provider-session.updated`, `provider-session.detached`
- `provider-thread.updated`, `provider-turn.updated`
- `runtime-request.updated`, `message.updated`, `turn-item.updated`, `plan.updated`
- `checkpoint-scope.created`, `checkpoint.captured`, `checkpoint.rollback-requested`
- `context-handoff.updated`, `context-transfer.created`, `context-transfer.updated`
Project events keep their own application-event shape in the same `orchestration_events` log
(`ApplicationProjectEvent`, `orchestrationProject.ts`, `applicationEvent.ts`).

### 3.17 Stream item types
- Shell: `synchronized`, `snapshot`, `project.updated`, `project.removed`, `thread.updated`,
  `thread.removed` (+ archived shell variant).
- Thread: `synchronized`, `snapshot`, `event`, decode-only `unknown-event`.
- Command output: `{kind: replace|append, text, truncated, running}`.
- Outbox effect types: `provider-runtime.continue`, `provider-session.detach`,
  `provider-turn.start`, `provider-turn.interrupt`, `provider-turn.steer`,
  `provider-turn.restart`, `runtime-request.respond`, `provider-thread.rollback`,
  `checkpoint.capture`, `terminal.cleanup`, `preview.cleanup`, `attachment.cleanup`,
  `thread-title.generate`, `delegated-tasks.stop`.

## 4. Wire & performance techniques (and documented regressions to avoid)

Primary sources: `docs/internals/performance-regressions.md`, `docs/internals/terminal-runtime.md`,
`docs/internals/resource-telemetry.md`, `docs/internals/connection-runtime.md`,
`docs/internals/providers.md` (diagnostics), and the code files cited below. Regression command:
`vp run test:perf:v2-wire` (real v2 projection + client reducers; v1 fixtures give false comfort).

### 4.1 Snapshot + cursor (incremental sync)
- One global, monotonically increasing event `sequence` (SQLite `INTEGER PRIMARY KEY
  AUTOINCREMENT`). Every snapshot carries `snapshotSequence`; streams deliver only `sequence > N`.
  Clients resume with `afterSequence` (shell and thread streams).
- Optional `requestCompletionMarker` makes the server emit `{kind: "synchronized"}` once replay is
  caught up, separating "transport up" from "data fresh" (`connection-runtime.md`).
- Thread resume decision (`orchestration-v2/ThreadStream.ts`): replay at most **128 events** and
  **1 MiB** of projected event JSON, after a **1 MiB raw persisted-payload** preflight before
  decoding; otherwise send one fresh snapshot. Don't raise the budget because outputs project small:
  repeated large tool updates inflate server allocations.
- Subscribe to the live PubSub **before** reading the high-water mark, then replay the gap and
  dedupe (`LiveStreamBudget.replayAndBufferLiveEvents`); live buffer capped at **1,000 items / 8 MiB**
  — on overflow fail the subscription so the client resubscribes rather than buffering unboundedly.
- Sequence cursors must **seek an index**, never scan history: `idx_orchestration_events_
  application_high_water`, `idx_orchestration_events_agent_stream_sequence`, per-thread sequence
  index (`Migrations/OrchestrationV2/ApplicationEventSequenceIndexes.ts`). Unknown/legacy threads
  return 0 without scanning others. SQL uses `INDEXED BY` hints in hot queries
  (`persistence/OrchestrationEventStore.ts`).

### 4.2 Payload bounding
- **Bounded thread snapshot** for cold open: target **75 recent timeline rows / ~1 MiB**, up to 10
  user turns (`threadHistoryPaging.ts` `THREAD_HISTORY_PAGE_POLICY`); required control records
  (pending requests, dependency rows) always included; at least one row always admitted so an
  oversized item cannot deadlock paging; bounds per fork ancestor. Opaque history cursor ≤4,096
  chars; older pages via HTTP (`/api/orchestration/threads/:id/history`), merged without disturbing
  the live scroll window. Older clients/WebSocket-only starts still get a full-snapshot fallback
  (capability negotiation via `acceptBoundedSnapshot`, `acceptCompactTurnItems`).
- `turnItemsOmitLocalVisible`: opt-in compaction that drops duplicated local rows from
  `projection.turnItems` when they already appear in `visibleTurnItems`.
- Bounded snapshot must **not** retain the full duplicate conversation-message table.
- **Wire projection** (`WireProjection.ts`): raw command output, dynamic-tool result bodies and
  inline file-change bodies are omitted on the wire (even small ones) with `outputOmitted` and
  `outputIndicatesFailure` flags; dynamic values > limit become a ≤160-char first-line summary;
  handoff summary text stripped. Persisted events stay complete; `orchestration.getTurnItem` and the
  diff endpoints serve full content on demand. Explicit cross-environment transcript exports keep
  full bodies but reject JSON above the attachment limit.
- File-change diffs truncated even **in storage** (`boundFileChangeForStorage`) since clients can
  never receive more.
- Images inside tool output are referenced by markers/index and loaded separately
  (`toolOutputImageBytes.ts`).
- Initial shell contains only active navigation rows; archived threads via a dedicated query;
  transcript bodies never in the shell. Shell resume sends deltas + compact repository-enrichment
  metadata (not a full project/thread snapshot); unchanged thread shells are skipped but re-sent
  after 5 s so a skipped delta can't strand a cursor (`ShellStream.ts`).
- Shell snapshot reads in one transaction but decodes after commit (single shared SQLite connection;
  decoding is slower than reading).
- Project enrichment (git identity) uses only immediately-available data in snapshots; awaiting git
  probes can exceed the client's snapshot budget (`orchestration-v2/http.ts`).

### 4.3 Live-output isolation and coalescing
- Command output deltas are **never orchestration events**: adapters push `command_output.delta`
  to `CommandOutputHub`, which keeps one bounded tail per running command (64 live, 16 finished),
  throttles subscribers (100 ms frames, 500 ms after overflow, 16 KiB append cap) and serves only
  clients with the row expanded (`orchestration.subscribeCommandOutput`). Routing deltas through
  `turn_item.updated` would flood the event store and block ingestion for every thread.
- Running tool `turn-item.updated` events coalesced in 50 ms windows (≤512 pending)
  (`ThreadLiveEventCoalescer.ts`); assistant text deltas coalesced in adapters
  (`Adapters/ProviderTextDeltaCoalescer.ts`, `assistantStreaming.ts`).
- Provider diagnostic logs filter token deltas and duplicate frames before copying/redacting; 64 KiB
  per-payload encoded budget with structural summaries; bounded traversal before serialization;
  Codex opts out of `turn/diff/updated` (diffs come from checkpoints).

### 4.4 Server-side dispatch and reads
- Auto/steer/restart sends resolve delivery inside the server's per-thread lock from authoritative
  state — no client pre-fetch of the full projection. Model selection and identified-checkpoint
  rollback also dispatch without a projection read when the server advertises support.
- Read receipts (`thread.visit`) load only canonical thread metadata, advance `lastVisitedAt`
  monotonically, don't bump `updatedAt` or decode history.
- Shell queries use a covering thread/run index for item counts and a partial index for
  nonterminal background items; latest user-message reads use an update-time index
  (`Migrations/OrchestrationV2/ShellIndexes.ts`, `060_ThreadSnapshotWindowIndexes.ts`,
  `062_ThreadFailureLookupIndex.ts`).
- Settlement worker reads only candidate rows (`ProjectionStore.getSettlementCandidates`): no
  archived histories, fork ancestry, transcript counts or provider sessions.
- Startup recovery selects candidates from projection state (queued runs, pending requests, live
  sessions, background work, unfinished delegated deliveries) before reading full projections
  (`Migrations/OrchestrationV2/RecoveryIndexes.ts`).
- Projection verification/rebuild decodes in bounded pages and replays event pages inside one
  transaction — never collect the whole log.

### 4.5 I/O and background work
- Trace records and provider logs use **async serialized writes with rotation**; synchronous
  appends stall the event loop on slow disks (Windows especially). While a write is slow, producers
  keep up to one trace file of records; provider logging keeps backpressure; shutdown drains.
- Relay awareness (mobile live activity): one pending publication per thread with a follow-up slot;
  transcript deltas/tool output don't enqueue; ≤5 retries with exponential delay, reset by new
  relevant events.
- VCS: command-palette VCS/PR queries are leased to the visible viewport; local git status skips
  divergence unless needed; cached fetch failures share backoff.
- Terminal history: incremental chunk appends (never re-materialize scrollback per callback);
  server cap 5,000 lines **and** 8 MiB UTF-8 per terminal; eviction never splits code points;
  coalesced persistence reading the latest buffer at write time; restore reads only the bounded tail
  and skips incomplete UTF-8 prefixes; query/response escape sequences stripped from retained
  history; client cap 512 KiB.
- Resource telemetry: native Rust child samples only while diagnostics has live subscribers
  (interval 0 = on-demand); history bounded by age, count, rows and bytes; Linux per-task
  enumeration disabled; process identity = pid + start time.
- Effect outbox: post-commit notify for latency, durable `available_at` backoff for retries, 30 s
  liveness poll; per-thread FIFO; title generation in its own lane.
- Provider sessions: 30 min idle release, pinned up to 4 h while background work runs.
- SQLite: WAL, `synchronous=NORMAL`, incremental auto-vacuum, `journal_size_limit`, busy timeout
  5 s, `PRAGMA checkpoint_fullfsync`.

### 4.6 Compatibility techniques
- `ORCHESTRATION_PROTOCOL_VERSION` gate at `/ws` (HTTP 426) + environment descriptor check.
- Forward-compatible unions (unknown event/turn-item kinds decode to skippable records; cursor
  still advances); decoding defaults for new fields so old persisted events replay.
- Capability flags in the environment descriptor (e.g. `threadPullRequests`,
  `threadPullRequestLinking`) instead of client version checks; legacy wire fields kept.
- Persisted file-attachment events broke image-only servers on replay — rollouts must account for
  stored history (`providers.md`).

### 4.7 Regressions explicitly called out (do not repeat)
1. Benchmarking v2 clients with v1 fixtures (misses the active path).
2. Retaining the full message table in bounded snapshots.
3. Raising replay budgets because projected output looks small.
4. Routing command output deltas through `turn_item.updated`.
5. Sending full project+thread snapshots on shell resume.
6. Scanning event history for cursors / for legacy or unknown threads.
7. Loading completed thread history to discover "no recovery needed".
8. Collecting the entire event log during projection rebuild.
9. Settlement/read-receipt paths decoding full histories.
10. Synchronous trace/log appends.
11. Materializing full terminal scrollback per PTY callback; reading whole legacy history files at
    startup.
12. Treating smaller socket frames as proof of whole-server memory reduction.
13. Awaiting git-backed enrichment inside shell snapshot responses.
14. Unbounded live-subscription buffering while a catch-up query blocks.


---

## Appendix A. Detailed catalogue: Git / checkpoints / worktrees / source control / files / projects / preview

From a delegated read-only sub-audit. Large files were mapped from their structure rather than read line by line.

I audited the T3 Code server slice for Git, checkpoints, worktrees, source control, files, projects and previews, reading only. The catalogue below has about 75 features, each with a description, key source paths and a size estimate for a Rust rewrite (S, M, L, XL). After the features come the exact method strings, routes and tool names, then a short list of cross-cutting rules.

I mapped the large files (`apps/server/src/vcs/GitVcsDriverCore.ts`, `apps/server/src/git/GitManager.ts`, `apps/server/src/pullRequest/PullRequestService.ts`, the GitHub package) from their function lists, constants, comments and contracts, not line by line. The exact git argument sequences in status and push are not listed here and need a direct read before porting. Per-host details below are summarised; a wire-compatible rewrite needs each host file read in full.

All paths are relative to the repo root `t3code/`.

---

# A. Version control base layer (`apps/server/src/vcs/`)

**A1. VCS driver abstraction and registry** – M
- One driver interface: `execute`, `detectRepository`, `isInsideWorkTree`, `listWorkspaceFiles`, `listRemotes`, `filterIgnoredPaths`, `initRepository`, plus optional `checkpoints` and `getDiffPreview`.
- The registry detects which driver applies to a cwd.
  - Detection results are cached (2048 entries, 5 min TTL) and re-checked against the disk on each hit.
  - It resolves "auto" vs a requested kind and fails with `VcsUnsupportedOperationError` when an operation is missing.
- Driver kinds are `git`, `jj` and `unknown`. Only `git` is implemented; jj appears in discovery only.
- Paths: `apps/server/src/vcs/VcsDriver.ts`, `VcsDriverRegistry.ts`, `packages/contracts/src/vcs.ts` (kinds, capabilities, freshness, repository identity, the `VcsError` union).

**A2. Per-project VCS kind override (`.t3code/vcs.json`)** – S
- Walks up from the cwd looking for `.t3code/vcs.json` and reads `vcs.kind` or the legacy `vcsKind`.
- Path: `apps/server/src/vcs/VcsProjectConfig.ts`.

**A3. Process runner for git and host CLIs** – M
- Concurrency caps: at most 8 VCS processes and at most 4 `gh` processes at once.
- Defaults: 30 s timeout and 1 MB output cap, after which output is truncated and `[truncated]` appended.
- Git failures are sorted into fixed reason codes (`GitCommandFailureReason`), so stderr text, which can contain credentials, never reaches clients.
- Recognises a held `index.lock` and shows a dedicated message (`GIT_INDEX_LOCKED_DETAIL`).
- Config is passed through `GIT_CONFIG_COUNT`/`KEY`/`VALUE` environment entries rather than argv (`gitConfigEnv`), with Windows case handling.
- Sets the safe bare-repository environment (`explicitBareRepositoryEnv`) so git will not open a folder that merely looks like a bare repo.
- On macOS, retries with `core.fsyncMethod=fsync` when a full fsync is rejected.
- Paths: `apps/server/src/vcs/VcsProcess.ts`, `apps/server/src/processRunner.ts`.

**A4. Git driver core (large)** – XL. One file of about 4,200 lines implements everything below.

- **Command execution**
  - Uses git trace2 to capture hook start and finish events.
  - Collects output line by line, including checkout progress lines (`Updating files: N% (a/b)`).
  - Sets a Windows long-path config. Option-like ref names are refused.
- **Status**
  - Reads `status --porcelain=v2` and branch ahead/behind counts.
  - Builds a per-file insertions/deletions list for the working tree.
  - Computes branch change totals against the base branch, and the ahead count against the default branch.
  - Base branch fallback is `main`, then `master`.
  - A non-repository returns a fixed placeholder status.
- **Background fetch with backoff**
  - Fetches the upstream every 15 s with a 5 s timeout.
  - Fetches run without prompts (`GIT_TERMINAL_PROMPT=0`, askpass disabled).
  - Failures back off exponentially from 30 s to 15 min. Cached default-branch and origin-exists answers live 5 min.
- **Caches**
  - Repository paths: 10 min TTL, coalesced within 5 s.
  - Ref listing snapshot: 2 min TTL, 30 s cooldown after a failure.
- **Ref listing**
  - Paginates with a default limit of 100 and a maximum of 200.
  - Supports a query filter, local/remote/all kinds and matching remote refs.
  - Marks refs checked out in other worktrees.
  - Reads refs from the git common directory as a snapshot.
- **Commits**
  - `prepareCommitContext` returns staged patches, capped at 49 KB, for message generation.
  - `commit` stages the selected file paths, reports hook progress and has a 10 min timeout.
- **Push and pull**
  - `pushCurrentBranch` handles upstream setup, choice of push remote and publish branch name.
  - `pullCurrentBranch` returns `pulled` or `skipped_up_to_date`.
- **Range context**
  - `readRangeContext` returns commit summaries and the diff for a range, capped at 19 KB and 59 KB.
- **Worktrees**
  - Create: `git worktree add [-b new] -- path ref` with `checkout.workers` set and `GIT_PROGRESS_DELAY=0`.
  - Submodules are then initialised in recursive, top-level or none mode, taken from settings or `t3.json`.
  - Create has a 300 s timeout. Progress callbacks: worktree claimed, checkout percent, submodule start/line/finish/disabled.
  - Remove (5 min timeout), prune and list are also implemented.
- **Branches**: create ref, switch ref, rename branch, delete local branch, list local branch names, `initRepo`.
- **Remotes**
  - `ensureRemote`, primary remote name, push remote name, `fetchRemote` (retries once on a ref-lock race).
  - Fetch a remote branch or a remote-tracking branch, set the upstream, check whether a remote or remote branch exists.
  - Resolve a remote-tracking commit and the default branch name.
- **Pull request branches**
  - Fetch a PR branch or a PR head commit, and resolve a commit.
  - `refreshCheckedOutBranch` fast-forwards (`merge --ff-only`) or, for a rewritten head, runs `reset --merge`.
  - Before a reset it saves the old head to `refs/t3code/pre-refresh`.
- **Review diffs**: `getReviewDiffPreview` and `getReviewDiffFileContents` (see F1).
- Paths: `apps/server/src/vcs/GitVcsDriverCore.ts`, `apps/server/src/vcs/GitVcsDriver.ts` (interface, about 40 operations), `apps/server/src/git/remoteRefs.ts`.

**A5. Workspace file listing and ignore checks via git** – S
- `ls-files --cached --others --exclude-standard`, capped at 16 MB of output.
- `check-ignore --no-index -z --stdin` in chunks of up to 256 KB, run with `core.fsmonitor=false` and `core.untrackedCache=false`.
- `listRemotes` parses `git remote -v`.
- Path: `apps/server/src/vcs/GitVcsDriver.ts`.

**A6. Repository init** – S
- RPC `vcs.init` goes through `VcsProvisioningService`, then refreshes git status.
- Paths: `apps/server/src/vcs/VcsProvisioningService.ts`, `GitVcsDriver.ts` (`initRepository`).

**A7. Live status broadcaster (subscription)** – L
- Caches local and remote status per real path and publishes only when a fingerprint changes.
- Stream events: `snapshot`, `localUpdated`, `remoteUpdated`.
- Remote pollers are reference-counted; passive subscribers (`includeRemote: false`) do not keep a poller alive.
- Default poll interval is 30 s; refresh failures back off exponentially from 30 s to 15 min with sanitised diagnostics.
- A per-cwd lock on remote writes stops an older poll from overwriting a newer pull request result.
- Respects the background power policy.
- Explicit `refreshStatus` and `refreshPullRequestStatus` re-resolve the checked-out branch.
- Path: `apps/server/src/vcs/VcsStatusBroadcaster.ts`.

**A8. Automatic pull of the default branch** – M
- Enabled per project (`autoPull`) or through server settings.
- Pulls only when it can fast-forward, the checkout is clean and has no local commits. It skips other branches and branches without an upstream.
- Paths: `apps/server/src/vcs/VcsStatusBroadcaster.ts` (`VcsAutoPullPolicy`, `maybeAutoPull`), `docs/user/project-settings.md`.

---

# B. Checkpoints (hidden refs), diffs, rewind

**B1. Hidden-ref checkpoint capture** – XL
- Snapshots the whole working tree (tracked and untracked, not ignored) into a commit stored under a hidden ref.
  - Version 2 refs: `refs/t3/orchestration-v2/checkpoints/<base64url(first 32 hex of sha256(scopeId))>/ordinal/<n>`.
  - Legacy refs: `refs/t3/checkpoints/<base64url(threadId)>/turn/<n>` (`checkpointing/Utils.ts`).
- Capture steps:
  - Uses a temporary index at `<gitCommonDir>/t3-checkpoint-index-<uuid>`, removed afterwards. Split-index and sparse-index settings are guarded.
  - Seeds that index from the real index to keep git's racy timestamp, or from `read-tree --reset HEAD`.
  - Sparse checkout support: skip-worktree flags and `sparse-checkout check-rules`. A non-cone index is not rebuilt, to avoid recording false deletions.
  - Stages everything with `add`. When an embedded repository without commits blocks staging, those paths are found and excluded.
  - Runs `write-tree`, then `commit-tree` with HEAD as parent and message `t3 checkpoint ref=<ref>`, then `update-ref`.
  - Writes are durable (core.fsync settings) so a crash cannot leave 0-byte refs.
  - Recovery is bounded to 64 candidates and 5 s.
  - Captures run under a per-cwd lock.
- Paths: `apps/server/src/vcs/GitVcsDriver.ts` (`checkpoints.captureCheckpoint` etc.), `apps/server/src/checkpointing/CheckpointStore.ts`, `checkpointing/Errors.ts`, `apps/server/src/orchestration-v2/CheckpointService.ts`.

**B2. Checkpoint scopes and the per-run lifecycle** – L
- A scope is either `root_run` or nested. It carries the cwd, provider thread, ordinal and `advancesAppRunCount`.
- Operations:
  - Prepare the root scope.
  - Capture a baseline at ordinal 0, skipped if the ref already exists.
  - Materialise the baseline as a checkpoint (status `ready` or `missing`).
  - Capture at the end of a run: status `ready`, `missing` (not a git repo) or `error`, plus a numstat file summary.
  - Delete stale refs.
- The capture service is at-least-once and idempotent. It does not overwrite a stopped run's checkpoint and keeps a run discarded if a rollback landed first.
- Domain events: `checkpoint-scope.created`, `checkpoint.captured`, `checkpoint.rollback-requested`. Thread item type `checkpoint`.
- Paths: `apps/server/src/orchestration-v2/CheckpointService.ts`, `CheckpointCaptureService.ts`, `packages/contracts/src/orchestrationV2.ts`.

**B3. Turn diff and full-thread diff queries** – M
- RPCs `orchestration.getTurnDiff` and `orchestration.getFullThreadDiff`.
- Turn diff:
  - Finds ready checkpoints of completed runs and resolves the from/to refs. From-turn 0 uses ordinal 0 of the root scope; consecutive turns use the parent checkpoint.
  - `git diff --patch|--numstat -z --no-color --no-ext-diff --no-textconv --src-prefix=a/ --dst-prefix=b/ [--ignore-all-space]`, capped at 10 MB.
  - Whitespace is ignored by default.
  - Path batches are kept under 24,000 characters for the Windows command-line limit and run 4 at a time.
- Full-thread diff is the same with from-turn 0.
- Paths: `apps/server/src/checkpointing/CheckpointDiffQuery.ts`, `checkpointing/Diffs.ts`, `packages/contracts/src/checkpointDiff.ts`.

**B4. Filtering out files git imported (authored-path detection)** – M
- `listAuthoredPaths` compares the HEAD recorded in each checkpoint. It uses `log --no-walk --format=%P %ct` and diffs each head against its checkpoint.
- It also uses `log --diff-merges=remerge` so merge conflict fixes count as the turn's work while clean upstream changes do not.
- Files that git brought in (pull, merge, rebase) are dropped from the turn diff unless the turn edited them.
- Returns null when HEAD did not move, meaning every changed file belongs to the turn.
- Paths: `apps/server/src/vcs/GitVcsDriver.ts` (`listAuthoredPaths`), `checkpointing/Diffs.ts` (`isGitImport`), `docs/user/source-control.md` ("Review turn changes").

**B5. Rewind and restore** – L
- Command `checkpoint.rollback {threadId, scopeId, checkpointId, restoreFiles?}`; internal failure command `checkpoint.rollback.fail`.
- Restore steps:
  - `ls-files --with-tree`, then `git restore --source <commit> --worktree --staged -- .`.
  - Then `clean -fd -- .` with handling for Windows and for git failing to remove `./`, then `reset --quiet -- .`.
  - Long paths are forced on for Windows.
- Rollback also:
  - Recreates a missing managed worktree (prune, then create).
  - Checks that the workspace is isolated (B6).
  - Opens a provider session and calls `rollbackThread` for the provider-side turns.
  - Marks rolled-back runs, attempts and nodes, and deletes stale checkpoint refs.
  - Writes events under the thread-command lock.
- Error reasons: `rollback-target-invalid`, `active-provider-changed`, `provider-turn-unavailable`, `shared-workspace`, `unexpected-failure` (the git index-lock message is passed through).
- Paths: `apps/server/src/orchestration-v2/CheckpointRollbackService.ts`, `CheckpointService.ts` (`restore`), `GitVcsDriver.ts` (`restoreCheckpoint`).

**B6. Shared-workspace safety for restore** – M
- Restoring files is refused unless the thread has its own worktree that no other live thread, checkpoint scope or active provider session uses (compared by real path, both directions of containment).
- Message `SHARED_WORKSPACE_RESTORE_MESSAGE`.
- Path: `apps/server/src/orchestration-v2/CheckpointRestoreSafety.ts`.

**B7. Fork and merge-back from a checkpoint point** – M
- Commands `thread.fork` and `thread.merge_back`, with source point `latest_stable`, `run` or `checkpoint`.
- A fork copies pull request links but not the watch state.
- Paths: `apps/server/src/orchestration-v2/ThreadForkService.ts`, `packages/contracts/src/orchestrationV2.ts` (`OrchestrationV2ThreadForkSourcePoint`).

---

# C. Worktrees

**C1. Worktree creation RPC and managed location** – M
- `vcs.createWorktree {cwd, refName, newRefName?, baseRefName?, path|null}`. With a null path the server picks `<worktreesDir>/<repo>/<branch with / replaced by ->`.
- `worktreesDirectory` setting:
  - Expands `~` and must be absolute. A filesystem root is refused.
  - `previousWorktreesDirectories` keep older locations managed.
- `makeIsManagedWorktreePath` accepts only `<managed root>/<repo>/<branch>`, compared by real path. This is a security check, because paths come from clients.
- Paths: `apps/server/src/worktreesDirectory.ts`, `apps/server/src/git/GitManager.ts` (`createWorktree`), `GitVcsDriverCore.ts`.

**C2. Worktree removal and pruning** – S
- `vcs.removeWorktree {cwd, path, force?}`; prune is used before recreating.
- Paths: `GitVcsDriverCore.ts` (`removeWorktree`, `pruneWorktrees`, `listWorktreePaths`).

**C3. Thread launch with a workspace strategy** – XL
- Strategies: `root {branch?}`, `existing_worktree {worktreePath, branch?}`, `worktree {baseRef, branch?, startFromOrigin?}`.
- Preparation runs in the background:
  - Fetch the base when starting from origin.
  - Check out with progress, initialise submodules, run the setup script, then start the agent.
  - Validates base refs: `:/text` and `-` are refused, remote-tracking refs are allowed.
  - Starts on a temporary branch `t3/<8 hex>` (flattened to `t3-<hex>` on collision).
  - Then generates a branch name with the text model (static or semantic prefix, or custom instructions) and renames the branch, also updating subagents.
  - A failed preparation removes the worktree. Retries recover the original thread id. Scratch projects get their own folder.
- Failure code `workspace_preparation_failed`.
- Paths: `apps/server/src/orchestration-v2/ThreadLaunchService.ts`, `packages/shared/src/git.ts` (`buildTemporaryWorktreeBranchName`, `isTemporaryWorktreeBranch`, `formatGeneratedBranchName`, `sanitizeBranchFragment`), `docs/user/project-settings.md` ("Worktree branch names").

**C4. Worktree setup progress tracker** – M
- Per-thread snapshot with stages `fetch | checkout | submodules | setup-script | agent`.
- Stage status: `pending | running | done | skipped | warning | failed`. Phase: `running | done | failed | cancelled`.
- Keeps a tail of 4 lines; finished snapshots are kept 30 s. Cancellable until marked uncancellable.
- RPCs `subscribeWorktreeSetup` and `worktreeSetup.cancel`.
- Paths: `apps/server/src/project/WorktreeSetupTracker.ts`, `packages/contracts/src/worktreeSetup.ts`.

**C5. Setup and settle scripts** – M
- Runs a project script in a T3 terminal.
- Completion is detected with a per-run random sentinel (`__T3_SETUP_DONE__<token>`). The command is wrapped in a block so heredocs and comments cannot swallow the sentinel.
- Output lines are capped at 400 characters (partial lines at 4,096).
- Script options:
  - `runOnWorktreeCreate`, with `async` true by default (the agent starts while the script runs).
  - `runOnSettle`, which runs in the thread's own worktree only.
  - `previewUrl` and `autoOpenPreview`.
- Paths: `apps/server/src/project/ProjectSetupScriptRunner.ts`, `apps/server/src/orchestration-v2/ThreadSettlementService.ts` (settle script), `packages/contracts/src/project.ts` (`ProjectScript`).

**C6. Worktree handoff MCP tools** – M
- `t3_worktree_handoff {branch, baseRef?, startFromOrigin?, runSetupScript?, continuationPrompt?}`.
  - Moves a running thread into a new worktree and detaches the provider session.
  - The continuation is queued, started, steered or restarted.
- `t3_worktree_status` and `t3_worktree_list`.
- Failure codes include `already_in_worktree`, `handoff_in_progress` and `thread_credential_required`.
- Paths: `apps/server/src/mcp/WorktreeMcpService.ts`, `apps/server/src/mcp/toolkits/worktree/{tools,handlers}.ts`, `packages/contracts/src/worktreeMcp.ts`.

**C7. Worktree cleanup policies (storage cleanup)** – L
- Rules:
  - Inactive for N days, or a number of days after the thread settles.
  - After merge. The merge must be proven: commits contained in the remote default branch, or for a squash/rebase merge the PR's last commit equals the head.
  - No commits beyond the default branch.
  - Delete together with deleted threads.
- What protects a worktree from removal:
  - Uncommitted, any-local or tracked-edits modes (`node_modules` is ignored by these checks).
  - Active sessions, terminals and shared worktrees.
  - A 60 s grace after the settle script and a 1 h grace for orphans.
- Reports orphan folders with their sizes. Per-project inherit/off/custom. Runs at startup, when changed and hourly.
- Paths: `apps/server/src/storageCleanup.ts`, `docs/user/project-settings.md` ("Storage cleanup").

**C8. Workspace lease and lock** – S
- Per-cwd semaphore shared by storage cleanup and rollback.
- Path: `apps/server/src/workspace/workspaceLease.ts`.

---

# D. Branch, commit, push and pull request creation (Git actions)

**D1. Stacked Git action (commit, push, create PR)** – XL
- RPC `git.runStackedAction {actionId, cwd, action, commitMessage?, featureBranch?, filePaths?, threadId?, projectId?}`.
- Actions: `commit`, `push`, `create_pr`, `commit_push`, `commit_push_pr`.
- Returns a stream of `GitActionProgressEvent`: `action_started`, `phase_started` (branch, commit, push, pr), `hook_started`, `hook_output`, `hook_finished`, `action_finished`, `action_failed`.
- Steps:
  - Optional feature branch (auto-named, fallback `feature/update`).
  - Commit with an AI-generated or custom message. Messages are sanitised; the generator can also suggest a branch.
  - Push with upstream setup.
  - Create the PR with an AI title and body. The body is written to a file; the host's PR template is read (GitHub `gitHubChangeRequestTemplate.ts`). An existing open PR is detected and opened instead.
- Results report a status per step plus a toast with a call to action (`open_pr` or `run_action`).
- Afterwards: link the created PR to the thread (`linkCreatedPullRequest`), refresh pushed PRs, refresh git status.
- Writing style and model come from settings (repository conventions mode).
- Paths: `apps/server/src/git/GitManager.ts` (`runStackedAction`, `runCommitStep`, `runPrStep`, `runFeatureBranchStep`, `buildCompletionToast`), `apps/server/src/git/GitWorkflowService.ts`, `apps/server/src/git/linkCreatedPullRequest.ts`, `apps/server/src/git/refreshPushedPullRequests.ts`, `apps/server/src/ws.ts` (handler), `packages/contracts/src/git.ts`.

**D2. Branch pull request lookup for status** – L
- Finds the open PR for the current branch across remotes, including forks pushed without an upstream.
- Matches the head identity (owner/repository), probes several head selectors and finds the published-branch remote.
- Caches: 60 s for a hit, 5 min when there is no open PR, failures back off from 20 s to 15 min, git state 5 min, status results 1 s.
- Paths: `GitManager.ts` (`lookupStatusPr`, `lookupBranchPullRequest`, `resolveBranchHeadContext`, `findOpenPr`, `matchesBranchHeadContext`, `prLookupFailureTtl`).

**D3. Branch operations** – S each
- `vcs.listRefs`, `vcs.createRef {refName, switchRef?}`, `vcs.switchRef`, rename, delete local branch.
- `sanitizeNewRefName` and `sanitizeFeatureBranchName` in `packages/shared/src/git.ts`.
- Path: `GitVcsDriverCore.ts`.

**D4. Pull** – S
- RPC `vcs.pull`, which refreshes status afterwards. Paths: `GitVcsDriverCore.ts` (`pullCurrentBranch`), `ws.ts`.

**D5. Check out a pull request into a thread** – L
- `git.resolvePullRequest {cwd, reference}`.
- `git.preparePullRequestThread {cwd, reference, mode: local|worktree, threadId?}`:
  - Materialises the head branch, using local name `t3code/pr-<n>/<suffix>` for fork collisions.
  - Configures the upstream of a fork head remote, preferring SSH when the origin uses SSH.
  - Refreshes a reused worktree.
  - Returns `isOnPullRequestHead`.
- Path: `GitManager.ts` (`preparePullRequestThread`, `materializePullRequestHeadBranchBase`, `configurePullRequestHeadUpstreamBase`).

**D6. Remove agent credits from merge messages** – S
- Strips recognised agent co-author and "generated-by" lines (Copilot only when its name matches).
- Path: `packages/source-control-core/src/server/mergeMessage.ts`.

---

# E. Pull requests

**E1. Pull request service (multi-repository, cached)** – XL
- Read operations:
  - `list`: state, involvement, filters, projects, host, query, cursors; searches across repositories, 12 at a time.
  - `listStats`, `summary`, `stack`, `detail`, `preview`, `checks`, `activity`, `threadComments`.
  - `diff` (cursor paged, optional single commit), `diffFileContents`.
  - `filesViewed` / `setFilesViewed`, `reviewerCandidates`, `labelCandidates`, `linkedThreads`.
- Write operations:
  - `runAction` (stack scope with expected stack heads), `update` (title/body), `comment`, `updateComment`.
  - `submitReview` (verdict plus inline drafts positioned as added/deleted/context lines), `replyToThread`, `setThreadResolution`, `setReaction`.
  - `requestReviewers`, `setLabels`, `invalidate`, `reportState`, `subscribeRefreshes`, `refreshAfterTurn`.
  - `subscribeMerges` and `subscribeStateChanges`.
- Cache TTLs: list 30 s, detail 60 s (merged 10 min), checks 15 s, diff 60 s (commit 10 min, patches cached only up to 512 KB), viewer 10 min.
- A persisted read cache keeps data across restarts; entry files expire after a day.
- Refusals: requesting reviewers needs write access; changing labels needs triage access.
- Paths: `apps/server/src/pullRequest/PullRequestService.ts`, `PullRequestReadCache.ts`, `PullRequestProviderRegistry.ts`, `pullRequestSyncKey.ts`, `linkedThreads.ts`, `http.ts`, `packages/contracts/src/pullRequest.ts`, `packages/source-control-core/src/server/PullRequestProvider.ts`.

**E2. Cross-environment GitHub routing ("GitHub sharing")** – L
- `pullRequests.routing` and `pullRequests.routingIdentity`, `withRoutingCredential`, `expectedAccountId` and `allowStale` on `PullRequestRef`.
- Another environment signed in to the same GitHub account can answer reads and permitted actions. Credentials verified earlier stay usable for 10 minutes during an outage.
- Paths: `PullRequestService.ts` (`routing`), `docs/user/source-control.md`.

**E3. Thread pull request linking** – M
- Commands: `thread.pull-request.link`, `thread.pull-request.unlink`, `thread.pull-request-link.sync`, `thread.pull-request.sync`. Event `thread.pull-request-link-updated`.
- Link sources: `manual | created | agent | stack | stack-dismissed`. The last is a tombstone for an unlinked stack layer.
- A link stores a snapshot (state, title, branches, draft flag, timestamps, author, sizes, review decision, checks, mergeability) and a native stack (bottom to top).
- MCP tools `link_pull_request`, `unlink_pull_request`, `list_thread_pull_requests`.
- Paths: `apps/server/src/orchestration-v2/ThreadPullRequestService.ts`, `packages/contracts/src/threadPullRequest.ts`, `apps/server/src/mcp/toolkits/pullRequests/*`.

**E4. Pull request sync reactor** – L
- Sweeps linked PRs. Requests are batched so bursts share GitHub summary batches.
- Rate-limit pauses apply per project and host. Closed PRs re-read slowly (15 min); a merged snapshot missing its merge time is re-read.
- Re-reads after a run that ran `gh pr merge|close` or `glab mr merge|close`.
- Discovers branch PRs after a turn, with 5 backfill attempts and guards against a changed project or workspace.
- Paths: `apps/server/src/orchestration-v2/PullRequestSyncReactor.ts`, `ThreadPullRequestService.ts`.

**E5. Pull request watches ("babysit")** – L
- Commands `thread.pull-request.watch` and internal `thread.pull-request-watch.sync`.
- Sweeps every 2 minutes and wakes the agent when:
  - a check fails, or the required checks pass;
  - someone else comments or reviews;
  - the branch starts to conflict, or the head is pushed.
- Uses the host's watch fingerprint where available, with a 30 min forced re-read; without one, a 10 min quiet re-read.
- Stops after 10 comment-only wakes in a row, 8 failed reads in a row, a merge or close, or Stop, settle or archive.
- `endedWatch` prevents the same news being repeated. Comments from the user's own account do not wake it.
- MCP tools `watch_pull_request`, `unwatch_pull_request`.
- Paths: `PullRequestWatchReactor.ts`, `pullRequestWatch.ts` (`evaluatePullRequestWatch`, `pullRequestWatchMessage`, `PULL_REQUEST_WATCH_WAKE_LIMIT`).

**E6. Merge detection and auto-settle** – M
- `subscribeMerges` drives a thread settlement sweep. Auto-settle on merge happens when every linked PR is terminal.
- A merge event drops cached branch answers. The settle script then runs in the thread's own worktree.
- Path: `apps/server/src/orchestration-v2/ThreadSettlementService.ts`.

**E7. Review guide (AI pull request tour)** – L
- `pullRequests.reviewGuide` and `pullRequests.generateReviewGuide`.
- Generation:
  - Fetches the base and head into refs owned by the run and validates base names.
  - Creates a detached temporary worktree in `worktreesDir` and runs the text-generation guide model.
  - Normalises the output into stops: title, summary, attention note, excerpts, files, notes, questions.
- The guide is stored per head SHA. One run per PR at a time (atomic claim). Run states `Running` and `Failed`.
- MCP tools `write_pull_request_guides`, `read_pull_request_guide`.
- Paths: `apps/server/src/pullRequest/PullRequestReviewGuideService.ts`, `pullRequestReviewGuideInput.ts`, `pullRequestReviewGuideNormalize.ts`, `apps/server/src/persistence/PullRequestReviewGuides.ts`, `packages/contracts/src/pullRequestReviewGuide.ts`.

**E8. Viewed files and file revisions** – L
- GitHub stores viewed marks on the host. Other hosts store them in this environment and record each file's blob revision so a later push reports a cleared file as "Changed".
- The semantics are subtle:
  - Below the provider boundary, an absent path means the file is not in that revision.
  - Above it, an absent path means the host could not say.
  - The empty string means deleted. A null map loses staleness tracking. A null on a stored mark means it was stored without a baseline.
- Caps: 1,000 paths per scope, 64 scopes cached.
- Paths: `apps/server/src/pullRequest/pullRequestViewedFiles.ts`, `apps/server/src/persistence/PullRequestFilesViewed.ts`, `packages/source-control-core/src/server/diffFileRevisions.ts`, `docs/internals/pull-request-file-revisions.md`.

**E9. Pull request actions** – M (per host)
- Actions: `merge`, `ready`, `draft`, `close`, `reopen`, `delete-branch`, `update-branch`, `enable-auto-merge`, `disable-auto-merge`, `revert`, `approve-workflows`.
- Merge methods: merge, squash, rebase. Update methods: merge, rebase.
- GitHub stack merge and stack rebase (bottom to top, remote only).
- Capability flags tell clients what a host offers.
- Paths: `packages/contracts/src/pullRequest.ts`, `packages/source-control-github/src/server/githubStackActions.ts`, `githubStackRebase.ts`, `stackActions.graphql.ts`.

**E10. Pull request diff over HTTP** – S
- `POST /api/pull-requests/diff` for compression and flow control on large patches. Needs the orchestration read scope.
- Paths: `apps/server/src/pullRequest/http.ts`, `packages/contracts/src/environmentHttp.ts`.

**E11. Check deduplication and timestamp parsing** – S
- Path: `packages/source-control-core/src/server/pullRequestChecks.ts`.

**E12. Source control rate limiting** – M
- Pauses keyed by credential scope. An `Interactive` flag lets user actions spend a reserved quota. Honours `retry-after`; the fallback cooldown grows exponentially.
- Path: `packages/source-control-core/src/server/SourceControlRateLimit.ts`.

---

# F. Local diff review (working tree and branch)

**F1. Review diff preview and file contents** – L
- `review.getDiffPreview {cwd, baseRef?, ignoreWhitespace?, file?}` returns two sources: `working-tree` (against HEAD, including untracked files via a temporary review index, up to 5,000 untracked files) and `branch-range` (from the merge base with the base ref).
- Each source has `diffHash`, `truncated` and per-file stats. Patches are capped at 120 KB, or 1 MB for a single file.
- `review.getDiffFileContents` returns old and new contents for `change | rename-pure | rename-changed | new | deleted`.
- The cwd must be a known workspace.
- Paths: `apps/server/src/review/ReviewService.ts`, `GitVcsDriverCore.ts` (`getReviewDiffPreview`, `prepareReviewIndex`, `prepareUntrackedReviewIndex`, `resolveReviewMergeBase`, `readBranchChangeTotals`, `getReviewDiffFileContents`), `packages/contracts/src/review.ts`.

---

# G. Source control host integrations (`packages/source-control-*`)

**G0. Core contracts** – L
- `SourceControlProvider`: list/get/create change requests, clone URLs, `createRepository`, default branch, checkout, optional head probes, template reader, link resolution, repository identity refinement.
- `PullRequestProvider`: about 35 operations (viewer, list/listAcross/stats, checks, preview, summary, watch fingerprint, stack, activity, thread comments, permissions, diff, diff file contents, viewed files, file revisions, run action, update, comment, update comment, submit review, reviewer candidates and requests, label candidates and labels, reply, reaction, resolve).
- Error reasons: `missing-tool | unauthenticated | rate-limited | not-found | failed`.
- Discovery specs come in three kinds: CLI, API and managed CLI.
- `SourceControlHost` is the dependency bundle passed to providers.
- Paths: `packages/source-control-core/src/server/{SourceControlProvider,PullRequestProvider,SourceControlHost,discovery,driver}.ts`, `src/client/definition.ts`.

**G1. GitHub** – XL
- Transport: GraphQL and REST over `fetch`, plus the `gh` CLI for some flows (`gh pr checkout`, auth).
- Credentials: `GH_TOKEN`, `GH_ENTERPRISE_TOKEN` (only to the host named by `GH_HOST`), or a token from gh; a token can also be saved in settings. Covers GitHub Enterprise and GHE.com.
- Request policy: shared request slots, quota reserve with no local debit, a host-wide pause, rate-limit detection even on HTTP 200.
- About 40 GraphQL operations, for example:
  - Summaries, stats, stack membership and watch fingerprints, batched.
  - Search and listings; reads of core data, checks, preview, activity, heads, subjects and permissions.
  - Reviewer and label candidates, files viewed, merge message, action state, branch to delete.
  - Mutations: reply, react, resolve, update, revert, update comments, set file viewed, merge, auto-merge, update branch, ready/draft, close/reopen, comment, delete branch.
- Also: conditional checks via ETag/304, PR templates, stacks with merge and rebase, viewed files on the host, labels, reactions.
- Path: `packages/source-control-github/src/server/*`.

**G2. GitLab** – L
- Uses the `glab` CLI (`glab api` and `glab mr`) for all reads and writes.
- Viewed files are stored in the environment; file revisions are fetched in batches.
- Update method rebase only; reactions via award emoji.
- Path: `packages/source-control-gitlab/src/server/{GitLabCli,GitLabPullRequestCli,GitLabPullRequestProvider,GitLabSourceControlProvider,gitLabMergeRequestJson,gitLabAuthStatus}.ts`.

**G3. Bitbucket** – L
- REST API with credentials saved in settings: access token, or email plus API token.
- Refuses to follow URLs that come back in responses (pagination or redirects).
- Actions merge and close only; no reactions.
- Revisions come from `index` lines in the patch.
- Checkout means `git clone --single-branch` of the head repository.
- Path: `packages/source-control-bitbucket/src/server/{BitbucketApi,BitbucketPullRequestApi,...}.ts`.

**G4. Azure DevOps** – L
- Uses the `az` CLI (`az repos`).
- Merge methods merge and squash; no search, no reactions. Revisions come from the iteration listing.
- SSH and web repository spellings are reconciled (`azureDevOpsRepositoryKey` in `packages/shared/src/git.ts`).
- Path: `packages/source-control-azure-devops/src/server/*`.

**G5. Forgejo and Gitea** – L
- Uses the `fj` and `tea` CLIs plus git.
- Checkout fetches `refs/pull/<n>/head`. Actions merge, close, reopen, update-branch. Labels and reactions are supported.
- Requesting changes requires a review summary.
- Path: `packages/source-control-forgejo/src/server/*`.

**G6. GitCafe** – M/L
- REST API plus the `cafe` CLI. Credential `CAFE_TOKEN`, scoped to the `CAFE_HOST` host.
- Only production and staging hosts exist. Stacks and stack actions are supported.
- Path: `packages/source-control-gitcafe/src/server/*`.

**G7. Provider registry, discovery and host secrets** – M
- The registry resolves the provider for a cwd and remote and caches verdicts for unknown remotes. Discovery probes git and jj versions and each provider's auth.
- RPC `server.discoverSourceControl`.
- Host secrets are named `source-control-<b64(kind)>-<b64(field)>[-<b64(host)>]`.
- Paths: `apps/server/src/sourceControl/{SourceControlProviderRegistry,SourceControlDiscovery,ServerSourceControlHost,builtInDrivers,sourceControlHostSecrets}.ts`, `packages/contracts/src/sourceControl.ts`.

**G8. Repository lookup, clone and publish** – M
- `sourceControl.lookupRepository`, `sourceControl.cloneRepository` (protocol auto/ssh/https, destination normalisation).
- `sourceControl.publishRepository`:
  - Creates the hosted repository (private or public) and ensures the remote.
  - Pushes; returns `remote_added` when there are no commits yet.
- Path: `apps/server/src/sourceControl/SourceControlRepositoryService.ts`.

---

# H. Projects

**H1. Project model, add/update/delete, snapshots** – M
- RPCs `projects.mutate` (`project.create | project.update | project.delete {force?}`), `projects.list`, `projects.add`, `projects.remove`. HTTP `GET /api/projects` and `POST /api/projects/mutate`.
- A project has a title, workspace root, repository identity, favicon path, icon override (lucide, emoji or monogram), default model, default thread environment mode, auto pull and scripts.
- Paths: `apps/server/src/project/ProjectService.ts`, `ProjectMutation.ts`, `http.ts`, `packages/contracts/src/project.ts`.

**H2. Clone into a new project, with progress** – M
- `projectClone.start`, `.cancel`, `.retry`, and `subscribeProjectClones`.
- Stages `connecting | counting | receiving | resolving | checkout`, parsed from git's carriage-return progress output.
- The project exists before its files do; sending a turn waits for the clone. Finished clones are kept 30 s.
- Paths: `apps/server/src/project/ProjectCloneTracker.ts`, `gitCloneProgress.ts`, `packages/contracts/src/projectClone.ts`.

**H3. Scratch and named new projects** – M
- `projects.ensureScratch`.
- `projects.createNew {name}` creates `~/.t3/projects/<slug>` with git init, a README, `assets/icon.svg` and a first commit. It still succeeds without a first commit if git has no identity.
- Path: `apps/server/src/project/ManagedProjectFolders.ts`.

**H4. Checked-in project file `t3.json`** – S
- Fields: `$schema`, `iconPath`, `defaultThreadEnvMode`, `worktreeSubmodules`, `scripts[]` (at most 50).
- Settings backed by the file resolve in this order: project override, environment setting, `t3.json`, built-in default.
- Paths: `apps/server/src/project/T3ProjectFileLoader.ts`, `packages/contracts/src/t3ProjectFile.ts`.

**H5. Repository identity resolution** – S/M
- Canonical repository key from the remote URL. Local remotes such as `.` are ignored. Cached 15 min (negative results 1 min).
- Paths: `apps/server/src/project/RepositoryIdentityResolver.ts`, `packages/shared/src/git.ts` (`normalizeGitRemoteUrl`).

**H6. Favicon detection and project enrichment** – M
- Scans candidate files and parses `<link rel=icon>` and framework metadata with a linear-time scan.
- Caches: 10 min for a hit, 1 min for a miss. The enrichment queue runs 4 at a time.
- Paths: `apps/server/src/project/ProjectFaviconResolver.ts`, `ProjectEnrichmentService.ts`.

**H7. Import outside agent sessions** – L
- `agentSessions.scan` and `agentSessions.import` read Claude Code (`~/.claude`) and Codex (`sessions/YYYY/MM/DD`) transcripts from the last 30 days, with many byte, record and size caps.
- Paths: `apps/server/src/project/AgentSessionScanner.ts`, `AgentSessionImporter.ts`, `AgentSessionJson.ts`.

---

# I. Workspace files

**I1. Path rules and known workspaces** – M
- Workspace root normalisation and the related errors: missing, create failed, stat failed, not a directory, path outside root.
- A client-supplied cwd counts only if it is a managed worktree folder, a registered project or a git-listed worktree of the thread's project.
- Paths: `apps/server/src/workspace/WorkspacePaths.ts`, `KnownWorkspaces.ts`.

**I2. Read and write files** – M
- `projects.readFile`:
  - Up to 1 MB; detects binary files; flags truncation.
  - Accepts a workspace-relative path or an absolute host path.
  - Opens non-blocking so a FIFO cannot hang the request, and refuses symlink escapes.
- `projects.writeFile`:
  - Workspace-relative paths only.
  - Never writes under `.git`, including Windows aliases (`GIT~1`, trailing dots or spaces, `:` streams).
  - Never writes into a folder that git would treat as a repository (HEAD plus objects/refs, or a `commondir` file).
  - Uses no-follow opens.
- Paths: `apps/server/src/workspace/WorkspaceFileSystem.ts`, `fileHeaderMimeType.ts`.

**I3. Directory listing and browsing** – M
- `projects.listEntries` labels entries git ignores (`check-ignore` in chunks of 1,000).
- `filesystem.browse {partialPath, cwd?}` completes paths.
- `filesystem.getMetadata` takes up to 64 absolute or `~` paths and returns kind, byte length and MIME type.
- Paths: `apps/server/src/workspace/WorkspaceEntries.ts`, `packages/contracts/src/filesystem.ts`.

**I4. File and content search index** – L
- `projects.searchEntries`: fuzzy path search, limit 200, kind filter, images only.
- `projects.searchContents`: grep with case sensitivity, whole word and regex; limit 500; 250 ms time budget; 100 matches per file; falls back from a bad regex with `regexFallbackError`.
- Built on the native `@ff-labs/fff-node` library. Index of up to 25,000 entries, 15 s scan timeout, dropped after 15 min idle; separate index variants for paths and content.
- A Rust port would use the `ignore`, `grep` and nucleo crates.
- Path: `apps/server/src/workspace/WorkspaceSearchIndex.ts`.

**I5. Reading workflow transcript files** – S
- Reads `~/.claude/projects/*` contained files, resolving the file's real path, checking the inode and reading only complete lines from an offset.
- Path: `apps/server/src/orchestration-v2/workflowFileRead.ts`.

---

# J. Attachments, images, assets and archives

**J1. Attachment store and id scheme** – M
- Id format: `<thread-segment>-<uuid>[-ext]`. Pending uploads use `pending-<uuid>`.
- Deterministic ids are derived from sha256 of `[threadId, stableKey]`, where the stable key is the message id, the index and the bytes.
- `.part` files are in-flight uploads. Stale pending files are swept after 24 h, partial uploads after 1 h.
- Paths: `apps/server/src/attachmentStore.ts`, `attachmentPaths.ts`.

**J2. Signed upload URLs** – M
- `attachments.createUploadUrl` covers images (supported MIME types, at most 10 MB) and files (at most 50 MB). The URL is HMAC-signed and lives 10 minutes.
- `attachments.delete` removes a pending upload. The upload route is `/api/attachments/upload/<payload>.<sig>`. Pending files are swept every 15 minutes.
- Path: `apps/server/src/assets/AttachmentUpload.ts`.

**J3. Persisting data-URL attachments** – S
- `assets.persistChatAttachments` parses base64 data URLs without running a regex over the payload.
- It checks the MIME type and size, then writes with a deterministic id.
- Paths: `apps/server/src/ws.ts` (`persistChatAttachments`), `apps/server/src/imageMime.ts`.

**J4. Claiming attachments at dispatch** – S
- Validates limits: 100 attachments, 80 MB of images in total.
- Copies pending files to their final location (copy, not move, so a retry still has its source), uninterruptibly, and releases claims on failure.
- Path: `apps/server/src/orchestration-v2/AttachmentClaims.ts`.

**J5. Signed asset URLs (resolver)** – L
- `assets.createUrl {resource}`. Resource kinds:
  - `workspace-file`, `media-file`, `draft-workspace-file`
  - `attachment` (inline or attachment disposition, `mcp-app` render intent)
  - `tool-output-image`, `project-favicon`, `native-app-icon`, `github-media`
- Tokens are HMAC-signed and live 1 hour; favicon and HTML tokens are bucketed into 30 min windows for caching. Route `GET /api/assets/<token>/<filename>`.
- Files are opened without following symlinks and without blocking. Range requests are supported. SVG is served with a sandbox CSP.
- `.icns` icons are converted to PNG. MCP-app HTML is read for rendering. PDFs open inline.
- Paths: `apps/server/src/assets/AssetAccess.ts`, `MediaFile.ts`, `icns.ts`, `apps/server/src/http.ts` (`layerAssetRoute`, `layerAttachmentUploadRoute`), `packages/contracts/src/assets.ts`.

**J6. GitHub media proxy** – S
- Fetches GitHub-hosted PR images with the `gh` credential. The token is not forwarded on redirects; up to 3 redirects; Range headers are forwarded.
- Path: `apps/server/src/assets/GitHubMediaFetch.ts`.

**J7. Native app icon resolver (macOS)** – S
- Uses `mdfind`, `mdls`, `plutil` and `sips` to produce a 64 px PNG; cached 1 hour.
- Path: `apps/server/src/assets/NativeAppIconResolver.ts`.

**J8. Stripping tool-output image bytes** – S
- Removes base64 image blocks from tool output that will never be served, so they are not stored twice.
- Path: `apps/server/src/orchestration-v2/toolOutputImageBytes.ts`.

**J9. Zip archive reader** – S
- Used to unpack the preview browser download and in provider installation.
- Paths: `apps/server/src/zipArchive.ts`, used by `apps/server/src/preview/PreviewBrowser.ts`.

**J10. Thread deletion cleanup** – S
- Deleting a thread queues terminal, preview and attachment cleanup, including the thread's HTML render attachments.
- Paths: `apps/server/src/orchestration-v2/ThreadDeletion.ts`, `attachmentStore.ts` (`threadHtmlRenderAttachmentIds`).

**J11. Composer context records** – S on the server side (pass-through)
- Record kinds: image, file, terminal, element, preview-annotation, review-comment, mention, skill, thread, unknown.
- References look like `[label](t3-context://v1/<kind>/<id>)`. The provider sees them as a `<t3_context>` envelope.
- Paths: `packages/contracts/src/composerContext.ts`, `docs/internals/composer-context-references.md`.

---

# K. Previews and browser automation

**K1. Preview session manager** – M
- RPCs: `preview.open {threadId, url?, viewport?, profileId?, runtime?, reveal?}`, `navigate`, `resize`, `adjust` (color scheme, zoom, hard reload, clear cookies or cache), `refresh`, `close`, `list`, `reportStatus`, `subscribePreviewEvents`.
- Event types: opened, navigated, resized, failed, closed.
- Runtime is `desktop` or `server`.
- Paths: `apps/server/src/preview/Manager.ts`, `packages/contracts/src/preview.ts`.

**K2. Discovering local dev servers (port scanner)** – L
- `subscribeDiscoveredLocalServers {configuredUrls}` polls every 3 s.
- Listening ports come from `lsof`, `/proc/net/tcp` (fd walk up to 50,000) or `netstat -ano` on Windows.
- Each port gets an HTTP probe (1 s timeout, 15 s cache, 16 at a time) and is attributed to the terminal process that owns it.
- Path: `apps/server/src/preview/PortScanner.ts`.

**K3. Server-side headless browser** – XL
- Uses Playwright with `chrome-headless-shell` version 154.0.8037.92. The download is pinned per platform by SHA-256 and unpacked from a zip.
- On Linux: AppArmor profile, missing-library detection and setup, and a refusal to run as root.
- Browser profiles and contexts; the user agent is kept consistent with `Sec-CH-UA`.
- JPEG screencast paced at about 30 fps (2× device scale); an agent cursor overlay.
- WebSocket stream at `/api/preview-stream` (close code 4404 when the tab is gone, 4503 when host setup is needed), with input forwarding, a download endpoint and uploads (20 files of up to 100 MB).
- Session control: claims by agent vs human, and takeover.
- Clearing profiles: `preview.clearProfile`, `preview.reportProfiles`.
- Paths: `apps/server/src/preview/{ServerBrowser,ServerBrowserPage,ServerBrowserStream,ServerBrowserContexts,PreviewBrowser,PreviewBrowserHost,SessionControl,PdfDocumentNavigation}.ts`.

**K4. Desktop browser channel** – M
- Bridges Playwright's CDP WebSocket to the Electron tab debugger over bootstrap file descriptors.
- Path: `apps/server/src/preview/DesktopBrowserChannel.ts`.

**K5. Preview automation broker and MCP tools** – L
- Server operations: status, open, navigate, snapshot (accessibility refs, masked password values), click, type, press, scroll, evaluate (64 KB cap), waitFor, recordingStart, recordingStop, resize, setColorScheme, dialog, close, upload, hover, select, drag.
- MCP tools: `preview_status`, `preview_open`, `preview_navigate`, `preview_snapshot`, `preview_click`, `preview_type`, `preview_press`, `preview_scroll`, `preview_evaluate`, `preview_wait_for`, `preview_recording_start`, `preview_recording_stop`, `preview_resize`, `preview_set_appearance`, `preview_dialog`, `preview_upload`, `preview_hover`, `preview_select`, `preview_drag`.
- Also `t3_preview_list` and `t3_preview_close`.
- Paths: `apps/server/src/mcp/PreviewAutomationBroker.ts`, `apps/server/src/mcp/toolkits/preview*/*`, `packages/contracts/src/previewAutomation.ts`.

**K6. Browser cookie import** – desktop only (the Electron app), not the server; out of scope for a server rewrite.
- Paths: `apps/desktop/src/preview/BrowserImport/*`, `packages/contracts/src/browserImport.ts`, `docs/user/browser-import.md`.

---

# L. HTML renders and MCP apps

**L1. HTML render ("visual replies")** – L
- MCP tool `html_preview` takes a width of 240–1600 and a theme. It returns a PNG, the content height and console output (20 messages of up to 500 characters each).
- MCP tool `html_render` publishes a page as a thread attachment with a title and a frame height.
- Local images (up to 10 MB each, 25 MB per page) are inlined as data URIs only if the bytes really are an image.
- Page loading and capture:
  - Pages load at `http://t3-page.localhost`, with at most 2 browsers and 3 measurements at a time.
  - Measurement times out after 6 s and capture after 20 s, with a maximum capture height of 4,000.
- All browser traffic goes through a SOCKS5 proxy that only reaches public addresses (blocks local, NAT64 and 6to4 ranges, so DNS rebinding cannot reach this machine).
- Paths: `apps/server/src/htmlRender/{HtmlRender,headlessChrome,publicProxy}.ts`, `apps/server/src/mcp/toolkits/html/*`, `packages/shared/src/htmlRender.ts`, `docs/user/html-renders.md`.

**L2. MCP apps (interactive tool results)** – M
- `mcpApps.callTool`, `mcpApps.toolInfo` (callable, read-only), `mcpApps.readResource`, `mcpApps.updateModelContext` (the latest note per app is sent with the next message).
- The target is `{threadId, itemId}`, so an app can only reach the MCP server it came from.
- Error reasons: `not-an-app`, `provider-unsupported`, `session-stopped`, `tool-not-callable`, `unsupported-content`, `request-failed`.
- Paths: `apps/server/src/mcpApps/{McpAppRequests,McpAppSnapshot,McpAppModelContext}.ts`, `packages/contracts/src/mcpApps.ts`, `AssetAccess.ts` (`readMcpAppHtml`).

---

# M. Editor integration

**M1. Open in editor or file manager** – M
- `shell.openInEditor {cwd, editor, reveal?}`.
- About 23 editors: Cursor, Trae, Kiro, VS Code, VS Code Insiders, VSCodium, Zed, Antigravity, and the JetBrains IDEs (IDEA, Aqua, CLion, DataGrip, DataSpell, GoLand, PhpStorm, PyCharm, Rider, RubyMine, RustRover, WebStorm), plus the file manager.
- Launch styles: `direct-path`, `goto`, `line-column`.
- Remote open URLs: `vscode-remote` over SSH, Zed SSH, JetBrains Gateway.
- Platform handling: Windows `.cmd` escaping, WSL PowerShell bridge, an `xdg-mime` directory-handler probe on Linux.
- Paths: `apps/server/src/process/externalLauncher.ts`, `packages/contracts/src/editor.ts`.

---

# N. RPC methods, commands and routes (exact strings, from `packages/contracts/src/rpc.ts` and `orchestrationV2.ts`)

**Projects and files**
- `projects.list`, `projects.add`, `projects.remove`, `projects.listEntries`, `projects.readFile`, `projects.searchContents`, `projects.searchEntries`, `projects.writeFile`, `projects.mutate`, `projects.ensureScratch`, `projects.createNew`
- `filesystem.browse`, `filesystem.getMetadata`
- `agentSessions.scan`, `agentSessions.import`
- `projectClone.start`, `projectClone.cancel`, `projectClone.retry`, `subscribeProjectClones`

**Editor**
- `shell.openInEditor`

**Assets and attachments**
- `assets.createUrl`, `assets.persistChatAttachments`, `attachments.createUploadUrl`, `attachments.delete`

**MCP apps**
- `mcpApps.callTool`, `mcpApps.toolInfo`, `mcpApps.readResource`, `mcpApps.updateModelContext`

**Version control and Git**
- `vcs.pull`, `vcs.refreshStatus`, `vcs.listRefs`, `vcs.createWorktree`, `vcs.removeWorktree`, `vcs.createRef`, `vcs.switchRef`, `vcs.init`, `subscribeVcsStatus`
- `git.runStackedAction` (streaming), `git.resolvePullRequest`, `git.preparePullRequestThread`
- `subscribeWorktreeSetup`, `worktreeSetup.cancel`

**Review**
- `review.getDiffPreview`, `review.getDiffFileContents`

**Pull requests**
- `pullRequests.list`, `pullRequests.listStats`, `pullRequests.summary`, `pullRequests.routing`, `pullRequests.routingIdentity`, `pullRequests.stack`, `pullRequests.linkedThreads`, `pullRequests.detail`, `pullRequests.preview`, `pullRequests.checks`, `pullRequests.activity`
- `pullRequests.reviewGuide`, `pullRequests.generateReviewGuide`, `pullRequests.threadComments`, `pullRequests.diffFileContents`, `pullRequests.filesViewed`, `pullRequests.setFilesViewed`
- `pullRequests.runAction`, `pullRequests.update`, `pullRequests.comment`, `pullRequests.updateComment`, `pullRequests.submitReview`, `pullRequests.replyToThread`, `pullRequests.setThreadResolution`, `pullRequests.setReaction`
- `pullRequests.invalidate`, `pullRequests.reportState`, `pullRequests.subscribeRefreshes`, `pullRequests.reviewerCandidates`, `pullRequests.requestReviewers`, `pullRequests.labelCandidates`, `pullRequests.setLabels`

**Source control**
- `sourceControl.lookupRepository`, `sourceControl.cloneRepository`, `sourceControl.publishRepository`, `server.discoverSourceControl`

**Preview**
- `preview.open`, `preview.navigate`, `preview.resize`, `preview.adjust`, `preview.refresh`, `preview.close`, `preview.list`, `preview.clearProfile`, `preview.reportProfiles`, `preview.reportStatus`
- `subscribePreviewEvents`, `subscribeDiscoveredLocalServers`

**Orchestration RPCs**
- `orchestration.getTurnDiff`, `orchestration.getFullThreadDiff`, `orchestration.launchThread`, `orchestration.dispatchCommand`

**Orchestration commands, dispatched through `orchestration.dispatchCommand`**
- `checkpoint.rollback`, `thread.fork`, `thread.merge_back`
- `thread.pull-request.link`, `thread.pull-request.unlink`, `thread.pull-request-link.sync`, `thread.pull-request.watch`, `thread.pull-request.sync`
- Internal: `thread.pull-request-watch.sync`, `checkpoint.rollback.fail`

**Orchestration events**
- `checkpoint-scope.created`, `checkpoint.captured`, `checkpoint.rollback-requested`, `thread.pull-request-link-updated`

**HTTP routes**
- `GET /api/assets/<token>/<file>`
- `PUT|POST /api/attachments/upload/<payload>.<sig>`
- `POST /api/pull-requests/diff`
- `GET /api/projects`, `POST /api/projects/mutate`
- WebSocket `/api/preview-stream` (also `/api/preview-stream/download`)

**MCP tools in this slice**
- `t3_worktree_handoff`, `t3_worktree_status`, `t3_worktree_list`
- `link_pull_request`, `unlink_pull_request`, `list_thread_pull_requests`, `watch_pull_request`, `unwatch_pull_request`, `write_pull_request_guides`, `read_pull_request_guide`
- `html_preview`, `html_render`
- The `preview_*` tools listed in K5, plus `t3_preview_list` and `t3_preview_close`
- `t3_project_clone`, `t3_project_create`, `t3_project_delete`, `t3_project_list`, `t3_project_read`, `t3_project_update`
- `t3_attachment_prepare_upload`, `t3_attachment_discard`

---

# O. Cross-cutting rules a rewrite must keep

- **Git subprocess hygiene**
  - Never put credentials in argv.
  - Classify stderr into fixed reason codes.
  - Pass config through `GIT_CONFIG_*` environment entries.
  - Disable fsmonitor and untrackedCache for workspace scans.
  - Check pathspecs with `:(top,literal)`.
  - Batch paths to stay under the Windows command-line limit.
- **Path security**
  - Validate with real paths.
  - Use no-follow and non-blocking opens.
  - Never write into `.git` or its Windows aliases.
  - Only recreate managed worktrees.
  - Gate cwds through KnownWorkspaces.
- **Many caches with exponential failure backoff.** The TTLs are listed in the features above; match them to avoid hammering git or host APIs.
- **Concurrency**: per-cwd checkpoint lock, workspace leases, the thread-command lock during rollback, one review-guide run per PR, per-cwd remote status write lock.

Size guide: S is about a day or less, M a few days, L one to two weeks, XL several weeks.