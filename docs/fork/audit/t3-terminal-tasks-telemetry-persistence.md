# T3 Code server: feature catalogue for terminal, scheduled tasks, notifications, telemetry, persistence and misc (for a Rust reimplementation)

Paths are relative to the repo root (`t3code/`). The audit was read-only. Complexity to reimplement in Rust: **S** = under a day, **M** = a few days, **L** = 1–2 weeks, **XL** = several weeks or high risk.

Four things a reimplementer will hit first:
- **Two migrations nearly orphaned their tables.** `orchestration_v2_events` and `orchestration_v2_command_receipts` are still created by migration 055. A later step (`ApplicationEventSource`) copies their rows into the V1-named `orchestration_events` and `orchestration_command_receipts` tables, using an `application_event_version` column of 1 or 2. Production code no longer reads `orchestration_v2_command_receipts`. The only remaining reference to `orchestration_v2_events` is the legacy V1 thread importer.
- **One table is never used.** `orchestration_v2_thread_launch_workflows` is created but has no reader or writer.
- **There is no server-side voice transcription.** It runs on-device on iOS only.
- **The server does not send push notifications itself.** It publishes signed "agent activity" state to the T3 Connect relay (`infra/relay`), and the relay sends APNs and FCM pushes.

---

## 0. On-disk layout the server owns

Defined in `apps/server/src/config.ts` (`deriveServerPaths`). `stateDir` is `<baseDir>/userdata`, or `<baseDir>/dev` for an implicit dev run.

| Path | Purpose |
|---|---|
| `stateDir/statev2.sqlite` | Main database (WAL mode, plus `-wal` and `-shm` files) |
| `stateDir/state.sqlite` | Legacy V1 database from before 0.1.0. Copied once into `statev2.sqlite` when that file is missing, then kept for rollback. |
| `stateDir/server-owner.sqlite` | Single-instance lock file: an exclusive SQLite transaction that holds no data (`apps/server/src/serverOwnership.ts`) |
| `stateDir/keybindings.json` | User keybindings |
| `stateDir/settings.json` | Server settings |
| `stateDir/themes/` | Environment themes |
| `stateDir/attachments/` | Chat attachments |
| `stateDir/browser-artifacts/` | Browser artifacts (cleaned by storage cleanup) |
| `stateDir/logs/server.trace.ndjson` (+ rotated `.1`…`.N`) | Local span/trace file |
| `stateDir/logs/event-loop-stalls.ndjson` (+ `.1`) | Event-loop stall log |
| `stateDir/logs/provider/events.log` | Provider event NDJSON log (rotating) |
| `stateDir/logs/terminals/*.log` | Terminal scrollback persistence |
| `stateDir/logs/server-<pid>-<ts>.heapsnapshot` | Heap snapshot written on SIGUSR2 |
| `stateDir/anonymous-id` | Anonymous analytics identity |
| `stateDir/environment-id` | Environment id |
| `stateDir/server-runtime.json` | Runtime state, including pid |
| `stateDir/secrets/` | Secret store |
| `stateDir/usage-model-rates.json`, `usage-scan-cache-v5.json` (legacy `usage-scan-cache.json`) | Usage cost caches |
| `baseDir/worktrees/` | Worktrees directory |
| `baseDir/caches/` | Provider status and managed-binary cache |
| `baseDir/tools/` | Managed tool binaries |

---

## 1. Terminal / PTY subsystem

Key source:
- `apps/server/src/terminal/Manager.ts` (3,190 lines)
- `apps/server/src/terminal/NodePtyAdapter.ts`
- `apps/server/src/terminal/OutputProtocol.ts`
- Contracts in `packages/contracts/src/terminal.ts`
- Docs: `docs/internals/terminal-runtime.md`, `docs/user/terminal.md`

**Overall complexity: XL.** Most of the risk is in ordering, locking, history bounds and the escape-sequence sanitiser.

### 1.1 PTY spawn with shell fallback — L
- **What it does:** spawns the user's shell in a PTY via node-pty. Every PTY is wrapped by `AgentScope.wrap({name:"terminal", survivesRestart:true})`.
- **Shell candidate order on POSIX:** the requested shell, then `$SHELL`, `/bin/zsh`, `/bin/bash`, `/bin/sh`, `zsh`, `bash`, `sh`.
- **Shell candidate order on Windows:** the requested shell, then `pwsh.exe`, `%SystemRoot%\…\powershell.exe`, `powershell.exe`, `%ComSpec%`, `cmd.exe` (system path), `cmd.exe`.
- **When it retries:** only if the error text contains "posix_spawnp failed", "enoent", "not found" or "no such file". The final error lists every shell it tried.
- **Defaults and limits:** 120×30 when the client gives no size. Contracts allow cols 1–1000 and rows 1–500.
- **Adapter details (`NodePtyAdapter.ts`):**
  - `name: "xterm-256color"`; on Windows it forces `TERM=xterm-256color` if unset.
  - It makes `spawn-helper` executable.
  - On Windows (ConPTY) it waits for the deferred pid and can cancel startup through the private `_agent.kill()`.

### 1.2 Terminal environment construction — M
- **Inherited:** the full server environment.
- **Removed (blocklist):** `PORT`, `ELECTRON_RENDERER_PORT`, `ELECTRON_RUN_AS_NODE`, `TMUX`, `TMUX_PANE`, `STY`, `WINDOW`, `ZELLIJ`, `ZELLIJ_SESSION_NAME`, `ZELLIJ_PANE_ID`, and any key starting `T3CODE_` or `VITE_`.
- **Runtime env from the client:** up to 128 keys, values up to 8 KiB.
  - Merged case-insensitively on Windows.
  - `CODEX_HOME` and `CLAUDE_CONFIG_DIR` get `~` expanded.
- **Defaults added:**
  - `COLORTERM=truecolor` when unset.
  - `fish_features` gets `no-query-term` appended unless it already mentions `query-term`.
- **AppImage clean-up:** removes `APPIMAGE`, `APPDIR`, `ARGV0` and `OWD`, and filters `$APPDIR` entries out of `PATH`, `LD_LIBRARY_PATH`, `XDG_DATA_DIRS` and `GSETTINGS_SCHEMA_DIR`.
- **Managed binaries:** the managed ACP-agent binary directories are **appended** (never prepended) to `PATH`.
- **Provider-instance environment:** when `providerInstanceId` is given, the environment is resolved from the server's settings for that instance:
  - The instance's environment is merged in.
  - Codex instances get `CODEX_HOME` from the Codex home layout.
  - Claude instances get the Claude environment.
  - Code: `resolveProviderInstanceTerminalEnvironment`.

### 1.3 Session model, keys and shared sessions — L
- **Identity:** a session is keyed by `threadId\0terminalId`. The default terminal id is `"term-1"`; ids are at most 128 characters.
- **Shared sessions:** one PTY is shared by every client that attaches. The state lives on the server, so clients can reconnect at any time.
- **Session state:**
  - `status`: `starting`, `running`, `exited` or `error`
  - `pid`, `cwd`, `worktreePath`, `cols`/`rows`, `exitCode`/`exitSignal`, `updatedAt`
  - a monotonically increasing `eventSequence`, plus `inputCount`
  - `hasRunningSubprocess` and `childCommandLabel`
  - the runtime env, a pending escape-sequence buffer, and a queue of pending process events
- **Concurrency:** a per-thread `KeyedLock`, plus a **workspace lease** on `resolve(worktreePath ?? cwd)` so storage cleanup cannot delete a worktree while a terminal is opening.
- **`open` behaviour:**
  - If no session exists: flush any pending persist, restore history from disk, create the session, then start it.
  - If one exists and the cwd, worktree or env changed: stop the process, clear the history and restart.
  - If one exists but is exited or errored: clear the history and restart.
  - If one is running: just resize.
- **Memory cap:** at most 128 inactive (non-running) sessions are kept in memory; older ones are evicted by `updatedAt`.
- **Close markers:** close markers are kept in memory, so a `resume` attach to a closed terminal returns `closed` rather than recreating it.

### 1.4 Attach / observe streaming with snapshot-then-live ordering — M
- **Two streaming calls:**
  - `attachStream` (`terminal.attach`) can create or restart the session.
  - `observeStream` (`terminal.observe`) requires an existing session.
- **Ordering:** both subscribe to events **before** taking the snapshot, buffer events meanwhile, send `{type:"snapshot", snapshot}` (or `closed`), replay the buffered events (dropping a duplicate `started`), then go live.
- **Attach input fields:** `cwd?`, `worktreePath?`, `cols?`, `rows?`, `env?`, `providerInstanceId?`, `restartIfNotRunning?`, `resume?`.
- **Event types:** `started`, `output`, `exited`, `closed`, `error`, `cleared`, `restarted`, `activity`. The attach stream maps `started` to `snapshot`.
- **Snapshot fields:** threadId, terminalId, cwd, worktreePath, status, pid, history, exitCode, exitSignal, label (at most 128 characters), updatedAt, sequence.

### 1.5 Bounded scrollback history — M
- **Class:** `BoundedTerminalHistory`.
- **Limits:** 5,000 lines **and** 8 MiB of UTF-8 per terminal.
  - The oldest chunks are evicted first, without splitting code points.
  - Chunks are capped at 16 KiB.
  - Live output is never truncated.
- **Design rule:** history is appended chunk by chunk. Live events carry only the new chunk; the full history string is built only for snapshots and persistence (cached).
- **Client cap:** clients keep their own, separate 512 KiB cap.

### 1.6 Query/response stripping from retained history — M
- **Code:** `sanitizeTerminalHistoryChunk`, a streaming parser that carries an incomplete escape sequence over between chunks.
- **What it removes, so replaying scrollback never makes the terminal answer again:**
  - **CSI** (ESC `[` or 0x9B), when the final byte is:
    - `n`
    - `R` with a body of digits/`;`/`?`
    - `c` with a body of `>`/digits/`;`/`?`
    - `p` or `y` with a body ending in `$` (DECRQM queries and DECRPM replies)
    - `q` with a body starting `>` (XTVERSION)
    - `u` with a body starting `?` (Kitty keyboard query)
  - **DCS** (ESC `P` or 0x90) whose content matches `^[01]?[$+][qr]` (DECRQSS, XTGETTCAP and their replies).
  - **OSC** (ESC `]` or 0x9D) `10;`, `11;` or `12;` followed by `?` or `rgb:` (colour queries and replies).
- **What it keeps:** every other ESC, CSI, OSC, DCS, APC and PM sequence passes through unchanged. String terminators handled are BEL, ST (0x9C) and ESC `\`.

### 1.7 History persistence and restore — M
- **File names:** `<terminalLogsDir>/terminal_<base64url(threadId)>.log` for the default terminal, and `terminal_<b64(threadId)>_<b64(terminalId)>.log` for others.
- **Legacy file:** `<threadId with [^a-zA-Z0-9._-] replaced by _>.log`. It is migrated for the default terminal only, then deleted.
- **Writing:** a keyed, coalescing worker debounces writes by 250 ms. `clear`, `restart` and `close` write immediately and drain the queue (`persistHistory` + `flushPersist`).
- **Restoring:**
  - Read only the last 8 MiB of the file (seek to the tail).
  - Skip any partial UTF-8 bytes at the start.
  - Apply the line cap.
  - Rewrite the file if it was truncated or capped.
- **Delete-history options:** `close` with `deleteHistory` deletes the file, or every file for the thread when no terminalId is given.

### 1.8 Write / resize / clear / restart / close / closeIdle — M
- **`write`:** data up to 64 KiB. If the session has exited this is a silent no-op; any other non-running state is a `TerminalNotRunningError`.
- **`resize`:** a no-op when the session is not running.
- **`clear`:** clears history, persists, and emits `cleared`.
- **`restart`:** always resets history, then spawns again and emits `restarted`.
- **`close`:** with no `terminalId`, closes all of the thread's terminals.
- **`closeIdle({threadId, terminalId?})`:** closes running terminals with no child process and no new activity, using a fresh process-table check rather than the last poll. Callers: `orchestration-v2/ThreadSettlementService.ts` and `project/ProjectSetupScriptRunner.ts`.
- **Kill escalation:** SIGTERM, then SIGKILL after a 1 s grace if the process has not exited. It subscribes to exit before signalling, and the kill runs as a background fiber.

### 1.9 Subprocess activity detection and labelling — M
- **Poll interval:** 1 s while any session is running. On failure it backs off exponentially (`interval × 2^failures`, capped at 60 s; failure count capped at 30).
- **One process-table snapshot per tick:** taken from the native resource monitor's `processTable` command. Fallbacks:
  - POSIX: `/bin/ps -eo pid=,ppid=,comm=` (absolute path resolved at startup; 1 s timeout, 512 KB cap).
  - Windows: PowerShell `Get-CimInstance Win32_Process` (1.5 s timeout, 256 KB cap).
  - A non-zero exit, timeout or truncated output makes the tick fail rather than mark every terminal idle.
- **Deciding "busy":**
  - The shell counts as busy if it has a child whose command name differs from the shell's, or a same-named child that itself has children. This ignores the forks that async prompt themes create.
  - The label is that child's basename (`.exe` stripped on Windows), shown as the terminal label while it runs.
- **Output:** an `activity` event `{hasRunningSubprocess, label}`. The process ids are also registered with `PortScanner.PortDiscovery` (`registerTerminalProcesses` / `unregisterTerminal`) for preview port discovery.

### 1.10 Terminal metadata stream — S
- **Method:** `subscribeTerminalMetadata`.
- **Events:** `{type:"snapshot", terminals: TerminalSummary[]}`, then `upsert` / `remove`.
- **What triggers an update:** `started`, `restarted`, `exited`, `closed`, `error` and `activity`. `output` and `cleared` do not.
- **Sort order:** `updatedAt` descending, then thread, then terminal.
- **Summary fields:** id, cwd, worktreePath, status, pid, exit info, `hasRunningSubprocess`, label, updatedAt.
- **Other users:** storage cleanup (`storageCleanup.ts`) subscribes so it never removes a worktree that has a live terminal.

### 1.11 Raw event bus — S
- **Method:** `subscribeTerminalEvents` delivers every `TerminalEvent` for all terminals.

### 1.12 Output flow control window — M
- **Code:** `apps/server/src/terminal/OutputProtocol.ts`, which wraps the RPC protocol.
- **Applies to:** streams for `terminal.attach`, `terminal.observe` and `subscribeTerminalEvents`.
- **How it works:**
  - The server tracks unacknowledged chunks per `clientId:requestId`.
  - The window is full at 8 chunks or 64 KiB of serialised JSON.
  - While the window is not full, the server synthesises the Ack itself, so the server-side stream keeps flowing.
  - Once full, it waits for real client Acks.
  - The window is cleared on `Exit`.
- **Rust equivalent:** this is backpressure on your WebSocket RPC layer.

### 1.13 Lifecycle and metrics — S
- **Shutdown:** a finalizer kills every session (full SIGTERM-then-SIGKILL escalation) when the service shuts down.
- **Metrics:** `t3_terminal_sessions_total{lifecycle}` and `t3_terminal_restarts_total{scope}`.
- **Span attributes:** `terminal.thread_id`, `terminal.id`, `terminal.event_type`, `terminal.cwd`.
- **Errors:** cwd errors, history read/truncate/migrate errors, resize/write errors, session lookup, not running, provider instance not found, provider environment error.

---

## 2. Scheduled tasks (cron-like, interval and webhook triggers)

Key source:
- `apps/server/src/scheduledTasks/ScheduledTaskService.ts` (1,613 lines)
- `apps/server/src/scheduledTasks/Schedule.ts`, `webhookRoute.ts`, `webhookTemplate.ts`, `webhookVerification.ts`, `RelayDeliveryProof.ts`
- `apps/server/src/scheduling/Scheduler.ts`
- Contracts: `packages/contracts/src/scheduledTask.ts`
- Migrations: `OrchestrationV2/ScheduledTasks.ts`, `057`, `058`
- User docs: `docs/user/project-settings.md` ("Scheduled tasks on mobile", "Webhook automations")

**Overall complexity: L to XL.** Webhooks are the bulk of it.

### 2.1 Generic due-work scheduler — S
- **Code:** `Scheduler.register(name, runDueWork)`.
- **Behaviour:**
  - Ticks every **5 s** across all registered sources.
  - Each source has a single permit; a tick is skipped if the previous run is still going.
  - The first run is parked until startup recovery finishes (`forkParked`).
  - Failures are logged as "Scheduler source failed".

### 2.2 Schedule types and next-run calculation — M
- **`interval`:** `{everyMs}`. Writes require at least 60,000 ms; stored rows below that still load, but run at most once a minute. The next run is `from + max(everyMs, 60s)` in elapsed time, so it is not affected by daylight-saving changes.
- **`fixed_time`:** `{timeOfDay:"HH:MM", weekdays?: 0..6}` where 0 is Sunday. Times use the server's **local time zone**. The next run is the first matching wall-clock time in the next 0–7 days.
- **`webhook`:** `{signature: {header, encoding: hex|base64, prefix} | null, maxDeliveryAgeMinutes?: 1..1440 | null}`. There is no next run.
- **`isSameSchedule`:**
  - Compares parsed times (so `9:00` equals `09:00`) and normalised weekdays (an empty list or all 7 days both mean daily).
  - Editing a title or prompt keeps `next_run_at`; changing the schedule or the enabled flag recomputes it.
- **Missed fixed-time runs:** if a fixed-time run is more than **10 minutes** late, it is skipped and rescheduled rather than run.
- **`describeSchedule`:** produces human text such as "Every 5 minutes" or "At 09:00 every weekday".

### 2.3 Task CRUD, persistence and change stream — M
- **Table:** `scheduled_tasks` (see §10).
- **Task fields:**
  - id, title, prompt, enabled, schedule, projectId
  - optional bound `threadId`, workspaceStrategy, modelSelection, runtimeMode, interactionMode
  - createdBy, creationSource, createdAt, updatedAt
  - nextRunAt, lastRunAt, lastRunStatus (`never`, `running`, `succeeded`, `failed`), lastRunError, runCount
  - `webhook {path, url | null, hasSecret}`
- **Upsert:**
  - Idempotent by `commandId`; `requireExisting` rejects a save for a task that was deleted.
  - Run-state columns are not overwritten when an edit conflicts with a run.
  - Existence is checked inside the write itself, so an edit cannot undo a concurrent delete.
- **`setEnabled`:** returns not-found through `RETURNING`.
- **Change stream:** `subscribeList` emits a full list on every change. It subscribes before taking the snapshot, and uses a sliding buffer of 1 so a slow subscriber only gets the latest signal.
- **Decoding:** the list result is forward-compatible, so an unknown trigger type does not break decoding for the whole list.

### 2.4 Task execution: how tasks start threads — M
- **`runTask(task, "scheduled" | "manual" | "webhook")`:**
  1. Takes an in-memory per-task reservation.
  2. Re-reads the row and skips if the task was deleted, paused or postponed.
  3. Marks it `running`.
  4. Builds a deterministic `commandId` of `scheduled-task:<id>:<epochMs>:<trigger>`, or `…:webhook:<deliveryId>` for webhooks, and a matching `messageId`.
- **How it starts work:**
  - **No bound thread:** calls `ThreadLaunchService.launch({commandId, projectId, title, modelSelection, runtimeMode, interactionMode, workspaceStrategy, initialMessage:{messageId, scheduledTaskId, text, attachments:[]}, createdBy, creationSource})`. This creates a new thread.
  - **Bound thread:** calls `ThreadManagementService.sendToThread({…, mode:"queue"})`. The stored model selection is ignored; the run follows the thread's current provider.
- **Failure handling:**
  - `Effect.exit` captures defects and interruptions too, and records them as a failed run.
  - `markCompleted` is guarded by `last_run_at = startedAtIso`, so a task deleted and recreated mid-run is not stamped.
  - `releaseStuckRun` writes a failed record if anything breaks between "running" and "completed".
- **Startup recovery:** rows stuck in `running` become `failed` with "Run was interrupted by a server restart.", get their next run recomputed, and have `run_count` incremented.
- **Due query:** `enabled=1 AND next_run_at <= now AND last_run_status <> 'running'`. Due tasks run one at a time.
- **Run now:** `runNow` is manual and errors if the task is already running.

### 2.5 Run history — S
- **No per-run history table.** Run history is the aggregate columns on the task row (`last_run_*`, `run_count`).
- **Webhook deliveries** have their own log (§2.6). The thread created by a run carries `scheduledTaskId` on its initial message.

### 2.6 Webhook triggers — XL
- **Route:**
  - `POST`, `PUT`, `PATCH` and `GET` on `/api/hooks/:hookId/:token` (`packages/contracts/src/environmentHttp.ts`, `webhookRoute.ts`).
  - The body is read raw, capped at **1 MiB** (`413 body_too_large`, also enforced for chunked bodies).
  - Header names are lower-cased.
  - Every response carries the header `x-t3-hook-outcome`.
- **Status code mapping:**

| Status | Body / outcome |
|---|---|
| 202 | `{deliveryId}` with outcome `accepted`, `duplicate` or `prompt_too_long` |
| 404 | `hook_not_found` (an unknown hook and a wrong token look identical) |
| 401 | `invalid_signature` |
| 409 | `hook_disabled` |
| 429 | `rate_limited` (outcome `rate_limited` or `queue_full`) |
| 410 | `delivery_too_old` |
| 500 | `internal_error` |

- **Token:**
  - 32 random bytes, stored in `scheduled_tasks.webhook_token`.
  - Compared in constant time (SHA-256 both sides, then `timingSafeEqual`).
  - `rotateWebhookToken` replaces it, guarded by `created_at`.
  - The public URL is `<relayUrl>/v1/hooks/<endpointKey>/<taskId>/<token>`, where `endpointKey` is the last 16-hex segment of the managed tunnel name. Without a managed tunnel, only the path is available.
- **Signature (optional):** HMAC-SHA256 over the raw body using the stored `webhook_secret`. The header, encoding (hex or base64) and prefix (e.g. `sha256=`) are configurable, and the comparison is case-insensitive for hex.
  - The secret can come from `secretRef`, which is consumed by the save, via `secrets.answerRequest` / `request_secret`.
- **Relay delivery proof (`RelayDeliveryProof.ts`):**
  - Headers: `x-t3-relay-delivery-id`, `x-t3-relay-received-at`, and the `RELAY_HOOK_DELIVERY_HEADER` JWT.
  - The JWT is checked against the cloud mint public key, with issuer = relay issuer, audience = `t3-env:<environmentId>`, and a maximum age of 25 h.
  - The payload must match the environment, delivery id, received-at time and hook id exactly.
  - Without a valid proof, the request is treated as direct.
  - Only proven relay requests join the relay's trace (`traceparent`).
- **Processing order inside `triggerWebhook`, which runs uninterruptibly from the claim to the fork:**
  1. **Relay dedup:** `INSERT … ON CONFLICT DO NOTHING` into `scheduled_task_webhook_relay_deliveries`. Rows older than 48 h are pruned. A rate-limited or queue-full request gives up its claim.
  2. **Rate limit:** at most 60 deliveries per task per minute (sliding window). Only the first rejection in a window is logged.
  3. **Disabled** check.
  4. **Signature** check.
  5. **Max age** check, against `min(relayReceivedAt, now)`.
  6. **Render the prompt.** If it is longer than `PROVIDER_SEND_TURN_MAX_INPUT_CHARS`, it is logged as `dispatch_failed` with outcome `prompt_too_long`.
  7. **Per-task queue:** at most 20 queued deliveries (key is task id + createdAt).
  8. **Log `accepted`.**
  9. **Run in the background** under a per-task semaphore. A failure or skip later updates the delivery row with a fixed reason; the cause is never written to the row.
- **Delivery log:** `scheduled_task_webhook_deliveries`.
  - Keeps the newest 50 per task.
  - Body and rendered prompt are truncated at 64 KiB.
  - Credential-looking headers and query parameters are redacted.
  - Stored outcomes: `accepted`, `dispatch_failed`, `rejected_signature`, `disabled`, `rate_limited`, `expired`.
  - Read through `listWebhookDeliveries` (summaries) and `getWebhookDelivery` (full record).
- **Template rendering (`webhookTemplate.ts`):**
  - **Placeholders:**
    - `{{body}}` — the raw body.
    - `{{body.a.b}}` — a field from the JSON or form body. Any body that parses as JSON is addressable; form-urlencoded bodies are decoded.
    - `{{headers}}` and `{{query}}` — redacted; `{{headers.name}}` and `{{query.name}}` — raw value.
    - `{{request}}` — the whole request (method, path, redacted headers, body).
  - **Missing values:** a placeholder with no value renders empty and is recorded in `missing`.
  - **Framing:** each value is wrapped in `<webhook-data-<6 random hex bytes>>…</…>`, followed by a trailing note that the tagged text is untrusted.
  - **Mention defusing:** a word joiner (U+2060) is inserted after every currency sign, after `@`, after a `/` that starts a single-segment word, inside `t3-context:`, and inside `webhook-data`.
  - **Credential pattern:** `^(authorization|proxy-authorization|cookie|set-cookie|x-t3-relay-.*)$|token|secret|signature|key|password|auth|credential|hmac|jwt|assertion` (case-insensitive).
- **Held hooks waker (`apps/server/src/relay/HeldHooksWaker.ts`):**
  - When the managed tunnel reconnects (debounced 3 s), if "hold webhooks while offline" is on, it calls relay `POST /v1/environments/:environmentId/hooks/wake`.
  - 10 s timeout, exponential retry ×3.
  - The relay holds requests for up to 24 h.
- **Metrics:** `t3_webhook_deliveries_total{outcome, source: direct | relay}`, `t3_webhook_delivery_duration`, `t3_webhook_held_delay`, `t3_webhook_runs_total{outcome}`.

### 2.7 Agent / MCP access — S (thin wrappers)
- **Orchestrator toolkit tools:** `schedule_task`, `list_scheduled_tasks`, `update_scheduled_task`, `delete_scheduled_task` (`apps/server/src/mcp/toolkits/orchestrator/tools.ts`).
- **Thread toolkit tool:** `run_scheduled_task_now` (`apps/server/src/mcp/toolkits/thread/tools.ts`).
- **Gating:** `apps/server/src/mcp/McpHttpServer.ts:778`.

---

## 3. Notification mechanisms

### 3.1 Agent-activity publishing to the T3 Connect relay (mobile push and Live Activities) — L
- **Code:**
  - `apps/server/src/relay/AgentAwarenessRelay.ts` (825 lines)
  - `apps/server/src/relay/relayEnvironmentClient.ts`
  - Contracts: `packages/contracts/src/relay.ts`
  - Docs: `docs/user/mobile-notifications.md`, `docs/operations/android-notifications.md`
- **What the server does:** watches domain events and publishes each thread's awareness state to the relay. The relay (`infra/relay`, a Cloudflare worker that is a separate service) sends APNs, Live Activity and FCM pushes.
- **Trigger events:**
  - Thread events: create (but not v1 imports), archive/unarchive, delete, metadata, pull request link/sync, model selection, provider switch.
  - Run events: `run.created`, `run.updated`, `runtime-request.updated`, `subagent.updated`, `provider-thread.updated`.
  - `turn-item.updated` only when the item can end background work.
- **Publish worker:**
  - Coalesces per thread (one queued entry per thread at a time).
  - Skips the publish if the state identity is unchanged (identity = JSON of the state without `updatedAt`).
  - **5 s confirmation delay:** a publish is deferred for 5 s when (a) the state became `null` (a tombstone) while the previously published state was live, or (b) a thread's *first* published state is `completed`.
  - Retries after 1, 2, 4, 8 and 16 s.
  - On startup, catches up the active threads.
  - Exposes `drain` and `requestCatchUp`.
- **State payload (`RelayAgentActivityState`):**
  - environmentId, threadId, projectTitle, threadTitle, modelTitle, updatedAt, deepLink
  - `phase`: `starting`, `running`, `waiting_for_approval`, `waiting_for_input`, `completed`, `failed`, `stale`
  - `headline`, and `detail` (at most 160 characters; for failures the detail is replaced with "The agent run failed.")
  - Built by `projectThreadAwarenessV2`.
- **Transport:**
  - `POST /v1/environments/:environmentId/threads/:threadId/agent-activity` with a bearer environment credential.
  - Carries a signed publish-proof JWT: `typ RELAY_ACTIVITY_PUBLISH_TYP`, `iss t3-env:<envId>`, `aud` = relay issuer, `jti`, 5-minute expiry, signed with the environment's private key.
  - The response returns per-device delivery stats (APNs status and reason), which are logged.
- **Gating:** a "publish agent activity" secret/setting (`PUBLISH_AGENT_ACTIVITY_SECRET`) and relay link credentials (relay URL, issuer, environment credential). Startup state is `waiting-for-link`, `disabled` or `enabled`.

### 3.2 Desktop and web notifications — client-only, out of server scope
- `apps/web/src/components/ThreadNotificationCoordinator.tsx` and `apps/web/src/components/settings/NotificationSettings.tsx` use the browser Notification API, driven by the normal shell/thread subscriptions.
- The server does nothing specific for them.

### 3.3 Agent-to-agent "notification mailbox" — part of orchestration, M
- **Code:** `apps/server/src/orchestration-v2/NotificationMailbox.ts`, plus the `notification.delivery.accept` command in `packages/contracts/src/orchestrationV2.ts` (handled in `Orchestrator.ts`).
- **What it does:** buffers provider notifications and delegated-task completions and delivers them into a thread as steer messages.
- **Delivery guarantee:** at least once. The message id is reused so recovery does not duplicate timeline items.
- **Related:** `DelegatedTaskQuestionReactor.ts` and `PullRequestWatchReactor.ts` in the same folder also deliver notifications to threads.

---

## 4. Voice input / transcription — not on the server
- **Doc:** `docs/internals/voice-input.md` says transcription is local on iOS (Apple speech) and that "environment-backed transcription is not implemented".
- **Client code:** `packages/client-runtime/src/voice-input/*`, `apps/mobile/src/native/voiceTranscription.ios.ts`.
- **User doc:** `docs/user/composer.md` ("Voice input on iPhone"): up to 5 minutes, deleted after use.
- **Rust work needed:** none.

---

## 5. Product analytics (telemetry)

- **Code:** `apps/server/src/telemetry/{AnalyticsService,Identify,FeatureUsage,ProviderDimensions,DelegatedTaskAnalytics}.ts`
- **Docs:** `docs/internals/product-analytics.md`, `docs/user/telemetry.md`
- **Overall complexity: M.**

### 5.1 Delivery — M
- **Endpoint:** events are batched and sent with `POST <host>/batch/`. The default host is `https://ingest.t3.codes`, which keeps a copy and forwards to PostHog.
- **Event shape:** each event gets a uuid when recorded, and carries `capturedAt`.
- **Batching:** batch size from `T3CODE_TELEMETRY_FLUSH_BATCH_SIZE` (default 20). Flush runs every 10 s with a 10 s send timeout.
- **Retries:**
  - Up to 5 attempts per batch, with jittered exponential backoff from 2 s to 300 s.
  - A batch rejected with 400, 401 or 413 is dropped immediately (408 and 429 are retried).
- **Concurrency:** a flush lock is shared with the shutdown flush.
- **Opt-out:** `T3CODE_TELEMETRY_ENABLED=false`.
- **Release channel:** build-time value; anything other than official builds reports `dev`.

### 5.2 Identity — S
- **Code:** `Identify.ts`.
- **Order of preference:**
  1. Hashed Codex account id from `~/.codex/auth.json` (`tokens` exist only for ChatGPT logins).
  2. Hashed Claude user id from `~/.claude.json`.
  3. A random anonymous id persisted atomically at `stateDir/anonymous-id`, then hashed.

### 5.3 Events — S each
- **Event names emitted:**
  - `server.boot.heartbeat` (`serverRuntimeStartup.ts`)
  - `client.connected`, `client.thread.started`, `client.turn.requested` (`ws.ts`, per connection)
  - `feature.used` (`FeatureUsage.ts`, an allowlist of RPC methods and orchestration commands; variants come only from closed enums)
  - `provider.turn.completed` (`orchestration-v2/ProviderEventIngestor.ts`)
  - `mcp.tool.invoked` (`mcp/McpHttpServer.ts`)
  - `mcp.delegated_task.finished` (`telemetry/DelegatedTaskAnalytics.ts`)
- **Dimension rules:**
  - Client dimensions belong to the connection (`surface`, app version).
  - Provider dimensions report driver kinds, never instance ids, and only catalogue (non-custom) model names (`ProviderDimensions.ts`).
  - Never sent: prompts, ids, paths or free text.

---

## 6. Observability: logging, tracing, metrics, rotation

- **Code:** `apps/server/src/observability/*`, `apps/server/src/serverLogger.ts`, `packages/shared/src/observability.ts` (trace sink and local file tracer), `packages/shared/src/logging.ts` (`RotatingFileSink`), `apps/server/src/diagnostics/TraceDiagnostics.ts`
- **Doc:** `docs/operations/observability.md`

| # | Feature | Detail | Cx |
|---|---|---|---|
| 6.1 | Console logging | Pretty stdout logger. Fields that are `Cause` objects are rendered with `Cause.pretty`. The minimum log level comes from config. | S |
| 6.2 | Local NDJSON trace file with rotation | `server.trace.ndjson`, records `effect-span` / `otlp-span`. Fields: `type`, `name`, `traceId`, `spanId`, `parentSpanId`, `durationMs`, `attributes`, `events`, `exit` or OTLP status. Rotation by size: `T3CODE_TRACE_MAX_BYTES` (default 10,485,760) and `T3CODE_TRACE_MAX_FILES` (default 10). Batched flush every `T3CODE_TRACE_BATCH_WINDOW_MS` (default 1000). `T3CODE_TRACE_MIN_LEVEL` (default Info), `T3CODE_TRACE_TIMING_ENABLED`, `T3CODE_TRACE_FILE`. Attribute truncation and compaction. While no OTLP logs endpoint is set, logs emitted inside spans become span events. | M |
| 6.3 | OTLP export (traces, metrics, logs) | `T3CODE_OTLP_{TRACES,METRICS,LOGS}_URL`, `T3CODE_OTLP_HEADERS`, `T3CODE_OTLP_PROTOCOL` (`http/json` or `http/protobuf`), `T3CODE_OTLP_EXPORT_INTERVAL_MS` (default 10000). Standard `OTEL_EXPORTER_OTLP_*`, `OTEL_*_EXPORTER=none`, `OTEL_SDK_DISABLED` / `T3CODE_OTEL_SDK_DISABLED` kill switch. Endpoints can also be saved in settings. Precedence rules are in the doc. | M |
| 6.4 | OTLP endpoint check | `server.checkOtlpEndpoint`: posts an empty export (5 s timeout, no redirects). Sends headers only when the URL equals the endpoint currently running for that signal. Tracing is off for the check. | S |
| 6.5 | Metrics | `apps/server/src/observability/Metrics.ts`: `t3_rpc_requests_total`, `t3_rpc_request_duration`, `t3_orchestration_effect_claims_total`, `t3_orchestration_effect_queue_wait`, `t3_provider_sessions_total`, `t3_provider_turns_total`, `t3_provider_turn_duration`, `t3_git_commands_total`, `t3_git_command_duration`, `t3_terminal_sessions_total`, `t3_terminal_restarts_total`, `t3_webhook_deliveries_total`, `t3_webhook_delivery_duration`, `t3_webhook_held_delay`, `t3_webhook_runs_total`, `t3_secret_requests_total`, `t3_secret_refs_consumed_total`. Not persisted locally. | S |
| 6.6 | RPC instrumentation | `RpcInstrumentation.ts`: a span per WebSocket RPC (`ws.rpc.<method>`) plus metrics. | S |
| 6.7 | Event-loop stall monitor | `EventLoopMonitor.ts`: perf_hooks delay histogram at 1 s resolution, sampled every 30 s. Records a stall when `delayMaxMs` > 2 s and the loop was busy (`process.resourceUsage` CPU time and page faults). The first sample is skipped. Emits a root `server.eventLoop.stall` span at Warn and appends NDJSON to `event-loop-stalls.ndjson` (256 KiB cap, one backup). | M (Tokio runtime metrics differ) |
| 6.8 | Heap snapshot on SIGUSR2 | `HeapSnapshot.ts`, not on Windows. V8-specific; in Rust this becomes some other memory-profiling hook. | S/N.A. |
| 6.9 | Browser trace ingest | `POST /api/observability/v1/traces` (`apps/server/src/http.ts:58`). Decodes OTLP from web clients into the same trace sink (`BrowserTraceCollector.ts`). | S |
| 6.10 | Defect reporter | `DefectReporter.ts`: logs only "Die" defects from RPC handlers, not typed failures. | S |
| 6.11 | Trace diagnostics | `diagnostics/TraceDiagnostics.ts`, `server.getTraceDiagnostics`: streams the trace file and its rotated backups and aggregates slow spans (default threshold 1 s, top 10, recent 20) for Settings → Diagnostics. Also the `t3 trace summary` CLI (`apps/server/src/cli/trace.ts`). | M |
| 6.12 | Provider event NDJSON log | `apps/server/src/provider/EventNdjsonLogger.ts`: batched, rotating, with streams `native`, `canonical` and `orchestration`. Adjacent to scope. | M |
| 6.13 | SQLite span attributes | Spans carry `db.connection` (`writer` or `reader`) and `db.lock_wait_ms`; `Statement.SpanPropagationEnabled` keeps a statement's span current while it waits. | S |

---

## 7. Resource telemetry (native Rust monitor child process)

- **Code:**
  - `native/resource-monitor/src/main.rs` (1,334 lines; `sysinfo 0.39`, `serde`)
  - `apps/server/src/resourceTelemetry/{NativeTelemetryClient,DesktopTelemetryReceiver,ResourceTelemetry,Model,ResourceTelemetryHistory,HostResources,ResourceAttribution,ResourceMonitorBinary}.ts`
  - Contracts: `packages/contracts/src/resourceTelemetry.ts`
  - Doc: `docs/internals/resource-telemetry.md`
- **Overall complexity: L to XL.** The monitor itself is already Rust and can be reused or embedded directly.

### 7.1 Monitor protocol (NDJSON over stdin/stdout, `PROTOCOL_VERSION = 3`) — reuse or embed (S if embedded)

**Commands** (`{"type": …, "version": 3, …}`, camelCase):

| Command | Fields | Behaviour |
|---|---|---|
| `configure` | `rootPid`, `sampleIntervalMs`, `externalProcesses?: [{pid, startTimeMs?}]` | Interval is clamped to 250 ms–60 s. 0 means sample only on request. |
| `setExternalProcesses` | `processes` | Requires `configure` first. |
| `setSampleInterval` | `sampleIntervalMs` | |
| `setStreaming` | `enabled` | Timer samples are written to stdout only while streaming is enabled; they are always recorded in history. |
| `sampleNow` | `requestId` | |
| `processTable` | `requestId` | |
| `readHistory` | `requestId`, `windowMs` | |
| `shutdown` | | |

**Events:**
- `hello`: `{sidecarVersion, sidecarPid, platform, arch, capabilities{cumulativeCpuTime, currentCpuPercent, residentMemory, virtualMemory, ioBytes, processStartTime, processTree}}`.
- `snapshot`: `{sequence, sampledAtUnixMs, collectionDurationMicros, scannedProcessCount, retainedProcessCount, inaccessibleProcessCount, requestId?, externalProcesses, processes[]}`.
  - Each process: `{pid, ppid, startTimeMs, runTimeMs, name (≤1 KiB), command (≤16 KiB), status (≤256 B), cpuPercent, cpuTimeMs, residentBytes, virtualBytes, ioReadBytes, ioWriteBytes, ioSemantics: "storage" | "all-io"}`. `all-io` is used on Windows.
- `processTable`: `{requestId, processes: [{pid, ppid, name}]}`.
- `historyChunk`: `{requestId, done, snapshots}`, 32 snapshots per chunk.
- `error`: `{code: invalid-command | protocol-mismatch | not-configured, message, recoverable}`.

**Tracked processes:**
- The root pid's process tree, plus external pids whose start time matches (normalised to whole seconds).
- Children are followed only if they started at or after their parent, which guards against PID reuse.
- Linux per-thread task enumeration is disabled.

**History bounds:** 1 h, 3,600 snapshots, 20,000 entries and 64 MiB of estimated bytes. Input queue of 64.

### 7.2 NativeTelemetryClient (supervisor) — L
- **Binary resolution (`ResourceMonitorBinary.ts`):** `T3CODE_RESOURCE_MONITOR_PATH`, then the configured `resourceMonitorPath`, then the bundled per-platform binary. Targets: `{aarch64,x86_64}-{apple-darwin, unknown-linux-gnu, pc-windows-msvc}`. musl Linux is unsupported.
- **Spawn and handshake:** 5 s handshake timeout.
- **Request timeouts:** 5 s for samples and the process table, 15 s for history.
- **Restarts:** backoff from 500 ms to 10 s; gives up after 5 failures within 60 s.
- **Generations:** each restart starts a new generation, and snapshot sequence numbers are compared only within a generation.
- **Sample interval policy:**

| Condition | Interval |
|---|---|
| Live subscribers | 1 s |
| No subscribers | 0 (idle, on-demand only) |
| On battery | 5 s |
| Constrained (host power / background policy) | 15 s |

- **API:** `capabilities`, `snapshots`, `readHistory`, `setExternalProcesses`, `setHostPowerState`, `sampleNow`, `processTable` (also used by the terminal subsystem, §1.9), `retry`, `health`, `subscribeHealth`.

### 7.3 Desktop telemetry receiver (Electron host feed) — M
- **Code:** `DesktopTelemetryReceiver.ts`.
- **Transport:** inherited file descriptors from Electron main. NDJSON in; control messages written with full-write and stall detection on `desktopTelemetryControlFd`.
- **Inbound messages:** `desktopTelemetryHello`, `desktopTelemetry` (host power plus Electron process metrics), `desktopUpdateStatus`.
- **Outbound control messages:** `setDiagnosticsDemand`, `setHostPowerIntervals`, `requestDesktopUpdate`, `commitDesktopUpdate`, `cancelDesktopUpdate`.
- **Timing:**
  - Initial sample deadline 90 s.
  - Stale-after = max(90 s, slowest heartbeat + 30 s grace), checked every 30 s.
  - Host power intervals: 30 s active, 120 s idle.
- **Coupling:** this channel also carries the desktop auto-update handshake, so a Rust server that is embedded in the desktop app must implement it.

### 7.4 Merge model and aggregation — L
- **Code:** `Model.ts`.
- **Merge:** combines native and Electron process lists. Identity is `pid + startTime`, with ±2 s tolerance for Electron.
- **Categories:** `backend`, `electron` and `monitor` (and possibly finer ones).
- **Counters:** per-group cumulative CPU time, I/O read/write bytes, process starts and exits, accumulated from deltas. A delta is only counted when the sample interval is ≤30 s.
- **Rule:** historical replay must not merge in the latest Electron values.

### 7.5 History bucketing — M
- **Code:** `ResourceTelemetryHistory.ts`.
- **Input:** `server.getResourceTelemetryHistory {windowMs, bucketMs}`, maximum window 1 h.
- **Output:** buckets of cpuPercent, rssBytes, processCount and I/O, plus per-process summaries. Also returns "legacy backend buckets".

### 7.6 ResourceTelemetry facade — M
- **API:** `latest`, `changes`, `subscribe` (ref-counted live demand drives the sample interval), `readHistory`, `refresh`, `validateProcessIdentity` (re-check with a fresh sample before signalling a process), `retry`.

### 7.7 Host resources — S
- **Code:** `HostResources.ts`, `server.getHostResources`.
- **Data:** CPU from `os.cpus()` times; memory from `vm_stat` on Darwin (free + inactive + speculative pages), with other platforms handled separately.

### 7.8 Resource attribution — S
- **Code:** `ResourceAttribution.ts`.
- **What it is:** an in-memory counter keyed by `{component, operation}` holding logical read/write bytes, count and duration. For example, trace-file appends record their write bytes here.

### 7.9 Process diagnostics and signalling — M
- **Code:** `apps/server/src/diagnostics/{ProcessDiagnostics,ProcessResourceMonitor}.ts`.
- **Methods:** `server.getProcessDiagnostics`, `server.getProcessResourceHistory`, and `server.signalProcess`, which re-validates process identity before sending a signal.

---

## 8. Background policy and host power — M
- **Code:** `apps/server/src/background/{BackgroundPolicy,HostPowerMonitor}.ts`, contracts `packages/contracts/src/background.ts`.
- **Client activity leases:** clients report activity with `server.reportClientActivity`.
  - Report fields: `clientId`, `clientKind` (`web`, `desktop-renderer`, `mobile`, `unknown`), `visible`, `focused`, `recentlyInteracted`, `appState`, `lowPowerMode`, `batteryState`, `networkType`, `scopes[]`, `ttlMs`, `observedAt`.
  - Lease TTL: default 45 s, maximum 120 s.
  - At most 16 leases per RPC client; leases are removed when the RPC client disconnects.
- **Scopes:** `server-config`, `provider-status{instanceId?}`, `vcs-status{cwd}`, `git-refs{cwd}`, `diagnostics`, `thread{threadId}`.
- **Host power snapshot:** source, idle, idleSeconds, locked, suspended, onBattery, lowPowerMode, thermalState, stale. Reported by `server.reportHostPowerState` or the desktop receiver.
- **Snapshot:** includes `activeForegroundLeaseCount`, `activeScopeKeys` and `shouldRunOpportunisticWork`.
- **API:** `hasDemand(scope)`, `shouldRunScopeWork`, `shouldRunOpportunisticWork`.
- **Used by:** telemetry sampling, usage-limit refresh, provider and VCS status polling.

---

## 9. Usage and cost accounting — XL

- **Code:**
  - `apps/server/src/usage/{UsageService,usageTranscriptReader,usageScanCache,usagePricing,usageAggregation,UsageLimitSources,cliproxyApi}.ts`
  - Per-provider readers: `apps/server/src/provider/Drivers/{codexUsage,claudeUsage,AntigravityUsage,antigravityUsageReader}.ts`, `packages/provider-{grok,opencode,pi,core}/src/server/usage.ts`, plus Cursor via the dashboard API
  - Contracts: `packages/contracts/src/usage.ts` (`USAGE_CONTRACT_VERSION = 6`, merge-compatible since 4), `providerUsageLimits.ts`, `usageLimitSourceId.ts`
  - Doc: `docs/user/usage.md`

| # | Feature | Detail | Cx |
|---|---|---|---|
| 9.1 | Transcript scanning | Streams JSONL transcripts for Claude, Codex, Grok, OpenCode (SQLite plus old JSON), Antigravity (local databases), Pi, and Cursor (API). Files are pre-filtered by mtime with 36 h slack. Files over 8 MiB are streamed. Marker gating uses a latin1 alternation. Read concurrency 4, stat concurrency 32. Honours custom homes (`CODEX_HOME`, `CLAUDE_CONFIG_DIR`, `GROK_HOME`, `OPENCODE_DATA_DIR`, `ANTIGRAVITY_DATA_DIR`, `PI_CODING_AGENT_DIR`, `PI_CODING_AGENT_SESSION_DIR`). | XL |
| 9.2 | Incremental scan cache | `stateDir/usage-scan-cache-v5.json` (version 5, speed-compatible since 4). Stores a resume offset and a 64-byte guard hash per file. Interned JSON. Pruned to 92 days. A legacy file name is also read. | L |
| 9.3 | Pricing | Model rates from the LiteLLM `model_prices_and_context_window.json` (raw GitHub, with a GitHub API fallback), cached in `stateDir/usage-model-rates.json` with a 24 h TTL and 60 s refresh floor. User price overrides and "map to" aliases come from settings. Cache read/write default to the input rate. `<synthetic>` and bare family names are unpriced; `[1m]` suffixes are stripped. Fast/ultrafast premium. Cost sources: `providerReported`, `modelPriced`, `unpriced`. | M |
| 9.4 | Aggregation | Day and hour buckets (local day via en-CA). Global dedupe keyed across files, because Claude copies records forward when a session is resumed or forked. Token totals: uncached input, cached input, cache creation, output, reasoning (reasoning is a subset of output). Per-category cost, cache savings, sessions, record counts. | M |
| 9.5 | Usage limits | `UsageLimitSources.ts`: subscription quota windows pooled per provider. Reset credits via `provider.consumeResetCredit`. CLIProxyAPI hub integration (`cliproxyApi.ts`, including `chatgpt.com/backend-api/wham` and Anthropic `oauth/usage`), with backoff gated by background policy. | L |
| 9.6 | RPC | `server.getUsageSummary` and `server.refreshUsageRates`. Limit streams travel through the provider/server config. | S |

---

## 10. SQLite persistence layer

- **Code:** `apps/server/src/persistence/*`, `apps/server/src/orchestration-v2/{ProjectionMaintenance,ProjectionStore,EventSink,TurnItemPositionStore}.ts`, `apps/server/src/serverOwnership.ts`
- **Overall complexity: XL** if the existing database files must be read, migrated and kept compatible.

### 10.1 Connection setup — M
- **Code:** `persistence/Sqlite.ts`.
- **Pragmas:**
  - `busy_timeout=5000` (the CLI and the server write from separate processes)
  - `foreign_keys=ON`
  - `auto_vacuum=INCREMENTAL`, set only on a brand-new database
  - `journal_mode=WAL`, `synchronous=NORMAL`, `checkpoint_fullfsync=ON`
  - `journal_size_limit=32 MiB`
- **Connections:** one writer on the event loop, plus a read-only reader on a worker thread (`readerWorker`). Checkpoints run on a worker (`checkpointWorker`).
- **Planner statistics:** none. If `sqlite_stat1` exists it is emptied and `ANALYZE sqlite_schema` is run; slow queries name their index with `INDEXED BY`.

### 10.2 Migrations — L
- **Runner:** Effect `Migrator`, which records history in the table **`effect_sql_migrations(migration_id, name)`**.
- **Gotcha:** the runner skips every id up to the highest recorded id, regardless of name.
- **Guards:**
  - `assertV2MigrationsCanRun` fails if a V2 id (55 or later) was recorded under a different name.
  - After running, divergent history is logged as a warning.
- **`reconcileV2PreviewMigration.ts`:** preview builds recorded V2 as id 53 or 54. This renumbers them to 55 (and 55 to 56), runs `PullRequestFilesViewed` and `ProjectionThreadsAutoSettleDisabledAt` if needed, and inserts the missing history rows.

### 10.3 V1 → V2 database bootstrap — S
- **Code:** `initializeV2Database.ts`.
- **Behaviour:** if `statev2.sqlite` is missing and `state.sqlite` exists, it opens `state.sqlite` read-only, copies it with the SQLite online backup API into `.v2-import-*/snapshot.sqlite`, and hard-links that to `statev2.sqlite` (never overwriting an existing file).
- **Afterwards:** the copied V1 tables are the source for lazy transcript import (`orchestration-v2/legacy/LegacyV1ThreadImporter.ts`, `orchestration_v2_legacy_imports`).

### 10.4 Online maintenance — M
- **Code:** `persistence/DatabaseStorage.ts`.
- **Event-store compaction:** starts 1 minute after boot, then every 6 hours (`ProjectionMaintenance.compactEventStore`).
  - Pages newest-first, 50 rows at a time.
  - Deletes superseded thread events per thread and type, but keeps the first and last event for each keyed entity (message, node, PR link).
  - Deletes v1 events for threads whose transcript has been imported.
  - Prunes receipts.
- **Free-page reclaim:** every minute, `PRAGMA incremental_vacuum(1)` up to 1,024 pages per pass, yielding between pages.
- **Status:** publishes `{databaseBytes, reclaimableBytes, incrementalVacuumEnabled, compactCommand}` through `server.getDatabaseStorage`, which is a stream.

### 10.5 Offline doctor — M
- **Code:** `persistence/DatabaseMaintenance.ts`, used by `t3 doctor`.
- **API:**
  - `inspect`, which also reports the size of the legacy `state.sqlite`.
  - `compact`: takes an exclusive lock (`busy_timeout=0`, which fails if any server has the file open), verifies projections, compacts history, then `VACUUM INTO` with incremental auto_vacuum.
  - `removeLegacyDatabase`.
- **Errors:** DatabaseMissing, DatabaseInUse, DatabaseProjectionsInvalid, DatabaseInspect, DatabaseCompact, LegacyDatabaseRemove.

### 10.6 Projection verify and rebuild — L
- **Code:** `orchestration-v2/ProjectionMaintenance.ts`.
- **Verify:** compares the set of threads with a `thread.created` event against `orchestration_v2_projection_threads`, and `orchestration_v2_projection_metadata.schema_version` / `last_sequence` against `ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION`.
- **Rebuild:** replays the event store to regenerate the projections.

### 10.7 Single-instance lock — S
- **File:** `stateDir/server-owner.sqlite`.
- **How:** `BEGIN EXCLUSIVE` with `busy_timeout=0`, held for the life of the process. The OS releases it even after SIGKILL. The file must never be deleted.

### 10.8 Every migration file

Registry: `apps/server/src/persistence/Migrations.ts`. Migration 055 runs a set of sub-steps in order (listed after the table).

| Id | File (`persistence/Migrations/…`) | Effect |
|---|---|---|
| 1 | `001_OrchestrationEvents.ts` | **CREATE `orchestration_events`** (`sequence` autoincrement primary key, `event_id` unique, `aggregate_kind`, `stream_id`, `stream_version`, `event_type`, `occurred_at`, `command_id`, `causation_event_id`, `correlation_id`, `actor_kind`, `payload_json`, `metadata_json`). Unique index on `(aggregate_kind, stream_id, stream_version)`; indexes on stream+sequence, `command_id`, `correlation_id`. |
| 2 | `002_OrchestrationCommandReceipts.ts` | **CREATE `orchestration_command_receipts`** (`command_id` primary key, `aggregate_kind`, `aggregate_id`, `accepted_at`, `result_sequence`, `status`, `error`) with indexes. |
| 3 | `003_CheckpointDiffBlobs.ts` | CREATE `checkpoint_diff_blobs` (thread_id, from/to turn count, diff, created_at). **Dropped in 63.** |
| 4 | `004_ProviderSessionRuntime.ts` | **CREATE `provider_session_runtime`** (`thread_id` primary key, `provider_name`, `adapter_key`, `runtime_mode`, `status`, `last_seen_at`, `resume_cursor_json`, `runtime_payload_json`). Indexes on status and provider. |
| 5 | `005_Projections.ts` | CREATE V1 projections: **`projection_projects`**, **`projection_threads`**, **`projection_thread_messages`**, `projection_thread_activities`, **`projection_thread_sessions`**, `projection_turns`, `projection_pending_approvals`, `projection_state`, with indexes. |
| 6 | `006_ProjectionThreadSessionRuntimeModeColumns.ts` | `projection_thread_sessions` + `runtime_mode` (default `full-access`), backfill. |
| 7 | `007_ProjectionThreadMessageAttachments.ts` | `projection_thread_messages` + `attachments_json`. |
| 8 | `008_ProjectionThreadActivitySequence.ts` | `projection_thread_activities` + `sequence`, plus index. |
| 9 | `009_ProviderSessionRuntimeMode.ts` | No-op. |
| 10 | `010_ProjectionThreadsRuntimeMode.ts` | `projection_threads` + `runtime_mode`. |
| 11 | `011_OrchestrationThreadCreatedRuntimeMode.ts` | Backfills `$.runtimeMode='full-access'` in the payloads of `thread.created` events. |
| 12 | `012_ProjectionThreadsInteractionMode.ts` | `projection_threads` + `interaction_mode` (default `default`). |
| 13 | `013_ProjectionThreadProposedPlans.ts` | CREATE `projection_thread_proposed_plans` (dropped in 63). |
| 14 | `014_ProjectionThreadProposedPlanImplementation.ts` | Plans + `implemented_at`, `implementation_thread_id`. |
| 15 | `015_ProjectionTurnsSourceProposedPlan.ts` | `projection_turns` + `source_proposed_plan_thread_id`, `source_proposed_plan_id`. |
| 16 | `016_CanonicalizeModelSelections.ts` | `projection_projects` + `default_model_selection_json`; `projection_threads` + `model_selection_json`. Drops the old `default_model` / `model` columns. Rewrites event payloads. |
| 17 | `017_ProjectionThreadsArchivedAt.ts` | `projection_threads` + `archived_at`. |
| 18 | `018_ProjectionThreadsArchivedAtIndex.ts` | Index on `(project, archived_at)`. |
| 19 | `019_ProjectionSnapshotLookupIndexes.ts` | Indexes on projects `(workspace_root, deleted_at)` and threads `(project, deleted, created)`. |
| 20 | `020_AuthAccessManagement.ts` | CREATE `auth_pairing_links` and `auth_sessions` (first version). |
| 21 | `021_AuthSessionClientMetadata.ts` | Pairing link + `label`; session + `client_label`, `client_ip_address`, `client_user_agent`, `client_device_type`, `client_os`, `client_browser`. |
| 22 | `022_AuthSessionLastConnectedAt.ts` | `auth_sessions` + `last_connected_at`. |
| 23 | `023_ProjectionThreadShellSummary.ts` | `projection_threads` + `latest_user_message_at`, `pending_approval_count`, `pending_user_input_count`, `has_actionable_proposed_plan`. |
| 24 | `024_BackfillProjectionThreadShellSummary.ts` | Backfills those counters and approvals. |
| 25 | `025_CleanupInvalidProjectionPendingApprovals.ts` | Deletes invalid approvals and fixes counts. |
| 26 | `026_CanonicalizeModelSelectionOptions.ts` | Rewrites model-selection option JSON in projections and events. |
| 27 | `027_ProviderSessionRuntimeInstanceId.ts` | `provider_session_runtime` + `provider_instance_id`, plus index. |
| 28 | `028_ProjectionThreadSessionInstanceId.ts` | `projection_thread_sessions` + `provider_instance_id`, plus index. |
| 29 | `029_ProjectionThreadDetailOrderingIndexes.ts` | Ordering indexes for activities and messages. |
| 30 | `030_ProjectionThreadShellArchiveIndexes.ts` | Shell active/archived indexes. |
| 31 | `031_AuthAuthorizationScopes.ts` | **Drops and recreates** `auth_pairing_links` (id, credential unique, method, scopes, subject, label, created_at, expires_at, consumed_at, revoked_at) and `auth_sessions` (session_id, subject, scopes, method, client_* columns, issued_at, expires_at, last_connected_at, revoked_at). Each gets an "active" index. |
| 32 | `032_AuthPairingProofKeyThumbprint.ts` | `auth_pairing_links` + `proof_key_thumbprint` (DPoP). |
| 33 | `033_ProjectionThreadsSettled.ts` | Threads + `settled_override`, `settled_at`. |
| 34 | `034_ProjectionThreadsSnoozed.ts` | Threads + `snoozed_until`, `snoozed_at`. |
| 35 | `035_ProjectionThreadTitleRegeneration.ts` | Threads + `title_regeneration_request_id`, `title_regeneration_started_at`. |
| 36 | `036_ProjectionThreadsPinned.ts` | Threads + `pinned_at`. |
| 37 | `037_ProjectionTurnsKeysetIndex.ts` | Turns keyset index. |
| 38 | `038_ProjectionThreadsPinOrderKey.ts` | Threads + `pin_order_key`. |
| 39 | `039_ProjectionProjectsDefaultThreadEnvMode.ts` | Projects + `default_thread_env_mode`. |
| 40 | `040_ProjectionProjectFaviconPath.ts` | Projects + `favicon_path`. |
| 41 | `041_AuthSessionClientConnection.ts` | Sessions + `client_surface`, `client_app_version`. |
| 42 | `042_ProjectionThreadLinkedPullRequest.ts` | Threads + `linked_pull_request_json`. |
| 43 | `043_ProjectionThreadsUnsettledAt.ts` | Threads + `unsettled_at`. |
| 44 | `044_ClearAutomaticProjectModelDefaults.ts` | Clears automatic project model defaults in projections and events. |
| 45 | `045_ProjectionProjectsAutoPull.ts` | Projects + `auto_pull`. |
| 46 | `046_RepairAutomaticSettlementTimestamps.ts` | Repairs `settled_at`. |
| 47 | `047_ProjectionProjectIcon.ts` | Projects + `project_icon_json`. |
| 48 | `048_ProjectionThreadBranchPullRequest.ts` | Threads + `branch_pull_request_json`. |
| 49 | `049_ProjectionThreadsActiveOrderKey.ts` | Threads + `active_order_key` (only if the column is missing). |
| 50 | `050_ProjectionThreadPullRequests.ts` | **CREATE `projection_thread_pull_requests`** (primary key thread_id+host+repository+number; url, source, linked_at, snapshot_json, stack_json), with a backfill from `linked_pull_request_json`. |
| 51 | `051_ProjectionThreadMessageContext.ts` | Messages + `context_json`. |
| 52 | `052_ProjectionThreadTitleState.ts` | Threads + `title_state_json`. |
| 53 | `053_PullRequestFilesViewed.ts` | **CREATE `pull_request_files_viewed`** (primary key provider, host, repository, number, viewer, path; revision, viewed_at; WITHOUT ROWID). |
| 54 | `054_ProjectionThreadsAutoSettleDisabledAt.ts` | Threads + `auto_settle_disabled_at`. |
| 55 | `055_OrchestrationV2.ts` | Base V2 schema (below), then sub-steps a–k. |
| 56 | `056_RemoveRedundantProjectionIndexes.ts` | Drops four redundant V1 indexes. |
| 57 | `057_ScheduledTaskWebhooks.ts` | `scheduled_tasks` + `webhook_token`, `webhook_secret`. **CREATE `scheduled_task_webhook_deliveries`** (delivery_id primary key, task_id, received_at, method, query, headers_json, body, body_bytes, body_truncated, outcome, signature_verified, missing_fields_json, rendered_prompt, error), index `(task_id, received_at)`. |
| 58 | `058_WebhookRelayDeliveries.ts` | **CREATE `scheduled_task_webhook_relay_deliveries`** (relay_delivery_id primary key, task_id, seen_at), index on `seen_at`. |
| 59 | `059_McpAppModelContext.ts` | **CREATE `mcp_app_model_context`** (primary key thread_id+item_id; server, tool, text, updated_at). |
| 60 | `060_ThreadSnapshotWindowIndexes.ts` | Partial indexes: turn items where `type='user_message'`; nodes where status is live. |
| 61 | `061_PullRequestReviewGuides.ts` | **CREATE `pull_request_review_guides`** (primary key host+repository+number; guide_json; WITHOUT ROWID). |
| 62 | `062_ThreadFailureLookupIndex.ts` | Partial index on turn items where `type='error' AND status='failed'`. |
| 63 | `063_DropUnusedV1Projections.ts` | **DROP** `projection_thread_activities`, `projection_turns`, `projection_thread_proposed_plans`, `projection_pending_approvals`, `projection_state`, `checkpoint_diff_blobs`. |
| 64 | `064_CanonicalizeThreadPullRequestLinks.ts` | Rewrites PR-link JSON in V2 projection threads and in events. |

**Base schema of migration 055:** `orchestration_v2_events`, `orchestration_v2_command_receipts`, and the V2 projection tables `orchestration_v2_projection_{threads, runs, run_attempts, nodes, provider_sessions, provider_threads, provider_turns, runtime_requests, messages, plans, turn_items, checkpoint_scopes, checkpoints, context_handoffs, context_transfers}`, each with indexes.

**Sub-steps of migration 055, in this order** (files in `persistence/Migrations/OrchestrationV2/`):
- (a) `Subagents.ts` — CREATE `orchestration_v2_projection_subagents`.
- (b) `Foundation.ts` — adds `driver` and `provider_instance_id` shadow columns to the events, threads, runs, run attempts, provider sessions, provider threads and subagents tables, and `source_/target_provider_instance_id` to context transfers. Creates **`orchestration_v2_effect_outbox`**, **`orchestration_v2_turn_item_positions`** (primary key thread+item, unique thread+ordinal, backfilled as `run.ordinal*1e6 + row_number`) and **`orchestration_v2_projection_metadata`** (projection_name, schema_version, last_sequence, updated_at; seeded with `thread-projections`).
- (c) `ProviderSessionBindings.ts` — CREATE `orchestration_v2_projection_provider_session_bindings` (provider_session_id, thread_id), backfilled.
- (d) `ThreadLaunchWorkflows.ts` — CREATE `orchestration_v2_thread_launch_workflows` (currently unused).
- (e) `ApplicationEventSource.ts` — `orchestration_events` + `application_event_version` (default 1), with an index. `orchestration_command_receipts` + `command_type` (default `legacy`). **Copies every `orchestration_v2_events` row into `orchestration_events` as version 2**, with V2 fields moved into the metadata, and copies the V2 receipts across.
- (f) `EffectCancellation.ts` — rebuilds the outbox table so `status` can also be `cancelled`, and adds the thread/status index.
- (g) `ScheduledTasks.ts` — **CREATE `scheduled_tasks`** (columns in §2.3) with a partial due index `(enabled, next_run_at) WHERE enabled=1 AND next_run_at IS NOT NULL` and an index on `(project_id, updated_at)`.
- (h) `LegacyV1ImportState.ts` — **CREATE `orchestration_v2_legacy_imports`** (thread_id, source_updated_at, shell_imported_at, transcript_imported_at, imported_message_count, last_error).
- (i) `ApplicationEventSequenceIndexes.ts` — application high-water and agent stream indexes on `orchestration_events`.
- (j) `RecoveryIndexes.ts` — recovery indexes (created threads; runs, requests and turn items).
- (k) `ShellIndexes.ts` — shell indexes (turn items thread/run, shell pending, latest user message).

### 10.9 Final live table catalogue in `statev2.sqlite` (after migration 64)

| Table | Group | Purpose |
|---|---|---|
| `effect_sql_migrations` | infra | Migration history |
| `orchestration_events` | **event store** | One append-only log for both V1 (`application_event_version=1`, aggregate project/thread) and V2 events (`=2`, run/node/driver/instance in `metadata_json`). Compacted. |
| `orchestration_command_receipts` | **receipts** | Command idempotency: command_id, aggregate, accepted_at, result_sequence, status, error, command_type |
| `orchestration_v2_effect_outbox` | **outbox** | Side effects after commit: effect_id, command_id, thread_id, effect_type, payload, status (`pending`, `running`, `succeeded`, `failed`, `cancelled`), attempt_count, available_at, lease_owner, lease_expires_at, timestamps, last_error. Claim index on `(status, available_at, lease_expires_at, created_at)`. |
| `orchestration_v2_projection_metadata` | projection | Projection schema version and last applied sequence |
| `orchestration_v2_projection_threads` | projection | Thread shell (project, title, default provider and instance, runtime/interaction mode, active provider thread, archived/deleted, payload) |
| `orchestration_v2_projection_runs`, `…_run_attempts` | projection | Runs (ordinal per thread, provider, status) and their attempts |
| `orchestration_v2_projection_nodes` | projection | Execution graph nodes (parent/root, kind, status, provider thread/turn, runtime request, checkpoint scope) |
| `orchestration_v2_projection_provider_sessions`, `…_provider_session_bindings`, `…_provider_threads`, `…_provider_turns` | projection | Provider runtime objects |
| `orchestration_v2_projection_runtime_requests` | projection | Approvals and user-input requests |
| `orchestration_v2_projection_messages` | projection | Messages (role, streaming); thread search reads `json_extract(payload_json,'$.text')` |
| `orchestration_v2_projection_plans` | projection | Plans |
| `orchestration_v2_projection_turn_items` | projection | Timeline items (activity log source), with partial indexes |
| `orchestration_v2_turn_item_positions` | projection | Stable ordinal per turn item |
| `orchestration_v2_projection_checkpoint_scopes`, `…_checkpoints` | projection | Git checkpoint tree |
| `orchestration_v2_projection_context_handoffs`, `…_context_transfers` | projection | Provider switching and thread context transfer |
| `orchestration_v2_projection_subagents` | projection | Subagents |
| `orchestration_v2_legacy_imports` | migration state | Lazy V1 transcript import progress |
| `orchestration_v2_thread_launch_workflows` | (unused) | Thread launch saga; no code reads or writes it |
| `orchestration_v2_events`, `orchestration_v2_command_receipts` | legacy | Superseded by the unified tables; only the legacy importer still references `orchestration_v2_events` |
| `projection_projects` | **live** | Project records (still read by `ProjectStore.ts`, `ThreadSearch.ts`, `ThreadFind.ts`, `serverSettings.ts`, `storageCleanup.ts`) |
| `projection_threads`, `projection_thread_messages`, `projection_thread_pull_requests` | V1 legacy, read-only | Source for the V1 thread importer |
| `projection_thread_sessions` | V1 legacy | Read by `serverSettings.ts` |
| `provider_session_runtime` | live | Provider resume cursor per thread (`persistence/ProviderSessionRuntime.ts`) |
| `auth_pairing_links` | auth | One-time pairing credentials (scopes, DPoP thumbprint) |
| `auth_sessions` | auth | Session tokens plus client metadata |
| `scheduled_tasks` | scheduler | Task definitions and run state, webhook token and secret |
| `scheduled_task_webhook_deliveries` | scheduler | Delivery log (newest 50 per task) |
| `scheduled_task_webhook_relay_deliveries` | scheduler | Relay dedup claims (48 h) |
| `mcp_app_model_context` | MCP apps | Model context text per thread and item (`mcpApps/McpAppModelContext.ts`, `mcpApps.updateModelContext`) |
| `pull_request_files_viewed` | PR review | "Viewed" checkboxes (`persistence/PullRequestFilesViewed.ts`, at most 500 rows per page; un-ticking deletes the row) |
| `pull_request_review_guides` | PR review | Cached AI review guides (`persistence/PullRequestReviewGuides.ts`) |

**Repository services** in `persistence/`: `OrchestrationEventStore.ts`, `OrchestrationCommandReceipts.ts`, `ProviderSessionRuntime.ts`, `AuthSessions.ts`, `AuthPairingLinks.ts`, `PullRequestFilesViewed.ts`, `PullRequestReviewGuides.ts`, plus `Errors.ts`.
- **`OrchestrationEventStore` API:** `appendProjectEvent`, `appendAgentEvents`, `readAgentEvents`, `getAgentReplayStats`, `getReplayStats`, `latestAgentSequence`, `latestApplicationSequence`, `readApplicationEvents`, `publishCommitted` (publish only after the transaction commits), `streamApplicationEvents`, `streamProjectedApplicationEvents`.

---

## 11. Thread search, find in thread, and activity log

| Feature | Detail | Paths | Cx |
|---|---|---|---|
| Cross-thread search (`orchestration.searchThreads`) | Query of 2–200 characters, limit 1–50, snippet up to 240 characters. SQL `LIKE … ESCAPE '!'` over `json_extract(messages.payload_json,'$.text')` joined to non-deleted threads and projects. Best match per thread (`thread_match_rank=1`). Runs on the reader worker because it scans every message. Result: `{threadId, projectId, source: user \| assistant, snippet, messageCreatedAt}`. | `orchestration-v2/ThreadSearch.ts`, `packages/contracts/src/threadSearch.ts` | M |
| Find in one thread (`orchestration.searchThread`, `orchestration.searchThreadStream`) | Searches user and assistant messages and proposed plans across the whole thread history. Segment extraction honours context, skills and cwd. Folded async answers are excluded. Counts occurrences. Streams results. Includes a memory projection with the same ordering as SQLite. | `orchestration-v2/ThreadFind.ts` | M |
| Activity log | Client-side grouping of consecutive tool-call turn items (`docs/user/activity-log.md`). The server's part is persisting turn items, and the live command-output stream `orchestration.subscribeCommandOutput` (`replace` / `append` chunks). | `orchestration-v2/ProjectionStore.ts` | S (server part) |
| Thread title regeneration | Contract `ThreadTitleRegeneration {requestId, startedAt}`; the projection columns and title state live on the thread. | `packages/contracts/src/threadTitle.ts` | S |

---

## 12. Keybindings — M
- **Code:** `apps/server/src/keybindings.ts` (709 lines), `packages/contracts/src/keybindings.ts`, `packages/shared/src/keybindings.ts` (`DEFAULT_KEYBINDINGS` and parsers). Doc: `docs/user/keybindings.md`.
- **Storage:** `stateDir/keybindings.json`, a JSON array of `{key, command, when?}`.
- **Limits:**
  - Key strings up to 64 characters; `when` up to 256 characters and 64 levels of nesting.
  - At most 256 rules.
  - Script ids up to 24 characters (`script.{id}.run`).
  - 85 static commands, plus thread-jump and model-picker-jump commands.
- **Behaviour:**
  - The server creates the file with defaults and on startup adds new defaults (`syncDefaultKeybindingsOnStartup`) without overriding commands the user customised.
  - "Late defaults" are added alongside an existing rule. Example: `composer.sendBackground` on `mod+enter` next to `mod+alt+enter`.
  - Invalid rules are dropped and reported as issues; a file that will not parse falls back to the defaults.
  - Rules are parsed and resolved: the shortcut is parsed (`mod`, `meta`/`cmd`, `ctrl`/`control`, `alt`/`option`, `shift`) and the `when` expression becomes an AST (`!`, `&&`, `||`, parentheses).
  - Watches the config directory with a 100 ms debounce, revalidates and emits changes.
  - Upsert can replace a specific rule; remove deletes one.
- **RPC and streams:** `server.upsertKeybinding`, `server.removeKeybinding`. The resolved config and issues are part of `server.getConfig` / `subscribeServerConfig`.

---

## 13. Attachments store — M
- **Code:** `apps/server/src/attachmentStore.ts`, `attachmentPaths.ts`; RPC `assets.*`, `attachments.createUploadUrl`, `attachments.delete`.
- **Attachment ids:** `<thread-segment, at most 80 chars, [a-z0-9_-]>-<uuid>.<ext>`.
  - Pending uploads use the segment `pending`.
  - Deterministic ids derive from the message plus a hash of the bytes, so retries keep the same id.
  - `.part` is reserved for in-flight uploads.
- **Clean-up:** pending files are swept after 24 h, partial uploads after 1 h.
- **Path safety:** paths are normalised and must stay inside the attachments root.
- **Ownership:** HTML-render attachment ids are scoped per thread, so deleting a fork never removes its source's files.
- **Limits (user doc):** 100 files per message; images up to 10 MiB each and 80 MiB total; other files up to 50 MiB.

---

## 14. Storage cleanup — L
- **Code:** `apps/server/src/storageCleanup.ts` (1,120 lines). Settings `StorageCleanupSettings` and `StorageCleanupReport` in `packages/contracts/src/settings.ts`.
- **When it runs:**
  - Every hour.
  - When the cleanup or worktree settings change.
  - On a `thread.deleted` event, or a provider session going stopped or errored, if a policy has `worktreeOnDelete`.
  - Manually through `server.runStorageCleanup`.
- **Worktree policies:**
  - `worktreeKeepWhen` (default `uncommitted-changes`), `worktreeAfterDays`, `worktreeSettledAfterDays`, `worktreeOnMerge`, `worktreeOnDelete`, `worktreeUnchanged`.
  - A worktree is never removed while a live terminal's cwd or worktree is inside it.
  - Orphaned worktrees get a 1 h grace period; settle scripts get 60 s.
  - "Merged" means the PR is merged and either the branch was integrated or the PR's head SHA equals the worktree head.
  - Only a complete subagent chain inside one worktree counts as sharing ownership.
  - The activity clock ignores PR metadata refreshes.
- **File policies:** `browserArtifactsAfterDays` (browser-artifacts directory) and `logsAfterDays` (rotated logs in the logs directory) delete files by mtime. The root's real path is checked first.
- **Report:**
  - Fields: `trigger` (`automatic` / `manual`), started/finished, `entries[]` (kind `worktree` / `browser-artifacts` / `logs`; outcome `removed` / `kept` / `failed`; reason, path, thread, bytes, files), counts, `omittedCount` (more than 200 entries), `bytesFreed`.
  - Served by `server.getStorageCleanupReport`, which is a stream.
- **Note:** heap snapshots are never cleaned up.

---

## 15. Small utilities

| Item | Detail | Path | Cx |
|---|---|---|---|
| `rpcInitialItems` | A stream that emits the initial items once and then drops its reference to them, so a long-lived subscription does not keep the snapshot in memory. | `apps/server/src/rpcInitialItems.ts` | S |
| `subscribeBeforeSnapshot` | Pattern: subscribe to a PubSub and take a snapshot, either both under a mutex or subscribe-first without one. Used everywhere for "snapshot then live" streams. | `apps/server/src/utils/subscribeBeforeSnapshot.ts` | S |
| `compileCache` | Flushes Node's module compile cache after startup (Windows force-kills on quit). | `apps/server/src/compileCache.ts` | N.A. in Rust |
| `serverLogger` | §6.1. | `apps/server/src/serverLogger.ts` | S |
| `forkParked` | Parks forked work until server activation and startup recovery finish (used by Scheduler, DatabaseStorage, StorageCleanup). | `apps/server/src/serverActivation.ts` | S |

---

## 16. RPC / WebSocket method names (exact strings)

`WS_METHODS` is defined in `packages/contracts/src/rpc.ts`.

**Terminal:**
- `terminal.open`
- `terminal.attach` (stream)
- `terminal.observe` (stream)
- `terminal.write`
- `terminal.resize`
- `terminal.clear`
- `terminal.restart`
- `terminal.close`
- `subscribeTerminalEvents` (stream)
- `subscribeTerminalMetadata` (stream)

**Scheduled tasks and secrets:**
- `scheduledTasks.list`
- `scheduledTasks.subscribe` (stream)
- `scheduledTasks.upsert`
- `scheduledTasks.setEnabled`
- `scheduledTasks.delete`
- `scheduledTasks.runNow`
- `scheduledTasks.rotateWebhookToken`
- `scheduledTasks.listWebhookDeliveries`
- `scheduledTasks.getWebhookDelivery`
- `secrets.answerRequest`

**HTTP webhook routes:** `POST`, `PUT`, `PATCH` and `GET` on `/api/hooks/:hookId/:token` (`environmentHttp.ts`, endpoint names `webhookPost`, `webhookPut`, `webhookPatch`, `webhookGet`).

**Telemetry, diagnostics and observability:**
- `server.getTraceDiagnostics`
- `server.getProcessDiagnostics`
- `server.getHostResources`
- `server.getProcessResourceHistory`
- `server.getResourceTelemetryHistory`
- `server.retryResourceTelemetry`
- `server.checkOtlpEndpoint`
- `server.signalProcess`
- `subscribeResourceTelemetry` (stream)
- HTTP `POST /api/observability/v1/traces` (browser OTLP ingest)

**Background and power:**
- `server.reportClientActivity`
- `server.reportHostPowerState`
- `server.getBackgroundPolicy`
- `subscribeBackgroundPolicy` (stream)

**Usage:**
- `server.getUsageSummary`
- `server.refreshUsageRates`
- `provider.consumeResetCredit`

**Storage and database:**
- `server.runStorageCleanup`
- `server.getStorageCleanupReport` (stream)
- `server.getDatabaseStorage` (stream)

**Keybindings and config:**
- `server.upsertKeybinding`
- `server.removeKeybinding`
- `server.getConfig`
- `subscribeServerConfig` (stream)
- `server.getSettings`
- `server.updateSettings`
- `subscribeServerLifecycle` (stream)

**Attachments and assets:**
- `assets.createUrl`
- `assets.persistChatAttachments`
- `attachments.createUploadUrl`
- `attachments.delete`
- `mcpApps.updateModelContext` (writes `mcp_app_model_context`)

**Thread search** (`ORCHESTRATION_V2_WS_METHODS` in `packages/contracts/src/orchestrationV2.ts`):
- `orchestration.searchThreads`
- `orchestration.searchThread`
- `orchestration.searchThreadStream`
- `orchestration.subscribeCommandOutput` (activity output)

The rest of that set, for reference: `orchestration.dispatchCommand`, `orchestration.getTurnDiff`, `orchestration.getFullThreadDiff`, `orchestration.getArchivedShellSnapshot`, `orchestration.getThreadProjection`, `orchestration.getWorkflowScript`, `orchestration.stopWorkflow`, `orchestration.getTurnItem`, `orchestration.launchThread`, `orchestration.subscribeArchivedShell`, `orchestration.subscribeShell`, `orchestration.subscribeThread`.

**Notification-related command:** `notification.delivery.accept`, an orchestration command sent through `orchestration.dispatchCommand`.

**Relay HTTP API called by the server** (`packages/contracts/src/relay.ts`):
- `POST /v1/environments/:environmentId/threads/:threadId/agent-activity` (`publishAgentActivity`)
- `POST /v1/environments/:environmentId/hooks/wake` (`wakeHeldHooks`)

**Native monitor stdin commands:** `configure`, `setExternalProcesses`, `setSampleInterval`, `setStreaming`, `sampleNow`, `processTable`, `readHistory`, `shutdown`.
**Native monitor events:** `hello`, `snapshot`, `processTable`, `historyChunk`, `error`.

**Desktop telemetry fd protocol:**
- Inbound: `desktopTelemetryHello`, `desktopTelemetry`, `desktopUpdateStatus`.
- Outbound: `setDiagnosticsDemand`, `setHostPowerIntervals`, `requestDesktopUpdate`, `commitDesktopUpdate`, `cancelDesktopUpdate`.

---

## 17. Complexity roll-up

| Area | Estimate |
|---|---|
| Terminal manager (PTY, environment, sessions, history, sanitiser, persistence, activity polling, flow control) | **XL** |
| Scheduled tasks core (schedules, CRUD, runs, recovery, scheduler) | **L** |
| Webhook triggers (route, token/HMAC, relay proof and dedup, rate limits and queues, template rendering, delivery log, held-hook wake) | **L/XL** |
| Agent-activity relay publishing (push) | **L** |
| Product analytics | **M** |
| Observability (trace file with rotation, OTLP, metrics, stall monitor, diagnostics) | **L** |
| Resource telemetry (reuse the Rust monitor; client supervisor, desktop fd receiver, merge model, history) | **L** (XL if the desktop fd/update channel is included) |
| Background policy | **M** |
| Usage and cost (7 providers, incremental cache, pricing, limits, CLIProxyAPI) | **XL** |
| Thread search and find | **M** |
| Keybindings | **M** |
| Attachments | **M** |
| Storage cleanup | **L** |
| SQLite layer: pragmas, migrator compatibility (`effect_sql_migrations`), V1→V2 copy, reconcile, compaction and vacuum, doctor, projection verify/rebuild, owner lock | **XL** (to stay byte-compatible with existing `statev2.sqlite` files) |
