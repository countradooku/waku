# Waku Feature Catalogue (for a fork aiming at T3 Code parity)

Source: `waku/` checkout (v0.1.20, Cargo.toml), read-only. Paths are relative to the repo root.
Verification levels used below: **[V]** verified by reading code or docs; **[F]** inferred from file names or module docs, not read in depth. Two deeper sweeps (the per-view GPUI inventory/keybindings and the waku-core subsystem deep-dive) were still running at handback, so Section 3 is partly [F]. Section 4 (web/mobile) is fully read.

---

## 1. Architecture

### 1.1 Process split [V]
- **waku-daemon** (`crates/waku-daemon/src/main.rs`, 204 lines) is a thin binary. It binds a TCP listener (default `127.0.0.1:0`), reads the bearer token from env `WAKU_DAEMON_TOKEN`, then removes it from the env so child processes cannot inherit it. It prints a `DaemonReady {address, protocolVersion, pid}` JSON line on stdout and calls `waku_core::serve(...)` with `WakuBackend`.
  - Flags: `--bind`, `--allow-non-loopback` (required for any non-loopback bind), `--parent-pid` (it exits when the parent dies, and enables the `Shutdown` message), and repeatable `--allow-origin` for exact browser origins.
- **waku-core** (`crates/waku-core`, ~54k lines) holds everything the daemon does: the `server.rs` WebSocket server, the `daemon.rs` backend, provider drivers, SQLite persistence, git, checkpoints, worktrees, PTY, usage, skills and model catalogs.
- **waku-protocol** (`crates/waku-protocol`, ~8k lines) is the serde + ts-rs wire contract with no I/O. `PROTOCOL_VERSION = 7` and `MAX_WIRE_MESSAGE_BYTES = 48 MiB`. The `export_types` bin generates the TS types into `packages/waku-client` (`bun run protocol:generate` / `protocol:check`).
- **waku-client** (`crates/waku-client`, ~3.5k lines) is the Rust RPC client used by the desktop.
  - `DaemonSupervisor` spawns the child daemon with the configured exposure, or connects to an external one (`client.rs`, `process.rs`).
  - It also holds desktop persistence (`app.json`, `state.json`), composer completion, and the command env.
- **Desktop app** (`src/`, ~78k lines, GPUI). `src/daemon.rs::start_process()` connects to an external daemon when both `WAKU_DAEMON_ADDRESS` and `WAKU_DAEMON_TOKEN` are set. Otherwise it spawns the bundled `waku-daemon` (in dev, `target/debug/waku-debug-daemon`, which the dev watcher can hot-swap without relaunching the app).
  - `self.daemon.is_remote()` gates local-only features (see 2.12).
- **waku-computer-use** (`crates/waku-computer-use`, 503 lines) is the helper process hosting the Cua Driver SDK (computer use). It is dev-builds only.
- **GPUI dependency.** GPUI is pinned to the fork `github.com/egoist/zed`, branch `waku-webview` (upstream main plus PR #61945, layered scene rendering, needed for menus over the WKWebView). This is fork-relevant: you inherit a non-upstream GPUI.

### 1.2 Wire protocol (waku-protocol `src/protocol.rs`) [V]
Transport is JSON over WebSocket.

**Client → server messages (`ClientMessage`)**
- `hello {protocolVersion, token, clientId, resumeFrom: ReplayCursor[]}`
- `request {requestId, sessionId, runtimeId, command}`
- `shutdown`

**Server → client messages (`ServerMessage`)**
- `hello {protocolVersion, daemonVersion}`
- `rejected {message}`
- `response {requestId, outcome: ok{payload} | error{error:{message}}}`
- `event(SequencedEvent {sessionId, runtimeId, epoch, sequence, event:{kind,payload}})`
- `taskStateChanged {revision}`
- `shuttingDown`

**Replay.** `ReplayCursor {sessionId, runtimeId, epoch, sequence}`. The epoch changes on every daemon restart. Clients persist `RuntimeEventCursor` in the session so a reattach replays only the missed events.

**RPC commands (`Command`, all 53):**

| Area | Commands |
|---|---|
| Runtime / turn | `attachSession`, `start{options: WireDriverStartOptions}`, `prompt{prompt, turnId?, messageId?}`, `steer{prompt}`, `cancel`, `applyOptions{options: WireSessionOptions}`, `closeSession` |
| Approvals / input | `respond{requestId, optionId}`, `respondUserInput{requestId, answers}` |
| Background work | `refreshBackgroundWork`, `stopBackgroundWork{key, controlId}` |
| Goals (Codex) | `goal{operation: refresh \| set{objective?, status?, replace} \| clear}` |
| Computer use | `cancelComputerUse`, `runComputerTool{request}`, `rejectComputerTool{request, reason}`, `probeComputerPermissions{prompt}` |
| Rewind / fork | `rollback{turns}`, `fork{turnsToRemove}`, `forkSessionFromResponse{turnCount}`, `rewindSessionToMessage{turnCount}`, `forkProviderSession{request}` |
| Settings / probes | `getSettings`, `updateSettings{settings: DaemonSettings}`, `probeProvider{provider, binaryOverride, discoverModels, probeVersion}`, `fetchPlanUsage{provider, binaryOverride, cliVersion}` |
| Usage | `loadUsageHistory{window, projectRoots}` |
| Skills | `loadSkills{projects}`, `setSkillsEnabled{dirs, enabled}`, `trashSkills{dirs}` |
| Task state | `loadTaskState`, `saveTaskState{projects, liveSessionIds, sessions}` (merge-only), `removeSession`, `hydrateSession{sessionId}`, `searchSessionMessages{query, limit}` |
| Import native sessions | `listProviderSessions{provider, limit}`, `loadProviderSession{cursor, cwd}` |
| Drafts | `loadComposerDrafts`, `saveComposerDrafts{drafts, generation}`, `applyComposerDraftChanges{changes}` |
| Blobs / attachments | `storeBlob{mimeType, bytes(b64)}`, `importAttachment{name, upload}`, `importPathAttachment{path}`, `readBlob{reference}`, `readAttachment{reference, path}`, `sweepBlobs` |
| Workspace | `workspace{operation: WorkspaceOperation}` (see below) |
| Terminal (daemon PTY) | `openTerminal{cwd, cols, rows}`, `writeTerminal{data b64}`, `resizeTerminal{cols, rows}`, `closeTerminal` |

**`WorkspaceOperation` sub-RPCs (27, in `src/workspace.rs`):**
- Files: `listTree`, `browseDirectory` (daemon-host folder browser), `readTextFile`, `writeTextFile`, `listProjectFiles{cap}`
- Composer: `discoverSlashCommands`
- Projectless workspaces: `createProjectlessWorkspace{prompt}`, `migrateProjectlessWorkspace`
- Branches and worktrees: `inspectBranches`, `checkoutBranch{branch, create}`, `createWorktree{projectPath, projectId, sessionId, prompt, baseBranch}`
- Commit and push: `inspectCommit`, `generateCommitMessage{includeUnstaged, invocation}`, `commit{message, includeUnstaged, push}`, `push`
- Checkpoints: `captureTurnStart`, `captureTurn`, `captureRef`, `restoreRef`, `hasRef`, `sessionTurnRefs`, `deleteRef`, `deleteTurnRefsAfter`, `deleteSessionRefs`, `copySessionRefs`
- Review: `collectReviewDiff{source: lastTurn \| uncommitted \| unstaged \| staged \| committed \| branch}`

**Response payloads (`ResponsePayload`):**
`ack`, `sessionRuntime{runtimeId?, supportsSteer}`, `started{supportsSteer}`, `optionsApplied{applied}`, `cursor`, `settings`, `providerProbe{probe, version}`, `planUsage`, `computerPermissions`, `usageHistory`, `skillsCatalog`, `taskState{projects, sessions, defaultCwd, projectlessRoot}`, `taskStateSaved`, `session`, `sessionMessageMatches`, `providerSessions`, `providerSessionHistory`, `composerDrafts`, `blobStored`, `attachmentStored`, `blobData`, `providerSessionForked`, `sessionForked{session, checkpointWarning}`, `sessionRewound{session, cleanupWarning}`, `workspace{result}`.

**Event kinds** (`WireDriverEvent.kind`; `src/driver_wire.rs` and `core/terminal.rs`):
- Session and turn lifecycle: `connected`, `agentPresetSelected`, `autoTitleUpdated`, `availableCommands`, `promptSubmitted{message, turnId, messageId}` (mirrors a prompt to all clients), `turnStarted`, `turnParked` (Claude background wake), `turnFinished{success, summary}`, `error`, `processExited`
- Streaming content: `textDelta`, `reasoningDelta`, `activity`, `richActivity(ActivityItem)`
- Session-level work: `backgroundWork(upsert | outputDelta | reconcileProcesses | reconcileLive | stopRequested | stopFailed)`
- Requests to the user: `permission{requestId, title, detail, options}`, `userInputRequested{requestId, questions}`
- Steering: `steerAccepted`, `steerRejected{reason}`
- Usage and goals: `usageUpdated{contextTokens, contextWindow}`, `planUsageUpdated`, `goalUpdated`
- Computer use: `computerUseUpdated`
- Terminal: `terminalOutput{data}`, `terminalExited`, `terminalError`

Comparison with T3: there is no generic "orchestration domain events" or command bus. The model is a per-session runtime actor with a sequenced event stream plus a coarse task-state snapshot invalidation (`taskStateChanged`).

### 1.3 Storage [V]
- **SQLite** via `rusqlite` in `crates/waku-core/src/persistence.rs`, opened in WAL mode with `synchronous=NORMAL`.
  - Location: `temp/app.db` in debug builds (inside the checkout); `dirs::data_local_dir()/<DATA_DIRECTORY_NAME>/app.db` in release (on Windows, `%LOCALAPPDATA%\Waku\app.db`). Blobs live in `blobs/` next to the DB.
- **Schema** (`db/schema.ts` → `db/migrations/0000_init.sql`, `0001_clear_sugar_man.sql`, `0002_cute_miracleman.sql`):
  - `projects(id PK, name, path, position, created_at)`
  - `sessions(id PK, project_id, title, auto_title, provider, model, status, created_at, updated_at, last_reply_at)`, with indexes `sessions_by_project(project_id, updated_at)`, `sessions_by_updated_at`, `sessions_by_last_reply_at`
  - `messages(id PK, session_id, turn_id, position, role, content, display_content, attachments JSON default '[]', created_at, streaming)`, with index `messages_by_session(session_id, position)`
  - `session_details(session_id PK, data TEXT JSON)`: the rest of `AgentSession` (transcript blocks, turns with checkpoints, provider cursor, queued messages, goal, context usage, runtime event cursor, workspace/worktree)
  - `migrations(tag, applied_at)`: the Rust runner's own ledger
- **Why drizzle is there.** It is only a build-time schema-diff tool. `bun run db:generate` (drizzle-kit) diffs `db/schema.ts` and emits plain SQL. `crates/waku-core/build.rs` embeds `db/migrations/*.sql` in numeric-prefix order into the binary, and `persistence::apply_migrations` runs each untagged migration once.
  - drizzle-orm never ships and Rust owns every query.
  - `drizzle.config.ts` uses `migrations.prefix: "index"`, and the runner ignores drizzle's `_journal.json`.
- **Other files:**
  - `~/.waku/app.json`: desktop preferences (debug uses `temp/app.json`)
  - `state.json`: desktop navigation/UI state (it was formerly the pre-SQLite all-in-one store, now migrated)
  - `composer-drafts.json`
  - `~/.waku/settings.json`: daemon provider and Computer Use settings, owned by `DaemonSettingsStore`
  - `%LOCALAPPDATA%\Waku\updater.json` on Windows
  - LiteLLM price-table cache beside the DB
- **Save strategy.** A save writes only changed rows (streaming costs KBs). Sessions load as list "skeletons" (`detail_loaded=false`) and are hydrated on open (`hydrateSession`).

### 1.4 Session/task model [V]
- **Project** (`model.rs`): `{id, name, path, created_at}`. The special "projectless" project lives under `~/.waku/projects/<date>/<slug>` (`projectless.rs`). The old `~/.waku/<date>/<slug>` layout is migrated on first load.
- **AgentSession is Waku's "task"**, the equivalent of a T3 "thread". Fields:
  - Identity and titles: `id`, `title` (user) vs `auto_title` (provider), `project_id`
  - Workspace: `workspace: Local | NewWorktree{base_branch} | Worktree{path, branch}`
  - Provider settings: `provider`, `model`, `runtime_mode` (Ask/Supervised, AutoAcceptEdits, Auto, FullAccess), `reasoning_effort`, `service_tier`, `context_window`, `agent_preset`
  - Status: `status` (Idle, Connecting, Working, Waiting, Background, Failed)
  - Resume and provider state: `provider_cursor: ProviderResumeCursor`, `available_commands`, `thread_goal`, `context_usage`, `runtime_event_cursor`
  - Content: `messages`, `transcript_blocks` (reasoning / activities), `turns: AgentTurn{id, turn_count, status, provider_resume_at, checkpoint}`, `queued_messages`
- **Exactly one provider per task.** Changing provider restarts the runtime.
- **Runtime lifetime.** Runtimes live per session in the daemon.
  - Lazily created (`ensure_driver`).
  - Reaped after 30 min idle (5-min sweep, skipping active turns), the same thresholds as T3's `ProviderSessionReaper`.
  - Dropped on: provider change; an option change the transport cannot absorb; delete; rewind/branch leaving a stale native session; process exit; quit; and Stop for Codex and Amp.
- **Multi-client.** Several clients can attach to one runtime (`attachSession`). `promptSubmitted` mirrors user rows, and saves are merge-only so a stale client cannot delete tasks.

### 1.5 Checkpoints [V]
`crates/waku-core/src/checkpoint.rs` is daemon-owned and uses hidden git refs, the same idea as T3's checkpoint refs.
- **Refs:**
  - `refs/waku/session-<id>-turn-<n>` (end of turn)
  - `refs/waku/session-<id>-turn-start-<n>` (exact state accepted before the provider starts, so edits made between turns are not attributed to either turn)
  - `refs/waku/session-<id>-turn-diff-<n>`
- **Capture.** A temporary `GIT_INDEX_FILE`, then `write-tree`, then `commit-tree`. The working tree and the user's index are untouched.
- **Checkpoint record** on each `AgentTurn`: `{turn_count, git_ref, status Ready|Unavailable|Error, files[{path, additions, deletions}], cached totals}`.
- **Operations:**
  - Rewind (`rewindSessionToMessage`) restores the ref and truncates the provider conversation (native or emulated, see 2.1), then the edited message is resubmitted.
  - Fork (`forkSessionFromResponse`) copies refs (`copySessionRefs`) and forks the provider conversation into a new task.
  - Cleanup uses `deleteTurnRefsAfter` and `deleteSessionRefs`.
- **Coverage.** Conversation rewind/branch is gated per provider by `ProviderKind::supports_conversation_rollback/fork`: false for Kimi and Fx.

### 1.6 Provider adapter structure (waku-core `driver/`) [V]
Entry point: `driver::start_local(provider, DriverStartOptions, DriverEventSender) -> DriverHandle`. It calls the `driver/` transports directly and routes:
- Codex → `codex.rs`
- Pi and Oh My Pi → `pi.rs` with a `PiFlavor`
- Cursor, Fx, Grok, Kimi → `acp.rs`
- DeepSeek → `deepseek.rs`
- OpenCode → `opencode.rs`
- Claude → `claude.rs`
- Amp → `amp.rs`

There is no `headless.rs` in this checkout. `docs/providers.md` still names it as the one-shot fallback, so that doc is stale on this point.

**Interface each transport implements (`trait DriverControl: Send + Sync`):**
```rust
fn prompt(&self, prompt: String);                       // required
fn supports_steer(&self) -> bool { false }
fn steer(&self, prompt: String) {}                       // -> SteerAccepted/SteerRejected events
fn cancel(&self);                                        // required
fn cancel_computer_use(&self) {}
fn refresh_background_work(&self) {}
fn stop_background_work(&self, key: BackgroundWorkKey, control_id: String) {}
fn respond(&self, request_id: String, option_id: String); // required (approvals)
fn respond_user_input(&self, request_id: String, answers: Vec<UserInputAnswer>) {}
fn goal(&self, op: GoalOperation) {}
fn run_computer_tool(&self, req: ComputerToolRequest) {}
fn reject_computer_tool(&self, req: ComputerToolRequest, reason: String) {}
fn apply_options(&self, o: SessionOptions) -> bool { false } // false = restart me
fn rollback(&self, turns: usize) -> Result<Option<ProviderResumeCursor>>; // required
fn fork(&self, turns_to_remove: usize) -> Result<ProviderResumeCursor> { bail!() }
```
- **Start options:** `binary`, `cwd`, `mode`, `model`, `reasoning_effort`, `service_tier`, `context_window`, `agent_preset`, `computer_use_enabled` (clamped off in release), `provider_cursor`.
- **Output** is the `DriverEvent` enum (see 1.2), sent on a crossbeam channel with a coalesced wake.
- **Activity normalization.** Tool activity is normalized in `driver/activity.rs` into `ActivityItem{kind: Reasoning|Command|FileChange|FileRead|FileSearch|FileList|Search|Plan|Tool, tool_name, mcp_server, arguments, output, image_urls, file_changes, display_target, …}`.
- **Out-of-band helpers.** Per-provider session helpers for import, cold fork and transcript rewrite live in `*_session.rs`: `claude_session.rs` (JSONL rewrite), `amp_session.rs`, `cursor_session.rs`, `grok_session.rs`, `opencode_session.rs`, `pi_session.rs`, `codex_session.rs`, `kimi_session.rs`, `acp_session.rs`, `deepseek_session.rs`.
- **Other driver support modules:** `model_catalog.rs` (model discovery and caching), `title_refresh.rs` (polled titles), `usage.rs` (plan usage).
- **Capability flags** live on `ProviderKind` (`supports_conversation_rollback`, `supports_conversation_fork`, `supports_model_discovery`, …), not on the driver. Contrast T3's declared `capabilities` record on its adapter contract.
- **How to add a provider:** see `docs/providers.md` "Adding a provider" (9 steps).

---

## 2. Feature catalogue

Maturity key: **complete** / **partial** / **stub**.

### 2.1 Providers & adapters
There are 11 `ProviderKind`s on 7 transports. All of them keep one long-lived session per conversation. Sources: `docs/providers.md` [V] and driver greps [V].

| Provider (binary) | Protocol | Resume cursor | Fork/rewind | Steer | Interrupt | Approvals | Plan mode | Subagents | Images in prompt | Usage | Maturity |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Codex (`codex app-server`) | JSON-RPC stdio | thread_id (`thread/resume`) | native `thread/rollback`, `thread/fork` | `turn/steer` (server-checked expectedTurnId) | `turn/interrupt`, but Stop drops the runtime | yes (`*requestApproval*`, accept/acceptForSession/decline); `item/tool/requestUserInput` → questions | none (Plan only as activity rows) | `collabAgentToolCall` → BackgroundWork Subagent | text-only input; images only as tool output | context `usageUpdated`; `account/rateLimits/updated` → plan meters; goals (`/goal`); `/fast` | complete |
| Claude Code (`claude -p --input-format stream-json … --permission-prompt-tool stdio`) | NDJSON stdio | session_id + resume_at (msg uuid) | emulated: JSONL rewrite to a new session (`claude_session::fork_session_at`) | same-stream user message, folded | `control_request interrupt` | `can_use_tool` control requests; `AskUserQuestion` → structured questions | none; `ExitPlanMode` not specially handled | Task / background Bash / monitors → BackgroundWork (live output, stop); `turnParked` | text-only | context usage; plan usage via OAuth `api.anthropic.com/api/oauth/usage` | complete |
| Cursor (`cursor-agent acp`) | ACP | session_id + fork_context | emulated re-seed (`cursor_session.rs`) | second `session/prompt` | `session/cancel` | `session/request_permission`; Cursor ask-question extension | ACP `plan` updates as activity; legacy "plan" mode reset to agent | – | text | `usage_update` | complete |
| Grok Build (`grok agent stdio`) | ACP | session_id | own ACP server + disk truncation (`grok_session.rs`) | yes | yes | yes, plus xAI user-input extension | as Cursor | – | text | `x.ai/billing` probe for plan usage | complete |
| Kimi Code (`kimi acp`) | ACP | session_id (`session/resume`) | **none** (hidden) | transport only (unprobed) | yes | yes | – | – | text | usage_update | partial |
| Fx (`fx acp`) | ACP | session_id | **none** | **no** (queue only) | yes | yes (ask/code modes) | – | – | text | usage_update | partial |
| OpenCode 2 (shared background service, HTTP+SSE) | HTTP + SSE | session_id + directory | service `POST /fork {before}` | `delivery:"steer"` | `/interrupt` | `permission.asked` (once/reject only; "always" kept driver-local); `form.created` → questions | via agent preset (`build`/`plan` agents) | `session.shell.*` → BackgroundWork | text | `session.step.ended` tokens; OpenCode Go plan usage | complete |
| Pi (`pi --mode rpc --approve`) | NDJSON RPC | session_id + session_file | native `fork`/`clone` | `steer` | `abort` | **none** (Full access only) | – | – | text | usageUpdated | complete within limits |
| Oh My Pi (`omp --mode rpc --yolo`) | NDJSON RPC v2 | as Pi | native `branch`, plus a shell-out `--fork` | yes | yes | none (Waku forces `--yolo`) | – | – | text | yes | partial |
| Amp (`amp --execute --stream-json-input …`) | NDJSON stdio | thread_id + fork_context | export, then new thread seeded with `WAKU_AMP_BRANCH_CONTEXT_V1` envelope | `"steer": true` | **none**: Stop kills, resume via `threads continue` | **none** (Full access only) | – | – | text | minimal | partial |
| DeepSeek Harness (`dsh`) | typed HTTP + downlink streams | session_id | yes (flags true) | yes | yes | user-input yes | agent presets (Standard / Code / Minimal / Creator) | some | `images: []` arg exists but is unused | yes | partial (undocumented in providers.md) |

Cross-cutting provider features [V]:
- **Access modes:** Supervised / Auto-accept edits / Auto / Full access, mapped per CLI (`providers.md` table). Changing mode restarts the runtime, except on OpenCode.
- **In-session model / effort / tier switch** on every provider except Amp.
- **Model discovery** for every provider except Amp, with cached catalogs and fallbacks (`model_catalog.rs`). A favorites list exists (`FavoriteModel`).
- **Titles:** provider-pushed, polled from native stores (Claude/Amp/Grok), or generated for Codex on `gpt-5.6-luna` (`docs/titles.md`).
- **Slash commands:** provider-reported `availableCommands` plus filesystem discovery of skills and commands (`slash_command_catalog.rs`, `skills.rs`).
- **Import native CLI sessions:** `/resume` or the command palette, via `listProviderSessions` / `loadProviderSession`, on all providers.
- **Login-shell env inheritance** (`command_env.rs`), Windows `PATHEXT` / npm shim detection, and per-provider binary override in Settings → Providers.

**Provider gaps vs T3:**
- No native image attachments: images are appended as path mentions. T3 sends image parts.
- No plan / interaction mode toggle: it was removed, and `RuntimeMode::Ask` is legacy.
- No `ExitPlanMode` plan-approval flow.
- Pi and Amp cannot approve.
- Kimi and Fx cannot rewind.
- Claude rewind uses a hand-rolled JSONL rewrite. `--fork-session` is untested.

### 2.2 Orchestration/session model [V]
- **What it is:** per-task daemon runtime actors with sequenced and replayable events, a follow-up queue, steering, background-work tracking, idle reaping, and multi-client mirroring.
- **Paths:** `crates/waku-core/src/daemon.rs`, `server.rs`, `src/app/runtime.rs`, `src/app/streaming.rs`, `src/app/sessions.rs`, `src/app/background_work.rs`.
- **Queue vs steer:**
  - Enter queues a visible, editable follow-up that runs when the current turn settles.
  - Cmd/Ctrl+Enter steers.
  - Cmd/Ctrl+Enter in an empty composer steers the oldest queued item.
  - If steering is rejected, the message falls back to the queue.
- **Background work:** detached processes, monitors and subagents with live output and per-item stop (`BackgroundWorkEvent`).
- **Goals:** Codex thread goals with a dialog (`src/app/goal_dialog.rs`) and a status chip.
- **Maturity:** complete.
- **Gaps vs T3:** no event-sourced orchestration layer or projection rebuild; no multi-provider thread; no server-side command queue beyond the follow-up queue.

### 2.3 Checkpoints / rewind / git / worktrees / source control [V]
- **Checkpoints and rewind:** see 1.5. Rewind-to-message happens via "edit message"; fork happens from any response. Maturity: complete for 9 of 11 providers.
- **Branches** (`git_branch.rs`, `src/app/branches.rs`): inspect, checkout, create, with branch change counts including untracked files. Complete.
- **Worktrees** (`worktree.rs`): opt-in per draft (`SessionWorkspace::NewWorktree{base_branch}`). The worktree is created on first submit under `~/.waku/worktrees`. Complete for creation. Cleanup/merge-back UI is unverified [F].
- **Commit and push** (`git_commit.rs`, `src/app/commit_dialog.rs`):
  - Commit staged or all changes, with optional push.
  - An empty message generates one headlessly with the session's own CLI: Claude is pinned to haiku-4.5, Codex to gpt-5.6-luna (`docs/commit-messages.md`).
  - Maturity: complete.
- **Not present:** PR creation / `gh` integration (no references found), and merge/rebase/stash UI. These are gaps vs T3 if T3 has PR flows.
- **Diff sources for review:** last turn, uncommitted, unstaged, staged, committed, branch.

### 2.4 Composer inputs [V/F]
- **Paths:** `src/app/composer.rs` (3.9k lines), `src/app/autocomplete.rs`, `src/app/drafts.rs`, `crates/*/composer_complete.rs`, `src/input.rs` (custom text input, 4.1k lines, key map in `docs/text-field-keys.md`).
- **Implemented:**
  - Multiline input with IME support.
  - Attachments from paste, drag, clipboard images and file paths. These are stored as daemon blobs (`storeBlob` / `importAttachment` / `importPathAttachment`) and shown as chips.
  - `@`-style file mentions via `listProjectFiles` and nucleo fuzzy matching [F].
  - Slash-command autocomplete, with built-ins `/resume` (all), `/fast` (Codex), `/goal` (Codex), plus provider-native commands and skills (with native invocation syntax for Codex/Pi/omp).
  - Pickers for model (with search and favorites), reasoning effort, service tier/fast, context window, access mode, and agent preset (DeepSeek/OpenCode).
  - Queued follow-up cards with per-message steer.
  - Answering agent questions inside the composer.
  - Drafts persisted per task and per new-task target (daemon `composer drafts` RPC).
  - Height cap with overlay scrollbar.
- **Maturity:** complete.
- **Gaps:** native image parts (2.1), plan-mode toggle.

### 2.5 Transcript content types [V/F]
- **Paths:** `src/app/transcript.rs`, `src/app/transcript_view.rs`, `src/app/components.rs`, `src/md/*` (custom markdown parser and renderer, syntax highlighting, LaTeX via `ratex`, streaming "veil" and "mend").
- **Row types:**
  - User message (markdown, linkified, compact scrolling bubble, attachments)
  - Assistant markdown with code highlighting, tables and inline/block math (Copy Expression; show-source setting)
  - Web citations (Codex markers rewritten to links)
  - Reasoning blocks
  - Activity groups: Command (with output viewport), FileChange (inline diffs), FileRead / FileSearch / FileList, Search, Plan, Tool / MCP tool (with image outputs)
  - Permission prompts
  - User-input questions
  - Background-work rows / panel
  - Goal chip
  - Errors
  - Per-response actions and changed-file summary (checkpoint totals)
  - Live "latest activity" header; earlier activity groups collapse automatically
- **Search:** find-in-transcript (`transcript_search.rs`, Cmd/Ctrl+F); cross-session message search (`searchSessionMessages`).
- **Maturity:** complete.
- **Gaps:** no dedicated subagent transcript tree. Subagents appear only as background-work items.

### 2.6 Terminal/PTY [V]
- **Desktop:** `src/terminal.rs` (2k lines) is a local PTY built on `alacritty_terminal` 0.26, shown in a right-panel Terminal surface with an overlay scrollbar and font-measured cells. On Windows it uses pwsh → powershell → COMSPEC, with Ctrl+Shift+C/V for copy/paste.
- **Daemon PTY:** `crates/waku-core/src/terminal.rs` is unix-only (`#[cfg(unix)]`) and streamed via `openTerminal` / `writeTerminal` / `resizeTerminal` / `closeTerminal` with `terminalOutput` events. It is used by the web client [F].
- **Gap.** When the desktop is connected to a remote daemon, `ensure_right_panel_terminal` refuses (`src/app/right_panel.rs` ~L2321). Its code comment says this stays so "until the protocol grows a daemon-owned streaming terminal", even though that protocol now exists. The README statement is stale on the protocol side; the desktop simply does not use it yet.
- **Gap.** A Windows daemon has no PTY for browser clients (`docs/windows.md`).
- **Maturity:** partial (local is complete; remote desktop is missing).

### 2.7 Files / diff / review [V/F]
- **Paths:** `src/app/right_panel.rs` (4.8k lines; tabs for Files, Terminal, Browser, Review/Diff [F]), `src/review_diff.rs`, `src/app/file_search.rs`, `src/app/activity_diff.rs`, `src/app/image_preview.rs`.
- **Features:**
  - File tree (`listTree`)
  - Text editor with unsaved-buffer tracking and external-change pickup (`writeTextFile`)
  - Markdown highlighting with a source / rendered-preview toggle
  - Fuzzy file search
  - Review diff over six sources
  - Inline diffs in the transcript
  - Image preview
  - "Open in…" an external app (macOS; hidden when the daemon is remote)
- **Maturity:** complete.
- **Gaps:** no inline review comments sent back to the agent [F].

### 2.8 Projects / threads management [V/F]
- **Paths:** `src/app/sidebar.rs`, `src/app/project_picker.rs`, `src/app/task_switcher.rs`, `src/app/sessions.rs`.
- **Features:**
  - Add project via the native folder picker (refused when the daemon is remote: `errors.remote_project_picker`).
  - "No project" projectless tasks.
  - Sidebar grouping by project or update date, newest/oldest ordering, collapsible sections.
  - Searchable project picker.
  - Inline rename, delete (`removeSession`), copy the task ID and the native thread ID.
  - Recent-task switching (Ctrl+Tab / Ctrl+Shift+Tab).
  - Import native sessions.
  - Fork a task from a response.
- **Maturity:** complete.
- **Gaps:** no archive/pin found [F]. The daemon has a `browseDirectory` RPC but the desktop does not use it for remote project adding.

### 2.9 Command palette / keybindings / focus [F]
- **Paths:** `src/app/command_palette.rs` (2.2k lines), `src/ui/menu.rs`, `src/app.rs` / `src/lib.rs` (actions and menus), `docs/text-field-keys.md`.
- **Palette:** commands plus "resume / import external CLI session", copy IDs, and others. The full entry list was not captured before handback.
- **Known shortcuts:** Cmd/Ctrl+F find, Ctrl+Tab task switcher, Cmd/Ctrl+Enter steer, Escape stop [F]. AGENTS.md also requires full keyboard operability.
- **Maturity:** complete [F].

### 2.10 Settings / appearance / themes / i18n [V/F]
- **Path:** `src/app/settings.rs` (2.6k lines). Known pages:
  - General: automatic updates, language, analytics sharing [F]
  - Appearance: theme light/dark/system via `ThemePreference`; UI and code font sizes
  - Providers: binary overrides, probes
  - Daemon: expose on a fixed port, exact origins, copy URL/token, LAN hostname; replaced by an "external daemon" notice when remote
  - Computer Use (debug builds only)
  - Usage (`usage_page.rs`)
  - Skills (`skills_page.rs`: enable/disable/trash skills)
- **i18n:** `rust-i18n` with `locales/app.yml` (en, 2.2k lines), `zh-CN.yml`, `ja.yml`, shared through waku-protocol so the daemon is localized too.
- **Themes:** `src/theme.rs` / `waku-protocol/theme.rs`, light/dark preference. Custom themes were not found [F].
- **Maturity:** complete for the listed items.

### 2.11 Remote & multi-device [V]
- **Daemon exposure:**
  - Loopback by default. Settings → Daemon can expose the child daemon on a fixed port with exact `--allow-origin` values (it refuses while tasks are active).
  - A standalone daemon needs `--allow-non-loopback`.
  - No TLS is built in: README advises a reverse proxy or tunnel with `wss://`.
- **Auth:** one stable bearer token, which is full control (`WAKU_DAEMON_TOKEN`), checked in `hello`. There is no pairing flow, no scoped tokens, and no per-device revocation [V for the token model].
- **Remote desktop:** set `WAKU_DAEMON_ADDRESS` + `WAKU_DAEMON_TOKEN`. It auto-reconnects (0.1.15).
  - Disabled when remote: folder picker, PTY, "Open in…", reveal-in-Finder, and local path links.
- **Web client:** `apps/web` (TanStack Start on a Cloudflare Worker; see §4).
- **Mobile:** `apps/mobile` (Expo for iOS/Android; multiple daemon profiles; tokens in SecureStore; see §4).
- **Maturity:** partial.

### 2.12 Notifications / background service / scheduled tasks [V]
- **Notifications:** `src/platform.rs::show_task_notification`, using UNUserNotificationCenter on macOS and a non-mac implementation elsewhere. Triggered on task completion / attention needed [F]. Maturity: complete (basic).
- **Background service:** the daemon is a child of the desktop (`--parent-pid`, it dies with the app) unless run standalone. There is no launchd/systemd service, tray or menu-bar agent. Gap.
- **Scheduled tasks / automations:** **none**. A grep for cron/schedule finds only UI timers. Gap.

### 2.13 MCP & agent tools [V]
- **What exists:**
  - MCP tool calls are rendered with `mcp_server` / `tool_name` and image outputs.
  - Waku registers its own QuickJS JS-REPL MCP server (`waku_js_repl`, tools `js` / `js_reset`) into Codex (`-c mcp_servers…`) and OpenCode (temporary `/api/experimental/mcp`). This is for Computer Use only, in debug builds.
  - Skills management page.
- **Missing:** no user-facing MCP server management. Gap.
- **Maturity:** stub as a user feature.

### 2.14 Usage / telemetry / activity / resource monitoring [V]
- **Context meter** (`src/app/usage_meter.rs`): live context-window occupancy per task.
- **Plan usage** (`usage.rs`):
  - Claude: OAuth usage API using the keychain or `~/.claude/.credentials.json`
  - Codex: ChatGPT backend usage plus live rateLimits
  - OpenCode Go usage API
  - Grok: `x.ai/billing`
- **Usage history page** (`usage_history.rs`, `src/app/usage_page.rs`): token and cost history scanned from `~/.claude/projects` and `~/.codex/sessions` (ccusage-style), priced with LiteLLM's table.
- **Telemetry** (`src/analytics.rs`):
  - Umami via `rust-umami`, release builds only.
  - Endpoint and site ID are compiled in from `WAKU_ANALYTICS_*` env at build time; a fork that doesn't set them gets no telemetry.
  - Opt-out with `WAKU_DISABLE_ANALYTICS=1` or the in-app sharing toggle.
  - Payload: no prompts or paths, plus a random installation ID.
- **Missing:** no CPU/memory resource monitor (there is only a dev FPS counter).
- **Maturity:** complete (usage), present (telemetry).

### 2.15 Install / update / packaging [V]
- **macOS:** signed and notarized `.dmg` via `scripts/bundle.sh`; Sparkle with deltas; feed `releases.waku.sh` (`resources/Info.plist` holds `SUFeedURL` / `SUPublicEDKey`).
- **Linux:**
  - `install.sh` (`website/public/install.sh`) installs to `~/.local/waku.app`, plus a tarball (`scripts/bundle-linux.sh`).
  - Self-update (`src/updater/linux.rs`, `src/bin/waku-updater.rs`) uses an Ed25519-verified feed, prefix swap and automatic rollback.
  - x86_64/aarch64, glibc 2.35 or newer.
- **Windows:**
  - Inno Setup per-user installer plus a portable zip (`scripts/bundle-windows.ts`, `resources/windows/waku.iss`).
  - EdDSA-verified self-update; optional Authenticode signing.
  - x86_64/arm64.
- **CI release:** `.github/workflows/release.yml` builds 5 targets on `v*` tags and creates a draft release; `sync-release.yml` copies assets to Cloudflare R2. CHANGELOG.md is the source of the in-app release notes.
- **Fork impact:** a fork must replace the signing keys, feed URLs, R2 bucket, Apple certificate and analytics env.
- **Maturity:** complete.

### 2.16 Browser / computer use / JS REPL / misc [V]
- **Browser** (`src/browser.rs`, 2.7k lines): an embedded right-panel browser using WKWebView on macOS (via `wry`) and WebView2 on Windows, with devtools, downloads and open-external. It is unavailable on Linux.
  - CONTRIBUTING still says macOS-only, while `docs/windows.md` documents WebView2. The docs are inconsistent.
- **Computer Use:** Cua Driver 0.28 SDK in the `waku-computer-use` helper, plus `src/js_repl.rs` (QuickJS via rquickjs), `docs/computer-use.md`.
  - Supported for Codex, Pi, OpenCode and Grok.
  - **Debug builds only.** Release builds clamp it off.
- **Misc:**
  - Window state restore
  - Animated sidebars
  - Reduce-motion support
  - Custom markdown engine with LaTeX
  - "Open in…" app detection (macOS)
  - Daemon hot-swap in dev

### 2.17 Persistence / migration [V]
- SQLite with drizzle-generated SQL migrations embedded by `build.rs` (1.3).
- Legacy migrations handled:
  - `state.json` → SQLite
  - `settings.json` → `app.json`
  - Old projectless layout
  - `openCode2` → `opencode` provider alias
  - Legacy `plan` runtime mode → `Ask`
  - OpenCode 1 cursors resume through the OpenCode 2 import (waits up to 60 s)
- Debug data is isolated in `temp/`.
- **Maturity:** complete.

---

## 3. Settings pages, keybindings/actions, GPUI views

### 3.1 GPUI views and panels (from `src/app/*.rs` and `src/ui/*.rs` file inventory [V]; purposes [F])

`src/app/`:

| View | File | Purpose |
|---|---|---|
| Main app entity `Waku` | `app.rs` (3.1k) | Root window and app state |
| Render root | `render.rs` | |
| Window chrome | `window_chrome.rs` | Title bar and traffic lights |
| Sidebar | `sidebar.rs` (2.6k) | Projects/tasks, grouping, footer update prompt |
| Composer | `composer.rs` (3.9k) | |
| Autocomplete popover | `autocomplete.rs` | |
| Drafts | `drafts.rs` | |
| Transcript | `transcript.rs`, `transcript_view.rs` (3.1k) | |
| Transcript find bar | `transcript_search.rs` | |
| Shared components | `components.rs` (2k) | Activity rows, permission and question cards, etc. |
| Activity diff | `activity_diff.rs` | Inline diffs |
| Right panel | `right_panel.rs` (4.8k) | Files / editor / Terminal / Browser / Review surfaces (`RightPanelSurface`) |
| Image preview | `image_preview.rs` | |
| Command palette | `command_palette.rs` | |
| Task switcher | `task_switcher.rs` | Ctrl+Tab overlay |
| Project picker | `project_picker.rs` | |
| File search | `file_search.rs` | |
| Branches | `branches.rs` | Branch picker |
| Commit dialog | `commit_dialog.rs` | |
| Goal dialog | `goal_dialog.rs` | |
| Background work | `background_work.rs` (2.2k) | Panel plus the "Open in" control |
| Usage meter | `usage_meter.rs` | Context meter popover |
| Usage page | `usage_page.rs` (3k) | |
| Skills page | `skills_page.rs` | |
| Settings | `settings.rs` | |

Non-view logic in `src/app/`: `runtime.rs` (3.8k), `streaming.rs`, `sessions.rs`. Tests are in `tests.rs` (2.2k).

`src/ui/`:
- `mod.rs`: primitives
- `menu.rs`: menus and popovers (1.8k lines)
- `scrollbar.rs`: overlay scrollbars
- `motion.rs`: pulse clock and animation cadence

Other top-level view modules: `src/input.rs` (text field), `src/terminal.rs`, `src/browser.rs`, `src/review_diff.rs`, `src/md/*` (markdown renderer).

### 3.2 Settings pages
From the code paths seen: General (automatic updates, language, analytics), Appearance (theme, UI and code font sizes, math source), Providers, Daemon, Computer Use (debug), Usage, Skills. The exact enum listing was not captured before handback.

### 3.3 Keybindings and actions
Not fully enumerated before handback. To get the full list, grep `actions!(` and `KeyBinding::new` in `src/app.rs`, `src/lib.rs`, `src/input.rs` and `src/app/*.rs`.
- Confirmed: Cmd/Ctrl+F find, Ctrl+Tab / Ctrl+Shift+Tab task switch, Cmd/Ctrl+Enter steer, Enter queue/send, Ctrl+Shift+C/V terminal copy/paste on Windows, and an app-menu "Check for Updates…".
- Text-field bindings per platform are in `docs/text-field-keys.md`.

---

## 4. apps/web and apps/mobile [V — read in depth]

### apps/web — near-complete secondary client
- **Stack:** React 19 + TanStack Start/Router + Vite 8 (React Compiler) + Tailwind v4 + shadcn/Base UI.
  - Deployed as a Cloudflare Worker (`waku-web`); dev port 3001.
  - Libraries: `@pierre/diffs` (diff view and editor), `@wterm/react` + `@wterm/ghostty` wasm (terminal, not xterm), `react-virtuoso`, `react-markdown` + GFM, `@tanstack/charts`.
- **Routes:** `/` (`?session=`) and `/settings/$page`.
- **Main components** (`apps/web/src/components/`):
  - `waku-app.tsx`, `sidebar.tsx`, `composer.tsx` (2.1k lines), `transcript.tsx` (2k lines)
  - `right-panel.tsx`, with surfaces files / file / changes / terminal / backgroundWork
  - `command-palette.tsx`, `commit-dialog.tsx`, `goal-dialog.tsx`, `model-picker.tsx`
  - `daemon-file-picker.tsx` (daemon-host folder browser)
  - `settings-view.tsx`: General, Appearance, Providers, Skills, Usage, Daemon
  - `usage-settings.tsx` and `usage-chart.tsx`
- **Features present:**
  - Tasks: list with search, rename and remove; projects and projectless tasks; worktree and branch pickers.
  - Composer: drag-and-drop attachments, slash commands, `@file` mentions, model / effort / tier / context / preset pickers, access modes.
  - Turn control: steer and queue; permissions; user-input questions; goals; fork from a response and rewind via message edit; per-turn checkpoints.
  - Code and git: review diff over six sources, file editor, commit / push / AI commit message.
  - Terminal over the daemon PTY.
  - Background work, skills, usage history, plan-usage lanes.
  - Resume import, en / zh-CN / ja i18n.
  - Shortcuts: Cmd+K / B / Shift+B / N / O / L / / / U / ,.
- **Gaps vs desktop:**
  - No computer use (hard-coded off).
  - No clipboard image paste.
  - No automatic reconnect (manual Reconnect button only).
  - No PWA or offline support.
  - No embedded browser and no "Open in".
  - Auth is manual URL + token entry, stored in localStorage or sessionStorage.
- **Tests:** about 140 pure-logic `bun test` cases. They are **not run in CI**.

### apps/mobile — functional "remote companion", narrower than web
- **Stack:** Expo SDK 57 with expo-router, React Native 0.86. iOS bundle and Android package are both `sh.waku.mobile`.
  - Native touches: SwiftUI menus and Liquid Glass.
  - Terminal: `expo-libghostty` (patched); clipboard: `@mattermost/react-native-paste-input` (patched).
  - Markdown: a custom mdast streaming renderer.
  - No `eas.json`; the native projects are generated by prebuild.
- **Screens:** onboarding / daemon picker, new task, `session/[id]`, daemons list, daemon editor. Also a task drawer and Terminal / Files / Review sheets.
- **Connection:**
  - Multiple daemon profiles, with tokens in SecureStore.
  - `DaemonLink` reconnects automatically with jittered backoff and a 30 s heartbeat.
  - Pairing is manual entry only (no QR code).
- **Features present:**
  - Streaming transcript (≈8.3 Hz commits) and activity detail sheets.
  - Permissions, user-input questions, steer and queue, goals, `/resume` import.
  - Attachments from files, camera, photos and paste.
  - Slash commands and `@file` completion; synced drafts; context gauge.
  - Read-only file view, review diffs, terminal (iOS/Android).
- **Missing:**
  - Commit, push and branch checkout.
  - Checkpoint capture (no `captureTurn*` calls), fork and rewind.
  - Settings, usage, skills and background-work controls.
  - Message search and i18n (English only).
- **Tests:** about 170 `bun test` cases, not in CI.

### packages/waku-client
- **Contents:** about 95 ts-rs-generated protocol types, plus `WakuClient` (497 lines).
- **Client behavior:**
  - `/v1` WebSocket path; hello handshake with typed connection errors.
  - Per-request UUIDs with timeouts; `notify()` for fire-and-forget calls.
  - Per-session/runtime subscriptions with pre-attach buffering (4096).
  - `{epoch, sequence}` dedup, with replay cursors sent as `resumeFrom` on reconnect.
  - No built-in retry loop.
- **Shared helpers:** event reducer, transcript presentation, composer autocomplete (`/fast`, `/resume`, `/goal`).
- **Tests:** 23 cases. Typecheck and tests run in CI on Linux.

### website/
- Marketing landing page (TanStack Start on Cloudflare) that resolves its download links from the appcast.
- Hosts `public/install.sh`, the Linux installer.

## 5. Code-health notes for a fork [V]

- **License:** GPL-3.0-only (root and crates). A fork must remain GPL-3.0 and ship source. Cua SDK artifacts carry their own `resources/computer-use/CUA-LICENSE`.
- **Upstream:** github.com/egoist/waku (single maintainer, "egoist"). The fork keeps `egoist/waku` as the `upstream` remote; upstream changes are cherry-picked rather than merged once Phase 2 rewrites the core.
  - Upstream moves fast: v0.0.8 → v0.1.20, with multiple provider-protocol changes per release.
  - Hot conflict zones: `src/app/runtime.rs`, `composer.rs`, `right_panel.rs`, the `driver/*.rs` files, `waku-protocol/src/model.rs`, and `protocol.rs` (`PROTOCOL_VERSION` bumps). Keep fork features in new modules where possible.
  - GPUI pin is `egoist/zed` branch `waku-webview`. Upstream controls that fork too.
- **Tests:** about 1,021 Rust `#[test]` attributes.
  - Counts: `src/` 425, waku-core 500 (29 `#[ignore]`d live-provider integration tests), protocol 70, client 21, daemon 3, computer-use 2.
  - TS: 2 client-package tests, 24 web lib tests, 3 mobile tests.
  - No UI or visual automation (by policy, "no visual test unless requested").
- **CI** (`.github/workflows/test.yml`, on every push/PR):
  - `cargo test --locked` on macOS, ubuntu-24.04 and windows (Rust 1.96.0)
  - Cua SDK / REPL smoke test on all three
  - On Linux only: `protocol:check`, `@waku/client` check and test
  - Web (~140) and mobile (~170) bun tests are not in CI. No clippy or fmt gate in CI; fmt is listed in CONTRIBUTING.
- **Dev workflow:** `bun run dev` runs the watcher that rebuilds, signs and relaunches `Waku Debug.app` and hot-swaps the daemon. `AGENTS.md` sets hard rules: no UI-thread I/O, virtualized lists, streaming cadence (≤8.3 Hz commits, ≤60 Hz pulses; see `docs/performance.md`), keyboard accessibility, reduce-motion, and using T3 Code as the product reference and Zed as the GPUI reference.
- **Stale or inconsistent docs to fix in a fork:**
  - The README says remote PTY and folder picker need protocol endpoints, but `openTerminal` and `browseDirectory` already exist; only the desktop is not wired to them.
  - `providers.md` mentions `headless.rs` (absent) and has no DeepSeek section.
  - CONTRIBUTING says the browser is macOS-only, but Windows has WebView2.

### Top parity gaps vs T3 (summary)
1. Native image attachments to providers.
2. Plan / interaction mode plus plan approval (`ExitPlanMode`).
3. Approvals for Pi and Amp (CLI limits) and for Oh My Pi (Waku limit).
4. Rewind for Kimi and Fx.
5. Remote desktop PTY and folder picker wiring.
6. Pairing, scoped auth and TLS for remote access.
7. PR / GitHub integration.
8. MCP server management UI.
9. Scheduled tasks and a persistent background service.
10. Subagent transcript views.
11. A declared adapter capability record (vs `ProviderKind` predicates).
12. Event-sourced orchestration.
