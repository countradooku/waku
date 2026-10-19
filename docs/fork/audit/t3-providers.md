# T3 Code audit: providers and external integrations

Scope: what T3 Code (TS/Effect, Electron + Node server) implements for agent providers, source-control hosts, remote access infrastructure and native helpers, written for a Rust/GPUI fork of Waku that wants full parity.

Repo root (read-only): `.../scratch-2026-10-19-f2254f/t3code`. All paths below are relative to it.

Complexity scale (Rust reimplementation, one experienced engineer, parity-quality including edge cases and replay tests):
**S** = days, **M** = 1–3 weeks, **L** = 1–2 months, **XL** = 2+ months.

Waku baseline, for context: `crates/waku-core` already has drivers for Codex (`app-server --stdio`), Claude (raw `stream-json` + `control_request`), generic ACP, OpenCode, Pi, Amp, Kimi, Grok, Cursor and DeepSeek. Waku has **no** source-control host integration, SSH, Tailscale, relay/tunnel, or ACP-registry code (grep for `api.github.com`, `glab`, `bitbucket`, `tailscale`, `"ssh"`, `cloudflared`, `agentclientprotocol` found nothing).

---

## 0. Inventory

| Driver kind | Where it lives | Transport | Adapter LOC (approx.) |
|---|---|---|---|
| `codex` | `apps/server/src/provider/Drivers/CodexDriver.ts`, `apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.ts`, `apps/server/src/provider/Codex*.ts`, protocol in `packages/effect-codex-app-server` | `codex app-server` JSON-RPC over stdio (generated from the upstream `codex-rs/app-server-protocol` JSON schemas) | 7.4k adapter + ~5k provider support |
| `claudeAgent` | `apps/server/src/provider/Drivers/ClaudeDriver.ts`, `.../Adapters/ClaudeAdapterV2.ts`, `apps/server/src/provider/Claude*.ts` | `@anthropic-ai/claude-agent-sdk` `query()`; the SDK spawns the `claude` CLI and speaks stream-json/control protocol | 9.9k adapter |
| `cursor` | `packages/provider-cursor` | `@cursor/sdk` 1.0.35, local-agent runtime (in-process SDK, **not** Cursor ACP) | 2.7k |
| `grok` | `packages/provider-grok` | ACP (stdio) via `grok agent stdio`, plus xAI extensions (`x.ai/*`, `_x.ai/*`) | 0.4k flavor + 1.6k xAI extension |
| `opencode` | `packages/provider-opencode` (1.x `src/server/adapter.ts`, 2.x `src/server/v2/*`) | HTTP+SSE to `opencode serve` (1.x through `@opencode-ai/sdk`, 2.x through `@opencode/client` 2.0.28) | 3.7k (1.x) + 4.0k (2.x) |
| `antigravity` | `apps/server/src/provider/Drivers/AntigravityDriver.ts`, `.../Adapters/AntigravityAdapterV2.ts`, `apps/server/src/provider/Antigravity*.ts`, `apps/server/src/provider/acp/Antigravity*.ts` | ACP (Google `antigravity-acp` binary) as a flavor of the shared ACP adapter | ~5k across files |
| `pi` | `packages/provider-pi` (reference package layout) | `pi --mode rpc` JSONL commands over stdio | 3.3k |
| `muse` | `packages/provider-muse` | `@muse-code/sdk` 1.4.6, `spawnMspConnection` (the Muse "MSP" JSON-RPC over stdio) | 2.3k |
| `acpRegistry` (generic ACP) | `packages/provider-acp-registry` + `packages/provider-acp` (shared adapter) + `packages/effect-acp` (protocol) | ACP v2 preview, with v1 negotiation fallback | 7.6k shared ACP adapter + 2.9k session runtime + 2k registry support |

Registration: `apps/server/src/provider/builtInDrivers.ts` (order: Codex, Claude, Cursor, Grok, OpenCode, Antigravity, Pi, Muse, AcpRegistry) and `apps/server/src/orchestration-v2/builtInProviderAdapterDrivers.ts`.

There are no Amp, Kimi, Fx, or DeepSeek drivers in T3. Those reach T3 only as ACP Registry entries or "Local ACP command" instances (the docs give DeepSeek Harness as an example: `dsh --profile acp`). Copilot and Devin are ACP Registry entries with small, opt-in shims in `provider-acp-registry` (`copilotAcp.ts`, `devinAcp.ts`).

Doc inconsistency to know about: `docs/user/providers-acp.md` says Antigravity "does not need an Antigravity-specific T3 Code provider". The code still ships a dedicated `AntigravityDriver` with a managed installer, Google auth, and usage limits, and `providers-antigravity.md` documents that driver.

---

## 1. Per-provider detail

### Capability matrix (from each adapter's `*ProviderCapabilitiesV2` constant)

Key: Y = yes, N = no, ~ = partial or negotiated at runtime.

| Capability | Codex | Claude | Cursor | Grok | OpenCode 1.x | OpenCode 2.x | Antigravity | Pi | Muse | Generic ACP |
|---|---|---|---|---|---|---|---|---|---|---|
| Resume native thread (required primitive) | Y | Y | Y (`Agent.resume`) | Y (`session/load`) | Y | Y | Y (`session/resume` preferred over `load`) | Y (session files) | Y (`session/resume`) | ~ if agent has `loadSession` |
| Multiple provider threads per session | Y | N | N | N | N | Y | N | N | N | N |
| Model switch in session | Y | Y | Y | Y | Y | Y | Y | Y | Y | ~ if a `model` config option exists |
| Runtime-mode switch in session | Y | N | N | N | N | Y | Y | N (restarts process, resumes) | N | N |
| Read thread snapshot | Y | N | Y (`Agent.messages.list`) | Y | Y | Y | N | Y | Y | ~ `loadSession` |
| Native rollback | Y (`thread/revert`) | Y (`resumeSessionAt`) | N | Y\* | Y | Y | **N** (revert is blocked) | Y | N | Y\* |
| Native fork | Y (incl. from turn and from subagent) | Y (`forkSession`, from turn) | N (portable handoff) | N | Y (incl. subagent) | Y | N | Y (`pi --fork` CLI in the destination cwd) | N | ~ if `session.fork` is advertised |
| Interrupt | Y | Y | Y | Y (restarts runtime after interrupt) | Y | Y | Y | Y (`abort`) | Y | Y |
| Active steering | Y (`turn/steer`) | Y (stream input; interrupts pending tools) | N, interrupt+restart | N, interrupt+restart | Y | Y | N, interrupt+restart | Y (`prompt` with `streamingBehavior: "steer"`) | Y (`turn/steer`) | N, interrupt+restart |
| Queued messages | Y | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| Approvals: command / file read / file change / apply_patch | Y/Y/Y/Y | Y/Y/Y/N | N (sandbox and Auto-review only) | Y/Y/Y/N | Y/Y/Y/Y | Y/Y/Y/Y | Y/Y/Y/N | Y/N/Y/N (via Pi tool hook) | Y/Y/Y/N | Y/Y/Y/N |
| Approvals from subagents | Y | N | N | N | Y | Y | N | N | Y | N |
| Structured questions (AskUserQuestion-like) | Y (async questions answered by a new message) | Y (`AskUserQuestion`) | N | Y (`x.ai/ask_user_question`) | Y | Y | Y (permission-question mapping) | Y (extension dialogs) | Y | Y (elicitation) |
| Plan mode toggle (`showInteractionModeToggle`) | Y (collaboration mode) | Y (`permissionMode: "plan"`, `ExitPlanMode`) | Y | N (`x.ai/exit_plan_mode` is still mapped) | Y (`plan` agent) on 2.x; N on 1.x | Y | N (native `/plan` only) | N | N | Y if the agent has plan/build modes |
| Proposed-plan card | Y | Y | Y | ~ | N | N | ~ | N | N | N |
| Todo list / plan updates | Y | Y | Y | Y | Y | N | Y | N | Y | Y |
| Subagents | Y (thread ids, wait, close, fork) | Y (lifecycle only; `stopSubagent`, `getSubagentMessages`) | Y (`task`, read-only child threads) | Y (`x.ai/task_*`, background tasks) | Y (with ids) | Y (with ids) | Y (batched, not controllable) | Y (T3 `delegate_task` plus Pi's `subagent` extension) | Y (lifecycle) | N (no portable lineage contract) |
| Images in prompt | Y | Y | Y | Y (forced on, because Grok misreports `promptCapabilities.image`) | path-based | ~ | Y; other files passed as paths | Y | Y | Y if negotiated |
| MCP (T3 thread-scoped server injected) | Y | Y | Y | Y | Y | Y (per-thread `t3-code-<thread>` entry with deny rules for other threads' entries) | Y | Y (generated Pi extension bridge) | Y | Y (stdio `t3 acp-mcp-bridge`) |
| MCP Apps (interactive tool UIs: `mcpApps.listTools/callTool/readResource`) | Y | N | N | N | N | N | N | N | N | N |
| Compaction (`compactThread`) | Y (`thread/compact/start`) | Y (`/compact`, auto-compact threshold setting) | Y | Y | Y | Y | Y | Y (`compact`) | Y (`session/compact`) | Y |
| Token / context usage | Y (`thread/tokenUsage/updated`) | Y | Y | Y | Y | Y | Y | Y (`get_session_stats`) | Y (`session/tokenUsage`, `session/contextUsage`) | Y if the agent reports usage |
| Rate limits / usage limits | Y (`account/rateLimits/*`, reset-credit consume) | Y (`rate_limit_event`, reset credits) | Y (`api2.cursor.sh`, dashboard usage events) | Y (`cli-chat-proxy.grok.com/v1/billing`) | Y | Y | Y (Gemini plus shared Claude/GPT pools, 5h and weekly) | session-log usage | Y | N |
| Historical usage reader (cost dashboard) | Y (`~/.codex/sessions`) | Y (`~/.claude/projects` JSONL) | Y | Y | Y | Y | Y | Y (Pi session files) | N | N |
| Model list | `model/list` from app-server, plus manifest `minVersion` gating | manifest-driven (`model-manifest.json`) | SDK catalog | ACP config options | server inventory | server inventory | account-scoped ACP catalog | `get_available_models` | `model/list` | ACP `configOptions` plus custom models |
| Reasoning effort | Y | Y (effort plus thinking; prompt effort prefix) | Y (SDK model params) | Y (ACP config option) | Y (variants) | Y | Y | Y (`set_thinking_level`) | Y | Y (config option) |
| Skills (`$` menu) | `skills/list` | `~/.claude/skills` plus `.claude/skills` | Y | Y | Y | Y | `.agents/.gemini/.agent` skill dirs | Y | N | `$`-prefixed agent commands |
| Text generation (titles, commit messages, PR bodies, review guides) | Y | Y | Y | Y | Y | Y | Y (with a hooks/MCP safety gate) | Y | Y | **N** |
| Runtime policy enforcement | native | native | native | native | native | native | native | client-boundary | native | client-boundary |
| Supported permission modes | all 4 | all 4 | all 4 | Supervised, Auto, Full (no Auto-accept edits) | all 4 (Auto = Supervised) | all 4 | per ACP mode | Supervised, Auto-accept edits, Full | Supervised, Full | answered by T3 policy |
| Feedback upload | Y (`feedback/upload`) | N | N | N | N | N | N | N | N | N |
| Native `/goal` | Y (`thread/goal/*`, one run owns several native turns) | Y (read from synthetic command output and Stop-hook feedback) | N | N | N | N | N | N | N | N |

\* Grok and generic ACP report `canRollbackThread: true`, but per `providers-acp.md` the provider conversation cannot rewind. After a checkpoint restore, the next turn starts a fresh ACP session. Treat this as "app-level rollback with a fresh session".

### 1.1 Codex — complexity **XL** (Waku has a base driver; parity is still L–XL)

- **Transport.** Spawns `codex app-server`; launch args are filtered by `apps/server/src/provider/codexLaunchArgs.ts`. Speaks JSON-RPC over stdio. Types are generated by `packages/effect-codex-app-server/scripts/generate.ts`, which pulls JSON schemas from GitHub `openai/codex/codex-rs/app-server-protocol` at a pinned ref. The generated schema is 57k lines; `meta.gen.ts` lists 100+ client methods.
- **Methods T3 uses.** `initialize`, `thread/start|resume|fork|read|revert|unsubscribe|unarchive|turns/list|inject_items|compact/start|goal/set|get|clear|backgroundTerminals/list|terminate`, `turn/start|steer|interrupt`, `skills/list`, `model/list`, `account/read`, `account/rateLimits/read`, `account/rateLimitResetCredit/consume`, `mcpServerStatus/list`, `mcpServer/tool/call`, `mcpServer/resource/read`, `feedback/upload`.
- **Notifications T3 handles.** `turn/started|completed|plan/updated|diff/updated` (T3 opts out of diffs and filters them), `item/started|completed|agentMessage/delta|reasoning/textDelta|summaryTextDelta|plan/delta|commandExecution/outputDelta`, `thread/tokenUsage/updated`, `thread/settings/updated`, `thread/goal/updated|cleared`, `model/rerouted`, `account/rateLimits/updated`, `account/updated`, `mcpServer/startupStatus/updated`.
- **Server→client requests.** `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval`, `item/tool/requestUserInput`, `mcpServer/elicitation/request` (see `CodexMcpElicitation.ts`).
- **Quirks.**
  - Async questions arrive as notifications and are answered with a new user message, not an RPC response. They are persisted as `user_input_request` turn items and survive restarts.
  - Goal turns are self-started, so one run owns several native turns.
  - Metadata-only reads on resume.
  - Rollback turn-count math excludes `/goal` control turns (`CodexThreadRevert.ts`).
  - Developer instructions are injected (`CodexDeveloperInstructions.ts`). Tool presentation lives in `CodexToolPresentation.ts`.
- **Auth.**
  - (a) The existing CLI login in `CODEX_HOME`.
  - (b) **T3-managed ChatGPT sign-in** (`CodexChatGptAuth.ts`, 984 lines): OIDC discovery at `https://auth.openai.com`, dynamic client registration stored in `ProviderCredentialStore`, PKCE with a loopback `127.0.0.1` listener, and a manual callback-URL paste fallback for remotes. Tokens are injected into a T3-owned Codex home (`CodexManagedRuntime.ts`, `CodexManagedHome.ts`, `CodexChatGptSessionLock.ts`).
  - (c) **Primary handoff** for remote environments (`CodexChatGptHandoff.ts`). A local primary completes OAuth, verifies it, then transfers the client registration and tokens to the remote. Only the destination persists and refreshes them, to avoid racing refresh-token rotation.
- **Multi-account.** "Shadow homes" (`Drivers/CodexHomeLayout.ts`): two instances share one `CODEX_HOME` for sessions and config but each has its own `auth.json`, so threads can switch accounts. Runtime lock files are symlinked to shared state.
- **Install and update.** Managed installer (`CodexInstallation.ts`, 767 lines) with checksummed download, staging, and Windows tar/Defender quirks. `providerMaintenance` and `maintenanceResolver` decide whether brew, npm, pnpm, bun, volta, or `codex update` owns the binary.
- **Rust notes.** Waku already speaks app-server. The parity gap is goals, async questions, MCP elicitation, MCP apps, inject_items, background terminals, feedback, the managed ChatGPT OAuth and handoff, shadow homes, the managed installer, rate-limit reset credits, and min-version model gating. Consider generating Rust types from the same upstream JSON schemas (typify/schemars): **M** for codegen alone.

### 1.2 Claude — complexity **XL**

- **Transport.** `@anthropic-ai/claude-agent-sdk` `query()` with a custom spawn of the resolved `claude` binary (`Drivers/ClaudeExecutable.ts`). The SDK speaks the stream-json control protocol to the CLI. Rust has no SDK, so you must implement the control protocol directly; Waku already does a subset (`--input-format stream-json --permission-prompt-tool stdio`, `control_request` interrupt).
- **SDK features used.**
  - `canUseTool` (approvals; `PermissionUpdate` for "always allow").
  - `setPermissionMode` and `setModel` mid-session.
  - `interrupt()`.
  - Streaming input for steering. Steering interrupts pending tools, so `activeSteeringInterruptsTools: true`.
  - `resume` plus `resumeSessionAt` for rollback; `forkSession()` for fork.
  - `getSubagentMessages`; `stopTask` for background tasks and subagents.
  - `includePartialMessages`, `settingSources`, hooks (Stop hook), `AskUserQuestion` and `ExitPlanMode` tool interception.
  - Effort and thinking settings (`claudeModelOptions.ts`, `applyClaudePromptEffortPrefix`).
  - `SDKRateLimitInfo` / `rate_limit_event`, `compact_boundary`, `SDKAPIRetryMessage`.
  - Messages `task_started`, `task_progress`, `task_notification` for background tasks, subagents and workflows (`claudeWorkflowProgress.ts`, `ClaudeTaskOutputTail.ts` tails task output files).
- **Other pieces.**
  - **History worker** (`claude-history-worker.ts`, `claudeHistoryWorker.ts`): reads transcripts off-thread.
  - **Model catalog** comes entirely from `model-manifest.json` (13 Claude models, each with an `adapter.claudeCode.minVersion`), via `ClaudeModelCatalog.ts` and `ClaudeModelManifest.ts`.
  - **Reset credits** (`claudeResetCredits.ts`) use an idempotent request id.
  - **Usage limits** (`claudeUsageLimits.ts`) cover the five_hour and seven_day windows.
  - **Usage history** (`Drivers/claudeUsage.ts`) parses `~/.claude/projects` JSONL.
  - **Skills discovery and dispatch** (`ClaudeSkills.ts`, `ClaudeSkillDispatch.ts`): only the last-named `disable-model-invocation` skill is run directly.
  - **Compaction.** An auto-compact threshold setting (100k–1M) plus a "Compact chip" when the prompt cache expired (1h on a subscription, 5m on API keys); see `@t3tools/shared/claudeCompaction`.
- **Auth and accounts.** No in-app login; it uses `claude auth login`. Multi-instance works through `CLAUDE_CONFIG_DIR` per instance (`Drivers/ClaudeHome.ts`). Instance env vars support OpenRouter and other routers (`ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`, an empty `ANTHROPIC_API_KEY`). Custom models can be added.
- **Quirks.**
  - No native turn ids (`nativeTurnIds: weak`); snapshot reads are unsupported.
  - Subagents do not expose thread ids.
  - Background tasks and subagents can wake the parent after the turn settled (many `claude_background_*` replay fixtures).
  - Usage-limit turns stay "working" until the window reopens.

### 1.3 Cursor — complexity **L**

- **Transport.** The official `@cursor/sdk` TypeScript package in local-agent mode (`CursorAgentSdk.ts`, `CursorSdk.ts`, `sdkModel.ts`). It does **not** use Cursor's ACP for V2. Node ≥ 22.13 is required. There is no Rust SDK: you would need to drive the `cursor-agent` CLI/ACP instead (Waku's `cursor_session.rs` is 121 lines) or embed Node.
- **Capabilities.** Create and resume agents; change the model between turns; streaming of text, reasoning, tools, plans and todos; `task` subagents projected as read-only children. `Agent.resume(agentId)` fails for subagents, so children get no thread ids. Images work; T3 MCP is supported.
- **Missing.** Native fork, rollback, active steering and approvals. Full access disables the sandbox; restricted modes enable it; Auto-review is driven by the approval policy.
- **Auth.** Browser sign-in that mints an API key (`auth.ts`), stored with `@napi-rs/keyring` (`CursorKeychain.ts`, `credentialStore.ts`). `CURSOR_API_KEY` overrides it.
- **Usage.** Account usage from `https://cursor.com/api/dashboard/get-filtered-usage-events` and limits from `api2.cursor.sh` (`accountUsage.ts`, `usageLimits.ts`). Model catalog in `CursorSdkCatalog.ts`.

### 1.4 Grok — complexity **M** (as an ACP flavor, after generic ACP exists)

- **Transport.** ACP over stdio. `grok agent stdio` is launched with `--permission-mode default|auto` or `--always-approve` (`acpSupport.ts`), and an OAuth referrer env var is set.
- **xAI extensions** (`xaiAcpExtension.ts`, 1.6k lines): `x.ai/session/prompt_complete`, `x.ai/session_notification`, `_x.ai/session/update`, `x.ai/task_backgrounded`, `x.ai/task_completed`, `x.ai/ask_user_question`, `x.ai/exit_plan_mode`. Rate-limit error code `-32003`. Background tasks are handled in `xaiBackgroundTasks.ts`.
- **Quirks.**
  - Grok's Auto mode forwards only classifier-blocked commands, and those must reach the user (`permissionDisposition`).
  - Wedged prompts need the prompt fiber interrupted before `session/cancel`, and the runtime restarted after an interrupt because `task_already_running` sticks.
  - Image prompts are forced on despite the capability report.
  - There is no session-wide command approval, because Grok would remember it for the whole project.
- **Status probes** avoid authentication and session creation (`status.ts`). Auth is `grok login` or `--device-auth`. Usage limits come from `https://cli-chat-proxy.grok.com/v1/billing?format=credits` using credentials read from Grok's auth file.

### 1.5 OpenCode — complexity **L** (two protocol generations)

- **Transport.** HTTP REST plus an SSE event stream against `opencode serve --hostname= --port=` (`OpenCodeRuntime.ts`), or against an external server URL with a password.
  - **1.x** (`@opencode-ai/sdk/v2`): one T3-managed server per thread, plus a shared idle-closing helper for catalog and text generation (`OpenCodeServerOwner.ts`, `OpenCodeServerLedger.ts`).
  - **2.x** (`@opencode/client` 2.0.28): one server per instance (`v2/OpenCode2Server.ts`).
- **Version probe.** `versionProbe.ts` picks the runtime. 1.x must be ≥ 1.14.19 and shows "Limited support"; 2.x must be ≥ 2.0.18.
- **Quirks.**
  - MCP registrations are directory-scoped. On 2.x, each thread registers `t3-code-<thread>` and permission rules deny every other thread's entry.
  - "Always" grants are project-wide, so automatic replies use `once`. A session-wide approval on 2.x becomes T3's own rule.
  - `.env*` files need approval in restricted modes.
  - Plan mode is the `plan` agent. Auto behaves like Supervised.
- **Usage.** OpenCode keeps a local DB; `usage.ts` and `usageLimits.ts` read it (hashes, file reads).

### 1.6 Antigravity (Google) — complexity **L** (ACP flavor plus installer, auth and isolation)

- **Transport.** ACP via the `antigravity-acp` executable together with its `localharness_external` helper (PyInstaller). Flavor code is in `AntigravityAdapterV2.ts` and `provider/acp/AntigravityAcpSupport.ts|Protocol.ts|ClientFiles.ts|SessionFiles.ts`. It implements the ACP client `fs/read_text_file` and `fs/write_text_file`. `preferResumeSession` is set because `session/load` replays slowly. Subagents arrive batched.
- **Managed installer** (`AntigravityInstallation.ts`, 876 lines). Downloads from `dl.google.com` per the ACP registry. Releases are immutable and an atomic pointer selects the version for new processes. Running processes hold leases, and updates or removal respect them. Includes disk-space checks and Windows DLL-lock handling. Supports Apple Silicon macOS, Linux x64/arm64, and Windows x64/arm64.
- **Auth** (`AntigravityAuth.ts`, `antigravityAuthSupport.ts`, `antigravityCallback.ts`). Four methods: Google account browser sign-in, Gemini Enterprise (with GCP project and location), Gemini API key, or Vertex (API key or ADC).
  - Browser sign-in uses a 127.0.0.1 loopback. Remote clients paste the return URL, which is forwarded only for the owning pending flow.
  - Each instance gets a separate profile with file-based credential storage (no keychain). Ambient Google credentials are stripped. User skill dirs are symlinked back to the real `~/.gemini`.
  - Sign-out closes admission to new processes, stops sessions, then clears credentials.
  - Text-generation helpers deny tools and reject profiles that have hooks or MCP configured (`textGeneration/AntigravityTextGeneration.ts`).
- **Limits.** `antigravityUsageLimits.ts` and `Drivers/antigravityUsageReader.ts` report the Gemini and shared Claude/GPT pools, each with 5h and weekly windows.
- **Missing.** Conversation rewind (revert is blocked before touching files) and the T3 plan toggle.

### 1.7 Pi — complexity **M** (Waku already has a 2.4k-line Pi driver)

- **Transport.** `pi --mode rpc` with JSONL commands over stdio (`rpc.ts`). T3 rejects user launch args that change mode or session (`mcpInjection.ts`).
- **Commands.** `prompt` (with `streamingBehavior: "steer"` for atomic steering), `abort`, `get_state`, `get_entries`, `get_messages`, `get_available_models`, `set_model`, `set_thinking_level`, `compact`, `new_session`, `switch_session`, `fork`, `set_session_name`, `get_session_stats`, `extension_ui_response`.
- **MCP.** T3 generates a Pi extension (`mcpExtensionSource.ts`) that bridges the T3 MCP server into Pi tools.
- **Forks.** Done with the `pi --fork` CLI in the destination directory, because RPC session switching keeps the source cwd.
- **Permissions.** Enforced through Pi's blocking tool hook (policy is client-boundary, not native).
- **Extension UI.** `select`, `confirm`, `input` and `editor` dialogs are mapped to T3 question panels.
- **Usage.** Read from Pi session files (`usage.ts`). Thinking levels are discovered per model (`thinkingCapabilities.ts`).

### 1.8 Muse Code (Meta) — complexity **M**, but blocked on the proprietary MSP protocol

- **Transport.** `@muse-code/sdk` `spawnMspConnection` (JSON-RPC over stdio).
- **Methods.** `session/start|resume|read|compact|setModel|setApprovalMode`, `turn/start|steer|interrupt`, `approval/decide`, `model/list`.
- **Notifications.** `turn/started|completed|retryScheduled`, `item/started|updated|completed|delta`, `approval/requested|resolved|updated`, `session/todoListChanged|tokenUsage|contextUsage`, `usage/changed`, `view/gap`.
- **Behavior.** `META_API_KEY` is stripped from the inherited environment. Only Supervised and Full access are offered. There is no in-app login (`muse login`) and no fork or rewind. The status probe does not check auth. Disabled by default (beta).
- **Rust notes.** You must reverse-engineer MSP from the SDK or from replay fixtures (`apps/server/scripts/record-muse-msp-replay-fixture.ts`).

### 1.9 Generic ACP / ACP Registry — complexity **XL** for parity (Waku has a 2.8k-line generic ACP driver)

- **Protocol package.** `packages/effect-acp`, generated from `agentclientprotocol/agent-client-protocol` release schemas: `schema.gen.ts` (v2 preview, 10k lines) and `schema-v1.gen.ts` (9.4k lines). `compat.ts` normalizes v1 and v2. The client offers `protocolVersion: 2` and falls back to v1.
- **ACP methods used.**
  - Client→agent: `initialize`, `authenticate`, `session/new|load|resume|prompt|cancel|set_mode|set_model|set_config_option|list|fork|delete|close`.
  - Agent→client: `session/request_permission`, `terminal/create|output|wait_for_exit|kill|release` (`clientTerminals.ts`; only Devin runs commands through T3 terminals), `fs/*`, and a `_session/retrying` extension.
  - Session updates include `session_info_update` and `terminal_output_chunk`.
- **Shared adapter** (`packages/provider-acp/src/server/adapter.ts`, 7.6k lines). `makeAcpAdapterV2(flavor)` takes a flavor with hooks: `normalizeSessionUpdate`, `applyModelSelection`, `sessionModeForPolicy`, `clientFileSystem`, `extractPermissionQuestion`, `permissionDisposition`, `approvalOptions`, `preferResumeSession`, `registerExtensions`, `extractSubagentUpdate`, `normalizeToolCall`, `extractProposedPlanMarkdown`, background-task extractors, `interruptPromptOnCancel`, `restartRuntimeAfterInterrupt`, `supportsImagePrompts`, `supportsCompaction`, `onUrlElicitation`, and others.
  - `negotiatedCapabilities()` upgrades the base capabilities from the agent's `initialize` result (`loadSession`, `sessionCapabilities.fork`, presence of a `model` config option).
  - MCP is always provided as a stdio bridge (`t3 acp-mcp-bridge`, `apps/server/src/mcp/AcpMcpStdioBridge.ts`) because agents drop injected HTTP MCP servers. It is also offered as an `acp`-type MCP server (`mcpOverAcpBridge.ts`).
- **Registry** (`packages/provider-acp-registry`).
  - Fetches `https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json` and caches it. Filters agents by host platform.
  - Installs under `<T3_HOME>/tools/<agent-id>/<version>/` with SHA-256 verification when provided (`@noble/hashes`). Supports binary archives, plus `npx` and `uvx` distributions in T3-owned npm prefixes and uv tool dirs (`AcpRegistrySupport.ts`, about 2k lines).
  - Also supports an executable override, a custom "Local ACP command" (literal argv, no shell), and custom models.
- **Auth** (`auth.ts`, `authenticationState.ts`). ACP `authMethods`: browser URL methods (show the URL, wait, then `authenticate`) or terminal methods (run in an in-app terminal on the environment, then reconnect). Optional logout.
- **Native session management** (`acpSessionManagement`): `session/list` import into deterministic T3 threads, delete, `listProviders`, `setProvider`, `disableProvider`.
- **Permissions.** T3 answers approval requests by thread mode (Supervised shows them; Auto-accept edits approves edits; Auto and Full approve everything). File reads never wait.
- **Missing.** Subagents and text generation. Rollback means a fresh session.

### 1.10 Shared provider infrastructure (all providers) — complexity **XL** in total

| Piece | Path | What | Rust complexity |
|---|---|---|---|
| Provider instance registry | `apps/server/src/provider/ProviderInstanceRegistry*.ts`, `ProviderRegistry.ts` (970 lines) | Many instances per driver, each with isolated state. Shadow "unavailable" snapshots on failure. Hydration. | L |
| Provider host port | `packages/provider-core/src/server/ProviderHost.ts` | Paths, settings, background demand, attachments, stored credentials | M |
| Credential store | `apps/server/src/provider/ProviderCredentialStore.ts`, `apps/server/src/secrets` | Per-instance encrypted secrets | M |
| Auth flow framework | `packages/provider-core/src/server/providerAuthFlow.ts`, `auth.ts`, `ProviderAuthService.ts` | `start/complete/cancel`, owner session, callback mode `server` or `client` (remote paste) | M |
| Status and snapshot probes | `snapshotProbe.ts`, `providerStatusCache.ts` | No side-effecting probes; per-cwd snapshots (skills, commands) | M |
| Maintenance and updates | `packages/provider-core/src/server/maintenanceResolver.ts` (930 lines), `apps/server/src/provider/providerMaintenance*.ts` | Prove which installer owns the binary (brew keg or cask, npm prefix, pnpm, bun, yarn, Vite+, Volta); mise is manual; otherwise the provider's own updater; version re-check afterwards | L |
| Model manifest | `apps/server/src/provider/model-manifest.json` (826 lines), `ModelManifest.ts`, `packages/provider-core/src/server/ModelCatalog.ts` | Bundled, then remote fetch from `main`; newest `updatedAt` wins; `minVersion` produces `updateRequiredModels`; capability profiles; `currentModels` overlays | M |
| Compatibility policy | `providerCompatibility.ts` | Minimum versions | S |
| Usage aggregation | `packages/provider-core/src/server/usage.ts`, `usageLimits.ts`, `ProviderUsageLimitsIngestion.ts` | `UsageRecord` (tokens, cost, speed), deduplication, limit windows, reset credits (`resetCreditCoordinator.ts`) | M |
| Native protocol logging | `EventNdjsonLogger.ts`, `nativeProtocolLogging.ts`, `ProviderEventLoggers.ts` | 64 KiB budget, structural summaries, token deltas filtered | M |
| Continuations | `ProviderContinuationRequests.ts` | Wake the parent when background work finishes after settle | M |
| Selection transitions | `selectionTransition.ts` | Model, instance and account switching plans | M |
| Text generation | `textGeneration*.ts` | Titles, branch names, commit messages, PR bodies | M |
| Attachments | `attachmentPrompt.ts`, `apps/server/src/orchestration-v2/AttachmentClaims.ts` | Native format per provider; never copied into the workspace | S |
| Instance env isolation | `instanceEnvironment.ts`, `processGroup.ts` | Strip ambient credentials; kill process groups | S |
| Replay testkit | `packages/provider-testing`, `apps/server/scripts/record-*-replay-fixture.ts` (Claude, Codex, Cursor, Grok, Muse, Pi), `apps/server/src/orchestration-v2/testkit/fixtures/*` | Recorded transcripts replayed through the real orchestrator | L (worth doing early) |

---

## 2. Adapter interface shape

Source: `packages/provider-core/src/server/ProviderAdapter.ts` (661 lines), `driver.ts`, `adapterDriver.ts`, and `packages/contracts/src/orchestrationV2.ts` lines 193–330. Design doc: `docs/orchestration-v2/provider-capability-system.md`. Rules: `docs/internals/adding-a-provider.md`.

### Two layers

1. **`ProviderDriver<Config>`** (per driver kind):
   - `driverKind`, `metadata { displayName, supportsMultipleInstances, hasDefaultInstance }`.
   - `configSchema`, `defaultConfig()`.
   - `create(input) -> ProviderInstance`.
   - Optional `usage` reader.

   **`ProviderInstance`** (one per configured instance):
   - Identity: `instanceId`, `driverKind`, `continuationIdentity { driverKind, continuationKey }` (decides which instances can continue each other's threads, e.g. same `CODEX_HOME`).
   - `snapshot` (a managed status, models and auth snapshot), `snapshotForCwd(cwd)`.
   - Optional `refreshModels`, `invalidateCaches`, `consumeResetCredit`.
   - `orchestrationAdapter`, `textGeneration`, optional `auth: ProviderAuthController`.
   - Optional `acpSessionManagement { listSessions, logout, deleteSession, listProviders, setProvider, disableProvider }`.
2. **`ProviderAdapterV2`** (service):
   - `getCapabilities()`, `planSelectionTransition(input)`.
   - `openSession(input) -> ProviderAdapterV2SessionRuntime` (scoped).

### `ProviderAdapterV2SessionRuntime` (what a session must implement)

Required:
- `events: Stream<ProviderAdapterV2Event>`
- `ensureThread`, `resumeThread`
- `startTurn(ProviderAdapterV2TurnInput)`, which carries app thread, run id and ordinal, attempt id, root node id, provider thread, message (text plus attachments plus creation metadata), model selection, runtime policy, and optional app context.
- `steerTurn` (may fail with `SteerRunUnsupported`), `interruptTurn` (optionally requesting a runtime restart)
- `respondToRuntimeRequest({ requestId, decision?, answers?, response? })`
- `readThreadSnapshot`, `rollbackThread({ target: thread_start | provider_turn })`, `forkThread({ source, turnId?, targetThreadId, ... })`

Optional:
- `subscribeEvents` (multi-consumer)
- `hasPendingBackgroundWork` / `hasPendingBackgroundWorkForThread`, `backgroundTasksEndWithProcess`
- `getModelContextWindow`, `canReuseContextUsage`
- `injectHistory` (handoff context), `compactThread`
- `stopTask`, `stopSubagent` (returning an outcome)
- `unloadThread`, `uploadFeedback`, `mcpApps { listTools, callTool, readResource }`

### Events (normalized)

`app_thread.created`, `provider_session.updated`, `provider_thread.updated` (with `retainedNativeTurnIds`), `provider_turn.updated`, `node.updated` (execution tree), `subagent.updated`, `message.updated`, `turn_item.updated`, `command_output.delta` (ephemeral, never persisted), `runtime_request.updated` (approvals and questions; `responseCapability` is `rpc` or `message`), `plan.updated`, and `turn.terminal`.

`turn.terminal` has two shapes:
- `completed`, `interrupted` or `cancelled`, with `threadDisposition: reusable | broken`.
- `failed`, with a typed `failure`, an optional `retry` and `retryStartedAt`, and `childTurnsContinue`.

Failures are classified, including `usage_limit` with a reset time.

### Errors

Typed tagged errors: `Capabilities`, `OpenSession`, `CloseSession`, `ResumeThread`, `EnsureThread`, `ReadThreadSnapshot`, `RollbackThread`, `ForkThread`, `TurnStart`, `SteerRunUnsupported`, `SteerRun`, `Interrupt`, `RuntimeRequestResponse`, `EventStream`, and `Protocol` (fixed reasons `previous_turn_active` and `attachment_unreadable`).

### Capabilities (`OrchestrationV2ProviderCapabilities`)

| Group | Flags |
|---|---|
| `sessions` | `supportsMultipleProviderThreadsPerSession`, `supportsModelSwitchInSession`, `supportsProviderSwitchingViaHandoff`, `supportsRuntimeModeSwitchInSession`, `pendingRequestsSurviveRestart` |
| `threads` | `canCreateEmptyThread`, `canReadThreadSnapshot`, `canRollbackThread`, `canForkThread`, `canForkFromTurn`, `canForkFromSubagentThread`, `exposesNativeThreadId` |
| `turns` | `exposesNativeTurnId`, `emitsTurnStarted`, `emitsTurnCompleted`, `supportsInterrupt`, `supportsActiveSteering`, `activeSteeringInterruptsTools?`, `supportsSteeringByInterruptRestart`, `supportsQueuedMessages`, `terminalStatusQuality` (strong, weak or none) |
| `streaming` | `streamsAssistantText`, `streamsReasoning`, `streamsToolOutput`, `streamsPlanText`, `emitsMessageCompleted` |
| `tools` | `exposesToolItemIds`, `emitsToolStarted`, `emitsToolCompleted`, `emitsToolOutput`, `supportsMcpTools`, `supportsDynamicToolCallbacks` |
| `approvals` | `supportsCommandApproval`, `supportsFileReadApproval`, `supportsFileChangeApproval`, `supportsApplyPatchApproval`, `approvalsHaveNativeRequestIds`, `approvalCallbacksAreLiveOnly`, `approvalsCanOriginateFromSubagents` |
| `planning` | `emitsPlanUpdated`, `emitsTodoList`, `emitsProposedPlan`, `supportsStructuredQuestions`, `planDeltasHaveItemIds` |
| `subagents` | `supportsSubagents`, `exposesSubagentThreadIds`, `emitsSubagentLifecycle`, `canWaitForSubagents`, `canCloseSubagents`, `canForkSubagentThread` |
| `context` | `acceptsSystemContext`, `acceptsDeveloperContext`, `acceptsSyntheticUserContext`, `canGenerateSummaries`, `canConsumeHandoffSummaries`, `supportsDeltaHandoff`, `supportsFullThreadHandoff`, `maxRecommendedHandoffChars` |
| `checkpointing` | `appCanCheckpointFilesystem`, `supportsNestedCheckpointScopes`, `providerCanRollbackConversation`, `providerRollbackReturnsSnapshot`, `providerCanReadConversationSnapshot` |
| `identity` | `nativeThreadIds`, `nativeTurnIds`, `nativeItemIds`, `nativeRequestIds`, each `strong`, `weak` or `none` (drives event correlation) |
| `runtimePolicy` | `enforcement` (`native` or `client-boundary`) |

The provider snapshot also carries presentation fields: `showInteractionModeToggle` and `supportedRuntimeModes`. A stored mode the provider does not offer runs as Supervised (`apps/server/src/orchestration-v2/RuntimePolicy.ts`).

### Degradation policies the orchestrator implements

| Missing capability | Fallback |
|---|---|
| Active steering | Interrupt and restart |
| Fork | Portable full-thread handoff summary |
| Rollback | Restore files and start a fresh provider context (marked divergent) |
| Structured approvals | Run under the sandbox |
| Plan updates | No todo UI |

**Rust complexity for the interface plus orchestrator glue: L.** Most cost sits in the orchestrator (V2 graph), which is outside this slice.

### Rules for adding a provider (`adding-a-provider.md`)

- Report only real capabilities.
- Offer only permission modes the provider enforces natively.
- Pass native option ids through unchanged.
- Keep state per instance; strip ambient credentials.
- Status checks must have no side effects.
- Inject the thread MCP server and survive its absence.
- Updates go only through the proven installer.
- Prove behavior with replay fixtures (`simple`, `multi_turn`, `queued_turn`, `turn_interrupt`, `message_steering`, `provider_thread_resume`, `tool_call_*`).
- Known traps:
  - Wait for config options to be advertised before writing them.
  - "Default" is a sentinel, never a model id.
  - A stop can arrive before the provider registered the turn.
  - Self-started work needs `ProviderContinuationRequests` and `hasPendingBackgroundWork`.
  - Subagents outlive their parent run.
  - Every terminal path must settle everything.
  - Classify `usage_limit` properly.
  - Handle symlinked cwds (macOS `/tmp`).

---

## 3. Source-control integrations

### Core

Core lives in `packages/source-control-core`.

**`SourceControlProvider`** (`SourceControlProvider.ts`):
- Required: `listChangeRequests`, `getChangeRequest`, `createChangeRequest` (body from a file), `getRepositoryCloneUrls`, `createRepository` (with visibility), `getDefaultBranch`, `checkoutChangeRequest`.
- Optional: `headBranchProbe`, `readChangeRequestTemplate`, `repositoryNameFromRemoteUrl`, `refineRepositoryIdentity`, `resolveLink` (turns a host URL into a title and body for prompt links).

**`PullRequestProvider`** (`PullRequestProvider.ts`, 546 lines; the PR review surface):
- Required: `getViewer`, `listChangeRequests`, `getChangeRequest`, `getChangeRequestActivity`, `getViewerPermissions`, `getDiff`, `runAction`, `comment`, `submitReview`, `listReviewerCandidates`, `setReviewerRequest`, `replyToThread`, `setReaction`, `setThreadResolution`.
- Optional: `listChangeRequestsAcross` (multi-repo inbox with cursors), `listChangeRequestStats`, `getChangeRequestChecks`, `getChangeRequestPreview`, `getChangeRequestSummary`, `getChangeRequestWatchFingerprint` (cheap change detection for `watch_pull_request`), `getChangeRequestStack`, `getReviewThreadComments`, `getDiffFileContents`, `getFilesViewed`/`setFilesViewed`, `getFileRevisions`, `updateChangeRequest`, `updateComment`, `listLabelCandidates`, `setLabels`, `repositoryKey`.

**Actions** (`packages/contracts/src/pullRequest.ts`): `merge` (merge, squash or rebase), `ready`, `draft`, `close`, `reopen`, `delete-branch`, `update-branch` (merge or rebase), `enable-auto-merge`, `disable-auto-merge`, `revert`, `approve-workflows`.

**Errors** carry a reason (`missing-tool`, `unauthenticated`, `rate-limited` with `retryAt`, `not-found`, `failed`). There is a shared rate limiter (`SourceControlRateLimit.ts`), a merge-message co-author stripper (`mergeMessage.ts`), checks normalization, `diffFileRevisions.ts`, and host discovery (`discovery.ts`).

**Server side.** `apps/server/src/sourceControl/*` (registry, discovery, per-host secrets) and `apps/server/src/pullRequest/*`, which covers:
- PR read cache
- Linked threads and PR watch (two-minute poll; wake on failed or passed checks, foreign comments, or conflicts; stop rules)
- Viewed files (server-kept for hosts with no native marks)
- **Review guides**: an LLM-generated "tour" of a PR, written in a temporary checkout (`PullRequestReviewGuideService.ts`)
- Cross-environment GitHub credential routing ("GitHub sharing": Read PRs or Read and act)

### Per host

| Host | Package / LOC | Transport and auth | Features implemented (optional methods present) | Gaps | Rust complexity |
|---|---|---|---|---|---|
| **GitHub** (+ Enterprise) | `packages/source-control-github` / 12.5k | Direct REST plus **GraphQL** (`gitHubGraphQlClient.ts`, `*.graphql.ts`). Token precedence: saved token in the secret store, then `GH_TOKEN` (or `GH_ENTERPRISE_TOKEN` with `GH_HOST`), then `gh auth token` (gh ≥ 2.81). Multi-account and per-host account selection. Quota tracking (`GitHubQuota.ts`) and request policy. | Inbox across repos with cursors and stats; checks; preview and summary; watch fingerprint via conditional requests (`gitHubConditionalChecks.ts`, ETag-like, to stay under the rate limit); **stacks** (`getChangeRequestStack`, merge stack, rebase stack: `githubStackActions.ts`, `githubStackRebase.ts`); review threads; diff file contents; **native viewed-file marks**; update PR; edit comments; labels; reactions; resolve threads; PR templates; link resolution; create repo; checkout; auto-merge; approve fork workflows; revert PR; strip agent co-author credits on merge. | `getFileRevisions` is not needed (native viewed marks). | **XL** |
| **GitLab** | `packages/source-control-gitlab` / 4.0k | `glab` CLI (`glab auth status`, `glab api ...`, `glab mr view|checkout|update|rebase`) | Checks, diff file contents, file revisions (batched), update MR, edit comments, reactions, resolve threads, identity refinement, link resolution, create repo, checkout, auto-merge | No stacks, labels, or native viewed marks | L |
| **Bitbucket Cloud** | `packages/source-control-bitbucket` / 3.4k | Direct REST to `https://api.bitbucket.org/2.0`. A repo/project/workspace access token, or an Atlassian email plus API token; stored in settings or env (`T3CODE_BITBUCKET_*`). | Checks, file revisions (parsed from the PR patch, with a byte ceiling), update, edit comments, reactions, resolve threads, create repo, checkout | Cannot reopen a declined PR; no labels or stacks | L |
| **Azure DevOps** | `packages/source-control-azure-devops` / 3.1k | `az` CLI with the `azure-devops` extension (`az repos pr list|show`, `az repos show --detect`, plus REST through `az`) | Summary, diff file contents, file revisions (from the iteration listing), update, reactions, resolve threads, create repo, checkout, auto-merge | Cannot edit comments (use the website) | L |
| **Forgejo / Gitea / Codeberg** | `packages/source-control-forgejo` / 2.3k | Token from the `fj` CLI (≥ 0.6) or `tea` (≥ 0.16), then direct `/api/v1/` REST with `Authorization: token`. `tea` is used for subpath installs. | Checks, summary, diff file contents, file revisions, update, edit comments, **labels**, reactions, resolve threads, owner/name from remote (subpath mounts), identity refinement, create repo, checkout | No stacks | M–L |
| **GitCafe** | `packages/source-control-gitcafe` / 2.4k | Direct API (`GitCafeApi.ts`). Token from settings, then `CAFE_TOKEN` (bound to `CAFE_HOST`), then the `cafe auth http credential get` git credential helper. `git.cafe` and `staging.git.cafe`. | Summary, **stacks** (merge lands the stack; updating a branch restacks the layers above), update, edit comments, reactions, resolve threads, create repo, checkout | No line comments, reviewer requests, or labels | M |

Client definitions (`packages/source-control-*/src/client/definition.ts`) describe settings fields, labels and icons for the UI. `packages/source-control-testing` provides a `TestSourceControlHost`.

**Total source-control parity in Rust: XL** (about 27k TS lines plus the server PR services, watch scheduler, review guides and GitHub sharing). Suggested order: GitHub (L–XL), then GitLab and Forgejo through CLI or REST (M each), then Bitbucket and Azure (M each), then GitCafe (S–M).

---

## 4. Remote infrastructure

### 4.1 Relay (`infra/relay`) — T3 Connect control plane — complexity **XL**

- **Runtime.** A Cloudflare Worker deployed with Alchemy (`alchemy.run.ts`). Postgres on PlanetScale through Hyperdrive (11 migrations in `migrations/postgres`), Cloudflare Queues for push delivery, and a Durable Object with SQLite (`hooks/HookInboxObject.ts`) for held webhooks. About 14.7k lines of TS. Clerk provides cloud identity.
- **Responsibilities.**
  - Link environments to a cloud account (`EnvironmentLinker.ts`, `EnvironmentLinks.ts`).
  - Provision **managed Cloudflare Tunnels and DNS** per user and environment (`ManagedEndpointProvider.ts` 1.5k lines, `ManagedEndpointAllocations.ts`). The generation counter, row locks and checkpointed provisioning are mutation-safe.
  - Run an idle-tunnel **reaper** (`ManagedEndpointReaper.ts`): a five-minute cron, modes off, dry-run or enabled, a bounded budget, and recovery registration.
  - Mint **one-time bootstrap credentials** (`EnvironmentConnector.ts`, `EnvironmentCredentials.ts`). The relay asks the environment, through its tunnel, to mint a credential bound to the client's DPoP key. The client redeems it directly with the environment. The relay never sees the environment session. Relay proofs are replay-guarded and signed (`EnvironmentPublishSignatures.ts`, `auth/DpopProofs.ts`, `auth/RelayTokens.ts`).
  - Forward webhooks (`hooks/HookForwarder.ts`): `/v1/hooks/:endpointKey/:hookId/:token` is forwarded to the environment's tunnel. Optionally, requests are held while the environment is offline (Durable Object, 1 MiB cap, 24h TTL, `x-t3-relay-delivery-id` for deduplication, a wake endpoint).
  - Mobile push: device registration, APNs (JWT, Live Activities) and FCM delivery, agent-activity aggregation and alerts (`agentActivity/*`, about 4k lines).
  - OAuth discovery documents (`/.well-known/oauth-authorization-server`, `/.well-known/oauth-protected-resource`).
- **HTTP API** (`packages/contracts/src/relay.ts`): `/health`, `/v1/environments` (list), `/v1/client/environment-links` (link, and `DELETE` to unlink), `/v1/client/environment-link-challenges`, `/v1/client/environment-links/:id/tunnel`, `/v1/client/dpop-token`, `/v1/client/devices` (also `/v2`), `/v1/environments/:id/connect|status|link-preferences|tunnel|tunnel/recovery|hooks/wake`, `/v1/environments/:id/threads/:threadId/agent-activity`, `/v1/mobile/devices[/:id]`, `/v1/mobile/agent-activity`, `/v1/mobile/live-activities`.
- **Auth.**
  - Clerk session-template JWTs (template `t3-relay`) for interactive clients.
  - Clerk CLI OAuth: a public client with PKCE through the hosted `/connect` page, or the **device authorization grant** for SSH and headless hosts (`packages/shared/src/connectAuth.ts`).
  - DPoP proofs on client calls.
- **Server side.** `apps/server/src/cloud/*`: `CloudLink.ts`, `ManagedEndpointRuntime.ts`, `managedTunnelStartup.ts`, CLI token manager, service launcher and self-update handoff. `apps/server/src/relay/*` covers agent-awareness publishing and the held-hook waker.
- **Connector.** `cloudflared`, pinned in `packages/shared/src/relayClient.ts` (2026.10.0 with checksums). Upgrades are zero-downtime (second connector, then stop the old one). There is a minimum-version gate. A user-supplied `cloudflared` is never replaced.
- **Rust notes.** For parity you either reuse T3's hosted relay protocol (unlikely to be allowed) or run your own control plane. The relay itself need not be Rust; a Worker in TS is fine. The **client and environment side** (link flow, DPoP, tunnel connector supervision, recovery registration) is **L** in Rust. The full relay with push is **XL**.

### 4.2 T3 Connect (client and environment) — complexity **L** (excluding the relay)

See `docs/internals/t3-connect.md`, `docs/internals/environment-auth.md` and `docs/operations/connect-setup.md`.

- **Environment auth.**
  - Scoped sessions: `terminal:operate`, `orchestration:read|operate`, `filesystem:read|write`, `source-control:write`, `access:write`, `relay:read`, and others.
  - Pairing links with a token in the URL fragment.
  - Cookie, bearer or DPoP-bound tokens; short-lived WebSocket tickets.
  - RPC-level scope middleware (`apps/server/src/auth/*`).
- **Connection runtime.**
  - An ordered route list per environment: LAN, Tailscale, VPN, public URL, SSH, T3 Connect.
  - Learned routes (LAN and tailnet addresses the server reports).
  - Preflight with a public descriptor and cooldowns; the supervisor fails back to earlier routes.
  - Code: `packages/client-runtime/src/connection/{driver,supervisor}.ts`.
  - Load balancing of new threads across environments using resource checks.
- **Rust complexity.** Auth model **L**; route driver and supervisor **M**.

### 4.3 Tailscale (`packages/tailscale`, 404 lines) — complexity **S**

Shells out to the `tailscale` CLI:
- `status --json`: parse the MagicDNS name and tailnet IPv4 (`100.64.0.0/10`, disambiguated from other VPNs).
- `debug prefs`: Tailscale SSH enabled?
- `ensureTailscaleServe` / `disableTailscaleServe`: `tailscale serve --https=<port>` to the local server.
- HTTPS endpoint probe; 3.5s status timeout; stderr diagnostics classification.

User surface: `t3 serve --tailscale-serve [--tailscale-serve-port]`, `t3 pair --tailscale`, and a desktop toggle.

### 4.4 SSH (`packages/ssh`, about 2.7k lines) — remote environments over SSH — complexity **L**

Yes, these are full remote environments. Desktop main runs the system `ssh` binary. Files: `tunnel.ts` (1.8k), `command.ts`, `config.ts`, `auth.ts`.

- **Host discovery.** Parses `~/.ssh/config` including `Include` globs, plus `known_hosts`. `ssh -G` resolves aliases.
- **Remote bootstrap script** (`buildRemoteLaunchScript`). Requires Linux or an Apple Silicon Mac with curl or wget, tar and sha256sum or shasum. Downloads the pinned T3 server archive to `~/.t3/runtime/versions/<ver>`, using a lock file, a staging dir and checksum verification. Picks a free port (a Node script). Launches or reuses the server, then issues a pairing token remotely (`issueRemotePairingToken`).
- **Port forward.** `-L` forward and HTTP readiness wait (`waitForHttpReady`, `resolveLoopbackSshHttpBaseUrl`).
- **Password and passphrase prompts.** A generated `SSH_ASKPASS` helper (sh, or cmd plus PowerShell on Windows) with `SSH_ASKPASS_REQUIRE=force` and a per-run secret. The desktop UI prompts the user (`SshPasswordPrompt`).
- **Lifecycle.**
  - The launcher stops a remote server only if it launched it.
  - Reconnect restores the forward before the app transport.
  - One session per desktop per host.
  - Provider CLIs must be on the PATH of a non-interactive login shell.

Related remote paths outside this package:
- WSL backend in desktop (`apps/desktop/src/wsl/DesktopWslBackend.ts`): another environment kind on Windows. **M**.
- Desktop "no local environment" mode (`localEnvironmentEnabled`).

### 4.5 Dev container (`.devcontainer/`, `docs/internals/devcontainer.md`) — complexity **S** (or not needed)

This is **maintainer tooling only**: Ubuntu 24.04, Node 24, pnpm, Rust stable, `vp`, `gh`, with named volumes for the pnpm store and `node_modules`, and Codespaces prebuild guidance. It is **not** a product feature: no "connect to a devcontainer" environment exists in apps or packages. For Waku, an equivalent `.devcontainer` with the Rust and GPUI deps is optional.

### 4.6 Outside agents (MCP server with OAuth) — see section 6.

---

## 5. `native/*` modules

| Module | Language / deps | Platform | What it does | Used by | Rust complexity for Waku |
|---|---|---|---|---|---|
| `native/resource-monitor` | Rust 2024, `sysinfo` 0.39, serde | macOS, Linux, Windows | A standalone child process speaking JSON lines over stdin/stdout (`PROTOCOL_VERSION 3`). Samples a process tree (CPU, memory, command line) for watched PIDs with start-time identity. Bounded history (1h, 3,600 snapshots, 20k rows, 64 MiB). Samples only while diagnostics have subscribers (interval 0 means on-demand). Linux task enumeration is disabled. | Server resource telemetry, diagnostics UI, load balancing (`docs/internals/resource-telemetry.md`) | **S**: already Rust and can be linked as a crate in-process |
| `native/browser-secret` | C, libsecret | Linux | A tiny helper that reads Chromium's "Safe Storage" key (`chrome_libsecret_os_crypt_password_v2`, `DONT_MATCH_NAME`) from the Secret Service. Distinct exit codes for missing, locked or failed. Prints the raw secret. | Desktop **browser cookie import** into the preview browser (`apps/desktop/src/preview/BrowserImport/LinuxBrowserSecret.ts`, `ChromiumKeys.ts`) | **S** (use the `secret-service` or `keyring` crates) |
| `native/hyprland-snap-shot` | Rust: wayland-client, smithay-client-toolkit, wayland-protocols(-wlr), png. Bundles `hyprland-toplevel-export-v1.xml` and `hyprland-toplevel-mapping-v1.xml`. | Linux / Hyprland (Wayland) | CLI with `check`, `capture <dir>`, `activate <pid> <title>` and `feedback <dir> <opts>`. Maps the foreign toplevel to the full 64-bit address, exports the active window to PNG, and returns window metadata only if it is unchanged after capture. Draws capture feedback (flash or flight) as a layer surface. Talks to Hyprland IPC. | **SnapShot** feature (window capture into the composer, with accessibility data) | **M** |
| `native/kde-snap-shot` | Rust (zbus 5 blocking) plus QML and a KWin script (`feedback.qml`, `activate.js`) | Linux / KDE Plasma (Wayland) | Calls `org.kde.KWin.ScreenShot2.CaptureWindow` (KWin authorizes by the executable path, so the binary is installed under the XDG data home with a desktop entry carrying `X-KDE-DBUS-Restricted-Interfaces`). Readiness check uses an invalid window id. Activates windows and shows feedback through a KWin script. | SnapShot | **M** |
| `native/libghostty-vt` | C headers only (`include/ghostty/vt/*.h`, about 8.9k lines) pinned to ghostty commit `9f62873b...` | All (built to **WASM** for web via `apps/web/scripts/build-libghostty-wasm.sh`) | Ghostty's VT parser and terminal state library: terminal, screen, selection, render, key and mouse encoders, kitty graphics, OSC, SGR, formatter. The web terminal renderer (`apps/web/src/terminal/ghostty/*`) runs on it. | Terminal emulation in web and desktop clients | **M** to integrate (Waku can link libghostty-vt natively through bindgen, or use `alacritty_terminal` or `wezterm-term`). Rendering in GPUI is the bigger job (L), and belongs to another slice. |

The other SnapShot backends are not in `native/`, but you need them for parity:
- macOS: ScreenCaptureKit plus the AX API.
- Windows.
- Niri: D-Bus `com.t3tools.SnapShot.Capture` invoked through `gdbus` from the compositor config.
- GNOME Shell extension `apps/desktop/gnome-extension` (UUID `snap-shot@t3.codes`).
- XDG Screenshot portal, GlobalShortcuts portal (`PortalCaptureShortcut`), and AT-SPI accessibility matching (PID plus a unique title and size).
- Safe dotfile config edits for compositor bindings.

**Full SnapShot parity: XL** (see `docs/internals/linux-snap-shot.md` and `docs/user/snap-shot.md`).

---

## 6. Outside agents and the ACP registry

### 6.1 Outside agents (`docs/user/outside-agents.md`) — complexity **L**

This is **inbound**: other agents drive T3. Each environment exposes an **MCP server over HTTP at `/mcp`** (`apps/server/src/mcp/McpHttpServer.ts`, `OrchestratorMcpService.ts`, toolkits under `apps/server/src/mcp/toolkits/{orchestrator,thread,project,worktree,pullRequests,preview,previewControls,device,environment,html,attachment}`).

External clients authenticate with a narrow **OAuth 2.1 authorization-code server** (`apps/server/src/auth/McpOAuth.ts`, `mcpOAuthHttp.ts`, `mcpOAuthHtml.ts`):
- Stateless dynamic client registration; every client is public and uses PKCE.
- Loopback redirects for local agents; any HTTPS redirect for hosted ones (ChatGPT, bots).
- An approval page names the redirect host. Approval spends a one-time pairing code, or uses a browser session with `access:write`.
- The user grants **read-only** access or a **runtime-mode ceiling** (Supervised through Full access) instead of scopes. The result is a session with subject `mcp-client` that only `/mcp` accepts.
- Every tool declares its access in `McpToolAccess.ts`. Thread targeting is checked in `threadAccess.ts`.
- Sign-ins last 30 days. Issuer and resource URLs come from Host and `X-Forwarded-Proto`, so loopback, Tailscale Serve and the T3 Connect tunnel all work.

Documented clients: `claude mcp add --transport http t3 <url>/mcp` then `claude mcp login t3`, `codex mcp add t3 --url ...` then `codex mcp login t3`, and ChatGPT custom MCP apps.

The same MCP server is injected **into** every provider session as the thread-scoped "t3-code" tool server. Tools include `delegate_task`, `link_pull_request`, `watch_pull_request`, worktree tools, preview tools, and others. The MCP server and OAuth are shared with orchestration; the OAuth and inbound part is about **M**, and the toolkits are **L**.

### 6.2 ACP Registry — complexity **L** on top of generic ACP

Covered in 1.9. In short:
- An online catalog (`cdn.agentclientprotocol.com/registry/v1/latest/registry.json`) of third-party ACP agents with per-platform distributions (binary archive, `npx`, `uvx`).
- T3 searches it, filters by host platform, installs into its home with SHA-256 checks, and runs the agent through the generic ACP adapter with **no per-agent code**. The documented exceptions are opt-in metadata shims for Copilot (`github.com/copilot` raw event feed for subagent attribution) and Devin (`cognition.ai/subagent_context` routing, streaming message ids, terminals routed through T3).
- Also: ACP auth-method UX (browser URL or in-app terminal), native session listing and import, logout, provider sub-selection (`listProviders`/`setProvider`), and "Local ACP command".
- Antigravity, Grok and Pi exist as dedicated flavors or drivers because they need behavior the spec does not cover.

For Waku this is the cheapest way to cover Amp, Kimi, Fx and DeepSeek if they publish ACP agents. Waku already has native drivers for some of them, which T3 does not.

---

## 7. Suggested Rust parity plan (this slice only)

1. **Adapter contract and capability struct** (L). Mirror `ProviderAdapterV2SessionRuntime`, the event union, the capability groups and the degradation policies. Build a replay-fixture harness first (L). It is what keeps 9 adapters honest.
2. **Generic ACP v1/v2 with the flavor-hook system** (XL), then Grok and Antigravity flavors (M and L) and the ACP Registry installer and auth (L).
3. **Codex parity** on Waku's existing app-server driver (L). Generate types from the upstream JSON schemas (M). Add managed ChatGPT OAuth and handoff (M), shadow homes (S), and the installer (M).
4. **Claude parity** on Waku's stream-json driver (XL). Re-implement what the Agent SDK provides: `canUseTool`, set mode and model, `resumeSessionAt`, fork, stopTask, subagent messages, hooks, rate-limit events, compaction. Add manifest-based models and reset credits.
5. **OpenCode 1.x and 2.x** (L), **Pi** (M; mostly present), **Cursor** without an SDK (L; ACP or CLI fallback, plus usage APIs), **Muse** (M, if MSP can be reverse-engineered from fixtures).
6. **Provider infra**: instances, credential store, auth flows, maintenance resolver, model manifest, usage readers and limits (XL in total).
7. **Source control**: GitHub first (XL), then GitLab, Forgejo, Bitbucket and Azure (L each), then GitCafe (M), plus PR watch, viewed files, review guides and GitHub sharing.
8. **Remote**: Tailscale (S), SSH environments (L), environment auth with scopes, pairing and DPoP (L), route driver and supervisor (M), T3 Connect client plus `cloudflared` supervision (L), and your own relay (XL, or skip).
9. **Native**: resource monitor (S, reuse), browser-secret (S), Hyprland and KDE helpers (M each), libghostty-vt (M to bind).
10. **Outside-agent MCP server with OAuth** (M) plus toolkits (L).
