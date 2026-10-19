# T3 Code: providers and adapters feature catalogue (read-only audit, for the Rust rewrite)

All paths are relative to the repo root (`.../t3code`). Complexity is the effort to reimplement in Rust: S is under a few days, M is about 1–2 weeks, L is several weeks, XL is a month or more and protocol-heavy.

Before the details, four things about the shape of this slice:

- **Two registries, not one.** `apps/server/src/provider/builtInDrivers.ts` lists 9 drivers: Codex, Claude, Cursor, Grok, OpenCode, Antigravity, Pi, Muse and ACP Registry. `apps/server/src/orchestration-v2/builtInProviderAdapterDrivers.ts` lists only 7 standalone adapter drivers. Antigravity and Muse build their orchestration adapter inside their own driver `create`, from `AntigravityDriver.ts` and `provider-muse/src/server/driver.ts`.
- **Some code lives in packages, not `apps/server`.** Six drivers have their own `packages/provider-*` package. Codex, Claude and Antigravity still live in `apps/server/src/provider/Drivers`.
- **`docs/internals/providers.md` points at a file that doesn't exist.** It links `apps/server/src/provider/providerMaintenance.ts`. The resolver actually lives in `packages/provider-core/src/server/maintenanceResolver.ts`.
- **The test surface is very large.** About 45k of the ~119k lines under `provider/` and `Adapters/` are tests and testkits. Replay recorders sit in `apps/server/scripts/record-*-replay-fixture.ts` for claude-agent-sdk, codex-app-server, cursor-agent-sdk, grok-acp, muse-msp and pi-rpc. Plan Rust replay tests against these transcripts.

---

## A. Core provider abstractions (`packages/provider-core/src/server`)

| # | Feature | Description | Key paths | Cx |
|---|---|---|---|---|
| A1 | ProviderDriver contract | Factory per driver kind: `driverKind`, metadata (`supportsMultipleInstances`, `hasDefaultInstance`), config schema/defaults, `create()` returning a `ProviderInstance`, optional `usage` reader. Instances of one driver must share no mutable state. | `driver.ts` | M |
| A2 | ProviderInstance shape | Per-instance object with:<br>• `snapshot` (managed status), `snapshotForCwd`, `refreshModels`, `invalidateCaches`<br>• `consumeResetCredit`, `orchestrationAdapter`, `textGeneration`, `auth`<br>• `acpSessionManagement` (listSessions / logout / deleteSession / listProviders / setProvider / disableProvider)<br>• `continuationIdentity` (`driver:instance:id`, or a shared key for shared homes) | `driver.ts` | M |
| A3 | ProviderAdapterV2 contract | Per instance: `getCapabilities`, `planSelectionTransition`, `openSession` returning a session runtime. | `ProviderAdapter.ts` | L |
| A4 | Session runtime surface | Methods:<br>• `events` stream / `subscribeEvents`<br>• `ensureThread`, `resumeThread`, `injectHistory?`, `startTurn`, `compactThread?`, `steerTurn`, `interruptTurn`<br>• `stopTask?`, `stopSubagent?`, `unloadThread?`, `respondToRuntimeRequest`, `readThreadSnapshot`<br>• `rollbackThread`, `forkThread`, `uploadFeedback?`, `mcpApps?`<br>• `hasPendingBackgroundWork` / `ForThread`, `backgroundTasksEndWithProcess`, `getModelContextWindow`, `canReuseContextUsage`<br><br>Event union: `app_thread.created`, `provider_session`/`thread`/`turn.updated`, `node.updated`, `subagent.updated`, `message.updated`, `turn_item.updated`, `command_output.delta` (ephemeral), `runtime_request.updated`, `plan.updated`, `turn.terminal` (completed / interrupted / cancelled / failed, plus retry, `threadDisposition` reusable/broken, `childTurnsContinue`).<br><br>15 tagged error types. | `ProviderAdapter.ts` | XL (event model) |
| A5 | Capability model | `OrchestrationV2ProviderCapabilities`: sessions, threads, turns, streaming, tools, approvals, planning, subagents, context, checkpointing, identity (strong/weak/none), and `runtimePolicy.enforcement` (native vs client-boundary). Orchestration branches on these flags, never on the driver kind. | `packages/contracts/src/orchestrationV2.ts:312`; `docs/orchestration-v2/provider-capability-system.md` | M |
| A6 | Selection transition plans | `apply_on_next_turn` / `restart_session` / `create_with_handoff` / `reject`. ACP rejects a model switch if the session lacks the capability. | `selectionTransition.ts` | S |
| A7 | ProviderHost port | Paths (state, cache, attachmentsDir), settings get/snapshot/changes, `shouldRunBackgroundWork(scope)`, `resolveAttachmentPath`, credential store `credentials(namespace, bindingId)`. | `ProviderHost.ts`; `apps/server/src/provider/ProviderHostLive.ts`, `ProviderCredentialStore.ts` | S |
| A8 | Managed snapshot engine | Holds the current `ServerProvider`. Runs `checkProvider` periodically and on settings change. Enrichment fiber (version advisory) is restarted on generation change. Also: PubSub of changes, `applyUsageLimits` (sparse merge), refresh semaphore. | `managedProvider.ts`, `snapshotSettings.ts` | M |
| A9 | Snapshot probe helpers | Command runner with timeouts (4 s default, 10 s auth). Command-not-found detection, including localized Windows cmd messages. Presentation fields: `showInteractionModeToggle`, `supportedRuntimeModes`, `reportsContextWindow`, `requiresNewThreadForModelChange`, `supportsConversationRollback`, `badgeLabel`. Built-in `/compact` command descriptor. | `snapshotProbe.ts` | M |
| A10 | Instance env merge | Merges the instance's env-var list (sensitive or redacted) over the process env. | `instanceEnvironment.ts`, `instanceIdentity.ts` | S |
| A11 | Failure normalization | `makeProviderFailure`, failure and retry turn items. Caps: message 4 KiB, code 128. `usage_limit` classification with reset time. | `failure.ts` | S |
| A12 | Id allocation | Deterministic ids for threads, nodes, items and plans, derived from provider thread refs. | `IdAllocator.ts` | M |
| A13 | Continuation requests | Adapter asks orchestration to wake the parent thread when background work, subagents or goals finish after a turn settled. Delivery is `adapter_buffered` or `message_text`. | `ProviderContinuationRequests.ts` | M |
| A14 | Subagent projection helpers | Child app-thread creation and titles, conversation artifacts, delegated-task progress, ending orphaned native subagents and runless root turns. | `subagentProjection.ts` | M |
| A15 | Background-work notifications | Notification turn items for shell / monitor / task / subagent completions. | `notification.ts` | S |
| A16 | Handoff budget | Context-handoff token cap (`T3CODE_CONTEXT_HANDOFF_TOKEN_CAP`, default 16k), native context usage for handoff, attachment token allowance. | `handoffBudget.ts` | M |
| A17 | Orchestration / runtime instructions | T3 MCP tool instructions in sections of at most 4 KB each (Codex truncates larger), browser-tool instructions. ACP has no system prompt, so instructions are injected into the first user prompt and again when tools or mode change. | `orchestrationInstructions.ts`, `runtimeInstructions.ts` | S |
| A18 | MCP session config | Per-thread `{endpoint, authorizationHeader, browserToolsAvailable, capabilities, agentDeviceEnvironment}`. An in-memory registry `McpProviderSessions` (set/read/clear by thread). `withAgentDeviceEnvironment` prepends a device shim directory to PATH. | `mcpSession.ts`, `McpProviderSessions.ts` | S |
| A19 | MCP tool presentation | Normalizes MCP tool titles, text and URLs for timeline rows. | `mcpToolPresentation.ts`; `apps/server/src/provider/CodexToolPresentation.ts` | S |
| A20 | Attachment prompt shaping | Native image vs file-path fallback (path list appended to the prompt). | `attachmentPrompt.ts`; limits in `packages/contracts/src/chatAttachment.ts` | S |
| A21 | Process group signalling | POSIX group kill helper. | `processGroup.ts` | S |
| A22 | Native protocol log summarizing | Structural method names. Payload summaries that never copy prompts or secrets. | `nativeProtocolLogging.ts`, `ProviderEventLoggers.ts` | S |
| A23 | Latest-version cache service | Shared npm/brew latest-version lookups. | `ProviderLatestVersions.ts` | S |
| A24 | Model catalog port | `ModelCatalog` service per driver: classify models current/legacy, apply catalog default and custom models. | `ModelCatalog.ts` | M |

## B. Instance registry, configuration and adapter registry

| # | Feature | Description | Key paths | Cx |
|---|---|---|---|---|
| B1 | Multiple instances per driver | `providerInstances` map in server settings, keyed by `ProviderInstanceId` (slug, at most 64 chars). Each entry has driver, displayName, accentColor, environment[], enabled, and an opaque per-driver config. Unknown drivers round-trip verbatim. The legacy default id equals the driver kind. | `packages/contracts/src/providerInstance.ts`, `settings.ts:1227` | M |
| B2 | Instance registry and reconcile | Builds instances from settings. Closes scopes of removed or changed instances before creating replacements. An explicit `enabled:false` on the envelope or in the config wins. `subscribeChanges` exists for hot reload. Create failures become "unavailable shadow" snapshots. | `apps/server/src/provider/ProviderInstanceRegistry.ts`, `ProviderInstanceRegistryMutator.ts`, `unavailableProviderSnapshot.ts` | L |
| B3 | Hydration with project overrides | A project override can enable an instance that is off machine-wide, so it is kept alive and probed. A sweep removes overrides for deleted projects. | `ProviderInstanceRegistryHydration.ts`, `ProjectSettingsOverrideSweep.ts` | M |
| B4 | Instance mutation RPC | `server.updateSettings` takes `providerInstanceMutation` (create / upsert / remove). | `packages/contracts/src/rpc.ts:758` | S |
| B5 | Adapter registry with auth admission | `ProviderAdapterRegistryV2.get/list/getMetadata`, built from the instance registry. Wraps `openSession` so that:<br>• it refuses while any instance sharing the credential binding is changing sign-in<br>• it runs inside every peer's `auth.withAccess` | `apps/server/src/orchestration-v2/ProviderAdapterRegistry.ts` | M |
| B6 | Provider session manager | Opens and caches provider sessions per instance:<br>• multiple threads per session where supported; fan-out event subscriptions per thread<br>• idle timeout 30 min; max idle pin 4 h while background work is pending<br>• per-thread idle unload; busy-turn tracking<br>• release reasons: `idle_timeout` / `runtime_error` / `manual_shutdown` / `server_shutdown`; detach / close / closeInstance<br>• per-thread MCP credential mint, reuse and revoke, safe against races with replacement sessions<br>• shutdown semantics (interrupt rather than end streams)<br>• background roster cleanup; scope-close watchdog for wedged finalizers | `apps/server/src/orchestration-v2/ProviderSessionManager.ts` (2.5k lines) | XL |
| B7 | Provider registry / status aggregator | Aggregates all snapshots and `streamChanges`. `refresh` (all or one instance, with optional `refreshModels`), `refreshWorkspaceSnapshot(instance, cwd)` (5-min TTL, commands and skills per workspace), merge rules per driver (Pi, ACP Registry, Antigravity keep previous auth and models when appropriate), maintenance action state. | `apps/server/src/provider/ProviderRegistry.ts` | L |
| B8 | Status cache on disk | Persists the last snapshots per instance and hydrates them at startup. Must match instance id and driver; removed custom models are dropped. Status ordering is defined here. | `providerStatusCache.ts` | S |
| B9 | ServerProvider snapshot schema | Fields:<br>• identity: instanceId, driver, displayName, accent, iconUrl, badgeLabel, continuation groupKey<br>• presentation flags; setup {canAuthenticate, canInstall, docsUrl}; nativeSessions {canList/Load/Resume/Delete}; configurableProviders; runtimePaths<br>• state: installed, version, status, auth {status, type, label, email, action, canLogout, subscriptionSharing, profileId, workspaceId}, availability<br>• catalog: models, updateRequiredModels, slashCommands, skills, workspaceSnapshots<br>• limits and advisories: usageLimits, versionAdvisory, compatibilityAdvisory, updateState | `packages/contracts/src/server.ts:60-281` | M |
| B10 | Orchestration adapter infrastructure layer | Composition of loggers, MCP sessions, continuation and similar services. | `ProviderOrchestrationAdapterInfrastructure.ts` | S |

## C. Installation, update and maintenance

| # | Feature | Description | Key paths | Cx |
|---|---|---|---|---|
| C1 | Ownership-proven update resolver | Detects which installer owns the resolved binary from its real path:<br>• npm global prefix, pnpm 10/11, Bun, Yarn global, Vite+, Volta shim<br>• Homebrew keg/cask (compares `brew info`), mise (manual only), WinGet (manual)<br>• npm `--prefix` pinning; npm 12 / pnpm install-script approvals; pnpm `minimumReleaseAge` exact specs<br>Falls back to the provider's own updater (`claude update`, `codex update` with `CODEX_HOME`, `opencode upgrade`, `pi update --self`, `grok update`). Ownership is cached per instance and re-read before running. | `packages/provider-core/src/server/maintenanceResolver.ts` (930 lines), `maintenance.ts` | L |
| C2 | Version advisory | `current` / `behind_latest` from the npm registry or brew. Gated by the `enableProviderUpdateChecks` setting. Produces `updateCommand` and `canUpdate`. | `maintenanceResolver.ts:802-930` | M |
| C3 | Maintenance runner | Runs the update command with progress lines and output capped at 10k chars. Refuses if the lock key changed. Verifies the version afterwards; an updater that exits 0 without updating is detected. A coordinator dedupes concurrent runs. RPC: `server.updateProvider`. | `apps/server/src/provider/providerMaintenanceRunner.ts`, `providerMaintenanceCommandCoordinator.ts` | M |
| C4 | Compatibility policy | Manifest `compatibility[]`: driver, t3CodeRange, recommendedRange, and status per version range (supported / graceful / unsupported / broken / unknown). Produces `compatibilityAdvisory`. | `providerCompatibility.ts`, `model-manifest.json` | S |
| C5 | Managed install (Codex, Antigravity) | RPCs `provider.install.start/cancel/subscribe/remove`. Phases: downloading, extracting, verifying, succeeded, failed, cancelled; byte progress; `source` managed or local; `canRemove`. Only `antigravity` and `codex` drivers are accepted. | `providerInstallation.ts`, `packages/contracts/src/providerSetup.ts` | M |
| C6 | Codex managed installation | Downloads pinned GitHub release tarballs (`rust-v0.156.1`, mac/linux-musl/windows × arm64/x64), verifies and extracts them, and resolves the executable for managed mode. | `CodexInstallation.ts` (767 lines) | M |
| C7 | Antigravity managed installation | Pinned zips from `dl.google.com` (`agy-acp-server-1.3.0`, 6 platforms) with byte-count and hash checks (handles gzip-encoded content-length).<br>• immutable versioned releases plus an atomic pointer<br>• process leases block removal or replacement while in use; outlives client connections<br>• PyInstaller unpack directory per process<br>• Windows `taskkill` tree and DLL-handle retries<br>• free-space checks | `AntigravityInstallation.ts` (876 lines), `antigravityRelease.ts` | L |
| C8 | Muse maintenance | Update only via the official launcher. Version parse and compare, latest-version lookup. | `packages/provider-muse/src/server/maintenance.ts` | S |
| C9 | ACP Registry agent install | Fetches `https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json` (cached).<br>• picks a distribution: binary (raw / zip / tar.gz / tar.bz2, platform target candidates, sha256 when given), npx, or uvx (exact versions required), with preference auto/binary/npx/uvx<br>• installs under `tools/<agent>/<version>/` with T3-owned npm prefix and Python tool directories; receipts<br>• search ranking<br>• uninstalls managed binaries when the last instance is removed<br>RPCs `server.searchAcpRegistry`, `server.prepareAcpRegistryAgent`, `server.uninstallAcpRegistryManagedBinary`. | `packages/provider-acp-registry/src/server/AcpRegistrySupport.ts` (2k lines) | L |

## D. Authentication and sign-in flows

| # | Feature | Description | Key paths | Cx |
|---|---|---|---|---|
| D1 | Generic auth controller and service | `ProviderAuthController` methods: start (methodId, returnUrl, callbackMode server/client), complete (callbackUrl), respond (browser accept/decline, terminal input, credentials), cancel, logout, subscribe. Also `withAccess` admission, `credentialBinding` (shared-credential peers), `isChangingCredentials`, `adoptCredentials`, `reconnectProfile`/`importProfile`.<br><br>`ProviderAuthService` routes RPCs, stops sessions bound to an instance when credentials change, and handles chat `/logout` prompts via `tryHandlePromptCommand`.<br><br>State phases: idle, starting, waiting, verifying, succeeded, failed, cancelled. Interactions: browser, deviceCode, terminal, credentials. | `packages/provider-core/src/server/auth.ts`, `providerAuthFlow.ts` (520 lines); `apps/server/src/provider/ProviderAuthService.ts`; `packages/contracts/src/providerSetup.ts` | L |
| D2 | Codex ChatGPT OAuth (managed) | OAuth against `auth.openai.com` (OIDC discovery):<br>• dynamic client registration per environment; PKCE; loopback `http://127.0.0.1:<port>/auth/callback`<br>• token exchange and refresh; requires scope `chatgpt.tokens.use.direct` (token sharing)<br>• multiple saved profiles (`chatgpt-profile:<clientId>`) in the server secret store<br>• cross-server session lock for refresh-token rotation; migration from localhost to 127.0.0.1 registrations; telemetry | `CodexChatGptAuth.ts` (984 lines), `CodexChatGptSessionLock.ts` | L |
| D3 | ChatGPT primary handoff | A remote environment's sign-in completes on a local primary. Uses an ephemeral in-memory secret store and the destination environment id, and exchanges and verifies before transferring the client registration and tokens. Only the destination persists them. | `CodexChatGptHandoff.ts`; RPC `provider.chatgpt.handoff.subscribe` (stream) | M |
| D4 | Codex local auth-callback receiver | Streams `ready` then `finished(callbackUrl)` from a local loopback listener for a remote flow. Callback page HTML. | `CodexAuthCallback.ts`, `CodexAuthCallbackPage.ts`; RPC `provider.codex.auth-callback.subscribe` | S |
| D5 | ChatGPT profile reconnect / import | RPCs `provider.chatgpt.reconnect-profile` and `provider.chatgpt.import-profile`. | `CodexChatGptAuth.ts:882-984` | S |
| D6 | Codex managed runtime | Launches with `-c` overrides for an `openai_token_sharing` model provider (`base_url` api.openai.com/v1, `env_key=ACCESS_TOKEN`, responses wire API, multi_agent off). Gets models from `api.openai.com/v1/models`. Strips the `serviceTier` option and the `/feedback` command. External usage link to chatgpt.com. | `CodexManagedRuntime.ts`, `CodexChatGptModels.ts`, `Drivers/CodexManagedProvider.ts`, `CodexManagedHome.ts`, `CodexManagedErrors.ts` | M |
| D7 | Antigravity auth | Auth methods: oauth-personal, oauth-business (GCP project/location), gemini-api-key, agent-platform (Vertex: key, or project plus ADC).<br>• **Per-instance profile:** `GEMINI_HOME` isolation, forced file credential storage, ambient Google creds stripped, skill directories symlinked or junctioned back to the real `~/.gemini`.<br>• **Browser capture:** a `BROWSER` env interception script prints the marker `__T3_ANTIGRAVITY_AUTH_URL__`.<br>• **Callback forwarding:** only the owned pending flow's loopback callback is forwarded.<br>• **Sign-out:** closes admission and stops processes before clearing account data.<br>• **`/logout` prompt:** handled as sign-out. | `AntigravityAuth.ts`, `antigravityAuthSupport.ts`, `antigravityCallback.ts` | L |
| D8 | Cursor sign-in | SDK browser login produces an API key with expiry, stored in a T3 credential store per instance. A `CURSOR_API_KEY` env override takes precedence. `withAccess` admission, sign-out closes sessions, `/logout` prompt, 5-min timeout. | `packages/provider-cursor/src/server/auth.ts`, `credentialStore.ts` | M |
| D9 | ACP Registry auth | Normalizes ACP `authMethods` (agent, env_var, terminal):<br>• agent methods via ACP `authenticate`<br>• terminal login in an in-app terminal, then reconnect<br>• env_var through secret env settings<br>• URL-elicitation consent: `server.acceptAcpRegistryUrlAuth`<br>• `server.logoutAcpRegistry`<br>Credentials are conservatively shared across instances of the same agent. | `provider-acp-registry/src/server/auth.ts`, `authenticationState.ts`, `AcpRegistryRuntimeCoordinator.ts` | M |
| D10 | Grok auth | Uses the cached `grok login` token (`cached_token`) or `XAI_API_KEY` (`xai.api_key`). Sets `GROK_OAUTH2_REFERRER=t3code`. Status never calls `authenticate`. | `provider-grok/src/server/acpSupport.ts`, `status.ts` | S |
| D11 | Other providers | No in-app sign-in. Claude uses CLI/keychain login detected via the SDK probe (`tokenSource` / `apiKeySource` / `apiProvider`, including Bedrock). Pi uses the user's `/login`. Muse uses `muse login`, with `META_API_KEY` stripped unless set on the instance. OpenCode uses its own auth, plus server password and URL. | `ClaudeProvider.ts:171-200`, `Drivers/ClaudeHome.ts` | S |

## E. Model catalogs, manifests and model options

| # | Feature | Description | Key paths | Cx |
|---|---|---|---|---|
| E1 | Remote model manifest | Bundled JSON `{version, updatedAt, compatibility, currentModels, providers{codex, claudeAgent, antigravity}}`. Fetched from GitHub raw `main` with a TTL and a backoff after failure. Cache on disk. A newer bundle (by `updatedAt`) outranks the cache. Validated against catalog references and the per-adapter schema. Exposed through `ModelCatalog.current(driver)` and `refreshInBackground`. | `apps/server/src/provider/ModelManifest.ts`, `model-manifest.json`; `docs/internals/model-manifest.md` | M |
| E2 | Claude model catalog | Entirely manifest-driven. Profiles hold `optionDescriptors`: effort `low`…`max`, `ultracode`, `ultrathink` (prompt-injected); `fastMode`; `thinking`. Adapter mapping: `effortMap`, `fixedContextWindowTokens`, `claudeCode.minVersion`. Handles aliases, version gating into `updateRequiredModels`, and an upgrade message. | `ClaudeModelCatalog.ts`, `ClaudeModelManifest.ts` | M |
| E3 | Claude selection compile | Compiles a selection into `apiModelId`, effort, prompt-injected effort and settings `{alwaysThinkingEnabled, fastMode, ultracode}`, plus a query identity. Decides whether the selection can be applied to a live process. | `apps/server/src/claudeModelOptions.ts`; `ClaudeAdapterV2.ts:1144-1170` | S |
| E4 | Codex model catalog | Live `model/list` from app-server. Maps `supportedReasoningEfforts` to a `reasoningEffort` select and `serviceTiers` to a `serviceTier` select (Standard / Fast / Ultrafast). Applies the preferred-default list. Manifest `minVersion` produces update-required models. `CODEX_INTERNAL_ORIGINATOR_OVERRIDE` avoids a models_cache race. | `CodexProvider.ts:100-400`, `codexModelCatalog.ts`, `codexModelOptions.ts` (fastMode → `serviceTier=fast`) | M |
| E5 | Default and alias tables | Default model per provider. Default text-generation model per provider. Slug aliases for Codex and Cursor. Sentinels that must never be sent to the provider: `antigravity-default`, Muse `default`, Pi `default`, Grok `grok-build`, ACP `default`. Display names. | `packages/contracts/src/model.ts` | S |
| E6 | Option descriptors | `select`/`boolean` descriptors with choices and defaults, plus `promptInjectedValues`. Selections are stored in `ModelSelection.options`. Custom models (slug, name, capabilities). | `packages/contracts/src/model.ts`, `modelSelection.ts` | S |
| E7 | Per-driver dynamic catalogs | Sources:<br>• **Cursor:** SDK model list (parameters → options, context window)<br>• **Grok:** ACP session model state and `grok models` CLI parse<br>• **OpenCode:** 1.x provider list; 2.x catalog loader with sub-provider names and two agreeing reads<br>• **Pi:** RPC `get_available_models` plus thinking ladder (`xhigh`/max opt-in)<br>• **Muse:** `model/list` plus reasoning effort<br>• **Antigravity:** session models plus manifest<br>• **ACP:** `configOptions` / models / modes (session mode option id `_t3/session-mode`, empty-value sentinel) | `provider-cursor/src/server/{CursorSdkCatalog,sdkModel,status}.ts`; `provider-grok/src/server/status.ts`; `provider-opencode/src/server/{status,openCode2Catalog}.ts`; `provider-pi/src/server/thinkingCapabilities.ts`; `provider-muse/src/server/modelCatalog.ts`; `AntigravityProvider.ts`; `provider-acp/src/server/sessionConfig.ts` | M each |

## F. Permission (runtime) modes and interaction (plan) mode

Shared contract: `RuntimeMode` is approval-required ("Supervised"), auto-accept-edits, auto, or full-access (the default). `ProviderInteractionMode` is default or plan. There are also approval-policy and sandbox overrides. Each provider advertises `supportedRuntimeModes`. The server runs an unoffered stored mode as Supervised (`apps/server/src/orchestration-v2/RuntimePolicy.ts`). Sources: `packages/contracts/src/providerPolicy.ts`, `docs/user/permission-modes.md`.

| Provider | Runtime-mode mapping | Plan | Cx |
|---|---|---|---|
| Codex | approval-required = untrusted / readOnly; auto-accept-edits = on-request / workspaceWrite; auto = on-request / workspaceWrite with `approvalsReviewer: auto_review`; full = never / dangerFullAccess. Reviewer is sent every turn. | `collaborationMode {mode: plan}` with developer instructions | S |
| Claude | `permissionMode`: plan / dontAsk / acceptEdits / bypassPermissions / default. Read-only tool sets. A `canUseTool` callback is installed only when approvals are needed. Launch args `--permission-mode` and `--dangerously-skip-permissions` win. | plan permissionMode; ExitPlanMode becomes a proposed-plan card | M |
| Cursor | Full access disables the sandbox; other modes enable it. Approval policy controls Auto-review. No interactive approvals. | `mode: "plan"` versus `agent` | S |
| Grok | Fixed at launch: `--permission-mode default\|auto` or `--always-approve`. No auto-accept-edits. Blocked commands in auto mode reach the user. | Hidden. `exit_plan_mode` is captured into a plan card. | S |
| OpenCode | All 4 modes through permission rules; Auto equals Supervised. Replies `once` rather than `always`. `.env` files gated. | Toggle shown on 2.x (plan agent) | M |
| Pi | Supervised, auto-accept-edits, full. Enforced by a blocking tool hook in the T3 extension; restarts the session on mode change. | none | S |
| Muse | Supervised and full only (`session/setApprovalMode`) | none | S |
| ACP / Registry | T3 answers `request_permission` by mode at the client boundary. Agent's own session modes via `set_mode`. Client-terminal grants. | Agent plan/build modes follow the T3 toggle | M (`provider-acp/src/server/clientPolicy.ts`) |
| Antigravity | `antigravityPermissionMode(runtimeMode)` sets the ACP mode. Native approvals can still arrive in full access. Remembered-approval options filtered. | Hidden (native `/plan`) | S |

## G. Per-driver catalogue

### G1. Codex: `codex app-server` (JSON-RPC over stdio)

Overall complexity XL (adapter 7.4k lines, protocol schema 57k generated lines).

**Driver**
- **Instance modes:** `setupMode` existing or managed. Settings: binaryPath, `CODEX_HOME` homePath, shadowHomePath (auth overlay), launchArgs (env override `T3CODE_CODEX_LAUNCH_ARGS`), customModels.
- **Shared/shadow home:** a separate shadow home holds `auth.json` while sharing state; it is materialized with conflict and symlink checks. The continuation identity lets instances sharing a home continue each other's threads (`Drivers/CodexHomeLayout.ts`).
- Files: `apps/server/src/provider/Drivers/CodexDriver.ts`, `CodexManagedProvider.ts`.

**Transport**
- Spawns `codex app-server <launchArgs>` (`codexLaunchArgs.ts`) through `packages/effect-codex-app-server`. That package is a JSON-RPC stdio client with requests, server notifications and server requests, schemas generated from the openai/codex repo, and a replay mode.
- `initialize` sends `experimentalApi`, `optOutNotificationMethods: ["turn/diff/updated"]`, and the MCP app extension mime type.

**Status probe** (`CodexProvider.ts`)
- `account/read` gives auth, email and plan label.
- `model/list`, `skills/list` per cwd (`snapshotForCwd`, 20 s timeout), `account/rateLimits/read`.

**Requests the adapter uses**
- Threads: `thread/start`, `thread/resume` (metadata-only read on resume), `thread/read`, `thread/fork`, `thread/unarchive`, `thread/unsubscribe`, `thread/inject_items`.
- Rollback: `thread/turns/list` + `thread/revert` (`CodexThreadRevert.ts`).
- Turns and compaction: `turn/start`, `turn/steer`, `turn/interrupt`, `thread/compact/start`.
- Goals: `thread/goal/set/get/clear`.
- Background terminals: `thread/backgroundTerminals/list/terminate`.
- MCP: `mcpServerStatus/list`, `mcpServer/tool/call`, `mcpServer/resource/read` (MCP apps).
- Other: `feedback/upload`, `account/rateLimitResetCredit/consume`.

**Notifications handled**
- Item deltas: agentMessage, commandExecution output, reasoning summary and text, plan.
- Turn and thread: `turn/plan/updated`, `turn/started`, `turn/completed`, `thread/tokenUsage/updated`, `thread/settings/updated`, `thread/goal/updated/cleared`.
- Account, model and errors: `account/rateLimits/updated`, `model/rerouted`, `error`.
- `item/started` and `item/completed` for contextCompaction, userMessage, subAgentActivity, collabAgentToolCall, fileChange, agentMessage, commandExecution, mcpToolCall/dynamicToolCall, webSearch, plan, reasoning.

**Server requests answered**
- `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval`.
- `mcpServer/elicitation/request` (app access approval, `CodexMcpElicitation.ts`).
- Legacy `execCommandApproval` and `applyPatchApproval`.
- `item/tool/requestUserInput`: async questions are persisted as `user_input_request` and answered by a new message with `responseCapability: message`.

**Turn params:** cwd, model, effort, serviceTier, `summary: "detailed"`, approvalPolicy / sandboxPolicy / approvalsReviewer, collaborationMode, `additionalContext`. T3 context has a 4 KB value cap; MCP-app context is sent as untrusted. Developer instructions apply when T3 MCP is attached (`CodexDeveloperInstructions.ts`).

**MCP injection:** thread config `mcp_servers.t3-code {url, tool_timeout_sec: 3900, http_headers Authorization}`, `tools.update_plan.enabled`, `mcp_optional_startup_grace_ms: 10000`.

**/goal**
- `parseCodexGoalCommand` handles show / edit / clear / pause / resume / set.
- Goal status mirrors active, paused, blocked, usage_limited, budget_limited, complete.
- One run owns several native turns (5 s continuation grace). Goal-only turns carry no native ref and are excluded from rollback counting.

**Other features**
- **Skills:** `$name` mention rewriting (`codexSkillMentionText`).
- **Background commands:** detail and output tail.
- **Token usage:** total versus last accumulation (`CodexTurnTokenUsage.ts`). Rate-limit partial merges (`codexUsageLimits.ts`).
- **Feedback:** `/feedback` upload. Text deltas are coalesced (`ProviderTextDeltaCoalescer.ts`).
- **Usage reader:** `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl` `token_count` events (`Drivers/codexUsage.ts`).

### G2. Claude: Claude Agent SDK (`@anthropic-ai/claude-agent-sdk` `query()` spawning the `claude` CLI)

Overall complexity XL (adapter 9.9k lines).

**Driver and settings**
- Settings: binaryPath, `CLAUDE_CONFIG_DIR` homePath, customModels, launchArgs, autoCompactWindow (100k–1M).
- Windows shim and executable resolution (`Drivers/ClaudeExecutable.ts`). Environment and continuation key per config dir; managed MCP config detection (`Drivers/ClaudeHome.ts`).
- Files: `apps/server/src/provider/Drivers/ClaudeDriver.ts`, `ClaudeProvider.ts`.

**Process isolation:** the SDK reads `CLAUDE_CONFIG_DIR` from `process.env`, so custom homes run in a worker (`layerQueryRunner`). History reads and forks run out of process through `claude-history-worker.ts` / `claudeHistoryWorker.ts` (`getSessionMessages`, `forkSession` via CLI argv, JSON on stdout; also `cli/claudeHistory.ts`).

**Status probe**
- An SDK query with a never-yielding prompt reads `initializationResult()`: account email, subscription, tokenSource, apiKeySource, apiProvider, and slash commands. No API call is made.
- Optional `usage_EXPERIMENTAL...({skipBehaviors})` for rate limits.
- 5-min TTL cache with a last-good fallback. `claude --version`.
- Workspace snapshot: skills plus `/compact` plus init commands.

**Query options** (`makeQueryOptions`)
- Model and effort. `resume` or `sessionId`; `resumeSessionAt` for rollback.
- allowed / disallowed tools, `canUseTool`, `onUserDialog`.
- `thinking: adaptive` with summarized display; `settings` (fastMode, ultracode, alwaysThinking, autoCompactWindow).
- `systemPrompt` preset `claude_code` with runtime and T3 instructions appended.
- `additionalDirectories` (cwd and attachments dir); extra CLI args.

**MCP**
- `mcpServers.t3-code` over http with a 65-min timeout. Pre-approves `mcp__t3-code__*`, or only read-only orchestrator tools in read-only sandboxes.
- Live updates via `setMcpServers`. MCP child processes are killed when the CLI exits.

**Live control:** `setModel`, `applyFlagSettings`, `setPermissionMode`. The process restarts when thinking changes or when an effort is cleared. Live-query reuse is keyed on the effective policy.

**Steering:** user message with `priority: "now"` (interrupts tools). Queued messages are supported.

**Compaction:** `compactThread` sends `/compact`. `compact_boundary` frames are handled.

**Plan and questions:** ExitPlanMode and EnterPlanMode handling, TodoWrite → plan steps, AskUserQuestion → structured questions.

**Subagents and background work**
- Task tool and `task_started` / `task_progress` / `task_notification`; `stopSubagent` and `stopTask`.
- `background_tasks_changed`. `hasPendingBackgroundWork`, `backgroundTasksEndWithProcess`.
- `local_workflow` multi-agent progress (`claudeWorkflowProgress.ts`).
- Live Bash output by tailing `<tmp>/claude-<uid>/<proj>/<session>/tasks/<id>.output` every 250 ms, up to 40 attempts and 256 KB per poll (`ClaudeTaskOutputTail.ts`).

**/goal:** SDK mode emits no goal events, so the adapter reads the synthetic command output and Stop-hook feedback in the transcript.

**Token usage:** per-message cumulative usage (`ClaudeTurnTokenUsage.ts`). Context window from the catalog (1M fixed).

**Usage limits:** `rate_limit_event` (sparse, overage buckets) and `get_usage` (`claudeUsageLimits.ts`).

**Reset credits:** POST `api.anthropic.com/api/organizations/{org}/reset_rate_limits` with an idempotency key; unavailable on macOS because of the keychain (`claudeResetCredits.ts`).

**Skills** (`Drivers/ClaudeSkills.ts`, `ClaudeSkillDispatch.ts`)
- Discovered from the config dir `skills/` and project `.claude/skills/`; precedence rules.
- Settings override files with YAML 1.1 booleans; disable-model-invocation and user-invocable flags.
- Dispatch is a leading `/name` text block.

**Other**
- Images as base64 content blocks. Prompt UUIDs and echo detection to identify foreign turns.
- Thinking-as-text heuristic for Claude 5.1. Native tool classification.
- **Usage reader:** `~/.claude/projects/**/*.jsonl` per content block, with dedupe (`Drivers/claudeUsage.ts`).

### G3. Cursor: `@cursor/sdk` in-process local agent (not ACP)

Overall complexity L.

- **Files:** `packages/provider-cursor/src/server/{driver,adapter,CursorAgentSdk,CursorSdk,sdk}.ts`. The SDK is loaded from disk with `createRequire` and `SqliteLocalAgentStore`. A guard catches SDK shell-spawn unhandled rejections that would otherwise crash Node.
- **Agent options:** `settingSources` covers every Cursor settings layer (rules, skills, hooks, MCP, team/MDM policy, plugins). Sandbox mode is derived from the runtime mode. MCP is `t3-code` http with an Authorization header (`cursorMcpServers`).
- **Features:** create and resume agent, `send` with ordered `onDelta`, `Agent.messages.list()` snapshot, cancel, images, plan/todo streaming, `task` subagents (read-only children, no resumable ids). Steering is interrupt-and-restart. Fork is a portable handoff. No rollback and no interactive approvals.
- **Skills:** scans user and project roots; `$name` is rewritten to `/name` (`skills.ts`).
- **Status:** SDK catalog probe (`status.ts`, `CursorSdkCatalog.ts`); `auto` maps to SDK `default`.
- **Text generation:** via the SDK, with sandbox-unavailable fallback (`textGeneration.ts`).
- **Usage limits:** `api2.cursor.sh` monthly, Auto vs API (`usageLimits.ts`).
- **Usage:** dashboard API `cursor.com/api/dashboard/get-filtered-usage-events` with the session cookie, incremental account cache (60 s TTL). macOS Keychain read with timeout and joinable in-flight read, opt-in "Enable Cursor usage" (`accountUsage.ts`, `accountCache.ts`, `CursorKeychain.ts`, `CursorAccountReader.ts`, `CursorUsageAccounts.ts`).
- **Errors:** transport failure classification, model-rejected errors.

### G4. Grok: `grok agent stdio` (ACP flavor)

Overall complexity L.

- **Spawn** (`acpSupport.ts`): args `[--permission-mode default|auto] agent [--always-approve] stdio`. `GROK_OAUTH2_REFERRER` set. Probe env disables self-update (`probeEnvironment.ts`). Process ownership and group kill.
- **xAI ACP extensions** (`xaiAcpExtension.ts`, 1.6k lines; `xaiBackgroundTasks.ts`):
  - methods: `_x.ai/ask_user_question`, `_x.ai/exit_plan_mode` (captured into a plan card), `_x.ai/session/prompt_complete`, `_x.ai/session/update`, `_x.ai/session_notification`, `_x.ai/task_backgrounded`, `_x.ai/task_completed`, `_x.ai/queue/changed`, `_x.ai/sessions/changed`
  - rate-limit error code -32003
  - wake notifications for `task-completed-*` prompts
  - monitor and background shell tasks; subagent updates and end notices with child session ids
- **Cancel quirks:** cancel meta `{cancelTrigger: "ctrl_c"}`. Interrupts the prompt fiber on cancel and restarts the runtime after interrupt (task_already_running). Image prompts are forced on.
- **Capabilities:** snapshot read, subagents with thread ids, no fork or rollback, no runtime-mode switch in session.
- **Status** (`status.ts`): never calls `authenticate` or `session/new`. Uses `initialize` commands plus the `grok models` CLI text parse.
- **Skills:** asks the CLI, which includes plugin skills (`skills.ts`).
- **Usage limits:** `cli-chat-proxy.grok.com/v1/billing?format=credits`, using an `accounts.x.ai` token (`usageLimits.ts`).
- **Usage:** `GROK_HOME` transcripts, cost in ticks (1e10 per USD) (`usage.ts`).
- **Text generation:** no investigation support (`textGeneration.ts`).
- **Updates:** `grok update` except for Homebrew casks.

### G5. OpenCode: 1.x and 2.x behind one driver (version-probed)

Overall complexity XL (adapters 3.7k + 4k lines).

- **Driver** (`packages/provider-opencode/src/server/driver.ts`, `versionProbe.ts`): probes `--version` and `/api/info` JSON to pick generation 1.x (`opencode-ai`, min 1.14.19) or 2.x (`@opencode/cli`, ≥2.0.18). Never migrates packages. Settings: binaryPath, serverUrl, serverPassword (external server mode).
- **1.x** (`OpenCodeRuntime.ts`, `OpenCodeServerOwner.ts`, `adapter.ts`):
  - `opencode serve --hostname --port`, one server per thread
  - MCP injected via `OPENCODE_CONFIG_CONTENT`; `OPENCODE_SERVER_PASSWORD`
  - instance-owned helper server for catalog and text generation, closed after an idle period
  - SDK HTTP plus SSE (`event.subscribe`); prompt admission state machine; session idle is the terminal signal
  - user message used as the turn ref; DB-locked startup classification
- **2.x** (`v2/OpenCode2Server.ts`, `v2/OpenCode2Client.ts`, `v2/adapter.ts`):
  - one server per instance, HTTP Basic auth with a generated in-memory password
  - `/api/event` SSE with heartbeat timeout detection
  - per-thread MCP entry `t3-code-<thread>` (digested if longer than 64 chars) via `mcp.add/remove/status`, plus session permission rules that deny other threads' entries
  - SDK calls: `session.*` (create, get, update, abort, revert, fork, messages, promptAsync, command, active, switchModel, switchAgent, summarize, compact, children, instructions, inbox, interrupt, status, move), `permission.reply/list`, `question.reply`, `form`, `model/provider/agent/skill/command.list`, `app.agents/skills`, `global.health`
  - events: session text/reasoning/tool/step/execution/compaction/inbox, `permission.asked`, `question.asked`, `form.*`, `provider.quota`, and others
- **Server ledger:** records spawned server process groups (pid, start time via `ps lstart`, command line) and kills orphans after a crash without touching recycled pids (`OpenCodeServerLedger.ts`).
- **Features:** subagent child sessions (`session.children`), steering (2.x inbox steer prompt ids), compaction, fork/revert rollback, snapshot read, `unloadThread` (2.x), tool item projection (`toolItems.ts`).
- **Usage limits:** OpenCode Go at `opencode.ai/zen/go/v1/usage` with `OPENCODE_API_KEY`; local servers only (`usageLimits.ts`).
- **Usage:** SQLite DB plus legacy JSON history; `OPENCODE_DATA_DIR` (`usage.ts`).
- **Text generation:** temporary session per request; asks rejected; free-tier 403 workaround (`textGeneration.ts`, `v2/textGeneration.ts`).
- **Workspace snapshot:** commands and skills, waiting on `command.updated` / `skill.updated` for an unscanned directory.

### G6. Pi: `pi --mode rpc` (LF-delimited JSON over stdio)

Overall complexity L.

- **RPC framing** (`rpc.ts`): manual LF split, never readline because of U+2028.
- **Commands:** prompt, abort, set_model, set_thinking_level, get_state, get_entries, get_commands, get_available_models, compact, fork, new_session, switch_session, set_session_name, get_session_stats, get_messages, get_last_assistant_text, extension_ui_response.
- **Events:** agent_start, turn_start, message_start/update/end, text/thinking deltas, tool_execution_start/update/end, agent_settled, extension_ui_request, provider_error.
- **Launch args** (`mcpInjection.ts`): reserved-flag validation; `--provider` requires `--model`. `MINIMUM_PI_VERSION` 0.80.5.
- **MCP bridge:** a generated TypeScript Pi extension is written to disk (`pi-t3-mcp-extension.ts`) and loaded with `--extension`. Env `T3_MCP_URL`, `T3_MCP_BEARER_TOKEN`, `T3_PI_RUNTIME_MODE`. It also enforces runtime-mode approvals through a blocking tool hook; edit/write are file-change approvals (`mcpExtensionSource.ts`, 459 lines).
- **Sessions:** native session files for resume and rollback. Turn tree refs. Fork runs the Pi CLI `--fork <sessionFile>` in the destination cwd.
- **Steering:** `streamingBehavior: "steer"`. Compaction maps `/compact` to RPC compact (`commands.ts`).
- **Extension dialogs:** select / confirm / input / editor become user-input requests.
- **Subagents:** shown from Pi's example `subagent` extension.
- **Usage:** `~/.pi/agent/sessions` or `PI_CODING_AGENT_SESSION_DIR`, plus tool/summary costs (`usage.ts`).
- **Text generation:** no investigation support (`textGeneration.ts`).

### G7. Muse Code: `@muse-code/sdk` `spawnMspConnection` (MSP over stdio, beta, off by default)

Overall complexity L.

- **Environment** (`sdk.ts`): `META_API_KEY` stripped unless set on the instance; `MUSE_NO_AUTO_UPDATE=1`. Windows verbatim realpath handling. Read-only and sessionLogging launch options. `grantedCapabilities` includes `sessionMcp`.
- **Requests:** `session/start`, `session/resume`, `session/read`, `session/compact`, `session/setModel`, `session/setApprovalMode`, `model/list`, `turn/start`, `turn/steer`, `turn/interrupt`, `approval/decide`.
- **Notifications:** `item/started/delta/updated/completed`, `turn/started/completed/retryScheduled`, `approval/requested/updated/resolved`, `session/tokenUsage`, `session/contextUsage`, `session/todoListChanged`, `usage/changed`, `view/gap`.
- **Capabilities:** no fork or rollback; inline child sessions are not addressable. Item presentation in `itemPresentation.ts`.
- **Usage limits** (`usageLimits.ts`): windows from `usage/changed`; quota-refusal text parse with reset-time computation.
- **Text generation:** read-only session with JSON schema output (`textGeneration.ts`).

### G8. Antigravity: Google `agy-acp-server` (ACP flavor, dedicated driver)

Overall complexity XL including install and auth.

- **Driver:** `apps/server/src/provider/Drivers/AntigravityDriver.ts`, `AntigravityProvider.ts`. The adapter (`apps/server/src/orchestration-v2/Adapters/AntigravityAdapterV2.ts`) is a flavor over the shared ACP adapter.
- **Runtime:**
  - `preferResumeSession` (session/resume rather than a slow load), `subagentsIdleOnTurnCompletion`, `supportsCompaction`, image prompts, model and runtime-mode switching in session
  - ACP client fs read/write restricted to cwd plus attachments dir, with symlink resolution (`acp/AntigravityClientFiles.ts`)
  - approval option filtering ("Allow Always" prompt-injection guard) and question extraction from `request_permission` (`acp/AntigravityProtocol.ts`)
  - `start_subagent` tool batches become subagents
  - temporary session-file cleanup and stale temp-dir sweep (`acp/AntigravitySessionFiles.ts`)
  - model selection never overwrites a saved selection with a cold-resume default (`acp/AntigravityAcpSupport.ts`)
- **Health check:** never spawns (the binary unpacks about 1 GB per launch). Authenticated catalog sessions run only on explicit refresh.
- **Skills:** `.gemini/skills`, `.agents/skills`, `.agent/skills`; user dirs under `GEMINI_HOME` (`Drivers/AntigravitySkills.ts`).
- **Usage limits** (`antigravityUsageLimits.ts`): `cloudcode-pa.googleapis.com` and the daily endpoint, `v1internal:fetchAvailableModels` / quota, OAuth refresh via `oauth2.googleapis.com/token`, `userinfo` for the account id; windows merged across endpoints.
- **Usage:** reads Antigravity SQLite conversation databases with a rowid-seek paged reader, cache keyed by canonical path and ctime; `ANTIGRAVITY_DATA_DIR` (`Drivers/antigravityUsageReader.ts`, `AntigravityUsage.ts`).
- **Text generation:** denies tool requests and refuses profiles with hooks or MCP config; `supportsTextGeneration` is computed (`apps/server/src/textGeneration/AntigravityTextGeneration.ts`).

### G9. Generic ACP adapter (`packages/provider-acp`, used by Grok, Antigravity and the Registry)

Overall complexity XL (adapter 7.5k + runtime 2.9k lines).

- **Session runtime** (`AcpSessionRuntime.ts`):
  - spawn plus `initialize` (ACP v2 preview with v1 negotiation, `effect-acp/src/compat.ts`) and `authenticate`
  - session lifecycle: `session/new`, `load`, `resume`, `list`, `delete`, `fork`, `close`
  - turn and config control: `session/prompt`, `cancel`, `set_mode`, `set_model`, `set_config_option`
  - `session/request_permission`, `session/elicitation` (+ complete, URL elicitation), `session/update` stream
  - process ownership: POSIX process-group trees, Linux cgroup v2 leases and stale-sibling sweep, Windows tree kill
  - stderr tail of 4 KiB, sanitized (`stderr.ts`)
- **Client-side services:**
  - `fs/read_text_file`, `fs/write_text_file`
  - client terminals: `terminal/create/output/wait_for_exit/kill/release`, embedded terminal content (`clientTerminals.ts`; Devin runs commands through T3 terminals)
  - MCP-over-ACP bridge: `mcp/connect/message/disconnect` (`mcpOverAcpBridge.ts`)
  - permission policy: allow / ask / deny; reads follow the sandbox; grants per turn or session (`clientPolicy.ts`)
- **Normalization:**
  - session updates into turn items; tool-call state merge with a bounded tail for cumulative output; plan updates; available commands (a `$` prefix means a skill); config options to descriptors; usage_update / session_info_update into the context meter and cost (`runtimeModel.ts`, `coreRuntimeEvents.ts`, `sessionConfig.ts`)
  - instructions sent in the first prompt; images when negotiated or forced
  - rollback restarts a fresh session
- **Flavor hooks:** `AcpAdapterV2Flavor` exposes about 40 hooks for per-agent quirks (`adapter.ts:195-330`).
- **Native logging:** `nativeLogging.ts`.

### G10. ACP Registry (`acpRegistry` driver)

Overall complexity L.

- **Settings** (`packages/provider-acp-registry/src/settings.ts`): source `registry` or `local` (local command plus args), agentId, commandPath override, commandArgs, authMethodId, distribution preference, customModels.
- **Driver** (`server/driver.ts`): status is a cached discovery probe (disposable `session/new`); failed or unauthenticated results are not cached. Readiness. Live configuration and available commands are published from running sessions. `supportsTextGeneration: false`. `nativeSessions` and `configurableProviders` flags.
- **Probe** (`server/probe.ts`): auth methods, models, current model, config options, session management (list, load, resume, logout, delete, configure providers).
- **Native session operations:** list, import into a deterministic thread (idempotent), delete. Sub-provider configuration via `providers/list|set|disable`. Logout.
- **Coordinator** (`AcpRegistryRuntimeCoordinator.ts`): foreground startup vs background probe; session mutation serialization; URL-auth consent with expiry.
- **Agent-specific flavors:**
  - Copilot: raw-event subagent routing, client capability meta (`copilotAcp.ts`)
  - Devin: update and tool normalization, subagents (`devinAcp.ts`)
  - Mistral Vibe: rate-limit code and backoff extension (`adapter.ts`)

## H. Cross-cutting runtime features by provider

| Feature | Codex | Claude | Cursor | Grok | OpenCode | Pi | Muse | Antigravity / ACP |
|---|---|---|---|---|---|---|---|---|
| Active steering | `turn/steer` | priority "now" | interrupt + restart | interrupt + restart | yes (2.x inbox) | steer streamingBehavior | `turn/steer` | ACP: interrupt + restart |
| Compaction | `thread/compact/start` | `/compact` | yes | via ACP | `session.compact` / `summarize` | RPC compact | `session/compact` | flavor `supportsCompaction` |
| Rollback | `thread/revert` | `resumeSessionAt` | no | no | `session.revert` | session tree | no | fresh session |
| Fork | `thread/fork` | `forkSession` (worker) | handoff | no | `session.fork` | CLI `--fork` | no (copy context) | no |
| Subagents | collabAgent / subAgentActivity, child threads | Task / workflow | `task` (read-only) | xAI subagents with ids | child sessions | extension | inline children | `start_subagent` batches; Copilot / Devin |
| T3 MCP injection | thread config `mcp_servers` | `mcpServers` + allowedTools | SDK `mcpServers` | ACP `mcpServers` | 1.x config env; 2.x `mcp.add` per thread | Pi extension | `sessionMcp` | ACP + MCP-over-ACP |
| Questions | async `requestUserInput` | AskUserQuestion | none | `_x.ai/ask_user_question` | question / form | extension dialogs | user input | permission-shaped questions |
| Images | native | base64 blocks | yes | forced on | yes | image type | yes | negotiated / forced |
| /goal | native | transcript-parsed | — | — | — | — | — | — |
| History injection | `thread/inject_items` | — | — | — | — | — | — | — |
| MCP apps / feedback | yes / `feedback/upload` | — | — | — | — | — | — | — |

Further notes on these features:

- **Attachments generally:** attachments are validated and claimed outside the workspace (`apps/server/src/orchestration-v2/AttachmentClaims.ts`). The attachments dir is granted to sandboxed agents (Claude `additionalDirectories`, Antigravity file roots). Non-image files become path lists. Question-answer attachments are appended as paths (`apps/server/src/provider/userInputAttachments.ts`). Limits: 120k chars, 100 attachments, 10 MB per image, 80 MB total images, 50 MB per file. Complexity M.
- **Slash commands and skills discovery:** per-workspace snapshots, refreshed every 5 minutes (`PROVIDER_WORKSPACE_SNAPSHOT_TTL_MS`).
  - Codex: `skills/list`. Claude: init commands plus filesystem skills. Cursor: filesystem scan.
  - Grok: CLI. OpenCode: `command.list` / `skill.list`. Pi: `get_commands` plus `/compact`.
  - ACP: `available_commands_update`. Antigravity: filesystem.
  - `/usage-limits` is injected into catalogs only for clients that declare `usageLimitsCommand` (`apps/server/src/ws.ts:1657,2924`).
  - Complexity M overall.
- **Text-delta coalescing:** batches streaming deltas per (turn, item) and flushes on an interval or on completion (`ProviderTextDeltaCoalescer.ts`). Complexity S.
- **Citations:** not provider-side. Citation links are expanded with the shared formatter before dispatch to the adapter, and input limits apply after expansion (`packages/shared/src/assistantCitations.ts`, `docs/internals/assistant-citations.md`). Complexity S.

## I. Usage limits and rate limits

| # | Feature | Description | Key paths | Cx |
|---|---|---|---|---|
| I1 | Usage limits model | Windows {id, kind session/weekly/monthly/other, usedPercent, resetsAt, duration}, credential fingerprint, reset credits, externalUsage link, `unavailable` (unsupported vs probeFailed). Sparse updates merge by window id. Rules for keeping results after a probe failure. | `packages/contracts/src/providerUsageLimits.ts`; `provider-core/src/server/usageLimits.ts` | M |
| I2 | Per-provider limit sources | Codex (read and updated notifications, partial merge, failure messages), Claude (stream and get_usage), Cursor, Grok, OpenCode Go, Muse, Antigravity. | see G sections | M each |
| I3 | Reset-credit redemption | RPC `provider.consumeResetCredit` (by instance or by hub account). Outcomes: reset, nothingToReset, noCredit, alreadyRedeemed. Serialized per account directory with a sticky idempotency key; re-probes to confirm (`resetCreditCoordinator.ts`). Codex uses `account/rateLimitResetCredit/consume`; Claude uses the org endpoint. | `CodexDriver.ts:279-337`, `claudeResetCredits.ts` | M |
| I4 | CLIProxyAPI hub sources | Settings `UsageLimitSourceConfig {kind: cliproxy, url, managementKey}`. Calls `v0/management/{auth-files, api-call, reset-quota}` to read Codex (`chatgpt.com/backend-api/wham`) and Claude (`api.anthropic.com/api/oauth/usage`) quotas per pooled account. Extra-CA handling, sanitized errors, UUIDv5 idempotency. Snapshots are carried in the server config stream when the client opts in with `usageLimitSources`. | `apps/server/src/usage/cliproxyApi.ts`, `UsageLimitSources.ts` | M |
| I5 | Usage-limit failures and recovery | Turns that fail on quota are classified `usage_limit` with a reset time; orchestration offers limit recovery (`OrchestrationV2LimitRecovery`). Implemented in Codex, Claude, Grok, Muse, OpenCode 2, ACP Registry. | `provider-core/src/server/failure.ts` | M |

## J. Usage and cost aggregation

`UsageService` complexity is L. Feature list:

- **Transcript scanning:**
  - streamed via `node:fs` with resume offsets and 64-byte guard hashes to detect rewritten files (`usageTranscriptReader.ts`)
  - per-version scan cache file `usage-scan-cache-v5.json` with string interning (`usageScanCache.ts`)
  - mtime pre-filter
- **Aggregation:** global dedupe across files, day/hour buckets, model aliases and "Map to" remapping, fast/ultrafast premium (`usageAggregation.ts`).
- **Pricing:** LiteLLM `model_prices_and_context_window.json` from GitHub raw or API. Cache read/write discount and premium rules. Per-environment custom price overrides (`usagePricing.ts`).
- **Contract:** v6, merge-compatible since v4. Provider kinds: claude, codex, grok, cursor, opencode, antigravity, pi.
- **RPCs:** `server.getUsageSummary`, `server.refreshUsageRates`.
- **Readers:** one per driver via `driver.usage` in `BUILT_IN_USAGE_DRIVERS`; order matters for dedupe.
- **Paths:** `apps/server/src/usage/*`, `packages/contracts/src/usage.ts`, `provider-core/src/server/usage.ts`.

## K. Text generation (titles, commit messages, PR descriptions, branch names, review guides)

| # | Feature | Description | Key paths | Cx |
|---|---|---|---|---|
| K1 | Operations interface | `generateCommitMessage` (subject, body, optional branch), `generatePrContent` (title, body, change-request template), `generateBranchName`, `generateThreadTitle` (linked context, previous title, `needsRefinement`, attachments), `generateReviewGuide` (stacked PRs, needs read-only investigation with a 15-min timeout). | `provider-core/src/server/textGeneration.ts`, `textGenerationOperations.ts` | M |
| K2 | Prompts and sanitizers | Prompt builders, JSON-schema output, section limits, subject / PR-title / thread-title sanitizers, CLI error normalization, title context budget. | `textGenerationPrompts.ts`, `textGenerationUtils.ts`, `threadTitleContext.ts` | M |
| K3 | Policies | default / conventional_commits / repo_conventions / custom, with per-artifact instructions and inferred repo conventions. | `textGenerationPolicy.ts`; `apps/server/src/textGeneration/TextGenerationPresets.ts` | S |
| K4 | Routing service | Routes by `modelSelection.instanceId`. Thread titles resolve linked PR/issue context through source control. | `apps/server/src/textGeneration/TextGeneration.ts`, `ThreadTitleLinks.ts` | S |
| K5 | Per-provider runners | Codex: `codex exec --output-schema`, plugins and apps disabled, launch-arg filtering, investigation supported. Claude: `claude -p --output-format json`, read-only tools allowlist, git deny rules, investigation supported. Cursor: SDK. Grok, Pi: no investigation. OpenCode 1.x and 2.x: temporary sessions. Muse: read-only session. Antigravity: guarded. ACP Registry: unsupported. | `apps/server/src/textGeneration/{Codex,Claude,Antigravity}TextGeneration.ts`; `packages/provider-*/src/server/textGeneration.ts` | M each |

## L. Importing outside agent sessions and history

| # | Feature | Description | Key paths | Cx |
|---|---|---|---|---|
| L1 | Agent session scan | Scans the Claude projects directory `*.jsonl` and Codex `sessions/YYYY/MM/DD/rollout-*.jsonl` for every configured Claude or Codex instance home. Streams field selection so multi-GB transcripts with screenshots are not allocated. Derives cwd, title (skips setup context blocks), thread count, last activity and git remote. Normalizes `/private/tmp`. Groups into project candidates with an `alreadyImported` flag; may truncate. | `apps/server/src/project/AgentSessionScanner.ts` (1.4k lines), `AgentSessionJson.ts` | L |
| L2 | Agent session import | Imports threads for a project, checks the expected workspace root, returns imported and skipped counts. | `apps/server/src/project/AgentSessionImporter.ts`; RPCs `agentSessions.scan`, `agentSessions.import`; `packages/contracts/src/agentSessions.ts` (sources: claudeAgent, codex) | M |
| L3 | Claude history worker | Out-of-process `getSessionMessages` / `forkSession` (options: dir, includeSystemMessages, upToMessageId). Feeds Claude snapshots and forks. | `apps/server/src/claude-history-worker.ts`, `claudeHistoryWorker.ts`, `cli/claudeHistory.ts`; `ClaudeAdapterV2.ts:758-800` | S |
| L4 | ACP native session import | List, import (deterministic thread) and delete native ACP sessions. | RPCs `server.listAcpRegistrySessions`, `server.importAcpRegistrySession`, `server.deleteAcpRegistrySession`; `provider-acp-registry/src/server/probe.ts` | M |
| L5 | Outside agents (MCP server) | External Claude Code, Codex, ChatGPT and bots connect to T3's `/mcp` with OAuth, pairing codes and scopes (read-only to full access, 30-day grants). This belongs to the MCP/auth slice, not providers; listed only because it was in the requested docs. | `docs/user/outside-agents.md` | — |

## M. Diagnostics and raw provider event logging

| # | Feature | Description | Key paths | Cx |
|---|---|---|---|---|
| M1 | NDJSON event logger | Per-thread files in three streams: native, canonical, orchestration (`_global` for unscoped events).<br>• batching: 1 s window, 1 MiB / 512-record buffer<br>• rotation: 10 MiB × 10 files; retention 512 MiB total and 14 days, checked every 5 min<br>• record budget: 64 KiB, 1,024 fields, depth 16<br>• token deltas and duplicate raw frames dropped (realtime audio/transcript, running snapshots, `turn/diff/updated`); structural summaries keep ids, methods, status and errors<br>• decode failures logged as diagnostic frames | `apps/server/src/provider/EventNdjsonLogger.ts` (831 lines) | M |
| M2 | Per-protocol loggers | Claude SDK query logger, Cursor SDK logger, OpenCode protocol logger, ACP native logger. | `ClaudeAdapterV2.ts:555-760`; `CursorAgentSdk.ts`; `provider-opencode/src/server/adapter.ts`; `provider-acp/src/server/nativeLogging.ts` | S each |
| M3 | Command output hub | `command_output.delta` is never persisted; it streams only to clients viewing the row. | `apps/server/src/orchestration-v2/CommandOutputHub.ts` | S |

## N. Protocol libraries (Rust needs equivalents)

| # | Library | Description | Cx |
|---|---|---|---|
| N1 | `packages/effect-codex-app-server` | JSON-RPC 2.0 over stdio: requests, server notifications, server requests. Typed schema generated from openai/codex `app-server-protocol` JSON schemas (57k lines; namespaces and meta). Error taxonomy. Replay client for tests. In Rust, generate types from the same JSON Schemas. | L |
| N2 | `packages/effect-acp` | ACP client and agent over stdio JSON-RPC. Generated v2 and v1 schemas with a compat layer; protocol version 2. Agent methods: initialize, auth/login|logout, providers/list|set|disable, session/*, mcp/message, plus unused nes/* and document/*. Client methods: request_permission, update, mcp connect/message/disconnect, elicitation create/complete. Extension (`_` prefixed) routing. | L |
| N3 | Muse MSP SDK | Third-party `@muse-code/sdk` 1.4.6. Rust needs a clean-room MSP client. | M |
| N4 | Cursor SDK | Third-party in-process JS SDK. Rust needs either a Node sidecar or a protocol reimplementation. Highest-risk item. | XL |
| N5 | Claude Agent SDK | JS SDK driving the `claude` CLI's stream-json control protocol: initialize, control requests (setModel, setPermissionMode, setMcpServers, applyFlagSettings, usage, interrupt), `canUseTool`, `onUserDialog`, priorities. Rust needs the control protocol reimplemented or a Node sidecar. | XL |
| N6 | OpenCode SDK | HTTP + SSE clients for 1.x (`@opencode-ai/sdk`) and 2.x (custom client with Basic auth and heartbeat watchdog). | M |

## O. Provider-related RPC methods (from `packages/contracts/src/rpc.ts`)

**Provider setup and auth** (`providerSetup.ts`, `provider.ts`, `providerUsageLimits.ts`)
- `provider.auth.start`, `provider.auth.respond`, `provider.auth.complete`, `provider.auth.cancel`, `provider.auth.logout`, `provider.auth.subscribe` (stream)
- `provider.chatgpt.reconnect-profile`, `provider.chatgpt.import-profile`, `provider.chatgpt.handoff.subscribe` (stream), `provider.codex.auth-callback.subscribe` (stream)
- `provider.install.start`, `provider.install.cancel`, `provider.install.subscribe` (stream), `provider.install.remove`
- `provider.consumeResetCredit`, `provider.uploadFeedback`

**Server and provider status**
- `server.getConfig` and `subscribeServerConfig`: providers snapshot stream. Client flags `usageLimitSources` and `usageLimitsCommand` control hub snapshots and `/usage-limits` injection.
- `server.refreshProviders` (`{instanceId?, refreshModels?}`), `server.updateProvider`, `server.updateSettings` (with `providerInstanceMutation`)

**ACP Registry** (`acpRegistry.ts`)
- `server.searchAcpRegistry`, `server.prepareAcpRegistryAgent`, `server.uninstallAcpRegistryManagedBinary`, `server.acceptAcpRegistryUrlAuth`
- `server.listAcpRegistrySessions`, `server.importAcpRegistrySession`, `server.deleteAcpRegistrySession`
- `server.listAcpRegistryProviders`, `server.setAcpRegistryProvider`, `server.disableAcpRegistryProvider`, `server.logoutAcpRegistry`

**Usage** (`usage.ts`): `server.getUsageSummary`, `server.refreshUsageRates`

**Agent sessions** (`agentSessions.ts`): `agentSessions.scan`, `agentSessions.import`

**MCP apps** (served by the Codex adapter `mcpApps`): `mcpApps.callTool`, `mcpApps.toolInfo`, `mcpApps.readResource`, `mcpApps.updateModelContext`

**Legacy provider wire types still in `provider.ts`:** ProviderSession, ProviderSessionStartInput, ProviderSendTurnInput, ProviderInterruptTurnInput, ProviderRespondToRequestInput, ProviderRespondToUserInputInput, ProviderEvent (session / notification / request / error). `model.ts` has no RPCs, only defaults, aliases and option descriptors. `providerInstance.ts` has schemas only.

## P. Complexity roll-up (largest first)

| Area | Cx |
|---|---|
| Claude adapter (SDK control protocol, subagents, workflows, goal parsing, live reconfig) | XL |
| Codex adapter (app-server protocol, goals, async questions, subagents, MCP apps, rollback) | XL |
| Generic ACP adapter and runtime (v1/v2, terminals, MCP bridge, process trees and cgroups) | XL |
| OpenCode 1.x + 2.x (two adapters, server ownership, SSE, permissions) | XL |
| Provider session manager | XL |
| Cursor (SDK dependency) | XL due to the SDK, otherwise L |
| Antigravity (install, auth, profiles, ACP flavor, usage) | XL combined |
| Adapter event model and capabilities (A4/A5) | XL / M |
| Instance and status registries (B2, B7) | L |
| Maintenance resolver | L |
| ChatGPT OAuth plus handoff | L |
| ACP Registry install and driver | L |
| Grok, Pi, Muse adapters | L each |
| Usage aggregation service | L |
| Agent session scanner | L |
| Text generation (shared plus per-provider runners) | M + M×9 |
| Usage limits (per provider plus hub plus reset credits) | M each |
| Event NDJSON logger | M |
| Model manifest and catalogs | M |
| Everything else in tables A–O | S–M |
