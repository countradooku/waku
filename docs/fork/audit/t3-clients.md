# T3 Code client surfaces: audit for a Waku (Rust/GPUI) parity fork

Scope: `apps/web` (React 19 + Vite + TanStack Router + Effect atoms), `apps/desktop` (Electron 44 shell), `apps/mobile` (Expo / React Native), `packages/client-runtime` (shared headless client), all of `docs/user/*`, and the requested `docs/internals/*` pages.

All paths are relative to the T3 repo root (`.../t3code`).

**Surface legend:** W = web (local `npx t3` and hosted app.t3.codes), D = desktop (Electron wraps the same web bundle and adds the shell), M = mobile (iOS/Android).

**Complexity legend** (effort to reimplement in a native Rust GPUI client, assuming the T3 server or an equivalent backend already exists):

| Grade | Effort |
|---|---|
| S | ≤ 1 engineer-week |
| M | 1–3 weeks |
| L | 1–2 months |
| XL | > 2 months, or a whole sub-product |

## 0. Architecture facts that drive every estimate

- **Every client is a thin view over one or more "environments".** An environment is a T3 server owning providers, PTYs, git, files, browser tabs, devices and the event-sourced DB.
  - Clients speak a typed Effect-RPC protocol over WebSocket (`packages/contracts/src/rpc.ts`, `WS_METHODS` ~190 methods, plus `orchestration.*`), plus a few bounded HTTP endpoints for history paging and snapshots.
  - The desktop renderer is *not* privileged. It attaches to its own bundled server over the same WebSocket. See `docs/internals/terminal-runtime.md` and `docs/internals/remote.md`.
- **Implication for Waku.** "Feature parity with T3 Code" is mostly parity with the T3 **server protocol and its features**. The client is a large but mostly dumb projection.
  - **Strategy 1:** a Rust GPUI client speaking T3's protocol. This needs a Rust port of `packages/client-runtime` (connection driver/supervisor, shell + thread projection reducers, ~57k LOC TS incl. tests) and Rust types for `packages/contracts` (Effect Schema, versioned `ORCHESTRATION_PROTOCOL_VERSION=2`).
  - **Strategy 2:** Waku's own backend, which must re-implement the server half as well.
  - Decide this first; the complexity grades below are client-side only.
- **`packages/client-runtime` is the shared brain for web and mobile.** Main areas:
  - `connection/`: multi-route driver, supervisor with preflight and cooldown, registry, credential/profile stores, compatibility check, wakeups.
  - `rpc/`: WS client with 5 s ping and 15 s timeout (`rpc/protocol.ts`), session, HTTP.
  - `state/`: shell reducer, thread detail, history paging/merge, orchestration v2 projection, pull requests, VCS, terminal sessions, usage, device, MCP apps.
  - Smaller modules: `voice-input/`, `work-log/` (tool-call presentation), `device/` (H.264/MJPEG stream decoding, 3D phone scene), `mcpApps/host.ts`, `relay/` (T3 Connect).
- **Protocol gate.** Client and server must agree on `ORCHESTRATION_PROTOCOL_VERSION` (2). `/ws?orchestrationProtocol=2` returns HTTP 426 on mismatch (`packages/client-runtime/src/connection/compatibility.ts`).
- **Web app routes** (`apps/web/src/routes/`):

  | Route | Purpose |
  |---|---|
  | `_chat.index` | Home |
  | `_chat.new` | New-thread composer |
  | `_chat.draft.$draftId` | Draft thread |
  | `_chat.$environmentId.$threadId` | Thread view |
  | `_chat.pull-requests` | PR inbox and review (2.5k LOC) |
  | `usage` | Usage page |
  | `welcome` | Onboarding |
  | `pair` | Pairing |
  | `connect` | T3 Connect CLI auth |
  | `connect-agent` | MCP OAuth approval for outside agents |
  | `projects.$projectKey` | Redirects to project settings |
  | `settings.*` | 14 settings pages |

---

## 1. Feature catalogue

Each table row lists: Feature | Description | Surfaces | Key source paths | Complexity.

### 1.1 Providers & adapters (UI side)

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Model picker | Popover with a provider sidebar (Favorites, per-instance, "Limited"/"Not ready"/"Unavailable" states) and fuzzy model search. Left/Shift+Tab moves to the provider list; mod+shift+↑/↓ switches provider; mod+1..9 jumps to a model. | W D M | `apps/web/src/components/chat/ProviderModelPicker.tsx`, `ModelPickerContent.tsx`, `ModelPickerSidebar.tsx`, `ModelListRow.tsx`, `modelPickerSearch.ts`, `modelPickerKeys.ts`, `apps/web/src/modelOrdering.ts`, `modelPickerVisibility.ts`, `providerModels.ts` | M |
| Multi-model fan-out | Shift-click several models in a *new* thread; each starts its own thread and worktree with the same prompt (git projects only). | W D | `ProviderModelPicker.tsx` ("Choose models"), `docs/user/thread-sidebar.md#start-in-the-background` | S |
| Favorites and per-provider model preferences | Starred models, hidden/ordered models, remembered last model and options for new threads. | W D M | `ClientSettingsSchema.favorites` / `providerModelPreferences` in `packages/contracts/src/settings.ts`, `apps/web/src/modelSelection.ts` | S |
| Traits picker | Reasoning effort (Off … Ultrathink, provider-specific), Fast/Ultrafast speed tier, agent-specific ACP options (approval mode, etc.). "Unset" means the provider default. Increase/decrease effort commands wrap around. | W D M | `components/chat/TraitsPicker.tsx`, `TraitsSpeed.tsx`, `composerProviderState.tsx` | M |
| Access / permission mode | Supervised, Auto-accept edits, Auto (provider AI review), Full access. Set per thread; default for new threads comes from Settings and can be overridden per project. Per-provider availability (Pi has no Auto; Muse/Grok are limited). | W D M | `components/chat/runtimeModeConfig.ts`, `docs/user/permission-modes.md`, command `thread.runtime-mode.set` | S |
| Plan / Build interaction mode | `/plan` and `/default` toggle, `thread.interaction-mode.set`. Proposed-plan card offers Implement / Refine / save plan to workspace. A legacy "Plan mode" toggle exists in Settings. | W D M | `ProposedPlanCard.tsx`, `ComposerPlanFollowUpBanner.tsx`, `apps/web/src/proposedPlan.ts` | M |
| Provider instances | Several instances per driver (separate accounts, env vars marked sensitive, binary path, launch args, `CODEX_HOME` + shadow home, `CLAUDE_CONFIG_DIR`), each with an accent color and display name. | W D (M: accounts only) | `components/settings/ProviderInstanceCard.tsx`, `AddProviderInstanceDialog.tsx`, `ProviderSettingsForm.tsx`, `CodexSetupSection.tsx`, `ProviderAccentColorPicker.tsx` | L |
| Provider sign-in flows | Browser sign-in with paste-back of the return URL for remote hosts; in-app **terminal** sign-in (`ProviderAuthTerminal`); ChatGPT "Connect" handoff; Cursor and Antigravity sign-in; change account / sign out. Desktop receives auth callbacks via a deep link. | W D M (M: "Provider accounts") | `ProviderAuthenticationSection.tsx`, `ProviderAuthTerminal.tsx`, `ProviderAuthCallbackCoordinator.tsx`, `ChatGptConnectionButton.tsx`, `ChatGptAccountPicker.tsx`, RPC `provider.auth.*` | L |
| Provider install and update | Managed installs (Codex runtime, Antigravity, ACP registry agents), install progress, "Update now" via the owning package manager, "Update all" across environments, compatibility warnings. | W D M | `ProviderSetupSection.tsx`, `ProviderUpdatesAction.tsx`, `ProviderUpdateLaunchNotification.tsx`, `SidebarProviderUpdatePill.tsx`, RPC `provider.install.*`, `server.updateProvider` | M |
| ACP Registry | Search the registry and add an agent; add a local ACP command; list and import native sessions; executable override; auth method. | W D | `AcpRegistrySearchStep.tsx`, `AcpSessionManagementSection.tsx`, `docs/user/providers-acp.md`, `server.*AcpRegistry*` RPCs | M |
| Custom models | Add unlisted model IDs per instance; server-side rename. | W D | `CustomModelEditor.tsx`, `customModelEditor.logic.ts` | S |
| Provider status banners | Not installed, signed out, version incompatible, rate-limited. "Open provider setup" deep link. | W D M | `ProviderStatusBanner.tsx`, `components/settings/providerStatus.ts` | S |
| Usage-limit UX | "Limited" thread state; **Resume at reset** (scheduled continuation); **Snooze until reset**; auto-resume / auto-snooze settings; Claude "Compact" chip when the prompt cache expired; `/usage-limits` inline panel above the composer. | W D M | `UsageLimitRecoveryBanner.tsx`, `ComposerUsageLimits.tsx`, `useUsageLimitsPanel.ts`, `docs/user/providers-claude.md` | M |
| Context window meter | Token usage ring with "Compact context" action; legacy toggle. | W D | `ContextWindowMeter.tsx`, `ContextWindowMeter.logic.ts`, `apps/web/src/lib/contextWindow.ts` | S |
| Provider switch / handoff | Switching provider mid-thread transfers budgeted context (`provider.switch`). Restart agent session from the palette. | W D M | `docs/user/portable-handoffs.md`, `CommandPalette.tsx` ("Restart agent session") | S (UI) |
| Codex extras | Async questions panel, app-access approvals (once / session / always), `/feedback` upload, "Manage usage" (ChatGPT), banked reset credits ("Use reset"). | W D M | `ComposerPendingUserInputPanel.tsx`, `ComposerPendingApprovalActions.tsx`, `ChatGptUsageButton.tsx`, RPC `provider.uploadFeedback`, `provider.consumeResetCredit` | M |
| Provider extension dialogs (Pi) | Pi select/confirm/input/editor dialogs rendered in the composer. | W D M | `docs/user/providers-pi.md`, pending-user-input panel | S |

### 1.2 Orchestration and session model

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Send / stop / interrupt | Send button morphs: Submit, Steer, Queue, Interrupt (empty draft), Resume, Compact-and-send, "Preparing worktree", "Connecting". `thread.stop` has no default binding. | W D M | `components/chat/ComposerPrimaryActions.tsx`, `composerSubmission.ts`, `apps/web/src/components/chat/useChatShortcuts.ts` | M |
| Follow-up behavior: queue vs steer | Per-client default (Settings → General). mod+Enter does the opposite action; mobile uses long-press. Steer is offered only when the provider supports it; otherwise the orchestrator interrupts and restarts. | W D M | `ChatComposer.tsx`, `ClientSettings.followUpBehavior`, `docs/user/composer.md#send-while-the-agent-is-working` | S |
| Server-persisted message queue | Queued rows above the composer with image thumbnails. Drag-reorder by handle or arrow keys, edit in place (pencil, checkmark, Cancel), promote to steer, remove. Held after stop or restart; "Resume" in an empty composer. `thread.steerQueuedMessage` (mod+shift+Enter) and `thread.editQueuedMessage` (alt+↑). | W D M | `QueuedRunsControl.tsx`, `queuedMessageEdit.ts`, `useQueuedRunEdit.ts`, `packages/client-runtime/src/state/threadWorkflows.ts` (`ThreadQueueWorkflowState`), command `queue.resume` | M |
| Start in background / send and new thread | mod+Enter in a draft (or mod+alt+Enter) submits and opens a fresh draft. | W D | `composer.sendBackground`, `composer.sendAndNewThread` | S |
| Goals (`/goal`) | Persistent goal row above the composer with progress (Codex and Claude); pause / resume / clear. | W D M | `docs/user/composer.md#goals`, `ComposerBannerStack.tsx` | S |
| Approvals | Inline approval panel: command, file change, file read, app access, permission request. Approve, Decline, "Always allow this session", "More approval options". | W D M | `ComposerPendingApprovalPanel.tsx`, `ComposerPendingApprovalActions.tsx`, `usePendingThreadRequests.ts`, `packages/client-runtime/src/state/threadRequests.ts` | M |
| Questions (user-input requests) | Multi-question wizard (Next / Previous / Submit answers), custom answers with **file attachments**, dismiss without answering, unanswered questions survive reconnect. | W D M | `ComposerPendingUserInputPanel.tsx`, `apps/web/src/pendingUserInput.ts`, `questionAttachments.ts`, `docs/user/question-attachments.md` | M |
| Secret requests | Agent asks for a secret; a secure card answers it without persisting the value in the transcript. | W D M | `components/chat/SecretRequestCard.tsx`, `packages/client-runtime/src/secretRequest.ts`, RPC `secrets.answerRequest` | S |
| Subagent views | Delegated tasks (`delegate_task`) become read-only child threads. ProviderSubagentBar shows model, effort and a live status timer; "Open parent thread"; "Subagent of" chips; stopping the parent stops children. Antigravity shows batched subagents. | W D M | `ProviderSubagentBar.tsx`, `SubagentTooltipContent.tsx`, `agentSpawnSummary.ts`, `packages/client-runtime/src/state/threadSubagents.ts`, `subagentDisplay.ts`, `subagentRuntime.ts` | L |
| Workflows (orchestrator scripts) | The T3 Orchestrator runs a JS workflow script that spawns agent groups in phases. The UI shows a "Workflow opening" block (plan, member agents per phase, folded script) and a lineage row; "Stop workflow". | W D (M likely) | `WorkflowOpeningBlock.tsx`, `ThreadLineageWorkflowRow.tsx`, RPC `orchestration.getWorkflowScript`, `orchestration.stopWorkflow` | L |
| Thread relationships | Fork from a response ("Fork from this response"), "Open fork", "Merge back to source conversation", parent / related / previous agents, "Context transfer" lineage. | W D | `ThreadRelationshipsControl.tsx`, `ThreadRelationshipIcon.tsx`, `packages/client-runtime/src/state/threadRelationships.ts`, command `thread.fork` | M |
| Orchestration v2 lifecycle rows | Inline system rows: run created/interrupted, context compaction (start / done / failed / stopped), context handoff pending, worktree setup, "Sent by another agent", "Sent by automation", "Steered the active turn", "Originally queued, then promoted". | W D M | `components/chat/V2LifecycleRow.tsx`, `V2LifecycleRow.logic.ts`, `TimelineSystemDivider.tsx`, `apps/web/src/lib/orchestrationV2Timeline.ts`, `packages/client-runtime/src/state/turnItemPresentation.ts` | M |
| V2 item inspector | Debug/inspect panel for a turn item (raw item, support matrix). | W D | `V2ItemInspector.tsx`, `apps/web/src/state/v2ItemSupport.ts`, `client-runtime/state/itemSupport.ts` | S |
| Restart continuation | Threads resume after server update or crash (setting). UI shows "Run interrupted" / resume state. | W D M | `docs/user/updating.md#before-you-update`, setting `continueThreadsAfterServerUpdate` | S (UI) |
| Background-work awareness | "Working" status covers monitors, background shells, PR watches, scheduled continuations. Elapsed timers. | W D M | `AgentElapsed.tsx`, `ThreadStatusIndicators.tsx`, `ComposerActivityStatus.tsx` | S |
| Thread automations panel | Per-thread list of automations (scheduled tasks and PR watches) with run / pause and "Manage scheduled tasks". | W D | `components/chat/ThreadAutomationsPanel.tsx` | S |
| Running-thread keep-alive | Desktop keeps running threads' subscriptions alive (no power-save blocker). | D | `components/desktop/RunningThreadKeepAlive.tsx`, `state/threads.ts:runningThreadKeepAliveAtom` | S |

### 1.3 Checkpoints, rewind, git, worktrees, source control

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Edit from here (rewind) | Rewinds the conversation to before a user message. Options: "Revert and keep changes" or "Revert files too" (worktree threads only; refused if the directory is shared). The prompt and its attachments return to the composer. Shown only when the provider supports rewind. | W D | `MessagesTimeline.tsx` ("Edit from here"), commands `checkpoint.rollback*`, `docs/user/composer.md#edit-an-earlier-prompt` | M |
| Turn diffs and checkpoints | Each turn ends in a hidden git ref. "Changed files" tree per turn; diff only the turn's own work; full thread diff. | W D M | `ChangedFilesTree.tsx`, `DiffStatLabel.tsx`, `apps/web/src/lib/turnDiffTree.ts`, `checkpointDiffState.ts`, RPC `orchestration.getTurnDiff`, `getFullThreadDiff` | M |
| Branch toolbar | Under the composer for new threads (and persistent with "Composer context"): environment / host selector, workspace mode (current checkout, **New worktree**, previous worktree), branch picker with paginated refs and create-ref, base-branch picker, "New thread in this worktree", auto-balance host. | W D M | `BranchToolbar.tsx`, `BranchToolbarBranchSelector.tsx`, `BranchToolbarEnvModeSelector.tsx`, `BranchToolbarEnvironmentSelector.tsx`, `BranchPicker.tsx`, `WorktreeBaseBranchPicker.tsx`, `PreviousWorktreeItemContent.tsx`, `apps/web/src/state/paginatedBranches.ts` | M |
| Worktree setup | Progress card ("Setting up worktree…", setup script output, failure, cancel); submodule policy; start from origin. | W D M | `WorktreeSetupCard.tsx`, `useWorktreeSetup.ts`, `client-runtime/src/worktreeSetup.ts`, RPC `subscribeWorktreeSetup` | S |
| Git actions control | Commit (AI-generated message), Push, Pull, create PR (generated title and description), Initialize Git, Publish repository (host, visibility), copy branch, open PR. Gated by `source-control:write`. | W D M (M: Git overview) | `GitActionsControl.tsx`, `GitActionsControl.logic.ts`, `apps/web/src/lib/sourceControlActions.ts`, `client-runtime/state/vcsAction.ts`, RPC `git.runStackedAction`, `vcs.*` | M |
| Project scripts / actions | Per-project actions (from `t3.json` or settings), run in a terminal, keybinding `script.{id}.run`, run-on-settle, import from `t3.json`. | W D | `ProjectScriptsControl.tsx`, `projectScriptEditor.tsx`, `useProjectScriptEditing.ts`, `components/settings/ProjectActionsSettings.tsx`, `apps/web/src/projectScripts.ts`, `lib/projectScriptKeybindings.ts` | M |
| Open in editor | Pick and remember a favorite editor (Cursor, Trae, Kiro, VS Code (+Insiders), VSCodium, Zed, Antigravity, JetBrains family, File Manager); `editor.openFavorite` (mod+O); remote editor probing on desktop. | W D | `chat/OpenInPicker.tsx`, `OpenInPickerShortcut.ts`, `packages/contracts/src/editor.ts`, RPC `shell.openInEditor` | S |
| Pull Requests page | Cross-environment PR inbox. Filters: state, involvement, provider, host, project, drafts. Sort; Shift quick actions; drag-to-close many; stack badges; checks popover; linked threads. | W D M | `routes/_chat.pull-requests.tsx`, `components/pullRequest/PullRequestListRow.tsx`, `PullRequestListFilters.tsx`, `pullRequestList.logic.ts`, `PullRequestSpeedActions.tsx` | L |
| PR detail / review | Tabs: Summary, Timeline, Code (diff with viewed-file ticks, line comments, find), Checks, Guide.<br>Actions: edit title / description / comments, reactions, labels, reviewers, approve / request changes / submit review, merge (method), auto-merge, update branch (rebase), close / reopen with comment, draft ↔ ready, delete branch, revert PR, approve fork workflows, check out locally or in a worktree, "Fix check" / "Fix findings" / "Ask agent" into a thread. | W D (M: read and review) | `components/pullRequest/*` (~60 files: `PullRequestDetailPanel.tsx`, `PullRequestCodeTab.tsx`, `PullRequestReviewForm.tsx`, `PullRequestTimelineTab.tsx`, `PullRequestSummaryTab.tsx`, `PullRequestChecksPopover.tsx`, `usePullRequestActions.ts`, `usePullRequestFilesViewed.ts`), RPC `pullRequests.*` (~30 methods) | XL |
| GitHub stacks | Stack header and layers navigation, Merge stack, Rebase stack. | W D | `PullRequestStackHeader.tsx`, `PullRequestStackLayers.tsx`, `PullRequestStackMenu.tsx`, `usePullRequestStack.ts`, `pullRequestStackSnapshot.ts` | M |
| Review guides (beta) | AI-written tour of a PR: stops, "Look closely", excerpts with line-select-to-comment or ask-agent, "Finish review"; write / rewrite guide with progress. | W D M | `components/pullRequest/guide/*` (`GuideTour.tsx`, `GuideStop.tsx`, `GuideRail.tsx`, `guideRunStore.ts`, `PullRequestGuideRunCoordinator.tsx`), `client-runtime/src/reviewGuideTour.ts` | L |
| Linked PRs per thread | Multiple PRs per thread, auto-linked by branch; link / unlink dialog; watch PR (eye icon); thread badges with stack count. | W D (M: view) | `ThreadPullRequestsPanel.tsx`, `LinkPullRequestDialog.tsx`, `PullRequestThreadLinks.tsx`, `ThreadDetailsPrRows.tsx`, `useThreadLinkedPullRequest.ts`, `hooks/usePullRequestLinking.ts`, `pullRequestLinkContextMenu.ts` | M |
| Source control hosts | GitHub (token / GH_TOKEN / gh multi-account), GitLab, Forgejo/Gitea, Bitbucket, Azure DevOps, GitCafe. Rescan; per-host account picker; GitHub sharing across environments ("Read PRs" / "Read and act"). | W D (M: some) | `components/settings/SourceControlSettings.tsx`, `SourceControlHostSettings.tsx`, `GitHubAccountSettings.tsx`, `GitHubTokenSettings.tsx`, `GitHubRoutingSettings.tsx`, `client-runtime/src/sourceControlClients.ts` | M |
| Clone / new project / publish | Add project via clone (hosted repo picker or URL, background clone with toast progress, cancel, retry); New project (scaffolded repo in `~/.t3/projects`, optional private GitHub repo); Publish repository. | W D M | `CommandPalette.tsx` add-project flow, `ProjectCloneToastCoordinator.tsx`, `hooks/useNewProject.ts`, RPC `sourceControl.*`, `projectClone.*` | M |
| Worktree storage cleanup | Policies (inactive days, settled, merged, unchanged, deleted threads), keep-local-changes level, worktree location, "Delete now" with report. | W D M | `components/settings/StorageSettings.tsx`, `apps/web/src/worktreeCleanup.ts` | S |
| Auto pull / git fetch interval | Keep the default branch current; fetch interval. | W D M | `ProjectDefaultsSettings.tsx`, Source Control settings | S |
| Branch naming | Static prefix, semantic prefix, or custom instructions. | W D | `components/settings/BranchNamingSettings.tsx` | S |

### 1.4 Composer inputs

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Rich-text Markdown composer | Tiptap / ProseMirror editor that keeps Markdown markers literally ("styled as you type").<br>Supports lists, task lists, quotes, headings, rules, fenced code blocks with a language picker and highlighting (disabled for huge blocks). Enter, Shift+Enter, Tab and Backspace have block semantics. Plain-mode toggle. Byte-identical Markdown round-trip; custom undo grouping. | W D | `components/ComposerPromptEditorTiptap.tsx` (1.9k LOC), `composer-rich-text*.ts`, `composer-list-continuation.ts`, `composer-code-block.ts`, `composer-code-languages.ts`, `composer-undo-grouping.ts`, `ComposerCodeBlockNodeView.tsx`, `docs/internals/composer-editors.md` | XL (a GPUI rich editor is the hardest single piece) |
| Inline context chips | Canonical links `t3-context://v1/<kind>/<id>` inside prose, for: image, file, terminal excerpt, picked page element, preview annotation, review comment, PR, thread, skill, file mention.<br>Chips are moved by cut/paste and deleted like characters; hover details; unresolved dashed state. Records travel in `message.context.records`; the provider gets markers plus a `<t3_context>` envelope. | W D (M: read-only labels) | `components/ContextChip.tsx`, `contextChipParts.tsx`, `composerContextPresentation.tsx`, `contextPresentationRegistry.ts`, `apps/web/src/lib/composerContextRecords.ts`, `packages/shared/src/composerContextReferences.ts`, `packages/contracts/src/composerContext.ts`, `docs/internals/composer-context-references.md` | L |
| Chip clipboard | Copy and paste chips across drafts, threads and environments. Uses a custom MIME `web application/x-t3-context-fragment+json`; binaries are re-fetched from the source environment. | W D | `composerInlineTokenPaste.ts`, `apps/web/src/markdown-clipboard.ts`, `lib/copiedComposerAttachments.ts`, `packages/contracts/src/composerContextClipboard.ts` | M |
| Trigger menus | `/` commands: `/model`, `/plan`, `/default`, provider commands, skills as `/skill:x`, `/compact`, `/goal`, `/usage-limits`, `/feedback`, `/logout`.<br>`$` skills. `@` files (path search) and threads. `#` PRs (recent list, numeric filter, text search, linked first). | W D M | `components/chat/ComposerCommandMenu.tsx`, `composerSlashCommandSearch.ts`, `useComposerTriggerState.ts`, `packages/shared/src/composerTrigger.ts`, `apps/web/src/providerSkillSearch.ts`, `client-runtime/state/composerPathSearch.ts` | M |
| Attachments | Up to 100 files per message: images ≤10 MiB (80 MiB total), others ≤50 MiB. Uploads start on add (retry / remove); HEIC → JPEG; image resize; drag-drop and paste; drop files on sidebar thread rows; drop a folder. Image thumbnail shelf plus image chips. | W D M | `composerAttachmentFiles.ts`, `ComposerImageThumbnail.tsx`, `apps/web/src/lib/attachmentUploadQueue.ts`, `attachmentUploadState.ts`, `imageCompression.ts`, `folderDrop.ts`, `workspaceFileDrop.ts`, `sidebarPendingFileDropStore.ts`, RPC `attachments.createUploadUrl` | M |
| Large paste → text attachment | ≥32 KiB pastes become a text-file attachment; mod+Shift+V (desktop "Paste as Text" menu) keeps them inline. | W D M | `client-runtime/src/textPaste.ts`, `apps/web/src/lib/desktopPasteAsText.ts` | S |
| Message length validation | 120,000-character limit, counted again after citation expansion. | W D M | `ComposerPromptLengthValidation.tsx` | S |
| Assistant citations ("Cite in composer") | Select text in an assistant reply, use the selection toolbar to quote it with an optional comment. The chip navigates back to the source (text selectors, virtual-row-aware highlight). | W D (M: display) | `AssistantSelectionToolbar.tsx`, `AssistantCitationChip.tsx`, `AssistantCitationCommentEditor.tsx`, `AssistantCitationSource.tsx`, `useAssistantCitationTarget.ts`, `lib/assistantTextSelection.ts`, `lib/assistantCitationNavigation.ts`, `packages/shared/src/assistantCitations.ts`, `docs/internals/assistant-citations.md` | M |
| Prompt history recall | ↑/↓ in an empty composer walks previously sent prompts (visual-line aware). | W D | `components/chat/composerPromptHistory.ts` | S |
| Prompt stash | mod+S stashes a draft with attachments and chips; stash menu; restore; 24 h upload retention. | W D | `apps/web/src/promptStashStore.ts`, `ComposerStashMenu.tsx`, `ComposerStashBadge.tsx` | S |
| Drafts | Server-independent drafts per thread and draft route; discard with undo; "Unsent draft" sidebar rows; legacy migration. | W D M | `apps/web/src/composerDraftStore.ts` (persistent), `lib/discardComposerDraft.ts`, `pendingDraftWork.ts` | M |
| Send shortcut modes | Enter sends / mod+Enter for multiline / always mod+Enter; collapse composer on scroll. | W D | `useComposerMultilinePrompt.ts`, `composerScrollGesture.ts` | S |
| Prompt links | `app.t3.codes/new?prompt=&repo=&provider=&model=&pr=` opens a draft (never auto-runs); thread link with `?prompt=`; invisible-character stripping with a warning; "Copy prompt link (with repo and model)". | W D M | `components/promptLinks/*`, `client-runtime/src/promptLinks.ts`, `docs/user/prompt-links.md` | S |
| Voice input | On-device transcription (iOS 26 SpeechAnalyzer), inserted at the selection; mic ranking. | M only | `packages/client-runtime/src/voice-input/*`, `apps/mobile/src/native/voiceTranscription.ios.ts`, `docs/internals/voice-input.md` | M (could use OS STT on desktop) |
| SnapShot attachment | Global-shortcut window capture with accessibility tree, attached to the draft; details popover. | D | `SnapShotCoordinator.tsx`, `SnapShotAttachmentDetails.tsx`, `lib/snapShot*.ts`, `apps/desktop/src/snapShot/*` | XL (per-OS capture) |
| Thread-as-context | Drag sidebar threads onto the composer, or `@thread`. Cross-environment drop attaches a transcript export file. | W D M | `components/chat/threadContextDrag.ts`, `ThreadContextDragGhost.tsx`, `composerThreadImport.ts`, `lib/threadContextAttachment.ts`, `ThreadContextChip.tsx` | M |
| Typing guard / focus redirect | Keystrokes typed while focus is elsewhere are redirected to the composer; the terminal-start race keeps composer focus. | W D | `composerTypingGuard.ts`, `useComposerTypingGuard.ts`, `composerFocusRedirect.ts`, `docs/user/keyboard-focus.md` | S |

### 1.5 Transcript content types

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Timeline row model | Row kinds: `message`, `assistant-meta`, `thinking`, `work`, `work-live`, `work-toggle`, `working`, `event`, `turn-fold`, `attempt-fold`, `context-compaction`, `proposed-plan`, `html-render`, `mcp-app`, `worktree-setup`. Derived from the v2 projection. | W D M | `components/chat/MessagesTimeline.logic.ts` (2.3k LOC), `MessagesTimeline.tsx` (5.7k LOC), `hooks/useIncrementalTimelineEntries.ts` | L |
| Turn item types (v2) | `user_message`, `assistant_message`, `reasoning`, `command_execution`, `file_change`, `file_search`, `web_search`, `dynamic_tool`, `todo_list`, `proposed_plan`, `approval_request`, `user_input_request`, `secret_request`, `notification`, `system_notice`, `error`, `compaction`, `checkpoint`, `subagent`, `handoff`, `thread_created`, `runtime_exit`, plus background command / task and delegated task. | W D M | `packages/contracts/src/orchestrationV2.ts`, `client-runtime/src/work-log/*` | — |
| Markdown | GFM tables, task lists, GitHub alerts, footnotes; incremental streaming parser; links to files (open in the file viewer or editor), threads, PRs (with link/unlink context menu), citations, context chips; images gallery; video player; copy-as-Markdown. | W D M | `components/ChatMarkdown.tsx` (3.5k LOC), `apps/web/src/markdown-incremental.ts`, `markdown-links.ts`, `markdown-clipboard.ts`, `chat/markdownImageGallery.ts`, `chat/MarkdownThreadLink.tsx` | L |
| Code blocks | Shiki (via `@pierre/diffs` highlighter) with **incremental grammar-state highlighting**; copy; expand; language icons. | W D M | `components/chat/HighlightedCodeLines.tsx`, `HighlightedTokens.tsx`, `apps/web/src/lib/incrementalHighlighting.ts`, `lib/syntaxHighlighting.ts` | M (tree-sitter or syntect in Rust) |
| Mermaid diagrams | Rendered inline. | W D | `components/chat/MermaidDiagram.tsx` (`mermaid` dep) | M (no native mermaid; could shell out or render to SVG) |
| Tool calls / work log | Grouped "Ran 2 commands and sent messages to 3 threads" summaries; expand each call; live command output via `orchestration.subscribeCommandOutput` (tail only); exit codes; "Open diff" for file changes; shell-wrapper shortening. | W D (M: own UI) | `WorkLog.tsx`, `CommandBlock.tsx`, `ShellCommandBlock.tsx`, `CommandOutputPanel.tsx`, `client-runtime/src/work-log/presentation.ts`, `toolPresentation.ts`, `commandLabel.ts`, `t3ToolSummary.ts`, `state/commandOutput.ts`, `docs/user/activity-log.md` | L |
| Thinking / reasoning | Collapsible "Thinking" / "Thought" rows. | W D M | `MessagesTimeline.tsx` | S |
| Todo / tasks badge | Live task list ("Tasks" badge above the composer: Pending / Running / Completed). | W D M | `ComposerTasksBadge.tsx` | S |
| Proposed plans | Card with expand / collapse, copy, save to workspace, Implement / Refine. | W D M | `ProposedPlanCard.tsx` | S |
| HTML renders ("visual replies") | Agent-published self-contained HTML shown above the reply in a sandboxed iframe, themed to the current palette, with zoom and full screen. | W D M | `HtmlRenderFrame.tsx`, `HtmlRenderDialog.tsx`, `hooks/useHtmlRenderTheme.ts`, `docs/user/html-renders.md` | L (needs an embedded webview, e.g. wry) |
| MCP Apps | Interactive MCP-App iframes replace tool calls (Codex). Host bridge for tool calls (with consent), messages, model context; full screen; anti-clickjacking 5 s rule. | W D M | `McpAppFrame.tsx`, `client-runtime/src/mcpApps/host.ts`, `state/mcpApps.ts`, RPC `mcpApps.*` | L (webview) |
| Images and video in messages | Gallery preview (zoom), video player, right-click save / copy path, tool-output images. | W D M | `ExpandedImageDialog.tsx`, `ZoomableImage.tsx`, `components/media/*`, `lib/videoFirstFrame.ts` | M |
| Question attachments | Files attached to answers render in history. | W D M | `docs/user/question-attachments.md` | S |
| Citations in user messages | Quote chips with comment; navigate to the source. | W D (M: display) | see 1.4 | — |
| Message actions | Copy (Markdown with links in place of chips), Copy prompt link, Edit from here, Fork from this response, "Show full message" collapse for long user messages. | W D M | `MessageCopyButton.tsx`, `MessagesTimeline.tsx` | S |
| Errors and notices | Thread error banner (dismiss), provider runtime-exit, usage-limit notices, "Synthetic" messages, interrupted runs. | W D M | `ThreadErrorBanner.tsx`, `UsageLimitRecoveryBanner.tsx` | S |
| History paging | "Load earlier turns" with a bounded cold open (75 rows, ~1 MiB) and HTTP pages. | W D M | `client-runtime/src/state/threadHistoryController.ts`, `threadHistoryHttp.ts`, `threadHistoryMerge.ts`, `boundedThreadSnapshotHttp.ts` | M |
| Find in thread | mod+F searches the whole thread server-side (including unloaded history and plans), with Enter / Shift+Enter navigation and highlights. | W D | `ThreadFindBar.tsx`, `ThreadFindProvider.tsx`, `threadFind*.ts`, `useThreadFind*.ts`, RPC `orchestration.searchThread(Stream)` | M |
| Timeline minimap and scroll anchoring | Per-turn minimap of user and assistant pairs; follow-tail mode; prev/next turn; anchored scroll on prepend. | W D | `timelineMinimapItems.ts`, `timelineMinimapWheel.ts`, `timelineScrollAnchoring.ts`, `timelineScrollTarget.ts`, `pageScrollController.ts` | M |
| Legacy-format messages | In-memory upgrade of `<terminal_context>` and similar trailing blocks. | W D M | `packages/shared/src/composerContextLegacy.ts` | S |

### 1.6 Terminal / PTY

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Terminal drawer | Per-thread terminals in a bottom drawer: tabs, split horizontally and vertically (mod+D, mod+shift+D), new (mod+N in terminal), close (mod+W), toggle (mod+J); persistent across thread switches; also a right-panel "Terminal" surface. | W D M | `components/ThreadTerminalDrawer.tsx` (1.9k LOC), `PersistentThreadTerminals.tsx`, `components/terminal/useThreadTerminalActions.ts`, `apps/web/src/terminalUiStateStore.ts`, `lib/terminalFocus.ts`, `lib/terminalCloseShortcut.ts`, `lib/terminalCloseConfirm.ts` | L |
| Terminal emulator | libghostty-vt compiled to **WASM**, Canvas-2D renderer batching style runs. Handles IME, selection, links, cursor blink, Nerd-font symbols; React stays out of the render loop. Scrollback keys (Shift+PgUp/PgDn, mod+shift+Home/End), select-all, "Jump to latest". | W D (M: native Ghostty) | `apps/web/src/terminal/ghostty/{runtime,core,renderer,surface,keyCodes}.ts`, `native/libghostty-vt/VERSION`, `docs/internals/terminal-runtime.md`, `docs/user/terminal.md` | L (Waku can link libghostty-vt natively or use alacritty_terminal) |
| Server-owned PTYs | Attach / observe / write / resize / clear / restart / close. 5,000 lines / 8 MiB retention server-side, 512 KiB client buffer. Replays detach the PTY writer to avoid query junk. | W D M | RPC `terminal.*`, `subscribeTerminalEvents`, `client-runtime/state/terminal*.ts` | M |
| Terminal context to composer | Select lines and insert a "Terminal excerpt" chip. | W D | `TerminalContextInlineChip.tsx`, `ComposerPendingTerminalContexts.tsx`, `lib/terminalContext.ts` | S |
| Terminal links | File paths and URLs clickable; open URLs in the in-app preview. | W D | `apps/web/src/terminal-links.ts`, `components/preview/openTerminalLinkInPreview.ts` | S |
| Provider-sign-in terminals | Terminals launched with provider env for login flows (setup wizard). | W D M | `ProviderAuthTerminal.tsx` | S |
| Settle closes idle terminals | Thread settle closes idle-prompt terminals and keeps their output. | W D | server behaviour, `docs/user/thread-sidebar.md` | — |

### 1.7 Files, diff, review

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Diff panel | Turn, thread and branch diffs with `@pierre/diffs` (split or stacked, word wrap, hide whitespace, collapse all, file tree), Web-Worker highlighting pool, lazy file loading for huge diffs, **find across all files** (mod+F). | W D M | `components/DiffPanel.tsx`, `DiffPanelShell.tsx`, `DiffWorkerPoolProvider.tsx`, `components/diffs/*` (`StyledDiffCodeView.tsx`, `DiffFileTree.tsx`, `useReviewFilePatches.ts`), `apps/web/src/diffPanelStore.ts`, `lib/diffRendering.ts`, `lib/diffCollapse.ts` | L |
| Review comments on diffs | Annotate lines and add them to the composer as review-comment chips (mod+Enter submits). | W D | `components/diffs/AnnotatableCodeView.tsx`, `DiffCommentAnnotation.tsx`, `commentSubmitShortcut.ts`, `apps/web/src/reviewCommentContext.ts` | M |
| File browser | Project tree with expand state, drag a file into the composer as a mention, context menu (copy path, open in editor, reveal). | W D M | `components/files/FileBrowserPanel.tsx`, `fileTreeExpansion.ts`, `fileTreeDragMention.ts`, `useDirectoryEntries.ts`, `apps/web/src/fileContextMenu.ts`, `@pierre/trees` | M |
| File preview / editor | Syntax view; Markdown, HTML, CSV/TSV rendered vs raw; PDF; audio; images. **Editing and saving** with `filesystem:write` (save coordinator, revision checks). Line reveal and links. Read-only for files outside the workspace. | W D M | `FilePreviewPanel.tsx`, `ReadOnlySourcePreview.tsx`, `FileMarkdownPreview.tsx`, `DelimitedTablePreview.tsx`, `AudioPreview.tsx`, `BrowserDocumentFrame.tsx`, `fileSaveCoordinator.ts`, `fileContentRevision.ts`, `fileCommentAnnotations.ts`, RPC `projects.readFile` / `writeFile` | L |
| Attachment preview | Preview files attached to messages beside the conversation (Copy contents, Save file). | W D M | `components/files/AttachmentFilePreview.tsx` | S |
| Go to file / search contents | mod+P fuzzy file picker; mod+shift+F project content search dialog. | W D | `components/files/ProjectFilePicker.tsx`, `components/search/ProjectContentSearchDialog.tsx`, RPC `projects.searchEntries` / `searchContents` | M |
| PR Code tab | See 1.3. Per-file viewed marks (GitHub native, or server-kept on other hosts with revision tracking). | W D M | `PullRequestCodeTab.tsx`, `pullRequestFilesViewed.logic.ts`, `docs/internals/pull-request-file-revisions.md` | (in XL above) |
| Diff color scheme and layout settings | Diff colors, layout, default collapsed state. | W D | Settings → General / Appearance | S |

### 1.8 Projects and threads management

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Sidebar sections | Pinned, Active (manual order), Working (beta fold), Snoozed shelf, Settled shelf. Unsent drafts; per-project grouping and "Filter by project"; status indicators (Working, Waiting, Approval, Input, Failed, Limited, Goal, terminal running, unread). Relative times. | W D M | `components/Sidebar.tsx` (5.1k LOC), `Sidebar.logic.ts`, `Sidebar.drag.ts`, `Sidebar.motion.ts`, `Sidebar.pointer.ts`, `Sidebar.snooze.ts`, `ThreadStatusIndicators.tsx`, `apps/web/src/sidebarProjectGrouping.ts`, `client-runtime/state/threadSort.ts`, `threadInbox.ts`, `threadSettled.ts` | XL (drag physics + sections) |
| Drag interactions | Drag between sections to pin / unpin / settle / un-settle / wake, with action badges; reorder within sections; drag-settle a range by pressing Settle and dragging; multi-select. | W D (M: "Arrange threads" sheet) | `Sidebar.drag.ts`, `Sidebar.pointer.ts`, `@dnd-kit/*` | L |
| Thread menu | Copy (path, branch, thread ID); new thread on branch; pin / unpin; settle / un-settle; snooze presets and Custom… (date/time or duration); wake; rename; regenerate title; mark unread; filter by project; auto-settle behavior; project settings; archive; delete; discard draft. | W D M | `components/threadActionMenu.logic.ts`, `hooks/useThreadActionMenu.ts`, `hooks/useThreadActions.ts`, `CustomSnoozeDialog.tsx` | M |
| Undo notice | 5 s undo for unpin, settle, snooze, archive and discard draft (mod+Z). | W D | `SidebarThreadUndoNotice.tsx`, `hooks/threadUndo.ts`, `hooks/showThreadUndoNotice.ts` | S |
| Auto-settle | Inactive days, merged PR, keep threads with PR. Runs server-side; per-thread disable. | W D M | Settings → General / Thread behavior | S |
| Thread search | Sidebar search and palette search across environments (titles plus message search after 2 chars). | W D M | `ThreadSearchMatch.tsx`, `client-runtime/state/threadSearch.ts`, RPC `orchestration.searchThreads` | M |
| Archive | Archive / unarchive; Settings → Archive page lists archived threads (separate archive shell query). | W D M | `routes/settings.archived.tsx`, `apps/web/src/lib/archivedThreadsState.ts`, `client-runtime/state/archivedThreads.ts` | S |
| Thread header | Breadcrumb (project / thread), inline rename, environment badge, panel layout controls. | W D M | `chat/ChatHeader.tsx`, `WorkspaceBreadcrumb.tsx`, `ProjectEnvironmentBadge.tsx` | S |
| Thread details card / panel | Workspace and version-control details, PR rows with watch eye, relationships, automations. Popover or inline. | W D | `ThreadDetailsCard.tsx`, `ThreadDetailsPanel.tsx`, `ThreadDetailsControl.tsx`, `ThreadDetailsPrRow(s).tsx` | M |
| Threads without project | Scratch-folder threads (`~/.t3/scratch`), mod+alt+N. | W D M | `hooks/useScratchProject.ts`, RPC `projects.ensureScratch` | S |
| Project management | Add (local folder, WSL folder, clone, new); rename; icon (auto favicon / emoji / monogram / image); checkouts across environments ("logical projects"); remove; project overrides. | W D M | `components/settings/ProjectSettingsPanel.tsx`, `ProjectIconPickerDialog.tsx`, `ProjectFaviconPickerDialog.tsx`, `ProjectMonogram.tsx`, `ProjectFavicon.tsx`, `apps/web/src/logicalProject.ts`, `projectIdentity.ts`, `client-runtime/state/projectGrouping.ts` | M |
| Project grouping / order settings | Group by repo or flat; sort orders; preview count. | W D | ClientSettings `sidebarProjectGrouping*`, `sidebarThreadSortOrder` | S |
| Load balancing | Auto-pick an environment for new threads by CPU / memory with per-machine weights. | W D | `components/settings/LoadBalancingSettings.tsx`, `hooks/useLoadBalancedEnvironment.ts`, `client-runtime/src/load-balancing.ts` | M |
| Thread migration from V1 | Toast and import of legacy threads; transcripts load lazily. | W D | `LegacyThreadMigrationToast.tsx`, `hooks/useThreadVisitedMigration.ts`, `docs/user/thread-migration.md` | S (UI) |
| Import agent sessions | Import Claude Code / Codex conversations (welcome wizard) and ACP native sessions. | W D | RPC `agentSessions.scan` / `import`, `onboarding/projectImport.logic.ts` | M |
| Read state | Visit tracking, Mark unread, unread dots. | W D M | command `thread.visit` / `thread.mark-unread` | S |
| Navigation history | Back / forward (mod+[ / mod+]), previous / next thread (mod+shift+[ / ]), jump 1–9, reopen closed view (mod+shift+T) for files, diffs, PRs, browsers, devices. | W D | `hooks/useNavigateBack.ts`, `apps/web/src/closedViewStore.ts`, `reopenClosedView.ts`, `ReopenClosedViewShortcut.tsx` | S |
| Right panel tabs | Tabbed surfaces: Diff, Files, File, Terminal, Browser, Device, Pull request(s). Tab context menu (close others / right / all, rename, mute), maximize, resize, popover vs inline thread panel, proactive panels (auto-open on agent activity). | W D | `RightPanelTabs.tsx`, `RightPanelSheet.tsx`, `chat/RightPanelSurfaceContent.tsx`, `apps/web/src/rightPanelStore.ts`, `rightPanelLayout.ts`, `hooks/useProactivePanels.ts`, `components/preview/RightPanelResizeHandle.tsx` | L |

### 1.9 Command palette, keybindings, focus

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Command palette | mod+K. Contents: actions, recent threads, projects, settings (deep search), themes and appearance sub-pages, add-project wizard (directory browser, clone flow, env picker), `>` prefix for actions only, number shortcuts, virtualized list with viewport-leased VCS queries. | W D M (iPad) | `components/CommandPalette.tsx` (3.4k LOC), `CommandPalette.logic.ts`, `CommandPaletteResults.tsx`, `apps/web/src/commandPaletteBus.ts` | L |
| Configurable keybindings | JSON rules `{key, command, when}` stored on the server (`~/.t3/userdata/keybindings.json`). When-expression parser (`!`, `&&`, `||`, parentheses); last-match-wins; settings editor with recorder and conflict display; update toast when new defaults arrive. | W D | `packages/contracts/src/keybindings.ts`, `packages/shared/src/keybindings.ts`, `apps/web/src/keybindings.ts`, `components/settings/KeybindingsSettings.tsx`, `KeybindingsConfigWarning.tsx`, `KeybindingsUpdateToast.logic.ts`, RPC `server.upsertKeybinding` / `removeKeybinding` | M |
| Preview-forwarded shortcuts | App shortcuts keep working while a browser webview has focus (forwarded from the guest). | D | `PreviewForwardedShortcut` in contracts, desktop `preview.setForwardedShortcuts` | S |
| Quit shortcut | Hold mod+Q (1.2 s) or double-press; modes Hold / Direct / Double. | D | `QuitHoldOverlay.tsx`, `apps/desktop/src/window/QuitHold.ts` | S |
| Focus model | Palette keeps focus and returns it to the composer; number keys go to an open picker; `editableFocus` context. | W D | `apps/web/src/lib/editableFocus.ts`, `docs/user/keyboard-focus.md` | S |

### 1.10 Settings, appearance, themes, i18n

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Scoped settings model | "Applying settings for …" sentence: All / one environment × All projects / one project. Shows Mixed values, an inheritance-chain popover (layers icon), Reset / Reset all. Device-local settings are separate. Searchable settings sidebar. | W D M | `components/settings/settingsScope*.ts`, `SettingsScopeSentence.tsx`, `SettingInheritance.tsx`, `ScopedSwitch.tsx`, `useScopedSettings.ts`, `settingsSearch.ts`, `docs/user/project-settings.md` | L |
| Themes | Built-in palettes; separate light and dark picks; System/Light/Dark; contrast; glass opacity; chat width; environment identification colors. **Theme editor** with color inspector ("pick a color from the app"), advanced tokens, JSON import/export. **VS Code theme import** (files or `~/.vscode/extensions`); **Open VSX marketplace search and install**; server-published "environment themes" (`t3 theme set`). | W D (M: own themes, Material You) | `apps/web/src/themePalette.ts` (1.6k LOC), `vscodeThemeImport.ts`, `openVsxThemes.ts`, `components/settings/ThemeSettings.tsx`, `ThemeEditorPanel.tsx`, `ThemeEditorHost.tsx`, `themeInspector.ts`, `ThemeImportDialog.tsx`, `ThemeSearchSection.tsx`, `hooks/useTheme.ts`, `useEnvironmentTheme.ts`, `useCustomThemes.ts` | L |
| Fonts | Interface, prompt, code and terminal families and sizes; font-smoothing; system font enumeration. | W D | `apps/web/src/appearanceFonts.ts`, `FontFamilyPicker.tsx`, `SettingsFontPreviews.tsx` | S |
| Motion | Panel animation slider (0–400 ms, default 0 = instant); respects reduced motion. | W D | `apps/web/src/panelAnimations.ts`, `PanelAnimationsPreview.tsx` | S |
| Timestamp format | Locale / 12 h / 24 h; week start from locale. | W D M | `apps/web/src/timestampFormat.ts` | S |
| i18n | **None.** English-only UI strings hardcoded. Locale is used only for dates (`Intl`); desktop `getSystemLocale`; `electronLanguages: ["en-US"]`. Mobile handles RTL in one control. | — | — | — (not a parity item) |

### 1.11 Remote and multi-device

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Connection catalog | Saved environments, each with an ordered **route list** (LAN, Tailscale, VPN, public, SSH, T3 Connect). First-working-route connect; preflight of earlier routes with cooldown; learned LAN/tailnet routes; drag reorder; route count; "In use" marker; permissions view. | W D M | `packages/client-runtime/src/connection/{driver,supervisor,registry,routes,resolver,catalog}.ts`, `apps/web/src/components/settings/ConnectionsSettings.tsx` (4k LOC), `EnvironmentRoutesList.tsx`, `EnvironmentRow.tsx`, `SessionPermissions.tsx` | XL |
| Pairing | One-time pairing URL / QR (secret in the fragment), scopes, "Add environment", hosted pairing flow, revoke sessions and links, authorized clients list. | W D M | `apps/web/src/hostedPairing.ts`, `pairingUrl.ts`, `components/auth/PairingRouteSurface.tsx`, `components/settings/pairingUrls.ts`, `client-runtime/src/authorization/*`, `docs/internals/environment-auth.md` | M |
| T3 Connect (cloud relay) | Clerk sign-in (web and Electron), DPoP-bound bootstrap credentials, environment discovery list, link / deregister, managed tunnel, relay client (cloudflared) install dialog, hold webhooks while offline, publish agent activity. | W D M | `apps/web/src/cloud/*` (`dpop.ts`, `managedAuth.tsx`, `linkEnvironment.ts`, `useCloudLinkController.ts`), `components/cloud/*`, `client-runtime/src/relay/*`, `docs/internals/t3-connect.md` | XL |
| Desktop server exposure | Local-only vs network access, Tailscale HTTPS serve toggle, advertised endpoints. | D | `apps/desktop/src/backend/DesktopServerExposure.ts`, `state/desktopNetworkAccess.ts` | M |
| Desktop-managed SSH | Add an SSH environment: host discovery from `~/.ssh/config`, remote server download/launch, port forward, password prompt dialog. | D | `apps/desktop/src/ssh/*`, `packages/ssh/src/tunnel.ts`, `components/desktop/SshPasswordPromptDialog.tsx`, `state/desktopSshHosts.ts` | L |
| WSL backend | Run the server in a WSL distro (Windows), WSL-only mode, distro picker. | D (Win) | `apps/desktop/src/wsl/*`, `state/desktopWslState.ts`, `apps/web/src/wslPaths.ts` | M |
| Local environment off | Desktop as a pure remote client. | D | `LocalEnvironmentSetting.tsx`, `docs/internals/remote.md#desktop-without-a-local-environment` | S |
| Hosted web app | app.t3.codes static client; connection catalog in the browser; HTTPS-only backends; prompt-link landing ("open in desktop / mobile / this browser"). | W | `apps/web/src/hostedPairing.ts`, `components/promptLinks/HostedPromptLinkLanding.tsx`, `vercel.ts` | (skip for native) |
| Outside agents (MCP) | Copy MCP URL; `connect-agent` OAuth consent page (pairing code, scope Read-only → Full access); revoke. | W D | `routes/connect-agent.tsx`, `components/auth/ConnectAgentSurface.tsx`, `docs/user/outside-agents.md` | M (page could stay server-served HTML) |
| Version skew and server updates | Outdated-server notices; "Update server" with progress; copy update / relaunch command; **Update everything** button (providers → servers → desktop app) with countdown and per-machine progress. | W D M | `apps/web/src/versionSkew.ts`, `ServerUpdateAction.tsx`, `components/sidebar/UpdateEverythingDialog.tsx`, `updateEverything.logic.ts`, `useUpdateEverything.ts`, `client-runtime/connection/outdatedHostUpdate.ts`, `state/outdatedServerUpdate.ts` | M |
| GitHub routing / sharing | Use another environment's GitHub credential (Read PRs / Read and act). | W D M | `GitHubRoutingSettings.tsx`, `client-runtime/connection/githubRoutingPermissions.ts`, `state/pullRequestRouting.ts` | M |
| Devices (simulators) | Live iOS Simulator / Android Emulator panel: H.264 via WebCodecs or MJPEG fallback, touch / keys, Home / Back / rotate / power, tools drawer (appearance, text size, a11y overlay, location, permissions, push), 3D device model with fold controls (iPhone Duo / Android foldables), float over chat, SSH device hosts, setup wizard. | W D M | `apps/web/src/components/device/*` (~25 files), `client-runtime/src/device/*` (scene, stream, framing), `docs/user/devices.md`, `docs/internals/devices.md` | XL |
| Offline caching | Persisted shell and thread snapshots for instant start; offline PR content (mobile). | W D M | `client-runtime/src/platform/orchestrationCache.ts`, `state/cachePersistence.ts`, `queryPersistence.ts` | M |

### 1.12 Notifications, background service, scheduled tasks

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Thread notifications | Completed, failed, approval needed, input needed, usage limit. Modes: Notifications / Notifications with sound / Sound only; in-app toasts with "Open thread"; dock / taskbar badge count (Windows overlay icon). | W D | `components/ThreadNotificationCoordinator.tsx`, `apps/web/src/threadNotifications.ts`, `components/settings/NotificationSettings.tsx`, `apps/desktop/src/ipc/methods/notificationBadge.ts` | M |
| Mobile push and live activity | APNs/FCM via T3 Connect; iOS Live Activities, Android ongoing / Live Update notifications. | M | see section 4, `docs/user/mobile-notifications.md` | L |
| Scheduled tasks | Create / edit / pause / resume / run now / delete. Schedules: interval, daily, weekdays, custom days, **On webhook** (URL with rotate, `{{body.x}}` placeholders, HMAC signature verification such as GitHub, skip-older-than, deliveries log). Fields: project / checkout / new worktree, model, prompt. | W D M | `components/settings/ScheduledTasksSettings.tsx` (1.6k LOC), `scheduledTasksSettings.logic.ts`, `client-runtime/src/scheduledTaskWebhook.ts`, `webhookAddress.ts`, RPC `scheduledTasks.*`, `docs/user/project-settings.md#webhook-automations` | L |
| PR watch | Agent-initiated or manual "Watch for changes" on a PR wakes the thread on checks, comments or conflicts. | W D | `ThreadPullRequestsPanel.tsx` row menu, command `thread.pull-request.watch` | S (UI) |
| Background service | `t3 service install` (systemd / launchd), `t3 doctor`. CLI only, no GUI except the Storage "reclaim database" notice. | (CLI) | `docs/user/background-service.md`, `components/settings/DatabaseStorageNotice.tsx` | — |
| Background activity profile | Server setting controlling background polling intensity (fed by desktop power state). | W D | `ServerSettings.backgroundActivity` / `backgroundActivityProfile`, `apps/web/src/lib/backgroundActivityReporter.ts` | S |

### 1.13 Usage, telemetry, activity log

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Usage page | Cost / Tokens / Limits tabs (C / T / L). Period 24 h / 7 d / 30 d / 90 d (mod+shift+1..4). Per-provider charts, model breakdown (trend, cache hit, $/Mtok), Premium and Other cost, environment filter with scanning state. | W D M | `components/usage/UsagePage.tsx` (1.6k LOC), `UsageProviderChart.tsx`, `UsageModelDialog.tsx`, `usageBreakdown.ts`, `usageShortcuts.ts`, `client-runtime/state/usage*.ts` | L |
| Pooled limits | Per-provider pool of accounts; segmented bar; reset estimates; "Use reset" credits; CLIProxyAPI hubs. | W D M | `UsageLimitsPooled.tsx`, `UsageLimits.tsx`, `AddUsageLimitSourceDialog.tsx`, `UsageProviderSettings.tsx` | M |
| Custom model prices | Editable price table with Map-to, multi-environment save, retry failed. | W D | `UsagePriceOverrides.tsx`, `usagePriceTable.ts`, `usagePriceForm.ts`, `usagePriceTargets.ts` | M |
| Usage widgets | iOS / Android home-screen "Subscription usage" widget. | M | see section 4 | M |
| Activity log | Grouped tool-call summaries (see 1.5). | W D | `docs/user/activity-log.md` | — |
| Diagnostics | Trace diagnostics (slowest spans, failures, top names, logs); **resource telemetry** (host and collection, live process tree with signal actions, resource timeline, instrumented I/O); OTLP export config and test. | W D (M: Diagnostics crash log) | `components/settings/DiagnosticsSettings.tsx`, `ResourceTelemetryDiagnostics.tsx`, `ProcessSignalActions.tsx`, `TelemetryExportSettings.tsx`, `docs/internals/resource-telemetry.md` | M |
| Product analytics | Server-side only (`T3CODE_TELEMETRY_ENABLED`); clients send client activity / OTLP traces. | — | `apps/web/src/observability/clientTracing.ts`, `docs/user/telemetry.md` | S |
| Slow RPC toasts | Warn when requests stall. | W D | `SlowRpcRequestToastCoordinator.tsx`, `apps/web/src/rpc/requestLatencyState.ts` | S |

### 1.14 Install, update, welcome wizard

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| Welcome wizard | Steps: connect computers (T3 Connect / add computer); check agents (Claude Code, Codex) with an install / login terminal; import projects (scan Claude / Codex history, grouped by repo, 30-day default selection, conversation import ≤200 messages); progress bar; "Still connecting" and "Could not read settings" states. | W D | `components/onboarding/WelcomeWizard.tsx` (1.8k LOC), `FirstRunGate.tsx`, `apps/web/src/onboarding/*`, `docs/user/welcome-wizard.md` | L |
| ChatGPT welcome | First-run coordinator for Codex via ChatGPT. | W D | `ChatGptWelcomeCoordinator.tsx` | S |
| Desktop auto-update | electron-updater, stable / nightly channel, sidebar update pill or capsule with release notes, download, install, "did not apply" detection, remote-triggered desktop updates. | D | `apps/desktop/src/updates/*`, `apps/web/src/state/desktopUpdate.ts`, `components/sidebar/SidebarUpdatePill.tsx`, `SidebarUpdateCapsule.tsx`, `SidebarUpdateReleaseNotes.tsx`, `desktopUpdate.toast.tsx` | M |
| `t3` CLI install | Settings → About → install / remove the `t3` command. | D | `CliCommandSettingsRow.tsx`, `apps/desktop/src/app/DesktopCliCommand.ts` | S |
| Mobile app links | Store links and QR codes in Settings → General. | W D | `MobileAppSettingsRow.tsx` | S |
| Open-source licenses | Searchable license list with full notices. | W D M | `components/settings/OpenSourceLicenses.tsx`, `routes/settings.open-source-licenses.tsx` | S |
| Chunk reload guard / boot errors | Recover from stale bundles; boot error screen. | W D | `apps/web/src/lib/chunkReloadGuard.ts`, `lib/bootError.ts` | — |

### 1.15 Browser, SnapShot, default browser, misc

| Feature | Description | Surfaces | Key paths | Cx |
|---|---|---|---|---|
| In-app browser (preview) | Tabs in the right panel. URL bar with search engine, back / forward / reload, zoom, device toolbar (responsive presets, viewport resize handles, rotate, aspect lock), dark / light emulation, DevTools, profiles (multiple, default), mute, recent URLs, discovered local dev servers. | D (webview) / W M (server-streamed) | `apps/web/src/components/preview/*` (~35 files), `apps/web/src/browser/*` (~35 files: `ElectronBrowserHost.tsx`, `HostedBrowserWebview.tsx`, `ServerBrowserSurface.tsx`, `BrowserDeviceToolbar.tsx`), `apps/desktop/src/preview/Manager.ts` | XL |
| Server-hosted browser | Headless Chrome on the environment, streamed to any client; take / release control; clipboard, file-picker and download bridging; agent tabs shared with the user. | W D M | `client-runtime/src/preview/serverBrowserStream.ts`, `browser/ServerBrowserSurface.tsx`, `browser/serverPictureInPicture.ts` | L |
| Agent browser automation UI | Agent-opened tabs float over chat (mini player) with an agent cursor overlay; auto-show floating preview setting; PiP / separate window. | W D M | `ThreadPreviewMiniPlayer.tsx`, `previewMiniPlayerLayout.ts`, `AgentBrowserCursor.tsx`, `agentBrowserCursorLogic.ts`, `useAgentOpenedPreviews.ts`, `apps/web/src/previewMiniPlayerStore.ts` | L |
| Element picker and annotations | Pick a DOM element → "element context" chip; annotate preview (elements, regions, drawings) → screenshot plus annotation chip. | D (W M partial) | `lib/elementContext.ts`, `lib/previewAnnotation.ts`, `browser/annotationTheme.ts`, desktop `preview-pick-preload.ts` | L |
| Screenshots and recording | Capture screenshot (copy / save); record the tab with key and mouse overlays at a configurable frame rate. | D | `browser/browserRecording.ts`, `recordingCompositor.ts`, `browserRecordingScope.ts` | M |
| Browser profile import | Import cookies from Chrome / Chromium / Firefox / Safari / Helium (OS keyring decrypt; Safari needs Full Disk Access). | D | `components/settings/BrowserImportWizard.tsx`, `apps/desktop/src/preview/BrowserImport/*`, `native/browser-secret` | L |
| Passkeys in browser | WebAuthn in preview webviews (macOS). | D | `apps/desktop/src/preview/Passkeys.ts` | M |
| Default browser | macOS: T3 Code registers as an http/https handler. Links open as a browser tab in the visible thread, or as a new no-project thread with the browser maximized. HTML files from Finder open the same way. | D (mac) | `components/desktop/DesktopWebLinkCoordinator.tsx`, `apps/desktop/src/app/DesktopWebLinks.ts`, `docs/user/default-browser.md` | M |
| `t3 app` activation | CLI opens a thread for a cwd in the running desktop app (local socket / pipe). | D | `components/desktop/DesktopAppActivationCoordinator.tsx`, `apps/desktop/src/app/DesktopAppActivation*.ts` | S |
| SnapShots | Global shortcut (both Shifts by default) captures the foreground window, its accessibility tree and icon, attaches it to the draft with sound, flash and fly-in animation. Pending captures persisted. Per-OS setup wizards (macOS permissions, GNOME extension, KDE helper, Hyprland / Niri config edits, XDG portal). | D | `components/settings/SnapShotSettings.tsx`, `SnapShotSetupDialog.tsx`, `CaptureShortcutConfig.tsx`, `components/desktop/SnapShotCoordinator.tsx`, `lib/snapShot*.ts`, `apps/desktop/src/snapShot/*` (~40 files), `native/{hyprland,kde}-snap-shot` | XL |
| Open-link routing | "Open links in" setting (in-app vs system browser); open PRs in T3 Code. | W D | `browser/browserLinkTarget.ts`, `browser/useOpenLink.ts` | S |
| Context menus | Native on desktop (IPC), DOM fallback on web. | W D | `apps/web/src/contextMenuFallback.ts`, desktop `context-menu` IPC | S |
| Confirm dialogs | Unpin / archive / delete confirmations (configurable). | W D | `ConfirmDialogHost.tsx`, `apps/web/src/confirmDialog.ts` | S |
| Project favicon detection | Auto icon from the repo, cached. | W D M | `client-runtime/src/projectFaviconCache.ts`, `ProjectFavicon.tsx` | S |

---

## 2. Settings pages and keybindings (enumerated from source)

### 2.1 Settings pages (web and desktop)

Source: `apps/web/src/components/settings/settingsSearch.ts` (`SETTINGS_SECTION_LABELS`, `SETTINGS_SEARCH_ITEMS`), `SettingsPanels.tsx`, and route files `apps/web/src/routes/settings.*.tsx`.

The sidebar nav order is General, Appearance, Keybindings, SnapShots, Providers, Integrations, Scheduled Tasks, Source Control, Storage, Connections, Archive. "Project" appears only when a project is selected. Diagnostics is reached from General.

| Page | Route | Sections / searchable settings |
|---|---|---|
| **General** | `/settings/general` | **New threads:** default model, Permissions (default runtime mode), workspace mode, Submodules, Start from origin, "Add project starts in".<br>**Behavior:** Follow-up behavior, Send shortcut, Rich text composer, Collapse composer on scroll, Show skills in slash menu, Proactive panels, Response streaming, Hide whitespace changes, Default diff file state, Diff layout, Time format.<br>**Notifications:** thread notifications mode, in-app notifications.<br>**Projects & threads / Organization:** Project grouping, Project order, Working section (beta), Snooze limited threads, Auto-resume limited threads, Auto-settle inactive threads, Days of inactivity, Auto-settle merged threads, Keep threads with a PR.<br>**Text generation:** text-generation model.<br>**Background:** Provider update checks, Continue threads after restarts, Background activity.<br>**Confirmations:** Unpin, Archive, Delete, Quit shortcut.<br>**About:** Version, Update track (desktop channel), t3 command, Mobile app, Privacy policy, Diagnostics link, Open source licenses.<br>**Legacy toggles:** Plan mode, Context window indicator, Sidebar (legacy). |
| **Appearance** | `/settings/appearance` | Colors & themes: color scheme, themes, create / import / Open VSX, environment themes.<br>Contrast, Glass opacity, Diff colors, Chat width, Motion: Panel animations, Environment identification.<br>Typography: interface / prompt / code / terminal font, Font smoothing, Word wrap.<br>Composer context. |
| **Keybindings** | `/settings/keybindings` | Grouped command list with defaults, recorder, `when` editor, "New keybinding", config warnings. |
| **SnapShots** | `/settings/snap-shot` | Enable, setup wizard, Include app text, Capture shortcut, Capture sound, Capture flash, Capture animations, Manage capture. |
| **Providers** | `/settings/providers` | Environment picker; provider instance cards (enable switch, scoped per project; display name, accent, driver, runtime / setup / install, account sign-in, binary path, env variables with sensitive flag, launch args, models / custom models, advanced, Codex homes, Claude auto-compact, Antigravity sign-in method, OpenCode server URL / password); Add provider (built-in / ACP Registry / local ACP command); native sessions; Update all.<br>**Usage providers:** CLIProxyAPI hubs, Cursor account usage; Health check interval. |
| **Integrations** | `/settings/integrations` | **Browser:** Agent browser access, Browser profiles (+ import wizard), Default profile, viewport, zoom, appearance, recording frame rate, show key / mouse presses, Open links in, Search engine (+ custom URL), Open pull requests in T3 Code, Auto-show floating preview.<br>**Devices:** Device hub, Simulator support, Agent device access, Device hosts (SSH, test connection), tool versions. |
| **Scheduled Tasks** | `/settings/scheduled-tasks` | Task list per environment / project, editor (schedule type incl. webhook, signature, headers / query placeholders, skip-older-than), deliveries. |
| **Source Control** | `/settings/source-control` | Server environment, Source Control Providers (per host: GitHub accounts and token, Bitbucket, GitCafe, Rescan), Automatically pull, Git fetch interval, Default merge method, Remove agent credits when merging, Worktree branch naming, Text generation: writing style, follow change-request templates, writer model; Review guides (beta), review guide model, review guide instructions. |
| **Storage** | `/settings/storage` | Database (reclaim notice), Worktrees (Worktree location, Automatic worktree cleanup: inactive / settled / merged / unchanged / deleted threads; keep local changes), Cleanup (Delete now, Last run), Artifacts and logs (browser artifacts, rotated logs). |
| **Connections** | `/settings/connections` | This machine (Environment icon, Local environment, Network access, Tailscale HTTPS, WSL backend / distro / WSL-only, Version), Pairing links + QR, Authorized clients (sessions, revoke), Environments (saved, routes, permissions, Copy MCP URL), T3 Connect (sign-in, link, hold webhooks while offline, publish agent activity), Load balancing, GitHub sharing. |
| **Archive** | `/settings/archived` | Archived threads list. |
| **Project** | `/settings/projects` (`?project=`) | Overview / name, Project icon, Checkouts, Model / Workspace / Automatically pull defaults, Actions (scripts, `t3.json` import, run-on-settle), Danger (remove). |
| **Diagnostics** | `/settings/diagnostics` | Trace Diagnostics (Slowest Spans, Most Common Failures, Top Span Names, Span Logs), Resource monitor (Host & collection, Live process tree with signal actions, Resource timeline, Instrumented I/O), OpenTelemetry export. |
| **Open source licenses** | `/settings/open-source-licenses` | Project source, Third-party notices. |

Device-local (ClientSettings) keys, from `packages/contracts/src/settings.ts:313`:

- **Notifications and confirmations:** notificationMode, inAppNotificationsEnabled, confirmQuit / ThreadArchive / ThreadDelete / ThreadUnpin.
- **Appearance:** diffColorScheme, chatWidth, appearanceContrast, panelAnimationDurationMs, glassOpacity, environmentIdentificationMode, font sizes and families (interface / prompt / code / terminal), fontSmoothing, timestampFormat, wordWrap.
- **Browser:** browserDefault* (viewport, zoom, appearance), browserRecording*, browserLinkTarget, browserSearchEngine, browserCustomSearchUrl, browserAutoShowFloatingPreview, browserProfiles, browserDefaultProfileId.
- **Diff:** diffFilesCollapsed, diffIgnoreWhitespace, diffLayout.
- **Composer:** persistComposerContextStrip, composerCollapseOnScroll, composerRichTextEnabled, sendShortcut, followUpBehavior, showSkillsInSlashMenu, proactivePanelsEnabled.
- **Models and PRs:** favorites, providerModelPreferences, pullRequestMergeMethodOverrides, pullRequestLinksOpenInApp.
- **Legacy / feature toggles:** planModeEnabled, contextWindowMeterEnabled, legacySidebarEnabled.
- **Sidebar:** sidebarWorkingShelfEnabled, sidebarProjectGroupingMode / Overrides / SortOrder, sidebarThreadSortOrder / PreviewCount.
- **Load balancing and onboarding:** loadBalancingEnabled / Weights, onboardingCompletedAt, dismissedProviderUpdateNotificationKeys.
- **SnapShot:** snapShotEnabled / IncludeAccessibility / Shortcut / PlaySound / Sound / Flash / Animations.

Server (environment) settings, from `settings.ts:1068`:

- **Models and runtime:** favorites, providerModelPreferences, defaultModelSelection, defaultRuntimeMode, defaultThreadEnvMode, textGenerationModelSelection.
- **Storage and worktrees:** worktreeCleanup, storageCleanup, worktreesDirectory, previousWorktreesDirectories, newWorktreesStartFromOrigin, worktreeSubmodules, addProjectBaseDirectory.
- **Behaviour:** responseStreamingMode, enableProviderUpdateChecks, continueThreadsAfterServerUpdate.
- **Agent capabilities:** enableAgentBrowserAccess (+ project overrides), enableAgentDeviceAccess, enableDeviceSupport, deviceOnboardingCompleted, deviceHosts.
- **Git:** defaultAutoPull (+ overrides), automaticGitFetchInterval.
- **Projects:** defaultProjectScripts, projectScriptOverrides, projectSettingsOverrides, projectSettingsFolded.
- **Thread lifecycle:** sidebarAutoSettleAfterDays / KeepPullRequests / OnMerge, snoozeLimitedThreads, autoResumeLimitedThreads.
- **Background and health:** backgroundActivity, backgroundActivityProfile, providerHealthRefreshInterval.
- **Theme and identity:** defaultTheme / SetAt, environmentIcon.

### 2.2 Keybinding commands and defaults

Source: `packages/contracts/src/keybindings.ts` (`STATIC_KEYBINDING_COMMANDS`) and `packages/shared/src/keybindings.ts:148-233` (`DEFAULT_KEYBINDINGS`). `mod` = Cmd on macOS, Ctrl elsewhere. The last matching rule wins.

**When-context keys:**
- documented: `terminalFocus`, `terminalOpen`, `previewFocus`, `previewOpen`, `modelPickerOpen`, `usagePageOpen`, `composerFocus`, `composerDraft`, `turnRunning`, `editableFocus`, `isWeb`, `isDesktop`;
- also used in source: `rightPanelOpen`, `draftThreadRoute`.

| Command | Default key | When |
|---|---|---|
| sidebar.toggle | mod+b | |
| navigation.back / navigation.forward | mod+[ / mod+] | !terminalFocus |
| terminal.toggle | mod+j | |
| terminal.split / terminal.splitVertical | mod+d / mod+shift+d | terminalFocus |
| terminal.new | mod+n | terminalFocus |
| terminal.close | mod+w | terminalFocus |
| rightPanel.toggle | mod+alt+b | |
| threadPanel.toggle | mod+shift+b | !terminalFocus |
| rightPanel.new | mod+t | rightPanelOpen && !terminalFocus |
| rightPanel.close | mod+w | !terminalFocus |
| rightPanel.toggleMaximized | (unbound) | |
| view.reopenClosed | mod+shift+t | |
| diff.toggle | mod+d | !terminalFocus |
| preview.toggle | mod+shift+j | |
| preview.refresh / focusUrl | mod+r / mod+l | previewFocus |
| preview.zoomIn / zoomOut / resetZoom | mod+= (and mod++) / mod+- / mod+0 | previewFocus |
| commandPalette.toggle | mod+k | !terminalFocus |
| filePicker.toggle | mod+p | !terminalFocus |
| projectSearch.toggle | mod+shift+f | !terminalFocus |
| usage.open | mod+u | !terminalFocus |
| theme.select | mod+alt+a | !terminalFocus |
| appearance.cycle | mod+alt+shift+a | !terminalFocus |
| themeEditor.toggle | mod+alt+shift+t | |
| composer.stash | mod+s | !terminalFocus |
| thread.steerQueuedMessage | mod+shift+enter | !terminalFocus |
| thread.editQueuedMessage | alt+arrowup | composerFocus |
| composer.sendAlternate | mod+enter | composerFocus && turnRunning |
| composer.sendBackground | mod+enter, mod+alt+enter | composerFocus && draftThreadRoute |
| composer.sendAndNewThread | mod+alt+enter | composerFocus && !draftThreadRoute |
| chat.new | mod+n, mod+shift+o | !terminalFocus |
| chat.newLocal | mod+shift+n | !terminalFocus |
| chat.newWithoutProject | mod+alt+n | !terminalFocus |
| chat.find | mod+f | !terminalFocus && !previewFocus |
| modelPicker.toggle | mod+shift+m | !terminalFocus |
| modelPicker.previousProvider / nextProvider | mod+shift+arrowup / arrowdown | modelPickerOpen |
| modelPicker.jump.1–9 | mod+1..9 | modelPickerOpen && isDesktop |
| composer.host | mod+shift+h | !terminalFocus |
| composer.cycleHost | (unbound) | |
| composer.effort | mod+shift+e | !terminalFocus |
| composer.increaseEffort / decreaseEffort | (unbound) | |
| composer.mode (access mode) | mod+shift+a | !terminalFocus |
| composer.workspace | mod+shift+x | !terminalFocus |
| composer.branch | mod+shift+g | !terminalFocus |
| composer.previousWorktree | mod+shift+l | !terminalFocus |
| pullRequest.copyNumber | mod+shift+k | !terminalFocus |
| editor.openFavorite | mod+o | |
| thread.previous / thread.next | mod+shift+[ / mod+shift+] | |
| thread.copyReference | mod+shift+c | !terminalFocus |
| thread.settle | mod+shift+s | !terminalFocus |
| thread.pin | mod+shift+p | !terminalFocus |
| thread.undo | mod+z | !terminalFocus && !editableFocus |
| thread.stop | (unbound) | |
| thread.jump.1–9 | mod+1..9 | !modelPickerOpen && isDesktop |
| usage.cost / tokens / limits | c / t / l | usagePageOpen |
| usage.period.day / week / month / quarter | mod+shift+1 / 2 / 3 / 4 | usagePageOpen |
| script.{id}.run | (user-defined) | |

**Non-configurable keys:**
- diff find (mod+F inside a diff);
- composer Enter / Shift+Enter / Tab semantics;
- prompt recall ↑/↓;
- terminal scrollback keys;
- `Escape` closes popovers;
- desktop menu accelerators: Settings `CmdOrCtrl+,`, Reload, Force Reload, DevTools, zoom, Paste as Text `CmdOrCtrl+Shift+V`;
- quit hold.

**iPad:** Cmd+K palette, Cmd+1..9, Cmd+Shift+H host cycle, Return-key behavior setting.

### 2.3 Command palette commands

Source: `apps/web/src/components/CommandPalette.tsx:1590-2100`.

**Actions:**
- New thread (`chat.newLocal`)
- New thread in… (project chooser)
- New thread without a project
- Find in current thread
- Copy prompt link / Copy prompt link with repo and model
- Copy thread reference (PR link or thread ID)
- Link pull request to thread / Show linked pull requests
- Restart agent session (with confirmation)
- Go to file
- Search project contents
- New project
- Add project, with sub-flows: Local folder, clone from hosted repo or URL with destination picker, Add existing project, environment chooser
- Open WSL folder
- Change theme (sub-list of themes with light/dark previews)
- Change appearance (System / Light / Dark)
- Toggle theme editor
- Open pull requests
- Open usage
- Open settings
- Project settings

**Searchable groups beyond the actions:**
- every Settings search item (`setting:<id>`, including every keybinding command);
- Projects;
- Threads and Recent Threads (cross-environment, with message search);
- Directories (folder browser while adding a project);
- Environments.

---

## 3. Desktop-specific (Electron) features

Electron 44, electron-builder 26, electron-updater 6, electron-store, @clerk/electron (passkeys). Main entry is `apps/desktop/src/main.ts` → `app/DesktopApp.ts`; every Electron API is wrapped as an Effect service in `apps/desktop/src/electron/*`.

### 3.1 Backend management

The bundled server runs as a **child process** (`process.execPath` with `ELECTRON_RUN_AS_NODE=1` and `apps/server/dist/bin.mjs --bootstrap-fd 3`). Code: `apps/desktop/src/backend/DesktopBackendPool.ts`, `DesktopBackendManager.ts`, `DesktopBackendConfiguration.ts`.

**File descriptors:**

| fd | Purpose |
|---|---|
| 3 | Bootstrap JSON: port, t3Home, desktop bootstrap token, Tailscale, `resourceMonitorPath`, OTLP |
| 4 | Telemetry out: power state and update reports |
| 5 | Remote-update control in |
| 6 / 7 | Browser host channel: CDP relay, so agents can drive desktop preview tabs |

**Port and readiness:**
- Port is `T3CODE_PORT` or a scan upward from 3773.
- Readiness is polled at `/.well-known/t3/environment`.
- Restart uses exponential backoff from 500 ms to 10 s.

**Other instances:**
- An optional WSL backend instance runs in the pool.
- When the local environment is off, no backend starts.

The renderer is served from a custom `t3code://app/` protocol (`apps/desktop/src/electron/ElectronProtocol.ts`), with a strict CSP and an SPA fallback. API traffic goes to the environment URL.

### 3.2 IPC

`window.desktopBridge` is defined in `apps/desktop/src/preload.ts` and typed as `DesktopBridge` in `packages/contracts/src/ipc.ts`. Channels are listed in `src/ipc/channels.ts`, handlers are in `src/ipc/DesktopIpcHandlers.ts`, and every payload is Schema-decoded. All channels are prefixed `desktop:`.

| Group | Methods / channels |
|---|---|
| Sync | `getAppBranding`, `getSystemLocale`, `getLocalEnvironmentBootstraps`, `getLocalEnvironmentEnabled`, `getWindowFullscreenState`, legacy localStorage take/complete |
| Window / OS | `setNotificationBadge`, `onNotificationBadgeClear`, `onTrackpadScrollEnd`, `onWindowFullscreenStateChange`, `onQuitShortcut`, `onMenuAction` (open-settings, paste-as-text), `pickFolder`, `pickProjectFavicon`, `pickThemeFiles`, `setTheme` (nativeTheme + titlebar overlay), `showContextMenu`, `openExternal`, `openSystemSettings`, `checkSystemPermission`, `probeRemoteEditors`, `pasteAsText`, plus local `getPathForFile` / `getClientPlatform` / `getWindowZoomFactor` |
| Settings / catalog | `get/setClientSettings`, `get/set/clearConnectionCatalog` (safeStorage-encrypted), `getLocalEnvironmentBearerToken`, `setLocalEnvironmentEnabled` |
| SnapShot | request permissions, get state, setup, preview / apply config, check / suppress shortcut, list / read / acknowledge pending, animation destination / dismiss, `onSnapShotEvent` |
| SSH | discover / resolve hosts, ensure / disconnect environment, fetch descriptor, bootstrap bearer session, fetch session state, issue WS ticket, password prompt on / resolve |
| Server exposure | get state, `setServerExposureMode` (local-only / network-accessible), `setTailscaleServeEnabled`, `getAdvertisedEndpoints` |
| WSL | `getWslState`, `setWslBackendEnabled`, `setWslDistro`, `setWslOnly` |
| Updates | get state, set channel, check, download, install, `onUpdateState` |
| CLI | `cliCommand.getState` / `install` / `uninstall` |
| Links / activation | `webLinks.setReady` / `onOpen`, `appActivation.setReady` / `complete` / `onRequest`, `receive/cancelProviderAuthCallback` |
| Preview (24 handlers) | `createTab`, `closeTab`, `registerWebview`, navigate / back / forward / refresh / hardReload, zoom, `setColorScheme`, `setAudioMuted`, `openDevTools`, `listBrowserImportSources`, `importBrowserCookies`, clear cookies / cache, `getPreviewConfig`, annotation theme / send, `pickElement` / cancel, `captureScreenshot`, `revealArtifact`, `copyArtifactToClipboard`, PiP open / close, recording start / stop / save + frame / input streams, `onStateChange` / `onPointerEvent` / `onOpenLink`, `setForwardedShortcuts` |

**Additional preloads:**
- `preview-pick-preload.ts`: guest `<webview>` element pick, annotations, recording input, passkey bridge (`preview:*` channels in `preview/GuestProtocol.ts`).
- `preview-pip-preload.ts`: PiP frames.
- `mac-permission-preload.ts`: drag-to-System-Settings helper.

### 3.3 Tray, menus, windows

- **Tray:** none. There is no `Tray`, no `powerSaveBlocker` and no login items (verified by grep in `apps/desktop/src`).
- **Application menu** (`apps/desktop/src/window/DesktopApplicationMenu.ts`):
  - macOS app menu: About, Check for Updates…, Settings… (`CmdOrCtrl+,`), Services, Hide / Hide Others / Unhide, Quit.
  - File: Settings (non-mac), Close or Quit.
  - Edit: undo / redo / cut / copy / paste, **Paste as Text** (`CmdOrCtrl+Shift+V`), delete, select all, Speech (mac).
  - View: custom Reload / Force Reload / DevTools / zoom handlers that target the app webContents rather than a focused webview; Toggle Fullscreen.
  - Window: role.
  - Help: Check for Updates.
- **Windows:**
  - Main window: min 840×620, hidden until ready; bounds and maximized state persisted and validated against displays; crash auto-reload.
  - macOS chrome: `hiddenInset` with traffic lights; no vibrancy. Windows/Linux chrome: `titleBarOverlay` with themed colors.
  - Other windows: WSL connecting splash, always-on-top preview PiP panel, mac permission helper, SnapShot fly-in overlay.
- **Single-instance lock**, with `second-instance` link forwarding.

### 3.4 Auto-update

Code: `apps/desktop/src/updates/DesktopUpdates.ts`.

- **Channels:** `latest` and `nightly` from GitHub releases; preview builds have no feed. Nightly allows prerelease and downgrade.
- **Polling:** no auto-download; startup check, then every 4 min.
- **Install:** two-phase handoff (stop backends, then `quitAndInstall`).
- **Failure handling:** "did not apply" detection; remote-triggered updates from other devices through backend fd 5 (rate-capped).
- **Disabled when:** dev build, no feed, `T3CODE_DISABLE_AUTO_UPDATE`, or a Linux build other than AppImage/deb.
- **UI:** sidebar pill or capsule plus native dialogs from the menu.

### 3.5 Deep links and OS integrations

- **Schemes:** `t3code://` and `t3code-dev://`.
- **Incoming URLs** are handled in `app/DesktopClerk.ts`: Codex auth handoff, provider-auth return, app.t3.codes prompt links, http/https and `.html`/`.xhtml` opened in the in-app browser.
- **Default browser:** macOS Info.plist declares http/https Viewer plus `public.html` document types.
- **Linux URL handler:** writes a hidden `.desktop` file and calls `xdg-mime` (`app/DesktopLinuxUrlHandler.ts`).
- **Notifications:** the renderer uses Web Notifications; the main process only sets badges.
- **Badge:** dock / taskbar badge (Windows overlay icon drawn by the renderer), cleared on focus.
- **Global shortcuts:** only for SnapShot.
  - Electron `globalShortcut` key chords.
  - macOS modifier-pair poller child.
  - Windows Shift+Shift worker.
  - Linux XDG portal GlobalShortcuts over D-Bus, Niri D-Bus, and Hyprland/Niri config edits.
- **Power:** `ElectronPowerMonitor` (battery, idle, lock, suspend, thermal) streams to the server for background policy.
- **Shell env hydration:** `$SHELL -ilc` (POSIX) or PowerShell (Windows); merges PATH, mise, fnm, CA and locale (`shell/DesktopShellEnvironment.ts`).
- **safeStorage:** encrypts the connection catalog and saved environments. Linux password-store selection is based on the desktop environment.
- **App activation socket** (Unix socket / named pipe) lets the `t3 app` CLI drive the running app.
- **CLI shim installer:** writes a `t3` launcher on PATH, or edits PATH in the Windows registry.
- **macOS permission helpers:** Screen Recording, Accessibility, Full Disk Access.
- **Windows:** AppUserModelId, NSIS Start Menu shortcut, `ffi-rs` user32 focus helpers.
- **Linux:** WM class, a Hyprland Chromium workaround, `.deb` metainfo. No Snap target exists; "snap" in this repo means SnapShot.
- **Passkeys:** in the in-app browser on macOS via `electron-webauthn`.

### 3.6 `native/` modules

| Module | Language | What it does | Used by |
|---|---|---|---|
| `native/libghostty-vt` | Zig upstream, C ABI (headers + VERSION pin only) | Ghostty terminal-emulation core: parser, screen, key/mouse encoders, kitty graphics | Web: WASM build (`apps/web/scripts/build-libghostty-wasm.sh`) → `apps/web/src/terminal/ghostty`. Android: JNI (`apps/mobile/modules/t3-terminal/android`). iOS: GhosttyKit.xcframework. **Directly reusable from Rust via FFI.** |
| `native/resource-monitor` | Rust 2024, `sysinfo` | `t3-resource-monitor`: JSON-lines child process sampling the process tree (CPU / mem / IO) with bounded history (1 h, 3600 snapshots, 64 MB); samples only while subscribers exist | Spawned by the **server** (`apps/server/src/resourceTelemetry/ResourceMonitorBinary.ts`); desktop passes its path. Feeds Settings → Diagnostics. **Reusable as-is in Waku.** |
| `native/browser-secret` | C (~60 lines) + libsecret, Linux | Reads Chromium "Safe Storage" password from the keyring for cookie import | `apps/desktop/src/preview/BrowserImport/LinuxBrowserSecret.ts` |
| `native/hyprland-snap-shot` | Rust, wayland-client / sctk, Hyprland toplevel-export protocols | `check` / `capture` / `activate` / `feedback` (flash overlay) for SnapShots on Hyprland | `apps/desktop/src/snapShot/HyprlandSnapShot.ts` |
| `native/kde-snap-shot` | Rust, zbus → `org.kde.KWin.ScreenShot2`, plus a QML feedback overlay | KWin window capture (identity-gated, so a dedicated executable) | `apps/desktop/src/snapShot/KdeSnapShot.ts` |

The rest of SnapShot covers macOS (screencapture / CG), Windows (xa11y region capture), GNOME (bundled shell extension in `apps/desktop/gnome-extension/`), Niri (socket) and generic XDG portal. Accessibility trees come from `@crowecawcaw/xa11y`.

### 3.7 Packaging

`scripts/build-desktop-artifact.ts`:

- **macOS:** dmg + zip, arm64 / x64 / universal, notarized.
- **Linux:** AppImage + deb, x64 and arm64; AUR package.
- **Windows:** NSIS x64 / arm64, Azure Trusted Signing; WSL runtime tarball.
- **Extra resources:** resource-monitor, cursor-sdk, Linux capture helpers, browser-secret, `server.asar` on Windows.
- **Build checks:** Rust targets are built per job.

---

## 4. Mobile app

T3 mobile is **not** a companion viewer. It is a near-complete second client. Stack: Expo SDK 58 / RN 0.88, React Compiler, React Navigation 7 static config (`apps/mobile/src/Stack.tsx`), roughly 1,400 files, 8 local native modules, and patched react-native-screens and react-native-enriched-markdown.

### 4.1 How it connects

**Shared runtime.** It uses the same `packages/client-runtime` as web: Effect RPC over WebSocket plus HTTP for assets, auth and bounded history.
- Wiring: `apps/mobile/src/connection/runtime.ts` and `platform.ts`.
- It runs on a hot-swappable atom runtime (`apps/mobile/src/lib/hot-swappable-atom-runtime.ts`).

**Ways to pair:**
- **Direct, LAN or Tailscale:**
  - Address plus pairing code, a QR scan (expo-camera), or a `t3code://…?pairingUrl=` deep link (`apps/mobile/src/features/connection/pairing.ts`, `ConnectionsNewRouteScreen.tsx`).
  - The saved connection holds `{httpBaseUrl, wsBaseUrl, bearerToken | dpopAccessToken, relayManaged}` (`apps/mobile/src/lib/connection.ts`).
  - iOS ATS allows local-network HTTP.
  - SSH environments are **not** supported on mobile.
- **T3 Connect:**
  - Sign-in uses Clerk (`@clerk/expo`, Apple / Google).
  - The app discovers linked environments from the relay.
  - The relay brokers a one-time bootstrap credential bound to a DPoP key held in SecureStore. After that, the client talks **directly** to the environment's tunnel host; the relay does not proxy WebSocket traffic.
  - Code: `apps/mobile/src/features/cloud/*`, `state/relay.ts`.

**Storage and offline:**
- Tokens live in `expo-secure-store`.
- An offline-first SQLite cache `t3code-client.db` holds shell, thread, server-config, vcs-refs, favicon and PR records (`apps/mobile/src/persistence/mobile-database.ts`).
- A **file-backed offline outbox** drains when connected (`state/thread-outbox*.ts`). Queued messages and drafts survive restarts.

**Reconnect.** The shared supervisor reconnects with jittered backoff (capped at 5 min). Mobile adds wake-ups for network-path change, app foregrounding (forced reconnect after more than 10 s in the background) and relay credential changes (`connection/app-state-wakeups.ts`).

**Push notifications.** These require T3 Connect.
- **Registration and delivery:**
  - The device registers its APNs / FCM token with the **relay**, not with the environment (`features/agent-awareness/remoteRegistration.ts`).
  - The environment publishes agent activity to the relay ("Publish agent activity" setting).
  - Events: finished, failed, needs approval, needs input.
- **iOS:** Live Activities via expo-widgets, with push-to-start tokens (`agentLiveActivity.ios.ts`).
- **Android:** a custom FCM service produces ongoing / Live Update notifications (`modules/t3-agent-notifications`).
- Foreground alerts are suppressed.

### 4.2 Screens

**Home** has Threads and Pulls tabs. Native tabs on iOS; a sidebar / FAB navigation on Android.

**Thread** (`features/threads/ThreadDetailScreen.tsx`, feed `ThreadFeed.tsx` at 2.6k LOC) has these sub-routes:
- terminal
- devices
- browser
- review
- review-comment
- files and file
- MCP app (full screen)
- attachment
- sheets: thread settings, queue, agents (subagents)
- git: overview, PRs, commit, branches, confirm

**Other top-level areas:**
- PR stack: detail, guide stop, files, file, reviewers, discussion, inline comment, review sheet.
- New-task stack: project picker, draft, environment / branch / model pickers, add project (local, clone, new).
- Settings stack, about 25 screens:
  - T3 Account, Environments and Environment detail (check for updates, refresh providers)
  - New threads, Source control, Agent behavior, Provider accounts, Maintenance
  - Notifications, Thread behavior, Follow-ups, Keyboard, Microphone, Appearance
  - Organization / project grouping, Project overview, Scheduled tasks (+ editor), Usage, Archive
  - Client storage, Diagnostics (startup crash log), Licenses / Legal
- Connections.
- iPad / foldable adaptive split view with a sidebar and inspector pane (`features/layout/AdaptiveWorkspaceLayout.tsx`).

### 4.3 Feature coverage vs web

| Area | Coverage | Notes |
|---|---|---|
| Composer | Yes | Native rich editor module `t3-composer-editor` (Swift / Kotlin) with chips; attachments (photos, files, paste, **share extension**, up to 8 items); `/`, `$`, `@path`, PR mentions; model / effort / permission / plan pickers; queue vs steer (long-press); **on-device voice input (iOS 26)** with mic priority ranking. Does **not** create citations. |
| Transcript | Yes | Native Markdown (`react-native-enriched-markdown`), Shiki, work log, HTML renders and MCP apps in a WebView, Quick Look / AVKit media (`modules/t3-native-controls`), fork, copy prompt link. |
| Approvals, questions (with attachments), secrets | Yes | |
| Terminal | Yes | Native Ghostty (iOS GhosttyKit.xcframework; Android `libghostty-vt.so` + JNI + Canvas); multi-session; "Attach output" to composer. |
| Files | Yes | Tree, search, native selectable source text (`t3-source-text`), Markdown / image / video / HTML / audio / PDF previews. |
| Review and diffs | Yes | Native diff canvas (`t3-review-diff`), per-turn diffs, line comments into chat. |
| Git | Yes | Commit, branches / worktrees, pull, merge back, project scripts. |
| Pull requests | Yes | List, detail, timeline, files with viewed marks, inline comments, review submit, merge / close / draft, stacks, guide, AI helper actions, offline cache. |
| Thread list | Yes | Search, filters, pin, settle, snooze (custom), archive, rename, regenerate title, "Arrange threads" sheet. |
| Browser and device streams | Yes | Server-streamed WebView with PiP; device viewer with hardware buttons and shake gesture. |
| Usage and limits | Yes | SwiftUI charts via `@expo/ui`; **home / lock-screen Subscription usage widget** (iOS WidgetKit + Android Glance). |
| Scheduled tasks | Yes | Interval, fixed time, webhook (cannot enable signatures). |
| Keyboard | Yes | iPad / hardware keyboard command palette and shortcuts via native UIKeyCommand (`features/keyboard/*`). |
| OS integration | Yes | App quick actions (expo-quick-actions), OTA updates (expo-updates), Material You (Android 12+). |
| Not on mobile | — | SSH, SnapShots, in-app (Electron) browser features such as recording, picking and cookie import, citations creation, keybinding editing, theme editor, Diagnostics traces, provider instance management (accounts only), linking / unlinking PRs. |

### 4.4 Recommendation for the Waku fork

- **Parity requires a mobile client.** The T3 parity surface includes push, Live Activities, widgets, offline outbox, share sheet, voice input, mobile PR review and remote device / browser streams.
  - A GPUI desktop app cannot provide any of these.
  - GPUI does not target iOS or Android in production today.
- **Waku already has an Expo app.** It ships `apps/mobile` (expo-router, `@waku/client`, `expo-libghostty`). The pragmatic path is to keep a React Native mobile client and point it at the same server protocol the GPUI desktop speaks.
  - **If the fork adopts T3's server protocol**, porting or reusing T3's `apps/mobile` together with `packages/client-runtime` is far cheaper than growing Waku's mobile app to parity (T3 mobile ≈ 1,400 files).
  - **If Waku keeps its own backend**, the mobile parity list above is the backlog.
- **Push depends on a relay.** Push requires a cloud relay (T3 Connect equivalent: Clerk + DPoP + Cloudflare tunnels + APNs / FCM registration). This is infrastructure, not client work.
- **Biggest native pieces on mobile.** Composer editor, diff canvas, terminal and source text are native modules. They are the main reason T3 mobile feels fluid, and they are also the main reimplementation cost (XL).

---

## 5. Rendering and performance techniques

T3's stated non-negotiables (`AGENTS.md`):

- "sending too much data over websockets, css animations causing gpu spikes, lists being hard to render"
- "**No continuously repainting animations; they peg the GPU on high-refresh displays.**"

These map directly onto a GPUI design.

### 5.1 Wire and payload discipline

Sources: `docs/internals/performance-regressions.md`, regression command `vp run test:perf:v2-wire`.

- **Bounded cold open.** A thread opens with about **75 recent timeline rows / ~1 MiB** encoded. Older activity comes in bounded **HTTP pages**, merged without disturbing the scroll window (`client-runtime/src/state/threadHistory*.ts`, `boundedThreadSnapshotHttp.ts`).
- **Bounded resume catch-up.** Resume replays at most **128 events / 1 MiB**, else it falls back to a bounded snapshot.
- **Bodies omitted at the wire boundary.** Raw command output, dynamic-tool results and inline file-change bodies are never shipped in orchestration events, even when small. Status and counts are preserved via flags and IDs. Diffs and files are fetched on demand.
- **Live command output is a separate, scoped subscription.** `orchestration.subscribeCommandOutput` is opened only by an expanded command row and closes with it. The server keeps one bounded tail per running command and throttles each subscriber (`apps/server/src/orchestration-v2/CommandOutputHub.ts`). Output deltas are never routed through the event store.
- **Shell vs detail split.** The initial shell carries **active navigation rows only**. Archived threads come from a separate query, and message bodies live only in thread detail. Shell resume sends deltas plus compact repository-enrichment metadata.
- **Fewer pre-send reads.** Sends resolve delivery server-side under a per-thread lock, so clients do not fetch the full projection before dispatching.
- **Viewport-leased queries.** Command palette and sidebar VCS / PR-status queries are leased to the visible viewport plus overscan. Cached badges remain when leases are released.
- **Identity-stable polling.** Unchanged desktop bootstrap reads keep array identity, so polling does not invalidate subscribers.
- **Terminal byte caps.** The server sends incremental chunks with 5,000 lines / 8 MiB retention. Clients cap at 512 KiB. Replay strips terminal query/response traffic and detaches the PTY writer.
- **Keepalive.** WebSocket ping every 5 s, 15 s timeout (`client-runtime/src/rpc/protocol.ts`).
- **Resource telemetry only on demand.** The native monitor samples only while there are subscribers; idle means interval 0 (`docs/internals/resource-telemetry.md`).
- **Bounded relay awareness.** One pending publication per thread, at most 5 retries, and no traffic for transcript deltas.

### 5.2 List virtualization

- **LegendList (`@legendapp/list`) is the virtualizer everywhere.**
  - Web: timeline `MessagesTimeline.tsx` / `ChatView.tsx`, command palette `CommandPaletteResults.tsx`, model picker, branch picker, font picker, thread PR panel.
  - Mobile: `KeyboardAwareLegendList` in `ThreadFeed.tsx`, plus home, archive and PR lists with `recycleItems` and `estimatedItemSize`.
- **Timeline scroll behaviour.** The timeline uses `maintainScrollAtEnd` / follow-tail logic, anchored prepends (`timelineScrollAnchoring.ts`) and an end-space filler.
- **Citation navigation across virtualization.** It must fetch, unfold, mount and measure the virtual row before positioning (`docs/internals/assistant-citations.md`).
- **Folding.** Turn folds, attempt folds and "work-toggle" rows collapse long tool runs so the row count stays small. Large diffs load files lazily as you scroll; diff find searches only the loaded files.

### 5.3 Streaming Markdown and highlighting

- **Incremental Markdown parse** (`apps/web/src/markdown-incremental.ts`). Parsing caches the AST of a completed prefix that ends at a closed top-level code fence followed by a blank line, and re-parses only the suffix on each token. Definitions or CR/BOM force a full parse.
- **Incremental syntax highlighting** (`apps/web/src/lib/incrementalHighlighting.ts`). It caches the Shiki grammar state at the last complete line and tokenizes only new lines while code streams.
- **Highlighting off the main thread.** Diff highlighting runs in a **Web Worker pool** (`DiffWorkerPoolProvider.tsx`, `@pierre/diffs` worker). Mobile uses a highlight scheduler with prewarming (`reviewDiffHighlightScheduler.ts`). Very large composer code blocks skip highlighting.
- **Response streaming mode** is a server setting (`responseStreamingMode`).

### 5.4 Memoization and React discipline

- **React Compiler mandatory on web and mobile.** `reactCompilerCoverage.test.ts` fails on any skipped component. Lint rule `t3code/no-manual-memo` bans `useMemo` / `useCallback`, and `t3code/no-hookless-use-function` bans hookless `use*` functions.
- **Closure retention.** In long-lived giant components (`ChatView`, `ChatComposer`), handlers are plain functions wrapped with `useStableCallback` only at boundaries. This avoids V8 shared-closure scopes retaining old renders' server config and thread data, which is a memory leak class (`docs/internals/web-ui.md`).
- **Fine-grained subscriptions.** `effect/reactivity` atom families give per-entity subscriptions, for example `itemSupportAtomFamily` with an equality-guarded previous value (`apps/web/src/state/v2ItemSupport.ts`). Heavy subscribers sit in null-rendering leaves (mobile `ThreadOutboxDrainWorker`).
- **Timers bypass React.** Live elapsed timers write `textContent` directly from a 1 s interval inside a layout effect, so a running timer never re-renders the chat view (`ProviderSubagentBar.tsx`, `AgentElapsed.tsx`).
- **Terminal off-React.** The terminal render loop is off-React: Ghostty WASM → batched Canvas 2D runs (`terminal/ghostty/renderer.ts`), with a single WASM instance per tab.

### 5.5 Animation policy

- **Pause offscreen animations.** `observeVisibleAnimation` (`apps/web/src/lib/visibleAnimation.ts`) toggles CSS `--visible-animation-state` and `will-change` via IntersectionObserver plus `visibilitychange` plus `prefers-reduced-motion`. Spinners and shimmers stop when offscreen, when the tab is hidden, or under reduced motion.
- **Instant panels by default.** Panel open/close is instant (0 ms); the user can opt into 0–400 ms (`panelAnimations.ts`). Switching threads snaps without replaying transitions.
- **Mobile.** Reanimated 4 worklets; entering animations only for newly born rows; UI-runtime GC on iOS memory warning.

### 5.6 Documented regressions and fixes

| Regression | Fix |
|---|---|
| v1-seeded benchmarks gave false reassurance for the v2 client | v2 wire perf test |
| Command-output deltas flooded the event store and blocked ingestion | Separate output hub |
| Full snapshots retained duplicate conversation-message tables | Bounded snapshot |
| Sequence cursors scanned history | Indexed seeks |
| Startup loaded completed thread histories | Recovery candidates selected from projection state |
| Synchronous trace/log appends stalled the event loop (Windows) | Async serialized writes |
| Polling invalidated subscribers | Stable array identity |
| Live VCS queries for every palette row | Viewport leases |
| Relay awareness generated continuous traffic during outages | Retry budget |
| Terminal history materialized full scrollback per callback | Incremental chunks |

`AGENTS.md` also calls out the commit-title pattern `fix(web): new threads no longer spike CPU` as typical of the class of bug watched for.

### 5.7 Implications for a GPUI implementation

- **Reuse the protocol's discipline as-is:**
  - bounded cold open plus HTTP history paging;
  - scoped command-output subscription;
  - shell / detail split;
  - viewport-leased status queries.

  Most of T3's perf wins are protocol shape, not React tricks.
- **GPUI repaints on demand.** Any spinner, shimmer or elapsed timer must request frames only while visible, and timers should tick at 1 Hz with text-only invalidation. Never run a continuous animation loop; this is the high-refresh-display trap `AGENTS.md` names.
- **Virtualize everything.** Use GPUI `uniform_list` / `list` with measured heights for the timeline (variable height, anchored prepend, follow-tail), sidebar, palette and diff files.
- **Stream incrementally.** Port incremental Markdown (prefix cache at closed fences) and incremental highlighting (tree-sitter incremental parse, or syntect state caching per line). Move diff highlighting to a background executor.
- **Terminal.** Link `libghostty-vt` natively via its C ABI (`native/libghostty-vt` pins the revision) instead of a WASM / Canvas path. This shares terminal semantics with T3 mobile.
- **Resource monitor.** Reuse `native/resource-monitor` (Rust, `sysinfo`) directly for diagnostics.

---

## 6. Gaps and notes

- **Stale internal doc.** `docs/internals/composer-context-references.md` still says "Lexical node". The current editor is **Tiptap** (`ComposerPromptEditor.tsx` simply re-exports `ComposerPromptEditorTiptap`).
- **No i18n** anywhere in the clients; there is nothing to port.
- **No tray, no launch-at-login, no power-save blocker** in desktop.
- **Biggest client-side XL items for GPUI parity**, in rough order:
  1. Rich Markdown composer with inline chips.
  2. PR review client.
  3. Connection catalog / multi-route / T3 Connect.
  4. In-app browser (needs an embedded webview such as wry/WebView2/WKWebView, plus CDP for agent control).
  5. SnapShots.
  6. Device panel (H.264 decode + input + 3D frames).
  7. Sidebar with drag semantics.
- **Webview needs.** HTML renders and MCP Apps also need a sandboxed webview inside the GPUI app.
