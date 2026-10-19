# Waku extra notes (from core + desktop sweeps)

## Core
- Daemon: JSON over WS, PROTOCOL_VERSION 7, 48MiB frames, base64 blobs in JSON. Thread-per-connection, 25ms poll accept loop, MAX_CONNECTIONS 64 silent drop. No TLS, no pairing, single full-control token, no keepalive ping.
- Replay journal in memory, 2048 events/runtime, epoch UUID per daemon process. Terminal output ephemeral (no scrollback replay). Slow subscriber kicked at 16384 queue.
- Local client not auto-reconnected if kicked while child alive (process.rs L561-574).
- Daemon dies with desktop (--parent-pid) -> no background service.
- CRITICAL: transcripts are CLIENT projections; daemon doesn't fold driver events into AgentSession. No client attached = nothing persisted beyond 2048-event journal. Blocks background agents / scheduled tasks.
- Persistence: rusqlite bundled, WAL, tables projects/sessions/session_details(JSON blob of whole AgentSession minus messages)/messages. Search = instr(lower(content)), no FTS. drizzle build-time only.
- Checkpoints: hidden refs refs/waku/session-<id>-turn[-start|-diff]-<n>, temp index; restore whole cwd; refs accumulate; forks share worktree.
- Git: shells out to git. Worktrees ~/.waku/worktrees/<project>/<slug> branch waku/<slug>; NO cleanup ever. Review diffs: LastTurn/Uncommitted/Unstaged/Staged/Committed/Branch, 32MiB cap. Commit msg AI gen.
- Daemon terminal (alacritty_terminal, unix only) used by web/mobile; desktop uses own local PTY -> unavailable remote.
- Usage: plan limits Claude/Codex/OpenCode Go/Grok; history Claude+Codex only, LiteLLM prices.
- Skills mgmt (enable/disable rename SKILL.md), slash command discovery per provider.
- Images: NO native image parts to any provider; @path mentions only.
- MCP: only own waku_js_repl (QuickJS) injected; no user MCP management.
- No scheduler, tray, launchd, background service. Notifications only when window inactive.
- Computer use: debug builds only.
- Subagents: BackgroundWork items, not nested transcripts (Claude best, Codex collabAgentToolCall).
- Plan mode removed. RuntimeMode Ask|AutoAcceptEdits|Auto|FullAccess. UserInputRequested supported Claude/Codex/OpenCode/ACP/DeepSeek.

## Desktop
- Views: sidebar, header, transcript (virtualized, turn rail, find), composer, queued card, footer (project/worktree/branch/usage), permission/question cards, CU overlay, autocomplete, model picker (favorites), traits menu, access menu, goal dialog (Codex), commit dialog, branch picker, project picker, cmd palette (⌘K), task switcher (ctrl-tab), image preview, background-work popover, usage meter, usage page, skills page, settings, file find/replace, toasts.
- Right panel tabs: Browser (wry; no Linux), Terminal, Files (editor w/ save, md preview), File, Diff/Review (unified only, 50k lines trunc, no stage/revert/comment), BackgroundWork.
- Settings: General, Appearance (theme sys/light/dark, lang en/zh/ja, font sizes), Providers, Skills, Usage, Daemon, Computer Use.
- Sidebar: group by project/date; rename/remove only; no pin/archive/snooze/project removal.
- Transcript: md parser in-house, highlight in-house, math (ratex), streaming veil fade; activity kinds Reasoning/Command/FileChange/FileRead/FileSearch/FileList/Search/Plan/Tool. No citations. Rewind/fork/edit message.
- Gaps: PR creation, pin/archive, custom themes, dock badge, notif prefs, line:col in editor, split diff, Open-in only macOS, remote limitations (project picker, terminal, reveal).
- Analytics: Umami.
- Updater: Sparkle mac, custom appcast win/linux.
