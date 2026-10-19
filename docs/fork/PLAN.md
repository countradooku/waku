# Waku fork → T3 Code parity, faster and more fluid

**Audited:** `pingdotgg/t3code` @ `a71a6c37d1` (2026-10-19, also in [`T3_PIN`](T3_PIN)) and `egoist/waku` @ `10bcd72` (2026-10-02). The fork was created from upstream `27d381b`; line references in the audits may have shifted slightly.

**Execution:** every item is a GitHub issue. Phase epics are #163–#172; the fluidity track is #173. Design drafts are in [`rfcs/`](rfcs/).
**Method:** five parallel read-only audits plus six sub-sweeps. Nothing was built or profiled, so every performance number here is an estimate that the benchmark harness (§5) must confirm.

The full audit reports are in [`audit/`](audit/):

| Report | Covers |
|---|---|
| [t3-server.md](audit/t3-server.md) | Orchestration v2, ~195 RPCs, 83 MCP tools, storage, wire rules |
| [t3-clients.md](audit/t3-clients.md) | ~150 UI features, 14 settings pages, ~70 keybindings, the mobile app |
| [t3-providers.md](audit/t3-providers.md) | Provider matrix, adapter contract, source control, relay, SSH, `native/` |
| [t3-provider-adapters-detail.md](audit/t3-provider-adapters-detail.md) | Per-adapter details |
| [waku-features.md](audit/waku-features.md) | Waku's feature catalogue |
| [waku-core-and-desktop-notes.md](audit/waku-core-and-desktop-notes.md) | Daemon internals and desktop views |
| [waku-perf.md](audit/waku-perf.md) | 18 ranked hotspots, fixes, and a benchmark harness |
| [t3-terminal-tasks-telemetry-persistence.md](audit/t3-terminal-tasks-telemetry-persistence.md) | Terminal, scheduled tasks, notifications, telemetry, on-disk layout |
| [t3-remote-auth-install-cli.md](audit/t3-remote-auth-install-cli.md) | Effect-RPC wire format, auth, pairing, relay, SSH, CLI, updates |

---

## 0. Summary

1. **Waku is the right base, with a gap at its core.** It is already a native Rust/GPUI app with a separate daemon, and it is more efficient than Electron.
   - **The core gap:** the desktop builds and saves the transcript, not the daemon. With no window attached, nothing is saved beyond a 2,048-event in-memory buffer.
   - **What depends on fixing it:** background agents, scheduled tasks, remote clients that see the same state, and crash recovery. T3's v2 orchestrator covers all four with a design we can copy: commands, a pure decision step, then one SQLite transaction holding the events, projections, receipt and outbox, then effect workers.
   - **Plan:** rebuild `waku-daemon` as a Rust event-sourced orchestrator. Everything else in the parity work depends on this.
2. **The parity surface is large.** T3 is about 210k lines of server TypeScript (83k of it orchestration v2) plus about 57k lines of shared client runtime and a near-complete second client on mobile.
   - Port the semantics and contracts, not the code.
   - The work is executed by AI agents, issue by issue, with **one pull request per phase**. The plan orders work by dependency, not by calendar.
3. **Speed comes from structure, not micro-optimisation.** Waku's remaining perf ceilings are:
   - saves run on the UI thread;
   - sockets poll every 25 ms;
   - every streaming commit re-renders the whole window;
   - streaming commits are capped at 8 Hz because GPUI's `list()` rebuilds every visible row on every frame.

   The first three are contained fixes. The last needs list rows that are measured once and replayed in the GPUI fork, which unlocks streaming at display rate. That is the "visibly more fluid than T3" win.
4. **Five decisions** in §2 change the plan's shape. D1 is settled: the daemon mirrors T3 v2 semantics but speaks **our own typed protocol**, and no T3-compatible gateway is planned. So web and mobile parity are built on Waku's own clients.

---

## 1. Where each app stands

| | T3 Code | Waku (today) |
|---|---|---|
| Shell | Electron 44 plus a Node server child; React 19 web bundle | GPUI (the `egoist/zed` fork, branch `waku-webview`), native |
| Backend | Node/Effect, event-sourced v2 orchestrator, SQLite `statev2` (64 migrations) | Rust `waku-daemon` over `waku-core`; rusqlite; **client-owned projections** |
| Wire | Effect-RPC over WebSocket, ~195 methods, protocol v2 gate, bounded snapshots and history paging, 5 s ping | JSON over WebSocket, 53 commands plus 27 workspace operations, protocol v7, 2,048-event replay journal, 25 ms polling, no ping, no TLS |
| Clients | Web (local and hosted), desktop, a near-complete Expo mobile app (~1,400 files, 8 native modules) | GPUI desktop; web (TanStack, close to parity on the core loop); an early Expo mobile companion |
| Providers | Codex, Claude (Agent SDK), Cursor (SDK), Grok, OpenCode 1.x/2.x, Antigravity, Pi, Muse, ACP Registry | Codex, Claude (stream-json), Cursor/Fx/Grok/Kimi (ACP), OpenCode 2, Pi, Oh My Pi, Amp, DeepSeek |
| License | MIT | **GPL-3.0-only**, so the fork must stay GPL. MIT code from T3 may be copied in if its notices are kept. |

### Waku advantages to keep

- Native drivers T3 lacks: Amp, Kimi, Fx, DeepSeek, Oh My Pi.
- Native math rendering (RaTeX).
- An in-house incremental Markdown parser.
- Rewind with git checkpoints.
- An AI-written commit message.
- A QuickJS REPL and MCP server (`waku_js_repl`). This is a natural host for T3-style **workflow scripts**.
- Computer Use. It is debug-only today.
- i18n in English, Chinese and Japanese; T3 has none.
- A self-parking animation clock.
- About 1,020 Rust tests.

---

## 2. Decisions to make before Phase 2

### D1: what protocol the daemon speaks (the biggest lever)

| Option | Gain | Cost | Verdict |
|---|---|---|---|
| **A. Own protocol** (evolve `waku-protocol` to v8 with T3 v2 semantics) — **chosen** | Full control. Typed binary frames for the native client. | Mobile and web parity must be built by us: T3 mobile is about 1,400 files. | Safe, but slow for mobile. |
| **B. Speak T3's wire protocol** (Effect-RPC, ~195 methods, `ORCHESTRATION_PROTOCOL_VERSION=2`) | T3's **MIT web and mobile clients work against our daemon**. We could fork them and get mobile parity almost free. | We chase a fast-moving TypeScript contract we don't own. Effect Schema encoding has to be matched exactly. The relay and push stack (Clerk, Cloudflare) comes with it. | High leverage, high coupling. |
| **C. Hybrid** (was recommended; superseded by the decision below) | The daemon's domain model mirrors T3 v2 **one-to-one**: same entities, entity-snapshot events, turn-item variants and capability groups. The native client uses a typed binary protocol. A separate **`t3-compat` gateway** crate can later expose T3's wire format. | A small extra layer. The gateway is postponed. | Keeps option B open without blocking Phase 2. |

> **Decided (2026-10-20): our own protocol (option A, with option C's domain model).**
> - The daemon's domain model still mirrors T3 v2 one-to-one: entities, entity-snapshot events, turn-item variants and capability groups. That is a proven model, and it keeps T3 a useful reference.
> - On the wire, every client speaks **our own typed protocol**, `waku-protocol` v8: binary frames for the native client, JSON for web and mobile.
> - **No T3-compatible gateway is planned.** T3's web and mobile clients are not reused.
> - Waku's own `apps/web` and `apps/mobile` are grown to parity instead (see D2).

What option B costs ([t3-remote-auth-install-cli.md §0](audit/t3-remote-auth-install-cli.md)):
- T3's socket carries Effect RPC serialized as JSON. A Rust server has to reproduce its framing: request, chunk/stream, exit, interrupt, ack, ping, and tagged errors.
- Terminal output has its own flow-control window.
- The server uses per-message deflate.
- Together that makes the gateway XL on its own.

### D2: mobile

- Keep a React Native client, because GPUI does not target iOS or Android in production.
- Grow Waku's `apps/mobile` (Expo) toward the T3 list in [t3-clients.md §4.3](audit/t3-clients.md). Order: thread list, transcript, composer, approvals, terminal, then git and PRs, push, widgets and the offline outbox.
- T3 mobile is MIT, so individual native modules (composer editor, diff canvas, terminal) can be lifted where that saves time, with their notices kept.
- Generate the TypeScript client from the Rust protocol, as `packages/waku-client` already does, so web and mobile never drift from the daemon.

### D3: cloud relay and push

T3 Connect runs on Cloudflare Workers, Durable Objects, PlanetScale Postgres and Clerk. It adds managed tunnels, DPoP-bound credentials, APNs/FCM and Live Activities.

Options:
- (a) Build an equivalent. XL, and it costs money to operate.
- (b) Ship Tailscale, LAN pairing and SSH only, and add push later through a minimal relay.

Recommend (b) first.

### D4: GPUI ownership

Waku pins `egoist/zed@waku-webview`. The fluid-streaming work (§5.3) needs changes inside GPUI, so the fork must either maintain its own GPUI fork rebased on upstream Zed, or upstream the changes. Budget for owning it.

### D5: identity

- New name and bundle ids.
- Signing keys, the Sparkle/appcast feeds, an R2 bucket and the analytics endpoint.
- Keep GPL headers and add MIT notices for any T3-derived code.

---

## 3. Target architecture

```
GPUI desktop ─┐    typed binary frames (local UDS / named pipe), wss:// remote
web (TS)  ────┼──► waku-daemon ── session gateway (auth scopes, pairing, ping, cursors)
mobile (RN) ──┘         │
                        ├─ Orchestrator (pure decide: Command → Events + Effects)
                        ├─ EventSink: ONE sqlite txn = events + projections + receipt + outbox
                        ├─ EffectWorker pool (lease, per-thread FIFO, process-loss reconcile)
                        ├─ ProviderSessionManager → DriverV2 trait (normalized entity-upsert stream)
                        │      codex · claude · cursor · acp(grok/kimi/fx/registry) · opencode · pi/omp · amp · deepseek
                        ├─ Streams: shell / thread (bounded snapshot + ≤128-event replay) / command-output hub
                        ├─ MCP server (rmcp, streamable HTTP, per-session scoped tokens, OAuth for outside agents)
                        ├─ Scheduler (interval / fixed time / webhook) · PR watch · auto-settle · usage-limit resume
                        ├─ Git/VCS · checkpoints · worktrees + setup scripts · source-control hosts
                        ├─ PTY host (all platforms) · file system · attachments/blobs (binary frames)
                        └─ runs as launchd/systemd/Windows service, outlives the desktop
```

### Rules carried over from T3

- App ids are primary; provider ids are only references to them.
- Run, provider turn and run attempt are separate concepts.
- Only the root node completing ends a turn the user sees.
- Every event carries the full entity, so applying it is an upsert by id, and unknown event kinds are skipped instead of failing (`#[serde(other)]`).
- Command output never becomes an event.
- Large tool outputs and file bodies stay off the wire and are fetched on demand.
- Test with replay transcripts: only the provider transport is faked. Reuse T3's `record-*-replay-fixture` transcript format.

### Driver migration

Waku's `DriverControl` trait (prompt, steer, cancel, respond, rollback, fork) maps onto T3's `ProviderAdapterV2`:

| T3 method | Waku equivalent |
|---|---|
| `ensureThread` / `resumeThread` | Start the session and resume from the stored cursor |
| `startTurn` | prompt |
| `steerTurn` | steer |
| `interruptTurn` | cancel |
| `respondToRuntimeRequest` | respond |
| `rollbackThread` | rollback |
| `forkThread` | fork |
| `readThreadSnapshot` | Partly exists already |

- **Capability flags.** Move them from `ProviderKind` to per-session `Capabilities`, using T3's 12 groups.
- **Driver output.** Each driver emits `AdapterEvent::{SessionUpdated, ProviderThreadUpdated, TurnUpdated, NodeUpdated, SubagentUpdated, MessageUpdated, TurnItemUpdated, CommandOutputDelta, RuntimeRequestUpdated, PlanUpdated, TurnTerminal}` instead of today's `DriverEvent`.

---

## 4. Parity matrix (gap → work → phase)

Legend: ✅ have, 🟡 partial, ❌ missing. Sizes are relative complexity: S small and contained, M several components, L a subsystem, XL multiple subsystems or high risk.

### 4.1 Orchestration and session model

| T3 feature | Waku | Work | Size | Phase |
|---|---|---|---|---|
| Event-sourced core: commands, receipts, outbox, effect workers, idempotent dispatch | ❌ (client projections) | New `waku-orchestrator` crate | XL | 2 |
| Execution graph: Run, RunAttempt, ExecutionNode, ProviderThread/Turn, 26 turn-item variants | ❌ | Projection tables plus Rust enums | XL | 2 |
| Bounded snapshot + cursor streams, history paging, command-output hub, live budgets | 🟡 (2,048-event replay) | Shell and thread streams | L | 2 |
| Restart recovery and continuation, process-loss reconcile | ❌ | Recovery service | L | 2 |
| Daemon as a background service (launchd/systemd/Windows) | ❌ (dies with the desktop) | Service install plus a CLI | M | 2 |
| Server-persisted queue: reorder, edit, promote to steer, hold, resume | 🟡 (client queue, steer) | Commands plus UI | M | 3 |
| Steering policy (native steer, or interrupt and restart) | 🟡 | Capability-driven policy | M | 3 |
| Plan mode, proposed plans, implement or refine, todo lists | ❌ (removed) | Restore per provider | M | 3 |
| Approvals (once / session / always) and multi-question input with file attachments | 🟡 (no attachments) | Extend | M | 3 |
| Secret requests (private card, value kept in a secret store) | ❌ | | S | 3 |
| Subagents as child threads (native) and app-owned **delegated tasks** with completion wakes and stop cascade | 🟡 (background-work items) | Child threads | XL | 3 |
| Lazy forks (native or portable), merge-back, lineage UI | 🟡 (eager fork, shared worktree) | ContextTransfer | L | 3 |
| Provider switch mid-thread with a budgeted handoff (16k tokens) | ❌ | Handoff service | L | 3 |
| `/goal`, `/compact`, usage-limit auto-resume and snooze | 🟡 (Codex `/goal`) | | M | 3 |
| **Native image input** to providers | ❌ (`@path` text) | Per driver: Codex `localImage`, Claude image blocks, ACP image content, OpenCode file parts | M | 3 |
| Multi-model fan-out (one prompt across N models, each in its own worktree) | ❌ | | S | 3 |
| Workflows (orchestrator scripts that spawn phased agent groups) | ❌ | Run them on QuickJS; the stop/script RPCs | L | 4 |

### 4.2 MCP and agent tools

| T3 feature | Waku | Work | Size | Phase |
|---|---|---|---|---|
| App MCP server `/mcp`: 83 tools across orchestrator, thread, queue, project, worktree, PR, attachment, environment, preview, HTML and device toolkits | ❌ (only `waku_js_repl`) | `rmcp` server; ship the tools in tranches | L | 4 |
| Per-session scoped credentials, privilege ceiling, injection into each provider (including an ACP stdio↔HTTP bridge) | ❌ | | M | 4 |
| Outside agents: OAuth 2.1 + PKCE, read-only or capped grants | ❌ | | M | 4 |
| Scheduled tasks (interval / fixed time / webhook with HMAC), delivery log | ❌ | Scheduler plus UI | L | 4 |
| PR watch wakes | ❌ | | M | 6 |
| MCP Apps (interactive iframes) and HTML renders | ❌ | Sandboxed wry webview inside GPUI | L | 5 |

### 4.3 Git, worktrees and source control

| T3 feature | Waku | Work | Size | Phase |
|---|---|---|---|---|
| Hidden-ref checkpoints, turn and thread diffs, rewind with "keep changes" or "revert files" | ✅ / 🟡 | Add scope nesting and the shared-workspace safety check | S | 3 |
| Commit, push, pull, AI commit message, create PR with an AI title and body, init, publish | 🟡 (commit, push) | Pull, PR, publish | M | 6 |
| Worktree setup scripts (`t3.json`-style), project scripts with keybindings, run-on-settle | ❌ | | M | 6 |
| Worktree cleanup policies and storage page | ❌ (never cleaned) | | S | 2 |
| PR inbox and detail (summary, timeline, code with viewed marks, checks, guide), review, merge | ❌ | **GitHub first** (REST + GraphQL), then GitLab, Bitbucket, Azure DevOps, Forgejo | XL | 6 |
| Stacks and review guides | ❌ | | L | 6 |
| Linked PRs per thread, branch auto-linking, auto-settle on merge | ❌ | | M | 6 |
| Clone, new project, open in editor (all platforms) | 🟡 (Open in… on macOS only) | | M | 5 |

### 4.4 Composer

| T3 feature | Waku | Work | Size | Phase |
|---|---|---|---|---|
| Rich Markdown composer that styles as you type, with code blocks and list continuation | ❌ (plain `TextInput`) | GPUI rich editor: rope, block model, IME | **XL** | 5 |
| Inline context chips (`t3-context://`): files, terminal excerpts, review comments, PRs, threads, skills, elements; chip clipboard | ❌ | Inline atoms in the editor | L | 5 |
| Trigger menus: `/` commands and skills, `$` skills, `@` files and threads, `#` PRs | 🟡 (`/` and `@` files only) | | M | 5 |
| Attachments: 100 files, HEIC conversion, resize, upload queue; large paste becomes a file | 🟡 (32 MB, base64 JSON) | Binary upload frames | M | 3 |
| Prompt history (↑/↓), prompt stash (mod+S), citations from assistant text, prompt links | ❌ | | M | 5 |
| Voice input | ❌ | OS speech-to-text: macOS `SFSpeechRecognizer`, Windows `SpeechRecognizer` | M | 9 |

### 4.5 Transcript

| T3 feature | Waku | Work | Size | Phase |
|---|---|---|---|---|
| v2 lifecycle rows (compaction, handoff, steered, queued→promoted, sent by agent or automation) | ❌ | Driven by turn items | M | 5 |
| Grouped work log, live command output, open diff | 🟡 | Scoped output subscription | M | 5 |
| Minimap, follow-tail, anchored prepend, "load earlier" paging | 🟡 (turn rail) | | M | 5 |
| Server-side find across a thread, including unloaded history | 🟡 (local find) | | M | 5 |
| Mermaid | ❌ | `mermaid-rs` / headless render to SVG, cached | M | 9 |
| GitHub alerts, footnotes, video, image gallery | 🟡 | | S | 5 |
| Math | ✅ (better than T3) | Keep | — | — |

### 4.6 Terminal, files and review

| T3 feature | Waku | Work | Size | Phase |
|---|---|---|---|---|
| Daemon-owned PTYs everywhere, including Windows; scrollback replay (5k lines / 8 MiB) | 🟡 (Unix only, no replay; the desktop uses a local PTY) | One PTY host for all clients (`portable-pty`, ConPTY) | M | 2 |
| Terminal drawer: tabs, splits, persistent per thread, terminal excerpt to composer | 🟡 (right-panel tabs) | | M | 5 |
| Diff panel: split view, word wrap, hide whitespace, find across files, comments into the composer | 🟡 (unified only, no comments) | | L | 5 |
| File preview: Markdown, HTML, CSV, PDF, audio, images; save with revision checks | 🟡 (editor plus Markdown preview) | | M | 5 |
| Go to file (mod+P), project content search (mod+shift+F) | ❌ | `nucleo` + `grep-searcher` in the daemon | M | 5 |

### 4.7 Sidebar, navigation and settings

| T3 feature | Waku | Work | Size | Phase |
|---|---|---|---|---|
| Sections: Pinned, Active, Working, Snoozed, Settled; drag between sections; multi-select; undo | ❌ (group and rename only) | Server-owned order keys | L | 5 |
| Thread menu: pin, settle, snooze (custom), archive, delete, mark unread, regenerate title | 🟡 | | M | 5 |
| Archive page, scratch threads, importing Claude and Codex sessions | 🟡 (resume picker) | | S | 5 |
| Command palette with deep settings search and an add-project wizard | 🟡 | | M | 5 |
| **Configurable keybindings** (JSON plus `when` expressions, recorder, conflicts); ~70 commands | ❌ (hard-coded) | | M | 5 |
| 14 settings pages; scoped settings (environment × project) with inheritance | 🟡 (7 pages, global) | | L | 5 |
| Themes: editor, VS Code import, Open VSX, fonts per surface, contrast, chat width | ❌ (light, dark, system) | | L | 5 |
| Notifications (with sound / sound only), in-app toasts, dock badge | 🟡 | | S | 5 |
| Usage: cost, tokens and limits for all providers; pooled limits; price overrides | 🟡 (Claude and Codex only) | | M | 5 |
| Welcome wizard, diagnostics (traces, process tree), resource monitor | ❌ | Reuse T3 `native/resource-monitor` (Rust) | M | 5 |

### 4.8 Remote and multi-device

| T3 feature | Waku | Work | Size | Phase |
|---|---|---|---|---|
| Pairing (URL and QR), scoped sessions, revoke, authorized clients | ❌ (one full-control token) | | L | 7 |
| TLS / `wss://`, keepalive ping, reconnect for local clients too | ❌ | | M | 2 |
| Connection catalog with multiple routes (LAN, Tailscale, SSH, relay), preflight, cooldown | ❌ | | L | 7 |
| Desktop as a remote client: daemon-host folder picker, remote PTY, remote reveal | 🟡 (the protocol supports them; the desktop refuses) | Wire them up | S | 2 |
| SSH environments (bootstrap the daemon remotely, forward ports, password prompt) | ❌ | | L | 7 |
| Tailscale serve | ❌ | | S | 7 |
| Multi-environment client, load balancing | ❌ | | M | 7 |
| Relay, managed tunnels, push, Live Activities | ❌ | See D3 | XL | 8 |
| Mobile app at T3 parity | 🟡 (early) | See D1/D2 | XL | 8 |

### 4.9 Providers

| T3 feature | Waku | Work | Size | Phase |
|---|---|---|---|---|
| Provider **instances** (several accounts per driver, env vars, launch args, accent colour) | ❌ (binary override only) | | L | 3 |
| Sign-in flows (browser paste-back, terminal sign-in, ChatGPT handoff) | ❌ | | L | 7 |
| Install and update management with installer-ownership proof; remotely updated model manifest | ❌ | | L | 9 |
| ACP Registry (CDN catalog, SHA-verified installs, session import) | ❌ (generic ACP exists) | | L | 9 |
| Antigravity, Muse | ❌ | Muse's protocol is proprietary, so evaluate first | L each | 9 |
| Claude: background-task wakeups, workflows, reset credits | 🟡 | | M | 3 |

### 4.10 Desktop and OS extras (long tail)

| T3 feature | Waku | Size | Phase |
|---|---|---|---|
| In-app browser for agents: CDP automation, 20 `preview_*` tools, profiles, recording, element picker, annotations, cookie import, passkeys | 🟡 (wry webview, no agent control) | XL | 9 |
| Server-hosted headless browser streamed to clients | ❌ | L | 9 |
| Devices: iOS Simulator and Android Emulator streamed (H.264), agent device tools | ❌ | XL | 9 |
| SnapShots: global-shortcut window capture plus accessibility tree | ❌ | XL | 9 |
| Default browser handler, `waku app` CLI activation, quit-hold | ❌ | M | 9 |
| Auto-update channels (stable and nightly), "update everything" | 🟡 | M | 7 |

---

## 5. Performance program: beat T3 on CPU, memory and fluidity

### 5.1 Targets

Measure both apps through the same harness. Report the app plus daemon separately from provider CLIs.

| Metric | Target |
|---|---|
| Idle | < 1 frame/s with the composer unfocused; < 5 wakeups/s total (today about 120/s, estimated); CPU < 0.1% |
| Streaming CPU | ≤ ⅓ of T3's shell CPU **at a higher visual update rate** (display rate versus T3's React commits) |
| Frame time | p99 < 8.3 ms at 120 Hz while scrolling 2,000 messages with a terminal and diff open, and while streaming; 0 hitches during saves |
| Memory | RSS flat once N ≥ 5 long sessions are open; < ⅓ of T3's footprint |
| Cold start | First content < 250 ms warm, < 600 ms cold |
| 10k-line diff | First paint < 150 ms; scrolling p99 < 8.3 ms |
| Terminal | Throughput at least that of Zed's terminal; echo latency < 1 frame plus PTY latency |
| Local RPC | p50 < 1 ms (today about 25 ms, estimated) |

### 5.2 Phase 1 quick wins (these also reduce risk for Phase 2)

Numbers in brackets are the hotspot ranks in [waku-perf.md §2](audit/waku-perf.md).

1. **[#1] Saves off the UI thread.**
   - Take `Arc` snapshots, write on a background writer, and skip writes whose fingerprint has not changed.
   - Use compact JSON with a 500 ms trailing debounce, and no `F_FULLFSYNC` except at shutdown.
2. **[#2] Scope scroll redraws.** Replace `window.refresh()` in the transcript scroll handlers (`src/app.rs:2281,2292`) and in `ui/scrollbar.rs` with a notify of the pane entity.
3. **[#3] Sockets that wait instead of poll.**
   - Split reader and writer threads, or move to tokio with `tokio-tungstenite`.
   - Use a worker pool instead of a thread per request.
   - Use a Unix domain socket or named pipe for the local daemon.
4. **[#4] Targeted redraws.** Replace the root `cx.notify()` per commit with a `DirtySet { sidebar, transcript, right_panel, root }`.
5. **[#7] Wire.**
   - Use a typed `#[serde(tag)]` event enum instead of `Value`.
   - Serialize once into `Arc<Bytes>` and fan that out.
   - Bound the journal by bytes.
   - Coalesce text deltas in the daemon every 8–16 ms.
   - Send blobs as binary frames.
6. **[#6] Terminal.**
   - Event-driven wakeups instead of the 24 ms poll, with no work while hidden.
   - Use `Term::damage()` to rebuild only changed rows, and stop allocating a `String` per cell.
   - Alternatively, link `libghostty-vt` natively, which shares terminal semantics with T3 mobile.
7. **[#9] Memory bounds.**
   - Dehydrate idle sessions.
   - Use a byte-budgeted LRU for the Markdown and image caches.
   - Store messages as `Arc<Message>`.
8. **[#18] Build.**
   - Fat LTO plus PGO driven by the benchmark scenarios.
   - Measure mimalloc in the daemon.
   - Put `ts-rs` behind a feature flag.
   - Add a `profiling` profile with line tables.
   - Add `tracing` and optional tracy spans.

### 5.3 The fluidity project (GPUI fork work, runs alongside Phases 2–3)

This is the differentiator: streaming text that grows smoothly at 120 Hz without burning CPU.

1. **Rows measured once in `list()`.**
   - Each transcript row carries a `content_version`. The cached view replays its layout and paint when `(version, width)` is unchanged.
   - Result: a commit rebuilds **one** row, not all 10–30 visible ones.
2. **Commits at display rate.**
   - Tie them to the frame callback (`on_next_frame`) instead of the 120 ms `STREAM_FRAME_INTERVAL`.
   - Reveal text on the display link for the tail row only.
3. **A clock aligned to vsync.**
   - Drive the pulse clock from frame callbacks instead of a free-running 16.67 ms timer.
   - Use an even stride at 120 Hz to avoid judder.
4. **Per-line code shaping.**
   - Split code blocks into per-line text so GPUI's line cache hits for unchanged lines.
   - Re-lex incrementally from the last complete line (the `Carry` state already exists).
5. **Panel slides that don't reflow.** Animate a clip or offset and keep the target width during the slide, so every visible paragraph doesn't re-wrap on each frame.
6. **Split the 580-line `Waku` root entity** into SessionStore, TranscriptView, Sidebar, RightPanel and Composer entities, as Zed does. The caret blink then repaints only the composer.
7. **Policy: no continuous repaint.** Allow only leased, visible, frame-aligned animation. Panels open instantly by default. Spinners rest at 0 Hz when offscreen. This matches T3's own `AGENTS.md` rule and should be enforced by a counter-budget test.

### 5.4 Memory model after Phase 2

- The daemon owns state. A client holds only the bounded window: 10 user turns, 75 items, about 1 MiB. Older pages are fetched on scroll.
- Tool outputs and diffs are fetched on demand and are never in the transcript model.
- Per-view caches are dropped with their view.

This is the main reason the fork can stay **flat** where Electron grows.

### 5.5 Harness (build in Phase 0, gate in CI from Phase 1)

- **`waku-bench-daemon`.** Replays scripted event timelines: prose, burst, fast reasoning, a code wall, a tool storm and images. Fake provider CLIs run the same scripts against T3.
- **`perf-counters` feature.** Counts renders per pane per commit and allocated bytes per commit. CI asserts budgets, for example 0 sidebar renders while streaming and 0 UI-thread time in save.
- **Microbenchmarks** (divan/criterion): the parser, flattening, the highlighter, the diff parser, the terminal snapshot, wire encode/decode, and saving. CI fails on a > 10% regression.
- **Nightly cross-app runs** on Apple Silicon with ProMotion: `powermetrics`, the Instruments Animation Hitches template, `footprint`, and the App Launch template. Publish the trend dashboard.

---

## 6. Roadmap

Work is executed by AI agents.

- Each phase has an epic issue on GitHub listing its issues, one long-lived branch (`phase/<n>-<name>`), and **one pull request** into `main` that closes all of the phase's issues.
- Agents work issue by issue on the phase branch. Parallel agents use local sub-branches that merge into the phase branch and never get their own PR.
- Phases are ordered by dependency. A phase that needs unmerged work from an earlier phase merges that phase branch in.

The details are in `AGENTS.md` under "Fork workflow".

| Phase | Epic | Content | Exit criteria |
|---|---|---|---|
| **0. Fork hygiene and baseline** | [#163](https://github.com/countradooku/waku/issues/163) | Rename, signing, update feeds, analytics off by default. CI with clippy, fmt, web and mobile tests. `tracing`, `perf-counters`, `waku-bench-daemon`. **Baseline numbers for Waku and T3.** Decide D2–D5 (D1 is settled: our own protocol). | A reproducible perf dashboard. The decisions are written down. |
| **1. Perf quick wins** | [#164](https://github.com/countradooku/waku/issues/164) | §5.2 items 1–8 | Idle < 5 wakeups/s, 0 save hitches, scroll p99 < 8.3 ms, RPC p50 < 1 ms |
| **2. Orchestrator core** | [#165](https://github.com/countradooku/waku/issues/165) | Event store, receipts, outbox, effect workers, v2 projection graph. `DriverV2` trait, with all 11 drivers ported to normalized streams. Daemon-owned transcripts; bounded snapshot and cursor streams; command-output hub. Typed protocol v8 with binary local frames, ping and TLS. Background service. Restart recovery. PTY host on every platform. Worktree cleanup. Remote desktop wiring. Replay-fixture tests. | Kill the desktop mid-turn: the turn finishes, persists, and re-renders when the client reconnects. Two clients stay in sync. |
| **3. Session-model parity** | [#166](https://github.com/countradooku/waku/issues/166) | Queue, steer/restart policy, plan mode, approvals, questions, secrets, subagent child threads, delegated tasks, lazy forks, merge-back, provider switch handoffs, `/goal`, `/compact`, limit auto-resume, native images, provider instances, multi-model fan-out | Every §4.1 row ✅ for Codex and Claude; capability-driven fallbacks verified for the rest |
| **4. MCP and automation** | [#167](https://github.com/countradooku/waku/issues/167) | `/mcp` server with scoped tokens and per-provider injection; toolkits in order: orchestrator → thread → queue → project → worktree → attachments → environment; scheduled tasks and webhooks; outside-agent OAuth; workflows on QuickJS | An agent can `delegate_task`, `t3_thread_send` and `schedule_task` in every provider that supports MCP |
| **5. UI parity** (can start once the protocol v8 RFC is merged) | [#168](https://github.com/countradooku/waku/issues/168) | §4.4–4.7: rich composer with chips (start early, it is the hardest piece), trigger menus, stash, history, citations, v2 timeline rows, minimap, paging, server-side find, sidebar sections with drag and undo, keybindings engine, 14 settings pages with scoping, themes plus VS Code import, terminal drawer, diff split view and comments, file previews, go-to-file and search, usage, wizard, diagnostics, HTML renders and MCP Apps webview | A checklist walk of [t3-clients.md §1–2](audit/t3-clients.md) with all S/M/L rows closed |
| **6. Source control** | [#169](https://github.com/countradooku/waku/issues/169) | Pull, PR creation, publish, setup and project scripts; GitHub PR inbox, detail, review and merge; stacks; review guides; PR watch and linking; then GitLab, Bitbucket, Azure DevOps and Forgejo | Full PR lifecycle on GitHub from inside the app |
| **7. Remote** | [#170](https://github.com/countradooku/waku/issues/170) | Pairing and scopes, connection catalog with routes, Tailscale, SSH environments, multi-environment client, load balancing, provider sign-in flows for remote hosts, update channels | Phone or second machine pairs over LAN, Tailscale or SSH with scoped access |
| **8. Mobile, web and relay** (starts once protocol v8 lands) | [#171](https://github.com/countradooku/waku/issues/171) | Grow Waku `apps/web` and `apps/mobile` on the generated protocol client (D2); minimal relay plus push (D3) | Mobile at the parity level D2 chose |
| **9. Long tail** | [#172](https://github.com/countradooku/waku/issues/172) | Agent browser automation (CDP), headless browser, devices panel, SnapShots, ACP Registry, Antigravity, Muse, provider install/update management, Mermaid, voice, default browser, CLI activation | Per item |

The fluidity track (§5.3, epic [#173](https://github.com/countradooku/waku/issues/173)) runs alongside Phases 2–3; it only depends on the perf counters from Phase 0.

---

## 7. Risks

| Risk | Mitigation |
|---|---|
| T3 ships fast (commits daily) and the parity target keeps moving | Pin a T3 snapshot per phase. Run the scheduled drift workflow (`.github/workflows/t3-drift.yml`) over `docs/user`, `packages/contracts` and `orchestrationV2.ts`, and file new rows into the matrix. |
| Upstream Waku also moves fast, and the conflict hotspots (`runtime.rs`, `composer.rs`, `right_panel.rs`, drivers, `model.rs`) are exactly what Phase 2 rewrites | Treat upstream as a source to cherry-pick after Phase 2, not to merge. Keep driver protocol fixes flowing through small adapter shims. |
| The rich-text composer in GPUI | Start in Phase 2 and prototype on Zed's `editor` crate patterns: rope plus inline atoms. Keep a plain-mode fallback, as T3 does. |
| GPUI fork maintenance | Upstream the measured-once-row changes to Zed if possible. Otherwise, rebase every quarter. |
| GPL-3.0 constrains commercial plans | Decide early; the license cannot change without upstream relicensing. |
| Provider protocols are proprietary or unstable (Muse, Cursor SDK, Antigravity) | Gate on capability flags. Use replay fixtures to detect drift. |
| Costs of the relay and push infrastructure | Phase it last and keep it minimal (D3-b). |

---

## 8. Where to start

These Phase 0 issues have no dependencies and can run in parallel, one agent session each:

- [#1](https://github.com/countradooku/waku/issues/1) rebrand · [#3](https://github.com/countradooku/waku/issues/3) analytics off · [#4](https://github.com/countradooku/waku/issues/4) CI gates · [#5](https://github.com/countradooku/waku/issues/5) tracing · [#6](https://github.com/countradooku/waku/issues/6) perf counters · [#14](https://github.com/countradooku/waku/issues/14) decisions D2–D5
- [#11](https://github.com/countradooku/waku/issues/11) [#12](https://github.com/countradooku/waku/issues/12) [#13](https://github.com/countradooku/waku/issues/13) the three RFCs (drafts in [`rfcs/`](rfcs/))

Then, as their dependencies close: [#7](https://github.com/countradooku/waku/issues/7) bench daemon → [#8](https://github.com/countradooku/waku/issues/8) fake CLIs → [#9](https://github.com/countradooku/waku/issues/9) baseline, and the Phase 1 quick wins starting with [#17](https://github.com/countradooku/waku/issues/17) scroll redraws and [#16](https://github.com/countradooku/waku/issues/16) saves off the UI thread.

Each issue lists its dependencies, acceptance criteria and references, so an agent can pick up any open issue whose dependencies are done. All Phase 0 work goes on the `phase/0-baseline` branch and lands in one PR.
