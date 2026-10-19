# Waku performance & rendering architecture audit

Scope: Waku v0.1.20 (Rust, GPUI from the `egoist/zed` `waku-webview` fork), the desktop app (`src/`), `waku-client`, `waku-protocol`, `waku-core` and `waku-daemon`.
Goal: decide what the fork must change to beat T3 Code (Electron/React) on CPU, memory and fluidity at 120–240 Hz.
Method: static reading only. Nothing was built or profiled. Every number below is either quoted from the repo or labelled **estimate**.
Paths are relative to the repo root.

---

## 0. Executive summary

Waku already has a solid performance culture. `docs/performance.md` records a real investigation that took streaming CPU from 40–60% down to about 10%. The app virtualizes the transcript, parses Markdown incrementally, caches flattened text, and runs animations on a self-parking pulse clock. The remaining ceilings are **structural**, not micro-level:

1. **Persistence runs on the UI thread, and it is a full snapshot.** While streaming, `Waku::save()` runs at least once a second (and from 43 other call sites). Each run does three things on the UI thread:
   - deep-clones every dirty `AgentSession`, including all messages and activities;
   - writes two pretty-printed JSON files with `fsync`;
   - ships the whole session as JSON to the daemon, which re-serializes it.

   At 120 Hz the frame budget is 8.3 ms. The fsyncs alone can eat it (`F_FULLFSYNC` on macOS), so you get a dropped frame about once a second while streaming. This is the single biggest jank source.
2. **The transport polls instead of blocking.** Both ends of the WebSocket use 25 ms read-timeout loops, and writes only happen between reads. That adds up to ~50 ms (about 25 ms on average) to every request/response, including terminal input, completions and hydration. It also causes ~120 idle wakeups per second across the two processes. Events are double-encoded (`DriverEvent → serde_json::Value → String`) and deep-cloned once per subscriber.
3. **Scrolling invalidates the whole window.** The transcript scroll handlers call `window.refresh()` on every scroll event (`src/app.rs:2281,2292`), and so do the overlay scrollbars. That bypasses every cached pane, so each 120 Hz trackpad tick also rebuilds the sidebar and the right panel (terminal grid snapshot, diff rows, a whole-file editor layout).
4. **One root entity holds almost everything.** `Waku` is a ~580-line struct (`src/app.rs:1043–1619`) with 33 `impl Waku` blocks. Any root `cx.notify()` fans out to all three cached panes. Every stream commit takes this path, so the sidebar and right panel rebuild at commit rate.
5. **Streaming smoothness is capped by design.** Text commits run at about 8.3 Hz (`STREAM_FRAME_INTERVAL = 120ms`, `src/app.rs:119`), and the dissolve runs at about 30 Hz on a timer that is not aligned with vsync. The reason is that every GPUI `list()` frame rebuilds every visible row. To be decisively more fluid than React, commit cost must become O(changed row), not O(visible rows). That requires fork work: measure-once cached list rows.
6. **Terminal and editor do whole-buffer work.**
   - The terminal polls every 24 ms per instance, even when hidden. Each render does one `String` allocation per cell and rebuilds the whole grid with no damage tracking.
   - The right-panel file editor re-tokenizes and lays out the entire file on every keystroke and frame.

If you fix items 1–4 (days of work, low risk), I expect (**estimate**) idle wakeups to drop by more than 10×, the periodic streaming hitch to go away, and scroll frame time to fall by roughly the cost of the sidebar plus right panel. Items 5–6 are the "decisive win" projects (weeks of work).

---

## 1. Current architecture by area

### 1.1 Process and threading model
- **Startup.** The desktop app spawns `waku-daemon` and blocks until the daemon prints its ready line. Only then does it initialize GPUI (`src/lib.rs:194–197`, `src/daemon.rs`).
  - The daemon prints ready *before* opening SQLite (`crates/waku-daemon/src/main.rs`), which is good. The client then connects with a WebSocket handshake.
  - The daemon monitors its parent PID every 500 ms. The supervisor polls for rebuilds and reconnects every 500 ms (`crates/waku-client/src/process.rs:24,512`).
- **Threads (desktop).**
  - GPUI main thread plus its background executor.
  - One `waku-daemon-client` socket thread (`crates/waku-client/src/client.rs:255`).
  - One forwarding thread per attached session runtime (`src/driver/mod.rs:98`).
  - One task-state-sync thread (`src/app/runtime.rs:916`).
  - One Alacritty PTY event loop per terminal.
- **Threads (daemon).**
  - One accept loop, polling every 25 ms (`crates/waku-core/src/server.rs:27,595`).
  - One thread per connection (`server.rs:582`).
  - **One new OS thread per non-runtime request** (`server.rs:461`), and that includes the 1 Hz `SaveTaskState`.
  - One mailbox thread per runtime (`server.rs:476–520`), plus provider CLI subprocesses.
- **Ownership.** The desktop owns the authoritative in-memory session model. The daemon persists whatever the desktop sends in `SaveTaskState` (`crates/waku-client/src/persistence.rs:1017–1048` → `crates/waku-core/src/persistence.rs:1320–1450`). The daemon sees every event but does **not** project them itself.

### 1.2 Event delivery (daemon → UI)
- **Wire format.** JSON text WebSocket frames (tungstenite). `ServerMessage::Event(SequencedEvent{session_id, runtime_id, epoch, sequence, event: WireDriverEvent{kind: String, payload: serde_json::Value}})` (`crates/waku-protocol/src/protocol.rs:302–329`).
  - The daemon converts `DriverEvent → Value` (`crates/waku-core/src/daemon.rs:1837+`) and then `Value → String`.
  - The client parses `String → ServerMessage` (`client.rs:276`) and then runs `serde_json::from_value` per kind (`crates/waku-protocol/src/driver_wire.rs:132–152`).
  - Each text delta carries about 150 bytes of envelope: three UUIDs, the sequence and the kind.
- **Fan-out.** The daemon's `Hub::emit` (`server.rs:247–272`) clones each event into a replay journal capped at 2048 events per runtime by count, not bytes (`server.rs:35,264–269`). `broadcast` then does a `message.clone()` (deep `Value` clone) for each subscriber (`server.rs:319–333`).
  - The queue per subscriber is bounded at 16384 messages; overflowing it kicks the client, which reconnects with a replay cursor.
- **Daemon connection loop** (`server.rs:686–735`).
  - It drains the outgoing queue, then calls `socket.read()` with a 25 ms timeout (`server.rs:677`). Queued events and responses wait out the remainder of that timeout.
  - Every outgoing message is serialized with a fresh `serde_json::to_string` (`server.rs:1068`).
- **Client loop** (`client.rs:255–352`). It mirrors the daemon loop, with the same 25 ms read timeout (`client.rs:22,104`); outgoing requests wait for it. Dedup works by `(epoch, sequence)` per runtime (`client.rs:291–307`). Events that arrive before subscription are buffered up to 4096 (`client.rs:24`).
- **Forwarding thread** (`src/driver/mod.rs:101–141`).
  - It converts each event and sends **two** messages per event over an unbounded crossbeam channel: the event and then `RuntimeEventCursorAdvanced`.
  - It wakes the UI through a `bounded(1)` smol channel (`src/app.rs:2212`, `crates/waku-client/src/driver.rs:17–32`).
- **UI pump** (`src/app.rs:2647–2690`, `src/app/runtime.rs:3526–3560`).
  - It drains everything on a wake.
  - If text or reasoning deltas are pending, it sleeps a full `STREAM_FRAME_INTERVAL` (120 ms) without racing the wake channel. It then coalesces adjacent deltas (`pop_stream_batch`, `src/app/streaming.rs:974–1005`), applies them, remeasures the last 3 rows, and calls **one root `cx.notify()`** (`runtime.rs:3529–3538`).
  - The batching is correct and well documented.
- **Replay.** On reconnect the client sends `ReplayCursor`s. The daemon replays its journal under the hub lock (`server.rs:274–310`).

### 1.3 Entity/notify structure
- **Panes.** `Waku` is the root view. The sidebar, transcript and right panel are `WakuPane` islands embedded with the fork's `Entity::cached` (`src/app.rs:663–709`, `src/app/render.rs:340,364,401`).
  - Each pane observes the root, so **any root notify re-renders all three panes**. The only exception is a panel slide.
- **Root render** (`src/app/render.rs:227+`). It runs on any dirty descendant. It rebuilds the header, composer, footer, toasts and overlays, settles panel slides, and syncs the browser webviews.
- **Pulse clock** (`src/ui/motion.rs:20–31,76–130`). A background timer fires every 16.67 ms and notifies leased views at stride 1/2/4 (60/30/15 Hz). It is **not vsync-aligned**, and it parks 300 ms after the last lease.
- **Remaining whole-window invalidations:**
  - transcript scroll handlers (`src/app.rs:2272–2293`);
  - overlay scrollbar hover, click and drag (`src/ui/scrollbar.rs:363,393,412,423`);
  - menus (`src/ui/menu.rs`, many);
  - theme changes (`src/theme.rs:231`).
- **Continuous `request_animation_frame` users:**
  - panel slide, 200 ms (`render.rs:235`);
  - FPS counter (`render.rs:222`);
  - browser view while a page is loading (`src/browser.rs:2486`);
  - caret follow (`src/input.rs:2341`, one-shot).
- **Periodic timers:**
  - background-work tick, 1 Hz UI update (`app.rs:126`, `background_work.rs:613`);
  - plan usage, 30 s;
  - idle reap, 5 min;
  - caret blink, 500 ms, on both the composer (`src/input.rs:72,272–290`) and the terminal;
  - terminal poll, 24 ms per instance (`src/terminal.rs:665–676`).

### 1.4 Transcript rendering
- **Virtualization.** The transcript uses GPUI `list()` with variable heights. There are two `ListState`s, bottom-aligned and anchored (`src/app.rs:2259–2260`, `src/app/transcript_view.rs:353`).
  - Per commit, only the last 3 rows are remeasured (`src/app/transcript.rs:372–377`).
  - The row kinds are cached under a per-frame O(messages) fingerprint (`transcript.rs:826+`).
- **Row builder** (`transcript_view.rs:1210–1425`). Per visible row, per frame, it:
  - clones the whole `Message` (`.cloned()` at `transcript_view.rs:1251`);
  - builds menu-handle ids with `format!`;
  - calls `MarkdownView::set_text`, which does an O(n) string compare against the parser's copy (`src/md/render.rs:514–549`);
  - renders the Markdown.
- **Markdown.**
  - Custom parsing on `pulldown-cmark` with `IncrementalParser` (`src/md/parser.rs:728–818`), which re-parses from the last stable block on append. A link reference definition anywhere forces a full reparse on every append (`parser.rs:789–806`).
  - Flattened `TextRun` vectors are cached per element ordinal and pruned back to the volatile final block (`render.rs:449–591`).
  - A "mended" tail closes hanging markers while streaming.
  - The live reasoning peek renders a 6–18 KB byte window (`app.rs:177–185`).
- **Syntax highlighting.** A hand-written single-pass line lexer covers 17 language families (`src/md/highlight.rs:1–15,103–152`); anything else is plain.
  - A code block is tokenized whole on a flatten miss (`render.rs:1799`) and becomes **one** `StyledText` for the whole block (`render.rs:1638–1760`).
  - The block is cached once settled. While it is the streaming tail block, it is re-tokenized and re-flattened every commit, which is O(block) per commit.
- **Text shaping.** This relies on GPUI's line-layout cache, which is keyed by text, runs and wrap width, and swapped each frame. There is no app-level shaping cache. A panel slide changes the transcript width every frame for 200 ms, so every visible paragraph misses the wrap cache during the slide.
- **Math.** RaTeX runs on two workers, with bounded caches (256 entries / 32 MiB) and explicit atlas eviction (`docs/performance.md:124–150`, `src/md/math.rs`). This is good.
- **Images.**
  - Blob and attachment images are fetched off-thread over the daemon as base64-in-JSON (`crates/waku-protocol/src/protocol.rs:207–227`, `crates/waku-client/src/persistence.rs:1241–1265`). They are cached as `Arc<gpui::Image>` in an **unbounded** `remote_images` map (`src/app/image_preview.rs:47–100`). GPUI decodes and caches the RGBA image, also with no eviction.
  - Legacy `data:` URLs are base64-decoded **on every render** (`transcript_view.rs:2860–2885`, `md/render.rs:1626–1636`).
- **Retention.**
  - Every session the user ever opened stays fully hydrated in `state.sessions`; nothing dehydrates it.
  - The Markdown view cache is **cleared wholesale** once it exceeds 512 KB of source (`src/app/sessions.rs:845–854`, `app.rs:154`). There is no LRU.
  - The activity Markdown cache is cleared on every session switch (`sessions.rs:859–864`).

### 1.5 Terminal
- **Parser and model.** `alacritty_terminal` 0.26 runs in-process for local terminals (`src/terminal.rs:182–252`), with 10k lines of scrollback (`terminal.rs:59`). There is also a daemon-owned PTY path for remote clients (`crates/waku-core/src/terminal.rs`).
- **Change detection.** A 24 ms UI-thread poll checks an `AtomicBool` dirty flag (`terminal.rs:660–676,723–747`). The poll runs whenever the entity is alive, including when the panel is hidden.
- **Render.**
  - `snapshot()` locks the `Term` and walks `display_iter`. It allocates **one `String` per cell** (`cell.c.to_string()`, `terminal.rs:351`), builds `TerminalCell`s, and then merges them into per-row runs (`terminal.rs:309–425`).
  - Each row becomes a `div` + `StyledText`, with run backgrounds standing in for cell backgrounds (`terminal.rs:1141–1180`).
  - There is no damage tracking: the whole grid is rebuilt on any change, on every blink tick (500 ms), and on every whole-window refresh.
  - Throughput is capped at about 41 visible updates per second.

### 1.6 Review / diff view
- **Pipeline.** The daemon runs git. The client parses and tokenizes off the UI thread (`src/review_diff.rs:1–21,351–566,803`), with a hard cap of 50,000 rendered lines (`review_diff.rs:21`). Context collapses with expandable gaps.
- **Render.** The view uses `list()` with variable heights because lines soft-wrap (`whitespace_normal`, `src/app/right_panel.rs:440`; list at `right_panel.rs:3705`).
  - Per visible row, per frame: `line.content.clone()`, `MarkdownPalette::from_theme`, run building, and three `format!` element ids (`right_panel.rs:362–480`).
  - A sticky file header is looked up through cached header indices.
- **Activity diffs** in the transcript reuse the same row renderer.

### 1.7 Persistence / SQLite
- **Daemon.** `rusqlite` with the bundled build, WAL and `synchronous=NORMAL` (`crates/waku-core/src/persistence.rs:1000–1005`).
  - `save_task_state` runs one transaction. It upserts dirty sessions and writes the session detail blob, which is `serde_json::to_value(session)`, then removes `messages`, then `to_string` (`persistence.rs:1596–1602`).
  - Messages are skipped when their FNV fingerprint is unchanged (`persistence.rs:1635–1680`), but every message body is still hashed on every save.
  - All 16 statements go through `.execute(...)`. **No `prepare_cached`.**
  - The session search scan uses a separate read-only connection (`persistence.rs:766`).
- **Client save** (`src/app/runtime.rs:1603–1620` → `crates/waku-client/src/persistence.rs:1017–1048,1110–1127`). All of this runs on the UI thread:
  - `write_json_atomically` twice (app settings and app state): `to_vec_pretty`, then `sync_all()`, then rename, then chmod, **with no fingerprint skip**;
  - a clone of every dirty `AgentSession` plus all projects into `Command::SaveTaskState`, which goes to the socket thread for serialization.
- **Cadence.** The client saves every `STREAM_SAVE_INTERVAL` (1 s) while streaming (`app.rs:128`, `runtime.rs:3709–3714`), on any force-save event, and from 43 call sites in total.
- **Daemon response cache.** It holds up to 2048 responses / 64 MB for retries, and **serializes each response once just to measure it** (`server.rs:398–425`).

### 1.8 Memory
- **Desktop:**
  - all hydrated sessions;
  - parsed Markdown (the code comment cites about 17× the source size, `app.rs:146–151`);
  - the flatten caches;
  - unbounded image maps;
  - GPUI asset caches;
  - terminal grids (10k × columns per terminal).
- **Daemon:**
  - replay journals, 2048 events per runtime, unbounded by bytes, so tool outputs and diffs can be large;
  - the response cache (≤ 64 MB);
  - subscriber queues (≤ 16384 messages each);
  - a thread per request.
- **Providers.** Provider CLIs (often Node) usually dominate total RSS. They are reaped after 30 minutes idle (`app.rs:123`). T3 Code spawns the same CLIs, so benchmarks must report app+daemon RSS separately from provider subprocesses.

### 1.9 Build / binary
- **Release profile** (`Cargo.toml:204–207`): `lto = "thin"`, `codegen-units = 1`, `strip = true`, with the default `panic = "unwind"` and opt-level 3. No PGO, no `target-cpu`, and no allocator override (system malloc).
- **Size contributors:**
  - `rquickjs` (the QuickJS engine for the JS REPL);
  - `wry` / WebKit bindings;
  - `ratex-svg` with `embed-fonts`;
  - about 4.2 MB of `include_bytes!` fonts and icons (`src/assets.rs:13,212–220`);
  - `rust-i18n`;
  - `ts-rs` as an unconditional dependency of `waku-protocol` (only `src/bin/export_types.rs` needs it);
  - a `tokio` runtime alongside `smol`.
- **Dev profile.** opt-level 1 for the app and 2 for dependencies (`Cargo.toml:211–215`), which is good. The `gpui/test-support` feature is restricted to dev-dependencies, with a comment explaining why (`Cargo.toml:65–75`).
- **Profiling and benchmarks.** There is no `tracing` and no benchmark crate. The only benchmark is the ignored test `md::math::tests::benchmark_native_math_cache` (`src/md/math.rs:448`). `scripts/seed-mock-sessions.ts` seeds oversized sessions; that is useful for the harness.

---

## 2. Hotspots ranked by expected impact

Impact is user-visible (CPU, memory, frame pacing), weighted by how often the problem hits. Effort: S < 2 days, M < 1 week, L = multi-week / fork work.

| # | Hotspot | Where | Symptom | Impact | Effort |
|---|---|---|---|---|---|
| 1 | Synchronous snapshot persistence on the UI thread: 2× `fsync`'d pretty JSON, deep clone of dirty sessions, every 1 s while streaming and on 43 call sites | `src/app/runtime.rs:1603`, `crates/waku-client/src/persistence.rs:1017–1048,1110–1127` | Periodic 1–5 frame hitches at 120 Hz; O(session) CPU/s; daemon re-serializes whole sessions | **High** | S (move off-thread + fingerprint), L (daemon-side projection) |
| 2 | `window.refresh()` on every transcript scroll event and scrollbar move | `src/app.rs:2272–2293`, `src/ui/scrollbar.rs:363–423` | Every scroll frame rebuilds sidebar + right panel (terminal snapshot, diff, editor) | **High** (scroll fluidity is the most-felt metric) | S |
| 3 | 25 ms polling sockets on both ends; writes wait for the read timeout | `crates/waku-client/src/client.rs:22,104,255+`; `crates/waku-core/src/server.rs:27–28,595,677,686+` | +0–50 ms per RPC round trip; event latency of up to 25 ms; ~120 wakeups/s idle | **High** for latency and idle power | M |
| 4 | Root-entity fan-out: every stream commit does a root `cx.notify()` that re-renders all three panes, header and composer | `src/app/runtime.rs:3529–3538`, `src/app.rs:663–699` | Streaming CPU scales with the whole window, not with the transcript | **High** | M (targeted notify) → L (entity split) |
| 5 | `list()` rebuilds every visible row on every transcript frame; commits are throttled to 8.3 Hz and the veil to 30 Hz to compensate | `docs/performance.md:9–14,177–186`, `src/app.rs:119` | Visible "chunky" streaming on ProMotion; the design is CPU-bound by visible rows | **High** for perceived fluidity | L (fork) |
| 6 | Terminal: 24 ms poll per instance (even hidden), a `String` per cell, full grid rebuild, no damage | `src/terminal.rs:351,660–676,1141–1180` | Idle wakeups; high CPU under `cat bigfile`; ~41 fps cap | Med–High | M |
| 7 | Event encoding: `DriverEvent→Value→String` and back; a deep `Value` clone per subscriber and per journal entry; 2 channel sends per event; an extra serialize per cached response | `protocol.rs:304`, `daemon.rs:1837`, `server.rs:264,326,412`, `src/driver/mod.rs:128–131` | Allocation churn on both processes during bursts; daemon RSS from the journal | Med | S–M |
| 8 | The streaming tail code block is re-tokenized and re-flattened in full each commit, as one giant `StyledText` | `src/md/render.rs:1638–1650,1799` | O(n²) over a long streamed code block; long blocks re-wrap whole | Med | M |
| 9 | Unbounded memory: hydrated sessions never evicted, `remote_images` unbounded, replay journal sized by count not bytes, Markdown cache flushed wholesale | `src/app/sessions.rs:845–864`, `src/app/image_preview.rs:59–79`, `server.rs:35,264` | RSS grows with sessions visited; thrash when switching between large sessions | Med (long-running sessions are T3's pain point too) | S–M |
| 10 | Panel slide animates *width*, so the transcript re-wraps every visible paragraph at display rate for 200 ms | `src/app/render.rs:227–236,330–420` | Dropped frames during sidebar/right-panel toggle on heavy transcripts | Med | S–M |
| 11 | File editor: whole-file re-tokenize per edit and whole-text layout per frame | `src/input.rs:925–945,2558–2640` | Typing lag in large files (T3 parity likely needs a real editor) | Med (parity-dependent) | M–L |
| 12 | Per-row, per-frame allocations: `Message` clone, `format!` ids, diff `content.clone()`, palette rebuild, data-URL base64 decode | `transcript_view.rs:1251,1268–1276,2860–2885`; `right_panel.rs:390,464–480` | Constant per-frame churn, worst when frames are frequent (scroll) | Low–Med | S |
| 13 | Pulse clock on a 16.67 ms background timer, not vsync-aligned; 30 Hz veil, 60 Hz spinners | `src/ui/motion.rs:20–23,92–130` | Uneven frame pacing (judder) on 120 Hz | Low–Med (perceptual) | S–M |
| 14 | Browser: `request_animation_frame` every frame while `loading` | `src/browser.rs:2483–2487` | 120 Hz redraw of the whole window during page loads | Low–Med | S |
| 15 | Thread spawn per daemon request; no `prepare_cached`; FNV over every message body on every save | `server.rs:461`, `crates/waku-core/src/persistence.rs` (16 `.execute`), `1643–1648` | Daemon CPU per save scales with transcript size | Low–Med | S |
| 16 | Serial cold start: spawn daemon → wait for ready → handshake → GPUI init → window → task-state load | `src/lib.rs:194–200`, `crates/waku-client/src/process.rs` | Avoidable tens of ms of cold start (**estimate**) | Low–Med | S |
| 17 | Caret blink at 2 Hz re-renders the root (composer is in the root) | `src/input.rs:272–290`, `src/app/render.rs:373` | ~2 root frames/s while idle and focused | Low | S |
| 18 | Build: thin LTO, no PGO, no allocator choice, symbols stripped (no symbolized production profiles), `ts-rs` always compiled | `Cargo.toml:204–207`, `crates/waku-protocol/Cargo.toml` | A few % CPU and size; slower profiling loop | Low | S |

---

## 3. Recommended changes (what, where, expected gain)

Ordered by ROI. "Gain" is an **estimate** to be confirmed with the harness in §4.

### 3.1 Get persistence off the UI thread, then make it incremental (#1)
1. **Immediate (S).** Split `Waku::save()` (`src/app/runtime.rs:1603`) into a cheap snapshot step and a background step.
   - Snapshot step on the UI thread: take `dirty_sessions` and produce **`Arc`-shared** session snapshots (see step 3), so this is not a deep clone.
   - Background step: hand the snapshot to the existing socket thread or a background-executor task.
   - Move `write_app_settings` / `write_app_state` (`crates/waku-client/src/persistence.rs:1018–1019`) to a background writer that:
     - fingerprint-skips unchanged content, as the daemon path already does at `crates/waku-core/src/persistence.rs:1324–1338`;
     - uses compact `to_vec` instead of `to_vec_pretty`;
     - **debounces** writes (for example a 500 ms trailing edge).
   - Keep `sync_all` only for shutdown, or use plain `fsync` (`libc::fsync`, not `F_FULLFSYNC`) for settings files. A torn settings file is already handled by the tmp+rename.
   - Gain: removes the 1 Hz streaming hitch (likely 5–40 ms per second on the UI thread) and adds latency headroom to session switches, settings toggles and so on.
2. **Medium (M).** Stop sending whole sessions. Send **deltas**: `AppendMessageText{message_id, from_len, text}`, `UpsertActivity{...}`, `SetTurnStatus{...}`. Better still, have the daemon persist from the event stream it already emits (see 3).
3. **Structural (L).** Make the daemon the source of truth and project events into SQLite (event-sourcing, which is also T3 Code's model).
   - The daemon already sequences every event (`server.rs:247`). Apply each event to a daemon-side session model, then batch-write to SQLite every N ms on a dedicated writer thread with `prepare_cached` statements.
   - The desktop then only persists UI-local state (drafts, layout).
   - This removes `SaveTaskState` traffic entirely. Reload, crash recovery and multi-client sync become consistent.
   - Inside the client, store `AgentSession.messages` as `Vec<Arc<Message>>` or a persistent vector (`im`/`rpds`), so a snapshot is O(changed).

### 3.2 Scope scroll invalidation (#2)
- In the transcript `set_scroll_handler` closures (`src/app.rs:2272–2293`), replace `window.refresh()` with `cx.notify(transcript_pane_entity_id)`.
  - GPUI's `ListState` scroll path already notifies the list's current view.
  - If the root itself needs to know (for example to show a scroll-to-bottom button), notify the root **without** the pane fan-out: add a `root_only_dirty` flag that `WakuPane::bind` checks, like `panels_sliding()`.
- In `src/ui/scrollbar.rs:363,393,412,423`, capture `window.current_view()` at paint time and `cx.notify(view)` instead of refreshing the window.
- Gain: per-scroll-frame cost goes from "whole window" to "transcript pane", plausibly a 2–4× reduction in scroll frame time with a terminal or diff open, and the main lever for p99 frame time at 120 Hz.

### 3.3 Replace polling sockets with blocking or evented I/O (#3)
- **Option A (M, smallest diff).** Split each WebSocket into a reader thread and a writer thread.
  - tungstenite needs a `try_clone`d `TcpStream`, or use `WebSocket::read`/`write` under a mutex with the reader blocking without a timeout.
  - The writer thread blocks on the crossbeam `outgoing` receiver and writes immediately.
  - Remove `READ_POLL_INTERVAL` (`client.rs:22`) and `SOCKET_POLL_INTERVAL` (`server.rs:28`).
  - Make the accept loop blocking, using a self-pipe or a connect-to-self to break it for shutdown (`server.rs:555–600`).
- **Option B (M–L).** Move the daemon to `tokio` (already in the dependency graph via the app) with `tokio-tungstenite`, one task per connection, and `bytes::Bytes` frames.
- In both options, measure kicks and wakeups.
- Also replace thread-per-request (`server.rs:461`) with a fixed worker pool (for example 4 threads plus a separate blocking pool for git/fs).
- **Local transport.** For local daemons, consider a Unix domain socket (or a named pipe on Windows) instead of TCP loopback. That avoids the TCP stack and allows peer-credential auth.
- Gain:
  - every RPC loses 0–50 ms (average about 25 ms) of artificial latency: session hydration, file search, completions, composer slash-command lookups (`src/app/composer.rs:1959`), blob reads;
  - idle wakeups drop from about 80/s (sockets) to near 0, which matters for battery and Activity Monitor's "Energy Impact", where Electron is weak.

### 3.4 Kill the root fan-out during streaming (#4)
- **Short term (M).** In `drain_event_pump` (`runtime.rs:3529`), stop calling root `cx.notify()` when only selected-session transcript content changed. Notify the transcript pane entity instead, and notify the sidebar pane only when sidebar-visible state changed (status, title, unread, recency bucket).
  - Introduce a `DirtySet { root, sidebar, transcript, right_panel }` that event handlers populate; the pump then notifies only those.
- **Long term (L).** Split `Waku` into real entities, the way Zed does:
  - `SessionStore` (model, emits events);
  - `TranscriptView` per session, owning its `ListState`, Markdown caches and row caches;
  - `Sidebar`;
  - `RightPanel` (with `TerminalView` and `ReviewView` as children);
  - `Composer`.

  Views `observe`/`subscribe` to the specific model entities they display.
  - This also removes the `RefCell` caches hanging off `Waku`.
  - It makes per-session caches drop naturally with the view; today `activity_markdown` is cleared on every switch (`sessions.rs:859`).
- Gain: streaming CPU stops scaling with the sidebar and right-panel content. Expect a 20–40% reduction in streaming frame cost when the right panel shows a diff or terminal (**estimate**).

### 3.5 Make streaming O(changed row) so it can run at display rate (#5)
This is the "decisively smoother than React" project.
1. **Fork work: measure-once cached rows in `list()`.** The docs already identify this (`docs/performance.md:177–186`).
   - Extend the fork's `ViewElement` caching so a cached row view remembers its measured height for a given `(content_version, width)` key, and replays prepaint/paint when unchanged.
   - Wrap each transcript row in a lightweight row entity (or a keyed cache on `ListState`) carrying a `content_version: u64` that the stream bumps only for the tail message.
   - Result: a commit rebuilds 1 row instead of all ~10–30 visible rows.
2. **Then raise the commit rate.**
   - Drop `STREAM_FRAME_INTERVAL` from 120 ms to one display frame or to 33 ms, gated on measured frame cost. Keep the "don't race the wake channel" rule, but tie it to `on_next_frame` instead of a 120 ms timer.
   - Drive the veil/typewriter reveal on the display link only for the tail row.
3. **Make the pulse clock vsync-aligned** (`src/ui/motion.rs:92–130`). Schedule ticks from frame callbacks (`window.on_next_frame`) and use frame timestamps for the phase, rather than a free-running 16.67 ms timer. At 120 Hz use stride 2, which gives even 60 Hz pacing instead of alternating 1- and 2-vsync gaps.
4. **Bound the tail block's work** (#8).
   - For a streaming fenced code block, keep per-line token caches. `tokenize_line` already carries `Carry` (`src/md/highlight.rs:809`), so on append re-lex only from the last complete line.
   - Split code blocks into **per-line `StyledText` children**, or a custom code element that shapes visible lines only, so GPUI's per-line layout cache hits for every unchanged line.
   - For huge settled code blocks, consider no-wrap with horizontal scroll, which also enables uniform line heights.
- Gain: smooth per-frame text growth at 60–120 Hz at roughly today's CPU (**estimate**), which is the visible differentiator versus Electron.

### 3.6 Terminal (#6)
- **Change detection.** Replace the 24 ms poll (`src/terminal.rs:660–676`) with event-driven wakeups.
  - `TerminalEventProxy::send_event(Event::Wakeup)` should `try_send` into a `smol::channel::bounded(1)` that the view's task awaits.
  - Coalesce bursts to one notify per display frame via `on_next_frame`.
  - Stop all work when the terminal is not visible: only set the dirty flag.
- **Damage tracking.** Use alacritty's `Term::damage()` (`TermDamage::Partial` line ranges, available in 0.26) to rebuild only damaged rows, and keep a per-row cache of the built `(SharedString, Vec<TextRun>)`.
- **Snapshot allocations.** Remove the per-cell `String` (`terminal.rs:351`): push `cell.c` straight into the row `String` and handle zero-width characters inline. Pre-size the vectors.
- **Rendering.** Draw backgrounds as merged quads in a custom element and shape per row with `shape_line` (Zed's `terminal_view` approach) instead of `div` + `StyledText` with run backgrounds. Cache `terminal_font()` variants instead of constructing a `Font` per run (`terminal.rs:1147`).
- Gain:
  - zero idle wakeups for hidden terminals;
  - `cat 100MB` throughput limited by the PTY reader, not the UI;
  - frame cost proportional to damaged rows.

### 3.7 Wire efficiency (#7)
- **Remove the double encoding.** Make `WireDriverEvent` a typed, tagged enum (`#[serde(tag="kind", content="payload")]`) instead of `{kind: String, payload: Value}`. That removes the `to_value`/`from_value` hops (`daemon.rs:1837+`, `driver_wire.rs:132–152`).
- **Serialize once, share.** In `Hub::emit`, serialize the `ServerMessage` **once** to `Arc<str>` / `Bytes`, then store that in the journal and push the same `Arc` to every subscriber queue. This removes the per-subscriber deep clones (`server.rs:264,326`) and per-connection re-serialization (`server.rs:1068`).
- **Bound the journal by bytes** (for example 8 MB per runtime) as well as by count (`server.rs:35`).
- **Fix `cache_response` sizing** (`server.rs:412`): reuse the serialized bytes the connection writes, or estimate the size without serializing.
- **Coalesce at the source.** In the daemon, merge `TextDelta`s per runtime within ~8–16 ms before emitting. Providers can emit hundreds of chunks per second, and the current 150 B envelope is paid per chunk.
- **One message per event.** Send only one message per event to the UI channel: fold `RuntimeEventCursorAdvanced` into an `(event, cursor)` tuple (`src/driver/mod.rs:128–131`).
- **Binary encoding (optional).** Once typed, consider MessagePack/bincode/postcard frames for the local native client, keeping JSON for browser clients. Blob bytes should travel as binary frames, or be read directly from disk when the daemon is local and the path is known (`read_remote_reference`, `crates/waku-client/src/persistence.rs:1241`), rather than as base64-in-JSON.

### 3.8 Memory bounds (#9)
- **Dehydrate sessions** that haven't been selected for N minutes and have no live runtime. Drop `messages` and `transcript_blocks` and set `detail_loaded=false`; the hydration path already exists (`src/app/sessions.rs:100–150`).
- **Make the Markdown cache a byte-budgeted LRU** keyed by message id (`sessions.rs:845–854`) instead of clearing it wholesale. Key the activity Markdown cache by `(session_id, block_id)` so it survives a switch.
- **Bound `remote_images` by decoded bytes**, and drop GPUI asset-cache entries on eviction (the math cache already shows how, `docs/performance.md:139–141`).
- **Decode legacy data-URL images once** and cache them by URL hash (`transcript_view.rs:2860–2885`).
- Gain: flat RSS for a long-running window regardless of how many sessions are visited.

### 3.9 Smaller fixes (S each)
- **Panel slide (#10).** Keep the transcript's layout width at the *target* width for the slide's duration and animate a clip/offset, or freeze the old width until the slide ends, so the wrap cache keeps hitting.
- **Row builder churn (#12).**
  - Borrow `&Message` instead of `.cloned()` (`transcript_view.rs:1251`). This needs a split borrow, or `Arc<Message>` in the model.
  - Cache `SharedString` ids per message id.
  - Cache `MarkdownPalette` per theme.
  - In the diff rows, keep a prebuilt `FlatText` per line in the snapshot (it is computed off-thread anyway) instead of rebuilding it each frame (`right_panel.rs:390,464`).
- **`MarkdownView::set_text`** (`md/render.rs:514`). Compare a `(len, version)` pair instead of the full string, with the message carrying a `content_version`.
- **Browser loading** (`browser.rs:2486`). Observe `estimatedProgress` via KVO, or poll at 10 Hz, instead of `request_animation_frame` every display frame.
- **Caret blink** (#17). Render the composer caret in a small cached child view so a blink re-renders only that view. Also consider stopping the blink after N seconds of inactivity, as macOS does.
- **Daemon SQLite** (#15).
  - Use `prepare_cached` for all statements.
  - Keep per-message fingerprints in memory, updated from append events, instead of hashing every body each save.
  - Run the writer on a dedicated thread with a bounded queue. Optionally set `PRAGMA mmap_size` and `cache_size`.
  - With WAL, `wal_autocheckpoint` defaults are fine.
- **Cold start** (#16).
  - Spawn the daemon on a background thread *in parallel* with GPUI init and window open. Render the shell (sidebar skeleton) immediately and fill it in when `LoadTaskState` arrives.
  - Load sidebar data from a small cached snapshot file so first paint has content.
  - Defer `rquickjs`, updater, analytics and provider probing until after first frame.
- **Build** (#18). Add these profiles:
  - `[profile.release] lto = "fat"` (measure the build time);
  - `[profile.profiling] inherits = "release", debug = "line-tables-only", strip = false`;
  - a PGO pipeline with `cargo pgo` driven by the benchmark scenarios (typically 5–15% CPU on UI-heavy Rust; **estimate**);
  - for macOS arm64-only artifacts, `-C target-cpu=apple-m1`.

  Then:
  - Evaluate `mimalloc` for the daemon, which allocates many short-lived JSON `Value`s, and for the app, measuring RSS and CPU both ways.
  - Gate `ts-rs` behind a `ts` feature in `waku-protocol`, enabled only for the `export_types` bin.
  - Use `cargo bloat --crates` to confirm what `rquickjs`, `ratex-svg` (embed-fonts) and `wry` cost in size.
- **Instrumentation.** Add `tracing` spans plus an optional `tracy`/`puffin` feature around these:
  - `drain_event_pump`;
  - `Waku::render`;
  - each pane render;
  - `transcript_row`;
  - Markdown `set_text`/flatten;
  - `save`;
  - daemon `emit`, `dispatch` and `save_task_state`.

  Ship the per-second counters `docs/performance.md:157–162` describes behind a `perf-counters` feature instead of wiring them temporarily each time.

---

## 4. Benchmark & regression harness

### 4.1 Principles
- Measure **both apps the same way, from outside**: process-tree sampling plus Instruments. Measure Waku **from inside** too (counters, frame timings), because counters are deterministic and CI-gateable. Sampling is noisy (see `docs/performance.md:152–175`).
- Separate **app+daemon** from **provider subprocesses** in every CPU and RSS number. Both apps spawn the same CLIs; report "shell overhead" and "total" side by side.
- Use **scripted providers**, so token rate and content are identical across runs and across apps.

### 4.2 Fixtures and drivers
1. **`waku-bench-daemon`**: a small binary speaking the existing protocol (`ClientMessage`/`ServerMessage`) that replays scripted `DriverEvent` timelines.
   - Waku already supports an external daemon via `WAKU_DAEMON_ADDRESS` + `WAKU_DAEMON_TOKEN` (`src/daemon.rs:5–27`).
   - Scripts:
     - `prose` (Markdown paragraphs and lists, 100 tok/s, 20k tokens);
     - `burst` (2,000 tok/s);
     - `reasoning-fast`;
     - `code-wall` (a single 3,000-line fenced block);
     - `tool-storm` (300 activities with 50 KB outputs and diffs);
     - `images` (20 PNG blobs).
   - Reuse the shapes from `scripts/seed-mock-sessions.ts`.
2. **T3 Code**: use the same scripts via its mock or replay provider, or a fake CLI on `PATH` (for example a fake `codex app-server` / `claude` emitting the same JSON-lines timeline). That fake CLI also works for Waku's real daemon path, giving an apples-to-apples end-to-end variant.
3. **Corpora:**
   - `seed-mock-sessions.ts --scale N` for long transcripts;
   - a git fixture repo with a generated 10,000-line diff (5 files × 2k lines, mixed languages);
   - a 100 MB text file and `vtebench` for terminal throughput.

### 4.3 Metrics and how to measure them

| Metric | Scenario | Waku measurement | Cross-app measurement | Target to "beat decisively" (proposal) |
|---|---|---|---|---|
| **Idle CPU & wakeups** | App open 5 min, 3 sessions, terminal panel open, composer focused, no streaming | `perf-counters`: frames/s, pane renders/s | `powermetrics --samplers tasks -i 1000` (CPU ms/s and **interrupt wakeups/s** per process tree); `top -l` | Fewer than 1 frame/s with an unfocused composer; total wakeups < 5/s (today ~120/s from polling **estimate**); CPU < 0.1% |
| **CPU during heavy streaming** | `prose`, `burst`, `reasoning-fast`, `code-wall`, each 60 s, right panel showing a diff | counters: commits/s, renders per pane per s, alloc bytes/s (counting global allocator in the bench build) | sum `proc_pid_rusage` user+sys over app+daemon (Waku) and main+renderer+GPU+utility+node server (T3), polled every 500 ms; report mean, p95, phase plateaus | ≤ 1/3 of T3's shell CPU at equal or higher visual update rate |
| **Frame time p99 at 120 Hz** | (a) trackpad scroll through a 2,000-message transcript with terminal+diff open; (b) streaming `prose`; (c) panel toggle | fork hook: timestamp `Window::draw` start/end and `presentDrawable` completion → histogram; count frames > 8.33 ms and missed vsyncs | Instruments **Animation Hitches** template (`xctrace record --template 'Animation Hitches'`) for both apps; hitch-time ratio (ms/s) | p99 < 8.3 ms, hitch ratio < 1 ms/s in (a) and (b); 0 hitches during streaming saves |
| **RSS with N long transcripts** | Open N = 1, 5, 20 sessions of 2k messages each (`--scale`), visit each, then idle 2 min | counters for cached Markdown bytes, image bytes, hydrated sessions | `footprint -p` / `proc_pid_rusage(RUSAGE_INFO_V4).ri_phys_footprint` summed over the process tree (excluding provider CLIs) | Flat after N ≈ 5 (bounded caches); < 1/3 of T3's footprint |
| **Cold start** | Cold (after `purge`) and warm launch with 200 sessions in the DB | log `t0` = `main()`, `t_window` = first frame, `t_content` = first frame with sidebar rows, `t_interactive` = composer accepts a key | `xctrace` *App Launch* template; or wall clock from `open -a` to the first window plus a scripted keystroke via accessibility | `t_content` < 250 ms warm, < 600 ms cold |
| **10k-line diff open** | Click Review on the fixture repo; then fling-scroll top to bottom | time from click → first diff frame (parse is off-thread: report daemon git time + parse time + first paint); scroll p99 frame time | Instruments hitches + wall clock | First paint < 150 ms; scroll p99 < 8.3 ms |
| **Terminal throughput & latency** | `time cat 100MB.txt`; `vtebench` suite; keystroke→echo latency (`typometer`-style) | counters: terminal renders/s, rows rebuilt per frame | wall time and CPU of the app process tree; for latency, high-speed camera or `typometer` | ≥ alacritty-in-Zed throughput; echo latency < 1 frame plus PTY latency |
| **RPC latency** | 1,000 × `HydrateSession` (small) and file-search requests | client-side round-trip histogram | n/a (internal) | p50 < 1 ms local; today about 25 ms from the 25 ms polls (**estimate**) |

### 4.4 Regression gating
- **Deterministic CI (Linux and macOS runners).**
  - `cargo bench` with `divan` or `criterion` microbenches for:
    - `IncrementalParser::append` across 20k tokens (assert total work is near-linear, not quadratic);
    - `flatten` and `code_runs` on a 3,000-line block;
    - `highlight::tokenize`;
    - `review_diff::parse` on a 10k-line patch;
    - `TerminalSession::snapshot` on a 200×60 grid;
    - wire encode/decode of 10k events;
    - daemon `save_task_state` on a 2k-message session.
  - Store baselines and fail on > 10% regression, using `--save-baseline`, or bencher.dev / codspeed for noise-controlled CI.
- **Counter budgets (headless, deterministic).** Run the app with `perf-counters` and `waku-bench-daemon` under a scripted 10 s `burst`, then assert:
  - commits/s ≤ budget;
  - transcript pane renders per commit ≤ 1;
  - sidebar and right-panel renders during streaming = 0 (after §3.4);
  - root renders during scroll = 0 (after §3.2);
  - UI-thread blocked time inside `save` = 0;
  - allocated bytes per commit ≤ budget.

  These are stable across machines and catch regressions like the "`ReasoningDelta` didn't set `markdown_changed`" class described in `docs/performance.md:63–69`.
- **Nightly on dedicated Apple Silicon (ProMotion display attached or headless at 120 Hz).** The full §4.3 matrix for Waku and T3 Code, with results published as a trend dashboard. Use `taskpolicy`/`caffeinate` and a fixed brightness/power state to reduce variance, and report the median of 5 runs.

---

## 5. What not to break (existing good decisions)
- Stream coalescing that does not race the wake channel (`src/app.rs:2662–2670`).
- The rule that reasoning deltas set `markdown_changed` (`runtime.rs:3640–3648`).
- The self-parking pulse clock and stride rules (`src/ui/motion.rs`). Keep the semantics when moving to vsync alignment.
- The incremental parser and stable-prefix flatten cache (`src/md/parser.rs:728+`, `src/md/render.rs:449+`).
- Off-thread diff parsing and tokenizing (`src/review_diff.rs`).
- Off-thread hydration (`src/app/sessions.rs:100`).
- The math workers and bounded caches.
- Bounded subscriber queues with kick-and-replay (`server.rs:312–333`).
- WAL + `synchronous=NORMAL`.
- Fingerprint-skipped message writes.
- `test-support` kept out of release builds (`Cargo.toml:65–75`).
