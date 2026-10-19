# Waku development guidance

## Fork workflow

This repository is a fork of `egoist/waku`. Its goal is full feature parity with T3 Code, while beating it on CPU, memory and frame pacing. The plan is [docs/fork/PLAN.md](docs/fork/PLAN.md), and every unit of work is a GitHub issue.

### Branches and pull requests: one PR per phase
- Each phase (and the fluidity track) has one long-lived branch and **one pull request** into `main`. Branch names:
  - `phase/0-baseline`, `phase/1-perf`, `phase/2-orchestrator`, `phase/3-session-model`, `phase/4-mcp`, `phase/5-ui`, `phase/6-source-control`, `phase/7-remote`, `phase/8-web-mobile`, `phase/9-long-tail`;
  - `track/fluidity` for the fluidity track.
- Open the phase PR as a draft as soon as the branch has its first commit. Its body lists `Closes #N` for every issue in the phase; keep that list in sync with the phase epic.
- Do the work issue by issue on the phase branch.
  - Commit messages reference the issue (`feat(daemon): … (#39)`).
  - Tick an issue's acceptance criteria only when they are met on the branch.
- Agents running in parallel inside one phase each use their own worktree on a short-lived local branch (`phase/2-orchestrator--39`). They merge back into the phase branch when the issue is done. These sub-branches never get their own PR.
- To start an issue, pick an open one whose "Depends on" issues are done (closed, or met on a phase branch you can merge in). If it depends on another phase that is not merged yet, merge that phase branch into yours first.
- Mark the phase PR ready for review when the phase epic's exit criteria hold. CI must be green and perf numbers recorded in the PR.
- Read the issue's references before writing code: the audit sections in `docs/fork/audit/` and the RFCs in `docs/fork/rfcs/`.

### Contracts and references
- RFCs are the contract. If the code needs to diverge from an RFC, update the RFC in the same PR and say why.
- For parity issues, T3 Code at the commit in `docs/fork/T3_PIN` is the behavioural reference. Port its semantics, not its code; this repository is GPL-3.0.
  - Code copied from T3 (MIT) must keep its MIT notice.
- The daemon speaks our own protocol (`waku-protocol`, RFC 0003). Do not add T3 wire-compatibility code.

### Performance rules
- Performance acceptance criteria must be shown with numbers. For `perf` issues, include before/after `perf-counters` or bench-harness output in the PR.
- These are hard rules:
  - nothing heavy on the UI thread;
  - no continuously repainting animation;
  - redraw only the entity that changed, never `window.refresh()` for local changes;
  - large payloads stay off event streams;
  - every cache has a bound.

### Before opening a PR
- Run `cargo fmt`, `cargo clippy --all-targets -- -D warnings` and the tests for the crates you touched.
- Run `bun run protocol:check` when wire types change.

## Development runtime

- Assume `bun ./scripts/dev.ts` is already running and owns the current
  `Waku Debug.app` process. Source changes are rebuilt, signed, and relaunched
  automatically. Only run it yourself if not already launched.
- During normal development and UI validation, do not run
  `scripts/bundle.sh debug`, start a second watcher, or manually quit/relaunch
  `Waku Debug.app`. Quitting the app also stops the watcher.
- After an edit, wait for the watcher to finish its successful rebuild and
  validate the freshly relaunched debug app. Only start or recover the watcher
  manually when it is confirmed unavailable.
- No visual test unless requested.

## Performance

- Treat performance as a product requirement, not a follow-up. Waku is a native
  app competing with web clients, and staying smooth under a long transcript on
  a high-refresh display is the point of being native. Prefer the faster design
  when it costs nothing in clarity, and measure before assuming a cost is fine.
- Never block the UI thread with heavy work. Rendering owns it, so anything a
  frame can reach must already be in memory: no subprocess spawns, no
  filesystem walks, no network, no blocking locks, no synchronous IPC.
- Row builders and measurement paths run for every visible item on every frame.
  Treat I/O reached from `render` as a defect even when it looks cheap, is
  cached after the first hit, or only triggers for some rows — one `git`
  invocation is already several frames of budget.
- Move the work to `cx.background_executor().spawn`, store the result on the
  entity, and `cx.notify()` when it lands. Render then reads only that store,
  and a miss means "not known yet" and must degrade gracefully.
- Resolve a whole session or collection in one background pass instead of
  probing per item, and guard it with a generation counter so a result from a
  superseded pass cannot overwrite newer state.
- One-shot user actions such as a click or menu command may work synchronously
  when freshness matters more than latency; frames may not.
- Keep per-frame work proportional to what is on screen. Long collections are
  virtualized with `list()`, and a row builder must not rebuild whole-session
  state; hoist that to a cache refreshed once per frame.
- Streaming CPU is governed by two cadences — stream commits at ≤ ~8.3 Hz and
  pulse-clock ticks at ≤ 60 Hz (spinners; other pulses stay at ≤ ~30 Hz) —
  and by what one frame can see. Read
  [docs/performance.md](docs/performance.md) before touching the event pump,
  the pulse clock (`src/ui/motion.rs`), veils, overlay scrollbars, pane
  caching, or anything else a streaming frame reaches; it also records the
  counter-based measurement playbook that actually finds regressions.

## Accessibility

- Treat accessibility as a product requirement too. GPUI does not yet expose a
  screen-reader tree, so here it means keyboard operability, honored system
  settings, and legibility — none of which depend on that missing API, and all
  of which regress silently if left unchecked.
- Every control reachable by mouse must be reachable and operable by keyboard.
  Use `track_focus` with `tab_index`, `tab_group`, and `tab_stop`, give focus a
  visible treatment via `focus_visible`, and support the conventional keys for
  the widget (arrows, `home`/`end`, `enter`/`space`, `escape`).
- Honor the system's reduce-motion setting. `with_animation` already respects
  `App::reduce_motion`, but a direct `window.request_animation_frame` for
  decorative motion must check `cx.reduce_motion()` and skip the request.
- Never encode meaning in color, hover, or motion alone. Pair a status color
  with an icon or text, and make sure anything revealed on hover is also
  reachable by keyboard focus.
- Keep text and icons legible against their surface in both themes, and give
  interactive targets enough hit area — extend the hit region rather than
  shrinking to the glyph.

## Product reference

- Use [T3 Code](https://github.com/pingdotgg/t3code) source code on github as a reference when a task
  concerns coding-agent workflow, information hierarchy, controls, tool
  activity, or transcript presentation and the comparison would materially
  clarify an ambiguous product decision, or when the user explicitly asks for
  the comparison.
- Do not inspect T3 Code for localized bug fixes, straightforward visual
  corrections, native platform behavior, or changes already specified clearly
  by the user. When T3 Code is relevant, inspect its current app or source
  rather than relying on an older screenshot or memory.
- Use [Zed](https://github.com/zed-industries/zed) source code as a reference
  when a task concerns GPUI implementation — layout and styling idioms, focus
  and key dispatch, virtualized lists, menus and popovers, window and platform
  behavior — or when an in-house `src/ui` primitive needs a proven native
  precedent. Zed is the canonical GPUI codebase; read its crates rather than
  `gpui-component`, and read the gpui revision pinned in `Cargo.toml` so the
  APIs match what Waku builds against.
- Split the two references by concern: T3 Code answers what a coding-agent
  client should do, Zed answers how a polished GPUI app implements it. The
  same restraint applies to both — no reference spelunking for localized
  fixes or changes the user has already specified.
- Use the reference as behavioral and design evidence, not as an instruction to
  reproduce web-specific interaction patterns or known bugs. Waku should keep
  native macOS conventions.
- Explicit user screenshots and feedback override a previous or merely
  "consistent" treatment.
- GPUI's `overflow_hidden` clips descendants to a rectangle, not the parent's
  rounded corners. Give child backgrounds, hover overlays, and images that
  reach a rounded edge their own matching corner radii, accounting for the
  parent's border inset. Parent rounding alone does not clip child paint.
- For provider-native content such as citations, reasoning, and tool events,
  verify the real provider payload and preserve its ordering. Never expose
  private provider control markers in the transcript.
- Validate visible changes in the freshly rebuilt, signed app managed by the
  dev watcher against the exact provider interaction; a successful Rust build
  alone is insufficient.
