# Waku

> **Built on [Waku](https://github.com/egoist/waku), created by
> [egoist](https://github.com/egoist).** This is an independent community fork.
> Waku's native Rust/GPUI application, agent integrations, daemon architecture,
> and desktop, web, and mobile clients are the work of egoist and Waku's
> contributors. Please visit the original project, give it a star, and
> [support egoist](https://github.com/sponsors/egoist).

This fork builds on that foundation with additional Git and review workflows,
informed by [T3 Code](https://github.com/pingdotgg/t3code) and adapted to Waku's
architecture. It is maintained independently; it is not an official Waku or
T3 Code release. Original history, authorship, and license notices are retained.

The first increment adds file staging and unstaging, fast-forward-only pull,
worktree inspection, worktree creation checks, and an ignore-whitespace review
option. See [the workflow guide](docs/source-control.md) for availability and
limits, and [the parity roadmap](docs/parity-roadmap.md) for remaining work.
Full T3 Code feature parity is a continuing goal, not a current claim.

**This fork has no published binary release yet.** Build this checkout using
the Development instructions below to try its changes. The Install links below
are for the original Waku releases and do not include this fork's additions.

Waku is a fast, native desktop app for working with local coding agents. It is
built in Rust with [GPUI](https://github.com/zed-industries/zed/tree/main/crates/gpui)
and keeps projects, sessions, transcripts on your machine.

## Install

Download builds from [GitHub Releases](https://github.com/countradooku/waku/releases).
This fork's release host and update feeds are configured per
[docs/fork/RELEASING.md](docs/fork/RELEASING.md); builds made without that
configuration do not update themselves.

On Linux, once releases are published:

```sh
curl -fsSL "$WAKU_RELEASES_URL/install.sh" | sh
```

The script installs into `~/.local` without root. See
[docs/linux.md](docs/linux.md) for requirements, manual installation, and
uninstalling.

On Windows, run `Waku-<version>-<arch>-Setup.exe` from the
[latest release](https://github.com/countradooku/waku/releases/latest). It
installs per-user. A portable `.zip` is published alongside it. See
[docs/windows.md](docs/windows.md) for requirements and what is not available
there yet.

## Supported agents

Waku works with:

- [Amp](https://ampcode.com/)
- Claude Code
- Codex CLI
- Cursor CLI
- [Fx](https://fx.sh/)
- Grok Build
- Kimi Code
- OpenCode
- Pi

Install and authenticate at least one supported agent CLI before starting Waku.
Waku detects available CLIs automatically and uses each provider's native
structured protocol and session continuity.

## Highlights

- Keep projects and independent agent sessions in one native app.
- Switch models, reasoning effort, and access modes from a shared interface.
- Queue or steer follow-up messages while an agent is working.
- Rewind Git-backed tasks with conversation-aware checkpoints.
- Store app state locally, with no Waku account or remote service required.

## Architecture

The native desktop is an RPC client of the standalone `waku-daemon` process.
Provider sessions run in [`waku-core`](crates/waku-core), behind the
authenticated, versioned WebSocket contract in
[`waku-protocol`](crates/waku-protocol). Waku Desktop depends on
[`waku-client`](crates/waku-client), not on the daemon implementation. The
daemon owns task SQLite data, uploaded attachments, provider-native session
forks, and all workspace filesystem and Git operations; paths returned by it
always refer to the daemon host. The desktop retains only presentation state
and a disposable preview cache.

The browser client lives at [`apps/web`](apps/web) and uses the generated
browser transport in [`packages/waku-client`](packages/waku-client). Its
checked-in types are generated directly from the Rust protocol, while its
WebSocket client implements the same handshake, request IDs, subscriptions,
sequence deduplication, and replay cursors as the Rust client. Run
`bun run protocol:generate` after changing a wire type and
`bun run protocol:check` to verify that generated files are current.

Projectless task workspaces live on the daemon host under
`~/.waku/projects/<date>/<slug>`. The daemon moves workspaces created by the
older `~/.waku/<date>/<slug>` layout on first load.

Configuration ownership is separate too: the Release desktop writes
`~/.waku/app.json`, while Debug stays isolated at `temp/app.json`. Daemon
provider and Computer Use settings live in `~/.waku/settings.json`. The
desktop's Settings → Daemon page can explicitly
expose the child daemon on a fixed port, configure exact browser origins, and
copy its stable authentication token. It remains loopback-only by default.

When connected to a daemon managed outside the desktop process, Waku never
interprets daemon paths on the client machine. New project browses the daemon
host's folders instead of opening the local folder dialog, and terminals are
always daemon-owned PTYs (ConPTY on Windows) that keep 5,000 lines of
scrollback and replay it when a client attaches or reconnects. Files, diffs,
Git, skills, usage, task state, and attachments use daemon RPC too. Actions
that only make sense on the daemon's own machine are not offered for a remote
daemon: "Open in…" and a skill's Open and Reveal are hidden, an attachment's
"Show in File Manager" is disabled with a note saying why, and a file link in
the transcript or a terminal names a daemon-host path that the desktop does
not try to open.

Release apps bundle and sign `waku-daemon`. Development keeps the daemon at
`target/debug/waku-debug-daemon`, allowing provider-only edits to rebuild and
replace the daemon without relaunching Waku Debug.

## Development

Development is supported on macOS, Linux, and Windows and requires
[Rust 1.96 or newer](https://www.rust-lang.org/tools/install) and
[Bun](https://bun.sh/). Linux supports both Wayland and X11, and Windows needs
the MSVC toolchain; install the native build prerequisites listed in
[CONTRIBUTING.md](CONTRIBUTING.md) first.

```sh
bun install
bun run dev
```

The embedded browser and experimental computer-use integration currently
remain macOS-only. Agent sessions, projects, transcripts, skills, usage,
diffs, file editing, and the terminal run natively on Linux and Windows.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and checks.
Release maintainers should also read [RELEASING.md](RELEASING.md).

## Sponsorship

Please support **egoist, Waku's original author**, via
[GitHub Sponsors](https://github.com/sponsors/egoist). This link supports the
upstream project that makes this fork possible.

## License

Waku is licensed under the [GNU General Public License v3.0 only](LICENSE).
