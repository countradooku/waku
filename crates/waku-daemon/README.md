# waku-daemon

`waku-daemon` is the standalone process that hosts Waku's provider sessions.
It defaults to a loopback-only listener, authenticates clients with
`WAKU_DAEMON_TOKEN`, and
prints one JSON readiness record to stdout containing its address, protocol
version, and process ID.

```text
WAKU_DAEMON_TOKEN=<secret> waku-daemon --bind 127.0.0.1:0 [--parent-pid PID] [--allow-origin ORIGIN]...
```

## Background service (#58)

```text
waku-daemon service install|uninstall|status [--dry-run]
waku-daemon doctor [--json]
waku-daemon --service
```

`service install` makes the daemon a per-user service that outlives the
desktop: a launchd user agent on macOS (`~/Library/LaunchAgents/<label>.plist`),
a systemd user unit on Linux (`~/.config/systemd/user/<unit>.service`), a
logon scheduled task on Windows (written from the documentation, not run on
Windows yet). The definition runs a stable launcher,
`~/.waku-fork/bin/waku-fork-daemon-service` (`waku-fork-dev-…` for debug
builds), which execs the binary that installed it with `--service`; output
goes to `~/.waku-fork/logs/waku-fork-daemon.log`. `--dry-run` prints the files
and commands without touching anything. launchd and systemd restart a daemon
that died, and the restarted daemon continues the turns the crash
interrupted (#59).

`--service` listens on the default local socket only and reads its token
from the local token file next to it (`run/local.token`, mode 0600, created
on first start). Waku Desktop looks there first: when the socket answers the
handshake with that token it attaches to the running daemon and never stops
it; only when nothing answers does it spawn its own child. With "keep
running" (`DaemonExposureSettings::keep_running`) the child it spawns is
detached instead of following the desktop's pid, writes the same token file
and log, and the next launch attaches to it.

`doctor` checks the data directory, the orchestrator database (integrity,
migrations, the projection verify, the effect outbox), which provider CLIs
are found, whether the local socket answers, the service state and the log
location; it exits 1 when a check fails. Opening the database applies
migrations the binary knows, as a start would.

Waku Desktop supervises this process. Debug builds use the feature-gated
`waku-debug-daemon` target at `target/debug/waku-debug-daemon`, so rebuilding
provider code replaces only the daemon. Release distributions place the signed
`waku-daemon` binary beside the desktop executable.

The token is a full-control capability for a trusted Waku client, not a user or
workspace-scoped credential. Browser handshakes are rejected unless their exact
Origin was supplied with `--allow-origin`; native clients send no Origin. A
non-loopback bind is refused unless `--allow-non-loopback` is also present.
Waku Desktop adds that flag only after the user enables exposure in Settings →
Daemon. The daemon does not terminate TLS itself. For access outside a private
network, put a trusted TLS proxy or tunnel in front of it and use `wss://`. Do
not give the daemon token to untrusted page JavaScript.
