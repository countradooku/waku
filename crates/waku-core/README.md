# waku-core

`waku-core` is Waku's daemon-only runtime. It contains the provider drivers
(all on `DriverV2`, RFC 0002), provider discovery and model metadata, the
composition of the orchestrator (RFC 0001) with its effect ports, attachment
and blob storage, workspace filesystem and Git services, the PTY host,
Computer Use process control, and daemon-owned settings. It depends on the
serializable contract in [`waku-protocol`](../waku-protocol), but contains no
desktop transport or UI.

The daemon speaks protocol v8 only (RFC 0003; decision D6 removed the legacy
protocol): a WebSocket upgrade to `/v8` with the `waku.v8` subprotocol on the
TCP listener (loopback by default), or the 8-byte `WAKU` preface on the
per-user local socket. Projects, threads and transcripts live in the
orchestrator's event store (`state.sqlite`); clients render from its `shell`
and `thread` streams.

`V8Client` lives in [`waku-client`](../waku-client), which is what Waku
Desktop depends on. `serve_listeners`, `WakuBackend` and `DaemonHost` are used
by the `waku-daemon` binary.

Configuration ownership is explicit:

- the desktop owns `~/.waku/app.json` in Release and checkout-local
  `temp/app.json` in Debug;
- the daemon owns `~/.waku/settings.json`.

The orchestrator database and durable attachment materializations are
daemon-owned as well. Client-local attachment paths are upload inputs or
caches only; provider prompts and persisted messages use daemon-issued paths
and references. Projectless task directories are daemon-owned too and live
beneath `~/.waku/projects`.
