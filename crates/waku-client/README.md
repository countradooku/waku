# waku-client

`waku-client` is the Rust client for `waku-daemon`. It owns the protocol v8
connection (`V8Client`: the handshake, calls, streams and their credit),
local-daemon supervision (`DaemonSupervisor`), the client-side projections
the desktop renders from, and disposable client preview caches. It depends on
`waku-protocol`, never on `waku-core`.

`unix:<path>` and `pipe:<name>` local endpoints, bare socket addresses and
complete `ws://` or `wss://` URLs are accepted. Dropping a connection to an
externally managed daemon does not stop it.
