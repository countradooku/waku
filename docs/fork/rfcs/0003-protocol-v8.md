# RFC 0003: `waku-protocol` v8, our own typed protocol

- **Status:** Draft. Tracked in [#13](https://github.com/countradooku/waku/issues/13).
- **Implements:** [#53](https://github.com/countradooku/waku/issues/53) (protocol), [#54](https://github.com/countradooku/waku/issues/54) (streams), [#56](https://github.com/countradooku/waku/issues/56) (output hub), [#57](https://github.com/countradooku/waku/issues/57) (wire projection) and [#144](https://github.com/countradooku/waku/issues/144) (TS client).
- **Decision D1:** every client speaks this protocol. No T3 wire compatibility is planned.

## Problems with v7

- JSON text frames carry `{kind: String, payload: serde_json::Value}` events, which are double-encoded and deep-cloned per subscriber.
- Both ends poll every 25 ms, and writes wait for the read timeout.
- Blobs travel as base64 inside JSON, bounded by a 48 MiB frame.
- A single full-control bearer token; no TLS; no keepalive.
- Replay is limited to an in-memory 2,048-event journal, and clients receive whole sessions.

## Design

### Schema

- **Source of truth:** a Rust crate (`waku-protocol`). Every message is a Rust type.
- **TypeScript:** types and codecs are generated for web and mobile. `protocol:check` keeps them byte-identical.
- **Forward compatibility:** every tagged union has `#[serde(other)] Unknown`. Clients skip unknown variants and still advance cursors.

### Encodings

| Client | Encoding |
|---|---|
| Native desktop (local) | Binary (postcard) frames over a Unix domain socket or named pipe |
| Native desktop (remote) | Binary over `wss://` |
| Web, mobile | JSON over `wss://` (binary optional later) |
| Blobs and attachments | Raw binary frames (chunked), referenced by id |

### Transport

- **Local:** a per-user socket (mode 0600) with a peer-credential check. TCP is opened only when exposure is enabled.
- **Remote:** TLS. Use the daemon's own certificate (pinned during pairing) or terminate TLS at Tailscale or an SSH tunnel.
- **Keepalive:** ping every 5 s, timeout after 15 s.
- **Handshake:** `Hello{protocol_version, client_kind, credential, resume: Vec<StreamCursor>}`, answered with `Welcome{daemon_version, environment_id, scopes}` or `Rejected{reason}` (version gate).

### Messages

```
Client → Daemon
  Request { id, method: Method }                 // unary
  Subscribe { id, stream: StreamSpec, after: Option<Cursor> }
  Unsubscribe { id }
  BlobPut { id, chunk, last } / BlobGet { id, blob }

Daemon → Client
  Response { id, result: Result<MethodResult, Error> }
  StreamItem { sub, item }                        // see streams
  BlobData { id, chunk, last }
  Notice { kind }                                 // server lifecycle, update available, …
```

`Method` groups:

| Group | Methods |
|---|---|
| orchestration | `dispatch(Command) -> Sequence`, `get_turn_item`, `history_page`, `search_threads`, `search_thread` |
| projects, files | List, search entries/contents, read/write with revision, browse directory |
| vcs | Status, refs, worktrees, diffs, commit/push/pull, PR operations |
| terminal | Open, write, resize, clear, restart, close |
| providers | Status, instances, auth flows, models, usage |
| settings, keybindings, scheduled tasks, auth | Pairing, sessions, revoke |

### Streams

| Stream | Items |
|---|---|
| `Shell{project?}` | `Snapshot{rows, sequence}`, `ThreadUpdated{row, location}`, `ThreadRemoved`, `ProjectUpdated`, `Synchronized{sequence}` |
| `Thread{thread_id}` | `Snapshot{projection, history_cursor, has_more_history}` (bounded: 10 user turns, 75 items, 1 MiB), `Event{sequence, DomainEvent}`, `Synchronized` |
| `CommandOutput{thread_id, item_id}` | `Replace{bytes}`, `Append{bytes}` (100 ms frame interval, 16 KiB append cap); never events |
| `Terminal{terminal_id}` | Bounded tail on attach, then output chunks, with flow control |
| `VcsStatus{project}` | Leased to the visible viewport |

Rules:
- **Resume:** when the gap is at most 128 events and 1 MiB, replay events; otherwise send a fresh bounded snapshot.
- **Live budget:** at most 1,000 queued items or 8 MiB per subscription. When it overflows, fail the subscription and let the client resubscribe with its cursor.
- **Wire projection:** tool outputs and file-change bodies are omitted (`output_omitted`, `output_indicates_failure`). Clients fetch them with `get_turn_item` when a row expands.
- **Coalescing:** text and reasoning deltas are merged in 8–16 ms windows. Running tool updates are coalesced over 50 ms.

### Auth

- **Pairing:** a one-time URL or QR code with the secret in the fragment. It yields a scoped session credential.
- **Scopes:** `read`, `operate` and `admin`, plus capability scopes (`source_control:write`, `filesystem:write`, …). Every method declares the scope it needs.
- **Sessions:** a client list with revoke. MCP credentials are a separate audience (RFC 0001, Phase 4).

### Versioning

- `PROTOCOL_VERSION = 8`. A breaking change bumps the major version.
- Additive changes rely on the unknown-variant rule.
- The daemon serves exactly one major version.

## Migration

- v7 stays in place until the desktop and web clients are on v8.
- The daemon serves v8 only after the switch.
- Mobile moves with the generated TS client ([#144](https://github.com/countradooku/waku/issues/144)).

## Open questions

1. postcard or MessagePack for native frames. postcard is smaller and faster; MessagePack is easier to inspect.
2. Whether web should also use binary frames once the TS codec exists.
3. Per-message deflate for JSON clients on slow links.
