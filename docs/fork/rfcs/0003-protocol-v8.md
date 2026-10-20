# RFC 0003: `waku-protocol` v8, our own typed protocol

- **Status:** Accepted (pending merge of Phase 0 PR). Tracked in [#13](https://github.com/countradooku/waku/issues/13).
- **Implements:** [#53](https://github.com/countradooku/waku/issues/53) (protocol), [#54](https://github.com/countradooku/waku/issues/54) (streams), [#56](https://github.com/countradooku/waku/issues/56) (output hub), [#57](https://github.com/countradooku/waku/issues/57) (wire projection) and [#144](https://github.com/countradooku/waku/issues/144) (TS client).
- **Decision D1:** every client speaks this protocol. No T3 wire compatibility is planned.
- **Decision D2:** mobile becomes native Swift and Kotlin apps later, so the schema must generate more than TypeScript.
- **Related:** [RFC 0001](0001-orchestrator.md) (event store, entities, cursors), [RFC 0002](0002-driver-v2.md) (normalized adapter events, capabilities).
- **Audits:** [t3-server.md](../audit/t3-server.md) §1.12, §3, §4; [waku-perf.md](../audit/waku-perf.md) §1.2, §3.3, §3.7; [t3-remote-auth-install-cli.md](../audit/t3-remote-auth-install-cli.md) §2–§4A.

## Naming

- **v7** in this RFC means the current JSON protocol in `crates/waku-protocol/src/protocol.rs`. The audit recorded it as `PROTOCOL_VERSION = 7`; upstream has since bumped that constant to `8` without changing the design.
- **v8** means the protocol specified here.
- The two never share an endpoint. Legacy JSON stays on TCP `/v1`. v8 lives on its own socket and WebSocket path and starts with its own preface, so equal numbers cannot be confused.
- In code, the legacy constant is renamed `LEGACY_PROTOCOL_VERSION`, and the new one is `waku_protocol::v8::PROTOCOL_MAJOR = 8`.

## Problems with v7

- JSON text frames carry `{kind: String, payload: serde_json::Value}` events. They are double-encoded (`DriverEvent → Value → String`) and deep-cloned per subscriber.
- Both ends poll every 25 ms, so writes wait out the read timeout. That adds about 25 ms to every RPC and about 80 wakeups per second when idle.
- Blobs and terminal bytes travel as base64 inside JSON, bounded by a 48 MiB frame.
- There is a single full-control bearer token, no TLS and no keepalive.
- Replay is limited to an in-memory journal of 2,048 events per runtime. Clients receive whole sessions (`HydrateSession`, `LoadTaskState`) and write them back (`SaveTaskState`).
- A slow subscriber is kicked off the whole connection, not just the stream that overflowed.
- `RpcError` is a single string, so clients cannot branch on the cause.

## Decisions

| # | Question | Decision |
|---|---|---|
| P1 | postcard or MessagePack for native frames | **MessagePack** (`rmp-serde`, structs as maps with named fields). postcard is rejected. See [P1](#p1-messagepack-not-postcard). |
| P2 | Forward compatibility for unknown variants | Every open union is internally tagged (`"type"`) and ends in `#[serde(other)] Unknown`. Unknown struct fields are ignored. This works because both encodings are self-describing. |
| P3 | Binary for web | Web sends and receives **messages as JSON text frames** and **bulk bytes as binary DATA frames**. MessagePack for web is allowed by the handshake but stays off until a benchmark justifies it. |
| P4 | Compression | **No permessage-deflate.** Optional per-frame zstd for MSG bodies of 16 KiB or more, only on remote native connections and only when both sides agree. Local sockets and web never compress. |
| P5 | Framing | A 12-byte header (length, kind, flags, channel) on every binary frame. One frame per WebSocket message. 4 MiB message cap, 256 KiB DATA chunks. See [§1.4](#14-framing). |
| P6 | Codegen | Rust types are the source of truth. They export a JSON Schema subset plus a protocol manifest, and our own generator emits TypeScript now and Swift/Kotlin later. ts-rs stays only for legacy types. See [§9](#9-codegen). |
| P7 | Transport runtime | The daemon's transport moves to tokio (reader task and writer task per connection). Nothing polls. Whether drivers also move to tokio stays open in RFC 0002. |
| P8 | Resume | There is no server-side session resumption. After `Hello`, the client re-subscribes with per-stream cursors and the daemon decides between replay and a snapshot. Mutations are retried with idempotency keys. |
| P9 | Backpressure | Overflowing one subscription ends that subscription (`end{overflow}`) and the client resubscribes from its cursor. Byte streams use credit-based flow control. The connection is closed only when the socket stops draining. |
| P10 | Local auth | Unix domain socket (named pipe on Windows) with a peer-credential check plus a local token. TCP is opened only when remote access is enabled. |

### P1: MessagePack, not postcard

postcard is smaller and faster to encode, but it does not fit a protocol that must evolve without a major bump:

1. **It cannot skip what it does not know.** postcard writes no field names and no lengths. A decoder that meets an unknown variant tag, or a struct with an extra trailing field, cannot tell where the value ends, so the rest of the frame is lost. `#[serde(other)]` can map the tag, but it cannot skip the payload. Fixing that means length-prefixing every extensible struct and enum by hand, which amounts to reinventing protobuf.
2. **It rejects serde features that both our types and the forward-compatibility rule rely on.** Internally tagged enums and `#[serde(other)]` need `deserialize_any`, as do `skip_serializing_if` (61 uses in `waku-protocol` today), `untagged`, `flatten` and `serde_json::Value` (70 uses).
3. **Every other language would need a generated byte-exact codec.** MessagePack has maintained libraries for TypeScript, Swift and Kotlin, so generated code only needs to map types.
4. **One data model serves both encodings.** MessagePack with named fields has the same logical shape as JSON, so one set of serde attributes, one schema and one TS type set cover native and web. A frame can be dumped to JSON for debugging.

MessagePack with named fields costs more bytes than postcard. Field names add roughly 1.3–2× on small messages. That cost is minor here:
- per-message overhead is dominated by delta coalescing (8–16 ms windows);
- the costs v7 actually paid were the `Value` hop, per-subscriber clones, polling and base64, and this design removes all four.

`waku-bench-daemon` measures bytes per event and encode/decode time per event as part of #53. Storage encoding for the event log (RFC 0001, open question 1) is a separate decision.

## 1. Wire format

### 1.1 Data-model rules (enforced by the schema exporter)

- **Field names.** Structs serialize as maps with `camelCase` field names. Never use `deny_unknown_fields`.
- **Optional fields.** New fields must be `Option<T>` or carry `#[serde(default)]`. An `Option` that is `None` is omitted, not written as `null`, and decoders accept both.
- **Tags.** Every enum that carries data is internally tagged with `#[serde(tag = "type", rename_all = "camelCase")]`. Variants are struct or unit variants only; a newtype of a primitive is not allowed.
- **Open string enums** (error codes, scopes, provider kinds, statuses) keep the raw value in `Unknown(String)`. The `open_string_enum!` macro generates the serde impls.
- **No `serde_json::Value`, no `untagged`, no `flatten`.** Opaque provider JSON (computer-use arguments, native metadata) travels as `JsonText(String)`.
- **No `HashMap`.** Maps use `BTreeMap<String, T>` so encoding is deterministic.
- **Integers.** `u64` values on the wire stay at or below 2^53 − 1, so JavaScript numbers can hold them. This includes sequences, sizes and offsets.
- **Ids** are canonical lowercase hyphenated UUID strings. (Implemented by running `rmp-serde` in human-readable mode, so `Uuid` serializes as a string in both encodings; see `waku_protocol::v8::codec`.)
- **Timestamps** are `i64` milliseconds since the Unix epoch (UTC).
- **Paths** are UTF-8 strings. Non-UTF-8 paths are reported as errors, not converted lossily.
- **Bytes:** MessagePack `bin` via `serde_bytes`, and standard padded base64 in JSON. Byte fields are reserved for small values (under 64 KiB). Bulk bytes use DATA frames ([§5.6](#56-blob-transfer)).
- **Large values.** A response field that can exceed 1 MiB is typed `Payload<T> = inline{value: T} | blob{reference: BlobRef, size}`. The client fetches the blob form through blob transfer.

### 1.2 Forward compatibility

- An unknown variant tag decodes to `Unknown`. The rest of that value is skipped, and the enclosing frame still decodes.
- A client that receives `Unknown` inside a stream item still advances its cursor (T3's `ForwardCompatibleUnion` rule).
- An unknown method name decodes to `Method::Unknown{name}` with its params skipped, and the daemon replies `unknownMethod`.
- An unknown frame `kind` is skipped using its length. Unknown frame flags are a protocol error, because a flag can change how the body is read.
- Behaviour is gated by feature strings exchanged in the handshake, not by version comparisons.
- A breaking change bumps `PROTOCOL_MAJOR`. Breaking changes are: removing or renaming a field, variant or method; changing a type; or changing the meaning of an existing value. The daemon serves exactly one major version.

### 1.3 Encodings

| Client | Messages | Bulk bytes |
|---|---|---|
| Desktop, local | MessagePack MSG frames over the local socket | DATA frames |
| Desktop, remote | MessagePack MSG frames over `wss://` (binary WS messages) | DATA frames |
| Web | JSON over `wss://` (text WS messages) | DATA frames (binary WS messages) |
| Native mobile, later | JSON over `wss://` at first, MessagePack once measured | DATA frames |

A connection's message encoding is fixed by its first message:
- on WebSocket, a text `Hello` means JSON and a binary `Hello` means MessagePack;
- the local socket is always MessagePack.

Mixing message encodings on one connection is a protocol error.

### 1.4 Framing

**Local stream transports** (UDS, named pipe) start with an 8-byte preface in each direction:

```
57 41 4B 55   "WAKU"
00            reserved
08            protocol major
00 00         reserved
```

- The client sends its preface first, and the daemon answers with its own.
- If the majors differ, the daemon sends its preface and closes. The client reports `versionUnsupported`.
- Frames follow.

**Binary frame** (used on local streams and as the body of every binary WebSocket message):

| Offset | Size | Field | Meaning |
|---|---|---|---|
| 0 | 4 | `body_len` | `u32` big-endian length of the body, excluding this 12-byte header |
| 4 | 1 | `kind` | `0x01` MSG (an encoded message), `0x02` DATA (raw bytes). Other values are skipped. |
| 5 | 1 | `flags` | bit 0 `FIN` (DATA: last chunk of a transfer), bit 1 `ZSTD` (MSG body is zstd-compressed). Other bits must be 0. |
| 6 | 2 | reserved | `0` |
| 8 | 4 | `channel` | `u32` big-endian. `0` for MSG. For DATA, the subscription or transfer channel ([§2.4](#24-streams)). |
| 12 | `body_len` | body | |

**WebSocket mapping:**
- path `/v8`, subprotocol `waku.v8`;
- an upgrade without the subprotocol is answered with HTTP 426;
- one frame per WebSocket message;
- for a binary message, `body_len` must equal the message length minus 12;
- a text message is one MSG frame encoded as JSON, with no header;
- WS-level pings are answered, but the application ping below is the keepalive that counts, because browsers cannot send WS pings;
- the daemon refuses the `permessage-deflate` extension.

**Size limits:**

| Limit | Value |
|---|---|
| MSG body before `Welcome` | 64 KiB |
| MSG body after `Welcome` (`maxMessageBytes`) | 4 MiB, measured after decompression |
| DATA body (`maxDataChunkBytes`) | 256 KiB |
| Blob size (`maxBlobBytes`) | 64 MiB by default; attachments keep their 32 MiB per-file cap |

- A frame over a limit is a protocol error: the receiver sends `goodbye{frameTooLarge}` where possible and closes.
- Larger content is chunked into DATA frames or paged through cursors. No message is designed to exceed 1 MiB in normal use; bounded snapshots and history pages are 1 MiB.

**Compression:**
- `Hello.compression` may offer `["zstd"]`, and `Welcome.compression` selects it or nothing.
- When selected, either side may compress a MSG body of 16 KiB or more at zstd level 3 and set `ZSTD`.
- A compressed body must declare its content size in the zstd frame header. That size must be within `maxMessageBytes`, and decoding stops at the cap.
- Local sockets never select compression. JSON text frames cannot carry it.
- DATA is never compressed by the protocol.
- permessage-deflate is rejected for three reasons: it keeps per-connection inflate state, compresses tiny delta frames for nothing, and mixes secrets with attacker-influenced text in one compression context.

> **Changed in #53.** Two receiver rules are stricter than the table above: a frame whose reserved bytes (6–7) are not zero is a protocol error, like an unknown flag, because both could change how the body is read; and a frame of an unknown `kind` is still skipped by length, but only up to `maxMessageBytes` (otherwise `frameTooLarge`), so a peer cannot make the receiver read gigabytes it throws away. A `ZSTD` MSG frame is a protocol error until compression is implemented; `welcome.compression` is always absent for now.

**Writer priority:**
- each connection's writer drains three queues in order: control (`pong`, `goodbye`), messages (replies and stream items, kept in their own order), then DATA;
- it never writes more than one DATA chunk between checks of the higher queues, so a 64 MiB download cannot delay a reply by more than one chunk.

## 2. Connection lifecycle

### 2.1 Handshake

```rust
// client → daemon, first message
struct Hello {
    protocol: ProtocolVersion,        // { major: 8, minor: u16 }
    client: ClientInfo,               // { kind, appVersion, os, deviceLabel?, instanceId: Uuid }
    credential: Credential,
    compression: Vec<Compression>,    // offered for the daemon → client direction
    features: Vec<String>,            // client feature flags
    maxMessageBytes: Option<u32>,     // receive cap; the daemon uses min(own, client)
}
enum ClientKind { desktop, web, ios, android, cli, bench, Unknown(String) }
enum Credential {
    local { token: String },                      // local socket only
    session { token: String },                    // issued by pairing
    pairing { secret: String, label: String },    // one-time; Welcome returns a session token
}

// daemon → client
struct Welcome {
    protocol: ProtocolVersion,
    daemonVersion: String,
    environment: EnvironmentInfo,     // { id: Uuid, name, hostOs, hostArch, storeId: Uuid }
    session: SessionInfo,             // { id: Uuid, scopes: Vec<Scope>, expiresAt: Option<i64> }
    issuedCredential: Option<IssuedCredential>,   // { token, expiresAt }, pairing only
    limits: Limits,                   // maxMessageBytes, maxDataChunkBytes, maxBlobBytes,
                                      // pingIntervalMs, pingTimeoutMs, maxInFlightCalls, maxSubscriptions
    compression: Option<Compression>,
    features: Vec<String>,            // daemon feature flags, e.g. "boundedSnapshot", "compactTurnItems"
}
struct Rejected { code: RejectCode, message: String, retryAfterMs: Option<u64> }
enum RejectCode { versionUnsupported, unauthenticated, expired, revoked, rateLimited,
                  originNotAllowed, overloaded, Unknown(String) }
```

- The client may **pipeline** calls and subscriptions right after `Hello`, without waiting for `Welcome`. If the daemon answers `rejected`, everything after `Hello` is discarded. This saves one round trip on remote links.
- `Hello` must arrive within 10 seconds of the connection opening, or the daemon closes the connection.
- The draft's `Hello.resume` list is replaced by per-subscription cursors ([§2.6](#26-reconnect-and-resume)).

### 2.2 Frame catalogue

```rust
enum ClientMessage {                                    // tag "type"
    hello(Hello),
    call { id: u32, method: Method },                   // "method" and "params" keys, see §5
    cancel { id: u32 },                                 // best effort; the reply becomes `cancelled` if it wins
    subscribe { channel: u32, stream: StreamSpec, after: Option<StreamCursor>, window: Option<u32> },
    unsubscribe { channel: u32 },
    credit { channel: u32, bytes: u32 },                // DATA flow control (§2.4)
    ping { nonce: u64 },
    pong { nonce: u64 },
    goodbye { code: GoodbyeCode, message: String },
    Unknown,
}

enum ServerMessage {
    welcome(Welcome),
    rejected(Rejected),
    reply { id: u32, ok: Option<MethodResult>, error: Option<Error> },   // exactly one is present
    items { channel: u32, items: Vec<StreamItem> },     // 1..n items, in stream order
    end { channel: u32, reason: EndReason, resumeAfter: Option<StreamCursor> },
    notice(Notice),
    ping { nonce: u64 },
    pong { nonce: u64 },
    goodbye { code: GoodbyeCode, message: String, reconnect: bool },
    Unknown,
}

enum EndReason { unsubscribed, completed, overflow, notFound, forbidden,
                 error { error: Error }, superseded, shuttingDown, Unknown }
enum GoodbyeCode { shuttingDown, revoked, expired, protocolError, frameTooLarge, idleTimeout,
                   writeStalled, overloaded, Unknown(String) }
enum Notice { updateAvailable { version: String }, credentialExpiring { at: i64 }, Unknown }
```

**Wire shapes:**

```json
{"type":"call","id":7,"method":"threads.turnItem","params":{"threadId":"…","itemId":"…"}}
{"type":"reply","id":7,"ok":{"type":"commandExecution","id":"…", …}}
{"type":"reply","id":8,"error":{"code":"forbidden","message":"…","details":{"type":"missingScope","required":"vcs:write"}}}
{"type":"items","channel":3,"items":[{"type":"event","seq":1042,"event":{…}}]}
```

- `notice` nests its open union under a `notice` key (`{"type":"notice","notice":{"type":"updateAvailable",…}}`), because both levels are tagged by `type`.
- `reply.ok` may be `null` for a method whose result is an `Option`. A reply whose `id` is not pending decodes with its `ok` skipped.
- `reply.ok` is the method's result value as-is, with no extra wrapper. The receiver decodes it according to the method of the pending call with that `id`.
- The `methods!` macro generates `Method`, `MethodResult` and their serde impls:
  - the daemon decodes `params` according to `method`, in any key order;
  - clients get a decoder for `ok` keyed by method name.

### 2.3 Calls

- **Call ids** are `u32`, chosen by the client, and unique among its in-flight calls on that connection. Replies may arrive in any order.
- **In-flight cap.** At most `maxInFlightCalls` (64) calls per connection; further calls get `busy`.
- **Malformed params.** A `call` whose `params` do not decode for its `method` is answered `badRequest`; only an undecodable envelope closes the connection with `protocolError`.
- **Cancellation.** `cancel{id}` is best effort. A daemon that has already finished sends the normal reply.
- **Idempotency.**
  - `orchestration.dispatch` is idempotent by `commandId`, through durable command receipts (RFC 0001).
  - Every other mutating method carries `opId: Uuid`. The daemon keeps receipts per credential session (2,048 entries, 64 MiB, 10 minutes; these are the v7 response-cache bounds). Re-sending a known `opId` returns the stored result without running the method again.
  - Read methods are not deduplicated; they are simply retried.
- **Read-your-writes.** `orchestration.dispatch` replies `{sequence}`. On the same connection, every event with `seq <= sequence` for a subscribed thread or shell is queued before the reply. Other clients reach the same state when their stream cursor passes `sequence`.

### 2.4 Streams

- **Channels.** A subscription is identified by a client-chosen `channel: u32`. Subscriptions and upload transfers share one channel space per connection. Channel `0` is reserved.
- **Reuse.** A channel id can be reused only after the daemon has sent `end` for it.
- **Order.** Items of one stream arrive in order. There is no ordering across streams, except the dispatch guarantee in [§2.3](#23-calls).
- **Stream start.** Every stream starts with either a snapshot or the replayed gap, followed by `synchronized`. That separates "transport up" from "data fresh". Failures arrive as `end{error|notFound|forbidden}`; there is no separate subscribe acknowledgement.
- **Live budget** (MSG item streams): at most 1,000 queued items or 8 MiB of encoded items per subscription.
  - On overflow the daemon drops that subscription's queue and sends `end{overflow, resumeAfter: <last cursor delivered>}`.
  - The client resubscribes with that cursor. Other streams on the connection are unaffected.
  - Before reading the replay high-water mark, the daemon subscribes to the live bus, so nothing is lost in between (T3 `LiveStreamBudget`).
- **Credit** (DATA streams: terminal output, blob downloads):
  - `subscribe.window` sets the initial credit in bytes (default 1 MiB).
  - The daemon never sends more DATA bytes than the credit granted. The client returns credit with `credit{channel, bytes}` as it consumes.
  - A terminal subscriber that runs out of credit stops receiving output. The PTY keeps draining into the retained tail. When credit returns, the subscriber continues from its offset if that offset is still retained, otherwise it receives `reset` and the tail.
- **Connection budget.** Each connection's writer queue is capped at 32 MiB. If the socket makes no write progress for 10 seconds (`writeStalled`), the daemon closes the connection, and the client reconnects and resumes.
- **Encode once.** Each committed event is wire-projected once and encoded at most once per encoding into shared `Bytes`. Each subscriber only adds the small `items` envelope around those pre-encoded bytes. This removes v7's per-subscriber deep clones and per-connection re-serialization.

### 2.5 Keepalive

- Each side sends `ping{nonce}` when it has sent nothing for `pingIntervalMs` (5,000). The peer answers `pong` from the control queue.
- If nothing at all arrives for `pingTimeoutMs` (15,000), the connection is considered dead and is closed.
- The same values apply to local sockets. Daemon death shows up as EOF immediately there; the ping covers a hung daemon.
- Keepalive is the only periodic timer. It is re-armed by traffic, so an idle connection costs 0.2 wakeups per second.

### 2.6 Reconnect and resume

**Backoff:**
- the first retry is immediate;
- after that, 250 ms doubling to a 10 s cap, with ±20% jitter;
- a network-change or app-foreground signal resets the backoff;
- for a local daemon, after 3 failed connects the existing `DaemonSupervisor` checks the process and relaunches it.

**After reconnect:**
1. The client sends `Hello` with the same `client.instanceId` and credential.
2. It re-subscribes every open stream, passing its last *applied* cursor as `after`.
3. It re-issues in-flight calls: reads as they were, mutations with the same `commandId` or `opId`.
4. It restarts uploads that were in progress. Re-sending is cheap because of content deduplication ([§5.6](#56-blob-transfer)).

Resume rules per stream are in [§4](#4-cursors). Replies are never replayed across connections; the idempotency keys make re-issuing safe.

### 2.7 Concurrency (no polling)

**Daemon:**
- `tokio` multi-thread runtime for the transport, with an evented accept loop;
- per connection, one reader task (blocking reads, no timeout) and one writer task waiting on its priority queues;
- calls run on a worker pool, and git, filesystem and SQLite work goes through `spawn_blocking` with a global cap;
- `orchestration.dispatch` serializes per thread under RFC 0001's thread lock. Unrelated threads and read methods run in parallel, which replaces v7's per-session mailbox;
- terminal operations are ordered per terminal.

**Rust client (`waku-client`):**
- one connection thread running a current-thread tokio runtime, with a reader task blocked on the socket and a writer task blocked on the outgoing queue; keepalive is a `timeout(pingInterval)` around the writer's wait;
- wakes GPUI through the existing smol channel;
- deliberately runtime-agnostic: the runtime is private to the connection thread, and callers block on crossbeam channels.

> **Changed in #18 (v7 transport).** The draft above called for a blocking reader thread and writer thread. A blocking `tungstenite` socket cannot be split safely: its reader answers pings by writing on the same stream, and a `wss://` (rustls) stream cannot be cloned at all. A private current-thread runtime with `tokio-tungstenite` gives the same shape — reads and writes never wait on each other, nothing polls — on one thread, and the same code serves TCP, TLS, the Unix socket and the named pipe (#19). The daemon side uses the multi-thread runtime described above, with backend calls on its blocking pool.

**Timers** exist only for keepalive, coalescing windows and backoff, and each is armed only while it has pending work.

## 3. Error model

```rust
struct Error {
    code: ErrorCode,
    message: String,                 // English, for logs; clients localize by code
    retryable: bool,
    retryAfterMs: Option<u64>,
    details: Option<ErrorDetails>,
    traceId: Option<String>,
}
enum ErrorCode {                     // open string enum
    badRequest, unknownMethod, unauthenticated, forbidden, notFound, conflict, alreadyExists,
    unsupported, notReady, busy, rateLimited, payloadTooLarge, cancelled, timeout,
    provider, vcs, io, internal, Unknown(String),
}
enum ErrorDetails {                  // open union
    missingScope { required: Scope },
    validation { field: String, reason: String },
    notFound { resource: String, id: String },
    revisionConflict { current: String },                       // files.write, settings.update, drafts.apply
    capability { capability: String },                          // RFC 0002 Unsupported
    provider { class: ProviderFailureClass, retryable: bool, resetAt: Option<i64>, code: Option<String> },
    vcs { kind: VcsFailureKind },     // notARepository, dirtyWorktree, mergeConflict, authRequired,
                                      // nonFastForward, refNotFound, Unknown(String)
    limit { limit: u64 },             // payloadTooLarge, busy
    Unknown,
}
```

- `ProviderFailureClass` is RFC 0002's set: `usageLimit`, `providerError`, `transportError`, `permissionError`, `validationError`, `unknown`.
- Provider failures inside a run are not errors of the dispatch call. They arrive as `RunUpdated` with a failure.
- Connection-level failures use `rejected` (during the handshake) or `goodbye` (after it), never `Error`.

## 4. Cursors

```rust
enum StreamCursor {
    sequence { store: Uuid, seq: u64 },          // shell, thread
    revision { revision: u64 },                  // settings, providers, access, drafts
    offset { generation: Uuid, offset: u64 },    // terminal (byte offset), blob download
    Unknown,
}
type HistoryCursor = String;                     // opaque, at most 4,096 bytes
```

- **`store`** identifies the event store (RFC 0001's `state.sqlite`, random at creation). It replaces v7's per-process `epoch`. If it does not match, the client gets a fresh snapshot.
- **`seq`** is RFC 0001's global event sequence. Shell and thread streams share it. Lookups use the per-thread sequence index and never scan history.

**Resume rules:**

| Stream | `after` given and valid | Otherwise |
|---|---|---|
| `shell` | Replay deltas `(seq, high-water]` when the gap is at most 4,096 events | Snapshot |
| `thread` | Replay events when the thread's gap is at most 128 events **and** at most 1 MiB encoded, with a 1 MiB raw-payload pre-check | Bounded snapshot |
| `terminal` | Bytes from `offset` if still retained and `generation` matches | `reset` + retained tail |
| `blobDownload` | Bytes from `offset` | From 0 |
| `settings`, `providers`, `access`, `drafts` | `synchronized` only, if `revision` is current | Snapshot (these are small) |
| `commandOutput`, `vcsStatus`, `terminalList` | No cursor | Snapshot (`replace`) |

- Applying an event is an upsert by entity id (RFC 0001 rule 4). Events with `seq` at or below the client's applied cursor are therefore harmless no-ops.
- Unknown items still advance the cursor.

## 5. Message catalogue

### 5.1 Common types

```rust
type ThreadId = Uuid; type ProjectId = Uuid; type RunId = Uuid; type ItemId = Uuid;
type MessageId = Uuid; type RequestId = Uuid; type NodeId = Uuid; type TerminalId = Uuid;
type ProviderInstanceId = String;            // e.g. "codex", "claude-work"; config lives in settings
type BlobRef = String;                       // "waku-blob:<sha256>.<ext>" or "waku-attachment:…"
type RelPath = String;                       // relative to a root, "/" separators
struct JsonText(String);                     // opaque JSON, never parsed by the protocol layer

enum RootRef {                               // where a files/vcs/terminal call operates
    project { projectId },
    thread { threadId },                     // the thread's worktree, or its project root
    path { path: String },                   // absolute daemon-host path
    Unknown,
}
struct ProviderSelection {                   // replaces WireDriverStartOptions / WireSessionOptions
    instanceId: ProviderInstanceId,
    model: Option<String>, reasoningEffort: Option<String>, serviceTier: Option<String>,
    contextWindow: Option<String>, agentPreset: Option<String>, computerUse: bool,
}
enum RuntimeMode { approvalRequired, autoAcceptEdits, auto, fullAccess, Unknown(String) }
enum InteractionMode { default, plan, Unknown(String) }
struct MessageInput { text: String, attachments: Vec<BlobRef>, context: Vec<ContextRecord> }
enum Payload<T> { inline { value: T }, blob { reference: BlobRef, size: u64 }, Unknown }
```

Entity types for threads, runs, nodes, turn items and the rest are defined by RFC 0001 and RFC 0002. [§5.5](#55-domain-events-and-turn-items) fixes their wire envelope and variant names.

### 5.2 Methods

- Method names are `domain.verb` in camelCase.
- **Scope** is the scope the method requires ([§6.1](#61-scopes)). A `+` marks a payload-dependent addition ([§6.3](#63-payload-dependent-scopes)).
- **v7** names the command or operation the method replaces. "new" means it has no v7 counterpart.
- Methods marked *reserved* have their names fixed here and their shapes specified in their phase's issue. #53 implements everything else.

**Daemon and session**

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `daemon.info` | `{}` | `DaemonInfo{version, environment, features, uptimeMs, defaultCwd, projectlessRoot?}` | `read` | `ServerMessage::Hello.daemonVersion`, `TaskState.defaultCwd`, `TaskState.projectlessRoot` |
| `daemon.shutdown` | `{reason}` | `ack` | `admin`, and only on a desktop-owned daemon (`allowShutdown`) | `ClientMessage::Shutdown` |

**Orchestration and threads** (RFC 0001)

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `orchestration.dispatch` | `{commandId: Uuid, command: Command}` ([§5.3](#53-orchestration-commands)) | `DispatchResult{sequence, duplicate: bool, created: Option<CreatedIds>}` | per command | Prompt, Steer, Cancel, Respond, … |
| `threads.snapshot` | `{threadId, compactTurnItems: bool}` | `ThreadSnapshot` (the same bounded shape as the stream snapshot) | `read` | `HydrateSession` |
| `threads.historyPage` | `{threadId, cursor: HistoryCursor}` | `HistoryPage{items: Vec<TurnItem>, messages, next: Option<HistoryCursor>}`, at most 1 MiB | `read` | `HydrateSession` (paged) |
| `threads.turnItem` | `{threadId, itemId}` | `TurnItem`, unprojected; large bodies are `Payload` | `read` | new (T3 `getTurnItem`) |
| `threads.search` | `{query, limit ≤ 200, projectId?, includeArchived}` | `Vec<ThreadSearchHit{threadId, messageId, role, snippet}>` | `read` | `SearchSessionMessages` |
| `threads.find` | `{threadId, query, limit ≤ 50}` | `Vec<ThreadFindHit{itemId, ordinal, snippet}>` | `read` | new (T3 `searchThread`) |
| `threads.archived` | `{cursor?: HistoryCursor, limit ≤ 200}` | `{rows: Vec<ThreadRow>, next}` | `read` | new |
| `threads.turnDiff` | `{threadId, runId, ignoreWhitespace}` | `ReviewDiff` | `read` | `CollectReviewDiff{LastTurn}` |
| `threads.fullDiff` | `{threadId, ignoreWhitespace}` | `ReviewDiff` | `read` | new |
| `checkpoints.list` | `{threadId}` | `Vec<CheckpointInfo{runOrdinal, ref, capturedAt}>` | `read` | `SessionTurnRefs` |

**Projects**

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `projects.list` | `{}` | `Vec<Project>` | `read` | `LoadTaskState.projects` |

Project mutations go through dispatch ([§5.3](#53-orchestration-commands)).

**Files and filesystem**

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `files.tree` | `{root: RootRef, expanded: Vec<RelPath>}` | `Vec<TreeEntry{path, name, isDir, expanded, depth}>` | `files:read` | `ListTree` |
| `files.searchEntries` | `{root, query, limit ≤ 10,000}` | `Vec<FileEntry>` | `files:read` | `ListProjectFiles` |
| `files.searchContents` | `{root, query, regex, caseSensitive, limit ≤ 1,000}` | `Vec<ContentHit{path, line, column, preview}>` | `files:read` | new |
| `files.read` | `{root, path, maxBytes?}` | `FileContent{text: Option<String>, blob: Option<BlobRef>, binary, size, revision}` (text inline up to 2 MiB, otherwise `blob`) | `files:read` | `ReadTextFile` |
| `files.write` | `{root, path, content, baseRevision?, opId}` | `{revision}`, or `conflict` with `revisionConflict` | `files:write` | `WriteTextFile` |
| `fs.browse` | `{path?}` | `Directory{path, parent, home, filesystemRoot, entries}` | `files:read` | `BrowseDirectory` |

`revision` is a content hash. `files:read` covers any path the daemon user can read. Project roots are a convenience, not a security boundary.

**Composer, skills and drafts**

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `composer.slashCommands` | `{threadId?, instanceId?, root}` | `Vec<SlashCommand>` | `read` | `DiscoverSlashCommands` |
| `skills.list` | `{projectIds}` | `SkillsCatalog` | `read` | `LoadSkills` |
| `skills.setEnabled` | `{dirs, enabled, opId}` | `ack` | `settings:write` | `SetSkillsEnabled` |
| `skills.trash` | `{dirs, opId}` | `ack` | `settings:write` | `TrashSkills` |
| `drafts.get` | `{}` | `{revision, drafts: ComposerDrafts}` | `read` | `LoadComposerDrafts` |
| `drafts.apply` | `{baseRevision?, changes: Vec<ComposerDraftChange>, opId}` | `{revision}` | `operate` | `SaveComposerDrafts`, `ApplyComposerDraftChanges` |

**Blobs and attachments** ([§5.6](#56-blob-transfer))

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `blobs.put` | `{channel, size, mime, name?, sha256?}`, then DATA on `channel` | `{reference, size, sha256, deduplicated}` | `operate` | `StoreBlob` |
| `blobs.stat` | `{reference}` | `{size, mime, sha256, localPath?}` (`localPath` only on the local socket) | `read` | new |
| `attachments.import` | `{name, kind: file\|directory, parts: Vec<{relativePath, blob: BlobRef}>, opId}` | `StoredAttachment` | `operate` | `ImportAttachment` |
| `attachments.importPath` | `{path, opId}` | `StoredAttachment` | `operate` + `files:read` | `ImportPathAttachment` |

Downloads use the `blobDownload` stream, which replaces `ReadBlob` and `ReadAttachment`.

**Version control and review** (every method takes `root: RootRef`)

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `vcs.status` | `{root}` | `GitStatus` | `read` | `GitStatus` |
| `vcs.refs` | `{root}` | `Option<BranchSnapshot>` | `read` | `InspectBranches` |
| `vcs.switchRef` | `{root, name, create, opId}` | `BranchSnapshot` | `vcs:write` | `CheckoutBranch` |
| `vcs.worktrees` | `{root}` | `Vec<GitWorktree>` | `read` | `ListWorktrees` |
| `vcs.createWorktree` | `{projectId, baseRef?, branchHint, opId}` | `CreatedWorktree` | `vcs:write` | `CreateWorktree` |
| `vcs.removeWorktree` | `{path, force, opId}` | `ack` | `vcs:write` | new |
| `vcs.commitPreview` | `{root}` | `CommitSnapshot` | `read` | `InspectCommit` |
| `vcs.stage` | `{root, paths, stage, opId}` | `GitStatus` | `vcs:write` | `StageFiles` |
| `vcs.generateCommitMessage` | `{root, includeUnstaged, selection: ProviderSelection}` | `{message}` | `operate` | `GenerateCommitMessage` |
| `vcs.commit` | `{root, message, includeUnstaged, push, opId}` | `CommitResult{sha, pushed}` | `vcs:write` | `Commit` |
| `vcs.push` | `{root, opId}` | `ack` | `vcs:write` | `Push` |
| `vcs.pull` | `{root, opId}` | `PullResult{updated, conflicts}` | `vcs:write` | `Pull` |
| `review.diff` | `{root, source: ReviewDiffSource, ignoreWhitespace}` | `ReviewDiff{source, numstat, files: Vec<FileDiffSummary>, patch: Payload<String>, completeContext}` | `read` | `CollectReviewDiff` |
| `review.fileContents` | *reserved* (Phase 5) | | `read` | new |
| `pullRequests.*`, `sourceControl.*` | *reserved* (Phase 6) | | `read` / `vcs:write` | new |

**Terminal**

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `terminal.open` | `{root, cols, rows, title?, opId}` | `{terminalId, generation}` | `terminal` | `OpenTerminal` |
| `terminal.resize` | `{terminalId, cols, rows}` | `ack` | `terminal` | `ResizeTerminal` |
| `terminal.clear` | `{terminalId}` | `ack` | `terminal` | new |
| `terminal.restart` | `{terminalId}` | `{generation}` | `terminal` | new |
| `terminal.close` | `{terminalId}` | `ack` | `terminal` | `CloseTerminal` |

Input is sent as DATA frames on an `interactive` terminal subscription's channel, replacing `WriteTerminal`. Terminals are no longer tied to a session or runtime id.

**Providers and usage**

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `providers.list` | `{}` | `Vec<ProviderStatus>` | `read` | new |
| `providers.probe` | `{instanceId, discoverModels, probeVersion}` | `{probe: ProviderProbe, version?}` | `read` | `ProbeProvider` |
| `providers.planUsage` | `{instanceId}` | `Option<PlanUsage>` | `read` | `FetchPlanUsage` |
| `providers.models` | *reserved* (Phase 3) | | `read` | new |
| `providers.authStart`, `.authRespond`, `.authCancel`, `.logout` | *reserved* (Phase 7) | | `providers:manage` | new |
| `providerSessions.list` | `{instanceId, limit ≤ 500}` | `Vec<ProviderSessionSummary>` | `read` | `ListProviderSessions` |
| `providerSessions.preview` | `{instanceId, nativeRef: ProviderResumeCursor, cwd}` | `ProviderSessionHistory` | `read` | `LoadProviderSession` |
| `usage.history` | `{window: UsageWindow, projectIds}` | `UsageHistory` | `read` | `LoadUsageHistory` |

`binaryOverride` leaves the request. The binary belongs to the provider instance in settings, and changing it needs `providers:manage`.

**Settings**

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `settings.get` | `{}` | `{revision, settings: DaemonSettings}` with secrets redacted | `read` | `GetSettings` |
| `settings.update` | `{baseRevision?, patch: SettingsPatch, opId}` | `{revision, settings}` | `settings:write` + | `UpdateSettings` (whole object → patch) |
| `settings.upsertKeybinding`, `.removeKeybinding` | *reserved* (Phase 5) | | `settings:write` | new |

**Computer use** (Waku extension)

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `computerUse.permissions` | `{prompt}` | `ComputerPermissions` | `read`; `prompt: true` needs `operate` and the local socket | `ProbeComputerPermissions` |

**Access**

| Method | Request | Response | Scope | v7 |
|---|---|---|---|---|
| `access.createPairing` | `{scopes, label, ttlSecs ≤ 900}` | `PairingLink{id, url, qrPayload, expiresAt}` | `admin` | new |
| `access.listPairings` | `{}` | `Vec<PairingInfo>` (no secrets) | `admin` | new |
| `access.revokePairing` | `{id}` | `ack` | `admin` | new |
| `access.listClients` | `{}` | `Vec<ClientSession{id, label, kind, os, scopes, createdAt, lastSeenAt, connected}>` | `admin` | new |
| `access.revokeClient` | `{sessionId}` | `ack` (`forbidden` for the caller's own session) | `admin` | new |
| `access.revokeOtherClients` | `{}` | `{revoked: u32}` | `admin` | new |
| `access.rotateCertificate` | `{}` | `{fingerprint}` (existing pins break; remote clients must re-pair) | `admin` | new |

**Reserved domains:** `scheduledTasks.*` (Phase 4), `mcpApps.*` (Phase 5), `preview.*` and `device.*` (Phase 9).

### 5.3 Orchestration commands

`Command` is an open union carried by `orchestration.dispatch`.
- **Command ids.** Clients set `commandId`. Retries reuse it, and ids derived from stable inputs make retries idempotent (RFC 0001 rule 2).
- **Entity ids.** The client may supply ids for new entities (`threadId`, `messageId`, `runId`) so that it can render optimistic rows. This replaces v7's `Prompt{turnId, messageId}`.
- **Validation.** The daemon validates and rejects with `conflict`, `unsupported` or `badRequest`.

| Command | Fields | Scope | v7 |
|---|---|---|---|
| `threadCreate` | `threadId, projectId, title?, selection: ProviderSelection, runtimeMode, interactionMode, workspace: root \| existingWorktree{path} \| newWorktree{baseRef?}, initialMessage?: MessageInput` | `operate` + | `Start` (with `CreateWorktree`) |
| `threadDelete` | `threadId` | `operate` | `RemoveSession` |
| `threadArchive`, `threadUnarchive` | `threadId` | `operate` | new |
| `threadPin`, `threadUnpin`, `threadPinReorder` | `threadId`, `orderKey?` | `operate` | new |
| `threadVisit`, `threadMarkUnread` | `threadId` | `read` | new |
| `threadMetadataUpdate` | `threadId, title?` | `operate` | `SaveTaskState` (title edits) |
| `threadSelectionSet` | `threadId, selection` | `operate` | `ApplyOptions` |
| `threadRuntimeModeSet` | `threadId, runtimeMode` | `operate` | `ApplyOptions{mode}` |
| `threadInteractionModeSet` | `threadId, interactionMode` | `operate` | new |
| `threadGoal` | `threadId, operation: GoalOperation` | `operate` | `Goal` |
| `threadFork` | `sourceThreadId, newThreadId, throughRunOrdinal?` | `operate` | `Fork`, `ForkSessionFromResponse` |
| `threadImport` | `newThreadId, projectId, instanceId, nativeRef: ProviderResumeCursor, forkThroughTurn?` | `operate` | `ForkProviderSession` and adopting a `LoadProviderSession` result |
| `messageDispatch` | `threadId, messageId, runId, input: MessageInput, mode: startImmediately \| queueAfterActive \| steerActive \| restartActive` | `operate` | `Prompt`, `Steer` |
| `runInterrupt` | `threadId, runId?` | `operate` | `Cancel` |
| `runtimeRequestRespond` | `threadId, requestId, response: approval{optionId} \| userInput{answers: Vec<UserInputAnswer>} \| dynamicTool{approve, reason?}` | `operate` | `Respond`, `RespondUserInput`, `RunComputerTool`, `RejectComputerTool` |
| `subagentStop` | `threadId, nodeId` | `operate` | `StopBackgroundWork` |
| `backgroundWorkRefresh` | `threadId` | `operate` | `RefreshBackgroundWork` |
| `computerUseCancel` | `threadId` | `operate` | `CancelComputerUse` |
| `checkpointRollback` | `threadId, toRunOrdinal, restoreFiles: bool` | `operate` + | `Rollback`, `RewindSessionToMessage` |
| `providerSessionDetach` | `threadId` | `operate` | `CloseSession` |
| `projectCreate` | `projectId, path, name?` | `operate` + `files:read` | `SaveTaskState` (new project) |
| `projectUpdate` | `projectId, name?, orderKey?` | `operate` | `SaveTaskState` (project edits) |
| `projectRemove` | `projectId` | `operate` | `SaveTaskState` (project removal) |
| `projectCreateScratch` | `projectId, prompt?` | `operate` | `CreateProjectlessWorkspace` |
| `projectPromoteScratch` | `projectId, path` | `operate` + `files:write` | `MigrateProjectlessWorkspace` |
| `queuedRunCancel`, `queuedRunReorder`, `queuedRunEdit`, `queuedMessagePromoteToSteer`, `queueResume` | *reserved* (Phase 3) | `operate` | new |
| `threadSettle`, `threadUnsettle`, `threadSnooze`, `threadUnsnooze`, `providerSwitch`, `threadMergeBack`, `delegatedTask*` | *reserved* (Phases 3–4) | `operate` | new |

Commands that are internal in T3 (`thread.stop`, `checkpoint.rollback.fail`, and so on) are never accepted from clients. They decode as `Unknown` and are rejected with `badRequest`.

### 5.4 Streams

`StreamSpec` is an open union. Every stream ends with `end`.

| Stream | Spec | Items | Cursor | Scope |
|---|---|---|---|---|
| `shell` | `{location: active \| archived, projectId?}` | `snapshot{cursor, projects: Vec<ProjectRow>, threads: Vec<ThreadRow>, complete}`, `projectUpdated{seq, row}`, `projectRemoved{seq, projectId}`, `threadUpdated{seq, row, location}`, `threadRemoved{seq, threadId}`, `synchronized{cursor}` | `sequence` | `read` |
| `thread` | `{threadId, compactTurnItems: bool}` | `snapshot{cursor, projection: ThreadProjection, history: Option<HistoryCursor>, hasMoreHistory, budgetExceeded}`, `event{seq, event: DomainEvent}`, `delta{itemId, channel: text \| reasoning, rev: u32, text}`, `computerUse{state: ComputerUseState}`, `synchronized{cursor}` | `sequence` | `read` |
| `commandOutput` | `{threadId, itemId}` (item or background node) | `replace{text, truncated, running}`, `append{text}`, `finished{exitCode?, truncated}` | none | `read` |
| `terminal` | `{terminalId, interactive: bool}` | `attached{generation, cols, rows, offset, title?}`, then output as DATA on the channel, `resized{cols, rows}`, `reset{offset}`, `exited{code?}`, `error{message}` | `offset` | `read`; `interactive` needs `terminal` |
| `terminalList` | `{root?}` | `snapshot{terminals: Vec<TerminalInfo>}`, `upserted{terminal}`, `removed{terminalId}` | none | `read` |
| `vcsStatus` | `{root}` | `status{status: GitStatus, at}`, coalesced to at most one per second and computed only while subscribed | none | `read` |
| `blobDownload` | `{reference, offset?}` | `meta{size, mime, sha256}`, then DATA with `FIN` on the last chunk, then `end{completed}` | `offset` | `read` |
| `settings` | `{}` | `snapshot{revision, settings}`, `updated{revision, settings}`, `synchronized{cursor}` | `revision` | `read` |
| `providers` | `{}` | `snapshot{revision, statuses}`, `statusUpdated{revision, status}`, `planUsageUpdated{instanceId, usage}` | `revision` | `read` |
| `drafts` | `{}` | `snapshot{revision, drafts}`, `changed{revision, changes}` | `revision` | `read` |
| `access` | `{}` | `snapshot{revision, pairings, clients}`, `pairingUpserted`, `pairingRemoved`, `clientUpserted`, `clientRemoved` (each with `revision`) | `revision` | `admin` |
| `providerAuth` | *reserved* (Phase 7) | `progress{stage, url?, userCode?}`, `completed`, `failed{error}` | none | `providers:manage` |
| `scheduledTasks` | *reserved* (Phase 4) | | `revision` | `read` |

> **Changed in #53.** A settings `revision` is a content hash (53-bit FNV-1a of the MessagePack encoding of `Settings`), not a counter: equal revisions mean equal settings even across daemon restarts, so `after` stays valid without persisted state. Revisions are compared for equality only. Settings changed through the legacy protocol are published to v8 subscribers as well.

**Stream rules:**
- **Bounded thread snapshot.** At most 10 user turns, 75 turn items and 1 MiB. Required control records, such as pending runtime requests, are always included. At least one row is always admitted. Older rows come from `threads.historyPage`.
- **Split snapshots.** A snapshot over 1 MiB (only possible for `shell`) is sent as several `snapshot` items. Only the last one has `complete: true`.
- **Wire projection** (#57). Command output, dynamic-tool results and inline file-change bodies are omitted from `TurnItem`s on the stream, with `outputOmitted` and `outputIndicatesFailure` set. Dynamic values over the limit become a summary of at most 160 characters. Images inside tool output become `BlobRef`s, never data URLs. Full bodies come from `threads.turnItem`.
- **Streaming text.**
  - `delta` items are live-only. They are neither sequenced nor persisted, and never replayed.
  - The orchestrator coalesces provider deltas in 8–16 ms windows. It commits a `turnItemUpdated` snapshot of a streaming item at item completion, on status changes, and otherwise at finished Markdown lines at most every 250 ms (RFC 0001 R12).
  - Each snapshot carries `streamRev`, the count of deltas folded into it.
  - A client applies a `delta` only when `delta.rev == streamRev + 1` and then increments `streamRev`. On a gap it ignores deltas until the next snapshot.
  - The client therefore never computes offsets in an encoding-specific unit.
- **Coalescing.** Running tool `turnItemUpdated` events are coalesced over 50 ms, with at most 512 pending.
- **Output hub** (#56). Command output and background-process output never become events.
  - `commandOutput` sends frames at most every 100 ms (500 ms after an overflow), with appends capped at 16 KiB.
  - The hub keeps 64 live tails and 16 finished tails.
  - Clients subscribe only while a row is expanded.
- **Terminal tail.** Each terminal retains 5,000 lines and 8 MiB. Eviction never splits a UTF-8 code point, and query/response escape sequences are stripped from retained history.
- **No polling.** `vcsStatus` and `providers` compute only while someone is subscribed (the "leased to the viewport" rule).

### 5.5 Domain events and turn items

`DomainEvent` is an open union with a common envelope:

```rust
struct DomainEvent {
    id: Uuid, threadId: ThreadId, runId: Option<RunId>, nodeId: Option<NodeId>,
    occurredAt: i64,
    body: DomainEventBody,          // nested, not flattened (§1.1)
}
enum DomainEventBody {
    threadUpdated { thread: AppThread },          // covers title, goal, selection, modes, archive, pin, visit
    threadDeleted {},
    runUpdated { run: Run },
    runAttemptUpdated { attempt: RunAttempt },
    nodeUpdated { node: ExecutionNode },
    subagentUpdated { subagent: Subagent },
    providerSessionUpdated { session: ProviderSession },   // includes capabilities and available commands
    providerThreadUpdated { thread: ProviderThread },
    providerTurnUpdated { turn: ProviderTurn },            // token and context usage
    runtimeRequestUpdated { request: RuntimeRequest },
    messageUpdated { message: Message },
    turnItemUpdated { item: TurnItem },                     // wire-projected
    planUpdated { plan: Plan },
    checkpointUpdated { checkpoint: Checkpoint },
    contextTransferUpdated { transfer: ContextTransfer },
    Unknown,
}
```

**`TurnItem` variants:**
- from T3's 24 variants: `notification`, `userMessage`, `assistantMessage`, `reasoning`, `proposedPlan`, `todoList`, `userInputRequest`, `fileChange`, `commandExecution`, `fileSearch`, `webSearch`, `approvalRequest`, `checkpoint`, `runInterruptRequest`, `runInterruptResult`, `systemNotice`, `error`, `compaction`, `handoff`, `fork`, `threadCreated`, `secretRequest`, `subagent`, `dynamicTool`;
- Waku additions, so v7 `ActivityKind`s map one-to-one: `fileRead`, `fileList`;
- `Unknown`.

**Common `TurnItem` fields:** `id`, `ordinal`, `parentItemId?`, `status`, `startedAt`, `updatedAt`, `streamRev`, the presentation hints (`toolSurface`, `toolIcon`, `toolSource`), and provider refs. The full field lists belong to #165, under the rules in [§1.1](#11-data-model-rules-enforced-by-the-schema-exporter).

### 5.6 Blob transfer

**Upload (`blobs.put`):**
1. The client picks a free channel and sends `call blobs.put{channel, size, mime, sha256?}`.
2. It immediately streams DATA frames of at most 256 KiB on that channel, setting `FIN` on the last one.
3. The daemon writes to a temporary file and verifies the size and, if given, the sha256. Then it moves the file into the content-addressed store and replies.

Details:
- **Deduplication.** If `sha256` is given and the blob already exists, the daemon replies `{deduplicated: true}` immediately. The client then stops and sends an empty DATA frame with `FIN`.
- **Early errors** (too large, quota). The reply arrives early, and the daemon discards DATA on that channel until `FIN`.
- **Disconnect.** An interrupted upload is discarded and restarted from zero.
- **Directory attachments.** One `blobs.put` per file, then one `attachments.import` with the manifest. This replaces `AttachmentUpload::Directory` with its base64 entries.

**Download:** subscribe to `blobDownload{reference, offset?}`. The stream sends `meta`, then DATA within the credit window, `FIN` on the last chunk, then `end{completed}`. Unsubscribing cancels it.

**Local fast path.** On the local socket, `blobs.stat` returns `localPath`, and the desktop reads the file directly (waku-perf §3.7).

## 6. Scopes and auth

### 6.1 Scopes

| Scope | Grants |
|---|---|
| `read` | All read methods and streams except `access`; `threadVisit` and `threadMarkUnread` |
| `operate` | Orchestration commands, drafts, uploads, attachment import, commit-message generation |
| `files:read` | `files.*` reads, `fs.browse`, `attachments.importPath` |
| `files:write` | `files.write`, file-restoring rollback, scratch promotion |
| `vcs:write` | Every `vcs.*` mutation, new-worktree threads |
| `terminal` | Opening and driving terminals. This is arbitrary code execution on the host. |
| `providers:manage` | Provider instances, binaries, auth flows |
| `settings:write` | `settings.update`, skills, keybindings |
| `diagnostics:read` | *reserved* (Phase 9) |
| `admin` | Everything, including `access.*` and `daemon.shutdown` |

**Presets for pairing:**
- **Viewer:** `read`.
- **Operator:** `read`, `operate`, `files:read`.
- **Full:** `admin`.

The local socket always receives `admin`.

### 6.2 Credentials and pairing

**Local socket.** Two checks apply:
- the peer-credential check ([§7.1](#71-local-socket-and-named-pipe)), which is the security boundary;
- a 256-bit local token in `run/local.token` (mode 0600). This is defense in depth.

The daemon removes `WAKU_DAEMON_TOKEN` from provider child environments, as it already does in v7.

**Pairing:**
- `access.createPairing` creates a single-use secret: 16 characters from `23456789ABCDEFGHJKLMNPQRSTUVWXYZ` (80 bits). The TTL is 5 minutes by default, 15 at most.
- The links are `waku://pair?v=8&host=<host:port>&fp=<base64url SHA-256 of the SPKI>#<secret>` for native clients (shown as a QR code) and `https://<host>/pair#<secret>` for web.
- The secret is always in the fragment, never in the query string.

**Exchange:**
1. The client connects with `credential: pairing{secret, label}`.
2. The daemon consumes the link atomically, with the scope check inside the same transaction, and returns `Welcome.issuedCredential`.
3. The client stores that token: in the OS keychain on native, under the `waku-fork` service from `identity.rs`; in IndexedDB on web.

**Session tokens:**
- 256-bit random values, stored as SHA-256 hashes in `auth_sessions` (RFC 0001);
- a 30-day TTL that slides on use;
- revocation closes live connections with `goodbye{revoked}`;
- credentials never appear in URLs or WebSocket query strings, only in `Hello`;
- MCP credentials are a separate audience (RFC 0001, Phase 4).

**Rate limits:**
- 10 failed `Hello`s per minute per remote address;
- 64 connections per daemon (the v7 cap);
- 8 connections per session.

> **Changed in #53 (interim).** Until pairing and session tokens land (Phase 7), the daemon accepts only `credential: local`, on both the local socket and `/v8`, checked against the daemon token in constant time; that is the same trust as the legacy `/v1` bearer token. `session` and `pairing` are answered `rejected{unauthenticated}`. Every accepted connection gets `admin`.

### 6.3 Payload-dependent scopes

- `threadCreate` with `newWorktree` adds `vcs:write`.
- `checkpointRollback` with `restoreFiles: true` adds `files:write`.
- `projectCreate` adds `files:read`; `projectPromoteScratch` adds `files:write`.
- `settings.update` adds `providers:manage` when the patch touches provider instances, binaries or environment variables.
- `computerUse.permissions{prompt: true}` needs `operate` and the local socket.
- `attachments.importPath` adds `files:read`.

The `methods!` macro declares the static scope for each method. The payload rules are one function per method, and they are unit-tested.

## 7. Transport

### 7.1 Local socket and named pipe

Names come from `crates/waku-protocol/src/identity.rs` (D5). Debug builds use the debug data directory and the `waku-fork-dev` prefix.

| Platform | Endpoint |
|---|---|
| macOS | `~/Library/Application Support/<DATA_DIRECTORY_NAME>/run/daemon-v8.sock`. If the path exceeds 100 bytes (`sun_path` is 104), use `$TMPDIR/waku-fork-<uid>/daemon-v8.sock`. |
| Linux | `$XDG_RUNTIME_DIR/waku-fork/daemon-v8.sock`, falling back to `~/.waku-fork/run/daemon-v8.sock` |
| Windows | `\\.\pipe\waku-fork-<user SID>-v8` |

- **Override.** `WAKU_DAEMON_SOCKET` overrides the path (tests, multiple daemons), and the `DaemonReady` stdout line gains a `socket` field.

> **Changed in #53.** v8 shares the legacy daemon socket (`daemon.sock`, #19) instead of opening `daemon-v8.sock`. The first four bytes tell the protocols apart: a v8 client opens with the `WAKU` preface, a legacy client with an HTTP `GET`. One socket keeps one lock, one permission check and one peer-credential check, and the `DaemonReady` line does not change. Endpoints still never mix: a connection is one protocol from its first byte. The socket can take the `-v8` name when the legacy protocol is removed.
- **Permissions (Unix).**
  - The `run` directory is mode 0700 and owned by the daemon's euid. The daemon refuses to start if it is not.
  - The daemon binds with umask `0177`, then sets 0600 explicitly.
  - A `run/daemon.lock` (`flock`) enforces a single instance. A leftover socket is unlinked only while holding that lock.
- **Peer check (Unix).** `getpeereid` (macOS) or `SO_PEERCRED` (Linux) must equal the daemon's euid. Otherwise the daemon closes the socket before reading a byte.
- **Windows.**
  - The pipe is created with `FILE_FLAG_FIRST_PIPE_INSTANCE` and `PIPE_REJECT_REMOTE_CLIENTS`.
  - Its DACL grants access only to the current user SID.
  - The client's token user SID, obtained via `GetNamedPipeClientProcessId` and `OpenProcessToken`, must match.

### 7.2 Remote: TCP and TLS

- **Off by default.** TCP listens only when remote access is enabled in settings (bind address and port).
- **Plain `ws://`** is accepted only on loopback binds (the web dev server, SSH tunnels). A non-loopback bind requires TLS.
- **TLS modes:**
  - `selfSignedPinned` (default): an ECDSA P-256 certificate generated on first enable and stored 0600 under the data directory. Native clients pin the SPKI SHA-256 from the pairing link with a custom rustls verifier, so expiry plays no part. Rotating it means re-pairing.
  - `custom`: a user-supplied certificate and key, for example from `tailscale cert` or ACME.
  - `external`: TLS is terminated by Tailscale Serve, a reverse proxy or an SSH tunnel, and the daemon listens on loopback.
- **Browsers.** A browser cannot pin a self-signed certificate. Remote web therefore uses `custom`, `external`, or an SSH tunnel to `localhost`, which is a secure context.
- **Library.** TLS uses rustls, with TLS 1.3 only.

### 7.3 Web origin

- Browser upgrades carry `Origin`, which must be in the settings allowlist. The current `x-waku-client: native` header rule stays for non-browser clients that send an `Origin`.
- Authentication happens in `Hello`, never through cookies, so an allowed origin cannot ride an ambient credential.

## 8. v7 → v8 mapping

Every v7 message, command, operation, response, event and constant appears below, so nothing is lost.

### 8.1 Envelope, handshake and constants

| v7 | v8 |
|---|---|
| `ClientMessage::Hello{protocolVersion, token, clientId, resumeFrom}` | `hello{protocol, client.instanceId, credential, …}`; `resumeFrom` → per-stream `subscribe.after` |
| `ClientMessage::Request(Request)` | `call{id, method, params}` |
| `ClientMessage::Shutdown` | `daemon.shutdown` |
| `Request.requestId: Uuid` (nil = notification) | `call.id: u32`; idempotency by `commandId` or `opId`; no notifications (terminal input is DATA) |
| `Request.sessionId` | `threadId` inside the method params |
| `Request.runtimeId` | Removed. Runtimes are daemon-owned `ProviderSession`s. |
| `ServerMessage::Hello{protocolVersion, daemonVersion}` | `welcome` |
| `ServerMessage::Rejected{message}` | `rejected{code, message}` (handshake) or `end{forbidden}` / `goodbye` |
| `ServerMessage::Response{requestId, outcome}` | `reply{id, ok \| error}` |
| `ResponseOutcome::Ok/Error`, `RpcError{message}` | `reply.ok` / `reply.error: Error{code, details, …}` |
| `ServerMessage::Event(SequencedEvent)` | `items{channel, [event{seq, DomainEvent}]}` on a `thread` stream |
| `SequencedEvent{sessionId, runtimeId, epoch, sequence}` | `StreamCursor::sequence{store, seq}` |
| `ReplayCursor` | `StreamCursor` passed in `subscribe.after` |
| `ServerMessage::TaskStateChanged{revision}` | `shell` stream items (`threadUpdated`, `projectUpdated`, …) |
| `ServerMessage::ShuttingDown` | `goodbye{shuttingDown, reconnect: true}` |
| `WireDriverEvent{kind, payload: Value}` | Typed `DomainEvent` (no `Value`) |
| `WireDriverStartOptions` | `ProviderSelection` + `RuntimeMode` + `threadCreate.workspace`; `binary` → provider instance settings; `providerCursor` → daemon-owned `ProviderThread` |
| `WireSessionOptions` | `threadSelectionSet` / `threadRuntimeModeSet` |
| `WireComputerToolRequest{callId, tool, arguments: Value}` | `RuntimeRequest{kind: dynamicToolCall}`, with `arguments: JsonText` |
| `DaemonReady{address, protocolVersion, pid}` (stdout JSON line) | Kept for supervised launch, plus `socket` and `protocolMajor` |
| `PROTOCOL_VERSION` | `LEGACY_PROTOCOL_VERSION` (legacy) and `v8::PROTOCOL_MAJOR = 8` |
| `MAX_WIRE_MESSAGE_BYTES` (48 MiB) | `maxMessageBytes` 4 MiB + 256 KiB DATA chunks |
| `DAEMON_TOKEN_ENV`, `DAEMON_ADDRESS_ENV`, `APP_EXECUTABLE_ENV` | Kept; `WAKU_DAEMON_SOCKET` added |
| `x-waku-client: native` header, `/v1` path | Header rule kept; path `/v8` + subprotocol `waku.v8` |
| `decode_enum` / `encode_enum` / `event_to_wire` / `event_from_wire` (two copies, `driver_wire.rs` and `daemon.rs`) | Deleted. Typed serde end to end. |

### 8.2 Commands (all 50)

| v7 `Command` | v8 |
|---|---|
| `AttachSession` | Subscribe to the `thread` stream; `supportsSteer` → `ProviderSession.capabilities` |
| `Start{options}` | `dispatch threadCreate`, or implicit on the first `messageDispatch` (provider start is an effect) |
| `Prompt{prompt, turnId?, messageId?}` | `dispatch messageDispatch{mode: startImmediately \| queueAfterActive}` |
| `Steer{prompt}` | `dispatch messageDispatch{mode: steerActive}` (degrades per RFC 0002) |
| `Cancel` | `dispatch runInterrupt` |
| `CancelComputerUse` | `dispatch computerUseCancel` |
| `RefreshBackgroundWork` | `dispatch backgroundWorkRefresh` |
| `StopBackgroundWork{key: Value, controlId}` | `dispatch subagentStop{nodeId}` (typed id) |
| `Respond{requestId, optionId}` | `dispatch runtimeRequestRespond{approval{optionId}}` |
| `RespondUserInput{requestId, answers}` | `dispatch runtimeRequestRespond{userInput{answers}}` |
| `Goal{operation}` | `dispatch threadGoal` |
| `RunComputerTool{request}` | `dispatch runtimeRequestRespond{dynamicTool{approve: true}}` |
| `RejectComputerTool{request, reason}` | `dispatch runtimeRequestRespond{dynamicTool{approve: false, reason}}` |
| `ApplyOptions{options}` | `dispatch threadSelectionSet` / `threadRuntimeModeSet`; `OptionsApplied{applied}` → resulting `threadUpdated` |
| `Rollback{turns}` | `dispatch checkpointRollback{toRunOrdinal}` |
| `Fork{turnsToRemove}` | `dispatch threadFork{throughRunOrdinal}` |
| `GetSettings` | `settings.get` / `settings` stream |
| `UpdateSettings{settings}` | `settings.update{patch, baseRevision}` |
| `ProbeProvider{provider, binaryOverride, discoverModels, probeVersion}` | `providers.probe{instanceId, …}` |
| `FetchPlanUsage{provider, binaryOverride, cliVersion}` | `providers.planUsage{instanceId}` |
| `ProbeComputerPermissions{prompt}` | `computerUse.permissions{prompt}` |
| `LoadUsageHistory{window, projectRoots}` | `usage.history{window, projectIds}` |
| `LoadSkills{projects}` | `skills.list{projectIds}` |
| `SetSkillsEnabled{dirs, enabled}` | `skills.setEnabled` |
| `TrashSkills{dirs}` | `skills.trash` |
| `LoadTaskState` | `shell` stream snapshot + `projects.list` |
| `SaveTaskState{projects, liveSessionIds, sessions}` | **Removed.** The daemon owns state (RFC 0001); edits become `threadMetadataUpdate`, `projectCreate`, `projectUpdate`, `projectRemove` |
| `RemoveSession` | `dispatch threadDelete` |
| `HydrateSession{sessionId}` | `thread` stream bounded snapshot + `threads.historyPage` |
| `SearchSessionMessages{query, limit}` | `threads.search` |
| `ListProviderSessions{provider, limit}` | `providerSessions.list` |
| `LoadProviderSession{cursor, cwd}` | `providerSessions.preview` |
| `LoadComposerDrafts` | `drafts.get` / `drafts` stream |
| `SaveComposerDrafts{drafts, generation}` | `drafts.apply` (`generation` → `baseRevision`) |
| `ApplyComposerDraftChanges{changes}` | `drafts.apply` |
| `StoreBlob{mimeType, bytes}` | `blobs.put` + DATA |
| `ImportAttachment{name, upload}` | `blobs.put` per file + `attachments.import` |
| `ImportPathAttachment{path}` | `attachments.importPath` |
| `ReadBlob{reference}` | `blobDownload` stream (or `blobs.stat.localPath` locally) |
| `ReadAttachment{reference, path}` | `blobDownload` stream |
| `SweepBlobs` | Removed from the wire; daemon effect `attachment.cleanup` |
| `ForkSessionFromResponse{turnCount}` | `dispatch threadFork{throughRunOrdinal}` |
| `RewindSessionToMessage{turnCount}` | `dispatch checkpointRollback{restoreFiles: true}`, then `messageDispatch` of the edited text |
| `ForkProviderSession{request}` | `dispatch threadImport{forkThroughTurn}` (per-provider fork logic moves behind `DriverV2::fork`) |
| `Workspace{operation}` | Split into `files.*`, `fs.*`, `vcs.*`, `review.*`, `composer.*` and daemon-internal checkpoint effects (§8.3) |
| `OpenTerminal{cwd, cols, rows}` | `terminal.open{root, cols, rows}` + `terminal` stream |
| `WriteTerminal{data}` | DATA frames on the interactive `terminal` channel |
| `ResizeTerminal{cols, rows}` | `terminal.resize` |
| `CloseTerminal` | `terminal.close` |
| `CloseSession` | `dispatch providerSessionDetach` (the thread survives) |

### 8.3 Workspace operations (all 30)

| v7 `WorkspaceOperation` | v8 |
|---|---|
| `ListTree` | `files.tree` |
| `BrowseDirectory` | `fs.browse` |
| `ReadTextFile` | `files.read` |
| `WriteTextFile` | `files.write` |
| `ListProjectFiles` | `files.searchEntries` |
| `DiscoverSlashCommands` | `composer.slashCommands` |
| `CreateProjectlessWorkspace` | `dispatch projectCreateScratch` |
| `MigrateProjectlessWorkspace` | `dispatch projectPromoteScratch` |
| `InspectBranches` | `vcs.refs` |
| `CheckoutBranch` | `vcs.switchRef` |
| `CreateWorktree` | `vcs.createWorktree`, or `threadCreate{workspace: newWorktree}` |
| `InspectCommit` | `vcs.commitPreview` |
| `GitStatus` | `vcs.status` / `vcsStatus` stream |
| `StageFiles` | `vcs.stage` |
| `Pull` | `vcs.pull` |
| `ListWorktrees` | `vcs.worktrees` |
| `GenerateCommitMessage` | `vcs.generateCommitMessage` (`AgentInvocation` → `ProviderSelection`) |
| `Commit` | `vcs.commit` |
| `Push` | `vcs.push` |
| `CaptureTurnStart`, `CaptureTurn`, `CaptureRef` | Daemon-internal effect `checkpoint.capture` (RFC 0001) |
| `RestoreRef` | Daemon-internal, part of `checkpointRollback` |
| `HasRef` | Daemon-internal |
| `SessionTurnRefs` | `checkpoints.list` |
| `DeleteRef`, `DeleteTurnRefsAfter`, `DeleteSessionRefs` | Daemon-internal (rollback, thread deletion) |
| `CopySessionRefs` | Daemon-internal, part of `threadFork` |
| `CollectReviewDiff` | `review.diff`; `LastTurn` source → `threads.turnDiff` |

### 8.4 Responses

| v7 `ResponsePayload` | v8 |
|---|---|
| `Ack` | `ack` result of the relevant method |
| `SessionRuntime{runtimeId, supportsSteer}` | `thread` snapshot (`ProviderSession` + capabilities) |
| `Started{supportsSteer}` | `DispatchResult` + `providerSessionUpdated` event |
| `OptionsApplied{applied}` | `DispatchResult` + `threadUpdated` |
| `Cursor{cursor: Value}` | Removed (provider cursors are daemon-owned) |
| `Settings` | `settings.get` result |
| `ProviderProbe{probe, version}` | `providers.probe` result |
| `PlanUsage` | `providers.planUsage` result |
| `ComputerPermissions` | `computerUse.permissions` result |
| `UsageHistory` | `usage.history` result |
| `SkillsCatalog` | `skills.list` result |
| `TaskState{projects, sessions, defaultCwd, projectlessRoot}` | `shell` snapshot + `projects.list`; `defaultCwd` and `projectlessRoot` → `daemon.info` |
| `TaskStateSaved{sessions}` | Removed with `SaveTaskState` |
| `Session{session}` | `thread` snapshot |
| `SessionMessageMatches` | `threads.search` result |
| `ProviderSessions` | `providerSessions.list` result |
| `ProviderSessionHistory` | `providerSessions.preview` result |
| `ComposerDrafts` | `drafts.get` result |
| `BlobStored{reference, path}` | `blobs.put` result (path via `blobs.stat.localPath`) |
| `AttachmentStored` | `attachments.import` / `.importPath` result |
| `BlobData{bytes}` | `blobDownload` DATA frames |
| `ProviderSessionForked` | `DispatchResult` of `threadImport` + `threadUpdated` |
| `SessionForked{session, checkpointWarning}` | `DispatchResult` + events; the warning → `systemNotice` turn item |
| `SessionRewound{session, cleanupWarning}` | Same as above |
| `Workspace{result}` | Per-method results below |

| v7 `WorkspaceResult` | v8 |
|---|---|
| `Ack` | `ack` |
| `WorkingTree` | `files.tree` result |
| `Directory` | `fs.browse` result |
| `TextFile{content}` | `FileContent` (adds `revision`) |
| `ProjectFiles` | `files.searchEntries` result |
| `SlashCommands` | `composer.slashCommands` result |
| `ProjectlessWorkspace{cwd}` | `DispatchResult.created` + `projectUpdated` |
| `Branches` | `vcs.refs` result |
| `BranchChanged` | `vcs.switchRef` result |
| `WorktreeCreated` | `vcs.createWorktree` result |
| `CommitSnapshot` | `vcs.commitPreview` result |
| `GitStatus` | `vcs.status` / `vcs.stage` result |
| `Worktrees` | `vcs.worktrees` result |
| `CommitMessage` | `vcs.generateCommitMessage` result |
| `Checkpoint` | Daemon-internal; `checkpointUpdated` event |
| `Bool` | Daemon-internal (`HasRef`) |
| `TurnRefs` | `checkpoints.list` result |
| `ReviewDiff` | `review.diff` / `threads.turnDiff` result |

### 8.5 Driver and terminal events

| v7 event `kind` | v8 |
|---|---|
| `connected{providerCursor}` | `providerSessionUpdated{status: connected}` + `providerThreadUpdated` |
| `agentPresetSelected` | `threadUpdated` (selection's `agentPreset`) |
| `autoTitleUpdated` | `threadUpdated` (auto title; never overrides a user title) |
| `availableCommands` | `providerSessionUpdated.availableCommands` |
| `promptSubmitted{message, turnId, messageId}` | `messageUpdated` + `runUpdated{queued \| starting}` |
| `turnStarted` | `runUpdated{running}` |
| `turnParked` | `runUpdated{waiting}` with background work |
| `textDelta` | `thread` `delta{channel: text}` + periodic `turnItemUpdated{assistantMessage}` |
| `reasoningDelta` | `thread` `delta{channel: reasoning}` + `turnItemUpdated{reasoning}` |
| `activity{id, kind, title, detail, complete}` | `turnItemUpdated` with the matching variant (`commandExecution`, `fileChange`, `fileRead`, `fileSearch`, `fileList`, `webSearch`, `proposedPlan`/`todoList`, `dynamicTool`, `reasoning`) |
| `richActivity(ActivityItem)` | `turnItemUpdated` (wire-projected; `imageUrls` → `BlobRef`s) |
| `backgroundWork(Upsert/Reconcile*/StopRequested)` | `nodeUpdated` / `subagentUpdated` + shell row flags |
| `backgroundWork(OutputDelta)` | `commandOutput` stream for that node |
| `permission{requestId, title, detail, options}` | `runtimeRequestUpdated{kind: approval}` + `turnItemUpdated{approvalRequest}` |
| `userInputRequested{requestId, questions}` | `runtimeRequestUpdated{kind: userInput}` + `turnItemUpdated{userInputRequest}` |
| `computerUseUpdated{target, phase, visible, imageUrl}` | `thread` `computerUse{state}` (live-only; image as `BlobRef`) |
| `steerAccepted{message}` | `messageUpdated{inputIntent: steer, delivered}` |
| `steerRejected{message, reason}` | `messageUpdated{rejected, reason}` + `runAttemptUpdated` if degraded to restart |
| `usageUpdated{contextTokens, contextWindow}` | `providerTurnUpdated` (token and context usage) |
| `planUsageUpdated` | `providers` stream `planUsageUpdated` (account-level, not per thread) |
| `goalUpdated` | `threadUpdated` (goal) or `providerThreadUpdated.goal` |
| `turnFinished{success, summary}` | `runUpdated{completed \| failed}` from `TurnTerminal` (RFC 0002) |
| `error` | `runUpdated.failure`, or `turnItemUpdated{error}` outside a run |
| `processExited` | `providerSessionUpdated{status: exited}` |
| `terminalOutput{data: base64}` | `terminal` DATA frames (raw bytes) |
| `terminalExited` | `terminal` item `exited{code}` |
| `terminalError` | `terminal` item `error{message}` |
| `RuntimeEventCursorAdvanced` (client-only) | Removed; the client tracks its applied `StreamCursor` |

## 9. Codegen

**Source of truth.** Rust types in `crates/waku-protocol/src/v8/` carry `serde` and `schemars::JsonSchema` derives. The schemars derive sits behind a `schema` feature so daemon and desktop builds do not pay for it.

**Macros.** Three declarative macros produce the enums, their serde impls, the static scope table and a `PROTOCOL_MANIFEST`:

```rust
methods! {
    "threads.turnItem" => ThreadsTurnItem(TurnItemRequest) -> TurnItem, scope = Read;
    "vcs.commit"       => VcsCommit(CommitRequest)         -> CommitResult, scope = VcsWrite, op_id;
    …
}
streams!  { "thread" => Thread(ThreadSpec) -> ThreadItem, cursor = Sequence, scope = Read; … }
commands! { "messageDispatch" => MessageDispatch(MessageDispatchCommand), scope = Operate; … }
```

**Pipeline:**
1. `export_types` grows an `export_schema` mode. It writes `crates/waku-protocol/schema/v8.json`, which contains the manifest (methods, streams, commands, scopes, error codes, constants, limits) and a JSON Schema 2020-12 subset of every type. The file is committed, so a protocol change shows up as a reviewable diff.
2. The exporter lints [§1.1](#11-data-model-rules-enforced-by-the-schema-exporter). It fails on `untagged`, `flatten`, `HashMap`, `Value`, data-carrying enums without `Unknown`, and newtype-of-primitive variants.
3. A `waku-codegen` binary reads `v8.json` and emits:
   - **TypeScript** in `packages/waku-client/src/generated/v8/`:
     - `types.ts`, with discriminated unions that include `{type: "unknown"; rawType: string}` and optional fields as `field?: T`;
     - `methods.ts` and `streams.ts`, typed maps for `call<M>(method, params): Promise<Result<M>>` and `subscribe<S>(spec, handlers)`;
     - `constants.ts`;
     - `decode.ts`, which walks only the open unions to normalize unknown tags. MessagePack support uses `@msgpack/msgpack` behind the encoding flag.
   - **Swift** (when D2 work starts): `Codable` structs and enums, with custom `init(from:)` for open unions that maps unknown tags to `.unknown(String)`.
   - **Kotlin** (when D2 work starts): `kotlinx.serialization` sealed classes with `classDiscriminator = "type"` and a default polymorphic deserializer for `Unknown`.
   - Mobile starts on JSON. MessagePack is added through each platform's Codable or kotlinx format once measurements justify it.
4. `protocol:generate` and `protocol:check` cover v8 the same way they cover v7 today: regenerate into a temporary directory, then diff byte for byte.

> **Changed in #53.** The exporter, the lint and the TypeScript generator live in one crate, `crates/waku-codegen` (binary `waku-codegen`, `--check` for CI), instead of a mode of `export_types`, which stays the legacy ts-rs exporter. The generated TypeScript goes to `packages/waku-client/src/v8/generated/`, beside the hand-written v8 client in `src/v8/`, because `export_types` owns and rewrites `src/generated/`. The TypeScript MessagePack codec is our own (`src/v8/msgpack.ts`, about 300 lines) rather than `@msgpack/msgpack`: it writes the same bytes as `rmp-serde` for the same values (verified against the golden fixtures in both directions) and adds no dependency. The lint also rejects closed string enums: every string enum on the wire is an `open_string_enum!`. The `schema` feature only builds through `waku-codegen`, which is not a default workspace member.

**ts-rs stays only for legacy types** until legacy removal, behind a feature flag (PLAN §5.2 item 8). It is not used for v8 because:
- it emits TypeScript only, which rules it out under D2;
- it models `Option` as `T | null`;
- it does not know the open-union rule.

**Rejected alternatives:**
- **serde-reflection / serde-generate** target bincode-like formats and do not support internally tagged enums.
- **Typeshare** requires adjacently tagged enums and has no fallback variant or manifest.
- **Protobuf / prost** would mean a second model next to the serde types.

## 10. Migration

1. **#53, protocol crate.**
   - Move the current types to `waku_protocol::legacy`, re-exported at the old paths so nothing breaks.
   - Add `waku_protocol::v8`: the types in this RFC, the macros, the frame codec (preface, header, limits, zstd) and `export_schema`.
   - Rename the legacy constant.
   - Done in #53 (part 1) with one difference: `LEGACY_PROTOCOL_VERSION` is added as an alias of `PROTOCOL_VERSION` instead of renaming it, so in-flight work on the legacy protocol does not conflict; the old name goes with the `legacy` module (step 5). The legacy types stay where they are until then.
2. **Daemon serves both.** Legacy JSON stays on TCP `/v1`; v8 runs on the local socket and on `/v8`.
   - v8 methods that already have a v7 backend (settings, probe, files, vcs, skills, drafts, blobs, terminal, usage) start as adapters over the existing `Backend`.
   - Orchestration methods, the `shell` and `thread` streams and the output hub land with RFC 0001 (#165), #54 and #56.
3. **Desktop.** `waku-client` gets the v8 transport (blocking reader and writer threads, keepalive, resume). The desktop switches behind a setting, then by default.
4. **Web** (#144). The generated TS client replaces `packages/waku-client/src/client.ts`, and `apps/web` moves to it.
5. **Removal.** Remove the legacy endpoint, the `legacy` module, `SaveTaskState`, the duplicate `event_to_wire` in `daemon.rs`, and the ts-rs export.

**Existing data and other clients:**
- **Persisted state.** No persisted state uses the wire format. `waku-blob:` and `waku-attachment:` references stay valid. Composer drafts are migrated by the daemon.
- **Expo app.** `apps/mobile` stays on legacy and stops working against the fork's daemon at step 5. That is accepted under D2. Native apps arrive later via codegen.
- **Version skew.** A local daemon and the desktop ship together. A remote daemon with a different major answers `rejected{versionUnsupported}`, and the client asks the user to update one side.

## 11. Test plan

- **Golden encodings.**
  - A `golden` generator builds canonical sample values for every message variant (frames, every method's params and result, every stream item, `DomainEvent`, `TurnItem`, `Error`).
  - It writes `crates/waku-protocol/tests/v8/<name>.json` and `<name>.msgpack.hex` (plus raw frames as `<name>.hex` and an `index.json` with the routes needed to decode replies and items), from `tests/v8_golden.rs` with `WAKU_UPDATE_GOLDEN=1`.
  - Rust tests assert `encode(sample) == golden` byte for byte, and `decode(golden) == sample`.
  - Determinism comes from declaration-order fields, `BTreeMap`, omitted `None`s and minimal-width integers.
- **Forward compatibility.** Vectors with unknown variants, unknown fields, unknown methods, unknown frame kinds and unknown string-enum values. Each must decode to `Unknown` or be skipped without losing the frame, and stream cursors must still advance.
- **Cross-language round trip.**
  - TypeScript (vitest) loads every vector, decodes the JSON and MessagePack forms, re-encodes them and compares structurally with the golden value. Byte equality is required only from Rust.
  - Swift and Kotlin suites run the same vectors when they exist; the vectors are the contract.
  - CI runs the TS suite.
- **Framing.**
  - Partial reads and coalesced reads.
  - Every limit, including the oversize handshake.
  - Reserved bits set, and zstd bombs (missing or oversized content size).
  - MSG and DATA interleaving and writer priority.
  - Text/binary mixing on WebSocket.
  - `cargo fuzz` targets for the frame decoder and both message decoders: no panics, and allocation bounded by the limits.
- **Transport.**
  - Socket and directory modes, and refusal of a directory with the wrong owner.
  - Peer-credential rejection.
  - Stale socket cleanup under the lock.
  - Named-pipe DACL and SID check (Windows CI).
  - TLS pinning accept/reject with rustls.
  - Origin allowlist.
  - Pairing consumption is atomic and single-use.
  - Revocation closes live connections.
- **Streams and resume.**
  - Replay versus snapshot at the 128-event and 1 MiB thresholds.
  - `store` mismatch.
  - Overflow → `end{overflow}` → resubscribe without loss.
  - Terminal credit exhaustion → `reset`.
  - `delta` `streamRev` gaps.
  - Split shell snapshots.
  - Read-your-writes ordering of dispatch replies.
  - Forced disconnects during dispatch and `opId` calls, retried without double effects.
- **Keepalive and backoff** with a paused clock: ping cadence, a 15-second death, and jittered backoff bounds.
- **Performance** (`waku-bench-daemon`, PLAN §5.5): bytes per event, encode/decode time per event for both encodings, local RPC p50 under 1 ms, fewer than 5 idle wakeups per second, and fan-out to 8 subscribers with no per-subscriber re-encoding.
- **Migration.** Legacy `/v1` and v8 served side by side by one daemon. The existing `server.rs` integration tests port to v8 equivalents: sequencing, a late attach, supervisor reconnect, the terminal round trip, kicking a slow subscriber, and per-thread ordering.

## 12. Constants

| Constant | Value |
|---|---|
| `PROTOCOL_MAJOR` | 8 |
| Preface | `57 41 4B 55 00 08 00 00` |
| Frame header | 12 bytes |
| Handshake message cap | 64 KiB |
| `maxMessageBytes` | 4 MiB (after decompression) |
| `maxDataChunkBytes` | 256 KiB |
| `maxBlobBytes` | 64 MiB (attachments: 32 MiB per file) |
| Compression threshold | 16 KiB, zstd level 3 |
| `pingIntervalMs` / `pingTimeoutMs` | 5,000 / 15,000 |
| `Hello` deadline | 10 s |
| Write stall | 10 s |
| `maxInFlightCalls` / `maxSubscriptions` | 64 / 256 per connection |
| Connections | 64 per daemon, 8 per session |
| Live budget | 1,000 items / 8 MiB per subscription |
| Connection queue | 32 MiB |
| Replay budget (thread) | 128 events / 1 MiB |
| Replay budget (shell) | 4,096 events |
| Thread snapshot | 10 user turns / 75 items / 1 MiB |
| History cursor | ≤ 4,096 bytes |
| Text coalescing / streaming commit | 8–16 ms / ≤ 1 s |
| Tool update coalescing | 50 ms, ≤ 512 pending |
| Command output | 100 ms frames (500 ms after overflow), 16 KiB append cap, 64 live / 16 finished |
| Terminal retained tail | 5,000 lines and 8 MiB |
| Terminal initial credit | 1 MiB |
| Receipt cache | 2,048 entries / 64 MiB / 10 min |
| Pairing secret | 16 chars, 5 min TTL (≤ 15 min), single use |
| Session TTL | 30 days, sliding |
| Failed `Hello` limit | 10 per minute per remote address |
| Reconnect backoff | 0, then 250 ms doubling to 10 s, ±20% jitter |
