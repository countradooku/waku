// The protocol v8 client transport for web and other TypeScript clients
// (RFC 0003 §2): handshake, typed calls, stream subscriptions with
// kick-and-resume, DATA frames with credit, and keepalive.
//
// Web clients speak JSON text messages; bulk bytes arrive as binary DATA
// frames either way. MessagePack (binary MSG frames) is available for
// native-like clients and tests.

import { encodeMsgFrame, FrameError, parseFrame, type Frame } from "./frame";
import * as msgpack from "./msgpack";
import {
  HELLO_DEADLINE_MS,
  PROTOCOL_MAJOR,
  PROTOCOL_MINOR,
  WEBSOCKET_PATH,
  WEBSOCKET_SUBPROTOCOL,
} from "./generated/constants";
import { itemNormalizers, normalizeServerMessage, resultNormalizers } from "./generated/decode";
import type { MethodName, MethodParams, MethodResultOf } from "./generated/methods";
import { STREAMS, type StreamItemOf, type StreamName, type StreamSpecOf } from "./generated/streams";
import type * as T from "./generated/types";

export type Encoding = "json" | "msgpack";

/** The part of the WebSocket API the client uses. */
export interface V8Socket {
  binaryType: string;
  send(data: string | Uint8Array): void;
  close(code?: number, reason?: string): void;
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: ((event: unknown) => void) | null;
  onerror: ((event: unknown) => void) | null;
}

export type SocketFactory = (url: string, protocols: string[]) => V8Socket;

export interface V8ConnectOptions {
  /** `ws://host:port` or `wss://…`; the `/v8` path is added when missing. */
  url: string;
  credential: T.Credential;
  client: T.ClientInfo;
  /** `json` (the default, as browsers use) or `msgpack`. */
  encoding?: Encoding;
  features?: string[];
  createSocket?: SocketFactory;
  /** The handshake must finish within this. */
  connectTimeoutMs?: number;
}

/** The daemon refused the `hello`. */
export class RejectedError extends Error {
  constructor(readonly rejected: T.Rejected) {
    super(`daemon rejected the connection (${rejected.code}): ${rejected.message}`);
  }
}

/** A call's `reply.error`. */
export class CallError extends Error {
  constructor(readonly error: T.Error) {
    super(`${error.code}: ${error.message}`);
  }
}

/** The connection is gone; `code` is the `goodbye` code when there was one. */
export class ConnectionClosedError extends Error {
  constructor(
    message: string,
    readonly code?: string,
  ) {
    super(message);
  }
}

export interface StreamHandlers<S extends StreamName> {
  /** One item, already normalized (unknown tags become `{type: "unknown"}`). */
  item(item: StreamItemOf<S>): void;
  /** Bytes of a DATA stream; credit is returned once this returns. */
  data?(bytes: Uint8Array, fin: boolean): void;
  /** The subscription ended for good (an overflow resubscribes instead). */
  end?(end: T.End): void;
}

export interface SubscribeOptions {
  /** Resume after this cursor. */
  after?: T.StreamCursor;
  /** Initial DATA credit in bytes. */
  window?: number;
}

interface PendingCall {
  method: MethodName;
  resolve(result: unknown): void;
  reject(error: Error): void;
}

type Incoming = { kind: "message"; value: unknown } | Extract<Frame, { kind: "data" }>;

function v8Url(address: string): string {
  const url = new URL(address);
  if (url.pathname === "" || url.pathname === "/") url.pathname = WEBSOCKET_PATH;
  return url.toString();
}

function encodeMessage(encoding: Encoding, message: unknown): string | Uint8Array {
  return encoding === "json" ? JSON.stringify(message) : encodeMsgFrame(msgpack.encode(message));
}

function decodeIncoming(encoding: Encoding, data: unknown): Incoming | null {
  if (typeof data === "string") {
    if (encoding !== "json") throw new FrameError("text message on a MessagePack connection", "protocolError");
    return { kind: "message", value: JSON.parse(data) };
  }
  const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) : (data as Uint8Array);
  const frame = parseFrame(bytes);
  if (frame === null) return null;
  if (frame.kind === "data") return frame;
  if (encoding !== "msgpack") throw new FrameError("binary MSG frame on a JSON connection", "protocolError");
  return { kind: "message", value: msgpack.decode(frame.body) };
}

export class Subscription<S extends StreamName> {
  private currentChannel = 0;
  private currentCursor: T.StreamCursor | undefined;
  private ended = false;

  /** @internal */
  constructor(
    private readonly client: V8Client,
    readonly stream: S,
    private readonly spec: StreamSpecOf<S>,
    /** @internal */
    readonly handlers: StreamHandlers<S>,
    private readonly window: number | undefined,
  ) {}

  /** The channel of the current subscription (it changes on resume). */
  get channel(): number {
    return this.currentChannel;
  }

  /** The last cursor applied; resubscribing passes it as `after`. */
  get cursor(): T.StreamCursor | undefined {
    return this.currentCursor;
  }

  get isEnded(): boolean {
    return this.ended;
  }

  unsubscribe(): void {
    if (this.ended) return;
    this.ended = true;
    this.client.unsubscribeChannel(this.currentChannel);
  }

  /** @internal */
  open(after: T.StreamCursor | undefined): void {
    this.currentCursor = after ?? this.currentCursor;
    this.currentChannel = this.client.openChannel(this, {
      type: this.stream,
      ...(this.spec as object),
    } as T.StreamSpec, this.currentCursor, this.window);
  }

  /** @internal */
  deliver(raw: unknown): void {
    const item = itemNormalizers[this.stream](raw) as StreamItemOf<S>;
    const record = item as { type?: string; cursor?: T.StreamCursor; revision?: unknown; seq?: unknown };
    // Unknown items still advance nothing they do not carry, but they are
    // delivered so the caller can count them (RFC 0003 §4).
    const kind = STREAMS[this.stream].cursor;
    if ((record.type === "synchronized" || record.type === "snapshot") && record.cursor) {
      this.currentCursor = record.cursor;
    } else if (kind === "revision" && typeof record.revision === "number") {
      this.currentCursor = { type: "revision", revision: record.revision };
    } else if (kind === "sequence" && typeof record.seq === "number" && this.currentCursor?.type === "sequence") {
      // Shell and thread items carry the sequence of the store their
      // snapshot or `synchronized` named.
      this.currentCursor = { ...this.currentCursor, seq: record.seq };
    }
    this.handlers.item(item);
  }

  /** @internal */
  finish(end: T.End): void {
    if (end.reason.type === "overflow" && !this.ended) {
      // Kick-and-resume: only this subscription overflowed; pick up from the
      // last cursor the daemon delivered.
      this.open(end.resumeAfter ?? this.currentCursor);
      return;
    }
    this.ended = true;
    this.handlers.end?.(end);
  }
}

export class V8Client {
  private nextCallId = 1;
  private nextChannelId = 1;
  private nextNonce = 1;
  private readonly pending = new Map<number, PendingCall>();
  private readonly channels = new Map<number, Subscription<StreamName>>();
  private lastSent = Date.now();
  private lastReceived = Date.now();
  private timer: ReturnType<typeof setTimeout> | undefined;
  private closedWith: Error | undefined;
  private readonly closeListeners = new Set<(error: Error) => void>();
  /** Daemon notices (`updateAvailable`, …). */
  onNotice: ((notice: T.Notice) => void) | undefined;

  private constructor(
    private readonly socket: V8Socket,
    readonly encoding: Encoding,
    readonly welcome: T.Welcome,
  ) {
    socket.onmessage = (event) => this.receive(event.data);
    socket.onclose = () => this.fail(new ConnectionClosedError("the daemon connection closed"));
    socket.onerror = () => this.fail(new ConnectionClosedError("the daemon connection failed"));
    this.arm();
  }

  /** Connect, send `hello` and wait for `welcome`. */
  static connect(options: V8ConnectOptions): Promise<V8Client> {
    const encoding = options.encoding ?? "json";
    const createSocket = options.createSocket ?? ((url, protocols) => new WebSocket(url, protocols) as unknown as V8Socket);
    const socket = createSocket(v8Url(options.url), [WEBSOCKET_SUBPROTOCOL]);
    socket.binaryType = "arraybuffer";
    const hello: T.Hello = {
      protocol: { major: PROTOCOL_MAJOR, minor: PROTOCOL_MINOR },
      client: options.client,
      credential: options.credential,
      compression: [],
      features: options.features ?? [],
    };
    return new Promise((resolve, reject) => {
      let settled = false;
      const settle = (outcome: () => void) => {
        if (settled) return;
        settled = true;
        clearTimeout(deadline);
        outcome();
      };
      const deadline = setTimeout(() => {
        settle(() => reject(new ConnectionClosedError("the daemon handshake timed out")));
        socket.close();
      }, options.connectTimeoutMs ?? HELLO_DEADLINE_MS);
      socket.onopen = () => socket.send(encodeMessage(encoding, { type: "hello", ...hello }));
      socket.onmessage = (event) => {
        let incoming: Incoming | null;
        try {
          incoming = decodeIncoming(encoding, event.data);
        } catch (error) {
          settle(() => reject(error as Error));
          socket.close();
          return;
        }
        if (incoming?.kind !== "message") return;
        const message = normalizeServerMessage(incoming.value);
        if (message.type === "welcome") {
          const { type: _, ...welcome } = message;
          settle(() => resolve(new V8Client(socket, encoding, welcome as T.Welcome)));
        } else if (message.type === "rejected") {
          const { type: _, ...rejected } = message;
          settle(() => reject(new RejectedError(rejected as T.Rejected)));
          socket.close();
        }
      };
      socket.onerror = () => settle(() => reject(new ConnectionClosedError("could not connect to the daemon")));
      socket.onclose = () => settle(() => reject(new ConnectionClosedError("the daemon closed the connection during the handshake")));
    });
  }

  get isClosed(): boolean {
    return this.closedWith !== undefined;
  }

  /** Runs once when the connection ends, with the reason. */
  onClose(listener: (error: Error) => void): () => void {
    if (this.closedWith) {
      listener(this.closedWith);
      return () => {};
    }
    this.closeListeners.add(listener);
    return () => this.closeListeners.delete(listener);
  }

  /** Call `method`; rejects with a {@link CallError} for `reply.error`. */
  call<M extends MethodName>(method: M, params: MethodParams<M>, options: { signal?: AbortSignal } = {}): Promise<MethodResultOf<M>> {
    if (this.closedWith) return Promise.reject(this.closedWith);
    const id = this.nextCallId;
    this.nextCallId = this.nextCallId >= 0xffffffff ? 1 : this.nextCallId + 1;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { method, resolve: resolve as (result: unknown) => void, reject });
      this.send({ type: "call", id, method, params });
      options.signal?.addEventListener("abort", () => {
        if (this.pending.has(id)) this.send({ type: "cancel", id });
      });
    });
  }

  /** Subscribe to `stream`; items arrive in order on `handlers.item`. */
  subscribe<S extends StreamName>(
    stream: S,
    spec: StreamSpecOf<S>,
    handlers: StreamHandlers<S>,
    options: SubscribeOptions = {},
  ): Subscription<S> {
    const subscription = new Subscription(this, stream, spec, handlers, options.window);
    subscription.open(options.after);
    return subscription;
  }

  close(): void {
    if (this.closedWith) return;
    this.send({ type: "goodbye", code: "shuttingDown", message: "client closed" });
    this.socket.close(1000);
    this.fail(new ConnectionClosedError("closed by the client", "shuttingDown"));
  }

  /** @internal */
  openChannel(
    subscription: Subscription<StreamName>,
    stream: T.StreamSpec,
    after: T.StreamCursor | undefined,
    window: number | undefined,
  ): number {
    let channel = this.nextChannelId;
    while (this.channels.has(channel)) channel = channel >= 0xffffffff ? 1 : channel + 1;
    this.nextChannelId = channel >= 0xffffffff ? 1 : channel + 1;
    this.channels.set(channel, subscription);
    const message: Record<string, unknown> = { type: "subscribe", channel, stream };
    if (after) message.after = after;
    if (window !== undefined) message.window = window;
    this.send(message);
    return channel;
  }

  /** @internal */
  unsubscribeChannel(channel: number): void {
    // The channel stays reserved until the daemon's `end` arrives.
    this.send({ type: "unsubscribe", channel });
  }

  private send(message: unknown): void {
    if (this.closedWith) return;
    this.lastSent = Date.now();
    this.socket.send(encodeMessage(this.encoding, message));
  }

  /**
   * Keepalive (RFC 0003 §2.5): one timer, re-armed only when it fires, at
   * the earlier of "ping due" and "peer dead". Traffic just moves the
   * timestamps it reads.
   */
  private arm(): void {
    const { pingIntervalMs, pingTimeoutMs } = this.welcome.limits;
    const due = Math.min(this.lastSent + pingIntervalMs, this.lastReceived + pingTimeoutMs);
    this.timer = setTimeout(() => this.tick(), Math.max(0, due - Date.now()));
  }

  private tick(): void {
    if (this.closedWith) return;
    const { pingIntervalMs, pingTimeoutMs } = this.welcome.limits;
    const now = Date.now();
    if (now - this.lastReceived >= pingTimeoutMs) {
      this.socket.close();
      this.fail(new ConnectionClosedError(`nothing received from the daemon for ${pingTimeoutMs} ms`, "idleTimeout"));
      return;
    }
    if (now - this.lastSent >= pingIntervalMs) this.send({ type: "ping", nonce: this.nextNonce++ });
    this.arm();
  }

  private receive(data: unknown): void {
    this.lastReceived = Date.now();
    let incoming: Incoming | null;
    try {
      incoming = decodeIncoming(this.encoding, data);
    } catch (error) {
      const code = error instanceof FrameError ? error.code : "protocolError";
      this.send({ type: "goodbye", code, message: (error as Error).message });
      this.socket.close();
      this.fail(new ConnectionClosedError((error as Error).message, code));
      return;
    }
    if (incoming === null) return;
    if (incoming.kind === "data") {
      const subscription = this.channels.get(incoming.channel);
      if (!subscription) return;
      subscription.handlers.data?.(incoming.body, incoming.fin);
      if (incoming.body.length > 0) this.send({ type: "credit", channel: incoming.channel, bytes: incoming.body.length });
      return;
    }
    const message = normalizeServerMessage(incoming.value);
    switch (message.type) {
      case "reply": {
        const call = this.pending.get(message.id);
        if (!call) return;
        this.pending.delete(message.id);
        if (message.error) call.reject(new CallError(message.error));
        else call.resolve(resultNormalizers[call.method](message.ok));
        return;
      }
      case "items": {
        const subscription = this.channels.get(message.channel);
        if (!subscription) return;
        for (const item of message.items) subscription.deliver(item);
        return;
      }
      case "end": {
        const subscription = this.channels.get(message.channel);
        if (!subscription) return;
        this.channels.delete(message.channel);
        subscription.finish(message);
        return;
      }
      case "ping":
        this.send({ type: "pong", nonce: message.nonce });
        return;
      case "notice":
        this.onNotice?.(message.notice);
        return;
      case "goodbye":
        this.socket.close();
        this.fail(new ConnectionClosedError(message.message, message.code));
        return;
      default:
        return;
    }
  }

  private fail(error: Error): void {
    if (this.closedWith) return;
    this.closedWith = error;
    clearTimeout(this.timer);
    for (const call of this.pending.values()) call.reject(error);
    this.pending.clear();
    this.channels.clear();
    for (const listener of this.closeListeners) listener(error);
    this.closeListeners.clear();
  }
}
