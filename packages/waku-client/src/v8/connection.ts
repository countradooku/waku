// A daemon connection that survives disconnects (RFC 0003 §2.6): it
// reconnects with backoff, re-subscribes every open stream from its last
// applied cursor, and re-issues calls that are safe to repeat (reads, and
// mutations carrying their `opId` or `commandId`).
//
// `V8Client` is one WebSocket; `DaemonConnection` is what an app holds for
// its whole session with a daemon.

import {
  CallError,
  ConnectionClosedError,
  RejectedError,
  Subscription,
  V8Client,
  type StreamHandlers,
  type SubscribeOptions,
  type UploadOptions,
  type V8ConnectOptions,
} from "./client";
import { METHODS, type MethodName, type MethodParams, type MethodResultOf } from "./generated/methods";
import type { StreamName, StreamSpecOf } from "./generated/streams";
import type * as T from "./generated/types";

export type ConnectionPhase = "idle" | "connecting" | "connected" | "reconnecting" | "failed" | "closed";

export interface ConnectionState {
  phase: ConnectionPhase;
  /** Consecutive failed attempts since the last welcome. */
  attempt: number;
  /** Why the last attempt or connection failed. */
  error?: Error;
  /** When the next attempt is due (epoch ms), while reconnecting. */
  retryAt?: number;
  /** The daemon's welcome of the current (or last) connection. */
  welcome?: T.Welcome;
}

export interface DaemonConnectionOptions extends V8ConnectOptions {
  /** Delay of the second retry; the first retry is immediate. */
  initialDelayMs?: number;
  maxDelayMs?: number;
  /** ±fraction applied to every delay. */
  jitter?: number;
  /**
   * Keep retrying when the very first attempt fails (a daemon still
   * starting). Off by default: a first failure is reported as `failed`, so a
   * user who typed a wrong address sees it at once.
   */
  retryInitialFailure?: boolean;
  /** Test seams. */
  random?: () => number;
  connect?: (options: V8ConnectOptions) => Promise<V8Client>;
}

/** Params as callers pass them: a mutation's `opId` is filled in once. */
export type CallParams<M extends MethodName> = MethodParams<M> extends { opId: string }
  ? Omit<MethodParams<M>, "opId"> & { opId?: string }
  : MethodParams<M>;

export interface CallOptions {
  signal?: AbortSignal;
  /** Re-issue after a reconnect even when the method is not idempotent. */
  retry?: boolean;
}

/** The daemon refused the credential or protocol; retrying cannot help. */
function permanent(error: Error): boolean {
  if (error instanceof RejectedError) {
    return !["rateLimited", "overloaded"].includes(error.rejected.code);
  }
  if (error instanceof ConnectionClosedError) {
    return error.code === "revoked" || error.code === "expired";
  }
  return false;
}

function retryAfter(error: Error | undefined): number | undefined {
  return error instanceof RejectedError ? error.rejected.retryAfterMs : undefined;
}

export function newId(): string {
  return globalThis.crypto.randomUUID();
}

/** Whether a call may be sent again after the connection dropped. */
export function idempotent(method: MethodName): boolean {
  const info = METHODS[method];
  return method === "orchestration.dispatch" || info.opId || info.scope === "read" || info.scope === "files:read";
}

/**
 * The backoff of RFC 0003 §2.6: the first retry is immediate, then 250 ms
 * doubling to a 10 s cap, each with ±20% jitter.
 */
export function backoffDelay(
  attempt: number,
  options: { initialDelayMs?: number; maxDelayMs?: number; jitter?: number; random?: () => number } = {},
): number {
  if (attempt <= 1) return 0;
  const initial = options.initialDelayMs ?? 250;
  const max = options.maxDelayMs ?? 10_000;
  const jitter = options.jitter ?? 0.2;
  const base = Math.min(max, initial * 2 ** (attempt - 2));
  const factor = 1 + jitter * (2 * (options.random ?? Math.random)() - 1);
  return Math.round(base * factor);
}

/** A stream subscription that lasts across reconnects. */
export class StreamHandle<S extends StreamName> {
  private inner: Subscription<S> | undefined;
  private lastCursor: T.StreamCursor | undefined;
  private finished = false;

  /** @internal */
  constructor(
    private readonly connection: DaemonConnection,
    readonly stream: S,
    private readonly spec: StreamSpecOf<S>,
    private readonly handlers: StreamHandlers<S> & { reopened?(): void },
    private readonly options: SubscribeOptions,
  ) {
    this.lastCursor = options.after;
  }

  /** The last cursor applied; a reconnect resumes from it. */
  get cursor(): T.StreamCursor | undefined {
    return this.inner?.cursor ?? this.lastCursor;
  }

  get isEnded(): boolean {
    return this.finished;
  }

  /** Send DATA on the current channel (an interactive terminal's input). */
  sendData(bytes: Uint8Array): boolean {
    if (!this.inner || this.inner.isEnded) return false;
    this.inner.sendData(bytes);
    return true;
  }

  unsubscribe(): void {
    if (this.finished) return;
    this.finished = true;
    this.inner?.unsubscribe();
    this.inner = undefined;
    this.connection.forget(this);
  }

  /** @internal Open on a fresh connection. */
  open(client: V8Client, reopened: boolean): void {
    if (this.finished) return;
    if (reopened) this.handlers.reopened?.();
    const subscribeOptions: SubscribeOptions = { ...this.options, after: this.cursor };
    this.inner = client.subscribe(
      this.stream,
      this.spec,
      {
        item: (item) => this.handlers.item(item),
        data: this.handlers.data && ((bytes, fin) => this.handlers.data?.(bytes, fin)),
        end: (end) => this.ended(end),
      },
      subscribeOptions,
    );
  }

  /** @internal The connection dropped; keep the cursor for the next one. */
  detach(): void {
    if (this.inner) this.lastCursor = this.inner.cursor ?? this.lastCursor;
    this.inner = undefined;
  }

  private ended(end: T.End): void {
    // The daemon is going away; the reconnect resumes this stream.
    if (end.reason.type === "shuttingDown") {
      this.detach();
      return;
    }
    this.finished = true;
    this.inner = undefined;
    this.connection.forget(this);
    this.handlers.end?.(end);
  }
}

export class DaemonConnection {
  private current: V8Client | undefined;
  private snapshot: ConnectionState = { phase: "idle", attempt: 0 };
  private readonly listeners = new Set<(state: ConnectionState) => void>();
  private readonly streams = new Set<StreamHandle<StreamName>>();
  private waiters: Array<{ resolve(client: V8Client): void; reject(error: Error): void }> = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private everConnected = false;
  private generation = 0;

  constructor(private readonly options: DaemonConnectionOptions) {}

  get state(): ConnectionState {
    return this.snapshot;
  }

  /** The live client, when connected. */
  get client(): V8Client | undefined {
    return this.current && !this.current.isClosed ? this.current : undefined;
  }

  onState(listener: (state: ConnectionState) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /** Begin connecting; resolves with the first welcome, rejects if the first attempt fails for good. */
  start(): Promise<T.Welcome> {
    if (this.snapshot.phase === "idle" || this.snapshot.phase === "failed") {
      this.attempt(1);
    }
    return this.ready().then((client) => client.welcome);
  }

  /** The live client, waiting for a connection when there is none. */
  ready(signal?: AbortSignal): Promise<V8Client> {
    const live = this.client;
    if (live && this.snapshot.phase === "connected") return Promise.resolve(live);
    if (this.snapshot.phase === "closed" || this.snapshot.phase === "failed" || this.snapshot.phase === "idle") {
      return Promise.reject(this.snapshot.error ?? new ConnectionClosedError("not connected to the daemon"));
    }
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject };
      this.waiters.push(waiter);
      signal?.addEventListener("abort", () => {
        this.waiters = this.waiters.filter((candidate) => candidate !== waiter);
        reject(new ConnectionClosedError("aborted"));
      });
    });
  }

  /**
   * Try now instead of waiting out the backoff (a network change or the app
   * returning to the foreground), and start the backoff over.
   */
  reconnectNow(): void {
    if (this.snapshot.phase !== "reconnecting") return;
    clearTimeout(this.timer);
    this.attempt(1);
  }

  /** Close for good; pending calls and waiters fail. */
  close(): void {
    if (this.snapshot.phase === "closed") return;
    clearTimeout(this.timer);
    this.generation += 1;
    const client = this.current;
    this.current = undefined;
    for (const stream of this.streams) stream.detach();
    this.streams.clear();
    this.setState({ phase: "closed", attempt: 0 });
    this.flushWaiters(new ConnectionClosedError("closed by the client", "shuttingDown"));
    client?.close();
  }

  /**
   * Call `method`. A call interrupted by a disconnect is sent again on the
   * next connection when that is safe (see {@link idempotent}); a mutation's
   * `opId` is generated once, so the daemon's receipt deduplicates it.
   */
  async call<M extends MethodName>(
    method: M,
    params: CallParams<M>,
    options: CallOptions = {},
  ): Promise<MethodResultOf<M>> {
    const full = (
      METHODS[method].opId && !(params as { opId?: string }).opId ? { ...params, opId: newId() } : params
    ) as MethodParams<M>;
    const retry = options.retry ?? idempotent(method);
    for (;;) {
      const client = await this.ready(options.signal);
      try {
        return await client.call(method, full, { signal: options.signal });
      } catch (error) {
        if (error instanceof ConnectionClosedError && retry && !options.signal?.aborted && this.alive()) continue;
        throw error;
      }
    }
  }

  /** `orchestration.dispatch`; `commandId` makes a retry a receipt hit. */
  dispatch(command: T.Command, commandId: string = newId()): Promise<T.DispatchResult> {
    return this.call("orchestration.dispatch", { commandId, command });
  }

  /** Subscribe for as long as the handle lives, across reconnects. */
  subscribe<S extends StreamName>(
    stream: S,
    spec: StreamSpecOf<S>,
    handlers: StreamHandlers<S> & { reopened?(): void },
    options: SubscribeOptions = {},
  ): StreamHandle<S> {
    const handle = new StreamHandle(this, stream, spec, handlers, options);
    this.streams.add(handle as unknown as StreamHandle<StreamName>);
    const client = this.client;
    if (client && this.snapshot.phase === "connected") handle.open(client, false);
    return handle;
  }

  /** `blobs.put` with DATA; an interrupted upload starts over. */
  async uploadBlob(bytes: Uint8Array, options: UploadOptions): Promise<T.BlobPut> {
    for (;;) {
      const client = await this.ready();
      try {
        return await client.uploadBlob(bytes, options);
      } catch (error) {
        if (error instanceof ConnectionClosedError && this.alive()) continue;
        throw error;
      }
    }
  }

  /** A blob's bytes (or a file inside an attachment). */
  async downloadBlob(reference: string, path?: string): Promise<{ meta: T.BlobDownloadItem; bytes: Uint8Array }> {
    for (;;) {
      const client = await this.ready();
      try {
        return await client.downloadBlob(reference, path);
      } catch (error) {
        if (error instanceof ConnectionClosedError && this.alive()) continue;
        throw error;
      }
    }
  }

  /** @internal */
  forget(handle: StreamHandle<StreamName>): void {
    this.streams.delete(handle);
  }

  private alive(): boolean {
    return this.snapshot.phase !== "closed" && this.snapshot.phase !== "failed";
  }

  private attempt(attempt: number): void {
    const generation = ++this.generation;
    this.setState({
      phase: this.everConnected ? "reconnecting" : "connecting",
      attempt: attempt - 1,
      error: this.snapshot.error,
      welcome: this.snapshot.welcome,
    });
    const connect = this.options.connect ?? ((options: V8ConnectOptions) => V8Client.connect(options));
    connect(this.options).then(
      (client) => {
        if (generation !== this.generation) {
          client.close();
          return;
        }
        this.connected(client);
      },
      (error: Error) => {
        if (generation !== this.generation) return;
        this.failed(error, attempt);
      },
    );
  }

  private connected(client: V8Client): void {
    const reopened = this.everConnected;
    this.everConnected = true;
    this.current = client;
    client.onClose((error) => {
      if (this.current !== client) return;
      this.current = undefined;
      for (const stream of this.streams) stream.detach();
      this.failed(error, 1);
    });
    this.setState({ phase: "connected", attempt: 0, welcome: client.welcome });
    for (const stream of [...this.streams]) stream.open(client, reopened);
    const waiters = this.waiters;
    this.waiters = [];
    for (const waiter of waiters) waiter.resolve(client);
  }

  private failed(error: Error, attempt: number): void {
    if (this.snapshot.phase === "closed") return;
    if (permanent(error) || (!this.everConnected && !this.options.retryInitialFailure)) {
      this.setState({ phase: "failed", attempt, error, welcome: this.snapshot.welcome });
      this.flushWaiters(error);
      return;
    }
    const next = attempt + (this.snapshot.phase === "connected" ? 0 : 1);
    const delay = Math.max(retryAfter(error) ?? 0, backoffDelay(next, this.options));
    const generation = this.generation;
    this.setState({
      phase: this.everConnected ? "reconnecting" : "connecting",
      attempt: next - 1,
      error,
      retryAt: Date.now() + delay,
      welcome: this.snapshot.welcome,
    });
    this.timer = setTimeout(() => {
      if (generation === this.generation) this.attempt(next);
    }, delay);
  }

  private flushWaiters(error: Error): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const waiter of waiters) waiter.reject(error);
  }

  private setState(state: ConnectionState): void {
    this.snapshot = state;
    for (const listener of [...this.listeners]) listener(state);
  }
}

export { CallError, ConnectionClosedError, RejectedError };
