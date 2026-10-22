// DaemonConnection against a scripted daemon on a real WebSocket: reconnect
// with backoff, stream resume from the applied cursor, and call re-issue.

import { afterEach, describe, expect, test } from "bun:test";
import type { ServerWebSocket } from "bun";

import { ConnectionClosedError } from "./client";
import { backoffDelay, DaemonConnection, idempotent } from "./connection";
import { WEBSOCKET_SUBPROTOCOL } from "./generated/constants";
import type * as T from "./generated/types";
import { DraftsStore, ThreadStore, applyDraftsItem, applyProvidersItem, createProvidersState } from "./stores";

type Message = Record<string, any>;

interface Peer {
  received: Message[];
  send(message: Message): void;
  close(): void;
}

const STORE = "00000000-0000-4000-8000-000000000002";

function welcome(): Message {
  return {
    type: "welcome",
    protocol: { major: 8, minor: 0 },
    daemonVersion: "test",
    environment: { id: "00000000-0000-4000-8000-000000000001", name: "test", hostOs: "macos", hostArch: "aarch64", storeId: STORE },
    session: { id: "00000000-0000-4000-8000-000000000003", scopes: ["admin"] },
    limits: {
      maxMessageBytes: 4 * 1024 * 1024,
      maxDataChunkBytes: 256 * 1024,
      maxBlobBytes: 64 * 1024 * 1024,
      pingIntervalMs: 5000,
      pingTimeoutMs: 15000,
      maxInFlightCalls: 64,
      maxSubscriptions: 256,
    },
  };
}

const servers: Array<{ stop(force?: boolean): void }> = [];
const connections: DaemonConnection[] = [];
afterEach(() => {
  for (const connection of connections.splice(0)) connection.close();
  for (const server of servers.splice(0)) server.stop(true);
});

function daemon(script: (peer: Peer, message: Message, index: number) => void, hello?: (peer: Peer, index: number) => void) {
  const peers: Peer[] = [];
  const server = Bun.serve<{ peer: Peer | undefined }>({
    port: 0,
    fetch(request, server) {
      const upgraded = server.upgrade(request, {
        headers: { "Sec-WebSocket-Protocol": WEBSOCKET_SUBPROTOCOL },
        data: { peer: undefined },
      });
      return upgraded ? undefined : new Response("upgrade failed", { status: 400 });
    },
    websocket: {
      message(socket: ServerWebSocket<{ peer: Peer | undefined }>, raw) {
        if (typeof raw !== "string") return;
        const message = JSON.parse(raw) as Message;
        if (!socket.data.peer) {
          const peer: Peer = {
            received: [],
            send: (reply) => void socket.send(JSON.stringify(reply)),
            close: () => socket.close(),
          };
          socket.data.peer = peer;
          peers.push(peer);
        }
        const peer = socket.data.peer;
        peer.received.push(message);
        const index = peers.indexOf(peer);
        if (message.type === "hello") {
          if (hello) hello(peer, index);
          else peer.send(welcome());
          return;
        }
        script(peer, message, index);
      },
    },
  });
  servers.push(server);
  return { url: `ws://127.0.0.1:${server.port}`, peers };
}

function open(url: string, options: Partial<ConstructorParameters<typeof DaemonConnection>[0]> = {}) {
  const connection = new DaemonConnection({
    url,
    credential: { type: "local", token: "secret" },
    client: { kind: "web", appVersion: "test", os: "test", instanceId: "00000000-0000-4000-8000-0000000000ff" },
    initialDelayMs: 5,
    maxDelayMs: 20,
    ...options,
  });
  connections.push(connection);
  return connection;
}

function until(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const check = () => {
      if (predicate()) resolve();
      else if (Date.now() - started > timeoutMs) reject(new Error("timed out"));
      else setTimeout(check, 2);
    };
    check();
  });
}

const settings: T.Settings = { disabledProviders: [] };

describe("backoff", () => {
  test("first retry is immediate, then 250 ms doubling to 10 s with ±20% jitter", () => {
    const exact = { random: () => 0.5 };
    expect([1, 2, 3, 4, 5].map((attempt) => backoffDelay(attempt, exact))).toEqual([0, 250, 500, 1000, 2000]);
    expect(backoffDelay(20, exact)).toBe(10_000);
    expect(backoffDelay(2, { random: () => 0 })).toBe(200);
    expect(backoffDelay(2, { random: () => 1 })).toBe(300);
  });

  test("only reads and keyed mutations are re-issued", () => {
    expect(idempotent("threads.historyPage")).toBe(true);
    expect(idempotent("files.tree")).toBe(true);
    expect(idempotent("orchestration.dispatch")).toBe(true);
    expect(idempotent("vcs.commit")).toBe(true);
    expect(idempotent("terminal.restart")).toBe(false);
    expect(idempotent("vcs.generateCommitMessage")).toBe(false);
  });
});

describe("DaemonConnection", () => {
  test("reconnects and resumes a stream from its applied cursor", async () => {
    const { url, peers } = daemon((peer, message, index) => {
      if (message.type !== "subscribe") return;
      if (index === 0) {
        peer.send({
          type: "items",
          channel: message.channel,
          items: [
            { type: "snapshot", revision: 7, settings },
            { type: "synchronized", cursor: { type: "revision", revision: 7 } },
          ],
        });
        setTimeout(() => peer.close(), 5);
      } else {
        peer.send({ type: "items", channel: message.channel, items: [{ type: "synchronized", cursor: message.after }] });
      }
    });
    const connection = open(url);
    const phases: string[] = [];
    connection.onState((state) => phases.push(state.phase));
    await connection.start();
    const items: string[] = [];
    let reopened = 0;
    connection.subscribe("settings", {}, { item: (item) => items.push(item.type), reopened: () => (reopened += 1) });
    await until(() => peers.length === 2 && peers[1]!.received.some((message) => message.type === "subscribe"));
    const resubscribe = peers[1]!.received.find((message) => message.type === "subscribe")!;
    expect(resubscribe.after).toEqual({ type: "revision", revision: 7 });
    expect(resubscribe.stream).toEqual({ type: "settings" });
    await until(() => items.length === 3);
    expect(items).toEqual(["snapshot", "synchronized", "synchronized"]);
    expect(reopened).toBe(1);
    expect(phases).toContain("reconnecting");
    expect(connection.state.phase).toBe("connected");
  });

  test("re-issues an interrupted keyed mutation with the same opId", async () => {
    const { url, peers } = daemon((peer, message, index) => {
      if (message.type !== "call") return;
      if (index === 0) peer.close();
      else peer.send({ type: "reply", id: message.id, ok: { revision: "sha256:1" } });
    });
    const connection = open(url);
    await connection.start();
    const result = await connection.call("files.write", {
      root: { type: "path", path: "/tmp" },
      path: "a.txt",
      content: "x",
    });
    expect(result).toEqual({ revision: "sha256:1" });
    const first = peers[0]!.received.find((message) => message.type === "call")!;
    const second = peers[1]!.received.find((message) => message.type === "call")!;
    expect(first.params.opId).toBeString();
    expect(second.params.opId).toBe(first.params.opId);
  });

  test("dispatch keeps its commandId across a reconnect", async () => {
    const { url, peers } = daemon((peer, message, index) => {
      if (message.type !== "call") return;
      if (index === 0) peer.close();
      else peer.send({ type: "reply", id: message.id, ok: { sequence: 9 } });
    });
    const connection = open(url);
    await connection.start();
    const result = await connection.dispatch({ type: "thread.archive", threadId: "t" }, "command-1");
    expect(result.sequence).toBe(9);
    expect(peers[1]!.received.find((message) => message.type === "call")!.params.commandId).toBe("command-1");
  });

  test("a call that is not safe to repeat fails with the disconnect", async () => {
    const { url } = daemon((peer, message) => {
      if (message.type === "call") peer.close();
    });
    const connection = open(url);
    await connection.start();
    await expect(connection.call("terminal.restart", { terminalId: "x" })).rejects.toBeInstanceOf(ConnectionClosedError);
  });

  test("a refused credential fails for good", async () => {
    const { url, peers } = daemon(
      () => {},
      (peer) => peer.send({ type: "rejected", code: "unauthenticated", message: "bad token" }),
    );
    const connection = open(url, { retryInitialFailure: true });
    await expect(connection.start()).rejects.toThrow("bad token");
    expect(connection.state.phase).toBe("failed");
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(peers.length).toBe(1);
  });

  test("an unreachable daemon is retried when asked to", async () => {
    let accept = false;
    const { url, peers } = daemon(
      () => {},
      (peer) => {
        if (accept) peer.send(welcome());
        else peer.close();
      },
    );
    const connection = open(url, { retryInitialFailure: true });
    const started = connection.start();
    await until(() => peers.length >= 2);
    accept = true;
    await started;
    expect(connection.state.phase).toBe("connected");
  });
});

describe("stores", () => {
  const thread = (seq: number) => ({
    type: "snapshot",
    cursor: { type: "sequence", store: STORE, seq },
    projection: {
      thread: { id: "t1", projectId: "p", title: "x" },
      turnItems: [
        { id: `i${seq}`, threadId: "t1", position: 100 + seq, status: "completed", updatedAt: 1, kind: { type: "system_notice", message: "m" } },
      ],
    },
    history: "older",
    hasMoreHistory: true,
  });

  test("ThreadStore pages history and drops a page a newer snapshot replaced", async () => {
    let pages = 0;
    let channel = 0;
    let peerRef: Peer | undefined;
    const { url } = daemon((peer, message) => {
      peerRef = peer;
      if (message.type === "subscribe") {
        channel = message.channel;
        peer.send({ type: "items", channel, items: [thread(1), { type: "synchronized", cursor: { type: "sequence", store: STORE, seq: 1 } }] });
      } else if (message.type === "call" && message.method === "threads.historyPage") {
        pages += 1;
        const reply = () =>
          peer.send({
            type: "reply",
            id: message.id,
            ok: { items: [{ id: `old${pages}`, threadId: "t1", position: 1, status: "completed", updatedAt: 1, kind: { type: "system_notice", message: "o" } }] },
          });
        if (pages === 2) {
          // A fresh snapshot lands before this page.
          peer.send({ type: "items", channel, items: [thread(5)] });
          setTimeout(reply, 5);
        } else reply();
      }
    });
    const connection = open(url);
    await connection.start();
    const store = new ThreadStore(connection, "t1").start();
    await until(() => store.status === "ready");
    expect([...store.state.turnItems.keys()]).toEqual(["i1"]);
    await store.loadOlder();
    expect([...store.state.turnItems.keys()].sort()).toEqual(["i1", "old1"]);
    expect(store.state.hasMoreHistory).toBe(false);
    store.state.hasMoreHistory = true;
    store.state.history = "older";
    await store.loadOlder();
    expect([...store.state.turnItems.keys()]).toEqual(["i5"]);
    expect(store.state.history).toBe("older");
    expect(peerRef).toBeDefined();
    store.close();
  });

  test("DraftsStore applies locally and on the daemon", async () => {
    const calls: Message[] = [];
    const { url } = daemon((peer, message) => {
      if (message.type === "subscribe") {
        peer.send({
          type: "items",
          channel: message.channel,
          items: [{ type: "snapshot", revision: 1, drafts: { threads: { a: { text: "hi" } } } }],
        });
      } else if (message.type === "call") {
        calls.push(message);
        peer.send({ type: "reply", id: message.id, ok: { revision: 2 } });
      }
    });
    const connection = open(url);
    await connection.start();
    const drafts = new DraftsStore(connection).start();
    await until(() => drafts.status === "ready");
    await drafts.apply([
      { target: { type: "thread", threadId: "a" } },
      { target: { type: "newThread", projectId: "p" }, draft: { text: "new" } },
    ]);
    expect(drafts.state.drafts).toEqual({ threads: {}, newThreads: { p: { text: "new" } } });
    expect(drafts.state.revision).toBe(2);
    expect(calls[0]!.method).toBe("drafts.apply");
    expect(calls[0]!.params.opId).toBeString();
  });

  test("drafts and providers reducers", () => {
    const state = { revision: undefined as number | undefined, drafts: {} as T.ComposerDrafts };
    applyDraftsItem(state, { type: "snapshot", revision: 1, drafts: { newThreads: { p: { text: "a" } } } });
    applyDraftsItem(state, {
      type: "changed",
      revision: 2,
      changes: [
        { target: { type: "newThread", projectId: "p" }, draft: { text: "" } },
        { target: { type: "thread", threadId: "t" }, draft: { text: "b" } },
      ],
    });
    expect(state).toEqual({ revision: 2, drafts: { newThreads: {}, threads: { t: { text: "b" } } } });

    const providers = createProvidersState();
    const status: T.ProviderStatus = { instanceId: "codex", kind: "codex", displayName: "Codex", enabled: true, installed: true, planUsage: true };
    applyProvidersItem(providers, { type: "snapshot", revision: 1, statuses: [status] });
    applyProvidersItem(providers, { type: "statusUpdated", revision: 2, status: { ...status, installed: false } });
    applyProvidersItem(providers, { type: "planUsageUpdated", instanceId: "codex", usage: { windows: [] } });
    expect(providers.statuses.get("codex")?.installed).toBe(false);
    expect(providers.planUsage.get("codex")).toEqual({ windows: [] });
  });
});
