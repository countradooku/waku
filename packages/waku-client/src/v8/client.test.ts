// The v8 client transport against a scripted daemon on a real WebSocket.

import { afterEach, describe, expect, test } from "bun:test";
import type { ServerWebSocket } from "bun";

import { CallError, ConnectionClosedError, RejectedError, V8Client, type Encoding } from "./client";
import { encodeDataFrame, encodeMsgFrame, parseFrame } from "./frame";
import { WEBSOCKET_SUBPROTOCOL } from "./generated/constants";
import type * as T from "./generated/types";
import * as msgpack from "./msgpack";

type Message = Record<string, any>;

interface Peer {
  /** Every message the client sent, decoded. */
  received: Message[];
  /** Every DATA frame the client sent. */
  data: Array<{ channel: number; fin: boolean; body: Uint8Array }>;
  send(message: Message): void;
  sendData(channel: number, fin: boolean, body: Uint8Array): void;
  close(): void;
}

type Script = (peer: Peer, message: Message) => void;

const limits: T.Limits = {
  maxMessageBytes: 4 * 1024 * 1024,
  maxDataChunkBytes: 256 * 1024,
  maxBlobBytes: 64 * 1024 * 1024,
  pingIntervalMs: 5000,
  pingTimeoutMs: 15000,
  maxInFlightCalls: 64,
  maxSubscriptions: 256,
};

function welcome(overrides: Partial<T.Limits> = {}): Message {
  return {
    type: "welcome",
    protocol: { major: 8, minor: 0 },
    daemonVersion: "test",
    environment: {
      id: "00000000-0000-4000-8000-000000000001",
      name: "test",
      hostOs: "macos",
      hostArch: "aarch64",
      storeId: "00000000-0000-4000-8000-000000000002",
    },
    session: { id: "00000000-0000-4000-8000-000000000003", scopes: ["admin"] },
    limits: { ...limits, ...overrides },
    features: [],
  };
}

const servers: Array<{ stop(force?: boolean): void }> = [];
afterEach(() => {
  for (const server of servers.splice(0)) server.stop(true);
});

/** A daemon that answers `hello` with `welcome` and hands everything else to `script`. */
function daemon(script: Script, options: { hello?: (peer: Peer, hello: Message) => void } = {}) {
  const peers: Peer[] = [];
  const server = Bun.serve<{ encoding: Encoding | undefined; peer: Peer | undefined }>({
    port: 0,
    fetch(request, server) {
      const offered = (request.headers.get("sec-websocket-protocol") ?? "").split(",").map((value) => value.trim());
      if (!offered.includes(WEBSOCKET_SUBPROTOCOL)) return new Response("subprotocol required", { status: 426 });
      if (new URL(request.url).pathname !== "/v8") return new Response("not found", { status: 404 });
      const upgraded = server.upgrade(request, {
        headers: { "Sec-WebSocket-Protocol": WEBSOCKET_SUBPROTOCOL },
        data: { encoding: undefined, peer: undefined },
      });
      return upgraded ? undefined : new Response("upgrade failed", { status: 400 });
    },
    websocket: {
      message(socket: ServerWebSocket<{ encoding: Encoding | undefined; peer: Peer | undefined }>, raw) {
        let message: Message;
        if (typeof raw === "string") {
          socket.data.encoding ??= "json";
          message = JSON.parse(raw);
        } else {
          socket.data.encoding ??= "msgpack";
          const frame = parseFrame(new Uint8Array(raw));
          if (frame?.kind === "data") {
            const peer = socket.data.peer;
            if (!peer) return;
            peer.data.push({ channel: frame.channel, fin: frame.fin, body: frame.body.slice() });
            script(peer, { type: "data", channel: frame.channel, fin: frame.fin, size: frame.body.length });
            return;
          }
          if (frame?.kind !== "msg") return;
          message = msgpack.decode(frame.body) as Message;
        }
        if (!socket.data.peer) {
          const encoding = socket.data.encoding;
          const peer: Peer = {
            received: [],
            data: [],
            send: (reply) =>
              void socket.send(encoding === "json" ? JSON.stringify(reply) : encodeMsgFrame(msgpack.encode(reply))),
            sendData: (channel, fin, body) => void socket.send(encodeDataFrame(channel, fin, body)),
            close: () => socket.close(),
          };
          socket.data.peer = peer;
          peers.push(peer);
        }
        const peer = socket.data.peer;
        peer.received.push(message);
        if (message.type === "hello") {
          if (options.hello) options.hello(peer, message);
          else peer.send(welcome());
          return;
        }
        script(peer, message);
      },
    },
  });
  servers.push(server);
  return { url: `ws://127.0.0.1:${server.port}`, peers };
}

function connect(url: string, encoding: Encoding = "json"): Promise<V8Client> {
  return V8Client.connect({
    url,
    encoding,
    credential: { type: "local", token: "secret" },
    client: { kind: "web", appVersion: "test", os: "test", instanceId: "00000000-0000-4000-8000-0000000000ff" },
  });
}

const settings: T.Settings = {
  computerUseEnabled: false,
  computerUseAllowedApps: [],
  disabledProviders: [],
  providerBinaryOverrides: {},
};

function until(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const check = () => {
      if (predicate()) resolve();
      else if (Date.now() - started > timeoutMs) reject(new Error("condition not reached"));
      else setTimeout(check, 5);
    };
    check();
  });
}

describe("V8Client", () => {
  for (const encoding of ["json", "msgpack"] as const) {
    test(`handshakes and calls over ${encoding}`, async () => {
      const { url, peers } = daemon((peer, message) => {
        if (message.type === "call" && message.method === "settings.get") {
          peer.send({ type: "reply", id: message.id, ok: { revision: 5, settings } });
        }
      });
      const client = await connect(url, encoding);
      expect(client.welcome.session.scopes).toEqual(["admin"]);
      const result = await client.call("settings.get", {});
      expect(result).toEqual({ revision: 5, settings });
      const hello = peers[0]!.received[0]!;
      expect(hello).toMatchObject({ type: "hello", protocol: { major: 8, minor: 0 }, credential: { type: "local" } });
      expect(peers[0]!.received[1]).toEqual({ type: "call", id: 1, method: "settings.get", params: {} });
      client.close();
    });
  }

  test("a refused hello rejects with the reject code", async () => {
    const { url } = daemon(() => {}, {
      hello: (peer) => peer.send({ type: "rejected", code: "unauthenticated", message: "bad token" }),
    });
    const error = await connect(url).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(RejectedError);
    expect((error as RejectedError).rejected.code).toBe("unauthenticated");
  });

  test("reply errors keep their code and details", async () => {
    const { url } = daemon((peer, message) => {
      peer.send({
        type: "reply",
        id: message.id,
        error: { code: "notFound", message: "missing", details: { type: "notFound", resource: "file", id: "a.txt" } },
      });
    });
    const client = await connect(url);
    const error = await client
      .call("files.read", { root: { type: "path", path: "/tmp" }, path: "a.txt" })
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(CallError);
    expect((error as CallError).error.code).toBe("notFound");
    expect((error as CallError).error.details).toEqual({ type: "notFound", resource: "file", id: "a.txt" });
    client.close();
  });

  test("subscriptions deliver normalized items, advance the cursor and resume after an overflow", async () => {
    const { url, peers } = daemon((peer, message) => {
      if (message.type !== "subscribe") return;
      if (!message.after) {
        peer.send({
          type: "items",
          channel: message.channel,
          items: [
            { type: "snapshot", revision: 1, settings },
            { type: "synchronized", cursor: { type: "revision", revision: 1 } },
            { type: "fromTheFuture" },
          ],
        });
        peer.send({ type: "end", channel: message.channel, reason: { type: "overflow" }, resumeAfter: { type: "revision", revision: 1 } });
      } else {
        peer.send({
          type: "items",
          channel: message.channel,
          items: [{ type: "synchronized", cursor: message.after }],
        });
      }
    });
    const client = await connect(url);
    const items: T.SettingsItem[] = [];
    let ended = false;
    const subscription = client.subscribe("settings", {}, { item: (item) => items.push(item), end: () => (ended = true) });
    await until(() => items.length === 4);
    expect(items.map((item) => item.type)).toEqual(["snapshot", "synchronized", "unknown", "synchronized"]);
    expect(items[2]).toEqual({ type: "unknown", rawType: "fromTheFuture" });
    expect(ended).toBe(false);
    expect(subscription.cursor).toEqual({ type: "revision", revision: 1 });
    const subscribes = peers[0]!.received.filter((message) => message.type === "subscribe");
    expect(subscribes).toEqual([
      { type: "subscribe", channel: 1, stream: { type: "settings" } },
      { type: "subscribe", channel: 2, stream: { type: "settings" }, after: { type: "revision", revision: 1 } },
    ]);
    client.close();
  });

  test("sequence streams advance their cursor with every event and resume from it", async () => {
    const store = "00000000-0000-4000-8000-000000000002";
    const threadId = "00000000-0000-4000-8000-0000000000aa";
    const { url, peers } = daemon((peer, message) => {
      if (message.type !== "subscribe") return;
      if (message.after) return;
      peer.send({
        type: "items",
        channel: message.channel,
        items: [
          { type: "synchronized", cursor: { type: "sequence", store, seq: 10 } },
          { type: "event", seq: 12, event: { type: "galaxy.updated" } },
          { type: "delta", itemId: threadId, channel: "text", rev: 1, text: "x" },
        ],
      });
      peer.send({ type: "end", channel: message.channel, reason: { type: "overflow" } });
    });
    const client = await connect(url);
    const items: T.ThreadItem[] = [];
    const subscription = client.subscribe("thread", { threadId }, { item: (item) => items.push(item) });
    await until(() => peers[0]!.received.filter((message) => message.type === "subscribe").length === 2);
    expect(items.map((item) => item.type)).toEqual(["synchronized", "event", "delta"]);
    // Deltas are unsequenced: the cursor stays at the last event.
    expect(subscription.cursor).toEqual({ type: "sequence", store, seq: 12 });
    const resumed = peers[0]!.received.filter((message) => message.type === "subscribe")[1]!;
    expect(resumed.after).toEqual({ type: "sequence", store, seq: 12 });
    client.close();
  });

  test("DATA frames are delivered and their credit returned", async () => {
    const { url, peers } = daemon((peer, message) => {
      if (message.type !== "subscribe") return;
      peer.send({ type: "items", channel: message.channel, items: [{ type: "meta", size: 5, mime: "text/plain", sha256: "00" }] });
      peer.sendData(message.channel, false, new TextEncoder().encode("hel"));
      peer.sendData(message.channel, true, new TextEncoder().encode("lo"));
      peer.send({ type: "end", channel: message.channel, reason: { type: "completed" } });
    });
    const client = await connect(url);
    const chunks: string[] = [];
    let end: T.End | undefined;
    client.subscribe(
      "blobDownload",
      { reference: "waku-blob:00.txt" },
      {
        item: () => {},
        data: (bytes, fin) => chunks.push(`${new TextDecoder().decode(bytes)}${fin ? "|fin" : ""}`),
        end: (finished) => (end = finished),
      },
      { window: 1024 },
    );
    await until(() => end !== undefined);
    expect(chunks).toEqual(["hel", "lo|fin"]);
    expect(end?.reason).toEqual({ type: "completed" });
    await until(() => peers[0]!.received.filter((message) => message.type === "credit").length === 2);
    expect(peers[0]!.received.filter((message) => message.type === "credit")).toEqual([
      { type: "credit", channel: 1, bytes: 3 },
      { type: "credit", channel: 1, bytes: 2 },
    ]);
    expect(peers[0]!.received.find((message) => message.type === "subscribe")?.window).toBe(1024);
    client.close();
  });

  test("answers daemon pings and pings an idle daemon", async () => {
    const { url, peers } = daemon(
      (peer, message) => {
        if (message.type === "ping") peer.send({ type: "pong", nonce: message.nonce });
      },
      {
        hello: (peer) => {
          peer.send(welcome({ pingIntervalMs: 40, pingTimeoutMs: 400 }));
          peer.send({ type: "ping", nonce: 99 });
        },
      },
    );
    const client = await connect(url);
    await until(() => peers[0]!.received.some((message) => message.type === "pong"));
    expect(peers[0]!.received.find((message) => message.type === "pong")).toEqual({ type: "pong", nonce: 99 });
    await until(() => peers[0]!.received.filter((message) => message.type === "ping").length >= 2);
    expect(client.isClosed).toBe(false);
    client.close();
  });

  test("uploads stream the blob as DATA after blobs.put", async () => {
    let call: Message | undefined;
    let received = 0;
    const { url, peers } = daemon((peer, message) => {
      if (message.type === "call" && message.method === "blobs.put") call = message;
      if (message.type === "data") {
        received += message.size;
        if (message.fin) {
          peer.send({
            type: "reply",
            id: call!.id,
            ok: { reference: "waku-blob:00.png", size: received, sha256: "ab", deduplicated: false },
          });
        }
      }
    });
    const client = await connect(url);
    const bytes = new Uint8Array(600 * 1024).map((_, index) => index % 251);
    const put = await client.uploadBlob(bytes, { mime: "image/png", name: "shot.png" });
    expect(put).toEqual({ reference: "waku-blob:00.png", size: bytes.length, sha256: "ab", deduplicated: false });
    expect(call?.params).toEqual({ channel: 1, size: bytes.length, mime: "image/png", name: "shot.png" });
    const data = peers[0]!.data;
    expect(data.map((frame) => [frame.channel, frame.fin, frame.body.length])).toEqual([
      [1, false, 256 * 1024],
      [1, false, 256 * 1024],
      [1, true, 600 * 1024 - 512 * 1024],
    ]);
    client.close();
  });

  test("a deduplicated upload stops and closes its transfer", async () => {
    const { url, peers } = daemon((peer, message) => {
      if (message.type === "call" && message.method === "blobs.put") {
        peer.send({
          type: "reply",
          id: message.id,
          ok: { reference: "waku-blob:00.png", size: message.params.size, sha256: message.params.sha256, deduplicated: true },
        });
      }
    });
    const client = await connect(url);
    const bytes = new Uint8Array(2 * 1024 * 1024);
    const put = await client.uploadBlob(bytes, { mime: "image/png", sha256: "cd" });
    expect(put.deduplicated).toBe(true);
    await until(() => peers[0]!.data.some((frame) => frame.fin));
    const data = peers[0]!.data;
    expect(data.length).toBeLessThan(8);
    expect(data[data.length - 1]).toMatchObject({ channel: 1, fin: true });
    client.close();
  });

  test("terminal input goes out as DATA and output moves the offset past the replay", async () => {
    const generation = "00000000-0000-4000-8000-0000000000bb";
    const terminalId = "00000000-0000-4000-8000-0000000000cc";
    const { url, peers } = daemon((peer, message) => {
      if (message.type !== "subscribe") return;
      peer.send({
        type: "items",
        channel: message.channel,
        items: [{ type: "attached", generation, cols: 80, rows: 24, offset: 100, replay: 3, truncated: false, exited: false }],
      });
      peer.sendData(message.channel, false, new TextEncoder().encode("old"));
      peer.sendData(message.channel, false, new TextEncoder().encode("live!"));
    });
    const client = await connect(url);
    const output: string[] = [];
    const terminal = client.subscribe(
      "terminal",
      { terminalId, interactive: true },
      { item: () => {}, data: (bytes) => output.push(new TextDecoder().decode(bytes)) },
    );
    await until(() => output.length === 2);
    expect(terminal.cursor).toEqual({ type: "offset", generation, offset: 105 });
    terminal.sendData(new TextEncoder().encode("ls\r"));
    await until(() => peers[0]!.data.length === 1);
    expect(peers[0]!.data[0]!.channel).toBe(terminal.channel);
    expect(new TextDecoder().decode(peers[0]!.data[0]!.body)).toBe("ls\r");
    client.close();
  });

  test("downloadBlob collects the DATA of a blobDownload", async () => {
    const { url, peers } = daemon((peer, message) => {
      if (message.type !== "subscribe") return;
      peer.send({ type: "items", channel: message.channel, items: [{ type: "meta", size: 5, mime: "text/plain", sha256: "00" }] });
      peer.sendData(message.channel, false, new TextEncoder().encode("hel"));
      peer.sendData(message.channel, true, new TextEncoder().encode("lo"));
      peer.send({ type: "end", channel: message.channel, reason: { type: "completed" } });
    });
    const client = await connect(url);
    const { meta, bytes } = await client.downloadBlob("waku-attachment:x", "/a/b.txt");
    expect(meta).toEqual({ type: "meta", size: 5, mime: "text/plain", sha256: "00" });
    expect(new TextDecoder().decode(bytes)).toBe("hello");
    expect(peers[0]!.received.find((message) => message.type === "subscribe")?.stream).toEqual({
      type: "blobDownload",
      reference: "waku-attachment:x",
      path: "/a/b.txt",
    });
    client.close();
  });

  test("a silent daemon is declared dead after pingTimeoutMs", async () => {
    const { url } = daemon(() => {}, {
      hello: (peer) => peer.send(welcome({ pingIntervalMs: 20, pingTimeoutMs: 150 })),
    });
    const client = await connect(url);
    const started = Date.now();
    const closed = new Promise<Error>((resolve) => client.onClose(resolve));
    const pending = client.call("settings.get", {}).catch((caught: unknown) => caught);
    const error = await closed;
    expect(error).toBeInstanceOf(ConnectionClosedError);
    expect((error as ConnectionClosedError).code).toBe("idleTimeout");
    expect(Date.now() - started).toBeLessThan(1000);
    expect(await pending).toBe(error);
  });
});
