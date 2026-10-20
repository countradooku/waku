// The Rust golden fixtures (crates/waku-protocol/tests/v8) decode in
// TypeScript, re-encode to the same MessagePack bytes, and normalize.

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { parseFrame } from "./frame";
import { PREFACE } from "./generated/constants";
import {
  itemNormalizers,
  normalizeClientMessage,
  normalizeServerMessage,
  resultNormalizers,
} from "./generated/decode";
import type { MethodName } from "./generated/methods";
import type { StreamName } from "./generated/streams";
import * as msgpack from "./msgpack";

const fixtures = join(import.meta.dir, "../../../../crates/waku-protocol/tests/v8");

interface Index {
  routes: { calls: Record<string, MethodName>; channels: Record<string, StreamName> };
  messages: Array<{ name: string; direction: "client" | "server" }>;
  frames: string[];
}

const index = JSON.parse(readFileSync(join(fixtures, "index.json"), "utf8")) as Index;

function fromHex(text: string): Uint8Array {
  const clean = text.trim();
  const bytes = new Uint8Array(clean.length / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = Number.parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  return bytes;
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function read(name: string): { json: unknown; msgpackHex: string } {
  return {
    json: JSON.parse(readFileSync(join(fixtures, `${name}.json`), "utf8")),
    msgpackHex: readFileSync(join(fixtures, `${name}.msgpack.hex`), "utf8").trim(),
  };
}

describe("v8 golden fixtures", () => {
  test("cover every message direction", () => {
    expect(index.messages.length).toBeGreaterThan(20);
    expect(new Set(index.messages.map((message) => message.direction))).toEqual(new Set(["client", "server"]));
  });

  for (const { name, direction } of index.messages) {
    test(`${name}: MessagePack and JSON decode to the same value and re-encode byte for byte`, () => {
      const { json, msgpackHex } = read(name);
      const decoded = msgpack.decode(fromHex(msgpackHex));
      expect(decoded).toEqual(json);
      expect(toHex(msgpack.encode(json))).toBe(msgpackHex);
    });

    test(`${name}: normalizes without losing known values`, () => {
      const { json } = read(name);
      const copy = structuredClone(json) as Record<string, unknown>;
      if (direction === "client") {
        expect(normalizeClientMessage(copy)).toEqual(json as never);
        return;
      }
      const message = normalizeServerMessage(copy);
      expect(message.type).not.toBe("unknown");
      if (message.type === "reply" && message.ok !== undefined) {
        const method = index.routes.calls[String(message.id)]!;
        expect(resultNormalizers[method](structuredClone(message.ok))).toEqual(message.ok as never);
      }
      if (message.type === "items") {
        const stream = index.routes.channels[String(message.channel)]!;
        for (const item of message.items) {
          expect(itemNormalizers[stream](structuredClone(item))).toEqual(item as never);
        }
      }
      expect(message).toEqual(json as never);
    });
  }

  test("frames parse as the Rust codec wrote them", () => {
    const hex = (name: string) => fromHex(readFileSync(join(fixtures, `${name}.hex`), "utf8"));
    expect(Array.from(hex("frame_preface"))).toEqual([...PREFACE]);
    const ping = parseFrame(hex("frame_msg_ping"));
    expect(ping?.kind).toBe("msg");
    expect(msgpack.decode(ping!.body)).toEqual({ type: "ping", nonce: 7 });
    expect(parseFrame(hex("frame_data_chunk"))).toEqual({
      kind: "data",
      channel: 2,
      fin: false,
      body: new TextEncoder().encode("chunk-1"),
    });
    expect(parseFrame(hex("frame_data_fin"))).toEqual({ kind: "data", channel: 2, fin: true, body: new Uint8Array() });
  });
});

describe("forward compatibility", () => {
  test("unknown tags become the unknown fallback, unknown fields are kept", () => {
    const message = normalizeServerMessage({
      type: "end",
      channel: 3,
      reason: { type: "error", error: { code: "quantum", message: "m", details: { type: "newDetail", x: 1 } } },
      resumeAfter: { type: "wormhole" },
      futureField: true,
    });
    expect(message).toEqual({
      type: "end",
      channel: 3,
      reason: { type: "error", error: { code: "quantum", message: "m", details: { type: "unknown", rawType: "newDetail" } } },
      resumeAfter: { type: "unknown", rawType: "wormhole" },
      futureField: true,
    } as never);
    expect(normalizeServerMessage({ type: "hologram" })).toEqual({ type: "unknown", rawType: "hologram" });
    expect(itemNormalizers.settings({ type: "synchronized", cursor: { type: "revision", revision: 3 } })).toEqual({
      type: "synchronized",
      cursor: { type: "revision", revision: 3 },
    });
    expect(itemNormalizers.settings({ type: "settingsMoved", to: "x" })).toEqual({ type: "unknown", rawType: "settingsMoved" });
  });
});
