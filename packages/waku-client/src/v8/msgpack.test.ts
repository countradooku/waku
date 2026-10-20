import { describe, expect, test } from "bun:test";

import { decode, encode, MessagePackError } from "./msgpack";

const hex = (bytes: Uint8Array) => Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

describe("MessagePack", () => {
  test("integers use the smallest form, like rmp-serde", () => {
    const cases: Array<[number, string]> = [
      [0, "00"],
      [127, "7f"],
      [128, "cc80"],
      [200, "ccc8"],
      [65535, "cdffff"],
      [65536, "ce00010000"],
      [2 ** 32, "cf0000000100000000"],
      [Number.MAX_SAFE_INTEGER, "cf001fffffffffffff"],
      [-1, "ff"],
      [-32, "e0"],
      [-33, "d0df"],
      [-200, "d1ff38"],
      [-40000, "d2ffff63c0"],
    ];
    for (const [value, expected] of cases) {
      expect(hex(encode(value))).toBe(expected);
      expect(decode(encode(value))).toBe(value);
    }
  });

  test("strings, binaries, arrays and maps round-trip", () => {
    const value = {
      short: "é",
      long: "x".repeat(40),
      huge: "y".repeat(70_000),
      bytes: new Uint8Array([1, 2, 3]),
      list: Array.from({ length: 20 }, (_, index) => index),
      nested: { a: null, b: true, c: false, d: 1.5 },
    };
    expect(decode(encode(value))).toEqual(value);
    // Keys keep insertion order, and undefined values are left out.
    expect(hex(encode({ type: "ping", skipped: undefined, nonce: 7 }))).toBe("82a474797065a470696e67a56e6f6e636507");
  });

  test("a __proto__ key stays an own property", () => {
    const decoded = decode(encode(JSON.parse('{"__proto__": {"polluted": true}}'))) as Record<string, unknown>;
    expect(Object.keys(decoded)).toEqual(["__proto__"]);
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });

  test("integers beyond 2^53 - 1 and truncated input are rejected", () => {
    expect(() => decode(new Uint8Array([0xcf, 0, 0x20, 0, 0, 0, 0, 0, 0]))).toThrow(MessagePackError);
    expect(() => decode(new Uint8Array([0xa5, 0x61]))).toThrow(MessagePackError);
  });
});
