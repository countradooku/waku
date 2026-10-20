// Re-encode the v8 golden fixtures with the TypeScript codec, for the Rust
// cross-language test (`crates/waku-protocol/tests/v8_golden.rs`):
//
//   bun reencode-golden.ts <fixtures dir> <output dir>
//
// Each message fixture is decoded from Rust's MessagePack, re-encoded as
// MessagePack (`<name>.msgpack.hex`), and decoded from Rust's JSON and
// re-encoded as JSON (`<name>.json`). Rust then decodes both.

import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import * as msgpack from "../msgpack";

const [fixtures, output] = process.argv.slice(2);
if (!fixtures || !output) {
  console.error("usage: bun reencode-golden.ts <fixtures dir> <output dir>");
  process.exit(2);
}

function fromHex(text: string): Uint8Array {
  const clean = text.trim();
  const bytes = new Uint8Array(clean.length / 2);
  for (let index = 0; index < bytes.length; index++) bytes[index] = Number.parseInt(clean.slice(index * 2, index * 2 + 2), 16);
  return bytes;
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

const index = JSON.parse(readFileSync(join(fixtures, "index.json"), "utf8")) as { messages: Array<{ name: string }> };
for (const { name } of index.messages) {
  const fromMsgpack = msgpack.decode(fromHex(readFileSync(join(fixtures, `${name}.msgpack.hex`), "utf8")));
  writeFileSync(join(output, `${name}.msgpack.hex`), `${toHex(msgpack.encode(fromMsgpack))}\n`);
  const fromJson = JSON.parse(readFileSync(join(fixtures, `${name}.json`), "utf8"));
  writeFileSync(join(output, `${name}.json`), JSON.stringify(fromJson));
}
