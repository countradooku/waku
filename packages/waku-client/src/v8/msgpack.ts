// MessagePack for protocol v8 (RFC 0003 §1.3).
//
// The daemon encodes with rmp-serde: structs as maps with named fields,
// integers in their smallest form, keys in declaration order. This codec
// covers the subset the protocol uses and writes the same bytes for the
// same values, so a value round-trips byte for byte. Integers above 2^53 − 1
// never appear on the wire (RFC 0003 §1.1) and are rejected when decoding.

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export class MessagePackError extends Error {}

class Writer {
  private buffer = new Uint8Array(256);
  private view = new DataView(this.buffer.buffer);
  length = 0;

  private reserve(extra: number): void {
    if (this.length + extra <= this.buffer.length) return;
    let size = this.buffer.length * 2;
    while (size < this.length + extra) size *= 2;
    const next = new Uint8Array(size);
    next.set(this.buffer.subarray(0, this.length));
    this.buffer = next;
    this.view = new DataView(next.buffer);
  }

  byte(value: number): void {
    this.reserve(1);
    this.buffer[this.length++] = value;
  }

  u16(value: number): void {
    this.reserve(2);
    this.view.setUint16(this.length, value);
    this.length += 2;
  }

  u32(value: number): void {
    this.reserve(4);
    this.view.setUint32(this.length, value);
    this.length += 4;
  }

  u64(value: bigint): void {
    this.reserve(8);
    this.view.setBigUint64(this.length, value);
    this.length += 8;
  }

  i64(value: bigint): void {
    this.reserve(8);
    this.view.setBigInt64(this.length, value);
    this.length += 8;
  }

  f64(value: number): void {
    this.reserve(8);
    this.view.setFloat64(this.length, value);
    this.length += 8;
  }

  bytes(value: Uint8Array): void {
    this.reserve(value.length);
    this.buffer.set(value, this.length);
    this.length += value.length;
  }

  finish(): Uint8Array {
    return this.buffer.slice(0, this.length);
  }
}

function writeUnsigned(writer: Writer, value: number): void {
  if (value < 0x80) writer.byte(value);
  else if (value < 0x100) {
    writer.byte(0xcc);
    writer.byte(value);
  } else if (value < 0x10000) {
    writer.byte(0xcd);
    writer.u16(value);
  } else if (value < 0x100000000) {
    writer.byte(0xce);
    writer.u32(value);
  } else {
    writer.byte(0xcf);
    writer.u64(BigInt(value));
  }
}

function writeSigned(writer: Writer, value: number): void {
  if (value >= -32) writer.byte(0xe0 | (value + 32));
  else if (value >= -0x80) {
    writer.byte(0xd0);
    writer.byte(value & 0xff);
  } else if (value >= -0x8000) {
    writer.byte(0xd1);
    writer.u16(value & 0xffff);
  } else if (value >= -0x80000000) {
    writer.byte(0xd2);
    writer.u32(value >>> 0);
  } else {
    writer.byte(0xd3);
    writer.i64(BigInt(value));
  }
}

function writeLength(writer: Writer, length: number, fix: number, fixMax: number, m16: number, m32: number): void {
  if (length < fixMax) writer.byte(fix | length);
  else if (length < 0x10000) {
    writer.byte(m16);
    writer.u16(length);
  } else {
    writer.byte(m32);
    writer.u32(length);
  }
}

function writeValue(writer: Writer, value: unknown): void {
  if (value === null || value === undefined) {
    writer.byte(0xc0);
  } else if (typeof value === "boolean") {
    writer.byte(value ? 0xc3 : 0xc2);
  } else if (typeof value === "number") {
    if (Number.isInteger(value) && Math.abs(value) <= Number.MAX_SAFE_INTEGER) {
      if (value >= 0) writeUnsigned(writer, value);
      else writeSigned(writer, value);
    } else {
      writer.byte(0xcb);
      writer.f64(value);
    }
  } else if (typeof value === "bigint") {
    if (value >= 0n) {
      writer.byte(0xcf);
      writer.u64(value);
    } else {
      writer.byte(0xd3);
      writer.i64(value);
    }
  } else if (typeof value === "string") {
    const bytes = encoder.encode(value);
    if (bytes.length < 32) writer.byte(0xa0 | bytes.length);
    else if (bytes.length < 0x100) {
      writer.byte(0xd9);
      writer.byte(bytes.length);
    } else if (bytes.length < 0x10000) {
      writer.byte(0xda);
      writer.u16(bytes.length);
    } else {
      writer.byte(0xdb);
      writer.u32(bytes.length);
    }
    writer.bytes(bytes);
  } else if (value instanceof Uint8Array) {
    if (value.length < 0x100) {
      writer.byte(0xc4);
      writer.byte(value.length);
    } else if (value.length < 0x10000) {
      writer.byte(0xc5);
      writer.u16(value.length);
    } else {
      writer.byte(0xc6);
      writer.u32(value.length);
    }
    writer.bytes(value);
  } else if (Array.isArray(value)) {
    writeLength(writer, value.length, 0x90, 16, 0xdc, 0xdd);
    for (const item of value) writeValue(writer, item);
  } else if (typeof value === "object") {
    // Like JSON.stringify, keys whose value is undefined are left out.
    const entries = Object.entries(value as Record<string, unknown>).filter(([, item]) => item !== undefined);
    writeLength(writer, entries.length, 0x80, 16, 0xde, 0xdf);
    for (const [key, item] of entries) {
      writeValue(writer, key);
      writeValue(writer, item);
    }
  } else {
    throw new MessagePackError(`cannot encode a ${typeof value}`);
  }
}

/** Encode one value. */
export function encode(value: unknown): Uint8Array {
  const writer = new Writer();
  writeValue(writer, value);
  return writer.finish();
}

class Reader {
  offset = 0;
  private readonly view: DataView;

  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  private need(count: number): void {
    if (this.offset + count > this.bytes.length) throw new MessagePackError("truncated MessagePack value");
  }

  u8(): number {
    this.need(1);
    return this.bytes[this.offset++]!;
  }

  u16(): number {
    this.need(2);
    const value = this.view.getUint16(this.offset);
    this.offset += 2;
    return value;
  }

  u32(): number {
    this.need(4);
    const value = this.view.getUint32(this.offset);
    this.offset += 4;
    return value;
  }

  i8(): number {
    this.need(1);
    return this.view.getInt8(this.offset++);
  }

  i16(): number {
    this.need(2);
    const value = this.view.getInt16(this.offset);
    this.offset += 2;
    return value;
  }

  i32(): number {
    this.need(4);
    const value = this.view.getInt32(this.offset);
    this.offset += 4;
    return value;
  }

  u64(): number {
    this.need(8);
    return safe(this.view.getBigUint64((this.offset += 8) - 8));
  }

  i64(): number {
    this.need(8);
    return safe(this.view.getBigInt64((this.offset += 8) - 8));
  }

  f32(): number {
    this.need(4);
    const value = this.view.getFloat32(this.offset);
    this.offset += 4;
    return value;
  }

  f64(): number {
    this.need(8);
    const value = this.view.getFloat64(this.offset);
    this.offset += 8;
    return value;
  }

  take(count: number): Uint8Array {
    this.need(count);
    const slice = this.bytes.subarray(this.offset, this.offset + count);
    this.offset += count;
    return slice;
  }
}

function safe(value: bigint): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) {
    throw new MessagePackError(`integer ${value} is outside the protocol's safe range`);
  }
  return Number(value);
}

function readArray(reader: Reader, length: number, depth: number): unknown[] {
  const items: unknown[] = [];
  for (let index = 0; index < length; index++) items.push(readValue(reader, depth + 1));
  return items;
}

function readMap(reader: Reader, length: number, depth: number): Record<string, unknown> {
  const map: Record<string, unknown> = {};
  for (let index = 0; index < length; index++) {
    const raw = readValue(reader, depth + 1);
    const key = typeof raw === "string" ? raw : String(raw);
    const value = readValue(reader, depth + 1);
    // An own property, like JSON.parse makes it, never the prototype.
    if (key === "__proto__") {
      Object.defineProperty(map, key, { value, enumerable: true, writable: true, configurable: true });
    } else {
      map[key] = value;
    }
  }
  return map;
}

const MAX_DEPTH = 512;

function readValue(reader: Reader, depth: number): unknown {
  if (depth > MAX_DEPTH) throw new MessagePackError("MessagePack value nests too deeply");
  const marker = reader.u8();
  if (marker < 0x80) return marker;
  if (marker >= 0xe0) return marker - 0x100;
  if ((marker & 0xf0) === 0x80) return readMap(reader, marker & 0x0f, depth);
  if ((marker & 0xf0) === 0x90) return readArray(reader, marker & 0x0f, depth);
  if ((marker & 0xe0) === 0xa0) return decoder.decode(reader.take(marker & 0x1f));
  switch (marker) {
    case 0xc0:
      return null;
    case 0xc2:
      return false;
    case 0xc3:
      return true;
    case 0xc4:
      return reader.take(reader.u8()).slice();
    case 0xc5:
      return reader.take(reader.u16()).slice();
    case 0xc6:
      return reader.take(reader.u32()).slice();
    case 0xca:
      return reader.f32();
    case 0xcb:
      return reader.f64();
    case 0xcc:
      return reader.u8();
    case 0xcd:
      return reader.u16();
    case 0xce:
      return reader.u32();
    case 0xcf:
      return reader.u64();
    case 0xd0:
      return reader.i8();
    case 0xd1:
      return reader.i16();
    case 0xd2:
      return reader.i32();
    case 0xd3:
      return reader.i64();
    case 0xd9:
      return decoder.decode(reader.take(reader.u8()));
    case 0xda:
      return decoder.decode(reader.take(reader.u16()));
    case 0xdb:
      return decoder.decode(reader.take(reader.u32()));
    case 0xdc:
      return readArray(reader, reader.u16(), depth);
    case 0xdd:
      return readArray(reader, reader.u32(), depth);
    case 0xde:
      return readMap(reader, reader.u16(), depth);
    case 0xdf:
      return readMap(reader, reader.u32(), depth);
    default:
      throw new MessagePackError(`unsupported MessagePack marker 0x${marker.toString(16)}`);
  }
}

/** Decode one value; bytes after it are ignored, like the daemon does. */
export function decode(bytes: Uint8Array): unknown {
  return readValue(new Reader(bytes), 0);
}
