// Binary frames (RFC 0003 §1.4): a 12-byte header (body length, kind, flags,
// reserved, channel) and a body. On WebSocket one binary message is exactly
// one frame; text messages are JSON MSG bodies without a header.

import {
  FRAME_FLAG_FIN,
  FRAME_FLAG_ZSTD,
  FRAME_HEADER_BYTES,
  FRAME_KIND_DATA,
  FRAME_KIND_MSG,
  MAX_DATA_CHUNK_BYTES,
  MAX_MESSAGE_BYTES,
} from "./generated/constants";

export type Frame =
  | { kind: "msg"; body: Uint8Array }
  | { kind: "data"; channel: number; fin: boolean; body: Uint8Array };

export interface FrameLimits {
  maxMessageBytes: number;
  maxDataBytes: number;
}

export const DEFAULT_FRAME_LIMITS: FrameLimits = {
  maxMessageBytes: MAX_MESSAGE_BYTES,
  maxDataBytes: MAX_DATA_CHUNK_BYTES,
};

/** A frame the receiver must refuse; `code` is the `goodbye` code to send. */
export class FrameError extends Error {
  constructor(
    message: string,
    readonly code: "frameTooLarge" | "protocolError",
  ) {
    super(message);
  }
}

function encodeFrame(kind: number, flags: number, channel: number, body: Uint8Array): Uint8Array {
  const frame = new Uint8Array(FRAME_HEADER_BYTES + body.length);
  const view = new DataView(frame.buffer);
  view.setUint32(0, body.length);
  frame[4] = kind;
  frame[5] = flags;
  view.setUint32(8, channel);
  frame.set(body, FRAME_HEADER_BYTES);
  return frame;
}

export function encodeMsgFrame(body: Uint8Array): Uint8Array {
  return encodeFrame(FRAME_KIND_MSG, 0, 0, body);
}

export function encodeDataFrame(channel: number, fin: boolean, body: Uint8Array): Uint8Array {
  return encodeFrame(FRAME_KIND_DATA, fin ? FRAME_FLAG_FIN : 0, channel, body);
}

/**
 * Parse one binary WebSocket message. `null` is a frame of a kind this
 * client does not know, to be ignored.
 */
export function parseFrame(message: Uint8Array, limits: FrameLimits = DEFAULT_FRAME_LIMITS): Frame | null {
  if (message.length < FRAME_HEADER_BYTES) throw new FrameError("binary message is shorter than a frame header", "protocolError");
  const view = new DataView(message.buffer, message.byteOffset, message.byteLength);
  const length = view.getUint32(0);
  const kind = message[4]!;
  const flags = message[5]!;
  const channel = view.getUint32(8);
  if (message[6] !== 0 || message[7] !== 0) throw new FrameError("frame header has non-zero reserved bytes", "protocolError");
  if (length !== message.length - FRAME_HEADER_BYTES) throw new FrameError("frame length does not match the message", "protocolError");
  if ((flags & ~(FRAME_FLAG_FIN | FRAME_FLAG_ZSTD)) !== 0) throw new FrameError(`unknown frame flags ${flags}`, "protocolError");
  const body = message.subarray(FRAME_HEADER_BYTES);
  if (kind === FRAME_KIND_MSG) {
    if (flags !== 0) throw new FrameError("MSG frame flags are not supported", "protocolError");
    if (channel !== 0) throw new FrameError("MSG frame on a channel", "protocolError");
    if (length > limits.maxMessageBytes) throw new FrameError(`MSG frame of ${length} bytes`, "frameTooLarge");
    return { kind: "msg", body };
  }
  if (kind === FRAME_KIND_DATA) {
    if ((flags & FRAME_FLAG_ZSTD) !== 0) throw new FrameError("DATA frames are never compressed", "protocolError");
    if (channel === 0) throw new FrameError("DATA frame on channel 0", "protocolError");
    if (length > limits.maxDataBytes) throw new FrameError(`DATA frame of ${length} bytes`, "frameTooLarge");
    return { kind: "data", channel, fin: (flags & FRAME_FLAG_FIN) !== 0, body };
  }
  if (length > limits.maxMessageBytes) throw new FrameError(`frame of ${length} bytes`, "frameTooLarge");
  return null;
}
