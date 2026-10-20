// The stream reducers against what the daemon sent and what it holds
// afterwards (`fixtures/reducers.json`, recorded by waku-core's
// `reducer_fixture` test), plus the delta and history rules.

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { itemNormalizers } from "./generated/decode";
import type * as T from "./generated/types";
import {
  applyCommandOutputItem,
  applyDelta,
  applyShellItem,
  applyThreadItem,
  createCommandOutputState,
  createShellState,
  createThreadState,
  mergeHistoryPage,
  orderedTurnItems,
  threadProjectionOf,
} from "./projection";

interface Fixture {
  thread: { items: unknown[]; expected: T.ThreadProjection };
  shell: { items: unknown[]; expectedThreads: T.ThreadRow[] };
}

const fixture = JSON.parse(readFileSync(join(import.meta.dir, "fixtures/reducers.json"), "utf8")) as Fixture;

const STORE = "00000000-0000-0000-0000-000000000000";

function byId<V extends { id: string }>(rows: V[] | undefined): Record<string, V> {
  return Object.fromEntries((rows ?? []).map((row) => [row.id, row]));
}

describe("thread reducer", () => {
  test("snapshot + events + deltas equals the daemon's projection", () => {
    const state = createThreadState();
    for (const raw of fixture.thread.items) applyThreadItem(state, itemNormalizers.thread(structuredClone(raw)));
    const actual = threadProjectionOf(state)!;
    const expected = fixture.thread.expected;
    expect(actual.thread).toEqual(expected.thread);
    // The window the daemon would send now is exactly what the client holds.
    expect(actual.turnItems).toEqual(expected.turnItems);
    // Every control row the daemon would send is held, unchanged.
    for (const name of ["runs", "runAttempts", "nodes", "runtimeRequests", "plans", "checkpoints", "messages"] as const) {
      const held = byId(actual[name] as Array<{ id: string }> | undefined);
      for (const row of (expected[name] ?? []) as Array<{ id: string }>) expect(held[row.id]).toEqual(row);
    }
    // The cursor followed the events.
    const last = fixture.thread.items.at(-1) as { type: string; seq: number };
    expect(last.type).toBe("event");
    expect(state.cursor).toEqual({ type: "sequence", store: STORE, seq: last.seq });
    expect(state.synchronized).toBe(true);
  });

  test("deltas apply in streamRev order and wait for a snapshot after a gap", () => {
    const state = createThreadState();
    const items = fixture.thread.items.map((raw) => itemNormalizers.thread(structuredClone(raw)));
    const deltas = items.filter((item): item is Extract<T.ThreadItem, { type: "delta" }> => item.type === "delta");
    expect(deltas.map((delta) => delta.rev)).toEqual([3, 4, 6]);
    for (const item of items) {
      if (item === deltas[0]) break;
      applyThreadItem(state, item);
    }
    const answer = () => state.turnItems.get(deltas[0]!.itemId)!;
    const text = () => (answer().kind as { text: string }).text;
    expect(text()).toBe("He");
    expect(applyDelta(state, deltas[0]!)).toBe(true);
    expect(applyDelta(state, deltas[0]!)).toBe(false); // already applied
    expect(applyDelta(state, deltas[1]!)).toBe(true);
    expect([text(), answer().streamRev]).toEqual(["Hello,", 4]);
    expect(applyDelta(state, deltas[2]!)).toBe(false); // rev 6 after 4: a gap
    expect(text()).toBe("Hello,");
  });

  test("history pages merge older items without replacing newer ones", () => {
    const state = createThreadState();
    for (const raw of fixture.thread.items) applyThreadItem(state, itemNormalizers.thread(structuredClone(raw)));
    const [first] = orderedTurnItems(state);
    const older: T.TurnItem = { ...first!, id: "00000000-0000-0000-0000-0000000000aa", position: 1 };
    const stale: T.TurnItem = { ...first!, title: "stale" };
    mergeHistoryPage(state, { items: [older, stale], runs: [], next: "h1.next" });
    expect(orderedTurnItems(state)[0]).toEqual(older);
    expect(state.turnItems.get(first!.id)).toEqual(first);
    expect([state.history, state.hasMoreHistory]).toEqual(["h1.next", true]);
    mergeHistoryPage(state, { items: [] });
    expect(state.hasMoreHistory).toBe(false);
  });

  test("unknown items and events are skipped but still advance the cursor", () => {
    const state = createThreadState();
    applyThreadItem(state, itemNormalizers.thread(structuredClone(fixture.thread.items[0])));
    const before = state.cursor as { seq: number };
    applyThreadItem(state, itemNormalizers.thread({ type: "event", seq: before.seq + 7, event: { type: "galaxy.updated", id: "x" } }));
    expect((state.cursor as { seq: number }).seq).toBe(before.seq + 7);
    applyThreadItem(state, itemNormalizers.thread({ type: "hologram" }));
    expect((state.cursor as { seq: number }).seq).toBe(before.seq + 7);
  });
});

describe("shell reducer", () => {
  test("snapshot + deltas equals the daemon's active rows", () => {
    const state = createShellState();
    for (const raw of fixture.shell.items) applyShellItem(state, itemNormalizers.shell(structuredClone(raw)));
    expect(state.complete && state.synchronized).toBe(true);
    const expected = Object.fromEntries(fixture.shell.expectedThreads.map((row) => [row.thread.id, row]));
    expect(Object.fromEntries(state.threads)).toEqual(expected);
  });

  test("split snapshots are applied whole", () => {
    const state = createShellState();
    const cursor: T.StreamCursor = { type: "sequence", store: STORE, seq: 3 };
    const [row] = fixture.shell.expectedThreads;
    applyShellItem(state, { type: "snapshot", cursor, threads: [row!], complete: false });
    expect(state.threads.size).toBe(0);
    applyShellItem(state, { type: "snapshot", cursor, complete: true });
    expect(state.threads.size).toBe(1);
    applyShellItem(state, { type: "threadUpdated", seq: 9, row: row!, location: "archived" });
    expect(state.threads.size).toBe(0);
    expect(state.cursor).toEqual({ ...cursor, seq: 9 });
  });
});

describe("command output reducer", () => {
  test("replace, append and finished", () => {
    const state = createCommandOutputState();
    applyCommandOutputItem(state, { type: "replace", text: "a", truncated: false, running: true });
    applyCommandOutputItem(state, { type: "append", text: "b" });
    applyCommandOutputItem(state, { type: "replace", text: "…tail", truncated: true, running: true });
    applyCommandOutputItem(state, { type: "finished", exitCode: 2, truncated: true });
    expect(state).toEqual({ text: "…tail", truncated: true, running: false, exitCode: 2 });
  });
});
