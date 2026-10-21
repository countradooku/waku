// Client-side reducers for the v8 orchestration streams (RFC 0003 §5.4):
// a thread projection fed by `thread` items and history pages, the shell's
// navigation rows fed by `shell` items, and a command's output fed by
// `commandOutput` frames.
//
// Applying an event is an upsert by entity id (RFC 0001 rule 4), so
// replays and duplicates are harmless. Unknown items and events still
// advance the cursor. Live `delta`s apply only in `streamRev` order: a gap
// is ignored until the next snapshot of the item replaces it.

import type * as T from "./generated/types";

type Id = string;

/** A thread as one client holds it. */
export interface ThreadState {
  /** Where the client stands; resubscribe with it after a disconnect. */
  cursor: T.StreamCursor | undefined;
  /** A snapshot or replay has been followed by `synchronized`. */
  synchronized: boolean;
  thread: T.AppThread | undefined;
  runs: Map<Id, T.Run>;
  runAttempts: Map<Id, T.RunAttempt>;
  nodes: Map<Id, T.ExecutionNode>;
  subagents: Map<Id, T.Subagent>;
  providerSessions: Map<Id, T.ProviderSession>;
  providerThreads: Map<Id, T.ProviderThread>;
  providerTurns: Map<Id, T.ProviderTurn>;
  runtimeRequests: Map<Id, T.RuntimeRequest>;
  messages: Map<Id, T.Message>;
  plans: Map<Id, T.Plan>;
  checkpointScopes: Map<Id, T.CheckpointScope>;
  checkpoints: Map<Id, T.Checkpoint>;
  contextTransfers: Map<Id, T.ContextTransfer>;
  contextHandoffs: Map<Id, T.ContextHandoff>;
  /** Turn items by id; see {@link orderedTurnItems}. */
  turnItems: Map<Id, T.TurnItem>;
  /** Pass to `threads.historyPage` for older items. */
  history: string | undefined;
  hasMoreHistory: boolean;
  budgetExceeded: boolean;
}

export function createThreadState(): ThreadState {
  return {
    cursor: undefined,
    synchronized: false,
    thread: undefined,
    runs: new Map(),
    runAttempts: new Map(),
    nodes: new Map(),
    subagents: new Map(),
    providerSessions: new Map(),
    providerThreads: new Map(),
    providerTurns: new Map(),
    runtimeRequests: new Map(),
    messages: new Map(),
    plans: new Map(),
    checkpointScopes: new Map(),
    checkpoints: new Map(),
    contextTransfers: new Map(),
    contextHandoffs: new Map(),
    turnItems: new Map(),
    history: undefined,
    hasMoreHistory: false,
    budgetExceeded: false,
  };
}

function byId<V extends { id: Id }>(rows: V[] | undefined): Map<Id, V> {
  return new Map((rows ?? []).map((row) => [row.id, row]));
}

/** The entity collections of a projection, by their wire names. */
const COLLECTIONS = [
  "runs",
  "runAttempts",
  "nodes",
  "subagents",
  "providerSessions",
  "providerThreads",
  "providerTurns",
  "runtimeRequests",
  "messages",
  "plans",
  "checkpointScopes",
  "checkpoints",
  "contextTransfers",
  "contextHandoffs",
  "turnItems",
] as const;

export type CollectionName = (typeof COLLECTIONS)[number];

function advance(state: ThreadState, seq: number): void {
  if (state.cursor?.type === "sequence") state.cursor = { ...state.cursor, seq: Math.max(state.cursor.seq, seq) };
}

/** Replaces the state with a bounded snapshot. */
export function applyThreadSnapshot(state: ThreadState, snapshot: T.ThreadSnapshot): void {
  const projection = snapshot.projection;
  Object.assign(state, createThreadState(), {
    cursor: snapshot.cursor,
    thread: projection.thread,
    history: snapshot.history,
    hasMoreHistory: snapshot.hasMoreHistory,
    budgetExceeded: snapshot.budgetExceeded ?? false,
  });
  for (const name of COLLECTIONS) {
    state[name] = byId((projection as Record<CollectionName, Array<{ id: Id }> | undefined>)[name]) as never;
  }
}

/** Applies one committed event (an upsert by entity id). */
export function applyThreadEvent(state: ThreadState, event: T.DomainEvent): void {
  switch (event.type) {
    case "thread.updated":
      if (!state.thread || event.thread.id === state.thread.id) state.thread = event.thread;
      return;
    case "run.updated": {
      const { type: _, ...run } = event;
      state.runs.set(run.id, run as T.Run);
      return;
    }
    case "run_attempt.updated": {
      const { type: _, ...attempt } = event;
      state.runAttempts.set(attempt.id, attempt as T.RunAttempt);
      return;
    }
    case "node.updated": {
      const { type: _, ...node } = event;
      state.nodes.set(node.id, node as T.ExecutionNode);
      return;
    }
    case "subagent.updated": {
      const { type: _, ...subagent } = event;
      state.subagents.set(subagent.id, subagent as T.Subagent);
      return;
    }
    case "provider_session.updated": {
      const { type: _, ...session } = event;
      state.providerSessions.set(session.id, session as T.ProviderSession);
      return;
    }
    case "provider_thread.updated": {
      const { type: _, ...providerThread } = event;
      // A provider thread that left this thread is dropped, as on the daemon.
      if (state.thread && providerThread.appThreadId !== state.thread.id) state.providerThreads.delete(providerThread.id);
      else state.providerThreads.set(providerThread.id, providerThread as T.ProviderThread);
      return;
    }
    case "provider_turn.updated": {
      const { type: _, ...turn } = event;
      state.providerTurns.set(turn.id, turn as T.ProviderTurn);
      return;
    }
    case "runtime_request.updated": {
      const { type: _, ...request } = event;
      state.runtimeRequests.set(request.id, request as T.RuntimeRequest);
      return;
    }
    case "message.updated": {
      const { type: _, ...message } = event;
      state.messages.set(message.id, message as T.Message);
      return;
    }
    case "plan.updated": {
      const { type: _, ...plan } = event;
      state.plans.set(plan.id, plan as T.Plan);
      return;
    }
    case "turn_item.updated": {
      // A sequenced snapshot replaces the item and its live text.
      const { type: _, ...item } = event;
      state.turnItems.set(item.id, item as T.TurnItem);
      return;
    }
    case "checkpoint_scope.updated": {
      const { type: _, ...scope } = event;
      state.checkpointScopes.set(scope.id, scope as T.CheckpointScope);
      return;
    }
    case "checkpoint.updated": {
      const { type: _, ...checkpoint } = event;
      state.checkpoints.set(checkpoint.id, checkpoint as T.Checkpoint);
      return;
    }
    case "context_transfer.updated": {
      const { type: _, ...transfer } = event;
      state.contextTransfers.set(transfer.id, transfer as T.ContextTransfer);
      return;
    }
    case "context_handoff.updated": {
      const { type: _, ...handoff } = event;
      state.contextHandoffs.set(handoff.id, handoff as T.ContextHandoff);
      return;
    }
    default:
      // project.updated does not belong to a thread; unknown events are
      // skipped (the cursor still advances).
      return;
  }
}

/**
 * Applies a live delta when it is the next one of its item (`rev ==
 * streamRev + 1`). Returns whether it applied; a gap is ignored until the
 * item's next snapshot.
 */
export function applyDelta(state: ThreadState, delta: Extract<T.ThreadItem, { type: "delta" }>): boolean {
  const item = state.turnItems.get(delta.itemId);
  if (!item || delta.rev !== (item.streamRev ?? 0) + 1) return false;
  const kind = item.kind;
  let next: T.TurnItemKind;
  if (delta.channel === "reasoning" && kind.type === "reasoning") {
    next = { ...kind, text: kind.text + delta.text };
  } else if (delta.channel === "text" && kind.type === "assistant_message") {
    next = { ...kind, text: kind.text + delta.text };
  } else if (delta.channel === "text" && kind.type === "proposed_plan") {
    next = { ...kind, markdown: kind.markdown + delta.text };
  } else {
    return false;
  }
  state.turnItems.set(item.id, { ...item, kind: next, streamRev: delta.rev });
  return true;
}

/** Applies one `thread` stream item. */
export function applyThreadItem(state: ThreadState, item: T.ThreadItem): void {
  switch (item.type) {
    case "snapshot": {
      const { type: _, ...snapshot } = item;
      applyThreadSnapshot(state, snapshot as T.ThreadSnapshot);
      return;
    }
    case "event":
      applyThreadEvent(state, item.event);
      advance(state, item.seq);
      return;
    case "delta":
      applyDelta(state, item);
      return;
    case "synchronized":
      state.cursor = item.cursor;
      state.synchronized = true;
      return;
    default:
      return;
  }
}

/** Merges an older page from `threads.historyPage`. */
export function mergeHistoryPage(state: ThreadState, page: T.HistoryPage): void {
  for (const item of page.items) {
    // A live update may already be newer; keep it.
    if (!state.turnItems.has(item.id)) state.turnItems.set(item.id, item);
  }
  for (const run of page.runs ?? []) {
    if (!state.runs.has(run.id)) state.runs.set(run.id, run);
  }
  // The paged-in runs' checkpoints, so they can be rewound to or diffed.
  for (const scope of page.checkpointScopes ?? []) {
    if (!state.checkpointScopes.has(scope.id)) state.checkpointScopes.set(scope.id, scope);
  }
  for (const checkpoint of page.checkpoints ?? []) {
    if (!state.checkpoints.has(checkpoint.id)) state.checkpoints.set(checkpoint.id, checkpoint);
  }
  state.history = page.next;
  state.hasMoreHistory = page.next !== undefined;
}

/** Turn items in display order. */
export function orderedTurnItems(state: ThreadState): T.TurnItem[] {
  return [...state.turnItems.values()].sort((left, right) => left.position - right.position);
}

/** The state as a projection, collections sorted by id. */
export function threadProjectionOf(state: ThreadState): T.ThreadProjection | undefined {
  if (!state.thread) return undefined;
  const projection: Record<string, unknown> = { thread: state.thread };
  for (const name of COLLECTIONS) {
    const rows = [...(state[name] as Map<Id, { id: Id }>).values()];
    if (rows.length === 0) continue;
    projection[name] =
      name === "turnItems"
        ? (rows as T.TurnItem[]).sort((left, right) => left.position - right.position)
        : rows.sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
  }
  return projection as T.ThreadProjection;
}

/** The shell's navigation rows. */
export interface ShellState {
  location: T.ShellLocation;
  cursor: T.StreamCursor | undefined;
  synchronized: boolean;
  /** The last snapshot has arrived whole. */
  complete: boolean;
  projects: Map<Id, T.Project>;
  threads: Map<Id, T.ThreadRow>;
  /** Parts of a split snapshot still arriving. */
  partial: { projects: Map<Id, T.Project>; threads: Map<Id, T.ThreadRow> } | undefined;
}

export function createShellState(location: T.ShellLocation = "active"): ShellState {
  return {
    location,
    cursor: undefined,
    synchronized: false,
    complete: false,
    projects: new Map(),
    threads: new Map(),
    partial: undefined,
  };
}

function shellAdvance(state: ShellState, seq: number): void {
  if (state.cursor?.type === "sequence") state.cursor = { ...state.cursor, seq: Math.max(state.cursor.seq, seq) };
}

/** Applies one `shell` stream item. */
export function applyShellItem(state: ShellState, item: T.ShellItem): void {
  switch (item.type) {
    case "snapshot": {
      const partial = state.partial ?? { projects: new Map(), threads: new Map() };
      for (const project of item.projects ?? []) partial.projects.set(project.id, project);
      for (const row of item.threads ?? []) partial.threads.set(row.thread.id, row);
      state.cursor = item.cursor;
      if (item.complete) {
        state.projects = partial.projects;
        state.threads = partial.threads;
        state.partial = undefined;
        state.complete = true;
      } else {
        state.partial = partial;
      }
      return;
    }
    case "projectUpdated":
      state.projects.set(item.row.id, item.row);
      shellAdvance(state, item.seq);
      return;
    case "projectRemoved":
      state.projects.delete(item.projectId);
      shellAdvance(state, item.seq);
      return;
    case "threadUpdated":
      // A row that moved to another location leaves this one.
      if (item.location === state.location) state.threads.set(item.row.thread.id, item.row);
      else state.threads.delete(item.row.thread.id);
      shellAdvance(state, item.seq);
      return;
    case "threadRemoved":
      state.threads.delete(item.threadId);
      shellAdvance(state, item.seq);
      return;
    case "synchronized":
      state.cursor = item.cursor;
      state.synchronized = true;
      return;
    default:
      return;
  }
}

/** One command's output, as an expanded row shows it. */
export interface CommandOutputState {
  text: string;
  truncated: boolean;
  running: boolean;
  exitCode: number | undefined;
}

export function createCommandOutputState(): CommandOutputState {
  return { text: "", truncated: false, running: true, exitCode: undefined };
}

/** Applies one `commandOutput` frame. */
export function applyCommandOutputItem(state: CommandOutputState, item: T.CommandOutputItem): void {
  switch (item.type) {
    case "replace":
      state.text = item.text;
      state.truncated = item.truncated;
      state.running = item.running;
      return;
    case "append":
      state.text += item.text;
      return;
    case "finished":
      state.running = false;
      state.exitCode = item.exitCode;
      state.truncated = state.truncated || item.truncated;
      return;
    default:
      return;
  }
}
