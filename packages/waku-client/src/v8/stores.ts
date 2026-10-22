// Observable client state over a `DaemonConnection`: each store owns one
// stream subscription (which survives reconnects), folds its items with the
// pure reducers of `projection.ts` and the ones below, and notifies
// listeners once per microtask. The shape fits React's
// `useSyncExternalStore` (`subscribe` + a `version` snapshot) and anything
// else that polls a version.

import type { DaemonConnection, StreamHandle } from "./connection";
import { CallError } from "./client";
import type * as T from "./generated/types";
import {
  applyCommandOutputItem,
  applyShellItem,
  applyThreadItem,
  createCommandOutputState,
  createShellState,
  createThreadState,
  mergeHistoryPage,
  type CommandOutputState,
  type ShellState,
  type ThreadState,
} from "./projection";

export type StoreStatus = "loading" | "ready" | "ended";

/** A version counter with batched change notifications. */
export class Observable {
  private readonly listeners = new Set<() => void>();
  private scheduled = false;
  private counter = 0;

  /** Bumped on every change; compare it to know whether to re-read. */
  get version(): number {
    return this.counter;
  }

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  readonly getVersion = (): number => this.counter;

  protected changed(): void {
    this.counter += 1;
    if (this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      for (const listener of [...this.listeners]) listener();
    });
  }
}

function endError(end: T.End): Error | undefined {
  switch (end.reason.type) {
    case "error":
      return new CallError(end.reason.error);
    case "notFound":
      return new CallError({ code: "notFound", message: "not found" });
    case "forbidden":
      return new CallError({ code: "forbidden", message: "forbidden" });
    default:
      return undefined;
  }
}

/** One thread: its bounded projection, live events and deltas, and older pages on demand. */
export class ThreadStore extends Observable {
  readonly state: ThreadState = createThreadState();
  status: StoreStatus = "loading";
  error: Error | undefined;
  loadingHistory = false;
  private handle: StreamHandle<"thread"> | undefined;
  private snapshots = 0;

  constructor(
    private readonly connection: DaemonConnection,
    readonly threadId: string,
    private readonly options: { compactTurnItems?: boolean } = {},
  ) {
    super();
  }

  start(): this {
    if (this.handle) return this;
    this.handle = this.connection.subscribe(
      "thread",
      { threadId: this.threadId, compactTurnItems: this.options.compactTurnItems ?? true },
      {
        item: (item) => {
          if (item.type === "snapshot") this.snapshots += 1;
          applyThreadItem(this.state, item);
          if (item.type === "synchronized") this.status = "ready";
          this.changed();
        },
        end: (end) => {
          this.status = "ended";
          this.error = endError(end);
          this.changed();
        },
        reopened: () => {
          this.state.synchronized = false;
          this.changed();
        },
      },
    );
    return this;
  }

  /** Fetch the next older page; a no-op without one or while one loads. */
  async loadOlder(): Promise<void> {
    const cursor = this.state.history;
    if (!cursor || !this.state.hasMoreHistory || this.loadingHistory) return;
    const snapshots = this.snapshots;
    this.loadingHistory = true;
    this.changed();
    try {
      const page = await this.connection.call("threads.historyPage", { threadId: this.threadId, cursor });
      // A snapshot that arrived meanwhile replaced the window and its cursor.
      if (snapshots === this.snapshots) mergeHistoryPage(this.state, page);
    } finally {
      this.loadingHistory = false;
      this.changed();
    }
  }

  close(): void {
    this.handle?.unsubscribe();
    this.handle = undefined;
  }
}

/** The navigation rows: projects and the threads of one location. */
export class ShellStore extends Observable {
  readonly state: ShellState;
  status: StoreStatus = "loading";
  error: Error | undefined;
  private handle: StreamHandle<"shell"> | undefined;

  constructor(
    private readonly connection: DaemonConnection,
    private readonly spec: T.ShellSpec = { location: "active" },
  ) {
    super();
    this.state = createShellState(spec.location ?? "active");
  }

  start(): this {
    if (this.handle) return this;
    this.handle = this.connection.subscribe("shell", this.spec, {
      item: (item) => {
        applyShellItem(this.state, item);
        if (item.type === "synchronized" || (item.type === "snapshot" && item.complete)) this.status = "ready";
        this.changed();
      },
      end: (end) => {
        this.status = "ended";
        this.error = endError(end);
        this.changed();
      },
    });
    return this;
  }

  close(): void {
    this.handle?.unsubscribe();
    this.handle = undefined;
  }
}

export interface SettingsState {
  revision: number | undefined;
  settings: T.Settings | undefined;
}

export function applySettingsItem(state: SettingsState, item: T.SettingsItem): void {
  if (item.type === "snapshot" || item.type === "updated") {
    state.revision = item.revision;
    state.settings = item.settings;
  }
}

export interface ProvidersState {
  revision: number | undefined;
  statuses: Map<string, T.ProviderStatus>;
  /** Plan usage pushed by `providers.planUsage` fetches; `null` = none. */
  planUsage: Map<string, T.PlanUsage | null>;
}

export function createProvidersState(): ProvidersState {
  return { revision: undefined, statuses: new Map(), planUsage: new Map() };
}

export function applyProvidersItem(state: ProvidersState, item: T.ProvidersItem): void {
  switch (item.type) {
    case "snapshot":
      state.revision = item.revision;
      state.statuses = new Map(item.statuses.map((status) => [status.instanceId, status]));
      return;
    case "statusUpdated":
      state.revision = item.revision;
      state.statuses.set(item.status.instanceId, item.status);
      return;
    case "planUsageUpdated":
      state.planUsage.set(item.instanceId, item.usage ?? null);
      return;
    default:
      return;
  }
}

export interface DraftsState {
  revision: number | undefined;
  drafts: T.ComposerDrafts;
}

function draftEmpty(draft: T.ComposerDraft | undefined): boolean {
  return !draft || !(draft.text?.length || draft.attachments?.length);
}

/** Applies one keyed change: an absent or empty draft removes it. */
export function applyDraftChange(drafts: T.ComposerDrafts, change: T.DraftChange): void {
  const target = change.target;
  let entries: { [key: string]: T.ComposerDraft };
  let id: string;
  if (target.type === "newThread") {
    entries = drafts.newThreads ??= {};
    id = target.projectId;
  } else if (target.type === "thread") {
    entries = drafts.threads ??= {};
    id = target.threadId;
  } else {
    return;
  }
  if (draftEmpty(change.draft)) delete entries[id];
  else entries[id] = change.draft!;
}

export function applyDraftsItem(state: DraftsState, item: T.DraftsItem): void {
  if (item.type === "snapshot") {
    state.revision = item.revision;
    state.drafts = structuredClone(item.drafts);
  } else if (item.type === "changed") {
    state.revision = item.revision;
    for (const change of item.changes) applyDraftChange(state.drafts, change);
  }
}

/** A stream whose state is one small value, replaced or patched by revision. */
class ValueStore<S extends "settings" | "providers" | "drafts", V> extends Observable {
  status: StoreStatus = "loading";
  error: Error | undefined;
  private handle: StreamHandle<S> | undefined;

  constructor(
    protected readonly connection: DaemonConnection,
    private readonly stream: S,
    readonly state: V,
    private readonly reduce: (state: V, item: import("./generated/streams").StreamItemOf<S>) => void,
  ) {
    super();
  }

  start(): this {
    if (this.handle) return this;
    this.handle = this.connection.subscribe(this.stream, {} as never, {
      item: (item) => {
        this.reduce(this.state, item);
        if (item.type !== "unknown") this.status = "ready";
        this.changed();
      },
      end: (end) => {
        this.status = "ended";
        this.error = endError(end);
        this.changed();
      },
    });
    return this;
  }

  close(): void {
    this.handle?.unsubscribe();
    this.handle = undefined;
  }
}

export class SettingsStore extends ValueStore<"settings", SettingsState> {
  constructor(connection: DaemonConnection) {
    super(connection, "settings", { revision: undefined, settings: undefined }, applySettingsItem);
  }

  /** `settings.update` with this revision as the base; the stream carries the result. */
  async update(patch: T.SettingsPatch): Promise<T.SettingsSnapshot> {
    const result = await this.connection.call("settings.update", { patch });
    applySettingsItem(this.state, { type: "updated", ...result });
    this.changed();
    return result;
  }
}

export class ProvidersStore extends ValueStore<"providers", ProvidersState> {
  constructor(connection: DaemonConnection) {
    super(connection, "providers", createProvidersState(), applyProvidersItem);
  }
}

export class DraftsStore extends ValueStore<"drafts", DraftsState> {
  constructor(connection: DaemonConnection) {
    super(connection, "drafts", { revision: undefined, drafts: {} }, applyDraftsItem);
  }

  /** Apply changes locally at once, then on the daemon (keyed, so no base revision). */
  async apply(changes: T.DraftChange[]): Promise<void> {
    if (!changes.length) return;
    for (const change of changes) applyDraftChange(this.state.drafts, change);
    this.changed();
    const result = await this.connection.call("drafts.apply", { changes });
    this.state.revision = result.revision;
  }
}

/** One command's output while its row is expanded. */
export class CommandOutputStore extends Observable {
  readonly state: CommandOutputState = createCommandOutputState();
  status: StoreStatus = "loading";
  error: Error | undefined;
  private handle: StreamHandle<"commandOutput"> | undefined;

  constructor(
    private readonly connection: DaemonConnection,
    readonly threadId: string,
    readonly itemId: string,
  ) {
    super();
  }

  start(): this {
    if (this.handle) return this;
    this.handle = this.connection.subscribe(
      "commandOutput",
      { threadId: this.threadId, itemId: this.itemId },
      {
        item: (item) => {
          applyCommandOutputItem(this.state, item);
          this.status = "ready";
          this.changed();
        },
        end: (end) => {
          this.status = "ended";
          this.error = endError(end);
          this.changed();
        },
      },
    );
    return this;
  }

  close(): void {
    this.handle?.unsubscribe();
    this.handle = undefined;
  }
}
