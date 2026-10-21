import { ThreadStore, type DaemonConnection } from '@waku/client/v8'

/** How long a thread nobody shows stays subscribed, so switching back is instant. */
const LINGER_MS = 30_000

interface Entry {
  store: ThreadStore
  users: number
  timer: ReturnType<typeof setTimeout> | undefined
}

/**
 * The open `thread` subscriptions, shared by every view of a thread and
 * closed a while after the last one lets go.
 */
export class ThreadStores {
  private readonly entries = new Map<string, Entry>()

  constructor(
    private readonly connection: DaemonConnection,
    private readonly lingerMs = LINGER_MS,
  ) {}

  /** The thread's store, subscribing on first use. Pair with {@link release}. */
  acquire(threadId: string): ThreadStore {
    let entry = this.entries.get(threadId)
    if (!entry) {
      entry = { store: new ThreadStore(this.connection, threadId).start(), users: 0, timer: undefined }
      this.entries.set(threadId, entry)
    }
    clearTimeout(entry.timer)
    entry.timer = undefined
    entry.users += 1
    return entry.store
  }

  /** The store when it is already open. */
  peek(threadId: string): ThreadStore | undefined {
    return this.entries.get(threadId)?.store
  }

  release(threadId: string): void {
    const entry = this.entries.get(threadId)
    if (!entry) return
    entry.users = Math.max(0, entry.users - 1)
    if (entry.users > 0) return
    entry.timer = setTimeout(() => {
      if (entry.users > 0) return
      entry.store.close()
      this.entries.delete(threadId)
    }, this.lingerMs)
  }

  close(): void {
    for (const entry of this.entries.values()) {
      clearTimeout(entry.timer)
      entry.store.close()
    }
    this.entries.clear()
  }
}
