import { CommandOutputStore, type Observable, type ThreadStore } from '@waku/client/v8'
import type * as T from '@waku/client/v8'
import { useEffect, useMemo, useState, useSyncExternalStore } from 'react'
import { useDaemon } from '@/lib/daemon-context'
import {
  shellView,
  taskFromThread,
  type Task,
  type TaskSummary,
} from '@/lib/thread-model'

const NO_STORE = {
  subscribe: () => () => {},
  getVersion: () => -1,
}

/** Re-render when the store changes; returns its version. */
export function useStoreVersion(store: Observable | null | undefined): number {
  const source = store ?? NO_STORE
  return useSyncExternalStore(source.subscribe, source.getVersion, source.getVersion)
}

export interface ShellView {
  ready: boolean
  projects: T.Project[]
  tasks: TaskSummary[]
  rows: Map<string, T.ThreadRow>
  error: Error | undefined
}

/** Projects and active tasks from the `shell` stream. */
export function useShell(): ShellView {
  const { stores } = useDaemon()
  const shell = stores?.shell
  const version = useStoreVersion(shell)
  return useMemo(() => {
    if (!shell) return { ready: false, projects: [], tasks: [], rows: new Map(), error: undefined }
    const view = shellView(shell.state)
    return {
      ready: shell.status === 'ready',
      projects: view.projects,
      tasks: view.tasks,
      rows: shell.state.threads,
      error: shell.error,
    }
    // `version` is the store's change counter.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [shell, version])
}

export interface TaskView {
  task: Task | undefined
  store: ThreadStore | undefined
  loading: boolean
  error: Error | undefined
}

/** One thread from its `thread` stream, while the caller is mounted. */
export function useTask(threadId: string | undefined): TaskView {
  const { stores } = useDaemon()
  const threads = stores?.threads
  const [store, setStore] = useState<ThreadStore | undefined>(() =>
    threadId ? threads?.peek(threadId) : undefined)
  useEffect(() => {
    if (!threads || !threadId) {
      setStore(undefined)
      return
    }
    const acquired = threads.acquire(threadId)
    setStore(acquired)
    return () => threads.release(threadId)
  }, [threadId, threads])
  const active = store?.threadId === threadId ? store : undefined
  const version = useStoreVersion(active)
  const shellVersion = useStoreVersion(stores?.shell)
  return useMemo(() => {
    if (!active) return { task: undefined, store: undefined, loading: Boolean(threadId), error: undefined }
    const row = stores?.shell.state.threads.get(active.threadId)
    return {
      task: taskFromThread(active.state, row),
      store: active,
      loading: active.status === 'loading' && !active.state.thread,
      error: active.error,
    }
    // `version` and `shellVersion` are the stores' change counters.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [active, version, shellVersion, threadId])
}

/** Daemon settings from the `settings` stream. */
export function useSettingsStream() {
  const { stores } = useDaemon()
  const settings = stores?.settings
  const version = useStoreVersion(settings)
  return useMemo(() => ({
    ready: settings?.status === 'ready',
    revision: settings?.state.revision,
    settings: settings?.state.settings,
    store: settings,
    error: settings?.error,
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [settings, version])
}

/** Provider statuses and plan usage from the `providers` stream. */
export function useProviderStatuses() {
  const { stores } = useDaemon()
  const providers = stores?.providers
  const version = useStoreVersion(providers)
  return useMemo(() => ({
    ready: providers?.status === 'ready',
    statuses: providers ? [...providers.state.statuses.values()] : [],
    byId: providers?.state.statuses ?? new Map<string, T.ProviderStatus>(),
    planUsage: providers?.state.planUsage ?? new Map<string, T.PlanUsage | null>(),
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [providers, version])
}

/**
 * A command's output while `enabled` (its row is expanded): the
 * `commandOutput` stream, which the daemon serves from the output hub while
 * the command runs and from the stored item afterwards.
 */
export function useCommandOutput(threadId: string | undefined, itemId: string | undefined, enabled: boolean) {
  const { connection } = useDaemon()
  const [store, setStore] = useState<CommandOutputStore | undefined>()
  useEffect(() => {
    if (!connection || !threadId || !itemId || !enabled) {
      setStore(undefined)
      return
    }
    const next = new CommandOutputStore(connection, threadId, itemId).start()
    setStore(next)
    return () => next.close()
  }, [connection, enabled, itemId, threadId])
  const version = useStoreVersion(store)
  return useMemo(() => store
    ? {
        text: store.state.text,
        running: store.state.running,
        truncated: store.state.truncated,
        exitCode: store.state.exitCode,
        ready: store.status !== 'loading',
        error: store.error,
      }
    : undefined,
  // `version` is the store's change counter.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  [store, version])
}

/** Composer drafts from the `drafts` stream. */
export function useDraftsStream() {
  const { stores } = useDaemon()
  const drafts = stores?.drafts
  const version = useStoreVersion(drafts)
  return useMemo(() => ({
    ready: drafts?.status === 'ready',
    drafts: drafts?.state.drafts ?? {},
    store: drafts,
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [drafts, version])
}
