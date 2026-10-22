// Small v8 values for unit tests.

import type * as T from '@waku/client/v8'
import { summarizeThread, type TaskSummary } from './thread-model'

export function appThread(patch: Partial<T.AppThread> = {}): T.AppThread {
  return {
    id: 'thread',
    projectId: 'project',
    createdBy: 'user',
    creationSource: 'web',
    title: 'New task',
    titleSource: 'placeholder',
    modelSelection: { instance: 'codex' },
    runtimeMode: 'full_access',
    interactionMode: 'default',
    historyOrigin: 'native',
    lineage: { rootThreadId: patch.id ?? 'thread' },
    createdAt: 10_000,
    updatedAt: 10_000,
    ...patch,
  }
}

export function taskSummary(
  patch: Partial<T.AppThread> = {},
  row: Partial<Omit<T.ThreadRow, 'thread'>> = {},
): TaskSummary {
  const thread = appThread(patch)
  return summarizeThread(thread, {
    thread,
    activity: 'idle',
    pendingRequestCount: 0,
    queuedRunCount: 0,
    ...row,
  })
}

export function project(id: string, name: string, path: string): T.Project {
  return { id, name, path, orderKey: id, createdAt: 0, updatedAt: 0 }
}

/** A connection double that records calls and answers from `replies`. */
export function fakeConnection(replies: Record<string, (params: any) => unknown> = {}) {
  const calls: Array<{ method: string; params: any }> = []
  const downloads: Array<{ reference: string; path?: string }> = []
  const connection = {
    calls,
    downloads,
    async call(method: string, params: any) {
      calls.push({ method, params })
      const reply = replies[method]
      if (!reply) throw new Error(`unexpected call ${method}`)
      return reply(params)
    },
    async dispatch(command: T.Command, commandId = `generated-${calls.length}`) {
      return connection.call('orchestration.dispatch', { commandId, command })
    },
    async downloadBlob(reference: string, path?: string) {
      downloads.push({ reference, path })
      const reply = replies['blobDownload']
      return reply ? reply({ reference, path }) : { meta: { type: 'meta', mime: 'text/plain', sha256: '', size: 0 }, bytes: new Uint8Array() }
    },
    async uploadBlob(bytes: Uint8Array, options: { mime: string; name?: string; sha256?: string }) {
      calls.push({ method: 'blobs.put', params: { size: bytes.length, ...options } })
      return { reference: 'waku-blob:abc.txt', size: bytes.length, sha256: options.sha256 ?? '', deduplicated: false }
    },
  }
  return connection
}
