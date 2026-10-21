// Everything the web changes about threads and projects goes through
// `orchestration.dispatch` (RFC 0003 §5.3). The daemon owns state, so the
// streams carry every result back; the reply's read-your-writes guarantee
// (§2.3) means the shell and thread stores already hold it when a dispatch
// resolves.

import type * as T from '@waku/client/v8'
import { newId, type DaemonConnection, type ThreadStore } from '@waku/client/v8'
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from 'react'
import {
  commandIds,
  compactSelection,
  dispatchMode,
  firstMessageMode,
  importedThreadCreate,
  messageDispatch,
  threadCreate,
  type MessageInput,
  type NewTask,
  type SendIntent,
} from './commands'
import { requireConnection, useDaemon, type DaemonStores } from './daemon-context'
import { loadDaemonInfo, normalizeProjectPath, projectNameFromPath } from './daemon-api'
import { PROJECTLESS_NAME } from './project-presentation'
import type { Task } from './thread-model'

interface TaskActions {
  /** Thread ids with a fork in flight, and the run it forks from. */
  forking: Record<string, string | undefined>
  /** Thread ids with a rewind in flight, and the run it rewinds to. */
  rewinding: Record<string, string | undefined>
  addProject: (path: string, name?: string) => Promise<T.Project>
  projectlessProject: () => Promise<T.Project>
  startTask: (task: Omit<NewTask, 'threadId'>, input: MessageInput) => Promise<string>
  importSession: (summary: T.ProviderSessionSummary, projectId: string, runtimeMode: T.RuntimeMode) => Promise<string>
  sendMessage: (task: Task, input: MessageInput, intent?: SendIntent) => Promise<void>
  interrupt: (task: Task) => Promise<void>
  respondApproval: (threadId: string, requestId: string, optionId: string) => Promise<void>
  respondUserInput: (threadId: string, requestId: string, answers: T.UserInputAnswer[]) => Promise<void>
  dismissUserInput: (threadId: string, requestId: string) => Promise<void>
  cancelQueued: (threadId: string, runId: string) => Promise<void>
  renameTask: (threadId: string, title: string) => Promise<void>
  archiveTask: (threadId: string) => Promise<void>
  deleteTask: (threadId: string) => Promise<void>
  visitTask: (threadId: string) => Promise<void>
  setSelection: (threadId: string, selection: T.ModelSelection) => Promise<void>
  setRuntimeMode: (threadId: string, mode: T.RuntimeMode) => Promise<void>
  setInteractionMode: (threadId: string, mode: T.InteractionMode) => Promise<void>
  forkFromRun: (task: Task, runId: string) => Promise<string>
  rewindToRun: (task: Task, runId: string, input: MessageInput) => Promise<void>
  stopSubagent: (threadId: string, runId: string, subagentId: string) => Promise<void>
}

const TaskActionsContext = createContext<TaskActions | null>(null)

/** Waits until the thread's pending rollback settles; throws its failure. */
export function waitForRollback(store: ThreadStore, timeoutMs = 120_000): Promise<void> {
  return new Promise((resolve, reject) => {
    let unsubscribe = () => {}
    const timer = setTimeout(() => {
      unsubscribe()
      reject(new Error('The rewind did not finish in time'))
    }, timeoutMs)
    const check = () => {
      const rollback = store.state.thread?.rollback
      if (rollback?.failure) {
        clearTimeout(timer)
        unsubscribe()
        reject(new Error(rollback.failure))
      } else if (!rollback) {
        clearTimeout(timer)
        unsubscribe()
        resolve()
      }
    }
    unsubscribe = store.subscribe(check)
    check()
  })
}

function withEntry(
  setter: (update: (current: Record<string, string | undefined>) => Record<string, string | undefined>) => void,
  threadId: string,
  value: string | undefined,
) {
  setter((current) => {
    const next = { ...current }
    if (value === undefined) delete next[threadId]
    else next[threadId] = value
    return next
  })
}

export function createTaskActions(
  connection: DaemonConnection,
  stores: DaemonStores | null,
): Omit<TaskActions, 'forking' | 'rewinding' | 'forkFromRun' | 'rewindToRun'> & {
  forkFromRun: (task: Task, runId: string) => Promise<string>
  rewindToRun: (task: Task, runId: string, input: MessageInput) => Promise<void>
} {
  const dispatch = (command: T.Command, commandId?: string) => connection.dispatch(command, commandId)

  const findProject = (path: string) =>
    stores ? [...stores.shell.state.projects.values()].find((project) =>
      project.path === path && project.removedAt === undefined) : undefined

  const addProject = async (rawPath: string, name?: string): Promise<T.Project> => {
    const path = normalizeProjectPath(rawPath)
    const existing = findProject(path)
    if (existing) return existing
    const projectId = newId()
    const command: T.Command = {
      type: 'project.create',
      projectId,
      path,
      name: name ?? projectNameFromPath(path),
    }
    await dispatch(command, commandIds.createProject(projectId))
    const created = stores?.shell.state.projects.get(projectId)
    if (created) return created
    const now = Date.now()
    return { id: projectId, name: command.name, path, orderKey: '', createdAt: now, updatedAt: now }
  }

  return {
    addProject,

    // There is no scratch-project command yet (RFC 0003 `projectCreateScratch`
    // is not served), so "No project" is one project at the daemon's
    // projectless folder.
    async projectlessProject() {
      const info = await loadDaemonInfo(connection)
      return addProject(info.projectlessRoot ?? info.defaultCwd, PROJECTLESS_NAME)
    },

    async startTask(task, input) {
      const threadId = newId()
      const messageId = newId()
      const spec = { ...task, threadId }
      await dispatch(threadCreate(spec), commandIds.createThread(threadId))
      await dispatch(
        messageDispatch(threadId, messageId, input, firstMessageMode(spec)),
        commandIds.dispatchMessage(messageId),
      )
      return threadId
    },

    // `threadImport` is not served yet; a thread adopts a listed provider
    // conversation through `thread.create{importedNativeThread}` (RFC 0003
    // §5.2, "Changed by merging the catalog into the #55 cut-over").
    async importSession(summary, projectId, runtimeMode) {
      const threadId = newId()
      await dispatch(importedThreadCreate(threadId, projectId, summary, runtimeMode), commandIds.createThread(threadId))
      return threadId
    },

    async sendMessage(task, input, intent = 'send') {
      const messageId = newId()
      await dispatch(
        messageDispatch(task.id, messageId, input, dispatchMode(task, intent)),
        commandIds.dispatchMessage(messageId),
      )
    },

    async interrupt(task) {
      const run = task.activeRun
      if (!run) return
      await dispatch({ type: 'run.interrupt', threadId: task.id, runId: run.id })
    },

    async respondApproval(threadId, requestId, optionId) {
      await dispatch({
        type: 'runtime_request.respond',
        threadId,
        requestId,
        response: { type: 'approval', optionId },
      })
    },

    async respondUserInput(threadId, requestId, answers) {
      await dispatch({
        type: 'runtime_request.respond',
        threadId,
        requestId,
        response: { type: 'user_input', answers },
      })
    },

    async dismissUserInput(threadId, requestId) {
      await dispatch({ type: 'user_input.dismiss', threadId, requestId })
    },

    async cancelQueued(threadId, runId) {
      await dispatch({ type: 'queued_run.cancel', threadId, runId })
    },

    async renameTask(threadId, title) {
      await dispatch({ type: 'thread.rename', threadId, title })
    },

    async archiveTask(threadId) {
      await dispatch({ type: 'thread.archive', threadId })
    },

    async deleteTask(threadId) {
      await dispatch({ type: 'thread.delete', threadId })
    },

    async visitTask(threadId) {
      await dispatch({ type: 'thread.visit', threadId, visitedAt: Date.now() })
    },

    async setSelection(threadId, selection) {
      await dispatch({ type: 'thread.model_selection.set', threadId, modelSelection: compactSelection(selection) })
    },

    async setRuntimeMode(threadId, mode) {
      await dispatch({ type: 'thread.runtime_mode.set', threadId, runtimeMode: mode })
    },

    async setInteractionMode(threadId, mode) {
      await dispatch({ type: 'thread.interaction_mode.set', threadId, interactionMode: mode })
    },

    async forkFromRun(task, runId) {
      const targetThreadId = newId()
      await dispatch({
        type: 'thread.fork',
        sourceThreadId: task.id,
        targetThreadId,
        sourcePoint: { type: 'run', runId },
      })
      return targetThreadId
    },

    async rewindToRun(task, runId, input) {
      const target = task.rewindTargets.get(runId)
      if (!target) throw new Error('This message has no checkpoint to rewind to')
      await dispatch({
        type: 'checkpoint.rollback',
        threadId: task.id,
        checkpointId: target.checkpointId,
        scopeId: target.scopeId,
        restoreFiles: true,
      })
      const store = stores?.threads.peek(task.id)
      if (store) await waitForRollback(store)
      const messageId = newId()
      await dispatch(
        messageDispatch(task.id, messageId, input, { type: 'queue_after_active' }),
        commandIds.dispatchMessage(messageId),
      )
    },

    async stopSubagent(threadId, runId, subagentId) {
      await dispatch({ type: 'subagent.stop', threadId, runId, subagentId })
    },
  }
}

export function TaskActionsProvider({ children }: { children: ReactNode }) {
  const { connection, stores } = useDaemon()
  const [forking, setForking] = useState<Record<string, string | undefined>>({})
  const [rewinding, setRewinding] = useState<Record<string, string | undefined>>({})

  const base = useMemo(
    () => (connection ? createTaskActions(connection, stores) : null),
    [connection, stores],
  )
  const required = useCallback(() => {
    if (!base) requireConnection(null)
    return base!
  }, [base])

  const value = useMemo<TaskActions>(() => {
    const proxy = <K extends keyof ReturnType<typeof createTaskActions>>(key: K) =>
      ((...args: unknown[]) => (required()[key] as (...values: unknown[]) => unknown)(...args)) as ReturnType<typeof createTaskActions>[K]
    return {
      forking,
      rewinding,
      addProject: proxy('addProject'),
      projectlessProject: proxy('projectlessProject'),
      startTask: proxy('startTask'),
      importSession: proxy('importSession'),
      sendMessage: proxy('sendMessage'),
      interrupt: proxy('interrupt'),
      respondApproval: proxy('respondApproval'),
      respondUserInput: proxy('respondUserInput'),
      dismissUserInput: proxy('dismissUserInput'),
      cancelQueued: proxy('cancelQueued'),
      renameTask: proxy('renameTask'),
      archiveTask: proxy('archiveTask'),
      deleteTask: proxy('deleteTask'),
      visitTask: proxy('visitTask'),
      setSelection: proxy('setSelection'),
      setRuntimeMode: proxy('setRuntimeMode'),
      setInteractionMode: proxy('setInteractionMode'),
      stopSubagent: proxy('stopSubagent'),
      async forkFromRun(task, runId) {
        if (forking[task.id]) throw new Error('This task is already forking')
        withEntry(setForking, task.id, runId)
        try {
          return await required().forkFromRun(task, runId)
        } finally {
          withEntry(setForking, task.id, undefined)
        }
      },
      async rewindToRun(task, runId, input) {
        if (rewinding[task.id]) throw new Error('This task is already rewinding')
        withEntry(setRewinding, task.id, runId)
        try {
          await required().rewindToRun(task, runId, input)
        } finally {
          withEntry(setRewinding, task.id, undefined)
        }
      },
    }
  }, [forking, required, rewinding])

  return <TaskActionsContext.Provider value={value}>{children}</TaskActionsContext.Provider>
}

export function useTaskActions(): TaskActions {
  const context = useContext(TaskActionsContext)
  if (!context) throw new Error('useTaskActions must be used inside TaskActionsProvider')
  return context
}
