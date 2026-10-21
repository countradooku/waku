// Dispatch payloads: what the web sends through `orchestration.dispatch`.

import { describe, expect, test } from 'bun:test'
import type * as T from '@waku/client/v8'
import type { DaemonConnection, ThreadStore } from '@waku/client/v8'
import {
  dispatchMode,
  driverOfInstance,
  firstMessageMode,
  messageDispatch,
  providerText,
  threadCreate,
} from './commands'
import type { DaemonStores } from './daemon-context'
import { createTaskActions, waitForRollback } from './task-actions'
import { fakeConnection, project } from './test-fixtures'
import type { BackgroundWorkItem, Task } from './thread-model'

const attachment: T.Attachment = { name: 'a.png', path: '/att/a.png', mention: 'shots/a.png', isImage: true }

describe('command builders', () => {
  test('a message carries provider text with mentions, the typed text and a title seed', () => {
    expect(providerText({ text: ' look ', attachments: [attachment] })).toBe('look @shots/a.png')
    expect(messageDispatch('t', 'm', { text: 'look', attachments: [attachment] }, { type: 'queue_after_active' })).toEqual({
      type: 'message.dispatch',
      threadId: 't',
      messageId: 'm',
      text: 'look @shots/a.png',
      displayText: 'look',
      titleSeed: 'look',
      attachments: [attachment],
      mode: { type: 'queue_after_active' },
    })
    // A slash-command expansion is sent as is; plain text needs no displayText.
    expect(messageDispatch('t', 'm', { text: 'hi', attachments: [] }, { type: 'auto' })).toEqual({
      type: 'message.dispatch', threadId: 't', messageId: 'm', text: 'hi', titleSeed: 'hi', mode: { type: 'auto' },
    })
    expect(providerText({ text: '/review', attachments: [], providerText: 'Review this ' })).toBe('Review this')
  })

  test('steering targets the active run; everything else waits behind it', () => {
    const task = { activeRun: { id: 'run-1' }, supportsSteer: true } as Task
    expect(dispatchMode(task, 'steer')).toEqual({ type: 'steer_active', targetRunId: 'run-1' })
    expect(dispatchMode(task, 'send')).toEqual({ type: 'queue_after_active' })
    expect(dispatchMode({ supportsSteer: true } as Task, 'steer')).toEqual({ type: 'queue_after_active' })
  })

  test('a new worktree task creates the thread, then defers its first run to prepare it', () => {
    const spec = {
      threadId: 't',
      projectId: 'p',
      selection: { instance: 'codex', model: 'gpt', reasoningEffort: '' },
      runtimeMode: 'full_access',
      interactionMode: 'default',
      worktreeFrom: 'main',
    } as const
    expect(threadCreate(spec)).toEqual({
      type: 'thread.create',
      threadId: 't',
      projectId: 'p',
      modelSelection: { instance: 'codex', model: 'gpt' },
      runtimeMode: 'full_access',
      interactionMode: 'default',
      workspace: { type: 'new_worktree', baseRef: 'main' },
    })
    expect(firstMessageMode(spec)).toEqual({ type: 'defer_start', workspace: { type: 'new_worktree', baseRef: 'main' } })
    expect(firstMessageMode({})).toEqual({ type: 'start_immediately' })
    expect(driverOfInstance('deepseek')).toBe('deep_seek')
    expect(driverOfInstance('ohmypi')).toBe('oh_my_pi')
    expect(driverOfInstance('codex')).toBe('codex')
  })
})

function actionsWith(stores: Partial<DaemonStores> = {}) {
  const connection = fakeConnection({
    'orchestration.dispatch': () => ({ sequence: 1 }),
    'daemon.info': () => ({ defaultCwd: '/home/me', projectlessRoot: '/home/me/.waku/projects', environment: {}, features: [], uptimeMs: 0, version: 'x' }),
  })
  const actions = createTaskActions(connection as unknown as DaemonConnection, stores as DaemonStores)
  const commands = () => connection.calls
    .filter((call) => call.method === 'orchestration.dispatch')
    .map((call) => call.params as T.DispatchParams)
  return { actions, commands }
}

describe('task actions', () => {
  test('starting a task dispatches thread.create then its first message, with derived command ids', async () => {
    const { actions, commands } = actionsWith()
    const threadId = await actions.startTask(
      { projectId: 'p', selection: { instance: 'claude' }, runtimeMode: 'ask', interactionMode: 'default' },
      { text: 'hello', attachments: [] },
    )
    const [create, message] = commands()
    expect(create!.commandId).toBe(`thread.create:${threadId}`)
    expect(create!.command).toMatchObject({ type: 'thread.create', threadId, projectId: 'p', workspace: { type: 'root' } })
    expect(message!.command).toMatchObject({ type: 'message.dispatch', threadId, text: 'hello', mode: { type: 'start_immediately' } })
    expect(message!.commandId).toBe(`message.dispatch:${(message!.command as { messageId: string }).messageId}`)
  })

  test('approvals, questions, interrupts, queue and thread lifecycle map to their commands', async () => {
    const { actions, commands } = actionsWith()
    const task = { id: 't', activeRun: { id: 'r' } } as Task
    await actions.respondApproval('t', 'req', 'allow')
    await actions.respondUserInput('t', 'q', [{ questionId: 'x', answers: ['yes'] }])
    await actions.interrupt(task)
    await actions.cancelQueued('t', 'queued')
    await actions.renameTask('t', 'Renamed')
    await actions.archiveTask('t')
    await actions.deleteTask('t')
    await actions.setRuntimeMode('t', 'auto')
    await actions.setSelection('t', { instance: 'codex', model: 'm', serviceTier: '' })
    await actions.stopSubagent('t', 'r', 'node')
    expect(commands().map((params) => params.command)).toEqual([
      { type: 'runtime_request.respond', threadId: 't', requestId: 'req', response: { type: 'approval', optionId: 'allow' } },
      { type: 'runtime_request.respond', threadId: 't', requestId: 'q', response: { type: 'user_input', answers: [{ questionId: 'x', answers: ['yes'] }] } },
      { type: 'run.interrupt', threadId: 't', runId: 'r' },
      { type: 'queued_run.cancel', threadId: 't', runId: 'queued' },
      { type: 'thread.rename', threadId: 't', title: 'Renamed' },
      { type: 'thread.archive', threadId: 't' },
      { type: 'thread.delete', threadId: 't' },
      { type: 'thread.runtime_mode.set', threadId: 't', runtimeMode: 'auto' },
      { type: 'thread.model_selection.set', threadId: 't', modelSelection: { instance: 'codex', model: 'm' } },
      { type: 'subagent.stop', threadId: 't', runId: 'r', subagentId: 'node' },
    ])
  })

  test('adding a project reuses one at the same path', async () => {
    const existing = project('p1', 'waku', '/repos/waku')
    const shell = { state: { projects: new Map([[existing.id, existing]]) } }
    const { actions, commands } = actionsWith({ shell } as unknown as Partial<DaemonStores>)
    expect(await actions.addProject('/repos/waku/')).toBe(existing)
    expect(commands()).toEqual([])
  })

  test('every projectless task gets a scratch project, returned once the shell holds it', async () => {
    const projects = new Map<string, T.Project>()
    const listeners = new Set<() => void>()
    const shell = {
      state: { projects },
      subscribe(listener: () => void) {
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
    }
    const { actions, commands } = actionsWith({ shell } as unknown as Partial<DaemonStores>)
    const pending = actions.projectlessProject()
    await new Promise((resolve) => setTimeout(resolve, 5))
    const [create] = commands()
    const projectId = (create!.command as { projectId: string }).projectId
    expect(create).toEqual({ commandId: `project.create_scratch:${projectId}`, command: { type: 'project.create_scratch', projectId } })
    const scratch = project(projectId, 'No project', '/home/me/.waku/projects/scratch-1')
    projects.set(projectId, scratch)
    for (const listener of listeners) listener()
    expect(await pending).toBe(scratch)
    expect(listeners.size).toBe(0)

    const second = actions.projectlessProject()
    await new Promise((resolve) => setTimeout(resolve, 5))
    const secondId = (commands()[1]!.command as { projectId: string }).projectId
    expect(secondId).not.toBe(projectId)
    projects.set(secondId, project(secondId, 'No project', '/home/me/.waku/projects/scratch-2'))
    for (const listener of listeners) listener()
    expect((await second).id).toBe(secondId)
  })

  test('processes and monitors stop through background_work.stop, subagents through subagent.stop', async () => {
    const { actions, commands } = actionsWith()
    const work = (patch: Partial<BackgroundWorkItem>): BackgroundWorkItem => ({
      id: 'task-1', kind: 'process', title: 'npm run dev', status: 'running', canStop: true, ...patch,
    })
    await actions.stopBackgroundWork('t', work({ providerThreadId: 'pt' }))
    await actions.stopBackgroundWork('t', work({ kind: 'monitor', id: 'mon' }))
    await actions.stopBackgroundWork('t', work({ kind: 'subagent', id: 'node', runId: 'r' }))
    await actions.refreshBackgroundWork('t')
    expect(commands().map((params) => params.command)).toEqual([
      { type: 'background_work.stop', threadId: 't', taskId: 'task-1', providerThreadId: 'pt' },
      { type: 'background_work.stop', threadId: 't', taskId: 'mon' },
      { type: 'subagent.stop', threadId: 't', runId: 'r', subagentId: 'node' },
      { type: 'background_work.refresh', threadId: 't' },
    ])
    await expect(actions.stopBackgroundWork('t', work({ kind: 'subagent' }))).rejects.toThrow('cannot be stopped')
  })

  test('importing a provider conversation creates a thread that adopts it', async () => {
    const { actions, commands } = actionsWith()
    const threadId = await actions.importSession({
      instanceId: 'deepseek', nativeId: 'n-1', resumeCursor: '{}', title: 'Old chat', cwd: '/repos', createdAt: 1, updatedAt: 2,
    }, 'p', 'full_access')
    expect(commands()[0]!.command).toEqual({
      type: 'thread.create',
      threadId,
      projectId: 'p',
      title: 'Old chat',
      modelSelection: { instance: 'deepseek' },
      runtimeMode: 'full_access',
      interactionMode: 'default',
      workspace: { type: 'root' },
      importedNativeThread: { driver: 'deep_seek', nativeId: 'n-1', strength: 'strong' },
    })
  })

  test('rewinding rolls back to the previous checkpoint, waits for it, then sends the edit', async () => {
    const listeners = new Set<() => void>()
    const store = {
      state: { thread: { rollback: { checkpointId: 'c', scopeId: 's', requestId: 'x', requestedAt: 1 } } },
      subscribe(listener: () => void) {
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
    }
    const threads = { peek: () => store }
    const { actions, commands } = actionsWith({ threads } as unknown as Partial<DaemonStores>)
    const task = { id: 't', rewindTargets: new Map([['run-2', { checkpointId: 'c', scopeId: 's' }]]) } as unknown as Task
    const rewound = actions.rewindToRun(task, 'run-2', { text: 'again', attachments: [] })
    await new Promise((resolve) => setTimeout(resolve, 5))
    expect(commands().map((params) => params.command.type)).toEqual(['checkpoint.rollback'])
    expect(commands()[0]!.command).toEqual({ type: 'checkpoint.rollback', threadId: 't', checkpointId: 'c', scopeId: 's', restoreFiles: true })
    store.state.thread.rollback = undefined as never
    for (const listener of listeners) listener()
    await rewound
    expect(commands()[1]!.command).toMatchObject({ type: 'message.dispatch', threadId: 't', text: 'again' })
    await expect(actions.rewindToRun(task, 'run-1', { text: 'x', attachments: [] })).rejects.toThrow('no checkpoint')
  })

  test('a failed rollback surfaces its failure', async () => {
    const store = {
      state: { thread: { rollback: { failure: 'ref is gone' } } },
      subscribe: () => () => {},
    } as unknown as ThreadStore
    await expect(waitForRollback(store)).rejects.toThrow('ref is gone')
  })
})
