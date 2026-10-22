// The web's view model, fed by recorded v8 stream items through the client
// reducers (`packages/waku-client/src/v8/fixtures/reducers.json`, recorded
// from the daemon) and by small synthetic projections.

import { describe, expect, test } from 'bun:test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import type * as T from '@waku/client/v8'
import {
  applyShellItem,
  applyThreadItem,
  createShellState,
  createThreadState,
  itemNormalizers,
} from '@waku/client/v8'
import { activityFromItem, shellView, taskFromThread } from './thread-model'
import { appThread } from './test-fixtures'

const fixture = JSON.parse(readFileSync(
  join(import.meta.dir, '../../../../packages/waku-client/src/v8/fixtures/reducers.json'),
  'utf8',
)) as { thread: { items: unknown[] }; shell: { items: unknown[] } }

describe('recorded daemon streams', () => {
  test('the shell stream becomes projects and task rows', () => {
    const state = createShellState()
    for (const raw of fixture.shell.items) applyShellItem(state, itemNormalizers.shell(structuredClone(raw)))
    const view = shellView(state)
    expect(view.projects.map((project) => project.name)).toEqual(['fixture'])
    expect(view.tasks.map((task) => [task.title, task.status, task.projectId])).toEqual([
      ['Fixture, renamed', 'idle', view.projects[0]!.id],
    ])
  })

  test('the thread stream becomes a transcript of messages and activities in run order', () => {
    const state = createThreadState()
    for (const raw of fixture.thread.items) applyThreadItem(state, itemNormalizers.thread(structuredClone(raw)))
    const task = taskFromThread(state)!
    expect(task.status).toBe('idle')
    expect(task.runs.map((run) => run.ordinal)).toEqual([1, 2, 3, 4])
    expect(task.entries.map((entry) => entry.kind === 'message' ? entry.message.role : entry.kind)).toEqual([
      'user', 'activity', 'assistant',
      'user', 'activity', 'assistant',
      'user', 'activity', 'assistant',
      'user', 'assistant',
    ])
    const command = task.entries.find((entry) => entry.kind === 'activity')
    expect(command?.kind === 'activity' && command.activity).toMatchObject({
      kind: 'command',
      displayTarget: 'cargo test --package seed-0',
      omitted: true,
      streamsOutput: true,
      complete: true,
      failed: false,
      exitCode: 0,
    })
    // The live deltas folded into the last answer.
    const last = task.entries.at(-1)
    expect(last?.kind === 'message' && last.message.text).toBe('Hello, world')
    expect(task.synchronized).toBe(true)
  })
})

const THREAD = 'thread'

function item(id: string, position: number, kind: T.TurnItemKind, patch: Partial<T.TurnItem> = {}): T.TurnItem {
  return { id, threadId: THREAD, position, status: 'completed', updatedAt: 1, kind, ...patch }
}

function run(id: string, ordinal: number | undefined, status: T.RunStatus, patch: Partial<T.Run> = {}): T.Run {
  return { id, threadId: THREAD, ordinal, status, modelSelection: { instance: 'codex' }, requestedAt: ordinal ?? 9, userMessageId: `${id}-message`, ...patch }
}

function stateOf(projection: Omit<T.ThreadProjection, 'thread'>, thread: Partial<T.AppThread> = {}) {
  const state = createThreadState()
  applyThreadItem(state, {
    type: 'snapshot',
    cursor: { type: 'sequence', store: 's', seq: 1 },
    projection: { thread: appThread({ id: THREAD, ...thread }), ...projection },
    hasMoreHistory: true,
    history: 'older',
  })
  return state
}

describe('a thread in progress', () => {
  const state = stateOf({
    runs: [
      run('r1', 1, 'completed'),
      run('r2', 2, 'rolled_back'),
      run('r3', 3, 'waiting', { startedAt: 30 }),
      run('q1', undefined, 'queued', { queuePosition: 2 }),
      run('q0', undefined, 'queued', { queuePosition: 1, queueHeld: true }),
    ],
    messages: [
      { id: 'q0-message', threadId: THREAD, role: 'user', text: 'first queued @a.txt', displayText: 'first queued', createdAt: 1, updatedAt: 1, createdBy: 'user', creationSource: 'web' },
      { id: 'q1-message', threadId: THREAD, role: 'user', text: 'second queued', createdAt: 2, updatedAt: 2, createdBy: 'user', creationSource: 'web' },
    ],
    runtimeRequests: [
      {
        id: 'approve', threadId: THREAD, nodeId: 'n', kind: 'command_approval', status: 'pending', createdAt: 5,
        responseCapability: { type: 'live', providerSessionId: 'ps' },
        options: [{ id: 'yes', label: 'Allow', allow: true }, { id: 'no', label: 'Deny' }],
      },
      {
        id: 'ask', threadId: THREAD, nodeId: 'n2', kind: 'user_input', status: 'pending', createdAt: 6,
        responseCapability: { type: 'live', providerSessionId: 'ps' },
        questions: [{ id: 'q', header: 'Pick', question: 'Which?', options: [{ label: 'A' }] }],
      },
    ],
    checkpointScopes: [{ id: 'root', threadId: THREAD, kind: 'root_run', nodeId: 'n', cwd: '/w', createdAt: 1, ordinalWithinParent: 0 }],
    checkpoints: [
      { id: 'c1', threadId: THREAD, scopeId: 'root', nodeId: 'n', gitRef: 'refs/waku/1', ordinalWithinScope: 1, appRunOrdinal: 1, capturedAt: 1, status: 'ready', files: [{ path: 'a.rs', kind: 'modified', additions: 2, deletions: 1 }] },
    ],
    subagents: [
      { id: 'sub', threadId: THREAD, runId: 'r3', parentNodeId: 'n', driver: 'codex', providerInstanceId: 'codex', createdBy: 'agent', origin: 'provider_native', prompt: 'Review the diff\nthoroughly', status: 'running', updatedAt: 40, startedAt: 35 },
    ],
    providerSessions: [
      { id: 'ps', driver: 'codex', providerInstanceId: 'codex', cwd: '/w', status: 'running', createdAt: 1, updatedAt: 2, capabilities: { turns: { active_steering: true } } },
    ],
    providerThreads: [
      { id: 'pt', appThreadId: THREAD, driver: 'codex', providerInstanceId: 'codex', status: 'active', createdAt: 1, updatedAt: 2, contextUsage: { tokens: 1200, window: 200000 }, goal: { objective: 'Ship it', status: 'active' } },
    ],
    turnItems: [
      item('u1', 1 << 24, { type: 'user_message', messageId: 'm1', text: 'hi', createdBy: 'user', inputIntent: 'turn_start' }, { runId: 'r1' }),
      item('cp1', (1 << 24) + 1, { type: 'checkpoint', checkpointId: 'c1', scopeId: 'root', files: [{ path: 'a.rs', kind: 'modified', additions: 2, deletions: 1 }] }, { runId: 'r1' }),
      item('u2', 2 << 24, { type: 'user_message', messageId: 'm2', text: 'gone', createdBy: 'user', inputIntent: 'turn_start' }, { runId: 'r2' }),
      item('u3', 3 << 24, { type: 'user_message', messageId: 'm3', text: 'now', createdBy: 'user', inputIntent: 'turn_start' }, { runId: 'r3' }),
      item('think', (3 << 24) + 1, { type: 'reasoning', text: 'Plan', streaming: true }, { runId: 'r3', status: 'running', startedAt: 31 }),
      item('ap', (3 << 24) + 2, { type: 'approval_request', requestId: 'approve', requestKind: 'command_approval', prompt: 'rm -rf build' }, { runId: 'r3', status: 'waiting', title: 'Run rm?' }),
      item('edit', (3 << 24) + 3, { type: 'file_change', fileName: 'b.rs', additions: 3, deletions: 0, bodyOmitted: true, changes: [{ path: 'b.rs', operation: 'add' }] }, { runId: 'r3' }),
      item('err', (3 << 24) + 4, { type: 'error', failure: { class: 'provider_error', message: 'Rate limited' } }, { runId: 'r3' }),
    ],
  })
  const task = taskFromThread(state, {
    thread: state.thread!,
    activity: 'waiting',
    pendingRequestCount: 2,
    queuedRunCount: 2,
  })!

  test('status, active run, steering, goal and context come from the projection', () => {
    expect(task.status).toBe('waiting')
    expect(task.activeRun?.id).toBe('r3')
    expect(task.supportsSteer).toBe(true)
    expect(task.goal?.objective).toBe('Ship it')
    expect(task.contextUsage).toEqual({ tokens: 1200, window: 200000 })
    expect(task.hasMoreHistory).toBe(true)
  })

  test('rolled-back runs leave the transcript; checkpoints become change cards', () => {
    const kinds = task.entries.map((entry) => [entry.kind, entry.key])
    expect(kinds).not.toContainEqual(['message', 'item-u2'])
    const card = task.entries.find((entry) => entry.kind === 'checkpoint')
    expect(card?.kind === 'checkpoint' && card.checkpoint).toEqual({
      id: 'c1', scopeId: 'root', runId: 'r1', files: [{ path: 'a.rs', additions: 2, deletions: 1 }], additions: 2, deletions: 1,
    })
    const error = task.entries.find((entry) => entry.key === 'item-err')
    expect(error?.kind === 'message' && error.message).toMatchObject({ role: 'system', text: 'Rate limited', tone: 'error' })
  })

  test('tool, reasoning and file items become activities', () => {
    const activities = task.entries.flatMap((entry) => entry.kind === 'activity' ? [entry.activity] : [])
    expect(activities.map((activity) => [activity.kind, activity.complete])).toEqual([
      ['reasoning', false],
      ['tool', false],
      ['fileChange', true],
    ])
    expect(activities[2]).toMatchObject({ omitted: true, fileChanges: [{ path: 'b.rs', status: 'added', additions: 3, deletions: 0 }] })
  })

  test('pending requests become the approval and question panels', () => {
    expect(task.approval).toEqual({
      requestId: 'approve',
      title: 'Run rm?',
      detail: 'rm -rf build',
      options: [{ id: 'yes', label: 'Allow', allow: true }, { id: 'no', label: 'Deny', allow: false }],
    })
    expect(task.userInput).toEqual({
      requestId: 'ask',
      questions: [{ id: 'q', header: 'Pick', question: 'Which?', options: [{ label: 'A', description: undefined }], multiSelect: false }],
    })
  })

  test('queued runs are listed in queue order with their messages', () => {
    expect(task.queued.map((queued) => [queued.runId, queued.text, queued.displayText, queued.held])).toEqual([
      ['q0', 'first queued @a.txt', 'first queued', true],
      ['q1', 'second queued', undefined, false],
    ])
  })

  test('a run without a turn-start checkpoint rewinds to the previous run\'s ending one', () => {
    expect(task.rewindTargets.get('r1')).toBeUndefined()
    expect(task.rewindTargets.get('r3')).toBeUndefined()
    const later = stateOf({
      runs: [run('r1', 1, 'completed', { checkpointId: 'c1' }), run('r2', 2, 'completed')],
      checkpointScopes: [{ id: 'root', threadId: THREAD, kind: 'root_run', nodeId: 'n', cwd: '/w', createdAt: 1, ordinalWithinParent: 0 }],
      checkpoints: [{ id: 'c1', threadId: THREAD, scopeId: 'root', nodeId: 'n', gitRef: 'r', ordinalWithinScope: 1, appRunOrdinal: 1, capturedAt: 1, status: 'ready' }],
    })
    expect(taskFromThread(later)!.rewindTargets.get('r2')).toEqual({ checkpointId: 'c1', scopeId: 'root' })
  })

  test('subagents become background work that can be stopped', () => {
    expect(task.backgroundWork).toEqual([{
      id: 'sub',
      kind: 'subagent',
      title: 'Review the diff',
      detail: undefined,
      output: undefined,
      status: 'running',
      startedAt: 35,
      updatedAt: 40,
      model: undefined,
      runId: 'r3',
      itemId: undefined,
      canStop: true,
    }])
  })
})

describe('activity details', () => {
  test('a dynamic tool shows pretty arguments, its MCP server and blob images', () => {
    const activity = activityFromItem(item('tool', 1, {
      type: 'dynamic_tool',
      toolName: 'mcp__github__search',
      input: '{"q":"bug"}',
      output: '{"image":"waku-blob:shot.png"}',
    }, { tool: { source: { key: 'github', kind: 'integration', name: 'GitHub' } } }))
    expect(activity).toMatchObject({
      kind: 'tool',
      title: 'mcp__github__search',
      mcpServer: 'GitHub',
      arguments: '{\n  "q": "bug"\n}',
      imageUrls: ['waku-blob:shot.png'],
    })
  })

  test('message-like items are not activities', () => {
    expect(activityFromItem(item('m', 1, { type: 'assistant_message', messageId: 'm', text: 'x' }))).toBeUndefined()
  })
})

describe('turn-start checkpoints', () => {
  const scope = (id: string, runId: string, kind: T.ScopeKind = 'root_run'): T.CheckpointScope => ({
    id, threadId: THREAD, kind, nodeId: 'n', cwd: '/w', createdAt: 1, ordinalWithinParent: 0, runId,
  })
  const checkpoint = (id: string, scopeId: string, runId: string, ordinalWithinScope: number, appRunOrdinal: number, patch: Partial<T.Checkpoint> = {}): T.Checkpoint => ({
    id, threadId: THREAD, scopeId, nodeId: 'n', gitRef: `refs/${id}`, ordinalWithinScope, appRunOrdinal, runId, capturedAt: 1, status: 'ready', ...patch,
  })
  // Each run's root scope holds its turn start (ordinal 0, app run ordinal
  // = run ordinal - 1) and the checkpoint it ended with.
  const state = stateOf({
    runs: [
      run('r1', 1, 'completed', { checkpointId: 'end1' }),
      run('r2', 2, 'completed', { checkpointId: 'end2' }),
      run('r3', 3, 'completed', { checkpointId: 'end3' }),
      run('r4', 4, 'running'),
    ],
    checkpointScopes: [scope('s1', 'r1'), scope('s2', 'r2'), scope('s3', 'r3'), scope('s4', 'r4'), scope('nested', 'r2', 'subagent')],
    checkpoints: [
      checkpoint('start1', 's1', 'r1', 0, 0),
      checkpoint('end1', 's1', 'r1', 1, 1, { parentCheckpointId: 'start1' }),
      checkpoint('start2', 's2', 'r2', 0, 1),
      checkpoint('end2', 's2', 'r2', 1, 2, { parentCheckpointId: 'start2' }),
      checkpoint('nested0', 'nested', 'r2', 0, 1),
      checkpoint('start3', 's3', 'r3', 0, 2, { status: 'missing' }),
      checkpoint('end3', 's3', 'r3', 1, 3),
      checkpoint('start4', 's4', 'r4', 0, 3),
    ],
  })
  const task = taskFromThread(state)!

  test('every run rewinds to its own turn start, the first message included', () => {
    expect(task.rewindTargets.get('r1')).toEqual({ checkpointId: 'start1', scopeId: 's1' })
    expect(task.rewindTargets.get('r2')).toEqual({ checkpointId: 'start2', scopeId: 's2' })
    expect(task.rewindTargets.get('r4')).toEqual({ checkpointId: 'start4', scopeId: 's4' })
  })

  test('a missing turn start falls back to the previous live run\'s ending checkpoint', () => {
    expect(task.rewindTargets.get('r3')).toEqual({ checkpointId: 'end2', scopeId: 's2' })
    const rewound = stateOf({
      runs: [
        run('r1', 1, 'completed', { checkpointId: 'end1' }),
        run('r2', 2, 'rolled_back', { checkpointId: 'end2' }),
        run('r3', 3, 'completed', { checkpointId: 'end3' }),
      ],
      checkpointScopes: [scope('s1', 'r1'), scope('s2', 'r2'), scope('s3', 'r3')],
      checkpoints: [
        checkpoint('end1', 's1', 'r1', 1, 1),
        checkpoint('end2', 's2', 'r2', 1, 2),
        checkpoint('end3', 's3', 'r3', 1, 3),
      ],
    })
    const targets = taskFromThread(rewound)!.rewindTargets
    expect(targets.get('r1')).toBeUndefined()
    expect(targets.has('r2')).toBe(false)
    expect(targets.get('r3')).toEqual({ checkpointId: 'end1', scopeId: 's1' })
  })

  test('runs that ended with a ready checkpoint can be reviewed as turns', () => {
    expect(task.runs.map((candidate) => [candidate.id, candidate.diffable])).toEqual([
      ['r1', true], ['r2', true], ['r3', true], ['r4', false],
    ])
  })
})

describe('background processes and session commands', () => {
  const state = stateOf({
    providerThreads: [{
      id: 'pt', appThreadId: THREAD, driver: 'codex', providerInstanceId: 'codex', status: 'active', createdAt: 1, updatedAt: 2,
      pendingBackgroundTasks: [
        { taskId: 'proc', kind: 'process', itemId: 'cmd' },
        { taskId: 'watch', kind: 'monitor', description: 'Watch logs' },
        { taskId: 'agent', kind: 'subagent' },
      ],
    }],
    turnItems: [
      item('cmd', 1, { type: 'command_execution', input: 'npm run dev\n--port 3000' }, { runId: 'r1', status: 'running', startedAt: 5, updatedAt: 6 }),
    ],
    providerSessions: [
      {
        id: 'old', driver: 'codex', providerInstanceId: 'codex', cwd: '/w', status: 'running', createdAt: 1, updatedAt: 1,
        availableCommands: [{ name: 'review', description: 'Review changes' }, { name: 'compact' }],
      },
      {
        id: 'new', driver: 'codex', providerInstanceId: 'codex', cwd: '/w', status: 'running', createdAt: 2, updatedAt: 3,
        availableCommands: [{ name: '/compact', description: 'Compact the context' }, { name: 'review' }],
      },
      {
        id: 'gone', driver: 'codex', providerInstanceId: 'codex', cwd: '/w', status: 'closed', createdAt: 1, updatedAt: 9, detachedAt: 9,
        availableCommands: [{ name: 'detached' }],
      },
      {
        id: 'other', driver: 'claude', providerInstanceId: 'claude', cwd: '/w', status: 'running', createdAt: 1, updatedAt: 9,
        availableCommands: [{ name: 'other-provider' }],
      },
    ],
  })
  const task = taskFromThread(state)!

  test('processes and monitors can be stopped and stream their command item', () => {
    expect(task.backgroundWork).toEqual([
      { id: 'watch', kind: 'monitor', title: 'Watch logs', status: 'running', providerThreadId: 'pt', canStop: true },
      {
        id: 'proc', kind: 'process', title: 'npm run dev', status: 'running', startedAt: 5, updatedAt: 6, runId: 'r1',
        itemId: 'cmd', providerThreadId: 'pt', canStop: true,
      },
    ])
  })

  test('open sessions of the thread\'s provider contribute their slash commands once each', () => {
    expect(task.sessionCommands).toEqual([
      { name: 'compact', description: 'Compact the context' },
      { name: 'review', description: 'Review changes' },
    ])
  })
})
