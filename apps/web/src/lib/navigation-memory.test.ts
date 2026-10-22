import { describe, expect, test } from 'bun:test'
import {
  readRememberedNavigation,
  routeDestinationTransition,
  taskRemovalDestination,
  writeRememberedNavigation,
} from './navigation-memory'
import { project, taskSummary } from './test-fixtures'

describe('remembered navigation', () => {
  test('restores a new task and its project per daemon', () => {
    const storage = memoryStorage()
    writeRememberedNavigation(storage, 'ws://first', { kind: 'newTask', projectId: 'waku' })
    writeRememberedNavigation(storage, 'ws://second', { kind: 'session', sessionId: 'session-2' })

    expect(readRememberedNavigation(storage, 'ws://first')).toEqual({ kind: 'newTask', projectId: 'waku' })
    expect(readRememberedNavigation(storage, 'ws://second')).toEqual({ kind: 'session', sessionId: 'session-2' })
  })

  test('ignores malformed state', () => {
    expect(readRememberedNavigation(memoryStorage('{broken'), 'ws://first')).toBeNull()
  })
})

describe('browser route transitions', () => {
  test('Back from New Task activates the session in the URL', () => {
    expect(routeDestinationTransition(undefined, 'session-1', true)).toBe('session')
  })

  test('Forward from a session restores the New Task route', () => {
    expect(routeDestinationTransition('session-1', undefined, false)).toBe('newTask')
  })

  test('an explicit New Task transition does not reinitialize its draft', () => {
    expect(routeDestinationTransition('session-1', undefined, true)).toBeNull()
  })
})

describe('selected task removal', () => {
  const waku = project('waku', 'waku', '/repos/waku')

  test('opens the newest remaining task in the same project', () => {
    const removed = taskSummary({ id: 'removed', projectId: 'waku' })
    const older = taskSummary({ id: 'older', projectId: 'waku', updatedAt: 10 })
    const newer = taskSummary({ id: 'newer', projectId: 'waku', updatedAt: 20 })
    expect(taskRemovalDestination([waku], [waku], [older, newer, removed], removed))
      .toEqual({ kind: 'session', sessionId: 'newer' })
  })

  test('opens a fresh task in the same ordinary project when history is empty', () => {
    const removed = taskSummary({ projectId: 'waku' })
    expect(taskRemovalDestination([waku], [waku], [], removed)).toEqual({ kind: 'newTask', project: waku })
  })

  test('creates a fresh projectless task after deleting its last task', () => {
    const projectless = project('projectless', 'No project', '/home/user/.waku/projects')
    const removed = taskSummary({ projectId: 'projectless' })
    expect(taskRemovalDestination([projectless], [], [], removed)).toEqual({ kind: 'projectless' })
  })
})

function memoryStorage(initial?: string) {
  const values = new Map<string, string>()
  if (initial !== undefined) values.set('waku.navigation', initial)
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => { values.set(key, value) },
  }
}
