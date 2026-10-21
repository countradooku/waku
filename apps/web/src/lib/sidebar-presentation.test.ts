import { describe, expect, test } from 'bun:test'
import {
  dateGroup,
  formatTimeAgo,
  formatWorkingElapsed,
  groupSessions,
  nextSidebarUpdateDelay,
  sessionHasStarted,
  sessionTimeLabel,
  sidebarRows,
} from './sidebar-presentation'
import { project, taskSummary } from './test-fixtures'

describe('desktop sidebar presentation', () => {
  test('uses a Monday-based current week instead of a rolling seven days', () => {
    const wednesday = new Date(2026, 7, 12, 12)
    expect(dateGroup(atLocalNoon(2026, 7, 12), wednesday)).toBe('today')
    expect(dateGroup(atLocalNoon(2026, 7, 11), wednesday)).toBe('yesterday')
    expect(dateGroup(atLocalNoon(2026, 7, 10), wednesday)).toBe('week')
    expect(dateGroup(atLocalNoon(2026, 7, 9), wednesday)).toBe('month')
  })

  test('keeps Add Project in an empty history through the first group header', () => {
    expect(sidebarRows([], new Set())).toEqual([
      { kind: 'search', key: 'search' },
      {
        kind: 'group',
        key: 'group:today',
        group: { id: 'today', label: 'Today', sessions: [] },
        collapsed: false,
        first: true,
      },
    ])
  })

  test('matches desktop settled and live time labels', () => {
    expect(formatTimeAgo(0)).toBe('just now')
    expect(formatTimeAgo(604_800)).toBe('7d')
    expect(formatWorkingElapsed(65)).toBe('1m 5s')
    expect(formatWorkingElapsed(3_720)).toBe('1h 2m')

    const live = taskSummary({ lastReplyAt: 100_000 }, { activity: 'working' })
    expect(sessionTimeLabel(live, 165)).toBe('Working for 1m 5s')
    expect(nextSidebarUpdateDelay([live], 165)).toBe(1)
    const settled = taskSummary({ lastReplyAt: 100_000 })
    expect(sessionTimeLabel(settled, 220)).toBe('2m')
  })

  test('lists threads with a reply or their own title, without inventing a reply time', () => {
    const placeholder = taskSummary()
    expect(sessionHasStarted(placeholder)).toBe(false)
    const imported = taskSummary({ title: 'Imported', titleSource: 'user' })
    expect(sessionHasStarted(imported)).toBe(true)
    expect(sessionTimeLabel(imported, 1_000)).toBeNull()
    expect(sessionHasStarted(taskSummary({}, { queuedRunCount: 1 }))).toBe(true)
  })

  test('presents the projectless sentinel with the localized desktop name', () => {
    const groups = groupSessions(
      [project('project', 'No project', '/home/me/.waku/projects')],
      [taskSummary({ lastReplyAt: new Date(2026, 7, 15, 11).getTime() })],
      new Date(2026, 7, 15, 12),
      'Unknown project',
      'プロジェクトなし',
    )
    expect(groups[0]?.id).toBe('today')
    expect(groups[0]?.sessions[0]?.projectName).toBe('プロジェクトなし')
  })
})

function atLocalNoon(year: number, month: number, day: number): number {
  return Math.floor(new Date(year, month, day, 12).getTime() / 1_000)
}
