import { describe, expect, test } from 'bun:test'
import type { ActivityKind, ActivityView, RunView, TranscriptEntry } from './thread-model'
import {
  activityActionLabel,
  activityDisclosureSections,
  activityDisplayTitle,
  activityFileChangeStats,
  activityGroupIsLive,
  activityHeaderTitle,
  activityPreview,
  activityRowDetail,
  activitySummary,
  activityTextRows,
  assistantResponseFooters,
  fencedCode,
  reasoningTitle,
  shouldVirtualizeActivityText,
  turnAnswerStart,
  turnFoldLabel,
  formatMessageTime,
  formatWorkingElapsed,
} from './transcript-presentation'

describe('desktop transcript language', () => {
  test('uses the desktop stopped and worked labels', () => {
    expect(turnFoldLabel(run('interrupted', 100, 164))).toBe('You stopped after 1 minute 4 seconds')
    expect(turnFoldLabel(run('completed', 100, 160))).toBe('Worked for 1 minute')
    expect(turnFoldLabel(run('completed', 100, 3_820))).toBe('Worked for 1 hour 2 minutes')
  })

  test('calls provider reasoning a thought', () => {
    expect(reasoningTitle(reasoning(false, 0, 0))).toBe('Thinking')
    expect(reasoningTitle(reasoning(true, 1_000, 65_000))).toBe('Thought for 1 minute 4 seconds')
  })

  test('uses thought nouns in grouped activity summaries', () => {
    expect(activitySummary([
      reasoning(true, 0, 1_000),
      { ...reasoning(true, 0, 1_000), id: 'thought-2' },
      activity('command', true),
    ])).toBe('Ran 2 thoughts · 1 command')
    expect(activitySummary([reasoning(false, 0, 0)])).toBe('Running 1 thought')
  })

  test('summarizes an activity group only after it leaves the live tail', () => {
    const command = {
      ...activity('command', false),
      displayTarget: 'git log --oneline -15',
    }
    const activities = [reasoning(true, 0, 1_000), command]

    expect(activityHeaderTitle(activities, true)).toBe('Running git log --oneline -15')
    expect(activityGroupIsLive(true, true)).toBe(true)
    command.complete = true
    expect(activityHeaderTitle(activities, true)).toBe('Ran git log --oneline -15')
    expect(activityHeaderTitle(activities, false)).toBe('Ran 1 thought · 1 command')
    expect(activityGroupIsLive(true, false)).toBe(false)
    expect(activityGroupIsLive(false, true)).toBe(false)
    expect(activityActionLabel(command)).toBe('Run')
    expect(activityRowDetail(command)).toBe('git log --oneline -15')
  })

  test('keeps generic tool names and labels AskUserQuestion by purpose', () => {
    const named = {
      ...activity('tool', true),
      title: 'mcp__threads__create_thread',
    }
    const question = {
      ...activity('tool', true),
      title: 'AskUserQuestion',
    }
    const unnamed = {
      ...activity('tool', true),
      title: 'Tool',
    }

    expect(activityActionLabel(named)).toBe('Tool')
    expect(activityRowDetail(named)).toBe('Create thread')
    expect(activityDisplayTitle(named)).toBe('Create thread')
    expect(activityActionLabel(unnamed)).toBe('Tool')
    expect(activityRowDetail(unnamed)).toBe('')
    expect(activityActionLabel(question)).toBe('Ask questions')
    expect(activityRowDetail(question)).toBe('')
    expect(activityDisplayTitle(question)).toBe('Ask questions')
  })

  test('derives the same provider-neutral activity titles as desktop', () => {
    expect(activityDisplayTitle({
      ...activity('fileChange', true),
      title: 'Edit file',
      fileChanges: [{ path: '/tmp/src/app.ts', additions: 4, deletions: 1 }],
    })).toBe('Edited app.ts')
    expect(activityDisplayTitle({
      ...activity('command', false),
      title: 'Run command',
      displayTarget: 'bun test',
    })).toBe('Running bun test')
    expect(activityDisplayTitle({
      ...activity('command', true),
      title: 'Run command',
      displayTarget: 'python3 analyze.py',
      displayDescription: 'Analyze color statistics',
    })).toBe('Ran command: Analyze color statistics')
    expect(activityDisplayTitle({
      ...activity('command', false),
      title: 'Run command',
      displayTarget: 'python3 analyze.py',
      displayDescription: 'Analyze color statistics',
    })).toBe('Running command: Analyze color statistics')
    expect(activityDisplayTitle({
      ...activity('fileRead', true),
      title: 'Read file',
      displayTarget: '/tmp/README.md',
    })).toBe('Read README.md')
  })

  test('keeps activity arguments and output in separate disclosure sections', () => {
    const item = {
      ...activity('tool', true),
      arguments: '{"cmd":"bun test"}',
      output: '12 pass',
      detail: 'Completed',
    }
    expect(activityDisclosureSections(item)).toEqual([
      { kind: 'arguments', label: 'Arguments', content: '{"cmd":"bun test"}' },
      { kind: 'output', label: 'Output', content: '12 pass' },
    ])
    expect(activityPreview({ ...item, detail: 'failed', output: '\npermission denied\nmore' }))
      .toBe('permission denied')
  })

  test('shows only the normalized command and output for command details', () => {
    const item = {
      ...activity('command', true),
      arguments: 'bun test',
      displayTarget: 'bun test',
      output: '12 pass',
      detail: 'Completed',
    }
    expect(activityDisclosureSections(item)).toEqual([
      { kind: 'command', label: 'Command', content: 'bun test' },
      { kind: 'output', label: 'Output', content: '12 pass' },
    ])
  })

  test('shows file edit stats only when every settled edit has counts', () => {
    const item = {
      ...activity('fileChange', true),
      fileChanges: [
        { path: 'a.ts', additions: 3, deletions: 1 },
        { path: 'b.ts', additions: 2, deletions: 4 },
      ],
    }
    expect(activityFileChangeStats(item)).toEqual({ additions: 5, deletions: 5 })
    expect(activityFileChangeStats({ ...item, fileChanges: [{ path: 'a.ts' }] })).toBeNull()
  })

  test('keeps the live working duration compact', () => {
    expect(formatWorkingElapsed(9)).toBe('9s')
    expect(formatWorkingElapsed(65)).toBe('1m 5s')
    expect(formatWorkingElapsed(3_720)).toBe('1h 2m')
  })

  test('adds desktop calendar context to message times', () => {
    const now = new Date(2026, 7, 9, 16, 0)
    expect(formatMessageTime(seconds(2026, 7, 9, 9, 5), now, 'en-US')).toBe('9:05 AM')
    expect(formatMessageTime(seconds(2026, 7, 8, 17, 0), now, 'en-US')).toBe('Yesterday 5:00 PM')
    expect(formatMessageTime(seconds(2026, 7, 7, 13, 12), now, 'en-US')).toBe('Friday 1:12 PM')
    expect(formatMessageTime(seconds(2026, 4, 12, 23, 0), now, 'en-US')).toBe('May 12th, 11:00 PM')
    expect(formatMessageTime(seconds(2024, 7, 4, 11, 0), now, 'en-US')).toBe('Aug 4th 2024, 11:00 AM')
  })

  test('formats message times without Intl.Locale and Intl.RelativeTimeFormat (Hermes)', () => {
    const globalIntl = Intl as unknown as Record<string, unknown>
    const savedLocale = globalIntl.Locale
    const savedRelative = globalIntl.RelativeTimeFormat
    globalIntl.Locale = undefined
    globalIntl.RelativeTimeFormat = undefined
    try {
      const now = new Date(2026, 7, 9, 16, 0)
      expect(formatMessageTime(seconds(2026, 7, 8, 17, 0), now, 'en-US')).toBe('Yesterday 5:00 PM')
      expect(formatMessageTime(seconds(2026, 4, 12, 23, 0), now, 'en-US')).toBe('May 12th, 11:00 PM')
    } finally {
      globalIntl.Locale = savedLocale
      globalIntl.RelativeTimeFormat = savedRelative
    }
  })

  test('puts one footer on the terminal answer and copies every visible part', () => {
    const { entries, runs } = transcript()
    const footers = assistantResponseFooters(entries, runs)
    expect([...footers.keys()]).toEqual(['message-3'])
    expect(footers.get('message-3')).toEqual({
      content: 'First half.\n\nSecond half.',
      timestamp: 200_000,
    })
  })

  test('gives a message outside any run its own footer', () => {
    const entries: TranscriptEntry[] = [message('assistant', 'Notice.', 0, undefined)]
    expect(assistantResponseFooters(entries, new Map()).get('message-0')).toEqual({
      content: 'Notice.',
      timestamp: 100_000,
    })
  })

  test('copies fenced code with the same language-marker handling as desktop', () => {
    expect(fencedCode('before\n```ts\nconst one = 1\n```\nafter\n```\nplain\n```')).toBe(
      'const one = 1\n\nplain',
    )
    expect(fencedCode('no code here')).toBeNull()
  })

  test('virtualizes huge activity output and bounds individual layout rows', () => {
    const manyLines = Array.from({ length: 201 }, (_, index) => `line ${index}`).join('\n')
    const manyRows = activityTextRows(manyLines)
    expect(manyRows).toHaveLength(201)
    expect(shouldVirtualizeActivityText(manyLines, manyRows)).toBe(true)

    const hugeLine = 'x'.repeat(4_500)
    const hugeRows = activityTextRows(hugeLine)
    expect(hugeRows.map((row) => row.length)).toEqual([2_000, 2_000, 500])
    expect(shouldVirtualizeActivityText(hugeLine, hugeRows)).toBe(false)

    const hugePayload = 'x'.repeat(40_001)
    expect(shouldVirtualizeActivityText(hugePayload, activityTextRows(hugePayload))).toBe(true)
  })

  test('uses the desktop fold boundary when work follows the final text part', () => {
    const rows = ['thought', 'answer one', 'answer two', 'tool']
    expect(turnAnswerStart(rows, (row) => row.startsWith('answer'))).toBe(1)
    expect(turnAnswerStart(['thought', 'tool'], () => false)).toBe(2)
  })

  test('does not publish a footer while the run is live or the answer streams', () => {
    const { entries, runs } = transcript()
    runs.set('run', { ...runs.get('run')!, status: 'running', live: true, completedAt: undefined })
    expect(assistantResponseFooters(entries, runs).size).toBe(0)

    const streaming = transcript()
    const last = streaming.entries.at(-1)!
    if (last.kind === 'message') last.message.streaming = true
    expect(assistantResponseFooters(streaming.entries, streaming.runs).size).toBe(0)
  })
})

function transcript(): { entries: TranscriptEntry[]; runs: Map<string, RunView> } {
  return {
    entries: [
      message('user', 'Build it', 0),
      message('assistant', 'Interim commentary.', 1),
      { kind: 'activity', key: 'activity-1', runId: 'run', position: 2, activity: activity('command', true) },
      message('assistant', 'First half.', 2),
      message('assistant', 'Second half.', 3),
    ],
    runs: new Map([['run', run('completed', 100, 200)]]),
  }
}

function message(
  role: 'user' | 'assistant' | 'system',
  text: string,
  index: number,
  runId: string | undefined = 'run',
): TranscriptEntry {
  return {
    kind: 'message',
    key: `message-${index}`,
    runId,
    position: index,
    message: {
      id: `message-${index}`,
      role,
      text,
      attachments: [],
      createdAt: (100 + index) * 1_000,
      streaming: false,
    },
  }
}

/** Epoch milliseconds, as v8 timestamps are. */
function seconds(year: number, month: number, day: number, hour: number, minute: number) {
  return new Date(year, month, day, hour, minute).getTime()
}

/** A run that started and finished at these second marks. */
function run(status: RunView['status'], startedAt: number, completedAt: number): RunView {
  return {
    id: 'run',
    ordinal: 1,
    status,
    requestedAt: startedAt * 1_000,
    startedAt: startedAt * 1_000,
    completedAt: completedAt * 1_000,
    live: false,
  }
}

function reasoning(complete: boolean, startedAt: number, finishedAt: number): ActivityView {
  return {
    ...activity('reasoning', complete),
    reasoning: {
      content: 'Checking',
      startedAtMs: startedAt,
      finishedAtMs: finishedAt,
    },
  }
}

function activity(kind: ActivityKind, complete: boolean): ActivityView {
  return {
    id: kind,
    kind,
    title: kind,
    failed: false,
    complete,
    imageUrls: [],
    fileChanges: [],
    omitted: false,
    streamsOutput: false,
  }
}
