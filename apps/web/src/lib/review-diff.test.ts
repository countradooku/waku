import { describe, expect, test } from 'bun:test'
import { hydratePartialDiff, parsePatchFiles } from '@pierre/diffs'
import {
  compactReviewPatch,
  createReviewDiffLoader,
  diffSourceChoices,
  reviewDiffSourceLabel,
  reviewTurns,
} from './review-diff'
import type { RunView } from './thread-model'

const translate = (key: string, params?: Record<string, string | number>) =>
  params ? `${key}(${JSON.stringify(params)})` : key

function runOf(id: string, status: RunView['status'], diffable: boolean): RunView {
  return { id, status, diffable, requestedAt: 0, live: false }
}

describe('review sources', () => {
  test('labels every review.diff source', () => {
    expect(reviewDiffSourceLabel('uncommitted')).toBe('Uncommitted')
    expect(reviewDiffSourceLabel('staged')).toBe('Staged')
    expect(reviewDiffSourceLabel('branch')).toBe('Branch')
    expect(reviewDiffSourceLabel('committed', (key) => key)).toBe('diff.source_committed')
  })

  test('numbers turns among the runs that were not rolled back', () => {
    const turns = reviewTurns([
      runOf('r1', 'completed', true),
      runOf('gone', 'rolled_back', false),
      runOf('r2', 'failed', false),
      runOf('r3', 'completed', true),
      runOf('r4', 'running', false),
    ])
    expect(turns).toEqual([{ runId: 'r1', turn: 1 }, { runId: 'r3', turn: 3 }])
  })

  test('labels the last turn, an earlier turn and the whole thread', () => {
    const turns = [{ runId: 'r1', turn: 1 }, { runId: 'r3', turn: 3 }]
    expect(reviewDiffSourceLabel({ type: 'turn', runId: 'r3' }, undefined, turns)).toBe('Last Turn')
    expect(reviewDiffSourceLabel({ type: 'turn', runId: 'r1' }, undefined, turns)).toBe('Turn 1')
    expect(reviewDiffSourceLabel({ type: 'turn', runId: 'r1' }, translate, turns)).toBe('diff.source_turn({"turn":1})')
    expect(reviewDiffSourceLabel({ type: 'turn', runId: 'r3' }, translate, turns)).toBe('diff.source_last_turn')
    expect(reviewDiffSourceLabel({ type: 'thread' }, translate, turns)).toBe('diff.source_all_turns')
  })

  test('the menu offers turns of a task around the working-tree sources', () => {
    const turns = [{ runId: 'r1', turn: 1 }, { runId: 'r2', turn: 2 }, { runId: 'r3', turn: 3 }]
    const choices = diffSourceChoices({ type: 'turn', runId: 'r2' }, turns)
    expect(choices.map((choice) => [choice.id, choice.label, choice.selected, Boolean(choice.separatorBefore)])).toEqual([
      ['turn:r3', 'Last Turn', false, false],
      ['thread', 'All Turns', false, false],
      ['uncommitted', 'Uncommitted', false, true],
      ['unstaged', 'Unstaged', false, false],
      ['staged', 'Staged', false, false],
      ['committed', 'Committed', false, true],
      ['branch', 'Branch', false, false],
      ['turn:r2', 'Turn 2', true, true],
      ['turn:r1', 'Turn 1', false, false],
    ])
    expect(choices[0]!.source).toEqual({ type: 'turn', runId: 'r3' })
  })

  test('a task without reviewable turns disables them; a new task has none', () => {
    const empty = diffSourceChoices('uncommitted', [])
    expect(empty.slice(0, 2).map((choice) => [choice.id, choice.disabled, choice.source])).toEqual([
      ['last-turn', true, null],
      ['thread', true, { type: 'thread' }],
    ])
    expect(diffSourceChoices('staged', undefined).map((choice) => choice.id)).toEqual([
      'uncommitted', 'unstaged', 'staged', 'committed', 'branch',
    ])
  })

  test('earlier turns are capped, keeping the selected one', () => {
    const turns = Array.from({ length: 6 }, (_, index) => ({ runId: `r${index + 1}`, turn: index + 1 }))
    const ids = diffSourceChoices({ type: 'turn', runId: 'r1' }, turns, undefined, 2)
      .filter((choice) => choice.id.startsWith('turn:'))
      .map((choice) => choice.id)
    expect(ids).toEqual(['turn:r6', 'turn:r5', 'turn:r4', 'turn:r1'])
  })
})

describe('compactReviewPatch', () => {
  test('trims hydrated context independently for every file', () => {
    const patch = hydratedFilePatch('one.txt', 20) + hydratedFilePatch('two.txt', 30)

    const compacted = compactReviewPatch(patch)
    const files = parsePatchFiles(compacted, 'test', true).flatMap((entry) => entry.files)

    expect(files).toHaveLength(2)
    expect(compacted).toContain('@@ -17,7 +17,7 @@')
    expect(compacted).toContain('@@ -27,7 +27,7 @@')
    expect(compacted).not.toContain('\n line 1\n')
    expect(compacted).not.toContain('\n line 40\n')
  })

  test('hydrates a compact file when unchanged context is expanded', async () => {
    const patch = hydratedFilePatch('one.txt', 20)
    const compacted = compactReviewPatch(patch)
    const file = parsePatchFiles(compacted, 'compact', true)[0]?.files[0]
    expect(file).toBeDefined()

    const loaded = await createReviewDiffLoader(patch, 'test')(file!)
    const hydrated = hydratePartialDiff('clone', file!, loaded)

    expect(hydrated.isPartial).toBe(false)
    expect(hydrated.deletionLines.join('')).toContain('line 1\n')
    expect(hydrated.additionLines.join('')).toContain('line 40\n')
  })
})

function hydratedFilePatch(name: string, changedLine: number): string {
  const lines = Array.from({ length: 40 }, (_, index) => ` line ${index + 1}`)
  lines[changedLine - 1] = `-line ${changedLine}`
  lines.splice(changedLine, 0, `+changed ${changedLine}`)
  return [
    `diff --git a/${name} b/${name}`,
    `--- a/${name}`,
    `+++ b/${name}`,
    '@@ -1,40 +1,40 @@',
    ...lines,
  ].join('\n') + '\n'
}
