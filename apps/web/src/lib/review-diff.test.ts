import { describe, expect, test } from 'bun:test'
import { hydratePartialDiff, parsePatchFiles } from '@pierre/diffs'
import {
  compactReviewPatch,
  createReviewDiffLoader,
  reviewDiffSourceLabel,
} from './review-diff'

describe('review sources', () => {
  test('labels every review.diff source', () => {
    expect(reviewDiffSourceLabel('uncommitted')).toBe('Uncommitted')
    expect(reviewDiffSourceLabel('staged')).toBe('Staged')
    expect(reviewDiffSourceLabel('branch')).toBe('Branch')
    expect(reviewDiffSourceLabel('committed', (key) => key)).toBe('diff.source_committed')
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
