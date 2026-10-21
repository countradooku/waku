import {
  parsePatchFiles,
  trimPatchContext,
  type FileContents,
  type FileDiffContentsLoader,
  type FileDiffMetadata,
} from '@pierre/diffs'
import type { DiffSource } from './daemon-api'

const REVIEW_DIFF_CONTEXT_LINES = 3

export function reviewDiffSourceLabel(
  source: DiffSource,
  t?: (key: string, params?: Record<string, string | number>) => string,
): string {
  if (t) return t({
    uncommitted: 'diff.source_uncommitted',
    unstaged: 'diff.source_unstaged',
    staged: 'diff.source_staged',
    committed: 'diff.source_committed',
    branch: 'diff.source_branch',
  }[source])
  return {
    uncommitted: 'Uncommitted',
    unstaged: 'Unstaged',
    staged: 'Staged',
    committed: 'Committed',
    branch: 'Branch',
  }[source]
}

export function compactReviewPatch(patch: string): string {
  // trimPatchContext handles one unified file patch at a time. Feeding it a
  // multi-file Git patch makes the following `diff --git` header look like
  // hunk contents, which produces invalid line counts.
  return patch
    .split(/(?=^diff --git )/m)
    .map((filePatch) => trimPatchContext(filePatch, REVIEW_DIFF_CONTEXT_LINES))
    .join('')
}

export function createReviewDiffLoader(
  patch: string,
  cacheKeyPrefix: string,
): FileDiffContentsLoader {
  let hydratedFiles: Map<string, FileDiffMetadata> | undefined

  return async (fileDiff) => {
    hydratedFiles ??= new Map(
      parsePatchFiles(patch, `${cacheKeyPrefix}:full`, true)
        .flatMap((entry) => entry.files)
        .map((file) => [fileIdentity(file), file]),
    )
    const hydrated = hydratedFiles.get(fileIdentity(fileDiff))
    if (!hydrated) throw new Error(`Could not hydrate diff context for ${fileDiff.name}`)

    const newFile = fileContents(hydrated, 'new')
    if (hydrated.type === 'rename-pure') return { oldFile: null, newFile }
    return {
      oldFile: fileContents(hydrated, 'old'),
      newFile,
    }
  }
}

function fileIdentity(file: FileDiffMetadata): string {
  return `${file.prevName ?? ''}\0${file.name}\0${file.type}`
}

function fileContents(file: FileDiffMetadata, side: 'old' | 'new'): FileContents {
  return {
    name: side === 'old' ? file.prevName ?? file.name : file.name,
    contents: (side === 'old' ? file.deletionLines : file.additionLines).join(''),
    cacheKey: file.cacheKey ? `${file.cacheKey}:${side}` : undefined,
  }
}
