import {
  parsePatchFiles,
  trimPatchContext,
  type FileContents,
  type FileDiffContentsLoader,
  type FileDiffMetadata,
} from '@pierre/diffs'
import { diffSourceKey, sameDiffSource, type DiffSource, type WorkingTreeDiffSource } from './daemon-api'
import type { RunView } from './thread-model'

const REVIEW_DIFF_CONTEXT_LINES = 3

type Translate = (key: string, params?: Record<string, string | number>) => string

/** A run whose changes `threads.turnDiff` can show, numbered among the live turns. */
export interface ReviewTurn {
  runId: string
  turn: number
}

/** Earlier turns the source menu lists besides the last one. */
export const REVIEW_TURN_MENU_LIMIT = 20

const WORKING_TREE_LABELS: Record<WorkingTreeDiffSource, [key: string, fallback: string]> = {
  uncommitted: ['diff.source_uncommitted', 'Uncommitted'],
  unstaged: ['diff.source_unstaged', 'Unstaged'],
  staged: ['diff.source_staged', 'Staged'],
  committed: ['diff.source_committed', 'Committed'],
  branch: ['diff.source_branch', 'Branch'],
}

/**
 * The turns a task can review, oldest first. Runs are numbered by their
 * position among the runs that were not rolled back, as the transcript
 * shows them; only those that ended with a ready checkpoint are listed.
 */
export function reviewTurns(runs: readonly Pick<RunView, 'id' | 'status' | 'diffable'>[]): ReviewTurn[] {
  const turns: ReviewTurn[] = []
  let turn = 0
  for (const run of runs) {
    if (run.status === 'rolled_back') continue
    turn += 1
    if (run.diffable) turns.push({ runId: run.id, turn })
  }
  return turns
}

export function reviewDiffSourceLabel(
  source: DiffSource,
  t?: Translate,
  turns: readonly ReviewTurn[] = [],
): string {
  const label = (key: string, fallback: string, params?: Record<string, string | number>) =>
    t ? t(key, params) : fallback
  if (typeof source === 'string') {
    const [key, fallback] = WORKING_TREE_LABELS[source]
    return label(key, fallback)
  }
  if (source.type === 'thread') return label('diff.source_all_turns', 'All Turns')
  const index = turns.findIndex((turn) => turn.runId === source.runId)
  if (index >= 0 && index === turns.length - 1) return label('diff.source_last_turn', 'Last Turn')
  const turn = index >= 0 ? turns[index]!.turn : '?'
  return label('diff.source_turn', `Turn ${turn}`, { turn })
}

export interface DiffSourceChoice {
  id: string
  label: string
  /** `null` for a disabled entry with nothing to review. */
  source: DiffSource | null
  selected: boolean
  disabled?: boolean
  separatorBefore?: boolean
}

/**
 * The review panel's source menu: a task's last turn and all its turns
 * (when `turns` is given), the working-tree sources, then earlier turns,
 * newest first.
 */
export function diffSourceChoices(
  selected: DiffSource,
  turns: readonly ReviewTurn[] | undefined,
  t?: Translate,
  limit = REVIEW_TURN_MENU_LIMIT,
): DiffSourceChoice[] {
  const choice = (source: DiffSource, extra: Partial<DiffSourceChoice> = {}): DiffSourceChoice => ({
    id: diffSourceKey(source),
    label: reviewDiffSourceLabel(source, t, turns),
    source,
    selected: sameDiffSource(selected, source),
    ...extra,
  })
  const choices: DiffSourceChoice[] = []
  if (turns) {
    const last = turns.at(-1)
    choices.push(last
      ? choice({ type: 'turn', runId: last.runId })
      : {
          id: 'last-turn',
          label: t ? t('diff.source_last_turn') : 'Last Turn',
          source: null,
          selected: false,
          disabled: true,
        })
    choices.push(choice({ type: 'thread' }, { disabled: !last }))
  }
  choices.push(
    choice('uncommitted', { separatorBefore: choices.length > 0 }),
    choice('unstaged'),
    choice('staged'),
    choice('committed', { separatorBefore: true }),
    choice('branch'),
  )
  if (turns && turns.length > 1) {
    const selectedRun = typeof selected !== 'string' && selected.type === 'turn' ? selected.runId : undefined
    const earlier = turns.slice(0, -1).reverse()
      .filter((turn, index) => index < limit || turn.runId === selectedRun)
    earlier.forEach((turn, index) => {
      choices.push(choice({ type: 'turn', runId: turn.runId }, { separatorBefore: index === 0 }))
    })
  }
  return choices
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
