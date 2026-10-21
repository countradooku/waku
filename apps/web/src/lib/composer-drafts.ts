// Unsent composer text, keyed like the v8 `drafts` stream: per project for a
// task not started yet, per thread otherwise.

import type * as T from '@waku/client/v8'

export type DraftTarget =
  | { type: 'newThread'; projectId: string }
  | { type: 'thread'; threadId: string }

export interface ComposerDraftValue {
  text: string
  attachments: T.DraftAttachment[]
}

export function composerDraftId(target: DraftTarget): string {
  return target.type === 'newThread'
    ? `new:${target.projectId}`
    : `thread:${target.threadId}`
}

export function composerDraftFor(
  drafts: T.ComposerDrafts,
  target: DraftTarget,
): ComposerDraftValue {
  const draft = target.type === 'newThread'
    ? drafts.newThreads?.[target.projectId]
    : drafts.threads?.[target.threadId]
  return {
    text: draft?.text ?? '',
    attachments: draft?.attachments ?? [],
  }
}

/** Sets one draft in `drafts` and returns the change to send, or `null` when nothing changed. */
export function setComposerDraft(
  drafts: T.ComposerDrafts,
  target: DraftTarget,
  draft: Partial<ComposerDraftValue>,
): T.DraftChange | null {
  const normalized = normalizeDraft(draft)
  const current = composerDraftFor(drafts, target)
  if (draftsEqual(current, normalized)) return null
  const entries = target.type === 'newThread'
    ? (drafts.newThreads ??= {})
    : (drafts.threads ??= {})
  const id = target.type === 'newThread' ? target.projectId : target.threadId
  if (draftEmpty(normalized)) {
    delete entries[id]
    return { target }
  }
  entries[id] = normalized
  return { target, draft: normalized }
}

export function moveComposerDraftToEmpty(
  drafts: T.ComposerDrafts,
  source: DraftTarget,
  destination: DraftTarget,
): T.DraftChange[] {
  if (composerDraftId(source) === composerDraftId(destination)) return []
  const sourceDraft = composerDraftFor(drafts, source)
  if (draftEmpty(sourceDraft) || !draftEmpty(composerDraftFor(drafts, destination))) return []
  const changes: T.DraftChange[] = []
  const destinationChange = setComposerDraft(drafts, destination, sourceDraft)
  const sourceChange = setComposerDraft(drafts, source, { text: '', attachments: [] })
  if (destinationChange) changes.push(destinationChange)
  if (sourceChange) changes.push(sourceChange)
  return changes
}

function normalizeDraft(draft: Partial<ComposerDraftValue>): ComposerDraftValue {
  return {
    text: draft.text ?? '',
    attachments: draft.attachments ?? [],
  }
}

function draftEmpty(draft: ComposerDraftValue): boolean {
  return !(draft.text.length || draft.attachments.length)
}

function draftsEqual(left: ComposerDraftValue, right: ComposerDraftValue): boolean {
  return left.text === right.text
    && JSON.stringify(left.attachments) === JSON.stringify(right.attachments)
}
