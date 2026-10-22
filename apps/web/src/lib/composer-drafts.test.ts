import { describe, expect, test } from 'bun:test'
import type * as T from '@waku/client/v8'
import {
  composerDraftFor,
  moveComposerDraftToEmpty,
  setComposerDraft,
} from './composer-drafts'

describe('composer drafts', () => {
  test('updates one key without replacing unrelated client drafts', () => {
    const drafts: T.ComposerDrafts = { threads: { other: { text: 'from desktop' } } }
    const change = setComposerDraft(drafts, { type: 'newThread', projectId: 'waku' }, { text: 'from web' })

    expect(change).toEqual({
      target: { type: 'newThread', projectId: 'waku' },
      draft: { text: 'from web', attachments: [] },
    })
    expect(drafts.threads?.other?.text).toBe('from desktop')
  })

  test('an emptied draft is sent as a removal (no `draft`)', () => {
    const drafts: T.ComposerDrafts = { threads: { a: { text: 'x' } } }
    expect(setComposerDraft(drafts, { type: 'thread', threadId: 'a' }, { text: '' }))
      .toEqual({ target: { type: 'thread', threadId: 'a' } })
    expect(drafts.threads).toEqual({})
    expect(setComposerDraft(drafts, { type: 'thread', threadId: 'a' }, { text: '' })).toBeNull()
  })

  test('moves a new-task draft only when the destination is empty', () => {
    const drafts: T.ComposerDrafts = {
      newThreads: { first: { text: 'carry me' }, occupied: { text: 'keep me' } },
    }
    expect(moveComposerDraftToEmpty(
      drafts,
      { type: 'newThread', projectId: 'first' },
      { type: 'newThread', projectId: 'second' },
    )).toHaveLength(2)
    expect(composerDraftFor(drafts, { type: 'newThread', projectId: 'second' }).text).toBe('carry me')

    expect(moveComposerDraftToEmpty(
      drafts,
      { type: 'newThread', projectId: 'second' },
      { type: 'newThread', projectId: 'occupied' },
    )).toEqual([])
    expect(composerDraftFor(drafts, { type: 'newThread', projectId: 'occupied' }).text).toBe('keep me')
  })
})
