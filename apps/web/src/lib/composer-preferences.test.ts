import { describe, expect, test } from 'bun:test'
import {
  readComposerPreferences,
  rememberedModelTraits,
  rememberedSelection,
  rememberComposerSelection,
  writeComposerPreferences,
} from './composer-preferences'

describe('composer preferences', () => {
  test('remembers the last model per daemon and its traits', () => {
    const storage = memoryStorage()
    const remembered = rememberComposerSelection(
      readComposerPreferences(storage, 'ws://first'),
      {
        instance: 'codex',
        model: 'gpt-5.6-sol',
        reasoningEffort: 'high',
        serviceTier: 'fast',
        contextWindow: '1m',
      },
    )
    writeComposerPreferences(storage, 'ws://first', remembered)

    expect(readComposerPreferences(storage, 'ws://first')).toMatchObject({
      lastProvider: 'codex',
      lastModel: 'gpt-5.6-sol',
      lastReasoningEffort: 'high',
      lastServiceTier: 'fast',
      lastContextWindow: '1m',
    })
    expect(rememberedModelTraits(remembered, 'codex', 'gpt-5.6-sol')).toEqual({
      reasoningEffort: 'high',
      serviceTier: 'fast',
      contextWindow: '1m',
    })
    expect(readComposerPreferences(storage, 'ws://second').lastModel).toBeNull()
  })

  test('does not erase an explicit model when a blank selection is chosen', () => {
    const preferences = rememberComposerSelection(
      readComposerPreferences(null, 'ws://first'),
      { instance: 'claude', model: 'claude-opus-4-1' },
    )
    expect(rememberComposerSelection(preferences, { instance: 'codex' })).toBe(preferences)
  })

  test('a new task starts from the remembered selection', () => {
    const preferences = rememberComposerSelection(
      readComposerPreferences(null, 'ws://first'),
      { instance: 'fx', model: 'openai/gpt-5.6-sol', reasoningEffort: 'low' },
    )
    expect(rememberedSelection(preferences)).toEqual({
      instance: 'fx',
      model: 'openai/gpt-5.6-sol',
      reasoningEffort: 'low',
    })
  })

  test('starts over from malformed storage', () => {
    const storage = memoryStorage()
    storage.setItem('waku.composer-preferences.v2', '{bad json')
    expect(readComposerPreferences(storage, 'ws://first').lastProvider).toBe('codex')
  })
})

function memoryStorage() {
  const values = new Map<string, string>()
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => { values.set(key, value) },
  }
}
