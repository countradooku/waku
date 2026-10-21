// The composer's remembered provider and model choices, per daemon. Provider
// ids are v8 provider instance ids (`codex`, `claude`, `opencode`, …).

import type { ModelSelection } from '@waku/client/v8'

const STORAGE_KEY = 'waku.composer-preferences.v2'

type StorageLike = Pick<Storage, 'getItem' | 'setItem'>

export interface RememberedModelTraits {
  reasoningEffort: string | null
  serviceTier: string | null
  contextWindow: string | null
}

export interface ComposerPreferences {
  lastProvider: string
  lastModel: string | null
  lastReasoningEffort: string | null
  lastServiceTier: string | null
  lastContextWindow: string | null
  modelTraits: Record<string, RememberedModelTraits>
}

const DEFAULT_PREFERENCES: ComposerPreferences = {
  lastProvider: 'codex',
  lastModel: null,
  lastReasoningEffort: null,
  lastServiceTier: null,
  lastContextWindow: null,
  modelTraits: {},
}

export function browserComposerPreferenceStorage(): StorageLike | null {
  if (typeof window === 'undefined') return null
  try {
    return window.localStorage
  } catch {
    return null
  }
}

export function readComposerPreferences(
  storage: StorageLike | null,
  daemonAddress: string,
): ComposerPreferences {
  if (!storage) return { ...DEFAULT_PREFERENCES }
  try {
    const entries = JSON.parse(storage.getItem(STORAGE_KEY) ?? '{}') as Record<string, unknown>
    return parsePreferences(entries[daemonAddress])
  } catch {
    return { ...DEFAULT_PREFERENCES }
  }
}

export function writeComposerPreferences(
  storage: StorageLike | null,
  daemonAddress: string,
  preferences: ComposerPreferences,
): void {
  if (!storage) return
  let entries: Record<string, unknown> = {}
  try {
    const parsed = JSON.parse(storage.getItem(STORAGE_KEY) ?? '{}')
    if (isRecord(parsed)) entries = parsed
  } catch {
    // Replace malformed disposable app state with the current preference.
  }
  entries[daemonAddress] = preferences
  try {
    storage.setItem(STORAGE_KEY, JSON.stringify(entries))
  } catch {
    // Composer choices still work when browser storage is unavailable.
  }
}

/** Remember a selection the user sent with. */
export function rememberComposerSelection(
  preferences: ComposerPreferences,
  selection: ModelSelection,
): ComposerPreferences {
  if (!selection.model) return preferences
  const reasoningEffort = selection.reasoningEffort ?? null
  const serviceTier = selection.serviceTier ?? null
  const contextWindow = selection.contextWindow ?? null
  return {
    ...preferences,
    lastProvider: selection.instance,
    lastModel: selection.model,
    lastReasoningEffort: reasoningEffort,
    lastServiceTier: serviceTier,
    lastContextWindow: contextWindow,
    modelTraits: {
      ...preferences.modelTraits,
      [modelKey(selection.instance, selection.model)]: { reasoningEffort, serviceTier, contextWindow },
    },
  }
}

export function rememberedModelTraits(
  preferences: ComposerPreferences,
  provider: string,
  model: string,
): RememberedModelTraits | undefined {
  return preferences.modelTraits[modelKey(provider, model)]
}

/** The selection a new task starts with. */
export function rememberedSelection(preferences: ComposerPreferences): ModelSelection {
  const selection: ModelSelection = { instance: preferences.lastProvider }
  if (preferences.lastModel) selection.model = preferences.lastModel
  if (preferences.lastReasoningEffort) selection.reasoningEffort = preferences.lastReasoningEffort
  if (preferences.lastServiceTier) selection.serviceTier = preferences.lastServiceTier
  if (preferences.lastContextWindow) selection.contextWindow = preferences.lastContextWindow
  return selection
}

function parsePreferences(value: unknown): ComposerPreferences {
  if (!isRecord(value) || typeof value.lastProvider !== 'string' || !value.lastProvider) {
    return { ...DEFAULT_PREFERENCES }
  }
  const modelTraits: Record<string, RememberedModelTraits> = {}
  if (isRecord(value.modelTraits)) {
    for (const [key, traits] of Object.entries(value.modelTraits)) {
      if (!isRecord(traits)) continue
      const reasoningEffort = nullableString(traits.reasoningEffort)
      const serviceTier = nullableString(traits.serviceTier)
      const contextWindow = nullableString(traits.contextWindow) ?? null
      if (reasoningEffort !== undefined && serviceTier !== undefined) {
        modelTraits[key] = { reasoningEffort, serviceTier, contextWindow }
      }
    }
  }
  return {
    lastProvider: value.lastProvider,
    lastModel: nullableString(value.lastModel) ?? null,
    lastReasoningEffort: nullableString(value.lastReasoningEffort) ?? null,
    lastServiceTier: nullableString(value.lastServiceTier) ?? null,
    lastContextWindow: nullableString(value.lastContextWindow) ?? null,
    modelTraits,
  }
}

function nullableString(value: unknown): string | null | undefined {
  return value === null || typeof value === 'string' ? value : undefined
}

function modelKey(provider: string, model: string): string {
  return `${provider}\u0000${model}`
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
