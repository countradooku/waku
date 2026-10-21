import type { AgentPreset } from '@waku/client/v8'

type Translator = (key: string) => string

const BUILT_IN_PRESET_KEYS: Record<string, { label: string; description: string }> = {
  standard: {
    label: 'agent_preset.standard',
    description: 'agent_preset.standard_description',
  },
  code: {
    label: 'agent_preset.code',
    description: 'agent_preset.code_description',
  },
  minimal: {
    label: 'agent_preset.minimal',
    description: 'agent_preset.minimal_description',
  },
  cordis: {
    label: 'agent_preset.creator',
    description: 'agent_preset.creator_description',
  },
}

export function agentPresetLabel(preset: AgentPreset, t: Translator) {
  if (preset.isCustom) return preset.name
  const keys = BUILT_IN_PRESET_KEYS[preset.id]
  return keys ? t(keys.label) : preset.name
}

export function agentPresetDescription(preset: AgentPreset, t: Translator) {
  if (preset.isCustom) return preset.description ?? null
  const keys = BUILT_IN_PRESET_KEYS[preset.id]
  return keys ? t(keys.description) : preset.description ?? null
}

export function agentPresetIdLabel(id: string, t: Translator) {
  const keys = BUILT_IN_PRESET_KEYS[id]
  return keys ? t(keys.label) : id
}
