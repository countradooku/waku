// Composer `/` and `@` completion over the v8 catalog (`composer.slashCommands`,
// `files.searchEntries`).

import type { FileEntry, ModelOption, SessionCommand, SlashCommand } from '@waku/client/v8'
import { fuzzyScore } from '@waku/client/fuzzy-search'

export const COMPOSER_AUTOCOMPLETE_CAP = 64

export type ComposerTriggerKind = 'command' | 'file'

export interface ComposerTrigger {
  kind: ComposerTriggerKind
  query: string
  start: number
  end: number
}

export type ComposerAutocompleteRow =
  | { kind: 'command'; command: SlashCommand }
  | { kind: 'file'; file: FileEntry }

export function detectComposerTrigger(text: string, cursor: number): ComposerTrigger | null {
  const end = Math.max(0, Math.min(cursor, text.length))
  const lineStart = text.lastIndexOf('\n', end - 1) + 1
  const linePrefix = text.slice(lineStart, end)
  if (linePrefix.startsWith('/')) {
    const query = linePrefix.slice(1)
    if (/\s/u.test(query)) return null
    return { kind: 'command', query, start: lineStart, end }
  }

  let tokenStart = end
  while (tokenStart > 0 && !/\s/u.test(text[tokenStart - 1]!)) tokenStart -= 1
  const token = text.slice(tokenStart, end)
  if (!token.startsWith('@')) return null
  return { kind: 'file', query: token.slice(1), start: tokenStart, end }
}

/** The scope of a command only the live provider session reported. */
export const SESSION_COMMAND_SCOPE = 'session'

/**
 * The catalog's slash commands (`composer.slashCommands`) with the ones the
 * live session reported (`ProviderSession.availableCommands`), one per name.
 * A catalog entry wins, and takes the session's description when it has none.
 */
export function mergeSessionCommands(
  catalog: readonly SlashCommand[],
  session: readonly SessionCommand[],
): SlashCommand[] {
  const merged = [...catalog]
  const byName = new Map(merged.map((command, index) => [command.name, index]))
  for (const command of session) {
    const name = command.name.replace(/^\//u, '').trim()
    if (!name) continue
    const index = byName.get(name)
    if (index === undefined) {
      byName.set(name, merged.length)
      merged.push({ name, description: command.description ?? '', scope: SESSION_COMMAND_SCOPE })
    } else if (!merged[index]!.description.trim() && command.description) {
      merged[index] = { ...merged[index]!, description: command.description }
    }
  }
  return merged
}

/** Slash commands in menu order: built-ins first, then project, user and skills. */
export function sortComposerCommands(commands: SlashCommand[]): SlashCommand[] {
  return [...commands].sort((left, right) => {
    const byScope = commandScopeRank(left.scope) - commandScopeRank(right.scope)
    return byScope || left.name.localeCompare(right.name)
  })
}

export function isFastModeToggleSubmission(
  provider: string,
  prompt: string,
  commands: SlashCommand[],
): boolean {
  return provider === 'codex'
    && prompt.trim() === '/fast'
    && commands.some((command) => command.name === 'fast'
      && command.scope === 'builtin'
      && !command.template)
}

/** Waku's provider-neutral terminal-session picker. */
export function isResumeSubmission(prompt: string): boolean {
  return prompt.trim() === '/resume'
}

export type GoalCommand =
  | { kind: 'show' }
  | { kind: 'edit' }
  | { kind: 'pause' }
  | { kind: 'resume' }
  | { kind: 'clear' }
  | { kind: 'set'; objective: string }

/**
 * Parse the submitted text as Codex's native `/goal` command, which Waku
 * bridges to `thread/goal/*`. `null` when it is not one — wrong provider,
 * other text, or a project/user command that deliberately owns `/goal`
 * (resolution precedence stands).
 */
export function parseGoalSubmission(
  provider: string,
  prompt: string,
  commands: SlashCommand[],
): GoalCommand | null {
  if (provider !== 'codex') return null
  const invocation = prompt.trim()
  if (!invocation.startsWith('/')) return null
  const body = invocation.slice(1)
  const split = body.match(/^(\S+)(?:\s+([\s\S]*))?$/u)
  if (!split || split[1] !== 'goal') return null
  const goalIsCodexBuiltin = commands.some((command) => command.name === 'goal'
    && command.scope === 'builtin'
    && !command.template)
  if (!goalIsCodexBuiltin) return null
  const argument = (split[2] ?? '').trim()
  switch (argument) {
    case '': return { kind: 'show' }
    case 'edit': return { kind: 'edit' }
    case 'pause': return { kind: 'pause' }
    case 'resume': return { kind: 'resume' }
    case 'clear': return { kind: 'clear' }
    default: return { kind: 'set', objective: argument }
  }
}

export function toggledFastServiceTier(
  current: string | null | undefined,
  serviceTiers: ModelOption[],
): string | null {
  const fast = serviceTiers.find((tier) => ['fast', 'priority'].includes(tier.id)
    || tier.label.toLocaleLowerCase() === 'fast')
  if (!fast) return null
  return current === fast.id ? 'default' : fast.id
}

export function composerAutocompleteRows(
  trigger: ComposerTrigger,
  commands: SlashCommand[],
  files: FileEntry[],
  cap = COMPOSER_AUTOCOMPLETE_CAP,
): ComposerAutocompleteRow[] {
  if (!trigger.query.trim()) {
    return trigger.kind === 'command'
      ? commands.slice(0, cap).map((command) => ({ kind: 'command', command }))
      : files.slice(0, cap).map((file) => ({ kind: 'file', file }))
  }
  const source = trigger.kind === 'command'
    ? commands.map((command) => ({
        row: { kind: 'command' as const, command },
        candidate: command.name,
      }))
    : files.map((file) => ({
        row: { kind: 'file' as const, file },
        candidate: file.path,
      }))
  return source
    .map(({ row, candidate }, index) => ({
      row,
      index,
      score: fuzzyScore(trigger.query, candidate),
    }))
    .filter((item): item is typeof item & { score: number } => item.score !== null)
    .sort((left, right) => right.score - left.score || left.index - right.index)
    .slice(0, cap)
    .map(({ row }) => row)
}

export function replaceComposerTrigger(
  text: string,
  trigger: ComposerTrigger,
  row: ComposerAutocompleteRow,
): { text: string; cursor: number } {
  const insert = row.kind === 'command'
    ? `/${row.command.name} `
    : `@${row.file.path} `
  const next = `${text.slice(0, trigger.start)}${insert}${text.slice(trigger.end)}`
  return { text: next, cursor: trigger.start + insert.length }
}

export function expandCommandTemplate(template: string, args: string): string {
  const positional = args.split(/\s+/u).filter(Boolean)
  let expanded = ''
  let consumedArgs = false
  let rest = template
  while (true) {
    const index = rest.indexOf('$')
    if (index < 0) break
    expanded += rest.slice(0, index)
    const after = rest.slice(index + 1)
    if (after.startsWith('ARGUMENTS')) {
      expanded += args
      consumedArgs = true
      rest = after.slice('ARGUMENTS'.length)
    } else if (after.startsWith('@')) {
      expanded += args
      consumedArgs = true
      rest = after.slice(1)
    } else if (/^[1-9]/u.test(after)) {
      expanded += positional[Number(after[0]) - 1] ?? ''
      consumedArgs = true
      rest = after.slice(1)
    } else {
      expanded += '$'
      rest = after
    }
  }
  expanded += rest
  if (!consumedArgs && args) expanded += `\n\n${args}`
  return expanded
}

export function expandedComposerSubmission(
  provider: string,
  prompt: string,
  commands: SlashCommand[],
): string | null {
  if (!prompt.startsWith('/')) return null
  const invocation = prompt.slice(1)
  const whitespace = invocation.search(/\s/u)
  const name = whitespace < 0 ? invocation : invocation.slice(0, whitespace)
  const args = whitespace < 0 ? '' : invocation.slice(whitespace).trim()
  const skill = commands.find((item) => item.name === name && item.scope === 'skill')
  if (skill) {
    if (provider === 'codex' || provider === 'fx') return `$${invocation}`
    if (provider === 'pi' || provider === 'ohmypi') return `/skill:${invocation}`
  }
  const command = commands.find((item) => item.name === name && item.template)
  return command?.template ? expandCommandTemplate(command.template, args) : null
}

function commandScopeRank(scope: SlashCommand['scope']): number {
  switch (scope) {
    case 'builtin':
    case 'waku':
      return 0
    case 'project':
      return 1
    case 'user':
      return 2
    default:
      return 3
  }
}
