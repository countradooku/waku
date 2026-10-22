// Orchestration commands the web dispatches (RFC 0001 "Commands", RFC 0003
// §5.3). Pure builders, so the payloads are unit-tested; task-actions.tsx
// sends them.

import type * as T from '@waku/client/v8'
import type { Task } from './thread-model'

export interface MessageInput {
  /** What the user typed. */
  text: string
  attachments: T.Attachment[]
  /** Provider-facing text when it differs (a slash-command expansion). */
  providerText?: string
}

/** How a message joins the thread: start or queue, or steer the active run. */
export type SendIntent = 'send' | 'queue' | 'steer'

/** The provider-facing text: what was typed plus `@` mentions of the attachments. */
export function providerText(input: MessageInput): string {
  if (input.providerText !== undefined) return input.providerText.trim()
  const mentions = input.attachments.map((attachment) => `@${attachment.mention}`).join(' ')
  return [input.text.trim(), mentions].filter(Boolean).join(' ')
}

export function dispatchMode(task: Pick<Task, 'activeRun' | 'supportsSteer'> | undefined, intent: SendIntent): T.DispatchMode {
  if (intent === 'steer' && task?.activeRun) {
    return { type: 'steer_active', targetRunId: task.activeRun.id }
  }
  // Starts an idle thread, waits behind an active run otherwise.
  return { type: 'queue_after_active' }
}

export function messageDispatch(
  threadId: string,
  messageId: string,
  input: MessageInput,
  mode: T.DispatchMode,
  options: { modelSelection?: T.ModelSelection } = {},
): Extract<T.Command, { type: 'message.dispatch' }> {
  const text = providerText(input)
  const typed = input.text.trim()
  const command: Extract<T.Command, { type: 'message.dispatch' }> = {
    type: 'message.dispatch',
    threadId,
    messageId,
    text,
    mode,
  }
  if (input.attachments.length) command.attachments = input.attachments
  if (typed !== text) command.displayText = typed
  if (typed) command.titleSeed = typed
  if (options.modelSelection) command.modelSelection = options.modelSelection
  return command
}

export interface NewTask {
  threadId: string
  projectId: string
  selection: T.ModelSelection
  runtimeMode: T.RuntimeMode
  interactionMode: T.InteractionMode
  /** A fresh worktree from this ref instead of the project checkout. */
  worktreeFrom?: string
  title?: string
}

export function workspaceStrategy(task: Pick<NewTask, 'worktreeFrom'>): T.WorkspaceStrategy {
  return task.worktreeFrom
    ? { type: 'new_worktree', baseRef: task.worktreeFrom }
    : { type: 'root' }
}

export function threadCreate(task: NewTask): Extract<T.Command, { type: 'thread.create' }> {
  const command: Extract<T.Command, { type: 'thread.create' }> = {
    type: 'thread.create',
    threadId: task.threadId,
    projectId: task.projectId,
    modelSelection: compactSelection(task.selection),
    runtimeMode: task.runtimeMode,
    interactionMode: task.interactionMode,
    workspace: workspaceStrategy(task),
  }
  if (task.title?.trim()) command.title = task.title.trim()
  return command
}

/** The orchestrator's driver tag of a provider instance id (`deepseek` → `deep_seek`). */
export function driverOfInstance(instanceId: string): T.DriverKind {
  const base = instanceId.split(':')[0] ?? instanceId
  return ({ deepseek: 'deep_seek', opencode: 'open_code', ohmypi: 'oh_my_pi' } as Record<string, string>)[base] ?? base
}

/** A thread that adopts a provider conversation listed by `providerSessions.list`. */
export function importedThreadCreate(
  threadId: string,
  projectId: string,
  summary: T.ProviderSessionSummary,
  runtimeMode: T.RuntimeMode,
): Extract<T.Command, { type: 'thread.create' }> {
  const command: Extract<T.Command, { type: 'thread.create' }> = {
    type: 'thread.create',
    threadId,
    projectId,
    modelSelection: { instance: summary.instanceId },
    runtimeMode,
    interactionMode: 'default',
    workspace: { type: 'root' },
    importedNativeThread: {
      driver: driverOfInstance(summary.instanceId),
      nativeId: summary.nativeId,
      strength: 'strong',
    },
  }
  if (summary.title.trim()) command.title = summary.title.trim()
  return command
}

/** The first message of a new task: a new worktree is prepared before the run starts. */
export function firstMessageMode(task: Pick<NewTask, 'worktreeFrom'>): T.DispatchMode {
  const workspace = workspaceStrategy(task)
  return workspace.type === 'new_worktree'
    ? { type: 'defer_start', workspace }
    : { type: 'start_immediately' }
}

/** Drops empty options, which the daemon would store as chosen values. */
export function compactSelection(selection: T.ModelSelection): T.ModelSelection {
  const compact: T.ModelSelection = { instance: selection.instance }
  if (selection.model) compact.model = selection.model
  if (selection.reasoningEffort) compact.reasoningEffort = selection.reasoningEffort
  if (selection.serviceTier) compact.serviceTier = selection.serviceTier
  if (selection.contextWindow) compact.contextWindow = selection.contextWindow
  if (selection.agentPreset) compact.agentPreset = selection.agentPreset
  return compact
}

export function sameSelection(left: T.ModelSelection, right: T.ModelSelection): boolean {
  return JSON.stringify(compactSelection(left)) === JSON.stringify(compactSelection(right))
}

/** Command ids derived from the entity they create, so a retry is a receipt hit. */
export const commandIds = {
  createThread: (threadId: string) => `thread.create:${threadId}`,
  dispatchMessage: (messageId: string) => `message.dispatch:${messageId}`,
  createProject: (projectId: string) => `project.create:${projectId}`,
  createScratchProject: (projectId: string) => `project.create_scratch:${projectId}`,
}
