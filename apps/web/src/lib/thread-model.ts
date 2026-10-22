// The web's view of protocol v8 state: shell rows become task summaries,
// a thread projection becomes a task with its transcript entries, queue,
// pending requests and background work. Everything here is pure, so it is
// tested by feeding recorded v8 stream items through the client reducers.

import type * as T from '@waku/client/v8'
import type { ShellState, ThreadState } from '@waku/client/v8'

export type TaskStatus = 'idle' | 'preparing' | 'working' | 'waiting' | 'finalizing' | 'failed'

export type ProviderId = string

export interface TaskSummary {
  id: string
  projectId: string
  title: string
  status: TaskStatus
  /** Epoch milliseconds, like every v8 timestamp. */
  createdAt: number
  updatedAt: number
  lastReplyAt?: number
  lastVisitedAt?: number
  archived: boolean
  pinned: boolean
  pendingRequestCount: number
  queuedRunCount: number
  selection: T.ModelSelection
  runtimeMode: T.RuntimeMode
  interactionMode: T.InteractionMode
  worktreePath?: string
  branch?: string
  thread: T.AppThread
}

export interface QueuedMessage {
  runId: string
  messageId: string
  text: string
  displayText?: string
  attachments: T.Attachment[]
  requestedAt: number
  held: boolean
}

export interface PendingApproval {
  requestId: string
  title: string
  detail: string
  options: Array<{ id: string; label: string; allow: boolean }>
}

export interface PendingUserInput {
  requestId: string
  questions: Array<{
    id: string
    header: string
    question: string
    options: Array<{ label: string; description?: string }>
    multiSelect: boolean
  }>
}

export type ActivityKind =
  | 'reasoning'
  | 'command'
  | 'fileChange'
  | 'fileRead'
  | 'fileSearch'
  | 'fileList'
  | 'search'
  | 'plan'
  | 'tool'

export interface ActivityFileChange {
  path: string
  additions?: number
  deletions?: number
  status?: 'added' | 'modified' | 'deleted' | 'renamed'
  diff?: string
}

/** One activity row, built from a tool, reasoning or plan turn item. */
export interface ActivityView {
  id: string
  kind: ActivityKind
  title: string
  toolName?: string
  mcpServer?: string
  detail?: string
  arguments?: string
  output?: string
  imageUrls: string[]
  failed: boolean
  complete: boolean
  fileChanges: ActivityFileChange[]
  displayTarget?: string
  displayDescription?: string
  reasoning?: { content: string; startedAtMs: number; finishedAtMs: number }
  /** The wire projection left a body out; `threads.turnItem` or `commandOutput` has it. */
  omitted: boolean
  /** A command whose output streams through `commandOutput`. */
  streamsOutput: boolean
  exitCode?: number
}

export interface TranscriptMessage {
  /** The turn item id. */
  id: string
  messageId?: string
  role: 'user' | 'assistant' | 'system'
  text: string
  /** What the user typed, before attachment mentions. */
  displayText?: string
  attachments: T.Attachment[]
  createdAt: number
  streaming: boolean
  tone?: 'error'
}

export interface CheckpointView {
  id: string
  scopeId: string
  /** The run that ended with this checkpoint; its turn diff reviews it. */
  runId?: string
  files: Array<{ path: string; additions: number; deletions: number }>
  additions: number
  deletions: number
}

export type TranscriptEntry =
  | { kind: 'message'; key: string; runId?: string; position: number; message: TranscriptMessage }
  | { kind: 'activity'; key: string; runId?: string; position: number; activity: ActivityView }
  | { kind: 'checkpoint'; key: string; runId?: string; position: number; checkpoint: CheckpointView }

export interface RunView {
  id: string
  ordinal?: number
  status: T.RunStatus
  requestedAt: number
  startedAt?: number
  completedAt?: number
  live: boolean
  failure?: T.ProviderFailure
  /** The run ended with a ready checkpoint, so `threads.turnDiff` can review it. */
  diffable: boolean
}

export type BackgroundWorkKind = 'subagent' | 'process' | 'monitor'
export type BackgroundWorkStatus = 'pending' | 'running' | 'completed' | 'failed' | 'stopped'

export interface BackgroundWorkItem {
  id: string
  kind: BackgroundWorkKind
  title: string
  detail?: string
  output?: string
  status: BackgroundWorkStatus
  startedAt?: number
  updatedAt?: number
  model?: string
  runId?: string
  /**
   * The turn item a transcript row links to: the subagent's item, or the
   * command item whose output a process streams (`commandOutput`).
   */
  itemId?: string
  /** The provider thread holding a process or monitor (`background_work.stop`). */
  providerThreadId?: string
  canStop: boolean
}

/** A task being composed: nothing exists on the daemon until its first message. */
export interface NewTaskDraft {
  /** Client-side key, stable while the draft is shown. */
  id: string
  projectId: string
  selection: T.ModelSelection
  runtimeMode: T.RuntimeMode
  interactionMode: T.InteractionMode
  /** Work in a fresh worktree instead of the project checkout. */
  isolated: boolean
  /** The ref a fresh worktree starts from; the current branch when absent. */
  baseRef?: string
}

export interface RewindTarget {
  checkpointId: string
  scopeId: string
}

export interface Task extends TaskSummary {
  runs: RunView[]
  activeRun?: RunView
  queued: QueuedMessage[]
  entries: TranscriptEntry[]
  approval?: PendingApproval
  userInput?: PendingUserInput
  goal?: T.ThreadGoal
  contextUsage?: T.ContextUsage
  capabilities?: T.Capabilities
  supportsSteer: boolean
  rollback?: T.RollbackState
  /** Per run id: the checkpoint that restores the state before that run. */
  rewindTargets: Map<string, RewindTarget>
  backgroundWork: BackgroundWorkItem[]
  /** Slash commands the thread's open provider sessions reported. */
  sessionCommands: T.SessionCommand[]
  hasMoreHistory: boolean
  synchronized: boolean
}

const ACTIVE_RUN: ReadonlySet<string> = new Set(['preparing', 'queued', 'starting', 'running', 'waiting', 'finalizing'])
const TERMINAL_ITEM: ReadonlySet<string> = new Set(['completed', 'failed', 'cancelled', 'interrupted'])

export function isBusy(status: TaskStatus): boolean {
  return status === 'preparing' || status === 'working' || status === 'waiting' || status === 'finalizing'
}

export function taskStatusFromActivity(activity: T.ThreadActivity): TaskStatus {
  switch (activity) {
    case 'preparing':
    case 'working':
    case 'waiting':
    case 'finalizing':
    case 'failed':
      return activity as TaskStatus
    default:
      return 'idle'
  }
}

export function displayTitle(task: Pick<TaskSummary, 'title'> & { thread?: T.AppThread }): string {
  return task.title.trim() || 'New task'
}

export function summarizeThread(thread: T.AppThread, row?: T.ThreadRow): TaskSummary {
  return {
    id: thread.id,
    projectId: thread.projectId,
    title: thread.title,
    status: row ? taskStatusFromActivity(row.activity) : 'idle',
    createdAt: thread.createdAt,
    updatedAt: thread.updatedAt,
    lastReplyAt: thread.lastReplyAt,
    lastVisitedAt: thread.lastVisitedAt,
    archived: thread.archivedAt !== undefined,
    pinned: thread.pinnedAt !== undefined,
    pendingRequestCount: row?.pendingRequestCount ?? 0,
    queuedRunCount: row?.queuedRunCount ?? 0,
    selection: thread.modelSelection,
    runtimeMode: thread.runtimeMode,
    interactionMode: thread.interactionMode,
    worktreePath: thread.worktreePath,
    branch: thread.branch,
    thread,
  }
}

/** Projects in their sidebar order, and the shell's tasks. */
export function shellView(state: ShellState): { projects: T.Project[]; tasks: TaskSummary[] } {
  const projects = [...state.projects.values()]
    .filter((project) => project.removedAt === undefined)
    .sort((left, right) => compareKeys(left.orderKey, right.orderKey) || left.createdAt - right.createdAt)
  const tasks = [...state.threads.values()]
    .filter((row) => row.thread.deletedAt === undefined)
    .map((row) => summarizeThread(row.thread, row))
  return { projects, tasks }
}

function compareKeys(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0
}

function runView(run: T.Run, checkpoints: ReadonlyMap<string, T.Checkpoint>): RunView {
  return {
    id: run.id,
    ordinal: run.ordinal,
    status: run.status,
    requestedAt: run.requestedAt,
    startedAt: run.startedAt,
    completedAt: run.completedAt,
    live: ACTIVE_RUN.has(run.status) && run.status !== 'queued',
    failure: run.failure,
    diffable: run.status !== 'rolled_back' && readyCheckpoint(checkpoints, run.checkpointId) !== undefined,
  }
}

function statusOfRuns(runs: T.Run[]): TaskStatus {
  const active = runs.find((run) => ACTIVE_RUN.has(run.status) && run.status !== 'queued')
  if (active) {
    if (active.status === 'preparing' || active.status === 'starting') return 'preparing'
    if (active.status === 'waiting') return 'waiting'
    if (active.status === 'finalizing') return 'finalizing'
    return 'working'
  }
  const last = runs
    .filter((run) => run.ordinal !== undefined && run.status !== 'rolled_back')
    .sort((left, right) => (right.ordinal ?? 0) - (left.ordinal ?? 0))[0]
  return last?.status === 'failed' ? 'failed' : 'idle'
}

/** The open thread as the task screen shows it. */
export function taskFromThread(state: ThreadState, row?: T.ThreadRow): Task | undefined {
  const thread = state.thread
  if (!thread) return undefined
  const allRuns = [...state.runs.values()]
  const runs = allRuns
    .filter((run) => run.ordinal !== undefined)
    .sort((left, right) => (left.ordinal ?? 0) - (right.ordinal ?? 0))
    .map((run) => runView(run, state.checkpoints))
  const status = statusOfRuns(allRuns)
  const activeRun = runs.find((run) => run.live)
  const providerThread = activeProviderThread(state)
  const openSessions = [...state.providerSessions.values()]
    .filter((candidate) => candidate.detachedAt === undefined)
    .sort((left, right) => right.updatedAt - left.updatedAt)
  const session = openSessions[0]
  const capabilities = session?.capabilities
  return {
    ...summarizeThread(thread, row),
    status,
    runs,
    activeRun,
    queued: queuedMessages(state),
    entries: transcriptEntries(state),
    approval: pendingApproval(state),
    userInput: pendingUserInput(state),
    goal: providerThread?.goal,
    contextUsage: providerThread?.contextUsage,
    capabilities,
    supportsSteer: Boolean(capabilities?.turns?.active_steering || capabilities?.turns?.steering_by_interrupt_restart),
    rollback: thread.rollback,
    rewindTargets: rewindTargets(state),
    backgroundWork: backgroundWork(state),
    sessionCommands: sessionCommands(openSessions.filter((candidate) =>
      candidate.providerInstanceId === thread.modelSelection.instance)),
    hasMoreHistory: state.hasMoreHistory,
    synchronized: state.synchronized,
  }
}

function activeProviderThread(state: ThreadState): T.ProviderThread | undefined {
  const id = state.thread?.activeProviderThreadId
  if (id) return state.providerThreads.get(id)
  return [...state.providerThreads.values()]
    .filter((candidate) => candidate.ownerNodeId === undefined)
    .sort((left, right) => right.updatedAt - left.updatedAt)[0]
}

export function queuedMessages(state: ThreadState): QueuedMessage[] {
  return [...state.runs.values()]
    .filter((run) => run.status === 'queued')
    .sort((left, right) =>
      (left.queuePosition ?? 0) - (right.queuePosition ?? 0) || left.requestedAt - right.requestedAt)
    .map((run) => {
      const message = state.messages.get(run.userMessageId)
      return {
        runId: run.id,
        messageId: run.userMessageId,
        text: message?.text ?? '',
        displayText: message?.displayText,
        attachments: message?.attachments ?? [],
        requestedAt: run.requestedAt,
        held: run.queueHeld ?? false,
      }
    })
}

function approvalItem(state: ThreadState, requestId: string) {
  for (const item of state.turnItems.values()) {
    if (item.kind.type === 'approval_request' && item.kind.requestId === requestId) return item
  }
  return undefined
}

export function pendingApproval(state: ThreadState): PendingApproval | undefined {
  const request = [...state.runtimeRequests.values()]
    .filter((candidate) =>
      candidate.status === 'pending'
      && candidate.response === undefined
      && candidate.kind !== 'user_input'
      && candidate.kind !== 'dynamic_tool_call'
      && candidate.kind !== 'auth_refresh')
    .sort((left, right) => left.createdAt - right.createdAt)[0]
  if (!request) return undefined
  const item = approvalItem(state, request.id)
  const prompt = item?.kind.type === 'approval_request' ? item.kind.prompt : undefined
  const options = request.options?.length
    ? request.options
    : item?.kind.type === 'approval_request' ? item.kind.options ?? [] : []
  return {
    requestId: request.id,
    title: item?.title ?? approvalTitle(request.kind),
    detail: prompt ?? '',
    options: options.map((option) => ({ id: option.id, label: option.label, allow: option.allow ?? false })),
  }
}

function approvalTitle(kind: T.RuntimeRequestKind): string {
  switch (kind) {
    case 'command_approval':
      return 'Run this command?'
    case 'file_read_approval':
      return 'Read this file?'
    case 'file_change_approval':
    case 'apply_patch_approval':
      return 'Apply these changes?'
    default:
      return 'Permission requested'
  }
}

export function pendingUserInput(state: ThreadState): PendingUserInput | undefined {
  const request = [...state.runtimeRequests.values()]
    .filter((candidate) => candidate.status === 'pending' && candidate.response === undefined && candidate.kind === 'user_input')
    .sort((left, right) => left.createdAt - right.createdAt)[0]
  if (!request) return undefined
  let questions = request.questions
  if (!questions?.length) {
    for (const item of state.turnItems.values()) {
      if (item.kind.type === 'user_input_request' && item.kind.requestId === request.id) questions = item.kind.questions
    }
  }
  return {
    requestId: request.id,
    questions: (questions ?? []).map((question) => ({
      id: question.id,
      header: question.header,
      question: question.question,
      options: (question.options ?? []).map((option) => ({ label: option.label, description: option.description })),
      multiSelect: question.multiSelect ?? false,
    })),
  }
}

function readyCheckpoint(
  checkpoints: ReadonlyMap<string, T.Checkpoint>,
  id: string | undefined,
): T.Checkpoint | undefined {
  const checkpoint = id === undefined ? undefined : checkpoints.get(id)
  return checkpoint?.status === 'ready' ? checkpoint : undefined
}

/**
 * Per run id, the ready checkpoints it may have started from: ordinal 0 of
 * its root scope (the run's own ending checkpoint is excluded per run).
 */
function turnStartCandidates(state: ThreadState): Map<string, T.Checkpoint[]> {
  const byRun = new Map<string, T.Checkpoint[]>()
  for (const checkpoint of state.checkpoints.values()) {
    if (
      checkpoint.runId === undefined
      || checkpoint.ordinalWithinScope !== 0
      || checkpoint.status !== 'ready'
      || state.checkpointScopes.get(checkpoint.scopeId)?.kind !== 'root_run'
    ) continue
    const candidates = byRun.get(checkpoint.runId) ?? []
    candidates.push(checkpoint)
    byRun.set(checkpoint.runId, candidates)
  }
  return byRun
}

/**
 * Rewinding to a run's message rolls back that run and every later one, to
 * the state just before it started: its turn-start checkpoint, else the one
 * the previous live run ended with (waku-orchestrator `rewind_point`). The
 * first message can be rewound through its turn-start checkpoint.
 */
export function rewindTargets(state: ThreadState): Map<string, RewindTarget> {
  const live = [...state.runs.values()]
    .filter((run) => run.ordinal !== undefined && run.status !== 'rolled_back')
    .sort((left, right) => left.ordinal! - right.ordinal!)
  const starts = turnStartCandidates(state)
  const targets = new Map<string, RewindTarget>()
  let previousEnd: T.Checkpoint | undefined
  for (const run of live) {
    const start = starts.get(run.id)?.find((checkpoint) => checkpoint.id !== run.checkpointId)
    const target = start?.appRunOrdinal !== undefined ? start : previousEnd
    if (target) targets.set(run.id, { checkpointId: target.id, scopeId: target.scopeId })
    previousEnd = readyCheckpoint(state.checkpoints, run.checkpointId) ?? previousEnd
  }
  return targets
}

/** The slash commands open sessions reported, newest session first, one per name. */
export function sessionCommands(sessions: readonly T.ProviderSession[]): T.SessionCommand[] {
  const commands = new Map<string, T.SessionCommand>()
  for (const session of sessions) {
    for (const command of session.availableCommands ?? []) {
      const name = command.name.replace(/^\//, '').trim()
      if (!name) continue
      const known = commands.get(name)
      if (!known) commands.set(name, { ...command, name })
      else if (!known.description && command.description) commands.set(name, { ...known, description: command.description })
    }
  }
  return [...commands.values()]
}

function subagentStatus(status: T.SubagentStatus): BackgroundWorkStatus {
  switch (status) {
    case 'completed':
      return 'completed'
    case 'failed':
      return 'failed'
    case 'cancelled':
    case 'interrupted':
      return 'stopped'
    case 'idle':
    case 'pending':
      return 'pending'
    default:
      return 'running'
  }
}

export function backgroundWork(state: ThreadState): BackgroundWorkItem[] {
  const subagentItems = new Map<string, string>()
  for (const item of state.turnItems.values()) {
    if (item.kind.type === 'subagent') subagentItems.set(item.kind.subagentId, item.id)
  }
  const items: BackgroundWorkItem[] = [...state.subagents.values()].map((subagent) => {
    const status = subagentStatus(subagent.status)
    return {
      id: subagent.id,
      kind: 'subagent',
      title: subagent.title?.trim() || firstLine(subagent.prompt) || 'Subagent',
      detail: subagent.progress,
      output: subagent.result,
      status,
      startedAt: subagent.startedAt,
      updatedAt: subagent.updatedAt,
      model: subagent.model,
      runId: subagent.runId,
      itemId: subagentItems.get(subagent.id),
      canStop: (status === 'running' || status === 'pending') && subagent.runId !== undefined,
    }
  })
  for (const providerThread of state.providerThreads.values()) {
    for (const task of providerThread.pendingBackgroundTasks ?? []) {
      if (task.kind === 'subagent' || items.some((item) => item.id === task.taskId)) continue
      const commandItem = task.itemId ? state.turnItems.get(task.itemId) : undefined
      items.push({
        id: task.taskId,
        kind: task.kind === 'monitor' ? 'monitor' : 'process',
        title: task.description?.trim() || commandTitle(commandItem) || task.taskId,
        status: 'running',
        startedAt: commandItem?.startedAt,
        updatedAt: commandItem?.updatedAt,
        runId: commandItem?.runId,
        itemId: task.itemId,
        providerThreadId: providerThread.id,
        canStop: true,
      })
    }
  }
  return items.sort((left, right) => (left.startedAt ?? 0) - (right.startedAt ?? 0))
}

/** A command item's first input line: the title of a process without a description. */
function commandTitle(item: T.TurnItem | undefined): string {
  return item?.kind.type === 'command_execution' ? firstLine(item.kind.input) : ''
}

function firstLine(text: string | undefined): string {
  return text?.split('\n').find((line) => line.trim())?.trim() ?? ''
}

function fileStatus(operation: string | undefined): ActivityFileChange['status'] {
  switch (operation) {
    case 'add':
    case 'added':
    case 'create':
      return 'added'
    case 'delete':
    case 'deleted':
    case 'remove':
      return 'deleted'
    case 'move':
    case 'rename':
    case 'renamed':
      return 'renamed'
    default:
      return 'modified'
  }
}

function prettyJson(text: string | undefined): string | undefined {
  if (!text?.trim()) return undefined
  try {
    return JSON.stringify(JSON.parse(text), null, 2)
  } catch {
    return text
  }
}

function toolOutput(text: string | undefined): { output?: string; images: string[] } {
  if (!text?.trim()) return { images: [] }
  try {
    const parsed = JSON.parse(text) as unknown
    if (typeof parsed === 'string') return { output: parsed, images: blobRefs(parsed) }
    const images: string[] = []
    collectImages(parsed, images)
    return { output: JSON.stringify(parsed, null, 2), images }
  } catch {
    return { output: text, images: blobRefs(text) }
  }
}

function blobRefs(text: string): string[] {
  return [...text.matchAll(/waku-blob:[A-Za-z0-9._-]+/g)].map((match) => match[0])
}

function collectImages(value: unknown, images: string[]): void {
  if (typeof value === 'string') {
    if (value.startsWith('waku-blob:')) images.push(value)
    return
  }
  if (Array.isArray(value)) {
    for (const entry of value) collectImages(entry, images)
    return
  }
  if (value && typeof value === 'object') {
    for (const entry of Object.values(value)) collectImages(entry, images)
  }
}

/** The activity row of a tool, reasoning or plan item; `undefined` for message-like items. */
export function activityFromItem(item: T.TurnItem): ActivityView | undefined {
  const kind = item.kind
  const complete = TERMINAL_ITEM.has(item.status)
  const failed = item.status === 'failed'
  const base = {
    id: item.id,
    title: item.title ?? '',
    imageUrls: [] as string[],
    failed,
    complete,
    fileChanges: [] as ActivityFileChange[],
    omitted: false,
    streamsOutput: false,
  }
  switch (kind.type) {
    case 'reasoning':
      return {
        ...base,
        kind: 'reasoning',
        title: item.title ?? 'Reasoning',
        complete: complete || kind.streaming === false,
        reasoning: {
          content: kind.text,
          startedAtMs: item.startedAt ?? item.updatedAt,
          finishedAtMs: item.completedAt ?? item.updatedAt,
        },
      }
    case 'command_execution':
      return {
        ...base,
        kind: 'command',
        title: item.title ?? 'command',
        failed: failed || Boolean(kind.outputIndicatesFailure),
        displayTarget: firstLine(kind.input),
        arguments: kind.input,
        output: kind.output,
        omitted: Boolean(kind.outputOmitted),
        streamsOutput: true,
        exitCode: kind.exitCode,
      }
    case 'file_change': {
      const changes = kind.changes?.length
        ? kind.changes.map((change) => ({ path: change.path, status: fileStatus(change.operation) }))
        : [{ path: kind.fileName, status: 'modified' as const }]
      const fileChanges: ActivityFileChange[] = changes.map((change, index) => ({
        ...change,
        additions: changes.length === 1 ? kind.additions : undefined,
        deletions: changes.length === 1 ? kind.deletions : undefined,
        diff: index === 0 && changes.length === 1 ? kind.diff : undefined,
      }))
      return {
        ...base,
        kind: 'fileChange',
        title: item.title ?? 'file change',
        fileChanges,
        detail: kind.diff,
        omitted: Boolean(kind.bodyOmitted),
      }
    }
    case 'file_search':
      return {
        ...base,
        kind: 'fileSearch',
        title: item.title ?? 'file search',
        displayTarget: kind.pattern,
        output: kind.results?.map((result) =>
          [result.fileName, result.line, result.column].filter((part) => part !== undefined).join(':')
          + (result.preview ? `  ${result.preview}` : '')).join('\n'),
      }
    case 'web_search':
      return {
        ...base,
        kind: 'search',
        title: item.title ?? 'web search',
        displayTarget: kind.patterns?.join(', '),
        output: kind.results?.map((result) => [result.title, result.url, result.snippet].filter(Boolean).join('\n')).join('\n\n'),
      }
    case 'todo_list':
      return {
        ...base,
        kind: 'plan',
        title: item.title ?? 'plan',
        detail: [
          kind.explanation,
          ...kind.steps.map((step) => `${step.status === 'completed' ? '[x]' : step.status === 'running' ? '[~]' : '[ ]'} ${step.text}`),
        ].filter(Boolean).join('\n'),
      }
    case 'proposed_plan':
      return {
        ...base,
        kind: 'plan',
        title: item.title ?? 'plan',
        complete: complete || kind.streaming === false,
        detail: kind.markdown,
      }
    case 'dynamic_tool': {
      const output = toolOutput(kind.output)
      const nonExecution = item.tool?.nonExecutionKind
      return {
        ...base,
        kind: nonExecution === 'read' ? 'fileRead' : nonExecution === 'list' ? 'fileList' : 'tool',
        title: kind.toolName ?? item.title ?? 'tool',
        toolName: kind.toolName,
        mcpServer: item.tool?.source?.kind === 'integration' ? item.tool.source.name : undefined,
        arguments: prettyJson(kind.input),
        output: output.output,
        imageUrls: [...output.images, ...(kind.viewedImagePath ? [kind.viewedImagePath] : [])],
        omitted: Boolean(kind.outputOmitted),
      }
    }
    case 'subagent':
      return {
        ...base,
        kind: 'tool',
        title: item.title ?? 'Subagent',
        displayTarget: firstLine(kind.prompt),
        arguments: kind.prompt,
        detail: kind.progress,
        output: kind.result,
      }
    case 'approval_request':
      return {
        ...base,
        kind: 'tool',
        title: item.title ?? 'Permission',
        detail: kind.prompt,
      }
    case 'user_input_request':
      return {
        ...base,
        kind: 'tool',
        title: 'AskUserQuestion',
        arguments: kind.questions.map((question) => question.question).join('\n'),
        output: kind.answers?.map((answer) => answer.answers.join(', ')).join('\n'),
      }
    default:
      return undefined
  }
}

function messageFromItem(item: T.TurnItem): TranscriptMessage | undefined {
  const kind = item.kind
  const createdAt = item.startedAt ?? item.updatedAt
  const base = { id: item.id, attachments: [] as T.Attachment[], createdAt, streaming: false }
  switch (kind.type) {
    case 'user_message':
      return {
        ...base,
        messageId: kind.messageId,
        role: 'user',
        text: kind.text,
        displayText: kind.displayText,
        attachments: kind.attachments ?? [],
      }
    case 'assistant_message':
      return { ...base, messageId: kind.messageId, role: 'assistant', text: kind.text, streaming: Boolean(kind.streaming) }
    case 'notification':
      return { ...base, role: 'system', text: kind.summary }
    case 'system_notice':
    case 'run_interrupt_request':
      return { ...base, role: 'system', text: kind.message }
    case 'error':
      return { ...base, role: 'system', text: kind.failure.message, tone: 'error' }
    case 'compaction':
      return { ...base, role: 'system', text: kind.summary?.trim() ? `Context compacted: ${firstLine(kind.summary)}` : 'Context compacted' }
    case 'handoff':
      return { ...base, role: 'system', text: `Context handed to ${kind.toProviderInstanceId}` }
    case 'secret_request':
      return { ...base, role: 'system', text: kind.reason }
    default:
      return undefined
  }
}

function checkpointFromItem(state: ThreadState, item: T.TurnItem): CheckpointView | undefined {
  if (item.kind.type !== 'checkpoint') return undefined
  const stored = state.checkpoints.get(item.kind.checkpointId)
  if (stored && stored.status !== 'ready') return undefined
  const files = (item.kind.files ?? stored?.files ?? []).map((file) => ({
    path: file.path,
    additions: file.additions,
    deletions: file.deletions,
  }))
  if (!files.length) return undefined
  return {
    id: item.kind.checkpointId,
    scopeId: item.kind.scopeId,
    runId: item.runId,
    files,
    additions: files.reduce((total, file) => total + file.additions, 0),
    deletions: files.reduce((total, file) => total + file.deletions, 0),
  }
}

/** Turn items in position order, without rolled-back runs, as transcript entries. */
export function transcriptEntries(state: ThreadState): TranscriptEntry[] {
  const rolledBack = new Set(
    [...state.runs.values()].filter((run) => run.status === 'rolled_back').map((run) => run.id),
  )
  const items = [...state.turnItems.values()]
    .filter((item) => !item.runId || !rolledBack.has(item.runId))
    .filter((item) => !item.parentItemId)
    .sort((left, right) => left.position - right.position)
  const entries: TranscriptEntry[] = []
  for (const item of items) {
    const message = messageFromItem(item)
    if (message) {
      entries.push({ kind: 'message', key: `item-${item.id}`, runId: item.runId, position: item.position, message })
      continue
    }
    const checkpoint = checkpointFromItem(state, item)
    if (checkpoint) {
      entries.push({ kind: 'checkpoint', key: `item-${item.id}`, runId: item.runId, position: item.position, checkpoint })
      continue
    }
    const activity = activityFromItem(item)
    if (activity) {
      entries.push({ kind: 'activity', key: `item-${item.id}`, runId: item.runId, position: item.position, activity })
    }
  }
  return entries
}
