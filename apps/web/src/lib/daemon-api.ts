// Catalog reads and writes over protocol v8 (RFC 0003 §5.2): files, vcs,
// review, providers, usage, skills, settings, storage and search. Thread and
// project changes go through `orchestration.dispatch` (task-actions.ts).

import type { DaemonConnection } from '@waku/client/v8'
import type * as T from '@waku/client/v8'

export type DiffSource = 'uncommitted' | 'unstaged' | 'staged' | 'committed' | 'branch'

export const daemonKeys = {
  info: (address: string) => ['daemon', address, 'info'] as const,
  providers: (address: string) => ['daemon', address, 'providers'] as const,
  provider: (address: string, instanceId: string, discoverModels = true) =>
    [...daemonKeys.providers(address), 'probe', instanceId, discoverModels] as const,
  providerModels: (address: string, instanceId: string) =>
    [...daemonKeys.providers(address), 'models', instanceId] as const,
  planUsage: (address: string, instanceId: string) =>
    ['daemon', address, 'plan-usage', instanceId] as const,
  skills: (address: string) => ['daemon', address, 'skills'] as const,
  usage: (address: string, window: T.UsageWindow) =>
    ['daemon', address, 'usage', JSON.stringify(window)] as const,
  workspace: (address: string, root: string) =>
    ['daemon', address, 'workspace', root] as const,
  composerSources: (address: string) => ['daemon', address, 'composer-sources'] as const,
  composerFiles: (address: string, root: string) =>
    [...daemonKeys.composerSources(address), 'files', root] as const,
  slashCommands: (address: string, instanceId: string, root: string) =>
    [...daemonKeys.composerSources(address), 'commands', instanceId, root] as const,
  workspaceTree: (address: string, root: string, expanded: string[]) =>
    ['daemon', address, 'workspace-tree', root, ...[...expanded].sort()] as const,
  directory: (address: string, path: string | null) =>
    ['daemon', address, 'directory', path] as const,
  workspaceFile: (address: string, root: string, path: string) =>
    ['daemon', address, 'workspace-file', root, path] as const,
  workspaceDiff: (address: string, root: string, source: DiffSource = 'uncommitted') =>
    ['daemon', address, 'workspace-diff', root, source] as const,
  storage: (address: string) => ['daemon', address, 'storage'] as const,
}

/** A stable cache key for a root. */
export function rootKey(root: T.RootRef): string {
  switch (root.type) {
    case 'project':
      return `project:${root.projectId}`
    case 'thread':
      return `thread:${root.threadId}`
    case 'path':
      return `path:${root.path}`
    default:
      return 'unknown'
  }
}

export function threadRoot(threadId: string): T.RootRef {
  return { type: 'thread', threadId }
}

export function projectRoot(projectId: string): T.RootRef {
  return { type: 'project', projectId }
}

export function loadDaemonInfo(connection: DaemonConnection): Promise<T.DaemonInfo> {
  return connection.call('daemon.info', {})
}

export async function searchThreads(
  connection: DaemonConnection,
  query: string,
  limit = 40,
): Promise<T.ThreadSearchHit[]> {
  return connection.call('threads.search', { query, limit, includeArchived: false })
}

export function listProviderSessions(
  connection: DaemonConnection,
  instanceId: string,
  limit = 250,
): Promise<T.ProviderSessionSummary[]> {
  return connection.call('providerSessions.list', { instanceId, limit })
}

export interface ProviderCatalog {
  installed: boolean
  path?: string
  version?: string
  models: T.ProviderModel[]
  agentPresets: T.AgentPreset[]
}

export async function probeProvider(
  connection: DaemonConnection,
  instanceId: string,
  options: { discoverModels?: boolean; probeVersion?: boolean } = {},
): Promise<ProviderCatalog> {
  const { discoverModels = true, probeVersion = true } = options
  const result = await connection.call('providers.probe', { instanceId, discoverModels, probeVersion })
  return {
    installed: result.probe.installed,
    path: result.probe.path,
    version: result.version,
    models: result.probe.models ?? [],
    agentPresets: result.probe.agentPresets ?? [],
  }
}

export function fetchPlanUsage(
  connection: DaemonConnection,
  instanceId: string,
): Promise<T.PlanUsage | null> {
  return connection.call('providers.planUsage', { instanceId })
}

export function updateDaemonSettings(
  connection: DaemonConnection,
  patch: T.SettingsPatch,
): Promise<T.SettingsSnapshot> {
  return connection.call('settings.update', { patch })
}

export function loadSkills(connection: DaemonConnection, projectIds: string[]): Promise<T.SkillsCatalog> {
  return connection.call('skills.list', { projectIds })
}

export async function setSkillsEnabled(
  connection: DaemonConnection,
  dirs: string[],
  enabled: boolean,
): Promise<void> {
  await connection.call('skills.setEnabled', { dirs, enabled })
}

export async function trashSkills(connection: DaemonConnection, dirs: string[]): Promise<void> {
  await connection.call('skills.trash', { dirs })
}

export function loadUsageHistory(
  connection: DaemonConnection,
  window: T.UsageWindow,
  projectIds: string[],
): Promise<T.UsageHistory> {
  return connection.call('usage.history', { window, projectIds })
}

export function worktreeCleanupStatus(connection: DaemonConnection): Promise<T.WorktreeCleanupStatus> {
  return connection.call('storage.worktreeCleanupStatus', {})
}

export function runWorktreeCleanup(connection: DaemonConnection): Promise<T.WorktreeCleanupReport> {
  return connection.call('storage.runWorktreeCleanup', {})
}

export function listWorkspaceTree(
  connection: DaemonConnection,
  root: T.RootRef,
  expanded: string[],
): Promise<T.TreeEntry[]> {
  return connection.call('files.tree', { root, expanded })
}

export function browseDaemonDirectory(
  connection: DaemonConnection,
  path: string | null,
): Promise<T.Directory> {
  return connection.call('fs.browse', path ? { path } : {})
}

export interface TextFile {
  text: string
  revision: string
}

/** A text file, downloaded from the blob store when it is too large to send inline. */
export async function readWorkspaceTextFile(
  connection: DaemonConnection,
  root: T.RootRef,
  path: string,
): Promise<TextFile> {
  const content = await connection.call('files.read', { root, path })
  if (content.binary) throw new Error('This file is binary')
  if (content.text !== undefined) return { text: content.text, revision: content.revision }
  if (content.blob) {
    const { bytes } = await connection.downloadBlob(content.blob)
    return { text: new TextDecoder().decode(bytes), revision: content.revision }
  }
  return { text: '', revision: content.revision }
}

export async function writeWorkspaceTextFile(
  connection: DaemonConnection,
  root: T.RootRef,
  path: string,
  content: string,
  baseRevision?: string,
): Promise<string> {
  const params: Omit<T.FilesWriteParams, 'opId'> = { root, path, content }
  if (baseRevision !== undefined) params.baseRevision = baseRevision
  const result = await connection.call('files.write', params)
  return result.revision
}

export function listComposerFiles(
  connection: DaemonConnection,
  root: T.RootRef,
  limit = 10_000,
): Promise<T.FileEntry[]> {
  return connection.call('files.searchEntries', { root, limit })
}

export function discoverComposerCommands(
  connection: DaemonConnection,
  instanceId: string,
  root: T.RootRef,
  threadId?: string,
): Promise<T.SlashCommand[]> {
  const params: T.ComposerSlashCommandsParams = { instanceId, root }
  if (threadId) params.threadId = threadId
  return connection.call('composer.slashCommands', params)
}

export function inspectWorkspaceBranches(
  connection: DaemonConnection,
  root: T.RootRef,
): Promise<T.BranchSnapshot | null> {
  return connection.call('vcs.refs', { root })
}

export function checkoutWorkspaceBranch(
  connection: DaemonConnection,
  root: T.RootRef,
  name: string,
  create = false,
): Promise<T.BranchSnapshot> {
  return connection.call('vcs.switchRef', { root, name, create })
}

export function inspectGitStatus(connection: DaemonConnection, root: T.RootRef): Promise<T.GitStatus> {
  return connection.call('vcs.status', { root })
}

export function stageGitFiles(
  connection: DaemonConnection,
  root: T.RootRef,
  paths: string[],
  stage: boolean,
): Promise<T.GitStatus> {
  return connection.call('vcs.stage', { root, paths, stage })
}

export function pullGitBranch(connection: DaemonConnection, root: T.RootRef): Promise<T.PullResult> {
  return connection.call('vcs.pull', { root })
}

export function listGitWorktrees(connection: DaemonConnection, root: T.RootRef): Promise<T.GitWorktree[]> {
  return connection.call('vcs.worktrees', { root })
}

export interface WorkspaceDiff {
  patch: string
  numstat: string
  files: T.FileDiffSummary[]
  completeContext: boolean
}

/** `review.diff`, with a patch over the inline limit downloaded from the blob store. */
export async function collectWorkspaceDiff(
  connection: DaemonConnection,
  root: T.RootRef,
  source: DiffSource = 'uncommitted',
  ignoreWhitespace = false,
): Promise<WorkspaceDiff> {
  const diff = await connection.call('review.diff', { root, source: { type: source }, ignoreWhitespace })
  let patch = ''
  if (diff.patch.type === 'inline') patch = diff.patch.value
  else if (diff.patch.type === 'blob') {
    const { bytes } = await connection.downloadBlob(diff.patch.reference)
    patch = new TextDecoder().decode(bytes)
  }
  return { patch, numstat: diff.numstat, files: diff.files, completeContext: diff.completeContext }
}

export function inspectWorkspaceCommit(
  connection: DaemonConnection,
  root: T.RootRef,
): Promise<T.CommitSnapshot> {
  return connection.call('vcs.commitPreview', { root })
}

export async function generateWorkspaceCommitMessage(
  connection: DaemonConnection,
  root: T.RootRef,
  includeUnstaged: boolean,
  selection: T.ModelSelection,
): Promise<string> {
  const result = await connection.call('vcs.generateCommitMessage', { root, includeUnstaged, selection })
  return result.message
}

export function commitWorkspace(
  connection: DaemonConnection,
  root: T.RootRef,
  message: string,
  includeUnstaged: boolean,
  push: boolean,
): Promise<T.CommitResult> {
  return connection.call('vcs.commit', { root, message, includeUnstaged, push })
}

export async function pushWorkspace(connection: DaemonConnection, root: T.RootRef): Promise<void> {
  await connection.call('vcs.push', { root })
}

export function selectableProjects(projects: T.Project[], selected?: T.Project): T.Project[] {
  const choices = selected
    ? [selected, ...projects.filter((project) => project.id !== selected.id)]
    : projects
  let hasProjectless = false
  return choices.filter((project, index) => {
    if (choices.findIndex((candidate) => candidate.id === project.id) !== index) return false
    if (project.name !== 'No project') return true
    if (hasProjectless) return false
    hasProjectless = true
    return true
  })
}

/** Where the task's files live: its worktree, else its project. */
export function taskCwd(
  task: { worktreePath?: string } | null | undefined,
  project: T.Project | null | undefined,
): string | undefined {
  return task?.worktreePath ?? project?.path
}

/** The project name an absolute path would get. */
export function projectNameFromPath(path: string): string {
  const input = path.trim()
  const normalized = input === '/' ? input : input.replace(/[\\/]+$/, '')
  return normalized.split(/[\\/]/).filter(Boolean).at(-1) ?? 'Project'
}

export function normalizeProjectPath(path: string): string {
  const input = path.trim()
  if (!input.startsWith('/') && !/^[a-z]:[\\/]/i.test(input)) {
    throw new Error('Enter an absolute path on the daemon host')
  }
  return input === '/' ? input : input.replace(/[\\/]+$/, '')
}
