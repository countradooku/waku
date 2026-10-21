import { describe, expect, test } from 'bun:test'
import type { DaemonConnection } from '@waku/client/v8'
import {
  browseDaemonDirectory,
  collectWorkspaceDiff,
  normalizeProjectPath,
  probeProvider,
  projectNameFromPath,
  readWorkspaceTextFile,
  rootKey,
  selectableProjects,
  taskCwd,
  threadRoot,
  writeWorkspaceTextFile,
} from './daemon-api'
import { fakeConnection, project } from './test-fixtures'

function asConnection(connection: ReturnType<typeof fakeConnection>) {
  return connection as unknown as DaemonConnection
}

describe('catalog calls', () => {
  test('browses an absolute directory, or the daemon home without a path', async () => {
    const directory = { path: '/srv', home: '/home/me', filesystemRoot: '/', entries: [] }
    const connection = fakeConnection({ 'fs.browse': () => directory })
    expect(await browseDaemonDirectory(asConnection(connection), '/srv')).toEqual(directory)
    await browseDaemonDirectory(asConnection(connection), null)
    expect(connection.calls.map((call) => call.params)).toEqual([{ path: '/srv' }, {}])
  })

  test('detects a provider without model or version discovery', async () => {
    const connection = fakeConnection({
      'providers.probe': () => ({ probe: { instanceId: 'codex', installed: true, path: '/bin/codex' } }),
    })
    const catalog = await probeProvider(asConnection(connection), 'codex', { discoverModels: false, probeVersion: false })
    expect(connection.calls[0]).toEqual({
      method: 'providers.probe',
      params: { instanceId: 'codex', discoverModels: false, probeVersion: false },
    })
    expect(catalog).toEqual({ installed: true, path: '/bin/codex', version: undefined, models: [], agentPresets: [] })
  })

  test('reads text inline, or from the blob store when it is too large', async () => {
    let inline = true
    const connection = fakeConnection({
      'files.read': () => inline
        ? { binary: false, size: 2, revision: 'sha256:a', text: 'hi' }
        : { binary: false, size: 3, revision: 'sha256:b', blob: 'waku-blob:big.txt' },
      blobDownload: () => ({ meta: { type: 'meta', mime: 'text/plain', sha256: '', size: 3 }, bytes: new TextEncoder().encode('big') }),
    })
    const root = threadRoot('t1')
    expect(await readWorkspaceTextFile(asConnection(connection), root, 'a.txt')).toEqual({ text: 'hi', revision: 'sha256:a' })
    inline = false
    expect(await readWorkspaceTextFile(asConnection(connection), root, 'b.txt')).toEqual({ text: 'big', revision: 'sha256:b' })
    expect(connection.downloads).toEqual([{ reference: 'waku-blob:big.txt', path: undefined }])
  })

  test('writes against the revision it read', async () => {
    const connection = fakeConnection({ 'files.write': () => ({ revision: 'sha256:new' }) })
    const revision = await writeWorkspaceTextFile(asConnection(connection), threadRoot('t1'), 'a.txt', 'x', 'sha256:old')
    expect(revision).toBe('sha256:new')
    expect(connection.calls[0]!.params).toEqual({
      root: { type: 'thread', threadId: 't1' },
      path: 'a.txt',
      content: 'x',
      baseRevision: 'sha256:old',
    })
  })

  test('downloads a diff patch that came back as a blob', async () => {
    const connection = fakeConnection({
      'review.diff': () => ({
        source: { type: 'branch' },
        numstat: '1\t0\ta.txt',
        files: [{ path: 'a.txt', binary: false, additions: 1, deletions: 0 }],
        patch: { type: 'blob', reference: 'waku-blob:patch.diff', size: 5 },
        completeContext: false,
      }),
      blobDownload: () => ({ meta: { type: 'meta', mime: 'text/x-diff', sha256: '', size: 5 }, bytes: new TextEncoder().encode('patch') }),
    })
    const diff = await collectWorkspaceDiff(asConnection(connection), threadRoot('t1'), 'branch', true)
    expect(connection.calls[0]!.params).toEqual({ root: threadRoot('t1'), source: { type: 'branch' }, ignoreWhitespace: true })
    expect(diff.patch).toBe('patch')
    expect(diff.files).toHaveLength(1)
  })
})

describe('project paths and roots', () => {
  test('normalizes a remote absolute path without collapsing the root', () => {
    expect(normalizeProjectPath(' /repos/waku/ ')).toBe('/repos/waku')
    expect(normalizeProjectPath('/')).toBe('/')
    expect(projectNameFromPath('/repos/waku/')).toBe('waku')
  })

  test('rejects paths that depend on the browser process cwd', () => {
    expect(() => normalizeProjectPath('repos/waku')).toThrow('absolute path')
  })

  test('keys roots and resolves a task workspace', () => {
    expect(rootKey(threadRoot('t'))).toBe('thread:t')
    expect(rootKey({ type: 'project', projectId: 'p' })).toBe('project:p')
    const waku = project('p', 'waku', '/repos/waku')
    expect(taskCwd({ worktreePath: '/wt/one' }, waku)).toBe('/wt/one')
    expect(taskCwd({}, waku)).toBe('/repos/waku')
  })

  test('represents projectless tasks as one choice while preserving the selected workspace', () => {
    const ordinary = project('repo', 'waku', '/srv/waku')
    const first = project('one', 'No project', '/home/me/.waku/projects/one')
    const selected = project('two', 'No project', '/home/me/.waku/projects/two')
    expect(selectableProjects([ordinary, first, selected], selected)).toEqual([selected, ordinary])
  })
})
