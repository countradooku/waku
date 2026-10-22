// End to end against a real `waku-daemon` driving the fake Codex CLI
// (`crates/waku-bench-daemon`, `fake-codex`): the web's data layer —
// DaemonConnection, the shell and thread stores, the dispatch actions and
// the view model — with no browser.
//
// Skipped unless WAKU_E2E_DAEMON names a debug or release `waku-daemon` and
// WAKU_E2E_FAKE_CODEX names the matching `fake-codex`:
//
//   cargo build -p waku-daemon -p waku-bench-daemon --bin waku-daemon --bin fake-codex
//   WAKU_E2E_DAEMON=target/debug/waku-daemon WAKU_E2E_FAKE_CODEX=target/debug/fake-codex \
//     bun test src/e2e

import { afterAll, describe, expect, test } from 'bun:test'
import { spawn, spawnSync, type ChildProcess } from 'node:child_process'
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { DaemonConnection, ShellStore } from '@waku/client/v8'
import { createTaskActions } from '@/lib/task-actions'
import { ThreadStores } from '@/lib/thread-stores'
import { shellView, taskFromThread } from '@/lib/thread-model'
import type { DaemonStores } from '@/lib/daemon-context'

const daemonBinary = process.env.WAKU_E2E_DAEMON
const fakeCodex = process.env.WAKU_E2E_FAKE_CODEX
const enabled = Boolean(daemonBinary && fakeCodex)

const cleanup: Array<() => void> = []
afterAll(() => {
  for (const step of cleanup.reverse()) {
    try {
      step()
    } catch {
      // Best effort.
    }
  }
})

function until(predicate: () => boolean, timeoutMs = 30_000, label = 'condition'): Promise<void> {
  return new Promise((resolvePromise, reject) => {
    const started = Date.now()
    const check = () => {
      if (predicate()) resolvePromise()
      else if (Date.now() - started > timeoutMs) reject(new Error(`timed out waiting for ${label}`))
      else setTimeout(check, 20)
    }
    check()
  })
}

async function startDaemon(home: string): Promise<{ address: string; process: ChildProcess }> {
  const child = spawn(resolve(daemonBinary!), [], {
    env: { ...process.env, HOME: home, WAKU_DAEMON_TOKEN: 'e2e-token' },
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  cleanup.push(() => child.kill('SIGTERM'))
  const address = await new Promise<string>((resolveAddress, reject) => {
    let buffer = ''
    child.stdout!.on('data', (chunk: Buffer) => {
      buffer += chunk.toString()
      for (const line of buffer.split('\n')) {
        try {
          const ready = JSON.parse(line) as { address?: string }
          if (ready.address) resolveAddress(ready.address)
        } catch {
          // Not the ready line yet.
        }
      }
    })
    child.on('exit', (code) => reject(new Error(`waku-daemon exited with ${code}`)))
    setTimeout(() => reject(new Error('waku-daemon did not report its address')), 60_000)
  })
  return { address: address.startsWith('ws') ? address : `ws://${address}`, process: child }
}

describe.skipIf(!enabled)('web data layer against a real daemon', () => {
  test('creates a project and a task, streams the reply, survives a reconnect and sends a follow-up', async () => {
    const scratch = mkdtempSync(join(tmpdir(), 'waku-web-e2e-'))
    cleanup.push(() => rmSync(scratch, { recursive: true, force: true }))
    const home = join(scratch, 'home')
    const repo = join(scratch, 'repo')
    const fakes = join(scratch, 'fakes')
    mkdirSync(home)
    mkdirSync(repo)
    mkdirSync(fakes)
    symlinkSync(resolve(fakeCodex!), join(fakes, 'codex'))
    writeFileSync(join(fakes, 'waku-bench.json'), JSON.stringify({ timeline: 'prose', rate: 'max', loops: 1 }))
    writeFileSync(join(repo, 'README.md'), '# e2e\n')
    spawnSync('git', ['init', '-q'], { cwd: repo })
    spawnSync('git', ['-c', 'user.email=e2e@waku', '-c', 'user.name=e2e', 'add', '.'], { cwd: repo })
    spawnSync('git', ['-c', 'user.email=e2e@waku', '-c', 'user.name=e2e', 'commit', '-qm', 'init'], { cwd: repo })

    const { address } = await startDaemon(home)
    const connection = new DaemonConnection({
      url: address,
      credential: { type: 'local', token: 'e2e-token' },
      client: { kind: 'web', appVersion: 'e2e', os: 'test', instanceId: crypto.randomUUID() },
      initialDelayMs: 20,
    })
    cleanup.push(() => connection.close())
    const welcome = await connection.start()
    expect(welcome.protocol.major).toBe(8)

    await connection.call('settings.update', { patch: { providerBinaryOverrides: { codex: join(fakes, 'codex') } } })

    const shell = new ShellStore(connection).start()
    const threads = new ThreadStores(connection, 0)
    const stores = { shell, threads } as unknown as DaemonStores
    cleanup.push(() => threads.close())
    cleanup.push(() => shell.close())
    await until(() => shell.status === 'ready', 10_000, 'the shell snapshot')

    const actions = createTaskActions(connection, stores)
    const project = await actions.addProject(repo)
    expect(shellView(shell.state).projects.map((candidate) => candidate.path)).toContain(repo)

    const threadId = await actions.startTask(
      { projectId: project.id, selection: { instance: 'codex' }, runtimeMode: 'full_access', interactionMode: 'default' },
      { text: 'Say hello', attachments: [] },
    )
    // Read-your-writes: the shell already holds the new thread.
    expect(shell.state.threads.has(threadId)).toBe(true)

    const store = threads.acquire(threadId)
    const task = () => taskFromThread(store.state, shell.state.threads.get(threadId))
    const answered = (runs: number) => {
      const current = task()
      return Boolean(current
        && current.runs.filter((run) => run.status === 'completed').length >= runs
        && current.entries.filter((entry) => entry.kind === 'message' && entry.message.role === 'assistant' && entry.message.text.trim()).length >= runs)
    }
    await until(() => answered(1), 60_000, 'the first reply')
    expect(task()!.entries[0]).toMatchObject({ kind: 'message', message: { role: 'user', text: 'Say hello' } })

    // Drop the socket: the connection comes back by itself and the thread
    // stream resumes from its cursor.
    const before = store.state.cursor
    connection.client!.close()
    await until(() => connection.state.phase === 'reconnecting' || connection.state.phase === 'connected', 5_000, 'the drop')
    await until(() => connection.state.phase === 'connected' && store.state.synchronized, 15_000, 'the resume')
    expect(store.state.cursor?.type).toBe('sequence')
    if (before?.type === 'sequence' && store.state.cursor?.type === 'sequence') {
      expect(store.state.cursor.seq).toBeGreaterThanOrEqual(before.seq)
    }

    await actions.sendMessage(task()!, { text: 'And again', attachments: [] })
    await until(() => answered(2), 60_000, 'the second reply')
    expect(task()!.runs.map((run) => run.ordinal)).toEqual([1, 2])

    await actions.renameTask(threadId, 'E2E task')
    await until(() => shell.state.threads.get(threadId)?.thread.title === 'E2E task', 5_000, 'the rename')
    await actions.archiveTask(threadId)
    await until(() => !shell.state.threads.has(threadId), 5_000, 'the archive')
  }, 180_000)
})
