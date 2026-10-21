import { useQuery, useQueryClient } from '@tanstack/react-query'
import type * as T from '@waku/client/v8'
import { useEffect, useRef, useState, type RefObject } from 'react'
import { Virtuoso } from 'react-virtuoso'
import { toast } from 'sonner'
import { Dialog, DialogContent, DialogTitle } from '@/components/ui/dialog'
import { WakuIcon, type WakuIconName } from '@/components/waku-icon'
import {
  commitWorkspace,
  generateWorkspaceCommitMessage,
  inspectGitStatus,
  inspectWorkspaceCommit,
  listGitWorktrees,
  pullGitBranch,
  pushWorkspace,
  rootKey,
  stageGitFiles,
} from '@/lib/daemon-api'
import { requireConnection, useDaemon } from '@/lib/daemon-context'
import { useI18n } from '@/lib/i18n'
import { usePrimaryShortcut } from '@/lib/platform'
import { cn } from '@/lib/utils'

type CommitAction = 'commit' | 'commitAndPush' | 'push' | 'pull' | 'stage'

export function CommitDialog({
  open,
  root,
  selection,
  returnFocus,
  onOpenChange,
}: {
  open: boolean
  root?: T.RootRef
  /** The task's provider and model, which write a missing commit message. */
  selection?: T.ModelSelection
  returnFocus?: RefObject<HTMLElement | null>
  onOpenChange: (open: boolean) => void
}) {
  const { t } = useI18n()
  const { connection, config, phase } = useDaemon()
  const queryClient = useQueryClient()
  const [message, setMessage] = useState('')
  const [includeUnstaged, setIncludeUnstaged] = useState(true)
  const [pending, setPending] = useState<CommitAction | null>(null)
  const [error, setError] = useState<string | null>(null)
  const operationPending = useRef(false)
  const mounted = useRef(true)
  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false }
  }, [])
  const commitShortcut = usePrimaryShortcut('⌘↩', 'Ctrl+Enter')
  const key = root ? rootKey(root) : 'none'

  const snapshot = useQuery({
    queryKey: ['daemon', config?.address ?? 'disconnected', 'commit', key],
    queryFn: () => inspectWorkspaceCommit(requireConnection(connection), root!),
    enabled: open && phase === 'connected' && Boolean(connection && config && root),
  })
  const gitStatus = useQuery({
    queryKey: ['daemon', config?.address, 'git-status', key],
    queryFn: () => inspectGitStatus(requireConnection(connection), root!),
    enabled: open && phase === 'connected' && Boolean(connection && root),
  })
  const worktrees = useQuery({
    queryKey: ['daemon', config?.address, 'git-worktrees', key],
    queryFn: () => listGitWorktrees(requireConnection(connection), root!),
    enabled: open && phase === 'connected' && Boolean(connection && root),
  })

  useEffect(() => {
    if (!open) return
    setMessage('')
    setIncludeUnstaged(true)
    setError(null)
  }, [open, key])

  const additions = includeUnstaged
    ? snapshot.data?.additions ?? 0
    : snapshot.data?.stagedAdditions ?? 0
  const deletions = includeUnstaged
    ? snapshot.data?.deletions ?? 0
    : snapshot.data?.stagedDeletions ?? 0
  const available = phase === 'connected' && !pending && !snapshot.isError && !gitStatus.isError
  const canCommit = available && !snapshot.isFetching && Boolean(
    snapshot.data?.hasStaged || (includeUnstaged && snapshot.data?.hasUnstaged),
  )
  const canPush = available && !snapshot.isFetching && Boolean(snapshot.data?.canPush)
  const canPull = available && !gitStatus.isFetching && Boolean(gitStatus.data?.upstream && gitStatus.data.branch && gitStatus.data.files.length === 0)

  async function stageFile(path: string, stage: boolean) {
    if (!connection || !root || operationPending.current) return
    operationPending.current = true
    setPending('stage')
    setError(null)
    // Explicit file selection must never be overridden by "include all".
    setIncludeUnstaged(false)
    try {
      await stageGitFiles(connection, root, [path], stage)
    } catch (cause) {
      setError(errorMessage(cause))
    } finally {
      await Promise.all([
        snapshot.refetch(),
        gitStatus.refetch(),
        queryClient.invalidateQueries({ queryKey: ['daemon', config?.address, 'workspace-diff'] }),
      ])
      operationPending.current = false
      setPending(null)
    }
  }

  async function run(action: CommitAction) {
    if (!connection || !config || !root || operationPending.current || action === 'stage') return
    operationPending.current = true
    setPending(action)
    setError(null)
    try {
      if (action === 'push') {
        await pushWorkspace(connection, root)
      } else if (action === 'pull') {
        await pullGitBranch(connection, root)
      } else {
        let commitMessage = message.trim()
        if (!commitMessage) {
          if (!selection) throw new Error(t('commit.no_task'))
          commitMessage = await generateWorkspaceCommitMessage(
            connection,
            root,
            includeUnstaged,
            selection,
          )
          setMessage(commitMessage)
        }
        await commitWorkspace(connection, root, commitMessage, includeUnstaged, action === 'commitAndPush')
      }
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['daemon', config.address, 'workspace'] }),
        queryClient.invalidateQueries({ queryKey: ['daemon', config.address, 'workspace-tree'] }),
        queryClient.invalidateQueries({ queryKey: ['daemon', config.address, 'workspace-diff'] }),
        queryClient.invalidateQueries({ queryKey: ['daemon', config.address, 'git-status'] }),
        queryClient.invalidateQueries({ queryKey: ['daemon', config.address, 'commit'] }),
      ])
      if (!mounted.current) return
      toast.success(t(action === 'commit'
        ? 'commit.committed'
        : action === 'commitAndPush'
          ? 'commit.committed_and_pushed'
          : action === 'pull' ? 'commit.pulled' : 'commit.pushed'))
      onOpenChange(false)
    } catch (cause) {
      setError(errorMessage(cause))
      void snapshot.refetch()
      void gitStatus.refetch()
    } finally {
      operationPending.current = false
      setPending(null)
    }
  }

  return (
    <Dialog open={open} onOpenChange={(value) => { if (!operationPending.current) onOpenChange(value) }}>
      <DialogContent className="max-w-[420px] overflow-hidden rounded-[18px] bg-[var(--raised)] p-0" finalFocus={returnFocus}>
        <DialogTitle className="sr-only">{t('environment.commit_or_push')}</DialogTitle>
        <div className="flex h-12 items-center gap-2.5 px-4 text-[14px]">
          <WakuIcon className="size-[15px]" name="gitBranch" />
          <span className="min-w-0 flex-1 truncate">{snapshot.data?.branch ?? 'HEAD'}</span>
        </div>
        {gitStatus.data?.upstream && <p className="px-4 text-xs text-muted-foreground">{gitStatus.data.upstream} · ↑{gitStatus.data.ahead} ↓{gitStatus.data.behind}</p>}
        {worktrees.data && worktrees.data.length > 1 && <details className="px-4 py-1 text-xs">
          <summary>{t('commit.worktrees')} ({worktrees.data.length})</summary>
          <Virtuoso
            style={{ height: 96 }}
            data={worktrees.data}
            computeItemKey={(_, worktree) => worktree.path}
            itemContent={(_, worktree) => <div className="py-1">
              <div className="flex items-center gap-2">
                <span className="min-w-0 flex-1 truncate">{worktree.branch ?? worktree.head.slice(0, 12)}</span>
                {worktree.locked && <span className="shrink-0 whitespace-nowrap text-muted-foreground">{t('commit.locked')}</span>}
              </div>
              <p className="break-all text-[10px] text-muted-foreground">{worktree.path}</p>
            </div>}
          />
        </details>}
        <textarea
          autoFocus
          aria-label={t('commit.message')}
          className="h-28 w-full resize-none border-0 bg-transparent px-4 py-2.5 text-[14px] leading-[21px] outline-none placeholder:text-[var(--text-ghost)]"
          disabled={Boolean(pending)}
          placeholder={t('commit.message_placeholder')}
          value={message}
          onChange={(event) => setMessage(event.target.value)}
          onKeyDown={(event) => {
            if ((event.metaKey || event.ctrlKey) && event.key === 'Enter' && canCommit) {
              event.preventDefault()
              void run('commit')
            }
          }}
        />
        <div className="px-2">
          <button
            aria-checked={includeUnstaged}
            className="flex h-11 w-full items-center gap-2.5 rounded-[9px] px-3 text-[14px] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring disabled:opacity-45"
            disabled={Boolean(pending)}
            role="checkbox"
            type="button"
            onClick={() => setIncludeUnstaged((current) => !current)}
          >
            <span className={cn(
              'grid size-[15px] place-items-center rounded border',
              includeUnstaged ? 'border-input bg-background' : 'border-[var(--text-ghost)]',
            )}>
              {includeUnstaged && <WakuIcon className="size-3" name="check" />}
            </span>
            <span className="min-w-0 flex-1 text-left">{t('commit.include_unstaged')}</span>
            <span className="flex items-center gap-1.5 text-[13.5px] font-medium">
              <span className="text-[var(--success)]">+{additions.toLocaleString()}</span>
              <span className="text-destructive">-{deletions.toLocaleString()}</span>
            </span>
          </button>
        </div>
        {gitStatus.data && gitStatus.data.files.length > 0 && <Virtuoso
          style={{ height: Math.min(gitStatus.data.files.length, 5) * 36 }}
          data={gitStatus.data.files}
          computeItemKey={(_, file) => file.path}
          itemContent={(_, file) => {
            const staged = isGitFileStaged(file.indexStatus)
            return <label className="flex h-9 items-center gap-2 px-4 text-xs" title={file.previousPath ? `${file.previousPath} → ${file.path}` : file.path}>
              <input type="checkbox" aria-label={`${staged ? t('commit.unstage') : t('commit.stage')} ${file.path}`} checked={staged} disabled={!available || gitStatus.isFetching} onChange={() => void stageFile(file.path, !staged)} />
              <span className="min-w-0 flex-1 truncate">{file.path}</span><code>{file.indexStatus}{file.worktreeStatus}</code>
            </label>
          }}
        />}
        {(error || snapshot.error || gitStatus.error || worktrees.error) && (
          <p className="px-5 pb-2 text-[11.5px] leading-4 text-destructive">
            {error ?? errorMessage(snapshot.error ?? gitStatus.error ?? worktrees.error)}
          </p>
        )}
        <div className="mx-2 mt-1 border-t" />
        <div className="flex flex-col gap-0.5 p-2">
          <CommitActionRow
            enabled={canCommit}
            icon="gitCommitHorizontal"
            label={pending === 'commit'
              ? t(message.trim() ? 'commit.committing' : 'commit.generating_message')
              : t('commit.commit')}
            pending={pending === 'commit'}
            shortcut={commitShortcut}
            onClick={() => void run('commit')}
          />
          <CommitActionRow
            enabled={canCommit}
            icon="cloudUpload"
            label={pending === 'commitAndPush'
              ? t(message.trim() ? 'commit.committing_and_pushing' : 'commit.generating_message')
              : t('commit.commit_and_push')}
            pending={pending === 'commitAndPush'}
            onClick={() => void run('commitAndPush')}
          />
          <CommitActionRow
            enabled={canPush}
            icon="cloudUpload"
            label={t(pending === 'push' ? 'commit.pushing' : 'commit.push')}
            pending={pending === 'push'}
            onClick={() => void run('push')}
          />
          <CommitActionRow enabled={canPull} icon="gitBranch" label={t(pending === 'pull' ? 'commit.pulling' : 'commit.pull')} pending={pending === 'pull'} onClick={() => void run('pull')} />
        </div>
      </DialogContent>
    </Dialog>
  )
}

function CommitActionRow({
  icon,
  label,
  shortcut,
  enabled,
  pending,
  onClick,
}: {
  icon: WakuIconName
  label: string
  shortcut?: string
  enabled: boolean
  pending: boolean
  onClick: () => void
}) {
  return (
    <button
      className="flex min-h-9 w-full items-center gap-2.5 rounded-lg px-3 text-[13.5px] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring disabled:opacity-40"
      disabled={!enabled || pending}
      type="button"
      onClick={onClick}
    >
      <WakuIcon className={cn('size-3.5 text-[var(--text-secondary)]', pending && 'motion-safe:animate-spin')} name={pending ? 'loaderCircle' : icon} />
      <span className="min-w-0 flex-1 truncate text-left">{label}</span>
      {shortcut && <span className="text-[11px] text-[var(--text-ghost)]">{shortcut}</span>}
    </button>
  )
}

/** A path is staged when git's index column records a change. */
function isGitFileStaged(indexStatus: string): boolean {
  return indexStatus !== '' && indexStatus !== ' ' && indexStatus !== '.' && indexStatus !== '?'
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}
