import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Popover } from '@base-ui/react/popover'
import type * as T from '@waku/client/v8'
import {
  useEffect,
  useRef,
  useState,
  type KeyboardEvent,
  type MouseEvent as ReactMouseEvent,
  type RefObject,
} from 'react'
import { Virtuoso, type VirtuosoHandle } from 'react-virtuoso'
import { toast } from 'sonner'
import { ControlMenu, type ControlMenuItem } from '@/components/control-menu'
import { GoalDialog, goalStatusClass, goalUsageReadout } from '@/components/goal-dialog'
import { DaemonFilePicker } from '@/components/daemon-file-picker'
import { PreviewableImage } from '@/components/image-preview'
import { ModelPicker } from '@/components/model-picker'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { FileTypeIcon, WakuIcon } from '@/components/waku-icon'
import {
  useComposerCommands,
  useComposerFiles,
  useProviderProbe,
  useWorkspaceBranches,
} from '@/hooks/use-daemon-data'
import { useProviderStatuses } from '@/hooks/use-daemon-stores'
import {
  draftAttachment,
  importDaemonPathAttachment,
  importFiles,
  readAttachmentImage,
} from '@/lib/attachments'
import type { MessageInput } from '@/lib/commands'
import type { ComposerDraftValue } from '@/lib/composer-drafts'
import {
  checkoutWorkspaceBranch,
  daemonKeys,
  fetchPlanUsage,
  projectRoot,
  rootKey,
  selectableProjects,
  taskCwd,
  threadRoot,
  type ProviderCatalog,
} from '@/lib/daemon-api'
import { useDaemon } from '@/lib/daemon-context'
import { useI18n } from '@/lib/i18n'
import {
  composerAutocompleteRows,
  detectComposerTrigger,
  expandedComposerSubmission,
  isFastModeToggleSubmission,
  isResumeSubmission,
  parseGoalSubmission,
  replaceComposerTrigger,
  sortComposerCommands,
  toggledFastServiceTier,
  type ComposerAutocompleteRow,
} from '@/lib/composer-autocomplete'
import {
  browserComposerPreferenceStorage,
  readComposerPreferences,
  rememberComposerSelection,
  rememberedModelTraits,
  writeComposerPreferences,
} from '@/lib/composer-preferences'
import {
  isEscapeStopArmed,
  pressEscapeStop,
  sameEscapeStopArm,
  type EscapeStopArm,
} from '@/lib/escape-stop'
import { usePrimaryShortcut } from '@/lib/platform'
import { agentPresetDescription, agentPresetLabel } from '@/lib/agent-preset-presentation'
import { isProjectlessProject, projectDisplayName } from '@/lib/project-presentation'
import { useTaskActions } from '@/lib/task-actions'
import {
  isBusy,
  type NewTaskDraft,
  type PendingUserInput,
  type QueuedMessage,
  type Task,
} from '@/lib/thread-model'
import { cn } from '@/lib/utils'

type Translator = (key: string, params?: Record<string, string | number>) => string

function preserveComposerFocusOnMouseDown(event: ReactMouseEvent<HTMLElement>) {
  // Portal events still bubble through the React tree. Only cancel the native
  // focus transfer for controls physically inside this footer, not menu rows.
  if (event.button === 0 && event.currentTarget.contains(event.target as Node)) {
    event.preventDefault()
  }
}

const MODEL_OPTION_KEYS: Record<string, string> = {
  none: 'model_option.none',
  minimal: 'model_option.minimal',
  low: 'model_option.low',
  medium: 'model_option.medium',
  high: 'model_option.high',
  xhigh: 'model_option.extra_high',
  extraHigh: 'model_option.extra_high',
  extra_high: 'model_option.extra_high',
  max: 'model_option.max',
  '200k': 'model_option.context_200k',
  '1m': 'model_option.context_1m',
  ultra: 'model_option.ultra',
  ultracode: 'model_option.ultracode',
  auto: 'model_option.auto',
  fast: 'model_option.fast',
}

function modelOptionLabel(id: string, fallback: string, t: Translator) {
  const key = MODEL_OPTION_KEYS[id]
  return key ? t(key) : fallback
}

export function Composer({
  task,
  newTask,
  project,
  projects,
  onNewTaskChange,
  onActivated,
  focusSignal,
  modelPickerSignal,
  usagePanelSignal,
  prefillSignal,
  prefillText,
  initialComposerDraft,
  onComposerDraftChange,
  onComposerDraftSubmitted,
  onAddProject,
  onProjectless,
  onResume,
  onFocusSignalHandled,
  onModelPickerSignalHandled,
  onUsagePanelSignalHandled,
  onPrefillSignalHandled,
}: {
  /** An existing thread; exactly one of `task` and `newTask` is set. */
  task?: Task
  /** A task being composed; nothing exists on the daemon until it is sent. */
  newTask?: NewTaskDraft
  project: T.Project
  projects: T.Project[]
  onNewTaskChange?: (draft: NewTaskDraft) => void
  onActivated?: (threadId: string) => void
  focusSignal?: number
  modelPickerSignal?: number
  usagePanelSignal?: number
  prefillSignal?: number
  prefillText?: string
  initialComposerDraft?: ComposerDraftValue
  onComposerDraftChange?: (draft: ComposerDraftValue) => void
  onComposerDraftSubmitted?: () => void
  onAddProject?: () => void
  onProjectless?: () => void
  onResume?: () => void
  onFocusSignalHandled?: () => void
  onModelPickerSignalHandled?: () => void
  onUsagePanelSignalHandled?: () => void
  onPrefillSignalHandled?: () => void
}) {
  const { t } = useI18n()
  const { connection, config } = useDaemon()
  const queryClient = useQueryClient()
  const actions = useTaskActions()
  const selection: T.ModelSelection = task?.selection ?? newTask?.selection ?? { instance: 'codex' }
  const runtimeMode: T.RuntimeMode = task?.runtimeMode ?? newTask?.runtimeMode ?? 'full_access'
  const interactionMode: T.InteractionMode = task?.interactionMode ?? newTask?.interactionMode ?? 'default'
  const provider = selection.instance
  const subjectId = task?.id ?? newTask?.id ?? project.id
  const root: T.RootRef = task ? threadRoot(task.id) : projectRoot(project.id)
  const rootId = rootKey(root)
  const cwd = taskCwd(task, project)
  const probe = useProviderProbe(provider)
  const branches = useWorkspaceBranches(root)
  const composerFiles = useComposerFiles(root)
  const composerCommands = useComposerCommands(provider, root, task?.id)
  const [prompt, setPrompt] = useState(initialComposerDraft?.text ?? '')
  const [goalDialog, setGoalDialog] = useState<{ prefill: string | null; replace: boolean } | null>(
    null,
  )
  const [attachments, setAttachments] = useState<T.Attachment[]>(
    () => initialComposerDraft?.attachments ?? [],
  )
  const [submitting, setSubmitting] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [filePickerOpen, setFilePickerOpen] = useState(false)
  const [branchPending, setBranchPending] = useState(false)
  const [inputFocused, setInputFocused] = useState(false)
  const [cursor, setCursor] = useState(() => (initialComposerDraft?.text ?? '').length)
  const [autocompleteSelection, setAutocompleteSelection] = useState({ key: '', index: 0 })
  const [dismissedAutocomplete, setDismissedAutocomplete] = useState<string | null>(null)
  const [escapeStopArm, setEscapeStopArm] = useState<EscapeStopArm | null>(null)
  const composerInput = useRef<HTMLTextAreaElement>(null)
  const autocompleteList = useRef<VirtuosoHandle>(null)
  const pendingCursor = useRef<number | null>(null)
  const escapeStopTimer = useRef<number | null>(null)
  const mounted = useRef(true)
  const draftChange = useRef(onComposerDraftChange)
  draftChange.current = onComposerDraftChange
  const busy = task ? isBusy(task.status) : false
  const escapeStopTarget = `${subjectId}:${task?.activeRun?.id ?? ''}`
  const escapeStopArmed = busy && isEscapeStopArmed(escapeStopArm, escapeStopTarget, Date.now())
  const approval = task?.approval
  const userInput = task?.userInput
  const goal = task?.goal ?? null
  const selectedModel = probe.data?.models.find((model) => model.id === selection.model)
    ?? probe.data?.models.find((model) => model.isDefault)
    ?? probe.data?.models[0]
  const hasDraft = Boolean(prompt.trim() || attachments.length)
  const canSteer = Boolean(busy && task?.status !== 'preparing' && task?.supportsSteer)
  // A thread's project and workspace are fixed once it exists.
  const started = Boolean(task)
  const plannedWorktree = Boolean(newTask?.isolated)
  const projectChoices = selectableProjects(projects, project)
  const projectless = isProjectlessProject(project)
  const projectName = projectDisplayName(project, t('project.no_project_name'))
  const workspaceLocal = newTask ? !newTask.isolated : !task?.worktreePath
  const workspaceLabel = newTask
    ? newTask.isolated ? t('workspace.new_worktree') : t('workspace.local')
    : task?.worktreePath
      ? task.branch ?? task.worktreePath.split('/').filter(Boolean).at(-1) ?? t('workspace.new_worktree')
      : t('workspace.local')
  const availableCommands = sortComposerCommands(composerCommands.data ?? [])
  const autocompleteTrigger = inputFocused ? detectComposerTrigger(prompt, cursor) : null
  const autocompleteKey = autocompleteTrigger
    ? `${autocompleteTrigger.kind}:${autocompleteTrigger.start}:${autocompleteTrigger.end}:${autocompleteTrigger.query}`
    : null
  const autocompleteRows = autocompleteTrigger
    ? composerAutocompleteRows(
        autocompleteTrigger,
        availableCommands,
        composerFiles.data ?? [],
      )
    : []
  const autocompleteLoading = autocompleteTrigger?.kind === 'command'
    ? composerCommands.isPending || composerCommands.isFetching
    : autocompleteTrigger?.kind === 'file'
      ? composerFiles.isPending || composerFiles.isFetching
      : false
  const autocompleteVisible = Boolean(
    autocompleteKey
      && autocompleteKey !== dismissedAutocomplete
      && (autocompleteRows.length || autocompleteLoading),
  )
  const autocompleteOpen = autocompleteVisible && autocompleteRows.length > 0

  function clearEscapeStop() {
    if (escapeStopTimer.current !== null) window.clearTimeout(escapeStopTimer.current)
    escapeStopTimer.current = null
    setEscapeStopArm(null)
  }

  function stopTurn() {
    clearEscapeStop()
    if (!task) return
    void actions.interrupt(task).catch((error) => toast.error(errorMessage(error)))
  }

  useEffect(() => {
    if (!busy) {
      clearEscapeStop()
      return
    }
    setEscapeStopArm((current) => current?.target === escapeStopTarget ? current : null)
  }, [busy, escapeStopTarget])

  useEffect(() => {
    if (!busy) return
    const onEscape = (event: globalThis.KeyboardEvent) => {
      if (
        event.key !== 'Escape'
        || event.repeat
        || event.defaultPrevented
        || event.metaKey
        || event.ctrlKey
        || event.altKey
        || event.shiftKey
      ) return
      if (document.querySelector('[role="dialog"], [role="menu"], [role="listbox"]')) return

      event.preventDefault()
      const press = pressEscapeStop(escapeStopArm, escapeStopTarget, Date.now())
      if (press.type === 'stop') {
        stopTurn()
        return
      }

      if (escapeStopTimer.current !== null) window.clearTimeout(escapeStopTimer.current)
      setEscapeStopArm(press.arm)
      escapeStopTimer.current = window.setTimeout(() => {
        setEscapeStopArm((current) => sameEscapeStopArm(current, press.arm) ? null : current)
        escapeStopTimer.current = null
      }, Math.max(0, press.arm.expiresAt - Date.now()))
    }
    window.addEventListener('keydown', onEscape)
    return () => window.removeEventListener('keydown', onEscape)
  }, [busy, escapeStopArm, escapeStopTarget])

  useEffect(() => () => {
    if (escapeStopTimer.current !== null) window.clearTimeout(escapeStopTimer.current)
  }, [])
  const autocompleteHighlight = autocompleteSelection.key === autocompleteKey
    ? Math.min(autocompleteSelection.index, Math.max(0, autocompleteRows.length - 1))
    : 0

  useEffect(() => {
    if (focusSignal) composerInput.current?.focus()
    if (focusSignal) onFocusSignalHandled?.()
  }, [focusSignal, onFocusSignalHandled])

  useEffect(() => {
    if (!prefillSignal) return
    const nextPrompt = prefillText ?? ''
    pendingCursor.current = nextPrompt.length
    setPrompt(nextPrompt)
    composerInput.current?.focus()
    onPrefillSignalHandled?.()
  }, [onPrefillSignalHandled, prefillSignal, prefillText])

  useEffect(() => {
    setFilePickerOpen(false)
    setDismissedAutocomplete(null)
    setAutocompleteSelection({ key: '', index: 0 })
  }, [rootId, subjectId])

  useEffect(() => {
    const nextCursor = pendingCursor.current
    if (nextCursor === null) return
    pendingCursor.current = null
    composerInput.current?.focus()
    composerInput.current?.setSelectionRange(nextCursor, nextCursor)
    setCursor(nextCursor)
  }, [prompt])

  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
    }
  }, [])

  useEffect(() => {
    draftChange.current?.({ text: prompt, attachments: attachments.map(draftAttachment) })
  }, [attachments, prompt])

  function messageInput(
    submittedPrompt: string,
    submittedAttachments: T.Attachment[],
  ): MessageInput {
    const expanded = expandedComposerSubmission(
      provider,
      submittedPrompt.trim(),
      availableCommands,
    )
    const input: MessageInput = { text: submittedPrompt, attachments: submittedAttachments }
    if (expanded !== null) {
      input.providerText = [
        expanded,
        submittedAttachments.map((attachment) => `@${attachment.mention}`).join(' '),
      ].filter(Boolean).join(' ')
    }
    return input
  }

  function executeLocalComposerCommand(submittedPrompt = prompt): boolean {
    return executeResumeCommand(submittedPrompt)
      || executeFastModeToggle(submittedPrompt)
      || executeGoalCommand(submittedPrompt)
  }

  function clearComposerDraft() {
    setPrompt('')
    setCursor(0)
    setDismissedAutocomplete(null)
    setAutocompleteSelection({ key: '', index: 0 })
  }

  function executeResumeCommand(submittedPrompt: string): boolean {
    if (!onResume || !isResumeSubmission(submittedPrompt)) return false
    clearComposerDraft()
    onResume()
    return true
  }

  function executeFastModeToggle(submittedPrompt: string): boolean {
    if (!isFastModeToggleSubmission(provider, submittedPrompt, availableCommands)) return false
    const nextTier = toggledFastServiceTier(
      selection.serviceTier,
      selectedModel?.serviceTiers ?? [],
    )
    if (!nextTier) return false
    const enabled = nextTier !== 'default'
    clearComposerDraft()
    applySelection({ ...selection, serviceTier: nextTier })
    toast.success(t(enabled ? 'commands.fast_enabled' : 'commands.fast_disabled'))
    return true
  }

  /** Protocol v8 has no goal command yet: goals are shown, not changed. */
  function goalUnsupported() {
    toast.error(t('goal.unsupported'))
  }

  /** Codex's native `/goal` command. Showing the goal works; changing it
   * needs a daemon command v8 does not have yet. */
  function executeGoalCommand(submittedPrompt: string): boolean {
    const command = parseGoalSubmission(provider, submittedPrompt, availableCommands)
    if (!command) return false
    if (command.kind === 'show' || command.kind === 'edit') {
      setGoalDialog({ prefill: null, replace: false })
    } else {
      goalUnsupported()
    }
    clearComposerDraft()
    return true
  }

  function worktreeFrom(): string | undefined {
    if (!newTask?.isolated) return undefined
    const snapshot = branches.data
    return newTask.baseRef ?? snapshot?.current ?? snapshot?.defaultBranch ?? 'HEAD'
  }

  async function submit() {
    if (submitting || (!prompt.trim() && attachments.length === 0)) return
    if (executeLocalComposerCommand()) return
    const submittedPrompt = prompt
    const submittedAttachments = attachments
    const input = messageInput(submittedPrompt, submittedAttachments)
    let cleared = false
    setSubmitting(true)
    try {
      const pending = newTask
        ? actions.startTask({
            projectId: newTask.projectId,
            selection,
            runtimeMode,
            interactionMode,
            worktreeFrom: worktreeFrom(),
          }, input)
        : task
          ? actions.sendMessage(task, input, 'send').then(() => undefined)
          : Promise.resolve(undefined)
      onComposerDraftSubmitted?.()
      setPrompt('')
      setCursor(0)
      setDismissedAutocomplete(null)
      setAttachments([])
      cleared = true
      const threadId = await pending
      rememberSelection(selection)
      if (newTask && typeof threadId === 'string') onActivated?.(threadId)
    } catch (error) {
      if (cleared && mounted.current) {
        setPrompt((current) => current || submittedPrompt)
        setAttachments((current) => current.length ? current : submittedAttachments)
      }
      toast.error(errorMessage(error))
    } finally {
      setSubmitting(false)
    }
  }

  async function steer() {
    if (!hasDraft || !canSteer || !task) return
    if (executeLocalComposerCommand()) return
    const submittedPrompt = prompt
    const submittedAttachments = attachments
    let cleared = false
    setSubmitting(true)
    try {
      const pending = actions.sendMessage(task, messageInput(submittedPrompt, submittedAttachments), 'steer')
      onComposerDraftSubmitted?.()
      setPrompt('')
      setCursor(0)
      setDismissedAutocomplete(null)
      setAttachments([])
      cleared = true
      await pending
    } catch (error) {
      if (cleared && mounted.current) {
        setPrompt((current) => current || submittedPrompt)
        setAttachments((current) => current.length ? current : submittedAttachments)
      }
      toast.error(errorMessage(error))
    } finally {
      setSubmitting(false)
    }
  }

  async function addFiles(files: File[]) {
    if (!connection || !files.length) return
    setUploading(true)
    try {
      const imported = await importFiles(connection, files)
      if (!mounted.current) return
      setAttachments((current) => [...current, ...imported])
    } catch (error) {
      toast.error(errorMessage(error))
    } finally {
      setUploading(false)
    }
  }

  async function addDaemonFile(path: string): Promise<boolean> {
    if (!connection) return false
    if (attachments.some((attachment) => attachment.mention === path)) return true
    setUploading(true)
    try {
      const imported = await importDaemonPathAttachment(connection, path)
      if (!mounted.current) return false
      setAttachments((current) => current.some((attachment) => attachment.mention === imported.mention)
        ? current
        : [...current, imported])
      return true
    } catch (error) {
      toast.error(errorMessage(error))
      return false
    } finally {
      setUploading(false)
    }
  }

  function rememberSelection(next: T.ModelSelection) {
    if (!config || !next.model) return
    const storage = browserComposerPreferenceStorage()
    writeComposerPreferences(
      storage,
      config.address,
      rememberComposerSelection(readComposerPreferences(storage, config.address), next),
    )
  }

  function applySelection(next: T.ModelSelection) {
    rememberSelection(next)
    if (newTask) onNewTaskChange?.({ ...newTask, selection: next })
    else if (task) {
      void actions.setSelection(task.id, next).catch((error) => toast.error(errorMessage(error)))
    }
  }

  function applyRuntimeMode(mode: T.RuntimeMode) {
    if (newTask) onNewTaskChange?.({ ...newTask, runtimeMode: mode })
    else if (task) {
      void actions.setRuntimeMode(task.id, mode).catch((error) => toast.error(errorMessage(error)))
    }
  }

  function acceptAutocomplete(index = autocompleteHighlight) {
    if (!autocompleteTrigger) return
    const row = autocompleteRows[index]
    if (!row) return
    const replacement = replaceComposerTrigger(prompt, autocompleteTrigger, row)
    if (row.kind === 'command' && executeLocalComposerCommand(replacement.text)) return
    pendingCursor.current = replacement.cursor
    setPrompt(replacement.text)
    setDismissedAutocomplete(null)
    setAutocompleteSelection({ key: '', index: 0 })
  }

  function moveAutocomplete(direction: -1 | 1) {
    if (!autocompleteKey || !autocompleteRows.length) return
    const next = (autocompleteHighlight + direction + autocompleteRows.length)
      % autocompleteRows.length
    setAutocompleteSelection({ key: autocompleteKey, index: next })
    autocompleteList.current?.scrollToIndex({ index: next, align: 'center' })
  }

  function keyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (autocompleteVisible && event.key === 'Escape') {
      event.preventDefault()
      setDismissedAutocomplete(autocompleteKey)
      return
    }
    if (autocompleteOpen) {
      if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault()
        moveAutocomplete(event.key === 'ArrowDown' ? 1 : -1)
        return
      }
      if (
        (event.key === 'Enter' && !event.shiftKey && !event.metaKey && !event.ctrlKey && !event.altKey)
        || (event.key === 'Tab' && !event.shiftKey)
      ) {
        event.preventDefault()
        acceptAutocomplete()
        return
      }
    }
    if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent.isComposing) return
    event.preventDefault()
    if ((event.metaKey || event.ctrlKey) && canSteer) void steer()
    else void submit()
  }

  async function switchBranch(branch: string, create = false) {
    if (!connection || !config || !branch || branchPending) return
    if (!create && newTask?.isolated) {
      onNewTaskChange?.({ ...newTask, baseRef: branch })
      return
    }
    setBranchPending(true)
    try {
      await checkoutWorkspaceBranch(connection, root, branch, create)
      await queryClient.invalidateQueries({
        queryKey: daemonKeys.workspace(config.address, rootId),
      })
    } catch (error) {
      toast.error(errorMessage(error))
    } finally {
      setBranchPending(false)
    }
  }

  return (
    <div className="shrink-0 px-3 pb-2 sm:px-5">
      <div className="mx-auto w-full max-w-[720px]">
        {task && approval && !userInput && (
          <section className="mb-2 rounded-xl border border-[color:var(--warning)]/30 bg-card p-3 shadow-lg">
            <div className="text-[13px] font-medium">{approval.title}</div>
            {approval.detail && (
              <p className="mt-1 max-h-24 overflow-auto whitespace-pre-wrap text-xs leading-5 text-[var(--text-tertiary)]">
                {approval.detail}
              </p>
            )}
            <div className="mt-3 flex flex-wrap justify-end gap-2">
              {approval.options.map((option) => (
                <Button
                  key={option.id}
                  size="sm"
                  variant={option.allow ? 'default' : 'outline'}
                  onClick={() => {
                    void actions.respondApproval(task.id, approval.requestId, option.id).catch((error) =>
                      toast.error(errorMessage(error)),
                    )
                  }}
                >
                  {option.allow && <WakuIcon name="check" />}
                  {option.label}
                </Button>
              ))}
            </div>
          </section>
        )}

        {task && userInput && (
          <UserInputPanel
            input={userInput}
            onDismiss={() => actions.dismissUserInput(task.id, userInput.requestId)}
            onSubmit={(answers) => actions.respondUserInput(
              task.id,
              userInput.requestId,
              answers,
            )}
          />
        )}

        {task && (
          <QueuedMessages
            canSteer={canSteer}
            messages={task.queued}
            onEdit={(message) => {
              // Queued runs cannot be edited in place yet; take it back into the composer.
              setPrompt(message.displayText ?? message.text)
              setAttachments(message.attachments)
              void actions.cancelQueued(task.id, message.runId).catch((error) =>
                toast.error(errorMessage(error)),
              )
            }}
            onRemove={(runId) =>
              void actions.cancelQueued(task.id, runId).catch((error) =>
                toast.error(errorMessage(error)),
              )
            }
            onSteer={(message) => {
              void actions.cancelQueued(task.id, message.runId)
                .then(() => actions.sendMessage(
                  task,
                  {
                    text: message.displayText ?? message.text,
                    attachments: message.attachments,
                    providerText: message.text,
                  },
                  'steer',
                ))
                .catch((error) => toast.error(errorMessage(error)))
            }}
          />
        )}

        <div className="relative">
          {autocompleteVisible && (
            <ComposerAutocomplete
              highlight={autocompleteHighlight}
              listRef={autocompleteList}
              loading={autocompleteLoading && autocompleteRows.length === 0}
              rows={autocompleteRows}
              t={t}
              onAccept={acceptAutocomplete}
              onHighlight={(index) => {
                if (autocompleteKey) {
                  setAutocompleteSelection({ key: autocompleteKey, index })
                }
              }}
            />
          )}
          <section
            className="rounded-[13px] border bg-card p-2.5 focus-within:border-input"
            onDragOver={(event) => {
              event.preventDefault()
              event.dataTransfer.dropEffect = 'copy'
            }}
            onDrop={(event) => {
              event.preventDefault()
              void addFiles([...event.dataTransfer.files])
            }}
          >
          {attachments.length > 0 && (
            <div className="flex flex-wrap gap-2 px-1 pb-2 pt-0.5">
              {attachments.map((attachment, index) => (
                <ComposerAttachmentTile
                  attachment={attachment}
                  key={attachment.blobReference ?? `${attachment.mention}-${index}`}
                  t={t}
                  onRemove={() => setAttachments((current) => current.filter((_, item) => item !== index))}
                />
              ))}
            </div>
          )}
            <Textarea
              aria-controls={autocompleteVisible ? 'composer-autocomplete' : undefined}
              aria-expanded={autocompleteVisible}
              aria-label={t('composer.message')}
              aria-activedescendant={autocompleteOpen
                ? `composer-autocomplete-${autocompleteHighlight}`
                : undefined}
              aria-autocomplete="list"
              className="max-h-48 min-h-[46px] resize-none border-0 bg-transparent px-1 pb-1 pt-0 text-[14px] leading-5 shadow-none focus-visible:ring-0"
              placeholder={t(busy ? 'composer.queue_placeholder' : 'composer.prompt_placeholder')}
              ref={composerInput}
              role="combobox"
              value={prompt}
              onBlur={() => setInputFocused(false)}
              onChange={(event) => {
                setPrompt(event.target.value)
                setCursor(event.target.selectionStart)
              }}
              onClick={(event) => setCursor(event.currentTarget.selectionStart)}
              onFocus={() => setInputFocused(true)}
              onKeyDown={keyDown}
              onSelect={(event) => setCursor(event.currentTarget.selectionStart)}
            />
          <div
            className="mt-2 flex min-w-0 items-center gap-1 pb-px text-[11.5px] leading-[14px]"
            onMouseDown={preserveComposerFocusOnMouseDown}
          >
            <ModelPicker
              currentProbe={probe.data}
              disabled={busy}
              locked={Boolean(task?.runs.length)}
              modelId={selection.model}
              openSignal={modelPickerSignal}
              provider={provider}
              onOpenSignalHandled={onModelPickerSignalHandled}
              returnFocus={composerInput}
              onChange={(nextProvider, model) => {
                const preferences = config
                  ? readComposerPreferences(browserComposerPreferenceStorage(), config.address)
                  : null
                const remembered = preferences
                  ? rememberedModelTraits(preferences, nextProvider, model.id)
                  : undefined
                const next: T.ModelSelection = { instance: nextProvider, model: model.id }
                const reasoningEffort = remembered?.reasoningEffort ?? model.defaultReasoningEffort
                const serviceTier = remembered?.serviceTier ?? model.defaultServiceTier
                const contextWindow = remembered?.contextWindow ?? undefined
                if (reasoningEffort) next.reasoningEffort = reasoningEffort
                if (serviceTier) next.serviceTier = serviceTier
                if (contextWindow) next.contextWindow = contextWindow
                if (nextProvider === provider && selection.agentPreset) next.agentPreset = selection.agentPreset
                applySelection(next)
              }}
            />
            {selectedModel && (
              <ModelTraitsControl
                model={selectedModel}
                returnFocus={composerInput}
                selection={selection}
                onSelection={applySelection}
              />
            )}
            {newTask && (
              <AgentPresetControl
                probe={probe.data}
                returnFocus={composerInput}
                selection={selection}
                onSelection={applySelection}
              />
            )}
            <AccessControl mode={runtimeMode} returnFocus={composerInput} onMode={applyRuntimeMode} />
            <GoalControl
              busy={busy}
              goal={goal}
              onOpen={() => setGoalDialog({ prefill: null, replace: false })}
            />
            <div className="flex-1" />
            <div className="flex items-center gap-2">
              {busy && (
                <Button
                  aria-label={t(escapeStopArmed ? 'composer.stop_confirm' : 'composer.stop')}
                  className="rounded-full"
                  size="icon-sm"
                  variant="secondary"
                  onClick={stopTurn}
                >
                  {escapeStopArmed
                    ? <span className="text-[10px] font-semibold">Esc</span>
                    : <WakuIcon className="size-[18px]" name="stopFilled" />}
                </Button>
              )}
              <Button
                aria-expanded={filePickerOpen}
                aria-haspopup="dialog"
                aria-label={t('composer.attach_daemon')}
                className="size-6 rounded-md text-[var(--text-secondary)]"
                disabled={!connection || uploading}
                size="icon-sm"
                title={t('composer.attach_daemon_title')}
                type="button"
                variant="ghost"
                onClick={() => setFilePickerOpen(true)}
              >
                <WakuIcon className="size-[14px]" name="paperclip" />
              </Button>
              {busy ? (
                hasDraft && (
                  <Button
                    aria-label={t('composer.queue_followup')}
                    className="rounded-full"
                    disabled={submitting || uploading}
                    size="icon-sm"
                    onClick={() => void submit()}
                  >
                    <WakuIcon name="arrowUp" />
                  </Button>
                )
              ) : (
                <Button
                  aria-label={t('common.send')}
                  className="rounded-full"
                  disabled={submitting || uploading || !hasDraft}
                  size="icon-sm"
                  onClick={() => void submit()}
                >
                  <WakuIcon name="arrowUp" />
                </Button>
              )}
            </div>
            </div>
          </section>
        </div>

        {filePickerOpen && (
          <DaemonFilePicker
            root={cwd ?? null}
            returnFocus={composerInput}
            workspaceLabel={projectName}
            onClose={() => setFilePickerOpen(false)}
            onSelect={addDaemonFile}
          />
        )}
        <GoalDialog
          goal={goal}
          open={goalDialog !== null}
          prefill={goalDialog?.prefill ?? null}
          replace={goalDialog?.replace ?? false}
          returnFocus={composerInput}
          onClear={goalUnsupported}
          onOpenChange={(open) => {
            if (!open) setGoalDialog(null)
          }}
          onSetStatus={goalUnsupported}
          onSubmit={goalUnsupported}
        />

        <div
          className="flex h-8 min-w-0 items-center gap-1 px-2 text-[11px] text-[var(--text-tertiary)]"
          onMouseDown={preserveComposerFocusOnMouseDown}
        >
          <ControlMenu
            caret={false}
            disabled={busy || started}
            icon="folder"
            items={[
              ...projectChoices
                .filter((item) => item.name !== 'No project')
                .map((item) => ({
                  id: item.id,
                  label: item.name,
                  selected: item.id === project.id,
                  onSelect: () => {
                    if (newTask) onNewTaskChange?.({ ...newTask, projectId: item.id, isolated: false, baseRef: undefined })
                  },
                })),
              ...(onAddProject ? [{
                id: 'new-project',
                label: t('project.new_project'),
                icon: 'folderNew' as const,
                onSelect: onAddProject,
              }] : []),
              ...(onProjectless ? [{
                id: 'projectless',
                label: t('project.no_project'),
                icon: 'x' as const,
                selected: projectless,
                onSelect: projectless ? () => {} : onProjectless,
              }] : []),
            ]}
            label={projectless ? t('project.choose_project') : projectName}
            menuClassName="w-56"
            returnFocus={composerInput}
            triggerClassName="h-6 max-w-36 px-1.5 text-[11px]"
          />
          <ControlMenu
            caret={false}
            disabled={busy || started}
            icon={workspaceLocal ? 'laptop' : 'fork'}
            items={[
              {
                id: 'local',
                section: t('workspace.work_in'),
                label: t('workspace.local'),
                icon: 'laptop',
                selected: workspaceLocal,
                onSelect: () => {
                  if (newTask) onNewTaskChange?.({ ...newTask, isolated: false, baseRef: undefined })
                },
              },
              {
                id: 'newWorktree',
                section: t('workspace.work_in'),
                label: t('workspace.new_worktree'),
                icon: 'fork',
                disabled: projectless,
                selected: !workspaceLocal,
                onSelect: () => {
                  if (newTask) onNewTaskChange?.({ ...newTask, isolated: true })
                },
              },
            ]}
            label={workspaceLabel}
            returnFocus={composerInput}
            triggerClassName="h-6 max-w-32 px-1.5 text-[11px]"
          />
          {!projectless && branches.data && (
            <BranchPicker
              disabled={busy || branchPending}
              pending={branchPending}
              plannedBase={newTask?.baseRef}
              plannedWorktree={plannedWorktree}
              returnFocus={composerInput}
              snapshot={branches.data}
              worktreeBranch={task?.branch}
              onCreate={(branch) => void switchBranch(branch, true)}
              onRefresh={() => void branches.refetch()}
              onSelect={(branch) => void switchBranch(branch)}
            />
          )}
          <div className="flex-1" />
          {task?.status === 'preparing' && <span>{t('composer.starting_agent')}</span>}
          <UsageMeter
            contextUsage={task?.contextUsage}
            openSignal={usagePanelSignal}
            provider={provider}
            returnFocus={composerInput}
            onOpenSignalHandled={onUsagePanelSignalHandled}
          />
        </div>
      </div>
    </div>
  )
}

function UserInputPanel({
  input,
  onSubmit,
  onDismiss,
}: {
  input: PendingUserInput
  onSubmit: (answers: Array<{ questionId: string; answers: string[] }>) => Promise<void>
  onDismiss: () => Promise<void>
}) {
  const { t } = useI18n()
  const [questionIndex, setQuestionIndex] = useState(0)
  const [selections, setSelections] = useState<Record<string, string[]>>({})
  const [customAnswers, setCustomAnswers] = useState<Record<string, string>>({})
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    setQuestionIndex(0)
    setSelections({})
    setCustomAnswers({})
    setSubmitting(false)
  }, [input.requestId])

  const question = input.questions[questionIndex]
  if (!question) return null
  const selected = selections[question.id] ?? []
  const custom = customAnswers[question.id] ?? ''
  const canContinue = Boolean(custom.trim() || selected.length)
  const last = questionIndex + 1 === input.questions.length

  function select(label: string) {
    setCustomAnswers((current) => ({ ...current, [question.id]: '' }))
    setSelections((current) => {
      const previous = current[question.id] ?? []
      return {
        ...current,
        [question.id]: question.multiSelect
          ? previous.includes(label)
            ? previous.filter((answer) => answer !== label)
            : [...previous, label]
          : [label],
      }
    })
  }

  function setCustom(value: string) {
    setCustomAnswers((current) => ({ ...current, [question.id]: value }))
    if (value.trim()) {
      setSelections((current) => ({ ...current, [question.id]: [] }))
    }
  }

  async function advance() {
    if (!canContinue || submitting) return
    if (!last) {
      setQuestionIndex((current) => current + 1)
      return
    }
    setSubmitting(true)
    try {
      await onSubmit(input.questions.map((item) => {
        const custom = customAnswers[item.id]?.trim()
        return {
          questionId: item.id,
          answers: custom ? [custom] : selections[item.id] ?? [],
        }
      }))
    } catch (error) {
      toast.error(errorMessage(error))
      setSubmitting(false)
    }
  }

  return (
    <section className="mb-2 w-full rounded-[13px] border bg-card px-3.5 pb-2.5 pt-3">
      <div className="flex items-center gap-2">
        <div className="text-[11px] font-semibold text-[var(--text-tertiary)]">
          {question.header}
        </div>
        {input.questions.length > 1 && (
          <div className="flex h-[18px] items-center rounded-[5px] bg-muted/50 px-1.5 text-[10px] font-medium tabular-nums text-[var(--text-tertiary)]">
            {t('user_input.progress', {
              current: questionIndex + 1,
              total: input.questions.length,
            })}
          </div>
        )}
      </div>
      <p className="mt-1.5 whitespace-pre-wrap text-[13px] font-medium leading-[18px]">
        {question.question}
      </p>
      {!!question.options.length && (
        <div className="mt-2.5 grid gap-1" role={question.multiSelect ? 'group' : 'radiogroup'}>
          {question.options.map((option) => {
            const checked = selected.includes(option.label)
            return (
              <button
                aria-checked={checked}
                className={cn(
                  'flex min-h-9 w-full items-center gap-2 rounded-lg border border-transparent bg-muted/40 px-2.5 py-1.5 text-left outline-none transition-colors hover:border-border hover:bg-muted/60 focus-visible:border-ring focus-visible:ring-1 focus-visible:ring-ring/30',
                  checked && 'border-primary/35 bg-primary/[0.08]',
                )}
                key={option.label}
                role={question.multiSelect ? 'checkbox' : 'radio'}
                type="button"
                onClick={() => select(option.label)}
              >
                <span className="min-w-0 flex-1">
                  <span className="block text-[12px] font-medium">{option.label}</span>
                  {option.description && option.description !== option.label && (
                    <span className="mt-0.5 block text-[10.5px] leading-[15px] text-[var(--text-tertiary)]">
                      {option.description}
                    </span>
                  )}
                </span>
                {checked && <WakuIcon className="size-3 shrink-0 text-primary" name="check" />}
              </button>
            )
          })}
        </div>
      )}
      <div
        className={cn(
          'mt-1 flex h-[34px] items-center gap-2 rounded-lg border border-transparent bg-muted/40 px-2.5 focus-within:border-ring',
          custom.trim() && 'border-primary/35 bg-primary/[0.06]',
        )}
      >
        <WakuIcon
          className={cn('size-3 shrink-0 text-[var(--text-ghost)]', custom.trim() && 'text-primary')}
          name="pencil"
        />
        <input
          className="min-w-0 flex-1 bg-transparent text-[12px] outline-none placeholder:text-[var(--text-ghost)]"
          placeholder={t('user_input.other_placeholder')}
          value={custom}
          onChange={(event) => setCustom(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && !event.nativeEvent.isComposing) {
              event.preventDefault()
              void advance()
            }
          }}
        />
      </div>
      <div className="mt-2 flex items-center gap-2">
        {questionIndex > 0 && (
          <Button
            className="h-7 px-2 text-[11px]"
            size="sm"
            variant="ghost"
            onClick={() => setQuestionIndex((value) => value - 1)}
          >
            {t('user_input.back')}
          </Button>
        )}
        <div className="flex-1" />
        <Button
          className="h-7 px-2 text-[11px]"
          disabled={submitting}
          size="sm"
          variant="ghost"
          onClick={() => {
            setSubmitting(true)
            void onDismiss().catch((error) => {
              toast.error(errorMessage(error))
              setSubmitting(false)
            })
          }}
        >
          {t('common.cancel')}
        </Button>
        <Button
          className="h-7 px-2.5 text-[11px]"
          disabled={!canContinue || submitting}
          size="sm"
          onClick={() => void advance()}
        >
          {last ? t('user_input.submit') : t('user_input.next')}
        </Button>
      </div>
    </section>
  )
}

function ComposerAutocomplete({
  rows,
  highlight,
  listRef,
  loading,
  t,
  onAccept,
  onHighlight,
}: {
  rows: ComposerAutocompleteRow[]
  highlight: number
  listRef: RefObject<VirtuosoHandle | null>
  loading: boolean
  t: Translator
  onAccept: (index: number) => void
  onHighlight: (index: number) => void
}) {
  return (
    <div
      aria-label={t('composer.suggestions')}
      aria-live={loading ? 'polite' : undefined}
      className="waku-popover-surface absolute bottom-[calc(100%+6px)] left-0 z-[70] w-full overflow-hidden rounded-[11px] p-1"
      id="composer-autocomplete"
      role={loading ? 'status' : 'listbox'}
      style={{ height: loading ? 38 : Math.min(302, rows.length * 30 + 8) }}
    >
      {loading
        ? (
            <div className="flex h-[30px] items-center gap-2 px-2 text-[12px] text-[var(--text-tertiary)]">
              <WakuIcon className="size-3 motion-safe:animate-spin" name="loaderCircle" />
              {t('composer.loading_suggestions')}
            </div>
          )
        : (
            <Virtuoso
              className="size-full outline-none"
              computeItemKey={(_, row) => row.kind === 'command'
                ? `command:${row.command.scope}:${row.command.name}`
                : `file:${row.file.path}`}
              data={rows}
              fixedItemHeight={30}
              increaseViewportBy={90}
              ref={listRef}
              itemContent={(index, row) => (
                <button
                  aria-selected={highlight === index}
                  className={cn(
                    'flex h-[30px] w-full items-center gap-2 rounded-md px-2 text-left outline-none',
                    highlight === index ? 'bg-accent' : 'hover:bg-accent/70',
                  )}
                  id={`composer-autocomplete-${index}`}
                  role="option"
                  tabIndex={-1}
                  type="button"
                  onMouseDown={(event) => {
                    event.preventDefault()
                    onAccept(index)
                  }}
                  onMouseEnter={() => onHighlight(index)}
                >
                  <AutocompleteRowContents row={row} />
                </button>
              )}
            />
          )}
    </div>
  )
}

function AutocompleteRowContents({ row }: { row: ComposerAutocompleteRow }) {
  if (row.kind === 'command') {
    const { command } = row
    return (
      <>
        {command.scope === 'skill'
          ? <WakuIcon className="size-3 text-[var(--text-tertiary)]" name="sparkle" />
          : <WakuIcon className="size-3 text-[var(--text-tertiary)]" name="command" />}
        <span className="max-w-[260px] shrink-0 truncate text-[12px] font-medium">
          /{command.name}
        </span>
        {command.argumentHint && (
          <span className="shrink-0 text-[11px] text-[var(--text-ghost)]">
            {command.argumentHint}
          </span>
        )}
        <span className="min-w-0 flex-1 truncate text-[11px] text-[var(--text-tertiary)]">
          {command.description}
        </span>
        <span className="flex h-4 shrink-0 items-center rounded border px-1.5 text-[9px] font-semibold text-[var(--text-tertiary)]">
          {command.scope}
        </span>
      </>
    )
  }

  const trimmedLength = row.file.path.replace(/\/+$/u, '').length
  const nameStart = row.file.path.lastIndexOf('/', Math.max(0, trimmedLength - 1)) + 1
  const name = row.file.path.slice(nameStart)
  const parent = row.file.path.slice(0, Math.max(0, nameStart - 1))
  return (
    <>
      {row.file.isDir
        ? <WakuIcon className="size-[13px] text-[var(--text-tertiary)]" name="folder" />
        : <FileTypeIcon className="size-[13px]" path={row.file.path} />}
      <span className="max-w-[300px] shrink-0 truncate text-[12px]">{name}</span>
      {parent && (
        <span className="min-w-0 flex-1 truncate text-[11px] text-[var(--text-ghost)]">
          {parent}
        </span>
      )}
    </>
  )
}

function ComposerAttachmentTile({
  attachment,
  onRemove,
  t,
}: {
  attachment: T.Attachment
  onRemove: () => void
  t: Translator
}) {
  const { connection, phase } = useDaemon()
  const [source, setSource] = useState<string | null>(null)

  useEffect(() => {
    if (!attachment.isImage || phase !== 'connected' || !connection) {
      setSource(null)
      return
    }
    let active = true
    void readAttachmentImage(connection, attachment)
      .then((value) => active && setSource(value))
      .catch(() => active && setSource(null))
    return () => { active = false }
  }, [attachment.blobReference, attachment.isImage, attachment.name, attachment.path, connection, phase])

  const contents = attachment.isImage && source ? (
    <PreviewableImage
      buttonClassName="size-full"
      imageClassName="size-full object-cover"
      name={attachment.name}
      source={source}
    />
  ) : (
    <div className="flex size-full flex-col items-center justify-center gap-[5px] px-[5px]">
      {attachment.isDir
        ? <WakuIcon className="size-4 text-[var(--text-tertiary)]" name="folder" />
        : <FileTypeIcon className="size-4" path={attachment.mention || attachment.name} />}
      {!attachment.isImage && (
        <span className="w-full truncate text-center text-[8.5px] text-[var(--text-tertiary)]">
          {attachment.name}
        </span>
      )}
    </div>
  )

  return (
    <div
      className="relative size-16 overflow-hidden rounded-lg border bg-[var(--inset)] outline-none focus-within:border-ring"
      title={`@${attachment.mention}`}
    >
      {contents}
      <button
        aria-label={t('composer.remove_attachment', { name: attachment.name })}
        className="absolute right-[3px] top-[3px] z-10 grid size-4 place-items-center rounded-[5px] bg-background/80 text-[var(--text-secondary)] outline-none hover:bg-background focus-visible:ring-1 focus-visible:ring-ring"
        type="button"
        onClick={onRemove}
        onMouseDown={(event) => event.preventDefault()}
      >
        <WakuIcon className="size-[9px]" name="x" />
      </button>
    </div>
  )
}

const GOAL_CHIP_KEYS: Record<string, string> = {
  active: 'goal.chip_active',
  paused: 'goal.chip_paused',
  blocked: 'goal.chip_stalled',
  usage_limited: 'goal.chip_usage_limited',
  budget_limited: 'goal.chip_budget_limited',
  complete: 'goal.chip_complete',
}

/** The thread-goal chip: present only while the provider reports a goal, it
 * pairs a target icon with the status phrase and consumption — token budget
 * when one bounds the goal, elapsed pursuit time otherwise (ticking live
 * while the task works, like the Codex CLI) — and opens the goal dialog. */
function GoalControl({
  goal,
  busy,
  onOpen,
}: {
  goal: T.ThreadGoal | null
  busy: boolean
  onOpen: () => void
}) {
  const { t } = useI18n()
  const observedAt = useRef(Date.now())
  const accountingKey = goal
    ? [goal.objective, goal.status, goal.timeUsedSeconds, goal.tokensUsed].join('\u0000')
    : ''
  useEffect(() => {
    observedAt.current = Date.now()
  }, [accountingKey])
  const ticking = Boolean(goal && goal.status === 'active' && busy && goal.tokenBudget == null)
  const [, setTick] = useState(0)
  useEffect(() => {
    if (!ticking) return
    const timer = window.setInterval(() => setTick((value) => value + 1), 1_000)
    return () => window.clearInterval(timer)
  }, [ticking])
  if (!goal) return null
  const liveElapsed = goal.status === 'active' && busy
    ? Math.floor((Date.now() - observedAt.current) / 1_000)
    : 0
  const chipKey = GOAL_CHIP_KEYS[goal.status]
  const phrase = chipKey ? t(chipKey) : goal.status
  const usage = ['active', 'complete', 'budget_limited'].includes(goal.status)
    ? goalUsageReadout(goal, liveElapsed)
    : null
  return (
    <button
      className={cn(
        'flex h-6 items-center gap-1.5 rounded-md px-1.5 outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring',
        goalStatusClass(goal.status),
      )}
      title={goal.objective}
      type="button"
      onClick={onOpen}
    >
      <WakuIcon className="size-[11px]" name="target" />
      <span className="max-w-[220px] truncate">
        {usage ? `${phrase} (${usage})` : phrase}
      </span>
    </button>
  )
}

function ModelTraitsControl({
  selection,
  model,
  onSelection,
  returnFocus,
}: {
  selection: T.ModelSelection
  model: T.ProviderModel
  onSelection: (selection: T.ModelSelection) => void
  returnFocus: RefObject<HTMLElement | null>
}) {
  const { t } = useI18n()
  const reasoningEfforts = model.reasoningEfforts ?? []
  const serviceTiers = model.serviceTiers ?? []
  const contextWindows = model.contextWindows ?? []
  if (!reasoningEfforts.length && !serviceTiers.length && !contextWindows.length) {
    return null
  }
  const effort = selection.reasoningEffort
    && reasoningEfforts.some((option) => option.id === selection.reasoningEffort)
    ? selection.reasoningEffort
    : model.defaultReasoningEffort ?? reasoningEfforts[0]?.id ?? null
  const effortOption = reasoningEfforts.find((option) => option.id === effort)
  const effortLabel = effortOption && modelOptionLabel(effortOption.id, effortOption.label, t)
  const tier = selection.serviceTier
    && (selection.serviceTier === 'default' || serviceTiers.some((option) => option.id === selection.serviceTier))
    ? selection.serviceTier
    : model.defaultServiceTier ?? 'default'
  const tierLabel = tier === 'default'
    ? t('models.standard')
    : modelOptionLabel(tier, serviceTiers.find((option) => option.id === tier)?.label ?? tier, t)
  const fast = tier === 'fast' || tierLabel.toLowerCase() === 'fast'
  const contextWindow = selection.contextWindow
    && contextWindows.some((option) => option.id === selection.contextWindow)
    ? selection.contextWindow
    : model.defaultContextWindow ?? contextWindows[0]?.id ?? null
  // A non-default window changes cost and capacity, so it reads on the trigger.
  const windowOption = contextWindow && contextWindow !== model.defaultContextWindow
    ? contextWindows.find((option) => option.id === contextWindow)
    : undefined
  const windowLabel = windowOption && modelOptionLabel(windowOption.id, windowOption.label, t)
  const items: ControlMenuItem[] = [
    ...reasoningEfforts.map((option) => ({
      id: `effort-${option.id}`,
      section: t('models.reasoning'),
      label: modelOptionLabel(option.id, option.label, t),
      description: option.description ?? undefined,
      suffix: model.defaultReasoningEffort === option.id ? t('common.default') : undefined,
      selected: effort === option.id,
      onSelect: () => onSelection({ ...selection, reasoningEffort: option.id }),
    })),
    ...(serviceTiers.length ? [
      {
        id: 'tier-default',
        section: t('models.service_tier'),
        label: t('models.standard'),
        suffix: (model.defaultServiceTier ?? 'default') === 'default' ? t('common.default') : undefined,
        selected: tier === 'default',
        onSelect: () => onSelection({ ...selection, serviceTier: 'default' }),
      },
      ...serviceTiers.map((option) => ({
        id: `tier-${option.id}`,
        section: t('models.service_tier'),
        label: modelOptionLabel(option.id, option.label, t),
        description: option.description ?? undefined,
        suffix: model.defaultServiceTier === option.id ? t('common.default') : undefined,
        selected: tier === option.id,
        onSelect: () => onSelection({ ...selection, serviceTier: option.id }),
      })),
    ] : []),
    ...contextWindows.map((option) => ({
      id: `context-${option.id}`,
      section: t('models.context_window'),
      label: modelOptionLabel(option.id, option.label, t),
      description: option.description ?? undefined,
      suffix: model.defaultContextWindow === option.id ? t('common.default') : undefined,
      selected: contextWindow === option.id,
      onSelect: () => onSelection({ ...selection, contextWindow: option.id }),
    })),
  ]
  return (
    <ControlMenu
      caret={false}
      icon={fast ? 'zap' : undefined}
      items={items}
      label={windowLabel ? `${effortLabel ?? tierLabel} · ${windowLabel}` : effortLabel ?? tierLabel}
      menuClassName="w-56"
      returnFocus={returnFocus}
    />
  )
}

function AgentPresetControl({
  selection,
  probe,
  onSelection,
  returnFocus,
}: {
  selection: T.ModelSelection
  probe?: ProviderCatalog
  onSelection: (selection: T.ModelSelection) => void
  returnFocus: RefObject<HTMLElement | null>
}) {
  const { t } = useI18n()
  // Presets are chosen before the first message (DeepSeek Harness).
  if (selection.instance !== 'deepseek' || !probe?.agentPresets.length) return null
  const selected = probe.agentPresets.find((preset) => preset.id === selection.agentPreset)
    ?? probe.agentPresets.find((preset) => preset.isDefault)
    ?? probe.agentPresets[0]
  return (
    <ControlMenu
      caret={false}
      icon="bot"
      items={probe.agentPresets.map((preset) => ({
        id: preset.id,
        label: `${agentPresetLabel(preset, t)}${preset.isCustom ? ` · ${t('agent_preset.custom')}` : ''}`,
        description: agentPresetDescription(preset, t) ?? t('agent_preset.no_description'),
        selected: preset.id === selected?.id,
        onSelect: () => onSelection({ ...selection, agentPreset: preset.id }),
      }))}
      label={selected ? agentPresetLabel(selected, t) : t('agent_preset.standard')}
      menuClassName="w-80"
      returnFocus={returnFocus}
    />
  )
}

const ACCESS_MODES: Array<{
  id: T.RuntimeMode
  labelKey: string
  descriptionKey: string
  icon: 'lock' | 'pencil' | 'sparkle' | 'lockOpen'
}> = [
  { id: 'ask', labelKey: 'mode.supervised', descriptionKey: 'mode.supervised_description', icon: 'lock' },
  { id: 'auto_accept_edits', labelKey: 'mode.auto_accept_edits', descriptionKey: 'mode.auto_accept_edits_description', icon: 'pencil' },
  { id: 'auto', labelKey: 'mode.auto', descriptionKey: 'mode.auto_description', icon: 'sparkle' },
  { id: 'full_access', labelKey: 'mode.full_access', descriptionKey: 'mode.full_access_description', icon: 'lockOpen' },
]

function AccessControl({
  mode: current,
  onMode,
  returnFocus,
}: {
  mode: T.RuntimeMode
  onMode: (mode: T.RuntimeMode) => void
  returnFocus: RefObject<HTMLElement | null>
}) {
  const { t } = useI18n()
  const selected = ACCESS_MODES.find((mode) => mode.id === current) ?? ACCESS_MODES[3]!
  return (
    <ControlMenu
      caret={false}
      icon={selected.icon}
      items={ACCESS_MODES.map((mode) => ({
        id: mode.id,
        icon: mode.icon,
        label: t(mode.labelKey),
        description: t(mode.descriptionKey),
        selected: mode.id === selected.id,
        onSelect: () => onMode(mode.id),
      }))}
      label={t(selected.labelKey)}
      menuClassName="w-[304px]"
      returnFocus={returnFocus}
    />
  )
}

function QueuedMessages({
  messages,
  canSteer,
  onEdit,
  onSteer,
  onRemove,
}: {
  messages: QueuedMessage[]
  canSteer: boolean
  onEdit: (message: QueuedMessage) => void
  onSteer: (message: QueuedMessage) => void
  onRemove: (runId: string) => void
}) {
  const { t } = useI18n()
  if (!messages.length) return null
  return (
    <div className="px-3.5">
      {/* The card tucks against the composer's top edge: rounded top corners
          only, open bottom, and overflow-hidden so row hover fills clip to
          the rounding. */}
      <div className="overflow-hidden rounded-t-xl border border-b-0 bg-card py-1">
        {messages.map((message) => (
          <div
            className="flex h-[30px] w-full items-center gap-2 pr-1.5 text-[12.5px] hover:bg-accent"
            key={message.runId}
          >
            <button
              className="flex min-w-0 flex-1 items-center gap-2 self-stretch pl-3 text-left outline-none focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring"
              title={t('composer.edit_in_composer')}
              type="button"
              onClick={() => onEdit(message)}
            >
              <WakuIcon className="size-3 shrink-0 text-[var(--text-tertiary)]" name="queue" />
              <span className="min-w-0 flex-1 truncate">
                {message.displayText || message.text || message.attachments.map((item) => item.name).join(', ')}
              </span>
            </button>
            <div className="flex shrink-0 items-center gap-0.5">
              {canSteer && (
                <button
                  className="flex h-6 items-center gap-1.5 rounded-md px-1.5 text-[11.5px] text-[var(--text-secondary)] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring"
                  title={t('composer.steer_current')}
                  type="button"
                  onClick={() => onSteer(message)}
                >
                  <WakuIcon className="size-[11px]" name="cornerDownRight" />
                  {t('composer.steer')}
                </button>
              )}
              <button
                aria-label={t('composer.remove_followup')}
                className="grid size-6 shrink-0 place-items-center rounded-md text-[var(--text-secondary)] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring"
                type="button"
                onClick={() => onRemove(message.runId)}
              >
                <WakuIcon className="size-3" name="trash" />
              </button>
              <ControlMenu
                caret={false}
                items={[
                  {
                    id: 'edit',
                    label: t('composer.edit_in_composer'),
                    icon: 'pencil',
                    onSelect: () => onEdit(message),
                  },
                  {
                    id: 'remove',
                    label: t('composer.remove_followup'),
                    icon: 'trash',
                    onSelect: () => onRemove(message.runId),
                  },
                ]}
                align="right"
                label={t('composer.queued_message_actions')}
                placement="below"
                selectionMode="status"
                triggerClassName="grid size-6 place-items-center px-0 rounded-md"
              >
                <WakuIcon className="size-3" name="ellipsis" />
              </ControlMenu>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function BranchPicker({
  snapshot,
  plannedWorktree,
  plannedBase,
  worktreeBranch,
  disabled,
  pending,
  returnFocus,
  onSelect,
  onCreate,
  onRefresh,
}: {
  snapshot: T.BranchSnapshot
  /** A new worktree will start from the chosen branch; nothing is checked out. */
  plannedWorktree: boolean
  plannedBase?: string
  worktreeBranch?: string
  disabled: boolean
  pending: boolean
  returnFocus: RefObject<HTMLElement | null>
  onSelect: (branch: string) => void
  onCreate: (branch: string) => void
  onRefresh: () => void
}) {
  const { t } = useI18n()
  const [open, setOpen] = useState(false)
  const [mode, setMode] = useState<'browse' | 'create'>('browse')
  const [query, setQuery] = useState('')
  const [branchName, setBranchName] = useState('')
  const [active, setActive] = useState(0)
  const input = useRef<HTMLInputElement>(null)
  const selected = plannedWorktree
    ? plannedBase ?? snapshot.current ?? snapshot.defaultBranch ?? snapshot.detachedHead
    : snapshot.current ?? worktreeBranch ?? snapshot.detachedHead
  const normalized = query.trim().toLowerCase()
  const visible = [...snapshot.branches]
    .filter((branch) => normalized.split(/\s+/).filter(Boolean).every((part) => branch.name.toLowerCase().includes(part)))
    .sort((left, right) => {
      if (left.name === selected) return -1
      if (right.name === selected) return 1
      return left.name.localeCompare(right.name)
    })
  const actions = [
    ...visible
      .filter((branch) => plannedWorktree || !branch.checkedOutElsewhere || branch.name === selected)
      .map((branch) => ({ kind: 'branch' as const, branch })),
    ...(!plannedWorktree ? [{ kind: 'create' as const }] : []),
  ]

  function updateOpen(next: boolean) {
    setOpen(next)
    if (!next) return
    setMode('browse')
    setQuery('')
    setBranchName('')
    setActive(0)
    onRefresh()
    requestAnimationFrame(() => input.current?.focus())
  }

  function choose(branch: string) {
    onSelect(branch)
    setOpen(false)
  }

  function submitCreate() {
    const next = branchName.trim()
    if (!next) return
    onCreate(next)
    setOpen(false)
  }

  return (
    <Popover.Root modal={false} open={open} onOpenChange={updateOpen}>
      <Popover.Trigger
        className={cn(
          'flex h-6 max-w-52 shrink-0 items-center gap-1.5 rounded-[6px] px-[7px] text-[11px] text-[var(--text-secondary)] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring disabled:opacity-45',
          open && 'bg-accent text-foreground',
        )}
        disabled={disabled}
        onKeyDown={(event) => {
          if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
            event.preventDefault()
            updateOpen(true)
          }
        }}
      >
        <WakuIcon className="size-[11px] text-[var(--text-tertiary)]" name="gitBranch" />
        <span className="truncate">{pending ? t('branches.switching') : selected ?? t('branches.detached_head')}</span>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner
          align="start"
          className="z-[100] outline-none"
          collisionPadding={8}
          side="top"
          sideOffset={4}
        >
          <Popover.Popup
            aria-label={t('branches.choose')}
            className="waku-popover-surface flex max-h-[390px] w-[360px] flex-col overflow-hidden rounded-[13px] outline-none"
            finalFocus={(closeType) => closeType === 'keyboard' ? true : returnFocus.current}
            initialFocus={input}
            role="dialog"
            onKeyDown={(event) => {
              if (mode === 'create') {
                if (event.key === 'Enter') {
                  event.preventDefault()
                  submitCreate()
                }
                return
              }
              if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
                event.preventDefault()
                if (!actions.length) return
                setActive((current) => (
                  event.key === 'ArrowDown'
                    ? (current + 1) % actions.length
                    : (current - 1 + actions.length) % actions.length
                ))
              } else if (event.key === 'Home') {
                event.preventDefault()
                setActive(0)
              } else if (event.key === 'End') {
                event.preventDefault()
                setActive(Math.max(0, actions.length - 1))
              } else if (event.key === 'Enter') {
                event.preventDefault()
                const action = actions[active] ?? actions[0]
                if (action?.kind === 'branch') choose(action.branch.name)
                else if (action?.kind === 'create') {
                  setMode('create')
                  requestAnimationFrame(() => input.current?.focus())
                }
              }
            }}
          >
            {mode === 'create' ? (
              <div className="p-3.5">
                <div className="flex items-center gap-2 text-[13px] font-medium">
                  <WakuIcon className="size-3.5 text-[var(--text-secondary)]" name="plus" />
                  {t('branches.create_and_checkout')}
                </div>
                <input
                  autoFocus
                  className="mt-3 h-9 w-full rounded-[9px] border bg-background px-2.5 text-[12px] outline-none focus:border-ring"
                  placeholder={t('input.new_branch_name')}
                  ref={input}
                  value={branchName}
                  onChange={(event) => setBranchName(event.target.value)}
                />
                <div className="mt-2 text-[10.5px] text-[var(--text-tertiary)]">{t('branches.create_hint')}</div>
              </div>
            ) : (
              <>
                <div className="h-[52px] shrink-0 px-3 pb-2 pt-2.5">
                  <label className="flex h-[34px] items-center gap-2 rounded-[9px] bg-background px-2.5 focus-within:ring-1 focus-within:ring-ring">
                    <WakuIcon className="size-[15px] text-[var(--text-secondary)]" name="search" />
                    <input
                      className="min-w-0 flex-1 bg-transparent text-[12px] outline-none"
                      placeholder={t('input.search_branches')}
                      ref={input}
                      value={query}
                      onChange={(event) => {
                        setQuery(event.target.value)
                        setActive(0)
                      }}
                    />
                  </label>
                </div>
                <div className="px-3.5 pb-1 pt-0.5 text-[12px] font-medium text-[var(--text-tertiary)]">{t('branches.title')}</div>
                <div className="min-h-0 max-h-[260px] overflow-y-auto px-1">
                  {!visible.length && (
                    <div className="grid h-16 place-items-center text-[11.5px] text-[var(--text-ghost)]">{t('branches.none_found')}</div>
                  )}
                  {visible.map((branch) => {
                    const disabledBranch = branch.checkedOutElsewhere && !plannedWorktree && branch.name !== selected
                    const actionIndex = actions.findIndex((action) => action.kind === 'branch' && action.branch.name === branch.name)
                    return (
                      <button
                        className={cn(
                          'flex h-8 w-full items-center gap-2 rounded-[6px] px-2 text-left text-[11.5px] outline-none hover:bg-accent focus-visible:bg-accent',
                          disabledBranch && 'text-[var(--text-ghost)] hover:bg-transparent',
                          actionIndex >= 0 && actionIndex === active && 'bg-accent',
                        )}
                        disabled={disabledBranch}
                        key={branch.name}
                        title={disabledBranch ? t('branches.checked_out_elsewhere') : undefined}
                        type="button"
                        onMouseEnter={() => actionIndex >= 0 && setActive(actionIndex)}
                        onClick={() => choose(branch.name)}
                      >
                        <WakuIcon className="size-3 text-[var(--text-tertiary)]" name="gitBranch" />
                        <span className="min-w-0 flex-1 truncate">{branch.name}</span>
                        {branch.name === selected && <WakuIcon className="size-[11px] text-[var(--text-tertiary)]" name="check" />}
                      </button>
                    )
                  })}
                </div>
                {!plannedWorktree && (
                  <>
                    <div className="mx-1.5 my-1 h-px bg-border" />
                    <button
                      className={cn(
                        'mx-1 mb-1 flex h-8 items-center gap-2 rounded-[6px] px-2 text-left text-[11.5px] outline-none hover:bg-accent focus-visible:bg-accent',
                        actions[active]?.kind === 'create' && 'bg-accent',
                      )}
                      type="button"
                      onMouseEnter={() => setActive(Math.max(0, actions.length - 1))}
                      onClick={() => {
                        setMode('create')
                        requestAnimationFrame(() => input.current?.focus())
                      }}
                    >
                      <WakuIcon className="size-3 text-[var(--text-secondary)]" name="plus" />
                      {t('branches.create_and_checkout_ellipsis')}
                    </button>
                  </>
                )}
              </>
            )}
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  )
}

function UsageMeter({
  provider,
  contextUsage,
  openSignal,
  onOpenSignalHandled,
  returnFocus,
}: {
  provider: string
  contextUsage?: T.ContextUsage
  openSignal?: number
  onOpenSignalHandled?: () => void
  returnFocus: RefObject<HTMLElement | null>
}) {
  const { locale, t } = useI18n()
  const { connection, config, phase } = useDaemon()
  const providers = useProviderStatuses()
  const [open, setOpen] = useState(false)
  const usageShortcut = usePrimaryShortcut('⌘U', 'Ctrl+U')
  const context = contextUsage
  const contextPercent = context?.window
    ? context.tokens * 100 / context.window
    : null
  const supportsPlanUsage = providers.byId.get(provider)?.planUsage ?? false
  const pushed = providers.planUsage.get(provider)
  const plan = useQuery({
    queryKey: daemonKeys.planUsage(config?.address ?? 'disconnected', provider),
    queryFn: () => fetchPlanUsage(connection!, provider),
    enabled: open && phase === 'connected' && Boolean(connection && config && supportsPlanUsage),
    staleTime: 30_000,
  })
  // A fetch publishes `planUsageUpdated` to every client; prefer the newest push.
  const planData = pushed !== undefined ? pushed : plan.data

  useEffect(() => {
    if (!openSignal) return
    setOpen((current) => !current)
    onOpenSignalHandled?.()
  }, [openSignal, onOpenSignalHandled])

  const error = plan.error ? errorMessage(plan.error) : null
  const tooltip = error
    ? t('usage.refresh_failed', { error })
    : contextPercent == null
      ? t('usage.shortcut', { shortcut: usageShortcut })
      : t('usage.context_used', { percent: contextPercent.toFixed(0), shortcut: usageShortcut })

  return (
    <Popover.Root modal={false} open={open} onOpenChange={setOpen}>
      <Popover.Trigger
        aria-label={tooltip}
        className={cn(
          'grid h-5 w-[23px] place-items-center rounded-[5px] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring',
          open && 'bg-accent',
        )}
        title={tooltip}
      >
        <ContextGauge percent={contextPercent} />
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner
          align="end"
          className="z-[100] outline-none"
          collisionPadding={8}
          side="top"
          sideOffset={4}
        >
          <Popover.Popup
            aria-label={t('settings.usage')}
            className="waku-popover-surface flex w-80 flex-col gap-3 rounded-[10px] p-3.5 text-xs text-popover-foreground outline-none"
            finalFocus={(closeType) => closeType === 'keyboard' ? true : returnFocus.current}
            initialFocus={false}
            role="dialog"
          >
            <UsageLane
              label={t('usage.context_window')}
              percent={contextPercent ?? 0}
              value={context?.window && contextPercent != null
                ? `${formatTokens(context.tokens)} / ${formatTokens(context.window)} (${contextPercent.toFixed(0)}%)`
                : formatTokens(context?.tokens ?? 0)}
            />
            {(planData || plan.isFetching || error) && <div className="h-px bg-border" />}
            {planData && <PlanUsageLanes locale={locale} plan={planData} provider={provider} t={t} />}
            {plan.isFetching && !planData && supportsPlanUsage && <UsageSkeleton t={t} />}
            {error && (
              <div className="space-y-1 text-[11px]">
                <div className="text-[var(--text-tertiary)]">{t('usage.plan_limits')}</div>
                <div className="break-words text-[var(--text-secondary)]">{t('usage.unavailable', { error })}</div>
              </div>
            )}
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  )
}

function ContextGauge({ percent }: { percent: number | null }) {
  const radius = 5.25
  const circumference = 2 * Math.PI * radius
  const bounded = percent == null ? 0 : Math.min(100, Math.max(5, percent))
  const color = percent != null && percent >= 95
    ? 'var(--destructive)'
    : percent != null && percent >= 80
      ? 'var(--warning)'
      : 'var(--text-secondary)'
  return (
    <svg aria-hidden="true" className="size-[13px] -rotate-90" viewBox="0 0 13 13">
      <circle
        className="text-input"
        cx="6.5"
        cy="6.5"
        fill="none"
        r={radius}
        stroke="currentColor"
        strokeWidth="2.5"
      />
      {percent != null && (
        <circle
          cx="6.5"
          cy="6.5"
          fill="none"
          r={radius}
          stroke={color}
          strokeDasharray={circumference}
          strokeDashoffset={circumference * (1 - bounded / 100)}
          strokeLinecap="round"
          strokeWidth="2.5"
        />
      )}
    </svg>
  )
}

function PlanUsageLanes({
  plan,
  provider,
  locale,
  t,
}: {
  plan: T.PlanUsage
  provider: string
  locale: string
  t: Translator
}) {
  const usageUrl = provider === 'claude'
    ? 'https://claude.ai/settings/usage'
    : provider === 'codex'
      ? 'https://chatgpt.com/codex/settings/usage'
      : null
  const header = plan.planLabel
    ? t('usage.plan_limits_named', { plan: plan.planLabel })
    : t('usage.plan_limits')
  return (
    <>
      {usageUrl ? (
        <a
          className="flex min-w-0 items-center gap-1 text-[11px] text-[var(--text-tertiary)] outline-none hover:text-[var(--text-secondary)] focus-visible:ring-1 focus-visible:ring-ring"
          href={usageUrl}
          rel="noreferrer"
          target="_blank"
        >
          <span className="min-w-0 flex-1 truncate">{header}</span>
          <WakuIcon className="size-2.5" name="arrowRight" />
        </a>
      ) : (
        <div className="truncate text-[11px] text-[var(--text-tertiary)]">{header}</div>
      )}
      {plan.windows.map((window) => (
        <UsageLane
          key={`${window.label}-${window.resetsAt ?? 'none'}`}
          label={window.label}
          percent={window.percent}
          reset={window.resetsAt == null ? undefined : resetLabel(window.resetsAt, locale, t)}
          value={`${window.percent.toFixed(0)}%`}
        />
      ))}
    </>
  )
}

function UsageLane({
  label,
  percent,
  reset,
  value,
}: {
  label: string
  percent: number
  reset?: string
  value: string
}) {
  return (
    <div className="space-y-[7px]">
      <div className="flex min-w-0 items-center gap-2">
        <span className="min-w-0 flex-1 truncate">{label}</span>
        {reset && <span className="shrink-0 text-[11px] text-[var(--text-tertiary)]">{reset}</span>}
        <span className="shrink-0 text-[11px] text-[var(--text-secondary)]">{value}</span>
      </div>
      <div className="h-[3px] overflow-hidden rounded-full bg-accent">
        <div
          className={cn(
            'h-full rounded-full bg-[var(--text-secondary)]',
            percent >= 80 && 'bg-[var(--warning)]',
            percent >= 95 && 'bg-destructive',
          )}
          style={{ width: `${Math.min(100, Math.max(0, percent))}%` }}
        />
      </div>
    </div>
  )
}

function UsageSkeleton({ t }: { t: Translator }) {
  return (
    <div aria-label={t('usage.plan_limits')} className="space-y-3" role="status">
      <div className="h-2.5 w-24 rounded bg-accent motion-safe:animate-pulse" />
      {[0, 1].map((row) => (
        <div className="space-y-2" key={row}>
          <div className="flex justify-between">
            <div className="h-2.5 w-20 rounded bg-accent motion-safe:animate-pulse" />
            <div className="h-2.5 w-10 rounded bg-accent motion-safe:animate-pulse" />
          </div>
          <div className="h-[3px] rounded-full bg-accent motion-safe:animate-pulse" />
        </div>
      ))}
    </div>
  )
}

/** `resetsAt` is epoch milliseconds. */
function resetLabel(resetsAt: number, locale: string, t: Translator) {
  const seconds = Math.floor((resetsAt - Date.now()) / 1_000)
  if (seconds <= 0) return t('usage.resets_soon')
  const minutes = Math.ceil(seconds / 60)
  if (minutes < 60) return t('usage.resets_in_minutes', { count: minutes })
  if (minutes < 24 * 60) {
    const hours = Math.floor(minutes / 60)
    const remainder = minutes % 60
    return remainder
      ? t('usage.resets_in_hours_minutes', { hours, minutes: remainder })
      : t('usage.resets_in_hours', { count: hours })
  }
  return t('usage.resets_date', { date: new Intl.DateTimeFormat(locale, {
    weekday: 'short',
    hour: 'numeric',
    minute: '2-digit',
  }).format(new Date(resetsAt)) })
}

function formatTokens(tokens: number) {
  if (tokens >= 999_500) return `${(tokens / 1_000_000).toFixed(1)}M`
  return tokens >= 1_000 ? `${(tokens / 1_000).toFixed(1)}k` : `${tokens}`
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}
