import { useNavigate, useSearch } from '@tanstack/react-router'
import { Popover } from '@base-ui/react/popover'
import type * as T from '@waku/client/v8'
import { useEffect, useRef, useState } from 'react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  CommandPalette,
  type CommandPaletteActions,
  type CommandPaletteView,
} from '@/components/command-palette'
import { CommitDialog } from '@/components/commit-dialog'
import { Composer } from '@/components/composer'
import { ControlMenu } from '@/components/control-menu'
import { DaemonFilePicker } from '@/components/daemon-file-picker'
import { RightPanel, type PanelSurface } from '@/components/right-panel'
import { Sidebar } from '@/components/sidebar'
import { StartupScreen } from '@/components/startup-screen'
import type { SettingsPageId } from '@/components/settings-view'
import { Transcript } from '@/components/transcript'
import { WakuIcon } from '@/components/waku-icon'
import { useWorkspaceBranches } from '@/hooks/use-daemon-data'
import { useDraftsStream, useShell, useTask } from '@/hooks/use-daemon-stores'
import { useDocumentTitle } from '@/hooks/use-document-title'
import {
  projectRoot,
  selectableProjects,
  taskCwd,
  threadRoot,
  type DiffSource,
} from '@/lib/daemon-api'
import {
  composerDraftFor,
  composerDraftId,
  moveComposerDraftToEmpty,
  setComposerDraft,
  type ComposerDraftValue,
  type DraftTarget,
} from '@/lib/composer-drafts'
import { useDaemon } from '@/lib/daemon-context'
import { useI18n } from '@/lib/i18n'
import {
  browserComposerPreferenceStorage,
  readComposerPreferences,
  rememberedSelection,
} from '@/lib/composer-preferences'
import {
  browserNavigationStorage,
  readRememberedNavigation,
  routeDestinationTransition,
  taskRemovalDestination,
  writeRememberedNavigation,
  type RememberedNavigation,
} from '@/lib/navigation-memory'
import { transcriptLinkRoute } from '@/lib/transcript-links'
import { shouldShowInitialDestination } from '@/lib/workspace-presentation'
import { agentPresetIdLabel } from '@/lib/agent-preset-presentation'
import { isProjectlessProject, projectDisplayName } from '@/lib/project-presentation'
import { sessionHasStarted } from '@/lib/sidebar-presentation'
import { useTaskActions } from '@/lib/task-actions'
import {
  displayTitle,
  isBusy,
  type BackgroundWorkItem,
  type BackgroundWorkStatus,
  type NewTaskDraft,
  type Task,
} from '@/lib/thread-model'

/** What a right panel works on: a thread, or the project of a task being composed. */
interface PanelSubject {
  key: string
  root: T.RootRef
  cwd?: string
  task?: Task
  selection?: T.ModelSelection
}

type Translator = (key: string, params?: Record<string, string | number>) => string

export function WakuApp() {
  const { t } = useI18n()
  const navigate = useNavigate({ from: '/' })
  const search = useSearch({ from: '/' })
  const { config, stores } = useDaemon()
  const actions = useTaskActions()
  const shell = useShell()
  const drafts = useDraftsStream()
  const selected = useTask(search.session)
  const [mobileSidebar, setMobileSidebar] = useState(false)
  const [sidebarVisible, setSidebarVisible] = useState(readSidebarVisible)
  const [sidebarWidth, setSidebarWidth] = useState(readSidebarWidth)
  const [rightPanelWidth, setRightPanelWidth] = useState(readRightPanelWidth)
  const [rightPanelOpenBySession, setRightPanelOpenBySession] = useState<Record<string, boolean>>({})
  const retainedPanels = useRef(new Map<string, PanelSubject>())
  const [retainedPanelIds, setRetainedPanelIds] = useState<string[]>([])
  const [paletteOpen, setPaletteOpen] = useState(false)
  const [paletteInitialView, setPaletteInitialView] = useState<CommandPaletteView>('commands')
  const [focusComposerSignal, setFocusComposerSignal] = useState(0)
  const [modelPickerSignal, setModelPickerSignal] = useState(0)
  const [usagePanelSignal, setUsagePanelSignal] = useState(0)
  const [composerPrefill, setComposerPrefill] = useState<{
    sessionId: string
    text: string
    signal: number
  } | null>(null)
  const [requestedPanel, setRequestedPanel] = useState<PanelSurface>('files')
  const [requestedFile, setRequestedFile] = useState<string | null>(null)
  const [requestedDiffSource, setRequestedDiffSource] = useState<DiffSource>('uncommitted')
  const [requestedBackgroundWorkId, setRequestedBackgroundWorkId] = useState<string | null>(null)
  const [panelRequestSessionId, setPanelRequestSessionId] = useState<string | null>(null)
  const [panelRequestSignal, setPanelRequestSignal] = useState(0)
  const [commitDialogOpen, setCommitDialogOpen] = useState(false)
  const commitDialogReturnFocus = useRef<HTMLElement | null>(null)
  const [projectPickerOpen, setProjectPickerOpen] = useState(false)
  const projectPickerReturnFocus = useRef<HTMLElement | null>(null)
  const [projectlessPending, setProjectlessPending] = useState(false)
  const projectlessRequest = useRef(false)
  const composerDraftState = useRef<T.ComposerDrafts>({})
  const composerDraftTimers = useRef(new Map<string, number>())
  const composerDraftWriteQueue = useRef<Promise<void>>(Promise.resolve())
  const [composerDraftsLoaded, setComposerDraftsLoaded] = useState(false)
  const rememberedNavigation = useRef(config
    ? readRememberedNavigation(browserNavigationStorage(), config.address)
    : null)
  const previousRouteSession = useRef(search.session)
  const pendingPaletteFocusSession = useRef<string | null>(null)
  const presentedWorkspaceFor = useRef<string | null>(null)
  const enteringNewTask = useRef(false)
  const [newTaskMode, setNewTaskMode] = useState(
    () => !search.session && rememberedNavigation.current?.kind === 'newTask',
  )
  const [draft, setDraft] = useState<NewTaskDraft | null>(null)
  const projects = shell.projects
  const tasks = shell.tasks
  const draftProject = draft ? projects.find((project) => project.id === draft.projectId) : undefined
  const current = newTaskMode ? undefined : selected.task
  useDocumentTitle(newTaskMode ? t('menu.new_task') : current ? displayTitle(current) : null)
  const currentProject = current
    ? projects.find((project) => project.id === current.projectId)
    : undefined
  const activeProject = currentProject ?? draftProject
  const activeSubject: PanelSubject | null = current
    ? {
        key: current.id,
        root: threadRoot(current.id),
        cwd: taskCwd(current, currentProject),
        task: current,
        selection: current.selection,
      }
    : draft && draftProject
      ? {
          key: draft.id,
          root: projectRoot(draftProject.id),
          cwd: draftProject.path,
          selection: draft.selection,
        }
      : null
  const rightPanelVisible = activeSubject
    ? rightPanelOpenBySession[activeSubject.key] ?? false
    : false
  const draftsSettled = drafts.ready || drafts.store?.status === 'ended'
  const choosingInitialDestination = Boolean(shell.ready && !search.session && !newTaskMode)
  const restoringNewTask = Boolean(
    shell.ready && newTaskMode && projects.length && (!draft || !draftProject),
  )
  const hydratingInitialSession = Boolean(
    search.session && !newTaskMode && !selected.task && selected.loading,
  )
  const hasPresentedWorkspace = Boolean(
    config && presentedWorkspaceFor.current === config.address,
  )
  const showingInitialDestination = shouldShowInitialDestination(
    hasPresentedWorkspace,
    {
      choosing: choosingInitialDestination,
      restoringNewTask,
      hydratingSession: hydratingInitialSession,
    },
  )

  useEffect(() => {
    const preload = () => {
      void import('@/components/code-surfaces')
        .then((module) => module.preloadCodeSurfaces())
        .catch(() => {
          // Code surfaces retain their own visible error handling if an
          // optional syntax-highlighting chunk cannot be warmed in advance.
        })
    }
    if (typeof window.requestIdleCallback === 'function') {
      const idle = window.requestIdleCallback(preload, { timeout: 2_000 })
      return () => window.cancelIdleCallback(idle)
    }
    const timer = globalThis.setTimeout(preload, 750)
    return () => globalThis.clearTimeout(timer)
  }, [])

  useEffect(() => {
    if (!config || !shell.ready || !composerDraftsLoaded) return
    if (showingInitialDestination) return
    presentedWorkspaceFor.current = config.address
  }, [composerDraftsLoaded, config, shell.ready, showingInitialDestination])

  useEffect(() => {
    const transition = routeDestinationTransition(
      previousRouteSession.current,
      search.session,
      newTaskMode,
    )
    previousRouteSession.current = search.session
    if (transition === 'session') {
      enteringNewTask.current = false
      return
    }
    if (transition !== 'newTask') return

    enteringNewTask.current = true
    const project = currentProject ?? draftProject ?? projects[0]
    setNewTaskMode(true)
    setDraft(project ? createRememberedDraft(project.id) : null)
    rememberNavigation({ kind: 'newTask', projectId: project?.id })
  }, [search.session])

  useEffect(() => {
    if (!search.session && newTaskMode) enteringNewTask.current = false
  }, [newTaskMode, search.session])

  useEffect(() => {
    // A click from New Task keeps that surface mounted while the thread's
    // snapshot arrives; switch once it has.
    if (search.session && selected.task?.id === search.session && newTaskMode && !enteringNewTask.current) {
      setNewTaskMode(false)
      setDraft(null)
    }
  }, [newTaskMode, search.session, selected.task?.id])

  useEffect(() => {
    if (!current || pendingPaletteFocusSession.current !== current.id) return
    pendingPaletteFocusSession.current = null
    setFocusComposerSignal((value) => value + 1)
  }, [current?.id])

  // The composer keeps its own copy of the drafts; local edits are sent to
  // the daemon a moment after typing stops.
  useEffect(() => {
    if (composerDraftsLoaded || !draftsSettled) return
    composerDraftState.current = structuredClone(drafts.drafts)
    setComposerDraftsLoaded(true)
  }, [composerDraftsLoaded, drafts.drafts, draftsSettled])

  useEffect(() => () => {
    for (const timer of new Set(composerDraftTimers.current.values())) {
      window.clearTimeout(timer)
    }
    composerDraftTimers.current.clear()
  }, [])

  useEffect(() => {
    window.localStorage.setItem('waku.sidebarVisible', String(sidebarVisible))
  }, [sidebarVisible])

  useEffect(() => {
    const timer = window.setTimeout(() => {
      window.localStorage.setItem('waku.sidebarWidth', String(Math.round(sidebarWidth)))
    }, 150)
    return () => window.clearTimeout(timer)
  }, [sidebarWidth])

  useEffect(() => {
    const timer = window.setTimeout(() => {
      window.localStorage.setItem('waku.rightPanelWidth', String(Math.round(rightPanelWidth)))
    }, 150)
    return () => window.clearTimeout(timer)
  }, [rightPanelWidth])

  useEffect(() => {
    if (!activeSubject || !retainedPanelIds.includes(activeSubject.key)) return
    retainedPanels.current.set(activeSubject.key, activeSubject)
  })

  useEffect(() => {
    if (!shell.ready) return
    if (search.session) return
    if (enteringNewTask.current && !newTaskMode) return
    if (newTaskMode) {
      const rememberedProjectId = rememberedNavigation.current?.kind === 'newTask'
        ? rememberedNavigation.current.projectId
        : undefined
      const project = projects.find((item) => item.id === rememberedProjectId) ?? projects[0]
      if (!project) {
        setDraft(null)
        return
      }
      setDraft((currentDraft) => currentDraft?.projectId === project.id
        ? currentDraft
        : createRememberedDraft(project.id))
      return
    }
    const remembered = rememberedNavigation.current
    const rememberedTask = remembered?.kind === 'session'
      ? tasks.find((task) => task.id === remembered.sessionId)
      : undefined
    const newest = rememberedTask ?? [...tasks]
      .filter(sessionHasStarted)
      .sort((a, b) => (b.lastReplyAt ?? b.createdAt) - (a.lastReplyAt ?? a.createdAt))[0]
    if (newest) void navigate({ search: { session: newest.id }, replace: true })
    else startNewTask(projects[0])
  }, [shell.ready, projects, tasks, search.session, newTaskMode, navigate])

  useEffect(() => {
    if (!config || !search.session) return
    rememberNavigation({ kind: 'session', sessionId: search.session })
  }, [config, search.session])

  useEffect(() => {
    const keyDown = (event: KeyboardEvent) => {
      if (!(event.metaKey || event.ctrlKey)) return
      const key = event.key.toLowerCase()
      if (key === 'k') {
        event.preventDefault()
        if (paletteOpen) setPaletteOpen(false)
        else openCommandPalette('commands')
        return
      }
      if (key === ',') {
        event.preventDefault()
        openSettings('general')
        return
      }
      if (key === 'b' && event.shiftKey) {
        event.preventDefault()
        toggleRightPanel()
        return
      }
      if (key === 'b') {
        event.preventDefault()
        setSidebarVisible((value) => !value)
        return
      }
      if (!shell.ready) return
      if (key === 'n') {
        event.preventDefault()
        startNewTask()
      } else if (key === 'o') {
        event.preventDefault()
        openProjectPicker()
      } else if (key === 'l') {
        event.preventDefault()
        setFocusComposerSignal((value) => value + 1)
      } else if (key === '/') {
        event.preventDefault()
        setModelPickerSignal((value) => value + 1)
      } else if (key === 'u') {
        event.preventDefault()
        setUsagePanelSignal((value) => value + 1)
      }
    }
    window.addEventListener('keydown', keyDown)
    return () => window.removeEventListener('keydown', keyDown)
  })

  if (!shell.ready || !composerDraftsLoaded) {
    const startupError = shell.error
    return <StartupScreen error={startupError ? errorMessage(startupError) : undefined} />
  }

  if (showingInitialDestination) {
    return <StartupScreen />
  }

  if (search.session && !newTaskMode && !selected.task && selected.error) {
    return (
      <StartupScreen
        error={errorMessage(selected.error)}
        onRetry={() => startNewTask()}
      />
    )
  }

  function openProjectPicker() {
    projectPickerReturnFocus.current = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null
    setProjectPickerOpen(true)
  }

  function openCommandPalette(view: CommandPaletteView = 'commands') {
    setPaletteInitialView(view)
    setPaletteOpen(true)
  }

  function rememberNavigation(navigation: RememberedNavigation) {
    if (!config) return
    rememberedNavigation.current = navigation
    writeRememberedNavigation(browserNavigationStorage(), config.address, navigation)
  }

  function openSettings(page: SettingsPageId) {
    void navigate({
      to: '/settings/$page',
      params: { page },
      search: { session: search.session },
    })
  }

  function showSidebar() {
    setSidebarVisible(true)
    if (window.matchMedia('(max-width: 1023px)').matches) setMobileSidebar(true)
  }

  function hideSidebar() {
    if (window.matchMedia('(max-width: 1023px)').matches) setMobileSidebar(false)
    else setSidebarVisible(false)
  }

  function openPanel(
    surface: PanelSurface,
    source: DiffSource = 'uncommitted',
    file: string | null = null,
  ) {
    if (!activeSubject) return
    retainPanel(activeSubject)
    setRequestedPanel(surface)
    setRequestedFile(surface === 'files' ? file : null)
    if (surface === 'changes') setRequestedDiffSource(source)
    setPanelRequestSessionId(activeSubject.key)
    setPanelRequestSignal((value) => value + 1)
    setRightPanelForSession(activeSubject.key, true)
  }

  function openTranscriptLink(target: string) {
    const route = transcriptLinkRoute(target, activeSubject?.cwd)
    if (route.kind === 'external') return false
    if (route.kind === 'remoteFile') {
      toast.error(t('errors.path_outside_workspace'))
      return true
    }
    openPanel('files', 'uncommitted', route.path)
    return true
  }

  function openBackgroundWork(id: string) {
    if (!activeSubject) return
    retainPanel(activeSubject)
    setRequestedBackgroundWorkId(id)
    setRequestedPanel('backgroundWork')
    setPanelRequestSessionId(activeSubject.key)
    setPanelRequestSignal((value) => value + 1)
    setRightPanelForSession(activeSubject.key, true)
  }

  function retainPanel(subject: PanelSubject) {
    retainedPanels.current.set(subject.key, subject)
    setRetainedPanelIds((currentIds) => currentIds.includes(subject.key)
      ? currentIds
      : [...currentIds, subject.key])
  }

  function setRightPanelForSession(
    key: string,
    next: boolean | ((current: boolean) => boolean),
  ) {
    setRightPanelOpenBySession((current) => {
      const previous = current[key] ?? false
      const value = typeof next === 'function' ? next(previous) : next
      return value === previous ? current : { ...current, [key]: value }
    })
  }

  function setActiveRightPanel(next: boolean | ((current: boolean) => boolean)) {
    if (!activeSubject) return
    retainPanel(activeSubject)
    setRightPanelForSession(activeSubject.key, next)
  }

  function toggleRightPanel() {
    setActiveRightPanel((value) => !value)
  }

  function forgetRightPanelSession(key: string) {
    retainedPanels.current.delete(key)
    setRetainedPanelIds((currentIds) => currentIds.filter((id) => id !== key))
    setRightPanelOpenBySession((value) => {
      if (!(key in value)) return value
      const next = { ...value }
      delete next[key]
      return next
    })
    setPanelRequestSessionId((value) => value === key ? null : value)
  }

  function startNewTask(preferred?: T.Project | null) {
    const project = preferred === null
      ? undefined
      : preferred ?? currentProject ?? draftProject ?? projects[0]
    if (newTaskMode && draft) forgetRightPanelSession(draft.id)
    enteringNewTask.current = true
    pendingPaletteFocusSession.current = null
    setFocusComposerSignal((value) => value + 1)
    setNewTaskMode(true)
    setDraft(project ? createRememberedDraft(project.id) : null)
    rememberNavigation({ kind: 'newTask', projectId: project?.id })
    void navigate({ search: { session: undefined }, replace: false })
  }

  function selectSession(sessionId: string) {
    if (sessionId === search.session && !newTaskMode) return
    if (newTaskMode && draft && draft.id !== sessionId) forgetRightPanelSession(draft.id)
    enteringNewTask.current = false
    if (!newTaskMode) setDraft(null)
    rememberNavigation({ kind: 'session', sessionId })
    void navigate({ search: { session: sessionId } })
  }

  async function resumeProviderSession(summary: T.ProviderSessionSummary) {
    const project = projects.find((candidate) => candidate.path === summary.cwd)
      ?? await actions.addProject(summary.cwd)
    const threadId = await actions.importSession(
      summary,
      project.id,
      current?.runtimeMode ?? draft?.runtimeMode ?? 'full_access',
    )
    pendingPaletteFocusSession.current = threadId
    selectSession(threadId)
  }

  async function forkResponse(task: Task, runId: string) {
    try {
      const threadId = await actions.forkFromRun(task, runId)
      selectSession(threadId)
      toast.success(t('session.forked_from_response'))
    } catch (error) {
      toast.error(t('errors.fork_task', { error: errorMessage(error) }))
    }
  }

  async function rewindMessage(
    task: Task,
    runId: string,
    prompt: string,
    attachments: T.Attachment[],
  ) {
    const ordinal = task.runs.find((run) => run.id === runId)?.ordinal ?? 0
    try {
      await actions.rewindToRun(task, runId, { text: prompt, attachments })
      toast(t('session.rewound', { turn: ordinal }))
    } catch (error) {
      toast.error(t('errors.rewind_task', { error: errorMessage(error) }))
      throw error
    }
  }

  async function renameSession(sessionId: string, title: string) {
    try {
      await actions.renameTask(sessionId, title)
    } catch (error) {
      toast.error(t('errors.rename_task', { error: errorMessage(error) }))
      throw error
    }
  }

  /** After a task leaves the list, go where the desktop would. */
  async function leaveTask(removedId: string) {
    const removed = tasks.find((task) => task.id === removedId)
    removeStoredComposerDraft({ type: 'thread', threadId: removedId })
    forgetRightPanelSession(removedId)
    if (search.session !== removedId) return
    if (!removed) {
      startNewTask(null)
      return
    }
    const destination = taskRemovalDestination(
      projects,
      projects,
      tasks.filter((task) => task.id !== removedId),
      removed,
    )
    if (destination.kind === 'session') selectSession(destination.sessionId)
    else if (destination.kind === 'newTask') startNewTask(destination.project)
    else if (destination.kind === 'projectless') {
      startNewTask(null)
      await createProjectlessTask()
    } else startNewTask(null)
  }

  async function removeSessionById(sessionId: string) {
    try {
      await actions.deleteTask(sessionId)
      await leaveTask(sessionId)
    } catch (error) {
      toast.error(t('errors.remove_task', { error: errorMessage(error) }))
      throw error
    }
  }

  async function archiveSessionById(sessionId: string) {
    try {
      await actions.archiveTask(sessionId)
      await leaveTask(sessionId)
    } catch (error) {
      toast.error(t('errors.archive_task', { error: errorMessage(error) }))
      throw error
    }
  }

  function chooseProject(project: T.Project) {
    if (draftProject && draftProject.id !== project.id) {
      moveStoredComposerDraft(
        { type: 'newThread', projectId: draftProject.id },
        { type: 'newThread', projectId: project.id },
      )
    }
    enteringNewTask.current = true
    setDraft((currentDraft) => currentDraft
      ? { ...currentDraft, projectId: project.id, isolated: false, baseRef: undefined }
      : createRememberedDraft(project.id))
    setNewTaskMode(true)
    rememberNavigation({ kind: 'newTask', projectId: project.id })
    void navigate({ search: { session: undefined }, replace: false })
  }

  async function addRemoteProject(path: string): Promise<boolean> {
    try {
      chooseProject(await actions.addProject(path))
      return true
    } catch (error) {
      toast.error(errorMessage(error))
      return false
    }
  }

  async function createProjectlessTask() {
    if (projectlessRequest.current) return
    projectlessRequest.current = true
    setProjectlessPending(true)
    try {
      chooseProject(await actions.projectlessProject())
    } catch (error) {
      toast.error(errorMessage(error))
    } finally {
      projectlessRequest.current = false
      setProjectlessPending(false)
    }
  }

  function createRememberedDraft(projectId: string): NewTaskDraft {
    const preferences = readComposerPreferences(
      browserComposerPreferenceStorage(),
      config?.address ?? 'disconnected',
    )
    return {
      id: crypto.randomUUID(),
      projectId,
      selection: rememberedSelection(preferences),
      runtimeMode: 'full_access',
      interactionMode: 'default',
      isolated: false,
    }
  }

  function updateStoredComposerDraft(target: DraftTarget, value: ComposerDraftValue) {
    const change = setComposerDraft(composerDraftState.current, target, value)
    if (!change) return
    const id = composerDraftId(target)
    const pending = composerDraftTimers.current.get(id)
    if (pending !== undefined) window.clearTimeout(pending)
    const timer = window.setTimeout(() => {
      if (composerDraftTimers.current.get(id) === timer) {
        composerDraftTimers.current.delete(id)
      }
      enqueueComposerDraftChanges([change])
    }, 250)
    composerDraftTimers.current.set(id, timer)
  }

  function removeStoredComposerDraft(target: DraftTarget) {
    const change = setComposerDraft(composerDraftState.current, target, { text: '', attachments: [] })
    if (!change) return
    cancelComposerDraftTimer(target)
    enqueueComposerDraftChanges([change])
  }

  function moveStoredComposerDraft(source: DraftTarget, destination: DraftTarget) {
    const changes = moveComposerDraftToEmpty(composerDraftState.current, source, destination)
    if (!changes.length) return
    cancelComposerDraftTimer(source)
    cancelComposerDraftTimer(destination)
    enqueueComposerDraftChanges(changes)
  }

  function cancelComposerDraftTimer(target: DraftTarget) {
    const id = composerDraftId(target)
    const timer = composerDraftTimers.current.get(id)
    if (timer !== undefined) window.clearTimeout(timer)
    composerDraftTimers.current.delete(id)
  }

  function enqueueComposerDraftChanges(changes: T.DraftChange[]) {
    const store = stores?.drafts
    if (!store || !changes.length) return
    composerDraftWriteQueue.current = composerDraftWriteQueue.current
      .catch(() => {})
      .then(() => store.apply(changes))
      .catch((error) => {
        toast.error(t('errors.save_draft', { error: errorMessage(error) }))
      })
  }

  const paletteActions: CommandPaletteActions = {
    newTask: () => startNewTask(),
    openProject: openProjectPicker,
    chooseModel: () => {
      setModelPickerSignal((value) => value + 1)
    },
    focusComposer: () => {
      setFocusComposerSignal((value) => value + 1)
    },
    toggleUsage: () => {
      setUsagePanelSignal((value) => value + 1)
    },
    toggleSidebar: () => setSidebarVisible((value) => !value),
    toggleRightPanel,
    openSettings,
    selectTask: (sessionId) => {
      if (!newTaskMode && current?.id === sessionId) {
        setFocusComposerSignal((value) => value + 1)
        return
      }
      pendingPaletteFocusSession.current = sessionId
      selectSession(sessionId)
    },
    resumeProviderSession,
  }

  const activeSelection = current?.selection ?? draft?.selection
  const palette = (
    <CommandPalette
      actions={paletteActions}
      canChooseModel={Boolean(current ? !isBusy(current.status) : draft)}
      canToggleUsage={Boolean(current ?? draft)}
      currentProvider={activeSelection?.instance ?? 'codex'}
      initialView={paletteInitialView}
      open={paletteOpen}
      projects={projects}
      rightPanelVisible={rightPanelVisible}
      selectedSessionId={search.session}
      sidebarVisible={sidebarVisible}
      tasks={tasks}
      onOpenChange={setPaletteOpen}
    />
  )

  const panelSubjects = (activeSubject && !retainedPanelIds.includes(activeSubject.key)
    ? [...retainedPanelIds, activeSubject.key]
    : retainedPanelIds)

  return (
    <div className="flex h-dvh w-full overflow-hidden bg-background">
      {sidebarVisible && (
        <Sidebar
          mobileOpen={mobileSidebar}
          onAddProject={openProjectPicker}
          onArchiveSession={archiveSessionById}
          onMobileOpenChange={setMobileSidebar}
          onNewTask={() => startNewTask()}
          onRemoveSession={removeSessionById}
          onRenameSession={renameSession}
          onSearch={() => openCommandPalette('commands')}
          onSelectSession={selectSession}
          onSettings={() => openSettings('general')}
          onToggleSidebar={hideSidebar}
          onWidthChange={setSidebarWidth}
          projects={projects}
          selectedSessionId={search.session}
          tasks={tasks}
          width={sidebarWidth}
        />
      )}

      <main className="relative flex min-w-0 flex-1 flex-col">
        <TaskHeader
          onCommit={(returnFocus) => {
            commitDialogReturnFocus.current = returnFocus
            setCommitDialogOpen(true)
          }}
          onCompareBranch={() => openPanel('changes', 'branch')}
          onOpenBackgroundWork={openBackgroundWork}
          onMenu={showSidebar}
          onOpenChanges={() => openPanel('changes', 'uncommitted')}
          onTogglePanel={toggleRightPanel}
          root={activeSubject?.root}
          task={current}
          sidebarVisible={sidebarVisible}
          title={newTaskMode ? t('menu.new_task') : current ? displayTitle(current) : 'Waku'}
        />

        {newTaskMode ? (
          draft && draftProject ? (
            <>
              <NewTaskCanvas
                project={draftProject}
                projects={projects}
                onAddProject={openProjectPicker}
                onProject={(project) => chooseProject(project)}
                onProjectless={() => void createProjectlessTask()}
              />
              <Composer
                focusSignal={focusComposerSignal}
                initialComposerDraft={composerDraftFor(composerDraftState.current, {
                  type: 'newThread',
                  projectId: draftProject.id,
                })}
                key={composerDraftId({ type: 'newThread', projectId: draftProject.id })}
                modelPickerSignal={modelPickerSignal}
                newTask={draft}
                onAddProject={openProjectPicker}
                onFocusSignalHandled={() => setFocusComposerSignal(0)}
                onModelPickerSignalHandled={() => setModelPickerSignal(0)}
                onProjectless={() => void createProjectlessTask()}
                onResume={() => openCommandPalette('resume')}
                onUsagePanelSignalHandled={() => setUsagePanelSignal(0)}
                project={draftProject}
                projects={selectableProjects(projects, draftProject)}
                usagePanelSignal={usagePanelSignal}
                onComposerDraftChange={(value) => updateStoredComposerDraft(
                  { type: 'newThread', projectId: draftProject.id },
                  value,
                )}
                onComposerDraftSubmitted={() => removeStoredComposerDraft({
                  type: 'newThread',
                  projectId: draftProject.id,
                })}
                onActivated={(threadId) => {
                  forgetRightPanelSession(draft.id)
                  enteringNewTask.current = false
                  rememberNavigation({ kind: 'session', sessionId: threadId })
                  void navigate({ search: { session: threadId } })
                }}
                onNewTaskChange={(next) => {
                  if (next.projectId !== draft.projectId) {
                    moveStoredComposerDraft(
                      { type: 'newThread', projectId: draft.projectId },
                      { type: 'newThread', projectId: next.projectId },
                    )
                  }
                  setDraft(next)
                }}
              />
            </>
          ) : (
            <NoProjectState
              projectlessPending={projectlessPending}
              onAddProject={openProjectPicker}
              onProjectless={() => void createProjectlessTask()}
            />
          )
        ) : current ? (
          <>
            <Transcript
              forkingRunId={actions.forking[current.id]}
              loadingOlder={selected.store?.loadingHistory ?? false}
              rewindingRunId={actions.rewinding[current.id]}
              task={current}
              onCopyToComposer={(text) => setComposerPrefill((previous) => ({
                sessionId: current.id,
                text,
                signal: (previous?.signal ?? 0) + 1,
              }))}
              onForkResponse={(runId) => void forkResponse(current, runId)}
              onLoadOlder={() => {
                void selected.store?.loadOlder().catch((error) => toast.error(errorMessage(error)))
              }}
              onOpenBackgroundWork={openBackgroundWork}
              onOpenLink={openTranscriptLink}
              onReviewChanges={(source) => openPanel('changes', source)}
              onRewindMessage={(runId, prompt, attachments) =>
                rewindMessage(current, runId, prompt, attachments)}
            />
            {currentProject && (
              <div aria-busy={!current.synchronized} className="shrink-0">
                <Composer
                  focusSignal={focusComposerSignal}
                  initialComposerDraft={composerDraftFor(composerDraftState.current, {
                    type: 'thread',
                    threadId: current.id,
                  })}
                  key={composerDraftId({ type: 'thread', threadId: current.id })}
                  modelPickerSignal={modelPickerSignal}
                  prefillSignal={composerPrefill?.sessionId === current.id ? composerPrefill.signal : 0}
                  prefillText={composerPrefill?.sessionId === current.id ? composerPrefill.text : undefined}
                  project={currentProject}
                  projects={projects}
                  task={current}
                  usagePanelSignal={usagePanelSignal}
                  onAddProject={openProjectPicker}
                  onFocusSignalHandled={() => setFocusComposerSignal(0)}
                  onModelPickerSignalHandled={() => setModelPickerSignal(0)}
                  onPrefillSignalHandled={() => setComposerPrefill((value) =>
                    value?.sessionId === current.id ? null : value)}
                  onProjectless={() => void createProjectlessTask()}
                  onResume={() => openCommandPalette('resume')}
                  onUsagePanelSignalHandled={() => setUsagePanelSignal(0)}
                  onComposerDraftChange={(value) => updateStoredComposerDraft(
                    { type: 'thread', threadId: current.id },
                    value,
                  )}
                  onComposerDraftSubmitted={() => removeStoredComposerDraft({
                    type: 'thread',
                    threadId: current.id,
                  })}
                />
              </div>
            )}
            {selected.error && (
              <div role="alert" className="absolute left-1/2 top-14 z-20 -translate-x-1/2 rounded-lg bg-destructive px-3 py-2 text-xs text-white shadow-lg">
                {errorMessage(selected.error)}
              </div>
            )}
          </>
        ) : search.session && selected.loading ? (
          <div className="min-h-0 flex-1" />
        ) : (
          <NoProjectState
            projectlessPending={projectlessPending}
            onAddProject={openProjectPicker}
            onProjectless={() => void createProjectlessTask()}
          />
        )}
      </main>

      {panelSubjects.map((key) => {
        const isActive = activeSubject?.key === key
        const subject = isActive ? activeSubject : retainedPanels.current.get(key)
        if (!subject) return null
        return (
          <RightPanel
            active={isActive}
            cwd={subject.cwd}
            key={key}
            open={isActive && (rightPanelOpenBySession[key] ?? false)}
            panelKey={key}
            panelWidth={rightPanelWidth}
            requestedBackgroundWorkId={requestedBackgroundWorkId}
            requestedDiffSource={requestedDiffSource}
            requestedFile={requestedFile}
            requestedSurface={requestedPanel}
            requestSignal={panelRequestSessionId === key ? panelRequestSignal : 0}
            root={subject.root}
            sidebarWidth={sidebarVisible ? sidebarWidth : 0}
            task={subject.task}
            onOpenChange={(open) => setRightPanelForSession(key, open)}
            onPanelWidthChange={setRightPanelWidth}
          />
        )
      })}

      <CommitDialog
        key={`${config?.address}:${activeSubject?.key}`}
        open={commitDialogOpen}
        returnFocus={commitDialogReturnFocus}
        root={activeSubject?.root}
        selection={activeSubject?.selection}
        onOpenChange={setCommitDialogOpen}
      />

      {projectPickerOpen && (
        <DaemonFilePicker
          root={null}
          returnFocus={projectPickerReturnFocus}
          selectionMode="directory"
          onClose={() => setProjectPickerOpen(false)}
          onSelect={addRemoteProject}
        />
      )}
      {palette}
    </div>
  )
}

function readSidebarWidth(): number {
  if (typeof window === 'undefined') return 252
  const raw = window.localStorage.getItem('waku.sidebarWidth')
  if (raw === null) return 252
  const stored = Number(raw)
  return Number.isFinite(stored) ? Math.min(420, Math.max(180, stored)) : 252
}

function readSidebarVisible(): boolean {
  if (typeof window === 'undefined') return true
  return window.localStorage.getItem('waku.sidebarVisible') !== 'false'
}

function readRightPanelWidth(): number {
  if (typeof window === 'undefined') return 460
  const raw = window.localStorage.getItem('waku.rightPanelWidth')
  if (raw === null) return 460
  const stored = Number(raw)
  return Number.isFinite(stored) ? Math.min(1_000, Math.max(280, stored)) : 460
}

function TaskHeader({
  title,
  task,
  root,
  onMenu,
  onOpenChanges,
  onCommit,
  onCompareBranch,
  onOpenBackgroundWork,
  onTogglePanel,
  sidebarVisible,
}: {
  title: string
  task?: Task
  root?: T.RootRef
  onMenu: () => void
  onOpenChanges: () => void
  onCommit: (returnFocus: HTMLElement | null) => void
  onCompareBranch: () => void
  onOpenBackgroundWork: (id: string) => void
  onTogglePanel: () => void
  sidebarVisible: boolean
}) {
  const { t } = useI18n()
  const branches = useWorkspaceBranches(root)
  const preset = task?.selection.instance === 'deepseek' && task.runs.length
    ? agentPresetIdLabel(task.selection.agentPreset || 'standard', t)
    : null
  return (
    <header className="flex h-12 shrink-0 items-center gap-2 px-3 lg:px-3.5">
      <Button
        aria-label={t('sidebar.open')}
        className={sidebarVisible ? 'lg:hidden' : undefined}
        size="icon-sm"
        variant="ghost"
        onClick={onMenu}
      >
        <WakuIcon name="panelLeft" />
      </Button>
      <h1 className="min-w-0 truncate text-[13px] font-medium">{title}</h1>
      {preset && (
        <span className="max-w-44 truncate rounded-md bg-accent px-1.5 py-1 text-[11px] font-medium text-[var(--text-secondary)]">
          {preset}
        </span>
      )}
      <div className="flex-1" />
      {branches.data && (branches.data.additions > 0 || branches.data.deletions > 0) && (
        <button
          className="flex items-center gap-1 rounded px-1.5 py-1 text-[11px] hover:bg-accent"
          type="button"
          onClick={onOpenChanges}
        >
          <span className="text-[var(--success)]">+{branches.data.additions}</span>
          <span className="text-destructive">-{branches.data.deletions}</span>
        </button>
      )}
      {task && (
        <EnvironmentPopover
          task={task}
          onCommit={onCommit}
          onCompareBranch={onCompareBranch}
          onOpenBackgroundWork={onOpenBackgroundWork}
        />
      )}
      <Button aria-label={t('right_panel.toggle')} size="icon-sm" variant="ghost" onClick={onTogglePanel}>
        <WakuIcon name="panelRight" />
      </Button>
    </header>
  )
}

function EnvironmentPopover({
  task,
  onCommit,
  onCompareBranch,
  onOpenBackgroundWork,
}: {
  task: Task
  onCommit: (returnFocus: HTMLElement | null) => void
  onCompareBranch: () => void
  onOpenBackgroundWork: (id: string) => void
}) {
  const { t } = useI18n()
  const { stopBackgroundWork } = useTaskActions()
  const [open, setOpen] = useState(false)
  const popup = useRef<HTMLDivElement>(null)
  const trigger = useRef<HTMLButtonElement>(null)
  function focusTrigger() {
    requestAnimationFrame(() => trigger.current?.focus())
  }

  const items = [...task.backgroundWork].reverse()
  const processes = items.filter((item) => item.kind !== 'subagent')
  const agents = items.filter((item) => item.kind === 'subagent')
  const hasBackgroundWork = processes.length > 0 || agents.length > 0
  const stop = (item: BackgroundWorkItem) => {
    if (!item.canStop) return
    void stopBackgroundWork(task.id, item).catch((error) => toast.error(errorMessage(error)))
  }

  function focusMenuItem(position: 'first' | 'last') {
    requestAnimationFrame(() => {
      const menuItems = popup.current?.querySelectorAll<HTMLElement>('[role="menuitem"]')
      const target = position === 'first' ? menuItems?.[0] : menuItems?.[Math.max(0, (menuItems?.length ?? 1) - 1)]
      target?.focus()
    })
  }

  return (
    <Popover.Root modal={false} open={open} onOpenChange={setOpen}>
      <Popover.Trigger
        aria-label={t('environment.summary')}
        ref={trigger}
        render={<Button className={open ? 'bg-accent' : undefined} size="icon-sm" variant="ghost" />}
        onKeyDown={(event) => {
          if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return
          event.preventDefault()
          setOpen(true)
          focusMenuItem(event.key === 'ArrowDown' ? 'first' : 'last')
        }}
        title={backgroundWorkCountSummary(items, t)}
      >
        <WakuIcon name="info" />
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner
          align="end"
          className="z-[100] outline-none"
          collisionPadding={8}
          side="bottom"
          sideOffset={4}
        >
          <Popover.Popup
            aria-label={t('environment.title')}
            className="waku-popover-surface max-h-[420px] w-[min(300px,calc(100vw-24px))] overflow-y-auto rounded-[12px] p-2 text-popover-foreground outline-none"
            initialFocus={false}
            ref={popup}
            role="menu"
            onKeyDown={(event) => {
              if (!['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) return
              const menuItems = [...event.currentTarget.querySelectorAll<HTMLElement>('[role="menuitem"]')]
              if (!menuItems.length) return
              const index = menuItems.indexOf(document.activeElement as HTMLElement)
              const target = event.key === 'Home'
                ? 0
                : event.key === 'End'
                  ? menuItems.length - 1
                  : event.key === 'ArrowDown'
                    ? (index + 1 + menuItems.length) % menuItems.length
                    : (index - 1 + menuItems.length) % menuItems.length
              event.preventDefault()
              menuItems[target]?.focus()
            }}
          >
            <div className="flex h-[30px] items-center px-2 text-[13.5px] text-[var(--text-tertiary)]">
              {t('environment.title')}
            </div>
            <EnvironmentAction
              icon="gitCommitHorizontal"
              label={t('environment.commit_or_push')}
              onClick={() => {
                setOpen(false)
                onCommit(trigger.current)
              }}
            />
            <EnvironmentAction
              icon="github"
              label={t('environment.compare_branch')}
              trailing="arrowUpRight"
              onClick={() => {
                setOpen(false)
                onCompareBranch()
                focusTrigger()
              }}
            />
            {hasBackgroundWork && <div className="mx-2 my-2 h-px bg-border" />}
            {processes.length > 0 && (
              <EnvironmentWorkSection
                items={processes}
                label={t('background.processes')}
                onOpen={(id) => {
                  setOpen(false)
                  onOpenBackgroundWork(id)
                  focusTrigger()
                }}
                onStop={stop}
                t={t}
              />
            )}
            {agents.length > 0 && (
              <EnvironmentWorkSection
                items={agents}
                label={t('background.agents')}
                onOpen={(id) => {
                  setOpen(false)
                  onOpenBackgroundWork(id)
                  focusTrigger()
                }}
                onStop={stop}
                t={t}
              />
            )}
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  )
}

function EnvironmentWorkSection({
  label,
  items,
  onOpen,
  onStop,
  t,
}: {
  label: string
  items: BackgroundWorkItem[]
  onOpen: (id: string) => void
  onStop: (item: BackgroundWorkItem) => void
  t: Translator
}) {
  return (
    <section className="flex flex-col gap-[5px]" aria-label={label}>
      <div className="px-2 text-[13px] text-[var(--text-tertiary)]">{label}</div>
      <div className="flex flex-col gap-0.5">
        {items.map((item) => {
          const stoppable = isLiveBackgroundStatus(item.status) && item.canStop
          return (
            <div
              className="group flex h-8 w-full cursor-default items-center gap-[9px] rounded-[8px] px-2 outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring"
              key={`${item.kind}:${item.id}`}
              role="menuitem"
              tabIndex={0}
              onClick={() => onOpen(item.id)}
              onKeyDown={(event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault()
                  onOpen(item.id)
                }
              }}
            >
              <WakuIcon
                className="size-3.5 text-[var(--text-secondary)]"
                name={item.kind === 'subagent' ? 'bot' : 'terminalSquare'}
              />
              <span className={item.kind === 'subagent'
                ? 'min-w-0 flex-1 truncate text-left text-[13.5px]'
                : 'min-w-0 flex-1 truncate text-left text-[12px] text-[var(--text-secondary)]'}>
                {item.title}
              </span>
              {(item.kind !== 'subagent' || stoppable) && (
                <span className="relative grid size-6 shrink-0 place-items-center">
                  {item.kind !== 'subagent' && (
                    <EnvironmentWorkStatus
                      className={stoppable ? 'group-hover:invisible group-focus-within:invisible' : undefined}
                      status={item.status}
                    />
                  )}
                  {stoppable && (
                    <button
                      aria-label={t('background.stop_named', { name: item.title })}
                      className="absolute inset-0 grid place-items-center rounded-md opacity-0 outline-none hover:bg-accent group-hover:opacity-100 group-focus-within:opacity-100 focus-visible:ring-1 focus-visible:ring-ring"
                      type="button"
                      onClick={(event) => {
                        event.stopPropagation()
                        onStop(item)
                      }}
                      onPointerDown={(event) => event.stopPropagation()}
                    >
                      <WakuIcon className="size-3 text-[var(--text-tertiary)]" name="stopFilled" />
                    </button>
                  )}
                </span>
              )}
            </div>
          )
        })}
      </div>
    </section>
  )
}

function EnvironmentWorkStatus({ status, className }: { status: BackgroundWorkStatus; className?: string }) {
  const live = isLiveBackgroundStatus(status)
  const icon = live
    ? 'loaderCircle'
    : status === 'completed'
      ? 'check'
      : status === 'failed'
        ? 'x'
        : 'stop'
  return (
    <WakuIcon
      className={`${className ?? ''} size-3 ${live ? 'text-ring motion-safe:animate-spin' : status === 'completed' ? 'text-[var(--success)]' : status === 'failed' ? 'text-destructive' : 'text-[var(--text-tertiary)]'}`}
      name={icon}
    />
  )
}

function backgroundWorkCountSummary(items: BackgroundWorkItem[], t: Translator) {
  const processes = items.filter((item) => item.kind !== 'subagent' && isLiveBackgroundStatus(item.status)).length
  const agents = items.filter((item) => item.kind === 'subagent' && isLiveBackgroundStatus(item.status)).length
  const parts = []
  if (processes) parts.push(t(processes === 1 ? 'background.process_count_one' : 'background.process_count', { count: processes }))
  if (agents) parts.push(t(agents === 1 ? 'background.agent_count_one' : 'background.agent_count', { count: agents }))
  return parts.length ? parts.join(' · ') : t('environment.summary')
}

function isLiveBackgroundStatus(status: BackgroundWorkStatus) {
  return status === 'pending' || status === 'running'
}

function EnvironmentAction({
  icon,
  label,
  trailing,
  onClick,
}: {
  icon: 'gitCommitHorizontal' | 'github'
  label: string
  trailing?: 'arrowUpRight'
  onClick: () => void
}) {
  return (
    <button
      className="flex min-h-8 w-full items-center gap-2.5 rounded-[8px] px-2 text-[13.5px] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring"
      role="menuitem"
      type="button"
      onClick={onClick}
    >
      <WakuIcon className="size-3.5 text-[var(--text-secondary)]" name={icon} />
      <span className="min-w-0 flex-1 truncate text-left">{label}</span>
      {trailing && <WakuIcon className="size-[13px] text-[var(--text-tertiary)]" name={trailing} />}
    </button>
  )
}

function NewTaskCanvas({
  project,
  projects,
  onProject,
  onAddProject,
  onProjectless,
}: {
  project: T.Project
  projects: T.Project[]
  onProject: (project: T.Project) => void
  onAddProject: () => void
  onProjectless: () => void
}) {
  const { t } = useI18n()
  const projectless = isProjectlessProject(project)
  return (
    <div className="flex min-h-0 flex-1 items-center justify-center px-8 pb-12">
      <div className="text-center">
        <WakuIcon className="mx-auto size-5 text-ring" name="sparkle" />
        <div className="mt-3 flex flex-wrap items-baseline justify-center gap-1 text-xl font-medium">
          {projectless ? (
            <span>{t('onboarding.what_should_we_build')}</span>
          ) : (
            <>
              <span>{t('onboarding.what_should_we_build_in')}</span>
              <ControlMenu
                caret={false}
                items={[
                  ...selectableProjects(projects, project)
                    .filter((item) => item.name !== 'No project')
                    .map((item) => ({
                      id: item.id,
                      label: item.name,
                      selected: item.id === project.id,
                      onSelect: () => onProject(item),
                    })),
                  { id: 'add', label: t('project.new_project'), icon: 'folderNew' as const, onSelect: onAddProject },
                  { id: 'projectless', label: t('project.no_project'), icon: 'x' as const, onSelect: onProjectless },
                ]}
                label={projectDisplayName(project, t('project.no_project_name'))}
                menuClassName="w-60 text-sm"
                placement="below"
                triggerClassName="h-auto max-w-64 rounded-none border-b border-dashed border-[var(--text-tertiary)] px-0 text-[20px] font-medium leading-6 text-foreground hover:bg-transparent focus-visible:ring-0 focus-visible:border-ring"
              />
              <span>{t('onboarding.question_mark')}</span>
            </>
          )}
        </div>
      </div>
    </div>
  )
}

function NoProjectState({
  projectlessPending,
  onAddProject,
  onProjectless,
}: {
  projectlessPending: boolean
  onAddProject: () => void
  onProjectless: () => void
}) {
  const { t } = useI18n()
  return (
    <div className="flex min-h-0 flex-1 items-center justify-center px-8 pb-12">
      <div className="max-w-sm text-center">
        <WakuIcon className="mx-auto size-6 text-ring" name="sparkle" />
        <h2 className="mt-4 text-xl font-medium">{t('onboarding.open_project_to_begin')}</h2>
        <p className="mt-2 text-[12.5px] leading-[19px] text-[var(--text-tertiary)]">
          {t('onboarding.web_description')}
        </p>
        <div className="mt-5 flex flex-col items-center gap-2">
          <Button className="rounded-full" onClick={onAddProject}>
            <WakuIcon name="plus" /> {t('sidebar.add_project')}
          </Button>
          <Button
            className="rounded-full"
            disabled={projectlessPending}
            variant="ghost"
            onClick={onProjectless}
          >
            {projectlessPending ? t('common.creating') : t('project.no_project_name')}
          </Button>
        </div>
      </div>
    </div>
  )
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}
