import type * as T from '@waku/client/v8'
import { ContextMenu } from '@base-ui/react/context-menu'
import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode, type RefObject } from 'react'
import { Virtuoso, type ListItem, type VirtuosoHandle } from 'react-virtuoso'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { PreviewableImage } from '@/components/image-preview'
import { FileTypeIcon, WakuIcon, type WakuIconName } from '@/components/waku-icon'
import { useTurnItem } from '@/hooks/use-daemon-data'
import { useCommandOutput } from '@/hooks/use-daemon-stores'
import { readAttachmentImage } from '@/lib/attachments'
import { useDaemon } from '@/lib/daemon-context'
import type { DiffSource } from '@/lib/daemon-api'
import { useI18n, type AppLocale } from '@/lib/i18n'
import {
  advanceMarkdownVeil,
  createMarkdownVeilState,
  markdownVeilPlugin,
} from '@/lib/markdown-veil'
import {
  activityFromItem,
  type ActivityView,
  type BackgroundWorkItem,
  type BackgroundWorkStatus,
  type CheckpointView,
  type RunView,
  type Task,
  type TranscriptEntry,
  type TranscriptMessage,
} from '@/lib/thread-model'
import {
  activityActionLabel,
  activityDisclosureSections,
  activityFileChangeStats,
  activityGroupIsLive,
  activityHeaderTitle,
  activityPreview,
  activityRowDetail,
  activityTextRows,
  assistantResponseFooters,
  fencedCode,
  formatMessageTime,
  formatWorkingElapsed,
  shouldVirtualizeActivityText,
  turnAnswerStart,
  turnFoldLabel,
} from '@/lib/transcript-presentation'
import type { AssistantResponseFooter, Translator } from '@/lib/transcript-presentation'
import {
  activeNavigationTurn,
  firstVisibleTranscriptItem,
} from '@/lib/transcript-navigation'
import { cn } from '@/lib/utils'

const NAVIGATION_RAIL_MIN_WIDTH = 872
const NAVIGATION_RAIL_PITCH = 12
const NAVIGATION_PREVIEW_HEIGHT = 126
/** Gestures by which the reader scrolls away from the newest output. */
const USER_SCROLL_EVENTS = ['wheel', 'touchmove', 'keydown', 'pointerdown'] as const
const SCROLL_UP_KEYS = new Set(['ArrowUp', 'PageUp', 'Home'])

/** Virtuoso's index of the first row; it shrinks as older pages are prepended. */
const FIRST_ITEM_INDEX = 1_000_000
const TranscriptLinkContext = createContext<(target: string) => boolean>(() => false)

type NavigationTurn = {
  messageId: string
  prompt: string
  response: string
}

type ResponseForkAction = {
  runId: string
  pending: boolean
  onFork: (runId: string) => void
}

type MessageRewindAction = {
  runId: string
  pending: boolean
  onBegin: () => void
}

type MessageEdit = {
  messageId: string
  runId: string
  content: string
  attachments: T.Attachment[]
}

export function Transcript({
  task,
  onLoadOlder,
  loadingOlder = false,
  onReviewChanges,
  onOpenLink,
  onOpenBackgroundWork,
  onCopyToComposer,
  onForkResponse,
  onRewindMessage,
  forkingRunId,
  rewindingRunId,
}: {
  task: Task
  onLoadOlder?: () => void
  loadingOlder?: boolean
  onReviewChanges?: (source: DiffSource) => void
  onOpenLink?: (target: string) => boolean
  onOpenBackgroundWork?: (id: string) => void
  onCopyToComposer?: (content: string) => void
  onForkResponse?: (runId: string) => void
  onRewindMessage?: (runId: string, prompt: string, attachments: T.Attachment[]) => Promise<void>
  forkingRunId?: string
  rewindingRunId?: string
}) {
  const { locale, t } = useI18n()
  const root = useRef<HTMLDivElement>(null)
  const transcript = useRef<VirtuosoHandle>(null)
  const transcriptScroller = useRef<HTMLElement | null>(null)
  const renderedItems = useRef<ListItem<TranscriptRenderItem>[]>([])
  const renderedItemsTask = useRef(task.id)
  const navigationTurnsRef = useRef<Array<NavigationTurn & { itemIndex: number }>>([])
  const activeTurnFrame = useRef<number | null>(null)
  const wasNearBottom = useRef(true)
  const expandedTask = useRef(task.id)
  const firstItem = useRef({ taskId: task.id, key: undefined as string | undefined, index: FIRST_ITEM_INDEX })
  const [expandedRuns, setExpandedRuns] = useState<Set<string>>(new Set())
  const [atTop, setAtTop] = useState(true)
  const [atBottom, setAtBottom] = useState(true)
  const [railFits, setRailFits] = useState(false)
  const [activeTurnIndex, setActiveTurnIndex] = useState(0)
  const [messageEdit, setMessageEdit] = useState<MessageEdit | null>(null)
  const items = buildTranscriptItems(task, expandedRuns)

  // Older pages are prepended: keep the rows on screen where they are by
  // lowering Virtuoso's first index by the rows that appeared above them.
  const anchor = firstItem.current
  if (anchor.taskId !== task.id) {
    firstItem.current = { taskId: task.id, key: items[0]?.key, index: FIRST_ITEM_INDEX }
  } else {
    if (anchor.key && items[0]?.key !== anchor.key) {
      const shift = items.findIndex((item) => item.key === anchor.key)
      if (shift > 0) anchor.index = Math.max(0, anchor.index - shift)
    }
    anchor.key = items[0]?.key
  }
  const firstItemIndex = firstItem.current.index

  const itemIndexesByMessage = new Map<string, number>()
  items.forEach((item, index) => {
    if (item.kind === 'message' && item.message.role === 'user') {
      itemIndexesByMessage.set(item.message.id, index)
    }
  })
  const navigationTurns = transcriptNavigationTurns(task).flatMap((turn) => {
    const itemIndex = itemIndexesByMessage.get(turn.messageId)
    return itemIndex === undefined ? [] : [{ ...turn, itemIndex }]
  })
  navigationTurnsRef.current = navigationTurns
  if (renderedItemsTask.current !== task.id) {
    renderedItemsTask.current = task.id
    renderedItems.current = []
  }

  const updateActiveTurnFromViewport = useCallback(() => {
    activeTurnFrame.current = null
    const scroller = transcriptScroller.current
    if (!scroller) return
    const atBottom = scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight <= 100
    const firstVisibleItem = firstVisibleTranscriptItem(renderedItems.current, scroller.scrollTop)
    if (firstVisibleItem === null && !atBottom) return
    const active = activeNavigationTurn(
      navigationTurnsRef.current,
      firstVisibleItem ?? 0,
      atBottom,
    )
    if (active !== null) setActiveTurnIndex(active)
  }, [])

  const scheduleActiveTurnUpdate = useCallback(() => {
    if (activeTurnFrame.current !== null) return
    activeTurnFrame.current = requestAnimationFrame(updateActiveTurnFromViewport)
  }, [updateActiveTurnFromViewport])

  // Only the reader leaves the bottom: a streaming answer that grows past
  // the threshold must not stop the transcript from following it.
  const leaveBottom = useCallback((event: Event) => {
    if (event instanceof WheelEvent && event.deltaY >= 0) return
    if (event instanceof KeyboardEvent && !SCROLL_UP_KEYS.has(event.key)) return
    // A press counts only on the scrollbar, past the content box.
    if (
      event instanceof PointerEvent
      && event.currentTarget instanceof HTMLElement
      && event.offsetX < event.currentTarget.clientWidth
    ) return
    wasNearBottom.current = false
  }, [])

  const setTranscriptScroller = useCallback((next: HTMLElement | Window | null) => {
    const element = next instanceof HTMLElement ? next : null
    if (transcriptScroller.current === element) return
    transcriptScroller.current?.removeEventListener('scroll', scheduleActiveTurnUpdate)
    for (const type of USER_SCROLL_EVENTS) transcriptScroller.current?.removeEventListener(type, leaveBottom)
    transcriptScroller.current = element
    transcriptScroller.current?.addEventListener('scroll', scheduleActiveTurnUpdate, { passive: true })
    for (const type of USER_SCROLL_EVENTS) {
      transcriptScroller.current?.addEventListener(type, leaveBottom, { passive: true })
    }
    scheduleActiveTurnUpdate()
  }, [leaveBottom, scheduleActiveTurnUpdate])

  useEffect(() => () => {
    transcriptScroller.current?.removeEventListener('scroll', scheduleActiveTurnUpdate)
    for (const type of USER_SCROLL_EVENTS) transcriptScroller.current?.removeEventListener(type, leaveBottom)
    if (activeTurnFrame.current !== null) cancelAnimationFrame(activeTurnFrame.current)
  }, [leaveBottom, scheduleActiveTurnUpdate])

  useEffect(() => {
    if (expandedTask.current === task.id) return
    expandedTask.current = task.id
    setExpandedRuns(new Set())
    setMessageEdit(null)
    wasNearBottom.current = true
    setAtBottom(true)
    setActiveTurnIndex(Math.max(0, navigationTurns.length - 1))
  }, [task.id])

  useEffect(() => {
    const rootElement = root.current
    if (!rootElement) return
    const resizeObserver = new ResizeObserver(() => {
      setRailFits(rootElement.clientWidth >= NAVIGATION_RAIL_MIN_WIDTH)
    })
    resizeObserver.observe(rootElement)
    setRailFits(rootElement.clientWidth >= NAVIGATION_RAIL_MIN_WIDTH)
    return () => resizeObserver.disconnect()
  }, [])

  // A run counts as content before its first item exists: a provider-started
  // run (a goal continuation) reasons for a while before its first delta.
  const empty = task.entries.length === 0 && !task.activeRun
  const loadingOlderLabel = t('transcript.loading_earlier')
  return (
    <TranscriptLinkContext.Provider value={onOpenLink ?? (() => false)}>
      <div className="relative min-h-0 flex-1" ref={root}>
      {empty ? (
        <div className="absolute inset-0 grid place-items-center pb-8">
          <div className="text-center">
            <WakuIcon className="mx-auto size-5 text-ring" name="sparkle" />
            <h2 className="mt-3 text-xl font-medium tracking-tight">
              {t('onboarding.what_should_we_build')}
            </h2>
          </div>
        </div>
      ) : (
        <Virtuoso
          atBottomStateChange={(nextAtBottom) => {
            if (nextAtBottom) wasNearBottom.current = true
            setAtBottom(nextAtBottom)
            if (nextAtBottom && navigationTurns.length) {
              setActiveTurnIndex(navigationTurns.length - 1)
            }
          }}
          atBottomThreshold={100}
          atTopStateChange={setAtTop}
          className="absolute inset-0 overscroll-contain"
          components={{ Header: TranscriptHeader }}
          context={{ loadingOlder, label: loadingOlderLabel }}
          computeItemKey={(_, item) => item.key}
          data={items}
          defaultItemHeight={64}
          firstItemIndex={firstItemIndex}
          followOutput={(isAtBottom) => isAtBottom || wasNearBottom.current ? 'auto' : false}
          increaseViewportBy={{ top: 800, bottom: 1_200 }}
          initialTopMostItemIndex={{ index: Math.max(0, items.length - 1), align: 'end' }}
          itemsRendered={(nextItems) => {
            // Rendered indexes carry the first index; navigation uses data indexes.
            renderedItems.current = nextItems.map((item) => ({ ...item, index: item.index - firstItemIndex }))
            scheduleActiveTurnUpdate()
          }}
          itemContent={(_, item) => (
            <TranscriptItemView
              backgroundWork={task.backgroundWork}
              item={item}
              locale={locale}
              taskId={task.id}
              t={t}
              onCopyToComposer={onCopyToComposer}
              onOpenBackgroundWork={onOpenBackgroundWork}
              onForkResponse={onForkResponse}
              forkingRunId={forkingRunId}
              messageEdit={messageEdit}
              onBeginMessageEdit={(message, runId) => setMessageEdit({
                messageId: message.id,
                runId,
                content: message.displayText ?? message.text,
                attachments: message.attachments,
              })}
              onCancelMessageEdit={() => {
                if (messageEdit?.runId !== rewindingRunId) setMessageEdit(null)
              }}
              onSubmitMessageEdit={async (prompt) => {
                if (!messageEdit || !onRewindMessage) return
                const editing = messageEdit
                try {
                  await onRewindMessage(editing.runId, prompt, editing.attachments)
                  setMessageEdit((current) => current?.messageId === editing.messageId ? null : current)
                } catch {
                  // The app-level handler reports the failure. Keep the edit
                  // open so the user's replacement remains recoverable.
                }
              }}
              rewindingRunId={rewindingRunId}
              onReviewChanges={onReviewChanges}
              onToggleRun={(runId) => setExpandedRuns((current) => {
                const next = new Set(current)
                if (next.has(runId)) next.delete(runId)
                else next.add(runId)
                return next
              })}
            />
          )}
          key={task.id}
          minOverscanItemCount={{ top: 2, bottom: 3 }}
          ref={transcript}
          scrollerRef={setTranscriptScroller}
          startReached={() => {
            if (task.hasMoreHistory && !loadingOlder) onLoadOlder?.()
          }}
          totalListHeightChanged={() => {
            if (wasNearBottom.current) {
              transcript.current?.scrollToIndex({
                index: Math.max(0, items.length - 1),
                align: 'end',
                behavior: 'auto',
              })
            }
          }}
        />
      )}
      {railFits && !(atTop && atBottom) && navigationTurns.length >= 2 && (
        <ConversationNavigationRail
          activeIndex={Math.min(activeTurnIndex, navigationTurns.length - 1)}
          t={t}
          turns={navigationTurns}
          onActivate={(index) => {
            wasNearBottom.current = false
            setActiveTurnIndex(index)
            transcript.current?.scrollToIndex({
              index: navigationTurns[index]!.itemIndex,
              align: 'start',
              behavior: 'auto',
            })
          }}
        />
      )}
      {!atBottom && !empty && (
        <button
          aria-label={t('transcript.scroll_to_bottom')}
          className="absolute bottom-2 left-1/2 z-10 grid size-8 -translate-x-1/2 place-items-center rounded-full border bg-card shadow-sm outline-none hover:bg-[var(--raised)] focus-visible:ring-1 focus-visible:ring-ring"
          type="button"
          onClick={() => transcript.current?.scrollToIndex({
            index: Math.max(0, items.length - 1),
            align: 'end',
            behavior: 'auto',
          })}
        >
          <WakuIcon className="size-4" name="arrowDown" />
        </button>
      )}
      </div>
    </TranscriptLinkContext.Provider>
  )
}

function transcriptNavigationTurns(task: Task): NavigationTurn[] {
  const runs = new Map(task.runs.map((run) => [run.id, run]))
  const messages = task.entries.flatMap((entry) => entry.kind === 'message' ? [entry] : [])
  const userIndexes: number[] = []
  messages.forEach((entry, index) => {
    if (entry.message.role === 'user') userIndexes.push(index)
  })
  return userIndexes.map((messageIndex, turnIndex) => {
    const entry = messages[messageIndex]!
    const message = entry.message
    const nextUserIndex = userIndexes[turnIndex + 1] ?? messages.length
    const visible = message.displayText ?? message.text
    const prompt = visible.trim()
      ? navigationPreviewSnippet(visible, 100)
      : message.attachments.map((attachment) => attachment.name).join(', ')
    const running = entry.runId ? runs.get(entry.runId)?.live ?? false : false
    let response = ''
    if (!running) {
      for (let index = nextUserIndex - 1; index > messageIndex; index--) {
        const candidate = messages[index]!.message
        if (candidate.role === 'assistant' && candidate.text.trim()) {
          response = navigationPreviewSnippet(candidate.text, 240)
          break
        }
      }
    }
    return { messageId: message.id, prompt, response }
  })
}

function navigationPreviewSnippet(content: string, limit: number) {
  const normalized = content.trim().split(/\s+/).join(' ')
  const characters = Array.from(normalized)
  return characters.length > limit ? `${characters.slice(0, limit).join('')}…` : normalized
}

function ConversationNavigationRail({
  turns,
  activeIndex,
  t,
  onActivate,
}: {
  turns: NavigationTurn[]
  activeIndex: number
  t: Translator
  onActivate: (index: number) => void
}) {
  const root = useRef<HTMLElement>(null)
  const rail = useRef<VirtuosoHandle>(null)
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)
  const [focusedIndex, setFocusedIndex] = useState<number | null>(null)
  const [previewTop, setPreviewTop] = useState(0)
  const [atTop, setAtTop] = useState(true)
  const [atBottom, setAtBottom] = useState(true)
  const totalHeight = turns.length * NAVIGATION_RAIL_PITCH
  const emphasizedIndex = hoveredIndex ?? focusedIndex

  function updatePreviewPosition(index: number) {
    const railBounds = root.current?.getBoundingClientRect()
    const tickBounds = document
      .getElementById(`conversation-navigation-turn-${turns[index]!.messageId}`)
      ?.getBoundingClientRect()
    if (!railBounds || !tickBounds) return
    const centered = tickBounds.top - railBounds.top
      + tickBounds.height / 2 - NAVIGATION_PREVIEW_HEIGHT / 2
    setPreviewTop(Math.max(0, Math.min(railBounds.height - NAVIGATION_PREVIEW_HEIGHT, centered)))
  }

  function focusTurn(index: number) {
    rail.current?.scrollIntoView({ index, behavior: 'auto' })
    requestAnimationFrame(() => {
      document.getElementById(`conversation-navigation-turn-${turns[index]!.messageId}`)?.focus()
    })
  }

  useEffect(() => {
    rail.current?.scrollIntoView({ index: activeIndex, behavior: 'auto' })
  }, [activeIndex, turns.length])

  return (
    <nav
      aria-label={t('transcript.conversation_turns')}
      className="absolute left-4 top-1/2 z-10 w-11 -translate-y-1/2"
      ref={root}
      style={{ height: `min(${totalHeight}px, 80%)` }}
    >
      <Virtuoso
        atBottomStateChange={setAtBottom}
        atTopStateChange={setAtTop}
        className="h-full w-11 overscroll-contain"
        computeItemKey={(_, turn) => turn.messageId}
        data={turns}
        fixedItemHeight={NAVIGATION_RAIL_PITCH}
        increaseViewportBy={NAVIGATION_RAIL_PITCH * 5}
        itemContent={(index, turn) => {
          const emphasizedDistance = emphasizedIndex === null
            ? Number.POSITIVE_INFINITY
            : Math.abs(index - emphasizedIndex)
          const scale = emphasizedDistance === 0
            ? 1
            : emphasizedDistance === 1
              ? 0.68
              : emphasizedDistance === 2
                ? 0.44
                : 0.25
          const prominent = index === activeIndex || index === emphasizedIndex
          return (
            <button
              aria-current={index === activeIndex ? 'step' : undefined}
              aria-label={t('transcript.turn_label', { number: index + 1, prompt: turn.prompt })}
              className="flex h-3 w-11 items-center rounded outline-none focus-visible:ring-1 focus-visible:ring-ring"
              id={`conversation-navigation-turn-${turn.messageId}`}
              tabIndex={index === activeIndex ? 0 : -1}
              type="button"
              onBlur={() => setFocusedIndex((current) => current === index ? null : current)}
              onClick={(event) => {
                setHoveredIndex(null)
                setFocusedIndex(null)
                if (event.detail > 0) event.currentTarget.blur()
                onActivate(index)
              }}
              onFocus={() => {
                setFocusedIndex(index)
                requestAnimationFrame(() => updatePreviewPosition(index))
              }}
              onKeyDown={(event) => {
                let target: number | null = null
                if (event.key === 'ArrowUp') target = Math.max(0, index - 1)
                else if (event.key === 'ArrowDown') target = Math.min(turns.length - 1, index + 1)
                else if (event.key === 'Home') target = 0
                else if (event.key === 'End') target = turns.length - 1
                if (target === null) return
                event.preventDefault()
                focusTurn(target)
              }}
              onMouseEnter={() => {
                setHoveredIndex(index)
                requestAnimationFrame(() => updatePreviewPosition(index))
              }}
              onMouseLeave={() => setHoveredIndex((current) => current === index ? null : current)}
            >
              <span
                className={cn(
                  'h-0.5 rounded-full transition-[width] duration-300 ease-out motion-reduce:transition-none',
                  prominent ? 'bg-foreground' : 'bg-[var(--text-ghost)] opacity-45',
                )}
                style={{ width: 32 * scale }}
              />
            </button>
          )
        }}
        rangeChanged={() => {
          if (emphasizedIndex !== null) {
            requestAnimationFrame(() => updatePreviewPosition(emphasizedIndex))
          }
        }}
        ref={rail}
      />
      <div
        aria-hidden="true"
        className={cn(
          'pointer-events-none absolute inset-x-0 top-0 h-5 bg-gradient-to-b from-background to-transparent transition-opacity',
          'motion-reduce:transition-none',
          !atTop ? 'opacity-100' : 'opacity-0',
        )}
      />
      <div
        aria-hidden="true"
        className={cn(
          'pointer-events-none absolute inset-x-0 bottom-0 h-5 bg-gradient-to-t from-background to-transparent transition-opacity',
          'motion-reduce:transition-none',
          !atBottom ? 'opacity-100' : 'opacity-0',
        )}
      />
      {emphasizedIndex !== null && (
        <div
          className="waku-popover-surface pointer-events-none absolute left-[60px] z-20 flex max-h-[126px] w-80 flex-col gap-1.5 overflow-hidden rounded-[14px] px-[15px] py-3 text-popover-foreground"
          style={{ top: previewTop }}
        >
          <div className="truncate text-sm font-semibold leading-5">
            {turns[emphasizedIndex]!.prompt}
          </div>
          {turns[emphasizedIndex]!.response && (
            <div className="max-h-[60px] overflow-hidden text-[13px] leading-5 text-[var(--text-tertiary)]">
              {turns[emphasizedIndex]!.response}
            </div>
          )}
        </div>
      )}
    </nav>
  )
}


type TranscriptRenderItem =
  | {
      kind: 'fold'
      key: string
      runId: string
      run: RunView
      expanded: boolean
    }
  | {
      kind: 'block'
      key: string
      activities: ActivityView[]
      liveGroup: boolean
    }
  | {
      kind: 'message'
      key: string
      message: TranscriptMessage
      first: boolean
      followUp: boolean
      footer: AssistantResponseFooter | null
      forkRunId: string | null
      rewindRunId: string | null
      checkpoint: CheckpointView | null
    }
  | {
      kind: 'changed'
      key: string
      checkpoint: CheckpointView
    }
  | {
      kind: 'working'
      key: 'working'
      startedAt: number
    }
  | {
      kind: 'tail'
      key: 'tail'
    }

/** One transcript row before folding: a message, a block of activities, or changed files. */
type TranscriptRow =
  | { kind: 'message'; key: string; runId?: string; message: TranscriptMessage }
  | { kind: 'block'; key: string; runId?: string; activities: ActivityView[] }
  | { kind: 'checkpoint'; key: string; runId?: string; checkpoint: CheckpointView }

/** Consecutive activities of one run share a block. */
function transcriptRows(entries: readonly TranscriptEntry[]): TranscriptRow[] {
  const rows: TranscriptRow[] = []
  for (const entry of entries) {
    if (entry.kind === 'activity') {
      const previous = rows.at(-1)
      if (previous?.kind === 'block' && previous.runId === entry.runId) {
        previous.activities.push(entry.activity)
      } else {
        rows.push({ kind: 'block', key: `block-${entry.key}`, runId: entry.runId, activities: [entry.activity] })
      }
    } else if (entry.kind === 'message') {
      rows.push({ kind: 'message', key: entry.key, runId: entry.runId, message: entry.message })
    } else {
      rows.push({ kind: 'checkpoint', key: entry.key, runId: entry.runId, checkpoint: entry.checkpoint })
    }
  }
  return rows
}

/** The work of a settled run before its answer folds behind a "Worked for" line. */
function runFolds(runs: ReadonlyMap<string, RunView>, rows: readonly TranscriptRow[]) {
  const hidden = new Set<string>()
  const anchors = new Map<string, RunView>()
  const rowsByRun = new Map<string, TranscriptRow[]>()
  for (const row of rows) {
    if (!row.runId || row.kind === 'checkpoint' || (row.kind === 'message' && row.message.role !== 'assistant')) continue
    const runRows = rowsByRun.get(row.runId) ?? []
    runRows.push(row)
    rowsByRun.set(row.runId, runRows)
  }
  for (const [runId, runRows] of rowsByRun) {
    const run = runs.get(runId)
    if (!run || run.live) continue
    const answerStart = turnAnswerStart(runRows, (row) => (
      row.kind === 'message' && Boolean(row.message.text.trim())
    ))
    const work = runRows.slice(0, answerStart)
    if (!work.length) continue
    anchors.set(work[0]!.key, run)
    for (const row of work) hidden.add(row.key)
  }
  return { hidden, anchors }
}

function buildTranscriptItems(
  task: Task,
  expandedRuns: ReadonlySet<string>,
): TranscriptRenderItem[] {
  const runs = new Map(task.runs.map((run) => [run.id, run]))
  const rows = transcriptRows(task.entries)
  const folds = runFolds(runs, rows)
  const footers = assistantResponseFooters(task.entries, runs)
  const settled = task.status === 'idle' || task.status === 'failed'

  // A settled run's changed files sit under its last assistant message when
  // that message has text; otherwise they keep their own row.
  const lastAssistantByRun = new Map<string, string>()
  for (const row of rows) {
    if (row.kind === 'message' && row.message.role === 'assistant' && row.runId) {
      lastAssistantByRun.set(row.runId, row.key)
    }
  }
  const inlineCheckpoints = new Map<string, CheckpointView>()
  const inlined = new Set<string>()
  for (const row of rows) {
    if (row.kind !== 'checkpoint' || !row.runId || runs.get(row.runId)?.live) continue
    const messageKey = lastAssistantByRun.get(row.runId)
    const message = rows.find((candidate) => candidate.key === messageKey)
    if (message?.kind === 'message' && message.message.text.trim()) {
      inlineCheckpoints.set(message.key, row.checkpoint)
      inlined.add(row.key)
    }
  }

  const items: TranscriptRenderItem[] = []
  let seenUserMessage = false
  let firstMessage = true
  rows.forEach((row, index) => {
    const foldRun = folds.anchors.get(row.key)
    if (foldRun) {
      items.push({
        kind: 'fold',
        key: `fold-${foldRun.id}`,
        runId: foldRun.id,
        run: foldRun,
        expanded: expandedRuns.has(foldRun.id),
      })
    }
    if (folds.hidden.has(row.key) && !expandedRuns.has(row.runId ?? '')) return
    if (row.kind === 'block') {
      const run = row.runId ? runs.get(row.runId) : undefined
      items.push({
        kind: 'block',
        key: row.key,
        activities: row.activities,
        liveGroup: activityGroupIsLive(Boolean(run?.live), index === rows.length - 1),
      })
      return
    }
    if (row.kind === 'checkpoint') {
      if (!inlined.has(row.key)) items.push({ kind: 'changed', key: row.key, checkpoint: row.checkpoint })
      return
    }
    const { message } = row
    const startsFollowUp = message.role === 'user' && seenUserMessage
    if (message.role === 'user') seenUserMessage = true
    const footer = footers.get(row.key) ?? null
    const run = row.runId ? runs.get(row.runId) : undefined
    items.push({
      kind: 'message',
      key: row.key,
      message,
      first: firstMessage,
      followUp: startsFollowUp,
      footer,
      forkRunId: footer && message.role === 'assistant' && run && !run.live && settled ? run.id : null,
      rewindRunId: message.role === 'user' && row.runId && settled && task.rewindTargets.has(row.runId)
        ? row.runId
        : null,
      checkpoint: inlineCheckpoints.get(row.key) ?? null,
    })
    firstMessage = false
  })

  if (task.activeRun) {
    items.push({
      kind: 'working',
      key: 'working',
      startedAt: task.activeRun.startedAt ?? task.activeRun.requestedAt,
    })
  }
  items.push({ kind: 'tail', key: 'tail' })
  return items
}

function TranscriptHeader({ context }: { context?: { loadingOlder: boolean; label: string } }) {
  if (!context?.loadingOlder) return null
  return (
    <div aria-live="polite" className="flex h-8 items-center justify-center gap-1.5 text-[11.5px] text-[var(--text-tertiary)]">
      <WakuIcon className="size-3 motion-safe:animate-spin" name="loaderCircle" />
      {context.label}
    </div>
  )
}

function TranscriptItemView({
  item,
  backgroundWork,
  locale,
  taskId,
  t,
  onToggleRun,
  onReviewChanges,
  onCopyToComposer,
  onOpenBackgroundWork,
  onForkResponse,
  forkingRunId,
  messageEdit,
  onBeginMessageEdit,
  onCancelMessageEdit,
  onSubmitMessageEdit,
  rewindingRunId,
}: {
  item: TranscriptRenderItem
  backgroundWork: BackgroundWorkItem[]
  locale: AppLocale
  taskId: string
  t: Translator
  onToggleRun: (runId: string) => void
  onReviewChanges?: (source: DiffSource) => void
  onCopyToComposer?: (content: string) => void
  onOpenBackgroundWork?: (id: string) => void
  onForkResponse?: (runId: string) => void
  forkingRunId?: string
  messageEdit: MessageEdit | null
  onBeginMessageEdit: (message: TranscriptMessage, runId: string) => void
  onCancelMessageEdit: () => void
  onSubmitMessageEdit: (prompt: string) => Promise<void>
  rewindingRunId?: string
}) {
  if (item.kind === 'tail') return <div className="h-5" />
  let content: ReactNode
  // A run's changed files review that run's turn diff.
  const review = (checkpoint: CheckpointView) => onReviewChanges
    ? () => onReviewChanges(checkpoint.runId ? { type: 'turn', runId: checkpoint.runId } : 'uncommitted')
    : undefined
  if (item.kind === 'fold') {
    content = (
      <TurnFold
        expanded={item.expanded}
        label={turnFoldLabel(item.run, t)}
        onToggle={() => onToggleRun(item.runId)}
      />
    )
  } else if (item.kind === 'block') {
    content = (
      <ActivityGroup
        activities={item.activities}
        backgroundWork={backgroundWork}
        liveGroup={item.liveGroup}
        t={t}
        taskId={taskId}
        onOpenBackgroundWork={onOpenBackgroundWork}
      />
    )
  } else if (item.kind === 'changed') {
    content = (
      <ChangedFilesCard
        additions={item.checkpoint.additions}
        deletions={item.checkpoint.deletions}
        files={item.checkpoint.files}
        t={t}
        onReview={review(item.checkpoint)}
      />
    )
  } else if (item.kind === 'working') {
    content = <WorkingIndicator startedAt={item.startedAt} t={t} />
  } else {
    content = (
      <MessageRow
        footer={item.footer}
        locale={locale}
        message={item.message}
        t={t}
        onCopyToComposer={onCopyToComposer}
        editing={messageEdit?.messageId === item.message.id}
        editContent={messageEdit?.messageId === item.message.id ? messageEdit.content : undefined}
        onCancelEdit={onCancelMessageEdit}
        onSubmitEdit={onSubmitMessageEdit}
        forkAction={item.forkRunId && onForkResponse
          ? {
              runId: item.forkRunId,
              pending: forkingRunId === item.forkRunId,
              onFork: onForkResponse,
            }
          : undefined}
        rewindAction={item.rewindRunId
          ? {
              runId: item.rewindRunId,
              pending: rewindingRunId === item.rewindRunId,
              onBegin: () => onBeginMessageEdit(item.message, item.rewindRunId!),
            }
          : undefined}
        beforeFooter={item.checkpoint
          ? (
            <ChangedFilesCard
              additions={item.checkpoint.additions}
              deletions={item.checkpoint.deletions}
              files={item.checkpoint.files}
              t={t}
              onReview={review(item.checkpoint)}
            />
          )
          : null}
      />
    )
  }
  return (
    <div className="mx-auto w-full max-w-[760px] px-5 sm:px-5">
      <TranscriptRow
        first={item.kind === 'message' && item.first}
        followUp={item.kind === 'message' && item.followUp}
      >
        {content}
      </TranscriptRow>
    </div>
  )
}

function TurnFold({
  label,
  expanded,
  onToggle,
}: {
  label: string
  expanded: boolean
  onToggle: () => void
}) {
  return (
    <div className="flex h-6 w-full items-center gap-2.5">
      <div className="h-px flex-1 bg-border" />
      <button
        aria-expanded={expanded}
        className="flex h-6 shrink-0 items-center gap-1 rounded px-0.5 text-[11.5px] font-medium leading-4 text-[var(--text-tertiary)] outline-none hover:text-[var(--text-secondary)] focus-visible:ring-1 focus-visible:ring-ring"
        type="button"
        onClick={onToggle}
      >
        {label}
        <WakuIcon className="size-2.5" name={expanded ? 'chevronDown' : 'chevronRight'} />
      </button>
      <div className="h-px flex-1 bg-border" />
    </div>
  )
}

/** `startedAt` is epoch milliseconds. */
function WorkingIndicator({ startedAt, t }: { startedAt: number; t: Translator }) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    let timeout = 0
    const tick = () => {
      setNow(Date.now())
      timeout = window.setTimeout(tick, 1_000 - (Date.now() % 1_000) + 8)
    }
    timeout = window.setTimeout(tick, 1_000 - (Date.now() % 1_000) + 8)
    return () => window.clearTimeout(timeout)
  }, [startedAt])
  return (
    <div className="flex h-[22px] items-center gap-2 text-[11.5px] text-[var(--text-tertiary)]">
      <span className="flex items-center gap-[3px]" aria-hidden="true">
        <span className="size-1 animate-pulse rounded-full bg-current motion-reduce:animate-none" />
        <span className="size-1 animate-pulse rounded-full bg-current [animation-delay:120ms] motion-reduce:animate-none" />
        <span className="size-1 animate-pulse rounded-full bg-current [animation-delay:240ms] motion-reduce:animate-none" />
      </span>
      <span className="font-medium">
        {t('transcript.working_for', {
          duration: formatWorkingElapsed(Math.max(0, Math.floor((now - startedAt) / 1_000)), t),
        })}
      </span>
    </div>
  )
}

function TranscriptRow({
  children,
  first = false,
  followUp = false,
}: {
  children: ReactNode
  first?: boolean
  followUp?: boolean
}) {
  return (
    <div className={cn('py-2', first && 'pt-[22px]', followUp && 'pt-8')}>
      <div className="min-w-0">{children}</div>
    </div>
  )
}

function MessageRow({
  message,
  locale,
  t,
  footer,
  beforeFooter,
  onCopyToComposer,
  forkAction,
  rewindAction,
  editing = false,
  editContent,
  onCancelEdit,
  onSubmitEdit,
}: {
  message: TranscriptMessage
  locale: AppLocale
  t: Translator
  footer: AssistantResponseFooter | null
  beforeFooter?: ReactNode
  onCopyToComposer?: (content: string) => void
  forkAction?: ResponseForkAction
  rewindAction?: MessageRewindAction
  editing?: boolean
  editContent?: string
  onCancelEdit?: () => void
  onSubmitEdit?: (prompt: string) => Promise<void>
}) {
  const visible = message.displayText ?? message.text
  const copyContent = footer?.content ?? visible
  if (message.role === 'user') {
    return (
      <MessageContextMenu
        content={copyContent}
        t={t}
        copyToComposer={!rewindAction && visible && onCopyToComposer
          ? () => onCopyToComposer(visible)
          : undefined}
        rewindAction={rewindAction}
      >
        <article className="group/message flex w-full flex-col items-end gap-1">
          {message.attachments.length ? (
            <div className="flex max-w-[540px] flex-wrap justify-end gap-2">
              {message.attachments.map((attachment) => (
                <Attachment key={`${attachment.blobReference}-${attachment.name}`} attachment={attachment} />
              ))}
            </div>
          ) : null}
          {editing && onCancelEdit && onSubmitEdit ? (
            <MessageEditBubble
              attachments={message.attachments}
              initialContent={editContent ?? visible}
              pending={rewindAction?.pending ?? false}
              t={t}
              onCancel={onCancelEdit}
              onSubmit={onSubmitEdit}
            />
          ) : visible ? (
            <div className="max-w-[540px] min-w-0 rounded-xl bg-[var(--raised)] px-3 py-2 text-[14px] leading-5">
              <Markdown text={visible} compact />
            </div>
          ) : null}
          {!editing && (
            <MessageFooter
              alignRight
              content={copyContent}
              rewindAction={rewindAction}
              locale={locale}
              t={t}
              timestamp={message.createdAt}
            />
          )}
        </article>
      </MessageContextMenu>
    )
  }
  if (message.role === 'system') {
    return (
      <div className="flex justify-center">
        <div
          className={cn(
            'rounded-full px-2.5 py-1 text-[11px] leading-4',
            message.tone === 'error'
              ? 'bg-destructive/10 text-destructive'
              : 'bg-accent text-[var(--text-tertiary)]',
          )}
          role={message.tone === 'error' ? 'alert' : undefined}
        >
          {visible}
        </div>
      </div>
    )
  }
  return (
    <MessageContextMenu content={copyContent} forkAction={forkAction} t={t}>
      <article className="group/message min-w-0 py-1">
        <Markdown streaming={message.streaming} text={visible} />
        {beforeFooter && <div className="mb-[3px] mt-3 w-full">{beforeFooter}</div>}
        {footer && (
          <MessageFooter
            content={footer.content}
            forkAction={forkAction}
            locale={locale}
            t={t}
            timestamp={footer.timestamp}
          />
        )}
      </article>
    </MessageContextMenu>
  )
}

function MessageEditBubble({
  initialContent,
  attachments,
  pending,
  t,
  onCancel,
  onSubmit,
}: {
  initialContent: string
  attachments: T.Attachment[]
  pending: boolean
  t: Translator
  onCancel: () => void
  onSubmit: (prompt: string) => Promise<void>
}) {
  const [content, setContent] = useState(initialContent)
  const input = useRef<HTMLTextAreaElement>(null)
  const canSubmit = Boolean(content.trim() || attachments.length)

  function resizeInput(element: HTMLTextAreaElement) {
    element.style.height = '0px'
    element.style.height = `${Math.min(200, Math.max(40, element.scrollHeight))}px`
  }

  useEffect(() => {
    if (input.current) resizeInput(input.current)
  }, [])

  function submit() {
    if (!canSubmit || pending) return
    void onSubmit(content)
  }

  return (
    <form
      className="w-full max-w-[540px] rounded-xl bg-[var(--raised)] px-3 pb-2 pt-[9px]"
      onSubmit={(event) => {
        event.preventDefault()
        submit()
      }}
    >
      <textarea
        aria-label={t('transcript.edit_message')}
        autoFocus
        className="block min-h-10 w-full resize-none overflow-y-auto bg-transparent text-[14px] leading-5 outline-none placeholder:text-[var(--text-ghost)]"
        disabled={pending}
        ref={input}
        rows={1}
        value={content}
        onChange={(event) => {
          setContent(event.target.value)
          resizeInput(event.target)
        }}
        onKeyDown={(event) => {
          if (event.key === 'Escape') {
            event.preventDefault()
            if (!pending) onCancel()
          } else if (
            event.key === 'Enter'
            && !event.shiftKey
            && !event.nativeEvent.isComposing
          ) {
            event.preventDefault()
            submit()
          }
        }}
      />
      <div className="mt-[7px] flex justify-end gap-1.5">
        <button
          className="h-[26px] rounded-[7px] border bg-card px-2.5 text-[11.5px] text-[var(--text-secondary)] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring disabled:opacity-45"
          disabled={pending}
          type="button"
          onClick={onCancel}
        >
          {t('common.cancel')}
        </button>
        <button
          className="flex h-[26px] items-center gap-1.5 rounded-[7px] bg-foreground px-[11px] text-[11.5px] font-medium text-background outline-none hover:opacity-90 focus-visible:ring-1 focus-visible:ring-ring disabled:bg-accent disabled:text-[var(--text-ghost)] disabled:opacity-100"
          disabled={!canSubmit || pending}
          type="submit"
        >
          {pending && <WakuIcon className="size-3 motion-safe:animate-spin" name="loaderCircle" />}
          {t('common.send')}
        </button>
      </div>
    </form>
  )
}

function MessageFooter({
  content,
  locale,
  t,
  timestamp,
  alignRight = false,
  forkAction,
  rewindAction,
}: {
  content: string
  locale: AppLocale
  t: Translator
  timestamp: number
  alignRight?: boolean
  forkAction?: ResponseForkAction
  rewindAction?: MessageRewindAction
}) {
  const [copied, setCopied] = useState(false)
  const copiedTimeout = useRef<number | null>(null)
  useEffect(() => () => {
    if (copiedTimeout.current !== null) window.clearTimeout(copiedTimeout.current)
  }, [])
  return (
    <div className={cn(
      'flex h-[27px] items-center gap-px text-[11.5px] text-[var(--text-ghost)] opacity-0 transition-opacity group-hover/message:opacity-100 group-focus-within/message:opacity-100 motion-reduce:transition-none',
      alignRight && 'justify-end',
      !alignRight && '-ml-[7px]',
    )}>
      {alignRight && <span className="flex h-[27px] items-center px-1">{formatMessageTime(timestamp, undefined, locale)}</span>}
      <button
        aria-label={t(copied ? 'common.copied' : 'common.copy_message')}
        className="grid size-[27px] place-items-center rounded-lg outline-none hover:bg-accent hover:text-[var(--text-secondary)] focus-visible:opacity-100 focus-visible:ring-1 focus-visible:ring-ring"
        title={t(copied ? 'common.copied' : 'common.copy_message')}
        type="button"
        onClick={() => {
          void navigator.clipboard.writeText(content)
          setCopied(true)
          if (copiedTimeout.current !== null) window.clearTimeout(copiedTimeout.current)
          copiedTimeout.current = window.setTimeout(() => {
            setCopied(false)
            copiedTimeout.current = null
          }, 2_000)
        }}
      >
        <WakuIcon className="size-3.5" name={copied ? 'check' : 'copy'} />
      </button>
      {alignRight && rewindAction && (
        <button
          aria-label={t(rewindAction.pending ? 'session.reverting_message' : 'session.revert_to_here')}
          className="grid size-[27px] place-items-center rounded-lg outline-none hover:bg-accent hover:text-[var(--text-secondary)] focus-visible:opacity-100 focus-visible:ring-1 focus-visible:ring-ring disabled:opacity-45"
          disabled={rewindAction.pending}
          title={t(rewindAction.pending ? 'session.reverting_message' : 'session.revert_to_here')}
          type="button"
          onClick={rewindAction.onBegin}
        >
          <WakuIcon
            className={cn('size-3.5', rewindAction.pending && 'motion-safe:animate-spin')}
            name={rewindAction.pending ? 'loaderCircle' : 'rewind'}
          />
        </button>
      )}
      {!alignRight && forkAction && (
        <button
          aria-label={t(forkAction.pending ? 'session.forking_task' : 'session.fork_task')}
          className="grid size-[27px] place-items-center rounded-lg outline-none hover:bg-accent hover:text-[var(--text-secondary)] focus-visible:opacity-100 focus-visible:ring-1 focus-visible:ring-ring disabled:opacity-45"
          disabled={forkAction.pending}
          title={t(forkAction.pending ? 'session.forking_task' : 'session.fork_task')}
          type="button"
          onClick={() => forkAction.onFork(forkAction.runId)}
        >
          <WakuIcon
            className={cn('size-3.5', forkAction.pending && 'motion-safe:animate-spin')}
            name={forkAction.pending ? 'loaderCircle' : 'fork'}
          />
        </button>
      )}
      {!alignRight && <span className="flex h-[27px] items-center px-1">{formatMessageTime(timestamp, undefined, locale)}</span>}
    </div>
  )
}

function MessageContextMenu({
  children,
  content,
  t,
  copyToComposer,
  forkAction,
  rewindAction,
}: {
  children: ReactNode
  content: string
  t: Translator
  copyToComposer?: () => void
  forkAction?: ResponseForkAction
  rewindAction?: MessageRewindAction
}) {
  const code = fencedCode(content)
  const [selectedText, setSelectedText] = useState('')
  return (
    <ContextMenu.Root onOpenChange={(open) => {
      if (open) setSelectedText(window.getSelection()?.toString() ?? '')
    }}>
      <ContextMenu.Trigger className="block w-full min-w-0 outline-none">
        {children}
      </ContextMenu.Trigger>
      <ContextMenu.Portal>
        <ContextMenu.Positioner className="z-[100] outline-none">
          <ContextMenu.Popup
            className="waku-menu-surface"
            finalFocus={false}
          >
            {selectedText && (
              <ContextMenu.Item
                className="waku-menu-item"
                onClick={() => void navigator.clipboard.writeText(selectedText)}
              >
                <WakuIcon className="size-3" name="copy" /> {t('common.copy_selection')}
              </ContextMenu.Item>
            )}
            <ContextMenu.Item
              className="waku-menu-item"
              onClick={() => void navigator.clipboard.writeText(content)}
            >
              <WakuIcon className="size-3" name="copy" /> {t('common.copy_message_title')}
            </ContextMenu.Item>
            {copyToComposer && (
              <ContextMenu.Item
                className="waku-menu-item"
                onClick={copyToComposer}
              >
                <WakuIcon className="size-3" name="compose" /> {t('common.copy_to_composer')}
              </ContextMenu.Item>
            )}
            {code && (
              <ContextMenu.Item
                className="waku-menu-item"
                onClick={() => void navigator.clipboard.writeText(code)}
              >
                <WakuIcon className="size-3" name="copy" /> {t('common.copy_code')}
              </ContextMenu.Item>
            )}
            {rewindAction && (
              <>
                <ContextMenu.Separator className="waku-menu-separator" />
                <ContextMenu.Item
                  className="waku-menu-item"
                  disabled={rewindAction.pending}
                  onClick={rewindAction.onBegin}
                >
                  <WakuIcon
                    className={cn('size-3', rewindAction.pending && 'motion-safe:animate-spin')}
                    name={rewindAction.pending ? 'loaderCircle' : 'rewind'}
                  />
                  {t(rewindAction.pending ? 'session.reverting_message_title' : 'session.revert_to_here_title')}
                </ContextMenu.Item>
              </>
            )}
            {forkAction && (
              <>
                <ContextMenu.Separator className="waku-menu-separator" />
                <ContextMenu.Item
                  className="waku-menu-item"
                  disabled={forkAction.pending}
                  onClick={() => forkAction.onFork(forkAction.runId)}
                >
                  <WakuIcon
                    className={cn('size-3', forkAction.pending && 'motion-safe:animate-spin')}
                    name={forkAction.pending ? 'loaderCircle' : 'fork'}
                  />
                  {t(forkAction.pending ? 'session.forking_task_title' : 'session.fork_task_title')}
                </ContextMenu.Item>
              </>
            )}
          </ContextMenu.Popup>
        </ContextMenu.Positioner>
      </ContextMenu.Portal>
    </ContextMenu.Root>
  )
}

function Markdown({
  text,
  compact = false,
  streaming = false,
}: {
  text: string
  compact?: boolean
  streaming?: boolean
}) {
  const onOpenLink = useContext(TranscriptLinkContext)
  // Match the painter's attach semantics: text already present when this row
  // mounts is the baseline; only later appends dissolve in.
  const veil = useRef(createMarkdownVeilState(text))
  const now = Date.now()
  const chunks = advanceMarkdownVeil(veil.current, text, streaming, now)
  return (
    <div className={cn('markdown min-w-0', compact && '[&>*:first-child]:mt-0 [&>*:last-child]:mb-0')}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={chunks.length ? [markdownVeilPlugin(chunks, now)] : []}
        components={{
          a: ({ children, href, ...props }) => (
            <a
              {...props}
              href={href}
              target="_blank"
              rel="noreferrer noopener"
              onClick={(event) => {
                if (href && onOpenLink(href)) event.preventDefault()
              }}
            >
              {children}
            </a>
          ),
          img: ({ alt, src }) => typeof src === 'string' ? (
            <PreviewableImage
              buttonClassName="max-w-full rounded-[9px] border bg-[var(--inset)]"
              imageClassName="max-h-64 max-w-full object-contain"
              name={alt || imageName(src)}
              source={src}
            />
          ) : null,
        }}
      >
        {text}
      </ReactMarkdown>
    </div>
  )
}

function ActivityGroup({
  activities,
  backgroundWork,
  liveGroup,
  t,
  taskId,
  onOpenBackgroundWork,
}: {
  activities: ActivityView[]
  backgroundWork: BackgroundWorkItem[]
  liveGroup: boolean
  t: Translator
  taskId: string
  onOpenBackgroundWork?: (id: string) => void
}) {
  const [expanded, setExpanded] = useState(liveGroup)
  useEffect(() => {
    setExpanded(liveGroup)
  }, [liveGroup])
  if (!activities.length) return null
  return (
    <div className="min-w-0 text-[12px] text-[var(--text-tertiary)]">
      <button
        aria-expanded={expanded}
        className="flex h-7 w-full min-w-0 items-center gap-1.5 rounded outline-none hover:text-foreground focus-visible:ring-1 focus-visible:ring-ring"
        type="button"
        onClick={() => setExpanded((value) => !value)}
      >
        <span className="min-w-0 truncate text-left text-[12.5px] font-medium text-[var(--text-secondary)]">{activityHeaderTitle(activities, liveGroup, t)}</span>
        <WakuIcon className="size-2.5 shrink-0" name={expanded ? 'chevronDown' : 'chevronRight'} />
      </button>
      {expanded && (
        <div className="ml-1.5 flex min-w-0 flex-col gap-2 border-l pb-0.5 pl-3">
          {activities.map((activity) => (
            <ActivityRow
              activity={activity}
              backgroundWork={backgroundWork.find((item) => item.itemId === activity.id)}
              key={activity.id}
              t={t}
              taskId={taskId}
              onOpenBackgroundWork={onOpenBackgroundWork}
            />
          ))}
        </div>
      )}
    </div>
  )
}

/**
 * The activity a row shows: a command's output from its `commandOutput`
 * stream while expanded, and a body the wire projection left out from the
 * stored turn item.
 */
function useActivityDetail(taskId: string, activity: ActivityView, expanded: boolean): ActivityView {
  const output = useCommandOutput(taskId, activity.id, expanded && activity.streamsOutput)
  const full = useTurnItem(taskId, activity.id, expanded && activity.omitted && !activity.streamsOutput)
  if (activity.streamsOutput) {
    return output?.text ? { ...activity, output: output.text } : activity
  }
  const stored = full.data ? activityFromItem(full.data) : undefined
  return stored ? { ...stored, complete: activity.complete, failed: activity.failed } : activity
}

function ActivityRow({
  activity: summary,
  backgroundWork,
  t,
  taskId,
  onOpenBackgroundWork,
}: {
  activity: ActivityView
  backgroundWork?: BackgroundWorkItem
  t: Translator
  taskId: string
  onOpenBackgroundWork?: (id: string) => void
}) {
  const [expanded, setExpanded] = useState(Boolean(summary.reasoning && !summary.complete))
  const activity = useActivityDetail(taskId, summary, expanded)
  const sections = activity.reasoning ? [] : activityDisclosureSections(activity, t)
  const reasoningContent = activity.reasoning?.content.trim() ?? ''
  const hasDetail = Boolean(reasoningContent || sections.length || summary.omitted || summary.streamsOutput)
  const iconName = activityIcon(activity)
  const preview = expanded || activity.reasoning ? '' : activityPreview(activity, t)
  const actionLabel = activityActionLabel(activity, t)
  const rowDetail = activityRowDetail(activity, t) || preview
  const fileStats = activityFileChangeStats(activity)
  const scrollContent = reasoningContent || (activity.kind === 'command'
    ? sections.find((section) => section.kind === 'output')?.content ?? ''
    : '')
  const detailScroll = useRef<HTMLDivElement>(null)
  const detailFollowsTail = useRef(true)
  const [detailEdges, setDetailEdges] = useState({ atTop: true, atBottom: true })
  const updateDetailEdges = useCallback((viewport: HTMLDivElement) => {
    const atTop = viewport.scrollTop <= 1
    const atBottom = viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight <= 1
    detailFollowsTail.current = atBottom
    setDetailEdges((current) => (
      current.atTop === atTop && current.atBottom === atBottom
        ? current
        : { atTop, atBottom }
    ))
  }, [])
  useEffect(() => {
    if (!expanded || !scrollContent) return
    const viewport = detailScroll.current
    if (!viewport) return
    if (!activity.complete && detailFollowsTail.current) {
      viewport.scrollTop = viewport.scrollHeight
    }
    updateDetailEdges(viewport)
  }, [activity.complete, expanded, scrollContent, updateDetailEdges])
  return (
    <div className="min-w-0 overflow-hidden rounded-[9px] border bg-[var(--activity-surface)]">
      <div className="flex h-7 w-full min-w-0 items-center">
        <button
          aria-expanded={hasDetail ? expanded : undefined}
          className={cn(
            'flex h-7 min-w-0 flex-1 items-center gap-2 px-2 text-left outline-none',
            expanded ? 'rounded-t-[8px]' : 'rounded-[8px]',
            hasDetail && 'hover:bg-[var(--activity-hover-surface)] active:bg-[var(--activity-active-surface)] focus-visible:bg-[var(--activity-hover-surface)] focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring',
          )}
          disabled={!hasDetail}
          type="button"
          onClick={() => setExpanded((value) => !value)}
        >
          <WakuIcon className="size-3 shrink-0 text-[var(--text-tertiary)]" name={iconName} />
          <span className="shrink-0 font-semibold text-[var(--text-secondary)]">{actionLabel}</span>
          {rowDetail && (
            <span className="min-w-0 flex-1 truncate text-[var(--text-secondary)]">
              {rowDetail}
            </span>
          )}
          {fileStats && (
            <>
              <span className="shrink-0 text-[var(--success)]">+{fileStats.additions}</span>
              <span className="shrink-0 text-destructive">-{fileStats.deletions}</span>
            </>
          )}
          <ActivityState activity={activity} expanded={expanded} hasDetail={hasDetail} t={t} />
        </button>
        {backgroundWork && onOpenBackgroundWork && (
          <button
            className={cn(
              'mr-2 flex h-5 shrink-0 items-center rounded-[5px] border px-1.5 text-[9.5px] outline-none hover:bg-accent focus-visible:border-ring',
              backgroundWorkStatusClass(backgroundWork.status),
            )}
            type="button"
            onClick={(event) => {
              event.stopPropagation()
              onOpenBackgroundWork(backgroundWork.id)
            }}
          >
            {t(backgroundWorkStatusKey(backgroundWork.status))}
          </button>
        )}
      </div>
      {expanded && hasDetail && (
        activity.reasoning ? (
          <div className="border-t">
            <ActivityScrollableContent
              className="px-3 py-2 text-[var(--text-secondary)]"
              edges={detailEdges}
              viewportRef={detailScroll}
              onScroll={updateDetailEdges}
            >
              <Markdown streaming={!activity.complete} text={reasoningContent} />
            </ActivityScrollableContent>
          </div>
        ) : (
          <div className="flex min-w-0 flex-col gap-2 overflow-hidden border-t px-3 py-2 font-mono text-[10.5px] leading-4 text-[var(--text-secondary)]">
            {sections.map((section) => (
              <ActivitySection
                edges={detailEdges}
                key={section.kind}
                scrollable={activity.kind === 'command' && section.kind === 'output'}
                section={section}
                t={t}
                viewportRef={detailScroll}
                onScroll={updateDetailEdges}
              />
            ))}
            {activity.imageUrls.map((url, index) => (
              <ActivityImage key={`${url}-${index}`} reference={url} t={t} />
            ))}
          </div>
        )
      )}
    </div>
  )
}

type ActivityScrollEdges = { atTop: boolean; atBottom: boolean }

function ActivityScrollableContent({
  children,
  className,
  edges,
  viewportRef,
  onScroll,
}: {
  children: ReactNode
  className?: string
  edges: ActivityScrollEdges
  viewportRef: RefObject<HTMLDivElement | null>
  onScroll: (viewport: HTMLDivElement) => void
}) {
  return (
    <div className="relative min-w-0 overflow-hidden">
      <div
        className={cn(
          'max-h-[400px] min-w-0 overflow-y-auto overscroll-contain',
          className,
        )}
        onScroll={(event) => onScroll(event.currentTarget)}
        ref={viewportRef}
      >
        {children}
      </div>
      <div
        aria-hidden="true"
        className={cn(
          'pointer-events-none absolute left-0 right-2 top-0 h-5 bg-gradient-to-b from-[var(--activity-surface)] to-transparent transition-opacity motion-reduce:transition-none',
          edges.atTop ? 'opacity-0' : 'opacity-100',
        )}
      />
      <div
        aria-hidden="true"
        className={cn(
          'pointer-events-none absolute bottom-0 left-0 right-2 h-5 bg-gradient-to-t from-[var(--activity-surface)] to-transparent transition-opacity motion-reduce:transition-none',
          edges.atBottom ? 'opacity-0' : 'opacity-100',
        )}
      />
    </div>
  )
}

function backgroundWorkStatusClass(status: BackgroundWorkStatus) {
  if (status === 'pending' || status === 'running') return 'text-ring'
  if (status === 'completed') return 'text-[var(--success)]'
  if (status === 'failed') return 'text-destructive'
  return 'text-[var(--text-tertiary)]'
}

function backgroundWorkStatusKey(status: BackgroundWorkStatus) {
  return `background.status.${status === 'pending' ? 'starting' : status}`
}

function ActivitySection({
  edges,
  scrollable,
  section,
  t,
  viewportRef,
  onScroll,
}: {
  edges: ActivityScrollEdges
  scrollable: boolean
  section: ReturnType<typeof activityDisclosureSections>[number]
  t: Translator
  viewportRef: RefObject<HTMLDivElement | null>
  onScroll: (viewport: HTMLDivElement) => void
}) {
  const [copied, setCopied] = useState(false)
  const copiedTimeout = useRef<number | null>(null)
  useEffect(() => () => {
    if (copiedTimeout.current !== null) window.clearTimeout(copiedTimeout.current)
  }, [])
  return (
    <div className="flex min-w-0 flex-col gap-[3px]">
      {section.label && (
        <div className="flex h-5 items-center justify-between font-medium">
          <span>{section.label}</span>
          {section.content && (
            <button
              aria-label={copied
                ? t('common.copied')
                : t('common.copy_named', { name: section.label })}
              className="grid size-5 place-items-center rounded-[5px] text-[var(--text-ghost)] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring"
              type="button"
              onClick={() => {
                void navigator.clipboard.writeText(section.content)
                setCopied(true)
                if (copiedTimeout.current !== null) window.clearTimeout(copiedTimeout.current)
                copiedTimeout.current = window.setTimeout(() => {
                  setCopied(false)
                  copiedTimeout.current = null
                }, 2_000)
              }}
            >
              <WakuIcon className="size-[11px]" name={copied ? 'check' : 'copy'} />
            </button>
          )}
        </div>
      )}
      {section.content && (
        scrollable ? (
          <ActivityScrollableContent
            className="py-1 pr-2"
            edges={edges}
            viewportRef={viewportRef}
            onScroll={onScroll}
          >
            <pre className="min-w-0 whitespace-pre-wrap break-words font-mono text-[10.5px] leading-4">
              {section.content}
            </pre>
          </ActivityScrollableContent>
        ) : section.kind === 'command' ? (
          <pre className="min-w-0 whitespace-pre-wrap break-words font-mono text-[10.5px] leading-4">
            {section.content}
          </pre>
        ) : (
          <ActivitySectionText content={section.content} label={section.label} t={t} />
        )
      )}
    </div>
  )
}

function ActivitySectionText({
  content,
  label,
  t,
}: {
  content: string
  label: string | null
  t: Translator
}) {
  const rows = activityTextRows(content)
  if (!shouldVirtualizeActivityText(content, rows)) {
    return (
      <pre className="max-h-72 min-w-0 overflow-auto whitespace-pre-wrap break-words font-mono text-[10.5px] leading-4">
        {content}
      </pre>
    )
  }
  return (
    <Virtuoso
      aria-label={label
        ? t('activity.named_content', { name: label })
        : t('activity.detail')}
      className="h-72 min-w-0 overscroll-contain font-mono text-[10.5px] leading-4"
      data={rows}
      defaultItemHeight={16}
      increaseViewportBy={128}
      itemContent={(_, row) => (
        <div className="min-h-4 whitespace-pre-wrap break-words">{row || '\u00a0'}</div>
      )}
    />
  )
}

function ActivityState({
  activity,
  expanded,
  hasDetail,
  t,
}: {
  activity: ActivityView
  expanded: boolean
  hasDetail: boolean
  t: Translator
}) {
  if (hasDetail) return <WakuIcon className="size-2.5 text-[var(--text-tertiary)]" name={expanded ? 'chevronDown' : 'chevronRight'} />
  if (activity.reasoning) return null
  if (activity.failed) return <WakuIcon label={t('background.status.failed')} className="size-3 text-destructive" name="alert" />
  if (activity.complete) return null
  return <span aria-label={t('background.status.running')} className="size-1.5 rounded-full bg-ring motion-safe:animate-pulse" role="img" />
}

function ChangedFilesCard({
  files,
  additions,
  deletions,
  t,
  onReview,
}: {
  files: Array<{ path: string; additions: number; deletions: number }>
  additions: number
  deletions: number
  t: Translator
  onReview?: () => void
}) {
  const [expanded, setExpanded] = useState(false)
  const shown = expanded ? files.slice(0, 12) : files.slice(0, 3)
  const clipped = expanded && files.length > 12
  return (
    <div className="overflow-hidden rounded-xl border bg-accent">
      <div className="flex min-h-[58px] items-center gap-2.5 px-3 py-[9px]">
        <span className="grid size-9 shrink-0 place-items-center rounded-[9px] bg-[var(--raised)]">
          <WakuIcon className="size-4 text-[var(--text-tertiary)]" name="fileDiff" />
        </span>
        <div className="min-w-0 flex-1">
          <div className="truncate text-[12.5px] font-medium">
            {t(files.length === 1 ? 'transcript.changed_file' : 'transcript.changed_files', {
              count: files.length,
            })}
          </div>
          <div className="mt-0.5 flex gap-1.5 text-[11px] leading-[14px]">
            <span className="text-[var(--success)]">+{additions}</span>{' '}
            <span className="text-destructive">-{deletions}</span>
          </div>
        </div>
        {onReview && (
          <button
            className="flex h-7 items-center gap-1 rounded-[7px] border bg-card px-2.5 text-[11.5px] font-medium text-[var(--text-secondary)] outline-none hover:bg-[var(--raised)] focus-visible:border-ring"
            type="button"
            onClick={onReview}
          >
            <WakuIcon className="size-3 text-[var(--text-tertiary)]" name="fileDiff" />
            {t('transcript.review_changes')}
          </button>
        )}
      </div>
      <div className="border-t">
        {shown.map((file) => (
          <div className="flex h-[31px] items-center gap-2 px-3 text-[11.5px]" key={file.path}>
            <span className="min-w-0 flex-1 truncate text-[var(--text-secondary)]" title={file.path}>{file.path}</span>
            <span className="text-[10.5px] text-[var(--success)]">+{file.additions}</span>
            <span className="text-[10.5px] text-destructive">-{file.deletions}</span>
          </div>
        ))}
        {files.length > 3 && (
          <button
            className="flex h-[34px] w-full items-center gap-1.5 border-t px-3 text-left text-[11.5px] font-medium text-[var(--text-secondary)] outline-none hover:bg-[var(--raised)] focus-visible:bg-[var(--raised)]"
            type="button"
            onClick={() => setExpanded((value) => !value)}
          >
            <span>{expanded
              ? t('transcript.show_fewer_files')
              : t('transcript.show_more_files', { count: files.length - 3 })}</span>
            {clipped && (
              <span className="min-w-0 flex-1 truncate font-normal text-[var(--text-ghost)]">
                {t('transcript.showing_first_files', { count: 12, total: files.length })}
              </span>
            )}
            <span className="flex-1" />
            <WakuIcon className="size-[11px] text-[var(--text-tertiary)]" name={expanded ? 'chevronDown' : 'chevronRight'} />
          </button>
        )}
      </div>
    </div>
  )
}

function Attachment({ attachment }: { attachment: T.Attachment }) {
  if (attachment.isImage) return <RemoteImage attachment={attachment} />
  return (
    <span
      className="flex h-20 w-24 flex-col items-center justify-center gap-[7px] overflow-hidden rounded-[9px] border bg-[var(--inset)] px-[7px]"
      title={attachment.name}
    >
      {attachment.isDir
        ? <WakuIcon className="size-[18px] text-[var(--text-tertiary)]" name="folder" />
        : <FileTypeIcon className="size-[18px]" path={attachment.mention || attachment.name} />}
      <span className="w-full truncate text-center text-[9.5px] text-[var(--text-secondary)]">
        {attachment.name}
      </span>
    </span>
  )
}

function RemoteImage({
  attachment,
  tile = true,
}: {
  attachment: T.Attachment
  tile?: boolean
}) {
  const { connection, phase } = useDaemon()
  const [source, setSource] = useState<string | null>(null)
  useEffect(() => {
    if (phase !== 'connected' || !connection || !attachment.blobReference) {
      setSource(null)
      return
    }
    let active = true
    void readAttachmentImage(connection, attachment)
      .then((value) => active && setSource(value))
      .catch(() => active && setSource(null))
    return () => { active = false }
    // The attachment's identity is its reference, path and name.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connection, phase, attachment.blobReference, attachment.path, attachment.name])

  if (!source) {
    return (
      <span
        className={cn(
          'grid place-items-center rounded-[9px] border bg-[var(--inset)]',
          tile ? 'h-20 w-24' : 'h-24 w-full',
        )}
        title={attachment.name}
      >
        <FileTypeIcon className="size-[18px] opacity-60" path={attachment.name} />
      </span>
    )
  }
  return (
    <PreviewableImage
      buttonClassName={cn(
        'rounded-[9px] border bg-[var(--inset)] focus-visible:border-ring',
        tile ? 'h-20 w-24' : 'max-w-full',
      )}
      imageClassName={tile ? 'size-full object-cover' : 'max-h-64 max-w-full object-contain'}
      name={attachment.name}
      source={source}
    />
  )
}

function ActivityImage({ reference, t }: { reference: string; t: Translator }) {
  if (reference.startsWith('data:') || reference.startsWith('https://')) {
    return (
      <PreviewableImage
        buttonClassName="max-w-full rounded-lg border"
        imageClassName="max-h-64 max-w-full object-contain"
        name={t('activity.image_output')}
        source={reference}
      />
    )
  }
  return (
    <RemoteImage
      tile={false}
      attachment={{
        path: reference,
        mention: reference,
        name: reference,
        isDir: false,
        isImage: true,
        blobReference: reference,
      }}
    />
  )
}

function activityIcon(activity: ActivityView): WakuIconName {
  if (activity.reasoning || activity.kind === 'reasoning') return 'sparkle'
  if (activity.kind === 'command') return 'terminal'
  if (activity.kind === 'search' || activity.kind === 'fileSearch') return 'search'
  if (activity.kind === 'fileRead') return 'file'
  if (activity.kind === 'fileChange') return 'pencil'
  if (activity.kind === 'fileList') return 'folder'
  if (activity.kind === 'plan') return 'list'
  return 'wrench'
}

function imageName(source: string) {
  if (source.startsWith('data:')) return 'Image'
  try {
    const path = new URL(source, window.location.href).pathname
    return decodeURIComponent(path.split('/').filter(Boolean).at(-1) ?? 'Image')
  } catch {
    return source.split('/').filter(Boolean).at(-1) ?? 'Image'
  }
}
