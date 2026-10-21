import { Navigate, createFileRoute, useNavigate } from '@tanstack/react-router'
import { ConnectionPanel } from '@/components/connection-panel'
import { StartupScreen } from '@/components/startup-screen'
import {
  SettingsView,
  SETTINGS_PAGES,
  isSettingsPageId,
  type SettingsPageId,
} from '@/components/settings-view'
import { useShell } from '@/hooks/use-daemon-stores'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useDaemon } from '@/lib/daemon-context'
import { useI18n } from '@/lib/i18n'

export const Route = createFileRoute('/settings/$page')({
  validateSearch: (search: Record<string, unknown>) => ({
    session: typeof search.session === 'string' ? search.session : undefined,
  }),
  component: SettingsPageRoute,
})

function SettingsPageRoute() {
  const { page } = Route.useParams()
  const search = Route.useSearch()
  if (!isSettingsPageId(page)) {
    return (
      <Navigate
        params={{ page: 'general' }}
        replace
        search={search}
        to="/settings/$page"
      />
    )
  }
  return <SettingsApp page={page} sessionId={search.session} />
}

function SettingsApp({
  page,
  sessionId,
}: {
  page: SettingsPageId
  sessionId?: string
}) {
  const { t } = useI18n()
  const { phase } = useDaemon()
  const pageMetadata = SETTINGS_PAGES.find((candidate) => candidate.id === page)
  const title = pageMetadata ? t(pageMetadata.labelKey) : t('common.settings')
  // A reconnect keeps the page; the streams resume underneath it.
  if (phase !== 'connected' && phase !== 'reconnecting') return <ConnectionPanel title={title} />
  return <ConnectedSettings page={page} sessionId={sessionId} />
}

function ConnectedSettings({
  page,
  sessionId,
}: {
  page: SettingsPageId
  sessionId?: string
}) {
  const { t } = useI18n()
  const navigate = useNavigate({ from: '/settings/$page' })
  const { reconnect } = useDaemon()
  const shell = useShell()
  const pageMetadata = SETTINGS_PAGES.find((candidate) => candidate.id === page)
  useDocumentTitle(pageMetadata ? t(pageMetadata.labelKey) : t('common.settings'))
  if (!shell.ready) {
    return <StartupScreen error={shell.error ? errorMessage(shell.error) : undefined} onRetry={() => void reconnect().catch(() => {})} />
  }

  return (
    <SettingsView
      page={page}
      projects={shell.projects}
      onBack={() => void navigate({
        to: '/',
        search: { session: sessionId },
      })}
      onPageChange={(next) => void navigate({
        to: '/settings/$page',
        params: { page: next },
        search: { session: sessionId },
      })}
    />
  )
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}
