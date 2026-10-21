import { lazy, Suspense, useEffect } from 'react'
import { ConnectionPanel } from '@/components/connection-panel'
import { StartupScreen } from '@/components/startup-screen'
import { WakuIcon } from '@/components/waku-icon'
import { useDaemon } from '@/lib/daemon-context'
import { useI18n } from '@/lib/i18n'

const loadConnectedApp = () => import('@/components/waku-app')
const ConnectedWakuApp = lazy(() => loadConnectedApp().then((module) => ({
  default: module.WakuApp,
})))

export function WakuShell() {
  const { config, phase } = useDaemon()

  useEffect(() => {
    if (phase === 'connecting' && config) void loadConnectedApp()
  }, [config, phase])

  // A dropped connection keeps the app on screen: the streams resume from
  // their cursors once the daemon is back.
  if (phase !== 'connected' && phase !== 'reconnecting') return <ConnectionPanel />
  return (
    <>
      <Suspense fallback={<StartupScreen />}>
        <ConnectedWakuApp />
      </Suspense>
      {phase === 'reconnecting' && <ReconnectingBanner />}
    </>
  )
}

function ReconnectingBanner() {
  const { t } = useI18n()
  const { error, reconnect } = useDaemon()
  return (
    <div
      className="fixed left-1/2 top-2 z-[90] flex -translate-x-1/2 items-center gap-2 rounded-full border bg-card px-3 py-1.5 text-[12px] shadow-sm"
      role="status"
      title={error ?? undefined}
    >
      <WakuIcon className="size-3 text-[var(--warning)] motion-safe:animate-spin" name="loaderCircle" />
      <span>{t('daemon.phase_reconnecting')}</span>
      <button
        className="rounded px-1.5 py-0.5 font-medium text-[var(--text-secondary)] outline-none hover:bg-accent focus-visible:ring-1 focus-visible:ring-ring"
        type="button"
        onClick={() => void reconnect().catch(() => {})}
      >
        {t('daemon.reconnect')}
      </button>
    </div>
  )
}
