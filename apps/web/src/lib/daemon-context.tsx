import { useQueryClient } from '@tanstack/react-query'
import {
  DaemonConnection,
  DraftsStore,
  ProvidersStore,
  SettingsStore,
  ShellStore,
  type ConnectionState,
  type Welcome,
} from '@waku/client/v8'
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import {
  clearStoredConnection,
  loadStoredConnection,
  storeConnection,
  validateConnectionConfig,
  type ConnectionConfig,
} from './connection'
import { translate, useI18n } from './i18n'
import { ThreadStores } from './thread-stores'

export type ConnectionPhase =
  | 'booting'
  | 'disconnected'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'error'

/** The streams every screen shares, each subscribed once per connection. */
export interface DaemonStores {
  shell: ShellStore
  settings: SettingsStore
  providers: ProvidersStore
  drafts: DraftsStore
  threads: ThreadStores
}

interface DaemonContextValue {
  connection: DaemonConnection | null
  stores: DaemonStores | null
  config: ConnectionConfig | null
  phase: ConnectionPhase
  error: string | null
  welcome: Welcome | null
  connect: (config: ConnectionConfig) => Promise<void>
  reconnect: () => Promise<void>
  disconnect: () => void
  forget: () => void
}

const DaemonContext = createContext<DaemonContextValue | null>(null)

const INSTANCE_KEY = 'waku.instanceId'

/** Stable per browser profile, so the daemon recognizes a reconnect. */
function instanceId(): string {
  try {
    const stored = window.localStorage.getItem(INSTANCE_KEY)
    if (stored) return stored
    const created = crypto.randomUUID()
    window.localStorage.setItem(INSTANCE_KEY, created)
    return created
  } catch {
    return crypto.randomUUID()
  }
}

export function createDaemonConnection(
  config: ConnectionConfig,
  options: { retryInitialFailure?: boolean } = {},
): DaemonConnection {
  return new DaemonConnection({
    url: config.address,
    credential: { type: 'local', token: config.token },
    client: {
      kind: 'web',
      appVersion: '0.1.0',
      os: typeof navigator === 'undefined' ? 'unknown' : navigator.platform || 'web',
      instanceId: instanceId(),
    },
    retryInitialFailure: options.retryInitialFailure,
  })
}

function createStores(connection: DaemonConnection): DaemonStores {
  return {
    shell: new ShellStore(connection, { location: 'active' }).start(),
    settings: new SettingsStore(connection).start(),
    providers: new ProvidersStore(connection).start(),
    drafts: new DraftsStore(connection).start(),
    threads: new ThreadStores(connection),
  }
}

function closeStores(stores: DaemonStores | null) {
  if (!stores) return
  stores.shell.close()
  stores.settings.close()
  stores.providers.close()
  stores.drafts.close()
  stores.threads.close()
}

function phaseOf(state: ConnectionState): ConnectionPhase {
  switch (state.phase) {
    case 'connected':
      return 'connected'
    case 'reconnecting':
      return 'reconnecting'
    case 'connecting':
      return 'connecting'
    case 'failed':
      return 'error'
    default:
      return 'disconnected'
  }
}

export function DaemonProvider({ children }: { children: ReactNode }) {
  const { locale } = useI18n()
  const queryClient = useQueryClient()
  const [connection, setConnection] = useState<DaemonConnection | null>(null)
  const [stores, setStores] = useState<DaemonStores | null>(null)
  const [config, setConfig] = useState<ConnectionConfig | null>(null)
  const [phase, setPhase] = useState<ConnectionPhase>('booting')
  const [error, setError] = useState<string | null>(null)
  const [welcome, setWelcome] = useState<Welcome | null>(null)
  const generation = useRef(0)
  const bootstrapped = useRef(false)
  const current = useRef<{ connection: DaemonConnection; stores: DaemonStores } | null>(null)

  const teardown = useCallback(() => {
    const previous = current.current
    current.current = null
    if (!previous) return
    closeStores(previous.stores)
    previous.connection.close()
  }, [])

  const open = useCallback(
    async (candidate: ConnectionConfig, retryInitialFailure = false) => {
      const normalized = validateConnectionConfig(
        candidate,
        (key) => translate(locale, key),
      )
      const attempt = ++generation.current
      teardown()
      queryClient.clear()
      setPhase('connecting')
      setError(null)
      setConfig(normalized)

      const next = createDaemonConnection(normalized, { retryInitialFailure })
      const nextStores = createStores(next)
      current.current = { connection: next, stores: nextStores }
      let wasConnected = false
      next.onState((state) => {
        if (generation.current !== attempt) return
        setPhase(phaseOf(state))
        setError(state.phase === 'failed' || state.phase === 'reconnecting'
          ? state.error?.message ?? null
          : null)
        if (state.welcome) setWelcome(state.welcome)
        if (state.phase === 'connected') {
          // Catalog reads are not streams; refetch them after a gap.
          if (wasConnected) void queryClient.invalidateQueries()
          wasConnected = true
        }
      })
      setConnection(next)
      setStores(nextStores)
      try {
        await next.start()
        if (generation.current !== attempt) return
        storeConnection(normalized)
      } catch (cause) {
        if (generation.current !== attempt) return
        const message = cause instanceof Error ? cause.message : String(cause)
        setError(message)
        setPhase('error')
        throw cause
      }
    },
    [locale, queryClient, teardown],
  )

  useEffect(() => {
    if (bootstrapped.current) return
    bootstrapped.current = true
    const stored = loadStoredConnection()
    if (!stored) {
      setPhase('disconnected')
      return
    }
    // A stored daemon may still be starting; keep trying instead of failing.
    void open(stored, true).catch(() => {})
    // Storage is read once at browser startup.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // A network change or the tab coming back cuts the reconnect backoff short.
  useEffect(() => {
    if (!connection) return
    const retry = () => connection.reconnectNow()
    const visible = () => {
      if (document.visibilityState === 'visible') retry()
    }
    window.addEventListener('online', retry)
    document.addEventListener('visibilitychange', visible)
    return () => {
      window.removeEventListener('online', retry)
      document.removeEventListener('visibilitychange', visible)
    }
  }, [connection])

  const reconnect = useCallback(async () => {
    if (!config) throw new Error(translate(locale, 'web.daemon_not_configured'))
    if (connection && connection.state.phase === 'reconnecting') {
      connection.reconnectNow()
      return
    }
    await open(config)
  }, [config, connection, locale, open])

  const disconnect = useCallback(() => {
    ++generation.current
    teardown()
    setConnection(null)
    setStores(null)
    setWelcome(null)
    setPhase('disconnected')
    setError(null)
    queryClient.clear()
  }, [queryClient, teardown])

  const forget = useCallback(() => {
    disconnect()
    clearStoredConnection()
    setConfig(null)
  }, [disconnect])

  useEffect(() => () => teardown(), [teardown])

  const value: DaemonContextValue = {
    connection,
    stores,
    config,
    phase,
    error,
    welcome,
    connect: open,
    reconnect,
    disconnect,
    forget,
  }

  return <DaemonContext.Provider value={value}>{children}</DaemonContext.Provider>
}

export function useDaemon() {
  const context = useContext(DaemonContext)
  if (!context) throw new Error('useDaemon must be used inside DaemonProvider')
  return context
}

/** The connection and stores, or an error when there are none. */
export function requireConnection(connection: DaemonConnection | null): DaemonConnection {
  if (!connection) throw new Error('Waku daemon is disconnected')
  return connection
}
