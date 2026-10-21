import { useQueries, useQuery } from '@tanstack/react-query'
import type * as T from '@waku/client/v8'
import { PROVIDERS } from '@/components/waku-icon'
import { useProviderStatuses, useSettingsStream } from '@/hooks/use-daemon-stores'
import { requireConnection, useDaemon } from '@/lib/daemon-context'
import {
  daemonKeys,
  discoverComposerCommands,
  inspectWorkspaceBranches,
  listComposerFiles,
  loadDaemonInfo,
  loadSkills,
  loadUsageHistory,
  probeProvider,
  rootKey,
  type ProviderCatalog,
} from '@/lib/daemon-api'
import {
  browserProviderProbeStorage,
  PROVIDER_PROBE_CACHE_STALE_TIME,
  readProviderProbeCache,
  writeProviderProbeCache,
} from '@/lib/provider-probe-cache'

function useLive() {
  const { connection, config, phase } = useDaemon()
  return {
    connection,
    address: config?.address ?? 'disconnected',
    live: phase === 'connected' && Boolean(connection && config),
  }
}

export function useDaemonInfo() {
  const { connection, address, live } = useLive()
  return useQuery({
    queryKey: daemonKeys.info(address),
    queryFn: () => loadDaemonInfo(requireConnection(connection)),
    enabled: live,
    staleTime: Number.POSITIVE_INFINITY,
  })
}

export function useWorkspaceBranches(root: T.RootRef | undefined) {
  const { connection, address, live } = useLive()
  return useQuery({
    queryKey: daemonKeys.workspace(address, root ? rootKey(root) : 'none'),
    queryFn: () => inspectWorkspaceBranches(requireConnection(connection), root!),
    enabled: live && Boolean(root),
    staleTime: 5_000,
  })
}

export function useComposerFiles(root: T.RootRef | undefined) {
  const { connection, address, live } = useLive()
  return useQuery({
    queryKey: daemonKeys.composerFiles(address, root ? rootKey(root) : 'none'),
    queryFn: () => listComposerFiles(requireConnection(connection), root!),
    enabled: live && Boolean(root),
    staleTime: Number.POSITIVE_INFINITY,
  })
}

export function useComposerCommands(
  instanceId: string | undefined,
  root: T.RootRef | undefined,
  threadId?: string,
) {
  const { connection, address, live } = useLive()
  return useQuery({
    queryKey: daemonKeys.slashCommands(address, instanceId ?? 'none', root ? rootKey(root) : 'none'),
    queryFn: () => discoverComposerCommands(requireConnection(connection), instanceId!, root!, threadId),
    enabled: live && Boolean(instanceId && root),
    staleTime: Number.POSITIVE_INFINITY,
  })
}

/** Daemon settings, from the `settings` stream, shaped like a query. */
export function useDaemonSettings() {
  const stream = useSettingsStream()
  return {
    data: stream.settings,
    revision: stream.revision,
    isPending: !stream.ready,
    error: stream.error,
    store: stream.store,
  }
}

function binaryOverride(settings: T.Settings | undefined, instanceId: string): string | null {
  return settings?.providerBinaryOverrides?.[instanceId] ?? null
}

/** A provider's installation and model catalog (`providers.probe`), cached in the browser. */
export function useProviderProbe(instanceId: string | undefined) {
  const { connection, address, live } = useLive()
  const settings = useDaemonSettings()
  const cached = live && instanceId
    ? readProviderProbeCache(browserProviderProbeStorage(), address, instanceId)
    : undefined
  const override = settings.data && instanceId
    ? binaryOverride(settings.data, instanceId)
    : cached?.binaryOverride ?? null
  const initial = cached?.binaryOverride === override ? cached : undefined
  return useQuery({
    queryKey: [...daemonKeys.provider(address, instanceId ?? 'none'), override],
    queryFn: async () => {
      const data = await probeProvider(requireConnection(connection), instanceId!)
      writeProviderProbeCache(browserProviderProbeStorage(), address, instanceId!, override, data)
      return data
    },
    enabled: live && Boolean(instanceId && settings.data),
    initialData: initial?.data,
    initialDataUpdatedAt: initial?.updatedAt,
    staleTime: PROVIDER_PROBE_CACHE_STALE_TIME,
  })
}

/** Every known provider's catalog, for pickers and settings. */
export function useProviderProbes(enabled = true) {
  const { connection, address, live } = useLive()
  const settings = useDaemonSettings()
  const storage = browserProviderProbeStorage()
  const active = enabled && live && Boolean(settings.data)
  const queries = useQueries({
    queries: PROVIDERS.map(({ id }) => {
      const cached = live ? readProviderProbeCache(storage, address, id) : undefined
      const override = settings.data ? binaryOverride(settings.data, id) : cached?.binaryOverride ?? null
      const initial = cached?.binaryOverride === override ? cached : undefined
      return {
        queryKey: [...daemonKeys.provider(address, id), override],
        queryFn: async () => {
          const data = await probeProvider(requireConnection(connection), id)
          writeProviderProbeCache(storage, address, id, override, data)
          return data
        },
        enabled: active,
        initialData: initial?.data,
        initialDataUpdatedAt: initial?.updatedAt,
        staleTime: PROVIDER_PROBE_CACHE_STALE_TIME,
      }
    }),
  })
  const data: Partial<Record<string, ProviderCatalog>> = {}
  const states: Record<string, { dataUpdatedAt: number; error: unknown; isPending: boolean }> = {}
  PROVIDERS.forEach(({ id }, index) => {
    const query = queries[index]!
    if (query.data) data[id] = query.data
    states[id] = { dataUpdatedAt: query.dataUpdatedAt, error: query.error, isPending: query.isPending }
  })
  return {
    data,
    states,
    isFetching: queries.some((query) => query.isFetching),
    isPending: queries.some((query) => query.isPending),
  }
}

/** Installation status of every provider, from the `providers` stream (no CLI is spawned). */
export function useProviderDetections() {
  return useProviderStatuses()
}

/**
 * The stored turn item, for a row whose body the wire projection left out
 * (a file change's diff, a tool's full output). Fetched once, when shown.
 */
export function useTurnItem(threadId: string | undefined, itemId: string | undefined, enabled: boolean) {
  const { connection, address, live } = useLive()
  return useQuery({
    queryKey: ['daemon', address, 'turn-item', threadId ?? 'none', itemId ?? 'none'],
    queryFn: () => requireConnection(connection).call('threads.turnItem', { threadId: threadId!, itemId: itemId! }),
    enabled: live && enabled && Boolean(threadId && itemId),
    staleTime: Number.POSITIVE_INFINITY,
  })
}

export function useSkills(projectIds: string[]) {
  const { connection, address, live } = useLive()
  return useQuery({
    queryKey: [...daemonKeys.skills(address), [...projectIds].sort().join(',')],
    queryFn: () => loadSkills(requireConnection(connection), projectIds),
    enabled: live,
  })
}

export function useUsageHistory(window: T.UsageWindow, projectIds: string[]) {
  const { connection, address, live } = useLive()
  return useQuery({
    queryKey: daemonKeys.usage(address, window),
    queryFn: () => loadUsageHistory(requireConnection(connection), window, projectIds),
    enabled: live,
    placeholderData: (previous) => previous,
  })
}
