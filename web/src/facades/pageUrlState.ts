import { readTablePreferenceSnapshot, type StorageLike, type TablePreferenceState } from '../lib/uiPreferences'

export type UrlStateValue = string | number | boolean | null | undefined

const VIEW_STATE_PARAM = 'view'

const PAGE_TABLE_STORAGE_KEYS: Array<{ route: string; storageKeys: string[] }> = [
  { route: '/', storageKeys: ['overview-comparison'] },
  { route: '/factors', storageKeys: ['factor-snapshot', 'factor-ranking', 'factor-missing'] },
  { route: '/backtests', storageKeys: ['backtest-monthly', 'backtest-importance', 'backtest-yearly', 'backtest-regime'] },
  { route: '/candidates', storageKeys: ['candidates-matrix', 'candidates-detail-fields'] },
  { route: '/watchlist', storageKeys: ['watchlist-matrix', 'watchlist-reduce-plan', 'watchlist-detail-fields', 'watchlist-discussion'] },
  {
    route: '/ai-review',
    storageKeys: [
      'ai-review-inference-candidates',
      'ai-review-inference-fields',
      'ai-review-inference-llm-response',
      'ai-review-historical-candidates',
      'ai-review-historical-fields',
      'ai-review-historical-llm-response',
    ],
  },
  { route: '/service', storageKeys: ['service-fields'] },
]

type SharedTablePreferenceMap = Record<string, TablePreferenceState>

function normalizeUrlStateValue(value: UrlStateValue): string | null {
  if (value === null || value === undefined || value === '') {
    return null
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false'
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return null
    }
    return String(value)
  }
  return String(value)
}

function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = ''
  bytes.forEach((value) => {
    binary += String.fromCharCode(value)
  })
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '')
}

function base64UrlToBytes(value: string): Uint8Array {
  const padded = value.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(value.length / 4) * 4, '=')
  const binary = atob(padded)
  return Uint8Array.from(binary, (char) => char.charCodeAt(0))
}

function isValidColumns(candidate: unknown): candidate is string[] {
  return Array.isArray(candidate) && candidate.every((item) => typeof item === 'string')
}

function normalizeWidths(candidate: unknown): Record<string, number> {
  if (!candidate || typeof candidate !== 'object') {
    return {}
  }

  const next: Record<string, number> = {}
  Object.entries(candidate).forEach(([key, value]) => {
    const numericValue = typeof value === 'number' ? value : Number(value)
    if (!Number.isFinite(numericValue) || numericValue <= 0) {
      return
    }
    next[key] = Math.round(numericValue)
  })
  return next
}

function normalizeSharedTablePreference(candidate: unknown): TablePreferenceState | null {
  if (!candidate || typeof candidate !== 'object') {
    return null
  }

  const record = candidate as { columns?: unknown; presetKey?: unknown; widths?: unknown }
  return {
    columns: isValidColumns(record.columns) ? record.columns : [],
    presetKey: typeof record.presetKey === 'string' && record.presetKey.trim() ? record.presetKey : null,
    widths: normalizeWidths(record.widths),
  }
}

function getBrowserStorage(): StorageLike | null {
  if (typeof window === 'undefined') {
    return null
  }

  try {
    return window.localStorage
  } catch {
    return null
  }
}

function getPageTableStorageKeys(pathname: string): string[] {
  const exactRoot = pathname === '/'
  const match = PAGE_TABLE_STORAGE_KEYS.find((item) =>
    item.route === '/' ? exactRoot : pathname === item.route || pathname.startsWith(`${item.route}/`),
  )
  return match?.storageKeys ?? []
}

function encodeSharedTablePreferences(preferences: SharedTablePreferenceMap): string | null {
  const entries = Object.entries(preferences).filter(([, state]) => state.columns.length || state.presetKey || Object.keys(state.widths).length)
  if (!entries.length) {
    return null
  }

  const payload = Object.fromEntries(
    entries.map(([storageKey, state]) => [
      storageKey,
      {
        columns: state.columns,
        presetKey: state.presetKey,
        widths: state.widths,
      },
    ]),
  )

  const bytes = new TextEncoder().encode(JSON.stringify(payload))
  return bytesToBase64Url(bytes)
}

export function decodeSharedTablePreferences(value: string | null): SharedTablePreferenceMap {
  if (!value) {
    return {}
  }

  try {
    const bytes = base64UrlToBytes(value)
    const decoded = new TextDecoder().decode(bytes)
    const parsed = JSON.parse(decoded) as Record<string, unknown>
    if (!parsed || typeof parsed !== 'object') {
      return {}
    }

    return Object.fromEntries(
      Object.entries(parsed)
        .map(([storageKey, candidate]) => [storageKey, normalizeSharedTablePreference(candidate)] as const)
        .filter((entry): entry is [string, TablePreferenceState] => Boolean(entry[1])),
    )
  } catch {
    return {}
  }
}

export function readSharedTablePreferenceFromSearch(search: string, storageKey: string): TablePreferenceState | null {
  const searchParams = new URLSearchParams(search.startsWith('?') ? search.slice(1) : search)
  return decodeSharedTablePreferences(searchParams.get(VIEW_STATE_PARAM))[storageKey] ?? null
}

export function buildShareablePageUrl(pathname: string, search: string, storage: StorageLike | null = getBrowserStorage()): string {
  const searchParams = new URLSearchParams(search.startsWith('?') ? search.slice(1) : search)
  const preferences = Object.fromEntries(
    getPageTableStorageKeys(pathname)
      .map((storageKey) => [storageKey, readTablePreferenceSnapshot(storageKey, storage)] as const)
      .filter((entry): entry is [string, TablePreferenceState] => Boolean(entry[1])),
  )
  const encodedState = encodeSharedTablePreferences(preferences)

  if (encodedState) {
    searchParams.set(VIEW_STATE_PARAM, encodedState)
  } else {
    searchParams.delete(VIEW_STATE_PARAM)
  }

  const query = searchParams.toString()
  const origin = typeof window !== 'undefined' ? window.location.origin : ''
  return `${origin}${pathname}${query ? `?${query}` : ''}`
}

export function mergeSearchParams(current: URLSearchParams, updates: Record<string, UrlStateValue>): URLSearchParams {
  const next = new URLSearchParams(current)
  Object.entries(updates).forEach(([key, value]) => {
    const normalized = normalizeUrlStateValue(value)
    if (normalized === null) {
      next.delete(key)
      return
    }
    next.set(key, normalized)
  })
  return next
}

export function buildApiPath(path: string, params: Record<string, UrlStateValue> = {}): string {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    const normalized = normalizeUrlStateValue(value)
    if (normalized !== null) {
      query.set(key, normalized)
    }
  })
  const queryText = query.toString()
  return queryText ? `${path}?${queryText}` : path
}

export function readStringParam(searchParams: URLSearchParams, key: string, fallback = ''): string {
  const value = searchParams.get(key)
  if (value === null || value === '') {
    return fallback
  }
  return value
}

export function readClampedIntParam(
  searchParams: URLSearchParams,
  key: string,
  options: {
    fallback: number
    min: number
    max: number
  },
): number {
  const { fallback, min, max } = options
  const raw = Number(searchParams.get(key) ?? fallback)
  if (!Number.isFinite(raw)) {
    return fallback
  }
  return Math.max(min, Math.min(max, Math.trunc(raw)))
}
