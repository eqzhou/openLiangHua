export type ThemeMode = 'light' | 'dark'

export interface StorageLike {
  getItem: (key: string) => string | null
  setItem: (key: string, value: string) => void
  removeItem: (key: string) => void
  key: (index: number) => string | null
  length: number
}

export interface TablePreferenceState {
  columns: string[]
  presetKey: string | null
  widths: Record<string, number>
}

const THEME_STORAGE_KEY = 'openlianghua.ui.theme'
const TABLE_PREFERENCES_PREFIX = 'openlianghua.table.preferences.'
export const UI_PREFERENCES_RESET_EVENT = 'openlianghua:ui-preferences-reset'

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

function isThemeMode(value: unknown): value is ThemeMode {
  return value === 'light' || value === 'dark'
}

function parseJson(value: string | null): unknown {
  if (!value) {
    return null
  }

  try {
    return JSON.parse(value)
  } catch {
    return null
  }
}

function normalizeColumns(columns: string[], candidate: unknown): string[] {
  if (!Array.isArray(candidate)) {
    return columns
  }

  const allowed = new Set(columns)
  const next: string[] = []

  candidate.forEach((item) => {
    if (typeof item !== 'string' || !allowed.has(item) || next.includes(item)) {
      return
    }
    next.push(item)
  })

  return next.length ? next : columns
}

function normalizeSnapshotColumns(candidate: unknown): string[] {
  if (!Array.isArray(candidate)) {
    return []
  }

  const next: string[] = []
  candidate.forEach((item) => {
    if (typeof item !== 'string' || !item.trim() || next.includes(item)) {
      return
    }
    next.push(item)
  })
  return next
}

function normalizePresetKey(candidate: unknown): string | null {
  return typeof candidate === 'string' && candidate.trim() ? candidate : null
}

function normalizeWidths(columns: string[], candidate: unknown): Record<string, number> {
  if (!candidate || typeof candidate !== 'object') {
    return {}
  }

  const allowAll = columns.length === 0
  const allowed = new Set(columns)
  const next: Record<string, number> = {}

  Object.entries(candidate).forEach(([key, value]) => {
    const numericValue = typeof value === 'number' ? value : Number(value)
    if (!Number.isFinite(numericValue) || numericValue <= 0) {
      return
    }
    if (!allowAll && !allowed.has(key)) {
      return
    }
    next[key] = Math.round(numericValue)
  })

  return next
}

function getTablePreferenceStorageKey(storageKey: string) {
  return `${TABLE_PREFERENCES_PREFIX}${storageKey}`
}

export function readThemeMode(storage: StorageLike | null = getBrowserStorage()): ThemeMode | null {
  const value = storage?.getItem(THEME_STORAGE_KEY)
  return isThemeMode(value) ? value : null
}

export function resolveInitialTheme(storage: StorageLike | null = getBrowserStorage()): ThemeMode {
  const storedTheme = readThemeMode(storage)
  if (storedTheme) {
    return storedTheme
  }

  return 'dark'
}

export function writeThemeMode(theme: ThemeMode, storage: StorageLike | null = getBrowserStorage()) {
  try {
    storage?.setItem(THEME_STORAGE_KEY, theme)
  } catch {
    // Ignore storage write failures and keep the in-memory theme.
  }
}

export function readTablePreferenceSnapshot(
  storageKey: string,
  storage: StorageLike | null = getBrowserStorage(),
): TablePreferenceState | null {
  const raw = parseJson(storage?.getItem(getTablePreferenceStorageKey(storageKey)) ?? null)

  if (Array.isArray(raw)) {
    return {
      columns: normalizeSnapshotColumns(raw),
      presetKey: null,
      widths: {},
    }
  }

  if (!raw || typeof raw !== 'object') {
    return null
  }

  const candidate = raw as { columns?: unknown; presetKey?: unknown; widths?: unknown }
  return {
    columns: normalizeSnapshotColumns(candidate.columns),
    presetKey: normalizePresetKey(candidate.presetKey),
    widths: normalizeWidths([], candidate.widths),
  }
}

export function readTablePreference(
  storageKey: string,
  columns: string[],
  storage: StorageLike | null = getBrowserStorage(),
): TablePreferenceState | null {
  const snapshot = readTablePreferenceSnapshot(storageKey, storage)
  if (!snapshot) {
    return null
  }

  return {
    columns: normalizeColumns(columns, snapshot.columns),
    presetKey: snapshot.presetKey,
    widths: normalizeWidths(columns, snapshot.widths),
  }
}

export function writeTablePreference(
  storageKey: string,
  state: TablePreferenceState,
  storage: StorageLike | null = getBrowserStorage(),
) {
  try {
    storage?.setItem(getTablePreferenceStorageKey(storageKey), JSON.stringify(state))
  } catch {
    // Ignore storage write failures and keep the current session state.
  }
}

export function clearUiPreferences(storage: StorageLike | null = getBrowserStorage()) {
  if (!storage) {
    return
  }

  try {
    storage.removeItem(THEME_STORAGE_KEY)

    const keys = Array.from({ length: storage.length }, (_, index) => storage.key(index)).filter(
      (item): item is string => Boolean(item),
    )
    keys.forEach((key) => {
      if (key.startsWith(TABLE_PREFERENCES_PREFIX)) {
        storage.removeItem(key)
      }
    })

    if (typeof window !== 'undefined') {
      window.dispatchEvent(new Event(UI_PREFERENCES_RESET_EVENT))
    }
  } catch {
    // Ignore storage cleanup failures and leave the current session state unchanged.
  }
}
