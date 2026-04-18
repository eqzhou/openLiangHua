import { useCallback, useEffect, useMemo, useRef, useState, type MouseEvent as ReactMouseEvent, type ReactNode } from 'react'
import { useLocation } from 'react-router-dom'

import { readSharedTablePreferenceFromSearch } from '../facades/pageUrlState'
import { formatCellValue, getFieldLabel } from '../lib/format'
import {
  UI_PREFERENCES_RESET_EVENT,
  readTablePreference,
  writeTablePreference,
  type TablePreferenceState,
} from '../lib/uiPreferences'
import type { JsonRecord } from '../types/api'

type TableDensity = 'compact' | 'comfortable'
type SortDirection = 'asc' | 'desc'

const MIN_COLUMN_WIDTH = 112
const MAX_COLUMN_WIDTH = 720

interface SortState {
  column: string
  direction: SortDirection
}

interface ViewPreset {
  key: string
  label: string
  columns: string[]
}

interface ResizeState {
  column: string
  startX: number
  startWidth: number
}

interface DataTableProps {
  rows: JsonRecord[]
  columns?: string[]
  emptyText?: string
  loading?: boolean
  loadingText?: string
  columnLabels?: Record<string, string>
  stickyFirstColumn?: boolean
  density?: TableDensity
  enableColumnManager?: boolean
  storageKey?: string
  viewPresets?: ViewPreset[]
  defaultPresetKey?: string
  getRowId?: (row: JsonRecord, index: number) => string
  onRowClick?: (row: JsonRecord) => void
  selectedRowId?: string | null
  cellRenderers?: Record<string, (row: JsonRecord, column: string) => ReactNode>
}

function clampWidth(value: number): number {
  return Math.max(MIN_COLUMN_WIDTH, Math.min(MAX_COLUMN_WIDTH, Math.round(value)))
}

function normalizeWidths(columns: string[], widths: Record<string, number>): Record<string, number> {
  const allowed = new Set(columns)
  return Object.fromEntries(
    Object.entries(widths)
      .filter(([column, value]) => allowed.has(column) && Number.isFinite(value) && value > 0)
      .map(([column, value]) => [column, clampWidth(value)]),
  )
}

function toComparableValue(value: unknown): number | string {
  if (typeof value === 'number') {
    return value
  }
  if (typeof value === 'boolean') {
    return value ? 1 : 0
  }

  const raw = String(value ?? '').trim()
  if (!raw) {
    return ''
  }

  const asDate = Date.parse(raw)
  if (!Number.isNaN(asDate)) {
    return asDate
  }

  return raw.toLowerCase()
}

function compareValues(left: unknown, right: unknown): number {
  const leftValue = toComparableValue(left)
  const rightValue = toComparableValue(right)

  if (typeof leftValue === 'number' && typeof rightValue === 'number') {
    return leftValue - rightValue
  }

  return String(leftValue).localeCompare(String(rightValue), 'zh-CN')
}

function nextSortState(current: SortState | null, column: string): SortState | null {
  if (!current || current.column !== column) {
    return { column, direction: 'asc' }
  }
  if (current.direction === 'asc') {
    return { column, direction: 'desc' }
  }
  return null
}

function moveColumn(columns: string[], fromColumn: string, toColumn: string): string[] {
  if (fromColumn === toColumn) {
    return columns
  }

  const next = [...columns]
  const fromIndex = next.indexOf(fromColumn)
  const toIndex = next.indexOf(toColumn)

  if (fromIndex === -1 || toIndex === -1) {
    return columns
  }

  next.splice(fromIndex, 1)
  next.splice(toIndex, 0, fromColumn)
  return next
}

function normalizePresetColumns(columns: string[], presetColumns: string[]): string[] {
  const allowed = new Set(columns)
  const next = presetColumns.filter((column) => allowed.has(column))
  return next.length ? next : columns
}

function normalizePreferenceState(
  resolvedColumns: string[],
  presets: ViewPreset[],
  preference: TablePreferenceState,
): TablePreferenceState {
  const presetMap = new Map(presets.map((preset) => [preset.key, preset.columns]))
  const columns = preference.columns.filter((column) => resolvedColumns.includes(column))

  return {
    columns: columns.length ? columns : resolvedColumns,
    presetKey: preference.presetKey && presetMap.has(preference.presetKey) ? preference.presetKey : null,
    widths: normalizeWidths(resolvedColumns, preference.widths),
  }
}

function resolveInitialLayoutState(
  resolvedColumns: string[],
  presets: ViewPreset[],
  storageKey?: string,
  defaultPresetKey?: string,
  sharedPreference?: TablePreferenceState | null,
): TablePreferenceState {
  const presetMap = new Map(presets.map((preset) => [preset.key, normalizePresetColumns(resolvedColumns, preset.columns)]))

  if (sharedPreference) {
    return normalizePreferenceState(resolvedColumns, presets, sharedPreference)
  }

  const storedPreference = storageKey ? readTablePreference(storageKey, resolvedColumns) : null
  if (storedPreference) {
    return normalizePreferenceState(resolvedColumns, presets, storedPreference)
  }

  if (defaultPresetKey && presetMap.has(defaultPresetKey)) {
    return {
      columns: presetMap.get(defaultPresetKey) ?? resolvedColumns,
      presetKey: defaultPresetKey,
      widths: {},
    }
  }

  return {
    columns: resolvedColumns,
    presetKey: null,
    widths: {},
  }
}

export function DataTable({
  rows,
  columns,
  emptyText = '暂无数据',
  loading = false,
  loadingText = '加载中...',
  columnLabels,
  stickyFirstColumn = false,
  density = 'compact',
  enableColumnManager = true,
  storageKey,
  viewPresets = [],
  defaultPresetKey,
  getRowId,
  onRowClick,
  selectedRowId = null,
  cellRenderers,
}: DataTableProps) {
  const location = useLocation()
  const resolvedColumns = useMemo(() => (columns && columns.length ? columns : Object.keys(rows[0] ?? {})), [columns, rows])
  const normalizedPresets = useMemo(
    () =>
      viewPresets
        .map((preset) => ({
          ...preset,
          columns: normalizePresetColumns(resolvedColumns, preset.columns),
        }))
        .filter((preset) => preset.columns.length > 0),
    [resolvedColumns, viewPresets],
  )
  const sharedPreference = useMemo(
    () => (storageKey ? readSharedTablePreferenceFromSearch(location.search, storageKey) : null),
    [location.search, storageKey],
  )
  const externalLayoutState = useMemo(
    () => resolveInitialLayoutState(resolvedColumns, normalizedPresets, storageKey, defaultPresetKey, sharedPreference),
    [defaultPresetKey, normalizedPresets, resolvedColumns, sharedPreference, storageKey],
  )
  const layoutSourceKey = useMemo(
    () =>
      JSON.stringify({
        columns: resolvedColumns,
        presets: normalizedPresets.map((preset) => ({ key: preset.key, columns: preset.columns })),
        storageKey,
        defaultPresetKey,
        sharedPreference,
      }),
    [defaultPresetKey, normalizedPresets, resolvedColumns, sharedPreference, storageKey],
  )

  const [layoutState, setLayoutState] = useState<{ sourceKey: string; layout: TablePreferenceState }>(() => ({
    sourceKey: layoutSourceKey,
    layout: externalLayoutState,
  }))
  const [sortState, setSortState] = useState<SortState | null>(null)
  const [columnManagerOpen, setColumnManagerOpen] = useState(false)
  const [dragColumn, setDragColumn] = useState<string | null>(null)
  const [dropColumn, setDropColumn] = useState<string | null>(null)
  const [resizingColumn, setResizingColumn] = useState<string | null>(null)
  const columnManagerRef = useRef<HTMLDivElement | null>(null)
  const resizeStateRef = useRef<ResizeState | null>(null)

  const currentLayoutState = layoutState.sourceKey === layoutSourceKey ? layoutState.layout : externalLayoutState
  const visibleColumns = currentLayoutState.columns
  const activePresetKey = currentLayoutState.presetKey
  const columnWidths = currentLayoutState.widths
  const effectiveSortState = sortState && resolvedColumns.includes(sortState.column) ? sortState : null
  const activePreset = normalizedPresets.find((preset) => preset.key === activePresetKey) ?? null

  const updateLayoutState = useCallback((updater: (current: TablePreferenceState) => TablePreferenceState) => {
    setLayoutState((current) => {
      const baseLayout = current.sourceKey === layoutSourceKey ? current.layout : externalLayoutState
      return {
        sourceKey: layoutSourceKey,
        layout: updater(baseLayout),
      }
    })
  }, [externalLayoutState, layoutSourceKey])

  useEffect(() => {
    if (!columnManagerOpen) {
      return undefined
    }

    const onMouseDown = (event: MouseEvent) => {
      if (!columnManagerRef.current?.contains(event.target as Node)) {
        setColumnManagerOpen(false)
      }
    }

    window.addEventListener('mousedown', onMouseDown)
    return () => window.removeEventListener('mousedown', onMouseDown)
  }, [columnManagerOpen])

  useEffect(() => {
    if (!resizingColumn) {
      return undefined
    }

    const onMouseMove = (event: MouseEvent) => {
      const resizeState = resizeStateRef.current
      if (!resizeState) {
        return
      }

      const nextWidth = clampWidth(resizeState.startWidth + (event.clientX - resizeState.startX))
      updateLayoutState((current) => ({
        ...current,
        presetKey: null,
        widths: {
          ...current.widths,
          [resizeState.column]: nextWidth,
        },
      }))
    }

    const onMouseUp = () => {
      resizeStateRef.current = null
      setResizingColumn(null)
    }

    window.addEventListener('mousemove', onMouseMove)
    window.addEventListener('mouseup', onMouseUp)
    return () => {
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('mouseup', onMouseUp)
    }
  }, [resizingColumn, updateLayoutState])

  useEffect(() => {
    if (!storageKey) {
      return undefined
    }

    const onReset = () => {
      const nextState = resolveInitialLayoutState(resolvedColumns, normalizedPresets, undefined, defaultPresetKey)
      setLayoutState({
        sourceKey: layoutSourceKey,
        layout: nextState,
      })
      setSortState(null)
      setColumnManagerOpen(false)
      setDragColumn(null)
      setDropColumn(null)
      setResizingColumn(null)
      resizeStateRef.current = null
    }

    window.addEventListener(UI_PREFERENCES_RESET_EVENT, onReset)
    return () => window.removeEventListener(UI_PREFERENCES_RESET_EVENT, onReset)
  }, [defaultPresetKey, layoutSourceKey, normalizedPresets, resolvedColumns, storageKey])

  const effectiveColumns = visibleColumns.filter((column) => resolvedColumns.includes(column))
  const safeColumns = effectiveColumns.length ? effectiveColumns : resolvedColumns.slice(0, 1)
  const presetMap = useMemo(() => new Map(normalizedPresets.map((preset) => [preset.key, preset.columns])), [normalizedPresets])

  useEffect(() => {
    if (!storageKey) {
      return
    }

    writeTablePreference(storageKey, {
      columns: safeColumns,
      presetKey: activePresetKey,
      widths: columnWidths,
    })
  }, [activePresetKey, columnWidths, safeColumns, storageKey])

  const sortedRows = useMemo(() => {
    if (!effectiveSortState) {
      return rows
    }

    const nextRows = [...rows]
    nextRows.sort((left, right) => {
      const comparison = compareValues(left[effectiveSortState.column], right[effectiveSortState.column])
      return effectiveSortState.direction === 'asc' ? comparison : -comparison
    })
    return nextRows
  }, [effectiveSortState, rows])

  const applyPreset = (presetKey: string) => {
    const presetColumns = presetMap.get(presetKey)
    if (!presetColumns) {
      return
    }

    updateLayoutState((current) => ({
      ...current,
      columns: presetColumns,
      presetKey,
    }))
    setColumnManagerOpen(false)
  }

  const restoreDefaultLayout = () => {
    const nextState = resolveInitialLayoutState(resolvedColumns, normalizedPresets, undefined, defaultPresetKey)
    setLayoutState({
      sourceKey: layoutSourceKey,
      layout: nextState,
    })
    setSortState(null)
    setColumnManagerOpen(false)
    setDragColumn(null)
    setDropColumn(null)
    setResizingColumn(null)
    resizeStateRef.current = null
  }

  const startColumnResize = (event: ReactMouseEvent<HTMLSpanElement>, column: string) => {
    event.preventDefault()
    event.stopPropagation()

    const headerCell = event.currentTarget.closest('th')
    const measuredWidth = headerCell?.getBoundingClientRect().width ?? columnWidths[column] ?? MIN_COLUMN_WIDTH
    resizeStateRef.current = {
      column,
      startX: event.clientX,
      startWidth: measuredWidth,
    }
    setResizingColumn(column)
  }

  const resetColumnWidth = (event: ReactMouseEvent<HTMLSpanElement>, column: string) => {
    event.preventDefault()
    event.stopPropagation()

    updateLayoutState((current) => {
      if (!(column in current.widths)) {
        return current
      }

      const nextWidths = { ...current.widths }
      delete nextWidths[column]
      return {
        ...current,
        presetKey: null,
        widths: nextWidths,
      }
    })
  }

  if (!rows.length) {
    return <div className="empty-state">{loading ? loadingText : emptyText}</div>
  }

  const tableWrapClassName = ['table-wrap', stickyFirstColumn ? 'table-wrap--sticky-first' : ''].filter(Boolean).join(' ')
  const tableClassName = ['data-table', `data-table--${density}`, onRowClick ? 'data-table--interactive' : ''].join(' ')
  const primaryColumn = safeColumns[0]

  return (
    <section className="table-shell" role="region" aria-live="polite" aria-busy={loading}>
      <div className="table-toolbar">
        <div className="table-toolbar__summary" aria-label="表格摘要">
          <div className="table-toolbar__meta">
            <span className="table-toolbar__chip">共 {sortedRows.length} 行</span>
            {activePreset ? <span className="table-toolbar__chip table-toolbar__chip--active">当前预设 {activePreset.label}</span> : null}
            {enableColumnManager && resolvedColumns.length > 1 ? (
              <span className="table-toolbar__chip table-toolbar__chip--secondary">显示 {safeColumns.length}/{resolvedColumns.length} 列</span>
            ) : null}
          </div>
        </div>

        <div className="table-toolbar__controls">
          {normalizedPresets.length ? (
            <div className="table-toolbar__presets" role="group" aria-label="切换表格预设">
              {normalizedPresets.map((preset) => (
                <button
                  key={preset.key}
                  type="button"
                  className={`button button--ghost button--small${activePresetKey === preset.key ? ' button--active' : ''}`}
                  onClick={() => applyPreset(preset.key)}
                >
                  {preset.label}
                </button>
              ))}
            </div>
          ) : null}

          {enableColumnManager && resolvedColumns.length > 1 ? (
            <div className="table-toolbar__actions" ref={columnManagerRef}>
              {storageKey ? (
                <button type="button" className="button button--ghost button--small" onClick={restoreDefaultLayout}>
                  重置表格
                </button>
              ) : null}
              <button type="button" className="button button--ghost button--small" onClick={() => setColumnManagerOpen((open) => !open)}>
                表格设置
              </button>
              {columnManagerOpen ? (
                <div className="column-manager" role="dialog" aria-label="表格字段设置">
                  {resolvedColumns.map((column) => {
                    const checked = safeColumns.includes(column)
                    const disabled = checked && safeColumns.length === 1

                    return (
                      <label key={column} className="column-manager__item">
                        <input
                          type="checkbox"
                          checked={checked}
                          disabled={disabled}
                          onChange={() => {
                            updateLayoutState((current) => {
                              if (current.columns.includes(column)) {
                                const nextColumns = current.columns.filter((item) => item !== column)
                                return {
                                  ...current,
                                  columns: nextColumns.length ? nextColumns : current.columns,
                                  presetKey: null,
                                }
                              }

                              const order = new Map(resolvedColumns.map((item, index) => [item, index]))
                              return {
                                ...current,
                                columns: [...current.columns, column].sort((left, right) => (order.get(left) ?? 0) - (order.get(right) ?? 0)),
                                presetKey: null,
                              }
                            })
                          }}
                        />
                        <span>{getFieldLabel(column, columnLabels)}</span>
                      </label>
                    )
                  })}
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>

      <div className={tableWrapClassName}>
        <table className={tableClassName}>
          <thead>
            <tr>
              {safeColumns.map((column, columnIndex) => {
                const isSorted = effectiveSortState?.column === column
                const indicator = !isSorted ? '↕' : effectiveSortState?.direction === 'asc' ? '↑' : '↓'
                const isDropTarget = dropColumn === column && dragColumn !== column
                const width = columnWidths[column]
                const columnStyle = width ? { width: `${width}px`, minWidth: `${width}px` } : undefined

                return (
                  <th
                    key={column}
                    style={columnStyle}
                    className={[
                      stickyFirstColumn && columnIndex === 0 ? 'data-table__sticky-column' : '',
                      safeColumns.length > 1 ? 'data-table__th--draggable' : '',
                      dragColumn === column ? 'data-table__th--dragging' : '',
                      isDropTarget ? 'data-table__th--drop-target' : '',
                      resizingColumn === column ? 'data-table__th--resizing' : '',
                    ]
                      .filter(Boolean)
                      .join(' ')}
                    draggable={safeColumns.length > 1}
                    onDragStart={(event) => {
                      if (resizingColumn) {
                        event.preventDefault()
                        return
                      }
                      setDragColumn(column)
                      setDropColumn(null)
                      event.dataTransfer.effectAllowed = 'move'
                      event.dataTransfer.setData('text/plain', column)
                    }}
                    onDragOver={(event) => {
                      if (!dragColumn || dragColumn === column) {
                        return
                      }
                      event.preventDefault()
                      event.dataTransfer.dropEffect = 'move'
                      setDropColumn(column)
                    }}
                    onDrop={(event) => {
                      event.preventDefault()
                      const sourceColumn = dragColumn ?? event.dataTransfer.getData('text/plain')
                      if (!sourceColumn || sourceColumn === column) {
                        setDragColumn(null)
                        setDropColumn(null)
                        return
                      }

                      updateLayoutState((current) => ({
                        ...current,
                        columns: moveColumn(current.columns, sourceColumn, column),
                        presetKey: null,
                      }))
                      setDragColumn(null)
                      setDropColumn(null)
                    }}
                    onDragEnd={() => {
                      setDragColumn(null)
                      setDropColumn(null)
                    }}
                  >
                    <div className="data-table__th-inner">
                      {safeColumns.length > 1 ? (
                        <span className="data-table__drag-handle" aria-hidden="true">
                          ⋮⋮
                        </span>
                      ) : null}
                      <button type="button" className="data-table__sort-button" onClick={() => setSortState((current) => nextSortState(current, column))}>
                        <span>{getFieldLabel(column, columnLabels)}</span>
                        <span className={`data-table__sort-indicator${isSorted ? ' data-table__sort-indicator--active' : ''}`}>{indicator}</span>
                      </button>
                    </div>
                    <span
                      className="data-table__resize-handle"
                      role="presentation"
                      onMouseDown={(event) => startColumnResize(event, column)}
                      onDoubleClick={(event) => resetColumnWidth(event, column)}
                    />
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row, index) => {
              const rowId = getRowId?.(row, index) ?? `${index}-${String(row[primaryColumn] ?? index)}`
              const isSelected = selectedRowId !== null && rowId === selectedRowId

              return (
                <tr
                  key={rowId}
                  className={`${onRowClick ? 'data-table__row--clickable' : ''}${isSelected ? ' data-table__row--selected' : ''}`}
                  onClick={onRowClick ? () => onRowClick(row) : undefined}
                >
                  {safeColumns.map((column, columnIndex) => {
                    const width = columnWidths[column]
                    const cellStyle = width ? { width: `${width}px`, minWidth: `${width}px` } : undefined

                    return (
                      <td
                        key={column}
                        style={cellStyle}
                        className={stickyFirstColumn && columnIndex === 0 ? 'data-table__sticky-column' : undefined}
                      >
                        {cellRenderers?.[column] ? cellRenderers[column](row, column) : formatCellValue(column, row[column])}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      <div className="table-mobile-list">
        {sortedRows.map((row, index) => {
          const rowId = getRowId?.(row, index) ?? `${index}-${String(row[primaryColumn] ?? index)}`
          const isSelected = selectedRowId !== null && rowId === selectedRowId

          return (
            <article
              key={rowId}
              className={`table-mobile-card${isSelected ? ' table-mobile-card--selected' : ''}${onRowClick ? ' table-mobile-card--interactive' : ''}`}
              onClick={onRowClick ? () => onRowClick(row) : undefined}
            >
              <div className="table-mobile-card__primary">
                <span className="table-mobile-card__eyebrow">{getFieldLabel(primaryColumn, columnLabels)}</span>
                <div className="table-mobile-card__primary-value">
                  {cellRenderers?.[primaryColumn]
                    ? cellRenderers[primaryColumn](row, primaryColumn)
                    : formatCellValue(primaryColumn, row[primaryColumn])}
                </div>
              </div>

              <dl className="table-mobile-card__fields">
                {safeColumns.slice(1).map((column) => (
                  <div key={column} className="table-mobile-card__field">
                    <dt>{getFieldLabel(column, columnLabels)}</dt>
                    <dd>{cellRenderers?.[column] ? cellRenderers[column](row, column) : formatCellValue(column, row[column])}</dd>
                  </div>
                ))}
              </dl>
            </article>
          )
        })}
      </div>
    </section>
  )
}
