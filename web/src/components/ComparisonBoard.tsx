import type { ReactNode } from 'react'

interface ComparisonColumn {
  key: string
  label: string
  description?: string
}

interface ComparisonRow {
  key: string
  label: string
  helper?: string
  values: Record<string, ReactNode>
}

interface ComparisonBoardProps {
  columns: ComparisonColumn[]
  rows: ComparisonRow[]
  emptyText?: string
}

export function ComparisonBoard({ columns, rows, emptyText = '暂无数据' }: ComparisonBoardProps) {
  if (!columns.length || !rows.length) {
    return <div className="empty-state">{emptyText}</div>
  }

  return (
    <section className="comparison-board" aria-label="横向比较面板">
      <div className="comparison-board__header comparison-board__grid">
        <div className="comparison-board__stub" />
        {columns.map((column) => (
          <div key={column.key} className="comparison-board__column">
            <strong className="comparison-board__column-title">{column.label}</strong>
            {column.description ? <span className="comparison-board__column-description">{column.description}</span> : null}
          </div>
        ))}
      </div>
      <div className="comparison-board__body">
        {rows.map((row) => (
          <div key={row.key} className="comparison-board__row comparison-board__grid">
            <div className="comparison-board__label">
              <strong>{row.label}</strong>
              {row.helper ? <span>{row.helper}</span> : null}
            </div>
            {columns.map((column) => (
              <div key={`${row.key}:${column.key}`} className="comparison-board__value">
                {row.values[column.key] ?? '-'}
              </div>
            ))}
          </div>
        ))}
      </div>
    </section>
  )
}
