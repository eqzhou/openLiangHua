import { formatValue } from '../lib/format'

interface ContextStripItem {
  label: string
  value: unknown
  helper?: string
  tone?: 'default' | 'good' | 'warn' | 'brand'
  priority?: 'primary' | 'secondary'
}

interface ContextStripProps {
  items: ContextStripItem[]
  className?: string
  compact?: boolean
}

export function ContextStrip({ items, className, compact = true }: ContextStripProps) {
  const visibleItems = items.filter((item) => {
    const value = formatValue(item.value)
    return value !== '-'
  })

  if (!visibleItems.length) {
    return null
  }

  return (
    <div className={['context-strip', compact ? 'context-strip--compact' : null, className].filter(Boolean).join(' ')} role="list">
      {visibleItems.map((item) => (
        <article
          key={`${item.label}-${String(item.value)}`}
          role="listitem"
          className={[
            'context-strip__item',
            `context-strip__item--${item.tone ?? 'default'}`,
            `context-strip__item--${item.priority ?? (item.tone && item.tone !== 'default' ? 'primary' : 'secondary')}`,
          ].join(' ')}
        >
          <span className="context-strip__label">{item.label}</span>
          <strong className="context-strip__value">{formatValue(item.value)}</strong>
          {item.helper && item.helper.trim() !== '-' ? <span className="context-strip__helper">{item.helper}</span> : null}
        </article>
      ))}
    </div>
  )
}
