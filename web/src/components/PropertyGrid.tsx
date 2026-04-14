import type { ReactNode } from 'react'

interface PropertyItem {
  label: string
  value: ReactNode
  helper?: string
  tone?: 'default' | 'good' | 'warn'
  span?: 'single' | 'double'
}

interface PropertyGridProps {
  items: PropertyItem[]
  columns?: 'double' | 'triple'
}

export function PropertyGrid({ items, columns = 'double' }: PropertyGridProps) {
  return (
    <div className={`property-grid property-grid--${columns}`}>
      {items.map((item, index) => (
        <article
          key={`${item.label}-${index}`}
          className={`property-card property-card--${item.tone ?? 'default'}${item.span === 'double' ? ' property-card--double' : ''}`}
        >
          <span className="property-card__label">{item.label}</span>
          <div className="property-card__value">{item.value}</div>
          {item.helper ? <p className="property-card__helper">{item.helper}</p> : null}
        </article>
      ))}
    </div>
  )
}
