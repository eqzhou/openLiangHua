import { formatValue } from '../lib/format'

interface MetricCardProps {
  label: string
  value: unknown
  tone?: 'default' | 'good' | 'warn'
  helper?: string
}

export function MetricCard({ label, value, tone = 'default', helper }: MetricCardProps) {
  return (
    <article className={`metric-card metric-card--${tone}`}>
      <span className="metric-card__label">{label}</span>
      <strong className="metric-card__value">{formatValue(value)}</strong>
      {helper ? <span className="metric-card__helper">{helper}</span> : null}
    </article>
  )
}
