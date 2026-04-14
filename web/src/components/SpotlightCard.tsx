import type { ReactNode } from 'react'

import { Badge } from './Badge'
import { MetricCard } from './MetricCard'

interface SpotlightBadge {
  label: string
  tone?: 'default' | 'brand' | 'good' | 'warn'
}

interface SpotlightMetric {
  label: string
  value: unknown
  helper?: string
  tone?: 'default' | 'good' | 'warn'
}

interface SpotlightCardProps {
  title: string
  subtitle?: string
  meta?: string
  badges?: SpotlightBadge[]
  metrics?: SpotlightMetric[]
  actions?: ReactNode
  children?: ReactNode
}

export function SpotlightCard({ title, subtitle, meta, badges = [], metrics = [], actions, children }: SpotlightCardProps) {
  const visibleBadges = badges.filter((item) => item.label)

  return (
    <div className="spotlight-card">
      <div className="headline-row">
        <div>
          <h3 className="headline-title">
            {title}
            {meta ? <span className="headline-title__meta">{meta}</span> : null}
          </h3>
          {subtitle ? <p className="headline-subtitle">{subtitle}</p> : null}
        </div>
        {(visibleBadges.length || actions) ? (
          <div className="spotlight-card__side">
            {visibleBadges.length ? (
              <div className="badge-row">
                {visibleBadges.map((badge) => (
                  <Badge key={`${badge.label}-${badge.tone ?? 'default'}`} tone={badge.tone}>
                    {badge.label}
                  </Badge>
                ))}
              </div>
            ) : null}
            {actions ? <div className="spotlight-card__actions">{actions}</div> : null}
          </div>
        ) : null}
      </div>

      {metrics.length ? (
        <div className={`metric-grid${metrics.length <= 3 ? ' metric-grid--three' : metrics.length === 4 ? ' metric-grid--four' : ''}`}>
          {metrics.map((metric) => (
            <MetricCard
              key={`${metric.label}-${String(metric.value)}`}
              label={metric.label}
              value={metric.value}
              helper={metric.helper}
              tone={metric.tone}
            />
          ))}
        </div>
      ) : null}

      {children}
    </div>
  )
}
