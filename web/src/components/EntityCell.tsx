import { Badge } from './Badge'

interface EntityBadge {
  label: string
  tone?: 'default' | 'brand' | 'good' | 'warn'
}

interface EntityCellProps {
  title: string
  subtitle?: string
  meta?: string
  badges?: EntityBadge[]
}

export function EntityCell({ title, subtitle, meta, badges = [] }: EntityCellProps) {
  const visibleBadges = badges.filter((item) => item.label)

  return (
    <div className="entity-cell">
      <div className="entity-cell__copy">
        <strong className="entity-cell__title">{title || '-'}</strong>
        {subtitle ? <span className="entity-cell__subtitle">{subtitle}</span> : null}
        {meta ? <span className="entity-cell__meta">{meta}</span> : null}
      </div>
      {visibleBadges.length ? (
        <div className="entity-cell__badges">
          {visibleBadges.map((badge) => (
            <Badge key={`${badge.label}-${badge.tone ?? 'default'}`} tone={badge.tone}>
              {badge.label}
            </Badge>
          ))}
        </div>
      ) : null}
    </div>
  )
}
