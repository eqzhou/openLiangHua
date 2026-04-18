import type { ReactNode } from 'react'

interface MobileInspectionCardProps {
  eyebrow?: string
  title: string
  subtitle?: string
  badges?: ReactNode
  body?: ReactNode
  actions?: ReactNode
}

export function MobileInspectionCard({
  eyebrow = 'Inspection',
  title,
  subtitle,
  badges,
  body,
  actions,
}: MobileInspectionCardProps) {
  return (
    <article className="mobile-inspection-card">
      <div className="mobile-inspection-card__header">
        <div className="mobile-inspection-card__copy">
          <p className="mobile-inspection-card__eyebrow">{eyebrow}</p>
          <h3 className="mobile-inspection-card__title">{title}</h3>
          {subtitle ? <p className="mobile-inspection-card__subtitle">{subtitle}</p> : null}
        </div>
        {badges ? <div className="mobile-inspection-card__badges">{badges}</div> : null}
      </div>
      {body ? <div className="mobile-inspection-card__body">{body}</div> : null}
      {actions ? <div className="mobile-inspection-card__actions">{actions}</div> : null}
    </article>
  )
}
