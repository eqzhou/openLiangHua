import type { PropsWithChildren, ReactNode } from 'react'

interface PageFilterBarProps extends PropsWithChildren {
  title: string
  description?: string
  eyebrow?: string
  meta?: ReactNode
  actions?: ReactNode
  compact?: boolean
  className?: string
}

export function PageFilterBar({
  title,
  description,
  eyebrow,
  meta,
  actions,
  compact = true,
  className,
  children,
}: PageFilterBarProps) {
  return (
    <section className={['page-filter-bar', compact ? 'page-filter-bar--compact' : null, className].filter(Boolean).join(' ')} aria-label={title}>
      <div className="page-filter-bar__intro">
        <div className="page-filter-bar__copy">
          {eyebrow ? <p className="page-filter-bar__eyebrow">{eyebrow}</p> : null}
          <p className="page-filter-bar__title">{title}</p>
          {description ? <p className="page-filter-bar__description">{description}</p> : null}
        </div>
        {meta || actions ? (
          <div className="page-filter-bar__aside">
            {meta ? <div className="page-filter-bar__meta">{meta}</div> : null}
            {actions ? <div className="page-filter-bar__actions">{actions}</div> : null}
          </div>
        ) : null}
      </div>
      <div className="page-filter-bar__controls">{children}</div>
    </section>
  )
}
