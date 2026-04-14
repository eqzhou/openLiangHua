import type { PropsWithChildren, ReactNode } from 'react'

interface SidebarSectionProps extends PropsWithChildren {
  title: string
  subtitle?: string
  eyebrow?: string
  actions?: ReactNode
}

export function SidebarSection({ title, subtitle, eyebrow, actions, children }: SidebarSectionProps) {
  return (
    <section className="shell-card shell-card--sidebar">
      <header className={`shell-card__header${actions ? ' shell-card__header--with-actions' : ''}`}>
        <div className="shell-card__title-group">
          {eyebrow ? <p className="shell-card__eyebrow">{eyebrow}</p> : null}
          <h2 className="shell-card__title">{title}</h2>
          {subtitle ? <p className="shell-card__subtitle">{subtitle}</p> : null}
        </div>
        {actions ? <div className="shell-card__actions">{actions}</div> : null}
      </header>
      <div className="shell-card__body">{children}</div>
    </section>
  )
}
