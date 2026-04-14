import type { PropsWithChildren, ReactNode } from 'react'

interface PanelProps extends PropsWithChildren {
  title: string
  subtitle?: string
  eyebrow?: string
  actions?: ReactNode
  tone?: 'default' | 'calm' | 'warm'
  className?: string
}

export function Panel({ title, subtitle, eyebrow, actions, tone = 'default', className, children }: PanelProps) {
  const panelClassName = ['panel', `panel--${tone}`, className].filter(Boolean).join(' ')

  return (
    <section className={panelClassName}>
      <header className={`panel__header${actions ? ' panel__header--with-actions' : ''}`}>
        <div className="panel__title-group">
          {eyebrow ? <p className="panel__eyebrow">{eyebrow}</p> : null}
          <h2 className="panel__title">{title}</h2>
          {subtitle ? <p className="panel__subtitle">{subtitle}</p> : null}
        </div>
        {actions ? <div className="panel__actions">{actions}</div> : null}
      </header>
      <div className="panel__body">{children}</div>
    </section>
  )
}
