import type { PropsWithChildren, ReactNode } from 'react'
import { useState } from 'react'

interface SectionBlockProps extends PropsWithChildren {
  title: string
  description?: string
  actions?: ReactNode
  eyebrow?: string
  tone?: 'default' | 'muted' | 'emphasis'
  className?: string
  collapsible?: boolean
  defaultExpanded?: boolean
}

export function SectionBlock({
  title,
  description,
  actions,
  eyebrow,
  tone = 'default',
  className,
  children,
  collapsible = false,
  defaultExpanded = true,
}: SectionBlockProps) {
  const [expanded, setExpanded] = useState(defaultExpanded)
  const sectionClassName = ['section-block', `section-block--${tone}`, className].filter(Boolean).join(' ')
  const sectionBody = <div className="section-block__body">{children}</div>

  return (
    <section className={sectionClassName}>
      <header className="section-block__header">
        <div className="section-block__copy">
          {eyebrow ? <p className="section-block__eyebrow">{eyebrow}</p> : null}
          <h3 className="section-block__title">{title}</h3>
          {description ? <p className="section-block__description">{description}</p> : null}
        </div>
        <div className="section-block__actions">
          {actions}
          {collapsible ? (
            <button
              type="button"
              className="button button--ghost button--small"
              onClick={() => setExpanded((value) => !value)}
              aria-expanded={expanded}
            >
              {expanded ? '收起' : '展开'}
            </button>
          ) : null}
        </div>
      </header>
      {!collapsible || expanded ? sectionBody : null}
    </section>
  )
}
