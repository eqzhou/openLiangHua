import type { ReactNode } from 'react'

interface WorkspaceHeroProps {
  eyebrow?: string
  title: string
  description?: string
  badges?: ReactNode
  controls?: ReactNode
  summary?: ReactNode
  className?: string
}

export function WorkspaceHero({ eyebrow, title, description, badges, controls, summary, className }: WorkspaceHeroProps) {
  return (
    <header className={['workspace-hero', className].filter(Boolean).join(' ')}>
      <div className="workspace-hero__intro">
        {eyebrow ? <p className="workspace-hero__eyebrow">{eyebrow}</p> : null}
        <div className="workspace-hero__row">
          <div className="workspace-hero__copy">
            <h1 className="workspace-hero__title">{title}</h1>
            {description ? <p className="workspace-hero__description">{description}</p> : null}
          </div>
          {summary ? <div className="workspace-hero__summary">{summary}</div> : null}
        </div>
        {badges ? <div className="workspace-hero__badges">{badges}</div> : null}
      </div>
      {controls ? <div className="workspace-hero__controls">{controls}</div> : null}
    </header>
  )
}
