import type { PropsWithChildren, ReactNode } from 'react'

import { MobileInspectionCard } from './MobileInspectionCard'
import { Panel } from './Panel'

interface SupportPanelMobileCard {
  eyebrow?: string
  title?: string
  subtitle?: string
  badges?: ReactNode
  body?: ReactNode
  actions?: ReactNode
}

interface SupportPanelProps extends PropsWithChildren {
  title: string
  subtitle?: string
  actions?: ReactNode
  eyebrow?: string
  className?: string
  compact?: boolean
  mobileCard?: SupportPanelMobileCard
}

export function SupportPanel({ title, subtitle, actions, eyebrow, className, compact = true, mobileCard, children }: SupportPanelProps) {
  const mergedClassName = ['panel--support-surface', compact ? 'panel--support-surface--compact' : null, className].filter(Boolean).join(' ')

  if (mobileCard) {
    return (
      <>
        <div className="desktop-only">
          <Panel title={title} subtitle={subtitle} actions={actions} eyebrow={eyebrow} className={mergedClassName}>
            {children}
          </Panel>
        </div>
        <MobileInspectionCard
          eyebrow={mobileCard.eyebrow ?? eyebrow}
          title={mobileCard.title ?? title}
          subtitle={mobileCard.subtitle ?? subtitle}
          badges={mobileCard.badges}
          body={mobileCard.body}
          actions={mobileCard.actions}
        />
      </>
    )
  }

  return (
    <Panel title={title} subtitle={subtitle} actions={actions} eyebrow={eyebrow} className={mergedClassName}>
      {children}
    </Panel>
  )
}
