import type { PropsWithChildren, ReactNode } from 'react'

import { Panel } from './Panel'

interface SupportPanelProps extends PropsWithChildren {
  title: string
  subtitle?: string
  actions?: ReactNode
  eyebrow?: string
  className?: string
  compact?: boolean
}

export function SupportPanel({ title, subtitle, actions, eyebrow, className, compact = true, children }: SupportPanelProps) {
  const mergedClassName = ['panel--support-surface', compact ? 'panel--support-surface--compact' : null, className].filter(Boolean).join(' ')

  return (
    <Panel title={title} subtitle={subtitle} actions={actions} eyebrow={eyebrow} className={mergedClassName}>
      {children}
    </Panel>
  )
}
