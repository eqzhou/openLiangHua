import type { ReactNode } from 'react'

import { PropertyGrid } from './PropertyGrid'
import { SectionBlock } from './SectionBlock'
import { SpotlightCard } from './SpotlightCard'

interface DetailSummaryBadge {
  label: string
  tone?: 'default' | 'brand' | 'good' | 'warn'
}

interface DetailSummaryMetric {
  label: string
  value: unknown
  helper?: string
  tone?: 'default' | 'good' | 'warn'
}

interface DetailSummaryProperty {
  label: string
  value: ReactNode
  helper?: string
  tone?: 'default' | 'good' | 'warn'
  span?: 'single' | 'double'
}

interface DetailSummarySectionProps {
  eyebrow?: string
  sectionTitle?: string
  sectionDescription?: string
  title: string
  meta?: string
  subtitle?: string
  badges?: DetailSummaryBadge[]
  metrics?: DetailSummaryMetric[]
  properties?: DetailSummaryProperty[]
  propertyColumns?: 'double' | 'triple'
  actions?: ReactNode
  children?: ReactNode
}

export function DetailSummarySection({
  eyebrow,
  sectionTitle = '当前概览',
  sectionDescription,
  title,
  meta,
  subtitle,
  badges,
  metrics,
  properties = [],
  propertyColumns = 'double',
  actions,
  children,
}: DetailSummarySectionProps) {
  return (
    <SectionBlock
      title={sectionTitle}
      description={sectionDescription}
      eyebrow={eyebrow}
      className="detail-summary-section"
    >
      <div className="section-stack">
        <SpotlightCard title={title} meta={meta} subtitle={subtitle} badges={badges} metrics={metrics} actions={actions} />
        {properties.length ? <PropertyGrid items={properties} columns={propertyColumns} /> : null}
        {children}
      </div>
    </SectionBlock>
  )
}
