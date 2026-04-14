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
  sectionTitle = '当前摘要',
  sectionDescription = '先看结论和关键字段，再按需展开后续信息。',
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
    <SectionBlock title={sectionTitle} description={sectionDescription}>
      <div className="section-stack">
        <SpotlightCard title={title} meta={meta} subtitle={subtitle} badges={badges} metrics={metrics} actions={actions} />
        {properties.length ? <PropertyGrid items={properties} columns={propertyColumns} /> : null}
        {children}
      </div>
    </SectionBlock>
  )
}
