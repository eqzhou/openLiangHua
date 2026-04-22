import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate, useParams } from 'react-router-dom'

import { apiGet } from '../api/client'
import { Badge } from '../components/Badge'
import { DataTable } from '../components/DataTable'
import { DetailPageNav } from '../components/DetailPageNav'
import { InsightList } from '../components/InsightList'
import { MarkdownCard } from '../components/MarkdownCard'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { aiReviewDetailClient, aiReviewPageClient, aiReviewSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatValue, splitTextPoints } from '../lib/format'
import { buildAiReviewPath } from '../lib/shareLinks'
import type { AiReviewDetailPayload, JsonRecord } from '../types/api'

const FIELD_COLUMNS = ['field', 'value']

function getConfidenceTone(value: unknown): 'default' | 'good' | 'warn' {
  const text = String(value ?? '')
  if (text.includes('高')) {
    return 'good'
  }
  if (text.includes('低')) {
    return 'warn'
  }
  return 'default'
}

export function AiReviewDetailPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const { params } = usePageSearchState(aiReviewPageClient)
  const { scope = 'inference', symbol = '' } = useParams<{ scope: 'inference' | 'historical'; symbol: string }>()

  const detailQuery = useQuery({
    queryKey: aiReviewDetailClient.queryKey(scope === 'historical' ? 'historical' : 'inference', symbol),
    queryFn: () => apiGet<AiReviewDetailPayload>(aiReviewDetailClient.path(scope === 'historical' ? 'historical' : 'inference', symbol)),
    enabled: Boolean(symbol),
  })

  const summaryQuery = useQuery({
    queryKey: aiReviewSummaryClient.queryKey(params),
    queryFn: () => apiGet<any>(aiReviewSummaryClient.path(params)),
  })

  const selected = detailQuery.data?.selectedRecord ?? {}
  const llmFieldRows = Object.entries(detailQuery.data?.llmResponse ?? {}).map(([field, value]) => ({ field, value } as JsonRecord))
  const panel = scope === 'historical' ? summaryQuery.data?.historical : summaryQuery.data?.inference
  const candidates = panel?.candidates ?? []
  const candidateSymbols = candidates.map((row: JsonRecord) => String(row.ts_code ?? '')).filter(Boolean)
  const currentIndex = candidateSymbols.findIndex((item: string) => item === symbol)
  const previousSymbol = currentIndex > 0 ? candidateSymbols[currentIndex - 1] : null
  const nextSymbol = currentIndex >= 0 && currentIndex < candidateSymbols.length - 1 ? candidateSymbols[currentIndex + 1] : null
  const currentPositionLabel = currentIndex >= 0 ? `${currentIndex + 1} / ${candidateSymbols.length}` : '-'

  return (
    <div className="page-stack">
      <WorkspaceHero
        title="AI 分析详情"
        eyebrow={scope === 'historical' ? 'AI 分析 / 历史验证详情' : 'AI 分析 / 最新推理详情'}
        badges={
          <>
            <Badge tone="brand">{scope === 'historical' ? '历史验证' : '最新推理'}</Badge>
            <Badge>{String(selected.ts_code ?? symbol)}</Badge>
            {selected.confidence_level ? <Badge tone={getConfidenceTone(selected.confidence_level)}>{String(selected.confidence_level)}</Badge> : null}
            <Badge>{`序号 ${currentPositionLabel}`}</Badge>
          </>
        }
      />

      <Panel title="详情" tone="warm" className="panel--summary-surface">
        <QueryNotice isLoading={detailQuery.isLoading} error={detailQuery.error} />
        {Object.keys(selected).length ? (
          <SectionBlock title="核心结论" tone="emphasis">
            <SpotlightCard
              title={String(selected.name ?? '-')}
              meta={String(selected.ts_code ?? '')}
              subtitle={String(selected.thesis_summary ?? selected.action_hint ?? '暂无结论')}
              badges={[
                { label: String(selected.action_hint ?? '继续观察'), tone: 'brand' },
                { label: `置信度 ${String(selected.confidence_level ?? '-')}`, tone: getConfidenceTone(selected.confidence_level) },
              ]}
              metrics={[
                { label: '综合得分', value: selected.final_score ?? '-' },
                { label: '量化得分', value: selected.quant_score ?? '-' },
                { label: '叠加得分', value: selected.factor_overlay_score ?? '-' },
                { label: '模型共识', value: selected.model_consensus ?? '-' },
              ]}
            />
          </SectionBlock>
        ) : (
          <div className="empty-state">暂无 AI 详情</div>
        )}
      </Panel>

      <DetailPageNav
        onBack={() => navigate({ pathname: '/ai-review', search: location.search })}
        prevLabel={previousSymbol}
        onPrev={previousSymbol ? () => navigate({ pathname: buildAiReviewPath(previousSymbol, scope === 'historical' ? 'historical' : 'inference'), search: location.search }) : null}
        nextLabel={nextSymbol}
        onNext={nextSymbol ? () => navigate({ pathname: buildAiReviewPath(nextSymbol, scope === 'historical' ? 'historical' : 'inference'), search: location.search }) : null}
      />

      <div className="split-layout">
        <Panel title="论点" tone="calm" className="panel--summary-surface">
          <InsightList title="看多要点" items={splitTextPoints(selected.bull_points)} tone="good" emptyText="暂无看多要点" />
          <InsightList title="风险提示" items={splitTextPoints(selected.risk_points)} tone="warn" emptyText="暂无风险提示" />
        </Panel>
        <Panel title="补充信息" tone="calm" className="panel--summary-surface">
          <PropertyGrid
            items={[
              { label: '公告概览', value: formatValue(selected.notice_digest), span: 'double' },
              { label: '新闻概览', value: formatValue(selected.news_digest), helper: formatValue(selected.news_source), span: 'double' },
              { label: '研报概览', value: formatValue(selected.research_digest), span: 'double' },
            ]}
          />
        </Panel>
      </div>

      <div className="split-layout">
        <Panel title="分析概览" tone="calm" className="panel--summary-surface">
          <MarkdownCard title="分析概览" content={detailQuery.data?.brief} />
          {detailQuery.data?.responseSummary ? <MarkdownCard title="外部模型概览" content={detailQuery.data.responseSummary} /> : null}
        </Panel>
        <Panel title="外部模型响应" tone="calm" className="panel--summary-surface">
          <DataTable rows={llmFieldRows} columns={FIELD_COLUMNS} storageKey={`ai-review-${scope}-llm-response`} enableColumnManager={false} emptyText="暂无模型响应" />
        </Panel>
      </div>

      <Panel title="完整字段" tone="calm" className="panel--summary-surface">
        <DataTable rows={detailQuery.data?.fieldRows ?? []} columns={FIELD_COLUMNS} storageKey={`ai-review-${scope}-all-fields`} enableColumnManager={false} emptyText="暂无详情字段" />
      </Panel>
    </div>
  )
}
