import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate } from 'react-router-dom'

import { apiGet } from '../api/client'
import { Badge } from '../components/Badge'
import { ContextStrip } from '../components/ContextStrip'
import { ControlField } from '../components/ControlField'
import { ControlGrid } from '../components/ControlGrid'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { MetricCard } from '../components/MetricCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { aiReviewPageClient, aiReviewSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDate } from '../lib/format'
import { buildAiReviewPath } from '../lib/shareLinks'
import type { AiReviewSummaryPayload, JsonRecord } from '../types/api'

const CANDIDATE_COLUMNS = [
  'name',
  'trade_date',
  'industry_display',
  'action_hint',
  'confidence_level',
  'final_score',
  'quant_score',
  'factor_overlay_score',
  'model_consensus',
]

const CANDIDATE_COLUMN_LABELS = {
  name: '股票',
  trade_date: '截面日期',
  industry_display: '行业',
  action_hint: '操作建议',
  confidence_level: '置信度',
  final_score: '综合得分',
  quant_score: '量化得分',
  factor_overlay_score: '叠加得分',
  model_consensus: '模型共识',
}

const AI_VIEW_PRESETS = [
  { key: 'thesis', label: '论点', columns: ['name', 'trade_date', 'action_hint', 'confidence_level', 'final_score', 'model_consensus'] },
  { key: 'scores', label: '分数', columns: ['name', 'trade_date', 'final_score', 'quant_score', 'factor_overlay_score', 'confidence_level'] },
]

function buildOptionLabel(row: JsonRecord): string {
  const code = String(row.ts_code ?? '')
  const name = String(row.name ?? '')
  return name ? `${code} / ${name}` : code
}

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

const AI_CANDIDATE_CELL_RENDERERS = {
  name: (row: JsonRecord) => (
    <EntityCell
      title={String(row.name ?? '-')}
      subtitle={String(row.ts_code ?? '')}
      meta={String(row.industry_display ?? row.industry ?? '')}
      badges={
        String(row.confidence_level ?? '').trim()
          ? [{ label: String(row.confidence_level), tone: getConfidenceTone(row.confidence_level) }]
          : []
      }
    />
  ),
}

function renderSummaryPanel(
  scope: 'inference' | 'historical',
  title: string,
  panel: AiReviewSummaryPayload['inference'] | AiReviewSummaryPayload['historical'] | undefined,
  emptyText: string,
  onRowClick: (row: JsonRecord) => void,
) {
  const storagePrefix = scope === 'inference' ? 'ai-review-inference' : 'ai-review-historical'

  return (
    <Panel title={title} className="panel--summary-surface ai-review-pool-panel">
      <SectionBlock title="候选总表" description="先比较列表，再点入单票详情页。">
        <DataTable
          rows={panel?.candidates ?? []}
          columns={CANDIDATE_COLUMNS}
          columnLabels={CANDIDATE_COLUMN_LABELS}
          storageKey={`${storagePrefix}-candidates`}
          viewPresets={AI_VIEW_PRESETS}
          defaultPresetKey="thesis"
          emptyText={emptyText}
          stickyFirstColumn
          getRowId={(row) => String(row.ts_code ?? '')}
          onRowClick={onRowClick}
          rowTitle="点击进入详情"
          cellRenderers={AI_CANDIDATE_CELL_RENDERERS}
        />
      </SectionBlock>
    </Panel>
  )
}

export function AiReviewPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const { params, updateParams } = usePageSearchState(aiReviewPageClient)

  const summaryQuery = useQuery({
    queryKey: aiReviewSummaryClient.queryKey(params),
    queryFn: () => apiGet<AiReviewSummaryPayload>(aiReviewSummaryClient.path(params)),
  })

  const inference = summaryQuery.data?.inference
  const historical = summaryQuery.data?.historical

  const aiContextItems = [
    {
      label: '最新推理池',
      value: inference?.candidateCount ?? 0,
      helper: formatDate(inference?.candidates?.[0]?.trade_date),
      tone: 'brand' as const,
    },
    {
      label: '历史验证池',
      value: historical?.candidateCount ?? 0,
      helper: formatDate(historical?.candidates?.[0]?.trade_date),
    },
    {
      label: '当前推理定位',
      value: params.inference || '-',
    },
    {
      label: '当前验证定位',
      value: params.historical || '-',
    },
  ]

  const aiHeroBadges = (
    <>
      <Badge tone="brand">{`最新推理 ${inference?.candidateCount ?? 0} 只`}</Badge>
      <Badge tone="default">{`历史验证 ${historical?.candidateCount ?? 0} 只`}</Badge>
      <Badge tone="brand">列表优先</Badge>
    </>
  )

  return (
    <div className="page-stack">
      <WorkspaceHero
        title="AI 分析对照"
        className="ai-review-anchor-hero"
        description="首屏先比较两个池子的列表与数量，再点击单票进入详情页，不会一上来就拉双边详情。"
        badges={aiHeroBadges}
      />

      <div className="metric-grid metric-grid--four">
        <MetricCard label="最新推理池" value={inference?.candidateCount ?? 0} />
        <MetricCard label="历史验证池" value={historical?.candidateCount ?? 0} />
        <MetricCard label="当前推理定位" value={params.inference || '-'} />
        <MetricCard label="当前验证定位" value={params.historical || '-'} />
      </div>

      <Panel title="筛选" subtitle="两池都先看列表，点一行再进入详情页。" tone="warm" className="panel--summary-surface ai-review-switch-panel">
        <QueryNotice isLoading={summaryQuery.isLoading} error={summaryQuery.error} />
        <SectionBlock title="列表模式" tone="emphasis">
          <SpotlightCard
            title="列表优先"
            meta="详情改为独立页面"
            subtitle="不再首屏自动请求双边 detail。"
            metrics={[
              { label: '最新推理池', value: inference?.candidateCount ?? 0 },
              { label: '历史验证池', value: historical?.candidateCount ?? 0 },
            ]}
          />
        </SectionBlock>
        <ContextStrip items={aiContextItems} />
        <PageFilterBar title="定位单票">
          <ControlGrid variant="double">
            <ControlField label="最新推理股票">
              <select
                value={params.inference}
                onChange={(event) => updateParams({ inference: event.target.value })}
              >
                <option value="">未选择</option>
                {(inference?.candidates ?? []).map((row) => {
                  const value = String(row.ts_code ?? '')
                  return (
                    <option key={value} value={value}>
                      {buildOptionLabel(row)}
                    </option>
                  )
                })}
              </select>
            </ControlField>
            <ControlField label="历史验证股票">
              <select
                value={params.historical}
                onChange={(event) => updateParams({ historical: event.target.value })}
              >
                <option value="">未选择</option>
                {(historical?.candidates ?? []).map((row) => {
                  const value = String(row.ts_code ?? '')
                  return (
                    <option key={value} value={value}>
                      {buildOptionLabel(row)}
                    </option>
                  )
                })}
              </select>
            </ControlField>
          </ControlGrid>
          <div className="inline-actions inline-actions--compact">
            <button type="button" className="button button--ghost" onClick={() => params.inference && navigate({ pathname: buildAiReviewPath(params.inference, 'inference'), search: location.search })} disabled={!params.inference}>
              查看推理详情
            </button>
            <button type="button" className="button button--ghost" onClick={() => params.historical && navigate({ pathname: buildAiReviewPath(params.historical, 'historical'), search: location.search })} disabled={!params.historical}>
              查看验证详情
            </button>
          </div>
        </PageFilterBar>
      </Panel>

      <div className="split-layout">
        {renderSummaryPanel('inference', '推理列表', inference, '暂无最新推理候选池', (row) => navigate({ pathname: buildAiReviewPath(String(row.ts_code ?? ''), 'inference'), search: location.search }))}
        {renderSummaryPanel('historical', '验证列表', historical, '暂无历史验证候选池', (row) => navigate({ pathname: buildAiReviewPath(String(row.ts_code ?? ''), 'historical'), search: location.search }))}
      </div>
    </div>
  )
}
