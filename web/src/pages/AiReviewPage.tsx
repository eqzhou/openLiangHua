import { useQuery } from '@tanstack/react-query'

import { apiGet } from '../api/client'
import { Badge } from '../components/Badge'
import { ContextStrip } from '../components/ContextStrip'
import { ControlField } from '../components/ControlField'
import { ControlGrid } from '../components/ControlGrid'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { InsightList } from '../components/InsightList'
import { MarkdownCard } from '../components/MarkdownCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { aiReviewDetailClient, aiReviewPageClient, aiReviewSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDate, formatPercent, formatValue, splitTags, splitTextPoints } from '../lib/format'
import type { AiReviewDetailPayload, AiReviewSummaryPayload, JsonRecord } from '../types/api'

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

const FIELD_COLUMNS = ['field', 'value']

const CANDIDATE_COLUMN_LABELS = {
  name: '股票',
  trade_date: '截面日期',
  industry_display: '行业',
  action_hint: '建议动作',
  confidence_level: '置信等级',
  final_score: '综合得分',
  quant_score: '量化得分',
  factor_overlay_score: '叠加得分',
  model_consensus: '模型一致性',
}

const AI_VIEW_PRESETS = [
  { key: 'thesis', label: '论点视图', columns: ['name', 'trade_date', 'action_hint', 'confidence_level', 'final_score', 'model_consensus'] },
  { key: 'scores', label: '分数视图', columns: ['name', 'trade_date', 'final_score', 'quant_score', 'factor_overlay_score', 'confidence_level'] },
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
  subtitle: string,
  panel: AiReviewSummaryPayload['inference'] | AiReviewSummaryPayload['historical'] | undefined,
  emptyText: string,
) {
  const selected = panel?.selectedRecord ?? {}
  const themeTags = splitTags(selected.theme_tags)
  const storagePrefix = scope === 'inference' ? 'ai-review-inference' : 'ai-review-historical'

  return (
    <Panel title={title} subtitle={subtitle} className="panel--summary-surface">
      <SectionBlock title="候选总表" description="先看列表，再看摘要。">
        <DataTable
          rows={panel?.candidates ?? []}
          columns={CANDIDATE_COLUMNS}
          columnLabels={CANDIDATE_COLUMN_LABELS}
          storageKey={`${storagePrefix}-candidates`}
          viewPresets={AI_VIEW_PRESETS}
          defaultPresetKey="thesis"
          emptyText={emptyText}
          stickyFirstColumn
          cellRenderers={AI_CANDIDATE_CELL_RENDERERS}
        />
      </SectionBlock>

      {panel?.selectedRecord && Object.keys(panel.selectedRecord).length ? (
        <SectionBlock title="当前选中标的" description="再看单股结论。">
          <SpotlightCard
            title={String(selected.name ?? '-')}
            meta={String(selected.ts_code ?? '')}
            subtitle={String(selected.thesis_summary ?? '暂无摘要')}
            badges={[
              { label: String(selected.action_hint ?? '继续观察'), tone: 'brand' },
              { label: `置信度 ${String(selected.confidence_level ?? '-')}`, tone: getConfidenceTone(selected.confidence_level) },
              { label: formatDate(selected.trade_date) },
            ]}
            metrics={[
              { label: '综合得分', value: selected.final_score ?? '-' },
              { label: '量化得分', value: selected.quant_score ?? '-' },
              { label: '叠加得分', value: selected.factor_overlay_score ?? '-' },
              { label: '模型一致性', value: selected.model_consensus ?? '-' },
            ]}
          />
          <PropertyGrid
            items={[
              { label: '行业', value: formatValue(selected.industry_display ?? selected.industry) },
              {
                label: '主题标签',
                value: themeTags.length ? <div className="badge-row">{themeTags.map((tag) => <Badge key={tag}>{tag}</Badge>)}</div> : '暂无主题',
                span: 'double',
              },
              { label: 'LGBM 分位', value: formatPercent(selected.lgbm_rank_pct) },
              { label: 'Ridge 分位', value: formatPercent(selected.ridge_rank_pct) },
            ]}
          />
        </SectionBlock>
      ) : (
        <div className="empty-state">暂无单股摘要</div>
      )}
    </Panel>
  )
}

function renderDetailPanel(
  scope: 'inference' | 'historical',
  title: string,
  subtitle: string,
  detail: AiReviewDetailPayload | undefined,
) {
  const selected = detail?.selectedRecord ?? {}
  const llmFieldRows = Object.entries(detail?.llmResponse ?? {}).map(([field, value]) => ({ field, value }))

  return (
    <SupportPanel title={title} subtitle={subtitle}>
      {selected && Object.keys(selected).length ? (
        <div className="section-stack">
          <SectionBlock title="核心论点" description="只保留结论和风险。">
            <div className="split-layout">
              <InsightList title="看多要点" items={splitTextPoints(selected.bull_points)} tone="good" emptyText="暂无看多要点" />
              <InsightList title="风险提示" items={splitTextPoints(selected.risk_points)} tone="warn" emptyText="暂无风险提示" />
            </div>
          </SectionBlock>

          <SectionBlock title="支持信息" description="公告、新闻和研报后置。" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                { label: '公告摘要', value: formatValue(selected.notice_digest), span: 'double' },
                { label: '新闻摘要', value: formatValue(selected.news_digest), helper: formatValue(selected.news_source), span: 'double' },
                { label: '研报摘要', value: formatValue(selected.research_digest), span: 'double' },
              ]}
            />
          </SectionBlock>

          <SectionBlock title="研判纪要与模型响应" description="长文本和模型响应后置。" collapsible defaultExpanded={false}>
            <div className="section-stack">
              <MarkdownCard title="研判纪要" content={detail?.brief} />
              {detail?.responseSummary ? <MarkdownCard title="外部模型摘要" content={detail.responseSummary} /> : null}
              {llmFieldRows.length ? (
                <DataTable
                  rows={llmFieldRows}
                  columns={FIELD_COLUMNS}
                  storageKey={`ai-review-${scope}-llm-response`}
                  stickyFirstColumn
                  enableColumnManager={false}
                />
              ) : null}
            </div>
          </SectionBlock>

          <details className="details-block">
            <summary>查看完整字段</summary>
            <DataTable
              rows={detail?.fieldRows ?? []}
              columns={FIELD_COLUMNS}
              storageKey={`ai-review-${scope}-all-fields`}
              stickyFirstColumn
              enableColumnManager={false}
            />
          </details>
        </div>
      ) : (
        <div className="empty-state">暂无详情</div>
      )}
    </SupportPanel>
  )
}

export function AiReviewPage() {
  const { params, updateParams } = usePageSearchState(aiReviewPageClient)

  const summaryQuery = useQuery({
    queryKey: aiReviewSummaryClient.queryKey(params),
    queryFn: () => apiGet<AiReviewSummaryPayload>(aiReviewSummaryClient.path(params)),
  })

  const inferenceSymbol = summaryQuery.data?.inference?.selectedSymbol ?? params.inference
  const historicalSymbol = summaryQuery.data?.historical?.selectedSymbol ?? params.historical

  const inferenceDetailQuery = useQuery({
    queryKey: aiReviewDetailClient.queryKey('inference', inferenceSymbol),
    queryFn: () => apiGet<AiReviewDetailPayload>(aiReviewDetailClient.path('inference', inferenceSymbol)),
    enabled: Boolean(inferenceSymbol),
  })

  const historicalDetailQuery = useQuery({
    queryKey: aiReviewDetailClient.queryKey('historical', historicalSymbol),
    queryFn: () => apiGet<AiReviewDetailPayload>(aiReviewDetailClient.path('historical', historicalSymbol)),
    enabled: Boolean(historicalSymbol),
  })

  const inference = summaryQuery.data?.inference
  const historical = summaryQuery.data?.historical

  const aiContextItems = [
    {
      label: '最新推理池',
      value: inference?.candidateCount ?? 0,
      helper: formatDate(inference?.selectedRecord?.trade_date ?? inference?.candidates?.[0]?.trade_date),
      tone: 'brand' as const,
    },
    {
      label: '历史验证池',
      value: historical?.candidateCount ?? 0,
      helper: formatDate(historical?.selectedRecord?.trade_date ?? historical?.candidates?.[0]?.trade_date),
    },
    {
      label: '当前推理标的',
      value: inference?.selectedRecord ? buildOptionLabel(inference.selectedRecord) : '-',
      helper: String(inference?.selectedRecord?.action_hint ?? ''),
    },
    {
      label: '当前验证标的',
      value: historical?.selectedRecord ? buildOptionLabel(historical.selectedRecord) : '-',
      helper: String(historical?.selectedRecord?.action_hint ?? ''),
    },
  ]

  return (
    <div className="page-stack">
      <Panel
        title="AI 研判"
        subtitle="先比较两个候选池，再下钻单股。"
        tone="warm"
        className="panel--summary-surface"
      >
        <QueryNotice
          isLoading={summaryQuery.isLoading}
          error={summaryQuery.error ?? inferenceDetailQuery.error ?? historicalDetailQuery.error}
        />
        <ContextStrip items={aiContextItems} />
        <PageFilterBar
          title="切换研判工作区"
          description="两个候选池各保留一个单股选择口。"
        >
          <ControlGrid variant="double">
            <ControlField label="最新推理股票">
              <select
                value={inference?.selectedSymbol ?? params.inference ?? ''}
                onChange={(event) => updateParams({ inference: event.target.value })}
              >
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
                value={historical?.selectedSymbol ?? params.historical ?? ''}
                onChange={(event) => updateParams({ historical: event.target.value })}
              >
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
        </PageFilterBar>
      </Panel>

      <div className="split-layout">
        {renderSummaryPanel('inference', '最新未标注推理', '先看列表，再看摘要。', inference, '暂无最新推理候选池')}
        {renderSummaryPanel('historical', '历史验证研判', '先看列表，再看摘要。', historical, '暂无历史验证候选池')}
      </div>

      <div className="split-layout">
        {renderDetailPanel('inference', '推理详情', '只保留论点和支持信息。', inferenceDetailQuery.data)}
        {renderDetailPanel('historical', '验证详情', '保持同层对比。', historicalDetailQuery.data)}
      </div>
    </div>
  )
}
