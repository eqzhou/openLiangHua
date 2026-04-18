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
import { MetricCard } from '../components/MetricCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { aiReviewDetailClient, aiReviewPageClient, aiReviewSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDate, formatValue, splitTags, splitTextPoints } from '../lib/format'
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

function normalizeActionHint(record: JsonRecord | undefined): string {
  return String(record?.action_hint ?? '').trim()
}

function buildDeskSummary(inference: JsonRecord | undefined, historical: JsonRecord | undefined) {
  const inferenceAction = normalizeActionHint(inference)
  const historicalAction = normalizeActionHint(historical)
  const inferenceScore = Number(inference?.final_score ?? 0)
  const historicalScore = Number(historical?.final_score ?? 0)
  const sameAction = inferenceAction && historicalAction && inferenceAction === historicalAction

  if (sameAction) {
    return {
      title: '两池一致',
      subtitle: `当前都倾向“${inferenceAction}”`,
      badges: [
        { label: '动作一致', tone: 'good' as const },
        { label: `推理 ${formatValue(inferenceScore || '-')}` },
        { label: `验证 ${formatValue(historicalScore || '-')}` },
      ],
    }
  }

  if (inferenceAction || historicalAction) {
    return {
      title: '两池不一致',
      subtitle: `推理：${inferenceAction || '暂无'} / 验证：${historicalAction || '暂无'}`,
      badges: [
        { label: '需要确认', tone: 'warn' as const },
        { label: `推理 ${formatValue(inferenceScore || '-')}` },
        { label: `验证 ${formatValue(historicalScore || '-')}` },
      ],
    }
  }

  return {
    title: '当前缺少明确结论',
    subtitle: '当前建议不足',
    badges: [{ label: '结论待补', tone: 'warn' as const }],
  }
}

function renderSummaryPanel(scope: 'inference' | 'historical', title: string, panel: AiReviewSummaryPayload['inference'] | AiReviewSummaryPayload['historical'] | undefined, emptyText: string) {
  const storagePrefix = scope === 'inference' ? 'ai-review-inference' : 'ai-review-historical'

  return (
    <Panel title={title} className="panel--summary-surface ai-review-pool-panel">
      <SectionBlock title="候选总表" description="主列表只负责池子比较。">
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
    </Panel>
  )
}

function renderDetailPanel(scope: 'inference' | 'historical', title: string, detail: AiReviewDetailPayload | undefined) {
  const selected = detail?.selectedRecord ?? {}
  const llmFieldRows = Object.entries(detail?.llmResponse ?? {}).map(([field, value]) => ({ field, value }))

  return (
    <SupportPanel title={title} className="ai-review-support-panel">
      {selected && Object.keys(selected).length ? (
        <div className="section-stack">
          <SectionBlock title="核心论点" collapsible defaultExpanded={false}>
            <div className="split-layout">
              <InsightList title="看多要点" items={splitTextPoints(selected.bull_points)} tone="good" emptyText="暂无看多要点" />
              <InsightList title="风险提示" items={splitTextPoints(selected.risk_points)} tone="warn" emptyText="暂无风险提示" />
            </div>
          </SectionBlock>

          <SectionBlock title="补充信息" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                { label: '公告概览', value: formatValue(selected.notice_digest), span: 'double' },
                { label: '新闻概览', value: formatValue(selected.news_digest), helper: formatValue(selected.news_source), span: 'double' },
                { label: '研报概览', value: formatValue(selected.research_digest), span: 'double' },
              ]}
            />
          </SectionBlock>

          <SectionBlock title="分析概览与模型响应" collapsible defaultExpanded={false}>
            <div className="section-stack">
              <MarkdownCard title="分析概览" content={detail?.brief} />
              {detail?.responseSummary ? <MarkdownCard title="外部模型概览" content={detail.responseSummary} /> : null}
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
      label: '当前推理股票',
      value: inference?.selectedRecord ? buildOptionLabel(inference.selectedRecord) : '-',
      helper: String(inference?.selectedRecord?.action_hint ?? ''),
    },
    {
      label: '当前验证股票',
      value: historical?.selectedRecord ? buildOptionLabel(historical.selectedRecord) : '-',
      helper: String(historical?.selectedRecord?.action_hint ?? ''),
    },
  ]

  const aiHeroBadges = (
    <>
      <Badge tone="brand">{`最新推理 ${inference?.candidateCount ?? 0} 只`}</Badge>
      <Badge tone="default">{`历史验证 ${historical?.candidateCount ?? 0} 只`}</Badge>
      {inference?.selectedRecord?.confidence_level ? (
        <Badge tone={getConfidenceTone(inference.selectedRecord.confidence_level)}>{`推理 ${String(inference.selectedRecord.confidence_level)}`}</Badge>
      ) : null}
      {historical?.selectedRecord?.confidence_level ? (
        <Badge tone={getConfidenceTone(historical.selectedRecord.confidence_level)}>{`验证 ${String(historical.selectedRecord.confidence_level)}`}</Badge>
      ) : null}
    </>
  )

  const deskSummary = buildDeskSummary(inference?.selectedRecord, historical?.selectedRecord)
  const inferenceThemeTags = splitTags(inference?.selectedRecord?.theme_tags)
  const historicalThemeTags = splitTags(historical?.selectedRecord?.theme_tags)

  return (
    <div className="page-stack">
      <WorkspaceHero
        title="AI 分析对照"
        className="ai-review-anchor-hero"
        badges={aiHeroBadges}
      />

      <div className="metric-grid metric-grid--four">
        <MetricCard label="最新推理池" value={inference?.candidateCount ?? 0} />
        <MetricCard label="历史验证池" value={historical?.candidateCount ?? 0} />
        <MetricCard label="推理综合得分" value={inference?.selectedRecord?.final_score ?? '-'} tone="good" />
        <MetricCard label="验证综合得分" value={historical?.selectedRecord?.final_score ?? '-'} tone="good" />
      </div>

      <Panel title="筛选" tone="warm" className="panel--summary-surface ai-review-switch-panel">
        <QueryNotice
          isLoading={summaryQuery.isLoading}
          error={summaryQuery.error ?? inferenceDetailQuery.error ?? historicalDetailQuery.error}
        />
        <ContextStrip items={aiContextItems} />
        <PageFilterBar title="切换股票">
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

      <Panel title="当前判断" className="panel--summary-surface ai-review-desk-panel">
        <SectionBlock title="当前判断" tone="emphasis" className="ai-review-desk-block">
          <SpotlightCard
            title={deskSummary.title}
            meta="两池概览"
            subtitle={deskSummary.subtitle}
            badges={deskSummary.badges}
            metrics={[
              { label: '最新推理池', value: inference?.candidateCount ?? 0 },
              { label: '历史验证池', value: historical?.candidateCount ?? 0 },
              { label: '推理建议', value: formatValue(inference?.selectedRecord?.action_hint ?? '-') },
              { label: '验证建议', value: formatValue(historical?.selectedRecord?.action_hint ?? '-') },
            ]}
          />
        </SectionBlock>

        <div className="split-layout ai-review-focus-grid">
          {inference?.selectedRecord && Object.keys(inference.selectedRecord).length ? (
            <SpotlightCard
              className="ai-review-focus-card ai-review-focus-card--inference"
              title={String(inference.selectedRecord.name ?? '-')}
              meta={`最新推理 / ${String(inference.selectedRecord.ts_code ?? '')}`}
              subtitle={String(inference.selectedRecord.thesis_summary ?? inference.selectedRecord.action_hint ?? '暂无结论')}
              badges={[
                { label: String(inference.selectedRecord.action_hint ?? '继续观察'), tone: 'brand' },
                { label: `置信度 ${String(inference.selectedRecord.confidence_level ?? '-')}`, tone: getConfidenceTone(inference.selectedRecord.confidence_level) },
              ]}
              metrics={[
                { label: '综合得分', value: inference.selectedRecord.final_score ?? '-' },
                { label: '量化得分', value: inference.selectedRecord.quant_score ?? '-' },
                { label: '叠加得分', value: inference.selectedRecord.factor_overlay_score ?? '-' },
                { label: '模型共识', value: inference.selectedRecord.model_consensus ?? '-' },
              ]}
            >
              <PropertyGrid
                items={[
                  { label: '行业', value: formatValue(inference.selectedRecord.industry_display ?? inference.selectedRecord.industry) },
                  {
                    label: '主题标签',
                    value: inferenceThemeTags.length ? <div className="badge-row">{inferenceThemeTags.map((tag) => <Badge key={tag}>{tag}</Badge>)}</div> : '暂无主题',
                    span: 'double',
                  },
                ]}
              />
            </SpotlightCard>
          ) : (
            <div className="empty-state">暂无最新推理结论</div>
          )}
          {historical?.selectedRecord && Object.keys(historical.selectedRecord).length ? (
            <SpotlightCard
              className="ai-review-focus-card ai-review-focus-card--historical"
              title={String(historical.selectedRecord.name ?? '-')}
              meta={`历史验证 / ${String(historical.selectedRecord.ts_code ?? '')}`}
              subtitle={String(historical.selectedRecord.thesis_summary ?? historical.selectedRecord.action_hint ?? '暂无结论')}
              badges={[
                { label: String(historical.selectedRecord.action_hint ?? '继续观察'), tone: 'brand' },
                { label: `置信度 ${String(historical.selectedRecord.confidence_level ?? '-')}`, tone: getConfidenceTone(historical.selectedRecord.confidence_level) },
              ]}
              metrics={[
                { label: '综合得分', value: historical.selectedRecord.final_score ?? '-' },
                { label: '量化得分', value: historical.selectedRecord.quant_score ?? '-' },
                { label: '叠加得分', value: historical.selectedRecord.factor_overlay_score ?? '-' },
                { label: '模型共识', value: historical.selectedRecord.model_consensus ?? '-' },
              ]}
            >
              <PropertyGrid
                items={[
                  { label: '行业', value: formatValue(historical.selectedRecord.industry_display ?? historical.selectedRecord.industry) },
                  {
                    label: '主题标签',
                    value: historicalThemeTags.length ? <div className="badge-row">{historicalThemeTags.map((tag) => <Badge key={tag}>{tag}</Badge>)}</div> : '暂无主题',
                    span: 'double',
                  },
                ]}
              />
            </SpotlightCard>
          ) : (
            <div className="empty-state">暂无历史验证结论</div>
          )}
        </div>
      </Panel>

      <div className="split-layout">
        {renderSummaryPanel('inference', '推理列表', inference, '暂无最新推理候选池')}
        {renderSummaryPanel('historical', '验证列表', historical, '暂无历史验证候选池')}
      </div>

      <div className="split-layout">
        {renderDetailPanel('inference', '推理补充', inferenceDetailQuery.data)}
        {renderDetailPanel('historical', '验证补充', historicalDetailQuery.data)}
      </div>
    </div>
  )
}
