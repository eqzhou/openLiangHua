import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate } from 'react-router-dom'

import { apiGet } from '../api/client'
import { Badge } from '../components/Badge'
import { ControlField } from '../components/ControlField'
import { ControlGrid } from '../components/ControlGrid'
import { ContextStrip } from '../components/ContextStrip'
import { DataTable } from '../components/DataTable'
import { DetailDrawer } from '../components/DetailDrawer'
import { DetailSummarySection } from '../components/DetailSummarySection'
import { DrawerQuickActions } from '../components/DrawerQuickActions'
import { EntityCell } from '../components/EntityCell'
import { LineChartCard } from '../components/LineChartCard'
import { MetricCard } from '../components/MetricCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SegmentedControl } from '../components/SegmentedControl'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { useToast } from '../components/ToastProvider'
import { candidateHistoryClient, candidatesPageClient, candidatesSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDate, formatPercent, formatValue, recordToFieldRows } from '../lib/format'
import { buildAiReviewPath, buildWatchlistPath, copyShareablePageLink } from '../lib/shareLinks'
import type { BootstrapPayload, CandidateHistoryPayload, CandidatesSummaryPayload, JsonRecord } from '../types/api'

interface CandidatesPageProps {
  bootstrap?: BootstrapPayload
}

const PICKS_COLUMNS = ['name', 'rank', 'industry', 'score', 'rank_pct', 'pct_chg', 'mom_20', 'close_to_ma_20', 'ret_t1_t10']

const PICK_COLUMN_LABELS = {
  name: '股票',
  rank: '排名',
  industry: '行业',
  score: '综合分数',
  rank_pct: '排名分位',
  pct_chg: '当日涨跌',
  mom_20: '20日动量',
  close_to_ma_20: '距20日线',
  ret_t1_t10: '未来10日收益',
}

const FIELD_COLUMNS = ['field', 'value']

const CANDIDATES_VIEW_PRESETS = [
  { key: 'decision', label: '决策视图', columns: ['name', 'rank', 'score', 'rank_pct', 'ret_t1_t10', 'pct_chg'] },
  { key: 'momentum', label: '动量视图', columns: ['name', 'rank', 'pct_chg', 'mom_20', 'close_to_ma_20', 'ret_t1_t10'] },
]

function buildOptionLabel(symbol: string, records: JsonRecord[]): string {
  const match = records.find((row) => String(row.ts_code ?? '') === symbol)
  const name = String(match?.name ?? '')
  return name ? `${symbol} / ${name}` : symbol
}

function buildScoreViewOptions(keys: string[]) {
  if (!keys.length) {
    return []
  }
  if (keys.length === 1) {
    return [{ key: 'single', label: '单线', lineKeys: keys, subtitle: '当前只有一条评分序列可用。' }]
  }
  return [
    { key: 'combined', label: '评分与收益', lineKeys: keys, subtitle: '把评分和后续收益放在一起看，先判断信号质量。' },
    { key: 'score', label: '只看评分', lineKeys: [keys[0]], subtitle: '单独观察综合评分是否还在抬升。' },
    { key: 'forward', label: '只看收益', lineKeys: [keys[keys.length - 1]], subtitle: '单独观察后续收益窗口的兑现情况。' },
  ]
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim()
  }
  return '请稍后重试。'
}

export function CandidatesPage({ bootstrap }: CandidatesPageProps) {
  const location = useLocation()
  const navigate = useNavigate()
  const { pushToast } = useToast()
  const { params, updateParams } = usePageSearchState(candidatesPageClient)
  const [drawerSymbol, setDrawerSymbol] = useState<string | null>(null)
  const [scoreView, setScoreView] = useState('combined')

  const summaryQuery = useQuery({
    queryKey: candidatesSummaryClient.queryKey(params),
    queryFn: () => apiGet<CandidatesSummaryPayload>(candidatesSummaryClient.path(params)),
  })

  const latestPicks = useMemo(() => summaryQuery.data?.latestPicks ?? [], [summaryQuery.data?.latestPicks])
  const symbolOptions = summaryQuery.data?.symbolOptions ?? []
  const selectedSymbol = summaryQuery.data?.selectedSymbol ?? params.symbol

  const historyQuery = useQuery({
    queryKey: candidateHistoryClient.queryKey(params, selectedSymbol ?? ''),
    queryFn: () => apiGet<CandidateHistoryPayload>(candidateHistoryClient.path(params, selectedSymbol ?? '')),
    enabled: Boolean(selectedSymbol),
  })

  const scoreHistory = useMemo(() => historyQuery.data?.scoreHistory ?? [], [historyQuery.data?.scoreHistory])
  const scoreKeys = useMemo(() => Object.keys(scoreHistory[0] ?? {}).filter((key) => key !== 'trade_date'), [scoreHistory])
  const scoreViewOptions = useMemo(() => buildScoreViewOptions(scoreKeys), [scoreKeys])
  const activeScoreView = scoreViewOptions.find((item) => item.key === scoreView) ?? scoreViewOptions[0]

  const selectedPick = useMemo(() => {
    if (!latestPicks.length) {
      const summaryRecord = summaryQuery.data?.selectedRecord ?? {}
      return Object.keys(summaryRecord).length ? summaryRecord : undefined
    }
    return latestPicks.find((row) => String(row.ts_code ?? '') === selectedSymbol) ?? latestPicks[0]
  }, [latestPicks, selectedSymbol, summaryQuery.data?.selectedRecord])

  const drawerRecord = useMemo(() => {
    if (!drawerSymbol) {
      return null
    }
    return latestPicks.find((row) => String(row.ts_code ?? '') === drawerSymbol) ?? null
  }, [drawerSymbol, latestPicks])

  const drawerFieldRows = useMemo(() => (drawerRecord ? recordToFieldRows(drawerRecord) : []), [drawerRecord])
  const candidateCount = latestPicks.length
  const positiveCount = latestPicks.filter((row) => typeof row.ret_t1_t10 === 'number' && Number(row.ret_t1_t10) > 0).length

  const candidateCellRenderers = useMemo(
    () => ({
      name: (row: JsonRecord) => (
        <EntityCell
          title={String(row.name ?? '-')}
          subtitle={String(row.ts_code ?? '')}
          meta={String(row.industry ?? '')}
          badges={String(row.action_hint ?? '').trim() ? [{ label: String(row.action_hint), tone: 'brand' as const }] : []}
        />
      ),
    }),
    [],
  )

  const candidatesContextItems = useMemo(
    () => [
      { label: '信号日期', value: summaryQuery.data?.latestDate ?? '-' },
      { label: '结果模型', value: String(summaryQuery.data?.modelName ?? '-').toUpperCase(), tone: 'brand' as const },
      { label: '样本分段', value: String(summaryQuery.data?.splitName ?? '-').toUpperCase() },
      { label: '候选数量', value: candidateCount, helper: `Top ${summaryQuery.data?.topN ?? params.topN}` },
      {
        label: '当前聚焦',
        value: selectedPick ? buildOptionLabel(String(selectedPick.ts_code ?? ''), latestPicks) : '-',
        helper: String(selectedPick?.action_hint ?? ''),
      },
    ],
    [candidateCount, latestPicks, params.topN, selectedPick, summaryQuery.data?.latestDate, summaryQuery.data?.modelName, summaryQuery.data?.splitName, summaryQuery.data?.topN],
  )

  const openDrawer = (symbol: string) => {
    if (!symbol) {
      return
    }
    updateParams({ symbol })
    setDrawerSymbol(symbol)
  }

  const copySymbol = async (symbol: string) => {
    if (!symbol) {
      return
    }
    try {
      await navigator.clipboard.writeText(symbol)
      pushToast({ tone: 'success', title: '已复制股票代码', description: symbol })
    } catch (error) {
      pushToast({ tone: 'error', title: '复制股票代码失败', description: toErrorMessage(error) })
    }
  }

  const copyCurrentViewLink = async () => {
    try {
      const shareUrl = await copyShareablePageLink(location.pathname, location.search)
      pushToast({ tone: 'success', title: '已复制当前视图链接', description: shareUrl })
    } catch (error) {
      pushToast({ tone: 'error', title: '复制视图链接失败', description: toErrorMessage(error) })
    }
  }

  const openAiReview = (symbol: string) => {
    if (!symbol) {
      return
    }
    navigate(buildAiReviewPath(symbol))
    setDrawerSymbol(null)
  }

  const openWatchlistPage = (symbol: string) => {
    if (!symbol) {
      return
    }
    navigate(buildWatchlistPath(symbol))
    setDrawerSymbol(null)
  }

  return (
    <div className="page-stack">
      <Panel
        title="候选股票"
        subtitle={
          summaryQuery.data?.latestDate
            ? `当前查看 ${String(summaryQuery.data.modelName).toUpperCase()} 在 ${String(summaryQuery.data.splitName).toUpperCase()} 截面的候选池，信号日期 ${formatDate(summaryQuery.data.latestDate)}。`
            : '先看候选池，再按需展开单票历史。'
        }
        tone="warm"
        className="panel--summary-surface"
      >
        <QueryNotice isLoading={summaryQuery.isLoading} error={summaryQuery.error} />

        <SectionBlock title="先看候选池概况" description="先看数量、模型和正收益样本。" tone="emphasis">
          <div className="metric-grid metric-grid--four">
            <MetricCard label="候选池数量" value={candidateCount} />
            <MetricCard label="模型" value={String(summaryQuery.data?.modelName ?? '-').toUpperCase()} />
            <MetricCard label="样本分段" value={String(summaryQuery.data?.splitName ?? '-').toUpperCase()} />
            <MetricCard label="正收益样本数" value={positiveCount} tone={positiveCount > 0 ? 'good' : 'default'} />
          </div>
        </SectionBlock>

        <ContextStrip items={candidatesContextItems} />

        <PageFilterBar title="切换候选池视角" description="这里只控制候选摘要。">
          <ControlGrid variant="quad">
            <ControlField label="结果模型">
              <select value={params.model} onChange={(event) => updateParams({ model: event.target.value })}>
                {(bootstrap?.modelNames ?? ['ridge', 'lgbm', 'ensemble']).map((item) => (
                  <option key={item} value={item}>
                    {bootstrap?.modelLabels?.[item] ?? item}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="数据集">
              <select value={params.split} onChange={(event) => updateParams({ split: event.target.value })}>
                {(bootstrap?.splitNames ?? ['valid', 'test']).map((item) => (
                  <option key={item} value={item}>
                    {bootstrap?.splitLabels?.[item] ?? item}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="候选数量">
              <input type="number" min={3} max={30} value={params.topN} onChange={(event) => updateParams({ topN: Number(event.target.value) || 10 })} />
            </ControlField>
            <ControlField label="查看股票">
              <select value={selectedSymbol ?? ''} onChange={(event) => updateParams({ symbol: event.target.value })}>
                {symbolOptions.map((symbol) => (
                  <option key={symbol} value={symbol}>
                    {buildOptionLabel(symbol, latestPicks)}
                  </option>
                ))}
              </select>
            </ControlField>
          </ControlGrid>
        </PageFilterBar>
      </Panel>

      <Panel title="候选池主表" subtitle="主表只负责总列表和切换。" tone="calm" className="panel--table-surface">
        <DataTable
          rows={latestPicks}
          columns={PICKS_COLUMNS}
          columnLabels={PICK_COLUMN_LABELS}
          storageKey="candidate-picks"
          viewPresets={CANDIDATES_VIEW_PRESETS}
          defaultPresetKey="decision"
          loading={summaryQuery.isLoading}
          emptyText="暂无候选股票数据"
          stickyFirstColumn
          getRowId={(row) => String(row.ts_code ?? '')}
          selectedRowId={selectedSymbol ?? null}
          onRowClick={(row) => openDrawer(String(row.ts_code ?? ''))}
          cellRenderers={candidateCellRenderers}
        />
      </Panel>

      <div className="split-layout">
        <SupportPanel title="当前候选标的" subtitle="先在主表选中，再看单票摘要。">
          {selectedPick ? (
            <div className="section-stack">
              <SectionBlock
                title="核心摘要"
                description="先承接主表里的排名和总分。"
                tone="emphasis"
              >
                <SpotlightCard
                  title={String(selectedPick.name ?? '-')}
                  meta={String(selectedPick.ts_code ?? '')}
                  subtitle={String(selectedPick.action_hint ?? '暂无建议')}
                  badges={[
                    { label: '候选池', tone: 'brand' },
                    { label: formatDate(summaryQuery.data?.latestDate) },
                  ]}
                  actions={
                    <div className="inline-actions inline-actions--compact">
                      <button type="button" className="button button--ghost button--small" onClick={() => openDrawer(String(selectedPick.ts_code ?? ''))}>
                        打开详情抽屉
                      </button>
                    </div>
                  }
                  metrics={[
                    { label: '当前排名', value: selectedPick.rank ?? '-' },
                    { label: '综合分数', value: selectedPick.score ?? '-' },
                    { label: '排名分位', value: formatPercent(selectedPick.rank_pct) },
                    { label: '未来10日收益', value: formatPercent(selectedPick.ret_t1_t10) },
                  ]}
                />
              </SectionBlock>

              <SectionBlock title="交易背景" description="行业、涨跌和动量后置。" collapsible defaultExpanded={false}>
                <PropertyGrid
                  items={[
                    { label: '行业', value: formatValue(selectedPick.industry) },
                    { label: '当日涨跌', value: formatPercent(selectedPick.pct_chg) },
                    { label: '20日动量', value: formatPercent(selectedPick.mom_20) },
                    { label: '距20日线', value: formatPercent(selectedPick.close_to_ma_20) },
                  ]}
                />
              </SectionBlock>
            </div>
          ) : (
            <div className="empty-state">暂无候选详情</div>
          )}
        </SupportPanel>

        <SupportPanel title="评分历史" subtitle="评分历史按当前选中标的单独加载。">
          <QueryNotice isLoading={historyQuery.isLoading} error={historyQuery.error} />
          {scoreViewOptions.length ? (
            <SegmentedControl
              label="切换评分视图"
              value={activeScoreView?.key ?? scoreViewOptions[0].key}
              options={scoreViewOptions.map((item) => ({ key: item.key, label: item.label }))}
              onChange={setScoreView}
            />
          ) : null}
          <LineChartCard
            data={scoreHistory}
            xKey="trade_date"
            lineKeys={activeScoreView?.lineKeys ?? scoreKeys}
            title="候选评分曲线"
            subtitle={activeScoreView?.subtitle ?? '暂无历史曲线'}
          />
        </SupportPanel>
      </div>

      <DetailDrawer
        open={Boolean(drawerRecord)}
        title={drawerRecord ? buildOptionLabel(String(drawerRecord.ts_code ?? ''), latestPicks) : '候选详情'}
        subtitle={drawerRecord ? String(drawerRecord.action_hint ?? '查看当前候选的完整字段。') : undefined}
        meta={
          drawerRecord ? (
            <div className="badge-row">
              <Badge tone="brand">候选池</Badge>
              <Badge>{formatDate(summaryQuery.data?.latestDate)}</Badge>
            </div>
          ) : null
        }
        onClose={() => setDrawerSymbol(null)}
      >
        {drawerRecord ? (
          <div className="section-stack">
            <DetailSummarySection
              title={String(drawerRecord.name ?? '-')}
              meta={String(drawerRecord.ts_code ?? '')}
              subtitle={String(drawerRecord.action_hint ?? '查看当前候选的核心结论。')}
              badges={[
                { label: '候选池', tone: 'brand' },
                { label: formatDate(summaryQuery.data?.latestDate) },
              ]}
              metrics={[
                { label: '排名', value: drawerRecord.rank ?? '-' },
                { label: '综合分数', value: drawerRecord.score ?? '-' },
                { label: '未来10日收益', value: formatPercent(drawerRecord.ret_t1_t10) },
              ]}
              properties={[
                { label: '行业', value: formatValue(drawerRecord.industry) },
                { label: '当日涨跌', value: formatPercent(drawerRecord.pct_chg) },
                { label: '20日动量', value: formatPercent(drawerRecord.mom_20) },
                { label: '距20日线', value: formatPercent(drawerRecord.close_to_ma_20) },
              ]}
            />

            <DrawerQuickActions
              title="快捷操作"
              description="需要时再执行复制、分享和跳转。"
              primaryActions={[
                { key: 'copy-symbol', label: '复制股票代码', onClick: () => copySymbol(String(drawerRecord.ts_code ?? '')), tone: 'primary' },
                { key: 'copy-view', label: '复制当前视图', onClick: copyCurrentViewLink },
              ]}
              secondaryActions={[
                { key: 'open-ai-review', label: '跳到 AI 研判', onClick: () => openAiReview(String(drawerRecord.ts_code ?? '')), tone: 'ghost' },
                { key: 'open-watchlist', label: '跳到观察持仓', onClick: () => openWatchlistPage(String(drawerRecord.ts_code ?? '')), tone: 'ghost' },
              ]}
            />

            <details className="details-block">
              <summary>查看完整字段</summary>
              <DataTable
                rows={drawerFieldRows}
                columns={FIELD_COLUMNS}
                storageKey="candidate-detail-fields"
                stickyFirstColumn
                enableColumnManager={false}
                density="comfortable"
                emptyText="暂无候选详情字段"
              />
            </details>
          </div>
        ) : null}
      </DetailDrawer>
    </div>
  )
}
