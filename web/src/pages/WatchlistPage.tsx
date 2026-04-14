import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useLocation, useNavigate } from 'react-router-dom'

import { apiGet, apiPost } from '../api/client'
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
import { RealtimeStatusBanner } from '../components/RealtimeStatusBanner'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { useToast } from '../components/ToastProvider'
import { usePageSearchState } from '../facades/usePageSearchState'
import { watchlistDetailClient, watchlistPageClient, watchlistSummaryClient } from '../facades/dashboardPageClient'
import { formatDateTime, formatPercent, formatValue } from '../lib/format'
import { describeRealtimeSource, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import { buildAiReviewPath, buildCandidatesPath, copyShareablePageLink } from '../lib/shareLinks'
import type { BootstrapPayload, JsonRecord, WatchlistDetailPayload, WatchlistSummaryPayload } from '../types/api'

interface WatchlistPageProps {
  bootstrap?: BootstrapPayload
  authenticated?: boolean
}

type MetricTone = 'default' | 'good' | 'warn'

const WATCHLIST_COLUMNS = [
  'entry_group',
  'name',
  'industry',
  'mark_price',
  'realtime_price',
  'realtime_pct_chg',
  'unrealized_pnl_pct',
  'ensemble_rank',
  'inference_ensemble_rank',
  'premarket_plan',
  'llm_latest_status',
]

const WATCHLIST_COLUMN_LABELS = {
  entry_group: '分组',
  name: '股票',
  industry: '行业',
  mark_price: '参考价',
  realtime_price: '最新价',
  realtime_pct_chg: '盘中涨跌',
  unrealized_pnl_pct: '浮动收益率',
  ensemble_rank: '历史验证排名',
  inference_ensemble_rank: '最新推理排名',
  premarket_plan: '盘前建议',
  llm_latest_status: '研讨状态',
}

const FIELD_COLUMNS = ['field', 'value']
const FIELD_COLUMN_LABELS = { field: '字段', value: '内容' }

const REDUCE_PLAN_COLUMN_LABELS = {
  plan_stage: '阶段',
  target_price: '目标价格',
  reduce_ratio: '减仓比例',
  target_shares: '目标股数',
  distance_from_mark_pct: '距离当前价',
  estimated_realized_pnl: '预计实现盈亏',
  plan_note: '说明',
}

const DEFAULT_SCOPE_LABELS: Record<string, string> = {
  all: '全部',
  holdings: '只看持仓',
  focus: '只看重点关注',
  overlay: '只看 AI 精选',
  inference: '只看最新推理池',
  loss: '只看浮亏较大',
}

const DEFAULT_SORT_LABELS: Record<string, string> = {
  inference_rank: '最新推理排名',
  historical_rank: '历史验证排名',
  drawdown: '浮亏比例',
  market_value: '参考市值',
}

const WATCHLIST_VIEW_PRESETS = [
  { key: 'trading', label: '交易视图', columns: ['name', 'realtime_price', 'realtime_pct_chg', 'mark_price', 'premarket_plan', 'llm_latest_status'] },
  { key: 'ranking', label: '排名视图', columns: ['entry_group', 'name', 'industry', 'ensemble_rank', 'inference_ensemble_rank', 'premarket_plan'] },
]

function toneFromSignedNumber(value: unknown): MetricTone {
  if (typeof value !== 'number' || Number.isNaN(value) || value === 0) {
    return 'default'
  }
  return value > 0 ? 'good' : 'warn'
}

function buildRankLabel(rank: unknown, size: unknown, pct: unknown): string {
  if (typeof rank !== 'number' || Number.isNaN(rank)) {
    return '-'
  }
  if (typeof size === 'number' && Number.isFinite(size)) {
    return `${rank} / ${size}`
  }
  if (typeof pct === 'number' && Number.isFinite(pct)) {
    return `${rank} / 优于 ${formatPercent(pct)}`
  }
  return String(rank)
}

function buildSymbolLabel(record: JsonRecord | undefined): string {
  const code = String(record?.ts_code ?? '')
  const name = String(record?.name ?? '')
  return name ? `${code} / ${name}` : code
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim()
  }
  return '请稍后重试。'
}

function formatDetailValue(field: string, value: unknown): string {
  if (value === null || value === undefined || value === '') {
    return '-'
  }
  if (typeof value === 'boolean') {
    return value ? '是' : '否'
  }
  if (field.endsWith('_pct') || field.startsWith('mom_') || field.includes('drawdown') || field.includes('close_to_ma')) {
    if (typeof value === 'number') {
      return formatPercent(value)
    }
  }
  if (field.endsWith('_at') || field.endsWith('_time') || field.includes('date')) {
    return formatDateTime(value)
  }
  if (Array.isArray(value)) {
    return value.map((item) => String(item ?? '')).filter(Boolean).join(' / ') || '-'
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value, null, 2)
    } catch {
      return String(value)
    }
  }
  return formatValue(value)
}

function toFieldRows(record?: JsonRecord | null): JsonRecord[] {
  if (!record) {
    return []
  }
  return Object.entries(record)
    .filter(([field]) => field !== 'llm_discussion_snapshot')
    .map(([field, value]) => ({
      field,
      value: formatDetailValue(field, value),
    }))
}

export function WatchlistPage({ bootstrap, authenticated = false }: WatchlistPageProps) {
  const queryClient = useQueryClient()
  const location = useLocation()
  const navigate = useNavigate()
  const { pushToast } = useToast()
  const { params, updateParams } = usePageSearchState(watchlistPageClient)
  const [drawerSymbol, setDrawerSymbol] = useState<string | null>(null)

  const summaryQuery = useQuery({
    queryKey: watchlistSummaryClient.queryKey(params),
    queryFn: () => apiGet<WatchlistSummaryPayload>(watchlistSummaryClient.path(params)),
  })

  const records = useMemo(() => summaryQuery.data?.records ?? [], [summaryQuery.data?.records])
  const selectedSymbol = summaryQuery.data?.selectedSymbol ?? params.symbol

  const detailQuery = useQuery({
    queryKey: watchlistDetailClient.queryKey(params, selectedSymbol ?? ''),
    queryFn: () => apiGet<WatchlistDetailPayload>(watchlistDetailClient.path(params, selectedSymbol ?? '')),
    enabled: Boolean(selectedSymbol),
  })

  const refreshRealtimeMutation = useMutation({
    mutationFn: () => apiGet<WatchlistSummaryPayload>(watchlistSummaryClient.realtimePath(params)),
    onSuccess: (payload) => {
      queryClient.setQueryData(watchlistSummaryClient.queryKey(params), payload)
      if (payload.selectedSymbol) {
        queryClient.invalidateQueries({ queryKey: watchlistDetailClient.queryKey(params, String(payload.selectedSymbol)) })
      }
      const realtime = payload.realtimeStatus ?? {}
      const successCount = Number(realtime.success_symbol_count ?? 0)
      const requestedCount = Number(realtime.requested_symbol_count ?? 0)
      const sourceLabel = describeRealtimeSource(realtime.source).label
      const snapshotLabel = String(realtime.snapshot_label_display ?? '最新行情')
      pushToast({
        tone: successCount > 0 ? 'success' : 'error',
        title: '最新行情已更新',
        description: `${snapshotLabel}，覆盖 ${successCount} / ${requestedCount}，来源 ${sourceLabel}，更新时间 ${formatDateTime(realtime.fetched_at)}。`,
      })
    },
    onError: (error) => {
      pushToast({ tone: 'error', title: '拉取最新行情失败', description: toErrorMessage(error) })
    },
  })

  const generateWatchPlanMutation = useMutation({
    mutationFn: () => apiPost(watchlistPageClient.watchPlanActionPath),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['watchlist-summary'] })
      queryClient.invalidateQueries({ queryKey: ['watchlist-detail'] })
      pushToast({ tone: 'success', title: '盯盘清单已生成', description: '观察持仓页已经刷新到最新盯盘清单。' })
    },
    onError: (error) => {
      pushToast({ tone: 'error', title: '盯盘清单生成失败', description: toErrorMessage(error) })
    },
  })

  const generateActionMemoMutation = useMutation({
    mutationFn: () => apiPost(watchlistPageClient.actionMemoActionPath),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['watchlist-summary'] })
      queryClient.invalidateQueries({ queryKey: ['watchlist-detail'] })
      pushToast({ tone: 'success', title: '操作备忘已生成', description: '最新操作备忘已经回写到页面。' })
    },
    onError: (error) => {
      pushToast({ tone: 'error', title: '操作备忘生成失败', description: toErrorMessage(error) })
    },
  })

  const scopeLabels = bootstrap?.watchScopes ? { ...bootstrap.watchScopes, ...DEFAULT_SCOPE_LABELS } : DEFAULT_SCOPE_LABELS
  const sortLabels = bootstrap?.watchSorts ? { ...bootstrap.watchSorts, ...DEFAULT_SORT_LABELS } : DEFAULT_SORT_LABELS
  const summarySelectedRecord = summaryQuery.data?.selectedRecord
  const detail = detailQuery.data?.detail ?? summarySelectedRecord
  const drawerRecord = useMemo(() => {
    if (!drawerSymbol) {
      return null
    }
    if (detail && String(detail.ts_code ?? '') === drawerSymbol) {
      return detail
    }
    return records.find((row) => String(row.ts_code ?? '') === drawerSymbol) ?? null
  }, [detail, drawerSymbol, records])
  const drawerFieldRows = useMemo(() => toFieldRows(drawerRecord), [drawerRecord])
  const realtimeStatus = summaryQuery.data?.realtimeStatus ?? {}
  const requestedCount = Number(realtimeStatus.requested_symbol_count ?? 0)
  const successCount = Number(realtimeStatus.success_symbol_count ?? 0)
  const failedSymbols = useMemo(() => normalizeRealtimeFailedSymbols(realtimeStatus.failed_symbols), [realtimeStatus.failed_symbols])
  const selectedRealtimeSource = describeRealtimeSource(detail?.realtime_quote_source ?? realtimeStatus.source).label
  const selectedRealtimeTime = detail?.realtime_time ?? realtimeStatus.fetched_at
  const writeLocked = !authenticated

  const recordOptions = useMemo(
    () =>
      records.map((row) => ({
        value: String(row.ts_code ?? ''),
        label: buildSymbolLabel(row),
      })),
    [records],
  )

  const watchlistCellRenderers = useMemo(
    () => ({
      name: (row: JsonRecord) => (
        <EntityCell
          title={String(row.name ?? '-')}
          subtitle={String(row.ts_code ?? '')}
          meta={String(row.industry ?? row.entry_group ?? '')}
          badges={[
            row.is_overlay_selected ? { label: '历史精选', tone: 'good' as const } : null,
            row.is_inference_overlay_selected ? { label: '最新推理', tone: 'brand' as const } : null,
          ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
        />
      ),
    }),
    [],
  )

  const watchlistContextItems = [
    { label: '当前范围', value: scopeLabels[params.scope] ?? params.scope, tone: 'brand' as const },
    { label: '排序方式', value: sortLabels[params.sortBy] ?? params.sortBy },
    {
      label: '筛选结果',
      value: summaryQuery.data?.filteredCount ?? 0,
      helper: `总计 ${summaryQuery.data?.overview.totalCount ?? 0}`,
    },
    {
      label: '行情模式',
      value: String(realtimeStatus.snapshot_label_display ?? '暂无快照'),
      helper: describeRealtimeSource(realtimeStatus.source).label,
    },
    {
      label: '当前标的',
      value: detail ? buildSymbolLabel(detail) : selectedSymbol || '-',
      helper: String(detail?.watch_level ?? detail?.action_brief ?? ''),
    },
    {
      label: '快照覆盖',
      value: requestedCount > 0 ? `${successCount} / ${requestedCount}` : '未刷新',
      helper: formatDateTime(realtimeStatus.fetched_at),
      tone: failedSymbols.length ? ('warn' as const) : requestedCount > 0 ? ('good' as const) : ('default' as const),
    },
  ]

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

  const openCandidatesPage = (symbol: string) => {
    if (!symbol) {
      return
    }
    navigate(buildCandidatesPath(symbol))
    setDrawerSymbol(null)
  }

  return (
    <div className="page-stack">
      <Panel
        title="观察持仓"
        subtitle={`当前 ${summaryQuery.data?.filteredCount ?? 0} 条记录。默认展示最新快照，手动刷新才拉实时行情。`}
        tone="warm"
        className="panel--summary-surface"
      >
        <QueryNotice isLoading={summaryQuery.isLoading} error={summaryQuery.error} loadingText="正在加载最新观察池..." />
        {writeLocked ? <div className="query-notice query-notice--info">当前未登录。可以查看行情和列表，但生成盯盘清单与操作备忘前需要先登录。</div> : null}

        <RealtimeStatusBanner
          status={realtimeStatus}
          isRefreshing={refreshRealtimeMutation.isPending}
          error={refreshRealtimeMutation.isError ? refreshRealtimeMutation.error : undefined}
          onRefresh={() => refreshRealtimeMutation.mutate()}
          onRetryFailed={() => refreshRealtimeMutation.mutate()}
        />

        <ContextStrip items={watchlistContextItems} />

        <SectionBlock title="先看观察池概况" description="先看总量和盈亏，再下钻单票。">
          <div className="metric-grid">
            <MetricCard label="观察池股票数" value={summaryQuery.data?.overview.totalCount ?? 0} />
            <MetricCard label="历史精选数" value={summaryQuery.data?.overview.overlayCount ?? 0} />
            <MetricCard label="最新推理池" value={summaryQuery.data?.overview.inferenceOverlayCount ?? 0} />
            <MetricCard label="参考市值" value={summaryQuery.data?.overview.marketValue ?? 0} />
            <MetricCard label="浮动盈亏" value={summaryQuery.data?.overview.unrealizedPnl ?? 0} tone={toneFromSignedNumber(summaryQuery.data?.overview.unrealizedPnl)} />
          </div>
        </SectionBlock>

        <PageFilterBar title="切换观察工作区" description="先筛选列表，再看单票摘要。">
          <ControlGrid variant="quad">
            <ControlField label="快速搜索">
              <input value={params.keyword} onChange={(event) => updateParams({ keyword: event.target.value })} placeholder="输入代码或名称" />
            </ControlField>
            <ControlField label="查看范围">
              <select value={params.scope} onChange={(event) => updateParams({ scope: event.target.value })}>
                {Object.entries(scopeLabels).map(([key, label]) => (
                  <option key={key} value={key}>
                    {label}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="排序方式">
              <select value={params.sortBy} onChange={(event) => updateParams({ sortBy: event.target.value })}>
                {Object.entries(sortLabels).map(([key, label]) => (
                  <option key={key} value={key}>
                    {label}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="查看股票">
              <select value={selectedSymbol ?? ''} onChange={(event) => updateParams({ symbol: event.target.value })}>
                <option value="">跟随筛选结果</option>
                {recordOptions.map((item) => (
                  <option key={item.value} value={item.value}>
                    {item.label}
                  </option>
                ))}
              </select>
            </ControlField>
          </ControlGrid>
        </PageFilterBar>
      </Panel>

      <Panel title="观察池主表" subtitle="先筛选总表，细节看右侧和抽屉。" tone="calm" className="panel--table-surface">
        <DataTable
          rows={records}
          columns={WATCHLIST_COLUMNS}
          columnLabels={WATCHLIST_COLUMN_LABELS}
          storageKey="watchlist-matrix"
          viewPresets={WATCHLIST_VIEW_PRESETS}
          defaultPresetKey="trading"
          loading={summaryQuery.isLoading}
          loadingText="正在加载观察池..."
          emptyText="暂无观察池数据"
          stickyFirstColumn
          getRowId={(row) => String(row.ts_code ?? '')}
          selectedRowId={selectedSymbol ?? null}
          onRowClick={(row) => openDrawer(String(row.ts_code ?? ''))}
          cellRenderers={watchlistCellRenderers}
        />
      </Panel>

      <div className="split-layout">
        <Panel title="当前关注标的" subtitle="先看单票摘要和关键价位。" className="panel--summary-surface">
          <QueryNotice isLoading={detailQuery.isLoading} error={detailQuery.error} />
          {detail ? (
            <div className="section-stack">
              <SectionBlock
                title="核心摘要"
                description="执行建议和关键价位前置。"
                tone="emphasis"
                actions={
                  <div className="inline-actions inline-actions--compact">
                    <button type="button" className="button" disabled={writeLocked || generateWatchPlanMutation.isPending} onClick={() => generateWatchPlanMutation.mutate()}>
                      {generateWatchPlanMutation.isPending ? '生成中...' : '生成盯盘清单'}
                    </button>
                    <button type="button" className="button" disabled={writeLocked || generateActionMemoMutation.isPending} onClick={() => generateActionMemoMutation.mutate()}>
                      {generateActionMemoMutation.isPending ? '生成中...' : '生成操作备忘'}
                    </button>
                  </div>
                }
              >
                <SpotlightCard
                  title={String(detail.name ?? '-')}
                  meta={String(detail.ts_code ?? '')}
                  subtitle={String(detail.premarket_plan ?? detail.watch_level ?? detail.action_brief ?? '暂无摘要')}
                  badges={[
                    { label: String(detail.entry_group ?? '观察池'), tone: 'brand' },
                    detail.is_watch_only ? { label: '仅观察', tone: 'warn' } : null,
                    detail.is_overlay_selected ? { label: '历史精选', tone: 'good' } : null,
                    detail.is_inference_overlay_selected ? { label: '最新推理', tone: 'good' } : null,
                  ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
                  metrics={[
                    { label: '参考价', value: detail.mark_price ?? '-' },
                    { label: '最新价', value: detail.realtime_price ?? '-', helper: selectedRealtimeSource },
                    { label: '盘中涨跌', value: formatPercent(detail.realtime_pct_chg), tone: toneFromSignedNumber(detail.realtime_pct_chg), helper: formatDateTime(selectedRealtimeTime) },
                    { label: '历史验证排名', value: buildRankLabel(detail.ensemble_rank, detail.universe_size, detail.ensemble_rank_pct) },
                    { label: '最新推理排名', value: buildRankLabel(detail.inference_ensemble_rank, null, detail.inference_ensemble_rank_pct) },
                    { label: '防守位', value: detail.defensive_price ?? '-', tone: 'warn' },
                  ]}
                />
              </SectionBlock>

              <SectionBlock title="执行参考" description="补充价格说明和研讨状态。">
                <PropertyGrid
                  items={[
                    { label: '价格状态', value: formatValue(detail.mark_status) },
                    { label: '状态说明', value: formatValue(detail.mark_status_note), span: 'double' },
                    { label: '动作提示', value: formatValue(detail.action_brief), span: 'double' },
                    { label: '解套价', value: formatValue(detail.breakeven_price) },
                    { label: '关注备注', value: formatValue(detail.focus_note || '暂无') },
                    { label: '排名备注', value: formatValue(detail.ranking_note), span: 'double' },
                    { label: '最新研讨状态', value: formatValue(detail.llm_latest_status) },
                    { label: '最新研讨摘要', value: formatValue(detail.llm_latest_summary), span: 'double' },
                  ]}
                />
              </SectionBlock>
            </div>
          ) : (
            <div className="empty-state">暂无单票详情</div>
          )}
        </Panel>

        <SupportPanel title="盘中与研究" subtitle="盘中状态和研究补充后置。">
          <SectionBlock title="盘中状态" description="确认快照来源和覆盖率。">
            <div className="metric-grid metric-grid--compact">
              <MetricCard label="实时来源" value={describeRealtimeSource(realtimeStatus.source).label} helper={formatDateTime(realtimeStatus.fetched_at)} />
              <MetricCard label="刷新覆盖" value={requestedCount > 0 ? `${successCount} / ${requestedCount}` : '未刷新'} tone={requestedCount > 0 && successCount === requestedCount ? 'good' : 'warn'} />
              <MetricCard label="盘中涨跌" value={formatPercent(detail?.realtime_pct_chg)} tone={toneFromSignedNumber(detail?.realtime_pct_chg)} />
              <MetricCard label="盘中振幅" value={formatPercent(detail?.realtime_amplitude)} tone={toneFromSignedNumber(detail?.realtime_amplitude)} />
            </div>
            <PropertyGrid
              items={[
                { label: '最近刷新时间', value: formatDateTime(realtimeStatus.fetched_at) },
                { label: '最近轮次', value: formatValue(detailQuery.data?.detail?.llm_latest_round) },
                { label: '失败股票', value: failedSymbols.length ? failedSymbols.join(' / ') : '无', span: 'double', tone: failedSymbols.length ? 'warn' : 'good' },
              ]}
            />
            <div className="inline-actions inline-actions--compact">
              <button type="button" className="button button--ghost" onClick={() => detail?.ts_code && openAiReview(String(detail.ts_code))}>
                打开 AI 研判页
              </button>
            </div>
          </SectionBlock>
        </SupportPanel>
      </div>

      <div className="split-layout">
        <SupportPanel title="分批观察计划" subtitle="计划单独后置。">
          <DataTable
            rows={detailQuery.data?.reducePlan ?? []}
            columnLabels={REDUCE_PLAN_COLUMN_LABELS}
            storageKey="watchlist-reduce-plan"
            emptyText="暂无计划表"
          />
        </SupportPanel>

        <SupportPanel title="评分历史" subtitle="历史曲线单独后置。">
          <LineChartCard
            data={detailQuery.data?.history ?? []}
            xKey="trade_date"
            lineKeys={['score']}
            title="历史评分曲线"
            subtitle="观察这只股票在历史验证中的评分变化。"
          />
        </SupportPanel>
      </div>

      <DetailDrawer
        open={Boolean(drawerRecord)}
        title={drawerRecord ? buildSymbolLabel(drawerRecord) : '持仓详情'}
        subtitle={drawerRecord ? String(drawerRecord.watch_level ?? drawerRecord.action_brief ?? '查看当前持仓的完整字段。') : undefined}
        meta={
          drawerRecord ? (
            <div className="badge-row">
              <Badge tone="brand">{String(drawerRecord.entry_group ?? '观察池')}</Badge>
              {drawerRecord.is_watch_only ? <Badge tone="warn">仅观察</Badge> : <Badge tone="good">持仓中</Badge>}
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
              subtitle={String(drawerRecord.premarket_plan ?? drawerRecord.watch_level ?? drawerRecord.action_brief ?? '查看当前持仓的核心信息。')}
              badges={[
                { label: String(drawerRecord.entry_group ?? '观察池'), tone: 'brand' },
                drawerRecord.is_watch_only ? { label: '仅观察', tone: 'warn' } : { label: '持仓中', tone: 'good' },
              ]}
              metrics={[
                { label: '参考价', value: drawerRecord.mark_price ?? '-' },
                { label: '最新价', value: drawerRecord.realtime_price ?? '-', helper: describeRealtimeSource(drawerRecord.realtime_quote_source).label },
                { label: '浮动收益率', value: formatPercent(drawerRecord.unrealized_pnl_pct), tone: toneFromSignedNumber(drawerRecord.unrealized_pnl_pct) },
              ]}
              properties={[
                { label: '盘前建议', value: formatValue(drawerRecord.premarket_plan), span: 'double', tone: 'good' },
                { label: '价格状态', value: formatValue(drawerRecord.mark_status) },
                { label: '解套价', value: formatValue(drawerRecord.breakeven_price) },
                { label: '实时来源', value: describeRealtimeSource(drawerRecord.realtime_quote_source).label },
                { label: '实时更新时间', value: formatDateTime(drawerRecord.realtime_time) },
                { label: '最新研讨状态', value: formatValue(drawerRecord.llm_latest_status) },
                { label: '最新研讨摘要', value: formatValue(drawerRecord.llm_latest_summary), span: 'double' },
              ]}
            />

            <DrawerQuickActions
              title="快捷操作"
              description="需要时再执行刷新、生成和跳转。"
              primaryActions={[
                {
                  key: 'generate-watch-plan',
                  label: generateWatchPlanMutation.isPending ? '生成中...' : '生成盯盘清单',
                  onClick: () => generateWatchPlanMutation.mutate(),
                  disabled: writeLocked || generateWatchPlanMutation.isPending,
                },
                {
                  key: 'generate-action-memo',
                  label: generateActionMemoMutation.isPending ? '生成中...' : '生成操作备忘',
                  onClick: () => generateActionMemoMutation.mutate(),
                  disabled: writeLocked || generateActionMemoMutation.isPending,
                },
              ]}
              secondaryActions={[
                { key: 'copy-symbol', label: '复制股票代码', onClick: () => copySymbol(String(drawerRecord.ts_code ?? '')), tone: 'ghost' },
                { key: 'copy-view', label: '复制当前视图', onClick: copyCurrentViewLink, tone: 'ghost' },
                { key: 'open-ai-review', label: '跳到 AI 研判', onClick: () => openAiReview(String(drawerRecord.ts_code ?? '')), tone: 'ghost' },
                { key: 'open-candidates', label: '跳到候选页', onClick: () => openCandidatesPage(String(drawerRecord.ts_code ?? '')), tone: 'ghost' },
              ]}
            />

            <details className="details-block">
              <summary>查看完整字段</summary>
              <DataTable
                rows={drawerFieldRows}
                columns={FIELD_COLUMNS}
                columnLabels={FIELD_COLUMN_LABELS}
                storageKey="watchlist-detail-fields"
                stickyFirstColumn
                enableColumnManager={false}
                density="comfortable"
                emptyText="暂无详情字段"
              />
            </details>
          </div>
        ) : null}
      </DetailDrawer>
    </div>
  )
}
