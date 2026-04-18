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
import { MarkdownCard } from '../components/MarkdownCard'
import { MetricCard } from '../components/MetricCard'
import { MobileInspectionCard } from '../components/MobileInspectionCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { RealtimeStatusBanner } from '../components/RealtimeStatusBanner'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { useToast } from '../components/ToastProvider'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { usePageSearchState } from '../facades/usePageSearchState'
import { realtimeRefreshClient, watchlistDetailClient, watchlistPageClient, watchlistSummaryClient } from '../facades/dashboardPageClient'
import { formatDateTime, formatPercent, formatValue, getFieldLabel } from '../lib/format'
import { describeRealtimeSource, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import { buildAiReviewPath, buildCandidatesPath, copyShareablePageLink } from '../lib/shareLinks'
import type { BootstrapPayload, JsonRecord, RealtimeRefreshPayload, WatchlistDetailPayload, WatchlistSummaryPayload } from '../types/api'

interface WatchlistPageProps {
  bootstrap?: BootstrapPayload
  authenticated?: boolean
}

type MetricTone = 'default' | 'good' | 'warn'

const WATCHLIST_COLUMNS = [
  'source_tags',
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
  source_tags: '来源标签',
  name: '股票',
  industry: '行业',
  mark_price: '参考价',
  realtime_price: '最新价',
  realtime_pct_chg: '盘中涨跌',
  unrealized_pnl_pct: '浮动收益率',
  ensemble_rank: '历史验证排名',
  inference_ensemble_rank: '最新推理排名',
  premarket_plan: '执行建议',
  llm_latest_status: '分析状态',
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
  { key: 'trading', label: '交易', columns: ['name', 'realtime_price', 'realtime_pct_chg', 'mark_price', 'premarket_plan', 'llm_latest_status'] },
  { key: 'ranking', label: '排名', columns: ['source_tags', 'name', 'industry', 'ensemble_rank', 'inference_ensemble_rank', 'premarket_plan'] },
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
  return '操作失败，请稍后再试。'
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
      field: getFieldLabel(field),
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

  const drawerDetailQuery = useQuery({
    queryKey: watchlistDetailClient.queryKey(params, drawerSymbol ?? ''),
    queryFn: () => apiGet<WatchlistDetailPayload>(watchlistDetailClient.path(params, drawerSymbol ?? '')),
    enabled: Boolean(drawerSymbol),
  })

  const refreshRealtimeMutation = useMutation({
    mutationFn: () => apiPost<RealtimeRefreshPayload>(realtimeRefreshClient.path()),
    onSuccess: (payload) => {
      void queryClient.invalidateQueries()
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
      pushToast({ tone: 'success', title: '盯盘清单已生成', description: '持仓页已经刷新到最新盯盘清单。' })
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
    const drawerDetail = drawerDetailQuery.data?.detail
    if (drawerDetail && String(drawerDetail.ts_code ?? '') === drawerSymbol) {
      return drawerDetail
    }
    return records.find((row) => String(row.ts_code ?? '') === drawerSymbol) ?? null
  }, [drawerDetailQuery.data?.detail, drawerSymbol, records])
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
          meta={String(row.industry ?? row.source_category ?? row.entry_group ?? '')}
          badges={[
            row.source_category ? { label: String(row.source_category), tone: 'brand' as const } : null,
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
      label: '当前股票',
      value: detail ? buildSymbolLabel(detail) : selectedSymbol || '-',
      helper: String(detail?.watch_level ?? detail?.action_brief ?? ''),
    },
    {
      label: '快照覆盖',
      value: requestedCount > 0 ? `${successCount} / ${requestedCount}` : '待刷新',
      helper: formatDateTime(realtimeStatus.fetched_at),
      tone: failedSymbols.length ? ('warn' as const) : requestedCount > 0 ? ('good' as const) : ('default' as const),
    },
  ]

  const watchlistHeroBadges = (
    <>
      <Badge tone="brand">{scopeLabels[params.scope] ?? params.scope}</Badge>
      <Badge tone="default">{sortLabels[params.sortBy] ?? params.sortBy}</Badge>
      <Badge tone={failedSymbols.length ? 'warn' : requestedCount > 0 ? 'good' : 'default'}>
        {requestedCount > 0 ? `覆盖 ${successCount} / ${requestedCount}` : '待刷新'}
      </Badge>
      <Badge tone={authenticated ? 'good' : 'default'}>{authenticated ? '可写' : '只读'}</Badge>
    </>
  )

  const openDrawer = (symbol: string) => {
    if (!symbol) {
      return
    }
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
      <WorkspaceHero
        title="持仓"
        className="watchlist-anchor-hero"
        badges={watchlistHeroBadges}
      />

      <div className="metric-grid metric-grid--five">
        <MetricCard label="观察池股票数" value={summaryQuery.data?.overview.totalCount ?? 0} />
        <MetricCard label="历史精选数" value={summaryQuery.data?.overview.overlayCount ?? 0} tone="good" />
        <MetricCard label="最新推理池" value={summaryQuery.data?.overview.inferenceOverlayCount ?? 0} />
        <MetricCard label="参考市值" value={summaryQuery.data?.overview.marketValue ?? 0} />
        <MetricCard
          label="浮动盈亏"
          value={summaryQuery.data?.overview.unrealizedPnl ?? 0}
          tone={toneFromSignedNumber(summaryQuery.data?.overview.unrealizedPnl)}
        />
      </div>

      <Panel title="筛选" subtitle={`当前 ${summaryQuery.data?.filteredCount ?? 0} 条记录`} tone="warm" className="panel--summary-surface">
        <QueryNotice isLoading={summaryQuery.isLoading} error={summaryQuery.error} loadingText="加载中..." />
        {writeLocked ? <div className="query-notice query-notice--info">当前只读，可查看最近快照和列表；如需刷新行情或生成内容，请先登录。</div> : null}

        <RealtimeStatusBanner
          title="盘中行情刷新状态"
          status={realtimeStatus}
          isRefreshing={refreshRealtimeMutation.isPending}
          error={refreshRealtimeMutation.isError ? refreshRealtimeMutation.error : undefined}
          onRefresh={() => refreshRealtimeMutation.mutate()}
          onRetryFailed={() => refreshRealtimeMutation.mutate()}
          disabled={writeLocked || refreshRealtimeMutation.isPending}
        />

        <ContextStrip items={watchlistContextItems} />

        <PageFilterBar title="切换股票">
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
            <ControlField label="查看持仓">
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

      <Panel title="当前股票" className="panel--summary-surface watchlist-execution-panel">
        <QueryNotice isLoading={detailQuery.isLoading} error={detailQuery.error} />
        {detail ? (
          <SectionBlock
            title="交易席位"
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
              subtitle={String(detail.llm_latest_summary ?? detail.premarket_plan ?? detail.watch_level ?? detail.action_brief ?? '暂无概览')}
              badges={[
                { label: String(detail.entry_group ?? '观察池'), tone: 'brand' },
                detail.source_category ? { label: String(detail.source_category), tone: 'brand' } : null,
                detail.is_watch_only ? { label: '仅观察', tone: 'warn' } : null,
                detail.is_overlay_selected ? { label: '历史精选', tone: 'good' } : null,
                detail.is_inference_overlay_selected ? { label: '最新推理', tone: 'good' } : null,
              ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
              metrics={[
                { label: '强弱分界', value: detail.mark_price ?? '-' },
                { label: '最新价', value: detail.realtime_price ?? detail.mark_price ?? '-', helper: `${selectedRealtimeSource} / ${formatDateTime(selectedRealtimeTime)}` },
                { label: '盘中涨跌', value: formatPercent(detail.realtime_pct_chg), tone: toneFromSignedNumber(detail.realtime_pct_chg) },
                { label: '防守位', value: detail.defensive_price ?? '-', tone: 'warn' },
                { label: '观察位', value: detail.halfway_recovery_price ?? '-' },
                { label: '最新推理排名', value: buildRankLabel(detail.inference_ensemble_rank, null, detail.inference_ensemble_rank_pct) },
              ]}
            />
            <PropertyGrid
              items={[
                { label: '来源分类', value: formatValue(detail.source_category) },
                { label: '来源标签', value: formatValue(detail.source_tags), span: 'double' },
                { label: '来源说明', value: formatValue(detail.source_note), span: 'double' },
                { label: '历史验证排名', value: buildRankLabel(detail.ensemble_rank, detail.universe_size, detail.ensemble_rank_pct) },
                { label: '解套价', value: formatValue(detail.breakeven_price) },
                { label: '价格状态', value: formatValue(detail.mark_status) },
                    { label: '分析状态', value: formatValue(detail.llm_latest_status) },
                { label: '最新研讨概览', value: formatValue(detail.llm_latest_summary), span: 'double' },
              ]}
            />
          </SectionBlock>
        ) : (
          <div className="empty-state">暂无持仓详情</div>
        )}
      </Panel>

      <Panel title="列表" subtitle="总表只负责排优先级和切换当前股票，不承载执行细节。" tone="calm" className="panel--table-surface watchlist-priority-panel">
        <DataTable
          rows={records}
          columns={WATCHLIST_COLUMNS}
          columnLabels={WATCHLIST_COLUMN_LABELS}
          storageKey="watchlist-matrix"
          viewPresets={WATCHLIST_VIEW_PRESETS}
          defaultPresetKey="trading"
          loading={summaryQuery.isLoading}
          loadingText="加载中..."
          emptyText="暂无观察池数据"
          stickyFirstColumn
          getRowId={(row) => String(row.ts_code ?? '')}
          selectedRowId={drawerSymbol ?? selectedSymbol ?? null}
          onRowClick={(row) => openDrawer(String(row.ts_code ?? ''))}
          cellRenderers={watchlistCellRenderers}
        />
      </Panel>

      <div className="mobile-inspection-stack mobile-only">
        <MobileInspectionCard
          title={String(detail?.name ?? '暂无持仓详情')}
          subtitle={String(detail?.premarket_plan ?? detail?.watch_level ?? detail?.action_brief ?? '先从持仓列表选择一只股票。')}
          badges={
            detail ? (
              <div className="badge-row">
                <Badge tone="brand">{String(detail.ts_code ?? '')}</Badge>
                <Badge tone={detail.is_watch_only ? 'warn' : 'good'}>{detail.is_watch_only ? '仅观察' : '持仓中'}</Badge>
              </div>
            ) : null
          }
          body={
            detail ? (
              <PropertyGrid
                items={[
                  { label: '最新价', value: formatValue(detail.realtime_price ?? '-') },
                  { label: '盘中涨跌', value: formatPercent(detail.realtime_pct_chg ?? '-'), tone: toneFromSignedNumber(detail.realtime_pct_chg) },
                  { label: '强弱分界', value: formatValue(detail.mark_price ?? '-') },
                  { label: '防守位', value: formatValue(detail.defensive_price ?? '-') },
                ]}
              />
            ) : null
          }
          actions={
            detail ? (
              <div className="inline-actions inline-actions--compact">
                <button type="button" className="button button--primary" onClick={() => setDrawerSymbol(String(detail.ts_code ?? ''))}>
                  查看详情
                </button>
                <button type="button" className="button button--ghost" onClick={() => openAiReview(String(detail.ts_code ?? ''))}>
                  AI 分析
                </button>
              </div>
            ) : null
          }
        />

      </div>

      <div className="split-layout desktop-only">
        <Panel title="详情" className="panel--summary-surface watchlist-review-panel">
          <QueryNotice isLoading={detailQuery.isLoading} error={detailQuery.error} />
          {detail ? (
            <div className="section-stack">
              <SectionBlock
                title="详情概览"
              >
                <SpotlightCard
                  title={String(detail.name ?? '-')}
                  meta={String(detail.ts_code ?? '')}
                  subtitle={String(detail.llm_latest_summary ?? detail.action_brief ?? detail.watch_level ?? '暂无概览')}
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
                    { label: '解套价', value: detail.breakeven_price ?? '-' },
                  ]}
                />
              </SectionBlock>

              <SectionBlock title="价格与研究备注" collapsible defaultExpanded={false}>
                <PropertyGrid
                  items={[
                    { label: '价格状态', value: formatValue(detail.mark_status) },
                    { label: '来源标签', value: formatValue(detail.source_tags), span: 'double' },
                    { label: '来源说明', value: formatValue(detail.source_note), span: 'double' },
                    { label: '状态说明', value: formatValue(detail.mark_status_note), span: 'double' },
                    { label: '操作建议', value: formatValue(detail.action_brief), span: 'double' },
                    { label: '解套价', value: formatValue(detail.breakeven_price) },
                    { label: '关注备注', value: formatValue(detail.focus_note || '暂无备注') },
                    { label: '排名备注', value: formatValue(detail.ranking_note), span: 'double' },
                    { label: '分析状态', value: formatValue(detail.llm_latest_status) },
                    { label: '分析结论', value: formatValue(detail.llm_latest_summary), span: 'double' },
                  ]}
                />
              </SectionBlock>
            </div>
          ) : (
            <div className="empty-state">暂无持仓详情</div>
          )}
        </Panel>

        <SupportPanel title="盘中" className="watchlist-support-panel">
          <SectionBlock title="盘中状态" collapsible defaultExpanded={false}>
            <div className="metric-grid metric-grid--compact">
              <MetricCard label="实时来源" value={describeRealtimeSource(realtimeStatus.source).label} helper={formatDateTime(realtimeStatus.fetched_at)} />
              <MetricCard label="刷新覆盖" value={requestedCount > 0 ? `${successCount} / ${requestedCount}` : '待刷新'} tone={requestedCount > 0 && successCount === requestedCount ? 'good' : 'warn'} />
              <MetricCard label="盘中涨跌" value={formatPercent(detail?.realtime_pct_chg)} tone={toneFromSignedNumber(detail?.realtime_pct_chg)} />
              <MetricCard label="盘中振幅" value={formatPercent(detail?.realtime_amplitude)} tone={toneFromSignedNumber(detail?.realtime_amplitude)} />
            </div>
            <PropertyGrid
              items={[
                { label: '最近刷新时间', value: formatDateTime(realtimeStatus.fetched_at) },
                { label: '最近轮次', value: formatValue(detailQuery.data?.detail?.llm_latest_round) },
                { label: '失败股票', value: failedSymbols.length ? failedSymbols.join(' / ') : '暂无', span: 'double', tone: failedSymbols.length ? 'warn' : 'good' },
              ]}
            />
            <div className="inline-actions inline-actions--compact">
              <button type="button" className="button button--ghost" onClick={() => detail?.ts_code && openAiReview(String(detail.ts_code))}>
                查看 AI 分析
              </button>
            </div>
          </SectionBlock>
        </SupportPanel>
      </div>

      <div className="split-layout">
        <SupportPanel
          title="计划"
          className="watchlist-support-panel"
          mobileCard={{
            title: '分批观察计划',
            subtitle: detail ? String(detail.name ?? detail.ts_code ?? '当前股票') : '先从持仓列表选择一只股票。',
            actions: (
              <details className="details-block">
                <summary>展开计划表</summary>
                <DataTable
                  rows={detailQuery.data?.reducePlan ?? []}
                  columnLabels={REDUCE_PLAN_COLUMN_LABELS}
                  storageKey="watchlist-reduce-plan-mobile"
                  emptyText="暂无计划表"
                />
              </details>
            ),
          }}
        >
          <DataTable
            rows={detailQuery.data?.reducePlan ?? []}
            columnLabels={REDUCE_PLAN_COLUMN_LABELS}
            storageKey="watchlist-reduce-plan"
            emptyText="暂无计划表"
          />
        </SupportPanel>

        <SupportPanel
          title="历史"
          className="watchlist-support-panel"
          mobileCard={{
            title: '评分历史',
            subtitle: detail ? String(detail.name ?? detail.ts_code ?? '当前股票') : '先从持仓列表选择一只股票。',
            body: (
              <LineChartCard
                data={detailQuery.data?.history ?? []}
                xKey="trade_date"
                lineKeys={['score']}
                title="历史评分曲线"
              />
            ),
          }}
        >
          <LineChartCard
            data={detailQuery.data?.history ?? []}
            xKey="trade_date"
            lineKeys={['score']}
            title="历史评分曲线"
          />
        </SupportPanel>
      </div>

      <SupportPanel title="AI Shortlist">
        <SectionBlock title="最新推理重点股票" collapsible defaultExpanded={false}>
          <MarkdownCard title="交易员可读 Shortlist" content={detailQuery.data?.latestAiShortlist} />
        </SectionBlock>
      </SupportPanel>

      <DetailDrawer
        open={Boolean(drawerRecord)}
        title={drawerRecord ? buildSymbolLabel(drawerRecord) : '持仓详情'}
        subtitle={drawerRecord ? String(drawerRecord.watch_level ?? drawerRecord.action_brief ?? '') : undefined}
        className="watchlist-review-drawer"
        status={
          drawerRecord ? (
            <div className="badge-row">
              <Badge tone="brand">{String(drawerRecord.entry_group ?? '观察池')}</Badge>
              {drawerRecord.source_category ? <Badge tone="brand">{String(drawerRecord.source_category)}</Badge> : null}
              {drawerRecord.is_watch_only ? <Badge tone="warn">仅观察</Badge> : <Badge tone="good">持仓中</Badge>}
            </div>
          ) : null
        }
        meta={
          drawerRecord ? (
            <div className="badge-row">
              <Badge tone="brand">{String(drawerRecord.entry_group ?? '观察池')}</Badge>
              {drawerRecord.source_category ? <Badge tone="brand">{String(drawerRecord.source_category)}</Badge> : null}
              {drawerRecord.is_watch_only ? <Badge tone="warn">仅观察</Badge> : <Badge tone="good">持仓中</Badge>}
            </div>
          ) : null
        }
        onClose={() => setDrawerSymbol(null)}
      >
        {drawerRecord ? (
          <div className="section-stack">
            <QueryNotice isLoading={drawerDetailQuery.isLoading} error={drawerDetailQuery.error} />
            <DetailSummarySection
              sectionTitle="详情概览"
              title={String(drawerRecord.name ?? '-')}
              meta={String(drawerRecord.ts_code ?? '')}
              subtitle={String(drawerRecord.action_brief ?? drawerRecord.watch_level ?? '查看当前持仓详情。')}
              badges={[
                { label: String(drawerRecord.entry_group ?? '观察池'), tone: 'brand' },
                drawerRecord.source_category ? { label: String(drawerRecord.source_category), tone: 'brand' } : null,
                drawerRecord.is_watch_only ? { label: '仅观察', tone: 'warn' } : { label: '持仓中', tone: 'good' },
              ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
              metrics={[
                { label: '参考价', value: drawerRecord.mark_price ?? '-' },
                { label: '最新价', value: drawerRecord.realtime_price ?? '-', helper: describeRealtimeSource(drawerRecord.realtime_quote_source).label },
                { label: '浮动收益率', value: formatPercent(drawerRecord.unrealized_pnl_pct), tone: toneFromSignedNumber(drawerRecord.unrealized_pnl_pct) },
              ]}
              properties={[
                { label: '概览', value: formatValue(drawerRecord.action_brief), span: 'double', tone: 'good' },
                { label: '来源标签', value: formatValue(drawerRecord.source_tags), span: 'double' },
                { label: '来源说明', value: formatValue(drawerRecord.source_note), span: 'double' },
                { label: '价格状态', value: formatValue(drawerRecord.mark_status) },
                { label: '解套价', value: formatValue(drawerRecord.breakeven_price) },
                { label: '行情来源', value: describeRealtimeSource(drawerRecord.realtime_quote_source).label },
                { label: '实时更新时间', value: formatDateTime(drawerRecord.realtime_time) },
                { label: '分析状态', value: formatValue(drawerRecord.llm_latest_status) },
                { label: '分析结论', value: formatValue(drawerRecord.llm_latest_summary), span: 'double' },
              ]}
            />

            <DrawerQuickActions
              title="详情操作"
              meta={String(drawerRecord.ts_code ?? '')}
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
                {
                  key: 'set-current-focus',
                  label: '设为当前股票',
                  onClick: () => {
                    updateParams({ symbol: String(drawerRecord.ts_code ?? '') })
                    setDrawerSymbol(null)
                  },
                  tone: 'ghost',
                },
                { key: 'copy-symbol', label: '复制股票代码', onClick: () => copySymbol(String(drawerRecord.ts_code ?? '')), tone: 'ghost' },
                { key: 'copy-view', label: '复制当前视图', onClick: copyCurrentViewLink, tone: 'ghost' },
                { key: 'open-ai-review', label: '查看 AI 分析', onClick: () => openAiReview(String(drawerRecord.ts_code ?? '')), tone: 'ghost' },
                { key: 'open-candidates', label: '查看候选', onClick: () => openCandidatesPage(String(drawerRecord.ts_code ?? '')), tone: 'ghost' },
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
