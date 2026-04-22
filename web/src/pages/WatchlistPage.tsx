import { useMemo } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useLocation, useNavigate } from 'react-router-dom'

import { apiGet, apiPost } from '../api/client'
import { Badge } from '../components/Badge'
import { ControlField } from '../components/ControlField'
import { ControlGrid } from '../components/ControlGrid'
import { ContextStrip } from '../components/ContextStrip'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { MetricCard } from '../components/MetricCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { QueryNotice } from '../components/QueryNotice'
import { RealtimeStatusBanner } from '../components/RealtimeStatusBanner'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { useToast } from '../components/ToastProvider'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { realtimeRefreshClient, watchlistPageClient, watchlistSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDateTime } from '../lib/format'
import { normalizeRealtimeFailedSymbols } from '../lib/realtime'
import { buildWatchlistPath, copyShareablePageLink } from '../lib/shareLinks'
import type { BootstrapPayload, JsonRecord, RealtimeRefreshPayload, WatchlistSummaryPayload } from '../types/api'

interface WatchlistPageProps {
  bootstrap?: BootstrapPayload
  authenticated?: boolean
}

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

function toneFromSignedNumber(value: unknown): 'default' | 'good' | 'warn' {
  if (typeof value !== 'number' || Number.isNaN(value) || value === 0) {
    return 'default'
  }
  return value > 0 ? 'good' : 'warn'
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim()
  }
  return '操作失败，请稍后再试。'
}

export function WatchlistPage({ bootstrap, authenticated = false }: WatchlistPageProps) {
  const queryClient = useQueryClient()
  const location = useLocation()
  const navigate = useNavigate()
  const { pushToast } = useToast()
  const { params, updateParams } = usePageSearchState(watchlistPageClient)

  const summaryQuery = useQuery({
    queryKey: watchlistSummaryClient.queryKey(params),
    queryFn: () => apiGet<WatchlistSummaryPayload>(watchlistSummaryClient.path(params)),
  })

  const refreshRealtimeMutation = useMutation({
    mutationFn: () => apiPost<RealtimeRefreshPayload>(realtimeRefreshClient.path()),
    onSuccess: (payload) => {
      void queryClient.invalidateQueries({ queryKey: ['watchlist-summary'] })
      const realtime = payload.realtimeStatus ?? {}
      const successCount = Number(realtime.success_symbol_count ?? 0)
      const requestedCount = Number(realtime.requested_symbol_count ?? 0)
      pushToast({
        tone: successCount > 0 ? 'success' : 'error',
        title: '最新行情已更新',
        description: `${String(realtime.snapshot_label_display ?? '最新行情')}，覆盖 ${successCount} / ${requestedCount}。`,
      })
    },
    onError: (error) => {
      pushToast({ tone: 'error', title: '拉取最新行情失败', description: toErrorMessage(error) })
    },
  })

  const records = useMemo(() => summaryQuery.data?.records ?? [], [summaryQuery.data?.records])
  const scopeLabels = bootstrap?.watchScopes ? { ...bootstrap.watchScopes, ...DEFAULT_SCOPE_LABELS } : DEFAULT_SCOPE_LABELS
  const sortLabels = bootstrap?.watchSorts ? { ...bootstrap.watchSorts, ...DEFAULT_SORT_LABELS } : DEFAULT_SORT_LABELS
  const realtimeStatus = summaryQuery.data?.realtimeStatus ?? {}
  const requestedCount = Number(realtimeStatus.requested_symbol_count ?? 0)
  const successCount = Number(realtimeStatus.success_symbol_count ?? 0)
  const failedSymbols = useMemo(() => normalizeRealtimeFailedSymbols(realtimeStatus.failed_symbols), [realtimeStatus.failed_symbols])
  const writeLocked = !authenticated

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
      helper: `第 ${summaryQuery.data?.page ?? params.page} / ${summaryQuery.data?.totalPages ?? 1} 页`,
    },
    {
      label: '行情模式',
      value: String(realtimeStatus.snapshot_label_display ?? '暂无快照'),
      helper: formatDateTime(realtimeStatus.fetched_at),
    },
    {
      label: '快照覆盖',
      value: requestedCount > 0 ? `${successCount} / ${requestedCount}` : '待刷新',
      helper: failedSymbols.length ? failedSymbols.join(' / ') : '暂无失败股票',
      tone: failedSymbols.length ? ('warn' as const) : requestedCount > 0 ? ('good' as const) : ('default' as const),
    },
  ]

  const watchlistHeroBadges = (
    <>
      <Badge tone="brand">{scopeLabels[params.scope] ?? params.scope}</Badge>
      <Badge tone="default">{sortLabels[params.sortBy] ?? params.sortBy}</Badge>
      <Badge tone="brand">列表优先</Badge>
      <Badge tone={authenticated ? 'good' : 'default'}>{authenticated ? '可写' : '只读'}</Badge>
    </>
  )

  const openDetail = (symbol: string) => {
    if (!symbol) {
      return
    }
    navigate({ pathname: buildWatchlistPath(symbol), search: location.search })
  }

  return (
    <div className="page-stack">
      <WorkspaceHero
        title="持仓"
        className="watchlist-anchor-hero"
        description="首屏先查列表、筛选和排序。点击股票后进入详情页，不会一上来把单票明细全拉下来。"
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

      <Panel title="筛选" subtitle={`当前 ${summaryQuery.data?.filteredCount ?? 0} 条记录，点击列表行后进入详情页。`} tone="warm" className="panel--summary-surface watchlist-filter-panel">
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

        <SectionBlock title="列表模式" tone="emphasis">
          <SpotlightCard
            title="ERP 风格"
            meta="列表页只负责浏览"
            subtitle="单票 detail/history/reduce plan/AI discussion 都放到详情页。"
            metrics={[
              { label: '筛选结果', value: summaryQuery.data?.filteredCount ?? 0 },
              { label: '当前页', value: summaryQuery.data?.page ?? params.page },
              { label: '总页数', value: summaryQuery.data?.totalPages ?? 1 },
              { label: '每页数量', value: summaryQuery.data?.pageSize ?? 30 },
            ]}
          />
        </SectionBlock>

        <ContextStrip items={watchlistContextItems} />

        <PageFilterBar title="筛选列表">
          <ControlGrid variant="triple">
            <ControlField label="快速搜索">
              <input value={params.keyword} onChange={(event) => updateParams({ keyword: event.target.value, page: 1 })} placeholder="输入代码或名称" />
            </ControlField>
            <ControlField label="查看范围">
              <select value={params.scope} onChange={(event) => updateParams({ scope: event.target.value, page: 1 })}>
                {Object.entries(scopeLabels).map(([key, label]) => (
                  <option key={key} value={key}>
                    {label}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="排序方式">
              <select value={params.sortBy} onChange={(event) => updateParams({ sortBy: event.target.value, page: 1 })}>
                {Object.entries(sortLabels).map(([key, label]) => (
                  <option key={key} value={key}>
                    {label}
                  </option>
                ))}
              </select>
            </ControlField>
          </ControlGrid>
          <div className="inline-actions inline-actions--compact">
            <button type="button" className="button button--ghost" disabled={(summaryQuery.data?.page ?? params.page) <= 1} onClick={() => updateParams({ page: Math.max(1, (summaryQuery.data?.page ?? params.page) - 1) })}>
              上一页
            </button>
            <button
              type="button"
              className="button button--ghost"
              disabled={(summaryQuery.data?.page ?? params.page) >= (summaryQuery.data?.totalPages ?? 1)}
              onClick={() => updateParams({ page: Math.min(summaryQuery.data?.totalPages ?? 1, (summaryQuery.data?.page ?? params.page) + 1) })}
            >
              下一页
            </button>
            <button type="button" className="button button--ghost" onClick={() => copyShareablePageLink(location.pathname, location.search)}>
              复制当前视图
            </button>
          </div>
        </PageFilterBar>
      </Panel>

      <Panel title="列表" subtitle="总表负责排优先级和进入详情页。" tone="calm" className="panel--table-surface watchlist-priority-panel">
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
          onRowClick={(row) => openDetail(String(row.ts_code ?? ''))}
          rowTitle="点击进入详情"
          cellRenderers={watchlistCellRenderers}
        />
      </Panel>
    </div>
  )
}
