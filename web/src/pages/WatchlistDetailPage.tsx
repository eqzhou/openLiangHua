import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useLocation, useNavigate, useParams } from 'react-router-dom'

import { apiGet, apiPost } from '../api/client'
import { Badge } from '../components/Badge'
import { DataTable } from '../components/DataTable'
import { DetailPageNav } from '../components/DetailPageNav'
import { DrawerQuickActions } from '../components/DrawerQuickActions'
import { LineChartCard } from '../components/LineChartCard'
import { MarkdownCard } from '../components/MarkdownCard'
import { Panel } from '../components/Panel'
import { QueryNotice } from '../components/QueryNotice'
import { RealtimeStatusBanner } from '../components/RealtimeStatusBanner'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { useToast } from '../components/ToastProvider'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { realtimeRefreshClient, watchlistDetailClient, watchlistPageClient, watchlistSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDateTime, formatPercent, formatValue, getFieldLabel } from '../lib/format'
import { describeRealtimeSource, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import { buildAiReviewPath, buildCandidatesPath, buildWatchlistPath } from '../lib/shareLinks'
import type { RealtimeRefreshPayload, WatchlistDetailPayload, WatchlistSummaryPayload } from '../types/api'

interface WatchlistDetailPageProps {
  authenticated?: boolean
}

const REDUCE_PLAN_COLUMN_LABELS = {
  plan_stage: '阶段',
  target_price: '目标价格',
  reduce_ratio: '减仓比例',
  target_shares: '目标股数',
  distance_from_mark_pct: '距离当前价',
  estimated_realized_pnl: '预计实现盈亏',
  plan_note: '说明',
}

function toneFromSignedNumber(value: unknown): 'default' | 'good' | 'warn' {
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
  return formatValue(value)
}

export function WatchlistDetailPage({ authenticated = false }: WatchlistDetailPageProps) {
  const location = useLocation()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { pushToast } = useToast()
  const { symbol = '' } = useParams<{ symbol: string }>()
  const { params } = usePageSearchState(watchlistPageClient)

  const summaryQuery = useQuery({
    queryKey: watchlistSummaryClient.queryKey(params),
    queryFn: () => apiGet<WatchlistSummaryPayload>(watchlistSummaryClient.path(params)),
  })

  const detailQuery = useQuery({
    queryKey: watchlistDetailClient.queryKey(params, symbol),
    queryFn: () => apiGet<WatchlistDetailPayload>(watchlistDetailClient.path(params, symbol)),
    enabled: Boolean(symbol),
  })

  const refreshRealtimeMutation = useMutation({
    mutationFn: () => apiPost<RealtimeRefreshPayload>(realtimeRefreshClient.path()),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['watchlist-summary'] })
      void queryClient.invalidateQueries({ queryKey: ['watchlist-detail'] })
      pushToast({ tone: 'success', title: '最新行情已更新', description: '详情页已同步最新盘中快照。' })
    },
    onError: (error) => {
      pushToast({ tone: 'error', title: '拉取最新行情失败', description: error instanceof Error ? error.message : '请稍后再试。' })
    },
  })

  const generateWatchPlanMutation = useMutation({
    mutationFn: () => apiPost(watchlistPageClient.watchPlanActionPath),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['watchlist-detail'] })
      pushToast({ tone: 'success', title: '盯盘清单已生成', description: '详情页已刷新。' })
    },
  })

  const generateActionMemoMutation = useMutation({
    mutationFn: () => apiPost(watchlistPageClient.actionMemoActionPath),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['watchlist-detail'] })
      pushToast({ tone: 'success', title: '操作备忘已生成', description: '详情页已刷新。' })
    },
  })

  const detail = detailQuery.data?.detail ?? {}
  const realtimeStatus = summaryQuery.data?.realtimeStatus ?? {}
  const failedSymbols = normalizeRealtimeFailedSymbols(realtimeStatus.failed_symbols)
  const detailFieldRows = Object.entries(detail).map(([field, value]) => ({ field: getFieldLabel(field), value: formatDetailValue(field, value) }))
  const records = summaryQuery.data?.records ?? []
  const recordSymbols = records.map((row) => String(row.ts_code ?? '')).filter(Boolean)
  const currentIndex = recordSymbols.findIndex((item: string) => item === symbol)
  const previousSymbol = currentIndex > 0 ? recordSymbols[currentIndex - 1] : null
  const nextSymbol = currentIndex >= 0 && currentIndex < recordSymbols.length - 1 ? recordSymbols[currentIndex + 1] : null
  const currentPositionLabel = currentIndex >= 0 ? `${currentIndex + 1} / ${recordSymbols.length}` : '-'

  return (
    <div className="page-stack">
      <WorkspaceHero
        title="持仓详情"
        eyebrow="持仓 / 详情"
        badges={
          <>
            <Badge tone="brand">{String(detail.ts_code ?? symbol)}</Badge>
            <Badge tone={detail.is_watch_only ? 'warn' : 'good'}>{detail.is_watch_only ? '仅观察' : '持仓中'}</Badge>
            <Badge>{`序号 ${currentPositionLabel}`}</Badge>
          </>
        }
      />

      <Panel title="详情" tone="warm" className="panel--summary-surface">
        <QueryNotice isLoading={summaryQuery.isLoading || detailQuery.isLoading} error={summaryQuery.error ?? detailQuery.error} />
        <RealtimeStatusBanner
          title="盘中行情刷新状态"
          status={realtimeStatus}
          isRefreshing={refreshRealtimeMutation.isPending}
          error={refreshRealtimeMutation.isError ? refreshRealtimeMutation.error : undefined}
          onRefresh={() => refreshRealtimeMutation.mutate()}
          onRetryFailed={() => refreshRealtimeMutation.mutate()}
          disabled={!authenticated || refreshRealtimeMutation.isPending}
        />
        {Object.keys(detail).length ? (
          <SectionBlock title="交易席位" tone="emphasis">
            <SpotlightCard
              title={String(detail.name ?? '-')}
              meta={String(detail.ts_code ?? '')}
              subtitle={String(detail.llm_latest_summary ?? detail.premarket_plan ?? detail.action_brief ?? detail.watch_level ?? '暂无概览')}
              badges={[
                { label: String(detail.entry_group ?? '观察池'), tone: 'brand' },
                detail.source_category ? { label: String(detail.source_category), tone: 'brand' } : null,
                detail.is_overlay_selected ? { label: '历史精选', tone: 'good' } : null,
                detail.is_inference_overlay_selected ? { label: '最新推理', tone: 'good' } : null,
              ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
              metrics={[
                { label: '参考价', value: detail.mark_price ?? '-' },
                { label: '最新价', value: detail.realtime_price ?? '-', helper: `${describeRealtimeSource(detail.realtime_quote_source ?? realtimeStatus.source).label} / ${formatDateTime(detail.realtime_time ?? realtimeStatus.fetched_at)}` },
                { label: '盘中涨跌', value: formatPercent(detail.realtime_pct_chg), tone: toneFromSignedNumber(detail.realtime_pct_chg) },
                { label: '历史验证排名', value: buildRankLabel(detail.ensemble_rank, detail.universe_size, detail.ensemble_rank_pct) },
                { label: '最新推理排名', value: buildRankLabel(detail.inference_ensemble_rank, null, detail.inference_ensemble_rank_pct) },
                { label: '解套价', value: detail.breakeven_price ?? '-' },
              ]}
            />
          </SectionBlock>
        ) : null}
      </Panel>

      <DetailPageNav
        onBack={() => navigate({ pathname: '/watchlist', search: location.search })}
        prevLabel={previousSymbol}
        onPrev={previousSymbol ? () => navigate({ pathname: buildWatchlistPath(previousSymbol), search: location.search }) : null}
        nextLabel={nextSymbol}
        onNext={nextSymbol ? () => navigate({ pathname: buildWatchlistPath(nextSymbol), search: location.search }) : null}
      />

      <DrawerQuickActions
        title="详情操作"
        meta={String(detail.ts_code ?? symbol)}
        primaryActions={[
          { key: 'generate-watch-plan', label: generateWatchPlanMutation.isPending ? '生成中...' : '生成盯盘清单', onClick: () => generateWatchPlanMutation.mutate(), disabled: !authenticated || generateWatchPlanMutation.isPending },
          { key: 'generate-action-memo', label: generateActionMemoMutation.isPending ? '生成中...' : '生成操作备忘', onClick: () => generateActionMemoMutation.mutate(), disabled: !authenticated || generateActionMemoMutation.isPending },
        ]}
        secondaryActions={[
          { key: 'open-ai-review', label: '查看 AI 分析', onClick: () => navigate(buildAiReviewPath(symbol)) , tone: 'ghost' },
          { key: 'open-candidates', label: '查看候选', onClick: () => navigate(buildCandidatesPath(symbol)), tone: 'ghost' },
        ]}
      />

      <div className="split-layout">
        <Panel title="计划" tone="calm" className="panel--summary-surface">
          <DataTable rows={detailQuery.data?.reducePlan ?? []} columnLabels={REDUCE_PLAN_COLUMN_LABELS} storageKey="watchlist-reduce-plan" emptyText="暂无计划表" />
        </Panel>
        <Panel title="历史" tone="calm" className="panel--summary-surface">
          <LineChartCard data={detailQuery.data?.history ?? []} xKey="trade_date" lineKeys={['score']} title="历史评分曲线" />
        </Panel>
      </div>

      <div className="split-layout">
        <Panel title="AI 研讨" tone="calm" className="panel--summary-surface">
          <DataTable rows={detailQuery.data?.discussionRows ?? []} storageKey="watchlist-discussion" emptyText="暂无 AI 研讨记录" />
        </Panel>
        <Panel title="价格与研究备注" tone="calm" className="panel--summary-surface">
          <DataTable rows={detailFieldRows} columns={['field', 'value']} storageKey="watchlist-detail-fields" emptyText="暂无详情字段" enableColumnManager={false} />
        </Panel>
      </div>

      <div className="split-layout">
        <Panel title="AI Shortlist" tone="calm" className="panel--summary-surface">
          <MarkdownCard title="交易员可读 Shortlist" content={detailQuery.data?.latestAiShortlist} />
        </Panel>
        <Panel title="盯盘与备忘" tone="calm" className="panel--summary-surface">
          <SectionBlock title="最新盯盘清单" collapsible defaultExpanded={false}>
            <MarkdownCard title="盯盘清单" content={String(detailQuery.data?.watchPlan?.content ?? '')} />
          </SectionBlock>
          <SectionBlock title="最新操作备忘" collapsible defaultExpanded={false}>
            <MarkdownCard title="操作备忘" content={String(detailQuery.data?.actionMemo?.content ?? '')} />
          </SectionBlock>
          {failedSymbols.length ? <div className="query-notice query-notice--warn">{`快照失败股票：${failedSymbols.join(' / ')}`}</div> : null}
        </Panel>
      </div>
    </div>
  )
}
