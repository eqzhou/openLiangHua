import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useLocation, useNavigate, useParams, NavLink } from 'react-router-dom'

import { apiGet, apiPost } from '../api/client'
import { DataTable } from '../components/DataTable'
import { MarkdownCard } from '../components/MarkdownCard'
import { QueryNotice } from '../components/QueryNotice'
import { useToast } from '../components/ToastProvider'
import { realtimeRefreshClient, watchlistDetailClient, watchlistPageClient, watchlistSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatPercent } from '../lib/format'
import { describeRealtimeSource } from '../lib/realtime'
import { buildAiReviewPath, buildWatchlistPath } from '../lib/shareLinks'
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
  const records = summaryQuery.data?.records ?? []
  const recordSymbols = records.map((row) => String(row.ts_code ?? '')).filter(Boolean)
  const currentIndex = recordSymbols.findIndex((item: string) => item === symbol)
  const previousSymbol = currentIndex > 0 ? recordSymbols[currentIndex - 1] : null
  const nextSymbol = currentIndex >= 0 && currentIndex < recordSymbols.length - 1 ? recordSymbols[currentIndex + 1] : null
  const currentPositionLabel = currentIndex >= 0 ? `${currentIndex + 1} / ${recordSymbols.length}` : '-'

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp bg-erp-bg">
      {/* Top Header Row for Detail */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <NavLink to={`/watchlist${location.search}`} className="toolbar-btn shrink-0">
          <i className="ph ph-arrow-left text-erp-primary"></i> 返回列表
        </NavLink>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-target text-erp-danger"></i>
          {String(detail.name || symbol)} ({symbol})
        </span>
        <span className={`px-1.5 py-0.5 text-erp-sm border rounded shrink-0 ${detail.is_watch_only ? 'bg-yellow-50 text-yellow-700 border-yellow-200' : 'bg-green-50 text-green-700 border-green-200'}`}>
          {detail.is_watch_only ? '仅观察' : '持仓中'}
        </span>
        <span className="px-1.5 py-0.5 bg-gray-100 text-gray-600 text-erp-sm border border-gray-200 rounded shrink-0">
          {String(detail.entry_group ?? '观察池')}
        </span>
        <span className="px-1.5 py-0.5 bg-gray-100 text-gray-600 text-erp-sm border border-gray-200 rounded shrink-0">
          序号 {currentPositionLabel}
        </span>

        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>

        {/* Action Buttons in Toolbar */}
        <button 
          className={`toolbar-btn shrink-0 ${refreshRealtimeMutation.isPending ? 'disabled' : ''}`}
          onClick={() => refreshRealtimeMutation.mutate()}
          disabled={!authenticated || refreshRealtimeMutation.isPending}
        >
          <i className={`ph ph-arrows-clockwise ${refreshRealtimeMutation.isPending ? 'animate-spin' : 'text-erp-success'}`}></i> 
          刷新盘中行情
        </button>

        <button 
          className={`toolbar-btn shrink-0 ${generateWatchPlanMutation.isPending ? 'disabled' : ''}`}
          onClick={() => generateWatchPlanMutation.mutate()}
          disabled={!authenticated || generateWatchPlanMutation.isPending}
        >
          <i className="ph ph-file-text"></i> 生成盯盘清单
        </button>
        <button 
          className={`toolbar-btn shrink-0 ${generateActionMemoMutation.isPending ? 'disabled' : ''}`}
          onClick={() => generateActionMemoMutation.mutate()}
          disabled={!authenticated || generateActionMemoMutation.isPending}
        >
          <i className="ph ph-note"></i> 生成操作备忘
        </button>
        <button className="toolbar-btn shrink-0" onClick={() => navigate(buildAiReviewPath(symbol))}>
          <i className="ph ph-brain text-erp-primary"></i> AI 分析
        </button>

        <div className="ml-auto flex items-center gap-2 text-erp-sm shrink-0">           <a href={`https://xueqiu.com/S/${symbol.replace('.', '')}`} target="_blank" rel="noreferrer" className="toolbar-btn text-erp-primary hover:underline">
             雪球
           </a>
           <a href={`https://quote.eastmoney.com/${symbol.replace('.', '').toLowerCase()}.html`} target="_blank" rel="noreferrer" className="toolbar-btn text-erp-primary hover:underline">
             东方财富
           </a>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto bg-white flex flex-col p-6 gap-8 text-erp">
        <QueryNotice isLoading={summaryQuery.isLoading || detailQuery.isLoading} error={summaryQuery.error ?? detailQuery.error} />
        
        {/* Core Detail Grid - Large Flat Row */}
        <div className="flex items-center gap-12 shrink-0 border-b erp-border pb-6">
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">参考价 / Mark Price</span>
            <span className="text-3xl font-mono font-bold leading-none text-gray-700">{String(detail.mark_price ?? '-')}</span>
          </div>
          <div className="w-px h-10 bg-gray-200"></div>
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">最新价 / Last Price</span>
            <span className="text-3xl font-mono font-bold leading-none">{String(detail.realtime_price ?? '-')}</span>
            <span className="text-[10px] text-gray-400 mt-1 uppercase">
              {describeRealtimeSource(detail.realtime_quote_source ?? realtimeStatus.source).label}
            </span>
          </div>
          <div className="w-px h-10 bg-gray-200"></div>
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">盘中涨跌</span>
            <span className={`text-3xl font-mono font-bold leading-none ${Number(detail.realtime_pct_chg) > 0 ? 'text-erp-danger' : Number(detail.realtime_pct_chg) < 0 ? 'text-erp-success' : ''}`}>
              {formatPercent(detail.realtime_pct_chg)}
            </span>
          </div>
          <div className="w-px h-10 bg-gray-200"></div>
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">解套价</span>
            <span className="text-3xl font-mono font-bold leading-none text-gray-400">{String(detail.breakeven_price ?? '-')}</span>
          </div>
          
          <div className="flex-1 max-w-xl border-l erp-border pl-8 ml-4">
            <div className="text-erp-primary font-bold text-xs mb-1 uppercase flex items-center gap-1">
              <i className="ph-fill ph-chat-centered-text"></i> AI 交易员策略简述:
            </div>
            <div className="text-gray-600 text-sm leading-relaxed italic">
              "{String(detail.llm_latest_summary ?? detail.premarket_plan ?? detail.action_brief ?? detail.watch_level ?? '暂无概览')}"
            </div>
          </div>
        </div>

        {/* Action Plans & Memorandums */}
        <section className="grid grid-cols-2 gap-12 pt-4">
           <div className="flex flex-col gap-4">
              <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                <i className="ph ph-notebook"></i> 交易计划与备忘
              </h4>
              <div className="bg-yellow-50/40 rounded-lg p-6 erp-border border-dashed flex flex-col gap-4 min-h-[250px]">
                 <div className="text-sm font-bold text-yellow-800">最新盯盘提醒:</div>
                 <div className="text-sm leading-relaxed text-gray-700">
                    <MarkdownCard title="" content={String(detailQuery.data?.watchPlan?.content || detailQuery.data?.actionMemo?.content || '*未生成盯盘备忘*')} />
                 </div>
              </div>
           </div>

           <div className="flex flex-col gap-4">
              <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                <i className="ph ph-list-numbers"></i> 减仓阶梯参考
              </h4>
              <div className="erp-border rounded-lg overflow-hidden h-[250px]">
                 <DataTable rows={detailQuery.data?.reducePlan ?? []} columnLabels={REDUCE_PLAN_COLUMN_LABELS} storageKey="watchlist-reduce-plan" emptyText="暂无计划表" />
              </div>
           </div>
        </section>

        {/* Discussion History */}
        <section className="border-t erp-border pt-8">
           <h4 className="text-gray-400 font-bold text-xs uppercase tracking-widest mb-6 flex items-center gap-2">
             <i className="ph ph-chat-circle-text"></i> 策略研讨历史纪录 (Discussion History)
           </h4>
           <div className="erp-border rounded-lg overflow-hidden min-h-[300px]">
              <DataTable rows={detailQuery.data?.discussionRows ?? []} storageKey="watchlist-discussion" emptyText="暂无研讨记录" />
           </div>
        </section>
      </div>

      {/* Pager Status Bar */}
      <div className="h-8 erp-border-t bg-gray-50 flex items-center px-2 justify-between text-gray-500 text-erp-sm shrink-0">
        <div className="flex items-center gap-4">
          <span>历史验证排名: {buildRankLabel(detail.ensemble_rank, detail.universe_size, detail.ensemble_rank_pct)}</span>
        </div>
        <div className="flex items-center gap-2">
          <button 
            className="toolbar-btn" 
            disabled={!previousSymbol} 
            onClick={previousSymbol ? () => navigate({ pathname: buildWatchlistPath(previousSymbol), search: location.search }) : undefined}
          >
            <i className="ph ph-caret-left"></i> 上一只 ({previousSymbol || '-'})
          </button>
          <span>{currentPositionLabel}</span>
          <button 
            className="toolbar-btn"
            disabled={!nextSymbol}
            onClick={nextSymbol ? () => navigate({ pathname: buildWatchlistPath(nextSymbol), search: location.search }) : undefined}
          >
            下一只 ({nextSymbol || '-'}) <i className="ph ph-caret-right"></i>
          </button>
        </div>
      </div>

    </div>
  )
}
