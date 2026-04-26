import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate, NavLink } from 'react-router-dom'

import { apiGet } from '../api/client'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { QueryNotice } from '../components/QueryNotice'
import { homeAiReviewClient, homeCandidatesClient, homeSummaryClient, homeWatchlistClient } from '../facades/dashboardPageClient'
import { formatPercent, formatValue } from '../lib/format'
import { HOME_REFETCH_INTERVAL_MS } from '../lib/polling'
import { buildCandidatesPath, buildWatchlistPath } from '../lib/shareLinks'
import { describeRealtimeSource, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import type { ActionResult, BootstrapPayload, HomeAiReviewPayload, HomeCandidatesPayload, HomeSummaryPayload, HomeWatchlistPayload, JsonRecord } from '../types/api'

interface HomePageProps {
  bootstrap?: BootstrapPayload
  latestAction: ActionResult | null
  authenticated: boolean
  currentUserLabel?: string | null
  actionPendingName: string | null
  sharingCurrentView: boolean
  onRunAction: (actionName: string) => void
  onShareCurrentView: () => void
}

const WATCHLIST_COLUMNS = ['name', 'source_tags', 'realtime_price', 'realtime_pct_chg', 'inference_ensemble_rank', 'premarket_plan']
const WATCHLIST_COLUMN_LABELS = {
  name: '股票',
  source_tags: '来源标签',
  realtime_price: '最新价',
  realtime_pct_chg: '盘中涨跌',
  inference_ensemble_rank: '最新推理排名',
  premarket_plan: '执行建议',
}

const INFERENCE_COLUMNS = ['name', 'industry_display', 'final_score', 'confidence_level', 'action_hint']
const INFERENCE_COLUMN_LABELS = {
  name: '推理股票',
  industry_display: '行业',
  final_score: 'AI总分',
  confidence_level: '置信度',
  action_hint: '操作建议',
}

function buildQuickActions(bootstrap?: BootstrapPayload) {
  const preferredOrder = ['latest_inference', 'overlay', 'watch_plan', 'action_memo']
  const actionLookup = new Map((bootstrap?.actions ?? []).map((item) => [item.actionName, item]))
  return preferredOrder.map((key) => actionLookup.get(key)).filter(Boolean) as NonNullable<BootstrapPayload['actions']>[number][]
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

export function HomePage({
  bootstrap,
  latestAction,
  authenticated,
  actionPendingName,
  onRunAction,
}: HomePageProps) {
  const navigate = useNavigate()
  const homeSummaryQuery = useQuery({
    queryKey: homeSummaryClient.queryKey(),
    queryFn: () => apiGet<HomeSummaryPayload>(homeSummaryClient.path()),
    refetchInterval: HOME_REFETCH_INTERVAL_MS,
  })
  const homeWatchlistQuery = useQuery({
    queryKey: homeWatchlistClient.queryKey(),
    queryFn: () => apiGet<HomeWatchlistPayload>(homeWatchlistClient.path()),
    refetchInterval: HOME_REFETCH_INTERVAL_MS,
  })
  const homeCandidatesQuery = useQuery({
    queryKey: homeCandidatesClient.queryKey(),
    queryFn: () => apiGet<HomeCandidatesPayload>(homeCandidatesClient.path()),
    refetchInterval: HOME_REFETCH_INTERVAL_MS,
  })
  const homeAiReviewQuery = useQuery({
    queryKey: homeAiReviewClient.queryKey(),
    queryFn: () => apiGet<HomeAiReviewPayload>(homeAiReviewClient.path()),
    refetchInterval: HOME_REFETCH_INTERVAL_MS,
  })

  const service = homeSummaryQuery.data?.service ?? {}
  const realtimeSnapshot = (service.realtime_snapshot as JsonRecord | undefined) ?? {}
  const overview = homeSummaryQuery.data?.overview
  const watchlist = homeWatchlistQuery.data
  const aiReview = homeAiReviewQuery.data
  const alerts = homeSummaryQuery.data?.alerts ?? []
  const focusWatchRecord = watchlist?.focusRecord ?? {}
  const focusCandidateRecord = aiReview?.focusRecord ?? homeCandidatesQuery.data?.focusRecord ?? {}
  const quickActions = useMemo(() => buildQuickActions(bootstrap), [bootstrap])
  const failedSymbols = normalizeRealtimeFailedSymbols(realtimeSnapshot.failed_symbols)
  const realtimeSource = describeRealtimeSource(realtimeSnapshot.source)
  const realtimeCoverage = (realtimeSnapshot.success_symbol_count !== undefined && realtimeSnapshot.requested_symbol_count !== undefined)
    ? `${realtimeSnapshot.success_symbol_count} / ${realtimeSnapshot.requested_symbol_count}`
    : '0/0'
  const serviceRunning = String(service.effective_state ?? '') === 'running'

  const watchlistCellRenderers = useMemo(
    () => ({
      name: (row: JsonRecord) => (
        <EntityCell
          title={String(row.name ?? '-')}
          subtitle={String(row.ts_code ?? '')}
          meta={String(row.industry ?? row.source_category ?? '')}
          badges={[
            row.is_overlay_selected ? { label: '精选', tone: 'good' as const } : null,
            row.is_inference_overlay_selected ? { label: '最新', tone: 'brand' as const } : null,
          ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
        />
      ),
    }),
    [],
  )

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp bg-erp-bg">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap text-erp">
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-house text-erp-primary"></i> 
          量化值班首页
        </span>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        {/* Quick Action Bar */}
        <div className="flex items-center gap-2 shrink-0">
          {quickActions.map((action) => (
            <button
              key={action.actionName}
              className={`toolbar-btn ${actionPendingName === action.actionName ? 'disabled' : ''}`}
              disabled={!authenticated || Boolean(actionPendingName)}
              onClick={() => onRunAction(action.actionName)}
            >
              {actionPendingName === action.actionName ? (
                <i className="ph ph-spinner animate-spin"></i>
              ) : (
                <i className="ph ph-play-circle text-erp-success"></i>
              )}
              {action.label}
            </button>
          ))}
          <div className="w-px h-5 bg-gray-300 mx-1"></div>
          <button className="toolbar-btn" onClick={() => navigate('/workspace')}>
            <i className="ph ph-monitor-play"></i> 进入工作台
          </button>
        </div>

        <div className="ml-auto flex items-center gap-4 text-erp-sm shrink-0">
          <div className="flex items-center gap-1">
             <span className="text-gray-500">行情覆盖:</span>
             <span className={`font-bold font-mono ${failedSymbols.length ? 'text-erp-warning' : 'text-erp-success'}`}>{realtimeCoverage}</span>
          </div>
          <div className="flex items-center gap-1">
             <span className="text-gray-500">页面服务:</span>
             <span className={`font-bold uppercase ${serviceRunning ? 'text-erp-success' : 'text-erp-danger'}`}>{String(service.status_label_display || 'OFFLINE')}</span>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto bg-white flex flex-col p-8 gap-12 text-erp">
        <QueryNotice
          isLoading={homeSummaryQuery.isLoading}
          error={homeSummaryQuery.error}
        />

        {/* Top Operational Metrics */}
        <div className="flex items-center gap-16 shrink-0 border erp-border bg-erp-surface rounded-xl p-6 shadow-sm mb-4">
          <div className="flex flex-col">
            <span className="text-erp-muted text-[10px] uppercase font-bold tracking-widest mb-1">观察池标的总数</span>
            <span className="text-4xl font-mono font-bold leading-none text-erp-text">{String(watchlist?.overview?.totalCount ?? 0)}</span>
            <span className="text-[10px] text-erp-muted mt-2 uppercase font-bold">精选占比: {formatPercent(Number(watchlist?.overview?.overlayCount ?? 0) / (Number(watchlist?.overview?.totalCount) || 1))}</span>
          </div>
          <div className="w-px h-12 bg-erp-border"></div>
          <div className="flex flex-col">
            <span className="text-erp-muted text-[10px] uppercase font-bold tracking-widest mb-1">今日风险提醒</span>
            <span className={`text-4xl font-mono font-bold leading-none ${alerts.length ? 'text-erp-danger' : 'text-erp-muted opacity-50'}`}>{alerts.length}</span>
            <span className="text-[10px] text-erp-muted mt-2 uppercase font-bold">高优先级: {alerts.filter(a => a.tone === 'warn').length}</span>
          </div>
          <div className="w-px h-12 bg-erp-border"></div>
          <div className="flex flex-col">
            <span className="text-erp-muted text-[10px] uppercase font-bold tracking-widest mb-1">当前最强模型 (年化)</span>
            <span className="text-4xl font-mono font-bold leading-none text-erp-danger">{formatPercent(overview?.bestAnnualized?.daily_portfolio_annualized_return)}</span>
            <span className="text-[10px] text-erp-muted mt-2 uppercase font-bold tracking-widest">{String(overview?.bestAnnualized?.model ?? '-').toUpperCase()}</span>
          </div>
          
          <div className="flex-1 max-w-xl border-l erp-border pl-10 ml-4">
             <div className={`p-4 rounded-lg border-2 border-dashed transition-colors ${serviceRunning ? 'bg-green-500/10 border-green-500/30' : 'bg-red-500/10 border-red-500/30'}`}>
                <div className="text-xs font-bold uppercase mb-2 flex items-center gap-2">
                   <div className={`w-2 h-2 rounded-full ${serviceRunning ? 'bg-erp-success animate-pulse shadow-[0_0_8px_rgba(34,197,94,0.6)]' : 'bg-erp-danger'}`}></div>
                   系统运行环境快照
                </div>
                <div className="text-erp-muted text-sm leading-relaxed">
                   当前行情源: <span className="font-bold text-erp-text">{realtimeSource.label}</span> | 
                   快照时间: <span className="font-mono">{String(realtimeSnapshot.snapshot_label_display || '未同步')}</span>
                   <br/>
                   最近操作结果: <span className={`font-bold ${latestAction?.ok ? 'text-erp-success' : 'text-erp-danger'}`}>{latestAction ? (latestAction.ok ? 'SUCCESS' : 'FAILED') : 'NONE'}</span>
                </div>
             </div>
          </div>
        </div>

        {/* Alerts Section (Only if exists) */}
        {alerts.length > 0 && (
          <section className="flex flex-col gap-6">
            <h4 className="text-erp-danger font-bold text-sm flex items-center gap-2 border-l-4 border-erp-danger pl-3 uppercase tracking-wider">
              <i className="ph-fill ph-warning-octagon"></i> 核心交易风险提醒 (Critical Alerts)
            </h4>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
               {alerts.map((alert, idx) => (
                 <div key={idx} className="flex gap-4 p-4 bg-red-50/20 border border-red-100 rounded-lg">
                    <i className={`ph-fill ${alert.tone === 'warn' ? 'ph-warning text-erp-danger' : 'ph-info text-blue-500'} text-xl shrink-0`}></i>
                    <div className="flex flex-col">
                       <span className="font-bold text-gray-800">{alert.title}</span>
                       <span className="text-sm text-gray-600 mt-1">{alert.detail}</span>
                    </div>
                 </div>
               ))}
            </div>
          </section>
        )}

        {/* Focused Work Area */}
        <div className="grid grid-cols-2 gap-16">
          {/* Focused Position */}
          <section className="flex flex-col gap-8">
             <div className="flex items-center justify-between">
                <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3 uppercase text-erp">
                  <i className="ph ph-target"></i> 核心持仓盯盘 (Focus Position)
                </h4>
                {Boolean(focusWatchRecord.ts_code) && (
                  <button className="text-xs text-erp-primary hover:underline font-bold" onClick={() => navigate(buildWatchlistPath(String(focusWatchRecord.ts_code)))}>
                    查看详情报告 <i className="ph ph-arrow-right"></i>
                  </button>
                )}
             </div>
             
             {focusWatchRecord.ts_code ? (
               <div className="flex flex-col gap-6">
                  <div className="flex items-end gap-4">
                     <span className="text-3xl font-bold text-gray-800">{String(focusWatchRecord.name)}</span>
                     <span className="text-lg font-mono text-gray-400 mb-1">{String(focusWatchRecord.ts_code)}</span>
                     <span className="px-2 py-0.5 bg-blue-50 text-blue-600 border border-blue-100 rounded text-xs mb-1.5 uppercase font-bold shrink-0">
                        {String(focusWatchRecord.source_category || '持仓')}
                     </span>
                  </div>
                  <div className="grid grid-cols-2 gap-8 bg-gray-50/50 p-6 rounded-xl border erp-border border-dashed">
                     <div className="flex flex-col">
                        <span className="text-[10px] text-gray-400 font-bold uppercase mb-1">盘中涨跌</span>
                        <span className={`text-2xl font-mono font-bold ${Number(focusWatchRecord.realtime_pct_chg) > 0 ? 'text-erp-danger' : 'text-erp-success'}`}>
                           {formatPercent(focusWatchRecord.realtime_pct_chg)}
                        </span>
                     </div>
                     <div className="flex flex-col">
                        <span className="text-[10px] text-gray-400 font-bold uppercase mb-1">推理排名</span>
                        <span className="text-2xl font-mono font-bold text-gray-700"># {String(focusWatchRecord.inference_ensemble_rank || '-')}</span>
                     </div>
                     <div className="col-span-2">
                        <span className="text-[10px] text-gray-400 font-bold uppercase mb-1">AI 策略摘要</span>
                        <p className="text-sm text-gray-600 leading-relaxed italic">
                           "{String(focusWatchRecord.llm_latest_summary || focusWatchRecord.premarket_plan || '暂无详细分析')}"
                        </p>
                     </div>
                  </div>
               </div>
             ) : (
               <div className="h-48 erp-border border-dashed rounded-xl flex items-center justify-center text-gray-400 text-sm">
                  今日暂无重点盯盘股票
               </div>
             )}
          </section>

          {/* Focused Candidate */}
          <section className="flex flex-col gap-8">
             <div className="flex items-center justify-between">
                <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3 uppercase text-erp">
                  <i className="ph ph-lightning"></i> 重点候选信号 (Focus Candidate)
                </h4>
                {Boolean(focusCandidateRecord.ts_code) && (
                  <button className="text-xs text-erp-primary hover:underline font-bold" onClick={() => navigate(buildCandidatesPath(String(focusCandidateRecord.ts_code)))}>
                    查看完整候选池 <i className="ph ph-arrow-right"></i>
                  </button>
                )}
             </div>

             {focusCandidateRecord.ts_code ? (
               <div className="flex flex-col gap-6">
                  <div className="flex items-end gap-4">
                     <span className="text-3xl font-bold text-gray-800">{String(focusCandidateRecord.name)}</span>
                     <span className="text-lg font-mono text-gray-400 mb-1">{String(focusCandidateRecord.ts_code)}</span>
                     <span className="px-2 py-0.5 bg-gray-100 text-gray-500 border border-gray-200 rounded text-xs mb-1.5 uppercase font-bold shrink-0">
                        {String(focusCandidateRecord.industry_display || '候选')}
                     </span>
                  </div>
                  <div className="grid grid-cols-2 gap-8 bg-gray-50/50 p-6 rounded-xl border erp-border border-dashed">
                     <div className="flex flex-col">
                        <span className="text-[10px] text-gray-400 font-bold uppercase mb-1">AI 综合总分</span>
                        <span className="text-2xl font-mono font-bold text-erp-primary">
                           {formatValue(focusCandidateRecord.final_score)}
                        </span>
                     </div>
                     <div className="flex flex-col">
                        <span className="text-[10px] text-gray-400 font-bold uppercase mb-1">预测 10D 收益</span>
                        <span className="text-2xl font-mono font-bold text-erp-danger">
                           {formatPercent(focusCandidateRecord.ret_t1_t10)}
                        </span>
                     </div>
                     <div className="col-span-2">
                        <span className="text-[10px] text-gray-400 font-bold uppercase mb-1">入选理由</span>
                        <p className="text-sm text-gray-600 leading-relaxed italic">
                           "{String(focusCandidateRecord.action_hint || focusCandidateRecord.thesis_summary || '暂无明确理由')}"
                        </p>
                     </div>
                  </div>
               </div>
             ) : (
               <div className="h-48 erp-border border-dashed rounded-xl flex items-center justify-center text-gray-400 text-sm">
                  今日暂无显著买入候选信号
               </div>
             )}
          </section>
        </div>

        {/* Global Summary Data Grids - Expandable Sections */}
        <section className="border-t erp-border pt-12 flex flex-col gap-10">
           <div className="flex flex-col gap-4">
              <div className="flex items-center justify-between">
                <h4 className="text-gray-700 font-bold text-xs uppercase tracking-widest flex items-center gap-2">
                  <i className="ph ph-table"></i> 今日观察池数据概览 (Watchlist Data Preview)
                </h4>
                <NavLink to="/watchlist" className="text-[10px] text-erp-primary font-bold hover:underline">查看全量表格</NavLink>
              </div>
              <div className="erp-border rounded-lg overflow-hidden h-[300px]">
                <DataTable
                  rows={watchlist?.records ?? []}
                  columns={WATCHLIST_COLUMNS}
                  columnLabels={WATCHLIST_COLUMN_LABELS}
                  storageKey="home-watchlist"
                  loading={homeWatchlistQuery.isLoading}
                  emptyText="暂无数据"
                  stickyFirstColumn
                  cellRenderers={watchlistCellRenderers}
                />
              </div>
           </div>

           <div className="flex flex-col gap-4">
              <div className="flex items-center justify-between">
                <h4 className="text-gray-700 font-bold text-xs uppercase tracking-widest flex items-center gap-2">
                  <i className="ph ph-table"></i> 智能研报精选列表 (AI Shortlist)
                </h4>
                <NavLink to="/ai-review" className="text-[10px] text-erp-primary font-bold hover:underline">查看深度研报</NavLink>
              </div>
              <div className="erp-border rounded-lg overflow-hidden h-[300px]">
                <DataTable
                  rows={aiReview?.inferenceRecords ?? []}
                  columns={INFERENCE_COLUMNS}
                  columnLabels={INFERENCE_COLUMN_LABELS}
                  storageKey="home-ai-inference"
                  loading={homeAiReviewQuery.isLoading}
                  emptyText="暂无数据"
                  stickyFirstColumn
                  cellRenderers={AI_CANDIDATE_CELL_RENDERERS}
                />
              </div>
           </div>
        </section>

        {/* System Config Snippet */}
        <section className="mb-10 bg-gray-50/50 p-6 rounded-xl erp-border border-dotted text-[11px] text-gray-500 leading-relaxed font-mono">
           <div className="font-bold text-gray-400 mb-2 uppercase">System Configuration Snippet:</div>
           {homeSummaryQuery.data?.configSummaryText || 'No custom parameters active.'}
        </section>
      </div>
    </div>
  )
}
