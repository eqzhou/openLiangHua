import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useLocation, useNavigate } from 'react-router-dom'

import { apiGet, apiPost, apiDelete } from '../api/client'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { useToast } from '../components/ToastProvider'
import { WatchlistEditDialog, type WatchlistEditItem } from '../components/WatchlistAddDialog'
import { realtimeRefreshClient, watchlistPageClient, watchlistSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { normalizeRealtimeFailedSymbols } from '../lib/realtime'
import { buildWatchlistPath, copyShareablePageLink } from '../lib/shareLinks'
import type { BootstrapPayload, JsonRecord, RealtimeRefreshPayload, WatchlistSummaryPayload } from '../types/api'

interface WatchlistPageProps {
  bootstrap?: BootstrapPayload
  authenticated?: boolean
}

const WATCHLIST_COLUMNS = [
  'name',
  'source_tags',
  'industry',
  'mark_price',
  'realtime_price',
  'realtime_pct_chg',
  'unrealized_pnl_pct',
  'ensemble_rank',
  'inference_ensemble_rank',
  'premarket_plan',
  'llm_latest_status',
  'actions',
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
  actions: '操作',
}

const DEFAULT_SCOPE_LABELS: Record<string, string> = {
  all: '全部',
  holdings: '只看持仓',
  focus: '只看重点关注',
  overlay: '只看 AI 精选',
  inference: '只看最新推理池',
  loss: '只看浮亏较大',
}

const WATCHLIST_VIEW_PRESETS = [
  { key: 'trading', label: '交易', columns: ['name', 'realtime_price', 'realtime_pct_chg', 'mark_price', 'premarket_plan', 'llm_latest_status', 'actions'] },
  { key: 'ranking', label: '排名', columns: ['source_tags', 'name', 'industry', 'ensemble_rank', 'inference_ensemble_rank', 'premarket_plan', 'actions'] },
]

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
  
  const [addDialogOpen, setAddDialogOpen] = useState(false)
  const [editingItem, setEditingItem] = useState<WatchlistEditItem | null>(null)

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

  const deleteMutation = useMutation({
    mutationFn: (args: { ts_code: string, type: string }) => apiDelete(`/api/watchlist-config/items/${args.ts_code}/${args.type}`),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['watchlist-summary'] })
      pushToast({ tone: 'success', title: '删除成功' })
    },
    onError: (error) => {
      pushToast({ tone: 'error', title: '删除失败', description: toErrorMessage(error) })
    }
  })

  const records = useMemo(() => summaryQuery.data?.records ?? [], [summaryQuery.data?.records])
  const scopeLabels = bootstrap?.watchScopes ? { ...bootstrap.watchScopes, ...DEFAULT_SCOPE_LABELS } : DEFAULT_SCOPE_LABELS
  const realtimeStatus = summaryQuery.data?.realtimeStatus ?? {}
  const failedSymbols = useMemo(() => normalizeRealtimeFailedSymbols(realtimeStatus.failed_symbols), [realtimeStatus.failed_symbols])
  const writeLocked = !authenticated

  const handleEdit = (row: JsonRecord, e: React.MouseEvent) => {
    e.stopPropagation()
    const type = String(row.entry_group ?? '') === '重点关注' ? 'focus' : 'holding'
    setEditingItem({
      ts_code: String(row.ts_code ?? ''),
      name: String(row.name ?? ''),
      type: type,
      cost: row.cost_basis ? Number(row.cost_basis) : null,
      shares: row.shares ? Number(row.shares) : null,
      note: String(row.focus_note ?? ''),
    })
    setAddDialogOpen(true)
  }

  const handleDelete = (row: JsonRecord, e: React.MouseEvent) => {
    e.stopPropagation()
    if (window.confirm(`确定要从数据库中删除 ${row.name || row.ts_code} 吗？`)) {
      const type = String(row.entry_group ?? '') === '重点关注' ? 'focus' : 'holding'
      deleteMutation.mutate({ ts_code: String(row.ts_code ?? ''), type })
    }
  }

  const watchlistCellRenderers = {
    name: (row: JsonRecord) => (
      <EntityCell
        title={String(row.name ?? '-')}
        subtitle={String(row.ts_code ?? '')}
        meta={String(row.industry || row.source_category || '')}
        badges={[
          row.is_overlay_selected ? { label: '精选', tone: 'good' as const } : null,
          row.is_inference_overlay_selected ? { label: '最新', tone: 'brand' as const } : null,
        ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
      />
    ),
    actions: (row: JsonRecord) => (
      <div className="flex items-center gap-2">
        <button 
          onClick={(e) => handleEdit(row, e)} 
          disabled={writeLocked}
          className="text-gray-400 hover:text-erp-primary transition-colors disabled:opacity-50 disabled:cursor-not-allowed p-1"
          title="修改持仓/备忘"
        >
          <i className="ph ph-pencil-simple text-lg"></i>
        </button>
        <button 
          onClick={(e) => handleDelete(row, e)} 
          disabled={writeLocked || deleteMutation.isPending}
          className="text-gray-400 hover:text-erp-danger transition-colors disabled:opacity-50 disabled:cursor-not-allowed p-1"
          title="删除"
        >
          <i className="ph ph-trash text-lg"></i>
        </button>
      </div>
    ),
  }

  const openDetail = (symbol: string) => {
    if (!symbol) {
      return
    }
    // Only navigate to the detail portion, preserve the rest of the workspace state
    navigate(buildWatchlistPath(symbol) + location.search)
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-list-dashes text-erp-primary"></i> 
          持仓与观察池
        </span>
        <span className="px-1.5 py-0.5 bg-blue-50 text-blue-700 text-erp-sm border border-blue-200 rounded shrink-0">
          {scopeLabels[params.scope] ?? params.scope}
        </span>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        {/* Quick Filters in Toolbar */}
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">范围:</span>
          <select 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary bg-white"
            value={params.scope} 
            onChange={(event) => updateParams({ scope: event.target.value, page: 1 })}
          >
            {Object.entries(scopeLabels).map(([key, label]) => (
              <option key={key} value={key}>{label}</option>
            ))}
          </select>
        </div>
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">搜索:</span>
          <input 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary w-32"
            value={params.keyword} 
            onChange={(event) => updateParams({ keyword: event.target.value, page: 1 })} 
            placeholder="代码/名称" 
          />
        </div>

        <button className="toolbar-btn ml-2 shrink-0" onClick={() => copyShareablePageLink(location.pathname, location.search)}>
          <i className="ph ph-link"></i> 复制视图
        </button>

        <button 
          className="toolbar-btn shrink-0"
          onClick={() => {
            setEditingItem(null)
            setAddDialogOpen(true)
          }}
          disabled={writeLocked}
        >
          <i className="ph ph-plus-circle text-erp-success"></i> 
          添加标的
        </button>

        <button 
          className={`toolbar-btn shrink-0 ${refreshRealtimeMutation.isPending ? 'disabled' : ''}`}
          onClick={() => refreshRealtimeMutation.mutate()}
          disabled={writeLocked || refreshRealtimeMutation.isPending}
        >
          <i className={`ph ph-arrows-clockwise ${refreshRealtimeMutation.isPending ? 'animate-spin' : 'text-erp-success'}`}></i> 
          刷新行情
        </button>
        
        <div className="ml-auto flex items-center gap-4 text-erp-sm shrink-0">
          <div className="flex items-center gap-1">
            <span className="text-gray-500">记录数:</span> 
            <span className="font-bold font-mono">{summaryQuery.data?.filteredCount ?? 0}</span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">参考市值:</span> 
            <span className="font-bold font-mono">{String(summaryQuery.data?.overview?.marketValue ?? 0)}</span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">浮动盈亏:</span> 
            <span className={`font-bold font-mono ${
              Number(summaryQuery.data?.overview?.unrealizedPnl ?? 0) > 0 ? 'text-erp-danger' : 
              Number(summaryQuery.data?.overview?.unrealizedPnl ?? 0) < 0 ? 'text-erp-success' : ''
            }`}>
              {String(summaryQuery.data?.overview?.unrealizedPnl ?? 0)}
            </span>
          </div>
        </div>
      </div>

      {/* Main Grid Area */}
      <div className="flex-1 bg-white flex flex-col relative overflow-hidden">
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
      </div>

      {/* Pager Status Bar */}
      <div className="h-8 erp-border-t bg-gray-50 flex items-center px-2 justify-between text-gray-500 text-erp-sm shrink-0">
        <div className="flex items-center gap-4">
          <span>行情快照: {String(realtimeStatus.snapshot_label_display ?? '暂无')}</span>
          {failedSymbols.length > 0 && <span className="text-erp-warning">失败: {failedSymbols.join(', ')}</span>}
        </div>
        <div className="flex items-center gap-2">
          <button 
            className="toolbar-btn" 
            disabled={(summaryQuery.data?.page ?? params.page) <= 1} 
            onClick={() => updateParams({ page: Math.max(1, (summaryQuery.data?.page ?? params.page) - 1) })}
          >
            <i className="ph ph-caret-left"></i> 上一页
          </button>
          <span>第 {summaryQuery.data?.page ?? params.page} / {summaryQuery.data?.totalPages ?? 1} 页</span>
          <button 
            className="toolbar-btn"
            disabled={(summaryQuery.data?.page ?? params.page) >= (summaryQuery.data?.totalPages ?? 1)}
            onClick={() => updateParams({ page: Math.min(summaryQuery.data?.totalPages ?? 1, (summaryQuery.data?.page ?? params.page) + 1) })}
          >
            下一页 <i className="ph ph-caret-right"></i>
          </button>
        </div>
      </div>
      
      <WatchlistEditDialog 
        key={String(addDialogOpen) + String(editingItem?.ts_code ?? '')}
        open={addDialogOpen} 
        onClose={() => {
          setAddDialogOpen(false)
          setEditingItem(null)
        }} 
        initialItem={editingItem}
      />
    </div>
  )
}
