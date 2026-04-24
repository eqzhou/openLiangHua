import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate } from 'react-router-dom'

import { apiGet } from '../api/client'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { candidatesPageClient, candidatesSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDate } from '../lib/format'
import { buildCandidatesPath, copyShareablePageLink } from '../lib/shareLinks'
import type { BootstrapPayload, CandidatesSummaryPayload, JsonRecord } from '../types/api'

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

const CANDIDATES_VIEW_PRESETS = [
  { key: 'decision', label: '决策', columns: ['name', 'rank', 'score', 'rank_pct', 'ret_t1_t10', 'pct_chg'] },
  { key: 'momentum', label: '动量', columns: ['name', 'rank', 'pct_chg', 'mom_20', 'close_to_ma_20', 'ret_t1_t10'] },
]

export function CandidatesPage({ bootstrap }: CandidatesPageProps) {
  const location = useLocation()
  const navigate = useNavigate()
  const { params, updateParams } = usePageSearchState(candidatesPageClient)

  const summaryQuery = useQuery({
    queryKey: candidatesSummaryClient.queryKey(params),
    queryFn: () => apiGet<CandidatesSummaryPayload>(candidatesSummaryClient.path(params)),
  })

  const latestPicks = useMemo(() => summaryQuery.data?.latestPicks ?? [], [summaryQuery.data?.latestPicks])
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

  const openDetail = (symbol: string) => {
    if (!symbol) {
      return
    }
    navigate(buildCandidatesPath(symbol) + location.search)
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-users-three text-erp-primary"></i> 
          候选池分析
        </span>
        <span className="px-1.5 py-0.5 bg-blue-50 text-blue-700 text-erp-sm border border-blue-200 rounded shrink-0">
          {String(summaryQuery.data?.modelName ?? params.model).toUpperCase()}
        </span>
        <span className="px-1.5 py-0.5 bg-gray-100 text-gray-600 text-erp-sm border border-gray-200 rounded shrink-0">
          {String(summaryQuery.data?.splitName ?? params.split).toUpperCase()}
        </span>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        {/* Quick Filters in Toolbar */}
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">模型:</span>
          <select 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary bg-white"
            value={params.model} 
            onChange={(event) => updateParams({ model: event.target.value, page: 1 })}
          >
            {(bootstrap?.modelNames ?? ['ridge', 'lgbm', 'ensemble']).map((item) => (
              <option key={item} value={item}>{bootstrap?.modelLabels?.[item] ?? item}</option>
            ))}
          </select>
        </div>
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">分段:</span>
          <select 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary bg-white"
            value={params.split} 
            onChange={(event) => updateParams({ split: event.target.value, page: 1 })}
          >
            {(bootstrap?.splitNames ?? ['valid', 'test']).map((item) => (
              <option key={item} value={item}>{bootstrap?.splitLabels?.[item] ?? item}</option>
            ))}
          </select>
        </div>
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">数量:</span>
          <input 
            type="number" min={10} max={100}
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary w-16"
            value={params.topN} 
            onChange={(event) => updateParams({ topN: Number(event.target.value) || 30, page: 1 })} 
          />
        </div>

        <button className="toolbar-btn ml-2 shrink-0" onClick={() => copyShareablePageLink(location.pathname, location.search)}>
          <i className="ph ph-link"></i> 复制视图
        </button>
        
        <div className="ml-auto flex items-center gap-4 text-erp-sm shrink-0">
          <div className="flex items-center gap-1">
            <span className="text-gray-500">信号日期:</span> 
            <span className="font-bold font-mono">{summaryQuery.data?.latestDate ? formatDate(summaryQuery.data.latestDate) : '-'}</span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">总候选数:</span> 
            <span className="font-bold font-mono">{summaryQuery.data?.totalCount ?? 0}</span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">正收益样本:</span> 
            <span className={`font-bold font-mono ${positiveCount > 0 ? 'text-erp-success' : ''}`}>
              {positiveCount}
            </span>
          </div>
        </div>
      </div>

      {/* Main Grid Area */}
      <div className="flex-1 bg-white flex flex-col relative overflow-hidden">
        <DataTable
          rows={latestPicks}
          columns={PICKS_COLUMNS}
          columnLabels={PICK_COLUMN_LABELS}
          storageKey="candidate-picks"
          viewPresets={CANDIDATES_VIEW_PRESETS}
          defaultPresetKey="decision"
          loading={summaryQuery.isLoading}
          emptyText="暂无候选股数据"
          stickyFirstColumn
          getRowId={(row) => String(row.ts_code ?? '')}
          onRowClick={(row) => openDetail(String(row.ts_code ?? ''))}
          rowTitle="点击进入详情"
          cellRenderers={candidateCellRenderers}
        />
      </div>

      {/* Pager Status Bar */}
      <div className="h-8 erp-border-t bg-gray-50 flex items-center px-2 justify-between text-gray-500 text-erp-sm shrink-0">
        <div className="flex items-center gap-4">
          <span>共 {summaryQuery.data?.totalCount ?? 0} 条记录</span>
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
    </div>
  )
}
