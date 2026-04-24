import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate } from 'react-router-dom'

import { apiGet } from '../api/client'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { QueryNotice } from '../components/QueryNotice'
import { aiReviewPageClient, aiReviewSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDate } from '../lib/format'
import { buildAiReviewPath } from '../lib/shareLinks'
import type { AiReviewSummaryPayload, JsonRecord } from '../types/api'

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

export function AiReviewPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const { params } = usePageSearchState(aiReviewPageClient)

  const summaryQuery = useQuery({
    queryKey: aiReviewSummaryClient.queryKey(params),
    queryFn: () => apiGet<AiReviewSummaryPayload>(aiReviewSummaryClient.path(params)),
  })

  const inference = summaryQuery.data?.inference
  const historical = summaryQuery.data?.historical

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-brain text-erp-primary"></i> 
          AI 智能分析对照
        </span>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        {/* State Indicators */}
        <div className="flex items-center gap-4 text-erp-sm shrink-0">
           <div className="flex items-center gap-1">
             <span className="text-gray-500">最新推理:</span>
             <span className="font-bold text-erp-primary">{inference?.candidateCount ?? 0} 只</span>
             <span className="text-[10px] text-gray-400">({formatDate(inference?.candidates?.[0]?.trade_date)})</span>
           </div>
           <div className="flex items-center gap-1">
             <span className="text-gray-500">历史验证:</span>
             <span className="font-bold">{historical?.candidateCount ?? 0} 只</span>
             <span className="text-[10px] text-gray-400">({formatDate(historical?.candidates?.[0]?.trade_date)})</span>
           </div>
        </div>

        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>

        {/* Quick Jumper in Toolbar */}
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">定位定位:</span>
          <select 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary bg-white max-w-[150px]"
            value={params.inference} 
            onChange={(event) => event.target.value && navigate({ pathname: buildAiReviewPath(event.target.value, 'inference'), search: location.search })}
          >
            <option value="">快速跳转推理详情...</option>
            {(inference?.candidates ?? []).map((row) => (
              <option key={String(row.ts_code)} value={String(row.ts_code)}>{buildOptionLabel(row)}</option>
            ))}
          </select>
        </div>
        
        <div className="ml-auto flex items-center gap-4 text-erp-sm shrink-0">
          <div className="flex items-center gap-1">
            <span className="text-gray-500">双池总数:</span> 
            <span className="font-bold font-mono">{(inference?.candidateCount ?? 0) + (historical?.candidateCount ?? 0)}</span>
          </div>
        </div>
      </div>

      {/* Main Workspace Area: Dual High Density Grids */}
      <div className="flex-1 bg-erp-bg p-2 overflow-hidden flex gap-2">
        <QueryNotice isLoading={summaryQuery.isLoading} error={summaryQuery.error} />
        
        {/* Left Column: Inference */}
        <div className="flex-1 bg-white erp-border flex flex-col overflow-hidden">
           <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-semibold text-gray-700 shrink-0">
             <i className="ph ph-lightning mr-2 text-erp-primary"></i> 
             最新推理池 (Current Inference)
           </div>
           <div className="flex-1 overflow-auto relative">
              <DataTable
                rows={inference?.candidates ?? []}
                columns={CANDIDATE_COLUMNS}
                columnLabels={CANDIDATE_COLUMN_LABELS}
                storageKey="ai-review-inference-candidates"
                viewPresets={AI_VIEW_PRESETS}
                defaultPresetKey="thesis"
                emptyText="暂无最新推理"
                stickyFirstColumn
                getRowId={(row) => String(row.ts_code ?? '')}
                onRowClick={(row) => navigate({ pathname: buildAiReviewPath(String(row.ts_code ?? ''), 'inference'), search: location.search })}
                rowTitle="点击进入详情"
                cellRenderers={AI_CANDIDATE_CELL_RENDERERS}
              />
           </div>
        </div>

        {/* Right Column: Historical */}
        <div className="flex-1 bg-white erp-border flex flex-col overflow-hidden">
           <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-semibold text-gray-700 shrink-0">
             <i className="ph ph-clock-counter-clockwise mr-2"></i> 
             历史验证池 (Historical Validation)
           </div>
           <div className="flex-1 overflow-auto relative text-erp">
              <DataTable
                rows={historical?.candidates ?? []}
                columns={CANDIDATE_COLUMNS}
                columnLabels={CANDIDATE_COLUMN_LABELS}
                storageKey="ai-review-historical-candidates"
                viewPresets={AI_VIEW_PRESETS}
                defaultPresetKey="thesis"
                emptyText="暂无历史验证"
                stickyFirstColumn
                getRowId={(row) => String(row.ts_code ?? '')}
                onRowClick={(row) => navigate({ pathname: buildAiReviewPath(String(row.ts_code ?? ''), 'historical'), search: location.search })}
                rowTitle="点击进入详情"
                cellRenderers={AI_CANDIDATE_CELL_RENDERERS}
              />
           </div>
        </div>
      </div>
    </div>
  )
}
