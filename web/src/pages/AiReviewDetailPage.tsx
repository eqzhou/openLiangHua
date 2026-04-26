import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate, useParams, NavLink } from 'react-router-dom'

import { apiGet } from '../api/client'
import { DataTable } from '../components/DataTable'
import { InsightList } from '../components/InsightList'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { aiReviewDetailClient, aiReviewPageClient, aiReviewSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { splitTextPoints } from '../lib/format'
import { buildAiReviewPath } from '../lib/shareLinks'
import type { AiReviewDetailPayload, AiReviewSummaryPayload, JsonRecord } from '../types/api'

const FIELD_COLUMNS = ['field', 'value']

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

export function AiReviewDetailPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const { params } = usePageSearchState(aiReviewPageClient)
  const { scope = 'inference', symbol = '' } = useParams<{ scope: 'inference' | 'historical'; symbol: string }>()

  const detailQuery = useQuery({
    queryKey: aiReviewDetailClient.queryKey(scope === 'historical' ? 'historical' : 'inference', symbol),
    queryFn: () => apiGet<AiReviewDetailPayload>(aiReviewDetailClient.path(scope === 'historical' ? 'historical' : 'inference', symbol)),
    enabled: Boolean(symbol),
  })

  const summaryQuery = useQuery({
    queryKey: aiReviewSummaryClient.queryKey(params),
    queryFn: () => apiGet<AiReviewSummaryPayload>(aiReviewSummaryClient.path(params)),
  })

  const selected = detailQuery.data?.selectedRecord ?? {}
  const llmFieldRows = Object.entries(detailQuery.data?.llmResponse ?? {}).map(([field, value]) => ({ field, value } as JsonRecord))
  const panel = scope === 'historical' ? summaryQuery.data?.historical : summaryQuery.data?.inference
  const candidates = panel?.candidates ?? []
  const candidateSymbols = candidates.map((row: JsonRecord) => String(row.ts_code ?? '')).filter(Boolean)
  const currentIndex = candidateSymbols.findIndex((item: string) => item === symbol)
  const previousSymbol = currentIndex > 0 ? candidateSymbols[currentIndex - 1] : null
  const nextSymbol = currentIndex >= 0 && currentIndex < candidateSymbols.length - 1 ? candidateSymbols[currentIndex + 1] : null
  const currentPositionLabel = currentIndex >= 0 ? `${currentIndex + 1} / ${candidateSymbols.length}` : '-'

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp bg-erp-bg">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <NavLink to={`/ai-review${location.search}`} className="toolbar-btn shrink-0">
          <i className="ph ph-arrow-left text-erp-primary"></i> 返回列表
        </NavLink>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-brain text-erp-primary"></i>
          {String(selected.name || symbol)} ({symbol})
        </span>
        <span className={`px-1.5 py-0.5 text-erp-sm border rounded shrink-0 ${scope === 'historical' ? 'bg-gray-100 text-gray-600 border-gray-200' : 'bg-blue-50 text-blue-700 border-blue-200'}`}>
          {scope === 'historical' ? '历史验证' : '最新推理'}
        </span>
        <span className={`px-1.5 py-0.5 text-erp-sm border rounded shrink-0 bg-white ${getConfidenceTone(selected.confidence_level) === 'good' ? 'text-erp-success border-erp-success/30' : getConfidenceTone(selected.confidence_level) === 'warn' ? 'text-erp-danger border-erp-danger/30' : 'text-gray-500 border-gray-300'}`}>
          置信度: {String(selected.confidence_level || '中')}
        </span>
        <span className="px-1.5 py-0.5 bg-gray-100 text-gray-600 text-erp-sm border border-gray-200 rounded shrink-0">
          序号 {currentPositionLabel}
        </span>
        
        <div className="ml-auto flex items-center gap-2 text-erp-sm shrink-0">
           <a href={`https://xueqiu.com/S/${symbol.replace('.', '')}`} target="_blank" rel="noreferrer" className="toolbar-btn text-erp-primary hover:underline">
             雪球
           </a>
           <a href={`https://quote.eastmoney.com/${symbol.replace('.', '').toLowerCase()}.html`} target="_blank" rel="noreferrer" className="toolbar-btn text-erp-primary hover:underline">
             东方财富
           </a>
        </div>
      </div>
      
      <div className="flex-1 overflow-y-auto bg-white flex flex-col p-6 gap-8">
        <QueryNotice isLoading={detailQuery.isLoading || summaryQuery.isLoading} error={detailQuery.error ?? summaryQuery.error} />
        
        {/* Header Summary Bar - Flat */}
        <div className="flex items-center gap-12 shrink-0 border erp-border bg-erp-surface rounded-xl p-6 shadow-sm mb-4">
          <div className="flex flex-col">
            <div className="text-erp-muted text-[10px] uppercase font-bold tracking-widest mb-1">综合得分 / Final Score</div>
            <div className="text-3xl font-mono font-bold text-erp-primary leading-none">{String(selected.final_score ?? '-')}</div>
          </div>
          <div className="w-px h-10 bg-erp-border"></div>
          <div className="grid grid-cols-3 gap-8">
             <div className="flex flex-col">
               <span className="text-erp-muted text-[10px] uppercase font-bold">量化得分</span>
               <span className="text-lg font-mono font-bold text-erp-text">{String(selected.quant_score ?? '-')}</span>
             </div>
             <div className="flex flex-col">
               <span className="text-erp-muted text-[10px] uppercase font-bold">叠加得分</span>
               <span className="text-lg font-mono font-bold text-erp-text">{String(selected.factor_overlay_score ?? '-')}</span>
             </div>
             <div className="flex flex-col">
               <span className="text-erp-muted text-[10px] uppercase font-bold">模型共识</span>
               <span className="text-lg font-mono font-bold text-erp-text">{String(selected.model_consensus ?? '-')}</span>
             </div>
          </div>
          <div className="flex-1 max-w-2xl border-l erp-border pl-8 ml-4">
            <div className="text-erp-muted text-[10px] uppercase font-bold mb-1">核心论点 (Thesis Summary)</div>
            <div className="text-erp-text text-sm leading-relaxed italic">
              "{String(selected.thesis_summary ?? selected.action_hint ?? '暂无结论摘要')}"
            </div>
          </div>
        </div>

        {/* Multi-Section Flow Area */}
        <div className="grid grid-cols-2 gap-12 shrink-0">
          {/* Left Column */}
          <div className="flex flex-col gap-8">
             <section>
                <h4 className="text-erp-primary font-bold text-sm mb-4 border-l-4 border-erp-primary pl-3">论点与风险核查</h4>
                <div className="flex flex-col gap-6">
                   <InsightList title="✅ 看多要点 (Bull Points)" items={splitTextPoints(selected.bull_points)} tone="good" emptyText="暂无看多要点" />
                   <InsightList title="⚠️ 风险提示 (Risk Points)" items={splitTextPoints(selected.risk_points)} tone="warn" emptyText="暂无风险提示" />
                </div>
             </section>
          </div>

          {/* Right Column */}
          <section>
             <h4 className="text-erp-primary font-bold text-sm mb-4 border-l-4 border-erp-primary pl-3">外部研究资料索引</h4>
             <div className="bg-gray-50 rounded-lg p-4 erp-border border-dashed">
                <PropertyGrid
                  items={[
                    { label: '公告概览', value: String(selected.notice_digest || '-'), span: 'double' },
                    { label: '新闻概览', value: String(selected.news_digest || '-'), helper: String(selected.news_source || ''), span: 'double' },
                    { label: '研报概览', value: String(selected.research_digest || '-'), span: 'double' },
                  ]}
                />
             </div>
          </section>
        </div>

        {/* Raw Models & Fields Tables */}
        <section className="border-t erp-border pt-8 mt-4 grid grid-cols-2 gap-12 shrink-0 h-[400px]">
           <div className="flex flex-col h-full overflow-hidden">
             <div className="text-[10px] text-gray-400 font-bold uppercase mb-2 flex items-center gap-2">
                <i className="ph ph-intersect"></i> 外部 LLM 原始响应对账
             </div>
             <div className="flex-1 overflow-hidden relative erp-border rounded">
                <DataTable rows={llmFieldRows} columns={FIELD_COLUMNS} storageKey={`ai-review-${scope}-llm-response`} enableColumnManager={false} emptyText="暂无数据" />
             </div>
           </div>
           
           <div className="flex flex-col h-full overflow-hidden">
             <div className="text-[10px] text-gray-400 font-bold uppercase mb-2 flex items-center gap-2">
                <i className="ph ph-list-dashes"></i> 全量细节字段 (Full Fields)
             </div>
             <div className="flex-1 overflow-hidden relative erp-border rounded">
                <DataTable rows={detailQuery.data?.fieldRows ?? []} columns={FIELD_COLUMNS} storageKey={`ai-review-${scope}-all-fields`} enableColumnManager={false} emptyText="暂无字段" />
             </div>
           </div>
        </section>
      </div>

      {/* Pager Status Bar */}
      <div className="h-8 erp-border-t bg-gray-50 flex items-center px-2 justify-between text-gray-500 text-erp-sm shrink-0">
        <div className="flex items-center gap-4">
          <span>AI 建议: <span className="font-bold text-erp-primary">{String(selected.action_hint ?? '继续持有')}</span></span>
        </div>
        <div className="flex items-center gap-2">
          <button 
            className="toolbar-btn" 
            disabled={!previousSymbol} 
            onClick={previousSymbol ? () => navigate({ pathname: buildAiReviewPath(previousSymbol, scope), search: location.search }) : undefined}
          >
            <i className="ph ph-caret-left"></i> 上一只 ({previousSymbol || '-'})
          </button>
          <span>{currentPositionLabel}</span>
          <button 
            className="toolbar-btn"
            disabled={!nextSymbol}
            onClick={nextSymbol ? () => navigate({ pathname: buildAiReviewPath(nextSymbol, scope), search: location.search }) : undefined}
          >
            下一只 ({nextSymbol || '-'}) <i className="ph ph-caret-right"></i>
          </button>
        </div>
      </div>
    </div>
  )
}
