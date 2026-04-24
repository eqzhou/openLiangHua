import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate, useParams, NavLink } from 'react-router-dom'

import { apiGet } from '../api/client'
import { LineChartCard } from '../components/LineChartCard'
import { QueryNotice } from '../components/QueryNotice'
import { candidateDetailClient, candidateHistoryClient, candidatesPageClient, candidatesSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDate, formatPercent, formatValue } from '../lib/format'
import { buildAiReviewPath, buildCandidatesPath, buildWatchlistPath } from '../lib/shareLinks'
import type { CandidateDetailPayload, CandidateHistoryPayload, CandidatesSummaryPayload } from '../types/api'

function buildScoreViewOptions(keys: string[]) {
  if (!keys.length) {
    return []
  }
  if (keys.length === 1) {
    return [{ key: 'single', label: '单线', lineKeys: keys, subtitle: '当前只有一条评分序列可用。' }]
  }
  return [
    { key: 'combined', label: '评分与收益', lineKeys: keys, subtitle: '把评分和后续收益放在一起看，先判断信号质量。' },
    { key: 'score', label: '只看评分', lineKeys: [keys[0]], subtitle: '单独观察综合评分是否还在抬升。' },
    { key: 'forward', label: '只看收益', lineKeys: [keys[keys.length - 1]], subtitle: '单独观察后续收益窗口的兑现情况。' },
  ]
}

export function CandidateDetailPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const { symbol = '' } = useParams<{ symbol: string }>()
  const { params } = usePageSearchState(candidatesPageClient)
  const [scoreView, setScoreView] = useState('combined')

  const detailQuery = useQuery({
    queryKey: candidateDetailClient.queryKey(params, symbol),
    queryFn: () => apiGet<CandidateDetailPayload>(candidateDetailClient.path(params, symbol)),
    enabled: Boolean(symbol),
  })

  const summaryQuery = useQuery({
    queryKey: candidatesSummaryClient.queryKey(params),
    queryFn: () => apiGet<CandidatesSummaryPayload>(candidatesSummaryClient.path(params)),
  })

  const historyQuery = useQuery({
    queryKey: candidateHistoryClient.queryKey(params, symbol),
    queryFn: () => apiGet<CandidateHistoryPayload>(candidateHistoryClient.path(params, symbol)),
    enabled: Boolean(symbol),
  })

  const detail = detailQuery.data?.selectedRecord ?? {}
  const scoreHistory = useMemo(() => historyQuery.data?.scoreHistory ?? [], [historyQuery.data?.scoreHistory])
  const scoreKeys = useMemo(() => Object.keys(scoreHistory[0] ?? {}).filter((key) => key !== 'trade_date'), [scoreHistory])
  const scoreViewOptions = useMemo(() => buildScoreViewOptions(scoreKeys), [scoreKeys])
  const activeScoreView = scoreViewOptions.find((item) => item.key === scoreView) ?? scoreViewOptions[0]
  const symbolOptions = summaryQuery.data?.symbolOptions ?? []
  const currentIndex = symbolOptions.findIndex((item: string) => item === symbol)
  const previousSymbol = currentIndex > 0 ? symbolOptions[currentIndex - 1] : null
  const nextSymbol = currentIndex >= 0 && currentIndex < symbolOptions.length - 1 ? symbolOptions[currentIndex + 1] : null
  const currentPositionLabel = currentIndex >= 0 ? `${currentIndex + 1} / ${symbolOptions.length}` : '-'

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp bg-erp-bg">
      {/* Top Header Row for Detail */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <NavLink to={`/candidates${location.search}`} className="toolbar-btn shrink-0">
          <i className="ph ph-arrow-left text-erp-primary"></i> 返回列表
        </NavLink>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-lightning text-erp-danger"></i>
          {String(detail.name || symbol)} ({symbol})
        </span>
        <span className="px-1.5 py-0.5 bg-blue-50 text-blue-700 text-erp-sm border border-blue-200 rounded shrink-0">
          {String(params.model).toUpperCase()} / {String(params.split).toUpperCase()}
        </span>
        <span className="px-1.5 py-0.5 bg-gray-100 text-gray-600 text-erp-sm border border-gray-200 rounded shrink-0">
          序号 {currentPositionLabel}
        </span>
        
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        <button className="toolbar-btn shrink-0" onClick={() => navigate(buildWatchlistPath(symbol))}>
          <i className="ph ph-target"></i> 查看持仓
        </button>
        <button className="toolbar-btn shrink-0" onClick={() => navigate(buildAiReviewPath(symbol))}>
          <i className="ph ph-brain text-erp-primary"></i> AI 分析
        </button>

        <div className="ml-auto flex items-center gap-2 text-erp-sm shrink-0">
           <a href={`https://xueqiu.com/S/${symbol.replace('.', '')}`} target="_blank" rel="noreferrer" className="toolbar-btn text-erp-primary hover:underline">雪球</a>
           <a href={`https://quote.eastmoney.com/${symbol.replace('.', '').toLowerCase()}.html`} target="_blank" rel="noreferrer" className="toolbar-btn text-erp-primary hover:underline">东方财富</a>
        </div>
      </div>
      
      <div className="flex-1 overflow-y-auto bg-white flex flex-col p-6 gap-8 text-erp">
        <QueryNotice isLoading={detailQuery.isLoading || historyQuery.isLoading} error={detailQuery.error ?? historyQuery.error} />
        
        {/* Core Detail Grid - Large Flat Row */}
        <div className="flex items-center gap-12 shrink-0 border-b erp-border pb-6">
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">综合分数 / Score</span>
            <span className="text-3xl font-mono font-bold leading-none text-erp-primary">{String(detail.score ?? '-')}</span>
            <span className="text-[10px] text-gray-400 mt-1 uppercase font-bold">
              {formatDate(detail.trade_date)}
            </span>
          </div>
          <div className="w-px h-10 bg-gray-200"></div>
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">模型排名 / Rank</span>
            <span className="text-3xl font-mono font-bold leading-none text-gray-700">#{String(detail.rank ?? '-')}</span>
            <span className="text-[10px] text-gray-400 mt-1 uppercase font-bold">
              分位: {formatPercent(detail.rank_pct)}
            </span>
          </div>
          <div className="w-px h-10 bg-gray-200"></div>
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">预测 10D 收益</span>
            <span className={`text-3xl font-mono font-bold leading-none ${Number(detail.ret_t1_t10) > 0 ? 'text-erp-danger' : Number(detail.ret_t1_t10) < 0 ? 'text-erp-success' : ''}`}>
              {formatPercent(detail.ret_t1_t10)}
            </span>
          </div>
          
          <div className="flex-1 max-w-xl border-l erp-border pl-8 ml-4">
            <div className="text-erp-primary font-bold text-xs mb-1 uppercase flex items-center gap-1">
              <i className="ph-fill ph-chat-centered-text"></i> 操作建议摘要:
            </div>
            <div className="text-gray-600 text-sm leading-relaxed italic">
              "{String(detail.action_hint ?? '暂无建议')}"
            </div>
          </div>
        </div>

        {/* Detail Specs & Charts */}
        <div className="grid grid-cols-5 gap-12 shrink-0">
           {/* Left Specs */}
           <div className="col-span-2 flex flex-col gap-6">
              <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                <i className="ph ph-list-numbers"></i> 交易特征与技术背景 (Technical Context)
              </h4>
              <div className="bg-gray-50 rounded-lg p-6 erp-border border-dashed grid grid-cols-2 gap-6">
                 <div className="flex flex-col gap-1 border-b erp-border pb-3">
                    <span className="text-[10px] text-gray-400 uppercase font-bold">所属行业</span>
                    <span className="font-bold text-sm text-gray-700">{formatValue(detail.industry)}</span>
                 </div>
                 <div className="flex flex-col gap-1 border-b erp-border pb-3">
                    <span className="text-[10px] text-gray-400 uppercase font-bold">当日涨跌</span>
                    <span className={`font-mono font-bold ${Number(detail.pct_chg) > 0 ? 'text-erp-danger' : Number(detail.pct_chg) < 0 ? 'text-erp-success' : ''}`}>{formatPercent(detail.pct_chg)}</span>
                 </div>
                 <div className="flex flex-col gap-1">
                    <span className="text-[10px] text-gray-400 uppercase font-bold">20日动量 (Mom_20)</span>
                    <span className="font-mono font-bold text-gray-700">{formatPercent(detail.mom_20)}</span>
                 </div>
                 <div className="flex flex-col gap-1">
                    <span className="text-[10px] text-gray-400 uppercase font-bold">距20日线 (Close_to_ma20)</span>
                    <span className="font-mono font-bold text-gray-700">{formatPercent(detail.close_to_ma_20)}</span>
                 </div>
              </div>
           </div>

           {/* Right Chart */}
           <div className="col-span-3 flex flex-col gap-6">
              <div className="flex items-center justify-between">
                <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                  <i className="ph ph-chart-line-up"></i> 候选评分时序曲线 (Score History)
                </h4>
                {scoreViewOptions.length > 0 && (
                  <div className="flex items-center gap-1 bg-gray-100 p-1 rounded erp-border">
                    {scoreViewOptions.map((item) => (
                      <button
                        key={item.key}
                        onClick={() => setScoreView(item.key)}
                        className={`px-3 py-1 text-[11px] rounded transition-all ${activeScoreView?.key === item.key ? 'bg-white shadow-sm font-bold text-erp-primary border erp-border' : 'text-gray-500 hover:text-gray-700'}`}
                      >
                        {item.label}
                      </button>
                    ))}
                  </div>
                )}
              </div>
              <div className="erp-border rounded-lg p-4 h-[350px]">
                 <LineChartCard
                  data={scoreHistory}
                  xKey="trade_date"
                  lineKeys={activeScoreView?.lineKeys ?? scoreKeys}
                  title=""
                  emptyText="暂无评分数据"
                />
              </div>
           </div>
        </div>

      </div>
      
      {/* Pager Status Bar */}
      <div className="h-8 erp-border-t bg-gray-50 flex items-center px-2 justify-between text-gray-500 text-erp-sm shrink-0">
        <div className="flex items-center gap-4">
          <span>数据集来源: {String(params.model).toUpperCase()} / {String(params.split).toUpperCase()}</span>
        </div>
        <div className="flex items-center gap-2">
          <button 
            className="toolbar-btn" 
            disabled={!previousSymbol} 
            onClick={previousSymbol ? () => navigate({ pathname: buildCandidatesPath(previousSymbol), search: location.search }) : undefined}
          >
            <i className="ph ph-caret-left"></i> 上一只 ({previousSymbol || '-'})
          </button>
          <span>{currentPositionLabel}</span>
          <button 
            className="toolbar-btn"
            disabled={!nextSymbol}
            onClick={nextSymbol ? () => navigate({ pathname: buildCandidatesPath(nextSymbol), search: location.search }) : undefined}
          >
            下一只 ({nextSymbol || '-'}) <i className="ph ph-caret-right"></i>
          </button>
        </div>
      </div>
    </div>
  )
}
