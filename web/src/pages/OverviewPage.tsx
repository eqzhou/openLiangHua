import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiGet } from '../api/client'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { LineChartCard } from '../components/LineChartCard'
import { QueryNotice } from '../components/QueryNotice'
import { overviewCurvesClient, overviewPageClient, overviewSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatPercent, formatValue } from '../lib/format'
import type { BootstrapPayload, JsonRecord, OverviewCurvesPayload, OverviewSummaryPayload } from '../types/api'

interface OverviewPageProps {
  bootstrap?: BootstrapPayload
}

const COMPARISON_COLUMNS = [
  'model',
  'rank_ic_mean',
  'top_n_hit_rate',
  'top_n_forward_mean',
  'daily_portfolio_annualized_return',
  'daily_portfolio_sharpe',
  'daily_portfolio_max_drawdown',
  'avg_turnover_ratio',
]

const COMPARISON_COLUMN_LABELS = {
  model: '模型',
  rank_ic_mean: 'RankIC',
  top_n_hit_rate: 'TopN 命中率',
  top_n_forward_mean: 'TopN 未来收益',
  daily_portfolio_annualized_return: '组合年化',
  daily_portfolio_sharpe: '组合夏普',
  daily_portfolio_max_drawdown: '最大回撤',
  avg_turnover_ratio: '平均换手',
}

function findBestRow(rows: JsonRecord[], field: string, mode: 'max' | 'min' = 'max'): JsonRecord | undefined {
  return rows
    .filter((row) => typeof row[field] === 'number' && Number.isFinite(Number(row[field])))
    .sort((left, right) => {
      const a = Number(left[field])
      const b = Number(right[field])
      return mode === 'max' ? b - a : a - b
    })[0]
}

function buildCurveViewOptions(keys: string[]) {
  if (!keys.length) {
    return []
  }

  const singleSeries = keys.map((key) => ({
    key: `single:${key}`,
    label: key,
    lineKeys: [key],
    subtitle: `只看 ${key} 这一条资金曲线。`,
  }))

  if (keys.length === 1) {
    return singleSeries
  }

  return [
    {
      key: 'all',
      label: '全部模型',
      lineKeys: keys,
      subtitle: '横向比较所有模型在同一分段下的净值轨迹。',
    },
    ...singleSeries,
  ]
}

export function OverviewPage({ bootstrap }: OverviewPageProps) {
  const { params, updateParams } = usePageSearchState(overviewPageClient)
  const [curveView, setCurveView] = useState('all')

  const overviewQuery = useQuery({
    queryKey: overviewSummaryClient.queryKey(params),
    queryFn: () => apiGet<OverviewSummaryPayload>(overviewSummaryClient.path(params)),
  })

  const curvesQuery = useQuery({
    queryKey: overviewCurvesClient.queryKey(params),
    queryFn: () => apiGet<OverviewCurvesPayload>(overviewCurvesClient.path(params)),
  })

  const summary = overviewQuery.data?.summary ?? {}
  const comparison = useMemo(() => overviewQuery.data?.comparison ?? [], [overviewQuery.data?.comparison])
  const equityCurves = useMemo(() => curvesQuery.data?.equityCurves ?? [], [curvesQuery.data?.equityCurves])
  const bestAnnualized = useMemo(() => findBestRow(comparison, 'daily_portfolio_annualized_return', 'max'), [comparison])
  const bestSharpe = useMemo(() => findBestRow(comparison, 'daily_portfolio_sharpe', 'max'), [comparison])

  const dailyBarState = summary.daily_bar as Record<string, unknown> | undefined
  const featuresState = summary.features as Record<string, unknown> | undefined

  const curveKeys = useMemo(() => Object.keys(equityCurves[0] ?? {}).filter((key) => key !== 'trade_date'), [equityCurves])
  const curveViewOptions = useMemo(() => buildCurveViewOptions(curveKeys), [curveKeys])
  const activeCurveView = curveViewOptions.find((option) => option.key === curveView) ?? curveViewOptions[0]

  const comparisonCellRenderers = useMemo(
    () => ({
      model: (row: JsonRecord) => (
        <EntityCell
          title={String(row.model ?? '-').toUpperCase()}
          subtitle={String(row.split ?? '').toUpperCase()}
          meta={`年化 ${formatPercent(row.daily_portfolio_annualized_return)} / 夏普 ${formatValue(row.daily_portfolio_sharpe)}`}
          badges={[
            String(row.split ?? '') === String(overviewQuery.data?.selectedSplit ?? params.split)
              ? { label: '当前分段', tone: 'brand' as const }
              : null,
          ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
        />
      ),
    }),
    [overviewQuery.data?.selectedSplit, params.split],
  )

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-chart-line-up text-erp-primary"></i> 
          研究概览
        </span>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        {/* Quick Filters in Toolbar */}
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">数据集:</span>
          <select 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary bg-white font-medium"
            value={params.split} 
            onChange={(event) => updateParams({ split: event.target.value })}
          >
            {(bootstrap?.splitNames ?? ['valid', 'test']).map((split) => (
              <option key={split} value={split}>
                {(bootstrap?.splitLabels?.[split] ?? split).toUpperCase()}
              </option>
            ))}
          </select>
        </div>

        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>

        {/* State Indicators */}
        <div className="flex items-center gap-3 text-erp-sm shrink-0">
           <div className="flex items-center gap-1">
             <span className="text-gray-500 text-[11px]">日线:</span>
             <span className={`font-bold ${dailyBarState?.exists ? 'text-erp-success' : 'text-erp-danger'}`}>
               {dailyBarState?.exists ? 'READY' : 'MISSING'}
             </span>
           </div>
           <div className="flex items-center gap-1">
             <span className="text-gray-500 text-[11px]">特征:</span>
             <span className={`font-bold ${featuresState?.exists ? 'text-erp-success' : 'text-erp-danger'}`}>
               {featuresState?.exists ? 'READY' : 'MISSING'}
             </span>
           </div>
        </div>
        
        <div className="ml-auto flex items-center gap-4 text-erp-sm shrink-0">
          <div className="flex items-center gap-1">
            <span className="text-gray-500">研究区间:</span> 
            <span className="font-bold font-mono">
              {summary.date_min && summary.date_max ? `${String(summary.date_min)} ~ ${String(summary.date_max)}` : '-'}
            </span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">最佳夏普:</span> 
            <span className="font-bold font-mono text-erp-success">
              {bestSharpe?.model ? String(bestSharpe.model).toUpperCase() : '-'} ({formatValue(bestSharpe?.daily_portfolio_sharpe)})
            </span>
          </div>
        </div>
      </div>

      {/* Main Content Area: High Density Grid & Charts */}
      <div className="flex-1 bg-white p-8 overflow-y-auto flex flex-col gap-12">
        <QueryNotice isLoading={overviewQuery.isLoading || curvesQuery.isLoading} error={overviewQuery.error ?? curvesQuery.error} />

        {/* Top Summary Row - Large Numbers */}
        <div className="flex items-center gap-16 shrink-0 border-b erp-border pb-8">
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">特征样本总数</span>
            <span className="text-4xl font-mono font-bold leading-none">{formatValue(summary.feature_rows ?? 0)}</span>
          </div>
          <div className="w-px h-10 bg-gray-200"></div>
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">股票覆盖数</span>
            <span className="text-4xl font-mono font-bold leading-none">{formatValue(summary.feature_symbols ?? 0)}</span>
          </div>
          <div className="w-px h-10 bg-gray-200"></div>
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">最佳年化收益</span>
            <span className="text-4xl font-mono font-bold leading-none text-erp-danger">{formatPercent(bestAnnualized?.daily_portfolio_annualized_return)}</span>
            <span className="text-[10px] text-gray-400 mt-2 uppercase font-bold">{String(bestAnnualized?.model ?? '-')}</span>
          </div>
          <div className="w-px h-10 bg-gray-200"></div>
          <div className="flex flex-col">
            <span className="text-gray-400 text-[10px] uppercase font-bold tracking-widest mb-1">平均换手率</span>
            <span className="text-4xl font-mono font-bold leading-none text-gray-700">{formatPercent(summary.avg_turnover_ratio)}</span>
          </div>
        </div>

        {/* Comparison Grid Section */}
        <section className="flex flex-col gap-6">
           <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
             <i className="ph ph-table"></i> 多模型绩效横向对账表 (Comparative Analytics)
           </h4>
           <div className="erp-border rounded-lg overflow-hidden min-h-[300px]">
              <DataTable
                rows={comparison}
                columns={COMPARISON_COLUMNS}
                columnLabels={COMPARISON_COLUMN_LABELS}
                storageKey="overview-comparison"
                loading={overviewQuery.isLoading}
                emptyText="暂无模型结果"
                stickyFirstColumn
                cellRenderers={comparisonCellRenderers}
              />
           </div>
        </section>

        {/* Equity Curves Section */}
        <section className="flex flex-col gap-6 border-t erp-border pt-10">
           <div className="flex items-center justify-between">
              <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                <i className="ph ph-chart-line"></i> 模型净值曲线对比 (Cumulative Equity)
              </h4>
              {curveViewOptions.length > 1 && (
                <div className="flex items-center gap-1 bg-gray-100 p-1 rounded-md erp-border">
                  {curveViewOptions.map((option) => (
                    <button
                      key={option.key}
                      onClick={() => setCurveView(option.key)}
                      className={`px-3 py-1 text-[11px] rounded transition-all ${activeCurveView.key === option.key ? 'bg-white shadow-sm font-bold text-erp-primary border erp-border' : 'text-gray-500 hover:text-gray-700'}`}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              )}
           </div>
           <div className="erp-border rounded-lg p-6 bg-gray-50/30 h-[500px]">
             <LineChartCard
                data={equityCurves}
                xKey="trade_date"
                lineKeys={activeCurveView?.lineKeys}
                title=""
                emptyText="暂无净值曲线"
              />
           </div>
        </section>
      </div>
    </div>
  )
}
