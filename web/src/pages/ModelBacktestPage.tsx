import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiGet } from '../api/client'
import { ComparisonBoard } from '../components/ComparisonBoard'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { LineChartCard } from '../components/LineChartCard'
import { QueryNotice } from '../components/QueryNotice'
import { modelBacktestDiagnosticsClient, modelBacktestPageClient, modelBacktestPortfolioClient, modelBacktestSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatPercent, formatValue } from '../lib/format'
import type { BacktestDiagnosticsPayload, BacktestPortfolioPayload, BacktestSummaryPayload, BootstrapPayload, JsonRecord } from '../types/api'

interface ModelBacktestPageProps {
  bootstrap?: BootstrapPayload
}

const MONTHLY_COLUMNS = ['month', 'net_return']
const IMPORTANCE_COLUMNS = ['feature', 'importance_gain', 'importance_split']
const YEARLY_COLUMNS = ['year', 'annualized_return', 'sharpe', 'max_drawdown', 'win_rate', 'risk_on_ratio']
const REGIME_COLUMNS = ['regime', 'annualized_return', 'sharpe', 'max_drawdown', 'win_rate', 'risk_on_ratio']

const MONTHLY_COLUMN_LABELS = {
  month: '月份',
  net_return: '净收益',
}

const IMPORTANCE_COLUMN_LABELS = {
  feature: '特征',
  importance_gain: 'Gain 重要性',
  importance_split: 'Split 重要性',
}

const DIAGNOSTIC_COLUMN_LABELS = {
  year: '年份',
  regime: '阶段',
  annualized_return: '年化收益',
  sharpe: '夏普比率',
  max_drawdown: '最大回撤',
  win_rate: '胜率',
  risk_on_ratio: '风控开启比例',
}

const IMPORTANCE_PRESETS = [
  { key: 'gain', label: '增益', columns: ['feature', 'importance_gain'] },
  { key: 'full', label: '完整', columns: ['feature', 'importance_gain', 'importance_split'] },
]

const YEARLY_DIAGNOSTIC_PRESETS = [
  { key: 'performance', label: '绩效', columns: ['year', 'annualized_return', 'sharpe', 'max_drawdown'] },
  { key: 'execution', label: '执行', columns: ['year', 'win_rate', 'risk_on_ratio'] },
]

const REGIME_DIAGNOSTIC_PRESETS = [
  { key: 'performance', label: '绩效', columns: ['regime', 'annualized_return', 'sharpe', 'max_drawdown'] },
  { key: 'execution', label: '执行', columns: ['regime', 'win_rate', 'risk_on_ratio'] },
]

const PORTFOLIO_VIEW_SPECS = [
  { key: 'equity', label: '净值曲线', lineKeys: ['equity_curve'], subtitle: '累计资金曲线' },
  { key: 'period', label: '区间收益', lineKeys: ['net_return', 'benchmark_proxy_period_return'], subtitle: '组合与基准对比' },
  { key: 'filtered', label: '风控前后', lineKeys: ['net_return', 'net_return_unfiltered'], subtitle: '风控前后收益对比' },
]

function buildChartViews(rows: JsonRecord[], specs: Array<{ key: string; label: string; lineKeys: string[]; subtitle: string }>) {
  const availableKeys = new Set(Object.keys(rows[0] ?? {}))
  return specs.filter((spec) => spec.lineKeys.every((key) => availableKeys.has(key)))
}

export function ModelBacktestPage({ bootstrap }: ModelBacktestPageProps) {
  const { params, updateParams } = usePageSearchState(modelBacktestPageClient)
  const [portfolioView, setPortfolioView] = useState('equity')

  const backtestSummaryQuery = useQuery({
    queryKey: modelBacktestSummaryClient.queryKey(params),
    queryFn: () => apiGet<BacktestSummaryPayload>(modelBacktestSummaryClient.path(params)),
  })

  const backtestPortfolioQuery = useQuery({
    queryKey: modelBacktestPortfolioClient.queryKey(params),
    queryFn: () => apiGet<BacktestPortfolioPayload>(modelBacktestPortfolioClient.path(params)),
  })

  const backtestDiagnosticsQuery = useQuery({
    queryKey: modelBacktestDiagnosticsClient.queryKey(params),
    queryFn: () => apiGet<BacktestDiagnosticsPayload>(modelBacktestDiagnosticsClient.path(params)),
  })

  const metrics = backtestSummaryQuery.data?.metrics ?? {}
  const stability = useMemo(() => backtestSummaryQuery.data?.stability ?? {}, [backtestSummaryQuery.data?.stability])
  const portfolioRows = useMemo(() => backtestPortfolioQuery.data?.portfolio ?? [], [backtestPortfolioQuery.data?.portfolio])
  const stabilityTone = String(stability.grade ?? '').includes('稳') ? 'good' : 'warn'
  const portfolioViews = useMemo(() => buildChartViews(portfolioRows, PORTFOLIO_VIEW_SPECS), [portfolioRows])
  const activePortfolioView = portfolioViews.find((item) => item.key === portfolioView) ?? portfolioViews[0]
  const stabilityComparisonColumns = useMemo(
    () => [
      { key: 'valid', label: '验证集', description: '看策略有没有先跑顺' },
      { key: 'test', label: '测试集', description: '看样本外能不能扛住' },
    ],
    [],
  )
  const stabilityComparisonRows = useMemo(
    () => [
      {
        key: 'return',
        label: '累计收益',
        helper: '先比较收益是否同步为正',
        values: {
          valid: formatPercent(stability.valid_return),
          test: formatPercent(stability.test_return),
        },
      },
      {
        key: 'drawdown',
        label: '最大回撤',
        helper: '再确认回撤有没有明显放大',
        values: {
          valid: formatPercent(stability.valid_drawdown),
          test: formatPercent(stability.test_drawdown),
        },
      },
      {
        key: 'rank-ic',
        label: 'RankIC',
        values: {
          valid: formatValue(stability.valid_rank_ic),
          test: formatValue(stability.test_rank_ic),
        },
      },
      {
        key: 'turnover',
        label: '换手率',
        values: {
          valid: formatPercent(stability.valid_turnover),
          test: formatPercent(stability.test_turnover),
        },
      },
    ],
    [stability],
  )
  const backtestCellRenderers = useMemo(
    () => ({
      month: (row: JsonRecord) => <EntityCell title={String(row.month ?? '-')} subtitle="月度节奏" />,
      feature: (row: JsonRecord) => (
        <EntityCell
          title={String(row.feature ?? '-')}
          subtitle="特征贡献"
          meta={`Gain ${formatValue(row.importance_gain)} / Split ${formatValue(row.importance_split)}`}
        />
      ),
      year: (row: JsonRecord) => (
        <EntityCell
          title={String(row.year ?? '-')}
          subtitle="年度诊断"
          meta={`年化 ${formatPercent(row.annualized_return)} / 回撤 ${formatPercent(row.max_drawdown)}`}
        />
      ),
      regime: (row: JsonRecord) => (
        <EntityCell
          title={String(row.regime ?? '-')}
          subtitle="阶段诊断"
          meta={`年化 ${formatPercent(row.annualized_return)} / 风控开启 ${formatPercent(row.risk_on_ratio)}`}
        />
      ),
    }),
    [],
  )

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-clock-counter-clockwise text-erp-primary"></i> 
          模型回测分析
        </span>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        {/* Quick Filters in Toolbar */}
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">模型:</span>
          <select 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary bg-white font-medium"
            value={params.model} 
            onChange={(event) => updateParams({ model: event.target.value })}
          >
            {(bootstrap?.modelNames ?? ['ridge', 'lgbm', 'ensemble']).map((model) => (
              <option key={model} value={model}>{bootstrap?.modelLabels?.[model] ?? model}</option>
            ))}
          </select>
        </div>
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">分段:</span>
          <select 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary bg-white font-medium"
            value={params.split} 
            onChange={(event) => updateParams({ split: event.target.value })}
          >
            {(bootstrap?.splitNames ?? ['valid', 'test']).map((split) => (
              <option key={split} value={split}>{bootstrap?.splitLabels?.[split] ?? split}</option>
            ))}
          </select>
        </div>

        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>

        <div className="flex items-center gap-3 text-erp-sm shrink-0">
           <div className="flex items-center gap-1">
             <span className="text-gray-500 text-[11px]">稳定性:</span>
             <span className={`font-bold ${stabilityTone === 'good' ? 'text-erp-success' : 'text-erp-warning'}`}>
               {String(stability.grade ?? 'PENDING')}
             </span>
           </div>
           <div className="flex items-center gap-1">
             <span className="text-gray-500 text-[11px]">检查:</span>
             <span className="font-bold">
               {String(stability.passed_checks)} / {String(stability.total_checks)}
             </span>
           </div>
        </div>
        
        <div className="ml-auto flex items-center gap-4 text-erp-sm shrink-0">
          <div className="flex items-center gap-1">
            <span className="text-gray-500">年化收益:</span> 
            <span className={`font-bold font-mono ${Number(metrics.daily_portfolio_annualized_return) > 0 ? 'text-erp-danger' : 'text-erp-success'}`}>
              {formatPercent(metrics.daily_portfolio_annualized_return)}
            </span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">最大回撤:</span> 
            <span className="font-bold font-mono text-erp-success">
              {formatPercent(metrics.daily_portfolio_max_drawdown)}
            </span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">夏普:</span> 
            <span className="font-bold font-mono">
              {String(metrics.daily_portfolio_sharpe ?? '-')}
            </span>
          </div>
        </div>
      </div>

      {/* Main Content Area: High Density Grid & Charts */}
      <div className="flex-1 bg-white p-8 overflow-y-auto flex flex-col gap-12">
        <QueryNotice isLoading={backtestSummaryQuery.isLoading || backtestPortfolioQuery.isLoading || backtestDiagnosticsQuery.isLoading} error={backtestSummaryQuery.error ?? backtestPortfolioQuery.error ?? backtestDiagnosticsQuery.error} />

        {/* Top Analysis Conclusion - Major Metric Row */}
        <div className="flex items-center gap-12 shrink-0 border erp-border bg-erp-surface rounded-xl p-6 shadow-sm mb-4">
           <div className="flex flex-col gap-1 w-[280px]">
              <div className="text-erp-muted text-[10px] uppercase font-bold tracking-widest mb-1">回测诊断结论 / Stability Grade</div>
              <div className={`text-3xl font-bold leading-none ${stabilityTone === 'good' ? 'text-erp-success' : 'text-erp-warning'}`}>
                {String(stability.conclusion ?? '评估中...')}
              </div>
           </div>
           <div className="w-px h-12 bg-erp-border"></div>
           <div className="grid grid-cols-4 gap-12 flex-1">
              <div className="flex flex-col">
                 <span className="text-erp-muted text-[10px] uppercase font-bold">年化收益</span>
                 <span className={`text-2xl font-mono font-bold leading-none ${Number(metrics.daily_portfolio_annualized_return) > 0 ? 'text-erp-danger' : 'text-erp-success'}`}>{formatPercent(metrics.daily_portfolio_annualized_return)}</span>
              </div>
              <div className="flex flex-col">
                 <span className="text-erp-muted text-[10px] uppercase font-bold">最大回撤</span>
                 <span className="text-2xl font-mono font-bold leading-none text-erp-success">{formatPercent(metrics.daily_portfolio_max_drawdown)}</span>
              </div>
              <div className="flex flex-col">
                 <span className="text-erp-muted text-[10px] uppercase font-bold">组合夏普</span>
                 <span className="text-2xl font-mono font-bold leading-none text-erp-text">{String(metrics.daily_portfolio_sharpe ?? '-')}</span>
              </div>
              <div className="flex flex-col">
                 <span className="text-erp-muted text-[10px] uppercase font-bold">TopN 命中率</span>
                 <span className="text-2xl font-mono font-bold leading-none text-erp-text">{formatPercent(metrics.top_n_hit_rate)}</span>
              </div>
           </div>
        </div>

        {/* Consistency Check & Equity Curve Row */}
        <div className="grid grid-cols-5 gap-12 shrink-0">
           <div className="col-span-2 flex flex-col gap-6">
              <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                <i className="ph ph-check-square"></i> 样本内外一致性核查 (Consistency)
              </h4>
              <div className="bg-gray-50 rounded-lg p-6 erp-border border-dashed">
                 <ComparisonBoard columns={stabilityComparisonColumns} rows={stabilityComparisonRows} />
              </div>
           </div>
           <div className="col-span-3 flex flex-col gap-6">
              <div className="flex items-center justify-between">
                <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                  <i className="ph ph-chart-line-up"></i> 组合净值曲线 (Equity Curve)
                </h4>
                {portfolioViews.length > 1 && (
                  <div className="flex items-center gap-1 bg-gray-100 p-1 rounded erp-border">
                    {portfolioViews.map((item) => (
                      <button
                        key={item.key}
                        onClick={() => setPortfolioView(item.key)}
                        className={`px-3 py-1 text-[11px] rounded transition-all ${portfolioView === item.key ? 'bg-white shadow-sm font-bold text-erp-primary border erp-border' : 'text-gray-500 hover:text-gray-700'}`}
                      >
                        {item.label}
                      </button>
                    ))}
                  </div>
                )}
              </div>
              <div className="erp-border rounded-lg p-4 h-[350px]">
                 <LineChartCard data={portfolioRows} xKey="trade_date" lineKeys={activePortfolioView?.lineKeys} title="" emptyText="暂无数据" />
              </div>
           </div>
        </div>

        {/* Multi-Report Grid Section */}
        <section className="flex flex-col gap-10 border-t erp-border pt-10">
           <h4 className="text-gray-400 font-bold text-xs uppercase tracking-widest mb-2 flex items-center gap-2">
             <i className="ph ph-presentation"></i> 策略绩效多维诊断报告 (Diagnostics)
           </h4>
           
           <div className="grid grid-cols-2 gap-12">
              <div className="flex flex-col gap-4">
                 <div className="text-sm font-bold text-gray-700">月度收益明细 (Monthly)</div>
                 <div className="erp-border rounded-lg overflow-hidden h-[300px]">
                    <DataTable rows={backtestPortfolioQuery.data?.monthlySummary ?? []} columns={MONTHLY_COLUMNS} storageKey="backtest-monthly" columnLabels={MONTHLY_COLUMN_LABELS} emptyText="暂无数据" cellRenderers={backtestCellRenderers} />
                 </div>
              </div>
              <div className="flex flex-col gap-4">
                 <div className="text-sm font-bold text-gray-700">特征重要性贡献 (Features)</div>
                 <div className="erp-border rounded-lg overflow-hidden h-[300px]">
                    <DataTable rows={backtestDiagnosticsQuery.data?.importance ?? []} columns={IMPORTANCE_COLUMNS} storageKey="backtest-importance" viewPresets={IMPORTANCE_PRESETS} defaultPresetKey="gain" columnLabels={IMPORTANCE_COLUMN_LABELS} emptyText="暂无数据" cellRenderers={backtestCellRenderers} />
                 </div>
              </div>
              <div className="flex flex-col gap-4">
                 <div className="text-sm font-bold text-gray-700">年度绩效统计 (Yearly)</div>
                 <div className="erp-border rounded-lg overflow-hidden h-[300px]">
                    <DataTable rows={backtestDiagnosticsQuery.data?.yearlyDiagnostics ?? []} columns={YEARLY_COLUMNS} storageKey="backtest-yearly" viewPresets={YEARLY_DIAGNOSTIC_PRESETS} defaultPresetKey="performance" columnLabels={DIAGNOSTIC_COLUMN_LABELS} emptyText="暂无数据" cellRenderers={backtestCellRenderers} />
                 </div>
              </div>
              <div className="flex flex-col gap-4">
                 <div className="text-sm font-bold text-gray-700">市场状态阶段统计 (Regime)</div>
                 <div className="erp-border rounded-lg overflow-hidden h-[300px]">
                    <DataTable rows={backtestDiagnosticsQuery.data?.regimeDiagnostics ?? []} columns={REGIME_COLUMNS} storageKey="backtest-regime" viewPresets={REGIME_DIAGNOSTIC_PRESETS} defaultPresetKey="performance" columnLabels={DIAGNOSTIC_COLUMN_LABELS} emptyText="暂无数据" cellRenderers={backtestCellRenderers} />
                 </div>
              </div>
           </div>
        </section>

        {/* Execution Summary Section */}
        <section className="bg-gray-50 rounded-xl p-8 erp-border flex flex-col gap-6 mb-8">
           <h4 className="text-gray-700 font-bold text-sm uppercase flex items-center gap-2">
             <i className="ph ph-activity"></i> 策略执行与风控细节摘要
           </h4>
           <div className="grid grid-cols-4 gap-8">
              <div className="flex flex-col border-r erp-border">
                 <span className="text-[10px] text-gray-400 uppercase mb-1">趋势过滤生效率</span>
                 <span className="text-xl font-mono font-bold text-erp-primary">{formatPercent(metrics.risk_filter_active_ratio)}</span>
                 <span className="text-[10px] text-gray-400 mt-1 uppercase">被过滤 {formatValue(metrics.risk_filter_filtered_periods)} 期</span>
              </div>
              <div className="flex flex-col border-r erp-border">
                 <span className="text-[10px] text-gray-400 uppercase mb-1">平均持仓/样本</span>
                 <span className="text-xl font-mono font-bold">{formatValue(metrics.avg_selected_count)}</span>
                 <span className="text-[10px] text-gray-400 mt-1 uppercase">总观测 {formatValue(metrics.observations)} 条</span>
              </div>
              <div className="flex flex-col border-r erp-border">
                 <span className="text-[10px] text-gray-400 uppercase mb-1">最大换手率</span>
                 <span className="text-xl font-mono font-bold text-erp-warning">{formatPercent(metrics.max_turnover_ratio)}</span>
                 <span className="text-[10px] text-gray-400 mt-1 uppercase">平均换手 {formatPercent(metrics.avg_turnover_ratio)}</span>
              </div>
              <div className="flex flex-col">
                 <span className="text-[10px] text-gray-400 uppercase mb-1">持有周期 (预期)</span>
                 <span className="text-xl font-mono font-bold">{formatValue(metrics.holding_period_days)} 天</span>
                 <span className="text-[10px] text-gray-400 mt-1 uppercase">交易断面 {formatValue(metrics.dates)} 个</span>
              </div>
           </div>
        </section>
      </div>
    </div>
  )
}
