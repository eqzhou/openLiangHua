import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiGet } from '../api/client'
import { ComparisonBoard } from '../components/ComparisonBoard'
import { ContextStrip } from '../components/ContextStrip'
import { ControlField } from '../components/ControlField'
import { ControlGrid } from '../components/ControlGrid'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { InsightList } from '../components/InsightList'
import { LineChartCard } from '../components/LineChartCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SegmentedControl } from '../components/SegmentedControl'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { modelBacktestPageClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatPercent, formatValue } from '../lib/format'
import type { BacktestPayload, BootstrapPayload, JsonRecord } from '../types/api'

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

const CHECK_LABELS: Record<string, string> = {
  valid_positive: '验证集收益为正',
  test_positive: '测试集收益为正',
  valid_drawdown_ok: '验证集回撤可控',
  test_drawdown_ok: '测试集回撤可控',
  valid_rank_ic_positive: '验证集 RankIC 为正',
  test_rank_ic_positive: '测试集 RankIC 为正',
  turnover_ok: '换手率在可接受范围',
  return_gap_ok: '样本外收益落差可接受',
}

const IMPORTANCE_PRESETS = [
  { key: 'gain', label: 'Gain 视图', columns: ['feature', 'importance_gain'] },
  { key: 'full', label: '完整视图', columns: ['feature', 'importance_gain', 'importance_split'] },
]

const YEARLY_DIAGNOSTIC_PRESETS = [
  { key: 'performance', label: '绩效视图', columns: ['year', 'annualized_return', 'sharpe', 'max_drawdown'] },
  { key: 'execution', label: '执行视图', columns: ['year', 'win_rate', 'risk_on_ratio'] },
]

const REGIME_DIAGNOSTIC_PRESETS = [
  { key: 'performance', label: '绩效视图', columns: ['regime', 'annualized_return', 'sharpe', 'max_drawdown'] },
  { key: 'execution', label: '执行视图', columns: ['regime', 'win_rate', 'risk_on_ratio'] },
]

const PORTFOLIO_VIEW_SPECS = [
  { key: 'equity', label: '净值曲线', lineKeys: ['equity_curve'], subtitle: '先看累计资金曲线，确认整体趋势和回撤位置。' },
  { key: 'period', label: '区间收益', lineKeys: ['net_return', 'benchmark_proxy_period_return'], subtitle: '比较组合和基准在每个持有周期的单期收益。' },
  { key: 'filtered', label: '风控前后', lineKeys: ['net_return', 'net_return_unfiltered'], subtitle: '比较风控过滤前后的收益差异。' },
]

const EXECUTION_VIEW_SPECS = [
  { key: 'selection', label: '持仓节奏', lineKeys: ['selected_count', 'overlap_count', 'retained_count'], subtitle: '观察每期入选数量、保留数量和重叠数量的节奏。' },
  { key: 'turnover', label: '换手节奏', lineKeys: ['turnover_ratio', 'holding_period_days'], subtitle: '确认换手和持有期是否稳定。' },
  { key: 'benchmark', label: '基准状态', lineKeys: ['benchmark_proxy_return', 'benchmark_proxy_period_return'], subtitle: '比较基准单期收益和持有期收益代理。' },
]

function buildChartViews(rows: JsonRecord[], specs: Array<{ key: string; label: string; lineKeys: string[]; subtitle: string }>) {
  const availableKeys = new Set(Object.keys(rows[0] ?? {}))
  return specs.filter((spec) => spec.lineKeys.every((key) => availableKeys.has(key)))
}

export function ModelBacktestPage({ bootstrap }: ModelBacktestPageProps) {
  const { params, updateParams } = usePageSearchState(modelBacktestPageClient)
  const [portfolioView, setPortfolioView] = useState('equity')
  const [executionView, setExecutionView] = useState('selection')

  const backtestQuery = useQuery({
    queryKey: modelBacktestPageClient.queryKey(params),
    queryFn: () => apiGet<BacktestPayload>(modelBacktestPageClient.path(params)),
  })

  const metrics = backtestQuery.data?.metrics ?? {}
  const stability = useMemo(() => backtestQuery.data?.stability ?? {}, [backtestQuery.data?.stability])
  const portfolioRows = useMemo(() => backtestQuery.data?.portfolio ?? [], [backtestQuery.data?.portfolio])
  const checks = useMemo(() => (stability.checks && typeof stability.checks === 'object' ? stability.checks : {}), [stability.checks])
  const passedChecks = useMemo(
    () => Object.entries(checks).filter(([, passed]) => passed).map(([key]) => CHECK_LABELS[key] ?? key),
    [checks],
  )
  const failedChecks = useMemo(
    () => Object.entries(checks).filter(([, passed]) => !passed).map(([key]) => CHECK_LABELS[key] ?? key),
    [checks],
  )
  const stabilityTone = String(stability.grade ?? '').includes('稳') ? 'good' : 'warn'
  const portfolioViews = useMemo(() => buildChartViews(portfolioRows, PORTFOLIO_VIEW_SPECS), [portfolioRows])
  const executionViews = useMemo(() => buildChartViews(portfolioRows, EXECUTION_VIEW_SPECS), [portfolioRows])
  const activePortfolioView = portfolioViews.find((item) => item.key === portfolioView) ?? portfolioViews[0]
  const activeExecutionView = executionViews.find((item) => item.key === executionView) ?? executionViews[0]
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
  const backtestContextItems = useMemo(
    () => [
      {
        label: '结果模型',
        value: String(backtestQuery.data?.modelName ?? params.model).toUpperCase(),
        tone: 'brand' as const,
      },
      {
        label: '样本分段',
        value: String(backtestQuery.data?.splitName ?? params.split).toUpperCase(),
      },
      {
        label: '稳定性等级',
        value: stability.grade ?? '-',
        helper: String(stability.conclusion ?? ''),
        tone: stabilityTone === 'good' ? ('good' as const) : ('warn' as const),
      },
      {
        label: '检查通过',
        value: `${formatValue(stability.passed_checks)} / ${formatValue(stability.total_checks)}`,
      },
      {
        label: '持有周期',
        value: metrics.holding_period_days ?? '-',
        helper: '交易日',
      },
    ],
    [backtestQuery.data?.modelName, backtestQuery.data?.splitName, metrics.holding_period_days, params.model, params.split, stability.conclusion, stability.grade, stability.passed_checks, stability.total_checks, stabilityTone],
  )

  return (
    <div className="page-stack">
      <Panel
        title="模型回测"
        subtitle="先看结论，再看曲线和诊断。"
        tone="warm"
        className="panel--summary-surface"
      >
        <QueryNotice isLoading={backtestQuery.isLoading} error={backtestQuery.error} />

        <SectionBlock title="先看回测结论" description="先判断值不值得继续看。" tone="emphasis">
          <SpotlightCard
            title={String(backtestQuery.data?.modelName ?? params.model).toUpperCase()}
            meta={String(backtestQuery.data?.splitName ?? params.split).toUpperCase()}
            subtitle={String(stability.conclusion ?? '暂无稳定性结论。')}
            badges={[
              { label: `稳定性 ${String(stability.grade ?? '待评估')}`, tone: stabilityTone },
              { label: `持有期 ${formatValue(metrics.holding_period_days)} 天`, tone: 'brand' },
              { label: `${formatValue(stability.passed_checks)} / ${formatValue(stability.total_checks)} 项通过` },
            ]}
            metrics={[
              {
                label: '年化收益',
                value: formatPercent(metrics.daily_portfolio_annualized_return),
                tone:
                  typeof metrics.daily_portfolio_annualized_return === 'number' && Number(metrics.daily_portfolio_annualized_return) > 0
                    ? 'good'
                    : 'warn',
              },
              { label: '夏普比率', value: metrics.daily_portfolio_sharpe ?? '-' },
              { label: '最大回撤', value: formatPercent(metrics.daily_portfolio_max_drawdown), tone: 'warn' },
              { label: '命中率', value: formatPercent(metrics.top_n_hit_rate) },
              {
                label: 'TopN 未来收益',
                value: formatPercent(metrics.top_n_forward_mean),
                tone: typeof metrics.top_n_forward_mean === 'number' && Number(metrics.top_n_forward_mean) > 0 ? 'good' : 'warn',
              },
            ]}
          />
        </SectionBlock>

        <ContextStrip items={backtestContextItems} />

        <PageFilterBar
          title="切换当前回测视角"
          description="模型和分段共用同一口径。"
        >
          <ControlGrid variant="double">
            <ControlField label="模型">
              <select value={params.model} onChange={(event) => updateParams({ model: event.target.value })}>
                {(bootstrap?.modelNames ?? ['ridge', 'lgbm', 'ensemble']).map((model) => (
                  <option key={model} value={model}>
                    {bootstrap?.modelLabels?.[model] ?? model}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="数据集">
              <select value={params.split} onChange={(event) => updateParams({ split: event.target.value })}>
                {(bootstrap?.splitNames ?? ['valid', 'test']).map((split) => (
                  <option key={split} value={split}>
                    {bootstrap?.splitLabels?.[split] ?? split}
                  </option>
                ))}
              </select>
            </ControlField>
          </ControlGrid>
        </PageFilterBar>
      </Panel>

      <Panel title="验证 / 测试对比" subtitle="先看样本内外差异。" tone="calm" className="panel--summary-surface">
        <ComparisonBoard columns={stabilityComparisonColumns} rows={stabilityComparisonRows} />
      </Panel>

      <div className="split-layout">
        <Panel title="组合曲线区" subtitle="先看净值，再切收益/风控。" tone="calm" className="panel--table-surface">
          <LineChartCard
            data={portfolioRows}
            xKey="trade_date"
            lineKeys={activePortfolioView?.lineKeys}
            title="组合曲线"
            subtitle={activePortfolioView?.subtitle ?? '查看组合净值和区间收益。'}
            actions={
              activePortfolioView ? (
                <SegmentedControl
                  label="切换组合曲线视图"
                  value={activePortfolioView.key}
                  options={portfolioViews.map((item) => ({ key: item.key, label: item.label }))}
                  onChange={setPortfolioView}
                />
              ) : null
            }
            emptyText="暂无组合曲线"
          />
        </Panel>

        <Panel title="执行节奏区" subtitle="看换手、持仓和基准节奏。" tone="calm" className="panel--table-surface">
          <LineChartCard
            data={portfolioRows}
            xKey="trade_date"
            lineKeys={activeExecutionView?.lineKeys}
            title="执行与市场节奏"
            subtitle={activeExecutionView?.subtitle ?? '查看执行节奏和基准状态。'}
            actions={
              activeExecutionView ? (
                <SegmentedControl
                  label="切换执行节奏视图"
                  value={activeExecutionView.key}
                  options={executionViews.map((item) => ({ key: item.key, label: item.label }))}
                  onChange={setExecutionView}
                />
              ) : null
            }
            emptyText="暂无执行节奏曲线"
          />
        </Panel>
      </div>

      <div className="split-layout">
        <Panel title="按月收益" subtitle="只保留月度节奏。" tone="calm" className="panel--table-surface">
          <DataTable rows={backtestQuery.data?.monthlySummary ?? []} columns={MONTHLY_COLUMNS} storageKey="backtest-monthly" columnLabels={MONTHLY_COLUMN_LABELS} emptyText="暂无月度汇总" cellRenderers={backtestCellRenderers} />
        </Panel>
        <Panel title="核心因子贡献" subtitle="只看特征前排。" tone="calm" className="panel--table-surface">
          <DataTable
            rows={backtestQuery.data?.importance ?? []}
            columns={IMPORTANCE_COLUMNS}
            storageKey="backtest-importance"
            viewPresets={IMPORTANCE_PRESETS}
            defaultPresetKey="gain"
            columnLabels={IMPORTANCE_COLUMN_LABELS}
            emptyText="暂无特征重要性"
            cellRenderers={backtestCellRenderers}
          />
        </Panel>
      </div>

      <div className="split-layout">
        <Panel title="年度诊断" subtitle="年度表现总表。" tone="calm" className="panel--table-surface">
          <DataTable
            rows={backtestQuery.data?.yearlyDiagnostics ?? []}
            columns={YEARLY_COLUMNS}
            storageKey="backtest-yearly"
            viewPresets={YEARLY_DIAGNOSTIC_PRESETS}
            defaultPresetKey="performance"
            columnLabels={DIAGNOSTIC_COLUMN_LABELS}
            emptyText="暂无年度诊断"
            cellRenderers={backtestCellRenderers}
          />
        </Panel>
        <Panel title="阶段诊断" subtitle="看是否依赖某类市场环境。" tone="calm" className="panel--table-surface">
          <DataTable
            rows={backtestQuery.data?.regimeDiagnostics ?? []}
            columns={REGIME_COLUMNS}
            storageKey="backtest-regime"
            viewPresets={REGIME_DIAGNOSTIC_PRESETS}
            defaultPresetKey="performance"
            columnLabels={DIAGNOSTIC_COLUMN_LABELS}
            emptyText="暂无阶段诊断"
            cellRenderers={backtestCellRenderers}
          />
        </Panel>
      </div>

      <div className="split-layout">
        <SupportPanel title="策略概览" subtitle="运行体量和节奏后置。">
          <SectionBlock title="运行摘要" description="需要时再看体量和基准状态。" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                { label: '观测样本数', value: formatValue(metrics.observations) },
                { label: '交易截面数', value: formatValue(metrics.dates) },
                { label: '平均换手率', value: formatPercent(metrics.avg_turnover_ratio) },
                { label: '最大换手率', value: formatPercent(metrics.max_turnover_ratio), tone: 'warn' },
                { label: '平均持仓数', value: formatValue(metrics.avg_selected_count) },
                { label: '趋势过滤生效率', value: formatPercent(metrics.risk_filter_active_ratio) },
                { label: '被过滤期数', value: formatValue(metrics.risk_filter_filtered_periods) },
                { label: '基准总收益', value: formatPercent(metrics.benchmark_proxy_total_return) },
              ]}
            />
          </SectionBlock>
        </SupportPanel>

        <SupportPanel title="稳定性判断" subtitle="通过项和风险点后置。">
          <SectionBlock title="检查结论" description="需要时再核对风险点。" collapsible defaultExpanded={false}>
            <div className="split-layout">
              <InsightList title="通过项" items={passedChecks} tone="good" emptyText="暂无通过项" />
              <InsightList title="待关注项" items={failedChecks} tone="warn" emptyText="暂无待关注项" />
            </div>
            <PropertyGrid
              items={[
                { label: '验证集收益', value: formatPercent(stability.valid_return) },
                { label: '测试集收益', value: formatPercent(stability.test_return) },
                { label: '验证集回撤', value: formatPercent(stability.valid_drawdown), tone: 'warn' },
                { label: '测试集回撤', value: formatPercent(stability.test_drawdown), tone: 'warn' },
                { label: '验证集 RankIC', value: formatValue(stability.valid_rank_ic) },
                { label: '测试集 RankIC', value: formatValue(stability.test_rank_ic) },
                { label: '验证集换手', value: formatPercent(stability.valid_turnover) },
                { label: '测试集换手', value: formatPercent(stability.test_turnover) },
              ]}
            />
          </SectionBlock>
        </SupportPanel>
      </div>
    </div>
  )
}
