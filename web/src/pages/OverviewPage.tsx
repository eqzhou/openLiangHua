import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiGet } from '../api/client'
import { Badge } from '../components/Badge'
import { ComparisonBoard } from '../components/ComparisonBoard'
import { ContextStrip } from '../components/ContextStrip'
import { ControlField } from '../components/ControlField'
import { ControlGrid } from '../components/ControlGrid'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { LineChartCard } from '../components/LineChartCard'
import { MetricCard } from '../components/MetricCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SegmentedControl } from '../components/SegmentedControl'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { overviewCurvesClient, overviewPageClient, overviewSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDateTime, formatPercent, formatValue } from '../lib/format'
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
  const bestDrawdown = useMemo(() => findBestRow(comparison, 'daily_portfolio_max_drawdown', 'min'), [comparison])

  const dailyBarState = summary.daily_bar as Record<string, unknown> | undefined
  const featuresState = summary.features as Record<string, unknown> | undefined
  const labelsState = summary.labels as Record<string, unknown> | undefined

  const curveKeys = useMemo(() => Object.keys(equityCurves[0] ?? {}).filter((key) => key !== 'trade_date'), [equityCurves])
  const curveViewOptions = useMemo(() => buildCurveViewOptions(curveKeys), [curveKeys])
  const activeCurveView = curveViewOptions.find((option) => option.key === curveView) ?? curveViewOptions[0]

  const comparisonColumns = useMemo(
    () =>
      comparison.map((row, index) => ({
        key: `comparison:${index}`,
        label: String(row.model ?? '-'),
        description: String(row.split ?? '').toUpperCase(),
      })),
    [comparison],
  )

  const comparisonRows = useMemo(
    () =>
      comparisonColumns.length
        ? [
            {
              key: 'annualized',
              label: '年化收益',
              values: Object.fromEntries(comparisonColumns.map((column, index) => [column.key, formatPercent(comparison[index]?.daily_portfolio_annualized_return)])),
            },
            {
              key: 'sharpe',
              label: '夏普比率',
              values: Object.fromEntries(comparisonColumns.map((column, index) => [column.key, formatValue(comparison[index]?.daily_portfolio_sharpe)])),
            },
            {
              key: 'drawdown',
              label: '最大回撤',
              values: Object.fromEntries(comparisonColumns.map((column, index) => [column.key, formatPercent(comparison[index]?.daily_portfolio_max_drawdown)])),
            },
            {
              key: 'hit-rate',
              label: 'TopN 命中率',
              values: Object.fromEntries(comparisonColumns.map((column, index) => [column.key, formatPercent(comparison[index]?.top_n_hit_rate)])),
            },
          ]
        : [],
    [comparison, comparisonColumns],
  )

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

  const overviewContextItems = [
    {
      label: '当前分段',
      value: String(overviewQuery.data?.selectedSplit ?? params.split).toUpperCase(),
      tone: 'brand' as const,
    },
    {
      label: '研究区间',
      value: summary.date_min && summary.date_max ? `${String(summary.date_min)} ~ ${String(summary.date_max)}` : '-',
    },
    {
      label: '股票覆盖',
      value: summary.feature_symbols ?? '-',
      helper: `样本 ${formatValue(summary.feature_rows ?? '-')}`,
    },
    {
      label: '日线状态',
      value: dailyBarState?.exists ? '可用' : '缺失',
      helper: String(summary.date_max ?? '-'),
      tone: dailyBarState?.exists ? ('good' as const) : ('warn' as const),
    },
    {
      label: '最佳夏普',
      value: bestSharpe?.model ? String(bestSharpe.model).toUpperCase() : '-',
      helper: formatValue(bestSharpe?.daily_portfolio_sharpe),
    },
  ]

  const splitOptions = (bootstrap?.splitNames ?? ['valid', 'test']).map((split) => ({
    key: split,
    label: bootstrap?.splitLabels?.[split] ?? split,
  }))

  const overviewHeroBadges = (
    <>
      <Badge tone="brand">{`当前分段 ${(overviewQuery.data?.selectedSplit ?? params.split).toUpperCase()}`}</Badge>
      <Badge tone={dailyBarState?.exists ? 'good' : 'warn'}>{`日线 ${dailyBarState?.exists ? '可用' : '缺失'}`}</Badge>
      <Badge tone={featuresState?.exists ? 'good' : 'warn'}>{`特征 ${featuresState?.exists ? '可用' : '缺失'}`}</Badge>
      <Badge tone={labelsState?.exists ? 'good' : 'warn'}>{`标签 ${labelsState?.exists ? '可用' : '缺失'}`}</Badge>
    </>
  )

  return (
    <div className="page-stack">
      <WorkspaceHero
        title="研究概览"
        badges={overviewHeroBadges}
        controls={
          <SegmentedControl
            label="切换概览分段"
            value={params.split}
            options={splitOptions}
            onChange={(value) => updateParams({ split: value })}
          />
        }
      />

      <div className="metric-grid metric-grid--four">
        <MetricCard label="特征样本数" value={summary.feature_rows ?? 0} />
        <MetricCard label="股票覆盖数" value={summary.feature_symbols ?? 0} />
        <MetricCard
          label="最佳年化收益"
          value={formatPercent(bestAnnualized?.daily_portfolio_annualized_return)}
          helper={bestAnnualized?.model ? String(bestAnnualized.model).toUpperCase() : '-'}
          tone="good"
        />
        <MetricCard
          label="最佳夏普"
          value={formatValue(bestSharpe?.daily_portfolio_sharpe)}
          helper={bestSharpe?.model ? String(bestSharpe.model).toUpperCase() : '-'}
          tone="good"
        />
      </div>

      <Panel title="数据" subtitle={summary.date_max ? `研究区间 ${String(summary.date_min)} 至 ${String(summary.date_max)}` : undefined} tone="warm" className="panel--summary-surface">
        <QueryNotice isLoading={overviewQuery.isLoading || curvesQuery.isLoading} error={overviewQuery.error ?? curvesQuery.error} />

        <SectionBlock title="当前概览">
          <SpotlightCard
            title={String(overviewQuery.data?.selectedSplit ?? params.split).toUpperCase()}
            meta="当前比较分段"
            badges={[
              { label: `日线 ${dailyBarState?.exists ? '可用' : '缺失'}`, tone: dailyBarState?.exists ? 'good' : 'warn' },
              { label: `特征 ${featuresState?.exists ? '可用' : '缺失'}`, tone: featuresState?.exists ? 'good' : 'warn' },
              { label: `标签 ${labelsState?.exists ? '可用' : '缺失'}`, tone: labelsState?.exists ? 'good' : 'warn' },
            ]}
            metrics={[
              { label: '股票覆盖数', value: summary.feature_symbols ?? 0 },
              { label: '特征样本数', value: summary.feature_rows ?? 0 },
              { label: '最佳年化', value: formatPercent(bestAnnualized?.daily_portfolio_annualized_return), tone: 'good' },
              { label: '最佳夏普', value: bestSharpe?.daily_portfolio_sharpe ?? '-', tone: 'good' },
              { label: '最浅回撤', value: formatPercent(bestDrawdown?.daily_portfolio_max_drawdown), tone: 'warn' },
            ]}
          />
        </SectionBlock>

        <ContextStrip items={overviewContextItems} />

        <PageFilterBar title="切换概览分段">
          <ControlGrid variant="double">
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

      <Panel title="比较" tone="calm" className="panel--summary-surface">
        <ComparisonBoard columns={comparisonColumns} rows={comparisonRows} />
      </Panel>

      <Panel title="模型" tone="calm" className="panel--table-surface">
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
      </Panel>

      <Panel title="曲线" tone="calm" className="panel--table-surface">
        <LineChartCard
          data={equityCurves}
          xKey="trade_date"
          lineKeys={activeCurveView?.lineKeys}
          title="模型资金曲线"
          subtitle={activeCurveView?.subtitle ?? '比较不同模型在同一分段上的资金曲线走势。'}
          actions={
            curveViewOptions.length > 1 ? (
              <SegmentedControl
                label="切换净值预设"
                value={activeCurveView?.key ?? 'all'}
                options={curveViewOptions.map((option) => ({ key: option.key, label: option.label }))}
                onChange={setCurveView}
              />
            ) : null
          }
          emptyText="暂无净值曲线"
        />
      </Panel>

      <div className="split-layout">
        <SupportPanel title="数据补充">
          <SectionBlock title="面板状态" tone="muted" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                { label: '日线面板体积', value: formatValue(dailyBarState?.size_mb ? `${dailyBarState.size_mb} MB` : '-') },
                { label: '日线更新时间', value: formatDateTime(dailyBarState?.updated) },
                { label: '特征面板体积', value: formatValue(featuresState?.size_mb ? `${featuresState.size_mb} MB` : '-') },
                { label: '特征更新时间', value: formatDateTime(featuresState?.updated) },
                { label: '标签面板体积', value: formatValue(labelsState?.size_mb ? `${labelsState.size_mb} MB` : '-') },
                { label: '标签更新时间', value: formatDateTime(labelsState?.updated) },
                { label: '缓存股票数', value: formatValue(summary.cached_symbols ?? 0) },
                { label: '研究区间', value: `${formatValue(summary.date_min)} 至 ${formatValue(summary.date_max)}`, span: 'double' },
              ]}
            />
          </SectionBlock>
        </SupportPanel>

        <SupportPanel title="模型补充">
          <SectionBlock title="最优解概览" tone="muted" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                {
                  label: '最佳年化模型',
                  value: bestAnnualized ? `${formatValue(bestAnnualized.model)} / ${formatValue(bestAnnualized.split)}` : '-',
                  helper: `年化 ${formatPercent(bestAnnualized?.daily_portfolio_annualized_return)}，回撤 ${formatPercent(bestAnnualized?.daily_portfolio_max_drawdown)}`,
                  span: 'double',
                  tone: 'good',
                },
                {
                  label: '最佳夏普模型',
                  value: bestSharpe ? `${formatValue(bestSharpe.model)} / ${formatValue(bestSharpe.split)}` : '-',
                  helper: `夏普 ${formatValue(bestSharpe?.daily_portfolio_sharpe)}，命中率 ${formatPercent(bestSharpe?.top_n_hit_rate)}`,
                  span: 'double',
                  tone: 'good',
                },
                {
                  label: '最浅回撤模型',
                  value: bestDrawdown ? `${formatValue(bestDrawdown.model)} / ${formatValue(bestDrawdown.split)}` : '-',
                  helper: `最大回撤 ${formatPercent(bestDrawdown?.daily_portfolio_max_drawdown)}，换手 ${formatPercent(bestDrawdown?.avg_turnover_ratio)}`,
                  span: 'double',
                },
              ]}
            />
          </SectionBlock>
        </SupportPanel>
      </div>
    </div>
  )
}
