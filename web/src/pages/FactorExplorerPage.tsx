import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiGet } from '../api/client'
import { Badge } from '../components/Badge'
import { ContextStrip } from '../components/ContextStrip'
import { ControlField } from '../components/ControlField'
import { ControlGrid } from '../components/ControlGrid'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { LineChartCard } from '../components/LineChartCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SegmentedControl } from '../components/SegmentedControl'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import {
  factorExplorerDetailClient,
  factorExplorerPageClient,
  factorExplorerSummaryClient,
} from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDate, formatValue } from '../lib/format'
import type { FactorDetailPayload, FactorSummaryPayload, JsonRecord } from '../types/api'

const RANKING_COLUMNS = ['name', 'close_to_ma_20']
const MISSING_COLUMNS = ['feature', 'missing_rate']
const SNAPSHOT_COLUMNS = ['字段', '数值']

const RANKING_COLUMN_LABELS = {
  name: '股票',
  close_to_ma_20: '距 20 日线',
}

const MISSING_COLUMN_LABELS = {
  feature: '因子',
  missing_rate: '缺失率',
}

const FACTOR_RANKING_PRESETS = [
  { key: 'compact', label: '精简视图', columns: ['name', 'close_to_ma_20'] },
  { key: 'focus', label: '关注视图', columns: ['name', 'close_to_ma_20'] },
]

function buildHistoryViewOptions(keys: string[]) {
  if (!keys.length) {
    return []
  }

  if (keys.length === 1) {
    return [
      {
        key: keys[0],
        label: '当前因子',
        lineKeys: keys,
        subtitle: '查看当前所选历史因子的单线走势。',
      },
    ]
  }

  return [
    {
      key: 'all',
      label: '全部序列',
      lineKeys: keys,
      subtitle: '对比同一只股票在多条因子序列上的变化。',
    },
    ...keys.map((key) => ({
      key,
      label: key,
      lineKeys: [key],
      subtitle: `只看 ${key} 的历史变化。`,
    })),
  ]
}

export function FactorExplorerPage() {
  const { params, updateParams } = usePageSearchState(factorExplorerPageClient)
  const [historyView, setHistoryView] = useState('all')

  const summaryQuery = useQuery({
    queryKey: factorExplorerSummaryClient.queryKey(params),
    queryFn: () => apiGet<FactorSummaryPayload>(factorExplorerSummaryClient.path(params)),
  })

  const selectedSymbol = summaryQuery.data?.selectedSymbol ?? params.symbol
  const detailQuery = useQuery({
    queryKey: factorExplorerDetailClient.queryKey(params, selectedSymbol),
    queryFn: () => apiGet<FactorDetailPayload>(factorExplorerDetailClient.path(params, selectedSymbol)),
    enabled: Boolean(selectedSymbol),
  })

  const factorOptions = useMemo(() => summaryQuery.data?.factorOptions ?? [], [summaryQuery.data?.factorOptions])
  const selectedFactorDescription = useMemo(
    () => factorOptions.find((item) => item.key === (summaryQuery.data?.selectedFactor ?? params.factor))?.description ?? '',
    [factorOptions, summaryQuery.data?.selectedFactor, params.factor],
  )
  const ranking = summaryQuery.data?.ranking ?? []
  const topRank = ranking[0]
  const missingRates = summaryQuery.data?.missingRates ?? []
  const worstMissing = missingRates[0]
  const historyRows = useMemo(() => detailQuery.data?.history ?? [], [detailQuery.data?.history])
  const historyKeys = useMemo(() => Object.keys(historyRows[0] ?? {}).filter((key) => key !== 'trade_date'), [historyRows])
  const historyViewOptions = useMemo(() => buildHistoryViewOptions(historyKeys), [historyKeys])
  const activeHistoryView = historyViewOptions.find((option) => option.key === historyView) ?? historyViewOptions[0]
  const selectedRecord = summaryQuery.data?.selectedRecord ?? {}

  const rankingCellRenderers = useMemo(
    () => ({
      name: (row: JsonRecord) => (
        <EntityCell
          title={String(row.name ?? '-')}
          subtitle={String(row.ts_code ?? '')}
          meta={`当前因子 ${formatValue(summaryQuery.data?.selectedFactor ?? params.factor)}`}
          badges={[
            String(row.ts_code ?? '') === String(topRank?.ts_code ?? '') ? { label: '头部标的', tone: 'good' as const } : null,
          ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
        />
      ),
    }),
    [summaryQuery.data?.selectedFactor, params.factor, topRank?.ts_code],
  )

  const factorContextItems = useMemo(
    () => [
      { label: '最新截面', value: summaryQuery.data?.latestDate ?? '-' },
      { label: '当前因子', value: summaryQuery.data?.selectedFactor ?? params.factor, tone: 'brand' as const },
      { label: '历史序列', value: summaryQuery.data?.selectedHistoryFactor ?? params.historyFactor },
      { label: '当前股票', value: summaryQuery.data?.selectedSymbol ?? params.symbol },
      {
        label: '头部标的',
        value: topRank ? `${formatValue(topRank.ts_code)} / ${formatValue(topRank.name)}` : '-',
        helper: topRank ? `因子值 ${formatValue(topRank.close_to_ma_20)}` : '',
      },
    ],
    [
      summaryQuery.data?.latestDate,
      summaryQuery.data?.selectedFactor,
      summaryQuery.data?.selectedHistoryFactor,
      summaryQuery.data?.selectedSymbol,
      params.factor,
      params.historyFactor,
      params.symbol,
      topRank,
    ],
  )

  return (
    <div className="page-stack">
      <Panel
        title="因子探索"
        subtitle={
          summaryQuery.data?.latestDate
            ? `最新截面 ${formatDate(summaryQuery.data.latestDate)}。先看总表，再看单股。`
            : '先看总表，再看单股。'
        }
        tone="warm"
        className="panel--summary-surface"
      >
        <QueryNotice isLoading={summaryQuery.isLoading} error={summaryQuery.error ?? detailQuery.error} />

        <SectionBlock
          title="先看当前因子结论"
          description="先确认可用性、头部标的和缺失风险。"
          tone="emphasis"
        >
          {selectedFactorDescription ? <p className="helper-text">{selectedFactorDescription}</p> : null}
          <SpotlightCard
            title={String((summaryQuery.data?.selectedFactor ?? params.factor) || '未选择因子')}
            meta={summaryQuery.data?.latestDate ? `最新截面 ${formatDate(summaryQuery.data.latestDate)}` : '当前因子'}
            subtitle="先看可用性、头部标的和缺失率风险，再切到完整排名和单股历史。"
            badges={[
              { label: summaryQuery.data?.available ? '因子可用' : '因子不可用', tone: summaryQuery.data?.available ? 'good' : 'warn' },
              { label: topRank ? `头部标的 ${formatValue(topRank.name)}` : '暂无头部标的', tone: topRank ? 'good' : 'default' },
            ]}
            metrics={[
              { label: '可用因子数', value: factorOptions.length },
              { label: '股票样本数', value: summaryQuery.data?.symbolOptions?.length ?? 0 },
              { label: '当前排名条数', value: ranking.length },
              { label: '缺失率记录数', value: missingRates.length, tone: missingRates.length ? 'warn' : 'default' },
            ]}
          />
        </SectionBlock>

        <ContextStrip items={factorContextItems} />

        <PageFilterBar
          title="切换因子工作区"
          description="用同一组筛选在总表和单股间切换。"
          meta={
            <div className="badge-row">
              <Badge tone={summaryQuery.data?.available ? 'good' : 'warn'}>
                {summaryQuery.data?.available ? '因子可用' : '因子不可用'}
              </Badge>
              <Badge tone="brand">{String((summaryQuery.data?.selectedSymbol ?? params.symbol) || '-')}</Badge>
            </div>
          }
        >
          <ControlGrid variant="triple">
            <ControlField label="查看排名的因子">
              <select value={summaryQuery.data?.selectedFactor ?? params.factor} onChange={(event) => updateParams({ factor: event.target.value })}>
                {factorOptions.map((option) => (
                  <option key={option.key} value={option.key}>
                    {option.label}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="历史走势因子">
              <select
                value={summaryQuery.data?.selectedHistoryFactor ?? params.historyFactor}
                onChange={(event) => updateParams({ historyFactor: event.target.value })}
              >
                {factorOptions.map((option) => (
                  <option key={option.key} value={option.key}>
                    {option.label}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="查看股票">
              <select value={summaryQuery.data?.selectedSymbol ?? params.symbol} onChange={(event) => updateParams({ symbol: event.target.value })}>
                {(summaryQuery.data?.symbolOptions ?? []).map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </ControlField>
          </ControlGrid>
        </PageFilterBar>
      </Panel>

      <div className="split-layout">
        <Panel title="因子排名" subtitle="先看总表。" tone="calm" className="panel--table-surface">
          <DataTable
            rows={ranking}
            columns={RANKING_COLUMNS}
            storageKey="factor-ranking"
            viewPresets={FACTOR_RANKING_PRESETS}
            defaultPresetKey="compact"
            columnLabels={RANKING_COLUMN_LABELS}
            loading={summaryQuery.isLoading}
            emptyText="暂无因子排名"
            stickyFirstColumn
            cellRenderers={rankingCellRenderers}
          />
        </Panel>

        <Panel title="缺失率" subtitle="只看覆盖风险。" tone="calm" className="panel--table-surface">
          <DataTable
            rows={missingRates}
            columns={MISSING_COLUMNS}
            storageKey="factor-missing"
            columnLabels={MISSING_COLUMN_LABELS}
            emptyText="暂无缺失率数据"
            stickyFirstColumn
          />
        </Panel>
      </div>

      <div className="split-layout">
        <Panel title="当前标的摘要" subtitle="总表选中后再看单票。" className="panel--summary-surface">
          <SectionBlock title="当前股票快照" description="先看位置和风险。">
            <SpotlightCard
              title={String(selectedRecord.name ?? summaryQuery.data?.selectedSymbol ?? '未选择股票')}
              meta={String(summaryQuery.data?.selectedSymbol ?? '-')}
              subtitle="结合总排名看当前标的，再决定要不要继续看时间序列。"
              badges={[
                selectedRecord.industry ? { label: String(selectedRecord.industry), tone: 'brand' as const } : null,
                topRank && String(topRank.ts_code ?? '') === String(summaryQuery.data?.selectedSymbol ?? '') ? { label: '当前头部标的', tone: 'good' as const } : null,
              ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
              metrics={[
                { label: '距 20 日线', value: formatValue(selectedRecord.close_to_ma_20) },
                { label: '距 60 日线', value: formatValue(selectedRecord.close_to_ma_60) },
                { label: '20 日动量', value: formatValue(selectedRecord.mom_20) },
                { label: '20 日波动', value: formatValue(selectedRecord.vol_20), tone: 'warn' },
              ]}
            />
          </SectionBlock>
        </Panel>

        <SupportPanel title="因子字段快照" subtitle="完整字段后置。">
          <SectionBlock title="完整字段快照" description="需要时再核对原始字段。" collapsible defaultExpanded={false}>
            <DataTable
              rows={detailQuery.data?.snapshot ?? []}
              columns={SNAPSHOT_COLUMNS}
              storageKey="factor-snapshot"
              emptyText="暂无因子快照"
              stickyFirstColumn
            />
          </SectionBlock>
        </SupportPanel>
      </div>

      <SupportPanel title="个股因子历史" subtitle="历史曲线后置。">
        <SectionBlock
          title="历史因子走势"
          description="最后再看时间序列。"
          tone="emphasis"
        >
          <PropertyGrid
            items={[
              { label: '当前因子', value: formatValue(summaryQuery.data?.selectedFactor ?? params.factor), span: 'double' },
              { label: '历史序列', value: formatValue(summaryQuery.data?.selectedHistoryFactor ?? params.historyFactor), span: 'double' },
              { label: '当前股票', value: formatValue(summaryQuery.data?.selectedSymbol ?? params.symbol), span: 'double' },
              { label: '头部股票', value: topRank ? `${formatValue(topRank.ts_code)} / ${formatValue(topRank.name)}` : '-', span: 'double', tone: 'good' },
              { label: '最高缺失因子', value: formatValue(worstMissing?.feature), span: 'double', tone: 'warn' },
              { label: '最高缺失率', value: formatValue(worstMissing?.missing_rate), tone: 'warn' },
              { label: '最新截面日期', value: formatDate(summaryQuery.data?.latestDate) },
            ]}
          />
        </SectionBlock>

        <LineChartCard
          data={historyRows}
          xKey="trade_date"
          lineKeys={activeHistoryView?.lineKeys}
          title="历史因子走势"
          subtitle={activeHistoryView?.subtitle ?? '按当前选中的股票和因子查看时间序列变化。'}
          actions={
            historyViewOptions.length > 1 ? (
              <SegmentedControl
                label="切换因子历史视图"
                value={activeHistoryView?.key ?? historyViewOptions[0].key}
                options={historyViewOptions.map((option) => ({ key: option.key, label: option.label }))}
                onChange={setHistoryView}
              />
            ) : null
          }
          emptyText={detailQuery.isLoading ? '正在加载因子历史' : '暂无因子历史'}
        />
      </SupportPanel>
    </div>
  )
}
