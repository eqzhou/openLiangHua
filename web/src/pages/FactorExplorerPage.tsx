import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiGet } from '../api/client'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { LineChartCard } from '../components/LineChartCard'
import { QueryNotice } from '../components/QueryNotice'
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
  { key: 'compact', label: '精简', columns: ['name', 'close_to_ma_20'] },
  { key: 'focus', label: '关注', columns: ['name', 'close_to_ma_20'] },
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
            String(row.ts_code ?? '') === String(topRank?.ts_code ?? '') ? { label: '头部股票', tone: 'good' as const } : null,
          ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
        />
      ),
    }),
    [summaryQuery.data?.selectedFactor, params.factor, topRank?.ts_code],
  )

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-function text-erp-primary"></i> 
          因子探索器
        </span>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        {/* Quick Filters in Toolbar */}
        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">因子:</span>
          <select 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary bg-white font-medium max-w-[200px]"
            value={summaryQuery.data?.selectedFactor ?? params.factor} 
            onChange={(event) => updateParams({ factor: event.target.value })}
          >
            {factorOptions.map((option) => (
              <option key={option.key} value={option.key}>{option.label}</option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2 text-erp-sm shrink-0">
          <span className="text-gray-500">股票:</span>
          <select 
            className="border border-gray-300 rounded px-1 py-0.5 outline-none focus:border-erp-primary bg-white font-medium"
            value={summaryQuery.data?.selectedSymbol ?? params.symbol} 
            onChange={(event) => updateParams({ symbol: event.target.value })}
          >
            {(summaryQuery.data?.symbolOptions ?? []).map((item) => (
              <option key={item} value={item}>{item}</option>
            ))}
          </select>
        </div>

        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>

        <div className="flex items-center gap-2 text-erp-sm shrink-0">
           <span className={`font-bold ${summaryQuery.data?.available ? 'text-erp-success' : 'text-erp-danger'}`}>
             {summaryQuery.data?.available ? 'DATA_READY' : 'DATA_MISSING'}
           </span>
           <span className="text-gray-400">|</span>
           <span className="text-gray-500">截面:</span>
           <span className="font-bold font-mono">{summaryQuery.data?.latestDate ? formatDate(summaryQuery.data.latestDate) : '-'}</span>
        </div>
        
        <div className="ml-auto flex items-center gap-4 text-erp-sm shrink-0">
          <div className="flex items-center gap-1">
            <span className="text-gray-500">可用因子:</span> 
            <span className="font-bold font-mono">{factorOptions.length}</span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">最高缺失:</span> 
            <span className="font-bold font-mono text-erp-warning">
               {worstMissing ? `${worstMissing.feature} (${formatValue(worstMissing.missing_rate)})` : '0%'}
            </span>
          </div>
        </div>
      </div>

      {/* Main Content Area: High Density Grid & Charts */}
      <div className="flex-1 bg-white p-8 overflow-y-auto flex flex-col gap-12">
        <QueryNotice isLoading={summaryQuery.isLoading} error={summaryQuery.error ?? detailQuery.error} />

        {/* Top Feature Summary Bar */}
        <div className="flex items-center gap-16 shrink-0 border erp-border bg-erp-surface rounded-xl p-6 shadow-sm mb-4">
          <div className="flex flex-col">
            <span className="text-erp-muted text-[10px] uppercase font-bold tracking-widest mb-1">当前聚焦标的</span>
            <span className="text-3xl font-bold text-erp-primary leading-none">{String(selectedRecord.name ?? summaryQuery.data?.selectedSymbol ?? '未选择')}</span>
            <span className="text-xs font-mono text-erp-muted mt-2 uppercase">{String(summaryQuery.data?.selectedSymbol ?? '-')}</span>
          </div>
          <div className="w-px h-10 bg-erp-border"></div>
          <div className="grid grid-cols-4 gap-8">
             <div className="flex flex-col">
               <span className="text-erp-muted text-[10px] uppercase font-bold">距20日线</span>
               <span className="text-xl font-mono font-bold text-erp-text">{formatValue(selectedRecord.close_to_ma_20)}</span>
             </div>
             <div className="flex flex-col">
               <span className="text-erp-muted text-[10px] uppercase font-bold">距60日线</span>
               <span className="text-xl font-mono font-bold text-erp-text">{formatValue(selectedRecord.close_to_ma_60)}</span>
             </div>
             <div className="flex flex-col">
               <span className="text-erp-muted text-[10px] uppercase font-bold">20日动量</span>
               <span className="text-xl font-mono font-bold text-erp-text">{formatValue(selectedRecord.mom_20)}</span>
             </div>
             <div className="flex flex-col">
               <span className="text-erp-muted text-[10px] uppercase font-bold">20日波动</span>
               <span className="text-xl font-mono font-bold text-erp-warning">{formatValue(selectedRecord.vol_20)}</span>
             </div>
          </div>
        </div>

        {/* Middle Section: Ranking and Missing Rates */}
        <div className="grid grid-cols-2 gap-12">
          {/* Factor Ranking */}
          <section className="flex flex-col gap-6">
            <div className="flex items-center justify-between">
              <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                <i className="ph ph-list-numbers"></i> 因子排名截面 (Factor Cross-Section)
              </h4>
              {selectedFactorDescription && <span className="text-[10px] text-gray-400 font-normal italic pr-2">{selectedFactorDescription}</span>}
            </div>
            <div className="erp-border rounded-lg overflow-hidden h-[400px]">
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
            </div>
          </section>

          {/* Missing Rates and Snapshot */}
          <div className="flex flex-col gap-10">
             <section className="flex flex-col gap-6">
                <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                  <i className="ph ph-database"></i> 数据缺失率核查 (Missing Rates)
                </h4>
                <div className="erp-border rounded-lg overflow-hidden h-[250px]">
                  <DataTable
                    rows={missingRates}
                    columns={MISSING_COLUMNS}
                    storageKey="factor-missing"
                    columnLabels={MISSING_COLUMN_LABELS}
                    emptyText="暂无缺失率数据"
                    stickyFirstColumn
                  />
                </div>
             </section>
             
             <section className="flex flex-col gap-6">
                <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                  <i className="ph ph-magnifying-glass"></i> 全因子属性快照 (Full Snapshot)
                </h4>
                <div className="erp-border rounded-lg overflow-hidden h-[300px]">
                   <DataTable
                    rows={detailQuery.data?.snapshot ?? []}
                    columns={SNAPSHOT_COLUMNS}
                    storageKey="factor-snapshot"
                    emptyText="暂无快照"
                    stickyFirstColumn
                  />
                </div>
             </section>
          </div>
        </div>

        {/* Bottom Section: History Curve */}
        <section className="flex flex-col gap-6 border-t erp-border pt-10">
           <div className="flex items-center justify-between">
              <h4 className="text-erp-primary font-bold text-sm flex items-center gap-2 border-l-4 border-erp-primary pl-3">
                <i className="ph ph-chart-line"></i> 因子历史时序走势 (Time Series)
              </h4>
              {historyViewOptions.length > 1 && (
                <div className="flex items-center gap-1 bg-gray-100 p-1 rounded-md erp-border">
                  {historyViewOptions.slice(0, 5).map((option) => (
                    <button
                      key={option.key}
                      onClick={() => setHistoryView(option.key)}
                      className={`px-3 py-1 text-[11px] rounded transition-all ${activeHistoryView.key === option.key ? 'bg-white shadow-sm font-bold text-erp-primary border erp-border' : 'text-gray-500 hover:text-gray-700'}`}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              )}
           </div>
           <div className="erp-border rounded-lg p-6 bg-gray-50/30 h-[500px]">
             <LineChartCard
                data={historyRows}
                xKey="trade_date"
                lineKeys={activeHistoryView?.lineKeys}
                title=""
                emptyText={detailQuery.isLoading ? '加载数据中...' : '暂无时序数据'}
              />
           </div>
        </section>
      </div>
    </div>
  )
}
