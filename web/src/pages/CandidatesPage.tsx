import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate } from 'react-router-dom'

import { apiGet } from '../api/client'
import { Badge } from '../components/Badge'
import { ControlField } from '../components/ControlField'
import { ControlGrid } from '../components/ControlGrid'
import { ContextStrip } from '../components/ContextStrip'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { MetricCard } from '../components/MetricCard'
import { PageFilterBar } from '../components/PageFilterBar'
import { Panel } from '../components/Panel'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { WorkspaceHero } from '../components/WorkspaceHero'
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

  const candidatesContextItems = useMemo(
    () => [
      { label: '信号日期', value: summaryQuery.data?.latestDate ?? '-' },
      { label: '结果模型', value: String(summaryQuery.data?.modelName ?? '-').toUpperCase(), tone: 'brand' as const },
      { label: '样本分段', value: String(summaryQuery.data?.splitName ?? '-').toUpperCase() },
      {
        label: '候选总数',
        value: summaryQuery.data?.totalCount ?? 0,
        helper: `第 ${summaryQuery.data?.page ?? params.page} / ${summaryQuery.data?.totalPages ?? 1} 页`,
      },
      { label: '每页数量', value: summaryQuery.data?.pageSize ?? params.topN },
    ],
    [params.page, params.topN, summaryQuery.data?.latestDate, summaryQuery.data?.modelName, summaryQuery.data?.page, summaryQuery.data?.pageSize, summaryQuery.data?.splitName, summaryQuery.data?.totalCount, summaryQuery.data?.totalPages],
  )

  const candidatesHeroBadges = (
    <>
      <Badge tone="brand">{String(summaryQuery.data?.modelName ?? params.model).toUpperCase()}</Badge>
      <Badge tone="default">{String(summaryQuery.data?.splitName ?? params.split).toUpperCase()}</Badge>
      <Badge tone={positiveCount > 0 ? 'good' : 'warn'}>{`${positiveCount} 只正收益样本`}</Badge>
      <Badge tone="brand">{`第 ${summaryQuery.data?.page ?? params.page} / ${summaryQuery.data?.totalPages ?? 1} 页`}</Badge>
    </>
  )

  const openDetail = (symbol: string) => {
    if (!symbol) {
      return
    }
    navigate({ pathname: buildCandidatesPath(symbol), search: location.search })
  }

  return (
    <div className="page-stack">
      <WorkspaceHero title="候选股" badges={candidatesHeroBadges} />

      <div className="metric-grid metric-grid--four">
        <MetricCard label="候选池总数" value={summaryQuery.data?.totalCount ?? 0} />
        <MetricCard label="当前页数量" value={latestPicks.length} />
        <MetricCard label="正收益样本" value={positiveCount} tone={positiveCount > 0 ? 'good' : 'default'} />
        <MetricCard label="当前模型" value={String(summaryQuery.data?.modelName ?? params.model).toUpperCase()} />
      </div>

      <Panel title="筛选" subtitle={summaryQuery.data?.latestDate ? `${String(summaryQuery.data.modelName).toUpperCase()} / ${String(summaryQuery.data.splitName).toUpperCase()} / ${formatDate(summaryQuery.data.latestDate)}` : undefined} tone="warm" className="panel--summary-surface">
        <QueryNotice isLoading={summaryQuery.isLoading} error={summaryQuery.error} />

        <SectionBlock title="列表模式" tone="emphasis">
          <SpotlightCard
            title="列表优先"
            meta="像 ERP 一样先查列表，再进入详情页"
            subtitle="不会首屏自动拉单票详情和历史曲线。"
            metrics={[
              { label: '候选总数', value: summaryQuery.data?.totalCount ?? 0 },
              { label: '当前页', value: summaryQuery.data?.page ?? params.page },
              { label: '总页数', value: summaryQuery.data?.totalPages ?? 1 },
              { label: '每页数量', value: summaryQuery.data?.pageSize ?? params.topN },
            ]}
          />
        </SectionBlock>

        <ContextStrip items={candidatesContextItems} />

        <PageFilterBar title="切换候选池视角">
          <ControlGrid variant="triple">
            <ControlField label="结果模型">
              <select value={params.model} onChange={(event) => updateParams({ model: event.target.value, page: 1 })}>
                {(bootstrap?.modelNames ?? ['ridge', 'lgbm', 'ensemble']).map((item) => (
                  <option key={item} value={item}>
                    {bootstrap?.modelLabels?.[item] ?? item}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="数据集">
              <select value={params.split} onChange={(event) => updateParams({ split: event.target.value, page: 1 })}>
                {(bootstrap?.splitNames ?? ['valid', 'test']).map((item) => (
                  <option key={item} value={item}>
                    {bootstrap?.splitLabels?.[item] ?? item}
                  </option>
                ))}
              </select>
            </ControlField>
            <ControlField label="每页数量">
              <input type="number" min={10} max={100} value={params.topN} onChange={(event) => updateParams({ topN: Number(event.target.value) || 30, page: 1 })} />
            </ControlField>
          </ControlGrid>
          <div className="inline-actions inline-actions--compact">
            <button type="button" className="button button--ghost" disabled={(summaryQuery.data?.page ?? params.page) <= 1} onClick={() => updateParams({ page: Math.max(1, (summaryQuery.data?.page ?? params.page) - 1) })}>
              上一页
            </button>
            <button
              type="button"
              className="button button--ghost"
              disabled={(summaryQuery.data?.page ?? params.page) >= (summaryQuery.data?.totalPages ?? 1)}
              onClick={() => updateParams({ page: Math.min(summaryQuery.data?.totalPages ?? 1, (summaryQuery.data?.page ?? params.page) + 1) })}
            >
              下一页
            </button>
            <button type="button" className="button button--ghost" onClick={() => copyShareablePageLink(location.pathname, location.search)}>
              复制当前视图
            </button>
          </div>
        </PageFilterBar>
      </Panel>

      <Panel title="列表" tone="calm" className="panel--table-surface">
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
      </Panel>
    </div>
  )
}
