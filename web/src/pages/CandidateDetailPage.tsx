import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate, useParams } from 'react-router-dom'

import { apiGet } from '../api/client'
import { Badge } from '../components/Badge'
import { DetailPageNav } from '../components/DetailPageNav'
import { LineChartCard } from '../components/LineChartCard'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SegmentedControl } from '../components/SegmentedControl'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { candidateDetailClient, candidateHistoryClient, candidatesPageClient, candidatesSummaryClient } from '../facades/dashboardPageClient'
import { usePageSearchState } from '../facades/usePageSearchState'
import { formatDate, formatPercent, formatValue } from '../lib/format'
import { buildAiReviewPath, buildCandidatesPath, buildWatchlistPath } from '../lib/shareLinks'
import type { CandidateDetailPayload, CandidateHistoryPayload } from '../types/api'

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
    queryFn: () => apiGet<any>(candidatesSummaryClient.path(params)),
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
    <div className="page-stack">
      <WorkspaceHero
        title="候选详情"
        eyebrow="候选股 / 详情"
        badges={
          <>
            <Badge tone="brand">{String(detail.ts_code ?? symbol)}</Badge>
            <Badge tone="default">{String(params.model).toUpperCase()}</Badge>
            <Badge tone="default">{String(params.split).toUpperCase()}</Badge>
            <Badge>{`序号 ${currentPositionLabel}`}</Badge>
          </>
        }
      />

      <Panel title="详情" tone="warm" className="panel--summary-surface">
        <QueryNotice isLoading={detailQuery.isLoading || historyQuery.isLoading} error={detailQuery.error ?? historyQuery.error} />
        {Object.keys(detail).length ? (
          <SectionBlock title="核心概览" tone="emphasis">
            <SpotlightCard
              title={String(detail.name ?? '-')}
              meta={String(detail.ts_code ?? '')}
              subtitle={String(detail.action_hint ?? '暂无建议')}
              badges={[
                { label: '候选池', tone: 'brand' },
                { label: formatDate(detail.trade_date) },
              ]}
              metrics={[
                { label: '当前排名', value: detail.rank ?? '-' },
                { label: '综合分数', value: detail.score ?? '-' },
                { label: '排名分位', value: formatPercent(detail.rank_pct) },
                { label: '未来10日收益', value: formatPercent(detail.ret_t1_t10) },
              ]}
            />
          </SectionBlock>
        ) : (
          <div className="empty-state">暂无候选详情</div>
        )}
      </Panel>

      <div className="split-layout">
        <SupportPanel title="交易背景">
          <PropertyGrid
            items={[
              { label: '行业', value: formatValue(detail.industry) },
              { label: '当日涨跌', value: formatPercent(detail.pct_chg) },
              { label: '20日动量', value: formatPercent(detail.mom_20) },
              { label: '距20日线', value: formatPercent(detail.close_to_ma_20) },
            ]}
          />
        </SupportPanel>

        <SupportPanel title="评分历史">
          {scoreViewOptions.length ? (
            <SegmentedControl
              label="切换评分预设"
              value={activeScoreView?.key ?? scoreViewOptions[0].key}
              options={scoreViewOptions.map((item) => ({ key: item.key, label: item.label }))}
              onChange={setScoreView}
            />
          ) : null}
          <LineChartCard
            data={scoreHistory}
            xKey="trade_date"
            lineKeys={activeScoreView?.lineKeys ?? scoreKeys}
            title="候选评分曲线"
            subtitle={activeScoreView?.label}
          />
        </SupportPanel>
      </div>

      <SupportPanel title="操作">
        <DetailPageNav
          onBack={() => navigate({ pathname: '/candidates', search: location.search })}
          prevLabel={previousSymbol}
          onPrev={previousSymbol ? () => navigate({ pathname: buildCandidatesPath(previousSymbol), search: location.search }) : null}
          nextLabel={nextSymbol}
          onNext={nextSymbol ? () => navigate({ pathname: buildCandidatesPath(nextSymbol), search: location.search }) : null}
        />
        <div className="inline-actions inline-actions--compact">
          <button type="button" className="button button--ghost" onClick={() => navigate(buildAiReviewPath(symbol))}>
            查看 AI 分析
          </button>
          <button type="button" className="button button--ghost" onClick={() => navigate(buildWatchlistPath(symbol))}>
            查看持仓
          </button>
        </div>
      </SupportPanel>
    </div>
  )
}
