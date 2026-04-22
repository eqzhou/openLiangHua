import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'

import { apiGet } from '../api/client'
import { ContextStrip } from '../components/ContextStrip'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { MetricCard } from '../components/MetricCard'
import { MarkdownCard } from '../components/MarkdownCard'
import { MobileInspectionCard } from '../components/MobileInspectionCard'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { Badge } from '../components/Badge'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { homeAiReviewClient, homeCandidatesClient, homeSummaryClient, homeWatchlistClient } from '../facades/dashboardPageClient'
import { formatDateTime, formatPercent, formatValue } from '../lib/format'
import { HOME_REFETCH_INTERVAL_MS } from '../lib/polling'
import { buildAiReviewPath, buildCandidatesPath, buildWatchlistPath } from '../lib/shareLinks'
import { describeRealtimeSource, formatRealtimeCoverage, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import type { ActionResult, BootstrapPayload, HomeAiReviewPayload, HomeCandidatesPayload, HomeSummaryPayload, HomeWatchlistPayload, JsonRecord } from '../types/api'

interface HomePageProps {
  bootstrap?: BootstrapPayload
  latestAction: ActionResult | null
  authenticated: boolean
  currentUserLabel?: string | null
  actionPendingName: string | null
  sharingCurrentView: boolean
  onRunAction: (actionName: string) => void
  onShareCurrentView: () => void
}

const WATCHLIST_COLUMNS = ['name', 'source_tags', 'realtime_price', 'realtime_pct_chg', 'inference_ensemble_rank', 'premarket_plan']
const WATCHLIST_COLUMN_LABELS = {
  name: '股票',
  source_tags: '来源标签',
  realtime_price: '最新价',
  realtime_pct_chg: '盘中涨跌',
  inference_ensemble_rank: '最新推理排名',
  premarket_plan: '执行建议',
}

const CANDIDATE_COLUMNS = ['name', 'rank', 'industry', 'score', 'rank_pct', 'ret_t1_t10']
const CANDIDATE_COLUMN_LABELS = {
  name: '候选股',
  rank: '排名',
  industry: '行业',
  score: '综合分数',
  rank_pct: '分位',
  ret_t1_t10: '未来10日收益',
}

const INFERENCE_COLUMNS = ['name', 'industry_display', 'final_score', 'confidence_level', 'action_hint']
const INFERENCE_COLUMN_LABELS = {
  name: '推理股票',
  industry_display: '行业',
  final_score: 'AI总分',
  confidence_level: '置信度',
  action_hint: '操作建议',
}

function toneToNoticeClass(tone: string) {
  if (tone === 'good') {
    return 'query-notice query-notice--success'
  }
  if (tone === 'warn') {
    return 'query-notice query-notice--error'
  }
  return 'query-notice query-notice--info'
}

function buildQuickActions(bootstrap?: BootstrapPayload) {
  const preferredOrder = ['latest_inference', 'overlay', 'watch_plan', 'action_memo']
  const actionLookup = new Map((bootstrap?.actions ?? []).map((item) => [item.actionName, item]))
  return preferredOrder.map((key) => actionLookup.get(key)).filter(Boolean) as NonNullable<BootstrapPayload['actions']>[number][]
}

function buildRecordTitle(record: JsonRecord | undefined): string {
  const name = String(record?.name ?? '')
  const symbol = String(record?.ts_code ?? '')
  if (name && symbol) {
    return `${name} / ${symbol}`
  }
  return name || symbol || '暂无重点股票'
}

function compactActionOutput(output: string | undefined): string {
  if (!output) {
    return '暂无输出'
  }
  return output.split(/\r?\n/).find((line) => line.trim())?.trim() ?? '暂无输出'
}

export function HomePage({
  bootstrap,
  latestAction,
  authenticated,
  currentUserLabel,
  actionPendingName,
  sharingCurrentView,
  onRunAction,
  onShareCurrentView,
}: HomePageProps) {
  const navigate = useNavigate()
  const homeSummaryQuery = useQuery({
    queryKey: homeSummaryClient.queryKey(),
    queryFn: () => apiGet<HomeSummaryPayload>(homeSummaryClient.path()),
    refetchInterval: HOME_REFETCH_INTERVAL_MS,
  })
  const homeWatchlistQuery = useQuery({
    queryKey: homeWatchlistClient.queryKey(),
    queryFn: () => apiGet<HomeWatchlistPayload>(homeWatchlistClient.path()),
    refetchInterval: HOME_REFETCH_INTERVAL_MS,
  })
  const homeCandidatesQuery = useQuery({
    queryKey: homeCandidatesClient.queryKey(),
    queryFn: () => apiGet<HomeCandidatesPayload>(homeCandidatesClient.path()),
    refetchInterval: HOME_REFETCH_INTERVAL_MS,
  })
  const homeAiReviewQuery = useQuery({
    queryKey: homeAiReviewClient.queryKey(),
    queryFn: () => apiGet<HomeAiReviewPayload>(homeAiReviewClient.path()),
    refetchInterval: HOME_REFETCH_INTERVAL_MS,
  })

  const service = homeSummaryQuery.data?.service ?? {}
  const realtimeSnapshot = (service.realtime_snapshot as JsonRecord | undefined) ?? {}
  const overview = homeSummaryQuery.data?.overview
  const watchlist = homeWatchlistQuery.data
  const candidates = homeCandidatesQuery.data
  const aiReview = homeAiReviewQuery.data
  const alerts = homeSummaryQuery.data?.alerts ?? []
  const focusWatchRecord = watchlist?.focusRecord ?? {}
  const focusCandidateRecord = aiReview?.focusRecord ?? candidates?.focusRecord ?? {}
  const quickActions = useMemo(() => buildQuickActions(bootstrap), [bootstrap])
  const failedSymbols = normalizeRealtimeFailedSymbols(realtimeSnapshot.failed_symbols)
  const realtimeSource = describeRealtimeSource(realtimeSnapshot.source)
  const realtimeCoverage = formatRealtimeCoverage(realtimeSnapshot.requested_symbol_count, realtimeSnapshot.success_symbol_count)
  const serviceRunning = String(service.effective_state ?? '') === 'running'

  const watchlistCellRenderers = useMemo(
    () => ({
      name: (row: JsonRecord) => (
        <EntityCell
          title={String(row.name ?? '-')}
          subtitle={String(row.ts_code ?? '')}
          meta={String(row.industry ?? row.source_category ?? '')}
          badges={[
            row.source_category ? { label: String(row.source_category), tone: 'brand' as const } : null,
            row.is_overlay_selected ? { label: '历史精选', tone: 'good' as const } : null,
            row.is_inference_overlay_selected ? { label: '最新推理', tone: 'brand' as const } : null,
          ].filter(Boolean) as Array<{ label: string; tone?: 'default' | 'brand' | 'good' | 'warn' }>}
        />
      ),
    }),
    [],
  )

  const candidateCellRenderers = useMemo(
    () => ({
      name: (row: JsonRecord) => (
        <EntityCell
          title={String(row.name ?? '-')}
          subtitle={String(row.ts_code ?? '')}
          meta={String(row.industry ?? row.industry_display ?? '')}
        />
      ),
    }),
    [],
  )

  const contextItems = [
    {
      label: '页面服务',
      value: String(service.status_label_display ?? '未知'),
      tone: String(service.effective_state ?? '') === 'running' ? ('good' as const) : ('warn' as const),
    },
    {
      label: '行情快照',
      value: String(realtimeSnapshot.snapshot_label_display ?? '暂无快照'),
      helper: formatDateTime(realtimeSnapshot.fetched_at),
    },
    {
      label: '观察池数量',
      value: watchlist?.overview?.totalCount ?? 0,
      helper: `浮盈亏 ${formatValue(watchlist?.overview?.unrealizedPnl ?? '-')}`,
    },
    {
      label: '模型候选日期',
      value: String(candidates?.latestDate ?? '-'),
      helper: String(candidates?.modelName ?? '').toUpperCase(),
    },
    {
      label: '最佳夏普模型',
      value: String(overview?.bestSharpe?.model ?? '-').toUpperCase(),
      helper: formatValue(overview?.bestSharpe?.daily_portfolio_sharpe ?? '-'),
    },
  ]

  const heroBadges = (
    <>
      <Badge tone={serviceRunning ? 'good' : 'warn'}>{serviceRunning ? '服务正常' : '服务待确认'}</Badge>
      <Badge tone={failedSymbols.length ? 'warn' : 'brand'}>{`行情覆盖 ${realtimeCoverage}`}</Badge>
      <Badge tone={alerts.length ? 'warn' : 'good'}>{alerts.length ? `${alerts.length} 条风险提醒` : '暂无高优先级提醒'}</Badge>
      <Badge tone={authenticated ? 'good' : 'default'}>{authenticated ? '可写' : '只读'}</Badge>
    </>
  )

  return (
    <div className="page-stack">
      <WorkspaceHero
        title="交易概览"
        className="home-anchor-hero"
        description="把服务状态、候选结果、持仓重点和 AI 推理压缩到一个班次首页，先判断今天该盯什么，再决定进入哪条工作流。"
        badges={heroBadges}
        summary={
          <dl className="workspace-hero__summary-grid">
            <div>
              <dt>页面服务</dt>
              <dd>{String(service.status_label_display ?? '未知')}</dd>
            </div>
            <div>
              <dt>行情快照</dt>
              <dd>{String(realtimeSnapshot.snapshot_label_display ?? '暂无快照')}</dd>
            </div>
            <div>
              <dt>观察池</dt>
              <dd>{String(watchlist?.overview?.totalCount ?? 0)} 只</dd>
            </div>
            <div>
              <dt>最强模型</dt>
              <dd>{String(overview?.bestAnnualized?.model ?? '-').toUpperCase()}</dd>
            </div>
          </dl>
        }
      />

      <div className="metric-grid metric-grid--four dashboard-status-grid">
        <MetricCard label="观察池总数" value={watchlist?.overview?.totalCount ?? 0} />
        <MetricCard label="AI 观察入池" value={watchlist?.overview?.overlayCount ?? 0} tone="good" />
        <MetricCard label="风险提醒" value={alerts.length} tone={alerts.length ? 'warn' : 'good'} helper={alerts.length ? alerts[0]?.title : '暂无提醒'} />
        <MetricCard
          label="当前最强模型"
          value={String(overview?.bestAnnualized?.model ?? '-').toUpperCase()}
          helper={formatPercent(overview?.bestAnnualized?.daily_portfolio_annualized_return)}
          tone="good"
        />
      </div>

      <Panel title="总览" subtitle="先看班次结论、风险提醒和当前模型重心。" tone="warm" className="panel--summary-surface home-desk-panel">
        <QueryNotice
          isLoading={homeSummaryQuery.isLoading || homeWatchlistQuery.isLoading || homeCandidatesQuery.isLoading || homeAiReviewQuery.isLoading}
          error={homeSummaryQuery.error ?? homeWatchlistQuery.error ?? homeCandidatesQuery.error ?? homeAiReviewQuery.error}
        />

        <SectionBlock title="值班结论">
          <SpotlightCard
            title={authenticated ? `欢迎回来，${currentUserLabel ?? '当前用户'}` : '当前为只读'}
            meta="当前班次"
            metrics={[
              { label: '观察池数量', value: watchlist?.overview?.totalCount ?? 0 },
              { label: '历史精选', value: watchlist?.overview?.overlayCount ?? 0 },
              { label: '最新推理池', value: watchlist?.overview?.inferenceOverlayCount ?? 0 },
              { label: '快照覆盖率', value: realtimeCoverage, tone: failedSymbols.length ? 'warn' : 'good' },
              {
                label: '当前最强模型',
                value: String(overview?.bestAnnualized?.model ?? '-').toUpperCase(),
                helper: formatPercent(overview?.bestAnnualized?.daily_portfolio_annualized_return),
              },
            ]}
            actions={
              <div className="spotlight-card__actions">
                {quickActions.slice(0, 2).map((action, index) => (
                  <button
                    key={action.actionName}
                    type="button"
                    className={`button ${index === 0 ? 'button--primary' : 'button--ghost'}`}
                    disabled={!authenticated || Boolean(actionPendingName)}
                    onClick={() => onRunAction(action.actionName)}
                  >
                    {actionPendingName === action.actionName ? action.spinnerText ?? '执行中...' : action.label}
                  </button>
                ))}
                <button type="button" className="button button--ghost" disabled={sharingCurrentView} onClick={onShareCurrentView}>
                  {sharingCurrentView ? '复制中...' : '复制当前视图'}
                </button>
                <button type="button" className="button button--ghost" onClick={() => navigate('/workspace')}>
                  查看工作台
                </button>
              </div>
            }
          />
        </SectionBlock>

        <ContextStrip items={contextItems} />

        {alerts.length ? (
          <SectionBlock title="风险提醒" tone="muted" collapsible defaultExpanded={false}>
            <div className="section-stack">
              {alerts.map((alert) => (
                <div key={`${alert.title}-${alert.detail}`} className={toneToNoticeClass(alert.tone)}>
                  <strong>{alert.title}</strong>
                  <div>{alert.detail}</div>
                </div>
              ))}
            </div>
          </SectionBlock>
        ) : null}
      </Panel>

      <Panel title="重点" subtitle="把当前最该关注的持仓与候选压成前台工作面。" className="panel--summary-surface home-focus-panel">
        <div className="split-layout">
          <SectionBlock title="持仓概览">
            <SpotlightCard
              title={buildRecordTitle(focusWatchRecord)}
              meta={String(focusWatchRecord.source_category ?? focusWatchRecord.entry_group ?? '持仓')}
              subtitle={String(focusWatchRecord.llm_latest_summary ?? focusWatchRecord.premarket_plan ?? focusWatchRecord.action_brief ?? '')}
              metrics={[
                { label: '最新价', value: formatValue(focusWatchRecord.realtime_price ?? focusWatchRecord.mark_price ?? '-') },
                {
                  label: '盘中涨跌',
                  value: formatPercent(focusWatchRecord.realtime_pct_chg ?? '-'),
                  tone:
                    typeof focusWatchRecord.realtime_pct_chg === 'number' && Number(focusWatchRecord.realtime_pct_chg) > 0
                      ? 'good'
                      : 'warn',
                },
                { label: '推理排名', value: formatValue(focusWatchRecord.inference_ensemble_rank ?? '-') },
                { label: '分析状态', value: formatValue(focusWatchRecord.llm_latest_status ?? '-') },
              ]}
              actions={
                String(focusWatchRecord.ts_code ?? '').trim() ? (
                  <div className="spotlight-card__actions">
                    <button type="button" className="button button--primary" onClick={() => navigate(buildWatchlistPath(String(focusWatchRecord.ts_code)))}>
                      查看持仓
                    </button>
                    <button type="button" className="button button--ghost" onClick={() => navigate(buildAiReviewPath(String(focusWatchRecord.ts_code)))}>
                      AI 分析
                    </button>
                  </div>
                ) : null
              }
            />
          </SectionBlock>

          <SectionBlock title="候选概览">
            <SpotlightCard
              title={buildRecordTitle(focusCandidateRecord)}
              meta={String(focusCandidateRecord.industry_display ?? focusCandidateRecord.industry ?? '候选股')}
              subtitle={String(focusCandidateRecord.action_hint ?? focusCandidateRecord.thesis_summary ?? '')}
              metrics={[
                { label: 'AI 总分', value: formatValue(focusCandidateRecord.final_score ?? '-') },
                { label: '模型分数', value: formatValue(focusCandidateRecord.score ?? focusCandidateRecord.quant_score ?? '-') },
                { label: '未来收益', value: formatPercent(focusCandidateRecord.ret_t1_t10 ?? '-') },
                { label: '置信度', value: formatValue(focusCandidateRecord.confidence_level ?? '-') },
              ]}
              actions={
                String(focusCandidateRecord.ts_code ?? '').trim() ? (
                  <div className="spotlight-card__actions">
                    <button type="button" className="button button--primary" onClick={() => navigate(buildCandidatesPath(String(focusCandidateRecord.ts_code)))}>
                      查看候选
                    </button>
                    <button type="button" className="button button--ghost" onClick={() => navigate(buildAiReviewPath(String(focusCandidateRecord.ts_code)))}>
                      查看 AI 分析
                    </button>
                  </div>
                ) : null
              }
            />
          </SectionBlock>
        </div>
      </Panel>

      <div className="mobile-inspection-stack mobile-only">
        <MobileInspectionCard
          title={alerts.length ? `${alerts.length} 条今日提醒` : '暂无高优先级提醒'}
          subtitle={alerts[0] ? String(alerts[0].detail ?? alerts[0].title ?? '') : undefined}
          badges={
            <div className="badge-row">
              <Badge tone={alerts.length ? 'warn' : 'good'}>{alerts.length ? '需要关注' : '状态正常'}</Badge>
              <Badge tone="brand">{realtimeCoverage}</Badge>
            </div>
          }
          actions={
            alerts.length ? (
              <details className="details-block">
                <summary>展开提醒列表</summary>
                <div className="section-stack">
                  {alerts.map((alert) => (
                    <div key={`${alert.title}-${alert.detail}`} className={toneToNoticeClass(alert.tone)}>
                      <strong>{alert.title}</strong>
                      <div>{alert.detail}</div>
                    </div>
                  ))}
                </div>
              </details>
            ) : null
          }
        />

        <MobileInspectionCard
          title={buildRecordTitle(focusWatchRecord)}
          subtitle={String(focusWatchRecord.premarket_plan ?? focusWatchRecord.action_brief ?? '')}
          badges={
            <div className="badge-row">
              <Badge tone="brand">{String(focusWatchRecord.entry_group ?? '持仓')}</Badge>
              {focusWatchRecord.source_category ? <Badge tone="brand">{String(focusWatchRecord.source_category)}</Badge> : null}
              <Badge tone={typeof focusWatchRecord.realtime_pct_chg === 'number' && Number(focusWatchRecord.realtime_pct_chg) > 0 ? 'good' : 'warn'}>
                {formatPercent(focusWatchRecord.realtime_pct_chg ?? '-')}
              </Badge>
            </div>
          }
          body={
            <PropertyGrid
              items={[
                { label: '来源标签', value: formatValue(focusWatchRecord.source_tags ?? '-') , span: 'double' },
                { label: '最新价', value: formatValue(focusWatchRecord.realtime_price ?? focusWatchRecord.mark_price ?? '-') },
                { label: '推理排名', value: formatValue(focusWatchRecord.inference_ensemble_rank ?? '-') },
              ]}
            />
          }
          actions={
            String(focusWatchRecord.ts_code ?? '').trim() ? (
              <div className="inline-actions inline-actions--compact">
                <button type="button" className="button button--primary" onClick={() => navigate(buildWatchlistPath(String(focusWatchRecord.ts_code)))}>
                  查看持仓
                </button>
                <button type="button" className="button button--ghost" onClick={() => navigate(buildAiReviewPath(String(focusWatchRecord.ts_code)))}>
                  AI 分析
                </button>
              </div>
            ) : null
          }
        />

        <MobileInspectionCard
          title={buildRecordTitle(focusCandidateRecord)}
          subtitle={String(focusCandidateRecord.action_hint ?? focusCandidateRecord.thesis_summary ?? '')}
          badges={
            <div className="badge-row">
              <Badge tone="brand">{String(focusCandidateRecord.industry_display ?? focusCandidateRecord.industry ?? '候选股')}</Badge>
              <Badge tone="good">{formatValue(focusCandidateRecord.final_score ?? '-')}</Badge>
            </div>
          }
          body={
            <PropertyGrid
              items={[
                { label: '模型分数', value: formatValue(focusCandidateRecord.score ?? focusCandidateRecord.quant_score ?? '-') },
                { label: '未来收益', value: formatPercent(focusCandidateRecord.ret_t1_t10 ?? '-') },
              ]}
            />
          }
          actions={
            String(focusCandidateRecord.ts_code ?? '').trim() ? (
              <div className="inline-actions inline-actions--compact">
                <button type="button" className="button button--primary" onClick={() => navigate(buildCandidatesPath(String(focusCandidateRecord.ts_code)))}>
                  查看候选
                </button>
                <button type="button" className="button button--ghost" onClick={() => navigate(buildAiReviewPath(String(focusCandidateRecord.ts_code)))}>
                  查看 AI 分析
                </button>
              </div>
            ) : null
          }
        />
      </div>

      <SupportPanel title="更多" className="home-support-panel">
        <SectionBlock title="AI Shortlist" collapsible defaultExpanded={false}>
          <MarkdownCard title="最新推理 Shortlist" content={aiReview?.shortlistMarkdown} />
        </SectionBlock>

        <SectionBlock title="观察池总表" collapsible defaultExpanded={false}>
          <DataTable
            rows={watchlist?.records ?? []}
            columns={WATCHLIST_COLUMNS}
            columnLabels={WATCHLIST_COLUMN_LABELS}
            storageKey="home-watchlist"
            loading={homeWatchlistQuery.isLoading}
            emptyText="暂无观察池数据"
            stickyFirstColumn
            cellRenderers={watchlistCellRenderers}
          />
        </SectionBlock>

        <SectionBlock title="候选池总表" collapsible defaultExpanded={false}>
          <DataTable
            rows={candidates?.records ?? []}
            columns={CANDIDATE_COLUMNS}
            columnLabels={CANDIDATE_COLUMN_LABELS}
            storageKey="home-candidates"
            loading={homeCandidatesQuery.isLoading}
            emptyText="暂无候选股"
            stickyFirstColumn
            cellRenderers={candidateCellRenderers}
          />
        </SectionBlock>

        <SectionBlock title="AI 最新推理池" tone="muted" collapsible defaultExpanded={false}>
          <DataTable
            rows={aiReview?.inferenceRecords ?? []}
            columns={INFERENCE_COLUMNS}
            columnLabels={INFERENCE_COLUMN_LABELS}
            storageKey="home-ai-inference"
            loading={homeAiReviewQuery.isLoading}
            emptyText="暂无 AI 推理股票"
            stickyFirstColumn
            cellRenderers={candidateCellRenderers}
          />
        </SectionBlock>

        <SectionBlock title="系统与行情" collapsible defaultExpanded={false}>
          <PropertyGrid
            items={[
              { label: '快照状态', value: String(realtimeSnapshot.snapshot_label_display ?? '暂无快照') },
              { label: '快照时间', value: formatDateTime(realtimeSnapshot.fetched_at) },
              { label: '行情来源', value: realtimeSource.label },
              { label: '覆盖率', value: realtimeCoverage, tone: failedSymbols.length ? 'warn' : 'good' },
              {
                label: '失败股票',
                value: failedSymbols.length ? failedSymbols.join(' / ') : '暂无',
                span: 'double',
                tone: failedSymbols.length ? 'warn' : 'good',
              },
              { label: '参数概览', value: homeSummaryQuery.data?.configSummaryText ?? '暂无参数概览', span: 'double' },
            ]}
          />
        </SectionBlock>

        <SectionBlock title="最近动作" collapsible defaultExpanded={false}>
          {latestAction ? (
            <div className="section-stack">
              <SpotlightCard
                title={latestAction.ok ? '执行完成' : '执行失败'}
                meta={latestAction.actionName}
                subtitle={compactActionOutput(latestAction.output)}
                metrics={[
                  { label: '动作名称', value: latestAction.actionName },
                  { label: '状态', value: latestAction.ok ? '完成' : '失败', tone: latestAction.ok ? 'good' : 'warn' },
                ]}
              />
              <details className="details-block">
                <summary>查看完整输出</summary>
                <pre className="log-block">{latestAction.output || '暂无输出'}</pre>
              </details>
            </div>
          ) : (
            <div className="empty-state">还没有执行记录，可先从首页快捷动作开始。</div>
          )}
        </SectionBlock>
      </SupportPanel>

    </div>
  )
}
