import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'

import { apiGet } from '../api/client'
import { ContextStrip } from '../components/ContextStrip'
import { DataTable } from '../components/DataTable'
import { EntityCell } from '../components/EntityCell'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { homePageClient } from '../facades/dashboardPageClient'
import { formatDateTime, formatPercent, formatValue } from '../lib/format'
import { buildAiReviewPath, buildCandidatesPath, buildWatchlistPath } from '../lib/shareLinks'
import { describeRealtimeSource, formatRealtimeCoverage, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import type { ActionResult, BootstrapPayload, HomePayload, JsonRecord } from '../types/api'

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

const WATCHLIST_COLUMNS = ['name', 'entry_group', 'realtime_price', 'realtime_pct_chg', 'inference_ensemble_rank', 'premarket_plan']
const WATCHLIST_COLUMN_LABELS = {
  name: '标的',
  entry_group: '分组',
  realtime_price: '最新价',
  realtime_pct_chg: '盘中涨跌',
  inference_ensemble_rank: '最新推理排名',
  premarket_plan: '盘前建议',
}

const CANDIDATE_COLUMNS = ['name', 'rank', 'industry', 'score', 'rank_pct', 'ret_t1_t10']
const CANDIDATE_COLUMN_LABELS = {
  name: '候选',
  rank: '排名',
  industry: '行业',
  score: '综合分数',
  rank_pct: '分位',
  ret_t1_t10: '未来10日收益',
}

const INFERENCE_COLUMNS = ['name', 'industry_display', 'final_score', 'confidence_level', 'action_hint']
const INFERENCE_COLUMN_LABELS = {
  name: '推理标的',
  industry_display: '行业',
  final_score: 'AI总分',
  confidence_level: '置信度',
  action_hint: '动作提示',
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
  return name || symbol || '暂无重点标的'
}

function compactActionOutput(output: string | undefined): string {
  if (!output) {
    return '暂无执行输出'
  }
  return output.split(/\r?\n/).find((line) => line.trim())?.trim() ?? '暂无执行输出'
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
  const homeQuery = useQuery({
    queryKey: homePageClient.queryKey(),
    queryFn: () => apiGet<HomePayload>(homePageClient.path()),
    refetchInterval: 15_000,
  })

  const payload = homeQuery.data
  const service = payload?.service ?? {}
  const realtimeSnapshot = (service.realtime_snapshot as JsonRecord | undefined) ?? {}
  const overview = payload?.overview
  const watchlist = payload?.watchlist
  const candidates = payload?.candidates
  const aiReview = payload?.aiReview
  const alerts = payload?.alerts ?? []
  const focusWatchRecord = watchlist?.focusRecord ?? {}
  const focusCandidateRecord = aiReview?.focusRecord ?? candidates?.focusRecord ?? {}
  const quickActions = useMemo(() => buildQuickActions(bootstrap), [bootstrap])
  const failedSymbols = normalizeRealtimeFailedSymbols(realtimeSnapshot.failed_symbols)
  const realtimeSource = describeRealtimeSource(realtimeSnapshot.source)
  const realtimeCoverage = formatRealtimeCoverage(realtimeSnapshot.requested_symbol_count, realtimeSnapshot.success_symbol_count)

  const watchlistCellRenderers = useMemo(
    () => ({
      name: (row: JsonRecord) => (
        <EntityCell
          title={String(row.name ?? '-')}
          subtitle={String(row.ts_code ?? '')}
          meta={String(row.industry ?? '')}
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

  return (
    <div className="page-stack">
      <Panel
        title="主操作台"
        subtitle="只保留今天要看和要做的摘要。"
        tone="warm"
        className="panel--summary-surface"
      >
        <QueryNotice isLoading={homeQuery.isLoading} error={homeQuery.error} />

        <SectionBlock title="今日操作结论" description="先确认状态和入口。">
          <SpotlightCard
            title={authenticated ? `欢迎回来，${currentUserLabel ?? '当前用户'}` : '当前处于只读模式'}
            meta="主操作入口"
            subtitle="默认显示最新可用数据；主动动作才会刷新结果。"
            metrics={[
              { label: '观察池数量', value: watchlist?.overview?.totalCount ?? 0 },
              { label: 'AI 观察入池', value: watchlist?.overview?.overlayCount ?? 0 },
              { label: '最新推理入池', value: watchlist?.overview?.inferenceOverlayCount ?? 0 },
              { label: '快照覆盖率', value: realtimeCoverage, tone: failedSymbols.length ? 'warn' : 'good' },
              {
                label: '最佳年化模型',
                value: String(overview?.bestAnnualized?.model ?? '-').toUpperCase(),
                helper: formatPercent(overview?.bestAnnualized?.daily_portfolio_annualized_return),
              },
            ]}
            actions={
              <div className="spotlight-card__actions">
                {quickActions.map((action, index) => (
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
                  打开高级工作台
                </button>
              </div>
            }
          />
        </SectionBlock>

        <ContextStrip items={contextItems} />

        {alerts.length ? (
          <SectionBlock title="今日提醒" description="提醒压成短条，避免首屏堆满说明。" tone="muted" collapsible defaultExpanded={false}>
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

      <div className="split-layout">
        <Panel
          title="重点持仓与观察"
          subtitle="先看总列表，再看当前优先标的。"
          tone="calm"
          className="panel--summary-surface"
          actions={
            <button type="button" className="button button--ghost button--small" onClick={() => navigate('/watchlist')}>
              打开持仓页
            </button>
          }
        >
          <SectionBlock title="观察池总表" description="先排优先级。">
            <DataTable
              rows={watchlist?.records ?? []}
              columns={WATCHLIST_COLUMNS}
              columnLabels={WATCHLIST_COLUMN_LABELS}
              storageKey="home-watchlist"
              loading={homeQuery.isLoading}
              emptyText="暂无观察池数据"
              stickyFirstColumn
              cellRenderers={watchlistCellRenderers}
            />
          </SectionBlock>

          <SectionBlock title="当前优先标的" description="再看单票摘要。">
            <SpotlightCard
              title={buildRecordTitle(focusWatchRecord)}
              meta={String(focusWatchRecord.entry_group ?? '观察对象')}
              subtitle={String(focusWatchRecord.premarket_plan ?? focusWatchRecord.action_brief ?? '先去持仓页查看完整关键价位。')}
              metrics={[
                { label: '参考价', value: formatValue(focusWatchRecord.mark_price ?? '-') },
                { label: '最新价', value: formatValue(focusWatchRecord.realtime_price ?? focusWatchRecord.mark_price ?? '-') },
                {
                  label: '盘中涨跌',
                  value: formatPercent(focusWatchRecord.realtime_pct_chg ?? '-'),
                  tone:
                    typeof focusWatchRecord.realtime_pct_chg === 'number' && Number(focusWatchRecord.realtime_pct_chg) > 0
                      ? 'good'
                      : 'warn',
                },
                { label: '最新推理排名', value: formatValue(focusWatchRecord.inference_ensemble_rank ?? '-') },
              ]}
              actions={
                String(focusWatchRecord.ts_code ?? '').trim() ? (
                  <div className="spotlight-card__actions">
                    <button type="button" className="button button--primary" onClick={() => navigate(buildWatchlistPath(String(focusWatchRecord.ts_code)))}>
                      查看持仓详情
                    </button>
                    <button type="button" className="button button--ghost" onClick={() => navigate(buildAiReviewPath(String(focusWatchRecord.ts_code)))}>
                      查看 AI 研判
                    </button>
                  </div>
                ) : null
              }
            />
          </SectionBlock>
        </Panel>

        <Panel
          title="候选与 AI 推理"
          subtitle="先用总列表定重点，再看当前焦点。"
          tone="calm"
          className="panel--summary-surface"
          actions={
            <div className="inline-actions inline-actions--compact">
              <button type="button" className="button button--ghost button--small" onClick={() => navigate('/candidates')}>
                打开候选页
              </button>
              <button type="button" className="button button--ghost button--small" onClick={() => navigate('/ai-review')}>
                打开 AI 研判页
              </button>
            </div>
          }
        >
          <SectionBlock title="模型候选总表" description="先看候选列表。">
            <DataTable
              rows={candidates?.records ?? []}
              columns={CANDIDATE_COLUMNS}
              columnLabels={CANDIDATE_COLUMN_LABELS}
              storageKey="home-candidates"
              loading={homeQuery.isLoading}
              emptyText="暂无模型候选"
              stickyFirstColumn
              cellRenderers={candidateCellRenderers}
            />
          </SectionBlock>

          <SectionBlock title="当前候选焦点" description="再看当前最值得切入的标的。">
            <SpotlightCard
              title={buildRecordTitle(focusCandidateRecord)}
              meta={String(focusCandidateRecord.industry_display ?? focusCandidateRecord.industry ?? '候选对象')}
              subtitle={String(focusCandidateRecord.action_hint ?? focusCandidateRecord.thesis_summary ?? '先看候选和 AI 推理总表，再决定是否下钻。')}
              metrics={[
                { label: 'AI 总分', value: formatValue(focusCandidateRecord.final_score ?? '-') },
                { label: '模型分数', value: formatValue(focusCandidateRecord.score ?? focusCandidateRecord.quant_score ?? '-') },
                { label: '分位', value: formatPercent(focusCandidateRecord.rank_pct ?? '-') },
                { label: '未来收益', value: formatPercent(focusCandidateRecord.ret_t1_t10 ?? '-') },
              ]}
              actions={
                String(focusCandidateRecord.ts_code ?? '').trim() ? (
                  <div className="spotlight-card__actions">
                    <button type="button" className="button button--primary" onClick={() => navigate(buildCandidatesPath(String(focusCandidateRecord.ts_code)))}>
                      查看候选详情
                    </button>
                    <button type="button" className="button button--ghost" onClick={() => navigate(buildAiReviewPath(String(focusCandidateRecord.ts_code)))}>
                      查看推理详情
                    </button>
                  </div>
                ) : null
              }
            />
          </SectionBlock>

          <SectionBlock title="AI 最新推理总表" description="补充列表，默认收起。" tone="muted" collapsible defaultExpanded={false}>
            <DataTable
              rows={aiReview?.inferenceRecords ?? []}
              columns={INFERENCE_COLUMNS}
              columnLabels={INFERENCE_COLUMN_LABELS}
              storageKey="home-ai-inference"
              loading={homeQuery.isLoading}
              emptyText="暂无 AI 推理候选"
              stickyFirstColumn
              cellRenderers={candidateCellRenderers}
            />
          </SectionBlock>
        </Panel>
      </div>

      <div className="split-layout">
        <SupportPanel title="系统与行情" subtitle="快照和系统状态后置。">
          <SectionBlock title="运行摘要" description="只保留关键状态。">
            <PropertyGrid
              items={[
                { label: '快照类型', value: String(realtimeSnapshot.snapshot_label_display ?? '暂无快照') },
                { label: '快照时间', value: formatDateTime(realtimeSnapshot.fetched_at) },
                { label: '行情来源', value: realtimeSource.label },
                { label: '覆盖率', value: realtimeCoverage, tone: failedSymbols.length ? 'warn' : 'good' },
              ]}
            />
          </SectionBlock>

          <SectionBlock title="支持信息" description="失败股票和参数摘要后置。" tone="muted" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                {
                  label: '失败股票',
                  value: failedSymbols.length ? failedSymbols.join(' / ') : '无',
                  span: 'double',
                  tone: failedSymbols.length ? 'warn' : 'good',
                },
                { label: '参数摘要', value: payload?.configSummaryText ?? '暂无参数摘要', span: 'double' },
              ]}
            />
          </SectionBlock>
        </SupportPanel>

        <SupportPanel title="最近动作" subtitle="结果摘要前置，完整输出后置。">
          {latestAction ? (
            <div className="section-stack">
              <SectionBlock title={latestAction.label ?? latestAction.actionName} description="这里只看状态和摘要。">
                <SpotlightCard
                  title={latestAction.ok ? '执行成功' : '执行失败'}
                  meta={latestAction.actionName}
                  subtitle={compactActionOutput(latestAction.output)}
                  metrics={[
                    { label: '动作名称', value: latestAction.actionName },
                    { label: '状态', value: latestAction.ok ? '成功' : '失败', tone: latestAction.ok ? 'good' : 'warn' },
                  ]}
                />
              </SectionBlock>

              <SectionBlock title="完整输出" description="排查时再展开。" tone="muted" collapsible defaultExpanded={false}>
                <pre className="log-block">{latestAction.output || '无输出'}</pre>
              </SectionBlock>
            </div>
          ) : (
            <div className="empty-state">还没有执行记录，可先从首页快捷动作开始。</div>
          )}
        </SupportPanel>
      </div>
    </div>
  )
}
