import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { apiGet, apiPost } from '../api/client'
import { Badge } from '../components/Badge'
import { MetricCard } from '../components/MetricCard'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { useToast } from '../components/ToastProvider'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { dataManagementClient } from '../facades/dashboardPageClient'
import { formatDateTime, formatValue } from '../lib/format'
import type { ActionResult, DataArtifactStatus, DataManagementPayload } from '../types/api'

interface DataManagementPageProps {
  authenticated?: boolean
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim()
  }
  return '操作失败，请稍后再试。'
}

function buildArtifactMetrics(status: DataArtifactStatus) {
  return [
    { label: '最新日期', value: status.latestTradeDate ?? '-' },
    { label: '行数', value: status.rowCount ?? 0 },
    { label: '股票数', value: status.symbolCount ?? 0 },
    { label: '更新时间', value: formatDateTime(status.updatedAt) },
  ]
}

export function DataManagementPage({ authenticated = false }: DataManagementPageProps) {
  const queryClient = useQueryClient()
  const { pushToast } = useToast()
  const [endDate, setEndDate] = useState('')
  const [latestAction, setLatestAction] = useState<ActionResult | null>(null)

  const dataQuery = useQuery({
    queryKey: dataManagementClient.queryKey(),
    queryFn: () => apiGet<DataManagementPayload>(dataManagementClient.path()),
    refetchInterval: 15_000,
  })

  const incrementalRefreshMutation = useMutation({
    mutationFn: () =>
      apiPost<ActionResult>(dataManagementClient.incrementalActionPath(), {
        target_source: payload?.targetSource ?? 'tushare',
        end_date: endDate || undefined,
      }),
    onSuccess: (payload) => {
      setLatestAction(payload)
      void queryClient.invalidateQueries()
      pushToast({
        tone: payload.ok ? 'success' : 'error',
        title: payload.label ?? 'Tushare 增量刷新',
        description: payload.output,
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: 'Tushare 增量刷新失败',
        description: toErrorMessage(error),
      })
    },
  })

  const fullRefreshMutation = useMutation({
    mutationFn: () =>
      apiPost<ActionResult>(dataManagementClient.fullRefreshActionPath(), {
        target_source: payload?.targetSource ?? 'tushare',
        end_date: endDate || undefined,
      }),
    onSuccess: (payload) => {
      setLatestAction(payload)
      void queryClient.invalidateQueries()
      pushToast({
        tone: payload.ok ? 'success' : 'error',
        title: payload.label ?? 'Tushare 全流程刷新',
        description: payload.output,
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: 'Tushare 全流程刷新失败',
        description: toErrorMessage(error),
      })
    },
  })

  const payload = useMemo(() => dataQuery.data, [dataQuery.data])
  const dailyBar = payload?.dailyBar ?? { exists: false, rowCount: 0, symbolCount: 0 }
  const featurePanel = payload?.featurePanel ?? { exists: false, rowCount: 0, symbolCount: 0 }
  const labelPanel = payload?.labelPanel ?? { exists: false, rowCount: 0, symbolCount: 0 }
  const datasetSummary = (payload?.datasetSummary ?? {}) as Record<string, unknown>
  const tokenConfigured = payload?.tokenConfigured ?? null
  const hasToken = tokenConfigured === true
  const tokenStatusLabel = tokenConfigured === null ? '登录后可见' : hasToken ? '已配置' : '未配置'
  const targetSource = payload?.targetSource ?? 'tushare'
  const pendingAction = incrementalRefreshMutation.isPending ? 'incremental' : fullRefreshMutation.isPending ? 'full' : null

  const heroBadges = (
    <>
      <Badge tone={tokenConfigured === null ? 'default' : hasToken ? 'good' : 'warn'}>{`Token ${tokenStatusLabel}`}</Badge>
      <Badge tone="brand">{`目标源 ${payload?.targetSource ?? 'akshare'}`}</Badge>
      <Badge tone={authenticated ? 'good' : 'default'}>{authenticated ? '可执行' : '只读'}</Badge>
    </>
  )

  return (
    <div className="page-stack">
      <WorkspaceHero title="数据管理" badges={heroBadges} />

      <div className="metric-grid metric-grid--four">
        <MetricCard
          label="Token 状态"
          value={tokenStatusLabel}
          tone={tokenConfigured === null ? 'default' : hasToken ? 'good' : 'warn'}
          helper={authenticated ? (payload?.envFileExists ? '检测到 .env' : '未发现 .env') : '登录后显示'}
        />
        <MetricCard label="日线最新日期" value={dailyBar.latestTradeDate ?? '-'} tone={dailyBar.exists ? 'good' : 'warn'} />
        <MetricCard label="特征最新日期" value={featurePanel.latestTradeDate ?? '-'} tone={featurePanel.exists ? 'good' : 'warn'} />
        <MetricCard label="标签最新日期" value={labelPanel.latestTradeDate ?? '-'} tone={labelPanel.exists ? 'good' : 'warn'} />
      </div>

      <Panel title="状态" tone="warm" className="panel--summary-surface">
        <QueryNotice isLoading={dataQuery.isLoading} error={dataQuery.error} />
        {!authenticated ? <div className="query-notice query-notice--info">当前只读，可查看数据状态；如需执行刷新，请先登录。</div> : null}
        {authenticated && tokenConfigured === false ? <div className="query-notice query-notice--error">当前未检测到 `TUSHARE_TOKEN`，请先更新 `.env` 或运行环境变量。</div> : null}

        <SectionBlock title="当前数据结论">
          <SpotlightCard
            title="本地研究数据状态"
            meta={`目标源 ${targetSource}`}
            subtitle={`日线最新日期 ${dailyBar.latestTradeDate ?? '-'}，特征最新日期 ${featurePanel.latestTradeDate ?? '-'}，标签最新日期 ${labelPanel.latestTradeDate ?? '-'}`}
            metrics={[
              { label: '日线行数', value: dailyBar.rowCount ?? 0, tone: dailyBar.exists ? 'good' : 'warn' },
              { label: '特征行数', value: featurePanel.rowCount ?? 0, tone: featurePanel.exists ? 'good' : 'warn' },
              { label: '标签行数', value: labelPanel.rowCount ?? 0, tone: labelPanel.exists ? 'good' : 'warn' },
              { label: '缓存股票数', value: formatValue(datasetSummary.cached_symbols ?? 0) },
            ]}
          />
        </SectionBlock>

        <SectionBlock title="环境信息" tone="muted">
          <PropertyGrid
            items={[
              { label: '.env 路径', value: payload?.envPath ?? '登录后可见', span: 'double' },
              { label: '.env 存在', value: payload?.envFileExists ?? false, tone: payload?.envFileExists ? 'good' : 'warn' },
              { label: 'Tushare Token', value: tokenStatusLabel, tone: tokenConfigured === null ? 'default' : hasToken ? 'good' : 'warn' },
              { label: '目标数据源', value: payload?.targetSource ?? 'akshare' },
              { label: '今日日期', value: payload?.today ?? '-' },
              { label: '研究区间', value: `${formatValue(datasetSummary.date_min)} 至 ${formatValue(datasetSummary.date_max)}`, span: 'double' },
            ]}
          />
        </SectionBlock>
      </Panel>

      <Panel title="执行" tone="calm" className="panel--table-surface">
        <div className="split-layout split-layout--workspace">
          <SectionBlock title="刷新控制">
            <form
              className="config-form"
              onSubmit={(event) => {
                event.preventDefault()
              }}
            >
              <label>
                <span>目标数据源</span>
                <input value={targetSource} disabled />
              </label>
              <label>
                <span>截止日期</span>
                <input type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} placeholder={payload?.today ?? ''} />
              </label>
              <div className="inline-actions inline-actions--compact">
                <button
                  type="button"
                  className="button button--primary"
                  disabled={!authenticated || tokenConfigured !== true || pendingAction !== null}
                  onClick={() => incrementalRefreshMutation.mutate()}
                >
                  {pendingAction === 'incremental' ? '增量刷新中...' : 'Tushare 增量刷新日线'}
                </button>
                <button
                  type="button"
                  className="button"
                  disabled={!authenticated || tokenConfigured !== true || pendingAction !== null}
                  onClick={() => fullRefreshMutation.mutate()}
                >
                  {pendingAction === 'full' ? '全流程刷新中...' : 'Tushare 全流程刷新'}
                </button>
              </div>
            </form>
          </SectionBlock>

          <SectionBlock title="最近一次执行结果">
            {latestAction ? (
              <SpotlightCard
                title={latestAction.ok ? '执行完成' : '执行失败'}
                meta={latestAction.label ?? latestAction.actionName}
                subtitle={latestAction.output}
                metrics={[
                  { label: '动作名称', value: latestAction.actionName },
                  { label: '执行状态', value: latestAction.ok ? '完成' : '失败', tone: latestAction.ok ? 'good' : 'warn' },
                ]}
              />
            ) : (
              <div className="empty-state">还没有执行过数据刷新动作。</div>
            )}
          </SectionBlock>
        </div>
      </Panel>

      <div className="split-layout">
        <SupportPanel title="数据文件">
          <SectionBlock title="日线面板" tone="muted">
            <PropertyGrid items={buildArtifactMetrics(dailyBar)} />
          </SectionBlock>
          <SectionBlock title="特征面板" tone="muted">
            <PropertyGrid items={buildArtifactMetrics(featurePanel)} />
          </SectionBlock>
          <SectionBlock title="标签面板" tone="muted">
            <PropertyGrid items={buildArtifactMetrics(labelPanel)} />
          </SectionBlock>
        </SupportPanel>

        <SupportPanel title="脚本">
          <SectionBlock title="手工命令" tone="muted">
            <pre className="log-block">{`powershell -ExecutionPolicy Bypass -File .\\scripts\\refresh_daily_bar_tushare.ps1 -TargetSource ${targetSource}${endDate ? ` -EndDate ${endDate}` : ''}`}</pre>
            <pre className="log-block">{`powershell -ExecutionPolicy Bypass -File .\\scripts\\refresh_full_pipeline_tushare.ps1 -TargetSource ${targetSource}${endDate ? ` -EndDate ${endDate}` : ''}`}</pre>
          </SectionBlock>
          <SectionBlock title="脚本路径" tone="muted" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                { label: '增量脚本', value: payload?.scripts.incremental ?? '-', span: 'double' },
                { label: '全流程脚本', value: payload?.scripts.fullRefresh ?? '-', span: 'double' },
              ]}
            />
          </SectionBlock>
        </SupportPanel>
      </div>
    </div>
  )
}
