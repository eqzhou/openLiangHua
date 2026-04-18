import { useMemo } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { apiGet, apiPost } from '../api/client'
import { Badge } from '../components/Badge'
import { DataTable } from '../components/DataTable'
import { MetricCard } from '../components/MetricCard'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { RealtimeStatusBanner } from '../components/RealtimeStatusBanner'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { useToast } from '../components/ToastProvider'
import { WorkspaceHero } from '../components/WorkspaceHero'
import { realtimeRefreshClient, servicePageClient } from '../facades/dashboardPageClient'
import { formatDateTime, formatValue, recordToFieldRows } from '../lib/format'
import { describeRealtimeSnapshotMode, describeRealtimeSource, formatRealtimeCoverage, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import type { JsonRecord, RealtimeRefreshPayload, ServicePayload } from '../types/api'

const FIELD_COLUMNS = ['field', 'value']

interface ServicePageProps {
  authenticated?: boolean
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim()
  }
  return '操作失败，请稍后再试。'
}

export function ServicePage({ authenticated = false }: ServicePageProps) {
  const queryClient = useQueryClient()
  const { pushToast } = useToast()
  const serviceQuery = useQuery({
    queryKey: servicePageClient.queryKey(),
    queryFn: () => apiGet<ServicePayload>(servicePageClient.path()),
    refetchInterval: 15_000,
  })

  const refreshRealtimeMutation = useMutation({
    mutationFn: () => apiPost<RealtimeRefreshPayload>(realtimeRefreshClient.path()),
    onSuccess: (payload) => {
      void queryClient.invalidateQueries()
      const realtime = payload.realtimeStatus ?? {}
      const successCount = Number(realtime.success_symbol_count ?? 0)
      const requestedCount = Number(realtime.requested_symbol_count ?? 0)
      const sourceLabel = describeRealtimeSource(realtime.source).label
      const snapshotLabel = String(realtime.snapshot_label_display ?? '最新行情')
      pushToast({
        tone: successCount > 0 ? 'success' : 'error',
        title: '实时快照已更新',
        description: `${snapshotLabel}，覆盖 ${successCount} / ${requestedCount}，来源 ${sourceLabel}，更新时间 ${formatDateTime(realtime.fetched_at)}。`,
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '实时快照更新失败',
        description: toErrorMessage(error),
      })
    },
  })

  const payload = useMemo(() => serviceQuery.data ?? {}, [serviceQuery.data])
  const lastStatus = (payload.last_status as JsonRecord | undefined) ?? {}
  const realtimeSnapshot = (payload.realtime_snapshot as JsonRecord | undefined) ?? {}
  const statusTone = String(payload.effective_state ?? '').toLowerCase() === 'running' ? 'good' : 'warn'
  const snapshotMode = describeRealtimeSnapshotMode(realtimeSnapshot.snapshot_bucket, realtimeSnapshot.served_from)
  const snapshotSource = describeRealtimeSource(realtimeSnapshot.source)
  const snapshotFailedSymbols = normalizeRealtimeFailedSymbols(realtimeSnapshot.failed_symbols)
  const snapshotCoverage = formatRealtimeCoverage(realtimeSnapshot.requested_symbol_count, realtimeSnapshot.success_symbol_count)
  const snapshotTone =
    realtimeSnapshot.available === true
      ? snapshotMode.tone === 'good'
        ? 'good'
        : snapshotMode.tone === 'warn'
          ? 'warn'
          : 'default'
      : 'default'

  const serviceFieldRows = useMemo(() => {
    const filtered = Object.fromEntries(
      Object.entries(payload).filter(([field]) => !['out_log_tail', 'err_log_tail', 'last_status', 'realtime_snapshot'].includes(field)),
    ) as JsonRecord
    return recordToFieldRows(filtered)
  }, [payload])

  const serviceHeroBadges = (
    <>
      <Badge tone={statusTone}>{`服务 ${String(payload.status_label_display ?? '未知')}`}</Badge>
      <Badge tone={payload.listener_present ? 'good' : 'warn'}>{payload.listener_present ? '监听正常' : '监听异常'}</Badge>
      <Badge tone={snapshotTone}>{String(realtimeSnapshot.snapshot_label_display ?? '暂无快照')}</Badge>
      <Badge tone="brand">{snapshotSource.label}</Badge>
    </>
  )

  return (
    <div className="page-stack">
      <WorkspaceHero
        title="服务"
        badges={serviceHeroBadges}
      />

      <div className="metric-grid metric-grid--four">
        <MetricCard label="页面进程 PID" value={payload.streamlit_pid ?? '-'} />
        <MetricCard label="守护进程 PID" value={payload.supervisor_pid ?? '-'} />
        <MetricCard label="监听端口数" value={(payload.listener_pids as unknown[] | undefined)?.length ?? 0} tone={payload.listener_present ? 'good' : 'warn'} />
        <MetricCard label="重启次数" value={lastStatus.restart_count ?? '-'} tone={Number(lastStatus.restart_count ?? 0) > 0 ? 'warn' : 'default'} />
      </div>

      <Panel title="状态" tone="warm" className="panel--summary-surface">
        <QueryNotice isLoading={serviceQuery.isLoading} error={serviceQuery.error} />
        {!authenticated ? <div className="query-notice query-notice--info">当前只读，可查看最近快照；如需刷新行情，请先登录。</div> : null}

        <SectionBlock title="运行结论">
          <SpotlightCard
            title={String(payload.status_label_display ?? '未知')}
            meta="前端服务状态"
            subtitle={`最近更新时间 ${formatDateTime(lastStatus.last_update)}，快照状态 ${String(realtimeSnapshot.snapshot_label_display ?? '暂无快照')}。`}
            badges={[
              { label: `服务 ${String(payload.status_label_display ?? '未知')}`, tone: statusTone },
              { label: payload.listener_present ? '8501 正常' : '8501 异常', tone: payload.listener_present ? 'good' : 'warn' },
              { label: String(realtimeSnapshot.snapshot_label_display ?? '暂无快照'), tone: snapshotTone },
              {
                label: payload.listener_matches_streamlit_pid ? '进程一致' : '进程异常',
                tone: payload.listener_matches_streamlit_pid ? 'good' : 'warn',
              },
            ]}
            metrics={[
              { label: '页面进程 PID', value: payload.streamlit_pid ?? '-' },
              { label: '守护进程 PID', value: payload.supervisor_pid ?? '-' },
              { label: '监听端口数', value: (payload.listener_pids as unknown[] | undefined)?.length ?? 0 },
              { label: '重启次数', value: lastStatus.restart_count ?? '-', tone: Number(lastStatus.restart_count ?? 0) > 0 ? 'warn' : 'default' },
            ]}
          />
        </SectionBlock>

        <SectionBlock title="盘中快照刷新">
          <RealtimeStatusBanner
            title="实时快照刷新"
            status={realtimeSnapshot}
            isRefreshing={refreshRealtimeMutation.isPending}
            error={refreshRealtimeMutation.isError ? refreshRealtimeMutation.error : undefined}
            onRefresh={() => refreshRealtimeMutation.mutate()}
            onRetryFailed={() => refreshRealtimeMutation.mutate()}
            disabled={!authenticated || refreshRealtimeMutation.isPending}
          />
        </SectionBlock>
      </Panel>

      <Panel title="字段" tone="calm" className="panel--table-surface">
        <DataTable rows={serviceFieldRows} columns={FIELD_COLUMNS} storageKey="service-fields" emptyText="暂无状态" stickyFirstColumn />
      </Panel>

      <div className="split-layout">
        <SupportPanel title="快照">
          <SectionBlock title="快照概览" tone="muted" collapsible defaultExpanded={false}>
            <div className="badge-row">
              <Badge tone={snapshotTone}>{String(realtimeSnapshot.snapshot_label_display ?? '暂无快照')}</Badge>
              <Badge tone="brand">{snapshotSource.label}</Badge>
              <Badge tone={snapshotFailedSymbols.length ? 'warn' : 'good'}>{snapshotCoverage}</Badge>
            </div>
            <PropertyGrid
              items={[
                { label: '交易日期', value: formatValue(realtimeSnapshot.trade_date) },
                { label: '抓取时间', value: formatDateTime(realtimeSnapshot.fetched_at) },
                { label: '数据来源', value: formatValue(realtimeSnapshot.served_from) },
                { label: '快照状态', value: snapshotMode.label },
                { label: '是否当天', value: formatValue(realtimeSnapshot.is_today), tone: realtimeSnapshot.is_today ? 'good' : 'default' },
                {
                  label: '失败股票',
                  value: snapshotFailedSymbols.length ? snapshotFailedSymbols.join(' / ') : '暂无',
                  span: 'double',
                  tone: snapshotFailedSymbols.length ? 'warn' : 'good',
                },
                {
                  label: '最近错误',
                  value: formatValue(realtimeSnapshot.error_message) === '-' ? '暂无' : formatValue(realtimeSnapshot.error_message),
                  span: 'double',
                  tone: formatValue(realtimeSnapshot.error_message) === '-' ? 'default' : 'warn',
                },
              ]}
            />
          </SectionBlock>
        </SupportPanel>

        <SupportPanel title="运行">
          <SectionBlock title="健康检查概览" tone="muted" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                { label: '守护进程状态', value: formatValue(payload.supervisor_running) },
                { label: '页面进程状态', value: formatValue(payload.streamlit_running) },
                { label: '监听端口', value: formatValue(payload.listener_pids), span: 'double' },
                { label: '状态文件陈旧', value: formatValue(payload.stale_status), tone: payload.stale_status ? 'warn' : 'good' },
                { label: '守护 PID 陈旧', value: formatValue(payload.stale_supervisor_pid), tone: payload.stale_supervisor_pid ? 'warn' : 'good' },
                { label: '页面 PID 陈旧', value: formatValue(payload.stale_streamlit_pid), tone: payload.stale_streamlit_pid ? 'warn' : 'good' },
                { label: '服务状态', value: formatValue(lastStatus.state) },
                { label: '最近更新时间', value: formatDateTime(lastStatus.last_update) },
              ]}
            />
          </SectionBlock>
        </SupportPanel>
      </div>

      <SupportPanel title="日志">
        <div className="split-layout">
          <SectionBlock title="标准输出" tone="muted" collapsible defaultExpanded={false}>
            <pre className="log-block">{String(payload.out_log_tail ?? '') || '暂无日志'}</pre>
          </SectionBlock>
          <SectionBlock title="错误输出" tone="muted" collapsible defaultExpanded={false}>
            <pre className="log-block">{String(payload.err_log_tail ?? '') || '暂无日志'}</pre>
          </SectionBlock>
        </div>
      </SupportPanel>
    </div>
  )
}
