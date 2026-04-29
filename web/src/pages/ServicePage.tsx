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
import { SERVICE_REFETCH_INTERVAL_MS } from '../lib/polling'
import { describeRealtimeSnapshotMode, describeRealtimeSource, formatRealtimeCoverage, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import type { JsonRecord, RealtimeRefreshPayload, ServicePayload } from '../types/api'

const FIELD_COLUMNS = ['field', 'value']

interface ServicePageProps {
  authenticated?: boolean
}

function normalizeServiceStatusLabel(label: unknown): string {
  const text = String(label ?? '').trim()
  if (text === '状态脚本不可用') {
    return '本机未启用状态脚本'
  }
  return text || '未知'
}

function serviceStatusDescription(label: unknown): string {
  const text = String(label ?? '').trim()
  if (text === '状态脚本不可用') {
    return '当前环境没有启用 PowerShell 状态脚本，页面运行状态改为根据本地日志与实时快照辅助判断。'
  }
  return ''
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim()
  }
  return '操作失败，请稍后再试。'
}

function asRecord(value: unknown): JsonRecord {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as JsonRecord) : {}
}

function asRecordArray(value: unknown): JsonRecord[] {
  return Array.isArray(value) ? value.filter((item): item is JsonRecord => item !== null && typeof item === 'object' && !Array.isArray(item)) : []
}

function availabilityTone(available: unknown): 'good' | 'warn' {
  return available === true ? 'good' : 'warn'
}

function availabilityLabel(available: unknown): string {
  return available === true ? '可用' : '不可用'
}

function healthPortLabel(record: JsonRecord): string {
  const label = String(record.label ?? '服务')
  const port = record.port ? `:${String(record.port)}` : ''
  return `${label}${port} ${availabilityLabel(record.available)}`
}

function streamlitHealthLabel(streamlitStatus: JsonRecord): string {
  if (streamlitStatus.listener_present === true || String(streamlitStatus.effective_state ?? '').toLowerCase() === 'running') {
    return 'Streamlit 回退可用'
  }
  return 'Streamlit 回退不可用'
}

function streamlitHealthTone(streamlitStatus: JsonRecord): 'good' | 'warn' {
  return streamlitStatus.listener_present === true || String(streamlitStatus.effective_state ?? '').toLowerCase() === 'running' ? 'good' : 'warn'
}

function pm2HealthLabel(pm2Status: JsonRecord, processCount: number): string {
  if (pm2Status.available === true) {
    return `PM2 ${processCount} 个进程`
  }
  return String(pm2Status.message ?? 'PM2 不可用')
}

export function ServicePage({ authenticated = false }: ServicePageProps) {
  const queryClient = useQueryClient()
  const { pushToast } = useToast()
  const serviceQuery = useQuery({
    queryKey: servicePageClient.queryKey(),
    queryFn: () => apiGet<ServicePayload>(servicePageClient.path()),
    refetchInterval: SERVICE_REFETCH_INTERVAL_MS,
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
  const apiStatus = asRecord(payload.apiStatus)
  const webStatus = asRecord(payload.webStatus)
  const pm2Status = asRecord(payload.pm2Status)
  const pm2Processes = asRecordArray(pm2Status.processes)
  const streamlitStatus = asRecord(payload.streamlitStatus)
  const logs = asRecord(payload.logs)
  const statusTone = streamlitHealthTone(streamlitStatus)
  const serviceStatusLabel = normalizeServiceStatusLabel(payload.status_label_display)
  const serviceStatusHint = serviceStatusDescription(payload.status_label_display)
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
      Object.entries(payload).filter(
        ([field]) =>
          ![
            'out_log_tail',
            'err_log_tail',
            'last_status',
            'realtime_snapshot',
            'streamlitStatus',
            'apiStatus',
            'webStatus',
            'pm2Status',
            'logs',
          ].includes(field),
      ),
    ) as JsonRecord
    return recordToFieldRows(filtered)
  }, [payload])

  const serviceHeroBadges = (
    <>
      <Badge tone={availabilityTone(apiStatus.available)}>{healthPortLabel(apiStatus)}</Badge>
      <Badge tone={availabilityTone(webStatus.available)}>{healthPortLabel(webStatus)}</Badge>
      <Badge tone={pm2Status.available === true ? 'good' : 'warn'}>{pm2HealthLabel(pm2Status, pm2Processes.length)}</Badge>
      <Badge tone={statusTone}>{streamlitHealthLabel(streamlitStatus)}</Badge>
      <Badge tone={snapshotTone}>{String(realtimeSnapshot.snapshot_label_display ?? '暂无快照')}</Badge>
    </>
  )

  return (
    <div className="page-stack">
      <WorkspaceHero
        title="服务"
        badges={serviceHeroBadges}
      />

      <div className="metric-grid metric-grid--four">
        <MetricCard label="FastAPI" value={availabilityLabel(apiStatus.available)} helper={`端口 ${formatValue(apiStatus.port ?? 8989)}`} tone={availabilityTone(apiStatus.available)} />
        <MetricCard label="React/Vite" value={availabilityLabel(webStatus.available)} helper={`端口 ${formatValue(webStatus.port ?? 5174)}`} tone={availabilityTone(webStatus.available)} />
        <MetricCard label="PM2 进程" value={pm2Processes.length} helper={String(pm2Status.message ?? '') || '进程清单只读'} tone={pm2Status.available === true ? 'good' : 'warn'} />
        <MetricCard
          label="Streamlit 回退"
          value={streamlitStatus.listener_present === true ? '监听中' : '未监听'}
          helper={`端口 8501 / PID ${formatValue(streamlitStatus.streamlit_pid ?? payload.streamlit_pid)}`}
          tone={streamlitHealthTone(streamlitStatus)}
        />
      </div>

      <Panel title="只读健康大盘" tone="warm" className="panel--summary-surface">
        <QueryNotice isLoading={serviceQuery.isLoading} error={serviceQuery.error} />
        {!authenticated ? <div className="query-notice query-notice--info">当前只读，可查看最近快照；如需刷新行情，请先登录。</div> : null}
        {serviceStatusHint ? <div className="query-notice query-notice--info">{serviceStatusHint}</div> : null}

        <SectionBlock title="运行结论">
          <SpotlightCard
            title="React + FastAPI 运维视图"
            meta="只读"
            subtitle={`最近状态更新时间 ${formatDateTime(lastStatus.last_update)}，快照状态 ${String(realtimeSnapshot.snapshot_label_display ?? '暂无快照')}。`}
            badges={[
              { label: healthPortLabel(apiStatus), tone: availabilityTone(apiStatus.available) },
              { label: healthPortLabel(webStatus), tone: availabilityTone(webStatus.available) },
              { label: pm2HealthLabel(pm2Status, pm2Processes.length), tone: pm2Status.available === true ? 'good' : 'warn' },
              { label: streamlitHealthLabel(streamlitStatus), tone: streamlitHealthTone(streamlitStatus) },
              { label: String(realtimeSnapshot.snapshot_label_display ?? '暂无快照'), tone: snapshotTone },
            ]}
            metrics={[
              { label: 'API 端口', value: apiStatus.port ?? 8989, tone: availabilityTone(apiStatus.available) },
              { label: 'Web 端口', value: webStatus.port ?? 5174, tone: availabilityTone(webStatus.available) },
              { label: 'PM2 进程数', value: pm2Processes.length, tone: pm2Status.available === true ? 'good' : 'warn' },
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
                { label: '快照状态', value: String(realtimeSnapshot.snapshot_label_display ?? snapshotMode.label) },
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
          <SectionBlock title="端口与回退服务" tone="muted" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                { label: 'FastAPI', value: healthPortLabel(apiStatus), tone: availabilityTone(apiStatus.available) },
                { label: 'React/Vite', value: healthPortLabel(webStatus), tone: availabilityTone(webStatus.available) },
                { label: 'Streamlit 状态', value: serviceStatusLabel, tone: streamlitHealthTone(streamlitStatus) },
                { label: 'Streamlit 监听', value: formatValue(streamlitStatus.listener_pids ?? payload.listener_pids), span: 'double', tone: streamlitHealthTone(streamlitStatus) },
                { label: '守护进程状态', value: formatValue(streamlitStatus.supervisor_running ?? payload.supervisor_running) },
                { label: '页面进程状态', value: formatValue(streamlitStatus.streamlit_running ?? payload.streamlit_running) },
                {
                  label: '状态文件陈旧',
                  value: formatValue(streamlitStatus.stale_status ?? payload.stale_status),
                  tone: (streamlitStatus.stale_status ?? payload.stale_status) ? 'warn' : 'good',
                },
                { label: '最近更新时间', value: formatDateTime(lastStatus.last_update) },
              ]}
            />
          </SectionBlock>
          <SectionBlock title="PM2 进程" tone="muted" collapsible defaultExpanded={false}>
            <DataTable
              rows={pm2Processes}
              columns={['name', 'status', 'pid', 'restartTime', 'uptime']}
              storageKey="service-pm2-processes"
              emptyText={String(pm2Status.message ?? '当前环境未返回 PM2 进程。')}
              density="compact"
            />
          </SectionBlock>
        </SupportPanel>
      </div>

      <SupportPanel title="日志">
        <div className="split-layout">
          <SectionBlock title="FastAPI" tone="muted" collapsible defaultExpanded={false}>
            <pre className="log-block">{String(logs.api ?? '') || '暂无 API 日志'}</pre>
          </SectionBlock>
          <SectionBlock title="React/Vite" tone="muted" collapsible defaultExpanded={false}>
            <pre className="log-block">{String(logs.web ?? '') || '暂无 Web 日志'}</pre>
          </SectionBlock>
          <SectionBlock title="Streamlit 标准输出" tone="muted" collapsible defaultExpanded={false}>
            <pre className="log-block">{String(logs.streamlitOut ?? payload.out_log_tail ?? '') || '暂无日志'}</pre>
          </SectionBlock>
          <SectionBlock title="Streamlit 错误输出" tone="muted" collapsible defaultExpanded={false}>
            <pre className="log-block">{String(logs.streamlitErr ?? payload.err_log_tail ?? '') || '暂无日志'}</pre>
          </SectionBlock>
        </div>
      </SupportPanel>
    </div>
  )
}
