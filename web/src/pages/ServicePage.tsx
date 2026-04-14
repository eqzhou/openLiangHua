import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiGet } from '../api/client'
import { Badge } from '../components/Badge'
import { DataTable } from '../components/DataTable'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { QueryNotice } from '../components/QueryNotice'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { servicePageClient } from '../facades/dashboardPageClient'
import { formatDateTime, formatValue, recordToFieldRows } from '../lib/format'
import { describeRealtimeSnapshotMode, describeRealtimeSource, formatRealtimeCoverage, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import type { JsonRecord, ServicePayload } from '../types/api'

const FIELD_COLUMNS = ['field', 'value']

export function ServicePage() {
  const serviceQuery = useQuery({
    queryKey: servicePageClient.queryKey(),
    queryFn: () => apiGet<ServicePayload>(servicePageClient.path()),
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

  return (
    <div className="page-stack">
      <Panel
        title="页面服务"
        subtitle="先看健康与快照，再决定是否排查。"
        tone="warm"
        className="panel--summary-surface"
      >
        <QueryNotice isLoading={serviceQuery.isLoading} error={serviceQuery.error} />

        <SectionBlock title="运行结论" description="首屏只保留服务健康、快照状态和最近更新时间。">
          <SpotlightCard
            title={String(payload.status_label_display ?? '未知')}
            meta="前端服务状态"
            subtitle={`最近更新时间 ${formatDateTime(lastStatus.last_update)}，快照状态 ${String(realtimeSnapshot.snapshot_label_display ?? '暂无快照')}。`}
            badges={[
              { label: `服务 ${String(payload.status_label_display ?? '未知')}`, tone: statusTone },
              { label: payload.listener_present ? '8501 已监听' : '8501 未监听', tone: payload.listener_present ? 'good' : 'warn' },
              { label: String(realtimeSnapshot.snapshot_label_display ?? '暂无快照'), tone: snapshotTone },
              {
                label: payload.listener_matches_streamlit_pid ? '监听与页面进程一致' : '监听与页面进程不一致',
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
      </Panel>

      <Panel title="关键状态字段" subtitle="状态总表前置，日志和支持解释后置。" tone="calm" className="panel--table-surface">
        <DataTable rows={serviceFieldRows} columns={FIELD_COLUMNS} storageKey="service-fields" emptyText="暂无服务状态" stickyFirstColumn />
      </Panel>

      <div className="split-layout">
        <SupportPanel title="快照支持" subtitle="快照解释统一后置。">
          <SectionBlock title="快照摘要" description="这里只保留支持性上下文。" tone="muted" collapsible defaultExpanded={false}>
            <div className="badge-row">
              <Badge tone={snapshotTone}>{String(realtimeSnapshot.snapshot_label_display ?? '暂无快照')}</Badge>
              <Badge tone="brand">{snapshotSource.label}</Badge>
              <Badge tone={snapshotFailedSymbols.length ? 'warn' : 'good'}>{snapshotCoverage}</Badge>
            </div>
            <PropertyGrid
              items={[
                { label: '交易日期', value: formatValue(realtimeSnapshot.trade_date) },
                { label: '抓取时间', value: formatDateTime(realtimeSnapshot.fetched_at) },
                { label: '数据入口', value: formatValue(realtimeSnapshot.served_from) },
                { label: '快照模式', value: snapshotMode.label },
                { label: '是否当天', value: formatValue(realtimeSnapshot.is_today), tone: realtimeSnapshot.is_today ? 'good' : 'default' },
                {
                  label: '失败股票',
                  value: snapshotFailedSymbols.length ? snapshotFailedSymbols.join(' / ') : '无',
                  span: 'double',
                  tone: snapshotFailedSymbols.length ? 'warn' : 'good',
                },
                {
                  label: '最近错误',
                  value: formatValue(realtimeSnapshot.error_message) === '-' ? '无' : formatValue(realtimeSnapshot.error_message),
                  span: 'double',
                  tone: formatValue(realtimeSnapshot.error_message) === '-' ? 'default' : 'warn',
                },
              ]}
            />
          </SectionBlock>
        </SupportPanel>

        <SupportPanel title="运行支持" subtitle="文件陈旧、端口一致性和最近状态后置。">
          <SectionBlock title="健康检查补充" description="需要排查时再看这些细节字段。" tone="muted" collapsible defaultExpanded={false}>
            <PropertyGrid
              items={[
                { label: '守护进程运行中', value: formatValue(payload.supervisor_running) },
                { label: '页面进程运行中', value: formatValue(payload.streamlit_running) },
                { label: '监听端口', value: formatValue(payload.listener_pids), span: 'double' },
                { label: '状态文件陈旧', value: formatValue(payload.stale_status), tone: payload.stale_status ? 'warn' : 'good' },
                { label: '守护 PID 陈旧', value: formatValue(payload.stale_supervisor_pid), tone: payload.stale_supervisor_pid ? 'warn' : 'good' },
                { label: '页面 PID 陈旧', value: formatValue(payload.stale_streamlit_pid), tone: payload.stale_streamlit_pid ? 'warn' : 'good' },
                { label: '最近状态', value: formatValue(lastStatus.state) },
                { label: '最近更新时间', value: formatDateTime(lastStatus.last_update) },
              ]}
            />
          </SectionBlock>
        </SupportPanel>
      </div>

      <SupportPanel title="运行日志" subtitle="日志后置，只在排查时展开。">
        <div className="split-layout">
          <SectionBlock title="标准输出" description="最近一次启动和运行输出。" tone="muted" collapsible defaultExpanded={false}>
            <pre className="log-block">{String(payload.out_log_tail ?? '') || '暂无内容'}</pre>
          </SectionBlock>
          <SectionBlock title="错误输出" description="最近异常和错误栈信息。" tone="muted" collapsible defaultExpanded={false}>
            <pre className="log-block">{String(payload.err_log_tail ?? '') || '暂无内容'}</pre>
          </SectionBlock>
        </div>
      </SupportPanel>
    </div>
  )
}
