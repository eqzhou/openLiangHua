import { Badge } from './Badge'
import { PropertyGrid } from './PropertyGrid'
import { formatDateTime } from '../lib/format'
import { describeRealtimeSnapshotMode, describeRealtimeSource, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import type { JsonRecord } from '../types/api'

type BannerTone = 'info' | 'success' | 'warn' | 'error' | 'loading'

interface RealtimeStatusBannerProps {
  status?: JsonRecord
  isRefreshing?: boolean
  error?: unknown
  onRefresh?: () => void
  onRetryFailed?: () => void
  disabled?: boolean
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim()
  }
  return '请稍后重试。'
}

function bannerToneFromState(
  isRefreshing: boolean,
  error: unknown,
  requestedCount: number,
  successCount: number,
  hasFallback: boolean,
): BannerTone {
  if (isRefreshing) {
    return 'loading'
  }
  if (error) {
    return 'error'
  }
  if (requestedCount === 0) {
    return 'info'
  }
  if (successCount === 0) {
    return 'error'
  }
  if (hasFallback || successCount < requestedCount) {
    return 'warn'
  }
  return 'success'
}

function bannerMessage(
  isRefreshing: boolean,
  error: unknown,
  requestedCount: number,
  successCount: number,
  failedCount: number,
  sourceLabel: string,
  hasFallback: boolean,
  snapshotLabel: string,
  servedFrom: string,
): string {
  if (isRefreshing) {
    return '正在抓取当前工作区的最新盘中行情。'
  }
  if (error) {
    return `实时刷新失败：${toErrorMessage(error)}`
  }
  if (requestedCount === 0) {
    return '页面会优先显示最近一份可复用的行情快照；需要向外部源拉取最新行情时，请点击手动刷新。'
  }
  if (successCount === 0) {
    return '本次刷新没有拿到有效实时行情。'
  }
  if (String(servedFrom) === 'database') {
    return `当前显示的是 ${snapshotLabel}，不再重复请求外部行情接口。`
  }
  if (String(servedFrom) === 'database-fallback') {
    return `外部行情暂不可用，当前回退展示的是 ${snapshotLabel}。`
  }
  if (snapshotLabel.includes('盘后')) {
    return `已抓取 ${snapshotLabel}，后续同日请求会直接复用这份结果。`
  }
  if (hasFallback) {
    return `主源不可用，当前显示为 ${sourceLabel} 的降级结果。`
  }
  if (failedCount > 0) {
    return `刷新已完成，但还有 ${failedCount} 只股票需要重试。`
  }
  return '实时行情已通过主源刷新完成。'
}

export function RealtimeStatusBanner({
  status = {},
  isRefreshing = false,
  error,
  onRefresh,
  onRetryFailed,
  disabled = false,
}: RealtimeStatusBannerProps) {
  const requestedCount = Number(status.requested_symbol_count ?? 0)
  const successCount = Number(status.success_symbol_count ?? 0)
  const failedSymbols = normalizeRealtimeFailedSymbols(status.failed_symbols)
  const source = describeRealtimeSource(status.source)
  const snapshotMode = describeRealtimeSnapshotMode(status.snapshot_bucket, status.served_from)
  const snapshotLabel = String(status.snapshot_label_display ?? '').trim() || snapshotMode.label
  const fallbackMode = source.isFallback || (requestedCount > 0 && successCount < requestedCount)
  const tone = bannerToneFromState(isRefreshing, error, requestedCount, successCount, fallbackMode)
  const runtimeError = String(status.error_message ?? '').trim()
  const message = bannerMessage(
    isRefreshing,
    error,
    requestedCount,
    successCount,
    failedSymbols.length,
    source.label,
    fallbackMode,
    snapshotLabel,
    String(status.served_from ?? ''),
  )

  return (
    <section className={`realtime-banner realtime-banner--${tone}`} role="status" aria-live="polite">
      <div className="realtime-banner__header">
        <div className="realtime-banner__copy">
          <p className="realtime-banner__eyebrow">实时监控</p>
          <h3 className="realtime-banner__title">实时行情状态</h3>
          <p className="realtime-banner__message">{message}</p>
        </div>
        <div className="realtime-banner__actions">
          <button type="button" className="button button--primary" onClick={onRefresh} disabled={disabled || isRefreshing}>
            {isRefreshing ? '刷新中...' : '手动拉取最新行情'}
          </button>
          {failedSymbols.length ? (
            <button type="button" className="button button--ghost" onClick={onRetryFailed ?? onRefresh} disabled={disabled || isRefreshing}>
              重试失败股票
            </button>
          ) : null}
        </div>
      </div>

      <div className="realtime-banner__badges">
        <Badge tone={source.badgeTone}>{source.mode}</Badge>
        <Badge tone={fallbackMode ? 'warn' : 'brand'}>{source.label}</Badge>
        <Badge tone={snapshotMode.tone}>{snapshotLabel}</Badge>
        <Badge tone={requestedCount > 0 && successCount === requestedCount ? 'good' : 'default'}>
          {requestedCount > 0 ? `覆盖 ${successCount} / ${requestedCount}` : '未刷新'}
        </Badge>
        {failedSymbols.length ? <Badge tone="warn">{`失败 ${failedSymbols.length}`}</Badge> : null}
      </div>

      <PropertyGrid
        columns="triple"
        items={[
          { label: '模式', value: source.mode, tone: fallbackMode ? 'warn' : 'good' },
          { label: '来源', value: source.label },
          { label: '数据形态', value: snapshotLabel },
          { label: '更新时间', value: formatDateTime(status.fetched_at) },
          { label: '覆盖率', value: requestedCount > 0 ? `${successCount} / ${requestedCount}` : '未刷新' },
          { label: '失败股票', value: failedSymbols.length ? failedSymbols.join(' / ') : '无', span: 'double', tone: failedSymbols.length ? 'warn' : 'good' },
          { label: '最近错误', value: error ? toErrorMessage(error) : runtimeError || '无', span: 'double', tone: error || runtimeError ? 'warn' : 'default' },
        ]}
      />
    </section>
  )
}
