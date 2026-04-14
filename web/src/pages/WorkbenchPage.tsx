import { MetricCard } from '../components/MetricCard'
import { Panel } from '../components/Panel'
import { PropertyGrid } from '../components/PropertyGrid'
import { SectionBlock } from '../components/SectionBlock'
import { SpotlightCard } from '../components/SpotlightCard'
import { SupportPanel } from '../components/SupportPanel'
import { formatDateTime } from '../lib/format'
import { describeRealtimeSnapshotMode, describeRealtimeSource, formatRealtimeCoverage, normalizeRealtimeFailedSymbols } from '../lib/realtime'
import type { ActionResult, JsonRecord, ShellPayload } from '../types/api'

interface WorkbenchPageProps {
  shell?: ShellPayload
  latestAction: ActionResult | null
  shellError?: string | null
  shellLoading: boolean
  authenticated: boolean
  currentUserLabel?: string | null
  configSaving: boolean
  actionPendingName: string | null
  clearingCache: boolean
  resettingUiPreferences: boolean
  sharingCurrentView: boolean
  onSaveConfig: (payload: Record<string, unknown>) => void
  onRunAction: (actionName: string) => void
  onClearCache: () => void
  onResetUiPreferences: () => void
  onShareCurrentView: () => void
}

function compactActionOutput(output: string | undefined): string {
  if (!output) {
    return '暂无执行输出'
  }
  return output.split(/\r?\n/).find((line) => line.trim())?.trim() ?? '暂无执行输出'
}

export function WorkbenchPage({
  shell,
  latestAction,
  shellError,
  shellLoading,
  authenticated,
  currentUserLabel,
  configSaving,
  actionPendingName,
  clearingCache,
  resettingUiPreferences,
  sharingCurrentView,
  onSaveConfig,
  onRunAction,
  onClearCache,
  onResetUiPreferences,
  onShareCurrentView,
}: WorkbenchPageProps) {
  const bootstrap = shell?.bootstrap
  const config = shell?.experimentConfig
  const service = shell?.service
  const realtimeSnapshot = (service?.realtime_snapshot as JsonRecord | undefined) ?? {}
  const labelOptions = bootstrap?.labelOptions ?? []
  const writeLocked = !authenticated
  const snapshotMode = describeRealtimeSnapshotMode(realtimeSnapshot.snapshot_bucket, realtimeSnapshot.served_from)
  const snapshotSource = describeRealtimeSource(realtimeSnapshot.source)
  const snapshotCoverage = formatRealtimeCoverage(realtimeSnapshot.requested_symbol_count, realtimeSnapshot.success_symbol_count)
  const snapshotFailedSymbols = normalizeRealtimeFailedSymbols(realtimeSnapshot.failed_symbols)
  const snapshotTone = snapshotMode.tone === 'good' ? 'good' : snapshotMode.tone === 'warn' ? 'warn' : 'default'

  return (
    <div className="page-stack">
      <Panel
        title="工作台"
        subtitle="只保留参数、动作和界面设置。"
        tone="warm"
        className="panel--summary-surface"
      >
        {shellError ? <div className="query-notice query-notice--error">{shellError}</div> : null}
        {shellLoading ? <div className="query-notice">正在同步工作台状态...</div> : null}
        {writeLocked ? (
          <div className="query-notice query-notice--info">当前为只读模式。请先登录，再执行保存参数、跑模型和清缓存等写操作。</div>
        ) : (
          <div className="query-notice query-notice--success">当前登录用户：{currentUserLabel ?? '已登录'}，写操作权限已开启。</div>
        )}

        <SectionBlock title="当前工作状态" description="先看状态，再决定调参还是执行。">
          <SpotlightCard
            title="工作台状态"
            meta={String(service?.status_label_display ?? '未知')}
            subtitle="支持性快照说明后置。"
            metrics={[
              { label: '页面服务', value: service?.status_label_display ?? '未知' },
              { label: '实时快照', value: String(realtimeSnapshot.snapshot_label_display ?? '暂无快照'), tone: snapshotTone, helper: formatDateTime(realtimeSnapshot.fetched_at) },
              { label: '观察池数量', value: shell?.watchlistEntryCount ?? 0 },
              { label: '最近动作', value: latestAction?.label ?? latestAction?.actionName ?? '暂无' },
            ]}
          />

          <div className="metric-grid metric-grid--four">
            <MetricCard label="快照覆盖率" value={snapshotCoverage} tone={snapshotFailedSymbols.length ? 'warn' : 'good'} />
            <MetricCard label="行情来源" value={snapshotSource.label} helper={snapshotSource.mode} />
            <MetricCard label="快照模式" value={snapshotMode.label} tone={snapshotTone} />
            <MetricCard label="当前用户" value={authenticated ? currentUserLabel ?? '已登录' : '只读模式'} tone={authenticated ? 'good' : 'warn'} />
          </div>
        </SectionBlock>

      </Panel>

      <Panel title="研究参数" eyebrow="01" subtitle="只负责定义研究口径。" className="panel--summary-surface">
        <div className="split-layout split-layout--workspace">
          <SectionBlock title="参数表单" description="先定义训练区间、标签周期和候选数量。">
            <form
              key={JSON.stringify(config ?? {})}
              className="config-form"
              onSubmit={(event) => {
                event.preventDefault()
                const form = new FormData(event.currentTarget)
                onSaveConfig({
                  train_start: form.get('train_start'),
                  train_end: form.get('train_end'),
                  valid_end: form.get('valid_end'),
                  test_end: form.get('test_end'),
                  label_col: form.get('label_col'),
                  top_n: Number(form.get('top_n') ?? 10),
                })
              }}
            >
              <label>
                <span>训练起始日期</span>
                <input name="train_start" type="date" defaultValue={String(config?.train_start ?? '')} />
              </label>
              <label>
                <span>训练截止日期</span>
                <input name="train_end" type="date" defaultValue={String(config?.train_end ?? '')} />
              </label>
              <label>
                <span>验证截止日期</span>
                <input name="valid_end" type="date" defaultValue={String(config?.valid_end ?? '')} />
              </label>
              <label>
                <span>测试截止日期</span>
                <input name="test_end" type="date" defaultValue={String(config?.test_end ?? '')} />
              </label>
              <label>
                <span>标签周期</span>
                <select name="label_col" defaultValue={String(config?.label_col ?? 'ret_t1_t10')}>
                  {labelOptions.map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>候选股票数量</span>
                <input name="top_n" type="number" min={1} max={100} defaultValue={Number(config?.top_n ?? 10)} />
              </label>
              <div className="inline-actions inline-actions--compact">
                <button type="submit" className="button button--primary" disabled={writeLocked || configSaving || shellLoading}>
                  {configSaving ? '保存中...' : '保存研究参数'}
                </button>
              </div>
            </form>
          </SectionBlock>

          <SectionBlock title="当前参数摘要" description="只保留当前生效值。">
            <PropertyGrid
              items={[
                { label: '当前摘要', value: shell?.configSummaryText ?? '暂无摘要', span: 'double' },
                { label: '训练起始', value: String(config?.train_start ?? '-') },
                { label: '训练截止', value: String(config?.train_end ?? '-') },
                { label: '验证截止', value: String(config?.valid_end ?? '-') },
                { label: '测试截止', value: String(config?.test_end ?? '-') },
                { label: '标签周期', value: String(config?.label_col ?? '-') },
                { label: '候选数量', value: String(config?.top_n ?? '-') },
              ]}
            />
          </SectionBlock>
        </div>
      </Panel>

      <Panel title="研究动作" eyebrow="02" subtitle="只负责执行和回看最近结果。" tone="calm" className="panel--table-surface">
        <div className="split-layout split-layout--workspace">
          <SectionBlock title="执行动作" description="动作列表前置。">
            <div className="action-stack">
              {(bootstrap?.actions ?? []).map((action) => (
                <button
                  key={action.buttonKey ?? action.actionName}
                  type="button"
                  className="button"
                  disabled={writeLocked || Boolean(actionPendingName)}
                  onClick={() => onRunAction(action.actionName)}
                >
                  {actionPendingName === action.actionName ? action.spinnerText ?? `执行中：${action.label}` : action.label}
                </button>
              ))}
              <button type="button" className="button button--ghost" disabled={writeLocked || clearingCache || Boolean(actionPendingName)} onClick={onClearCache}>
                {clearingCache ? '清理中...' : '清空页面缓存'}
              </button>
            </div>
          </SectionBlock>

          <SectionBlock title="最近一次执行结果" description="先看结果摘要。">
            {latestAction ? (
              <SpotlightCard
                title={latestAction.ok ? '执行成功' : '执行失败'}
                meta={latestAction.label ?? latestAction.actionName}
                subtitle={compactActionOutput(latestAction.output)}
                metrics={[
                  { label: '动作名称', value: latestAction.actionName },
                  { label: '执行状态', value: latestAction.ok ? '成功' : '失败', tone: latestAction.ok ? 'good' : 'warn' },
                ]}
              />
            ) : (
              <div className="empty-state">暂无动作执行记录</div>
            )}
          </SectionBlock>
        </div>

        <SectionBlock title="完整动作输出" description="排查时再展开。" tone="muted" collapsible defaultExpanded={false}>
          <pre className="log-block">{latestAction?.output || '暂无执行输出'}</pre>
        </SectionBlock>
      </Panel>

      <Panel title="界面设置" eyebrow="03" subtitle="界面层工具单独收口。" className="panel--summary-surface">
        <div className="split-layout split-layout--workspace">
          <SectionBlock title="界面操作" description="分享页面和重置偏好都在这里。">
            <div className="action-stack">
              <button type="button" className="button" disabled={sharingCurrentView} onClick={onShareCurrentView}>
                {sharingCurrentView ? '复制中...' : '复制当前页面分享链接'}
              </button>
              <button type="button" className="button button--ghost" disabled={resettingUiPreferences} onClick={onResetUiPreferences}>
                {resettingUiPreferences ? '重置中...' : '重置界面偏好'}
              </button>
            </div>
          </SectionBlock>

          <SectionBlock title="当前界面说明" description="只保留必要说明。">
            <PropertyGrid
              items={[
                { label: '主题切换', value: '已放到右上角' },
                { label: '分享链接', value: sharingCurrentView ? '正在生成' : '可复制当前页面状态' },
                { label: '界面偏好', value: resettingUiPreferences ? '正在重置' : '可手动重置' },
              ]}
            />
          </SectionBlock>
        </div>
      </Panel>

      <SupportPanel title="工作台支持" subtitle="失败股票、快照入口和配置摘要统一后置。">
        <SectionBlock title="运行支持信息" description="这里只保留支持性元信息。" tone="muted" collapsible defaultExpanded={false}>
          <PropertyGrid
            items={[
              { label: '配置摘要', value: shell?.configSummaryText ?? '未加载', span: 'double' },
              { label: '失败股票', value: snapshotFailedSymbols.length ? snapshotFailedSymbols.join(' / ') : '无', span: 'double', tone: snapshotFailedSymbols.length ? 'warn' : 'good' },
              { label: '抓取时间', value: formatDateTime(realtimeSnapshot.fetched_at) },
              { label: '数据入口', value: String(realtimeSnapshot.served_from ?? '-') },
            ]}
          />
        </SectionBlock>
      </SupportPanel>
    </div>
  )
}
