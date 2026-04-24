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

export function WorkbenchPage({
  shell,
  latestAction,
  shellLoading,
  authenticated,
  currentUserLabel,
  configSaving,
  actionPendingName,
  clearingCache,
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
    <div className="flex-1 flex flex-col overflow-hidden text-erp bg-erp-bg">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-monitor-play text-erp-primary"></i> 
          核心控制台
        </span>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        {/* Operation Mode */}
        <div className="flex items-center gap-3 text-erp-sm shrink-0">
           <div className={`flex items-center gap-1.5 px-2 py-0.5 rounded border ${authenticated ? 'bg-green-50 text-green-700 border-green-200' : 'bg-gray-100 text-gray-500 border-gray-200'}`}>
              <div className={`w-2 h-2 rounded-full ${authenticated ? 'bg-erp-success' : 'bg-gray-400'}`}></div>
              <span className="font-bold uppercase">{authenticated ? `WRITE_ENABLED: ${currentUserLabel}` : 'READ_ONLY_MODE'}</span>
           </div>
           <div className="flex items-center gap-1">
             <span className="text-gray-400 font-mono">| SNAPSHOT:</span>
             <span className={`font-bold ${snapshotTone === 'good' ? 'text-erp-success' : 'text-erp-warning'}`}>{snapshotMode.label}</span>
           </div>
        </div>

        <div className="ml-auto flex items-center gap-4 text-erp-sm shrink-0">
          <div className="flex items-center gap-1">
            <span className="text-gray-500">观察池:</span> 
            <span className="font-bold font-mono text-erp-primary">{shell?.watchlistEntryCount ?? 0}</span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">覆盖率:</span> 
            <span className={`font-bold font-mono ${snapshotFailedSymbols.length ? 'text-erp-warning' : ''}`}>{snapshotCoverage}</span>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto bg-erp-bg p-2 flex flex-col gap-2">
        <div className="flex-1 flex gap-2 min-h-[600px]">
          
          {/* Left Column: Config Form */}
          <div className="w-[400px] flex flex-col gap-2 shrink-0">
            <div className="bg-white erp-border flex flex-col h-full overflow-hidden">
               <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-semibold text-gray-700 shrink-0">
                 01. 研究参数配置 (Parameters)
               </div>
               <div className="flex-1 overflow-y-auto p-5">
                  <form
                    key={JSON.stringify(config ?? {})}
                    className="flex flex-col gap-6"
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
                    <div className="grid grid-cols-1 gap-4 text-erp-sm">
                       {[
                         { id: 'train_start', label: '训练起始日期', type: 'date', val: config?.train_start },
                         { id: 'train_end', label: '训练截止日期', type: 'date', val: config?.train_end },
                         { id: 'valid_end', label: '验证截止日期', type: 'date', val: config?.valid_end },
                         { id: 'test_end', label: '测试截止日期', type: 'date', val: config?.test_end },
                       ].map(field => (
                        <div key={field.id} className="flex flex-col gap-1.5">
                           <label className="text-gray-500 font-bold uppercase text-[10px] tracking-wider">{field.label}</label>
                           <input name={field.id} type={field.type as string} defaultValue={String(field.val ?? '')} 
                                  className="erp-border rounded px-2 h-8 bg-gray-50 focus:bg-white focus:ring-1 focus:ring-erp-primary outline-none transition-all" />
                        </div>
                       ))}
                       
                       <div className="flex flex-col gap-1.5">
                          <label className="text-gray-500 font-bold uppercase text-[10px] tracking-wider">预测标签周期 (Label)</label>
                          <select name="label_col" defaultValue={String(config?.label_col ?? 'ret_t1_t10')}
                                  className="erp-border rounded px-2 h-8 bg-gray-50 focus:bg-white outline-none">
                             {labelOptions.map((option) => (
                               <option key={option} value={option}>{option}</option>
                             ))}
                          </select>
                       </div>
                       
                       <div className="flex flex-col gap-1.5">
                          <label className="text-gray-500 font-bold uppercase text-[10px] tracking-wider">候选股入选数量 (TopN)</label>
                          <input name="top_n" type="number" min={1} max={100} defaultValue={Number(config?.top_n ?? 10)} 
                                 className="erp-border rounded px-2 h-8 bg-gray-50 focus:bg-white outline-none" />
                       </div>
                    </div>
                    
                    <div className="pt-4 mt-4 border-t erp-border">
                       <button type="submit" 
                               className={`w-full h-9 flex items-center justify-center gap-2 rounded font-bold text-white transition-all shadow-sm ${writeLocked || configSaving ? 'bg-gray-300 cursor-not-allowed' : 'bg-erp-primary hover:bg-erp-primary-hover active:scale-[0.98]'}`}
                               disabled={writeLocked || configSaving || shellLoading}>
                         {configSaving ? <i className="ph ph-spinner animate-spin"></i> : <i className="ph ph-floppy-disk"></i>}
                         {configSaving ? '正在写入数据库...' : '保存当前配置'}
                       </button>
                    </div>
                  </form>
               </div>
               
               <div className="p-4 bg-gray-50 erp-border-t">
                  <div className="text-[10px] text-gray-400 font-bold uppercase mb-2">活跃配置指纹:</div>
                  <p className="text-[11px] text-gray-500 leading-relaxed font-mono break-all italic bg-white p-2 erp-border rounded">
                    {shell?.configSummaryText || 'Default internal state active.'}
                  </p>
               </div>
            </div>
          </div>

          {/* Right Column: Execution & Console */}
          <div className="flex-1 flex flex-col gap-2">
             {/* Action Grid */}
             <div className="bg-white erp-border flex flex-col h-[280px] overflow-hidden shrink-0">
                <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-semibold text-gray-700 shrink-0">
                  02. 自动化研究动作 (Actions)
                </div>
                <div className="flex-1 p-4 grid grid-cols-2 lg:grid-cols-4 gap-3 overflow-y-auto">
                   {(bootstrap?.actions ?? []).map((action) => (
                    <button
                      key={action.buttonKey ?? action.actionName}
                      className={`flex flex-col items-center justify-center p-4 rounded-lg erp-border bg-gray-50 hover:bg-white hover:shadow-md transition-all gap-2 group ${actionPendingName === action.actionName ? 'animate-pulse border-erp-primary' : ''}`}
                      disabled={writeLocked || Boolean(actionPendingName)}
                      onClick={() => onRunAction(action.actionName)}
                    >
                      {actionPendingName === action.actionName ? (
                        <i className="ph ph-circle-notch animate-spin text-2xl text-erp-primary"></i>
                      ) : (
                        <i className={`ph ph-lightning-bolt text-2xl ${latestAction?.actionName === action.actionName ? 'text-erp-success' : 'text-gray-400 group-hover:text-erp-primary'}`}></i>
                      )}
                      <span className="text-[12px] font-bold text-gray-700">{action.label}</span>
                    </button>
                  ))}
                  
                  <button className="flex flex-col items-center justify-center p-4 rounded-lg erp-border border-dashed hover:bg-red-50 hover:border-erp-danger group transition-all gap-2"
                          disabled={writeLocked || clearingCache || Boolean(actionPendingName)} onClick={onClearCache}>
                    <i className="ph ph-trash text-2xl text-gray-300 group-hover:text-erp-danger"></i>
                    <span className="text-[12px] font-bold text-gray-400 group-hover:text-erp-danger">清空缓存</span>
                  </button>
                </div>
             </div>

             {/* Live Output Console */}
             <div className="flex-1 bg-white erp-border flex flex-col overflow-hidden">
                <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-semibold text-gray-700 shrink-0 justify-between">
                  <span>03. 系统执行日志 (Output Console)</span>
                  <div className="flex items-center gap-2">
                     <button onClick={onShareCurrentView} className="text-[10px] bg-white erp-border px-2 py-0.5 rounded hover:bg-gray-50 transition-colors">
                        <i className="ph ph-share"></i> 复制分享
                     </button>
                  </div>
                </div>
                <div className="flex-1 bg-[#020617] overflow-y-auto p-4 custom-scrollbar">
                   {latestAction ? (
                     <div className="flex flex-col gap-4">
                        <div className="flex items-center gap-4 border-b border-white/10 pb-3">
                           <span className={`text-xs px-2 py-0.5 rounded font-bold ${latestAction.ok ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}`}>
                             {latestAction.ok ? 'SUCCESS' : 'FAILED'}
                           </span>
                           <span className="text-white font-mono text-sm uppercase tracking-wider">{latestAction.label || latestAction.actionName}</span>
                        </div>
                        <pre className="font-mono text-[12px] text-gray-300 leading-relaxed whitespace-pre-wrap">
                          {latestAction.output || '> (Empty output received from server)'}
                        </pre>
                     </div>
                   ) : (
                     <div className="h-full flex flex-col items-center justify-center gap-4 opacity-30 grayscale">
                        <i className="ph ph-terminal-window text-5xl text-white"></i>
                        <span className="text-white font-mono text-sm uppercase tracking-widest italic">Waiting for command...</span>
                     </div>
                   )}
                </div>
             </div>
          </div>
        </div>

        {/* Bottom Metadata Section */}
        <section className="bg-white erp-border p-4 rounded-lg flex items-center gap-8 text-[11px] text-gray-400 shrink-0">
           <div className="flex flex-col gap-1">
              <span className="font-bold uppercase tracking-widest text-[9px] text-gray-500">Service Meta:</span>
              <div className="flex items-center gap-3">
                 <span>PID: {String(service?.pid || '-')}</span>
                 <span>HOST: 127.0.0.1</span>
                 <span>UPTIME: {String(service?.uptime || '-')}s</span>
              </div>
           </div>
           <div className="w-px h-6 bg-gray-200"></div>
           <div className="flex flex-col gap-1 flex-1">
              <span className="font-bold uppercase tracking-widest text-[9px] text-gray-500">Snapshot Info:</span>
              <div className="flex items-center gap-3">
                 <span>SOURCE: {snapshotSource.label} ({snapshotSource.mode})</span>
                 <span>FETCHED: {formatDateTime(realtimeSnapshot.fetched_at)}</span>
                 {snapshotFailedSymbols.length > 0 && <span className="text-erp-warning font-bold">FAILS: {snapshotFailedSymbols.join(' / ')}</span>}
              </div>
           </div>
           <div className="flex items-center gap-2">
              <button onClick={onResetUiPreferences} className="text-erp-primary hover:underline font-bold uppercase tracking-tighter">
                Reset UI Layer Preferences
              </button>
           </div>
        </section>
      </div>
    </div>
  )
}
