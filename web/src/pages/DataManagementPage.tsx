import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { apiGet, apiPost } from '../api/client'
import { QueryNotice } from '../components/QueryNotice'
import { useToast } from '../components/ToastProvider'
import { dataManagementClient } from '../facades/dashboardPageClient'
import { formatValue } from '../lib/format'
import { DATA_MANAGEMENT_REFETCH_INTERVAL_MS } from '../lib/polling'
import type { ActionResult, DataManagementPayload, MyquantStatusPayload } from '../types/api'

interface DataManagementPageProps {
  authenticated?: boolean
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim()
  }
  return '操作失败，请稍后再试。'
}

export function DataManagementPage({ authenticated = false }: DataManagementPageProps) {
  const queryClient = useQueryClient()
  const { pushToast } = useToast()
  const [endDate, setEndDate] = useState('')
  const [latestAction, setLatestAction] = useState<ActionResult | null>(null)

  const dataQuery = useQuery({
    queryKey: dataManagementClient.queryKey(),
    queryFn: () => apiGet<DataManagementPayload>(dataManagementClient.path()),
    refetchInterval: DATA_MANAGEMENT_REFETCH_INTERVAL_MS,
  })

  const myquantStatusQuery = useQuery({
    queryKey: dataManagementClient.myquantStatusQueryKey(),
    queryFn: () => apiGet<MyquantStatusPayload>(dataManagementClient.myquantStatusPath()),
    refetchInterval: DATA_MANAGEMENT_REFETCH_INTERVAL_MS,
  })

  const payload = dataQuery.data
  const marketBarsRefreshMutation = useMutation({
    mutationFn: () =>
      apiPost<ActionResult>(dataManagementClient.marketBarsActionPath(), {
        target_source: payload?.targetSource ?? 'tushare',
        end_date: endDate || undefined,
      }),
    onSuccess: (actionPayload) => {
      setLatestAction(actionPayload)
      void queryClient.invalidateQueries()
      pushToast({
        tone: actionPayload.ok ? 'success' : 'error',
        title: actionPayload.label ?? '市场日线主表更新',
        description: actionPayload.output,
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '市场日线主表更新失败',
        description: toErrorMessage(error),
      })
    },
  })

  const watchlistResearchRefreshMutation = useMutation({
    mutationFn: () =>
      apiPost<ActionResult>(dataManagementClient.watchlistResearchActionPath(), {
        target_source: payload?.targetSource ?? 'tushare',
        end_date: endDate || undefined,
      }),
    onSuccess: (actionPayload) => {
      setLatestAction(actionPayload)
      void queryClient.invalidateQueries()
      pushToast({
        tone: actionPayload.ok ? 'success' : 'error',
        title: actionPayload.label ?? '研究面板与 AI 候选刷新',
        description: actionPayload.output,
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '研究面板与 AI 候选刷新失败',
        description: toErrorMessage(error),
      })
    },
  })

  const incrementalRefreshMutation = useMutation({
    mutationFn: () =>
      apiPost<ActionResult>(dataManagementClient.incrementalActionPath(), {
        target_source: payload?.targetSource ?? 'tushare',
        end_date: endDate || undefined,
      }),
    onSuccess: (actionPayload) => {
      setLatestAction(actionPayload)
      void queryClient.invalidateQueries()
      pushToast({
        tone: actionPayload.ok ? 'success' : 'error',
        title: actionPayload.label ?? 'Tushare 增量刷新',
        description: actionPayload.output,
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
    onSuccess: (actionPayload) => {
      setLatestAction(actionPayload)
      void queryClient.invalidateQueries()
      pushToast({
        tone: actionPayload.ok ? 'success' : 'error',
        title: actionPayload.label ?? 'Tushare 全流程刷新',
        description: actionPayload.output,
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

  const myquantDownloadMutation = useMutation({
    mutationFn: () =>
      apiPost<ActionResult>(dataManagementClient.myquantDownloadActionPath(), {
        target_source: 'myquant',
        end_date: endDate || undefined,
      }),
    onSuccess: (actionPayload) => {
      setLatestAction(actionPayload)
      void queryClient.invalidateQueries()
      pushToast({ tone: actionPayload.ok ? 'success' : 'error', title: actionPayload.label ?? 'MyQuant 下载', description: actionPayload.output })
    },
    onError: (error) => pushToast({ tone: 'error', title: 'MyQuant 下载失败', description: toErrorMessage(error) }),
  })

  const myquantEnrichMutation = useMutation({
    mutationFn: () => apiPost<ActionResult>(dataManagementClient.myquantEnrichActionPath(), { target_source: 'myquant' }),
    onSuccess: (actionPayload) => {
      setLatestAction(actionPayload)
      void queryClient.invalidateQueries()
      pushToast({ tone: actionPayload.ok ? 'success' : 'error', title: actionPayload.label ?? 'MyQuant 清洗增强', description: actionPayload.output })
    },
    onError: (error) => pushToast({ tone: 'error', title: 'MyQuant 清洗增强失败', description: toErrorMessage(error) }),
  })

  const myquantResearchMutation = useMutation({
    mutationFn: () => apiPost<ActionResult>(dataManagementClient.myquantResearchActionPath(), { target_source: 'myquant' }),
    onSuccess: (actionPayload) => {
      setLatestAction(actionPayload)
      void queryClient.invalidateQueries()
      pushToast({ tone: actionPayload.ok ? 'success' : 'error', title: actionPayload.label ?? 'MyQuant 研究刷新', description: actionPayload.output })
    },
    onError: (error) => pushToast({ tone: 'error', title: 'MyQuant 研究刷新失败', description: toErrorMessage(error) }),
  })

  const dailyBar = payload?.dailyBar ?? { exists: false, rowCount: 0, symbolCount: 0 }
  const myquantStatus = myquantStatusQuery.data
  const myquantDailyBar = myquantStatus?.dailyBar ?? { exists: false, rowCount: 0, symbolCount: 0 }
  const researchPanel = payload?.researchPanel ?? { exists: false, rowCount: 0, symbolCount: 0 }
  const legacyFeatureView = payload?.legacyFeatureView ?? { exists: false, rowCount: 0, symbolCount: 0 }
  const datasetSummary = (payload?.datasetSummary ?? {}) as Record<string, unknown>
  const tokenConfigured = payload?.tokenConfigured ?? null
  const hasToken = tokenConfigured === true
  const tokenStatusLabel = tokenConfigured === null ? '登录后可见' : hasToken ? '已配置' : '未配置'
  const targetSource = payload?.targetSource ?? payload?.activeDataSource ?? 'akshare'
  const actualSource = payload?.activeDataSource ?? targetSource
  const configuredSource = payload?.configuredDataSource ?? actualSource
  const sourceMismatch = payload?.sourceMismatch ?? false
  const pendingAction = marketBarsRefreshMutation.isPending
    ? 'market'
    : watchlistResearchRefreshMutation.isPending
      ? 'research'
      : incrementalRefreshMutation.isPending
        ? 'incremental'
        : fullRefreshMutation.isPending
          ? 'full'
          : myquantDownloadMutation.isPending
            ? 'myquant-download'
            : myquantEnrichMutation.isPending
              ? 'myquant-enrich'
              : myquantResearchMutation.isPending ? 'myquant-research' : null
  const myquantTokenConfigured = myquantStatus?.tokenConfigured ?? null
  const myquantReady = myquantTokenConfigured === true && myquantStatus?.sdkAvailable === true
  const showMyquantTokenWarning = authenticated && myquantTokenConfigured === false
  const showMyquantSdkWarning = authenticated && myquantTokenConfigured === true && myquantStatus?.sdkAvailable === false
  const showWarningBanners = !authenticated || tokenConfigured === false || sourceMismatch || showMyquantTokenWarning || showMyquantSdkWarning

  return (
    <div className="flex-1 flex flex-col overflow-hidden text-erp bg-erp-bg">
      {/* Local Toolbar */}
      <div className="h-10 bg-white erp-border-b flex items-center px-3 gap-3 shrink-0 overflow-x-auto overflow-y-hidden whitespace-nowrap">
        <span className="font-bold text-gray-700 mr-2 flex items-center gap-2 shrink-0">
          <i className="ph-fill ph-database text-erp-primary"></i> 
          数据管理 (Data Management)
        </span>
        <div className="w-px h-5 bg-gray-300 mx-1 shrink-0"></div>
        
        {/* State Indicators */}
        <div className="flex items-center gap-3 text-erp-sm shrink-0">
           <div className={`flex items-center gap-1.5 px-2 py-0.5 rounded border ${tokenConfigured === null ? 'bg-gray-100 text-gray-500 border-gray-200' : hasToken ? 'bg-green-50 text-green-700 border-green-200' : 'bg-red-50 text-red-700 border-red-200'}`}>
              <div className={`w-2 h-2 rounded-full ${hasToken ? 'bg-erp-success' : 'bg-gray-400'}`}></div>
              <span className="font-bold uppercase tracking-wider">TOKEN: {tokenStatusLabel}</span>
           </div>
           <div className="flex items-center gap-1">
             <span className="text-gray-400 font-mono">| SOURCE:</span>
             <span className="font-bold text-erp-primary">{actualSource.toUpperCase()}</span>
           </div>
           {sourceMismatch && (
             <div className="flex items-center gap-1 text-erp-warning">
               <i className="ph-fill ph-warning"></i>
               <span className="font-bold">MISMATCH ({configuredSource.toUpperCase()})</span>
             </div>
           )}
        </div>

        <div className="ml-auto flex items-center gap-4 text-erp-sm shrink-0">
          <div className="flex items-center gap-1">
            <span className="text-gray-500">日线最新:</span> 
            <span className={`font-bold font-mono ${dailyBar.exists ? 'text-erp-success' : 'text-erp-warning'}`}>{dailyBar.latestTradeDate ?? '-'}</span>
          </div>
          <div className="flex items-center gap-1">
            <span className="text-gray-500">特征面板:</span> 
            <span className={`font-bold font-mono ${researchPanel.exists ? 'text-erp-success' : 'text-erp-warning'}`}>{researchPanel.latestTradeDate ?? '-'}</span>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto bg-erp-bg p-2 flex flex-col gap-2">
        <QueryNotice isLoading={dataQuery.isLoading} error={dataQuery.error} />
        
        {/* Top Summary Row */}
        <div className="flex bg-white erp-border shrink-0">
          <div className="flex-1 p-3 flex flex-col justify-center border-r erp-border">
            <div className="text-gray-500 text-[10px] uppercase font-bold tracking-widest mb-1">今日日期 (Today)</div>
            <div className="text-xl font-mono font-bold text-erp-primary">{String(payload?.today ?? '-')}</div>
          </div>
          <div className="flex-1 p-3 flex flex-col justify-center border-r erp-border">
            <div className="text-gray-500 text-[10px] uppercase font-bold tracking-widest mb-1">日线覆盖 (Daily Bar)</div>
            <div className="text-xl font-mono font-bold">{formatValue(dailyBar.symbolCount ?? 0)} 只</div>
            <div className="text-[10px] text-gray-400 mt-1 uppercase">{formatValue(dailyBar.rowCount ?? 0)} 条记录</div>
          </div>
          <div className="flex-1 p-3 flex flex-col justify-center border-r erp-border">
            <div className="text-gray-500 text-[10px] uppercase font-bold tracking-widest mb-1">研究面板 (Research Panel)</div>
            <div className="text-xl font-mono font-bold">{formatValue(researchPanel.symbolCount ?? 0)} 只</div>
            <div className="text-[10px] text-gray-400 mt-1 uppercase">{formatValue(researchPanel.rowCount ?? 0)} 条记录</div>
          </div>
          <div className="flex-1 p-3 flex flex-col justify-center">
            <div className="text-gray-500 text-[10px] uppercase font-bold tracking-widest mb-1">缓存快照 (Cache)</div>
            <div className="text-xl font-mono font-bold">{formatValue(datasetSummary.cached_symbols ?? 0)} 只</div>
            <div className="text-[10px] text-gray-400 mt-1 uppercase">研究区间 {String(datasetSummary.date_min ?? '-')} 至 {String(datasetSummary.date_max ?? '-')}</div>
          </div>
        </div>

        {/* Warning Banners */}
        {showWarningBanners && (
          <div className="flex flex-col gap-2 shrink-0">
            {!authenticated && <div className="p-3 bg-blue-50/20 border border-blue-100 rounded text-blue-600 text-sm flex items-center gap-2"><i className="ph-fill ph-info"></i> 当前只读，可查看数据状态；如需执行刷新，请先登录系统。</div>}
            {authenticated && tokenConfigured === false && <div className="p-3 bg-red-50/20 border border-red-100 rounded text-erp-danger text-sm flex items-center gap-2"><i className="ph-fill ph-warning"></i> 当前未检测到 TUSHARE_TOKEN，请先更新 .env 配置文件或设置环境变量。</div>}
            {sourceMismatch && <div className="p-3 bg-yellow-50/20 border border-yellow-100 rounded text-yellow-700 text-sm flex items-center gap-2"><i className="ph-fill ph-warning"></i> 当前实际落地数据源为 {actualSource}，但配置文件仍指向 {configuredSource}。系统已优先读取真实落地产物。</div>}
            {showMyquantTokenWarning && <div className="p-3 bg-yellow-50/20 border border-yellow-100 rounded text-yellow-700 text-sm flex items-center gap-2"><i className="ph-fill ph-warning"></i> MyQuant Token 未配置，MyQuant Web 刷新入口将保持禁用。</div>}
            {showMyquantSdkWarning && <div className="p-3 bg-yellow-50/20 border border-yellow-100 rounded text-yellow-700 text-sm flex items-center gap-2"><i className="ph-fill ph-warning"></i> 当前环境未检测到 gm SDK，请先安装 MyQuant 官方 SDK。</div>}
          </div>
        )}

        {/* Middle Two-Column Grid: Execution & Logs */}
        <div className="flex flex-1 gap-2 min-h-[400px]">
           {/* Left: Execution Form */}
           <div className="w-[450px] flex flex-col gap-2 shrink-0">
              <div className="flex-1 bg-white erp-border flex flex-col overflow-hidden">
                 <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-semibold text-gray-700 shrink-0">
                   01. 数据流刷新控制 (Data Pipeline)
                 </div>
                 <div className="p-5 flex flex-col gap-6">
                    <div className="flex flex-col gap-4">
                       <div className="flex flex-col gap-1.5">
                          <label className="text-gray-500 font-bold uppercase text-[10px] tracking-wider">目标数据源 (Target Source)</label>
                          <input value={targetSource} disabled className="erp-border rounded px-2 h-8 bg-gray-100 text-gray-500 outline-none cursor-not-allowed font-mono uppercase" />
                       </div>
                       <div className="flex flex-col gap-1.5">
                          <label className="text-gray-500 font-bold uppercase text-[10px] tracking-wider">截止日期 (End Date) - 留空默认为今日</label>
                          <input type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} 
                                 className="erp-border rounded px-2 h-8 bg-gray-50 focus:bg-white focus:ring-1 focus:ring-erp-primary outline-none transition-all" />
                       </div>
                    </div>
                    
                    <div className="flex flex-col gap-3 pt-6 border-t erp-border">
                       <button
                         type="button"
                         className={`w-full h-9 flex items-center justify-center gap-2 rounded font-bold text-white transition-all shadow-sm ${!authenticated || tokenConfigured !== true || pendingAction !== null ? 'bg-gray-300 cursor-not-allowed' : 'bg-erp-primary hover:bg-erp-primary-hover active:scale-[0.98]'}`}
                         disabled={!authenticated || tokenConfigured !== true || pendingAction !== null}
                         onClick={() => marketBarsRefreshMutation.mutate()}
                       >
                         {pendingAction === 'market' ? <><i className="ph ph-spinner animate-spin"></i> 更新主表中...</> : <><i className="ph ph-database"></i> 更新市场日线主表 (market.bars_1d)</>}
                       </button>
                       <button
                         type="button"
                         className={`w-full h-9 flex items-center justify-center gap-2 rounded font-bold transition-all shadow-sm ${!authenticated || pendingAction !== null ? 'bg-gray-100 text-gray-400 cursor-not-allowed erp-border' : 'bg-white erp-border hover:bg-gray-50 active:scale-[0.98] text-gray-700'}`}
                         disabled={!authenticated || pendingAction !== null}
                         onClick={() => watchlistResearchRefreshMutation.mutate()}
                       >
                         {pendingAction === 'research' ? <><i className="ph ph-spinner animate-spin"></i> 重建研究中...</> : <><i className="ph ph-chart-line-up"></i> 重建研究面板 + 最新推理 + AI候选</>}
                       </button>
                       <button
                         type="button"
                         className={`w-full h-9 flex items-center justify-center gap-2 rounded font-bold transition-all shadow-sm ${!authenticated || tokenConfigured !== true || pendingAction !== null ? 'bg-gray-100 text-gray-400 cursor-not-allowed erp-border' : 'bg-white erp-border hover:bg-gray-50 active:scale-[0.98] text-gray-700'}`}
                         disabled={!authenticated || tokenConfigured !== true || pendingAction !== null}
                         onClick={() => incrementalRefreshMutation.mutate()}
                       >
                         {pendingAction === 'incremental' ? <><i className="ph ph-spinner animate-spin"></i> 增量刷新中...</> : <><i className="ph ph-fast-forward-circle"></i> Tushare 增量同步日线 (Incremental)</>}
                       </button>
                       <button
                         type="button"
                         className={`w-full h-9 flex items-center justify-center gap-2 rounded font-bold transition-all shadow-sm ${!authenticated || tokenConfigured !== true || pendingAction !== null ? 'bg-gray-100 text-gray-400 cursor-not-allowed erp-border' : 'bg-white erp-border hover:bg-gray-50 active:scale-[0.98] text-gray-700'}`}
                         disabled={!authenticated || tokenConfigured !== true || pendingAction !== null}
                         onClick={() => fullRefreshMutation.mutate()}
                       >
                         {pendingAction === 'full' ? <><i className="ph ph-spinner animate-spin"></i> 全流程刷新中...</> : <><i className="ph ph-database"></i> Tushare 全流程重建 (Full Pipeline)</>}
                       </button>
                    </div>

                    <div className="flex flex-col gap-3 pt-6 border-t erp-border">
                       <div className="text-gray-500 font-bold uppercase text-[10px] tracking-wider">MyQuant 手动刷新</div>
                       <div className="grid grid-cols-2 gap-2 text-[11px]">
                         <div className="erp-border rounded p-2 bg-gray-50">
                           <div className="text-gray-400">Token</div>
                           <div className="font-bold">{myquantTokenConfigured === null ? '登录后可见' : myquantTokenConfigured ? '已配置' : '未配置'}</div>
                         </div>
                         <div className="erp-border rounded p-2 bg-gray-50">
                           <div className="text-gray-400">gm SDK</div>
                           <div className="font-bold">{myquantStatus?.sdkAvailable ? '可用' : '不可用'}</div>
                         </div>
                         <div className="erp-border rounded p-2 bg-gray-50 col-span-2">
                           <div className="text-gray-400">MyQuant 日线</div>
                           <div className="font-bold">{myquantDailyBar.latestTradeDate ?? '-'} / {formatValue(myquantDailyBar.symbolCount ?? 0)} 只 / {formatValue(myquantDailyBar.rowCount ?? 0)} 行</div>
                         </div>
                       </div>
                       <button
                         type="button"
                         className={`w-full h-9 flex items-center justify-center gap-2 rounded font-bold transition-all shadow-sm ${!authenticated || !myquantReady || pendingAction !== null ? 'bg-gray-100 text-gray-400 cursor-not-allowed erp-border' : 'bg-white erp-border hover:bg-gray-50 active:scale-[0.98] text-gray-700'}`}
                         disabled={!authenticated || !myquantReady || pendingAction !== null}
                         onClick={() => myquantDownloadMutation.mutate()}
                       >
                         {pendingAction === 'myquant-download' ? <><i className="ph ph-spinner animate-spin"></i> 下载中...</> : <><i className="ph ph-cloud-arrow-down"></i> 下载 MyQuant 数据</>}
                       </button>
                       <button
                         type="button"
                         className={`w-full h-9 flex items-center justify-center gap-2 rounded font-bold transition-all shadow-sm ${!authenticated || pendingAction !== null ? 'bg-gray-100 text-gray-400 cursor-not-allowed erp-border' : 'bg-white erp-border hover:bg-gray-50 active:scale-[0.98] text-gray-700'}`}
                         disabled={!authenticated || pendingAction !== null}
                         onClick={() => myquantEnrichMutation.mutate()}
                       >
                         {pendingAction === 'myquant-enrich' ? <><i className="ph ph-spinner animate-spin"></i> 清洗中...</> : <><i className="ph ph-funnel"></i> 清洗增强 MyQuant 面板</>}
                       </button>
                       <button
                         type="button"
                         className={`w-full h-9 flex items-center justify-center gap-2 rounded font-bold transition-all shadow-sm ${!authenticated || pendingAction !== null ? 'bg-gray-100 text-gray-400 cursor-not-allowed erp-border' : 'bg-white erp-border hover:bg-gray-50 active:scale-[0.98] text-gray-700'}`}
                         disabled={!authenticated || pendingAction !== null}
                         onClick={() => myquantResearchMutation.mutate()}
                       >
                         {pendingAction === 'myquant-research' ? <><i className="ph ph-spinner animate-spin"></i> 重建中...</> : <><i className="ph ph-chart-scatter"></i> 重建 MyQuant 研究面板</>}
                       </button>
                    </div>
                 </div>
              </div>

              {/* Status block below form */}
              <div className="h-[120px] bg-white erp-border flex flex-col overflow-hidden shrink-0">
                 <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-semibold text-gray-700 shrink-0">
                   数据文件体积核查 (File Sizes)
                 </div>
                 <div className="flex-1 overflow-y-auto p-3 flex flex-col gap-2 text-[11px] font-mono text-gray-600 bg-gray-50/50">
                    <div className="flex justify-between border-b erp-border pb-1">
                      <span>日线面板 (daily_bar)</span>
                      <span className="font-bold">{String((dailyBar as unknown as Record<string, unknown>).size_mb ?? '-')} MB</span>
                    </div>
                    <div className="flex justify-between border-b erp-border pb-1">
                      <span>研究面板 (research_panel)</span>
                      <span className="font-bold">{String((researchPanel as unknown as Record<string, unknown>).size_mb ?? '-')} MB</span>
                    </div>
                    <div className="flex justify-between pb-1 text-gray-400">
                      <span>旧特征视图 (features)</span>
                      <span>{String((legacyFeatureView as unknown as Record<string, unknown>).size_mb ?? '-')} MB</span>
                    </div>
                 </div>
              </div>
           </div>

           {/* Right: Logs & Scripts Console */}
           <div className="flex-1 flex flex-col gap-2">
              <div className="flex-[2] bg-white erp-border flex flex-col overflow-hidden">
                 <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-semibold text-gray-700 shrink-0 justify-between">
                   <span>02. 刷新任务控制台日志 (Pipeline Console)</span>
                   <span className="text-[10px] text-gray-400 font-mono tracking-wider">STDOUT / STDERR</span>
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
                        <span className="text-white font-mono text-sm uppercase tracking-widest italic">Waiting for pipeline trigger...</span>
                     </div>
                   )}
                 </div>
              </div>

              <div className="flex-[1] bg-white erp-border flex flex-col overflow-hidden">
                 <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-semibold text-gray-700 shrink-0">
                   手工执行脚本参考 (Manual CLI Scripts)
                 </div>
                 <div className="flex-1 overflow-y-auto p-4 flex flex-col gap-3 bg-gray-50">
                    <div className="flex flex-col gap-1">
                       <span className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">增量刷新脚本:</span>
                       <code className="text-[11px] bg-white erp-border p-2 rounded text-erp-primary break-all select-all">
                         ./scripts/sync_market_bars_tushare.sh --user-id 当前登录用户{endDate ? ` --end-date ${endDate}` : ''}
                       </code>
                    </div>
                    <div className="flex flex-col gap-1">
                       <span className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">兼容 artifact 全流程脚本:</span>
                       <code className="text-[11px] bg-white erp-border p-2 rounded text-erp-primary break-all select-all">
                         powershell -ExecutionPolicy Bypass -File .\scripts\refresh_full_pipeline_tushare.ps1 -TargetSource {targetSource}{endDate ? ` -EndDate ${endDate}` : ''}
                       </code>
                    </div>
                 </div>
              </div>
           </div>
        </div>

      </div>
    </div>
  )
}
