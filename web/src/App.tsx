import { Suspense, lazy, useState } from 'react'
import { QueryClient, QueryClientProvider, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { BrowserRouter, Route, Routes, useLocation } from 'react-router-dom'

import { ApiError, apiGet, apiPost, apiPut } from './api/client'
import { ThemeProvider } from './components/ThemeProvider'
import { ToastProvider, useToast } from './components/ToastProvider'
import { shellClient } from './facades/dashboardPageClient'
import { AUTH_SESSION_REFETCH_INTERVAL_MS, SHELL_REFETCH_INTERVAL_MS } from './lib/polling'
import { copyShareablePageLink } from './lib/shareLinks'
import { clearUiPreferences, resolveInitialTheme, setUiPreferenceScope } from './lib/uiPreferences'
import { AppLayout } from './layout/AppLayout'
import type { ActionResult, AuthSessionPayload, ShellPayload } from './types/api'

const queryClient = new QueryClient()
const HomePage = lazy(() => import('./pages/HomePage').then((module) => ({ default: module.HomePage })))
const WorkbenchPage = lazy(() => import('./pages/WorkbenchPage').then((module) => ({ default: module.WorkbenchPage })))
const OverviewPage = lazy(() => import('./pages/OverviewPage').then((module) => ({ default: module.OverviewPage })))
const FactorExplorerPage = lazy(() => import('./pages/FactorExplorerPage').then((module) => ({ default: module.FactorExplorerPage })))
const ModelBacktestPage = lazy(() => import('./pages/ModelBacktestPage').then((module) => ({ default: module.ModelBacktestPage })))
const CandidatesPage = lazy(() => import('./pages/CandidatesPage').then((module) => ({ default: module.CandidatesPage })))
const CandidateDetailPage = lazy(() => import('./pages/CandidateDetailPage').then((module) => ({ default: module.CandidateDetailPage })))
const WatchlistPage = lazy(() => import('./pages/WatchlistPage').then((module) => ({ default: module.WatchlistPage })))
const WatchlistDetailPage = lazy(() => import('./pages/WatchlistDetailPage').then((module) => ({ default: module.WatchlistDetailPage })))
const AiReviewPage = lazy(() => import('./pages/AiReviewPage').then((module) => ({ default: module.AiReviewPage })))
const AiReviewDetailPage = lazy(() => import('./pages/AiReviewDetailPage').then((module) => ({ default: module.AiReviewDetailPage })))
const DataManagementPage = lazy(() => import('./pages/DataManagementPage').then((module) => ({ default: module.DataManagementPage })))
const ServicePage = lazy(() => import('./pages/ServicePage').then((module) => ({ default: module.ServicePage })))

function toErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401) {
      return '请先从右上角登录,再执行需要写权限的操作。'
    }
    if (error.message.trim()) {
      return error.message.trim()
    }
  }

  if (error instanceof Error && error.message.trim()) {
    return error.message.trim()
  }
  return '请稍后重试。'
}

function DashboardApp() {
  const location = useLocation()
  const reactQueryClient = useQueryClient()
  const { pushToast } = useToast()
  const [latestAction, setLatestAction] = useState<ActionResult | null>(null)

  const authSessionQuery = useQuery({
    queryKey: ['auth', 'session'],
    queryFn: () => apiGet<AuthSessionPayload>('/api/auth/session'),
    refetchInterval: AUTH_SESSION_REFETCH_INTERVAL_MS,
  })

  const shellQuery = useQuery({
    queryKey: shellClient.queryKey(),
    queryFn: () => apiGet<ShellPayload>(shellClient.path()),
    refetchInterval: SHELL_REFETCH_INTERVAL_MS,
  })

  const loginMutation = useMutation({
    mutationFn: (payload: { username: string; password: string }) => apiPost<AuthSessionPayload>('/api/auth/login', payload),
    onSuccess: (payload) => {
      reactQueryClient.setQueryData(['auth', 'session'], payload)
      reactQueryClient.invalidateQueries()
      pushToast({
        tone: 'success',
        title: '登录成功',
        description: `${payload.user?.displayName ?? payload.user?.username ?? '当前用户'} 已进入工作台。`,
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '登录失败',
        description: toErrorMessage(error),
      })
    },
  })

  const logoutMutation = useMutation({
    mutationFn: () => apiPost<{ ok: boolean }>('/api/auth/logout'),
    onSuccess: () => {
      setLatestAction(null)
      reactQueryClient.setQueryData<AuthSessionPayload>(['auth', 'session'], {
        authenticated: false,
        user: null,
      })
      reactQueryClient.invalidateQueries()
      pushToast({
        tone: 'success',
        title: '已退出登录',
        description: '写操作权限已关闭。',
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '退出失败',
        description: toErrorMessage(error),
      })
    },
  })

  const changePasswordMutation = useMutation({
    mutationFn: (payload: { oldPassword: string; newPassword: string }) => apiPost<{ ok: boolean }>('/api/auth/change-password', payload),
    onSuccess: () => {
      pushToast({
        tone: 'success',
        title: '密码已更新',
        description: '新的管理员密码已经生效。',
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '密码修改失败',
        description: toErrorMessage(error),
      })
    },
  })

  const saveConfigMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => apiPut<Record<string, unknown>>('/api/config/experiment', payload),
    onSuccess: () => {
      reactQueryClient.invalidateQueries()
      pushToast({
        tone: 'success',
        title: '研究参数已保存',
        description: '新的训练区间和候选数量已经生效。',
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '研究参数保存失败',
        description: toErrorMessage(error),
      })
    },
  })

  const actionMutation = useMutation({
    mutationFn: (actionName: string) => apiPost<ActionResult>(`/api/actions/${actionName}`),
    onSuccess: (payload) => {
      setLatestAction(payload)
      reactQueryClient.invalidateQueries()
      pushToast({
        tone: payload.ok ? 'success' : 'error',
        title: payload.ok ? `${payload.label ?? payload.actionName} 已完成` : `${payload.label ?? payload.actionName} 执行失败`,
        description: payload.output,
      })
    },
    onError: (error, actionName) => {
      pushToast({
        tone: 'error',
        title: `${actionName} 执行失败`,
        description: toErrorMessage(error),
      })
    },
  })

  const clearCacheMutation = useMutation({
    mutationFn: () => apiPost<{ ok: boolean }>('/api/cache/clear'),
    onSuccess: () => {
      reactQueryClient.invalidateQueries()
      pushToast({
        tone: 'success',
        title: '页面缓存已清空',
        description: '下一次进入页面会重新拉取最新数据。',
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '清空缓存失败',
        description: toErrorMessage(error),
      })
    },
  })

  const resetUiPreferencesMutation = useMutation({
    mutationFn: async () => {
      clearUiPreferences()
      return resolveInitialTheme()
    },
    onSuccess: () => {
      reactQueryClient.invalidateQueries()
      pushToast({
        tone: 'success',
        title: '界面偏好已重置',
        description: '主题和表格布局都已经恢复为默认状态。',
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '重置界面偏好失败',
        description: toErrorMessage(error),
      })
    },
  })

  const shareCurrentViewMutation = useMutation({
    mutationFn: () => copyShareablePageLink(location.pathname, location.search),
    onSuccess: (shareUrl) => {
      pushToast({
        tone: 'success',
        title: '已复制分享链接',
        description: shareUrl,
      })
    },
    onError: (error) => {
      pushToast({
        tone: 'error',
        title: '复制分享链接失败',
        description: toErrorMessage(error),
      })
    },
  })

  const shellError = [shellQuery.error]
    .map((item) => (item instanceof Error ? item.message : ''))
    .find(Boolean)

  const isAuthenticated = Boolean(authSessionQuery.data?.authenticated)
  const activeUserId = authSessionQuery.data?.user?.userId ?? 'public'
  setUiPreferenceScope(activeUserId)

  return (
    <AppLayout
      authSession={authSessionQuery.data}
      authLoading={authSessionQuery.isLoading}
      loginPending={loginMutation.isPending}
      logoutPending={logoutMutation.isPending}
      onLogin={(payload) => loginMutation.mutateAsync(payload).then(() => undefined)}
      onLogout={() => logoutMutation.mutateAsync().then(() => undefined)}
      onChangePassword={(payload) => changePasswordMutation.mutateAsync(payload).then(() => undefined)}
    >
      <Suspense fallback={<div className="empty-state">页面加载中...</div>}>
        <Routes>
          <Route
            path="/"
            element={
              <HomePage
                bootstrap={shellQuery.data?.bootstrap}
                latestAction={latestAction}
                authenticated={isAuthenticated}
                currentUserLabel={authSessionQuery.data?.user?.displayName ?? authSessionQuery.data?.user?.username ?? null}
                actionPendingName={actionMutation.isPending ? actionMutation.variables ?? null : null}
                sharingCurrentView={shareCurrentViewMutation.isPending}
                onRunAction={(actionName) => actionMutation.mutate(actionName)}
                onShareCurrentView={() => shareCurrentViewMutation.mutate()}
              />
            }
          />
          <Route
            path="/workspace"
            element={
              <WorkbenchPage
                shell={shellQuery.data}
                latestAction={latestAction}
                shellError={shellError ?? null}
                shellLoading={shellQuery.isLoading}
                authenticated={isAuthenticated}
                currentUserLabel={authSessionQuery.data?.user?.displayName ?? authSessionQuery.data?.user?.username ?? null}
                configSaving={saveConfigMutation.isPending}
                actionPendingName={actionMutation.isPending ? actionMutation.variables ?? null : null}
                clearingCache={clearCacheMutation.isPending}
                resettingUiPreferences={resetUiPreferencesMutation.isPending}
                sharingCurrentView={shareCurrentViewMutation.isPending}
                onSaveConfig={(payload) => saveConfigMutation.mutate(payload)}
                onRunAction={(actionName) => actionMutation.mutate(actionName)}
                onClearCache={() => clearCacheMutation.mutate()}
                onResetUiPreferences={() => resetUiPreferencesMutation.mutate()}
                onShareCurrentView={() => shareCurrentViewMutation.mutate()}
              />
            }
          />
          <Route path="/overview" element={<OverviewPage bootstrap={shellQuery.data?.bootstrap} />} />
          <Route path="/factors" element={<FactorExplorerPage />} />
          <Route path="/backtests" element={<ModelBacktestPage bootstrap={shellQuery.data?.bootstrap} />} />
          <Route path="/candidates" element={<CandidatesPage bootstrap={shellQuery.data?.bootstrap} />} />
          <Route path="/candidates/:symbol" element={<CandidateDetailPage />} />
          <Route path="/watchlist" element={<WatchlistPage bootstrap={shellQuery.data?.bootstrap} authenticated={isAuthenticated} />} />
          <Route path="/watchlist/:symbol" element={<WatchlistDetailPage authenticated={isAuthenticated} />} />
          <Route path="/ai-review" element={<AiReviewPage />} />
          <Route path="/ai-review/:scope/:symbol" element={<AiReviewDetailPage />} />
          <Route path="/data" element={<DataManagementPage authenticated={isAuthenticated} />} />
          <Route path="/service" element={<ServicePage authenticated={isAuthenticated} />} />
        </Routes>
      </Suspense>
    </AppLayout>
  )
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <ToastProvider>
          <BrowserRouter>
            <DashboardApp />
          </BrowserRouter>
        </ToastProvider>
      </ThemeProvider>
    </QueryClientProvider>
  )
}
