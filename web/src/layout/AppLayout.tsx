import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { NavLink, useLocation } from 'react-router-dom'

import { LoginDialog } from '../components/LoginDialog'
import { useTheme } from '../components/themeContext'
import type { AuthSessionPayload } from '../types/api'

interface AppLayoutProps {
  children: ReactNode
  authSession?: AuthSessionPayload
  authLoading?: boolean
  loginPending?: boolean
  logoutPending?: boolean
  onLogin: (payload: { username: string; password: string }) => Promise<void>
  onLogout: () => Promise<void> | void
}

const navSections = [
  {
    key: 'workspace',
    label: '操作',
    items: [
      { to: '/', label: '首页' },
      { to: '/workspace', label: '工作台' },
    ],
  },
  {
    key: 'research',
    label: '研究',
    items: [
      { to: '/overview', label: '研究概览' },
      { to: '/factors', label: '因子探索' },
      { to: '/backtests', label: '模型回测' },
      { to: '/candidates', label: '候选股' },
      { to: '/watchlist', label: '持仓' },
      { to: '/ai-review', label: 'AI 分析' },
    ],
  },
  {
    key: 'system',
    label: '系统',
    items: [
      { to: '/data', label: '数据管理' },
      { to: '/service', label: '页面服务' },
    ],
  },
]

const bottomNavItems = [
  { to: '/', label: '概览' },
  { to: '/workspace', label: '工作台' },
  { to: '/overview', label: '研究' },
  { to: '/watchlist', label: '持仓' },
  { to: '/service', label: '系统' },
]

function isNavItemActive(pathname: string, to: string) {
  if (to === '/') {
    return pathname === '/'
  }
  return pathname.startsWith(to)
}

export function AppLayout({
  children,
  authSession,
  authLoading = false,
  loginPending = false,
  logoutPending = false,
  onLogin,
  onLogout,
}: AppLayoutProps) {
  const location = useLocation()
  const { theme, setTheme } = useTheme()
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [accountMenuOpen, setAccountMenuOpen] = useState(false)
  const [loginDialogOpen, setLoginDialogOpen] = useState(false)
  const [loginDialogVersion, setLoginDialogVersion] = useState(0)
  const accountMenuRef = useRef<HTMLDivElement | null>(null)
  const user = authSession?.user ?? null

  const flatItems = useMemo(() => navSections.flatMap((section) => section.items), [])
  const activeItem =
    flatItems.find((item) => (item.to === '/' ? location.pathname === '/' : location.pathname.startsWith(item.to))) ?? flatItems[0]

  useEffect(() => {
    if (!accountMenuOpen) {
      return undefined
    }

    const onPointerDown = (event: MouseEvent) => {
      if (!accountMenuRef.current?.contains(event.target as Node)) {
        setAccountMenuOpen(false)
      }
    }

    window.addEventListener('mousedown', onPointerDown)
    return () => window.removeEventListener('mousedown', onPointerDown)
  }, [accountMenuOpen])

  const openLoginDialog = () => {
    setAccountMenuOpen(false)
    setLoginDialogVersion((version) => version + 1)
    setLoginDialogOpen(true)
  }

  const userInitial = (user?.displayName ?? user?.username ?? 'U').trim().charAt(0).toUpperCase() || 'U'

  return (
    <>
      <header className="app-topbar">
        <div className="app-topbar__inner">
          <div className="app-topbar__brand-group">
            <div className="app-topbar__brand-stack">
              <div className="app-topbar__eyebrow">Research Terminal</div>
              <div className="app-topbar__brand-line">
                <strong className="app-topbar__brand">OpenLianghua</strong>
                <span className="app-topbar__divider" aria-hidden="true">
                  /
                </span>
                <span className="app-topbar__page">{activeItem.label}</span>
              </div>
            </div>
          </div>

          <div className="app-topbar__actions">
            <div className="theme-switch theme-switch--topbar" role="group" aria-label="切换主题">
              <button
                type="button"
                className={`button button--ghost button--small${theme === 'light' ? ' button--active' : ''}`}
                onClick={() => setTheme('light')}
              >
                浅色
              </button>
              <button
                type="button"
                className={`button button--ghost button--small${theme === 'dark' ? ' button--active' : ''}`}
                onClick={() => setTheme('dark')}
              >
                深色
              </button>
            </div>

            <div className="account-menu" ref={accountMenuRef}>
              {user ? (
                <>
                  <button
                    type="button"
                    className="account-menu__trigger"
                    aria-expanded={accountMenuOpen}
                    onClick={() => setAccountMenuOpen((open) => !open)}
                    disabled={logoutPending}
                  >
                    <span className="account-menu__avatar" aria-hidden="true">
                      {userInitial}
                    </span>
                    <span className="account-menu__copy">
                      <span className="account-menu__name">{user.displayName}</span>
                      <span className="account-menu__role">{user.title || user.username}</span>
                    </span>
                  </button>
                  {accountMenuOpen ? (
                    <div className="account-menu__panel">
                      <div className="account-menu__panel-copy">
                        <strong>{user.displayName}</strong>
                        <span>{user.title || user.username}</span>
                      </div>
                      <button
                        type="button"
                        className="button button--ghost button--small"
                        disabled={logoutPending}
                        onClick={async () => {
                          setAccountMenuOpen(false)
                          await onLogout()
                        }}
                      >
                        {logoutPending ? '退出中...' : '退出'}
                      </button>
                    </div>
                  ) : null}
                </>
              ) : (
                <button type="button" className="button button--ghost button--small" onClick={openLoginDialog} disabled={authLoading || loginPending}>
                  {authLoading ? '检查中...' : loginPending ? '登录中...' : '登录'}
                </button>
              )}
            </div>

            <button
              type="button"
              className="button button--ghost button--small app-topbar__menu-button"
              aria-expanded={sidebarOpen}
              aria-controls="app-primary-sidebar"
              onClick={() => setSidebarOpen((open) => !open)}
            >
              {sidebarOpen ? '收起菜单' : '打开菜单'}
            </button>
          </div>
        </div>
      </header>

      <div className={`app-shell${sidebarOpen ? ' app-shell--sidebar-open' : ''}`}>
        <div className={`app-shell__backdrop${sidebarOpen ? ' app-shell__backdrop--visible' : ''}`} onClick={() => setSidebarOpen(false)} aria-hidden="true" />

        <aside id="app-primary-sidebar" className={`app-sidebar app-sidebar--nav-only${sidebarOpen ? ' app-sidebar--open' : ''}`} aria-label="主菜单">
          <nav className="app-sidebar__nav" aria-label="主导航">
            {navSections.map((section) => (
              <section key={section.key} className="sidebar-nav__section">
                <p className="sidebar-nav__section-title">{section.label}</p>
                <ul className="sidebar-nav__list sidebar-nav__list--compact">
                  {section.items.map((item) => (
                    <li key={item.to} className="sidebar-nav__item">
                      <NavLink
                        to={item.to}
                        onClick={() => setSidebarOpen(false)}
                        className={({ isActive }) => `sidebar-nav__link sidebar-nav__link--compact${isActive ? ' sidebar-nav__link--active' : ''}`}
                      >
                        <span className="sidebar-nav__marker" aria-hidden="true" />
                        <span className="sidebar-nav__title">{item.label}</span>
                      </NavLink>
                    </li>
                  ))}
                </ul>
              </section>
            ))}
          </nav>
        </aside>

        <main className="workspace-main workspace-main--shell">{children}</main>
      </div>

      <nav className="bottom-tab-bar" aria-label="移动端底部导航">
        {bottomNavItems.map((item) => {
          const active = isNavItemActive(location.pathname, item.to)
          return (
            <NavLink
              key={item.to}
              to={item.to}
              className={`bottom-tab-bar__link${active ? ' bottom-tab-bar__link--active' : ''}`}
            >
              <span className="bottom-tab-bar__label">{item.label}</span>
            </NavLink>
          )
        })}
      </nav>

      <LoginDialog
        key={loginDialogVersion}
        open={loginDialogOpen}
        pending={loginPending}
        onClose={() => setLoginDialogOpen(false)}
        onSubmit={async (payload) => {
          await onLogin(payload)
          setLoginDialogOpen(false)
        }}
      />
    </>
  )
}
