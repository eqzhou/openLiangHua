import { useState, type ReactNode } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { LoginDialog } from '../components/LoginDialog'
import { useTheme } from '../components/themeContext'
import type { AuthSessionPayload } from '../types/api'

import '../erp.css'

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
      { to: '/', label: '首页', icon: 'ph-squares-four' },
      { to: '/workspace', label: '工作台', icon: 'ph-monitor-play' },
    ],
  },
  {
    key: 'research',
    label: '研究',
    items: [
      { to: '/overview', label: '研究概览', icon: 'ph-chart-line-up' },
      { to: '/factors', label: '因子探索', icon: 'ph-function' },
      { to: '/backtests', label: '模型回测', icon: 'ph-clock-counter-clockwise' },
      { to: '/candidates', label: '候选股', icon: 'ph-users-three' },
      { to: '/watchlist', label: '持仓', icon: 'ph-list-dashes' },
      { to: '/ai-review', label: 'AI 分析', icon: 'ph-brain' },
    ],
  },
  {
    key: 'system',
    label: '系统',
    items: [
      { to: '/data', label: '数据管理', icon: 'ph-database' },
      { to: '/service', label: '页面服务', icon: 'ph-hard-drives' },
    ],
  },
]

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
  const [treeOpen, setTreeOpen] = useState<Record<string, boolean>>({
    workspace: true,
    research: true,
    system: true,
  })
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [loginDialogOpen, setLoginDialogOpen] = useState(false)
  const [loginDialogVersion, setLoginDialogVersion] = useState(0)

  const { theme, toggleTheme } = useTheme()
  const user = authSession?.user ?? null

  const openLoginDialog = () => {
    setLoginDialogVersion((version) => version + 1)
    setLoginDialogOpen(true)
  }

  const toggleSection = (key: string) => {
    setTreeOpen((prev) => ({ ...prev, [key]: !prev[key] }))
  }

  return (
    <div className={`h-screen w-screen flex flex-col overflow-hidden select-none bg-erp-bg text-erp-text font-erp transition-colors duration-200`}>
      {/* Unified Top Navigation Bar */}
      <div className="h-10 bg-erp-header flex items-center px-3 text-erp-sm text-erp-header-text gap-4 shrink-0 z-50 shadow-md">
        {/* Left: Sidebar Toggle & Brand */}
        <div className="flex items-center gap-3">
          <button 
            className="w-8 h-8 flex items-center justify-center hover:bg-white/10 rounded transition-colors text-erp-primary-hover"
            onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            title={sidebarCollapsed ? "展开侧边栏" : "折叠侧边栏"}
          >
            <i className={`ph-bold ${sidebarCollapsed ? 'ph-layout' : 'ph-sidebar-simple'} text-[20px]`}></i>
          </button>
          <div className="font-bold text-erp-primary flex items-center gap-2 cursor-default mr-4">
            <i className="ph-fill ph-chart-line-up text-[20px]"></i> 
            <span className="tracking-tight text-base font-black">OpenLianghua ERP</span>
          </div>
        </div>

        {/* Menus */}
        <div className="hidden lg:flex items-center gap-1 border-l border-white/10 pl-4 h-full">
          <div className="cursor-pointer hover:bg-white/10 px-3 py-1 rounded transition-colors">文件(F)</div>
          <div className="cursor-pointer hover:bg-white/10 px-3 py-1 rounded transition-colors text-erp-primary-hover font-bold">策略(S)</div>
          <div className="cursor-pointer hover:bg-white/10 px-3 py-1 rounded transition-colors">视图(V)</div>
          <div className="cursor-pointer hover:bg-white/10 px-3 py-1 rounded transition-colors">工具(T)</div>
        </div>

        {/* Center: Search & Global Actions */}
        <div className="flex-1 flex justify-center max-w-2xl px-4">
          <div className="flex items-center gap-3 w-full bg-white/5 border border-white/10 px-3 py-1 rounded-md shadow-inner focus-within:bg-white/10 focus-within:border-erp-primary/50 transition-all">
            <i className="ph ph-magnifying-glass text-white/30 text-base"></i>
            <input 
              type="text" 
              placeholder="全域搜索指令、代码或研报..." 
              className="outline-none text-[13px] w-full bg-transparent text-erp-header-text placeholder:text-white/20" 
            />
            <div className="flex items-center gap-1 border-l border-white/10 pl-3">
               <button className="hover:text-erp-primary-hover transition-colors p-1" title="刷新系统状态" onClick={() => window.location.reload()}>
                 <i className="ph ph-arrows-clockwise text-base"></i>
               </button>
            </div>
          </div>
        </div>

        {/* Right: Theme, Env, User */}
        <div className="ml-auto flex items-center gap-4 h-full">
          <button 
            className="flex items-center gap-1 hover:bg-white/10 px-3 h-full transition-colors group"
            onClick={() => toggleTheme()}
            title="切换主题模式"
          >
            <i className={`ph-fill ${theme === 'dark' ? 'ph-moon-stars text-erp-primary' : 'ph-sun-dim text-yellow-400'} text-[20px] group-hover:scale-110 transition-transform`}></i>
          </button>
          
          <div className="hidden xl:flex items-center gap-2 bg-white/5 px-2 py-0.5 rounded border border-white/10">
            <div className="w-2 h-2 rounded-full bg-erp-success animate-pulse shadow-[0_0_8px_rgba(22,163,74,0.6)]"></div> 
            <span className="text-[10px] font-bold opacity-80">LIVE NODE</span>
          </div>
          
          <div className="w-px h-3 bg-white/20"></div>
          
          {user ? (
            <div className="flex items-center gap-4 mr-2">
              <div className="flex items-center gap-2 group cursor-pointer">
                <i className="ph ph-user-circle text-[22px] opacity-80 group-hover:text-erp-primary transition-colors"></i>
                <span className="font-bold text-erp-header-text/90 hidden md:inline">{user.displayName || user.username}</span>
              </div>
              <button
                className="text-white hover:text-erp-danger font-bold cursor-pointer disabled:opacity-50 transition-colors text-[12px] bg-white/10 hover:bg-erp-danger/20 border border-white/10 px-3 py-1 rounded"
                onClick={() => onLogout()}
                disabled={logoutPending}
              >
                退出登录
              </button>
            </div>
          ) : (
            <button
              className="bg-erp-primary hover:bg-erp-primary-hover text-white px-5 h-8 rounded font-bold cursor-pointer disabled:opacity-50 transition-all flex items-center shadow-lg active:scale-95"
              onClick={openLoginDialog}
              disabled={authLoading || loginPending}
            >
              登录系统
            </button>
          )}
        </div>
      </div>

      {/* Main Workspace */}
      <div className="flex flex-1 overflow-hidden relative">
        {/* Left Sidebar: Tree View */}
        <div className={`${sidebarCollapsed ? 'w-0 opacity-0 translate-x-[-100%]' : 'w-64 opacity-100 translate-x-0'} bg-white flex flex-col erp-border-r shrink-0 transition-all duration-300 ease-in-out z-30`}>
          <div className="h-8 bg-gray-100 erp-border-b flex items-center px-3 font-bold text-erp-sm text-gray-500 shrink-0 uppercase tracking-widest">
            导航资源树
          </div>
          <div className="flex-1 overflow-y-auto py-3 pb-10 custom-scrollbar">
            {navSections.map((section) => (
              <div className="mb-2" key={section.key}>
                <div
                  className="flex items-center gap-2 px-3 py-1.5 cursor-pointer hover:bg-erp-bg transition-colors group"
                  onClick={() => toggleSection(section.key)}
                >
                  <i
                    className={`ph-bold text-gray-400 text-[10px] w-3 transition-transform duration-200 ${
                      treeOpen[section.key] ? 'rotate-90' : 'rotate-0'
                    }`}
                  >
                    <i className="ph ph-caret-right"></i>
                  </i>
                  <i
                    className={`ph-fill ${
                      treeOpen[section.key] ? 'ph-folder-open' : 'ph-folder'
                    } text-yellow-500 text-[16px] group-hover:scale-110 transition-transform`}
                  ></i>
                  <span className="font-bold text-gray-600 text-[12px]">{section.label}</span>
                </div>
                <div className={`overflow-hidden transition-all duration-300 ${treeOpen[section.key] ? 'max-h-[500px] opacity-100' : 'max-h-0 opacity-0'}`}>
                  <div className="pl-8 flex flex-col gap-0.5 py-1">
                    {section.items.map((item) => {
                      const isActive =
                        item.to === '/' ? location.pathname === '/' : location.pathname.startsWith(item.to)
                      return (
                        <NavLink
                          key={item.to}
                          to={item.to}
                          className={`flex items-center gap-2 px-3 py-1.5 rounded-l-md transition-all relative ${
                            isActive
                              ? 'bg-erp-primary text-white shadow-sm translate-x-[-2px]'
                              : 'hover:bg-gray-100 text-gray-600 border-l-2 border-transparent hover:border-erp-primary/30'
                          }`}
                        >
                          <i className={`ph ${isActive ? item.icon.replace('ph-', 'ph-fill ph-') : item.icon} ${isActive ? 'text-white' : 'text-gray-400'} text-[16px]`}></i>
                          <span className={`text-[13px] ${isActive ? 'font-bold' : ''}`}>
                            {item.label}
                          </span>
                          {isActive && (
                             <div className="absolute right-0 top-0 bottom-0 w-1 bg-white/20 rounded-l-full"></div>
                          )}
                        </NavLink>
                      )
                    })}
                  </div>
                </div>
              </div>
            ))}
          </div>
          
          <div className="p-4 erp-border-t bg-gray-50 text-[10px] text-gray-400 font-mono">
             v2.4.0-STABLE | BUILD: 20260422
          </div>
        </div>

        {/* Resizer Placeholder (Visual only for now) */}
        {!sidebarCollapsed && <div className="splitter-v shrink-0 hover:bg-erp-primary transition-colors cursor-default"></div>}

        {/* Right Content Area */}
        <div className="flex flex-1 flex-col overflow-hidden bg-erp-bg relative z-20">
          {/* Tab Content -> The Router Children */}
          <div className="flex-1 flex flex-col overflow-y-auto bg-white relative">
            {children}
          </div>
        </div>
      </div>

      {/* Status Bar */}
      <div className="h-6 bg-erp-header flex items-center px-4 text-[11px] text-erp-header-text/70 gap-6 shrink-0 z-50">
        <div className="flex items-center gap-1.5">
          <span className="opacity-60 uppercase tracking-tighter">System:</span> 
          <span className="text-erp-success font-bold drop-shadow-[0_0_5px_rgba(34,197,94,0.4)] flex items-center gap-1">
             ONLINE
          </span>
        </div>
        <div className="w-px h-3 bg-white/10"></div>
        <div className="flex items-center gap-1.5">
          <span className="opacity-60 uppercase tracking-tighter">Latency:</span> 
          <span className="font-mono text-erp-primary-hover font-bold">12ms</span>
        </div>
        <div className="w-px h-3 bg-white/10"></div>
        <div className="flex items-center gap-1.5">
          <span className="opacity-60 uppercase tracking-tighter">Gateway:</span> 
          <span className="text-erp-success flex items-center gap-1 font-bold">
             <i className="ph-fill ph-check-circle"></i> CTP_LIVE_NODE
          </span>
        </div>
        
        <div className="ml-auto font-mono opacity-60 flex items-center gap-3 italic">
          <i className="ph ph-cpu text-[14px]"></i>
          {new Date().toLocaleTimeString()}
        </div>
      </div>

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
    </div>
  )
}
