/* eslint-disable react-refresh/only-export-components */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PropsWithChildren,
} from 'react'

type ToastTone = 'success' | 'error' | 'info'

interface ToastItem {
  id: number
  title: string
  description?: string
  tone: ToastTone
}

interface ToastInput {
  title: string
  description?: string
  tone?: ToastTone
}

interface ToastContextValue {
  pushToast: (input: ToastInput) => void
}

const ToastContext = createContext<ToastContextValue | null>(null)

function trimDescription(value?: string): string | undefined {
  const trimmed = value?.trim()
  if (!trimmed) {
    return undefined
  }
  return trimmed.length > 180 ? `${trimmed.slice(0, 180)}...` : trimmed
}

export function ToastProvider({ children }: PropsWithChildren) {
  const [toasts, setToasts] = useState<ToastItem[]>([])
  const idRef = useRef(0)
  const timersRef = useRef<Map<number, number>>(new Map())

  const dismissToast = useCallback((id: number) => {
    const timer = timersRef.current.get(id)
    if (timer) {
      window.clearTimeout(timer)
      timersRef.current.delete(id)
    }
    setToasts((current) => current.filter((item) => item.id !== id))
  }, [])

  const pushToast = useCallback(
    ({ title, description, tone = 'info' }: ToastInput) => {
      idRef.current += 1
      const id = idRef.current

      setToasts((current) => [
        ...current,
        {
          id,
          title,
          description: trimDescription(description),
          tone,
        },
      ])

      const timer = window.setTimeout(() => {
        dismissToast(id)
      }, 4200)
      timersRef.current.set(id, timer)
    },
    [dismissToast],
  )

  useEffect(() => {
    const timers = timersRef.current
    return () => {
      timers.forEach((timer) => window.clearTimeout(timer))
      timers.clear()
    }
  }, [])

  const contextValue = useMemo(() => ({ pushToast }), [pushToast])

  return (
    <ToastContext.Provider value={contextValue}>
      {children}
      <div className="toast-viewport" aria-live="polite" aria-atomic="true">
        {toasts.map((toast) => (
          <article key={toast.id} className={`toast toast--${toast.tone}`}>
            <div className="toast__content">
              <strong className="toast__title">{toast.title}</strong>
              {toast.description ? <p className="toast__description">{toast.description}</p> : null}
            </div>
            <button type="button" className="toast__close" onClick={() => dismissToast(toast.id)} aria-label="关闭提示">
              ×
            </button>
          </article>
        ))}
      </div>
    </ToastContext.Provider>
  )
}

export function useToast() {
  const context = useContext(ToastContext)
  if (!context) {
    throw new Error('useToast must be used within ToastProvider')
  }
  return context
}
