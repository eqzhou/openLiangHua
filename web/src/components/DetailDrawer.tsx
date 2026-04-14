import { useEffect, useId, type PropsWithChildren, type ReactNode } from 'react'

interface DetailDrawerProps extends PropsWithChildren {
  open: boolean
  title: string
  subtitle?: string
  meta?: ReactNode
  footer?: ReactNode
  onClose: () => void
}

export function DetailDrawer({ open, title, subtitle, meta, footer, onClose, children }: DetailDrawerProps) {
  const titleId = useId()
  const subtitleId = useId()

  useEffect(() => {
    if (!open) {
      return undefined
    }

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose()
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [open, onClose])

  if (!open) {
    return null
  }

  return (
    <>
      <div className="drawer-backdrop" onClick={onClose} aria-hidden="true" />
      <aside
        className="detail-drawer"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={subtitle ? subtitleId : undefined}
      >
        <header className="detail-drawer__header">
          <div className="detail-drawer__title-group">
            <h2 id={titleId} className="detail-drawer__title">
              {title}
            </h2>
            {subtitle ? (
              <p id={subtitleId} className="detail-drawer__subtitle">
                {subtitle}
              </p>
            ) : null}
            {meta ? <div className="detail-drawer__meta">{meta}</div> : null}
          </div>
          <button type="button" className="detail-drawer__close" onClick={onClose} aria-label="关闭详情抽屉">
            ×
          </button>
        </header>
        <div className="detail-drawer__body">{children}</div>
        {footer ? <footer className="detail-drawer__footer">{footer}</footer> : null}
      </aside>
    </>
  )
}
