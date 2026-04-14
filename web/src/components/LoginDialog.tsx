import { useEffect, useState } from 'react'

interface LoginDialogProps {
  open: boolean
  pending?: boolean
  onClose: () => void
  onSubmit: (payload: { username: string; password: string }) => Promise<void> | void
}

export function LoginDialog({ open, pending = false, onClose, onSubmit }: LoginDialogProps) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')

  useEffect(() => {
    if (!open) {
      return undefined
    }

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !pending) {
        onClose()
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [open, onClose, pending])

  if (!open) {
    return null
  }

  return (
    <>
      <div className="modal-backdrop" onClick={() => (!pending ? onClose() : undefined)} aria-hidden="true" />
      <div className="modal-card" role="dialog" aria-modal="true" aria-labelledby="login-dialog-title">
        <div className="modal-card__header">
          <div>
            <h2 id="login-dialog-title" className="modal-card__title">
              登录
            </h2>
            <p className="modal-card__subtitle">使用研究工作台账号登录,写操作会通过数据库会话做鉴权。</p>
          </div>
          <button type="button" className="modal-card__close" onClick={onClose} aria-label="关闭登录窗口" disabled={pending}>
            ×
          </button>
        </div>

        <form
          className="modal-card__body"
          onSubmit={async (event) => {
            event.preventDefault()
            if (!username.trim() || !password.trim() || pending) {
              return
            }
            await onSubmit({
              username: username.trim(),
              password,
            })
          }}
        >
          <label className="control-field">
            <span className="control-field__label">用户名</span>
            <div className="control-field__control">
              <input
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                placeholder="请输入工作台账号"
                autoFocus
                autoComplete="username"
              />
            </div>
          </label>

          <label className="control-field">
            <span className="control-field__label">密码</span>
            <div className="control-field__control">
              <input
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                placeholder="请输入密码"
                autoComplete="current-password"
              />
            </div>
          </label>

          <div className="modal-card__actions">
            <button type="button" className="button button--ghost" onClick={onClose} disabled={pending}>
              取消
            </button>
            <button type="submit" className="button button--primary" disabled={!username.trim() || !password.trim() || pending}>
              {pending ? '登录中...' : '登录'}
            </button>
          </div>
        </form>
      </div>
    </>
  )
}
