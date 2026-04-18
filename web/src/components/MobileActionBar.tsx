interface MobileActionBarAction {
  key: string
  label: string
  onClick: () => void | Promise<void>
  tone?: 'primary' | 'default' | 'ghost'
  disabled?: boolean
}

interface MobileActionBarProps {
  title?: string
  actions: MobileActionBarAction[]
  className?: string
}

function actionButtonClassName(tone: MobileActionBarAction['tone']) {
  if (tone === 'primary') {
    return 'button button--primary'
  }
  if (tone === 'ghost') {
    return 'button button--ghost'
  }
  return 'button'
}

export function MobileActionBar({ title = 'Quick Actions', actions, className }: MobileActionBarProps) {
  const visibleActions = actions.filter((action) => action.label)
  if (!visibleActions.length) {
    return null
  }

  const primaryActions = visibleActions.filter((action) => action.tone === 'primary')
  const secondaryActions = visibleActions.filter((action) => action.tone !== 'primary')

  return (
    <aside className={['mobile-action-bar', 'mobile-only', className].filter(Boolean).join(' ')} aria-label={title}>
      <p className="mobile-action-bar__title">{title}</p>
      {primaryActions.length ? (
        <div className="mobile-action-bar__actions mobile-action-bar__actions--primary">
          {primaryActions.map((action) => (
            <button
              key={action.key}
              type="button"
              className={actionButtonClassName(action.tone)}
              onClick={() => void action.onClick()}
              disabled={action.disabled}
            >
              {action.label}
            </button>
          ))}
        </div>
      ) : null}
      {secondaryActions.length ? (
        <div className="mobile-action-bar__actions mobile-action-bar__actions--secondary">
          {secondaryActions.map((action) => (
            <button
              key={action.key}
              type="button"
              className={actionButtonClassName(action.tone)}
              onClick={() => void action.onClick()}
              disabled={action.disabled}
            >
              {action.label}
            </button>
          ))}
        </div>
      ) : null}
    </aside>
  )
}
