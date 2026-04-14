interface DrawerQuickAction {
  key: string
  label: string
  onClick: () => void | Promise<void>
  tone?: 'primary' | 'default' | 'ghost'
  disabled?: boolean
}

interface DrawerQuickActionsProps {
  title?: string
  description?: string
  primaryActions?: DrawerQuickAction[]
  secondaryActions?: DrawerQuickAction[]
}

function buttonClassName(tone: DrawerQuickAction['tone']): string {
  if (tone === 'primary') {
    return 'button button--primary'
  }
  if (tone === 'ghost') {
    return 'button button--ghost'
  }
  return 'button'
}

export function DrawerQuickActions({
  title = '快捷操作',
  description,
  primaryActions = [],
  secondaryActions = [],
}: DrawerQuickActionsProps) {
  const visiblePrimaryActions = primaryActions.filter((item) => item.label)
  const visibleSecondaryActions = secondaryActions.filter((item) => item.label)

  if (!visiblePrimaryActions.length && !visibleSecondaryActions.length) {
    return null
  }

  return (
    <section className="drawer-action-shelf" aria-label={title}>
      <div className="drawer-action-shelf__header">
        <div>
          <h3 className="drawer-action-shelf__title">{title}</h3>
          {description ? <p className="drawer-action-shelf__description">{description}</p> : null}
        </div>
      </div>

      {visiblePrimaryActions.length ? (
        <div className="drawer-action-shelf__group drawer-action-shelf__group--primary">
          {visiblePrimaryActions.map((action) => (
            <button
              key={action.key}
              type="button"
              className={buttonClassName(action.tone)}
              onClick={() => void action.onClick()}
              disabled={action.disabled}
            >
              {action.label}
            </button>
          ))}
        </div>
      ) : null}

      {visibleSecondaryActions.length ? (
        <div className="drawer-action-shelf__group drawer-action-shelf__group--secondary">
          {visibleSecondaryActions.map((action) => (
            <button
              key={action.key}
              type="button"
              className={buttonClassName(action.tone ?? 'ghost')}
              onClick={() => void action.onClick()}
              disabled={action.disabled}
            >
              {action.label}
            </button>
          ))}
        </div>
      ) : null}
    </section>
  )
}
