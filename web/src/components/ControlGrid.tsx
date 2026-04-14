import type { PropsWithChildren } from 'react'

type ControlGridVariant = 'double' | 'triple' | 'quad'

interface ControlGridProps extends PropsWithChildren {
  variant?: ControlGridVariant
}

export function ControlGrid({ variant, children }: ControlGridProps) {
  const className = variant ? `control-grid control-grid--${variant}` : 'control-grid'
  return <div className={className}>{children}</div>
}
