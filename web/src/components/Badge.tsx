import type { ReactNode } from 'react'

interface BadgeProps {
  children: ReactNode
  tone?: 'default' | 'brand' | 'good' | 'warn'
}

export function Badge({ children, tone = 'default' }: BadgeProps) {
  return <span className={`badge badge--${tone}`}>{children}</span>
}
