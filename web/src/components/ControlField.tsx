import type { PropsWithChildren } from 'react'

interface ControlFieldProps extends PropsWithChildren {
  label: string
}

export function ControlField({ label, children }: ControlFieldProps) {
  return (
    <label className="control-field">
      <span className="control-field__label">{label}</span>
      <div className="control-field__control">{children}</div>
    </label>
  )
}
