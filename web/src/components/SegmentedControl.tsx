interface SegmentedOption {
  key: string
  label: string
}

interface SegmentedControlProps {
  label: string
  value: string
  options: SegmentedOption[]
  onChange: (value: string) => void
}

export function SegmentedControl({ label, value, options, onChange }: SegmentedControlProps) {
  if (!options.length) {
    return null
  }

  return (
    <div className="segmented-control" role="group" aria-label={label}>
      {options.map((option) => (
        <button
          key={option.key}
          type="button"
          className={`segmented-control__button${option.key === value ? ' segmented-control__button--active' : ''}`}
          onClick={() => onChange(option.key)}
        >
          {option.label}
        </button>
      ))}
    </div>
  )
}
