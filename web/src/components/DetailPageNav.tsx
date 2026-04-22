interface DetailPageNavProps {
  backLabel?: string
  onBack: () => void
  prevLabel?: string | null
  onPrev?: (() => void) | null
  nextLabel?: string | null
  onNext?: (() => void) | null
}

export function DetailPageNav({
  backLabel = '返回列表',
  onBack,
  prevLabel,
  onPrev,
  nextLabel,
  onNext,
}: DetailPageNavProps) {
  return (
    <div className="inline-actions inline-actions--compact">
      <button type="button" className="button button--primary" onClick={onBack}>
        {backLabel}
      </button>
      <button type="button" className="button button--ghost" onClick={onPrev ?? undefined} disabled={!onPrev || !prevLabel}>
        {prevLabel ? `上一条 · ${prevLabel}` : '上一条'}
      </button>
      <button type="button" className="button button--ghost" onClick={onNext ?? undefined} disabled={!onNext || !nextLabel}>
        {nextLabel ? `下一条 · ${nextLabel}` : '下一条'}
      </button>
    </div>
  )
}
