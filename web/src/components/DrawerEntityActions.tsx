import type { ReactNode } from 'react'

interface DrawerEntityActionsProps {
  onCopySymbol: () => void | Promise<void>
  onCopyShareLink: () => void | Promise<void>
  onOpenAiReview?: (() => void) | null
  onOpenWatchlist?: (() => void) | null
  onOpenCandidates?: (() => void) | null
  extraActions?: ReactNode
}

export function DrawerEntityActions({
  onCopySymbol,
  onCopyShareLink,
  onOpenAiReview = null,
  onOpenWatchlist = null,
  onOpenCandidates = null,
  extraActions = null,
}: DrawerEntityActionsProps) {
  return (
    <div className="inline-actions inline-actions--wrap">
      <button type="button" className="button button--ghost" onClick={() => void onCopySymbol()}>
        复制代码
      </button>
      <button type="button" className="button button--ghost" onClick={() => void onCopyShareLink()}>
        复制当前视图
      </button>
      {onOpenAiReview ? (
        <button type="button" className="button button--ghost" onClick={onOpenAiReview}>
          查看 AI 研判
        </button>
      ) : null}
      {onOpenWatchlist ? (
        <button type="button" className="button button--ghost" onClick={onOpenWatchlist}>
          跳到持仓页
        </button>
      ) : null}
      {onOpenCandidates ? (
        <button type="button" className="button button--ghost" onClick={onOpenCandidates}>
          跳到候选页
        </button>
      ) : null}
      {extraActions}
    </div>
  )
}
