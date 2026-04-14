import { buildShareablePageUrl } from '../facades/pageUrlState'

function buildPathWithSearch(pathname: string, params: Record<string, string | null | undefined>): string {
  const search = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (!value) {
      return
    }
    search.set(key, value)
  })
  const query = search.toString()
  return query ? `${pathname}?${query}` : pathname
}

export async function copyTextToClipboard(text: string) {
  if (!navigator.clipboard?.writeText) {
    throw new Error('当前浏览器不支持剪贴板写入。')
  }
  await navigator.clipboard.writeText(text)
}

export async function copyShareablePageLink(pathname: string, search: string): Promise<string> {
  const shareUrl = buildShareablePageUrl(pathname, search)
  await copyTextToClipboard(shareUrl)
  return shareUrl
}

export function buildAiReviewPath(symbol: string): string {
  return buildPathWithSearch('/ai-review', {
    inference: symbol,
    historical: symbol,
  })
}

export function buildWatchlistPath(symbol: string): string {
  return buildPathWithSearch('/watchlist', {
    symbol,
  })
}

export function buildCandidatesPath(symbol: string): string {
  return buildPathWithSearch('/candidates', {
    symbol,
  })
}
