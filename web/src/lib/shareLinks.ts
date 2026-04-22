import { buildShareablePageUrl } from '../facades/pageUrlState'

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

export function buildAiReviewPath(symbol: string, scope: 'inference' | 'historical' = 'inference'): string {
  return `/ai-review/${scope}/${symbol}`
}

export function buildWatchlistPath(symbol: string): string {
  return `/watchlist/${symbol}`
}

export function buildCandidatesPath(symbol: string): string {
  return `/candidates/${symbol}`
}
