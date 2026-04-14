export function normalizeRealtimeFailedSymbols(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item ?? '').trim()).filter(Boolean)
  }

  const text = String(value ?? '').trim()
  if (!text || text === '-' || text === '无' || text.toLowerCase() === 'none') {
    return []
  }

  return text
    .split(/[,\s/|]+/)
    .map((item) => item.trim())
    .filter(Boolean)
}

export function describeRealtimeSource(source: unknown): {
  label: string
  mode: string
  badgeTone: 'default' | 'brand' | 'good' | 'warn'
  isFallback: boolean
} {
  const value = String(source ?? '').trim()
  switch (value) {
    case 'sina-quote':
      return { label: '新浪实时报价', mode: '主源', badgeTone: 'good', isFallback: false }
    case 'sina-tick':
      return { label: '新浪逐笔回退', mode: '降级', badgeTone: 'warn', isFallback: true }
    case 'eastmoney-tick':
      return { label: '东财逐笔回退', mode: '降级', badgeTone: 'warn', isFallback: true }
    case 'eastmoney-minute':
      return { label: '东财分时回退', mode: '降级', badgeTone: 'warn', isFallback: true }
    case 'mixed':
      return { label: '混合降级结果', mode: '混合', badgeTone: 'warn', isFallback: true }
    default:
      return { label: '暂未刷新', mode: '待刷新', badgeTone: 'default', isFallback: false }
  }
}

export function describeRealtimeSnapshotMode(snapshotBucket: unknown, servedFrom: unknown): {
  label: string
  tone: 'default' | 'brand' | 'good' | 'warn'
} {
  const bucket = String(snapshotBucket ?? '').trim()
  const source = String(servedFrom ?? '').trim()
  if (bucket === 'post_close' && source === 'database') {
    return { label: '今日收盘后快照', tone: 'good' }
  }
  if (bucket === 'post_close') {
    return { label: '盘后快照', tone: 'brand' }
  }
  if (source === 'database-fallback') {
    return { label: '数据库回退', tone: 'warn' }
  }
  if (source === 'database') {
    return { label: '数据库快照', tone: 'brand' }
  }
  return { label: '实时刷新', tone: 'default' }
}

export function formatRealtimeCoverage(requestedCount: unknown, successCount: unknown): string {
  const requested = Number(requestedCount ?? 0)
  const success = Number(successCount ?? 0)
  if (!Number.isFinite(requested) || requested <= 0) {
    return success > 0 ? `${success}` : '-'
  }
  return `${success} / ${requested}`
}
