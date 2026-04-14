import type { JsonRecord } from '../types/api'

const FIELD_LABELS: Record<string, string> = {
  field: '字段',
  value: '内容',
  trade_date: '截面日期',
  latest_date: '最新日期',
  latest_bar_date: '最新日线日期',
  latest_bar_close: '最新日线收盘',
  signal_date: '历史验证日期',
  inference_signal_date: '最新推理日期',
  plan_date: '计划日期',
  mark_date: '估值日期',
  fetched_at: '抓取时间',
  ts_code: '股票代码',
  name: '股票名称',
  industry: '行业编码',
  industry_display: '行业',
  theme_tags: '主题标签',
  action_hint: '建议动作',
  final_score: '综合得分',
  quant_score: '量化得分',
  factor_overlay_score: '叠加得分',
  model_consensus: '模型一致性',
  confidence_level: '置信等级',
  lgbm_rank_pct: 'LGBM 分位',
  ridge_rank_pct: 'Ridge 分位',
  bull_points: '看多要点',
  risk_points: '风险提示',
  notice_digest: '公告摘要',
  news_digest: '新闻摘要',
  news_source: '资讯来源',
  research_digest: '研报摘要',
  thesis_summary: '核心论点',
  ai_brief: 'AI 摘要',
  module_name: '模块',
  split_name: '样本分段',
  model_name: '模型',
  entry_group: '分组',
  watch_level: '关注级别',
  action_brief: '执行提示',
  focus_note: '关注备注',
  mark_price: '当前估值',
  cost_basis: '成本价',
  shares: '持股数量',
  price_source: '价格来源',
  market_value: '参考市值',
  unrealized_pnl: '浮动盈亏',
  unrealized_pnl_pct: '浮动收益率',
  ensemble_rank: '历史验证排名',
  ensemble_rank_pct: '历史验证分位',
  inference_ensemble_rank: '最新推理排名',
  inference_ensemble_rank_pct: '最新推理分位',
  score: '融合分数',
  score_raw: '原始分数',
  inference_score: '推理分数',
  mom_5: '5日动量',
  mom_20: '20日动量',
  mom_60: '60日动量',
  close_to_ma_20: '距20日线',
  close_to_ma_60: '距60日线',
  drawdown_60: '60日回撤',
  pct_chg: '最新涨跌幅',
  is_manual_mark: '手工估值',
  is_overlay_selected: '历史精选',
  is_inference_overlay_selected: '最新推理入池',
  manual_mark_note: '手工估值说明',
  is_watch_only: '仅观察',
  premarket_plan: '盘前执行建议',
  premarket_plan_source: '建议来源',
  mark_status: '价格状态',
  mark_status_note: '状态说明',
  mark_vs_latest_bar_days: '距最新日线天数',
  breakeven_price: '解套价',
  gap_to_breakeven_pct: '距解套空间',
  halfway_recovery_price: '半程修复位',
  defensive_price: '防守位',
  ranking_note: '排名备注',
  llm_round_count: '研讨轮次',
  llm_selected_round_count: '入选轮次',
  llm_success_round_count: '成功轮次',
  llm_latest_round: '最近轮次',
  llm_latest_status: '最近状态',
  llm_latest_summary: '最近摘要',
  llm_overview: '研讨概览',
  source: '来源',
  available: '可用',
  requested_symbol_count: '请求股票数',
  success_symbol_count: '成功股票数',
  failed_symbols: '失败股票',
  error_message: '错误信息',
  realtime_snapshot: '实时快照',
  snapshot_bucket: '快照类型',
  snapshot_label_display: '快照说明',
  served_from: '数据入口',
  is_today: '是否当天',
  age_days: '距今天数',
  totalCount: '总数',
  overlayCount: '历史精选数',
  inferenceOverlayCount: '最新推理池',
  status_label_display: '服务状态',
  streamlit_pid: '页面进程',
  supervisor_pid: '守护进程',
  listener_present: '端口监听',
  listener_pid: '监听进程',
}

const DATE_ONLY_FIELDS = new Set([
  'trade_date',
  'latest_date',
  'latest_bar_date',
  'signal_date',
  'inference_signal_date',
  'plan_date',
  'mark_date',
])

function hasChineseCharacters(value: string): boolean {
  return /[\u3400-\u9fff]/.test(value)
}

function humanizeField(field: string): string {
  if (hasChineseCharacters(field)) {
    return field
  }

  const spaced = field
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/_/g, ' ')
    .trim()

  if (!spaced) {
    return field
  }

  return spaced.replace(/\b\w/g, (segment) => segment.toUpperCase())
}

function stringifyStructuredValue(value: unknown): string {
  try {
    const serialized = JSON.stringify(value, null, 2)
    if (!serialized) {
      return '-'
    }
    return serialized.length > 240 ? `${serialized.slice(0, 240)}...` : serialized
  } catch {
    return String(value)
  }
}

function isPercentField(field: string): boolean {
  return (
    /(^mom_\d+$)|(_pct$)|(^pct_)|(drawdown)|(close_to_ma)/.test(field) ||
    field.endsWith('_rank_pct') ||
    field === 'pct_chg'
  )
}

export function getFieldLabel(field: string, overrides: Record<string, string> = {}): string {
  return overrides[field] ?? FIELD_LABELS[field] ?? humanizeField(field)
}

export function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === '') {
    return '-'
  }

  if (typeof value === 'number') {
    if (Math.abs(value) >= 1000) {
      return value.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
    }
    return value.toLocaleString('zh-CN', { maximumFractionDigits: 4 })
  }

  if (typeof value === 'boolean') {
    return value ? '是' : '否'
  }

  if (Array.isArray(value) || typeof value === 'object') {
    return stringifyStructuredValue(value)
  }

  return String(value)
}

export function formatPercent(value: unknown): string {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return '-'
  }

  return `${(value * 100).toFixed(2)}%`
}

export function formatDate(value: unknown): string {
  if (!value) {
    return '-'
  }

  const raw = String(value)
  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (isoMatch) {
    return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`
  }

  const date = new Date(raw)
  if (Number.isNaN(date.getTime())) {
    return raw
  }

  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date)
}

export function formatDateTime(value: unknown): string {
  if (!value) {
    return '-'
  }

  const raw = String(value)
  const dateTimeMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2})/)
  if (dateTimeMatch) {
    return `${dateTimeMatch[1]}-${dateTimeMatch[2]}-${dateTimeMatch[3]} ${dateTimeMatch[4]}:${dateTimeMatch[5]}`
  }

  const date = new Date(raw)
  if (Number.isNaN(date.getTime())) {
    return raw
  }

  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

export function formatCellValue(field: string, value: unknown): string {
  if (value === null || value === undefined || value === '') {
    return '-'
  }

  if (DATE_ONLY_FIELDS.has(field)) {
    return formatDate(value)
  }

  if (field.endsWith('_at')) {
    return formatDateTime(value)
  }

  if (isPercentField(field) && typeof value === 'number') {
    return formatPercent(value)
  }

  return formatValue(value)
}

export function splitTextPoints(value: unknown): string[] {
  return String(value ?? '')
    .split(/[;;。]\s*|\r?\n+/)
    .map((item) => item.trim())
    .filter(Boolean)
}

export function splitTags(value: unknown): string[] {
  return String(value ?? '')
    .split(/\s*(?:\/|\|)\s*/)
    .map((item) => item.trim())
    .filter(Boolean)
}

export function recordToFieldRows(record: JsonRecord): JsonRecord[] {
  return Object.entries(record).map(([field, value]) => ({
    field: getFieldLabel(field),
    value: formatCellValue(field, value),
  }))
}
