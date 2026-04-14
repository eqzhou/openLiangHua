import { buildApiPath, readClampedIntParam, readStringParam, type UrlStateValue } from './pageUrlState'

type QueryKey = readonly unknown[]

export interface SearchPageClient<TParams> {
  readSearchParams: (searchParams: URLSearchParams) => TParams
  toSearchUpdates: (updates: Partial<TParams>) => Record<string, UrlStateValue>
  queryKey: (params: TParams) => QueryKey
  path: (params: TParams) => string
}

export interface OverviewPageParams {
  split: string
}

export interface FactorExplorerPageParams {
  factor: string
  historyFactor: string
  symbol: string
}

export interface ModelBacktestPageParams {
  model: string
  split: string
}

export interface CandidatesPageParams {
  model: string
  split: string
  topN: number
  symbol: string
}

export interface WatchlistPageParams {
  keyword: string
  scope: string
  sortBy: string
  symbol: string
}

export interface AiReviewPageParams {
  inference: string
  historical: string
}

export const overviewPageClient: SearchPageClient<OverviewPageParams> = {
  readSearchParams: (searchParams) => ({
    split: readStringParam(searchParams, 'split', 'test'),
  }),
  toSearchUpdates: (updates) => ({
    split: updates.split,
  }),
  queryKey: (params) => ['overview', params.split],
  path: (params) => buildApiPath('/api/overview', { split_name: params.split }),
}

export const factorExplorerPageClient: SearchPageClient<FactorExplorerPageParams> = {
  readSearchParams: (searchParams) => ({
    factor: readStringParam(searchParams, 'factor'),
    historyFactor: readStringParam(searchParams, 'historyFactor'),
    symbol: readStringParam(searchParams, 'symbol'),
  }),
  toSearchUpdates: (updates) => ({
    factor: updates.factor,
    historyFactor: updates.historyFactor,
    symbol: updates.symbol,
  }),
  queryKey: (params) => ['factors', params.factor, params.historyFactor, params.symbol],
  path: (params) =>
    buildApiPath('/api/factors', {
      factor_name: params.factor,
      history_factor: params.historyFactor,
      symbol: params.symbol,
    }),
}

export const factorExplorerSummaryClient = {
  queryKey: (params: FactorExplorerPageParams): QueryKey => ['factors-summary', params.factor, params.historyFactor, params.symbol],
  path: (params: FactorExplorerPageParams): string =>
    buildApiPath('/api/factors/summary', {
      factor_name: params.factor,
      history_factor: params.historyFactor,
      symbol: params.symbol,
    }),
}

export const factorExplorerDetailClient = {
  queryKey: (params: FactorExplorerPageParams, symbol: string): QueryKey => [
    'factors-detail',
    params.factor,
    params.historyFactor,
    symbol,
  ],
  path: (params: FactorExplorerPageParams, symbol: string): string =>
    buildApiPath('/api/factors/detail', {
      factor_name: params.factor,
      history_factor: params.historyFactor,
      symbol,
    }),
}

export const modelBacktestPageClient: SearchPageClient<ModelBacktestPageParams> = {
  readSearchParams: (searchParams) => ({
    model: readStringParam(searchParams, 'model', 'lgbm'),
    split: readStringParam(searchParams, 'split', 'test'),
  }),
  toSearchUpdates: (updates) => ({
    model: updates.model,
    split: updates.split,
  }),
  queryKey: (params) => ['backtests', params.model, params.split],
  path: (params) =>
    buildApiPath('/api/backtests', {
      model_name: params.model,
      split_name: params.split,
    }),
}

export const candidatesPageClient: SearchPageClient<CandidatesPageParams> = {
  readSearchParams: (searchParams) => ({
    model: readStringParam(searchParams, 'model', 'lgbm'),
    split: readStringParam(searchParams, 'split', 'test'),
    topN: readClampedIntParam(searchParams, 'topN', { fallback: 10, min: 1, max: 100 }),
    symbol: readStringParam(searchParams, 'symbol'),
  }),
  toSearchUpdates: (updates) => ({
    model: updates.model,
    split: updates.split,
    topN: updates.topN,
    symbol: updates.symbol,
  }),
  queryKey: (params) => ['candidates', params.model, params.split, params.topN, params.symbol],
  path: (params) =>
    buildApiPath('/api/candidates', {
      model_name: params.model,
      split_name: params.split,
      top_n: params.topN,
      symbol: params.symbol,
    }),
}

export const candidatesSummaryClient = {
  queryKey: (params: CandidatesPageParams): QueryKey => ['candidates-summary', params.model, params.split, params.topN, params.symbol],
  path: (params: CandidatesPageParams): string =>
    buildApiPath('/api/candidates/summary', {
      model_name: params.model,
      split_name: params.split,
      top_n: params.topN,
      symbol: params.symbol,
    }),
}

export const candidateHistoryClient = {
  queryKey: (params: CandidatesPageParams, symbol: string): QueryKey => ['candidates-history', params.model, params.split, symbol],
  path: (params: CandidatesPageParams, symbol: string): string =>
    buildApiPath('/api/candidates/history', {
      model_name: params.model,
      split_name: params.split,
      symbol,
    }),
}

export const watchlistPageClient: SearchPageClient<WatchlistPageParams> & {
  realtimePath: (params: WatchlistPageParams) => string
  watchPlanActionPath: string
  actionMemoActionPath: string
} = {
  readSearchParams: (searchParams) => ({
    keyword: readStringParam(searchParams, 'keyword'),
    scope: readStringParam(searchParams, 'scope', 'all'),
    sortBy: readStringParam(searchParams, 'sortBy', 'inference_rank'),
    symbol: readStringParam(searchParams, 'symbol'),
  }),
  toSearchUpdates: (updates) => ({
    keyword: updates.keyword,
    scope: updates.scope,
    sortBy: updates.sortBy,
    symbol: updates.symbol,
  }),
  queryKey: (params) => ['watchlist', params.keyword, params.scope, params.sortBy, params.symbol],
  path: (params) =>
    buildApiPath('/api/watchlist', {
      keyword: params.keyword,
      scope: params.scope,
      sort_by: params.sortBy,
      symbol: params.symbol,
    }),
  realtimePath: (params) =>
    buildApiPath('/api/watchlist', {
      keyword: params.keyword,
      scope: params.scope,
      sort_by: params.sortBy,
      symbol: params.symbol,
      include_realtime: true,
    }),
  watchPlanActionPath: '/api/actions/watch-plan',
  actionMemoActionPath: '/api/actions/action-memo',
}

export const watchlistSummaryClient = {
  queryKey: (params: WatchlistPageParams): QueryKey => ['watchlist-summary', params.keyword, params.scope, params.sortBy, params.symbol],
  path: (params: WatchlistPageParams): string =>
    buildApiPath('/api/watchlist/summary', {
      keyword: params.keyword,
      scope: params.scope,
      sort_by: params.sortBy,
      symbol: params.symbol,
    }),
  realtimePath: (params: WatchlistPageParams): string =>
    buildApiPath('/api/watchlist/summary', {
      keyword: params.keyword,
      scope: params.scope,
      sort_by: params.sortBy,
      symbol: params.symbol,
      include_realtime: true,
    }),
}

export const watchlistDetailClient = {
  queryKey: (params: WatchlistPageParams, symbol: string): QueryKey => ['watchlist-detail', params.keyword, params.scope, params.sortBy, symbol],
  path: (params: WatchlistPageParams, symbol: string): string =>
    buildApiPath('/api/watchlist/detail', {
      keyword: params.keyword,
      scope: params.scope,
      sort_by: params.sortBy,
      symbol,
    }),
}

export const aiReviewPageClient: SearchPageClient<AiReviewPageParams> = {
  readSearchParams: (searchParams) => ({
    inference: readStringParam(searchParams, 'inference'),
    historical: readStringParam(searchParams, 'historical'),
  }),
  toSearchUpdates: (updates) => ({
    inference: updates.inference,
    historical: updates.historical,
  }),
  queryKey: (params) => ['ai-review', params.inference, params.historical],
  path: (params) =>
    buildApiPath('/api/ai-review', {
      inference_symbol: params.inference,
      historical_symbol: params.historical,
    }),
}

export const aiReviewSummaryClient = {
  queryKey: (params: AiReviewPageParams): QueryKey => ['ai-review-summary', params.inference, params.historical],
  path: (params: AiReviewPageParams): string =>
    buildApiPath('/api/ai-review/summary', {
      inference_symbol: params.inference,
      historical_symbol: params.historical,
    }),
}

export const aiReviewDetailClient = {
  queryKey: (scope: 'inference' | 'historical', symbol: string): QueryKey => ['ai-review-detail', scope, symbol],
  path: (scope: 'inference' | 'historical', symbol: string): string =>
    buildApiPath('/api/ai-review/detail', {
      scope,
      symbol,
    }),
}

export const servicePageClient = {
  queryKey: (): QueryKey => ['service-page'],
  path: (): string => '/api/service',
}

export const homePageClient = {
  queryKey: (): QueryKey => ['home-page'],
  path: (): string => '/api/home',
}

export const shellClient = {
  queryKey: (): QueryKey => ['shell'],
  path: (): string => '/api/shell',
}
