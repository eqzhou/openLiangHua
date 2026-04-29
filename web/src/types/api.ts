export type JsonRecord = Record<string, unknown>

export interface BootstrapPayload {
  modelNames: string[]
  splitNames: string[]
  labelOptions: string[]
  modelLabels: Record<string, string>
  splitLabels: Record<string, string>
  fieldExplanations: Record<string, string>
  metricExplanations: Record<string, string>
  watchScopes: Record<string, string>
  watchSorts: Record<string, string>
  actions: Array<{ actionName: string; label: string; moduleName: string; spinnerText?: string; buttonKey?: string }>
}

export interface ShellPayload {
  bootstrap: BootstrapPayload
  experimentConfig: JsonRecord
  service: ServicePayload
  watchlistEntryCount: number
  configSummaryText: string
}

export interface HomePayload {
  configSummaryText: string
  service: ServicePayload
  overview: {
    selectedSplit: string
    summary: JsonRecord
    bestAnnualized: JsonRecord
    bestSharpe: JsonRecord
    bestDrawdown: JsonRecord
  }
  watchlist: {
    overview: JsonRecord
    realtimeStatus: JsonRecord
    records: JsonRecord[]
    focusRecord: JsonRecord
  }
  candidates: {
    modelName: string
    splitName: string
    latestDate?: string | null
    records: JsonRecord[]
    focusRecord: JsonRecord
  }
  aiReview: {
    inferenceRecords: JsonRecord[]
    historicalRecords: JsonRecord[]
    focusRecord: JsonRecord
    shortlistMarkdown?: string
  }
  alerts: Array<{
    tone: string
    title: string
    detail: string
  }>
}

export interface HomeSummaryPayload {
  configSummaryText: string
  service: ServicePayload
  overview: {
    selectedSplit: string
    summary: JsonRecord
    bestAnnualized: JsonRecord
    bestSharpe: JsonRecord
    bestDrawdown: JsonRecord
  }
  alerts: Array<{
    tone: string
    title: string
    detail: string
  }>
}

export interface HomeWatchlistPayload {
  overview: JsonRecord
  realtimeStatus: JsonRecord
  records: JsonRecord[]
  focusRecord: JsonRecord
}

export interface HomeCandidatesPayload {
  modelName: string
  splitName: string
  latestDate?: string | null
  records: JsonRecord[]
  focusRecord: JsonRecord
}

export interface HomeAiReviewPayload {
  inferenceRecords: JsonRecord[]
  historicalRecords: JsonRecord[]
  focusRecord: JsonRecord
  shortlistMarkdown?: string
}

export interface ActionResult {
  actionName: string
  label?: string
  ok: boolean
  output: string
}

export interface AuthUserPayload {
  userId: string
  username: string
  displayName: string
  title?: string | null
  sessionExpiresAt?: string | null
}

export interface AuthSessionPayload {
  authenticated: boolean
  user: AuthUserPayload | null
}

export interface RealtimeRefreshPayload {
  ok: boolean
  symbolCount: number
  realtimeRecordCount: number
  realtimeStatus: JsonRecord
}

export interface DataArtifactStatus {
  exists: boolean
  rowCount: number
  symbolCount: number
  latestTradeDate?: string | null
  updatedAt?: string | null
}

export interface DataManagementPayload {
  targetSource: string
  activeDataSource: string
  configuredDataSource?: string
  sourceMismatch?: boolean
  today: string
  envPath?: string | null
  envFileExists: boolean
  tokenConfigured?: boolean | null
  dailyBar: DataArtifactStatus
  researchPanel: DataArtifactStatus
  legacyFeatureView: DataArtifactStatus
  legacyLabelView: DataArtifactStatus
  datasetSummary: JsonRecord
  scripts: {
    incremental: string
    fullRefresh: string
  }
}

export interface MyquantStatusPayload {
  targetSource: string
  envFileExists: boolean
  tokenConfigured?: boolean | null
  sdkAvailable: boolean
  dailyBar: DataArtifactStatus
  qualityReportExists?: boolean
  scripts: {
    download: string
    enrich: string
    researchRefresh: string
  }
}

export interface OverviewPayload {
  summary: JsonRecord
  comparison: JsonRecord[]
  equityCurves: JsonRecord[]
  selectedSplit: string
}

export interface OverviewSummaryPayload {
  summary: JsonRecord
  comparison: JsonRecord[]
  selectedSplit: string
}

export interface OverviewCurvesPayload {
  equityCurves: JsonRecord[]
  selectedSplit: string
}

export interface FactorPayload {
  available: boolean
  latestDate?: string
  selectedFactor?: string
  selectedHistoryFactor?: string
  selectedSymbol?: string
  factorOptions: Array<{ key: string; label: string; description: string }>
  symbolOptions: string[]
  ranking: JsonRecord[]
  missingRates: JsonRecord[]
  history: JsonRecord[]
  snapshot: JsonRecord[]
  selectedRecord?: JsonRecord
}

export interface FactorSummaryPayload {
  available: boolean
  latestDate?: string
  selectedFactor?: string
  selectedHistoryFactor?: string
  selectedSymbol?: string
  factorOptions: Array<{ key: string; label: string; description: string }>
  symbolOptions: string[]
  ranking: JsonRecord[]
  missingRates: JsonRecord[]
  selectedRecord?: JsonRecord
}

export interface FactorDetailPayload {
  selectedFactor?: string
  selectedHistoryFactor?: string
  selectedSymbol?: string
  history: JsonRecord[]
  snapshot: JsonRecord[]
}

export interface BacktestPayload {
  modelName: string
  splitName: string
  metrics: JsonRecord
  stability: JsonRecord
  importance: JsonRecord[]
  portfolio: JsonRecord[]
  monthlySummary: JsonRecord[]
  yearlyDiagnostics: JsonRecord[]
  regimeDiagnostics: JsonRecord[]
}

export interface BacktestSummaryPayload {
  modelName: string
  splitName: string
  metrics: JsonRecord
  stability: JsonRecord
}

export interface BacktestPortfolioPayload {
  modelName: string
  splitName: string
  portfolio: JsonRecord[]
  monthlySummary: JsonRecord[]
}

export interface BacktestDiagnosticsPayload {
  modelName: string
  splitName: string
  importance: JsonRecord[]
  yearlyDiagnostics: JsonRecord[]
  regimeDiagnostics: JsonRecord[]
}

export interface CandidatesPayload {
  modelName: string
  splitName: string
  topN: number
  latestDate?: string
  selectedSymbol?: string
  symbolOptions: string[]
  latestPicks: JsonRecord[]
  scoreHistory: JsonRecord[]
}

export interface CandidatesSummaryPayload {
  modelName: string
  splitName: string
  topN: number
  page: number
  pageSize: number
  totalCount: number
  totalPages: number
  latestDate?: string
  selectedSymbol?: string
  symbolOptions: string[]
  latestPicks: JsonRecord[]
  selectedRecord: JsonRecord
}

export interface CandidateHistoryPayload {
  modelName: string
  splitName: string
  selectedSymbol?: string
  scoreHistory: JsonRecord[]
}

export interface CandidateDetailPayload {
  modelName: string
  splitName: string
  selectedSymbol?: string
  selectedRecord: JsonRecord
  fieldRows: JsonRecord[]
}

export interface WatchlistPayload {
  overview: JsonRecord
  realtimeStatus: JsonRecord
  selectedSymbol?: string
  filteredCount: number
  records: JsonRecord[]
  detail: JsonRecord
  reducePlan: JsonRecord[]
  history: JsonRecord[]
  discussionRows: JsonRecord[]
  watchPlan: JsonRecord
  actionMemo: JsonRecord
}

export interface WatchlistSummaryPayload {
  overview: JsonRecord
  realtimeStatus: JsonRecord
  filters: JsonRecord
  page: number
  pageSize: number
  totalPages: number
  refreshSymbols: string[]
  refreshPreviousCloses: JsonRecord
  selectedSymbol?: string
  filteredCount: number
  records: JsonRecord[]
  selectedRecord: JsonRecord
}

export interface WatchlistDetailPayload {
  selectedSymbol?: string
  detail: JsonRecord
  reducePlan: JsonRecord[]
  history: JsonRecord[]
  discussionRows: JsonRecord[]
  watchPlan: JsonRecord
  actionMemo: JsonRecord
  latestAiShortlist?: string
}

export interface AiPanelPayload {
  selectedSymbol?: string
  candidates: JsonRecord[]
  packet: JsonRecord
  brief: string
  selectedRecord: JsonRecord
  llmResponse: JsonRecord
  responseSummary?: string
}

export interface AiReviewPayload {
  inference: AiPanelPayload
  historical: AiPanelPayload
}

export interface AiPanelSummaryPayload {
  selectedSymbol?: string
  candidateCount: number
  candidates: JsonRecord[]
  selectedRecord: JsonRecord
}

export interface AiReviewSummaryPayload {
  inference: AiPanelSummaryPayload
  historical: AiPanelSummaryPayload
}

export interface AiReviewDetailPayload {
  selectedSymbol?: string
  selectedRecord: JsonRecord
  fieldRows: JsonRecord[]
  brief: string
  llmResponse: JsonRecord
  responseSummary?: string
}

export type ServicePayload = JsonRecord
