# Project Memory - 2026-04-03

This file is the current durable handoff for the workspace after the database and auth refactor moved from "code ready" to "live and usable".

## Current Live Entrypoints

- React workspace: `http://127.0.0.1:5174`
- API base: `http://127.0.0.1:8001`
- Streamlit fallback: `http://127.0.0.1:8501`

## Why The API Moved To 8001

- The machine has stale phantom listeners on port `8000`.
- Those listeners answer HTTP but do not expose the new `/api/auth/*` routes.
- The new FastAPI server is now intentionally started on `8001`.
- The React Vite proxy has been moved to `8001`, so the browser workspace still works through `/api/...`.

## Real Auth Status

- Backend auth/session now uses Postgres database `quant_db`.
- Tables in use:
  - `app_users`
  - `app_sessions`
- Live endpoints now confirmed:
  - `GET /api/auth/session`
  - `POST /api/auth/login`
  - `POST /api/auth/logout`
- Browser-proxied smoke test passed through `http://127.0.0.1:5174/api/...`.

Bootstrap local account:

- Username: `admin`
- Password: `Openlianghua@2026`

## Database Refactor Status

The project is no longer only "auth in DB, research on files". Core dashboard artifacts are now database-backed too.

### Stored In Postgres

- Config artifacts:
  - experiment config
  - watchlist config
- Report artifacts:
  - metrics
  - stability payloads
  - portfolios
  - predictions
  - feature importance
  - diagnostics
  - overlay / inference packets, briefs, candidate tables
  - watch plan / action memo markdown
- Core Parquet blobs:
  - `daily_bar`
  - `feature_panel`
  - `label_panel`
  - `trade_calendar`
  - `stock_basic`

### Schema / SQL Files

- `db/sql/001_app_auth.sql`
- `db/sql/002_dashboard_artifacts.sql`
- `db/sql/003_dashboard_artifact_bytes.sql`

### Sync Entry

- PowerShell helper: `scripts/sync_dashboard_db.ps1`
- Python entry: `python -m src.db.dashboard_sync`

Latest verified sync result:

- `54` dashboard artifacts synced into Postgres

## Automatic Refresh-Into-DB Coverage

These flows now trigger dashboard DB sync after successful output generation:

- `src.data.materialize_cache`
- `src.data.downloader`
- `src.data.myquant_downloader`
- `src.data.myquant_enrich`
- `src.features.build_feature_panel`
- `src.models.train_linear`
- `src.models.train_lgbm`
- `src.models.train_ensemble`
- `src.agents.overlay_report`
- `src.agents.overlay_inference_report`
- `src.agents.watch_plan`
- `src.agents.action_memo`
- dashboard facade actions and config-save paths

## Repository Read Path

The application repositories now prefer Postgres when available:

- `src/app/repositories/config_repository.py`
- `src/app/repositories/report_repository.py`

Important detail:

- `daily_bar`, `feature_panel`, and `label_panel` can now load from database binary artifacts instead of only local parquet files.
- Some raw acquisition caches still originate as files first, then get synchronized into Postgres.

## Validation Snapshot

Validated in this pass:

- `python -m py_compile` on touched DB/API/data-sync files
- `python -m unittest tests.test_database_repositories tests.test_web_api tests.test_web_api_auth tests.test_dashboard_data_service tests.test_dashboard_facade -q`
- `npm run build`
- direct API smoke on `8001`
- browser-proxy auth smoke on `5174`

## Next Recommended Work

1. Move the remaining raw-source cache ownership into explicit database contracts instead of file-first sync.
2. Add repository/service tests around `sync_dashboard_artifacts` coverage.
3. Decide whether `8000` should stay abandoned or be reclaimed by a separate OS-level cleanup.

## 2026-04-03 Afternoon Follow-up

- Cleaned the most visible polluted UI copy in the React shell and key pages:
  - `web/src/App.tsx`
  - `web/src/layout/AppLayout.tsx`
  - `web/src/pages/WorkbenchPage.tsx`
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/pages/CandidatesPage.tsx`
  - `web/src/pages/AiReviewPage.tsx`
  - `web/src/components/QueryNotice.tsx`
  - `web/src/components/LoginDialog.tsx`
- Cleaned shared backend/user-facing copy sources:
  - `src/utils/holding_marks.py`
  - `src/utils/premarket_plan.py`
  - `src/app/services/holding_snapshot_service.py` override copy
  - `src/app/services/dashboard_data_service.py` override labels/actions
  - `src/app/facades/dashboard_facade.py` clean config summary path
- Replaced polluted watchlist names/notes in `config/watchlist.yaml`.
- Rewrote auto-generated markdown templates:
  - `src/agents/watch_plan.py`
  - `src/agents/action_memo.py`
- Regenerated clean reports:
  - `reports/weekly/000078_watch_plan_2026-04-03.md`
  - `reports/weekly/000078_action_memo_2026-04-03.md`
- Updated bootstrap display metadata to ASCII-safe values:
  - `.env`
  - `.env.example`
  - `app_users` row for `admin` now uses `System Admin / Research Admin`
- Validation in this pass:
  - `python -m py_compile` on touched Python files
  - `python -m unittest tests.test_watch_plan tests.test_action_memo tests.test_holding_marks tests.test_dashboard_data_service tests.test_dashboard_facade tests.test_web_api tests.test_web_api_auth -q`
  - `npm run build`
  - endpoint smoke: `5174`, `8001/api/meta`, `8501`
- Regenerated and re-synced dashboard artifacts to Postgres again; latest sync completed with:
  - `Synced 54 dashboard artifacts to Postgres.`
- Added a same-day actionable report for daytime review:
  - `reports/weekly/realtime_stock_recommendation_2026-04-03.md`
- Latest recommendation stance for daytime review:
  - strongest watch: `600487.SH 亨通光电`
  - aggressive second choice: `600339.SH 中油工程`
  - backup only: `600583.SH 海油工程`
  - not recommended to chase today: `300058.SZ 蓝色光标`, `002195.SZ 岩山科技`, `600879.SH 航天电子`, `300136.SZ 信维通信`

## 2026-04-03 Evening Frontend UX Pass

- Added a reusable UI hierarchy block for React pages:
  - `web/src/components/SectionBlock.tsx`
  - shared styles in `web/src/index.css`
- Reworked the React shell and navigation so the top bar, account menu, and left navigation use a cleaner product-style hierarchy:
  - `web/src/layout/AppLayout.tsx`
- Reworked these pages with cleaner action proximity and section hierarchy:
  - `web/src/pages/WorkbenchPage.tsx`
  - `web/src/pages/OverviewPage.tsx`
  - `web/src/pages/FactorExplorerPage.tsx`
  - `web/src/pages/ModelBacktestPage.tsx`
  - `web/src/pages/CandidatesPage.tsx`
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/pages/AiReviewPage.tsx`
  - `web/src/pages/ServicePage.tsx`
- Key UX changes in this pass:
  - page-level summary cards now sit above filters and main tables
  - buttons are grouped closer to the data they affect
  - watchlist actions moved next to the selected holding summary and drawer
  - candidate and AI pages now separate thesis/summary from raw tables
  - workbench separates parameter editing, research actions, and UI settings more clearly
- Validation in this pass:
  - `npm run build`
  - route smoke on `5174`: `/`, `/workspace`, `/factors`, `/backtests`, `/candidates`, `/watchlist`, `/ai-review`, `/service`
  - API smoke on `5174/api/shell`
  - realtime watchlist API smoke on `5174/api/watchlist?include_realtime=true&scope=all&sort_by=inference_rank`

## 2026-04-03 Late Evening UI Density Pass

- Continued the React UX cleanup around high-frequency research pages so the first screen answers “what matters now” before showing raw tables.
- Added reusable entity-style table cells and wired them into the main list views:
  - `web/src/components/EntityCell.tsx`
  - `web/src/components/DataTable.tsx` now supports `cellRenderers`
- Updated `watchlist` main table so the stock column carries code, industry, and pool badges in one place:
  - `web/src/pages/WatchlistPage.tsx`
- Updated `candidates` main table so the first column is now the stock anchor instead of splitting code/name across separate early columns:
  - `web/src/pages/CandidatesPage.tsx`
- Updated `AI review` candidate tables so the first column behaves the same way and confidence can be scanned faster:
  - `web/src/pages/AiReviewPage.tsx`
- Added collapsible second-layer context blocks to keep summary pages lighter while preserving full reasoning context when expanded.
- Shared styling updates were kept in:
  - `web/src/index.css`
- Validation in this pass:
  - `npm run build`
  - route smoke on `5174`: `/candidates`, `/watchlist`, `/ai-review`

## 2026-04-03 Cross-Page Hierarchy Alignment

- Extended the same “entity-first + collapsible context” reading model to overview, factor explorer, and model backtest pages.
- Overview page updates:
  - `web/src/pages/OverviewPage.tsx`
  - model comparison now uses a model-focused first column that combines `model + split + core performance context`
  - secondary context blocks (`数据健康`, `当前最优解`) are now collapsible
- Factor explorer updates:
  - `web/src/pages/FactorExplorerPage.tsx`
  - factor ranking first column now uses an entity-style stock cell with `name + ts_code + top marker`
  - secondary context blocks (`当前因子概况`, `当前股票快照`) are now collapsible
- Model backtest updates:
  - `web/src/pages/ModelBacktestPage.tsx`
  - monthly/feature/year/regime tables now use first-column entity cells to make each row’s context more obvious
  - secondary context blocks (`运行摘要`, `检查结论`) are now collapsible
- Shared rendering reused:
  - `web/src/components/EntityCell.tsx`
  - `web/src/components/DataTable.tsx`
  - `web/src/components/SectionBlock.tsx`
- Validation in this pass:
  - `npm run build`
  - route smoke on `5174`: `/`, `/factors`, `/backtests`

## 2026-04-03 Comparison View + Realtime Snapshot Pass

- Added a reusable horizontal comparison component for React workspaces:
  - `web/src/components/ComparisonBoard.tsx`
  - shared styling in `web/src/index.css`
- Overview page now includes a side-by-side comparison board that lays out annualized return, Sharpe, drawdown, and hit rate across models in the current split:
  - `web/src/pages/OverviewPage.tsx`
- Model backtest page now includes an explicit `验证集 / 测试集` comparison board so sample-in vs sample-out degradation is visible before reading the diagnostic tables:
  - `web/src/pages/ModelBacktestPage.tsx`
- Realtime quote storage is now persisted directly to Postgres instead of only living in request memory:
  - SQL schema: `db/sql/004_realtime_quote_snapshots.sql`
  - store: `src/db/realtime_quote_store.py`
  - orchestration/caching logic: `src/app/services/realtime_quote_service.py`
- Watchlist realtime fetching now uses a managed path:
  - after `15:00` Asia/Shanghai, the system captures one `post_close` snapshot and then reuses the database snapshot for same-day requests
  - if a same-day post-close snapshot already exists, the API serves that snapshot instead of re-requesting remote quote providers
  - if live provider calls fail, the system can fall back to the latest same-day stored snapshot
- Watchlist API/facade now routes through the managed realtime path:
  - `src/app/facades/dashboard_facade.py`
- Realtime banner UI now distinguishes `实时刷新 / 盘后快照 / 数据库回退`:
  - `web/src/components/RealtimeStatusBanner.tsx`
- Service page and workbench now both surface the latest database-backed realtime snapshot summary, so users can see `快照类型 / 抓取时间 / 覆盖率 / 行情来源 / 失败股票` without opening the watchlist page:
  - `web/src/pages/ServicePage.tsx`
  - `web/src/pages/WorkbenchPage.tsx`
  - `src/app/facades/dashboard_facade.py`
- Realtime presentation helpers are now shared across watchlist/banner/service/workbench instead of duplicated page-by-page:
  - `web/src/lib/realtime.ts`
  - `web/src/pages/WatchlistPage.tsx`
- Added a shared page-level context strip so high-frequency pages expose the same first-screen layer of `口径 / 日期 / 当前对象 / 覆盖范围` before tables:
  - `web/src/components/ContextStrip.tsx`
  - `web/src/index.css`
  - applied to:
    - `web/src/pages/OverviewPage.tsx`
    - `web/src/pages/FactorExplorerPage.tsx`
    - `web/src/pages/ModelBacktestPage.tsx`
    - `web/src/pages/CandidatesPage.tsx`
    - `web/src/pages/WatchlistPage.tsx`
    - `web/src/pages/AiReviewPage.tsx`
- Watchlist page now defaults to the latest database-backed realtime snapshot instead of showing an empty realtime area on first load:
  - default `GET /api/watchlist` merges the most recent cached quote snapshot when available
  - external provider fetching still happens only when the user explicitly triggers manual refresh
  - duplicate refresh affordances were consolidated so the primary entrypoint is the realtime status banner; the drawer no longer repeats the same refresh action
  - copy now distinguishes `默认展示最近快照` and `手动拉取最新行情`
- Added regression coverage for post-close snapshot reuse and persistence:
  - `tests/test_realtime_quote_service.py`
- Added regression coverage for the new service payload snapshot shape:
  - `tests/test_dashboard_facade.py`
  - `tests/test_web_api.py`
- Real smoke completed:
  - wrote a real `2026-04-03` `post_close` snapshot to Postgres
  - stored `9` realtime rows
  - API after restart now returns `snapshot_bucket=post_close` and `served_from=database`
- Validation in this pass:
  - `python -m unittest tests.test_realtime_quote_service tests.test_dashboard_facade tests.test_web_api -q`
  - `npm run build`
  - route smoke on `5174`: `/`, `/factors`, `/backtests`, `/candidates`, `/watchlist`, `/ai-review`, `/workspace`, `/service`
  - API smoke on `8001/api/watchlist?include_realtime=true&scope=all&sort_by=inference_rank`
  - route smoke on `5174`: `/`, `/factors`, `/backtests`, `/watchlist`

## 2026-04-03 Spotlight Summary Pass

- Continued the React UI cleanup using the same `entity-first -> context strip -> focused summary -> table/detail` reading order.
- Added a reusable spotlight summary card:
  - `web/src/components/SpotlightCard.tsx`
  - styling in `web/src/index.css`
- Candidate, watchlist, and AI review pages now use the same spotlight summary pattern for the selected symbol / selected record:
  - `web/src/pages/CandidatesPage.tsx`
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/pages/AiReviewPage.tsx`
- Overview, factor explorer, and model backtest pages now also use the same spotlight summary structure at the top of the page instead of bespoke `headline + badges + metric-grid` blocks:
  - `web/src/pages/OverviewPage.tsx`
  - `web/src/pages/FactorExplorerPage.tsx`
  - `web/src/pages/ModelBacktestPage.tsx`
- Watchlist behavior is now clearer:
  - default page load shows the latest reusable database snapshot
  - manual refresh is explicitly labeled as a fresh pull from quote providers
  - duplicate manual refresh actions were removed from the selected-stock area and detail drawer
- Regression / verification completed in this pass:
  - `python -m unittest tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
  - `npm run build`
  - route smoke on `5174`: `/`, `/factors`, `/backtests`, `/candidates`, `/watchlist`, `/ai-review`
  - API smoke on `8001/api/watchlist?scope=all&sort_by=inference_rank`

## 2026-04-03 Table Density + Drawer Consistency Pass

- Continued frontend cleanup with the same ECC-style `focused summary first, dense table second, full fields last` structure.
- Added a reusable drawer summary block:
  - `web/src/components/DetailSummarySection.tsx`
- Candidate and watchlist drawers now share the same summary structure:
  - `web/src/pages/CandidatesPage.tsx`
  - `web/src/pages/WatchlistPage.tsx`
  - both now render `快捷操作 -> 详情摘要 -> 完整字段表`
- Table toolbar is now denser and easier to scan:
  - `web/src/components/DataTable.tsx`
  - `web/src/index.css`
  - it now shows `总行数 / 当前显示列数 / 视图密度 / 当前预设` as compact chips instead of a single loose text label
- Detail drawer close affordance was normalized:
  - `web/src/components/DetailDrawer.tsx`
- Regression / verification completed in this pass:
  - `python -m unittest tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
  - `npm run build`
  - route smoke on `5174`: `/candidates`, `/watchlist`
  - API smoke on `8001/api/watchlist?scope=all&sort_by=inference_rank`

## 2026-04-03 List-First Information Architecture Pass

- Recorded and adopted the new default implementation rule:
  - use ECC guidance by default on coding turns, especially `frontend-patterns`, `coding-standards`, and `verification-loop`
  - prefer `总列表 -> 个体详情` information flow to reduce UI clutter
- Reordered high-frequency pages so list/table comes before per-symbol detail:
  - `web/src/pages/CandidatesPage.tsx`
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/pages/FactorExplorerPage.tsx`
  - `web/src/pages/AiReviewPage.tsx`
- Current page rhythm is now more consistent:
  - top summary/context/filter
  - main list or ranking table
  - selected symbol/detail follow-up
  - full fields / long text last
- Updated wording so page subtitles match the new list-first reading order instead of still saying `先看单票结论`.
- Tightened regression determinism:
  - `tests/test_dashboard_facade.py`
  - fixed the realtime snapshot label test so it no longer depends on the machine's actual wall-clock date
- Verification in this pass:
  - `python -m unittest tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
  - `npm run build`
  - route smoke on `5174`: `/candidates`, `/watchlist`, `/factors`, `/ai-review`
