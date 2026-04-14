# PROJECT MEMORY 2026-04-04

## Context

- Workspace: `D:\openlianghua`
- Active UI stack:
  - React app: `http://127.0.0.1:5174`
  - API: `http://127.0.0.1:8001`
  - Streamlit fallback: `http://127.0.0.1:8501`
- Working preference recorded and adopted:
  - default to ECC guidance on coding turns, especially `frontend-patterns`, `coding-standards`, and `verification-loop`
  - prioritize `总列表 -> 个体详情` information architecture

## 2026-04-04 Overview / Backtest / Service Hierarchy Pass

- Reordered the remaining heavier pages so they follow the same reading rhythm as the high-frequency pages:
  - `web/src/pages/OverviewPage.tsx`
  - `web/src/pages/ModelBacktestPage.tsx`
  - `web/src/pages/ServicePage.tsx`
- `总览` now reads in this order:
  - top conclusion + context
  - horizontal comparison board
  - model comparison table
  - equity curve
  - supporting detail blocks (`数据快照`, `最佳模型摘要`)
- `模型回测` now reads in this order:
  - top conclusion + context
  - valid/test comparison board
  - main chart area
  - main diagnostic tables
  - supporting detail blocks (`策略概览`, `稳定性判断`)
- `服务页` now reads in this order:
  - top runtime conclusion
  - `关键服务字段` table
  - supporting detail blocks (`实时快照明细`, `服务摘要明细`)
  - logs

## 2026-04-04 Workbench / Copy Cleanup Pass

- Cleaned and normalized remaining polluted text on the workbench page:
  - `web/src/pages/WorkbenchPage.tsx`
- Confirmed the shared collapse control copy is clean:
  - `web/src/components/SectionBlock.tsx`
- The workbench now uses clear Chinese copy for:
  - current status
  - realtime snapshot reminder
  - research parameters
  - research actions
  - interface settings
- Searched `web/src` for previously seen mojibake-style residue after this pass and found no remaining matches in the current source tree.

## Verification

- Build:
  - `npm run build`
- Python regression:
  - `D:\openlianghua\.venv\Scripts\python.exe -m unittest tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
- Route/API smoke:
  - `http://127.0.0.1:5174/`
  - `http://127.0.0.1:5174/backtests`
  - `http://127.0.0.1:5174/service`
  - `http://127.0.0.1:5174/workspace`
  - `http://127.0.0.1:8001/api/service`
- All checks passed with `200` responses where applicable.

## 2026-04-04 Architecture Decision + Phase 1 Start

- Added two new durable decision docs:
  - `docs/SYSTEM_ARCHITECTURE_BLUEPRINT_2026-04-04.md`
  - `docs/MIGRATION_PLAN_2026-04-04.md`
- Decision recorded:
  - do not rewrite from zero
  - continue the React + FastAPI + facade + services migration
  - keep Streamlit as fallback only
- Phase 1 started with an explicit boundary cleanup:
  - created `src/app/viewmodels/`
  - moved shared payload helper logic out of page modules into:
    - `src/app/viewmodels/overview_vm.py`
    - `src/app/viewmodels/factor_explorer_vm.py`
    - `src/app/viewmodels/model_backtest_vm.py`
    - `src/app/viewmodels/candidates_vm.py`
    - `src/app/viewmodels/ai_review_vm.py`
  - `src/app/facades/dashboard_facade.py` now imports those helpers from `viewmodels` instead of `pages`
  - related tests now also import from `viewmodels`, so page modules are no longer the default home for shared payload logic
- Validation in this pass:
  - `D:\openlianghua\.venv\Scripts\python.exe -m unittest tests.test_overview_page tests.test_factor_explorer_page tests.test_model_backtest_page tests.test_candidates_page tests.test_ai_review_page tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
  - `D:\openlianghua\.venv\Scripts\python.exe -m py_compile D:\openlianghua\src\app\viewmodels\overview_vm.py D:\openlianghua\src\app\viewmodels\factor_explorer_vm.py D:\openlianghua\src\app\viewmodels\model_backtest_vm.py D:\openlianghua\src\app\viewmodels\candidates_vm.py D:\openlianghua\src\app\viewmodels\ai_review_vm.py D:\openlianghua\src\app\facades\dashboard_facade.py`

## 2026-04-04 Phase 1 Boundary Cleanup Complete

- Phase 1 is now complete and Phase 2 is ready to begin.
- Boundary fixes completed in this pass:
  - added `src/app/repositories/holding_repository.py`
  - updated `src/app/services/holding_snapshot_service.py` so holding snapshot assembly reads through repository functions instead of touching report/data files directly
  - tightened `src/app/repositories/config_repository.py` so explicit non-project roots do not accidentally read from or write to shared database-backed config state
  - updated `src/app/facades/dashboard_facade.py` so AI review payloads preload `responseSummary` instead of leaving file-path reads to the page layer
  - updated `src/app/pages/ai_review_page.py` and `streamlit_app.py` so the active payload render path no longer performs raw artifact reads
- Boundary audit summary after this pass:
  - active `src/app/services/` modules no longer perform direct file reads for dashboard payload assembly
  - shared facade payload assembly no longer imports helper logic from page modules
  - the active Streamlit payload path consumes prebuilt payloads instead of opening response-summary files at render time
- Verification gate for Phase 1 completion:
  - `npm run build`
  - `D:\openlianghua\.venv\Scripts\python.exe -m py_compile D:\openlianghua\src\app\repositories\holding_repository.py D:\openlianghua\src\app\repositories\config_repository.py D:\openlianghua\src\app\services\holding_snapshot_service.py D:\openlianghua\src\app\facades\dashboard_facade.py D:\openlianghua\src\app\pages\ai_review_page.py D:\openlianghua\streamlit_app.py`
  - `D:\openlianghua\.venv\Scripts\python.exe -m unittest tests.test_overview_page tests.test_factor_explorer_page tests.test_model_backtest_page tests.test_candidates_page tests.test_ai_review_page tests.test_watch_plan tests.test_action_memo tests.test_app_layer tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
  - smoke checks passed:
    - `http://127.0.0.1:5174/`
    - `http://127.0.0.1:5174/watchlist`
    - `http://127.0.0.1:5174/candidates`
    - `http://127.0.0.1:5174/backtests`
    - `http://127.0.0.1:5174/service`
  - `http://127.0.0.1:8001/api/watchlist?scope=all&sort_by=inference_rank`
  - `http://127.0.0.1:8001/api/service`

## 2026-04-04 Phase 2 Kickoff: React Operator Home

- Phase 2 has started.
- Added a new operator-home API payload:
  - `src/app/facades/dashboard_facade.py -> get_home_payload()`
  - `src/web_api/app.py -> GET /api/home`
- Added a new React main operator page:
  - `web/src/pages/HomePage.tsx`
- Route structure updated:
  - `/` is now the primary operator home
  - `/overview` now holds the previous research overview page
  - `web/src/layout/AppLayout.tsx` navigation was updated to reflect:
    - operator home
    - advanced workbench
    - research pages
    - service page
- The operator home now brings together:
  - current service and realtime snapshot status
  - quick actions for the main daily workflow
  - focus watchlist list
  - model candidate list
  - AI inference list
  - latest action result
- Added a `home` route contract in:
  - `web/src/facades/dashboardPageClient.ts`
  - `web/src/types/api.ts`
- Added tests for the new operator-home contract:
  - `tests/test_dashboard_facade.py`
  - `tests/test_web_api.py`
- Important performance note:
  - the raw watchlist assembly is still expensive
  - `dashboard_facade` now caches the home payload in-process
  - cache invalidation is wired to the existing dashboard cache clear path
  - after warm-up, repeated `/api/home` calls are fast
- API process on port `8001` was restarted to load the new route and payload code.

## 2026-04-04 Phase 2 Verification

- Python regression:
  - `D:\openlianghua\.venv\Scripts\python.exe -m unittest tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
- Frontend build:
  - `cd D:\openlianghua\web && npm run build`
- Smoke checks:
  - `http://127.0.0.1:5174/`
  - `http://127.0.0.1:5174/overview`
  - `http://127.0.0.1:5174/workspace`
  - `http://127.0.0.1:5174/watchlist`
  - `http://127.0.0.1:8001/api/home`
  - `http://127.0.0.1:8001/api/overview`
  - `http://127.0.0.1:8001/api/service`
- All checks passed.

## 2026-04-04 Phase 2 Cold-Start Snapshot Pass

- Continued Phase 2 with a focused cold-start reduction for the operator home and watchlist summary path.
- Added a new stable dashboard artifact:
  - `akshare:table:watchlist_snapshot`
- Code changes in this pass:
  - `src/db/dashboard_artifact_keys.py`
  - `src/db/dashboard_sync.py`
  - `src/app/repositories/report_repository.py`
  - `src/app/services/dashboard_data_service.py`
  - `src/app/facades/dashboard_facade.py`
  - `tests/test_dashboard_data_service.py`
- What changed:
  - dashboard sync can now persist a dedicated watchlist snapshot artifact to PostgreSQL
  - the repository layer can now read that stored snapshot directly
  - `build_watchlist_base_frame()` now prefers the stored snapshot and only rebuilds the watchlist frame when the artifact is missing
  - when a rebuild does happen, the result is immediately written back to PostgreSQL so future cold starts can reuse it
  - realtime refresh context was made tolerant of snapshot payloads that do not expose `latest_bar_close`
- Measured cold-start improvement after syncing the snapshot artifact:
  - `build_watchlist_base_frame()` improved from about `85.622s` to about `0.175s`
  - `get_home_payload()` improved from about `18.419s` to about `7.238s`
  - `get_watchlist_payload()` improved from about `17.002s` to about `5.926s`
- Artifact verification:
  - `watchlist_snapshot` stored with `9` rows in PostgreSQL
- Verification in this pass:
  - `D:\openlianghua\.venv\Scripts\python.exe -m unittest tests.test_dashboard_data_service tests.test_dashboard_facade tests.test_web_api -q`
  - `D:\openlianghua\.venv\Scripts\python.exe -m py_compile D:\openlianghua\src\db\dashboard_sync.py D:\openlianghua\src\app\repositories\report_repository.py D:\openlianghua\src\app\services\dashboard_data_service.py D:\openlianghua\src\db\dashboard_artifact_keys.py D:\openlianghua\src\app\facades\dashboard_facade.py D:\openlianghua\tests\test_dashboard_data_service.py`

## 2026-04-04 Phase 2 Candidate Summary Split Pass

- Continued the Phase 2 cold-start work with the next heaviest first-screen path: candidates.
- Added new artifact-backed candidate summary contracts:
  - `src/db/dashboard_artifact_keys.py`
  - `src/db/dashboard_sync.py`
  - `src/app/repositories/report_repository.py`
  - `src/app/services/dashboard_data_service.py`
  - `src/app/facades/dashboard_facade.py`
  - `src/web_api/app.py`
- Added a new candidate snapshot artifact for every `model/split` pair:
  - `candidate_snapshot:{model}:{split}`
- Added write-through behavior for candidate snapshots:
  - the system now prefers PostgreSQL candidate snapshots for summary loads
  - if a snapshot is missing, it rebuilds once from predictions and immediately writes the result back to PostgreSQL
- API contracts added in this pass:
  - `GET /api/candidates/summary`
  - `GET /api/candidates/history`
  - existing `GET /api/candidates` kept as a compatibility endpoint
- React candidates page was rewritten to follow the same `总列表 -> 个体详情` rule:
  - top candidate list loads first from the summary endpoint
  - score-history chart loads separately from the history endpoint
  - page file updated:
    - `web/src/pages/CandidatesPage.tsx`
- Candidate snapshot sync was run for:
  - `ridge/lgbm/ensemble`
  - `valid/test/inference`
- Candidate snapshot sizes after sync:
  - `valid`: `353` rows
  - `test`: `496` rows
  - `inference`: `497` rows
- Measured runtime after this pass:
  - `/api/candidates/summary` about `15ms`
  - `/api/home` about `1.9s`
  - `/api/candidates/history` about `5.8s`
- Verification in this pass:
  - `D:\openlianghua\.venv\Scripts\python.exe -m unittest tests.test_dashboard_data_service tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
  - `D:\openlianghua\.venv\Scripts\python.exe -m py_compile D:\openlianghua\src\db\dashboard_artifact_keys.py D:\openlianghua\src\app\repositories\report_repository.py D:\openlianghua\src\db\dashboard_sync.py D:\openlianghua\src\app\services\dashboard_data_service.py D:\openlianghua\src\app\facades\dashboard_facade.py D:\openlianghua\src\web_api\app.py D:\openlianghua\tests\test_dashboard_data_service.py D:\openlianghua\tests\test_dashboard_facade.py D:\openlianghua\tests\test_web_api.py`
  - `cd D:\openlianghua\web && npm run build`
  - smoke checks passed:
    - `http://127.0.0.1:8001/api/home`
    - `http://127.0.0.1:8001/api/candidates/summary?model_name=ensemble&split_name=test&top_n=10`
    - `http://127.0.0.1:8001/api/candidates/history?model_name=ensemble&split_name=test&symbol=688297.SH`
    - `http://127.0.0.1:5174/candidates`

## 2026-04-04 Phase 2 Factor Summary Split Pass

- Continued the Phase 2 cold-start work with the next heaviest first-screen path after watchlist and candidates: factor explorer.
- Added a new factor explorer snapshot artifact:
  - `akshare:json:factor_explorer_snapshot`
- Code changes in this pass:
  - `src/db/dashboard_artifact_keys.py`
  - `src/app/repositories/report_repository.py`
  - `src/db/dashboard_sync.py`
  - `src/app/services/dashboard_data_service.py`
  - `src/app/facades/dashboard_facade.py`
  - `src/web_api/app.py`
  - `web/src/facades/dashboardPageClient.ts`
  - `web/src/types/api.ts`
  - `web/src/pages/FactorExplorerPage.tsx`
  - `tests/test_dashboard_data_service.py`
  - `tests/test_dashboard_facade.py`
  - `tests/test_web_api.py`
- What changed:
  - factor explorer now prefers a PostgreSQL-backed summary snapshot instead of rebuilding its first screen from the full feature panel
  - new contracts were added:
    - `GET /api/factors/summary`
    - `GET /api/factors/detail`
    - `GET /api/factors` remains as a compatibility endpoint
  - the React factor explorer page now follows the same `summary first / detail later` rule as watchlist and candidates
  - ranking and missing-rate tables now load first
  - single-symbol snapshot and history load as a second query
- Artifact verification:
  - factor explorer snapshot synced with `499` latest cross-section rows
- Measured runtime after this pass:
  - `/api/factors/summary` about `0.059s`
  - `/api/factors/detail` about `0.867s`
  - the detail path cost is now isolated from the first-screen summary path
- Verification in this pass:
  - `D:\openlianghua\.venv\Scripts\python.exe -m unittest tests.test_factor_explorer_page tests.test_dashboard_data_service tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
  - `D:\openlianghua\.venv\Scripts\python.exe -m py_compile D:\openlianghua\src\db\dashboard_artifact_keys.py D:\openlianghua\src\app\repositories\report_repository.py D:\openlianghua\src\db\dashboard_sync.py D:\openlianghua\src\app\services\dashboard_data_service.py D:\openlianghua\src\app\facades\dashboard_facade.py D:\openlianghua\src\web_api\app.py`
  - `cd D:\openlianghua\web && npm run build`
  - smoke checks passed:
    - `http://127.0.0.1:8001/api/factors/summary`
    - `http://127.0.0.1:8001/api/factors/detail`
    - `http://127.0.0.1:5174/factors`

## 2026-04-04 Phase 2 Service Summary Cache Pass

- Continued the same pass by reducing repeated service-status assembly on the operator shell.
- Code changes in this pass:
  - `src/app/facades/dashboard_facade.py`
  - `tests/test_dashboard_facade.py`
  - `tests/test_web_api.py`
- What changed:
  - `/api/service` now uses a short-lived cached summary bucket
  - `/api/shell` and the operator home reuse the same cached service summary instead of repeatedly rebuilding the same runtime status block
  - cache invalidation is wired into the existing dashboard cache-clear path
- Measured runtime after this pass:
  - `/api/service` first request about `0.028s`
  - `/api/service` repeated request about `0.018s`
  - `/api/shell` about `0.008s`

## 2026-04-04 Phase 2 AI Review / Home Noise Reduction Pass

- Continued Phase 2 by reducing duplicated detail layers on the AI review page and operator home.
- Code changes in this pass:
  - `src/app/facades/dashboard_facade.py`
  - `src/web_api/app.py`
  - `web/src/facades/dashboardPageClient.ts`
  - `web/src/types/api.ts`
  - `web/src/pages/AiReviewPage.tsx`
  - `web/src/pages/HomePage.tsx`
  - `tests/test_dashboard_facade.py`
  - `tests/test_web_api.py`
- What changed:
  - AI review now has dedicated summary/detail contracts:
    - `GET /api/ai-review/summary`
    - `GET /api/ai-review/detail`
    - combined `GET /api/ai-review` kept for compatibility
  - the operator home now uses AI review summaries only and no longer carries unused AI detail payloads on the first screen
  - the React AI review page was rewritten to enforce `总列表 -> 个体详情`
  - the React home page was rewritten so watchlist and candidate tables come first, while single-stock spotlight summaries, support information, and action logs are pushed to lower-priority sections
  - long markdown, external model responses, and full field tables on AI review are now behind collapsible sections
  - support blocks on the home page, including latest action output and secondary snapshot details, are also moved into collapsible sections
- Measured runtime after this pass:
  - `/api/ai-review/summary` about `0.009s`
  - `/api/ai-review/detail?scope=inference` about `0.027s`
  - `/api/ai-review/detail?scope=historical` about `0.128s`
  - `/api/home` warm path about `0.005s`
- Verification in this pass:
  - `D:\openlianghua\.venv\Scripts\python.exe -m py_compile D:\openlianghua\src\app\facades\dashboard_facade.py D:\openlianghua\src\web_api\app.py`
  - `D:\openlianghua\.venv\Scripts\python.exe -m unittest tests.test_dashboard_facade tests.test_web_api tests.test_realtime_quote_service -q`
  - `cd D:\openlianghua\web && npm run build`
  - smoke checks passed:
    - `http://127.0.0.1:8001/api/ai-review/summary`
    - `http://127.0.0.1:8001/api/ai-review/detail?scope=inference`
    - `http://127.0.0.1:8001/api/home`
    - `http://127.0.0.1:5174/`
    - `http://127.0.0.1:5174/ai-review`

## 2026-04-04 Phase 2 Overview / Service / Workspace Support-Layer Compression Pass

- Continued Phase 2 using the existing ECC workflow:
  - `frontend-patterns`
  - `tdd-workflow`
  - `verification-loop`
- Code changes in this pass:
  - `web/src/components/themeContext.ts`
  - `web/src/components/ThemeProvider.tsx`
  - `web/src/layout/AppLayout.tsx`
  - `web/src/lib/format.ts`
  - `web/src/pages/HomePage.tsx`
  - `web/src/pages/OverviewPage.tsx`
  - `web/src/pages/ModelBacktestPage.tsx`
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/pages/ServicePage.tsx`
  - `web/src/pages/WorkbenchPage.tsx`
  - `web/src/types/api.ts`
- What changed:
  - overview first screen now keeps only split summary, horizontal comparison, the main model table, and the equity chart on the main path
  - overview data-health notes and best-model support explanations were pushed into collapsible support sections
  - service page first screen now focuses on runtime health, key status fields, and only exposes snapshot/log support information behind secondary folds
  - workspace now keeps only `research parameters / research actions / UI settings` on the main path
  - workspace runtime explanation, snapshot metadata, and full action output were pushed into lower-priority support sections
  - React hook/lint hygiene was tightened while doing this pass:
    - theme hook moved out of `ThemeProvider.tsx`
    - manual memoization warnings removed on `home / overview / watchlist`
    - minor type and regex cleanup landed in shared files
- Verification in this pass:
  - `cd D:\openlianghua\web && npm run build`
  - `cd D:\openlianghua\web && npm run lint`
  - smoke checks passed:
    - `http://127.0.0.1:5174/overview`
    - `http://127.0.0.1:5174/service`
    - `http://127.0.0.1:5174/workspace`
    - `http://127.0.0.1:8001/api/service`
    - `http://127.0.0.1:8001/api/shell`

## 2026-04-04 Phase 2 Support Panel Unification Pass

- Continued the same Phase 2 cleanup immediately after the first support-layer compression pass.
- Code changes in this pass:
  - `web/src/components/SupportPanel.tsx`
  - `web/src/index.css`
  - `web/src/pages/OverviewPage.tsx`
  - `web/src/pages/ServicePage.tsx`
  - `web/src/pages/WorkbenchPage.tsx`
- What changed:
  - introduced a shared `SupportPanel` wrapper so lower-priority blocks no longer look identical to first-screen operator panels
  - overview support blocks (`数据健康 / 最佳模型补充`) now use the same lower-contrast support surface
  - service support blocks (`实时快照支持信息 / 运行支持信息 / 运行日志`) now use the same support surface
  - workspace runtime support metadata was moved out of the top summary panel into its own support panel at the bottom of the page
  - CSS now gives support panels a lighter visual weight using a dashed border and softer shadow
- Verification in this pass:
  - `cd D:\openlianghua\web && npm run build`
  - `cd D:\openlianghua\web && npm run lint`
  - smoke checks passed:
    - `http://127.0.0.1:5174/overview`
    - `http://127.0.0.1:5174/service`
    - `http://127.0.0.1:5174/workspace`

## 2026-04-04 Phase 2 Home / AI Review / Candidates Support-Layer Alignment Pass

- Continued Phase 2 by extending the same `SupportPanel` convention to the remaining noisy operator pages.
- Code changes in this pass:
  - `web/src/pages/HomePage.tsx`
  - `web/src/pages/AiReviewPage.tsx`
  - `web/src/pages/CandidatesPage.tsx`
  - `docs/MIGRATION_PLAN_2026-04-04.md`
- What changed:
  - home lower-priority blocks (`系统与行情摘要 / 最近一次动作`) now use the shared support-layer surface instead of competing visually with the primary operator panels
  - AI review detail panels (`推理详情 / 验证详情`) now use the same support-layer surface, keeping first-screen attention on candidate lists and selected-stock summaries
  - candidates lower-priority blocks (`当前候选标的 / 评分历史`) now use the same support-layer surface so the main candidate table stays visually dominant
  - this keeps the preferred reading flow stable across pages:
    - `总列表`
    - `当前聚焦对象`
    - `支持说明 / 历史 / 长文本`
- Verification in this pass:
  - `cd D:\openlianghua\web && npm run build`
  - `cd D:\openlianghua\web && npm run lint`
  - smoke checks passed:
    - `http://127.0.0.1:5174/`
    - `http://127.0.0.1:5174/ai-review`
    - `http://127.0.0.1:5174/candidates`

## 2026-04-04 Phase 2 Watchlist / Factor / Backtest Support-Layer Alignment Pass

- Continued Phase 2 with the same ECC workflow:
  - `frontend-patterns`
  - `tdd-workflow`
  - `verification-loop`
- Code changes in this pass:
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/pages/FactorExplorerPage.tsx`
  - `web/src/pages/ModelBacktestPage.tsx`
  - `docs/MIGRATION_PLAN_2026-04-04.md`
- What changed:
  - watchlist support-heavy blocks now use the support layer:
    - `盘中与研究`
    - `分批观察计划`
    - `评分历史`
  - factor explorer support-heavy blocks now use the support layer:
    - `因子字段快照`
    - `个股因子历史`
  - model backtest support-heavy blocks now use the support layer:
    - `策略概览`
    - `稳定性判断`
  - the primary reading flow remains stable on all three pages:
    - `总列表 / 主比较`
    - `当前聚焦对象 / 主图`
    - `支持说明 / 历史 / 完整字段`
- Verification in this pass:
  - `cd D:\openlianghua\web && npm run build`
  - `cd D:\openlianghua\web && npm run lint`
  - smoke checks passed:
    - `http://127.0.0.1:5174/watchlist`
    - `http://127.0.0.1:5174/factors`
    - `http://127.0.0.1:5174/backtests`

## 2026-04-04 Phase 2 Thin-Copy Compression Pass

- Continued Phase 2 by reducing scattered helper copy instead of adding new UI layers.
- ECC workflow used in this pass:
  - `frontend-patterns`
  - `tdd-workflow`
  - `verification-loop`
- Code changes in this pass:
  - `web/src/index.css`
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/pages/FactorExplorerPage.tsx`
  - `web/src/pages/ModelBacktestPage.tsx`
  - `docs/MIGRATION_PLAN_2026-04-04.md`
- What changed:
  - compressed repetitive panel subtitles into shorter task-oriented hints
  - compressed section descriptions so they no longer restate the panel title in full sentences
  - shortened filter-bar descriptions on high-density pages
  - reduced the visual weight of helper copy:
    - smaller subtitle font
    - smaller section-description font
    - smaller filter-bar description font
    - smaller context-strip helper text
  - kept the main reading structure unchanged:
    - `总列表 / 主比较`
    - `当前聚焦对象`
    - `支持说明 / 历史 / 完整字段`
- Verification in this pass:
  - `cd D:\openlianghua\web && npm run build`
  - `cd D:\openlianghua\web && npm run lint`
  - smoke checks passed:
    - `http://127.0.0.1:5174/watchlist`
    - `http://127.0.0.1:5174/factors`
    - `http://127.0.0.1:5174/backtests`

## 2026-04-04 Phase 2 Thin Support-Layer Pass

- Continued Phase 2 with the same ECC workflow:
  - `frontend-patterns`
  - `tdd-workflow`
  - `verification-loop`
- Code changes in this pass:
  - `web/src/index.css`
  - `web/src/pages/HomePage.tsx`
  - `web/src/pages/AiReviewPage.tsx`
  - `web/src/pages/CandidatesPage.tsx`
  - `web/src/pages/WorkbenchPage.tsx`
  - `web/src/pages/OverviewPage.tsx`
  - `web/src/pages/ServicePage.tsx`
  - `docs/MIGRATION_PLAN_2026-04-04.md`
- What changed:
  - reduced support-panel visual weight:
    - smaller titles
    - smaller subtitles
    - tighter panel spacing
    - tighter inner section spacing
  - further compressed helper copy on `home / ai-review / candidates / workspace / overview / service`
  - removed one repeated local CTA in `CandidatesPage` so the focused-record block no longer repeats the same drawer action twice
  - kept the same page contract:
    - `总列表 / 主比较`
    - `当前聚焦对象`
    - `支持说明 / 历史 / 完整字段`
- Verification in this pass:
  - `cd D:\openlianghua\web && npm run build`
  - `cd D:\openlianghua\web && npm run lint`
  - smoke checks passed:
    - `http://127.0.0.1:5174/`
    - `http://127.0.0.1:5174/overview`
    - `http://127.0.0.1:5174/candidates`
    - `http://127.0.0.1:5174/ai-review`
    - `http://127.0.0.1:5174/workspace`
    - `http://127.0.0.1:5174/service`
    - `http://127.0.0.1:8001/api/home`
    - `http://127.0.0.1:8001/api/service`
    - `http://127.0.0.1:8001/api/ai-review/summary`
    - `http://127.0.0.1:8001/api/candidates/summary`

## 2026-04-04 Phase 2 Shared Rhythm Pass

- Continued Phase 2 with the same ECC workflow:
  - `frontend-patterns`
  - `tdd-workflow`
  - `verification-loop`
- Code changes in this pass:
  - `web/src/components/ContextStrip.tsx`
  - `web/src/components/PageFilterBar.tsx`
  - `web/src/components/SupportPanel.tsx`
  - `web/src/index.css`
  - `docs/MIGRATION_PLAN_2026-04-04.md`
- What changed:
  - `ContextStrip` now distinguishes lighter secondary items from highlighted primary items without changing page contracts
  - `PageFilterBar` now renders as a weaker utility layer:
    - tighter spacing
    - smaller copy
    - clearer separation between intro and controls
  - `SupportPanel` now uses a consistent compact support-surface variant
  - shared hierarchy is now more explicit:
    - summary panels stay primary
    - context strips stay factual
    - filter bars stay operational
    - support panels stay delayed
- Verification in this pass:
  - `cd D:\openlianghua\web && npm run build`
  - `cd D:\openlianghua\web && npm run lint`
  - smoke checks passed:
    - `http://127.0.0.1:5174/`
    - `http://127.0.0.1:5174/watchlist`
    - `http://127.0.0.1:5174/candidates`
    - `http://127.0.0.1:5174/ai-review`
    - `http://127.0.0.1:5174/overview`
    - `http://127.0.0.1:5174/service`
    - `http://127.0.0.1:5174/workspace`

## 2026-04-04 Phase 2 Table / Drawer Hierarchy Pass

- Continued Phase 2 with the same ECC workflow:
  - `frontend-patterns`
  - `tdd-workflow`
  - `verification-loop`
- Code changes in this pass:
  - `web/src/components/DataTable.tsx`
  - `web/src/components/DetailSummarySection.tsx`
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/pages/CandidatesPage.tsx`
  - `web/src/index.css`
  - `docs/MIGRATION_PLAN_2026-04-04.md`
- What changed:
  - table toolbars now read as:
    - list summary on the left
    - presets and table settings on the right
  - removed nonessential density copy from the default table toolbar so list scanning stays primary
  - standardized drawer reading order on high-frequency detail paths:
    - `当前摘要`
    - `快捷操作`
    - `完整字段`
  - full field tables in `watchlist / candidates` drawers are now folded behind `查看完整字段`
- Verification in this pass:
  - `cd D:\openlianghua\web && npm run build`
  - `cd D:\openlianghua\web && npm run lint`
  - smoke checks passed:
    - `http://127.0.0.1:5174/watchlist`
    - `http://127.0.0.1:5174/candidates`
    - `http://127.0.0.1:5174/ai-review`
    - `http://127.0.0.1:5174/overview`
