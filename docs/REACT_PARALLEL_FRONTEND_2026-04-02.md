# React Parallel Frontend On 2026-04-02

## Goal

- Keep the existing Streamlit research workstation online.
- Build a parallel React frontend instead of rewriting in place.
- Move the current features to `React + API` page by page, while keeping the Python research pipeline intact.

## Current Stack

### Existing

- `streamlit_app.py`
- `src/app/pages/*`
- `src/app/repositories/*`
- `src/app/services/*`

### New Parallel Stack

- React frontend:
  - `web/`
  - Vite + React + TypeScript
  - React Router
  - TanStack Query
  - Recharts
  - react-markdown
- Python API:
  - `src/web_api/app.py`
  - FastAPI
  - reuses current repositories/services through `src/app/facades/dashboard_facade.py`

## Design Principle

- Do not delete Streamlit.
- Do not fork the research logic into two incompatible implementations.
- Put new data assembly behind a facade that the React frontend can call.
- Keep expensive operations explicit:
  - realtime refresh
  - report generation
  - model actions

## Migrated Surface

- Overview
- Factor explorer
- Model backtest
- Candidates
- Watchlist
- AI review
- Service page
- Sidebar actions and experiment config form

## Startup

Open two terminals:

### Terminal 1

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_research_api.ps1
```

API default:

- `http://localhost:8000`

### Terminal 2

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_react_web.ps1
```

React frontend default:

- `http://localhost:5173`

## Verification

- Python API imports and returns valid responses for:
  - `/api/meta`
  - `/api/overview`
  - `/api/service`
- React production build passes with `npm run build`
- Existing Streamlit app is kept intact as fallback

## Current Follow-Up Status

- Shared dashboard loading/caching has now been centralized in:
  - `src/app/services/dashboard_data_service.py`
- Both of these layers now reuse that shared data service:
  - `src/app/facades/dashboard_facade.py`
  - `streamlit_app.py`
- React usability improvements already added:
  - query loading and error notices
  - pending state on actions and config save
  - URL-persisted filters for major pages
- Live dev endpoints verified on `2026-04-02`:
  - API: `http://127.0.0.1:8000`
  - React: `http://127.0.0.1:5173`

## Latest Convergence Status

- Streamlit workspace rendering now pulls facade payloads for the same page surfaces exposed by the API.
- Realtime watchlist refresh remains explicit in Streamlit, but the refreshed quotes are merged back into the shared watchlist payload shape before rendering.
- Service status is now also read through the facade path, so both frontends use the same status payload contract.
- The shell/sidebar layer now also has a shared contract:
  - new API endpoint: `/api/shell`
  - React shell now hydrates from that single payload
  - Streamlit sidebar also reads the same shell payload shape through the facade
- Shared shell contract currently includes:
  - bootstrap metadata
  - experiment config
  - service status
  - watchlist entry count
  - config summary text
  - centralized action catalog metadata

## Page Client Layer

- React page-level data fetching and URL state are now normalized through:
  - `web/src/facades/dashboardPageClient.ts`
  - `web/src/facades/pageUrlState.ts`
  - `web/src/facades/usePageSearchState.ts`
- This means page modules now reuse a shared client contract for:
  - search param defaults
  - search param updates
  - query keys
  - page payload endpoints
  - watchlist-specific realtime/action endpoints
- The result is that page modules are now thinner and closer to pure view components, while fetch and URL concerns are concentrated in one layer.

## Shared Display Layer

- React pages also now share a small display-layer component set for repeated layout patterns:
  - `ControlField`
  - `ControlGrid`
  - upgraded `DataTable`
  - upgraded `LineChartCard`
- This removes repeated page-level JSX for:
  - filter rows
  - label wrappers
  - loading-vs-empty table text
  - chart line-key inference
- Current split of concerns:
  - facade client layer owns page state/query concerns
  - page files own page composition
  - shared components own repeated display patterns

## Latest UX Polish

- The React shell now has a consistent top workspace toolbar with current-page context, quick status pills, and cleaner Chinese copy.
- Shared display primitives now also include:
  - `Badge`
  - `PropertyGrid`
  - `InsightList`
- `DataTable` is now more product-like:
  - friendlier field labels
  - field-aware value formatting
  - sticky first-column support for wide operational tables
- `WatchlistPage` no longer leads with a one-row raw record dump:
  - it now shows holding summary, key prices, ranking, premarket plan, realtime status, and discussion state first
  - the full payload is still available through a collapsible field sheet
- `AiReviewPage` now surfaces:
  - score strip
  - action/confidence badges
  - theme tags
  - bull/risk lists
  - curated digests
  - candidate table
  - collapsible full-field sheet
- This means the React frontend has moved from “payload viewer” closer to a real operator workstation, while still preserving the existing Streamlit version as the safe fallback.

## Candidate And Backtest Pages

- `CandidatesPage` is now organized as:
  - filter bar
  - selected-candidate summary
  - score history chart
  - curated candidate comparison table
- `ModelBacktestPage` is now organized as:
  - strategy headline and top metrics
  - strategy snapshot + stability review
  - equity curve + period return chart
  - monthly / importance / yearly / regime diagnostics
- This closes the same UX gap that previously existed on the watchlist and AI review pages: the operator now sees the decision-oriented summary first, while the denser tables stay available lower in the page rather than dominating the first screen.

## Overview Factor Service Pages

- `OverviewPage` now opens with:
  - dataset coverage and file health
  - best-model snapshots
  - curated comparison table
  - equity curve chart
- `FactorExplorerPage` now opens with:
  - selected factor context
  - top-ranked stock summary
  - worst missing-rate reminder
  - selected symbol snapshot
  - then ranking, missing-rate, and history views
- `ServicePage` now opens with:
  - status headline
  - listener and PID health
  - restart summary
  - freshness/staleness flags
  - logs kept below the operational summary
- At this point all major React pages now follow the same interaction pattern:
  - control row
  - summary-first headline
  - curated metrics/properties
  - denser tables and charts after the first-screen decision context

## Shared Visual System

- The React frontend now also shares a more explicit visual system rather than only a shared data/facade system.
- Common UI primitives now define the house style for:
  - panel headers
  - metric cards
  - notices
  - control fields
  - data tables
- The main visual changes are:
  - clearer button hierarchy and motion
  - tighter table density
  - stronger state-color feedback
  - more consistent spacing between sections and cards
  - better first-screen cadence across all pages
- This means the React app is no longer only structurally parallel to Streamlit; it now also has a coherent product-layer visual language across the whole workspace.

## Interaction Layer

- The React frontend now also has a shared interaction layer rather than only shared layout and shared page facades.
- New shared primitives:
  - `web/src/components/ToastProvider.tsx`
    - provides unified success / error / info toast feedback
  - `web/src/components/DetailDrawer.tsx`
    - provides a common operator detail surface for holdings and candidates
  - `web/src/components/DataTable.tsx`
    - now supports sorting
    - column visibility toggles
    - clickable rows
    - selected-row highlighting
- `WatchlistPage` now supports:
  - sortable / hideable holding table columns
  - inline partial-refresh feedback for realtime quote refresh
  - unified toasts for refresh / watch-plan / action-memo actions
  - holding detail drawer opened from the table or the selected summary card
- `CandidatesPage` now supports:
  - sortable / hideable candidate table columns
  - candidate detail drawer opened from the table or the selected summary card
- Styling groundwork now also prepares for future theme support:
  - tokens are split into `:root[data-theme='light']` and `:root[data-theme='dark']`
  - no theme switch is live yet, but future theme work can extend tokens without reworking component structure
- Verification for this pass:
  - React `npm run build`
  - live checks:
    - `http://127.0.0.1:5173`
    - `http://127.0.0.1:5173/api/shell`
    - `http://127.0.0.1:5173/api/watchlist`
    - `http://127.0.0.1:5173/api/candidates`

## Theme, Persistence, And Drawer Actions

- The React frontend now also has a shared preference layer instead of only shared layout and shared interaction components.
- New shared preference primitives:
  - `web/src/components/ThemeProvider.tsx`
    - manages light / dark theme state
    - persists the selected theme locally
    - drives CSS tokens through `data-theme`
  - `web/src/lib/uiPreferences.ts`
    - stores theme preference
    - stores per-table visible-column preference by `storageKey`
- `web/src/layout/AppLayout.tsx` now surfaces a visible theme switch in the main workspace toolbar.
- `web/src/components/DataTable.tsx` now supports:
  - persisted visible columns
  - restore-default action for persistent tables
- `web/src/pages/WatchlistPage.tsx` now includes drawer quick actions for:
  - copy symbol
  - open AI review
  - refresh realtime quotes
  - generate watch plan
  - generate action memo
- `web/src/pages/CandidatesPage.tsx` now includes drawer quick actions for:
  - copy symbol
  - open AI review
- `web/src/index.css` is now more theme-ready in practice, not just in structure:
  - several previously hard-coded light surfaces now use theme tokens
  - the current dark theme is usable enough for daily operation instead of only serving as scaffolding
- Verification for this pass:
  - React `npm run build`
  - live checks:
    - `http://127.0.0.1:5173`
    - `http://127.0.0.1:5173/api/shell`
    - `http://127.0.0.1:5173/api/watchlist`
    - `http://127.0.0.1:5173/api/candidates`

## Global Preference Reset And Wider Persistence

- The React frontend now also supports a global `重置界面偏好` action from the workspace toolbar.
- The reset flow now clears:
  - stored theme preference
  - stored visible-column preferences for all `DataTable` instances that use a `storageKey`
- This is implemented through:
  - `web/src/lib/uiPreferences.ts`
    - shared preference clear helper
    - shared browser reset event
  - `web/src/App.tsx`
    - top-level reset mutation and reset toast flow
  - `web/src/layout/AppLayout.tsx`
    - toolbar reset entry point
  - `web/src/components/DataTable.tsx`
    - immediate in-page reset handling for already-mounted tables
- Table preference persistence is now also active across more research pages:
  - `AI 研判`
  - `因子探索`
  - `模型回测`
  - `页面服务`
- This means the visible-column persistence model is no longer limited to operator pages like watchlist/candidates; it now covers the core research pages as well.
- Verification for this pass:
  - React `npm run build`
  - live checks:
    - `http://127.0.0.1:5173`
    - `http://127.0.0.1:5173/ai-review`
    - `http://127.0.0.1:5173/factors`
    - `http://127.0.0.1:5173/backtests`
    - `http://127.0.0.1:5173/service`
    - `http://127.0.0.1:5173/api/shell`
    - `http://127.0.0.1:5173/api/ai-review`
    - `http://127.0.0.1:5173/api/factors`
    - `http://127.0.0.1:5173/api/backtests`
    - `http://127.0.0.1:5173/api/service`

## Drag Reorder And View Presets

- The React table system now supports not only field show/hide persistence but also:
  - drag-based column reordering
  - page-specific view presets
  - persistence of both visible columns and preset selection
- `web/src/components/DataTable.tsx` is now the shared contract for:
  - sorting
  - visible column toggles
  - column drag reorder
  - row selection
  - per-table view presets
- View presets are now exposed on the key operator/research tables:
  - watchlist matrix
  - candidate matrix
  - AI candidate tables
  - factor ranking / missing-rate tables
  - backtest importance / diagnostics tables
- This means the frontend now has a more complete preference model:
  - choose a default page/table view
  - fine-tune visible fields
  - drag columns into a preferred order
  - persist the result
  - reset all of it from one global toolbar action
- Verification for this pass:
  - React `npm run build`
  - live checks:
    - `http://127.0.0.1:5173`
    - `http://127.0.0.1:5173/watchlist`
    - `http://127.0.0.1:5173/candidates`
    - `http://127.0.0.1:5173/ai-review`
    - `http://127.0.0.1:5173/factors`
    - `http://127.0.0.1:5173/backtests`
    - `http://127.0.0.1:5173/service`

## Column Width Memory And Shareable View Links

- The React workbench now persists one more layer of operator preference:
  - visible columns
  - column order
  - selected table preset
  - column widths
- `web/src/components/DataTable.tsx` is now the shared interaction contract for:
  - column drag reorder
  - field show/hide
  - preset switching
  - column width resizing
  - single-column width reset with double click
  - restoring layout state from a shared URL
- `web/src/lib/uiPreferences.ts` now stores richer table layout state, and
  `web/src/facades/pageUrlState.ts` now knows how to:
  - collect route-specific table layouts
  - encode them into a URL-safe `view` payload
  - restore them when someone opens the link
- `web/src/App.tsx` and `web/src/layout/AppLayout.tsx` now expose a global `复制分享链接` action in the top toolbar, so the copied URL carries:
  - current route
  - current page filters / selected symbols already present in query params
  - current per-page table layout state
- The overview comparison matrix now also participates in shared/persistent table layout through `storageKey="overview-comparison"`.
- Verification for this pass:
  - targeted eslint on the newly touched shared frontend files
  - React `npm run build`
  - live checks:
    - `http://127.0.0.1:5173`
    - `http://127.0.0.1:5173/watchlist`
    - `http://127.0.0.1:5173/ai-review`
    - `http://127.0.0.1:5173/api/shell`

## Shared Drawer Actions And Broader Shareable View Coverage

- Added `web/src/components/DrawerEntityActions.tsx` as the shared action footer for entity drawers.
- Added `web/src/lib/shareLinks.ts` as the shared route/link utility for:
  - copying the current page view
  - building AI review links
  - building watchlist links
  - building candidate links
- Reworked:
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/pages/CandidatesPage.tsx`
  - `web/src/pages/AiReviewPage.tsx`
- Result:
  - drawer actions now feel consistent across watchlist and candidates
  - more tables now participate in the same “shareable main view” contract
  - page-level share links now preserve more of the operator’s actual table state

## Data / Research Nightly Refresh Note

- On `2026-04-03`, the local machine could not use MyQuant terminal refresh, so the workbench proceeded with the AKShare fallback path.
- The active market panel was refreshed to `2026-04-02`.
- `watch_symbols` are now guaranteed to be included in the AKShare downloader output, so holdings outside the active index universe still appear in:
  - watchlist view
  - action memo generation
  - watch plan generation
- Whitebox outputs prepared for the next session:
  - `reports/weekly/akshare_overlay_latest_candidates.csv`
  - `reports/weekly/akshare_overlay_inference_candidates.csv`
  - `reports/weekly/000078_watch_plan_2026-04-03.md`
  - `reports/weekly/000078_action_memo_2026-04-03.md`

## Shell Layout Refresh

- The React shell now follows a clearer workbench model:
  - left sidebar for navigation / config / actions
  - right main content region for page hero + workspace sections
- New shared shell components:
  - `web/src/components/SidebarSection.tsx`
  - `web/src/components/WorkspaceHero.tsx`
- `web/src/layout/AppLayout.tsx` now supports:
  - sticky desktop sidebar
  - mobile sidebar toggle
  - semantic navigation and page structure
  - a more product-like top hero instead of a loose toolbar block
- Interaction component integration was tightened:
  - tables expose clearer region semantics
  - drawers use explicit dialog labeling
  - charts render inside figure / figcaption structure
- This is the current frontend direction to preserve going forward:
  - responsive-first
  - fluid spacing and auto-fit content grids
  - componentized shell primitives
  - semantic layout before page-specific polish

## Page Density Refresh

- Shared page-density primitives were added:
  - `web/src/components/PageFilterBar.tsx`
  - `web/src/components/SegmentedControl.tsx`
  - `web/src/components/DrawerQuickActions.tsx`
- Shared shell/UI components were extended:
  - `web/src/components/Panel.tsx`
  - `web/src/components/LineChartCard.tsx`
  - `web/src/components/QueryNotice.tsx`
  - `web/src/components/MarkdownCard.tsx`
  - `web/src/components/DetailDrawer.tsx`
- Key React pages were reworked to use the same page-density contract:
  - `OverviewPage`
  - `FactorExplorerPage`
  - `ModelBacktestPage`
  - `CandidatesPage`
  - `WatchlistPage`
  - `AiReviewPage`
- Intended interaction model after this pass:
  - top panel = summary + unified filter bar
  - middle section = decision-oriented summary cards
  - lower section = sortable/shareable tables
  - drawer top = quick actions first, detailed fields second
  - chart areas = segmented view switching where the payload provides multiple useful line groups
- Styling foundation extended in `web/src/index.css` for:
  - summary-vs-table hierarchy
  - reusable in-page filter bar
  - segmented chart toggles
  - drawer quick-action shelf
  - tighter section-heading rhythm
- Validation:
  - React `npm run build`
  - targeted eslint for touched shared components/pages
  - live checks:
    - `http://127.0.0.1:5173`
    - `http://127.0.0.1:5173/watchlist`
    - `http://127.0.0.1:5173/api/shell`

## Shell Simplification

- The shell now follows a stricter operator layout:
  - full-width fixed top bar
  - reduced header height and copy
  - top bar only shows system name + current page name
  - sidebar reduced to navigation only
- Reworked shell entry:
  - `web/src/layout/AppLayout.tsx`
- New functional page for the controls that previously lived in the sidebar:
  - `web/src/pages/WorkbenchPage.tsx`
  - includes:
    - research parameter editing
    - research actions
    - latest action output
    - interface tools such as theme/share/reset
- Routing updated in:
  - `web/src/App.tsx`
  - added `/workspace`
- Service visibility remains on-page through the refreshed:
  - `web/src/pages/ServicePage.tsx`
- CSS shell overrides in `web/src/index.css` now cover:
  - fixed top bar
  - nav-only sidebar
  - top-offset layout spacing
  - mobile nav reveal under the top bar
- Validation:
  - targeted eslint on shell/workbench/service files
  - React `npm run build`
  - live checks:
    - `http://127.0.0.1:5173`
    - `http://127.0.0.1:5173/workspace`
    - `http://127.0.0.1:5173/service`
    - `http://127.0.0.1:5173/api/shell`

## Workbench And Account Refinement

- The shell has now moved one step closer to a formal operator product layout:
  - top-right header area contains the global theme switch
  - top-right header area also contains account status:
    - `login` if no local user exists
    - local avatar/name/title + `logout` if a local user exists
  - left sidebar is grouped into menu sections instead of a flat list
  - current page highlight is stronger and more compact
- The workbench page is no longer a generic catch-all panel. It is explicitly divided into:
  - `research parameters`
  - `research actions`
  - `interface settings`
- This pass introduced:
  - `web/src/components/LoginDialog.tsx`
  - `web/src/lib/userSession.ts`
- This pass updated:
  - `web/src/layout/AppLayout.tsx`
  - `web/src/pages/WorkbenchPage.tsx`
  - `web/src/index.css`
- Current account behavior is intentionally frontend-local:
  - there is still no backend auth/session API
  - user presence is stored in browser local storage only
  - this keeps the UX complete enough for now without forcing a backend auth design prematurely
- Stability note:
  - the login dialog now remounts on each open so old draft values do not leak across sessions
- Validation:
  - targeted eslint on touched shell/account/workbench files
  - React `npm run build`
  - live checks:
    - `http://127.0.0.1:5173`
    - `http://127.0.0.1:5173/workspace`
    - `http://127.0.0.1:5173/service`
    - `http://127.0.0.1:5173/api/shell`

## Real Auth Integration

- The previous frontend-local account mock is now replaced by a real backend session flow.
- React now calls:
  - `GET /api/auth/session`
  - `POST /api/auth/login`
  - `POST /api/auth/logout`
- Frontend files updated for the real auth contract:
  - `web/src/App.tsx`
  - `web/src/api/client.ts`
  - `web/src/layout/AppLayout.tsx`
  - `web/src/components/LoginDialog.tsx`
  - `web/src/pages/WorkbenchPage.tsx`
  - `web/src/pages/WatchlistPage.tsx`
  - `web/src/types/api.ts`
- Interaction model after this pass:
  - top-right account area reads server session state instead of local storage
  - login dialog now uses `username/password`
  - workbench mutating actions are disabled when the user is logged out
  - watchlist markdown-generation actions are disabled when the user is logged out
  - all frontend fetch helpers now send cookies with `credentials: include`
- Important runtime note:
  - the code is ready and build/lint/test validation passed
  - however, the currently running API process on `localhost:8000` was an older worker that could not be stopped in this session due local process permissions
  - the running dev frontend therefore still needs the API process restarted before `/api/auth/session` becomes available in the live browser session

## 2026-04-03 Live Runtime Update

- The runtime issue on `8000` was worked around by moving the active FastAPI dev server to `http://127.0.0.1:8001`.
- The React dev workspace proxy was moved to the same new target.
- The live browser entry is now `http://127.0.0.1:5174`.
- Verified working in the browser-facing path:
  - `POST /api/auth/login`
  - `GET /api/auth/session`
  - `POST /api/auth/logout`
- The top-right account area is now backed by the real Postgres session flow in live runtime, not only in local tests.
