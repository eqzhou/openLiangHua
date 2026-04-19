# A-Share AI Research Starter

This repository is a minimal research-first starter for A-share stock selection.
It is designed for the workflow:

1. Download daily market data from AKShare.
2. Build a point-in-time feature panel and forward-return labels.
3. Train baseline models such as Ridge and LightGBM.
4. Review rank IC, top-N forward returns, and a simple daily portfolio curve.

For the current release-facing operator workflow, start with `docs/USER_MANUAL.md`.

## Durable Docs

If a chat or terminal session gets interrupted, start with these files instead of reconstructing context from scratch:

- `docs/README.md`
- `docs/USER_MANUAL.md`
- `docs/PROJECT_ARCHITECTURE.md`
- `docs/SYSTEM_ARCHITECTURE_BLUEPRINT_2026-04-04.md`
- `docs/MIGRATION_PLAN_2026-04-04.md`
- `docs/PROJECT_MEMORY_2026-04-04.md`
- `reports/weekly/000078_action_memo_2026-04-02.md`

The current scaffold is intentionally conservative:

- It focuses on daily data, not intraday execution.
- It uses next-day close as an entry proxy for labels.
- It uses the current index constituents for historical backtests in free mode, so there is survivorship bias.
- It now supports rolling retraining, industry-neutral scoring, and per-industry candidate caps.
- It still relies on current-industry snapshots and current index membership in free mode, so point-in-time realism is limited.
- It still does not implement intraday execution realism.
- It now has a parallel MyQuant data layer, but historical industry mapping is still using the current snapshot fallback when available.

## Quick Start

Create the environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Review `config/universe.yaml` before your first run. By default, it uses `mode: current_index` with `000905.SH`. If you want a smaller smoke test, switch back to `mode: explicit`.

## Run the Pipeline

Download data:

```powershell
python -m src.data.downloader --start-date 20180101 --end-date 20251231
```

If the long download is still running in the background, you can build a usable partial panel from cached symbol files:

```powershell
python -m src.data.materialize_cache
```

Build features and labels:

```powershell
python -m src.features.build_feature_panel
```

Train the linear baseline:

```powershell
python -m src.models.train_linear
```

Train the LightGBM baseline:

```powershell
python -m src.models.train_lgbm
```

Train the adaptive ensemble strategy:

```powershell
python -m src.models.train_ensemble
```

Generate the AI overlay research summary:

```powershell
python -m src.agents.overlay_report
```

Generate the latest unlabeled cross-section inference and AI overlay:

```powershell
python -m src.agents.overlay_inference_report
```

Generate holding watch plans:

```powershell
python -m src.agents.watch_plan
```

Generate holding action memos:

```powershell
python -m src.agents.action_memo
```

The training scripts now also write:

- yearly diagnostics: `reports/weekly/<source>_<model>_<split>_yearly.csv`
- regime diagnostics: `reports/weekly/<source>_<model>_<split>_regime.csv`
- trend-filter-aware portfolios: `reports/weekly/<source>_<model>_<split>_portfolio.csv`
- stability summary: `reports/weekly/<source>_<model>_stability.json`
- adaptive ensemble weights: `reports/weekly/<source>_ensemble_weights.json`
- AI overlay candidate pool: `reports/weekly/<source>_overlay_latest_candidates.csv`
- AI overlay packet: `reports/weekly/<source>_overlay_latest_packet.json`
- AI overlay markdown brief: `reports/weekly/<source>_overlay_latest_brief.md`
- AI overlay LLM requests: `reports/weekly/<source>_overlay_llm_requests.jsonl`
- AI overlay LLM responses: `reports/weekly/<source>_overlay_llm_responses.jsonl`
- AI overlay LLM response summary: `reports/weekly/<source>_overlay_llm_response_summary.md`
- latest unlabeled inference packet: `reports/weekly/<source>_inference_packet.json`
- latest unlabeled inference predictions: `reports/weekly/<source>_{ridge,lgbm,ensemble}_inference_predictions.csv`
- latest unlabeled overlay candidate pool: `reports/weekly/<source>_overlay_inference_candidates.csv`
- latest unlabeled overlay packet: `reports/weekly/<source>_overlay_inference_packet.json`
- latest unlabeled overlay markdown brief: `reports/weekly/<source>_overlay_inference_brief.md`
- latest unlabeled overlay LLM requests: `reports/weekly/<source>_overlay_inference_llm_requests.jsonl`
- latest unlabeled overlay LLM responses: `reports/weekly/<source>_overlay_inference_llm_responses.jsonl`
- latest unlabeled overlay LLM response summary: `reports/weekly/<source>_overlay_inference_llm_response_summary.md`
- holding watch plans: `reports/weekly/*_watch_plan_*.md`
- holding action memos: `reports/weekly/*_action_memo_*.md`

The default research config includes a simple benchmark-proxy trend filter based on the active index universe. You can adjust it in `config/experiment.yaml` under `risk_filter`.
The default AI overlay config lives in `config/experiment.yaml` under `overlay`; it combines Ridge, LightGBM, factor context, and model-consensus into a Chinese research brief. It now also supports configurable announcement/news lookback windows and item caps for the point-in-time event digest.
The system now also supports a latest unlabeled inference path that trains on all currently label-valid history, scores the newest feature date, skips future-dependent filters such as `can_enter_next_day`, and writes a separate inference artifact set instead of overwriting historical `test` backtest files.
The overlay layer now supports `weight_mode: validation_adaptive`, which reweights Ridge and LightGBM using validation-set return, Sharpe, rank IC, drawdown, turnover, and the latest stability summary. It also uses analyst research reports as a fallback when historical stock-news endpoints do not return a point-in-time result.
The optional LLM bridge is configured in `.env` via `OVERLAY_LLM_*`. When `OVERLAY_LLM_PROVIDER=openai`, `OVERLAY_LLM_MODEL=gpt-5.4`, and `OPENAI_API_KEY` is filled, generating the overlay report now auto-calls the OpenAI Responses API and saves both request and response artifacts.
The holding page and generated action memos now also backwrite the latest available multi-round LLM discussion state, including historical verified overlay and latest unlabeled inference overlay rounds. The watchlist summary table now compresses that into a one-line `盘前执行建议` column on the first screen.

Recommended `.env` fields for GPT-5.4:

```powershell
OVERLAY_LLM_ENABLED=true
OVERLAY_LLM_PROVIDER=openai
OVERLAY_LLM_MODEL=gpt-5.4
OVERLAY_LLM_REASONING_EFFORT=medium
OVERLAY_LLM_REASONING_SUMMARY=auto
OVERLAY_LLM_MAX_OUTPUT_TOKENS=1500
OPENAI_API_KEY=your_api_key_here
OPENAI_BASE_URL=
```

Launch the local dashboard:

```powershell
streamlit run streamlit_app.py
```

Keep the dashboard running in the background:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_streamlit.ps1
```

Check dashboard status:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\streamlit_status.ps1
```

Stop the dashboard supervisor:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stop_streamlit.ps1
```

## Local PM2 Startup

For local React + FastAPI usage, you can run both services under PM2.

Install PM2 once:

```bash
npm install -g pm2
```

Start both services from the repo root:

```bash
pm2 start ecosystem.config.cjs
```

Default local endpoints:

- FastAPI: `http://127.0.0.1:8001`
- React dev server: `http://127.0.0.1:5174`

Common PM2 commands:

```bash
pm2 status
pm2 logs openlianghua-api
pm2 logs openlianghua-web
pm2 restart openlianghua-api
pm2 restart openlianghua-web
pm2 stop ecosystem.config.cjs
pm2 delete ecosystem.config.cjs
```

Notes:

- `ecosystem.config.cjs` uses `scripts/start_research_api.sh` and `scripts/start_react_web.sh`.
- The API startup script will try `.venv`, then `.venv-codex`, then system `python3`.
- The React service runs the Vite dev server, which is suitable for local workstation use.

## Optional MyQuant Data Layer

If you have a MyQuant token and the official `gm` SDK installed in a supported Python 3.11/3.12 environment, you can download a parallel dataset without overwriting the free AKShare outputs by default.

1. Copy `.env.example` to `.env`.
2. Fill `MYQUANT_TOKEN`.
3. Install the optional SDK manually:

```powershell
pip install gm
```

4. Download the MyQuant dataset:

```powershell
python -m src.data.myquant_downloader --start-date 2018-01-01 --end-date 2025-12-31
```

5. Clean the MyQuant panel and enrich it with historical SW industry mapping:

```powershell
python -m src.data.myquant_enrich
```

This writes files such as:

- `data/staging/myquant_trade_calendar.parquet`
- `data/staging/myquant_stock_basic.parquet`
- `data/staging/myquant_index_membership_raw.parquet`
- `data/staging/myquant_index_membership_daily.parquet`
- `data/staging/myquant_instrument_infos.parquet`
- `data/staging/myquant_instrument_history.parquet`
- `data/staging/myquant_bars_raw.parquet`
- `data/staging/myquant_daily_bar.parquet`
- `data/staging/myquant_sw_industry_history.parquet`
- `data/staging/myquant_data_quality.json`

If you want to replace the canonical research inputs in `data/staging/*.parquet`, add:

```powershell
python -m src.data.myquant_downloader --start-date 2018-01-01 --end-date 2025-12-31 --write-canonical
```

Notes:

- The MyQuant downloader uses true historical index constituents when `config/universe.yaml` is set to `mode: current_index`.
- The MyQuant token used in this project does not have the dedicated industry API permission, so point-in-time industry is currently enriched from AkShare's SW history plus local fallback.
- This keeps the research schema aligned with `daily_bar.parquet`, which makes later model switching easier.

## Optional Tushare Incremental Refresh

If your local `daily_bar.parquet` (or source-prefixed variant such as `akshare_daily_bar.parquet`) already exists and you only want to append missing trading days without rewriting old rows, you can use the Tushare incremental helper.

1. Fill `TUSHARE_TOKEN` in `.env`.
2. Copy `.env.example` to `.env` if you have not created it yet.
3. Run the daily refresh script:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\refresh_daily_bar_tushare.ps1 -TargetSource akshare -EndDate 2026-04-16
```

On macOS / Linux, you can run:

```bash
bash ./scripts/refresh_daily_bar_tushare.sh akshare 2026-04-16
```

Or call the module directly:

```powershell
python -m src.data.tushare_incremental_sync --target-source akshare --end-date 2026-04-16 --write-canonical
```

Notes:

- `.env` is gitignored, so this is the right place to keep a frequently rotated `TUSHARE_TOKEN`.
- This helper only appends missing trade dates for the symbols already tracked in the local panel plus current `watch_symbols`.
- Existing historical rows are kept as-is; the command does not backfill or rewrite old dates.
- It updates the matching `trade_calendar` and `stock_basic` staging files alongside `daily_bar`.

If you want to refresh daily bars and then continue through `特征 / 标签 / Dashboard 快照` in one shot, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\refresh_full_pipeline_tushare.ps1 -TargetSource akshare -EndDate 2026-04-16
```

macOS / Linux:

```bash
bash ./scripts/refresh_full_pipeline_tushare.sh akshare 2026-04-16
```

The React workbench now also includes a dedicated `数据管理` page for the same two actions:

- `Tushare 增量刷新日线`
- `Tushare 全流程刷新`

## Outputs

- `data/staging/daily_bar.parquet`: merged market panel from AKShare
- `data/staging/trade_calendar.parquet`: open trading dates
- `data/staging/stock_basic.parquet`: stock metadata snapshot
- `data/staging/stock_snapshot.parquet`: cached industry and listing-date snapshot
- `data/staging/industry_board_map.parquet`: current industry board-to-stock mapping snapshot
- `data/staging/index_membership_raw.parquet`: current index membership snapshot
- `data/staging/index_membership_daily.parquet`: current-constituent membership expanded across history
- `data/features/feature_panel.parquet`: engineered factor panel
- `data/labels/label_panel.parquet`: forward-return labels
- `data/staging/myquant_*.parquet`: optional parallel dataset from MyQuant
- `reports/weekly/*.csv`: model predictions and portfolio returns
- `reports/weekly/*.json`: model summary metrics
- `reports/weekly/*ensemble*.json`: adaptive ensemble metrics, weights, and stability summary

## Suggested First Upgrade Path

After the baseline runs end-to-end, the next useful additions are:

1. Replace current-constituent backtests with true historical membership data.
2. Add point-in-time financial statement factors and analyst estimate factors.
3. Add benchmark-relative metrics and turnover-aware rebalancing.
4. Tighten point-in-time ST and limit-lock restoration.
5. Plug the cleaned data into Qlib for experiment management.
