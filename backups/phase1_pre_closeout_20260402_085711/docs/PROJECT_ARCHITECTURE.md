# Project Architecture

## 1. Project Goal

This project is an A-share research and monitoring workspace with four linked layers:

1. Daily market data ingestion.
2. Point-in-time factor and label construction.
3. Walk-forward model training and portfolio diagnostics.
4. AI overlay + Streamlit dashboard for candidate review and watchlist monitoring.

The current live configuration uses MyQuant as the active data source and keeps an A-share watchlist centered on `000078.SZ`.

## 2. Repository Layout

### Core code

- `src/data/`: data acquisition and daily panel assembly.
- `src/features/`: factor engineering and forward-return label generation.
- `src/models/`: walk-forward training, scoring, diagnostics, and ensemble generation.
- `src/backtest/`: costs, portfolio simulation, and market trend filter.
- `src/agents/`: adaptive model weighting, event/news context, AI overlay report, and optional LLM request/export/execution bridge.
- `src/utils/`: project root helpers, YAML/Parquet IO, logging, and data source routing.

### App and operations

- `streamlit_app.py`: the local dashboard entrypoint.
- `scripts/start_streamlit.ps1`: dashboard supervisor launcher.
- `scripts/stop_streamlit.ps1`: clean shutdown for supervisor plus managed listener processes.
- `scripts/streamlit_status.ps1`: authoritative dashboard health/status check.

### Configuration

- `config/universe.yaml`: active data source, benchmark, universe mode, index, and watch symbols.
- `config/experiment.yaml`: train/valid/test split, rolling retrain rules, portfolio rules, overlay rules, and feature selection thresholds.
- `config/cost.yaml`: commission/tax/slippage assumptions.
- `config/watchlist.yaml`: manual holdings and mark prices used by the watchlist view.

### Outputs

- `data/staging/`: merged daily market panels, membership tables, snapshots, and dashboard runtime status files.
- `data/features/`: feature panel parquet.
- `data/labels/`: forward label parquet.
- `reports/weekly/`: model metrics, predictions, portfolios, AI overlay packets, and position notes.
- `logs/`: Streamlit supervisor and app logs.

## 3. End-to-End Data Flow

```text
MyQuant / AKShare / Tushare helpers
  -> data/staging/*daily_bar*.parquet + membership/snapshot tables
  -> data/features/*feature_panel.parquet
  -> data/labels/*label_panel.parquet
  -> ridge / lgbm walk-forward predictions + metrics + portfolios
  -> adaptive ensemble predictions + weights + stability summary
  -> AI overlay candidates + packet + markdown brief + optional LLM request/response artifacts
  -> Streamlit dashboard + watchlist monitoring + manual holding notes
```

## 4. Module Responsibilities

### 4.1 Data layer

- `src/data/downloader.py`
  - AKShare route.
  - Downloads symbol panels, builds current-index membership history, caches snapshots, and writes canonical staging outputs.
- `src/data/myquant_downloader.py`
  - MyQuant route.
  - Pulls static instrument info, daily metadata, daily bars, and history membership.
  - Builds `myquant_daily_bar.parquet` and related prefixed staging tables.
  - Trims trailing trade dates when the trade calendar is ahead of the latest returned daily bar date.
- `src/data/myquant_enrich.py`
  - Cleans MyQuant bar limits and merges SW industry history/fallback data.
- `src/data/myquant_panel.py`
  - Shared helpers for trimming incomplete trailing trade dates and keeping price-panel dates aligned with available MyQuant bars.
- `src/data/materialize_cache.py`
  - Rebuilds a usable daily panel from already downloaded symbol cache files.
- `src/data/index_membership.py`
  - Expands index membership over historical trade dates.
- `src/data/universe.py`
  - Reads `config/universe.yaml`.

### 4.2 Feature + label layer

- `src/features/build_feature_panel.py`
  - Main feature/label builder.
  - Merges factor modules and label validity fields into the research panel.
- `src/features/alpha_factors.py`
  - Price/momentum style factors.
- `src/features/risk_factors.py`
  - Volatility, drawdown, and risk-related factors.
- `src/features/quality_factors.py`
  - Valuation and quality-style fields where available.
- `src/features/labels.py`
  - Builds `ret_t1_t5`, `ret_t1_t10`, `ret_t1_t20` style forward returns.

### 4.3 Model layer

- `src/models/train_linear.py`
  - Ridge baseline.
  - Handles dataset loading, feature selection, walk-forward scoring, portfolio metrics, and feature importance export.
- `src/models/train_lgbm.py`
  - LightGBM baseline with the same report contract.
- `src/models/train_ensemble.py`
  - Merges Ridge + LGBM predictions using adaptive weights.
  - Writes ensemble predictions, metrics, portfolios, diagnostics, weights, and stability summary.
- `src/models/latest_inference.py`
  - Fits Ridge + LightGBM on all currently label-valid history and scores the newest unlabeled feature date.
  - Writes a parallel `inference` artifact set without changing historical `valid` / `test` backtest files.
- `src/models/walkforward.py`
  - Shared walk-forward logic, retrain schedules, score neutralization, and research filters.
- `src/models/evaluate.py`
  - Rank IC, top-N forward returns, yearly/regime summaries, and diagnostics.
- `src/models/feature_selection.py`
  - Missing-rate filter, daily rank-IC screening, correlation pruning, and max feature cap.
- `src/models/stability.py`
  - Split-to-split robustness summary.

### 4.4 Backtest layer

- `src/backtest/portfolio.py`
  - Daily/period top-N portfolio construction and selection.
- `src/backtest/risk_filter.py`
  - Benchmark proxy and trend-on/trend-off filter.
- `src/backtest/costs.py`
  - Trading cost helpers.
- `src/backtest/metrics.py`
  - Annualized return, Sharpe, and max drawdown calculations.

### 4.5 Agent / overlay layer

- `src/agents/ensemble_weights.py`
  - Converts validation metrics and stability grades into adaptive Ridge/LGBM weights.
- `src/agents/news_context.py`
  - Builds point-in-time notice/news/research summaries with local event cache files.
- `src/agents/overlay_report.py`
  - Produces candidate pool ranking, Chinese thesis summaries, AI brief, and overlay packet/markdown.
- `src/agents/overlay_inference_report.py`
  - Runs the latest unlabeled inference path and then builds a same-format AI overlay packet for the newest feature date.
- `src/agents/watch_plan.py`
  - Generates per-holding next-trade-day watch-plan markdown from the latest saved signals, watchlist marks, and trade calendar fallback.
- `src/agents/action_memo.py`
  - Generates per-holding execution memo markdown from the same holding snapshot used by the watch-plan flow.
- `src/agents/llm_bridge.py`
  - Exports candidate prompts to JSONL and, when `.env` is configured for OpenAI, auto-calls the Responses API and saves response artifacts.
- `src/utils/llm_discussion.py`
  - Merges historical verified overlay and latest inference overlay discussion rounds into a single per-symbol snapshot for the holding page and action memos.

## 5. Active Configuration Snapshot

As of `2026-04-01`, the important live settings are:

- Data source: `myquant`
- Universe mode: `current_index`
- Benchmark / active index: `000905.SH`
- Label horizon: `ret_t1_t10`
- Top-N portfolio size: `20`
- Rolling retrain: enabled, `monthly`
- Industry neutralization: enabled
- Max per industry: `2`
- Risk filter: enabled, `MA150`, benchmark-members-only
- Overlay pool: `30`
- Overlay selected count: `10`
- Overlay weight mode: `validation_adaptive`
- Watchlist holdings: `000078.SZ` only

## 6. Report Contract

Each training/overlay cycle writes a stable contract into `reports/weekly/`.

### Baseline / ensemble outputs

- `<source>_<model>_<split>_predictions.csv`
- `<source>_<model>_<split>_metrics.json`
- `<source>_<model>_<split>_portfolio.csv`
- `<source>_<model>_<split>_yearly.csv`
- `<source>_<model>_<split>_regime.csv`
- `<source>_<model>_stability.json`
- `<source>_<model>_feature_importance.csv` where applicable

### Overlay outputs

- `<source>_overlay_latest_candidates.csv`
- `<source>_overlay_latest_packet.json`
- `<source>_overlay_latest_brief.md`
- `<source>_overlay_llm_requests.jsonl`
- `<source>_overlay_llm_summary.md`
- `<source>_overlay_llm_responses.jsonl`
- `<source>_overlay_llm_response_summary.md`

### Latest unlabeled inference outputs

- `<source>_ridge_inference_predictions.csv`
- `<source>_lgbm_inference_predictions.csv`
- `<source>_ensemble_inference_predictions.csv`
- `<source>_inference_packet.json`
- `<source>_overlay_inference_candidates.csv`
- `<source>_overlay_inference_packet.json`
- `<source>_overlay_inference_brief.md`
- `<source>_overlay_inference_llm_requests.jsonl`
- `<source>_overlay_inference_llm_summary.md`
- `<source>_overlay_inference_llm_responses.jsonl`
- `<source>_overlay_inference_llm_response_summary.md`

### Position notes

- `000078_watch_plan_2026-04-02.md`
- `000078_action_memo_2026-04-02.md`

Important nuance:

- `*_watch_plan_*.md` files are now generated by `src.agents.watch_plan.py`.
- `*action_memo*.md` files are now generated by `src.agents.action_memo.py`.
- If the local trade calendar does not yet contain a future trade date, the holding-note flow falls back to the next business day for the plan date.

- The feature panel currently reaches `2026-04-01`, but the latest ensemble signal in the saved reports is `2026-03-18`.
- That lag is expected because the active label is `ret_t1_t10`, so the scoring outputs stop at the latest date with valid forward labels.
- The latest unlabeled inference path is the explicit workaround for that lag. It scores the newest feature date separately and skips future-dependent filters such as `can_enter_next_day` and label-validity checks.

## 7. Dashboard Architecture

### Sidebar actions

These buttons in `streamlit_app.py` call modules directly:

- `刷新部分面板` -> `src.data.materialize_cache`
- `重建特征与标签` -> `src.features.build_feature_panel`
- `运行岭回归基线` -> `src.models.train_linear`
- `运行梯度提升树基线` -> `src.models.train_lgbm`
- `运行自适应融合策略` -> `src.models.train_ensemble`
- `生成AI研判摘要` -> `src.agents.overlay_report`
- `生成最新未标注截面推理` -> `src.agents.overlay_inference_report`
- `观察持仓 -> 生成最新盯盘清单` -> `src.agents.watch_plan`
- `观察持仓 -> 生成最新操作备忘` -> `src.agents.action_memo`

### Dashboard tabs

- `平台总览`: dataset presence, model summary, download status.
- `因子探索`: factor snapshots, missingness, single-symbol factor history.
- `模型回测`: metrics, stability summary, portfolio curves, yearly/regime tables.
- `候选股票`: latest model picks and symbol score history.
- `观察持仓`: watchlist table, P/L, price freshness flag, historical verified rank vs latest inference rank, reduce-plan table, and the latest watch-plan / action-memo markdown for the selected symbol.
- `AI研判`: now shows both the historical label-verified overlay snapshot and the newest unlabeled inference snapshot, each with event coverage, model weights, AI thesis, optional LLM prompts, and executed external-model results when available.
- `观察持仓`: now backwrites multi-round LLM discussion summaries into the holding detail view, alongside historical verified rank and latest inference rank.
- `观察持仓`: the summary table now includes a one-line `盘前执行建议` column that compresses multi-round discussion state plus key price levels into the first screen.
- `运行日志`: downloader logs and dashboard service status.

### Service health source of truth

- The dashboard no longer trusts stale PID files alone.
- `scripts/streamlit_status.ps1` verifies supervisor PID, app PID, and port `8501`.
- `streamlit_app.py` reads that script output and shows the real service state in the UI.

## 8. Operations Playbook

### Full research refresh

```powershell
python -m src.data.myquant_downloader --start-date 2018-01-01 --end-date 2026-04-01
python -m src.data.myquant_enrich
python -m src.features.build_feature_panel
python -m src.models.train_linear
python -m src.models.train_lgbm
python -m src.models.train_ensemble
python -m src.agents.overlay_report
python -m src.agents.overlay_inference_report
python -m src.agents.watch_plan
python -m src.agents.action_memo
```

### Dashboard lifecycle

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_streamlit.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\streamlit_status.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\stop_streamlit.ps1
```

## 9. Known Limitations

- The workspace is not a Git repository, so recovery depends on file-based notes and report timestamps rather than commit history.
- Backtests still have point-in-time realism limits, especially where current snapshot fallbacks are used.
- Automated testing is still light, but it now covers:
  - `tests/test_ensemble_weights.py`
  - `tests/test_watch_plan.py`
  - `tests/test_myquant_panel.py`
  - `tests/test_holding_marks.py`
  - `tests/test_action_memo.py`
- When MyQuant daily metadata/trade calendar is ahead of the latest delivered daily bar date, the pipeline now trims the trailing empty trade date instead of saving a full day of NaN OHLC rows.
- After the `2026-04-02` refresh, `000078.SZ` no longer needs a manual mark price because the `2026-04-01` MyQuant daily bar landed correctly.

## 10. What To Update First After Future Work

When the project changes, update these items in order:

1. `docs/PROJECT_MEMORY_*.md`
2. `config/watchlist.yaml` if the holding or mark price changes
3. `reports/weekly/*action_memo*.md` for the active position
4. `README.md` or `docs/README.md` if the main entrypoint changes
