# Project Memory - 2026-04-01

## 1. Why This File Exists

This file is the durable recovery snapshot for the workspace. The chat can be interrupted; this file should remain the stable handoff anchor.

## 2. Current Operating State

Snapshot date: `2026-04-01`

- Workspace: `D:\openlianghua`
- Git status: this directory is **not** a Git repository
- Active data source: `myquant`
- Dashboard URL: `http://localhost:8501`
- Dashboard control scripts:
  - `scripts/start_streamlit.ps1`
  - `scripts/streamlit_status.ps1`
  - `scripts/stop_streamlit.ps1`

Recent verified state:

- The Streamlit supervisor/status chain was repaired on `2026-04-01`.
- `streamlit_status.ps1` now checks real processes plus port `8501`, so it is the correct health source.
- The dashboard has been verified reachable locally after the repair.

## 3. Active Research Configuration

From `config/universe.yaml` and `config/experiment.yaml`:

- Benchmark/index: `000905.SH`
- Universe mode: `current_index`
- Label: `ret_t1_t10`
- Test end: `2026-04-01`
- Rolling retrain: monthly
- Risk filter: enabled
- Overlay source split: `test`
- Overlay candidate pool: `30`
- Overlay selected count: `10`
- Watch symbol: `000078.SZ`

## 4. Latest Durable Outputs

The latest important files already present are:

- `reports/weekly/myquant_ensemble_test_predictions.csv`
- `reports/weekly/myquant_ensemble_test_metrics.json`
- `reports/weekly/myquant_ensemble_weights.json`
- `reports/weekly/myquant_ensemble_stability.json`
- `reports/weekly/myquant_overlay_latest_packet.json`
- `reports/weekly/myquant_overlay_latest_candidates.csv`
- `reports/weekly/000078_watch_plan_2026-04-02.md`
- `reports/weekly/000078_action_memo_2026-04-02.md`

Watch-plan note:

- `reports/weekly/000078_watch_plan_2026-04-02.md` is now reproducible from code.
- Regenerate holding watch plans with:

```powershell
python -m src.agents.watch_plan
```

Important interpretation note:

- `data/staging/myquant_data_quality.json` now says the cleaned MyQuant panel reaches `2026-03-31`.
- The latest ensemble signal date in the saved reports is `2026-03-17`.
- That is not necessarily a bug; with `ret_t1_t10`, saved predictions lag the raw bar panel because they stop at the latest forward-label-valid date.

MyQuant panel repair note:

- On `2026-04-01`, the trade calendar and instrument metadata had advanced to `2026-04-01`, but `myquant_bars_raw.parquet` still stopped at `2026-03-31`.
- That had created a full trailing day of empty OHLC rows in `myquant_daily_bar.parquet`.
- The pipeline now trims trailing empty-price trade dates automatically, and the local panel was repaired on `2026-04-01`.

## 5. Watchlist / Position State

The watchlist currently contains one holding in `config/watchlist.yaml`:

- `000078.SZ`
- Name: `海王生物`
- Cost: `3.851`
- Shares: `15000`
- Manual mark price: `3.45`
- Manual mark date: `2026-04-01`

Why manual mark exists:

- The local MyQuant bar history currently ends at `2026-03-31` after trimming the incomplete trailing date `2026-04-01`.
- The dashboard therefore still uses the manual mark for the latest holding valuation on `2026-04-01`.
- The `观察持仓` 页面 now explicitly flags when the reference price is manual and shows the latest available local daily-bar date alongside it.

## 6. Recovery Checklist

If the chat is lost again, recover in this order:

1. Read `docs/README.md`.
2. Read `docs/PROJECT_ARCHITECTURE.md`.
3. Read this file.
4. Check dashboard state:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\streamlit_status.ps1
```

5. If needed, restart the dashboard:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\stop_streamlit.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\start_streamlit.ps1
```

6. Read the current holding memo:

```text
reports/weekly/000078_action_memo_2026-04-02.md
```

If PowerShell output shows garbled Chinese, read the file with UTF-8 explicitly:

```powershell
Get-Content -Encoding UTF8 .\reports\weekly\000078_action_memo_2026-04-02.md
```

7. Open `http://localhost:8501` and use the `观察持仓` and `AI研判` tabs.

8. If the watch-plan markdown is missing or stale, regenerate it:

```powershell
python -m src.agents.watch_plan
```

## 7. Latest Model Snapshot

Based on the saved MyQuant reports:

- Ensemble `valid` annualized return: about `15.07%`
- Ensemble `test` annualized return: about `17.74%`
- Ensemble `test` Sharpe: about `0.90`
- Ensemble `test` max drawdown: about `-13.78%`
- Stability grade: `较稳`
- Adaptive weights:
  - Ridge: about `77.1%`
  - LightGBM: about `22.9%`

Interpretation:

- Ridge is currently the dominant model.
- The ensemble is usable as a ranking aid, but not yet a fully automated trading system.

## 8. Known Issues To Remember

- No Git history is available here.
- The watch-plan markdown is auto-generated now, but the higher-level `action_memo` note is still manual.
- Automated testing is still thin, though it now includes:
  - `tests/test_ensemble_weights.py`
  - `tests/test_watch_plan.py`
  - `tests/test_myquant_panel.py`
- Some Chinese text in older generated markdown/log files may display garbled in certain terminals, but the file contents themselves are still usable when read with UTF-8-aware tools.
- Same-day MyQuant refreshes can still have the trade calendar ahead of the latest delivered daily bars, but the pipeline now trims those incomplete trailing dates instead of persisting blank OHLC rows.

## 9. Immediate Backlog

Recommended next engineering tasks:

1. Add automated tests for:
   - `streamlit_status.ps1` / dashboard status integration
   - `overlay_report.py`
   - `walkforward.py`
2. Consider adding a scripted generator for `*action_memo*.md` so the higher-level execution note is also reproducible.
3. Keep updating this memory file after each major pipeline refresh or position update.

## 10. Current Decision File

For tomorrow's trading review of the held stock, the active note is:

- `reports/weekly/000078_action_memo_2026-04-02.md`
