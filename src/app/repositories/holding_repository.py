from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.app.repositories.postgres_market_repository import (
    load_daily_bar_from_market_database,
    load_trade_dates_from_market_database,
)
from src.app.repositories.config_repository import load_universe_config
from src.app.repositories.report_repository import (
    load_daily_bar as repo_load_daily_bar,
    load_overlay_candidates as repo_load_overlay_candidates,
    load_overlay_inference_candidates as repo_load_overlay_inference_candidates,
    load_predictions as repo_load_predictions,
    load_trade_calendar as repo_load_trade_calendar,
)
from src.utils.data_source import normalize_data_source
from src.utils.io import project_root

WATCH_PLAN_FACTOR_COLUMNS = [
    "mom_5",
    "mom_20",
    "mom_60",
    "close_to_ma_20",
    "close_to_ma_60",
    "drawdown_60",
]


def resolve_data_source(root: Path | None = None) -> str:
    resolved_root = root or project_root()
    universe = load_universe_config(resolved_root, prefer_database=False)
    return normalize_data_source(universe.get("data_source", "akshare"))


def _latest_prediction_snapshot(frame: pd.DataFrame, usecols: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=usecols)

    available_columns = frame.columns.tolist()
    scoped_columns = [column for column in usecols if column in available_columns]
    required_columns = {"trade_date", "ts_code", "score"}
    if not required_columns.issubset(scoped_columns):
        return pd.DataFrame(columns=usecols)

    working = frame.loc[:, scoped_columns].copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    working = working.loc[working["trade_date"].notna()].copy()
    if working.empty:
        return pd.DataFrame(columns=usecols)

    latest_date = working["trade_date"].max()
    snapshot = working.loc[working["trade_date"] == latest_date].copy()
    for column in usecols:
        if column not in snapshot.columns:
            snapshot[column] = pd.NA
    snapshot = snapshot[usecols]
    snapshot = snapshot.sort_values("score", ascending=False).reset_index(drop=True)
    snapshot["rank"] = snapshot.index + 1
    snapshot["rank_pct"] = snapshot["score"].rank(pct=True, ascending=True)
    return snapshot


def load_prediction_snapshots(root: Path | None = None, data_source: str | None = None) -> dict[str, pd.DataFrame]:
    resolved_root = root or project_root()
    resolved_data_source = data_source or resolve_data_source(resolved_root)
    base_cols = ["trade_date", "ts_code", "name", "score"]
    return {
        "ridge": _latest_prediction_snapshot(
            repo_load_predictions(
                resolved_root,
                data_source=resolved_data_source,
                model_name="ridge",
                split_name="test",
                prefer_database=True,
            ),
            usecols=base_cols,
        ),
        "lgbm": _latest_prediction_snapshot(
            repo_load_predictions(
                resolved_root,
                data_source=resolved_data_source,
                model_name="lgbm",
                split_name="test",
                prefer_database=True,
            ),
            usecols=base_cols,
        ),
        "ensemble": _latest_prediction_snapshot(
            repo_load_predictions(
                resolved_root,
                data_source=resolved_data_source,
                model_name="ensemble",
                split_name="test",
                prefer_database=True,
            ),
            usecols=base_cols + WATCH_PLAN_FACTOR_COLUMNS,
        ),
    }


def load_overlay_symbols(
    root: Path | None = None,
    data_source: str | None = None,
    *,
    filename: str = "overlay_latest_candidates.csv",
) -> set[str]:
    resolved_root = root or project_root()
    resolved_data_source = data_source or resolve_data_source(resolved_root)
    if filename == "overlay_inference_candidates.csv":
        frame = repo_load_overlay_inference_candidates(
            resolved_root,
            data_source=resolved_data_source,
            prefer_database=True,
        )
    else:
        frame = repo_load_overlay_candidates(
            resolved_root,
            data_source=resolved_data_source,
            prefer_database=True,
        )
    if frame.empty or "ts_code" not in frame.columns:
        return set()
    return set(frame["ts_code"].astype(str))


def load_daily_bar_for_symbols(
    root: Path | None = None,
    *,
    data_source: str | None = None,
    symbols: list[str],
) -> pd.DataFrame:
    resolved_root = root or project_root()
    resolved_data_source = data_source or resolve_data_source(resolved_root)
    if not symbols:
        return pd.DataFrame()
    frame = repo_load_daily_bar(resolved_root, data_source=resolved_data_source, prefer_database=True)
    if frame.empty or "ts_code" not in frame.columns:
        return load_daily_bar_from_market_database(symbols)
    return (
        frame.loc[frame["ts_code"].astype(str).isin([str(symbol) for symbol in symbols])]
        .sort_values(["ts_code", "trade_date"])
        .reset_index(drop=True)
    )


def load_trade_dates(root: Path | None = None, data_source: str | None = None) -> pd.Series:
    resolved_root = root or project_root()
    resolved_data_source = data_source or resolve_data_source(resolved_root)

    frame = repo_load_trade_calendar(resolved_root, data_source=resolved_data_source, prefer_database=True)
    if frame.empty or "trade_date" not in frame.columns:
        return load_trade_dates_from_market_database()
    frame = frame[["trade_date"]].copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.loc[frame["trade_date"].notna()].drop_duplicates().sort_values("trade_date")
    return frame["trade_date"].reset_index(drop=True)
