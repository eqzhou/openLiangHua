from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from src.app.repositories.config_repository import load_universe_config
from src.utils.data_source import normalize_data_source, source_or_canonical_path
from src.utils.io import project_root

WATCH_PLAN_FACTOR_COLUMNS = [
    "mom_5",
    "mom_20",
    "mom_60",
    "close_to_ma_20",
    "close_to_ma_60",
    "drawdown_60",
]

PREDICTION_CHUNK_SIZE = 200_000


def resolve_data_source(root: Path | None = None) -> str:
    resolved_root = root or project_root()
    universe = load_universe_config(resolved_root, prefer_database=False)
    return normalize_data_source(universe.get("data_source", "akshare"))


def _read_prediction_latest_snapshot(path: Path, usecols: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=usecols)

    available_columns = pd.read_csv(path, nrows=0).columns.tolist()
    scoped_columns = [column for column in usecols if column in available_columns]
    required_columns = {"trade_date", "ts_code", "score"}
    if not required_columns.issubset(scoped_columns):
        return pd.DataFrame(columns=usecols)

    latest_date: pd.Timestamp | None = None
    latest_frames: list[pd.DataFrame] = []

    for chunk in pd.read_csv(path, usecols=scoped_columns, chunksize=PREDICTION_CHUNK_SIZE):
        chunk["trade_date"] = pd.to_datetime(chunk["trade_date"], errors="coerce")
        chunk = chunk.loc[chunk["trade_date"].notna()].copy()
        if chunk.empty:
            continue

        chunk_max = chunk["trade_date"].max()
        chunk_latest = chunk.loc[chunk["trade_date"] == chunk_max].copy()

        if latest_date is None or chunk_max > latest_date:
            latest_date = chunk_max
            latest_frames = [chunk_latest]
        elif chunk_max == latest_date:
            latest_frames.append(chunk_latest)

    if latest_date is None or not latest_frames:
        return pd.DataFrame(columns=usecols)

    snapshot = pd.concat(latest_frames, ignore_index=True)
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
    reports_dir = resolved_root / "reports" / "weekly"
    base_cols = ["trade_date", "ts_code", "name", "score"]
    return {
        "ridge": _read_prediction_latest_snapshot(
            source_or_canonical_path(reports_dir, "ridge_test_predictions.csv", resolved_data_source),
            usecols=base_cols,
        ),
        "lgbm": _read_prediction_latest_snapshot(
            source_or_canonical_path(reports_dir, "lgbm_test_predictions.csv", resolved_data_source),
            usecols=base_cols,
        ),
        "ensemble": _read_prediction_latest_snapshot(
            source_or_canonical_path(reports_dir, "ensemble_test_predictions.csv", resolved_data_source),
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
    path = source_or_canonical_path(resolved_root / "reports" / "weekly", filename, resolved_data_source)
    if not path.exists():
        return set()
    frame = pd.read_csv(path, usecols=["ts_code"])
    return set(frame["ts_code"].astype(str))


def load_daily_bar_for_symbols(
    root: Path | None = None,
    *,
    data_source: str | None = None,
    symbols: list[str],
) -> pd.DataFrame:
    resolved_root = root or project_root()
    resolved_data_source = data_source or resolve_data_source(resolved_root)
    path = source_or_canonical_path(resolved_root / "data" / "staging", "daily_bar.parquet", resolved_data_source)
    if not path.exists() or not symbols:
        return pd.DataFrame()

    columns = ["trade_date", "ts_code", "name", "close", "open", "high", "low", "pct_chg", "amount", "industry"]
    table = pq.read_table(path, columns=columns, filters=[("ts_code", "in", symbols)])
    frame = table.to_pandas()
    if frame.empty:
        return frame
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)


def load_trade_dates(root: Path | None = None, data_source: str | None = None) -> pd.Series:
    resolved_root = root or project_root()
    resolved_data_source = data_source or resolve_data_source(resolved_root)

    paths: list[Path] = []
    source_path = source_or_canonical_path(resolved_root / "data" / "staging", "trade_calendar.parquet", resolved_data_source)
    if source_path.exists():
        paths.append(source_path)

    canonical_path = resolved_root / "data" / "staging" / "trade_calendar.parquet"
    if canonical_path.exists() and canonical_path not in paths:
        paths.append(canonical_path)

    frames: list[pd.DataFrame] = []
    for path in paths:
        frame = pd.read_parquet(path)
        if "trade_date" not in frame.columns:
            continue
        frames.append(frame[["trade_date"]].copy())

    if not frames:
        return pd.Series(dtype="datetime64[ns]")

    combined = pd.concat(frames, ignore_index=True)
    combined["trade_date"] = pd.to_datetime(combined["trade_date"], errors="coerce")
    combined = combined.loc[combined["trade_date"].notna()].drop_duplicates().sort_values("trade_date")
    return combined["trade_date"].reset_index(drop=True)
