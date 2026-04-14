from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.utils.io import ensure_dir, project_root, save_parquet
from src.utils.logger import configure_logging

logger = configure_logging()


def _read_cached_panel(path: Path) -> pd.DataFrame | None:
    try:
        frame = pd.read_parquet(path)
        if frame.empty:
            return None
        return frame
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"Skipping unreadable cache file {path.name}: {exc}")
        return None


def materialize_cache(min_age_seconds: int = 30) -> dict[str, int]:
    root = project_root()
    raw_dir = root / "data" / "raw" / "akshare"
    staging_dir = ensure_dir(root / "data" / "staging")
    reports_dir = ensure_dir(root / "reports" / "weekly")

    now = pd.Timestamp.now()
    cache_files = sorted(raw_dir.glob("*.parquet"))
    eligible_files = [
        path
        for path in cache_files
        if (now - pd.Timestamp(path.stat().st_mtime, unit="s")).total_seconds() >= min_age_seconds
    ]

    frames: list[pd.DataFrame] = []
    for path in eligible_files:
        frame = _read_cached_panel(path)
        if frame is not None:
            frames.append(frame)

    if not frames:
        raise RuntimeError("No cached symbol files were safe to materialize yet.")

    daily_bar = pd.concat(frames, ignore_index=True)
    daily_bar["trade_date"] = pd.to_datetime(daily_bar["trade_date"])
    daily_bar = daily_bar.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

    membership_path = staging_dir / "index_membership_daily.parquet"
    if membership_path.exists():
        membership = pd.read_parquet(membership_path)[
            ["trade_date", "ts_code", "index_code", "index_weight", "is_index_member"]
        ].copy()
        membership["trade_date"] = pd.to_datetime(membership["trade_date"])
        daily_bar = daily_bar.merge(membership, on=["trade_date", "ts_code"], how="left")
        daily_bar["is_index_member"] = daily_bar["is_index_member"].fillna(False)
    else:
        daily_bar["index_code"] = None
        daily_bar["index_weight"] = np.nan
        daily_bar["is_index_member"] = True

    save_parquet(daily_bar, staging_dir / "daily_bar.parquet")

    summary = {
        "cached_files_total": len(cache_files),
        "cached_files_materialized": len(eligible_files),
        "symbols_in_panel": int(daily_bar["ts_code"].nunique()),
        "rows_in_panel": int(len(daily_bar)),
        "date_min": str(daily_bar["trade_date"].min().date()),
        "date_max": str(daily_bar["trade_date"].max().date()),
    }
    (reports_dir / "materialize_cache_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    logger.info(f"Materialized {summary['symbols_in_panel']} symbols into data/staging/daily_bar.parquet")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a current usable panel from cached AKShare symbol files.")
    parser.add_argument(
        "--min-age-seconds",
        type=int,
        default=30,
        help="Skip files modified more recently than this to avoid reading a file being written.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    materialize_cache(min_age_seconds=max(0, args.min_age_seconds))
    from src.db.dashboard_sync import sync_dashboard_artifacts

    summary = sync_dashboard_artifacts()
    logger.info(summary.message)
