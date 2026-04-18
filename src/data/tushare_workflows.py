from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path
from pprint import pformat

from src.data.tushare_incremental_sync import sync_incremental_daily_bar
from src.db.dashboard_sync import sync_dashboard_artifacts
from src.features.build_feature_panel import build_feature_label_artifacts
from src.utils.data_source import active_data_source, normalize_data_source
from src.utils.io import project_root
from src.utils.logger import configure_logging

logger = configure_logging()


def run_tushare_incremental_refresh(
    *,
    root: Path | None = None,
    target_source: str | None = None,
    end_date: str | None = None,
) -> dict[str, object]:
    resolved_root = root or project_root()
    resolved_target_source = normalize_data_source(target_source or active_data_source())
    summary = sync_incremental_daily_bar(
        root=resolved_root,
        target_source=resolved_target_source,
        end_date=end_date,
        write_canonical=True,
    )
    return asdict(summary)


def run_tushare_full_refresh(
    *,
    root: Path | None = None,
    target_source: str | None = None,
    end_date: str | None = None,
) -> dict[str, object]:
    resolved_root = root or project_root()
    resolved_target_source = normalize_data_source(target_source or active_data_source())
    incremental_summary = run_tushare_incremental_refresh(
        root=resolved_root,
        target_source=resolved_target_source,
        end_date=end_date,
    )
    feature_summary = build_feature_label_artifacts(
        root=resolved_root,
        data_source=resolved_target_source,
    )
    dashboard_sync_summary = sync_dashboard_artifacts(root=resolved_root, data_source=resolved_target_source)
    return {
        "ok": bool(dashboard_sync_summary.ok),
        "target_source": resolved_target_source,
        "incremental": incremental_summary,
        "features": feature_summary,
        "dashboardSync": asdict(dashboard_sync_summary),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Tushare-backed incremental or full refresh workflows.")
    parser.add_argument(
        "--mode",
        choices=("incremental", "full"),
        default="full",
        help="Workflow mode. incremental only appends daily bars; full also refreshes features/labels/dashboard snapshots.",
    )
    parser.add_argument(
        "--target-source",
        default=None,
        help="Existing local data source to extend. Defaults to active config source, or akshare when omitted.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Sync through this date. Accepts YYYYMMDD or YYYY-MM-DD. Defaults to today.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    target_source = normalize_data_source(args.target_source or active_data_source())
    if args.mode == "incremental":
        summary = run_tushare_incremental_refresh(target_source=target_source, end_date=args.end_date)
    else:
        summary = run_tushare_full_refresh(target_source=target_source, end_date=args.end_date)
    logger.info("Tushare workflow result:\n{}", pformat(summary, sort_dicts=False))


if __name__ == "__main__":
    main()
