from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from src.app.repositories.report_repository import (
    load_daily_bar,
    load_dataset_summary,
    load_feature_panel,
    load_label_panel,
)
from src.utils.data_source import active_data_source, normalize_data_source, source_or_canonical_path
from src.utils.io import project_root


def _env_token_status(root: Path) -> tuple[bool, bool]:
    if str(os.getenv("TUSHARE_TOKEN", "")).strip():
        return (root / ".env").exists(), True

    env_path = root / ".env"
    if not env_path.exists():
        return False, False

    token_configured = False
    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == "TUSHARE_TOKEN":
            token_configured = bool(value.strip())
            break
    return True, token_configured


def _frame_status(frame: pd.DataFrame, *, data_path: Path) -> dict[str, object]:
    if frame.empty:
        return {
            "exists": data_path.exists(),
            "rowCount": 0,
            "symbolCount": 0,
            "latestTradeDate": None,
            "updatedAt": pd.Timestamp(data_path.stat().st_mtime, unit="s").isoformat() if data_path.exists() else None,
        }

    working = frame.copy()
    latest_trade_date = None
    if "trade_date" in working.columns:
        working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
        latest_value = working["trade_date"].max()
        latest_trade_date = None if pd.isna(latest_value) else latest_value.date().isoformat()
    symbol_count = int(working["ts_code"].nunique()) if "ts_code" in working.columns else 0
    updated_at = pd.Timestamp(data_path.stat().st_mtime, unit="s").isoformat() if data_path.exists() else None
    return {
        "exists": data_path.exists(),
        "rowCount": int(len(working)),
        "symbolCount": symbol_count,
        "latestTradeDate": latest_trade_date,
        "updatedAt": updated_at,
    }


def build_data_management_payload(
    *,
    root: Path | None = None,
    target_source: str | None = None,
    include_sensitive: bool = True,
) -> dict[str, object]:
    resolved_root = root or project_root()
    resolved_target_source = normalize_data_source(target_source or active_data_source())
    env_exists, token_configured = _env_token_status(resolved_root)

    daily_bar_path = source_or_canonical_path(resolved_root / "data" / "staging", "daily_bar.parquet", resolved_target_source)
    feature_path = source_or_canonical_path(resolved_root / "data" / "features", "feature_panel.parquet", resolved_target_source)
    label_path = source_or_canonical_path(resolved_root / "data" / "labels", "label_panel.parquet", resolved_target_source)

    daily_bar = load_daily_bar(resolved_root, data_source=resolved_target_source, prefer_database=False)
    feature_panel = load_feature_panel(resolved_root, data_source=resolved_target_source, prefer_database=False)
    label_panel = load_label_panel(resolved_root, data_source=resolved_target_source, prefer_database=False)
    dataset_summary = load_dataset_summary(resolved_root, data_source=resolved_target_source, prefer_database=False)

    return {
        "targetSource": resolved_target_source,
        "activeDataSource": resolved_target_source,
        "today": pd.Timestamp.now(tz="Asia/Shanghai").date().isoformat(),
        "envPath": ".env",
        "envFileExists": env_exists,
        "tokenConfigured": token_configured if include_sensitive else None,
        "dailyBar": _frame_status(daily_bar, data_path=daily_bar_path),
        "featurePanel": _frame_status(feature_panel, data_path=feature_path),
        "labelPanel": _frame_status(label_panel, data_path=label_path),
        "datasetSummary": dataset_summary,
        "scripts": {
            "incremental": "scripts/refresh_daily_bar_tushare.ps1",
            "fullRefresh": "scripts/refresh_full_pipeline_tushare.ps1",
        },
    }
