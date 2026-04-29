from __future__ import annotations

import os
import importlib.util
from pathlib import Path

import pandas as pd

from src.app.repositories.report_repository import (
    get_artifact_metadata,
    load_daily_bar,
    load_dataset_summary,
    load_feature_panel,
    load_label_panel,
)
from src.app.repositories.postgres_market_repository import load_stock_bar_summary_from_market_database
from src.app.repositories.research_panel_repository import load_research_panel_summary
from src.app.repositories.research_panel_repository import load_latest_successful_panel_run
from src.db.dashboard_artifact_keys import binary_artifact_key
from src.utils.data_source import (
    active_data_source,
    detect_materialized_data_source,
    normalize_data_source,
)
from src.utils.io import project_root


def _env_token_status(root: Path) -> tuple[bool, bool]:
    return _env_key_status(root, "TUSHARE_TOKEN")


def _env_key_status(root: Path, key_name: str) -> tuple[bool, bool]:
    if str(os.getenv(key_name, "")).strip():
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
        if key.strip() == key_name:
            token_configured = bool(value.strip())
            break
    return True, token_configured


def _use_database_artifacts(root: Path) -> bool:
    try:
        return root.resolve() == project_root().resolve()
    except OSError:
        return False


def _frame_status(frame: pd.DataFrame, *, artifact_metadata: dict[str, object]) -> dict[str, object]:
    if frame.empty:
        return {
            "exists": bool(artifact_metadata),
            "rowCount": 0,
            "symbolCount": 0,
            "latestTradeDate": None,
            "updatedAt": artifact_metadata.get("updated_at"),
        }

    working = frame.copy()
    latest_trade_date = None
    if "trade_date" in working.columns:
        working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
        latest_value = working["trade_date"].max()
        latest_trade_date = None if pd.isna(latest_value) else latest_value.date().isoformat()
    symbol_count = int(working["ts_code"].nunique()) if "ts_code" in working.columns else 0
    return {
        "exists": bool(artifact_metadata),
        "rowCount": int(len(working)),
        "symbolCount": symbol_count,
        "latestTradeDate": latest_trade_date,
        "updatedAt": artifact_metadata.get("updated_at"),
    }


def _artifact_status(artifact_metadata: dict[str, object], *, dataset_summary: dict[str, object] | None = None) -> dict[str, object]:
    fallback_summary = dataset_summary or {}
    symbol_count = artifact_metadata.get("symbol_count") if "symbol_count" in artifact_metadata else fallback_summary.get("feature_symbols")
    latest_trade_date = artifact_metadata.get("latest_trade_date") if "latest_trade_date" in artifact_metadata else fallback_summary.get("date_max")
    return {
        "exists": bool(artifact_metadata),
        "rowCount": int(artifact_metadata.get("rows", 0) or 0),
        "symbolCount": int(symbol_count or 0),
        "latestTradeDate": latest_trade_date,
        "updatedAt": artifact_metadata.get("updated_at"),
    }


def build_data_management_payload(
    *,
    root: Path | None = None,
    target_source: str | None = None,
    include_sensitive: bool = True,
) -> dict[str, object]:
    resolved_root = root or project_root()
    configured_source = normalize_data_source(target_source or active_data_source())
    materialized_source = detect_materialized_data_source(resolved_root, configured_source)
    resolved_target_source = materialized_source
    use_database_artifacts = _use_database_artifacts(resolved_root)
    env_exists, token_configured = _env_token_status(resolved_root)

    daily_bar = load_daily_bar(resolved_root, data_source=resolved_target_source, prefer_database=use_database_artifacts) if not use_database_artifacts else pd.DataFrame()
    feature_panel = load_feature_panel(resolved_root, data_source=resolved_target_source, prefer_database=use_database_artifacts) if not use_database_artifacts else pd.DataFrame()
    label_panel = load_label_panel(resolved_root, data_source=resolved_target_source, prefer_database=use_database_artifacts) if not use_database_artifacts else pd.DataFrame()
    dataset_summary = load_dataset_summary(resolved_root, data_source=resolved_target_source, prefer_database=use_database_artifacts)

    daily_bar_artifact = get_artifact_metadata(binary_artifact_key(resolved_target_source, "daily_bar")) if use_database_artifacts else {"updated_at": None}
    panel_run = load_latest_successful_panel_run(data_source=resolved_target_source) if use_database_artifacts else {}
    panel_summary = load_research_panel_summary(data_source=resolved_target_source) if use_database_artifacts else {}
    feature_artifact = {"updated_at": panel_run.get("updated_at")} if panel_run else {"updated_at": None}
    label_artifact = {"updated_at": panel_run.get("updated_at")} if panel_run else {"updated_at": None}

    daily_bar_status = _frame_status(daily_bar, artifact_metadata=daily_bar_artifact)
    if use_database_artifacts:
        stock_bar_summary = load_stock_bar_summary_from_market_database()
        if stock_bar_summary.get("rowCount"):
            daily_bar_status["rowCount"] = stock_bar_summary["rowCount"]
            daily_bar_status["symbolCount"] = stock_bar_summary["symbolCount"]
            daily_bar_status["latestTradeDate"] = stock_bar_summary["latestTradeDate"]

    feature_status = _frame_status(feature_panel, artifact_metadata=feature_artifact)
    label_status = _frame_status(label_panel, artifact_metadata=label_artifact)
    research_panel_status = {
        "exists": False,
        "rowCount": 0,
        "symbolCount": 0,
        "latestTradeDate": None,
        "updatedAt": None,
    }
    if panel_summary or panel_run:
        summary_source = panel_summary or panel_run
        research_panel_status = {
            "exists": True,
            "rowCount": int(summary_source.get("row_count", 0) or 0),
            "symbolCount": int(summary_source.get("symbol_count", 0) or 0),
            "latestTradeDate": summary_source.get("date_max"),
            "updatedAt": panel_run.get("updated_at"),
        }
        for status_payload in (feature_status, label_status):
            status_payload["exists"] = True
            status_payload["rowCount"] = int(summary_source.get("row_count", 0) or 0)
            status_payload["symbolCount"] = int(summary_source.get("symbol_count", 0) or 0)
            status_payload["latestTradeDate"] = summary_source.get("date_max")
            status_payload["updatedAt"] = panel_run.get("updated_at")

    return {
        "targetSource": resolved_target_source,
        "activeDataSource": resolved_target_source,
        "configuredDataSource": configured_source,
        "sourceMismatch": configured_source != resolved_target_source,
        "today": pd.Timestamp.now(tz="Asia/Shanghai").date().isoformat(),
        "envPath": ".env",
        "envFileExists": env_exists,
        "tokenConfigured": token_configured if include_sensitive else None,
        "dailyBar": daily_bar_status,
        "researchPanel": research_panel_status,
        "legacyFeatureView": feature_status,
        "legacyLabelView": label_status,
        "datasetSummary": dataset_summary,
        "scripts": {
            "incremental": "scripts/refresh_daily_bar_tushare.ps1",
            "fullRefresh": "scripts/refresh_full_pipeline_tushare.ps1",
        },
    }


def build_myquant_status_payload(
    *,
    root: Path | None = None,
    include_sensitive: bool = True,
) -> dict[str, object]:
    resolved_root = root or project_root()
    env_exists, token_configured = _env_key_status(resolved_root, "MYQUANT_TOKEN")
    sdk_available = importlib.util.find_spec("gm") is not None
    use_database_artifacts = _use_database_artifacts(resolved_root)
    artifact_metadata = get_artifact_metadata(binary_artifact_key("myquant", "daily_bar")) if use_database_artifacts else {"updated_at": None}
    if use_database_artifacts:
        dataset_summary = load_dataset_summary(resolved_root, data_source="myquant", prefer_database=True)
        daily_bar_status = _artifact_status(artifact_metadata, dataset_summary=dataset_summary)
    else:
        daily_bar = load_daily_bar(
            resolved_root,
            data_source="myquant",
            prefer_database=False,
        )
        daily_bar_status = _frame_status(daily_bar, artifact_metadata=artifact_metadata)

    quality_path = resolved_root / "data" / "staging" / "myquant_data_quality.json"
    quality_exists = quality_path.exists()
    return {
        "targetSource": "myquant",
        "envFileExists": env_exists,
        "tokenConfigured": token_configured if include_sensitive else None,
        "sdkAvailable": sdk_available,
        "dailyBar": daily_bar_status,
        "qualityReportExists": quality_exists,
        "scripts": {
            "download": "python -m src.data.myquant_downloader",
            "enrich": "python -m src.data.myquant_enrich",
            "researchRefresh": "python -m src.features.build_feature_panel",
        },
    }
