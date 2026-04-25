from __future__ import annotations

import copy
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.app.services.dashboard_data_service import (
    LABEL_OPTIONS,
    MODEL_LABELS,
    MODEL_NAMES,
    ROOT,
    SPLITS,
    SPLIT_LABELS,
    WATCH_SCOPE_MAP,
    WATCH_SORT_MAP,
    load_dataset_summary,
    load_experiment_config,
    load_watchlist_config,
    sync_dashboard_database,
    clear_dashboard_data_caches,
)

# Constants moved from dashboard_facade.py or dashboard_data_service
WATCHLIST_SUMMARY_RECORD_FIELDS = [
    "ts_code",
    "name",
    "industry",
    "source_category",
    "source_tags",
    "entry_group",
    "ensemble_rank",
    "inference_ensemble_rank",
    "realtime_price",
    "realtime_pct_chg",
    "mark_price",
    "cost_basis",
    "shares",
    "unrealized_pnl",
    "unrealized_pnl_pct",
    "llm_latest_status",
    "premarket_plan",
    "focus_note",
    "is_overlay_selected",
    "is_inference_overlay_selected",
]

AI_REVIEW_SUMMARY_CANDIDATE_FIELDS = [
    "ts_code",
    "name",
    "industry",
    "industry_display",
    "final_score",
    "confidence_level",
    "action_hint",
    "trade_date",
]

AI_REVIEW_SUMMARY_SELECTED_FIELDS = [
    "ts_code",
    "name",
    "industry",
    "industry_display",
    "final_score",
    "quant_score",
    "factor_overlay_score",
    "model_consensus",
    "confidence_level",
    "thesis_summary",
    "action_hint",
    "trade_date",
]

def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is not None:
            return value.isoformat()
        return str(value.date())
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        val = float(value)
        return None if math.isnan(val) else val
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, dict):
        return {k: _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if pd.isna(value):
        return None
    return value


def _frame_records(frame: pd.DataFrame, *, limit: int | None = None) -> list[dict[str, Any]]:
    working = frame.copy()
    if limit is not None:
        working = working.head(limit)
    return [_json_ready(record) for record in working.to_dict(orient="records")]


def _project_record_fields(record: dict[str, Any], field_names: list[str]) -> dict[str, Any]:
    return {field_name: _json_ready(record.get(field_name)) for field_name in field_names if field_name in record}

def _watchlist_entry_count(watchlist_config: dict[str, Any]) -> int:
    holdings = watchlist_config.get("holdings", []) or []
    focus_pool = watchlist_config.get("focus_pool", []) or []
    return len(holdings) + len(focus_pool)

def _clean_config_summary_text(config: dict[str, Any]) -> str:
    if not config:
        return "当前还没有读取到研究参数。"
    rolling = config.get("rolling", {})
    selection = config.get("selection", {})
    rolling_text = "开启" if rolling.get("enabled", False) else "关闭"
    rolling_freq = rolling.get("freq", "-")
    neutral_text = "开启" if selection.get("neutralize", False) else "关闭"
    return (
        f"样本：{config.get('label_col', '-')} "
        f" | 滚动训练：{rolling_text}({rolling_freq})"
        f" | 行业中性化：{neutral_text}"
    )

def clear_dashboard_caches() -> None:
    clear_dashboard_data_caches()


def get_bootstrap_payload() -> dict[str, Any]:
    from src.app.services.dashboard_data_service import list_available_actions
    return {
        "modelNames": MODEL_NAMES,
        "splitNames": SPLITS,
        "labelOptions": LABEL_OPTIONS,
        "modelLabels": MODEL_LABELS,
        "splitLabels": SPLIT_LABELS,
        "watchScopes": WATCH_SCOPE_MAP,
        "watchSorts": WATCH_SORT_MAP,
        "actions": list_available_actions(),
    }
