from __future__ import annotations

import pandas as pd
from typing import Any

from src.app.facades.base import (
    _json_ready, 
    _frame_records,
)
from src.app.services.dashboard_data_service import (
    MODEL_LABELS,
    MODEL_NAMES,
    build_metrics_table,
    load_dataset_summary,
    load_portfolio,
)
from src.app.viewmodels.overview_vm import build_equity_curve_frame, build_model_comparison_frame


def get_overview_summary_payload(split_name: str = "test") -> dict[str, Any]:
    summary = load_dataset_summary()
    metrics_table = build_metrics_table()
    shown_columns = [
        "model",
        "split",
        "rank_ic_mean",
        "top_n_hit_rate",
        "top_n_forward_mean",
        "daily_portfolio_annualized_return",
        "daily_portfolio_sharpe",
        "daily_portfolio_max_drawdown",
        "avg_turnover_ratio",
    ]
    comparison = build_model_comparison_frame(metrics_table, shown_columns)
    return {
        "summary": _json_ready(summary),
        "comparison": _frame_records(comparison),
        "selectedSplit": split_name,
    }


def get_overview_curves_payload(split_name: str = "test") -> dict[str, Any]:
    equity_curves = build_equity_curve_frame(
        model_names=MODEL_NAMES,
        split_name=split_name,
        model_labels=MODEL_LABELS,
        load_portfolio=load_portfolio,
    )
    return {
        "selectedSplit": split_name,
        "equityCurves": _frame_records(equity_curves.reset_index()) if not equity_curves.empty else [],
    }


def get_overview_payload(split_name: str = "test") -> dict[str, Any]:
    return {
        **get_overview_summary_payload(split_name),
        **get_overview_curves_payload(split_name),
    }
