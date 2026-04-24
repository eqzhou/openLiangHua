from __future__ import annotations

from typing import Any
from src.app.facades.base import (
    _json_ready, 
    _frame_records,
)
from src.app.services.dashboard_data_service import (
    load_metrics,
    load_stability,
    load_portfolio,
    load_feature_importance,
    load_diagnostic_table,
)
from src.app.viewmodels.model_backtest_vm import build_monthly_summary, normalize_regime_view


def get_model_backtest_payload(*, model_name: str = "lgbm", split_name: str = "test") -> dict[str, Any]:
    return {
        **get_model_backtest_summary_payload(model_name=model_name, split_name=split_name),
        **get_model_backtest_portfolio_payload(model_name=model_name, split_name=split_name),
        **get_model_backtest_diagnostics_payload(model_name=model_name, split_name=split_name),
    }


def get_model_backtest_summary_payload(*, model_name: str = "lgbm", split_name: str = "test") -> dict[str, Any]:
    return {
        "modelName": model_name,
        "splitName": split_name,
        "metrics": _json_ready(load_metrics(model_name, split_name)),
        "stability": _json_ready(load_stability(model_name)),
    }


def get_model_backtest_portfolio_payload(*, model_name: str = "lgbm", split_name: str = "test") -> dict[str, Any]:
    portfolio = load_portfolio(model_name, split_name)
    monthly_summary = build_monthly_summary(portfolio)
    return {
        "modelName": model_name,
        "splitName": split_name,
        "portfolio": _frame_records(portfolio),
        "monthlySummary": _frame_records(monthly_summary.tail(24)),
    }


def get_model_backtest_diagnostics_payload(*, model_name: str = "lgbm", split_name: str = "test") -> dict[str, Any]:
    regime_diagnostics = normalize_regime_view(load_diagnostic_table(model_name, split_name, "regime"))
    return {
        "modelName": model_name,
        "splitName": split_name,
        "importance": _frame_records(load_feature_importance(model_name), limit=20),
        "yearlyDiagnostics": _frame_records(load_diagnostic_table(model_name, split_name, "yearly")),
        "regimeDiagnostics": _frame_records(regime_diagnostics),
    }
