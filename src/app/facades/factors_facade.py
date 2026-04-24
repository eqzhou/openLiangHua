from __future__ import annotations

import pandas as pd
from typing import Any

from src.app.facades.base import (
    _json_ready, 
    _frame_records,
)
from src.app.services.dashboard_data_service import (
    build_factor_explorer_snapshot,
    load_feature_history_for_symbol,
)
from src.app.viewmodels.factor_explorer_vm import (
    build_factor_ranking,
    build_latest_factor_snapshot,
)


def get_factor_explorer_summary_payload(
    *,
    factor_name: str | None = None,
    symbol: str | None = None,
    history_factor: str | None = None,
) -> dict[str, Any]:
    snapshot_payload = build_factor_explorer_snapshot()
    if not snapshot_payload.get("available"):
        return {
            "available": False,
            "latestDate": snapshot_payload.get("latestDate"),
            "factorOptions": [],
            "symbolOptions": [],
            "ranking": [],
            "missingRates": [],
            "selectedFactor": "",
            "selectedHistoryFactor": "",
            "selectedSymbol": "",
        }

    factor_options = list(snapshot_payload.get("factorOptions", []) or [])
    symbol_options = list(snapshot_payload.get("symbolOptions", []) or [])
    cross_section = pd.DataFrame(snapshot_payload.get("crossSection", []) or [])
    missing_rates = pd.DataFrame(snapshot_payload.get("missingRates", []) or [])

    factor_keys = [str(item.get("key", "")) for item in factor_options if str(item.get("key", ""))]
    selected_factor = factor_name or (factor_keys[min(11, len(factor_keys) - 1)] if factor_keys else "")
    selected_history_factor = history_factor or (factor_keys[min(3, len(factor_keys) - 1)] if factor_keys else "")
    selected_symbol = symbol or (symbol_options[0] if symbol_options else "")
    ranking = build_factor_ranking(cross_section, selected_factor)
    selected_row = cross_section.loc[cross_section["ts_code"].astype(str) == selected_symbol].head(1) if selected_symbol and not cross_section.empty else pd.DataFrame()

    return {
        "available": True,
        "latestDate": snapshot_payload.get("latestDate"),
        "selectedFactor": selected_factor,
        "selectedHistoryFactor": selected_history_factor,
        "selectedSymbol": selected_symbol,
        "factorOptions": factor_options,
        "symbolOptions": symbol_options,
        "ranking": _frame_records(ranking, limit=20),
        "missingRates": _frame_records(missing_rates, limit=15),
        "selectedRecord": _json_ready(selected_row.iloc[0].to_dict()) if not selected_row.empty else {},
    }


def get_factor_explorer_detail_payload(
    *,
    factor_name: str | None = None,
    symbol: str | None = None,
    history_factor: str | None = None,
) -> dict[str, Any]:
    snapshot_payload = build_factor_explorer_snapshot()
    factor_options = list(snapshot_payload.get("factorOptions", []) or [])
    factor_keys = [str(item.get("key", "")) for item in factor_options if str(item.get("key", ""))]
    selected_factor = factor_name or (factor_keys[min(11, len(factor_keys) - 1)] if factor_keys else "")
    selected_history_factor = history_factor or (factor_keys[min(3, len(factor_keys) - 1)] if factor_keys else "")

    cross_section = pd.DataFrame(snapshot_payload.get("crossSection", []) or [])
    symbol_options = list(snapshot_payload.get("symbolOptions", []) or [])
    selected_symbol = symbol or (symbol_options[0] if symbol_options else "")

    history = pd.DataFrame()
    if selected_symbol and selected_history_factor:
        history = (
            load_feature_history_for_symbol(selected_symbol, selected_history_factor)
            .dropna(subset=["trade_date", selected_history_factor])
            .sort_values("trade_date")
            .tail(240)
        )
    snapshot = build_latest_factor_snapshot(cross_section, symbol=selected_symbol, zh=lambda value: value)

    return {
        "selectedFactor": selected_factor,
        "selectedHistoryFactor": selected_history_factor,
        "selectedSymbol": selected_symbol,
        "history": _frame_records(history),
        "snapshot": _frame_records(snapshot),
    }


def get_factor_explorer_payload(
    *,
    factor_name: str | None = None,
    symbol: str | None = None,
    history_factor: str | None = None,
) -> dict[str, Any]:
    summary = get_factor_explorer_summary_payload(
        factor_name=factor_name,
        symbol=symbol,
        history_factor=history_factor,
    )
    detail = get_factor_explorer_detail_payload(
        factor_name=summary.get("selectedFactor"),
        symbol=summary.get("selectedSymbol"),
        history_factor=summary.get("selectedHistoryFactor"),
    )
    return {**summary, **detail}
