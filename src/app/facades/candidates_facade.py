from __future__ import annotations

import pandas as pd
from typing import Any

from src.app.facades.base import (
    _json_ready, 
    _frame_records,
    _project_record_fields,
)
from src.app.services.dashboard_data_service import (
    build_candidate_snapshot,
    load_prediction_history_for_symbol,
)
from src.app.viewmodels.candidates_vm import build_candidate_score_history


CANDIDATE_SUMMARY_FIELDS = [
    "ts_code",
    "name",
    "industry",
    "rank",
    "score",
    "rank_pct",
    "ret_t1_t10",
    "action_hint",
    "trade_date",
]


def get_candidates_summary_payload(
    *,
    model_name: str = "lgbm",
    split_name: str = "test",
    top_n: int = 30,
    page: int = 1,
    symbol: str | None = None,
) -> dict[str, Any]:
    candidate_snapshot = build_candidate_snapshot(model_name, split_name)

    latest_picks = pd.DataFrame()
    latest_date = None
    symbol_options: list[str] = []
    selected_symbol = symbol or ""
    total_count = 0
    page_size = max(1, int(top_n))
    normalized_page = max(1, int(page))
    total_pages = 0
    if candidate_snapshot is not None and not candidate_snapshot.empty:
        latest_date = candidate_snapshot["trade_date"].iloc[0] if "trade_date" in candidate_snapshot.columns else None
        total_count = int(len(candidate_snapshot))
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        normalized_page = min(normalized_page, total_pages)
        page_start = (normalized_page - 1) * page_size
        page_end = page_start + page_size
        latest_picks = candidate_snapshot.iloc[page_start:page_end].copy()
        symbol_options = candidate_snapshot["ts_code"].astype(str).tolist()

    selected = candidate_snapshot.loc[candidate_snapshot["ts_code"].astype(str) == selected_symbol].head(1) if candidate_snapshot is not None and not candidate_snapshot.empty and selected_symbol else pd.DataFrame()
    selected_record = _project_record_fields(selected.iloc[0].to_dict(), CANDIDATE_SUMMARY_FIELDS) if not selected.empty else {}
    summary_columns = [column for column in CANDIDATE_SUMMARY_FIELDS if column in latest_picks.columns]

    return {
        "modelName": model_name,
        "splitName": split_name,
        "topN": page_size,
        "page": normalized_page,
        "pageSize": page_size,
        "totalCount": total_count,
        "totalPages": total_pages,
        "latestDate": _json_ready(latest_date),
        "selectedSymbol": selected_symbol,
        "symbolOptions": symbol_options,
        "latestPicks": _frame_records(latest_picks[summary_columns].copy()) if summary_columns else [],
        "selectedRecord": _json_ready(selected_record),
    }


def get_candidate_detail_payload(
    *,
    model_name: str = "lgbm",
    split_name: str = "test",
    symbol: str | None = None,
) -> dict[str, Any]:
    candidate_snapshot = build_candidate_snapshot(model_name, split_name)
    selected_symbol = str(symbol or "").strip()
    if not selected_symbol and candidate_snapshot is not None and not candidate_snapshot.empty and "ts_code" in candidate_snapshot.columns:
        selected_symbol = str(candidate_snapshot.iloc[0]["ts_code"])

    selected = candidate_snapshot.loc[candidate_snapshot["ts_code"].astype(str) == selected_symbol].head(1) if selected_symbol and candidate_snapshot is not None and not candidate_snapshot.empty else pd.DataFrame()
    selected_record = selected.iloc[0].to_dict() if not selected.empty else {}
    field_rows = [{"field": key, "value": _json_ready(value)} for key, value in selected_record.items()]
    return {
        "modelName": model_name,
        "splitName": split_name,
        "selectedSymbol": selected_symbol,
        "selectedRecord": _json_ready(selected_record),
        "fieldRows": field_rows,
    }


def get_candidate_history_payload(
    *,
    model_name: str = "lgbm",
    split_name: str = "test",
    symbol: str | None = None,
) -> dict[str, Any]:
    selected_symbol = symbol or ""
    if not selected_symbol:
        candidate_snapshot = build_candidate_snapshot(model_name, split_name)
        if not candidate_snapshot.empty and "ts_code" in candidate_snapshot.columns:
            selected_symbol = str(candidate_snapshot.iloc[0]["ts_code"])

    score_history = pd.DataFrame()
    if selected_symbol:
        predictions = load_prediction_history_for_symbol(model_name, split_name, selected_symbol)
        if not predictions.empty:
            score_history = build_candidate_score_history(predictions, symbol=selected_symbol).reset_index()
    return {
        "modelName": model_name,
        "splitName": split_name,
        "selectedSymbol": selected_symbol,
        "scoreHistory": _frame_records(score_history),
    }


def get_candidates_payload(
    *,
    model_name: str = "lgbm",
    split_name: str = "test",
    top_n: int = 30,
    page: int = 1,
    symbol: str | None = None,
) -> dict[str, Any]:
    summary = get_candidates_summary_payload(
        model_name=model_name,
        split_name=split_name,
        top_n=top_n,
        page=page,
        symbol=symbol,
    )
    history = get_candidate_history_payload(
        model_name=model_name,
        split_name=split_name,
        symbol=str(summary.get("selectedSymbol", "") or ""),
    )
    return {
        **summary,
        "scoreHistory": history.get("scoreHistory", []),
    }
