from __future__ import annotations

import pandas as pd
from typing import Any

from src.app.facades.base import (
    _json_ready, 
    _frame_records,
    AI_REVIEW_SUMMARY_CANDIDATE_FIELDS,
    AI_REVIEW_SUMMARY_SELECTED_FIELDS,
)
from src.app.services.dashboard_data_service import (
    load_overlay_candidate_record,
    load_overlay_candidate_summary_records,
    load_overlay_llm_bundle,
    load_overlay_inference_packet,
    load_overlay_packet,
    load_overlay_inference_brief,
    load_overlay_brief,
)


def _build_ai_panel_payload(
    *,
    scope: str,
    candidates: pd.DataFrame,
    packet: dict[str, Any],
    brief: str,
    selected_symbol: str | None,
) -> dict[str, Any]:
    llm_bundle = load_overlay_llm_bundle(scope)
    response_lookup = dict(llm_bundle.get("response_lookup", {}) or {})
    response_summary = str(llm_bundle.get("response_summary", "") or "")
    has_symbol_column = "ts_code" in candidates.columns
    symbol = selected_symbol or (str(candidates.iloc[0]["ts_code"]) if has_symbol_column and not candidates.empty else "")
    selected = candidates.loc[candidates["ts_code"].astype(str) == symbol].head(1) if has_symbol_column and symbol else pd.DataFrame()
    selected_record = selected.iloc[0].to_dict() if not selected.empty else {}
    llm_response = response_lookup.get(symbol, {})
    return {
        "selectedSymbol": symbol,
        "candidates": _frame_records(candidates),
        "packet": _json_ready(packet),
        "brief": brief,
        "selectedRecord": _json_ready(selected_record),
        "llmResponse": _json_ready(llm_response),
        "responseSummary": response_summary,
    }


def _build_ai_panel_summary_payload(
    *,
    candidates: pd.DataFrame,
    selected_symbol: str | None,
) -> dict[str, Any]:
    has_symbol_column = "ts_code" in candidates.columns
    symbol = selected_symbol or (str(candidates.iloc[0]["ts_code"]) if has_symbol_column and not candidates.empty else "")
    selected = candidates.loc[candidates["ts_code"].astype(str) == symbol].head(1) if has_symbol_column and symbol else pd.DataFrame()
    selected_record = selected.iloc[0].to_dict() if not selected.empty else {}
    return {
        "selectedSymbol": symbol,
        "candidateCount": int(len(candidates)),
        "candidates": _frame_records(candidates),
        "selectedRecord": _json_ready(selected_record),
    }


def _build_ai_panel_detail_payload(
    *,
    scope: str,
    candidates: pd.DataFrame,
    packet: dict[str, Any],
    brief: str,
    selected_symbol: str | None,
) -> dict[str, Any]:
    full_payload = _build_ai_panel_payload(
        scope=scope,
        candidates=candidates,
        packet=packet,
        brief=brief,
        selected_symbol=selected_symbol,
    )
    selected_record = dict(full_payload.get("selectedRecord", {}) or {})
    field_rows = [
        {"field": key, "value": _json_ready(value)}
        for key, value in selected_record.items()
    ]
    return {
        "selectedSymbol": full_payload.get("selectedSymbol", ""),
        "selectedRecord": selected_record,
        "fieldRows": field_rows,
        "brief": str(full_payload.get("brief", "") or ""),
        "llmResponse": dict(full_payload.get("llmResponse", {}) or {}),
        "responseSummary": str(full_payload.get("responseSummary", "") or ""),
    }


def _build_ai_review_summary_panel_payload(*, scope: str, selected_symbol: str | None) -> dict[str, Any]:
    candidate_records = load_overlay_candidate_summary_records(scope, AI_REVIEW_SUMMARY_CANDIDATE_FIELDS)
    candidates = pd.DataFrame(candidate_records)
    symbol = str(selected_symbol or "").strip()
    selected_record = load_overlay_candidate_record(scope, symbol, AI_REVIEW_SUMMARY_SELECTED_FIELDS) if symbol else {}
    return {
        "selectedSymbol": symbol,
        "candidateCount": len(candidate_records),
        "candidates": _json_ready(candidate_records),
        "selectedRecord": _json_ready(selected_record),
    }


def _build_ai_review_detail_panel_payload(*, scope: str, selected_symbol: str | None) -> dict[str, Any]:
    summary_panel = _build_ai_review_summary_panel_payload(scope=scope, selected_symbol=selected_symbol)
    resolved_symbol = str(summary_panel.get("selectedSymbol", "") or "")
    selected_record = load_overlay_candidate_record(scope, resolved_symbol) if resolved_symbol else {}
    packet = load_overlay_inference_packet() if scope == "inference" else load_overlay_packet()
    brief = load_overlay_inference_brief() if scope == "inference" else load_overlay_brief()
    llm_bundle = load_overlay_llm_bundle(scope)
    response_lookup = dict(llm_bundle.get("response_lookup", {}) or {})
    field_rows = [{"field": key, "value": _json_ready(value)} for key, value in selected_record.items()]
    return {
        "selectedSymbol": resolved_symbol,
        "selectedRecord": _json_ready(selected_record),
        "fieldRows": field_rows,
        "brief": brief,
        "llmResponse": _json_ready(response_lookup.get(resolved_symbol, {})),
        "responseSummary": str(llm_bundle.get("response_summary", "") or ""),
        "packet": _json_ready(packet),
    }


def get_ai_review_summary_payload(
    *,
    inference_symbol: str | None = None,
    historical_symbol: str | None = None,
) -> dict[str, Any]:
    return {
        "inference": _build_ai_review_summary_panel_payload(scope="inference", selected_symbol=inference_symbol),
        "historical": _build_ai_review_summary_panel_payload(scope="historical", selected_symbol=historical_symbol),
    }


def get_ai_review_detail_payload(
    *,
    scope: str,
    symbol: str | None = None,
) -> dict[str, Any]:
    normalized_scope = "inference" if scope == "inference" else "historical"
    detail_payload = _build_ai_review_detail_panel_payload(scope=normalized_scope, selected_symbol=symbol)
    detail_payload.pop("packet", None)
    return detail_payload


def get_ai_review_payload(
    *,
    inference_symbol: str | None = None,
    historical_symbol: str | None = None,
) -> dict[str, Any]:
    inference_summary = _build_ai_review_summary_panel_payload(scope="inference", selected_symbol=inference_symbol)
    historical_summary = _build_ai_review_summary_panel_payload(scope="historical", selected_symbol=historical_symbol)
    inference_detail = _build_ai_review_detail_panel_payload(
        scope="inference",
        selected_symbol=str(inference_summary.get("selectedSymbol", "") or ""),
    )
    historical_detail = _build_ai_review_detail_panel_payload(
        scope="historical",
        selected_symbol=str(historical_summary.get("selectedSymbol", "") or ""),
    )
    return {
        "inference": {**inference_summary, **inference_detail},
        "historical": {**historical_summary, **historical_detail},
    }
