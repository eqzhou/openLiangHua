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
    load_watchlist_config,
    load_overlay_candidate_record,
    load_overlay_candidate_summary_records,
    load_overlay_llm_bundle,
    load_overlay_inference_packet,
    load_overlay_packet,
    load_overlay_inference_brief,
    load_overlay_brief,
)


def _watchlist_relation_lookup(user_id: str | None) -> dict[str, str]:
    watchlist = load_watchlist_config(user_id=user_id)
    lookup: dict[str, str] = {}
    for item in watchlist.get("focus_pool", []) or []:
        symbol = str(item.get("ts_code", "") or "").strip()
        if symbol:
            lookup[symbol] = "focus"
    for item in watchlist.get("holdings", []) or []:
        symbol = str(item.get("ts_code", "") or "").strip()
        if symbol:
            lookup[symbol] = "holding"
    return lookup


def _relation_label(relation: str) -> str:
    return {
        "holding": "持仓",
        "focus": "重点关注",
        "untracked": "未跟踪",
    }.get(relation, "未跟踪")


def _annotate_watchlist_relation(record: dict[str, Any], relation_lookup: dict[str, str]) -> dict[str, Any]:
    symbol = str(record.get("ts_code", "") or "").strip()
    relation = relation_lookup.get(symbol, "untracked")
    return {
        **record,
        "watchlist_relation": relation,
        "watchlist_relation_label": _relation_label(relation),
        "is_current_holding": relation == "holding",
        "is_focus_pool": relation == "focus",
        "is_watchlist_tracked": relation in {"holding", "focus"},
    }


def _annotate_candidate_rows(records: list[dict[str, Any]], relation_lookup: dict[str, str]) -> list[dict[str, Any]]:
    return [_annotate_watchlist_relation(dict(record), relation_lookup) for record in records]


def _artifact_user_id(scope: str, user_id: str | None) -> str | None:
    return user_id if scope == "inference" else None


def _build_ai_panel_payload(
    *,
    scope: str,
    candidates: pd.DataFrame,
    packet: dict[str, Any],
    brief: str,
    selected_symbol: str | None,
    user_id: str | None = None,
) -> dict[str, Any]:
    relation_lookup = _watchlist_relation_lookup(user_id)
    candidates = pd.DataFrame(_annotate_candidate_rows(_frame_records(candidates), relation_lookup))
    llm_bundle = load_overlay_llm_bundle(scope, user_id=_artifact_user_id(scope, user_id))
    response_lookup = dict(llm_bundle.get("response_lookup", {}) or {})
    response_summary = str(llm_bundle.get("response_summary", "") or "")
    has_symbol_column = "ts_code" in candidates.columns
    symbol = selected_symbol or (str(candidates.iloc[0]["ts_code"]) if has_symbol_column and not candidates.empty else "")
    selected = candidates.loc[candidates["ts_code"].astype(str) == symbol].head(1) if has_symbol_column and symbol else pd.DataFrame()
    selected_record = _annotate_watchlist_relation(selected.iloc[0].to_dict(), relation_lookup) if not selected.empty else {}
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
    user_id: str | None = None,
) -> dict[str, Any]:
    full_payload = _build_ai_panel_payload(
        scope=scope,
        candidates=candidates,
        packet=packet,
        brief=brief,
        selected_symbol=selected_symbol,
        user_id=user_id,
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


def _build_ai_review_summary_panel_payload(
    *,
    scope: str,
    selected_symbol: str | None,
    user_id: str | None = None,
) -> dict[str, Any]:
    relation_lookup = _watchlist_relation_lookup(user_id)
    candidate_records = _annotate_candidate_rows(
        [
            dict(record)
            for record in load_overlay_candidate_summary_records(
                scope,
                AI_REVIEW_SUMMARY_CANDIDATE_FIELDS,
                user_id=_artifact_user_id(scope, user_id),
            )
        ],
        relation_lookup,
    )
    candidates = pd.DataFrame(candidate_records)
    symbol = str(selected_symbol or "").strip()
    selected_record = (
        load_overlay_candidate_record(
            scope,
            symbol,
            AI_REVIEW_SUMMARY_SELECTED_FIELDS,
            user_id=_artifact_user_id(scope, user_id),
        )
        if symbol
        else {}
    )
    if selected_record:
        selected_record = _annotate_watchlist_relation(dict(selected_record), relation_lookup)
    return {
        "selectedSymbol": symbol,
        "candidateCount": len(candidate_records),
        "candidates": _json_ready(candidate_records),
        "selectedRecord": _json_ready(selected_record),
    }


def _build_ai_review_detail_panel_payload(
    *,
    scope: str,
    selected_symbol: str | None,
    user_id: str | None = None,
) -> dict[str, Any]:
    relation_lookup = _watchlist_relation_lookup(user_id)
    summary_panel = _build_ai_review_summary_panel_payload(
        scope=scope,
        selected_symbol=selected_symbol,
        user_id=user_id,
    )
    resolved_symbol = str(summary_panel.get("selectedSymbol", "") or "")
    selected_record = (
        load_overlay_candidate_record(scope, resolved_symbol, user_id=_artifact_user_id(scope, user_id))
        if resolved_symbol
        else {}
    )
    if selected_record:
        selected_record = _annotate_watchlist_relation(dict(selected_record), relation_lookup)
    packet = load_overlay_inference_packet(user_id=user_id) if scope == "inference" else load_overlay_packet()
    brief = load_overlay_inference_brief(user_id=user_id) if scope == "inference" else load_overlay_brief()
    llm_bundle = load_overlay_llm_bundle(scope, user_id=_artifact_user_id(scope, user_id))
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
    user_id: str | None = None,
) -> dict[str, Any]:
    return {
        "inference": _build_ai_review_summary_panel_payload(
            scope="inference",
            selected_symbol=inference_symbol,
            user_id=user_id,
        ),
        "historical": _build_ai_review_summary_panel_payload(
            scope="historical",
            selected_symbol=historical_symbol,
            user_id=user_id,
        ),
    }


def get_ai_review_detail_payload(
    *,
    scope: str,
    symbol: str | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    normalized_scope = "inference" if scope == "inference" else "historical"
    detail_payload = _build_ai_review_detail_panel_payload(
        scope=normalized_scope,
        selected_symbol=symbol,
        user_id=user_id,
    )
    detail_payload.pop("packet", None)
    return detail_payload


def get_ai_review_payload(
    *,
    inference_symbol: str | None = None,
    historical_symbol: str | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    inference_summary = _build_ai_review_summary_panel_payload(
        scope="inference",
        selected_symbol=inference_symbol,
        user_id=user_id,
    )
    historical_summary = _build_ai_review_summary_panel_payload(
        scope="historical",
        selected_symbol=historical_symbol,
        user_id=user_id,
    )
    inference_detail = _build_ai_review_detail_panel_payload(
        scope="inference",
        selected_symbol=str(inference_summary.get("selectedSymbol", "") or ""),
        user_id=user_id,
    )
    historical_detail = _build_ai_review_detail_panel_payload(
        scope="historical",
        selected_symbol=str(historical_summary.get("selectedSymbol", "") or ""),
        user_id=user_id,
    )
    return {
        "inference": {**inference_summary, **inference_detail},
        "historical": {**historical_summary, **historical_detail},
    }
