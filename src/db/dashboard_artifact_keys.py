from __future__ import annotations

import hashlib


def config_artifact_key(name: str) -> str:
    return f"config:{name}"


def json_artifact_key(data_source: str, name: str) -> str:
    return f"{data_source}:json:{name}"


def table_artifact_key(data_source: str, name: str) -> str:
    return f"{data_source}:table:{name}"


def _user_digest(user_id: str) -> str:
    return hashlib.sha256(str(user_id or "").strip().encode("utf-8")).hexdigest()[:16]


def user_json_artifact_key(data_source: str, name: str, user_id: str | None) -> str:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return json_artifact_key(data_source, name)
    return json_artifact_key(data_source, f"{name}:user:{_user_digest(normalized_user_id)}")


def user_table_artifact_key(data_source: str, name: str, user_id: str | None) -> str:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return table_artifact_key(data_source, name)
    return table_artifact_key(data_source, f"{name}:user:{_user_digest(normalized_user_id)}")


def user_text_artifact_key(data_source: str, name: str, user_id: str | None) -> str:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return text_artifact_key(data_source, name)
    return text_artifact_key(data_source, f"{name}:user:{_user_digest(normalized_user_id)}")


def watchlist_artifact_key(data_source: str) -> str:
    return table_artifact_key(data_source, "watchlist_snapshot")


def user_watchlist_artifact_key(data_source: str, user_id: str) -> str:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return watchlist_artifact_key(data_source)

    return user_table_artifact_key(data_source, "watchlist_snapshot", normalized_user_id)


def candidate_snapshot_artifact_key(data_source: str, model_name: str, split_name: str) -> str:
    return table_artifact_key(data_source, f"candidate_snapshot:{model_name}:{split_name}")


def factor_explorer_artifact_key(data_source: str) -> str:
    return json_artifact_key(data_source, "factor_explorer_snapshot")


def binary_artifact_key(data_source: str, name: str) -> str:
    return f"{data_source}:binary:{name}"


def text_artifact_key(data_source: str, name: str) -> str:
    return f"{data_source}:text:{name}"


def overlay_llm_responses_artifact_key(data_source: str, scope: str) -> str:
    prefix = "overlay_inference" if scope == "inference" else "overlay"
    return text_artifact_key(data_source, f"{prefix}_llm_responses")


def overlay_llm_response_summary_artifact_key(data_source: str, scope: str) -> str:
    prefix = "overlay_inference" if scope == "inference" else "overlay"
    return text_artifact_key(data_source, f"{prefix}_llm_response_summary")


def event_notice_cache_artifact_key(data_source: str, notice_date: str) -> str:
    return binary_artifact_key(data_source, f"event_notice:{notice_date}")


def event_research_cache_artifact_key(data_source: str, symbol_code: str) -> str:
    return binary_artifact_key(data_source, f"event_research:{symbol_code}")


def event_news_cache_artifact_key(data_source: str, symbol_code: str) -> str:
    return binary_artifact_key(data_source, f"event_news:{symbol_code}")


def llm_bridge_export_artifact_key(data_source: str, output_prefix: str, name: str) -> str:
    return text_artifact_key(data_source, f"{output_prefix}_{name}")


def note_artifact_key(data_source: str, symbol: str, note_kind: str) -> str:
    return f"{data_source}:note:{symbol}:{note_kind}"


def user_note_artifact_key(data_source: str, symbol: str, note_kind: str, user_id: str) -> str:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return note_artifact_key(data_source, symbol, note_kind)
    digest = hashlib.sha256(normalized_user_id.encode("utf-8")).hexdigest()[:16]
    return f"{data_source}:note:user:{digest}:{symbol}:{note_kind}"
