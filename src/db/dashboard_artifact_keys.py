from __future__ import annotations


def config_artifact_key(name: str) -> str:
    return f"config:{name}"


def json_artifact_key(data_source: str, name: str) -> str:
    return f"{data_source}:json:{name}"


def table_artifact_key(data_source: str, name: str) -> str:
    return f"{data_source}:table:{name}"


def watchlist_artifact_key(data_source: str) -> str:
    return table_artifact_key(data_source, "watchlist_snapshot")


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
