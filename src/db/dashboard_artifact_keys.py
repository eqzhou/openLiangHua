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


def note_artifact_key(data_source: str, symbol: str, note_kind: str) -> str:
    return f"{data_source}:note:{symbol}:{note_kind}"
