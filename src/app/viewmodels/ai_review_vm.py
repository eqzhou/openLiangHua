from __future__ import annotations


def build_llm_response_lookup(records: list[dict]) -> dict[str, dict]:
    return {
        str(record.get("custom_id", "")).strip(): record
        for record in records
        if str(record.get("custom_id", "")).strip()
    }
