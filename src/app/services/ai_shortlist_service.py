from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.app.repositories.report_repository import save_text_report
from src.utils.io import project_root


def _clean_text(value: str) -> str:
    return " ".join(str(value or "").split())


def _extract_trader_note(response_text: str, thesis_summary: str) -> str:
    paragraphs = [segment.strip() for segment in str(response_text or "").split("\n\n") if segment.strip()]
    for paragraph in paragraphs:
        cleaned = _clean_text(paragraph)
        if not cleaned:
            continue
        if cleaned.startswith("#") or cleaned.startswith("##") or cleaned.startswith("###"):
            continue
        if cleaned.startswith("---"):
            continue
        return cleaned

    cleaned_thesis = _clean_text(thesis_summary)
    return cleaned_thesis or "暂无最新 AI 结论。"


def build_trader_shortlist_rows(
    *,
    packet: dict[str, Any],
    response_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, candidate in enumerate(packet.get("selected_candidates", []) or [], start=1):
        ts_code = str(candidate.get("ts_code", "") or "").strip()
        response_record = response_lookup.get(ts_code, {})
        response_text = str(response_record.get("output_text", "") or "")
        thesis_summary = str(candidate.get("thesis_summary", "") or "")
        rows.append(
            {
                "rank": index,
                "ts_code": ts_code,
                "name": str(candidate.get("name", "") or ts_code),
                "action_hint": str(candidate.get("action_hint", "") or ""),
                "confidence_level": str(candidate.get("confidence_level", "") or ""),
                "final_score": candidate.get("final_score"),
                "trader_note": _extract_trader_note(response_text, thesis_summary),
            }
        )
    return rows


def build_trader_shortlist_markdown(
    *,
    data_source: str,
    latest_date: str,
    rows: list[dict[str, Any]],
) -> str:
    lines = [
        f"# AI 交易员 Shortlist（{data_source} / {latest_date}）",
        "",
        f"- 最新推理日期：{latest_date}",
        f"- 入选数量：{len(rows)}",
        "",
    ]

    for row in rows:
        lines.extend(
            [
                f"## {row['rank']}. {row['ts_code']} {row['name']}",
                f"- 定位：{row.get('action_hint') or '-'} / 置信度 {row.get('confidence_level') or '-'} / 综合分 {float(row.get('final_score') or 0.0):.2f}",
                f"- 交易员备注：{row.get('trader_note') or '暂无最新 AI 结论。'}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def save_overlay_inference_shortlist(
    *,
    packet: dict[str, Any],
    response_lookup: dict[str, dict[str, Any]],
    root: Path | None = None,
    data_source: str,
    user_id: str | None = None,
) -> dict[str, Any]:
    rows = build_trader_shortlist_rows(packet=packet, response_lookup=response_lookup)
    markdown = build_trader_shortlist_markdown(
        data_source=data_source,
        latest_date=str(packet.get("latest_date", "") or "-"),
        rows=rows,
    )
    artifact_ref = save_text_report(
        root=root or project_root(),
        data_source=data_source,
        filename="overlay_inference_shortlist.md",
        content=markdown,
        artifact_name="overlay_inference_shortlist",
        user_id=user_id,
    )
    return {
        "rows": rows,
        "markdown": markdown,
        "artifact_ref": str(artifact_ref),
    }
