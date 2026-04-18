from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.app.repositories.report_repository import (
    load_overlay_candidates,
    load_overlay_inference_candidates,
    load_overlay_inference_packet,
    load_overlay_llm_bundle,
    load_overlay_packet,
)
from src.utils.io import project_root

ROUND_SPECS = (
    {
        "round_key": "historical_verified",
        "round_label": "历史验证研讨",
        "packet_filename": "overlay_latest_packet.json",
        "candidates_filename": "overlay_latest_candidates.csv",
    },
    {
        "round_key": "latest_inference",
        "round_label": "最新推理研讨",
        "packet_filename": "overlay_inference_packet.json",
        "candidates_filename": "overlay_inference_candidates.csv",
    },
)

STATUS_LABELS = {
    "success": "已完成",
    "error": "执行出错",
    "executed": "已执行",
    "executed_with_errors": "部分成功",
    "export_only": "已导出未执行",
    "configuration_incomplete": "配置未完成",
    "execution_failed": "执行失败",
    "candidate_pool_only": "仅在候选池",
    "not_in_pool": "未入池",
    "not_available": "不可用",
    "not_found": "无响应",
}


def _candidate_pool_lookup(frame: pd.DataFrame) -> tuple[dict[str, int], int]:
    if frame.empty or "ts_code" not in frame.columns:
        return {}, 0
    frame = frame.reset_index(drop=True)
    lookup = {str(ts_code): int(index + 1) for index, ts_code in enumerate(frame["ts_code"].astype(str))}
    return lookup, int(len(frame))


def _selected_candidate_lookup(packet: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for candidate in packet.get("selected_candidates", []) or []:
        symbol = str(candidate.get("ts_code", "") or "").strip()
        if symbol:
            lookup[symbol] = candidate
    return lookup


def _truncate_text(text: str, limit: int = 220) -> str:
    cleaned = " ".join(str(text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _prefer_database(root: Path) -> bool:
    try:
        return root.resolve() == project_root().resolve()
    except OSError:
        return False


def _round_status_label(round_info: dict[str, Any]) -> str:
    response_status = str(round_info.get("response_status", "") or "").strip()
    if response_status in STATUS_LABELS:
        return STATUS_LABELS[response_status]
    execution_status = str(round_info.get("execution_status", "") or "").strip()
    return STATUS_LABELS.get(execution_status, execution_status or "未知")


def _round_summary_text(round_info: dict[str, Any]) -> str:
    if round_info.get("response_text"):
        return str(round_info["response_text"])
    if round_info.get("blocking_reason"):
        return str(round_info["blocking_reason"])
    if round_info.get("candidate_note"):
        return str(round_info["candidate_note"])
    return "当前轮次暂无外部模型结论。"


def _build_round_info(
    *,
    spec: dict[str, str],
    packet: dict[str, Any],
    candidate_pool_rank: int | None,
    candidate_pool_size: int,
    selected_candidate: dict[str, Any] | None,
    response_record: dict[str, Any] | None,
) -> dict[str, Any]:
    llm_bridge = packet.get("llm_bridge", {}) or {}
    latest_date = packet.get("latest_date")
    top_n = int(packet.get("top_n", 0) or 0)
    selected_for_llm = selected_candidate is not None
    in_candidate_pool = candidate_pool_rank is not None

    if response_record is not None:
        response_status = str(response_record.get("status", "") or "").strip() or "not_found"
        execution_status = str(llm_bridge.get("execution_status", "") or response_status)
        response_text = str(response_record.get("output_text", "") or "").strip()
        blocking_reason = str(response_record.get("error", "") or "").strip()
        candidate_note = ""
    elif selected_for_llm:
        execution_status = str(llm_bridge.get("execution_status", "") or "not_available")
        response_status = execution_status
        response_text = ""
        blocking_reason = str(llm_bridge.get("blocking_reason", "") or "").strip()
        candidate_note = ""
    elif in_candidate_pool:
        execution_status = "candidate_pool_only"
        response_status = "candidate_pool_only"
        response_text = ""
        blocking_reason = ""
        candidate_note = f"已进入{spec['round_label']}候选池第 {candidate_pool_rank}/{candidate_pool_size} 位，但未进入前 {top_n} 名自动研讨名单。"
    else:
        execution_status = "not_in_pool"
        response_status = "not_in_pool"
        response_text = ""
        blocking_reason = ""
        candidate_note = f"当前未进入{spec['round_label']}候选池。"

    round_info = {
        "round_key": spec["round_key"],
        "round_label": spec["round_label"],
        "latest_date": latest_date,
        "top_n": top_n,
        "candidate_pool_rank": candidate_pool_rank,
        "candidate_pool_size": candidate_pool_size,
        "in_candidate_pool": in_candidate_pool,
        "selected_for_llm": selected_for_llm,
        "execution_status": execution_status,
        "response_status": response_status,
        "display_status": STATUS_LABELS.get(response_status, STATUS_LABELS.get(execution_status, execution_status)),
        "provider": str(llm_bridge.get("provider", "") or ""),
        "model": str(llm_bridge.get("model", "") or ""),
        "blocking_reason": blocking_reason,
        "response_text": response_text,
        "candidate_note": candidate_note,
        "action_hint": str((selected_candidate or {}).get("action_hint", "") or ""),
        "thesis_summary": str((selected_candidate or {}).get("thesis_summary", "") or ""),
        "ai_brief": str((selected_candidate or {}).get("ai_brief", "") or ""),
    }
    round_info["summary_text"] = _round_summary_text(round_info)
    round_info["summary_preview"] = _truncate_text(round_info["summary_text"])
    return round_info


def load_symbol_discussion_snapshot(root: Path, data_source: str, symbol: str) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").strip()
    rounds: list[dict[str, Any]] = []

    for spec in ROUND_SPECS:
        if spec["round_key"] == "latest_inference":
            packet = load_overlay_inference_packet(root, data_source=data_source, prefer_database=_prefer_database(root))
            candidates = load_overlay_inference_candidates(root, data_source=data_source, prefer_database=_prefer_database(root))
            llm_bundle = load_overlay_llm_bundle(
                root,
                data_source=data_source,
                scope="inference",
                packet=packet,
                prefer_database=_prefer_database(root),
            )
        else:
            packet = load_overlay_packet(root, data_source=data_source, prefer_database=_prefer_database(root))
            candidates = load_overlay_candidates(root, data_source=data_source, prefer_database=_prefer_database(root))
            llm_bundle = load_overlay_llm_bundle(
                root,
                data_source=data_source,
                scope="historical",
                packet=packet,
                prefer_database=_prefer_database(root),
            )

        if not packet and candidates.empty:
            continue

        candidate_lookup, candidate_pool_size = _candidate_pool_lookup(candidates)
        selected_lookup = _selected_candidate_lookup(packet)
        response_lookup = dict(llm_bundle.get("response_lookup", {}) or {})
        round_info = _build_round_info(
            spec=spec,
            packet=packet,
            candidate_pool_rank=candidate_lookup.get(normalized_symbol),
            candidate_pool_size=candidate_pool_size,
            selected_candidate=selected_lookup.get(normalized_symbol),
            response_record=response_lookup.get(normalized_symbol),
        )
        rounds.append(round_info)

    rounds = sorted(
        rounds,
        key=lambda item: pd.Timestamp(item["latest_date"]) if item.get("latest_date") else pd.Timestamp.min,
    )
    successful_rounds = [round_info for round_info in rounds if round_info.get("response_status") == "success"]
    selected_rounds = [round_info for round_info in rounds if round_info.get("selected_for_llm")]
    candidate_rounds = [round_info for round_info in rounds if round_info.get("in_candidate_pool")]
    latest_round = rounds[-1] if rounds else {}

    if successful_rounds:
        overview = f"已拿到 {len(successful_rounds)} 轮外部模型研讨结果，最近一轮是“{latest_round.get('round_label', '-') }”。"
    elif selected_rounds:
        overview = f"已进入 {len(selected_rounds)} 轮自动研讨名单，但最近状态是“{latest_round.get('display_status', '-') }”。"
    elif candidate_rounds:
        overview = f"当前只进入候选池，尚未进入自动研讨名单；最近一轮是“{latest_round.get('round_label', '-') }”。"
    elif rounds:
        overview = "当前未进入历史验证或最新推理的外部模型研讨候选池。"
    else:
        overview = "当前还没有可用的外部模型研讨产物。"

    latest_summary = latest_round.get("summary_preview", "") if latest_round else ""
    return {
        "symbol": normalized_symbol,
        "rounds": rounds,
        "round_count": len(rounds),
        "selected_round_count": len(selected_rounds),
        "candidate_round_count": len(candidate_rounds),
        "success_round_count": len(successful_rounds),
        "latest_round_label": str(latest_round.get("round_label", "") or ""),
        "latest_status": str(latest_round.get("display_status", "") or ""),
        "latest_summary": str(latest_summary or ""),
        "overview": overview,
    }


def discussion_round_rows(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for round_info in snapshot.get("rounds", []) or []:
        rows.append(
            {
                "轮次": str(round_info.get("round_label", "") or ""),
                "截面日期": str(round_info.get("latest_date", "") or "-"),
                "状态": _round_status_label(round_info),
                "摘要": str(round_info.get("summary_preview", "") or ""),
            }
        )
    return rows
