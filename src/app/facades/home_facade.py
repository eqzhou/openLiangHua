from __future__ import annotations

import copy
import pandas as pd
from typing import Any

from src.app.facades.base import (
    _json_ready,
)
from src.app.services.dashboard_data_service import (
    load_overlay_inference_shortlist,
)

def _build_home_alerts(
    *,
    service_payload: dict[str, Any],
    watchlist_payload: dict[str, Any],
) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []

    effective_state = str(service_payload.get("effective_state", "") or "")
    service_label = str(service_payload.get("status_label_display", "未知") or "未知")
    if effective_state and effective_state != "running" and service_label != "状态脚本不可用":
        alerts.append(
            {
                "tone": "warn",
                "title": "页面服务需要关注",
                "detail": f"当前页面服务状态为“{service_label}”，白天优先检查服务页和日志。",
            }
        )

    realtime_snapshot = dict(service_payload.get("realtime_snapshot", {}) or {})
    snapshot_label = str(realtime_snapshot.get("snapshot_label_display", "暂无快照") or "暂无快照")
    if not bool(realtime_snapshot.get("available")):
        alerts.append(
            {
                "tone": "warn",
                "title": "还没有可用行情快照",
                "detail": "首页会先展示落库数据。盘中需要更实时的价格时，再去持仓页手动刷新行情。",
            }
        )
    elif not bool(realtime_snapshot.get("is_current_market_day")):
        alerts.append(
            {
                "tone": "warn",
                "title": "当前不是今日快照",
                "detail": f"最新可复用的行情来源是“{snapshot_label}”，日期不是今天，白天请主动确认最新盘中价格。",
            }
        )

    watchlist_overview = dict(watchlist_payload.get("overview", {}) or {})
    unrealized = pd.to_numeric(watchlist_overview.get("unrealizedPnl"), errors="coerce")
    if pd.notna(unrealized) and float(unrealized) < 0:
        alerts.append(
            {
                "tone": "default",
                "title": "观察池当前处于浮亏区间",
                "detail": f"按参考价格统计，观察池浮动盈亏约为 {float(unrealized):.2f}。今天先看防守位和减仓节奏。",
            }
        )

    if not alerts:
        alerts.append(
            {
                "tone": "good",
                "title": "主操作链路当前正常",
                "detail": "服务状态、行情快照和观察池摘要都可用，可以直接从首页进入白天工作流。",
            }
        )
    return alerts


def _get_home_watchlist_payload(user_id: str | None = None) -> dict[str, Any]:
    from src.app.facades.watchlist_facade import get_watchlist_summary_payload
    payload = get_watchlist_summary_payload(
        keyword="",
        scope="all",
        sort_by="inference_rank",
        include_realtime=False,
        user_id=user_id,
    )
    records = list(payload.get("records", []) or [])[:6]
    return {
        "overview": _json_ready(payload.get("overview", {})),
        "realtimeStatus": _json_ready(payload.get("realtimeStatus", {})),
        "records": _json_ready(records),
    }


def _get_home_payload_cached(user_id: str | None = None) -> dict[str, Any]:
    return {
        **get_home_summary_payload(user_id=user_id),
        "watchlist": get_home_watchlist_section_payload(user_id=user_id),
        "candidates": get_home_candidates_section_payload(),
        "aiReview": get_home_ai_review_section_payload(user_id=user_id),
    }


def get_home_payload(user_id: str | None = None) -> dict[str, Any]:
    return copy.deepcopy(_get_home_payload_cached(user_id=user_id))


def get_home_summary_payload(user_id: str | None = None) -> dict[str, Any]:
    from src.app.facades.service_facade import get_shell_payload
    from src.app.facades.overview_facade import get_overview_summary_payload
    from src.app.facades.shared_utils_facade import _best_comparison_record
    
    shell_payload = get_shell_payload(user_id=user_id)
    overview_payload = get_overview_summary_payload("test")
    watchlist_payload = _get_home_watchlist_payload(user_id=user_id)
    comparison = list(overview_payload.get("comparison", []) or [])
    return {
        "configSummaryText": str(shell_payload.get("configSummaryText", "") or ""),
        "service": _json_ready(shell_payload.get("service", {})),
        "overview": {
            "selectedSplit": str(overview_payload.get("selectedSplit", "test") or "test"),
            "summary": _json_ready(overview_payload.get("summary", {})),
            "bestAnnualized": _best_comparison_record(comparison, "daily_portfolio_annualized_return", mode="max"),
            "bestSharpe": _best_comparison_record(comparison, "daily_portfolio_sharpe", mode="max"),
            "bestDrawdown": _best_comparison_record(comparison, "daily_portfolio_max_drawdown", mode="min"),
        },
        "alerts": _json_ready(
            _build_home_alerts(
                service_payload=dict(shell_payload.get("service", {}) or {}),
                watchlist_payload=watchlist_payload,
            )
        ),
    }


def get_home_watchlist_section_payload(user_id: str | None = None) -> dict[str, Any]:
    watchlist_payload = _get_home_watchlist_payload(user_id=user_id)
    watchlist_records = list(watchlist_payload.get("records", []) or [])[:6]
    focus_watch_record = dict(watchlist_records[0]) if watchlist_records else {}
    return {
        "overview": _json_ready(watchlist_payload.get("overview", {})),
        "realtimeStatus": _json_ready(watchlist_payload.get("realtimeStatus", {})),
        "records": _json_ready(watchlist_records),
        "focusRecord": _json_ready(focus_watch_record),
    }


def get_home_candidates_section_payload() -> dict[str, Any]:
    from src.app.facades.candidates_facade import get_candidates_summary_payload
    candidates_payload = get_candidates_summary_payload(model_name="ensemble", split_name="test", top_n=6)
    candidate_records = list(candidates_payload.get("latestPicks", []) or [])[:6]
    return {
        "modelName": str(candidates_payload.get("modelName", "ensemble") or "ensemble"),
        "splitName": str(candidates_payload.get("splitName", "test") or "test"),
        "latestDate": _json_ready(candidates_payload.get("latestDate")),
        "records": _json_ready(candidate_records),
        "focusRecord": _json_ready(dict(candidate_records[0]) if candidate_records else {}),
    }


def get_home_ai_review_section_payload(user_id: str | None = None) -> dict[str, Any]:
    from src.app.facades.ai_review_facade import get_ai_review_summary_payload
    ai_review_payload = get_ai_review_summary_payload(user_id=user_id)
    inference_records = list(dict(ai_review_payload.get("inference", {}) or {}).get("candidates", []) or [])[:6]
    historical_records = list(dict(ai_review_payload.get("historical", {}) or {}).get("candidates", []) or [])[:6]
    focus_candidate_record = (
        dict(inference_records[0])
        if inference_records
        else {}
    )
    return {
        "inferenceRecords": _json_ready(inference_records),
        "historicalRecords": _json_ready(historical_records),
        "focusRecord": _json_ready(focus_candidate_record),
        "shortlistMarkdown": load_overlay_inference_shortlist(user_id=user_id),
    }
