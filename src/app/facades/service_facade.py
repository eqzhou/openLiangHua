from __future__ import annotations

import copy
import pandas as pd
from typing import Any
from functools import lru_cache

from src.app.facades.base import (
    _json_ready, 
    _watchlist_entry_count,
    _clean_config_summary_text,
)
from src.app.services.streamlit_runtime_service import get_streamlit_service_status
from src.app.services.dashboard_data_service import (
    ROOT,
    load_dataset_summary,
    load_watchlist_config,
    load_experiment_config,
    save_experiment_config,
)
from src.db.realtime_quote_store import get_realtime_quote_store


def _realtime_snapshot_label(snapshot_bucket: str, *, is_today: bool, available: bool) -> str:
    if not available:
        return "暂无快照"
    if snapshot_bucket == "post_close":
        return "今日盘后快照" if is_today else "盘后快照"
    if snapshot_bucket == "latest":
        return "最新盘中快照" if is_today else "历史盘中快照"
    return "数据库快照"


def _latest_market_reference_date() -> pd.Timestamp | None:
    dataset_summary = load_dataset_summary()
    latest_trade_date = dataset_summary.get("date_max")
    if not latest_trade_date:
        return None
    normalized_trade_date = pd.Timestamp(latest_trade_date)
    if pd.isna(normalized_trade_date):
        return None
    return normalized_trade_date.normalize()


def _is_post_close_like_snapshot(
    *,
    snapshot_bucket: str,
    trade_date: pd.Timestamp | None,
    fetched_at: pd.Timestamp | None,
) -> bool:
    if snapshot_bucket == "post_close":
        return True
    if snapshot_bucket != "latest" or trade_date is None or fetched_at is None:
        return False

    normalized_trade_date = pd.Timestamp(trade_date).date()
    fetched_timestamp = pd.Timestamp(fetched_at)
    if fetched_timestamp.date() < normalized_trade_date:
        return False
    return (fetched_timestamp.hour, fetched_timestamp.minute, fetched_timestamp.second) >= (15, 0, 0)


def _resolve_realtime_snapshot_display(
    *,
    snapshot_bucket: str,
    trade_date: pd.Timestamp | None,
    fetched_at: pd.Timestamp | None,
    available: bool,
) -> tuple[str, bool, bool]:
    if trade_date is None:
        return _realtime_snapshot_label(snapshot_bucket, is_today=False, available=available), False, False

    normalized_trade_date = pd.Timestamp(trade_date).date()
    today = pd.Timestamp.now(tz="Asia/Shanghai").date()
    market_reference_date = _latest_market_reference_date()
    is_today = normalized_trade_date == today
    is_current_market_day = market_reference_date is not None and normalized_trade_date == market_reference_date.date()
    is_post_close_like = _is_post_close_like_snapshot(
        snapshot_bucket=snapshot_bucket,
        trade_date=trade_date,
        fetched_at=fetched_at,
    )

    if not available:
        return "暂无快照", is_today, bool(is_current_market_day)
    if is_post_close_like:
        if is_today:
            return "今日盘后快照", is_today, bool(is_current_market_day)
        if is_current_market_day:
            return "最近交易日盘后快照", is_today, True
        return "盘后快照", is_today, False
    if snapshot_bucket == "latest":
        if is_today:
            return "最新盘中快照", is_today, bool(is_current_market_day)
        if is_current_market_day:
            return "最近交易日盘中快照", is_today, True
        return "历史盘中快照", is_today, False
    return "数据库快照", is_today, bool(is_current_market_day)


def _get_realtime_snapshot_summary() -> dict[str, Any]:
    summary: dict[str, Any] = {
        "available": False,
        "trade_date": "",
        "snapshot_bucket": "",
        "snapshot_label_display": "暂无快照",
        "source": "",
        "served_from": "",
        "requested_symbol_count": 0,
        "success_symbol_count": 0,
        "failed_symbols": [],
        "error_message": "",
        "fetched_at": "",
        "is_today": False,
        "is_current_market_day": False,
        "age_days": None,
    }
    try:
        latest = get_realtime_quote_store().get_latest_snapshot_summary()
    except Exception as exc:  # pragma: no cover - defensive path
        summary["error_message"] = f"读取实时快照失败：{exc}"
        summary["snapshot_label_display"] = "快照读取失败"
        return summary

    if not latest:
        return summary

    trade_date_value = latest.get("trade_date")
    fetched_at_value = latest.get("fetched_at")
    trade_date = pd.Timestamp(trade_date_value).date() if trade_date_value else None
    current_day = pd.Timestamp.now(tz="Asia/Shanghai").date()
    age_days = (current_day - trade_date).days if trade_date is not None else None
    snapshot_bucket = str(latest.get("snapshot_bucket", ""))
    snapshot_label, is_today, is_current_market_day = _resolve_realtime_snapshot_display(
        snapshot_bucket=snapshot_bucket,
        trade_date=pd.Timestamp(trade_date) if trade_date is not None else None,
        fetched_at=pd.Timestamp(fetched_at_value) if fetched_at_value else None,
        available=True,
    )

    summary.update(latest)
    summary["available"] = True
    summary["trade_date"] = str(trade_date) if trade_date is not None else ""
    summary["is_today"] = is_today
    summary["is_current_market_day"] = is_current_market_day
    summary["age_days"] = age_days
    summary["snapshot_label_display"] = snapshot_label
    summary["served_from"] = "database"
    return _json_ready(summary)


def get_shell_payload(user_id: str | None = None) -> dict[str, Any]:
    from src.app.facades.base import get_bootstrap_payload
    experiment_config = get_experiment_config_payload()
    watchlist_config = load_watchlist_config(user_id=user_id)
    return {
        "bootstrap": get_bootstrap_payload(),
        "experimentConfig": experiment_config,
        "service": get_service_payload(),
        "watchlistEntryCount": _watchlist_entry_count(watchlist_config),
        "configSummaryText": _clean_config_summary_text(experiment_config),
    }


@lru_cache(maxsize=4)
def _get_service_payload_cached(cache_bucket: int) -> dict[str, Any]:
    payload = _json_ready(get_streamlit_service_status(ROOT))
    payload["realtime_snapshot"] = _get_realtime_snapshot_summary()
    payload["cache_bucket"] = cache_bucket
    return payload


def get_service_payload() -> dict[str, Any]:
    cache_bucket = int(pd.Timestamp.now(tz="Asia/Shanghai").timestamp() // 15)
    payload = copy.deepcopy(_get_service_payload_cached(cache_bucket))
    payload.pop("cache_bucket", None)
    return payload


def refresh_realtime_payload(user_id: str | None = None) -> dict[str, Any]:
    from src.app.facades.watchlist_facade import _refresh_watchlist_realtime_snapshot
    realtime_quotes, realtime_status, symbols, _ = _refresh_watchlist_realtime_snapshot(user_id=user_id)
    return {
        "ok": bool(realtime_status.get("available")) or not symbols,
        "symbolCount": int(len(symbols)),
        "realtimeRecordCount": int(len(realtime_quotes)),
        "realtimeStatus": _json_ready(realtime_status),
    }


def get_experiment_config_payload() -> dict[str, Any]:
    return _json_ready(load_experiment_config())


def update_experiment_config_payload(payload: dict[str, Any]) -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    save_experiment_config(payload)
    clear_dashboard_caches()
    return get_experiment_config_payload()
