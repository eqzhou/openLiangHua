from __future__ import annotations

import pandas as pd
from typing import Any

from src.app.facades.base import (
    _json_ready, 
    _frame_records, 
    _project_record_fields,
    WATCHLIST_SUMMARY_RECORD_FIELDS,
)
from src.app.services.dashboard_data_service import (
    WATCH_SCOPE_MAP,
    WATCH_SORT_MAP,
    build_watchlist_base_frame,
    load_prediction_history_for_symbol,
    load_latest_symbol_markdown,
    load_watchlist_overview,
    load_watchlist_filtered_count,
    load_watchlist_record,
    load_watchlist_summary_records,
)
from src.app.services.realtime_quote_service import (
    fetch_managed_realtime_quotes,
    merge_realtime_quotes,
    merge_realtime_quote_records,
    merge_realtime_quote_record,
)
from src.db.realtime_quote_store import get_realtime_quote_store
from src.utils.llm_discussion import discussion_round_rows
from src.app.services.watchlist_service import build_reduce_plan, filtered_watchlist_view


def _watchlist_realtime_context(frame: pd.DataFrame) -> tuple[list[str], dict[str, float]]:
    if frame.empty or "ts_code" not in frame.columns:
        return [], {}
    previous_close_lookup: dict[str, float] = {}
    if "latest_bar_close" in frame.columns:
        for _, row in frame[["ts_code", "latest_bar_close"]].iterrows():
            latest_bar_close = pd.to_numeric(row["latest_bar_close"], errors="coerce")
            if pd.notna(latest_bar_close):
                previous_close_lookup[str(row["ts_code"])] = float(latest_bar_close)
    return frame["ts_code"].astype(str).tolist(), previous_close_lookup


def _watchlist_overview_payload(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "totalCount": int(len(frame)),
        "overlayCount": int(frame["is_overlay_selected"].fillna(False).sum()) if not frame.empty and "is_overlay_selected" in frame.columns else 0,
        "inferenceOverlayCount": int(frame["is_inference_overlay_selected"].fillna(False).sum()) if not frame.empty and "is_inference_overlay_selected" in frame.columns else 0,
        "marketValue": float(pd.to_numeric(frame["market_value"], errors="coerce").sum()) if not frame.empty and "market_value" in frame.columns else 0.0,
        "unrealizedPnl": float(pd.to_numeric(frame["unrealized_pnl"], errors="coerce").sum()) if not frame.empty and "unrealized_pnl" in frame.columns else 0.0,
    }


def _refresh_watchlist_realtime_snapshot(
    *,
    now: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], list[str], dict[str, float]]:
    watchlist_view = build_watchlist_base_frame()
    symbols, previous_close_lookup = _watchlist_realtime_context(watchlist_view)
    if not symbols:
        return pd.DataFrame(), _decorate_realtime_status(_empty_realtime_status()), symbols, previous_close_lookup

    realtime_quotes, realtime_status = fetch_managed_realtime_quotes(
        symbols,
        previous_close_lookup=previous_close_lookup,
        now=now or pd.Timestamp.now(tz="Asia/Shanghai"),
    )
    return realtime_quotes, _decorate_realtime_status(realtime_status), symbols, previous_close_lookup


def _resolve_watchlist_view(
    *,
    keyword: str = "",
    scope: str = "all",
    sort_by: str = "inference_rank",
    symbol: str | None = None,
    include_realtime: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, str, dict[str, Any], list[str], dict[str, float]]:
    watchlist_view = build_watchlist_base_frame()
    realtime_status = _empty_realtime_status()
    if not watchlist_view.empty:
        if include_realtime:
            realtime_quotes, realtime_status, _, _ = _refresh_watchlist_realtime_snapshot()
            if not realtime_quotes.empty:
                watchlist_view = merge_realtime_quotes(watchlist_view, realtime_quotes)
        else:
            try:
                latest_snapshot = get_realtime_quote_store().get_latest_snapshot()
            except Exception as exc:  # pragma: no cover - defensive path
                latest_snapshot = None
                realtime_status = _decorate_realtime_status(
                    {
                        **realtime_status,
                        "error_message": f"读取缓存快照失败：{exc}",
                    }
                )
            if latest_snapshot is not None and not latest_snapshot.quotes.empty:
                watchlist_view = merge_realtime_quotes(watchlist_view, latest_snapshot.quotes)
                realtime_status = _decorate_realtime_status(
                    {
                        **dict(latest_snapshot.status),
                        "available": True,
                        "trade_date": latest_snapshot.trade_date,
                        "snapshot_bucket": latest_snapshot.snapshot_bucket,
                        "served_from": "database",
                    }
                )

    filtered = filtered_watchlist_view(
        watchlist_view,
        keyword=keyword,
        scope=WATCH_SCOPE_MAP.get(scope, "全部"),
        sort_by=WATCH_SORT_MAP.get(sort_by, "最新推理排名"),
    )
    selected_symbol = symbol or (str(filtered.iloc[0]["ts_code"]) if not filtered.empty else "")
    refresh_symbols, refresh_previous_closes = _watchlist_realtime_context(watchlist_view)
    return watchlist_view, filtered, selected_symbol, realtime_status, refresh_symbols, refresh_previous_closes


def _empty_realtime_status() -> dict[str, Any]:
    return {
        "available": False,
        "source": "",
        "trade_date": str(pd.Timestamp.now(tz="Asia/Shanghai").date()),
        "fetched_at": "",
        "requested_symbol_count": 0,
        "success_symbol_count": 0,
        "failed_symbols": [],
        "error_message": "",
        "snapshot_bucket": "",
        "snapshot_label_display": "暂无快照",
        "served_from": "",
    }


def _decorate_realtime_status(status: dict[str, Any]) -> dict[str, Any]:
    from src.app.facades.service_facade import _resolve_realtime_snapshot_display
    payload = dict(status or {})
    snapshot_bucket = str(payload.get("snapshot_bucket", "") or "")
    trade_date_value = payload.get("trade_date")
    fetched_at_value = payload.get("fetched_at")
    trade_date = pd.Timestamp(trade_date_value) if trade_date_value else None
    fetched_at = pd.Timestamp(fetched_at_value) if fetched_at_value else None
    snapshot_label, is_today, is_current_market_day = _resolve_realtime_snapshot_display(
        snapshot_bucket=snapshot_bucket,
        trade_date=trade_date,
        fetched_at=fetched_at,
        available=bool(payload.get("available")),
    )
    payload["snapshot_label_display"] = snapshot_label
    payload["is_today"] = is_today
    payload["is_current_market_day"] = is_current_market_day
    return _json_ready(payload)


def get_watchlist_payload(
    *,
    keyword: str = "",
    scope: str = "all",
    sort_by: str = "inference_rank",
    symbol: str | None = None,
    include_realtime: bool = False,
) -> dict[str, Any]:
    summary = get_watchlist_summary_payload(
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        page=1,
        symbol=symbol,
        include_realtime=include_realtime,
    )
    detail = get_watchlist_detail_payload(
        symbol=str(summary.get("selectedSymbol", "") or ""),
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        include_realtime=include_realtime,
    )
    return {**summary, **detail}


def apply_realtime_to_watchlist_payload(
    payload: dict[str, Any],
    *,
    realtime_quotes: pd.DataFrame,
    realtime_status: dict[str, Any],
) -> dict[str, Any]:
    updated = dict(payload)
    updated["realtimeStatus"] = _decorate_realtime_status(realtime_status)
    if realtime_quotes.empty:
        return updated

    records = pd.DataFrame(updated.get("records", []) or [])
    if records.empty:
        return updated

    merged_records = merge_realtime_quotes(records, realtime_quotes)
    updated["records"] = _frame_records(merged_records)

    selected_symbol = str(updated.get("selectedSymbol", "") or "")
    if selected_symbol:
        selected = merged_records.loc[merged_records["ts_code"].astype(str) == selected_symbol].head(1)
        if not selected.empty:
            updated["detail"] = _json_ready(selected.iloc[0].to_dict())
    return updated


def get_watchlist_summary_payload(
    *,
    keyword: str = "",
    scope: str = "all",
    sort_by: str = "inference_rank",
    page: int = 1,
    symbol: str | None = None,
    include_realtime: bool = False,
) -> dict[str, Any]:
    page_size = 30
    if not include_realtime:
        summary_records = load_watchlist_summary_records(
            WATCHLIST_SUMMARY_RECORD_FIELDS,
            keyword=keyword,
            scope=scope,
            sort_by=sort_by,
            page=page,
            page_size=page_size,
        )
        refresh_context_rows = load_watchlist_summary_records(
            ["ts_code", "latest_bar_close"],
            keyword="",
            scope="all",
            sort_by="inference_rank",
        )
        selected_symbol = str(symbol or (summary_records[0].get("ts_code", "") if summary_records else "") or "")
        records_frame = pd.DataFrame(summary_records)
        realtime_status = _empty_realtime_status()
        if summary_records:
            try:
                latest_snapshot = get_realtime_quote_store().get_latest_snapshot()
            except Exception:
                latest_snapshot = None
            if latest_snapshot is not None and not latest_snapshot.quotes.empty:
                records_frame = merge_realtime_quotes(records_frame, latest_snapshot.quotes)
                realtime_status = _decorate_realtime_status(
                    {
                        **dict(latest_snapshot.status),
                        "available": True,
                        "trade_date": latest_snapshot.trade_date,
                        "snapshot_bucket": latest_snapshot.snapshot_bucket,
                        "served_from": "database",
                    }
                )
                summary_records = merge_realtime_quote_records(summary_records, latest_snapshot.quotes)
        selected_record = {}
        if selected_symbol:
            selected_record = next(
                (dict(record) for record in summary_records if str(record.get("ts_code", "") or "") == selected_symbol),
                {},
            )
        if not selected_record and selected_symbol:
            selected_record = load_watchlist_record(selected_symbol, WATCHLIST_SUMMARY_RECORD_FIELDS)

        refresh_symbols: list[str] = []
        refresh_previous_closes: dict[str, float] = {}
        for row in refresh_context_rows:
            symbol_value = str(row.get("ts_code", "") or "").strip()
            if not symbol_value:
                continue
            refresh_symbols.append(symbol_value)
            latest_bar_close = pd.to_numeric(row.get("latest_bar_close"), errors="coerce")
            if pd.notna(latest_bar_close):
                refresh_previous_closes[symbol_value] = float(latest_bar_close)

        filtered_count = load_watchlist_filtered_count(keyword=keyword, scope=scope)
        total_pages = max(1, (filtered_count + page_size - 1) // page_size) if filtered_count else 1
        return {
            "overview": _json_ready(load_watchlist_overview()),
            "realtimeStatus": _json_ready(realtime_status),
            "filters": {
                "keyword": keyword,
                "scope": scope,
                "sortBy": sort_by,
            },
            "page": max(1, int(page)),
            "pageSize": page_size,
            "totalPages": total_pages,
            "refreshSymbols": refresh_symbols,
            "refreshPreviousCloses": _json_ready(refresh_previous_closes),
            "selectedSymbol": selected_symbol,
            "filteredCount": filtered_count,
            "records": _json_ready(summary_records),
            "selectedRecord": _json_ready(selected_record),
        }

    watchlist_view, filtered, selected_symbol, realtime_status, refresh_symbols, refresh_previous_closes = _resolve_watchlist_view(
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        symbol=symbol,
        include_realtime=include_realtime,
    )
    selected_row = filtered.loc[filtered["ts_code"].astype(str) == selected_symbol].head(1) if selected_symbol and not filtered.empty else pd.DataFrame()
    selected_record = _project_record_fields(selected_row.iloc[0].to_dict(), WATCHLIST_SUMMARY_RECORD_FIELDS) if not selected_row.empty else {}
    summary_columns = [column for column in WATCHLIST_SUMMARY_RECORD_FIELDS if column in filtered.columns]
    summary_records = _frame_records(filtered[summary_columns].copy()) if summary_columns else []

    return {
        "overview": _watchlist_overview_payload(watchlist_view),
        "realtimeStatus": _json_ready(realtime_status),
        "filters": {
            "keyword": keyword,
            "scope": scope,
            "sortBy": sort_by,
        },
        "page": 1,
        "pageSize": int(len(summary_records)),
        "totalPages": 1,
        "refreshSymbols": refresh_symbols,
        "refreshPreviousCloses": _json_ready(refresh_previous_closes),
        "selectedSymbol": selected_symbol,
        "filteredCount": int(len(filtered)),
        "records": summary_records,
        "selectedRecord": _json_ready(selected_record),
    }


def get_watchlist_detail_payload(
    *,
    symbol: str | None = None,
    keyword: str = "",
    scope: str = "all",
    sort_by: str = "inference_rank",
    include_realtime: bool = False,
) -> dict[str, Any]:
    from src.app.services.dashboard_data_service import load_overlay_inference_shortlist
    selected_symbol = str(symbol or "").strip()
    selected_row = pd.DataFrame()
    if selected_symbol:
        selected_record = load_watchlist_record(selected_symbol)
        if selected_record:
            if include_realtime:
                previous_close_lookup: dict[str, float] = {}
                latest_bar_close = pd.to_numeric(selected_record.get("latest_bar_close"), errors="coerce")
                if pd.notna(latest_bar_close):
                    previous_close_lookup[selected_symbol] = float(latest_bar_close)
                realtime_quotes, _ = fetch_managed_realtime_quotes(
                    [selected_symbol],
                    previous_close_lookup=previous_close_lookup,
                    trade_date=pd.Timestamp.now(tz="Asia/Shanghai"),
                )
                if not realtime_quotes.empty:
                    selected_record = merge_realtime_quote_records([selected_record], realtime_quotes)[0]
            else:
                try:
                    latest_snapshot = get_realtime_quote_store().get_latest_snapshot()
                except Exception:
                    latest_snapshot = None
                if latest_snapshot is not None and not latest_snapshot.quotes.empty:
                    selected_quotes = latest_snapshot.quotes.loc[
                        latest_snapshot.quotes["ts_code"].astype(str) == selected_symbol
                    ].copy()
                    if not selected_quotes.empty:
                        selected_record = merge_realtime_quote_record(
                            selected_record,
                            dict(selected_quotes.iloc[0].to_dict()),
                        )
            selected_row = pd.DataFrame([selected_record])
        else:
            _, filtered, selected_symbol, _, _, _ = _resolve_watchlist_view(
                keyword=keyword,
                scope=scope,
                sort_by=sort_by,
                symbol=symbol,
                include_realtime=include_realtime,
            )
            selected_row = filtered.loc[filtered["ts_code"].astype(str) == selected_symbol].head(1) if selected_symbol and not filtered.empty else pd.DataFrame()
    else:
        _, filtered, selected_symbol, _, _, _ = _resolve_watchlist_view(
            keyword=keyword,
            scope=scope,
            sort_by=sort_by,
            symbol=symbol,
            include_realtime=include_realtime,
        )
        selected_row = filtered.loc[filtered["ts_code"].astype(str) == selected_symbol].head(1) if selected_symbol and not filtered.empty else pd.DataFrame()

    detail: dict[str, Any] = {}
    reduce_plan = pd.DataFrame()
    history = pd.DataFrame()
    watch_plan = {}
    action_memo = {}
    discussion_rows: list[dict[str, Any]] = []
    if not selected_row.empty:
        row = selected_row.iloc[0]
        detail = _json_ready(row.to_dict())
        reduce_plan = build_reduce_plan(row)
        history_source = load_prediction_history_for_symbol("ensemble", "test", selected_symbol)
        if not history_source.empty and {"ts_code", "trade_date", "score"}.issubset(history_source.columns):
            history = history_source.loc[
                history_source["ts_code"] == selected_symbol,
                ["trade_date", "score"],
            ].sort_values("trade_date").tail(120)
        discussion_snapshot = row.get("llm_discussion_snapshot")
        if isinstance(discussion_snapshot, dict):
            discussion_rows = discussion_round_rows(discussion_snapshot)
        watch_plan = load_latest_symbol_markdown(selected_symbol, "watch_plan")
        action_memo = load_latest_symbol_markdown(selected_symbol, "action_memo")

    return {
        "selectedSymbol": selected_symbol,
        "detail": detail,
        "reducePlan": _frame_records(reduce_plan),
        "history": _frame_records(history),
        "discussionRows": _json_ready(discussion_rows),
        "watchPlan": _json_ready(watch_plan),
        "actionMemo": _json_ready(action_memo),
        "latestAiShortlist": load_overlay_inference_shortlist(),
    }
