from __future__ import annotations

from typing import Any

from src.app.facades.base import (
    _json_ready, 
)
from src.app.services.data_management_service import build_data_management_payload
from src.app.services.dashboard_data_service import ROOT
from src.utils.data_source import active_data_source
from src.data.market_bars_tushare_sync import sync_market_bars_from_tushare
from src.data.tushare_workflows import run_tushare_full_refresh, run_tushare_incremental_refresh
from src.features.build_feature_panel import build_feature_label_artifacts
from src.agents.overlay_inference_report import generate_overlay_inference_report


def get_data_management_payload(*, include_sensitive: bool = True) -> dict[str, Any]:
    return _json_ready(
        build_data_management_payload(
            root=ROOT,
            target_source=active_data_source(),
            include_sensitive=include_sensitive,
        )
    )


def _validate_tushare_target_source(target_source: str) -> str:
    normalized = str(target_source or "akshare").strip().lower() or "akshare"
    if normalized not in {"akshare", "tushare"}:
        raise ValueError("Tushare refresh currently only supports the akshare/tushare local panel.")
    return normalized


def _format_refresh_output(title: str, lines: list[str]) -> str:
    rendered_lines = [title, *[line for line in lines if line]]
    return "\n".join(rendered_lines).strip()


def run_tushare_incremental_refresh_payload(*, target_source: str | None = None, end_date: str | None = None) -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    resolved_target_source = _validate_tushare_target_source(target_source or active_data_source())
    summary = run_tushare_incremental_refresh(root=ROOT, target_source=resolved_target_source, end_date=end_date)
    clear_dashboard_caches()
    output = _format_refresh_output(
        "Tushare 增量日线刷新完成",
        [
            f"目标数据源：{summary.get('target_source', resolved_target_source)}",
            f"上一最新日期：{summary.get('previous_latest_trade_date', '-')}",
            f"当前最新日期：{summary.get('latest_trade_date', '-')}",
            f"追加交易日：{summary.get('appended_trade_dates', 0)}",
            f"追加行数：{summary.get('appended_rows', 0)}",
            f"股票数量：{summary.get('symbols', 0)}",
        ],
    )
    return {
        "actionName": "tushare_incremental_refresh",
        "label": "Tushare 增量刷新日线",
        "ok": True,
        "output": output,
    }


def run_tushare_full_refresh_payload(*, target_source: str | None = None, end_date: str | None = None) -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    resolved_target_source = _validate_tushare_target_source(target_source or active_data_source())
    summary = run_tushare_full_refresh(root=ROOT, target_source=resolved_target_source, end_date=end_date)
    clear_dashboard_caches()
    incremental = summary.get("incremental", {}) if isinstance(summary.get("incremental"), dict) else {}
    feature_summary = summary.get("features", {}) if isinstance(summary.get("features"), dict) else {}
    dashboard_sync_summary = summary.get("dashboardSync", {}) if isinstance(summary.get("dashboardSync"), dict) else {}
    output = _format_refresh_output(
        "Tushare 全流程刷新完成",
        [
            f"目标数据源：{summary.get('target_source', resolved_target_source)}",
            f"日线最新日期：{incremental.get('latest_trade_date', '-')}",
            f"追加交易日：{incremental.get('appended_trade_dates', 0)}",
            f"追加行数：{incremental.get('appended_rows', 0)}",
            f"特征行数：{feature_summary.get('feature_rows', 0)}",
            f"标签行数：{feature_summary.get('label_rows', 0)}",
            f"快照同步：{dashboard_sync_summary.get('message', '-')}",
        ],
    )
    return {
        "actionName": "tushare_full_refresh",
        "label": "Tushare 全流程刷新",
        "ok": bool(summary.get("ok", False)),
        "output": output,
    }


def run_market_bars_refresh_payload(*, user_id: str, end_date: str | None = None) -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    summary = sync_market_bars_from_tushare(user_id=user_id, end_date=end_date)
    clear_dashboard_caches()
    output = _format_refresh_output(
        "市场日线主表更新完成",
        [
            f"用户范围：{summary.user_id or user_id}",
            f"上一最新日期：{summary.previous_latest_trade_date or '-'}",
            f"当前最新日期：{summary.latest_trade_date or '-'}",
            f"请求股票数：{summary.requested_symbols}",
            f"抓取行数：{summary.fetched_rows}",
            f"写入行数：{summary.upserted_rows}",
        ],
    )
    return {
        "actionName": "market_bars_refresh",
        "label": "更新市场日线主表",
        "ok": True,
        "output": output,
    }


def run_watchlist_research_refresh_payload(*, user_id: str, target_source: str | None = None) -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    resolved_target_source = _validate_tushare_target_source(target_source or active_data_source())
    feature_summary = build_feature_label_artifacts(
        root=ROOT,
        data_source=resolved_target_source,
        market_universe_user_id=user_id,
    )
    inference_packet = generate_overlay_inference_report(root=ROOT, execute_llm=False, user_id=user_id)
    clear_dashboard_caches()
    output = _format_refresh_output(
        "观察池研究面板与最新推理刷新完成",
        [
            f"目标数据源：{resolved_target_source}",
            f"研究面板最新日期：{feature_summary.get('date_max', '-')}",
            f"研究面板行数：{feature_summary.get('panel_rows', feature_summary.get('feature_rows', 0))}",
            f"股票数量：{feature_summary.get('symbol_count', 0)}",
            f"最新推理日期：{inference_packet.get('latest_feature_date', '-')}",
            f"推理股票数量：{inference_packet.get('inference_universe_size', 0)}",
            f"AI候选数量：{inference_packet.get('candidate_count', 0)}",
        ],
    )
    return {
        "actionName": "watchlist_research_refresh",
        "label": "重建研究面板与最新推理",
        "ok": True,
        "output": output,
    }
