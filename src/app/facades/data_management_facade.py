from __future__ import annotations

from typing import Any

from src.app.facades.base import (
    _json_ready, 
)
from src.app.services.data_management_service import build_data_management_payload
from src.app.services.dashboard_data_service import ROOT
from src.utils.data_source import active_data_source
from src.data.tushare_workflows import run_tushare_full_refresh, run_tushare_incremental_refresh


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
