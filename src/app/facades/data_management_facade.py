from __future__ import annotations

from typing import Any

from src.app.facades.base import (
    _json_ready, 
)
from src.app.repositories.config_repository import load_experiment_config
from src.app.services.data_management_service import build_data_management_payload
from src.app.services.data_management_service import build_myquant_status_payload
from src.app.services.dashboard_data_service import ROOT
from src.utils.data_source import active_data_source
from src.data.market_bars_tushare_sync import sync_market_bars_from_tushare
from src.data.tushare_workflows import run_tushare_full_refresh, run_tushare_incremental_refresh
from src.features.build_feature_panel import build_feature_label_artifacts
from src.agents.overlay_inference_report import generate_overlay_inference_report
import pandas as pd


def get_data_management_payload(*, include_sensitive: bool = True) -> dict[str, Any]:
    return _json_ready(
        build_data_management_payload(
            root=ROOT,
            target_source=active_data_source(),
            include_sensitive=include_sensitive,
        )
    )


def get_myquant_status_payload(*, include_sensitive: bool = True) -> dict[str, Any]:
    return _json_ready(build_myquant_status_payload(root=ROOT, include_sensitive=include_sensitive))


def _validate_tushare_target_source(target_source: str) -> str:
    normalized = str(target_source or "akshare").strip().lower() or "akshare"
    if normalized not in {"akshare", "tushare"}:
        raise ValueError("Tushare refresh currently only supports the akshare/tushare local panel.")
    return normalized


def _format_refresh_output(title: str, lines: list[str]) -> str:
    rendered_lines = [title, *[line for line in lines if line]]
    return "\n".join(rendered_lines).strip()


def _action_error_payload(action_name: str, label: str, exc: Exception) -> dict[str, Any]:
    return {
        "actionName": action_name,
        "label": label,
        "ok": False,
        "output": f"任务执行失败：{exc.__class__.__name__}。请查看服务端日志确认原因。",
    }


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


def run_myquant_download_payload(*, user_id: str, end_date: str | None = None) -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    from src.data.myquant_downloader import run as run_myquant_download
    from src.db.dashboard_sync import sync_dataset_summary_artifact, sync_watchlist_snapshot_artifact

    label = "MyQuant 下载"
    experiment = load_experiment_config(ROOT, prefer_database=True)
    start_date = str(experiment.get("train_start", "2018-01-01"))
    resolved_end_date = str(end_date or pd.Timestamp.now(tz="Asia/Shanghai").date().isoformat())
    try:
        run_myquant_download(
            start_date=start_date,
            end_date=resolved_end_date,
            chunk_size=10,
            write_canonical=active_data_source() == "myquant",
        )
        dataset_summary = sync_dataset_summary_artifact(root=ROOT, data_source="myquant")
        watchlist_summary = sync_watchlist_snapshot_artifact(root=ROOT, data_source="myquant", user_id=user_id)
        clear_dashboard_caches()
    except Exception as exc:
        clear_dashboard_caches()
        return _action_error_payload("myquant_download", label, exc)

    return {
        "actionName": "myquant_download",
        "label": label,
        "ok": True,
        "output": _format_refresh_output(
            "MyQuant 下载完成",
            [
                f"起始日期：{start_date}",
                f"截止日期：{resolved_end_date}",
                f"数据摘要：{dataset_summary.message}",
                f"观察池快照：{watchlist_summary.message}",
            ],
        ),
    }


def run_myquant_enrich_payload(*, user_id: str) -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches
    from src.data.myquant_enrich import run as run_myquant_enrich
    from src.db.dashboard_sync import sync_dataset_summary_artifact, sync_watchlist_snapshot_artifact

    label = "MyQuant 清洗增强"
    try:
        run_myquant_enrich(write_canonical=active_data_source() == "myquant")
        dataset_summary = sync_dataset_summary_artifact(root=ROOT, data_source="myquant")
        watchlist_summary = sync_watchlist_snapshot_artifact(root=ROOT, data_source="myquant", user_id=user_id)
        clear_dashboard_caches()
    except Exception as exc:
        clear_dashboard_caches()
        return _action_error_payload("myquant_enrich", label, exc)

    return {
        "actionName": "myquant_enrich",
        "label": label,
        "ok": True,
        "output": _format_refresh_output(
            "MyQuant 清洗增强完成",
            [
                f"数据摘要：{dataset_summary.message}",
                f"观察池快照：{watchlist_summary.message}",
            ],
        ),
    }


def run_myquant_research_refresh_payload(*, user_id: str) -> dict[str, Any]:
    from src.app.facades.base import clear_dashboard_caches

    label = "MyQuant 研究面板刷新"
    try:
        feature_summary = build_feature_label_artifacts(
            root=ROOT,
            data_source="myquant",
            prefer_source_daily_bar=True,
        )
        inference_packet = generate_overlay_inference_report(
            root=ROOT,
            execute_llm=False,
            user_id=user_id,
            data_source="myquant",
        )
        clear_dashboard_caches()
    except Exception as exc:
        clear_dashboard_caches()
        return _action_error_payload("myquant_research_refresh", label, exc)

    return {
        "actionName": "myquant_research_refresh",
        "label": label,
        "ok": True,
        "output": _format_refresh_output(
            "MyQuant 研究面板与最新推理刷新完成",
            [
                f"研究面板最新日期：{feature_summary.get('date_max', '-')}",
                f"研究面板行数：{feature_summary.get('panel_rows', feature_summary.get('feature_rows', 0))}",
                f"最新推理日期：{inference_packet.get('latest_feature_date', '-')}",
                f"AI候选数量：{inference_packet.get('candidate_count', 0)}",
            ],
        ),
    }
