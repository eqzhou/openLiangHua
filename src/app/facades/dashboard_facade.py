from __future__ import annotations

import copy
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.app.viewmodels.candidates_vm import build_candidate_score_history, build_top_candidates_snapshot
from src.app.viewmodels.factor_explorer_vm import (
    build_factor_ranking,
    build_latest_factor_snapshot,
    build_missing_rate_table,
    list_numeric_factor_columns,
)
from src.app.viewmodels.model_backtest_vm import build_monthly_summary, normalize_regime_view
from src.app.viewmodels.overview_vm import build_equity_curve_frame, build_model_comparison_frame
from src.app.services.data_management_service import build_data_management_payload
from src.app.services.dashboard_data_service import (
    FIELD_EXPLANATIONS,
    LABEL_OPTIONS,
    METRIC_EXPLANATIONS,
    MODEL_LABELS,
    MODEL_NAMES,
    ROOT,
    SPLITS,
    SPLIT_LABELS,
    WATCH_SCOPE_MAP,
    WATCH_SORT_MAP,
    build_factor_explorer_snapshot,
    build_metrics_table,
    build_watchlist_base_frame,
    clear_dashboard_data_caches,
    list_available_actions,
    load_daily_bar,
    load_diagnostic_table,
    load_dataset_summary,
    load_experiment_config,
    build_candidate_snapshot,
    load_feature_importance,
    load_feature_history_for_symbol,
    load_feature_panel,
    load_latest_symbol_markdown,
    load_metrics,
    load_overlay_candidate_record,
    load_overlay_candidate_summary_records,
    load_overlay_brief,
    load_overlay_candidates,
    load_overlay_inference_brief,
    load_overlay_inference_candidates,
    load_overlay_inference_shortlist,
    load_overlay_llm_bundle,
    load_overlay_inference_packet,
    load_overlay_packet,
    load_portfolio,
    load_prediction_history_for_symbol,
    load_predictions,
    load_stability,
    load_watchlist_config,
    load_watchlist_filtered_count,
    load_watchlist_record,
    load_watchlist_overview,
    load_watchlist_summary_records,
    run_module,
    save_experiment_config,
    sync_dashboard_database,
)
from src.app.services.realtime_quote_service import (
    fetch_managed_realtime_quotes,
    merge_realtime_quote_record,
    merge_realtime_quote_records,
    merge_realtime_quotes,
)
from src.app.services.streamlit_runtime_service import get_streamlit_service_status
from src.app.services.watchlist_service import build_reduce_plan, filtered_watchlist_view
from src.data.tushare_workflows import run_tushare_full_refresh, run_tushare_incremental_refresh
from src.db.realtime_quote_store import get_realtime_quote_store
from src.utils.data_source import active_data_source
from src.utils.llm_discussion import discussion_round_rows


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is not None:
            return value.isoformat()
        return value.isoformat()
    if isinstance(value, (pd.Timedelta,)):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, str | int | bool) or value is None:
        return value
    if pd.isna(value):
        return None
    return value


def _frame_records(frame: pd.DataFrame, *, limit: int | None = None) -> list[dict[str, Any]]:
    working = frame.copy()
    if limit is not None:
        working = working.head(limit)
    return [_json_ready(record) for record in working.to_dict(orient="records")]


WATCHLIST_SUMMARY_RECORD_FIELDS = [
    "ts_code",
    "name",
    "industry",
    "source_tags",
    "source_category",
    "entry_group",
    "is_watch_only",
    "is_overlay_selected",
    "is_inference_overlay_selected",
    "mark_price",
    "realtime_price",
    "realtime_pct_chg",
    "unrealized_pnl_pct",
    "ensemble_rank",
    "ensemble_rank_pct",
    "universe_size",
    "inference_ensemble_rank",
    "inference_ensemble_rank_pct",
    "premarket_plan",
    "llm_latest_status",
    "llm_latest_summary",
    "action_brief",
    "watch_level",
    "realtime_quote_source",
    "realtime_time",
    "market_value",
]

AI_REVIEW_SUMMARY_CANDIDATE_FIELDS = [
    "ts_code",
    "name",
    "trade_date",
    "industry_display",
    "industry",
    "action_hint",
    "confidence_level",
    "final_score",
    "quant_score",
    "factor_overlay_score",
    "model_consensus",
]

AI_REVIEW_SUMMARY_SELECTED_FIELDS = AI_REVIEW_SUMMARY_CANDIDATE_FIELDS + [
    "thesis_summary",
    "theme_tags",
]

CANDIDATE_SUMMARY_FIELDS = [
    "ts_code",
    "name",
    "industry",
    "rank",
    "score",
    "rank_pct",
    "pct_chg",
    "mom_20",
    "close_to_ma_20",
    "ret_t1_t10",
    "action_hint",
    "trade_date",
]


def _project_record_fields(record: dict[str, Any], field_names: list[str]) -> dict[str, Any]:
    return {field_name: _json_ready(record.get(field_name)) for field_name in field_names if field_name in record}


def clear_dashboard_caches() -> None:
    clear_dashboard_data_caches()
    getattr(_get_home_payload_cached, "cache_clear", lambda: None)()
    getattr(_get_service_payload_cached, "cache_clear", lambda: None)()


def get_bootstrap_payload() -> dict[str, Any]:
    return {
        "modelNames": MODEL_NAMES,
        "splitNames": SPLITS,
        "labelOptions": LABEL_OPTIONS,
        "modelLabels": MODEL_LABELS,
        "splitLabels": SPLIT_LABELS,
        "fieldExplanations": FIELD_EXPLANATIONS,
        "metricExplanations": METRIC_EXPLANATIONS,
        "watchScopes": WATCH_SCOPE_MAP,
        "watchSorts": WATCH_SORT_MAP,
        "actions": list_available_actions(),
    }


def _watchlist_entry_count(watchlist_config: dict[str, Any]) -> int:
    holdings = watchlist_config.get("holdings", []) or []
    focus_pool = watchlist_config.get("focus_pool", []) or []
    return len(holdings) + len(focus_pool)


def _config_summary_text(config: dict[str, Any]) -> str:
    if not config:
        return "当前还没有读取到研究参数。"
    rolling = config.get("rolling", {})
    selection = config.get("selection", {})
    rolling_text = "开启" if rolling.get("enabled", False) else "关闭"
    rolling_freq = rolling.get("retrain_frequency", "once")
    neutral_text = "开启" if selection.get("neutralize_by_industry", False) else "关闭"
    return (
        f"训练起点：{config.get('train_start', '-')}"
        f" | 训练截止：{config.get('train_end', '-')}"
        f" | 验证截止：{config.get('valid_end', '-')}"
        f" | 测试截止：{config.get('test_end', '-')}"
        f" | 标签周期：{config.get('label_col', '-')}"
        f" | 候选股数量：{config.get('top_n', '-')}"
        f" | 滚动训练：{rolling_text}({rolling_freq})"
        f" | 行业中性化：{neutral_text}"
    )


def _clean_config_summary_text(config: dict[str, Any]) -> str:
    if not config:
        return "当前还没有读取到研究参数。"
    rolling = config.get("rolling", {})
    selection = config.get("selection", {})
    rolling_text = "开启" if rolling.get("enabled", False) else "关闭"
    rolling_freq = rolling.get("retrain_frequency", "once")
    neutral_text = "开启" if selection.get("neutralize_by_industry", False) else "关闭"
    return (
        f"训练起点：{config.get('train_start', '-')}"
        f" | 训练截止：{config.get('train_end', '-')}"
        f" | 验证截止：{config.get('valid_end', '-')}"
        f" | 测试截止：{config.get('test_end', '-')}"
        f" | 标签周期：{config.get('label_col', '-')}"
        f" | 候选股数量：{config.get('top_n', '-')}"
        f" | 滚动训练：{rolling_text}({rolling_freq})"
        f" | 行业中性化：{neutral_text}"
    )


def get_shell_payload() -> dict[str, Any]:
    experiment_config = get_experiment_config_payload()
    watchlist_config = load_watchlist_config()
    return {
        "bootstrap": get_bootstrap_payload(),
        "experimentConfig": experiment_config,
        "service": get_service_payload(),
        "watchlistEntryCount": _watchlist_entry_count(watchlist_config),
        "configSummaryText": _clean_config_summary_text(experiment_config),
    }


def _best_comparison_record(comparison: list[dict[str, Any]], field: str, *, mode: str = "max") -> dict[str, Any]:
    if not comparison:
        return {}

    frame = pd.DataFrame(comparison)
    if frame.empty or field not in frame.columns:
        return {}

    numeric_series = pd.to_numeric(frame[field], errors="coerce")
    scoped = frame.loc[numeric_series.notna()].copy()
    if scoped.empty:
        return {}

    scoped[field] = numeric_series.loc[numeric_series.notna()].astype(float)
    ascending = mode == "min"
    return _json_ready(scoped.sort_values(field, ascending=ascending).iloc[0].to_dict())


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


def _get_home_watchlist_payload() -> dict[str, Any]:
    payload = get_watchlist_summary_payload(
        keyword="",
        scope="all",
        sort_by="inference_rank",
        include_realtime=False,
    )
    records = list(payload.get("records", []) or [])[:6]
    return {
        "overview": _json_ready(payload.get("overview", {})),
        "realtimeStatus": _json_ready(payload.get("realtimeStatus", {})),
        "records": _json_ready(records),
    }


def _get_home_payload_cached() -> dict[str, Any]:
    return {
        **get_home_summary_payload(),
        "watchlist": get_home_watchlist_section_payload(),
        "candidates": get_home_candidates_section_payload(),
        "aiReview": get_home_ai_review_section_payload(),
    }


def get_home_payload() -> dict[str, Any]:
    return copy.deepcopy(_get_home_payload_cached())


def get_home_summary_payload() -> dict[str, Any]:
    shell_payload = get_shell_payload()
    overview_payload = get_overview_summary_payload("test")
    watchlist_payload = _get_home_watchlist_payload()
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


def get_home_watchlist_section_payload() -> dict[str, Any]:
    watchlist_payload = _get_home_watchlist_payload()
    watchlist_records = list(watchlist_payload.get("records", []) or [])[:6]
    focus_watch_record = dict(watchlist_records[0]) if watchlist_records else {}
    return {
        "overview": _json_ready(watchlist_payload.get("overview", {})),
        "realtimeStatus": _json_ready(watchlist_payload.get("realtimeStatus", {})),
        "records": _json_ready(watchlist_records),
        "focusRecord": _json_ready(focus_watch_record),
    }


def get_home_candidates_section_payload() -> dict[str, Any]:
    candidates_payload = get_candidates_summary_payload(model_name="ensemble", split_name="test", top_n=6)
    candidate_records = list(candidates_payload.get("latestPicks", []) or [])[:6]
    return {
        "modelName": str(candidates_payload.get("modelName", "ensemble") or "ensemble"),
        "splitName": str(candidates_payload.get("splitName", "test") or "test"),
        "latestDate": _json_ready(candidates_payload.get("latestDate")),
        "records": _json_ready(candidate_records),
        "focusRecord": _json_ready(dict(candidate_records[0]) if candidate_records else {}),
    }


def get_home_ai_review_section_payload() -> dict[str, Any]:
    ai_review_payload = get_ai_review_summary_payload()
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
        "shortlistMarkdown": load_overlay_inference_shortlist(),
    }


def get_overview_summary_payload(split_name: str = "test") -> dict[str, Any]:
    summary = load_dataset_summary()
    metrics_table = build_metrics_table()
    shown_columns = [
        "model",
        "split",
        "rank_ic_mean",
        "top_n_hit_rate",
        "top_n_forward_mean",
        "daily_portfolio_annualized_return",
        "daily_portfolio_sharpe",
        "daily_portfolio_max_drawdown",
        "avg_turnover_ratio",
    ]
    comparison = build_model_comparison_frame(metrics_table, shown_columns)
    return {
        "summary": _json_ready(summary),
        "comparison": _frame_records(comparison),
        "selectedSplit": split_name,
    }


def get_overview_curves_payload(split_name: str = "test") -> dict[str, Any]:
    equity_curves = build_equity_curve_frame(
        model_names=MODEL_NAMES,
        split_name=split_name,
        model_labels=MODEL_LABELS,
        load_portfolio=load_portfolio,
    )
    return {
        "selectedSplit": split_name,
        "equityCurves": _frame_records(equity_curves.reset_index()) if not equity_curves.empty else [],
    }


def get_overview_payload(split_name: str = "test") -> dict[str, Any]:
    return {
        **get_overview_summary_payload(split_name),
        **get_overview_curves_payload(split_name),
    }


def get_factor_explorer_summary_payload(
    *,
    factor_name: str | None = None,
    symbol: str | None = None,
    history_factor: str | None = None,
) -> dict[str, Any]:
    snapshot_payload = build_factor_explorer_snapshot()
    if not snapshot_payload.get("available"):
        return {
            "available": False,
            "latestDate": snapshot_payload.get("latestDate"),
            "factorOptions": [],
            "symbolOptions": [],
            "ranking": [],
            "missingRates": [],
            "selectedFactor": "",
            "selectedHistoryFactor": "",
            "selectedSymbol": "",
        }

    factor_options = list(snapshot_payload.get("factorOptions", []) or [])
    symbol_options = list(snapshot_payload.get("symbolOptions", []) or [])
    cross_section = pd.DataFrame(snapshot_payload.get("crossSection", []) or [])
    missing_rates = pd.DataFrame(snapshot_payload.get("missingRates", []) or [])

    factor_keys = [str(item.get("key", "")) for item in factor_options if str(item.get("key", ""))]
    selected_factor = factor_name or (factor_keys[min(11, len(factor_keys) - 1)] if factor_keys else "")
    selected_history_factor = history_factor or (factor_keys[min(3, len(factor_keys) - 1)] if factor_keys else "")
    selected_symbol = symbol or (symbol_options[0] if symbol_options else "")
    ranking = build_factor_ranking(cross_section, selected_factor)
    selected_row = cross_section.loc[cross_section["ts_code"].astype(str) == selected_symbol].head(1) if selected_symbol and not cross_section.empty else pd.DataFrame()

    return {
        "available": True,
        "latestDate": snapshot_payload.get("latestDate"),
        "selectedFactor": selected_factor,
        "selectedHistoryFactor": selected_history_factor,
        "selectedSymbol": selected_symbol,
        "factorOptions": factor_options,
        "symbolOptions": symbol_options,
        "ranking": _frame_records(ranking, limit=20),
        "missingRates": _frame_records(missing_rates, limit=15),
        "selectedRecord": _json_ready(selected_row.iloc[0].to_dict()) if not selected_row.empty else {},
    }


def get_factor_explorer_detail_payload(
    *,
    factor_name: str | None = None,
    symbol: str | None = None,
    history_factor: str | None = None,
) -> dict[str, Any]:
    snapshot_payload = build_factor_explorer_snapshot()
    factor_options = list(snapshot_payload.get("factorOptions", []) or [])
    factor_keys = [str(item.get("key", "")) for item in factor_options if str(item.get("key", ""))]
    selected_factor = factor_name or (factor_keys[min(11, len(factor_keys) - 1)] if factor_keys else "")
    selected_history_factor = history_factor or (factor_keys[min(3, len(factor_keys) - 1)] if factor_keys else "")

    cross_section = pd.DataFrame(snapshot_payload.get("crossSection", []) or [])
    symbol_options = list(snapshot_payload.get("symbolOptions", []) or [])
    selected_symbol = symbol or (symbol_options[0] if symbol_options else "")

    history = pd.DataFrame()
    if selected_symbol and selected_history_factor:
        history = (
            load_feature_history_for_symbol(selected_symbol, selected_history_factor)
            .dropna(subset=["trade_date", selected_history_factor])
            .sort_values("trade_date")
            .tail(240)
        )
    snapshot = build_latest_factor_snapshot(cross_section, symbol=selected_symbol, zh=lambda value: value)

    return {
        "selectedFactor": selected_factor,
        "selectedHistoryFactor": selected_history_factor,
        "selectedSymbol": selected_symbol,
        "history": _frame_records(history),
        "snapshot": _frame_records(snapshot),
    }


def get_factor_explorer_payload(
    *,
    factor_name: str | None = None,
    symbol: str | None = None,
    history_factor: str | None = None,
) -> dict[str, Any]:
    summary = get_factor_explorer_summary_payload(
        factor_name=factor_name,
        symbol=symbol,
        history_factor=history_factor,
    )
    detail = get_factor_explorer_detail_payload(
        factor_name=summary.get("selectedFactor"),
        symbol=summary.get("selectedSymbol"),
        history_factor=summary.get("selectedHistoryFactor"),
    )
    return {**summary, **detail}


def get_model_backtest_payload(*, model_name: str = "lgbm", split_name: str = "test") -> dict[str, Any]:
    return {
        **get_model_backtest_summary_payload(model_name=model_name, split_name=split_name),
        **get_model_backtest_portfolio_payload(model_name=model_name, split_name=split_name),
        **get_model_backtest_diagnostics_payload(model_name=model_name, split_name=split_name),
    }


def get_model_backtest_summary_payload(*, model_name: str = "lgbm", split_name: str = "test") -> dict[str, Any]:
    return {
        "modelName": model_name,
        "splitName": split_name,
        "metrics": _json_ready(load_metrics(model_name, split_name)),
        "stability": _json_ready(load_stability(model_name)),
    }


def get_model_backtest_portfolio_payload(*, model_name: str = "lgbm", split_name: str = "test") -> dict[str, Any]:
    portfolio = load_portfolio(model_name, split_name)
    monthly_summary = build_monthly_summary(portfolio)
    return {
        "modelName": model_name,
        "splitName": split_name,
        "portfolio": _frame_records(portfolio),
        "monthlySummary": _frame_records(monthly_summary.tail(24)),
    }


def get_model_backtest_diagnostics_payload(*, model_name: str = "lgbm", split_name: str = "test") -> dict[str, Any]:
    regime_diagnostics = normalize_regime_view(load_diagnostic_table(model_name, split_name, "regime"))
    return {
        "modelName": model_name,
        "splitName": split_name,
        "importance": _frame_records(load_feature_importance(model_name), limit=20),
        "yearlyDiagnostics": _frame_records(load_diagnostic_table(model_name, split_name, "yearly")),
        "regimeDiagnostics": _frame_records(regime_diagnostics),
    }


def get_candidates_summary_payload(
    *,
    model_name: str = "lgbm",
    split_name: str = "test",
    top_n: int = 30,
    page: int = 1,
    symbol: str | None = None,
) -> dict[str, Any]:
    candidate_snapshot = build_candidate_snapshot(model_name, split_name)

    latest_picks = pd.DataFrame()
    latest_date = None
    symbol_options: list[str] = []
    selected_symbol = symbol or ""
    total_count = 0
    page_size = max(1, int(top_n))
    normalized_page = max(1, int(page))
    total_pages = 0
    if candidate_snapshot is not None and not candidate_snapshot.empty:
        latest_date = candidate_snapshot["trade_date"].iloc[0] if "trade_date" in candidate_snapshot.columns else None
        total_count = int(len(candidate_snapshot))
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        normalized_page = min(normalized_page, total_pages)
        page_start = (normalized_page - 1) * page_size
        page_end = page_start + page_size
        latest_picks = candidate_snapshot.iloc[page_start:page_end].copy()
        symbol_options = candidate_snapshot["ts_code"].astype(str).tolist()

    selected = candidate_snapshot.loc[candidate_snapshot["ts_code"].astype(str) == selected_symbol].head(1) if candidate_snapshot is not None and not candidate_snapshot.empty and selected_symbol else pd.DataFrame()
    selected_record = _project_record_fields(selected.iloc[0].to_dict(), CANDIDATE_SUMMARY_FIELDS) if not selected.empty else {}
    summary_columns = [column for column in CANDIDATE_SUMMARY_FIELDS if column in latest_picks.columns]

    return {
        "modelName": model_name,
        "splitName": split_name,
        "topN": page_size,
        "page": normalized_page,
        "pageSize": page_size,
        "totalCount": total_count,
        "totalPages": total_pages,
        "latestDate": _json_ready(latest_date),
        "selectedSymbol": selected_symbol,
        "symbolOptions": symbol_options,
        "latestPicks": _frame_records(latest_picks[summary_columns].copy()) if summary_columns else [],
        "selectedRecord": _json_ready(selected_record),
    }


def get_candidate_detail_payload(
    *,
    model_name: str = "lgbm",
    split_name: str = "test",
    symbol: str | None = None,
) -> dict[str, Any]:
    candidate_snapshot = build_candidate_snapshot(model_name, split_name)
    selected_symbol = str(symbol or "").strip()
    if not selected_symbol and candidate_snapshot is not None and not candidate_snapshot.empty and "ts_code" in candidate_snapshot.columns:
        selected_symbol = str(candidate_snapshot.iloc[0]["ts_code"])

    selected = candidate_snapshot.loc[candidate_snapshot["ts_code"].astype(str) == selected_symbol].head(1) if selected_symbol and candidate_snapshot is not None and not candidate_snapshot.empty else pd.DataFrame()
    selected_record = selected.iloc[0].to_dict() if not selected.empty else {}
    field_rows = [{"field": key, "value": _json_ready(value)} for key, value in selected_record.items()]
    return {
        "modelName": model_name,
        "splitName": split_name,
        "selectedSymbol": selected_symbol,
        "selectedRecord": _json_ready(selected_record),
        "fieldRows": field_rows,
    }


def get_candidate_history_payload(
    *,
    model_name: str = "lgbm",
    split_name: str = "test",
    symbol: str | None = None,
) -> dict[str, Any]:
    selected_symbol = symbol or ""
    if not selected_symbol:
        candidate_snapshot = build_candidate_snapshot(model_name, split_name)
        if not candidate_snapshot.empty and "ts_code" in candidate_snapshot.columns:
            selected_symbol = str(candidate_snapshot.iloc[0]["ts_code"])

    score_history = pd.DataFrame()
    if selected_symbol:
        predictions = load_prediction_history_for_symbol(model_name, split_name, selected_symbol)
        if not predictions.empty:
            score_history = build_candidate_score_history(predictions, symbol=selected_symbol).reset_index()
    return {
        "modelName": model_name,
        "splitName": split_name,
        "selectedSymbol": selected_symbol,
        "scoreHistory": _frame_records(score_history),
    }


def get_candidates_payload(
    *,
    model_name: str = "lgbm",
    split_name: str = "test",
    top_n: int = 30,
    page: int = 1,
    symbol: str | None = None,
) -> dict[str, Any]:
    summary = get_candidates_summary_payload(
        model_name=model_name,
        split_name=split_name,
        top_n=top_n,
        page=page,
        symbol=symbol,
    )
    history = get_candidate_history_payload(
        model_name=model_name,
        split_name=split_name,
        symbol=str(summary.get("selectedSymbol", "") or ""),
    )
    return {
        **summary,
        "scoreHistory": history.get("scoreHistory", []),
    }


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
        scope=WATCH_SCOPE_MAP.get(scope, "鍏ㄩ儴"),
        sort_by=WATCH_SORT_MAP.get(sort_by, "??????"),
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
    watchlist_view = build_watchlist_base_frame()
    realtime_status = _empty_realtime_status()
    if not watchlist_view.empty:
        if include_realtime:
            symbols, previous_close_lookup = _watchlist_realtime_context(watchlist_view)
            realtime_quotes, realtime_status = fetch_managed_realtime_quotes(
                symbols,
                previous_close_lookup=previous_close_lookup,
                trade_date=pd.Timestamp.now(tz="Asia/Shanghai"),
            )
            realtime_status = _decorate_realtime_status(realtime_status)
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
    selected_row = filtered.loc[filtered["ts_code"].astype(str) == selected_symbol].head(1)

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

    overview = {
        "totalCount": int(len(watchlist_view)),
        "overlayCount": int(watchlist_view["is_overlay_selected"].fillna(False).sum()) if not watchlist_view.empty and "is_overlay_selected" in watchlist_view.columns else 0,
        "inferenceOverlayCount": int(watchlist_view["is_inference_overlay_selected"].fillna(False).sum()) if not watchlist_view.empty and "is_inference_overlay_selected" in watchlist_view.columns else 0,
        "marketValue": float(pd.to_numeric(watchlist_view["market_value"], errors="coerce").sum()) if not watchlist_view.empty and "market_value" in watchlist_view.columns else 0.0,
        "unrealizedPnl": float(pd.to_numeric(watchlist_view["unrealized_pnl"], errors="coerce").sum()) if not watchlist_view.empty and "unrealized_pnl" in watchlist_view.columns else 0.0,
    }
    refresh_symbols, refresh_previous_closes = _watchlist_realtime_context(watchlist_view)

    return {
        "overview": overview,
        "realtimeStatus": _json_ready(realtime_status),
        "filters": {
            "keyword": keyword,
            "scope": scope,
            "sortBy": sort_by,
        },
        "refreshSymbols": refresh_symbols,
        "refreshPreviousCloses": _json_ready(refresh_previous_closes),
        "selectedSymbol": selected_symbol,
        "filteredCount": int(len(filtered)),
        "records": _frame_records(filtered),
        "detail": detail,
        "reducePlan": _frame_records(reduce_plan),
        "history": _frame_records(history),
        "discussionRows": _json_ready(discussion_rows),
        "watchPlan": _json_ready(watch_plan),
        "actionMemo": _json_ready(action_memo),
    }


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
            except Exception as exc:  # pragma: no cover - defensive path
                latest_snapshot = None
                realtime_status = _decorate_realtime_status(
                    {
                        **realtime_status,
                        "error_message": f"读取缓存快照失败：{exc}",
                    }
                )
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


def get_watchlist_payload(
    *,
    keyword: str = "",
    scope: str = "all",
    sort_by: str = "inference_rank",
    page: int = 1,
    symbol: str | None = None,
    include_realtime: bool = False,
) -> dict[str, Any]:
    summary = get_watchlist_summary_payload(
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        page=page,
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


def _build_ai_panel_payload(
    *,
    scope: str,
    candidates: pd.DataFrame,
    packet: dict[str, Any],
    brief: str,
    selected_symbol: str | None,
) -> dict[str, Any]:
    llm_bundle = load_overlay_llm_bundle(scope)
    response_lookup = dict(llm_bundle.get("response_lookup", {}) or {})
    response_summary = str(llm_bundle.get("response_summary", "") or "")
    has_symbol_column = "ts_code" in candidates.columns
    symbol = selected_symbol or (str(candidates.iloc[0]["ts_code"]) if has_symbol_column and not candidates.empty else "")
    selected = candidates.loc[candidates["ts_code"].astype(str) == symbol].head(1) if has_symbol_column and symbol else pd.DataFrame()
    selected_record = selected.iloc[0].to_dict() if not selected.empty else {}
    llm_response = response_lookup.get(symbol, {})
    return {
        "selectedSymbol": symbol,
        "candidates": _frame_records(candidates),
        "packet": _json_ready(packet),
        "brief": brief,
        "selectedRecord": _json_ready(selected_record),
        "llmResponse": _json_ready(llm_response),
        "responseSummary": response_summary,
    }


def _build_ai_panel_summary_payload(
    *,
    candidates: pd.DataFrame,
    selected_symbol: str | None,
) -> dict[str, Any]:
    has_symbol_column = "ts_code" in candidates.columns
    symbol = selected_symbol or (str(candidates.iloc[0]["ts_code"]) if has_symbol_column and not candidates.empty else "")
    selected = candidates.loc[candidates["ts_code"].astype(str) == symbol].head(1) if has_symbol_column and symbol else pd.DataFrame()
    selected_record = selected.iloc[0].to_dict() if not selected.empty else {}
    return {
        "selectedSymbol": symbol,
        "candidateCount": int(len(candidates)),
        "candidates": _frame_records(candidates),
        "selectedRecord": _json_ready(selected_record),
    }


def _build_ai_panel_detail_payload(
    *,
    scope: str,
    candidates: pd.DataFrame,
    packet: dict[str, Any],
    brief: str,
    selected_symbol: str | None,
) -> dict[str, Any]:
    full_payload = _build_ai_panel_payload(
        scope=scope,
        candidates=candidates,
        packet=packet,
        brief=brief,
        selected_symbol=selected_symbol,
    )
    selected_record = dict(full_payload.get("selectedRecord", {}) or {})
    field_rows = [
        {"field": key, "value": _json_ready(value)}
        for key, value in selected_record.items()
    ]
    return {
        "selectedSymbol": full_payload.get("selectedSymbol", ""),
        "selectedRecord": selected_record,
        "fieldRows": field_rows,
        "brief": str(full_payload.get("brief", "") or ""),
        "llmResponse": dict(full_payload.get("llmResponse", {}) or {}),
        "responseSummary": str(full_payload.get("responseSummary", "") or ""),
    }


def _build_ai_review_summary_panel_payload(*, scope: str, selected_symbol: str | None) -> dict[str, Any]:
    candidate_records = load_overlay_candidate_summary_records(scope, AI_REVIEW_SUMMARY_CANDIDATE_FIELDS)
    candidates = pd.DataFrame(candidate_records)
    symbol = str(selected_symbol or "").strip()
    selected_record = load_overlay_candidate_record(scope, symbol, AI_REVIEW_SUMMARY_SELECTED_FIELDS) if symbol else {}
    return {
        "selectedSymbol": symbol,
        "candidateCount": len(candidate_records),
        "candidates": _json_ready(candidate_records),
        "selectedRecord": _json_ready(selected_record),
    }


def _build_ai_review_detail_panel_payload(*, scope: str, selected_symbol: str | None) -> dict[str, Any]:
    summary_panel = _build_ai_review_summary_panel_payload(scope=scope, selected_symbol=selected_symbol)
    resolved_symbol = str(summary_panel.get("selectedSymbol", "") or "")
    selected_record = load_overlay_candidate_record(scope, resolved_symbol) if resolved_symbol else {}
    packet = load_overlay_inference_packet() if scope == "inference" else load_overlay_packet()
    brief = load_overlay_inference_brief() if scope == "inference" else load_overlay_brief()
    llm_bundle = load_overlay_llm_bundle(scope)
    response_lookup = dict(llm_bundle.get("response_lookup", {}) or {})
    field_rows = [{"field": key, "value": _json_ready(value)} for key, value in selected_record.items()]
    return {
        "selectedSymbol": resolved_symbol,
        "selectedRecord": _json_ready(selected_record),
        "fieldRows": field_rows,
        "brief": brief,
        "llmResponse": _json_ready(response_lookup.get(resolved_symbol, {})),
        "responseSummary": str(llm_bundle.get("response_summary", "") or ""),
        "packet": _json_ready(packet),
    }


def get_ai_review_summary_payload(
    *,
    inference_symbol: str | None = None,
    historical_symbol: str | None = None,
) -> dict[str, Any]:
    return {
        "inference": _build_ai_review_summary_panel_payload(scope="inference", selected_symbol=inference_symbol),
        "historical": _build_ai_review_summary_panel_payload(scope="historical", selected_symbol=historical_symbol),
    }


def get_ai_review_detail_payload(
    *,
    scope: str,
    symbol: str | None = None,
) -> dict[str, Any]:
    normalized_scope = "inference" if scope == "inference" else "historical"
    detail_payload = _build_ai_review_detail_panel_payload(scope=normalized_scope, selected_symbol=symbol)
    detail_payload.pop("packet", None)
    return detail_payload


def get_ai_review_payload(
    *,
    inference_symbol: str | None = None,
    historical_symbol: str | None = None,
) -> dict[str, Any]:
    inference_summary = _build_ai_review_summary_panel_payload(scope="inference", selected_symbol=inference_symbol)
    historical_summary = _build_ai_review_summary_panel_payload(scope="historical", selected_symbol=historical_symbol)
    inference_detail = _build_ai_review_detail_panel_payload(
        scope="inference",
        selected_symbol=str(inference_summary.get("selectedSymbol", "") or ""),
    )
    historical_detail = _build_ai_review_detail_panel_payload(
        scope="historical",
        selected_symbol=str(historical_summary.get("selectedSymbol", "") or ""),
    )
    return {
        "inference": {**inference_summary, **inference_detail},
        "historical": {**historical_summary, **historical_detail},
    }


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


def get_service_payload() -> dict[str, Any]:
    payload = _json_ready(get_streamlit_service_status(ROOT))
    payload["realtime_snapshot"] = _get_realtime_snapshot_summary()
    return payload


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


def refresh_realtime_payload() -> dict[str, Any]:
    realtime_quotes, realtime_status, symbols, _ = _refresh_watchlist_realtime_snapshot()
    return {
        "ok": bool(realtime_status.get("available")) or not symbols,
        "symbolCount": int(len(symbols)),
        "realtimeRecordCount": int(len(realtime_quotes)),
        "realtimeStatus": _json_ready(realtime_status),
    }


def get_experiment_config_payload() -> dict[str, Any]:
    return _json_ready(load_experiment_config())


def update_experiment_config_payload(payload: dict[str, Any]) -> dict[str, Any]:
    save_experiment_config(payload)
    clear_dashboard_caches()
    return get_experiment_config_payload()


def clear_cache_payload() -> dict[str, Any]:
    clear_dashboard_caches()
    return {"ok": True}


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


def run_named_action(action_name: str) -> dict[str, Any]:
    specs = {spec["actionName"]: spec for spec in list_available_actions()}
    if action_name not in specs:
        raise KeyError(action_name)
    spec = specs[action_name]
    ok, output = run_module(spec["moduleName"])
    if ok:
        sync_ok, sync_message = sync_dashboard_database()
        output = f"{output}\n\n[dashboard-db] {sync_message}".strip() if output else f"[dashboard-db] {sync_message}"
        ok = ok and sync_ok
    clear_dashboard_caches()
    return {
        "actionName": action_name,
        "label": spec["label"],
        "ok": ok,
        "output": output,
    }


def generate_watch_plan() -> dict[str, Any]:
    ok, output = run_module("src.agents.watch_plan")
    if ok:
        sync_ok, sync_message = sync_dashboard_database()
        output = f"{output}\n\n[dashboard-db] {sync_message}".strip() if output else f"[dashboard-db] {sync_message}"
        ok = ok and sync_ok
    clear_dashboard_caches()
    return {"actionName": "watch_plan", "ok": ok, "output": output}


def generate_action_memo() -> dict[str, Any]:
    ok, output = run_module("src.agents.action_memo")
    if ok:
        sync_ok, sync_message = sync_dashboard_database()
        output = f"{output}\n\n[dashboard-db] {sync_message}".strip() if output else f"[dashboard-db] {sync_message}"
        ok = ok and sync_ok
    clear_dashboard_caches()
    return {"actionName": "action_memo", "ok": ok, "output": output}
