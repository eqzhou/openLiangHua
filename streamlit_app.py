from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

from src.app.page_registry import PageDefinition, build_page_registry, page_labels, render_registered_page
from src.app.facades.dashboard_facade import (
    apply_realtime_to_watchlist_payload,
    clear_cache_payload,
    get_ai_review_payload,
    get_candidates_payload,
    get_factor_explorer_payload,
    get_model_backtest_payload,
    get_overview_payload,
    get_shell_payload,
    get_watchlist_payload,
    run_named_action,
    update_experiment_config_payload,
)
from src.app.pages.ai_review_page import render_ai_review_payload_page
from src.app.pages.candidates_page import render_candidates_payload_page
from src.app.pages.factor_explorer_page import render_factor_explorer_payload_page
from src.app.pages.model_backtest_page import render_model_backtest_payload_page
from src.app.pages.overview_page import render_overview_payload_page
from src.app.pages.service_page import render_service_page
from src.app.pages.watchlist_page import render_watchlist_payload_page
from src.app.repositories.report_repository import (
    load_label_panel as repo_load_label_panel,
    read_json as repo_read_json,
)
from src.app.services.dashboard_data_service import (
    WATCH_SCOPE_MAP as SHARED_WATCH_SCOPE_MAP,
    WATCH_SORT_MAP as SHARED_WATCH_SORT_MAP,
    build_metrics_table as shared_build_metrics_table,
    clear_dashboard_data_caches as shared_clear_dashboard_data_caches,
    load_daily_bar as shared_load_daily_bar,
    load_dataset_summary as shared_load_dataset_summary,
    load_diagnostic_table as shared_load_diagnostic_table,
    load_feature_importance as shared_load_feature_importance,
    load_feature_panel as shared_load_feature_panel,
    load_latest_symbol_markdown as shared_load_latest_symbol_markdown,
    load_metrics as shared_load_metrics,
    load_overlay_brief as shared_load_overlay_brief,
    load_overlay_candidates as shared_load_overlay_candidates,
    load_overlay_inference_brief as shared_load_overlay_inference_brief,
    load_overlay_inference_candidates as shared_load_overlay_inference_candidates,
    load_overlay_inference_packet as shared_load_overlay_inference_packet,
    load_overlay_packet as shared_load_overlay_packet,
    load_portfolio as shared_load_portfolio,
    load_predictions as shared_load_predictions,
    load_stability as shared_load_stability,
    run_module as shared_run_module,
)
from src.app.services.realtime_quote_service import fetch_realtime_quotes, merge_realtime_quotes
from src.app.services.watchlist_service import build_watchlist_view as svc_build_watchlist_view
from src.app.ui.app_shell import render_app_shell
from src.app.ui.sidebar_actions import (
    build_current_config_caption,
    render_sidebar,
)
from src.app.ui.ui_text import (
    PAGE_AI_REVIEW,
    PAGE_CANDIDATES,
    PAGE_FACTOR_EXPLORER,
    PAGE_MODEL_BACKTEST,
    PAGE_OVERVIEW,
    PAGE_SERVICE,
    PAGE_WATCHLIST,
)
from src.utils.data_source import active_data_source, source_or_canonical_path

ROOT = Path(__file__).resolve().parent
PROJECT_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
DATA_DIR = ROOT / "data"
ACTIVE_DATA_SOURCE = active_data_source()
RAW_DIR = DATA_DIR / "raw" / ACTIVE_DATA_SOURCE
STAGING_DIR = DATA_DIR / "staging"
FEATURE_PATH = source_or_canonical_path(DATA_DIR / "features", "feature_panel.parquet", ACTIVE_DATA_SOURCE)
LABEL_PATH = source_or_canonical_path(DATA_DIR / "labels", "label_panel.parquet", ACTIVE_DATA_SOURCE)
DAILY_BAR_PATH = source_or_canonical_path(STAGING_DIR, "daily_bar.parquet", ACTIVE_DATA_SOURCE)
REPORTS_DIR = ROOT / "reports" / "weekly"
LOG_DIR = ROOT / "logs"
EXPERIMENT_CONFIG_PATH = ROOT / "config" / "experiment.yaml"
WATCHLIST_CONFIG_PATH = ROOT / "config" / "watchlist.yaml"
STREAMLIT_STATUS_SCRIPT_PATH = ROOT / "scripts" / "streamlit_status.ps1"

MODEL_NAMES = ["ridge", "lgbm", "ensemble"]
SPLITS = ["valid", "test"]
LABEL_OPTIONS = ["ret_t1_t5", "ret_t1_t10", "ret_t1_t20"]
MODEL_LABELS = {
    "ridge": "岭回归基线",
    "lgbm": "梯度提升树基线",
    "ensemble": "自适应融合策略",
}
SPLIT_LABELS = {
    "valid": "验证集",
    "test": "测试集",
}
ACTION_LABELS = {
    "materialize": "刷新部分面板",
    "features": "重建特征与标签",
    "ridge": "运行岭回归基线",
    "lgbm": "运行梯度提升树基线",
    "ensemble": "运行自适应融合策略",
    "overlay": "生成AI研判摘要",
    "latest_inference": "生成最新未标注截面推理",
    "watch_plan": "生成持仓计划",
    "action_memo": "生成操作备忘",
    "config": "保存研究参数",
}
COLUMN_LABELS = {
    "trade_date": "交易日期",
    "ts_code": "股票代码",
    "name": "股票名称",
    "entry_group": "分组",
    "industry": "行业",
    "index_code": "指数代码",
    "is_current_name_st": "当前名称含 ST",
    "is_index_member": "指数成分股",
    "days_since_list": "上市天数",
    "turnover_rate": "当日换手率",
    "pct_chg": "日涨跌幅(%)",
    "index_weight": "指数权重",
    "ret_1d": "单日收益率",
    "mom_5": "5日趋势强度",
    "mom_20": "20日趋势强度",
    "mom_60": "60日趋势强度",
    "mom_120": "120日趋势强度",
    "vol_20": "20日波动水平",
    "close_to_ma_20": "偏离20日均线",
    "vol_60": "60日波动水平",
    "close_to_ma_60": "偏离60日均线",
    "turnover_20": "20日平均换手",
    "amount_20": "20日平均成交额",
    "downside_vol_20": "20日下行波动",
    "ret_skew_20": "20日收益偏斜",
    "drawdown_60": "近60日回撤",
    "can_enter_next_day": "次日可成交",
    "ret_next_1d": "次日收益率",
    "label_valid_t5": "5日标签可用",
    "ret_t1_t5": "未来5日收益(T+1建仓)",
    "label_valid_t10": "10日标签可用",
    "ret_t1_t10": "未来10日收益(T+1建仓)",
    "label_valid_t20": "20日标签可用",
    "ret_t1_t20": "未来20日收益(T+1建仓)",
    "score": "综合评分",
    "score_raw": "原始模型评分",
    "feature": "因子名称",
    "coefficient": "回归系数",
    "importance_gain": "增益贡献度",
    "importance_split": "分裂使用次数",
    "gross_return": "单期毛收益",
    "net_return": "单期净收益",
    "equity_curve": "组合净值",
    "month": "月份",
    "model": "模型",
    "split": "数据集",
    "observations": "样本条数",
    "dates": "交易日数量",
    "rank_ic_mean": "截面排序有效性均值",
    "rank_ic_std": "截面排序有效性波动",
    "top_n_forward_mean": "前N股票平均未来收益",
    "top_n_hit_rate": "前N股票正收益占比",
    "daily_portfolio_annualized_return": "组合年化收益",
    "daily_portfolio_sharpe": "组合夏普",
    "daily_portfolio_max_drawdown": "组合最大回撤",
    "holding_period_days": "持有周期(交易日)",
    "risk_filter_active_ratio": "趋势过滤开启占比",
    "risk_filter_filtered_periods": "趋势过滤触发次数",
    "benchmark_proxy_total_return": "基准代理累计收益",
    "avg_turnover_ratio": "平均换手比例",
    "max_turnover_ratio": "最大换手比例",
    "avg_selected_count": "平均持仓数",
    "year": "年份",
    "periods": "期数",
    "total_return": "累计收益",
    "annualized_return": "年化收益",
    "sharpe": "夏普",
    "max_drawdown": "最大回撤",
    "win_rate": "正收益占比",
    "avg_net_return": "平均净收益",
    "unfiltered_total_return": "过滤前累计收益",
    "unfiltered_avg_return": "过滤前平均收益",
    "benchmark_total_return": "基准累计收益",
    "benchmark_avg_return": "基准平均收益",
    "risk_on_ratio": "趋势开启占比",
    "regime": "趋势阶段",
    "quant_score": "量化合成分",
    "factor_overlay_score": "因子解释分",
    "model_consensus": "模型共识度",
    "lgbm_rank_pct": "梯度提升树分位",
    "ridge_rank_pct": "岭回归分位",
    "final_score": "AI合成总分",
    "confidence_level": "置信度",
    "bull_points": "正面要点",
    "risk_points": "风险提示",
    "industry_display": "行业显示",
    "theme_tags": "主题标签",
    "action_hint": "建议动作",
    "thesis_summary": "结论摘要",
    "ai_brief": "中文研判摘要",
    "agent_prompt": "AI继续研判提示词",
    "notice_digest": "公告摘要",
    "news_digest": "新闻/研报摘要",
    "news_source": "资讯来源",
    "research_digest": "研报补充",
    "missing_rate": "缺失率",
    "cost_basis": "持仓成本",
    "shares": "持股数量",
    "mark_price": "参考价格",
    "mark_date": "参考日期",
    "price_source": "价格来源",
    "realtime_price": "实时价格",
    "realtime_pct_chg": "盘中涨跌幅",
    "realtime_time": "分钟更新时间",
    "realtime_high": "日内最高",
    "realtime_low": "日内最低",
    "realtime_unrealized_pnl": "实时浮盈亏",
    "realtime_unrealized_pnl_pct": "实时盈亏比",
    "realtime_vs_mark_pct": "相对参考价变动",
    "market_value": "参考市值",
    "unrealized_pnl": "浮动盈亏",
    "unrealized_pnl_pct": "浮亏浮盈比例",
    "signal_date": "信号日期",
    "ensemble_rank": "融合排名",
    "ensemble_rank_pct": "融合分位",
    "inference_signal_date": "最新推理日期",
    "inference_ensemble_rank": "最新推理排名",
    "inference_ensemble_rank_pct": "最新推理分位",
    "inference_score": "最新推理分数",
    "ridge_rank_pct": "岭回归分位",
    "lgbm_rank_pct": "梯度提升树分位",
    "is_overlay_selected": "AI精选池",
    "is_inference_overlay_selected": "最新推理池",
    "llm_round_count": "已检查轮次",
    "llm_selected_round_count": "入选研讨轮次",
    "llm_success_round_count": "成功研讨轮次",
    "llm_latest_round": "最新研讨轮次",
    "llm_latest_status": "最新研讨状态",
    "llm_latest_summary": "最新研讨摘要",
    "llm_overview": "研讨总览",
    "premarket_plan": "盘前执行建议",
    "premarket_plan_source": "建议来源",
    "focus_note": "关注备注",
    "manual_mark_note": "价格备注",
    "latest_bar_date": "最新落库日线日期",
    "latest_bar_close": "最新落库收盘价",
    "mark_status": "价格状态",
    "mark_status_note": "价格状态说明",
    "mark_vs_latest_bar_days": "参考价相对日线天数差",
    "breakeven_price": "回本价",
    "gap_to_breakeven_pct": "距回本涨幅",
    "halfway_recovery_price": "半程修复位",
    "defensive_price": "观察防守位",
    "watch_level": "观察级别",
    "action_brief": "持仓提示",
    "plan_stage": "计划阶段",
    "target_price": "目标价",
    "reduce_ratio": "计划减仓比例",
    "target_shares": "对应股数",
    "distance_from_mark_pct": "距参考价涨幅",
    "estimated_realized_pnl": "预计实现盈亏",
    "plan_note": "计划说明",
}
FIELD_EXPLANATIONS = {
    "mom_5": "近5个交易日的趋势强度，数值越高通常代表短线更强。",
    "mom_20": "近20个交易日的趋势强度，常用来观察中短期强弱。",
    "mom_60": "近60个交易日的趋势强度，更接近中期趋势判断。",
    "mom_120": "近120个交易日的趋势强度，用来观察更长周期的延续性。",
    "vol_20": "近20日收益波动水平，越高代表短期价格起伏更大。",
    "vol_60": "近60日收益波动水平，用来观察中期稳定性。",
    "close_to_ma_20": "当前价格相对20日均线的位置，正值代表强于短期均线。",
    "close_to_ma_60": "当前价格相对60日均线的位置，正值代表强于中期均线。",
    "turnover_rate": "股票当日换手率，反映当天成交活跃度。",
    "turnover_20": "近20日平均换手，数值越高通常代表阶段性更活跃。",
    "amount_20": "近20日平均成交额，反映股票的成交容量。",
    "downside_vol_20": "近20日只统计下跌部分的波动，越高说明下跌过程更剧烈。",
    "ret_skew_20": "近20日收益分布偏斜程度，用来观察大涨大跌的偏向。",
    "drawdown_60": "近60日相对阶段高点的回撤幅度，越低表示回撤越深。",
    "index_weight": "股票在当前指数中的权重，占比越高通常越核心。",
    "pct_chg": "股票当日涨跌幅，单位为百分比。",
    "ret_t1_t10": "默认按T+1建仓后持有10个交易日得到的未来收益。",
    "score": "模型给股票的综合评分，越高通常越值得优先关注。",
    "rank_ic_mean": "衡量模型截面排序是否有效，越高说明高分股后续表现越好。",
    "top_n_hit_rate": "前N只高分股票里，未来收益为正的比例。",
    "top_n_forward_mean": "前N只高分股票的平均未来收益，用来看组合直观赚钱能力。",
    "daily_portfolio_annualized_return": "把日度组合收益换算成年化后得到的收益水平。",
    "daily_portfolio_sharpe": "单位波动对应的超额收益能力，越高越稳健。",
    "daily_portfolio_max_drawdown": "组合从阶段高点回撤到低点的最大跌幅。",
}
METRIC_EXPLANATIONS = {
    "rank_ic_mean": "看模型能不能把未来更强的股票排到前面，越高越好。",
    "top_n_hit_rate": "看前N只高分股票里有多少最终赚钱，越高越好。",
    "top_n_forward_mean": "看前N只高分股票平均未来收益是多少，越高越好。",
    "daily_portfolio_annualized_return": "把组合日收益折算成年化收益，用来看长期回报水平。",
    "daily_portfolio_sharpe": "收益和波动的性价比，通常越高代表风险回报比越好。",
    "daily_portfolio_max_drawdown": "组合历史上最深的一次回撤，绝对值越小越稳。",
    "avg_turnover_ratio": "看每次调仓大概换掉多少仓位，越低通常越容易落地，也更不容易被交易成本吃掉收益。",
}


st.set_page_config(page_title="A股研究平台", layout="wide")


def zh(name: str) -> str:
    return COLUMN_LABELS.get(name, name)


def explain(name: str) -> str:
    return FIELD_EXPLANATIONS.get(name, "该字段暂时还没有补充说明。")


def prettify_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.rename(columns={column: zh(column) for column in frame.columns})


def _read_json(path: Path) -> dict:
    return repo_read_json(path)

@st.cache_data(show_spinner=False)
def _read_jsonl_records(path_text: str) -> list[dict]:
    if not path_text:
        return []
    if str(path_text).startswith("artifact://"):
        return []
    path = Path(path_text)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

def _tail_text(path: Path, lines: int = 20) -> str:
    if not path.exists():
        return ""
    content = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return "\n".join(content[-lines:])


def _file_summary(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"exists": False}
    stat = path.stat()
    return {
        "exists": True,
        "size_mb": round(stat.st_size / (1024 * 1024), 2),
        "updated": pd.Timestamp(stat.st_mtime, unit="s"),
    }


@st.cache_data(show_spinner=False)
def _load_dataset_summary() -> dict[str, object]:
    return shared_load_dataset_summary()


@st.cache_data(show_spinner=False)
def _load_feature_panel() -> pd.DataFrame:
    return shared_load_feature_panel()


@st.cache_data(show_spinner=False)
def _load_label_panel() -> pd.DataFrame:
    return repo_load_label_panel(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_daily_bar() -> pd.DataFrame:
    return shared_load_daily_bar()


def _load_metrics(model_name: str, split_name: str) -> dict:
    return shared_load_metrics(model_name, split_name)


@st.cache_data(show_spinner=False)
def _load_stability(model_name: str) -> dict:
    return shared_load_stability(model_name)


@st.cache_data(show_spinner=False)
def _metrics_table() -> pd.DataFrame:
    return shared_build_metrics_table()


@st.cache_data(show_spinner=False)
def _load_portfolio(model_name: str, split_name: str) -> pd.DataFrame:
    return shared_load_portfolio(model_name, split_name)


@st.cache_data(show_spinner=False)
def _load_predictions(model_name: str, split_name: str) -> pd.DataFrame:
    return shared_load_predictions(model_name, split_name)


@st.cache_data(show_spinner=False)
def _load_feature_importance(model_name: str) -> pd.DataFrame:
    return shared_load_feature_importance(model_name)


@st.cache_data(show_spinner=False)
def _load_diagnostic_table(model_name: str, split_name: str, table_name: str) -> pd.DataFrame:
    return shared_load_diagnostic_table(model_name, split_name, table_name)


@st.cache_data(show_spinner=False)
def _load_overlay_candidates() -> pd.DataFrame:
    return shared_load_overlay_candidates()


@st.cache_data(show_spinner=False)
def _load_overlay_packet() -> dict:
    return shared_load_overlay_packet()


@st.cache_data(show_spinner=False)
def _load_overlay_brief() -> str:
    return shared_load_overlay_brief()


@st.cache_data(show_spinner=False)
def _load_overlay_inference_candidates() -> pd.DataFrame:
    return shared_load_overlay_inference_candidates()


@st.cache_data(show_spinner=False)
def _load_overlay_inference_packet() -> dict:
    return shared_load_overlay_inference_packet()


@st.cache_data(show_spinner=False)
def _load_overlay_inference_brief() -> str:
    return shared_load_overlay_inference_brief()


@st.cache_data(show_spinner=False)
def _load_latest_symbol_markdown(symbol: str, note_kind: str) -> dict[str, str]:
    return shared_load_latest_symbol_markdown(symbol, note_kind)


@st.cache_data(show_spinner=False)
def _load_latest_watch_plan(symbol: str) -> dict[str, str]:
    return _load_latest_symbol_markdown(symbol, "watch_plan")


@st.cache_data(show_spinner=False)
def _load_latest_action_memo(symbol: str) -> dict[str, str]:
    return _load_latest_symbol_markdown(symbol, "action_memo")


@st.cache_data(show_spinner=False, ttl=20)
def _load_realtime_quotes(
    symbols: tuple[str, ...],
    previous_close_items: tuple[tuple[str, float | None], ...],
    trade_date_text: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    previous_close_lookup: dict[str, float] = {}
    for symbol, value in previous_close_items:
        numeric_value = pd.to_numeric(value, errors="coerce")
        if pd.notna(numeric_value):
            previous_close_lookup[str(symbol)] = float(numeric_value)

    return fetch_realtime_quotes(
        list(symbols),
        previous_close_lookup=previous_close_lookup,
        trade_date=pd.Timestamp(trade_date_text),
    )

def _build_watchlist_view(
    watchlist_config: dict,
    daily_bar: pd.DataFrame,
    ridge_predictions: pd.DataFrame,
    lgbm_predictions: pd.DataFrame,
    ensemble_predictions: pd.DataFrame,
    overlay_candidates: pd.DataFrame,
    ensemble_inference_predictions: pd.DataFrame,
    overlay_inference_candidates: pd.DataFrame,
) -> pd.DataFrame:
    return svc_build_watchlist_view(
        root=ROOT,
        data_source=ACTIVE_DATA_SOURCE,
        watchlist_config=watchlist_config,
        daily_bar=daily_bar,
        ridge_predictions=ridge_predictions,
        lgbm_predictions=lgbm_predictions,
        ensemble_predictions=ensemble_predictions,
        overlay_candidates=overlay_candidates,
        ensemble_inference_predictions=ensemble_inference_predictions,
        overlay_inference_candidates=overlay_inference_candidates,
    )


def _downloader_status() -> dict[str, object]:
    pid_path = STAGING_DIR / "akshare_download.pid"
    stdout_path = LOG_DIR / "akshare_download.out.log"
    stderr_path = LOG_DIR / "akshare_download.err.log"
    status = {
        "pid": None,
        "running": False,
        "state_label": "未运行",
        "stale_pid": False,
        "stdout_tail": _tail_text(stdout_path),
        "stderr_tail": _tail_text(stderr_path),
    }
    if pid_path.exists():
        pid = pid_path.read_text(encoding="utf-8").strip()
        status["pid"] = pid
        if pid.isdigit():
            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    f"if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ 'running' }} else {{ 'stopped' }}",
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                check=False,
            )
            status["running"] = "running" in result.stdout.lower()
            status["stale_pid"] = not status["running"]

    if status["running"]:
        status["state_label"] = "运行中"
    elif "Saved AKShare market panel" in status["stdout_tail"]:
        status["state_label"] = "已完成"
    elif status["stale_pid"]:
        status["state_label"] = "已完成(残留PID)"
    return status


def _join_log_lines(value: object) -> str:
    if isinstance(value, list):
        return "\n".join(str(item) for item in value if str(item).strip())
    if value is None:
        return ""
    return str(value)


def _streamlit_service_status() -> dict[str, object]:
    status = {
        "supervisor_pid": None,
        "streamlit_pid": None,
        "supervisor_running": False,
        "streamlit_running": False,
        "listener_present": False,
        "listener_pids": [],
        "effective_state": "unknown",
        "status_label": "未知",
        "stale_supervisor_pid": False,
        "stale_streamlit_pid": False,
        "stale_status": False,
        "last_status": None,
        "status_label_display": "未知",
        "out_log_tail": "",
        "err_log_tail": "",
    }
    if not STREAMLIT_STATUS_SCRIPT_PATH.exists():
        return status

    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(STREAMLIT_STATUS_SCRIPT_PATH),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        status["status_label"] = "状态脚本失败"
        if result.stderr:
            status["err_log_tail"] = result.stderr.strip()
        return status

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        status["status_label"] = "状态解析失败"
        status["err_log_tail"] = result.stdout.strip()[-2000:]
        return status

    status.update(payload)
    listener_pids = status.get("listener_pids")
    if listener_pids is None:
        status["listener_pids"] = []
    elif isinstance(listener_pids, list):
        status["listener_pids"] = listener_pids
    else:
        status["listener_pids"] = [listener_pids]
    label_map = {
        "running": "运行中",
        "starting": "启动中",
        "stopped": "已停止",
        "port_busy": "端口被占用",
        "listener_without_supervisor": "端口监听中(无守护进程)",
    }
    status["status_label_display"] = label_map.get(str(status.get("status_label", "")), str(status.get("status_label", "未知")))
    status["out_log_tail"] = _join_log_lines(status.get("out_log_tail"))
    status["err_log_tail"] = _join_log_lines(status.get("err_log_tail"))
    return status


def _run_module(module_name: str) -> tuple[bool, str]:
    return shared_run_module(module_name)


def _refresh_cached_views() -> None:
    shared_clear_dashboard_data_caches()
    _load_dataset_summary.clear()
    _load_experiment_config.clear()
    _read_jsonl_records.clear()
    _load_realtime_quotes.clear()
    _load_feature_panel.clear()
    _load_label_panel.clear()
    _load_daily_bar.clear()
    _metrics_table.clear()
    _load_portfolio.clear()
    _load_predictions.clear()
    _load_stability.clear()
    _load_feature_importance.clear()
    _load_diagnostic_table.clear()
    _load_overlay_candidates.clear()
    _load_overlay_packet.clear()
    _load_overlay_brief.clear()
    _load_overlay_inference_candidates.clear()
    _load_overlay_inference_packet.clear()
    _load_overlay_inference_brief.clear()
    _load_latest_symbol_markdown.clear()
    _load_latest_watch_plan.clear()
    _load_latest_action_memo.clear()
    for key in (
        "watchlist_realtime_context",
        "watchlist_realtime_quotes",
        "watchlist_realtime_status",
    ):
        st.session_state.pop(key, None)


def _empty_realtime_status(*, trade_date_text: str | None = None) -> dict[str, object]:
    resolved_trade_date = trade_date_text or pd.Timestamp.now(tz="Asia/Shanghai").date().isoformat()
    return {
        "available": False,
        "source": "",
        "trade_date": resolved_trade_date,
        "fetched_at": "",
        "requested_symbol_count": 0,
        "success_symbol_count": 0,
        "failed_symbols": [],
        "error_message": "",
    }


def _set_watchlist_realtime_context(
    symbols: tuple[str, ...],
    previous_close_items: tuple[tuple[str, float | None], ...],
    trade_date_text: str,
) -> None:
    context = {
        "symbols": symbols,
        "previous_close_items": previous_close_items,
        "trade_date_text": trade_date_text,
    }
    previous_context = st.session_state.get("watchlist_realtime_context")
    if previous_context != context:
        st.session_state["watchlist_realtime_context"] = context
        st.session_state["watchlist_realtime_quotes"] = pd.DataFrame()
        st.session_state["watchlist_realtime_status"] = _empty_realtime_status(trade_date_text=trade_date_text)


def _get_watchlist_realtime_state() -> tuple[pd.DataFrame, dict[str, object]]:
    context = st.session_state.get("watchlist_realtime_context") or {}
    trade_date_text = str(context.get("trade_date_text") or pd.Timestamp.now(tz="Asia/Shanghai").date().isoformat())
    quotes = st.session_state.get("watchlist_realtime_quotes")
    if not isinstance(quotes, pd.DataFrame):
        quotes = pd.DataFrame()
    status = st.session_state.get("watchlist_realtime_status")
    if not isinstance(status, dict):
        status = _empty_realtime_status(trade_date_text=trade_date_text)
    return quotes, status


def _refresh_realtime_quotes() -> None:
    context = st.session_state.get("watchlist_realtime_context") or {}
    symbols = tuple(context.get("symbols", ()) or ())
    previous_close_items = tuple(context.get("previous_close_items", ()) or ())
    trade_date_text = str(context.get("trade_date_text") or pd.Timestamp.now(tz="Asia/Shanghai").date().isoformat())
    if not symbols:
        st.session_state["watchlist_realtime_quotes"] = pd.DataFrame()
        st.session_state["watchlist_realtime_status"] = _empty_realtime_status(trade_date_text=trade_date_text)
        return

    _load_realtime_quotes.clear()
    quotes, status = _load_realtime_quotes(symbols, previous_close_items, trade_date_text)
    st.session_state["watchlist_realtime_quotes"] = quotes
    st.session_state["watchlist_realtime_status"] = status


def _render_action_result() -> None:
    if "last_action" not in st.session_state:
        return
    action_name, ok, output = st.session_state["last_action"]
    st.subheader(f"最近一次操作：{ACTION_LABELS.get(action_name, action_name)}")
    (st.success if ok else st.error)("执行成功" if ok else "执行失败")
    if output:
        st.code(output[-6000:], language="text")


def _symbol_history(feature_panel: pd.DataFrame, symbol: str, factor_name: str, tail_n: int = 240) -> pd.DataFrame:
    scoped = feature_panel.loc[feature_panel["ts_code"] == symbol, ["trade_date", factor_name]].copy()
    scoped = scoped.dropna().sort_values("trade_date").tail(tail_n)
    return scoped.set_index("trade_date")


def _render_overview_workspace(*, watchlist_entry_count: int) -> None:
    requested_split = str(st.session_state.get("overview_split", "test") or "test")
    payload = get_overview_payload(requested_split if requested_split in SPLITS else "test")
    render_overview_payload_page(
        payload=payload,
        watchlist_count=watchlist_entry_count,
        splits=SPLITS,
        split_labels=SPLIT_LABELS,
        metric_explanations=METRIC_EXPLANATIONS,
        zh=zh,
        prettify_dataframe=prettify_dataframe,
    )


def _render_factor_explorer_workspace() -> None:
    payload = get_factor_explorer_payload(
        factor_name=str(st.session_state.get("factor_name", "") or "") or None,
        symbol=str(st.session_state.get("factor_symbol", "") or "") or None,
        history_factor=str(st.session_state.get("history_factor", "") or "") or None,
    )
    render_factor_explorer_payload_page(
        payload=payload,
        zh=zh,
        explain=explain,
        prettify_dataframe=prettify_dataframe,
    )


def _render_model_backtest_workspace(*, experiment_config: dict) -> None:
    model_name = str(st.session_state.get("lab_model", "lgbm") or "lgbm")
    split_name = str(st.session_state.get("lab_split", "test") or "test")
    payload = get_model_backtest_payload(
        model_name=model_name if model_name in MODEL_NAMES else "lgbm",
        split_name=split_name if split_name in SPLITS else "test",
    )
    render_model_backtest_payload_page(
        payload=payload,
        experiment_config=experiment_config,
        model_names=MODEL_NAMES,
        splits=SPLITS,
        model_labels=MODEL_LABELS,
        split_labels=SPLIT_LABELS,
        metric_explanations=METRIC_EXPLANATIONS,
        zh=zh,
        prettify_dataframe=prettify_dataframe,
    )


def _render_candidates_workspace(*, experiment_config: dict) -> None:
    model_name = str(st.session_state.get("pick_model", "lgbm") or "lgbm")
    split_name = str(st.session_state.get("pick_split", "test") or "test")
    top_n = int(st.session_state.get("pick_top_n", experiment_config.get("top_n", 10)) or experiment_config.get("top_n", 10) or 10)
    symbol = str(st.session_state.get("inspect_symbol", "") or "") or None
    payload = get_candidates_payload(
        model_name=model_name if model_name in MODEL_NAMES else "lgbm",
        split_name=split_name if split_name in SPLITS else "test",
        top_n=max(1, top_n),
        symbol=symbol,
    )
    render_candidates_payload_page(
        payload=payload,
        experiment_config=experiment_config,
        model_names=MODEL_NAMES,
        splits=SPLITS,
        model_labels=MODEL_LABELS,
        split_labels=SPLIT_LABELS,
        prettify_dataframe=prettify_dataframe,
    )


def _render_watchlist_workspace() -> None:
    selected_scope_label = str(st.session_state.get("watchlist_scope", "全部") or "全部")
    selected_sort_label = str(st.session_state.get("watchlist_sort", "最新推理排名") or "最新推理排名")
    scope_lookup = {label: key for key, label in SHARED_WATCH_SCOPE_MAP.items()}
    sort_lookup = {label: key for key, label in SHARED_WATCH_SORT_MAP.items()}
    payload = get_watchlist_payload(
        keyword=str(st.session_state.get("watchlist_keyword", "") or ""),
        scope=scope_lookup.get(selected_scope_label, "all"),
        sort_by=sort_lookup.get(selected_sort_label, "inference_rank"),
        symbol=str(st.session_state.get("watchlist_symbol", "") or "") or None,
    )
    refresh_symbols = tuple(str(symbol) for symbol in (payload.get("refreshSymbols") or []))
    refresh_previous_closes = tuple(
        (str(symbol), None if value is None else float(value))
        for symbol, value in dict(payload.get("refreshPreviousCloses", {}) or {}).items()
    )
    trade_date_text = pd.Timestamp.now(tz="Asia/Shanghai").date().isoformat()
    _set_watchlist_realtime_context(refresh_symbols, refresh_previous_closes, trade_date_text)
    realtime_quotes, realtime_quote_status = _get_watchlist_realtime_state()
    if isinstance(realtime_quote_status, dict):
        payload = apply_realtime_to_watchlist_payload(
            payload,
            realtime_quotes=realtime_quotes,
            realtime_status=realtime_quote_status,
        )
    render_watchlist_payload_page(
        payload=payload,
        prettify_dataframe=prettify_dataframe,
        zh=zh,
        run_module=_run_module,
        refresh_cached_views=_refresh_cached_views,
        refresh_realtime_quotes=_refresh_realtime_quotes,
    )


def _render_ai_review_workspace() -> None:
    payload = get_ai_review_payload(
        inference_symbol=str(st.session_state.get("overlay_inference_symbol", "") or "") or None,
        historical_symbol=str(st.session_state.get("overlay_symbol", "") or "") or None,
    )
    render_ai_review_payload_page(
        payload=payload,
        prettify_dataframe=prettify_dataframe,
    )


def _build_workspace_registry(*, experiment_config: dict, watchlist_entry_count: int, streamlit_service_status: dict[str, object]) -> list[PageDefinition]:
    return build_page_registry(
        PageDefinition(
            key="overview",
            label=PAGE_OVERVIEW,
            render=lambda: _render_overview_workspace(watchlist_entry_count=watchlist_entry_count),
        ),
        PageDefinition(
            key="factor_explorer",
            label=PAGE_FACTOR_EXPLORER,
            render=_render_factor_explorer_workspace,
        ),
        PageDefinition(
            key="model_backtest",
            label=PAGE_MODEL_BACKTEST,
            render=lambda: _render_model_backtest_workspace(experiment_config=experiment_config),
        ),
        PageDefinition(
            key="candidates",
            label=PAGE_CANDIDATES,
            render=lambda: _render_candidates_workspace(experiment_config=experiment_config),
        ),
        PageDefinition(
            key="watchlist",
            label=PAGE_WATCHLIST,
            render=_render_watchlist_workspace,
        ),
        PageDefinition(
            key="ai_review",
            label=PAGE_AI_REVIEW,
            render=_render_ai_review_workspace,
        ),
        PageDefinition(
            key="service",
            label=PAGE_SERVICE,
            render=lambda: render_service_page(streamlit_service_status),
        ),
    )



shell_payload = get_shell_payload()
streamlit_service_status = shell_payload["service"]
experiment_config = shell_payload["experimentConfig"]
watchlist_entry_count = int(shell_payload["watchlistEntryCount"])
page_registry = _build_workspace_registry(
    experiment_config=experiment_config,
    watchlist_entry_count=watchlist_entry_count,
    streamlit_service_status=streamlit_service_status,
)
current_page = render_app_shell(page_labels(page_registry))
render_sidebar(
    shell_payload=shell_payload,
    zh=zh,
    save_experiment_config=update_experiment_config_payload,
    refresh_cached_views=_refresh_cached_views,
    run_named_action=run_named_action,
    clear_cache=clear_cache_payload,
    render_action_result=_render_action_result,
)
st.caption(build_current_config_caption(str(shell_payload.get("configSummaryText", "") or "")))
render_registered_page(page_registry, current_page)
