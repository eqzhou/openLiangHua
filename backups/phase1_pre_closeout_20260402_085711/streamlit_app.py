from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

from src.app.pages.watchlist_page import render_watchlist_page
from src.app.repositories.config_repository import (
    load_experiment_config as repo_load_experiment_config,
    load_watchlist_config as repo_load_watchlist_config,
    save_experiment_config as repo_save_experiment_config,
)
from src.app.repositories.report_repository import (
    load_daily_bar as repo_load_daily_bar,
    load_dataset_summary as repo_load_dataset_summary,
    load_diagnostic_table as repo_load_diagnostic_table,
    load_feature_importance as repo_load_feature_importance,
    load_feature_panel as repo_load_feature_panel,
    load_label_panel as repo_load_label_panel,
    load_latest_symbol_markdown as repo_load_latest_symbol_markdown,
    load_metrics as repo_load_metrics,
    load_overlay_brief as repo_load_overlay_brief,
    load_overlay_candidates as repo_load_overlay_candidates,
    load_overlay_inference_brief as repo_load_overlay_inference_brief,
    load_overlay_inference_candidates as repo_load_overlay_inference_candidates,
    load_overlay_inference_packet as repo_load_overlay_inference_packet,
    load_overlay_packet as repo_load_overlay_packet,
    load_portfolio as repo_load_portfolio,
    load_predictions as repo_load_predictions,
    load_stability as repo_load_stability,
    read_json as repo_read_json,
    read_jsonl_records as repo_read_jsonl_records,
    read_text as repo_read_text,
)
from src.app.services.watchlist_service import build_watchlist_view as svc_build_watchlist_view
from src.utils.data_source import active_data_source, source_or_canonical_path
from src.utils.holding_marks import describe_price_reference
from src.utils.llm_discussion import discussion_round_rows, load_symbol_discussion_snapshot
from src.utils.premarket_plan import build_premarket_plan
from src.utils.prediction_snapshot import latest_prediction_details

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


def _read_text(path: Path) -> str:
    return repo_read_text(path)


@st.cache_data(show_spinner=False)
def _read_jsonl_records(path_text: str) -> list[dict]:
    if not path_text:
        return []
    return repo_read_jsonl_records(Path(path_text))


@st.cache_data(show_spinner=False)
def _load_llm_discussion_snapshot(symbol: str) -> dict:
    return load_symbol_discussion_snapshot(ROOT, ACTIVE_DATA_SOURCE, symbol)


@st.cache_data(show_spinner=False)
def _load_experiment_config() -> dict:
    return repo_load_experiment_config(ROOT)


def _save_experiment_config(config: dict) -> None:
    repo_save_experiment_config(config, ROOT)


@st.cache_data(show_spinner=False)
def _load_watchlist_config() -> dict:
    return repo_load_watchlist_config(ROOT)


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
    return repo_load_dataset_summary(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_feature_panel() -> pd.DataFrame:
    return repo_load_feature_panel(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_label_panel() -> pd.DataFrame:
    return repo_load_label_panel(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_daily_bar() -> pd.DataFrame:
    return repo_load_daily_bar(ROOT, data_source=ACTIVE_DATA_SOURCE)


def _load_metrics(model_name: str, split_name: str) -> dict:
    return repo_load_metrics(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name, split_name=split_name)


@st.cache_data(show_spinner=False)
def _load_stability(model_name: str) -> dict:
    return repo_load_stability(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name)


@st.cache_data(show_spinner=False)
def _metrics_table() -> pd.DataFrame:
    rows: list[dict] = []
    for model_name in MODEL_NAMES:
        for split_name in SPLITS:
            metrics = _load_metrics(model_name, split_name)
            if metrics:
                row = {"model": MODEL_LABELS[model_name], "split": SPLIT_LABELS[split_name]}
                row.update(metrics)
                rows.append(row)
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def _load_portfolio(model_name: str, split_name: str) -> pd.DataFrame:
    return repo_load_portfolio(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name, split_name=split_name)


@st.cache_data(show_spinner=False)
def _load_predictions(model_name: str, split_name: str) -> pd.DataFrame:
    return repo_load_predictions(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name, split_name=split_name)


@st.cache_data(show_spinner=False)
def _load_feature_importance(model_name: str) -> pd.DataFrame:
    return repo_load_feature_importance(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name)


@st.cache_data(show_spinner=False)
def _load_diagnostic_table(model_name: str, split_name: str, table_name: str) -> pd.DataFrame:
    return repo_load_diagnostic_table(
        ROOT,
        data_source=ACTIVE_DATA_SOURCE,
        model_name=model_name,
        split_name=split_name,
        table_name=table_name,
    )


@st.cache_data(show_spinner=False)
def _load_overlay_candidates() -> pd.DataFrame:
    return repo_load_overlay_candidates(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_overlay_packet() -> dict:
    return repo_load_overlay_packet(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_overlay_brief() -> str:
    return repo_load_overlay_brief(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_overlay_inference_candidates() -> pd.DataFrame:
    return repo_load_overlay_inference_candidates(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_overlay_inference_packet() -> dict:
    return repo_load_overlay_inference_packet(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_overlay_inference_brief() -> str:
    return repo_load_overlay_inference_brief(ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_latest_symbol_markdown(symbol: str, note_kind: str) -> dict[str, str]:
    return repo_load_latest_symbol_markdown(symbol, note_kind, root=ROOT, data_source=ACTIVE_DATA_SOURCE)


@st.cache_data(show_spinner=False)
def _load_latest_watch_plan(symbol: str) -> dict[str, str]:
    return _load_latest_symbol_markdown(symbol, "watch_plan")


@st.cache_data(show_spinner=False)
def _load_latest_action_memo(symbol: str) -> dict[str, str]:
    return _load_latest_symbol_markdown(symbol, "action_memo")


def _latest_symbol_bar(daily_bar: pd.DataFrame, symbol: str) -> tuple[pd.Series | None, pd.Series | None]:
    scoped = daily_bar.loc[daily_bar["ts_code"] == symbol].copy()
    if scoped.empty:
        return None, None
    scoped = scoped.sort_values("trade_date")
    latest_row = scoped.iloc[-1]
    valid_close = scoped.loc[pd.to_numeric(scoped["close"], errors="coerce").notna()]
    latest_valid = valid_close.iloc[-1] if not valid_close.empty else latest_row
    return latest_row, latest_valid

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

    holdings = watchlist_config.get("holdings", []) or []
    if not holdings:
        return pd.DataFrame()

    overlay_symbols = set()
    if not overlay_candidates.empty and "ts_code" in overlay_candidates.columns:
        overlay_symbols = set(overlay_candidates["ts_code"].astype(str).tolist())
    inference_overlay_symbols = set()
    if not overlay_inference_candidates.empty and "ts_code" in overlay_inference_candidates.columns:
        inference_overlay_symbols = set(overlay_inference_candidates["ts_code"].astype(str).tolist())

    rows: list[dict[str, object]] = []
    for item in holdings:
        symbol = str(item.get("ts_code", "") or "").strip()
        if not symbol:
            continue

        latest_row, latest_valid = _latest_symbol_bar(daily_bar, symbol=symbol)
        ridge_info = latest_prediction_details(ridge_predictions, symbol=symbol)
        lgbm_info = latest_prediction_details(lgbm_predictions, symbol=symbol)
        ensemble_info = latest_prediction_details(ensemble_predictions, symbol=symbol)
        inference_ensemble_info = latest_prediction_details(ensemble_inference_predictions, symbol=symbol)
        discussion_snapshot = _load_llm_discussion_snapshot(symbol)

        base_name = str(item.get("name") or "")
        if not base_name and latest_valid is not None and "name" in latest_valid.index:
            base_name = str(latest_valid.get("name") or symbol)
        if not base_name and inference_ensemble_info.get("name"):
            base_name = str(inference_ensemble_info.get("name") or symbol)

        price_source = "最新有效收盘价"
        mark_price = None
        mark_date = None
        manual_mark_price = item.get("manual_mark_price")
        latest_bar_date = pd.to_datetime(latest_valid.get("trade_date"), errors="coerce") if latest_valid is not None else pd.NaT
        latest_bar_close = (
            float(pd.to_numeric(latest_valid.get("close"), errors="coerce"))
            if latest_valid is not None and pd.notna(pd.to_numeric(latest_valid.get("close"), errors="coerce"))
            else None
        )
        if manual_mark_price not in (None, ""):
            mark_price = float(manual_mark_price)
            mark_date = pd.to_datetime(item.get("manual_mark_date"), errors="coerce")
            price_source = str(item.get("manual_mark_note") or "手工标记价格")
        elif latest_valid is not None:
            latest_close = pd.to_numeric(latest_valid.get("close"), errors="coerce")
            if pd.notna(latest_close):
                mark_price = float(latest_close)
                mark_date = pd.to_datetime(latest_valid.get("trade_date"), errors="coerce")

        is_manual_mark = manual_mark_price not in (None, "")
        mark_reference = describe_price_reference(
            is_manual_mark=is_manual_mark,
            mark_date=mark_date,
            latest_bar_date=latest_bar_date,
        )
        cost_basis = float(item.get("cost", 0.0) or 0.0)
        shares = int(item.get("shares", 0) or 0)
        market_value = float(mark_price * shares) if mark_price is not None else None
        unrealized_pnl = float((mark_price - cost_basis) * shares) if mark_price is not None else None
        unrealized_pnl_pct = float(mark_price / cost_basis - 1.0) if mark_price is not None and cost_basis else None
        breakeven_price = cost_basis if cost_basis else None
        gap_to_breakeven_pct = float(cost_basis / mark_price - 1.0) if mark_price and cost_basis else None
        halfway_recovery_price = (
            float(mark_price + 0.5 * (cost_basis - mark_price))
            if mark_price is not None and cost_basis
            else None
        )
        defensive_price = float(round(mark_price * 0.9768, 2)) if mark_price is not None else None

        ensemble_rank_pct = ensemble_info.get("rank_pct")
        inference_rank_pct = inference_ensemble_info.get("rank_pct")
        watch_level = "普通观察"
        action_brief = "先看量价是否继续配合，再决定是否转强。"
        if mark_price is not None and cost_basis:
            if ensemble_rank_pct is not None and float(ensemble_rank_pct) >= 0.9:
                watch_level = "高分修复观察"
                action_brief = (
                    f"系统评分较强，先看 {mark_price:.2f} 一带能否站稳，再看 {halfway_recovery_price:.2f} 和回本位 {cost_basis:.3f}。"
                )
            elif ensemble_rank_pct is not None and float(ensemble_rank_pct) >= 0.75:
                watch_level = "中等强度观察"
                action_brief = (
                    f"已有修复迹象，但还没到最强区，重点观察 {defensive_price:.2f} 防守和 {cost_basis:.3f} 解套压力。"
                )
            elif unrealized_pnl_pct is not None and float(unrealized_pnl_pct) <= -0.15:
                watch_level = "弱势风险观察"
                action_brief = (
                    f"系统排序不高且浮亏较深，若连 {defensive_price:.2f} 都守不住，短线压力会明显增大。"
                )

        ranking_note_parts: list[str] = []
        if ensemble_info.get("rank") is not None:
            ranking_note_parts.append(
                f"历史验证 {int(ensemble_info['rank'])}/{int(ensemble_info.get('universe_size') or 0)}"
            )
        if inference_ensemble_info.get("rank") is not None:
            ranking_note_parts.append(
                f"最新推理 {int(inference_ensemble_info['rank'])}/{int(inference_ensemble_info.get('universe_size') or 0)}"
            )
        ranking_note = " | ".join(ranking_note_parts)
        premarket_plan_payload = build_premarket_plan(
            discussion_snapshot=discussion_snapshot,
            action_brief=action_brief,
            anchor_price=mark_price,
            defensive_price=defensive_price,
            breakeven_price=breakeven_price,
        )

        rows.append(
            {
                "ts_code": symbol,
                "name": base_name or symbol,
                "premarket_plan": premarket_plan_payload.get("premarket_plan", ""),
                "premarket_plan_source": premarket_plan_payload.get("premarket_plan_source", ""),
                "industry": (
                    ensemble_info.get("industry")
                    or inference_ensemble_info.get("industry")
                    or ridge_info.get("industry")
                    or lgbm_info.get("industry")
                    or ""
                ),
                "cost_basis": cost_basis,
                "shares": shares,
                "mark_price": mark_price,
                "mark_date": mark_date,
                "price_source": price_source,
                "latest_bar_date": latest_bar_date,
                "latest_bar_close": latest_bar_close,
                "mark_status": mark_reference.get("mark_status"),
                "mark_status_note": mark_reference.get("mark_status_note"),
                "mark_vs_latest_bar_days": mark_reference.get("mark_vs_latest_bar_days"),
                "is_manual_mark": is_manual_mark,
                "market_value": market_value,
                "unrealized_pnl": unrealized_pnl,
                "unrealized_pnl_pct": unrealized_pnl_pct,
                "breakeven_price": breakeven_price,
                "gap_to_breakeven_pct": gap_to_breakeven_pct,
                "halfway_recovery_price": halfway_recovery_price,
                "defensive_price": defensive_price,
                "signal_date": ensemble_info.get("signal_date") or ridge_info.get("signal_date") or lgbm_info.get("signal_date"),
                "ensemble_rank": ensemble_info.get("rank"),
                "ensemble_rank_pct": ensemble_info.get("rank_pct"),
                "inference_signal_date": inference_ensemble_info.get("signal_date"),
                "inference_ensemble_rank": inference_ensemble_info.get("rank"),
                "inference_ensemble_rank_pct": inference_rank_pct,
                "inference_score": inference_ensemble_info.get("score"),
                "ridge_rank_pct": ridge_info.get("rank_pct"),
                "lgbm_rank_pct": lgbm_info.get("rank_pct"),
                "score": ensemble_info.get("score"),
                "score_raw": ensemble_info.get("score_raw"),
                "mom_5": ensemble_info.get("mom_5"),
                "mom_20": ensemble_info.get("mom_20"),
                "mom_60": ensemble_info.get("mom_60"),
                "close_to_ma_20": ensemble_info.get("close_to_ma_20"),
                "close_to_ma_60": ensemble_info.get("close_to_ma_60"),
                "drawdown_60": ensemble_info.get("drawdown_60"),
                "pct_chg": latest_valid.get("pct_chg") if latest_valid is not None else None,
                "is_overlay_selected": symbol in overlay_symbols,
                "is_inference_overlay_selected": symbol in inference_overlay_symbols,
                "watch_level": watch_level,
                "action_brief": action_brief,
                "ranking_note": ranking_note,
                "llm_round_count": discussion_snapshot.get("round_count", 0),
                "llm_selected_round_count": discussion_snapshot.get("selected_round_count", 0),
                "llm_success_round_count": discussion_snapshot.get("success_round_count", 0),
                "llm_latest_round": discussion_snapshot.get("latest_round_label", ""),
                "llm_latest_status": discussion_snapshot.get("latest_status", ""),
                "llm_latest_summary": discussion_snapshot.get("latest_summary", ""),
                "llm_overview": discussion_snapshot.get("overview", ""),
                "llm_discussion_snapshot": discussion_snapshot,
                "manual_mark_note": item.get("manual_mark_note", ""),
            }
        )

    return pd.DataFrame(rows)


def _build_reduce_plan(row: pd.Series) -> pd.DataFrame:
    if pd.isna(row.get("mark_price")) or pd.isna(row.get("cost_basis")) or pd.isna(row.get("shares")):
        return pd.DataFrame()

    mark_price = float(row["mark_price"])
    cost_basis = float(row["cost_basis"])
    shares = int(row["shares"])

    stages = [
        ("阶段一", float(row.get("halfway_recovery_price") or 0.0), 0.30, "先看修复是否延续"),
        ("阶段二", float(row.get("breakeven_price") or 0.0), 0.40, "接近回本区，观察抛压"),
        ("阶段三", round(cost_basis * 1.065, 2), 0.30, "明显超预期时再看"),
    ]

    plan_rows: list[dict[str, object]] = []
    for label, target_price, ratio, note in stages:
        raw_shares = shares * ratio
        target_shares = int(round(raw_shares / 100.0) * 100) if shares >= 100 else int(round(raw_shares))
        target_shares = max(target_shares, 0)
        distance_pct = (target_price / mark_price - 1.0) if mark_price else None
        realized_pnl = (target_price - cost_basis) * target_shares if target_shares else 0.0
        plan_rows.append(
            {
                "plan_stage": label,
                "target_price": target_price,
                "reduce_ratio": ratio,
                "target_shares": target_shares,
                "distance_from_mark_pct": distance_pct,
                "estimated_realized_pnl": realized_pnl,
                "plan_note": note,
            }
        )

    return pd.DataFrame(plan_rows)


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
    python_executable = str(PROJECT_PYTHON if PROJECT_PYTHON.exists() else Path(sys.executable))
    command = [python_executable, "-m", module_name]
    result = subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    output = result.stdout
    if result.stderr:
        output = f"{output}\n{result.stderr}".strip()
    return result.returncode == 0, output.strip()


def _refresh_cached_views() -> None:
    _load_dataset_summary.clear()
    _load_experiment_config.clear()
    _load_watchlist_config.clear()
    _read_jsonl_records.clear()
    _load_llm_discussion_snapshot.clear()
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


def _render_action_result() -> None:
    if "last_action" not in st.session_state:
        return
    action_name, ok, output = st.session_state["last_action"]
    st.subheader(f"最近一次操作：{ACTION_LABELS.get(action_name, action_name)}")
    (st.success if ok else st.error)("执行成功" if ok else "执行失败")
    if output:
        st.code(output[-6000:], language="text")


def _latest_snapshot(predictions: pd.DataFrame, top_n: int) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()
    latest_date = predictions["trade_date"].max()
    snapshot = predictions.loc[predictions["trade_date"] == latest_date].copy()
    return snapshot.sort_values("score", ascending=False).head(top_n)


def _filtered_watchlist_view(
    frame: pd.DataFrame,
    *,
    keyword: str,
    scope: str,
    sort_by: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame

    filtered = frame.copy()
    normalized_keyword = str(keyword or "").strip().lower()
    if normalized_keyword:
        ts_code_text = filtered["ts_code"].astype(str).str.lower()
        name_text = filtered["name"].astype(str).str.lower()
        filtered = filtered.loc[ts_code_text.str.contains(normalized_keyword) | name_text.str.contains(normalized_keyword)].copy()

    if scope == "只看 AI 精选":
        filtered = filtered.loc[filtered["is_overlay_selected"].fillna(False)].copy()
    elif scope == "只看最新推理池":
        filtered = filtered.loc[filtered["is_inference_overlay_selected"].fillna(False)].copy()
    elif scope == "只看浮亏较大":
        filtered = filtered.loc[pd.to_numeric(filtered["unrealized_pnl_pct"], errors="coerce") <= -0.1].copy()

    sort_map = {
        "最新推理排名": ("inference_ensemble_rank", True),
        "历史验证排名": ("ensemble_rank", True),
        "浮亏比例": ("unrealized_pnl_pct", True),
        "参考市值": ("market_value", False),
    }
    sort_column, ascending = sort_map.get(sort_by, ("inference_ensemble_rank", True))
    if sort_column in filtered.columns:
        numeric_sort = pd.to_numeric(filtered[sort_column], errors="coerce")
        filtered = filtered.assign(_sort_value=numeric_sort)
        na_position = "last"
        filtered = filtered.sort_values("_sort_value", ascending=ascending, na_position=na_position).drop(columns="_sort_value")
    return filtered.reset_index(drop=True)


def _render_overlay_panel(
    *,
    title: str,
    candidates: pd.DataFrame,
    packet: dict,
    brief: str,
    empty_message: str,
    inspect_key: str,
) -> None:
    st.markdown(f"**{title}**")
    if candidates.empty:
        st.info(empty_message)
        return

    latest_date_text = packet.get("latest_date", "-") if packet else "-"
    latest_risk_state = packet.get("latest_risk_state", {}) if packet else {}
    llm_bridge = packet.get("llm_bridge", {}) if packet else {}
    ensemble_weights = packet.get("ensemble_weights", {}) if packet else {}
    event_coverage = packet.get("event_coverage", {}) if packet else {}
    inference_packet = packet.get("inference_packet", {}) if packet else {}

    ai_cards = st.columns(4)
    ai_cards[0].metric("AI候选池数量", int(len(candidates)))
    ai_cards[1].metric("AI入选数量", int(packet.get("top_n", 0) or 0))
    ai_cards[2].metric("最新截面日期", latest_date_text)
    ai_cards[3].metric(
        "当前风控状态",
        "趋势开启" if latest_risk_state.get("risk_on") else "趋势过滤" if latest_risk_state else "未知",
    )

    if inference_packet:
        inference_cols = st.columns(4)
        inference_cols[0].metric("最新特征日", str(inference_packet.get("latest_feature_date", "-")))
        inference_cols[1].metric("最新带标签日", str(inference_packet.get("latest_labeled_date", "-")))
        inference_cols[2].metric("推理股票数", int(inference_packet.get("inference_universe_size", 0) or 0))
        inference_cols[3].metric("训练交易日数", int(inference_packet.get("history_dates", 0) or 0))
        skipped_filters = inference_packet.get("skipped_filters", []) or []
        if skipped_filters:
            st.caption(f"未标注截面推理已跳过未来依赖过滤：{', '.join(str(item) for item in skipped_filters)}")

    if ensemble_weights:
        st.markdown("**模型融合权重**")
        weight_cols = st.columns(4)
        weights = ensemble_weights.get("weights", {}) or {}
        weight_cols[0].metric("融合模式", str(ensemble_weights.get("mode", "manual")))
        weight_cols[1].metric("评估样本", str(ensemble_weights.get("evaluation_split", "manual") or "manual"))
        weight_cols[2].metric("梯度提升树权重", f"{float(weights.get('lgbm', 0.0) or 0.0):.1%}")
        weight_cols[3].metric("岭回归权重", f"{float(weights.get('ridge', 0.0) or 0.0):.1%}")
        if ensemble_weights.get("summary"):
            st.caption(str(ensemble_weights.get("summary")))

    if event_coverage:
        st.markdown("**资讯覆盖概览**")
        coverage_cols = st.columns(3)
        source_counts = event_coverage.get("news_source_counts", {}) or {}
        coverage_cols[0].metric("公告覆盖数", int(event_coverage.get("notice_covered_count", 0) or 0))
        coverage_cols[1].metric("研报覆盖数", int(event_coverage.get("research_covered_count", 0) or 0))
        coverage_cols[2].metric("资讯来源种类", int(len(source_counts)))
        if source_counts:
            coverage_table = pd.DataFrame([{"资讯来源": key, "数量": value} for key, value in source_counts.items()])
            st.dataframe(coverage_table, width="stretch")

    shown_columns = [
        "trade_date",
        "ts_code",
        "name",
        "industry_display",
        "theme_tags",
        "action_hint",
        "news_source",
        "final_score",
        "quant_score",
        "factor_overlay_score",
        "model_consensus",
        "confidence_level",
        "bull_points",
        "risk_points",
    ]
    st.dataframe(
        prettify_dataframe(candidates[[column for column in shown_columns if column in candidates.columns]]),
        width="stretch",
    )

    if llm_bridge:
        st.markdown("**可选大模型桥接**")
        llm_cols = st.columns(4)
        llm_cols[0].metric("请求包数量", int(llm_bridge.get("request_count", 0) or 0))
        llm_cols[1].metric("桥接模式", str(llm_bridge.get("provider", "prompt_only")))
        llm_cols[2].metric("模型名", str(llm_bridge.get("model", "") or "未配置"))
        llm_cols[3].metric("自动调用", "已开启" if llm_bridge.get("enabled") else "未开启")
        if llm_bridge.get("jsonl_path"):
            st.caption(f"请求包文件：{llm_bridge.get('jsonl_path')}")

    inspect_options = candidates["ts_code"].tolist()
    inspect_symbol = st.selectbox("查看单只股票的 AI 解释", inspect_options, key=inspect_key)
    inspected = candidates.loc[candidates["ts_code"] == inspect_symbol].head(1)
    if not inspected.empty:
        inspected_row = inspected.iloc[0]
        st.markdown("**投研结论**")
        if "thesis_summary" in inspected_row.index:
            st.write(inspected_row.get("thesis_summary", ""))
        if "theme_tags" in inspected_row.index:
            st.write(f"主题标签：{inspected_row.get('theme_tags', '')}")
        if "action_hint" in inspected_row.index:
            st.write(f"建议动作：{inspected_row.get('action_hint', '')}")
        if "notice_digest" in inspected_row.index:
            st.markdown("**公告摘要**")
            st.write(inspected_row.get("notice_digest", ""))
        if "news_digest" in inspected_row.index:
            st.markdown("**新闻/研报摘要**")
            st.write(inspected_row.get("news_digest", ""))
        if "news_source" in inspected_row.index:
            st.write(f"资讯来源：{inspected_row.get('news_source', '')}")
        if "research_digest" in inspected_row.index:
            st.markdown("**研报补充**")
            st.write(inspected_row.get("research_digest", ""))
        st.markdown("**中文研判摘要**")
        st.write(inspected_row.get("ai_brief", ""))
        st.markdown("**可继续交给大模型的提示词**")
        st.code(inspected_row.get("agent_prompt", ""), language="text")

    if brief:
        with st.expander("查看完整 AI 研判纪要", expanded=False):
            st.markdown(brief)


def _render_overlay_panel_v2(
    *,
    title: str,
    candidates: pd.DataFrame,
    packet: dict,
    brief: str,
    empty_message: str,
    inspect_key: str,
) -> None:
    st.markdown(f"**{title}**")
    if candidates.empty:
        st.info(empty_message)
        return

    latest_date_text = packet.get("latest_date", "-") if packet else "-"
    latest_risk_state = packet.get("latest_risk_state", {}) if packet else {}
    llm_bridge = packet.get("llm_bridge", {}) if packet else {}
    ensemble_weights = packet.get("ensemble_weights", {}) if packet else {}
    event_coverage = packet.get("event_coverage", {}) if packet else {}
    inference_packet = packet.get("inference_packet", {}) if packet else {}
    llm_response_records = _read_jsonl_records(str(llm_bridge.get("response_jsonl_path", ""))) if llm_bridge else []
    llm_response_lookup = {
        str(record.get("custom_id", "")).strip(): record
        for record in llm_response_records
        if str(record.get("custom_id", "")).strip()
    }

    ai_cards = st.columns(4)
    ai_cards[0].metric("AI候选池数量", int(len(candidates)))
    ai_cards[1].metric("AI入选数量", int(packet.get("top_n", 0) or 0))
    ai_cards[2].metric("最新截面日期", latest_date_text)
    ai_cards[3].metric(
        "当前风控状态",
        "趋势开启" if latest_risk_state.get("risk_on") else "趋势过滤" if latest_risk_state else "未知",
    )

    if inference_packet:
        inference_cols = st.columns(4)
        inference_cols[0].metric("最新特征日", str(inference_packet.get("latest_feature_date", "-")))
        inference_cols[1].metric("最新带标签日", str(inference_packet.get("latest_labeled_date", "-")))
        inference_cols[2].metric("推理股票数", int(inference_packet.get("inference_universe_size", 0) or 0))
        inference_cols[3].metric("训练交易日数", int(inference_packet.get("history_dates", 0) or 0))
        skipped_filters = inference_packet.get("skipped_filters", []) or []
        if skipped_filters:
            st.caption("未标注截面推理已跳过未来依赖过滤：" + ", ".join(str(item) for item in skipped_filters))

    if ensemble_weights:
        st.markdown("**模型融合权重**")
        weight_cols = st.columns(4)
        weights = ensemble_weights.get("weights", {}) or {}
        weight_cols[0].metric("融合模式", str(ensemble_weights.get("mode", "manual")))
        weight_cols[1].metric("评估样本", str(ensemble_weights.get("evaluation_split", "manual") or "manual"))
        weight_cols[2].metric("梯度提升树权重", f"{float(weights.get('lgbm', 0.0) or 0.0):.1%}")
        weight_cols[3].metric("岭回归权重", f"{float(weights.get('ridge', 0.0) or 0.0):.1%}")
        if ensemble_weights.get("summary"):
            st.caption(str(ensemble_weights.get("summary")))

    if event_coverage:
        st.markdown("**资讯覆盖概览**")
        coverage_cols = st.columns(3)
        source_counts = event_coverage.get("news_source_counts", {}) or {}
        coverage_cols[0].metric("公告覆盖数", int(event_coverage.get("notice_covered_count", 0) or 0))
        coverage_cols[1].metric("研报覆盖数", int(event_coverage.get("research_covered_count", 0) or 0))
        coverage_cols[2].metric("资讯来源种类", int(len(source_counts)))
        if source_counts:
            coverage_table = pd.DataFrame([{"资讯来源": key, "数量": value} for key, value in source_counts.items()])
            st.dataframe(coverage_table, width="stretch")

    shown_columns = [
        "trade_date",
        "ts_code",
        "name",
        "industry_display",
        "theme_tags",
        "action_hint",
        "news_source",
        "final_score",
        "quant_score",
        "factor_overlay_score",
        "model_consensus",
        "confidence_level",
        "bull_points",
        "risk_points",
    ]
    st.dataframe(
        prettify_dataframe(candidates[[column for column in shown_columns if column in candidates.columns]]),
        width="stretch",
    )

    if llm_bridge:
        st.markdown("**外部大模型桥接**")
        llm_cols = st.columns(6)
        llm_cols[0].metric("请求包数量", int(llm_bridge.get("request_count", 0) or 0))
        llm_cols[1].metric("已执行响应", int(llm_bridge.get("response_count", 0) or 0))
        llm_cols[2].metric("成功数量", int(llm_bridge.get("success_count", 0) or 0))
        llm_cols[3].metric("桥接模式", str(llm_bridge.get("provider", "prompt_only")))
        llm_cols[4].metric("模型名", str(llm_bridge.get("model", "") or "未配置"))
        llm_cols[5].metric("执行状态", str(llm_bridge.get("execution_status", "unknown")))
        if llm_bridge.get("jsonl_path"):
            st.caption(f"请求包文件：{llm_bridge.get('jsonl_path')}")
        if llm_bridge.get("response_jsonl_path"):
            st.caption(f"响应文件：{llm_bridge.get('response_jsonl_path')}")
        reasoning_parts: list[str] = []
        if llm_bridge.get("reasoning_effort"):
            reasoning_parts.append(f"reasoning.effort={llm_bridge.get('reasoning_effort')}")
        if llm_bridge.get("reasoning_summary"):
            reasoning_parts.append(f"reasoning.summary={llm_bridge.get('reasoning_summary')}")
        if llm_bridge.get("max_output_tokens"):
            reasoning_parts.append(f"max_output_tokens={llm_bridge.get('max_output_tokens')}")
        if reasoning_parts:
            st.caption(" | ".join(str(part) for part in reasoning_parts))
        blocking_reason = str(llm_bridge.get("blocking_reason", "") or "").strip()
        if blocking_reason:
            if llm_bridge.get("execution_status") in {"configuration_incomplete", "execution_failed", "executed_with_errors"}:
                st.warning(blocking_reason)
            else:
                st.info(blocking_reason)
        response_summary_path = str(llm_bridge.get("response_summary_path", "") or "").strip()
        response_summary = _read_text(Path(response_summary_path)) if response_summary_path else ""
        if response_summary:
            with st.expander("查看外部模型自动研讨纪要", expanded=False):
                st.markdown(response_summary)

    inspect_options = candidates["ts_code"].tolist()
    inspect_symbol = st.selectbox("查看单只股票的 AI 解释", inspect_options, key=inspect_key)
    inspected = candidates.loc[candidates["ts_code"] == inspect_symbol].head(1)
    if not inspected.empty:
        inspected_row = inspected.iloc[0]
        st.markdown("**投研结论**")
        if "thesis_summary" in inspected_row.index:
            st.write(inspected_row.get("thesis_summary", ""))
        if "theme_tags" in inspected_row.index:
            st.write(f"主题标签：{inspected_row.get('theme_tags', '')}")
        if "action_hint" in inspected_row.index:
            st.write(f"建议动作：{inspected_row.get('action_hint', '')}")
        if "notice_digest" in inspected_row.index:
            st.markdown("**公告摘要**")
            st.write(inspected_row.get("notice_digest", ""))
        if "news_digest" in inspected_row.index:
            st.markdown("**新闻/研报摘要**")
            st.write(inspected_row.get("news_digest", ""))
        if "news_source" in inspected_row.index:
            st.write(f"资讯来源：{inspected_row.get('news_source', '')}")
        if "research_digest" in inspected_row.index:
            st.markdown("**研报补充**")
            st.write(inspected_row.get("research_digest", ""))
        st.markdown("**中文研判摘要**")
        st.write(inspected_row.get("ai_brief", ""))
        llm_response = llm_response_lookup.get(inspect_symbol)
        if llm_response:
            st.markdown("**外部模型自动研讨**")
            if llm_response.get("status") == "success":
                st.write(llm_response.get("output_text", ""))
            else:
                st.error(str(llm_response.get("error", "外部模型执行失败")))
        st.markdown("**可继续交给大模型的提示词**")
        st.code(inspected_row.get("agent_prompt", ""), language="text")

    if brief:
        with st.expander("查看完整 AI 研判纪要", expanded=False):
            st.markdown(brief)


def _symbol_history(feature_panel: pd.DataFrame, symbol: str, factor_name: str, tail_n: int = 240) -> pd.DataFrame:
    scoped = feature_panel.loc[feature_panel["ts_code"] == symbol, ["trade_date", factor_name]].copy()
    scoped = scoped.dropna().sort_values("trade_date").tail(tail_n)
    return scoped.set_index("trade_date")


def _config_summary_text(config: dict) -> str:
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
        f" | 标签周期：{zh(config.get('label_col', '-'))}"
        f" | 候选股数量：{config.get('top_n', '-')}"
        f" | 滚动训练：{rolling_text}({rolling_freq})"
        f" | 行业中性化：{neutral_text}"
    )


st.title("A股量化研究平台")
st.caption("界面已经尽量改成中文投研表达，重点是让你能直接看懂数据状态、因子含义、模型结果和候选股票。")

summary = _load_dataset_summary()
streamlit_service_status = _streamlit_service_status()
experiment_config = _load_experiment_config()
watchlist_config = _load_watchlist_config()
metrics_table = _metrics_table()
feature_panel = _load_feature_panel()
daily_bar = _load_daily_bar()
overlay_candidates = _load_overlay_candidates()
overlay_packet = _load_overlay_packet()
overlay_brief = _load_overlay_brief()
overlay_inference_candidates = _load_overlay_inference_candidates()
overlay_inference_packet = _load_overlay_inference_packet()
overlay_inference_brief = _load_overlay_inference_brief()
ridge_predictions_test = _load_predictions("ridge", "test")
lgbm_predictions_test = _load_predictions("lgbm", "test")
ensemble_predictions_test = _load_predictions("ensemble", "test")
ensemble_predictions_inference = _load_predictions("ensemble", "inference")
watchlist_view = _build_watchlist_view(
    watchlist_config=watchlist_config,
    daily_bar=daily_bar,
    ridge_predictions=ridge_predictions_test,
    lgbm_predictions=lgbm_predictions_test,
    ensemble_predictions=ensemble_predictions_test,
    overlay_candidates=overlay_candidates,
    ensemble_inference_predictions=ensemble_predictions_inference,
    overlay_inference_candidates=overlay_inference_candidates,
)

with st.sidebar:
    st.header("页面服务")
    st.metric("已缓存股票数", int(summary["cached_symbols"]))
    st.metric("服务状态", str(streamlit_service_status.get("status_label_display", "未知")))
    st.metric("8501 端口", "监听中" if streamlit_service_status.get("listener_present") else "未监听")
    if streamlit_service_status.get("streamlit_pid"):
        st.code(f"Streamlit PID: {streamlit_service_status['streamlit_pid']}")
    st.caption("下载任务已从页面移出，后续请在单独的 CMD 或 PowerShell 窗口执行。")

    st.header("研究参数")
    with st.form("experiment_config_form", clear_on_submit=False):
        train_start = st.date_input(
            "训练起始日期",
            value=pd.Timestamp(experiment_config.get("train_start", "2018-01-01")).date(),
        )
        train_end = st.date_input(
            "训练截止日期",
            value=pd.Timestamp(experiment_config.get("train_end", "2022-12-31")).date(),
        )
        valid_end = st.date_input(
            "验证截止日期",
            value=pd.Timestamp(experiment_config.get("valid_end", "2023-12-31")).date(),
        )
        test_end = st.date_input(
            "测试截止日期",
            value=pd.Timestamp(experiment_config.get("test_end", "2025-12-31")).date(),
        )
        current_label = experiment_config.get("label_col", "ret_t1_t10")
        label_col = st.selectbox(
            "标签周期",
            LABEL_OPTIONS,
            index=LABEL_OPTIONS.index(current_label) if current_label in LABEL_OPTIONS else 1,
            format_func=zh,
        )
        top_n_config = st.number_input(
            "候选股数量 (Top N)",
            min_value=1,
            max_value=100,
            value=int(experiment_config.get("top_n", 10)),
            step=1,
        )
        save_config = st.form_submit_button("保存研究参数")

    if save_config:
        if not (train_start <= train_end <= valid_end <= test_end):
            st.error("日期顺序不合法，请确保 训练起始 <= 训练截止 <= 验证截止 <= 测试截止。")
        else:
            updated_config = dict(experiment_config)
            updated_config["train_start"] = str(train_start)
            updated_config["train_end"] = str(train_end)
            updated_config["valid_end"] = str(valid_end)
            updated_config["test_end"] = str(test_end)
            updated_config["label_col"] = label_col
            updated_config["top_n"] = int(top_n_config)
            _save_experiment_config(updated_config)
            st.session_state["last_action"] = (
                "config",
                True,
                "研究参数已保存到 config/experiment.yaml。",
            )
            _refresh_cached_views()
            st.rerun()

    st.caption(_config_summary_text(experiment_config))

    st.header("研究操作")
    if st.button("刷新部分面板", width="stretch"):
        with st.spinner("正在汇总已缓存股票..."):
            ok, output = _run_module("src.data.materialize_cache")
        st.session_state["last_action"] = ("materialize", ok, output)
        _refresh_cached_views()
        st.rerun()

    if st.button("重建特征与标签", width="stretch"):
        with st.spinner("正在构建特征和标签..."):
            ok, output = _run_module("src.features.build_feature_panel")
        st.session_state["last_action"] = ("features", ok, output)
        _refresh_cached_views()
        st.rerun()

    if st.button("运行岭回归基线", width="stretch"):
        with st.spinner("正在训练岭回归模型..."):
            ok, output = _run_module("src.models.train_linear")
        st.session_state["last_action"] = ("ridge", ok, output)
        _refresh_cached_views()
        st.rerun()

    if st.button("运行梯度提升树基线", width="stretch"):
        with st.spinner("正在训练梯度提升树模型..."):
            ok, output = _run_module("src.models.train_lgbm")
        st.session_state["last_action"] = ("lgbm", ok, output)
        _refresh_cached_views()
        st.rerun()

    if st.button("运行自适应融合策略", width="stretch"):
        with st.spinner("正在汇总单模型结果并生成融合策略..."):
            ok, output = _run_module("src.models.train_ensemble")
        st.session_state["last_action"] = ("ensemble", ok, output)
        _refresh_cached_views()
        st.rerun()

    if st.button("生成AI研判摘要", width="stretch"):
        with st.spinner("正在生成多模型共识和中文研判摘要..."):
            ok, output = _run_module("src.agents.overlay_report")
        st.session_state["last_action"] = ("overlay", ok, output)
        _refresh_cached_views()
        st.rerun()

    if st.button("生成最新未标注截面推理", width="stretch"):
        with st.spinner("正在生成最新未标注截面推理与AI候选池..."):
            ok, output = _run_module("src.agents.overlay_inference_report")
        st.session_state["last_action"] = ("latest_inference", ok, output)
        _refresh_cached_views()
        st.rerun()

    if st.button("清空页面缓存", width="stretch"):
        _refresh_cached_views()
        st.success("页面缓存已清空。")

    _render_action_result()

top_cards = st.columns(5)
top_cards[0].metric("已缓存股票", int(summary["cached_symbols"]))
top_cards[1].metric("特征样本数", int(summary.get("feature_rows", 0)))
top_cards[2].metric("可研究股票数", int(summary.get("feature_symbols", 0)))
top_cards[3].metric("页面服务", str(streamlit_service_status.get("status_label_display", "未知")))
top_cards[4].metric("观察池股票", int(len(watchlist_view)))

if summary.get("date_min") and summary.get("date_max"):
    st.info(f"当前可研究数据区间：{summary['date_min']} 到 {summary['date_max']}")

st.caption(f"当前研究参数：{_config_summary_text(experiment_config)}")

tabs = st.tabs(["平台总览", "因子探索", "模型回测", "候选股票", "观察持仓", "AI研判", "页面服务"])

with tabs[0]:
    st.subheader("数据面板状态")
    status_cols = st.columns(3)
    for card, name, key in zip(
        status_cols,
        ["日线面板", "特征面板", "标签面板"],
        ["daily_bar", "features", "labels"],
    ):
        file_info = summary[key]
        with card:
            st.markdown(f"**{name}**")
            if file_info["exists"]:
                st.write(f"大小：{file_info['size_mb']} MB")
                st.write(f"更新时间：{file_info['updated']}")
            else:
                st.warning("还没有生成。")

    st.subheader("模型对比")
    if metrics_table.empty:
        st.warning("模型结果还没有生成，请先在左侧运行基线训练。")
    else:
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
        comparison = prettify_dataframe(metrics_table[shown_columns].copy())
        st.dataframe(comparison, width="stretch")
        with st.expander("主要指标怎么理解", expanded=False):
            metric_guide = pd.DataFrame(
                [{"指标": zh(key), "说明": value} for key, value in METRIC_EXPLANATIONS.items()]
            )
            st.dataframe(metric_guide, width="stretch")

        split_for_curve = st.selectbox(
            "净值曲线对比数据集",
            SPLITS,
            index=1,
            format_func=lambda value: SPLIT_LABELS[value],
        )
        curves: list[pd.DataFrame] = []
        for model_name in MODEL_NAMES:
            portfolio = _load_portfolio(model_name, split_for_curve)
            if not portfolio.empty:
                curves.append(
                    portfolio[["trade_date", "equity_curve"]]
                    .rename(columns={"equity_curve": MODEL_LABELS[model_name]})
                    .set_index("trade_date")
                )
        if curves:
            chart_frame = pd.concat(curves, axis=1)
            st.line_chart(chart_frame)

with tabs[1]:
    st.subheader("因子面板浏览")
    if feature_panel.empty:
        st.warning("特征面板还没有生成。")
    else:
        numeric_columns = [
            column
            for column in feature_panel.columns
            if column not in {"trade_date", "ts_code", "name", "industry", "index_code"}
            and pd.api.types.is_numeric_dtype(feature_panel[column])
        ]
        latest_date = feature_panel["trade_date"].max()
        cross_section = feature_panel.loc[feature_panel["trade_date"] == latest_date].copy()

        left, right = st.columns([1.1, 1.4])
        with left:
            factor_name = st.selectbox(
                "查看排序的因子",
                numeric_columns,
                index=min(11, len(numeric_columns) - 1),
                format_func=zh,
            )
            st.caption(explain(factor_name))
            ranking = cross_section[["ts_code", "name", factor_name]].dropna().sort_values(factor_name, ascending=False)
            ranking = ranking.rename(columns={factor_name: zh(factor_name)})
            st.markdown(f"**最新截面日期：{latest_date.date()}**")
            st.dataframe(prettify_dataframe(ranking.head(20)), width="stretch")

            missing_rate = (
                feature_panel[numeric_columns]
                .isna()
                .mean()
                .sort_values(ascending=False)
                .rename("missing_rate")
                .reset_index()
                .rename(columns={"index": "feature"})
            )
            missing_rate["feature"] = missing_rate["feature"].map(zh)
            st.markdown("**缺失率最高的特征**")
            st.dataframe(prettify_dataframe(missing_rate.head(15)), width="stretch")

        with right:
            symbol_options = cross_section["ts_code"].sort_values().tolist()
            symbol = st.selectbox("查看股票因子历史", symbol_options)
            history_factor = st.selectbox(
                "历史走势因子",
                numeric_columns,
                index=min(3, len(numeric_columns) - 1),
                format_func=zh,
            )
            st.caption(explain(history_factor))
            if symbol:
                history = _symbol_history(feature_panel, symbol, history_factor)
                if not history.empty:
                    history = history.rename(columns={history_factor: zh(history_factor)})
                    st.line_chart(history)
                latest_row = cross_section.loc[cross_section["ts_code"] == symbol].head(1)
                if not latest_row.empty:
                    st.markdown("**该股票最新因子快照**")
                    latest_row = latest_row.iloc[0].to_dict()
                    latest_row_frame = pd.DataFrame(
                        [{"字段": zh(key), "原始列名": key, "数值": value} for key, value in latest_row.items()]
                    )
                    latest_row_frame["数值"] = latest_row_frame["数值"].map(
                        lambda value: "" if pd.isna(value) else str(value)
                    )
                    st.dataframe(latest_row_frame, width="stretch")

        with st.expander("常见因子与指标说明", expanded=False):
            glossary = pd.DataFrame(
                [{"字段": zh(key), "原始列名": key, "说明": explain(key)} for key in FIELD_EXPLANATIONS]
            )
            st.dataframe(glossary, width="stretch")

with tabs[2]:
    st.subheader("模型回测结果")
    model_name = st.selectbox(
        "选择模型",
        MODEL_NAMES,
        index=1,
        key="lab_model",
        format_func=lambda value: MODEL_LABELS[value],
    )
    split_name = st.selectbox(
        "选择数据集",
        SPLITS,
        index=1,
        key="lab_split",
        format_func=lambda value: SPLIT_LABELS[value],
    )
    st.caption(
        f"当前回测配置：标签周期为 {zh(experiment_config.get('label_col', '-'))}，候选股数量为 {experiment_config.get('top_n', '-')}。"
    )
    metrics = _load_metrics(model_name, split_name)
    portfolio = _load_portfolio(model_name, split_name)
    stability = _load_stability(model_name)
    importance = _load_feature_importance(model_name)
    yearly_diagnostics = _load_diagnostic_table(model_name, split_name, "yearly")
    regime_diagnostics = _load_diagnostic_table(model_name, split_name, "regime")

    left, right = st.columns([1.0, 1.5])
    with left:
        if metrics:
            st.metric("截面排序有效性 (RankIC)", f"{metrics['rank_ic_mean']:.4f}")
            st.metric("前N股票正收益占比", f"{metrics['top_n_hit_rate']:.2%}")
            st.metric("前N股票平均未来收益", f"{metrics['top_n_forward_mean']:.2%}")
            st.metric("组合年化收益", f"{metrics['daily_portfolio_annualized_return']:.2%}")
            st.metric("组合夏普", f"{metrics['daily_portfolio_sharpe']:.2f}")
            st.metric("组合最大回撤", f"{metrics['daily_portfolio_max_drawdown']:.2%}")
            if "avg_turnover_ratio" in metrics:
                st.metric("平均换手比例", f"{metrics['avg_turnover_ratio']:.2%}")
            if "holding_period_days" in metrics:
                st.metric("持有周期(交易日)", f"{metrics['holding_period_days']:.0f}")
            if "risk_filter_active_ratio" in metrics:
                st.metric("趋势过滤开启占比", f"{metrics['risk_filter_active_ratio']:.2%}")
            if stability:
                st.markdown("**稳定性结论**")
                st.write(f"评级：{stability.get('grade', '-')}")
                st.write(str(stability.get("conclusion", "")))
            with st.expander("这组回测指标怎么看", expanded=False):
                metric_guide = pd.DataFrame(
                    [{"指标": zh(key), "说明": value} for key, value in METRIC_EXPLANATIONS.items()]
                )
                st.dataframe(metric_guide, width="stretch")
        else:
            st.warning("该模型结果还没有生成。")

        if not importance.empty:
            st.markdown("**核心因子贡献**")
            importance = prettify_dataframe(importance.head(20))
            st.dataframe(importance, width="stretch")

    with right:
        if not portfolio.empty:
            chart_frame = portfolio.set_index("trade_date")[["equity_curve"]].rename(columns={"equity_curve": "组合净值"})
            st.line_chart(chart_frame)

            monthly = portfolio.copy()
            monthly["month"] = pd.to_datetime(monthly["trade_date"]).dt.to_period("M").astype(str)
            monthly_summary = monthly.groupby("month")["net_return"].sum().reset_index()
            st.markdown("**最近24个月收益汇总**")
            st.dataframe(prettify_dataframe(monthly_summary.tail(24)), width="stretch")
            if not yearly_diagnostics.empty:
                st.markdown("**按年份拆解**")
                st.dataframe(prettify_dataframe(yearly_diagnostics), width="stretch")
            if not regime_diagnostics.empty:
                regime_view = regime_diagnostics.copy()
                if "regime" in regime_view.columns:
                    regime_view["regime"] = regime_view["regime"].replace(
                        {
                            "trend_on": "趋势开启",
                            "trend_off": "趋势过滤",
                        }
                    )
                st.markdown("**按趋势阶段拆解**")
                st.dataframe(prettify_dataframe(regime_view), width="stretch")
        else:
            st.warning("组合净值文件还没有生成。")

with tabs[3]:
    st.subheader("最新候选股票")
    result_model = st.selectbox(
        "结果模型",
        MODEL_NAMES,
        index=1,
        key="pick_model",
        format_func=lambda value: MODEL_LABELS[value],
    )
    result_split = st.selectbox(
        "查看数据集",
        SPLITS,
        index=1,
        key="pick_split",
        format_func=lambda value: SPLIT_LABELS[value],
    )
    top_n_default = int(experiment_config.get("top_n", 10))
    top_n = st.slider(
        "显示前 N 只候选股票",
        min_value=1,
        max_value=max(30, top_n_default),
        value=min(max(1, top_n_default), max(30, top_n_default)),
        step=1,
    )
    predictions = _load_predictions(result_model, result_split)

    if predictions.empty:
        st.warning("预测文件还没有生成。")
    else:
        latest_picks = _latest_snapshot(predictions, top_n=top_n)
        if latest_picks.empty:
            st.warning("当前模型没有可展示的预测结果。")
        else:
            latest_date = latest_picks["trade_date"].iloc[0]
            st.markdown(f"**最新预测日期：{latest_date.date()}**")
            columns = [
                "trade_date",
                "ts_code",
                "name",
                "score",
                "ret_t1_t10",
                "mom_20",
                "mom_60",
                "close_to_ma_20",
                "drawdown_60",
            ]
            shown_columns = [column for column in columns if column in latest_picks.columns]
            st.dataframe(prettify_dataframe(latest_picks[shown_columns]), width="stretch")

            inspect_symbol = st.selectbox("查看单只股票评分历史", latest_picks["ts_code"].tolist(), key="inspect_symbol")
            symbol_predictions = (
                predictions.loc[predictions["ts_code"] == inspect_symbol, ["trade_date", "score", "ret_t1_t10"]]
                .sort_values("trade_date")
                .tail(240)
                .set_index("trade_date")
                .rename(columns={"score": "综合评分", "ret_t1_t10": "未来10日收益(T+1建仓)"})
            )
            st.line_chart(symbol_predictions[["综合评分"]])
            st.dataframe(symbol_predictions.tail(20), width="stretch")

with tabs[4]:
    render_watchlist_page(
        watchlist_view=watchlist_view,
        ensemble_predictions_test=ensemble_predictions_test,
        prettify_dataframe=prettify_dataframe,
        zh=zh,
        load_latest_watch_plan=_load_latest_watch_plan,
        load_latest_action_memo=_load_latest_action_memo,
        run_module=_run_module,
        refresh_cached_views=_refresh_cached_views,
    )
    """
    st.subheader("观察池与持仓")
    if watchlist_view.empty:
        st.warning("当前还没有配置观察池股票。你可以在 config/watchlist.yaml 里添加持仓或关注股票。")
    else:
        st.markdown("**观察池总览**")
        overview_cols = st.columns(5)
        overview_cols[0].metric("观察池股票数", int(len(watchlist_view)))
        overlay_count = int(watchlist_view["is_overlay_selected"].fillna(False).sum()) if "is_overlay_selected" in watchlist_view.columns else 0
        overview_cols[1].metric("进入AI精选池", overlay_count)
        inference_overlay_count = (
            int(watchlist_view["is_inference_overlay_selected"].fillna(False).sum())
            if "is_inference_overlay_selected" in watchlist_view.columns
            else 0
        )
        overview_cols[2].metric("进入最新推理池", inference_overlay_count)
        pnl_value = pd.to_numeric(watchlist_view["unrealized_pnl"], errors="coerce").sum()
        market_value = pd.to_numeric(watchlist_view["market_value"], errors="coerce").sum()
        overview_cols[3].metric("观察池参考市值", f"{market_value:,.0f}")
        overview_cols[4].metric("观察池浮动盈亏", f"{pnl_value:,.0f}")

        filter_cols = st.columns((1.5, 1, 1))
        watch_keyword = filter_cols[0].text_input(
            "快速搜索",
            value=st.session_state.get("watchlist_keyword", ""),
            key="watchlist_keyword",
            placeholder="输入代码或简称",
        )
        watch_scope = filter_cols[1].selectbox(
            "查看范围",
            ["全部", "只看 AI 精选", "只看最新推理池", "只看浮亏较大"],
            key="watchlist_scope",
        )
        watch_sort = filter_cols[2].selectbox(
            "排序方式",
            ["最新推理排名", "历史验证排名", "浮亏比例", "参考市值"],
            key="watchlist_sort",
        )
        filtered_watchlist = _filtered_watchlist_view(
            watchlist_view,
            keyword=watch_keyword,
            scope=watch_scope,
            sort_by=watch_sort,
        )
        st.caption(f"当前显示 {len(filtered_watchlist)} / {len(watchlist_view)} 只观察池股票。")

        watchlist_columns = [
            "ts_code",
            "name",
            "premarket_plan",
            "industry",
            "cost_basis",
            "shares",
            "mark_price",
            "mark_date",
            "price_source",
            "latest_bar_date",
            "latest_bar_close",
            "mark_status",
            "market_value",
            "unrealized_pnl",
            "unrealized_pnl_pct",
            "breakeven_price",
            "gap_to_breakeven_pct",
            "signal_date",
            "ensemble_rank",
            "ensemble_rank_pct",
            "inference_signal_date",
            "inference_ensemble_rank",
            "inference_ensemble_rank_pct",
            "ridge_rank_pct",
            "lgbm_rank_pct",
            "llm_latest_round",
            "llm_latest_status",
            "watch_level",
            "is_overlay_selected",
            "is_inference_overlay_selected",
        ]
        if filtered_watchlist.empty:
            st.info("当前筛选条件下没有匹配股票，可以放宽范围或清空搜索词。")
        else:
            st.dataframe(
                prettify_dataframe(filtered_watchlist[[column for column in watchlist_columns if column in filtered_watchlist.columns]]),
                width="stretch",
            )

            inspect_symbol = st.selectbox(
                "选择观察池股票",
                filtered_watchlist["ts_code"].tolist(),
                key="watchlist_symbol",
                format_func=lambda value: f"{value} {filtered_watchlist.loc[filtered_watchlist['ts_code'] == value, 'name'].iloc[0]}",
            )
            inspected = watchlist_view.loc[watchlist_view["ts_code"] == inspect_symbol].head(1)
            if not inspected.empty:
                row = inspected.iloc[0]
                detail_cols = st.columns(6)
                detail_cols[0].metric("参考价格", f"{float(row['mark_price']):.2f}" if pd.notna(row["mark_price"]) else "-")
                detail_cols[1].metric("浮动盈亏", f"{float(row['unrealized_pnl']):,.0f}" if pd.notna(row["unrealized_pnl"]) else "-")
                detail_cols[2].metric("浮亏浮盈比例", f"{float(row['unrealized_pnl_pct']):.2%}" if pd.notna(row["unrealized_pnl_pct"]) else "-")
                detail_cols[3].metric("历史验证排名", f"{int(row['ensemble_rank'])}" if pd.notna(row["ensemble_rank"]) else "-")
                detail_cols[4].metric("最新推理排名", f"{int(row['inference_ensemble_rank'])}" if pd.notna(row.get("inference_ensemble_rank")) else "-")
                detail_cols[5].metric("价格状态", str(row.get("mark_status") or "-"))

                guide_cols = st.columns(5)
                guide_cols[0].metric("回本价", f"{float(row['breakeven_price']):.3f}" if pd.notna(row["breakeven_price"]) else "-")
                guide_cols[1].metric("距回本涨幅", f"{float(row['gap_to_breakeven_pct']):.2%}" if pd.notna(row["gap_to_breakeven_pct"]) else "-")
                guide_cols[2].metric("半程修复位", f"{float(row['halfway_recovery_price']):.2f}" if pd.notna(row["halfway_recovery_price"]) else "-")
                guide_cols[3].metric("观察防守位", f"{float(row['defensive_price']):.2f}" if pd.notna(row["defensive_price"]) else "-")
                guide_cols[4].metric("最新落库日线", str(pd.to_datetime(row["latest_bar_date"]).date()) if pd.notna(row.get("latest_bar_date")) else "-")

                rank_cols = st.columns(4)
                rank_cols[0].metric("历史验证日期", str(pd.to_datetime(row["signal_date"]).date()) if pd.notna(row.get("signal_date")) else "-")
                rank_cols[1].metric("历史验证分位", f"{float(row['ensemble_rank_pct']):.2%}" if pd.notna(row.get("ensemble_rank_pct")) else "-")
                rank_cols[2].metric("最新推理日期", str(pd.to_datetime(row["inference_signal_date"]).date()) if pd.notna(row.get("inference_signal_date")) else "-")
                rank_cols[3].metric("最新推理分位", f"{float(row['inference_ensemble_rank_pct']):.2%}" if pd.notna(row.get("inference_ensemble_rank_pct")) else "-")

                if bool(row.get("is_manual_mark")):
                    st.warning(str(row.get("mark_status_note") or "当前持仓估值使用手工记录价格。"))
                elif row.get("mark_status_note"):
                    st.info(str(row.get("mark_status_note")))

                caption_parts = [f"价格来源：{row.get('price_source', '-')}"]
                if pd.notna(row.get("latest_bar_close")):
                    caption_parts.append(f"最新落库收盘：{float(row['latest_bar_close']):.2f}")
                if pd.notna(row.get("signal_date")):
                    caption_parts.append(f"历史验证：{pd.to_datetime(row['signal_date']).date()}")
                if pd.notna(row.get("inference_signal_date")):
                    caption_parts.append(f"最新推理：{pd.to_datetime(row['inference_signal_date']).date()}")
                st.caption(" | ".join(caption_parts))
                if row.get("ranking_note"):
                    st.caption(str(row.get("ranking_note")))
                st.markdown(f"**观察级别：{row.get('watch_level', '-')}**")
                if row.get("premarket_plan"):
                    st.info(str(row.get("premarket_plan")))
                st.write(str(row.get("action_brief", "")))

                discussion_snapshot = row.get("llm_discussion_snapshot")
                if isinstance(discussion_snapshot, dict) and discussion_snapshot:
                    st.markdown("**多轮 AI 研讨回写**")
                    discussion_cols = st.columns(4)
                    discussion_cols[0].metric("已检查轮次", int(discussion_snapshot.get("round_count", 0) or 0))
                    discussion_cols[1].metric("入选轮次", int(discussion_snapshot.get("selected_round_count", 0) or 0))
                    discussion_cols[2].metric("成功轮次", int(discussion_snapshot.get("success_round_count", 0) or 0))
                    discussion_cols[3].metric("最新状态", str(discussion_snapshot.get("latest_status", "") or "-"))
                    if discussion_snapshot.get("overview"):
                        st.write(str(discussion_snapshot.get("overview")))
                    discussion_rows = discussion_round_rows(discussion_snapshot)
                    if discussion_rows:
                        st.dataframe(pd.DataFrame(discussion_rows), width="stretch")
                    for round_info in discussion_snapshot.get("rounds", []) or []:
                        title = (
                            f"{round_info.get('round_label', '-')}"
                            f" | {round_info.get('latest_date', '-')}"
                            f" | {round_info.get('display_status', '-')}"
                        )
                        with st.expander(title, expanded=False):
                            summary_text = str(round_info.get("summary_text", "") or "").strip()
                            if summary_text:
                                st.write(summary_text)
                            thesis_summary = str(round_info.get("thesis_summary", "") or "").strip()
                            if thesis_summary and thesis_summary != summary_text:
                                st.caption(f"系统内结论：{thesis_summary}")
                action_cols = st.columns(2)
                if action_cols[0].button("生成最新盯盘清单", key=f"generate_watch_plan_{inspect_symbol}", width="stretch"):
                    with st.spinner("正在为观察池生成最新盯盘清单..."):
                        ok, output = _run_module("src.agents.watch_plan")
                    st.session_state["last_action"] = ("watch_plan", ok, output)
                    _refresh_cached_views()
                    st.rerun()
                if action_cols[1].button("生成最新操作备忘", key=f"generate_action_memo_{inspect_symbol}", width="stretch"):
                    with st.spinner("正在为观察池生成最新操作备忘..."):
                        ok, output = _run_module("src.agents.action_memo")
                    st.session_state["last_action"] = ("action_memo", ok, output)
                    _refresh_cached_views()
                    st.rerun()

                watch_plan = _load_latest_watch_plan(inspect_symbol)
                if watch_plan.get("content"):
                    st.markdown("**最新盯盘清单**")
                    watch_plan_meta: list[str] = []
                    if watch_plan.get("plan_date"):
                        watch_plan_meta.append(f"日期：{watch_plan['plan_date']}")
                    if watch_plan.get("name"):
                        watch_plan_meta.append(f"文件：{watch_plan['name']}")
                    if watch_plan_meta:
                        st.caption(" | ".join(watch_plan_meta))
                    with st.expander("查看完整盯盘清单", expanded=False):
                        st.markdown(str(watch_plan.get("content", "")))
                else:
                    st.info("当前股票还没有生成盯盘清单 Markdown，可直接点击上方按钮自动生成。")

                action_memo = _load_latest_action_memo(inspect_symbol)
                if action_memo.get("content"):
                    st.markdown("**最新操作备忘**")
                    action_memo_meta: list[str] = []
                    if action_memo.get("plan_date"):
                        action_memo_meta.append(f"日期：{action_memo['plan_date']}")
                    if action_memo.get("name"):
                        action_memo_meta.append(f"文件：{action_memo['name']}")
                    if action_memo_meta:
                        st.caption(" | ".join(action_memo_meta))
                    with st.expander("查看完整操作备忘", expanded=False):
                        st.markdown(str(action_memo.get("content", "")))
                else:
                    st.info("当前股票还没有生成操作备忘 Markdown，可直接点击上方按钮自动生成。")

                reduce_plan = _build_reduce_plan(row)
                if not reduce_plan.empty:
                    st.markdown("**分批观察计划**")
                    st.dataframe(prettify_dataframe(reduce_plan), width="stretch")

                history_source = ensemble_predictions_test if not ensemble_predictions_test.empty else pd.DataFrame()
                if not history_source.empty:
                    history = history_source.loc[history_source["ts_code"] == inspect_symbol, ["trade_date", "score"]].copy()
                    if not history.empty:
                        history = history.sort_values("trade_date").tail(120).set_index("trade_date").rename(columns={"score": "融合评分"})
                        st.line_chart(history)

                technical_cols = [
                    "mom_5",
                    "mom_20",
                    "mom_60",
                    "close_to_ma_20",
                    "close_to_ma_60",
                    "drawdown_60",
                    "pct_chg",
                    "latest_bar_date",
                    "latest_bar_close",
                    "mark_status_note",
                    "manual_mark_note",
                ]
                technical_frame = pd.DataFrame(
                    [{"字段": zh(key), "原始列名": key, "数值": "" if pd.isna(row.get(key)) else str(row.get(key))} for key in technical_cols]
                )
                st.dataframe(technical_frame, width="stretch")
    """

with tabs[5]:
    st.subheader("AI二次研判")
    if overlay_inference_candidates.empty and overlay_candidates.empty:
        st.warning("AI 研判摘要还没有生成，请先点击左侧的“生成AI研判摘要”或“生成最新未标注截面推理”。")
    else:
        _render_overlay_panel_v2(
            title="最新未标注截面推理",
            candidates=overlay_inference_candidates,
            packet=overlay_inference_packet,
            brief=overlay_inference_brief,
            empty_message="最新未标注截面推理还没有生成，可先点击左侧按钮生成更贴近最新行情的候选池。",
            inspect_key="overlay_inference_symbol",
        )
        st.divider()
        _render_overlay_panel_v2(
            title="历史已验证 AI 叠加候选",
            candidates=overlay_candidates,
            packet=overlay_packet,
            brief=overlay_brief,
            empty_message="历史已验证的 AI 研判摘要还没有生成，请先点击左侧的“生成AI研判摘要”。",
            inspect_key="overlay_symbol",
        )

with tabs[6]:
    st.subheader("页面服务")
    service_cols = st.columns(4)
    service_cols[0].metric("服务状态", str(streamlit_service_status.get("status_label_display", "未知")))
    service_cols[1].metric("守护进程 PID", str(streamlit_service_status.get("supervisor_pid") or "-"))
    service_cols[2].metric("页面进程 PID", str(streamlit_service_status.get("streamlit_pid") or "-"))
    service_cols[3].metric("8501 端口", "监听中" if streamlit_service_status.get("listener_present") else "未监听")
    if streamlit_service_status.get("listener_pids"):
        st.write(f"监听进程：{', '.join(str(pid) for pid in streamlit_service_status['listener_pids'])}")
    if streamlit_service_status.get("stale_status"):
        st.caption("检测到残留的页面状态文件，当前显示已按真实进程状态纠正。")
    st.write("本地地址：http://localhost:8501")
    st.info("下载任务已从界面移出。后续请在单独的 CMD 或 PowerShell 窗口执行数据下载与补数任务。")
    with st.expander("查看页面标准输出", expanded=False):
        st.code(streamlit_service_status.get("out_log_tail") or "(页面标准输出暂无内容)", language="text")
    with st.expander("查看页面错误输出", expanded=False):
        st.code(streamlit_service_status.get("err_log_tail") or "(页面错误输出暂无内容)", language="text")
    st.markdown("**当前说明**")
    st.markdown(
        """
- 当前仍是免费数据路线，存在“当前成分股回看历史”的幸存者偏差。
- 现阶段主要目标是把研究平台、数据链路和模型流程跑通。
- 下载与补数任务已改为页面外执行，界面只保留研究、持仓和 AI 研判相关操作。
"""
    )
