from __future__ import annotations
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.agents.ensemble_weights import resolve_model_weights
from src.agents.llm_bridge import export_llm_requests
from src.agents.news_context import build_event_context
from src.app.repositories.config_repository import load_experiment_config
from src.app.repositories.report_repository import (
    load_daily_bar as repo_load_daily_bar,
    load_metrics as repo_load_metrics,
    load_portfolio as repo_load_portfolio,
    load_predictions as repo_load_predictions,
    save_overlay_outputs as repo_save_overlay_outputs,
)
from src.utils.data_source import active_data_source, source_or_canonical_path
from src.utils.io import ensure_dir, project_root
from src.utils.logger import configure_logging

logger = configure_logging()

MODEL_NAMES = ("lgbm", "ridge")
DISPLAY_COLUMNS = [
    "trade_date",
    "ts_code",
    "name",
    "industry",
    "industry_display",
    "theme_tags",
    "action_hint",
    "final_score",
    "quant_score",
    "factor_overlay_score",
    "model_consensus",
    "confidence_level",
    "lgbm_rank_pct",
    "ridge_rank_pct",
    "bull_points",
    "risk_points",
    "notice_digest",
    "news_digest",
    "news_source",
    "research_digest",
    "thesis_summary",
    "ai_brief",
]

FACTOR_RULES = {
    "mom_20": {
        "label": "20日趋势",
        "direction": "high",
        "weight": 1.0,
        "positive": "20日趋势偏强",
        "negative": "20日趋势偏弱",
    },
    "mom_60": {
        "label": "60日趋势",
        "direction": "high",
        "weight": 1.4,
        "positive": "60日趋势明显占优",
        "negative": "60日趋势走弱",
    },
    "close_to_ma_20": {
        "label": "20日均线位置",
        "direction": "high",
        "weight": 0.8,
        "positive": "股价站上20日均线",
        "negative": "股价跌破20日均线",
    },
    "close_to_ma_60": {
        "label": "60日均线位置",
        "direction": "high",
        "weight": 1.1,
        "positive": "股价站上60日均线",
        "negative": "股价跌破60日均线",
    },
    "drawdown_60": {
        "label": "60日回撤",
        "direction": "high",
        "weight": 1.2,
        "positive": "近60日回撤控制较好",
        "negative": "近60日回撤偏深",
    },
    "vol_20": {
        "label": "20日波动",
        "direction": "low",
        "weight": 0.9,
        "positive": "短期波动相对可控",
        "negative": "短期波动偏大",
    },
    "downside_vol_20": {
        "label": "20日下行波动",
        "direction": "low",
        "weight": 1.1,
        "positive": "下行波动较小",
        "negative": "下行波动偏大",
    },
    "amount_20": {
        "label": "20日成交额",
        "direction": "high",
        "weight": 0.7,
        "positive": "成交额较充足",
        "negative": "成交额一般",
    },
    "ret_skew_20": {
        "label": "20日收益偏度",
        "direction": "high",
        "weight": 0.5,
        "positive": "收益分布偏向正向跳升",
        "negative": "收益分布偏向负向尾部",
    },
}

THEME_RULES = [
    ("新能源链", ("新能源", "锂电", "光伏", "储能", "电池", "风电", "能源金属")),
    ("科技硬件", ("半导体", "消费电子", "元件", "电子", "面板", "设备")),
    ("数字科技", ("计算机", "软件", "通信", "互联网", "传媒")),
    ("医药健康", ("医药", "生物", "医疗", "器械", "制药")),
    ("资源材料", ("有色", "金属", "化工", "材料", "钢铁", "煤炭")),
    ("制造升级", ("机械", "装备", "汽车", "军工", "自动化")),
    ("消费内需", ("食品", "饮料", "家电", "家居", "商贸", "零售", "旅游")),
    ("金融地产", ("银行", "证券", "保险", "房地产", "多元金融")),
    ("公用事业", ("电力", "公用事业", "燃气", "港口", "交通运输")),
]


def _prefer_database(root: Path) -> bool:
    try:
        return root.resolve() == project_root().resolve()
    except OSError:
        return False


def _overlay_config(experiment: dict) -> dict:
    overlay = dict(experiment.get("overlay", {}))
    overlay.setdefault("split", "test")
    overlay.setdefault("candidate_pool_size", max(int(experiment.get("top_n", 20)) * 2, 30))
    overlay.setdefault("top_n", min(int(experiment.get("top_n", 20)), 10))
    overlay.setdefault("lgbm_weight", 0.6)
    overlay.setdefault("ridge_weight", 0.4)
    overlay.setdefault("weight_mode", "validation_adaptive")
    overlay.setdefault("weight_evaluation_split", "valid")
    overlay.setdefault("min_model_weight", 0.2)
    overlay.setdefault("quant_weight", 0.7)
    overlay.setdefault("factor_weight", 0.2)
    overlay.setdefault("consensus_weight", 0.1)
    overlay.setdefault("notice_lookback_days", 7)
    overlay.setdefault("notice_max_items", 3)
    overlay.setdefault("news_lookback_days", 7)
    overlay.setdefault("news_max_items", 3)
    overlay.setdefault("research_lookback_days", 30)
    overlay.setdefault("research_max_items", 3)
    return overlay


def _load_predictions(root: Path, data_source: str, model_name: str, split_name: str) -> pd.DataFrame:
    frame = repo_load_predictions(
        root,
        data_source=data_source,
        model_name=model_name,
        split_name=split_name,
        prefer_database=_prefer_database(root),
    )
    if frame.empty:
        return frame
    frame = frame.copy()
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def _load_metrics(root: Path, data_source: str, model_name: str, split_name: str) -> dict:
    return repo_load_metrics(
        root,
        data_source=data_source,
        model_name=model_name,
        split_name=split_name,
        prefer_database=_prefer_database(root),
    )


def _load_portfolio(root: Path, data_source: str, model_name: str, split_name: str) -> pd.DataFrame:
    frame = repo_load_portfolio(
        root,
        data_source=data_source,
        model_name=model_name,
        split_name=split_name,
        prefer_database=_prefer_database(root),
    )
    if frame.empty:
        return frame
    frame = frame.copy()
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def _load_industry_name_map(root: Path, data_source: str) -> dict[str, str]:
    mapping: dict[str, str] = {}

    snapshot_path = root / "data" / "staging" / "stock_snapshot.parquet"
    if snapshot_path.exists():
        snapshot = pd.read_parquet(snapshot_path)
        if {"ts_code", "industry"}.issubset(snapshot.columns):
            valid = snapshot.loc[snapshot["industry"].notna()].copy()
            mapping.update(valid.drop_duplicates("ts_code").set_index("ts_code")["industry"].astype(str).to_dict())

    board_path = root / "data" / "staging" / "industry_board_map.parquet"
    if board_path.exists():
        board_map = pd.read_parquet(board_path)
        if {"ts_code", "industry"}.issubset(board_map.columns):
            valid = board_map.loc[board_map["industry"].notna()].copy()
            mapping.update(valid.drop_duplicates("ts_code").set_index("ts_code")["industry"].astype(str).to_dict())

    daily_bar = repo_load_daily_bar(root, data_source=data_source, prefer_database=_prefer_database(root))
    if not daily_bar.empty and "ts_code" in daily_bar.columns:
        read_columns = [column for column in ("trade_date", "ts_code", "industry_current", "industry") if column in daily_bar.columns]
        if "ts_code" not in read_columns:
            return mapping
        daily_bar = daily_bar.loc[:, read_columns].copy()
        if "trade_date" in daily_bar.columns:
            daily_bar["trade_date"] = pd.to_datetime(daily_bar["trade_date"], errors="coerce")
            daily_bar = daily_bar.sort_values("trade_date")
        daily_bar = daily_bar.drop_duplicates("ts_code", keep="last")
        if "industry_current" in daily_bar.columns:
            current_valid = daily_bar.loc[
                daily_bar["industry_current"].notna()
                & ~daily_bar["industry_current"].astype(str).str.startswith("SW_")
            ].copy()
            mapping.update(current_valid.set_index("ts_code")["industry_current"].astype(str).to_dict())
        if "industry" in daily_bar.columns:
            fallback = daily_bar.loc[
                daily_bar["industry"].notna()
                & ~daily_bar["industry"].astype(str).str.startswith("SW_")
            ].copy()
            missing = {
                ts_code: str(industry)
                for ts_code, industry in fallback.set_index("ts_code")["industry"].astype(str).to_dict().items()
                if ts_code not in mapping
            }
            mapping.update(missing)

    return mapping


def _pct_rank(series: pd.Series, direction: str = "high") -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    ranked = pd.Series(np.nan, index=series.index, dtype=float)
    mask = numeric.notna()
    if not mask.any():
        return ranked
    ranked.loc[mask] = numeric.loc[mask].rank(pct=True, ascending=(direction == "low"))
    return ranked


def _confidence_label(final_score: float, consensus: float) -> str:
    if final_score >= 0.82 and consensus >= 0.8:
        return "高"
    if final_score >= 0.68 and consensus >= 0.6:
        return "中高"
    if final_score >= 0.55:
        return "中等"
    return "观察"


def _apply_factor_overlay(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    factor_score_parts: list[pd.Series] = []
    total_weight = 0.0
    for column, rule in FACTOR_RULES.items():
        if column not in working.columns:
            continue
        pct_column = f"{column}_pct"
        working[pct_column] = _pct_rank(working[column], direction=rule["direction"])
        factor_score_parts.append(working[pct_column].fillna(0.5) * float(rule["weight"]))
        total_weight += float(rule["weight"])

    if factor_score_parts and total_weight > 0:
        working["factor_overlay_score"] = (sum(factor_score_parts) / total_weight).clip(0.0, 1.0)
    else:
        working["factor_overlay_score"] = 0.5
    return working


def _build_reasons(row: pd.Series) -> tuple[str, str]:
    positives: list[str] = []
    negatives: list[str] = []

    for column, rule in FACTOR_RULES.items():
        pct_value = row.get(f"{column}_pct")
        if pd.isna(pct_value):
            continue
        if float(pct_value) >= 0.7:
            positives.append(str(rule["positive"]))
        elif float(pct_value) <= 0.3:
            negatives.append(str(rule["negative"]))

    disagreement = float(row.get("model_disagreement", 0.0) or 0.0)
    if disagreement <= 0.12:
        positives.append("两套模型观点高度一致")
    elif disagreement >= 0.35:
        negatives.append("两套模型分歧较大")

    research_signal = str(row.get("research_signal", "") or "")
    if research_signal == "偏积极":
        positives.append("近期有偏积极研报跟踪")
    elif research_signal == "偏谨慎":
        negatives.append("近期研报观点偏谨慎")

    if float(row.get("days_since_list", 0.0) or 0.0) < 500:
        negatives.append("上市时间相对较短")

    positive_text = "；".join(positives[:3]) if positives else "当前没有特别突出的量化顺风信号"
    negative_text = "；".join(negatives[:3]) if negatives else "当前没有特别突出的量化风险"
    return positive_text, negative_text


def _industry_display(row: pd.Series, industry_name_map: dict[str, str]) -> str:
    ts_code = str(row.get("ts_code", ""))
    readable = industry_name_map.get(ts_code)
    raw_industry = str(row.get("industry", "") or "")
    if readable:
        if raw_industry.startswith("SW_") and readable != raw_industry:
            return f"{readable}（{raw_industry}）"
        return readable
    return raw_industry or "未知行业"


def _theme_tags(row: pd.Series, market_risk_on: bool | None) -> str:
    tags: list[str] = []
    industry_text = str(row.get("industry_display", "") or "")
    for tag_name, keywords in THEME_RULES:
        if any(keyword in industry_text for keyword in keywords):
            tags.append(tag_name)
            break

    if float(row.get("model_consensus", 0.0) or 0.0) >= 0.9:
        tags.append("双模型共识")
    if float(row.get("factor_overlay_score", 0.0) or 0.0) >= 0.6:
        tags.append("因子顺风")
    if float(row.get("mom_20_pct", 0.5) or 0.5) >= 0.7 and float(row.get("close_to_ma_20_pct", 0.5) or 0.5) >= 0.7:
        tags.append("短线走强")
    if float(row.get("drawdown_60_pct", 0.5) or 0.5) >= 0.7 and float(row.get("vol_20_pct", 0.5) or 0.5) >= 0.6:
        tags.append("低回撤")
    if market_risk_on is False:
        tags.append("趋势过滤期")

    ordered: list[str] = []
    for tag in tags:
        if tag not in ordered:
            ordered.append(tag)
    return " / ".join(ordered) if ordered else "常规观察"


def _action_hint(row: pd.Series, market_risk_on: bool | None) -> str:
    score = float(row.get("final_score", 0.0) or 0.0)
    consensus = float(row.get("model_consensus", 0.0) or 0.0)
    if market_risk_on is False and score < 0.9:
        return "趋势过滤期，先观察"
    if score >= 0.85 and consensus >= 0.8:
        return "重点跟踪"
    if score >= 0.7:
        return "进入候选池"
    if score >= 0.55:
        return "继续观察"
    return "暂缓纳入"


def _thesis_summary(row: pd.Series, market_risk_on: bool | None) -> str:
    direction = "偏积极" if float(row.get("quant_score", 0.0) or 0.0) >= 0.6 else "偏中性"
    market_text = "当前大盘趋势允许进攻" if market_risk_on is not False else "当前大盘处于趋势过滤阶段"
    notice_raw = row.get("notice_signal", "中性")
    notice_text = "中性" if pd.isna(notice_raw) else str(notice_raw)
    research_raw = row.get("research_signal", "中性")
    research_text = "中性" if pd.isna(research_raw) else str(research_raw)
    if notice_text == "偏积极" or research_text == "偏积极":
        event_text = "公告面偏积极"
    elif notice_text == "偏谨慎" or research_text == "偏谨慎":
        event_text = "公告面偏谨慎"
    else:
        event_text = "公告面暂时中性"
    return (
        f"{row.get('name', row.get('ts_code', '该标的'))} 属于 {row.get('industry_display', '未知行业')}，"
        f"量化主信号{direction}，{market_text}，{event_text}，更适合作为“{row.get('action_hint', '继续观察')}”对象。"
    )


def _compose_brief(row: pd.Series) -> str:
    notice_raw = row.get("notice_digest", "")
    news_raw = row.get("news_digest", "")
    research_raw = row.get("research_digest", "")
    notice_digest = "暂无公告摘要。" if pd.isna(notice_raw) or not str(notice_raw).strip() else str(notice_raw)
    news_digest = "暂无新闻/研报摘要。" if pd.isna(news_raw) or not str(news_raw).strip() else str(news_raw)
    research_digest = "暂无研报补充。" if pd.isna(research_raw) or not str(research_raw).strip() else str(research_raw)
    news_source = str(row.get("news_source", "") or "未知")
    if news_source == "研报回退" and research_digest == news_digest:
        research_digest = "已合并到上方新闻/研报摘要。"
    return (
        f"{row.get('ts_code', '')} 当前综合得分为 {float(row.get('final_score', 0.0)):.2f}，"
        f"建议动作是“{row.get('action_hint', '继续观察')}”。"
        f"正面主要来自：{row.get('bull_points', '暂无')}。"
        f"需要留意：{row.get('risk_points', '暂无')}。"
        f"公告摘要：{notice_digest}"
        f"资讯来源：{news_source}"
        f"新闻/研报摘要：{news_digest}"
        f"研报补充：{research_digest}"
    )


def _compose_agent_prompt(row: pd.Series, market_risk_on: bool | None) -> str:
    market_text = "趋势开启" if market_risk_on is not False else "趋势过滤"
    notice_raw = row.get("notice_digest", "")
    news_raw = row.get("news_digest", "")
    research_raw = row.get("research_digest", "")
    notice_digest = "暂无公告摘要。" if pd.isna(notice_raw) or not str(notice_raw).strip() else str(notice_raw)
    news_digest = "暂无新闻/研报摘要。" if pd.isna(news_raw) or not str(news_raw).strip() else str(news_raw)
    research_digest = "暂无研报补充。" if pd.isna(research_raw) or not str(research_raw).strip() else str(research_raw)
    news_source = str(row.get("news_source", "") or "未知")
    if news_source == "研报回退" and research_digest == news_digest:
        research_digest = "已合并到上方新闻/研报摘要。"
    return (
        f"请围绕 A 股个股 {row.get('ts_code', '')}（{row.get('name', '')}）做二次研判。\n"
        f"- 当前行业：{row.get('industry_display', row.get('industry', '未知'))}\n"
        f"- 当前市场状态：{market_text}\n"
        f"- 建议动作：{row.get('action_hint', '继续观察')}\n"
        f"- 主题标签：{row.get('theme_tags', '常规观察')}\n"
        f"- 量化综合分：{float(row.get('final_score', 0.0)):.2f}\n"
        f"- 模型共识度：{float(row.get('model_consensus', 0.0)):.2f}\n"
        f"- 正面要点：{row.get('bull_points', '暂无')}\n"
        f"- 风险提示：{row.get('risk_points', '暂无')}\n"
        f"- 历史公告摘要：{notice_digest}\n"
        f"- 资讯来源：{news_source}\n"
        f"- 历史新闻/研报摘要：{news_digest}\n"
        f"- 历史研报补充：{research_digest}\n"
        "请重点补充：主营业务变化、近期新闻、业绩催化、估值压力、行业风险，"
        "以及这些信息是否支持继续纳入候选池。请用中文输出，并给出一句结论。"
    )


def _latest_risk_state(portfolio: pd.DataFrame) -> dict:
    if portfolio.empty:
        return {}
    latest_row = portfolio.sort_values("trade_date").iloc[-1].to_dict()
    trade_date = latest_row.get("trade_date")
    risk_on = bool(latest_row.get("risk_on", True)) if "risk_on" in latest_row else None
    return {
        "trade_date": str(pd.Timestamp(trade_date).date()) if trade_date is not None else None,
        "risk_on": risk_on,
        "risk_state": latest_row.get("risk_state"),
        "benchmark_proxy_close": latest_row.get("benchmark_proxy_close"),
        "benchmark_ma": latest_row.get("benchmark_ma"),
    }


def _build_markdown(
    data_source: str,
    split_name: str,
    latest_date: pd.Timestamp,
    selected: pd.DataFrame,
    latest_risk_state: dict,
) -> str:
    risk_text = "趋势开启" if latest_risk_state.get("risk_on") else "趋势过滤"
    lines = [
        f"# AI 叠加研判摘要（{data_source} / {split_name}）",
        "",
        f"- 最新截面日期：{latest_date.date()}",
        f"- 当前组合风控状态：{risk_text}" if latest_risk_state else "- 当前组合风控状态：未知",
        f"- 入选候选数量：{len(selected)}",
        "",
        "## 入选候选股",
        "",
    ]

    for idx, row in selected.reset_index(drop=True).iterrows():
        lines.extend(
            [
                f"### {idx + 1}. {row.get('ts_code', '')} {row.get('name', '')}",
                f"- 行业：{row.get('industry_display', row.get('industry', '未知行业'))}",
                f"- 主题标签：{row.get('theme_tags', '常规观察')}",
                f"- 建议动作：{row.get('action_hint', '继续观察')}",
                f"- 综合总分：{float(row.get('final_score', 0.0)):.2f}",
                f"- 量化合成分：{float(row.get('quant_score', 0.0)):.2f}",
                f"- 因子解释分：{float(row.get('factor_overlay_score', 0.0)):.2f}",
                f"- 模型共识：{float(row.get('model_consensus', 0.0)):.2f}",
                f"- 置信度：{row.get('confidence_level', '观察')}",
                f"- 正面要点：{row.get('bull_points', '暂无')}",
                f"- 风险提示：{row.get('risk_points', '暂无')}",
                f"- 公告摘要：{row.get('notice_digest', '暂无')}",
                f"- 资讯来源：{row.get('news_source', '未知')}",
                f"- 新闻/研报摘要：{row.get('news_digest', '暂无')}",
                f"- 研报补充：{row.get('research_digest', '暂无')}",
                f"- 结论摘要：{row.get('thesis_summary', '')}",
                "",
            ]
        )

    return "\n".join(lines)


def build_overlay_report_from_frames(
    *,
    root: Path,
    data_source: str,
    overlay: dict,
    split_name: str,
    lgbm: pd.DataFrame,
    ridge: pd.DataFrame,
    latest_risk_state: dict | None = None,
    model_metrics: dict | None = None,
    prediction_mode: str = "historical_split",
) -> tuple[pd.DataFrame, dict, str]:
    reports_dir = ensure_dir(root / "reports" / "weekly")
    if lgbm.empty or ridge.empty:
        raise RuntimeError("Overlay report requires both Ridge and LightGBM prediction files.")

    common_dates = sorted(
        set(pd.to_datetime(lgbm["trade_date"]).dt.normalize())
        & set(pd.to_datetime(ridge["trade_date"]).dt.normalize())
    )
    if not common_dates:
        raise RuntimeError("No common prediction date found between Ridge and LightGBM.")
    latest_date = pd.Timestamp(common_dates[-1])

    meta_columns = [
        "trade_date",
        "ts_code",
        "name",
        "industry",
        "index_code",
        "days_since_list",
        "mom_20",
        "mom_60",
        "close_to_ma_20",
        "close_to_ma_60",
        "drawdown_60",
        "vol_20",
        "downside_vol_20",
        "amount_20",
        "ret_skew_20",
    ]
    lgbm_latest = (
        lgbm.loc[pd.to_datetime(lgbm["trade_date"]).dt.normalize() == latest_date, [column for column in meta_columns if column in lgbm.columns] + ["score"]]
        .rename(columns={"score": "lgbm_score"})
        .copy()
    )
    ridge_latest = (
        ridge.loc[pd.to_datetime(ridge["trade_date"]).dt.normalize() == latest_date, ["ts_code", "score"]]
        .rename(columns={"score": "ridge_score"})
        .copy()
    )
    merged = lgbm_latest.merge(ridge_latest, on="ts_code", how="inner")
    if merged.empty:
        raise RuntimeError("Latest overlay snapshot is empty after merging Ridge and LightGBM predictions.")

    industry_name_map = _load_industry_name_map(root, data_source)

    merged["lgbm_rank_pct"] = _pct_rank(merged["lgbm_score"], direction="high")
    merged["ridge_rank_pct"] = _pct_rank(merged["ridge_score"], direction="high")
    ensemble_weights = resolve_model_weights(
        reports_dir=reports_dir,
        data_source=data_source,
        overlay_config=overlay,
        model_names=MODEL_NAMES,
    )
    lgbm_weight = float(ensemble_weights["weights"].get("lgbm", overlay["lgbm_weight"]))
    ridge_weight = float(ensemble_weights["weights"].get("ridge", overlay["ridge_weight"]))
    merged["quant_score"] = (
        merged["lgbm_rank_pct"].fillna(0.5) * lgbm_weight
        + merged["ridge_rank_pct"].fillna(0.5) * ridge_weight
    ) / max(lgbm_weight + ridge_weight, 1e-9)
    merged["model_disagreement"] = (merged["lgbm_rank_pct"] - merged["ridge_rank_pct"]).abs()
    merged["model_consensus"] = 1.0 - merged["model_disagreement"].clip(0.0, 1.0)
    merged = _apply_factor_overlay(merged)

    quant_weight = float(overlay["quant_weight"])
    factor_weight = float(overlay["factor_weight"])
    consensus_weight = float(overlay["consensus_weight"])
    total_weight = max(quant_weight + factor_weight + consensus_weight, 1e-9)
    merged["final_score"] = (
        merged["quant_score"].fillna(0.5) * quant_weight
        + merged["factor_overlay_score"].fillna(0.5) * factor_weight
        + merged["model_consensus"].fillna(0.5) * consensus_weight
    ) / total_weight
    merged["confidence_level"] = merged.apply(
        lambda row: _confidence_label(float(row["final_score"]), float(row["model_consensus"])),
        axis=1,
    )

    candidate_pool_size = int(overlay["candidate_pool_size"])
    top_n = int(overlay["top_n"])
    preliminary_ranked = (
        merged.sort_values(["final_score", "model_consensus", "quant_score"], ascending=False)
        .reset_index(drop=True)
        .copy()
    )
    event_codes = preliminary_ranked.head(max(candidate_pool_size, top_n))["ts_code"].astype(str).tolist()
    event_context = build_event_context(
        ts_codes=event_codes,
        as_of_date=latest_date,
        cache_dir=root / "data" / "staging" / "event_cache",
        data_source=data_source,
        notice_lookback_days=int(overlay["notice_lookback_days"]),
        notice_max_items=int(overlay["notice_max_items"]),
        news_lookback_days=int(overlay["news_lookback_days"]),
        news_max_items=int(overlay["news_max_items"]),
        research_lookback_days=int(overlay["research_lookback_days"]),
        research_max_items=int(overlay["research_max_items"]),
    )
    if not event_context.empty:
        merged = merged.merge(event_context, on="ts_code", how="left")

    resolved_latest_risk_state = latest_risk_state or {}
    market_risk_on = resolved_latest_risk_state.get("risk_on")
    merged["industry_display"] = merged.apply(lambda row: _industry_display(row, industry_name_map), axis=1)

    bull_points: list[str] = []
    risk_points: list[str] = []
    theme_tags: list[str] = []
    action_hints: list[str] = []
    thesis_summaries: list[str] = []
    ai_briefs: list[str] = []
    agent_prompts: list[str] = []

    for _, row in merged.iterrows():
        positive_text, negative_text = _build_reasons(row)
        working = row.copy()
        working["bull_points"] = positive_text
        working["risk_points"] = negative_text
        working["theme_tags"] = _theme_tags(working, market_risk_on)
        working["action_hint"] = _action_hint(working, market_risk_on)
        working["thesis_summary"] = _thesis_summary(working, market_risk_on)
        working["ai_brief"] = _compose_brief(working)
        working["agent_prompt"] = _compose_agent_prompt(working, market_risk_on)

        bull_points.append(working["bull_points"])
        risk_points.append(working["risk_points"])
        theme_tags.append(working["theme_tags"])
        action_hints.append(working["action_hint"])
        thesis_summaries.append(working["thesis_summary"])
        ai_briefs.append(working["ai_brief"])
        agent_prompts.append(working["agent_prompt"])

    merged["bull_points"] = bull_points
    merged["risk_points"] = risk_points
    merged["theme_tags"] = theme_tags
    merged["action_hint"] = action_hints
    merged["thesis_summary"] = thesis_summaries
    merged["ai_brief"] = ai_briefs
    merged["agent_prompt"] = agent_prompts

    overlay_candidates = (
        merged.sort_values(["final_score", "model_consensus", "quant_score"], ascending=False)
        .reset_index(drop=True)
        .head(candidate_pool_size)
        .copy()
    )
    selected = overlay_candidates.head(top_n).copy()

    resolved_model_metrics = model_metrics or {
        model_name: _load_metrics(root, data_source, model_name, split_name) for model_name in MODEL_NAMES
    }
    selected_packet_frame = selected[
        [
            "trade_date",
            "ts_code",
            "name",
            "industry",
            "industry_display",
            "theme_tags",
            "action_hint",
            "final_score",
            "quant_score",
            "factor_overlay_score",
            "model_consensus",
            "confidence_level",
            "bull_points",
            "risk_points",
            "notice_digest",
            "news_digest",
            "news_source",
            "research_digest",
            "thesis_summary",
            "ai_brief",
            "agent_prompt",
        ]
    ].copy()
    selected_packet_frame["trade_date"] = pd.to_datetime(selected_packet_frame["trade_date"]).dt.strftime("%Y-%m-%d")
    news_source_counts = (
        selected_packet_frame.get("news_source", pd.Series(dtype=str))
        .fillna("未知")
        .astype(str)
        .value_counts()
        .to_dict()
    )
    event_coverage = {
        "news_source_counts": news_source_counts,
        "notice_covered_count": int((selected.get("notice_count", pd.Series(dtype=float)).fillna(0) > 0).sum()),
        "research_covered_count": int((selected.get("research_count", pd.Series(dtype=float)).fillna(0) > 0).sum()),
    }

    packet = {
        "data_source": data_source,
        "split": split_name,
        "prediction_mode": prediction_mode,
        "latest_date": str(latest_date.date()),
        "candidate_pool_size": candidate_pool_size,
        "top_n": top_n,
        "model_metrics": resolved_model_metrics,
        "ensemble_weights": ensemble_weights,
        "event_coverage": event_coverage,
        "latest_risk_state": resolved_latest_risk_state,
        "selected_candidates": selected_packet_frame.to_dict(orient="records"),
    }
    markdown = _build_markdown(data_source, split_name, latest_date, selected, resolved_latest_risk_state)
    return overlay_candidates, packet, markdown


def build_overlay_report() -> tuple[pd.DataFrame, dict, str]:
    root = project_root()
    experiment = load_experiment_config(root, prefer_database=False)
    data_source = active_data_source()
    overlay = _overlay_config(experiment)
    split_name = str(overlay["split"])

    lgbm = _load_predictions(root, data_source, "lgbm", split_name)
    ridge = _load_predictions(root, data_source, "ridge", split_name)
    latest_risk_state = _latest_risk_state(_load_portfolio(root, data_source, "lgbm", split_name))
    return build_overlay_report_from_frames(
        root=root,
        data_source=data_source,
        overlay=overlay,
        split_name=split_name,
        lgbm=lgbm,
        ridge=ridge,
        latest_risk_state=latest_risk_state,
        prediction_mode="historical_split",
    )


def run() -> None:
    root = project_root()
    data_source = active_data_source()
    reports_dir = ensure_dir(root / "reports" / "weekly")

    overlay_candidates, packet, markdown = build_overlay_report()
    export_frame = overlay_candidates[[column for column in DISPLAY_COLUMNS if column in overlay_candidates.columns]].copy()
    repo_save_overlay_outputs(
        root=root,
        data_source=data_source,
        scope="historical",
        candidates=export_frame,
        packet=packet,
        brief=markdown,
    )

    llm_artifacts = export_llm_requests(
        packet=packet,
        reports_dir=reports_dir,
        data_source=data_source,
    )
    packet["llm_bridge"] = llm_artifacts
    repo_save_overlay_outputs(
        root=root,
        data_source=data_source,
        scope="historical",
        candidates=export_frame,
        packet=packet,
        brief=markdown,
    )
    from src.db.dashboard_sync import sync_watchlist_snapshot_artifact

    summary = sync_watchlist_snapshot_artifact(root=root, data_source=data_source)
    logger.info(summary.message if summary.ok else f"Watchlist snapshot sync failed: {summary.message}")
    logger.info(f"Saved overlay reports to {reports_dir}")


if __name__ == "__main__":
    run()
