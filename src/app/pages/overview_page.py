from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import streamlit as st

from src.app.pages.payload_utils import records_to_frame
from src.app.viewmodels.overview_vm import build_equity_curve_frame, build_model_comparison_frame


def render_overview_page(
    *,
    summary: dict[str, object],
    watchlist_count: int,
    metrics_table: pd.DataFrame,
    model_names: list[str],
    splits: list[str],
    split_labels: dict[str, str],
    model_labels: dict[str, str],
    metric_explanations: dict[str, str],
    zh: Callable[[str], str],
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
    load_portfolio: Callable[[str, str], pd.DataFrame],
) -> None:
    st.subheader("平台总览")

    top_cards = st.columns(5)
    top_cards[0].metric("已缓存股票", int(summary.get("cached_symbols", 0) or 0))
    top_cards[1].metric("特征样本数", int(summary.get("feature_rows", 0) or 0))
    top_cards[2].metric("可研究股票数", int(summary.get("feature_symbols", 0) or 0))
    top_cards[3].metric("日线面板", "已生成" if summary.get("daily_bar", {}).get("exists") else "缺失")
    top_cards[4].metric("观察池股票", int(watchlist_count))

    date_min = summary.get("date_min")
    date_max = summary.get("date_max")
    if date_min and date_max:
        st.info(f"当前可研究数据区间：{date_min} 到 {date_max}")

    st.markdown("**数据面板状态**")
    status_cols = st.columns(3)
    for card, name, key in zip(
        status_cols,
        ["日线面板", "特征面板", "标签面板"],
        ["daily_bar", "features", "labels"],
        strict=False,
    ):
        file_info = summary.get(key, {}) or {}
        with card:
            st.markdown(f"**{name}**")
            if file_info.get("exists"):
                st.write(f"大小：{file_info.get('size_mb', '-') } MB")
                st.write(f"更新时间：{file_info.get('updated', '-')}")
            else:
                st.warning("还没有生成。")

    st.markdown("**模型对比**")
    if metrics_table.empty:
        st.warning("模型结果还没有生成，请先在左侧运行基线训练。")
        return

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
    if not comparison.empty:
        st.dataframe(prettify_dataframe(comparison), width="stretch")

    with st.expander("主要指标怎么理解", expanded=False):
        metric_guide = pd.DataFrame(
            [{"指标": zh(key), "说明": value} for key, value in metric_explanations.items()]
        )
        st.dataframe(metric_guide, width="stretch")

    split_for_curve = st.selectbox(
        "净值曲线对比数据集",
        splits,
        index=1 if len(splits) > 1 else 0,
        format_func=lambda value: split_labels[value],
    )
    chart_frame = build_equity_curve_frame(
        model_names=model_names,
        split_name=split_for_curve,
        model_labels=model_labels,
        load_portfolio=load_portfolio,
    )
    if not chart_frame.empty:
        st.line_chart(chart_frame)


def render_overview_payload_page(
    *,
    payload: dict[str, object],
    watchlist_count: int,
    splits: list[str],
    split_labels: dict[str, str],
    metric_explanations: dict[str, str],
    zh: Callable[[str], str],
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    summary = dict(payload.get("summary", {}) or {})
    comparison = records_to_frame(payload.get("comparison"))  # type: ignore[arg-type]
    chart_frame = records_to_frame(payload.get("equityCurves"), index_col="trade_date")  # type: ignore[arg-type]
    selected_split = str(payload.get("selectedSplit", "test") or "test")

    st.subheader("平台总览")

    top_cards = st.columns(5)
    top_cards[0].metric("已缓存股票", int(summary.get("cached_symbols", 0) or 0))
    top_cards[1].metric("特征样本数", int(summary.get("feature_rows", 0) or 0))
    top_cards[2].metric("可研究股票数", int(summary.get("feature_symbols", 0) or 0))
    top_cards[3].metric("日线面板", "已生成" if summary.get("daily_bar", {}).get("exists") else "缺失")
    top_cards[4].metric("观察池股票", int(watchlist_count))

    date_min = summary.get("date_min")
    date_max = summary.get("date_max")
    if date_min and date_max:
        st.info(f"当前可研究数据区间：{date_min} 到 {date_max}")

    st.markdown("**数据面板状态**")
    status_cols = st.columns(3)
    for card, name, key in zip(
        status_cols,
        ["日线面板", "特征面板", "标签面板"],
        ["daily_bar", "features", "labels"],
        strict=False,
    ):
        file_info = summary.get(key, {}) or {}
        with card:
            st.markdown(f"**{name}**")
            if file_info.get("exists"):
                st.write(f"大小：{file_info.get('size_mb', '-')} MB")
                st.write(f"更新时间：{file_info.get('updated', '-')}")
            else:
                st.warning("还没有生成。")

    st.markdown("**模型对比**")
    if comparison.empty:
        st.warning("模型结果还没有生成，请先在左侧运行基线训练。")
        return

    st.dataframe(prettify_dataframe(comparison), width="stretch")

    with st.expander("主要指标怎么理解", expanded=False):
        metric_guide = pd.DataFrame(
            [{"指标": zh(key), "说明": value} for key, value in metric_explanations.items()]
        )
        st.dataframe(metric_guide, width="stretch")

    default_index = splits.index(selected_split) if selected_split in splits else (1 if len(splits) > 1 else 0)
    st.selectbox(
        "净值曲线对比数据集",
        splits,
        index=default_index,
        key="overview_split",
        format_func=lambda value: split_labels[value],
    )
    if not chart_frame.empty:
        st.line_chart(chart_frame)
