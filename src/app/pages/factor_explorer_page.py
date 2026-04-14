from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import streamlit as st

from src.app.pages.payload_utils import records_to_frame
from src.app.viewmodels.factor_explorer_vm import (
    build_factor_ranking,
    build_latest_factor_snapshot,
    build_missing_rate_table,
    list_numeric_factor_columns,
)


def render_factor_explorer_page(
    *,
    feature_panel: pd.DataFrame,
    zh: Callable[[str], str],
    explain: Callable[[str], str],
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
    symbol_history: Callable[[pd.DataFrame, str, str, int], pd.DataFrame],
) -> None:
    st.subheader("因子探索")
    if feature_panel.empty:
        st.warning("特征面板还没有生成。")
        return

    numeric_columns = list_numeric_factor_columns(feature_panel)
    if not numeric_columns:
        st.warning("当前没有可展示的数值型因子。")
        return

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
        ranking = build_factor_ranking(cross_section, factor_name)
        if not ranking.empty:
            ranking = ranking.rename(columns={factor_name: zh(factor_name)})
        st.markdown(f"**最新截面日期：{pd.Timestamp(latest_date).date()}**")
        st.dataframe(prettify_dataframe(ranking.head(20)), width="stretch")

        missing_rate = build_missing_rate_table(feature_panel, numeric_columns)
        if not missing_rate.empty:
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
            history = symbol_history(feature_panel, symbol, history_factor, 240)
            if not history.empty:
                st.line_chart(history.rename(columns={history_factor: zh(history_factor)}))
            latest_snapshot = build_latest_factor_snapshot(cross_section, symbol=symbol, zh=zh)
            if not latest_snapshot.empty:
                st.markdown("**该股票最新因子快照**")
                st.dataframe(latest_snapshot, width="stretch")


def render_factor_explorer_payload_page(
    *,
    payload: dict[str, object],
    zh: Callable[[str], str],
    explain: Callable[[str], str],
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    st.subheader("因子探索")
    if not bool(payload.get("available", False)):
        st.warning("特征面板还没有生成。")
        return

    factor_options = payload.get("factorOptions", []) or []
    numeric_columns = [str(item.get("key")) for item in factor_options if item.get("key")]
    if not numeric_columns:
        st.warning("当前没有可展示的数值型因子。")
        return

    factor_descriptions = {
        str(item.get("key")): str(item.get("description") or "")
        for item in factor_options
        if item.get("key")
    }
    latest_date = payload.get("latestDate")
    selected_factor = str(payload.get("selectedFactor") or numeric_columns[0])
    selected_history_factor = str(payload.get("selectedHistoryFactor") or numeric_columns[0])
    selected_symbol = str(payload.get("selectedSymbol") or "")
    ranking = records_to_frame(payload.get("ranking"))  # type: ignore[arg-type]
    missing_rate = records_to_frame(payload.get("missingRates"))  # type: ignore[arg-type]
    history = records_to_frame(payload.get("history"), index_col="trade_date")  # type: ignore[arg-type]
    latest_snapshot = records_to_frame(payload.get("snapshot"))  # type: ignore[arg-type]
    symbol_options = [str(item) for item in (payload.get("symbolOptions") or [])]

    left, right = st.columns([1.1, 1.4])
    with left:
        factor_index = numeric_columns.index(selected_factor) if selected_factor in numeric_columns else 0
        current_factor = st.selectbox(
            "查看排序的因子",
            numeric_columns,
            index=factor_index,
            key="factor_name",
            format_func=zh,
        )
        st.caption(factor_descriptions.get(current_factor) or explain(current_factor))
        st.markdown(f"**最新截面日期：{pd.Timestamp(latest_date).date()}**" if latest_date else "**最新截面日期：-**")
        st.dataframe(prettify_dataframe(ranking.head(20)), width="stretch")

        if not missing_rate.empty:
            st.markdown("**缺失率最高的特征**")
            st.dataframe(prettify_dataframe(missing_rate.head(15)), width="stretch")

    with right:
        if not symbol_options:
            st.info("当前截面没有可查看的股票。")
            return
        symbol_index = symbol_options.index(selected_symbol) if selected_symbol in symbol_options else 0
        st.selectbox("查看股票因子历史", symbol_options, index=symbol_index, key="factor_symbol")
        history_index = numeric_columns.index(selected_history_factor) if selected_history_factor in numeric_columns else 0
        current_history_factor = st.selectbox(
            "历史走势因子",
            numeric_columns,
            index=history_index,
            key="history_factor",
            format_func=zh,
        )
        st.caption(factor_descriptions.get(current_history_factor) or explain(current_history_factor))
        if not history.empty:
            st.line_chart(history.rename(columns={current_history_factor: zh(current_history_factor)}))
        if not latest_snapshot.empty:
            st.markdown("**该股票最新因子快照**")
            st.dataframe(latest_snapshot, width="stretch")
