from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import streamlit as st

from src.app.pages.payload_utils import records_to_frame
from src.app.viewmodels.candidates_vm import build_candidate_score_history, build_top_candidates_snapshot


def render_candidates_page(
    *,
    experiment_config: dict,
    model_names: list[str],
    splits: list[str],
    model_labels: dict[str, str],
    split_labels: dict[str, str],
    load_predictions: Callable[[str, str], pd.DataFrame],
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    st.subheader("最新候选股票")
    result_model = st.selectbox(
        "结果模型",
        model_names,
        index=1,
        key="pick_model",
        format_func=lambda value: model_labels[value],
    )
    result_split = st.selectbox(
        "查看数据集",
        splits,
        index=1,
        key="pick_split",
        format_func=lambda value: split_labels[value],
    )
    top_n_default = int(experiment_config.get("top_n", 10))
    top_n = st.slider(
        "显示前 N 只候选股票",
        min_value=1,
        max_value=max(30, top_n_default),
        value=min(max(1, top_n_default), max(30, top_n_default)),
        step=1,
    )
    predictions = load_predictions(result_model, result_split)

    if predictions.empty:
        st.warning("预测文件还没有生成。")
        return

    latest_picks = build_top_candidates_snapshot(predictions, top_n=top_n)
    if latest_picks.empty:
        st.warning("当前模型没有可展示的预测结果。")
        return

    latest_date = latest_picks["trade_date"].iloc[0]
    st.markdown(f"**最新预测日期：{pd.Timestamp(latest_date).date()}**")
    columns = [
        "trade_date",
        "rank",
        "rank_pct",
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
    symbol_predictions = build_candidate_score_history(predictions, symbol=inspect_symbol)
    if symbol_predictions.empty:
        st.info("该股票暂无可展示的评分历史。")
        return

    st.line_chart(symbol_predictions[["综合评分"]])
    st.dataframe(symbol_predictions.tail(20), width="stretch")


def render_candidates_payload_page(
    *,
    payload: dict[str, object],
    experiment_config: dict,
    model_names: list[str],
    splits: list[str],
    model_labels: dict[str, str],
    split_labels: dict[str, str],
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    st.subheader("最新候选股票")
    selected_model = str(payload.get("modelName", "lgbm") or "lgbm")
    selected_split = str(payload.get("splitName", "test") or "test")
    top_n_default = int(experiment_config.get("top_n", 10))
    top_n = int(payload.get("topN", top_n_default) or top_n_default)
    model_index = model_names.index(selected_model) if selected_model in model_names else 0
    split_index = splits.index(selected_split) if selected_split in splits else 0
    st.selectbox(
        "结果模型",
        model_names,
        index=model_index,
        key="pick_model",
        format_func=lambda value: model_labels[value],
    )
    st.selectbox(
        "查看数据集",
        splits,
        index=split_index,
        key="pick_split",
        format_func=lambda value: split_labels[value],
    )
    st.slider(
        "显示前 N 只候选股票",
        min_value=1,
        max_value=max(30, top_n_default),
        value=min(max(1, top_n), max(30, top_n_default)),
        step=1,
        key="pick_top_n",
    )

    latest_picks = records_to_frame(payload.get("latestPicks"))  # type: ignore[arg-type]
    if latest_picks.empty:
        st.warning("当前模型没有可展示的预测结果。")
        return

    latest_date = payload.get("latestDate")
    if latest_date:
        st.markdown(f"**最新预测日期：{pd.Timestamp(latest_date).date()}**")
    columns = [
        "trade_date",
        "rank",
        "rank_pct",
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

    symbol_options = [str(item) for item in (payload.get("symbolOptions") or [])]
    selected_symbol = str(payload.get("selectedSymbol") or "")
    if not symbol_options:
        return
    symbol_index = symbol_options.index(selected_symbol) if selected_symbol in symbol_options else 0
    st.selectbox("查看单只股票评分历史", symbol_options, index=symbol_index, key="inspect_symbol")
    score_history = records_to_frame(payload.get("scoreHistory"), index_col="trade_date")  # type: ignore[arg-type]
    if score_history.empty:
        st.info("该股票暂无可展示的评分历史。")
        return

    if "综合评分" in score_history.columns:
        st.line_chart(score_history[["综合评分"]])
    else:
        st.line_chart(score_history)
    st.dataframe(score_history.tail(20), width="stretch")
