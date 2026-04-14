from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import streamlit as st

from src.app.pages.payload_utils import records_to_frame
from src.app.viewmodels.model_backtest_vm import build_monthly_summary, normalize_regime_view


def render_model_backtest_page(
    *,
    experiment_config: dict,
    model_names: list[str],
    splits: list[str],
    model_labels: dict[str, str],
    split_labels: dict[str, str],
    metric_explanations: dict[str, str],
    zh: Callable[[str], str],
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
    load_metrics: Callable[[str, str], dict],
    load_portfolio: Callable[[str, str], pd.DataFrame],
    load_stability: Callable[[str], dict],
    load_feature_importance: Callable[[str], pd.DataFrame],
    load_diagnostic_table: Callable[[str, str, str], pd.DataFrame],
) -> None:
    st.subheader("模型回测结果")
    model_name = st.selectbox(
        "选择模型",
        model_names,
        index=1,
        key="lab_model",
        format_func=lambda value: model_labels[value],
    )
    split_name = st.selectbox(
        "选择数据集",
        splits,
        index=1,
        key="lab_split",
        format_func=lambda value: split_labels[value],
    )
    st.caption(
        f"当前回测配置：标签周期为 {zh(experiment_config.get('label_col', '-'))}，候选股数量为 {experiment_config.get('top_n', '-')}。"
    )

    metrics = load_metrics(model_name, split_name)
    portfolio = load_portfolio(model_name, split_name)
    stability = load_stability(model_name)
    importance = load_feature_importance(model_name)
    yearly_diagnostics = load_diagnostic_table(model_name, split_name, "yearly")
    regime_diagnostics = load_diagnostic_table(model_name, split_name, "regime")

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
                    [{"指标": zh(key), "说明": value} for key, value in metric_explanations.items()]
                )
                st.dataframe(metric_guide, width="stretch")
        else:
            st.warning("该模型结果还没有生成。")

        if not importance.empty:
            st.markdown("**核心因子贡献**")
            st.dataframe(prettify_dataframe(importance.head(20)), width="stretch")

    with right:
        if portfolio.empty:
            st.warning("组合净值文件还没有生成。")
            return

        chart_frame = portfolio.set_index("trade_date")[["equity_curve"]].rename(columns={"equity_curve": "组合净值"})
        st.line_chart(chart_frame)

        monthly_summary = build_monthly_summary(portfolio)
        if not monthly_summary.empty:
            st.markdown("**最近24个月收益汇总**")
            st.dataframe(prettify_dataframe(monthly_summary.tail(24)), width="stretch")

        if not yearly_diagnostics.empty:
            st.markdown("**按年份拆解**")
            st.dataframe(prettify_dataframe(yearly_diagnostics), width="stretch")

        if not regime_diagnostics.empty:
            st.markdown("**按趋势阶段拆解**")
            st.dataframe(prettify_dataframe(normalize_regime_view(regime_diagnostics)), width="stretch")


def render_model_backtest_payload_page(
    *,
    payload: dict[str, object],
    experiment_config: dict,
    model_names: list[str],
    splits: list[str],
    model_labels: dict[str, str],
    split_labels: dict[str, str],
    metric_explanations: dict[str, str],
    zh: Callable[[str], str],
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    st.subheader("模型回测结果")
    selected_model = str(payload.get("modelName", "lgbm") or "lgbm")
    selected_split = str(payload.get("splitName", "test") or "test")
    model_index = model_names.index(selected_model) if selected_model in model_names else 0
    split_index = splits.index(selected_split) if selected_split in splits else 0
    st.selectbox(
        "选择模型",
        model_names,
        index=model_index,
        key="lab_model",
        format_func=lambda value: model_labels[value],
    )
    st.selectbox(
        "选择数据集",
        splits,
        index=split_index,
        key="lab_split",
        format_func=lambda value: split_labels[value],
    )
    st.caption(
        f"当前回测配置：标签周期为 {zh(experiment_config.get('label_col', '-'))}，候选股数量为 {experiment_config.get('top_n', '-')}。"
    )

    metrics = dict(payload.get("metrics", {}) or {})
    stability = dict(payload.get("stability", {}) or {})
    importance = records_to_frame(payload.get("importance"))  # type: ignore[arg-type]
    portfolio = records_to_frame(payload.get("portfolio"), index_col="trade_date")  # type: ignore[arg-type]
    monthly_summary = records_to_frame(payload.get("monthlySummary"))  # type: ignore[arg-type]
    yearly_diagnostics = records_to_frame(payload.get("yearlyDiagnostics"))  # type: ignore[arg-type]
    regime_diagnostics = records_to_frame(payload.get("regimeDiagnostics"))  # type: ignore[arg-type]

    left, right = st.columns([1.0, 1.5])
    with left:
        if metrics:
            st.metric("截面排序有效性 (RankIC)", f"{float(metrics.get('rank_ic_mean', 0.0)):.4f}")
            st.metric("前N股票正收益占比", f"{float(metrics.get('top_n_hit_rate', 0.0)):.2%}")
            st.metric("前N股票平均未来收益", f"{float(metrics.get('top_n_forward_mean', 0.0)):.2%}")
            st.metric("组合年化收益", f"{float(metrics.get('daily_portfolio_annualized_return', 0.0)):.2%}")
            st.metric("组合夏普", f"{float(metrics.get('daily_portfolio_sharpe', 0.0)):.2f}")
            st.metric("组合最大回撤", f"{float(metrics.get('daily_portfolio_max_drawdown', 0.0)):.2%}")
            if metrics.get("avg_turnover_ratio") is not None:
                st.metric("平均换手比例", f"{float(metrics.get('avg_turnover_ratio', 0.0)):.2%}")
            if metrics.get("holding_period_days") is not None:
                st.metric("持有周期(交易日)", f"{float(metrics.get('holding_period_days', 0.0)):.0f}")
            if metrics.get("risk_filter_active_ratio") is not None:
                st.metric("趋势过滤开启占比", f"{float(metrics.get('risk_filter_active_ratio', 0.0)):.2%}")
            if stability:
                st.markdown("**稳定性结论**")
                st.write(f"评级：{stability.get('grade', '-')}")
                st.write(str(stability.get("conclusion", "")))
            with st.expander("这组回测指标怎么看", expanded=False):
                metric_guide = pd.DataFrame(
                    [{"指标": zh(key), "说明": value} for key, value in metric_explanations.items()]
                )
                st.dataframe(metric_guide, width="stretch")
        else:
            st.warning("该模型结果还没有生成。")

        if not importance.empty:
            st.markdown("**核心因子贡献**")
            st.dataframe(prettify_dataframe(importance.head(20)), width="stretch")

    with right:
        if portfolio.empty:
            st.warning("组合净值文件还没有生成。")
            return

        if "equity_curve" in portfolio.columns:
            st.line_chart(portfolio[["equity_curve"]].rename(columns={"equity_curve": "组合净值"}))

        if not monthly_summary.empty:
            st.markdown("**最近24个月收益汇总**")
            st.dataframe(prettify_dataframe(monthly_summary.tail(24)), width="stretch")

        if not yearly_diagnostics.empty:
            st.markdown("**按年份拆解**")
            st.dataframe(prettify_dataframe(yearly_diagnostics), width="stretch")

        if not regime_diagnostics.empty:
            st.markdown("**按趋势阶段拆解**")
            st.dataframe(prettify_dataframe(regime_diagnostics), width="stretch")
