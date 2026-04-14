from __future__ import annotations

from collections.abc import Callable

import pandas as pd


def build_model_comparison_frame(metrics_table: pd.DataFrame, shown_columns: list[str]) -> pd.DataFrame:
    if metrics_table.empty:
        return pd.DataFrame()
    columns = [column for column in shown_columns if column in metrics_table.columns]
    if not columns:
        return pd.DataFrame()
    return metrics_table[columns].copy()


def build_equity_curve_frame(
    *,
    model_names: list[str],
    split_name: str,
    model_labels: dict[str, str],
    load_portfolio: Callable[[str, str], pd.DataFrame],
) -> pd.DataFrame:
    curves: list[pd.DataFrame] = []
    for model_name in model_names:
        portfolio = load_portfolio(model_name, split_name)
        if portfolio.empty or "trade_date" not in portfolio.columns or "equity_curve" not in portfolio.columns:
            continue
        curves.append(
            portfolio[["trade_date", "equity_curve"]]
            .rename(columns={"equity_curve": model_labels[model_name]})
            .set_index("trade_date")
        )
    if not curves:
        return pd.DataFrame()
    return pd.concat(curves, axis=1)
