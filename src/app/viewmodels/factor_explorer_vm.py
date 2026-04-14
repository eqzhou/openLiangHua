from __future__ import annotations

from collections.abc import Callable

import pandas as pd


def list_numeric_factor_columns(feature_panel: pd.DataFrame) -> list[str]:
    if feature_panel.empty:
        return []
    excluded = {"trade_date", "ts_code", "name", "industry", "index_code"}
    return [
        column
        for column in feature_panel.columns
        if column not in excluded and pd.api.types.is_numeric_dtype(feature_panel[column])
    ]


def build_factor_ranking(cross_section: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    if cross_section.empty or factor_name not in cross_section.columns:
        return pd.DataFrame()
    return (
        cross_section[["ts_code", "name", factor_name]]
        .dropna()
        .sort_values(factor_name, ascending=False)
        .reset_index(drop=True)
    )


def build_missing_rate_table(feature_panel: pd.DataFrame, numeric_columns: list[str]) -> pd.DataFrame:
    if feature_panel.empty or not numeric_columns:
        return pd.DataFrame()
    return (
        feature_panel[numeric_columns]
        .isna()
        .mean()
        .sort_values(ascending=False)
        .rename("missing_rate")
        .reset_index()
        .rename(columns={"index": "feature"})
    )


def build_latest_factor_snapshot(
    cross_section: pd.DataFrame,
    *,
    symbol: str,
    zh: Callable[[str], str],
) -> pd.DataFrame:
    latest_row = cross_section.loc[cross_section["ts_code"] == symbol].head(1)
    if latest_row.empty:
        return pd.DataFrame()
    row = latest_row.iloc[0].to_dict()
    snapshot = pd.DataFrame(
        [{"字段": zh(key), "原始列名": key, "数值": value} for key, value in row.items()]
    )
    snapshot["数值"] = snapshot["数值"].map(lambda value: "" if pd.isna(value) else str(value))
    return snapshot
