from __future__ import annotations

import pandas as pd


PRICE_COLUMNS = ["open", "high", "low", "close", "pre_close"]


def apply_backward_adjustment(frame: pd.DataFrame) -> pd.DataFrame:
    adjusted = frame.copy()
    if adjusted.empty or "adj_factor" not in adjusted.columns:
        return adjusted

    factor = adjusted["adj_factor"].ffill().bfill()
    if factor.isna().all():
        return adjusted

    latest_factor = factor.dropna().iloc[-1]
    ratio = factor / latest_factor

    for column in PRICE_COLUMNS:
        if column in adjusted.columns:
            adjusted[f"{column}_adj"] = adjusted[column] * ratio

    return adjusted
