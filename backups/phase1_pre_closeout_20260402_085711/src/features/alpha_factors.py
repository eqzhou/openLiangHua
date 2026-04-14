from __future__ import annotations

import pandas as pd


def add_price_factors(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy().sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    grouped = enriched.groupby("ts_code", group_keys=False)

    enriched["ret_1d"] = grouped["close_adj"].pct_change()

    for window in (5, 20, 60, 120):
        enriched[f"mom_{window}"] = grouped["close_adj"].pct_change(window)

    for window in (20, 60):
        min_periods = max(10, window // 2)
        enriched[f"vol_{window}"] = enriched.groupby("ts_code")["ret_1d"].transform(
            lambda series: series.rolling(window, min_periods=min_periods).std()
        )
        enriched[f"close_to_ma_{window}"] = grouped["close_adj"].transform(
            lambda series: series / series.rolling(window, min_periods=min_periods).mean() - 1.0
        )

    if "turnover_rate" in enriched.columns:
        enriched["turnover_20"] = grouped["turnover_rate"].transform(
            lambda series: series.rolling(20, min_periods=10).mean()
        )

    if "amount" in enriched.columns:
        enriched["amount_20"] = grouped["amount"].transform(
            lambda series: series.rolling(20, min_periods=10).mean()
        )

    return enriched
