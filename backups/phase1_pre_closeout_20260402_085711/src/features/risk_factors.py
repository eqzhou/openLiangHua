from __future__ import annotations

import pandas as pd


def _rolling_drawdown(series: pd.Series, window: int) -> pd.Series:
    rolling_peak = series.rolling(window, min_periods=max(10, window // 2)).max()
    return series / rolling_peak - 1.0


def add_risk_factors(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy().sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    grouped = enriched.groupby("ts_code", group_keys=False)

    enriched["downside_vol_20"] = grouped["ret_1d"].transform(
        lambda series: series.where(series < 0).rolling(20, min_periods=10).std()
    )
    enriched["ret_skew_20"] = grouped["ret_1d"].transform(
        lambda series: series.rolling(20, min_periods=10).skew()
    )
    enriched["drawdown_60"] = grouped["close_adj"].transform(lambda series: _rolling_drawdown(series, 60))
    return enriched
