from __future__ import annotations

import pandas as pd


def build_monthly_summary(portfolio: pd.DataFrame) -> pd.DataFrame:
    if portfolio.empty or "trade_date" not in portfolio.columns or "net_return" not in portfolio.columns:
        return pd.DataFrame()

    working = portfolio.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    working = working.loc[working["trade_date"].notna()].copy()
    if working.empty:
        return pd.DataFrame()

    working["month"] = working["trade_date"].dt.to_period("M").astype(str)
    return working.groupby("month", as_index=False)["net_return"].sum()


def normalize_regime_view(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "regime" not in frame.columns:
        return frame.copy()

    normalized = frame.copy()
    normalized["regime"] = normalized["regime"].replace(
        {
            "trend_on": "趋势开启",
            "trend_off": "趋势过滤",
        }
    )
    return normalized
