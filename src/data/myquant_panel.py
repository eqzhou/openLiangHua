from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

PRICE_COLUMNS = ["open", "high", "low", "close", "vol", "amount"]


def trim_open_dates_to_bars(open_dates: pd.Series, bars: pd.DataFrame) -> tuple[pd.Series, pd.Timestamp | None]:
    if open_dates.empty or bars.empty or "trade_date" not in bars.columns:
        return open_dates, None

    latest_bar_date = pd.to_datetime(bars["trade_date"], errors="coerce").dropna().max()
    if pd.isna(latest_bar_date):
        return open_dates, None

    trimmed = pd.to_datetime(open_dates, errors="coerce")
    trimmed = trimmed.loc[trimmed.notna() & (trimmed <= latest_bar_date)].reset_index(drop=True)
    return trimmed, pd.Timestamp(latest_bar_date)


def drop_trailing_empty_price_dates(
    panel: pd.DataFrame,
    price_columns: Iterable[str] | None = None,
) -> tuple[pd.DataFrame, list[pd.Timestamp]]:
    if panel.empty or "trade_date" not in panel.columns:
        return panel, []

    scoped_columns = [column for column in (price_columns or PRICE_COLUMNS) if column in panel.columns]
    if not scoped_columns:
        return panel, []

    working = panel.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    working = working.loc[working["trade_date"].notna()].copy()
    if working.empty:
        return working, []

    price_presence = working[scoped_columns].notna().any(axis=1)
    by_date = (
        working.assign(_has_price=price_presence)
        .groupby("trade_date", sort=True)["_has_price"]
        .any()
        .sort_index()
    )
    if by_date.empty:
        return working, []

    trailing_dates: list[pd.Timestamp] = []
    for trade_date, has_price in by_date.iloc[::-1].items():
        if has_price:
            break
        trailing_dates.append(pd.Timestamp(trade_date))

    if not trailing_dates:
        return working, []

    trimmed = working.loc[~working["trade_date"].isin(trailing_dates)].copy()
    return trimmed.reset_index(drop=True), sorted(trailing_dates)
