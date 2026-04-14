from __future__ import annotations

import pandas as pd


def build_trade_calendar(client, start_date: str, end_date: str, exchange: str = "SSE") -> pd.DataFrame:
    calendar = client.trade_cal(start_date=start_date, end_date=end_date, exchange=exchange).copy()
    calendar["cal_date"] = pd.to_datetime(calendar["cal_date"], format="%Y%m%d")
    calendar["pretrade_date"] = pd.to_datetime(calendar["pretrade_date"], format="%Y%m%d", errors="coerce")
    calendar["is_open"] = calendar["is_open"].astype(int)
    return calendar.sort_values("cal_date").reset_index(drop=True)


def open_trade_dates(calendar: pd.DataFrame) -> pd.Series:
    return calendar.loc[calendar["is_open"] == 1, "cal_date"].reset_index(drop=True)
