from __future__ import annotations

import os

import pandas as pd
from dotenv import load_dotenv

from src.utils.io import project_root


class TushareClient:
    def __init__(self, token: str | None = None) -> None:
        import tushare as ts

        load_dotenv(project_root() / ".env")
        resolved_token = token or os.getenv("TUSHARE_TOKEN")
        if not resolved_token:
            raise RuntimeError("TUSHARE_TOKEN is missing. Add it to .env before downloading data.")

        ts.set_token(resolved_token)
        self.pro = ts.pro_api(resolved_token)

    def stock_basic(self):
        frames = []
        for status in ("L", "D", "P"):
            frames.append(
                self.pro.stock_basic(
                    exchange="",
                    list_status=status,
                    fields="ts_code,symbol,name,area,industry,list_date,list_status",
                )
            )
        return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts_code"], keep="first")

    def trade_cal(self, start_date: str, end_date: str, exchange: str = "SSE"):
        return self.pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date)

    def daily(
        self,
        ts_code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        trade_date: str | None = None,
    ):
        query: dict[str, str] = {}
        if ts_code:
            query["ts_code"] = ts_code
        if start_date:
            query["start_date"] = start_date
        if end_date:
            query["end_date"] = end_date
        if trade_date:
            query["trade_date"] = trade_date
        return self.pro.daily(**query)

    def daily_basic(
        self,
        ts_code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        trade_date: str | None = None,
    ):
        query: dict[str, str] = {
            "fields": "ts_code,trade_date,turnover_rate,volume_ratio,pe_ttm,pb,ps_ttm,total_mv,circ_mv",
        }
        if ts_code:
            query["ts_code"] = ts_code
        if start_date:
            query["start_date"] = start_date
        if end_date:
            query["end_date"] = end_date
        if trade_date:
            query["trade_date"] = trade_date
        return self.pro.daily_basic(**query)

    def adj_factor(
        self,
        ts_code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        trade_date: str | None = None,
    ):
        query: dict[str, str] = {}
        if ts_code:
            query["ts_code"] = ts_code
        if start_date:
            query["start_date"] = start_date
        if end_date:
            query["end_date"] = end_date
        if trade_date:
            query["trade_date"] = trade_date
        return self.pro.adj_factor(**query)

    def index_weight(self, index_code: str, start_date: str, end_date: str):
        return self.pro.index_weight(index_code=index_code, start_date=start_date, end_date=end_date)

    def stk_limit(
        self,
        ts_code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        trade_date: str | None = None,
    ):
        query: dict[str, str] = {}
        if ts_code:
            query["ts_code"] = ts_code
        if start_date:
            query["start_date"] = start_date
        if end_date:
            query["end_date"] = end_date
        if trade_date:
            query["trade_date"] = trade_date
        return self.pro.stk_limit(**query)

    def stock_st(self, ts_code: str, start_date: str, end_date: str):
        return self.pro.stock_st(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def namechange(self, ts_code: str, start_date: str, end_date: str):
        return self.pro.namechange(ts_code=ts_code, start_date=start_date, end_date=end_date)
