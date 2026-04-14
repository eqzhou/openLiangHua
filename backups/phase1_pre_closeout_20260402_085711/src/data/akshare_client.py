from __future__ import annotations

import os

import akshare as ak
import pandas as pd


def strip_exchange_suffix(symbol: str) -> str:
    return symbol.split(".")[0]


def to_ts_code(symbol: str) -> str:
    if symbol.startswith(("600", "601", "603", "605", "688", "689", "900")):
        return f"{symbol}.SH"
    if symbol.startswith(("000", "001", "002", "003", "300", "301", "200")):
        return f"{symbol}.SZ"
    if symbol.startswith(("430", "440", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879", "920")):
        return f"{symbol}.BJ"
    if symbol.startswith(("4", "8")):
        return f"{symbol}.BJ"
    if symbol.startswith(("5", "6", "9")):
        return f"{symbol}.SH"
    return f"{symbol}.SZ"


def normalize_index_code(index_code: str) -> str:
    return strip_exchange_suffix(index_code)


def to_ak_symbol(symbol: str) -> str:
    code = strip_exchange_suffix(symbol)
    if code.startswith(("600", "601", "603", "605", "688", "689", "900", "500", "510", "511", "512", "513", "515", "518", "560", "588")):
        return f"sh{code}"
    if code.startswith(("000", "001", "002", "003", "159", "200", "300", "301")):
        return f"sz{code}"
    if code.startswith(("4", "8")):
        return f"bj{code}"
    return f"sz{code}"


class AKShareClient:
    def __init__(self) -> None:
        # This desktop environment exposes a local proxy that breaks some
        # public China market data endpoints; disabling it makes the free
        # routes much more reliable.
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
            os.environ[key] = ""
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"

    def trade_calendar(self) -> pd.DataFrame:
        calendar = ak.tool_trade_date_hist_sina().copy()
        calendar["trade_date"] = pd.to_datetime(calendar["trade_date"])
        return calendar.sort_values("trade_date").reset_index(drop=True)

    def current_index_members(self, index_code: str) -> pd.DataFrame:
        symbol = normalize_index_code(index_code)
        members = ak.index_stock_cons_weight_csindex(symbol=symbol).copy()
        members = members.rename(
            columns={
                "日期": "snapshot_date",
                "指数代码": "index_code",
                "成分券代码": "code",
                "成分券名称": "name",
                "权重": "index_weight",
            }
        )
        members["snapshot_date"] = pd.to_datetime(members["snapshot_date"])
        members["ts_code"] = members["code"].astype(str).str.zfill(6).map(to_ts_code)
        members["index_code"] = index_code
        return members[["snapshot_date", "index_code", "code", "ts_code", "name", "index_weight"]]

    def current_index_entry_dates(self, index_code: str) -> pd.DataFrame:
        symbol = normalize_index_code(index_code)
        members = ak.index_stock_cons(symbol=symbol).copy()
        members = members.rename(
            columns={
                "品种代码": "code",
                "品种名称": "name",
                "纳入日期": "entry_date",
            }
        )
        members["entry_date"] = pd.to_datetime(members["entry_date"], errors="coerce")
        members["ts_code"] = members["code"].astype(str).str.zfill(6).map(to_ts_code)
        members["index_code"] = index_code
        members = (
            members[["index_code", "code", "ts_code", "name", "entry_date"]]
            .sort_values(["ts_code", "entry_date"])
            .drop_duplicates(subset=["ts_code"], keep="first")
            .reset_index(drop=True)
        )
        return members

    def stock_hist(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        ak_symbol = to_ak_symbol(symbol)
        try:
            history = ak.stock_zh_a_hist_tx(
                symbol=ak_symbol,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            ).copy()
            if not history.empty:
                history = history.rename(
                    columns={
                        "date": "trade_date",
                        "open": "open",
                        "close": "close",
                        "high": "high",
                        "low": "low",
                        "amount": "vol",
                    }
                )
                history["amount"] = pd.NA
                history["turnover_rate"] = pd.NA
        except Exception:
            history = ak.stock_zh_a_daily(symbol=ak_symbol, adjust="qfq").copy()
            if not history.empty:
                history = history.rename(
                    columns={
                        "date": "trade_date",
                        "open": "open",
                        "close": "close",
                        "high": "high",
                        "low": "low",
                        "volume": "vol",
                        "amount": "amount",
                        "turnover": "turnover_rate",
                    }
                )
                history = history.loc[
                    (pd.to_datetime(history["trade_date"]) >= pd.Timestamp(start_date))
                    & (pd.to_datetime(history["trade_date"]) <= pd.Timestamp(end_date))
                ].copy()

        if history.empty:
            return history
        history["trade_date"] = pd.to_datetime(history["trade_date"])
        history["ts_code"] = strip_exchange_suffix(symbol)
        history["ts_code"] = history["ts_code"].astype(str).str.zfill(6).map(to_ts_code)
        history["pct_chg"] = history["close"].pct_change() * 100.0
        return history

    def stock_individual_snapshot(self, symbol: str) -> dict[str, object]:
        info = ak.stock_individual_info_em(symbol=strip_exchange_suffix(symbol)).copy()
        if info.empty:
            return {"ts_code": symbol, "industry": None, "list_date": pd.NaT}

        info["item"] = info["item"].astype(str)
        info_map = dict(zip(info["item"], info["value"], strict=False))
        list_date = pd.to_datetime(str(info_map.get("上市时间", "")), format="%Y%m%d", errors="coerce")
        industry = info_map.get("行业")
        industry = None if pd.isna(industry) else str(industry).strip()
        if industry == "":
            industry = None

        return {
            "ts_code": symbol,
            "industry": industry,
            "list_date": list_date,
        }

    def current_board_industry_map(self) -> pd.DataFrame:
        boards = ak.stock_board_industry_name_em().copy()
        rows: list[pd.DataFrame] = []
        for board_name in boards["板块名称"].dropna().astype(str).tolist():
            try:
                members = ak.stock_board_industry_cons_em(symbol=board_name).copy()
            except Exception:  # noqa: BLE001
                continue
            if members.empty:
                continue
            scoped = pd.DataFrame(
                {
                    "ts_code": members["代码"].astype(str).str.zfill(6).map(to_ts_code),
                    "industry": board_name,
                }
            )
            rows.append(scoped)

        if not rows:
            return pd.DataFrame(columns=["ts_code", "industry"])

        return (
            pd.concat(rows, ignore_index=True)
            .drop_duplicates(subset=["ts_code"], keep="first")
            .sort_values("ts_code")
            .reset_index(drop=True)
        )

    def current_st_symbols(self) -> set[str]:
        st_frame = ak.stock_zh_a_st_em().copy()
        if st_frame.empty:
            return set()
        return set(st_frame["代码"].astype(str).str.zfill(6).map(to_ts_code))
