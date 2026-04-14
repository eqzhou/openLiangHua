from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from src.utils.io import project_root

EXCHANGE_TO_GM = {
    "SH": "SHSE",
    "SZ": "SZSE",
    "BJ": "BJSE",
}

GM_TO_EXCHANGE = {value: key for key, value in EXCHANGE_TO_GM.items()}


def ts_code_to_gm_symbol(ts_code: str) -> str:
    code, exchange = ts_code.split(".")
    market = EXCHANGE_TO_GM.get(exchange.upper())
    if not market:
        raise ValueError(f"Unsupported ts_code exchange: {ts_code}")
    return f"{market}.{code}"


def gm_symbol_to_ts_code(symbol: str) -> str:
    market, code = symbol.split(".")
    exchange = GM_TO_EXCHANGE.get(market.upper())
    if not exchange:
        raise ValueError(f"Unsupported MyQuant symbol: {symbol}")
    return f"{code}.{exchange}"


def ensure_gm_symbol(symbol: str) -> str:
    if not symbol:
        raise ValueError("Symbol is empty.")

    left, _, right = symbol.partition(".")
    if left.upper() in GM_TO_EXCHANGE:
        return symbol
    if right.upper() in EXCHANGE_TO_GM:
        return ts_code_to_gm_symbol(symbol)
    raise ValueError(f"Unsupported symbol format: {symbol}")


def _normalize_datetime(value: Any) -> pd.Timestamp:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return pd.NaT
    if getattr(timestamp, "tzinfo", None) is not None:
        timestamp = timestamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return pd.Timestamp(timestamp)


def _normalize_date_text(value: str | pd.Timestamp) -> str:
    timestamp = pd.Timestamp(value)
    return timestamp.strftime("%Y-%m-%d")


def _as_symbol_argument(symbols: str | Iterable[str]) -> str:
    if isinstance(symbols, str):
        return ensure_gm_symbol(symbols)
    return ",".join(ensure_gm_symbol(symbol) for symbol in symbols)


class MyQuantClient:
    def __init__(
        self,
        token: str | None = None,
        service_addr: str | None = None,
    ) -> None:
        load_dotenv(project_root() / ".env")
        resolved_token = token or os.getenv("MYQUANT_TOKEN")
        if not resolved_token:
            raise RuntimeError("MYQUANT_TOKEN is missing. Add it to .env before using MyQuant data.")

        try:
            from gm import api as gm_api
        except ImportError as exc:  # pragma: no cover - depends on optional SDK
            raise RuntimeError(
                "The optional MyQuant SDK is not installed. Install `gm` in a supported Python 3.11/3.12 environment first."
            ) from exc

        self.gm = gm_api
        self.gm.set_token(resolved_token)

        resolved_addr = service_addr or os.getenv("MYQUANT_SERV_ADDR", "").strip()
        if resolved_addr:
            for attr in ("set_serv_addr", "set_addr", "set_endpoint"):
                fn = getattr(self.gm, attr, None)
                if callable(fn):
                    fn(resolved_addr)
                    break

        self.adjust_prev = getattr(self.gm, "ADJUST_PREV", None)
        self.adjust_none = getattr(self.gm, "ADJUST_NONE", 0)
        self.adjust_post = getattr(self.gm, "ADJUST_POST", None)

    def trade_calendar(self, start_date: str, end_date: str, exchange: str = "SHSE") -> pd.DataFrame:
        values = None
        calendar_by_year = getattr(self.gm, "get_trading_dates_by_year", None)
        if callable(calendar_by_year):
            try:
                values = calendar_by_year(
                    exchange=exchange,
                    start_year=pd.Timestamp(start_date).year,
                    end_year=pd.Timestamp(end_date).year,
                )
            except Exception:  # noqa: BLE001
                values = None

        if values is None:
            values = self.gm.get_trading_dates(
                exchange=exchange,
                start_date=_normalize_date_text(start_date),
                end_date=_normalize_date_text(end_date),
            )

        if isinstance(values, pd.DataFrame):
            if "trade_date" in values.columns:
                calendar = values[["trade_date"]].copy()
            else:
                first_col = values.columns[0]
                calendar = values[[first_col]].rename(columns={first_col: "trade_date"})
        else:
            calendar = pd.DataFrame({"trade_date": [_normalize_datetime(value) for value in values or []]})

        calendar["trade_date"] = calendar["trade_date"].map(_normalize_datetime)
        calendar = calendar.loc[
            (calendar["trade_date"] >= pd.Timestamp(start_date))
            & (calendar["trade_date"] <= pd.Timestamp(end_date))
        ].copy()
        calendar = calendar.dropna().drop_duplicates().sort_values("trade_date").reset_index(drop=True)
        return calendar

    def instrument_infos(
        self,
        symbols: str | Iterable[str],
        fields: str | None = None,
    ) -> pd.DataFrame:
        payload = self.gm.get_instrumentinfos(
            symbols=_as_symbol_argument(symbols),
            fields=fields or "symbol,sec_name,listed_date,delisted_date,exchange,sec_id,sec_abbr,sec_type",
            df=True,
        )
        frame = pd.DataFrame(payload).copy()
        if frame.empty:
            return frame

        for column in ("listed_date", "delisted_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(_normalize_datetime)
        frame["ts_code"] = frame["symbol"].map(gm_symbol_to_ts_code)
        if "sec_name" in frame.columns and "name" not in frame.columns:
            frame = frame.rename(columns={"sec_name": "name"})
        return frame.sort_values("ts_code").reset_index(drop=True)

    def history_constituents(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        index_symbol = ts_code_to_gm_symbol(index_code)
        rows: list[dict[str, Any]] = []
        payload = self.gm.get_history_constituents(
            index=index_symbol,
            start_date=_normalize_date_text(start_date),
            end_date=_normalize_date_text(end_date),
        )
        for item in payload or []:
            trade_date = _normalize_datetime(item.get("trade_date"))
            for symbol, weight in (item.get("constituents") or {}).items():
                rows.append(
                    {
                        "index_code": index_code,
                        "trade_date": trade_date,
                        "con_code": gm_symbol_to_ts_code(symbol),
                        "weight": float(weight),
                    }
                )

        if not rows:
            calendar = self.trade_calendar(start_date=start_date, end_date=end_date)
            snapshot_dates = pd.Series(pd.to_datetime(calendar["trade_date"]).dropna().unique()).sort_values()
            if snapshot_dates.empty:
                return pd.DataFrame(columns=["index_code", "trade_date", "con_code", "weight"])

            month_end_dates = snapshot_dates.groupby(snapshot_dates.dt.to_period("M")).max().tolist()
            first_date = pd.Timestamp(snapshot_dates.iloc[0])
            last_date = pd.Timestamp(snapshot_dates.iloc[-1])
            candidates = [first_date] + [pd.Timestamp(value) for value in month_end_dates] + [last_date]
            selected_dates = (
                pd.Series(candidates)
                .dropna()
                .drop_duplicates()
                .sort_values()
                .reset_index(drop=True)
            )

            new_api = getattr(self.gm, "stk_get_index_constituents", None)
            if callable(new_api):
                for snapshot_date in selected_dates.tolist():
                    snapshot = pd.DataFrame(
                        new_api(
                            index=index_symbol,
                            trade_date=_normalize_date_text(snapshot_date),
                        )
                    ).copy()
                    if snapshot.empty:
                        continue
                    if "trade_date" not in snapshot.columns:
                        snapshot["trade_date"] = snapshot_date
                    snapshot["trade_date"] = snapshot["trade_date"].map(_normalize_datetime)
                    snapshot["con_code"] = snapshot["symbol"].map(gm_symbol_to_ts_code)
                    snapshot["index_code"] = index_code
                    if "weight" not in snapshot.columns:
                        snapshot["weight"] = 0.0
                    rows.extend(
                        snapshot[["index_code", "trade_date", "con_code", "weight"]]
                        .to_dict(orient="records")
                    )

        if not rows:
            return pd.DataFrame(columns=["index_code", "trade_date", "con_code", "weight"])
        return pd.DataFrame(rows).drop_duplicates().sort_values(["trade_date", "con_code"]).reset_index(drop=True)

    def history_instruments(
        self,
        symbols: str | Iterable[str],
        start_date: str,
        end_date: str,
        fields: str | None = None,
    ) -> pd.DataFrame:
        default_fields = (
            "symbol,trade_date,sec_name,listed_date,delisted_date,sec_level,"
            "is_suspended,is_st,pre_close,upper_limit,lower_limit,adj_factor"
        )
        payload = self.gm.get_history_instruments(
            symbols=_as_symbol_argument(symbols),
            start_date=_normalize_date_text(start_date),
            end_date=_normalize_date_text(end_date),
            fields=fields or default_fields,
            df=True,
        )
        frame = pd.DataFrame(payload).copy()
        if frame.empty:
            return frame

        if "trade_date" in frame.columns:
            frame["trade_date"] = frame["trade_date"].map(_normalize_datetime)
        if "listed_date" in frame.columns:
            frame["listed_date"] = frame["listed_date"].map(_normalize_datetime)
        if "delisted_date" in frame.columns:
            frame["delisted_date"] = frame["delisted_date"].map(_normalize_datetime)
        frame["ts_code"] = frame["symbol"].map(gm_symbol_to_ts_code)
        if "sec_name" in frame.columns and "name" not in frame.columns:
            frame = frame.rename(columns={"sec_name": "name"})
        return frame.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

    def history_bars(
        self,
        symbols: str | Iterable[str],
        start_date: str,
        end_date: str,
        frequency: str = "1d",
        adjust: str = "none",
        adjust_end_time: str | None = None,
    ) -> pd.DataFrame:
        adjust_key = (adjust or "none").lower()
        adjust_value = self.adjust_none
        if adjust_key == "prev":
            adjust_value = self.adjust_prev
        elif adjust_key == "post":
            adjust_value = self.adjust_post

        payload = self.gm.history(
            symbol=_as_symbol_argument(symbols),
            frequency=frequency,
            start_time=_normalize_date_text(start_date),
            end_time=_normalize_date_text(end_date),
            fields="symbol,open,close,high,low,volume,amount,eob",
            adjust=adjust_value,
            adjust_end_time=_normalize_date_text(adjust_end_time) if adjust_end_time else "",
            df=True,
        )
        frame = pd.DataFrame(payload).copy()
        if frame.empty:
            return frame

        frame["trade_date"] = frame["eob"].map(_normalize_datetime).dt.normalize()
        frame["ts_code"] = frame["symbol"].map(gm_symbol_to_ts_code)
        frame = frame.rename(
            columns={
                "volume": "vol",
            }
        )
        keep_cols = ["trade_date", "ts_code", "open", "close", "high", "low", "vol", "amount"]
        return frame[keep_cols].sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
