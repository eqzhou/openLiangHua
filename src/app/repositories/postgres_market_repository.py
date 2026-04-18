from __future__ import annotations

import os
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from src.db.settings import get_database_settings

DEFAULT_MARKET_DATABASE = "cc_project"
LEGACY_MARKET_DATABASES = ("a_stock_quant_db",)


def _candidate_database_names() -> list[str]:
    settings = get_database_settings()
    configured_market_db = str(os.getenv("APP_MARKET_DB_NAME", "")).strip()

    candidates: list[str] = []
    for name in (configured_market_db, DEFAULT_MARKET_DATABASE, *LEGACY_MARKET_DATABASES, settings.name):
        if name and name not in candidates:
            candidates.append(name)
    return candidates


def _load_query_rows(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    settings = get_database_settings()
    candidate_names = _candidate_database_names()

    for index, db_name in enumerate(candidate_names):
        try:
            with psycopg.connect(
                host=settings.host,
                port=settings.port,
                dbname=db_name,
                user=settings.user,
                password=settings.password,
                connect_timeout=settings.connect_timeout,
                row_factory=dict_row,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    rows = [dict(row) for row in cur.fetchall()]
                    if rows or index == len(candidate_names) - 1:
                        return rows
        except psycopg.Error:
            continue

    return []


def _normalize_symbol_code(symbol: str) -> str:
    return str(symbol or "").strip().upper().split(".")[0]


def _infer_exchange(code: str, market: str | None) -> str:
    market_text = str(market or "").strip()
    if "北" in market_text or code.startswith(("4", "8")):
        return "BJ"
    if "沪" in market_text or "科创" in market_text or code.startswith(("5", "6", "9")):
        return "SH"
    if "深" in market_text or "创业" in market_text or code.startswith(("0", "1", "2", "3")):
        return "SZ"
    return ""


def _to_ts_code(code: str, market: str | None) -> str:
    normalized = _normalize_symbol_code(code)
    if "." in str(code):
        return str(code).upper()
    exchange = _infer_exchange(normalized, market)
    return f"{normalized}.{exchange}" if exchange else normalized


def load_daily_bar_from_market_database(symbols: list[str]) -> pd.DataFrame:
    normalized_symbols = sorted({_normalize_symbol_code(symbol) for symbol in symbols if str(symbol or "").strip()})
    if not normalized_symbols:
        return pd.DataFrame()

    rows = _load_query_rows(
        """
        select
            b.trade_date,
            b.symbol,
            i.name,
            i.industry,
            b.open,
            b.close,
            b.high,
            b.low,
            b.amount,
            null::text as market
        from market.bars_1d b
        left join ref.instruments i on i.symbol = b.symbol
        where b.symbol = any(%s)
        order by b.symbol, b.trade_date
        """,
        (normalized_symbols,),
    )
    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame["symbol"] = frame["symbol"].astype(str)
    frame["ts_code"] = [
        _to_ts_code(symbol, market)
        for symbol, market in zip(frame["symbol"].tolist(), frame.get("market", pd.Series(dtype=object)).tolist(), strict=False)
    ]
    if "name" in frame.columns:
        frame["name"] = frame["name"].fillna(frame["symbol"])
    if "industry" in frame.columns:
        frame["industry"] = frame["industry"].fillna("")
    frame = frame.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    frame["pct_chg"] = frame.groupby("ts_code")["close"].transform(lambda series: series.pct_change())

    columns = ["trade_date", "ts_code", "name", "close", "open", "high", "low", "pct_chg", "amount", "industry"]
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[columns].sort_values(["ts_code", "trade_date"]).reset_index(drop=True)


def load_trade_dates_from_market_database() -> pd.Series:
    rows = _load_query_rows(
        """
        select trading_day as trade_date
        from ref.trading_calendar
        where is_open = true
        order by trading_day
        """
    )
    if not rows:
        return pd.Series(dtype="datetime64[ns]")

    frame = pd.DataFrame(rows)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.loc[frame["trade_date"].notna()].drop_duplicates().sort_values("trade_date")
    return frame["trade_date"].reset_index(drop=True)
