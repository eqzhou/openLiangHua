from __future__ import annotations

import os
import uuid
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from src.db.settings import get_database_settings
from src.utils.logger import configure_logging

DEFAULT_MARKET_DATABASE = "cc_project"
LEGACY_MARKET_DATABASES = ("a_stock_quant_db",)
logger = configure_logging()


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
    requested_symbols = sorted({str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()})
    normalized_symbols = sorted({_normalize_symbol_code(symbol) for symbol in requested_symbols})
    if not requested_symbols and not normalized_symbols:
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
        inner join ref.instruments i on i.symbol = b.symbol
        where i.asset_type = 'equity'
          and b.adjust_type = 'qfq'
          and (b.symbol = any(%s) or split_part(b.symbol, '.', 1) = any(%s))
        order by b.symbol, b.trade_date
        """,
        (requested_symbols, normalized_symbols),
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


def load_equity_symbols_from_market_database() -> list[str]:
    rows = _load_query_rows(
        """
        select symbol
        from ref.instruments
        where asset_type = 'equity'
        order by symbol
        """
    )
    return [str(row["symbol"]) for row in rows if str(row.get("symbol", "")).strip()]


def load_daily_bar_batch_from_market_database(
    symbols: list[str],
    *,
    benchmark_code: str | None = None,
) -> pd.DataFrame:
    requested_symbols = sorted({str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()})
    normalized_symbols = sorted({_normalize_symbol_code(symbol) for symbol in requested_symbols})
    if not requested_symbols and not normalized_symbols:
        return pd.DataFrame()

    rows = _load_query_rows(
        """
        select
            b.trade_date,
            b.symbol as ts_code,
            i.name,
            i.industry,
            i.list_date,
            i.is_st,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume as vol,
            b.amount
        from market.bars_1d b
        inner join ref.instruments i on i.symbol = b.symbol
        where i.asset_type = 'equity'
          and b.adjust_type = 'qfq'
          and (b.symbol = any(%s) or split_part(b.symbol, '.', 1) = any(%s))
        order by b.symbol, b.trade_date
        """,
        (requested_symbols, normalized_symbols),
    )
    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame["list_date"] = pd.to_datetime(frame["list_date"], errors="coerce")
    for column in ("open", "high", "low", "close", "vol", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["index_code"] = benchmark_code or pd.NA
    frame["is_index_member"] = False
    frame["is_current_name_st"] = frame["is_st"].fillna(False).astype(bool)
    frame["is_st"] = frame["is_st"].fillna(False).astype(bool)
    frame["is_suspend"] = frame["close"].isna()
    frame["is_limit_up_close"] = False
    frame["is_limit_down_close"] = False
    frame["is_buy_locked"] = False
    frame["is_sell_locked"] = False
    frame["up_limit"] = pd.NA
    frame["down_limit"] = pd.NA
    frame["adj_factor"] = 1.0
    frame["open_adj"] = frame["open"]
    frame["high_adj"] = frame["high"]
    frame["low_adj"] = frame["low"]
    frame["close_adj"] = frame["close"]
    frame = frame.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    frame["pre_close"] = frame.groupby("ts_code")["close"].shift(1)
    frame["pre_close_adj"] = frame.groupby("ts_code")["close_adj"].shift(1)
    frame["pct_chg"] = frame.groupby("ts_code")["close"].pct_change() * 100.0
    return frame


def load_full_daily_bar_from_market_database(*, benchmark_code: str | None = None, batch_size: int = 200_000) -> pd.DataFrame:
    settings = get_database_settings()
    candidate_names = _candidate_database_names()
    sql = """
        select
            b.trade_date,
            b.symbol as ts_code,
            i.name,
            i.industry,
            i.list_date,
            i.is_st,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume as vol,
            b.amount
        from market.bars_1d b
        inner join ref.instruments i on i.symbol = b.symbol
        where i.asset_type = 'equity'
          and b.adjust_type = 'qfq'
        order by b.symbol, b.trade_date
    """

    for db_name in candidate_names:
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
                cursor_name = f"bars_1d_{uuid.uuid4().hex}"
                with conn.cursor(name=cursor_name) as cur:
                    cur.execute(sql)
                    frames: list[pd.DataFrame] = []
                    batch_count = 0
                    row_count = 0
                    while True:
                        rows = cur.fetchmany(batch_size)
                        if not rows:
                            break
                        batch_count += 1
                        row_count += len(rows)
                        frames.append(pd.DataFrame([dict(row) for row in rows]))
                        logger.info(
                            "Loading full market daily bar from {} batch {} rows {} total_rows {}",
                            db_name,
                            batch_count,
                            len(rows),
                            row_count,
                        )

                if not frames:
                    continue

                frame = pd.concat(frames, ignore_index=True)
                logger.info("Loaded full market daily bar from {} total_rows {}", db_name, len(frame))
                frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
                frame["list_date"] = pd.to_datetime(frame["list_date"], errors="coerce")
                for column in ("open", "high", "low", "close", "vol", "amount"):
                    frame[column] = pd.to_numeric(frame[column], errors="coerce")
                frame["index_code"] = benchmark_code or pd.NA
                frame["is_index_member"] = False
                frame["is_current_name_st"] = frame["is_st"].fillna(False).astype(bool)
                frame["is_st"] = frame["is_st"].fillna(False).astype(bool)
                frame["is_suspend"] = frame["close"].isna()
                frame["is_limit_up_close"] = False
                frame["is_limit_down_close"] = False
                frame["is_buy_locked"] = False
                frame["is_sell_locked"] = False
                frame["up_limit"] = pd.NA
                frame["down_limit"] = pd.NA
                frame["adj_factor"] = 1.0
                frame["open_adj"] = frame["open"]
                frame["high_adj"] = frame["high"]
                frame["low_adj"] = frame["low"]
                frame["close_adj"] = frame["close"]
                frame = frame.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
                frame["pre_close"] = frame.groupby("ts_code")["close"].shift(1)
                frame["pre_close_adj"] = frame.groupby("ts_code")["close_adj"].shift(1)
                frame["pct_chg"] = frame.groupby("ts_code")["close"].pct_change() * 100.0
                return frame
        except psycopg.Error:
            continue

    return pd.DataFrame()


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


def load_stock_bar_summary_from_market_database() -> dict[str, Any]:
    rows = _load_query_rows(
        """
        select
            min(b.trade_date) as min_trade_date,
            max(b.trade_date) as max_trade_date,
            count(*) as row_count,
            count(distinct b.symbol) as symbol_count
        from market.bars_1d b
        inner join ref.instruments i on i.symbol = b.symbol
        where i.asset_type = 'equity'
          and b.adjust_type = 'qfq'
        """
    )
    if not rows:
        return {
            "rowCount": 0,
            "symbolCount": 0,
            "latestTradeDate": None,
            "minTradeDate": None,
        }

    row = rows[0]
    min_trade_date = row.get("min_trade_date")
    max_trade_date = row.get("max_trade_date")
    return {
        "rowCount": int(row.get("row_count", 0) or 0),
        "symbolCount": int(row.get("symbol_count", 0) or 0),
        "latestTradeDate": str(pd.Timestamp(max_trade_date).date()) if max_trade_date is not None else None,
        "minTradeDate": str(pd.Timestamp(min_trade_date).date()) if min_trade_date is not None else None,
    }
