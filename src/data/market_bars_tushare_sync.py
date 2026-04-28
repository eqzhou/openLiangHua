from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime
import os
from pprint import pformat
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from src.app.repositories.postgres_market_repository import _candidate_database_names
from src.app.repositories.postgres_watchlist_store import PostgresWatchlistStore
from src.data.tushare_client import TushareClient
from src.db.settings import get_database_settings
from src.utils.logger import configure_logging
from src.web_api.settings import get_api_settings

logger = configure_logging()


@dataclass(frozen=True)
class MarketBarsSyncSummary:
    user_id: str | None
    start_date: str
    end_date: str
    previous_latest_trade_date: str | None
    latest_trade_date: str | None
    requested_symbols: int
    fetched_rows: int
    upserted_rows: int


def _today_ts_date() -> str:
    return datetime.now().strftime("%Y%m%d")


def normalize_ts_date(value: str | None) -> str:
    if not value:
        return _today_ts_date()
    normalized = str(value).strip()
    if not normalized:
        return _today_ts_date()
    if len(normalized) == 8 and normalized.isdigit():
        return normalized
    return pd.Timestamp(normalized).strftime("%Y%m%d")


def _next_ts_date(value: pd.Timestamp | None) -> str:
    if value is None or pd.isna(value):
        return "19900101"
    return (pd.Timestamp(value) + pd.Timedelta(days=1)).strftime("%Y%m%d")


def _dedupe_symbols(symbols: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized = str(symbol or "").strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    return deduped


def load_watchlist_symbols(user_id: str) -> list[str]:
    watchlist = PostgresWatchlistStore(get_api_settings()).load_watchlist(user_id)
    symbols = [
        str(item.get("ts_code", "") or "")
        for item in [*(watchlist.get("holdings", []) or []), *(watchlist.get("focus_pool", []) or [])]
    ]
    return _dedupe_symbols(symbols)


def _connect_market_database() -> psycopg.Connection:
    settings = get_database_settings()
    for db_name in _candidate_database_names():
        try:
            conn = psycopg.connect(
                host=settings.host,
                port=settings.port,
                dbname=db_name,
                user=settings.user,
                password=settings.password,
                connect_timeout=settings.connect_timeout,
                row_factory=dict_row,
            )
            with conn.cursor() as cur:
                cur.execute("select to_regclass('market.bars_1d') as table_name")
                row = cur.fetchone()
                if row and row.get("table_name"):
                    return conn
            conn.close()
        except psycopg.Error:
            continue
    raise RuntimeError("Could not connect to a database containing market.bars_1d.")


def load_latest_market_trade_date(symbols: list[str] | None = None, *, adjust_type: str = "qfq") -> pd.Timestamp | None:
    normalized_symbols = _dedupe_symbols(symbols or [])
    with _connect_market_database() as conn:
        with conn.cursor() as cur:
            if normalized_symbols:
                cur.execute(
                    """
                    select max(trade_date) as latest_trade_date
                    from market.bars_1d
                    where adjust_type = %s
                      and symbol = any(%s)
                    """,
                    (adjust_type, normalized_symbols),
                )
            else:
                cur.execute(
                    """
                    select max(trade_date) as latest_trade_date
                    from market.bars_1d
                    where adjust_type = %s
                    """,
                    (adjust_type,),
                )
            row = cur.fetchone()
    latest = row.get("latest_trade_date") if row else None
    return pd.Timestamp(latest) if latest is not None else None


def load_latest_market_trade_dates(symbols: list[str], *, adjust_type: str = "qfq") -> dict[str, pd.Timestamp | None]:
    normalized_symbols = _dedupe_symbols(symbols)
    latest_by_symbol: dict[str, pd.Timestamp | None] = {symbol: None for symbol in normalized_symbols}
    if not normalized_symbols:
        return latest_by_symbol

    with _connect_market_database() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select symbol, max(trade_date) as latest_trade_date
                from market.bars_1d
                where adjust_type = %s
                  and symbol = any(%s)
                group by symbol
                """,
                (adjust_type, normalized_symbols),
            )
            rows = cur.fetchall()

    for row in rows:
        symbol = str(row.get("symbol", "") or "").strip().upper()
        latest = row.get("latest_trade_date")
        if symbol in latest_by_symbol and latest is not None:
            latest_by_symbol[symbol] = pd.Timestamp(latest)
    return latest_by_symbol


def fetch_qfq_bars(
    client: TushareClient,
    *,
    symbols: list[str],
    start_date: str,
    end_date: str,
    start_dates_by_symbol: dict[str, str] | None = None,
) -> pd.DataFrame:
    import tushare as ts

    frames: list[pd.DataFrame] = []
    for symbol in symbols:
        resolved_start_date = (start_dates_by_symbol or {}).get(symbol, start_date)
        if pd.Timestamp(resolved_start_date) > pd.Timestamp(end_date):
            continue
        frame = ts.pro_bar(
            ts_code=symbol,
            api=client.pro,
            start_date=resolved_start_date,
            end_date=end_date,
            freq="D",
            asset="E",
            adj="qfq",
        )
        if frame is None or frame.empty:
            logger.info("No qfq bar rows returned for {} between {} and {}.", symbol, resolved_start_date, end_date)
            continue
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def normalize_qfq_bars(frame: pd.DataFrame, *, source: str = "tushare_pro_bar") -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount", "adjust_type", "source"]
        )

    working = frame.copy()
    if "symbol" not in working.columns and "ts_code" in working.columns:
        working = working.rename(columns={"ts_code": "symbol"})
    if "volume" not in working.columns and "vol" in working.columns:
        working = working.rename(columns={"vol": "volume"})

    for column in ("symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"):
        if column not in working.columns:
            working[column] = pd.NA

    working["symbol"] = working["symbol"].astype(str).str.strip().str.upper()
    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume", "amount"):
        working[column] = pd.to_numeric(working[column], errors="coerce")
    working["volume"] = working["volume"].fillna(0).round().astype("Int64")
    working["adjust_type"] = "qfq"
    working["source"] = source

    required = ["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"]
    working = working.dropna(subset=required).copy()
    return (
        working[["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount", "adjust_type", "source"]]
        .drop_duplicates(subset=["symbol", "trade_date", "adjust_type"], keep="last")
        .sort_values(["symbol", "trade_date"])
        .reset_index(drop=True)
    )


def _py_value(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def upsert_market_bars(frame: pd.DataFrame, *, chunk_size: int = 5000) -> int:
    normalized = normalize_qfq_bars(frame)
    if normalized.empty:
        return 0

    insert_sql = """
        insert into market.bars_1d (
            symbol, trade_date, open, high, low, close, volume, amount, adjust_type, source, created_at
        ) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
        )
        on conflict (symbol, trade_date, adjust_type) do update set
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            amount = excluded.amount,
            source = excluded.source
    """
    rows = [tuple(_py_value(value) for value in row) for row in normalized.itertuples(index=False, name=None)]
    with _connect_market_database() as conn:
        with conn.cursor() as cur:
            for start in range(0, len(rows), chunk_size):
                cur.executemany(insert_sql, rows[start : start + chunk_size])
        conn.commit()
    return len(rows)


def sync_market_bars_from_tushare(
    *,
    user_id: str | None = None,
    symbols: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> MarketBarsSyncSummary:
    resolved_symbols = _dedupe_symbols(symbols or [])
    if not resolved_symbols:
        resolved_user_id = str(user_id or os.getenv("OPENLIANGHUA_USER_ID") or "bootstrap-admin").strip() or "bootstrap-admin"
        resolved_symbols = load_watchlist_symbols(resolved_user_id)
    else:
        resolved_user_id = user_id

    if not resolved_symbols:
        raise RuntimeError("No symbols were provided and no watchlist_items symbols were found.")

    latest_by_symbol = load_latest_market_trade_dates(resolved_symbols)
    latest_values = [value for value in latest_by_symbol.values() if value is not None]
    latest_before = max(latest_values) if latest_values else None
    explicit_start = normalize_ts_date(start_date) if start_date else None
    start_dates_by_symbol = {
        symbol: explicit_start or _next_ts_date(latest_by_symbol.get(symbol))
        for symbol in resolved_symbols
    }
    resolved_start = min(start_dates_by_symbol.values()) if start_dates_by_symbol else normalize_ts_date(start_date)
    resolved_end = normalize_ts_date(end_date)
    if all(pd.Timestamp(symbol_start) > pd.Timestamp(resolved_end) for symbol_start in start_dates_by_symbol.values()):
        return MarketBarsSyncSummary(
            user_id=resolved_user_id,
            start_date=resolved_start,
            end_date=resolved_end,
            previous_latest_trade_date=str(latest_before.date()) if latest_before is not None else None,
            latest_trade_date=str(latest_before.date()) if latest_before is not None else None,
            requested_symbols=len(resolved_symbols),
            fetched_rows=0,
            upserted_rows=0,
        )

    client = TushareClient()
    raw = fetch_qfq_bars(
        client,
        symbols=resolved_symbols,
        start_date=resolved_start,
        end_date=resolved_end,
        start_dates_by_symbol=start_dates_by_symbol,
    )
    normalized = normalize_qfq_bars(raw)
    upserted_rows = upsert_market_bars(normalized)
    latest_after = load_latest_market_trade_date(resolved_symbols)

    return MarketBarsSyncSummary(
        user_id=resolved_user_id,
        start_date=resolved_start,
        end_date=resolved_end,
        previous_latest_trade_date=str(latest_before.date()) if latest_before is not None else None,
        latest_trade_date=str(latest_after.date()) if latest_after is not None else None,
        requested_symbols=len(resolved_symbols),
        fetched_rows=int(len(normalized)),
        upserted_rows=int(upserted_rows),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally upsert Tushare qfq daily bars into market.bars_1d.")
    parser.add_argument("--user-id", default=None, help="Read symbols from this user's watchlist_items when --symbols is omitted.")
    parser.add_argument("--symbols", default="", help="Comma-separated ts_codes, e.g. 000001.SZ,600519.SH.")
    parser.add_argument("--start-date", default=None, help="YYYYMMDD or YYYY-MM-DD. Defaults to latest market date + 1 day.")
    parser.add_argument("--end-date", default=None, help="YYYYMMDD or YYYY-MM-DD. Defaults to today.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbols = [symbol.strip() for symbol in str(args.symbols or "").split(",") if symbol.strip()]
    summary = sync_market_bars_from_tushare(
        user_id=args.user_id,
        symbols=symbols or None,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    logger.info("Market bars Tushare sync result:\n{}", pformat(asdict(summary), sort_dicts=False))


if __name__ == "__main__":
    main()
