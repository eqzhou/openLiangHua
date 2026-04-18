from __future__ import annotations

from pathlib import Path

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from src.app.repositories.config_repository import load_universe_config, load_watchlist_config
from src.app.repositories.report_repository import save_binary_dataset
from src.db.settings import get_database_settings
from src.utils.data_source import active_data_source
from src.utils.io import ensure_dir, project_root


def _connect():
    settings = get_database_settings()
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.name,
        user=settings.user,
        password=settings.password,
        connect_timeout=settings.connect_timeout,
        row_factory=dict_row,
    )


def _selected_symbols(root: Path) -> list[str]:
    universe = load_universe_config(root, prefer_database=False)
    watchlist = load_watchlist_config(root, prefer_database=False)

    symbols: list[str] = []
    for item in universe.get("symbols", []) or []:
        symbol = str(item or "").strip()
        if symbol and symbol not in symbols:
            symbols.append(symbol)

    for item in universe.get("watch_symbols", []) or []:
        symbol = str(item or "").strip()
        if symbol and symbol not in symbols:
            symbols.append(symbol)

    for group in ("holdings", "focus_pool"):
        for item in watchlist.get(group, []) or []:
            symbol = str(item.get("ts_code", "") or "").strip()
            if symbol and symbol not in symbols:
                symbols.append(symbol)

    return symbols


def _load_calendar() -> pd.DataFrame:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select trading_day as trade_date
                from ref.trading_calendar
                where is_open = true
                order by trading_day
                """
            )
            rows = cur.fetchall()
    frame = pd.DataFrame(rows)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def _load_instruments(symbols: list[str]) -> pd.DataFrame:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select symbol as ts_code, name, industry, list_date, is_st, exchange
                from ref.instruments
                where symbol = any(%s)
                order by symbol
                """,
                (symbols,),
            )
            rows = cur.fetchall()
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["list_date"] = pd.to_datetime(frame["list_date"], errors="coerce")
    return frame


def _load_bars(symbols: list[str]) -> pd.DataFrame:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    symbol as ts_code,
                    trade_date,
                    open,
                    high,
                    low,
                    close,
                    volume as vol,
                    amount
                from market.bars_1d
                where symbol = any(%s)
                order by symbol, trade_date
                """,
                (symbols,),
            )
            rows = cur.fetchall()
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        for column in ("open", "high", "low", "close", "vol", "amount"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def build_ccproject_daily_bar(root: Path | None = None) -> dict[str, object]:
    resolved_root = root or project_root()
    data_source = active_data_source()
    benchmark_code = str(load_universe_config(resolved_root, prefer_database=False).get("benchmark", "000905.SH"))
    symbols = _selected_symbols(resolved_root)
    if not symbols:
        raise RuntimeError("No symbols found in universe/watchlist configuration.")

    calendar = _load_calendar()
    instruments = _load_instruments(symbols)
    bars = _load_bars(symbols)
    if instruments.empty or bars.empty:
        raise RuntimeError("Missing instruments or bars from cc_project.")

    instruments["list_date"] = pd.to_datetime(instruments["list_date"], errors="coerce")
    calendar_dates = calendar["trade_date"].sort_values().drop_duplicates().reset_index(drop=True)

    symbol_frames: list[pd.DataFrame] = []
    for row in instruments.to_dict(orient="records"):
        symbol = str(row["ts_code"])
        list_date = pd.Timestamp(row["list_date"]) if row.get("list_date") is not None else calendar_dates.min()
        symbol_calendar = calendar_dates.loc[calendar_dates >= list_date].to_frame(name="trade_date")
        symbol_calendar["ts_code"] = symbol
        symbol_bars = bars.loc[bars["ts_code"] == symbol].copy()
        merged = symbol_calendar.merge(symbol_bars, on=["ts_code", "trade_date"], how="left")
        merged["name"] = row.get("name") or symbol
        merged["industry"] = row.get("industry") or ""
        merged["list_date"] = list_date
        merged["index_code"] = benchmark_code
        merged["is_index_member"] = True
        merged["is_current_name_st"] = bool(row.get("is_st"))
        merged["is_st"] = bool(row.get("is_st"))
        merged["is_suspend"] = merged["close"].isna()
        merged["is_limit_up_close"] = False
        merged["is_limit_down_close"] = False
        merged["is_buy_locked"] = False
        merged["is_sell_locked"] = False
        merged["up_limit"] = pd.NA
        merged["down_limit"] = pd.NA
        merged["adj_factor"] = 1.0
        merged["open_adj"] = merged["open"]
        merged["high_adj"] = merged["high"]
        merged["low_adj"] = merged["low"]
        merged["close_adj"] = merged["close"]
        merged["pre_close"] = merged["close"].shift(1)
        merged["pre_close_adj"] = merged["close_adj"].shift(1)
        merged["pct_chg"] = merged["close"].pct_change() * 100.0
        symbol_frames.append(merged)

    daily_bar = pd.concat(symbol_frames, ignore_index=True).sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

    ensure_dir(resolved_root / "data" / "staging")
    save_binary_dataset(
        resolved_root,
        data_source=data_source,
        directory="data/staging",
        filename="daily_bar.parquet",
        artifact_name="daily_bar",
        frame=daily_bar,
    )
    save_binary_dataset(
        resolved_root,
        data_source=data_source,
        directory="data/staging",
        filename="trade_calendar.parquet",
        artifact_name="trade_calendar",
        frame=calendar,
    )
    save_binary_dataset(
        resolved_root,
        data_source=data_source,
        directory="data/staging",
        filename="stock_basic.parquet",
        artifact_name="stock_basic",
        frame=instruments[["ts_code", "name", "industry", "list_date"]],
    )

    return {
        "data_source": data_source,
        "symbols": len(symbols),
        "rows": int(len(daily_bar)),
        "date_min": str(daily_bar["trade_date"].min().date()),
        "date_max": str(daily_bar["trade_date"].max().date()),
    }


def run() -> None:
    summary = build_ccproject_daily_bar()
    print(summary)
    from src.db.dashboard_sync import sync_dataset_summary_artifact, sync_watchlist_snapshot_artifact

    dataset_summary = sync_dataset_summary_artifact()
    watchlist_summary = sync_watchlist_snapshot_artifact()
    print(dataset_summary.message)
    print(watchlist_summary.message)


if __name__ == "__main__":
    run()
