from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.data.akshare_client import AKShareClient
from src.data.universe import load_universe
from src.utils.io import ensure_dir, project_root, save_parquet
from src.utils.logger import configure_logging

logger = configure_logging()


def _snapshot_cache_path(output_dir: Path) -> Path:
    return output_dir / "stock_snapshot.parquet"


def _board_map_cache_path(output_dir: Path) -> Path:
    return output_dir / "industry_board_map.parquet"


def _extend_symbols_with_watchlist(
    symbols: list[str],
    metadata: pd.DataFrame,
    watch_symbols: list[str],
    *,
    default_index_code: str,
) -> tuple[list[str], pd.DataFrame]:
    normalized_watch_symbols = [str(symbol).strip() for symbol in watch_symbols if str(symbol).strip()]
    existing_symbols = set(symbols)
    extra_symbols = [symbol for symbol in normalized_watch_symbols if symbol not in existing_symbols]
    if not extra_symbols:
        return symbols, metadata

    extra_metadata = pd.DataFrame(
        {
            "ts_code": extra_symbols,
            "name": extra_symbols,
            "index_code": default_index_code,
        }
    )
    combined_metadata = (
        pd.concat([metadata, extra_metadata], ignore_index=True)
        .drop_duplicates(subset=["ts_code"], keep="first")
        .reset_index(drop=True)
    )
    combined_symbols = list(dict.fromkeys([*symbols, *extra_symbols]))
    return combined_symbols, combined_metadata


def fetch_symbol_panel(
    client: AKShareClient,
    symbol: str,
    start_date: str,
    end_date: str,
    open_dates: pd.Series,
    metadata_row: pd.Series,
    current_st_symbols: set[str],
) -> pd.DataFrame:
    history = client.stock_hist(symbol=symbol, start_date=start_date, end_date=end_date)
    if history.empty:
        return pd.DataFrame()

    first_trade_date = history["trade_date"].min()
    base = pd.DataFrame({"trade_date": open_dates})
    base = base.loc[base["trade_date"] >= first_trade_date].copy()
    base["ts_code"] = symbol

    merged = base.merge(
        history[
            ["trade_date", "ts_code", "open", "close", "high", "low", "vol", "amount", "turnover_rate", "pct_chg"]
        ],
        on=["trade_date", "ts_code"],
        how="left",
    )
    merged = merged.sort_values("trade_date").reset_index(drop=True)

    merged["pre_close"] = merged["close"].shift(1)
    merged["open_adj"] = merged["open"]
    merged["high_adj"] = merged["high"]
    merged["low_adj"] = merged["low"]
    merged["close_adj"] = merged["close"]
    merged["pre_close_adj"] = merged["pre_close"]
    merged["adj_factor"] = 1.0

    list_date = pd.to_datetime(metadata_row.get("list_date"), errors="coerce")
    if pd.isna(list_date):
        list_date = first_trade_date

    merged["name"] = metadata_row["name"]
    merged["industry"] = metadata_row.get("industry")
    merged["list_date"] = list_date
    merged["is_current_name_st"] = symbol in current_st_symbols or "ST" in str(metadata_row["name"]).upper()
    merged["is_st"] = merged["is_current_name_st"]
    merged["is_suspend"] = merged["close"].isna()

    one_word_board = (
        merged["open"].notna()
        & merged["high"].notna()
        & merged["low"].notna()
        & merged["close"].notna()
        & (merged["open"] == merged["high"])
        & (merged["high"] == merged["low"])
        & (merged["low"] == merged["close"])
    )
    merged["is_limit_up_close"] = merged["pct_chg"].ge(9.5).fillna(False)
    merged["is_limit_down_close"] = merged["pct_chg"].le(-9.5).fillna(False)
    merged["is_buy_locked"] = (one_word_board & merged["pct_chg"].ge(9.5)).fillna(False)
    merged["is_sell_locked"] = (one_word_board & merged["pct_chg"].le(-9.5)).fillna(False)
    merged["up_limit"] = np.nan
    merged["down_limit"] = np.nan

    return merged


def build_current_index_membership(
    client: AKShareClient,
    index_code: str,
    open_dates: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    raw_membership = client.current_index_members(index_code=index_code)
    entry_dates = client.current_index_entry_dates(index_code=index_code)
    membership = raw_membership.merge(
        entry_dates[["ts_code", "entry_date"]],
        on="ts_code",
        how="left",
    )
    membership["entry_date"] = pd.to_datetime(membership["entry_date"], errors="coerce")
    membership["entry_date"] = membership["entry_date"].fillna(open_dates.min())

    daily_membership = membership[["index_code", "ts_code", "name", "index_weight", "entry_date"]].merge(
        open_dates.to_frame(name="trade_date"),
        how="cross",
    )
    daily_membership = daily_membership.loc[daily_membership["trade_date"] >= daily_membership["entry_date"]].copy()
    daily_membership["is_index_member"] = True
    symbols = membership["ts_code"].drop_duplicates().sort_values().tolist()
    raw_membership = membership
    return raw_membership, daily_membership, symbols


def _symbol_cache_path(raw_dir: Path, symbol: str) -> Path:
    return raw_dir / f"{symbol}.parquet"


def _download_one_symbol(
    client: AKShareClient,
    raw_dir: Path,
    symbol: str,
    start_date: str,
    end_date: str,
    open_dates: pd.Series,
    metadata_row: pd.Series,
    current_st_symbols: set[str],
    skip_existing: bool,
) -> tuple[str, int, str]:
    cache_path = _symbol_cache_path(raw_dir, symbol)
    if skip_existing and cache_path.exists():
        cached = pd.read_parquet(cache_path)
        return symbol, len(cached), "cached"

    panel = fetch_symbol_panel(
        client=client,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        open_dates=open_dates,
        metadata_row=metadata_row,
        current_st_symbols=current_st_symbols,
    )
    if panel.empty:
        return symbol, 0, "empty"

    save_parquet(panel, cache_path)
    return symbol, len(panel), "downloaded"


def _fetch_symbol_snapshot(
    client: AKShareClient,
    symbol: str,
) -> dict[str, object]:
    snapshot = client.stock_individual_snapshot(symbol)
    return snapshot


def _load_or_build_board_industry_map(
    client: AKShareClient,
    output_dir: Path,
) -> pd.DataFrame:
    cache_path = _board_map_cache_path(output_dir)
    if cache_path.exists():
        board_map = pd.read_parquet(cache_path)
    else:
        board_map = client.current_board_industry_map()
        save_parquet(board_map, cache_path)
    if board_map.empty:
        return pd.DataFrame(columns=["ts_code", "industry"])
    return board_map.drop_duplicates(subset=["ts_code"], keep="first").reset_index(drop=True)


def _load_or_build_symbol_snapshot(
    client: AKShareClient,
    output_dir: Path,
    symbols: list[str],
    max_workers: int,
) -> pd.DataFrame:
    cache_path = _snapshot_cache_path(output_dir)
    if cache_path.exists():
        snapshot = pd.read_parquet(cache_path)
    else:
        snapshot = pd.DataFrame(columns=["ts_code", "industry", "list_date"])

    if not snapshot.empty:
        snapshot = snapshot.drop_duplicates(subset=["ts_code"], keep="last").copy()
        snapshot["list_date"] = pd.to_datetime(snapshot["list_date"], errors="coerce")

    indexed_snapshot = snapshot.set_index("ts_code", drop=False) if not snapshot.empty else snapshot
    missing_symbols: list[str] = []
    for symbol in symbols:
        if indexed_snapshot.empty or symbol not in indexed_snapshot.index:
            missing_symbols.append(symbol)

    if missing_symbols:
        rows: list[dict[str, object]] = []
        future_to_symbol = {}
        metadata_workers = max(1, min(4, max_workers))
        with ThreadPoolExecutor(max_workers=metadata_workers) as executor:
            for symbol in missing_symbols:
                future = executor.submit(_fetch_symbol_snapshot, client, symbol)
                future_to_symbol[future] = symbol

            for future in tqdm(as_completed(future_to_symbol), total=len(future_to_symbol), desc="Fetching metadata"):
                symbol = future_to_symbol[future]
                try:
                    rows.append(future.result())
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"Failed to fetch metadata for {symbol}: {exc}")
                    rows.append({"ts_code": symbol, "industry": None, "list_date": pd.NaT})

        if rows:
            fresh = pd.DataFrame(rows)
            fresh["list_date"] = pd.to_datetime(fresh["list_date"], errors="coerce")
            snapshot = (
                pd.concat([snapshot.reset_index(drop=True), fresh], ignore_index=True)
                .drop_duplicates(subset=["ts_code"], keep="last")
                .sort_values("ts_code")
                .reset_index(drop=True)
            )
            save_parquet(snapshot, cache_path)

    if not snapshot.empty and "ts_code" in snapshot.columns:
        missing_industry_mask = snapshot["ts_code"].isin(symbols) & snapshot["industry"].isna()
        if missing_industry_mask.any():
            board_map = _load_or_build_board_industry_map(client=client, output_dir=output_dir)
            if not board_map.empty:
                industry_lookup = board_map.set_index("ts_code")["industry"]
                snapshot.loc[missing_industry_mask, "industry"] = snapshot.loc[
                    missing_industry_mask, "ts_code"
                ].map(industry_lookup)
                save_parquet(snapshot.reset_index(drop=True), cache_path)

    if not snapshot.empty and "ts_code" in snapshot.columns:
        return snapshot.reset_index(drop=True).loc[snapshot["ts_code"].isin(symbols)].copy()
    return pd.DataFrame(columns=["ts_code", "industry", "list_date"])


def _combine_cached_symbols(raw_dir: Path, symbols: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for symbol in symbols:
        cache_path = _symbol_cache_path(raw_dir, symbol)
        if cache_path.exists():
            frames.append(pd.read_parquet(cache_path))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def run(
    start_date: str,
    end_date: str,
    max_workers: int,
    symbol_offset: int,
    symbol_limit: int | None,
    skip_existing: bool,
) -> None:
    root = project_root()
    output_dir = ensure_dir(root / "data" / "staging")
    raw_dir = ensure_dir(root / "data" / "raw" / "akshare")
    universe = load_universe()

    client = AKShareClient()
    calendar = client.trade_calendar()
    open_dates = calendar.loc[
        (calendar["trade_date"] >= pd.Timestamp(start_date)) & (calendar["trade_date"] <= pd.Timestamp(end_date)),
        "trade_date",
    ].reset_index(drop=True)
    current_st_symbols = client.current_st_symbols()

    index_membership_raw = pd.DataFrame()
    index_membership_daily = pd.DataFrame()

    if universe["mode"] == "current_index":
        index_membership_raw, index_membership_daily, symbols = build_current_index_membership(
            client=client,
            index_code=universe["index_code"],
            open_dates=open_dates,
        )
        metadata = index_membership_daily[["ts_code", "name"]].drop_duplicates().copy()
        metadata["index_code"] = universe["index_code"]
    else:
        symbols = universe["symbols"]
        metadata = pd.DataFrame({"ts_code": symbols, "name": symbols, "index_code": universe["benchmark"]})

    symbols, metadata = _extend_symbols_with_watchlist(
        symbols=symbols,
        metadata=metadata,
        watch_symbols=universe.get("watch_symbols", []),
        default_index_code=universe["benchmark"],
    )

    snapshot = _load_or_build_symbol_snapshot(
        client=client,
        output_dir=output_dir,
        symbols=metadata["ts_code"].drop_duplicates().sort_values().tolist(),
        max_workers=max_workers,
    )
    if not snapshot.empty:
        metadata = metadata.merge(snapshot, on="ts_code", how="left")

    save_parquet(calendar, output_dir / "trade_calendar.parquet")
    save_parquet(metadata.sort_values("ts_code").reset_index(drop=True), output_dir / "stock_basic.parquet")
    if not index_membership_raw.empty:
        save_parquet(index_membership_raw, output_dir / "index_membership_raw.parquet")
        save_parquet(index_membership_daily, output_dir / "index_membership_daily.parquet")

    scoped_symbols = symbols[symbol_offset:]
    if symbol_limit is not None:
        scoped_symbols = scoped_symbols[:symbol_limit]
    if not scoped_symbols:
        raise RuntimeError("No symbols left to download after applying offset/limit.")

    future_to_symbol = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for symbol in scoped_symbols:
            row = metadata.loc[metadata["ts_code"] == symbol]
            if row.empty:
                continue
            future = executor.submit(
                _download_one_symbol,
                client,
                raw_dir,
                symbol,
                start_date,
                end_date,
                open_dates,
                row.iloc[0],
                current_st_symbols,
                skip_existing,
            )
            future_to_symbol[future] = symbol

        for future in tqdm(as_completed(future_to_symbol), total=len(future_to_symbol), desc="Downloading symbols"):
            symbol = future_to_symbol[future]
            try:
                _, rows, status = future.result()
                if status == "empty":
                    logger.warning(f"{symbol}: no rows returned from AKShare.")
            except Exception as exc:  # noqa: BLE001
                logger.exception(f"Failed to download {symbol}: {exc}")

    daily_bar = _combine_cached_symbols(raw_dir=raw_dir, symbols=scoped_symbols)
    if daily_bar.empty:
        raise RuntimeError("No symbol data was cached from AKShare.")

    for column in ("name", "industry", "list_date"):
        if column in metadata.columns:
            value_map = metadata.drop_duplicates(subset=["ts_code"]).set_index("ts_code")[column]
            daily_bar[column] = daily_bar[column].where(daily_bar[column].notna(), daily_bar["ts_code"].map(value_map))

    if not index_membership_daily.empty:
        daily_bar = daily_bar.merge(
            index_membership_daily[["trade_date", "ts_code", "index_code", "index_weight", "is_index_member"]],
            on=["trade_date", "ts_code"],
            how="left",
        )
        daily_bar["is_index_member"] = daily_bar["is_index_member"].fillna(False)
    else:
        daily_bar["index_code"] = universe["benchmark"]
        daily_bar["index_weight"] = np.nan
        daily_bar["is_index_member"] = True

    save_parquet(daily_bar, output_dir / "daily_bar.parquet")
    logger.info(f"Saved AKShare market panel to {output_dir / 'daily_bar.parquet'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download free A-share daily data from AKShare.")
    parser.add_argument("--start-date", default="20180101", help="YYYYMMDD")
    parser.add_argument("--end-date", default="20251231", help="YYYYMMDD")
    parser.add_argument("--max-workers", type=int, default=4, help="Concurrent download workers.")
    parser.add_argument("--symbol-offset", type=int, default=0, help="Start from this symbol offset.")
    parser.add_argument("--symbol-limit", type=int, default=None, help="Maximum number of symbols to download.")
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Redownload symbols even if cached parquet files already exist.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        start_date=args.start_date,
        end_date=args.end_date,
        max_workers=max(1, args.max_workers),
        symbol_offset=max(0, args.symbol_offset),
        symbol_limit=args.symbol_limit,
        skip_existing=not args.refresh_existing,
    )
    from src.db.dashboard_sync import sync_dashboard_artifacts

    summary = sync_dashboard_artifacts()
    logger.info(summary.message)
