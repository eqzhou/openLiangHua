from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.app.repositories.report_repository import save_binary_dataset
from src.data.index_membership import expand_index_membership
from src.data.myquant_client import MyQuantClient
from src.data.myquant_panel import drop_trailing_empty_price_dates, trim_open_dates_to_bars
from src.data.universe import load_universe
from src.utils.io import ensure_dir, project_root
from src.utils.logger import configure_logging

logger = configure_logging()

INSTRUMENT_INFO_FIELDS = "symbol,sec_name,listed_date,delisted_date,exchange,sec_id,sec_abbr,sec_type"
INSTRUMENT_HISTORY_FIELDS = "symbol,trade_date,sec_name,is_suspended,is_st,pre_close,upper_limit,lower_limit,adj_factor"


def _chunked(values: list[str], chunk_size: int) -> Iterable[list[str]]:
    for idx in range(0, len(values), chunk_size):
        yield values[idx : idx + chunk_size]


def _concat_frames(frames: list[pd.DataFrame], columns: list[str] | None = None) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=columns or [])
    return pd.concat(frames, ignore_index=True)


def _load_industry_fallback(output_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    snapshot_path = output_dir / "stock_snapshot.parquet"
    if snapshot_path.exists():
        snapshot = pd.read_parquet(snapshot_path)
        keep_cols = [column for column in ("ts_code", "industry") if column in snapshot.columns]
        if keep_cols:
            frames.append(snapshot[keep_cols].copy())

    board_map_path = output_dir / "industry_board_map.parquet"
    if board_map_path.exists():
        board_map = pd.read_parquet(board_map_path)
        keep_cols = [column for column in ("ts_code", "industry") if column in board_map.columns]
        if keep_cols:
            frames.append(board_map[keep_cols].copy())

    if not frames:
        return pd.DataFrame(columns=["ts_code", "industry"])

    return (
        pd.concat(frames, ignore_index=True)
        .dropna(subset=["ts_code"])
        .drop_duplicates(subset=["ts_code"], keep="first")
        .reset_index(drop=True)
    )


def _normalize_symbol_metadata(
    metadata: pd.DataFrame,
    symbols: list[str],
    output_dir: Path,
) -> pd.DataFrame:
    frame = pd.DataFrame({"ts_code": symbols}).merge(metadata, on="ts_code", how="left")

    if "name" not in frame.columns:
        frame["name"] = frame["ts_code"]
    frame["name"] = frame["name"].fillna(frame["ts_code"])

    if "listed_date" in frame.columns:
        frame["list_date"] = pd.to_datetime(frame["listed_date"], errors="coerce")
    else:
        frame["list_date"] = pd.NaT

    if "delisted_date" in frame.columns:
        frame["delist_date"] = pd.to_datetime(frame["delisted_date"], errors="coerce")
    else:
        frame["delist_date"] = pd.NaT

    industry = _load_industry_fallback(output_dir)
    if not industry.empty:
        frame = frame.merge(industry, on="ts_code", how="left", suffixes=("", "_fallback"))
        if "industry_fallback" in frame.columns:
            if "industry" in frame.columns:
                frame["industry"] = frame["industry"].fillna(frame["industry_fallback"])
            else:
                frame["industry"] = frame["industry_fallback"]
            frame = frame.drop(columns=["industry_fallback"])
    elif "industry" not in frame.columns:
        frame["industry"] = pd.NA

    keep_cols = ["ts_code", "name", "industry", "list_date", "delist_date"]
    return frame[keep_cols].drop_duplicates(subset=["ts_code"], keep="first").sort_values("ts_code").reset_index(drop=True)


def _fetch_instrument_info(
    client: MyQuantClient,
    symbols: list[str],
    chunk_size: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    chunks = list(_chunked(symbols, chunk_size))
    for chunk in tqdm(chunks, desc="MyQuant static info", leave=False):
        frame = client.instrument_infos(chunk, fields=INSTRUMENT_INFO_FIELDS)
        if not frame.empty:
            frames.append(frame)
    return _concat_frames(frames)


def _fetch_history_instruments(
    client: MyQuantClient,
    symbols: list[str],
    start_date: str,
    end_date: str,
    chunk_size: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    chunks = list(_chunked(symbols, chunk_size))
    for chunk in tqdm(chunks, desc="MyQuant daily metadata", leave=False):
        frame = client.history_instruments(
            chunk,
            start_date=start_date,
            end_date=end_date,
            fields=INSTRUMENT_HISTORY_FIELDS,
        )
        if not frame.empty:
            frames.append(frame)

    history = _concat_frames(frames)
    if history.empty:
        return history
    return history.drop_duplicates(subset=["trade_date", "ts_code"], keep="last").reset_index(drop=True)


def _fetch_history_bars(
    client: MyQuantClient,
    symbols: list[str],
    start_date: str,
    end_date: str,
    chunk_size: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    chunks = list(_chunked(symbols, chunk_size))
    for chunk in tqdm(chunks, desc="MyQuant daily bars", leave=False):
        frame = client.history_bars(
            chunk,
            start_date=start_date,
            end_date=end_date,
            frequency="1d",
            adjust="none",
        )
        if not frame.empty:
            frames.append(frame)

    bars = _concat_frames(frames)
    if bars.empty:
        return bars
    return bars.drop_duplicates(subset=["trade_date", "ts_code"], keep="last").reset_index(drop=True)


def _build_symbol_panel(
    open_dates: pd.Series,
    metadata: pd.DataFrame,
    daily_meta: pd.DataFrame,
    bars: pd.DataFrame,
    daily_membership: pd.DataFrame,
    benchmark: str,
) -> pd.DataFrame:
    base = metadata.merge(open_dates.to_frame(name="trade_date"), how="cross")
    base["trade_date"] = pd.to_datetime(base["trade_date"])
    base["list_date"] = pd.to_datetime(base["list_date"], errors="coerce")
    base["delist_date"] = pd.to_datetime(base["delist_date"], errors="coerce")

    first_open = open_dates.min()
    base["effective_list_date"] = base["list_date"].fillna(first_open)
    base = base.loc[base["trade_date"] >= base["effective_list_date"]].copy()
    has_delist = base["delist_date"].notna()
    base = base.loc[~has_delist | (base["trade_date"] <= base["delist_date"])].copy()
    base = base.drop(columns=["effective_list_date", "delist_date"])

    daily_cols = [
        column
        for column in (
            "trade_date",
            "ts_code",
            "name",
            "is_suspended",
            "is_st",
            "pre_close",
            "upper_limit",
            "lower_limit",
            "adj_factor",
        )
        if column in daily_meta.columns
    ]
    if daily_cols:
        merged = base.merge(
            daily_meta[daily_cols].rename(columns={"name": "daily_name"}),
            on=["trade_date", "ts_code"],
            how="left",
        )
    else:
        merged = base.copy()

    if not bars.empty:
        merged = merged.merge(bars, on=["trade_date", "ts_code"], how="left")

    if "daily_name" in merged.columns:
        merged["name"] = merged["name"].fillna(merged["daily_name"])
        merged = merged.drop(columns=["daily_name"])
    merged["name"] = merged["name"].fillna(merged["ts_code"])

    for column in ("open", "high", "low", "close", "vol", "amount", "pre_close", "upper_limit", "lower_limit", "adj_factor"):
        if column in merged.columns:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")

    if "pre_close" not in merged.columns:
        merged["pre_close"] = np.nan
    merged["pre_close"] = merged["pre_close"].where(
        merged["pre_close"].notna(),
        merged.groupby("ts_code")["close"].shift(1),
    )

    if "adj_factor" not in merged.columns:
        merged["adj_factor"] = 1.0
    merged["adj_factor"] = merged.groupby("ts_code")["adj_factor"].transform(lambda series: series.ffill().bfill())
    merged["adj_factor"] = merged["adj_factor"].fillna(1.0)
    latest_factor = merged.groupby("ts_code")["adj_factor"].transform("last")
    scale = pd.Series(1.0, index=merged.index, dtype="float64")
    valid_scale = merged["adj_factor"].notna() & latest_factor.notna() & (latest_factor != 0)
    scale.loc[valid_scale] = merged.loc[valid_scale, "adj_factor"] / latest_factor.loc[valid_scale]

    for column in ("open", "high", "low", "close"):
        merged[f"{column}_adj"] = merged[column] * scale
    merged["pre_close_adj"] = merged.groupby("ts_code")["close_adj"].shift(1)

    merged["is_current_name_st"] = merged["name"].fillna("").str.contains("ST", case=False, na=False)
    if "is_st" in merged.columns:
        merged["is_st"] = merged["is_st"].where(merged["is_st"].notna(), merged["is_current_name_st"]).astype(bool)
    else:
        merged["is_st"] = merged["is_current_name_st"]

    if "is_suspended" in merged.columns:
        merged["is_suspend"] = merged["is_suspended"].where(merged["is_suspended"].notna(), merged["close"].isna()).astype(bool)
        merged = merged.drop(columns=["is_suspended"])
    else:
        merged["is_suspend"] = merged["close"].isna()

    merged["up_limit"] = merged["upper_limit"] if "upper_limit" in merged.columns else np.nan
    merged["down_limit"] = merged["lower_limit"] if "lower_limit" in merged.columns else np.nan
    merged["is_limit_up_close"] = (
        merged["close"].notna()
        & pd.Series(merged["up_limit"]).notna()
        & (merged["close"] >= merged["up_limit"] - 1e-6)
    )
    merged["is_limit_down_close"] = (
        merged["close"].notna()
        & pd.Series(merged["down_limit"]).notna()
        & (merged["close"] <= merged["down_limit"] + 1e-6)
    )
    one_word_board = (
        merged["open"].notna()
        & merged["high"].notna()
        & merged["low"].notna()
        & merged["close"].notna()
        & (merged["open"] == merged["high"])
        & (merged["high"] == merged["low"])
        & (merged["low"] == merged["close"])
    )
    merged["is_buy_locked"] = (one_word_board & merged["is_limit_up_close"]).astype(bool)
    merged["is_sell_locked"] = (one_word_board & merged["is_limit_down_close"]).astype(bool)

    if not daily_membership.empty:
        merged = merged.merge(
            daily_membership[["trade_date", "ts_code", "index_code", "index_weight", "is_index_member"]],
            on=["trade_date", "ts_code"],
            how="left",
        )
        merged["is_index_member"] = merged["is_index_member"].where(merged["is_index_member"].notna(), False).astype(bool)
    else:
        merged["index_code"] = benchmark
        merged["index_weight"] = np.nan
        merged["is_index_member"] = True

    keep_cols = [
        "trade_date",
        "ts_code",
        "name",
        "industry",
        "list_date",
        "index_code",
        "index_weight",
        "is_index_member",
        "is_current_name_st",
        "is_st",
        "is_suspend",
        "is_limit_up_close",
        "is_limit_down_close",
        "is_buy_locked",
        "is_sell_locked",
        "up_limit",
        "down_limit",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "open_adj",
        "high_adj",
        "low_adj",
        "close_adj",
        "pre_close_adj",
        "vol",
        "amount",
        "adj_factor",
    ]
    for column in keep_cols:
        if column not in merged.columns:
            merged[column] = np.nan

    return merged[keep_cols].sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def _save_with_prefix(root: Path, frame: pd.DataFrame, filename: str, artifact_name: str, write_canonical: bool = False) -> None:
    save_binary_dataset(
        root,
        data_source="myquant",
        directory="data/staging",
        filename=filename,
        artifact_name=artifact_name,
        frame=frame,
        write_canonical=write_canonical,
    )


def run(
    start_date: str,
    end_date: str,
    chunk_size: int,
    write_canonical: bool,
) -> None:
    root = project_root()
    output_dir = ensure_dir(root / "data" / "staging")
    ensure_dir(root / "data" / "raw" / "myquant")

    universe = load_universe()
    client = MyQuantClient()

    calendar = client.trade_calendar(start_date=start_date, end_date=end_date, exchange="SHSE")
    if calendar.empty:
        raise RuntimeError("MyQuant returned an empty trading calendar.")
    open_dates = calendar["trade_date"].drop_duplicates().sort_values().reset_index(drop=True)

    membership_raw = pd.DataFrame(columns=["index_code", "trade_date", "con_code", "weight"])
    membership_daily = pd.DataFrame(columns=["trade_date", "ts_code", "index_code", "index_weight", "is_index_member"])

    if universe["mode"] == "current_index":
        membership_raw = client.history_constituents(
            index_code=universe["index_code"],
            start_date=start_date,
            end_date=end_date,
        )
        if membership_raw.empty:
            raise RuntimeError(
                "MyQuant historical constituents are empty. Check MYQUANT_TOKEN permissions or the index code."
            )
        membership_daily = expand_index_membership(membership_raw, open_dates=open_dates)
        symbols = membership_raw["con_code"].drop_duplicates().sort_values().tolist()
    else:
        symbols = sorted(set(universe["symbols"]))

    if not symbols:
        raise RuntimeError("No symbols resolved for the MyQuant data download.")

    static_meta_raw = _fetch_instrument_info(client=client, symbols=symbols, chunk_size=chunk_size)
    metadata = _normalize_symbol_metadata(static_meta_raw, symbols=symbols, output_dir=output_dir)

    daily_meta = _fetch_history_instruments(
        client=client,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        chunk_size=chunk_size,
    )
    bars = _fetch_history_bars(
        client=client,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        chunk_size=chunk_size,
    )
    if bars.empty:
        raise RuntimeError("MyQuant returned no daily bar history for the requested range.")
    aligned_open_dates, latest_bar_date = trim_open_dates_to_bars(open_dates, bars)
    if latest_bar_date is not None and len(aligned_open_dates) < len(open_dates):
        logger.warning(
            "MyQuant bars stop at {} while the trade calendar reaches {}. "
            "Trailing empty trade dates will be trimmed from the daily panel.",
            latest_bar_date.date(),
            pd.Timestamp(open_dates.max()).date(),
        )
        open_dates = aligned_open_dates

    daily_bar = _build_symbol_panel(
        open_dates=open_dates,
        metadata=metadata,
        daily_meta=daily_meta,
        bars=bars,
        daily_membership=membership_daily,
        benchmark=universe["benchmark"],
    )
    daily_bar, trailing_empty_dates = drop_trailing_empty_price_dates(daily_bar)
    if trailing_empty_dates:
        logger.warning(
            "Dropped trailing empty-price dates from MyQuant daily panel: {}",
            ", ".join(str(date.date()) for date in trailing_empty_dates),
        )

    _save_with_prefix(root, calendar, "trade_calendar.parquet", "trade_calendar", write_canonical=write_canonical)
    _save_with_prefix(root, metadata, "stock_basic.parquet", "stock_basic", write_canonical=write_canonical)
    _save_with_prefix(root, static_meta_raw, "instrument_infos.parquet", "instrument_infos")
    _save_with_prefix(root, daily_meta, "instrument_history.parquet", "instrument_history")
    _save_with_prefix(root, bars, "bars_raw.parquet", "bars_raw")
    if not membership_raw.empty:
        _save_with_prefix(root, membership_raw, "index_membership_raw.parquet", "index_membership_raw", write_canonical=write_canonical)
        _save_with_prefix(root, membership_daily, "index_membership_daily.parquet", "index_membership_daily", write_canonical=write_canonical)
    _save_with_prefix(root, daily_bar, "daily_bar.parquet", "daily_bar", write_canonical=write_canonical)

    logger.info(f"MyQuant daily bar rows: {len(daily_bar):,}")
    logger.info(f"MyQuant symbols: {daily_bar['ts_code'].nunique():,}")
    logger.info(f"Saved MyQuant outputs to {output_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download A-share daily data from MyQuant in a research-friendly schema.")
    parser.add_argument("--start-date", default="2018-01-01", help="YYYY-MM-DD")
    parser.add_argument("--end-date", default="2025-12-31", help="YYYY-MM-DD")
    parser.add_argument("--chunk-size", type=int, default=10, help="Symbols per MyQuant request chunk.")
    parser.add_argument(
        "--write-canonical",
        action="store_true",
        help="Also overwrite data/staging canonical files like daily_bar.parquet with MyQuant outputs.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        start_date=args.start_date,
        end_date=args.end_date,
        chunk_size=max(1, args.chunk_size),
        write_canonical=args.write_canonical,
    )
    from src.db.dashboard_sync import sync_dataset_summary_artifact, sync_watchlist_snapshot_artifact

    dataset_summary = sync_dataset_summary_artifact(data_source="myquant")
    watchlist_summary = sync_watchlist_snapshot_artifact(data_source="myquant")
    logger.info(dataset_summary.message)
    logger.info(watchlist_summary.message)
