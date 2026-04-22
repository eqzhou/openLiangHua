from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.app.repositories.report_repository import save_binary_dataset
from src.data.tushare_client import TushareClient
from src.data.universe import load_universe
from src.utils.data_source import active_data_source, normalize_data_source, source_or_canonical_path
from src.utils.io import ensure_dir, project_root
from src.utils.logger import configure_logging

logger = configure_logging()

REQUIRED_PANEL_COLUMNS = [
    "trade_date",
    "ts_code",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
    "name",
    "industry",
    "list_date",
    "index_code",
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
    "adj_factor",
    "open_adj",
    "high_adj",
    "low_adj",
    "close_adj",
    "pre_close",
    "pre_close_adj",
    "pct_chg",
]


@dataclass(frozen=True)
class IncrementalSyncSummary:
    target_source: str
    previous_latest_trade_date: str | None
    latest_trade_date: str | None
    appended_trade_dates: int
    appended_rows: int
    symbols: int
    daily_bar_artifact_ref: str
    trade_calendar_artifact_ref: str
    stock_basic_artifact_ref: str


def _today_ts_date() -> str:
    return datetime.now().strftime("%Y%m%d")


def _normalize_ts_date(value: str | None) -> str:
    if not value:
        return _today_ts_date()
    normalized = str(value).strip()
    if not normalized:
        return _today_ts_date()
    if len(normalized) == 8 and normalized.isdigit():
        return normalized
    return pd.Timestamp(normalized).strftime("%Y%m%d")


def _read_local_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    for column in ("trade_date", "list_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def _normalize_trade_calendar(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["trade_date"])

    working = frame.copy()
    if "cal_date" in working.columns and "trade_date" not in working.columns:
        working = working.rename(columns={"cal_date": "trade_date"})

    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    if "is_open" in working.columns:
        open_mask = working["is_open"].astype(str).isin({"1", "True", "true"})
        working = working.loc[open_mask].copy()

    return (
        working[["trade_date"]]
        .dropna(subset=["trade_date"])
        .drop_duplicates(subset=["trade_date"], keep="first")
        .sort_values("trade_date")
        .reset_index(drop=True)
    )


def _normalize_stock_basic(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["ts_code", "name", "industry", "list_date"])

    working = frame.copy()
    if "list_date" in working.columns:
        working["list_date"] = pd.to_datetime(working["list_date"], errors="coerce")

    for column in ("ts_code", "name", "industry", "list_date"):
        if column not in working.columns:
            working[column] = pd.NA

    return (
        working[["ts_code", "name", "industry", "list_date"]]
        .dropna(subset=["ts_code"])
        .drop_duplicates(subset=["ts_code"], keep="last")
        .sort_values("ts_code")
        .reset_index(drop=True)
    )


def _normalize_market_frame(frame: pd.DataFrame, *, numeric_columns: list[str] | None = None) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    working = frame.copy()
    if "trade_date" in working.columns:
        working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")

    for column in numeric_columns or []:
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")

    return working


def _load_existing_inputs(root: Path, target_source: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Path, Path, Path]:
    staging_dir = ensure_dir(root / "data" / "staging")
    daily_bar_path = source_or_canonical_path(staging_dir, "daily_bar.parquet", target_source)
    stock_basic_path = source_or_canonical_path(staging_dir, "stock_basic.parquet", target_source)
    trade_calendar_path = source_or_canonical_path(staging_dir, "trade_calendar.parquet", target_source)

    daily_bar = _read_local_frame(daily_bar_path)
    stock_basic = _read_local_frame(stock_basic_path)
    trade_calendar = _read_local_frame(trade_calendar_path)
    return daily_bar, stock_basic, trade_calendar, daily_bar_path, stock_basic_path, trade_calendar_path


def _tracked_symbols(existing_panel: pd.DataFrame, existing_stock_basic: pd.DataFrame, universe: dict[str, object]) -> list[str]:
    symbols: list[str] = []

    if not existing_panel.empty and "ts_code" in existing_panel.columns:
        for value in existing_panel["ts_code"].dropna().astype(str).tolist():
            if value not in symbols:
                symbols.append(value)

    if not existing_stock_basic.empty and "ts_code" in existing_stock_basic.columns:
        for value in existing_stock_basic["ts_code"].dropna().astype(str).tolist():
            if value not in symbols:
                symbols.append(value)

    for value in universe.get("watch_symbols", []) or []:
        normalized = str(value or "").strip()
        if normalized and normalized not in symbols:
            symbols.append(normalized)

    return sorted(symbols)


def _build_symbol_state(
    existing_panel: pd.DataFrame,
    existing_stock_basic: pd.DataFrame,
    fresh_stock_basic: pd.DataFrame,
    benchmark: str,
) -> pd.DataFrame:
    normalized_existing_stock_basic = _normalize_stock_basic(existing_stock_basic)
    normalized_fresh_stock_basic = _normalize_stock_basic(fresh_stock_basic)

    if existing_panel.empty:
        latest_panel = pd.DataFrame(columns=["ts_code"])
    else:
        latest_panel = (
            existing_panel.sort_values(["ts_code", "trade_date"])
            .drop_duplicates(subset=["ts_code"], keep="last")
            .reset_index(drop=True)
        )

    base = latest_panel.copy()
    if base.empty and not normalized_existing_stock_basic.empty:
        base = normalized_existing_stock_basic.copy()

    if "ts_code" not in base.columns:
        base["ts_code"] = pd.Series(dtype="object")

    if not normalized_existing_stock_basic.empty:
        base = base.merge(
            normalized_existing_stock_basic[["ts_code", "name", "industry", "list_date"]],
            on="ts_code",
            how="outer",
            suffixes=("", "_existing_basic"),
        )
    if not normalized_fresh_stock_basic.empty:
        base = base.merge(
            normalized_fresh_stock_basic[["ts_code", "name", "industry", "list_date"]],
            on="ts_code",
            how="outer",
            suffixes=("", "_fresh_basic"),
        )

    for column in ("name", "industry", "list_date"):
        existing_basic_column = f"{column}_existing_basic"
        fresh_basic_column = f"{column}_fresh_basic"
        if fresh_basic_column in base.columns:
            base[column] = base[column].where(base[column].notna(), base[fresh_basic_column])
        if existing_basic_column in base.columns:
            base[column] = base[column].where(base[column].notna(), base[existing_basic_column])

    for raw_column, adj_column in (
        ("open", "open_adj"),
        ("high", "high_adj"),
        ("low", "low_adj"),
        ("close", "close_adj"),
    ):
        ratio_column = f"{raw_column}_adj_ratio"
        if raw_column in base.columns and adj_column in base.columns:
            raw_values = pd.to_numeric(base[raw_column], errors="coerce")
            adj_values = pd.to_numeric(base[adj_column], errors="coerce")
            ratio = pd.Series(np.nan, index=base.index, dtype="float64")
            valid = raw_values.notna() & adj_values.notna() & (raw_values != 0)
            ratio.loc[valid] = adj_values.loc[valid] / raw_values.loc[valid]
            base[ratio_column] = ratio.fillna(1.0)
        else:
            base[ratio_column] = 1.0

    if "close" in base.columns:
        base["last_close"] = pd.to_numeric(base["close"], errors="coerce")
    else:
        base["last_close"] = np.nan
    if "close_adj" in base.columns:
        base["last_close_adj"] = pd.to_numeric(base["close_adj"], errors="coerce")
    else:
        base["last_close_adj"] = np.nan
    if "adj_factor" in base.columns:
        base["last_adj_factor"] = pd.to_numeric(base["adj_factor"], errors="coerce")
    else:
        base["last_adj_factor"] = 1.0

    if "index_code" not in base.columns:
        base["index_code"] = benchmark
    base["index_code"] = base["index_code"].fillna(benchmark)

    if "is_index_member" not in base.columns:
        base["is_index_member"] = True
    base["is_index_member"] = base["is_index_member"].fillna(True).astype(bool)

    if "is_st" not in base.columns:
        base["is_st"] = False
    if "is_current_name_st" not in base.columns:
        base["is_current_name_st"] = False

    if "list_date" in base.columns:
        base["list_date"] = pd.to_datetime(base["list_date"], errors="coerce")

    keep_columns = [
        "ts_code",
        "name",
        "industry",
        "list_date",
        "index_code",
        "is_index_member",
        "is_st",
        "is_current_name_st",
        "last_close",
        "last_close_adj",
        "last_adj_factor",
        "open_adj_ratio",
        "high_adj_ratio",
        "low_adj_ratio",
        "close_adj_ratio",
    ]
    if "index_weight" in base.columns:
        keep_columns.append("index_weight")

    available_columns = [column for column in keep_columns if column in base.columns]
    return base[available_columns].drop_duplicates(subset=["ts_code"], keep="last").sort_values("ts_code").reset_index(drop=True)


def _append_trade_day_snapshots(
    client: TushareClient,
    trade_dates: list[pd.Timestamp],
) -> tuple[list[pd.Timestamp], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    appended_trade_dates: list[pd.Timestamp] = []
    daily_frames: list[pd.DataFrame] = []
    basic_frames: list[pd.DataFrame] = []
    limit_frames: list[pd.DataFrame] = []

    for trade_date in trade_dates:
        trade_date_text = trade_date.strftime("%Y%m%d")
        daily_frame = _normalize_market_frame(
            client.daily(trade_date=trade_date_text),
            numeric_columns=["open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"],
        )
        if daily_frame.empty:
            logger.info("Skip trade_date {} because Tushare daily endpoint returned no rows yet.", trade_date_text)
            continue

        appended_trade_dates.append(pd.Timestamp(trade_date))
        daily_frames.append(daily_frame)
        basic_frames.append(
            _normalize_market_frame(
                client.daily_basic(trade_date=trade_date_text),
                numeric_columns=["turnover_rate"],
            )
        )
        limit_frames.append(
            _normalize_market_frame(
                client.stk_limit(trade_date=trade_date_text),
                numeric_columns=["up_limit", "down_limit"],
            )
        )

    daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    basic = pd.concat(basic_frames, ignore_index=True) if basic_frames else pd.DataFrame()
    limit = pd.concat(limit_frames, ignore_index=True) if limit_frames else pd.DataFrame()

    if not limit.empty:
        if "up_limit" not in limit.columns and "upper_limit" in limit.columns:
            limit = limit.rename(columns={"upper_limit": "up_limit"})
        if "down_limit" not in limit.columns and "lower_limit" in limit.columns:
            limit = limit.rename(columns={"lower_limit": "down_limit"})

    return appended_trade_dates, daily, basic, limit


def _seeded_previous_close(frame: pd.DataFrame, *, column: str, seed_map: dict[str, float | int | None]) -> pd.Series:
    working = pd.to_numeric(frame[column], errors="coerce")
    seeded = working.groupby(frame["ts_code"]).shift(1)
    first_row_mask = frame.groupby("ts_code").cumcount().eq(0) & seeded.isna()
    seeded.loc[first_row_mask] = frame.loc[first_row_mask, "ts_code"].map(seed_map)
    return seeded


def _build_incremental_rows(
    trade_dates: list[pd.Timestamp],
    symbols: list[str],
    symbol_state: pd.DataFrame,
    daily: pd.DataFrame,
    daily_basic: pd.DataFrame,
    limit: pd.DataFrame,
    target_columns: list[str],
) -> pd.DataFrame:
    base = pd.MultiIndex.from_product([trade_dates, symbols], names=["trade_date", "ts_code"]).to_frame(index=False)
    working = base.merge(symbol_state, on="ts_code", how="left")

    if not daily.empty:
        daily = daily.loc[daily["ts_code"].isin(symbols)].copy()
        working = working.merge(
            daily[["trade_date", "ts_code", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"]],
            on=["trade_date", "ts_code"],
            how="left",
        )
    if not daily_basic.empty and "turnover_rate" in target_columns:
        daily_basic = daily_basic.loc[daily_basic["ts_code"].isin(symbols)].copy()
        keep_columns = [column for column in ("trade_date", "ts_code", "turnover_rate") if column in daily_basic.columns]
        if keep_columns:
            working = working.merge(daily_basic[keep_columns], on=["trade_date", "ts_code"], how="left")
    if not limit.empty:
        limit = limit.loc[limit["ts_code"].isin(symbols)].copy()
        keep_columns = [column for column in ("trade_date", "ts_code", "up_limit", "down_limit") if column in limit.columns]
        if keep_columns:
            working = working.merge(limit[keep_columns], on=["trade_date", "ts_code"], how="left")

    working = working.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

    name_series = working["name"].fillna(working["ts_code"])
    working["is_current_name_st"] = name_series.astype(str).str.contains("ST", case=False, na=False)
    working["is_st"] = working["is_st"].where(working["is_st"].notna(), working["is_current_name_st"]).astype(bool)
    working["is_suspend"] = working["close"].isna()
    working["up_limit"] = pd.to_numeric(working["up_limit"], errors="coerce") if "up_limit" in working.columns else np.nan
    working["down_limit"] = pd.to_numeric(working["down_limit"], errors="coerce") if "down_limit" in working.columns else np.nan

    close_seed_map = symbol_state.set_index("ts_code")["last_close"].to_dict()
    close_adj_seed_map = symbol_state.set_index("ts_code")["last_close_adj"].to_dict()
    adj_factor_seed_map = symbol_state.set_index("ts_code")["last_adj_factor"].to_dict()

    working["pre_close"] = pd.to_numeric(working.get("pre_close"), errors="coerce")
    missing_pre_close = working["pre_close"].isna()
    working.loc[missing_pre_close, "pre_close"] = _seeded_previous_close(
        working.loc[:, ["ts_code", "trade_date", "close"]],
        column="close",
        seed_map=close_seed_map,
    ).loc[missing_pre_close]

    working["adj_factor"] = working["ts_code"].map(adj_factor_seed_map).fillna(1.0)

    close_ratio_map = symbol_state.set_index("ts_code")["close_adj_ratio"].to_dict()
    for raw_column, adj_column in (
        ("open", "open_adj"),
        ("high", "high_adj"),
        ("low", "low_adj"),
        ("close", "close_adj"),
    ):
        ratio_column = f"{raw_column}_adj_ratio"
        ratio_map = symbol_state.set_index("ts_code")[ratio_column].to_dict() if ratio_column in symbol_state.columns else close_ratio_map
        ratio_series = working["ts_code"].map(ratio_map)
        fallback_ratio_series = working["ts_code"].map(close_ratio_map)
        ratio_series = ratio_series.fillna(fallback_ratio_series).fillna(1.0)
        working[adj_column] = pd.to_numeric(working.get(raw_column), errors="coerce") * ratio_series

    working["pre_close_adj"] = _seeded_previous_close(
        working.loc[:, ["ts_code", "trade_date", "close_adj"]],
        column="close_adj",
        seed_map=close_adj_seed_map,
    )

    working["is_limit_up_close"] = (
        working["close"].notna()
        & working["up_limit"].notna()
        & (pd.to_numeric(working["close"], errors="coerce") >= working["up_limit"] - 1e-6)
    )
    working["is_limit_down_close"] = (
        working["close"].notna()
        & working["down_limit"].notna()
        & (pd.to_numeric(working["close"], errors="coerce") <= working["down_limit"] + 1e-6)
    )
    one_word_board = (
        working["open"].notna()
        & working["high"].notna()
        & working["low"].notna()
        & working["close"].notna()
        & (working["open"] == working["high"])
        & (working["high"] == working["low"])
        & (working["low"] == working["close"])
    )
    working["is_buy_locked"] = (one_word_board & working["is_limit_up_close"]).astype(bool)
    working["is_sell_locked"] = (one_word_board & working["is_limit_down_close"]).astype(bool)

    for column in target_columns:
        if column not in working.columns:
            working[column] = np.nan

    return working[target_columns].sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def sync_incremental_daily_bar(
    *,
    root: Path | None = None,
    target_source: str | None = None,
    end_date: str | None = None,
    write_canonical: bool | None = None,
) -> IncrementalSyncSummary:
    resolved_root = root or project_root()
    resolved_target_source = normalize_data_source(target_source or active_data_source())
    resolved_end_date = _normalize_ts_date(end_date)
    resolved_write_canonical = bool(
        active_data_source() == resolved_target_source if write_canonical is None else write_canonical
    )

    existing_panel, existing_stock_basic, existing_trade_calendar, daily_bar_path, stock_basic_path, trade_calendar_path = _load_existing_inputs(
        resolved_root,
        resolved_target_source,
    )
    if existing_panel.empty:
        raise FileNotFoundError(
            f"Missing existing daily bar file for {resolved_target_source}: {daily_bar_path}. "
            "Bootstrap the panel first, then run incremental sync."
        )

    for column in REQUIRED_PANEL_COLUMNS:
        if column not in existing_panel.columns:
            existing_panel[column] = np.nan
    existing_panel = existing_panel.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

    last_trade_date = pd.to_datetime(existing_panel["trade_date"], errors="coerce").max()
    if pd.isna(last_trade_date):
        raise RuntimeError("Existing daily bar file does not contain a valid latest trade_date.")

    universe_config_path = resolved_root / "config" / "universe.yaml"
    if universe_config_path.exists():
        universe = load_universe(universe_config_path)
    else:
        universe = {"benchmark": "000905.SH", "watch_symbols": []}
    benchmark = str(universe.get("benchmark", "000905.SH") or "000905.SH")
    symbols = _tracked_symbols(existing_panel, existing_stock_basic, universe)
    if not symbols:
        raise RuntimeError("No symbols resolved from the existing panel or current universe configuration.")

    client = TushareClient()
    full_trade_calendar = _normalize_trade_calendar(
        client.trade_cal(start_date=last_trade_date.strftime("%Y%m%d"), end_date=resolved_end_date, exchange="SSE")
    )
    missing_trade_dates = full_trade_calendar.loc[full_trade_calendar["trade_date"] > last_trade_date, "trade_date"].tolist()

    fresh_stock_basic = _normalize_stock_basic(client.stock_basic())
    fresh_stock_basic = fresh_stock_basic.loc[fresh_stock_basic["ts_code"].isin(symbols)].reset_index(drop=True)
    symbol_state = _build_symbol_state(existing_panel, existing_stock_basic, fresh_stock_basic, benchmark)
    symbol_state = symbol_state.loc[symbol_state["ts_code"].isin(symbols)].reset_index(drop=True)

    target_columns = existing_panel.columns.tolist()

    appended_rows = pd.DataFrame(columns=target_columns)
    appended_trade_dates: list[pd.Timestamp] = []
    if missing_trade_dates:
        appended_trade_dates, daily, daily_basic, limit = _append_trade_day_snapshots(client, missing_trade_dates)
        if appended_trade_dates:
            appended_rows = _build_incremental_rows(
                trade_dates=appended_trade_dates,
                symbols=symbols,
                symbol_state=symbol_state,
                daily=daily,
                daily_basic=daily_basic,
                limit=limit,
                target_columns=target_columns,
            )

    combined_panel = (
        pd.concat([existing_panel, appended_rows], ignore_index=True, sort=False)
        .drop_duplicates(subset=["trade_date", "ts_code"], keep="first")
        .sort_values(["trade_date", "ts_code"])
        .reset_index(drop=True)
    )

    combined_trade_calendar = (
        pd.concat([existing_trade_calendar, full_trade_calendar], ignore_index=True, sort=False)
        if not existing_trade_calendar.empty
        else full_trade_calendar.copy()
    )
    combined_trade_calendar = _normalize_trade_calendar(combined_trade_calendar)

    combined_stock_basic = (
        pd.concat([fresh_stock_basic, _normalize_stock_basic(existing_stock_basic)], ignore_index=True, sort=False)
        if not fresh_stock_basic.empty or not existing_stock_basic.empty
        else pd.DataFrame(columns=["ts_code", "name", "industry", "list_date"])
    )
    combined_stock_basic = _normalize_stock_basic(combined_stock_basic)

    saved_daily_bar_artifact_ref = save_binary_dataset(
        resolved_root,
        data_source=resolved_target_source,
        directory="data/staging",
        filename="daily_bar.parquet",
        artifact_name="daily_bar",
        frame=combined_panel,
        write_canonical=resolved_write_canonical,
    )
    saved_trade_calendar_artifact_ref = save_binary_dataset(
        resolved_root,
        data_source=resolved_target_source,
        directory="data/staging",
        filename="trade_calendar.parquet",
        artifact_name="trade_calendar",
        frame=combined_trade_calendar,
        write_canonical=resolved_write_canonical,
    )
    saved_stock_basic_artifact_ref = save_binary_dataset(
        resolved_root,
        data_source=resolved_target_source,
        directory="data/staging",
        filename="stock_basic.parquet",
        artifact_name="stock_basic",
        frame=combined_stock_basic,
        write_canonical=resolved_write_canonical,
    )

    latest_trade_date = pd.to_datetime(combined_panel["trade_date"], errors="coerce").max()
    return IncrementalSyncSummary(
        target_source=resolved_target_source,
        previous_latest_trade_date=last_trade_date.strftime("%Y-%m-%d"),
        latest_trade_date=None if pd.isna(latest_trade_date) else latest_trade_date.strftime("%Y-%m-%d"),
        appended_trade_dates=len(appended_trade_dates),
        appended_rows=int(len(appended_rows)),
        symbols=len(symbols),
        daily_bar_artifact_ref=str(saved_daily_bar_artifact_ref),
        trade_calendar_artifact_ref=str(saved_trade_calendar_artifact_ref),
        stock_basic_artifact_ref=str(saved_stock_basic_artifact_ref),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append missing daily-bar trade dates with Tushare without rewriting old rows.")
    parser.add_argument(
        "--target-source",
        default=None,
        help="Existing local data source to extend (defaults to config/universe.yaml data_source).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Sync through this date. Accepts YYYYMMDD or YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--write-canonical",
        action="store_true",
        help="Also overwrite canonical files like data/staging/daily_bar.parquet.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    summary = sync_incremental_daily_bar(
        target_source=args.target_source,
        end_date=args.end_date,
        write_canonical=args.write_canonical or None,
    )
    logger.info(
        "Tushare incremental sync finished: source={}, previous_latest={}, latest={}, appended_trade_dates={}, appended_rows={}, symbols={}",
        summary.target_source,
        summary.previous_latest_trade_date,
        summary.latest_trade_date,
        summary.appended_trade_dates,
        summary.appended_rows,
        summary.symbols,
    )

    from src.db.dashboard_sync import sync_dataset_summary_artifact, sync_watchlist_snapshot_artifact

    dataset_summary = sync_dataset_summary_artifact(data_source=summary.target_source)
    watchlist_summary = sync_watchlist_snapshot_artifact(data_source=summary.target_source)
    logger.info(dataset_summary.message)
    logger.info(watchlist_summary.message)
