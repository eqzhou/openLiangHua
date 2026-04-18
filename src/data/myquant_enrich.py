from __future__ import annotations

import argparse

import akshare as ak
import numpy as np
import pandas as pd

from src.app.repositories.report_repository import save_binary_dataset, save_json_report
from src.data.myquant_panel import drop_trailing_empty_price_dates
from src.utils.data_source import active_data_source, source_or_canonical_path
from src.utils.io import ensure_dir, project_root
from src.utils.logger import configure_logging

logger = configure_logging()


def _normalize_existing_panel_columns(panel: pd.DataFrame) -> pd.DataFrame:
    normalized = panel.copy()
    if "ts_code" not in normalized.columns:
        if "ts_code_x" in normalized.columns:
            normalized = normalized.rename(columns={"ts_code_x": "ts_code"})
        elif "ts_code_y" in normalized.columns:
            normalized = normalized.rename(columns={"ts_code_y": "ts_code"})

    drop_columns = [
        column
        for column in ("ts_code_y", "start_date", "update_time", "industry_sw_code", "industry_sw", "industry_current", "industry_source")
        if column in normalized.columns
    ]
    if drop_columns:
        normalized = normalized.drop(columns=drop_columns)
    return normalized


def _clean_price_limits(panel: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int | float]]:
    cleaned = panel.copy()
    active_mask = ~cleaned["is_suspend"].fillna(False)

    zero_limit_rows = int(
        ((cleaned["up_limit"].fillna(0) <= 0) | (cleaned["down_limit"].fillna(0) <= 0)).sum()
    )
    cleaned["up_limit"] = pd.to_numeric(cleaned["up_limit"], errors="coerce")
    cleaned["down_limit"] = pd.to_numeric(cleaned["down_limit"], errors="coerce")
    cleaned.loc[cleaned["up_limit"] <= 0, "up_limit"] = np.nan
    cleaned.loc[cleaned["down_limit"] <= 0, "down_limit"] = np.nan

    cleaned["pct_chg"] = np.where(
        cleaned["pre_close"].notna() & (cleaned["pre_close"] > 0) & cleaned["close"].notna(),
        cleaned["close"] / cleaned["pre_close"] - 1.0,
        np.nan,
    )
    cleaned["pre_close_adj"] = np.where(
        cleaned["pre_close"].notna() & cleaned["adj_factor"].notna(),
        cleaned["pre_close"] * cleaned["adj_factor"] / cleaned.groupby("ts_code")["adj_factor"].transform("last"),
        cleaned["pre_close_adj"],
    )
    cleaned["is_limit_up_close"] = (
        active_mask
        & cleaned["up_limit"].notna()
        & cleaned["close"].notna()
        & (cleaned["close"] >= cleaned["up_limit"] - 1e-6)
    )
    cleaned["is_limit_down_close"] = (
        active_mask
        & cleaned["down_limit"].notna()
        & cleaned["close"].notna()
        & (cleaned["close"] <= cleaned["down_limit"] + 1e-6)
    )
    one_word_board = (
        active_mask
        & cleaned["open"].notna()
        & cleaned["high"].notna()
        & cleaned["low"].notna()
        & cleaned["close"].notna()
        & (cleaned["open"] == cleaned["high"])
        & (cleaned["high"] == cleaned["low"])
        & (cleaned["low"] == cleaned["close"])
    )
    cleaned["is_buy_locked"] = (one_word_board & cleaned["is_limit_up_close"]).astype(bool)
    cleaned["is_sell_locked"] = (one_word_board & cleaned["is_limit_down_close"]).astype(bool)

    report = {
        "rows_with_non_positive_limits_before_clean": zero_limit_rows,
        "close_above_up_limit_after_clean": int(
            (active_mask & cleaned["up_limit"].notna() & (cleaned["close"] > cleaned["up_limit"] + 1e-6)).sum()
        ),
        "close_below_down_limit_after_clean": int(
            (active_mask & cleaned["down_limit"].notna() & (cleaned["close"] < cleaned["down_limit"] - 1e-6)).sum()
        ),
        "buy_locked_true": int(cleaned["is_buy_locked"].sum()),
        "sell_locked_true": int(cleaned["is_sell_locked"].sum()),
    }
    return cleaned, report


def _build_symbol_lookup(panel: pd.DataFrame) -> pd.DataFrame:
    lookup = panel[["ts_code"]].drop_duplicates().copy()
    lookup["symbol"] = lookup["ts_code"].str.split(".").str[0]
    return lookup


def _load_sw_industry_history(symbol_lookup: pd.DataFrame) -> pd.DataFrame:
    history = ak.stock_industry_clf_hist_sw()
    history["symbol"] = history["symbol"].astype(str).str.zfill(6)
    history["start_date"] = pd.to_datetime(history["start_date"], errors="coerce")
    history["update_time"] = pd.to_datetime(history["update_time"], errors="coerce")
    history["industry_code"] = history["industry_code"].astype(str).str.strip()
    history = history.merge(symbol_lookup, on="symbol", how="inner")
    history = (
        history.sort_values(["ts_code", "start_date", "update_time"])
        .drop_duplicates(subset=["ts_code", "start_date"], keep="last")
        .reset_index(drop=True)
    )
    history["industry_sw_code"] = history["industry_code"]
    history["industry_sw"] = "SW_" + history["industry_sw_code"]
    return history[["ts_code", "start_date", "update_time", "industry_sw_code", "industry_sw"]]


def _merge_sw_history(panel: pd.DataFrame, sw_history: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    if sw_history.empty:
        return panel.copy(), {"sw_history_rows": 0, "sw_history_symbols": 0, "sw_mapped_rows": 0}

    working = panel.copy().sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    sw_sorted = sw_history.sort_values(["ts_code", "start_date"]).reset_index(drop=True)
    merged_parts: list[pd.DataFrame] = []

    for ts_code, part in working.groupby("ts_code", sort=False):
        history_part = sw_sorted.loc[sw_sorted["ts_code"] == ts_code].copy()
        symbol_part = part.sort_values("trade_date").copy()
        if history_part.empty:
            symbol_part["start_date"] = pd.NaT
            symbol_part["update_time"] = pd.NaT
            symbol_part["industry_sw_code"] = pd.NA
            symbol_part["industry_sw"] = pd.NA
            merged_parts.append(symbol_part)
            continue

        history_part = history_part[["start_date", "update_time", "industry_sw_code", "industry_sw"]].copy()
        merged_parts.append(
            pd.merge_asof(
                symbol_part,
                history_part.sort_values("start_date"),
                left_on="trade_date",
                right_on="start_date",
                direction="backward",
                allow_exact_matches=True,
            )
        )

    merged = pd.concat(merged_parts, ignore_index=True)
    merged["industry_current"] = merged["industry"]
    merged["industry"] = merged["industry_sw"].where(merged["industry_sw"].notna(), merged["industry"])
    merged["industry_source"] = np.where(merged["industry_sw"].notna(), "sw_history", "snapshot_fallback")

    report = {
        "sw_history_rows": int(len(sw_history)),
        "sw_history_symbols": int(sw_history["ts_code"].nunique()),
        "sw_mapped_rows": int(merged["industry_sw"].notna().sum()),
    }
    return merged, report


def run(write_canonical: bool = False) -> None:
    root = project_root()
    staging_dir = ensure_dir(root / "data" / "staging")
    myquant_panel_path = staging_dir / "myquant_daily_bar.parquet"
    if not myquant_panel_path.exists():
        raise FileNotFoundError("Missing data/staging/myquant_daily_bar.parquet. Run the MyQuant downloader first.")

    panel = pd.read_parquet(myquant_panel_path)
    panel = _normalize_existing_panel_columns(panel)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"])
    if "list_date" in panel.columns:
        panel["list_date"] = pd.to_datetime(panel["list_date"], errors="coerce")
    panel, trailing_empty_dates = drop_trailing_empty_price_dates(panel)
    if trailing_empty_dates:
        logger.warning(
            "Dropped trailing empty-price dates from existing MyQuant panel: {}",
            ", ".join(str(date.date()) for date in trailing_empty_dates),
        )

    cleaned_panel, limit_report = _clean_price_limits(panel)
    sw_history = _load_sw_industry_history(_build_symbol_lookup(cleaned_panel))
    save_binary_dataset(
        root,
        data_source="myquant",
        directory="data/staging",
        filename="sw_industry_history.parquet",
        artifact_name="sw_industry_history",
        frame=sw_history,
        write_canonical=False,
    )

    enriched_panel, industry_report = _merge_sw_history(cleaned_panel, sw_history)
    save_binary_dataset(
        root,
        data_source="myquant",
        directory="data/staging",
        filename="daily_bar.parquet",
        artifact_name="daily_bar",
        frame=enriched_panel,
        write_canonical=write_canonical or active_data_source() == "myquant",
    )

    report = {
        "rows": int(len(enriched_panel)),
        "symbols": int(enriched_panel["ts_code"].nunique()),
        "date_min": str(enriched_panel["trade_date"].min().date()),
        "date_max": str(enriched_panel["trade_date"].max().date()),
        **limit_report,
        **industry_report,
        "trimmed_trailing_empty_trade_dates": [str(date.date()) for date in trailing_empty_dates],
        "industry_non_null_rows": int(enriched_panel["industry"].notna().sum()),
        "industry_source_sw_history_rows": int((enriched_panel["industry_source"] == "sw_history").sum()),
    }
    report_path = save_json_report(
        root,
        data_source="myquant",
        filename="myquant_data_quality.json",
        payload=report,
        artifact_name="myquant_data_quality",
    )
    logger.info(f"Saved MyQuant enrichment outputs to {staging_dir}")
    logger.info(report)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post-process MyQuant daily data with cleaner limits and historical SW industry.")
    parser.add_argument(
        "--write-canonical",
        action="store_true",
        help="Also overwrite data/staging/daily_bar.parquet when the enriched panel is ready.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(write_canonical=args.write_canonical)
    from src.db.dashboard_sync import sync_dataset_summary_artifact, sync_watchlist_snapshot_artifact

    dataset_summary = sync_dataset_summary_artifact(data_source="myquant")
    watchlist_summary = sync_watchlist_snapshot_artifact(data_source="myquant")
    logger.info(dataset_summary.message)
    logger.info(watchlist_summary.message)
