from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from pprint import pformat
from typing import Any

import numpy as np
import pandas as pd

from src.app.repositories.config_repository import load_universe_config
from src.app.repositories.postgres_watchlist_store import PostgresWatchlistStore
from src.app.repositories.report_repository import (
    load_daily_bar,
    load_stock_basic,
    load_trade_calendar,
    save_binary_dataset,
)
from src.data.tushare_client import TushareClient
from src.db.dashboard_sync import sync_watchlist_snapshot_artifact
from src.features.build_feature_panel import build_feature_label_artifacts
from src.utils.data_source import normalize_data_source
from src.utils.io import project_root
from src.utils.logger import configure_logging
from src.web_api.settings import get_api_settings

logger = configure_logging()

DEFAULT_USER_ID = "bootstrap-admin"
DEFAULT_BENCHMARK = "000905.SH"


@dataclass(frozen=True)
class ResearchPoolSummary:
    ok: bool
    data_source: str
    user_id: str
    latest_trade_date: str | None
    selected_count: int
    watchlist_written: int
    daily_bar_rows: int
    daily_bar_symbols: int
    feature_rows: int
    watchlist_snapshot_ok: bool
    message: str
    selected_symbols: list[str]


def _normalize_ts_date(value: str | None) -> str:
    if not value:
        return datetime.now().strftime("%Y%m%d")
    normalized = str(value).strip()
    if len(normalized) == 8 and normalized.isdigit():
        return normalized
    return pd.Timestamp(normalized).strftime("%Y%m%d")


def _normalize_trade_calendar(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["trade_date"])
    working = frame.copy()
    if "cal_date" in working.columns and "trade_date" not in working.columns:
        working = working.rename(columns={"cal_date": "trade_date"})
    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    if "is_open" in working.columns:
        working = working.loc[working["is_open"].astype(str).isin({"1", "true", "True"})].copy()
    return (
        working[["trade_date"]]
        .dropna(subset=["trade_date"])
        .drop_duplicates(subset=["trade_date"], keep="first")
        .sort_values("trade_date")
        .reset_index(drop=True)
    )


def _normalize_daily_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    working = frame.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    for column in ("open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"):
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")
    return working.dropna(subset=["trade_date", "ts_code"]).reset_index(drop=True)


def _normalize_stock_basic(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["ts_code", "name", "industry", "list_date", "list_status"])
    working = frame.copy()
    for column in ("ts_code", "name", "industry", "list_date", "list_status"):
        if column not in working.columns:
            working[column] = pd.NA
    list_date_text = working["list_date"].astype(str).str.strip()
    compact_date_mask = list_date_text.str.fullmatch(r"\d{8}", na=False)
    working["list_date"] = pd.to_datetime(list_date_text, errors="coerce")
    working.loc[compact_date_mask, "list_date"] = pd.to_datetime(
        list_date_text.loc[compact_date_mask],
        format="%Y%m%d",
        errors="coerce",
    )
    return (
        working[["ts_code", "name", "industry", "list_date", "list_status"]]
        .dropna(subset=["ts_code"])
        .drop_duplicates(subset=["ts_code"], keep="first")
        .reset_index(drop=True)
    )


def fetch_research_pool_inputs(
    *,
    client: TushareClient | None = None,
    end_date: str | None = None,
    lookback_trade_days: int = 121,
    calendar_lookback_days: int = 260,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    resolved_client = client or TushareClient()
    end_ts = pd.Timestamp(_normalize_ts_date(end_date))
    start_ts = end_ts - pd.Timedelta(days=calendar_lookback_days)
    calendar = _normalize_trade_calendar(
        resolved_client.trade_cal(
            start_date=start_ts.strftime("%Y%m%d"),
            end_date=end_ts.strftime("%Y%m%d"),
            exchange="SSE",
        )
    )
    if calendar.empty:
        raise RuntimeError("Tushare trade calendar returned no open trade dates.")

    open_dates = calendar["trade_date"].tolist()
    latest_trade_date: pd.Timestamp | None = None
    latest_daily = pd.DataFrame()
    latest_index = -1
    for index in range(len(open_dates) - 1, -1, -1):
        trade_date = pd.Timestamp(open_dates[index])
        daily = _normalize_daily_frame(resolved_client.daily(trade_date=trade_date.strftime("%Y%m%d")))
        if daily.empty:
            logger.info("Skip research pool trade_date {} because daily endpoint returned no rows.", trade_date.strftime("%Y%m%d"))
            continue
        latest_trade_date = trade_date
        latest_daily = daily
        latest_index = index
        break

    if latest_trade_date is None:
        raise RuntimeError("Tushare daily endpoint returned no rows for recent open trade dates.")

    selected_dates = open_dates[max(0, latest_index - lookback_trade_days + 1) : latest_index + 1]
    daily_frames: list[pd.DataFrame] = []
    latest_text = latest_trade_date.strftime("%Y%m%d")
    for trade_date in selected_dates:
        trade_date = pd.Timestamp(trade_date)
        if trade_date == latest_trade_date:
            daily = latest_daily
        else:
            daily = _normalize_daily_frame(resolved_client.daily(trade_date=trade_date.strftime("%Y%m%d")))
        if not daily.empty:
            daily_frames.append(daily)

    daily_history = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    stock_basic = _normalize_stock_basic(resolved_client.stock_basic())
    trade_calendar = calendar.loc[calendar["trade_date"].isin(selected_dates)].reset_index(drop=True)
    return daily_history, stock_basic, trade_calendar, latest_text


def _factor_snapshot(group: pd.DataFrame, latest_trade_date: pd.Timestamp) -> dict[str, Any]:
    working = group.sort_values("trade_date").copy()
    close = pd.to_numeric(working["close"], errors="coerce")
    amount = pd.to_numeric(working.get("amount"), errors="coerce")
    returns = close.pct_change()
    latest_close = close.iloc[-1]
    close_20 = close.iloc[-20] if len(close) >= 20 else close.iloc[0]
    close_60 = close.iloc[-60] if len(close) >= 60 else close.iloc[0]
    window_60 = close.tail(60)
    rolling_high = window_60.max()
    drawdown_60 = latest_close / rolling_high - 1.0 if pd.notna(rolling_high) and rolling_high else np.nan
    return {
        "ts_code": str(working.iloc[-1]["ts_code"]),
        "latest_trade_date": latest_trade_date,
        "latest_close": latest_close,
        "history_days": int(len(working)),
        "mom_20": latest_close / close_20 - 1.0 if pd.notna(close_20) and close_20 else np.nan,
        "mom_60": latest_close / close_60 - 1.0 if pd.notna(close_60) and close_60 else np.nan,
        "amount_20": amount.tail(20).mean(),
        "vol_20": returns.tail(20).std(),
        "drawdown_60": drawdown_60,
    }


def _zscore(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    std = values.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(0.0, index=series.index)
    return (values - values.mean()) / std


def select_research_pool(
    daily_history: pd.DataFrame,
    stock_basic: pd.DataFrame,
    *,
    limit: int = 50,
    min_list_days: int = 250,
    min_history_days: int = 60,
    liquidity_quantile: float = 0.5,
    industry_cap: int = 5,
) -> pd.DataFrame:
    if daily_history.empty:
        raise RuntimeError("daily_history is empty; cannot select research pool.")
    normalized_daily = _normalize_daily_frame(daily_history)
    normalized_stock_basic = _normalize_stock_basic(stock_basic)
    latest_trade_date = pd.to_datetime(normalized_daily["trade_date"], errors="coerce").max()
    if pd.isna(latest_trade_date):
        raise RuntimeError("daily_history has no valid trade_date.")

    snapshots = pd.DataFrame(
        [_factor_snapshot(group, pd.Timestamp(latest_trade_date)) for _, group in normalized_daily.groupby("ts_code")]
    )
    enriched = snapshots.merge(normalized_stock_basic, on="ts_code", how="left")
    enriched["name"] = enriched["name"].fillna(enriched["ts_code"])
    enriched["industry"] = enriched["industry"].fillna("未分类")
    enriched["list_days"] = (pd.Timestamp(latest_trade_date) - pd.to_datetime(enriched["list_date"], errors="coerce")).dt.days
    name_text = enriched["name"].astype(str)
    eligible = enriched.loc[
        (enriched["list_status"].fillna("L").astype(str) == "L")
        & ~name_text.str.contains("ST", case=False, na=False)
        & (pd.to_numeric(enriched["list_days"], errors="coerce") >= min_list_days)
        & (pd.to_numeric(enriched["history_days"], errors="coerce") >= min_history_days)
    ].copy()
    if eligible.empty:
        raise RuntimeError("No eligible stocks after status, ST, listing-days and history filters.")

    amount_threshold = pd.to_numeric(eligible["amount_20"], errors="coerce").quantile(liquidity_quantile)
    eligible = eligible.loc[pd.to_numeric(eligible["amount_20"], errors="coerce") >= amount_threshold].copy()
    if eligible.empty:
        raise RuntimeError("No eligible stocks after liquidity filter.")

    eligible["research_score"] = (
        0.35 * _zscore(eligible["mom_20"])
        + 0.25 * _zscore(eligible["mom_60"])
        + 0.20 * _zscore(eligible["amount_20"])
        + 0.10 * _zscore(eligible["drawdown_60"])
        - 0.10 * _zscore(eligible["vol_20"])
    )
    eligible = eligible.sort_values(
        ["research_score", "amount_20", "mom_20"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    selected_rows: list[dict[str, Any]] = []
    industry_counts: dict[str, int] = {}
    for row in eligible.to_dict(orient="records"):
        industry = str(row.get("industry") or "未分类")
        if industry_counts.get(industry, 0) >= industry_cap:
            continue
        selected_rows.append(row)
        industry_counts[industry] = industry_counts.get(industry, 0) + 1
        if len(selected_rows) >= limit:
            break

    if len(selected_rows) < limit:
        selected_symbols = {str(row["ts_code"]) for row in selected_rows}
        for row in eligible.to_dict(orient="records"):
            if str(row["ts_code"]) in selected_symbols:
                continue
            selected_rows.append(row)
            if len(selected_rows) >= limit:
                break

    selected = pd.DataFrame(selected_rows).head(limit).copy()
    selected["research_rank"] = range(1, len(selected) + 1)
    columns = [
        "research_rank",
        "ts_code",
        "name",
        "industry",
        "list_date",
        "latest_trade_date",
        "latest_close",
        "mom_20",
        "mom_60",
        "drawdown_60",
        "vol_20",
        "amount_20",
        "research_score",
        "history_days",
    ]
    return selected[[column for column in columns if column in selected.columns]].reset_index(drop=True)


def prepare_selected_daily_bar(
    daily_history: pd.DataFrame,
    selected: pd.DataFrame,
    *,
    benchmark: str,
) -> pd.DataFrame:
    symbols = set(selected["ts_code"].astype(str).tolist())
    working = _normalize_daily_frame(daily_history)
    working = working.loc[working["ts_code"].astype(str).isin(symbols)].copy()
    metadata = selected[["ts_code", "name", "industry", "list_date"]].drop_duplicates(subset=["ts_code"], keep="first")
    working = working.merge(metadata, on="ts_code", how="left")
    working["list_date"] = pd.to_datetime(working["list_date"], errors="coerce")
    working["index_code"] = benchmark
    working["is_index_member"] = False
    working["is_current_name_st"] = working["name"].astype(str).str.contains("ST", case=False, na=False)
    working["is_st"] = working["is_current_name_st"]
    working["is_suspend"] = working["close"].isna()
    working["is_limit_up_close"] = False
    working["is_limit_down_close"] = False
    working["is_buy_locked"] = False
    working["is_sell_locked"] = False
    working["up_limit"] = pd.NA
    working["down_limit"] = pd.NA
    working["adj_factor"] = 1.0
    for raw_column, adj_column in (
        ("open", "open_adj"),
        ("high", "high_adj"),
        ("low", "low_adj"),
        ("close", "close_adj"),
    ):
        working[adj_column] = pd.to_numeric(working.get(raw_column), errors="coerce")
    if "pre_close" not in working.columns:
        working["pre_close"] = working.groupby("ts_code")["close"].shift(1)
    if "pct_chg" not in working.columns:
        working["pct_chg"] = working.groupby("ts_code")["close"].pct_change() * 100.0
    working["pre_close_adj"] = working.groupby("ts_code")["close_adj"].shift(1)

    required_columns = [
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
    for column in required_columns:
        if column not in working.columns:
            working[column] = pd.NA
    return working[required_columns].sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def _merge_daily_bar(existing_daily_bar: pd.DataFrame, selected_daily_bar: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing_daily_bar, selected_daily_bar], ignore_index=True, sort=False)
    if combined.empty:
        return combined
    combined["trade_date"] = pd.to_datetime(combined["trade_date"], errors="coerce")
    return (
        combined.dropna(subset=["trade_date", "ts_code"])
        .drop_duplicates(subset=["trade_date", "ts_code"], keep="last")
        .sort_values(["trade_date", "ts_code"])
        .reset_index(drop=True)
    )


def _merge_trade_calendar(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing, fetched], ignore_index=True, sort=False)
    return _normalize_trade_calendar(combined)


def _merge_stock_basic(existing: pd.DataFrame, selected: pd.DataFrame) -> pd.DataFrame:
    selected_basic = _normalize_stock_basic(selected)
    combined = pd.concat([existing, selected_basic], ignore_index=True, sort=False)
    return _normalize_stock_basic(combined).drop_duplicates(subset=["ts_code"], keep="last").reset_index(drop=True)


def write_research_pool_to_watchlist(
    selected: pd.DataFrame,
    *,
    user_id: str,
    store: PostgresWatchlistStore,
    replace_focus: bool = True,
) -> int:
    if replace_focus:
        store.clear_items(user_id, "focus")

    written = 0
    for row in selected.sort_values("research_rank").to_dict(orient="records"):
        rank = int(row.get("research_rank", written + 1) or written + 1)
        score = float(row.get("research_score", 0.0) or 0.0)
        note = (
            f"自动研究池#{rank} | 行业={row.get('industry', '')} | "
            f"score={score:.4f} | mom20={float(row.get('mom_20', 0.0) or 0.0):.2%} | "
            f"drawdown60={float(row.get('drawdown_60', 0.0) or 0.0):.2%}"
        )
        store.add_item(
            user_id,
            str(row["ts_code"]),
            str(row.get("name") or row["ts_code"]),
            "focus",
            note=note,
        )
        written += 1
    return written


def run_research_pool_refresh(
    *,
    root: Path | None = None,
    data_source: str = "tushare",
    user_id: str = DEFAULT_USER_ID,
    end_date: str | None = None,
    limit: int = 50,
    replace_focus: bool = True,
    rebuild_features: bool = True,
) -> ResearchPoolSummary:
    resolved_root = root or project_root()
    resolved_data_source = normalize_data_source(data_source)
    universe = load_universe_config(resolved_root, prefer_database=True)
    benchmark = str(universe.get("benchmark", DEFAULT_BENCHMARK) or DEFAULT_BENCHMARK)
    daily_history, stock_basic, trade_calendar, latest_trade_date = fetch_research_pool_inputs(end_date=end_date)
    selected = select_research_pool(daily_history, stock_basic, limit=limit)
    selected_daily_bar = prepare_selected_daily_bar(daily_history, selected, benchmark=benchmark)

    existing_daily_bar = load_daily_bar(resolved_root, data_source=resolved_data_source, prefer_database=True)
    existing_trade_calendar = load_trade_calendar(resolved_root, data_source=resolved_data_source, prefer_database=True)
    existing_stock_basic = load_stock_basic(resolved_root, data_source=resolved_data_source, prefer_database=True)
    merged_daily_bar = _merge_daily_bar(existing_daily_bar, selected_daily_bar)
    merged_trade_calendar = _merge_trade_calendar(existing_trade_calendar, trade_calendar)
    merged_stock_basic = _merge_stock_basic(existing_stock_basic, selected)

    save_binary_dataset(
        resolved_root,
        data_source=resolved_data_source,
        directory="data/staging",
        filename="daily_bar.parquet",
        artifact_name="daily_bar",
        frame=merged_daily_bar,
        write_canonical=True,
    )
    save_binary_dataset(
        resolved_root,
        data_source=resolved_data_source,
        directory="data/staging",
        filename="trade_calendar.parquet",
        artifact_name="trade_calendar",
        frame=merged_trade_calendar,
        write_canonical=True,
    )
    save_binary_dataset(
        resolved_root,
        data_source=resolved_data_source,
        directory="data/staging",
        filename="stock_basic.parquet",
        artifact_name="stock_basic",
        frame=merged_stock_basic,
        write_canonical=True,
    )

    store = PostgresWatchlistStore(get_api_settings())
    watchlist_written = write_research_pool_to_watchlist(
        selected,
        user_id=user_id,
        store=store,
        replace_focus=replace_focus,
    )

    feature_summary: dict[str, Any] = {}
    if rebuild_features:
        feature_summary = build_feature_label_artifacts(
            root=resolved_root,
            data_source=resolved_data_source,
            market_universe_user_id=user_id,
        )

    watchlist_summary = sync_watchlist_snapshot_artifact(
        root=resolved_root,
        data_source=resolved_data_source,
        watchlist_config=store.load_watchlist(user_id),
        user_id=user_id,
    )

    selected_symbols = selected["ts_code"].astype(str).tolist()
    return ResearchPoolSummary(
        ok=bool(watchlist_summary.ok),
        data_source=resolved_data_source,
        user_id=user_id,
        latest_trade_date=pd.Timestamp(latest_trade_date).strftime("%Y-%m-%d") if latest_trade_date else None,
        selected_count=int(len(selected)),
        watchlist_written=int(watchlist_written),
        daily_bar_rows=int(len(merged_daily_bar)),
        daily_bar_symbols=int(merged_daily_bar["ts_code"].nunique()) if not merged_daily_bar.empty else 0,
        feature_rows=int(feature_summary.get("feature_rows", 0) or 0),
        watchlist_snapshot_ok=bool(watchlist_summary.ok),
        message=watchlist_summary.message,
        selected_symbols=selected_symbols,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a 50-stock A-share research pool from Tushare and write focus_pool.")
    parser.add_argument("--data-source", default="tushare", help="Target dashboard data source.")
    parser.add_argument("--user-id", default=DEFAULT_USER_ID, help="Watchlist user id to update.")
    parser.add_argument("--end-date", default=None, help="YYYYMMDD or YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--limit", type=int, default=50, help="Number of focus stocks to select.")
    parser.add_argument("--append-focus", action="store_true", help="Append/update focus items instead of replacing focus_pool.")
    parser.add_argument("--skip-feature-rebuild", action="store_true", help="Only update daily_bar and watchlist.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_research_pool_refresh(
        data_source=args.data_source,
        user_id=args.user_id,
        end_date=args.end_date,
        limit=args.limit,
        replace_focus=not args.append_focus,
        rebuild_features=not args.skip_feature_rebuild,
    )
    logger.info("Research pool refresh result:\n{}", pformat(asdict(summary), sort_dicts=False))


if __name__ == "__main__":
    main()
