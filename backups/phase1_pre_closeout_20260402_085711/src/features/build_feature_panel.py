from __future__ import annotations

import numpy as np
import pandas as pd

from src.data.universe import load_universe
from src.features.alpha_factors import add_price_factors
from src.features.labels import add_forward_returns
from src.features.quality_factors import add_valuation_factors
from src.features.risk_factors import add_risk_factors
from src.utils.data_source import active_data_source, source_prefixed_path, source_or_canonical_path
from src.utils.io import project_root, save_parquet
from src.utils.logger import configure_logging

logger = configure_logging()

RESERVED_COLUMNS = {
    "trade_date",
    "ts_code",
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
    "days_since_list",
    "universe_eligible",
}


def _ensure_bool_column(frame: pd.DataFrame, column: str, default: bool) -> None:
    if column not in frame.columns:
        frame[column] = default
    frame[column] = frame[column].fillna(default).astype(bool)


def build_feature_and_label_panels(daily_bar: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    universe_config = load_universe()
    strict_st_filter = bool(universe_config.get("strict_st_filter", True))
    strict_limit_filter = bool(universe_config.get("strict_limit_filter", True))
    watch_symbols = {str(symbol) for symbol in universe_config.get("watch_symbols", []) if symbol}

    panel = daily_bar.copy().sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"])
    panel["list_date"] = pd.to_datetime(panel["list_date"])
    _ensure_bool_column(panel, "is_index_member", True)
    _ensure_bool_column(panel, "is_st", False)
    _ensure_bool_column(panel, "is_suspend", True)
    _ensure_bool_column(panel, "is_buy_locked", False)
    _ensure_bool_column(panel, "is_sell_locked", False)
    panel["days_since_list"] = (panel["trade_date"] - panel["list_date"]).dt.days

    if not strict_limit_filter:
        panel["is_buy_locked"] = False
        panel["is_sell_locked"] = False
    st_block = panel["is_st"] if strict_st_filter else pd.Series(False, index=panel.index)

    listing_days_min = int(universe_config.get("listing_days_min", 0))
    watch_mask = panel["ts_code"].astype(str).isin(watch_symbols) if watch_symbols else pd.Series(False, index=panel.index)
    panel["universe_eligible"] = (
        panel["close_adj"].notna()
        & ~panel["is_suspend"]
        & ~st_block
        & (panel["days_since_list"] >= listing_days_min)
        & (panel["is_index_member"] | watch_mask)
    )

    tradable = panel.loc[panel["universe_eligible"]].copy()
    tradable = add_price_factors(tradable)
    tradable = add_valuation_factors(tradable)
    tradable = add_risk_factors(tradable)
    tradable = tradable.replace([np.inf, -np.inf], np.nan)

    label_input = tradable[
        [
            "trade_date",
            "ts_code",
            "close_adj",
            "is_st",
            "is_suspend",
            "is_buy_locked",
            "is_sell_locked",
        ]
    ].copy()
    if not strict_st_filter:
        label_input["is_st"] = False
    if not strict_limit_filter:
        label_input["is_buy_locked"] = False
        label_input["is_sell_locked"] = False

    labeled = add_forward_returns(label_input)

    feature_columns = [
        column
        for column in tradable.columns
        if column not in RESERVED_COLUMNS and pd.api.types.is_numeric_dtype(tradable[column])
    ]
    label_columns = [
        column
        for column in labeled.columns
        if column not in {"trade_date", "ts_code", "close_adj", "is_st", "is_suspend", "is_buy_locked", "is_sell_locked"}
    ]

    feature_panel = tradable[
        [
            "trade_date",
            "ts_code",
            "name",
            "industry",
            "index_code",
            "is_current_name_st",
            "is_index_member",
            "days_since_list",
        ]
        + feature_columns
    ].copy()
    label_panel = labeled[["trade_date", "ts_code"] + label_columns].copy()

    return feature_panel, label_panel


def run() -> None:
    root = project_root()
    data_source = active_data_source()
    daily_bar_path = source_or_canonical_path(root / "data" / "staging", "daily_bar.parquet", data_source)
    if not daily_bar_path.exists():
        raise FileNotFoundError(f"Missing {daily_bar_path}. Run the downloader first.")

    daily_bar = pd.read_parquet(daily_bar_path)
    feature_panel, label_panel = build_feature_and_label_panels(daily_bar)

    feature_dir = root / "data" / "features"
    label_dir = root / "data" / "labels"
    save_parquet(feature_panel, source_prefixed_path(feature_dir, "feature_panel.parquet", data_source))
    save_parquet(label_panel, source_prefixed_path(label_dir, "label_panel.parquet", data_source))
    save_parquet(feature_panel, feature_dir / "feature_panel.parquet")
    save_parquet(label_panel, label_dir / "label_panel.parquet")

    logger.info(f"Data source: {data_source}")
    logger.info(f"Feature panel rows: {len(feature_panel):,}")
    logger.info(f"Label panel rows: {len(label_panel):,}")


if __name__ == "__main__":
    run()
