from __future__ import annotations

from pathlib import Path
import uuid

import numpy as np
import pandas as pd

from src.app.repositories.postgres_market_repository import (
    load_daily_bar_batch_from_market_database,
    load_full_daily_bar_from_market_database,
    load_equity_symbols_from_market_database,
)
from src.app.repositories.research_panel_repository import (
    build_panel_run_payload,
    merge_feature_and_label_frames,
    save_panel_run,
    save_research_panel,
)
from src.app.repositories.report_repository import save_binary_dataset
from src.data.universe import load_universe
from src.features.alpha_factors import add_price_factors
from src.features.labels import add_forward_returns
from src.features.quality_factors import add_valuation_factors
from src.features.risk_factors import add_risk_factors
from src.utils.data_source import active_data_source, normalize_data_source, source_or_canonical_path
from src.utils.io import project_root
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


def _uses_primary_project_root(root: Path | None) -> bool:
    if root is None:
        return True
    try:
        return root.resolve() == project_root().resolve()
    except OSError:
        return False


def _chunked_symbols(symbols: list[str], chunk_size: int) -> list[list[str]]:
    return [symbols[index : index + chunk_size] for index in range(0, len(symbols), chunk_size)]


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
    ) | watch_mask

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


def build_feature_label_artifacts(
    root: Path | None = None,
    *,
    data_source: str | None = None,
) -> dict[str, object]:
    resolved_root = root or project_root()
    resolved_data_source = normalize_data_source(data_source or active_data_source())
    if _uses_primary_project_root(resolved_root):
        benchmark_code = str(load_universe().get("benchmark", "000905.SH") or "000905.SH")
        symbols = load_equity_symbols_from_market_database()
        if not symbols:
            raise RuntimeError("Market database does not contain a usable full daily bar equity panel.")
        run_id = uuid.uuid4()
        cumulative_rows = 0
        cumulative_symbols = 0
        feature_columns: list[str] = []
        label_columns: list[str] = []
        chunk_size = 200
        save_panel_run(
            build_panel_run_payload(
                run_id=run_id,
                data_source=resolved_data_source,
                status="running",
                panel_frame=pd.DataFrame(columns=["trade_date", "ts_code"]),
                feature_columns=[],
                label_columns=[],
                message="research.panel rebuild started",
            )
        )

        for symbol_chunk in _chunked_symbols(symbols, chunk_size):
            logger.info("Building research.panel chunk size {} first_symbol {} last_symbol {}", len(symbol_chunk), symbol_chunk[0], symbol_chunk[-1])
            daily_bar = load_daily_bar_batch_from_market_database(symbol_chunk, benchmark_code=benchmark_code)
            if daily_bar.empty:
                continue
            feature_panel, label_panel = build_feature_and_label_panels(daily_bar)
            panel_frame = merge_feature_and_label_frames(
                data_source=resolved_data_source,
                run_id=run_id,
                feature_frame=feature_panel,
                label_frame=label_panel,
            )
            saved_rows = save_research_panel(panel_frame=panel_frame, chunk_size=50_000)
            cumulative_rows += saved_rows
            cumulative_symbols += int(panel_frame["ts_code"].nunique()) if not panel_frame.empty else 0
            if not feature_columns:
                feature_columns = [column for column in feature_panel.columns if column not in {"trade_date", "ts_code"}]
            if not label_columns:
                label_columns = [column for column in label_panel.columns if column not in {"trade_date", "ts_code"}]

        completed_payload = build_panel_run_payload(
            run_id=run_id,
            data_source=resolved_data_source,
            status="succeeded",
            panel_frame=pd.DataFrame(
                [{"trade_date": daily_bar["trade_date"].min(), "ts_code": daily_bar["ts_code"].iloc[0]}]
            )
            if 'daily_bar' in locals() and not daily_bar.empty
            else pd.DataFrame(columns=["trade_date", "ts_code"]),
            feature_columns=feature_columns,
            label_columns=label_columns,
            message=f"saved {cumulative_rows} rows across {cumulative_symbols} symbol-chunks",
        )
        completed_payload["row_count"] = cumulative_rows
        completed_payload["symbol_count"] = len(symbols)
        completed_payload["date_min"] = None
        completed_payload["date_max"] = None
        save_panel_run(completed_payload)
        feature_artifact_ref = f"research-panel://{resolved_data_source}/run/{run_id}/feature"
        label_artifact_ref = f"research-panel://{resolved_data_source}/run/{run_id}/label"
        from src.db.dashboard_sync import (
            sync_dataset_summary_artifact,
            sync_factor_explorer_snapshot_artifact,
        )
        dataset_summary = sync_dataset_summary_artifact(root=resolved_root, data_source=resolved_data_source)
        factor_snapshot = sync_factor_explorer_snapshot_artifact(root=resolved_root, data_source=resolved_data_source)
        logger.info(dataset_summary.message if dataset_summary.ok else f"Dataset summary sync failed: {dataset_summary.message}")
        logger.info(factor_snapshot.message if factor_snapshot.ok else f"Factor snapshot sync failed: {factor_snapshot.message}")
        return {
            "data_source": resolved_data_source,
            "feature_rows": cumulative_rows,
            "label_rows": cumulative_rows,
            "feature_artifact_ref": str(feature_artifact_ref),
            "label_artifact_ref": str(label_artifact_ref),
            "dataset_summary_ok": bool(dataset_summary.ok),
            "dataset_summary_message": dataset_summary.message,
            "factor_snapshot_ok": bool(factor_snapshot.ok),
            "factor_snapshot_message": factor_snapshot.message,
        }
    else:
        daily_bar_path = source_or_canonical_path(resolved_root / "data" / "staging", "daily_bar.parquet", resolved_data_source)
        if not daily_bar_path.exists():
            raise FileNotFoundError(f"Missing {daily_bar_path}. Run the downloader first.")
        daily_bar = pd.read_parquet(daily_bar_path)
    feature_panel, label_panel = build_feature_and_label_panels(daily_bar)
    feature_artifact_ref = save_binary_dataset(
        resolved_root,
        data_source=resolved_data_source,
        directory="data/features",
        filename="feature_panel.parquet",
        artifact_name="feature_panel",
        frame=feature_panel,
    )
    label_artifact_ref = save_binary_dataset(
        resolved_root,
        data_source=resolved_data_source,
        directory="data/labels",
        filename="label_panel.parquet",
        artifact_name="label_panel",
        frame=label_panel,
    )

    from src.db.dashboard_sync import (
        sync_dataset_summary_artifact,
        sync_factor_explorer_snapshot_artifact,
    )

    dataset_summary = sync_dataset_summary_artifact(root=resolved_root, data_source=resolved_data_source)
    factor_snapshot = sync_factor_explorer_snapshot_artifact(
        root=resolved_root,
        data_source=resolved_data_source,
        feature_panel=feature_panel,
    )
    logger.info(f"Data source: {resolved_data_source}")
    logger.info(f"Feature panel rows: {len(feature_panel):,}")
    logger.info(f"Label panel rows: {len(label_panel):,}")
    logger.info(dataset_summary.message if dataset_summary.ok else f"Dataset summary sync failed: {dataset_summary.message}")
    logger.info(factor_snapshot.message if factor_snapshot.ok else f"Factor snapshot sync failed: {factor_snapshot.message}")
    return {
        "data_source": resolved_data_source,
        "feature_rows": int(len(feature_panel)),
        "label_rows": int(len(label_panel)),
        "feature_artifact_ref": str(feature_artifact_ref),
        "label_artifact_ref": str(label_artifact_ref),
        "dataset_summary_ok": bool(dataset_summary.ok),
        "dataset_summary_message": dataset_summary.message,
        "factor_snapshot_ok": bool(factor_snapshot.ok),
        "factor_snapshot_message": factor_snapshot.message,
    }


def run() -> None:
    summary = build_feature_label_artifacts()
    logger.info(f"Feature artifact: {summary['feature_artifact_ref']}")
    logger.info(f"Label artifact: {summary['label_artifact_ref']}")


if __name__ == "__main__":
    run()
