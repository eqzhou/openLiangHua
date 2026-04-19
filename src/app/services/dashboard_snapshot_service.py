from __future__ import annotations

import pandas as pd

from src.app.repositories.config_repository import load_watchlist_config
from src.app.repositories.report_repository import (
    load_candidate_snapshot as repo_load_candidate_snapshot,
    load_daily_bar as repo_load_daily_bar,
    load_factor_explorer_snapshot as repo_load_factor_explorer_snapshot,
    load_feature_panel as repo_load_feature_panel,
    load_overlay_candidates as repo_load_overlay_candidates,
    load_overlay_inference_candidates as repo_load_overlay_inference_candidates,
    load_predictions as repo_load_predictions,
    load_watchlist_snapshot as repo_load_watchlist_snapshot,
)
from src.app.services.watchlist_service import build_watchlist_view
from src.app.viewmodels.factor_explorer_vm import build_missing_rate_table, list_numeric_factor_columns
from src.db.dashboard_sync import (
    sync_candidate_snapshot_artifact,
    sync_factor_explorer_snapshot_artifact,
    sync_watchlist_snapshot_artifact,
)
from src.utils.data_source import active_data_source
from src.utils.io import project_root

ROOT = project_root()


def _current_data_source() -> str:
    return active_data_source()


def clear_snapshot_caches() -> None:
    return None


def load_watchlist_snapshot() -> pd.DataFrame | None:
    return repo_load_watchlist_snapshot(ROOT, data_source=_current_data_source())


def load_candidate_snapshot(model_name: str, split_name: str) -> pd.DataFrame | None:
    return repo_load_candidate_snapshot(ROOT, data_source=_current_data_source(), model_name=model_name, split_name=split_name)


def load_factor_explorer_snapshot() -> dict[str, object] | None:
    return repo_load_factor_explorer_snapshot(ROOT, data_source=_current_data_source())


def build_watchlist_base_frame() -> pd.DataFrame:
    current_data_source = _current_data_source()
    watchlist_snapshot = load_watchlist_snapshot()
    if watchlist_snapshot is not None:
        return watchlist_snapshot.copy()

    watchlist_config = load_watchlist_config(ROOT, prefer_database=True)
    frame = build_watchlist_view(
        root=ROOT,
        data_source=current_data_source,
        watchlist_config=watchlist_config,
        daily_bar=repo_load_daily_bar(ROOT, data_source=current_data_source),
        ridge_predictions=repo_load_predictions(ROOT, data_source=current_data_source, model_name="ridge", split_name="test"),
        lgbm_predictions=repo_load_predictions(ROOT, data_source=current_data_source, model_name="lgbm", split_name="test"),
        ensemble_predictions=repo_load_predictions(ROOT, data_source=current_data_source, model_name="ensemble", split_name="test"),
        overlay_candidates=repo_load_overlay_candidates(ROOT, data_source=current_data_source),
        ensemble_inference_predictions=repo_load_predictions(ROOT, data_source=current_data_source, model_name="ensemble", split_name="inference"),
        overlay_inference_candidates=repo_load_overlay_inference_candidates(ROOT, data_source=current_data_source),
    )
    sync_watchlist_snapshot_artifact(
        root=ROOT,
        data_source=current_data_source,
        watchlist_config=watchlist_config,
        snapshot_frame=frame,
    )
    return frame


def build_candidate_snapshot(model_name: str, split_name: str) -> pd.DataFrame:
    current_data_source = _current_data_source()
    stored_snapshot = load_candidate_snapshot(model_name, split_name)
    if stored_snapshot is not None:
        return stored_snapshot.copy()

    predictions = repo_load_predictions(ROOT, data_source=current_data_source, model_name=model_name, split_name=split_name)
    from src.utils.prediction_snapshot import build_latest_prediction_snapshot

    snapshot = build_latest_prediction_snapshot(predictions)
    sync_candidate_snapshot_artifact(
        root=ROOT,
        data_source=current_data_source,
        model_name=model_name,
        split_name=split_name,
        predictions=predictions,
        snapshot_frame=snapshot,
    )
    return snapshot


def build_factor_explorer_snapshot(field_explanations_items: tuple[tuple[str, str], ...]) -> dict[str, object]:
    current_data_source = _current_data_source()
    field_explanations = dict(field_explanations_items)
    stored_snapshot = load_factor_explorer_snapshot()
    if stored_snapshot is not None:
        return dict(stored_snapshot)

    feature_panel = repo_load_feature_panel(ROOT, data_source=current_data_source)
    numeric_columns = list_numeric_factor_columns(feature_panel)
    if not numeric_columns:
        return {
            "available": False,
            "latestDate": None,
            "factorOptions": [],
            "symbolOptions": [],
            "crossSection": [],
            "missingRates": [],
        }

    latest_date = feature_panel["trade_date"].max()
    cross_section = feature_panel.loc[feature_panel["trade_date"] == latest_date].copy()
    snapshot_payload: dict[str, object] = {
        "available": True,
        "latestDate": latest_date.isoformat() if isinstance(latest_date, pd.Timestamp) else str(latest_date),
        "factorOptions": [
            {"key": column, "label": column, "description": field_explanations.get(column, "")}
            for column in numeric_columns
        ],
        "symbolOptions": cross_section["ts_code"].sort_values().tolist(),
        "crossSection": cross_section.to_dict(orient="records"),
        "missingRates": build_missing_rate_table(feature_panel, numeric_columns).to_dict(orient="records"),
    }
    sync_factor_explorer_snapshot_artifact(
        root=ROOT,
        data_source=current_data_source,
        feature_panel=feature_panel,
        snapshot_payload=snapshot_payload,
    )
    return snapshot_payload
