from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.app.repositories.config_repository import load_experiment_config, load_universe_config
from src.app.repositories.report_repository import (
    load_feature_panel,
    load_label_panel,
    load_predictions,
)
from src.utils.data_source import active_data_source, normalize_data_source
from src.utils.io import project_root


@dataclass(frozen=True)
class ModelWorkspace:
    root: Path
    data_source: str
    experiment: dict


def resolve_model_workspace(root: Path | None = None) -> ModelWorkspace:
    resolved_root = root or project_root()
    try:
        universe = load_universe_config(resolved_root, prefer_database=False)
        data_source = normalize_data_source(universe.get("data_source", active_data_source()))
    except Exception:
        data_source = active_data_source()
    experiment = load_experiment_config(resolved_root, prefer_database=False)
    experiment["data_source"] = data_source
    return ModelWorkspace(root=resolved_root, data_source=data_source, experiment=experiment)


def load_feature_label_panel(workspace: ModelWorkspace) -> tuple[pd.DataFrame, pd.DataFrame]:
    features = load_feature_panel(workspace.root, data_source=workspace.data_source, prefer_database=False)
    labels = load_label_panel(workspace.root, data_source=workspace.data_source, prefer_database=False)
    return features, labels


def build_model_panel(workspace: ModelWorkspace) -> pd.DataFrame:
    features, labels = load_feature_label_panel(workspace)
    panel = features.merge(labels, on=["trade_date", "ts_code"], how="inner")
    panel["trade_date"] = pd.to_datetime(panel["trade_date"])
    return panel.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def load_prediction_artifact(
    workspace: ModelWorkspace,
    *,
    model_name: str,
    split_name: str,
) -> pd.DataFrame:
    frame = load_predictions(
        workspace.root,
        data_source=workspace.data_source,
        model_name=model_name,
        split_name=split_name,
        prefer_database=False,
    )
    if "trade_date" in frame.columns:
        frame = frame.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame
