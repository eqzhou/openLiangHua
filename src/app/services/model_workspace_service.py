from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import pandas as pd

from src.app.repositories.config_repository import load_experiment_config, load_universe_config
from src.app.repositories.research_panel_repository import load_research_panel
from src.app.repositories.report_repository import (
    load_feature_panel,
    load_label_panel,
    load_predictions,
)
from src.data.akshare_client import AKShareClient
from src.utils.data_source import active_data_source, normalize_data_source
from src.utils.io import project_root


@dataclass(frozen=True)
class ModelWorkspace:
    root: Path
    data_source: str
    experiment: dict
    universe: dict


def _use_database_artifacts(root: Path) -> bool:
    try:
        return root.resolve() == project_root().resolve()
    except OSError:
        return False


def resolve_model_workspace(root: Path | None = None) -> ModelWorkspace:
    resolved_root = root or project_root()
    universe: dict = {}
    try:
        universe = load_universe_config(resolved_root, prefer_database=True)
        data_source = normalize_data_source(universe.get("data_source", active_data_source()))
    except Exception:
        data_source = active_data_source()
    experiment = load_experiment_config(resolved_root, prefer_database=True)
    experiment["data_source"] = data_source
    return ModelWorkspace(root=resolved_root, data_source=data_source, experiment=experiment, universe=universe)


def load_feature_label_panel(workspace: ModelWorkspace) -> tuple[pd.DataFrame, pd.DataFrame]:
    use_database_artifacts = _use_database_artifacts(workspace.root)
    features = load_feature_panel(workspace.root, data_source=workspace.data_source, prefer_database=use_database_artifacts)
    labels = load_label_panel(workspace.root, data_source=workspace.data_source, prefer_database=use_database_artifacts)
    return features, labels


def _scoped_universe_kwargs(workspace: ModelWorkspace) -> dict[str, object]:
    universe = dict(workspace.universe or {})
    mode = str(universe.get("mode", "") or "").strip()
    watch_symbols = [str(symbol).strip() for symbol in (universe.get("watch_symbols", []) or []) if str(symbol).strip()]
    symbols = [str(symbol).strip() for symbol in (universe.get("symbols", []) or []) if str(symbol).strip()]

    if mode == "explicit":
        scoped_symbols = list(dict.fromkeys([*symbols, *watch_symbols]))
        return {"symbols": scoped_symbols}
    if mode == "current_index":
        index_code = str(universe.get("index_code", "") or "").strip()
        current_symbols = _current_index_symbols(index_code) if index_code else []
        if current_symbols:
            return {"symbols": list(dict.fromkeys([*current_symbols, *watch_symbols]))}
        if symbols:
            return {"symbols": list(dict.fromkeys([*symbols, *watch_symbols]))}
    return {}


@lru_cache(maxsize=8)
def _current_index_symbols(index_code: str) -> tuple[str, ...]:
    normalized_index_code = str(index_code or "").strip()
    if not normalized_index_code:
        return tuple()
    try:
        members = AKShareClient().current_index_members(normalized_index_code)
    except Exception:
        return tuple()
    if members.empty or "ts_code" not in members.columns:
        return tuple()
    return tuple(
        dict.fromkeys(
            str(symbol).strip()
            for symbol in members["ts_code"].astype(str).tolist()
            if str(symbol).strip()
        )
    )


def build_model_panel(
    workspace: ModelWorkspace,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    if _use_database_artifacts(workspace.root):
        panel = load_research_panel(
            data_source=workspace.data_source,
            date_from=date_from,
            date_to=date_to,
            **_scoped_universe_kwargs(workspace),
        )
        if not panel.empty and "trade_date" in panel.columns:
            panel["trade_date"] = pd.to_datetime(panel["trade_date"], errors="coerce")
        return panel.sort_values(["trade_date", "ts_code"]).reset_index(drop=True) if not panel.empty else panel

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
    use_database_artifacts = _use_database_artifacts(workspace.root)
    frame = load_predictions(
        workspace.root,
        data_source=workspace.data_source,
        model_name=model_name,
        split_name=split_name,
        prefer_database=use_database_artifacts,
    )
    if "trade_date" in frame.columns:
        frame = frame.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame
