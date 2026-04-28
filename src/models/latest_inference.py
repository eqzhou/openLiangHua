from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.impute import SimpleImputer

from src.agents.ensemble_weights import resolve_model_weights
from src.app.repositories.report_repository import (
    save_feature_quality_report,
    save_inference_packet,
    save_model_split_reports,
)
from src.app.services.model_workspace_service import build_model_panel, resolve_model_workspace
from src.backtest.risk_filter import build_benchmark_proxy, latest_trend_state
from src.models.feature_selection import select_feature_columns
from src.models.train_ensemble import _apply_ensemble_score
from src.models.train_linear import build_estimator, infer_feature_columns
from src.models.walkforward import apply_inference_filters, apply_research_filters, history_until, neutralize_scores
from src.utils.io import ensure_dir, project_root
from src.utils.logger import configure_logging

logger = configure_logging()

MODEL_NAMES = ("ridge", "lgbm")


def _training_history(panel: pd.DataFrame, experiment: dict, label_col: str, latest_feature_date: pd.Timestamp) -> pd.DataFrame:
    training_panel = panel.copy()
    training_panel[label_col] = pd.to_numeric(training_panel[label_col], errors="coerce")
    training_panel = training_panel.loc[np.isfinite(training_panel[label_col])].copy()
    training_panel = apply_research_filters(training_panel, experiment=experiment, label_col=label_col)

    rolling = experiment.get("rolling", {})
    min_history_size = int(rolling.get("min_history_size", 252) or 252)
    train_window_size = rolling.get("train_window_size")
    train_window_size = None if train_window_size in (None, "", 0) else int(train_window_size)
    history = history_until(
        panel=training_panel,
        cutoff_date=pd.Timestamp(latest_feature_date),
        min_history_size=min_history_size,
        train_window_size=train_window_size,
    )
    if history.empty:
        raise RuntimeError("Latest inference history is empty after research filters/windowing.")
    return history.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def _inference_snapshot(panel: pd.DataFrame, experiment: dict, label_col: str, latest_feature_date: pd.Timestamp) -> pd.DataFrame:
    snapshot = panel.loc[panel["trade_date"] == pd.Timestamp(latest_feature_date)].copy()
    snapshot = apply_inference_filters(snapshot, experiment=experiment, label_col=label_col)
    if snapshot.empty:
        raise RuntimeError("Latest unlabeled inference snapshot is empty after inference filters.")
    return snapshot.sort_values("ts_code").reset_index(drop=True)


def _fit_ridge(history: pd.DataFrame, feature_columns: list[str], label_col: str):
    return build_estimator(alpha=1.0).fit(history[feature_columns], history[label_col])


def _fit_lgbm(history: pd.DataFrame, feature_columns: list[str], label_col: str) -> tuple[SimpleImputer, LGBMRegressor]:
    imputer = SimpleImputer(strategy="median")
    x_imputed = pd.DataFrame(
        imputer.fit_transform(history[feature_columns]),
        columns=feature_columns,
        index=history.index,
    )
    model = LGBMRegressor(**history.attrs["experiment"]["lgbm"])
    model.fit(x_imputed, history[label_col])
    return imputer, model


def _predict_lgbm(bundle: tuple[SimpleImputer, LGBMRegressor], frame: pd.DataFrame, feature_columns: list[str]) -> np.ndarray:
    imputer, model = bundle
    x_imputed = pd.DataFrame(
        imputer.transform(frame[feature_columns]),
        columns=feature_columns,
        index=frame.index,
    )
    return model.predict(x_imputed)


def _score_snapshot(
    frame: pd.DataFrame,
    *,
    model_name: str,
    feature_columns: list[str],
    raw_score: np.ndarray,
    experiment: dict,
) -> pd.DataFrame:
    scored = frame.copy()
    scored["score_raw"] = pd.to_numeric(pd.Series(raw_score, index=scored.index), errors="coerce")
    scored["score"] = neutralize_scores(scored, score_col="score_raw", experiment=experiment)
    logger.info(f"Built latest inference snapshot for {model_name} with {len(scored)} rows.")
    return scored


def generate_latest_inference(root: Path | None = None, *, user_id: str | None = None) -> dict[str, object]:
    resolved_root = root or project_root()
    workspace = resolve_model_workspace(resolved_root)
    data_source = workspace.data_source
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")

    panel = build_model_panel(workspace, user_id=user_id)
    experiment = workspace.experiment
    market_panel = panel.copy()
    label_col = str(experiment["label_col"])
    latest_feature_date = pd.Timestamp(panel["trade_date"].max())

    history = _training_history(panel, experiment=experiment, label_col=label_col, latest_feature_date=latest_feature_date)
    history.attrs["experiment"] = experiment
    latest_labeled_date = pd.Timestamp(history["trade_date"].max())
    snapshot = _inference_snapshot(panel, experiment=experiment, label_col=label_col, latest_feature_date=latest_feature_date)

    raw_feature_columns = infer_feature_columns(history, label_col)
    feature_columns, feature_quality = select_feature_columns(
        frame=history,
        feature_columns=raw_feature_columns,
        label_col=label_col,
        feature_selection_config=experiment.get("feature_selection", {}),
    )
    if not feature_columns:
        raise RuntimeError("Latest inference could not select any usable feature columns.")

    ridge_model = _fit_ridge(history, feature_columns=feature_columns, label_col=label_col)
    ridge_frame = _score_snapshot(
        snapshot,
        model_name="ridge",
        feature_columns=feature_columns,
        raw_score=ridge_model.predict(snapshot[feature_columns]),
        experiment=experiment,
    )

    lgbm_bundle = _fit_lgbm(history, feature_columns=feature_columns, label_col=label_col)
    lgbm_frame = _score_snapshot(
        snapshot,
        model_name="lgbm",
        feature_columns=feature_columns,
        raw_score=_predict_lgbm(lgbm_bundle, snapshot, feature_columns=feature_columns),
        experiment=experiment,
    )

    ensemble_weights = resolve_model_weights(
        reports_dir=reports_dir,
        data_source=data_source,
        overlay_config=experiment.get("overlay", {}),
        model_names=MODEL_NAMES,
    )
    weights = {name: float(ensemble_weights.get("weights", {}).get(name, 0.0)) for name in MODEL_NAMES}

    ensemble_base = ridge_frame.rename(
        columns={
            "score": "ridge_score",
            "score_raw": "ridge_score_raw",
        }
    ).merge(
        lgbm_frame[["trade_date", "ts_code", "score", "score_raw"]].rename(
            columns={
                "score": "lgbm_score",
                "score_raw": "lgbm_score_raw",
            }
        ),
        on=["trade_date", "ts_code"],
        how="inner",
    )
    ensemble_frame = _apply_ensemble_score(ensemble_base, weights=weights)
    if ensemble_frame.empty:
        raise RuntimeError("Latest inference ensemble snapshot is empty after merging model outputs.")

    benchmark_proxy = build_benchmark_proxy(market_panel, experiment=experiment)
    risk_state = latest_trend_state(benchmark_proxy, experiment=experiment, as_of_date=latest_feature_date)

    ridge_paths = save_model_split_reports(
        resolved_root,
        data_source=data_source,
        model_name="ridge",
        split_name="inference",
        predictions=ridge_frame,
        portfolio=pd.DataFrame(),
        metrics={},
        diagnostics={},
        user_id=user_id,
    )
    lgbm_paths = save_model_split_reports(
        resolved_root,
        data_source=data_source,
        model_name="lgbm",
        split_name="inference",
        predictions=lgbm_frame,
        portfolio=pd.DataFrame(),
        metrics={},
        diagnostics={},
        user_id=user_id,
    )
    ensemble_paths = save_model_split_reports(
        resolved_root,
        data_source=data_source,
        model_name="ensemble",
        split_name="inference",
        predictions=ensemble_frame,
        portfolio=pd.DataFrame(),
        metrics={},
        diagnostics={},
        user_id=user_id,
    )

    feature_quality_export = feature_quality.copy()
    if not feature_quality_export.empty:
        quality_artifact_ref = save_feature_quality_report(
            resolved_root,
            data_source=data_source,
            filename="inference_feature_quality.csv",
            frame=feature_quality_export,
        )
        quality_paths = {"artifact_ref": str(quality_artifact_ref)}
    else:
        quality_paths = {}

    skipped_filters: list[str] = []
    filters = experiment.get("filters", {})
    if filters.get("require_can_enter_next_day", True):
        skipped_filters.append("can_enter_next_day")
    if filters.get("require_label_valid", True):
        skipped_filters.append(f"label_valid({label_col})")

    packet = {
        "data_source": data_source,
        "user_id": user_id,
        "prediction_mode": "latest_unlabeled_inference",
        "label_col": label_col,
        "latest_feature_date": str(latest_feature_date.date()),
        "latest_labeled_date": str(latest_labeled_date.date()),
        "history_window_start": str(pd.Timestamp(history["trade_date"].min()).date()),
        "history_window_end": str(pd.Timestamp(history["trade_date"].max()).date()),
        "history_observations": int(len(history)),
        "history_dates": int(history["trade_date"].nunique()),
        "inference_universe_size": int(len(snapshot)),
        "feature_count": int(len(feature_columns)),
        "feature_columns": feature_columns,
        "ensemble_weights": ensemble_weights,
        "latest_risk_state": risk_state,
        "skipped_filters": skipped_filters,
        "notes": [
            "Latest unlabeled inference skips future-dependent entry filters because next-day tradability is not observable yet.",
            "Scores are generated from the newest feature cross-section and do not overwrite historical test backtest files.",
        ],
        "artifacts": {
            "ridge": ridge_paths,
            "lgbm": lgbm_paths,
            "ensemble": ensemble_paths,
            "feature_quality": quality_paths,
        },
    }

    packet_artifact_ref = save_inference_packet(resolved_root, data_source=data_source, payload=packet, user_id=user_id)
    logger.info(f"Saved latest inference packet to {packet_artifact_ref}")
    return packet


def run() -> None:
    packet = generate_latest_inference()
    print(json.dumps(packet, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    run()
