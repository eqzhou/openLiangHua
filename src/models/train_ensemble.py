from __future__ import annotations
from pathlib import Path

import pandas as pd

from src.agents.ensemble_weights import resolve_model_weights
from src.app.repositories.report_repository import (
    save_ensemble_weights_report,
    save_model_split_reports,
)
from src.app.services.model_workspace_service import load_prediction_artifact, resolve_model_workspace
from src.backtest.risk_filter import build_benchmark_proxy
from src.models.evaluate import build_performance_diagnostics, summarize_predictions
from src.models.stability import save_stability_summary
from src.models.train_linear import load_dataset
from src.models.walkforward import selection_kwargs
from src.utils.io import ensure_dir
from src.utils.logger import configure_logging

logger = configure_logging()

MODEL_NAMES = ("ridge", "lgbm")


def _prediction_path(reports_dir: Path, model_name: str, split_name: str, data_source: str) -> Path:
    return reports_dir / f"{data_source}_{model_name}_{split_name}_predictions.csv"


def _load_prediction_frame(workspace, model_name: str, split_name: str) -> pd.DataFrame:
    frame = load_prediction_artifact(
        workspace,
        model_name=model_name,
        split_name=split_name,
    )
    if frame.empty:
        return frame
    renamed = frame.rename(
        columns={
            "score": f"{model_name}_score",
            "score_raw": f"{model_name}_score_raw",
        }
    )
    return renamed


def _merge_prediction_frames(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for model_name, frame in frames.items():
        if frame.empty:
            return pd.DataFrame()

        score_columns = ["trade_date", "ts_code", f"{model_name}_score", f"{model_name}_score_raw"]
        available_score_columns = [column for column in score_columns if column in frame.columns]

        if merged is None:
            merged = frame.copy()
            continue

        merged = merged.merge(frame[available_score_columns], on=["trade_date", "ts_code"], how="inner")

    if merged is None:
        return pd.DataFrame()
    return merged.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def _apply_ensemble_score(frame: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    scored = frame.copy()
    score_parts: list[pd.Series] = []
    raw_parts: list[pd.Series] = []

    for model_name, weight in weights.items():
        score_col = f"{model_name}_score"
        raw_col = f"{model_name}_score_raw"
        if score_col in scored.columns:
            score_parts.append(pd.to_numeric(scored[score_col], errors="coerce").fillna(0.0) * float(weight))
        if raw_col in scored.columns:
            raw_parts.append(pd.to_numeric(scored[raw_col], errors="coerce").fillna(0.0) * float(weight))

    if not score_parts:
        return pd.DataFrame()

    scored["score"] = sum(score_parts)
    scored["score_raw"] = sum(raw_parts) if raw_parts else scored["score"]
    return scored


def run() -> None:
    workspace = resolve_model_workspace()
    root = workspace.root
    reports_dir = ensure_dir(root / "reports" / "weekly")
    panel, experiment = load_dataset()
    data_source = experiment.get("data_source", workspace.data_source)
    overlay_config = experiment.get("overlay", {})
    benchmark_proxy = build_benchmark_proxy(panel, experiment=experiment)
    portfolio_kwargs = selection_kwargs(experiment)

    weight_result = resolve_model_weights(
        reports_dir=reports_dir,
        data_source=data_source,
        overlay_config=overlay_config,
        model_names=MODEL_NAMES,
    )
    weights = {name: float(weight_result.get("weights", {}).get(name, 0.0)) for name in MODEL_NAMES}

    metrics_by_split: dict[str, dict] = {}
    diagnostics_by_split: dict[str, dict[str, pd.DataFrame]] = {}

    for split_name in ("valid", "test"):
        frames = {
            model_name: _load_prediction_frame(
                workspace=workspace,
                model_name=model_name,
                split_name=split_name,
            )
            for model_name in MODEL_NAMES
        }
        merged = _merge_prediction_frames(frames)
        if merged.empty:
            logger.warning(f"Skipping ensemble {split_name}: prediction inputs are missing.")
            continue

        scored = _apply_ensemble_score(merged, weights=weights)
        if scored.empty:
            logger.warning(f"Skipping ensemble {split_name}: no usable model scores were found.")
            continue

        summary, portfolio = summarize_predictions(
            frame=scored,
            score_col="score",
            label_col=experiment["label_col"],
            top_n=int(experiment["top_n"]),
            group_col=portfolio_kwargs["group_col"],
            max_per_group=portfolio_kwargs["max_per_group"],
            benchmark_proxy=benchmark_proxy,
            experiment=experiment,
        )
        summary["weight_mode"] = weight_result.get("mode")
        summary["weight_evaluation_split"] = weight_result.get("evaluation_split")
        summary["ridge_weight"] = weights.get("ridge", 0.0)
        summary["lgbm_weight"] = weights.get("lgbm", 0.0)

        diagnostics = build_performance_diagnostics(portfolio, label_col=experiment["label_col"])
        metrics_by_split[split_name] = summary
        diagnostics_by_split[split_name] = diagnostics

        save_model_split_reports(
            root,
            data_source=data_source,
            model_name="ensemble",
            split_name=split_name,
            predictions=scored,
            portfolio=portfolio,
            metrics=summary,
            diagnostics=diagnostics,
        )

    save_ensemble_weights_report(root, data_source=data_source, payload=weight_result)

    if metrics_by_split:
        save_stability_summary(
            reports_dir=reports_dir,
            model_name="ensemble",
            data_source=data_source,
            metrics_by_split=metrics_by_split,
        )
        logger.info(f"Saved ensemble reports to {reports_dir}")

    from src.db.dashboard_sync import sync_watchlist_snapshot_artifact

    summary = sync_watchlist_snapshot_artifact(root=root, data_source=data_source)
    logger.info(summary.message if summary.ok else f"Watchlist snapshot sync failed: {summary.message}")


if __name__ == "__main__":
    run()
