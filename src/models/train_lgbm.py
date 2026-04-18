from __future__ import annotations

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.impute import SimpleImputer

from src.app.repositories.report_repository import (
    save_feature_importance_report,
    save_feature_quality_report,
    save_model_split_reports,
)
from src.app.services.model_workspace_service import resolve_model_workspace
from src.backtest.risk_filter import build_benchmark_proxy
from src.models.evaluate import build_performance_diagnostics, summarize_predictions
from src.models.feature_selection import select_feature_columns
from src.models.stability import save_stability_summary
from src.models.train_linear import infer_feature_columns, load_dataset, split_dataset
from src.models.walkforward import apply_research_filters, selection_kwargs, walk_forward_score
from src.utils.io import ensure_dir, project_root
from src.utils.logger import configure_logging

logger = configure_logging()


def run() -> None:
    workspace = resolve_model_workspace()
    root = workspace.root
    panel, experiment = load_dataset()
    market_panel = panel.copy()
    data_source = experiment.get("data_source", workspace.data_source)
    label_col = experiment["label_col"]
    panel[label_col] = pd.to_numeric(panel[label_col], errors="coerce")
    panel = panel.loc[np.isfinite(panel[label_col])].copy()
    panel = apply_research_filters(panel, experiment=experiment, label_col=label_col)
    benchmark_proxy = build_benchmark_proxy(market_panel, experiment=experiment)
    train, valid, test = split_dataset(panel, experiment)

    if train.empty or test.empty:
        raise RuntimeError("Train/test split is empty. Adjust config/experiment.yaml or your data range.")

    reports_dir = ensure_dir(root / "reports" / "weekly")
    importance_frames: list[pd.DataFrame] = []
    metrics_by_split: dict[str, dict] = {}
    portfolio_kwargs = selection_kwargs(experiment)
    raw_feature_columns = infer_feature_columns(panel, label_col)
    feature_columns, feature_quality = select_feature_columns(
        frame=train,
        feature_columns=raw_feature_columns,
        label_col=label_col,
        feature_selection_config=experiment.get("feature_selection", {}),
    )
    if not feature_quality.empty:
        save_feature_quality_report(root, data_source=data_source, filename="feature_quality.csv", frame=feature_quality)
    logger.info(f"Selected {len(feature_columns)} features for LightGBM from {len(raw_feature_columns)} candidates.")

    def fit_model(x: pd.DataFrame, y: pd.Series) -> tuple[SimpleImputer, LGBMRegressor]:
        imputer = SimpleImputer(strategy="median")
        x_imputed = pd.DataFrame(
            imputer.fit_transform(x),
            columns=feature_columns,
            index=x.index,
        )
        model = LGBMRegressor(**experiment["lgbm"])
        model.fit(x_imputed, y)
        return imputer, model

    def predict_model(bundle: tuple[SimpleImputer, LGBMRegressor], x: pd.DataFrame) -> np.ndarray:
        imputer, model = bundle
        x_imputed = pd.DataFrame(
            imputer.transform(x),
            columns=feature_columns,
            index=x.index,
        )
        return model.predict(x_imputed)

    def extract_importance(bundle: tuple[SimpleImputer, LGBMRegressor]) -> pd.DataFrame:
        _, model = bundle
        return (
            pd.DataFrame(
                {
                    "feature": feature_columns,
                    "importance_gain": model.booster_.feature_importance(importance_type="gain"),
                    "importance_split": model.booster_.feature_importance(importance_type="split"),
                }
            )
            .sort_values("importance_gain", ascending=False)
            .reset_index(drop=True)
        )

    for split_name, split_frame in {"valid": valid, "test": test}.items():
        if split_frame.empty:
            logger.warning(f"Skipping {split_name}: split is empty.")
            continue

        scored, importance_frame = walk_forward_score(
            panel=panel,
            split_frame=split_frame,
            feature_columns=feature_columns,
            label_col=label_col,
            experiment=experiment,
            fit_model=fit_model,
            predict_model=predict_model,
            extract_importance=extract_importance,
        )
        if scored.empty:
            logger.warning(f"Skipping {split_name}: no rows passed label validity filters.")
            continue
        if not importance_frame.empty:
            importance_frame["split"] = split_name
            importance_frames.append(importance_frame)
        summary, daily_portfolio = summarize_predictions(
            frame=scored,
            score_col="score",
            label_col=label_col,
            top_n=int(experiment["top_n"]),
            group_col=portfolio_kwargs["group_col"],
            max_per_group=portfolio_kwargs["max_per_group"],
            benchmark_proxy=benchmark_proxy,
            experiment=experiment,
        )
        diagnostics = build_performance_diagnostics(daily_portfolio, label_col=label_col)
        metrics_by_split[split_name] = summary

        save_model_split_reports(
            root,
            data_source=data_source,
            model_name="lgbm",
            split_name=split_name,
            predictions=scored,
            portfolio=daily_portfolio,
            metrics=summary,
            diagnostics=diagnostics,
        )
        logger.info(f"Saved LightGBM {split_name} reports to {reports_dir}")

    if importance_frames:
        importance = (
            pd.concat(importance_frames, ignore_index=True)
            .groupby("feature", as_index=False)[["importance_gain", "importance_split"]]
            .mean()
            .sort_values("importance_gain", ascending=False)
            .reset_index(drop=True)
        )
        save_feature_importance_report(root, data_source=data_source, model_name="lgbm", frame=importance)

    from src.db.dashboard_sync import sync_watchlist_snapshot_artifact

    summary = sync_watchlist_snapshot_artifact(root=root, data_source=data_source)
    logger.info(summary.message if summary.ok else f"Watchlist snapshot sync failed: {summary.message}")

    if metrics_by_split:
        save_stability_summary(
            reports_dir=reports_dir,
            model_name="lgbm",
            data_source=data_source,
            metrics_by_split=metrics_by_split,
        )


if __name__ == "__main__":
    run()
