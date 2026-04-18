from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.app.services.model_workspace_service import build_model_panel, resolve_model_workspace
from src.app.repositories.report_repository import (
    save_feature_importance_report,
    save_feature_quality_report,
    save_model_split_reports,
)
from src.backtest.risk_filter import build_benchmark_proxy
from src.models.feature_selection import select_feature_columns
from src.models.evaluate import build_performance_diagnostics, summarize_predictions
from src.models.stability import save_stability_summary
from src.models.walkforward import apply_research_filters, selection_kwargs, walk_forward_score
from src.utils.io import ensure_dir
from src.utils.logger import configure_logging

logger = configure_logging()

META_COLUMNS = {
    "trade_date",
    "ts_code",
    "name",
    "industry",
    "index_code",
    "is_current_name_st",
    "is_index_member",
    "days_since_list",
}


def load_dataset() -> tuple[pd.DataFrame, dict]:
    workspace = resolve_model_workspace()
    return build_model_panel(workspace), workspace.experiment


def split_dataset(panel: pd.DataFrame, experiment: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train_end = pd.Timestamp(experiment["train_end"])
    valid_end = pd.Timestamp(experiment["valid_end"])
    test_end = pd.Timestamp(experiment["test_end"])
    train_start = pd.Timestamp(experiment["train_start"])

    scoped = panel.loc[(panel["trade_date"] >= train_start) & (panel["trade_date"] <= test_end)].copy()
    train = scoped.loc[scoped["trade_date"] <= train_end].copy()
    valid = scoped.loc[(scoped["trade_date"] > train_end) & (scoped["trade_date"] <= valid_end)].copy()
    test = scoped.loc[(scoped["trade_date"] > valid_end) & (scoped["trade_date"] <= test_end)].copy()
    return train, valid, test


def infer_feature_columns(panel: pd.DataFrame, label_col: str) -> list[str]:
    excluded = META_COLUMNS | {label_col, "can_enter_next_day", "ret_next_1d"}
    return [
        column
        for column in panel.columns
        if column not in excluded
        and not column.startswith("ret_t1_t")
        and not column.startswith("label_valid_")
        and pd.api.types.is_numeric_dtype(panel[column])
    ]


def build_estimator(alpha: float = 1.0) -> Pipeline:
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", Ridge(alpha=alpha)),
        ]
    )


def extract_importance(estimator: Pipeline, feature_columns: list[str]) -> pd.DataFrame:
    ridge_model: Ridge = estimator.named_steps["model"]
    return (
        pd.DataFrame({"feature": feature_columns, "coefficient": ridge_model.coef_})
        .sort_values("coefficient", key=lambda series: series.abs(), ascending=False)
        .reset_index(drop=True)
    )


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

    raw_feature_columns = infer_feature_columns(panel, label_col)
    feature_columns, feature_quality = select_feature_columns(
        frame=train,
        feature_columns=raw_feature_columns,
        label_col=label_col,
        feature_selection_config=experiment.get("feature_selection", {}),
    )

    reports_dir = ensure_dir(root / "reports" / "weekly")
    importance_frames: list[pd.DataFrame] = []
    metrics_by_split: dict[str, dict] = {}
    portfolio_kwargs = selection_kwargs(experiment)
    if not feature_quality.empty:
        save_feature_quality_report(root, data_source=data_source, filename="feature_quality.csv", frame=feature_quality)
    logger.info(f"Selected {len(feature_columns)} features for Ridge from {len(raw_feature_columns)} candidates.")

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
            fit_model=lambda x, y: build_estimator(alpha=1.0).fit(x, y),
            predict_model=lambda model, x: model.predict(x),
            extract_importance=lambda model: extract_importance(model, feature_columns),
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
            model_name="ridge",
            split_name=split_name,
            predictions=scored,
            portfolio=daily_portfolio,
            metrics=summary,
            diagnostics=diagnostics,
        )
        logger.info(f"Saved Ridge {split_name} reports to {reports_dir}")

    if importance_frames:
        coefficient_frame = (
            pd.concat(importance_frames, ignore_index=True)
            .groupby("feature", as_index=False)["coefficient"]
            .mean()
            .sort_values("coefficient", key=lambda series: series.abs(), ascending=False)
            .reset_index(drop=True)
        )
        save_feature_importance_report(root, data_source=data_source, model_name="ridge", frame=coefficient_frame)

    from src.db.dashboard_sync import sync_watchlist_snapshot_artifact

    summary = sync_watchlist_snapshot_artifact(root=root, data_source=data_source)
    logger.info(summary.message if summary.ok else f"Watchlist snapshot sync failed: {summary.message}")

    if metrics_by_split:
        save_stability_summary(
            reports_dir=reports_dir,
            model_name="ridge",
            data_source=data_source,
            metrics_by_split=metrics_by_split,
        )


if __name__ == "__main__":
    run()
