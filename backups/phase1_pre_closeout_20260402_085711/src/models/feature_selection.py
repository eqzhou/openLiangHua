from __future__ import annotations

import numpy as np
import pandas as pd


def _daily_rank_ic(frame: pd.DataFrame, feature_col: str, label_col: str, min_obs: int) -> pd.Series:
    scoped = frame[["trade_date", feature_col, label_col]].dropna()
    if scoped.empty:
        return pd.Series(dtype="float64")

    results: dict[pd.Timestamp, float] = {}
    for trade_date, group in scoped.groupby("trade_date", sort=True):
        if len(group) < min_obs:
            results[trade_date] = np.nan
            continue
        x = group[feature_col].rank()
        y = group[label_col].rank()
        if x.nunique() <= 1 or y.nunique() <= 1:
            results[trade_date] = np.nan
            continue
        results[trade_date] = float(x.corr(y))

    if not results:
        return pd.Series(dtype="float64")

    return pd.Series(results, name="rank_ic", dtype="float64")


def summarize_feature_quality(
    frame: pd.DataFrame,
    feature_columns: list[str],
    label_col: str,
    min_ic_observations: int = 20,
) -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []
    for column in feature_columns:
        series = frame[column]
        ic_series = _daily_rank_ic(frame, column, label_col, min_ic_observations)
        rows.append(
            {
                "feature": column,
                "missing_rate": float(series.isna().mean()),
                "nunique": int(series.nunique(dropna=True)),
                "rank_ic_mean": float(ic_series.mean(skipna=True)) if not ic_series.empty else np.nan,
                "rank_ic_abs_mean": float(ic_series.abs().mean(skipna=True)) if not ic_series.empty else np.nan,
                "rank_ic_std": float(ic_series.std(skipna=True)) if not ic_series.empty else np.nan,
                "rank_ic_days": int(ic_series.notna().sum()),
            }
        )

    return pd.DataFrame(rows).sort_values(["rank_ic_abs_mean", "feature"], ascending=[False, True]).reset_index(drop=True)


def _correlation_prune(
    frame: pd.DataFrame,
    quality: pd.DataFrame,
    correlation_threshold: float,
    sample_size: int,
) -> list[str]:
    if quality.empty:
        return []

    ordered_features = quality["feature"].tolist()
    sampled = frame[ordered_features].copy()
    if len(sampled) > sample_size:
        sampled = sampled.sample(sample_size, random_state=42)
    sampled = sampled.fillna(sampled.median(numeric_only=True))
    corr = sampled.corr().abs()

    selected: list[str] = []
    for feature in ordered_features:
        if not selected:
            selected.append(feature)
            continue
        if corr.loc[feature, selected].max() < correlation_threshold:
            selected.append(feature)
    return selected


def select_feature_columns(
    frame: pd.DataFrame,
    feature_columns: list[str],
    label_col: str,
    feature_selection_config: dict | None = None,
) -> tuple[list[str], pd.DataFrame]:
    config = feature_selection_config or {}
    quality = summarize_feature_quality(
        frame=frame,
        feature_columns=feature_columns,
        label_col=label_col,
        min_ic_observations=int(config.get("min_ic_observations", 20) or 20),
    )
    if quality.empty:
        return feature_columns, quality

    if not bool(config.get("enabled", False)):
        return feature_columns, quality

    filtered = quality.copy()
    max_missing_rate = float(config.get("max_missing_rate", 1.0))
    filtered = filtered.loc[filtered["missing_rate"] <= max_missing_rate]

    min_abs_rank_ic = float(config.get("min_abs_rank_ic", 0.0))
    filtered = filtered.loc[filtered["rank_ic_abs_mean"].fillna(0.0) >= min_abs_rank_ic]

    min_rank_ic_days = int(config.get("min_rank_ic_days", 0) or 0)
    if min_rank_ic_days > 0:
        filtered = filtered.loc[filtered["rank_ic_days"] >= min_rank_ic_days]

    filtered = filtered.loc[filtered["nunique"] > 1]

    exclude_features = {str(value) for value in config.get("exclude_features", [])}
    if exclude_features:
        filtered = filtered.loc[~filtered["feature"].isin(exclude_features)]

    if filtered.empty:
        return feature_columns, quality

    selected = _correlation_prune(
        frame=frame,
        quality=filtered,
        correlation_threshold=float(config.get("correlation_threshold", 0.98)),
        sample_size=int(config.get("correlation_sample_size", 80000) or 80000),
    )

    max_features = int(config.get("max_features", 0) or 0)
    if max_features > 0:
        selected = selected[:max_features]

    if not selected:
        selected = feature_columns
    return selected, quality
