from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd


FitModelFn = Callable[[pd.DataFrame, pd.Series], object]
PredictModelFn = Callable[[object, pd.DataFrame], np.ndarray]
ExtractImportanceFn = Callable[[object], pd.DataFrame]


def label_valid_column(label_col: str) -> str | None:
    if not label_col.startswith("ret_t1_t"):
        return None
    suffix = label_col.removeprefix("ret_t1_t")
    return f"label_valid_t{suffix}"


def apply_research_filters(
    panel: pd.DataFrame,
    experiment: dict,
    label_col: str,
    *,
    require_can_enter_next_day: bool | None = None,
    require_label_valid: bool | None = None,
) -> pd.DataFrame:
    filters = experiment.get("filters", {})
    filtered = panel.copy()

    if filters.get("exclude_current_name_st", True) and "is_current_name_st" in filtered.columns:
        filtered = filtered.loc[~filtered["is_current_name_st"].fillna(False)]

    enforce_can_enter = (
        filters.get("require_can_enter_next_day", True)
        if require_can_enter_next_day is None
        else require_can_enter_next_day
    )
    if enforce_can_enter and "can_enter_next_day" in filtered.columns:
        filtered = filtered.loc[filtered["can_enter_next_day"].fillna(False)]

    valid_col = label_valid_column(label_col)
    enforce_label_valid = (
        filters.get("require_label_valid", True)
        if require_label_valid is None
        else require_label_valid
    )
    if enforce_label_valid and valid_col and valid_col in filtered.columns:
        filtered = filtered.loc[filtered[valid_col].fillna(False)]

    min_listing_days = int(filters.get("min_listing_days", 0) or 0)
    if min_listing_days > 0 and "days_since_list" in filtered.columns:
        filtered = filtered.loc[filtered["days_since_list"].fillna(0) >= min_listing_days]

    return filtered.reset_index(drop=True)


def apply_inference_filters(panel: pd.DataFrame, experiment: dict, label_col: str) -> pd.DataFrame:
    return apply_research_filters(
        panel,
        experiment=experiment,
        label_col=label_col,
        require_can_enter_next_day=False,
        require_label_valid=False,
    )


def selection_kwargs(experiment: dict) -> dict[str, object]:
    selection = experiment.get("selection", {})
    group_col = selection.get("industry_column", "industry")
    neutralize = bool(selection.get("neutralize_by_industry", False))
    max_per_group = selection.get("max_per_industry")
    if max_per_group in (None, "", 0):
        max_per_group = None
    else:
        max_per_group = int(max_per_group)
    return {
        "group_col": group_col if neutralize or max_per_group else None,
        "max_per_group": max_per_group,
    }


def neutralize_scores(frame: pd.DataFrame, score_col: str, experiment: dict) -> pd.Series:
    selection = experiment.get("selection", {})
    if not selection.get("neutralize_by_industry", False):
        return frame[score_col]

    group_col = selection.get("industry_column", "industry")
    if group_col not in frame.columns:
        return frame[score_col]

    working = frame.copy()
    working[group_col] = working[group_col].fillna("未知行业")
    min_group_size = int(selection.get("min_industry_size", 3) or 3)
    group_size = working.groupby(group_col)[score_col].transform("count")
    centered = working[score_col] - working.groupby(group_col)[score_col].transform("mean")
    fallback = working[score_col] - working[score_col].mean()
    return centered.where(group_size >= min_group_size, fallback)


def retrain_dates(trade_dates: pd.Series, frequency: str) -> list[pd.Timestamp]:
    unique_dates = pd.Series(pd.to_datetime(trade_dates).dropna().unique()).sort_values().reset_index(drop=True)
    if unique_dates.empty:
        return []

    normalized = (frequency or "once").lower()
    if normalized in {"once", "single"}:
        return [pd.Timestamp(unique_dates.iloc[0])]
    if normalized in {"daily", "day"}:
        return [pd.Timestamp(value) for value in unique_dates.tolist()]

    buckets = pd.DataFrame({"trade_date": unique_dates})
    if normalized in {"weekly", "week"}:
        buckets["bucket"] = buckets["trade_date"].dt.to_period("W-FRI").astype(str)
    elif normalized in {"monthly", "month"}:
        buckets["bucket"] = buckets["trade_date"].dt.to_period("M").astype(str)
    elif normalized in {"quarterly", "quarter"}:
        buckets["bucket"] = buckets["trade_date"].dt.to_period("Q").astype(str)
    else:
        raise ValueError(f"Unsupported rolling retrain frequency: {frequency}")

    return [pd.Timestamp(value) for value in buckets.groupby("bucket")["trade_date"].min().tolist()]


def history_until(
    panel: pd.DataFrame,
    cutoff_date: pd.Timestamp,
    min_history_size: int,
    train_window_size: int | None,
) -> pd.DataFrame:
    history = panel.loc[panel["trade_date"] < cutoff_date].copy()
    if history.empty:
        return history

    unique_dates = history["trade_date"].drop_duplicates().sort_values().reset_index(drop=True)
    if len(unique_dates) < min_history_size:
        return pd.DataFrame(columns=history.columns)

    if train_window_size:
        selected_dates = set(unique_dates.iloc[-train_window_size:].tolist())
        history = history.loc[history["trade_date"].isin(selected_dates)].copy()

    return history


def walk_forward_score(
    panel: pd.DataFrame,
    split_frame: pd.DataFrame,
    feature_columns: list[str],
    label_col: str,
    experiment: dict,
    fit_model: FitModelFn,
    predict_model: PredictModelFn,
    extract_importance: ExtractImportanceFn,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if split_frame.empty:
        return pd.DataFrame(), pd.DataFrame()

    rolling = experiment.get("rolling", {})
    enabled = bool(rolling.get("enabled", False))
    frequency = rolling.get("retrain_frequency", "monthly")
    min_history_size = int(rolling.get("min_history_size", 252) or 252)
    train_window_size = rolling.get("train_window_size")
    train_window_size = None if train_window_size in (None, "", 0) else int(train_window_size)

    split_dates = split_frame["trade_date"].drop_duplicates().sort_values().reset_index(drop=True)
    anchors = retrain_dates(split_dates, frequency if enabled else "once")

    scored_parts: list[pd.DataFrame] = []
    importance_parts: list[pd.DataFrame] = []

    for idx, anchor_date in enumerate(anchors):
        history = history_until(
            panel=panel,
            cutoff_date=pd.Timestamp(anchor_date),
            min_history_size=min_history_size,
            train_window_size=train_window_size,
        )
        if history.empty:
            continue

        model = fit_model(history[feature_columns], history[label_col])
        importance = extract_importance(model).copy()
        if not importance.empty:
            importance["retrain_date"] = pd.Timestamp(anchor_date)
            importance_parts.append(importance)

        next_anchor = anchors[idx + 1] if idx + 1 < len(anchors) else None
        bucket = split_frame.loc[split_frame["trade_date"] >= anchor_date].copy()
        if next_anchor is not None:
            bucket = bucket.loc[bucket["trade_date"] < next_anchor].copy()
        if bucket.empty:
            continue

        bucket["score_raw"] = predict_model(model, bucket[feature_columns])
        bucket["score"] = neutralize_scores(bucket, score_col="score_raw", experiment=experiment)
        scored_parts.append(bucket)

    if not scored_parts:
        return pd.DataFrame(), pd.DataFrame()

    scored = pd.concat(scored_parts, ignore_index=True).sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

    if not importance_parts:
        return scored, pd.DataFrame()

    importance = pd.concat(importance_parts, ignore_index=True)
    numeric_cols = [column for column in importance.columns if column not in {"feature", "retrain_date"}]
    aggregated = importance.groupby("feature", as_index=False)[numeric_cols].mean()
    sort_col = numeric_cols[0] if numeric_cols else "feature"
    aggregated = aggregated.sort_values(sort_col, ascending=False).reset_index(drop=True)
    return scored, aggregated
