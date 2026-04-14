from __future__ import annotations

import json
from pathlib import Path

from src.utils.data_source import source_or_canonical_path


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_metrics(reports_dir: Path, model_name: str, data_source: str, split_name: str) -> dict[str, object]:
    return _load_json(source_or_canonical_path(reports_dir, f"{model_name}_{split_name}_metrics.json", data_source))


def _load_stability(reports_dir: Path, model_name: str, data_source: str) -> dict[str, object]:
    return _load_json(source_or_canonical_path(reports_dir, f"{model_name}_stability.json", data_source))


def _has_usable_metrics(metrics: dict[str, object]) -> bool:
    if not metrics:
        return False
    required_keys = {
        "observations",
        "dates",
        "daily_portfolio_annualized_return",
        "daily_portfolio_sharpe",
        "rank_ic_mean",
    }
    if not required_keys.issubset(metrics.keys()):
        return False
    return _safe_float(metrics.get("observations")) > 0 and _safe_float(metrics.get("dates")) > 0


def _stability_multiplier(stability_grade: str) -> float:
    if stability_grade == "较稳":
        return 1.0
    if stability_grade == "观察":
        return 0.85
    if stability_grade == "偏弱":
        return 0.65
    return 0.85


def _score_components(metrics: dict[str, object], stability: dict[str, object]) -> dict[str, float]:
    annualized = _clip01(_safe_float(metrics.get("daily_portfolio_annualized_return")) / 0.20)
    sharpe = _clip01(_safe_float(metrics.get("daily_portfolio_sharpe")) / 1.00)
    rank_ic = _clip01(_safe_float(metrics.get("rank_ic_mean")) / 0.08)
    drawdown = _clip01(1.0 - abs(_safe_float(metrics.get("daily_portfolio_max_drawdown"))) / 0.25)
    turnover = _clip01(1.0 - _safe_float(metrics.get("avg_turnover_ratio")) / 0.80)
    stability_multiplier = _stability_multiplier(str(stability.get("grade", "")))
    return {
        "annualized_component": annualized,
        "sharpe_component": sharpe,
        "rank_ic_component": rank_ic,
        "drawdown_component": drawdown,
        "turnover_component": turnover,
        "stability_multiplier": stability_multiplier,
    }


def _model_weight_score(metrics: dict[str, object], stability: dict[str, object]) -> tuple[float, dict[str, float]]:
    components = _score_components(metrics, stability)
    raw_score = (
        0.35 * components["annualized_component"]
        + 0.25 * components["sharpe_component"]
        + 0.20 * components["rank_ic_component"]
        + 0.10 * components["drawdown_component"]
        + 0.10 * components["turnover_component"]
    )
    return raw_score * components["stability_multiplier"], components


def _normalize_weights(weights: dict[str, float], min_model_weight: float = 0.0) -> dict[str, float]:
    if not weights:
        return {}

    total = sum(max(0.0, value) for value in weights.values())
    if total <= 0:
        equal = 1.0 / len(weights)
        return {key: equal for key in weights}

    normalized = {key: max(0.0, value) / total for key, value in weights.items()}
    floor = max(0.0, min(0.49, float(min_model_weight)))
    if floor <= 0:
        return normalized

    adjusted = {key: max(value, floor) for key, value in normalized.items()}
    adjusted_total = sum(adjusted.values())
    return {key: value / adjusted_total for key, value in adjusted.items()}


def _manual_weight_result(
    manual_weights: dict[str, float],
    model_names: tuple[str, ...],
    mode: str,
    summary: str,
    evaluation_split: str | None = None,
    diagnostics: dict[str, object] | None = None,
) -> dict[str, object]:
    weights = _normalize_weights({name: manual_weights.get(name, 0.0) for name in model_names})
    return {
        "mode": mode,
        "evaluation_split": evaluation_split,
        "weights": weights,
        "diagnostics": diagnostics or {},
        "summary": summary,
    }


def resolve_model_weights(
    reports_dir: Path,
    data_source: str,
    overlay_config: dict[str, object],
    model_names: tuple[str, ...] = ("lgbm", "ridge"),
) -> dict[str, object]:
    weight_mode = str(overlay_config.get("weight_mode", "validation_adaptive") or "validation_adaptive").strip()
    manual_weights = {
        "lgbm": _safe_float(overlay_config.get("lgbm_weight"), 0.6),
        "ridge": _safe_float(overlay_config.get("ridge_weight"), 0.4),
    }

    if weight_mode in {"manual", "fixed"}:
        return _manual_weight_result(
            manual_weights=manual_weights,
            model_names=model_names,
            mode="manual",
            evaluation_split=None,
            summary="当前采用固定权重融合。",
        )

    evaluation_split = str(overlay_config.get("weight_evaluation_split", "valid") or "valid").strip()
    min_model_weight = _safe_float(overlay_config.get("min_model_weight"), 0.15)

    diagnostics: dict[str, object] = {}
    raw_scores: dict[str, float] = {}
    missing_models: list[str] = []
    for model_name in model_names:
        metrics = _load_metrics(reports_dir, model_name, data_source, evaluation_split)
        stability = _load_stability(reports_dir, model_name, data_source)
        usable_metrics = _has_usable_metrics(metrics)
        if usable_metrics:
            score, components = _model_weight_score(metrics, stability)
            raw_scores[model_name] = max(score, 1e-6)
        else:
            score = 0.0
            components = {}
            missing_models.append(model_name)

        diagnostics[model_name] = {
            "evaluation_split": evaluation_split,
            "usable_metrics": usable_metrics,
            "stability_grade": stability.get("grade", ""),
            "metrics": metrics,
            "components": components,
            "raw_score": score,
        }

    if missing_models:
        summary = (
            f"检测到 {', '.join(model.upper() for model in missing_models)} 缺少可用的 {evaluation_split} 集指标，"
            "已回退到固定权重融合。"
        )
        return _manual_weight_result(
            manual_weights=manual_weights,
            model_names=model_names,
            mode="manual_fallback",
            evaluation_split=evaluation_split,
            diagnostics=diagnostics,
            summary=summary,
        )

    weights = _normalize_weights(raw_scores, min_model_weight=min_model_weight)
    summary_parts = [f"{model_name.upper()} 权重 {weights.get(model_name, 0.0):.1%}" for model_name in model_names]
    summary = f"当前采用基于 {evaluation_split} 集表现的自适应融合，" + "，".join(summary_parts) + "。"
    return {
        "mode": "validation_adaptive",
        "evaluation_split": evaluation_split,
        "weights": weights,
        "diagnostics": diagnostics,
        "summary": summary,
    }
