from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.app.services.holding_snapshot_service import build_holding_snapshot
from src.utils.holding_marks import describe_price_reference
from src.utils.llm_discussion import load_symbol_discussion_snapshot
from src.utils.premarket_plan import build_premarket_plan
from src.utils.prediction_snapshot import build_latest_prediction_snapshot, latest_prediction_details


def _overlay_symbol_set(frame: pd.DataFrame) -> set[str]:
    if frame.empty or "ts_code" not in frame.columns:
        return set()
    return set(frame["ts_code"].astype(str).tolist())


def build_watchlist_view(
    *,
    root: Path,
    data_source: str,
    watchlist_config: dict,
    daily_bar: pd.DataFrame,
    ridge_predictions: pd.DataFrame,
    lgbm_predictions: pd.DataFrame,
    ensemble_predictions: pd.DataFrame,
    overlay_candidates: pd.DataFrame,
    ensemble_inference_predictions: pd.DataFrame,
    overlay_inference_candidates: pd.DataFrame,
) -> pd.DataFrame:
    holdings = watchlist_config.get("holdings", []) or []
    if not holdings:
        return pd.DataFrame()

    prediction_snapshots = {
        "ridge": build_latest_prediction_snapshot(ridge_predictions),
        "lgbm": build_latest_prediction_snapshot(lgbm_predictions),
        "ensemble": build_latest_prediction_snapshot(ensemble_predictions),
    }
    overlay_symbols = _overlay_symbol_set(overlay_candidates)
    inference_overlay_symbols = _overlay_symbol_set(overlay_inference_candidates)

    rows: list[dict[str, object]] = []
    for item in holdings:
        symbol = str(item.get("ts_code", "") or "").strip()
        if not symbol:
            continue

        snapshot = build_holding_snapshot(
            item,
            daily_bar=daily_bar,
            prediction_snapshots=prediction_snapshots,
            overlay_symbols=overlay_symbols,
        )
        inference_ensemble_info = latest_prediction_details(ensemble_inference_predictions, symbol=symbol)
        discussion_snapshot = load_symbol_discussion_snapshot(root, data_source, symbol)

        mark_reference = describe_price_reference(
            is_manual_mark=bool(snapshot.get("is_manual_mark")),
            mark_date=snapshot.get("mark_date"),
            latest_bar_date=snapshot.get("latest_bar_date"),
        )

        ranking_note_parts: list[str] = []
        if snapshot.get("ensemble_rank") is not None:
            ranking_note_parts.append(
                f"历史验证 {int(snapshot['ensemble_rank'])}/{int(snapshot.get('universe_size') or 0)}"
            )
        if inference_ensemble_info.get("rank") is not None:
            ranking_note_parts.append(
                f"最新推理 {int(inference_ensemble_info['rank'])}/{int(inference_ensemble_info.get('universe_size') or 0)}"
            )

        levels = snapshot.get("levels", {})
        premarket_plan_payload = build_premarket_plan(
            discussion_snapshot=discussion_snapshot,
            action_brief=str(snapshot.get("action_brief", "") or ""),
            anchor_price=float(levels.get("anchor_price")) if levels.get("anchor_price") is not None else None,
            defensive_price=float(levels.get("defensive_price")) if levels.get("defensive_price") is not None else None,
            breakeven_price=float(snapshot.get("cost_basis") or 0.0) if snapshot.get("cost_basis") else None,
        )

        row = dict(snapshot)
        row.update(
            {
                "premarket_plan": premarket_plan_payload.get("premarket_plan", ""),
                "premarket_plan_source": premarket_plan_payload.get("premarket_plan_source", ""),
                "mark_status": mark_reference.get("mark_status"),
                "mark_status_note": mark_reference.get("mark_status_note"),
                "mark_vs_latest_bar_days": mark_reference.get("mark_vs_latest_bar_days"),
                "breakeven_price": snapshot.get("cost_basis") or None,
                "gap_to_breakeven_pct": (
                    float(float(snapshot["cost_basis"]) / float(snapshot["mark_price"]) - 1.0)
                    if snapshot.get("mark_price") not in (None, 0) and snapshot.get("cost_basis")
                    else None
                ),
                "halfway_recovery_price": (
                    float(float(snapshot["mark_price"]) + 0.5 * (float(snapshot["cost_basis"]) - float(snapshot["mark_price"])))
                    if snapshot.get("mark_price") is not None and snapshot.get("cost_basis")
                    else None
                ),
                "defensive_price": levels.get("defensive_price"),
                "inference_signal_date": inference_ensemble_info.get("signal_date"),
                "inference_ensemble_rank": inference_ensemble_info.get("rank"),
                "inference_ensemble_rank_pct": inference_ensemble_info.get("rank_pct"),
                "inference_score": inference_ensemble_info.get("score"),
                "is_inference_overlay_selected": symbol in inference_overlay_symbols,
                "ranking_note": " | ".join(ranking_note_parts),
                "llm_round_count": discussion_snapshot.get("round_count", 0),
                "llm_selected_round_count": discussion_snapshot.get("selected_round_count", 0),
                "llm_success_round_count": discussion_snapshot.get("success_round_count", 0),
                "llm_latest_round": discussion_snapshot.get("latest_round_label", ""),
                "llm_latest_status": discussion_snapshot.get("latest_status", ""),
                "llm_latest_summary": discussion_snapshot.get("latest_summary", ""),
                "llm_overview": discussion_snapshot.get("overview", ""),
                "llm_discussion_snapshot": discussion_snapshot,
            }
        )
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    return frame.drop(columns=["levels", "latest_row", "latest_valid_row"], errors="ignore")


def filtered_watchlist_view(
    frame: pd.DataFrame,
    *,
    keyword: str,
    scope: str,
    sort_by: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame

    filtered = frame.copy()
    normalized_keyword = str(keyword or "").strip().lower()
    if normalized_keyword:
        ts_code_text = filtered["ts_code"].astype(str).str.lower()
        name_text = filtered["name"].astype(str).str.lower()
        filtered = filtered.loc[
            ts_code_text.str.contains(normalized_keyword) | name_text.str.contains(normalized_keyword)
        ].copy()

    if scope == "只看 AI 精选":
        filtered = filtered.loc[filtered["is_overlay_selected"].fillna(False)].copy()
    elif scope == "只看最新推理池":
        filtered = filtered.loc[filtered["is_inference_overlay_selected"].fillna(False)].copy()
    elif scope == "只看浮亏较大":
        filtered = filtered.loc[pd.to_numeric(filtered["unrealized_pnl_pct"], errors="coerce") <= -0.1].copy()

    sort_map = {
        "最新推理排名": ("inference_ensemble_rank", True),
        "历史验证排名": ("ensemble_rank", True),
        "浮亏比例": ("unrealized_pnl_pct", True),
        "参考市值": ("market_value", False),
    }
    sort_column, ascending = sort_map.get(sort_by, ("inference_ensemble_rank", True))
    if sort_column in filtered.columns:
        filtered = filtered.assign(_sort_value=pd.to_numeric(filtered[sort_column], errors="coerce"))
        filtered = filtered.sort_values("_sort_value", ascending=ascending, na_position="last").drop(columns="_sort_value")
    return filtered.reset_index(drop=True)


def build_reduce_plan(row: pd.Series) -> pd.DataFrame:
    if pd.isna(row.get("mark_price")) or pd.isna(row.get("cost_basis")) or pd.isna(row.get("shares")):
        return pd.DataFrame()

    mark_price = float(row["mark_price"])
    cost_basis = float(row["cost_basis"])
    shares = int(row["shares"])

    stages = [
        ("阶段一", float(row.get("halfway_recovery_price") or 0.0), 0.30, "先看修复是否延续"),
        ("阶段二", float(row.get("breakeven_price") or 0.0), 0.40, "接近回本区，观察抛压"),
        ("阶段三", round(cost_basis * 1.065, 2), 0.30, "明显超预期时再看"),
    ]

    plan_rows: list[dict[str, object]] = []
    for label, target_price, ratio, note in stages:
        raw_shares = shares * ratio
        target_shares = int(round(raw_shares / 100.0) * 100) if shares >= 100 else int(round(raw_shares))
        target_shares = max(target_shares, 0)
        distance_pct = (target_price / mark_price - 1.0) if mark_price else None
        realized_pnl = (target_price - cost_basis) * target_shares if target_shares else 0.0
        plan_rows.append(
            {
                "plan_stage": label,
                "target_price": target_price,
                "reduce_ratio": ratio,
                "target_shares": target_shares,
                "distance_from_mark_pct": distance_pct,
                "estimated_realized_pnl": realized_pnl,
                "plan_note": note,
            }
        )

    return pd.DataFrame(plan_rows)
