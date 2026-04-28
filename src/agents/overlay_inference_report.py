from __future__ import annotations

import pandas as pd

from src.agents.llm_bridge import export_llm_requests
from src.app.services.ai_shortlist_service import save_overlay_inference_shortlist
from src.agents.overlay_report import (
    DISPLAY_COLUMNS,
    _overlay_config,
    _prefer_database,
    build_overlay_report_from_frames,
    load_overlay_symbol_universe,
    resolve_overlay_user_id,
)
from src.app.repositories.config_repository import load_experiment_config
from src.app.repositories.report_repository import (
    load_overlay_llm_bundle,
    load_predictions as repo_load_predictions,
    save_overlay_outputs as repo_save_overlay_outputs,
)
from src.models.latest_inference import generate_latest_inference
from src.utils.data_source import active_data_source
from src.utils.io import ensure_dir, project_root
from src.utils.logger import configure_logging

logger = configure_logging()


def _load_prediction_frame(root, data_source: str, filename: str, *, user_id: str | None = None) -> pd.DataFrame:
    model_name = filename.replace("_inference_predictions.csv", "").strip()
    if model_name not in {"lgbm", "ridge", "ensemble"}:
        return pd.DataFrame()

    frame = repo_load_predictions(
        root,
        data_source=data_source,
        model_name=model_name,
        split_name="inference",
        prefer_database=_prefer_database(root) if root is not None else True,
        user_id=user_id,
    )
    if frame.empty:
        return frame
    frame = frame.copy()
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def generate_overlay_inference_report(
    root=None,
    *,
    execute_llm: bool = True,
    user_id: str | None = None,
) -> dict[str, object]:
    resolved_root = root or project_root()
    reports_dir = ensure_dir(resolved_root / "reports" / "weekly")
    experiment = load_experiment_config(resolved_root, prefer_database=True)
    data_source = active_data_source()
    overlay = _overlay_config(experiment)

    overlay_user_id = str(user_id or resolve_overlay_user_id()).strip()
    inference_packet = generate_latest_inference(root=resolved_root, user_id=overlay_user_id)
    lgbm = _load_prediction_frame(resolved_root, data_source, "lgbm_inference_predictions.csv", user_id=overlay_user_id)
    ridge = _load_prediction_frame(resolved_root, data_source, "ridge_inference_predictions.csv", user_id=overlay_user_id)
    candidate_symbols = load_overlay_symbol_universe(overlay_user_id)
    overlay_candidates, packet, markdown = build_overlay_report_from_frames(
        root=resolved_root,
        data_source=data_source,
        overlay=overlay,
        split_name="inference",
        lgbm=lgbm,
        ridge=ridge,
        latest_risk_state=inference_packet.get("latest_risk_state", {}),
        prediction_mode="latest_unlabeled_inference",
        candidate_symbols=candidate_symbols,
        candidate_universe_source=f"watchlist_items:{overlay_user_id}" if candidate_symbols else "model_predictions",
    )
    packet["inference_packet"] = inference_packet

    export_frame = overlay_candidates[[column for column in DISPLAY_COLUMNS if column in overlay_candidates.columns]].copy()
    repo_save_overlay_outputs(
        root=resolved_root,
        data_source=data_source,
        scope="inference",
        candidates=export_frame,
        packet=packet,
        brief=markdown,
        user_id=overlay_user_id,
    )

    if execute_llm:
        llm_artifacts = export_llm_requests(
            packet=packet,
            reports_dir=reports_dir,
            data_source=data_source,
            output_prefix="overlay_inference_llm",
            user_id=overlay_user_id,
        )
        packet["llm_bridge"] = llm_artifacts
        llm_bundle = load_overlay_llm_bundle(
            root=resolved_root,
            data_source=data_source,
            scope="inference",
            packet=packet,
            user_id=overlay_user_id,
            prefer_database=False,
        )
        shortlist_artifact = save_overlay_inference_shortlist(
            packet=packet,
            response_lookup=dict(llm_bundle.get("response_lookup", {}) or {}),
            root=resolved_root,
            data_source=data_source,
            user_id=overlay_user_id,
        )
        packet["shortlist"] = {
            "artifact_ref": shortlist_artifact["artifact_ref"],
            "row_count": len(shortlist_artifact["rows"]),
        }
        repo_save_overlay_outputs(
            root=resolved_root,
            data_source=data_source,
            scope="inference",
            candidates=export_frame,
            packet=packet,
            brief=markdown,
            user_id=overlay_user_id,
        )

    from src.db.dashboard_sync import sync_watchlist_snapshot_artifact

    summary = sync_watchlist_snapshot_artifact(root=resolved_root, data_source=data_source, user_id=overlay_user_id)
    logger.info(summary.message if summary.ok else f"Watchlist snapshot sync failed: {summary.message}")
    logger.info(f"Saved overlay inference reports to {reports_dir}")
    return {
        "data_source": data_source,
        "latest_feature_date": inference_packet.get("latest_feature_date"),
        "inference_universe_size": inference_packet.get("inference_universe_size", 0),
        "candidate_count": int(len(export_frame)),
        "watchlist_snapshot_ok": bool(summary.ok),
        "watchlist_snapshot_message": summary.message,
    }


def run() -> None:
    generate_overlay_inference_report()


if __name__ == "__main__":
    run()
