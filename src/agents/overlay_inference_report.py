from __future__ import annotations

import pandas as pd

from src.agents.llm_bridge import export_llm_requests
from src.app.services.ai_shortlist_service import save_overlay_inference_shortlist
from src.agents.overlay_report import DISPLAY_COLUMNS, _overlay_config, _prefer_database, build_overlay_report_from_frames
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


def _load_prediction_frame(root, data_source: str, filename: str) -> pd.DataFrame:
    model_name = filename.replace("_inference_predictions.csv", "").strip()
    if model_name not in {"lgbm", "ridge", "ensemble"}:
        return pd.DataFrame()

    frame = repo_load_predictions(
        root,
        data_source=data_source,
        model_name=model_name,
        split_name="inference",
        prefer_database=_prefer_database(root) if root is not None else True,
    )
    if frame.empty:
        return frame
    frame = frame.copy()
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    return frame


def run() -> None:
    root = project_root()
    reports_dir = ensure_dir(root / "reports" / "weekly")
    experiment = load_experiment_config(root, prefer_database=True)
    data_source = active_data_source()
    overlay = _overlay_config(experiment)

    inference_packet = generate_latest_inference(root=root)
    lgbm = _load_prediction_frame(root, data_source, "lgbm_inference_predictions.csv")
    ridge = _load_prediction_frame(root, data_source, "ridge_inference_predictions.csv")
    overlay_candidates, packet, markdown = build_overlay_report_from_frames(
        root=root,
        data_source=data_source,
        overlay=overlay,
        split_name="inference",
        lgbm=lgbm,
        ridge=ridge,
        latest_risk_state=inference_packet.get("latest_risk_state", {}),
        prediction_mode="latest_unlabeled_inference",
    )
    packet["inference_packet"] = inference_packet

    export_frame = overlay_candidates[[column for column in DISPLAY_COLUMNS if column in overlay_candidates.columns]].copy()
    repo_save_overlay_outputs(
        root=root,
        data_source=data_source,
        scope="inference",
        candidates=export_frame,
        packet=packet,
        brief=markdown,
    )

    llm_artifacts = export_llm_requests(
        packet=packet,
        reports_dir=reports_dir,
        data_source=data_source,
        output_prefix="overlay_inference_llm",
    )
    packet["llm_bridge"] = llm_artifacts
    llm_bundle = load_overlay_llm_bundle(
        root=root,
        data_source=data_source,
        scope="inference",
        packet=packet,
        prefer_database=False,
    )
    shortlist_artifact = save_overlay_inference_shortlist(
        packet=packet,
        response_lookup=dict(llm_bundle.get("response_lookup", {}) or {}),
        root=root,
        data_source=data_source,
    )
    packet["shortlist"] = {
        "artifact_ref": shortlist_artifact["artifact_ref"],
        "row_count": len(shortlist_artifact["rows"]),
    }
    repo_save_overlay_outputs(
        root=root,
        data_source=data_source,
        scope="inference",
        candidates=export_frame,
        packet=packet,
        brief=markdown,
    )
    from src.db.dashboard_sync import sync_watchlist_snapshot_artifact

    summary = sync_watchlist_snapshot_artifact(root=root, data_source=data_source)
    logger.info(summary.message if summary.ok else f"Watchlist snapshot sync failed: {summary.message}")
    logger.info(f"Saved overlay inference reports to {reports_dir}")


if __name__ == "__main__":
    run()
