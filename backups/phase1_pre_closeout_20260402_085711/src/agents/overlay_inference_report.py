from __future__ import annotations

import json

import pandas as pd

from src.agents.llm_bridge import export_llm_requests
from src.agents.overlay_report import DISPLAY_COLUMNS, _overlay_config, build_overlay_report_from_frames
from src.models.latest_inference import generate_latest_inference
from src.utils.data_source import active_data_source, source_or_canonical_path, source_prefixed_path
from src.utils.io import ensure_dir, load_yaml, project_root, save_text
from src.utils.logger import configure_logging

logger = configure_logging()


def _load_prediction_frame(root, data_source: str, filename: str) -> pd.DataFrame:
    path = source_or_canonical_path(root / "reports" / "weekly", filename, data_source)
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    if "trade_date" in frame.columns:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


def run() -> None:
    root = project_root()
    reports_dir = ensure_dir(root / "reports" / "weekly")
    experiment = load_yaml(root / "config" / "experiment.yaml")
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
    csv_source_path = source_prefixed_path(reports_dir, "overlay_inference_candidates.csv", data_source)
    json_source_path = source_prefixed_path(reports_dir, "overlay_inference_packet.json", data_source)
    markdown_source_path = source_prefixed_path(reports_dir, "overlay_inference_brief.md", data_source)

    export_frame.to_csv(csv_source_path, index=False, encoding="utf-8-sig")
    export_frame.to_csv(reports_dir / "overlay_inference_candidates.csv", index=False, encoding="utf-8-sig")
    json_source_path.write_text(json.dumps(packet, ensure_ascii=False, indent=2), encoding="utf-8")
    (reports_dir / "overlay_inference_packet.json").write_text(
        json.dumps(packet, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    save_text(markdown, markdown_source_path)
    save_text(markdown, reports_dir / "overlay_inference_brief.md")

    llm_artifacts = export_llm_requests(
        packet=packet,
        reports_dir=reports_dir,
        data_source=data_source,
        output_prefix="overlay_inference_llm",
    )
    packet["llm_bridge"] = llm_artifacts
    json_source_path.write_text(json.dumps(packet, ensure_ascii=False, indent=2), encoding="utf-8")
    (reports_dir / "overlay_inference_packet.json").write_text(
        json.dumps(packet, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"Saved overlay inference reports to {reports_dir}")


if __name__ == "__main__":
    run()
