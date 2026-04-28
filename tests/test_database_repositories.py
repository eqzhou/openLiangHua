from __future__ import annotations

import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import pandas as pd

from src.app.repositories import config_repository, holding_repository, report_repository
from src.db.dashboard_artifact_keys import (
    overlay_llm_response_summary_artifact_key,
    overlay_llm_responses_artifact_key,
    user_note_artifact_key,
    user_watchlist_artifact_key,
)
from src.db.dashboard_artifact_store import DashboardArtifact


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


class DatabaseRepositoryTests(unittest.TestCase):
    def test_build_watchlist_snapshot_uses_global_historical_and_user_inference_artifacts(self) -> None:
        from src.db import dashboard_sync

        with (
            patch("src.db.dashboard_sync.load_daily_bar", return_value=pd.DataFrame()),
            patch("src.db.dashboard_sync.load_predictions", return_value=pd.DataFrame()) as prediction_loader,
            patch("src.db.dashboard_sync.load_overlay_candidates", return_value=pd.DataFrame()) as historical_overlay_loader,
            patch("src.db.dashboard_sync.load_overlay_inference_candidates", return_value=pd.DataFrame()) as inference_overlay_loader,
            patch("src.db.dashboard_sync.build_watchlist_view", return_value=pd.DataFrame()),
        ):
            dashboard_sync._build_watchlist_snapshot(
                root=Path("/repo"),
                data_source="tushare",
                watchlist_config={"holdings": [], "focus_pool": [{"ts_code": "000001.SZ"}]},
                user_id="user-1",
            )

        self.assertNotIn("user_id", historical_overlay_loader.call_args.kwargs)
        self.assertEqual(inference_overlay_loader.call_args.kwargs["user_id"], "user-1")
        inference_prediction_calls = [
            call
            for call in prediction_loader.call_args_list
            if call.kwargs.get("model_name") == "ensemble" and call.kwargs.get("split_name") == "inference"
        ]
        self.assertEqual(inference_prediction_calls[0].kwargs["user_id"], "user-1")

    def test_load_experiment_config_prefers_database_payload(self) -> None:
        original = config_repository._load_config_from_database
        try:
            config_repository._load_config_from_database = lambda name: {"label_col": "ret_t1_t5", "top_n": 8} if name == "experiment" else None

            payload = config_repository.load_experiment_config(prefer_database=True)

            self.assertEqual(payload["label_col"], "ret_t1_t5")
            self.assertEqual(payload["top_n"], 8)
        finally:
            config_repository._load_config_from_database = original

    def test_load_daily_bar_prefers_database_binary_artifact(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-02", "ts_code": "000001.SZ", "close": 12.3},
                {"trade_date": "2026-04-03", "ts_code": "000001.SZ", "close": 12.6},
            ]
        )
        artifact = DashboardArtifact(
            artifact_key="akshare:binary:daily_bar",
            data_source="akshare",
            artifact_kind="parquet",
            payload_json=None,
            payload_text=None,
            payload_bytes=_parquet_bytes(frame),
            metadata_json={},
        )
        original = report_repository._artifact_or_none
        try:
            report_repository._artifact_or_none = lambda key: artifact if key == "akshare:binary:daily_bar" else None

            loaded = report_repository.load_daily_bar(data_source="akshare", prefer_database=True)

            self.assertEqual(len(loaded), 2)
            self.assertEqual(loaded["ts_code"].tolist(), ["000001.SZ", "000001.SZ"])
            self.assertTrue(pd.api.types.is_datetime64_any_dtype(loaded["trade_date"]))
        finally:
            report_repository._artifact_or_none = original

    def test_load_feature_panel_falls_back_to_file_when_database_missing(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-02", "ts_code": "000001.SZ", "mom_5": 0.12},
            ]
        )
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            feature_dir = root / "data" / "features"
            feature_dir.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(feature_dir / "akshare_feature_panel.parquet", index=False)

            original = report_repository._artifact_or_none
            try:
                report_repository._artifact_or_none = lambda key: None
                loaded = report_repository.load_feature_panel(root=root, data_source="akshare", prefer_database=True)
            finally:
                report_repository._artifact_or_none = original

            self.assertEqual(len(loaded), 1)
            self.assertEqual(str(loaded.iloc[0]["ts_code"]), "000001.SZ")

    def test_load_feature_history_for_symbol_prefers_scoped_research_panel_query(self) -> None:
        frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-02", "ts_code": "000001.SZ", "mom_20": 0.12},
                {"trade_date": "2026-04-03", "ts_code": "000001.SZ", "mom_20": 0.18},
            ]
        )

        with patch("src.app.repositories.report_repository.load_research_panel", return_value=frame) as load_research_panel:
            loaded = report_repository.load_feature_history_for_symbol(data_source="akshare", symbol="000001.SZ", factor_name="mom_20")

        self.assertEqual(len(loaded), 2)
        self.assertEqual(load_research_panel.call_args.kwargs["columns"], ["trade_date", "ts_code", "mom_20"])
        self.assertEqual(load_research_panel.call_args.kwargs["symbols"], ["000001.SZ"])

    def test_load_prediction_history_for_symbol_prefers_database_filtered_rows(self) -> None:
        fake_store = Mock()
        fake_store.get_projected_json_records.return_value = [
            {"trade_date": "2026-04-02", "ts_code": "000001.SZ", "score": 0.8},
            {"trade_date": "2026-04-03", "ts_code": "000001.SZ", "score": 0.9},
        ]

        with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
            loaded = report_repository.load_prediction_history_for_symbol(
                data_source="akshare",
                model_name="lgbm",
                split_name="test",
                symbol="000001.SZ",
            )

        self.assertEqual(len(loaded), 2)
        self.assertEqual(fake_store.get_projected_json_records.call_args.kwargs["filter_field_name"], "ts_code")
        self.assertEqual(fake_store.get_projected_json_records.call_args.kwargs["filter_field_value"], "000001.SZ")
        self.assertEqual(fake_store.get_projected_json_records.call_args.kwargs["field_names"], ["trade_date", "ts_code", "score", "ret_t1_t10"])
        self.assertEqual(fake_store.get_projected_json_records.call_args.kwargs["limit"], 240)

    def test_load_overlay_candidate_summary_records_prefers_database_projection(self) -> None:
        fake_store = Mock()
        fake_store.get_projected_json_records.return_value = [
            {"ts_code": "000001.SZ", "name": "示例", "trade_date": "2026-04-03"},
        ]

        with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
            loaded = report_repository.load_overlay_candidate_summary_records(
                data_source="akshare",
                scope="inference",
                field_names=["ts_code", "name", "trade_date"],
            )

        self.assertEqual(len(loaded), 1)
        self.assertEqual(fake_store.get_projected_json_records.call_args.kwargs["field_names"], ["ts_code", "name", "trade_date"])

    def test_load_watchlist_record_prefers_database_filtered_row(self) -> None:
        fake_store = Mock()
        fake_store.get_json_records_by_field.return_value = [
            {"ts_code": "000001.SZ", "name": "示例持仓", "mark_price": 10.5},
        ]

        with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
            loaded = report_repository.load_watchlist_record(
                data_source="akshare",
                symbol="000001.SZ",
            )

        self.assertEqual(loaded["ts_code"], "000001.SZ")
        self.assertEqual(fake_store.get_json_records_by_field.call_args.kwargs["field_value"], "000001.SZ")

    def test_load_watchlist_record_uses_user_scoped_artifact_key(self) -> None:
        fake_store = Mock()
        fake_store.get_json_records_by_field.return_value = [
            {"ts_code": "000001.SZ", "name": "用户持仓", "mark_price": 10.5},
        ]

        with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
            loaded = report_repository.load_watchlist_record(
                data_source="akshare",
                symbol="000001.SZ",
                user_id="user-1",
            )

        self.assertEqual(loaded["name"], "用户持仓")
        self.assertEqual(
            fake_store.get_json_records_by_field.call_args.kwargs["artifact_key"],
            user_watchlist_artifact_key("akshare", "user-1"),
        )

    def test_save_model_split_reports_uses_user_scoped_prediction_artifacts(self) -> None:
        from src.db.dashboard_artifact_keys import user_table_artifact_key

        fake_store = Mock()
        predictions = pd.DataFrame(
            [{"trade_date": "2026-04-28", "ts_code": "000001.SZ", "score": 0.8}]
        )

        with (
            patch("src.app.repositories.report_repository._uses_primary_project_root", return_value=True),
            patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store),
        ):
            report_repository.save_model_split_reports(
                data_source="akshare",
                model_name="ensemble",
                split_name="inference",
                predictions=predictions,
                portfolio=pd.DataFrame(),
                metrics={},
                user_id="user-1",
            )

        self.assertEqual(
            fake_store.upsert_bytes.call_args.kwargs["artifact_key"],
            user_table_artifact_key("akshare", "predictions:ensemble:inference", "user-1"),
        )
        self.assertEqual(fake_store.upsert_bytes.call_args.kwargs["metadata"]["user_id"], "user-1")

    def test_load_latest_symbol_markdown_uses_user_scoped_note_artifact(self) -> None:
        artifact = DashboardArtifact(
            artifact_key=user_note_artifact_key("akshare", "000001.SZ", "watch_plan", "user-1"),
            data_source="akshare",
            artifact_kind="note",
            payload_json=None,
            payload_text="user note",
            payload_bytes=None,
            metadata_json={"plan_date": "2026-04-28", "name": "user-note.md"},
        )
        original = report_repository._artifact_or_none
        requested_keys: list[str] = []
        try:
            def fake_artifact_lookup(key: str):
                requested_keys.append(key)
                return artifact if key == artifact.artifact_key else None

            report_repository._artifact_or_none = fake_artifact_lookup
            loaded = report_repository.load_latest_symbol_markdown(
                "000001.SZ",
                "watch_plan",
                data_source="akshare",
                user_id="user-1",
            )
        finally:
            report_repository._artifact_or_none = original

        self.assertEqual(loaded["content"], "user note")
        self.assertEqual(requested_keys, [user_note_artifact_key("akshare", "000001.SZ", "watch_plan", "user-1")])

    def test_save_symbol_note_uses_user_scoped_note_artifact(self) -> None:
        fake_store = Mock()

        with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
            report_repository.save_symbol_note(
                data_source="akshare",
                symbol="000001.SZ",
                note_kind="action_memo",
                plan_date="2026-04-28",
                content="user memo",
                user_id="user-1",
            )

        self.assertEqual(
            fake_store.upsert_text.call_args.kwargs["artifact_key"],
            user_note_artifact_key("akshare", "000001.SZ", "action_memo", "user-1"),
        )
        self.assertEqual(fake_store.upsert_text.call_args.kwargs["metadata"]["user_id"], "user-1")

    def test_load_overlay_llm_bundle_prefers_database_text_artifacts(self) -> None:
        response_artifact = DashboardArtifact(
            artifact_key=overlay_llm_responses_artifact_key("akshare", "historical"),
            data_source="akshare",
            artifact_kind="jsonl",
            payload_json=None,
            payload_text='{"custom_id":"000001.SZ","status":"success","output_text":"database answer"}',
            payload_bytes=None,
            metadata_json={},
        )
        summary_artifact = DashboardArtifact(
            artifact_key=overlay_llm_response_summary_artifact_key("akshare", "historical"),
            data_source="akshare",
            artifact_kind="markdown",
            payload_json=None,
            payload_text="database summary",
            payload_bytes=None,
            metadata_json={},
        )
        original = report_repository._artifact_or_none
        try:
            report_repository._artifact_or_none = lambda key: (
                response_artifact
                if key == overlay_llm_responses_artifact_key("akshare", "historical")
                else summary_artifact
                if key == overlay_llm_response_summary_artifact_key("akshare", "historical")
                else None
            )

            payload = report_repository.load_overlay_llm_bundle(
                data_source="akshare",
                scope="historical",
                prefer_database=True,
            )
        finally:
            report_repository._artifact_or_none = original

        self.assertEqual(payload["response_summary"], "database summary")
        self.assertEqual(payload["response_lookup"]["000001.SZ"]["output_text"], "database answer")

    def test_holding_repository_prediction_snapshots_use_repository_frames(self) -> None:
        prediction_frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-02", "ts_code": "000001.SZ", "name": "示例", "score": 0.8, "mom_5": 0.1},
                {"trade_date": "2026-04-03", "ts_code": "000001.SZ", "name": "示例", "score": 0.9, "mom_5": 0.2},
                {"trade_date": "2026-04-03", "ts_code": "000002.SZ", "name": "备选", "score": 0.7, "mom_5": 0.1},
            ]
        )
        original = holding_repository.repo_load_predictions
        try:
            holding_repository.repo_load_predictions = lambda *args, **kwargs: prediction_frame.copy()

            snapshots = holding_repository.load_prediction_snapshots(data_source="akshare")
        finally:
            holding_repository.repo_load_predictions = original

        self.assertEqual(set(snapshots.keys()), {"ridge", "lgbm", "ensemble"})
        self.assertEqual(pd.Timestamp(snapshots["ensemble"]["trade_date"].iloc[0]), pd.Timestamp("2026-04-03"))
        self.assertEqual(snapshots["ensemble"]["ts_code"].tolist(), ["000001.SZ", "000002.SZ"])

    def test_save_overlay_outputs_writes_files_and_store(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidates = pd.DataFrame([{"ts_code": "000001.SZ", "name": "示例", "trade_date": "2026-04-03"}])
            packet = {"latest_date": "2026-04-03", "selected_candidates": [{"ts_code": "000001.SZ"}]}
            fake_store = Mock()

            with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
                output = report_repository.save_overlay_outputs(
                    root=root,
                    data_source="akshare",
                    scope="historical",
                    candidates=candidates,
                    packet=packet,
                    brief="测试纪要",
                )

            self.assertTrue(Path(output["csv_source_path"]).exists())
            self.assertEqual(Path(output["csv_source_path"]).name, "akshare_overlay_latest_candidates.csv")
            self.assertTrue(Path(output["packet_source_path"]).exists())
            self.assertEqual(Path(output["packet_source_path"]).name, "akshare_overlay_latest_packet.json")
            self.assertTrue(Path(output["brief_source_path"]).exists())
            self.assertEqual(Path(output["brief_source_path"]).name, "akshare_overlay_latest_brief.md")
            self.assertTrue(fake_store.upsert_json.called)
            self.assertTrue(fake_store.upsert_text.called)

    def test_save_overlay_outputs_keeps_source_prefixed_paths_when_canonical_exists(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports_dir = root / "reports" / "weekly"
            reports_dir.mkdir(parents=True, exist_ok=True)
            (reports_dir / "overlay_latest_candidates.csv").write_text("old", encoding="utf-8")
            (reports_dir / "overlay_latest_packet.json").write_text("{}", encoding="utf-8")
            (reports_dir / "overlay_latest_brief.md").write_text("old", encoding="utf-8")
            candidates = pd.DataFrame([{"ts_code": "000001.SZ", "name": "示例", "trade_date": "2026-04-03"}])
            packet = {"latest_date": "2026-04-03"}
            fake_store = Mock()

            with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
                output = report_repository.save_overlay_outputs(
                    root=root,
                    data_source="akshare",
                    scope="historical",
                    candidates=candidates,
                    packet=packet,
                    brief="测试纪要",
                )

            self.assertEqual(Path(output["csv_source_path"]).name, "akshare_overlay_latest_candidates.csv")
            self.assertEqual(Path(output["packet_source_path"]).name, "akshare_overlay_latest_packet.json")
            self.assertEqual(Path(output["brief_source_path"]).name, "akshare_overlay_latest_brief.md")

    def test_save_symbol_note_writes_file_and_store(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake_store = Mock()

            with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
                output_path = report_repository.save_symbol_note(
                    root=root,
                    data_source="akshare",
                    symbol="000001.SZ",
                    note_kind="watch_plan",
                    plan_date="2026-04-03",
                    content="测试盯盘清单",
                )

            self.assertTrue(output_path.exists())
            self.assertEqual(output_path.name, "000001_watch_plan_2026-04-03.md")
            self.assertTrue(fake_store.upsert_text.called)

    def test_save_llm_bridge_outputs_writes_files_and_store(self) -> None:
        with TemporaryDirectory() as temp_dir:
            reports_dir = Path(temp_dir) / "reports" / "weekly"
            fake_store = Mock()

            with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
                output = report_repository.save_llm_bridge_outputs(
                    reports_dir,
                    data_source="akshare",
                    output_prefix="overlay_llm",
                    request_jsonl_text='{"custom_id":"000001.SZ"}',
                    request_summary_text="request summary",
                    response_jsonl_text='{"custom_id":"000001.SZ","status":"success"}',
                    response_summary_text="response summary",
                )

            self.assertTrue(Path(output["jsonl_path"]).exists())
            self.assertTrue(Path(output["summary_path"]).exists())
            self.assertTrue(Path(output["response_jsonl_path"]).exists())
            self.assertTrue(Path(output["response_summary_path"]).exists())
            self.assertTrue(fake_store.upsert_text.called)

    def test_save_model_split_reports_writes_files_and_store(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake_store = Mock()
            predictions = pd.DataFrame([{"trade_date": "2026-04-03", "ts_code": "000001.SZ", "score": 0.9}])
            portfolio = pd.DataFrame([{"trade_date": "2026-04-03", "daily_return": 0.01}])
            diagnostics = {"yearly": pd.DataFrame([{"year": 2026, "return": 0.1}]), "regime": pd.DataFrame()}

            with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
                output = report_repository.save_model_split_reports(
                    root=root,
                    data_source="akshare",
                    model_name="ridge",
                    split_name="test",
                    predictions=predictions,
                    portfolio=portfolio,
                    metrics={"daily_portfolio_sharpe": 1.2},
                    diagnostics=diagnostics,
                )

            self.assertTrue(Path(output["prediction_path"]).exists())
            self.assertTrue(Path(output["portfolio_path"]).exists())
            self.assertTrue(Path(output["metrics_path"]).exists())
            self.assertTrue(Path(output["diagnostic_yearly_path"]).exists())
            self.assertTrue(fake_store.upsert_json.called)

    def test_save_inference_packet_writes_file_and_store(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake_store = Mock()

            with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
                output_path = report_repository.save_inference_packet(
                    root=root,
                    data_source="akshare",
                    payload={"latest_feature_date": "2026-04-03"},
                )

            self.assertTrue(output_path.exists())
            self.assertEqual(output_path.name, "akshare_inference_packet.json")
            self.assertTrue(fake_store.upsert_json.called)

    def test_save_binary_dataset_writes_parquet_and_store(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fake_store = Mock()
            frame = pd.DataFrame([{"trade_date": "2026-04-03", "ts_code": "000001.SZ"}])

            with patch("src.app.repositories.report_repository.get_dashboard_artifact_store", return_value=fake_store):
                output_path = report_repository.save_binary_dataset(
                    root=root,
                    data_source="akshare",
                    directory="data/features",
                    filename="feature_panel.parquet",
                    artifact_name="feature_panel",
                    frame=frame,
                )

            self.assertTrue(output_path.exists())
            self.assertEqual(output_path.name, "akshare_feature_panel.parquet")
            self.assertTrue(fake_store.upsert_bytes.called)


if __name__ == "__main__":
    unittest.main()
