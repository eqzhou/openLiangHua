from __future__ import annotations

import unittest
import uuid
from unittest.mock import Mock, patch

import pandas as pd


class ResearchPanelRepositoryTests(unittest.TestCase):
    def test_merge_feature_and_label_frames_builds_wide_panel_rows(self) -> None:
        from src.app.repositories.research_panel_repository import merge_feature_and_label_frames

        feature_frame = pd.DataFrame(
            [
                {
                    "trade_date": "2026-04-17",
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "industry": "J 金融业",
                    "index_code": "000905.SH",
                    "is_current_name_st": False,
                    "is_index_member": True,
                    "days_since_list": 1000,
                    "mom_5": 0.12,
                }
            ]
        )
        label_frame = pd.DataFrame(
            [
                {
                    "trade_date": "2026-04-17",
                    "ts_code": "000001.SZ",
                    "can_enter_next_day": True,
                    "label_valid_t10": False,
                    "ret_t1_t10": None,
                }
            ]
        )

        panel = merge_feature_and_label_frames(
            data_source="tushare",
            run_id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
            feature_frame=feature_frame,
            label_frame=label_frame,
        )

        self.assertEqual(len(panel), 1)
        self.assertEqual(panel.iloc[0]["data_source"], "tushare")
        self.assertEqual(str(panel.iloc[0]["ts_code"]), "000001.SZ")
        self.assertEqual(float(panel.iloc[0]["mom_5"]), 0.12)
        self.assertTrue(bool(panel.iloc[0]["can_enter_next_day"]))
        self.assertFalse(bool(panel.iloc[0]["label_valid_t10"]))

    def test_build_panel_run_payload_summarizes_frame_contract(self) -> None:
        from src.app.repositories.research_panel_repository import build_panel_run_payload

        panel_frame = pd.DataFrame(
            [
                {"trade_date": pd.Timestamp("2026-04-16"), "ts_code": "000001.SZ", "mom_5": 0.1, "ret_t1_t10": 0.02},
                {"trade_date": pd.Timestamp("2026-04-17"), "ts_code": "000002.SZ", "mom_5": 0.2, "ret_t1_t10": None},
            ]
        )

        payload = build_panel_run_payload(
            run_id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
            data_source="tushare",
            status="succeeded",
            panel_frame=panel_frame,
            feature_columns=["mom_5"],
            label_columns=["ret_t1_t10"],
            message="ok",
        )

        self.assertEqual(payload["data_source"], "tushare")
        self.assertEqual(payload["status"], "succeeded")
        self.assertEqual(payload["row_count"], 2)
        self.assertEqual(payload["symbol_count"], 2)
        self.assertEqual(payload["date_min"], "2026-04-16")
        self.assertEqual(payload["date_max"], "2026-04-17")
        self.assertEqual(payload["feature_columns"], ["mom_5"])
        self.assertEqual(payload["label_columns"], ["ret_t1_t10"])

    def test_load_feature_and_label_frames_from_research_panel_projection(self) -> None:
        from src.app.repositories.research_panel_repository import (
            load_feature_frame_from_research_panel,
            load_label_frame_from_research_panel,
        )

        panel_frame = pd.DataFrame(
            [
                {
                    "data_source": "tushare",
                    "trade_date": "2026-04-17",
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "industry": "J 金融业",
                    "index_code": "000905.SH",
                    "is_current_name_st": False,
                    "is_index_member": True,
                    "days_since_list": 1000,
                    "mom_5": 0.12,
                    "can_enter_next_day": True,
                    "ret_t1_t10": None,
                }
            ]
        )

        from unittest.mock import patch

        with patch("src.app.repositories.research_panel_repository.load_research_panel", return_value=panel_frame):
            feature_frame = load_feature_frame_from_research_panel(data_source="tushare")
            label_frame = load_label_frame_from_research_panel(data_source="tushare")

        self.assertIn("mom_5", feature_frame.columns)
        self.assertNotIn("ret_t1_t10", feature_frame.columns)
        self.assertIn("ret_t1_t10", label_frame.columns)
        self.assertNotIn("mom_5", label_frame.columns)

    def test_delete_research_panel_source_removes_rows_by_data_source(self) -> None:
        from src.app.repositories.research_panel_repository import delete_research_panel_source

        fake_cursor = Mock()
        fake_cursor.rowcount = 42
        fake_cursor.__enter__ = Mock(return_value=fake_cursor)
        fake_cursor.__exit__ = Mock(return_value=None)

        fake_connection = Mock()
        fake_connection.cursor.return_value = fake_cursor
        fake_connection.__enter__ = Mock(return_value=fake_connection)
        fake_connection.__exit__ = Mock(return_value=None)

        with (
            patch("src.app.repositories.research_panel_repository.ensure_research_panel_schema") as ensure_schema,
            patch("src.app.repositories.research_panel_repository.connect_database", return_value=fake_connection),
        ):
            deleted_rows = delete_research_panel_source(data_source="tushare")

        ensure_schema.assert_called_once()
        fake_cursor.execute.assert_called_once()
        sql, params = fake_cursor.execute.call_args.args
        self.assertIn("delete from research.panel", sql.lower())
        self.assertEqual(params, ("tushare",))
        fake_connection.commit.assert_called_once()
        self.assertEqual(deleted_rows, 42)


if __name__ == "__main__":
    unittest.main()
