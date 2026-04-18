from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from src.agents.overlay_inference_report import _load_prediction_frame


class OverlayInferenceReportTests(unittest.TestCase):
    def test_load_prediction_frame_prefers_repository_contract(self) -> None:
        prediction_frame = pd.DataFrame(
            [
                {"trade_date": "2026-04-03", "ts_code": "000001.SZ", "score": 0.91},
                {"trade_date": "2026-04-03", "ts_code": "000002.SZ", "score": 0.82},
            ]
        )

        with patch("src.agents.overlay_inference_report.repo_load_predictions", return_value=prediction_frame.copy()):
            loaded = _load_prediction_frame(root=None, data_source="myquant", filename="lgbm_inference_predictions.csv")

        self.assertEqual(len(loaded), 2)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(loaded["trade_date"]))


if __name__ == "__main__":
    unittest.main()
