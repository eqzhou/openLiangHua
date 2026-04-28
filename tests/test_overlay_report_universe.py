from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import pandas as pd


class OverlayReportUniverseTests(unittest.TestCase):
    def test_load_overlay_symbol_universe_reads_watchlist_items_for_user(self) -> None:
        from src.agents.overlay_report import load_overlay_symbol_universe

        store = Mock()
        store.load_watchlist.return_value = {
            "holdings": [{"ts_code": "000001.SZ"}],
            "focus_pool": [{"ts_code": "000002.SZ"}, {"ts_code": "000001.SZ"}],
        }

        with (
            patch("src.agents.overlay_report.PostgresWatchlistStore", return_value=store),
            patch("src.agents.overlay_report.get_api_settings", return_value=Mock()),
        ):
            symbols = load_overlay_symbol_universe(user_id="user-1")

        store.load_watchlist.assert_called_once_with("user-1")
        self.assertEqual(symbols, ["000001.SZ", "000002.SZ"])

    def test_build_overlay_report_from_frames_uses_database_symbol_universe_instead_of_config_cap(self) -> None:
        from src.agents.overlay_report import build_overlay_report_from_frames

        lgbm = pd.DataFrame(
            [
                {"trade_date": "2026-04-28", "ts_code": "000001.SZ", "name": "一号", "industry": "银行", "score": 0.9, "mom_20": 0.1, "mom_60": 0.2, "drawdown_60": -0.01, "vol_20": 0.02, "amount_20": 100},
                {"trade_date": "2026-04-28", "ts_code": "000002.SZ", "name": "二号", "industry": "科技", "score": 0.8, "mom_20": 0.2, "mom_60": 0.3, "drawdown_60": -0.02, "vol_20": 0.03, "amount_20": 200},
                {"trade_date": "2026-04-28", "ts_code": "000003.SZ", "name": "三号", "industry": "消费", "score": 0.7, "mom_20": 0.3, "mom_60": 0.4, "drawdown_60": -0.03, "vol_20": 0.04, "amount_20": 300},
            ]
        )
        ridge = pd.DataFrame(
            [
                {"trade_date": "2026-04-28", "ts_code": "000001.SZ", "score": 0.3},
                {"trade_date": "2026-04-28", "ts_code": "000002.SZ", "score": 0.2},
                {"trade_date": "2026-04-28", "ts_code": "000003.SZ", "score": 0.1},
            ]
        )
        overlay = {
            "candidate_pool_size": 1,
            "top_n": 1,
            "lgbm_weight": 0.5,
            "ridge_weight": 0.5,
            "quant_weight": 0.6,
            "factor_weight": 0.3,
            "consensus_weight": 0.1,
            "notice_lookback_days": 30,
            "notice_max_items": 1,
            "news_lookback_days": 30,
            "news_max_items": 1,
            "research_lookback_days": 30,
            "research_max_items": 1,
        }

        with (
            patch("src.agents.overlay_report.ensure_dir"),
            patch("src.agents.overlay_report._load_industry_name_map", return_value={}),
            patch("src.agents.overlay_report.resolve_model_weights", return_value={"weights": {"lgbm": 0.5, "ridge": 0.5}}),
            patch(
                "src.agents.overlay_report.build_event_context",
                return_value=pd.DataFrame(
                    [
                        {"ts_code": "000001.SZ", "notice_digest": "", "news_digest": "", "news_source": "", "research_digest": ""},
                        {"ts_code": "000002.SZ", "notice_digest": "", "news_digest": "", "news_source": "", "research_digest": ""},
                    ]
                ),
            ),
        ):
            candidates, packet, _ = build_overlay_report_from_frames(
                root=__import__("pathlib").Path("/tmp/openlianghua-test"),
                data_source="tushare",
                overlay=overlay,
                split_name="inference",
                lgbm=lgbm,
                ridge=ridge,
                prediction_mode="latest_unlabeled_inference",
                candidate_symbols=["000001.SZ", "000002.SZ"],
                candidate_universe_source="watchlist_items:user-1",
            )

        self.assertEqual(set(candidates["ts_code"].tolist()), {"000001.SZ", "000002.SZ"})
        self.assertEqual(packet["candidate_pool_size"], 2)
        self.assertEqual(packet["candidate_universe_source"], "watchlist_items:user-1")

    def test_historical_overlay_report_does_not_use_user_watchlist_universe(self) -> None:
        from src.agents.overlay_report import build_overlay_report

        with (
            patch("src.agents.overlay_report.project_root", return_value=__import__("pathlib").Path("/tmp/openlianghua-test")),
            patch("src.agents.overlay_report.load_experiment_config", return_value={"overlay": {"split": "test", "candidate_pool_size": 30, "top_n": 10}}),
            patch("src.agents.overlay_report.active_data_source", return_value="tushare"),
            patch("src.agents.overlay_report._load_predictions", return_value=pd.DataFrame([{"ts_code": "000001.SZ"}])),
            patch("src.agents.overlay_report._load_portfolio", return_value=pd.DataFrame()),
            patch("src.agents.overlay_report._latest_risk_state", return_value={}),
            patch("src.agents.overlay_report.load_overlay_symbol_universe", side_effect=AssertionError("historical overlay must stay global")),
            patch(
                "src.agents.overlay_report.build_overlay_report_from_frames",
                return_value=(pd.DataFrame(), {"candidate_universe_source": "model_predictions"}, ""),
            ) as build_from_frames,
        ):
            build_overlay_report()

        self.assertEqual(build_from_frames.call_args.kwargs["candidate_universe_source"], "model_predictions")
        self.assertNotIn("candidate_symbols", build_from_frames.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()
