from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.app.facades.dashboard_facade import (
    apply_realtime_to_watchlist_payload,
    get_ai_review_detail_payload,
    get_ai_review_summary_payload,
    get_candidate_history_payload,
    get_candidates_summary_payload,
    get_data_management_payload,
    get_factor_explorer_detail_payload,
    get_factor_explorer_summary_payload,
    get_home_payload,
    get_overview_payload,
    refresh_realtime_payload,
    run_tushare_full_refresh_payload,
    run_tushare_incremental_refresh_payload,
    get_service_payload,
    get_shell_payload,
    get_watchlist_detail_payload,
    get_watchlist_payload,
    get_watchlist_summary_payload,
)


class DashboardFacadeTests(unittest.TestCase):
    def test_data_management_payload_passes_through_status_contract(self) -> None:
        with patch(
            "src.app.facades.dashboard_facade.build_data_management_payload",
            return_value={
                "targetSource": "akshare",
                "tokenConfigured": True,
                "dailyBar": {"latestTradeDate": "2026-04-16"},
                "featurePanel": {"latestTradeDate": "2026-04-16"},
                "labelPanel": {"latestTradeDate": "2026-04-16"},
                "scripts": {"incremental": "scripts/refresh_daily_bar_tushare.ps1", "fullRefresh": "scripts/refresh_full_pipeline_tushare.ps1"},
            },
        ):
            payload = get_data_management_payload()

        self.assertEqual(payload["targetSource"], "akshare")
        self.assertTrue(payload["tokenConfigured"])
        self.assertIn("dailyBar", payload)
        self.assertIn("scripts", payload)

    def test_run_tushare_incremental_refresh_payload_clears_dashboard_caches(self) -> None:
        with (
            patch(
                "src.app.facades.dashboard_facade.run_tushare_incremental_refresh",
                return_value={
                    "target_source": "akshare",
                    "latest_trade_date": "2026-04-16",
                    "appended_rows": 116,
                },
            ),
            patch("src.app.facades.dashboard_facade.clear_dashboard_caches") as clear_caches,
        ):
            payload = run_tushare_incremental_refresh_payload(target_source="akshare", end_date="2026-04-16")

        self.assertEqual(payload["actionName"], "tushare_incremental_refresh")
        self.assertTrue(payload["ok"])
        self.assertIn("2026-04-16", payload["output"])
        clear_caches.assert_called_once()

    def test_run_tushare_full_refresh_payload_clears_dashboard_caches(self) -> None:
        with (
            patch(
                "src.app.facades.dashboard_facade.run_tushare_full_refresh",
                return_value={
                    "ok": True,
                    "target_source": "akshare",
                    "incremental": {"latest_trade_date": "2026-04-16"},
                    "features": {"feature_rows": 1200, "label_rows": 1180},
                    "dashboardSync": {"message": "dashboard synced"},
                },
            ),
            patch("src.app.facades.dashboard_facade.clear_dashboard_caches") as clear_caches,
        ):
            payload = run_tushare_full_refresh_payload(target_source="akshare", end_date="2026-04-16")

        self.assertEqual(payload["actionName"], "tushare_full_refresh")
        self.assertTrue(payload["ok"])
        self.assertIn("1200", payload["output"])
        clear_caches.assert_called_once()

    def test_overview_payload_exposes_selected_split_and_tables(self) -> None:
        payload = get_overview_payload("test")

        self.assertEqual(payload["selectedSplit"], "test")
        self.assertIn("summary", payload)
        self.assertIn("comparison", payload)

    def test_shell_payload_exposes_sidebar_contract(self) -> None:
        payload = get_shell_payload()

        self.assertIn("bootstrap", payload)
        self.assertIn("experimentConfig", payload)
        self.assertIn("service", payload)
        self.assertIn("watchlistEntryCount", payload)
        self.assertIn("configSummaryText", payload)

    def test_home_payload_exposes_operator_sections(self) -> None:
        payload = get_home_payload()

        self.assertIn("service", payload)
        self.assertIn("overview", payload)
        self.assertIn("watchlist", payload)
        self.assertIn("candidates", payload)
        self.assertIn("aiReview", payload)
        self.assertIn("shortlistMarkdown", payload["aiReview"])
        self.assertIn("alerts", payload)

    def test_candidates_summary_payload_prefers_precomputed_snapshot(self) -> None:
        candidate_snapshot = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "name": "示例股票", "trade_date": pd.Timestamp("2026-04-03"), "score": 0.9, "rank": 1},
                {"ts_code": "000002.SZ", "name": "后备股票", "trade_date": pd.Timestamp("2026-04-03"), "score": 0.8, "rank": 2},
            ]
        )

        with (
            patch("src.app.facades.dashboard_facade.build_candidate_snapshot", return_value=candidate_snapshot),
            patch("src.app.facades.dashboard_facade.load_predictions", side_effect=AssertionError("should not load full predictions")),
        ):
            payload = get_candidates_summary_payload(model_name="ensemble", split_name="test", top_n=1)

        self.assertEqual(payload["modelName"], "ensemble")
        self.assertEqual(payload["latestDate"], "2026-04-03T00:00:00")
        self.assertEqual(len(payload["latestPicks"]), 1)
        self.assertEqual(payload["selectedSymbol"], "000001.SZ")

    def test_candidate_history_payload_returns_selected_symbol_history(self) -> None:
        predictions = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": "2026-04-01", "score": 0.7, "ret_t1_t10": 0.02},
                {"ts_code": "000001.SZ", "trade_date": "2026-04-02", "score": 0.8, "ret_t1_t10": 0.03},
            ]
        )

        with patch("src.app.facades.dashboard_facade.load_predictions", return_value=predictions):
            payload = get_candidate_history_payload(model_name="ensemble", split_name="test", symbol="000001.SZ")

        self.assertEqual(payload["selectedSymbol"], "000001.SZ")
        self.assertEqual(len(payload["scoreHistory"]), 2)

    def test_ai_review_summary_payload_keeps_candidates_and_selected_record(self) -> None:
        inference_candidates = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "name": "示例推理", "trade_date": "2026-04-03", "final_score": 0.9},
                {"ts_code": "000002.SZ", "name": "备选推理", "trade_date": "2026-04-03", "final_score": 0.8},
            ]
        )
        historical_candidates = pd.DataFrame(
            [
                {"ts_code": "300001.SZ", "name": "示例验证", "trade_date": "2026-04-02", "final_score": 0.7},
            ]
        )

        with (
            patch("src.app.facades.dashboard_facade.load_overlay_inference_candidates", return_value=inference_candidates),
            patch("src.app.facades.dashboard_facade.load_overlay_candidates", return_value=historical_candidates),
        ):
            payload = get_ai_review_summary_payload(inference_symbol="000002.SZ")

        self.assertEqual(payload["inference"]["selectedSymbol"], "000002.SZ")
        self.assertEqual(len(payload["inference"]["candidates"]), 2)
        self.assertIn("selectedRecord", payload["historical"])
        self.assertNotIn("brief", payload["inference"])

    def test_ai_review_summary_payload_gracefully_handles_candidates_without_symbol_column(self) -> None:
        inference_candidates = pd.DataFrame([{"name": "无代码候选", "final_score": 0.4}])

        with (
            patch("src.app.facades.dashboard_facade.load_overlay_inference_candidates", return_value=inference_candidates),
            patch("src.app.facades.dashboard_facade.load_overlay_candidates", return_value=pd.DataFrame()),
        ):
            payload = get_ai_review_summary_payload()

        self.assertEqual(payload["inference"]["selectedSymbol"], "")
        self.assertEqual(payload["inference"]["selectedRecord"], {})
        self.assertEqual(payload["inference"]["candidateCount"], 1)

    def test_ai_review_detail_payload_returns_selected_record_and_detail_fields(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "示例推理",
                    "trade_date": "2026-04-03",
                    "final_score": 0.9,
                    "bull_points": "强势;放量",
                    "risk_points": "回撤;追高",
                }
            ]
        )
        with (
            patch("src.app.facades.dashboard_facade.load_overlay_inference_candidates", return_value=candidates),
            patch("src.app.facades.dashboard_facade.load_overlay_inference_brief", return_value="测试纪要"),
            patch(
                "src.app.facades.dashboard_facade.load_overlay_llm_bundle",
                return_value={
                    "response_lookup": {
                        "000001.SZ": {
                            "custom_id": "000001.SZ",
                            "status": "success",
                            "output_text": "数据库回写结果",
                        }
                    },
                    "response_summary": "数据库回写纪要",
                },
            ),
        ):
            payload = get_ai_review_detail_payload(scope="inference", symbol="000001.SZ")

        self.assertEqual(payload["selectedSymbol"], "000001.SZ")
        self.assertEqual(payload["selectedRecord"]["name"], "示例推理")
        self.assertEqual(payload["brief"], "测试纪要")
        self.assertEqual(payload["llmResponse"]["status"], "success")
        self.assertEqual(payload["responseSummary"], "数据库回写纪要")
        self.assertIn("fieldRows", payload)

    def test_factor_summary_payload_prefers_snapshot_contract(self) -> None:
        factor_snapshot = {
            "available": True,
            "latestDate": "2026-04-03T00:00:00",
            "factorOptions": [{"key": "mom_20", "label": "mom_20", "description": ""}],
            "symbolOptions": ["000001.SZ"],
            "crossSection": [
                {"ts_code": "000001.SZ", "name": "示例股票", "mom_20": 0.8, "close_to_ma_20": 0.1},
                {"ts_code": "000002.SZ", "name": "备选股票", "mom_20": 0.3, "close_to_ma_20": -0.1},
            ],
            "missingRates": [{"feature": "mom_20", "missing_rate": 0.0}],
        }

        with (
            patch("src.app.facades.dashboard_facade.build_factor_explorer_snapshot", return_value=factor_snapshot),
            patch("src.app.facades.dashboard_facade.load_feature_panel", side_effect=AssertionError("should not load full feature panel")),
        ):
            payload = get_factor_explorer_summary_payload(factor_name="mom_20")

        self.assertTrue(payload["available"])
        self.assertEqual(payload["selectedSymbol"], "000001.SZ")
        self.assertEqual(len(payload["ranking"]), 2)
        self.assertIn("selectedRecord", payload)

    def test_factor_detail_payload_returns_selected_symbol_history(self) -> None:
        factor_snapshot = {
            "available": True,
            "latestDate": "2026-04-03T00:00:00",
            "factorOptions": [{"key": "mom_20", "label": "mom_20", "description": ""}],
            "symbolOptions": ["000001.SZ"],
            "crossSection": [{"ts_code": "000001.SZ", "name": "示例股票", "mom_20": 0.8}],
            "missingRates": [],
        }
        feature_panel = pd.DataFrame(
            [
                {"trade_date": "2026-04-01", "ts_code": "000001.SZ", "name": "示例股票", "mom_20": 0.7},
                {"trade_date": "2026-04-02", "ts_code": "000001.SZ", "name": "示例股票", "mom_20": 0.8},
            ]
        )

        with (
            patch("src.app.facades.dashboard_facade.build_factor_explorer_snapshot", return_value=factor_snapshot),
            patch("src.app.facades.dashboard_facade.load_feature_panel", return_value=feature_panel),
        ):
            payload = get_factor_explorer_detail_payload(factor_name="mom_20", history_factor="mom_20", symbol="000001.SZ")

        self.assertEqual(payload["selectedSymbol"], "000001.SZ")
        self.assertEqual(len(payload["history"]), 2)
        self.assertEqual(payload["snapshot"][0]["字段"], "ts_code")

    def test_service_payload_exposes_realtime_snapshot_summary(self) -> None:
        fake_store = Mock()
        fake_store.get_latest_snapshot_summary.return_value = {
            "trade_date": "2026-04-03",
            "snapshot_bucket": "post_close",
            "source": "sina-quote",
            "requested_symbol_count": 9,
            "success_symbol_count": 9,
            "failed_symbols": [],
            "error_message": "",
            "fetched_at": "2026-04-03T15:05:00+08:00",
        }

        with (
            patch("src.app.facades.dashboard_facade.get_realtime_quote_store", return_value=fake_store),
            patch("src.app.facades.dashboard_facade.pd.Timestamp.now", return_value=pd.Timestamp("2026-04-03 16:00:00+08:00")),
        ):
            payload = get_service_payload()

        self.assertIn("realtime_snapshot", payload)
        snapshot = payload["realtime_snapshot"]
        self.assertTrue(snapshot["available"])
        self.assertEqual(snapshot["snapshot_bucket"], "post_close")
        self.assertEqual(snapshot["snapshot_label_display"], "今日盘后快照")

    def test_watchlist_payload_includes_refresh_context(self) -> None:
        payload = get_watchlist_payload()

        self.assertIn("filters", payload)
        self.assertIn("refreshSymbols", payload)
        self.assertIn("refreshPreviousCloses", payload)

    def test_refresh_realtime_payload_reports_snapshot_status(self) -> None:
        base_frame = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "示例股票",
                    "latest_bar_close": 9.8,
                }
            ]
        )
        realtime_quotes = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "realtime_price": 10.5,
                    "realtime_time": pd.Timestamp("2026-04-03 10:00:00"),
                    "realtime_quote_source": "sina-quote",
                }
            ]
        )

        with (
            patch("src.app.facades.dashboard_facade.build_watchlist_base_frame", return_value=base_frame),
            patch(
                "src.app.facades.dashboard_facade.fetch_managed_realtime_quotes",
                return_value=(
                    realtime_quotes,
                    {
                        "available": True,
                        "source": "sina-quote",
                        "trade_date": "2026-04-03",
                        "fetched_at": "2026-04-03T10:00:00+08:00",
                        "requested_symbol_count": 1,
                        "success_symbol_count": 1,
                        "failed_symbols": [],
                        "error_message": "",
                        "snapshot_bucket": "latest",
                        "served_from": "provider",
                    },
                ),
            ),
            patch("src.app.facades.dashboard_facade.pd.Timestamp.now", return_value=pd.Timestamp("2026-04-03 10:00:00+08:00")),
        ):
            payload = refresh_realtime_payload()

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["symbolCount"], 1)
        self.assertEqual(payload["realtimeRecordCount"], 1)
        self.assertEqual(payload["realtimeStatus"]["snapshot_label_display"], "最新盘中快照")

    def test_watchlist_summary_payload_returns_list_first_contract(self) -> None:
        payload = get_watchlist_summary_payload()

        self.assertIn("overview", payload)
        self.assertIn("records", payload)
        self.assertIn("selectedRecord", payload)
        self.assertNotIn("history", payload)

    def test_watchlist_detail_payload_returns_detail_contract(self) -> None:
        payload = get_watchlist_detail_payload()

        self.assertIn("detail", payload)
        self.assertIn("history", payload)
        self.assertIn("watchPlan", payload)
        self.assertIn("latestAiShortlist", payload)

    def test_watchlist_detail_payload_handles_missing_prediction_columns(self) -> None:
        base_frame = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "示例股票",
                    "industry": "银行",
                    "entry_group": "持仓",
                    "mark_price": 10.0,
                    "latest_bar_close": 9.8,
                    "market_value": 10000.0,
                    "unrealized_pnl": 200.0,
                    "unrealized_pnl_pct": 0.02,
                    "watch_level": "观察",
                    "action_brief": "继续观察",
                    "premarket_plan": "观察承接",
                }
            ]
        )

        with (
            patch("src.app.facades.dashboard_facade.build_watchlist_base_frame", return_value=base_frame),
            patch("src.app.facades.dashboard_facade.filtered_watchlist_view", side_effect=lambda frame, **_: frame.reset_index(drop=True)),
            patch("src.app.facades.dashboard_facade.load_predictions", return_value=pd.DataFrame()),
            patch("src.app.facades.dashboard_facade.load_latest_symbol_markdown", return_value={}),
        ):
            payload = get_watchlist_detail_payload(symbol="000001.SZ")

        self.assertEqual(payload["selectedSymbol"], "000001.SZ")
        self.assertEqual(payload["history"], [])

    def test_watchlist_payload_uses_cached_snapshot_by_default(self) -> None:
        base_frame = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "示例股票",
                    "industry": "银行",
                    "entry_group": "持仓",
                    "mark_price": 10.0,
                    "latest_bar_close": 9.8,
                    "market_value": 10000.0,
                    "unrealized_pnl": 200.0,
                    "unrealized_pnl_pct": 0.02,
                    "is_overlay_selected": False,
                    "is_inference_overlay_selected": False,
                    "watch_level": "观察",
                    "action_brief": "继续观察",
                    "ensemble_rank": 1,
                    "universe_size": 10,
                    "ensemble_rank_pct": 0.9,
                    "inference_ensemble_rank": 2,
                    "inference_ensemble_rank_pct": 0.8,
                    "premarket_plan": "观察承接",
                    "llm_latest_status": "未入池",
                }
            ]
        )
        cached_snapshot = Mock()
        cached_snapshot.trade_date = "2026-04-03"
        cached_snapshot.snapshot_bucket = "post_close"
        cached_snapshot.status = {
            "available": True,
            "source": "sina-quote",
            "requested_symbol_count": 1,
            "success_symbol_count": 1,
            "failed_symbols": [],
            "error_message": "",
            "fetched_at": "2026-04-03T15:05:00+08:00",
        }
        cached_snapshot.quotes = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "realtime_price": 10.5,
                    "realtime_time": pd.Timestamp("2026-04-03 15:01:00"),
                    "realtime_quote_source": "sina-quote",
                }
            ]
        )
        fake_store = Mock()
        fake_store.get_latest_snapshot.return_value = cached_snapshot

        with (
            patch("src.app.facades.dashboard_facade.build_watchlist_base_frame", return_value=base_frame),
            patch("src.app.facades.dashboard_facade.filtered_watchlist_view", side_effect=lambda frame, **_: frame.reset_index(drop=True)),
            patch("src.app.facades.dashboard_facade.build_reduce_plan", return_value=pd.DataFrame()),
            patch("src.app.facades.dashboard_facade.load_predictions", return_value=pd.DataFrame(columns=["ts_code", "trade_date", "score"])),
            patch("src.app.facades.dashboard_facade.load_latest_symbol_markdown", return_value={}),
            patch("src.app.facades.dashboard_facade.get_realtime_quote_store", return_value=fake_store),
        ):
            payload = get_watchlist_payload()

        self.assertEqual(payload["realtimeStatus"]["served_from"], "database")
        self.assertEqual(payload["realtimeStatus"]["snapshot_bucket"], "post_close")
        self.assertAlmostEqual(payload["records"][0]["realtime_price"], 10.5)
        self.assertAlmostEqual(payload["detail"]["realtime_price"], 10.5)

    def test_apply_realtime_to_watchlist_payload_updates_records_and_detail(self) -> None:
        payload = {
            "selectedSymbol": "000001.SZ",
            "records": [
                {
                    "ts_code": "000001.SZ",
                    "name": "示例股票",
                    "mark_price": 10.0,
                    "latest_bar_close": 9.8,
                }
            ],
            "detail": {
                "ts_code": "000001.SZ",
                "name": "示例股票",
                "mark_price": 10.0,
                "latest_bar_close": 9.8,
            },
        }
        realtime_quotes = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "realtime_price": 10.5,
                    "realtime_pct_chg": 0.0714,
                    "realtime_time": "2026-04-02T10:00:00",
                }
            ]
        )

        updated = apply_realtime_to_watchlist_payload(
            payload,
            realtime_quotes=realtime_quotes,
            realtime_status={"available": True, "success_symbol_count": 1},
        )

        self.assertEqual(updated["realtimeStatus"]["success_symbol_count"], 1)
        self.assertAlmostEqual(updated["records"][0]["realtime_price"], 10.5)
        self.assertAlmostEqual(updated["detail"]["realtime_price"], 10.5)


if __name__ == "__main__":
    unittest.main()
