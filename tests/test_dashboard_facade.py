from __future__ import annotations

import unittest
from unittest.mock import ANY, Mock, patch

import pandas as pd

from src.app.facades.dashboard_facade import (
    _build_home_alerts,
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
    run_market_bars_refresh_payload,
    run_tushare_full_refresh_payload,
    run_tushare_incremental_refresh_payload,
    run_watchlist_research_refresh_payload,
    get_service_payload,
    get_shell_payload,
    get_watchlist_detail_payload,
    get_watchlist_payload,
    get_watchlist_summary_payload,
)


class DashboardFacadeTests(unittest.TestCase):
    def setUp(self) -> None:
        from src.app.facades.service_facade import _get_service_payload_cached

        _get_service_payload_cached.cache_clear()

    def test_data_management_payload_passes_through_status_contract(self) -> None:
        with patch(
            "src.app.facades.data_management_facade.build_data_management_payload",
            return_value={
                "targetSource": "akshare",
                "tokenConfigured": True,
                "dailyBar": {"latestTradeDate": "2026-04-16"},
                "researchPanel": {"latestTradeDate": "2026-04-16"},
                "legacyFeatureView": {"latestTradeDate": "2026-04-16"},
                "legacyLabelView": {"latestTradeDate": "2026-04-16"},
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
                "src.app.facades.data_management_facade.run_tushare_incremental_refresh",
                return_value={
                    "target_source": "akshare",
                    "latest_trade_date": "2026-04-16",
                    "appended_rows": 116,
                },
            ),
            patch("src.app.facades.base.clear_dashboard_caches") as clear_caches,
        ):
            payload = run_tushare_incremental_refresh_payload(target_source="akshare", end_date="2026-04-16")

        self.assertEqual(payload["actionName"], "tushare_incremental_refresh")
        self.assertTrue(payload["ok"])
        self.assertIn("2026-04-16", payload["output"])
        clear_caches.assert_called_once()

    def test_run_tushare_full_refresh_payload_clears_dashboard_caches(self) -> None:
        with (
            patch(
                "src.app.facades.data_management_facade.run_tushare_full_refresh",
                return_value={
                    "ok": True,
                    "target_source": "akshare",
                    "incremental": {"latest_trade_date": "2026-04-16"},
                    "features": {"feature_rows": 1200, "label_rows": 1180},
                    "dashboardSync": {"message": "dashboard synced"},
                },
            ),
            patch("src.app.facades.base.clear_dashboard_caches") as clear_caches,
        ):
            payload = run_tushare_full_refresh_payload(target_source="akshare", end_date="2026-04-16")

        self.assertEqual(payload["actionName"], "tushare_full_refresh")
        self.assertTrue(payload["ok"])
        self.assertIn("1200", payload["output"])
        clear_caches.assert_called_once()

    def test_run_market_bars_refresh_payload_uses_authenticated_user_scope(self) -> None:
        with (
            patch("src.app.facades.data_management_facade.sync_market_bars_from_tushare") as sync_market,
            patch("src.app.facades.base.clear_dashboard_caches") as clear_caches,
        ):
            sync_market.return_value.user_id = "user-1"
            sync_market.return_value.previous_latest_trade_date = "2026-04-17"
            sync_market.return_value.latest_trade_date = "2026-04-28"
            sync_market.return_value.requested_symbols = 51
            sync_market.return_value.fetched_rows = 357
            sync_market.return_value.upserted_rows = 357

            payload = run_market_bars_refresh_payload(user_id="user-1", end_date="2026-04-28")

        sync_market.assert_called_once_with(user_id="user-1", end_date="2026-04-28")
        self.assertEqual(payload["actionName"], "market_bars_refresh")
        self.assertIn("2026-04-28", payload["output"])
        clear_caches.assert_called_once()

    def test_run_watchlist_research_refresh_payload_rebuilds_panel_and_inference(self) -> None:
        with (
            patch(
                "src.app.facades.data_management_facade.build_feature_label_artifacts",
                return_value={"date_max": "2026-04-28", "panel_rows": 125135, "symbol_count": 51},
            ) as build_features,
            patch(
                "src.app.facades.data_management_facade.generate_overlay_inference_report",
                return_value={"latest_feature_date": "2026-04-28", "inference_universe_size": 51, "candidate_count": 51},
            ) as overlay_inference,
            patch("src.app.facades.base.clear_dashboard_caches") as clear_caches,
        ):
            payload = run_watchlist_research_refresh_payload(user_id="user-1", target_source="tushare")

        build_features.assert_called_once()
        self.assertEqual(build_features.call_args.kwargs["market_universe_user_id"], "user-1")
        overlay_inference.assert_called_once_with(root=ANY, execute_llm=False, user_id="user-1")
        self.assertEqual(payload["actionName"], "watchlist_research_refresh")
        self.assertIn("125135", payload["output"])
        self.assertIn("AI候选数量：51", payload["output"])
        clear_caches.assert_called_once()

    def test_overview_payload_exposes_selected_split_and_tables(self) -> None:
        payload = get_overview_payload("test")

        self.assertEqual(payload["selectedSplit"], "test")
        self.assertIn("summary", payload)
        self.assertIn("comparison", payload)

    def test_shell_payload_exposes_sidebar_contract(self) -> None:
        with (
            patch("src.app.facades.service_facade.get_experiment_config_payload", return_value={"label_col": "ret_t1_t10", "top_n": 10}),
            patch("src.app.facades.service_facade.load_watchlist_config", return_value={"holdings": [{"ts_code": "000001.SZ"}], "focus_pool": []}),
            patch("src.app.facades.service_facade.get_service_payload", return_value={"effective_state": "running"}),
        ):
            payload = get_shell_payload()

        self.assertIn("bootstrap", payload)
        self.assertIn("experimentConfig", payload)
        self.assertIn("service", payload)
        self.assertIn("watchlistEntryCount", payload)
        self.assertIn("configSummaryText", payload)

    def test_home_payload_exposes_operator_sections(self) -> None:
        with (
            patch(
                "src.app.facades.home_facade._get_home_payload_cached",
                return_value={
                    "service": {"effective_state": "running"},
                    "overview": {"selectedSplit": "test"},
                    "watchlist": {"records": []},
                    "candidates": {"records": []},
                    "aiReview": {"shortlistMarkdown": "test"},
                    "alerts": [],
                },
            ),
        ):
            payload = get_home_payload()

        self.assertIn("service", payload)
        self.assertIn("overview", payload)
        self.assertIn("watchlist", payload)
        self.assertIn("candidates", payload)
        self.assertIn("aiReview", payload)
        self.assertIn("shortlistMarkdown", payload["aiReview"])
        self.assertIn("alerts", payload)

    def test_build_home_alerts_ignores_missing_powershell_and_recent_market_snapshot(self) -> None:
        alerts = _build_home_alerts(
            service_payload={
                "effective_state": "unknown",
                "status_label_display": "状态脚本不可用",
                "realtime_snapshot": {
                    "available": True,
                    "snapshot_label_display": "最近交易日盘中快照",
                    "is_today": False,
                    "is_current_market_day": True,
                },
            },
            watchlist_payload={
                "overview": {
                    "unrealizedPnl": 0,
                }
            },
        )

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["tone"], "good")
        self.assertIn("主操作链路当前正常", alerts[0]["title"])

    def test_candidates_summary_payload_prefers_precomputed_snapshot(self) -> None:
        candidate_snapshot = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "name": "示例股票", "trade_date": pd.Timestamp("2026-04-03"), "score": 0.9, "rank": 1},
                {"ts_code": "000002.SZ", "name": "后备股票", "trade_date": pd.Timestamp("2026-04-03"), "score": 0.8, "rank": 2},
            ]
        )

        with (
            patch("src.app.facades.candidates_facade.build_candidate_snapshot", return_value=candidate_snapshot),
        ):
            payload = get_candidates_summary_payload(model_name="ensemble", split_name="test", top_n=1)

        self.assertEqual(payload["modelName"], "ensemble")
        self.assertEqual(payload["latestDate"], "2026-04-03")
        self.assertEqual(len(payload["latestPicks"]), 1)
        self.assertEqual(payload["selectedSymbol"], "")

    def test_candidate_history_payload_returns_selected_symbol_history(self) -> None:
        predictions = pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": "2026-04-01", "score": 0.7, "ret_t1_t10": 0.02},
                {"ts_code": "000001.SZ", "trade_date": "2026-04-02", "score": 0.8, "ret_t1_t10": 0.03},
            ]
        )

        with patch("src.app.facades.candidates_facade.load_prediction_history_for_symbol", return_value=predictions):
            payload = get_candidate_history_payload(model_name="ensemble", split_name="test", symbol="000001.SZ")

        self.assertEqual(payload["selectedSymbol"], "000001.SZ")
        self.assertEqual(len(payload["scoreHistory"]), 2)

    def test_ai_review_summary_payload_keeps_candidates_and_selected_record(self) -> None:
        with (
            patch(
                "src.app.facades.ai_review_facade.load_overlay_candidate_summary_records",
                side_effect=[
                    [
                        {"ts_code": "000001.SZ", "name": "示例推理", "trade_date": "2026-04-03", "final_score": 0.9},
                        {"ts_code": "000002.SZ", "name": "备选推理", "trade_date": "2026-04-03", "final_score": 0.8},
                    ],
                    [
                        {"ts_code": "300001.SZ", "name": "示例验证", "trade_date": "2026-04-02", "final_score": 0.7},
                    ],
                ],
            ),
            patch(
                "src.app.facades.ai_review_facade.load_overlay_candidate_record",
                side_effect=[
                    {"ts_code": "000002.SZ", "name": "备选推理", "trade_date": "2026-04-03", "final_score": 0.8},
                    {"ts_code": "300001.SZ", "name": "示例验证", "trade_date": "2026-04-02", "final_score": 0.7},
                ],
            ),
        ):
            payload = get_ai_review_summary_payload(inference_symbol="000002.SZ")

        self.assertEqual(payload["inference"]["selectedSymbol"], "000002.SZ")
        self.assertEqual(len(payload["inference"]["candidates"]), 2)
        self.assertIn("selectedRecord", payload["historical"])
        self.assertNotIn("brief", payload["inference"])

    def test_ai_review_summary_payload_is_user_scoped_and_marks_watchlist_relation(self) -> None:
        with (
            patch(
                "src.app.facades.ai_review_facade.load_overlay_candidate_summary_records",
                side_effect=[
                    [
                        {"ts_code": "000001.SZ", "name": "持仓命中", "trade_date": "2026-04-03", "final_score": 0.9},
                        {"ts_code": "000002.SZ", "name": "未跟踪", "trade_date": "2026-04-03", "final_score": 0.8},
                    ],
                    [
                        {"ts_code": "300001.SZ", "name": "关注命中", "trade_date": "2026-04-02", "final_score": 0.7},
                    ],
                ],
            ) as summary_loader,
            patch(
                "src.app.facades.ai_review_facade.load_overlay_candidate_record",
                side_effect=[
                    {"ts_code": "000001.SZ", "name": "持仓命中", "trade_date": "2026-04-03", "final_score": 0.9},
                    {"ts_code": "300001.SZ", "name": "关注命中", "trade_date": "2026-04-02", "final_score": 0.7},
                ],
            ) as record_loader,
            patch(
                "src.app.facades.ai_review_facade.load_watchlist_config",
                return_value={
                    "holdings": [{"ts_code": "000001.SZ", "name": "持仓命中"}],
                    "focus_pool": [{"ts_code": "300001.SZ", "name": "关注命中"}],
                },
            ) as watchlist_loader,
        ):
            payload = get_ai_review_summary_payload(
                inference_symbol="000001.SZ",
                historical_symbol="300001.SZ",
                user_id="user-a",
            )

        self.assertEqual(summary_loader.call_args_list[0].kwargs["user_id"], "user-a")
        self.assertIsNone(summary_loader.call_args_list[1].kwargs["user_id"])
        self.assertEqual(record_loader.call_args_list[0].kwargs["user_id"], "user-a")
        self.assertIsNone(record_loader.call_args_list[1].kwargs["user_id"])
        self.assertEqual(watchlist_loader.call_args.kwargs["user_id"], "user-a")
        inference_rows = payload["inference"]["candidates"]
        historical_rows = payload["historical"]["candidates"]
        self.assertEqual(inference_rows[0]["watchlist_relation"], "holding")
        self.assertEqual(inference_rows[0]["watchlist_relation_label"], "持仓")
        self.assertEqual(inference_rows[1]["watchlist_relation"], "untracked")
        self.assertEqual(historical_rows[0]["watchlist_relation"], "focus")
        self.assertTrue(payload["inference"]["selectedRecord"]["is_current_holding"])

    def test_ai_review_summary_payload_gracefully_handles_candidates_without_symbol_column(self) -> None:
        with (
            patch(
                "src.app.facades.ai_review_facade.load_overlay_candidate_summary_records",
                side_effect=[
                    [{"name": "无代码候选", "final_score": 0.4}],
                    [],
                ],
            ),
            patch("src.app.facades.ai_review_facade.load_overlay_candidate_record", return_value={}),
        ):
            payload = get_ai_review_summary_payload()

        self.assertEqual(payload["inference"]["selectedSymbol"], "")
        self.assertEqual(payload["inference"]["selectedRecord"], {})
        self.assertEqual(payload["inference"]["candidateCount"], 1)

    def test_ai_review_detail_payload_returns_selected_record_and_detail_fields(self) -> None:
        with (
            patch(
                "src.app.facades.ai_review_facade.load_overlay_candidate_summary_records",
                return_value=[{"ts_code": "000001.SZ", "name": "示例推理", "trade_date": "2026-04-03", "final_score": 0.9}],
            ),
            patch(
                "src.app.facades.ai_review_facade.load_overlay_candidate_record",
                return_value={
                    "ts_code": "000001.SZ",
                    "name": "示例推理",
                    "trade_date": "2026-04-03",
                    "final_score": 0.9,
                    "bull_points": "强势;放量",
                    "risk_points": "回撤;追高",
                },
            ),
            patch("src.app.facades.ai_review_facade.load_overlay_inference_brief", return_value="测试纪要"),
            patch(
                "src.app.facades.ai_review_facade.load_overlay_llm_bundle",
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

    def test_ai_review_detail_payload_is_user_scoped(self) -> None:
        with (
            patch(
                "src.app.facades.ai_review_facade.load_overlay_candidate_summary_records",
                return_value=[{"ts_code": "000001.SZ", "name": "示例推理", "trade_date": "2026-04-03", "final_score": 0.9}],
            ) as summary_loader,
            patch(
                "src.app.facades.ai_review_facade.load_overlay_candidate_record",
                return_value={"ts_code": "000001.SZ", "name": "示例推理", "trade_date": "2026-04-03", "final_score": 0.9},
            ) as record_loader,
            patch("src.app.facades.ai_review_facade.load_overlay_inference_brief", return_value="测试纪要") as brief_loader,
            patch("src.app.facades.ai_review_facade.load_overlay_llm_bundle", return_value={"response_lookup": {}, "response_summary": ""}) as llm_loader,
            patch(
                "src.app.facades.ai_review_facade.load_watchlist_config",
                return_value={"holdings": [{"ts_code": "000001.SZ", "name": "示例推理"}], "focus_pool": []},
            ),
        ):
            payload = get_ai_review_detail_payload(scope="inference", symbol="000001.SZ", user_id="user-a")

        self.assertEqual(summary_loader.call_args.kwargs["user_id"], "user-a")
        self.assertEqual(record_loader.call_args.kwargs["user_id"], "user-a")
        self.assertEqual(brief_loader.call_args.kwargs["user_id"], "user-a")
        self.assertEqual(llm_loader.call_args.kwargs["user_id"], "user-a")
        self.assertEqual(payload["selectedRecord"]["watchlist_relation_label"], "持仓")

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
            patch("src.app.facades.factors_facade.build_factor_explorer_snapshot", return_value=factor_snapshot),
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
            patch("src.app.facades.factors_facade.build_factor_explorer_snapshot", return_value=factor_snapshot),
            patch("src.app.facades.factors_facade.load_feature_history_for_symbol", return_value=feature_panel),
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
            patch("src.app.facades.service_facade.get_realtime_quote_store", return_value=fake_store),
            patch("src.app.facades.service_facade.pd.Timestamp.now", return_value=pd.Timestamp("2026-04-03 16:00:00+08:00")),
        ):
            payload = get_service_payload()

        self.assertIn("realtime_snapshot", payload)
        snapshot = payload["realtime_snapshot"]
        self.assertTrue(snapshot["available"])
        self.assertEqual(snapshot["snapshot_bucket"], "post_close")
        self.assertEqual(snapshot["snapshot_label_display"], "今日盘后快照")

    def test_service_payload_marks_after_close_latest_snapshot_as_recent_post_close(self) -> None:
        fake_store = Mock()
        fake_store.get_latest_snapshot_summary.return_value = {
            "trade_date": "2026-04-03",
            "snapshot_bucket": "latest",
            "source": "sina-quote",
            "requested_symbol_count": 9,
            "success_symbol_count": 9,
            "failed_symbols": [],
            "error_message": "",
            "fetched_at": "2026-04-03T15:05:00+08:00",
        }
        with (
            patch("src.app.facades.service_facade.get_realtime_quote_store", return_value=fake_store),
            patch(
                "src.app.facades.service_facade.load_dataset_summary",
                return_value={"date_max": "2026-04-03"},
            ),
            patch("src.app.facades.service_facade.pd.Timestamp.now", return_value=pd.Timestamp("2026-04-05 10:00:00+08:00")),
        ):
            payload = get_service_payload()

        snapshot = payload["realtime_snapshot"]
        self.assertEqual(snapshot["snapshot_bucket"], "latest")
        self.assertEqual(snapshot["snapshot_label_display"], "最近交易日盘后快照")
        self.assertTrue(snapshot["is_current_market_day"])

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
            patch("src.app.facades.watchlist_facade.build_watchlist_base_frame", return_value=base_frame),
            patch(
                "src.app.facades.watchlist_facade.fetch_managed_realtime_quotes",
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
            patch("src.app.facades.service_facade.pd.Timestamp.now", return_value=pd.Timestamp("2026-04-03 10:00:00+08:00")),
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

    def test_watchlist_detail_payload_loads_user_scoped_notes(self) -> None:
        with (
            patch(
                "src.app.facades.watchlist_facade.load_watchlist_record",
                return_value={
                    "ts_code": "000001.SZ",
                    "name": "示例股票",
                    "latest_bar_close": 9.8,
                    "mark_price": 10.0,
                },
            ),
            patch("src.app.facades.watchlist_facade.load_prediction_history_for_symbol", return_value=pd.DataFrame()),
            patch("src.app.facades.watchlist_facade.build_reduce_plan", return_value=pd.DataFrame()),
            patch("src.app.facades.watchlist_facade.load_latest_symbol_markdown", return_value={}) as note_loader,
        ):
            payload = get_watchlist_detail_payload(symbol="000001.SZ", user_id="user-1")

        self.assertEqual(payload["selectedSymbol"], "000001.SZ")
        self.assertEqual(note_loader.call_args_list[0].kwargs["user_id"], "user-1")
        self.assertEqual(note_loader.call_args_list[1].kwargs["user_id"], "user-1")

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
            patch("src.app.facades.watchlist_facade.build_watchlist_base_frame", return_value=base_frame),
            patch("src.app.facades.watchlist_facade.filtered_watchlist_view", side_effect=lambda frame, **_: frame.reset_index(drop=True)),
            patch("src.app.facades.watchlist_facade.load_watchlist_record", return_value={}),
            patch("src.app.facades.watchlist_facade.load_prediction_history_for_symbol", return_value=pd.DataFrame()),
            patch("src.app.facades.watchlist_facade.load_latest_symbol_markdown", return_value={}),
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
            "requested_symbol_count": 2,
            "success_symbol_count": 2,
            "failed_symbols": ["000002.SZ"],
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
                },
                {
                    "ts_code": "000002.SZ",
                    "realtime_price": 20.5,
                    "realtime_time": pd.Timestamp("2026-04-03 15:01:00"),
                    "realtime_quote_source": "sina-quote",
                }
            ]
        )
        fake_store = Mock()
        fake_store.get_latest_snapshot.return_value = cached_snapshot

        with (
            patch("src.app.facades.watchlist_facade.build_watchlist_base_frame", return_value=base_frame),
            patch("src.app.facades.watchlist_facade.filtered_watchlist_view", side_effect=lambda frame, **_: frame.reset_index(drop=True)),
            patch("src.app.facades.watchlist_facade.build_reduce_plan", return_value=pd.DataFrame()),
            patch(
                "src.app.facades.watchlist_facade.load_watchlist_summary_records",
                side_effect=[
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
                        }
                    ],
                    [
                        {
                            "ts_code": "000001.SZ",
                            "latest_bar_close": 9.8,
                        }
                    ],
                ],
            ),
            patch(
                "src.app.facades.watchlist_facade.load_watchlist_overview",
                return_value={
                    "totalCount": 1,
                    "overlayCount": 0,
                    "inferenceOverlayCount": 0,
                    "marketValue": 10000.0,
                    "unrealizedPnl": 200.0,
                },
            ),
            patch("src.app.facades.watchlist_facade.load_watchlist_record", return_value={}),
            patch("src.app.facades.watchlist_facade.load_prediction_history_for_symbol", return_value=pd.DataFrame(columns=["ts_code", "trade_date", "score"])),
            patch("src.app.facades.watchlist_facade.load_latest_symbol_markdown", return_value={}),
            patch("src.app.facades.watchlist_facade.get_realtime_quote_store", return_value=fake_store),
        ):
            payload = get_watchlist_payload()

        self.assertEqual(payload["realtimeStatus"]["served_from"], "database")
        self.assertEqual(payload["realtimeStatus"]["snapshot_bucket"], "post_close")
        self.assertEqual(payload["realtimeStatus"]["requested_symbol_count"], 1)
        self.assertEqual(payload["realtimeStatus"]["success_symbol_count"], 1)
        self.assertEqual(payload["realtimeStatus"]["failed_symbols"], [])
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
