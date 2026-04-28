from __future__ import annotations

import unittest

import pandas as pd
from fastapi.testclient import TestClient

from src.web_api.app import app


class WebApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def assertDateLike(self, value) -> None:
        self.assertNotIn(value, (None, ""))
        parsed = pd.Timestamp(value)
        self.assertFalse(pd.isna(parsed))

    def test_meta_endpoint_returns_frontend_bootstrap(self) -> None:
        response = self.client.get("/api/meta")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("modelNames", payload)
        self.assertIn("actions", payload)

    def test_shell_endpoint_returns_shared_shell_contract(self) -> None:
        response = self.client.get("/api/shell")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("bootstrap", payload)
        self.assertIn("experimentConfig", payload)
        self.assertIn("service", payload)
        self.assertIn("X-OpenLianghua-Response-Ms", response.headers)

    def test_home_endpoint_returns_operator_payload(self) -> None:
        response = self.client.get("/api/home")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("X-OpenLianghua-Legacy-Aggregate"), "true")
        payload = response.json()
        self.assertIn("service", payload)
        self.assertIn("watchlist", payload)
        self.assertIn("candidates", payload)
        self.assertIn("alerts", payload)
        self.assertIn("aiReview", payload)
        candidates = payload["candidates"]
        self.assertIn("latestDate", candidates)
        self.assertDateLike(candidates["latestDate"])
        self.assertIn("focusRecord", candidates)
        if candidates["focusRecord"]:
            self.assertDateLike(candidates["focusRecord"]["trade_date"])
        ai_review = payload["aiReview"]
        self.assertIn("inferenceRecords", ai_review)
        self.assertIn("historicalRecords", ai_review)
        if ai_review["inferenceRecords"]:
            self.assertDateLike(ai_review["inferenceRecords"][0]["trade_date"])
        if ai_review["historicalRecords"]:
            self.assertDateLike(ai_review["historicalRecords"][0]["trade_date"])

    def test_home_summary_endpoint_returns_observability_header_without_legacy_marker(self) -> None:
        response = self.client.get("/api/home/summary")

        self.assertEqual(response.status_code, 200)
        self.assertIn("X-OpenLianghua-Response-Ms", response.headers)
        self.assertNotIn("X-OpenLianghua-Legacy-Aggregate", response.headers)

    def test_candidates_summary_endpoint_returns_snapshot_contract(self) -> None:
        response = self.client.get("/api/candidates/summary")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("latestPicks", payload)
        self.assertIn("symbolOptions", payload)
        self.assertIn("selectedRecord", payload)
        self.assertIn("latestDate", payload)
        self.assertDateLike(payload["latestDate"])
        if payload["latestPicks"]:
            self.assertDateLike(payload["latestPicks"][0]["trade_date"])
        if payload["selectedRecord"]:
            self.assertDateLike(payload["selectedRecord"]["trade_date"])

    def test_candidates_history_endpoint_returns_history_contract(self) -> None:
        response = self.client.get("/api/candidates/history")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("selectedSymbol", payload)
        self.assertIn("scoreHistory", payload)
        if payload["scoreHistory"]:
            self.assertDateLike(payload["scoreHistory"][0]["trade_date"])

    def test_watchlist_summary_endpoint_returns_list_first_contract(self) -> None:
        response = self.client.get("/api/watchlist/summary")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("records", payload)
        self.assertIn("selectedRecord", payload)
        self.assertIn("realtimeStatus", payload)
        self.assertIn("page", payload)
        self.assertIn("pageSize", payload)
        self.assertIn("totalPages", payload)
        if payload["records"]:
            first_row = payload["records"][0]
            self.assertIn("ts_code", first_row)
            self.assertIn("name", first_row)
        if payload["selectedRecord"]:
            self.assertIn("ts_code", payload["selectedRecord"])
            self.assertIn("name", payload["selectedRecord"])
        realtime_status = payload["realtimeStatus"]
        if realtime_status.get("trade_date"):
            self.assertDateLike(realtime_status["trade_date"])
        if realtime_status.get("fetched_at"):
            self.assertDateLike(realtime_status["fetched_at"])

    def test_watchlist_detail_endpoint_returns_detail_contract(self) -> None:
        response = self.client.get("/api/watchlist/detail")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("detail", payload)
        self.assertIn("history", payload)
        self.assertIn("discussionRows", payload)
        if payload["detail"]:
            self.assertDateLike(payload["detail"]["latest_bar_date"])
            self.assertDateLike(payload["detail"]["inference_signal_date"])
        if payload["history"]:
            self.assertDateLike(payload["history"][0]["trade_date"])
        if payload["discussionRows"]:
            self.assertDateLike(payload["discussionRows"][0]["截面日期"])

    def test_ai_review_summary_endpoint_returns_summary_contract(self) -> None:
        response = self.client.get("/api/ai-review/summary")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("inference", payload)
        self.assertIn("historical", payload)
        self.assertIn("candidates", payload["inference"])
        self.assertNotIn("brief", payload["inference"])
        if payload["inference"]["candidates"]:
            self.assertDateLike(payload["inference"]["candidates"][0]["trade_date"])
        if payload["historical"]["candidates"]:
            self.assertDateLike(payload["historical"]["candidates"][0]["trade_date"])

    def test_ai_review_detail_endpoint_returns_detail_contract(self) -> None:
        response = self.client.get("/api/ai-review/detail?scope=inference")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("selectedRecord", payload)
        self.assertIn("fieldRows", payload)
        self.assertIn("llmResponse", payload)
        if payload["selectedRecord"]:
            self.assertDateLike(payload["selectedRecord"]["trade_date"])

    def test_candidates_detail_endpoint_returns_detail_contract(self) -> None:
        response = self.client.get("/api/candidates/detail")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("selectedRecord", payload)
        self.assertIn("fieldRows", payload)

    def test_factor_summary_endpoint_returns_list_first_contract(self) -> None:
        response = self.client.get("/api/factors/summary")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("ranking", payload)
        self.assertIn("selectedRecord", payload)

    def test_factor_detail_endpoint_returns_detail_contract(self) -> None:
        response = self.client.get("/api/factors/detail")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("history", payload)
        self.assertIn("snapshot", payload)

    def test_overview_endpoint_returns_summary_and_tables(self) -> None:
        response = self.client.get("/api/overview")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("summary", payload)
        self.assertIn("comparison", payload)

    def test_overview_summary_endpoint_returns_summary_contract(self) -> None:
        response = self.client.get("/api/overview/summary")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("summary", payload)
        self.assertIn("comparison", payload)
        self.assertIn("selectedSplit", payload)

    def test_overview_curves_endpoint_returns_curve_contract(self) -> None:
        response = self.client.get("/api/overview/curves")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("equityCurves", payload)
        self.assertIn("selectedSplit", payload)

    def test_backtests_summary_endpoint_returns_summary_contract(self) -> None:
        response = self.client.get("/api/backtests/summary")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("metrics", payload)
        self.assertIn("stability", payload)

    def test_backtests_portfolio_endpoint_returns_portfolio_contract(self) -> None:
        response = self.client.get("/api/backtests/portfolio")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("portfolio", payload)
        self.assertIn("monthlySummary", payload)

    def test_backtests_diagnostics_endpoint_returns_diagnostics_contract(self) -> None:
        response = self.client.get("/api/backtests/diagnostics")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("importance", payload)
        self.assertIn("yearlyDiagnostics", payload)
        self.assertIn("regimeDiagnostics", payload)

    def test_service_endpoint_returns_status_shape(self) -> None:
        response = self.client.get("/api/service")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("status_label", payload)
        self.assertIn("effective_state", payload)
        self.assertIn("realtime_snapshot", payload)

    def test_data_management_endpoint_returns_status_shape(self) -> None:
        response = self.client.get("/api/data-management")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("targetSource", payload)
        self.assertIn("dailyBar", payload)
        self.assertIn("researchPanel", payload)
        self.assertIn("legacyFeatureView", payload)
        self.assertIn("legacyLabelView", payload)
        self.assertIn("tokenConfigured", payload)

    def test_data_management_refresh_endpoint_requires_auth(self) -> None:
        response = self.client.post("/api/data-management/tushare-refresh", json={"target_source": "akshare"})

        self.assertEqual(response.status_code, 401)

    def test_market_bars_refresh_endpoint_requires_auth(self) -> None:
        response = self.client.post("/api/data-management/market-bars-refresh", json={"target_source": "tushare"})

        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
