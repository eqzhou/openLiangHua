from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from src.web_api.app import app


class WebApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

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

    def test_home_endpoint_returns_operator_payload(self) -> None:
        response = self.client.get("/api/home")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("service", payload)
        self.assertIn("watchlist", payload)
        self.assertIn("candidates", payload)
        self.assertIn("alerts", payload)

    def test_candidates_summary_endpoint_returns_snapshot_contract(self) -> None:
        response = self.client.get("/api/candidates/summary")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("latestPicks", payload)
        self.assertIn("symbolOptions", payload)
        self.assertIn("selectedRecord", payload)

    def test_candidates_history_endpoint_returns_history_contract(self) -> None:
        response = self.client.get("/api/candidates/history")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("selectedSymbol", payload)
        self.assertIn("scoreHistory", payload)

    def test_watchlist_summary_endpoint_returns_list_first_contract(self) -> None:
        response = self.client.get("/api/watchlist/summary")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("records", payload)
        self.assertIn("selectedRecord", payload)

    def test_watchlist_detail_endpoint_returns_detail_contract(self) -> None:
        response = self.client.get("/api/watchlist/detail")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("detail", payload)
        self.assertIn("history", payload)

    def test_ai_review_summary_endpoint_returns_summary_contract(self) -> None:
        response = self.client.get("/api/ai-review/summary")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("inference", payload)
        self.assertIn("historical", payload)
        self.assertIn("candidates", payload["inference"])
        self.assertNotIn("brief", payload["inference"])

    def test_ai_review_detail_endpoint_returns_detail_contract(self) -> None:
        response = self.client.get("/api/ai-review/detail?scope=inference")

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
        self.assertIn("featurePanel", payload)
        self.assertIn("labelPanel", payload)
        self.assertIn("tokenConfigured", payload)

    def test_data_management_refresh_endpoint_requires_auth(self) -> None:
        response = self.client.post("/api/data-management/tushare-refresh", json={"target_source": "akshare"})

        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
