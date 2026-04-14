from __future__ import annotations

import json
import os
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from src.agents.llm_bridge import export_llm_requests


TEST_TMP_ROOT = Path(__file__).resolve().parent / ".tmp"


class _FakeResponse:
    def __init__(self, *, model: str, output_text: str) -> None:
        self.id = "resp_test_1"
        self.model = model
        self.output_text = output_text
        self.usage = {"input_tokens": 123, "output_tokens": 456, "total_tokens": 579}


class _FakeResponsesApi:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeResponse(model=kwargs["model"], output_text="外部模型自动研讨结论")


class _FakeClient:
    def __init__(self) -> None:
        self.responses = _FakeResponsesApi()


class LlmBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.case_root = TEST_TMP_ROOT / f"llm_bridge_{uuid.uuid4().hex}"
        self.reports_dir = self.case_root / "reports" / "weekly"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.packet = {
            "data_source": "myquant",
            "split": "inference",
            "prediction_mode": "latest_unlabeled_inference",
            "latest_date": "2026-04-01",
            "latest_risk_state": {"risk_on": True},
            "selected_candidates": [
                {
                    "ts_code": "000078.SZ",
                    "agent_prompt": "请分析这只股票明日是否值得继续跟踪，并说明风险点。",
                }
            ],
        }

    def tearDown(self) -> None:
        shutil.rmtree(self.case_root, ignore_errors=True)

    def test_export_llm_requests_stays_export_only_without_api_key(self) -> None:
        env = {
            "OVERLAY_LLM_ENABLED": "true",
            "OVERLAY_LLM_PROVIDER": "openai",
            "OVERLAY_LLM_MODEL": "gpt-5.4",
        }
        with patch("src.agents.llm_bridge.load_dotenv", return_value=True), patch.dict(os.environ, env, clear=True):
            artifacts = export_llm_requests(
                packet=self.packet,
                reports_dir=self.reports_dir,
                data_source="myquant",
            )

        self.assertEqual(artifacts["execution_status"], "export_only")
        self.assertEqual(artifacts["request_count"], 1)
        self.assertEqual(artifacts["response_count"], 0)
        self.assertIn("OPENAI_API_KEY", artifacts["blocking_reason"])
        self.assertTrue(Path(artifacts["jsonl_path"]).exists())
        self.assertTrue(Path(artifacts["summary_path"]).exists())
        self.assertTrue(Path(artifacts["response_summary_path"]).exists())
        self.assertEqual(artifacts["response_jsonl_path"], "")

    def test_export_llm_requests_executes_openai_when_ready(self) -> None:
        fake_client = _FakeClient()
        env = {
            "OVERLAY_LLM_ENABLED": "true",
            "OVERLAY_LLM_PROVIDER": "openai",
            "OVERLAY_LLM_MODEL": "gpt-5.4",
            "OVERLAY_LLM_REASONING_EFFORT": "low",
            "OVERLAY_LLM_REASONING_SUMMARY": "auto",
            "OVERLAY_LLM_MAX_OUTPUT_TOKENS": "900",
            "OPENAI_API_KEY": "sk-test",
        }
        with (
            patch("src.agents.llm_bridge.load_dotenv", return_value=True),
            patch.dict(os.environ, env, clear=True),
            patch("src.agents.llm_bridge._build_openai_client", return_value=fake_client),
        ):
            artifacts = export_llm_requests(
                packet=self.packet,
                reports_dir=self.reports_dir,
                data_source="myquant",
                output_prefix="overlay_inference_llm",
            )

        self.assertEqual(artifacts["execution_status"], "executed")
        self.assertEqual(artifacts["response_count"], 1)
        self.assertEqual(artifacts["success_count"], 1)
        self.assertEqual(len(fake_client.responses.calls), 1)
        payload = fake_client.responses.calls[0]
        self.assertEqual(payload["model"], "gpt-5.4")
        self.assertEqual(payload["reasoning"], {"effort": "low", "summary": "auto"})
        self.assertEqual(payload["max_output_tokens"], 900)
        self.assertIn("[SYSTEM]", payload["input"])
        self.assertIn("000078.SZ", Path(artifacts["response_summary_path"]).read_text(encoding="utf-8"))

        response_path = Path(artifacts["response_jsonl_path"])
        self.assertTrue(response_path.exists())
        records = [json.loads(line) for line in response_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(records[0]["custom_id"], "000078.SZ")
        self.assertEqual(records[0]["status"], "success")
        self.assertEqual(records[0]["output_text"], "外部模型自动研讨结论")


if __name__ == "__main__":
    unittest.main()
