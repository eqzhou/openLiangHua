from __future__ import annotations

import json
import os
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from src.agents.llm_bridge import export_llm_requests
from src.db.dashboard_artifact_keys import (
    llm_bridge_export_artifact_key,
    overlay_llm_response_summary_artifact_key,
    overlay_llm_responses_artifact_key,
)
from src.db.dashboard_artifact_store import get_dashboard_artifact_store


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


class _FakeChatMessage:
    def __init__(self, content: str) -> None:
        self.content = content


class _FakeChatChoice:
    def __init__(self, content: str) -> None:
        self.message = _FakeChatMessage(content)


class _FakeChatCompletionResponse:
    def __init__(self, *, model: str, content: str) -> None:
        self.id = "chatcmpl_test_1"
        self.model = model
        self.choices = [_FakeChatChoice(content)]
        self.usage = {"prompt_tokens": 111, "completion_tokens": 222, "total_tokens": 333}


class _FakeChatCompletionsApi:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeChatCompletionResponse(model=kwargs["model"], content="兼容代理自动研讨结论")


class _FakeChatNamespace:
    def __init__(self) -> None:
        self.completions = _FakeChatCompletionsApi()


class _FakeClient:
    def __init__(self) -> None:
        self.responses = _FakeResponsesApi()
        self.chat = _FakeChatNamespace()


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

    def _artifact_text(self, artifact_key: str) -> str:
        artifact = get_dashboard_artifact_store().get_artifact(artifact_key)
        self.assertIsNotNone(artifact)
        assert artifact is not None
        return str(artifact.payload_text or "")

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
        self.assertEqual(artifacts["jsonl_path"], "artifact://myquant:text:overlay_llm_requests")
        self.assertEqual(artifacts["summary_path"], "artifact://myquant:text:overlay_llm_summary")
        self.assertEqual(artifacts["response_summary_path"], "artifact://myquant:text:overlay_llm_response_summary")
        self.assertIn(
            "000078.SZ",
            self._artifact_text(llm_bridge_export_artifact_key("myquant", "overlay_llm", "requests")),
        )
        self.assertIn(
            "OPENAI_API_KEY",
            self._artifact_text(llm_bridge_export_artifact_key("myquant", "overlay_llm", "summary")),
        )
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
        self.assertIn(
            "000078.SZ",
            self._artifact_text(overlay_llm_response_summary_artifact_key("myquant", "inference")),
        )
        response_text = self._artifact_text(overlay_llm_responses_artifact_key("myquant", "inference"))
        records = [json.loads(line) for line in response_text.splitlines() if line.strip()]
        self.assertEqual(records[0]["custom_id"], "000078.SZ")
        self.assertEqual(records[0]["status"], "success")
        self.assertEqual(records[0]["output_text"], "外部模型自动研讨结论")

    def test_export_llm_requests_uses_chat_completions_for_compatible_proxy(self) -> None:
        fake_client = _FakeClient()
        env = {
            "OVERLAY_LLM_ENABLED": "true",
            "OVERLAY_LLM_PROVIDER": "openai",
            "OVERLAY_LLM_MODEL": "gpt-5.4",
            "OPENAI_API_KEY": "sk-test",
            "OPENAI_BASE_URL": "http://127.0.0.1:8317/v1",
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
        self.assertEqual(len(fake_client.chat.completions.calls), 1)
        self.assertEqual(len(fake_client.responses.calls), 0)
        payload = fake_client.chat.completions.calls[0]
        self.assertEqual(payload["model"], "gpt-5.4")
        self.assertEqual(payload["messages"][0]["role"], "system")
        self.assertEqual(payload["messages"][1]["role"], "user")
        self.assertIn(
            "000078.SZ",
            self._artifact_text(overlay_llm_response_summary_artifact_key("myquant", "inference")),
        )
        response_text = self._artifact_text(overlay_llm_responses_artifact_key("myquant", "inference"))
        records = [json.loads(line) for line in response_text.splitlines() if line.strip()]
        self.assertEqual(records[0]["output_text"], "兼容代理自动研讨结论")


if __name__ == "__main__":
    unittest.main()
