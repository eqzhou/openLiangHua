from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from src.utils.data_source import source_prefixed_path
from src.utils.io import project_root, save_text
from src.utils.logger import configure_logging

logger = configure_logging()

DEFAULT_SYSTEM_PROMPT = (
    "你是一名中文股票投研助手。请基于用户提供的量化信号、行业信息和风险提示，"
    "输出谨慎、可解释、面向研究的结论，不要夸大确定性。"
)
DEFAULT_PROVIDER = "prompt_only"
DEFAULT_REASONING_EFFORT = "medium"
DEFAULT_REASONING_SUMMARY = "auto"
SUPPORTED_PROVIDERS = {"prompt_only", "openai"}


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_positive_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _clean_provider(value: str | None) -> str:
    provider = (value or DEFAULT_PROVIDER).strip().lower()
    return provider if provider in SUPPORTED_PROVIDERS else DEFAULT_PROVIDER


def _blocking_reason(
    *,
    enabled: bool,
    provider: str,
    model: str,
    api_key: str,
    request_count: int,
) -> str:
    if request_count <= 0:
        return "当前没有可发送给外部模型的候选股票。"
    if not enabled:
        return "已导出提示词，但 OVERLAY_LLM_ENABLED=false，未自动调用外部模型。"
    if provider == "prompt_only":
        return "当前桥接模式为 prompt_only，只导出请求包，不自动调用外部模型。"
    if provider != "openai":
        return f"暂不支持的外部模型提供商：{provider}。"
    if not model:
        return "已启用 OpenAI，但未配置 OVERLAY_LLM_MODEL。"
    if not api_key:
        return "已启用 OpenAI，但缺少 OPENAI_API_KEY。"
    return ""


def load_llm_settings() -> dict[str, Any]:
    load_dotenv(project_root() / ".env")
    enabled = _parse_bool(os.getenv("OVERLAY_LLM_ENABLED"), default=False)
    provider = _clean_provider(os.getenv("OVERLAY_LLM_PROVIDER"))
    model = (os.getenv("OVERLAY_LLM_MODEL", "") or "").strip()
    reasoning_effort = (os.getenv("OVERLAY_LLM_REASONING_EFFORT", DEFAULT_REASONING_EFFORT) or "").strip()
    reasoning_summary = (os.getenv("OVERLAY_LLM_REASONING_SUMMARY", DEFAULT_REASONING_SUMMARY) or "").strip()
    max_output_tokens = _parse_positive_int(os.getenv("OVERLAY_LLM_MAX_OUTPUT_TOKENS"))
    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    base_url = (os.getenv("OPENAI_BASE_URL", "") or "").strip()

    auto_execute = enabled and provider == "openai"
    ready = auto_execute and bool(model) and bool(api_key)
    return {
        "enabled": enabled,
        "provider": provider,
        "model": model,
        "reasoning_effort": reasoning_effort or DEFAULT_REASONING_EFFORT,
        "reasoning_summary": reasoning_summary or DEFAULT_REASONING_SUMMARY,
        "max_output_tokens": max_output_tokens,
        "api_key": api_key,
        "base_url": base_url,
        "auto_execute": auto_execute,
        "ready": ready,
    }


def _build_request(candidate: dict, settings: dict[str, Any], market_context: dict) -> dict[str, Any]:
    user_prompt = str(candidate.get("agent_prompt", "") or "").strip()
    return {
        "custom_id": str(candidate.get("ts_code", "") or ""),
        "provider": settings.get("provider", DEFAULT_PROVIDER),
        "model": settings.get("model", ""),
        "market_context": market_context,
        "messages": [
            {"role": "system", "content": DEFAULT_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    }


def _request_input_text(request: dict[str, Any]) -> str:
    market_context = request.get("market_context", {}) or {}
    messages = request.get("messages", []) or []
    sections: list[str] = []
    if market_context:
        sections.append("市场上下文(JSON)：\n" + json.dumps(market_context, ensure_ascii=False, indent=2))
    for message in messages:
        role = str(message.get("role", "user") or "user").strip().upper()
        content = str(message.get("content", "") or "").strip()
        if content:
            sections.append(f"[{role}]\n{content}")
    return "\n\n".join(sections).strip()


def _serializable_object(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _serializable_object(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serializable_object(item) for item in value]
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "__dict__"):
        return {
            key: _serializable_object(item)
            for key, item in vars(value).items()
            if not str(key).startswith("_")
        }
    return str(value)


def _build_openai_client(settings: dict[str, Any]) -> Any:
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError("缺少 openai 依赖，请先执行 `pip install -r requirements.txt`。") from exc

    kwargs: dict[str, Any] = {"api_key": settings.get("api_key", "")}
    if settings.get("base_url"):
        kwargs["base_url"] = settings["base_url"]
    return OpenAI(**kwargs)


def _openai_payload(request: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": settings.get("model", ""),
        "input": _request_input_text(request),
    }
    reasoning: dict[str, Any] = {}
    if settings.get("reasoning_effort"):
        reasoning["effort"] = settings["reasoning_effort"]
    if settings.get("reasoning_summary"):
        reasoning["summary"] = settings["reasoning_summary"]
    if reasoning:
        payload["reasoning"] = reasoning
    if settings.get("max_output_tokens"):
        payload["max_output_tokens"] = int(settings["max_output_tokens"])
    return payload


def _success_record(request: dict[str, Any], response: Any, payload: dict[str, Any]) -> dict[str, Any]:
    usage = _serializable_object(getattr(response, "usage", None))
    record = {
        "custom_id": request.get("custom_id", ""),
        "status": "success",
        "provider": "openai",
        "model": getattr(response, "model", None) or payload.get("model"),
        "request_payload": payload,
        "response_id": getattr(response, "id", None),
        "output_text": str(getattr(response, "output_text", "") or "").strip(),
        "usage": usage,
    }
    return record


def _error_record(request: dict[str, Any], settings: dict[str, Any], payload: dict[str, Any], error: Exception) -> dict[str, Any]:
    return {
        "custom_id": request.get("custom_id", ""),
        "status": "error",
        "provider": settings.get("provider", DEFAULT_PROVIDER),
        "model": settings.get("model", ""),
        "request_payload": payload,
        "response_id": None,
        "output_text": "",
        "usage": None,
        "error": str(error),
    }


def _execute_openai_requests(requests: list[dict[str, Any]], settings: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    client = _build_openai_client(settings)
    records: list[dict[str, Any]] = []
    for request in requests:
        payload = _openai_payload(request, settings)
        try:
            response = client.responses.create(**payload)
            records.append(_success_record(request, response, payload))
        except Exception as exc:  # pragma: no cover - network/provider failures are environment-specific
            logger.exception("OpenAI overlay request failed for {}", request.get("custom_id"))
            records.append(_error_record(request, settings, payload, exc))
    status = "executed_with_errors" if any(record["status"] == "error" for record in records) else "executed"
    return records, status


def _write_text_variants(text: str, reports_dir: Path, filename: str, data_source: str) -> Path:
    source_path = source_prefixed_path(reports_dir, filename, data_source)
    save_text(text, source_path)
    save_text(text, reports_dir / filename)
    return source_path


def _build_request_summary(
    *,
    settings: dict[str, Any],
    request_count: int,
    execution_status: str,
    blocking_reason: str,
) -> str:
    lines = [
        "# LLM Request Export",
        "",
        f"- Provider: {settings.get('provider', DEFAULT_PROVIDER)}",
        f"- Model: {settings.get('model', '') or 'not_configured'}",
        f"- Auto Execute Enabled: {'true' if settings.get('enabled') else 'false'}",
        f"- Ready To Execute: {'true' if settings.get('ready') else 'false'}",
        f"- Execution Status: {execution_status}",
        f"- Request Count: {request_count}",
    ]
    if settings.get("reasoning_effort"):
        lines.append(f"- Reasoning Effort: {settings['reasoning_effort']}")
    if settings.get("reasoning_summary"):
        lines.append(f"- Reasoning Summary: {settings['reasoning_summary']}")
    if settings.get("max_output_tokens"):
        lines.append(f"- Max Output Tokens: {settings['max_output_tokens']}")
    if blocking_reason:
        lines.extend(["", "## Note", "", blocking_reason])
    return "\n".join(lines)


def _build_response_summary(
    *,
    settings: dict[str, Any],
    response_records: list[dict[str, Any]],
    execution_status: str,
    blocking_reason: str,
) -> str:
    success_records = [record for record in response_records if record.get("status") == "success"]
    error_records = [record for record in response_records if record.get("status") == "error"]
    lines = [
        "# LLM Response Summary",
        "",
        f"- Provider: {settings.get('provider', DEFAULT_PROVIDER)}",
        f"- Model: {settings.get('model', '') or 'not_configured'}",
        f"- Execution Status: {execution_status}",
        f"- Response Count: {len(response_records)}",
        f"- Success Count: {len(success_records)}",
        f"- Error Count: {len(error_records)}",
    ]
    if blocking_reason:
        lines.extend(["", "## Note", "", blocking_reason])
    if success_records:
        lines.extend(["", "## Successful Responses", ""])
        for record in success_records:
            lines.extend(
                [
                    f"### {record.get('custom_id', '')}",
                    record.get("output_text", "") or "(no output text)",
                    "",
                ]
            )
    if error_records:
        lines.extend(["", "## Errors", ""])
        for record in error_records:
            lines.append(f"- {record.get('custom_id', '')}: {record.get('error', 'unknown error')}")
    return "\n".join(lines)


def export_llm_requests(
    packet: dict[str, Any],
    reports_dir: Path,
    data_source: str,
    *,
    output_prefix: str = "overlay_llm",
) -> dict[str, Any]:
    settings = load_llm_settings()
    selected = packet.get("selected_candidates", []) or []
    market_context = {
        "data_source": packet.get("data_source"),
        "split": packet.get("split"),
        "prediction_mode": packet.get("prediction_mode"),
        "latest_date": packet.get("latest_date"),
        "latest_risk_state": packet.get("latest_risk_state", {}),
    }
    requests = [_build_request(candidate, settings, market_context) for candidate in selected]

    request_count = len(requests)
    blocking_reason = _blocking_reason(
        enabled=bool(settings.get("enabled")),
        provider=str(settings.get("provider", DEFAULT_PROVIDER)),
        model=str(settings.get("model", "")),
        api_key=str(settings.get("api_key", "")),
        request_count=request_count,
    )
    execution_status = "ready" if not blocking_reason and request_count > 0 else "export_only"
    response_records: list[dict[str, Any]] = []

    if request_count > 0 and not blocking_reason:
        try:
            response_records, execution_status = _execute_openai_requests(requests, settings)
            if execution_status == "executed_with_errors":
                blocking_reason = "部分股票的外部模型研讨已执行成功，但存在请求失败，请查看错误列表。"
        except RuntimeError as exc:
            blocking_reason = str(exc)
            execution_status = "configuration_incomplete"
        except Exception as exc:  # pragma: no cover - defensive fallback
            logger.exception("LLM bridge execution crashed")
            blocking_reason = f"外部模型自动研讨失败：{exc}"
            execution_status = "execution_failed"

    jsonl_lines = [json.dumps(item, ensure_ascii=False) for item in requests]
    request_jsonl_text = "\n".join(jsonl_lines)
    request_summary_text = _build_request_summary(
        settings=settings,
        request_count=request_count,
        execution_status=execution_status,
        blocking_reason=blocking_reason,
    )

    request_jsonl_filename = f"{output_prefix}_requests.jsonl"
    request_summary_filename = f"{output_prefix}_summary.md"
    request_jsonl_path = _write_text_variants(request_jsonl_text, reports_dir, request_jsonl_filename, data_source)
    request_summary_path = _write_text_variants(request_summary_text, reports_dir, request_summary_filename, data_source)

    response_jsonl_path = None
    if response_records:
        response_jsonl_filename = f"{output_prefix}_responses.jsonl"
        response_jsonl_text = "\n".join(json.dumps(item, ensure_ascii=False) for item in response_records)
        response_jsonl_path = _write_text_variants(response_jsonl_text, reports_dir, response_jsonl_filename, data_source)

    response_summary_filename = f"{output_prefix}_response_summary.md"
    response_summary_text = _build_response_summary(
        settings=settings,
        response_records=response_records,
        execution_status=execution_status,
        blocking_reason=blocking_reason,
    )
    response_summary_path = _write_text_variants(response_summary_text, reports_dir, response_summary_filename, data_source)

    success_count = sum(1 for record in response_records if record.get("status") == "success")
    error_count = sum(1 for record in response_records if record.get("status") == "error")
    return {
        "enabled": bool(settings.get("enabled")),
        "ready": bool(settings.get("ready")),
        "auto_execute": bool(settings.get("auto_execute")),
        "provider": settings.get("provider", DEFAULT_PROVIDER),
        "model": settings.get("model", ""),
        "reasoning_effort": settings.get("reasoning_effort", DEFAULT_REASONING_EFFORT),
        "reasoning_summary": settings.get("reasoning_summary", DEFAULT_REASONING_SUMMARY),
        "max_output_tokens": settings.get("max_output_tokens"),
        "execution_status": execution_status,
        "blocking_reason": blocking_reason,
        "request_count": request_count,
        "response_count": len(response_records),
        "success_count": success_count,
        "error_count": error_count,
        "jsonl_path": str(request_jsonl_path),
        "summary_path": str(request_summary_path),
        "response_jsonl_path": str(response_jsonl_path) if response_jsonl_path else "",
        "response_summary_path": str(response_summary_path),
    }
