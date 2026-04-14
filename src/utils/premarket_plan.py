from __future__ import annotations

from typing import Any


def _fmt_price(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{float(value):.2f}"


def _compact_text(text: str, limit: int = 30) -> str:
    cleaned = " ".join(str(text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "…"


def _price_tail(anchor_price: float | None, defensive_price: float | None, breakeven_price: float | None) -> str:
    parts: list[str] = []
    if anchor_price is not None:
        parts.append(f"先看 {_fmt_price(anchor_price)} 强弱")
    if defensive_price is not None:
        parts.append(f"失守 {_fmt_price(defensive_price)} 转防守")
    if breakeven_price is not None:
        parts.append(f"靠近 {_fmt_price(breakeven_price)} 看减仓")
    return "，".join(parts)


def build_premarket_plan(
    *,
    discussion_snapshot: dict[str, Any] | None,
    action_brief: str,
    anchor_price: float | None,
    defensive_price: float | None,
    breakeven_price: float | None,
) -> dict[str, str]:
    snapshot = discussion_snapshot or {}
    rounds = snapshot.get("rounds", []) or []
    latest_round = rounds[-1] if rounds else {}
    successful_rounds = [round_info for round_info in rounds if round_info.get("response_status") == "success"]
    preferred_round = successful_rounds[-1] if successful_rounds else latest_round

    price_tail = _price_tail(anchor_price, defensive_price, breakeven_price)
    source = "系统默认"

    if preferred_round and preferred_round.get("response_status") == "success":
        lead = f"{preferred_round.get('round_label', '外部研讨')}：{_compact_text(preferred_round.get('summary_text', ''), 32)}"
        source = str(preferred_round.get("round_label", "") or "外部研讨")
    elif latest_round and latest_round.get("selected_for_llm"):
        lead = f"{latest_round.get('round_label', '外部研讨')} {latest_round.get('display_status', '待处理')}"
        source = str(latest_round.get("round_label", "") or "外部研讨")
    elif latest_round and latest_round.get("in_candidate_pool"):
        lead = f"{latest_round.get('round_label', '外部研讨')} 仅入候选池"
        source = str(latest_round.get("round_label", "") or "候选池")
    elif latest_round:
        lead = "外部研讨暂无覆盖"
        source = str(latest_round.get("round_label", "") or "系统默认")
    else:
        lead = _compact_text(action_brief, 32)

    if not price_tail:
        summary = lead
    elif lead:
        summary = f"{lead}，{price_tail}。"
    else:
        summary = f"{price_tail}。"
    return {
        "premarket_plan": summary,
        "premarket_plan_source": source,
    }
