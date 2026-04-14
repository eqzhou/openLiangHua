from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd
import streamlit as st

from src.app.pages.payload_utils import records_to_frame
from src.app.viewmodels.ai_review_vm import build_llm_response_lookup


def _render_overlay_panel(
    *,
    title: str,
    candidates: pd.DataFrame,
    packet: dict,
    brief: str,
    empty_message: str,
    inspect_key: str,
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
    read_jsonl_records: Callable[[str], list[dict]],
    read_text: Callable[[Path], str],
) -> None:
    st.markdown(f"**{title}**")
    if candidates.empty:
        st.info(empty_message)
        return

    latest_date_text = packet.get("latest_date", "-") if packet else "-"
    latest_risk_state = packet.get("latest_risk_state", {}) if packet else {}
    llm_bridge = packet.get("llm_bridge", {}) if packet else {}
    ensemble_weights = packet.get("ensemble_weights", {}) if packet else {}
    event_coverage = packet.get("event_coverage", {}) if packet else {}
    inference_packet = packet.get("inference_packet", {}) if packet else {}
    llm_response_records = read_jsonl_records(str(llm_bridge.get("response_jsonl_path", ""))) if llm_bridge else []
    llm_response_lookup = build_llm_response_lookup(llm_response_records)
    response_summary_path = str(llm_bridge.get("response_summary_path", "") or "").strip()
    response_summary = read_text(Path(response_summary_path)) if response_summary_path else ""

    ai_cards = st.columns(4)
    ai_cards[0].metric("AI候选池数量", int(len(candidates)))
    ai_cards[1].metric("AI入选数量", int(packet.get("top_n", 0) or 0))
    ai_cards[2].metric("最新截面日期", latest_date_text)
    ai_cards[3].metric(
        "当前风控状态",
        "趋势开启" if latest_risk_state.get("risk_on") else "趋势过滤" if latest_risk_state else "未知",
    )

    if inference_packet:
        inference_cols = st.columns(4)
        inference_cols[0].metric("最新特征日", str(inference_packet.get("latest_feature_date", "-")))
        inference_cols[1].metric("最新带标签日", str(inference_packet.get("latest_labeled_date", "-")))
        inference_cols[2].metric("推理股票数", int(inference_packet.get("inference_universe_size", 0) or 0))
        inference_cols[3].metric("训练交易日数", int(inference_packet.get("history_dates", 0) or 0))
        skipped_filters = inference_packet.get("skipped_filters", []) or []
        if skipped_filters:
            st.caption("未标注截面推理已跳过未来依赖过滤：" + ", ".join(str(item) for item in skipped_filters))

    if ensemble_weights:
        st.markdown("**模型融合权重**")
        weight_cols = st.columns(4)
        weights = ensemble_weights.get("weights", {}) or {}
        weight_cols[0].metric("融合模式", str(ensemble_weights.get("mode", "manual")))
        weight_cols[1].metric("评估样本", str(ensemble_weights.get("evaluation_split", "manual") or "manual"))
        weight_cols[2].metric("梯度提升树权重", f"{float(weights.get('lgbm', 0.0) or 0.0):.1%}")
        weight_cols[3].metric("岭回归权重", f"{float(weights.get('ridge', 0.0) or 0.0):.1%}")
        if ensemble_weights.get("summary"):
            st.caption(str(ensemble_weights.get("summary")))

    if event_coverage:
        st.markdown("**资讯覆盖概览**")
        coverage_cols = st.columns(3)
        source_counts = event_coverage.get("news_source_counts", {}) or {}
        coverage_cols[0].metric("公告覆盖数", int(event_coverage.get("notice_covered_count", 0) or 0))
        coverage_cols[1].metric("研报覆盖数", int(event_coverage.get("research_covered_count", 0) or 0))
        coverage_cols[2].metric("资讯来源种类", int(len(source_counts)))
        if source_counts:
            coverage_table = pd.DataFrame([{"资讯来源": key, "数量": value} for key, value in source_counts.items()])
            st.dataframe(coverage_table, width="stretch")

    shown_columns = [
        "trade_date",
        "ts_code",
        "name",
        "industry_display",
        "theme_tags",
        "action_hint",
        "news_source",
        "final_score",
        "quant_score",
        "factor_overlay_score",
        "model_consensus",
        "confidence_level",
        "bull_points",
        "risk_points",
    ]
    st.dataframe(
        prettify_dataframe(candidates[[column for column in shown_columns if column in candidates.columns]]),
        width="stretch",
    )

    if llm_bridge:
        st.markdown("**外部大模型桥接**")
        llm_cols = st.columns(6)
        llm_cols[0].metric("请求包数量", int(llm_bridge.get("request_count", 0) or 0))
        llm_cols[1].metric("已执行响应", int(llm_bridge.get("response_count", 0) or 0))
        llm_cols[2].metric("成功数量", int(llm_bridge.get("success_count", 0) or 0))
        llm_cols[3].metric("桥接模式", str(llm_bridge.get("provider", "prompt_only")))
        llm_cols[4].metric("模型名", str(llm_bridge.get("model", "") or "未配置"))
        llm_cols[5].metric("执行状态", str(llm_bridge.get("execution_status", "unknown")))
        if llm_bridge.get("jsonl_path"):
            st.caption(f"请求包文件：{llm_bridge.get('jsonl_path')}")
        if llm_bridge.get("response_jsonl_path"):
            st.caption(f"响应文件：{llm_bridge.get('response_jsonl_path')}")
        reasoning_parts: list[str] = []
        if llm_bridge.get("reasoning_effort"):
            reasoning_parts.append(f"reasoning.effort={llm_bridge.get('reasoning_effort')}")
        if llm_bridge.get("reasoning_summary"):
            reasoning_parts.append(f"reasoning.summary={llm_bridge.get('reasoning_summary')}")
        if llm_bridge.get("max_output_tokens"):
            reasoning_parts.append(f"max_output_tokens={llm_bridge.get('max_output_tokens')}")
        if reasoning_parts:
            st.caption(" | ".join(str(part) for part in reasoning_parts))
        blocking_reason = str(llm_bridge.get("blocking_reason", "") or "").strip()
        if blocking_reason:
            if llm_bridge.get("execution_status") in {"configuration_incomplete", "execution_failed", "executed_with_errors"}:
                st.warning(blocking_reason)
            else:
                st.info(blocking_reason)
        if response_summary:
            with st.expander("查看外部模型自动研讨纪要", expanded=False):
                st.markdown(response_summary)

    inspect_options = candidates["ts_code"].tolist()
    inspect_symbol = st.selectbox("查看单只股票的 AI 解释", inspect_options, key=inspect_key)
    inspected = candidates.loc[candidates["ts_code"] == inspect_symbol].head(1)
    if not inspected.empty:
        inspected_row = inspected.iloc[0]
        st.markdown("**投研结论**")
        if "thesis_summary" in inspected_row.index:
            st.write(inspected_row.get("thesis_summary", ""))
        if "theme_tags" in inspected_row.index:
            st.write(f"主题标签：{inspected_row.get('theme_tags', '')}")
        if "action_hint" in inspected_row.index:
            st.write(f"建议动作：{inspected_row.get('action_hint', '')}")
        if "notice_digest" in inspected_row.index:
            st.markdown("**公告摘要**")
            st.write(inspected_row.get("notice_digest", ""))
        if "news_digest" in inspected_row.index:
            st.markdown("**新闻/研报摘要**")
            st.write(inspected_row.get("news_digest", ""))
        if "news_source" in inspected_row.index:
            st.write(f"资讯来源：{inspected_row.get('news_source', '')}")
        if "research_digest" in inspected_row.index:
            st.markdown("**研报补充**")
            st.write(inspected_row.get("research_digest", ""))
        st.markdown("**中文研判摘要**")
        st.write(inspected_row.get("ai_brief", ""))
        llm_response = llm_response_lookup.get(inspect_symbol)
        if llm_response:
            st.markdown("**外部模型自动研讨**")
            if llm_response.get("status") == "success":
                st.write(llm_response.get("output_text", ""))
            else:
                st.error(str(llm_response.get("error", "外部模型执行失败")))
        st.markdown("**可继续交给大模型的提示词**")
        st.code(inspected_row.get("agent_prompt", ""), language="text")

    if brief:
        with st.expander("查看完整 AI 研判纪要", expanded=False):
            st.markdown(brief)


def render_ai_review_page(
    *,
    overlay_inference_candidates: pd.DataFrame,
    overlay_inference_packet: dict,
    overlay_inference_brief: str,
    overlay_candidates: pd.DataFrame,
    overlay_packet: dict,
    overlay_brief: str,
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
    read_jsonl_records: Callable[[str], list[dict]],
    read_text: Callable[[Path], str],
) -> None:
    st.subheader("AI二次研判")
    if overlay_inference_candidates.empty and overlay_candidates.empty:
        st.warning("AI 研判摘要还没有生成，请先点击左侧的“生成AI研判摘要”或“生成最新未标注截面推理”。")
        return

    _render_overlay_panel(
        title="最新未标注截面推理",
        candidates=overlay_inference_candidates,
        packet=overlay_inference_packet,
        brief=overlay_inference_brief,
        empty_message="最新未标注截面推理还没有生成，可先点击左侧按钮生成更贴近最新行情的候选池。",
        inspect_key="overlay_inference_symbol",
        prettify_dataframe=prettify_dataframe,
        read_jsonl_records=read_jsonl_records,
        read_text=read_text,
    )
    st.divider()
    _render_overlay_panel(
        title="历史已验证 AI 叠加候选",
        candidates=overlay_candidates,
        packet=overlay_packet,
        brief=overlay_brief,
        empty_message="历史已验证的 AI 研判摘要还没有生成，请先点击左侧的“生成AI研判摘要”。",
        inspect_key="overlay_symbol",
        prettify_dataframe=prettify_dataframe,
        read_jsonl_records=read_jsonl_records,
        read_text=read_text,
    )


def _render_overlay_payload_panel(
    *,
    title: str,
    panel_payload: dict[str, object],
    empty_message: str,
    inspect_key: str,
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    candidates = records_to_frame(panel_payload.get("candidates"))  # type: ignore[arg-type]
    packet = dict(panel_payload.get("packet", {}) or {})
    brief = str(panel_payload.get("brief", "") or "")
    selected_record = dict(panel_payload.get("selectedRecord", {}) or {})
    llm_response = dict(panel_payload.get("llmResponse", {}) or {})
    response_summary = str(panel_payload.get("responseSummary", "") or "")

    st.markdown(f"**{title}**")
    if candidates.empty:
        st.info(empty_message)
        return

    latest_date_text = packet.get("latest_date", "-") if packet else "-"
    latest_risk_state = packet.get("latest_risk_state", {}) if packet else {}
    llm_bridge = packet.get("llm_bridge", {}) if packet else {}
    ensemble_weights = packet.get("ensemble_weights", {}) if packet else {}
    event_coverage = packet.get("event_coverage", {}) if packet else {}
    inference_packet = packet.get("inference_packet", {}) if packet else {}

    ai_cards = st.columns(4)
    ai_cards[0].metric("AI候选池数量", int(len(candidates)))
    ai_cards[1].metric("AI入选数量", int(packet.get("top_n", 0) or 0))
    ai_cards[2].metric("最新截面日期", latest_date_text)
    ai_cards[3].metric(
        "当前风控状态",
        "趋势开启" if latest_risk_state.get("risk_on") else "趋势过滤" if latest_risk_state else "未知",
    )

    if inference_packet:
        inference_cols = st.columns(4)
        inference_cols[0].metric("最新特征日", str(inference_packet.get("latest_feature_date", "-")))
        inference_cols[1].metric("最新带标签日", str(inference_packet.get("latest_labeled_date", "-")))
        inference_cols[2].metric("推理股票数", int(inference_packet.get("inference_universe_size", 0) or 0))
        inference_cols[3].metric("训练交易日数", int(inference_packet.get("history_dates", 0) or 0))
        skipped_filters = inference_packet.get("skipped_filters", []) or []
        if skipped_filters:
            st.caption("未标注截面推理已跳过未来依赖过滤：" + ", ".join(str(item) for item in skipped_filters))

    if ensemble_weights:
        st.markdown("**模型融合权重**")
        weight_cols = st.columns(4)
        weights = ensemble_weights.get("weights", {}) or {}
        weight_cols[0].metric("融合模式", str(ensemble_weights.get("mode", "manual")))
        weight_cols[1].metric("评估样本", str(ensemble_weights.get("evaluation_split", "manual") or "manual"))
        weight_cols[2].metric("梯度提升树权重", f"{float(weights.get('lgbm', 0.0) or 0.0):.1%}")
        weight_cols[3].metric("岭回归权重", f"{float(weights.get('ridge', 0.0) or 0.0):.1%}")
        if ensemble_weights.get("summary"):
            st.caption(str(ensemble_weights.get("summary")))

    if event_coverage:
        st.markdown("**资讯覆盖概览**")
        coverage_cols = st.columns(3)
        source_counts = event_coverage.get("news_source_counts", {}) or {}
        coverage_cols[0].metric("公告覆盖数", int(event_coverage.get("notice_covered_count", 0) or 0))
        coverage_cols[1].metric("研报覆盖数", int(event_coverage.get("research_covered_count", 0) or 0))
        coverage_cols[2].metric("资讯来源种类", int(len(source_counts)))
        if source_counts:
            coverage_table = pd.DataFrame([{"资讯来源": key, "数量": value} for key, value in source_counts.items()])
            st.dataframe(coverage_table, width="stretch")

    shown_columns = [
        "trade_date",
        "ts_code",
        "name",
        "industry_display",
        "theme_tags",
        "action_hint",
        "news_source",
        "final_score",
        "quant_score",
        "factor_overlay_score",
        "model_consensus",
        "confidence_level",
        "bull_points",
        "risk_points",
    ]
    st.dataframe(
        prettify_dataframe(candidates[[column for column in shown_columns if column in candidates.columns]]),
        width="stretch",
    )

    if llm_bridge:
        st.markdown("**外部大模型桥接**")
        llm_cols = st.columns(6)
        llm_cols[0].metric("请求包数量", int(llm_bridge.get("request_count", 0) or 0))
        llm_cols[1].metric("已执行响应", int(llm_bridge.get("response_count", 0) or 0))
        llm_cols[2].metric("成功数量", int(llm_bridge.get("success_count", 0) or 0))
        llm_cols[3].metric("桥接模式", str(llm_bridge.get("provider", "prompt_only")))
        llm_cols[4].metric("模型名", str(llm_bridge.get("model", "") or "未配置"))
        llm_cols[5].metric("执行状态", str(llm_bridge.get("execution_status", "unknown")))
        if llm_bridge.get("jsonl_path"):
            st.caption(f"请求包文件：{llm_bridge.get('jsonl_path')}")
        if llm_bridge.get("response_jsonl_path"):
            st.caption(f"响应文件：{llm_bridge.get('response_jsonl_path')}")
        reasoning_parts: list[str] = []
        if llm_bridge.get("reasoning_effort"):
            reasoning_parts.append(f"reasoning.effort={llm_bridge.get('reasoning_effort')}")
        if llm_bridge.get("reasoning_summary"):
            reasoning_parts.append(f"reasoning.summary={llm_bridge.get('reasoning_summary')}")
        if llm_bridge.get("max_output_tokens"):
            reasoning_parts.append(f"max_output_tokens={llm_bridge.get('max_output_tokens')}")
        if reasoning_parts:
            st.caption(" | ".join(str(part) for part in reasoning_parts))
        blocking_reason = str(llm_bridge.get("blocking_reason", "") or "").strip()
        if blocking_reason:
            if llm_bridge.get("execution_status") in {"configuration_incomplete", "execution_failed", "executed_with_errors"}:
                st.warning(blocking_reason)
            else:
                st.info(blocking_reason)
        if response_summary:
            with st.expander("查看外部模型自动研讨纪要", expanded=False):
                st.markdown(response_summary)

    inspect_options = candidates["ts_code"].astype(str).tolist()
    selected_symbol = str(panel_payload.get("selectedSymbol") or inspect_options[0])
    inspect_index = inspect_options.index(selected_symbol) if selected_symbol in inspect_options else 0
    st.selectbox("查看单只股票的 AI 解释", inspect_options, index=inspect_index, key=inspect_key)
    if selected_record:
        st.markdown("**投研结论**")
        if selected_record.get("thesis_summary"):
            st.write(selected_record.get("thesis_summary", ""))
        if selected_record.get("theme_tags"):
            st.write(f"主题标签：{selected_record.get('theme_tags', '')}")
        if selected_record.get("action_hint"):
            st.write(f"建议动作：{selected_record.get('action_hint', '')}")
        if selected_record.get("notice_digest"):
            st.markdown("**公告摘要**")
            st.write(selected_record.get("notice_digest", ""))
        if selected_record.get("news_digest"):
            st.markdown("**新闻/研报摘要**")
            st.write(selected_record.get("news_digest", ""))
        if selected_record.get("news_source"):
            st.write(f"资讯来源：{selected_record.get('news_source', '')}")
        if selected_record.get("research_digest"):
            st.markdown("**研报补充**")
            st.write(selected_record.get("research_digest", ""))
        st.markdown("**中文研判摘要**")
        st.write(selected_record.get("ai_brief", ""))
        if llm_response:
            st.markdown("**外部模型自动研讨**")
            if llm_response.get("status") == "success":
                st.write(llm_response.get("output_text", ""))
            else:
                st.error(str(llm_response.get("error", "外部模型执行失败")))
        st.markdown("**可继续交给大模型的提示词**")
        st.code(selected_record.get("agent_prompt", ""), language="text")

    if brief:
        with st.expander("查看完整 AI 研判纪要", expanded=False):
            st.markdown(brief)


def render_ai_review_payload_page(
    *,
    payload: dict[str, object],
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    inference = dict(payload.get("inference", {}) or {})
    historical = dict(payload.get("historical", {}) or {})
    st.subheader("AI二次研判")
    if not inference.get("candidates") and not historical.get("candidates"):
        st.warning("AI 研判摘要还没有生成，请先点击左侧的“生成AI研判摘要”或“生成最新未标注截面推理”。")
        return

    _render_overlay_payload_panel(
        title="最新未标注截面推理",
        panel_payload=inference,
        empty_message="最新未标注截面推理还没有生成，可先点击左侧按钮生成更贴近最新行情的候选池。",
        inspect_key="overlay_inference_symbol",
        prettify_dataframe=prettify_dataframe,
    )
    st.divider()
    _render_overlay_payload_panel(
        title="历史已验证 AI 叠加候选",
        panel_payload=historical,
        empty_message="历史已验证的 AI 研判摘要还没有生成，请先点击左侧的“生成AI研判摘要”。",
        inspect_key="overlay_symbol",
        prettify_dataframe=prettify_dataframe,
    )
