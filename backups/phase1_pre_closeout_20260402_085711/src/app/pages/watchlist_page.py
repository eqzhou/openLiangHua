from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import streamlit as st

from src.app.services.watchlist_service import build_reduce_plan, filtered_watchlist_view
from src.utils.llm_discussion import discussion_round_rows


def render_watchlist_page(
    *,
    watchlist_view: pd.DataFrame,
    ensemble_predictions_test: pd.DataFrame,
    prettify_dataframe: Callable[[pd.DataFrame], pd.DataFrame],
    zh: Callable[[str], str],
    load_latest_watch_plan: Callable[[str], dict[str, str]],
    load_latest_action_memo: Callable[[str], dict[str, str]],
    run_module: Callable[[str], tuple[bool, str]],
    refresh_cached_views: Callable[[], None],
) -> None:
    st.subheader("观察池与持仓")
    if watchlist_view.empty:
        st.warning("当前还没有配置观察池股票。你可以在 config/watchlist.yaml 里添加持仓或关注股票。")
        return

    st.markdown("**观察池总览**")
    overview_cols = st.columns(5)
    overview_cols[0].metric("观察池股票数", int(len(watchlist_view)))
    overlay_count = int(watchlist_view["is_overlay_selected"].fillna(False).sum()) if "is_overlay_selected" in watchlist_view.columns else 0
    overview_cols[1].metric("进入AI精选池", overlay_count)
    inference_overlay_count = (
        int(watchlist_view["is_inference_overlay_selected"].fillna(False).sum())
        if "is_inference_overlay_selected" in watchlist_view.columns
        else 0
    )
    overview_cols[2].metric("进入最新推理池", inference_overlay_count)
    pnl_value = pd.to_numeric(watchlist_view["unrealized_pnl"], errors="coerce").sum()
    market_value = pd.to_numeric(watchlist_view["market_value"], errors="coerce").sum()
    overview_cols[3].metric("观察池参考市值", f"{market_value:,.0f}")
    overview_cols[4].metric("观察池浮动盈亏", f"{pnl_value:,.0f}")

    filter_cols = st.columns((1.5, 1, 1))
    watch_keyword = filter_cols[0].text_input(
        "快速搜索",
        value=st.session_state.get("watchlist_keyword", ""),
        key="watchlist_keyword",
        placeholder="输入代码或简称",
    )
    watch_scope = filter_cols[1].selectbox(
        "查看范围",
        ["全部", "只看 AI 精选", "只看最新推理池", "只看浮亏较大"],
        key="watchlist_scope",
    )
    watch_sort = filter_cols[2].selectbox(
        "排序方式",
        ["最新推理排名", "历史验证排名", "浮亏比例", "参考市值"],
        key="watchlist_sort",
    )
    filtered_watchlist = filtered_watchlist_view(
        watchlist_view,
        keyword=watch_keyword,
        scope=watch_scope,
        sort_by=watch_sort,
    )
    st.caption(f"当前显示 {len(filtered_watchlist)} / {len(watchlist_view)} 只观察池股票。")

    watchlist_columns = [
        "ts_code",
        "name",
        "premarket_plan",
        "industry",
        "cost_basis",
        "shares",
        "mark_price",
        "mark_date",
        "price_source",
        "latest_bar_date",
        "latest_bar_close",
        "mark_status",
        "market_value",
        "unrealized_pnl",
        "unrealized_pnl_pct",
        "breakeven_price",
        "gap_to_breakeven_pct",
        "signal_date",
        "ensemble_rank",
        "ensemble_rank_pct",
        "inference_signal_date",
        "inference_ensemble_rank",
        "inference_ensemble_rank_pct",
        "ridge_rank_pct",
        "lgbm_rank_pct",
        "llm_latest_round",
        "llm_latest_status",
        "watch_level",
        "is_overlay_selected",
        "is_inference_overlay_selected",
    ]
    if filtered_watchlist.empty:
        st.info("当前筛选条件下没有匹配股票，可以放宽范围或清空搜索词。")
        return

    st.dataframe(
        prettify_dataframe(filtered_watchlist[[column for column in watchlist_columns if column in filtered_watchlist.columns]]),
        width="stretch",
    )

    inspect_symbol = st.selectbox(
        "选择观察池股票",
        filtered_watchlist["ts_code"].tolist(),
        key="watchlist_symbol",
        format_func=lambda value: f"{value} {filtered_watchlist.loc[filtered_watchlist['ts_code'] == value, 'name'].iloc[0]}",
    )
    inspected = watchlist_view.loc[watchlist_view["ts_code"] == inspect_symbol].head(1)
    if inspected.empty:
        return

    row = inspected.iloc[0]
    detail_cols = st.columns(6)
    detail_cols[0].metric("参考价格", f"{float(row['mark_price']):.2f}" if pd.notna(row["mark_price"]) else "-")
    detail_cols[1].metric("浮动盈亏", f"{float(row['unrealized_pnl']):,.0f}" if pd.notna(row["unrealized_pnl"]) else "-")
    detail_cols[2].metric("浮亏浮盈比例", f"{float(row['unrealized_pnl_pct']):.2%}" if pd.notna(row["unrealized_pnl_pct"]) else "-")
    detail_cols[3].metric("历史验证排名", f"{int(row['ensemble_rank'])}" if pd.notna(row["ensemble_rank"]) else "-")
    detail_cols[4].metric("最新推理排名", f"{int(row['inference_ensemble_rank'])}" if pd.notna(row.get("inference_ensemble_rank")) else "-")
    detail_cols[5].metric("价格状态", str(row.get("mark_status") or "-"))

    guide_cols = st.columns(5)
    guide_cols[0].metric("回本价", f"{float(row['breakeven_price']):.3f}" if pd.notna(row["breakeven_price"]) else "-")
    guide_cols[1].metric("距回本涨幅", f"{float(row['gap_to_breakeven_pct']):.2%}" if pd.notna(row["gap_to_breakeven_pct"]) else "-")
    guide_cols[2].metric("半程修复位", f"{float(row['halfway_recovery_price']):.2f}" if pd.notna(row["halfway_recovery_price"]) else "-")
    guide_cols[3].metric("观察防守位", f"{float(row['defensive_price']):.2f}" if pd.notna(row["defensive_price"]) else "-")
    guide_cols[4].metric("最新落库日线", str(pd.to_datetime(row["latest_bar_date"]).date()) if pd.notna(row.get("latest_bar_date")) else "-")

    rank_cols = st.columns(4)
    rank_cols[0].metric("历史验证日期", str(pd.to_datetime(row["signal_date"]).date()) if pd.notna(row.get("signal_date")) else "-")
    rank_cols[1].metric("历史验证分位", f"{float(row['ensemble_rank_pct']):.2%}" if pd.notna(row.get("ensemble_rank_pct")) else "-")
    rank_cols[2].metric("最新推理日期", str(pd.to_datetime(row["inference_signal_date"]).date()) if pd.notna(row.get("inference_signal_date")) else "-")
    rank_cols[3].metric("最新推理分位", f"{float(row['inference_ensemble_rank_pct']):.2%}" if pd.notna(row.get("inference_ensemble_rank_pct")) else "-")

    if bool(row.get("is_manual_mark")):
        st.warning(str(row.get("mark_status_note") or "当前持仓估值使用手工记录价格。"))
    elif row.get("mark_status_note"):
        st.info(str(row.get("mark_status_note")))

    caption_parts = [f"价格来源：{row.get('price_source', '-')}"]
    if pd.notna(row.get("latest_bar_close")):
        caption_parts.append(f"最新落库收盘：{float(row['latest_bar_close']):.2f}")
    if pd.notna(row.get("signal_date")):
        caption_parts.append(f"历史验证：{pd.to_datetime(row['signal_date']).date()}")
    if pd.notna(row.get("inference_signal_date")):
        caption_parts.append(f"最新推理：{pd.to_datetime(row['inference_signal_date']).date()}")
    st.caption(" | ".join(caption_parts))
    if row.get("ranking_note"):
        st.caption(str(row.get("ranking_note")))
    st.markdown(f"**观察级别：{row.get('watch_level', '-')}**")
    if row.get("premarket_plan"):
        st.info(str(row.get("premarket_plan")))
    st.write(str(row.get("action_brief", "")))

    discussion_snapshot = row.get("llm_discussion_snapshot")
    if isinstance(discussion_snapshot, dict) and discussion_snapshot:
        st.markdown("**多轮 AI 研讨回写**")
        discussion_cols = st.columns(4)
        discussion_cols[0].metric("已检查轮次", int(discussion_snapshot.get("round_count", 0) or 0))
        discussion_cols[1].metric("入选轮次", int(discussion_snapshot.get("selected_round_count", 0) or 0))
        discussion_cols[2].metric("成功轮次", int(discussion_snapshot.get("success_round_count", 0) or 0))
        discussion_cols[3].metric("最新状态", str(discussion_snapshot.get("latest_status", "") or "-"))
        if discussion_snapshot.get("overview"):
            st.write(str(discussion_snapshot.get("overview")))
        discussion_rows = discussion_round_rows(discussion_snapshot)
        if discussion_rows:
            st.dataframe(pd.DataFrame(discussion_rows), width="stretch")
        for round_info in discussion_snapshot.get("rounds", []) or []:
            title = (
                f"{round_info.get('round_label', '-')}"
                f" | {round_info.get('latest_date', '-')}"
                f" | {round_info.get('display_status', '-')}"
            )
            with st.expander(title, expanded=False):
                summary_text = str(round_info.get("summary_text", "") or "").strip()
                if summary_text:
                    st.write(summary_text)
                thesis_summary = str(round_info.get("thesis_summary", "") or "").strip()
                if thesis_summary and thesis_summary != summary_text:
                    st.caption(f"系统内结论：{thesis_summary}")

    action_cols = st.columns(2)
    if action_cols[0].button("生成最新盯盘清单", key=f"generate_watch_plan_{inspect_symbol}", width="stretch"):
        with st.spinner("正在为观察池生成最新盯盘清单..."):
            ok, output = run_module("src.agents.watch_plan")
        st.session_state["last_action"] = ("watch_plan", ok, output)
        refresh_cached_views()
        st.rerun()
    if action_cols[1].button("生成最新操作备忘", key=f"generate_action_memo_{inspect_symbol}", width="stretch"):
        with st.spinner("正在为观察池生成最新操作备忘..."):
            ok, output = run_module("src.agents.action_memo")
        st.session_state["last_action"] = ("action_memo", ok, output)
        refresh_cached_views()
        st.rerun()

    watch_plan = load_latest_watch_plan(inspect_symbol)
    if watch_plan.get("content"):
        st.markdown("**最新盯盘清单**")
        watch_plan_meta: list[str] = []
        if watch_plan.get("plan_date"):
            watch_plan_meta.append(f"日期：{watch_plan['plan_date']}")
        if watch_plan.get("name"):
            watch_plan_meta.append(f"文件：{watch_plan['name']}")
        if watch_plan_meta:
            st.caption(" | ".join(watch_plan_meta))
        with st.expander("查看完整盯盘清单", expanded=False):
            st.markdown(str(watch_plan.get("content", "")))
    else:
        st.info("当前股票还没有生成盯盘清单 Markdown，可直接点击上方按钮自动生成。")

    action_memo = load_latest_action_memo(inspect_symbol)
    if action_memo.get("content"):
        st.markdown("**最新操作备忘**")
        action_memo_meta: list[str] = []
        if action_memo.get("plan_date"):
            action_memo_meta.append(f"日期：{action_memo['plan_date']}")
        if action_memo.get("name"):
            action_memo_meta.append(f"文件：{action_memo['name']}")
        if action_memo_meta:
            st.caption(" | ".join(action_memo_meta))
        with st.expander("查看完整操作备忘", expanded=False):
            st.markdown(str(action_memo.get("content", "")))
    else:
        st.info("当前股票还没有生成操作备忘 Markdown，可直接点击上方按钮自动生成。")

    reduce_plan = build_reduce_plan(row)
    if not reduce_plan.empty:
        st.markdown("**分批观察计划**")
        st.dataframe(prettify_dataframe(reduce_plan), width="stretch")

    history_source = ensemble_predictions_test if not ensemble_predictions_test.empty else pd.DataFrame()
    if not history_source.empty:
        history = history_source.loc[history_source["ts_code"] == inspect_symbol, ["trade_date", "score"]].copy()
        if not history.empty:
            history = history.sort_values("trade_date").tail(120).set_index("trade_date").rename(columns={"score": "融合评分"})
            st.line_chart(history)

    technical_cols = [
        "mom_5",
        "mom_20",
        "mom_60",
        "close_to_ma_20",
        "close_to_ma_60",
        "drawdown_60",
        "pct_chg",
        "latest_bar_date",
        "latest_bar_close",
        "mark_status_note",
        "manual_mark_note",
    ]
    technical_frame = pd.DataFrame(
        [{"字段": zh(key), "原始列名": key, "数值": "" if pd.isna(row.get(key)) else str(row.get(key))} for key in technical_cols]
    )
    st.dataframe(technical_frame, width="stretch")
