from __future__ import annotations

import streamlit as st


def format_listener_pids(listener_pids: object) -> str:
    if isinstance(listener_pids, list):
        values = [str(pid) for pid in listener_pids if str(pid).strip()]
        return ", ".join(values)
    if listener_pids is None:
        return ""
    text = str(listener_pids).strip()
    return text


def render_service_page(streamlit_service_status: dict[str, object]) -> None:
    st.subheader("页面服务")
    service_cols = st.columns(4)
    service_cols[0].metric("服务状态", str(streamlit_service_status.get("status_label_display", "未知")))
    service_cols[1].metric("守护进程 PID", str(streamlit_service_status.get("supervisor_pid") or "-"))
    service_cols[2].metric("页面进程 PID", str(streamlit_service_status.get("streamlit_pid") or "-"))
    service_cols[3].metric("8501 端口", "监听中" if streamlit_service_status.get("listener_present") else "未监听")

    listener_text = format_listener_pids(streamlit_service_status.get("listener_pids"))
    if listener_text:
        st.write(f"监听进程：{listener_text}")
    if streamlit_service_status.get("stale_status"):
        st.caption("检测到残留的页面状态文件，当前显示已按真实进程状态纠正。")

    st.write("本地地址：http://localhost:8501")
    st.info("下载任务已从界面移出。后续请在单独的 CMD 或 PowerShell 窗口执行数据下载与补数任务。")

    with st.expander("查看页面标准输出", expanded=False):
        st.code(streamlit_service_status.get("out_log_tail") or "(页面标准输出暂无内容)", language="text")
    with st.expander("查看页面错误输出", expanded=False):
        st.code(streamlit_service_status.get("err_log_tail") or "(页面错误输出暂无内容)", language="text")

    st.markdown("**当前说明**")
    st.markdown(
        """
- 当前仍是免费数据路线，存在“当前成分股回看历史”的幸存者偏差。
- 现阶段重点是把研究平台、数据链路和模型流程跑通。
- 下载与补数任务已改为界面外执行，界面只保留研究、持仓和 AI 研判相关操作。
"""
    )
