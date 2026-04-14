from __future__ import annotations

import streamlit as st

from src.app.ui.ui_text import APP_CAPTION, APP_TITLE, WORKSPACE_LABEL


def render_app_shell(page_labels: list[str], *, current_page_key: str = "active_page") -> str:
    st.title(APP_TITLE)
    st.caption(APP_CAPTION)
    return st.radio(
        WORKSPACE_LABEL,
        page_labels,
        key=current_page_key,
        horizontal=True,
        label_visibility="collapsed",
    )


def render_current_config_caption(summary_text: str) -> None:
    st.caption(summary_text)

