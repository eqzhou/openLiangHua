from __future__ import annotations

from collections.abc import Callable
from datetime import date

import pandas as pd
import streamlit as st

from src.app.ui.ui_text import (
    CLEAR_CACHE_LABEL,
    CLEAR_CACHE_SUCCESS_MESSAGE,
    CONFIG_INVALID_DATES_MESSAGE,
    CONFIG_LABEL_COL_LABEL,
    CONFIG_SAVE_SUCCESS_MESSAGE,
    CONFIG_SUBMIT_LABEL,
    CONFIG_TEST_END_LABEL,
    CONFIG_TOP_N_LABEL,
    CONFIG_TRAIN_END_LABEL,
    CONFIG_TRAIN_START_LABEL,
    CONFIG_VALID_END_LABEL,
    CURRENT_CONFIG_PREFIX,
    SIDEBAR_ACTIONS_HEADER,
    SIDEBAR_CONFIG_HEADER,
    SIDEBAR_DOWNLOAD_NOTE,
    SIDEBAR_PORT_ACTIVE,
    SIDEBAR_PORT_INACTIVE,
    SIDEBAR_PORT_LABEL,
    SIDEBAR_REFRESH_NOTE,
    SIDEBAR_SERVICE_HEADER,
    SIDEBAR_SERVICE_STATUS_LABEL,
    SIDEBAR_WATCHLIST_ENTRY_LABEL,
)

def has_valid_config_dates(train_start: date, train_end: date, valid_end: date, test_end: date) -> bool:
    return train_start <= train_end <= valid_end <= test_end


def port_status_text(listener_present: bool) -> str:
    return SIDEBAR_PORT_ACTIVE if listener_present else SIDEBAR_PORT_INACTIVE


def render_sidebar(
    *,
    shell_payload: dict[str, object],
    zh: Callable[[str], str],
    save_experiment_config: Callable[[dict], dict],
    refresh_cached_views: Callable[[], None],
    run_named_action: Callable[[str], dict[str, object]],
    clear_cache: Callable[[], dict[str, object]],
    render_action_result: Callable[[], None],
) -> None:
    bootstrap = shell_payload.get("bootstrap", {}) if isinstance(shell_payload.get("bootstrap"), dict) else {}
    experiment_config = shell_payload.get("experimentConfig", {}) if isinstance(shell_payload.get("experimentConfig"), dict) else {}
    streamlit_service_status = shell_payload.get("service", {}) if isinstance(shell_payload.get("service"), dict) else {}
    watchlist_entry_count = int(shell_payload.get("watchlistEntryCount", 0) or 0)
    config_summary_text = str(shell_payload.get("configSummaryText", "") or "")
    label_options = list(bootstrap.get("labelOptions", []) or [])
    action_specs = list(bootstrap.get("actions", []) or [])

    with st.sidebar:
        st.header(SIDEBAR_SERVICE_HEADER)
        st.metric(SIDEBAR_SERVICE_STATUS_LABEL, str(streamlit_service_status.get("status_label_display", "未知")))
        st.metric(SIDEBAR_WATCHLIST_ENTRY_LABEL, int(watchlist_entry_count))
        st.metric(SIDEBAR_PORT_LABEL, port_status_text(bool(streamlit_service_status.get("listener_present"))))
        if streamlit_service_status.get("streamlit_pid"):
            st.code(f"Streamlit PID: {streamlit_service_status['streamlit_pid']}")
        st.caption(SIDEBAR_REFRESH_NOTE)
        st.caption(SIDEBAR_DOWNLOAD_NOTE)

        st.header(SIDEBAR_CONFIG_HEADER)
        with st.form("experiment_config_form", clear_on_submit=False):
            train_start = st.date_input(
                CONFIG_TRAIN_START_LABEL,
                value=pd.Timestamp(experiment_config.get("train_start", "2018-01-01")).date(),
                key="config_train_start",
            )
            train_end = st.date_input(
                CONFIG_TRAIN_END_LABEL,
                value=pd.Timestamp(experiment_config.get("train_end", "2022-12-31")).date(),
                key="config_train_end",
            )
            valid_end = st.date_input(
                CONFIG_VALID_END_LABEL,
                value=pd.Timestamp(experiment_config.get("valid_end", "2023-12-31")).date(),
                key="config_valid_end",
            )
            test_end = st.date_input(
                CONFIG_TEST_END_LABEL,
                value=pd.Timestamp(experiment_config.get("test_end", "2025-12-31")).date(),
                key="config_test_end",
            )
            current_label = experiment_config.get("label_col", "ret_t1_t10")
            label_col = st.selectbox(
                CONFIG_LABEL_COL_LABEL,
                label_options,
                index=label_options.index(current_label) if current_label in label_options else 1,
                format_func=zh,
                key="config_label_col",
            )
            top_n_config = st.number_input(
                CONFIG_TOP_N_LABEL,
                min_value=1,
                max_value=100,
                value=int(experiment_config.get("top_n", 10)),
                step=1,
                key="config_top_n",
            )
            save_config = st.form_submit_button(CONFIG_SUBMIT_LABEL, key="config_submit")

        if save_config:
            if not has_valid_config_dates(train_start, train_end, valid_end, test_end):
                st.error(CONFIG_INVALID_DATES_MESSAGE)
            else:
                updated_config = dict(experiment_config)
                updated_config["train_start"] = str(train_start)
                updated_config["train_end"] = str(train_end)
                updated_config["valid_end"] = str(valid_end)
                updated_config["test_end"] = str(test_end)
                updated_config["label_col"] = label_col
                updated_config["top_n"] = int(top_n_config)
                save_experiment_config(updated_config)
                st.session_state["last_action"] = ("config", True, CONFIG_SAVE_SUCCESS_MESSAGE)
                refresh_cached_views()
                st.rerun()

        st.caption(config_summary_text)

        st.header(SIDEBAR_ACTIONS_HEADER)
        for spec in action_specs:
            action_name = str(spec.get("actionName", "") or "")
            button_label = str(spec.get("label", action_name) or action_name)
            spinner_text = str(spec.get("spinnerText", button_label) or button_label)
            button_key = str(spec.get("buttonKey", f"action_{action_name}") or f"action_{action_name}")
            if st.button(button_label, key=button_key, width="stretch"):
                with st.spinner(spinner_text):
                    result = run_named_action(action_name)
                st.session_state["last_action"] = (
                    action_name,
                    bool(result.get("ok", False)),
                    str(result.get("output", "") or ""),
                )
                refresh_cached_views()
                st.rerun()

        if st.button(CLEAR_CACHE_LABEL, key="action_clear_cache", width="stretch"):
            clear_cache()
            refresh_cached_views()
            st.success(CLEAR_CACHE_SUCCESS_MESSAGE)

        render_action_result()


def build_current_config_caption(config_summary_text: str) -> str:
    return f"{CURRENT_CONFIG_PREFIX}{config_summary_text}"
