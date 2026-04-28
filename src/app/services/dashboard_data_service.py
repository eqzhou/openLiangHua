from __future__ import annotations

import os
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

import pandas as pd

from src.app.repositories.config_repository import (
    load_experiment_config as repo_load_experiment_config,
    load_watchlist_config as repo_load_watchlist_config,
    save_experiment_config as repo_save_experiment_config,
)
from src.app.repositories.report_repository import (
    load_daily_bar as repo_load_daily_bar,
    load_dataset_summary as repo_load_dataset_summary,
    load_diagnostic_table as repo_load_diagnostic_table,
    load_feature_history_for_symbol as repo_load_feature_history_for_symbol,
    load_feature_importance as repo_load_feature_importance,
    load_feature_panel as repo_load_feature_panel,
    load_latest_symbol_markdown as repo_load_latest_symbol_markdown,
    load_metrics as repo_load_metrics,
    load_overlay_candidate_record as repo_load_overlay_candidate_record,
    load_overlay_candidate_summary_records as repo_load_overlay_candidate_summary_records,
    load_overlay_brief as repo_load_overlay_brief,
    load_overlay_candidates as repo_load_overlay_candidates,
    load_overlay_inference_brief as repo_load_overlay_inference_brief,
    load_overlay_inference_candidates as repo_load_overlay_inference_candidates,
    load_overlay_inference_shortlist as repo_load_overlay_inference_shortlist,
    load_overlay_llm_bundle as repo_load_overlay_llm_bundle,
    load_overlay_inference_packet as repo_load_overlay_inference_packet,
    load_overlay_packet as repo_load_overlay_packet,
    load_portfolio as repo_load_portfolio,
    load_prediction_history_for_symbol as repo_load_prediction_history_for_symbol,
    load_predictions as repo_load_predictions,
    load_stability as repo_load_stability,
    load_watchlist_record as repo_load_watchlist_record,
    load_watchlist_overview as repo_load_watchlist_overview,
    load_watchlist_filtered_count as repo_load_watchlist_filtered_count,
    load_watchlist_summary_records as repo_load_watchlist_summary_records,
)
from src.app.services.dashboard_snapshot_service import (
    build_candidate_snapshot as snapshot_build_candidate_snapshot,
    build_factor_explorer_snapshot as snapshot_build_factor_explorer_snapshot,
    build_watchlist_base_frame as snapshot_build_watchlist_base_frame,
    clear_snapshot_caches,
    load_candidate_snapshot as snapshot_load_candidate_snapshot,
    load_factor_explorer_snapshot as snapshot_load_factor_explorer_snapshot,
    load_watchlist_snapshot as snapshot_load_watchlist_snapshot,
)
from src.db.dashboard_sync import (
    sync_dashboard_artifacts,
)
from src.utils.data_source import active_data_source
from src.utils.io import project_root

ROOT = project_root()
PROJECT_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"


def _current_data_source() -> str:
    return active_data_source()

MODEL_NAMES = ["ridge", "lgbm", "ensemble"]
SPLITS = ["valid", "test"]
LABEL_OPTIONS = ["ret_t1_t5", "ret_t1_t10", "ret_t1_t20"]
MODEL_LABELS = {
    "ridge": "岭回归基线",
    "lgbm": "梯度提升树基线",
    "ensemble": "自适应融合策略",
}
SPLIT_LABELS = {
    "valid": "验证集",
    "test": "测试集",
}
WATCH_SCOPE_MAP = {
    "all": "全部",
    "holdings": "只看持仓",
    "focus": "只看重点关注",
    "overlay": "只看 AI 精选",
    "inference": "只看最新推理池",
    "loss": "只看浮亏较大",
}
WATCH_SORT_MAP = {
    "inference_rank": "最新推理排名",
    "historical_rank": "历史验证排名",
    "drawdown": "浮亏比例",
    "market_value": "参考市值",
}
FIELD_EXPLANATIONS = {
    "mom_5": "近 5 个交易日的趋势强度，数值越高通常代表短线更强。",
    "mom_20": "近 20 个交易日的趋势强度，常用于观察中短期强弱。",
    "mom_60": "近 60 个交易日的趋势强度，更接近中期趋势判断。",
    "mom_120": "近 120 个交易日的趋势强度，用来观察更长周期的延续性。",
    "vol_20": "近 20 日收益波动水平，越高代表短期价格起伏更大。",
    "vol_60": "近 60 日收益波动水平，用来观察中期稳定性。",
    "close_to_ma_20": "当前价格相对 20 日均线的位置，正值代表强于短期均线。",
    "close_to_ma_60": "当前价格相对 60 日均线的位置，正值代表强于中期均线。",
    "turnover_rate": "股票当日换手率，反映当天成交活跃度。",
    "turnover_20": "近 20 日平均换手，数值越高通常代表阶段性更活跃。",
    "amount_20": "近 20 日平均成交额，反映股票的成交容量。",
    "downside_vol_20": "近 20 日只统计下跌部分的波动，越高说明下跌过程更剧烈。",
    "ret_skew_20": "近 20 日收益分布偏斜程度，用来观察大涨大跌的偏向。",
    "drawdown_60": "近 60 日相对阶段高点的回撤幅度，越低表示回撤越深。",
}
METRIC_EXPLANATIONS = {
    "rank_ic_mean": "看模型能不能把未来更强的股票排到前面，越高越好。",
    "top_n_hit_rate": "看前 N 只高分股票里有多少最终赚钱，越高越好。",
    "top_n_forward_mean": "看前 N 只高分股票平均未来收益是多少，越高越好。",
    "daily_portfolio_annualized_return": "把组合日收益折算成年化收益，用来看长期回报水平。",
    "daily_portfolio_sharpe": "收益和波动的性价比，通常越高代表风险回报比越好。",
    "daily_portfolio_max_drawdown": "组合历史上最深的一次回撤，绝对值越小越稳。",
    "avg_turnover_ratio": "看每次调仓大概换掉多少仓位，越低通常越容易落地。",
}

ACTION_SPECS = [
    {
        "actionName": "materialize",
        "label": "刷新部分面板",
        "spinnerText": "正在汇总已缓存股票...",
        "moduleName": "src.data.materialize_cache",
        "buttonKey": "action_materialize",
    },
    {
        "actionName": "features",
        "label": "重建特征与标签",
        "spinnerText": "正在构建特征和标签...",
        "moduleName": "src.features.build_feature_panel",
        "buttonKey": "action_features",
    },
    {
        "actionName": "ridge",
        "label": "运行岭回归基线",
        "spinnerText": "正在训练岭回归模型...",
        "moduleName": "src.models.train_linear",
        "buttonKey": "action_ridge",
    },
    {
        "actionName": "lgbm",
        "label": "运行梯度提升树基线",
        "spinnerText": "正在训练梯度提升树模型...",
        "moduleName": "src.models.train_lgbm",
        "buttonKey": "action_lgbm",
    },
    {
        "actionName": "ensemble",
        "label": "运行自适应融合策略",
        "spinnerText": "正在汇总单模型结果并生成融合策略...",
        "moduleName": "src.models.train_ensemble",
        "buttonKey": "action_ensemble",
    },
    {
        "actionName": "overlay",
        "label": "生成AI研判摘要",
        "spinnerText": "正在生成多模型共识和中文研判摘要...",
        "moduleName": "src.agents.overlay_report",
        "buttonKey": "action_overlay",
    },
    {
        "actionName": "latest_inference",
        "label": "生成最新未标注截面推理",
        "spinnerText": "正在生成最新未标注截面推理和 AI 候选池...",
        "moduleName": "src.agents.overlay_inference_report",
        "buttonKey": "action_latest_inference",
    },
]

# Override previously polluted copy with clean UI-facing labels.
MODEL_LABELS = {
    "ridge": "岭回归基线",
    "lgbm": "梯度提升树基线",
    "ensemble": "自适应融合策略",
}
SPLIT_LABELS = {
    "valid": "验证集",
    "test": "测试集",
}
WATCH_SCOPE_MAP = {
    "all": "全部",
    "holdings": "只看持仓",
    "focus": "只看重点关注",
    "overlay": "只看 AI 精选",
    "inference": "只看最新推理池",
    "loss": "只看浮亏较大",
}
WATCH_SORT_MAP = {
    "inference_rank": "最新推理排名",
    "historical_rank": "历史验证排名",
    "drawdown": "浮亏比例",
    "market_value": "参考市值",
}
FIELD_EXPLANATIONS = {
    "mom_5": "近 5 个交易日的趋势强度，数值越高通常代表短线更强。",
    "mom_20": "近 20 个交易日的趋势强度，常用于观察中短期强弱。",
    "mom_60": "近 60 个交易日的趋势强度，更接近中期趋势判断。",
    "mom_120": "近 120 个交易日的趋势强度，用来观察更长周期的延续性。",
    "vol_20": "近 20 日收益波动水平，越高代表短期价格起伏更大。",
    "vol_60": "近 60 日收益波动水平，用来观察中期稳定性。",
    "close_to_ma_20": "当前价格相对 20 日均线的位置，正值通常代表强于短期均线。",
    "close_to_ma_60": "当前价格相对 60 日均线的位置，正值通常代表强于中期均线。",
    "turnover_rate": "股票当日换手率，反映当天成交活跃度。",
    "turnover_20": "近 20 日平均换手，数值越高通常代表阶段性更活跃。",
    "amount_20": "近 20 日平均成交额，反映股票的成交容量。",
    "downside_vol_20": "近 20 日只统计下跌部分的波动，越高说明下跌过程更剧烈。",
    "ret_skew_20": "近 20 日收益分布偏斜程度，用来观察大涨大跌的偏向。",
    "drawdown_60": "近 60 日相对阶段高点的回撤幅度，越低表示回撤越深。",
}
METRIC_EXPLANATIONS = {
    "rank_ic_mean": "看模型能不能把未来更强的股票排到前面，越高越好。",
    "top_n_hit_rate": "看前 N 只高分股票里有多少最终赚钱，越高越好。",
    "top_n_forward_mean": "看前 N 只高分股票平均未来收益是多少，越高越好。",
    "daily_portfolio_annualized_return": "把组合日收益折算成年化收益，用来看长期回报水平。",
    "daily_portfolio_sharpe": "收益和波动的性价比，通常越高代表风险回报比越好。",
    "daily_portfolio_max_drawdown": "组合历史上最深的一次回撤，绝对值越小越稳。",
    "avg_turnover_ratio": "看每次调仓大概换掉多少仓位，越低通常越容易落地。",
}
ACTION_SPECS = [
    {
        "actionName": "materialize",
        "label": "刷新部分面板",
        "spinnerText": "正在汇总已缓存股票...",
        "moduleName": "src.data.materialize_cache",
        "buttonKey": "action_materialize",
    },
    {
        "actionName": "features",
        "label": "重建特征与标签",
        "spinnerText": "正在构建特征和标签...",
        "moduleName": "src.features.build_feature_panel",
        "buttonKey": "action_features",
    },
    {
        "actionName": "ridge",
        "label": "运行岭回归基线",
        "spinnerText": "正在训练岭回归模型...",
        "moduleName": "src.models.train_linear",
        "buttonKey": "action_ridge",
    },
    {
        "actionName": "lgbm",
        "label": "运行梯度提升树基线",
        "spinnerText": "正在训练梯度提升树模型...",
        "moduleName": "src.models.train_lgbm",
        "buttonKey": "action_lgbm",
    },
    {
        "actionName": "ensemble",
        "label": "运行自适应融合策略",
        "spinnerText": "正在汇总单模型结果并生成融合策略...",
        "moduleName": "src.models.train_ensemble",
        "buttonKey": "action_ensemble",
    },
    {
        "actionName": "overlay",
        "label": "生成 AI 研判摘要",
        "spinnerText": "正在生成多模型共识和中文研判摘要...",
        "moduleName": "src.agents.overlay_report",
        "buttonKey": "action_overlay",
    },
    {
        "actionName": "latest_inference",
        "label": "生成最新未标注截面推理",
        "spinnerText": "正在生成最新未标注截面推理和 AI 候选池...",
        "moduleName": "src.agents.overlay_inference_report",
        "buttonKey": "action_latest_inference",
    },
]


MODEL_LABELS = {
    "ridge": "岭回归基线",
    "lgbm": "梯度提升树基线",
    "ensemble": "自适应融合策略",
}
SPLIT_LABELS = {
    "valid": "验证集",
    "test": "测试集",
}
WATCH_SCOPE_MAP = {
    "all": "全部",
    "holdings": "只看持仓",
    "focus": "只看重点关注",
    "overlay": "只看 AI 精选",
    "inference": "只看最新推理池",
    "loss": "只看浮亏较大",
}
WATCH_SORT_MAP = {
    "inference_rank": "最新推理排名",
    "historical_rank": "历史验证排名",
    "drawdown": "浮亏比例",
    "market_value": "参考市值",
}
FIELD_EXPLANATIONS = {
    "mom_5": "近 5 个交易日的趋势强度，数值越高通常代表短线更强。",
    "mom_20": "近 20 个交易日的趋势强度，常用于观察中短期强弱。",
    "mom_60": "近 60 个交易日的趋势强度，更接近中期趋势判断。",
    "mom_120": "近 120 个交易日的趋势强度，用来观察更长周期的延续性。",
    "vol_20": "近 20 日收益波动水平，越高代表短期价格起伏更大。",
    "vol_60": "近 60 日收益波动水平，用来观察中期稳定性。",
    "close_to_ma_20": "当前价格相对 20 日均线的位置，正值通常代表强于短期均线。",
    "close_to_ma_60": "当前价格相对 60 日均线的位置，正值通常代表强于中期均线。",
    "turnover_rate": "股票当日换手率，反映当天成交活跃度。",
    "turnover_20": "近 20 日平均换手，数值越高通常代表阶段性更活跃。",
    "amount_20": "近 20 日平均成交额，反映股票的成交容量。",
    "downside_vol_20": "近 20 日只统计下跌部分的波动，越高说明下跌过程更剧烈。",
    "ret_skew_20": "近 20 日收益分布偏斜程度，用来观察大涨大跌的偏向。",
    "drawdown_60": "近 60 日相对阶段高点的回撤幅度，越低表示回撤越深。",
}
METRIC_EXPLANATIONS = {
    "rank_ic_mean": "看模型能不能把未来更强的股票排到前面，越高越好。",
    "top_n_hit_rate": "看前 N 只高分股票里有多少最终赚钱，越高越好。",
    "top_n_forward_mean": "看前 N 只高分股票平均未来收益是多少，越高越好。",
    "daily_portfolio_annualized_return": "把组合日收益折算成年化收益，用来看长期回报水平。",
    "daily_portfolio_sharpe": "收益和波动的性价比，通常越高代表风险回报比越好。",
    "daily_portfolio_max_drawdown": "组合历史上最深的一次回撤，绝对值越小越稳。",
    "avg_turnover_ratio": "看每次调仓大概换掉多少仓位，越低通常越容易落地。",
}
ACTION_SPECS = [
    {
        "actionName": "materialize",
        "label": "刷新局部面板",
        "spinnerText": "正在汇总已缓存股票...",
        "moduleName": "src.data.materialize_cache",
        "buttonKey": "action_materialize",
    },
    {
        "actionName": "features",
        "label": "重建特征与标签",
        "spinnerText": "正在构建特征和标签...",
        "moduleName": "src.features.build_feature_panel",
        "buttonKey": "action_features",
    },
    {
        "actionName": "ridge",
        "label": "运行岭回归基线",
        "spinnerText": "正在训练岭回归模型...",
        "moduleName": "src.models.train_linear",
        "buttonKey": "action_ridge",
    },
    {
        "actionName": "lgbm",
        "label": "运行梯度提升树基线",
        "spinnerText": "正在训练梯度提升树模型...",
        "moduleName": "src.models.train_lgbm",
        "buttonKey": "action_lgbm",
    },
    {
        "actionName": "ensemble",
        "label": "运行自适应融合策略",
        "spinnerText": "正在汇总单模型结果并生成融合策略...",
        "moduleName": "src.models.train_ensemble",
        "buttonKey": "action_ensemble",
    },
    {
        "actionName": "overlay",
        "label": "生成 AI 研判摘要",
        "spinnerText": "正在生成多模型共识和中文研判摘要...",
        "moduleName": "src.agents.overlay_report",
        "buttonKey": "action_overlay",
    },
    {
        "actionName": "latest_inference",
        "label": "生成最新未标注截面推理",
        "spinnerText": "正在生成最新未标注截面推理和 AI 候选池...",
        "moduleName": "src.agents.overlay_inference_report",
        "buttonKey": "action_latest_inference",
    },
]

def run_module(module_name: str, *, extra_env: dict[str, str] | None = None) -> tuple[bool, str]:
    python_executable = str(PROJECT_PYTHON if PROJECT_PYTHON.exists() else Path(sys.executable))
    command = [python_executable, "-m", module_name]
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    result = subprocess.run(
        command,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    output = result.stdout
    if result.stderr:
        output = f"{output}\n{result.stderr}".strip()
    return result.returncode == 0, output.strip()


def sync_dashboard_database() -> tuple[bool, str]:
    summary = sync_dashboard_artifacts(root=ROOT, data_source=_current_data_source())
    return summary.ok, summary.message


def list_available_actions() -> list[dict[str, str]]:
    return [dict(spec) for spec in ACTION_SPECS]


def clear_dashboard_data_caches() -> None:
    for cached_func in (
        load_experiment_config,
        load_watchlist_config,
        load_dataset_summary,
        load_feature_panel,
        load_daily_bar,
        load_overlay_candidates,
        load_overlay_packet,
        load_overlay_brief,
        load_overlay_inference_candidates,
        load_overlay_inference_shortlist,
        load_overlay_inference_packet,
        load_overlay_inference_brief,
        load_overlay_llm_bundle,
        load_predictions,
        load_candidate_snapshot,
        build_candidate_snapshot,
        load_factor_explorer_snapshot,
        build_factor_explorer_snapshot,
        load_portfolio,
        load_metrics,
        load_feature_importance,
        load_diagnostic_table,
        load_stability,
        load_latest_symbol_markdown,
        build_metrics_table,
        build_watchlist_base_frame,
        load_watchlist_snapshot,
    ):
        getattr(cached_func, "cache_clear", lambda: None)()
    clear_snapshot_caches()


@lru_cache(maxsize=1)
def load_experiment_config() -> dict:
    return repo_load_experiment_config(ROOT)


def save_experiment_config(config: dict) -> None:
    repo_save_experiment_config(config, ROOT)
    sync_dashboard_database()
    clear_dashboard_data_caches()


@lru_cache(maxsize=128)
def load_watchlist_config(user_id: str | None = None) -> dict:
    return repo_load_watchlist_config(ROOT, user_id=user_id)


def load_dataset_summary() -> dict[str, object]:
    return repo_load_dataset_summary(ROOT, data_source=_current_data_source())


def load_feature_panel() -> pd.DataFrame:
    return repo_load_feature_panel(ROOT, data_source=_current_data_source())


def load_feature_history_for_symbol(symbol: str, factor_name: str) -> pd.DataFrame:
    return repo_load_feature_history_for_symbol(ROOT, data_source=_current_data_source(), symbol=symbol, factor_name=factor_name)


def load_daily_bar() -> pd.DataFrame:
    return repo_load_daily_bar(ROOT, data_source=_current_data_source())


def load_overlay_candidates(user_id: str | None = None) -> pd.DataFrame:
    return repo_load_overlay_candidates(ROOT, data_source=_current_data_source(), user_id=user_id)


def load_overlay_candidate_summary_records(
    scope: str,
    field_names: list[str],
    user_id: str | None = None,
) -> list[dict[str, object]]:
    return repo_load_overlay_candidate_summary_records(
        ROOT,
        data_source=_current_data_source(),
        scope=scope,
        field_names=field_names,
        user_id=user_id,
    )


def load_overlay_candidate_record(
    scope: str,
    symbol: str,
    field_names: list[str] | None = None,
    user_id: str | None = None,
) -> dict[str, object]:
    return repo_load_overlay_candidate_record(
        ROOT,
        data_source=_current_data_source(),
        scope=scope,
        symbol=symbol,
        field_names=field_names,
        user_id=user_id,
    )


def load_overlay_packet(user_id: str | None = None) -> dict:
    return repo_load_overlay_packet(ROOT, data_source=_current_data_source(), user_id=user_id)


def load_overlay_brief(user_id: str | None = None) -> str:
    return repo_load_overlay_brief(ROOT, data_source=_current_data_source(), user_id=user_id)


def load_overlay_inference_candidates(user_id: str | None = None) -> pd.DataFrame:
    return repo_load_overlay_inference_candidates(ROOT, data_source=_current_data_source(), user_id=user_id)


def load_overlay_inference_shortlist(user_id: str | None = None) -> str:
    return repo_load_overlay_inference_shortlist(ROOT, data_source=_current_data_source(), user_id=user_id)


def load_overlay_inference_packet(user_id: str | None = None) -> dict:
    return repo_load_overlay_inference_packet(ROOT, data_source=_current_data_source(), user_id=user_id)


def load_overlay_inference_brief(user_id: str | None = None) -> str:
    return repo_load_overlay_inference_brief(ROOT, data_source=_current_data_source(), user_id=user_id)


def load_overlay_llm_bundle(scope: str, user_id: str | None = None) -> dict[str, object]:
    normalized_scope = "inference" if scope == "inference" else "historical"
    packet = (
        load_overlay_inference_packet(user_id=user_id)
        if normalized_scope == "inference"
        else load_overlay_packet(user_id=user_id)
    )
    return repo_load_overlay_llm_bundle(
        ROOT,
        data_source=_current_data_source(),
        scope=normalized_scope,
        packet=packet,
        user_id=user_id,
    )


def load_predictions(model_name: str, split_name: str, user_id: str | None = None) -> pd.DataFrame:
    return repo_load_predictions(
        ROOT,
        data_source=_current_data_source(),
        model_name=model_name,
        split_name=split_name,
        user_id=user_id,
    )


def load_prediction_history_for_symbol(model_name: str, split_name: str, symbol: str) -> pd.DataFrame:
    return repo_load_prediction_history_for_symbol(
        ROOT,
        data_source=_current_data_source(),
        model_name=model_name,
        split_name=split_name,
        symbol=symbol,
    )


def build_candidate_snapshot(model_name: str, split_name: str) -> pd.DataFrame:
    return snapshot_build_candidate_snapshot(model_name, split_name)


def load_candidate_snapshot(model_name: str, split_name: str) -> pd.DataFrame | None:
    return snapshot_load_candidate_snapshot(model_name, split_name)


def build_factor_explorer_snapshot() -> dict[str, object]:
    return snapshot_build_factor_explorer_snapshot(tuple(sorted(FIELD_EXPLANATIONS.items())))


def load_factor_explorer_snapshot() -> dict[str, object] | None:
    return snapshot_load_factor_explorer_snapshot()


def load_portfolio(model_name: str, split_name: str) -> pd.DataFrame:
    return repo_load_portfolio(ROOT, data_source=_current_data_source(), model_name=model_name, split_name=split_name)


def load_metrics(model_name: str, split_name: str) -> dict:
    return repo_load_metrics(ROOT, data_source=_current_data_source(), model_name=model_name, split_name=split_name)


def load_feature_importance(model_name: str) -> pd.DataFrame:
    return repo_load_feature_importance(ROOT, data_source=_current_data_source(), model_name=model_name)


def load_diagnostic_table(model_name: str, split_name: str, table_name: str) -> pd.DataFrame:
    return repo_load_diagnostic_table(
        ROOT,
        data_source=_current_data_source(),
        model_name=model_name,
        split_name=split_name,
        table_name=table_name,
    )


def load_stability(model_name: str) -> dict:
    return repo_load_stability(ROOT, data_source=_current_data_source(), model_name=model_name)


def load_latest_symbol_markdown(
    symbol: str,
    note_kind: str,
    *,
    user_id: str | None = None,
) -> dict[str, str]:
    return repo_load_latest_symbol_markdown(
        symbol,
        note_kind,
        root=ROOT,
        data_source=_current_data_source(),
        user_id=user_id,
    )


def load_watchlist_summary_records(
    field_names: list[str],
    *,
    keyword: str = "",
    scope: str = "all",
    sort_by: str = "inference_rank",
    page: int = 1,
    page_size: int = 30,
    user_id: str | None = None,
) -> list[dict[str, object]]:
    return repo_load_watchlist_summary_records(
        ROOT,
        data_source=_current_data_source(),
        field_names=field_names,
        keyword=keyword,
        scope=scope,
        sort_by=sort_by,
        page=page,
        page_size=page_size,
        user_id=user_id,
    )


def load_watchlist_record(
    symbol: str,
    field_names: list[str] | None = None,
    *,
    user_id: str | None = None,
) -> dict[str, object]:
    return repo_load_watchlist_record(
        ROOT,
        data_source=_current_data_source(),
        symbol=symbol,
        field_names=field_names,
        user_id=user_id,
    )


def load_watchlist_overview(user_id: str | None = None) -> dict[str, object]:
    return repo_load_watchlist_overview(ROOT, data_source=_current_data_source(), user_id=user_id)


def load_watchlist_filtered_count(*, keyword: str = "", scope: str = "all", user_id: str | None = None) -> int:
    return repo_load_watchlist_filtered_count(
        ROOT,
        data_source=_current_data_source(),
        keyword=keyword,
        scope=scope,
        user_id=user_id,
    )


def build_metrics_table() -> pd.DataFrame:
    rows: list[dict] = []
    for model_name in MODEL_NAMES:
        for split_name in SPLITS:
            metrics = load_metrics(model_name, split_name)
            if metrics:
                row = {"model": MODEL_LABELS[model_name], "split": SPLIT_LABELS[split_name]}
                row.update(metrics)
                rows.append(row)
    return pd.DataFrame(rows)


def build_watchlist_base_frame(user_id: str | None = None) -> pd.DataFrame:
    return snapshot_build_watchlist_base_frame(user_id=user_id)


def load_watchlist_snapshot(user_id: str | None = None) -> pd.DataFrame | None:
    return snapshot_load_watchlist_snapshot(user_id=user_id)
