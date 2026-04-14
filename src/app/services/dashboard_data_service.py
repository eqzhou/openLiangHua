from __future__ import annotations

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
    load_candidate_snapshot as repo_load_candidate_snapshot,
    load_daily_bar as repo_load_daily_bar,
    load_dataset_summary as repo_load_dataset_summary,
    load_diagnostic_table as repo_load_diagnostic_table,
    load_factor_explorer_snapshot as repo_load_factor_explorer_snapshot,
    load_feature_importance as repo_load_feature_importance,
    load_feature_panel as repo_load_feature_panel,
    load_latest_symbol_markdown as repo_load_latest_symbol_markdown,
    load_metrics as repo_load_metrics,
    load_overlay_brief as repo_load_overlay_brief,
    load_overlay_candidates as repo_load_overlay_candidates,
    load_overlay_inference_brief as repo_load_overlay_inference_brief,
    load_overlay_inference_candidates as repo_load_overlay_inference_candidates,
    load_overlay_inference_packet as repo_load_overlay_inference_packet,
    load_overlay_packet as repo_load_overlay_packet,
    load_portfolio as repo_load_portfolio,
    load_predictions as repo_load_predictions,
    load_stability as repo_load_stability,
    load_watchlist_snapshot as repo_load_watchlist_snapshot,
)
from src.app.viewmodels.factor_explorer_vm import build_missing_rate_table, list_numeric_factor_columns
from src.app.services.watchlist_service import build_watchlist_view
from src.db.dashboard_sync import (
    sync_candidate_snapshot_artifact,
    sync_dashboard_artifacts,
    sync_factor_explorer_snapshot_artifact,
    sync_watchlist_snapshot_artifact,
)
from src.utils.data_source import active_data_source
from src.utils.io import project_root

ROOT = project_root()
ACTIVE_DATA_SOURCE = active_data_source()
PROJECT_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"

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

def run_module(module_name: str) -> tuple[bool, str]:
    python_executable = str(PROJECT_PYTHON if PROJECT_PYTHON.exists() else Path(sys.executable))
    command = [python_executable, "-m", module_name]
    result = subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    output = result.stdout
    if result.stderr:
        output = f"{output}\n{result.stderr}".strip()
    return result.returncode == 0, output.strip()


def sync_dashboard_database() -> tuple[bool, str]:
    summary = sync_dashboard_artifacts(root=ROOT, data_source=ACTIVE_DATA_SOURCE)
    return summary.ok, summary.message


def list_available_actions() -> list[dict[str, str]]:
    return [dict(spec) for spec in ACTION_SPECS]


def clear_dashboard_data_caches() -> None:
    load_experiment_config.cache_clear()
    load_watchlist_config.cache_clear()
    load_dataset_summary.cache_clear()
    load_feature_panel.cache_clear()
    load_daily_bar.cache_clear()
    load_overlay_candidates.cache_clear()
    load_overlay_packet.cache_clear()
    load_overlay_brief.cache_clear()
    load_overlay_inference_candidates.cache_clear()
    load_overlay_inference_packet.cache_clear()
    load_overlay_inference_brief.cache_clear()
    load_predictions.cache_clear()
    load_candidate_snapshot.cache_clear()
    build_candidate_snapshot.cache_clear()
    load_factor_explorer_snapshot.cache_clear()
    build_factor_explorer_snapshot.cache_clear()
    load_portfolio.cache_clear()
    load_metrics.cache_clear()
    load_feature_importance.cache_clear()
    load_diagnostic_table.cache_clear()
    load_stability.cache_clear()
    load_latest_symbol_markdown.cache_clear()
    load_watchlist_snapshot.cache_clear()
    build_metrics_table.cache_clear()
    build_watchlist_base_frame.cache_clear()


@lru_cache(maxsize=1)
def load_experiment_config() -> dict:
    return repo_load_experiment_config(ROOT)


def save_experiment_config(config: dict) -> None:
    repo_save_experiment_config(config, ROOT)
    sync_dashboard_database()
    clear_dashboard_data_caches()


@lru_cache(maxsize=1)
def load_watchlist_config() -> dict:
    return repo_load_watchlist_config(ROOT)


@lru_cache(maxsize=1)
def load_dataset_summary() -> dict[str, object]:
    return repo_load_dataset_summary(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
def load_feature_panel() -> pd.DataFrame:
    return repo_load_feature_panel(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
def load_daily_bar() -> pd.DataFrame:
    return repo_load_daily_bar(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
def load_overlay_candidates() -> pd.DataFrame:
    return repo_load_overlay_candidates(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
def load_overlay_packet() -> dict:
    return repo_load_overlay_packet(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
def load_overlay_brief() -> str:
    return repo_load_overlay_brief(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
def load_overlay_inference_candidates() -> pd.DataFrame:
    return repo_load_overlay_inference_candidates(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
def load_overlay_inference_packet() -> dict:
    return repo_load_overlay_inference_packet(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
def load_overlay_inference_brief() -> str:
    return repo_load_overlay_inference_brief(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=8)
def load_predictions(model_name: str, split_name: str) -> pd.DataFrame:
    return repo_load_predictions(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name, split_name=split_name)


@lru_cache(maxsize=16)
def load_candidate_snapshot(model_name: str, split_name: str) -> pd.DataFrame | None:
    return repo_load_candidate_snapshot(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name, split_name=split_name)


@lru_cache(maxsize=1)
def load_factor_explorer_snapshot() -> dict[str, object] | None:
    return repo_load_factor_explorer_snapshot(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=16)
def build_candidate_snapshot(model_name: str, split_name: str) -> pd.DataFrame:
    stored_snapshot = load_candidate_snapshot(model_name, split_name)
    if stored_snapshot is not None:
        return stored_snapshot.copy()

    predictions = load_predictions(model_name, split_name)
    from src.utils.prediction_snapshot import build_latest_prediction_snapshot

    snapshot = build_latest_prediction_snapshot(predictions)
    sync_candidate_snapshot_artifact(
        root=ROOT,
        data_source=ACTIVE_DATA_SOURCE,
        model_name=model_name,
        split_name=split_name,
        predictions=predictions,
        snapshot_frame=snapshot,
    )
    load_candidate_snapshot.cache_clear()
    return snapshot


@lru_cache(maxsize=1)
def build_factor_explorer_snapshot() -> dict[str, object]:
    stored_snapshot = load_factor_explorer_snapshot()
    if stored_snapshot is not None:
        return dict(stored_snapshot)

    feature_panel = load_feature_panel()
    numeric_columns = list_numeric_factor_columns(feature_panel)
    if not numeric_columns:
        return {
            "available": False,
            "latestDate": None,
            "factorOptions": [],
            "symbolOptions": [],
            "crossSection": [],
            "missingRates": [],
        }

    latest_date = feature_panel["trade_date"].max()
    cross_section = feature_panel.loc[feature_panel["trade_date"] == latest_date].copy()
    snapshot_payload: dict[str, object] = {
        "available": True,
        "latestDate": latest_date.isoformat() if isinstance(latest_date, pd.Timestamp) else str(latest_date),
        "factorOptions": [
            {"key": column, "label": column, "description": FIELD_EXPLANATIONS.get(column, "")}
            for column in numeric_columns
        ],
        "symbolOptions": cross_section["ts_code"].sort_values().tolist(),
        "crossSection": cross_section.to_dict(orient="records"),
        "missingRates": build_missing_rate_table(feature_panel, numeric_columns).to_dict(orient="records"),
    }
    sync_factor_explorer_snapshot_artifact(
        root=ROOT,
        data_source=ACTIVE_DATA_SOURCE,
        feature_panel=feature_panel,
        snapshot_payload=snapshot_payload,
    )
    load_factor_explorer_snapshot.cache_clear()
    return snapshot_payload


@lru_cache(maxsize=8)
def load_portfolio(model_name: str, split_name: str) -> pd.DataFrame:
    return repo_load_portfolio(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name, split_name=split_name)


@lru_cache(maxsize=8)
def load_metrics(model_name: str, split_name: str) -> dict:
    return repo_load_metrics(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name, split_name=split_name)


@lru_cache(maxsize=4)
def load_feature_importance(model_name: str) -> pd.DataFrame:
    return repo_load_feature_importance(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name)


@lru_cache(maxsize=16)
def load_diagnostic_table(model_name: str, split_name: str, table_name: str) -> pd.DataFrame:
    return repo_load_diagnostic_table(
        ROOT,
        data_source=ACTIVE_DATA_SOURCE,
        model_name=model_name,
        split_name=split_name,
        table_name=table_name,
    )


@lru_cache(maxsize=4)
def load_stability(model_name: str) -> dict:
    return repo_load_stability(ROOT, data_source=ACTIVE_DATA_SOURCE, model_name=model_name)


@lru_cache(maxsize=64)
def load_latest_symbol_markdown(symbol: str, note_kind: str) -> dict[str, str]:
    return repo_load_latest_symbol_markdown(symbol, note_kind, root=ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
def load_watchlist_snapshot() -> pd.DataFrame | None:
    return repo_load_watchlist_snapshot(ROOT, data_source=ACTIVE_DATA_SOURCE)


@lru_cache(maxsize=1)
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


@lru_cache(maxsize=1)
def build_watchlist_base_frame() -> pd.DataFrame:
    watchlist_snapshot = load_watchlist_snapshot()
    if watchlist_snapshot is not None:
        return watchlist_snapshot.copy()

    watchlist_config = load_watchlist_config()
    frame = build_watchlist_view(
        root=ROOT,
        data_source=ACTIVE_DATA_SOURCE,
        watchlist_config=watchlist_config,
        daily_bar=load_daily_bar(),
        ridge_predictions=load_predictions("ridge", "test"),
        lgbm_predictions=load_predictions("lgbm", "test"),
        ensemble_predictions=load_predictions("ensemble", "test"),
        overlay_candidates=load_overlay_candidates(),
        ensemble_inference_predictions=load_predictions("ensemble", "inference"),
        overlay_inference_candidates=load_overlay_inference_candidates(),
    )
    sync_watchlist_snapshot_artifact(
        root=ROOT,
        data_source=ACTIVE_DATA_SOURCE,
        watchlist_config=watchlist_config,
        snapshot_frame=frame,
    )
    load_watchlist_snapshot.cache_clear()
    return frame
