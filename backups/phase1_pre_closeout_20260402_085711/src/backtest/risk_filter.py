from __future__ import annotations

import numpy as np
import pandas as pd

from src.utils.io import load_yaml, project_root


def _default_benchmark_code() -> str | None:
    config = load_yaml(project_root() / "config" / "universe.yaml")
    benchmark_code = config.get("benchmark") or config.get("index_code")
    if benchmark_code:
        return str(benchmark_code)
    return None


def _resolve_benchmark_code(panel: pd.DataFrame, configured_code: str | None = None) -> str | None:
    if configured_code:
        return str(configured_code)
    fallback = _default_benchmark_code()
    if fallback:
        return fallback
    if "index_code" not in panel.columns:
        return None
    codes = panel["index_code"].dropna().astype(str)
    if codes.empty:
        return None
    return str(codes.mode().iloc[0])


def build_benchmark_proxy(panel: pd.DataFrame, experiment: dict) -> pd.DataFrame:
    if panel.empty or "trade_date" not in panel.columns or "pct_chg" not in panel.columns:
        return pd.DataFrame()

    risk_config = experiment.get("risk_filter", {})
    benchmark_code = _resolve_benchmark_code(panel, configured_code=risk_config.get("benchmark_code"))
    use_index_members_only = bool(risk_config.get("use_index_members_only", True))

    working = panel.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"])

    if benchmark_code and "index_code" in working.columns:
        same_index = working["index_code"].fillna("").astype(str) == str(benchmark_code)
        if same_index.any():
            working = working.loc[same_index].copy()

    if use_index_members_only and "is_index_member" in working.columns:
        member_mask = working["is_index_member"].fillna(False)
        if member_mask.any():
            working = working.loc[member_mask].copy()

    working["proxy_return_input"] = pd.to_numeric(working["pct_chg"], errors="coerce") / 100.0
    working = working.loc[np.isfinite(working["proxy_return_input"])].copy()
    if working.empty:
        return pd.DataFrame()

    if "index_weight" in working.columns:
        working["_weight"] = pd.to_numeric(working["index_weight"], errors="coerce").clip(lower=0.0)
    else:
        working["_weight"] = np.nan

    rows: list[dict[str, object]] = []
    for trade_date, group in working.groupby("trade_date", sort=True):
        valid = group.loc[np.isfinite(group["proxy_return_input"])].copy()
        if valid.empty:
            continue

        weights = pd.to_numeric(valid["_weight"], errors="coerce")
        usable_weights = weights.notna() & (weights > 0)
        if usable_weights.any():
            normalized = weights.loc[usable_weights] / weights.loc[usable_weights].sum()
            proxy_return = float((valid.loc[usable_weights, "proxy_return_input"] * normalized).sum())
            weighting_method = "index_weight"
            weighted_member_count = int(usable_weights.sum())
        else:
            proxy_return = float(valid["proxy_return_input"].mean())
            weighting_method = "equal_weight"
            weighted_member_count = 0

        rows.append(
            {
                "trade_date": pd.Timestamp(trade_date),
                "benchmark_code": benchmark_code,
                "benchmark_proxy_return": proxy_return,
                "member_count": int(len(valid)),
                "weighted_member_count": weighted_member_count,
                "weighting_method": weighting_method,
            }
        )

    if not rows:
        return pd.DataFrame()

    proxy = pd.DataFrame(rows).sort_values("trade_date").reset_index(drop=True)
    proxy["benchmark_proxy_close"] = (1.0 + proxy["benchmark_proxy_return"].fillna(0.0)).cumprod() * 100.0
    return proxy


def build_trend_signal(benchmark_proxy: pd.DataFrame, experiment: dict) -> pd.DataFrame:
    if benchmark_proxy.empty:
        return pd.DataFrame()

    risk_config = experiment.get("risk_filter", {})
    if not risk_config.get("enabled", False):
        return pd.DataFrame()

    ma_window = max(2, int(risk_config.get("ma_window", 120) or 120))
    slope_window = max(1, int(risk_config.get("slope_window", 20) or 20))
    require_price_above_ma = bool(risk_config.get("require_price_above_ma", True))
    require_ma_slope_positive = bool(risk_config.get("require_ma_slope_positive", False))

    signal = benchmark_proxy.copy()
    signal["benchmark_ma"] = signal["benchmark_proxy_close"].rolling(
        window=ma_window,
        min_periods=max(10, ma_window // 3),
    ).mean()
    signal["benchmark_ma_slope"] = signal["benchmark_ma"].pct_change(slope_window)

    risk_on = pd.Series(True, index=signal.index, dtype=bool)
    warmup_mask = signal["benchmark_ma"].isna()
    if require_price_above_ma:
        risk_on &= signal["benchmark_proxy_close"] >= signal["benchmark_ma"]
    if require_ma_slope_positive:
        warmup_mask |= signal["benchmark_ma_slope"].isna()
        risk_on &= signal["benchmark_ma_slope"] >= 0.0
    signal["risk_on"] = risk_on.where(~warmup_mask, True).fillna(True).astype(bool)
    signal["risk_state"] = np.where(signal["risk_on"], "trend_on", "trend_off")
    return signal


def latest_trend_state(
    benchmark_proxy: pd.DataFrame,
    experiment: dict,
    as_of_date: pd.Timestamp | None = None,
) -> dict[str, object]:
    signal = build_trend_signal(benchmark_proxy, experiment)
    if signal.empty:
        return {}

    working = signal.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"])
    if as_of_date is not None:
        working = working.loc[working["trade_date"] <= pd.Timestamp(as_of_date)].copy()
        if working.empty:
            return {}

    latest_row = working.sort_values("trade_date").iloc[-1].to_dict()
    trade_date = latest_row.get("trade_date")
    return {
        "trade_date": str(pd.Timestamp(trade_date).date()) if trade_date is not None else None,
        "risk_on": bool(latest_row.get("risk_on", True)) if "risk_on" in latest_row else None,
        "risk_state": latest_row.get("risk_state"),
        "benchmark_proxy_close": latest_row.get("benchmark_proxy_close"),
        "benchmark_ma": latest_row.get("benchmark_ma"),
        "benchmark_ma_slope": latest_row.get("benchmark_ma_slope"),
        "benchmark_code": latest_row.get("benchmark_code"),
    }


def apply_trend_filter(
    portfolio: pd.DataFrame,
    benchmark_proxy: pd.DataFrame,
    experiment: dict,
) -> pd.DataFrame:
    if portfolio.empty:
        return portfolio

    risk_config = experiment.get("risk_filter", {})
    if not risk_config.get("enabled", False):
        return portfolio
    if benchmark_proxy.empty:
        return portfolio

    cash_return = float(risk_config.get("cash_return", 0.0) or 0.0)
    signal = build_trend_signal(benchmark_proxy, experiment)
    if signal.empty:
        return portfolio

    merged = portfolio.merge(
        signal[
            [
                "trade_date",
                "benchmark_code",
                "benchmark_proxy_return",
                "benchmark_proxy_close",
                "benchmark_ma",
                "benchmark_ma_slope",
                "member_count",
                "weighted_member_count",
                "weighting_method",
                "risk_on",
                "risk_state",
            ]
        ],
        on="trade_date",
        how="left",
    ).sort_values("trade_date")

    merged["benchmark_proxy_period_return"] = merged["benchmark_proxy_close"].pct_change().fillna(0.0)
    merged["risk_on"] = merged["risk_on"].fillna(True).astype(bool)
    merged["gross_return_unfiltered"] = merged["gross_return"]
    merged["net_return_unfiltered"] = merged["net_return"]

    risk_off_mask = ~merged["risk_on"]
    merged.loc[risk_off_mask, "gross_return"] = 0.0
    merged.loc[risk_off_mask, "net_return"] = cash_return
    merged["risk_filter_applied"] = risk_off_mask
    merged["equity_curve"] = (1.0 + merged["net_return"].fillna(0.0)).cumprod()
    return merged.reset_index(drop=True)
