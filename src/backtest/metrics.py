from __future__ import annotations

import numpy as np
import pandas as pd


def annualized_return(returns: pd.Series, periods_per_year: int = 252) -> float:
    clean = returns.dropna()
    if clean.empty:
        return float("nan")
    compounded = (1.0 + clean).prod()
    years = len(clean) / periods_per_year
    if years == 0:
        return float("nan")
    return compounded ** (1.0 / years) - 1.0


def sharpe_ratio(returns: pd.Series, periods_per_year: int = 252) -> float:
    clean = returns.dropna()
    if clean.empty or clean.std() == 0:
        return float("nan")
    return np.sqrt(periods_per_year) * clean.mean() / clean.std()


def max_drawdown(returns: pd.Series) -> float:
    clean = returns.fillna(0.0)
    equity = (1.0 + clean).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    return float(drawdown.min())
