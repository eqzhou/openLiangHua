from __future__ import annotations

import pandas as pd

from src.utils.prediction_snapshot import build_latest_prediction_snapshot


def build_top_candidates_snapshot(predictions: pd.DataFrame, *, top_n: int) -> pd.DataFrame:
    snapshot = build_latest_prediction_snapshot(predictions)
    if snapshot.empty:
        return pd.DataFrame()
    return snapshot.head(top_n).copy()


def build_candidate_score_history(predictions: pd.DataFrame, *, symbol: str, tail_n: int = 240) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()

    history = predictions.loc[predictions["ts_code"] == symbol, ["trade_date", "score", "ret_t1_t10"]].copy()
    if history.empty:
        return pd.DataFrame()

    history["trade_date"] = pd.to_datetime(history["trade_date"], errors="coerce")
    history = history.loc[history["trade_date"].notna()].sort_values("trade_date").tail(tail_n)
    if history.empty:
        return pd.DataFrame()

    return history.set_index("trade_date").rename(
        columns={
            "score": "综合评分",
            "ret_t1_t10": "未来10日收益(T+1建仓)",
        }
    )
