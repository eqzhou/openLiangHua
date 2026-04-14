from __future__ import annotations

import pandas as pd


def build_latest_prediction_snapshot(predictions: pd.DataFrame) -> pd.DataFrame:
    if predictions.empty or "trade_date" not in predictions.columns or "ts_code" not in predictions.columns:
        return pd.DataFrame()

    working = predictions.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    working = working.loc[working["trade_date"].notna()].copy()
    if working.empty or "score" not in working.columns:
        return pd.DataFrame()

    latest_date = pd.Timestamp(working["trade_date"].max())
    snapshot = working.loc[working["trade_date"] == latest_date].copy()
    if snapshot.empty:
        return pd.DataFrame()

    snapshot = snapshot.sort_values("score", ascending=False).reset_index(drop=True)
    snapshot["rank"] = snapshot.index + 1
    snapshot["rank_pct"] = pd.to_numeric(snapshot["score"], errors="coerce").rank(pct=True, ascending=True)
    return snapshot


def latest_prediction_details(predictions: pd.DataFrame, symbol: str) -> dict[str, object]:
    snapshot = build_latest_prediction_snapshot(predictions)
    if snapshot.empty:
        return {}

    row = snapshot.loc[snapshot["ts_code"].astype(str) == str(symbol)].head(1)
    if row.empty:
        return {}

    record = row.iloc[0].to_dict()
    record["signal_date"] = snapshot["trade_date"].iloc[0]
    record["universe_size"] = int(len(snapshot))
    return record
