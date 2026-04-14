from __future__ import annotations

import pandas as pd


def fetch_index_membership_history(client, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    for month in pd.period_range(start=start, end=end, freq="M"):
        window_start = month.start_time.strftime("%Y%m%d")
        window_end = month.end_time.strftime("%Y%m%d")
        chunk = client.index_weight(index_code=index_code, start_date=window_start, end_date=window_end)
        if chunk is not None and not chunk.empty:
            frames.append(chunk)

    if not frames:
        return pd.DataFrame(columns=["index_code", "con_code", "trade_date", "weight"])

    membership = pd.concat(frames, ignore_index=True).drop_duplicates()
    membership["trade_date"] = pd.to_datetime(membership["trade_date"], format="%Y%m%d")
    return membership.sort_values(["trade_date", "con_code"]).reset_index(drop=True)


def expand_index_membership(
    membership_history: pd.DataFrame,
    open_dates: pd.Series,
) -> pd.DataFrame:
    if membership_history.empty:
        return pd.DataFrame(columns=["trade_date", "ts_code", "index_code", "index_weight", "is_index_member"])

    snapshots = membership_history["trade_date"].drop_duplicates().sort_values().tolist()
    frames: list[pd.DataFrame] = []

    for idx, snapshot_date in enumerate(snapshots):
        next_snapshot = snapshots[idx + 1] if idx + 1 < len(snapshots) else None
        members = membership_history.loc[
            membership_history["trade_date"] == snapshot_date,
            ["index_code", "con_code", "weight"],
        ].copy()
        if members.empty:
            continue

        valid_dates = open_dates.loc[open_dates >= snapshot_date]
        if next_snapshot is not None:
            valid_dates = valid_dates.loc[valid_dates < next_snapshot]
        if valid_dates.empty:
            continue

        expanded = members.merge(valid_dates.to_frame(name="trade_date"), how="cross")
        expanded = expanded.rename(columns={"con_code": "ts_code", "weight": "index_weight"})
        expanded["is_index_member"] = True
        frames.append(expanded)

    if not frames:
        return pd.DataFrame(columns=["trade_date", "ts_code", "index_code", "index_weight", "is_index_member"])

    daily_membership = pd.concat(frames, ignore_index=True)
    return daily_membership.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
