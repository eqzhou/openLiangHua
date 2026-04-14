from __future__ import annotations

import pandas as pd


def records_to_frame(
    records: list[dict] | None,
    *,
    index_col: str | None = None,
    sort_by: str | None = None,
) -> pd.DataFrame:
    frame = pd.DataFrame(records or [])
    if frame.empty:
        return frame
    if index_col and index_col in frame.columns:
        frame[index_col] = pd.to_datetime(frame[index_col], errors="coerce")
        frame = frame.loc[frame[index_col].notna()].copy()
        if frame.empty:
            return frame
        frame = frame.set_index(index_col)
    if sort_by and sort_by in frame.columns:
        frame = frame.sort_values(sort_by)
    elif index_col:
        frame = frame.sort_index()
    return frame
