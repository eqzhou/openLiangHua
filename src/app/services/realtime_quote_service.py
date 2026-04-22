from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from src.db.realtime_quote_store import RealtimeQuoteSnapshot, get_realtime_quote_store

try:
    import akshare as ak
except Exception:  # pragma: no cover - import safety for environments without akshare
    ak = None


MinuteFetcher = Callable[..., pd.DataFrame]
TickFetcher = Callable[..., pd.DataFrame]
QuoteFetcher = Callable[..., str]
MARKET_TZ = ZoneInfo("Asia/Shanghai")
MARKET_CLOSE_HOUR = 15

SINA_QUOTE_URL = "https://hq.sinajs.cn/list={symbols}"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://finance.sina.com.cn",
}


def _symbol_code(ts_code: str) -> str:
    return str(ts_code or "").split(".")[0]


def _sina_symbol(ts_code: str) -> str:
    code, _, exchange = str(ts_code or "").partition(".")
    market_prefix = {"SZ": "sz", "SH": "sh", "BJ": "bj"}.get(exchange.upper(), exchange.lower() or "sz")
    return f"{market_prefix}{code}"


def _default_sina_quote_fetcher(*, symbols: list[str]) -> str:
    if not symbols:
        return ""
    response = requests.get(
        SINA_QUOTE_URL.format(symbols=",".join(symbols)),
        headers=HTTP_HEADERS,
        timeout=10,
    )
    response.raise_for_status()
    return response.content.decode("gbk", errors="ignore")


def _default_sina_tick_fetcher(*, symbol: str, date: str) -> pd.DataFrame:
    if ak is None:
        raise RuntimeError("akshare is not installed")
    return ak.stock_intraday_sina(symbol=symbol, date=date)


def _default_tick_fetcher(*, symbol: str) -> pd.DataFrame:
    if ak is None:
        raise RuntimeError("akshare is not installed")
    return ak.stock_intraday_em(symbol=symbol)


def _default_minute_fetcher(*, symbol: str, start_date: str, end_date: str, period: str, adjust: str) -> pd.DataFrame:
    if ak is None:
        raise RuntimeError("akshare is not installed")
    return ak.stock_zh_a_hist_min_em(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        period=period,
        adjust=adjust,
    )


def _normalize_tick_frame(frame: pd.DataFrame, *, trade_date: pd.Timestamp) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    required_columns = {"时间", "成交价", "手数"}
    if not required_columns.issubset(frame.columns):
        return pd.DataFrame()

    working = frame.copy().rename(
        columns={
            "时间": "quote_time",
            "成交价": "price",
            "手数": "lots",
            "买卖盘性质": "trade_side",
        }
    )
    working["quote_time"] = pd.to_datetime(
        working["quote_time"].astype(str).map(lambda value: f"{trade_date.date().isoformat()} {value}"),
        errors="coerce",
    )
    for column in ("price", "lots"):
        working[column] = pd.to_numeric(working[column], errors="coerce")

    working = working.loc[working["quote_time"].notna()].copy()
    if working.empty:
        return pd.DataFrame()

    return working.sort_values("quote_time").reset_index(drop=True)


def _normalize_sina_tick_frame(frame: pd.DataFrame, *, trade_date: pd.Timestamp) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    required_columns = {"ticktime", "price", "volume"}
    if not required_columns.issubset(frame.columns):
        return pd.DataFrame()

    working = frame.copy().rename(
        columns={
            "ticktime": "quote_time",
            "kind": "trade_side",
            "prev_price": "prev_price",
        }
    )
    working["quote_time"] = pd.to_datetime(
        working["quote_time"].astype(str).map(lambda value: f"{trade_date.date().isoformat()} {value}"),
        errors="coerce",
    )
    for column in ("price", "volume", "prev_price"):
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")

    working = working.loc[working["quote_time"].notna()].copy()
    if working.empty:
        return pd.DataFrame()

    return working.sort_values("quote_time").reset_index(drop=True)


def _normalize_intraday_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    required_columns = {"时间", "开盘", "收盘", "最高", "最低", "成交量", "成交额"}
    if not required_columns.issubset(frame.columns):
        return pd.DataFrame()

    working = frame.copy().rename(
        columns={
            "时间": "quote_time",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "均价": "avg_price",
        }
    )
    working["quote_time"] = pd.to_datetime(working["quote_time"], errors="coerce")
    for column in ("open", "close", "high", "low", "volume", "amount", "avg_price"):
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")

    working = working.loc[working["quote_time"].notna()].copy()
    if working.empty:
        return pd.DataFrame()

    return working.sort_values("quote_time").reset_index(drop=True)


def _build_change_fields(*, latest_price: float, day_high: float, day_low: float, previous_close: float | None) -> dict[str, Any]:
    realtime_change = None
    realtime_pct_chg = None
    realtime_amplitude = None
    realtime_is_limit_up = None

    if previous_close not in (None, 0) and pd.notna(previous_close):
        previous_close_value = float(previous_close)
        realtime_change = latest_price - previous_close_value
        realtime_pct_chg = latest_price / previous_close_value - 1.0
        realtime_amplitude = (day_high - day_low) / previous_close_value
        realtime_is_limit_up = bool(realtime_pct_chg >= 0.098)

    return {
        "realtime_prev_close": float(previous_close) if previous_close not in (None, 0) and pd.notna(previous_close) else None,
        "realtime_change": realtime_change,
        "realtime_pct_chg": realtime_pct_chg,
        "realtime_amplitude": realtime_amplitude,
        "realtime_is_limit_up": realtime_is_limit_up,
    }


def _build_tick_quote_row(
    *,
    ts_code: str,
    frame: pd.DataFrame,
    previous_close: float | None,
    trade_date: pd.Timestamp,
) -> dict[str, Any]:
    intraday = _normalize_tick_frame(frame, trade_date=trade_date)
    if intraday.empty:
        raise RuntimeError("empty tick frame")

    first_row = intraday.iloc[0]
    last_row = intraday.iloc[-1]
    latest_price = float(last_row["price"])
    day_high = float(intraday["price"].max())
    day_low = float(intraday["price"].min())
    total_volume = float(intraday["lots"].sum() * 100.0)
    total_amount = float((intraday["price"] * intraday["lots"] * 100.0).sum())
    avg_price = (total_amount / total_volume) if total_volume else None

    row = {
        "ts_code": ts_code,
        "realtime_price": latest_price,
        "realtime_open": float(first_row["price"]),
        "realtime_high": day_high,
        "realtime_low": day_low,
        "realtime_volume": total_volume,
        "realtime_amount": total_amount,
        "realtime_avg_price": avg_price,
        "realtime_time": pd.Timestamp(last_row["quote_time"]),
        "realtime_quote_source": "eastmoney-tick",
        "realtime_trade_date": str(trade_date.date()),
    }
    row.update(_build_change_fields(latest_price=latest_price, day_high=day_high, day_low=day_low, previous_close=previous_close))
    return row


def _build_sina_tick_quote_row(
    *,
    ts_code: str,
    frame: pd.DataFrame,
    previous_close: float | None,
    trade_date: pd.Timestamp,
) -> dict[str, Any]:
    intraday = _normalize_sina_tick_frame(frame, trade_date=trade_date)
    if intraday.empty:
        raise RuntimeError("empty sina tick frame")

    first_row = intraday.iloc[0]
    last_row = intraday.iloc[-1]
    latest_price = float(last_row["price"])
    day_high = float(intraday["price"].max())
    day_low = float(intraday["price"].min())
    total_volume = float(intraday["volume"].sum())
    total_amount = float((intraday["price"] * intraday["volume"]).sum())
    avg_price = (total_amount / total_volume) if total_volume else None
    inferred_previous_close = previous_close
    if inferred_previous_close in (None, 0) and "prev_price" in intraday.columns:
        prev_series = pd.to_numeric(intraday["prev_price"], errors="coerce")
        if prev_series.notna().any():
            inferred_previous_close = float(prev_series.dropna().iloc[0])

    row = {
        "ts_code": ts_code,
        "realtime_price": latest_price,
        "realtime_open": float(first_row["price"]),
        "realtime_high": day_high,
        "realtime_low": day_low,
        "realtime_volume": total_volume,
        "realtime_amount": total_amount,
        "realtime_avg_price": avg_price,
        "realtime_time": pd.Timestamp(last_row["quote_time"]),
        "realtime_quote_source": "sina-tick",
        "realtime_trade_date": str(trade_date.date()),
    }
    row.update(
        _build_change_fields(
            latest_price=latest_price,
            day_high=day_high,
            day_low=day_low,
            previous_close=inferred_previous_close,
        )
    )
    return row


def _build_minute_quote_row(
    *,
    ts_code: str,
    frame: pd.DataFrame,
    previous_close: float | None,
    trade_date: pd.Timestamp,
) -> dict[str, Any]:
    intraday = _normalize_intraday_frame(frame)
    if intraday.empty:
        raise RuntimeError("empty intraday frame")

    first_row = intraday.iloc[0]
    last_row = intraday.iloc[-1]
    latest_price = float(last_row["close"])
    day_high = float(intraday["high"].max())
    day_low = float(intraday["low"].min())
    total_volume = float(intraday["volume"].sum())
    total_amount = float(intraday["amount"].sum())

    row = {
        "ts_code": ts_code,
        "realtime_price": latest_price,
        "realtime_open": float(first_row["open"]),
        "realtime_high": day_high,
        "realtime_low": day_low,
        "realtime_volume": total_volume,
        "realtime_amount": total_amount,
        "realtime_avg_price": (
            float(last_row["avg_price"]) if "avg_price" in last_row.index and pd.notna(last_row["avg_price"]) else None
        ),
        "realtime_time": pd.Timestamp(last_row["quote_time"]),
        "realtime_quote_source": "eastmoney-minute",
        "realtime_trade_date": str(trade_date.date()),
    }
    row.update(_build_change_fields(latest_price=latest_price, day_high=day_high, day_low=day_low, previous_close=previous_close))
    return row


def _parse_sina_quote_payload(payload: str) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {}
    pattern = re.compile(r'var hq_str_(?P<symbol>[a-z]{2}\d+)="(?P<body>.*)";')
    for raw_line in (payload or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = pattern.match(line)
        if not match:
            continue
        rows[match.group("symbol")] = match.group("body").split(",")
    return rows


def _build_sina_quote_row(
    *,
    ts_code: str,
    fields: list[str],
    previous_close: float | None,
    trade_date: pd.Timestamp,
) -> dict[str, Any]:
    if len(fields) < 10:
        raise RuntimeError("invalid sina quote payload")

    open_price = pd.to_numeric(fields[1], errors="coerce")
    quote_prev_close = pd.to_numeric(fields[2], errors="coerce")
    latest_price = pd.to_numeric(fields[3], errors="coerce")
    day_high = pd.to_numeric(fields[4], errors="coerce")
    day_low = pd.to_numeric(fields[5], errors="coerce")
    volume = pd.to_numeric(fields[8], errors="coerce")
    amount = pd.to_numeric(fields[9], errors="coerce")

    if pd.isna(latest_price) or pd.isna(open_price) or pd.isna(day_high) or pd.isna(day_low):
        raise RuntimeError("missing core quote fields")

    if previous_close in (None, 0) and pd.notna(quote_prev_close):
        previous_close = float(quote_prev_close)

    quote_date = str(fields[30]).strip() if len(fields) > 30 else str(trade_date.date())
    quote_time = str(fields[31]).strip() if len(fields) > 31 else ""
    quote_timestamp = pd.to_datetime(f"{quote_date} {quote_time}".strip(), errors="coerce")
    if pd.isna(quote_timestamp):
        quote_timestamp = pd.Timestamp(trade_date)

    total_volume = float(volume) if pd.notna(volume) else None
    total_amount = float(amount) if pd.notna(amount) else None
    avg_price = (total_amount / total_volume) if total_volume not in (None, 0) and total_amount is not None else None

    row = {
        "ts_code": ts_code,
        "realtime_price": float(latest_price),
        "realtime_open": float(open_price),
        "realtime_high": float(day_high),
        "realtime_low": float(day_low),
        "realtime_volume": total_volume,
        "realtime_amount": total_amount,
        "realtime_avg_price": avg_price,
        "realtime_time": pd.Timestamp(quote_timestamp),
        "realtime_quote_source": "sina-quote",
        "realtime_trade_date": quote_date,
    }
    row.update(
        _build_change_fields(
            latest_price=float(latest_price),
            day_high=float(day_high),
            day_low=float(day_low),
            previous_close=previous_close,
        )
    )
    return row


def _normalize_market_timestamp(value: pd.Timestamp | str | None = None) -> pd.Timestamp:
    timestamp = pd.Timestamp(value or pd.Timestamp.now(tz=MARKET_TZ))
    if timestamp.tzinfo is None:
        return timestamp.tz_localize(MARKET_TZ)
    return timestamp.tz_convert(MARKET_TZ)


def _snapshot_bucket_for_timestamp(now: pd.Timestamp) -> str:
    return "post_close" if (now.hour, now.minute, now.second) >= (MARKET_CLOSE_HOUR, 0, 0) else "latest"


def _cached_status(snapshot: RealtimeQuoteSnapshot, *, served_from: str) -> dict[str, Any]:
    status = dict(snapshot.status)
    status["available"] = not snapshot.quotes.empty
    status["snapshot_bucket"] = snapshot.snapshot_bucket
    status["served_from"] = served_from
    return status


def fetch_managed_realtime_quotes(
    symbols: list[str],
    *,
    previous_close_lookup: dict[str, float] | None = None,
    trade_date: pd.Timestamp | None = None,
    now: pd.Timestamp | None = None,
    quote_fetcher: QuoteFetcher | None = None,
    fetcher: MinuteFetcher | None = None,
    tick_fetcher: TickFetcher | None = None,
    minute_fetcher: MinuteFetcher | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    resolved_now = _normalize_market_timestamp(now or trade_date)
    resolved_trade_date = pd.Timestamp(trade_date or resolved_now).date()
    snapshot_bucket = _snapshot_bucket_for_timestamp(resolved_now)
    store = get_realtime_quote_store()

    cached_post_close = None
    if snapshot_bucket == "post_close":
        cached_post_close = store.get_snapshot(trade_date=resolved_trade_date, snapshot_bucket="post_close")
        if cached_post_close is not None and not cached_post_close.quotes.empty:
            return cached_post_close.quotes.copy(), _cached_status(cached_post_close, served_from="database")

    realtime_quotes, realtime_status = fetch_realtime_quotes(
        symbols,
        previous_close_lookup=previous_close_lookup,
        trade_date=pd.Timestamp(resolved_trade_date),
        quote_fetcher=quote_fetcher,
        fetcher=fetcher,
        tick_fetcher=tick_fetcher,
        minute_fetcher=minute_fetcher,
    )

    realtime_status = dict(realtime_status)
    realtime_status["snapshot_bucket"] = snapshot_bucket
    realtime_status["served_from"] = "provider"

    if not realtime_quotes.empty:
        store.upsert_snapshot(
            trade_date=resolved_trade_date,
            snapshot_bucket=snapshot_bucket,
            quotes=realtime_quotes,
            status=realtime_status,
        )
        if snapshot_bucket == "post_close":
            store.upsert_snapshot(
                trade_date=resolved_trade_date,
                snapshot_bucket="latest",
                quotes=realtime_quotes,
                status={**realtime_status, "snapshot_bucket": "latest"},
            )
        return realtime_quotes, realtime_status

    fallback_buckets = ["post_close", "latest"] if snapshot_bucket == "post_close" else ["latest"]
    for bucket in fallback_buckets:
        cached_snapshot = store.get_snapshot(trade_date=resolved_trade_date, snapshot_bucket=bucket)
        if cached_snapshot is not None and not cached_snapshot.quotes.empty:
            fallback_status = _cached_status(cached_snapshot, served_from="database-fallback")
            fallback_status["error_message"] = realtime_status.get("error_message", "")
            return cached_snapshot.quotes.copy(), fallback_status

    return realtime_quotes, realtime_status


def fetch_realtime_quotes(
    symbols: list[str],
    *,
    previous_close_lookup: dict[str, float] | None = None,
    trade_date: pd.Timestamp | None = None,
    quote_fetcher: QuoteFetcher | None = None,
    fetcher: MinuteFetcher | None = None,
    tick_fetcher: TickFetcher | None = None,
    minute_fetcher: MinuteFetcher | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    unique_symbols = list(dict.fromkeys(str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()))
    resolved_trade_date = pd.Timestamp(trade_date or pd.Timestamp.now().date())
    previous_closes = dict(previous_close_lookup or {})
    resolved_quote_fetcher = quote_fetcher or _default_sina_quote_fetcher
    resolved_tick_fetcher = tick_fetcher
    resolved_minute_fetcher = minute_fetcher
    if fetcher is not None and resolved_minute_fetcher is None:
        resolved_minute_fetcher = fetcher
    if resolved_tick_fetcher is None and fetcher is None:
        resolved_tick_fetcher = _default_tick_fetcher
    if resolved_minute_fetcher is None:
        resolved_minute_fetcher = _default_minute_fetcher
    use_default_provider_stack = fetcher is None and tick_fetcher is None and minute_fetcher is None and quote_fetcher is None

    if not unique_symbols:
        return pd.DataFrame(), {
            "available": False,
            "source": "sina-quote",
            "trade_date": str(resolved_trade_date.date()),
            "fetched_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "requested_symbol_count": 0,
            "success_symbol_count": 0,
            "failed_symbols": [],
            "error_message": "",
        }

    start_date = f"{resolved_trade_date.date().isoformat()} 09:30:00"
    end_date = f"{resolved_trade_date.date().isoformat()} 15:00:00"
    rows: list[dict[str, Any]] = []
    failed_symbols: list[str] = []
    errors: list[str] = []

    quote_payload_errors: list[str] = []
    quote_rows: dict[str, list[str]] = {}
    sina_symbol_lookup = {ts_code: _sina_symbol(ts_code) for ts_code in unique_symbols}
    if resolved_quote_fetcher is not None:
        try:
            payload = resolved_quote_fetcher(symbols=list(sina_symbol_lookup.values()))
            quote_rows = _parse_sina_quote_payload(payload)
        except Exception as exc:
            quote_payload_errors.append(f"sina-quote {type(exc).__name__}: {exc}")

    for ts_code in unique_symbols:
        symbol_code = _symbol_code(ts_code)
        row: dict[str, Any] | None = None
        provider_errors: list[str] = list(quote_payload_errors)

        try:
            previous_close = previous_closes.get(ts_code)
            quote_fields = quote_rows.get(sina_symbol_lookup[ts_code])
            if quote_fields:
                try:
                    row = _build_sina_quote_row(
                        ts_code=ts_code,
                        fields=quote_fields,
                        previous_close=previous_close,
                        trade_date=resolved_trade_date,
                    )
                except Exception as exc:
                    provider_errors.append(f"sina-quote-row {type(exc).__name__}: {exc}")

            if row is None and use_default_provider_stack:
                try:
                    sina_frame = _default_sina_tick_fetcher(
                        symbol=_sina_symbol(ts_code),
                        date=resolved_trade_date.strftime("%Y%m%d"),
                    )
                    row = _build_sina_tick_quote_row(
                        ts_code=ts_code,
                        frame=sina_frame,
                        previous_close=previous_close,
                        trade_date=resolved_trade_date,
                    )
                except Exception as exc:
                    provider_errors.append(f"sina {type(exc).__name__}: {exc}")

            if row is None and resolved_tick_fetcher is not None:
                try:
                    tick_frame = resolved_tick_fetcher(symbol=symbol_code)
                    row = _build_tick_quote_row(
                        ts_code=ts_code,
                        frame=tick_frame,
                        previous_close=previous_close,
                        trade_date=resolved_trade_date,
                    )
                except Exception as exc:
                    provider_errors.append(f"tick {type(exc).__name__}: {exc}")

            if row is None and resolved_minute_fetcher is not None:
                try:
                    minute_frame = resolved_minute_fetcher(
                        symbol=symbol_code,
                        start_date=start_date,
                        end_date=end_date,
                        period="1",
                        adjust="",
                    )
                    row = _build_minute_quote_row(
                        ts_code=ts_code,
                        frame=minute_frame,
                        previous_close=previous_close,
                        trade_date=resolved_trade_date,
                    )
                except Exception as exc:
                    provider_errors.append(f"minute {type(exc).__name__}: {exc}")

            if row is None:
                raise RuntimeError(" | ".join(provider_errors) or "all realtime providers failed")

            rows.append(row)
        except Exception as exc:
            failed_symbols.append(ts_code)
            errors.append(f"{ts_code}: {type(exc).__name__}: {exc}")

    realtime_quotes = pd.DataFrame(rows)
    if not realtime_quotes.empty:
        realtime_quotes = realtime_quotes.sort_values("ts_code").reset_index(drop=True)
        source_values = sorted(set(realtime_quotes["realtime_quote_source"].dropna().astype(str).tolist()))
        status_source = source_values[0] if len(source_values) == 1 else "mixed"
    else:
        status_source = "sina-quote"

    status = {
        "available": not realtime_quotes.empty,
        "source": status_source,
        "trade_date": str(resolved_trade_date.date()),
        "fetched_at": pd.Timestamp.now().isoformat(timespec="seconds"),
        "requested_symbol_count": len(unique_symbols),
        "success_symbol_count": int(len(realtime_quotes)),
        "failed_symbols": failed_symbols,
        "error_message": " | ".join(errors[:3]),
    }
    return realtime_quotes, status


def merge_realtime_quotes(watchlist_view: pd.DataFrame, realtime_quotes: pd.DataFrame) -> pd.DataFrame:
    if watchlist_view.empty:
        return watchlist_view.copy()

    if realtime_quotes.empty:
        return watchlist_view.copy()

    merged = watchlist_view.merge(realtime_quotes, on="ts_code", how="left")

    def _numeric_series(column_name: str) -> pd.Series:
        if column_name in merged.columns:
            return pd.to_numeric(merged[column_name], errors="coerce")
        return pd.Series(float("nan"), index=merged.index, dtype="float64")

    realtime_price = _numeric_series("realtime_price")
    has_realtime_price = realtime_price.notna()
    cost_basis = _numeric_series("cost_basis")
    shares = _numeric_series("shares")
    mark_price = _numeric_series("mark_price")

    merged["realtime_market_value"] = pd.NA
    merged.loc[has_realtime_price, "realtime_market_value"] = realtime_price.loc[has_realtime_price] * shares.loc[has_realtime_price]

    merged["realtime_unrealized_pnl"] = pd.NA
    valid_pnl = has_realtime_price & cost_basis.notna() & shares.notna()
    merged.loc[valid_pnl, "realtime_unrealized_pnl"] = (
        (realtime_price.loc[valid_pnl] - cost_basis.loc[valid_pnl]) * shares.loc[valid_pnl]
    )

    merged["realtime_unrealized_pnl_pct"] = pd.NA
    valid_pnl_pct = has_realtime_price & cost_basis.notna() & (cost_basis != 0)
    merged.loc[valid_pnl_pct, "realtime_unrealized_pnl_pct"] = (
        realtime_price.loc[valid_pnl_pct] / cost_basis.loc[valid_pnl_pct] - 1.0
    )

    merged["realtime_vs_mark_pct"] = pd.NA
    valid_mark = has_realtime_price & mark_price.notna() & (mark_price != 0)
    merged.loc[valid_mark, "realtime_vs_mark_pct"] = (
        realtime_price.loc[valid_mark] / mark_price.loc[valid_mark] - 1.0
    )

    return merged


def merge_realtime_quote_record(record: dict[str, Any], realtime_quote: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(record)
    if not realtime_quote:
        return merged

    merged.update(realtime_quote)

    realtime_price = pd.to_numeric(realtime_quote.get("realtime_price"), errors="coerce")
    if pd.isna(realtime_price):
        merged.setdefault("realtime_market_value", pd.NA)
        merged.setdefault("realtime_unrealized_pnl", pd.NA)
        merged.setdefault("realtime_unrealized_pnl_pct", pd.NA)
        merged.setdefault("realtime_vs_mark_pct", pd.NA)
        return merged

    cost_basis = pd.to_numeric(record.get("cost_basis"), errors="coerce")
    shares = pd.to_numeric(record.get("shares"), errors="coerce")
    mark_price = pd.to_numeric(record.get("mark_price"), errors="coerce")

    merged["realtime_market_value"] = realtime_price * shares if pd.notna(shares) else pd.NA
    merged["realtime_unrealized_pnl"] = (realtime_price - cost_basis) * shares if pd.notna(cost_basis) and pd.notna(shares) else pd.NA
    merged["realtime_unrealized_pnl_pct"] = realtime_price / cost_basis - 1.0 if pd.notna(cost_basis) and cost_basis != 0 else pd.NA
    merged["realtime_vs_mark_pct"] = realtime_price / mark_price - 1.0 if pd.notna(mark_price) and mark_price != 0 else pd.NA
    return merged


def merge_realtime_quote_records(records: list[dict[str, Any]], realtime_quotes: pd.DataFrame) -> list[dict[str, Any]]:
    if not records or realtime_quotes.empty or "ts_code" not in realtime_quotes.columns:
        return [dict(record) for record in records]

    quote_lookup = {
        str(record.get("ts_code", "") or ""): dict(record)
        for record in realtime_quotes.to_dict(orient="records")
        if str(record.get("ts_code", "") or "")
    }
    return [
        merge_realtime_quote_record(record, quote_lookup.get(str(record.get("ts_code", "") or "")))
        for record in records
    ]
