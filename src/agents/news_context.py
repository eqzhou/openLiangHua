from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import akshare as ak
import pandas as pd

from src.utils.io import ensure_dir


def _disable_proxy() -> None:
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        os.environ[key] = ""
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"


def _symbol_code(ts_code: str) -> str:
    return str(ts_code).split(".")[0]


def _notice_cache_path(cache_dir: Path, notice_date: pd.Timestamp) -> Path:
    return cache_dir / f"notice_{notice_date.strftime('%Y%m%d')}.parquet"


def _research_cache_path(cache_dir: Path, symbol_code: str) -> Path:
    return cache_dir / f"research_{symbol_code}.parquet"


def _fetch_notice_day(cache_dir: Path, notice_date: pd.Timestamp) -> pd.DataFrame:
    cache_path = _notice_cache_path(cache_dir, notice_date)
    if cache_path.exists():
        return pd.read_parquet(cache_path)

    _disable_proxy()
    frame = ak.stock_notice_report(symbol="全部", date=notice_date.strftime("%Y%m%d")).copy()
    if not frame.empty:
        frame["公告日期"] = pd.to_datetime(frame["公告日期"], errors="coerce")
    frame.to_parquet(cache_path, index=False)
    return frame


def _fetch_research_reports(cache_dir: Path, ts_code: str) -> pd.DataFrame:
    symbol_code = _symbol_code(ts_code)
    cache_path = _research_cache_path(cache_dir, symbol_code)
    if cache_path.exists():
        return pd.read_parquet(cache_path)

    _disable_proxy()
    frame = ak.stock_research_report_em(symbol=symbol_code).copy()
    if not frame.empty and "日期" in frame.columns:
        frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    frame.to_parquet(cache_path, index=False)
    return frame


def _classify_notice_signal(titles: list[str]) -> str:
    joined = " ".join(titles)
    risk_keywords = ("风险", "异常波动", "减持", "问询", "诉讼", "违规", "退市", "质押", "被冻结")
    positive_keywords = ("业绩", "回购", "增持", "中标", "签约", "分红", "激励", "预增", "合同")
    if any(keyword in joined for keyword in risk_keywords):
        return "偏谨慎"
    if any(keyword in joined for keyword in positive_keywords):
        return "偏积极"
    return "中性"


def _classify_research_signal(ratings: list[str], titles: list[str]) -> str:
    joined = " ".join(ratings + titles)
    positive_keywords = ("买入", "增持", "强烈推荐", "推荐", "上调", "超预期", "景气")
    negative_keywords = ("卖出", "减持", "回避", "下调", "承压", "不及预期", "风险")
    if any(keyword in joined for keyword in negative_keywords):
        return "偏谨慎"
    if any(keyword in joined for keyword in positive_keywords):
        return "偏积极"
    return "中性"


def _summarize_notices(frame: pd.DataFrame, ts_code: str, lookback_days: int, max_items: int) -> dict[str, object]:
    code = _symbol_code(ts_code)
    scoped = frame.loc[frame["代码"].astype(str) == code].copy()
    scoped = scoped.sort_values("公告日期", ascending=False).head(max_items)
    if scoped.empty:
        return {
            "notice_count": 0,
            "notice_titles": "",
            "notice_digest": f"近{lookback_days}天未检索到公告。",
            "notice_signal": "中性",
        }

    titles = scoped["公告标题"].astype(str).tolist()
    notice_types = scoped["公告类型"].astype(str).tolist()
    digest = f"近{lookback_days}天公告{len(scoped)}条：" + "；".join(
        f"{title}（{notice_type}）" for title, notice_type in zip(titles, notice_types, strict=False)
    )
    return {
        "notice_count": int(len(scoped)),
        "notice_titles": "；".join(titles),
        "notice_digest": digest,
        "notice_signal": _classify_notice_signal(titles),
    }


def _summarize_news(ts_code: str, as_of_date: pd.Timestamp, lookback_days: int, max_items: int) -> dict[str, object]:
    _disable_proxy()
    frame = ak.stock_news_em(symbol=_symbol_code(ts_code)).copy()
    if frame.empty:
        return {
            "news_count": 0,
            "news_titles": "",
            "news_digest": "未检索到新闻。",
            "news_status": "none",
            "news_source": "无可用新闻",
        }

    publish_col = frame.columns[3]
    title_col = frame.columns[1]
    source_col = frame.columns[4]
    try:
        frame[publish_col] = pd.to_datetime(frame[publish_col], format="mixed", errors="coerce")
    except TypeError:
        frame[publish_col] = pd.to_datetime(frame[publish_col], errors="coerce")
    start_date = pd.Timestamp(as_of_date) - timedelta(days=max(1, int(lookback_days)))
    scoped = frame.loc[
        frame[publish_col].notna()
        & (frame[publish_col] <= pd.Timestamp(as_of_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
        & (frame[publish_col] >= start_date)
    ].copy()
    scoped = scoped.sort_values(publish_col, ascending=False).head(max_items)

    if scoped.empty:
        return {
            "news_count": 0,
            "news_titles": "",
            "news_digest": "当前接口未返回该时点附近的历史新闻。",
            "news_status": "historical_unavailable",
            "news_source": "新闻缺口",
        }

    items = [
        f"{row[title_col]}（{row[source_col]}，{pd.Timestamp(row[publish_col]).strftime('%Y-%m-%d')}）"
        for _, row in scoped.iterrows()
    ]
    return {
        "news_count": int(len(scoped)),
        "news_titles": "；".join(scoped[title_col].astype(str).tolist()),
        "news_digest": f"近{lookback_days}天新闻{len(scoped)}条：" + "；".join(items),
        "news_status": "historical_available",
        "news_source": "历史新闻",
    }


def _summarize_research_reports(
    cache_dir: Path,
    ts_code: str,
    as_of_date: pd.Timestamp,
    lookback_days: int,
    max_items: int,
) -> dict[str, object]:
    frame = _fetch_research_reports(cache_dir, ts_code)
    if frame.empty or "日期" not in frame.columns:
        return {
            "research_count": 0,
            "research_titles": "",
            "research_digest": f"近{lookback_days}天未检索到研报。",
            "research_signal": "中性",
            "research_status": "none",
        }

    start_date = pd.Timestamp(as_of_date) - timedelta(days=max(1, int(lookback_days)))
    scoped = frame.loc[
        frame["日期"].notna()
        & (frame["日期"] <= pd.Timestamp(as_of_date))
        & (frame["日期"] >= start_date)
    ].copy()
    scoped = scoped.sort_values("日期", ascending=False).head(max_items)
    if scoped.empty:
        return {
            "research_count": 0,
            "research_titles": "",
            "research_digest": f"近{lookback_days}天未检索到研报。",
            "research_signal": "中性",
            "research_status": "historical_unavailable",
        }

    titles = scoped["报告名称"].astype(str).tolist()
    ratings = scoped["东财评级"].fillna("未评级").astype(str).tolist()
    institutions = scoped["机构"].fillna("未知机构").astype(str).tolist()
    dates = pd.to_datetime(scoped["日期"]).dt.strftime("%Y-%m-%d").tolist()
    items = [
        f"{title}（{institution}，{rating}，{date}）"
        for title, institution, rating, date in zip(titles, institutions, ratings, dates, strict=False)
    ]
    return {
        "research_count": int(len(scoped)),
        "research_titles": "；".join(titles),
        "research_digest": f"近{lookback_days}天研报{len(scoped)}篇：" + "；".join(items),
        "research_signal": _classify_research_signal(ratings, titles),
        "research_status": "historical_available",
    }


def _merge_news_and_research(news_summary: dict[str, object], research_summary: dict[str, object]) -> dict[str, object]:
    merged = dict(news_summary)
    merged.update(research_summary)

    if news_summary.get("news_status") == "historical_available":
        if research_summary.get("research_status") == "historical_available":
            merged["news_digest"] = (
                f"{news_summary.get('news_digest', '')} 研报补充：{research_summary.get('research_digest', '')}"
            ).strip()
            merged["news_source"] = "历史新闻 + 研报补充"
        return merged

    if research_summary.get("research_status") == "historical_available":
        merged["news_count"] = int(research_summary.get("research_count", 0) or 0)
        merged["news_titles"] = str(research_summary.get("research_titles", "") or "")
        merged["news_digest"] = str(research_summary.get("research_digest", "") or "未检索到可用资讯。")
        merged["news_status"] = "research_fallback"
        merged["news_source"] = "研报回退"
        return merged

    merged["news_source"] = "无可用资讯"
    return merged


def build_event_context(
    ts_codes: list[str],
    as_of_date: pd.Timestamp,
    cache_dir: Path,
    notice_lookback_days: int = 7,
    notice_max_items: int = 3,
    news_lookback_days: int = 7,
    news_max_items: int = 3,
    research_lookback_days: int = 30,
    research_max_items: int = 3,
) -> pd.DataFrame:
    if not ts_codes:
        return pd.DataFrame()

    ensure_dir(cache_dir)
    as_of_date = pd.Timestamp(as_of_date).normalize()

    notice_frames: list[pd.DataFrame] = []
    for offset in range(max(1, int(notice_lookback_days))):
        notice_date = as_of_date - timedelta(days=offset)
        try:
            notice_frames.append(_fetch_notice_day(cache_dir, notice_date))
        except Exception:
            continue
    notice_frame = pd.concat(notice_frames, ignore_index=True) if notice_frames else pd.DataFrame()
    if not notice_frame.empty and "公告日期" in notice_frame.columns:
        notice_frame["公告日期"] = pd.to_datetime(notice_frame["公告日期"], errors="coerce")

    rows: list[dict[str, object]] = []
    for ts_code in ts_codes:
        notice_summary = _summarize_notices(
            frame=notice_frame,
            ts_code=ts_code,
            lookback_days=notice_lookback_days,
            max_items=notice_max_items,
        )
        try:
            news_summary = _summarize_news(
                ts_code=ts_code,
                as_of_date=as_of_date,
                lookback_days=news_lookback_days,
                max_items=news_max_items,
            )
        except Exception:
            news_summary = {
                "news_count": 0,
                "news_titles": "",
                "news_digest": "新闻接口拉取失败。",
                "news_status": "error",
                "news_source": "接口异常",
            }

        try:
            research_summary = _summarize_research_reports(
                cache_dir=cache_dir,
                ts_code=ts_code,
                as_of_date=as_of_date,
                lookback_days=research_lookback_days,
                max_items=research_max_items,
            )
        except Exception:
            research_summary = {
                "research_count": 0,
                "research_titles": "",
                "research_digest": "研报接口拉取失败。",
                "research_signal": "中性",
                "research_status": "error",
            }

        rows.append(
            {
                "ts_code": ts_code,
                **notice_summary,
                **_merge_news_and_research(news_summary, research_summary),
            }
        )

    return pd.DataFrame(rows)
