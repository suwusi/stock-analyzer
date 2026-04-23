#!/usr/bin/env python3
"""Fetch and cache forward-looking risk signals for an A-share stock."""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from typing import Any

from common import RISK_CACHE_DIR, parse_date, normalize_symbol, print_json
from market_data import (
    get_financial_report_notices,
    get_recent_stock_news,
    get_reduction_announcements,
    get_report_disclosures,
)


NEGATIVE_NEWS_KEYWORDS = (
    "不及预期",
    "下滑",
    "亏损",
    "减值",
    "问询",
    "处罚",
    "立案",
    "诉讼",
    "风险",
    "终止",
    "暴跌",
    "大跌",
    "违约",
    "减持",
    "爆雷",
)


def _risk_cache_path(symbol: str):
    RISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return RISK_CACHE_DIR / f"{normalize_symbol(symbol)}.json"


def _load_cached_report(symbol: str) -> dict[str, Any] | None:
    path = _risk_cache_path(symbol)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if payload.get("fetched_on") != date.today().isoformat():
        return None
    return payload


def _save_cached_report(symbol: str, payload: dict[str, Any]) -> None:
    path = _risk_cache_path(symbol)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _filter_recent(items: list[dict[str, Any]], key: str, center: date, window_days: int) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in items:
        event_date = parse_date(str(item.get(key, "")))
        if not event_date:
            continue
        if 0 <= (center - event_date).days <= window_days:
            filtered.append(item)
    return filtered


def _filter_upcoming_report_events(items: list[dict[str, Any]], center: date, window_days: int) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in items:
        raw_value = item.get("实际披露") or item.get("首次预约")
        event_date = parse_date(str(raw_value or ""))
        if not event_date:
            continue
        if 0 <= (event_date - center).days <= window_days:
            filtered.append(item)
    return filtered


def _negative_news_within_window(symbol: str, lookback_days: int) -> list[dict[str, Any]]:
    center = date.today()
    matches: list[dict[str, Any]] = []
    for item in get_recent_stock_news(symbol, limit=30):
        combined = f"{item.get('新闻标题', '')} {item.get('新闻内容', '')}"
        matched_keywords = [keyword for keyword in NEGATIVE_NEWS_KEYWORDS if keyword in combined]
        if not matched_keywords:
            continue
        publish_date = parse_date(str(item.get("发布时间", "")))
        if publish_date and 0 <= (center - publish_date).days <= lookback_days:
            enriched = dict(item)
            enriched["matched_keywords"] = matched_keywords
            matches.append(enriched)
    return matches


def build_risk_event_report(
    symbol: str,
    window_days: int = 14,
    lookback_days: int = 30,
    refresh: bool = False,
) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not refresh:
        cached = _load_cached_report(normalized)
        if cached is not None:
            return cached

    today = date.today()
    start_date = (today - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end_date = (today + timedelta(days=window_days)).strftime("%Y%m%d")

    missing_data: list[str] = []
    report_events: list[dict[str, Any]] = []
    report_notices: list[dict[str, Any]] = []
    reduction_events: list[dict[str, Any]] = []
    negative_news: list[dict[str, Any]] = []

    try:
        report_events = get_report_disclosures(normalized)
    except Exception as exc:
        missing_data.append(f"report_disclosures: {exc}")

    try:
        report_notices = get_financial_report_notices(normalized, start_date, end_date)
    except Exception as exc:
        missing_data.append(f"financial_report_notices: {exc}")

    try:
        reduction_events = get_reduction_announcements(normalized, start_date, end_date)
    except Exception as exc:
        missing_data.append(f"reduction_announcements: {exc}")

    try:
        negative_news = _negative_news_within_window(normalized, lookback_days)
    except Exception as exc:
        missing_data.append(f"negative_news: {exc}")

    report_window = _filter_upcoming_report_events(report_events, today, window_days)
    report_notice_window = _filter_recent(report_notices, "公告时间", today, lookback_days)
    reduction_window = _filter_recent(reduction_events, "公告时间", today, lookback_days)
    high_risk_flags: list[str] = []
    if report_window:
        high_risk_flags.append("upcoming_earnings_disclosure")
        high_risk_flags.append("earnings_disclosure_within_window")
    if report_notice_window:
        high_risk_flags.append("recent_financial_report_notice")
    if reduction_window:
        high_risk_flags.append("recent_reduction_announcement")
    if negative_news:
        high_risk_flags.append("recent_negative_news")

    payload = {
        "symbol": normalized,
        "window_days": window_days,
        "lookback_days": lookback_days,
        "fetched_on": today.isoformat(),
        "earnings_disclosures": report_window,
        "recent_financial_report_notices": report_notice_window,
        "reduction_announcements": reduction_window,
        "negative_news": negative_news,
        "risk_flags": high_risk_flags,
        "missing_data": missing_data,
    }
    _save_cached_report(normalized, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and cache risk signals for an A-share stock.")
    parser.add_argument("symbol", help="6-digit A-share symbol")
    parser.add_argument("--window-days", type=int, default=14, help="Days ahead for upcoming events")
    parser.add_argument("--lookback-days", type=int, default=30, help="Days back for recent events and news")
    parser.add_argument("--refresh", action="store_true", help="Force refresh instead of using today's cache")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()

    payload = build_risk_event_report(
        args.symbol,
        window_days=args.window_days,
        lookback_days=args.lookback_days,
        refresh=args.refresh,
    )
    print_json(payload, pretty=args.pretty)


if __name__ == "__main__":
    main()
