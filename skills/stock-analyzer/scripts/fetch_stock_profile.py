#!/usr/bin/env python3
"""Fetch a live A-share stock profile."""

from __future__ import annotations

import argparse
from typing import Any

from common import ensure_storage, normalize_symbol, print_json, tracked_concept_sector
from market_data import (
    compute_flow_snapshot,
    compute_history_snapshot,
    get_daily_history,
    get_fund_flow,
    get_quote_snapshot,
    get_recent_financial_report,
    get_stock_info,
    resolve_concept_sector,
)


def build_stock_profile(symbol: str) -> dict[str, Any]:
    ensure_storage()
    normalized = normalize_symbol(symbol)
    local_concept_sector = tracked_concept_sector(normalized)

    missing_data: list[str] = []
    quote: dict[str, Any] = {}
    stock_info: dict[str, Any] = {}
    history: list[dict[str, Any]] = []
    fund_flow: list[dict[str, Any]] = []
    financial_report: dict[str, Any] = {}

    try:
        quote = get_quote_snapshot(normalized)
    except Exception as exc:
        missing_data.append(f"quote_snapshot: {exc}")

    try:
        stock_info = get_stock_info(normalized)
    except Exception as exc:
        missing_data.append(f"stock_info: {exc}")

    try:
        history = get_daily_history(normalized, days=30)
    except Exception as exc:
        missing_data.append(f"daily_history: {exc}")

    try:
        fund_flow = get_fund_flow(normalized, days=5)
    except Exception as exc:
        missing_data.append(f"fund_flow: {exc}")

    try:
        financial_report = get_recent_financial_report(normalized)
    except Exception as exc:
        missing_data.append(f"recent_financial_report: {exc}")

    try:
        sector = resolve_concept_sector(normalized, local_override=local_concept_sector)
    except Exception as exc:
        sector = {
            "concept_sector": local_concept_sector or "",
            "source": "tracked_entry" if local_concept_sector else "unresolved",
            "candidates": [local_concept_sector] if local_concept_sector else [],
        }
        missing_data.append(f"concept_sector: {exc}")

    return {
        "symbol": normalized,
        "name": quote.get("名称") or stock_info.get("股票简称") or "",
        "market": quote.get("市场") or stock_info.get("市场") or "",
        "concept_sector": sector,
        "quote": quote,
        "stock_info": stock_info,
        "history_snapshot": compute_history_snapshot(history),
        "financial_snapshot": financial_report,
        "fund_flow_snapshot": compute_flow_snapshot(fund_flow),
        "missing_data": missing_data,
    }


def build_list_profile(list_type: str) -> list[dict[str, Any]]:
    """Build profiles for all stocks in the specified list."""
    from common import load_json, WATCHLIST_PATH, watchlist_bucket_name

    data = load_json(WATCHLIST_PATH, {"watchlist": [], "positions": []})
    bucket = watchlist_bucket_name(list_type)
    entries = data.get(bucket, [])

    profiles = []
    for entry in entries:
        symbol = entry.get("ticker", "")
        if symbol:
            profile = build_stock_profile(symbol)
            profiles.append(profile)

    return profiles


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch the latest stock profile for an A-share symbol.")
    parser.add_argument("symbol", nargs="?", help="6-digit A-share symbol")
    parser.add_argument("--list", help="Fetch profiles for all stocks in watchlist or positions")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()

    if args.list:
        profiles = build_list_profile(args.list)
        print_json(profiles, pretty=args.pretty)
    elif args.symbol:
        payload = build_stock_profile(args.symbol)
        print_json(payload, pretty=args.pretty)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
