#!/usr/bin/env python3
"""Refresh cached daily risk signals for tracked A-share symbols."""

from __future__ import annotations

import argparse
from typing import Any

from common import WATCHLIST_PATH, ensure_storage, load_json, print_json
from fetch_risk_events import build_risk_event_report


def _tracked_symbols(bucket: str) -> list[str]:
    ensure_storage()
    data = load_json(WATCHLIST_PATH, {"watchlist": [], "positions": []})
    buckets = ["positions", "watchlist"] if bucket == "all" else [bucket]
    seen: set[str] = set()
    symbols: list[str] = []
    for current_bucket in buckets:
        for entry in data.get(current_bucket, []):
            symbol = str(entry.get("ticker") or "").strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


def refresh_daily_risks(bucket: str, window_days: int, lookback_days: int) -> dict[str, Any]:
    symbols = _tracked_symbols(bucket)
    refreshed: list[dict[str, Any]] = []
    for symbol in symbols:
        payload = build_risk_event_report(
            symbol,
            window_days=window_days,
            lookback_days=lookback_days,
            refresh=True,
        )
        refreshed.append(
            {
                "symbol": payload["symbol"],
                "fetched_on": payload.get("fetched_on"),
                "risk_flags": payload.get("risk_flags", []),
                "missing_data": payload.get("missing_data", []),
            }
        )
    return {
        "bucket": bucket,
        "symbol_count": len(symbols),
        "items": refreshed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh daily risk caches for tracked A-share symbols.")
    parser.add_argument("--bucket", choices=("positions", "watchlist", "all"), default="all")
    parser.add_argument("--window-days", type=int, default=14, help="Days ahead for upcoming events")
    parser.add_argument("--lookback-days", type=int, default=30, help="Days back for recent events and news")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()

    payload = refresh_daily_risks(
        bucket=args.bucket,
        window_days=args.window_days,
        lookback_days=args.lookback_days,
    )
    print_json(payload, pretty=args.pretty)


if __name__ == "__main__":
    main()
