#!/usr/bin/env python3
"""Shared helpers for the stock-analyzer skill."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
WATCHLIST_PATH = DATA_DIR / "watchlists.json"
HISTORY_CACHE_DIR = DATA_DIR / "history_cache"
RISK_CACHE_DIR = DATA_DIR / "risk_cache"

DEFAULT_WATCHLIST_DATA = {
    "watchlist": [],
    "positions": [],
}


def default_entry_fields() -> dict[str, Any]:
    return {
        "ticker": "",
        "name": "",
        "market": "unknown",
        "bucket": "watchlist",
        "concept_sector": "",
        "tags": [],
        "note": "",
        "cost_basis": None,
        "position_pct": None,
        "target_price": None,
        "stop_loss": None,
        "updated_at": "",
    }


def ensure_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    RISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if not WATCHLIST_PATH.exists():
        save_json(WATCHLIST_PATH, DEFAULT_WATCHLIST_DATA)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if path == WATCHLIST_PATH:
        return normalize_watchlist_data(payload)
    return payload


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_watchlist_data(payload: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(payload, dict):
        return DEFAULT_WATCHLIST_DATA.copy()

    normalized: dict[str, list[dict[str, Any]]] = {
        "watchlist": [],
        "positions": [],
    }
    defaults = default_entry_fields()

    for bucket in ("watchlist", "positions"):
        items = payload.get(bucket, [])
        if not isinstance(items, list):
            continue
        for raw_entry in items:
            if not isinstance(raw_entry, dict):
                continue
            entry = defaults.copy()
            entry.update(raw_entry)
            entry["bucket"] = bucket
            normalized[bucket].append(entry)

    return normalized


def tracked_concept_sector(symbol: str) -> str:
    normalized = normalize_symbol(symbol)
    data = load_json(WATCHLIST_PATH, DEFAULT_WATCHLIST_DATA)
    for bucket in ("positions", "watchlist"):
        for entry in data.get(bucket, []):
            if entry.get("ticker") == normalized:
                return str(entry.get("concept_sector") or "").strip()
    return ""


def normalize_symbol(raw_symbol: str) -> str:
    symbol = re.sub(r"\D", "", raw_symbol or "")
    if len(symbol) != 6:
        raise ValueError(f"Expected a 6-digit A-share symbol, got: {raw_symbol!r}")
    return symbol


def market_for_symbol(symbol: str) -> str:
    normalized = normalize_symbol(symbol)
    if normalized.startswith(("600", "601", "603", "605", "688", "689")):
        return "sh"
    if normalized.startswith(("000", "001", "002", "003", "300", "301")):
        return "sz"
    if normalized.startswith(("430", "800", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879", "920")):
        return "bj"
    return "unknown"


def watchlist_bucket_name(bucket: str) -> str:
    if bucket not in {"watchlist", "positions"}:
        raise ValueError("bucket must be 'watchlist' or 'positions'")
    return bucket


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def filter_window(items: list[dict[str, Any]], key: str, center: date, window_days: int) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in items:
        event_date = parse_date(str(item.get(key, "")))
        if not event_date:
            continue
        if abs((event_date - center).days) <= window_days:
            filtered.append(item)
    return filtered


def print_json(payload: Any, pretty: bool = False) -> None:
    if pretty:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return
    print(json.dumps(payload, ensure_ascii=False))
