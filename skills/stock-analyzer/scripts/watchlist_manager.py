#!/usr/bin/env python3
"""Manage the local watchlist and positions pools."""

from __future__ import annotations

import argparse
import re
from typing import Any

from common import (
    WATCHLIST_PATH,
    ensure_storage,
    load_json,
    market_for_symbol,
    normalize_symbol,
    now_iso,
    print_json,
    save_json,
    watchlist_bucket_name,
)
from market_data import find_quote_by_name, get_quote_snapshot, get_stock_info, resolve_concept_sector


def _find_entry(data: dict[str, Any], symbol: str) -> tuple[str | None, dict[str, Any] | None]:
    for bucket in ("watchlist", "positions"):
        for entry in data.get(bucket, []):
            if entry.get("ticker") == symbol:
                return bucket, entry
    return None, None

def _looks_like_symbol(value: str | None) -> bool:
    return bool(re.fullmatch(r"\D*\d{6}\D*", value or ""))


def _resolve_symbol_from_name(name: str) -> tuple[str, str]:
    quote = find_quote_by_name(name)
    symbol = str(quote.get("代码") or "").strip()
    if symbol:
        return normalize_symbol(symbol), "quote_snapshot"
    raise RuntimeError(f"Unable to resolve symbol for {name}")


def _resolve_security_name(symbol: str) -> tuple[str, str]:
    try:
        quote = get_quote_snapshot(symbol)
        name = str(quote.get("名称") or "").strip()
        if name:
            return name, "quote_snapshot"
    except Exception:
        pass

    info = get_stock_info(symbol)
    name = str(info.get("股票简称") or "").strip()
    if name:
        return name, "stock_info"
    raise RuntimeError(f"Unable to resolve security name for {symbol}")


def add_entry(args: argparse.Namespace) -> dict[str, Any]:
    ensure_storage()
    data = load_json(WATCHLIST_PATH, {"watchlist": [], "positions": []})
    identifier = args.identifier
    symbol_source = "user_input"
    if args.auto_symbol and not _looks_like_symbol(identifier):
        symbol, symbol_source = _resolve_symbol_from_name(identifier)
    else:
        symbol = normalize_symbol(identifier)
    bucket = watchlist_bucket_name(args.bucket)

    existing_bucket, existing_entry = _find_entry(data, symbol)
    if existing_entry:
        return {
            "status": "exists",
            "bucket": existing_bucket,
            "entry": existing_entry,
        }

    concept_sector = args.concept_sector
    sector_source = "user_input" if concept_sector else "unknown"
    name = args.name or (identifier if args.auto_symbol and not _looks_like_symbol(identifier) else "")
    name_source = "user_input" if name else "unknown"

    if not name and args.auto_name:
        try:
            name, name_source = _resolve_security_name(symbol)
        except Exception as exc:
            name_source = f"auto_name_failed: {exc}"

    if not concept_sector and args.auto_sector:
        try:
            resolved = resolve_concept_sector(symbol)
            concept_sector = resolved.get("concept_sector") or ""
            sector_source = resolved.get("source", "unknown")
        except Exception as exc:
            sector_source = f"auto_sector_failed: {exc}"

    entry = {
        "ticker": symbol,
        "name": name,
        "market": market_for_symbol(symbol),
        "bucket": bucket,
        "concept_sector": concept_sector or "",
        "tags": args.tags or [],
        "note": args.note or "",
        "cost_basis": args.cost_basis,
        "position_pct": args.position_pct,
        "target_price": args.target_price,
        "stop_loss": args.stop_loss,
        "updated_at": now_iso(),
    }
    data[bucket].append(entry)
    save_json(WATCHLIST_PATH, data)
    return {
        "status": "added",
        "symbol_source": symbol_source,
        "name_source": name_source,
        "sector_source": sector_source,
        "entry": entry,
    }


def list_entries(args: argparse.Namespace) -> dict[str, Any]:
    ensure_storage()
    data = load_json(WATCHLIST_PATH, {"watchlist": [], "positions": []})

    if args.list:
        # Handle --list parameter
        bucket = watchlist_bucket_name(args.list)
        return {"bucket": bucket, "entries": data.get(bucket, [])}
    elif args.bucket:
        bucket = watchlist_bucket_name(args.bucket)
        return {"bucket": bucket, "entries": data.get(bucket, [])}
    return data


def move_entry(args: argparse.Namespace) -> dict[str, Any]:
    ensure_storage()
    data = load_json(WATCHLIST_PATH, {"watchlist": [], "positions": []})
    symbol = normalize_symbol(args.symbol)
    target_bucket = watchlist_bucket_name(args.bucket)
    current_bucket, entry = _find_entry(data, symbol)
    if not entry or not current_bucket:
        raise SystemExit(f"Symbol {symbol} is not tracked.")
    if current_bucket == target_bucket:
        return {"status": "unchanged", "entry": entry}

    data[current_bucket] = [item for item in data[current_bucket] if item.get("ticker") != symbol]
    entry["bucket"] = target_bucket
    entry["updated_at"] = now_iso()
    if args.cost_basis is not None:
        entry["cost_basis"] = args.cost_basis
    if args.position_pct is not None:
        entry["position_pct"] = args.position_pct
    if args.stop_loss is not None:
        entry["stop_loss"] = args.stop_loss
    data[target_bucket].append(entry)
    save_json(WATCHLIST_PATH, data)
    return {"status": "moved", "from": current_bucket, "to": target_bucket, "entry": entry}


def update_entry(args: argparse.Namespace) -> dict[str, Any]:
    ensure_storage()
    data = load_json(WATCHLIST_PATH, {"watchlist": [], "positions": []})
    symbol = normalize_symbol(args.symbol)
    _, entry = _find_entry(data, symbol)
    if not entry:
        raise SystemExit(f"Symbol {symbol} is not tracked.")

    name_source = "unchanged"
    if args.name is not None:
        entry["name"] = args.name
        name_source = "user_input"
    if args.auto_name:
        try:
            entry["name"], name_source = _resolve_security_name(symbol)
        except Exception as exc:
            name_source = f"auto_name_failed: {exc}"
    if args.note is not None:
        entry["note"] = args.note
    if args.concept_sector is not None:
        entry["concept_sector"] = args.concept_sector
    if args.tags is not None:
        entry["tags"] = args.tags
    if args.cost_basis is not None:
        entry["cost_basis"] = args.cost_basis
    if args.position_pct is not None:
        entry["position_pct"] = args.position_pct
    if args.target_price is not None:
        entry["target_price"] = args.target_price
    if args.stop_loss is not None:
        entry["stop_loss"] = args.stop_loss
    entry["updated_at"] = now_iso()
    save_json(WATCHLIST_PATH, data)
    return {"status": "updated", "name_source": name_source, "entry": entry}


def enrich_entries(args: argparse.Namespace) -> dict[str, Any]:
    ensure_storage()
    data = load_json(WATCHLIST_PATH, {"watchlist": [], "positions": []})
    buckets = [watchlist_bucket_name(args.bucket)] if args.bucket else ["watchlist", "positions"]
    results: list[dict[str, Any]] = []

    for bucket in buckets:
        for entry in data.get(bucket, []):
            raw_symbol = entry.get("ticker", "")
            symbol = normalize_symbol(raw_symbol) if raw_symbol else ""
            result = {
                "ticker": symbol,
                "bucket": bucket,
                "name": entry.get("name", ""),
                "concept_sector": entry.get("concept_sector", ""),
            }
            touched = False

            if args.auto_symbol and not symbol and entry.get("name"):
                try:
                    entry["ticker"], source = _resolve_symbol_from_name(str(entry["name"]))
                    entry["market"] = market_for_symbol(entry["ticker"])
                    symbol = entry["ticker"]
                    result["ticker"] = symbol
                    result["symbol_source"] = source
                    touched = True
                except Exception as exc:
                    result["symbol_source"] = f"auto_symbol_failed: {exc}"

            if args.auto_name and symbol and not entry.get("name"):
                try:
                    entry["name"], source = _resolve_security_name(symbol)
                    result["name"] = entry["name"]
                    result["name_source"] = source
                    touched = True
                except Exception as exc:
                    result["name_source"] = f"auto_name_failed: {exc}"

            if args.auto_sector and symbol and not entry.get("concept_sector"):
                try:
                    resolved = resolve_concept_sector(symbol)
                    entry["concept_sector"] = resolved.get("concept_sector") or ""
                    result["concept_sector"] = entry["concept_sector"]
                    result["sector_source"] = resolved.get("source", "unknown")
                    touched = True
                except Exception as exc:
                    result["sector_source"] = f"auto_sector_failed: {exc}"

            if touched:
                entry["updated_at"] = now_iso()
            results.append(result)

    save_json(WATCHLIST_PATH, data)
    return {"status": "enriched", "entries": results}


def remove_entry(args: argparse.Namespace) -> dict[str, Any]:
    ensure_storage()
    data = load_json(WATCHLIST_PATH, {"watchlist": [], "positions": []})
    symbol = normalize_symbol(args.symbol)
    bucket, entry = _find_entry(data, symbol)
    if not entry or not bucket:
        raise SystemExit(f"Symbol {symbol} is not tracked.")
    data[bucket] = [item for item in data[bucket] if item.get("ticker") != symbol]
    save_json(WATCHLIST_PATH, data)
    return {"status": "removed", "bucket": bucket, "entry": entry}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the local watchlist and positions files.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("add", help="Add a stock to watchlist or positions")
    add_parser.add_argument("identifier", help="6-digit A-share symbol, or security name when used with --auto-symbol")
    add_parser.add_argument("--bucket", default="watchlist", choices=["watchlist", "positions"])
    add_parser.add_argument("--name")
    add_parser.add_argument("--auto-symbol", action="store_true")
    add_parser.add_argument("--auto-name", action="store_true")
    add_parser.add_argument("--concept-sector")
    add_parser.add_argument("--auto-sector", action="store_true")
    add_parser.add_argument("--tags", nargs="*")
    add_parser.add_argument("--note")
    add_parser.add_argument("--cost-basis", type=float)
    add_parser.add_argument("--position-pct", type=float)
    add_parser.add_argument("--target-price", type=float)
    add_parser.add_argument("--stop-loss", type=float)
    add_parser.set_defaults(func=add_entry)

    list_parser = subparsers.add_parser("list", help="List tracked stocks")
    list_parser.add_argument("--bucket", choices=["watchlist", "positions"])
    list_parser.set_defaults(func=list_entries)

    move_parser = subparsers.add_parser("move", help="Move a stock between watchlist and positions")
    move_parser.add_argument("symbol")
    move_parser.add_argument("--bucket", required=True, choices=["watchlist", "positions"])
    move_parser.add_argument("--cost-basis", type=float)
    move_parser.add_argument("--position-pct", type=float)
    move_parser.add_argument("--stop-loss", type=float)
    move_parser.set_defaults(func=move_entry)

    update_parser = subparsers.add_parser("update", help="Update a tracked stock")
    update_parser.add_argument("symbol")
    update_parser.add_argument("--name")
    update_parser.add_argument("--auto-name", action="store_true")
    update_parser.add_argument("--concept-sector")
    update_parser.add_argument("--tags", nargs="*")
    update_parser.add_argument("--note")
    update_parser.add_argument("--cost-basis", type=float)
    update_parser.add_argument("--position-pct", type=float)
    update_parser.add_argument("--target-price", type=float)
    update_parser.add_argument("--stop-loss", type=float)
    update_parser.set_defaults(func=update_entry)

    remove_parser = subparsers.add_parser("remove", help="Remove a tracked stock")
    remove_parser.add_argument("symbol")
    remove_parser.set_defaults(func=remove_entry)

    enrich_parser = subparsers.add_parser("enrich", help="Auto-fill missing codes, names, or sectors for tracked stocks")
    enrich_parser.add_argument("--bucket", choices=["watchlist", "positions"])
    enrich_parser.add_argument("--auto-symbol", action="store_true")
    enrich_parser.add_argument("--auto-name", action="store_true")
    enrich_parser.add_argument("--auto-sector", action="store_true")
    enrich_parser.set_defaults(func=enrich_entries)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.list:
        # Handle --list parameter
        list_entries(args)
    else:
        payload = args.func(args)
        print_json(payload, pretty=True)


if __name__ == "__main__":
    main()
