#!/usr/bin/env python3
"""Find the strongest three stocks inside a concept sector."""

from __future__ import annotations

import argparse
from typing import Any

from common import ensure_storage, normalize_symbol, print_json, tracked_concept_sector
from market_data import (
    compute_breakout_time,
    compute_flow_snapshot,
    compute_history_snapshot,
    get_concept_board_members,
    get_daily_history,
    get_fund_flow,
    resolve_concept_sector,
)


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _rank_map(values: list[tuple[str, float]], reverse: bool = True) -> dict[str, float]:
    ordered = sorted(values, key=lambda item: item[1], reverse=reverse)
    total = max(len(ordered), 1)
    result: dict[str, float] = {}
    for index, (symbol, _) in enumerate(ordered):
        result[symbol] = (total - index) / total
    return result


def _breakout_score(timestamp: str | None) -> float:
    if not timestamp:
        return 0.0
    time_part = timestamp.split(" ")[-1]
    hour, minute, _ = [int(part) for part in time_part.split(":")]
    return max(0.0, 1.0 - ((hour * 60 + minute) - (9 * 60 + 30)) / 330.0)


def _candidate_metrics(row: dict[str, Any]) -> dict[str, Any]:
    symbol = normalize_symbol(str(row.get("代码")))
    try:
        history = get_daily_history(symbol, days=15)
    except Exception:
        history = []
    try:
        fund_flow = get_fund_flow(symbol, days=5)
    except Exception:
        fund_flow = []
    history_snapshot = compute_history_snapshot(history)
    flow_snapshot = compute_flow_snapshot(fund_flow)
    try:
        breakout_time = compute_breakout_time(symbol)
    except Exception:
        breakout_time = None
    return {
        "symbol": symbol,
        "name": row.get("名称"),
        "current_change_pct": _to_float(row.get("涨跌幅")),
        "latest_price": row.get("最新价"),
        "turnover_pct": _to_float(row.get("换手率")),
        "amount": _to_float(row.get("成交额")),
        "price_to_book": row.get("市净率"),
        "price_to_earnings_dynamic": row.get("市盈率-动态"),
        "history_snapshot": history_snapshot,
        "fund_flow_snapshot": flow_snapshot,
        "breakout_time": breakout_time,
    }


def build_sector_leader_analysis(symbol: str) -> dict[str, Any]:
    ensure_storage()
    normalized = normalize_symbol(symbol)
    local_concept_sector = tracked_concept_sector(normalized)
    missing_data: list[str] = []
    try:
        sector_resolution = resolve_concept_sector(normalized, local_override=local_concept_sector)
    except Exception as exc:
        sector_resolution = {
            "concept_sector": local_concept_sector,
            "source": "tracked_entry" if local_concept_sector else "unresolved",
            "candidates": [local_concept_sector] if local_concept_sector else [],
        }
        missing_data.append(f"concept_sector: {exc}")
    sector_name = sector_resolution.get("concept_sector") or ""
    if not sector_name:
        return {
            "symbol": normalized,
            "sector": sector_resolution,
            "leaders": [],
            "comparison_table": [],
            "needs_manual_concept_sector": True,
            "manual_prompt": "未获取到板块数据，建议用户手动补充 concept_sector 后再重跑分析。",
            "missing_data": missing_data + ["concept_sector unresolved"],
        }

    try:
        members = get_concept_board_members(sector_name)
    except Exception as exc:
        return {
            "symbol": normalized,
            "sector": sector_resolution,
            "leaders": [],
            "comparison_table": [],
            "needs_manual_concept_sector": False,
            "manual_prompt": "",
            "missing_data": missing_data + [f"concept_board_members: {exc}"],
        }
    sector_avg_change = 0.0
    if members:
        sector_avg_change = sum(_to_float(item.get("涨跌幅")) for item in members) / len(members)

    candidates = sorted(members, key=lambda item: _to_float(item.get("涨跌幅")), reverse=True)[:12]
    enriched = [_candidate_metrics(row) for row in candidates]
    if not any(item["symbol"] == normalized for item in enriched):
        target_row = next((row for row in members if normalize_symbol(str(row.get("代码"))) == normalized), None)
        if target_row:
            enriched.append(_candidate_metrics(target_row))

    five_day_ranks = _rank_map(
        [
            (item["symbol"], _to_float(item["history_snapshot"].get("five_day_return_pct")))
            for item in enriched
        ]
    )
    current_change_ranks = _rank_map([(item["symbol"], item["current_change_pct"]) for item in enriched])
    amount_ranks = _rank_map([(item["symbol"], item["amount"]) for item in enriched])
    flow_ranks = _rank_map(
        [
            (item["symbol"], _to_float(item["fund_flow_snapshot"].get("three_day_main_net_inflow")))
            for item in enriched
        ]
    )
    resilience_ranks = _rank_map(
        [
            (item["symbol"], _to_float(item["history_snapshot"].get("ten_day_resilience_pct")))
            for item in enriched
        ]
    )

    for item in enriched:
        breakout_component = _breakout_score(item["breakout_time"])
        relative_sector_strength = max(0.0, item["current_change_pct"] - sector_avg_change) / 10.0
        item["leader_score"] = round(
            current_change_ranks.get(item["symbol"], 0) * 35
            + five_day_ranks.get(item["symbol"], 0) * 20
            + breakout_component * 10
            + amount_ranks.get(item["symbol"], 0) * 15
            + flow_ranks.get(item["symbol"], 0) * 10
            + resilience_ranks.get(item["symbol"], 0) * 5
            + min(relative_sector_strength * 5, 5),
            2,
        )
        item["stronger_than_sector_avg"] = item["current_change_pct"] > sector_avg_change

    leaders = sorted(enriched, key=lambda item: item["leader_score"], reverse=True)[:3]
    target = next((item for item in enriched if item["symbol"] == normalized), None)
    comparison_table: list[dict[str, Any]] = []
    rows = [target] + leaders if target else leaders
    seen: set[str] = set()
    for item in rows:
        if not item or item["symbol"] in seen:
            continue
        seen.add(item["symbol"])
        comparison_table.append(
            {
                "symbol": item["symbol"],
                "name": item["name"],
                "current_change_pct": item["current_change_pct"],
                "five_day_return_pct": item["history_snapshot"].get("five_day_return_pct"),
                "amount": item["amount"],
                "turnover_pct": item["turnover_pct"],
                "three_day_main_net_inflow": item["fund_flow_snapshot"].get("three_day_main_net_inflow"),
                "stronger_than_sector_avg": item["stronger_than_sector_avg"],
                "leader_score": item["leader_score"],
            }
        )

    return {
        "symbol": normalized,
        "sector": sector_resolution,
        "sector_avg_change_pct": round(sector_avg_change, 2),
        "leaders": leaders,
        "comparison_table": comparison_table,
        "needs_manual_concept_sector": False,
        "manual_prompt": "",
        "missing_data": missing_data,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch the top 3 concept-sector leaders for a stock.")
    parser.add_argument("symbol", help="6-digit A-share symbol")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()

    payload = build_sector_leader_analysis(args.symbol)
    print_json(payload, pretty=args.pretty)


if __name__ == "__main__":
    main()
