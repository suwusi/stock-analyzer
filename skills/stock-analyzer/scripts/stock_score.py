#!/usr/bin/env python3
"""Generate a structured trade recommendation for an A-share stock."""

from __future__ import annotations

import argparse
from typing import Any

from common import WATCHLIST_PATH, ensure_storage, load_json, normalize_symbol, print_json
from fetch_risk_events import build_risk_event_report
from fetch_sector_leaders import build_sector_leader_analysis
from fetch_stock_profile import build_stock_profile


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _find_bucket(symbol: str) -> tuple[str | None, dict[str, Any] | None]:
    ensure_storage()
    data = load_json(WATCHLIST_PATH, {"watchlist": [], "positions": []})
    for bucket in ("positions", "watchlist"):
        for entry in data.get(bucket, []):
            if entry.get("ticker") == symbol:
                return bucket, entry
    return None, None


def compute_scores(profile: dict[str, Any], leaders: dict[str, Any], risks: dict[str, Any]) -> dict[str, Any]:
    history = profile.get("history_snapshot", {})
    quote = profile.get("quote", {})
    financial = profile.get("financial_snapshot", {})
    target_bucket, tracked_entry = _find_bucket(profile["symbol"])

    technical = 40.0
    if _to_float(history.get("latest_close")) > _to_float(history.get("ma5")) > _to_float(history.get("ma10")):
        technical += 20
    if _to_float(history.get("five_day_return_pct")) > 0:
        technical += 15
    if _to_float(quote.get("量比")) >= 1:
        technical += 10
    if _to_float(quote.get("换手率")) >= 3:
        technical += 10
    if _to_float(history.get("ten_day_drawdown_pct")) < -8:
        technical -= 15
    if _to_float(quote.get("涨跌幅")) < _to_float(leaders.get("sector_avg_change_pct")):
        technical -= 10
    technical = max(0.0, min(100.0, technical))

    fundamental = 45.0
    if 0 < _to_float(quote.get("市盈率-动态")) <= 40:
        fundamental += 10
    if 0 < _to_float(quote.get("市净率")) <= 5:
        fundamental += 10
    if _to_float(financial.get("营业总收入-同比增长")) > 0:
        fundamental += 10
    if _to_float(financial.get("净利润-同比增长")) > 0:
        fundamental += 15
    if _to_float(financial.get("净资产收益率")) >= 8:
        fundamental += 5
    if _to_float(financial.get("每股经营现金流量")) < 0:
        fundamental -= 10
    fundamental = max(0.0, min(100.0, fundamental))

    risk = 80.0
    risk_notes: list[str] = []
    if "earnings_disclosure_within_window" in risks.get("risk_flags", []):
        risk -= 20
        risk_notes.append("未来或近期30天内存在财报披露窗口，波动风险上升。")
    if "recent_financial_report_notice" in risks.get("risk_flags", []):
        risk -= 10
        risk_notes.append("近期已披露财报或业绩预告，建议先核对是否存在预期差。")
    if "recent_reduction_announcement" in risks.get("risk_flags", []):
        risk -= 25
        risk_notes.append("近30天内出现减持相关公告，建议降低进攻性。")
    if "recent_negative_news" in risks.get("risk_flags", []):
        risk -= 15
        risk_notes.append("近期出现负面新闻或风险提示，建议降低结论强度。")
    if profile.get("missing_data") or leaders.get("missing_data") or risks.get("missing_data"):
        risk -= 10
        risk_notes.append("部分实时数据缺失，建议降低结论强度。")
    risk = max(0.0, min(100.0, risk))

    total = round(technical * 0.4 + fundamental * 0.3 + risk * 0.3, 2)

    action = "观察"
    position_guidance = "先观察，不超过计划仓位的10%。"
    stop_loss = tracked_entry.get("stop_loss") if tracked_entry else None
    take_profit = tracked_entry.get("target_price") if tracked_entry else None

    if target_bucket == "positions":
        if total >= 75:
            action = "持有"
            position_guidance = "维持核心仓位，可在回踩确认后小幅加仓。"
        elif total >= 60:
            action = "持有"
            position_guidance = "维持现有仓位，等待风险事件落地。"
        elif total >= 45:
            action = "减仓"
            position_guidance = "把仓位降到半仓以下，优先控制回撤。"
        else:
            action = "卖出"
            position_guidance = "优先退出弱势仓位，避免继续承受事件风险。"
    else:
        if total >= 75:
            action = "买入"
            position_guidance = "可分两笔建仓，总仓位控制在20%-30%。"
        elif total >= 60:
            action = "观察"
            position_guidance = "等待放量确认或风险事件过去后再分批介入。"
        elif total >= 45:
            action = "观察"
            position_guidance = "暂不追高，仅保留跟踪。"
        else:
            action = "卖出"
            position_guidance = "不参与当前结构，转向更强标的。"

    latest_close = _to_float(profile.get("history_snapshot", {}).get("latest_close"))
    if stop_loss is None and latest_close:
        stop_loss = round(latest_close * 0.93, 2)
    if take_profit is None and latest_close:
        take_profit = round(latest_close * 1.12, 2)

    return {
        "bucket": target_bucket or "untracked",
        "technical_score": round(technical, 2),
        "fundamental_score": round(fundamental, 2),
        "risk_score": round(risk, 2),
        "total_score": total,
        "action": action,
        "position_guidance": position_guidance,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "risk_notes": risk_notes,
    }


def build_recommendation(symbol: str) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    profile = build_stock_profile(normalized)
    leaders = build_sector_leader_analysis(normalized)
    risks = build_risk_event_report(normalized, window_days=30)
    scores = compute_scores(profile, leaders, risks)
    manual_concept_sector_prompt = ""
    if leaders.get("needs_manual_concept_sector"):
        manual_concept_sector_prompt = (
            f"未获取到 {normalized} 的板块数据。是否手动添加 concept_sector 后再重跑分析？"
        )

    return {
        "symbol": normalized,
        "basic_info": {
            "symbol": normalized,
            "name": profile.get("name"),
            "market": profile.get("market"),
        },
        "concept_sector": profile.get("concept_sector"),
        "target_metrics": {
            "latest_price": profile.get("quote", {}).get("最新价"),
            "current_change_pct": profile.get("quote", {}).get("涨跌幅"),
            "five_day_return_pct": profile.get("history_snapshot", {}).get("five_day_return_pct"),
            "turnover_pct": profile.get("quote", {}).get("换手率"),
            "volume_ratio": profile.get("quote", {}).get("量比"),
            "main_net_inflow_3d": profile.get("fund_flow_snapshot", {}).get("three_day_main_net_inflow"),
        },
        "sector_comparison": leaders.get("comparison_table", []),
        "manual_input_needed": {
            "concept_sector": bool(leaders.get("needs_manual_concept_sector")),
            "prompt": manual_concept_sector_prompt or leaders.get("manual_prompt", ""),
        },
        "scores": {
            "technical": scores["technical_score"],
            "fundamental": scores["fundamental_score"],
            "risk": scores["risk_score"],
            "total": scores["total_score"],
        },
        "action": scores["action"],
        "position_guidance": scores["position_guidance"],
        "stop_loss": scores["stop_loss"],
        "take_profit": scores["take_profit"],
        "risk_events": {
            "earnings_disclosures": risks.get("earnings_disclosures", []),
            "recent_financial_report_notices": risks.get("recent_financial_report_notices", []),
            "reduction_announcements": risks.get("reduction_announcements", []),
            "negative_news": risks.get("negative_news", []),
            "notes": scores["risk_notes"],
        },
        "conclusion": _build_conclusion(profile, leaders, scores),
        "missing_data": profile.get("missing_data", []) + leaders.get("missing_data", []) + risks.get("missing_data", []),
    }


def _build_conclusion(profile: dict[str, Any], leaders: dict[str, Any], scores: dict[str, Any]) -> str:
    name = profile.get("name") or profile["symbol"]
    sector = profile.get("concept_sector", {}).get("concept_sector") or "未知板块"
    leader_names = [item.get("name") for item in leaders.get("leaders", [])]
    leader_text = "、".join(filter(None, leader_names)) or "暂无有效板块龙头"
    return (
        f"{name}当前归属于{sector}。板块强势对标股为{leader_text}。"
        f"综合评分 {scores['total_score']}，建议动作为{scores['action']}；"
        f"{scores['position_guidance']}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a structured A-share trading recommendation.")
    parser.add_argument("symbol", help="6-digit A-share symbol")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()

    payload = build_recommendation(args.symbol)
    print_json(payload, pretty=args.pretty)


if __name__ == "__main__":
    main()
