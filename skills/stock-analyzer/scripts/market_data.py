#!/usr/bin/env python3
"""Live market-data helpers backed by AkShare and CNInfo."""

from __future__ import annotations

import contextlib
import io
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from common import HISTORY_CACHE_DIR, market_for_symbol, normalize_symbol


_SINA_SPOT_CACHE: list[dict[str, Any]] | None = None
_SINA_SPOT_CACHE_AT: datetime | None = None
_SINA_SPOT_CACHE_TTL_SECONDS = 180
_SINA_SPOT_RETRIES = 3


def _get_akshare():
    try:
        import akshare as ak  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "akshare is required for live fetching. Install it before running live scripts."
        ) from exc
    return ak


def _records(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if hasattr(frame, "fillna"):
        frame = frame.fillna("")
    if hasattr(frame, "to_dict"):
        return list(frame.to_dict(orient="records"))
    return list(frame)


def _quiet_call(func, *args, **kwargs):
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return func(*args, **kwargs)


def _find_by_symbol(records: list[dict[str, Any]], symbol: str, key: str = "代码") -> dict[str, Any] | None:
    normalized = normalize_symbol(symbol)
    for item in records:
        candidate = str(item.get(key, "")).strip()
        try:
            if normalize_symbol(candidate) == normalized:
                return item
        except Exception:
            continue
    return None


def _find_by_name(records: list[dict[str, Any]], name: str, key: str = "名称") -> dict[str, Any] | None:
    normalized = str(name or "").strip()
    if not normalized:
        return None
    exact_matches: list[dict[str, Any]] = []
    partial_matches: list[dict[str, Any]] = []
    for item in records:
        candidate = str(item.get(key, "")).strip()
        if not candidate:
            continue
        if candidate == normalized:
            exact_matches.append(item)
        elif normalized in candidate:
            partial_matches.append(item)
    if exact_matches:
        return exact_matches[0]
    if partial_matches:
        return partial_matches[0]
    return None


def recent_report_periods(today: date | None = None) -> list[str]:
    now = today or date.today()
    quarter_labels = [
        (3, 31, "一季报"),
        (6, 30, "半年报"),
        (9, 30, "三季报"),
        (12, 31, "年报"),
    ]
    periods: list[str] = []
    for year in range(now.year, now.year - 3, -1):
        for month, day, label in reversed(quarter_labels):
            period_date = date(year, month, day)
            if period_date <= now + timedelta(days=120):
                periods.append(f"{year}{label}")
    ordered: list[str] = []
    seen = set()
    for item in periods:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered[:8]


def recent_report_dates(today: date | None = None) -> list[str]:
    now = today or date.today()
    quarter_dates = ["0331", "0630", "0930", "1231"]
    values: list[str] = []
    for year in range(now.year, now.year - 3, -1):
        for quarter in reversed(quarter_dates):
            values.append(f"{year}{quarter}")
    return values[:8]


def _sina_spot_records(force_refresh: bool = False) -> list[dict[str, Any]]:
    global _SINA_SPOT_CACHE, _SINA_SPOT_CACHE_AT

    now = datetime.now()
    if (
        not force_refresh
        and _SINA_SPOT_CACHE is not None
        and _SINA_SPOT_CACHE_AT is not None
        and (now - _SINA_SPOT_CACHE_AT).total_seconds() < _SINA_SPOT_CACHE_TTL_SECONDS
    ):
        return _SINA_SPOT_CACHE

    records: list[dict[str, Any]] | None = None
    last_error: Exception | None = None
    for attempt in range(_SINA_SPOT_RETRIES):
        try:
            ak = _get_akshare()
            # Sina spot pulls the whole market page-by-page and prints a progress bar.
            # Cache the result briefly so per-symbol analysis does not repeat that scan.
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                records = _records(ak.stock_zh_a_spot())
            if records:
                break
        except Exception as exc:
            last_error = exc
        if attempt < _SINA_SPOT_RETRIES - 1:
            time.sleep(0.5)

    if not records:
        if last_error:
            raise last_error
        raise RuntimeError("sina spot returned no records")

    _SINA_SPOT_CACHE = records
    _SINA_SPOT_CACHE_AT = datetime.now()
    return records


def get_quote_snapshot(symbol: str) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    errors: list[str] = []

    try:
        records = _sina_spot_records()
        record = _find_by_symbol(records, normalized, key="代码")
        if record:
            return _normalize_sina_quote(record, normalized)
    except Exception as exc:
        errors.append(f"sina_primary: {exc}")

    try:
        ak = _get_akshare()
        records = _records(ak.stock_zh_a_spot_em())
        record = _find_by_symbol(records, normalized)
        if record:
            return record
        errors.append(f"Quote snapshot not found for symbol {normalized}")
    except Exception as exc:
        errors.append(f"eastmoney_fallback: {exc}")

    raise RuntimeError(f"Quote snapshot not found for symbol {normalized}; errors: {' | '.join(errors)}")


def find_quote_by_name(name: str) -> dict[str, Any]:
    errors: list[str] = []
    try:
        records = _sina_spot_records()
        record = _find_by_name(records, name, key="名称")
        if record:
            symbol = str(record.get("代码") or "").strip()
            return _normalize_sina_quote(record, symbol)
    except Exception as exc:
        errors.append(f"sina_primary: {exc}")

    try:
        ak = _get_akshare()
        records = _records(ak.stock_zh_a_spot_em())
        record = _find_by_name(records, name)
        if record:
            return record
        errors.append(f"Quote snapshot not found for name {name}")
    except Exception as exc:
        errors.append(f"eastmoney_fallback: {exc}")

    raise RuntimeError(f"Quote snapshot not found for name {name}; errors: {' | '.join(errors)}")


def _normalize_sina_quote(record: dict[str, Any], symbol: str) -> dict[str, Any]:
    return {
        "代码": normalize_symbol(symbol),
        "名称": record.get("名称") or "",
        "最新价": record.get("最新价"),
        "涨跌幅": record.get("涨跌幅"),
        "涨跌额": record.get("涨跌额"),
        "今开": record.get("今开"),
        "最高": record.get("最高"),
        "最低": record.get("最低"),
        "昨收": record.get("昨收"),
        "成交量": record.get("成交量"),
        "成交额": record.get("成交额"),
        "买入": record.get("买入"),
        "卖出": record.get("卖出"),
        "换手率": None,
        "量比": None,
        "市盈率-动态": None,
        "市净率": None,
        "总市值": None,
        "流通市值": None,
        "市场": market_for_symbol(symbol),
        "时间戳": record.get("时间戳"),
        "source": "sina_spot",
    }


def get_stock_info(symbol: str) -> dict[str, Any]:
    ak = _get_akshare()
    rows = _records(ak.stock_individual_info_em(symbol=normalize_symbol(symbol)))
    info: dict[str, Any] = {}
    for row in rows:
        info[str(row.get("item", ""))] = row.get("value")
    return info


def get_daily_history(symbol: str, days: int = 60) -> list[dict[str, Any]]:
    normalized = normalize_symbol(symbol)
    cached = _read_daily_history_cache(normalized)
    if cached is not None:
        return cached[-days:]

    end_date = date.today()
    start_date = end_date - timedelta(days=max(days * 3, 120))
    errors: list[str] = []

    try:
        records = _fetch_sina_daily_history(normalized, start_date, end_date)
        if records:
            _write_daily_history_cache(normalized, records)
            return records[-days:]
        errors.append("sina returned no records")
    except Exception as exc:
        errors.append(f"sina_primary: {exc}")

    try:
        ak = _get_akshare()
        records = _records(
            ak.stock_zh_a_hist(
                symbol=normalized,
                period="daily",
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
                adjust="qfq",
            )
        )
        if records:
            _write_daily_history_cache(normalized, records)
            return records[-days:]
        errors.append("eastmoney returned no records")
    except Exception as exc:
        errors.append(f"eastmoney_fallback: {exc}")

    raise RuntimeError(f"Daily history not found for symbol {normalized}; errors: {' | '.join(errors)}")


def _history_cache_path(symbol: str) -> Path:
    HISTORY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return HISTORY_CACHE_DIR / f"{normalize_symbol(symbol)}.json"


def _read_daily_history_cache(symbol: str) -> list[dict[str, Any]] | None:
    cache_path = _history_cache_path(symbol)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if payload.get("fetched_on") != date.today().isoformat():
        return None
    records = payload.get("records")
    if not isinstance(records, list):
        return None
    return records


def _write_daily_history_cache(symbol: str, records: list[dict[str, Any]]) -> None:
    cache_path = _history_cache_path(symbol)
    payload = {
        "symbol": normalize_symbol(symbol),
        "fetched_on": date.today().isoformat(),
        "records": records,
    }
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _fetch_sina_daily_history(symbol: str, start_date: date, end_date: date) -> list[dict[str, Any]]:
    ak = _get_akshare()
    sina_symbol = f"{market_for_symbol(symbol)}{normalize_symbol(symbol)}"
    frame = ak.stock_zh_a_daily(
        symbol=sina_symbol,
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        adjust="qfq",
    )
    records = _records(frame)
    normalized_records: list[dict[str, Any]] = []
    for row in records:
        normalized_records.append(
            {
                "日期": str(row.get("date", "")),
                "开盘": row.get("open"),
                "收盘": row.get("close"),
                "最高": row.get("high"),
                "最低": row.get("low"),
                "成交量": row.get("volume"),
                "成交额": row.get("amount"),
                "换手率": row.get("turnover"),
            }
        )
    return normalized_records


def get_fund_flow(symbol: str, days: int = 5) -> list[dict[str, Any]]:
    ak = _get_akshare()
    records = _records(
        ak.stock_individual_fund_flow(
            stock=normalize_symbol(symbol),
            market=market_for_symbol(symbol),
        )
    )
    return records[-days:]


def get_recent_financial_report(symbol: str) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    errors: list[str] = []

    try:
        report = _fetch_sina_financial_summary(normalized)
        if report:
            return report
    except Exception as exc:
        errors.append(f"sina_primary: {exc}")

    try:
        ak = _get_akshare()
        for report_date in recent_report_dates():
            records = _records(_quiet_call(ak.stock_yjbb_em, date=report_date))
            match = _find_by_symbol(records, normalized, key="股票代码")
            if match:
                match["报告期"] = report_date
                match["source"] = "eastmoney_yjbb"
                return match
    except Exception as exc:
        errors.append(f"eastmoney_fallback: {exc}")

    if errors:
        raise RuntimeError(" | ".join(errors))
    return {}


def _fetch_sina_financial_summary(symbol: str) -> dict[str, Any]:
    ak = _get_akshare()
    df = _quiet_call(ak.stock_financial_abstract, symbol=normalize_symbol(symbol))
    records = _records(df)
    if not records:
        return {}

    date_columns = [col for col in df.columns if str(col).isdigit()]
    if not date_columns:
        return {}
    latest_col = date_columns[0]
    latest_date = str(latest_col)
    prior_col = _same_period_last_year(date_columns, latest_col)

    def row_value(indicator_name: str, column: str) -> float | None:
        for row in records:
            if str(row.get("指标", "")).strip() == indicator_name:
                value = row.get(column)
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None
        return None

    revenue = row_value("营业总收入", latest_col)
    revenue_prior = row_value("营业总收入", prior_col) if prior_col else None
    profit = row_value("归母净利润", latest_col)
    profit_prior = row_value("归母净利润", prior_col) if prior_col else None

    return {
        "报告期": latest_date,
        "营业总收入": revenue,
        "归母净利润": profit,
        "营业总收入-同比增长": _calc_yoy_pct(revenue, revenue_prior),
        "净利润-同比增长": _calc_yoy_pct(profit, profit_prior),
        "净资产收益率": row_value("净资产收益率(ROE)", latest_col),
        "每股经营现金流量": row_value("每股现金流", latest_col),
        "source": "sina_financial_abstract",
    }


def _same_period_last_year(date_columns: list[str], latest_col: str) -> str | None:
    if len(latest_col) != 8:
        return None
    target = f"{int(latest_col[:4]) - 1}{latest_col[4:]}"
    return target if target in date_columns else None


def _calc_yoy_pct(current: float | None, prior: float | None) -> float | None:
    if current is None or prior in (None, 0):
        return None
    return round((current / prior - 1) * 100, 2)


def get_concept_board_list() -> list[dict[str, Any]]:
    ak = _get_akshare()
    records = _records(ak.stock_board_concept_name_em())
    return sorted(records, key=lambda item: float(item.get("涨跌幅") or 0), reverse=True)


def get_concept_board_members(symbol_or_name: str) -> list[dict[str, Any]]:
    ak = _get_akshare()
    return _records(ak.stock_board_concept_cons_em(symbol=symbol_or_name))


def resolve_concept_sector(symbol: str, local_override: str | None = None) -> dict[str, Any]:
    if local_override:
        return {
            "concept_sector": local_override,
            "source": "local_override",
            "candidates": [local_override],
        }

    normalized = normalize_symbol(symbol)
    board_candidates = get_concept_board_list()
    matches: list[dict[str, Any]] = []
    for board in board_candidates:
        board_name = str(board.get("板块名称", "")).strip()
        if not board_name:
            continue
        try:
            members = get_concept_board_members(board_name)
        except Exception:
            continue
        if _find_by_symbol(members, normalized):
            matches.append(
                {
                    "name": board_name,
                    "code": board.get("板块代码"),
                    "change_pct": board.get("涨跌幅"),
                }
            )
        if len(matches) >= 5:
            break

    if not matches:
        return {
            "concept_sector": "",
            "source": "unresolved",
            "candidates": [],
        }

    return {
        "concept_sector": matches[0]["name"],
        "source": "live_board_scan",
        "candidates": matches,
    }


def get_report_disclosures(symbol: str) -> list[dict[str, Any]]:
    ak = _get_akshare()
    normalized = normalize_symbol(symbol)
    matches: list[dict[str, Any]] = []
    for period in recent_report_periods():
        records = _records(_quiet_call(ak.stock_report_disclosure, market="沪深京", period=period))
        record = _find_by_symbol(records, normalized, key="股票代码")
        if record:
            record["报告期"] = period
            matches.append(record)
    return matches


def get_financial_report_notices(symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    ak = _get_akshare()
    normalized = normalize_symbol(symbol)
    categories = ["年报", "半年报", "一季报", "三季报", "业绩预告"]
    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for category in categories:
        try:
            records = _records(
                _quiet_call(
                    ak.stock_zh_a_disclosure_report_cninfo,
                    symbol=normalized,
                    market="沪深京",
                    keyword="",
                    category=category,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
        except Exception:
            continue

        for item in records:
            title = str(item.get("公告标题", ""))
            if not any(token in title for token in ("年报", "半年报", "一季报", "三季报", "业绩预告", "业绩快报")):
                continue
            dedupe_key = (
                str(item.get("代码", "")),
                title,
                str(item.get("公告时间", "")),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            enriched = dict(item)
            enriched["category"] = category
            matches.append(enriched)

    matches.sort(key=lambda item: str(item.get("公告时间", "")), reverse=True)
    return matches


def get_reduction_announcements(symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    ak = _get_akshare()
    records = _records(
        _quiet_call(
            ak.stock_zh_a_disclosure_report_cninfo,
            symbol=normalize_symbol(symbol),
            market="沪深京",
            keyword="减持",
            category="股权变动",
            start_date=start_date,
            end_date=end_date,
        )
    )
    filtered: list[dict[str, Any]] = []
    for item in records:
        title = str(item.get("公告标题", ""))
        if "减持" in title:
            filtered.append(item)
    return filtered


def get_recent_stock_news(symbol: str, limit: int = 20) -> list[dict[str, Any]]:
    ak = _get_akshare()
    records = _records(_quiet_call(ak.stock_news_em, symbol=normalize_symbol(symbol)))
    return records[:limit]


def compute_history_snapshot(history: list[dict[str, Any]]) -> dict[str, Any]:
    if not history:
        return {}
    closes = [float(row.get("收盘") or 0) for row in history if row.get("收盘") not in ("", None)]
    if not closes:
        return {}
    latest = closes[-1]
    ma5 = sum(closes[-5:]) / min(5, len(closes))
    ma10 = sum(closes[-10:]) / min(10, len(closes))
    five_day_return = 0.0
    if len(closes) >= 6 and closes[-6] != 0:
        five_day_return = (latest / closes[-6] - 1) * 100
    max_close = max(closes[-10:]) if len(closes) >= 10 else max(closes)
    min_close = min(closes[-10:]) if len(closes) >= 10 else min(closes)
    drawdown = 0.0
    if max_close:
        drawdown = (latest / max_close - 1) * 100
    resilience = 0.0
    if min_close:
        resilience = (latest / min_close - 1) * 100
    return {
        "latest_close": latest,
        "ma5": round(ma5, 2),
        "ma10": round(ma10, 2),
        "five_day_return_pct": round(five_day_return, 2),
        "ten_day_drawdown_pct": round(drawdown, 2),
        "ten_day_resilience_pct": round(resilience, 2),
    }


def compute_flow_snapshot(fund_flow_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not fund_flow_rows:
        return {}
    latest = fund_flow_rows[-1]
    recent_main = [
        float(item.get("主力净流入-净额") or 0)
        for item in fund_flow_rows
        if item.get("主力净流入-净额") not in ("", None)
    ]
    return {
        "latest_date": latest.get("日期"),
        "latest_main_net_inflow": latest.get("主力净流入-净额"),
        "latest_main_net_inflow_pct": latest.get("主力净流入-净占比"),
        "three_day_main_net_inflow": round(sum(recent_main[-3:]), 2),
    }


def compute_breakout_time(symbol: str) -> str | None:
    ak = _get_akshare()
    end_dt = datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
    start_dt = end_dt.replace(hour=9, minute=30)
    records = _records(
        ak.stock_zh_a_hist_min_em(
            symbol=normalize_symbol(symbol),
            start_date=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            end_date=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            period="1",
            adjust="",
        )
    )
    if not records:
        return None
    first_close = float(records[0].get("收盘") or 0)
    if not first_close:
        return None
    threshold = first_close * 1.02
    for row in records:
        close_price = float(row.get("收盘") or 0)
        if close_price >= threshold:
            return str(row.get("时间"))
    return None
