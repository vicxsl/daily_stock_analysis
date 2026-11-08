# -*- coding: utf-8 -*-
"""OTC fund identity and NAV lookup for portfolio valuation.

路由原则（2026-10-09 个人投研系统 T1）：
- 场外基金（OTC fund）不得走股票实时行情接口；估值使用基金单位净值（NAV），
  并保留净值日期（provider_time = nav_date, kind = "nav"）。
- 身份判定以 akshare 开放式基金名单（fund_name_em）为准，不靠代码前缀猜测。
- 6 位代码空间里场外基金与深市股票（000/001/002/003 开头）存在真实重叠
  （如 000001 既是平安银行也是华夏成长）。这类代码按 "ambiguous" 处理：
  先走股票行情，失败才回退 NAV，最终估值披露实际命中来源；确定性 OTC
  代码（不在上市号段，如 016xxx/024xxx）直接走 NAV，股票接口调用次数为 0。
- 名单与单只基金净值历史均做进程内缓存，避免组合估值反复拉取。
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional, Set

logger = logging.getLogger(__name__)

_FUND_NAME_CACHE_TTL_SECONDS = 24 * 60 * 60
_FUND_NAME_NEGATIVE_CACHE_TTL_SECONDS = 10 * 60
_NAV_CACHE_TTL_SECONDS = 30 * 60
_NAV_CACHE_MAX_ENTRIES = 256

# A-share listed code ranges where an OTC fund code can genuinely collide with
# a listed STOCK (e.g. 000001 平安银行 vs 华夏成长). These drive the
# "ambiguous" stock-first-then-NAV routing.
_STOCK_COLLISION_PREFIXES = (
    "600", "601", "603", "605", "688", "689",  # Shanghai stocks
    "000", "001", "002", "003", "300", "301",  # Shenzhen stocks
    "43", "83", "87", "92",                    # Beijing
)

# Exchange-traded fund ranges (ETF/LOF). The OTC registry (fund_name_em)
# legitimately contains these codes because many ETFs/LOFs are open-end too,
# but their valuation is the exchange quote — a registry hit here must NOT
# route to NAV (2026-10-09 live incident: pre-market 159632 fell back to the
# OTC twin's NAV when the quote source returned nothing).
_ETF_LOF_PREFIXES = (
    "159",                                      # Shenzhen ETF
    "510", "511", "512", "513", "514", "515", "516", "517", "518",  # SH ETF
    "560", "561", "562", "563", "564", "565", "566", "567", "568",  # SH ETF
    "580", "581", "582", "583", "584", "585", "586", "587", "588", "589",  # SH ETF
    "501", "502", "503", "506", "508",         # Shanghai LOF/FOF
    "160", "161", "162", "163", "164", "165", "166", "167", "168", "169",  # SZ LOF
)

_LISTED_CN_PREFIXES = _STOCK_COLLISION_PREFIXES + _ETF_LOF_PREFIXES

_fund_name_lock = threading.Lock()
_fund_name_cache: Dict[str, Any] = {"codes": None, "fetched_at": 0.0}

_nav_lock = threading.Lock()
_nav_cache: Dict[str, Dict[str, Any]] = {}


def _strip_exchange_suffix(symbol: str) -> str:
    raw = (symbol or "").strip().upper()
    for suffix in (".SH", ".SZ", ".SS", ".BJ"):
        if raw.endswith(suffix):
            return raw[: -len(suffix)]
    if len(raw) == 8 and raw[:2] in {"SH", "SZ", "BJ"} and raw[2:].isdigit():
        return raw[2:]
    return raw


def _has_exchange_prefix(symbol: str) -> bool:
    raw = (symbol or "").strip().upper()
    if len(raw) == 8 and raw[:2] in {"SH", "SZ", "BJ"} and raw[2:].isdigit():
        return True
    for prefix in ("SH", "SZ", "BJ"):
        if "/" in raw and raw.split("/", 1)[0] == prefix:
            return True
    return False


def _load_otc_fund_codes() -> Optional[Set[str]]:
    """Load the OTC (open-end) fund code set, cached for 24h. None when unavailable."""
    now = time.time()
    with _fund_name_lock:
        if _fund_name_cache["codes"] is not None and now - _fund_name_cache["fetched_at"] < _FUND_NAME_CACHE_TTL_SECONDS:
            return _fund_name_cache["codes"]
        if _fund_name_cache["codes"] is None and (now - _fund_name_cache["fetched_at"]) < _FUND_NAME_NEGATIVE_CACHE_TTL_SECONDS:
            return None
    try:
        import akshare as ak

        frame = ak.fund_name_em()
        codes = set()
        for value in frame["基金代码"].astype(str):
            value = value.strip()
            if value:
                codes.add(value)
    except Exception as exc:  # noqa: BLE001 - external network failure must not break valuation
        logger.warning("[fund_nav] fund_name_em unavailable, OTC routing disabled: %s", exc)
        with _fund_name_lock:
            _fund_name_cache["fetched_at"] = now
        return None
    with _fund_name_lock:
        _fund_name_cache["codes"] = codes
        _fund_name_cache["fetched_at"] = now
    return codes


def classify_cn_symbol(symbol: str) -> str:
    """Classify one CN symbol for valuation routing.

    Returns one of:
      "listed"    — exchange-prefixed or bare code outside the OTC registry
                    (or in a listed range without a registry entry): stock path.
      "otc"       — in the OTC registry and NOT inside a listed CN code range:
                    NAV path, stock quote APIs must not be called.
      "ambiguous" — in the OTC registry AND inside a listed CN code range
                    (e.g. 000001/003096): stock quote first, NAV fallback.
      "unknown"   — registry unavailable and the bare code is outside every
                    known listed range: stock path, time semantics as disclosed
                    by the provider; never claim NAV.
    """
    raw = _strip_exchange_suffix(symbol)
    if _has_exchange_prefix(symbol):
        return "listed"
    if not raw or not raw.isdigit() or len(raw) != 6:
        return "listed"
    codes = _load_otc_fund_codes()
    in_registry = codes is not None and raw in codes
    in_stock_collision = raw.startswith(_STOCK_COLLISION_PREFIXES)
    in_etf_lof_range = raw.startswith(_ETF_LOF_PREFIXES)
    if in_registry and in_etf_lof_range and not in_stock_collision:
        # ETF/LOF twin in the registry: exchange-traded instrument wins; NAV
        # must never substitute for the exchange quote.
        return "listed"
    if in_registry and in_stock_collision:
        return "ambiguous"
    if in_registry and not in_stock_collision:
        return "otc"
    if codes is None and not raw.startswith(_LISTED_CN_PREFIXES):
        return "unknown"
    return "listed"


def is_otc_fund(symbol: str) -> Optional[bool]:
    """Compatibility helper: True only for definite OTC routing; None = unknown."""
    kind = classify_cn_symbol(symbol)
    if kind == "otc":
        return True
    if kind == "listed":
        return False
    return None


def fetch_latest_nav(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch the latest published unit NAV for one OTC fund.

    Returns {"nav": float, "nav_date": date} or None on failure.
    Result is cached briefly (30 min) because NAV publishes once per day.
    """
    raw = _strip_exchange_suffix(symbol)
    now = time.time()
    with _nav_lock:
        hit = _nav_cache.get(raw)
        if hit and now - hit["fetched_at"] < _NAV_CACHE_TTL_SECONDS:
            return hit["value"]
    value: Optional[Dict[str, Any]] = None
    try:
        import akshare as ak

        frame = ak.fund_open_fund_info_em(symbol=raw, indicator="单位净值走势")
        if frame is not None and not frame.empty:
            last = frame.iloc[-1]
            nav = float(last["单位净值"])
            nav_date = last["净值日期"]
            if nav > 0 and nav_date is not None:
                value = {"nav": nav, "nav_date": nav_date}
    except Exception as exc:  # noqa: BLE001 - degrade to missing price, never crash replay
        logger.warning("[fund_nav] NAV fetch failed for %s: %s", raw, exc)
    with _nav_lock:
        if len(_nav_cache) >= _NAV_CACHE_MAX_ENTRIES:
            oldest = min(_nav_cache.items(), key=lambda item: item[1]["fetched_at"])[0]
            _nav_cache.pop(oldest, None)
        _nav_cache[raw] = {"fetched_at": now, "value": value}
    return value
