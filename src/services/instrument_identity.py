# -*- coding: utf-8 -*-
"""Confirmed instrument identity registry for valuation routing (R3/F06).

Routing rule (fixed order):
1. CONFIRMED identity (this registry) — otc_fund -> NAV; stock/etf/index ->
   exchange quote. Confirmation happens at baseline time (asset_kind) or via
   the controlled identity endpoint; it is instrument metadata, not an account
   fact.
2. Heuristic (src.services.fund_nav.classify_cn_symbol) — deterministic OTC
   codes go NAV, ETF/LOF registry hits stay listed; "ambiguous" (real
   stock/fund code collision such as 000001/003096) or "unknown" (registry
   unavailable and outside listed ranges) -> valuation PAUSED for that symbol.
   A stock-quote failure must never be answered with the same-code fund NAV
   and vice versa.

Alias normalization: 600887 / 600887.SH / 600887.SS / SH600887 all resolve to
one bare-code registry row.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import select

from src.services.fund_nav import classify_cn_symbol
from src.storage import DatabaseManager, PortfolioInstrumentIdentity

logger = logging.getLogger(__name__)

VALID_IDENTITY_KINDS = {"stock", "etf", "index", "otc_fund"}

# Valuation routes an instrument may resolve to.
ROUTE_EXCHANGE = "exchange"
ROUTE_NAV = "nav"
ROUTE_PAUSED = "paused_identity"


def normalize_identity_symbol(symbol: str) -> str:
    raw = (symbol or "").strip().upper()
    for suffix in (".SH", ".SZ", ".SS", ".BJ", ".OF"):
        if raw.endswith(suffix):
            return raw[: -len(suffix)]
    if len(raw) == 8 and raw[:2] in {"SH", "SZ", "BJ"} and raw[2:].isdigit():
        return raw[2:]
    return raw


def route_for_symbol(symbol: str, confirmed_kind: Optional[str], classify=None) -> str:
    """Resolve the valuation route for one symbol.

    confirmed_kind: registry hit (already normalized) or None.
    classify: classifier callable (defaults to fund_nav.classify_cn_symbol);
    callers pass their own module reference so test patches keep working.
    Returns ROUTE_EXCHANGE / ROUTE_NAV / ROUTE_PAUSED.
    """
    if confirmed_kind == "otc_fund":
        return ROUTE_NAV
    if confirmed_kind in ("stock", "etf", "index"):
        return ROUTE_EXCHANGE
    heuristic = (classify or classify_cn_symbol)(symbol)
    if heuristic == "otc":
        return ROUTE_NAV
    if heuristic == "listed":
        return ROUTE_EXCHANGE
    # ambiguous (stock/fund code collision) or unknown (registry unavailable):
    # identity is NOT confirmed — pause this symbol, never guess by trying the
    # other asset class (F06).
    return ROUTE_PAUSED


class InstrumentIdentityService:
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def get(self, symbol: str) -> Optional[dict]:
        key = normalize_identity_symbol(symbol)
        with self.db.get_session() as session:
            row = session.execute(
                select(PortfolioInstrumentIdentity).where(
                    PortfolioInstrumentIdentity.symbol == key
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            return {
                "symbol": row.symbol,
                "kind": row.kind,
                "market": row.market,
                "currency": row.currency,
                "source": row.source,
                "confirmed_by": row.confirmed_by,
                "confirmed_at": row.confirmed_at.isoformat() if row.confirmed_at else None,
                "note": row.note,
            }

    def upsert(
        self,
        *,
        symbol: str,
        kind: str,
        market: str = "cn",
        currency: str = "CNY",
        source: Optional[str] = None,
        confirmed_by: Optional[str] = None,
        note: Optional[str] = None,
    ) -> dict:
        kind_norm = (kind or "").strip().lower()
        if kind_norm not in VALID_IDENTITY_KINDS:
            raise ValueError(f"kind must be one of {sorted(VALID_IDENTITY_KINDS)}")
        key = normalize_identity_symbol(symbol)
        if not key:
            raise ValueError("symbol is required")
        with self.db.get_session() as session:
            row = session.execute(
                select(PortfolioInstrumentIdentity).where(
                    PortfolioInstrumentIdentity.symbol == key
                )
            ).scalar_one_or_none()
            if row is None:
                row = PortfolioInstrumentIdentity(symbol=key)
                session.add(row)
            row.kind = kind_norm
            row.market = market
            row.currency = currency
            row.source = (source or "manual")[:32]
            row.confirmed_by = (confirmed_by or "")[:64] or None
            row.note = (note or "")[:255] or None
            from datetime import datetime as _dt

            row.confirmed_at = _dt.now()
            session.commit()
            return {"symbol": key, "kind": kind_norm, "source": row.source}

    def confirmed_kind(self, symbol: str) -> Optional[str]:
        entry = self.get(symbol)
        return entry["kind"] if entry else None
