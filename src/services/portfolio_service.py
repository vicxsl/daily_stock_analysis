# -*- coding: utf-8 -*-
"""Portfolio service for P0 account/events/snapshot workflow."""

from __future__ import annotations

import json
import hashlib
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from data_provider.base import canonical_stock_code, normalize_stock_code
from data_provider.realtime_types import upstream_group_for
from src.config import get_config
from src.core.trading_calendar import is_market_open
from src.repositories.portfolio_repo import (
    DuplicateTradeDedupHashError,
    DuplicateTradeUidError,
    PortfolioBusyError as RepoPortfolioBusyError,
    PortfolioRepository,
)
from src.services.fund_nav import classify_cn_symbol, fetch_latest_nav

logger = logging.getLogger(__name__)

PortfolioBusyError = RepoPortfolioBusyError

try:
    import yfinance as yf
except Exception:  # pragma: no cover - optional dependency path
    yf = None

EPS = 1e-8
VALID_MARKETS = {"cn", "hk", "us", "jp", "kr", "tw"}
PARTIAL_VALUATION_MARKETS = {"jp", "kr", "tw"}
VALID_COST_METHODS = {"fifo", "avg"}
VALID_SIDES = {"buy", "sell"}
# Event provenance on PortfolioTrade: real trades move cash; baseline/init
# events establish positions without cash movement and are excluded from
# real-trade statistics (D12).
VALID_EVENT_TYPES = {"trade", "baseline", "init"}
BASELINE_LIKE_EVENT_TYPES = {"baseline", "init"}
VALID_CASH_DIRECTIONS = {"in", "out"}
VALID_CORPORATE_ACTIONS = {"cash_dividend", "split_adjustment"}
PORTFOLIO_FX_REFRESH_DISABLED_REASON = "portfolio_fx_update_disabled"
PORTFOLIO_REALTIME_QUOTE_MAX_WORKERS = 4

# Intraday realtime quotes older than this are flagged stale while the CN
# session is live. Quotes dated on a previous trading day while today's CN
# session has already opened are always stale. Off-session latest closes are
# NOT stale (they are the most recent official price) — their price_date is
# exposed honestly instead (T02).
REALTIME_STALENESS_MAX_AGE_SECONDS = 60 * 60
# OTC fund NAV publishes with a normal T-1 (or longer for QDII) delay; only
# flag NAV stale when the latest published NAV date lags the as-of date by
# more than this many calendar days.
NAV_STALENESS_MAX_LAG_DAYS = 7

_CN_TZ = timezone(timedelta(hours=8))


def _strip_symbol_suffix(symbol: str) -> str:
    """Normalize a CN symbol to its bare 6-digit code for fund lookups."""
    raw = (symbol or "").strip().upper()
    for suffix in (".SH", ".SZ", ".SS", ".BJ", ".OF"):
        if raw.endswith(suffix):
            return raw[: -len(suffix)]
    if len(raw) == 8 and raw[:2] in {"SH", "SZ", "BJ"} and raw[2:].isdigit():
        return raw[2:]
    return raw


def _fetch_stock_realtime_quote(symbol: str, fetched_at: str) -> Optional[Dict[str, Any]]:
    """Fetch one realtime quote through the stock DataFetcherManager; None when unusable."""
    try:
        from data_provider.base import DataFetcherManager

        fetcher_manager = DataFetcherManager()
        quote = fetcher_manager.get_realtime_quote(symbol, log_final_failure=False)
    except Exception as exc:
        logger.warning("Failed to fetch realtime portfolio price: %s", exc)
        return None

    if quote is None:
        return None

    price = getattr(quote, "price", None)
    try:
        numeric_price = float(price)
    except (TypeError, ValueError):
        return None

    if numeric_price <= 0:
        return None

    source = getattr(quote, "source", None)
    provider = getattr(source, "value", None) or (str(source) if source is not None else None)
    provider_time = getattr(quote, "provider_timestamp", None)
    if provider_time is not None and not isinstance(provider_time, str):
        provider_time = str(provider_time)
    quote_fetched_at = getattr(quote, "fetched_at", None)
    if not isinstance(quote_fetched_at, str):
        quote_fetched_at = fetched_at
    return {
        "price": numeric_price,
        "provider": provider,
        "upstream_group": upstream_group_for(provider),
        "kind": "realtime",
        "provider_time": provider_time,
        "fetched_at": quote_fetched_at,
    }


def _portfolio_limitations_for_market(market: str) -> List[str]:
    """Return explicit snapshot limitations for markets with partial valuation semantics."""

    if market not in PARTIAL_VALUATION_MARKETS:
        return []
    return [
        "realtime_quote_best_effort",
        "fx_and_cost_basis_partial",
        "sector_and_risk_metrics_limited",
    ]


def _merge_portfolio_limitations(*groups: Iterable[str]) -> List[str]:
    merged: List[str] = []
    seen: Set[str] = set()
    for group in groups:
        for item in group:
            if item and item not in seen:
                seen.add(item)
                merged.append(item)
    return merged


class PortfolioConflictError(Exception):
    """Raised when request conflicts with existing portfolio state."""


class PortfolioOversellError(ValueError):
    """Raised when a sell would exceed the available position quantity."""

    def __init__(
        self,
        *,
        symbol: str,
        trade_date: Optional[date],
        requested_quantity: float,
        available_quantity: float,
    ) -> None:
        self.symbol = symbol
        self.trade_date = trade_date
        self.requested_quantity = float(requested_quantity)
        self.available_quantity = max(0.0, float(available_quantity))
        date_hint = f" on {trade_date.isoformat()}" if trade_date is not None else ""
        super().__init__(
            "Oversell detected for "
            f"{symbol}{date_hint}: requested={round(self.requested_quantity, 8)}, "
            f"available={round(self.available_quantity, 8)}"
        )


@dataclass
class _AvgState:
    quantity: float = 0.0
    total_cost: float = 0.0


@dataclass(frozen=True)
class _ResolvedPositionPrice:
    price: float
    source: str
    price_date: Optional[date]
    is_stale: bool
    is_available: bool
    provider: Optional[str] = None
    # T1 price-time semantics: 'realtime' (intraday quote), 'nav' (fund unit
    # NAV, price_date == nav date), or 'close' (historical daily close).
    price_kind: str = "close"
    # Provider-side quote timestamp (ISO string) or NAV date; None means the
    # provider did not disclose a time — consumers must not claim real-time.
    provider_time: Optional[str] = None
    fetched_at: Optional[str] = None
    # 'valid' = provider_time present and usable; 'unknown' = provider time
    # missing (fetch time only); price_date then reflects the fetch date and
    # downstream reports must mark the valuation as time-unknown.
    time_quality: str = "valid"


class PortfolioService:
    """Business logic for account CRUD, event writes, and snapshot replay."""

    def __init__(self, repo: Optional[PortfolioRepository] = None):
        self.repo = repo or PortfolioRepository()

    # ------------------------------------------------------------------
    # Account CRUD
    # ------------------------------------------------------------------
    def create_account(
        self,
        *,
        name: str,
        broker: Optional[str],
        market: str,
        base_currency: str,
        owner_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        name_norm = (name or "").strip()
        if not name_norm:
            raise ValueError("name is required")
        market_norm = self._normalize_market(market)
        base_currency_norm = self._normalize_currency(base_currency)
        row = self.repo.create_account(
            name=name_norm,
            broker=(broker or "").strip() or None,
            market=market_norm,
            base_currency=base_currency_norm,
            owner_id=(owner_id or "").strip() or None,
        )
        return self._account_to_dict(row)

    def list_accounts(self, include_inactive: bool = False) -> List[Dict[str, Any]]:
        rows = self.repo.list_accounts(include_inactive=include_inactive)
        return [self._account_to_dict(r) for r in rows]

    def update_account(
        self,
        account_id: int,
        *,
        name: Optional[str] = None,
        broker: Optional[str] = None,
        market: Optional[str] = None,
        base_currency: Optional[str] = None,
        owner_id: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        fields: Dict[str, Any] = {}
        if name is not None:
            name_norm = name.strip()
            if not name_norm:
                raise ValueError("name is required")
            fields["name"] = name_norm
        if broker is not None:
            fields["broker"] = broker.strip() or None
        if market is not None:
            fields["market"] = self._normalize_market(market)
        if base_currency is not None:
            fields["base_currency"] = self._normalize_currency(base_currency)
        if owner_id is not None:
            fields["owner_id"] = owner_id.strip() or None
        if is_active is not None:
            fields["is_active"] = bool(is_active)
        if not fields:
            raise ValueError("No fields provided for update")

        row = self.repo.update_account(account_id, fields)
        if row is None:
            return None
        return self._account_to_dict(row)

    def deactivate_account(self, account_id: int) -> bool:
        return self.repo.deactivate_account(account_id)

    # ------------------------------------------------------------------
    # Event writes
    # ------------------------------------------------------------------
    def record_trade(
        self,
        *,
        account_id: int,
        symbol: str,
        trade_date: date,
        side: str,
        quantity: float,
        price: float,
        fee: float = 0.0,
        tax: float = 0.0,
        market: Optional[str] = None,
        currency: Optional[str] = None,
        trade_uid: Optional[str] = None,
        dedup_hash: Optional[str] = None,
        note: Optional[str] = None,
        event_type: str = "trade",
    ) -> Dict[str, Any]:
        side_norm = (side or "").strip().lower()
        if side_norm not in VALID_SIDES:
            raise ValueError("side must be buy or sell")
        event_type_norm = (event_type or "trade").strip().lower()
        if event_type_norm not in VALID_EVENT_TYPES:
            raise ValueError("event_type must be trade, baseline or init")
        if event_type_norm in BASELINE_LIKE_EVENT_TYPES and side_norm != "buy":
            raise ValueError("baseline/init events must use side=buy")
        if quantity <= 0 or price <= 0:
            raise ValueError("quantity and price must be > 0")
        if fee < 0 or tax < 0:
            raise ValueError("fee and tax must be >= 0")
        symbol_norm = self._normalize_symbol_for_storage(symbol)
        if not symbol_norm:
            raise ValueError("symbol is required")
        trade_uid_norm = (trade_uid or "").strip() or None
        dedup_hash_norm = (dedup_hash or "").strip() or None
        try:
            with self.repo.portfolio_write_session() as session:
                account = self._require_active_account_in_session(session=session, account_id=account_id)
                market_norm = self._normalize_market(market or account.market)
                currency_norm = self._normalize_currency(currency or self._default_currency_for_market(market_norm))
                if event_type_norm == "trade":
                    # R2: real events cannot share a baseline date (undecidable order).
                    self._reject_same_day_baseline(
                        session=session, account_id=account_id, day=trade_date
                    )
                self._validate_trade_identity(
                    account_id=account_id,
                    trade_uid=trade_uid_norm,
                    dedup_hash=dedup_hash_norm,
                    session=session,
                    )
                if side_norm == "sell":
                    self._validate_sell_quantity(
                        account_id=account_id,
                        symbol=symbol,
                        market=market_norm,
                        currency=currency_norm,
                        trade_date=trade_date,
                        quantity=float(quantity),
                        session=session,
                    )
                row = self.repo.add_trade_in_session(
                    session=session,
                    account_id=account_id,
                    trade_uid=trade_uid_norm,
                    symbol=symbol_norm,
                    market=market_norm,
                    currency=currency_norm,
                    trade_date=trade_date,
                    side=side_norm,
                    quantity=float(quantity),
                    price=float(price),
                    fee=float(fee),
                    tax=float(tax),
                    note=(note or "").strip() or None,
                    dedup_hash=dedup_hash_norm,
                    event_type=event_type_norm,
                )
                return {"id": int(row.id)}
        except (DuplicateTradeUidError, DuplicateTradeDedupHashError) as exc:
            raise PortfolioConflictError(str(exc)) from exc

    def record_baseline(
        self,
        *,
        account_id: int,
        baseline_date: date,
        positions: List[Dict[str, Any]],
        cash: Optional[float] = None,
        cash_currency: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create one confirmed account baseline (D12; R2 semantics 2026-10-09).

        A baseline is the confirmed whole-account position/cash STARTING POINT
        at baseline_date — not additive buys and not a cash inflow:

        - The active baseline (latest baseline_date <= as_of) REPLACES all
          earlier history during replay: old init rows, older baselines and
          pre-baseline cash/corp events are superseded (rows stay in the DB
          for history queries). A symbol missing from the baseline means "no
          position at that time"; no sell is fabricated.
        - positions may be empty (confirmed empty account) — but then cash must
          be declared, otherwise nothing anchors the baseline.
        - cost_per_unit None/0 = cost unknown: the position is kept, its PnL is
          not computed.
        - cash None = unknown (never substituted with 0); cash 0 = confirmed
          zero cash; cash > 0 = confirmed amount.
        - Same-day boundary: a baseline cannot share its date with real events
          (trades / non-baseline cash / corporate actions) — records are
          date-granular so ordering would be undecidable. Conflicts raise
          PortfolioConflictError for one manual verification, never a guess.
        - Idempotency is whole-baseline: re-submitting the identical baseline
          (same position set + same cash) returns the existing records;
          different content for the same date conflicts. Re-confirmation uses
          a NEW baseline_date.
        """
        normalized: List[Dict[str, Any]] = []
        for item in positions:
            symbol_raw = str(item.get("symbol") or "").strip()
            if not symbol_raw:
                raise ValueError("each baseline position needs symbol")
            quantity = float(item.get("quantity") or 0.0)
            if quantity <= 0:
                raise ValueError("each baseline position needs quantity > 0")
            cost_raw = item.get("cost_per_unit")
            cost_unknown = cost_raw is None or float(cost_raw) <= 0
            unit_cost = 0.0 if cost_unknown else float(cost_raw)
            normalized.append(
                {
                    "symbol": symbol_raw,
                    "quantity": quantity,
                    "cost_per_unit": unit_cost,
                    "cost_unknown": cost_unknown,
                    "currency": item.get("currency"),
                    "asset_kind": str(item.get("asset_kind") or "").strip().lower(),
                }
            )
        if not normalized and cash is None:
            raise ValueError(
                "empty baseline must declare cash (0 for confirmed zero); "
                "otherwise there is nothing to confirm"
            )
        if cash is not None and float(cash) < 0:
            raise ValueError("cash must be >= 0 (or None for unknown)")

        results: List[Dict[str, Any]] = []
        with self.repo.portfolio_write_session() as session:
            account = self._require_active_account_in_session(session=session, account_id=account_id)
            market_norm = self._normalize_market(account.market)

            self._reject_same_day_real_events(
                session=session, account_id=account_id, day=baseline_date
            )

            existing_same_day: Dict[str, Any] = {}
            for row in self.repo.list_trades_in_session(
                session=session, account_id=account_id, as_of=baseline_date
            ):
                record_type = (getattr(row, "event_type", None) or "trade").strip().lower()
                if row.trade_date == baseline_date and record_type == "baseline":
                    existing_same_day[self._normalize_symbol_for_storage(row.symbol)] = row

            cash_note = f"baseline-cash:{account_id}:{baseline_date.isoformat()}"
            existing_cash = self.repo.find_cash_ledger_by_note(account_id, cash_note, session=session)

            if existing_same_day or existing_cash is not None:
                # Whole-baseline idempotency: identical content returns the
                # original records; anything else conflicts (use a new date).
                stored = {
                    symbol: (float(row.quantity), float(row.price or 0.0))
                    for symbol, row in existing_same_day.items()
                }
                submitted: Dict[str, Tuple[float, float]] = {}
                for item in normalized:
                    symbol_norm = self._normalize_symbol_for_storage(item["symbol"])
                    if not symbol_norm:
                        raise ValueError(f"baseline symbol invalid: {item['symbol']}")
                    if symbol_norm in submitted:
                        raise ValueError(f"duplicate baseline symbol: {symbol_norm}")
                    submitted[symbol_norm] = (item["quantity"], item["cost_per_unit"])
                stored_cash = float(existing_cash.amount) if existing_cash is not None else None
                cash_matches = (cash is None and stored_cash is None) or (
                    cash is not None
                    and stored_cash is not None
                    and abs(stored_cash - float(cash)) <= EPS
                )
                if stored == submitted and cash_matches:
                    return {
                        "account_id": account_id,
                        "baseline_date": baseline_date.isoformat(),
                        "positions": [
                            {"symbol": symbol, "id": int(row.id), "duplicate": True}
                            for symbol, row in existing_same_day.items()
                        ],
                        "cash": (
                            {"amount": stored_cash, "duplicate": True}
                            if stored_cash is not None
                            else None
                        ),
                        "duplicate": True,
                    }
                raise PortfolioConflictError(
                    f"account {account_id} already has a baseline on "
                    f"{baseline_date.isoformat()} with different content; "
                    "re-confirmation requires a new baseline_date"
                )

            for item in normalized:
                symbol_norm = self._normalize_symbol_for_storage(item["symbol"])
                if not symbol_norm:
                    raise ValueError(f"baseline symbol invalid: {item['symbol']}")
                trade_uid = f"baseline:{account_id}:{baseline_date.isoformat()}:{symbol_norm}"
                row = self.repo.add_trade_in_session(
                    session=session,
                    account_id=account_id,
                    trade_uid=trade_uid,
                    symbol=symbol_norm,
                    market=market_norm,
                    currency=self._normalize_currency(
                        item.get("currency") or self._default_currency_for_market(market_norm)
                    ),
                    trade_date=baseline_date,
                    side="buy",
                    quantity=item["quantity"],
                    price=item["cost_per_unit"],
                    fee=0.0,
                    tax=0.0,
                    note=(note or "confirmed account baseline").strip()[:255],
                    dedup_hash=None,
                    event_type="baseline",
                )
                results.append({"symbol": symbol_norm, "id": int(row.id), "duplicate": False})
                # R3: a baseline-declared asset_kind confirms the instrument
                # identity used for valuation routing (metadata, not a fact row).
                asset_kind_raw = str(item.get("asset_kind") or "").strip().lower()
                if asset_kind_raw:
                    from src.services.instrument_identity import InstrumentIdentityService

                    InstrumentIdentityService().upsert(
                        symbol=symbol_norm,
                        kind=asset_kind_raw,
                        market=market_norm,
                        source=f"baseline:{baseline_date.isoformat()}",
                        note="confirmed at account baseline",
                    )

            cash_result: Optional[Dict[str, Any]] = None
            if cash is not None and float(cash) >= 0:
                amount = float(cash)
                currency = self._normalize_currency(cash_currency or account.base_currency)
                self.repo.add_cash_ledger_in_session(
                    session=session,
                    account_id=account_id,
                    event_date=baseline_date,
                    direction="in",
                    amount=amount,
                    currency=currency,
                    note=cash_note,
                )
                cash_result = {"amount": amount, "currency": currency, "duplicate": False}

        return {
            "account_id": account_id,
            "baseline_date": baseline_date.isoformat(),
            "positions": results,
            "cash": cash_result,
            "duplicate": False,
        }

    def _reject_same_day_real_events(self, *, session: Any, account_id: int, day: date) -> None:
        """A baseline date must not carry real events (date-granular ordering is undecidable)."""
        for row in self.repo.list_trades_in_session(session=session, account_id=account_id, as_of=day):
            record_type = (getattr(row, "event_type", None) or "trade").strip().lower()
            if row.trade_date == day and record_type == "trade":
                raise PortfolioConflictError(
                    f"account {account_id} has real trade(s) on {day.isoformat()}; "
                    "a baseline cannot share a date with real events — verify the "
                    "date or choose another baseline_date"
                )
        for row in self.repo.list_cash_ledger_in_session(session=session, account_id=account_id, as_of=day):
            if row.event_date == day and not str(row.note or "").startswith("baseline-cash:"):
                raise PortfolioConflictError(
                    f"account {account_id} has non-baseline cash ledger row(s) on "
                    f"{day.isoformat()}; confirm the baseline on a day without cash events"
                )
        for row in self.repo.list_corporate_actions_in_session(session=session, account_id=account_id, as_of=day):
            if row.effective_date == day:
                raise PortfolioConflictError(
                    f"account {account_id} has corporate action(s) on {day.isoformat()}; "
                    "a baseline cannot share a date with corporate actions"
                )

    def _reject_same_day_baseline(self, *, session: Any, account_id: int, day: date) -> None:
        """Real events cannot land on a date that already carries a baseline."""
        for row in self.repo.list_trades_in_session(session=session, account_id=account_id, as_of=day):
            record_type = (getattr(row, "event_type", None) or "trade").strip().lower()
            if row.trade_date == day and record_type == "baseline":
                raise PortfolioConflictError(
                    f"account {account_id} has a confirmed baseline on {day.isoformat()}; "
                    "real events cannot share the baseline date (ordering undecidable)"
                )

    def record_cash_ledger(
        self,
        *,
        account_id: int,
        event_date: date,
        direction: str,
        amount: float,
        currency: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        direction_norm = (direction or "").strip().lower()
        if direction_norm not in VALID_CASH_DIRECTIONS:
            raise ValueError("direction must be in or out")
        if amount <= 0:
            raise ValueError("amount must be > 0")
        with self.repo.portfolio_write_session() as session:
            account = self._require_active_account_in_session(session=session, account_id=account_id)
            if not (note or "").strip().startswith("baseline-cash:"):
                self._reject_same_day_baseline(session=session, account_id=account_id, day=event_date)
            currency_norm = self._normalize_currency(currency or account.base_currency)
            row = self.repo.add_cash_ledger_in_session(
                session=session,
                account_id=account_id,
                event_date=event_date,
                direction=direction_norm,
                amount=float(amount),
                currency=currency_norm,
                note=(note or "").strip() or None,
            )
            return {"id": int(row.id)}

    def record_corporate_action(
        self,
        *,
        account_id: int,
        symbol: str,
        effective_date: date,
        action_type: str,
        market: Optional[str] = None,
        currency: Optional[str] = None,
        cash_dividend_per_share: Optional[float] = None,
        split_ratio: Optional[float] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        action_type_norm = (action_type or "").strip().lower()
        if action_type_norm not in VALID_CORPORATE_ACTIONS:
            raise ValueError("action_type must be cash_dividend or split_adjustment")

        if action_type_norm == "cash_dividend":
            if cash_dividend_per_share is None or cash_dividend_per_share < 0:
                raise ValueError("cash_dividend_per_share must be >= 0 for cash_dividend")
        if action_type_norm == "split_adjustment":
            if split_ratio is None or split_ratio <= 0:
                raise ValueError("split_ratio must be > 0 for split_adjustment")
        with self.repo.portfolio_write_session() as session:
            account = self._require_active_account_in_session(session=session, account_id=account_id)
            market_norm = self._normalize_market(market or account.market)
            currency_norm = self._normalize_currency(currency or self._default_currency_for_market(market_norm))
            symbol_norm = self._normalize_symbol_for_storage(symbol)
            if not symbol_norm:
                raise ValueError("symbol is required")
            row = self.repo.add_corporate_action_in_session(
                session=session,
                account_id=account_id,
                symbol=symbol_norm,
                market=market_norm,
                currency=currency_norm,
                effective_date=effective_date,
                action_type=action_type_norm,
                cash_dividend_per_share=cash_dividend_per_share,
                split_ratio=split_ratio,
                note=(note or "").strip() or None,
            )
            return {"id": int(row.id)}

    def delete_trade_event(self, trade_id: int) -> bool:
        with self.repo.portfolio_write_session() as session:
            return self.repo.delete_trade_in_session(session=session, trade_id=trade_id)

    def delete_cash_ledger_event(self, entry_id: int) -> bool:
        with self.repo.portfolio_write_session() as session:
            return self.repo.delete_cash_ledger_in_session(session=session, entry_id=entry_id)

    def delete_corporate_action_event(self, action_id: int) -> bool:
        with self.repo.portfolio_write_session() as session:
            return self.repo.delete_corporate_action_in_session(session=session, action_id=action_id)

    def list_trade_events(
        self,
        *,
        account_id: Optional[int] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        symbol: Optional[str] = None,
        side: Optional[str] = None,
        event_type: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        if account_id is not None:
            self._require_active_account(account_id)
        page, page_size = self._validate_paging(page=page, page_size=page_size)
        if date_from is not None and date_to is not None and date_from > date_to:
            raise ValueError("date_from must be <= date_to")

        symbol_filters: Optional[List[str]] = None
        if symbol is not None and symbol.strip():
            symbol_filters = self._build_symbol_filter_values(symbol)
            if not symbol_filters:
                raise ValueError("symbol is invalid")

        side_norm: Optional[str] = None
        if side is not None and side.strip():
            side_norm = side.strip().lower()
            if side_norm not in VALID_SIDES:
                raise ValueError("side must be buy or sell")

        event_type_norm: Optional[str] = None
        if event_type is not None and event_type.strip():
            event_type_norm = event_type.strip().lower()
            if event_type_norm not in VALID_EVENT_TYPES:
                raise ValueError("event_type must be trade, baseline or init")

        rows, total = self.repo.query_trades(
            account_id=account_id,
            date_from=date_from,
            date_to=date_to,
            symbols=symbol_filters,
            side=side_norm,
            page=page,
            page_size=page_size,
            event_type=event_type_norm,
        )
        return {
            "items": [self._trade_row_to_dict(row) for row in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    def list_cash_ledger_events(
        self,
        *,
        account_id: Optional[int] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        direction: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        if account_id is not None:
            self._require_active_account(account_id)
        page, page_size = self._validate_paging(page=page, page_size=page_size)
        if date_from is not None and date_to is not None and date_from > date_to:
            raise ValueError("date_from must be <= date_to")

        direction_norm: Optional[str] = None
        if direction is not None and direction.strip():
            direction_norm = direction.strip().lower()
            if direction_norm not in VALID_CASH_DIRECTIONS:
                raise ValueError("direction must be in or out")

        rows, total = self.repo.query_cash_ledger(
            account_id=account_id,
            date_from=date_from,
            date_to=date_to,
            direction=direction_norm,
            page=page,
            page_size=page_size,
        )
        return {
            "items": [self._cash_ledger_row_to_dict(row) for row in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    def list_corporate_action_events(
        self,
        *,
        account_id: Optional[int] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        symbol: Optional[str] = None,
        action_type: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        if account_id is not None:
            self._require_active_account(account_id)
        page, page_size = self._validate_paging(page=page, page_size=page_size)
        if date_from is not None and date_to is not None and date_from > date_to:
            raise ValueError("date_from must be <= date_to")

        symbol_filters: Optional[List[str]] = None
        if symbol is not None and symbol.strip():
            symbol_filters = self._build_symbol_filter_values(symbol)
            if not symbol_filters:
                raise ValueError("symbol is invalid")

        action_norm: Optional[str] = None
        if action_type is not None and action_type.strip():
            action_norm = action_type.strip().lower()
            if action_norm not in VALID_CORPORATE_ACTIONS:
                raise ValueError("action_type must be cash_dividend or split_adjustment")

        rows, total = self.repo.query_corporate_actions(
            account_id=account_id,
            date_from=date_from,
            date_to=date_to,
            symbols=symbol_filters,
            action_type=action_norm,
            page=page,
            page_size=page_size,
        )
        return {
            "items": [self._corporate_action_row_to_dict(row) for row in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    # ------------------------------------------------------------------
    # Snapshot replay
    # ------------------------------------------------------------------
    def get_portfolio_snapshot(
        self,
        *,
        account_id: Optional[int] = None,
        as_of: Optional[date] = None,
        cost_method: str = "fifo",
        include_realtime: bool = True,
        read_only: bool = False,
    ) -> Dict[str, Any]:
        """Replay holdings; read-only callers skip live quotes and snapshot writes."""
        as_of_date = as_of or date.today()
        method = self._normalize_cost_method(cost_method)

        if account_id is not None:
            account = self._require_active_account(account_id)
            account_rows = [account]
        else:
            account_rows = self.repo.list_accounts(include_inactive=False)

        accounts_payload: List[Dict[str, Any]] = []
        aggregate_currency = "CNY"
        aggregate = {
            "total_cash": 0.0,
            "total_market_value": 0.0,
            "total_equity": 0.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "fee_total": 0.0,
            "tax_total": 0.0,
            "fx_stale": False,
            "limitations": [],
        }

        for account in account_rows:
            account_snapshot = self._replay_account(
                account=account,
                as_of_date=as_of_date,
                cost_method=method,
                include_realtime=include_realtime and not read_only,
            )

            if not read_only:
                self.repo.replace_positions_lots_and_snapshot(
                    account_id=account.id,
                    snapshot_date=as_of_date,
                    cost_method=method,
                    base_currency=account.base_currency,
                    total_cash=account_snapshot["total_cash"] if account_snapshot["cash_known"] else 0.0,
                    total_market_value=account_snapshot["total_market_value"],
                    total_equity=account_snapshot["total_equity"] if account_snapshot["cash_known"] else 0.0,
                    unrealized_pnl=account_snapshot["unrealized_pnl"],
                    realized_pnl=account_snapshot["realized_pnl"],
                    fee_total=account_snapshot["fee_total"],
                    tax_total=account_snapshot["tax_total"],
                    fx_stale=account_snapshot["fx_stale"],
                    payload=json.dumps(account_snapshot["payload"], ensure_ascii=False),
                    positions=account_snapshot["positions_cache"],
                    lots=account_snapshot["lots_cache"],
                    valuation_currency=account.base_currency,
                )

            accounts_payload.append(account_snapshot["public"])
            aggregate["limitations"] = _merge_portfolio_limitations(
                aggregate["limitations"],
                account_snapshot["public"].get("limitations", []),
            )

            if not account_snapshot["cash_known"]:
                # One account with unknown cash makes the aggregate cash/equity
                # incomplete: report unknown instead of summing partials (R2).
                aggregate["cash_known"] = False

            cash_cny, stale_cash, _ = self._convert_amount(
                amount=account_snapshot["total_cash"] if account_snapshot["cash_known"] else 0.0,
                from_currency=account.base_currency,
                to_currency=aggregate_currency,
                as_of_date=as_of_date,
            )
            mv_cny, stale_mv, _ = self._convert_amount(
                amount=account_snapshot["total_market_value"],
                from_currency=account.base_currency,
                to_currency=aggregate_currency,
                as_of_date=as_of_date,
            )
            account_snapshot["public"]["total_market_value_aggregate"] = round(mv_cny, 6)
            eq_cny, stale_eq, _ = self._convert_amount(
                amount=account_snapshot["total_equity"],
                from_currency=account.base_currency,
                to_currency=aggregate_currency,
                as_of_date=as_of_date,
            )
            realized_cny, stale_realized, _ = self._convert_amount(
                amount=account_snapshot["realized_pnl"],
                from_currency=account.base_currency,
                to_currency=aggregate_currency,
                as_of_date=as_of_date,
            )
            unrealized_cny, stale_unrealized, _ = self._convert_amount(
                amount=account_snapshot["unrealized_pnl"],
                from_currency=account.base_currency,
                to_currency=aggregate_currency,
                as_of_date=as_of_date,
            )
            fee_cny, stale_fee, _ = self._convert_amount(
                amount=account_snapshot["fee_total"],
                from_currency=account.base_currency,
                to_currency=aggregate_currency,
                as_of_date=as_of_date,
            )
            tax_cny, stale_tax, _ = self._convert_amount(
                amount=account_snapshot["tax_total"],
                from_currency=account.base_currency,
                to_currency=aggregate_currency,
                as_of_date=as_of_date,
            )

            aggregate["total_cash"] += cash_cny
            aggregate["total_market_value"] += mv_cny
            aggregate["total_equity"] += eq_cny
            aggregate["realized_pnl"] += realized_cny
            aggregate["unrealized_pnl"] += unrealized_cny
            aggregate["fee_total"] += fee_cny
            aggregate["tax_total"] += tax_cny
            aggregate["fx_stale"] = aggregate["fx_stale"] or any(
                [
                    stale_cash,
                    stale_mv,
                    stale_eq,
                    stale_realized,
                    stale_unrealized,
                    stale_fee,
                    stale_tax,
                ]
            )

        # R3: one valuation identity per snapshot build — positions, account and
        # aggregate totals in THIS response share the same quote set; consumers
        # (reports, alerts) reference this id + quote_refs instead of re-fetching
        # a second price version. Deterministic over (as_of, method, quotes).
        quote_refs = []
        for account_payload_item in accounts_payload:
            for position in account_payload_item.get("positions", []):
                quote_refs.append(
                    "{symbol}|{price}|{provider}|{kind}|{time}|{date}|{q}".format(
                        symbol=position.get("symbol"),
                        price=position.get("last_price"),
                        provider=position.get("price_provider"),
                        kind=position.get("price_kind"),
                        time=position.get("price_provider_time"),
                        date=position.get("price_date"),
                        q=position.get("price_time_quality"),
                    )
                )
        valuation_digest = hashlib.sha256(
            "|".join(
                [
                    as_of_date.isoformat(),
                    method,
                    ",".join(sorted(str(a["account_id"]) for a in accounts_payload)),
                    ";".join(sorted(quote_refs)),
                    "price-policy-v2",
                ]
            ).encode("utf-8")
        ).hexdigest()[:16]

        return {
            "as_of": as_of_date.isoformat(),
            "cost_method": method,
            "currency": aggregate_currency,
            "account_count": len(account_rows),
            "valuation_id": f"val-{valuation_digest}",
            "quote_refs": [
                {
                    "account_id": account_payload_item["account_id"],
                    "symbol": position.get("symbol"),
                    "price": position.get("last_price"),
                    "provider": position.get("price_provider"),
                    "price_kind": position.get("price_kind"),
                    "provider_time": position.get("price_provider_time"),
                    "price_date": position.get("price_date"),
                    "time_quality": position.get("price_time_quality"),
                    "price_stale": position.get("price_stale"),
                }
                for account_payload_item in accounts_payload
                for position in account_payload_item.get("positions", [])
            ],
            "total_cash": round(aggregate["total_cash"], 6) if aggregate.get("cash_known", True) else None,
            "total_market_value": round(aggregate["total_market_value"], 6),
            "total_equity": round(aggregate["total_equity"], 6) if aggregate.get("cash_known", True) else None,
            "realized_pnl": round(aggregate["realized_pnl"], 6),
            "unrealized_pnl": round(aggregate["unrealized_pnl"], 6),
            "fee_total": round(aggregate["fee_total"], 6),
            "tax_total": round(aggregate["tax_total"], 6),
            "fx_stale": aggregate["fx_stale"],
            "cash_known": aggregate.get("cash_known", True),
            "data_quality": "partial" if aggregate["limitations"] else "ok",
            "limitations": aggregate["limitations"],
            "accounts": accounts_payload,
        }

    def refresh_fx_rates(
        self,
        *,
        account_id: Optional[int] = None,
        as_of: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Refresh account FX pairs online with stale fallback when fetch fails."""
        as_of_date = as_of or date.today()
        config = get_config()
        refresh_enabled = bool(getattr(config, "portfolio_fx_update_enabled", True))
        if account_id is not None:
            account_rows = [self._require_active_account(account_id)]
        else:
            account_rows = self.repo.list_accounts(include_inactive=False)

        summary = {
            "as_of": as_of_date.isoformat(),
            "account_count": len(account_rows),
            "refresh_enabled": refresh_enabled,
            "disabled_reason": None if refresh_enabled else PORTFOLIO_FX_REFRESH_DISABLED_REASON,
            "pair_count": 0,
            "updated_count": 0,
            "stale_count": 0,
            "error_count": 0,
        }
        for account in account_rows:
            item = self._refresh_account_fx_rates(
                account=account,
                as_of_date=as_of_date,
                refresh_enabled=refresh_enabled,
            )
            summary["pair_count"] += item["pair_count"]
            summary["updated_count"] += item["updated_count"]
            summary["stale_count"] += item["stale_count"]
            summary["error_count"] += item["error_count"]
        return summary

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _validate_trade_identity(
        self,
        *,
        account_id: int,
        trade_uid: Optional[str],
        dedup_hash: Optional[str],
        session: Optional[Any] = None,
    ) -> None:
        if trade_uid and self._has_trade_uid(account_id=account_id, trade_uid=trade_uid, session=session):
            raise PortfolioConflictError(f"Duplicate trade_uid for account_id={account_id}: {trade_uid}")
        if dedup_hash and self._has_trade_dedup_hash(account_id=account_id, dedup_hash=dedup_hash, session=session):
            raise PortfolioConflictError(f"Duplicate dedup_hash for account_id={account_id}: {dedup_hash}")

    def _validate_sell_quantity(
        self,
        *,
        account_id: int,
        symbol: str,
        market: str,
        currency: str,
        trade_date: date,
        quantity: float,
        session: Optional[Any] = None,
    ) -> None:
        key = (
            self._normalize_symbol_for_position(symbol),
            self._normalize_market(market),
            self._normalize_currency(currency),
        )
        available_quantity = self._calculate_available_quantity(
            account_id=account_id,
            key=key,
            as_of_date=trade_date,
            session=session,
        )
        if available_quantity + EPS < quantity:
            raise PortfolioOversellError(
                symbol=key[0],
                trade_date=trade_date,
                requested_quantity=quantity,
                available_quantity=available_quantity,
            )

    def _calculate_available_quantity(
        self,
        *,
        account_id: int,
        key: Tuple[str, str, str],
        as_of_date: date,
        session: Optional[Any] = None,
    ) -> float:
        if session is None:
            trades = self.repo.list_trades(account_id, as_of=as_of_date)
            corporate_actions = self.repo.list_corporate_actions(account_id, as_of=as_of_date)
        else:
            trades = self.repo.list_trades_in_session(session=session, account_id=account_id, as_of=as_of_date)
            corporate_actions = self.repo.list_corporate_actions_in_session(
                session=session,
                account_id=account_id,
                as_of=as_of_date,
            )

        events = []
        for row in corporate_actions:
            event_key = (
                self._normalize_symbol_for_position(row.symbol),
                self._normalize_market(row.market),
                self._normalize_currency(row.currency),
            )
            if event_key == key:
                events.append(("corp", row.effective_date, row.id, row))
        for row in trades:
            event_key = (
                self._normalize_symbol_for_position(row.symbol),
                self._normalize_market(row.market),
                self._normalize_currency(row.currency),
            )
            if event_key == key:
                events.append(("trade", row.trade_date, row.id, row))

        # Quantity validation only depends on position-changing events for one symbol.
        # Cash ledger entries do not affect shares held, so we keep the same corp->trade
        # ordering as full replay without pulling unrelated cash events into this path.
        event_priority = {"corp": 1, "trade": 2}
        events.sort(key=lambda item: (item[1], event_priority[item[0]], item[2]))

        # R2 baseline takeover, identical to full replay: the latest account
        # baseline at or before as_of replaces earlier history. A symbol absent
        # from the active baseline means zero holdings at that date.
        baseline_dates = [
            row.trade_date
            for row in trades
            if (getattr(row, "event_type", None) or "trade").strip().lower() == "baseline"
        ]
        active_baseline_date = max(baseline_dates) if baseline_dates else None

        quantity_held = 0.0
        if active_baseline_date is not None:
            for row in trades:
                record_type = (getattr(row, "event_type", None) or "trade").strip().lower()
                if (
                    record_type == "baseline"
                    and row.trade_date == active_baseline_date
                    and (
                        self._normalize_symbol_for_position(row.symbol),
                        self._normalize_market(row.market),
                        self._normalize_currency(row.currency),
                    ) == key
                ):
                    quantity_held += float(row.quantity or 0.0)

        for event_type, event_date, _, event in events:
            if active_baseline_date is not None:
                if event_date <= active_baseline_date:
                    continue  # superseded by the active baseline
                if (getattr(event, "event_type", None) or "trade").strip().lower() in BASELINE_LIKE_EVENT_TYPES:
                    continue  # baseline/init rows after the active baseline are artifacts

            if event_type == "corp":
                action_type = (event.action_type or "").strip().lower()
                if action_type != "split_adjustment":
                    continue
                split_ratio = float(event.split_ratio or 0.0)
                if split_ratio <= 0:
                    raise ValueError(f"Invalid split_ratio for {key[0]}")
                if abs(split_ratio - 1.0) <= EPS:
                    continue
                quantity_held *= split_ratio
                continue

            qty = float(event.quantity or 0.0)
            if qty <= 0:
                raise ValueError(f"Invalid trade quantity for {key[0]}")
            side = (event.side or "").strip().lower()
            if side == "buy":
                quantity_held += qty
                continue
            if side != "sell":
                raise ValueError(f"Unsupported trade side: {event.side}")
            if quantity_held + EPS < qty:
                raise PortfolioOversellError(
                    symbol=key[0],
                    trade_date=event_date,
                    requested_quantity=qty,
                    available_quantity=quantity_held,
                )
            quantity_held -= qty
            if quantity_held <= EPS:
                quantity_held = 0.0

        return quantity_held

    def _replay_account(
        self,
        *,
        account: Any,
        as_of_date: date,
        cost_method: str,
        include_realtime: bool,
    ) -> Dict[str, Any]:
        trades = self.repo.list_trades(account.id, as_of=as_of_date)
        cash_ledger = self.repo.list_cash_ledger(account.id, as_of=as_of_date)
        corporate_actions = self.repo.list_corporate_actions(account.id, as_of=as_of_date)

        events = []
        for row in cash_ledger:
            events.append(("cash", row.event_date, row.id, row))
        for row in trades:
            events.append(("trade", row.trade_date, row.id, row))
        for row in corporate_actions:
            events.append(("corp", row.effective_date, row.id, row))

        # Same-day deterministic ordering: cash -> corporate action -> trade.
        event_priority = {"cash": 0, "corp": 1, "trade": 2}
        events.sort(key=lambda item: (item[1], event_priority[item[0]], item[2]))

        # R2 baseline takeover: the LATEST confirmed baseline at or before
        # as_of is the account's starting point. Everything dated at or before
        # it (old init rows, older baselines, real trades, cash, corp actions)
        # is superseded history — kept in the DB, excluded from replay, never
        # double-counted on top of the baseline.
        baseline_rows_by_date: Dict[date, List[Any]] = {}
        for row in trades:
            if (getattr(row, "event_type", None) or "").strip().lower() == "baseline":
                baseline_rows_by_date.setdefault(row.trade_date, []).append(row)
        active_baseline_date: Optional[date] = (
            max(baseline_rows_by_date) if baseline_rows_by_date else None
        )

        cash_balances: Dict[str, float] = defaultdict(float)
        fees_total_base = 0.0
        taxes_total_base = 0.0
        realized_pnl_base = 0.0
        fx_stale = False
        baseline_date: Optional[date] = active_baseline_date
        cash_known = True
        superseded_count = 0
        event_type_counts: Dict[str, int] = defaultdict(int)

        fifo_lots: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        avg_state: Dict[Tuple[str, str, str], _AvgState] = defaultdict(_AvgState)

        if active_baseline_date is not None:
            # Seed positions from the confirmed baseline snapshot (replace, not add).
            event_type_counts["baseline"] = len(baseline_rows_by_date[active_baseline_date])
            for row in baseline_rows_by_date[active_baseline_date]:
                key = (
                    self._normalize_symbol_for_position(row.symbol),
                    self._normalize_market(row.market),
                    self._normalize_currency(row.currency),
                )
                qty = float(row.quantity or 0.0)
                unit_cost = float(row.price or 0.0)
                if qty <= 0:
                    raise ValueError(f"Invalid baseline quantity for {row.symbol}")
                if cost_method == "fifo":
                    fifo_lots[key].append(
                        {
                            "symbol": key[0],
                            "market": key[1],
                            "currency": key[2],
                            "open_date": active_baseline_date,
                            "remaining_quantity": qty,
                            "unit_cost": unit_cost,
                            "source_trade_id": row.id,
                        }
                    )
                else:
                    state = avg_state[key]
                    state.quantity += qty
                    state.total_cost += qty * unit_cost
            # Seed cash from the baseline-cash ledger note; no row = cash unknown.
            baseline_cash_note = f"baseline-cash:{account.id}:{active_baseline_date.isoformat()}"
            seeded_cash = [row for row in cash_ledger if row.note == baseline_cash_note]
            if seeded_cash:
                cash_row = seeded_cash[0]
                cash_balances[self._normalize_currency(cash_row.currency)] = float(
                    cash_row.amount or 0.0
                )
            else:
                cash_known = False

        for event_type, event_date, _, event in events:
            if active_baseline_date is not None:
                if event_date <= active_baseline_date:
                    # Superseded by the baseline: pre-baseline/baseline-date
                    # history never replays on top of the confirmed snapshot.
                    superseded_count += 1
                    continue
                if event_type == "trade" and (
                    (getattr(event, "event_type", None) or "trade").strip().lower()
                    in BASELINE_LIKE_EVENT_TYPES
                ):
                    # baseline/init rows dated after the active baseline are
                    # historical artifacts, never real trades.
                    superseded_count += 1
                    continue

            if event_type == "cash":
                currency = self._normalize_currency(event.currency)
                amount = float(event.amount or 0.0)
                if event.direction == "in":
                    cash_balances[currency] += amount
                elif event.direction == "out":
                    cash_balances[currency] -= amount
                else:
                    raise ValueError(f"Unsupported cash direction: {event.direction}")
                continue

            if event_type == "trade":
                key = (
                    self._normalize_symbol_for_position(event.symbol),
                    self._normalize_market(event.market),
                    self._normalize_currency(event.currency),
                )
                qty = float(event.quantity or 0.0)
                price = float(event.price or 0.0)
                fee = float(event.fee or 0.0)
                tax = float(event.tax or 0.0)
                if qty <= 0 or price <= 0:
                    raise ValueError(f"Invalid trade quantity or price for {event.symbol}")

                # Event provenance (D12): baseline/init events establish the
                # confirmed position snapshot — they do NOT move cash and are
                # excluded from realized PnL / fee statistics. Real trades
                # replay exactly as before.
                record_type = (getattr(event, "event_type", None) or "trade").strip().lower()
                event_type_counts[record_type] = event_type_counts.get(record_type, 0) + 1
                if record_type == "baseline" and (baseline_date is None or event.trade_date < baseline_date):
                    baseline_date = event.trade_date
                is_baseline_like = record_type in BASELINE_LIKE_EVENT_TYPES

                gross = qty * price
                side = (event.side or "").lower().strip()
                if side == "buy":
                    if not is_baseline_like:
                        cash_balances[key[2]] -= (gross + fee + tax)
                    if cost_method == "fifo":
                        unit_cost = (gross + (0.0 if is_baseline_like else fee + tax)) / qty
                        fifo_lots[key].append(
                            {
                                "symbol": key[0],
                                "market": key[1],
                                "currency": key[2],
                                "open_date": event_date,
                                "remaining_quantity": qty,
                                "unit_cost": unit_cost,
                                "source_trade_id": event.id,
                            }
                        )
                    else:
                        state = avg_state[key]
                        state.quantity += qty
                        state.total_cost += gross + (0.0 if is_baseline_like else fee + tax)
                elif side == "sell":
                    cash_balances[key[2]] += (gross - fee - tax)
                    proceeds_net = gross - fee - tax
                    if cost_method == "fifo":
                        cost_basis = self._consume_fifo_lots(
                            fifo_lots[key],
                            qty,
                            key[0],
                            event_date,
                        )
                    else:
                        cost_basis = self._consume_avg_position(
                            avg_state[key],
                            qty,
                            key[0],
                            event_date,
                        )
                    realized_local = proceeds_net - cost_basis
                    realized_base, stale_realized, _ = self._convert_amount(
                        amount=realized_local,
                        from_currency=key[2],
                        to_currency=account.base_currency,
                        as_of_date=event_date,
                    )
                    realized_pnl_base += realized_base
                    fx_stale = fx_stale or stale_realized
                else:
                    raise ValueError(f"Unsupported trade side: {event.side}")

                if not is_baseline_like:
                    fee_base, stale_fee, _ = self._convert_amount(
                        amount=fee,
                        from_currency=key[2],
                        to_currency=account.base_currency,
                        as_of_date=event_date,
                    )
                    tax_base, stale_tax, _ = self._convert_amount(
                        amount=tax,
                        from_currency=key[2],
                        to_currency=account.base_currency,
                        as_of_date=event_date,
                    )
                    fees_total_base += fee_base
                    taxes_total_base += tax_base
                    fx_stale = fx_stale or stale_fee or stale_tax
                continue

            if event_type == "corp":
                key = (
                    self._normalize_symbol_for_position(event.symbol),
                    self._normalize_market(event.market),
                    self._normalize_currency(event.currency),
                )
                action_type = (event.action_type or "").strip().lower()
                if action_type == "cash_dividend":
                    per_share = float(event.cash_dividend_per_share or 0.0)
                    if per_share <= 0:
                        continue
                    qty_held = self._held_quantity(
                        key=key,
                        cost_method=cost_method,
                        fifo_lots=fifo_lots,
                        avg_state=avg_state,
                    )
                    if qty_held > EPS:
                        cash_balances[key[2]] += qty_held * per_share
                elif action_type == "split_adjustment":
                    split_ratio = float(event.split_ratio or 0.0)
                    if split_ratio <= 0:
                        raise ValueError(f"Invalid split_ratio for {event.symbol}")
                    if abs(split_ratio - 1.0) <= EPS:
                        continue
                    if cost_method == "fifo":
                        for lot in fifo_lots[key]:
                            lot["remaining_quantity"] *= split_ratio
                            lot["unit_cost"] /= split_ratio
                    else:
                        state = avg_state[key]
                        state.quantity *= split_ratio
                else:
                    raise ValueError(f"Unsupported corporate action type: {event.action_type}")

        position_rows, lot_rows, market_value_base, total_cost_base, stale_pos, coverage = self._build_positions(
            account=account,
            as_of_date=as_of_date,
            cost_method=cost_method,
            fifo_lots=fifo_lots,
            avg_state=avg_state,
            include_realtime=include_realtime,
        )
        fx_stale = fx_stale or stale_pos

        total_cash_base = 0.0
        for currency, amount in cash_balances.items():
            converted, stale, _ = self._convert_amount(
                amount=amount,
                from_currency=currency,
                to_currency=account.base_currency,
                as_of_date=as_of_date,
            )
            total_cash_base += converted
            fx_stale = fx_stale or stale

        unrealized_pnl_base = market_value_base - total_cost_base
        total_equity_base = total_cash_base + market_value_base
        position_limitations = [
            limitation
            for position in position_rows
            for limitation in position.get("limitations", [])
        ]
        limitations = _merge_portfolio_limitations(
            _portfolio_limitations_for_market(account.market),
            position_limitations,
        )
        if not cash_known:
            limitations = _merge_portfolio_limitations(
                limitations,
                [
                    "cash unknown at baseline "
                    f"{active_baseline_date.isoformat()}; total_cash/total_equity not computed"
                ],
            )

        account_payload = {
            "account_id": account.id,
            "account_name": account.name,
            "owner_id": account.owner_id,
            "broker": account.broker,
            "market": account.market,
            "base_currency": account.base_currency,
            "as_of": as_of_date.isoformat(),
            "cost_method": cost_method,
            "total_cash": round(total_cash_base, 6) if cash_known else None,
            "total_market_value": round(market_value_base, 6),
            "total_equity": round(total_equity_base, 6) if cash_known else None,
            "realized_pnl": round(realized_pnl_base, 6),
            "unrealized_pnl": round(unrealized_pnl_base, 6),
            "fee_total": round(fees_total_base, 6),
            "tax_total": round(taxes_total_base, 6),
            "fx_stale": fx_stale,
            "cash_known": cash_known,
            "data_quality": "partial" if limitations else "ok",
            "limitations": limitations,
            "positions": position_rows,
            "valuation_coverage": {
                "valued": coverage["valued"],
                "total": coverage["total"],
                "unvalued_symbols": coverage["unvalued_symbols"],
            },
            "baseline_date": baseline_date.isoformat() if baseline_date else None,
            "event_type_counts": dict(sorted(event_type_counts.items())) if event_type_counts else {},
            "superseded_event_count": superseded_count if active_baseline_date is not None else None,
        }

        return {
            "public": account_payload,
            "payload": account_payload,
            "positions_cache": position_rows,
            "lots_cache": lot_rows,
            "cash_known": cash_known,
            "total_cash": float(total_cash_base),
            "total_market_value": float(market_value_base),
            "total_equity": float(total_equity_base),
            "realized_pnl": float(realized_pnl_base),
            "unrealized_pnl": float(unrealized_pnl_base),
            "fee_total": float(fees_total_base),
            "tax_total": float(taxes_total_base),
            "fx_stale": fx_stale,
        }

    def _build_positions(
        self,
        *,
        account: Any,
        as_of_date: date,
        cost_method: str,
        fifo_lots: Dict[Tuple[str, str, str], List[Dict[str, Any]]],
        avg_state: Dict[Tuple[str, str, str], _AvgState],
        include_realtime: bool = True,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], float, float, bool]:
        position_rows: List[Dict[str, Any]] = []
        lot_rows: List[Dict[str, Any]] = []
        market_value_base = 0.0
        total_cost_base = 0.0
        fx_stale = False
        unvalued_symbols: List[str] = []

        keys: Iterable[Tuple[str, str, str]]
        if cost_method == "fifo":
            keys = list(fifo_lots.keys())
        else:
            keys = list(avg_state.keys())

        active_symbols: List[str] = []
        if include_realtime and as_of_date == date.today():
            for key in sorted(keys):
                symbol, _, _ = key
                if cost_method == "fifo":
                    qty = sum(
                        float(lot["remaining_quantity"])
                        for lot in fifo_lots[key]
                        if lot["remaining_quantity"] > EPS
                    )
                else:
                    qty = float(avg_state[key].quantity)
                if qty > EPS:
                    active_symbols.append(symbol)
        realtime_prices = (
            self._prefetch_realtime_position_prices(active_symbols)
            if active_symbols
            else None
        )

        for key in sorted(keys):
            symbol, market, currency = key

            if cost_method == "fifo":
                active_lots = [lot for lot in fifo_lots[key] if lot["remaining_quantity"] > EPS]
                qty = sum(float(lot["remaining_quantity"]) for lot in active_lots)
                if qty <= EPS:
                    continue
                total_cost = sum(float(lot["remaining_quantity"]) * float(lot["unit_cost"]) for lot in active_lots)
                avg_cost = total_cost / qty
                lot_rows.extend(active_lots)
            else:
                state = avg_state[key]
                qty = float(state.quantity)
                total_cost = float(state.total_cost)
                if qty <= EPS:
                    continue
                avg_cost = total_cost / qty
                lot_rows.append(
                    {
                        "symbol": symbol,
                        "market": market,
                        "currency": currency,
                        "open_date": as_of_date,
                        "remaining_quantity": qty,
                        "unit_cost": avg_cost,
                        "source_trade_id": None,
                    }
                )

            price_info = self._resolve_position_price(
                symbol=symbol,
                as_of_date=as_of_date,
                realtime_prices=realtime_prices,
                include_realtime=include_realtime,
            )
            last_price = price_info.price
            limitations = _portfolio_limitations_for_market(market)

            if price_info.is_available:
                local_market_value = qty * float(last_price)
                market_base, stale_market, _ = self._convert_amount(
                    amount=local_market_value,
                    from_currency=currency,
                    to_currency=account.base_currency,
                    as_of_date=as_of_date,
                )
                cost_base, stale_cost, _ = self._convert_amount(
                    amount=total_cost,
                    from_currency=currency,
                    to_currency=account.base_currency,
                    as_of_date=as_of_date,
                )
                if qty > EPS and total_cost <= EPS:
                    # Confirmed position with unknown cost (R2): market value is
                    # shown, but PnL must NOT be computed — neither as market
                    # value nor as fake break-even zero.
                    cost_basis_unknown = True
                    unrealized_base = 0.0
                else:
                    cost_basis_unknown = False
                    unrealized_base = market_base - cost_base
                fx_stale = fx_stale or stale_market or stale_cost
            else:
                # Missing price: keep shares/cost visible, contribute zero
                # valuation, and surface the gap explicitly (T03). Never fake
                # a zero-value total-equity as if it were complete.
                cost_basis_unknown = qty > EPS and total_cost <= EPS
                market_base = 0.0
                cost_base, stale_cost, _ = self._convert_amount(
                    amount=total_cost,
                    from_currency=currency,
                    to_currency=account.base_currency,
                    as_of_date=as_of_date,
                )
                fx_stale = fx_stale or stale_cost
                unrealized_base = 0.0
                unvalued_symbols.append(symbol)
                if price_info.source == "identity_unconfirmed":
                    limitations = list(limitations) + [
                        f"identity unconfirmed for {symbol}; valuation paused until the "
                        "instrument type (stock/etf/otc_fund) is confirmed"
                    ]
                else:
                    limitations = list(limitations) + [
                        f"price unavailable for {symbol}; shares and cost kept, valuation excluded"
                    ]
            # 10-24：数值可用不代表时效已验证；逐层传递到account/aggregate。
            if price_info.is_available and (price_info.time_quality == "unknown" or not price_info.price_date):
                limitations = list(limitations) + [f"price source time unknown for {symbol}; freshness unverified, not a current-price trigger"]
            if price_info.is_available and price_info.is_stale:
                limitations = list(limitations) + [f"stale price for {symbol}; reference valuation only"]
            if cost_basis_unknown:
                limitations = list(limitations) + [
                    f"cost basis unknown for {symbol}; quantity and market value kept, PnL not computed"
                ]

            unrealized_pct = None
            if price_info.is_available and not cost_basis_unknown and abs(cost_base or 0.0) > EPS:
                unrealized_pct = unrealized_base / cost_base * 100.0

            position_rows.append(
                {
                    "symbol": symbol,
                    "market": market,
                    "currency": currency,
                    "quantity": round(qty, 8),
                    "avg_cost": round(avg_cost, 8),
                    "total_cost": round(total_cost, 8),
                    "last_price": round(float(last_price), 8),
                    "market_value_base": round(market_base, 8),
                    "unrealized_pnl_base": round(unrealized_base, 8),
                    "unrealized_pnl_pct": round(unrealized_pct, 8) if unrealized_pct is not None else None,
                    "valuation_currency": account.base_currency,
                    "price_source": price_info.source,
                    "price_provider": price_info.provider,
                    "price_date": price_info.price_date.isoformat() if price_info.price_date else None,
                    "price_stale": price_info.is_stale,
                    "price_available": price_info.is_available,
                    "price_kind": price_info.price_kind,
                    "price_provider_time": price_info.provider_time,
                    "price_fetched_at": price_info.fetched_at,
                    "price_time_quality": price_info.time_quality,
                    # R4/F09：共享上游披露（akshare ETF / efinance ETF / 基金净值
                    # 同属 eastmoney 组），报告不得称其为独立双源。
                    "price_upstream_group": upstream_group_for(price_info.provider),
                    "cost_basis_unknown": cost_basis_unknown,
                    "data_quality": "partial" if limitations else "ok",
                    "limitations": limitations,
                }
            )

            market_value_base += market_base
            if price_info.is_available:
                total_cost_base += cost_base

        coverage = {
            "valued": len(position_rows) - len(unvalued_symbols),
            "total": len(position_rows),
            "unvalued_symbols": unvalued_symbols,
        }
        return position_rows, lot_rows, market_value_base, total_cost_base, fx_stale, coverage

    def _resolve_position_price(
        self,
        *,
        symbol: str,
        as_of_date: date,
        realtime_prices: Optional[Dict[str, Dict[str, Any]]] = None,
        include_realtime: bool = True,
    ) -> _ResolvedPositionPrice:
        today = date.today()

        if include_realtime and as_of_date == today:
            if realtime_prices is None:
                quote_info = self._fetch_realtime_position_price(symbol)
            else:
                quote_info = realtime_prices.get(symbol)
            if quote_info is not None and quote_info.get("kind") == "identity_paused":
                # Identity unconfirmed and ambiguous (R3/F06): pause THIS
                # symbol's valuation — no exchange quote, no NAV guess, and no
                # history-close fallback either.
                return _ResolvedPositionPrice(
                    price=0.0,
                    source="identity_unconfirmed",
                    price_date=None,
                    is_stale=True,
                    is_available=False,
                    provider="identity_unconfirmed",
                    price_kind="identity_paused",
                    time_quality="unknown",
                )
            if quote_info is not None:
                price = float(quote_info.get("price") or 0.0)
                if price > 0:
                    return self._build_realtime_resolved_price(
                        symbol=symbol,
                        as_of_date=as_of_date,
                        quote_info=quote_info,
                    )

        close = self.repo.get_latest_close_with_date(symbol=symbol, as_of=as_of_date)
        if close is not None:
            close_price, close_date = close
            if close_price > 0:
                return _ResolvedPositionPrice(
                    price=float(close_price),
                    source="history_close",
                    price_date=close_date,
                    is_stale=close_date < as_of_date,
                    is_available=True,
                    price_kind="close",
                )

        return _ResolvedPositionPrice(
            price=0.0,
            source="missing",
            price_date=None,
            is_stale=True,
            is_available=False,
            price_kind="missing",
        )

    def _build_realtime_resolved_price(
        self,
        *,
        symbol: str,
        as_of_date: date,
        quote_info: Dict[str, Any],
        now_cn: Optional[datetime] = None,
    ) -> _ResolvedPositionPrice:
        """Turn a fetched quote/NAV into a resolved price with honest time semantics.

        Rules (T1/T02):
        - price_date comes from the provider timestamp / NAV date, never from
          the fetch clock. A positive price alone no longer means "today".
        - NAV: never treated as realtime; stale only when the latest published
          NAV lags the as-of date beyond NAV_STALENESS_MAX_LAG_DAYS (QDII T-2+
          publication is normal).
        - Realtime quote with provider time:
            * provider date == as-of date and the CN session is live right now
              -> stale when the quote age exceeds 60 minutes;
            * provider date == as-of date, session closed -> official close of
              today, not stale;
            * provider date < as-of date and today's CN session has opened
              -> stale (previous-session price while a newer session trades);
            * provider date < as-of date and today's session has not opened
              (pre-market / non-trading day) -> latest official close, not
              stale, price_date kept honest.
        - Realtime quote without provider time: the source moment is unknown —
          price_date stays None and time_quality='unknown'; the valuation is
          usable but must be reported as time-unknown, never as realtime.
        """

        price = float(quote_info.get("price"))
        provider = quote_info.get("provider")
        kind = str(quote_info.get("kind") or "realtime")
        provider_time_raw = quote_info.get("provider_time")
        fetched_at_raw = quote_info.get("fetched_at")

        provider_dt: Optional[datetime] = None
        if isinstance(provider_time_raw, datetime):
            provider_dt = provider_time_raw
        elif isinstance(provider_time_raw, str) and provider_time_raw:
            try:
                provider_dt = datetime.fromisoformat(provider_time_raw)
            except ValueError:
                provider_dt = None
        fetched_dt: Optional[datetime] = None
        if isinstance(fetched_at_raw, datetime):
            fetched_dt = fetched_at_raw
        elif isinstance(fetched_at_raw, str) and fetched_at_raw:
            try:
                fetched_dt = datetime.fromisoformat(fetched_at_raw)
            except ValueError:
                fetched_dt = None

        if kind == "nav":
            nav_date = provider_dt.date() if provider_dt is not None else None
            # NAV carries DATE precision only: keep the provider time verbatim
            # (a plain date string); never fabricate an intraday publish time (R3).
            provider_time_iso = (
                str(provider_time_raw) if provider_time_raw else None
            )
            return _ResolvedPositionPrice(
                price=price,
                source="fund_nav",
                price_date=nav_date,
                is_stale=(
                    nav_date is None
                    or (as_of_date - nav_date).days > NAV_STALENESS_MAX_LAG_DAYS
                ),
                is_available=True,
                provider=provider,
                price_kind="nav",
                provider_time=provider_time_iso,
                fetched_at=fetched_at_raw if isinstance(fetched_at_raw, str) else None,
                time_quality="valid" if nav_date is not None else "unknown",
            )

        # Realtime quote path.
        now_cn = now_cn or datetime.now(_CN_TZ)
        session_live = self._cn_session_live(now_cn, as_of_date)
        session_opened_today = self._cn_session_opened_today(now_cn, as_of_date)

        if provider_dt is not None:
            if provider_dt.tzinfo is None:
                provider_dt = provider_dt.replace(tzinfo=_CN_TZ)
            provider_dt_cn = provider_dt.astimezone(_CN_TZ)
            provider_date = provider_dt_cn.date()
            if provider_date == as_of_date:
                is_stale = bool(
                    session_live
                    and fetched_dt is not None
                    and (
                        (fetched_dt.astimezone(_CN_TZ) - provider_dt_cn).total_seconds()
                        > REALTIME_STALENESS_MAX_AGE_SECONDS
                    )
                )
            else:
                # Provider quote from an earlier date than the as-of date.
                is_stale = bool(session_opened_today)
            time_quality = "valid"
        else:
            # No provider time disclosed: the source moment is UNKNOWN — keep
            # price_date None (never fill it with the fetch date) and mark
            # time_quality='unknown'; consumers must not present it as
            # realtime/normal (R3/F07).
            provider_date = None
            is_stale = False
            time_quality = "unknown"

        return _ResolvedPositionPrice(
            price=price,
            source="realtime_quote",
            price_date=provider_date,
            is_stale=is_stale,
            is_available=True,
            provider=provider,
            price_kind="realtime",
            provider_time=(
                provider_dt.astimezone(_CN_TZ).isoformat() if provider_dt is not None else None
            ),
            fetched_at=fetched_at_raw if isinstance(fetched_at_raw, str) else None,
            time_quality=time_quality,
        )

    @staticmethod
    def _cn_session_live(now_cn: datetime, check_date: date) -> bool:
        """True when now (CN time) falls inside the A-share continuous session on a trading day."""
        if check_date != now_cn.date():
            return False
        if not is_market_open("cn", check_date):
            return False
        minutes = now_cn.hour * 60 + now_cn.minute
        return (9 * 60 + 25) <= minutes <= (15 * 60 + 5)

    @staticmethod
    def _cn_session_opened_today(now_cn: datetime, check_date: date) -> bool:
        """True when the CN session for check_date has already opened (09:25 or later)."""
        if check_date != now_cn.date():
            return check_date < now_cn.date()
        return is_market_open("cn", check_date) and (now_cn.hour * 60 + now_cn.minute) >= (9 * 60 + 25)

    def _prefetch_realtime_position_prices(
        self,
        symbols: Iterable[str],
    ) -> Dict[str, Dict[str, Any]]:
        unique_symbols = sorted({symbol for symbol in symbols if symbol})
        if not unique_symbols:
            return {}

        # Bulk prefetch (when applicable) only warms the fetcher-module-level realtime cache;
        # the manager itself is discarded so per-symbol workers cannot serialize through its
        # per-fetcher call locks when individual reads still need a live fetch (e.g. mixed
        # markets, cache miss, or bulk source returning fewer rows than requested).
        # Definite-OTC funds never touch the stock prefetch (T01).
        listed_symbols = [
            symbol
            for symbol in unique_symbols
            if classify_cn_symbol(self._normalize_symbol_for_position(symbol)) != "otc"
        ]
        if len(listed_symbols) >= 5:
            try:
                from data_provider.base import DataFetcherManager

                DataFetcherManager().prefetch_realtime_quotes(listed_symbols)
            except Exception as exc:
                logger.warning("Failed to prefetch realtime portfolio quotes: %s", exc)

        results: Dict[str, Dict[str, Any]] = {}
        pending = list(unique_symbols)
        if len(pending) == 1:
            symbol = pending[0]
            info = self._fetch_realtime_position_price(symbol)
            if info is not None:
                results[symbol] = info
            return results

        max_workers = min(PORTFOLIO_REALTIME_QUOTE_MAX_WORKERS, len(pending))
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="portfolio-quote") as executor:
            futures = {
                executor.submit(self._fetch_realtime_position_price, symbol): symbol
                for symbol in pending
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    info = future.result()
                except Exception as exc:  # pragma: no cover - defensive guard for patched fetchers
                    logger.warning("Failed to prefetch realtime portfolio price for %s: %s", symbol, exc)
                    continue
                if info is not None:
                    results[symbol] = info

        return results

    @staticmethod
    def _fetch_realtime_position_price(symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch a live valuation price for one position, routed by asset identity.

        Returns a dict {price, provider, kind, provider_time, fetched_at}, or
        a {"kind": "identity_paused", ...} marker when the symbol's identity is
        unconfirmed and ambiguous (R3/F06), or None when nothing usable came
        back. Routing order:
        1. CONFIRMED identity (portfolio_instrument_identities, confirmed at
           baseline time or via the controlled identity endpoint):
           otc_fund -> NAV; stock/etf/index -> exchange quote.
        2. Heuristic (classify_cn_symbol): definite OTC -> NAV (stock APIs
           never called); ETF/LOF registry hits -> exchange; 'ambiguous'
           (real stock/fund code collision, e.g. 000001/003096) or 'unknown'
           (OTC registry unavailable outside listed ranges) -> identity PAUSED.
           A stock-quote failure is NEVER answered with the same-code fund NAV
           and vice versa — that guess is exactly what T01 forbids.
        """
        fetched_at = datetime.now(_CN_TZ).isoformat()
        normalized = _strip_symbol_suffix(symbol)
        route_for_symbol = None
        confirmed_kind = None
        try:
            from src.services.instrument_identity import (
                InstrumentIdentityService,
                route_for_symbol,
            )

            confirmed_kind = InstrumentIdentityService().confirmed_kind(symbol)
        except Exception as exc:
            logger.warning("[identity] registry unavailable, heuristic routing only: %s", exc)
        if route_for_symbol is not None:
            route = route_for_symbol(symbol, confirmed_kind, classify=classify_cn_symbol)
        else:
            # Registry broken: fall back to pure heuristics (still no guessing
            # across asset classes — ambiguous/unknown stay paused).
            heuristic = classify_cn_symbol(symbol)
            route = "nav" if heuristic == "otc" else ("exchange" if heuristic == "listed" else "paused_identity")

        def nav_quote() -> Optional[Dict[str, Any]]:
            nav_info = fetch_latest_nav(normalized)
            if nav_info is None:
                return None
            nav_date = nav_info["nav_date"]
            # NAV publishes with DATE precision only — keep the provider time
            # as the nav date string; never fabricate a 15:00 publish time (R3).
            if isinstance(nav_date, (datetime, date)):
                provider_time = nav_date.isoformat()
            else:
                provider_time = None
            try:
                nav_value = float(nav_info["nav"])
            except (TypeError, ValueError):
                return None
            if nav_value is None or nav_value <= 0:
                return None
            return {
                "price": nav_value,
                "provider": "akshare_fund_nav",
                "upstream_group": upstream_group_for("akshare_fund_nav"),
                "kind": "nav",
                "provider_time": provider_time,
                "fetched_at": fetched_at,
            }

        if route == "paused_identity":
            return {
                "kind": "identity_paused",
                "price": None,
                "provider": "identity_unconfirmed",
                "provider_time": None,
                "fetched_at": fetched_at,
            }
        if route == "nav":
            return nav_quote()

        stock_quote = _fetch_stock_realtime_quote(symbol, fetched_at)
        if stock_quote is not None:
            return stock_quote
        return None

    @staticmethod
    def _normalize_prefetch_symbols(symbols) -> "List[str]":
        return [
            symbol for symbol in symbols
            if classify_cn_symbol(symbol) != "otc"
        ]

    @staticmethod
    def _normalize_symbol_for_storage(symbol: str) -> str:
        return canonical_stock_code(symbol)

    @staticmethod
    def _normalize_symbol_for_position(symbol: str) -> str:
        if not (symbol or "").strip():
            return ""

        raw = canonical_stock_code(symbol)
        if len(raw) >= 8 and raw[:2] in {"SH", "SZ", "BJ"} and raw[2:].isdigit():
            return raw

        if "." in raw:
            base, suffix = raw.rsplit(".", 1)
            if base.isdigit() and suffix in {"SH", "SS", "SZ", "BJ"}:
                exchange = "SH" if suffix == "SS" else suffix
                return f"{exchange}{base}"

        return canonical_stock_code(normalize_stock_code(symbol))

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        """
        Canonicalization for symbol filtering with exchange-qualified input preservation.

        Keep explicit A-share exchange annotations (SH/SZ/BJ) intact to avoid collapsing
        different exchange variants of the same 6-digit core code.
        """
        raw = canonical_stock_code(symbol)
        if not raw:
            return ""

        if len(raw) >= 8 and raw[:2] in {"SH", "SZ", "BJ"} and raw[2:].isdigit():
            return raw

        if "." in raw:
            base, suffix = raw.rsplit(".", 1)
            if base.isdigit() and suffix in {"SH", "SS", "SZ", "BJ"}:
                exchange = "SH" if suffix == "SS" else suffix
                return f"{exchange}{base}"

        return canonical_stock_code(normalize_stock_code(symbol))

    @classmethod
    def _build_symbol_filter_values(cls, symbol: str) -> List[str]:
        original = (symbol or "").strip().upper()
        normalized = cls._normalize_symbol(original)
        if not normalized:
            return []

        seen: Set[str] = set()
        values: List[str] = []

        def _add(value: Optional[str]) -> None:
            candidate = (value or "").strip().upper()
            if candidate and candidate not in seen:
                seen.add(candidate)
                values.append(candidate)

        _add(original)
        _add(normalized)

        if normalized.startswith("HK"):
            hk_digits = normalized[2:]
            if hk_digits.isdigit() and len(hk_digits) == 5:
                legacy_hk_digits = str(int(hk_digits))
                _add(f"HK{hk_digits}")
                _add(f"HK{legacy_hk_digits}")
                _add(f"{hk_digits}.HK")
                _add(f"{legacy_hk_digits}.HK")
            return values

        explicit_exchange: Optional[str] = None
        if len(original) >= 8 and original[:2] in {"SH", "SZ", "BJ"} and original[2:].isdigit():
            explicit_exchange = original[:2]
            explicit_code = original[2:]
        elif "." in original:
            base, suffix = original.rsplit(".", 1)
            if base.isdigit() and suffix in {"SH", "SS", "SZ", "BJ"}:
                explicit_exchange = "SH" if suffix == "SS" else suffix
                explicit_code = base
            else:
                explicit_code = None
        else:
            explicit_code = None

        if normalized.isdigit():
            if len(normalized) == 6:
                exchanges = [explicit_exchange] if explicit_exchange else ["SH", "SZ", "BJ"]
                for exchange in exchanges:
                    if exchange is None:
                        continue
                    _add(f"{exchange}{normalized}")
                    _add(f"{normalized}.{'SS' if exchange == 'SH' else exchange}")
                    if exchange == "SH":
                        _add(f"{normalized}.SH")
            return values

        if explicit_exchange is not None and explicit_code is not None and explicit_code.isdigit():
            if len(explicit_code) == 6:
                _add(f"{explicit_exchange}{explicit_code}")
                _add(f"{explicit_code}.{'SS' if explicit_exchange == 'SH' else explicit_exchange}")
                if explicit_exchange == "SH":
                    _add(f"{explicit_code}.SH")
            elif len(normalized) == 5:
                _add(f"HK{normalized}")
                _add(f"{normalized}.HK")

        return values

    @staticmethod
    def _consume_fifo_lots(
        lots: List[Dict[str, Any]],
        quantity: float,
        symbol: str,
        trade_date: Optional[date] = None,
    ) -> float:
        remaining = quantity
        cost_basis = 0.0
        while remaining > EPS:
            if not lots:
                raise PortfolioOversellError(
                    symbol=symbol,
                    trade_date=trade_date,
                    requested_quantity=quantity,
                    available_quantity=quantity - remaining,
                )
            head = lots[0]
            take = min(remaining, float(head["remaining_quantity"]))
            cost_basis += take * float(head["unit_cost"])
            head["remaining_quantity"] = float(head["remaining_quantity"]) - take
            remaining -= take
            if head["remaining_quantity"] <= EPS:
                lots.pop(0)
        return cost_basis

    @staticmethod
    def _consume_avg_position(
        state: _AvgState,
        quantity: float,
        symbol: str,
        trade_date: Optional[date] = None,
    ) -> float:
        if state.quantity + EPS < quantity:
            raise PortfolioOversellError(
                symbol=symbol,
                trade_date=trade_date,
                requested_quantity=quantity,
                available_quantity=state.quantity,
            )
        if state.quantity <= EPS:
            raise PortfolioOversellError(
                symbol=symbol,
                trade_date=trade_date,
                requested_quantity=quantity,
                available_quantity=0.0,
            )
        avg_cost = state.total_cost / state.quantity
        cost_basis = avg_cost * quantity
        state.quantity -= quantity
        state.total_cost -= cost_basis
        if state.quantity <= EPS:
            state.quantity = 0.0
            state.total_cost = 0.0
        return cost_basis

    @staticmethod
    def _held_quantity(
        *,
        key: Tuple[str, str, str],
        cost_method: str,
        fifo_lots: Dict[Tuple[str, str, str], List[Dict[str, Any]]],
        avg_state: Dict[Tuple[str, str, str], _AvgState],
    ) -> float:
        if cost_method == "fifo":
            return sum(float(lot["remaining_quantity"]) for lot in fifo_lots.get(key, []))
        return float(avg_state.get(key, _AvgState()).quantity)

    def _convert_amount(
        self,
        *,
        amount: float,
        from_currency: str,
        to_currency: str,
        as_of_date: date,
    ) -> Tuple[float, bool, str]:
        from_norm = self._normalize_currency(from_currency)
        to_norm = self._normalize_currency(to_currency)
        if abs(amount) <= EPS:
            return 0.0, False, "zero"
        if from_norm == to_norm:
            return float(amount), False, "identity"

        direct = self.repo.get_latest_fx_rate(
            from_currency=from_norm,
            to_currency=to_norm,
            as_of=as_of_date,
        )
        if direct is not None and direct.rate > 0:
            return float(amount) * float(direct.rate), bool(direct.is_stale), "direct_rate"

        inverse = self.repo.get_latest_fx_rate(
            from_currency=to_norm,
            to_currency=from_norm,
            as_of=as_of_date,
        )
        if inverse is not None and inverse.rate > 0:
            return float(amount) / float(inverse.rate), bool(inverse.is_stale), "inverse_rate"

        # P0 fallback: keep pipeline available even when FX cache is missing.
        return float(amount), True, "fallback_1_to_1"

    def convert_amount(
        self,
        *,
        amount: float,
        from_currency: str,
        to_currency: str,
        as_of_date: date,
    ) -> Tuple[float, bool, str]:
        """Public conversion entry for cross-service consumers."""
        return self._convert_amount(
            amount=amount,
            from_currency=from_currency,
            to_currency=to_currency,
            as_of_date=as_of_date,
        )

    def _list_account_refresh_fx_currencies(
        self,
        *,
        account: Any,
        as_of_date: date,
        strict: bool = True,
    ) -> List[str]:
        """Return distinct non-base currencies participating in refresh for one account."""
        base_currency = self._normalize_currency(account.base_currency)
        currencies: Set[str] = set()
        rows = list(self.repo.list_trades(account.id, as_of=as_of_date))
        rows.extend(self.repo.list_cash_ledger(account.id, as_of=as_of_date))
        for row in rows:
            try:
                currency = self._normalize_currency(row.currency)
            except ValueError:
                if strict:
                    raise
                logger.warning(
                    "Skip invalid FX refresh currency for account %s on %s: %r",
                    account.id,
                    as_of_date.isoformat(),
                    getattr(row, "currency", None),
                )
                continue
            if currency != base_currency:
                currencies.add(currency)
        return sorted(currencies)

    def _refresh_account_fx_rates(
        self,
        *,
        account: Any,
        as_of_date: date,
        refresh_enabled: bool,
    ) -> Dict[str, int]:
        """Refresh FX pairs for one account and keep stale fallback on failures."""
        refresh_currencies = self._list_account_refresh_fx_currencies(
            account=account,
            as_of_date=as_of_date,
            strict=refresh_enabled,
        )
        if not refresh_enabled:
            return {
                "pair_count": len(refresh_currencies),
                "updated_count": 0,
                "stale_count": 0,
                "error_count": 0,
            }

        base_currency = self._normalize_currency(account.base_currency)
        summary = {
            "pair_count": len(refresh_currencies),
            "updated_count": 0,
            "stale_count": 0,
            "error_count": 0,
        }
        for from_currency in refresh_currencies:
            try:
                rate = self._fetch_fx_rate_from_yfinance(
                    from_currency=from_currency,
                    to_currency=base_currency,
                    as_of_date=as_of_date,
                )
                if rate is not None and rate > 0:
                    self.repo.save_fx_rate(
                        from_currency=from_currency,
                        to_currency=base_currency,
                        rate_date=as_of_date,
                        rate=rate,
                        source="yfinance",
                        is_stale=False,
                    )
                    summary["updated_count"] += 1
                    continue
            except Exception as exc:
                logger.warning(
                    "FX online fetch failed for %s/%s on %s: %s",
                    from_currency,
                    base_currency,
                    as_of_date.isoformat(),
                    exc,
                )

            fallback = self.repo.get_latest_fx_rate(
                from_currency=from_currency,
                to_currency=base_currency,
                as_of=as_of_date,
            )
            if fallback is not None and float(fallback.rate or 0.0) > 0:
                self.repo.save_fx_rate(
                    from_currency=from_currency,
                    to_currency=base_currency,
                    rate_date=as_of_date,
                    rate=float(fallback.rate),
                    source=(fallback.source or "cache_fallback"),
                    is_stale=True,
                )
                summary["stale_count"] += 1
            else:
                summary["error_count"] += 1
        return summary

    @staticmethod
    def _fetch_fx_rate_from_yfinance(
        *,
        from_currency: str,
        to_currency: str,
        as_of_date: date,
    ) -> Optional[float]:
        """Fetch latest available FX close rate around as_of date."""
        if yf is None:
            return None
        symbol = f"{from_currency}{to_currency}=X"
        ticker = yf.Ticker(symbol)
        history = ticker.history(
            start=(as_of_date - timedelta(days=7)).isoformat(),
            end=(as_of_date + timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
        )
        if history is None or history.empty or "Close" not in history:
            return None
        close = history["Close"].dropna()
        if close.empty:
            return None
        value = float(close.iloc[-1])
        if value <= 0:
            return None
        return value

    def _require_active_account(self, account_id: int) -> Any:
        account = self.repo.get_account(account_id, include_inactive=False)
        if account is None:
            raise ValueError(f"Active account not found: {account_id}")
        return account

    def _require_active_account_in_session(self, *, session: Any, account_id: int) -> Any:
        account = self.repo.get_account_in_session(
            session=session,
            account_id=account_id,
            include_inactive=False,
        )
        if account is None:
            raise ValueError(f"Active account not found: {account_id}")
        return account

    def _has_trade_uid(self, *, account_id: int, trade_uid: str, session: Optional[Any] = None) -> bool:
        if session is None:
            return self.repo.has_trade_uid(account_id, trade_uid)
        return self.repo.has_trade_uid_in_session(session=session, account_id=account_id, trade_uid=trade_uid)

    def _has_trade_dedup_hash(
        self,
        *,
        account_id: int,
        dedup_hash: str,
        session: Optional[Any] = None,
    ) -> bool:
        if session is None:
            return self.repo.has_trade_dedup_hash(account_id, dedup_hash)
        return self.repo.has_trade_dedup_hash_in_session(
            session=session,
            account_id=account_id,
            dedup_hash=dedup_hash,
        )

    @staticmethod
    def _account_to_dict(row: Any) -> Dict[str, Any]:
        return {
            "id": row.id,
            "owner_id": row.owner_id,
            "name": row.name,
            "broker": row.broker,
            "market": row.market,
            "base_currency": row.base_currency,
            "is_active": bool(row.is_active),
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _trade_row_to_dict(row: Any) -> Dict[str, Any]:
        return {
            "id": int(row.id),
            "account_id": int(row.account_id),
            "trade_uid": row.trade_uid,
            "symbol": row.symbol,
            "market": row.market,
            "currency": row.currency,
            "trade_date": row.trade_date.isoformat() if row.trade_date else "",
            "side": row.side,
            "event_type": (getattr(row, "event_type", None) or "trade"),
            "quantity": float(row.quantity),
            "price": float(row.price),
            "fee": float(row.fee),
            "tax": float(row.tax),
            "note": row.note,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _cash_ledger_row_to_dict(row: Any) -> Dict[str, Any]:
        return {
            "id": int(row.id),
            "account_id": int(row.account_id),
            "event_date": row.event_date.isoformat() if row.event_date else "",
            "direction": row.direction,
            "amount": float(row.amount),
            "currency": row.currency,
            "note": row.note,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _corporate_action_row_to_dict(row: Any) -> Dict[str, Any]:
        return {
            "id": int(row.id),
            "account_id": int(row.account_id),
            "symbol": row.symbol,
            "market": row.market,
            "currency": row.currency,
            "effective_date": row.effective_date.isoformat() if row.effective_date else "",
            "action_type": row.action_type,
            "cash_dividend_per_share": (
                float(row.cash_dividend_per_share) if row.cash_dividend_per_share is not None else None
            ),
            "split_ratio": float(row.split_ratio) if row.split_ratio is not None else None,
            "note": row.note,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _validate_paging(*, page: int, page_size: int) -> Tuple[int, int]:
        if page < 1:
            raise ValueError("page must be >= 1")
        if page_size < 1 or page_size > 100:
            raise ValueError("page_size must be in [1, 100]")
        return page, page_size

    @staticmethod
    def _normalize_market(value: str) -> str:
        market = (value or "").strip().lower()
        if market not in VALID_MARKETS:
            raise ValueError("market must be one of: cn, hk, us, jp, kr, tw")
        return market

    @staticmethod
    def _normalize_currency(value: str) -> str:
        currency = (value or "").strip().upper()
        if not currency:
            raise ValueError("currency is required")
        return currency

    @staticmethod
    def _normalize_cost_method(value: str) -> str:
        method = (value or "").strip().lower()
        if method not in VALID_COST_METHODS:
            raise ValueError("cost_method must be fifo or avg")
        return method

    @staticmethod
    def _default_currency_for_market(market: str) -> str:
        if market == "hk":
            return "HKD"
        if market == "us":
            return "USD"
        return "CNY"
