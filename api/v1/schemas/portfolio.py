# -*- coding: utf-8 -*-
"""Portfolio API schemas."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class PortfolioAccountCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    broker: Optional[str] = Field(None, max_length=64)
    market: Literal["cn", "hk", "us", "jp", "kr", "tw"] = "cn"
    base_currency: str = Field("CNY", min_length=3, max_length=8)
    owner_id: Optional[str] = Field(None, max_length=64)


class PortfolioAccountUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=64)
    broker: Optional[str] = Field(None, max_length=64)
    market: Optional[Literal["cn", "hk", "us", "jp", "kr", "tw"]] = None
    base_currency: Optional[str] = Field(None, min_length=3, max_length=8)
    owner_id: Optional[str] = Field(None, max_length=64)
    is_active: Optional[bool] = None


class PortfolioAccountItem(BaseModel):
    id: int
    owner_id: Optional[str] = None
    name: str
    broker: Optional[str] = None
    market: str
    base_currency: str
    is_active: bool
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class PortfolioAccountListResponse(BaseModel):
    accounts: List[PortfolioAccountItem] = Field(default_factory=list)


class PortfolioTradeCreateRequest(BaseModel):
    account_id: int
    symbol: str = Field(..., min_length=1, max_length=16)
    trade_date: date
    side: Literal["buy", "sell"]
    quantity: float = Field(..., gt=0)
    price: float = Field(..., gt=0)
    fee: float = Field(0.0, ge=0)
    tax: float = Field(0.0, ge=0)
    market: Optional[Literal["cn", "hk", "us", "jp", "kr", "tw"]] = None
    currency: Optional[str] = Field(None, min_length=3, max_length=8)
    trade_uid: Optional[str] = Field(None, max_length=128)
    note: Optional[str] = Field(None, max_length=255)
    event_type: Literal["trade", "baseline", "init"] = "trade"


class PortfolioBaselinePosition(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=16)
    quantity: float = Field(..., gt=0)
    # None/0 = cost unknown at confirmation time: the position is kept but its
    # unrealized PnL / pct are not computed (R2).
    cost_per_unit: Optional[float] = Field(None, ge=0)
    # Optional confirmed instrument type — recorded into the identity registry
    # used for valuation routing (R3/F06).
    asset_kind: Optional[Literal["stock", "etf", "index", "otc_fund"]] = None
    market: Optional[Literal["cn", "hk", "us", "jp", "kr", "tw"]] = None
    currency: Optional[str] = Field(None, min_length=3, max_length=8)


class PortfolioBaselineCreateRequest(BaseModel):
    account_id: int
    baseline_date: date
    # Empty positions = confirmed empty account; the service then requires cash
    # to be declared, otherwise there is nothing to anchor the baseline.
    positions: List[PortfolioBaselinePosition] = Field(default_factory=list)
    cash: Optional[float] = Field(None, ge=0)
    cash_currency: Optional[str] = Field(None, min_length=3, max_length=8)
    note: Optional[str] = Field(None, max_length=255)


class PortfolioBaselinePositionResult(BaseModel):
    symbol: str
    id: int
    duplicate: bool


class PortfolioBaselineCashResult(BaseModel):
    amount: float
    currency: str
    duplicate: bool


class PortfolioBaselineResponse(BaseModel):
    account_id: int
    baseline_date: str
    positions: List[PortfolioBaselinePositionResult] = Field(default_factory=list)
    cash: Optional[PortfolioBaselineCashResult] = None
    duplicate: bool = False


class PortfolioInstrumentIdentityRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=16)
    kind: Literal["stock", "etf", "index", "otc_fund"]
    market: str = "cn"
    currency: str = "CNY"
    confirmed_by: Optional[str] = Field(None, max_length=64)
    note: Optional[str] = Field(None, max_length=255)


class PortfolioInstrumentIdentityItem(BaseModel):
    symbol: str
    kind: str
    market: str
    currency: str
    source: Optional[str] = None
    confirmed_by: Optional[str] = None
    confirmed_at: Optional[str] = None
    note: Optional[str] = None


class PortfolioInstrumentIdentityResponse(BaseModel):
    ok: bool = True
    identity: PortfolioInstrumentIdentityItem


class PortfolioCashLedgerCreateRequest(BaseModel):
    account_id: int
    event_date: date
    direction: Literal["in", "out"]
    amount: float = Field(..., gt=0)
    currency: Optional[str] = Field(None, min_length=3, max_length=8)
    note: Optional[str] = Field(None, max_length=255)


class PortfolioCorporateActionCreateRequest(BaseModel):
    account_id: int
    symbol: str = Field(..., min_length=1, max_length=16)
    effective_date: date
    action_type: Literal["cash_dividend", "split_adjustment"]
    market: Optional[Literal["cn", "hk", "us", "jp", "kr", "tw"]] = None
    currency: Optional[str] = Field(None, min_length=3, max_length=8)
    cash_dividend_per_share: Optional[float] = Field(None, ge=0)
    split_ratio: Optional[float] = Field(None, gt=0)
    note: Optional[str] = Field(None, max_length=255)


class PortfolioEventCreatedResponse(BaseModel):
    id: int


class PortfolioDeleteResponse(BaseModel):
    deleted: int


class PortfolioTradeListItem(BaseModel):
    id: int
    account_id: int
    trade_uid: Optional[str] = None
    symbol: str
    market: str
    currency: str
    trade_date: str
    side: str
    event_type: str = "trade"
    quantity: float
    price: float
    fee: float
    tax: float
    note: Optional[str] = None
    created_at: Optional[str] = None


class PortfolioTradeListResponse(BaseModel):
    items: List[PortfolioTradeListItem] = Field(default_factory=list)
    total: int
    page: int
    page_size: int


class PortfolioCashLedgerListItem(BaseModel):
    id: int
    account_id: int
    event_date: str
    direction: str
    amount: float
    currency: str
    note: Optional[str] = None
    created_at: Optional[str] = None


class PortfolioCashLedgerListResponse(BaseModel):
    items: List[PortfolioCashLedgerListItem] = Field(default_factory=list)
    total: int
    page: int
    page_size: int


class PortfolioCorporateActionListItem(BaseModel):
    id: int
    account_id: int
    symbol: str
    market: str
    currency: str
    effective_date: str
    action_type: str
    cash_dividend_per_share: Optional[float] = None
    split_ratio: Optional[float] = None
    note: Optional[str] = None
    created_at: Optional[str] = None


class PortfolioCorporateActionListResponse(BaseModel):
    items: List[PortfolioCorporateActionListItem] = Field(default_factory=list)
    total: int
    page: int
    page_size: int


class PortfolioPositionItem(BaseModel):
    symbol: str
    market: str
    currency: str
    quantity: float
    avg_cost: float
    total_cost: float
    last_price: float
    market_value_base: float
    unrealized_pnl_base: float
    unrealized_pnl_pct: Optional[float] = None
    valuation_currency: str
    price_source: str = "unknown"
    price_provider: Optional[str] = None
    price_date: Optional[str] = None
    price_stale: bool = False
    price_available: bool = True
    price_kind: str = "close"
    price_provider_time: Optional[str] = None
    price_fetched_at: Optional[str] = None
    price_time_quality: str = "valid"
    # R4/F09: shared-upstream disclosure — akshare ETF / efinance ETF / fund
    # NAV all ride eastmoney public endpoints and are NOT independent sources.
    price_upstream_group: Optional[str] = None
    # True = quantity kept but the confirmed cost is unknown: unrealized PnL is
    # reported as not-computed, never as market value or fake zero-PnL (R2).
    cost_basis_unknown: bool = False
    data_quality: str = "ok"
    limitations: List[str] = Field(default_factory=list)


class PortfolioValuationCoverage(BaseModel):
    valued: int
    total: int
    unvalued_symbols: List[str] = Field(default_factory=list)


class PortfolioPositionAnalysisRequest(BaseModel):
    account_id: Optional[int] = Field(None, description="Optional account id; required when a symbol is held in multiple accounts")
    analysis_phase: Literal["auto", "premarket", "intraday", "postmarket"] = "auto"
    force: bool = Field(False, description="Force refresh analysis inputs without bypassing duplicate in-flight tasks")


class PortfolioAccountSnapshot(BaseModel):
    account_id: int
    account_name: str
    owner_id: Optional[str] = None
    broker: Optional[str] = None
    market: str
    base_currency: str
    as_of: str
    cost_method: str
    # None = cash unknown at the active baseline (never substituted with 0);
    # total_equity is then not computed either (R2/F02).
    total_cash: Optional[float] = None
    total_market_value: float
    total_market_value_aggregate: Optional[float] = Field(
        None, description="Account market value converted into the parent snapshot currency"
    )
    total_equity: Optional[float] = None
    realized_pnl: float
    unrealized_pnl: float
    fee_total: float
    tax_total: float
    fx_stale: bool
    cash_known: bool = True
    data_quality: str = "ok"
    limitations: List[str] = Field(default_factory=list)
    positions: List[PortfolioPositionItem] = Field(default_factory=list)
    valuation_coverage: Optional[PortfolioValuationCoverage] = None
    baseline_date: Optional[str] = None
    event_type_counts: Dict[str, int] = Field(default_factory=dict)
    superseded_event_count: Optional[int] = None


class PortfolioQuoteRef(BaseModel):
    account_id: int
    symbol: str
    price: Optional[float] = None
    provider: Optional[str] = None
    price_kind: Optional[str] = None
    provider_time: Optional[str] = None
    price_date: Optional[str] = None
    time_quality: Optional[str] = None
    price_stale: bool = False


class PortfolioSnapshotResponse(BaseModel):
    as_of: str
    cost_method: str
    currency: str
    account_count: int
    # R3: one identity for the quotes this response was built from; positions,
    # totals and consumers (reports/alerts) reference the same valuation.
    valuation_id: Optional[str] = None
    quote_refs: List[PortfolioQuoteRef] = Field(default_factory=list)
    total_cash: Optional[float] = None
    total_market_value: float
    total_equity: Optional[float] = None
    realized_pnl: float
    unrealized_pnl: float
    fee_total: float
    tax_total: float
    fx_stale: bool
    cash_known: bool = True
    data_quality: str = "ok"
    limitations: List[str] = Field(default_factory=list)
    accounts: List[PortfolioAccountSnapshot] = Field(default_factory=list)


class PortfolioImportTradeItem(BaseModel):
    trade_date: str
    symbol: str
    side: Literal["buy", "sell"]
    quantity: float
    price: float
    fee: float
    tax: float
    trade_uid: Optional[str] = None
    dedup_hash: str
    currency: Optional[str] = None


class PortfolioImportParseResponse(BaseModel):
    broker: str
    record_count: int
    skipped_count: int
    error_count: int
    records: List[PortfolioImportTradeItem] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


class PortfolioImportCommitResponse(BaseModel):
    account_id: int
    record_count: int
    inserted_count: int
    duplicate_count: int
    failed_count: int
    dry_run: bool
    errors: List[str] = Field(default_factory=list)


class PortfolioImportBrokerItem(BaseModel):
    broker: str
    aliases: List[str] = Field(default_factory=list)
    display_name: Optional[str] = None


class PortfolioImportBrokerListResponse(BaseModel):
    brokers: List[PortfolioImportBrokerItem] = Field(default_factory=list)


class PortfolioFxRefreshResponse(BaseModel):
    as_of: str
    account_count: int
    refresh_enabled: bool
    disabled_reason: Optional[str] = None
    pair_count: int
    updated_count: int
    stale_count: int
    error_count: int


class PortfolioDecisionSignalRiskItem(BaseModel):
    account_id: Optional[int] = None
    symbol: str
    market: str
    signal: Dict[str, Any] = Field(default_factory=dict)


class PortfolioDecisionSignalRiskBlock(BaseModel):
    available: bool = True
    total: int = 0
    actions: Dict[str, int] = Field(default_factory=dict)
    items: List[PortfolioDecisionSignalRiskItem] = Field(default_factory=list)


class PortfolioRiskResponse(BaseModel):
    valuation_id: Optional[str] = None
    quote_refs: List[Dict[str, Any]] = Field(default_factory=list)
    data_quality: str = "unknown"
    limitations: List[str] = Field(default_factory=list)
    as_of: str
    account_id: Optional[int] = None
    cost_method: str
    currency: str
    thresholds: Dict[str, Any] = Field(default_factory=dict)
    concentration: Dict[str, Any] = Field(default_factory=dict)
    sector_concentration: Dict[str, Any] = Field(default_factory=dict)
    drawdown: Dict[str, Any] = Field(default_factory=dict)
    stop_loss: Dict[str, Any] = Field(default_factory=dict)
    decision_signal_risk: PortfolioDecisionSignalRiskBlock = Field(default_factory=PortfolioDecisionSignalRiskBlock)
