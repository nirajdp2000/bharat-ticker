"""API response envelope models — deeply nested, comprehensive JSON output.

Every API endpoint wraps its data in an ``APIResponse`` envelope that
includes market state, metadata, and data quality indicators alongside
the actual payload.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


# ── Market State ─────────────────────────────────────────────────────────────

class MarketState(BaseModel):
    """Current market session information."""

    status: Literal["OPEN", "CLOSED", "PRE_OPEN", "POST_CLOSE"] = "CLOSED"
    session: str = Field(
        default="CLOSED",
        description="Human-readable session name",
    )
    exchange_time: datetime | None = Field(None, description="Last known exchange time")
    next_open: datetime | None = None
    next_close: datetime | None = None
    is_holiday: bool = False
    holiday_name: str | None = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── Response Metadata ────────────────────────────────────────────────────────

class ResponseMeta(BaseModel):
    """Metadata about the response — source, latency, data quality."""

    source: str = Field(..., description="Data provider (nse_scraper, fyers_ws, yahoo, etc.)")
    source_latency_ms: float = Field(default=0.0, ge=0, description="Provider response time")
    cache_age_ms: float = Field(default=0.0, ge=0, description="Age of cached data")
    data_quality: Literal[
        "REAL_TIME", "NEAR_REAL_TIME", "DELAYED", "END_OF_DAY", "STALE"
    ] = "REAL_TIME"
    last_refresh: datetime | None = None
    provider_tier: int = Field(default=1, ge=0, le=3, description="Provider tier (0=persistent DB, 1=best live)")

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── Provider Health ──────────────────────────────────────────────────────────

class ProviderStatus(BaseModel):
    """Health status of a single data provider."""

    name: str
    tier: int = Field(ge=1, le=3)
    status: Literal["HEALTHY", "DEGRADED", "DOWN"] = "HEALTHY"
    circuit_state: Literal["CLOSED", "OPEN", "HALF_OPEN"] = "CLOSED"
    last_success: datetime | None = None
    last_error: str | None = None
    error_count: int = Field(default=0, ge=0)
    avg_latency_ms: float = Field(default=0.0, ge=0)


# ── Technical Indicators (computed server-side when available) ───────────────

class TechnicalIndicators(BaseModel):
    """Optional technical indicators — populated from historical data."""

    ema_20: Decimal | None = None
    sma_50: Decimal | None = None
    sma_200: Decimal | None = None
    rsi_14: float | None = Field(None, ge=0, le=100)
    atr_14: Decimal | None = None
    beta: float | None = None
    pe_ratio: float | None = None
    pb_ratio: float | None = None
    market_cap: Decimal | None = None
    dividend_yield: float | None = None

    class Config:
        json_encoders = {Decimal: str}


# ── Nested Quote Data ────────────────────────────────────────────────────────

class QuoteInfo(BaseModel):
    """Instrument identification block."""

    symbol: str
    name: str | None = None
    isin: str | None = None
    exchange: Literal["NSE", "BSE"]
    series: str | None = None
    sector: str | None = None
    industry: str | None = None
    face_value: Decimal | None = None
    listing_date: str | None = None


class QuotePrice(BaseModel):
    """Price block."""

    ltp: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    change: Decimal
    pct_change: Decimal
    vwap: Decimal | None = None
    upper_circuit: Decimal | None = None
    lower_circuit: Decimal | None = None
    week_52_high: Decimal | None = None
    week_52_low: Decimal | None = None

    class Config:
        json_encoders = {Decimal: str}


class QuoteVolume(BaseModel):
    """Volume & value block."""

    total_traded_volume: int = 0
    total_traded_value: Decimal | None = None
    delivery_quantity: int | None = None
    delivery_percentage: Decimal | None = None
    total_market_cap: Decimal | None = None

    class Config:
        json_encoders = {Decimal: str}


class QuoteDepthLevel(BaseModel):
    price: Decimal
    quantity: int
    orders: int

    class Config:
        json_encoders = {Decimal: str}


class QuoteDepth(BaseModel):
    """Market depth block for the API response."""

    buy: list[QuoteDepthLevel] = Field(default_factory=list)
    sell: list[QuoteDepthLevel] = Field(default_factory=list)
    total_buy_quantity: int = 0
    total_sell_quantity: int = 0
    imbalance_ratio: float = 0.0


class FullQuoteData(BaseModel):
    """Complete quote payload — deeply nested."""

    info: QuoteInfo
    price: QuotePrice
    volume: QuoteVolume
    market_depth: QuoteDepth | None = None
    indicators: TechnicalIndicators | None = None


# ── Generic API Response Envelope ────────────────────────────────────────────

class APIResponse(BaseModel, Generic[T]):
    """Standard API response wrapper used by all endpoints."""

    status: Literal["success", "error", "partial"] = "success"
    timestamp: datetime = Field(default_factory=datetime.now)
    market_state: MarketState
    data: T
    meta: ResponseMeta
    errors: list[str] | None = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat(), Decimal: str}


class ErrorResponse(BaseModel):
    """Error response body."""

    status: Literal["error"] = "error"
    timestamp: datetime = Field(default_factory=datetime.now)
    error: str
    detail: str | None = None
    code: int = 500

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class BulkQuoteResponse(BaseModel):
    """Response for /quotes?symbols=... bulk endpoint."""

    status: Literal["success", "partial", "error"] = "success"
    timestamp: datetime = Field(default_factory=datetime.now)
    market_state: MarketState
    data: list[FullQuoteData]
    failed_symbols: list[str] = Field(default_factory=list)
    meta: ResponseMeta

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat(), Decimal: str}
