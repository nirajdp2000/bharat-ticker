"""Real-time tick data models with strict Pydantic v2 validation.

These models define the canonical shape of every market data point flowing
through the system.  Every provider (NSE scraper, broker WS, Yahoo, etc.)
MUST normalise its raw response into a ``TickData`` instance before the
data enters the pipeline.  This is the single source of truth for schema
validation — if a provider returns data that cannot be parsed into this
model, the circuit-breaker fires.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator


# ── Market Depth (Level-2 Order Book) ────────────────────────────────────────

class MarketDepthLevel(BaseModel):
    """A single price level in the order book."""

    price: Decimal = Field(..., description="Price at this level")
    quantity: int = Field(..., ge=0, description="Aggregate quantity at this level")
    orders: int = Field(..., ge=0, description="Number of orders at this level")

    class Config:
        json_encoders = {Decimal: str}


class MarketDepth(BaseModel):
    """5-level order book snapshot."""

    buy: list[MarketDepthLevel] = Field(
        default_factory=list,
        max_length=20,
        description="Best bid levels (highest price first)",
    )
    sell: list[MarketDepthLevel] = Field(
        default_factory=list,
        max_length=20,
        description="Best ask levels (lowest price first)",
    )
    total_buy_quantity: int = Field(default=0, ge=0)
    total_sell_quantity: int = Field(default=0, ge=0)

    @property
    def imbalance_ratio(self) -> float:
        """Order imbalance: positive = more buying pressure."""
        total = self.total_buy_quantity + self.total_sell_quantity
        if total == 0:
            return 0.0
        return round((self.total_buy_quantity - self.total_sell_quantity) / total, 4)

    @property
    def spread(self) -> Decimal | None:
        """Best ask - best bid spread."""
        if self.buy and self.sell:
            return self.sell[0].price - self.buy[0].price
        return None


# ── Core Tick Data ───────────────────────────────────────────────────────────

class TickData(BaseModel):
    """Canonical real-time tick data structure.

    Every data provider must normalise its output into this schema.
    Fields marked Optional (None) are populated when available; Tier-3
    providers (Yahoo) may not supply market depth or VWAP.
    """

    # ── Identity ─────────────────────────────────────────────────────────
    symbol: str = Field(..., min_length=1, max_length=30, description="Exchange symbol (e.g. RELIANCE)")
    isin: str | None = Field(None, pattern=r"^INE[A-Z0-9]{9}$", description="ISIN code")
    exchange: Literal["NSE", "BSE"] = Field(..., description="Source exchange")
    series: str | None = Field(None, description="Trading series (EQ, BE, etc.)")

    # ── Price ────────────────────────────────────────────────────────────
    ltp: Decimal = Field(..., ge=0, description="Last Traded Price")
    open: Decimal = Field(..., ge=0, description="Day open price")
    high: Decimal = Field(..., ge=0, description="Day high price")
    low: Decimal = Field(..., ge=0, description="Day low price")
    close: Decimal = Field(..., ge=0, description="Previous day close")
    change: Decimal = Field(default=Decimal("0"), description="Absolute price change from close")
    pct_change: Decimal = Field(default=Decimal("0"), description="Percentage change from close")

    # ── Volume & Value ───────────────────────────────────────────────────
    volume: int = Field(default=0, ge=0, description="Total traded volume")
    value: Decimal | None = Field(None, ge=0, description="Total traded value in INR")
    vwap: Decimal | None = Field(None, ge=0, description="Volume Weighted Average Price")

    # ── Circuit Limits ───────────────────────────────────────────────────
    upper_circuit: Decimal | None = Field(None, ge=0, description="Upper circuit limit")
    lower_circuit: Decimal | None = Field(None, ge=0, description="Lower circuit limit")

    # ── Order-book aggregates / derivatives (populated when the source has them) ──
    total_buy_qty: int | None = Field(None, ge=0, description="Total bid quantity")
    total_sell_qty: int | None = Field(None, ge=0, description="Total ask quantity")
    open_interest: int | None = Field(None, ge=0, description="Open interest (F&O)")

    # ── 52-Week Range ────────────────────────────────────────────────────
    week_52_high: Decimal | None = Field(None, ge=0)
    week_52_low: Decimal | None = Field(None, ge=0)

    # ── Market Depth ─────────────────────────────────────────────────────
    market_depth: MarketDepth | None = Field(None, description="Level-2 order book")

    # ── Metadata ─────────────────────────────────────────────────────────
    timestamp: datetime = Field(..., description="Exchange timestamp of this tick")
    source: str = Field(..., description="Provider that produced this tick")
    source_latency_ms: float = Field(default=0.0, ge=0, description="Provider response time in ms")

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("change", mode="before")
    @classmethod
    def compute_change(cls, v: Decimal | None, info) -> Decimal:
        if v is not None:
            return v
        data = info.data
        if "ltp" in data and "close" in data:
            return data["ltp"] - data["close"]
        return Decimal("0")

    class Config:
        json_encoders = {Decimal: str, datetime: lambda v: v.isoformat()}


# ── Compact Tick (for Redis Streams / high-frequency transfer) ───────────────

class CompactTick(BaseModel):
    """Minimal tick for Redis Streams — only the essential price fields."""

    symbol: str
    exchange: Literal["NSE", "BSE"]
    ltp: Decimal
    volume: int
    timestamp: datetime
    source: str

    class Config:
        json_encoders = {Decimal: str, datetime: lambda v: v.isoformat()}
