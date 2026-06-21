"""Market depth (order book) models."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field

from .tick import MarketDepth, MarketDepthLevel


class FullMarketDepth(BaseModel):
    """Extended market depth response with derived analytics."""

    symbol: str
    exchange: Literal["NSE", "BSE"]
    depth: MarketDepth
    timestamp: datetime

    # ── Derived Analytics ────────────────────────────────────────────────
    spread: Decimal | None = Field(None, description="Best ask - best bid")
    spread_pct: Decimal | None = Field(None, description="Spread as % of mid price")
    mid_price: Decimal | None = Field(None, description="(Best bid + Best ask) / 2")
    imbalance_ratio: float = Field(default=0.0, description="Buy-sell imbalance [-1, 1]")
    weighted_bid_price: Decimal | None = Field(None, description="Volume-weighted avg bid")
    weighted_ask_price: Decimal | None = Field(None, description="Volume-weighted avg ask")

    @classmethod
    def from_market_depth(
        cls,
        symbol: str,
        exchange: Literal["NSE", "BSE"],
        depth: MarketDepth,
        timestamp: datetime,
    ) -> "FullMarketDepth":
        """Factory: compute all derived fields from raw depth."""
        spread = depth.spread
        mid_price = None
        spread_pct = None
        weighted_bid = None
        weighted_ask = None

        if depth.buy and depth.sell:
            mid = (depth.buy[0].price + depth.sell[0].price) / 2
            mid_price = mid
            if mid > 0 and spread is not None:
                spread_pct = (spread / mid) * 100

        # Volume-weighted average prices
        weighted_bid = _weighted_avg(depth.buy)
        weighted_ask = _weighted_avg(depth.sell)

        return cls(
            symbol=symbol,
            exchange=exchange,
            depth=depth,
            timestamp=timestamp,
            spread=spread,
            spread_pct=spread_pct,
            mid_price=mid_price,
            imbalance_ratio=depth.imbalance_ratio,
            weighted_bid_price=weighted_bid,
            weighted_ask_price=weighted_ask,
        )

    class Config:
        json_encoders = {Decimal: str, datetime: lambda v: v.isoformat()}


def _weighted_avg(levels: list[MarketDepthLevel]) -> Decimal | None:
    """Compute volume-weighted average price from order book levels."""
    if not levels:
        return None
    total_qty = sum(l.quantity for l in levels)
    if total_qty == 0:
        return None
    return sum(l.price * l.quantity for l in levels) / total_qty
