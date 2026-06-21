"""OHLCV candle models for historical data and aggregated views."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class OHLCVCandle(BaseModel):
    """Single OHLCV candle — produced by TimescaleDB continuous aggregates."""

    timestamp: datetime = Field(..., description="Candle open time (bucket start)")
    symbol: str
    exchange: Literal["NSE", "BSE"]
    open: Decimal = Field(..., ge=0)
    high: Decimal = Field(..., ge=0)
    low: Decimal = Field(..., ge=0)
    close: Decimal = Field(..., ge=0)
    volume: int = Field(default=0, ge=0)
    value: Decimal | None = Field(None, ge=0, description="Traded value in INR")
    vwap: Decimal | None = Field(None, ge=0)

    @property
    def body_size(self) -> Decimal:
        return abs(self.close - self.open)

    @property
    def range_size(self) -> Decimal:
        return self.high - self.low

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open

    class Config:
        json_encoders = {Decimal: str, datetime: lambda v: v.isoformat()}


class EODData(BaseModel):
    """End-of-Day summary — persisted daily after market close."""

    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$", description="YYYY-MM-DD")
    symbol: str
    exchange: Literal["NSE", "BSE"]
    open: Decimal = Field(..., ge=0)
    high: Decimal = Field(..., ge=0)
    low: Decimal = Field(..., ge=0)
    close: Decimal = Field(..., ge=0)
    vwap: Decimal | None = Field(None, ge=0)
    volume: int = Field(default=0, ge=0)
    value: Decimal | None = Field(None, ge=0)
    delivery_qty: int | None = Field(None, ge=0, description="Delivery quantity")
    delivery_pct: Decimal | None = Field(None, ge=0, le=100, description="Delivery %")
    trades: int | None = Field(None, ge=0, description="Number of trades")

    class Config:
        json_encoders = {Decimal: str}


class CandleInterval(BaseModel):
    """Supported candle intervals for the /historical endpoint."""

    interval: Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"] = "1d"

    @property
    def timescale_bucket(self) -> str:
        """Map interval to TimescaleDB time_bucket argument."""
        mapping = {
            "1m": "1 minute",
            "5m": "5 minutes",
            "15m": "15 minutes",
            "30m": "30 minutes",
            "1h": "1 hour",
            "4h": "4 hours",
            "1d": "1 day",
            "1w": "1 week",
            "1M": "1 month",
        }
        return mapping[self.interval]
