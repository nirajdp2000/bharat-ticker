"""Instrument / symbol metadata models."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class Instrument(BaseModel):
    """Master instrument record — seeded from exchange master files."""

    symbol: str = Field(..., min_length=1, max_length=30)
    isin: str | None = Field(None, pattern=r"^INE[A-Z0-9]{9}$")
    name: str = Field(..., min_length=1)
    exchange: Literal["NSE", "BSE"]
    series: str | None = Field(None, description="EQ, BE, BZ, etc.")
    sector: str | None = None
    industry: str | None = None
    lot_size: int = Field(default=1, ge=1)
    face_value: Decimal | None = Field(None, ge=0)
    listing_date: date | None = None
    is_active: bool = True
    updated_at: datetime | None = None

    class Config:
        json_encoders = {Decimal: str, date: str, datetime: lambda v: v.isoformat()}


class InstrumentSearchResult(BaseModel):
    """Lightweight result for the /search endpoint."""

    symbol: str
    name: str
    exchange: Literal["NSE", "BSE"]
    series: str | None = None
    sector: str | None = None
    is_active: bool = True
