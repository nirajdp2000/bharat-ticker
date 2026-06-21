"""Unit tests for the YahooFinanceProvider."""

from __future__ import annotations

from decimal import Decimal
import pytest

from src.providers.yahoo_finance import YahooFinanceProvider
from src.models.tick import TickData


def test_parse_yahoo_data():
    """Verify parsing of yfinance ticker info dictionaries into TickData."""
    raw_info = {
        "regularMarketPrice": 2945.50,
        "regularMarketOpen": 2930.00,
        "regularMarketDayHigh": 2958.75,
        "regularMarketDayLow": 2925.10,
        "regularMarketPreviousClose": 2935.25,
        "regularMarketVolume": 4523891,
        "fiftyTwoWeekHigh": 3217.90,
        "fiftyTwoWeekLow": 2220.30,
    }

    provider = YahooFinanceProvider()
    tick = provider._parse_yahoo_data(raw_info, "RELIANCE", latency_ms=120.0)

    assert isinstance(tick, TickData)
    assert tick.symbol == "RELIANCE"
    assert tick.exchange == "NSE"
    assert tick.ltp == Decimal("2945.50")
    assert tick.open == Decimal("2930.00")
    assert tick.high == Decimal("2958.75")
    assert tick.low == Decimal("2925.10")
    assert tick.close == Decimal("2935.25")
    assert tick.volume == 4523891
    assert tick.change == Decimal("10.25")
    assert tick.pct_change == Decimal("0.35")
    assert tick.week_52_high == Decimal("3217.90")
    assert tick.week_52_low == Decimal("2220.30")
    assert tick.market_depth is None  # Yahoo has no depth
