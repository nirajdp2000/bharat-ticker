"""Unit tests for the NSEScraper."""

from __future__ import annotations

from decimal import Decimal
import pytest

from src.providers.nse_scraper import NSEScraper
from src.models.tick import TickData


def test_safe_decimal():
    """Verify safe decimal conversions from strings, floats, and invalid values."""
    assert NSEScraper._safe_decimal("2,945.50") == Decimal("2945.50")
    assert NSEScraper._safe_decimal(" - ") == Decimal("0")
    assert NSEScraper._safe_decimal(None) == Decimal("0")
    assert NSEScraper._safe_decimal(150.25) == Decimal("150.25")
    assert NSEScraper._safe_decimal("invalid") == Decimal("0")


def test_parse_nse_quote():
    """Verify parsing of internal NSE JSON quote responses into TickData."""
    raw_response = {
        "info": {
            "symbol": "RELIANCE",
            "isin": "INE002A01018",
            "series": "EQ",
        },
        "priceInfo": {
            "lastPrice": 2945.50,
            "open": 2930.00,
            "intraDayHighLow": {"max": 2958.75, "min": 2925.10},
            "previousClose": 2935.25,
            "change": 10.25,
            "pChange": 0.35,
            "vwap": 2941.87,
            "upperCP": 3228.75,
            "lowerCP": 2641.75,
            "weekHighLow": {"max": 3217.90, "min": 2220.30},
        },
        "securityInfo": {
            "tradedVolume": 4523891,
            "totalTradedValue": 13311567432.50,
        },
        "marketDeptOrderBook": {
            "totalBuyQuantity": 21250,
            "totalSellQuantity": 15380,
            "bid": [
                {"price": 2945.30, "quantity": 1250, "number": 12},
                {"price": 2945.15, "quantity": 3400, "number": 28},
            ],
            "ask": [
                {"price": 2945.50, "quantity": 980, "number": 8},
                {"price": 2945.70, "quantity": 2200, "number": 15},
            ],
        },
    }

    scraper = NSEScraper()
    tick = scraper._parse_nse_quote(raw_response, "RELIANCE", latency_ms=45.0)

    assert isinstance(tick, TickData)
    assert tick.symbol == "RELIANCE"
    assert tick.isin == "INE002A01018"
    assert tick.ltp == Decimal("2945.50")
    assert tick.open == Decimal("2930.00")
    assert tick.high == Decimal("2958.75")
    assert tick.low == Decimal("2925.10")
    assert tick.close == Decimal("2935.25")
    assert tick.volume == 4523891
    assert tick.value == Decimal("13311567432.50")
    assert tick.vwap == Decimal("2941.87")
    assert tick.upper_circuit == Decimal("3228.75")
    assert tick.lower_circuit == Decimal("2641.75")
    assert tick.week_52_high == Decimal("3217.90")
    assert tick.week_52_low == Decimal("2220.30")

    # Depth check
    assert tick.market_depth is not None
    assert tick.market_depth.total_buy_quantity == 21250
    assert tick.market_depth.total_sell_quantity == 15380
    assert len(tick.market_depth.buy) == 2
    assert tick.market_depth.buy[0].price == Decimal("2945.30")
    assert tick.market_depth.buy[0].quantity == 1250
    assert tick.market_depth.buy[0].orders == 12
    assert tick.market_depth.imbalance_ratio > 0  # more buy than sell
