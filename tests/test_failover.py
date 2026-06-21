"""Unit tests for the FailoverController."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from src.engine.failover import FailoverController
from src.providers.base import DataProvider, ProviderError
from src.models.tick import TickData


class MockProvider(DataProvider):
    """Simple mock data provider for testing failover."""

    def __init__(self, name: str, tier: int, exchange: str = "NSE") -> None:
        super().__init__()
        self.name = name
        self.tier = tier
        self.exchange = exchange
        self.fetch_quote = AsyncMock()
        self.fetch_bulk = AsyncMock()
        self.health_check = AsyncMock(return_value=True)

    async def connect(self) -> None:
        self._is_connected = True

    async def disconnect(self) -> None:
        self._is_connected = False


@pytest.mark.asyncio
async def test_failover_registration_and_sorting():
    """Verify that providers are correctly sorted by tier and name."""
    fc = FailoverController()
    p1 = MockProvider("nse_scraper", tier=1)
    p2 = MockProvider("yahoo_finance", tier=3)
    p3 = MockProvider("fyers_ws", tier=2)

    fc.register_provider(p2)
    fc.register_provider(p1)
    fc.register_provider(p3)

    # Sorted order should be: tier 1 (nse_scraper) -> tier 2 (fyers_ws) -> tier 3 (yahoo_finance)
    assert fc.providers[0] == p1
    assert fc.providers[1] == p3
    assert fc.providers[2] == p2


@pytest.mark.asyncio
async def test_failover_success_on_primary(sample_tick):
    """Verify that the primary (Tier-1) provider is used when healthy."""
    fc = FailoverController()
    p1 = MockProvider("nse_scraper", tier=1)
    p2 = MockProvider("yahoo_finance", tier=3)

    fc.register_provider(p1)
    fc.register_provider(p2)

    p1.fetch_quote.return_value = sample_tick

    res = await fc.fetch_quote("RELIANCE")
    assert res == sample_tick
    p1.fetch_quote.assert_called_once_with("RELIANCE")
    p2.fetch_quote.assert_not_called()


@pytest.mark.asyncio
async def test_failover_fallback_cascading(sample_tick):
    """Verify that when Tier-1 fails, it cascades to Tier-3."""
    fc = FailoverController()
    p1 = MockProvider("nse_scraper", tier=1)
    p2 = MockProvider("yahoo_finance", tier=3)

    fc.register_provider(p1)
    fc.register_provider(p2)

    # Primary fails
    p1.fetch_quote.side_effect = ProviderError("nse_scraper", "WAF blocked")
    # Secondary succeeds
    p2.fetch_quote.return_value = sample_tick

    res = await fc.fetch_quote("RELIANCE")
    assert res == sample_tick
    p1.fetch_quote.assert_called_once_with("RELIANCE")
    p2.fetch_quote.assert_called_once_with("RELIANCE")


@pytest.mark.asyncio
async def test_failover_all_exhausted():
    """Verify that if all providers fail, a ProviderError is raised."""
    fc = FailoverController()
    p1 = MockProvider("nse_scraper", tier=1)
    p2 = MockProvider("yahoo_finance", tier=3)

    fc.register_provider(p1)
    fc.register_provider(p2)

    p1.fetch_quote.side_effect = ProviderError("nse_scraper", "Network error")
    p2.fetch_quote.side_effect = Exception("Unexpected error")

    with pytest.raises(ProviderError) as excinfo:
        await fc.fetch_quote("RELIANCE")

    assert "All providers failed" in str(excinfo.value)
