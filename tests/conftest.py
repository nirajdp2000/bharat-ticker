"""Shared pytest fixtures and mock objects for the test suite."""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.cache.redis_client import RedisManager
from src.db.connection import DatabaseManager
from src.models.tick import TickData, MarketDepth, MarketDepthLevel


# ── Mock Redis Client ────────────────────────────────────────────────────────

class MockRedisPipeline:
    """Mock for Redis Async Pipeline."""

    def __init__(self, client: MockRedisClient) -> None:
        self.client = client
        self.commands: list[tuple[str, tuple, dict]] = []

    def hset(self, key: str, mapping: dict) -> MockRedisPipeline:
        self.commands.append(("hset", (key, mapping), {}))
        return self

    def expire(self, key: str, seconds: int) -> MockRedisPipeline:
        self.commands.append(("expire", (key, seconds), {}))
        return self

    def xadd(self, key: str, fields: dict, maxlen: int | None = None) -> MockRedisPipeline:
        self.commands.append(("xadd", (key, fields), {"maxlen": maxlen}))
        return self

    def publish(self, channel: str, message: str) -> MockRedisPipeline:
        self.commands.append(("publish", (channel, message), {}))
        return self

    def hgetall(self, key: str) -> MockRedisPipeline:
        self.commands.append(("hgetall", (key,), {}))
        return self

    async def execute(self) -> list[any]:
        results = []
        for cmd, args, kwargs in self.commands:
            if cmd == "hgetall":
                key = args[0]
                results.append(await self.client.hgetall(key))
            else:
                results.append(True)
        return results


class MockRedisClient:
    """Mock for the async Redis client."""

    def __init__(self) -> None:
        self.data: dict[str, dict[str, str] | str] = {}
        self.expiry: dict[str, float] = {}

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        pass

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.data[key] = str(value)
        return True

    async def get(self, key: str) -> str | None:
        return self.data.get(key)

    async def hset(self, key: str, mapping: dict) -> int:
        if key not in self.data or not isinstance(self.data[key], dict):
            self.data[key] = {}
        self.data[key].update({k: str(v) for k, v in mapping.items()})
        return len(mapping)

    async def hgetall(self, key: str) -> dict[str, str]:
        val = self.data.get(key, {})
        return val if isinstance(val, dict) else {}

    async def expire(self, key: str, seconds: int) -> bool:
        return True

    async def xadd(self, key: str, fields: dict, maxlen: int | None = None) -> str:
        return "12345-0"

    async def publish(self, channel: str, message: str) -> int:
        return 1

    async def incr(self, key: str) -> int:
        val = self.data.get(key, "0")
        try:
            new_val = int(val) + 1
        except ValueError:
            new_val = 1
        self.data[key] = str(new_val)
        return new_val

    def pipeline(self, transaction: bool = False) -> MockRedisPipeline:
        return MockRedisPipeline(self)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_redis(monkeypatch) -> MockRedisClient:
    """Mock redis_manager's connection and client."""
    client = MockRedisClient()

    async def mock_connect():
        pass

    async def mock_disconnect():
        pass

    monkeypatch.setattr(RedisManager, "connect", mock_connect)
    monkeypatch.setattr(RedisManager, "disconnect", mock_disconnect)
    monkeypatch.setattr(RedisManager, "client", client)
    monkeypatch.setattr(RedisManager, "health_check", lambda self: asyncio.sleep(0.01) or True)

    return client


@pytest.fixture(autouse=True)
def mock_db(monkeypatch):
    """Mock database connection and session management."""
    mock_session = AsyncMock()
    mock_session_factory = MagicMock(return_value=mock_session)

    async def mock_connect():
        pass

    async def mock_disconnect():
        pass

    monkeypatch.setattr(DatabaseManager, "connect", mock_connect)
    monkeypatch.setattr(DatabaseManager, "disconnect", mock_disconnect)
    monkeypatch.setattr(DatabaseManager, "get_session", lambda self: mock_session)
    monkeypatch.setattr(DatabaseManager, "health_check", lambda self: asyncio.sleep(0.01) or True)

    return mock_session


@pytest.fixture
def sample_tick() -> TickData:
    """Return a validated TickData instance for tests."""
    return TickData(
        symbol="RELIANCE",
        isin="INE002A01018",
        exchange="NSE",
        series="EQ",
        ltp=Decimal("2945.50"),
        open=Decimal("2930.00"),
        high=Decimal("2958.75"),
        low=Decimal("2925.10"),
        close=Decimal("2935.25"),
        change=Decimal("10.25"),
        pct_change=Decimal("0.35"),
        volume=4523891,
        value=Decimal("13311567432.50"),
        vwap=Decimal("2941.87"),
        upper_circuit=Decimal("3228.75"),
        lower_circuit=Decimal("2641.75"),
        week_52_high=Decimal("3217.90"),
        week_52_low=Decimal("2220.30"),
        market_depth=MarketDepth(
            buy=[
                MarketDepthLevel(price=Decimal("2945.30"), quantity=1250, orders=12),
                MarketDepthLevel(price=Decimal("2945.15"), quantity=3400, orders=28),
            ],
            sell=[
                MarketDepthLevel(price=Decimal("2945.50"), quantity=980, orders=8),
                MarketDepthLevel(price=Decimal("2945.70"), quantity=2200, orders=15),
            ],
            total_buy_quantity=4650,
            total_sell_quantity=3180,
        ),
        timestamp=datetime.now(),
        source="nse_scraper",
        source_latency_ms=145.3,
    )
