"""Real-time ticker cache operations — read/write to Redis Hashes.

This module provides the zero-latency cache layer. Every tick from the
scraping engine is written here, and every API read comes from here
during market hours.
"""

from __future__ import annotations

import time
from datetime import datetime
from decimal import Decimal
from typing import Any

import orjson

from ..config.constants import REDIS_PREFIX_STOCK, REDIS_PREFIX_STREAM
from ..config.settings import settings
from ..models.tick import TickData, MarketDepth
from ..utils.logger import get_logger
from .redis_client import redis_manager

log = get_logger(__name__)


class TickerCache:
    """High-performance ticker cache backed by Redis Hashes and Streams."""

    # ── Write Operations ─────────────────────────────────────────────────

    async def set_tick(self, tick: TickData) -> None:
        """Write a tick to the Redis hash cache and push to the stream.

        The hash stores the latest tick per symbol for instant lookups.
        The stream provides the ordered event log for consumers.
        """
        client = redis_manager.client
        key = f"{REDIS_PREFIX_STOCK}:{tick.exchange}:{tick.symbol}:latest"

        # Flatten tick to a flat hash-friendly dict
        tick_dict = self._tick_to_hash(tick)

        pipe = client.pipeline(transaction=False)

        # 1. Write to hash (latest tick)
        pipe.hset(key, mapping=tick_dict)
        pipe.expire(key, settings.redis_tick_ttl_seconds)

        # 2. Push to Redis Stream for consumers
        stream_key = f"{REDIS_PREFIX_STREAM}:{tick.exchange}"
        stream_data = {
            "symbol": tick.symbol,
            "ltp": str(tick.ltp),
            "volume": str(tick.volume),
            "timestamp": tick.timestamp.isoformat(),
            "source": tick.source,
        }
        pipe.xadd(stream_key, stream_data, maxlen=100000)

        # 3. Publish to Pub/Sub for WebSocket subscribers
        pubsub_data = orjson.dumps({
            "symbol": tick.symbol,
            "exchange": tick.exchange,
            "ltp": str(tick.ltp),
            "change": str(tick.change),
            "pct_change": str(tick.pct_change),
            "volume": tick.volume,
            "timestamp": tick.timestamp.isoformat(),
        }).decode()
        pipe.publish(f"ticks:{tick.exchange}", pubsub_data)

        await pipe.execute()

    async def set_market_depth(self, symbol: str, exchange: str, depth: MarketDepth) -> None:
        """Write market depth data to a separate hash (shorter TTL)."""
        client = redis_manager.client
        key = f"{REDIS_PREFIX_STOCK}:{exchange}:{symbol}:depth"

        depth_dict: dict[str, str] = {}
        for i, level in enumerate(depth.buy[:5]):
            depth_dict[f"buy_{i}_price"] = str(level.price)
            depth_dict[f"buy_{i}_qty"] = str(level.quantity)
            depth_dict[f"buy_{i}_orders"] = str(level.orders)
        for i, level in enumerate(depth.sell[:5]):
            depth_dict[f"sell_{i}_price"] = str(level.price)
            depth_dict[f"sell_{i}_qty"] = str(level.quantity)
            depth_dict[f"sell_{i}_orders"] = str(level.orders)
        depth_dict["total_buy_qty"] = str(depth.total_buy_quantity)
        depth_dict["total_sell_qty"] = str(depth.total_sell_quantity)
        depth_dict["updated_at"] = datetime.now().isoformat()

        pipe = client.pipeline(transaction=False)
        pipe.hset(key, mapping=depth_dict)
        pipe.expire(key, settings.redis_depth_ttl_seconds)
        await pipe.execute()

    async def set_bulk_ticks(self, ticks: list[TickData]) -> None:
        """Write multiple ticks in a single pipeline for efficiency."""
        for tick in ticks:
            await self.set_tick(tick)

    # ── Read Operations ──────────────────────────────────────────────────

    async def get_tick(self, symbol: str, exchange: str = "NSE") -> dict[str, Any] | None:
        """Get the latest tick data for a symbol."""
        client = redis_manager.client
        key = f"{REDIS_PREFIX_STOCK}:{exchange}:{symbol}:latest"
        data = await client.hgetall(key)
        if not data:
            return None
        return data

    async def get_market_depth(self, symbol: str, exchange: str = "NSE") -> dict[str, Any] | None:
        """Get the latest market depth for a symbol."""
        client = redis_manager.client
        key = f"{REDIS_PREFIX_STOCK}:{exchange}:{symbol}:depth"
        data = await client.hgetall(key)
        if not data:
            return None
        return data

    async def get_bulk_ticks(
        self, symbols: list[str], exchange: str = "NSE"
    ) -> dict[str, dict[str, Any]]:
        """Get latest ticks for multiple symbols in a single pipeline."""
        client = redis_manager.client
        pipe = client.pipeline(transaction=False)
        keys = [f"{REDIS_PREFIX_STOCK}:{exchange}:{s}:latest" for s in symbols]
        for key in keys:
            pipe.hgetall(key)
        results = await pipe.execute()
        return {
            symbol: data
            for symbol, data in zip(symbols, results)
            if data  # skip empty
        }

    async def get_tick_age_ms(self, symbol: str, exchange: str = "NSE") -> float | None:
        """Get the age of the cached tick in milliseconds."""
        data = await self.get_tick(symbol, exchange)
        if not data or "cached_at" not in data:
            return None
        cached_at = float(data["cached_at"])
        return (time.time() - cached_at) * 1000

    async def is_tick_fresh(
        self, symbol: str, exchange: str = "NSE", max_age_ms: float = 1000
    ) -> bool:
        """Check if the cached tick is within the freshness threshold."""
        age = await self.get_tick_age_ms(symbol, exchange)
        if age is None:
            return False
        return age <= max_age_ms

    # ── Market State ─────────────────────────────────────────────────────

    async def set_market_status(self, status: str) -> None:
        """Update the cached market status."""
        client = redis_manager.client
        await client.set("market:status", status, ex=30)

    async def get_market_status(self) -> str | None:
        """Get the cached market status."""
        client = redis_manager.client
        return await client.get("market:status")

    # ── Provider Health ──────────────────────────────────────────────────

    async def set_provider_status(self, name: str, status: str) -> None:
        client = redis_manager.client
        pipe = client.pipeline(transaction=False)
        pipe.set(f"provider:{name}:status", status, ex=120)
        pipe.set(f"provider:{name}:last_update", datetime.now().isoformat(), ex=120)
        await pipe.execute()

    async def increment_provider_errors(self, name: str) -> int:
        client = redis_manager.client
        key = f"provider:{name}:error_count"
        count = await client.incr(key)
        await client.expire(key, 300)
        return count

    async def reset_provider_errors(self, name: str) -> None:
        client = redis_manager.client
        await client.set(f"provider:{name}:error_count", 0, ex=300)

    # ── Internal Helpers ─────────────────────────────────────────────────

    @staticmethod
    def _tick_to_hash(tick: TickData) -> dict[str, str]:
        """Flatten a TickData model into a string-keyed dict for Redis HSET."""
        return {
            "symbol": tick.symbol,
            "exchange": tick.exchange,
            "isin": tick.isin or "",
            "series": tick.series or "",
            "ltp": str(tick.ltp),
            "open": str(tick.open),
            "high": str(tick.high),
            "low": str(tick.low),
            "close": str(tick.close),
            "change": str(tick.change),
            "pct_change": str(tick.pct_change),
            "volume": str(tick.volume),
            "value": str(tick.value) if tick.value else "",
            "vwap": str(tick.vwap) if tick.vwap else "",
            "upper_circuit": str(tick.upper_circuit) if tick.upper_circuit else "",
            "lower_circuit": str(tick.lower_circuit) if tick.lower_circuit else "",
            "week_52_high": str(tick.week_52_high) if tick.week_52_high else "",
            "week_52_low": str(tick.week_52_low) if tick.week_52_low else "",
            "timestamp": tick.timestamp.isoformat(),
            "source": tick.source,
            "source_latency_ms": str(tick.source_latency_ms),
            "cached_at": str(time.time()),
        }


# Singleton
ticker_cache = TickerCache()
