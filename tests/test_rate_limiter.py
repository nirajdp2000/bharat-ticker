"""Unit tests for the TokenBucketRateLimiter module."""

from __future__ import annotations

import asyncio
import pytest
import time

from src.engine.rate_limiter import TokenBucketRateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_acquire():
    """Verify that the rate limiter permits execution and consumes tokens."""
    limiter = TokenBucketRateLimiter(rate=5, per=0.05)
    assert limiter.available_tokens == 5.0

    # Acquire 3 tokens
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()

    assert limiter.available_tokens <= 2.1  # 5 - 3 = 2 (plus a tiny refill)


@pytest.mark.asyncio
async def test_rate_limiter_blocking():
    """Verify that rate limiter blocks when tokens are exhausted and refills them."""
    # 2 tokens per 0.1 seconds (20 req/sec)
    limiter = TokenBucketRateLimiter(rate=2, per=0.1)

    # Consume available tokens
    await limiter.acquire()
    await limiter.acquire()
    assert limiter.available_tokens < 1.0

    t0 = time.monotonic()
    # This should block until the bucket refills
    await limiter.acquire()
    t1 = time.monotonic()

    # It must take at least ~0.05 seconds to refill 1 token
    assert (t1 - t0) >= 0.04


@pytest.mark.asyncio
async def test_rate_limiter_context_manager():
    """Verify that the rate limiter works as an async context manager."""
    limiter = TokenBucketRateLimiter(rate=2, per=0.05)
    async with limiter:
        assert limiter.available_tokens <= 1.1
