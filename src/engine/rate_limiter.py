"""Token bucket rate limiter for controlling request frequency."""

from __future__ import annotations

import asyncio
import time


class TokenBucketRateLimiter:
    """Async token bucket rate limiter.

    Controls the rate of outgoing requests to avoid triggering
    exchange rate limits.

    Usage::

        limiter = TokenBucketRateLimiter(rate=5, per=1.0)  # 5 req/sec
        async with limiter:
            await make_request()
    """

    def __init__(self, rate: int, per: float = 1.0) -> None:
        """
        Parameters
        ----------
        rate : int
            Maximum number of tokens (requests) in the bucket.
        per : float
            Time period in seconds to refill the bucket.
        """
        self._rate = rate
        self._per = per
        self._tokens = float(rate)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a token is available, then consume it."""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

            # Wait for a refill
            await asyncio.sleep(self._per / self._rate)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * (self._rate / self._per)
        self._tokens = min(self._rate, self._tokens + new_tokens)
        self._last_refill = now

    async def __aenter__(self) -> "TokenBucketRateLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, *args) -> None:
        pass

    @property
    def available_tokens(self) -> float:
        self._refill()
        return self._tokens
