"""Abstract base class for all data providers.

Every data source (NSE scraper, broker WebSocket, Yahoo Finance, etc.)
must implement this interface.  The orchestrator uses the interface to
manage provider lifecycles, health checks, and failover.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal

from ..models.tick import TickData
from ..utils.logger import get_logger

log = get_logger(__name__)


class DataProvider(ABC):
    """Contract that every data provider must fulfill."""

    # ── Class-level metadata (override in subclass) ──────────────────────
    name: str = "base"
    tier: int = 1  # 1 = Primary, 2 = Secondary, 3 = Tertiary
    priority: int = 0  # Lower = tried first within failover (0 → falls back to tier*10)
    delayed: bool = False  # True = delayed feed (e.g. Yahoo) — barred from live quotes
    exchange: Literal["NSE", "BSE", "BOTH"] = "NSE"

    def __init__(self) -> None:
        self._is_connected: bool = False
        self._error_count: int = 0
        self._total_requests: int = 0
        self._total_latency_ms: float = 0.0

    # ── Lifecycle ────────────────────────────────────────────────────────

    @abstractmethod
    async def connect(self) -> None:
        """Initialize sessions, authenticate, warm up cookies, etc."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Clean up resources."""
        ...

    # ── Data Fetching ────────────────────────────────────────────────────

    @abstractmethod
    async def fetch_quote(self, symbol: str) -> TickData:
        """Fetch a single real-time quote for a symbol.

        Raises
        ------
        ProviderError
            If the fetch fails after retries.
        """
        ...

    @abstractmethod
    async def fetch_bulk(self, symbols: list[str]) -> list[TickData]:
        """Fetch quotes for multiple symbols.

        Default implementation calls fetch_quote in a loop, but providers
        that support bulk endpoints should override for efficiency.
        """
        ...

    # ── Health ───────────────────────────────────────────────────────────

    @abstractmethod
    async def health_check(self) -> bool:
        """Return True if the provider is reachable and functional."""
        ...

    # ── Metrics ──────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    @property
    def error_rate(self) -> float:
        if self._total_requests == 0:
            return 0.0
        return self._error_count / self._total_requests

    @property
    def avg_latency_ms(self) -> float:
        if self._total_requests == 0:
            return 0.0
        return self._total_latency_ms / self._total_requests

    def record_success(self, latency_ms: float) -> None:
        self._total_requests += 1
        self._total_latency_ms += latency_ms

    def record_error(self) -> None:
        self._total_requests += 1
        self._error_count += 1

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name} tier={self.tier} connected={self._is_connected}>"


class ProviderError(Exception):
    """Raised when a data provider fails to fetch data."""

    def __init__(self, provider: str, message: str, status_code: int | None = None) -> None:
        self.provider = provider
        self.status_code = status_code
        super().__init__(f"[{provider}] {message}")


class ProviderBlockedError(ProviderError):
    """Raised when the provider is blocked (403, captcha, rate limit)."""
    pass


class ProviderSchemaError(ProviderError):
    """Raised when the response doesn't match the expected schema."""
    pass
