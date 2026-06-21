"""Circuit Breaker pattern for provider health management.

Prevents cascading failures by temporarily disabling a provider that
is consistently failing, giving it time to recover before retrying.

States:
    CLOSED    → Normal operation.  Requests pass through.
    OPEN      → Provider is failing.  All requests immediately fail.
    HALF_OPEN → Testing recovery.  Limited requests pass through.
"""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Callable, Any

from ..config.settings import settings
from ..utils.logger import get_logger

log = get_logger(__name__)


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """Per-provider circuit breaker with configurable thresholds."""

    def __init__(
        self,
        name: str,
        failure_threshold: int | None = None,
        recovery_timeout: float | None = None,
        half_open_max_calls: int | None = None,
    ) -> None:
        self.name = name
        self._failure_threshold = failure_threshold or settings.cb_failure_threshold
        self._recovery_timeout = recovery_timeout or settings.cb_recovery_timeout_seconds
        self._half_open_max_calls = half_open_max_calls or settings.cb_half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float = 0.0
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Current circuit state, with automatic OPEN → HALF_OPEN transition."""
        if self._state == CircuitState.OPEN:
            if time.time() - self._last_failure_time > self._recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
                log.info("circuit_half_open", provider=self.name)
        return self._state

    @property
    def is_closed(self) -> bool:
        return self.state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    async def record_success(self) -> None:
        """Record a successful call."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._half_open_max_calls:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    log.info("circuit_closed", provider=self.name, reason="recovery_confirmed")
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = max(0, self._failure_count - 1)

    async def record_failure(self, error: str = "") -> None:
        """Record a failed call."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open immediately opens the circuit
                self._state = CircuitState.OPEN
                log.warning("circuit_opened", provider=self.name, reason="half_open_failure", error=error)
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self._failure_threshold:
                    self._state = CircuitState.OPEN
                    log.warning(
                        "circuit_opened",
                        provider=self.name,
                        reason="threshold_exceeded",
                        failures=self._failure_count,
                        threshold=self._failure_threshold,
                        error=error,
                    )

    async def can_execute(self) -> bool:
        """Check if a request can pass through the circuit breaker."""
        state = self.state
        if state == CircuitState.CLOSED:
            return True
        elif state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self._half_open_max_calls
        else:
            return False

    async def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute a function through the circuit breaker.

        Raises CircuitOpenError if the circuit is open.
        """
        if not await self.can_execute():
            raise CircuitOpenError(self.name)

        if self.state == CircuitState.HALF_OPEN:
            self._half_open_calls += 1

        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception as e:
            await self.record_failure(str(e))
            raise

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        log.info("circuit_reset", provider=self.name)

    def to_dict(self) -> dict:
        """Serialize state for monitoring."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "last_failure": self._last_failure_time,
            "threshold": self._failure_threshold,
            "recovery_timeout": self._recovery_timeout,
        }


class CircuitOpenError(Exception):
    """Raised when a request is rejected because the circuit is open."""

    def __init__(self, provider: str) -> None:
        self.provider = provider
        super().__init__(f"Circuit breaker OPEN for provider: {provider}")
