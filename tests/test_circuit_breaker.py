"""Unit tests for the CircuitBreaker module."""

from __future__ import annotations

import asyncio
import pytest
import time

from src.engine.circuit_breaker import CircuitBreaker, CircuitState, CircuitOpenError


@pytest.mark.asyncio
async def test_circuit_breaker_initial_state():
    """Verify circuit breaker starts CLOSED and allows execution."""
    cb = CircuitBreaker("test-cb", failure_threshold=2, recovery_timeout=0.1)
    assert cb.state == CircuitState.CLOSED
    assert cb.is_closed
    assert not cb.is_open
    assert await cb.can_execute()


@pytest.mark.asyncio
async def test_circuit_breaker_failure_trip():
    """Verify circuit breaker trips to OPEN when failures exceed threshold."""
    cb = CircuitBreaker("test-cb", failure_threshold=2, recovery_timeout=0.1)

    async def fail_func():
        raise ValueError("Failed call")

    # First failure
    with pytest.raises(ValueError):
        await cb.execute(fail_func)
    assert cb.state == CircuitState.CLOSED

    # Second failure -> trips
    with pytest.raises(ValueError):
        await cb.execute(fail_func)
    assert cb.state == CircuitState.OPEN
    assert cb.is_open

    # Further attempts should fail immediately with CircuitOpenError
    with pytest.raises(CircuitOpenError):
        await cb.execute(fail_func)


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_recovery():
    """Verify circuit transitions to HALF_OPEN after timeout and recovers on success."""
    cb = CircuitBreaker("test-cb", failure_threshold=1, recovery_timeout=0.05, half_open_max_calls=2)

    async def fail_func():
        raise ValueError("Failed")

    async def success_func():
        return "success"

    # Trip the circuit
    with pytest.raises(ValueError):
        await cb.execute(fail_func)
    assert cb.is_open

    # Wait for recovery timeout
    await asyncio.sleep(0.06)
    assert cb.state == CircuitState.HALF_OPEN

    # First success in half-open
    res1 = await cb.execute(success_func)
    assert res1 == "success"
    assert cb.state == CircuitState.HALF_OPEN

    # Second success in half-open -> closes
    res2 = await cb.execute(success_func)
    assert res2 == "success"
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_failure_reopen():
    """Verify any failure in HALF_OPEN immediately reopens the circuit."""
    cb = CircuitBreaker("test-cb", failure_threshold=1, recovery_timeout=0.05, half_open_max_calls=2)

    async def fail_func():
        raise ValueError("Failed")

    async def success_func():
        return "success"

    # Trip
    with pytest.raises(ValueError):
        await cb.execute(fail_func)
    assert cb.is_open

    # Wait for recovery timeout
    await asyncio.sleep(0.06)
    assert cb.state == CircuitState.HALF_OPEN

    # First call successful
    await cb.execute(success_func)
    assert cb.state == CircuitState.HALF_OPEN

    # Second call fails -> immediately OPENs
    with pytest.raises(ValueError):
        await cb.execute(fail_func)
    assert cb.state == CircuitState.OPEN
