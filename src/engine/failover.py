"""Multi-tier failover controller.

Manages the priority order of data providers and automatically cascades
through tiers when a provider fails or its circuit breaker opens.

Failover chain:
    Tier-1: NSE Scraper → BSE Scraper
    Tier-2: Fyers WS → Angel WS
    Tier-3: Yahoo Finance → EODHD → Google Finance
    Last Resort: Return cached data from Redis
"""

from __future__ import annotations

import asyncio
from typing import Any

from ..models.tick import TickData
from ..providers.base import DataProvider, ProviderError
from ..utils.logger import get_logger
from .circuit_breaker import CircuitBreaker, CircuitOpenError

log = get_logger(__name__)


def _source_match(provider: DataProvider, source: str) -> bool:
    """Match a provider against a source keyword ('nse'/'bse'/'yahoo') or name."""
    s = source.lower()
    if s == provider.name.lower():
        return True
    if s in ("nse", "bse"):
        return provider.name.lower().startswith(s)
    if s in ("yahoo", "yfinance"):
        return "yahoo" in provider.name.lower()
    return False


class FailoverController:
    """Orchestrates multi-tier provider failover with circuit breakers."""

    def __init__(self) -> None:
        self._providers: list[DataProvider] = []
        self._circuit_breakers: dict[str, CircuitBreaker] = {}
        self._enabled: dict[str, bool] = {}

    def register_provider(self, provider: DataProvider) -> None:
        """Register a data provider, maintaining tier order."""
        self._providers.append(provider)
        self._providers.sort(key=lambda p: (p.priority or p.tier * 10, p.name))
        self._circuit_breakers[provider.name] = CircuitBreaker(provider.name)
        self._enabled.setdefault(provider.name, True)
        log.info("provider_registered", name=provider.name, tier=provider.tier)

    # ── Runtime source control (for the UI toggle buttons) ────────────────
    def set_enabled(self, name: str, enabled: bool) -> bool:
        """Enable/disable a provider (or a source group like 'nse'/'bse')."""
        matched = False
        for p in self._providers:
            if p.name == name or _source_match(p, name):
                self._enabled[p.name] = enabled
                matched = True
        return matched

    def is_enabled(self, name: str) -> bool:
        return self._enabled.get(name, True)

    async def fetch_quote(
        self, symbol: str, source: str | None = None, exclude_delayed: bool = False
    ) -> TickData:
        """Fetch a quote, cascading through providers on failure.

        Tries each provider in priority order.  Skips providers that are
        disabled, whose circuit breaker is open, that don't match the requested
        ``source`` ('nse' | 'bse' | 'yahoo' | provider name), or — when
        ``exclude_delayed`` — any delayed feed (Yahoo).  The latter enforces the
        rule that **live quotes only ever come from NSE/BSE, never Yahoo**.
        Raises ProviderError if all candidates fail.
        """
        errors: list[str] = []
        forced = bool(source and source != "auto")

        for provider in self._providers:
            cb = self._circuit_breakers[provider.name]

            # Skip if disabled or not matching the requested source
            if not self._enabled.get(provider.name, True):
                continue
            if forced and not _source_match(provider, source):
                continue
            # Bar delayed feeds from live quotes (unless this exact source is forced)
            if exclude_delayed and provider.delayed and not forced:
                continue

            # Skip if circuit is open
            if not await cb.can_execute():
                log.debug("provider_skipped_circuit_open", provider=provider.name, symbol=symbol)
                continue

            # Skip if not connected
            if not provider.is_connected:
                try:
                    await provider.connect()
                except Exception as e:
                    log.warning("provider_connect_failed", provider=provider.name, error=str(e))
                    await cb.record_failure(str(e))
                    continue

            try:
                tick = await cb.execute(provider.fetch_quote, symbol)
                return tick

            except CircuitOpenError:
                log.debug("provider_circuit_open", provider=provider.name)
                continue

            except ProviderError as e:
                errors.append(f"{provider.name}: {e}")
                log.warning("provider_failed", provider=provider.name, symbol=symbol, error=str(e))
                continue

            except Exception as e:
                errors.append(f"{provider.name}: {e}")
                await cb.record_failure(str(e))
                log.warning("provider_unexpected_error", provider=provider.name, error=str(e))
                continue

        # All providers exhausted
        error_summary = " | ".join(errors) if errors else "No providers available"
        raise ProviderError("failover", f"All providers failed for {symbol}: {error_summary}")

    async def fetch_bulk(self, symbols: list[str]) -> list[TickData]:
        """Fetch bulk quotes with per-symbol failover."""
        results: list[TickData] = []
        failed: list[str] = []

        # Try the highest-priority working provider for bulk fetch first
        for provider in self._providers:
            cb = self._circuit_breakers[provider.name]
            if not await cb.can_execute() or not provider.is_connected:
                continue

            try:
                ticks = await cb.execute(provider.fetch_bulk, symbols)
                if ticks:
                    fetched_symbols = {t.symbol for t in ticks}
                    results.extend(ticks)
                    remaining = [s for s in symbols if s not in fetched_symbols]
                    if not remaining:
                        return results
                    # Fetch remaining symbols individually
                    for sym in remaining:
                        try:
                            tick = await self.fetch_quote(sym)
                            results.append(tick)
                        except ProviderError:
                            failed.append(sym)
                    return results
            except Exception as e:
                log.warning("bulk_provider_failed", provider=provider.name, error=str(e))
                continue

        # Fallback: fetch individually
        for sym in symbols:
            try:
                tick = await self.fetch_quote(sym)
                results.append(tick)
            except ProviderError:
                failed.append(sym)

        if failed:
            log.warning("bulk_fetch_partial", total=len(symbols), success=len(results), failed=len(failed))

        return results

    async def health_check_all(self) -> dict[str, dict[str, Any]]:
        """Run health checks on all registered providers."""
        statuses = {}
        for provider in self._providers:
            cb = self._circuit_breakers[provider.name]
            try:
                is_healthy = await provider.health_check()
                statuses[provider.name] = {
                    "tier": provider.tier,
                    "status": "HEALTHY" if is_healthy else "DOWN",
                    "circuit_state": cb.state.value,
                    "avg_latency_ms": round(provider.avg_latency_ms, 1),
                    "error_rate": round(provider.error_rate * 100, 1),
                }
            except Exception as e:
                statuses[provider.name] = {
                    "tier": provider.tier,
                    "status": "DOWN",
                    "circuit_state": cb.state.value,
                    "error": str(e),
                }
        return statuses

    async def test_provider(self, name: str, symbol: str = "RELIANCE") -> dict[str, Any]:
        """Live-test a single provider/source by fetching one symbol directly.

        Bypasses the cascade so the UI can verify each connection individually.
        """
        import time
        targets = [p for p in self._providers if p.name == name or _source_match(p, name)]
        if not targets:
            return {"name": name, "ok": False, "error": "no such source"}
        results = []
        for p in targets:
            t0 = time.time()
            try:
                if not p.is_connected:
                    await p.connect()
                tick = await p.fetch_quote(symbol)
                results.append({
                    "name": p.name, "ok": True, "delayed": p.delayed,
                    "latency_ms": round((time.time() - t0) * 1000, 1),
                    "symbol": tick.symbol, "ltp": str(tick.ltp), "exchange": tick.exchange,
                })
            except Exception as e:
                results.append({
                    "name": p.name, "ok": False, "delayed": p.delayed,
                    "latency_ms": round((time.time() - t0) * 1000, 1),
                    "error": str(e)[:140],
                })
        return {"name": name, "symbol": symbol,
                "ok": any(r["ok"] for r in results), "providers": results}

    async def list_status(self) -> list[dict[str, Any]]:
        """Per-provider status for the UI (enabled flag + circuit + metrics)."""
        out: list[dict[str, Any]] = []
        for p in self._providers:
            cb = self._circuit_breakers[p.name]
            out.append({
                "name": p.name,
                "exchange": p.exchange,
                "tier": p.tier,
                "priority": p.priority or p.tier * 10,
                "enabled": self._enabled.get(p.name, True),
                "circuit_state": cb.state.value,
                "avg_latency_ms": round(p.avg_latency_ms, 1),
                "error_rate": round(p.error_rate * 100, 1),
                "connected": p.is_connected,
            })
        return out

    async def connect_all(self) -> None:
        """Connect all registered providers."""
        for provider in self._providers:
            try:
                await provider.connect()
                log.info("provider_connected", name=provider.name, tier=provider.tier)
            except Exception as e:
                log.warning("provider_connect_failed", name=provider.name, error=str(e))

    async def disconnect_all(self) -> None:
        """Disconnect all providers."""
        for provider in self._providers:
            try:
                await provider.disconnect()
            except Exception as e:
                log.warning("provider_disconnect_error", name=provider.name, error=str(e))

    @property
    def providers(self) -> list[DataProvider]:
        return list(self._providers)

    @property
    def circuit_breakers(self) -> dict[str, CircuitBreaker]:
        return dict(self._circuit_breakers)
