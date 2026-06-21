"""Market status and index endpoints.

Endpoints:
    GET /api/v1/market/status     — Current market state
    GET /api/v1/market/indices    — All index values (placeholder)
    GET /api/v1/providers/status  — Health of all data providers
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ...cache.redis_client import redis_manager
from ...cache.ticker_cache import ticker_cache
from ...engine.market_data import market_data_service
from ...models.api_response import MarketState
from ...utils.ist_clock import (
    get_market_session,
    get_market_state_dict,
    now_ist,
)
from ...utils.logger import get_logger

log = get_logger(__name__)
router = APIRouter(tags=["Market"])


@router.get(
    "/market/status",
    summary="Get current market status",
    description="Returns the current market session, next open/close times, and holiday info.",
)
async def get_market_status():
    """Get the current market state."""
    state = get_market_state_dict()
    cached_status = await ticker_cache.get_market_status()

    return {
        "status": "success",
        "timestamp": now_ist().isoformat(),
        "data": {
            **state,
            "cached_status": cached_status,
            "server_time_ist": now_ist().strftime("%Y-%m-%d %H:%M:%S %Z"),
        },
    }


@router.get(
    "/providers/status",
    summary="Get data provider health status",
    description="Returns the health status, circuit breaker state, and latency of all registered data providers.",
)
async def get_providers_status():
    """Get live health status of all registered data providers."""
    try:
        providers = await market_data_service.provider_health()
    except Exception as e:
        log.warning("provider_health_failed", error=str(e))
        providers = {}
    return {
        "status": "success",
        "timestamp": now_ist().isoformat(),
        "data": {
            "cache_backend": "redis" if redis_manager.is_real else "in_memory_fallback",
            "providers": providers,
        },
    }


@router.get("/snapshot", summary="ALL stocks on an exchange in one call (~0ms read)")
async def all_stocks(exchange: str = "NSE", limit: int = 6000, offset: int = 0):
    """Whole-market snapshot served from memory — every symbol, single request.

    Bulk values = last-traded-day (official bhavcopy); the live-polled subset is
    fresher. True tick-by-tick all-stocks-live needs a broker WebSocket.
    """
    rows = await market_data_service.get_all_snapshot(exchange.strip().upper())
    total = len(rows)
    rows = rows[offset:offset + limit]
    return {
        "status": "success", "timestamp": now_ist().isoformat(),
        "exchange": exchange.upper(), "total": total, "count": len(rows),
        "data_quality": "EOD_SNAPSHOT", "data": rows,
    }


@router.get("/indices", summary="Live NSE index values")
async def get_indices(q: str | None = None):
    """All NSE indices (NIFTY 50/Bank/sectoral). Optional ?q= filters by name."""
    data = await market_data_service.get_indices()
    if q:
        ql = q.strip().upper()
        data = [d for d in data if ql in (d.get("index") or "").upper()]
    return {"status": "success", "timestamp": now_ist().isoformat(),
            "count": len(data), "data": data}


@router.get("/sources", summary="List data sources and their state")
async def list_sources():
    """Per-provider state (enabled, circuit, latency) for the UI toggle panel."""
    try:
        sources = await market_data_service.list_sources()
    except Exception as e:
        log.warning("list_sources_failed", error=str(e))
        sources = []
    return {"status": "success", "timestamp": now_ist().isoformat(), "data": sources}


@router.get("/sources/{name}/test", summary="Live-test a single source connection")
async def test_source(name: str, symbol: str = "RELIANCE"):
    """Verify a source works by fetching one symbol directly through it."""
    try:
        result = await market_data_service.test_source(name, symbol.strip().upper())
    except Exception as e:
        log.warning("source_test_failed", name=name, error=str(e))
        return {"status": "error", "name": name, "ok": False, "error": str(e)[:140]}
    return {"status": "success", "timestamp": now_ist().isoformat(), "data": result}


@router.post("/sources/{name}/{action}", summary="Enable/disable a data source")
async def toggle_source(name: str, action: str):
    """Connect (enable) or disconnect (disable) a source/group: nse|bse|yahoo|<provider>."""
    if action not in ("enable", "disable", "connect", "disconnect"):
        raise HTTPException(status_code=400, detail="action must be enable|disable")
    enabled = action in ("enable", "connect")
    ok = await market_data_service.set_source_enabled(name, enabled)
    if not ok:
        raise HTTPException(status_code=404, detail=f"No source matching '{name}'")
    return {"status": "success", "source": name, "enabled": enabled}
