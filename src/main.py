"""Bharat Ticker — FastAPI Application Entry Point.

This is the main application that ties together all components:
  - API endpoints (quotes, market, historical, search, WebSocket)
  - Middleware (market hours, CORS, rate limiting)
  - Lifecycle hooks (Redis, TimescaleDB connections)
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, ORJSONResponse

from .api.endpoints.historical import router as historical_router
from .api.endpoints.market import router as market_router
from .api.endpoints.quotes import router as quotes_router
from .api.endpoints.search import router as search_router
from .api.endpoints.superbrain import router as superbrain_router
from .api.endpoints.websocket import router as ws_router
from .api.middleware.market_hours import MarketHoursMiddleware
from .cache import redis_manager
from .config.settings import settings
from .db.connection import db_manager
from .utils.ist_clock import get_market_state_dict, now_ist
from .utils.logger import get_logger, setup_logging

log = get_logger(__name__)


# ── Application Lifecycle ────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown of external connections."""
    setup_logging()
    log.info("bharat_ticker_starting", env=settings.app_env)

    # Startup: connect to Redis and TimescaleDB
    await redis_manager.connect()
    try:
        await db_manager.connect()
    except Exception as e:
        log.warning("db_connection_deferred", error=str(e))

    # Pre-warm the data providers (loads NSE archive) in the background so the
    # first quote request is fast.
    import asyncio
    from .engine.market_data import market_data_service
    asyncio.create_task(market_data_service.warm())

    log.info("bharat_ticker_ready", port=settings.api_port)

    yield  # Application is running

    # Shutdown: clean up connections
    log.info("bharat_ticker_shutting_down")
    await redis_manager.disconnect()
    await db_manager.disconnect()
    log.info("bharat_ticker_stopped")


# ── FastAPI Application ──────────────────────────────────────────────────────

app = FastAPI(
    title="Bharat Ticker",
    description=(
        "🇮🇳 Production-grade Real-Time Indian Stock Market Data API.\n\n"
        "Serves comprehensive NSE/BSE market data with sub-millisecond latency "
        "via Redis caching, TimescaleDB persistence, and multi-tier provider failover.\n\n"
        "**Features:**\n"
        "- Real-time quotes with market depth (Level-2)\n"
        "- Smart market-hours routing (live data ↔ EOD data)\n"
        "- Historical OHLCV candles (1m to 1M intervals)\n"
        "- WebSocket streaming for live ticks\n"
        "- Multi-tier provider failover (NSE → Broker → Yahoo)\n"
    ),
    version="1.0.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)


# ── Middleware ───────────────────────────────────────────────────────────────

# CORS — origins resolved from env (CORS_ORIGINS), so production deploys can let
# the superbrain origin through without code changes:
#   CORS_ORIGINS set        → use that list (exact value the deployer provides)
#   unset + debug (dev)     → allow all ("*")
#   unset + production       → safe localhost fallback
# allow_credentials must be False when origins is "*" (browsers reject the combo).
if settings.cors_origins:
    _cors_origins = settings.cors_origins
elif settings.debug:
    _cors_origins = ["*"]
else:
    _cors_origins = ["http://localhost:3000", "http://localhost:8080"]

_allow_all = _cors_origins == ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=not _allow_all,
    allow_methods=["*"],
    allow_headers=["*"],
)
log.info("cors_configured", origins=_cors_origins, credentials=not _allow_all)

# Market hours context injection
app.add_middleware(MarketHoursMiddleware)


# ── Routers ──────────────────────────────────────────────────────────────────

API_V1_PREFIX = "/api/v1"

app.include_router(quotes_router, prefix=API_V1_PREFIX)
app.include_router(market_router, prefix=API_V1_PREFIX)
app.include_router(historical_router, prefix=API_V1_PREFIX)
app.include_router(search_router, prefix=API_V1_PREFIX)
app.include_router(superbrain_router, prefix=API_V1_PREFIX)
app.include_router(ws_router, prefix=API_V1_PREFIX)


# ── Root & Health Endpoints ──────────────────────────────────────────────────

_WEB = Path(__file__).resolve().parent / "web"


@app.get("/", include_in_schema=False)
@app.get("/ui", tags=["System"])
async def ui():
    """Interactive dashboard — quotes, source toggles, health, history chart."""
    return FileResponse(_WEB / "index.html")


@app.get("/all", tags=["System"])
async def all_stocks_page():
    """All-stocks table — every NSE/BSE symbol in one page (single snapshot call)."""
    return FileResponse(_WEB / "all.html")


@app.get("/sb", tags=["System"])
async def superbrain_page():
    """Superbrain feed dashboard — /sb adapter (rich quote, real intraday, MMI,
    screener) with JSON links + market-aware continuous refresh."""
    return FileResponse(_WEB / "sb.html")


@app.get("/api", tags=["System"])
async def root():
    """API root — system info."""
    return {
        "name": "Bharat Ticker",
        "version": "1.0.0",
        "description": "Real-Time Indian Stock Market Data Ingestion Engine",
        "docs": "/docs",
        "ui": "/ui",
        "api_base": API_V1_PREFIX,
        "server_time_ist": now_ist().isoformat(),
        "market_state": get_market_state_dict(),
    }


@app.get("/health", tags=["System"])
async def health_check():
    """System health check — verifies Redis and DB connectivity."""
    redis_ok = await redis_manager.health_check()

    db_ok = False
    try:
        db_ok = await db_manager.health_check()
    except Exception:
        pass

    status = "healthy" if (redis_ok and db_ok) else "degraded"

    return {
        "status": status,
        "timestamp": now_ist().isoformat(),
        "components": {
            "redis": {"status": "up" if redis_ok else "down"},
            "timescaledb": {"status": "up" if db_ok else "down"},
            "api": {"status": "up"},
        },
    }


@app.get("/api/v1/ping", tags=["System"])
async def ping():
    """Simple ping for latency measurement."""
    return {"pong": True, "timestamp": now_ist().isoformat()}
