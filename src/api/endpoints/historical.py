"""Historical OHLCV data endpoints.

Endpoints:
    GET /api/v1/historical/{symbol}  — Historical candles from TimescaleDB
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from ...db.connection import db_manager
from ...db.queries import TickerQueries
from ...engine.market_data import market_data_service
from ...utils.ist_clock import now_ist
from ...utils.logger import get_logger

log = get_logger(__name__)
router = APIRouter(prefix="/historical", tags=["Historical"])


@router.get(
    "/{symbol}",
    summary="Get historical OHLCV candles",
    description="Returns historical OHLCV candle data from TimescaleDB. Supports intervals from 1m to 1M.",
)
async def get_historical(
    symbol: str,
    exchange: str = Query("NSE", regex="^(NSE|BSE)$"),
    interval: Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"] = "1d",
    start: str | None = Query(None, description="Start date (YYYY-MM-DD) or ISO datetime"),
    end: str | None = Query(None, description="End date (YYYY-MM-DD) or ISO datetime"),
    limit: int = Query(500, ge=1, le=5000, description="Max number of candles"),
):
    """Get historical OHLCV candles for a symbol."""
    symbol = symbol.strip().upper()

    # Parse dates
    end_dt = _parse_datetime(end) if end else now_ist()
    start_dt = _parse_datetime(start) if start else _default_start(interval, end_dt)

    # Map interval to TimescaleDB time_bucket format
    interval_map = {
        "1m": "1 minute",
        "5m": "5 minutes",
        "15m": "15 minutes",
        "30m": "30 minutes",
        "1h": "1 hour",
        "4h": "4 hours",
        "1d": "1 day",
        "1w": "1 week",
    }
    bucket_interval = interval_map[interval]

    candles: list = []
    source = "timescaledb"

    # 1. Try stored data from TimescaleDB (when connected).
    if db_manager.is_connected:
        try:
            session = db_manager.get_session()
            async with session:
                queries = TickerQueries(session)
                if interval in ("1d", "1w"):
                    candles = await queries.get_eod_range(
                        symbol=symbol, exchange=exchange,
                        start_date=start_dt.date(), end_date=end_dt.date(),
                    )
                else:
                    candles = await queries.get_ohlcv_candles(
                        symbol=symbol, exchange=exchange, interval=bucket_interval,
                        start_time=start_dt, end_time=end_dt,
                    )
        except Exception as e:
            log.warning("historical_db_failed", symbol=symbol, error=str(e))

    # 2. Backfill from Yahoo when there is nothing stored yet ("old data").
    if not candles:
        candles = await market_data_service.get_history(
            symbol=symbol, exchange=exchange, interval=interval,
            start=start_dt, end=end_dt, limit=limit,
        )
        source = "yahoo_finance"

    candles = candles[:limit]

    return {
        "status": "success",
        "timestamp": now_ist().isoformat(),
        "symbol": symbol,
        "exchange": exchange,
        "interval": interval,
        "source": source,
        "count": len(candles),
        "data": [
            {
                "timestamp": str(c.get("bucket", c.get("date", ""))),
                "open": str(c.get("open", 0)),
                "high": str(c.get("high", 0)),
                "low": str(c.get("low", 0)),
                "close": str(c.get("close", 0)),
                "volume": c.get("volume", 0),
                "value": str(c.get("value", "")) if c.get("value") else None,
                "vwap": str(c.get("vwap", "")) if c.get("vwap") else None,
            }
            for c in candles
        ],
    }


@router.get(
    "/{symbol}/eod",
    summary="Get end-of-day data",
    description="Get the latest end-of-day summary for a symbol.",
)
async def get_eod(
    symbol: str,
    exchange: str = Query("NSE", regex="^(NSE|BSE)$"),
):
    """Get the latest EOD data."""
    symbol = symbol.strip().upper()

    eod = None
    if db_manager.is_connected:
        try:
            session = db_manager.get_session()
            async with session:
                queries = TickerQueries(session)
                eod = await queries.get_latest_eod(symbol, exchange)
        except Exception as e:
            log.warning("eod_db_failed", symbol=symbol, error=str(e))

    if eod:
        return {
            "status": "success",
            "source": "timescaledb",
            "timestamp": now_ist().isoformat(),
            "data": {k: str(v) if isinstance(v, Decimal) else v for k, v in eod.items()},
        }

    # Fall back to the latest Yahoo daily candle.
    candles = await market_data_service.get_history(
        symbol=symbol, exchange=exchange, interval="1d", limit=1,
    )
    if not candles:
        raise HTTPException(status_code=404, detail=f"No EOD data for {symbol}")
    c = candles[-1]
    return {
        "status": "success",
        "source": "yahoo_finance",
        "timestamp": now_ist().isoformat(),
        "data": {
            "symbol": symbol, "exchange": exchange, "date": c.get("bucket"),
            "open": c.get("open"), "high": c.get("high"), "low": c.get("low"),
            "close": c.get("close"), "volume": c.get("volume"),
        },
    }


def _parse_datetime(value: str) -> datetime:
    """Parse a date or datetime string."""
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {value}")


def _default_start(interval: str, end: datetime) -> datetime:
    """Calculate a sensible default start date based on interval."""
    defaults = {
        "1m": timedelta(days=1),
        "5m": timedelta(days=5),
        "15m": timedelta(days=10),
        "30m": timedelta(days=15),
        "1h": timedelta(days=30),
        "4h": timedelta(days=60),
        "1d": timedelta(days=365),
        "1w": timedelta(days=365 * 3),
    }
    return end - defaults.get(interval, timedelta(days=365))
