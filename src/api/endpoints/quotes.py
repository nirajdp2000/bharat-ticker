"""Quote endpoints — the primary API for fetching stock data.

Endpoints:
    GET /api/v1/quote/{symbol}       — Full quote with depth, OHLCV, metadata
    GET /api/v1/quote/{symbol}/depth  — Market depth only
    GET /api/v1/quotes               — Bulk quote fetch
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from ...cache.ticker_cache import ticker_cache
from ...config.constants import DATA_QUALITY_EOD, DATA_QUALITY_REALTIME, DATA_QUALITY_STALE
from ...db.connection import db_manager
from ...db.queries import TickerQueries
from ...engine.market_data import market_data_service
from ...models.api_response import (
    APIResponse,
    BulkQuoteResponse,
    ErrorResponse,
    FullQuoteData,
    MarketState,
    QuoteDepth,
    QuoteDepthLevel,
    QuoteInfo,
    QuotePrice,
    QuoteVolume,
    ResponseMeta,
    TechnicalIndicators,
)
from ...utils.ist_clock import get_market_state_dict, is_market_open, now_ist
from ...utils.logger import get_logger

log = get_logger(__name__)
router = APIRouter(prefix="/quote", tags=["Quotes"])


@router.get(
    "/{symbol}",
    response_model=APIResponse[FullQuoteData],
    summary="Get full quote for a symbol",
    description="Returns comprehensive real-time (market hours) or EOD (closed) data including price, volume, market depth, and indicators.",
)
async def get_quote(
    symbol: str,
    exchange: str = Query("NSE", regex="^(NSE|BSE)$"),
    source: str = Query("auto", description="Force a source: auto|nse|bse|yahoo"),
):
    """Get a comprehensive quote for a single symbol.

    ``source`` lets the UI pin the feed to a specific exchange/provider; the
    default ``auto`` cascades live→archive across NSE, BSE and Yahoo.
    """
    symbol = symbol.strip().upper()
    market_state = MarketState(**get_market_state_dict())

    # When a source is forced, always go straight through the provider stack
    # (skip the warm cache, which may hold another source's tick).
    if source and source != "auto":
        tick = await market_data_service.get_quote_through(
            symbol, exchange, source=source, write_cache=False
        )
        if not tick:
            raise HTTPException(status_code=404, detail=f"No {source} data for {symbol}")
        return APIResponse(
            status="success", timestamp=now_ist(), market_state=market_state,
            data=_build_quote_from_tick(tick),
            meta=ResponseMeta(
                source=tick.source, source_latency_ms=tick.source_latency_ms,
                data_quality=DATA_QUALITY_REALTIME if is_market_open() else DATA_QUALITY_EOD,
                provider_tier=1,
            ),
        )

    if is_market_open():
        return await _get_realtime_quote(symbol, exchange, market_state)
    else:
        return await _get_eod_quote(symbol, exchange, market_state)


@router.get("/{symbol}/details", summary="Rich quote details (fundamentals + stats)")
async def get_details(
    symbol: str,
    source: str = Query("auto", description="auto|bse|nse"),
):
    """Comprehensive panel — OHLC, VWAP, 52wk, price band, traded stats, Mcap,
    sector/industry, EPS/PE/PB/ROE — fetched live from BSE/NSE structured APIs."""
    symbol = symbol.strip().upper()
    d = await market_data_service.get_details(symbol, source)
    if not d:
        raise HTTPException(status_code=404, detail=f"No details for {symbol}")
    return {"status": "success", "timestamp": now_ist().isoformat(), "data": d}


@router.get("/{symbol}/analytics", summary="Returns + volatility (computed)")
async def get_analytics(symbol: str, exchange: str = Query("NSE", regex="^(NSE|BSE)$")):
    """Multi-period returns (1W/1M/YTD/1Y/3Y/5Y), daily/annualised volatility,
    52-week range — computed from historical data (works without live NSE)."""
    a = await market_data_service.get_analytics(symbol.strip().upper(), exchange)
    if not a:
        raise HTTPException(status_code=404, detail=f"Not enough history for {symbol}")
    return {"status": "success", "timestamp": now_ist().isoformat(), "data": a}


@router.get("/{symbol}/nse", summary="NSE full quote (trade/price/securities info)")
async def get_nse_full(symbol: str):
    """NSE trade/price/securities information (live; needs PROXY_LIST — Akamai)."""
    d = await market_data_service.get_nse_full(symbol.strip().upper())
    return {"status": "success", "timestamp": now_ist().isoformat(), "data": d}


@router.get("/{symbol}/options", summary="NSE option chain")
async def get_option_chain(symbol: str):
    """NSE equity option chain (live; needs PROXY_LIST — Akamai)."""
    d = await market_data_service.get_option_chain(symbol.strip().upper())
    return {"status": "success", "timestamp": now_ist().isoformat(), "data": d}


@router.get(
    "/{symbol}/depth",
    summary="Get market depth for a symbol",
    description="Returns the 5-level order book (bid/ask) for a symbol. Only available during market hours.",
)
async def get_depth(symbol: str, exchange: str = Query("NSE", regex="^(NSE|BSE)$")):
    """Get market depth / order book."""
    symbol = symbol.strip().upper()

    depth_data = await ticker_cache.get_market_depth(symbol, exchange)
    if not depth_data:
        # Live fall-through to BSE 5-level order book.
        live = await market_data_service.get_depth(symbol)
        if live and (live.get("buy") or live.get("sell")):
            return {"status": "success", "symbol": symbol, "exchange": "BSE",
                    "depth": live, "timestamp": live.get("updated_at")}
        raise HTTPException(status_code=404,
                            detail=f"No depth for {symbol} (market closed or unavailable)")

    # Parse depth from Redis hash
    buy_levels = []
    sell_levels = []
    for i in range(5):
        bp = depth_data.get(f"buy_{i}_price")
        if bp:
            buy_levels.append(QuoteDepthLevel(
                price=Decimal(bp),
                quantity=int(depth_data.get(f"buy_{i}_qty", 0)),
                orders=int(depth_data.get(f"buy_{i}_orders", 0)),
            ))
        sp = depth_data.get(f"sell_{i}_price")
        if sp:
            sell_levels.append(QuoteDepthLevel(
                price=Decimal(sp),
                quantity=int(depth_data.get(f"sell_{i}_qty", 0)),
                orders=int(depth_data.get(f"sell_{i}_orders", 0)),
            ))

    return {
        "status": "success",
        "symbol": symbol,
        "exchange": exchange,
        "depth": QuoteDepth(
            buy=buy_levels,
            sell=sell_levels,
            total_buy_quantity=int(depth_data.get("total_buy_qty", 0)),
            total_sell_quantity=int(depth_data.get("total_sell_qty", 0)),
        ),
        "timestamp": depth_data.get("updated_at"),
    }


@router.get(
    "s",
    response_model=BulkQuoteResponse,
    summary="Bulk quote fetch",
    description="Fetch quotes for multiple symbols (comma-separated, max 50).",
)
async def get_bulk_quotes(
    symbols: str = Query(..., description="Comma-separated symbols (max 50)"),
    exchange: str = Query("NSE", regex="^(NSE|BSE)$"),
):
    """Fetch multiple quotes in a single request."""
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()][:50]

    if not symbol_list:
        raise HTTPException(status_code=400, detail="No valid symbols provided")

    market_state = MarketState(**get_market_state_dict())
    data: list[FullQuoteData] = []
    failed: list[str] = []

    # Bulk fetch from Redis
    ticks = await ticker_cache.get_bulk_ticks(symbol_list, exchange)

    miss: list[str] = []
    for sym in symbol_list:
        tick_data = ticks.get(sym)
        if tick_data:
            try:
                data.append(_build_quote_from_cache(tick_data))
            except Exception:
                miss.append(sym)
        else:
            miss.append(sym)

    # Fetch-through any cache misses so bulk works for arbitrary symbols too.
    for sym in miss:
        tick = await market_data_service.get_quote_through(sym, exchange, exclude_delayed=True)
        if tick:
            data.append(_build_quote_from_tick(tick))
        else:
            failed.append(sym)

    return BulkQuoteResponse(
        status="success" if not failed else "partial",
        timestamp=now_ist(),
        market_state=market_state,
        data=data,
        failed_symbols=failed,
        meta=ResponseMeta(
            source="redis_cache",
            data_quality=DATA_QUALITY_REALTIME if is_market_open() else DATA_QUALITY_EOD,
            provider_tier=1,
        ),
    )


# ── Internal Helpers ─────────────────────────────────────────────────────────

async def _get_realtime_quote(
    symbol: str, exchange: str, market_state: MarketState
) -> APIResponse[FullQuoteData]:
    """Build quote from Redis cache during market hours.

    On a cache miss (symbol outside the warm scraper set, or orchestrator not
    running) fall through to a live on-demand fetch via NSE → BSE → Yahoo.
    """
    tick_data = await ticker_cache.get_tick(symbol, exchange)

    if tick_data:
        cache_age = await ticker_cache.get_tick_age_ms(symbol, exchange) or 0
        quote = _build_quote_from_cache(tick_data)
        return APIResponse(
            status="success",
            timestamp=now_ist(),
            market_state=market_state,
            data=quote,
            meta=ResponseMeta(
                source=tick_data.get("source", "unknown"),
                source_latency_ms=float(tick_data.get("source_latency_ms", 0)),
                cache_age_ms=cache_age,
                data_quality=DATA_QUALITY_REALTIME if cache_age < 5000 else DATA_QUALITY_STALE,
                last_refresh=datetime.fromisoformat(tick_data["timestamp"]) if "timestamp" in tick_data else None,
                provider_tier=1,
            ),
        )

    # Cache miss → on-demand fetch-through. Live quotes: NSE/BSE only (no Yahoo).
    tick = await market_data_service.get_quote_through(symbol, exchange, exclude_delayed=True)
    if not tick:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for {symbol} on {exchange}",
        )

    return APIResponse(
        status="success",
        timestamp=now_ist(),
        market_state=market_state,
        data=_build_quote_from_tick(tick),
        meta=ResponseMeta(
            source=tick.source,
            source_latency_ms=tick.source_latency_ms,
            cache_age_ms=0,
            data_quality=DATA_QUALITY_REALTIME,
            last_refresh=tick.timestamp,
            provider_tier=1,
        ),
    )


async def _get_eod_quote(
    symbol: str, exchange: str, market_state: MarketState
) -> APIResponse[FullQuoteData]:
    """Build quote from TimescaleDB EOD data when market is closed."""
    # First try Redis cache (might have last market-hours data)
    tick_data = await ticker_cache.get_tick(symbol, exchange)
    if tick_data:
        quote = _build_quote_from_cache(tick_data)
        return APIResponse(
            status="success",
            timestamp=now_ist(),
            market_state=market_state,
            data=quote,
            meta=ResponseMeta(
                source=tick_data.get("source", "cache"),
                data_quality=DATA_QUALITY_EOD,
                provider_tier=1,
            ),
        )

    # Fall back to TimescaleDB (when connected)
    eod = None
    if db_manager.is_connected:
        try:
            session = db_manager.get_session()
            async with session:
                queries = TickerQueries(session)
                eod = await queries.get_latest_eod(symbol, exchange)
        except Exception as e:
            log.warning("eod_db_lookup_failed", symbol=symbol, error=str(e))

    if not eod:
        # No stored EOD — fetch from NSE/BSE (never Yahoo for quotes).
        tick = await market_data_service.get_quote_through(symbol, exchange, exclude_delayed=True)
        if not tick:
            raise HTTPException(status_code=404, detail=f"No data for {symbol}")
        return APIResponse(
            status="success",
            timestamp=now_ist(),
            market_state=market_state,
            data=_build_quote_from_tick(tick),
            meta=ResponseMeta(
                source=tick.source,
                data_quality=DATA_QUALITY_EOD,
                provider_tier=1,
            ),
        )

    try:
        quote = FullQuoteData(
            info=QuoteInfo(symbol=symbol, exchange=exchange),
            price=QuotePrice(
                ltp=eod["close"],
                open=eod["open"],
                high=eod["high"],
                low=eod["low"],
                close=eod["close"],
                change=Decimal("0"),
                pct_change=Decimal("0"),
                vwap=eod.get("vwap"),
            ),
            volume=QuoteVolume(
                total_traded_volume=eod.get("volume", 0),
                total_traded_value=eod.get("value"),
                delivery_quantity=eod.get("delivery_qty"),
                delivery_percentage=eod.get("delivery_pct"),
            ),
        )

        return APIResponse(
            status="success",
            timestamp=now_ist(),
            market_state=market_state,
            data=quote,
            meta=ResponseMeta(
                source="timescaledb",
                data_quality=DATA_QUALITY_EOD,
                provider_tier=0,
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        log.error("eod_query_error", symbol=symbol, error=str(e))
        raise HTTPException(status_code=500, detail="Database error")


def _build_quote_from_tick(tick) -> FullQuoteData:
    """Build a FullQuoteData directly from a fetched TickData object.

    Used by the on-demand fetch-through path so the response is independent of
    which exchange actually served the data (avoids cache-key mismatch when the
    failover cascades NSE → BSE → Yahoo).
    """
    return FullQuoteData(
        info=QuoteInfo(
            symbol=tick.symbol,
            isin=tick.isin,
            exchange=tick.exchange,
            series=tick.series,
        ),
        price=QuotePrice(
            ltp=tick.ltp, open=tick.open, high=tick.high, low=tick.low,
            close=tick.close, change=tick.change, pct_change=tick.pct_change,
            vwap=tick.vwap, upper_circuit=tick.upper_circuit,
            lower_circuit=tick.lower_circuit,
            week_52_high=tick.week_52_high, week_52_low=tick.week_52_low,
        ),
        volume=QuoteVolume(
            total_traded_volume=tick.volume,
            total_traded_value=tick.value,
        ),
    )


def _build_quote_from_cache(data: dict[str, Any]) -> FullQuoteData:
    """Build a FullQuoteData from a Redis hash dict."""
    return FullQuoteData(
        info=QuoteInfo(
            symbol=data.get("symbol", ""),
            isin=data.get("isin") or None,
            exchange=data.get("exchange", "NSE"),
            series=data.get("series") or None,
        ),
        price=QuotePrice(
            ltp=Decimal(data.get("ltp", "0")),
            open=Decimal(data.get("open", "0")),
            high=Decimal(data.get("high", "0")),
            low=Decimal(data.get("low", "0")),
            close=Decimal(data.get("close", "0")),
            change=Decimal(data.get("change", "0")),
            pct_change=Decimal(data.get("pct_change", "0")),
            vwap=Decimal(data["vwap"]) if data.get("vwap") else None,
            upper_circuit=Decimal(data["upper_circuit"]) if data.get("upper_circuit") else None,
            lower_circuit=Decimal(data["lower_circuit"]) if data.get("lower_circuit") else None,
            week_52_high=Decimal(data["week_52_high"]) if data.get("week_52_high") else None,
            week_52_low=Decimal(data["week_52_low"]) if data.get("week_52_low") else None,
        ),
        volume=QuoteVolume(
            total_traded_volume=int(data.get("volume", 0)),
            total_traded_value=Decimal(data["value"]) if data.get("value") else None,
        ),
    )
