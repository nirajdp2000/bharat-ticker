"""Shared on-demand market-data service.

The orchestrator keeps a fixed universe warm in the cache, but API consumers
ask for *arbitrary* symbols.  This module provides a single shared
:class:`FailoverController` plus two capabilities the REST layer needs:

    * ``get_quote_through`` — fetch-through on cache miss: hit NSE → BSE →
      Yahoo for any symbol, write it to the cache, and return it.  This makes
      ``/quote/{symbol}`` work for every listed name, even ones outside the
      scraper's warm set and even when the orchestrator isn't running.

    * ``get_history`` — historical OHLCV backfill via Yahoo Finance, used when
      TimescaleDB has no stored candles yet (so "old data" works on day one).
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from functools import partial
from typing import Any

from ..config.constants import canonical_symbol
from ..config.settings import settings
from ..models.tick import TickData
from ..utils.logger import get_logger
from .failover import FailoverController

log = get_logger(__name__)

# Map our interval codes to yfinance (interval, default_period).
_YF_INTERVAL = {
    "1m": ("1m", "7d"), "5m": ("5m", "60d"), "15m": ("15m", "60d"),
    "30m": ("30m", "60d"), "1h": ("60m", "730d"), "4h": ("60m", "730d"),
    "1d": ("1d", "5y"), "1w": ("1wk", "10y"),
}

# Index symbols → Yahoo index tickers. Indices have no exchange suffix on Yahoo
# (".NS"/".BO" would yield an invalid ticker → empty history). Friendly aliases
# and the literal "^"-ticker both resolve here so consumers can pass either.
_INDEX_YF = {
    "^NSEI": "^NSEI", "NSEI": "^NSEI", "NIFTY": "^NSEI", "NIFTY50": "^NSEI",
    "NIFTY 50": "^NSEI",
    "^NSEBANK": "^NSEBANK", "NSEBANK": "^NSEBANK", "BANKNIFTY": "^NSEBANK",
    "NIFTYBANK": "^NSEBANK", "NIFTY BANK": "^NSEBANK",
    "^CNXIT": "^CNXIT", "NIFTYIT": "^CNXIT", "NIFTY IT": "^CNXIT",
    "^BSESN": "^BSESN", "SENSEX": "^BSESN",
}


def _yahoo_symbol(symbol: str, exchange: str) -> str:
    """Resolve a request symbol to its Yahoo ticker.

    Indices map to "^"-tickers with no suffix; symbols that already carry a
    Yahoo qualifier ("^", ".", "=") pass through; equities get .NS/.BO.
    """
    idx = _INDEX_YF.get(symbol)
    if idx:
        return idx
    if symbol.startswith("^") or "." in symbol or "=" in symbol:
        return symbol
    return symbol + (".BO" if exchange == "BSE" else ".NS")


class MarketDataService:
    """Lazily-initialized shared provider stack for the serving layer."""

    def __init__(self) -> None:
        self._failover: FailoverController | None = None
        self._lock = asyncio.Lock()

    async def _controller(self) -> FailoverController:
        if self._failover is not None:
            return self._failover
        async with self._lock:
            if self._failover is not None:
                return self._failover
            fc = FailoverController()
            # Import here to avoid heavy imports at module load.
            if settings.nse_enabled:
                from ..providers.groww import GrowwProvider
                from ..providers.nse_archive import NSEArchiveProvider
                from ..providers.nse_live import NSELiveProvider
                # Groww public feed — richest NSE live quote (circuit/52wk/OI). Top priority.
                fc.register_provider(GrowwProvider())
                # Live NSE quotes via Moneycontrol relay (not Akamai-gated).
                fc.register_provider(NSELiveProvider())
                # Direct NSE quote-equity only with a proxy (Akamai-gated).
                if settings.proxy_list:
                    from ..providers.nse_scraper import NSEScraper
                    fc.register_provider(NSEScraper())
                fc.register_provider(NSEArchiveProvider())
            if settings.bse_enabled:
                from ..providers.bse_scraper import BSEScraper
                from ..providers.bse_archive import BSEArchiveProvider
                from ..providers.nse_live import BSELiveProvider
                fc.register_provider(BSEScraper())
                fc.register_provider(BSELiveProvider())
                fc.register_provider(BSEArchiveProvider())
            if settings.yahoo_enabled:
                from ..providers.yahoo_finance import YahooFinanceProvider
                fc.register_provider(YahooFinanceProvider())
            await fc.connect_all()
            self._failover = fc
            log.info("market_data_service_ready", providers=len(fc.providers))
            return fc

    # ── Live fetch-through ───────────────────────────────────────────────
    async def get_quote_through(
        self, symbol: str, exchange: str = "NSE", source: str | None = None,
        write_cache: bool = True, exclude_delayed: bool = False,
    ) -> TickData | None:
        """Fetch a live quote on demand and (optionally) write it to the cache.

        ``source`` ('nse'|'bse'|'yahoo'|'auto'|provider name) restricts which
        providers are used — drives the UI source-toggle buttons.  Forced-source
        reads pass ``write_cache=False`` so an explicit override doesn't pollute
        the shared default (auto) cache entry.
        """
        symbol = canonical_symbol(symbol)
        fc = await self._controller()
        try:
            tick = await fc.fetch_quote(symbol, source=source, exclude_delayed=exclude_delayed)
        except Exception as e:
            log.warning("fetch_through_failed", symbol=symbol, source=source, error=str(e))
            return None
        if write_cache:
            try:
                from ..cache.ticker_cache import ticker_cache
                await ticker_cache.set_tick(tick)
                if tick.market_depth:
                    await ticker_cache.set_market_depth(tick.symbol, tick.exchange, tick.market_depth)
            except Exception as e:
                log.debug("fetch_through_cache_write_failed", symbol=symbol, error=str(e))
        return tick

    async def warm(self) -> None:
        """Pre-build the provider stack (and load the NSE archive) at startup
        so the first user request doesn't pay the cold-init cost."""
        try:
            await self._controller()
        except Exception as e:
            log.warning("market_data_warm_failed", error=str(e))

    async def provider_health(self) -> dict[str, Any]:
        fc = await self._controller()
        return await fc.health_check_all()

    async def list_sources(self) -> list[dict[str, Any]]:
        fc = await self._controller()
        return await fc.list_status()

    async def set_source_enabled(self, name: str, enabled: bool) -> bool:
        fc = await self._controller()
        return fc.set_enabled(name, enabled)

    async def test_source(self, name: str, symbol: str = "RELIANCE") -> dict[str, Any]:
        fc = await self._controller()
        return await fc.test_provider(name, symbol)

    async def get_all_snapshot(self, exchange: str = "NSE") -> list[dict[str, Any]]:
        """Every stock on an exchange in one shot, served from memory (~0ms read).

        Bulk data = last-traded-day (archive); the live-polled subset is fresher.
        For true tick-by-tick all-stocks-live, a broker WebSocket is required.
        """
        fc = await self._controller()
        ex = exchange.upper()
        wants = ["nse_archive", "bse_archive"] if ex == "ALL" else \
                ["nse_archive"] if ex == "NSE" else ["bse_archive"]
        out: list[dict[str, Any]] = []
        for want in wants:
            for p in fc.providers:
                if p.name == want and hasattr(p, "snapshot"):
                    if not p.is_connected:
                        await p.connect()
                    out.extend(p.snapshot())  # type: ignore[attr-defined]
        return out

    async def get_isin(self, symbol: str) -> str | None:
        """Resolve a symbol's ISIN from the in-memory BSE master (no HTTP)."""
        symbol = canonical_symbol(symbol)
        try:
            from ..providers.bse_master import bse_master
            await bse_master.ensure_loaded()
            return bse_master.isin_for(symbol)
        except Exception as e:  # noqa: BLE001
            log.debug("get_isin_failed", symbol=symbol, error=str(e))
            return None

    async def get_indices(self) -> list[dict[str, Any]]:
        """Live NSE index values (NIFTY 50/Bank/sectoral — ~139 indices)."""
        from ..providers.nse_public import nse_public
        return await nse_public.fetch_indices()

    async def get_analytics(self, symbol: str, exchange: str = "NSE") -> dict[str, Any] | None:
        """Multi-period returns + volatility + 52wk range, computed from history.

        Matches the NSE quote-page boxes (1W/1M/YTD/1Y/3Y/5Y returns, daily &
        annualised volatility) — derived locally so it works without live NSE.
        """
        import math
        from datetime import datetime as _dt
        candles = await self.get_history(symbol, exchange, "1d", limit=1300)
        closes = [float(c["close"]) for c in candles if float(c.get("close", 0)) > 0]
        dates = [str(c.get("bucket", "")) for c in candles]
        if len(closes) < 10:
            return None
        last = closes[-1]
        def ret(n: int):
            if len(closes) > n and closes[-1 - n] > 0:
                return round((last / closes[-1 - n] - 1) * 100, 2)
            return None
        # YTD: first close of the current year
        year = _dt.now().year
        ytd = None
        for d, c in zip(dates, closes):
            if d[:4] == str(year) and c > 0:
                ytd = round((last / c - 1) * 100, 2); break
        # daily log/simple returns for volatility (last ~250 sessions)
        window = closes[-251:]
        rets = [window[i] / window[i - 1] - 1 for i in range(1, len(window)) if window[i - 1] > 0]
        daily_vol = (sum((r - sum(rets) / len(rets)) ** 2 for r in rets) / len(rets)) ** 0.5 if rets else 0.0
        yr = closes[-252:] if len(closes) >= 252 else closes
        return {
            "symbol": symbol.upper(), "exchange": exchange, "ltp": round(last, 2),
            "returns_pct": {
                "1W": ret(5), "1M": ret(21), "YTD": ytd,
                "1Y": ret(252), "3Y": ret(756), "5Y": ret(1260),
            },
            "volatility": {
                "daily_pct": round(daily_vol * 100, 2),
                "annualised_pct": round(daily_vol * math.sqrt(252) * 100, 2),
            },
            "week_52_high": round(max(yr), 2), "week_52_low": round(min(yr), 2),
            "sessions": len(closes),
        }

    async def get_nse_full(self, symbol: str) -> dict[str, Any]:
        """NSE trade/price/securities-info quote (live, proxy-gated)."""
        from ..providers.nse_public import nse_public
        return await nse_public.fetch_quote_full(symbol)

    async def get_option_chain(self, symbol: str) -> dict[str, Any]:
        """NSE option chain (live, proxy-gated)."""
        from ..providers.nse_public import nse_public
        return await nse_public.fetch_option_chain(symbol)

    async def get_depth(self, symbol: str) -> dict[str, Any] | None:
        """Live BSE 5-level order book for a symbol."""
        fc = await self._controller()
        for p in fc.providers:
            if p.name == "bse_scraper" and hasattr(p, "fetch_depth"):
                try:
                    if not p.is_connected:
                        await p.connect()
                    return await p.fetch_depth(symbol)  # type: ignore[attr-defined]
                except Exception as e:
                    log.debug("get_depth_failed", symbol=symbol, error=str(e))
        return None

    # Short-TTL cache so many timeframe requests + diagnostics share ONE BSE
    # fetch (the StockReachGraph endpoint rate-limits rapid repeat calls).
    _intraday_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
    _INTRADAY_TTL_S = 45.0

    async def get_intraday_series(self, symbol: str, flag: str = "0") -> list[dict[str, Any]] | None:
        """Real intraday price/volume curve (BSE StockReachGraph). None if unavailable.

        flag "0"/"1" = today's session points; "3M"/"6M"/"1Y" = daily closes.
        Cached ~45s and retried once (BSE rate-limits rapid repeats).
        """
        import time as _time
        symbol = canonical_symbol(symbol)
        key = f"{symbol.upper()}:{flag}"
        hit = self._intraday_cache.get(key)
        if hit and (_time.time() - hit[0]) < self._INTRADAY_TTL_S and hit[1]:
            return hit[1]

        fc = await self._controller()
        for p in fc.providers:
            if p.name == "bse_scraper" and hasattr(p, "fetch_intraday_graph"):
                for attempt in range(2):
                    try:
                        if not p.is_connected:
                            await p.connect()
                        res = await p.fetch_intraday_graph(symbol, flag)  # type: ignore[attr-defined]
                        if res:
                            self._intraday_cache[key] = (_time.time(), res)
                            return res
                        # empty → re-warm cookies and retry once
                        if attempt == 0 and hasattr(p, "_warmed"):
                            p._warmed = 0.0  # force re-warm
                            await asyncio.sleep(0.4)
                    except Exception as e:
                        log.debug("get_intraday_series_failed", symbol=symbol, attempt=attempt, error=str(e))
                        await asyncio.sleep(0.4)
        # serve a slightly-stale cached copy rather than nothing
        return hit[1] if hit else None

    async def get_details(self, symbol: str, source: str = "auto") -> dict[str, Any] | None:
        """Rich fundamentals + stats panel (BSE live aggregate / NSE archive)."""
        from .failover import _source_match
        symbol = canonical_symbol(symbol)
        fc = await self._controller()
        # nse_live (Moneycontrol, ~155 fields) is richest → prefer for auto/nse.
        order = ["bse", "nse"] if source == "bse" else ["nse", "bse"]
        for src in order:
            for p in fc.providers:
                if _source_match(p, src) and hasattr(p, "fetch_details") and fc.is_enabled(p.name):
                    try:
                        if not p.is_connected:
                            await p.connect()
                        d = await p.fetch_details(symbol)  # type: ignore[attr-defined]
                        if d:
                            return d
                    except Exception as e:
                        log.debug("get_details_failed", symbol=symbol, src=src, error=str(e))
        return None

    # ── Historical backfill (Yahoo) ──────────────────────────────────────
    async def get_history(
        self,
        symbol: str,
        exchange: str = "NSE",
        interval: str = "1d",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Return historical OHLCV candles via Yahoo Finance."""
        symbol = canonical_symbol(symbol)
        yf_interval, default_period = _YF_INTERVAL.get(interval, ("1d", "5y"))
        yf_symbol = _yahoo_symbol(symbol, exchange)

        loop = asyncio.get_event_loop()
        try:
            candles = await loop.run_in_executor(
                None,
                partial(self._yahoo_history_sync, yf_symbol, yf_interval, default_period, start, end),
            )
        except Exception as e:
            log.warning("yahoo_history_failed", symbol=symbol, error=str(e))
            return []
        return candles[-limit:] if limit else candles

    @staticmethod
    def _yahoo_history_sync(
        yf_symbol: str, yf_interval: str, period: str,
        start: datetime | None, end: datetime | None,
    ) -> list[dict[str, Any]]:
        import yfinance as yf

        ticker = yf.Ticker(yf_symbol)
        if start is not None:
            df = ticker.history(start=start, end=end, interval=yf_interval, auto_adjust=False)
        else:
            df = ticker.history(period=period, interval=yf_interval, auto_adjust=False)
        if df is None or df.empty:
            return []

        out: list[dict[str, Any]] = []
        for ts, row in df.iterrows():
            out.append({
                "bucket": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                "open": str(round(float(row.get("Open", 0) or 0), 2)),
                "high": str(round(float(row.get("High", 0) or 0), 2)),
                "low": str(round(float(row.get("Low", 0) or 0), 2)),
                "close": str(round(float(row.get("Close", 0) or 0), 2)),
                "volume": int(row.get("Volume", 0) or 0),
                "value": None,
                "vwap": None,
            })
        return out


# Singleton
market_data_service = MarketDataService()
