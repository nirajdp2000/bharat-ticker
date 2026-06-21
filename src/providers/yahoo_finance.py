"""Tier-3 Fallback: Yahoo Finance via yfinance.

This is the most reliable fallback — Yahoo Finance rarely goes down,
but the data is typically delayed by 15 minutes for Indian stocks.
Used as a last resort when Tier-1 (NSE/BSE scraping) and Tier-2
(broker APIs) are both unavailable.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from decimal import Decimal
from functools import partial
from typing import Any
from zoneinfo import ZoneInfo

# NOTE: yfinance (and its heavy pandas/numpy deps) is imported LAZILY inside the
# fetch helpers below — not at module load. Yahoo is the delayed last-resort feed
# (never the live path), so deferring the import keeps ~80-150 MB of pandas/numpy
# out of RAM until a Yahoo fallback actually fires. Live providers are unaffected.

from ..config.constants import YAHOO_NSE_SUFFIX
from ..models.tick import TickData
from ..utils.logger import get_logger
from .base import DataProvider, ProviderError

log = get_logger(__name__)
IST = ZoneInfo("Asia/Kolkata")


class YahooFinanceProvider(DataProvider):
    """Yahoo Finance data provider via yfinance library."""

    name = "yahoo_finance"
    tier = 3
    priority = 40  # Last resort
    delayed = True  # 15-min delayed — historical only, never the live feed
    exchange = "BOTH"

    async def connect(self) -> None:
        """No persistent connection needed for yfinance."""
        self._is_connected = True
        log.info("yahoo_finance_connected")

    async def disconnect(self) -> None:
        self._is_connected = False
        log.info("yahoo_finance_disconnected")

    async def fetch_quote(self, symbol: str) -> TickData:
        """Fetch a quote from Yahoo Finance.

        Yahoo uses .NS suffix for NSE and .BO for BSE.
        We default to NSE (.NS).
        """
        start = time.time()
        yahoo_symbol = symbol + YAHOO_NSE_SUFFIX if "." not in symbol else symbol

        try:
            # yfinance is synchronous — run in executor
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(
                None,
                partial(self._fetch_sync, yahoo_symbol),
            )
            latency_ms = (time.time() - start) * 1000

            tick = self._parse_yahoo_data(info, symbol, latency_ms)
            self.record_success(latency_ms)
            log.debug("yahoo_quote_fetched", symbol=symbol, ltp=str(tick.ltp), latency_ms=round(latency_ms, 1))
            return tick

        except Exception as e:
            self.record_error()
            log.error("yahoo_fetch_error", symbol=symbol, error=str(e))
            raise ProviderError(self.name, f"Yahoo Finance failed for {symbol}: {e}")

    async def fetch_bulk(self, symbols: list[str]) -> list[TickData]:
        """Fetch multiple quotes from Yahoo Finance."""
        results: list[TickData] = []
        # yfinance supports batch downloads
        yahoo_symbols = [
            (s + YAHOO_NSE_SUFFIX if "." not in s else s) for s in symbols
        ]

        start = time.time()
        try:
            loop = asyncio.get_event_loop()
            tickers_data = await loop.run_in_executor(
                None,
                partial(self._fetch_bulk_sync, yahoo_symbols),
            )

            for orig_symbol, yahoo_sym in zip(symbols, yahoo_symbols):
                info = tickers_data.get(yahoo_sym)
                if info:
                    try:
                        latency_ms = (time.time() - start) * 1000
                        tick = self._parse_yahoo_data(info, orig_symbol, latency_ms)
                        results.append(tick)
                    except Exception as e:
                        log.warning("yahoo_bulk_parse_error", symbol=orig_symbol, error=str(e))

        except Exception as e:
            log.error("yahoo_bulk_fetch_error", error=str(e))

        return results

    async def health_check(self) -> bool:
        """Check if Yahoo Finance is accessible."""
        try:
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(
                None,
                partial(self._fetch_sync, "RELIANCE.NS"),
            )
            return info.get("regularMarketPrice", 0) > 0
        except Exception:
            return False

    # ── Synchronous Helpers (run in executor) ────────────────────────────

    @staticmethod
    def _fetch_sync(yahoo_symbol: str) -> dict[str, Any]:
        """Synchronous single-ticker fetch."""
        import yfinance as yf  # lazy — loads pandas/numpy only when Yahoo is used
        ticker = yf.Ticker(yahoo_symbol)
        info = ticker.info
        if not info or info.get("regularMarketPrice") is None:
            # Fallback to fast_info
            fi = ticker.fast_info
            return {
                "regularMarketPrice": getattr(fi, "last_price", 0),
                "regularMarketOpen": getattr(fi, "open", 0),
                "regularMarketDayHigh": getattr(fi, "day_high", 0),
                "regularMarketDayLow": getattr(fi, "day_low", 0),
                "regularMarketPreviousClose": getattr(fi, "previous_close", 0),
                "regularMarketVolume": getattr(fi, "last_volume", 0),
                "fiftyTwoWeekHigh": getattr(fi, "year_high", None),
                "fiftyTwoWeekLow": getattr(fi, "year_low", None),
                "marketCap": getattr(fi, "market_cap", None),
            }
        return info

    @staticmethod
    def _fetch_bulk_sync(yahoo_symbols: list[str]) -> dict[str, dict[str, Any]]:
        """Synchronous bulk fetch using yfinance Tickers."""
        import yfinance as yf  # lazy — loads pandas/numpy only when Yahoo is used
        result: dict[str, dict[str, Any]] = {}
        tickers = yf.Tickers(" ".join(yahoo_symbols))
        for sym in yahoo_symbols:
            try:
                ticker = tickers.tickers.get(sym)
                if ticker:
                    info = ticker.info
                    if info:
                        result[sym] = info
            except Exception:
                continue
        return result

    # ── Response Parsing ─────────────────────────────────────────────────

    def _parse_yahoo_data(self, info: dict[str, Any], symbol: str, latency_ms: float) -> TickData:
        """Parse yfinance info dict into TickData."""
        ltp = Decimal(str(info.get("regularMarketPrice", 0) or 0))
        open_price = Decimal(str(info.get("regularMarketOpen", 0) or 0))
        high = Decimal(str(info.get("regularMarketDayHigh", 0) or 0))
        low = Decimal(str(info.get("regularMarketDayLow", 0) or 0))
        close = Decimal(str(info.get("regularMarketPreviousClose", 0) or 0))
        volume = int(info.get("regularMarketVolume", 0) or 0)

        change = ltp - close if close > 0 else Decimal("0")
        pct_change = (change / close * 100) if close > 0 else Decimal("0")

        w52_high = info.get("fiftyTwoWeekHigh")
        w52_low = info.get("fiftyTwoWeekLow")

        return TickData(
            symbol=symbol,
            isin=None,
            exchange="NSE",
            series=None,
            ltp=ltp,
            open=open_price if open_price > 0 else ltp,
            high=high if high > 0 else ltp,
            low=low if low > 0 else ltp,
            close=close,
            change=change,
            pct_change=pct_change.quantize(Decimal("0.01")),
            volume=volume,
            value=None,
            vwap=None,
            upper_circuit=None,
            lower_circuit=None,
            week_52_high=Decimal(str(w52_high)) if w52_high else None,
            week_52_low=Decimal(str(w52_low)) if w52_low else None,
            market_depth=None,  # Yahoo doesn't provide L2 data
            timestamp=datetime.now(tz=IST),
            source=self.name,
            source_latency_ms=latency_ms,
        )
