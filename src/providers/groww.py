"""Tier-1 NSE LIVE — Groww public price feed.

Groww's public `latest_prices_ohlc` endpoint relays real NSE exchange data with
the RICHEST field set of any reachable source: LTP/OHLC, change, volume, **upper
& lower circuit limits**, **52-week range**, total buy/sell quantity, and open
interest — all without auth, sub-second. Used as the top-priority NSE quote
provider (richer than the Moneycontrol relay).
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from curl_cffi.requests import AsyncSession

from ..config.settings import settings
from ..models.tick import TickData
from ..utils.fingerprint import fingerprint_mgr
from ..utils.logger import get_logger
from .base import DataProvider, ProviderError

log = get_logger(__name__)
IST = ZoneInfo("Asia/Kolkata")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
OHLC = ("https://groww.in/v1/api/stocks_data/v1/accord_points/exchange/{ex}"
        "/segment/CASH/latest_prices_ohlc/{sym}")


class GrowwProvider(DataProvider):
    """Live NSE quotes via Groww's public accord_points feed."""

    name = "groww"
    tier = 1
    priority = 5            # richest live source → tried first
    delayed = False
    exchange = "NSE"
    _ex_code = "NSE"

    def __init__(self) -> None:
        super().__init__()
        self._session: AsyncSession | None = None

    async def connect(self) -> None:
        if self._is_connected:
            return
        self._session = AsyncSession(impersonate=fingerprint_mgr.get_random(),
                                     timeout=settings.scrape_timeout_seconds, verify=True)
        self._is_connected = True
        log.info("groww_connected")

    async def disconnect(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        self._is_connected = False

    async def fetch_quote(self, symbol: str) -> TickData:
        if not self._is_connected:
            await self.connect()
        symbol = symbol.upper()
        start = time.time()
        try:
            r = await self._session.get(
                OHLC.format(ex=self._ex_code, sym=symbol), headers={"User-Agent": UA})
            latency = (time.time() - start) * 1000
            if r.status_code != 200 or r.text[:1] not in "{[":
                self.record_error()
                raise ProviderError(self.name, f"HTTP {r.status_code} for {symbol}")
            d = r.json() or {}
            tick = self._parse(d, symbol, latency)
            self.record_success(latency)
            return tick
        except ProviderError:
            raise
        except Exception as e:
            self.record_error()
            raise ProviderError(self.name, f"Failed {symbol}: {e}")

    async def fetch_bulk(self, symbols: list[str]) -> list[TickData]:
        sem = asyncio.Semaphore(min(settings.scrape_concurrency, 8))
        out: list[TickData] = []

        async def one(s: str) -> None:
            async with sem:
                try:
                    out.append(await self.fetch_quote(s))
                except Exception:
                    pass
        await asyncio.gather(*(one(s) for s in symbols))
        return out

    async def health_check(self) -> bool:
        try:
            if not self._is_connected:
                await self.connect()
            r = await self._session.get(
                OHLC.format(ex=self._ex_code, sym="RELIANCE"), headers={"User-Agent": UA})
            return r.status_code == 200 and float((r.json() or {}).get("ltp", 0)) > 0
        except Exception:
            return False

    def _parse(self, d: dict[str, Any], symbol: str, latency: float) -> TickData:
        g = self._dec
        ltp = g(d.get("ltp")); prev = g(d.get("close"))
        if ltp <= 0 and prev <= 0:
            raise ProviderError(self.name, "empty Groww payload")
        if ltp <= 0:
            ltp = prev
        return TickData(
            symbol=symbol, isin=None, exchange="NSE", series="EQ",
            ltp=ltp, open=g(d.get("open")) or ltp, high=g(d.get("high")) or ltp,
            low=g(d.get("low")) or ltp, close=prev,
            change=g(d.get("dayChange")), pct_change=g(d.get("dayChangePerc")),
            volume=int(g(d.get("volume"))), value=None, vwap=None,
            upper_circuit=g(d.get("highPriceRange")) or None,
            lower_circuit=g(d.get("lowPriceRange")) or None,
            week_52_high=g(d.get("yearHighPrice")) or None,
            week_52_low=g(d.get("yearLowPrice")) or None,
            total_buy_qty=int(g(d.get("totalBuyQty"))) if d.get("totalBuyQty") is not None else None,
            total_sell_qty=int(g(d.get("totalSellQty"))) if d.get("totalSellQty") is not None else None,
            open_interest=int(g(d.get("openInterest"))) if d.get("openInterest") else None,
            market_depth=None, timestamp=datetime.now(tz=IST),
            source=self.name, source_latency_ms=latency,
        )

    @staticmethod
    def _dec(v: Any) -> Decimal:
        if v is None:
            return Decimal("0")
        try:
            s = str(v).replace(",", "").strip()
            return Decimal(s) if s and s not in ("-", "NA", "None") else Decimal("0")
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")
