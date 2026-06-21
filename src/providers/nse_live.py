"""Tier-1 NSE LIVE — real-time NSE/BSE quotes relayed via Moneycontrol.

NSE's own ``/api/quote-equity`` is Akamai bot-gated and unreachable from
automated contexts.  Moneycontrol's public price feed relays the **same live
NSE/BSE exchange data** and is NOT bot-gated, so it provides genuine real-time
quotes (LTP/OHLC/prev-close/52wk/volume) without a broker account or proxy.

Resolution: symbol → Moneycontrol ``sc_id`` via the autosuggest endpoint
(cached for the process; seeded for the heaviest names).
"""

from __future__ import annotations

import asyncio
import json
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
PRICE = "https://priceapi.moneycontrol.com/pricefeed/{ex}/equitycash/{sc}"
SUGGEST = ("https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php"
           "?classic=true&query={q}&type=1&format=json&callback=")

# Seed sc_id for the heaviest names (instant, offline).
SC_SEED: dict[str, str] = {
    "RELIANCE": "RI", "TCS": "TCS", "INFY": "IT", "HDFCBANK": "HDF01",
    "ICICIBANK": "ICI02", "SBIN": "SBI", "HINDUNILVR": "HL", "ITC": "ITC",
    "BHARTIARTL": "BA08", "KOTAKBANK": "KMF", "LT": "LT", "AXISBANK": "AB16",
    "BAJFINANCE": "BAF", "MARUTI": "MU01", "ASIANPAINT": "API", "HCLTECH": "HCL02",
    "SUNPHARMA": "SPI", "TITAN": "TI01", "WIPRO": "W", "ULTRACEMCO": "UTC01",
    # NOTE: legacy "TATAMOTORS" dropped — NSE split it into TMCV (commercial) +
    # TMPV (passenger, orig ISIN). Seeding it pointed MC at the wrong entity; the
    # /sb layer aliases TATAMOTORS→TMPV. Use TMCV/TMPV directly.
    "TATASTEEL": "TIS", "APOLLOTYRE": "AT", "IRCTC": "IRC",
    "ETERNAL": "Z01",
}


class NSELiveProvider(DataProvider):
    """Live NSE quotes (Moneycontrol relay of NSE exchange data)."""

    name = "nse_live"
    tier = 1
    priority = 10           # Live, tried first
    delayed = False
    exchange = "NSE"
    _ex_code = "nse"        # subclass/override -> "bse"

    def __init__(self) -> None:
        super().__init__()
        self._session: AsyncSession | None = None
        self._sc: dict[str, str] = dict(SC_SEED)

    async def connect(self) -> None:
        if self._is_connected:
            return
        self._session = AsyncSession(impersonate=fingerprint_mgr.get_random(),
                                     timeout=settings.scrape_timeout_seconds, verify=True)
        self._is_connected = True
        log.info("nse_live_connected", ex=self._ex_code)

    async def disconnect(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        self._is_connected = False

    async def _resolve(self, symbol: str) -> str | None:
        symbol = symbol.upper()
        if symbol in self._sc:
            return self._sc[symbol]
        try:
            r = await self._session.get(SUGGEST.format(q=symbol), headers={"User-Agent": UA})
            txt = r.text.strip().lstrip("(").rstrip(");").rstrip(")")
            rows = json.loads(txt) if txt[:1] in "[{" else []
            for row in (rows if isinstance(rows, list) else []):
                sc = (row.get("sc_id") or "").strip()
                nm = (row.get("link_src") or "").upper()
                if sc:
                    self._sc[symbol] = sc
                    return sc
        except Exception as e:
            log.debug("nse_live_resolve_failed", symbol=symbol, error=str(e))
        return None

    async def fetch_quote(self, symbol: str) -> TickData:
        if not self._is_connected:
            await self.connect()
        sc = await self._resolve(symbol)
        if not sc:
            self.record_error()
            raise ProviderError(self.name, f"No Moneycontrol code for {symbol}")
        start = time.time()
        try:
            r = await self._session.get(
                PRICE.format(ex=self._ex_code, sc=sc), headers={"User-Agent": UA})
            latency = (time.time() - start) * 1000
            if r.status_code != 200 or r.text[:1] not in "{[":
                self.record_error()
                raise ProviderError(self.name, f"HTTP {r.status_code} for {symbol}")
            d = (r.json() or {}).get("data", {})
            tick = self._parse(d, symbol, latency)
            self.record_success(latency)
            return tick
        except ProviderError:
            raise
        except Exception as e:
            self.record_error()
            raise ProviderError(self.name, f"Failed {symbol}: {e}")

    async def fetch_details(self, symbol: str) -> dict[str, Any] | None:
        """Comprehensive panel from the Moneycontrol feed (~155 fields).

        Pulls price, bands, 52wk/lifetime range, moving averages, bid/ask,
        traded stats, fundamentals, full multi-period returns, IDs and the
        exchange ``last_updated`` timestamp (so data freshness is explicit).
        """
        if not self._is_connected:
            await self.connect()
        sc = await self._resolve(symbol)
        if not sc:
            return None
        try:
            r = await self._session.get(
                PRICE.format(ex=self._ex_code, sc=sc), headers={"User-Agent": UA})
            if r.status_code != 200 or r.text[:1] not in "{[":
                return None
            d = (r.json() or {}).get("data", {})
        except Exception as e:
            log.debug("nse_live_details_failed", symbol=symbol, error=str(e))
            return None
        if not d.get("pricecurrent"):
            return None
        gv = d.get
        def ret(p):  # MC return % for a period prefix
            return gv(f"cl{p}PerChange")
        return {
            "symbol": symbol.upper(), "name": d.get("company") or symbol,
            "exchange": "BSE" if self._ex_code == "bse" else "NSE",
            "source": self.name, "last_updated": d.get("lastupd"),
            "market_state": d.get("market_state"), "session": d.get("sessionId"),
            "price": {
                "ltp": gv("pricecurrent"), "change": gv("pricechange"),
                "pct_change": gv("pricepercentchange"), "open": gv("OPN"),
                "high": gv("HP"), "low": gv("LP"), "prev_close": gv("priceprevclose"),
                "vwap": gv("AVGP"), "upper_band": gv("upper_circuit_limit"),
                "lower_band": gv("lower_circuit_limit"),
            },
            "range": {
                "week_52_high": gv("52H"), "week_52_high_date": gv("52HDate"),
                "week_52_low": gv("52L"), "week_52_low_date": gv("52LDate"),
                "lifetime_high": gv("LTH"), "lifetime_high_date": gv("LTHDate"),
                "lifetime_low": gv("LTL"), "lifetime_low_date": gv("LTLDate"),
            },
            "moving_averages": {
                "5d": gv("5DayAvg"), "30d": gv("30DayAvg"), "50d": gv("50DayAvg"),
                "150d": gv("150DayAvg"), "200d": gv("200DayAvg"),
            },
            "bid_ask": {
                "bid": gv("BIDP"), "bid_qty": gv("BIDQ"),
                "ask": gv("OFFERP"), "ask_qty": gv("OFFERQ"),
                "total_buy_qty": gv("tot_buy_qty"), "total_sell_qty": gv("tot_sell_qty"),
            },
            "traded": {
                "volume": gv("VOL"), "mcap_cr": gv("MKTCAP"), "lot_size": gv("MKT_LOT"),
                "delivery_pct": gv("DELV"), "avg_delivery_20d_pct": gv("AvgDelVolPer_20day"),
                "avg_volume_20d": gv("DVolAvg20"),
            },
            "fundamentals": {
                "isin": gv("isinid"), "face_value": gv("FV"), "eps_ttm": gv("sc_ttm_cons"),
                "pe": gv("sc_mapindex"), "industry_pe": gv("IND_PE"),
                "book_value": gv("BV"), "book_value_cons": gv("BVCONS"), "ceps": gv("CEPS"),
                "div_yield": gv("DY"), "dividend": gv("DIVPR"),
                "sector": gv("main_sector"), "subsector": gv("newSubsector"),
                "nse_id": gv("NSEID"), "bse_id": gv("BSEID"),
            },
            "returns_pct": {
                "1W": ret("1w"), "1M": ret("1m"), "3M": ret("3m"), "6M": ret("6m"),
                "YTD": gv("clYtdPerChange"), "1Y": ret("1y"), "2Y": ret("2y"),
                "3Y": ret("3y"), "5Y": ret("5y"), "7Y": ret("7y"),
            },
        }

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
                PRICE.format(ex=self._ex_code, sc="RI"), headers={"User-Agent": UA})
            return r.status_code == 200 and float(r.json()["data"]["pricecurrent"]) > 0
        except Exception:
            return False

    def _parse(self, d: dict[str, Any], symbol: str, latency: float) -> TickData:
        g = self._dec
        ltp = g(d.get("pricecurrent")); prev = g(d.get("priceprevclose"))
        if ltp <= 0:
            raise ProviderError(self.name, "empty MC payload")
        return TickData(
            symbol=symbol.upper(), isin=None,
            exchange="BSE" if self._ex_code == "bse" else "NSE",
            series="EQ", ltp=ltp,
            open=g(d.get("OPN")) or ltp, high=g(d.get("HP")) or ltp, low=g(d.get("LP")) or ltp,
            close=prev, change=g(d.get("pricechange")), pct_change=g(d.get("pricepercentchange")),
            volume=int(g(d.get("VOL"))), value=None, vwap=g(d.get("VWAP")) or None,
            upper_circuit=None, lower_circuit=None,
            week_52_high=g(d.get("52H")) or None, week_52_low=g(d.get("52L")) or None,
            market_depth=None, timestamp=datetime.now(tz=IST),
            source=self.name, source_latency_ms=latency,
        )

    @staticmethod
    def _dec(v: Any) -> Decimal:
        if v is None:
            return Decimal("0")
        try:
            s = str(v).replace(",", "").strip()
            return Decimal(s) if s and s not in ("-", "NA") else Decimal("0")
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")


class BSELiveProvider(NSELiveProvider):
    """Live BSE quotes (Moneycontrol relay) — backup to the direct BSE API."""
    name = "bse_live"
    priority = 13
    exchange = "BSE"
    _ex_code = "bse"
