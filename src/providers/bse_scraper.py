"""Tier-1 Primary: BSE Internal JSON API Scraper.

BSE serves its website data from ``api.bseindia.com/BseIndiaAPI``.  Unlike NSE,
BSE keys everything off a numeric **scrip code** (e.g. RELIANCE = 500325), not
the trading symbol.  This provider therefore maintains a symbol→scripcode map:

    1. A built-in seed for the most-traded names (instant, offline).
    2. Lazy resolution via BSE's autocomplete endpoint for anything else,
       cached for the process lifetime.

Like the NSE scraper it uses ``curl_cffi`` TLS impersonation and rotating
browser headers.  BSE additionally requires ``Origin``/``Referer`` to point at
``www.bseindia.com`` or it returns empty payloads.
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
from ..utils.user_agents import ua_rotator
from ..utils.logger import get_logger
from .base import DataProvider, ProviderBlockedError, ProviderError, ProviderSchemaError

log = get_logger(__name__)
IST = ZoneInfo("Asia/Kolkata")

# BSE internal API host (separate from the www host).
BSE_API = "https://api.bseindia.com/BseIndiaAPI/api"

# Seed map for the heaviest names so common symbols work with zero lookups.
BSE_SCRIP_CODES: dict[str, str] = {
    "RELIANCE": "500325", "TCS": "532540", "INFY": "500209", "HDFCBANK": "500180",
    "ICICIBANK": "532174", "SBIN": "500112", "HINDUNILVR": "500696", "ITC": "500875",
    "BHARTIARTL": "532454", "KOTAKBANK": "500247", "LT": "500510", "AXISBANK": "532215",
    "BAJFINANCE": "500034", "MARUTI": "532500", "ASIANPAINT": "500820", "HCLTECH": "532281",
    "SUNPHARMA": "524715", "TITAN": "500114", "WIPRO": "507685", "ULTRACEMCO": "532538",
    "NESTLEIND": "500790", "TATAMOTORS": "500570", "TATASTEEL": "500470", "POWERGRID": "532898",
    "NTPC": "532555", "ONGC": "500312", "COALINDIA": "533278", "JSWSTEEL": "500228",
    "ADANIENT": "512599", "ADANIPORTS": "532921", "ADANIENTERP": "512599",
}


class BSEScraper(DataProvider):
    """Asynchronous BSE internal API scraper with TLS impersonation."""

    name = "bse_scraper"
    tier = 1
    priority = 11  # BSE realtime (live JSON API) — right after NSE realtime
    exchange = "BSE"

    def __init__(self) -> None:
        super().__init__()
        self._session: AsyncSession | None = None
        self._code_cache: dict[str, str] = dict(BSE_SCRIP_CODES)
        self._warmed: float = 0.0

    # ── Lifecycle ────────────────────────────────────────────────────────
    async def connect(self) -> None:
        if self._is_connected:
            return
        self._session = AsyncSession(
            impersonate=fingerprint_mgr.get_random(),
            timeout=settings.scrape_timeout_seconds,
            verify=True,
        )
        await self._warmup()
        self._is_connected = True
        log.info("bse_scraper_connected")

    async def disconnect(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        self._is_connected = False
        log.info("bse_scraper_disconnected")

    async def _warmup(self) -> None:
        """Touch the BSE homepage to obtain session cookies."""
        now = time.time()
        if now - self._warmed < settings.session_refresh_interval_seconds:
            return
        try:
            await self._session.get(settings.bse_base_url + "/", headers=self._headers())
            self._warmed = now
        except Exception as e:
            log.debug("bse_warmup_failed", error=str(e))

    def _headers(self, referer: str | None = None) -> dict[str, str]:
        h = ua_rotator.get_headers(referer=referer or settings.bse_base_url)
        h["Origin"] = settings.bse_base_url
        h["Referer"] = settings.bse_base_url + "/"
        return h

    # ── Scrip-code resolution ────────────────────────────────────────────
    async def _resolve_code(self, symbol: str) -> str | None:
        """Resolve a BSE scrip code via the official bhavcopy-derived master.

        Far more durable than the website autocomplete (which is HTML/JS-gated
        and changes often). Falls back to the built-in seed for offline use.
        """
        symbol = symbol.upper()
        if symbol in self._code_cache:
            return self._code_cache[symbol]
        try:
            from .bse_master import bse_master
            await bse_master.ensure_loaded()
            code = bse_master.code_for(symbol)
            if code:
                self._code_cache[symbol] = code
                return code
        except Exception as e:
            log.debug("bse_master_resolve_failed", symbol=symbol, error=str(e))
        return None

    # ── Data fetching ────────────────────────────────────────────────────
    async def fetch_quote(self, symbol: str) -> TickData:
        if not self._is_connected:
            await self.connect()
        await self._warmup()

        code = await self._resolve_code(symbol)
        if not code:
            self.record_error()
            raise ProviderError(self.name, f"No BSE scrip code for {symbol}")

        start = time.time()
        url = f"{BSE_API}/getScripHeaderData/w?Debtflag=&scripcode={code}&seriesid="
        try:
            resp = await self._session.get(url, headers=self._headers())
            latency_ms = (time.time() - start) * 1000

            if resp.status_code == 403:
                self.record_error()
                raise ProviderBlockedError(self.name, f"Blocked fetching {symbol}", 403)
            if resp.status_code != 200:
                self.record_error()
                raise ProviderError(self.name, f"HTTP {resp.status_code} for {symbol}", resp.status_code)

            data = resp.json()
            tick = self._parse(data, symbol, latency_ms)
            self.record_success(latency_ms)
            log.debug("bse_quote_fetched", symbol=symbol, ltp=str(tick.ltp), latency_ms=round(latency_ms, 1))
            return tick
        except (ProviderError, ProviderBlockedError):
            raise
        except Exception as e:
            self.record_error()
            raise ProviderError(self.name, f"Failed to fetch {symbol}: {e}")

    async def fetch_depth(self, symbol: str) -> dict[str, Any] | None:
        """Live 5-level order book from BSE MarketDepth/w (null when closed)."""
        if not self._is_connected:
            await self.connect()
        code = await self._resolve_code(symbol)
        if not code:
            return None
        try:
            r = await self._session.get(
                f"{BSE_API}/MarketDepth/w?Debtflag=&scripcode={code}&seriesid=",
                headers=self._headers(),
            )
            if r.status_code != 200 or r.text[:1] not in "{[":
                return None
            d = r.json()
        except Exception as e:
            log.debug("bse_depth_failed", symbol=symbol, error=str(e))
            return None
        bids, asks = [], []
        for i in range(1, 6):
            bp, bq = d.get(f"BPrice{i}"), d.get(f"BQty{i}")
            sp, sq = d.get(f"SPrice{i}"), d.get(f"SQty{i}")
            if bp:
                bids.append({"price": str(self._dec(bp)), "quantity": int(self._dec(bq))})
            if sp:
                asks.append({"price": str(self._dec(sp)), "quantity": int(self._dec(sq))})
        return {
            "buy": bids, "sell": asks,
            "total_buy_quantity": int(self._dec(d.get("TotalBQty"))),
            "total_sell_quantity": int(self._dec(d.get("TotalSQty"))),
            "updated_at": d.get("dttm"),
        }

    async def fetch_intraday_graph(self, symbol: str, flag: str = "0") -> list[dict[str, Any]] | None:
        """Real intraday price/volume curve from BSE StockReachGraph.

        ``flag`` "0"/"1" → today's session (one point ~per minute);
        "3M"/"6M"/"1Y" → daily closes. Returns ascending points
        ``[{timestamp, price, volume}]`` (volume is per-interval, not cumulative).
        This is genuine BSE exchange data — the real-time intraday source the
        Akamai-blocked NSE chart endpoint can't provide.
        """
        if not self._is_connected:
            await self.connect()
        await self._warmup()
        code = await self._resolve_code(symbol)
        if not code:
            return None
        try:
            r = await self._session.get(
                f"{BSE_API}/StockReachGraph/w?scripcode={code}&flag={flag}&fromdate=&todate=&seriesid=",
                headers=self._headers(),
            )
            if r.status_code != 200 or r.text[:1] not in "{[":
                return None
            payload = r.json()
        except Exception as e:
            log.debug("bse_intraday_graph_failed", symbol=symbol, error=str(e))
            return None

        import json as _json
        raw = payload.get("Data")
        pts = _json.loads(raw) if isinstance(raw, str) else (raw or [])
        out: list[dict[str, Any]] = []
        for p in pts:
            dttm = p.get("dttm")
            price = self._dec(p.get("vale1"))
            if not dttm or price <= 0:
                continue
            try:
                ts = datetime.strptime(dttm, "%a %b %d %Y %H:%M:%S").replace(tzinfo=IST)
            except ValueError:
                continue
            out.append({"timestamp": ts.isoformat(), "price": float(price),
                        "volume": int(self._dec(p.get("vole")))})
        out.sort(key=lambda x: x["timestamp"])
        return out

    async def fetch_details(self, symbol: str) -> dict[str, Any] | None:
        """Aggregate BSE's structured JSON endpoints into a rich detail panel.

        Combines getScripHeaderData (price/OHLC) + ComHeader (fundamentals) +
        HighLow (52wk/week/month) + StockTrading (VWAP/TTQ/turnover/mcap/band).
        All JSON — independent of the BSE website's HTML layout.
        """
        if not self._is_connected:
            await self.connect()
        await self._warmup()
        code = await self._resolve_code(symbol)
        if not code:
            return None

        async def _j(path: str) -> dict[str, Any]:
            try:
                r = await self._session.get(f"{BSE_API}/{path}", headers=self._headers())
                if r.status_code == 200 and r.text[:1] in "{[":
                    return r.json()
            except Exception as e:
                log.debug("bse_details_sub_failed", path=path, error=str(e))
            return {}

        hdr = await _j(f"getScripHeaderData/w?Debtflag=&scripcode={code}&seriesid=")
        com = await _j(f"ComHeader/w?quotetype=EQ&scripcode={code}&seriesid=")
        hl = await _j(f"HighLow/w?Type=EQ&flag=C&scripcode={code}")
        trd = await _j(f"StockTrading/w?flag=&quotetype=EQ&scripcode={code}")
        depth = await self.fetch_depth(symbol)

        H = hdr.get("Header", {}) or {}
        C = hdr.get("CurrRate", {}) or {}
        N = hdr.get("Cmpname", {}) or {}
        if not (C.get("LTP") or H.get("LTP")):
            return None

        return {
            "symbol": symbol.upper(),
            "name": N.get("FullN") or symbol,
            "exchange": "BSE",
            "scrip_code": code,
            "source": self.name,
            "as_on": H.get("Ason"),
            "price": {
                "ltp": C.get("LTP") or H.get("LTP"),
                "change": C.get("Chg"), "pct_change": C.get("PcChg"),
                "open": H.get("Open"), "high": H.get("High"), "low": H.get("Low"),
                "prev_close": H.get("PrevClose"), "vwap": trd.get("WAP"),
            },
            "range": {
                "week_52_high": hl.get("Fifty2WkHigh_unadj") or hl.get("Fifty2WkHigh_adj"),
                "week_52_low": hl.get("Fifty2WkLow_unadj") or hl.get("Fifty2WkLow_adj"),
                "month_high_low": hl.get("MonthHighLow"), "week_high_low": hl.get("WeekHighLow"),
                "price_band": trd.get("CktLimit"), "circuit_filter": hl.get("CktFilter"),
            },
            "traded": {
                "ttq_lakh": trd.get("TTQ"), "turnover_cr": trd.get("Turnover"),
                "two_wk_avg_qty_lakh": trd.get("TwoWkAvgQty"),
                "mcap_full_cr": trd.get("MktCapFull"), "mcap_ff_cr": trd.get("MktCapFF"),
            },
            "fundamentals": {
                "isin": com.get("ISIN"), "face_value": com.get("FaceVal"),
                "group": com.get("Group"), "index": com.get("Index"),
                "category": N.get("Category"),
                "sector": com.get("Sector"), "industry": com.get("Industry") or com.get("IndustryNew"),
                "eps": com.get("EPS"), "pe": com.get("PE"), "pb": com.get("PB"),
                "roe": com.get("ROE"), "ceps": com.get("CEPS"),
            },
            "market_depth": depth,
        }

    async def fetch_bulk(self, symbols: list[str]) -> list[TickData]:
        sem = asyncio.Semaphore(min(settings.scrape_concurrency, 6))
        results: list[TickData] = []

        async def one(sym: str) -> None:
            async with sem:
                try:
                    results.append(await self.fetch_quote(sym))
                except Exception as e:
                    log.debug("bse_bulk_error", symbol=sym, error=str(e))

        tasks = []
        for i, s in enumerate(symbols):
            tasks.append(one(s))
            if (i + 1) % 5 == 0:
                await asyncio.gather(*tasks)
                tasks = []
                await asyncio.sleep(0.3)
        if tasks:
            await asyncio.gather(*tasks)
        return results

    async def health_check(self) -> bool:
        try:
            if not self._is_connected:
                return False
            resp = await self._session.get(
                f"{BSE_API}/getScripHeaderData/w?Debtflag=&scripcode=500325&seriesid=",
                headers=self._headers(),
            )
            return resp.status_code == 200
        except Exception:
            return False

    # ── Parsing ──────────────────────────────────────────────────────────
    def _parse(self, data: dict[str, Any], symbol: str, latency_ms: float) -> TickData:
        try:
            header = data.get("Header", data) or {}
            curr = header.get("CurrRate", {}) or {}

            ltp = self._dec(curr.get("LTP") or header.get("LTP") or curr.get("Ltp"))
            prev_close = self._dec(header.get("PrevClose") or header.get("Prevclose"))
            open_p = self._dec(header.get("Open"))
            high = self._dec(header.get("High"))
            low = self._dec(header.get("Low"))
            change = self._dec(curr.get("Chg") or header.get("Change"))
            pct = self._dec(curr.get("PcChg") or header.get("PChange"))
            volume = int(self._dec(header.get("Volume") or header.get("TotalTradedQty")))
            wk_high = self._dec(header.get("Week52High") or header.get("WeekHigh"))
            wk_low = self._dec(header.get("Week52Low") or header.get("WeekLow"))

            if ltp <= 0 and prev_close <= 0:
                raise ValueError("empty BSE payload")

            if change == 0 and prev_close > 0:
                change = ltp - prev_close
            if pct == 0 and prev_close > 0:
                pct = (change / prev_close * 100).quantize(Decimal("0.01"))

            return TickData(
                symbol=symbol,
                isin=header.get("ISIN") or None,
                exchange="BSE",
                series="EQ",
                ltp=ltp if ltp > 0 else prev_close,
                open=open_p if open_p > 0 else (ltp if ltp > 0 else prev_close),
                high=high if high > 0 else (ltp if ltp > 0 else prev_close),
                low=low if low > 0 else (ltp if ltp > 0 else prev_close),
                close=prev_close,
                change=change,
                pct_change=pct,
                volume=volume,
                value=None,
                vwap=None,
                upper_circuit=None,
                lower_circuit=None,
                week_52_high=wk_high if wk_high > 0 else None,
                week_52_low=wk_low if wk_low > 0 else None,
                market_depth=None,
                timestamp=datetime.now(tz=IST),
                source=self.name,
                source_latency_ms=latency_ms,
            )
        except Exception as e:
            raise ProviderSchemaError(self.name, f"Failed to parse BSE response for {symbol}: {e}")

    @staticmethod
    def _dec(value: Any) -> Decimal:
        if value is None:
            return Decimal("0")
        try:
            if isinstance(value, str):
                value = value.replace(",", "").strip()
                if value in ("", "-"):
                    return Decimal("0")
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")
