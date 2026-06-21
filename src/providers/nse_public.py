"""NSE public (non-Akamai-gated) JSON endpoints.

A handful of NSE's internal APIs are reachable without the bot-gated cookie
flow that blocks ``/api/quote-equity``.  The most valuable is ``/api/allIndices``
— live values for all ~139 NSE indices (NIFTY 50, BANK, sectoral, etc.).  This
is genuine real-time NSE-origin index data and works from any context.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from curl_cffi.requests import AsyncSession

from ..config.settings import settings
from ..utils.fingerprint import fingerprint_mgr
from ..utils.user_agents import ua_rotator
from ..utils.logger import get_logger

log = get_logger(__name__)


class NSEPublic:
    """Cached fetcher for NSE's reachable public JSON endpoints."""

    def __init__(self) -> None:
        self._session: AsyncSession | None = None
        self._indices: list[dict[str, Any]] = []
        self._indices_at: float = 0.0
        self._lock = asyncio.Lock()

    async def _ensure_session(self) -> None:
        if self._session is None:
            kwargs: dict[str, Any] = dict(
                impersonate=fingerprint_mgr.get_random(),
                timeout=settings.scrape_timeout_seconds,
                verify=True,
            )
            # Route through a residential proxy when configured — unlocks the
            # Akamai-gated NSE endpoints (quote-equity, option-chain, …).
            if settings.proxy_list:
                proxy = settings.proxy_list[0]
                kwargs["proxies"] = {"http": proxy, "https": proxy}
            self._session = AsyncSession(**kwargs)
            try:
                await self._session.get(settings.nse_base_url + "/", headers=ua_rotator.get_headers())
            except Exception:
                pass

    async def _nse_json(self, path: str, referer: str | None = None) -> dict[str, Any] | None:
        """GET an NSE /api JSON path. Returns None on Akamai block / failure."""
        await self._ensure_session()
        try:
            r = await self._session.get(
                settings.nse_base_url + path,
                headers=ua_rotator.get_headers(referer=referer or settings.nse_base_url + "/"),
            )
            if r.status_code == 200 and r.text[:1] in "{[":
                return r.json()
            if r.status_code in (401, 403):
                return {"_blocked": True, "status": r.status_code}
        except Exception as e:
            log.debug("nse_json_failed", path=path, error=str(e))
        return None

    @staticmethod
    def _proxy_note() -> dict[str, Any]:
        return {
            "available": False,
            "requires_proxy": not bool(settings.proxy_list),
            "note": ("NSE live endpoint is Akamai-gated. Set PROXY_LIST to a residential "
                     "Indian proxy (or run from a residential IP) to enable it."),
        }

    async def fetch_quote_full(self, symbol: str) -> dict[str, Any]:
        """NSE full quote: trade / price / securities information."""
        symbol = symbol.upper()
        ref = f"{settings.nse_base_url}/get-quotes/equity?symbol={symbol}"
        j = await self._nse_json(f"/api/quote-equity?symbol={symbol}", ref)
        ti = await self._nse_json(f"/api/quote-equity?symbol={symbol}&section=trade_info", ref)
        if not j or j.get("_blocked"):
            return self._proxy_note()
        info = j.get("info", {}); pi = j.get("priceInfo", {}); md = j.get("metadata", {})
        si = j.get("securityInfo", {})
        tinfo = (ti or {}).get("marketDeptOrderBook", {}) if ti else {}
        sec = (ti or {}).get("securityWiseDP", {}) if ti else {}
        return {
            "available": True, "symbol": symbol, "name": info.get("companyName"),
            "exchange": "NSE", "source": "nse_quote_equity",
            "price": {
                "ltp": pi.get("lastPrice"), "change": pi.get("change"),
                "pct_change": pi.get("pChange"), "open": pi.get("open"),
                "high": pi.get("intraDayHighLow", {}).get("max"),
                "low": pi.get("intraDayHighLow", {}).get("min"),
                "prev_close": pi.get("previousClose"), "vwap": pi.get("vwap"),
                "upper_band": pi.get("upperCP"), "lower_band": pi.get("lowerCP"),
                "price_band": pi.get("pPriceBand"), "tick_size": pi.get("tickSize"),
            },
            "range": {
                "week_52_high": pi.get("weekHighLow", {}).get("max"),
                "week_52_high_date": pi.get("weekHighLow", {}).get("maxDate"),
                "week_52_low": pi.get("weekHighLow", {}).get("min"),
                "week_52_low_date": pi.get("weekHighLow", {}).get("minDate"),
            },
            "trade_info": {
                "traded_volume_lakh": (tinfo.get("tradedVolume")),
                "total_market_cap_cr": (sec.get("totalMarketCap") if sec else None),
                "delivery_pct": (sec.get("deliveryToTradedQuantity") if sec else None),
                "delivery_qty": (sec.get("deliveryQuantity") if sec else None),
            },
            "securities_info": {
                "status": md.get("status"), "trading_status": si.get("tradingStatus") if si else None,
                "listing_date": md.get("listingDate"), "isin": info.get("isin"),
                "industry": info.get("industry") or md.get("industry"),
                "pe": md.get("pdSymbolPe"), "sector_pe": md.get("pdSectorPe"),
                "index": md.get("pdSectorInd"), "face_value": si.get("faceValue") if si else None,
            },
        }

    async def fetch_option_chain(self, symbol: str) -> dict[str, Any]:
        """NSE equity option chain (records + filtered strikes)."""
        symbol = symbol.upper()
        j = await self._nse_json(
            f"/api/option-chain-equities?symbol={symbol}",
            f"{settings.nse_base_url}/option-chain",
        )
        if not j or j.get("_blocked") or not j.get("records"):
            return self._proxy_note()
        rec = j.get("records", {})
        rows = []
        for d in rec.get("data", []):
            ce, pe = d.get("CE", {}), d.get("PE", {})
            rows.append({
                "strike": d.get("strikePrice"), "expiry": d.get("expiryDate"),
                "ce_ltp": ce.get("lastPrice"), "ce_oi": ce.get("openInterest"),
                "ce_chng": ce.get("change"), "ce_iv": ce.get("impliedVolatility"),
                "ce_volume": ce.get("totalTradedVolume"),
                "pe_ltp": pe.get("lastPrice"), "pe_oi": pe.get("openInterest"),
                "pe_chng": pe.get("change"), "pe_iv": pe.get("impliedVolatility"),
                "pe_volume": pe.get("totalTradedVolume"),
            })
        return {
            "available": True, "symbol": symbol, "exchange": "NSE",
            "underlying_value": rec.get("underlyingValue"),
            "expiry_dates": rec.get("expiryDates", []),
            "strikes": rows,
        }

    async def fetch_indices(self) -> list[dict[str, Any]]:
        """All NSE index values (cached 10s)."""
        async with self._lock:
            if self._indices and (time.time() - self._indices_at) < 10:
                return self._indices
            await self._ensure_session()
            try:
                r = await self._session.get(
                    settings.nse_base_url + "/api/allIndices",
                    headers=ua_rotator.get_headers(referer=settings.nse_base_url + "/"),
                )
                if r.status_code != 200:
                    return self._indices
                rows = (r.json() or {}).get("data", [])
                out = []
                for x in rows:
                    out.append({
                        "index": x.get("index"),
                        "symbol": x.get("indexSymbol"),
                        "last": x.get("last"),
                        "open": x.get("open"),
                        "high": x.get("high"),
                        "low": x.get("low"),
                        "prev_close": x.get("previousClose"),
                        "change": x.get("variation"),
                        "pct_change": x.get("percentChange"),
                        "year_high": x.get("yearHigh"),
                        "year_low": x.get("yearLow"),
                        "pe": x.get("pe"), "pb": x.get("pb"), "dy": x.get("dy"),
                        "advances": x.get("advances"), "declines": x.get("declines"),
                        "unchanged": x.get("unchanged"),
                        "pct_change_30d": x.get("perChange30d"),
                        "pct_change_365d": x.get("perChange365d"),
                    })
                if out:
                    self._indices = out
                    self._indices_at = time.time()
                    log.info("nse_indices_loaded", count=len(out))
            except Exception as e:
                log.debug("nse_indices_failed", error=str(e))
            return self._indices


# Singleton
nse_public = NSEPublic()
