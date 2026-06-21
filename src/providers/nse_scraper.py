"""Tier-1 Primary: NSE Internal JSON API Scraper.

This is the crown jewel of the ingestion engine. It uses ``curl_cffi``
with TLS fingerprint impersonation to access the internal JSON API
endpoints served by nseindia.com.  When ``curl_cffi`` gets blocked
(HTTP 403, captcha pages), it falls back to Playwright headless Chromium
to solve JS challenges and extract ``cf_clearance`` cookies.

Architecture:
    1. Warm-up:  Navigate to nseindia.com homepage to obtain session cookies.
    2. Fetch:    Hit /api/quote-equity?symbol=X with the session cookies.
    3. Validate: Parse the JSON response through Pydantic TickData.
    4. Refresh:  Auto-refresh cookies every 90 seconds.
    5. Fallback: On 403 → Playwright cookie extraction → retry.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from curl_cffi.requests import AsyncSession

from ..config.constants import NSE_ENDPOINTS
from ..config.settings import settings
from ..models.tick import MarketDepth, MarketDepthLevel, TickData
from ..utils.fingerprint import fingerprint_mgr
from ..utils.logger import get_logger
from ..utils.user_agents import ua_rotator
from .base import DataProvider, ProviderBlockedError, ProviderError, ProviderSchemaError
from .proxy_pool import mask_proxy, proxy_pool

log = get_logger(__name__)
IST = ZoneInfo("Asia/Kolkata")


class NSEScraper(DataProvider):
    """Asynchronous NSE internal API scraper with TLS impersonation."""

    name = "nse_scraper"
    tier = 1
    priority = 10  # NSE realtime first
    exchange = "NSE"

    def __init__(self) -> None:
        super().__init__()
        self._session: AsyncSession | None = None
        self._proxy: str | None = None
        self._cookies: dict[str, str] = {}
        self._last_cookie_refresh: float = 0.0
        self._base_url: str = settings.nse_base_url
        self._lock = asyncio.Lock()

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Initialize the curl_cffi session and obtain initial cookies."""
        if self._is_connected:
            return

        log.info("nse_scraper_connecting")
        session_kwargs: dict[str, Any] = dict(
            impersonate=fingerprint_mgr.get_random(),
            timeout=settings.scrape_timeout_seconds,
            verify=True,
        )
        # Route through the current rotating proxy (residential Indian IP) when
        # configured — this is what makes the Akamai-gated /api/quote-equity
        # reachable from blocked networks. Rotates on a persistent 403.
        self._proxy = proxy_pool.current()
        proxies = proxy_pool.as_proxies(self._proxy)
        if proxies:
            session_kwargs["proxies"] = proxies
            log.info("nse_scraper_using_proxy", proxy=mask_proxy(self._proxy))
        self._session = AsyncSession(**session_kwargs)
        await self._refresh_cookies()
        self._is_connected = True
        log.info("nse_scraper_connected", cookies=len(self._cookies))

    async def _rotate_session(self, reason: str) -> None:
        """Park the current proxy, rebuild the session on the next one, re-warm
        cookies. No-op rebuild when only one (or zero) proxy is configured."""
        proxy_pool.rotate(bad=self._proxy, reason=reason)
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None
        self._cookies = {}
        self._last_cookie_refresh = 0.0
        self._is_connected = False
        await self.connect()

    async def disconnect(self) -> None:
        """Close the session."""
        if self._session:
            await self._session.close()
            self._session = None
        self._is_connected = False
        log.info("nse_scraper_disconnected")

    # ── Cookie Management ────────────────────────────────────────────────

    async def _refresh_cookies(self) -> None:
        """Navigate to the NSE homepage to obtain fresh session cookies.

        NSE uses server-side sessions; the initial GET to the homepage
        sets cookies like ``nsit``, ``nseappid``, ``bm_sv``, etc.
        """
        async with self._lock:
            now = time.time()
            if now - self._last_cookie_refresh < settings.session_refresh_interval_seconds:
                return  # Still fresh

            log.debug("nse_refreshing_cookies")
            try:
                headers = ua_rotator.get_headers(referer=None)
                resp = await self._session.get(
                    self._base_url + NSE_ENDPOINTS["home"],
                    headers=headers,
                )

                if resp.status_code == 200:
                    # Extract cookies from the response
                    for cookie in resp.cookies.jar:
                        self._cookies[cookie.name] = cookie.value
                    self._last_cookie_refresh = time.time()
                    log.info("nse_cookies_refreshed", count=len(self._cookies))
                elif resp.status_code == 403:
                    log.warning("nse_cookie_refresh_blocked", status=403)
                    await self._playwright_cookie_fallback()
                else:
                    log.warning("nse_cookie_refresh_unexpected", status=resp.status_code)

            except Exception as e:
                log.error("nse_cookie_refresh_error", error=str(e))
                raise ProviderError(self.name, f"Cookie refresh failed: {e}")

    async def _playwright_cookie_fallback(self) -> None:
        """Use Playwright to solve JS challenges and extract cookies.

        This is the heavyweight fallback — only triggered when curl_cffi
        is fully blocked by Akamai/Cloudflare JS challenges.
        """
        log.info("nse_playwright_fallback_starting")
        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=ua_rotator.get_random(),
                    viewport={"width": 1920, "height": 1080},
                )
                page = await context.new_page()

                # Navigate and wait for JS challenge to resolve
                await page.goto(self._base_url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(5000)  # Wait for cookie to be set

                # Extract all cookies
                cookies = await context.cookies()
                for cookie in cookies:
                    self._cookies[cookie["name"]] = cookie["value"]

                await browser.close()

            self._last_cookie_refresh = time.time()
            log.info("nse_playwright_cookies_extracted", count=len(self._cookies))

        except ImportError:
            log.error("playwright_not_installed", hint="pip install playwright && playwright install chromium")
        except Exception as e:
            log.error("nse_playwright_fallback_failed", error=str(e))

    async def _ensure_fresh_cookies(self) -> None:
        """Ensure cookies are fresh before making an API call."""
        elapsed = time.time() - self._last_cookie_refresh
        if elapsed > settings.session_refresh_interval_seconds:
            await self._refresh_cookies()

    # ── Data Fetching ────────────────────────────────────────────────────

    async def fetch_quote(self, symbol: str) -> TickData:
        """Fetch a full equity quote from NSE's internal API."""
        if not self._is_connected:
            await self.connect()

        await self._ensure_fresh_cookies()

        start = time.time()
        url = f"{self._base_url}{NSE_ENDPOINTS['quote_equity']}?symbol={symbol}"
        headers = ua_rotator.get_headers(referer=f"{self._base_url}/get-quotes/equity?symbol={symbol}")

        try:
            resp = await self._session.get(
                url,
                headers=headers,
                cookies=self._cookies,
            )

            latency_ms = (time.time() - start) * 1000

            if resp.status_code == 403:
                self.record_error()
                log.warning("nse_fetch_blocked", symbol=symbol, status=403)
                # Trigger cookie refresh and retry once
                await self._refresh_cookies()
                resp = await self._session.get(url, headers=headers, cookies=self._cookies)
                latency_ms = (time.time() - start) * 1000

                if resp.status_code == 403:
                    # Cookie refresh didn't clear it → likely an IP/Akamai block.
                    # Rotate to the next proxy and try once more before giving up.
                    if proxy_pool.count > 1:
                        await self._rotate_session("http_403")
                        await self._ensure_fresh_cookies()
                        resp = await self._session.get(url, headers=headers, cookies=self._cookies)
                        latency_ms = (time.time() - start) * 1000
                    if resp.status_code == 403:
                        raise ProviderBlockedError(self.name, f"Blocked fetching {symbol}", 403)

            if resp.status_code != 200:
                self.record_error()
                raise ProviderError(self.name, f"HTTP {resp.status_code} for {symbol}", resp.status_code)

            data = resp.json()
            tick = self._parse_nse_quote(data, symbol, latency_ms)
            self.record_success(latency_ms)
            log.debug("nse_quote_fetched", symbol=symbol, ltp=str(tick.ltp), latency_ms=round(latency_ms, 1))
            return tick

        except (ProviderError, ProviderBlockedError):
            raise
        except Exception as e:
            self.record_error()
            log.error("nse_fetch_error", symbol=symbol, error=str(e))
            raise ProviderError(self.name, f"Failed to fetch {symbol}: {e}")

    async def fetch_bulk(self, symbols: list[str]) -> list[TickData]:
        """Fetch quotes for multiple symbols with controlled concurrency."""
        semaphore = asyncio.Semaphore(settings.scrape_concurrency)
        results: list[TickData] = []
        errors: list[str] = []

        async def fetch_one(sym: str) -> None:
            async with semaphore:
                try:
                    tick = await self.fetch_quote(sym)
                    results.append(tick)
                except Exception as e:
                    errors.append(f"{sym}: {e}")
                    log.warning("nse_bulk_fetch_error", symbol=sym, error=str(e))

        # Small delay between batches to avoid rate limiting
        tasks = []
        for i, symbol in enumerate(symbols):
            tasks.append(fetch_one(symbol))
            if (i + 1) % 5 == 0:  # Throttle: 5 requests, then brief pause
                await asyncio.gather(*tasks)
                tasks = []
                await asyncio.sleep(0.3)

        if tasks:
            await asyncio.gather(*tasks)

        if errors:
            log.warning("nse_bulk_fetch_partial", total=len(symbols), success=len(results), errors=len(errors))

        return results

    # ── Health Check ─────────────────────────────────────────────────────

    async def health_check(self) -> bool:
        """Test connectivity by hitting the market status endpoint."""
        try:
            if not self._is_connected:
                return False
            await self._ensure_fresh_cookies()
            headers = ua_rotator.get_headers(referer=self._base_url)
            resp = await self._session.get(
                f"{self._base_url}{NSE_ENDPOINTS['market_status']}",
                headers=headers,
                cookies=self._cookies,
            )
            return resp.status_code == 200
        except Exception:
            return False

    # ── Response Parsing ─────────────────────────────────────────────────

    def _parse_nse_quote(self, data: dict[str, Any], symbol: str, latency_ms: float) -> TickData:
        """Parse the NSE /api/quote-equity JSON response into a TickData.

        The NSE response structure (as of 2026):
        {
          "info": { "symbol": "RELIANCE", "isin": "INE002A01018", ... },
          "priceInfo": {
            "lastPrice": 2945.50,
            "open": 2930.00,
            "high": 2958.75,
            ...
          },
          "securityInfo": { "tradedVolume": ..., "totalTradedValue": ... },
          "marketDeptOrderBook": { "bid": [...], "ask": [...] }
        }
        """
        try:
            info = data.get("info", {})
            price_info = data.get("priceInfo", {})
            security_info = data.get("securityInfo", {})
            depth_data = data.get("marketDeptOrderBook", {})

            # Parse market depth if available
            market_depth = self._parse_market_depth(depth_data)

            # Parse price data
            ltp = self._safe_decimal(price_info.get("lastPrice", 0))
            open_price = self._safe_decimal(price_info.get("open", 0))
            high = self._safe_decimal(price_info.get("intraDayHighLow", {}).get("max", 0))
            low = self._safe_decimal(price_info.get("intraDayHighLow", {}).get("min", 0))
            close = self._safe_decimal(price_info.get("previousClose", 0))
            change = self._safe_decimal(price_info.get("change", 0))
            pct_change = self._safe_decimal(price_info.get("pChange", 0))
            vwap = self._safe_decimal(price_info.get("vwap", None))
            upper_circuit = self._safe_decimal(price_info.get("upperCP", None))
            lower_circuit = self._safe_decimal(price_info.get("lowerCP", None))

            # Volume data
            volume = int(security_info.get("tradedVolume", 0) or 0)
            value = self._safe_decimal(security_info.get("totalTradedValue", None))

            # 52-week range
            wk52 = price_info.get("weekHighLow", {})
            week_52_high = self._safe_decimal(wk52.get("max", None))
            week_52_low = self._safe_decimal(wk52.get("min", None))

            tick = TickData(
                symbol=symbol,
                isin=info.get("isin"),
                exchange="NSE",
                series=info.get("series", "EQ"),
                ltp=ltp,
                open=open_price,
                high=high if high > 0 else ltp,
                low=low if low > 0 else ltp,
                close=close,
                change=change,
                pct_change=pct_change,
                volume=volume,
                value=value,
                vwap=vwap,
                upper_circuit=upper_circuit,
                lower_circuit=lower_circuit,
                week_52_high=week_52_high,
                week_52_low=week_52_low,
                market_depth=market_depth,
                timestamp=datetime.now(tz=IST),
                source=self.name,
                source_latency_ms=latency_ms,
            )
            return tick

        except Exception as e:
            raise ProviderSchemaError(
                self.name,
                f"Failed to parse NSE response for {symbol}: {e}"
            )

    def _parse_market_depth(self, depth_data: dict[str, Any]) -> MarketDepth | None:
        """Parse the market depth / order book from NSE response."""
        if not depth_data:
            return None

        bids_raw = depth_data.get("bid", [])
        asks_raw = depth_data.get("ask", [])

        if not bids_raw and not asks_raw:
            return None

        bids = []
        for b in bids_raw[:5]:
            try:
                bids.append(MarketDepthLevel(
                    price=self._safe_decimal(b.get("price", 0)),
                    quantity=int(b.get("quantity", 0) or 0),
                    orders=int(b.get("number", 0) or 0),
                ))
            except Exception:
                continue

        asks = []
        for a in asks_raw[:5]:
            try:
                asks.append(MarketDepthLevel(
                    price=self._safe_decimal(a.get("price", 0)),
                    quantity=int(a.get("quantity", 0) or 0),
                    orders=int(a.get("number", 0) or 0),
                ))
            except Exception:
                continue

        total_buy = int(depth_data.get("totalBuyQuantity", 0) or 0)
        total_sell = int(depth_data.get("totalSellQuantity", 0) or 0)

        return MarketDepth(
            buy=bids,
            sell=asks,
            total_buy_quantity=total_buy,
            total_sell_quantity=total_sell,
        )

    @staticmethod
    def _safe_decimal(value: Any) -> Decimal:
        """Safely convert a value to Decimal, defaulting to 0."""
        if value is None:
            return Decimal("0")
        try:
            # Handle strings with commas (e.g. "2,945.50")
            if isinstance(value, str):
                value = value.replace(",", "").strip()
                if value == "" or value == "-":
                    return Decimal("0")
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")
