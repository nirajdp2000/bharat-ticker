"""Tier-1 NSE: Official archive (bhavcopy) provider — genuine NSE-origin data.

NSE's realtime ``/api/quote-equity`` endpoint is gated by Akamai Bot Manager and
is unreachable from automated/datacenter contexts (the ``_abck`` sensor never
validates).  However NSE's **static archive host** ``nsearchives.nseindia.com``
is NOT bot-gated and serves the official daily *full securities bhavcopy* —
every traded NSE symbol with OPEN/HIGH/LOW/CLOSE, volume, turnover, VWAP and
delivery quantity/percentage, straight from the exchange.

This provider downloads + caches the latest available bhavcopy and serves quotes
from it.  It is the primary **NSE-origin** source for last-traded-day data and
also yields the full NSE equity universe.  For live intraday NSE quotes either
run from a residential context or set ``PROXY_LIST`` (see ``NSEScraper``).
"""

from __future__ import annotations

import asyncio
import csv
import io
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from curl_cffi.requests import AsyncSession

from ..config.settings import settings
from ..models.tick import TickData
from ..utils.fingerprint import fingerprint_mgr
from ..utils.user_agents import ua_rotator
from ..utils.logger import get_logger
from .base import DataProvider, ProviderError

log = get_logger(__name__)
IST = ZoneInfo("Asia/Kolkata")

ARCHIVE_HOST = "https://nsearchives.nseindia.com"
BHAV_PATH = "/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"


class NSEArchiveProvider(DataProvider):
    """Serves NSE last-traded-day quotes from the official full bhavcopy."""

    name = "nse_archive"
    tier = 1
    priority = 20  # NSE EOD archive — first fallback once live sources fail
    exchange = "NSE"

    def __init__(self) -> None:
        super().__init__()
        self._session: AsyncSession | None = None
        self._rows: dict[str, dict[str, str]] = {}
        self._loaded_date: str | None = None
        self._loaded_at: float = 0.0
        self._last_attempt: float = 0.0
        self._lock = asyncio.Lock()

    # ── Lifecycle ────────────────────────────────────────────────────────
    async def connect(self) -> None:
        if self._is_connected:
            return
        self._session = AsyncSession(
            impersonate=fingerprint_mgr.get_random(),
            timeout=settings.scrape_timeout_seconds + 10,
            verify=True,
        )
        # Warm cookies (archive host tolerates this but it keeps headers consistent).
        try:
            await self._session.get(settings.nse_base_url + "/", headers=ua_rotator.get_headers())
        except Exception:
            pass
        await self._ensure_loaded()
        self._is_connected = True
        log.info("nse_archive_connected", symbols=len(self._rows), date=self._loaded_date)

    async def disconnect(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        self._is_connected = False

    # ── Bhavcopy load ────────────────────────────────────────────────────
    async def _ensure_loaded(self) -> None:
        """Load (or refresh) the latest available bhavcopy, max once/hour."""
        async with self._lock:
            if self._rows and (time.time() - self._loaded_at) < 3600:
                return
            # Cooldown: if the archive is empty (download failing), don't retry
            # the full 8-day walk on every request — fail fast for 120s.
            if not self._rows and (time.time() - self._last_attempt) < 120:
                return
            self._last_attempt = time.time()
            for back in range(0, 8):  # walk back over weekends/holidays
                day = datetime.now(tz=IST) - timedelta(days=back)
                ddmmyyyy = day.strftime("%d%m%Y")
                if ddmmyyyy == self._loaded_date and self._rows:
                    return
                rows = await self._download(ddmmyyyy)
                if rows:
                    self._rows = rows
                    self._loaded_date = ddmmyyyy
                    self._loaded_at = time.time()
                    log.info("nse_archive_loaded", date=ddmmyyyy, symbols=len(rows))
                    return
            log.warning("nse_archive_no_bhavcopy_found")

    async def _download(self, ddmmyyyy: str) -> dict[str, dict[str, str]] | None:
        url = ARCHIVE_HOST + BHAV_PATH.format(ddmmyyyy=ddmmyyyy)
        headers = ua_rotator.get_headers(referer=settings.nse_base_url + "/")
        try:
            resp = await self._session.get(url, headers=headers)
            if resp.status_code != 200 or not resp.text or "<html" in resp.text[:200].lower():
                return None
            out: dict[str, dict[str, str]] = {}
            reader = csv.DictReader(io.StringIO(resp.text))
            for raw in reader:
                row = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
                sym = row.get("SYMBOL", "")
                series = row.get("SERIES", "")
                if sym and series in ("EQ", "BE", "BZ", "SM", "ST"):
                    out[sym.upper()] = row
            return out or None
        except Exception as e:
            log.debug("nse_archive_download_error", date=ddmmyyyy, error=str(e))
            return None

    # ── Fetching ─────────────────────────────────────────────────────────
    async def fetch_quote(self, symbol: str) -> TickData:
        if not self._is_connected:
            await self.connect()
        await self._ensure_loaded()

        row = self._rows.get(symbol.upper())
        if not row:
            self.record_error()
            raise ProviderError(self.name, f"{symbol} not in NSE bhavcopy")

        start = time.time()
        tick = self._parse(row, symbol)
        self.record_success((time.time() - start) * 1000)
        return tick

    async def fetch_bulk(self, symbols: list[str]) -> list[TickData]:
        if not self._is_connected:
            await self.connect()
        await self._ensure_loaded()
        out: list[TickData] = []
        for s in symbols:
            row = self._rows.get(s.upper())
            if row:
                try:
                    out.append(self._parse(row, s))
                    self.record_success(0.0)
                except Exception:
                    self.record_error()
        return out

    async def health_check(self) -> bool:
        try:
            await self._ensure_loaded()
            return bool(self._rows)
        except Exception:
            return False

    def universe(self) -> list[str]:
        """All symbols present in the loaded bhavcopy (full NSE equity universe)."""
        return sorted(self._rows.keys())

    def snapshot(self) -> list[dict[str, Any]]:
        """Lightweight quote for EVERY NSE symbol — served from memory (~0ms)."""
        d = self._dec
        out: list[dict[str, Any]] = []
        for sym, row in self._rows.items():
            close = d(row.get("CLOSE_PRICE")); prev = d(row.get("PREV_CLOSE"))
            chg = (close - prev) if prev > 0 else 0
            turnover_lacs = d(row.get("TURNOVER_LACS"))
            out.append({
                "symbol": sym, "exchange": "NSE", "series": row.get("SERIES"),
                "ltp": float(close), "open": float(d(row.get("OPEN_PRICE"))),
                "high": float(d(row.get("HIGH_PRICE"))), "low": float(d(row.get("LOW_PRICE"))),
                "prev_close": float(prev), "change": round(float(chg), 2),
                "pct_change": round(float(chg / prev * 100), 2) if prev > 0 else 0,
                "volume": int(d(row.get("TTL_TRD_QNTY"))), "vwap": float(d(row.get("AVG_PRICE"))),
                "value_cr": round(float(turnover_lacs) / 100, 2) if turnover_lacs > 0 else 0,
                "trades": int(d(row.get("NO_OF_TRADES"))),
                "delivery_qty": int(d(row.get("DELIV_QTY"))) if d(row.get("DELIV_QTY")) > 0 else None,
                "delivery_pct": row.get("DELIV_PER"),
                "live": False, "source": self.name,
            })
        return out

    async def fetch_details(self, symbol: str) -> dict[str, Any] | None:
        """Detail panel from the NSE bhavcopy row (OHLC, VWAP, delivery, value)."""
        await self._ensure_loaded()
        row = self._rows.get(symbol.upper())
        if not row:
            return None
        d = self._dec
        return {
            "symbol": symbol.upper(), "name": symbol.upper(), "exchange": "NSE",
            "source": self.name, "as_on": row.get("DATE1"),
            "price": {
                "ltp": row.get("CLOSE_PRICE"), "change": None, "pct_change": None,
                "open": row.get("OPEN_PRICE"), "high": row.get("HIGH_PRICE"),
                "low": row.get("LOW_PRICE"), "prev_close": row.get("PREV_CLOSE"),
                "vwap": row.get("AVG_PRICE"),
            },
            "range": {"week_52_high": None, "week_52_low": None,
                      "price_band": None, "circuit_filter": None},
            "traded": {
                "ttq_lakh": str(round(int(d(row.get("TTL_TRD_QNTY"))) / 1e5, 2)),
                "turnover_cr": str(round(float(d(row.get("TURNOVER_LACS"))) / 100, 2)),
                "no_of_trades": row.get("NO_OF_TRADES"),
                "delivery_qty": row.get("DELIV_QTY"), "delivery_pct": row.get("DELIV_PER"),
            },
            "fundamentals": {"isin": None, "series": row.get("SERIES")},
        }

    # ── Parse ────────────────────────────────────────────────────────────
    def _parse(self, row: dict[str, str], symbol: str) -> TickData:
        close = self._dec(row.get("CLOSE_PRICE"))
        last = self._dec(row.get("LAST_PRICE")) or close
        prev = self._dec(row.get("PREV_CLOSE"))
        open_p = self._dec(row.get("OPEN_PRICE"))
        high = self._dec(row.get("HIGH_PRICE"))
        low = self._dec(row.get("LOW_PRICE"))
        vwap = self._dec(row.get("AVG_PRICE"))
        vol = int(self._dec(row.get("TTL_TRD_QNTY")))
        turnover_lacs = self._dec(row.get("TURNOVER_LACS"))
        value = turnover_lacs * Decimal("100000") if turnover_lacs > 0 else None
        change = (close - prev) if prev > 0 else Decimal("0")
        pct = (change / prev * 100).quantize(Decimal("0.01")) if prev > 0 else Decimal("0")

        ltp = close if close > 0 else last
        return TickData(
            symbol=symbol.upper(),
            isin=None,
            exchange="NSE",
            series=row.get("SERIES") or "EQ",
            ltp=ltp,
            open=open_p if open_p > 0 else ltp,
            high=high if high > 0 else ltp,
            low=low if low > 0 else ltp,
            close=prev,
            change=change,
            pct_change=pct,
            volume=vol,
            value=value,
            vwap=vwap if vwap > 0 else None,
            upper_circuit=None,
            lower_circuit=None,
            week_52_high=None,
            week_52_low=None,
            market_depth=None,
            timestamp=datetime.now(tz=IST),
            source=self.name,
            source_latency_ms=0.0,
        )

    @staticmethod
    def _dec(value: Any) -> Decimal:
        if value is None:
            return Decimal("0")
        try:
            s = str(value).replace(",", "").strip()
            if s in ("", "-"):
                return Decimal("0")
            return Decimal(s)
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")
