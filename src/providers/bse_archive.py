"""Tier-1 BSE: official UDiFF bhavcopy provider — genuine BSE-origin EOD data.

Serves last-traded-day quotes for every BSE security from the official daily
bhavcopy (via the shared :data:`bse_master`).  Structured CSV → immune to
website UI changes.  Complements :class:`BSEScraper` (live JSON API): if the
realtime API is unreachable, this still provides BSE-origin data, and vice
versa — that is the NSE/BSE "if one breaks the other is active" guarantee.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from ..models.tick import TickData
from ..utils.logger import get_logger
from .base import DataProvider, ProviderError
from .bse_master import bse_master

log = get_logger(__name__)
IST = ZoneInfo("Asia/Kolkata")


class BSEArchiveProvider(DataProvider):
    """BSE last-traded-day quotes from the official bhavcopy."""

    name = "bse_archive"
    tier = 1
    priority = 25  # After BSE realtime, before Yahoo
    exchange = "BSE"

    async def connect(self) -> None:
        # Lazy: do NOT load the ~4358-row BSE UDiFF master here. This is the EOD
        # fallback provider (not a live feed); the master loads on first actual
        # BSE-archive use (fetch_quote/snapshot/details all call ensure_loaded).
        # Keeps the master out of RAM until BSE EOD data is genuinely requested.
        # Live BSE data is served by bse_scraper, which is unaffected.
        self._is_connected = True
        log.info("bse_archive_connected_lazy")

    async def disconnect(self) -> None:
        self._is_connected = False

    async def fetch_quote(self, symbol: str) -> TickData:
        await bse_master.ensure_loaded()
        row = bse_master.row_for(symbol)
        if not row:
            self.record_error()
            raise ProviderError(self.name, f"{symbol} not in BSE bhavcopy")
        self.record_success(0.0)
        return self._parse(row, symbol)

    async def fetch_bulk(self, symbols: list[str]) -> list[TickData]:
        await bse_master.ensure_loaded()
        out: list[TickData] = []
        for s in symbols:
            row = bse_master.row_for(s)
            if row:
                try:
                    out.append(self._parse(row, s))
                    self.record_success(0.0)
                except Exception:
                    self.record_error()
        return out

    async def health_check(self) -> bool:
        try:
            await bse_master.ensure_loaded()
            return bse_master.size > 0
        except Exception:
            return False

    def universe(self) -> list[str]:
        return bse_master.universe()

    def snapshot(self) -> list[dict[str, Any]]:
        """Lightweight quote for EVERY BSE symbol — served from memory (~0ms)."""
        d = self._dec
        out: list[dict[str, Any]] = []
        for sym in bse_master.universe():
            row = bse_master.row_for(sym) or {}
            close = d(row.get("ClsPric")); prev = d(row.get("PrvsClsgPric"))
            chg = (close - prev) if prev > 0 else 0
            out.append({
                "symbol": sym, "exchange": "BSE", "scrip_code": row.get("FinInstrmId"),
                "ltp": float(close), "open": float(d(row.get("OpnPric"))),
                "high": float(d(row.get("HghPric"))), "low": float(d(row.get("LwPric"))),
                "prev_close": float(prev), "change": float(chg),
                "pct_change": round(float(chg / prev * 100), 2) if prev > 0 else 0,
                "volume": int(d(row.get("TtlTradgVol"))), "isin": row.get("ISIN"),
                "source": self.name,
            })
        return out

    def _parse(self, row: dict[str, str], symbol: str) -> TickData:
        close = self._dec(row.get("ClsPric"))
        last = self._dec(row.get("LastPric")) or close
        prev = self._dec(row.get("PrvsClsgPric"))
        open_p = self._dec(row.get("OpnPric"))
        high = self._dec(row.get("HghPric"))
        low = self._dec(row.get("LwPric"))
        vol = int(self._dec(row.get("TtlTradgVol")))
        value = self._dec(row.get("TtlTrfVal")) or None
        ltp = close if close > 0 else last
        change = (ltp - prev) if prev > 0 else Decimal("0")
        pct = (change / prev * 100).quantize(Decimal("0.01")) if prev > 0 else Decimal("0")
        return TickData(
            symbol=symbol.upper(),
            isin=row.get("ISIN") or None,
            exchange="BSE",
            series=row.get("SctySrs") or "A",
            ltp=ltp,
            open=open_p if open_p > 0 else ltp,
            high=high if high > 0 else ltp,
            low=low if low > 0 else ltp,
            close=prev,
            change=change,
            pct_change=pct,
            volume=vol,
            value=value,
            vwap=None,
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
