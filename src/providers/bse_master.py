"""Shared BSE security master, sourced from the official UDiFF bhavcopy.

BSE keys data off numeric **scrip codes**, not symbols.  The website's
autocomplete endpoint (``PortNewGetScrip``) is brittle and HTML-driven, so
instead we derive the entire symbol→code map from BSE's official daily
*UDiFF bhavcopy* CSV (``bseindia.com/download/BhavCopy/Equity/...``).  This is a
structured, versioned data file — it does not change when the website UI
changes — making it a far more durable source than scraping pages.

The loaded rows double as:
  * the BSE realtime scrip-code resolver (``code_for``),
  * the full BSE equity universe (``universe``),
  * the BSE end-of-day quote source (consumed by ``BSEArchiveProvider``).
"""

from __future__ import annotations

import asyncio
import csv
import io
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curl_cffi.requests import AsyncSession

from ..config.settings import settings
from ..utils.fingerprint import fingerprint_mgr
from ..utils.logger import get_logger

log = get_logger(__name__)
IST = ZoneInfo("Asia/Kolkata")

BHAV_URL = (
    "https://www.bseindia.com/download/BhavCopy/Equity/"
    "BhavCopy_BSE_CM_0_0_0_{yyyymmdd}_F_0000.CSV"
)
# Equity series to keep (drop debt/ETF-only noise but keep main boards).
KEEP_SERIES = {"A", "B", "T", "X", "XT", "M", "MT", "P", "Z", "ZP", "W"}


class BSEMaster:
    """Loads + caches the BSE security master from the daily bhavcopy."""

    def __init__(self) -> None:
        self._rows: dict[str, dict[str, str]] = {}   # SYMBOL -> row
        self._by_code: dict[str, dict[str, str]] = {} # scripcode -> row
        self._loaded_date: str | None = None
        self._loaded_at: float = 0.0
        self._last_attempt: float = 0.0
        self._lock = asyncio.Lock()
        self._session: AsyncSession | None = None

    async def ensure_loaded(self) -> None:
        async with self._lock:
            if self._rows and (time.time() - self._loaded_at) < 3600:
                return
            if not self._rows and (time.time() - self._last_attempt) < 120:
                return
            self._last_attempt = time.time()
            if self._session is None:
                self._session = AsyncSession(
                    impersonate=fingerprint_mgr.get_random(),
                    timeout=settings.scrape_timeout_seconds + 15,
                    verify=True,
                )
                try:
                    await self._session.get(settings.bse_base_url + "/")
                except Exception:
                    pass
            for back in range(0, 8):
                day = datetime.now(tz=IST) - timedelta(days=back)
                yyyymmdd = day.strftime("%Y%m%d")
                rows = await self._download(yyyymmdd)
                if rows:
                    self._rows = rows
                    self._by_code = {r["FinInstrmId"]: r for r in rows.values() if r.get("FinInstrmId")}
                    self._loaded_date = yyyymmdd
                    self._loaded_at = time.time()
                    log.info("bse_master_loaded", date=yyyymmdd, symbols=len(rows))
                    return
            log.warning("bse_master_no_bhavcopy_found")

    async def _download(self, yyyymmdd: str) -> dict[str, dict[str, str]] | None:
        url = BHAV_URL.format(yyyymmdd=yyyymmdd)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Origin": settings.bse_base_url,
            "Referer": settings.bse_base_url + "/",
        }
        try:
            resp = await self._session.get(url, headers=headers)
            if resp.status_code != 200 or "<html" in resp.text[:200].lower():
                return None
            out: dict[str, dict[str, str]] = {}
            reader = csv.DictReader(io.StringIO(resp.text))
            for raw in reader:
                row = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
                sym = (row.get("TckrSymb") or "").upper()
                series = (row.get("SctySrs") or "").strip()
                ftp = (row.get("FinInstrmTp") or "").strip()
                if sym and ftp == "STK" and (series in KEEP_SERIES or not series):
                    # First (main-board) row wins for a given symbol.
                    out.setdefault(sym, row)
            return out or None
        except Exception as e:
            log.debug("bse_master_download_error", date=yyyymmdd, error=str(e))
            return None

    # ── Accessors ────────────────────────────────────────────────────────
    def code_for(self, symbol: str) -> str | None:
        row = self._rows.get(symbol.upper())
        return row.get("FinInstrmId") if row else None

    def row_for(self, symbol: str) -> dict[str, str] | None:
        return self._rows.get(symbol.upper())

    def isin_for(self, symbol: str) -> str | None:
        row = self._rows.get(symbol.upper())
        isin = (row or {}).get("ISIN") or (row or {}).get("ISINNo")
        return isin or None

    def universe(self) -> list[str]:
        return sorted(self._rows.keys())

    @property
    def loaded_date(self) -> str | None:
        return self._loaded_date

    @property
    def size(self) -> int:
        return len(self._rows)


# Singleton shared by BSE realtime + archive providers.
bse_master = BSEMaster()
