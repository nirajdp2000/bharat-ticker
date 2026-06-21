"""Chartink screener — run technical scans, get matched stocks as JSON.

`chartink.com/screener/process` accepts a `scan_clause` and returns every stock
matching it ({nsecode, name, bsecode, close, per_chg, volume}). Requires a CSRF
token + cookie harvested from the screener page (refreshed on expiry). Powerful
future data source: gainers, breakouts, 52-week highs, volume shockers, or any
custom clause.
"""

from __future__ import annotations

import re
from typing import Any

from curl_cffi.requests import AsyncSession

from ..config.settings import settings
from ..utils.fingerprint import fingerprint_mgr
from ..utils.logger import get_logger

log = get_logger(__name__)

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
PAGE = "https://chartink.com/screener/"
PROCESS = "https://chartink.com/screener/process"

# Ready-made scans (Chartink DSL — verified idioms).
PREBUILT_SCANS: dict[str, str] = {
    "gainers": "( {cash} ( latest close > 1 day ago close * 1.02 and latest volume > 100000 ) )",
    "losers": "( {cash} ( latest close < 1 day ago close * 0.98 and latest volume > 100000 ) )",
    "volume_shockers": "( {cash} ( latest volume > 3 * sma( latest volume , 20 ) and latest close > latest open and latest volume > 200000 ) )",
    "breakout_20d_high": "( {cash} ( latest close > max( 20 , 1 day ago high ) and latest volume > 100000 ) )",
    "near_52w_high": "( {cash} ( latest close > max( 250 , latest high ) * 0.95 and latest volume > 100000 ) )",
    "near_52w_low": "( {cash} ( latest close < min( 250 , latest low ) * 1.05 and latest volume > 100000 ) )",
    "above_sma_50_200": "( {cash} ( latest close > sma( latest close , 50 ) and latest close > sma( latest close , 200 ) ) )",
    "rsi_oversold": "( {cash} ( latest rsi( 14 ) < 30 and latest volume > 100000 ) )",
    "rsi_overbought": "( {cash} ( latest rsi( 14 ) > 70 and latest volume > 100000 ) )",
}

async def run_scan(clause: str, limit: int = 500) -> dict[str, Any] | None:
    """Run a raw Chartink scan_clause. Returns {total, count, stocks[]}.

    Uses a fresh session per call (GET page → harvest CSRF+cookie → POST) so it
    works across event loops; curl_cffi sessions don't survive a loop change.
    """
    try:
        async with AsyncSession(impersonate=fingerprint_mgr.get_random(),
                                timeout=settings.scrape_timeout_seconds, verify=True) as s:
            page = await s.get(PAGE, headers={"User-Agent": UA})
            m = re.search(r'name="csrf-token"\s+content="([^"]+)"', page.text or "")
            if not m:
                return None
            headers = {"User-Agent": UA, "X-CSRF-TOKEN": m.group(1),
                       "X-Requested-With": "XMLHttpRequest",
                       "Content-Type": "application/x-www-form-urlencoded",
                       "Referer": PAGE}
            r = await s.post(PROCESS, headers=headers, data={"scan_clause": clause})
            if r.status_code != 200 or r.text[:1] not in "{[":
                return None
            j = r.json()
    except Exception as e:  # noqa: BLE001
        log.debug("chartink_scan_failed", error=str(e))
        return None
    rows = j.get("data") or []
    stocks = [{
        "symbol": row.get("nsecode"),
        "name": row.get("name"),
        "bseCode": row.get("bsecode"),
        "close": row.get("close"),
        "changePct": row.get("per_chg"),
        "volume": row.get("volume"),
    } for row in rows[:limit]]
    return {"total": j.get("recordsTotal", len(rows)), "count": len(stocks), "stocks": stocks}
