"""Macro / cross-asset context — FX, commodities, Sensex, FII-DII.

Superbrain's `getMarketContext` reads Nifty, Sensex, USDINR, Brent, Gold (price +
% change) plus FII/DII net flows.  Nifty/VIX come from the NSE indices feed; the
rest are sourced here:
  - Sensex (^BSESN), USDINR (INR=X), Brent (BZ=F), Gold (GC=F) via yfinance
  - FII/DII net via the NSE public JSON (best-effort; None when unreachable)
"""

from __future__ import annotations

import asyncio
import time
from functools import partial
from typing import Any

from ..config.settings import settings
from ..utils.fingerprint import fingerprint_mgr
from ..utils.logger import get_logger

log = get_logger(__name__)

_CACHE: dict[str, tuple[float, Any]] = {}
_TTL_S = 60

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

# label -> yahoo symbol (fallback)
_INSTRUMENTS = {
    "SENSEX": "^BSESN",
    "USDINR": "INR=X",
    "BRENT": "BZ=F",
    "GOLD": "GC=F",
}
# label -> Investing.com pairId (real-time primary). Sanity-bounds keep a wrong
# id from poisoning the feed (falls back to Yahoo if out of range).
_INVESTING = {
    "USDINR": (2124, 50, 200),
    "BRENT": (8833, 20, 250),
    "GOLD": (8830, 500, 12000),
    "SENSEX": (17936, 20000, 200000),
}


async def _investing_quote(pair_id: int, lo: float, hi: float) -> dict[str, Any] | None:
    """Real-time-ish macro quote from Investing.com daily chart (last 2 closes)."""
    url = (f"https://api.investing.com/api/financialdata/{pair_id}"
           f"/historical/chart/?period=P1M&interval=P1D&pointscount=60")
    try:
        from curl_cffi.requests import AsyncSession
        async with AsyncSession(impersonate=fingerprint_mgr.get_random(),
                                timeout=settings.scrape_timeout_seconds, verify=True) as s:
            r = await s.get(url, headers={"User-Agent": UA, "Accept": "application/json",
                                          "domain-id": "www"})
            if r.status_code != 200 or r.text[:1] not in "{[":
                return None
            rows = (r.json() or {}).get("data") or []
    except Exception as e:  # noqa: BLE001
        log.debug("investing_macro_failed", pair_id=pair_id, error=str(e))
        return None
    closes = [row[4] for row in rows if len(row) > 4 and row[4]]
    if len(closes) < 2:
        return None
    price, prev = float(closes[-1]), float(closes[-2])
    if not (lo <= price <= hi) or prev <= 0:
        return None  # implausible → let Yahoo handle it
    return {"price": round(price, 2),
            "changePct": round((price - prev) / prev * 100, 2),
            "source": "investing.com"}


def _yahoo_quote_sync(yf_symbol: str) -> dict[str, Any] | None:
    import yfinance as yf

    t = yf.Ticker(yf_symbol)
    price = change_pct = None
    try:
        fi = t.fast_info
        price = getattr(fi, "last_price", None)
        prev = getattr(fi, "previous_close", None)
        if price and prev:
            change_pct = (price - prev) / prev * 100
    except Exception:  # noqa: BLE001
        pass
    if price is None:
        try:
            info = t.info
            price = info.get("regularMarketPrice")
            prev = info.get("regularMarketPreviousClose")
            if price and prev:
                change_pct = (price - prev) / prev * 100
        except Exception:  # noqa: BLE001
            return None
    if price is None:
        return None
    return {"price": round(float(price), 2),
            "changePct": round(float(change_pct), 2) if change_pct is not None else None}


async def fetch_macro() -> dict[str, Any]:
    """Return {USDINR, BRENT, GOLD, SENSEX} → {price, changePct, source} (cached 60s).

    Investing.com (real-time) first; Yahoo (15-min delayed) only as fallback.
    """
    hit = _CACHE.get("macro")
    if hit and (time.time() - hit[0]) < _TTL_S:
        return hit[1]
    loop = asyncio.get_event_loop()

    async def one(label: str, sym: str):
        # 1. Investing.com (real)
        inv = _INVESTING.get(label)
        if inv:
            res = await _investing_quote(*inv)
            if res:
                return label, res
        # 2. Yahoo fallback (delayed)
        try:
            res = await loop.run_in_executor(None, partial(_yahoo_quote_sync, sym))
            if res:
                res["source"] = "yahoo_delayed"
            return label, res
        except Exception as e:  # noqa: BLE001
            log.debug("macro_fetch_failed", label=label, error=str(e))
            return label, None

    pairs = await asyncio.gather(*(one(l, s) for l, s in _INSTRUMENTS.items()))
    out = {label: res for label, res in pairs if res}
    _CACHE["macro"] = (time.time(), out)
    return out


async def fetch_fii_dii() -> dict[str, Any] | None:
    """Best-effort FII/DII net flows from NSE public JSON. None when unavailable."""
    hit = _CACHE.get("fii_dii")
    if hit and (time.time() - hit[0]) < 30 * 60:
        return hit[1]
    from ..providers.nse_public import nse_public
    try:
        data = await nse_public._nse_json(
            "/api/fiidiiTradeReact", referer="https://www.nseindia.com/reports-indices-fii-dii")  # type: ignore[attr-defined]
    except Exception as e:  # noqa: BLE001
        log.debug("fii_dii_failed", error=str(e))
        data = None
    if not data or (isinstance(data, dict) and data.get("_blocked")):
        return None
    rows = data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []
    fii_net = dii_net = None
    date = None
    for row in rows:
        cat = str(row.get("category", "")).upper()
        net = row.get("netValue") or row.get("net")
        date = row.get("date") or date
        try:
            net = float(str(net).replace(",", "")) if net is not None else None
        except (TypeError, ValueError):
            net = None
        if "FII" in cat or "FPI" in cat:
            fii_net = net
        elif "DII" in cat:
            dii_net = net
    if fii_net is None and dii_net is None:
        return None
    result = {
        "date": date,
        "fiiNetBuy": fii_net or 0,
        "diiNetBuy": dii_net or 0,
        "mood": "BULLISH" if (fii_net or 0) >= 0 else "BEARISH",
        "source": "NSE_PUBLIC",
    }
    _CACHE["fii_dii"] = (time.time(), result)
    return result
