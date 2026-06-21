"""Tickertape public API — real NSE intraday, batch quotes, sector, MMI.

All endpoints are public JSON, reachable without auth, low-latency:
  - `/search`                 → symbol → sid + sector + name (resolver)
  - `/stocks/quotes?sids=`    → batch NSE quotes
  - `/stocks/charts/inter/{sid}?duration=` → real NSE intraday points
  - `/mmi/now`                → Market Mood Index (sentiment gauge)

Tickertape keys stocks by an internal ``sid`` (RELIANCE → RELI); the resolver
maps and caches it.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from curl_cffi.requests import AsyncSession

from ..config.settings import settings
from ..utils.fingerprint import fingerprint_mgr
from ..utils.logger import get_logger

log = get_logger(__name__)

BASE = "https://api.tickertape.in"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

_sid_cache: dict[str, dict[str, Any]] = {}   # symbol → {sid, sector, name}
_mmi_cache: tuple[float, dict[str, Any]] | None = None

# /search is per-IP RATE-LIMITED (403 REQUEST_LIMIT_EXCEEDED). The data
# endpoints (/stocks/quotes, /stocks/charts) are NOT — so the failure mode was
# self-inflicted: a sweep fired _RESOLVE_CONCURRENCY=16 concurrent /search calls,
# tripped the limit, and EVERY tickertape read then cascaded to fail. Fix:
#   1. seed verified sids so the hot path needs no /search,
#   2. serialize + pace /search through one lock,
#   3. on 403, cooldown — stop calling /search so resolve() returns fast and the
#      caller cleanly fails over to Groww/BSE instead of hammering a limited API.
# Only VERIFIED sids are seeded (a wrong sid = wrong stock's data).
_SID_SEED: dict[str, str] = {
    "RELIANCE": "RELI",
}
_search_lock = asyncio.Lock()
_last_search_ts = 0.0
_search_cooldown_until = 0.0
_SEARCH_MIN_GAP = 0.6        # min seconds between /search calls (no bursts)
_SEARCH_COOLDOWN_S = 120.0   # park /search this long after a 403

# Bulk sid map — /stocks/list returns the WHOLE universe (~5768 ticker→sid) in
# ONE un-rate-limited call, so ANY symbol resolves WITHOUT /search. Loaded lazily,
# refreshed daily. /search stays only as a last resort for renamed/absent tickers.
_all_sids: dict[str, str] = {}
_all_sids_at = 0.0
_all_sids_lock = asyncio.Lock()
_SIDMAP_TTL = 86400.0


async def _get(path: str, params: dict | None = None) -> Any | None:
    status, j = await _get_raw(path, params)
    return j


async def _get_raw(path: str, params: dict | None = None) -> tuple[int | None, Any | None]:
    # Fresh session per call — a module-global AsyncSession binds to the event
    # loop it was created on and fails on later requests (different loop).
    try:
        async with AsyncSession(impersonate=fingerprint_mgr.get_random(),
                                timeout=settings.scrape_timeout_seconds, verify=True) as s:
            r = await s.get(f"{BASE}{path}", params=params or {},
                            headers={"User-Agent": UA, "Accept": "application/json"})
            if r.status_code == 200 and r.text[:1] in "{[":
                return 200, r.json()
            return r.status_code, None
    except Exception as e:  # noqa: BLE001
        log.debug("tickertape_get_failed", path=path, error=str(e))
        return None, None


async def _search(text: str) -> Any | None:
    """Throttled, cooldown-guarded /search. Returns None (fast) while rate-limited
    so callers fail over instead of cascading."""
    global _last_search_ts, _search_cooldown_until
    if time.time() < _search_cooldown_until:
        return None
    async with _search_lock:
        if time.time() < _search_cooldown_until:
            return None
        gap = time.time() - _last_search_ts
        if gap < _SEARCH_MIN_GAP:
            await asyncio.sleep(_SEARCH_MIN_GAP - gap)
        status, j = await _get_raw("/search", {"text": text, "types": "stock"})
        _last_search_ts = time.time()
        if status == 403:
            _search_cooldown_until = time.time() + _SEARCH_COOLDOWN_S
            log.warning("tickertape_search_rate_limited", cooldown_s=_SEARCH_COOLDOWN_S)
            return None
        return j


async def _ensure_sid_map() -> None:
    """Load the full ticker→sid universe from /stocks/list (one call, daily TTL)."""
    global _all_sids, _all_sids_at
    if _all_sids and (time.time() - _all_sids_at) < _SIDMAP_TTL:
        return
    async with _all_sids_lock:
        if _all_sids and (time.time() - _all_sids_at) < _SIDMAP_TTL:
            return
        j = await _get("/stocks/list")
        rows = (j or {}).get("data") or []
        m = {}
        for x in rows:
            t = str(x.get("ticker", "")).upper()
            sid = x.get("sid")
            if t and sid:
                m[t] = sid
        if m:
            _all_sids = m
            _all_sids_at = time.time()
            log.info("tickertape_sid_map_loaded", count=len(m))


async def resolve(symbol: str) -> dict[str, Any] | None:
    """symbol → {sid, sector, name}. Seed → bulk sid map → cache → /search (last resort)."""
    symbol = symbol.strip().upper()
    if symbol in _sid_cache:
        return _sid_cache[symbol]
    seed = _SID_SEED.get(symbol)
    if seed:
        rec = {"sid": seed, "sector": None, "name": None}
        _sid_cache[symbol] = rec
        return rec
    # Bulk universe map — resolves any listed ticker with NO /search.
    await _ensure_sid_map()
    sid = _all_sids.get(symbol)
    if sid:
        rec = {"sid": sid, "sector": None, "name": None}
        _sid_cache[symbol] = rec
        return rec
    # Last resort: throttled /search for renamed/absent tickers.
    j = await _search(symbol)
    stocks = ((j or {}).get("data") or {}).get("stocks") or []
    # Prefer exact ticker match.
    best = None
    for st in stocks:
        if str(st.get("ticker", "")).upper() == symbol:
            best = st
            break
    best = best or (stocks[0] if stocks else None)
    if not best:
        return None
    rec = {"sid": best.get("sid"), "sector": best.get("sector"), "name": best.get("name")}
    if rec["sid"]:
        _sid_cache[symbol] = rec
    return rec


async def fetch_intraday_points(symbol: str, duration: str = "1d") -> list[dict[str, Any]] | None:
    """Real NSE intraday curve → ascending [{timestamp(IST), price, volume(per-interval)}].

    Tickertape ``v`` is CUMULATIVE day-volume and ``ts`` is UTC — convert to
    per-interval volume (delta) and IST so the OHLC builder and the rest of the
    API stay consistent.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo
    ist = ZoneInfo("Asia/Kolkata")

    rec = await resolve(symbol)
    if not rec or not rec.get("sid"):
        return None
    j = await _get(f"/stocks/charts/inter/{rec['sid']}", {"duration": duration})
    data = (j or {}).get("data") or []
    pts = data[0].get("points") if data else None
    if not pts:
        return None

    raw = []
    for p in pts:
        lp, ts = p.get("lp"), p.get("ts")
        if lp is None or not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(ist)
        except ValueError:
            continue
        raw.append((dt, float(lp), float(p.get("v") or 0)))
    raw.sort(key=lambda x: x[0])

    out = []
    prev_cum = None
    for dt, price, cum in raw:
        inc = cum - prev_cum if (prev_cum is not None and cum >= prev_cum) else 0
        prev_cum = cum
        out.append({"timestamp": dt.isoformat(), "price": price, "volume": int(inc)})
    return out


# Tickertape accepts many sids per call; chunk to stay well under any URL/row cap.
_BATCH_SIDS = 50
_RESOLVE_CONCURRENCY = 16


async def fetch_quotes(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """Batch real-NSE quotes (non-delayed) → {symbol: {price, close, change, high,
    low, volume, date}}.

    THE bulk path: one HTTP serves a whole chunk of symbols. ``sid`` resolution
    is parallelised (and process-cached) so a cold sweep isn't N sequential
    /search calls; sids are then chunked and the batch calls fan out concurrently.
    """
    # 1) resolve symbol → sid concurrently (cached after first time).
    sem = asyncio.Semaphore(_RESOLVE_CONCURRENCY)

    async def _res(s: str):
        async with sem:
            try:
                return s.upper(), await resolve(s)
            except Exception:  # noqa: BLE001
                return s.upper(), None

    sid_map: dict[str, str] = {}   # sid → SYMBOL
    for sym, rec in await asyncio.gather(*(_res(s) for s in symbols)):
        if rec and rec.get("sid"):
            sid_map[rec["sid"]] = sym
    if not sid_map:
        return {}

    # 2) chunk sids and fetch each chunk's batch quote concurrently (1 HTTP/chunk).
    sids = list(sid_map.keys())
    chunks = [sids[i:i + _BATCH_SIDS] for i in range(0, len(sids), _BATCH_SIDS)]

    async def _batch(chunk: list[str]):
        return await _get("/stocks/quotes", {"sids": ",".join(chunk)})

    out: dict[str, dict[str, Any]] = {}
    for j in await asyncio.gather(*(_batch(c) for c in chunks)):
        for row in (j or {}).get("data", []):
            sym = sid_map.get(row.get("sid"))
            if sym:
                out[sym] = row
    return out


def _mmi_zone(value: float) -> str:
    if value < 30:
        return "EXTREME_FEAR"
    if value < 50:
        return "FEAR"
    if value < 70:
        return "GREED"
    return "EXTREME_GREED"


async def fetch_mmi() -> dict[str, Any] | None:
    """Market Mood Index + components (cached 5 min)."""
    global _mmi_cache
    if _mmi_cache and (time.time() - _mmi_cache[0]) < 300:
        return _mmi_cache[1]
    j = await _get("/mmi/now")
    d = (j or {}).get("data")
    if not d or d.get("currentValue") is None:
        return None
    val = round(float(d["currentValue"]), 2)
    def _ind(snap):
        v = (snap or {}).get("indicator")
        return round(float(v), 2) if v is not None else None

    out = {
        "value": val,
        "zone": _mmi_zone(val),
        "lastDay": _ind(d.get("lastDay")),
        "lastWeek": _ind(d.get("lastWeek")),
        "lastMonth": _ind(d.get("lastMonth")),
        "lastYear": _ind(d.get("lastYear")),
        "date": d.get("date"),
        "source": "tickertape_mmi",
    }
    _mmi_cache = (time.time(), out)
    return out
