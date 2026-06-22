"""Superbrain-compatible adapter endpoints (`/api/v1/sb/*`).

These expose the EXACT field shapes the superbrain app consumes (camelCase
quotes, numeric ascending candles, today-session intraday incl. 1s/10s live
bars, full fundamentals, macro context, instrument resolver) so bharat-ticker
can be its single market-data backend. Thin layer over the existing provider
stack — no change to the core engine or the public `/api/v1` surface.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from ...engine import chartink, tickertape
from ...engine.corp_actions import adjust_for_splits
from ...engine.fundamentals import fetch_fundamentals
from ...engine.intraday_builder import INTRADAY_SECONDS, points_to_ohlc
from ...engine.intraday_store import read_past_intraday
from ...engine.live_candles import SAMPLE_INTERVAL_S, live_candle_engine
from ...engine.watchlist_recorder import watchlist_recorder
from ...engine.macro import fetch_fii_dii, fetch_macro
from ...engine.market_data import market_data_service
from ...providers.bse_master import bse_master
from ...utils.ist_clock import is_market_open, now_ist
from ...utils.logger import get_logger

log = get_logger(__name__)
router = APIRouter(prefix="/sb", tags=["Superbrain Adapter"])

# symbol → (name, sector) — opportunistically filled from /details so quotes
# stop returning null companyName/sector. 24h in-process cache.
_META_CACHE: dict[str, dict[str, Any]] = {}

# Deprecated/renamed symbol canonicalisation (shared with the core provider
# stack) — post-demerger legacy tickers (e.g. TATAMOTORS → TMPV) map to their
# original-ISIN successor so every endpoint returns ONE consistent instrument.
from ...config.constants import canonical_symbol as _canon  # noqa: E402

# Index pseudo-symbols are NOT equities — Groww/feeds return junk/stale values
# for them. Block from the equity quote/candle paths; consumers use /sb/context
# or /api/v1/indices for index values.
_INDEX_SYMBOLS = {
    "NIFTY", "NIFTY50", "NIFTY 50", "BANKNIFTY", "NIFTYBANK", "NIFTY BANK",
    "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX", "NIFTYIT", "NIFTYNXT50",
}


def _reject_index(symbol: str) -> None:
    if symbol.upper() in _INDEX_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"'{symbol}' is an index, not an equity. Use /api/v1/sb/context or /api/v1/indices.")


def _latest_session_only(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only the most recent trading day's bars (intraday = current session).

    Tickertape's ``duration=1d`` returns ~1.5 sessions; without this the bar set
    mixes two days and per-bar volume sums to ~1.6× the day's true volume.
    """
    if not candles:
        return candles
    last_date = candles[-1]["timestamp"][:10]
    return [c for c in candles if c["timestamp"][:10] == last_date]

# range → number of daily candles
_RANGE_LIMIT = {
    "1mo": 25, "3mo": 70, "6mo": 130, "1y": 260,
    "2y": 520, "3y": 780, "5y": 1300, "max": 2500,
}
# superbrain/Upstox interval names → bharat (Yahoo/TSDB) interval codes
_INTERVAL_MAP = {
    "day": "1d", "1d": "1d", "week": "1w", "1w": "1w",
    "30minute": "30m", "30m": "30m", "15minute": "15m", "15m": "15m",
    "5minute": "5m", "5m": "5m", "1minute": "1m", "1m": "1m",
}
# Upstream (Yahoo) history depth cap per interval code, in calendar days. Kept
# 1 day INSIDE Yahoo's hard window (1m≤7d, intraday≤60d) — a start sitting
# exactly on the boundary is rejected ("must be within the last 60 days").
# Sub-daily feeds are windowed; daily/weekly are effectively unbounded.
_INTERVAL_MAX_DAYS = {
    "1m": 6, "5m": 59, "15m": 59, "30m": 59,
    "1d": 365 * 10, "1w": 365 * 20,
}
# Sub-minute / sub-second intervals → bucket seconds (float). Served by the live
# tick-sampling engine (NSE/BSE feed, never delayed). Finest meaningful bucket is
# bounded by the sampler cadence (LIVE_SAMPLE_INTERVAL_S, default 0.5s).
_SECONDS_INTERVALS = {
    "250ms": 0.25, "500ms": 0.5,
    "1second": 1, "1s": 1,
    "5second": 5, "5s": 5,
    "10second": 10, "10s": 10,
    "15second": 15, "15s": 15,
    "30second": 30, "30s": 30,
}

# Max concurrent upstream fetches per bulk-quotes request (fan-out bound).
_BULK_CONCURRENCY = 24

# ── Intraday history cache ───────────────────────────────────────────────────
# A universe sweep re-pulls the same candles every scan. Split the cost by
# mutability: settled PAST bars are immutable within a session (long TTL); the
# live TODAY portion moves (short TTL).
#
# CRITICAL: a single 5m entry is ~250KB+ live (2925 bars), so an entry-COUNT cap
# would let a universe sweep balloon to GBs and OOM-kill a 256-512MB host. The
# cache is therefore bounded by approximate BYTES (default 64MB total), evicting
# oldest first — safe on the smallest Northflank/Oracle free tiers regardless of
# how many symbols are swept. In-process (per worker); a shared Redis layer would
# generalise it to multi-worker.
_HIST_TTL_S = float(os.environ.get("SB_HIST_TTL_S", "3600"))     # past bars: 1h
_TODAY_TTL_S = float(os.environ.get("SB_TODAY_TTL_S", "30"))     # today bars: 30s


class _TTLCache:
    """TTL cache bounded by approximate total memory (candle lists are large)."""

    def __init__(self, ttl_s: float, max_mb: float) -> None:
        self._ttl = ttl_s
        self._budget = int(max_mb * 1024 * 1024)
        self._store: dict[tuple, tuple[float, Any, int]] = {}  # key → (ts, value, bytes)
        self._bytes = 0

    @staticmethod
    def _sizeof(value: Any) -> int:
        # Candle lists dominate. A 7-key candle dict (unique ts string + 6 nums)
        # is ~450 B live in CPython, so budgetMB ≈ real MB (honest, not optimistic).
        if isinstance(value, list):
            return len(value) * 450 + 64
        return 512

    def get(self, key: tuple) -> Any | None:
        hit = self._store.get(key)
        if hit and (time.time() - hit[0]) < self._ttl:
            return hit[1]
        if hit:
            self._evict(key)
        return None

    def put(self, key: tuple, value: Any) -> None:
        if key in self._store:
            self._evict(key)
        b = self._sizeof(value)
        self._store[key] = (time.time(), value, b)
        self._bytes += b
        while self._bytes > self._budget and len(self._store) > 1:
            self._evict(min(self._store, key=lambda k: self._store[k][0]))

    def _evict(self, key: tuple) -> None:
        ent = self._store.pop(key, None)
        if ent:
            self._bytes -= ent[2]

    def stats(self) -> dict[str, Any]:
        return {"entries": len(self._store), "approxMB": round(self._bytes / 1024 / 1024, 1),
                "budgetMB": round(self._budget / 1024 / 1024, 1), "ttlS": self._ttl}

    def clear(self) -> int:
        n = len(self._store)
        self._store.clear()
        self._bytes = 0
        return n


_HIST_CACHE = _TTLCache(_HIST_TTL_S, float(os.environ.get("SB_HIST_CACHE_MB", "48")))
_TODAY_CACHE = _TTLCache(_TODAY_TTL_S, float(os.environ.get("SB_TODAY_CACHE_MB", "16")))


async def _cached_history(symbol: str, exchange: str, code: str,
                          start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Yahoo history with a session-length, memory-bounded cache."""
    key = (symbol, exchange, code, start.date().isoformat(), end.date().isoformat())
    cached = _HIST_CACHE.get(key)
    if cached is not None:
        return cached
    # limit=None → every bar in the range (no truncation; data-completeness rule).
    rows = await market_data_service.get_history(
        symbol, exchange, code, start=start, end=end, limit=None)
    _HIST_CACHE.put(key, rows)
    return rows


async def _cached_today_bars(symbol: str, exchange: str, bucket: int) -> list[dict[str, Any]]:
    """Today's scraper bars with a short TTL (they keep forming)."""
    key = (symbol, exchange, bucket)
    cached = _TODAY_CACHE.get(key)
    if cached is not None:
        return cached
    bars = await _today_session_bars(symbol, exchange, bucket)
    _TODAY_CACHE.put(key, bars)
    return bars


def _f(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _quality(source: str) -> str:
    """Classify freshness from the serving source + market state."""
    s = (source or "").lower()
    if "yahoo" in s:
        return "DELAYED"          # 15-min delayed feed
    if "archive" in s:
        return "END_OF_DAY"
    return "REAL_TIME" if is_market_open() else "LAST_CLOSE"


# Core fields are ALWAYS present (never null — verified). Rich fields are
# included only when the serving source actually has them, so the JSON carries
# no null/empty noise.
_OPTIONAL_QUOTE_FIELDS = (
    "upperCircuit", "lowerCircuit", "week52High", "week52Low",
    "totalBuyQty", "totalSellQty", "openInterest", "vwap", "isin", "sector",
)


def _freshness(asof_iso: str | None) -> dict[str, Any]:
    """Honest data-age fields shared by EVERY quote shape (B1 fix).

    ``asOf``      = upstream data timestamp when the feed provides one
                    (Tickertape batch ``date``), else the fetch time (Groww
                    relays no per-tick stamp, so fetch time is the best truth).
    ``fetchedAt`` = server wall-clock when WE obtained it.
    ``feedLagSec``= fetchedAt − asOf — how stale the upstream stamp is (0 when
                    the feed carries no older stamp than fetch).

    Lets a latency-sensitive consumer SEE staleness instead of trusting a single
    timestamp that two code paths used to compute inconsistently (one stamped
    ``now()`` and overstated freshness; the other echoed a ~60 s-old feed stamp).
    """
    now = now_ist()
    out: dict[str, Any] = {"fetchedAt": now.isoformat()}
    if asof_iso:
        out["asOf"] = asof_iso
        try:
            t = datetime.fromisoformat(str(asof_iso).replace("Z", "+00:00"))
            out["feedLagSec"] = round((now - t).total_seconds(), 1)
        except (ValueError, TypeError):
            pass
    else:
        out["asOf"] = now.isoformat()
        out["feedLagSec"] = 0.0
    return out


def _sb_quote(tick) -> dict[str, Any]:
    """TickData → superbrain camelCase quote. Null rich fields are omitted."""
    meta = _META_CACHE.get(tick.symbol, {})
    quality = _quality(tick.source)
    q = {
        "symbol": tick.symbol,
        "companyName": meta.get("name") or getattr(tick, "name", None) or tick.symbol,
        "sector": meta.get("sector"),
        "price": _f(tick.ltp),
        "change": _f(tick.change),
        "changePct": _f(tick.pct_change),
        "open": _f(tick.open),
        "high": _f(tick.high),
        "low": _f(tick.low),
        "volume": int(tick.volume or 0),
        "previousClose": _f(tick.close),
        "upperCircuit": _f(tick.upper_circuit),
        "lowerCircuit": _f(tick.lower_circuit),
        "week52High": _f(tick.week_52_high),
        "week52Low": _f(tick.week_52_low),
        "totalBuyQty": getattr(tick, "total_buy_qty", None),
        "totalSellQty": getattr(tick, "total_sell_qty", None),
        "openInterest": getattr(tick, "open_interest", None),
        "vwap": _f(tick.vwap) or meta.get("vwap"),
        "isin": getattr(tick, "isin", None) or meta.get("isin"),
        "source": tick.source,
        "dataQuality": quality,
        "live": quality == "REAL_TIME",
        "feedLatencyMs": round(getattr(tick, "source_latency_ms", 0.0) or 0.0, 1),
        **_freshness(tick.timestamp.isoformat() if getattr(tick, "timestamp", None) else None),
    }
    # 52-week bounds must contain the live price + today's range. Upstream feeds
    # (Groww yearHigh/Low) can lag a fresh intraday extreme, otherwise reporting a
    # 52w-low ABOVE the current price (or a high below it) — logically wrong for a
    # consumer. Clamp to the live price/low/high for consistency.
    px, lo, hi = q.get("price"), q.get("low"), q.get("high")
    if q.get("week52Low") is not None:
        q["week52Low"] = min(x for x in (q["week52Low"], lo, px) if x is not None)
    if q.get("week52High") is not None:
        q["week52High"] = max(x for x in (q["week52High"], hi, px) if x is not None)

    # Drop optional fields that are genuinely unavailable (no null/empty in JSON).
    for k in _OPTIONAL_QUOTE_FIELDS:
        v = q.get(k)
        if v is None or (isinstance(v, str) and v.strip() == ""):
            q.pop(k, None)
    return q


def _sb_quote_from_tt(symbol: str, row: dict[str, Any]) -> dict[str, Any]:
    """Tickertape batch row → superbrain quote (real NSE, non-delayed).

    Row = {sid, price, close, change, high, low, volume, date}. Lacks ``open`` /
    circuit / 52wk / OI (Groww-only) — those are filled per-symbol via /sb/quote
    or by the Groww fallback. ``changePct`` is derived from change vs prev close.
    """
    meta = _META_CACHE.get(symbol, {})
    price = _f(row.get("price"))
    prev = _f(row.get("close"))
    change = _f(row.get("change"))
    pct = round(change / prev * 100, 2) if (change is not None and prev) else None
    quality = "REAL_TIME" if is_market_open() else "LAST_CLOSE"
    q = {
        "symbol": symbol,
        "companyName": meta.get("name") or symbol,
        "sector": meta.get("sector"),
        "price": price,
        "change": change,
        "changePct": pct,
        "high": _f(row.get("high")),
        "low": _f(row.get("low")),
        "volume": int(_f(row.get("volume")) or 0),
        "previousClose": prev,
        "source": "tickertape_realtime_nse",
        "dataQuality": quality,
        "live": quality == "REAL_TIME",
        **_freshness(row.get("date")),
    }
    if q["sector"] is None:
        q.pop("sector", None)
    return q


async def _enrich_meta(symbol: str, exchange: str) -> None:
    """Fill name/sector/isin cache (kills null companyName/sector/isin).

    Tickertape search resolves name+sector fast; ISIN comes from the in-memory
    BSE master (no HTTP). Details feed is the fallback for name/sector.
    """
    if symbol in _META_CACHE:
        return
    rec = None
    try:
        rec = await tickertape.resolve(symbol)
    except Exception:  # noqa: BLE001
        rec = None
    isin = await market_data_service.get_isin(symbol)
    if rec and rec.get("sector"):
        _META_CACHE[symbol] = {"name": rec.get("name"), "sector": rec.get("sector"), "isin": isin}
        return
    try:
        d = await market_data_service.get_details(symbol, "bse" if exchange == "BSE" else "auto")
    except Exception:  # noqa: BLE001
        d = None
    if d:
        _META_CACHE[symbol] = {
            "name": d.get("name"),
            "sector": (d.get("fundamentals") or {}).get("sector"),
            "isin": isin or (d.get("fundamentals") or {}).get("isin"),
            "vwap": _f((d.get("price") or {}).get("vwap")),
        }
    elif isin:
        _META_CACHE[symbol] = {"isin": isin}


def _norm_candles(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """get_history rows → numeric ascending {timestamp,o,h,l,c,v}."""
    out: list[dict[str, Any]] = []
    for c in rows:
        ts = c.get("timestamp") or c.get("bucket") or c.get("date") or ""
        out.append({
            "timestamp": str(ts),
            "open": _f(c.get("open")), "high": _f(c.get("high")),
            "low": _f(c.get("low")), "close": _f(c.get("close")),
            "volume": int(_f(c.get("volume")) or 0),
        })
    out = [c for c in out if c["close"] is not None]
    out.sort(key=lambda c: c["timestamp"])
    return out


# ── Live-quote single-flight micro-cache (B3) ───────────────────────────────
# A burst of concurrent requests for the SAME symbol (universe sweep, many SSE
# openers, retry storms) must NOT fan out to N upstream fetches — on a single
# process that saturates the host and serialises every caller (observed: one
# request stalled 110 s under load). Collapse them: the first caller starts ONE
# fetch task, concurrent callers await the same task, and the result is briefly
# cached so duplicate hits are sub-millisecond. TTL is short enough that a
# low-latency consumer still sees fresh ticks; the SSE stream stays uncached.
_LIVE_Q_TTL_S = float(os.environ.get("SB_LIVE_QUOTE_TTL_S", "1.0"))
_live_q_cache: dict[str, tuple[float, Any]] = {}
_live_q_inflight: dict[str, "asyncio.Future"] = {}


async def _do_live_fetch(symbol: str, exchange: str, key: str):
    tick = await market_data_service.get_quote_through(symbol, exchange, exclude_delayed=True)
    if not tick and not is_market_open():
        tick = await market_data_service.get_quote_through(symbol, exchange)
    if tick:
        # feed the live-candle sampler for free — only with a SAME-venue tick
        # (tagged by source) so the sampler's volume series stays single-source (B2).
        try:
            if tick.exchange == exchange.upper():
                live_candle_engine.record(symbol, exchange, tick.ltp, tick.volume, source=tick.source)
        except Exception:  # noqa: BLE001
            pass
        _live_q_cache[key] = (time.time(), tick)
    return tick


async def _live_quote(symbol: str, exchange: str):
    """Live (NSE/BSE) quote when open; allow EOD/last-close when shut.

    Single-flight + short TTL (B3): concurrent duplicate-symbol reads share ONE
    upstream fetch instead of hammering the host.
    """
    key = f"{exchange.upper()}:{symbol.upper()}"
    hit = _live_q_cache.get(key)
    if hit and time.time() - hit[0] < _LIVE_Q_TTL_S:
        return hit[1]
    task = _live_q_inflight.get(key)
    if task is None:
        task = asyncio.ensure_future(_do_live_fetch(symbol, exchange, key))
        _live_q_inflight[key] = task
        task.add_done_callback(lambda t: _live_q_inflight.pop(key, None))
    return await task


# ── Quotes ────────────────────────────────────────────────────────────────────

@router.get("/quote/{symbol}", summary="Live quote (superbrain shape)")
async def sb_quote(
    symbol: str,
    exchange: str = Query("NSE", pattern="^(NSE|BSE)$"),
    enrich: bool = Query(True, description="Fill companyName/sector"),
):
    original = (symbol or "").strip().upper()
    _reject_index(original)
    symbol = _canon(symbol)
    if enrich:
        await _enrich_meta(symbol, exchange)
    tick = await _live_quote(symbol, exchange)
    if not tick:
        raise HTTPException(status_code=404, detail=f"No quote for {symbol}")
    q = _sb_quote(tick)
    if symbol != original:
        q["aliasedFrom"] = original  # deprecated symbol canonicalised
    return q


async def _batch_quotes(syms: list[str], exchange: str = "NSE", rich: bool = False) -> dict[str, dict[str, Any]]:
    """symbol → real-NSE quote, minimum latency, no broker / no delayed feed.

    Fast path (NSE, not rich): Tickertape real-NSE batch, one HTTP per ~50-symbol
    chunk. Anything the batch misses (and the whole set when rich/BSE) falls back
    to the per-symbol Groww/MC live fan-out. Yahoo is never used.
    """
    by_sym: dict[str, dict[str, Any]] = {}
    if exchange == "NSE" and not rich:
        try:
            rows = await tickertape.fetch_quotes(syms)
        except Exception:  # noqa: BLE001
            rows = {}
        for s in syms:
            row = rows.get(s) or rows.get(s.upper())
            if row and _f(row.get("price")):
                by_sym[s] = _sb_quote_from_tt(s, row)

    missing = [s for s in syms if s not in by_sym]
    if missing:
        sem = asyncio.Semaphore(_BULK_CONCURRENCY)

        async def _one(s: str):
            async with sem:
                try:
                    tick = await _live_quote(s, exchange)
                    return s, (_sb_quote(tick) if tick else None)
                except Exception:  # noqa: BLE001
                    return s, None

        for s, q in await asyncio.gather(*(_one(s) for s in missing)):
            if q:
                by_sym[s] = q
    return by_sym


# ── Session-open backfill (gap-fade needs `open`; Tickertape batch lacks it) ──
# The real-NSE batch feed (Tickertape /stocks/quotes) returns price/high/low/
# close/volume but NO `open` — superbrain's gap-fade engine REQUIRES the session
# open (it synthesises today's bar from open/high/low). The session open is
# IMMUTABLE once the 09:15 auction prints, so we fetch it ONCE per symbol per day
# (Groww, which carries open + circuit/52wk/vwap) and cache it for the rest of the
# session. First sweep of the day warms the cache; every later sweep is O(1).
_SESSION_OPEN: dict[str, tuple[str, float]] = {}   # symbol → (yyyy-mm-dd, open)


async def _fill_open(by_sym: dict[str, dict[str, Any]], exchange: str) -> None:
    """Ensure each quote carries `open` (+ opportunistic circuit/52wk/vwap).

    Best-effort: a symbol whose live fetch is slow/unavailable simply keeps no
    `open` (same as before) — this never blocks or worsens the batch, only adds.
    """
    today = now_ist().date().isoformat()
    need = [s for s, q in by_sym.items() if q.get("open") is None]
    if not need:
        return

    fetch: list[str] = []
    for s in need:
        cached = _SESSION_OPEN.get(s)
        if cached and cached[0] == today:
            by_sym[s]["open"] = cached[1]
        else:
            fetch.append(s)
    if not fetch:
        return

    sem = asyncio.Semaphore(_BULK_CONCURRENCY)

    async def _one(s: str):
        async with sem:
            try:
                return s, await _live_quote(s, exchange)
            except Exception:  # noqa: BLE001
                return s, None

    for s, tick in await asyncio.gather(*(_one(s) for s in fetch)):
        if not tick:
            continue
        o = _f(tick.open)
        if o is not None:
            by_sym[s]["open"] = o
            _SESSION_OPEN[s] = (today, o)
        # the Groww tick is richer than the Tickertape row — fill bonus fields too.
        rich = _sb_quote(tick)
        for k in ("upperCircuit", "lowerCircuit", "week52High", "week52Low", "vwap"):
            if rich.get(k) is not None and by_sym[s].get(k) is None:
                by_sym[s][k] = rich[k]


@router.get("/quotes", summary="Bulk live quotes (real-NSE batch, no broker / no delayed)")
async def sb_quotes(
    symbols: str = Query(..., description="Comma-separated symbols (max 200)"),
    exchange: str = Query("NSE", pattern="^(NSE|BSE)$"),
    enrich: bool = Query(False, description="Fill companyName/sector (slower)"),
    rich: bool = Query(False, description="Force Groww fan-out (adds open/circuit/52wk/OI, slower)"),
    fast: bool = Query(False, description="Skip the session-open backfill (price-only sweep, leanest)"),
):
    """Bulk quotes optimised for universe sweeps — **no broker API, no delayed
    data**.

    Fast path (default, NSE): one HTTP per ~50-symbol chunk via Tickertape's real
    NSE batch feed (``price/high/low/close/change/volume``). Any symbol the batch
    misses falls back to the per-symbol Groww/MC live fan-out. Set ``rich=true``
    (or use BSE) to take the Groww path for every symbol (adds
    ``open/circuit/52wk/OI`` but is slower). Yahoo is never used here.
    """
    syms = [_canon(s) for s in symbols.split(",") if s.strip()][:200]
    if not syms:
        raise HTTPException(status_code=400, detail="No symbols")

    by_sym = await _batch_quotes(syms, exchange, rich)

    # Backfill the session `open` (Tickertape batch omits it; gap-fade needs it).
    # rich=true already carries open via the Groww fan-out; fast=true opts out.
    if not rich and not fast:
        await _fill_open(by_sym, exchange)

    # optional name/sector enrichment (resolve cache → cheap)
    if enrich:
        for s in list(by_sym.keys()):
            await _enrich_meta(s, exchange)
            meta = _META_CACHE.get(s, {})
            if meta.get("name"):
                by_sym[s]["companyName"] = meta["name"]
            if meta.get("sector"):
                by_sym[s]["sector"] = meta["sector"]

    out = [by_sym[s] for s in syms if s in by_sym]      # input order preserved
    failed = [s for s in syms if s not in by_sym]
    sources: dict[str, int] = {}
    for q in out:
        sources[q["source"]] = sources.get(q["source"], 0) + 1
    return {"count": len(out), "failed": failed, "sources": sources, "quotes": out}


# ── Candles ───────────────────────────────────────────────────────────────────

async def _today_bar_from_quote(symbol: str, exchange: str) -> dict[str, Any] | None:
    """Build TODAY's forming daily bar from the live quote (real, not delayed).

    Yahoo's daily series lags the current session; overlay the live MC/BSE quote
    so the latest daily candle is real-time.
    """
    tick = await _live_quote(symbol, exchange)
    if not tick or _f(tick.ltp) is None:
        return None
    o, h, l, c = _f(tick.open), _f(tick.high), _f(tick.low), _f(tick.ltp)
    return {
        "timestamp": now_ist().date().isoformat() + "T00:00:00+05:30",
        "open": o if o else c, "high": h if h else c, "low": l if l else c,
        "close": c, "volume": int(tick.volume or 0), "live": True,
        "source": tick.source,
    }


@router.get("/candles/{symbol}", summary="Daily candles (numeric, CA-adjusted, live today-bar)")
async def sb_candles(
    symbol: str,
    exchange: str = Query("NSE", pattern="^(NSE|BSE)$"),
    range: str = Query("6mo"),
    interval: str = Query("1d"),
    adjust: bool = Query(True, description="Back-adjust splits/bonus"),
    liveLast: bool = Query(True, description="Overlay today's forming bar with the live quote"),
):
    _reject_index(symbol)
    symbol = _canon(symbol)
    code = _INTERVAL_MAP.get(interval, "1d")
    limit = _RANGE_LIMIT.get(range, 130)
    rows = await market_data_service.get_history(symbol, exchange, code, limit=limit)
    candles = _norm_candles(rows)
    events: list[dict[str, Any]] = []
    if adjust and code in ("1d", "1w"):
        candles, events = adjust_for_splits(candles)

    # Overlay/append today's REAL forming bar (kills Yahoo's delayed last bar).
    live_overlay = False
    if liveLast and code == "1d" and is_market_open():
        tb = await _today_bar_from_quote(symbol, exchange)
        if tb:
            today = now_ist().date().isoformat()
            if candles and candles[-1]["timestamp"][:10] == today:
                candles[-1] = {k: tb[k] for k in ("timestamp", "open", "high", "low", "close", "volume")}
            else:
                candles.append({k: tb[k] for k in ("timestamp", "open", "high", "low", "close", "volume")})
            live_overlay = True

    return {
        "symbol": symbol, "exchange": exchange, "interval": code, "range": range,
        "count": len(candles), "adjusted": bool(events), "splitEvents": events,
        "liveLastBar": live_overlay, "candles": candles,
    }


@router.get("/intraday/{symbol}", summary="Real-time intraday candles (BSE live + 1s/10s tick)")
async def sb_intraday(
    symbol: str,
    exchange: str = Query("NSE", pattern="^(NSE|BSE)$"),
    interval: str = Query("30minute"),
    limit: int | None = Query(None, ge=1, le=100000, description="Max bars to return; OMIT for ALL data (no truncation)"),
    date: str | None = Query(None, description="YYYY-MM-DD — past-day sub-minute (1s/10s) pull from the durable store"),
):
    """``interval`` options, fastest first:
    - ``1second`` / ``10second`` → live tick aggregation (NSE/BSE relay, sub-second).
      Pass ``date=YYYY-MM-DD`` (a past day) to pull from the durable 1s store.
    - ``1minute``…``30minute`` → REAL BSE intraday curve (StockReachGraph),
      falling back to Yahoo only if BSE has no data for the symbol.
    """
    _reject_index(symbol)
    symbol = _canon(symbol)

    # Sub-minute → live sampled aggregation (lowest latency)
    if interval in _SECONDS_INTERVALS:
        seconds = _SECONDS_INTERVALS[interval]

        # PAST-DAY pull → durable TimescaleDB store (capture-or-never: only days
        # the recorder ran + flushed exist; reads stay honest about gaps).
        today = now_ist().date().isoformat()
        if date:
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid date (want YYYY-MM-DD): {date}")
        if date and date < today:
            res = await read_past_intraday(symbol, exchange, seconds, date)
            if res is None:
                raise HTTPException(
                    status_code=503,
                    detail="Durable sub-minute store not connected — past-day 1s/10s "
                           "needs a TimescaleDB DATABASE_URL. Same-day works without it.")
            served = res["candles"][-limit:] if limit else res["candles"]
            return {
                "symbol": symbol, "exchange": exchange, "interval": interval,
                "granularitySeconds": seconds, "date": date,
                "source": "timescaledb_intraday_store",
                "captured": res["captured"],
                "dataQuality": "HISTORICAL" if res["captured"] else "NOT_CAPTURED",
                "count": len(served), "candles": served,
            }

        await live_candle_engine.ensure_recorder(symbol, exchange)
        await live_candle_engine.sample_once(symbol, exchange)
        candles = live_candle_engine.build(symbol, exchange, seconds, limit=limit or 10**9)
        st = live_candle_engine.status(symbol, exchange)
        return {
            "symbol": symbol, "exchange": exchange, "interval": interval,
            "granularitySeconds": seconds, "source": "live_tick_aggregation",
            "dataQuality": "REAL_TIME" if st["market_open"] else "STALE_NO_SESSION",
            "warmingUp": len(candles) < 3, "recording": st["recording"],
            "marketOpen": st["market_open"], "samples": st["samples"],
            "count": len(candles), "candles": candles,
        }

    if interval not in INTRADAY_SECONDS:
        supported = sorted(set(_SECONDS_INTERVALS) | set(INTRADAY_SECONDS))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported intraday interval '{interval}'. Supported: {supported}")
    seconds = INTRADAY_SECONDS[interval]

    # PRIMARY: real NSE intraday from Tickertape → aggregate to the timeframe.
    try:
        tt_points = await tickertape.fetch_intraday_points(symbol, "1d")
    except Exception:  # noqa: BLE001
        tt_points = None
    if tt_points:
        candles = _latest_session_only(points_to_ohlc(tt_points, seconds, limit=(limit + 200) if limit else 10**9))
        if limit:
            candles = candles[-limit:]
        if candles:
            return {
                "symbol": symbol, "exchange": "NSE", "interval": interval,
                "source": "tickertape_realtime_nse",
                "dataQuality": "REAL_TIME" if is_market_open() else "TODAY_SESSION",
                "sessionDate": candles[-1]["timestamp"][:10],
                "count": len(candles), "candles": candles,
            }

    # SECONDARY: real BSE intraday curve (StockReachGraph).
    points = await market_data_service.get_intraday_series(symbol, "0")
    if points:
        candles = _latest_session_only(points_to_ohlc(points, seconds, limit=(limit + 200) if limit else 10**9))
        if limit:
            candles = candles[-limit:]
        if candles:
            return {
                "symbol": symbol, "exchange": "BSE", "interval": interval,
                "source": "bse_stockreach_realtime",
                "dataQuality": "REAL_TIME" if is_market_open() else "TODAY_SESSION",
                "sessionDate": candles[-1]["timestamp"][:10],
                "count": len(candles), "candles": candles,
            }

    # FALLBACK: Yahoo intraday (delayed) — only when both real feeds miss.
    code = _INTERVAL_MAP.get(interval)
    rows = await market_data_service.get_history(symbol, exchange, code, limit=limit) if code else []
    candles = _norm_candles(rows)
    today = now_ist().date().isoformat()
    today_bars = [c for c in candles if c["timestamp"][:10] == today]
    served = today_bars if today_bars else (candles[-limit:] if limit else candles)
    for c in served:
        c["oi"] = 0
    return {
        "symbol": symbol, "exchange": exchange, "interval": code,
        "source": "yahoo_intraday_delayed", "dataQuality": "DELAYED",
        "todayOnly": bool(today_bars),
        "note": "real NSE/BSE intraday unavailable for this symbol — Yahoo fallback (15-min delayed)",
        "count": len(served), "candles": served,
    }


@router.get("/intervals", summary="All supported candle timeframes")
async def sb_intervals():
    """Every timeframe the feed serves, fastest first — for the live-study UI."""
    return {
        "subSecondAndSeconds": {
            "intervals": list(_SECONDS_INTERVALS),
            "source": "live_tick_aggregation (NSE/BSE feed sampled, never delayed)",
            "finestSeconds": min(_SECONDS_INTERVALS.values()),
            "samplerCadenceSeconds": SAMPLE_INTERVAL_S,
            "note": (
                "Sub-second buckets are BEST-EFFORT: each sample is a live "
                "upstream fetch, so effective cadence = max(fetch_latency, "
                f"{SAMPLE_INTERVAL_S}s) — typically ~1-2s on a public feed, not "
                "true 250ms. See /sb/diagnostics or stream 'source' for the "
                "measured rate. Genuine tick-level needs a broker WebSocket."
            ),
        },
        "minutesAndHours": {
            "intervals": sorted(set(INTRADAY_SECONDS)),
            "source": "tickertape_realtime_nse -> bse_stockreach_realtime -> yahoo(last resort)",
        },
        "daily": {"endpoint": "/sb/candles", "ranges": list(_RANGE_LIMIT)},
        "stream": {"endpoint": "/sb/stream/{symbol}", "transport": "SSE",
                   "note": "server-push ticks, lowest latency"},
    }


@router.get("/recorder", summary="Always-on watchlist recorder status")
async def sb_recorder_status():
    """State of the gap-free same-day 1s capture loop (watchlist, ticks, flush)."""
    return watchlist_recorder.status()


@router.post("/recorder", summary="Set + start the watchlist recorder")
async def sb_recorder_set(symbols: str = Query(..., description="Comma-separated symbols to record")):
    """Point the always-on recorder at a watchlist and start it (e.g. the
    scanner's active universe). Persists 1s candles when a durable store is wired."""
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not syms:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    watchlist_recorder.set_watchlist(syms)
    await watchlist_recorder.start()
    return {"status": "started", **watchlist_recorder.status()}


@router.post("/prune", summary="Run the retention prune now")
async def sb_prune(
    intradayDays: int | None = Query(None, ge=1, description="Override intraday retention days"),
    ticksDays: int | None = Query(None, ge=1, description="Override ticks retention days"),
):
    """Manually delete sub-minute rows older than the retention window. Runs
    automatically every PRUNE_INTERVAL_H hours; this is the on-demand trigger."""
    from ...engine.intraday_store import prune_old
    return await prune_old(intradayDays, ticksDays)


@router.get("/cache", summary="Intraday history cache stats")
async def sb_cache_stats():
    """Size (entries + approx MB) + TTLs of the past/today candle caches."""
    return {"past": _HIST_CACHE.stats(), "today": _TODAY_CACHE.stats()}


@router.post("/cache/clear", summary="Clear the intraday history cache")
async def sb_cache_clear():
    """Drop all cached candles (e.g. after a corporate action / forced refresh)."""
    return {"cleared": _HIST_CACHE.clear() + _TODAY_CACHE.clear()}


@router.get("/stream/{symbol}", summary="Live tick stream (SSE, minimum latency, no broker)")
async def sb_stream(
    symbol: str,
    exchange: str = Query("NSE", pattern="^(NSE|BSE)$"),
    maxSeconds: int = Query(300, ge=5, le=3600, description="Auto-close after N seconds"),
):
    """Server-Sent-Events tick stream — pushes each new sampled tick the instant
    it is recorded (no client polling gap). Backed by the live-candle sampler
    (Groww/MC NSE/BSE feed, never delayed). Lowest-latency way to study live.

    Consume with EventSource('/api/v1/sb/stream/RELIANCE'); each `data:` line is
    `{timestamp, epoch, price, cumVolume}`.
    """
    _reject_index(symbol)
    symbol = _canon(symbol)
    await live_candle_engine.ensure_recorder(symbol, exchange)

    async def gen():
        yield f"event: open\ndata: {json.dumps({'symbol': symbol, 'exchange': exchange, 'marketOpen': is_market_open(), 'samplerSeconds': SAMPLE_INTERVAL_S})}\n\n"
        start = time.time()
        last_epoch = None
        poll = 0.2  # read faster than the sampler so a new tick is forwarded promptly
        while time.time() - start < maxSeconds:
            # keep the recorder alive + grab the freshest sample
            tick = live_candle_engine.latest_tick(symbol, exchange)
            if tick and tick["epoch"] != last_epoch:
                last_epoch = tick["epoch"]
                yield f"data: {json.dumps(tick)}\n\n"
            else:
                yield ": keep-alive\n\n"   # comment frame keeps the connection open
            await asyncio.sleep(poll)
        yield f"event: close\ndata: {json.dumps({'reason': 'maxSeconds reached'})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


async def _today_session_bars(
    symbol: str, exchange: str, bucket_seconds: int, limit: int = 500
) -> list[dict[str, Any]]:
    """TODAY's intraday OHLC bars built from REAL scrapers only (never Yahoo).

    ARMED RULE: the current market day is served exclusively by the live
    exchange scrapers — Tickertape (NSE) first, BSE StockReachGraph second —
    aggregated to the requested bucket. Returns [] outside a session or when no
    real feed is reachable (the caller then has no today bars, never delayed).
    """
    today = now_ist().date().isoformat()
    pts: list[dict[str, Any]] | None = None

    # PRIMARY: real NSE intraday curve (Tickertape).
    try:
        tt = await tickertape.fetch_intraday_points(symbol, "1d")
        if tt:
            pts = _latest_session_only(tt)
    except Exception:  # noqa: BLE001
        pts = None
    # SECONDARY: real BSE intraday curve (StockReachGraph).
    if not pts:
        try:
            bse = await market_data_service.get_intraday_series(symbol, "0")
            if bse:
                pts = _latest_session_only(bse)
        except Exception:  # noqa: BLE001
            pts = None
    if not pts:
        return []

    # Strict: keep ONLY the current calendar day's points.
    pts = [p for p in pts if str(p.get("timestamp", ""))[:10] == today]
    if not pts:
        return []
    return points_to_ohlc(pts, bucket_seconds, limit=limit)


@router.get("/history/{symbol}", summary="Dated candles (day/30minute, +oi, CA-adjusted)")
async def sb_history(
    symbol: str,
    exchange: str = Query("NSE", pattern="^(NSE|BSE)$"),
    interval: str = Query("day"),
    from_: str | None = Query(None, alias="from", description="YYYY-MM-DD"),
    to: str | None = Query(None, description="YYYY-MM-DD"),
    adjust: bool = Query(True),
    stream: bool = Query(False, description="Stream candles as NDJSON (one bar/line) for huge pulls"),
):
    symbol = _canon(symbol)
    code = _INTERVAL_MAP.get(interval)
    if not code:
        raise HTTPException(status_code=400, detail=f"Unsupported interval: {interval}")

    def _parse(d: str) -> datetime:
        try:
            return datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date: {d}")

    end_dt = _parse(to) if to else now_ist().replace(tzinfo=None)
    # Intraday intervals have a bounded history on the upstream feed (Yahoo caps
    # 1m≈7d, 5m/15m/30m≈60d). Default the lookback to that window — and clamp an
    # over-long `from` to it — so finer-timeframe requests return bars instead of
    # an empty set. Daily/weekly keep deep history.
    max_days = _INTERVAL_MAX_DAYS.get(code, 365 * 10)
    default_days = min(max_days, 365)
    start_dt = _parse(from_) if from_ else end_dt - timedelta(days=default_days)
    floor_dt = end_dt - timedelta(days=max_days)
    if start_dt < floor_dt:
        start_dt = floor_dt
    # Past bars from Yahoo, cached for the session (immutable once settled) so a
    # universe sweep doesn't re-fetch the same history every scan.
    rows = await _cached_history(symbol, exchange, code, start_dt, end_dt)
    candles = _norm_candles(rows)

    # ARMED RULE: for intraday timeframes the CURRENT market day is served by the
    # live exchange scrapers ONLY (never the 15-min-delayed Yahoo feed). Drop any
    # of today's bars Yahoo returned and replace them with the real scraper-built
    # session (Tickertape → BSE), cached ~30s. Past days stay on Yahoo (genuine).
    today = now_ist().date().isoformat()
    today_source = None
    if code in INTRADAY_SECONDS:
        candles = [c for c in candles if c["timestamp"][:10] < today]
        if start_dt.date().isoformat() <= today <= end_dt.date().isoformat():
            tbars = await _cached_today_bars(symbol, exchange, INTRADAY_SECONDS[code])
            if tbars:
                candles += tbars
                today_source = "scraper_live"

    events: list[dict[str, Any]] = []
    if adjust and code in ("1d", "1w"):
        candles, events = adjust_for_splits(candles)
    for c in candles:
        c.setdefault("oi", 0)

    meta = {
        "symbol": symbol, "exchange": exchange, "interval": code,
        "from": start_dt.date().isoformat(), "to": end_dt.date().isoformat(),
        "count": len(candles), "adjusted": bool(events), "splitEvents": events,
        "pastSource": "yahoo", "todaySource": today_source,
    }

    # NDJSON streaming: a header line, then one candle per line. Emits incrementally
    # so an arbitrarily large pull never materialises one giant JSON blob in RAM and
    # the client gets first bytes immediately. Combined with GZip this delivers ALL
    # bars cheaply. Non-stream path returns the usual single JSON object.
    if stream:
        import orjson

        def gen():
            yield orjson.dumps({"meta": meta}) + b"\n"
            for cdl in candles:
                yield orjson.dumps(cdl) + b"\n"

        return StreamingResponse(gen(), media_type="application/x-ndjson")

    return {**meta, "candles": candles}


# ── Fundamentals ───────────────────────────────────────────────────────────────

@router.get("/fundamentals/{symbol}", summary="Full fundamental ratio set")
async def sb_fundamentals(symbol: str):
    data = await fetch_fundamentals(_canon(symbol))
    if not data:
        raise HTTPException(status_code=404, detail=f"No fundamentals for {symbol}")
    return data


# ── Macro context ──────────────────────────────────────────────────────────────

@router.get("/context", summary="Indices + macro + FII/DII (regime context)")
async def sb_context():
    indices_raw = await market_data_service.get_indices()
    by_name = {(d.get("index") or "").upper(): d for d in indices_raw}

    def idx(name: str) -> dict[str, Any] | None:
        d = by_name.get(name.upper())
        if not d:
            return None
        return {"last": _f(d.get("last")), "changePct": _f(d.get("pct_change")),
                "change": _f(d.get("change")),
                "advances": d.get("advances"), "declines": d.get("declines")}

    indices = {n: idx(n) for n in ("NIFTY 50", "NIFTY BANK", "NIFTY NEXT 50",
                                   "NIFTY IT", "NIFTY MIDCAP 100") if idx(n)}
    vix = idx("INDIA VIX")
    macro = await fetch_macro()
    fii_dii = await fetch_fii_dii()
    try:
        mmi = await tickertape.fetch_mmi()
    except Exception:  # noqa: BLE001
        mmi = None

    nifty_pct = (indices.get("NIFTY 50") or {}).get("changePct") or 0
    usdinr_pct = (macro.get("USDINR") or {}).get("changePct") or 0
    brent_pct = (macro.get("BRENT") or {}).get("changePct") or 0
    risk_on = nifty_pct - max(0, usdinr_pct * 0.8) - max(0, brent_pct * 0.5)
    regime = "RISK_ON" if risk_on >= 0.8 else "RISK_OFF" if risk_on <= -0.8 else "BALANCED"

    return {
        "regime": regime,
        "riskOnScore": round(risk_on, 2),
        "indices": indices,
        "vix": (vix or {}).get("last"),
        "macro": macro,
        "fiiDii": fii_dii,
        "marketMood": mmi,
        "marketOpen": is_market_open(),
        "generatedAt": now_ist().isoformat(),
    }


@router.get("/mmi", summary="Market Mood Index (Tickertape sentiment gauge)")
async def sb_mmi():
    mmi = await tickertape.fetch_mmi()
    if not mmi:
        raise HTTPException(status_code=503, detail="MMI unavailable")
    return mmi


# ── Screener (Chartink) ─────────────────────────────────────────────────────────

@router.get("/scans", summary="List built-in Chartink scan names")
async def sb_scans():
    return {"scans": sorted(chartink.PREBUILT_SCANS.keys()),
            "usage": "/sb/screen?scan=<name>  OR  POST /sb/screen {clause}"}


@router.get("/screen", summary="Run a Chartink screener (real-NSE prices overlaid)")
async def sb_screen(
    scan: str | None = Query(None, description="Built-in scan name (see /sb/scans)"),
    clause: str | None = Query(None, description="Raw Chartink scan_clause"),
    limit: int = Query(500, ge=1, le=2000),
    live: bool = Query(True, description="Overlay row price/changePct/volume with the real-NSE feed"),
    liveLimit: int = Query(200, ge=1, le=500, description="Max rows to re-price live"),
):
    """Chartink decides **which symbols match** the scan; the row
    ``close/changePct/volume`` it returns is Chartink's own (free intraday feed
    lags). With ``live=true`` (default) those fields are overlaid with our
    minimum-latency real-NSE feed (Tickertape batch → Groww), so the prices are
    fresh — Chartink's stale value is kept as ``chartinkClose`` for reference.
    """
    raw = clause or (chartink.PREBUILT_SCANS.get(scan) if scan else None)
    if not raw:
        raise HTTPException(status_code=400,
                            detail=f"Provide ?scan= (one of {sorted(chartink.PREBUILT_SCANS)}) or ?clause=")
    result = await chartink.run_scan(raw, limit=limit)
    if result is None:
        raise HTTPException(status_code=503, detail="Chartink unavailable")

    price_source = "chartink_delayed"
    price_quality = "DELAYED"
    if live and result.get("stocks"):
        # re-price the matched symbols with the real-NSE feed (Chartink stays the
        # scan engine; only the displayed numbers are swapped for fresh ones).
        head = [r for r in result["stocks"][:liveLimit] if r.get("symbol")]
        canon = list({_canon(r["symbol"]) for r in head})
        try:
            live_map = await _batch_quotes(canon, "NSE", rich=False)
        except Exception:  # noqa: BLE001
            live_map = {}
        srcs: dict[str, int] = {}
        for r in result["stocks"]:
            q = live_map.get(_canon(r.get("symbol") or ""))
            if not q:
                continue
            if r.get("close") is not None:
                r["chartinkClose"] = r.get("close")
            r["close"] = q.get("price")
            r["price"] = q.get("price")
            if q.get("changePct") is not None:
                r["changePct"] = q["changePct"]
            if q.get("volume") is not None:
                r["volume"] = q["volume"]
            r["priceSource"] = q.get("source")
            srcs[q.get("source")] = srcs.get(q.get("source"), 0) + 1
        if srcs:
            price_source = srcs
            price_quality = "REAL_TIME" if is_market_open() else "LAST_CLOSE"

    return {"scan": scan, "clause": raw, **result,
            "priceSource": price_source, "priceQuality": price_quality,
            "livePriced": bool(live), "asOf": now_ist().isoformat()}


# ── Universe / resolver ────────────────────────────────────────────────────────

@router.get("/universe", summary="Instrument universe (symbol → identity)")
async def sb_universe(
    exchange: str = Query("NSE"),
    limit: int = Query(6000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    rows = await market_data_service.get_all_snapshot(exchange.strip().upper())
    total = len(rows)
    page = rows[offset:offset + limit]
    # The bhavcopy snapshot carries no ISIN — backfill from the in-memory BSE
    # master (O(1) dict lookup, no HTTP) so the resolver returns a usable ISIN.
    await bse_master.ensure_loaded()
    items = [{
        "symbol": r.get("symbol"),
        "name": r.get("name") or r.get("symbol"),
        "sector": r.get("sector"),
        "exchange": r.get("exchange", exchange.upper()),
        "series": r.get("series"),
        "isin": r.get("isin") or bse_master.isin_for(str(r.get("symbol") or "")),
    } for r in page]
    return {"exchange": exchange.upper(), "total": total, "count": len(items), "items": items}


@router.get("/resolve", summary="Resolve a symbol / company name / alias")
async def sb_resolve(q: str = Query(..., min_length=1), exchange: str = Query("NSE")):
    ql = q.strip().upper()
    rows = await market_data_service.get_all_snapshot(exchange.strip().upper())
    matches = []
    for r in rows:
        sym = str(r.get("symbol") or "").upper()
        if ql == sym:
            matches.insert(0, r)
        elif ql in sym:
            matches.append(r)
        if len(matches) >= 20:
            break
    if not matches:
        raise HTTPException(status_code=404, detail=f"No instrument matching '{q}'")
    await bse_master.ensure_loaded()   # ISIN backfill (snapshot has none)
    return {"query": q, "count": len(matches), "matches": [{
        "symbol": r.get("symbol"), "name": r.get("name") or r.get("symbol"),
        "exchange": r.get("exchange", exchange.upper()),
        "isin": r.get("isin") or bse_master.isin_for(str(r.get("symbol") or "")),
        "series": r.get("series"),
    } for r in matches[:20]]}


# ── Data-availability diagnostics ───────────────────────────────────────────────

@router.get("/diagnostics/{symbol}", summary="Probe every source; report null/stale data")
async def sb_diagnostics(symbol: str, exchange: str = Query("NSE", pattern="^(NSE|BSE)$")):
    """Per-source availability matrix for a symbol — surfaces null/incorrect data.

    Probes the real feeds (live quote, BSE intraday, daily history, fundamentals,
    details) and reports what each returns, which fields are null, and the best
    real-data source available right now.
    """
    import time as _time
    symbol = _canon(symbol)
    report: dict[str, Any] = {"symbol": symbol, "exchange": exchange,
                              "marketOpen": is_market_open(), "sources": {}}

    async def probe(name: str, coro):
        t0 = _time.time()
        try:
            res = await coro
            return name, {"ok": bool(res), "latencyMs": round((_time.time() - t0) * 1000, 1), "data": res}
        except Exception as e:  # noqa: BLE001
            return name, {"ok": False, "latencyMs": round((_time.time() - t0) * 1000, 1), "error": str(e)[:140]}

    # Live quote (real)
    _, q = await probe("live_quote", market_data_service.get_quote_through(symbol, exchange, exclude_delayed=True))
    tick = q.pop("data", None)
    if tick:
        sbq = _sb_quote(tick)
        q["source"] = sbq["source"]
        q["dataQuality"] = sbq["dataQuality"]
        q["nullFields"] = [k for k, v in sbq.items() if v is None]
        q["ltp"] = sbq["price"]
    report["sources"]["live_quote"] = q

    # Tickertape intraday (real NSE — intraday primary)
    _, tt = await probe("tickertape_intraday", tickertape.fetch_intraday_points(symbol, "1d"))
    ttp = tt.pop("data", None)
    tt["points"] = len(ttp) if ttp else 0
    tt["lastPoint"] = ttp[-1] if ttp else None
    report["sources"]["tickertape_intraday"] = tt

    # BSE intraday curve (real — intraday secondary)
    _, intr = await probe("bse_intraday", market_data_service.get_intraday_series(symbol, "0"))
    pts = intr.pop("data", None)
    intr["points"] = len(pts) if pts else 0
    intr["lastPoint"] = pts[-1] if pts else None
    report["sources"]["bse_intraday"] = intr

    # Daily history (Yahoo completed)
    _, dh = await probe("daily_history", market_data_service.get_history(symbol, exchange, "1d", limit=3))
    rows = dh.pop("data", None)
    dh["candles"] = len(rows) if rows else 0
    dh["lastClose"] = _f(rows[-1].get("close")) if rows else None
    report["sources"]["daily_history"] = dh

    # Fundamentals
    _, fu = await probe("fundamentals", fetch_fundamentals(symbol))
    fdata = fu.pop("data", None)
    if fdata:
        fu["source"] = fdata.get("source")
        fu["presentFields"] = [k for k, v in fdata.items() if v is not None and k not in ("symbol", "source")]
        fu["nullFields"] = [k for k, v in fdata.items() if v is None]
    report["sources"]["fundamentals"] = fu

    # Overall verdict
    live_ok = report["sources"]["live_quote"].get("ok")
    tt_real = report["sources"]["tickertape_intraday"]["points"] > 0
    bse_real = report["sources"]["bse_intraday"]["points"] > 0
    report["verdict"] = {
        "liveQuote": "REAL" if live_ok else "UNAVAILABLE",
        "intraday": "REAL_NSE_TICKERTAPE" if tt_real else "REAL_BSE" if bse_real else "YAHOO_FALLBACK_OR_NONE",
        "daily": "OK" if report["sources"]["daily_history"]["candles"] > 0 else "UNAVAILABLE",
        "fundamentals": "OK" if report["sources"]["fundamentals"].get("ok") else "UNAVAILABLE",
    }
    return report
