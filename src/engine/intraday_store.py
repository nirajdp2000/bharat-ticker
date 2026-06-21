"""Durable sub-minute (1s/10s) candle store — past-day intraday retrieval.

ARMED-RULE CONTEXT: the current market day is served live from the in-memory
ring (``live_candle_engine``); this module is the *persistence* layer that makes
PAST days pullable. There is no upstream backfill for sub-minute Indian data —
a past day exists here only if the recorder ran and flushed it that session
(capture-or-never). Reads therefore distinguish three states honestly:

    store not connected   → None             (no durable DB wired)
    connected, no rows    → captured = False  (that day was never captured)
    connected, rows       → captured = True   (serve the bars)

Backed by TimescaleDB (``intraday_candles`` 1s base + time_bucket roll-up).
Everything degrades to a safe no-op when the DB is down, so the live path is
never blocked.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from ..db.connection import db_manager
from ..db.queries import TickerQueries
from ..utils.logger import get_logger
from .live_candles import live_candle_engine

log = get_logger(__name__)
IST = ZoneInfo("Asia/Kolkata")

# Retention (vanilla Postgres has no TimescaleDB auto-policy, so the app prunes).
INTRADAY_RETENTION_DAYS = int(os.environ.get("INTRADAY_RETENTION_DAYS", "60"))
TICKS_RETENTION_DAYS = int(os.environ.get("TICKS_RETENTION_DAYS", "14"))
PRUNE_INTERVAL_H = float(os.environ.get("PRUNE_INTERVAL_H", "24"))

_PRUNE_INTRADAY = sa.text(
    "DELETE FROM intraday_candles WHERE time < now() - make_interval(days => :d)")
_PRUNE_TICKS = sa.text(
    "DELETE FROM ticks WHERE time < now() - make_interval(days => :d)")

# Per-symbol high-water mark of the last flushed 1s bar (incremental flush).
_LAST_FLUSH_TS: dict[str, str] = {}


def _day_window(day: str) -> tuple[datetime, datetime]:
    """'YYYY-MM-DD' → [00:00, next-00:00) IST datetimes."""
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=IST)
    return start, start + timedelta(days=1)


async def flush_live_to_store(symbol: str, exchange: str = "NSE") -> int:
    """Persist the engine's current 1-second candles for a symbol (idempotent).

    No-op (returns 0) when no durable DB is connected. Safe to call repeatedly
    during a session — the upsert dedupes by (symbol, exchange, time).
    """
    if not db_manager.is_connected:
        return 0
    candles = live_candle_engine.build(symbol, exchange, seconds=1, limit=100000)
    if not candles:
        return 0
    key = f"{exchange.upper()}:{symbol.upper()}"
    # INCREMENTAL: only flush candles at/after the last flushed bar. Without this
    # every 30s flush re-upserts the entire day's ring (→ 22.5k rows/symbol by EOD
    # every cycle) and would crush a small Postgres. build() bars share one IST
    # offset, so plain ISO string compare is chronological. The last (forming) bar
    # is re-flushed once next cycle so its final value is corrected (idempotent).
    since = _LAST_FLUSH_TS.get(key, "")
    rows: list[dict[str, Any]] = []
    for c in candles:
        ts = c.get("timestamp", "")
        if ts < since:
            continue
        try:
            t = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            continue
        rows.append({
            "time": t, "symbol": symbol.upper(), "exchange": exchange.upper(),
            "open": c["open"], "high": c["high"], "low": c["low"],
            "close": c["close"], "volume": int(c.get("volume") or 0),
        })
    if not rows:
        return 0
    try:
        session = db_manager.get_session()
        async with session:
            await TickerQueries(session).upsert_intraday_candles(rows)
        _LAST_FLUSH_TS[key] = candles[-1].get("timestamp", since)
        return len(rows)
    except Exception as e:  # noqa: BLE001
        log.warning("intraday_flush_failed", symbol=symbol, error=str(e))
        return 0


async def read_past_intraday(
    symbol: str, exchange: str, bucket_seconds: float, day: str,
) -> dict[str, Any] | None:
    """Past-day sub-minute candles for ``day`` at ``bucket_seconds`` resolution.

    Returns None when no durable store is connected; otherwise a dict with
    ``captured`` (was the day ever recorded) and ``candles``.
    """
    if not db_manager.is_connected:
        return None
    start, end = _day_window(day)
    bucket_s = max(1.0, float(round(bucket_seconds)))
    try:
        session = db_manager.get_session()
        async with session:
            q = TickerQueries(session)
            n = await q.count_intraday(symbol.upper(), exchange.upper(), start, end)
            if n == 0:
                return {"captured": False, "candles": []}
            rows = await q.get_intraday_candles(
                symbol.upper(), exchange.upper(), bucket_s, start, end)
    except Exception as e:  # noqa: BLE001
        log.warning("intraday_read_failed", symbol=symbol, day=day, error=str(e))
        return None

    candles = [{
        "timestamp": r["bucket"].astimezone(IST).isoformat(),
        "open": round(float(r["open"]), 2), "high": round(float(r["high"]), 2),
        "low": round(float(r["low"]), 2), "close": round(float(r["close"]), 2),
        "volume": int(r["volume"] or 0), "oi": 0,
    } for r in rows]
    return {"captured": True, "candles": candles}


# ── Retention prune (replaces TimescaleDB's auto-policy on vanilla Postgres) ──

async def prune_old(intraday_days: int | None = None, ticks_days: int | None = None) -> dict[str, Any]:
    """Delete sub-minute rows older than the retention window. No-op without DB."""
    if not db_manager.is_connected:
        return {"pruned": False, "reason": "no_db"}
    idays = intraday_days or INTRADAY_RETENTION_DAYS
    tdays = ticks_days or TICKS_RETENTION_DAYS
    try:
        session = db_manager.get_session()
        async with session:
            await session.execute(_PRUNE_INTRADAY, {"d": idays})
            await session.execute(_PRUNE_TICKS, {"d": tdays})
            await session.commit()
        log.info("intraday_pruned", intradayDays=idays, ticksDays=tdays)
        return {"pruned": True, "intradayDays": idays, "ticksDays": tdays}
    except Exception as e:  # noqa: BLE001
        log.warning("intraday_prune_failed", error=str(e))
        return {"pruned": False, "error": str(e)}


_prune_task: asyncio.Task | None = None


async def _prune_loop() -> None:
    # First prune shortly after boot, then every PRUNE_INTERVAL_H hours.
    await asyncio.sleep(60)
    while True:
        await prune_old()
        await asyncio.sleep(PRUNE_INTERVAL_H * 3600)


def start_prune_scheduler() -> None:
    """Launch the daily retention prune (idempotent; no-op until DB is up)."""
    global _prune_task
    if _prune_task and not _prune_task.done():
        return
    _prune_task = asyncio.create_task(_prune_loop())
    log.info("prune_scheduler_started", intervalH=PRUNE_INTERVAL_H,
             intradayDays=INTRADAY_RETENTION_DAYS, ticksDays=TICKS_RETENTION_DAYS)
