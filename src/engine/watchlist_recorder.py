"""Always-on watchlist recorder — gap-free same-day 1s/10s capture.

WHY: the per-symbol on-demand recorder (live_candle_engine) only runs while a
symbol is being read and idle-stops after 120s, so the durable store fills with
HOLES (only the windows something happened to poll). This service keeps a fixed
watchlist sampled continuously through the whole session and flushes it, so
PAST-day sub-minute history is complete, not sparse — and reads on watchlist
symbols are always warm (no cold-start).

SCALES via BATCH: one Tickertape `/stocks/quotes` call covers the whole list per
cadence (1 HTTP/tick), so it does NOT hit the single shared Groww session
ceiling that per-symbol polling would. Opt-in: dormant unless WATCHLIST is set.

    WATCHLIST="RELIANCE,TCS,INFY,..."   # comma list; empty → disabled
    WATCHLIST_RECORD_INTERVAL_S=1.0      # sample cadence (1s → 1s base candles)
"""

from __future__ import annotations

import asyncio
import os
import time

from ..utils.ist_clock import is_market_open
from ..utils.logger import get_logger
from . import tickertape
from .intraday_store import flush_live_to_store
from .live_candles import STORE_FLUSH_INTERVAL_S, live_candle_engine

log = get_logger(__name__)


def _env_watchlist() -> list[str]:
    raw = os.environ.get("WATCHLIST", "")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


class WatchlistRecorder:
    """Batch-samples a watchlist every cadence and flushes 1s candles to the store."""

    def __init__(self) -> None:
        self._watchlist: list[str] = _env_watchlist()
        self._interval = max(0.5, float(os.environ.get("WATCHLIST_RECORD_INTERVAL_S", "1.0")))
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_flush = 0.0
        self._last_tick = 0.0
        self._ticks = 0

    @property
    def enabled(self) -> bool:
        return bool(self._watchlist)

    def set_watchlist(self, symbols: list[str]) -> None:
        self._watchlist = [s.strip().upper() for s in symbols if s and s.strip()]

    async def start(self) -> None:
        if self._running or not self._watchlist:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        log.info("watchlist_recorder_start", symbols=len(self._watchlist), interval_s=self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        while self._running:
            try:
                if not is_market_open():
                    await asyncio.sleep(60)        # idle off-session; re-check
                    continue
                wl = list(self._watchlist)
                if not wl:
                    await asyncio.sleep(30)
                    continue

                # ONE batch HTTP for the whole list (real NSE, non-delayed).
                quotes = await tickertape.fetch_quotes(wl)
                now = time.time()
                for sym, row in quotes.items():
                    price = row.get("price")
                    if price:
                        live_candle_engine.record(sym, "NSE", price, row.get("volume") or 0, ts=now)
                self._ticks += 1
                self._last_tick = now

                # Periodic durable flush (no-op when no DB connected).
                if now - self._last_flush >= STORE_FLUSH_INTERVAL_S:
                    self._last_flush = now
                    for sym in wl:
                        await flush_live_to_store(sym, "NSE")
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                log.warning("watchlist_record_failed", error=str(e))
            await asyncio.sleep(self._interval)

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "running": self._running,
            "watchlist": self._watchlist,
            "count": len(self._watchlist),
            "intervalSeconds": self._interval,
            "ticks": self._ticks,
            "lastTickAgeS": round(time.time() - self._last_tick, 1) if self._last_tick else None,
            "marketOpen": is_market_open(),
        }


# Singleton
watchlist_recorder = WatchlistRecorder()
