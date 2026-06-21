"""Live sub-minute candle aggregator (1-second / 10-second OHLCV).

NSE/BSE and the broker relays expose only 1-minute as the smallest historical
interval.  True 1s / 10s candles need a live tick stream.  This engine builds
them by SAMPLING the live last-traded price (via the failover provider stack —
NSE/BSE live, never delayed Yahoo) at a fixed cadence and bucketing the samples
into N-second OHLCV bars.

A per-symbol background recorder starts on first request and auto-stops after an
idle window (no reads).  Samples are kept in a bounded ring buffer in memory.
Per-bucket volume is the delta of the provider's cumulative day-volume between
samples (so each bar carries the volume actually traded in that window).
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..utils.ist_clock import is_market_open
from ..utils.logger import get_logger

log = get_logger(__name__)
IST = ZoneInfo("Asia/Kolkata")

# Poll cadence — how fresh the LATEST price is and the finest bucket we can fill.
# 0.5s (2 Hz) → sub-second 500ms buckets, latest price ≤0.5s stale, safe load on
# the public Groww/MC feed. Override with LIVE_SAMPLE_INTERVAL_S.
SAMPLE_INTERVAL_S = max(0.1, float(os.environ.get("LIVE_SAMPLE_INTERVAL_S", "0.5")))
SAMPLE_MAXLEN = 12000       # ~100 min of 0.5s samples per symbol
IDLE_TTL_S = 120            # stop a recorder after 2 min with no reads


class LiveCandleEngine:
    """Samples live LTP and serves 1s / 10s OHLCV candles on demand."""

    def __init__(self) -> None:
        # key -> deque[(epoch_seconds, price, cumulative_volume)]
        self._samples: dict[str, deque] = defaultdict(lambda: deque(maxlen=SAMPLE_MAXLEN))
        self._tasks: dict[str, asyncio.Task] = {}
        self._last_read: dict[str, float] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(symbol: str, exchange: str) -> str:
        return f"{exchange.upper()}:{symbol.upper()}"

    def record(self, symbol: str, exchange: str, price: Any, cum_volume: Any, ts: float | None = None) -> None:
        """Push one live sample. Ignores non-positive prices."""
        try:
            p = float(price)
        except (TypeError, ValueError):
            return
        if p <= 0:
            return
        try:
            v = float(cum_volume or 0)
        except (TypeError, ValueError):
            v = 0.0
        self._samples[self._key(symbol, exchange)].append((ts or time.time(), p, v))

    async def ensure_recorder(self, symbol: str, exchange: str = "NSE") -> None:
        """Start (or keep alive) the background sampler for this symbol."""
        key = self._key(symbol, exchange)
        self._last_read[key] = time.time()
        async with self._lock:
            task = self._tasks.get(key)
            if task and not task.done():
                return
            self._tasks[key] = asyncio.create_task(self._record_loop(symbol, exchange, key))

    async def sample_once(self, symbol: str, exchange: str = "NSE") -> bool:
        """Fetch one live tick immediately and record it. Returns True on success."""
        from .market_data import market_data_service
        try:
            tick = await market_data_service.get_quote_through(
                symbol, exchange, exclude_delayed=True, write_cache=False
            )
        except Exception as e:  # noqa: BLE001
            log.debug("live_candle_sample_once_failed", symbol=symbol, error=str(e))
            return False
        if tick:
            self.record(symbol, exchange, tick.ltp, tick.volume)
            return True
        return False

    async def _record_loop(self, symbol: str, exchange: str, key: str) -> None:
        from .market_data import market_data_service
        log.info("live_candle_recorder_start", symbol=symbol, exchange=exchange)
        try:
            while True:
                if time.time() - self._last_read.get(key, 0) > IDLE_TTL_S:
                    break
                try:
                    tick = await market_data_service.get_quote_through(
                        symbol, exchange, exclude_delayed=True, write_cache=False
                    )
                    if tick:
                        self.record(symbol, exchange, tick.ltp, tick.volume)
                except Exception as e:  # noqa: BLE001
                    log.debug("live_candle_sample_failed", symbol=symbol, error=str(e))
                await asyncio.sleep(SAMPLE_INTERVAL_S)
        finally:
            self._tasks.pop(key, None)
            log.info("live_candle_recorder_stop", symbol=symbol, exchange=exchange)

    def build(self, symbol: str, exchange: str, seconds: float, limit: int = 300) -> list[dict[str, Any]]:
        """Bucket the recorded samples into ``seconds``-wide OHLCV candles.

        ``seconds`` may be fractional (e.g. 0.5 → 500ms bars) for sub-second
        study; resolution is bounded by the sample cadence (SAMPLE_INTERVAL_S).
        """
        key = self._key(symbol, exchange)
        self._last_read[key] = time.time()
        samples = list(self._samples.get(key, ()))
        if not samples or seconds <= 0:
            return []

        buckets: dict[int, dict[str, Any]] = {}
        order: list[int] = []
        prev_cum: float | None = None
        for ts, price, cum in samples:
            idx = math.floor(ts / seconds)        # int bucket index (fractional-safe)
            cell = buckets.get(idx)
            if cell is None:
                cell = {"o": price, "h": price, "l": price, "c": price, "v": 0.0}
                buckets[idx] = cell
                order.append(idx)
            cell["h"] = max(cell["h"], price)
            cell["l"] = min(cell["l"], price)
            cell["c"] = price
            if prev_cum is not None and cum >= prev_cum:
                cell["v"] += (cum - prev_cum)
            prev_cum = cum

        sub_second = seconds < 1
        out: list[dict[str, Any]] = []
        for idx in order[-limit:]:
            c = buckets[idx]
            ts_iso = datetime.fromtimestamp(idx * seconds, tz=IST).isoformat(
                timespec="milliseconds" if sub_second else "seconds")
            out.append({
                "timestamp": ts_iso,
                "open": round(c["o"], 2), "high": round(c["h"], 2),
                "low": round(c["l"], 2), "close": round(c["c"], 2),
                "volume": int(c["v"]), "oi": 0,
            })
        return out

    def latest_tick(self, symbol: str, exchange: str = "NSE") -> dict[str, Any] | None:
        """Most recent recorded sample (for SSE push / lowest-latency read)."""
        key = self._key(symbol, exchange)
        self._last_read[key] = time.time()
        dq = self._samples.get(key)
        if not dq:
            return None
        ts, price, cum = dq[-1]
        return {
            "timestamp": datetime.fromtimestamp(ts, tz=IST).isoformat(timespec="milliseconds"),
            "epoch": ts, "price": round(price, 2), "cumVolume": int(cum),
        }

    def status(self, symbol: str, exchange: str = "NSE") -> dict[str, Any]:
        key = self._key(symbol, exchange)
        return {
            "recording": bool(self._tasks.get(key) and not self._tasks[key].done()),
            "samples": len(self._samples.get(key, ())),
            "market_open": is_market_open(),
        }


# Singleton
live_candle_engine = LiveCandleEngine()
