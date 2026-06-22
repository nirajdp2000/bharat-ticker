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
# Ring depth per symbol = a FULL trading session so same-day 1s/10s is NEVER cut
# (6h15m @0.5s ≈ 45k samples ≈ ~7 MB/symbol). DATA-COMPLETENESS is the rule; RAM
# is managed by SCOPE not by dropping data — the recorder is opt-in and you size
# the watchlist to the host (RAM ≈ SAMPLE_MAXLEN*~150 B*watchlist). The durable
# store holds past days. Override LIVE_SAMPLE_MAXLEN only to fit a tiny host.
SAMPLE_MAXLEN = int(os.environ.get("LIVE_SAMPLE_MAXLEN", "46000"))
IDLE_TTL_S = 120            # stop a recorder after 2 min with no reads
# A symbol's sample stream is owned by ONE source; a 2nd source only takes over
# after the owner has been silent this long (B2 single-source rule).
PRIMARY_SOURCE_TTL_S = float(os.environ.get("LIVE_PRIMARY_TTL_S", "5"))
STORE_FLUSH_INTERVAL_S = float(os.environ.get("STORE_FLUSH_INTERVAL_S", "30"))  # 1s-candle → DB flush


class LiveCandleEngine:
    """Samples live LTP and serves 1s / 10s OHLCV candles on demand."""

    def __init__(self) -> None:
        # key -> deque[(epoch_seconds, price, cumulative_volume, source)]
        self._samples: dict[str, deque] = defaultdict(lambda: deque(maxlen=SAMPLE_MAXLEN))
        self._tasks: dict[str, asyncio.Task] = {}
        self._last_read: dict[str, float] = {}
        # Single-source rule (B2): a symbol can be fed by >1 recorder at once (the
        # on-demand sampler AND the always-on watchlist recorder use DIFFERENT
        # feeds — Groww vs Tickertape). Both are NSE, but interleaving their two
        # cumulative-volume series makes the tape jump backward AND would let
        # build() count both feeds' deltas (volume DOUBLED). So one source owns a
        # key at a time; a second source is dropped while the owner stays fresh,
        # and only takes over after the owner goes quiet (PRIMARY_TTL).
        self._primary_src: dict[str, str] = {}
        self._primary_ts: dict[str, float] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(symbol: str, exchange: str) -> str:
        return f"{exchange.upper()}:{symbol.upper()}"

    def record(self, symbol: str, exchange: str, price: Any, cum_volume: Any,
               ts: float | None = None, source: str | None = None) -> None:
        """Push one live sample. Ignores non-positive prices.

        ``source`` tags the venue/provider so the volume-delta in :meth:`build`
        is only taken between SAME-source samples — a cross-exchange flip
        (NSE↔BSE) carries a different cumulative volume and would otherwise
        inject a spurious spike or a backward jump (B2 fix).

        Single-source rule (B2): one source owns the key at a time. A different
        source is DROPPED while the owner is still fresh, so two concurrent
        recorders (Groww on-demand + Tickertape watchlist) can't interleave two
        cumulative-volume series (backward tape / doubled bucket volume). After
        the owner goes quiet for ``PRIMARY_SOURCE_TTL_S`` a new source takes over.
        """
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
        key = self._key(symbol, exchange)
        now_ts = ts if ts is not None else time.time()
        if source is not None:
            owner = self._primary_src.get(key)
            owner_fresh = (now_ts - self._primary_ts.get(key, 0.0)) <= PRIMARY_SOURCE_TTL_S
            if owner is not None and owner != source and owner_fresh:
                return  # secondary source while the owner is live — drop it
            self._primary_src[key] = source
            self._primary_ts[key] = now_ts
        self._samples[key].append((now_ts, p, v, source))

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
                symbol, exchange, exclude_delayed=True, write_cache=False, pin_exchange=True
            )
        except Exception as e:  # noqa: BLE001
            log.debug("live_candle_sample_once_failed", symbol=symbol, error=str(e))
            return False
        if tick:
            self.record(symbol, exchange, tick.ltp, tick.volume, source=tick.source)
            return True
        return False

    async def _record_loop(self, symbol: str, exchange: str, key: str) -> None:
        from .market_data import market_data_service
        log.info("live_candle_recorder_start", symbol=symbol, exchange=exchange)
        last_flush = time.time()
        try:
            while True:
                if time.time() - self._last_read.get(key, 0) > IDLE_TTL_S:
                    break
                loop_t0 = time.time()
                try:
                    # pin_exchange=True keeps every sample on ONE venue so the
                    # cumulative-volume series stays single-source (B2).
                    tick = await market_data_service.get_quote_through(
                        symbol, exchange, exclude_delayed=True, write_cache=False, pin_exchange=True
                    )
                    if tick:
                        self.record(symbol, exchange, tick.ltp, tick.volume, source=tick.source)
                except Exception as e:  # noqa: BLE001
                    log.debug("live_candle_sample_failed", symbol=symbol, error=str(e))
                # Periodically persist 1s candles so PAST days survive a restart /
                # the idle stop. No-op when no durable DB is connected.
                if time.time() - last_flush >= STORE_FLUSH_INTERVAL_S:
                    last_flush = time.time()
                    try:
                        from .intraday_store import flush_live_to_store
                        await flush_live_to_store(symbol, exchange)
                    except Exception as e:  # noqa: BLE001
                        log.debug("live_candle_flush_failed", symbol=symbol, error=str(e))
                # Non-additive cadence (B6): the per-sample upstream fetch already
                # consumes wall-clock — sleep only the REMAINDER so the loop period
                # is max(fetch, SAMPLE_INTERVAL_S), not fetch + SAMPLE_INTERVAL_S.
                await asyncio.sleep(max(0.0, SAMPLE_INTERVAL_S - (time.time() - loop_t0)))
        finally:
            # Final flush on stop so the tail of the session is durable.
            try:
                from .intraday_store import flush_live_to_store
                await flush_live_to_store(symbol, exchange)
            except Exception:  # noqa: BLE001
                pass
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
        prev_src: str | None = None
        for ts, price, cum, src in samples:
            idx = math.floor(ts / seconds)        # int bucket index (fractional-safe)
            cell = buckets.get(idx)
            if cell is None:
                cell = {"o": price, "h": price, "l": price, "c": price, "v": 0.0}
                buckets[idx] = cell
                order.append(idx)
            cell["h"] = max(cell["h"], price)
            cell["l"] = min(cell["l"], price)
            cell["c"] = price
            # Volume delta ONLY between same-source samples (B2): a venue/provider
            # flip carries an unrelated cumulative total — counting that delta
            # would fabricate volume. Re-baseline silently on a source change.
            if prev_cum is not None and src == prev_src and cum >= prev_cum:
                cell["v"] += (cum - prev_cum)
            prev_cum = cum
            prev_src = src

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
        ts, price, cum, src = dq[-1]
        return {
            "timestamp": datetime.fromtimestamp(ts, tz=IST).isoformat(timespec="milliseconds"),
            "epoch": ts, "price": round(price, 2), "cumVolume": int(cum), "source": src,
        }

    def measured_cadence_s(self, symbol: str, exchange: str = "NSE") -> float | None:
        """Median inter-sample gap over the recent tail — the TRUE achievable
        resolution (bounded by upstream fetch latency), not the configured
        target. Lets callers see real frequency instead of an aspirational one (B6).
        """
        dq = self._samples.get(self._key(symbol, exchange))
        if not dq or len(dq) < 3:
            return None
        tail = list(dq)[-20:]
        gaps = sorted(tail[i + 1][0] - tail[i][0] for i in range(len(tail) - 1))
        return round(gaps[len(gaps) // 2], 2) if gaps else None

    def status(self, symbol: str, exchange: str = "NSE") -> dict[str, Any]:
        key = self._key(symbol, exchange)
        return {
            "recording": bool(self._tasks.get(key) and not self._tasks[key].done()),
            "samples": len(self._samples.get(key, ())),
            "samplerTargetSeconds": SAMPLE_INTERVAL_S,
            "measuredCadenceSeconds": self.measured_cadence_s(symbol, exchange),
            "market_open": is_market_open(),
        }


# Singleton
live_candle_engine = LiveCandleEngine()
